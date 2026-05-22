package com.example.inventory.consumer;

import com.example.common.event.OrderCreatedEvent;
import com.example.inventory.domain.InventoryItemRepository;
import com.example.inventory.domain.ProcessedEventRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.time.Instant;
import java.util.List;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Integracioni test: prava Postgres, prava Kafka u Docker kontejnerima.
 * Salje OrderCreatedEvent preko KafkaTemplate na "orders.created" i ceka
 * da Inventory consumer obradi.
 *
 * Pokriva: happy path + idempotencija (isti event poslat dvaput).
 */
@SpringBootTest
@Testcontainers
class InventoryConsumerIT {

    @Container
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16-alpine")
            .withDatabaseName("ecommerce")
            .withUsername("app")
            .withPassword("app");

    @Container
    static KafkaContainer kafka = new KafkaContainer(
            DockerImageName.parse("confluentinc/cp-kafka:7.6.1"));

    @DynamicPropertySource
    static void props(DynamicPropertyRegistry r) {
        r.add("spring.datasource.url", postgres::getJdbcUrl);
        r.add("spring.datasource.username", postgres::getUsername);
        r.add("spring.datasource.password", postgres::getPassword);
        r.add("spring.kafka.bootstrap-servers", kafka::getBootstrapServers);
        r.add("spring.kafka.consumer.group-id", () -> "inventory-test-" + System.currentTimeMillis());
    }

    @Autowired KafkaTemplate<String, Object> kafkaTemplate;
    @Autowired InventoryItemRepository inventoryRepo;
    @Autowired ProcessedEventRepository processedEventRepo;

    private OrderCreatedEvent buildEvent(String eventId, String orderId, String sku, int qty) {
        return new OrderCreatedEvent(
                eventId, "OrderCreated", 1, Instant.now(), orderId,
                new OrderCreatedEvent.OrderPayload(orderId, "CUST-1",
                        List.of(new OrderCreatedEvent.OrderLine(sku, qty)))
        );
    }

    @BeforeEach
    void cleanProcessed() {
        // svaki test krece sa praznom processed_event tabelom
        processedEventRepo.deleteAll();
    }

    @Test
    @DisplayName("happy path: event stigne, zalihe se skinu")
    void consumerReducesStock() {
        // schema.sql ucitava SKU-001 sa 100 komada
        int startQty = inventoryRepo.findById("SKU-001").orElseThrow().getAvailableQuantity();

        OrderCreatedEvent event = buildEvent("evt-happy-1", "ORDER-H1", "SKU-001", 3);
        kafkaTemplate.send("orders.created", event.aggregateId(), event);

        await().atMost(15, SECONDS).untilAsserted(() -> {
            int now = inventoryRepo.findById("SKU-001").orElseThrow().getAvailableQuantity();
            assertThat(now).isEqualTo(startQty - 3);
            assertThat(processedEventRepo.existsById("evt-happy-1")).isTrue();
        });
    }

    @Test
    @DisplayName("idempotencija: isti event poslat dvaput skida zalihe SAMO jednom")
    void duplicateEventReducesStockOnce() {
        int startQty = inventoryRepo.findById("SKU-002").orElseThrow().getAvailableQuantity();

        OrderCreatedEvent event = buildEvent("evt-dup-1", "ORDER-D1", "SKU-002", 5);

        // posalji isti event DVAPUT - simulira at-least-once isporuku
        kafkaTemplate.send("orders.created", event.aggregateId(), event);
        kafkaTemplate.send("orders.created", event.aggregateId(), event);

        // saceka da se prvi obradi
        await().atMost(15, SECONDS).untilAsserted(() ->
                assertThat(processedEventRepo.existsById("evt-dup-1")).isTrue());

        // pa jos malo da bismo bili sigurni da drugi NE skida ponovo
        await().during(3, SECONDS).atMost(5, SECONDS).untilAsserted(() -> {
            int now = inventoryRepo.findById("SKU-002").orElseThrow().getAvailableQuantity();
            assertThat(now).isEqualTo(startQty - 5);   // skinuto JEDNOM, ne 10
        });
    }
}
