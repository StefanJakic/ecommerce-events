package com.example.orderapi;

import com.example.common.event.InventoryRejectedEvent;
import com.example.common.event.InventoryReservedEvent;
import com.example.orderapi.api.CreateOrderRequest;
import com.example.orderapi.api.OrderResponse;
import com.example.orderapi.domain.OrderRepository;
import com.example.orderapi.outbox.OutboxEventRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * End-to-end test ZA ORDER API:
 *  1. HTTP POST /orders -> proverava da order I outbox red postoje
 *  2. simulira Inventory tako sto direktno objavi event na Kafku
 *  3. ceka da status ordera predje na CONFIRMED ili REJECTED
 *
 * Outbox relay i Inventory servis NISU pokrenuti u testu - simuliramo ih.
 * Time testiramo Order API granicu sistema sa svih strana:
 *  - HTTP ulaz (POST)
 *  - DB izlaz (outbox)
 *  - Kafka ulaz (InventoryReserved/Rejected)
 *  - DB ishod (status, rejection_reason)
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@Testcontainers
class OrderApiEndToEndIT {

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
        r.add("spring.kafka.consumer.group-id", () -> "order-api-test-" + System.currentTimeMillis());
    }

    @Autowired TestRestTemplate http;
    @Autowired OrderRepository orderRepository;
    @Autowired OutboxEventRepository outboxRepository;

    private KafkaTemplate<String, Object> testProducer;

    /**
     * Pravimo producer u testu (Order API ga nema u production kodu, on samo konzumira).
     * Koristimo JsonSerializer sa JavaTimeModule da bi Instant bio serijalizovan
     * isto kao sto bi pravi Inventory servis poslao.
     */
    private KafkaTemplate<String, Object> createProducer() {
        ObjectMapper mapper = new ObjectMapper().registerModule(new JavaTimeModule());
        JsonSerializer<Object> valueSerializer = new JsonSerializer<>(mapper);
        valueSerializer.setAddTypeInfo(false);

        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());

        return new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(
                props, new StringSerializer(), valueSerializer));
    }

    @AfterEach
    void closeProducer() {
        if (testProducer != null) {
            testProducer.destroy();
            testProducer = null;
        }
    }

    @Test
    @DisplayName("POST /orders -> outbox upisan -> InventoryReserved -> status CONFIRMED")
    void happyPath_endToEnd() {
        testProducer = createProducer();

        // 1. POST /orders
        var request = new CreateOrderRequest("CUST-E2E",
                List.of(new CreateOrderRequest.Item("SKU-001", 2)));

        ResponseEntity<OrderResponse> response = http.postForEntity("/orders", request, OrderResponse.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.CREATED);
        String orderId = response.getBody().orderId();
        assertThat(orderId).startsWith("ORDER-");

        // 2. Order i outbox red postoje
        var order = orderRepository.findById(orderId).orElseThrow();
        assertThat(order.getStatus()).isEqualTo("CREATED");
        assertThat(outboxRepository.count()).isGreaterThanOrEqualTo(1);

        // 3. Simuliraj Inventory: objavi InventoryReserved
        InventoryReservedEvent reserved = new InventoryReservedEvent(
                "evt-reserved-1",
                InventoryReservedEvent.EVENT_TYPE,
                1,
                Instant.now(),
                orderId,
                new InventoryReservedEvent.Payload(orderId, "originating-evt-1")
        );
        testProducer.send(new ProducerRecord<>("inventory.reserved", orderId, reserved));
        testProducer.flush();

        // 4. Status prelazi na CONFIRMED
        await().atMost(15, SECONDS).untilAsserted(() -> {
            var fresh = orderRepository.findById(orderId).orElseThrow();
            assertThat(fresh.getStatus()).isEqualTo("CONFIRMED");
        });
    }

    @Test
    @DisplayName("POST /orders -> InventoryRejected stigne -> status REJECTED sa razlogom")
    void rejectionPath_endToEnd() {
        testProducer = createProducer();

        var request = new CreateOrderRequest("CUST-E2E-REJ",
                List.of(new CreateOrderRequest.Item("SKU-X", 1)));

        ResponseEntity<OrderResponse> response = http.postForEntity("/orders", request, OrderResponse.class);
        String orderId = response.getBody().orderId();

        InventoryRejectedEvent rejected = new InventoryRejectedEvent(
                "evt-rejected-1",
                InventoryRejectedEvent.EVENT_TYPE,
                1,
                Instant.now(),
                orderId,
                new InventoryRejectedEvent.Payload(orderId, "originating-evt-1",
                        InventoryRejectedEvent.Reason.UNKNOWN_SKU,
                        "Nepoznat SKU: SKU-X")
        );
        testProducer.send(new ProducerRecord<>("inventory.rejected", orderId, rejected));
        testProducer.flush();

        await().atMost(15, SECONDS).untilAsserted(() -> {
            var fresh = orderRepository.findById(orderId).orElseThrow();
            assertThat(fresh.getStatus()).isEqualTo("REJECTED");
            assertThat(fresh.getRejectionReason()).isEqualTo("UNKNOWN_SKU");
        });
    }
}
