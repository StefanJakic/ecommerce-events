package com.example.inventory.consumer;

import com.example.common.event.OrderCreatedEvent;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.kafka.clients.consumer.ConsumerConfig.*;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifikuje da POSLOVNE greske (nepoznat SKU, nedovoljno zaliha) zavrsavaju
 * kao InventoryRejected event na inventory.rejected topiku - NE na DLT.
 *
 * Ovo je kljucni test za saga "negativnu" granu.
 */
@SpringBootTest
@Testcontainers
class InventoryRejectedPublishingIT {

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
        r.add("spring.kafka.consumer.group-id", () -> "inv-rejected-test-" + System.currentTimeMillis());
    }

    @Autowired KafkaTemplate<String, Object> kafkaTemplate;

    private Consumer<String, String> consumer;

    @AfterEach
    void close() {
        if (consumer != null) consumer.close();
    }

    private Consumer<String, String> subscribeTo(String topic) {
        Map<String, Object> p = new HashMap<>(KafkaTestUtils.consumerProps(
                kafka.getBootstrapServers(), "rejection-test-" + topic, "true"));
        p.put(AUTO_OFFSET_RESET_CONFIG, "earliest");
        p.put(KEY_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringDeserializer");
        p.put(VALUE_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringDeserializer");
        Consumer<String, String> c = new org.apache.kafka.clients.consumer.KafkaConsumer<>(p);
        c.subscribe(List.of(topic));
        return c;
    }

    @Test
    @DisplayName("nepoznat SKU -> InventoryRejected sa razlogom UNKNOWN_SKU")
    void unknownSkuGoesToRejected() {
        consumer = subscribeTo("inventory.rejected");

        OrderCreatedEvent event = new OrderCreatedEvent(
                "evt-rej-1", "OrderCreated", 1, Instant.now(), "ORDER-REJ-1",
                new OrderCreatedEvent.OrderPayload("ORDER-REJ-1", "CUST-X",
                        List.of(new OrderCreatedEvent.OrderLine("SKU-NE-POSTOJI", 1)))
        );
        kafkaTemplate.send("orders.created", event.aggregateId(), event);

        ConsumerRecord<String, String> record = KafkaTestUtils.getSingleRecord(
                consumer, "inventory.rejected", Duration.ofSeconds(20));

        assertThat(record).isNotNull();
        assertThat(record.key()).isEqualTo("ORDER-REJ-1");
        assertThat(record.value()).contains("InventoryRejected");
        assertThat(record.value()).contains("UNKNOWN_SKU");
        assertThat(record.value()).contains("SKU-NE-POSTOJI");
    }

    @Test
    @DisplayName("nedovoljno zaliha -> InventoryRejected sa razlogom INSUFFICIENT_STOCK")
    void insufficientStockGoesToRejected() {
        consumer = subscribeTo("inventory.rejected");

        // SKU-042 ima 10 komada, trazimo 999
        OrderCreatedEvent event = new OrderCreatedEvent(
                "evt-rej-2", "OrderCreated", 1, Instant.now(), "ORDER-REJ-2",
                new OrderCreatedEvent.OrderPayload("ORDER-REJ-2", "CUST-Y",
                        List.of(new OrderCreatedEvent.OrderLine("SKU-042", 999)))
        );
        kafkaTemplate.send("orders.created", event.aggregateId(), event);

        ConsumerRecord<String, String> record = KafkaTestUtils.getSingleRecord(
                consumer, "inventory.rejected", Duration.ofSeconds(20));

        assertThat(record).isNotNull();
        assertThat(record.value()).contains("InventoryRejected");
        assertThat(record.value()).contains("INSUFFICIENT_STOCK");
        assertThat(record.value()).contains("trazeno 999");
        assertThat(record.value()).contains("dostupno 10");
    }
}
