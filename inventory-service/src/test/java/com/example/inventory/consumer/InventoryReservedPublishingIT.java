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
 * Verifikuje da posle uspesne rezervacije zaliha, Inventory objavi
 * InventoryReserved event na inventory.reserved topic - preko outbox-a.
 */
@SpringBootTest
@Testcontainers
class InventoryReservedPublishingIT {

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
        r.add("spring.kafka.consumer.group-id", () -> "inv-reserved-test-" + System.currentTimeMillis());
    }

    @Autowired KafkaTemplate<String, Object> kafkaTemplate;

    private Consumer<String, String> consumer;

    @AfterEach
    void close() {
        if (consumer != null) consumer.close();
    }

    @Test
    @DisplayName("posle skidanja zaliha, InventoryReserved event ide na inventory.reserved")
    void publishesInventoryReserved() {
        // listener za inventory.reserved
        Map<String, Object> p = new HashMap<>(KafkaTestUtils.consumerProps(
                kafka.getBootstrapServers(), "reserved-listener", "true"));
        p.put(AUTO_OFFSET_RESET_CONFIG, "earliest");
        p.put(KEY_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringDeserializer");
        p.put(VALUE_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringDeserializer");
        consumer = new org.apache.kafka.clients.consumer.KafkaConsumer<>(p);
        consumer.subscribe(List.of("inventory.reserved"));

        // posalji OrderCreated event
        OrderCreatedEvent event = new OrderCreatedEvent(
                "evt-saga-1", "OrderCreated", 1, Instant.now(), "ORDER-SAGA-1",
                new OrderCreatedEvent.OrderPayload("ORDER-SAGA-1", "CUST-S",
                        List.of(new OrderCreatedEvent.OrderLine("SKU-001", 1)))
        );
        kafkaTemplate.send("orders.created", event.aggregateId(), event);

        // saceka InventoryReserved
        ConsumerRecord<String, String> record = KafkaTestUtils.getSingleRecord(
                consumer, "inventory.reserved", Duration.ofSeconds(20));

        assertThat(record).isNotNull();
        assertThat(record.key()).isEqualTo("ORDER-SAGA-1");
        assertThat(record.value()).contains("InventoryReserved");
        assertThat(record.value()).contains("ORDER-SAGA-1");
        assertThat(record.value()).contains("evt-saga-1");  // originatingEventId
    }
}
