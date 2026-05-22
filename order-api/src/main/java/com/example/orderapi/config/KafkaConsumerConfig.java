package com.example.orderapi.config;

import com.example.common.event.InventoryRejectedEvent;
import com.example.common.event.InventoryReservedEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.kafka.support.serializer.JsonDeserializer;

import java.util.HashMap;
import java.util.Map;

/**
 * Order API konzumira dva topica:
 *  - inventory.reserved  -> InventoryReservedEvent -> status CONFIRMED
 *  - inventory.rejected  -> InventoryRejectedEvent -> status REJECTED
 *
 * Pravimo zaseban ConsumerFactory za svaki tip jer JsonDeserializer
 * mora da zna konkretan tip pri konstrukciji (USE_TYPE_INFO_HEADERS=false).
 * Container factory-ji imaju imena koja listeneri referenciraju.
 */
@Configuration
public class KafkaConsumerConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${spring.kafka.consumer.group-id}")
    private String groupId;

    private final ObjectMapper objectMapper;

    public KafkaConsumerConfig(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    private Map<String, Object> baseProps() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        return props;
    }

    private <T> ConsumerFactory<String, T> factoryFor(Class<T> type) {
        JsonDeserializer<T> json = new JsonDeserializer<>(type, objectMapper, false);
        json.addTrustedPackages("com.example.common.event");

        ErrorHandlingDeserializer<String> keyDeser =
                new ErrorHandlingDeserializer<>(new StringDeserializer());
        ErrorHandlingDeserializer<T> valueDeser = new ErrorHandlingDeserializer<>(json);

        return new DefaultKafkaConsumerFactory<>(baseProps(), keyDeser, valueDeser);
    }

    // ---------- InventoryReserved ----------
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, InventoryReservedEvent>
            reservedListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, InventoryReservedEvent> f =
                new ConcurrentKafkaListenerContainerFactory<>();
        f.setConsumerFactory(factoryFor(InventoryReservedEvent.class));
        // Observation flag iz application.yml ne primenjuje se na rucno definisanu
        // factory - moramo eksplicitno. Bez ovoga, consumer ne cita traceparent
        // header pa Tempo ne vidi parent span iz order-api ili outbox-relay.
        f.getContainerProperties().setObservationEnabled(true);
        return f;
    }

    // ---------- InventoryRejected ----------
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, InventoryRejectedEvent>
            rejectedListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, InventoryRejectedEvent> f =
                new ConcurrentKafkaListenerContainerFactory<>();
        f.setConsumerFactory(factoryFor(InventoryRejectedEvent.class));
        f.getContainerProperties().setObservationEnabled(true);
        return f;
    }
}
