package com.example.inventory.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.serializer.JsonSerializer;

import java.util.HashMap;
import java.util.Map;

/**
 * Dva KafkaTemplate-a, svaki za drugu svrhu:
 *
 * 1. @Primary KafkaTemplate<String, Object> -> koristi ga @RetryableTopic za
 *    premestanje poruka na retry topике i DLT. Mora JSON da bi serijalizovao
 *    OrderCreatedEvent. JavaTimeModule potreban za Instant.
 *
 * 2. KafkaTemplate<String, String> "stringKafkaTemplate" -> koristi ga
 *    InventoryOutboxRelay. Payload je vec serijalizovan u outbox tabeli kao
 *    string, pa StringSerializer salje bajt-u-bajt - bez duplog enkodovanja.
 */
@Configuration
public class KafkaProducerConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    private Map<String, Object> baseProps() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        return props;
    }

    @Bean
    @Primary
    public ProducerFactory<String, Object> producerFactory() {
        ObjectMapper mapper = new ObjectMapper().registerModule(new JavaTimeModule());
        JsonSerializer<Object> valueSerializer = new JsonSerializer<>(mapper);
        valueSerializer.setAddTypeInfo(false);
        return new DefaultKafkaProducerFactory<>(baseProps(), new StringSerializer(), valueSerializer);
    }

    @Bean
    @Primary
    public KafkaTemplate<String, Object> kafkaTemplate() {
        KafkaTemplate<String, Object> template = new KafkaTemplate<>(producerFactory());
        // Producer-side observation: KafkaTemplate sam upisuje traceparent header
        // iz trenutnog span context-a. Bez ovoga consumer ne moze da zna parent trace.
        template.setObservationEnabled(true);
        return template;
    }

    @Bean
    public ProducerFactory<String, String> stringProducerFactory() {
        return new DefaultKafkaProducerFactory<>(baseProps(), new StringSerializer(), new StringSerializer());
    }

    @Bean("stringKafkaTemplate")
    public KafkaTemplate<String, String> stringKafkaTemplate() {
        KafkaTemplate<String, String> template = new KafkaTemplate<>(stringProducerFactory());
        template.setObservationEnabled(true);
        return template;
    }
}
