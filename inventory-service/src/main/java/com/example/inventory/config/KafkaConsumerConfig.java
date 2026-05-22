package com.example.inventory.config;

import com.example.common.event.OrderCreatedEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
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

@Configuration
public class KafkaConsumerConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${spring.kafka.consumer.group-id}")
    private String groupId;

    /**
     * ObjectMapper sa JavaTimeModule -> ume da parsira java.time.Instant.
     * Bez ovoga JsonDeserializer pravi svoj default mapper koji baca:
     *   "Java 8 date/time type `java.time.Instant` not supported by default"
     */
    private ObjectMapper objectMapper() {
        return new ObjectMapper().registerModule(new JavaTimeModule());
    }

    /**
     * JsonDeserializer eksplicitno sa nasim ObjectMapper-om i tipom.
     * Tip moramo da kazemo u konstruktoru jer headeri tipa su iskljuceni
     * (USE_TYPE_INFO_HEADERS=false).
     */
    private JsonDeserializer<OrderCreatedEvent> jsonDeserializer() {
        JsonDeserializer<OrderCreatedEvent> jd =
                new JsonDeserializer<>(OrderCreatedEvent.class, objectMapper(), false);
        jd.addTrustedPackages("com.example.common.event");
        return jd;
    }

    @Bean
    public ConsumerFactory<String, OrderCreatedEvent> consumerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        // ErrorHandlingDeserializer omotac - ako parser baci, error handler preuzme
        // umesto da konzument zaglavi u beskonacnoj petlji deserijalizacije.
        ErrorHandlingDeserializer<String> keyDeser =
                new ErrorHandlingDeserializer<>(new StringDeserializer());
        ErrorHandlingDeserializer<OrderCreatedEvent> valueDeser =
                new ErrorHandlingDeserializer<>(jsonDeserializer());

        // Konstruisemo factory sa explicit deserijalizatorima (ne preko props mape)
        // tako da nas ObjectMapper sa JavaTimeModule sigurno bude korisсen.
        return new DefaultKafkaConsumerFactory<>(props, keyDeser, valueDeser);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, OrderCreatedEvent> kafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, OrderCreatedEvent> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory());
        // Eksplicitno aktiviramo observation: kad container factory definisemo
        // kao @Bean, Spring Boot vise NE primenjuje auto-konfiguraciju iz
        // spring.kafka.listener.observation-enabled property-ja. Moramo rucno.
        // Ovo omogucava distributed tracing - listener cita traceparent header
        // i nastavlja postojeci trace umesto da pravi novi.
        factory.getContainerProperties().setObservationEnabled(true);
        return factory;
    }
}
