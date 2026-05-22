package com.example.inventory.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Inventory nije web servis, pa Spring Boot ne pravi default ObjectMapper.
 * Definicija ovde da bismo imali jedan mapper za:
 *  - upis InventoryReserved u outbox (InventoryService)
 *  - eventualnu serijalizaciju u Kafka producer config
 */
@Configuration
public class JacksonConfig {

    @Bean
    public ObjectMapper objectMapper() {
        return new ObjectMapper()
                .registerModule(new JavaTimeModule())
                // ISO 8601 string umesto numeric timestamp - citljivije
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
    }
}
