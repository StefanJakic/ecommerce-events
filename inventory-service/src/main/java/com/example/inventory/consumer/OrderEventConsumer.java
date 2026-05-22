package com.example.inventory.consumer;

import com.example.common.event.OrderCreatedEvent;
import com.example.common.logging.LoggingContext;
import com.example.inventory.domain.InventoryService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.DltHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.retry.annotation.Backoff;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class OrderEventConsumer {

    private final InventoryService inventoryService;

    @RetryableTopic(
            attempts = "3",
            backoff = @Backoff(delay = 200, multiplier = 2.0)
    )
    @KafkaListener(topics = "orders.created", groupId = "${spring.kafka.consumer.group-id}")
    public void handle(@Payload OrderCreatedEvent event,
                       @Header(KafkaHeaders.RECEIVED_PARTITION) int partition) {
        LoggingContext.setOrderContext(event.payload().orderId(), event.eventId(), event.eventType());
        try {
            log.debug("Primljen event sa particije {}", partition);
            inventoryService.reserve(event);
        } finally {
            LoggingContext.clear();
        }
    }

    @DltHandler
    public void handleDlt(@Payload OrderCreatedEvent event,
                          @Header(KafkaHeaders.ORIGINAL_TOPIC) String originalTopic,
                          @Header(name = KafkaHeaders.EXCEPTION_MESSAGE, required = false) String error) {
        String orderId = event.payload().orderId();
        LoggingContext.setOrderContext(orderId, event.eventId(), event.eventType());
        try {
            log.error("DLT: event sa topica {} nije obradjen. Razlog: {}", originalTopic, error);
            inventoryService.publishSystemError(
                    orderId,
                    event.eventId(),
                    "Tehnicka greska: " + (error != null ? error : "nepoznat razlog")
            );
        } finally {
            LoggingContext.clear();
        }
    }
}

