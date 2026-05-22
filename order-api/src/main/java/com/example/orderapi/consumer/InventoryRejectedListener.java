package com.example.orderapi.consumer;

import com.example.common.event.InventoryRejectedEvent;
import com.example.common.logging.LoggingContext;
import com.example.orderapi.domain.Order;
import com.example.orderapi.domain.OrderRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.util.Optional;

@Slf4j
@Component
@RequiredArgsConstructor
public class InventoryRejectedListener {

    private final OrderRepository orderRepository;

    @KafkaListener(
            topics = "inventory.rejected",
            groupId = "${spring.kafka.consumer.group-id}",
            containerFactory = "rejectedListenerContainerFactory")
    @Transactional
    public void handle(InventoryRejectedEvent event) {
        String orderId = event.payload().orderId();
        InventoryRejectedEvent.Reason reason = event.payload().reason();
        LoggingContext.setOrderContext(orderId, event.eventId(), event.eventType());
        try {
            log.debug("Primljen InventoryRejected, razlog {}", reason);

            Optional<Order> opt = orderRepository.findById(orderId);
            if (opt.isEmpty()) {
                log.warn("Order ne postoji, preskacem InventoryRejected event");
                return;
            }

            Order order = opt.get();
            if ("REJECTED".equals(order.getStatus())) {
                LoggingContext.setStatus("REJECTED");
                log.debug("Vec REJECTED, preskacem");
                return;
            }
            if ("CONFIRMED".equals(order.getStatus())) {
                LoggingContext.setStatus("CONFIRMED");
                log.error("Vec CONFIRMED ali stigao InventoryRejected ({})! Pogledaj uzrok.", reason);
                return;
            }

            order.setStatus("REJECTED");
            order.setRejectionReason(reason.name());
            orderRepository.save(order);
            LoggingContext.setStatus("REJECTED");
            log.info("Prebacen u status REJECTED, razlog: {} ({})", reason, event.payload().details());
        } finally {
            LoggingContext.clear();
        }
    }
}

