package com.example.orderapi.consumer;

import com.example.common.event.InventoryReservedEvent;
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
public class InventoryReservedListener {

    private final OrderRepository orderRepository;

    @KafkaListener(
            topics = "inventory.reserved",
            groupId = "${spring.kafka.consumer.group-id}",
            containerFactory = "reservedListenerContainerFactory")
    @Transactional
    public void handle(InventoryReservedEvent event) {
        String orderId = event.payload().orderId();
        LoggingContext.setOrderContext(orderId, event.eventId(), event.eventType());
        try {
            log.debug("Primljen InventoryReserved");

            Optional<Order> opt = orderRepository.findById(orderId);
            if (opt.isEmpty()) {
                log.warn("Order ne postoji, preskacem InventoryReserved event");
                return;
            }

            Order order = opt.get();
            if ("CONFIRMED".equals(order.getStatus())) {
                LoggingContext.setStatus("CONFIRMED");
                log.debug("Vec CONFIRMED, preskacem");
                return;
            }

            order.setStatus("CONFIRMED");
            orderRepository.save(order);
            LoggingContext.setStatus("CONFIRMED");
            log.info("Prebacen u status CONFIRMED");
        } finally {
            LoggingContext.clear();
        }
    }
}

