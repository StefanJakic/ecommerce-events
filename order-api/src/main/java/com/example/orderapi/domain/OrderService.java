package com.example.orderapi.domain;

import com.example.common.event.OrderCreatedEvent;
import com.example.orderapi.api.CreateOrderRequest;
import com.example.orderapi.outbox.OutboxEvent;
import com.example.orderapi.outbox.OutboxEventRepository;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

@Slf4j
@Service
@RequiredArgsConstructor
public class OrderService {

    private final OrderRepository orderRepository;
    private final OutboxEventRepository outboxRepository;
    private final ObjectMapper objectMapper;

    /**
     * Kreira porudzbinu I outbox red u ISTOJ transakciji.
     * Ako bilo sta padne -> rollback oba -> nema ordera bez eventa ni obrnuto.
     * KLJUC: ovde NEMA kafkaTemplate.send() - objavu radi Outbox Relay.
     */
    @Transactional
    public Order createOrder(CreateOrderRequest request) {
        String orderId = "ORDER-" + UUID.randomUUID().toString().substring(0, 8).toUpperCase();

        Order order = new Order(orderId, request.customerId());
        request.items().forEach(i -> order.addItem(i.sku(), i.quantity()));
        orderRepository.save(order);
        log.debug("Order {} upisan u bazu", orderId);

        OrderCreatedEvent event = buildEvent(order);
        outboxRepository.save(new OutboxEvent(
                orderId,
                OrderCreatedEvent.EVENT_TYPE,
                toJson(event)
        ));
        log.info("Order {} kreiran, outbox event spreman za objavu", orderId);

        return order;
    }

    private OrderCreatedEvent buildEvent(Order order) {
        List<OrderCreatedEvent.OrderLine> lines = order.getItems().stream()
                .map(i -> new OrderCreatedEvent.OrderLine(i.getSku(), i.getQuantity()))
                .toList();

        return new OrderCreatedEvent(
                UUID.randomUUID().toString(),
                OrderCreatedEvent.EVENT_TYPE,
                OrderCreatedEvent.CURRENT_SCHEMA_VERSION,
                Instant.now(),
                order.getId(),
                new OrderCreatedEvent.OrderPayload(
                        order.getId(),
                        order.getCustomerId(),
                        lines
                )
        );
    }

    private String toJson(OrderCreatedEvent event) {
        try {
            return objectMapper.writeValueAsString(event);
        } catch (JsonProcessingException e) {
            // ovo je programerska greska, ne sme da se desi sa validnim recordom
            throw new IllegalStateException("Serijalizacija eventa nije uspela", e);
        }
    }
}
