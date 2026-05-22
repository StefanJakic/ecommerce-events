package com.example.orderapi.api;

import com.example.orderapi.domain.Order;
import com.fasterxml.jackson.annotation.JsonInclude;

import java.time.Instant;
import java.util.List;

@JsonInclude(JsonInclude.Include.NON_NULL)
public record OrderResponse(
        String orderId,
        String customerId,
        String status,
        String rejectionReason,
        Instant createdAt,
        List<Line> items
) {
    public record Line(String sku, int quantity) {}

    public static OrderResponse from(Order order) {
        List<Line> lines = order.getItems().stream()
                .map(i -> new Line(i.getSku(), i.getQuantity()))
                .toList();
        return new OrderResponse(
                order.getId(),
                order.getCustomerId(),
                order.getStatus(),
                order.getRejectionReason(),
                order.getCreatedAt(),
                lines
        );
    }
}
