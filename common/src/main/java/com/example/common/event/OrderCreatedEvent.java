package com.example.common.event;

import java.time.Instant;
import java.util.List;

/**
 * Event koji Order API objavljuje kad se kreira porudzbina.
 * Ovo je UGOVOR izmedju servisa - menjati samo uz povecanje schemaVersion.
 *
 * - eventId      : jedinstven po eventu; Inventory ga koristi za idempotenciju
 * - aggregateId  : orderId; koristi se kao Kafka message key (redosled po orderu)
 * - schemaVersion: verzija seme, za buducu evoluciju
 */
public record OrderCreatedEvent(
        String eventId,
        String eventType,
        int schemaVersion,
        Instant occurredAt,
        String aggregateId,
        OrderPayload payload
) {
    public static final String EVENT_TYPE = "OrderCreated";
    public static final int CURRENT_SCHEMA_VERSION = 1;

    public record OrderPayload(
            String orderId,
            String customerId,
            List<OrderLine> items
    ) {}

    public record OrderLine(
            String sku,
            int quantity
    ) {}
}
