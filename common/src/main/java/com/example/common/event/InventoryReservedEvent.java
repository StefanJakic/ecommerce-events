package com.example.common.event;

import java.time.Instant;

/**
 * Event koji Inventory Service objavljuje nakon uspesne rezervacije zaliha.
 * Order API konzumira ovaj event i postavlja status ordera na CONFIRMED.
 *
 * Ovo zatvara saga: Order API ne zna unapred da li ce zalihe biti
 * dostupne; ceka da Inventory potvrdi. To je choreography saga pattern -
 * nema centralnog orkestratora, servisi se dogovaraju preko eventa.
 */
public record InventoryReservedEvent(
        String eventId,
        String eventType,
        int schemaVersion,
        Instant occurredAt,
        String aggregateId,
        Payload payload
) {
    public static final String EVENT_TYPE = "InventoryReserved";
    public static final int CURRENT_SCHEMA_VERSION = 1;

    public record Payload(
            String orderId,
            String originatingEventId   // eventId iz OrderCreated - za sledivost
    ) {}
}
