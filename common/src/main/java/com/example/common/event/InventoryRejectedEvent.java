package com.example.common.event;

import java.time.Instant;

/**
 * Event koji Inventory objavljuje kad rezervacija ne moze da se zavrsi
 * iz POSLOVNIH razloga (nepoznat SKU, nedovoljno zaliha).
 *
 * Order API ga konzumira i postavlja status na REJECTED sa razlogom.
 *
 * Razlika u odnosu na DLT:
 *  - Ovo je normalan poslovni ishod -> klijent treba da sazna
 *  - DLT je za TEHNICKE greske (bug, malformiran JSON) -> operater
 */
public record InventoryRejectedEvent(
        String eventId,
        String eventType,
        int schemaVersion,
        Instant occurredAt,
        String aggregateId,
        Payload payload
) {
    public static final String EVENT_TYPE = "InventoryRejected";
    public static final int CURRENT_SCHEMA_VERSION = 1;

    /**
     * Razlog odbijanja - TIPOVAN, ne slobodan tekst.
     * Peers koji odlucuju na osnovu razloga koriste enum.
     * Slobodan tekst ide u 'details' - to je za log i UI.
     */
    public enum Reason {
        UNKNOWN_SKU,
        INSUFFICIENT_STOCK,
        SYSTEM_ERROR   // tehnicka greska iz DLT-a (deserialization, bug, baza)
    }

    public record Payload(
            String orderId,
            String originatingEventId,
            Reason reason,
            String details
    ) {}
}
