package com.example.inventory.outbox;

import jakarta.persistence.*;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.time.Instant;
import java.util.UUID;

/**
 * Outbox za Inventory servis - isti pattern kao u Order API-ju.
 * Upisuje se u istoj transakciji sa skidanjem zaliha.
 * InventoryOutboxRelay ga kasnije objavljuje na Kafku.
 */
@Entity
@Table(name = "inventory_outbox")
@Getter
@Setter
@NoArgsConstructor
public class InventoryOutboxEvent {

    @Id
    private UUID id;

    @Column(nullable = false)
    private String aggregateId;

    @Column(nullable = false)
    private String eventType;

    @Column(nullable = false, columnDefinition = "text")
    private String payload;

    @Column(nullable = false)
    private Instant createdAt;

    @Column
    private Instant publishedAt;

    public InventoryOutboxEvent(String aggregateId, String eventType, String payload) {
        this.id = UUID.randomUUID();
        this.aggregateId = aggregateId;
        this.eventType = eventType;
        this.payload = payload;
        this.createdAt = Instant.now();
    }
}
