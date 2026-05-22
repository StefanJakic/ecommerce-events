package com.example.orderapi.outbox;

import jakarta.persistence.*;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.time.Instant;
import java.util.UUID;

/**
 * Jedan red u outbox tabeli = jedan event koji ceka da bude objavljen na Kafku.
 * Upisuje se u ISTOJ transakciji sa Order entitetom (atomicnost).
 * Outbox Relay servis kasnije cita ovu tabelu i objavljuje na Kafku.
 */
@Entity
@Table(name = "outbox")
@Getter
@Setter
@NoArgsConstructor
public class OutboxEvent {

    @Id
    private UUID id;

    /** orderId - ide kao Kafka message key da svi eventi za isti order idu u istu particiju */
    @Column(nullable = false)
    private String aggregateId;

    @Column(nullable = false)
    private String eventType;

    /** ceo event serijalizovan u JSON */
    @Column(nullable = false, columnDefinition = "text")
    private String payload;

    @Column(nullable = false)
    private Instant createdAt;

    /** NULL = jos nije objavljeno; popunjava ga Relay nakon uspesnog send-a */
    @Column
    private Instant publishedAt;

    public OutboxEvent(String aggregateId, String eventType, String payload) {
        this.id = UUID.randomUUID();
        this.aggregateId = aggregateId;
        this.eventType = eventType;
        this.payload = payload;
        this.createdAt = Instant.now();
    }
}
