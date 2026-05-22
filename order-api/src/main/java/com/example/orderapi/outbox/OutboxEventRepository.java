package com.example.orderapi.outbox;

import org.springframework.data.jpa.repository.JpaRepository;

import java.util.UUID;

/**
 * Order API samo UPISUJE u outbox. Citanje/objavu radi zaseban Outbox Relay servis.
 */
public interface OutboxEventRepository extends JpaRepository<OutboxEvent, UUID> {
}
