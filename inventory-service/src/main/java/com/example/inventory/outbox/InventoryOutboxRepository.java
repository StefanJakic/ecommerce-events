package com.example.inventory.outbox;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

public interface InventoryOutboxRepository extends JpaRepository<InventoryOutboxEvent, UUID> {

    @Query(value = """
           SELECT * FROM inventory_outbox
           WHERE published_at IS NULL
           ORDER BY created_at ASC
           LIMIT :limit
           FOR UPDATE SKIP LOCKED
           """, nativeQuery = true)
    List<InventoryOutboxEvent> lockUnpublishedBatch(@Param("limit") int limit);

    @Modifying
    @Query("UPDATE InventoryOutboxEvent o SET o.publishedAt = :now WHERE o.id IN :ids")
    void markPublished(@Param("ids") List<UUID> ids, @Param("now") Instant now);
}
