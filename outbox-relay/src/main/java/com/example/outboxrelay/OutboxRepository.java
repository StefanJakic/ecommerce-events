package com.example.outboxrelay;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

public interface OutboxRepository extends JpaRepository<OutboxEvent, UUID> {

    /**
     * Uzima batch neobjavljenih eventa i ZAKLJUCAVA te redove.
     *
     * FOR UPDATE SKIP LOCKED: ako vrtis vise instanci relay-ja, svaka preskace
     * redove koje je druga vec zakljucala -> nema duplikata, nema cekanja.
     * Sa jednom instancom radi identicno (nema sta da preskoci).
     *
     * Brava drzi do kraja transakcije pozivaoca - zato citanje i markiranje
     * moraju biti u ISTOJ @Transactional metodi (vidi OutboxRelay).
     *
     * Nativni SQL namerno: FOR UPDATE SKIP LOCKED je eksplicitno vidljiv.
     */
    @Query(value = """
           SELECT * FROM outbox
           WHERE published_at IS NULL
           ORDER BY created_at ASC
           LIMIT :limit
           FOR UPDATE SKIP LOCKED
           """, nativeQuery = true)
    List<OutboxEvent> lockUnpublishedBatch(@Param("limit") int limit);

    /**
     * Markira evente kao objavljene. Poziva se u istoj transakciji,
     * dok brave iz lockUnpublishedBatch jos drze.
     */
    @Modifying
    @Query("UPDATE OutboxEvent o SET o.publishedAt = :now WHERE o.id IN :ids")
    void markPublished(@Param("ids") List<UUID> ids, @Param("now") Instant now);

    /**
     * Count neobjavljenih za metriku - bez locking-a.
     */
    @Query("SELECT COUNT(o) FROM OutboxEvent o WHERE o.publishedAt IS NULL")
    long countUnpublished();
}
