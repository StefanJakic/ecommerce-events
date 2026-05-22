package com.example.inventory.domain;

import jakarta.persistence.*;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.time.Instant;

/**
 * Evidencija obradjenih eventa - srce idempotencije.
 *
 * Pre obrade proveravamo da li eventId vec postoji ovde. Ako da -
 * event je vec obradjen (npr. relay ga je poslao dvaput, ili je
 * @RetryableTopic retry-ovao posle delimicnog uspeha) -> preskacemo.
 *
 * Upis u ovu tabelu ide u ISTOJ transakciji sa skidanjem zaliha,
 * pa su "zalihe skinute" i "event zabelezen" atomicni.
 */
@Entity
@Table(name = "processed_event")
@Getter
@Setter
@NoArgsConstructor
public class ProcessedEvent {

    @Id
    private String eventId;

    @Column(nullable = false)
    private Instant processedAt;

    public ProcessedEvent(String eventId) {
        this.eventId = eventId;
        this.processedAt = Instant.now();
    }
}
