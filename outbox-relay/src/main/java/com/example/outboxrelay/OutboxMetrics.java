package com.example.outboxrelay;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

/**
 * Gauge metrika - koliko outbox redova ceka da bude objavljeno.
 * Prometheus scrape-uje na svakih nekoliko sekundi, ova metrika daje
 * trenutnu vrednost. Ako raste i ne pada -> Relay je zaglavljen ili
 * preopterecen.
 */
@Component
@RequiredArgsConstructor
public class OutboxMetrics {

    private final OutboxRepository outboxRepository;
    private final MeterRegistry meterRegistry;

    @PostConstruct
    public void register() {
        Gauge.builder("outbox.pending.size", outboxRepository, this::countUnpublished)
                .description("Broj outbox redova koji cekaju objavu na Kafku")
                .register(meterRegistry);
    }

    private double countUnpublished(OutboxRepository repo) {
        // Koristimo isti upit kao Relay, ali bez locking-a - samo prebrojavanje
        // Za demo: dovoljno je da pozovemo native query, kasnije moze count
        return repo.countUnpublished();
    }
}
