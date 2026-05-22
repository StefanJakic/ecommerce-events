package com.example.outboxrelay;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.List;

/**
 * Poll petlja: na svakih fixedDelay ms uzme batch neobjavljenih eventa,
 * objavi ih na Kafku, pa ih markira kao objavljene.
 *
 * Sve unutar JEDNE @Transactional metode jer FOR UPDATE brave moraju
 * da drze dok se redovi ne markiraju.
 *
 * Garancija je at-least-once: ako padne posle send-a a pre commita,
 * isti event ode ponovo pri sledecem pollu. Inventory to resava
 * idempotencijom (provera eventId).
 *
 * Tracing: KafkaTemplate ima observationEnabled=true (KafkaProducerConfig),
 * pa Spring sam upisuje traceparent header pri send-u. Consumer (sa
 * listener.observation-enabled=true u svom container factory-ju) cita
 * taj header i nastavlja trace. To znaci da je trace lanac:
 *   outbox-relay scheduled task -> inventory-service consume -> ...
 * jedan povezan trace u Tempo-u.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class OutboxRelay {

    private static final String TOPIC = "orders.created";

    private final OutboxRepository outboxRepository;
    private final KafkaTemplate<String, String> kafkaTemplate;

    @Value("${outbox.relay.batch-size:100}")
    private int batchSize;

    @Scheduled(fixedDelayString = "${outbox.relay.poll-interval-ms:500}")
    @Transactional
    public void publishPending() {
        List<OutboxEvent> batch = outboxRepository.lockUnpublishedBatch(batchSize);
        if (batch.isEmpty()) {
            return;
        }

        log.debug("Relay uzeo {} neobjavljenih eventa", batch.size());

        for (OutboxEvent event : batch) {
            // aggregateId (orderId) kao kljuc -> svi eventi za isti order
            // idu u istu particiju i obradjuju se po redu
            kafkaTemplate.send(TOPIC, event.getAggregateId(), event.getPayload());
            log.info("Objavljen event {} (order {}) na topic {}",
                    event.getEventType(), event.getAggregateId(), TOPIC);
        }

        // flush: cekaj da broker potvrdi sve send-ove pre nego markiramo
        kafkaTemplate.flush();

        List<java.util.UUID> ids = batch.stream().map(OutboxEvent::getId).toList();
        outboxRepository.markPublished(ids, Instant.now());
        log.debug("Markirano {} eventa kao objavljeno", ids.size());
    }
}
