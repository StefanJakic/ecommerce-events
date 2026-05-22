package com.example.inventory.outbox;

import com.example.common.event.InventoryRejectedEvent;
import com.example.common.event.InventoryReservedEvent;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

/**
 * Inventory-jev outbox relay - objavljuje evente na razlicite topike
 * zavisno od eventType:
 *  - InventoryReserved  -> inventory.reserved
 *  - InventoryRejected  -> inventory.rejected
 *
 * Ovo nije "vise topika u jednoj tabeli zbog lenjosti" - outbox tabela je
 * generican mehanizam za eventualno-objavljivanje. Topic je dispatch detalj,
 * a tabela cuva poredak po orderId-u (oba tipa za isti order moraju da idu redom).
 *
 * Tracing: KafkaTemplate ima observationEnabled=true; Spring sam upisuje
 * traceparent header. Tako se trace lanac od scheduled task-a propagira
 * do order-api consumer-a.
 */
@Slf4j
@Component
public class InventoryOutboxRelay {

    private final InventoryOutboxRepository outboxRepository;
    private final KafkaTemplate<String, String> kafkaTemplate;

    public InventoryOutboxRelay(InventoryOutboxRepository outboxRepository,
                                @Qualifier("stringKafkaTemplate") KafkaTemplate<String, String> kafkaTemplate) {
        this.outboxRepository = outboxRepository;
        this.kafkaTemplate = kafkaTemplate;
    }

    @Value("${inventory.outbox.relay.batch-size:100}")
    private int batchSize;

    /**
     * Dispatch: eventType -> topic. Ako stigne nepoznat tip, logujemo gresku
     * i prebacujemo na DLT-like topic (inventory.outbox-dlq) da ne blokiramo
     * obradu ostalih eventa.
     */
    private String topicFor(String eventType) {
        return switch (eventType) {
            case InventoryReservedEvent.EVENT_TYPE -> "inventory.reserved";
            case InventoryRejectedEvent.EVENT_TYPE -> "inventory.rejected";
            default -> {
                log.error("Nepoznat eventType '{}' u inventory_outbox, saljem na inventory.outbox-dlq", eventType);
                yield "inventory.outbox-dlq";
            }
        };
    }

    @Scheduled(fixedDelayString = "${inventory.outbox.relay.poll-interval-ms:500}")
    @Transactional
    public void publishPending() {
        List<InventoryOutboxEvent> batch = outboxRepository.lockUnpublishedBatch(batchSize);
        if (batch.isEmpty()) {
            return;
        }

        log.debug("Inventory relay uzeo {} neobjavljenih eventa", batch.size());

        for (InventoryOutboxEvent event : batch) {
            String topic = topicFor(event.getEventType());
            kafkaTemplate.send(topic, event.getAggregateId(), event.getPayload());
            log.info("Objavljen event {} (order {}) na topic {}",
                    event.getEventType(), event.getAggregateId(), topic);
        }

        kafkaTemplate.flush();

        List<UUID> ids = batch.stream().map(InventoryOutboxEvent::getId).toList();
        outboxRepository.markPublished(ids, Instant.now());
        log.debug("Markirano {} eventa kao objavljeno", ids.size());
    }
}
