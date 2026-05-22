package com.example.inventory.domain;

import com.example.common.event.InventoryRejectedEvent;
import com.example.common.event.InventoryReservedEvent;
import com.example.common.event.OrderCreatedEvent;
import com.example.inventory.outbox.InventoryOutboxEvent;
import com.example.inventory.outbox.InventoryOutboxRepository;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.Optional;
import java.util.UUID;

/**
 * Rezervacija zaliha za jednu porudzbinu.
 *
 * Tri ishoda, svi u JEDNOJ transakciji:
 *  1. uspeh             -> skidanje zaliha + processed_event + InventoryReserved u outbox
 *  2. poslovna greska   -> processed_event + InventoryRejected u outbox (bez throw!)
 *                          klijent (Order API) sazna iz eventa, status -> REJECTED
 *  3. tehnicka greska   -> throw exception -> @RetryableTopic retry -> DLT
 *
 * KLJUC: poslovne greske se NE bacaju kao exception. Da smo bacili exception,
 * transakcija bi se vratila i outbox red nikad ne bi bio upisan. Onda Order
 * API nikad ne bi saznao za odbijanje. Zato eksplicitan event + return.
 */
@Slf4j
@Service
public class InventoryService {

    private final InventoryItemRepository inventoryRepository;
    private final ProcessedEventRepository processedEventRepository;
    private final InventoryOutboxRepository outboxRepository;
    private final ObjectMapper objectMapper;
    private final MeterRegistry meterRegistry;
    private final Counter reservedCounter;

    public InventoryService(InventoryItemRepository inventoryRepository,
                            ProcessedEventRepository processedEventRepository,
                            InventoryOutboxRepository outboxRepository,
                            ObjectMapper objectMapper,
                            MeterRegistry meterRegistry) {
        this.inventoryRepository = inventoryRepository;
        this.processedEventRepository = processedEventRepository;
        this.outboxRepository = outboxRepository;
        this.objectMapper = objectMapper;
        this.meterRegistry = meterRegistry;
        // counter bez tagova - same uspesne rezervacije
        this.reservedCounter = Counter.builder("inventory.reserved.count")
                .description("Broj uspesno rezervisanih porudzbina")
                .register(meterRegistry);
    }

    @Transactional
    public void reserve(OrderCreatedEvent event) {
        String eventId = event.eventId();

        // --- 1. idempotencija ---
        if (processedEventRepository.existsById(eventId)) {
            log.info("Event {} je vec obradjen, preskacem (idempotencija)", eventId);
            return;
        }

        String orderId = event.payload().orderId();
        log.debug("Rezervisem zalihe za order {}, event {}", orderId, eventId);

        // --- 2. validacija + provera zaliha (bez modifikacije baze) ---
        for (OrderCreatedEvent.OrderLine line : event.payload().items()) {
            Optional<InventoryItem> opt = inventoryRepository.findById(line.sku());

            if (opt.isEmpty()) {
                log.warn("Order {} odbijen: nepoznat SKU {}", orderId, line.sku());
                publishRejection(event, InventoryRejectedEvent.Reason.UNKNOWN_SKU,
                        "Nepoznat SKU: " + line.sku());
                processedEventRepository.save(new ProcessedEvent(eventId));
                return;
            }

            InventoryItem item = opt.get();
            if (!item.hasEnough(line.quantity())) {
                log.warn("Order {} odbijen: nedovoljno zaliha za {} (trazeno {}, dostupno {})",
                        orderId, line.sku(), line.quantity(), item.getAvailableQuantity());
                publishRejection(event, InventoryRejectedEvent.Reason.INSUFFICIENT_STOCK,
                        "Nedovoljno zaliha za %s: trazeno %d, dostupno %d"
                                .formatted(line.sku(), line.quantity(), item.getAvailableQuantity()));
                processedEventRepository.save(new ProcessedEvent(eventId));
                return;
            }
        }

        // --- 3. skidanje zaliha (sve stavke prosle validaciju) ---
        for (OrderCreatedEvent.OrderLine line : event.payload().items()) {
            InventoryItem item = inventoryRepository.findById(line.sku()).orElseThrow();
            item.reduce(line.quantity());
            inventoryRepository.save(item);
            log.info("Skinuto {} x {} za order {} (ostalo {})",
                    line.quantity(), line.sku(), orderId, item.getAvailableQuantity());
        }

        // --- 4. zabelezi event kao obradjen ---
        processedEventRepository.save(new ProcessedEvent(eventId));

        // --- 5. upisi InventoryReserved u outbox ---
        InventoryReservedEvent reserved = new InventoryReservedEvent(
                UUID.randomUUID().toString(),
                InventoryReservedEvent.EVENT_TYPE,
                InventoryReservedEvent.CURRENT_SCHEMA_VERSION,
                Instant.now(),
                orderId,
                new InventoryReservedEvent.Payload(orderId, eventId)
        );
        outboxRepository.save(new InventoryOutboxEvent(
                orderId,
                InventoryReservedEvent.EVENT_TYPE,
                toJson(reserved)
        ));

        log.info("Order {} uspesno rezervisan, InventoryReserved spreman za objavu", orderId);
        reservedCounter.increment();
    }

    private void publishRejection(OrderCreatedEvent original,
                                  InventoryRejectedEvent.Reason reason,
                                  String details) {
        String orderId = original.payload().orderId();
        InventoryRejectedEvent rejected = new InventoryRejectedEvent(
                UUID.randomUUID().toString(),
                InventoryRejectedEvent.EVENT_TYPE,
                InventoryRejectedEvent.CURRENT_SCHEMA_VERSION,
                Instant.now(),
                orderId,
                new InventoryRejectedEvent.Payload(orderId, original.eventId(), reason, details)
        );
        outboxRepository.save(new InventoryOutboxEvent(
                orderId,
                InventoryRejectedEvent.EVENT_TYPE,
                toJson(rejected)
        ));

        // metrika: counter sa tagom po razlogu -> Grafana moze da nacrta breakdown
        meterRegistry.counter("inventory.rejected.count", "reason", reason.name()).increment();
    }

    /**
     * Public ulaz za DLT handler. Kad poruka zavrsi na DLT (tehnicka greska,
     * iscrpljen retry...), klijent (Order API) i dalje mora da sazna ishod.
     * Objavi InventoryRejected sa razlogom SYSTEM_ERROR -> klijent vidi REJECTED.
     *
     * Razlika u odnosu na publishRejection: ovo NE zahteva da imamo originalan
     * OrderCreatedEvent (mozda deserialization nije ni uspeo). Samo orderId.
     */
    @Transactional
    public void publishSystemError(String orderId, String originatingEventId, String details) {
        log.warn("DLT handler za order {} - objavi SYSTEM_ERROR", orderId);

        InventoryRejectedEvent rejected = new InventoryRejectedEvent(
                UUID.randomUUID().toString(),
                InventoryRejectedEvent.EVENT_TYPE,
                InventoryRejectedEvent.CURRENT_SCHEMA_VERSION,
                Instant.now(),
                orderId,
                new InventoryRejectedEvent.Payload(orderId, originatingEventId,
                        InventoryRejectedEvent.Reason.SYSTEM_ERROR, details)
        );
        outboxRepository.save(new InventoryOutboxEvent(
                orderId,
                InventoryRejectedEvent.EVENT_TYPE,
                toJson(rejected)
        ));

        meterRegistry.counter("inventory.rejected.count",
                "reason", InventoryRejectedEvent.Reason.SYSTEM_ERROR.name()).increment();
    }

    private String toJson(Object event) {
        try {
            return objectMapper.writeValueAsString(event);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Serijalizacija eventa nije uspela", e);
        }
    }
}
