package com.example.inventory.domain;

import com.example.common.event.OrderCreatedEvent;
import com.example.inventory.outbox.InventoryOutboxEvent;
import com.example.inventory.outbox.InventoryOutboxRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Instant;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class InventoryServiceTest {

    @Mock InventoryItemRepository inventoryRepo;
    @Mock ProcessedEventRepository processedEventRepo;
    @Mock InventoryOutboxRepository outboxRepo;

    private final ObjectMapper objectMapper = new ObjectMapper()
            .registerModule(new JavaTimeModule())
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

    private InventoryService service;

    @BeforeEach
    void setUp() {
        service = new InventoryService(inventoryRepo, processedEventRepo, outboxRepo, objectMapper, new SimpleMeterRegistry());
        lenient().when(processedEventRepo.existsById(any())).thenReturn(false);
    }

    private OrderCreatedEvent eventWith(String sku, int quantity) {
        return new OrderCreatedEvent(
                "evt-1", "OrderCreated", 1, Instant.now(), "ORDER-1",
                new OrderCreatedEvent.OrderPayload(
                        "ORDER-1", "CUST-1",
                        List.of(new OrderCreatedEvent.OrderLine(sku, quantity))
                )
        );
    }

    @Test
    @DisplayName("uspeh: skida zalihe i upisuje InventoryReserved u outbox")
    void happyPath() {
        InventoryItem item = new InventoryItem("SKU-001", 100);
        when(inventoryRepo.findById("SKU-001")).thenReturn(Optional.of(item));

        service.reserve(eventWith("SKU-001", 3));

        assertThat(item.getAvailableQuantity()).isEqualTo(97);
        verify(inventoryRepo).save(item);
        verify(processedEventRepo).save(any(ProcessedEvent.class));

        ArgumentCaptor<InventoryOutboxEvent> captor = ArgumentCaptor.forClass(InventoryOutboxEvent.class);
        verify(outboxRepo).save(captor.capture());
        InventoryOutboxEvent saved = captor.getValue();
        assertThat(saved.getEventType()).isEqualTo("InventoryReserved");
        assertThat(saved.getPayload()).contains("ORDER-1");
    }

    @Test
    @DisplayName("idempotencija: vec obradjen event se preskace, nista se ne upisuje")
    void idempotency() {
        when(processedEventRepo.existsById("evt-1")).thenReturn(true);

        service.reserve(eventWith("SKU-001", 3));

        verify(inventoryRepo, never()).findById(any());
        verify(inventoryRepo, never()).save(any());
        verify(processedEventRepo, never()).save(any());
        verify(outboxRepo, never()).save(any());
    }

    @Test
    @DisplayName("nepoznat SKU: NEMA exception, upisuje InventoryRejected sa razlogom UNKNOWN_SKU")
    void unknownSku_publishesRejection() {
        when(inventoryRepo.findById("SKU-X")).thenReturn(Optional.empty());

        // BITNO: ne bacamo exception, sve mora da prodje cisto
        service.reserve(eventWith("SKU-X", 1));

        // zalihe nisu pipnute
        verify(inventoryRepo, never()).save(any());

        // outbox red sa rejection-om mora biti tu
        ArgumentCaptor<InventoryOutboxEvent> captor = ArgumentCaptor.forClass(InventoryOutboxEvent.class);
        verify(outboxRepo).save(captor.capture());
        InventoryOutboxEvent saved = captor.getValue();
        assertThat(saved.getEventType()).isEqualTo("InventoryRejected");
        assertThat(saved.getPayload()).contains("UNKNOWN_SKU");
        assertThat(saved.getPayload()).contains("SKU-X");

        // i processed_event je upisan - rejection je takodje "obrada"
        verify(processedEventRepo).save(any(ProcessedEvent.class));
    }

    @Test
    @DisplayName("nedovoljno zaliha: NEMA exception, upisuje InventoryRejected sa razlogom INSUFFICIENT_STOCK")
    void insufficientStock_publishesRejection() {
        when(inventoryRepo.findById("SKU-001")).thenReturn(Optional.of(new InventoryItem("SKU-001", 2)));

        service.reserve(eventWith("SKU-001", 5));

        verify(inventoryRepo, never()).save(any());

        ArgumentCaptor<InventoryOutboxEvent> captor = ArgumentCaptor.forClass(InventoryOutboxEvent.class);
        verify(outboxRepo).save(captor.capture());
        InventoryOutboxEvent saved = captor.getValue();
        assertThat(saved.getEventType()).isEqualTo("InventoryRejected");
        assertThat(saved.getPayload()).contains("INSUFFICIENT_STOCK");
        assertThat(saved.getPayload()).contains("trazeno 5");
        assertThat(saved.getPayload()).contains("dostupno 2");

        verify(processedEventRepo).save(any(ProcessedEvent.class));
    }
}
