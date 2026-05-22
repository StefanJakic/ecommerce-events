package com.example.outboxrelay;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class OutboxRelayTest {

    @Mock OutboxRepository outboxRepository;
    @Mock KafkaTemplate<String, String> kafkaTemplate;

    @InjectMocks OutboxRelay relay;

    private OutboxEvent newEvent(String orderId, String payload) {
        OutboxEvent e = new OutboxEvent();
        e.setId(UUID.randomUUID());
        e.setAggregateId(orderId);
        e.setEventType("OrderCreated");
        e.setPayload(payload);
        e.setCreatedAt(Instant.now());
        return e;
    }

    @BeforeEach
    void injectBatchSize() throws Exception {
        // batchSize je @Value polje, nije injektovano u unit testu - postavi rucno
        var f = OutboxRelay.class.getDeclaredField("batchSize");
        f.setAccessible(true);
        f.setInt(relay, 100);
    }

    @Test
    @DisplayName("salje sve evente iz batch-a na orders.created topic")
    void publishesToCorrectTopic() {
        OutboxEvent e1 = newEvent("ORDER-1", "{\"order\":1}");
        OutboxEvent e2 = newEvent("ORDER-2", "{\"order\":2}");
        when(outboxRepository.lockUnpublishedBatch(100)).thenReturn(List.of(e1, e2));

        relay.publishPending();

        // svaki event poslat sa orderId kao key, payload kao vrednost
        verify(kafkaTemplate).send("orders.created", "ORDER-1", "{\"order\":1}");
        verify(kafkaTemplate).send("orders.created", "ORDER-2", "{\"order\":2}");
    }

    @Test
    @DisplayName("posle send-a poziva flush, pa onda markPublished")
    void flushesBeforeMarking() {
        OutboxEvent e = newEvent("ORDER-1", "{\"data\":1}");
        when(outboxRepository.lockUnpublishedBatch(100)).thenReturn(List.of(e));

        relay.publishPending();

        // redosled: send -> flush -> markPublished
        var inOrder = inOrder(kafkaTemplate, outboxRepository);
        inOrder.verify(kafkaTemplate).send(any(), any(), any());
        inOrder.verify(kafkaTemplate).flush();
        inOrder.verify(outboxRepository).markPublished(any(), any());
    }

    @Test
    @DisplayName("markPublished prima tacne ID-eve iz batch-a")
    void marksCorrectIds() {
        OutboxEvent e1 = newEvent("ORDER-1", "{}");
        OutboxEvent e2 = newEvent("ORDER-2", "{}");
        when(outboxRepository.lockUnpublishedBatch(100)).thenReturn(List.of(e1, e2));

        relay.publishPending();

        ArgumentCaptor<List<UUID>> idsCaptor = ArgumentCaptor.forClass(List.class);
        verify(outboxRepository).markPublished(idsCaptor.capture(), any(Instant.class));

        assertThat(idsCaptor.getValue()).containsExactly(e1.getId(), e2.getId());
    }

    @Test
    @DisplayName("prazan batch: ne salje nista, ne markira nista")
    void emptyBatchDoesNothing() {
        when(outboxRepository.lockUnpublishedBatch(100)).thenReturn(List.of());

        relay.publishPending();

        verify(kafkaTemplate, never()).send(any(), any(), any());
        verify(kafkaTemplate, never()).flush();
        verify(outboxRepository, never()).markPublished(any(), any());
    }
}
