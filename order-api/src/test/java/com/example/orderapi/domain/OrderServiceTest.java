package com.example.orderapi.domain;

import com.example.orderapi.api.CreateOrderRequest;
import com.example.orderapi.outbox.OutboxEvent;
import com.example.orderapi.outbox.OutboxEventRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
class OrderServiceTest {

    @Mock OrderRepository orderRepo;
    @Mock OutboxEventRepository outboxRepo;

    // ObjectMapper konfigurisan ISTO kao u produkciji (application.yml ima
    // write-dates-as-timestamps: false), da bi test verno proveravao izlaz.
    private final ObjectMapper objectMapper = new ObjectMapper()
            .registerModule(new JavaTimeModule())
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

    private OrderService service;

    @BeforeEach
    void setUp() {
        service = new OrderService(orderRepo, outboxRepo, objectMapper);
    }

    @Test
    @DisplayName("createOrder upisuje I order I outbox red - oba u istoj sesiji")
    void writesBothOrderAndOutbox() {
        CreateOrderRequest req = new CreateOrderRequest("CUST-1",
                List.of(new CreateOrderRequest.Item("SKU-001", 2)));

        Order order = service.createOrder(req);

        // 1. order entitet upisan
        verify(orderRepo).save(order);
        assertThat(order.getCustomerId()).isEqualTo("CUST-1");
        assertThat(order.getStatus()).isEqualTo("CREATED");
        assertThat(order.getItems()).hasSize(1);
        assertThat(order.getItems().get(0).getSku()).isEqualTo("SKU-001");
        assertThat(order.getItems().get(0).getQuantity()).isEqualTo(2);

        // 2. outbox red upisan SA istim aggregateId-jem
        ArgumentCaptor<OutboxEvent> outboxCaptor = ArgumentCaptor.forClass(OutboxEvent.class);
        verify(outboxRepo).save(outboxCaptor.capture());
        OutboxEvent outboxEvent = outboxCaptor.getValue();
        assertThat(outboxEvent.getAggregateId()).isEqualTo(order.getId());
        assertThat(outboxEvent.getEventType()).isEqualTo("OrderCreated");
        assertThat(outboxEvent.getPublishedAt()).isNull();  // jos nije objavljeno
    }

    @Test
    @DisplayName("outbox payload sadrzi JSON sa svim podacima ordera")
    void outboxPayloadHasFullOrderData() {
        CreateOrderRequest req = new CreateOrderRequest("CUST-X",
                List.of(
                        new CreateOrderRequest.Item("SKU-A", 1),
                        new CreateOrderRequest.Item("SKU-B", 5)
                ));

        Order order = service.createOrder(req);

        ArgumentCaptor<OutboxEvent> captor = ArgumentCaptor.forClass(OutboxEvent.class);
        verify(outboxRepo).save(captor.capture());
        String payload = captor.getValue().getPayload();

        assertThat(payload)
                .contains("CUST-X")
                .contains("SKU-A")
                .contains("SKU-B")
                .contains(order.getId())
                .contains("OrderCreated")
                // Instant kao ISO string, ne timestamp
                .containsPattern("\"occurredAt\":\"\\d{4}-\\d{2}-\\d{2}T");
    }

    @Test
    @DisplayName("svaki poziv createOrder generise jedinstven orderId")
    void generatesUniqueOrderIds() {
        CreateOrderRequest req = new CreateOrderRequest("CUST-1",
                List.of(new CreateOrderRequest.Item("SKU-001", 1)));

        Order first = service.createOrder(req);
        Order second = service.createOrder(req);

        assertThat(first.getId()).isNotEqualTo(second.getId());
        assertThat(first.getId()).startsWith("ORDER-");
        assertThat(second.getId()).startsWith("ORDER-");
    }
}
