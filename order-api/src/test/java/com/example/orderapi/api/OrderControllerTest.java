package com.example.orderapi.api;

import com.example.orderapi.domain.Order;
import com.example.orderapi.domain.OrderService;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

/**
 * Slice test - samo MVC sloj, brz (nema baze, nema Kafke).
 * Testira HTTP validaciju i mapiranje request -> response.
 */
@WebMvcTest(OrderController.class)
class OrderControllerTest {

    @Autowired MockMvc mockMvc;
    @Autowired ObjectMapper objectMapper;

    @MockBean OrderService orderService;

    @Test
    @DisplayName("validan zahtev -> 201 + JSON sa orderId i status CREATED")
    void validRequest_returns201() throws Exception {
        Order mockOrder = new Order("ORDER-TEST123", "CUST-1");
        mockOrder.addItem("SKU-001", 2);
        when(orderService.createOrder(any())).thenReturn(mockOrder);

        String body = """
                {
                  "customerId": "CUST-1",
                  "items": [ { "sku": "SKU-001", "quantity": 2 } ]
                }
                """;

        mockMvc.perform(post("/orders")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(body))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.orderId").value("ORDER-TEST123"))
                .andExpect(jsonPath("$.customerId").value("CUST-1"))
                .andExpect(jsonPath("$.status").value("CREATED"))
                .andExpect(jsonPath("$.items[0].sku").value("SKU-001"));
    }

    @Test
    @DisplayName("prazan customerId -> 400 sa error porukom")
    void emptyCustomerId_returns400() throws Exception {
        String body = """
                {
                  "customerId": "",
                  "items": [ { "sku": "SKU-001", "quantity": 1 } ]
                }
                """;

        mockMvc.perform(post("/orders")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(body))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.errors.customerId").exists());
    }

    @Test
    @DisplayName("prazna items lista -> 400")
    void emptyItems_returns400() throws Exception {
        String body = """
                {
                  "customerId": "CUST-1",
                  "items": []
                }
                """;

        mockMvc.perform(post("/orders")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(body))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.errors.items").exists());
    }

    @Test
    @DisplayName("quantity 0 -> 400")
    void zeroQuantity_returns400() throws Exception {
        String body = """
                {
                  "customerId": "CUST-1",
                  "items": [ { "sku": "SKU-001", "quantity": 0 } ]
                }
                """;

        mockMvc.perform(post("/orders")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(body))
                .andExpect(status().isBadRequest());
    }

    @Test
    @DisplayName("malformiran JSON -> 400")
    void malformedJson_returns400() throws Exception {
        mockMvc.perform(post("/orders")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{ not json }"))
                .andExpect(status().isBadRequest());
    }
}
