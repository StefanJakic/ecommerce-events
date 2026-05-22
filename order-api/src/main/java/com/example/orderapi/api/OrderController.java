package com.example.orderapi.api;

import com.example.common.logging.LoggingContext;
import com.example.orderapi.domain.Order;
import com.example.orderapi.domain.OrderService;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@Slf4j
@RestController
@RequestMapping("/orders")
@RequiredArgsConstructor
public class OrderController {

    private final OrderService orderService;

    @PostMapping
    public ResponseEntity<OrderResponse> create(@Valid @RequestBody CreateOrderRequest request) {
        log.debug("Primljen zahtev za kreiranje porudzbine, customer={}", request.customerId());
        Order order = orderService.createOrder(request);
        LoggingContext.setOrderContext(order.getId(), null, "OrderCreated");
        LoggingContext.setStatus(order.getStatus());
        try {
            log.info("Order {} kreiran", order.getId());
            return ResponseEntity
                    .status(HttpStatus.CREATED)
                    .body(OrderResponse.from(order));
        } finally {
            LoggingContext.clear();
        }
    }
}

