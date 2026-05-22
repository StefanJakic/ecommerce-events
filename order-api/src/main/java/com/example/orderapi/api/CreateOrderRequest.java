package com.example.orderapi.api;

import jakarta.validation.Valid;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;

import java.util.List;

public record CreateOrderRequest(
        @NotBlank(message = "customerId je obavezan")
        String customerId,

        @NotEmpty(message = "porudzbina mora imati bar jednu stavku")
        @Valid
        List<Item> items
) {
    public record Item(
            @NotBlank(message = "sku je obavezan")
            String sku,

            @Min(value = 1, message = "kolicina mora biti bar 1")
            int quantity
    ) {}
}
