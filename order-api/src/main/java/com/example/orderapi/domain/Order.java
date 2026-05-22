package com.example.orderapi.domain;

import jakarta.persistence.*;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

@Entity
@Table(name = "orders")
@Getter
@Setter
@NoArgsConstructor
public class Order {

    @Id
    private String id;

    @Column(nullable = false)
    private String customerId;

    @Column(nullable = false)
    private String status;

    @Column
    private String rejectionReason;

    @Column(nullable = false)
    private Instant createdAt;

    @OneToMany(mappedBy = "order", cascade = CascadeType.ALL, orphanRemoval = true)
    private List<OrderItem> items = new ArrayList<>();

    public Order(String id, String customerId) {
        this.id = id;
        this.customerId = customerId;
        this.status = "CREATED";
        this.createdAt = Instant.now();
    }

    public void addItem(String sku, int quantity) {
        OrderItem item = new OrderItem(this, sku, quantity);
        this.items.add(item);
    }
}
