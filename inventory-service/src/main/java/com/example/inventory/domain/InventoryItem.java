package com.example.inventory.domain;

import jakarta.persistence.*;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/**
 * Zaliha jednog artikla (SKU).
 *
 * @Version polje: Hibernate ga automatski uvecava pri svakom update-u
 * i dodaje "AND version = ?" u WHERE. Ako je neko drugi u medjuvremenu
 * promenio red, update pogodi 0 redova -> OptimisticLockException.
 * Nema zakljucavanja, nema cekanja - konzumenti se ne otimaju oko reda.
 */
@Entity
@Table(name = "inventory_item")
@Getter
@Setter
@NoArgsConstructor
public class InventoryItem {

    @Id
    private String sku;

    @Column(nullable = false)
    private int availableQuantity;

    @Version
    private long version;

    public InventoryItem(String sku, int availableQuantity) {
        this.sku = sku;
        this.availableQuantity = availableQuantity;
    }

    public boolean hasEnough(int requested) {
        return availableQuantity >= requested;
    }

    public void reduce(int quantity) {
        this.availableQuantity -= quantity;
    }
}
