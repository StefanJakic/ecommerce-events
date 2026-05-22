-- ============================================================
--  Sema za Inventory Service
-- ============================================================

CREATE TABLE IF NOT EXISTS inventory_item (
    sku                VARCHAR(64) PRIMARY KEY,
    available_quantity INTEGER     NOT NULL,
    version            BIGINT      NOT NULL DEFAULT 0
);

-- Idempotencija: jedan red po obradjenom eventId
CREATE TABLE IF NOT EXISTS processed_event (
    event_id     VARCHAR(64) PRIMARY KEY,
    processed_at TIMESTAMP   NOT NULL
);

-- Inventory outbox: InventoryReserved evente koje treba objaviti.
-- Isti pattern kao outbox u Order API-ju.
CREATE TABLE IF NOT EXISTS inventory_outbox (
    id           UUID PRIMARY KEY,
    aggregate_id VARCHAR(64)  NOT NULL,
    event_type   VARCHAR(64)  NOT NULL,
    payload      TEXT         NOT NULL,
    created_at   TIMESTAMP    NOT NULL,
    published_at TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_inventory_outbox_unpublished
    ON inventory_outbox (created_at)
    WHERE published_at IS NULL;

-- Pocetne zalihe za demo. ON CONFLICT DO NOTHING -> ne gazi
-- postojece kolicine pri restartu servisa.
INSERT INTO inventory_item (sku, available_quantity, version) VALUES
    ('SKU-001', 100, 0),
    ('SKU-002', 50,  0),
    ('SKU-042', 10,  0)
ON CONFLICT (sku) DO NOTHING;
