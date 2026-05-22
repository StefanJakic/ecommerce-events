-- ============================================================
--  Sema za Order API
--  ddl-auto=none, ovaj fajl je jedini izvor istine za semu
-- ============================================================

CREATE TABLE IF NOT EXISTS orders (
    id               VARCHAR(64) PRIMARY KEY,
    customer_id      VARCHAR(64) NOT NULL,
    status           VARCHAR(32) NOT NULL,
    rejection_reason VARCHAR(64),
    created_at       TIMESTAMP   NOT NULL
);

CREATE TABLE IF NOT EXISTS order_items (
    id       BIGSERIAL PRIMARY KEY,
    order_id VARCHAR(64) NOT NULL REFERENCES orders(id),
    sku      VARCHAR(64) NOT NULL,
    quantity INTEGER     NOT NULL
);

-- Outbox: event red se upisuje u istoj transakciji sa orderom.
-- published_at IS NULL znaci "jos nije objavljeno na Kafku".
CREATE TABLE IF NOT EXISTS outbox (
    id           UUID PRIMARY KEY,
    aggregate_id VARCHAR(64)  NOT NULL,
    event_type   VARCHAR(64)  NOT NULL,
    payload      TEXT         NOT NULL,
    created_at   TIMESTAMP    NOT NULL,
    published_at TIMESTAMP
);

-- Relay filtrira po published_at IS NULL i sortira po created_at;
-- delimicni indeks drzi samo neobjavljene redove -> brz poll.
CREATE INDEX IF NOT EXISTS idx_outbox_unpublished
    ON outbox (created_at)
    WHERE published_at IS NULL;
