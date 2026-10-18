-- Mini Order Processing Platform: database schema
-- Safe to run more than once (every statement is idempotent).

CREATE TABLE IF NOT EXISTS orders (
    id              SERIAL PRIMARY KEY,
    customer_name   VARCHAR(100) NOT NULL,
    item            VARCHAR(100) NOT NULL,
    quantity        INTEGER      NOT NULL CHECK (quantity > 0),
    status          VARCHAR(20)  NOT NULL DEFAULT 'PENDING'
                    CHECK (status IN ('PENDING', 'PROCESSING', 'COMPLETED', 'RETRYING', 'FAILED')),
    attempts        INTEGER      NOT NULL DEFAULT 0,
    failure_reason  TEXT,
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_orders_status     ON orders (status);
CREATE INDEX IF NOT EXISTS idx_orders_created_at ON orders (created_at DESC);
