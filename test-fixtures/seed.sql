-- seed.sql — Functional test database for pg_sage deep testing
-- Creates tables, indexes, sequences, and data patterns that exercise
-- every Tier 1 rule category in the analyzer.

BEGIN;

-- 1. Enable pg_stat_statements
CREATE EXTENSION IF NOT EXISTS pg_stat_statements;

-- 2. Customers table (10k rows)
CREATE TABLE IF NOT EXISTS customers (
    id    SERIAL PRIMARY KEY,
    name  TEXT NOT NULL,
    email TEXT UNIQUE,
    tier  TEXT DEFAULT 'free'
);

INSERT INTO customers (name, email, tier)
SELECT
    'customer_' || g,
    'customer_' || g || '@example.com',
    (ARRAY['free','pro','enterprise'])[1 + (g % 3)]
FROM generate_series(1, 10000) AS g;

-- 3. Products table (500 rows)
CREATE TABLE IF NOT EXISTS products (
    id       SERIAL PRIMARY KEY,
    name     TEXT,
    category TEXT,
    price    NUMERIC(10, 2)
);

INSERT INTO products (name, category, price)
SELECT
    'product_' || g,
    (ARRAY['electronics','books','clothing','food'])[1 + (g % 4)],
    round((random() * 500)::numeric, 2)
FROM generate_series(1, 500) AS g;

-- 4. Orders table (500k rows)
CREATE TABLE IF NOT EXISTS orders (
    id          SERIAL PRIMARY KEY,
    customer_id INT NOT NULL,
    product_id  INT NOT NULL,
    quantity    INT DEFAULT 1,
    total       NUMERIC(10, 2),
    status      TEXT DEFAULT 'pending',
    region      TEXT,
    created_at  TIMESTAMPTZ DEFAULT now(),
    updated_at  TIMESTAMPTZ DEFAULT now()
);

INSERT INTO orders (customer_id, product_id, quantity, total, status, region, created_at, updated_at)
SELECT
    1 + (random() * 9999)::int,
    1 + (random() * 499)::int,
    1 + (random() * 9)::int,
    round((random() * 1000)::numeric, 2),
    (ARRAY['pending','shipped','delivered','cancelled'])[1 + (random() * 3)::int],
    (ARRAY['us-east','us-west','eu-west','ap-south'])[1 + (random() * 3)::int],
    now() - (random() * interval '365 days'),
    now() - (random() * interval '30 days')
FROM generate_series(1, 500000);

-- 5. FK constraint — deliberately NO index on orders.customer_id
ALTER TABLE orders
    ADD CONSTRAINT fk_orders_customer
    FOREIGN KEY (customer_id) REFERENCES customers(id);

-- 6. Duplicate indexes on orders(status)
CREATE INDEX idx_orders_status     ON orders(status);
CREATE INDEX idx_orders_status_dup ON orders(status);

-- 7. Unused index on orders(region) — never queried
CREATE INDEX idx_orders_region ON orders(region);

-- 8. bloat_target table (100k rows)
CREATE TABLE IF NOT EXISTS bloat_target (
    id      SERIAL PRIMARY KEY,
    payload TEXT DEFAULT repeat('x', 200)
);

INSERT INTO bloat_target (payload)
SELECT repeat('x', 200)
FROM generate_series(1, 100000);

-- Disable autovacuum so dead tuples accumulate for bloat detection.
ALTER TABLE bloat_target SET (autovacuum_enabled = false);

-- 9. Sequence near exhaustion
CREATE SEQUENCE almost_done_seq MAXVALUE 1000 START 950 NO CYCLE;
-- Advance to start value
SELECT setval('almost_done_seq', 950);

-- 10. order_summary view
CREATE OR REPLACE VIEW order_summary AS
SELECT
    c.id          AS customer_id,
    c.name        AS customer_name,
    c.tier,
    count(o.id)   AS order_count,
    sum(o.total)  AS total_spent,
    max(o.created_at) AS last_order
FROM customers c
JOIN orders o ON o.customer_id = c.id
GROUP BY c.id, c.name, c.tier;

-- ============================================================
-- 13. Additional tables for deep testing
-- ============================================================

-- 13a. write_heavy_table (10k rows) — write-heavy ratio testing
CREATE TABLE IF NOT EXISTS write_heavy_table (
    id         SERIAL PRIMARY KEY,
    data       TEXT,
    updated_at TIMESTAMPTZ DEFAULT now()
);

INSERT INTO write_heavy_table (data, updated_at)
SELECT
    'payload_' || g,
    now() - (random() * interval '30 days')
FROM generate_series(1, 10000) AS g;

-- 13b. partitioned_orders — range-partitioned by created_at (3 monthly partitions)
CREATE TABLE IF NOT EXISTS partitioned_orders (
    id          SERIAL,
    customer_id INT NOT NULL,
    product_id  INT NOT NULL,
    quantity    INT DEFAULT 1,
    total       NUMERIC(10, 2),
    status      TEXT DEFAULT 'pending',
    region      TEXT,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at  TIMESTAMPTZ DEFAULT now()
) PARTITION BY RANGE (created_at);

CREATE TABLE partitioned_orders_2026_01 PARTITION OF partitioned_orders
    FOR VALUES FROM ('2026-01-01') TO ('2026-02-01');
CREATE TABLE partitioned_orders_2026_02 PARTITION OF partitioned_orders
    FOR VALUES FROM ('2026-02-01') TO ('2026-03-01');
CREATE TABLE partitioned_orders_2026_03 PARTITION OF partitioned_orders
    FOR VALUES FROM ('2026-03-01') TO ('2026-04-01');

INSERT INTO partitioned_orders (customer_id, product_id, quantity, total, status, region, created_at, updated_at)
SELECT
    1 + (random() * 9999)::int,
    1 + (random() * 499)::int,
    1 + (random() * 9)::int,
    round((random() * 1000)::numeric, 2),
    (ARRAY['pending','shipped','delivered','cancelled'])[1 + (random() * 3)::int],
    (ARRAY['us-east','us-west','eu-west','ap-south'])[1 + (random() * 3)::int],
    '2026-01-01'::timestamptz + (random() * interval '89 days'),
    now()
FROM generate_series(1, 30000);

-- 13c. multi_index_table — 10 indexes on various column combos
CREATE TABLE IF NOT EXISTS multi_index_table (
    id SERIAL PRIMARY KEY,
    a  INT,
    b  INT,
    c  INT,
    d  INT
);

INSERT INTO multi_index_table (a, b, c, d)
SELECT
    (random() * 1000)::int,
    (random() * 1000)::int,
    (random() * 1000)::int,
    (random() * 1000)::int
FROM generate_series(1, 10000);

CREATE INDEX idx_mi_a      ON multi_index_table(a);
CREATE INDEX idx_mi_b      ON multi_index_table(b);
CREATE INDEX idx_mi_c      ON multi_index_table(c);
CREATE INDEX idx_mi_d      ON multi_index_table(d);
CREATE INDEX idx_mi_ab     ON multi_index_table(a, b);
CREATE INDEX idx_mi_ac     ON multi_index_table(a, c);
CREATE INDEX idx_mi_bc     ON multi_index_table(b, c);
CREATE INDEX idx_mi_abc    ON multi_index_table(a, b, c);
CREATE INDEX idx_mi_bcd    ON multi_index_table(b, c, d);
CREATE INDEX idx_mi_abcd   ON multi_index_table(a, b, c, d);

-- 13d. expression_test — expression index validation
CREATE TABLE IF NOT EXISTS expression_test (
    id   SERIAL PRIMARY KEY,
    name TEXT,
    data JSONB
);

INSERT INTO expression_test (name, data)
SELECT
    'item_' || g,
    jsonb_build_object(
        'category', (ARRAY['electronics','books','clothing','food'])[1 + (g % 4)],
        'score', (random() * 100)::int,
        'tags', jsonb_build_array('tag_' || (g % 10), 'tag_' || (g % 5))
    )
FROM generate_series(1, 5000) AS g;

CREATE INDEX idx_expr_lower_name ON expression_test(lower(name));
CREATE INDEX idx_expr_jsonb_category ON expression_test((data->>'category'));

COMMIT;

-- 12. ANALYZE all tables (must be outside transaction)
ANALYZE customers;
ANALYZE products;
ANALYZE orders;
ANALYZE bloat_target;
ANALYZE write_heavy_table;
ANALYZE partitioned_orders;
ANALYZE multi_index_table;
ANALYZE expression_test;
