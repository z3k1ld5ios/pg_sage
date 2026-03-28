-- seed_objects.sql — Seed a PostgreSQL database with objects that trigger every pg_sage analyzer rule.
-- Safe for managed PostgreSQL (Cloud SQL, RDS, Aurora, Azure). No superuser or extensions required.
-- Target: PostgreSQL 14+. Idempotent via IF NOT EXISTS / ON CONFLICT.

BEGIN;

------------------------------------------------------------
-- SCHEMAS (#24 multiple schemas — sage schema filter test)
------------------------------------------------------------
CREATE SCHEMA IF NOT EXISTS app;
CREATE SCHEMA IF NOT EXISTS archive;

------------------------------------------------------------
-- SEQUENCES (#13 sequence_exhaustion — near int4 max)
------------------------------------------------------------
DROP SEQUENCE IF EXISTS app.nearly_exhausted_seq;
CREATE SEQUENCE app.nearly_exhausted_seq
    AS integer START WITH 2147483640 INCREMENT BY 1 NO CYCLE;

------------------------------------------------------------
-- TABLES — core data model with FK chains (#25 foreign key chains)
------------------------------------------------------------
CREATE TABLE IF NOT EXISTS app.regions (
    id serial PRIMARY KEY,
    name text NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS app.customers (
    id serial PRIMARY KEY,
    region_id integer NOT NULL REFERENCES app.regions(id),
    email text NOT NULL,
    name text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now()
);
-- #1 missing_fk_index — region_id has FK but NO index (deliberately omitted)

CREATE TABLE IF NOT EXISTS app.products (
    id serial PRIMARY KEY,
    sku text NOT NULL UNIQUE,
    name text NOT NULL,
    price numeric(10,2) NOT NULL DEFAULT 0,
    category text NOT NULL DEFAULT 'general'
);

-- #20 partitioned table — range on order_date
CREATE TABLE IF NOT EXISTS app.orders (
    id bigserial,
    customer_id integer NOT NULL,
    order_date date NOT NULL,
    total numeric(12,2) NOT NULL DEFAULT 0,
    status text NOT NULL DEFAULT 'pending',
    note text,
    PRIMARY KEY (id, order_date)
) PARTITION BY RANGE (order_date);

CREATE TABLE IF NOT EXISTS app.orders_2024 PARTITION OF app.orders
    FOR VALUES FROM ('2024-01-01') TO ('2025-01-01');
CREATE TABLE IF NOT EXISTS app.orders_2025 PARTITION OF app.orders
    FOR VALUES FROM ('2025-01-01') TO ('2026-01-01');
CREATE TABLE IF NOT EXISTS app.orders_2026 PARTITION OF app.orders
    FOR VALUES FROM ('2026-01-01') TO ('2027-01-01');

-- FK on orders.customer_id added after data load (partition FK requires PG14+)
-- #25 multi-level FK chain: regions -> customers -> orders -> order_items

CREATE TABLE IF NOT EXISTS app.order_items (
    id bigserial PRIMARY KEY,
    order_id bigint NOT NULL,
    order_date date NOT NULL,
    product_id integer NOT NULL REFERENCES app.products(id),
    qty integer NOT NULL DEFAULT 1,
    unit_price numeric(10,2) NOT NULL,
    FOREIGN KEY (order_id, order_date) REFERENCES app.orders(id, order_date)
);
-- #1 missing_fk_index — product_id and (order_id, order_date) have no indexes

------------------------------------------------------------
-- #21 unlogged table — WAL advisor filtering
------------------------------------------------------------
CREATE UNLOGGED TABLE IF NOT EXISTS app.session_cache (
    sid text PRIMARY KEY,
    data jsonb NOT NULL DEFAULT '{}',
    expires_at timestamptz NOT NULL
);

------------------------------------------------------------
-- #22 bloated table (#5 table_bloat) — UPDATE/DELETE without VACUUM
------------------------------------------------------------
CREATE TABLE IF NOT EXISTS app.audit_log (
    id bigserial PRIMARY KEY,
    action text NOT NULL,
    payload text,
    created_at timestamptz NOT NULL DEFAULT now()
);

------------------------------------------------------------
-- ARCHIVE SCHEMA — #24 cross-schema objects
------------------------------------------------------------
CREATE TABLE IF NOT EXISTS archive.old_orders (
    id bigint PRIMARY KEY,
    customer_id integer,
    order_date date,
    total numeric(12,2)
);

------------------------------------------------------------
-- BULK DATA — generate_series
------------------------------------------------------------

-- Regions (10 rows)
INSERT INTO app.regions (name)
SELECT 'region_' || g FROM generate_series(1, 10) g
ON CONFLICT (name) DO NOTHING;

-- Customers (~1000 rows)
INSERT INTO app.customers (region_id, email, name, created_at)
SELECT
    (g % 10) + 1,
    'user' || g || '@example.com',
    'Customer ' || g,
    '2024-01-01'::timestamptz + (g || ' minutes')::interval
FROM generate_series(1, 1000) g
ON CONFLICT DO NOTHING;

-- Products (200 rows)
INSERT INTO app.products (sku, name, price, category)
SELECT
    'SKU-' || lpad(g::text, 5, '0'),
    'Product ' || g,
    round((random() * 200 + 1)::numeric, 2),
    (ARRAY['electronics','clothing','food','tools','books'])[1 + (g % 5)]
FROM generate_series(1, 200) g
ON CONFLICT (sku) DO NOTHING;

-- Orders (~50K rows spread across partitions)
INSERT INTO app.orders (customer_id, order_date, total, status)
SELECT
    (g % 1000) + 1,
    '2024-01-01'::date + (g % 730),  -- spans 2024-2025
    round((random() * 500 + 5)::numeric, 2),
    (ARRAY['pending','shipped','delivered','cancelled'])[1 + (g % 4)]
FROM generate_series(1, 50000) g
ON CONFLICT DO NOTHING;

-- Add FK constraint on orders after data load (safe for partitioned tables PG14+)
DO $$ BEGIN
    ALTER TABLE app.orders
        ADD CONSTRAINT orders_customer_id_fk FOREIGN KEY (customer_id)
        REFERENCES app.customers(id);
EXCEPTION WHEN duplicate_object THEN NULL;
END $$;

-- Order items (~150K rows)
INSERT INTO app.order_items (order_id, order_date, product_id, qty, unit_price)
SELECT
    o.id,
    o.order_date,
    (g % 200) + 1,
    (g % 5) + 1,
    round((random() * 100 + 1)::numeric, 2)
FROM (
    SELECT id, order_date, row_number() OVER () AS rn FROM app.orders LIMIT 50000
) o
CROSS JOIN generate_series(1, 3) g
WHERE o.rn <= 50000
ON CONFLICT DO NOTHING;

------------------------------------------------------------
-- INDEXES — duplicates, composites, expressions
------------------------------------------------------------

-- #4 duplicate_indexes — exact duplicate
CREATE INDEX IF NOT EXISTS idx_customers_email     ON app.customers (email);
CREATE INDEX IF NOT EXISTS idx_customers_email_dup ON app.customers (email);

-- #4 duplicate_indexes — btree subset (a) is subset of (a, b)
CREATE INDEX IF NOT EXISTS idx_items_order_id       ON app.order_items (order_id);
CREATE INDEX IF NOT EXISTS idx_items_order_id_prod  ON app.order_items (order_id, product_id);

-- #2 unused_indexes — index that will never be scanned
CREATE INDEX IF NOT EXISTS idx_customers_unused ON app.customers (created_at, name, email);

-- #26 composite index — for column ordering detection
CREATE INDEX IF NOT EXISTS idx_orders_status_date ON app.orders (status, order_date);

-- #27 expression index — for expression volatility check
CREATE INDEX IF NOT EXISTS idx_products_lower_cat ON app.products (lower(category));

-- #19 seq_scan_watchdog — audit_log has NO indexes on action (large table, filtered)

------------------------------------------------------------
-- BLOAT GENERATION (#5 table_bloat, #22 bloated table)
------------------------------------------------------------
INSERT INTO app.audit_log (action, payload)
SELECT
    'event_' || (g % 10),
    repeat('x', 200)
FROM generate_series(1, 10000) g;

-- Churn rows to create dead tuples (bloat)
UPDATE app.audit_log SET payload = repeat('y', 200) WHERE id % 2 = 0;
DELETE FROM app.audit_log WHERE id % 3 = 0;
-- Deliberately NO VACUUM — leaves dead tuples for bloat detection

------------------------------------------------------------
-- SESSION CACHE data (unlogged table)
------------------------------------------------------------
INSERT INTO app.session_cache (sid, data, expires_at)
SELECT
    'sess_' || g,
    jsonb_build_object('user_id', g, 'ts', now()),
    now() + interval '1 hour'
FROM generate_series(1, 100) g
ON CONFLICT (sid) DO NOTHING;

------------------------------------------------------------
-- ARCHIVE data
------------------------------------------------------------
INSERT INTO archive.old_orders (id, customer_id, order_date, total)
SELECT g, (g % 500) + 1, '2023-01-01'::date + (g % 365), round((random() * 300)::numeric, 2)
FROM generate_series(1, 5000) g
ON CONFLICT (id) DO NOTHING;

COMMIT;

------------------------------------------------------------
-- QUERY PATTERNS — populate pg_stat_statements
-- Run outside transaction for statement-level stats
------------------------------------------------------------

-- #6 slow_queries — sequential scan on large table with expensive filter
SELECT count(*) FROM app.order_items oi
JOIN app.orders o ON o.id = oi.order_id AND o.order_date = oi.order_date
JOIN app.customers c ON c.id = o.customer_id
WHERE oi.unit_price > 50 AND c.name LIKE '%99%';

-- #7 high_plan_time — complex multi-join forcing planner work
SELECT c.name, r.name AS region, count(DISTINCT o.id) AS order_count,
       sum(oi.qty * oi.unit_price) AS total_spend
FROM app.customers c
JOIN app.regions r ON r.id = c.region_id
JOIN app.orders o ON o.customer_id = c.id
JOIN app.order_items oi ON oi.order_id = o.id AND oi.order_date = o.order_date
JOIN app.products p ON p.id = oi.product_id
WHERE o.order_date BETWEEN '2024-06-01' AND '2024-12-31'
  AND p.category = 'electronics'
GROUP BY c.name, r.name
ORDER BY total_spend DESC
LIMIT 20;

-- #12 high_total_time — lightweight query called many times
SELECT id, email FROM app.customers WHERE id = 1;
SELECT id, email FROM app.customers WHERE id = 2;
SELECT id, email FROM app.customers WHERE id = 3;
SELECT id, email FROM app.customers WHERE id = 4;
SELECT id, email FROM app.customers WHERE id = 5;
SELECT id, email FROM app.customers WHERE id = 6;
SELECT id, email FROM app.customers WHERE id = 7;
SELECT id, email FROM app.customers WHERE id = 8;
SELECT id, email FROM app.customers WHERE id = 9;
SELECT id, email FROM app.customers WHERE id = 10;

-- #17 sort_without_index — ORDER BY on unindexed column + LIMIT
SELECT * FROM app.order_items ORDER BY unit_price DESC LIMIT 100;

-- #19 seq_scan_watchdog — filter on unindexed column of large table
SELECT * FROM app.audit_log WHERE action = 'event_3' LIMIT 50;

-- #8 query_regression baseline — run a query to establish a baseline snapshot
SELECT o.status, count(*), avg(o.total)
FROM app.orders o
GROUP BY o.status;

-- Cross-schema query for schema filter testing
SELECT ao.id, ao.total FROM archive.old_orders ao WHERE ao.customer_id < 10;

------------------------------------------------------------
-- NOTES ON RULES THAT CANNOT BE SEEDED DIRECTLY
------------------------------------------------------------
-- #3  invalid_indexes     — requires pg_index.indisvalid=false; managed PG won't let
--                           you create one. Would need a failed concurrent build + crash.
-- #9  cache_hit_ratio     — system-wide metric from pg_stat_database; seed by running
--                           queries against cold cache (restart needed).
-- #10 checkpoint_pressure — system-wide from pg_stat_bgwriter; requires sustained writes
--                           exceeding checkpoint_completion_target.
-- #11 stat_statements_capacity — fires when pg_stat_statements rows approach max.
-- #14 replication_lag     — requires a streaming replica.
-- #15 inactive_slots      — requires CREATE replication SLOT (superuser on managed).
-- #16 connection_leak     — runtime detection via idle-in-transaction age.
-- #18 plan_regression     — detected via explain_cache snapshot diffs; run EXPLAIN on
--                           the baseline queries above, then change data distribution
--                           and compare.
