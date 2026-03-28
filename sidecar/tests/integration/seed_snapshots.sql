-- seed_snapshots.sql
-- Seeds 30 days of historical snapshot data into sage schema tables.
-- Runs AFTER sidecar bootstrap (tables already exist).
-- Idempotent: deletes seed-range data before inserting.

BEGIN;

-- Clear existing data in the seed date range.
DELETE FROM sage.snapshots
 WHERE collected_at >= now() - interval '31 days';

DELETE FROM sage.explain_cache
 WHERE captured_at >= now() - interval '31 days';

-- ============================================================
-- 1. System Snapshots (30 days, 1 per day)
--    category = 'system', data = SystemStats JSON object
-- ============================================================
INSERT INTO sage.snapshots (collected_at, category, data)
SELECT
    now() - interval '30 days' + (g * interval '1 day') + interval '6 hours',
    'system',
    jsonb_build_object(
        -- 100MB -> 500MB (~13.8MB/day) triggers forecast_disk_growth
        'db_size_bytes',       (104857600 + g * 13793103)::bigint,
        -- 20 -> 85 triggers forecast_connection_saturation
        'active_backends',     (10 + g * 1.5)::int,
        'total_backends',      (20 + g * 2.2)::int,
        'max_connections',     100,
        'idle_in_transaction', greatest(0, (g - 15))::int,
        -- 0.99 -> 0.88 triggers forecast_cache_pressure
        'cache_hit_ratio',     round((0.99 - g * 0.00379)::numeric, 4),
        -- increasing checkpoint frequency triggers forecast_checkpoint_pressure
        'total_checkpoints',   (500 + g * g * 2)::bigint,
        'deadlocks',           0::bigint,
        'blk_read_time',       round((120.0 + g * 15.0)::numeric, 2),
        'blk_write_time',      round((40.0 + g * 5.0)::numeric, 2),
        'is_replica',          false,
        'stat_statements_max', 5000
    )
FROM generate_series(0, 29) AS g;

-- ============================================================
-- 2. Query Snapshots (30 days, 1 per day)
--    category = 'queries', data = JSON array of QueryStats
-- ============================================================
INSERT INTO sage.snapshots (collected_at, category, data)
SELECT
    now() - interval '30 days' + (g * interval '1 day') + interval '6 hours',
    'queries',
    jsonb_build_array(
        -- Q1: regression query, mean_exec_time 200ms -> 800ms
        jsonb_build_object(
            'queryid',         -7216849732456821,
            'query',           'SELECT o.*, c.name FROM orders o JOIN customers c ON c.id = o.customer_id WHERE o.status = $1 AND o.created_at > $2',
            'calls',           (1200 + g * 30)::bigint,
            'mean_exec_time',  round((200.0 + g * 20.69)::numeric, 2),
            'total_exec_time', round(((200.0 + g * 20.69) * (1200 + g * 30))::numeric, 2),
            'min_exec_time',   round((50.0 + g * 5.0)::numeric, 2),
            'max_exec_time',   round((800.0 + g * 40.0)::numeric, 2),
            'stddev_exec_time', round((45.0 + g * 3.0)::numeric, 2),
            'rows',            (500 + g * 20)::bigint,
            'shared_blks_hit', (80000 + g * 1000)::bigint,
            'shared_blks_read',(2000 + g * 500)::bigint,
            'shared_blks_dirtied', 0::bigint,
            'shared_blks_written', 0::bigint,
            'temp_blks_read',  0::bigint,
            'temp_blks_written', 0::bigint,
            'blk_read_time',   0.0,
            'blk_write_time',  0.0,
            'total_plan_time', round(((2.0 + g * 0.1) * (1200 + g * 30))::numeric, 2),
            'mean_plan_time',  round((2.0 + g * 0.1)::numeric, 2)
        ),
        -- Q2: high-volume query, 5000 calls/day, moderate exec time
        jsonb_build_object(
            'queryid',         -1498230517643982,
            'query',           'SELECT id, email, status FROM users WHERE tenant_id = $1 AND active = true LIMIT $2',
            'calls',           (5000 + g * 50)::bigint,
            'mean_exec_time',  round((50.0 + (random() * 10 - 5))::numeric, 2),
            'total_exec_time', round((50.0 * (5000 + g * 50))::numeric, 2),
            'min_exec_time',   5.0,
            'max_exec_time',   200.0,
            'stddev_exec_time', 15.0,
            'rows',            (200 + g * 5)::bigint,
            'shared_blks_hit', (150000 + g * 2000)::bigint,
            'shared_blks_read',(500 + g * 10)::bigint,
            'shared_blks_dirtied', 0::bigint,
            'shared_blks_written', 0::bigint,
            'temp_blks_read',  0::bigint,
            'temp_blks_written', 0::bigint,
            'blk_read_time',   0.0,
            'blk_write_time',  0.0,
            'total_plan_time', round((0.5 * (5000 + g * 50))::numeric, 2),
            'mean_plan_time',  0.5
        ),
        -- Q3: high plan_time relative to exec_time (plan 25ms, exec 5ms)
        jsonb_build_object(
            'queryid',         -3852917406285103,
            'query',           'SELECT p.*, array_agg(t.name) FROM products p LEFT JOIN product_tags pt ON pt.product_id = p.id LEFT JOIN tags t ON t.id = pt.tag_id WHERE p.category_id = ANY($1) GROUP BY p.id',
            'calls',           (800 + g * 10)::bigint,
            'mean_exec_time',  round((5.0 + (random() * 2 - 1))::numeric, 2),
            'total_exec_time', round((5.0 * (800 + g * 10))::numeric, 2),
            'min_exec_time',   1.5,
            'max_exec_time',   30.0,
            'stddev_exec_time', 3.0,
            'rows',            (50 + g)::bigint,
            'shared_blks_hit', (20000 + g * 300)::bigint,
            'shared_blks_read',(100 + g * 5)::bigint,
            'shared_blks_dirtied', 0::bigint,
            'shared_blks_written', 0::bigint,
            'temp_blks_read',  0::bigint,
            'temp_blks_written', 0::bigint,
            'blk_read_time',   0.0,
            'blk_write_time',  0.0,
            'total_plan_time', round((25.0 * (800 + g * 10))::numeric, 2),
            'mean_plan_time',  25.0
        ),
        -- Q4: stable analytical query
        jsonb_build_object(
            'queryid',         -5603184927561047,
            'query',           'SELECT date_trunc($1, created_at) AS period, count(*), sum(amount) FROM transactions WHERE created_at BETWEEN $2 AND $3 GROUP BY 1',
            'calls',           (300 + g * 5)::bigint,
            'mean_exec_time',  round((150.0 + (random() * 20 - 10))::numeric, 2),
            'total_exec_time', round((150.0 * (300 + g * 5))::numeric, 2),
            'min_exec_time',   80.0,
            'max_exec_time',   500.0,
            'stddev_exec_time', 40.0,
            'rows',            (30 + g)::bigint,
            'shared_blks_hit', (50000 + g * 800)::bigint,
            'shared_blks_read',(5000 + g * 200)::bigint,
            'shared_blks_dirtied', 0::bigint,
            'shared_blks_written', 0::bigint,
            'temp_blks_read',  (100 + g * 10)::bigint,
            'temp_blks_written', (100 + g * 10)::bigint,
            'blk_read_time',   round((5.0 + g * 0.5)::numeric, 2),
            'blk_write_time',  0.0,
            'total_plan_time', round((3.0 * (300 + g * 5))::numeric, 2),
            'mean_plan_time',  3.0
        ),
        -- Q5: lightweight lookup, stable
        jsonb_build_object(
            'queryid',         -9374025816390274,
            'query',           'SELECT id, name, settings FROM tenants WHERE id = $1',
            'calls',           (10000 + g * 100)::bigint,
            'mean_exec_time',  round((0.3 + (random() * 0.1))::numeric, 3),
            'total_exec_time', round((0.3 * (10000 + g * 100))::numeric, 2),
            'min_exec_time',   0.05,
            'max_exec_time',   5.0,
            'stddev_exec_time', 0.1,
            'rows',            1::bigint,
            'shared_blks_hit', (30000 + g * 300)::bigint,
            'shared_blks_read', 10::bigint,
            'shared_blks_dirtied', 0::bigint,
            'shared_blks_written', 0::bigint,
            'temp_blks_read',  0::bigint,
            'temp_blks_written', 0::bigint,
            'blk_read_time',   0.0,
            'blk_write_time',  0.0,
            'total_plan_time', round((0.05 * (10000 + g * 100))::numeric, 2),
            'mean_plan_time',  0.05
        ),
        -- Q6: INSERT workload, growing
        jsonb_build_object(
            'queryid',         -4481927365018293,
            'query',           'INSERT INTO events (tenant_id, event_type, payload, created_at) VALUES ($1, $2, $3, now())',
            'calls',           (2000 + g * 100)::bigint,
            'mean_exec_time',  round((1.5 + (random() * 0.5))::numeric, 2),
            'total_exec_time', round((1.5 * (2000 + g * 100))::numeric, 2),
            'min_exec_time',   0.5,
            'max_exec_time',   20.0,
            'stddev_exec_time', 1.0,
            'rows',            1::bigint,
            'shared_blks_hit', (5000 + g * 200)::bigint,
            'shared_blks_read', (50 + g * 2)::bigint,
            'shared_blks_dirtied', (2000 + g * 100)::bigint,
            'shared_blks_written', (500 + g * 25)::bigint,
            'temp_blks_read',  0::bigint,
            'temp_blks_written', 0::bigint,
            'blk_read_time',   0.0,
            'blk_write_time',  round((0.5 + g * 0.05)::numeric, 2),
            'wal_records',     (2000 + g * 100)::bigint,
            'wal_bytes',       (256000 + g * 12800)::bigint,
            'total_plan_time', round((0.2 * (2000 + g * 100))::numeric, 2),
            'mean_plan_time',  0.2
        )
    )
FROM generate_series(0, 29) AS g;

-- ============================================================
-- 3. Sequence Snapshots (30 days)
--    category = 'sequences', data = JSON array of SequenceStats
-- ============================================================
INSERT INTO sage.snapshots (collected_at, category, data)
SELECT
    now() - interval '30 days' + (g * interval '1 day') + interval '6 hours',
    'sequences',
    jsonb_build_array(
        -- orders_id_seq: approaching int4 max, ~1M/day growth
        jsonb_build_object(
            'schemaname',   'public',
            'sequencename', 'orders_id_seq',
            'data_type',    'integer',
            'last_value',   (2147000000::bigint + g * 1000000),
            'max_value',    2147483647::bigint,
            'increment_by', 1::bigint,
            'pct_used',     round(((2147000000.0 + g::numeric * 1000000.0) / 2147483647.0 * 100)::numeric, 2)
        ),
        -- events_id_seq: healthy, no exhaustion risk
        jsonb_build_object(
            'schemaname',   'public',
            'sequencename', 'events_id_seq',
            'data_type',    'bigint',
            'last_value',   (50000000 + g * 200000)::bigint,
            'max_value',    9223372036854775807::bigint,
            'increment_by', 1::bigint,
            'pct_used',     0.0
        )
    )
FROM generate_series(0, 29) AS g;

-- ============================================================
-- 4. Table Snapshots (30 days)
--    category = 'tables', data = JSON array of TableStats
-- ============================================================
INSERT INTO sage.snapshots (collected_at, category, data)
SELECT
    now() - interval '30 days' + (g * interval '1 day') + interval '6 hours',
    'tables',
    jsonb_build_array(
        jsonb_build_object(
            'schemaname',        'public',
            'relname',           'orders',
            'seq_scan',          (100 + g * 15)::bigint,
            'seq_tup_read',      (500000 + g * 20000)::bigint,
            'idx_scan',          (50000 + g * 2000)::bigint,
            'idx_tup_fetch',     (200000 + g * 8000)::bigint,
            'n_tup_ins',         (1000 + g * 50)::bigint,
            'n_tup_upd',         (500 + g * 20)::bigint,
            'n_tup_del',         (50 + g * 2)::bigint,
            'n_tup_hot_upd',     (300 + g * 12)::bigint,
            'n_live_tup',        (2000000 + g * 30000)::bigint,
            'n_dead_tup',        (5000 + g * 500)::bigint,
            'vacuum_count',      (10 + g / 3)::bigint,
            'autovacuum_count',  (20 + g / 2)::bigint,
            'analyze_count',     (5 + g / 5)::bigint,
            'autoanalyze_count', (15 + g / 3)::bigint,
            'total_bytes',       (268435456 + g * 8388608)::bigint,
            'table_bytes',       (201326592 + g * 6291456)::bigint,
            'index_bytes',       (67108864 + g * 2097152)::bigint,
            'relpersistence',    'p'
        ),
        jsonb_build_object(
            'schemaname',        'public',
            'relname',           'events',
            'seq_scan',          (20 + g * 2)::bigint,
            'seq_tup_read',      (100000 + g * 5000)::bigint,
            'idx_scan',          (80000 + g * 3000)::bigint,
            'idx_tup_fetch',     (80000 + g * 3000)::bigint,
            'n_tup_ins',         (5000 + g * 200)::bigint,
            'n_tup_upd',         50::bigint,
            'n_tup_del',         0::bigint,
            'n_tup_hot_upd',     0::bigint,
            'n_live_tup',        (10000000 + g * 200000)::bigint,
            'n_dead_tup',        (1000 + g * 100)::bigint,
            'vacuum_count',      (5 + g / 5)::bigint,
            'autovacuum_count',  (30 + g)::bigint,
            'analyze_count',     (3 + g / 7)::bigint,
            'autoanalyze_count', (25 + g)::bigint,
            'total_bytes',       (1073741824 + g * 26214400)::bigint,
            'table_bytes',       (858993459 + g * 20971520)::bigint,
            'index_bytes',       (214748365 + g * 5242880)::bigint,
            'relpersistence',    'p'
        )
    )
FROM generate_series(0, 29) AS g;

-- ============================================================
-- 5. Index Snapshots (30 days)
--    category = 'indexes', data = JSON array of IndexStats
-- ============================================================
INSERT INTO sage.snapshots (collected_at, category, data)
SELECT
    now() - interval '30 days' + (g * interval '1 day') + interval '6 hours',
    'indexes',
    jsonb_build_array(
        jsonb_build_object(
            'schemaname',   'public',
            'relname',      'orders',
            'indexrelname',  'orders_pkey',
            'idx_scan',     (40000 + g * 1500)::bigint,
            'idx_tup_read', (40000 + g * 1500)::bigint,
            'idx_tup_fetch',(40000 + g * 1500)::bigint,
            'index_bytes',  (33554432 + g * 1048576)::bigint,
            'indisunique',  true,
            'indisprimary', true,
            'indisvalid',   true,
            'indexdef',     'CREATE UNIQUE INDEX orders_pkey ON public.orders USING btree (id)',
            'index_type',   'btree'
        ),
        jsonb_build_object(
            'schemaname',    'public',
            'relname',       'orders',
            'indexrelname',  'idx_orders_customer_id',
            'idx_scan',      (10000 + g * 500)::bigint,
            'idx_tup_read',  (50000 + g * 2000)::bigint,
            'idx_tup_fetch', (50000 + g * 2000)::bigint,
            'index_bytes',   (16777216 + g * 524288)::bigint,
            'indisunique',   false,
            'indisprimary',  false,
            'indisvalid',    true,
            'indexdef',      'CREATE INDEX idx_orders_customer_id ON public.orders USING btree (customer_id)',
            'index_type',    'btree'
        ),
        jsonb_build_object(
            'schemaname',    'public',
            'relname',       'events',
            'indexrelname',  'events_pkey',
            'idx_scan',      (70000 + g * 2500)::bigint,
            'idx_tup_read',  (70000 + g * 2500)::bigint,
            'idx_tup_fetch', (70000 + g * 2500)::bigint,
            'index_bytes',   (67108864 + g * 2097152)::bigint,
            'indisunique',   true,
            'indisprimary',  true,
            'indisvalid',    true,
            'indexdef',      'CREATE UNIQUE INDEX events_pkey ON public.events USING btree (id)',
            'index_type',    'btree'
        ),
        -- Unused index (0 scans) for unused_index detection
        jsonb_build_object(
            'schemaname',    'public',
            'relname',       'orders',
            'indexrelname',  'idx_orders_legacy_status',
            'idx_scan',      0::bigint,
            'idx_tup_read',  0::bigint,
            'idx_tup_fetch', 0::bigint,
            'index_bytes',   (8388608 + g * 262144)::bigint,
            'indisunique',   false,
            'indisprimary',  false,
            'indisvalid',    true,
            'indexdef',      'CREATE INDEX idx_orders_legacy_status ON public.orders USING btree (legacy_status)',
            'index_type',    'btree'
        )
    )
FROM generate_series(0, 29) AS g;

-- ============================================================
-- 6. Explain Cache Entries (plan diffing)
-- ============================================================

-- Pair A: Index Scan -> Seq Scan (10x cost, triggers plan_regression critical)
INSERT INTO sage.explain_cache
    (captured_at, queryid, query_text, plan_json, source, total_cost, execution_time)
VALUES
    (now() - interval '5 days',
     -7216849732456821,
     'SELECT o.*, c.name FROM orders o JOIN customers c ON c.id = o.customer_id WHERE o.status = $1 AND o.created_at > $2',
     '[{"Plan": {"Node Type": "Nested Loop", "Total Cost": 50.0, "Plans": [{"Node Type": "Index Scan", "Relation Name": "orders", "Total Cost": 30.0, "Plans": []}, {"Node Type": "Index Scan", "Relation Name": "customers", "Total Cost": 20.0, "Plans": []}]}}]',
     'auto_explain', 50.0, 180.0),
    (now() - interval '1 day',
     -7216849732456821,
     'SELECT o.*, c.name FROM orders o JOIN customers c ON c.id = o.customer_id WHERE o.status = $1 AND o.created_at > $2',
     '[{"Plan": {"Node Type": "Hash Join", "Total Cost": 500.0, "Plans": [{"Node Type": "Seq Scan", "Relation Name": "orders", "Total Cost": 400.0, "Plans": []}, {"Node Type": "Hash", "Total Cost": 100.0, "Plans": [{"Node Type": "Seq Scan", "Relation Name": "customers", "Total Cost": 100.0, "Plans": []}]}]}}]',
     'auto_explain', 500.0, 750.0);

-- Pair B: moderate cost increase (2.5x), same node types
INSERT INTO sage.explain_cache
    (captured_at, queryid, query_text, plan_json, source, total_cost, execution_time)
VALUES
    (now() - interval '4 days',
     -5603184927561047,
     'SELECT date_trunc($1, created_at) AS period, count(*), sum(amount) FROM transactions WHERE created_at BETWEEN $2 AND $3 GROUP BY 1',
     '[{"Plan": {"Node Type": "HashAggregate", "Total Cost": 200.0, "Plans": [{"Node Type": "Index Scan", "Relation Name": "transactions", "Total Cost": 180.0, "Plans": []}]}}]',
     'auto_explain', 200.0, 120.0),
    (now() - interval '1 day',
     -5603184927561047,
     'SELECT date_trunc($1, created_at) AS period, count(*), sum(amount) FROM transactions WHERE created_at BETWEEN $2 AND $3 GROUP BY 1',
     '[{"Plan": {"Node Type": "HashAggregate", "Total Cost": 500.0, "Plans": [{"Node Type": "Index Scan", "Relation Name": "transactions", "Total Cost": 480.0, "Plans": []}]}}]',
     'auto_explain', 500.0, 310.0);

-- Pair C: disk spill appearing in newer plan (Sort Disk)
INSERT INTO sage.explain_cache
    (captured_at, queryid, query_text, plan_json, source, total_cost, execution_time)
VALUES
    (now() - interval '3 days',
     -3852917406285103,
     'SELECT p.*, array_agg(t.name) FROM products p LEFT JOIN product_tags pt ON pt.product_id = p.id LEFT JOIN tags t ON t.id = pt.tag_id WHERE p.category_id = ANY($1) GROUP BY p.id',
     '[{"Plan": {"Node Type": "GroupAggregate", "Total Cost": 150.0, "Plans": [{"Node Type": "Sort", "Sort Space Type": "Memory", "Total Cost": 120.0, "Plans": [{"Node Type": "Nested Loop", "Total Cost": 100.0, "Plans": [{"Node Type": "Index Scan", "Relation Name": "products", "Total Cost": 40.0, "Plans": []}, {"Node Type": "Index Scan", "Relation Name": "product_tags", "Total Cost": 60.0, "Plans": []}]}]}]}}]',
     'auto_explain', 150.0, 4.5),
    (now() - interval '1 day',
     -3852917406285103,
     'SELECT p.*, array_agg(t.name) FROM products p LEFT JOIN product_tags pt ON pt.product_id = p.id LEFT JOIN tags t ON t.id = pt.tag_id WHERE p.category_id = ANY($1) GROUP BY p.id',
     '[{"Plan": {"Node Type": "GroupAggregate", "Total Cost": 280.0, "Plans": [{"Node Type": "Sort", "Sort Space Type": "Disk", "Total Cost": 250.0, "Plans": [{"Node Type": "Nested Loop", "Total Cost": 200.0, "Plans": [{"Node Type": "Index Scan", "Relation Name": "products", "Total Cost": 80.0, "Plans": []}, {"Node Type": "Index Scan", "Relation Name": "product_tags", "Total Cost": 120.0, "Plans": []}]}]}]}}]',
     'auto_explain', 280.0, 25.0);

COMMIT;
