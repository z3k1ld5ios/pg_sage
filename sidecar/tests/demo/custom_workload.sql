-- pg_sage demo workload: exercises analyzer rules via pgbench
-- Usage: pgbench -f tests/demo/custom_workload.sql -c 4 -T 300 postgres
--
-- Each statement runs once per transaction. pgbench \set provides
-- randomized bind values so pg_stat_statements sees one normalized query
-- per pattern, but different data each time.

-- Variables for randomized queries
\set event_type random(1, 10)
\set product_id random(1, 200)
\set region_id random(1, 10)
\set audit_id random(1, 6000)

-- 1) Seq scan on large table (triggers seq_scan_watchdog).
--    audit_log.action has no index, forcing a full table scan on ~60K rows.
SELECT count(*)
  FROM app.audit_log
 WHERE action = 'event_' || :event_type::text;

-- 2) Missing FK index scan (triggers missing_fk_index detection).
--    order_items.product_id is a foreign key but may lack an index.
SELECT oi.*
  FROM app.order_items oi
 WHERE oi.product_id = :product_id
 LIMIT 10;

-- 3) ORDER BY + LIMIT on unindexed column (triggers sort_without_index).
--    Sorting order_items by unit_price requires a full sort without an index.
SELECT *
  FROM app.order_items
 ORDER BY unit_price DESC
 LIMIT 50;

-- 4) Expensive multi-join (triggers slow_query).
--    Four-way join across customers, orders, order_items with aggregation.
SELECT c.name,
       count(oi.id),
       sum(oi.qty * oi.unit_price)
  FROM app.customers c
  JOIN app.orders o ON o.customer_id = c.id
  JOIN app.order_items oi ON oi.order_id = o.id
                          AND oi.order_date = o.order_date
 WHERE c.region_id = :region_id
 GROUP BY c.name
 ORDER BY sum(oi.qty * oi.unit_price) DESC
 LIMIT 20;

-- 5) Update without vacuum (grows bloat).
--    Repeated in-place updates create dead tuples that bloat the table.
UPDATE app.audit_log
   SET payload = repeat('z', 200)
 WHERE id = :audit_id;
