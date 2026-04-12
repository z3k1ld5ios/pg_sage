-- Repeatable workload to push pg_stat_statements call counts over min_query_calls=5.

SELECT count(*) FROM app.order_items oi
JOIN app.orders o ON o.id = oi.order_id AND o.order_date = oi.order_date
JOIN app.customers c ON c.id = o.customer_id
WHERE oi.unit_price > 50 AND c.name LIKE '%99%';

SELECT * FROM app.order_items ORDER BY unit_price DESC LIMIT 100;

SELECT * FROM app.audit_log WHERE action = 'event_3' LIMIT 50;

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

SELECT o.status, count(*), avg(o.total)
FROM app.orders o
GROUP BY o.status;

SELECT id, email FROM app.customers WHERE id = 11;
SELECT id, email FROM app.customers WHERE id = 12;
SELECT id, email FROM app.customers WHERE id = 13;
SELECT id, email FROM app.customers WHERE id = 14;
SELECT id, email FROM app.customers WHERE id = 15;
