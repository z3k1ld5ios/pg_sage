#!/bin/bash
# Slow query generator - triggers slow_query and seq_scan findings
PG_URL="${PG_URL:-postgres://postgres:test@localhost:5433/testdb}"
for i in $(seq 1 20); do
  psql "$PG_URL" -c "
    SELECT o.*, c.name, c.email, p.name as product_name
    FROM orders o
    JOIN customers c ON o.customer_id = c.id
    JOIN products p ON o.product_id = p.id
    WHERE c.tier = 'free'
      AND o.status = 'pending'
    ORDER BY o.total DESC
    LIMIT 1000;
  " > /dev/null 2>&1
done
echo "Slow query workload complete ($i iterations)"
