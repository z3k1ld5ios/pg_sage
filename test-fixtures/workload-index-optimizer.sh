#!/bin/bash
# Index optimizer workload - triggers index recommendations
PG_URL="${PG_URL:-postgres://postgres:test@localhost:5433/testdb}"
for i in $(seq 1 30); do
  psql "$PG_URL" -c "SELECT * FROM orders WHERE customer_id = $((RANDOM % 10000)) AND status = 'pending';" > /dev/null 2>&1
  psql "$PG_URL" -c "SELECT * FROM orders WHERE region = 'us-east' AND total > 500 ORDER BY created_at DESC LIMIT 100;" > /dev/null 2>&1
  psql "$PG_URL" -c "SELECT o.id, o.total, c.name FROM orders o JOIN customers c ON o.customer_id = c.id WHERE c.tier = 'premium' AND o.status = 'shipped';" > /dev/null 2>&1
done
echo "Index optimizer workload complete ($i iterations × 3 queries)"
