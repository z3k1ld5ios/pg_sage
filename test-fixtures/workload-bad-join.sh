#!/bin/bash
# Bad join generator - forces nested loop joins for tuner hint testing
PG_URL="${PG_URL:-postgres://postgres:test@localhost:5433/testdb}"
for i in $(seq 1 20); do
  psql "$PG_URL" -c "
    SET enable_hashjoin = off;
    SET enable_mergejoin = off;
    SELECT o.id, c.name
    FROM orders o, customers c
    WHERE o.customer_id = c.id
      AND o.total > 500
      AND c.tier = 'free';
  " > /dev/null 2>&1
done
echo "Bad join workload complete ($i iterations)"
