#!/bin/bash
# Sort spill generator - forces disk sort for work_mem hint
PG_URL="${PG_URL:-postgres://postgres:test@localhost:5433/testdb}"
for i in $(seq 1 20); do
  psql "$PG_URL" -c "
    SET work_mem = '64kB';
    SELECT * FROM orders ORDER BY total DESC, created_at ASC LIMIT 10000;
  " > /dev/null 2>&1
done
echo "Sort spill workload complete ($i iterations)"
