#!/bin/bash
# Query regression generator
# Phase 1: Establish baseline with index
# Phase 2: Drop index and re-run to trigger regression
PG_URL="${PG_URL:-postgres://postgres:test@localhost:5433/testdb}"

echo "Phase 1: Establishing baseline..."
psql "$PG_URL" -c "CREATE INDEX IF NOT EXISTS idx_regression_test ON orders(total);"
for i in $(seq 1 20); do
  psql "$PG_URL" -c "SELECT * FROM orders WHERE total > 900 ORDER BY total LIMIT 100;" > /dev/null 2>&1
done

echo "Waiting for 2 analyzer cycles (20s)..."
sleep 20

echo "Phase 2: Dropping index to trigger regression..."
psql "$PG_URL" -c "DROP INDEX IF EXISTS idx_regression_test;"
for i in $(seq 1 20); do
  psql "$PG_URL" -c "SELECT * FROM orders WHERE total > 900 ORDER BY total LIMIT 100;" > /dev/null 2>&1
done
echo "Regression workload complete"
