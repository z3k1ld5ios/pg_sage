#!/bin/bash
# Connection flood - triggers circuit breaker
PG_URL="${PG_URL:-postgres://postgres:test@localhost:5433/testdb}"
PIDS=()
for i in $(seq 1 80); do
  psql "$PG_URL" -c "SELECT pg_sleep(60)" &
  PIDS+=($!)
done
echo "Opened 80 connections (background), PIDs: ${PIDS[*]}"
echo "Run 'kill ${PIDS[*]}' to clean up"
