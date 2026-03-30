#!/bin/bash
# Idle transaction - triggers connection_leak detection
PG_URL="${PG_URL:-postgres://postgres:test@localhost:5433/testdb}"
psql "$PG_URL" -c "
  BEGIN;
  SELECT 1;
  SELECT pg_sleep(180);
" &
echo "Idle transaction opened (background PID: $!)"
