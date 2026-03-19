#!/bin/bash
set -e

echo "=== pg_sage Build & Test ==="
echo ""

# Build
echo "--- Building extension ---"
cd "$(dirname "$0")/.."
make clean 2>/dev/null || true
make

echo ""
echo "--- Installing extension ---"
make install

echo ""
echo "--- Restarting PostgreSQL ---"
pg_ctl restart -D "$PGDATA" -w -l /tmp/pg_sage_test.log

echo ""
echo "--- Waiting for PostgreSQL ---"
sleep 2

echo ""
echo "--- Running setup ---"
psql -d postgres -c "DROP EXTENSION IF EXISTS pg_sage CASCADE;"
psql -d postgres -c "CREATE EXTENSION pg_sage;"

echo ""
echo "--- Running regression tests ---"
psql -d postgres -f test/regression.sql

echo ""
echo "--- Running integration tests ---"
psql -d postgres -f test/run_tests.sql

echo ""
echo "=== All tests passed ==="
