#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PG_PORT=${PG_PORT:-5432}
PG_PASSWORD=${PG_PASSWORD:-demopw}
DURATION=${1:-300}  # Default 5 minutes
CLIENTS=${2:-8}

export PGPASSWORD="$PG_PASSWORD"

echo "=== pg_sage Demo Workload ==="
echo "Duration: ${DURATION}s | TPC-B clients: $CLIENTS | Custom clients: 4"
echo ""

# Cleanup on exit
cleanup() {
    echo ""
    echo "Stopping workloads..."
    kill $TPCB_PID $CUSTOM_PID 2>/dev/null || true
    wait $TPCB_PID $CUSTOM_PID 2>/dev/null || true

    echo ""
    echo "=== Results ==="
    echo ""
    echo "Check findings:"
    echo "  PGPASSWORD=sagepw psql -h localhost -U sage_agent -d postgres -c \\"
    echo "    \"SELECT category, severity, count(*) FROM sage.findings WHERE status='open' GROUP BY 1,2 ORDER BY 2,1;\""
    echo ""
    echo "Dashboard: http://localhost:8080"
}
trap cleanup EXIT

# Start TPC-B workload (background)
echo "[1/2] Starting TPC-B workload ($CLIENTS clients)..."
pgbench -h localhost -p "$PG_PORT" -U postgres \
    -c "$CLIENTS" -j 4 -T "$DURATION" --progress=30 postgres &
TPCB_PID=$!

# Start custom workload (background)
echo "[2/2] Starting custom workload (4 clients)..."
pgbench -h localhost -p "$PG_PORT" -U postgres \
    -f "$SCRIPT_DIR/custom_workload.sql" -c 4 -T "$DURATION" --progress=30 postgres &
CUSTOM_PID=$!

echo ""
echo "Workloads running. Press Ctrl+C to stop early."
echo "Watch pg_sage react at http://localhost:8080"
echo ""

# Wait for both
wait $TPCB_PID $CUSTOM_PID 2>/dev/null || true
