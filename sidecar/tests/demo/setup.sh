#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"
PG_CONTAINER="pg-sage-demo"
PG_IMAGE="pg-sage-demo:17"
PG_PORT=5432
PG_PASSWORD="demopw"
SAGE_PASSWORD="sagepw"
PGBENCH_SCALE=10

echo "=== pg_sage Demo Setup ==="

# 0. Check prerequisites
for cmd in docker psql pgbench go; do
    if ! command -v "$cmd" &>/dev/null; then
        echo "FAIL: $cmd not found. Install it first."
        echo "  On Debian/Ubuntu: sudo apt-get install -y docker.io postgresql-client golang-go"
        echo "  On RHEL/Fedora:   sudo dnf install -y docker postgresql pgbench golang"
        exit 1
    fi
done

# 1. Build custom PostgreSQL image with extensions
if docker images --format '{{.Repository}}:{{.Tag}}' | grep -q "^${PG_IMAGE}$"; then
    echo "[ok] Custom PostgreSQL image already built"
else
    echo "[1/7] Building PostgreSQL 17 + HypoPG + pg_hint_plan..."
    docker build -t "$PG_IMAGE" "$SCRIPT_DIR"
fi

# 2. Start PostgreSQL (or skip if already running)
if docker ps --format '{{.Names}}' | grep -q "^${PG_CONTAINER}$"; then
    echo "[ok] PostgreSQL container already running"
else
    echo "[2/7] Starting PostgreSQL 17..."
    docker run -d --name "$PG_CONTAINER" \
        -e POSTGRES_PASSWORD="$PG_PASSWORD" \
        -p ${PG_PORT}:5432 "$PG_IMAGE" \
        -c shared_preload_libraries='pg_stat_statements,pg_hint_plan,auto_explain' \
        -c pg_stat_statements.track=all \
        -c auto_explain.log_min_duration=500 \
        -c auto_explain.log_analyze=on \
        -c auto_explain.log_format=json

    echo "     Waiting for PostgreSQL to be ready..."
    for i in $(seq 1 30); do
        if docker exec "$PG_CONTAINER" pg_isready -q 2>/dev/null; then
            break
        fi
        sleep 1
    done
    docker exec "$PG_CONTAINER" pg_isready -q || { echo "FAIL: PostgreSQL did not start"; exit 1; }
    echo "     PostgreSQL is ready"
fi

# 3. Create sage_agent user + enable extensions
echo "[3/7] Creating sage_agent user and extensions..."
PGPASSWORD="$PG_PASSWORD" psql -h localhost -p "$PG_PORT" -U postgres -q <<'SQL'
DO $$ BEGIN
    CREATE USER sage_agent WITH PASSWORD 'sagepw';
EXCEPTION WHEN duplicate_object THEN NULL;
END $$;
GRANT pg_monitor TO sage_agent;
GRANT pg_read_all_stats TO sage_agent;
GRANT CREATE ON SCHEMA public TO sage_agent;
GRANT pg_signal_backend TO sage_agent;

-- Enable extensions
CREATE EXTENSION IF NOT EXISTS pg_stat_statements;
CREATE EXTENSION IF NOT EXISTS hypopg;
CREATE EXTENSION IF NOT EXISTS pg_hint_plan;
SQL
echo "     sage_agent user and extensions ready"

# 4. Seed test objects
echo "[4/7] Seeding realistic data (~200K rows)..."
PGPASSWORD="$PG_PASSWORD" psql -h localhost -p "$PG_PORT" -U postgres -q \
    -f "$REPO_DIR/tests/integration/seed_objects.sql"
echo "     Seed data loaded"

# 5. Initialize pgbench tables
echo "[5/7] Initializing pgbench (scale factor $PGBENCH_SCALE)..."
PGPASSWORD="$PG_PASSWORD" pgbench -h localhost -p "$PG_PORT" -U postgres \
    -i -s "$PGBENCH_SCALE" -q postgres
echo "     pgbench tables ready (${PGBENCH_SCALE}M rows in pgbench_accounts)"

# 6. Build pg_sage
echo "[6/7] Building pg_sage sidecar..."
cd "$REPO_DIR"
go build -o pg_sage_sidecar ./cmd/pg_sage_sidecar/
echo "     Binary built: $REPO_DIR/pg_sage_sidecar"

# 7. Done
echo "[7/7] Setup complete!"
echo ""
echo "  Extensions installed:"
echo "    - pg_stat_statements (query stats)"
echo "    - hypopg (hypothetical indexes — used by optimizer)"
echo "    - pg_hint_plan (per-query hints — used by tuner)"
echo "    - auto_explain (plan capture for slow queries)"
echo ""
echo "  Start pg_sage:"
echo "    ./pg_sage_sidecar --config tests/demo/config_demo.yaml"
echo ""
echo "  Generate load (in another terminal):"
echo "    PGPASSWORD=demopw pgbench -h localhost -U postgres -c 8 -j 4 -T 300 postgres"
echo ""
echo "  Custom workload (in another terminal):"
echo "    PGPASSWORD=demopw pgbench -h localhost -U postgres -f tests/demo/custom_workload.sql -c 4 -T 300 postgres"
echo ""
echo "  Dashboard: http://localhost:8080"
echo "  Prometheus: http://localhost:9187/metrics"
