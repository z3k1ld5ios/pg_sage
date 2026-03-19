#!/usr/bin/env bash
# =============================================================================
# test_pg_upgrade.sh -- pg_sage major-version upgrade test (PG16 -> PG17)
#
# Prerequisites:
#   - Docker (with BuildKit) must be installed and the daemon running.
#   - This script must be run from the pg_sage repository root, or the repo
#     root is auto-detected as the parent of this script's directory.
#
# Usage:
#   bash test/test_pg_upgrade.sh
#
# What it does:
#   1. Builds a PG16 image with pg_sage compiled and installed.
#   2. Starts a PG16 container, creates the extension, inserts test data.
#   3. Stops PG16 cleanly.
#   4. Builds a PG17 image with pg_sage compiled and installed.
#   5. Runs `pg_upgrade --check` against the PG16 data directory.
#   6. If the check passes, performs the actual upgrade.
#   7. Starts PG17, verifies the extension loads and data is intact.
#   8. Reports PASS / FAIL and exits 0 / 1.
#
# The script creates temporary Docker volumes and removes them on exit.
# =============================================================================
set -euo pipefail

# ---------------------------------------------------------------------------
# Resolve repo root
# ---------------------------------------------------------------------------
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
PG16_IMAGE="pg_sage_upgrade_test:pg16"
PG17_IMAGE="pg_sage_upgrade_test:pg17"
VOLUME_DATA="pgsage_upgrade_pgdata"
VOLUME_NEW="pgsage_upgrade_pgdata_new"
NETWORK="pgsage_upgrade_net"
PG16_CONTAINER="pgsage_upgrade_pg16"
PG17_CONTAINER="pgsage_upgrade_pg17"
UPGRADE_CONTAINER="pgsage_upgrade_runner"
POSTGRES_PASSWORD="upgrade_test_pw"
PGDATABASE="postgres"

# Counters
PASS=0
FAIL=0

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
info()  { echo "==> $*"; }
ok()    { echo "  [PASS] $*"; PASS=$((PASS + 1)); }
fail()  { echo "  [FAIL] $*"; FAIL=$((FAIL + 1)); }

cleanup() {
    info "Cleaning up..."
    docker rm -f "$PG16_CONTAINER"   2>/dev/null || true
    docker rm -f "$PG17_CONTAINER"   2>/dev/null || true
    docker rm -f "$UPGRADE_CONTAINER" 2>/dev/null || true
    docker volume rm "$VOLUME_DATA"  2>/dev/null || true
    docker volume rm "$VOLUME_NEW"   2>/dev/null || true
    docker network rm "$NETWORK"     2>/dev/null || true
    info "Cleanup complete."
}
trap cleanup EXIT

wait_for_pg() {
    local container="$1"
    local max_wait="${2:-30}"
    local elapsed=0
    while ! docker exec "$container" pg_isready -U postgres -q 2>/dev/null; do
        sleep 1
        elapsed=$((elapsed + 1))
        if [ "$elapsed" -ge "$max_wait" ]; then
            echo "ERROR: PostgreSQL in $container did not become ready within ${max_wait}s"
            return 1
        fi
    done
}

psql_exec() {
    local container="$1"; shift
    docker exec -e PGPASSWORD="$POSTGRES_PASSWORD" "$container" \
        psql -U postgres -d "$PGDATABASE" -v ON_ERROR_STOP=1 -tAq "$@"
}

# ---------------------------------------------------------------------------
# 0. Pre-flight
# ---------------------------------------------------------------------------
info "Pre-flight checks"
if ! command -v docker &>/dev/null; then
    echo "ERROR: docker is not installed or not in PATH."
    exit 1
fi
if ! docker info &>/dev/null; then
    echo "ERROR: Docker daemon is not running."
    exit 1
fi

# Clean any leftovers from a previous run
cleanup 2>/dev/null || true

# Create shared network and volumes
docker network create "$NETWORK" >/dev/null
docker volume create "$VOLUME_DATA" >/dev/null
docker volume create "$VOLUME_NEW"  >/dev/null

# ---------------------------------------------------------------------------
# 1. Build PG16 image with pg_sage
# ---------------------------------------------------------------------------
info "Building PG16 image with pg_sage..."

docker build -t "$PG16_IMAGE" -f - "$REPO_ROOT" <<'DOCKERFILE16'
FROM postgres:16-bookworm

RUN apt-get update && apt-get install -y \
    build-essential \
    postgresql-server-dev-16 \
    libcurl4-openssl-dev \
    && rm -rf /var/lib/apt/lists/*

COPY . /build/pg_sage/
WORKDIR /build/pg_sage

RUN make clean && make && make install

RUN echo "shared_preload_libraries = 'pg_stat_statements,pg_sage'" \
    >> /usr/share/postgresql/postgresql.conf.sample && \
    echo "sage.database = 'postgres'" \
    >> /usr/share/postgresql/postgresql.conf.sample
DOCKERFILE16

# ---------------------------------------------------------------------------
# 2. Start PG16, create extension, insert test data
# ---------------------------------------------------------------------------
info "Starting PG16 container..."
docker run -d \
    --name "$PG16_CONTAINER" \
    --network "$NETWORK" \
    -e POSTGRES_PASSWORD="$POSTGRES_PASSWORD" \
    -v "$VOLUME_DATA":/var/lib/postgresql/data \
    "$PG16_IMAGE" \
    postgres \
        -c shared_preload_libraries='pg_stat_statements,pg_sage' \
        -c sage.database='postgres' \
        -c sage.collector_interval=3600 \
        -c sage.analyzer_interval=3600 \
        -c sage.trust_level='advisory'

wait_for_pg "$PG16_CONTAINER"
info "PG16 is ready."

info "Creating extension and inserting test data on PG16..."
psql_exec "$PG16_CONTAINER" <<'SQL'
-- Enable extensions
CREATE EXTENSION IF NOT EXISTS pg_stat_statements;
CREATE EXTENSION IF NOT EXISTS pg_sage;

-- ---- Test data in pg_sage catalog tables ----

-- Findings
INSERT INTO sage.findings
    (category, severity, object_type, object_identifier, title, detail, recommendation, status)
VALUES
    ('index', 'medium', 'index', 'public.idx_test_dup', 'Duplicate index detected',
     '{"table":"test_tbl","columns":["col_a"]}'::jsonb,
     'DROP INDEX idx_test_dup;', 'open'),
    ('bloat', 'high', 'table', 'public.big_table', 'Table bloat > 40%',
     '{"dead_tup_ratio":0.42}'::jsonb,
     'VACUUM FULL public.big_table;', 'open'),
    ('query', 'low', 'query', 'queryid:123456', 'Sequential scan on large table',
     '{"rows":5000000,"table":"public.big_table"}'::jsonb,
     'Consider adding an index.', 'resolved');

-- Snapshots
INSERT INTO sage.snapshots (category, data)
VALUES
    ('tables', '{"tables":[{"name":"big_table","n_live_tup":5000000}]}'::jsonb),
    ('indexes', '{"indexes":[{"name":"idx_test","scans":0}]}'::jsonb),
    ('system', '{"load":0.75,"connections":12}'::jsonb);

-- Action log
INSERT INTO sage.action_log
    (action_type, sql_executed, rollback_sql, outcome)
VALUES
    ('create_index', 'CREATE INDEX idx_orders_status ON orders(status)',
     'DROP INDEX idx_orders_status', 'success'),
    ('vacuum', 'VACUUM ANALYZE public.big_table', NULL, 'success');

-- Config override
UPDATE sage.config SET value = '120', updated_by = 'upgrade_test'
    WHERE key = 'snapshot_interval_seconds';

SQL

# Capture expected counts
COUNT_FINDINGS=$(psql_exec "$PG16_CONTAINER" -c "SELECT count(*) FROM sage.findings;")
COUNT_SNAPSHOTS=$(psql_exec "$PG16_CONTAINER" -c "SELECT count(*) FROM sage.snapshots;")
COUNT_ACTIONS=$(psql_exec "$PG16_CONTAINER" -c "SELECT count(*) FROM sage.action_log;")
CONFIG_VAL=$(psql_exec "$PG16_CONTAINER" -c "SELECT value FROM sage.config WHERE key='snapshot_interval_seconds';")
PG16_VERSION=$(psql_exec "$PG16_CONTAINER" -c "SHOW server_version;")

info "PG16 version: $PG16_VERSION"
info "Counts -- findings:$COUNT_FINDINGS  snapshots:$COUNT_SNAPSHOTS  actions:$COUNT_ACTIONS  config_val:$CONFIG_VAL"

# ---------------------------------------------------------------------------
# 3. Stop PG16 cleanly
# ---------------------------------------------------------------------------
info "Stopping PG16 gracefully..."
docker exec "$PG16_CONTAINER" pg_ctl stop -D /var/lib/postgresql/data -m fast -w
docker stop "$PG16_CONTAINER" >/dev/null 2>&1 || true

# ---------------------------------------------------------------------------
# 4. Build PG17 image with pg_sage
# ---------------------------------------------------------------------------
info "Building PG17 image with pg_sage..."

docker build -t "$PG17_IMAGE" -f - "$REPO_ROOT" <<'DOCKERFILE17'
FROM postgres:17-bookworm

RUN apt-get update && apt-get install -y \
    build-essential \
    postgresql-server-dev-17 \
    libcurl4-openssl-dev \
    && rm -rf /var/lib/apt/lists/*

COPY . /build/pg_sage/
WORKDIR /build/pg_sage

RUN make clean && make && make install

# Also install PG16 binaries so pg_upgrade can find the old server
RUN echo "deb http://apt.postgresql.org/pub/repos/apt bookworm-pgdg main" \
        > /etc/apt/sources.list.d/pgdg.list && \
    apt-get update && \
    apt-get install -y --no-install-recommends postgresql-16 && \
    rm -rf /var/lib/apt/lists/*

RUN echo "shared_preload_libraries = 'pg_stat_statements,pg_sage'" \
    >> /usr/share/postgresql/postgresql.conf.sample && \
    echo "sage.database = 'postgres'" \
    >> /usr/share/postgresql/postgresql.conf.sample
DOCKERFILE17

# ---------------------------------------------------------------------------
# 5. pg_upgrade --check
# ---------------------------------------------------------------------------
info "Running pg_upgrade --check..."

docker run --rm \
    --name "$UPGRADE_CONTAINER" \
    -e PGDATA=/var/lib/postgresql/data_new \
    -v "$VOLUME_DATA":/var/lib/postgresql/data_old \
    -v "$VOLUME_NEW":/var/lib/postgresql/data_new \
    "$PG17_IMAGE" bash -c '
set -e

# Initialise a fresh PG17 cluster
chown -R postgres:postgres /var/lib/postgresql/data_new
su - postgres -c "\
    /usr/lib/postgresql/17/bin/initdb \
        -D /var/lib/postgresql/data_new \
        --locale-provider=libc"

# Ensure shared_preload_libraries is set on the new cluster
echo "shared_preload_libraries = '\''pg_stat_statements,pg_sage'\''" \
    >> /var/lib/postgresql/data_new/postgresql.conf
echo "sage.database = '\''postgres'\''" \
    >> /var/lib/postgresql/data_new/postgresql.conf

chown -R postgres:postgres /var/lib/postgresql/data_old
chown -R postgres:postgres /var/lib/postgresql/data_new

su - postgres -c "\
    /usr/lib/postgresql/17/bin/pg_upgrade \
        --check \
        --old-datadir=/var/lib/postgresql/data_old \
        --new-datadir=/var/lib/postgresql/data_new \
        --old-bindir=/usr/lib/postgresql/16/bin \
        --new-bindir=/usr/lib/postgresql/17/bin"
'
if [ $? -eq 0 ]; then
    ok "pg_upgrade --check passed"
else
    fail "pg_upgrade --check failed"
    echo "RESULT: FAIL ($PASS passed, $((FAIL)) failed)"
    exit 1
fi

# ---------------------------------------------------------------------------
# 6. Actual pg_upgrade
# ---------------------------------------------------------------------------
info "Running pg_upgrade (actual)..."

# We need fresh volumes since --check may leave state
docker volume rm "$VOLUME_NEW" 2>/dev/null || true
docker volume create "$VOLUME_NEW" >/dev/null

docker run --rm \
    --name "$UPGRADE_CONTAINER" \
    -e PGDATA=/var/lib/postgresql/data_new \
    -v "$VOLUME_DATA":/var/lib/postgresql/data_old \
    -v "$VOLUME_NEW":/var/lib/postgresql/data_new \
    "$PG17_IMAGE" bash -c '
set -e

chown -R postgres:postgres /var/lib/postgresql/data_new
su - postgres -c "\
    /usr/lib/postgresql/17/bin/initdb \
        -D /var/lib/postgresql/data_new \
        --locale-provider=libc"

echo "shared_preload_libraries = '\''pg_stat_statements,pg_sage'\''" \
    >> /var/lib/postgresql/data_new/postgresql.conf
echo "sage.database = '\''postgres'\''" \
    >> /var/lib/postgresql/data_new/postgresql.conf

chown -R postgres:postgres /var/lib/postgresql/data_old
chown -R postgres:postgres /var/lib/postgresql/data_new

su - postgres -c "\
    /usr/lib/postgresql/17/bin/pg_upgrade \
        --old-datadir=/var/lib/postgresql/data_old \
        --new-datadir=/var/lib/postgresql/data_new \
        --old-bindir=/usr/lib/postgresql/16/bin \
        --new-bindir=/usr/lib/postgresql/17/bin"
'
if [ $? -eq 0 ]; then
    ok "pg_upgrade completed successfully"
else
    fail "pg_upgrade failed"
    echo "RESULT: FAIL ($PASS passed, $((FAIL)) failed)"
    exit 1
fi

# ---------------------------------------------------------------------------
# 7. Start PG17 on upgraded data, verify everything
# ---------------------------------------------------------------------------
info "Starting PG17 on upgraded data..."

# Remove the old PG16 container so we can reuse the network
docker rm -f "$PG16_CONTAINER" 2>/dev/null || true

docker run -d \
    --name "$PG17_CONTAINER" \
    --network "$NETWORK" \
    -e POSTGRES_PASSWORD="$POSTGRES_PASSWORD" \
    -v "$VOLUME_NEW":/var/lib/postgresql/data \
    "$PG17_IMAGE" \
    postgres \
        -c shared_preload_libraries='pg_stat_statements,pg_sage' \
        -c sage.database='postgres' \
        -c sage.collector_interval=3600 \
        -c sage.analyzer_interval=3600 \
        -c sage.trust_level='advisory'

wait_for_pg "$PG17_CONTAINER" 30
info "PG17 is ready."

PG17_VERSION=$(psql_exec "$PG17_CONTAINER" -c "SHOW server_version;")
info "PG17 version: $PG17_VERSION"

# -- Verify extension is loaded --
EXT_EXISTS=$(psql_exec "$PG17_CONTAINER" -c \
    "SELECT count(*) FROM pg_extension WHERE extname = 'pg_sage';")
if [ "$EXT_EXISTS" = "1" ]; then
    ok "pg_sage extension exists after upgrade"
else
    fail "pg_sage extension NOT found after upgrade"
fi

# -- Verify data integrity: findings --
NEW_FINDINGS=$(psql_exec "$PG17_CONTAINER" -c "SELECT count(*) FROM sage.findings;")
if [ "$NEW_FINDINGS" = "$COUNT_FINDINGS" ]; then
    ok "Findings count preserved ($NEW_FINDINGS)"
else
    fail "Findings count mismatch: expected $COUNT_FINDINGS, got $NEW_FINDINGS"
fi

# -- Verify data integrity: snapshots --
NEW_SNAPSHOTS=$(psql_exec "$PG17_CONTAINER" -c "SELECT count(*) FROM sage.snapshots;")
if [ "$NEW_SNAPSHOTS" = "$COUNT_SNAPSHOTS" ]; then
    ok "Snapshots count preserved ($NEW_SNAPSHOTS)"
else
    fail "Snapshots count mismatch: expected $COUNT_SNAPSHOTS, got $NEW_SNAPSHOTS"
fi

# -- Verify data integrity: action_log --
NEW_ACTIONS=$(psql_exec "$PG17_CONTAINER" -c "SELECT count(*) FROM sage.action_log;")
if [ "$NEW_ACTIONS" = "$COUNT_ACTIONS" ]; then
    ok "Action log count preserved ($NEW_ACTIONS)"
else
    fail "Action log count mismatch: expected $COUNT_ACTIONS, got $NEW_ACTIONS"
fi

# -- Verify config value survived --
NEW_CONFIG_VAL=$(psql_exec "$PG17_CONTAINER" -c \
    "SELECT value FROM sage.config WHERE key='snapshot_interval_seconds';")
if [ "$NEW_CONFIG_VAL" = "$CONFIG_VAL" ]; then
    ok "Config value preserved (snapshot_interval_seconds=$NEW_CONFIG_VAL)"
else
    fail "Config value mismatch: expected $CONFIG_VAL, got $NEW_CONFIG_VAL"
fi

# -- Verify specific finding data --
BLOAT_TITLE=$(psql_exec "$PG17_CONTAINER" -c \
    "SELECT title FROM sage.findings WHERE category='bloat' LIMIT 1;")
if [ "$BLOAT_TITLE" = "Table bloat > 40%" ]; then
    ok "Finding detail data intact"
else
    fail "Finding detail data incorrect: '$BLOAT_TITLE'"
fi

# -- Verify JSONB data in snapshots --
SNAP_CHECK=$(psql_exec "$PG17_CONTAINER" -c \
    "SELECT data->'load' FROM sage.snapshots WHERE category='system' LIMIT 1;")
if [ "$SNAP_CHECK" = "0.75" ]; then
    ok "Snapshot JSONB data intact"
else
    fail "Snapshot JSONB data incorrect: '$SNAP_CHECK'"
fi

# -- Verify C functions are callable --
# sage.status() should return JSONB without error
STATUS_CHECK=$(psql_exec "$PG17_CONTAINER" -c \
    "SELECT jsonb_typeof(sage.status());" 2>&1) || true
if [ "$STATUS_CHECK" = "object" ]; then
    ok "sage.status() callable and returns JSONB object"
else
    fail "sage.status() returned unexpected: '$STATUS_CHECK'"
fi

# sage.findings_json() should work
FINDINGS_JSON_TYPE=$(psql_exec "$PG17_CONTAINER" -c \
    "SELECT jsonb_typeof(sage.findings_json('open'));" 2>&1) || true
if [ "$FINDINGS_JSON_TYPE" = "array" ]; then
    ok "sage.findings_json() callable and returns JSONB array"
else
    fail "sage.findings_json() returned unexpected: '$FINDINGS_JSON_TYPE'"
fi

# sage.health_json() should work
HEALTH_JSON_TYPE=$(psql_exec "$PG17_CONTAINER" -c \
    "SELECT jsonb_typeof(sage.health_json());" 2>&1) || true
if [ "$HEALTH_JSON_TYPE" = "object" ]; then
    ok "sage.health_json() callable and returns JSONB object"
else
    fail "sage.health_json() returned unexpected: '$HEALTH_JSON_TYPE'"
fi

# -- Verify the schema and all expected tables exist --
TABLE_COUNT=$(psql_exec "$PG17_CONTAINER" -c \
    "SELECT count(*) FROM information_schema.tables
     WHERE table_schema = 'sage'
       AND table_type = 'BASE TABLE';")
if [ "$TABLE_COUNT" -ge 7 ]; then
    ok "All sage schema tables present ($TABLE_COUNT tables)"
else
    fail "Expected >= 7 sage tables, found $TABLE_COUNT"
fi

# -- Verify extension version --
EXT_VERSION=$(psql_exec "$PG17_CONTAINER" -c \
    "SELECT extversion FROM pg_extension WHERE extname = 'pg_sage';")
if [ "$EXT_VERSION" = "0.5.0" ]; then
    ok "Extension version is 0.5.0"
else
    fail "Extension version unexpected: '$EXT_VERSION'"
fi

# ---------------------------------------------------------------------------
# 8. Summary
# ---------------------------------------------------------------------------
echo ""
echo "============================================="
TOTAL=$((PASS + FAIL))
if [ "$FAIL" -eq 0 ]; then
    echo "RESULT: PASS  ($PASS/$TOTAL checks passed)"
    echo "pg_sage survived pg_upgrade from PG16 ($PG16_VERSION) to PG17 ($PG17_VERSION)"
    echo "============================================="
    exit 0
else
    echo "RESULT: FAIL  ($PASS passed, $FAIL failed out of $TOTAL)"
    echo "============================================="
    exit 1
fi
