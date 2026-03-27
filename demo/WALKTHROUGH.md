# pg_sage v0.8.0 -- Hands-On Walkthrough

This guide walks you through every v0.8.0 feature with a live demo environment.
Budget about 30 minutes for the full walkthrough, or 10 minutes for just the
core loop (Quick Start through Dashboard Tour).

---

## Table of Contents

1. [Prerequisites](#1-prerequisites)
2. [Quick Start (5 min)](#2-quick-start-5-min)
3. [The Demo Database](#3-the-demo-database)
4. [Watch Discovery (~2 min)](#4-watch-discovery-2-min)
5. [Dashboard Tour](#5-dashboard-tour)
6. [API Exploration](#6-api-exploration)
7. [Interactive Features](#7-interactive-features)
8. [LLM Features (optional)](#8-llm-features-optional)
9. [Executor in Action](#9-executor-in-action)
10. [Fleet Mode (optional)](#10-fleet-mode-optional)
11. [Prometheus Metrics](#11-prometheus-metrics)
12. [MCP Server](#12-mcp-server)
13. [Cleanup](#13-cleanup)

---

## 1. Prerequisites

| Requirement | Version | Check command |
|---|---|---|
| Docker + Docker Compose | 20.10+ | `docker compose version` |
| Go | 1.24+ | `go version` |
| Node.js + npm | 18+ | `node --version` |
| psql | any | `psql --version` |
| curl | any | `curl --version` |
| Gemini API key | -- | Optional, needed for Section 8 only |

If you only want to explore the REST API and dashboard (no LLM features), you
can skip the Gemini API key entirely. Tier 1 (deterministic rules engine) works
without any LLM.

Verify your setup:

```bash
docker compose version   # Docker Compose v2
go version               # go1.24+
node --version           # v18+
curl --version           # any
psql --version           # optional
```

---

## 2. Quick Start (5 min)

### Step 1: Start the demo PostgreSQL instance

From the repo root (`pg_sage/`):

```bash
docker compose -f demo/docker-compose-live.yml up -d
```

**What happens:** A PostgreSQL 17 container starts on **port 5433** with
`pg_stat_statements` enabled and `max=10000`. The `demo/init/` scripts run
automatically and create the `sage_agent` user, the `sage` schema, 7 demo
tables with planted problems, and a slow-query workload that seeds
`pg_stat_statements`.

Wait for healthy status (~15 seconds for the container, ~30 seconds for the
init scripts to finish inserting ~2.3 million rows):

```bash
docker compose -f demo/docker-compose-live.yml exec postgres pg_isready -U postgres
```

**Expected output:** `localhost:5432 - accepting connections`

### Step 2: Build the sidecar

```bash
cd sidecar

# Build the React dashboard (output goes to internal/api/dist/)
cd web && npm ci && npm run build && cd ..

# Build the Go binary (embeds the dashboard via go:embed)
go build -o pg_sage_sidecar ./cmd/pg_sage_sidecar/
```

**Timing:** ~20 seconds for npm, ~5 seconds for Go build.

### Step 3: Start the sidecar

**Without LLM** (Tier 1 rules engine only):

```bash
./pg_sage_sidecar --config ../demo/config-live.yaml
```

**With LLM features** (Tier 1 + Tier 2 optimizer/advisor):

```bash
SAGE_GEMINI_API_KEY=your-key-here ./pg_sage_sidecar --config ../demo/config-live.yaml
```

Or use the convenience script from the repo root:

```bash
SAGE_GEMINI_API_KEY=your-key-here ./demo/run-live.sh
```

**Expected startup logs:**

```
INF pg_sage sidecar starting version=vdev
INF connected to PostgreSQL host=localhost port=5433 database=sage_demo
INF schema bootstrapped
INF collector started interval=30s
INF analyzer started interval=60s
INF API server listening addr=0.0.0.0:8080
INF MCP server listening addr=0.0.0.0:5434
INF Prometheus metrics listening addr=0.0.0.0:9187
```

### Step 4: Open the dashboard

Open **http://localhost:8080** in your browser.

You should see the Fleet Overview page with one database (`sage_demo`) and a
health score. Findings start appearing after the first analyzer cycle (~60
seconds).

### Demo config reference

The demo uses `demo/config-live.yaml`. Key settings for the demo:

- **Trust:** `autonomous` with `ramp_start: "2025-01-01T00:00:00Z"` so the
  executor can act immediately (bypasses the normal 31-day ramp).
- **Collector:** Every 30 seconds (production default: 60s).
- **Analyzer:** Every 60 seconds (production default: 120s).
- **`unused_index_window_days: 0`:** Flags unused indexes immediately. In
  production, use 7+ to avoid flagging newly created indexes.
- **`maintenance_window: "* * * * *"`:** Executor can act any time.

---

## 3. The Demo Database

The Docker Compose init scripts (`demo/init/01_setup.sql` and
`demo/init/02_demo_data.sql`) create a realistic e-commerce schema with
**7 pre-planted problems** for pg_sage to discover.

### Problem 1: Missing Foreign Key Index

The `orders` table (500K rows) has a foreign key
`customer_id -> customers(customer_id)` but **no index on
`orders.customer_id`**. Joins and lookups on this column require sequential
scans.

### Problem 2: Duplicate Indexes

The `line_items` table (1M rows) has two identical btree indexes on
`order_id`:

- `idx_li_order_id`
- `idx_li_order_id_dup`

One is pure waste -- same definition, double the storage and write overhead.

### Problem 3: Unused Index

`idx_li_product_name` on `line_items(product_name)` exists but no queries in
the workload use it. It costs write overhead on every INSERT/UPDATE.

### Problem 4: Missing Indexes on Unindexed Table

`order_events` (500K rows) has no primary key and no indexes at all. With
queries filtering on `order_id`, every lookup is a full sequential scan.

### Problem 5: Table Bloat (Dead Tuples)

The `audit_log` table has 100K rows inserted, then 33% updated and 20%
deleted -- creating a high dead tuple ratio (~40%+) that needs vacuuming.

### Problem 6: Near-Exhausted Sequence

`demo_sequence` is an INTEGER sequence starting at 2,147,483,600 with a max
of 2,147,483,647. After the init script calls `nextval` 10 times, it is at
99.9999995% capacity. This is a **critical** finding.

### Problem 7: Slow Query Workload

The init script runs 20 iterations of 8 query patterns plus standalone
executions to populate `pg_stat_statements`. These include:

- Sequential scans on large tables (`orders WHERE customer_id = ?`)
- Cross joins (`customers c1 CROSS JOIN customers c2`)
- Function-wrapped column filters (`EXTRACT(YEAR FROM created_at)`)
- Subquery anti-patterns (`WHERE customer_id IN (SELECT ...)`)

### Planting problems manually

If you are running against your own PostgreSQL instance instead of the demo
Docker container, here is the SQL to set up the environment:

```sql
-- === Run as superuser ===

CREATE EXTENSION IF NOT EXISTS pg_stat_statements;

CREATE USER sage_agent WITH PASSWORD 'sage_password';
GRANT pg_monitor TO sage_agent;
GRANT pg_read_all_stats TO sage_agent;
GRANT CREATE ON DATABASE your_db TO sage_agent;

CREATE SCHEMA IF NOT EXISTS sage AUTHORIZATION sage_agent;
GRANT ALL ON SCHEMA sage TO sage_agent;
GRANT ALL ON SCHEMA public TO sage_agent;
ALTER DEFAULT PRIVILEGES IN SCHEMA sage GRANT ALL ON TABLES TO sage_agent;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO sage_agent;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT USAGE ON SEQUENCES TO sage_agent;
ALTER DEFAULT PRIVILEGES IN SCHEMA sage GRANT USAGE ON SEQUENCES TO sage_agent;
```

Then run the demo data script to create tables and plant all 7 problems:

```sql
-- === Problem 1: Baseline + Missing FK Index ===

CREATE TABLE customers (
    customer_id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    email VARCHAR(255) UNIQUE NOT NULL,
    status VARCHAR(20) DEFAULT 'active',
    created_at TIMESTAMP DEFAULT now()
);

INSERT INTO customers (name, email, status)
SELECT 'Customer ' || i,
       'customer' || i || '@example.com',
       CASE WHEN random() < 0.9 THEN 'active' ELSE 'inactive' END
FROM generate_series(1, 50000) i;

CREATE TABLE orders (
    order_id SERIAL PRIMARY KEY,
    customer_id INT REFERENCES customers(customer_id),
    order_date TIMESTAMP DEFAULT now(),
    total_amount DECIMAL(10,2),
    status VARCHAR(20) DEFAULT 'pending'
);
-- NOTE: No index on orders.customer_id -- this is the bug.

INSERT INTO orders (customer_id, order_date, total_amount, status)
SELECT (random() * 49999 + 1)::int,
       now() - (random() * 365 || ' days')::interval,
       (random() * 500 + 10)::decimal(10,2),
       CASE (random() * 4)::int
           WHEN 0 THEN 'pending' WHEN 1 THEN 'shipped'
           WHEN 2 THEN 'delivered' ELSE 'cancelled'
       END
FROM generate_series(1, 500000) i;

-- === Problem 2 & 3: Duplicate + Unused Indexes ===

CREATE TABLE line_items (
    item_id SERIAL PRIMARY KEY,
    order_id INT REFERENCES orders(order_id),
    product_name VARCHAR(200),
    quantity INT,
    unit_price DECIMAL(10,2)
);

INSERT INTO line_items (order_id, product_name, quantity, unit_price)
SELECT (random() * 499999 + 1)::int,
       'Product ' || (random() * 1000)::int,
       (random() * 10 + 1)::int,
       (random() * 100 + 1)::decimal(10,2)
FROM generate_series(1, 1000000) i;

CREATE INDEX idx_li_order_id ON line_items(order_id);
CREATE INDEX idx_li_order_id_dup ON line_items(order_id);  -- DUPLICATE
CREATE INDEX idx_li_product_name ON line_items(product_name);  -- UNUSED

-- === Problem 4: Unindexed table ===

CREATE TABLE order_events (
    order_id INT,
    event_type VARCHAR(50),
    event_data JSONB,
    created_at TIMESTAMP DEFAULT now()
);
-- NOTE: No primary key, no indexes at all.

INSERT INTO order_events (order_id, event_type, event_data, created_at)
SELECT (random() * 499999 + 1)::int,
       CASE (random() * 5)::int
           WHEN 0 THEN 'created' WHEN 1 THEN 'payment_received'
           WHEN 2 THEN 'shipped' WHEN 3 THEN 'delivered'
           ELSE 'cancelled'
       END,
       jsonb_build_object('source', 'demo', 'seq', i),
       now() - (random() * 365 || ' days')::interval
FROM generate_series(1, 500000) i;

-- === Problem 5: Dead tuple bloat ===

CREATE TABLE audit_log (
    id SERIAL PRIMARY KEY,
    table_name VARCHAR(100),
    action VARCHAR(20),
    old_data JSONB,
    new_data JSONB,
    created_at TIMESTAMP DEFAULT now()
);

INSERT INTO audit_log (table_name, action, old_data, new_data)
SELECT CASE (random() * 3)::int
           WHEN 0 THEN 'customers' WHEN 1 THEN 'orders'
           ELSE 'line_items'
       END,
       CASE (random() * 2)::int WHEN 0 THEN 'UPDATE' ELSE 'INSERT' END,
       '{"before": true}'::jsonb,
       '{"after": true}'::jsonb
FROM generate_series(1, 100000) i;

UPDATE audit_log SET action = 'MODIFIED' WHERE id % 3 = 0;
DELETE FROM audit_log WHERE id % 5 = 0;

-- === Problem 6: Near-exhausted sequence ===

CREATE SEQUENCE demo_sequence AS INTEGER
    MAXVALUE 2147483647 START WITH 2147483600;
SELECT nextval('demo_sequence') FROM generate_series(1, 10);

-- === Problem 7: Slow query workload ===

DO $$
BEGIN
    FOR i IN 1..20 LOOP
        PERFORM count(*) FROM orders
            WHERE customer_id = (random() * 49999 + 1)::int;
        PERFORM count(*) FROM orders o
            JOIN customers c ON o.customer_id = c.customer_id
            WHERE c.status = 'active' AND o.total_amount > 100;
        PERFORM count(*) FROM order_events
            WHERE order_id = (random() * 499999 + 1)::int;
        PERFORM order_id FROM orders ORDER BY order_date DESC LIMIT 100;
        PERFORM count(*) FROM (
            SELECT 1 FROM customers c1 CROSS JOIN customers c2 LIMIT 1000
        ) sub;
        PERFORM count(*) FROM customers
            WHERE EXTRACT(YEAR FROM created_at) = 2025;
        PERFORM * FROM line_items
            WHERE order_id = (random() * 499999 + 1)::int LIMIT 10;
        PERFORM count(*) FROM orders WHERE customer_id IN (
            SELECT customer_id FROM customers WHERE status = 'inactive'
        );
    END LOOP;
END $$;

ANALYZE;

-- Transfer ownership so the executor can CREATE/DROP indexes
ALTER TABLE customers OWNER TO sage_agent;
ALTER TABLE orders OWNER TO sage_agent;
ALTER TABLE line_items OWNER TO sage_agent;
ALTER TABLE order_events OWNER TO sage_agent;
ALTER TABLE audit_log OWNER TO sage_agent;
```

---

## 4. Watch Discovery (~2 min)

After starting the sidecar, watch the logs. Here is what happens in order:

### First 30 seconds: Collector runs

```
INF collector snapshot complete tables=7 indexes=12 queries=15 sequences=6
```

The collector snapshots `pg_stat_statements`, `pg_stat_user_tables`,
`pg_stat_user_indexes`, sequences, and system stats. Everything is stored in
`sage.snapshots`.

### At ~60 seconds: Analyzer runs

The Tier 1 rules engine processes the snapshots and generates findings:

```
INF analyzer cycle complete findings_new=7 findings_total=7
```

You will see individual finding logs:

```
INF finding category=index_health severity=critical title="Duplicate index: idx_li_order_id_dup"
INF finding category=sequence severity=critical title="Sequence demo_sequence near exhaustion"
INF finding category=index_health severity=warning title="Missing FK index on orders.customer_id"
INF finding category=index_health severity=warning title="Unused index: idx_li_product_name"
INF finding category=vacuum severity=warning title="High dead tuple ratio on audit_log"
INF finding category=query_performance severity=info title="Sequential scan on large table"
```

### At ~60 seconds: Executor acts (if trust = autonomous)

With the demo config (`trust.level: autonomous`, `ramp_start` in the past),
the executor immediately acts on SAFE and MODERATE risk findings:

```
INF executor action=drop_duplicate_index target=idx_li_order_id_dup outcome=success
INF executor action=create_index target=orders(customer_id) outcome=success
```

Each action is logged to `sage.action_log` with the SQL executed, rollback SQL,
and before/after state.

### After 2-3 minutes: Steady state

After 2-3 analyzer cycles, expect roughly 12-15 findings across categories:
- `duplicate_index` (critical)
- `sequence_exhaustion` (critical)
- `unused_index` (warning)
- `missing_fk_index` (warning)
- `dead_tuples` / `table_bloat` (warning)
- `slow_query` (warning/info)
- `config` (warning/info)

If you set a Gemini API key, LLM-powered optimizer and advisor findings also
appear after ~2 minutes.

### Refresh the dashboard

Go to http://localhost:8080 and refresh. You should see:
- Health score for `sage_demo` (likely 60-80 due to the critical sequence)
- Multiple findings in the findings list
- Actions in the actions list (if executor ran)

---

## 5. Dashboard Tour

The React dashboard is a dark-themed SPA with 5 pages accessible from the
sidebar. All data refreshes via polling (every few seconds).

### Fleet Overview (home page)

**URL:** http://localhost:8080

**What you see:**
- **Stat cards at the top:** Database count (1), Healthy count, Degraded count,
  Critical Findings count
- **Database list:** Shows `sage_demo` with connection status, PostgreSQL
  version (17.x), health score (0-100), and finding counts by severity
- **Recent findings preview:** The 5 most recent findings with severity badges

Watch the stat cards auto-refresh as the analyzer discovers more issues.

### Findings Page

**URL:** http://localhost:8080/findings

**What you see:**
- List of all findings, sorted by severity (critical first by default)
- Each finding shows: severity badge, title, category, occurrence count, age
- **Filter controls:** severity dropdown, status tabs (open/suppressed/resolved)
- **Expandable rows:** Click any finding to see the full detail panel:
  - `recommended_sql` -- the exact SQL to fix the problem
  - `rollback_sql` -- how to undo the fix
  - `detail` -- full diagnostic JSONB (varies by finding type)
  - `category` and `object_identifier` -- which table/index is affected

Try filtering to `critical` severity. You should see the duplicate index and
sequence exhaustion findings.

### Actions Page

**URL:** http://localhost:8080/actions

**What you see:**
- List of all executor actions, newest first
- Each action shows: timestamp, action type, SQL executed, outcome badge
- **Expandable detail:** Click a row to see:
  - `sql_executed` -- the exact DDL that ran
  - `rollback_sql` -- how to reverse it
  - `before_state` / `after_state` -- what changed (JSON)
  - `finding_id` -- links back to the finding that triggered this action

In autonomous trust mode, you should see actions within 2-3 minutes.

### Database Detail Page

**URL:** http://localhost:8080/database/sage_demo

**What you see:**
- Time-series charts: cache hit ratio, connections, TPS, dead tuples
- Database size over time
- Filtered findings and actions for this specific database
- Configuration summary (trust level, executor status, LLM status)

### Settings Page

**URL:** http://localhost:8080/settings

**What you see:**
- Current configuration as JSON (trust level, LLM settings, analyzer thresholds)
- **Emergency Stop button:** Big red button that halts all autonomous actions
- **Resume button:** Appears after an emergency stop

---

## 6. API Exploration

All endpoints are under `http://localhost:8080/api/v1/`. Every GET endpoint
accepts an optional `?database=` parameter for fleet-mode filtering.

### 6.1 GET /api/v1/databases

Fleet overview with health scores.

```bash
curl -s http://localhost:8080/api/v1/databases | python -m json.tool
```

**Expected response:**

```json
{
    "mode": "standalone",
    "summary": {
        "total_databases": 1,
        "healthy": 1,
        "degraded": 0,
        "total_findings": 7,
        "total_critical": 2
    },
    "databases": [
        {
            "name": "sage_demo",
            "tags": null,
            "status": {
                "database_name": "sage_demo",
                "connected": true,
                "health_score": 70,
                "findings_open": 5,
                "findings_critical": 1,
                "last_collection": "2026-03-26T10:00:30Z",
                "last_analysis": "2026-03-26T10:01:00Z",
                "error": ""
            }
        }
    ]
}
```

### 6.2 GET /api/v1/findings

List findings with filtering. Defaults to `status=open`, sorted by severity
descending.

```bash
# All open findings
curl -s 'http://localhost:8080/api/v1/findings' | python -m json.tool

# Only critical findings
curl -s 'http://localhost:8080/api/v1/findings?severity=critical' | python -m json.tool

# Only warnings, limit 5
curl -s 'http://localhost:8080/api/v1/findings?severity=warning&limit=5' | python -m json.tool

# Filter by category
curl -s 'http://localhost:8080/api/v1/findings?category=index_health' | python -m json.tool

# Suppressed findings
curl -s 'http://localhost:8080/api/v1/findings?status=suppressed' | python -m json.tool

# Pagination
curl -s 'http://localhost:8080/api/v1/findings?limit=10&offset=0' | python -m json.tool
```

**Expected response (abbreviated):**

```json
{
    "database": "sage_demo",
    "filters": {
        "status": "open",
        "severity": "",
        "category": "",
        "sort": "severity",
        "order": "desc",
        "limit": 50,
        "offset": 0
    },
    "total": 7,
    "offset": 0,
    "limit": 50,
    "findings": [
        {
            "id": "1",
            "created_at": "2026-03-26T10:01:00Z",
            "last_seen": "2026-03-26T10:01:00Z",
            "occurrence_count": 1,
            "category": "sequence",
            "severity": "critical",
            "object_type": "sequence",
            "object_identifier": "public.demo_sequence",
            "title": "Sequence demo_sequence near exhaustion (99.99%)",
            "detail": {"current_value": 2147483610, "max_value": 2147483647},
            "recommendation": "ALTER SEQUENCE to BIGINT or reset",
            "recommended_sql": "ALTER SEQUENCE demo_sequence AS BIGINT;",
            "status": "open",
            "database_name": "sage_demo"
        },
        {
            "id": "2",
            "category": "index_health",
            "severity": "critical",
            "title": "Duplicate index: idx_li_order_id matches idx_li_order_id_dup",
            "object_identifier": "public.idx_li_order_id_dup",
            "recommended_sql": "DROP INDEX CONCURRENTLY idx_li_order_id_dup;",
            "status": "open",
            "database_name": "sage_demo"
        }
    ]
}
```

### 6.3 GET /api/v1/findings/:id

Full detail for a single finding, including rollback SQL and action linkage.

```bash
# Replace 1 with an actual finding ID from the list above
curl -s http://localhost:8080/api/v1/findings/1 | python -m json.tool
```

**Expected response:**

```json
{
    "id": "1",
    "created_at": "2026-03-26T10:01:00Z",
    "last_seen": "2026-03-26T10:01:00Z",
    "occurrence_count": 1,
    "category": "sequence",
    "severity": "critical",
    "object_type": "sequence",
    "object_identifier": "public.demo_sequence",
    "title": "Sequence demo_sequence near exhaustion (99.99%)",
    "detail": {"current_value": 2147483610, "max_value": 2147483647},
    "recommendation": "ALTER SEQUENCE to BIGINT or reset",
    "recommended_sql": "ALTER SEQUENCE demo_sequence AS BIGINT;",
    "rollback_sql": "ALTER SEQUENCE demo_sequence AS INTEGER;",
    "estimated_cost_usd": null,
    "status": "open",
    "suppressed_until": null,
    "resolved_at": null,
    "acted_on_at": null,
    "action_log_id": null
}
```

The detail view includes fields not shown in the list view: `rollback_sql`,
`estimated_cost_usd`, `suppressed_until`, `resolved_at`, `acted_on_at`, and
`action_log_id` (which links to the action that resolved this finding).

### 6.4 POST /api/v1/findings/:id/suppress

Suppress a finding (hide it from the default open view).

```bash
curl -s -X POST http://localhost:8080/api/v1/findings/1/suppress \
  | python -m json.tool
```

**Expected response:**

```json
{
    "ok": true,
    "id": "1",
    "status": "suppressed"
}
```

### 6.5 POST /api/v1/findings/:id/unsuppress

Re-open a suppressed finding.

```bash
curl -s -X POST http://localhost:8080/api/v1/findings/1/unsuppress \
  | python -m json.tool
```

**Expected response:**

```json
{
    "ok": true,
    "id": "1",
    "status": "open"
}
```

### 6.6 GET /api/v1/actions

List all executor actions, newest first.

```bash
curl -s 'http://localhost:8080/api/v1/actions' | python -m json.tool

# Pagination
curl -s 'http://localhost:8080/api/v1/actions?limit=10&offset=0' | python -m json.tool
```

**Expected response (abbreviated):**

```json
{
    "database": "sage_demo",
    "total": 2,
    "offset": 0,
    "limit": 50,
    "actions": [
        {
            "id": "1",
            "executed_at": "2026-03-26T10:01:05Z",
            "action_type": "drop_duplicate_index",
            "finding_id": "2",
            "sql_executed": "DROP INDEX CONCURRENTLY IF EXISTS idx_li_order_id_dup;",
            "rollback_sql": "CREATE INDEX CONCURRENTLY idx_li_order_id_dup ON line_items (order_id);",
            "before_state": {"index_size": "21 MB", "scans": 0},
            "after_state": {"index_dropped": true},
            "outcome": "success",
            "rollback_reason": "",
            "measured_at": null
        },
        {
            "id": "2",
            "executed_at": "2026-03-26T10:01:10Z",
            "action_type": "create_index",
            "finding_id": "3",
            "sql_executed": "CREATE INDEX CONCURRENTLY idx_orders_customer_id ON orders (customer_id);",
            "rollback_sql": "DROP INDEX CONCURRENTLY IF EXISTS idx_orders_customer_id;",
            "before_state": null,
            "after_state": {"index_created": true},
            "outcome": "success",
            "rollback_reason": "",
            "measured_at": null
        }
    ]
}
```

### 6.7 GET /api/v1/actions/:id

Full detail for a single action.

```bash
curl -s http://localhost:8080/api/v1/actions/1 | python -m json.tool
```

**Expected response:** Same shape as an individual action from the list, with
all fields populated including `before_state` and `after_state` JSON.

### 6.8 GET /api/v1/snapshots/latest

Latest collector snapshot. The `metric` parameter selects the category.

```bash
# Default: cache_hit_ratio
curl -s 'http://localhost:8080/api/v1/snapshots/latest' | python -m json.tool

# Table stats
curl -s 'http://localhost:8080/api/v1/snapshots/latest?metric=tables' | python -m json.tool

# Index stats
curl -s 'http://localhost:8080/api/v1/snapshots/latest?metric=indexes' | python -m json.tool

# Query stats (pg_stat_statements data)
curl -s 'http://localhost:8080/api/v1/snapshots/latest?metric=queries' | python -m json.tool

# Sequences
curl -s 'http://localhost:8080/api/v1/snapshots/latest?metric=sequences' | python -m json.tool

# System stats
curl -s 'http://localhost:8080/api/v1/snapshots/latest?metric=system' | python -m json.tool
```

**Valid metric values:** `tables`, `indexes`, `queries`, `sequences`,
`foreign_keys`, `system`, `io`, `locks`, `config_data`, `partitions`,
`cache_hit_ratio`, `connections`, `tps`, `dead_tuples`, `database_size`,
`replication_lag`.

**Expected response:**

```json
{
    "database": "sage_demo",
    "snapshot": {
        "cache_hit_ratio": 0.9987,
        "collected_at": "2026-03-26T10:00:30Z"
    }
}
```

### 6.9 GET /api/v1/snapshots/history

Time-series data for charts. Requires a `metric` parameter.

```bash
# Last hour of cache hit ratio
curl -s 'http://localhost:8080/api/v1/snapshots/history?metric=cache_hit_ratio&hours=1' \
  | python -m json.tool

# Last 24 hours of dead tuples
curl -s 'http://localhost:8080/api/v1/snapshots/history?metric=dead_tuples&hours=24' \
  | python -m json.tool
```

**Expected response:**

```json
{
    "database": "sage_demo",
    "metric": "cache_hit_ratio",
    "points": [
        {"timestamp": "2026-03-26T09:30:30Z", "data": {"cache_hit_ratio": 0.998}},
        {"timestamp": "2026-03-26T10:00:30Z", "data": {"cache_hit_ratio": 0.999}}
    ]
}
```

### 6.10 GET /api/v1/config

Current sidecar configuration.

```bash
curl -s http://localhost:8080/api/v1/config | python -m json.tool
```

**Expected response:**

```json
{
    "mode": "standalone",
    "trust": {
        "level": "autonomous",
        "ramp_start": "2025-01-01T00:00:00Z",
        "tier3_safe": true,
        "tier3_moderate": true,
        "maintenance_window": "* * * * *"
    },
    "collector": {"interval_seconds": 30},
    "analyzer": {
        "interval_seconds": 60,
        "slow_query_threshold_ms": 500,
        "seq_scan_min_rows": 10000,
        "unused_index_window_days": 0,
        "table_bloat_dead_tuple_pct": 10
    },
    "safety": null,
    "llm_enabled": true,
    "advisor": {"enabled": true, "interval_seconds": 120},
    "databases": 0
}
```

You can also get per-database config in fleet mode:

```bash
curl -s 'http://localhost:8080/api/v1/config?database=sage_demo' | python -m json.tool
```

### 6.11 PUT /api/v1/config

Hot-reload configuration. Currently supports changing the trust level at
runtime without restarting the sidecar.

```bash
curl -s -X PUT http://localhost:8080/api/v1/config \
  -H 'Content-Type: application/json' \
  -d '{"trust": {"level": "observation"}}' | python -m json.tool
```

**Expected response:**

```json
{
    "status": "updated"
}
```

**Valid trust levels:** `observation`, `advisory`, `autonomous`.

### 6.12 GET /api/v1/metrics

JSON metrics for the fleet (or a specific database).

```bash
# Fleet-wide metrics
curl -s http://localhost:8080/api/v1/metrics | python -m json.tool

# Per-database metrics
curl -s 'http://localhost:8080/api/v1/metrics?database=sage_demo' | python -m json.tool
```

**Expected response (fleet-wide):**

```json
{
    "fleet": {
        "total_databases": 1,
        "healthy": 1,
        "degraded": 0,
        "total_findings": 7,
        "total_critical": 2
    },
    "databases": [
        {
            "name": "sage_demo",
            "tags": null,
            "status": {
                "database_name": "sage_demo",
                "connected": true,
                "health_score": 70,
                "findings_open": 5,
                "findings_critical": 1
            }
        }
    ]
}
```

**Expected response (per-database):**

```json
{
    "database": "sage_demo",
    "status": {
        "database_name": "sage_demo",
        "connected": true,
        "health_score": 70,
        "findings_open": 5,
        "findings_critical": 1
    }
}
```

### 6.13 POST /api/v1/emergency-stop

Halt all autonomous actions immediately. The executor stops acting on findings.
Collector and analyzer continue running -- you retain full visibility.

```bash
# Stop all databases
curl -s -X POST http://localhost:8080/api/v1/emergency-stop | python -m json.tool

# Stop a specific database (fleet mode)
curl -s -X POST 'http://localhost:8080/api/v1/emergency-stop?database=sage_demo' \
  | python -m json.tool
```

**Expected response:**

```json
{
    "stopped": 1,
    "status": "stopped"
}
```

### 6.14 POST /api/v1/resume

Resume autonomous actions after an emergency stop.

```bash
# Resume all databases
curl -s -X POST http://localhost:8080/api/v1/resume | python -m json.tool

# Resume a specific database (fleet mode)
curl -s -X POST 'http://localhost:8080/api/v1/resume?database=sage_demo' \
  | python -m json.tool
```

**Expected response:**

```json
{
    "resumed": 1,
    "status": "resumed"
}
```

---

## 7. Interactive Features

### Emergency Stop and Resume

This is the most important safety feature. Try it end-to-end:

1. **Trigger the stop:**

   ```bash
   curl -s -X POST http://localhost:8080/api/v1/emergency-stop | python -m json.tool
   ```

   **Timing:** Immediate effect.

2. **Check the sidecar logs.** You should see:

   ```
   WRN emergency stop activated databases=1
   ```

3. **Check the dashboard.** The database card should show a stopped/paused
   indicator.

4. **Verify the executor is halted.** Wait for an analyzer cycle (~60 seconds).
   New findings will appear but **no new actions** will be taken:

   ```bash
   curl -s 'http://localhost:8080/api/v1/actions?limit=1' | python -m json.tool
   ```

   The action count should not increase after the stop.

5. **Resume:**

   ```bash
   curl -s -X POST http://localhost:8080/api/v1/resume | python -m json.tool
   ```

   **Expected log:**

   ```
   INF resumed after emergency stop databases=1
   ```

### Suppress and Unsuppress a Finding

Suppressing a finding tells pg_sage "I know about this, stop showing it in the
default view."

1. **List findings and pick one:**

   ```bash
   curl -s 'http://localhost:8080/api/v1/findings?limit=3' | python -m json.tool
   ```

   Note a finding ID (e.g., `"id": "2"`).

2. **Suppress it:**

   ```bash
   curl -s -X POST http://localhost:8080/api/v1/findings/2/suppress \
     | python -m json.tool
   ```

3. **Verify it is gone from the default view:**

   ```bash
   curl -s 'http://localhost:8080/api/v1/findings' | python -m json.tool
   ```

   The suppressed finding will not appear (default filter is `status=open`).

4. **See it in the suppressed list:**

   ```bash
   curl -s 'http://localhost:8080/api/v1/findings?status=suppressed' \
     | python -m json.tool
   ```

5. **Unsuppress it:**

   ```bash
   curl -s -X POST http://localhost:8080/api/v1/findings/2/unsuppress \
     | python -m json.tool
   ```

6. **Refresh the dashboard** -- the finding reappears in the open list.

### Config Hot-Reload

Change the trust level at runtime without restarting the sidecar:

1. **Check current config:**

   ```bash
   curl -s http://localhost:8080/api/v1/config | python -m json.tool
   ```

2. **Downgrade to observation mode (findings only, no actions):**

   ```bash
   curl -s -X PUT http://localhost:8080/api/v1/config \
     -H 'Content-Type: application/json' \
     -d '{"trust": {"level": "observation"}}' | python -m json.tool
   ```

3. **Verify:**

   ```bash
   curl -s http://localhost:8080/api/v1/config | python -m json.tool
   ```

   The `trust.level` field should now read `"observation"`.

4. **Restore autonomous mode:**

   ```bash
   curl -s -X PUT http://localhost:8080/api/v1/config \
     -H 'Content-Type: application/json' \
     -d '{"trust": {"level": "autonomous"}}' | python -m json.tool
   ```

---

## 8. LLM Features (optional)

**Requires:** A Gemini API key set as `SAGE_GEMINI_API_KEY` when starting
the sidecar. If you started without one, stop the sidecar (Ctrl+C) and
restart with the key set.

pg_sage supports any OpenAI-compatible LLM endpoint (Gemini, OpenAI, Groq,
Ollama, etc.). The demo config uses Gemini.

### Index Optimizer

The LLM-powered optimizer analyzes `pg_stat_statements` query patterns and
recommends indexes with confidence scores and detailed rationale. It goes
beyond the rules engine -- it can suggest composite indexes, covering indexes,
and partial indexes based on actual query workload.

**What to look for in logs:**

```
INF optimizer cycle started
INF optimizer recommendation table=orders index="CREATE INDEX ON orders (customer_id, status)" confidence=0.92
INF optimizer cycle complete recommendations=2
```

**Check optimizer findings:**

```bash
curl -s 'http://localhost:8080/api/v1/findings?category=index_optimization' \
  | python -m json.tool
```

Optimizer findings include an `llm_rationale` field in their `detail` JSON
explaining why the index was recommended.

The optimizer uses 8 validators (including HypoPG validation when available)
and assigns confidence scores from 0.0 to 1.0. Only recommendations above
the confidence threshold are surfaced.

### Configuration Advisor

The advisor analyzes your PostgreSQL configuration and recommends tuning
changes across 4 categories:

| Category | What It Analyzes |
|---|---|
| **Vacuum** | `autovacuum_vacuum_threshold`, `autovacuum_vacuum_scale_factor`, etc. |
| **WAL** | `wal_buffers`, `checkpoint_completion_target`, `max_wal_size` |
| **Connections** | `max_connections` vs. actual usage, `idle_in_transaction_session_timeout` |
| **Memory** | `shared_buffers`, `work_mem`, `effective_cache_size`, `maintenance_work_mem` |

**What to look for in logs (runs every 120s with demo config):**

```
INF advisor cycle started
INF advisor recommendation category=vacuum setting=autovacuum_vacuum_scale_factor current=0.2 recommended=0.1
INF advisor recommendation category=memory setting=work_mem current=4MB recommended=16MB
INF advisor cycle complete recommendations=4
```

**Check advisor findings:**

```bash
curl -s 'http://localhost:8080/api/v1/findings?category=memory_tuning' \
  | python -m json.tool
curl -s 'http://localhost:8080/api/v1/findings?category=vacuum_tuning' \
  | python -m json.tool
curl -s 'http://localhost:8080/api/v1/findings?category=connection_tuning' \
  | python -m json.tool
curl -s 'http://localhost:8080/api/v1/findings?category=wal_tuning' \
  | python -m json.tool
```

Each advisor finding includes `ALTER SYSTEM` recommendations and rationale.

### Health Briefings

Briefings are available via the MCP server's `briefing` tool (see Section 12).
When the LLM is enabled, the briefing includes natural-language narrative
summarizing the database state, top concerns, and recommended actions.

---

## 9. Executor in Action

The executor is pg_sage's hands. It takes findings and acts on them, respecting
the trust ramp and maintenance window.

### Trust Levels

| Level | Day Range | What It Does |
|---|---|---|
| observation | 0-7 | Findings only. Zero SQL executed. |
| advisory | 8-30 | SAFE actions (drop unused/duplicate indexes). |
| autonomous | 31+ | MODERATE actions (create indexes, vacuum). |

HIGH-risk actions (e.g., `ALTER SEQUENCE`, schema changes) **always** require
manual confirmation regardless of trust level.

The demo config skips the ramp by setting `ramp_start: "2025-01-01T00:00:00Z"`.

### What the executor does in the demo

After the first analyzer cycle, check the actions list:

```bash
curl -s 'http://localhost:8080/api/v1/actions' | python -m json.tool
```

Common actions you will see:

1. **Drop duplicate index** (`idx_li_order_id_dup`) -- SAFE risk. The executor
   identifies the duplicate and drops it with `DROP INDEX CONCURRENTLY`.

2. **Create missing FK index** (`orders(customer_id)`) -- MODERATE risk. The
   executor creates the index with `CREATE INDEX CONCURRENTLY`.

3. **VACUUM bloated table** (`audit_log`) -- MODERATE risk. Routed through a
   non-transaction connection (pgxpool wraps in tx by default; VACUUM cannot
   run inside a transaction).

### Examining an action in detail

```bash
curl -s http://localhost:8080/api/v1/actions/1 | python -m json.tool
```

**Key fields to examine:**

- `sql_executed` -- the exact DDL that was run
- `rollback_sql` -- the SQL to undo it (e.g., `CREATE INDEX CONCURRENTLY ...`
  for a dropped index)
- `before_state` -- JSON snapshot of the object before the change (index size,
  scan count, etc.)
- `after_state` -- JSON snapshot after the change
- `outcome` -- `success` or `failed`
- `finding_id` -- links back to the finding that triggered this action

### Verifying changes with psql

Connect to the demo database and check what changed:

```bash
psql -h localhost -p 5433 -U sage_agent -d sage_demo
```

```sql
-- Check if the duplicate index was dropped
SELECT indexname FROM pg_indexes WHERE tablename = 'line_items';
-- Expected: line_items_pkey, idx_li_order_id (NOT idx_li_order_id_dup)

-- Check if the FK index was created
SELECT indexname FROM pg_indexes WHERE tablename = 'orders';
-- Expected: orders_pkey, plus a new index on customer_id

-- Check the action log directly
SELECT id, action_type, sql_executed, outcome
FROM sage.action_log ORDER BY executed_at DESC;

-- Check dead tuples on audit_log (should be lower if VACUUM ran)
SELECT relname, n_dead_tup, n_live_tup,
       round(n_dead_tup::numeric / NULLIF(n_live_tup, 0) * 100, 1) AS dead_pct
FROM pg_stat_user_tables
WHERE relname = 'audit_log';
```

---

## 10. Fleet Mode (optional)

Fleet mode lets one sidecar binary monitor N databases simultaneously. Each
database gets its own collector, analyzer, and executor goroutines.

### Setting up a second database

To try fleet mode, you need a second PostgreSQL instance. The simplest approach
is to create a second database in the same container:

```bash
psql -h localhost -p 5433 -U postgres -c "CREATE DATABASE sage_demo_2;"
psql -h localhost -p 5433 -U postgres -d sage_demo_2 \
  -f demo/init/01_setup.sql
psql -h localhost -p 5433 -U postgres -d sage_demo_2 \
  -f demo/init/02_demo_data.sql
```

### Fleet config example

Create `demo/config-fleet.yaml`:

```yaml
mode: fleet

defaults:
  max_connections: 3
  trust_level: observation
  collector_interval_seconds: 30
  analyzer_interval_seconds: 120

databases:
  - name: demo-primary
    host: localhost
    port: 5433
    user: sage_agent
    password: sage_password
    database: sage_demo
    sslmode: disable
    tags: [demo, primary]
    trust_level: autonomous

  - name: demo-secondary
    host: localhost
    port: 5433
    user: sage_agent
    password: sage_password
    database: sage_demo_2
    sslmode: disable
    tags: [demo, secondary]
    trust_level: observation
    executor_enabled: false

llm:
  enabled: false

mcp:
  enabled: true
  listen_addr: "0.0.0.0:5434"

prometheus:
  listen_addr: "0.0.0.0:9187"

api:
  listen_addr: "0.0.0.0:8080"
```

Start the sidecar with the fleet config:

```bash
cd sidecar
./pg_sage_sidecar --config ../demo/config-fleet.yaml
```

### Fleet-specific features to try

**Per-database health scores.** Each database gets its own score (0-100):

```bash
curl -s http://localhost:8080/api/v1/databases | python -m json.tool
```

The `databases` array will have two entries. Databases are sorted worst-first
(lowest health score at the top).

**Per-database filtering.** Every API endpoint accepts `?database=`:

```bash
curl -s 'http://localhost:8080/api/v1/findings?database=demo-primary' \
  | python -m json.tool

curl -s 'http://localhost:8080/api/v1/findings?database=demo-secondary' \
  | python -m json.tool
```

**Per-database emergency stop:**

```bash
# Stop only demo-primary, leave demo-secondary running
curl -s -X POST 'http://localhost:8080/api/v1/emergency-stop?database=demo-primary' \
  | python -m json.tool

# Resume it
curl -s -X POST 'http://localhost:8080/api/v1/resume?database=demo-primary' \
  | python -m json.tool
```

**Per-database config view:**

```bash
curl -s 'http://localhost:8080/api/v1/config?database=demo-primary' \
  | python -m json.tool
```

**Expected response:**

```json
{
    "database": "demo-primary",
    "trust_level": "autonomous",
    "executor": true,
    "llm": false,
    "tags": ["demo", "primary"]
}
```

**Graceful failure isolation.** If one database goes down, the other continues
collecting and analyzing normally. The degraded database shows `connected: false`
and `health_score: 0` in the fleet overview.

**Per-database LLM token budget.** Configurable as `equal` (each DB gets
`total / N`), `proportional` (based on query volume), or `priority-weighted`
(tags determine allocation).

---

## 11. Prometheus Metrics

pg_sage exposes Prometheus-format metrics on port 9187.

```bash
curl -s http://localhost:9187/metrics
```

### Key metrics to look for

```prometheus
# HELP pg_sage_info pg_sage version information
# TYPE pg_sage_info gauge
pg_sage_info{version="vdev"} 1

# HELP pg_sage_findings_total Number of open findings by severity
# TYPE pg_sage_findings_total gauge
pg_sage_findings_total{severity="critical",database="sage_demo"} 2
pg_sage_findings_total{severity="warning",database="sage_demo"} 3
pg_sage_findings_total{severity="info",database="sage_demo"} 2

# HELP pg_sage_database_connected Database connection status
# TYPE pg_sage_database_connected gauge
pg_sage_database_connected{database="sage_demo"} 1

# HELP pg_sage_health_score Database health score 0-100
# TYPE pg_sage_health_score gauge
pg_sage_health_score{database="sage_demo"} 70

# HELP pg_sage_collector_duration_seconds Last collection duration
# TYPE pg_sage_collector_duration_seconds gauge
pg_sage_collector_duration_seconds{database="sage_demo"} 0.45

# HELP pg_sage_analyzer_duration_seconds Last analysis duration
# TYPE pg_sage_analyzer_duration_seconds gauge
pg_sage_analyzer_duration_seconds{database="sage_demo"} 0.12

# HELP pg_sage_executor_actions_total Total actions by outcome
# TYPE pg_sage_executor_actions_total counter
pg_sage_executor_actions_total{database="sage_demo",outcome="success"} 2

# HELP pg_sage_circuit_breaker_state Circuit breaker state (0=closed, 1=open)
# TYPE pg_sage_circuit_breaker_state gauge
pg_sage_circuit_breaker_state{breaker="db"} 0
pg_sage_circuit_breaker_state{breaker="llm"} 0

# HELP pg_sage_llm_tokens_used LLM tokens consumed (if enabled)
# TYPE pg_sage_llm_tokens_used counter
pg_sage_llm_tokens_used{database="sage_demo"} 4521

# HELP pg_sage_llm_requests_total LLM requests made (if enabled)
# TYPE pg_sage_llm_requests_total counter
pg_sage_llm_requests_total{database="sage_demo"} 3
```

Every metric includes a `database` label, making it easy to build per-database
Grafana dashboards in fleet mode.

### Grafana integration

Add `http://localhost:9187/metrics` as a Prometheus data source in Grafana.
Useful panels:

- **Health score gauge** per database
- **Findings count** time series by severity
- **Collector/analyzer duration** to spot performance degradation
- **Circuit breaker state** alert (fire when breaker opens)
- **LLM token budget** burndown (daily budget vs. consumed)

---

## 12. MCP Server

pg_sage includes a Model Context Protocol (MCP) server for AI agent
integration. It runs on port 5434 (configurable via `mcp.listen_addr`).

### Connecting Claude Desktop

Add this to your Claude Desktop config file:

**macOS:** `~/Library/Application Support/Claude/claude_desktop_config.json`
**Windows:** `%APPDATA%\Claude\claude_desktop_config.json`

```json
{
  "mcpServers": {
    "pg_sage": {
      "url": "http://localhost:5434/sse"
    }
  }
}
```

Restart Claude Desktop. You should see "pg_sage" in the MCP server list
(look for the hammer icon in the chat input area).

### Available MCP Tools

| Tool | Description | Requires LLM |
|---|---|---|
| `diagnose` | Ask an interactive diagnostic question about your database | Yes |
| `briefing` | Generate a health briefing of the current database state | Yes |
| `suggest_index` | Get index suggestions for a specific table | Yes |
| `review_migration` | Review DDL/migration SQL for production safety | Yes |

### Available MCP Resources

| Resource URI | Description |
|---|---|
| `sage://health` | System health overview |
| `sage://findings` | Open findings with recommendations |
| `sage://schema/{table}` | Table DDL, columns, indexes, constraints |
| `sage://stats/{table}` | Table size, rows, vacuum status |
| `sage://slow-queries` | Top slow queries from pg_stat_statements |

### Example prompts in Claude Desktop

Once connected, try these prompts:

- "Use pg_sage to give me a health briefing of the database"
- "Ask pg_sage to suggest indexes for the orders table"
- "Have pg_sage review this migration: `ALTER TABLE orders ADD COLUMN region TEXT DEFAULT 'US' NOT NULL`"
- "Use pg_sage to diagnose why queries on order_events are slow"

### Verifying the MCP server manually

```bash
# Health check
curl -s http://localhost:5434/health

# Start an SSE session (keep this terminal open)
curl -sN http://localhost:5434/sse
```

You will receive an endpoint event with a session ID:

```
event: endpoint
data: /messages?sessionId=<uuid>
```

In a separate terminal, send an MCP initialize request:

```bash
curl -s -X POST "http://localhost:5434/messages?sessionId=<uuid>" \
  -H "Content-Type: application/json" \
  -d '{
    "jsonrpc": "2.0",
    "id": 1,
    "method": "initialize",
    "params": {
      "protocolVersion": "2024-11-05",
      "capabilities": {},
      "clientInfo": {"name": "walkthrough", "version": "1.0"}
    }
  }'
```

Then list available tools:

```bash
curl -s -X POST "http://localhost:5434/messages?sessionId=<uuid>" \
  -H "Content-Type: application/json" \
  -d '{
    "jsonrpc": "2.0",
    "id": 2,
    "method": "tools/list",
    "params": {}
  }'
```

---

## 13. Cleanup

### Stop the sidecar

Press `Ctrl+C` in the terminal where the sidecar is running. It shuts down
gracefully, closing database connections and flushing any pending writes.

### Stop and remove the demo database

```bash
docker compose -f demo/docker-compose-live.yml down -v
```

The `-v` flag removes the Docker volume, deleting all demo data. Next time you
run `docker compose up -d`, it starts fresh.

### Remove the built binary

```bash
rm sidecar/pg_sage_sidecar
```

### Full clean slate

```bash
# From repo root
docker compose -f demo/docker-compose-live.yml down -v
rm -f sidecar/pg_sage_sidecar
rm -rf sidecar/web/node_modules
```

To start fresh again, go back to [Quick Start](#2-quick-start-5-min).

---

## Quick Reference

| Service | URL | Purpose |
|---|---|---|
| Dashboard | http://localhost:8080 | React SPA (5 pages) |
| REST API | http://localhost:8080/api/v1/ | 14 JSON endpoints |
| MCP Server | http://localhost:5434/sse | Claude Desktop / AI agents |
| Prometheus | http://localhost:9187/metrics | Metrics scraping |
| PostgreSQL | localhost:5433 | Demo database |

| Credential | Value |
|---|---|
| PG user | `sage_agent` |
| PG password | `sage_password` |
| PG database | `sage_demo` |
| PG superuser | `postgres` / `postgres` |

---

## Timing Cheat Sheet

| Event | When |
|---|---|
| PostgreSQL container ready | ~10 seconds after `docker compose up` |
| Init scripts complete (2.3M rows) | ~30 seconds after container start |
| First collector snapshot | ~30 seconds after sidecar start |
| First findings appear | ~60 seconds (first analyzer cycle) |
| Most findings discovered | ~2-3 minutes (2-3 analyzer cycles) |
| Executor actions start (autonomous mode) | ~2-3 minutes |
| LLM optimizer/advisor findings (if key set) | ~2-3 minutes |
| Full steady state | ~5 minutes |
