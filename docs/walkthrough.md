# pg_sage Product Walkthrough

This guide walks you through every feature of pg_sage step by step. You'll start the system, explore findings, test Tier 1/2/3 features, use the MCP sidecar, and verify Prometheus metrics.

**Time required**: ~15 minutes

---

## Prerequisites

- Docker and Docker Compose installed
- Terminal with `psql` available (the Docker container includes it)
- Ports 5432, 5433, and 9187 available (or adjust `docker-compose.yml`)

---

## Step 1: Start the Stack

```bash
git clone https://github.com/jasonmassie01/pg_sage.git
cd pg_sage
docker compose up -d
```

Wait for the health check to pass (~10 seconds):

```bash
docker compose ps
```

You should see both `pg_sage-pg_sage-1` (healthy) and `pg_sage-sidecar-1` (running).

---

## Step 2: Connect to PostgreSQL

```bash
docker exec -it pg_sage-pg_sage-1 psql -U postgres
```

You're now inside a PostgreSQL 17 instance with pg_sage loaded.

---

## Step 3: Check System Status

```sql
SELECT * FROM sage.status();
```

You'll see a JSONB object with:
- `version`: "0.5.0"
- `enabled`: true
- `trust_level`: "advisory" (the Docker default)
- `circuit_state`: "closed" (healthy)
- `collector_running`, `analyzer_running`, `briefing_running`: all true
- `emergency_stopped`: false

---

## Step 4: Wait for First Analysis (~60 seconds)

The collector runs every 30 seconds and the analyzer every 60 seconds. Wait about a minute after startup, then:

```sql
SELECT category, severity, title
FROM sage.findings
WHERE status = 'open'
ORDER BY severity, category;
```

Expected findings on the demo database:

| severity | category | what it found |
|----------|----------|---------------|
| critical | duplicate_index | Duplicate index on `orders` table |
| critical | sequence_exhaustion | `test_exhausted_seq` at 93.1% capacity |
| critical | config | Low cache hit ratio (expected on fresh DB) |
| warning | config | `shared_buffers` below recommended 25% of RAM |
| warning | config | `random_page_cost` at HDD default |
| warning | security_missing_rls | `customers` table has sensitive columns but no RLS |
| warning | unused_index | Unused indexes detected |
| info | config | `max_connections` exceeds peak usage |

---

## Step 5: Inspect a Finding in Detail

Pick any finding ID from the previous query:

```sql
SELECT id, category, severity, title, detail, recommendation, recommended_sql, rollback_sql
FROM sage.findings
WHERE status = 'open'
LIMIT 1;
```

Each finding includes:
- **detail**: JSON with specifics (table size, scan counts, bloat %, etc.)
- **recommendation**: Human-readable advice
- **recommended_sql**: The exact SQL to fix the issue
- **rollback_sql**: SQL to undo the fix if needed

---

## Step 6: Generate a Health Briefing (Tier 1)

```sql
SELECT sage.briefing();
```

Without an LLM configured, this produces a structured text briefing summarizing all open findings by severity. It covers:
- Critical issues requiring immediate attention
- Warnings to address soon
- Informational observations

The briefing is also stored in `sage.briefings`:

```sql
SELECT id, generated_at, delivered, length(content) AS content_length
FROM sage.briefings
ORDER BY generated_at DESC
LIMIT 3;
```

---

## Step 7: Test the Diagnose Function (Tier 2 — works in basic mode without LLM)

```sql
SELECT sage.diagnose('Why are my queries slow?');
```

Without an LLM, this returns findings related to query performance. With an LLM configured, it would use a ReAct reasoning loop to investigate step by step.

---

## Step 8: Explore EXPLAIN Plan Capture

First, run a query that gets tracked by `pg_stat_statements`:

```sql
-- Create a test query to analyze
SELECT count(*) FROM pg_class WHERE relkind = 'r';

-- Find its queryid
SELECT queryid, query, calls, mean_exec_time
FROM pg_stat_statements
WHERE query LIKE '%pg_class%' AND query NOT LIKE '%pg_stat_statements%'
ORDER BY calls DESC
LIMIT 5;
```

Capture and view the explain plan (use the queryid from above):

```sql
-- Replace with an actual queryid from the previous query
SELECT sage.explain(queryid_here);
```

This runs `EXPLAIN (FORMAT JSON, COSTS, VERBOSE)` against the query and caches the result in `sage.explain_cache`.

---

## Step 9: Suppress a Finding

If a finding is a known issue you want to ignore temporarily:

```sql
-- Get a finding ID
SELECT id, title FROM sage.findings WHERE status = 'open' LIMIT 1;

-- Suppress it for 30 days with a reason
SELECT sage.suppress(1, 'Known issue, vendor fix pending', 30);

-- Verify it's suppressed
SELECT id, title, status FROM sage.findings WHERE id = 1;
```

Suppressed findings auto-unsuppress when the duration expires.

---

## Step 10: Test Emergency Controls

```sql
-- Stop all autonomous activity
SELECT sage.emergency_stop();

-- Verify
SELECT * FROM sage.status();
-- emergency_stopped = true, circuit_state = "open"

-- Resume normal operation
SELECT sage.resume();

-- Verify
SELECT * FROM sage.status();
-- emergency_stopped = false, circuit_state = "closed"
```

---

## Step 11: Explore the Action Log (Tier 3)

The action executor logs every autonomous action:

```sql
SELECT id, action_type, category, finding_id, status,
       executed_at, duration_ms
FROM sage.action_log
ORDER BY executed_at DESC
LIMIT 10;
```

In advisory mode (the Docker default), pg_sage can execute SAFE actions like dropping duplicate/unused indexes. Each entry includes:
- `before_state`: System state before the action
- `after_state`: System state after
- `rollback_sql`: How to undo it

---

## Step 12: Check MCP Sidecar (from another terminal)

The sidecar exposes pg_sage capabilities via the Model Context Protocol. Open a new terminal:

### Prometheus Metrics

```bash
curl -s http://localhost:9187/metrics
```

You'll see:
- `pg_sage_info{version="0.5.0"} 1`
- `pg_sage_findings_total{severity="critical"} N`
- `pg_sage_findings_total{severity="warning"} N`
- `pg_sage_circuit_breaker_state{breaker="db"} 0`
- `pg_sage_circuit_breaker_state{breaker="llm"} 0`

### MCP Server

The MCP server runs on port 5433 using SSE (Server-Sent Events). AI assistants like Claude, Cursor, and Copilot can connect to it. To test manually:

```bash
# Initialize an MCP session
curl -X POST http://localhost:5433/ \
  -H "Content-Type: application/json" \
  -d '{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2024-11-05","capabilities":{},"clientInfo":{"name":"test","version":"1.0"}}}'
```

The response arrives via SSE. The sidecar supports these MCP resources:
- `sage://health` — System health overview
- `sage://findings` — Open findings
- `sage://schema/{table}` — Table DDL, indexes, constraints
- `sage://stats/{table}` — Table size, row counts, vacuum status
- `sage://slow-queries` — Top slow queries
- `sage://explain/{queryid}` — Cached EXPLAIN plans

---

## Step 13: View MCP JSON Functions Directly

These SQL functions return JSONB and are what the sidecar calls internally:

```sql
-- System health overview
SELECT sage.health_json();

-- Open findings as JSON array
SELECT sage.findings_json();

-- Table schema details
SELECT sage.schema_json('public.orders');

-- Table statistics
SELECT sage.stats_json('public.orders');

-- Slow queries
SELECT sage.slow_queries_json();
```

---

## Step 14: Explore Snapshots

The collector captures point-in-time snapshots every 30 seconds:

```sql
-- See snapshot categories
SELECT DISTINCT category FROM sage.snapshots;

-- View recent system snapshots
SELECT captured_at, data
FROM sage.snapshots
WHERE category = 'system'
ORDER BY captured_at DESC
LIMIT 3;

-- View index stats
SELECT captured_at, data
FROM sage.snapshots
WHERE category = 'indexes'
ORDER BY captured_at DESC
LIMIT 1;
```

Snapshot categories: `stat_statements`, `tables`, `indexes`, `system`, `locks`, `sequences`, `replication`.

---

## Step 15: Check Configuration

pg_sage's configuration is controlled via PostgreSQL GUCs:

```sql
-- View all sage.* settings
SELECT name, setting, short_desc
FROM pg_settings
WHERE name LIKE 'sage.%'
ORDER BY name;
```

Key settings to experiment with:
- `sage.slow_query_threshold` — Default 1s, lower it to catch more queries
- `sage.seq_scan_min_rows` — Default 100000, minimum rows for sequential scan alerts
- `sage.trust_level` — `observation` (read-only), `advisory` (safe actions), `autonomous` (moderate actions)
- `sage.llm_enabled` — Set to `on` after configuring `sage.llm_endpoint`

---

## Step 16: Test with LLM (Optional)

If you have access to an OpenAI-compatible API (OpenAI, Ollama, Claude via OpenRouter):

```sql
-- Configure LLM
ALTER SYSTEM SET sage.llm_endpoint = 'http://host.docker.internal:11434/v1/chat/completions';  -- Ollama example
ALTER SYSTEM SET sage.llm_model = 'llama3.2';
ALTER SYSTEM SET sage.llm_enabled = on;
SELECT pg_reload_conf();

-- Now briefings use natural language
SELECT sage.briefing();

-- Diagnose uses ReAct reasoning
SELECT sage.diagnose('What are the biggest performance risks in my database?');
```

---

## Step 17: Import the Grafana Dashboard (Optional)

If you have Grafana connected to Prometheus:

1. Open Grafana → Dashboards → Import
2. Upload `grafana/pg_sage_dashboard.json`
3. Select your Prometheus data source
4. The dashboard includes 18 panels: findings by severity, connections, cache hit ratio, TPS, deadlocks, circuit breaker status, and database size

---

## Step 18: Clean Up

```bash
docker compose down -v   # -v removes the pgdata volume
```

---

## Summary of What You've Seen

| Feature | Status | Tier |
|---------|--------|------|
| Automatic finding detection (indexes, queries, config, security, sequences) | Working | Tier 1 |
| Health briefings | Working | Tier 1 (enhanced with LLM in Tier 2) |
| EXPLAIN plan capture and caching | Working | Tier 1 |
| Emergency stop / resume | Working | Core |
| Circuit breakers (DB + LLM) | Working | Core |
| Finding suppression with auto-expiry | Working | Core |
| Action executor with trust gating | Working | Tier 3 |
| MCP sidecar for AI assistants | Working | v0.5 |
| Prometheus metrics | Working | v0.5 |
| Diagnose with ReAct reasoning | Working (basic without LLM) | Tier 2 |
| Cloud environment detection | Working | Sidecar |

pg_sage detects 8+ categories of issues, explains them with actionable recommendations, and can autonomously fix safe problems as it earns trust over time.
