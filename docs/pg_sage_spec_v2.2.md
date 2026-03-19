# pg_sage — The Autonomous PostgreSQL DBA Agent

## Product Spec & Feature List — v2.2 (Build-Ready)

---

## The Pitch

**"Senior DBA expertise, running 24/7, installed in 60 seconds."**

A self-hosted PostgreSQL extension that continuously monitors, diagnoses, optimizes, and explains your database — no SaaS dependency, no sending your data to anyone's cloud. Connect an LLM to unlock conversational diagnostics and natural language briefings. Works without one.

**The only DBA that lives inside your database — and dies cleanly with it.**

---

## Brand Architecture

**pg_sage** — the wise advisor. Watches, learns, diagnoses, recommends. The DBA you always wanted but couldn't hire. Open source, community-driven, earns trust through daily value.

**pg_mage** (future) — the one with actual power. Learned query optimization, GPU-accelerated execution, natural language queries (`pnl`), reverse insight generation, `pg_semantic` vector metadata. The full Clawgres engine.

The sage observes. The mage transforms.

**Internal codename**: pg_dead_dba

**CLI namespace**: `sage diagnose`, `sage briefing`, `sage explain`, `sage audit`, `sage shell`

**License**: AGPL-3.0

---

## Prerequisites & Compatibility

### Minimum PostgreSQL Version: 14+

| PG Version | Support Level | Notes |
|-----------|--------------|-------|
| **PG 17** | Full | All features including `pg_stat_io` |
| **PG 16** | Full | `pg_stat_io` available for IOPS tracking |
| **PG 15** | Full | All core features |
| **PG 14** | Full | Minimum supported. `pg_stat_statements` queryid stability. |
| **PG 13** | Degraded | Missing `pg_stat_progress_copy`, some `pg_stat_statements` fields. Not officially supported. |
| **PG 12-** | Not supported | |

### Required Extensions

**`pg_stat_statements`** — hard prerequisite. Must be in `shared_preload_libraries`. If not loaded, sage refuses to start and emits:

```
FATAL: pg_sage requires pg_stat_statements.
Add 'pg_stat_statements' to shared_preload_libraries in postgresql.conf and restart.
```

This is table stakes. Every production Postgres deployment should have it.

**`auto_explain`** — soft prerequisite for EXPLAIN plan capture. Sage manages it dynamically (see EXPLAIN Capture Mechanism below). If not in `shared_preload_libraries`, sage operates without automatic plan capture and emits a WARNING-level finding recommending it.

### Deployment Matrix

| Deployment | Extension Mode | Sidecar Mode | Notes |
|-----------|---------------|-------------|-------|
| **Self-managed (bare metal / VM)** | Full power | Optional | All features, all tiers. Recommended. |
| **Kubernetes (self-managed PG)** | Full power | Recommended (separate pod) | Resource target: <1% CPU, <100MB RAM. |
| **AWS RDS / Aurora** | Not supported | Required | No custom extensions/BG workers. No ALTER SYSTEM. Tier 3 limited to index/vacuum DDL via sidecar's connection role. |
| **GCP Cloud SQL** | Not supported | Required | Same constraints as RDS. |
| **GCP AlloyDB** | Partial (track) | Recommended | May support custom extensions. Test per release. |
| **Azure Flexible Server** | Partial | Recommended | `shared_preload_libraries` configurable. Test per version. |
| **Supabase** | Possible (Pro+) | Alternative | Background workers may be restricted. Test. |
| **Neon / Lakebase** | Not supported | Required | Serverless — no persistent BG workers. Sidecar with periodic polling. |

**Sidecar mode** connects via standard libpq, runs collection/analysis externally, writes findings to `sage` schema. Loses: in-process efficiency, sub-second cycles. Gains: universal compatibility.

**Phase 0.1 target**: Self-managed extension mode. Sidecar mode in Phase 0.2.

---

## Execution Model

pg_sage runs inside PostgreSQL via the Background Worker API. No external scheduler. `CREATE EXTENSION pg_sage` starts it. `DROP EXTENSION pg_sage` stops it.

### Instance Isolation

sage installs **per-database**. It monitors only the database it's installed in. `pg_stat_statements` is cluster-wide — sage filters to queries matching the current database OID. If the user wants to monitor multiple databases on the same cluster, they install the extension in each. sage uses a Postgres advisory lock (`pg_advisory_lock(hashtext('pg_sage'))`) on startup to prevent duplicate instances (extension + sidecar, or accidental double-install) against the same database.

### Background Workers

**The Collector** (30-60 second loop)
Snapshots `pg_stat_statements`, `pg_stat_user_tables`, `pg_stat_user_indexes`, `pg_stat_bgwriter`, `pg_stat_wal`, `pg_locks`, `pg_sequences`, replication stats, and WAL archiving status into `sage.snapshots`.

For massive schemas (10K+ tables), the collector operates in **incremental mode**: rotates through table subsets per cycle (configurable via `sage.collector_batch_size`, default: 1000 tables per cycle). `pg_stat_statements` is always collected in full (it's already aggregated). If any collector query exceeds the circuit breaker timeout, that specific query is skipped and the cycle continues.

**The Analyzer** (5-15 minute cycle, configurable via `sage.analyzer_interval`)
Processes snapshots through the rules engine. Writes structured findings to `sage.findings`. Partition-aware: analyzes parent partitioned tables holistically, not 500 individual partitions. Groups child partition findings under the parent.

**The Briefing Worker** (daily, configurable via `sage.briefing_schedule`)
Aggregates findings, connects to LLM if configured (falls back to structured output if unavailable), dispatches to configured channels.

### EXPLAIN Capture Mechanism

sage uses `auto_explain` with dynamic sampling — never constant full capture.

**Baseline mode** (always-on when auto_explain is loaded):
```sql
-- sage sets these on startup
SET auto_explain.log_min_duration = '1000';  -- matches sage.slow_query_threshold
SET auto_explain.sample_rate = 0.01;         -- 1% of qualifying queries
SET auto_explain.log_analyze = true;
SET auto_explain.log_buffers = true;
SET auto_explain.log_format = 'json';
SET auto_explain.log_nested_statements = false;
```
Plans trickle into the Postgres log. sage's collector parses them from `csvlog` (if `log_destination = 'csvlog'`) or from the `pg_log` directory.

**Targeted capture mode** (on-demand during diagnostics):
When sage investigates a specific slow query (via ReAct diagnostic loop or `sage explain <queryid>`), it temporarily bumps capture:
```sql
SET auto_explain.sample_rate = 1.0;  -- capture everything
-- Wait for target query to execute (2-5 min window)
SET auto_explain.sample_rate = 0.01; -- restore baseline
```
All auto_explain parameters are hot-reloadable. Zero restart, zero downtime.

**Without auto_explain**: sage still detects slow queries via `pg_stat_statements` timing data. Users can manually trigger plan capture: `SELECT sage.explain(queryid)` re-executes a parameterized EXPLAIN (using the query text from `pg_stat_statements` with dummy parameters for planning-only, no ANALYZE). This gives estimated plans, not actual — noted in the output.

---

## Safety Systems

### Privilege Model

sage's privilege model differs between extension mode and sidecar mode.

**Extension mode**: Background workers run under the postmaster's user (typically `postgres`, effectively superuser). This is how all PG background worker extensions operate (pg_cron, TimescaleDB, etc.). sage constrains itself via application logic — it only reads catalog views and writes to its own schema during Tier 1/2 operations. Tier 3 DDL actions (CREATE INDEX, DROP INDEX) execute under postmaster privileges but are gated by trust ramp + maintenance windows + individual action toggles + action journaling.

**Sidecar mode**: Connects as a dedicated role with minimal grants:
```sql
CREATE ROLE sage_agent WITH LOGIN PASSWORD '...';

-- Read access to monitoring views
GRANT pg_monitor TO sage_agent;           -- PG14+ built-in monitoring role
GRANT SELECT ON pg_stat_statements TO sage_agent;

-- Write access to sage schema only
GRANT ALL ON SCHEMA sage TO sage_agent;
GRANT ALL ON ALL TABLES IN SCHEMA sage TO sage_agent;
ALTER DEFAULT PRIVILEGES IN SCHEMA sage GRANT ALL ON TABLES TO sage_agent;

-- Tier 3 grants (added individually when enabled):
-- GRANT CREATE ON SCHEMA public TO sage_agent;  -- for index creation
-- GRANT pg_signal_backend TO sage_agent;          -- for session termination
```

`pg_monitor` (built-in since PG14) provides read access to all `pg_stat_*` views, `pg_settings`, server logs, and other monitoring data without superuser. This is the correct least-privilege approach for sidecar mode.

### Circuit Breaker (Observer Effect Protection)

pg_sage must never become the incident.

- **CPU ceiling**: Read `/proc/stat` (Linux) to compute system CPU utilization. Suspend collector and analyzer if CPU exceeds 90%. On non-Linux systems, fall back to checking `pg_stat_activity` active query count vs `max_connections` as a proxy for load
- **Query timeout**: `SET LOCAL statement_timeout = '500ms'` on every sage-issued query. Timeout kills only that query; cycle continues with remaining queries
- **Connection budget**: Dedicated pool of 2 connections (configurable via `sage.max_connections`). Never competes with application traffic
- **Backoff escalation**: 3 consecutive skipped cycles → dormant mode (1 collection per 10 minutes) until load recovers for 3 consecutive successful cycles
- **Disk pressure**: Suspend writes to `sage` schema if `pg_tablespace_size` indicates <5% free (configurable via `sage.disk_pressure_threshold`)
- **Resource target**: <1% CPU, <100MB RAM baseline

### LLM Circuit Breaker

- **Timeout**: Configurable (default: `sage.llm_timeout = 30` seconds)
- **Retry**: 3 retries with exponential backoff (1s, 4s, 16s), then circuit opens for `sage.llm_cooldown` (default: 300 seconds)
- **Fallback**: Briefings render as structured rules-only output. Diagnostic shell returns "LLM unavailable, showing raw findings." EXPLAIN analysis outputs raw plan JSON
- **Cost tracking**: Log token counts per call. Alert at `sage.llm_token_budget` (default: 50000 tokens/day)
- **Tier 1 independence**: Tier 1 never calls the LLM, period

### HA / Failover Awareness

- **Role detection**: Every collector cycle checks `pg_is_in_recovery()`. Primary = full operation. Replica = read-only observation mode (all Tier 3 suppressed, findings flagged "replica — action deferred to primary")
- **Patroni/Stolon/repmgr**: Detect via process list or well-known DCS keys. Log role transitions
- **Split-brain protection**: If role flips within N consecutive checks (default: 5), enter safe mode until stable

### Trust Ramp (Graduated Autonomy)

| Period | Mode | Behavior |
|--------|------|----------|
| **Day 0–7** | Observation | Collect, analyze, generate findings. Zero actions. Builds baseline. |
| **Day 8–30** | Advisory | Findings include exact DDL + rollback SQL. "I would run this." No execution. |
| **Day 31+** | Autonomous (opt-in) | Tier 3 actions available per individual toggle. |

Skip or accelerate via `sage.trust_level = 'autonomous'`. The ramp is a safe default, not a gate.

### Maintenance Windows

- **Schedule format**: Cron syntax with explicit timezone. `sage.maintenance_window = '0 2 * * 6 America/Chicago'`
- **Timezone handling**: Internal scheduling in UTC. Timezone conversion handles DST. Use `AT TIME ZONE` internally
- **Auto-rollback trigger**: If mean query latency (from `pg_stat_statements` mean_exec_time across top-50 queryids by calls) regresses >10% within 15-minute monitoring window after a config change, sage reverts. **Hysteresis**: rolled-back parameter is not retried for 7 days. (Note: mean, not p95. `pg_stat_statements` doesn't expose percentiles natively. Percentile estimation via `pg_stat_activity` sampling is a v1.0 enhancement.)
- **Action journaling**: Every action recorded in `sage.action_log` with timestamp, before/after state, triggering finding_id, outcome, and measured before/after mean latency
- **Emergency stop**: `SELECT sage.emergency_stop()` → observation-only until `SELECT sage.resume()`

---

## Architecture

```
┌──────────────────────────────────────────────────────────────┐
│  PostgreSQL Instance                                          │
│  ┌───────────────┐  ┌─────────────────────────────────────┐  │
│  │ pg_stat_*     │  │  Extension (pg_sage)                │  │
│  │ pg_catalog    │  │  ┌─────────────┐ ┌───────────────┐  │  │
│  │ pg_sequences  │──│  │ Collector   │ │ Rules Engine  │  │  │
│  │ auto_explain  │  │  │ (30-60s)    │ │ (5-15min)     │  │  │
│  │ pg_config     │  │  └──────┬──────┘ └───────┬───────┘  │  │
│  │ WAL stats     │  │         │                 │          │  │
│  │ pg_locks      │  │  ┌──────▼─────────────────▼───────┐  │  │
│  │ csvlog        │  │  │ sage schema                    │  │  │
│  └───────────────┘  │  │  (see Schema Reference)        │  │  │
│                      │  └──────┬────────────────┬───────┘  │  │
│  ┌───────────────┐  │  ┌──────▼──────┐  ┌──────▼───────┐  │  │
│  │ Circuit       │  │  │ Briefing    │  │ Action       │  │  │
│  │ Breakers      │──│  │ Worker      │  │ Executor     │  │  │
│  │ (DB + LLM)    │  │  │ (daily)     │  │ (windowed)   │  │  │
│  └───────────────┘  │  └──────┬──────┘  └──────────────┘  │  │
│  ┌───────────────┐  │  ┌──────▼──────┐                     │  │
│  │ HA Awareness  │──│  │ Self-Monitor│                     │  │
│  │ + Trust Ramp  │  │  └─────────────┘                     │  │
│  └───────────────┘  └──────────┬──────────────────────────┘  │
└─────────────────────────────────┼─────────────────────────────┘
                    ┌─────────────┼─────────────┐
                    │             │             │
          ┌─────────▼──┐  ┌──────▼──────┐  ┌──▼───────────┐
          │ LLM Backend│  │ MCP Server  │  │ Sidecar      │
          │ (pluggable)│  │ (future)    │  │ (optional)   │
          │ Ollama     │  │ Claude,     │  │ Dashboard,   │
          │ Claude API │  │ Cursor,     │  │ Prometheus,  │
          │ OpenAI     │  │ Windsurf,   │  │ Alerting     │
          │ OpenRouter │  │ any MCP     │  │              │
          │ vLLM       │  │ client      │  │              │
          └────────────┘  └─────────────┘  └──────────────┘
```

---

## Feature List

### TIER 1: Rules Engine (No LLM Required)

Pure SQL against Postgres catalog views + deterministic rules. Works immediately on install.

#### 1.1 Index Lifecycle Management
- **Unused index detection**: `pg_stat_user_indexes.idx_scan = 0` over configurable window (`sage.unused_index_window`, default: `30d`, options: `7d`, `30d`, `90d`)
- **Duplicate index detection**: Match ALL of: column list + order, index expressions, partial predicate (WHERE), collation, operator class, INCLUDE columns. Flag exact duplicates and strict subsets
- **Missing index suggestions**: Cross-reference sequential scans on large tables with slow query patterns from `pg_stat_statements`
- **Partial index opportunities**: Identify repeated WHERE clauses in >N% of queries against a table
- **Index bloat estimation**: `pgstattuple` if available, else heuristic from `pg_class.relpages` vs estimated pages. Threshold: `sage.index_bloat_threshold` (default: 30%)
- **Index write penalty scoring**: Calculate per-index write overhead (INSERT/UPDATE/DELETE impact from `pg_stat_user_tables` mutation counts × index count) vs read benefit (idx_scan frequency × estimated scan cost savings). Surface as ROI score in findings
- **Autonomous action (opt-in)**: Drop unused, create suggested — gated by trust ramp + maintenance window

#### 1.2 Vacuum & Bloat Management
- **Per-table autovacuum tuning**: Analyze dead tuple accumulation rates, recommend table-specific overrides
- **Bloat monitoring**: Track table and index bloat over time
- **XID wraparound early warning**: Alert at `age(datfrozenxid)` > 500M (warning), > 1B (critical)
- **Vacuum progress tracking**: `pg_stat_progress_vacuum` — estimate completion, flag stuck/canceled
- **Toast table bloat**: Identify oversized TOAST tables
- **Partition-aware analysis**: For partitioned tables, aggregate bloat/vacuum stats across children and report at the parent level. Individual partitions only surface if they're outliers (>2 std dev from sibling average)

#### 1.3 Configuration Tuning
- **Hardware-aware defaults**: Detect memory via `pg_sysconf('PAGESIZE') * pg_sysconf('PHYS_PAGES')`, CPU via `/proc/cpuinfo` or `pg_sysconf('NPROCESSORS_ONLN')`, storage type via `pg_test_timing` (SSD: <0.1ms avg, HDD: >1ms avg; fallback: assume SSD)
- **Workload-adaptive tuning**: Classify OLTP/OLAP/mixed from `pg_stat_statements` patterns
- **Connection management**: `max_connections` vs actual peak from `pg_stat_activity` snapshots. Detect connection leaks (idle >30min, zero queries executed)
- **Checkpoint tuning**: Analyze frequency/duration from `pg_stat_bgwriter`
- **Shared buffers optimization**: Track hit ratio, recommend based on working set
- **Hot-reload awareness**: Only auto-tune reload-safe params. Flag restart-required recommendations separately

#### 1.4 Dual Cache Observability
- **Shared buffers hit rate**: `blks_hit / (blks_hit + blks_read)` from `pg_stat_database`
- **OS page cache inference**: Compare `blks_read` against `pg_stat_io` physical reads (PG16+). High PG misses + low disk reads = OS cache compensating
- **Differential tuning**: Adjust recommendations based on which cache layer is actually serving reads
- **Cache pressure detection**: Alert when both layers miss

#### 1.5 Query Performance Monitoring
- **Slow query detection**: `pg_stat_statements` entries exceeding `sage.slow_query_threshold` (default: `1000` ms mean_exec_time)
- **Query regression detection**: Alert when queryid mean_exec_time increases >50% vs 7-day rolling baseline
- **Sequential scan watchdog**: Flag seq scans on tables > `sage.seq_scan_min_rows` (default: `100000`)
- **Lock contention**: `pg_locks` + `pg_stat_activity` wait graph
- **Temp file usage**: `pg_stat_statements.temp_blks_written > 0`
- **Deadlock detection**: Track `pg_stat_database.deadlocks` counter deltas. Parse csvlog for deadlock detail when accessible

#### 1.6 Sequence Exhaustion Monitoring
- **Capacity**: `last_value / max_value` from `pg_sequences`
- **Thresholds**: Warning at 75%, critical at 90%. BIGINT: only alert if growth rate projects exhaustion within 1 year
- **Growth rate projection**: Track advancement rate, project exhaustion date
- **INTEGER risk flag**: Specifically flag INTEGER sequences with high advancement — silent production killer

#### 1.7 Storage & Capacity
- **Per-table/index/schema size tracking** with growth rate projection
- **Linear capacity forecasting**: "Disk hits 90% in N days at current rate"
- **Tablespace balance monitoring**
- **IOPS tracking**: `pg_stat_io` on PG16+

#### 1.8 Security Audit
- **Default/empty password detection**
- **Overprivileged roles** (SUPERUSER/CREATEDB/CREATEROLE without need)
- **Missing RLS**: Flag tables with sensitive column names lacking RLS policies
- **Connection source analysis**: Unexpected IPs/application_names from `pg_stat_activity`
- **Extension vulnerability audit**: Installed versions vs known advisory list

#### 1.9 Replication & Backup Health
- **Physical replication lag**: `pg_stat_replication`, alert on divergence
- **Replication slot monitoring**: Inactive slots, WAL accumulation risk
- **Logical replication**: Publication/subscription status, apply lag, conflicts
- **Failover readiness scoring**: Lag + slot health + timeline consistency
- **WAL archiving health**: `pg_stat_archiver` — last successful archive time, failure count
- **Backup freshness** (best-effort): pgbackrest/Barman status when accessible. Alert when stale (>24h)
- **Materialized view staleness**: Track last refresh vs underlying table modification rate

#### 1.10 Self-Monitoring (sage_health)
- **Resource tracking**: CPU time, memory, `sage` schema disk footprint per cycle
- **Cycle health**: Collector/analyzer duration, skip rate, consecutive dormant cycles
- **Action outcomes**: Success, rollback, skipped counts
- **Finding volume**: Detect anomalous spikes or drops
- **Self-alert**: Generate finding against itself if exceeding resource targets

---

### TIER 2: LLM-Enhanced Features

LLM receives metadata only — never row data. See Privacy Model.

#### 2.1 EXPLAIN Plan Analysis
- **Auto-capture**: via auto_explain sampling (see Execution Model). Plans stored in `sage.explain_cache`
- **Manual capture**: `SELECT sage.explain(queryid)` runs planning-only EXPLAIN on parameterized query text
- **Natural language interpretation**: LLM translates plan JSON to plain English with root cause and fix
- **Fix generation**: CREATE INDEX, ALTER TABLE SET STATISTICS, query rewrite suggestions with tradeoff explanation

#### 2.2 Daily Briefing
- **Daily/weekly digest**: Findings summary, actions taken, trends, upcoming concerns
- **Delivery**: Slack webhook, email (SMTP), stdout, NOTIFY/LISTEN, `sage.briefings` table
- **Executive mode**: Health score + 3-sentence summary
- **Deep dive mode**: Full technical detail
- **Trend narration**: Contextual narrative connecting findings to timeline
- **LLM fallback**: Structured rules-only output when LLM unavailable

#### 2.3 ReAct Diagnostic Loop
- **Reason → Act → Observe cycle**: LLM forms hypothesis, sage executes diagnostic query, LLM evaluates result, refines, repeats
- **Scoped schema injection**: See Context Assembly Pipeline below
- **Multi-signal correlation**: Performance degradation × DDL changes × config changes × vacuum events × checkpoint spikes
- **Conversational investigation**: "Why is the checkout endpoint slow?" → step-by-step diagnosis
- **Error resilience**: Failed diagnostic queries become signals, not dead ends
- **Iteration limit**: Maximum `sage.react_max_steps` (default: 10) steps per investigation to prevent runaway LLM cost
- **Runbook generation**: Auto-generate for recurring issues

#### 2.4 Context Assembly Pipeline

This is the core mechanism for all LLM interactions — how sage decides what database context to include in each prompt.

**Named-object questions** ("why is the orders table slow?"):
1. Parse question to identify referenced objects (table names, index names, query patterns)
2. Pull DDL for identified tables + tables connected via foreign keys (1 hop)
3. Index definitions on those tables
4. Top 10 `pg_stat_statements` entries touching those tables (by total_time), parameterized
5. Recent EXPLAIN plans from `sage.explain_cache` for those queries
6. Current bloat/dead tuple stats for those tables from latest snapshots
7. Active locks involving those tables from `pg_locks`
8. Autovacuum history (last vacuum time, dead tuples at last vacuum)
9. Open `sage.findings` already tagged to those objects
10. Package into structured prompt sections: `[SCHEMA]`, `[QUERIES]`, `[PLANS]`, `[STATS]`, `[FINDINGS]`, `[QUESTION]`

**Vague/system-wide questions** ("why is the database slow?"):
1. Check for active incidents: mean latency spike across all queryids in last hour
2. Top 5 regressed queries from latest analyzer cycle
3. System-wide metrics: CPU proxy (active backends / max_connections), checkpoint frequency, replication lag, connection count
4. Any CRITICAL-severity open findings
5. Recent autonomous actions (last 24h from `sage.action_log`)
6. Package as system health overview — broader but shallower than named-object context

**Token budget management** (`sage.llm_context_budget`, default: 4096 tokens for context):
- Priority order: (1) directly referenced table DDL, (2) relevant findings, (3) query text + stats, (4) EXPLAIN plans, (5) related table DDL, (6) system metrics
- Each section is measured. When budget is exhausted, remaining sections are truncated with a note: "[Schema context trimmed — N additional tables omitted. Ask about specific tables for detail.]"
- For very large tables (DDL > 500 tokens due to many columns), include column names only without types/constraints, with a note

**MCP client requests** (see MCP section):
- Same context assembly pipeline, but the question comes from an external AI agent via MCP protocol instead of sage shell
- sage enforces the same privacy model and token budgeting regardless of request source

#### 2.5 Cost Attribution Engine
- **Cloud cost mapping**: Configurable pricing tables (`sage.cloud_provider`, `sage.instance_type`)
- **IOPS cost translation**: Missing index → excess reads → monthly cost estimate
- **Storage cost projection**: Bloat → wasted storage → monthly cost
- **Compute waste identification**: Oversized instance detection from actual usage vs provisioned
- **ROI framing**: Every cost-attributed finding includes payback calculation
- **Action efficacy tracking**: Before/after mean latency and IOPS after autonomous changes. Actual ROI vs predicted

#### 2.6 Migration Review
- **Pre-deployment DDL analysis**: Lock implications, estimated duration, safer alternatives
- **Index impact prediction**: Write overhead from insert/update rates
- **Rollback plan generation**: Auto-generate rollback SQL

#### 2.7 Schema Design Review
- **Normalization analysis**
- **Data type recommendations**: Suboptimal types, timezone-naive timestamps
- **Foreign key coverage**: Implied relationships lacking FK constraints
- **Naming convention enforcement**

#### 2.8 Natural Language Interface (sage shell)
- **Ask anything about your database**: Uses Context Assembly Pipeline (2.4) to build the right prompt
- **Contextual**: Knows your schema, metrics history, recent changes, open findings
- **Interactive CLI**: `sage shell` opens a REPL. User types questions, sage assembles context, queries LLM, returns answer
- **Follow-up awareness**: Maintains conversation context within a session — "what about the users table?" resolves against the previous question's context
- **Action suggestions**: When the answer implies an action ("you should add an index on..."), sage outputs the exact DDL and offers to schedule it if Tier 3 is enabled
- **Offline mode**: Without LLM, `sage shell` still works — queries route to the rules engine and return structured findings. "Show me slow queries" → table of `sage.findings` where category = 'query'

---

### TIER 3: Autonomous Actions (Opt-in, Graduated Trust)

All actions respect trust ramp + maintenance windows + auto-rollback. Journaled in `sage.action_log`.

#### 3.1 Safe Actions (Day 8+, default: enabled)
- Drop confirmed-unused indexes (0 scans for window exceeding `sage.unused_index_window`)
- Adjust per-table autovacuum settings
- Generate and store EXPLAIN plans for new slow queries
- Kill idle-in-transaction sessions exceeding `sage.idle_session_timeout` (default: 30 min)

#### 3.2 Moderate Actions (Day 31+, default: approval required)
- Create new indexes recommended by analysis
- Adjust reload-safe `postgresql.conf` parameters with auto-rollback (mean latency regression >10% within 15 min, 7-day hysteresis)
- REINDEX bloated indexes during maintenance windows
- Adjust `statement_timeout` / `lock_timeout` per-role

#### 3.3 High-Risk Actions (Default: disabled, notify only)
- Schema modifications
- VACUUM FULL on large tables
- Restart-requiring parameter changes
- Replication topology changes

#### 3.4 Workload-Adaptive Parameter Scheduling
- Time-based profiles for reload-safe parameters, learned from workload patterns
- `ALTER SYSTEM` + `pg_reload_conf()`, no restart
- Subject to auto-rollback and maintenance windows

---

## MCP Server Interface (Phase 0.5+)

sage exposes a **Model Context Protocol (MCP) server** that allows any MCP-compatible AI tool to safely interact with your database through sage as the authorized gateway.

### Why This Matters

Every MCP client — Claude, Cursor, Windsurf, custom agents — gets safe, context-rich database access without direct database credentials. sage is the gatekeeper. The AI tool asks sage for context; sage assembles the right metadata, enforces the privacy model, applies token budgets, and logs the interaction. No AI tool ever touches the database directly.

### Capabilities Exposed via MCP

**Resources** (read-only context):
- `sage://schema/{table_name}` — DDL for a specific table + indexes + constraints
- `sage://stats/{table_name}` — size, bloat, row count, index usage, vacuum status
- `sage://slow-queries` — current slow queries with timing stats
- `sage://findings` — open findings with severity and recommendations
- `sage://health` — system health overview (connections, cache hit ratio, replication lag, disk usage)
- `sage://explain/{queryid}` — cached or on-demand EXPLAIN plan

**Tools** (actions, subject to trust level + maintenance windows):
- `sage.diagnose(question)` — runs the full ReAct diagnostic loop and returns analysis
- `sage.suggest_index(table_name)` — returns index recommendations with DDL
- `sage.review_migration(ddl_text)` — analyzes DDL for risk
- `sage.briefing()` — generates current health briefing

**Prompts** (pre-built prompt templates):
- `investigate_slow_query` — template for slow query root cause analysis
- `review_schema` — template for schema design review
- `capacity_plan` — template for capacity planning discussion

### Safety Model

- MCP clients inherit sage's privacy model — metadata only, never row data
- Token budgets apply per-client (`sage.mcp_token_budget`, default: 10000/request)
- All MCP interactions are logged in `sage.mcp_log` (client_id, request, response_size, timestamp)
- MCP tools (actions) respect trust ramp and maintenance windows — an MCP client cannot bypass sage's safety systems
- Rate limiting: `sage.mcp_rate_limit` (default: 60 requests/minute per client)

### Distribution Play

This is the highest-leverage feature in the spec. Every MCP-compatible AI tool becomes a potential sage user. A developer using Cursor to write code can ask "is this migration safe?" and get a real answer from sage, backed by actual database metrics — not generic LLM knowledge. When pg_mage ships, MCP clients get learned optimization and natural language queries through the same interface for free.

---

## Schema Reference

### sage.snapshots

Stores raw metric snapshots from the collector. Partitioned by time (if table size warrants).

```sql
CREATE TABLE sage.snapshots (
    id              BIGSERIAL PRIMARY KEY,
    collected_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    category        TEXT NOT NULL,  -- 'tables', 'indexes', 'queries', 'system', 'locks', 'sequences', 'replication'
    data            JSONB NOT NULL  -- category-specific metrics blob
);
CREATE INDEX idx_snapshots_time ON sage.snapshots (collected_at DESC);
CREATE INDEX idx_snapshots_category ON sage.snapshots (category, collected_at DESC);
```

### sage.findings

Core output of the rules engine.

```sql
CREATE TABLE sage.findings (
    id                  BIGSERIAL PRIMARY KEY,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_seen           TIMESTAMPTZ NOT NULL DEFAULT now(),
    occurrence_count    INTEGER NOT NULL DEFAULT 1,
    category            TEXT NOT NULL,       -- 'index', 'vacuum', 'config', 'query', 'security',
                                             -- 'replication', 'sequence', 'capacity', 'backup',
                                             -- 'cache', 'sage_health'
    severity            TEXT NOT NULL,       -- 'info', 'warning', 'critical'
    object_type         TEXT,                -- 'table', 'index', 'query', 'role', 'config_param',
                                             -- 'sequence', 'replication_slot', 'matview', 'system'
    object_identifier   TEXT,                -- e.g., 'public.orders', 'idx_users_email', queryid
    title               TEXT NOT NULL,       -- one-line summary
    detail              JSONB NOT NULL,      -- structured evidence (metrics, thresholds, comparisons)
    recommendation      TEXT,                -- human-readable recommendation
    recommended_sql     TEXT,                -- exact DDL/SQL to remediate (if applicable)
    rollback_sql        TEXT,                -- DDL to undo the recommendation (if applicable)
    estimated_cost_usd  NUMERIC(10,2),       -- monthly cloud cost impact (if calculable)
    status              TEXT NOT NULL DEFAULT 'open',  -- 'open', 'suppressed', 'resolved', 'acted_on'
    suppressed_until    TIMESTAMPTZ,
    resolved_at         TIMESTAMPTZ,
    acted_on_at         TIMESTAMPTZ,
    action_log_id       BIGINT REFERENCES sage.action_log(id),
    UNIQUE (category, object_identifier, status) -- dedup key (for open findings)
);
CREATE INDEX idx_findings_status ON sage.findings (status, severity, last_seen DESC);
CREATE INDEX idx_findings_object ON sage.findings (object_identifier, category);
```

### sage.action_log

Audit trail for every autonomous action.

```sql
CREATE TABLE sage.action_log (
    id              BIGSERIAL PRIMARY KEY,
    executed_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    action_type     TEXT NOT NULL,       -- 'drop_index', 'create_index', 'alter_config',
                                          -- 'reindex', 'kill_session', 'autovacuum_tune'
    finding_id      BIGINT REFERENCES sage.findings(id),
    sql_executed    TEXT NOT NULL,
    rollback_sql    TEXT,
    before_state    JSONB,               -- metrics snapshot before action
    after_state     JSONB,               -- metrics snapshot after action (populated post-monitoring)
    outcome         TEXT NOT NULL DEFAULT 'pending',  -- 'pending', 'success', 'rolled_back', 'failed'
    rollback_reason TEXT,
    measured_at     TIMESTAMPTZ          -- when after_state was measured
);
CREATE INDEX idx_action_log_time ON sage.action_log (executed_at DESC);
```

### sage.explain_cache

Captured EXPLAIN plans.

```sql
CREATE TABLE sage.explain_cache (
    id              BIGSERIAL PRIMARY KEY,
    captured_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    queryid         BIGINT NOT NULL,     -- from pg_stat_statements
    query_text      TEXT,                -- parameterized SQL (nullable if redacted)
    plan_json       JSONB NOT NULL,      -- EXPLAIN output
    source          TEXT NOT NULL,       -- 'auto_explain', 'manual', 'diagnostic'
    total_cost      FLOAT,               -- extracted from plan for quick comparison
    execution_time  FLOAT                -- actual time if ANALYZE was used
);
CREATE INDEX idx_explain_queryid ON sage.explain_cache (queryid, captured_at DESC);
```

### sage.briefings

Generated briefing output.

```sql
CREATE TABLE sage.briefings (
    id              BIGSERIAL PRIMARY KEY,
    generated_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    period_start    TIMESTAMPTZ NOT NULL,
    period_end      TIMESTAMPTZ NOT NULL,
    mode            TEXT NOT NULL,       -- 'executive', 'deep_dive'
    content_text    TEXT NOT NULL,        -- rendered briefing
    content_json    JSONB NOT NULL,      -- structured data behind the briefing
    llm_used        BOOLEAN NOT NULL DEFAULT false,
    token_count     INTEGER,
    delivery_status JSONB                -- per-channel delivery results
);
```

### sage.config

Runtime configuration (supplements GUCs for sidecar mode).

```sql
CREATE TABLE sage.config (
    key             TEXT PRIMARY KEY,
    value           TEXT NOT NULL,
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_by      TEXT                 -- 'user', 'sage_auto', 'trust_ramp'
);
```

### sage.mcp_log (Phase 0.5+)

MCP interaction audit trail.

```sql
CREATE TABLE sage.mcp_log (
    id              BIGSERIAL PRIMARY KEY,
    requested_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    client_id       TEXT NOT NULL,
    request_type    TEXT NOT NULL,       -- 'resource', 'tool', 'prompt'
    request_detail  TEXT NOT NULL,
    response_tokens INTEGER,
    duration_ms     INTEGER
);
```

### Schema Extension Upgrades

sage uses PostgreSQL's built-in extension versioning. Each version bump includes a migration script:

```
pg_sage--0.1.0--0.2.0.sql
pg_sage--0.2.0--0.3.0.sql
```

Upgrade via standard: `ALTER EXTENSION pg_sage UPDATE TO '0.2.0';`

Migrations are additive (new tables, new columns with defaults, new indexes). Destructive changes (column type changes, table drops) are avoided. When unavoidable, data is migrated in-transaction.

---

## Findings Engine

### Deduplication & Suppression

- **Dedup key**: `(category, object_identifier, status='open')`. New finding matching an open finding updates `last_seen` and increments `occurrence_count`
- **Escalation**: Same finding open for 7 consecutive days → severity escalates one level (info → warning → critical). Briefing highlights escalations
- **Suppression**: `SELECT sage.suppress(finding_id, reason, duration_days)`. Hidden from briefings, tracked internally. Auto-expires (default: 30 days)
- **Resolution**: When condition clears, finding marked `resolved` with timestamp. Next briefing reports it as positive news
- **Partition-aware dedup**: Findings for child partitions are grouped under the parent. Only outlier children surface individually

### Data Retention

- **Defaults**: `sage.snapshots` 90 days, `sage.findings` 180 days, `sage.action_log` 365 days, `sage.briefings` 365 days, `sage.explain_cache` 90 days, `sage.mcp_log` 90 days
- **Configurable**: `sage.retention_snapshots`, `sage.retention_findings`, etc.
- **Auto-cleanup**: Batched DELETE (1000 rows per cycle) during analyzer runs. No long-running transactions
- **Self-monitoring**: sage schema size tracked in `sage_health`. Alert if exceeding `sage.max_schema_size` (default: 1GB)
- **Irony clause**: sage's own tables are included in its vacuum and bloat analysis

---

## Edge Cases & Mitigations

| Scenario | Mitigation |
|----------|-----------|
| **Massive schemas (10K+ tables)** | Incremental collection (`sage.collector_batch_size`). Rotating subset. Circuit breaker on slow queries. |
| **Managed cloud (RDS/Cloud SQL)** | Sidecar mode. No ALTER SYSTEM, no BG workers. Graceful degradation documented. |
| **Complex HA (Patroni + cascading replicas)** | Role detection every cycle. Safe mode during ambiguous transitions. |
| **High DDL churn (migration storms)** | Detect elevated DDL rate from `pg_stat_activity`. Pause analyzer until quiet. |
| **pg_upgrade in progress** | Detect process. Enter dormant mode until complete. |
| **LLM outage / rate limits** | LLM circuit breaker. Tier 1 unaffected. Briefing falls back to structured output. |
| **K8s CPU throttling** | Circuit breaker. Documented resource requests: 100m CPU, 128Mi RAM. |
| **Multiple PG databases on same cluster** | Per-database install. Advisory lock prevents duplicate instances per database. |
| **Multiple PG instances on same host** | Each gets own extension + sage schema. Distinct `application_name`. |
| **Extension conflicts** | Detect co-installed extensions at startup. Warn on external `pg_stat_statements` resets (counter discontinuity). Document safe co-existence. |
| **Findings flood** | Self-monitoring detects >10x baseline volume. Auto-suppress low-severity. Alert operator. |
| **Partitioned tables (500+ partitions)** | Partition-aware analysis. Report at parent level. Surface outlier children only. |
| **auto_explain not loaded** | Degrade gracefully. EXPLAIN capture disabled. Finding recommends installing it. Manual `sage.explain()` still works. |
| **Concurrent sage instances** | Advisory lock on startup. Second instance refuses to start with clear error message. |
| **Extension upgrade (0.1 → 0.2)** | Standard `ALTER EXTENSION pg_sage UPDATE`. Additive migrations. No data loss. |
| **Read-only filesystem (some containers)** | sage only writes to the database (sage schema), not to the filesystem. No temp files needed. |

---

## Privacy Model

**NEVER sent to LLM:**
- Actual row data / table contents
- User credentials or connection strings
- PII from any table

**Sent when LLM connected (metadata only):**
- Schema DDL (scoped via Context Assembly Pipeline, never full catalog)
- EXPLAIN plan JSON
- `pg_stat_statements` query text (already parameterized by Postgres: `$1, $2`)
- Aggregate metrics (row counts, bloat %, timing stats)
- Configuration parameter names/values
- Structured findings from rules engine

**Controls:**
- `sage.llm_enabled` (default: `true` if endpoint configured, Tier 1 works regardless)
- `sage.redact_queries` (default: `false`) — strip SQL text, send plan structure only
- `sage.anonymize_schema` (default: `false`) — replace names with tokens
- `sage.llm_token_budget` (default: `50000` tokens/day)
- `sage.llm_features` — comma-separated list of enabled LLM features (default: `'briefing,explain,diagnostic,shell'`)

---

## Configuration Reference

All configuration via PostgreSQL GUCs (`sage.*` namespace) or `sage.config` table (sidecar mode).

| Parameter | Type | Default | Hot-Reload | Description |
|-----------|------|---------|-----------|-------------|
| `sage.enabled` | bool | `true` | Yes | Master enable/disable |
| `sage.collector_interval` | integer (sec) | `60` | Yes | Seconds between collector cycles |
| `sage.analyzer_interval` | integer (sec) | `600` | Yes | Seconds between analyzer cycles |
| `sage.collector_batch_size` | integer | `1000` | Yes | Tables per collector cycle (incremental mode) |
| `sage.slow_query_threshold` | integer (ms) | `1000` | Yes | Mean exec time to flag as slow |
| `sage.seq_scan_min_rows` | integer | `100000` | Yes | Minimum table rows to flag seq scans |
| `sage.unused_index_window` | text | `'30d'` | Yes | Window for unused index detection |
| `sage.index_bloat_threshold` | integer (%) | `30` | Yes | Bloat % to flag |
| `sage.idle_session_timeout` | integer (min) | `30` | Yes | Idle-in-transaction kill threshold |
| `sage.disk_pressure_threshold` | integer (%) | `5` | Yes | Free disk % to suspend writes |
| `sage.max_connections` | integer | `2` | No (restart) | sage's connection pool size |
| `sage.trust_level` | text | `'observation'` | Yes | `'observation'`, `'advisory'`, `'autonomous'` |
| `sage.trust_ramp_start` | timestamptz | (install time) | No | Trust ramp start date |
| `sage.maintenance_window` | text | `''` (disabled) | Yes | Cron syntax with timezone |
| `sage.rollback_threshold` | integer (%) | `10` | Yes | Mean latency regression % to trigger rollback |
| `sage.rollback_window` | integer (min) | `15` | Yes | Monitoring window after config change |
| `sage.rollback_cooldown` | integer (days) | `7` | Yes | Hysteresis period per parameter |
| `sage.briefing_schedule` | text | `'0 6 * * * UTC'` | Yes | Briefing cron schedule |
| `sage.briefing_channels` | text | `'stdout'` | Yes | Comma-separated: `stdout`, `slack`, `email`, `table` |
| `sage.slack_webhook_url` | text | `''` | Yes | Slack webhook URL |
| `sage.email_smtp_url` | text | `''` | Yes | SMTP connection string |
| `sage.llm_enabled` | bool | `true` | Yes | Enable LLM features |
| `sage.llm_endpoint` | text | `''` | Yes | OpenAI-compatible API URL |
| `sage.llm_api_key` | text | `''` | Yes | API key (stored in PG, not in logs) |
| `sage.llm_model` | text | `''` | Yes | Model name (e.g., `claude-sonnet-4-20250514`) |
| `sage.llm_timeout` | integer (sec) | `30` | Yes | Per-call timeout |
| `sage.llm_token_budget` | integer | `50000` | Yes | Daily token cap |
| `sage.llm_context_budget` | integer | `4096` | Yes | Max tokens for schema context per call |
| `sage.llm_features` | text | `'briefing,explain,diagnostic,shell'` | Yes | Enabled LLM features |
| `sage.llm_cooldown` | integer (sec) | `300` | Yes | Circuit breaker cooldown |
| `sage.redact_queries` | bool | `false` | Yes | Strip SQL text from LLM context |
| `sage.anonymize_schema` | bool | `false` | Yes | Replace names with tokens |
| `sage.react_max_steps` | integer | `10` | Yes | Max ReAct diagnostic iterations |
| `sage.cloud_provider` | text | `''` | Yes | `'aws'`, `'gcp'`, `'azure'`, `''` (for cost attribution) |
| `sage.instance_type` | text | `''` | Yes | e.g., `'db.r6g.xlarge'` (for cost attribution) |
| `sage.retention_snapshots` | integer (days) | `90` | Yes | Snapshot retention |
| `sage.retention_findings` | integer (days) | `180` | Yes | Findings retention |
| `sage.retention_actions` | integer (days) | `365` | Yes | Action log retention |
| `sage.retention_explains` | integer (days) | `90` | Yes | EXPLAIN cache retention |
| `sage.max_schema_size` | text | `'1GB'` | Yes | Alert threshold for sage schema size |
| `sage.mcp_enabled` | bool | `false` | Yes | Enable MCP server |
| `sage.mcp_port` | integer | `5433` | No (restart) | MCP server listen port |
| `sage.mcp_token_budget` | integer | `10000` | Yes | Max tokens per MCP request |
| `sage.mcp_rate_limit` | integer | `60` | Yes | Max MCP requests per minute per client |
| `sage.autoexplain_sample_rate` | float | `0.01` | Yes | auto_explain baseline sample rate |
| `sage.autoexplain_capture_window` | integer (sec) | `300` | Yes | Duration of targeted capture mode |

---

## Competitive Differentiation

| Capability | PostgresAI | DBtune | Supabase Skills | Xata Agent | pg_sage |
|-----------|-----------|--------|---------|---------|---------|
| Self-hosted / local-first | ✗ (SaaS) | ✗ (SaaS) | Framework | ✗ (SaaS) | **✓** |
| Managed cloud support | ✓ | ✓ | ✓ | ✓ | **✓ (sidecar)** |
| Continuous autonomous | ✓ | Partial | ✗ | ✓ | **✓** |
| Full DBA surface area | Partial | Config only | Guidelines | Monitoring | **✓** |
| Pluggable LLM backend | ✗ | ✗ | ✗ | ✗ | **✓** |
| MCP server | ✗ | ✗ | ✗ | ✗ | **✓** |
| Cost attribution | ✗ | ✗ | ✗ | ✗ | **✓** |
| ReAct diagnostic | ✗ | ✗ | ✗ | ✗ | **✓** |
| Trust ramp | ✗ | ✗ | N/A | ✗ | **✓** |
| Auto-rollback | ✗ | ✗ | N/A | ✗ | **✓** |
| Self-monitoring | ✗ | ✗ | N/A | ✗ | **✓** |
| Works without LLM | N/A | N/A | N/A | ✗ | **✓ (Tier 1)** |
| Open source | Partial | ✗ | ✓ | ✗ | **✓ (AGPL-3.0)** |

---

## Tech Stack

- **Core extension**: Rust via `pgrx` — background workers, catalog queries, rules engine
- **Sidecar** (optional): Rust — LLM calls, dashboard, Prometheus, MCP server, alerting
- **LLM interface**: OpenAI-compatible API (Claude, OpenAI, Ollama, vLLM, OpenRouter, LiteLLM)
- **MCP server**: Embedded in sidecar, standard MCP protocol over SSE
- **CLI**: `sage` binary — shell, triggers, config
- **Alerting**: Slack, email (SMTP), stdout, NOTIFY/LISTEN, `sage.briefings` table
- **Storage**: `sage` schema in monitored database
- **License**: AGPL-3.0

---

## MVP Scope (Phase 0.1)

1. **Index lifecycle** — unused, duplicate (full structural match), missing suggestions, write penalty scoring
2. **Slow query detection** — `pg_stat_statements` monitoring + auto_explain sampling capture
3. **Sequence exhaustion monitoring**
4. **Config audit** — hardware-aware baseline, hot-reload awareness
5. **Circuit breaker** — CPU/query/disk limits
6. **HA awareness** — role detection, replica-safe mode
7. **Self-monitoring** — resource footprint tracking
8. **LLM connector** — pluggable backend, EXPLAIN plan narration
9. **Context assembly pipeline** — scoped schema injection for all LLM calls
10. **Daily briefing** — stdout/Slack, LLM fallback to structured output

**Target**: 4-6 weeks with Claude Code. PG14+. Requires `pg_stat_statements`. One `CREATE EXTENSION` and it's running.

### Post-MVP Roadmap

| Phase | Features | Est. Timeline |
|-------|----------|---------------|
| **0.2** | Vacuum/bloat management, security audit, backup health, materialized views, sidecar mode | +3-4 weeks |
| **0.3** | Maintenance windows, trust ramp, Tier 3 autonomous actions, auto-rollback, findings dedup | +3-4 weeks |
| **0.4** | ReAct diagnostic loop, cost attribution, data retention, sage shell CLI | +4-5 weeks |
| **0.5** | Migration review, schema review, MCP server, workload-adaptive scheduling, Prometheus | +4-5 weeks |
| **1.0** | Production-hardened. Full Tier 1-3. Extension + sidecar. MCP. | ~6 months total |

---

## The Wedge → Platform Play

```
pg_sage (AI DBA agent) — AGPL-3.0
  └─→ Installed on thousands of Postgres instances
       └─→ MCP server makes sage the gateway for all AI-database interaction
            └─→ pg_mage: Learned query steering (Bao/Lero)
                 └─→ pg_mage: pnl (natural language queries)
                      └─→ pg_mage: pg_semantic (vector metadata)
                           └─→ pg_mage: GPU-accelerated execution
                                └─→ Full Clawgres vision
```

The sage earns trust. The mage wields power.

pg_sage is not a side project — it's the distribution engine for the entire Clawgres platform. The MCP server is the multiplier — every AI tool that speaks MCP becomes a sage client, and every sage client becomes a future pg_mage user.
