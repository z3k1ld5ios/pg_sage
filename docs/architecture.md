# Architecture

pg_sage implements a three-tier architecture that separates deterministic analysis from LLM-enhanced intelligence and automated remediation. The system runs entirely inside PostgreSQL as background workers with shared memory for coordination.

---

## Three-Tier Design

### Tier 1 -- Rules Engine

Deterministic checks that run every analyzer interval. No LLM or external dependencies required.

| Category | What It Detects |
|---|---|
| Index health | Duplicate, unused, missing indexes; index bloat |
| Query performance | Slow queries, regressions, sequential scans on large tables |
| Sequences | Approaching exhaustion (integer/bigint overflow) |
| Maintenance | Vacuum needs, table bloat, dead tuples, XID wraparound |
| Configuration | `postgresql.conf` audit against best practices |
| Security | Overprivileged roles, missing RLS on sensitive tables |
| Replication | Lag monitoring, inactive slots, WAL archiving staleness |
| Self-monitoring | Extension health, circuit breaker status, schema footprint |

### Tier 2 -- LLM-Enhanced Analysis

Optional features powered by an external LLM. When no endpoint is configured, these degrade gracefully to Tier 1 behavior.

- **Daily briefings** -- natural-language health summaries
- **Interactive diagnose** -- ReAct reasoning loop that executes follow-up SQL autonomously
- **Explain narrative** -- human-readable query plan analysis
- **Cost attribution** -- map storage/IOPS costs to unused and missing indexes
- **Migration review** -- detect long-running DDL blocking production
- **Schema design review** -- timezone-naive timestamps, missing PKs, naming issues

### Tier 3 -- Action Executor

Automated remediation with graduated trust. Runs inside the analyzer worker after all analysis functions complete.

Actions are classified by risk:

| Risk Level | Examples |
|---|---|
| **SAFE** | Drop unused/duplicate indexes, vacuum tuning |
| **MODERATE** | CREATE INDEX, REINDEX, configuration changes |
| **HIGH** | Everything else -- logged only, requires manual confirmation |

---

## Background Workers

pg_sage registers three background workers with PostgreSQL:

```
PostgreSQL
├── Collector Worker    (sage.collector_interval, default 60s)
│   └── Captures snapshots from pg_stat_statements, pg_stat_user_tables,
│       pg_stat_user_indexes, sequences, system catalog
│
├── Analyzer Worker     (sage.analyzer_interval, default 600s)
│   ├── Runs all Tier 1 analysis checks
│   ├── Runs Tier 2 LLM analysis (if enabled)
│   ├── Runs Tier 3 action executor (if trust > observation)
│   └── Performs retention cleanup
│
└── Briefing Worker     (sage.briefing_schedule, default 06:00 UTC)
    └── Generates daily health briefing
```

All workers connect to the database specified by `sage.database` and coordinate through shared memory.

---

## Shared Memory Layout

pg_sage requests a single shared memory segment (`SageSharedState`) at startup:

| Field | Purpose |
|---|---|
| `lock` | LWLock for coordinated access |
| `circuit_state` | Database operations circuit breaker state |
| `llm_circuit_state` | LLM API circuit breaker state |
| `collector_running` | Whether the collector worker is active |
| `analyzer_running` | Whether the analyzer worker is active |
| `briefing_running` | Whether the briefing worker is active |
| `emergency_stopped` | Set by `sage.emergency_stop()` |
| `consecutive_skips` | Circuit breaker skip counter |
| `consecutive_successes` | Circuit breaker success counter |
| `last_circuit_change` | Timestamp of last circuit state transition |
| Adaptive scheduling fields | Baseline transaction rate, sample count |

Workers acquire `LW_SHARED` locks for reads and `LW_EXCLUSIVE` for writes. Lock hold times are kept minimal to avoid contention.

---

## Circuit Breaker Design

pg_sage must never become the incident. The circuit breaker monitors system load, disk pressure, and its own health to automatically back off.

**Two independent breakers:**

| Breaker | Protects Against |
|---|---|
| Database (`circuit_state`) | Excessive SPI queries, disk pressure, CPU load |
| LLM (`llm_circuit_state`) | API timeouts, rate limits, network failures |

**State machine:**

```
CLOSED  ──(errors exceed threshold)──►  OPEN
   ▲                                      │
   │                                      │ (cooldown expires)
   │                                      ▼
   └──────(probe succeeds)────────────  DORMANT
```

- **CLOSED**: normal operation
- **OPEN**: all operations skipped; `sage.emergency_stop()` forces this state
- **DORMANT**: periodic probe attempts; on success, transitions back to CLOSED

The circuit breaker checks:

- CPU utilization (via `/proc/stat` on Linux)
- Disk free space (via `statvfs`)
- Consecutive error counts
- `sage.max_schema_size` threshold

---

## Trust Ramp Model

pg_sage uses a graduated trust model to control autonomous actions:

```
Day 0                    Day 8                    Day 31
  │                        │                        │
  ▼                        ▼                        ▼
OBSERVATION ──────────► ADVISORY ──────────────► AUTONOMOUS
  (findings only)        (SAFE actions)           (SAFE + MODERATE)
```

| Trust Level | Timeline | Allowed Actions |
|---|---|---|
| `observation` | Day 0--7 | No actions; findings only |
| `advisory` | Day 8--30 | SAFE: drop unused/duplicate indexes, vacuum tuning |
| `autonomous` | Day 31+ | MODERATE: create indexes, reindex, configuration changes |

HIGH-risk actions always require manual confirmation regardless of trust level.

Every action is logged to `sage.action_log` with:

- Before-state snapshot
- The executed SQL
- Rollback SQL
- After-state snapshot
- Timestamp and trust level

The rollback checker monitors p95 latency after each action. If latency regresses beyond `sage.rollback_threshold` (default 10%) within `sage.rollback_window` (default 15 minutes), the action is automatically reverted.

---

## Workload-Adaptive Scheduling

The analyzer adjusts its interval based on database transaction rate:

- **High activity** -- interval shrinks to 1/3 of the base (minimum 30 seconds)
- **Low activity** -- interval grows to 3x the base
- Auto-calibrates a baseline from the first 5 samples

This ensures pg_sage analyzes more frequently during peak load and backs off during quiet periods.

| Constant | Value |
|---|---|
| `ADAPTIVE_BASELINE_SAMPLES` | 5 |
| `ADAPTIVE_MIN_MULTIPLIER` | 0.33 |
| `ADAPTIVE_MAX_MULTIPLIER` | 3.0 |
| `ADAPTIVE_MIN_INTERVAL_MS` | 30000 (30s floor) |

---

## MCP Sidecar Architecture

The MCP sidecar is a separate Go binary that bridges AI assistants to pg_sage via the Model Context Protocol.

```
┌──────────────────────┐     MCP (JSON-RPC over SSE)     ┌─────────────────┐
│  AI Assistant / IDE  │ ◄──────────────────────────────► │  sage-sidecar   │
│  (Claude, Cursor,    │          port 5433               │  (Go binary)    │
│   Copilot, etc.)     │                                  └────────┬────────┘
└──────────────────────┘                                           │ SQL
                                                          ┌────────▼────────┐
                                                          │   PostgreSQL    │
                                                          │   + pg_sage     │
                                                          └─────────────────┘
```

The sidecar:

- Connects to PostgreSQL via `pgx` connection pool
- Calls SQL functions (`sage.health_json()`, `sage.findings_json()`, etc.)
- Opens no additional ports on the database
- Auto-detects whether the pg_sage extension is installed
- Falls back to direct catalog queries in sidecar-only mode
- Manages SSE sessions with per-client state
- Enforces per-IP rate limiting and optional `SAGE_API_KEY` authentication

See [MCP Sidecar](mcp-sidecar.md) for the full protocol reference.

---

## Schema

All pg_sage objects live in the `sage` schema:

| Table | Purpose |
|---|---|
| `sage.snapshots` | Point-in-time system state captures |
| `sage.findings` | Detected issues with severity, recommendation, remediation SQL |
| `sage.action_log` | Audit trail for every autonomous action with rollback metadata |
| `sage.explain_cache` | Cached EXPLAIN plans keyed by queryid |
| `sage.briefings` | Generated briefing reports with delivery status |
| `sage.config` | Extension configuration overrides |
| `sage.mcp_log` | Audit log of MCP sidecar requests |
