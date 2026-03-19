# pg_sage

[![License: AGPL-3.0](https://img.shields.io/badge/License-AGPL--3.0-blue.svg)](https://www.gnu.org/licenses/agpl-3.0)
[![PostgreSQL 14+](https://img.shields.io/badge/PostgreSQL-14%2B-336791.svg)](https://www.postgresql.org/)
[![Works without LLM](https://img.shields.io/badge/LLM-optional-green.svg)](#tier-2----llm-enhanced-analysis)

**Autonomous PostgreSQL DBA Agent** -- a native C extension that continuously monitors, analyzes, and maintains your PostgreSQL database.

pg_sage runs inside PostgreSQL as three background workers and exposes its capabilities through SQL functions in the `sage` schema. It combines a deterministic rules engine with optional LLM-enhanced analysis and a trust-ramped action executor that gradually earns autonomy over time.

All Tier 1 analysis runs without any external dependencies. LLM integration is optional and only enhances Tier 2 features (briefings, diagnose, explain narrative).

---

## Quick Start

```bash
git clone https://github.com/jasonmassie01/pg_sage.git
cd pg_sage
docker compose up
```

Once the container is running:

```bash
docker exec -it pg_sage-pg_sage-1 psql -U postgres
```

```sql
-- Extension is auto-loaded via shared_preload_libraries
-- Check system status
SELECT * FROM sage.status();

-- See what pg_sage found
SELECT category, severity, title FROM sage.findings WHERE status = 'open' ORDER BY severity;

-- Get a health briefing
SELECT sage.briefing();
```

Example output after ~60 seconds:

```
 category            | severity | title
---------------------+----------+---------------------------------------------------------------
 duplicate_index     | critical | Duplicate index public.idx_orders_dup2 matches idx_orders_dup1
 sequence_exhaustion | critical | Sequence public.orders_seq at 93.1% capacity (integer)
 config              | warning  | shared_buffers below recommended 25% of RAM
 security_missing_rls| warning  | Table public.customers has sensitive columns but no RLS
 unused_index        | warning  | Unused index public.idx_old on public.orders (zero scans)
 config              | info     | max_connections significantly exceeds peak usage
```

---

## Why pg_sage?

| | pg_sage | pganalyze | OtterTune / DBtune |
|---|---|---|---|
| **Runs inside Postgres** | Native C extension, zero external infra | SaaS agent + cloud dashboard | Cloud-only SaaS |
| **Takes action** | Trust-ramped autonomous remediation | Recommendations only | Knob tuning only |
| **Self-hosted** | Fully, AGPL-3.0 | Proprietary | Proprietary |
| **LLM dependency** | Optional (Tier 1 works without it) | N/A | Required |

---

## Architecture

pg_sage implements a three-tier architecture:

### Tier 1 -- Rules Engine

Deterministic checks that run every analyzer interval, no LLM required:

| Category | What it detects |
|---|---|
| **Index health** | Duplicate indexes, unused indexes, missing indexes, index bloat |
| **Query performance** | Slow queries, query regressions, sequential scans on large tables |
| **Sequences** | Approaching exhaustion (bigint/int overflow) |
| **Maintenance** | Vacuum needs, table bloat, dead tuple accumulation, XID wraparound |
| **Configuration** | Audit of `postgresql.conf` against best practices |
| **Security** | Overprivileged roles, missing RLS on sensitive tables |
| **Replication** | Lag monitoring, inactive slots, WAL archiving staleness |
| **Self-monitoring** | Extension health, circuit breaker status, schema footprint |

### Tier 2 -- LLM-Enhanced Analysis

Optional features that use an external LLM for natural-language intelligence:

- **Daily briefings** -- summarized health reports delivered on schedule
- **Interactive diagnose** -- ReAct loop that reasons through problems step by step, executing follow-up SQL queries autonomously
- **Explain narrative** -- human-readable query plan analysis via `sage.explain(queryid)`
- **Cost attribution** -- map storage and IOPS costs to unused indexes and missing indexes
- **Migration review** -- detect long-running DDL blocking production
- **Schema design review** -- timezone-naive timestamps, missing PKs, naming issues

**EXPLAIN plan capture**: Plans are captured on-demand via `sage.explain(queryid)`, which runs `EXPLAIN (FORMAT JSON, COSTS, VERBOSE)` against the query text from `pg_stat_statements` and caches the result. No `auto_explain` dependency required.

### Tier 3 -- Action Executor

Automated remediation with a graduated trust model:

| Trust Level | Timeline | Allowed Actions |
|---|---|---|
| **OBSERVATION** | Day 0--7 | No actions; findings only |
| **ADVISORY** | Day 8--30 | SAFE actions (drop unused/duplicate indexes, vacuum tuning) |
| **AUTONOMOUS** | Day 31+ | MODERATE actions (create indexes, reindex, configuration changes) |

HIGH-risk actions always require manual confirmation regardless of trust level.

Every autonomous action is logged to `sage.action_log` with before/after state and rollback SQL. The rollback checker monitors for p95 latency regressions and automatically reverts actions that degrade performance.

---

## SQL Functions

```sql
-- System status as JSONB
SELECT * FROM sage.status();

-- Daily health briefing (works with or without LLM)
SELECT * FROM sage.briefing();

-- Interactive diagnostic with ReAct reasoning (Tier 2)
SELECT * FROM sage.diagnose('Why are my queries slow today?');

-- Human-readable query plan narrative (Tier 2)
SELECT * FROM sage.explain(query_id);

-- Suppress a specific finding
SELECT sage.suppress(finding_id, 'Known issue, vendor fix pending', 30);

-- Emergency controls
SELECT sage.emergency_stop();   -- halt all autonomous activity immediately
SELECT sage.resume();           -- resume normal operation
```

---

## Schema

All objects live in the `sage` schema:

| Table | Purpose |
|---|---|
| `sage.snapshots` | Point-in-time system state captures (indexes, tables, sequences, system) |
| `sage.findings` | Detected issues with severity, recommendation, and remediation SQL |
| `sage.action_log` | Audit trail for every autonomous action with rollback metadata |
| `sage.explain_cache` | Cached EXPLAIN plans keyed by queryid |
| `sage.briefings` | Generated briefing reports with delivery status |
| `sage.config` | Extension configuration overrides |

---

## Configuration (GUCs)

Set these in `postgresql.conf` or via `ALTER SYSTEM`:

| Parameter | Default | Description |
|---|---|---|
| `sage.enabled` | `on` | Master enable/disable switch |
| `sage.collector_interval` | `30s` | Interval between snapshot collections |
| `sage.analyzer_interval` | `60s` | Interval between analysis runs |
| `sage.trust_level` | `observation` | Current trust tier (`observation`, `advisory`, `autonomous`) |
| `sage.slow_query_threshold` | `1s` | Slow query threshold |
| `sage.seq_scan_min_rows` | `100000` | Minimum table rows for sequential scan alerts |
| `sage.rollback_threshold` | `10` | p95 latency regression % that triggers automatic rollback |
| `sage.llm_enabled` | `on` | Enable Tier 2 LLM features (gracefully degrades when no endpoint configured) |

---

## Circuit Breaker

pg_sage includes a circuit breaker that protects your database from runaway analysis or action loops:

- **Separate breakers** for database operations and LLM calls
- Trips automatically when error thresholds are exceeded
- `sage.emergency_stop()` trips both breakers immediately
- `sage.resume()` resets breakers and resumes normal operation

---

## Installation

### Prerequisites

- PostgreSQL 14, 15, 16, or 17
- `pg_stat_statements` extension
- `libcurl` development headers (for optional LLM integration)

### Docker (recommended)

```bash
docker compose up
```

The included `docker-compose.yml` configures `shared_preload_libraries`, `pg_stat_statements`, and default GUCs automatically.

### From Source

```bash
make
sudo make install
```

Add to `postgresql.conf`:

```
shared_preload_libraries = 'pg_stat_statements,pg_sage'
sage.database = 'postgres'
```

Restart PostgreSQL, then:

```sql
CREATE EXTENSION pg_stat_statements;
CREATE EXTENSION pg_sage;
```

> **Note on managed services**: pg_sage requires `shared_preload_libraries` access, which is not available on RDS, Aurora, or Cloud SQL. A sidecar deployment mode for managed databases is planned for a future release.

---

## Testing

Three test suites are included:

| Suite | Tests | Purpose |
|---|---|---|
| `test/regression.sql` | 27 | Core functionality and schema validation |
| `test/run_tests.sql` | 14 | Integration tests across tiers |
| `test/test_all_features.sql` | -- | Comprehensive feature coverage (all tiers) |

Run against the Docker container:

```bash
# Full feature test
docker exec -i pg_sage-pg_sage-1 psql -U postgres < test/test_all_features.sql

# Regression suite (27 pass/fail assertions)
docker exec -i pg_sage-pg_sage-1 psql -U postgres < test/regression.sql
```

---

## File Structure

```
pg_sage/
├── Dockerfile
├── Makefile
├── docker-compose.yml
├── pg_sage.control
├── include/
│   └── pg_sage.h                 # Shared header
├── sql/
│   └── pg_sage--0.1.0.sql        # Extension SQL (schema, functions, tables)
├── src/
│   ├── pg_sage.c                 # Entry point, shared memory, worker registration
│   ├── guc.c                     # GUC definitions
│   ├── collector.c               # Snapshot collection background worker
│   ├── analyzer.c                # Rules engine and main analysis loop
│   ├── analyzer_extra.c          # Vacuum/bloat, security, replication analysis
│   ├── action_executor.c         # Tier 3 action execution with trust gating
│   ├── briefing.c                # Briefing generation, diagnose, explain narrative
│   ├── tier2_extra.c             # Cost attribution, migration review, schema design
│   ├── context.c                 # Context assembly for LLM prompts
│   ├── llm.c                     # LLM HTTP integration (libcurl)
│   ├── circuit_breaker.c         # Circuit breaker implementation
│   ├── explain_capture.c         # EXPLAIN plan capture and caching
│   ├── findings.c                # Finding creation and management
│   ├── ha.c                      # High availability awareness
│   ├── self_monitor.c            # Self-monitoring checks
│   └── utils.c                   # SPI utilities, JSON helpers
├── test/
│   ├── regression.sql
│   ├── run_tests.sql
│   └── test_all_features.sql
├── docs/
│   └── pg_sage_spec_v2.2.md      # Full specification
└── docker-entrypoint-initdb.d/
```

---

## Roadmap

- **v0.2** -- PG14/15/16 CI matrix, `auto_explain` integration for passive plan capture
- **v0.5** -- MCP server for IDE/agent integration, sidecar mode for RDS/Aurora/Cloud SQL
- **v1.0** -- Production hardening, pg_upgrade compatibility, PGXN publishing

---

## License

pg_sage is licensed under the [GNU Affero General Public License v3.0](https://www.gnu.org/licenses/agpl-3.0.html).
