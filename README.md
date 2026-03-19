# pg_sage

[![License: AGPL-3.0](https://img.shields.io/badge/License-AGPL--3.0-blue.svg)](https://www.gnu.org/licenses/agpl-3.0)

**Autonomous PostgreSQL DBA Agent** -- a native C extension that continuously monitors, analyzes, and maintains your PostgreSQL 17 database.

pg_sage runs inside PostgreSQL as three background workers and exposes its capabilities through SQL functions in the `sage` schema. It combines a deterministic rules engine with optional LLM-enhanced analysis and a trust-ramped action executor that gradually earns autonomy over time.

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
CREATE EXTENSION pg_sage;
SELECT * FROM sage.status();
```

---

## Architecture

pg_sage implements a three-tier architecture (spec v2.2):

### Tier 1 -- Rules Engine

Deterministic checks that run every analyzer interval:

| Category | What it detects |
|---|---|
| **Index health** | Duplicate indexes, unused indexes, missing indexes, index bloat |
| **Query performance** | Slow queries, query regressions, sequential scans on large tables |
| **Sequences** | Approaching exhaustion (bigint/int overflow) |
| **Maintenance** | Vacuum needs, table bloat, dead tuple accumulation |
| **Configuration** | Audit of `postgresql.conf` against best practices |
| **Security** | Privilege escalation risks, authentication configuration |
| **Replication** | Lag monitoring, slot health |
| **Self-monitoring** | Extension health, circuit breaker status |

### Tier 2 -- LLM-Enhanced Analysis

Optional features that use an external LLM for natural-language intelligence:

- **Daily briefings** -- summarized health reports
- **Interactive diagnose** -- ReAct loop that reasons through problems step by step
- **Explain narrative** -- human-readable query plan analysis
- **Cost attribution** -- map resource consumption to queries and schemas
- **Migration review** -- assess DDL changes before deployment
- **Schema design review** -- identify normalization and type issues

### Tier 3 -- Action Executor

Automated remediation with a graduated trust model:

| Trust Level | Timeline | Allowed Actions |
|---|---|---|
| **OBSERVATION** | Day 0--7 | No actions; findings only |
| **ADVISORY** | Day 8--30 | SAFE actions (drop unused/duplicate indexes, vacuum tuning) |
| **AUTONOMOUS** | Day 31+ | MODERATE actions (create indexes, reindex, configuration changes) |

HIGH-risk actions always require manual confirmation regardless of trust level.

Every autonomous action is logged to `sage.action_log` with full rollback metadata.

---

## SQL Functions

```sql
-- System status as JSONB
SELECT * FROM sage.status();

-- Daily health briefing (Tier 2, requires LLM)
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
| `sage.snapshots` | Point-in-time system state captures |
| `sage.findings` | Detected issues with severity and metadata |
| `sage.action_log` | Audit trail for every autonomous action |
| `sage.explain_cache` | Cached EXPLAIN plans |
| `sage.briefings` | Generated briefing reports |
| `sage.config` | Extension configuration overrides |

---

## Configuration (GUCs)

Set these in `postgresql.conf` or via `ALTER SYSTEM`:

| Parameter | Default | Description |
|---|---|---|
| `sage.enabled` | `on` | Master enable/disable switch |
| `sage.collector_interval` | `30` | Seconds between snapshot collections |
| `sage.analyzer_interval` | `60` | Seconds between analysis runs |
| `sage.trust_level` | `observation` | Current trust tier (`observation`, `advisory`, `autonomous`) |
| `sage.slow_query_threshold` | `1000` | Slow query threshold in milliseconds |
| `sage.seq_scan_min_rows` | `10000` | Minimum rows for sequential scan alerts |
| `sage.rollback_threshold` | `5` | Failed actions before circuit breaker trips |
| `sage.llm_enabled` | `off` | Enable Tier 2 LLM features |

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

- PostgreSQL 17
- `pg_stat_statements` extension
- `libcurl` development headers (for LLM integration)

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

---

## Testing

Three test suites are included:

| Suite | Tests | Purpose |
|---|---|---|
| `test/regression.sql` | 27 | Core functionality and schema validation |
| `test/run_tests.sql` | 14 | Integration tests across tiers |
| `test/test_all_features.sql` | -- | Comprehensive feature coverage |

Run all tests against the Docker container:

```bash
docker exec -i pg_sage-pg_sage-1 psql -U postgres < test/test_all_features.sql
```

Or use the build-and-test script:

```bash
docker exec -i pg_sage-pg_sage-1 bash /build/pg_sage/test/build_and_test.sh
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
│   ├── test_all_features.sql
│   └── build_and_test.sh
└── docker-entrypoint-initdb.d/
```

---

## Background Workers

pg_sage registers three background workers:

1. **Collector** -- captures snapshots of `pg_stat_statements`, `pg_stat_user_tables`, `pg_stat_user_indexes`, sequences, and replication state at a configurable interval.
2. **Analyzer** -- runs the Tier 1 rules engine against collected snapshots, generates findings, and (at sufficient trust level) invokes the Tier 3 action executor.
3. **Briefing** -- generates periodic Tier 2 health briefings when LLM integration is enabled.

---

## License

pg_sage is licensed under the [GNU Affero General Public License v3.0](https://www.gnu.org/licenses/agpl-3.0.html).
