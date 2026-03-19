# pg_sage

**Autonomous PostgreSQL DBA Agent** -- a native C extension that continuously monitors, analyzes, and maintains your PostgreSQL database.

pg_sage runs inside PostgreSQL as three background workers and exposes its capabilities through SQL functions in the `sage` schema. It combines a deterministic rules engine with optional LLM-enhanced analysis and a trust-ramped action executor that gradually earns autonomy over time.

All Tier 1 analysis runs without any external dependencies. LLM integration is optional and only enhances Tier 2 features (briefings, diagnose, explain narrative).

---

## Key Features

| Feature | Description |
|---|---|
| **Runs inside Postgres** | Native C extension, zero external infrastructure |
| **Three-tier architecture** | Rules engine, LLM analysis, automated actions |
| **Trust-ramped autonomy** | Graduated from observation to autonomous over time |
| **LLM optional** | Tier 1 works without any LLM endpoint configured |
| **MCP sidecar** | Exposes capabilities to AI assistants via Model Context Protocol |
| **Self-protecting** | Circuit breaker prevents pg_sage from becoming the incident |
| **Full audit trail** | Every action logged with before/after state and rollback SQL |

---

## What It Detects

**Tier 1 -- Rules Engine** (no LLM required):

- **Index health**: duplicate indexes, unused indexes, missing indexes, index bloat
- **Query performance**: slow queries, query regressions, sequential scans on large tables
- **Sequences**: approaching exhaustion (bigint/int overflow)
- **Maintenance**: vacuum needs, table bloat, dead tuple accumulation, XID wraparound
- **Configuration**: audit of `postgresql.conf` against best practices
- **Security**: overprivileged roles, missing RLS on sensitive tables
- **Replication**: lag monitoring, inactive slots, WAL archiving staleness
- **Self-monitoring**: extension health, circuit breaker status, schema footprint

**Tier 2 -- LLM-Enhanced Analysis** (optional):

- Daily briefings with natural-language summaries
- Interactive diagnostic via ReAct reasoning loop
- Human-readable query plan narratives
- Cost attribution for unused/missing indexes
- Migration review for long-running DDL
- Schema design review

---

## Quick Start

```bash
git clone https://github.com/jasonmassie01/pg_sage.git
cd pg_sage
docker compose up
```

Connect to the running container:

```bash
docker exec -it pg_sage-pg_sage-1 psql -U postgres
```

Run your first queries:

```sql
-- Extension is auto-loaded via shared_preload_libraries
-- Check system status
SELECT * FROM sage.status();

-- See what pg_sage found
SELECT category, severity, title
FROM sage.findings
WHERE status = 'open'
ORDER BY severity;

-- Get a health briefing
SELECT sage.briefing();
```

Example output after approximately 60 seconds:

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

!!! tip "What next?"
    See the [Installation](installation.md) guide for detailed setup options, or jump straight to the [Configuration](configuration.md) reference.

---

## Comparison

| | pg_sage | pganalyze | OtterTune / DBtune |
|---|---|---|---|
| **Runs inside Postgres** | Native C extension, zero external infra | SaaS agent + cloud dashboard | Cloud-only SaaS |
| **Takes action** | Trust-ramped autonomous remediation | Recommendations only | Knob tuning only |
| **Self-hosted** | Fully, AGPL-3.0 | Proprietary | Proprietary |
| **LLM dependency** | Optional (Tier 1 works without it) | N/A | Required |

---

## License

pg_sage is licensed under the [GNU Affero General Public License v3.0](https://www.gnu.org/licenses/agpl-3.0.html).
