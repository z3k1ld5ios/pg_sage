# SQL Reference

All pg_sage SQL functions live in the `sage` schema. They are created automatically when the extension is installed via `CREATE EXTENSION pg_sage`.

---

## Control Functions

### `sage.status()`

Returns the current system status as a composite type with JSONB fields.

**Returns:** `SETOF record`

```sql
SELECT * FROM sage.status();
```

```json
{
  "version": "0.5.0",
  "enabled": true,
  "circuit_state": "closed",
  "llm_circuit_state": "closed",
  "emergency_stopped": false,
  "workers": {
    "collector": true,
    "analyzer": true,
    "briefing": true
  }
}
```

---

### `sage.briefing()`

Generates a health briefing summarizing current findings, performance metrics, and recommendations. Works with or without an LLM endpoint configured.

**Returns:** `SETOF record`

```sql
SELECT * FROM sage.briefing();
```

Without LLM, produces a structured text briefing from Tier 1 data. With LLM enabled, produces a natural-language summary.

---

### `sage.diagnose(question TEXT)`

Interactive diagnostic using a ReAct reasoning loop. The LLM reasons through problems step by step, executing follow-up SQL queries autonomously (up to `sage.react_max_steps` iterations).

**Returns:** `SETOF record`

**Requires:** LLM endpoint configured

```sql
SELECT * FROM sage.diagnose('Why are my queries slow today?');
```

```sql
SELECT * FROM sage.diagnose('Is my replication healthy?');
```

!!! note
    `sage.diagnose()` requires `sage.llm_enabled = on` and a valid LLM endpoint. Without these, it returns an error message.

---

### `sage.explain(queryid BIGINT)`

Captures an EXPLAIN plan for the given query and returns a human-readable narrative analysis. Runs `EXPLAIN (FORMAT JSON, COSTS, VERBOSE)` against the query text from `pg_stat_statements` and caches the result in `sage.explain_cache`.

**Returns:** `SETOF record`

```sql
-- Get the queryid from pg_stat_statements
SELECT queryid, query FROM pg_stat_statements ORDER BY total_exec_time DESC LIMIT 5;

-- Explain a specific query
SELECT * FROM sage.explain(1234567890);
```

With LLM enabled, includes a narrative explanation of the plan. Without LLM, returns the raw JSON plan.

---

### `sage.suppress(finding_id INTEGER, reason TEXT, duration_days INTEGER)`

Suppresses a specific finding for the given duration. Suppressed findings are hidden from `sage.findings` queries filtering on `status = 'open'`.

**Returns:** `void`

```sql
-- Suppress finding #42 for 30 days
SELECT sage.suppress(42, 'Known issue, vendor fix pending', 30);
```

```sql
-- Suppress finding #7 for 90 days
SELECT sage.suppress(7, 'Accepted risk per security review', 90);
```

!!! tip
    Use `SELECT id, title FROM sage.findings WHERE status = 'open';` to find the finding ID to suppress.

---

### `sage.emergency_stop()`

Immediately halts all autonomous activity by tripping both circuit breakers and setting the emergency stop flag in shared memory.

**Returns:** `void`

```sql
SELECT sage.emergency_stop();
```

After calling this:

- All background worker activity pauses
- No new analysis runs or actions execute
- The circuit breaker state becomes `open`

!!! warning
    Use this if pg_sage is causing unexpected behavior. It takes effect immediately without waiting for the current cycle to complete.

---

### `sage.resume()`

Resets both circuit breakers and clears the emergency stop flag, resuming normal operation.

**Returns:** `void`

```sql
SELECT sage.resume();
```

---

## JSON Functions (MCP)

These functions return JSONB and are primarily used by the MCP sidecar, but can be called directly from SQL.

### `sage.health_json()`

Returns a JSONB object with system health overview combining shared-memory state with live database health metrics.

**Returns:** `JSONB`

```sql
SELECT sage.health_json();
```

Includes: version, enabled state, circuit breaker states, worker status, connection counts, cache hit ratio, replication lag, and database size.

---

### `sage.findings_json(status_filter TEXT DEFAULT 'open')`

Returns a JSONB array of findings filtered by status.

**Returns:** `JSONB`

```sql
-- Open findings (default)
SELECT sage.findings_json();

-- Resolved findings
SELECT sage.findings_json('resolved');

-- Suppressed findings
SELECT sage.findings_json('suppressed');
```

Each finding object includes: `id`, `created_at`, `last_seen`, `occurrence_count`, `category`, `severity`, `object_type`, `object_identifier`, `title`, `detail`, `recommendation`, `recommended_sql`, `rollback_sql`, `status`, `suppressed_until`, `resolved_at`, `acted_on_at`.

---

### `sage.schema_json(table_name TEXT)`

Returns DDL, indexes, constraints, columns, and foreign keys for a specific table.

**Returns:** `JSONB`

```sql
SELECT sage.schema_json('public.orders');
```

Accepts both schema-qualified (`public.orders`) and unqualified (`orders`) table names.

!!! note
    Returns an error if `table_name` is NULL.

---

### `sage.stats_json(table_name TEXT)`

Returns table size, row counts, dead tuples, vacuum status, and index usage statistics.

**Returns:** `JSONB`

```sql
SELECT sage.stats_json('public.orders');
```

Includes: `total_size`, `total_bytes`, `table_size`, `indexes_size`, `live_tuples`, `dead_tuples`, `seq_scan`, `idx_scan`, `last_vacuum`, `last_autovacuum`, `dead_tuple_ratio_pct`, and more.

---

### `sage.slow_queries_json()`

Returns the top 20 slow queries from `pg_stat_statements`, ordered by total execution time.

**Returns:** `JSONB`

```sql
SELECT sage.slow_queries_json();
```

Returns an empty array if `pg_stat_statements` is not installed. Each query object includes: `queryid`, `query` (truncated to 500 characters), `calls`, `total_exec_time_ms`, `mean_exec_time_ms`, `max_exec_time_ms`, `min_exec_time_ms`, `stddev_exec_time_ms`, `rows`.

---

### `sage.explain_json(queryid BIGINT)`

Returns the most recent cached EXPLAIN plan for the given query ID from `sage.explain_cache`.

**Returns:** `JSONB` (or NULL if no cached plan exists)

```sql
SELECT sage.explain_json(1234567890);
```

Includes: `captured_at`, `queryid`, `query_text`, `plan` (raw JSON plan), `source`, `total_cost`, `execution_time`.

!!! tip
    Use `sage.explain(queryid)` first to capture a plan, then `sage.explain_json(queryid)` to retrieve it programmatically.
