# Findings Catalog

pg_sage detects issues across 15 categories. Each finding includes a severity level, human-readable title, detailed description, recommendation, and (where applicable) remediation SQL with rollback SQL.

Severities: **critical** > **warning** > **info**

---

## unused_index

Detects indexes with zero scans over the configured window.

**Severity:** warning

**What it detects:** Indexes that consume disk space and slow down writes but are never used for reads. Evaluated against `pg_stat_user_indexes.idx_scan` over the `sage.unused_index_window` period (default 30 days).

**Example output:**

```
Unused index public.idx_old on public.orders (zero scans)
```

**Recommended action:** Drop the index.

```sql
DROP INDEX CONCURRENTLY public.idx_old;
```

!!! tip
    Before dropping, verify the index is not used by another database or application that connects infrequently. Check `idx_scan` across all databases.

---

## duplicate_index

Detects indexes with identical column sets on the same table.

**Severity:** critical

**What it detects:** Two or more indexes covering the same columns in the same order on the same table. Wastes disk space and write throughput.

**Example output:**

```
Duplicate index public.idx_orders_dup2 matches idx_orders_dup1
```

**Recommended action:** Drop the duplicate, keeping the one referenced by constraints or with the most specific definition.

```sql
DROP INDEX CONCURRENTLY public.idx_orders_dup2;
```

---

## missing_index

Detects tables with sequential scan patterns that would benefit from indexing.

**Severity:** warning

**What it detects:** Tables where `seq_scan` is high relative to `idx_scan`, the table exceeds `sage.seq_scan_min_rows`, and query patterns from `pg_stat_statements` suggest filterable columns.

**Example output:**

```
Table public.orders may benefit from an index on (customer_id)
```

**Recommended action:** Create the suggested index.

```sql
CREATE INDEX CONCURRENTLY idx_orders_customer_id ON public.orders (customer_id);
```

---

## index_bloat

Detects indexes with excessive bloat beyond the configured threshold.

**Severity:** warning

**What it detects:** Indexes where estimated bloat percentage exceeds `sage.index_bloat_threshold` (default 30%). Uses statistical estimation from `pg_class` and `pg_statistic`.

**Example output:**

```
Index public.orders_pkey is 45% bloated (estimated 128 MB wasted)
```

**Recommended action:** REINDEX to reclaim space.

```sql
REINDEX INDEX CONCURRENTLY public.orders_pkey;
```

---

## slow_query

Detects queries with mean execution time above the threshold.

**Severity:** warning (critical if > 10x threshold)

**What it detects:** Queries from `pg_stat_statements` where `mean_exec_time` exceeds `sage.slow_query_threshold` (default 1000ms).

**Example output:**

```
Slow query (mean 3500ms, 15000 calls): SELECT * FROM orders WHERE ...
```

**Recommended action:** Examine the query plan via `sage.explain(queryid)`, add indexes, or rewrite the query.

---

## query_regression

Detects queries whose performance has degraded compared to previous snapshots.

**Severity:** warning

**What it detects:** Queries where `mean_exec_time` has increased significantly between consecutive snapshots, indicating a performance regression (plan change, data growth, lock contention).

**Example output:**

```
Query regression: mean_exec_time increased 340% (120ms -> 528ms) for queryid 1234567890
```

**Recommended action:** Investigate recent changes -- schema modifications, data volume changes, or configuration updates. Use `sage.explain(queryid)` to compare plans.

---

## seq_scan

Detects sequential scans on large tables.

**Severity:** info (warning if frequent)

**What it detects:** Tables exceeding `sage.seq_scan_min_rows` (default 100,000) that are accessed primarily via sequential scans rather than index scans.

**Example output:**

```
Sequential scan on public.events (2.5M rows, 95% seq_scan ratio)
```

**Recommended action:** Analyze query patterns and add appropriate indexes.

---

## sequence_exhaustion

Detects sequences approaching their maximum value.

**Severity:** critical

**What it detects:** Sequences where the current value is a high percentage of the maximum for their data type. Integer sequences (max ~2.1 billion) are flagged earlier than bigint sequences.

**Example output:**

```
Sequence public.orders_seq at 93.1% capacity (integer)
```

**Recommended action:** Alter the sequence to use `bigint`, or reset with a higher ceiling.

```sql
ALTER SEQUENCE public.orders_seq AS bigint;
```

!!! warning
    Sequence exhaustion causes `INSERT` failures. Address critical sequence findings immediately.

---

## config

Detects suboptimal PostgreSQL configuration settings.

**Severity:** warning or info

**What it detects:** Audits `postgresql.conf` settings against best practices:

- `shared_buffers` below 25% of RAM
- `work_mem` too low or too high
- `effective_cache_size` misconfigured
- `max_connections` significantly exceeding peak usage
- `random_page_cost` set for spinning disks when on SSD
- Checkpoint and WAL settings

**Example output:**

```
shared_buffers below recommended 25% of RAM
max_connections (200) significantly exceeds peak usage (12)
```

**Recommended action:** Adjust the flagged setting in `postgresql.conf` or via `ALTER SYSTEM`.

---

## vacuum_bloat

Detects tables with excessive dead tuples or stale vacuum state.

**Severity:** warning (critical for XID wraparound risk)

**What it detects:**

- High dead tuple ratio
- Tables not vacuumed within expected intervals
- XID age approaching wraparound threshold
- Autovacuum workers consistently maxed out

**Example output:**

```
Table public.events has 15% dead tuples (450,000 dead / 3,000,000 live)
XID age for public.orders approaching wraparound (1.8 billion)
```

**Recommended action:** Run manual VACUUM or tune autovacuum settings.

```sql
VACUUM (VERBOSE) public.events;
```

---

## security_missing_rls

Detects tables with sensitive columns that lack Row-Level Security policies.

**Severity:** warning

**What it detects:** Tables containing columns with names suggesting sensitive data (email, password, ssn, credit_card, token, secret, etc.) that do not have RLS enabled.

**Example output:**

```
Table public.customers has sensitive columns but no RLS
```

**Recommended action:** Enable RLS and create appropriate policies.

```sql
ALTER TABLE public.customers ENABLE ROW LEVEL SECURITY;
CREATE POLICY customer_access ON public.customers
  USING (tenant_id = current_setting('app.tenant_id')::int);
```

---

## replication_health

Detects replication issues on primary and standby servers.

**Severity:** warning or critical

**What it detects:**

- Replication lag exceeding thresholds
- Inactive replication slots (consuming WAL without a consumer)
- WAL archiving staleness
- Standby apply lag

**Example output:**

```
Replication slot 'old_subscriber' is inactive (last active 3 days ago)
Replication lag: 45 seconds behind primary
```

**Recommended action:** Drop inactive replication slots, investigate network issues, or scale up standby resources.

---

## cost_attribution

Maps storage and IOPS costs to database objects. Requires `sage.cloud_provider` and `sage.instance_type` to be configured.

**Severity:** info

**What it detects:** Estimates monthly cost of unused indexes, oversized tables, and wasted storage based on cloud provider pricing.

**Example output:**

```
Unused indexes on public.orders cost approximately $12.50/month in storage
```

**Recommended action:** Drop unused indexes or archive cold data.

!!! note
    This is a Tier 2 (LLM-enhanced) feature. Requires LLM endpoint and cloud provider configuration.

---

## migration_review

Reviews DDL changes for production safety.

**Severity:** warning or critical

**What it detects:**

- `ALTER TABLE` operations that acquire `ACCESS EXCLUSIVE` locks
- Adding columns with volatile defaults
- Dropping columns without checking dependencies
- Long-running DDL on large tables

**Example output:**

```
ALTER TABLE public.orders ADD COLUMN status text DEFAULT 'pending' acquires ACCESS EXCLUSIVE lock
```

**Recommended action:** Use safer alternatives (e.g., add column as NULL first, then backfill).

!!! note
    This is a Tier 2 (LLM-enhanced) feature.

---

## schema_design

Reviews table schema for design issues.

**Severity:** info or warning

**What it detects:**

- Timestamp columns without timezone (`timestamp` instead of `timestamptz`)
- Tables without primary keys
- Naming convention inconsistencies
- Overly wide rows
- Missing NOT NULL constraints on required columns

**Example output:**

```
Column public.events.created_at uses timestamp without time zone
Table public.audit_log has no primary key
```

**Recommended action:** Address the specific design issue flagged.

!!! note
    This is a Tier 2 (LLM-enhanced) feature.
