# pg_sage Sample Output

Expected output from key pg_sage commands. Use this as a documentation reference or screenshot substitute.

---

## sage.status()

System status as JSONB:

```json
{
    "version": "0.5.0",
    "enabled": true,
    "trust_level": "advisory",
    "collector_running": true,
    "analyzer_running": true,
    "emergency_stopped": false,
    "circuit_state": "closed",
    "llm_circuit_state": "closed",
    "uptime_seconds": 247,
    "snapshots_collected": 8,
    "findings_total": 11,
    "findings_open": 9,
    "actions_executed": 0,
    "last_collector_run": "2026-03-19T14:32:10.123456+00:00",
    "last_analyzer_run": "2026-03-19T14:32:40.654321+00:00"
}
```

---

## sage.findings

Open findings sorted by severity:

```
 category              | severity | title
-----------------------+----------+------------------------------------------------------------------
 duplicate_index       | critical | Duplicate index public.idx_orders_duplicate2 matches idx_orders_duplicate1
 sequence_exhaustion   | critical | Sequence public.test_exhausted_seq at 93.1% capacity (integer)
 config                | warning  | shared_buffers below recommended 25% of RAM
 security_missing_rls  | warning  | Table public.customers has sensitive columns but no RLS
 unused_index          | warning  | Unused index public.idx_orders_unused on public.orders (zero scans)
 schema_design         | warning  | Column public.orders.status uses TEXT — consider an ENUM type
 config                | info     | max_connections (100) significantly exceeds peak usage (5)
 config                | info     | random_page_cost=4.0 may be too high for SSD storage
 missing_index         | info     | Sequential scan on public.orders (100000 rows) — consider index on customer_id
(9 rows)
```

### Finding Detail (single finding)

```
-[ RECORD 1 ]-----+------------------------------------------------------------------
id                 | 1
created_at         | 2026-03-19 14:32:40.123456+00
category           | duplicate_index
severity           | critical
object_identifier  | public.idx_orders_duplicate2
title              | Duplicate index public.idx_orders_duplicate2 matches idx_orders_duplicate1
recommendation     | Drop the duplicate index to save disk space and reduce write overhead
recommended_sql    | DROP INDEX CONCURRENTLY public.idx_orders_duplicate2;
rollback_sql       | CREATE INDEX CONCURRENTLY idx_orders_duplicate2 ON public.orders (customer_id);
status             | open
```

---

## sage.briefing()

Health briefing output (Tier 1 deterministic mode):

```
=== pg_sage Health Briefing ===
Generated: 2026-03-19T14:35:00Z
Trust Level: ADVISORY | Emergency Stop: OFF

--- System Overview ---
PostgreSQL 17.2 | Uptime: 5 minutes
Connections: 5 active / 100 max (5.0%)
Cache Hit Ratio: 99.2%
Database Size: 48 MB

--- Critical Findings (2) ---
[CRITICAL] Duplicate index public.idx_orders_duplicate2 matches idx_orders_duplicate1
  → Recommendation: Drop the duplicate index to save disk space and reduce write overhead
  → SQL: DROP INDEX CONCURRENTLY public.idx_orders_duplicate2;

[CRITICAL] Sequence public.test_exhausted_seq at 93.1% capacity (integer)
  → Recommendation: ALTER SEQUENCE to use bigint before exhaustion
  → SQL: ALTER SEQUENCE public.test_exhausted_seq AS bigint;

--- Warnings (3) ---
[WARNING] shared_buffers below recommended 25% of RAM
[WARNING] Table public.customers has sensitive columns but no RLS
[WARNING] Unused index public.idx_orders_unused on public.orders (zero scans)

--- Info (3) ---
[INFO] max_connections significantly exceeds peak usage
[INFO] random_page_cost may be too high for SSD storage
[INFO] Sequential scan on public.orders — consider index on customer_id

--- Action Executor ---
Trust Level: ADVISORY (Day 0 of 30)
Actions pending: 2 (awaiting Day 8 for SAFE tier actions)
Actions executed: 0

=== End Briefing ===
```

---

## sage.schema_json('public.orders')

Table schema as JSONB:

```json
{
    "table": "public.orders",
    "columns": [
        {"name": "id", "type": "integer", "nullable": false, "default": "nextval('orders_id_seq')"},
        {"name": "customer_id", "type": "integer", "nullable": false, "default": null},
        {"name": "product_id", "type": "integer", "nullable": false, "default": null},
        {"name": "quantity", "type": "integer", "nullable": false, "default": "1"},
        {"name": "total_amount", "type": "numeric(10,2)", "nullable": true, "default": null},
        {"name": "status", "type": "text", "nullable": true, "default": "'pending'"},
        {"name": "created_at", "type": "timestamptz", "nullable": true, "default": "now()"},
        {"name": "updated_at", "type": "timestamptz", "nullable": true, "default": "now()"}
    ],
    "indexes": [
        {"name": "orders_pkey", "definition": "CREATE UNIQUE INDEX orders_pkey ON public.orders USING btree (id)", "is_unique": true, "is_primary": true},
        {"name": "idx_orders_duplicate1", "definition": "CREATE INDEX idx_orders_duplicate1 ON public.orders USING btree (customer_id)", "is_unique": false, "is_primary": false},
        {"name": "idx_orders_duplicate2", "definition": "CREATE INDEX idx_orders_duplicate2 ON public.orders USING btree (customer_id)", "is_unique": false, "is_primary": false},
        {"name": "idx_orders_unused", "definition": "CREATE INDEX idx_orders_unused ON public.orders USING btree (status, created_at, updated_at, quantity)", "is_unique": false, "is_primary": false}
    ],
    "constraints": [
        {"name": "orders_pkey", "type": "PRIMARY KEY", "definition": "PRIMARY KEY (id)"},
        {"name": "fk_orders_customer", "type": "FOREIGN KEY", "definition": "FOREIGN KEY (customer_id) REFERENCES customers(id)"},
        {"name": "fk_orders_product", "type": "FOREIGN KEY", "definition": "FOREIGN KEY (product_id) REFERENCES products(id)"}
    ],
    "row_estimate": 100000,
    "total_size": "26 MB"
}
```

---

## sage.slow_queries_json()

Top slow queries:

```json
{
    "slow_queries": [
        {
            "queryid": 1234567890,
            "query": "SELECT * FROM orders WHERE customer_id = $1 AND status = $2",
            "calls": 42,
            "mean_time_ms": 12.34,
            "total_time_ms": 518.28,
            "rows": 1680,
            "shared_blks_hit": 3200,
            "shared_blks_read": 45
        },
        {
            "queryid": 9876543210,
            "query": "SELECT o.*, c.name FROM orders o JOIN customers c ON c.id = o.customer_id WHERE o.created_at > $1",
            "calls": 15,
            "mean_time_ms": 8.56,
            "total_time_ms": 128.40,
            "rows": 7500,
            "shared_blks_hit": 1800,
            "shared_blks_read": 120
        }
    ],
    "threshold_ms": 1000,
    "captured_at": "2026-03-19T14:35:00Z"
}
```

---

## Emergency Stop / Resume

```sql
postgres=# SELECT sage.emergency_stop();
 emergency_stop
----------------
 t
(1 row)

postgres=# SELECT sage.status()->>'emergency_stopped' AS stopped,
                  sage.status()->>'circuit_state' AS circuit;
 stopped | circuit
---------+---------
 true    | open
(1 row)

postgres=# SELECT sage.resume();
 resume
--------
 t
(1 row)

postgres=# SELECT sage.status()->>'emergency_stopped' AS stopped,
                  sage.status()->>'circuit_state' AS circuit;
 stopped | circuit
---------+---------
 false   | closed
(1 row)
```

---

## Prometheus Metrics

Scraped from `http://localhost:9187/metrics`:

```
# HELP pg_sage_info pg_sage version information
# TYPE pg_sage_info gauge
pg_sage_info{version="0.5.0"} 1

# HELP pg_sage_findings_total Number of open findings by severity
# TYPE pg_sage_findings_total gauge
pg_sage_findings_total{severity="critical"} 2
pg_sage_findings_total{severity="warning"} 3
pg_sage_findings_total{severity="info"} 3

# HELP pg_sage_circuit_breaker_state Circuit breaker state (0=closed, 1=open)
# TYPE pg_sage_circuit_breaker_state gauge
pg_sage_circuit_breaker_state{breaker="db"} 0
pg_sage_circuit_breaker_state{breaker="llm"} 0

# TYPE pg_sage_status_uptime_seconds gauge
pg_sage_status_uptime_seconds 247
# TYPE pg_sage_status_snapshots_collected gauge
pg_sage_status_snapshots_collected 8
# TYPE pg_sage_status_findings_total gauge
pg_sage_status_findings_total 11
# TYPE pg_sage_status_findings_open gauge
pg_sage_status_findings_open 9
# TYPE pg_sage_status_actions_executed gauge
pg_sage_status_actions_executed 0
```

---

## MCP SSE Session

Connecting to the MCP sidecar:

```
$ curl -sN http://localhost:5433/sse

event: endpoint
data: /messages?sessionId=a1b2c3d4-e5f6-7890-abcd-ef1234567890
```

Sending an initialize request:

```bash
$ curl -s -X POST 'http://localhost:5433/messages?sessionId=a1b2c3d4-e5f6-7890-abcd-ef1234567890' \
    -H 'Content-Type: application/json' \
    -d '{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2024-11-05","capabilities":{},"clientInfo":{"name":"demo","version":"1.0"}}}'
```

Response (via SSE stream):

```json
{
    "jsonrpc": "2.0",
    "id": 1,
    "result": {
        "protocolVersion": "2024-11-05",
        "capabilities": {
            "resources": {"listChanged": false},
            "tools": {"listChanged": false},
            "prompts": {"listChanged": false}
        },
        "serverInfo": {
            "name": "pg_sage-sidecar",
            "version": "0.5.0"
        }
    }
}
```

### Available MCP Resources

| URI | Description |
|---|---|
| `sage://health` | System health overview |
| `sage://findings` | Open findings with severity and recommendations |
| `sage://schema/{table}` | Table DDL, columns, indexes, constraints |
| `sage://stats/{table}` | Table size, row counts, vacuum status |
| `sage://slow-queries` | Top slow queries from pg_stat_statements |
| `sage://explain/{queryid}` | Cached EXPLAIN plan for a query |

### Available MCP Tools

| Tool | Description |
|---|---|
| `diagnose` | Interactive diagnostic analysis via ReAct reasoning |
| `briefing` | Generate an on-demand health briefing |
| `suggest_index` | Get index recommendations for a table |
| `review_migration` | Review DDL for production safety |
