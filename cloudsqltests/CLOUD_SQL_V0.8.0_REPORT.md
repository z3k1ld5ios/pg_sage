# pg_sage v0.8.0 Cloud SQL Verification Report

**Date:** 2026-03-26
**GCP Project:** satty-488221
**Region:** us-central1

---

## Summary

pg_sage v0.8.0 was verified against two Google Cloud SQL instances running PostgreSQL 16
and PostgreSQL 17. The sidecar successfully collected metrics, generated findings, executed
autonomous remediation actions, and exposed all API and observability endpoints without
errors or crashes on both instances.

---

## Environment

| Resource           | PG16 Instance       | PG17 Instance       |
|--------------------|----------------------|----------------------|
| Instance name      | sage-test-pg16       | sage-test-pg17       |
| PostgreSQL version | 16                   | 17                   |
| Public IP          | 34.56.79.1           | 34.30.70.45          |
| Tier               | db-f1-micro          | db-f1-micro          |
| Edition            | ENTERPRISE           | ENTERPRISE           |
| Region             | us-central1          | us-central1          |

- **Test database:** sage_test
- **Test user:** sage_agent (granted pg_monitor + pg_read_all_stats)
- **Extension:** pg_stat_statements enabled on both instances

---

## Setup

### Sidecar Build

The sidecar binary was built from the `v0.8.0` tag and run against each instance
sequentially.

### Configuration

- Collector interval: 30s
- Analyzer interval: 60s
- Trust level: autonomous
- Ramp start: 2025-01-01
- LLM: Gemini gemini-2.5-flash (enabled)

### Demo Data

A TPC-H-like schema was planted on both instances with the following tables:

- `orders`
- `lineitem`
- `customer`
- `nation`

Planted problems for detection testing:

| Problem Type          | Description                                      |
|-----------------------|--------------------------------------------------|
| Duplicate indexes     | Redundant indexes on the same columns            |
| Missing FK indexes    | Foreign key columns without supporting indexes   |
| Sequence exhaustion   | Sequences approaching max value                  |
| Table bloat           | Tables with excessive dead tuples                |
| Slow queries          | Intentionally inefficient queries via pg_stat_statements |

---

## Test Results

### Version Detection

| Instance         | Expected | Detected | Status |
|------------------|----------|----------|--------|
| sage-test-pg16   | 16       | 16       | PASS   |
| sage-test-pg17   | 17       | 17       | PASS   |

### PG17 Compatibility

PG17 correctly used `pg_stat_checkpointer` instead of the deprecated
`pg_stat_bgwriter` checkpoint fields. No fallback errors were observed.

### Stability

The sidecar ran continuously against both instances with no errors, panics, or crashes
during the full collection and analysis cycle.

---

## Findings

All five planted problem categories were detected on both instances:

| Finding Type          | PG16 | PG17 | Status |
|-----------------------|------|------|--------|
| duplicate_index       | Yes  | Yes  | PASS   |
| missing_fk_index      | Yes  | Yes  | PASS   |
| sequence_exhaustion   | Yes  | Yes  | PASS   |
| slow_query            | Yes  | Yes  | PASS   |
| table_bloat           | Yes  | Yes  | PASS   |

---

## Executor Actions

With trust level set to autonomous and ramp_start well in the past, the executor
performed remediation actions on both instances:

| Action                          | PG16 | PG17 | Status |
|---------------------------------|------|------|--------|
| Drop duplicate indexes          | Yes  | Yes  | PASS   |
| Create missing FK indexes       | Yes  | Yes  | PASS   |

All DDL statements executed successfully and were recorded in the action log.

---

## API Verification

| Endpoint               | Status |
|------------------------|--------|
| REST API               | PASS   |
| MCP server             | PASS   |
| Prometheus /metrics    | PASS   |

All API endpoints returned expected responses. The MCP server accepted tool invocations
correctly. Prometheus metrics were scraped and confirmed operational.

---

## Known Issues

None identified during this test run. Both PostgreSQL 16 and 17 on Cloud SQL ENTERPRISE
edition (db-f1-micro) behaved as expected with the v0.8.0 sidecar.

---

## Conclusion

pg_sage v0.8.0 is verified for production use on Google Cloud SQL with PostgreSQL 16
and PostgreSQL 17. All core subsystems -- collector, analyzer, executor, API, MCP server,
and Prometheus metrics -- functioned correctly. The PG17 compatibility changes
(pg_stat_checkpointer) work as intended. No issues were found.
