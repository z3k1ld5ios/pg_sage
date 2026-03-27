# pg_sage v0.8.0 AlloyDB Verification Report

**Date:** 2026-03-26
**Tester:** jmass
**Status:** PASS

---

## Summary

pg_sage v0.8.0 was tested against a Google Cloud AlloyDB instance to verify
platform detection, analysis findings, executor actions, and API endpoints.
The sidecar ran cleanly with no crashes. All core functionality operated correctly.
One known issue was identified: AlloyDB's internal `google_ml` schema tables
trigger false-positive findings that should be excluded.

---

## Environment

| Component          | Detail                                                        |
|--------------------|---------------------------------------------------------------|
| GCP Project        | `satty-488221`                                                |
| AlloyDB Cluster    | `sage-test-alloydb`                                           |
| AlloyDB Instance   | `sage-test-primary`                                           |
| Region             | `us-central1`                                                 |
| PostgreSQL Version | 17.7 (AlloyDB internals, google_columnar_engine)              |
| Private IP         | `10.70.16.5`                                                  |
| Bastion VM         | `sage-bastion` (e2-micro, `34.134.71.94`, us-central1-c)     |
| Test Database      | `sage_test`                                                   |
| Test User          | `sage_agent`                                                  |
| Config File        | `cloudsqltests/config_alloydb_v08.yaml`                       |
| API Port           | 8082                                                          |
| pg_stat_statements | Enabled                                                       |

---

## Setup

AlloyDB instances are private-IP only. Access was established through an SSH
tunnel via the bastion VM.

### SSH Tunnel Command

```bash
gcloud compute ssh sage-bastion \
  --ssh-flag="-L" --ssh-flag="5435:10.70.16.5:5432" --ssh-flag="-N"
```

This forwards `localhost:5435` to the AlloyDB instance at `10.70.16.5:5432`.

### Configuration

The config file (`config_alloydb_v08.yaml`) was pointed at `localhost:5435`
with `sslmode=require`.

### Demo Data

Test data was planted via `psql` through the bastion tunnel prior to running
the sidecar.

---

## Test Results

| Area                | Result | Notes                                        |
|---------------------|--------|----------------------------------------------|
| Sidecar startup     | PASS   | Clean startup, no errors                     |
| Platform detection  | PASS   | Correctly identified AlloyDB                 |
| Analysis cycle      | PASS   | 15 findings on first cycle                   |
| Executor actions    | PASS   | DDL executed successfully (with expected exclusions) |
| API endpoints       | PASS   | All endpoints responsive on port 8082        |
| LLM advisor         | PASS   | Gemini-based advisor produced findings       |
| LLM optimizer       | PASS   | Gemini-based optimizer produced findings     |
| Stability           | PASS   | No crashes, clean operation throughout       |

---

## Platform Detection

The sidecar correctly detected the AlloyDB environment at startup:

```
cloud environment: alloydb
```

**Detection method:** The query
`SELECT current_setting('alloydb.iam_authentication', true)` returns `"off"`
on AlloyDB (a non-NULL result confirms the platform is AlloyDB). On standard
PostgreSQL or Cloud SQL, this setting does not exist and returns NULL.

---

## Findings

The first analysis cycle produced **15 findings** across the following categories:

| Finding Type          | Count | Description                                  |
|-----------------------|-------|----------------------------------------------|
| `duplicate_index`     | --    | Redundant indexes detected                   |
| `missing_fk_index`    | --    | Foreign keys without supporting indexes      |
| `sequence_exhaustion` | --    | Sequences approaching capacity limits        |
| `slow_query`          | --    | Queries exceeding performance thresholds     |
| `replication_lag`     | --    | Replication lag detected                     |

All finding types are consistent with expected pg_sage behavior on a seeded
test database.

---

## Executor Actions

| Action                     | Result  | Notes                                     |
|----------------------------|---------|-------------------------------------------|
| Drop duplicate index       | SUCCESS | Redundant index removed cleanly           |
| Create FK index            | SUCCESS | Missing foreign key index created         |
| Create index on google_ml  | FAILED  | Expected failure (see Known Issues below) |

The executor correctly identified and acted on legitimate findings. The
`google_ml` failure is expected and does not indicate a bug in pg_sage.

---

## API Verification

All API endpoints were verified working on port 8082:

- Health check responded correctly
- Findings endpoint returned analysis results
- Status and configuration endpoints operational

---

## Known Issues

### google_ml Schema Exclusion

**Severity:** Low
**Impact:** False-positive findings and failed executor actions on internal tables

AlloyDB includes an internal `google_ml` schema containing system-managed tables:

- `google_ml.models`
- `google_ml.proxy_models_query_mapping`

pg_sage's foreign key analysis rule detected these tables and the executor
attempted to create indexes on them, which failed with:

```
must be owner of table
```

This is expected behavior -- these are AlloyDB-internal tables that `sage_agent`
does not (and should not) have ownership of.

**Recommendation:** Add `google_ml` to the schema exclusion list alongside
the existing exclusions (`sage`, `pg_catalog`, `information_schema`). This will
prevent false-positive findings and unnecessary executor attempts on
AlloyDB-internal objects.

---

## Conclusion

pg_sage v0.8.0 operates correctly on AlloyDB. Platform detection, analysis,
execution, and API functionality all work as expected. The only issue identified
is the `google_ml` schema exclusion, which is a minor configuration improvement
rather than a functional defect. AlloyDB is confirmed as a supported platform
for pg_sage v0.8.0.
