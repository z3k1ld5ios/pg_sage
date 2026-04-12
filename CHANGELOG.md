# Changelog

## v0.8.5 (2026-04-11) — Hint Lifecycle, Stale-Stats, Role Memory, Tooltips

### F1 — Hint Revalidation Loop (CHECK-T01..T08)
- New `internal/tuner/revalidate.go` background cycle gated by `tuner.verify_after_apply`. Runs every `tuner.revalidation_interval_hours` (default 24h).
- Four checks per active hint:
  - **Check 1** — retire hints whose underlying query has fallen below `tuner.min_query_calls` or been pruned from `pg_stat_statements`.
  - **Check 2** — retire hints older than `tuner.hint_retirement_days` (default 14) unconditionally.
  - **Check 3** — cost-compare hinted vs unhinted plan. Keep if `hinted_cost <= keep_ratio * unhinted_cost` (1.2), mark broken and rollback when `hinted_cost > unhinted_cost / rollback_ratio` (0.8).
  - **Check 4** — detect plan directives that no longer affect the generated plan (redundant hints).
- New `hint_parse.go` directive parser supports `Set(work_mem "NMB")`, `IndexScan`, `BitmapScan`, `NestLoop`, `HashJoin`, `MergeJoin`, `Parallel`.
- EXPLAIN queries carry a dedicated `statement_timeout` (`tuner.revalidation_explain_timeout_ms`, default 10s) so revalidation cannot starve the collector.
- Scheduler wired from `main.go` — loop is inert when `tuner.verify_after_apply=false` (v0.8.4 behavior).

### F2 — Stale-Stats Detection + ANALYZE Action (CHECK-S01..S14)
- New tuner symptom `stale_stats`: triggers on row-estimate skew (`ActualRows/PlanRows > stale_stats_estimate_skew`, default 10x) corroborated by `pg_stat_user_tables.n_mod_since_analyze / reltuples > stale_stats_mod_ratio` (default 0.1) AND `last_analyze` older than `stale_stats_age_minutes` (default 60).
- New executor action `ANALYZE <schema>.<table>` with:
  - Per-table cooldown (`analyze_cooldown_minutes`, default 60) — respects autovacuum last_analyze as a recent-run signal.
  - Size ceiling (`analyze_max_table_mb`, default 10 GB) — larger tables emit advisory findings instead of executing.
  - Maintenance-window gating (`analyze_maintenance_threshold_mb`, default 1 GB) — tables above this only ANALYZE inside the configured `trust.maintenance_window`.
  - Statement timeout (`analyze_timeout_ms`, default 10 min); exceeding it marks the action `failed_timeout` and extends the cooldown.
  - Global concurrency cap (`max_concurrent_analyze`, default 1) — shared semaphore across the whole process, fleet-wide.

### F3 — Role-Level work_mem Promotion Advisor (CHECK-W01..W11)
- New analyzer rule `rules_workmem_promotion.go`: groups active `Set(work_mem)` hints in `hint_plan.hints` by the role that owns the query (via `pg_stat_statements.userid` → `pg_roles.rolname`).
- When a single role accumulates `analyzer.work_mem_promotion_threshold` (default 5) distinct active hints, emits a `work_mem_role_promotion` finding with `ALTER ROLE ... SET work_mem = '<max_hinted>'` SQL.
- NOLOGIN / SUPERUSER / reserved roles are excluded. Hint scan scopes to `pg_stat_statements.dbid` to avoid cross-database pollution in shared-cluster deployments.

### F4 — Extension Drift Detector
- Tightened drift detection for `pg_stat_statements`, `pg_hint_plan`, `hypopg`, `auto_explain`. Missing critical columns (`plan_time`, `wal_records`) now emit explicit remediation hints instead of generic warnings.

### F5 — Config Tooltip Infrastructure (CHECK-T01..T20)
- New `doc`, `warning`, `mode`, `docs_url`, `secret` struct tags on every Tier 1 and most Tier 2 config fields. Fields matching the sensitive-name suffix pattern (`password`, `api_key`, `secret`, `token`, `encryption_key`, `client_secret`, `tls_cert`, `tls_key`) must carry `secret:"true"` — verified by a reflection drift test that rejects leaked defaults at build time.
- New build tool `cmd/gen_config_meta`: reflects over `config.DefaultConfig()` and emits byte-stable JSON to `sidecar/web/src/generated/config_meta.json` (167 fields). Handles nested structs, slice-of-struct (`parent[].child`), slice-of-scalar, and opaque maps. Secret fields emit `"default": null`. `go run ./cmd/gen_config_meta/` is idempotent.
- New drift test `internal/config/meta_drift_test.go` — fails the build if Tier 1 doc strings in `config_meta.json` drift from live struct tags, and warns (non-fatal, progressive tightening) on missing Tier 2 docs.
- New React component `web/src/components/ConfigTooltip.jsx` using `@radix-ui/react-tooltip` — portal-mounted, keyboard-accessible (WCAG-compliant), graceful degradation when a config key has no metadata. Wired into `SettingsPage` field labels so every documented key shows a dotted-underline with hover/focus tooltip exposing: doc body, warning callout (yellow), mode badge (fleet-only / standalone-only), `secret` indicator, and optional `Read more →` link via `VITE_PG_SAGE_DOCS_BASE`.
- Tooltip trigger carries `data-config-key` for future Playwright e2e coverage.

### Documentation
- `config.example.yaml` bumped to v0.8.5 with every new tuner and analyzer field documented inline.
- Tier 1 config fields (trust.*, safety timeouts, rollback thresholds, tuner.verify_after_apply) all carry 20–200 char `doc` tags — enforced by `TestTier1DocLengthBounds`.

### Coverage
- `internal/config`: 75.4% (target 75%)
- `internal/analyzer`: 89.4% (target 70%)
- `internal/tuner`: 85.5% (target 70%)
- `cmd/gen_config_meta`: 86.3% (target 80%)

## v0.8.4 (2026-04-07) — Security Hardening + Tuner Pipeline

### Security
- **RBAC**: Added `RequireRole` to legacy API routes (config, emergency-stop, resume, suppress/unsuppress, pending count). Viewers can no longer escalate trust to autonomous or halt the fleet.
- **SQL injection**: Block EXPLAIN injection via multi-statement rejection + `statement_timeout` + read-only transactions. New `sanitize.QuoteIdentifier` package for all catalog-derived identifiers. Fixed VACUUM, ALTER DATABASE, and tuner SQL construction.
- **Executor allowlists**: Whitelisted 30 safe `ALTER SYSTEM` GUCs. Restricted SELECT to `pg_terminate_backend`/`pg_cancel_backend`. Restricted `ALTER TABLE` to `SET`/`RESET`/`TABLESPACE`. Validate trust level strings. Cap concurrent DDL at 3. Auto-drop invalid indexes after failed `CREATE INDEX CONCURRENTLY`. Derive advisor `ActionRisk` from SQL type instead of hardcoding `safe`.
- **Auth**: Cookie `Secure` flag enabled. Password minimum 8 characters. Login rate limiting (5 attempts / 15 min per email). Self-deletion + last-admin protection.
- **API hardening**: Security headers middleware. Content-Type validation. SSRF protection on `test-connection`. Error messages sanitized (no more `err.Error()` leaked to clients). `X-Forwarded-For` only trusted from loopback. Notification channel secrets masked.
- **Secrets**: Removed hardcoded Gemini API key from `docker-compose.test.yml`. Admin password printed to stderr only. `create_admin` uses environment variables. `DeriveKey` upgraded to argon2id with v1 backward compatibility. Demo config uses env var.

### Tuner Pipeline
- `BuildInsertSQL`/`BuildDeleteSQL` now use `norm_query_string` column (was `query_id`, which doesn't exist in `hint_plan.hints`).
- Executor allowlist updated for `INSERT`/`DELETE INTO hint_plan.hints`.

### Fleet Hardening
- Schema bootstrap advisory lock changed from non-blocking `pg_try_advisory_lock` to blocking `pg_advisory_lock` with 30s timeout.
- `CREATE SCHEMA` now uses `IF NOT EXISTS`. All `DROP SCHEMA` operations in tests wrapped in advisory lock.
- `autoexplain.ConfigureSession` tolerates permission denied (SQLSTATE 42501) on managed DBs where role-level defaults suffice. Coverage boost to 83%.

### API
- New LLM token usage endpoint.
- Config apply/audit handlers.
- Settings page UI improvements.
- New `create_admin` CLI command.

### Test Infrastructure
- `auth`, `notify`, and `store` tests now hold `pg_sage` advisory lock for test duration to prevent schema drops mid-test.
- Store config tests re-insert FK user before each test.
- All 22 packages pass with `-p 4` (zero failures, zero skips).

## v0.8.3 (2026-04-04) — Cloud E2E + LLM Token Optimization
- Cloud E2E validation across 8 managed PostgreSQL databases:
  RDS PG14/18, Aurora PG14/17, Cloud SQL PG14/18, AlloyDB PG14/17
- Auto-detect cloud environment (rds, aurora, cloud-sql, alloydb)
- ALTER SYSTEM → ALTER DATABASE rewriting for managed platforms
- Executor max-retry limit (3 failures → mark as acted_on)
- LLM deduplication: skip redundant calls when open findings/hints exist
  (optimizer, tuner, advisor all check sage.findings/sage.query_hints first)
- 11 token waste fixes: bloat category mismatch, vacuum validation bug,
  thinking-model budget, CapturePlans loop hoist, column stats filtering,
  per-cycle table cap, per-query rewrite dedup, briefing LIMIT,
  retry scope (429/503 only), tuner stats cap, single-symptom deterministic skip
- Thinking model support: +16384 token overhead for Gemini 2.5 reasoning
- Cross-platform findings: 1615 total, 373 open, 802 acted on across 8 DBs
- All packages above 70% test coverage

## v0.8.2 (2026-04-03) — LLM Tuner + Query Rewrites
- Query tuner: hybrid deterministic rules (7 symptom kinds) + LLM-enhanced hints
- LLM-powered query rewrite suggestions alongside pg_hint_plan directives
- Rewrite suggestions surfaced in dashboard with rationale
- Alert notification (`query_rewrite_suggested` event) when rewrite is suggested
- Index optimizer multi-query consolidation (8 queries → minimal index set)
- E2E test suite: 54 subtests against real Gemini API
- 771+ tests

## v0.8.1 (2026-03-27) — Patch
- Add `google_ml` to all schema exclusion lists (Cloud SQL compatibility)
- Bump default LLM max_tokens to 8192 (Gemini 2.5 Flash thinking token fix)
- Add retry loop to index validity post-check (catalog propagation delay)
- Prevent re-execution of already-acted findings (re-drop race fix)
- Executor cooldown for recently created indexes
- Verified on Cloud SQL PG16/17 and AlloyDB PG17
- 588 tests, 0 failures

## v0.8.0 (2026-03-26) — Fleet Mode + Dashboard
- Fleet manager: single sidecar → N databases via `mode: fleet` config
- `DatabaseManager` with per-database collector/analyzer/executor goroutines
- Per-database advisory locks, trust levels, executor toggles
- Per-database LLM token budget (equal, proportional, or priority-weighted)
- Database-aware data model (every finding, action, metric carries `database_name`)
- Prometheus labels: `{database="prod-orders"}`
- Graceful per-database failure (one DB down doesn't crash others)
- REST API: 14 endpoints on `:8080` alongside MCP
- Fleet overview: `GET /api/v1/databases` with health scores
- Findings, actions, snapshots, config — all filterable by `?database=`
- Config hot-reload via `PUT /api/v1/config`
- Emergency stop/resume per-database and fleet-wide
- Web dashboard (React SPA embedded in binary via `//go:embed`)
- Demo environment: Docker Compose with 7 pre-planted problems, 46 verification checks
- 584+ tests, 0 failures, CI green (6 workflows)

**Integration bug fixes shipped in v0.8.0:**
- VACUUM routed through non-transaction connection (pgxpool wraps in tx by default)
- Trust ramp `ramp_start` config honored on first boot (was always `now()`)
- Unused index window default changed to 7 days (was 0, caused index churn)
- Advisor strips markdown fences from Gemini JSON responses
- `database_name` resolved to actual instance name (was showing "all")

## v0.7.0 (2026-03-26)

### Go Sidecar — The Product

pg_sage is now a Go sidecar binary that connects to any PostgreSQL 14-17 database.
The C extension is frozen at v0.6.0-rc3 (security fixes only).

#### Features
- **Standalone mode** — single binary, no extension install required
- **Index Optimizer v2** — LLM-powered index recommendations with HypoPG validation, confidence scoring, per-table circuit breakers, 8 validators
- **Vacuum Tuning** — per-table autovacuum analysis via LLM
- **WAL/Checkpoint Tuning** — max_wal_size, wal_compression, checkpoint analysis
- **Connection Pool Analysis** — max_connections, idle timeout, pooler detection
- **Memory Tuning** — shared_buffers, work_mem, cache hit ratio, spill detection
- **Query Rewrite Suggestions** — N+1, correlated subquery, OFFSET pagination detection
- **Bloat Remediation Planning** — VACUUM FULL vs pg_repack vs do nothing
- **MCP Server** — Claude Desktop and AI agent interface
- **Prometheus Metrics** — full observability endpoint
- **Dual-Model LLM** — separate models for general tasks vs index optimization
- **Trust-Ramped Executor** — observation -> advisory -> autonomous with rollback

#### Verified Platforms
- Google Cloud SQL (PG14, PG15, PG16, PG17)
- Google AlloyDB (PG17)
- Self-managed PostgreSQL (PG14-17)
- Amazon Aurora — test plan ready
- Amazon RDS — test plan ready

#### Testing
- 530 tests across 14 packages, 0 failures
- Live integration testing on Cloud SQL PG16, PG17, and AlloyDB PG17
- E2e tests with Gemini: 3 real LLM findings verified

#### C Extension (Frozen)
- v0.6.0-rc3 — no new features
- Works on self-managed PostgreSQL with auto-explain hooks
- SQL functions: sage.explain(), sage.diagnose(), sage.briefing()
