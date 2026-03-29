# pg_sage — Autonomous PostgreSQL DBA Agent

## What This Is

pg_sage is an autonomous PostgreSQL DBA agent that continuously monitors, analyzes, and maintains PostgreSQL databases. It runs as an **external Go sidecar process** that connects to Postgres over the standard wire protocol — no C extension, no shared_preload_libraries. It combines a deterministic rules engine (Tier 1), optional LLM-enhanced analysis (Tier 2), and a trust-ramped action executor (Tier 3).

**License:** AGPL-3.0
**Module:** `github.com/pg-sage/sidecar`
**Go:** 1.24
**Target:** PostgreSQL 14+
**Tests:** 762+ test functions, 0 failures
**Modes:** Standalone (single DB) and Fleet (multi-DB from one sidecar)

## Architecture

### Core Goroutines (per database instance)

1. **Collector** — snapshots `pg_stat_statements`, `pg_stat_user_tables`, `pg_stat_user_indexes`, sequences, replication state
2. **Analyzer** — Tier 1 rules engine, generates findings, invokes Tier 3 action executor at sufficient trust
3. **Briefing** — periodic Tier 2 health briefings when LLM is enabled
4. **Optimizer** — LLM-powered index recommendations (optional)
5. **Advisor** — LLM-powered config tuning: vacuum, WAL, connections, memory (optional)

### Tier 1 — Rules Engine (deterministic)
Index health (duplicates, unused, missing, bloat), query performance (slow queries, regressions, seq scans on large tables), sequence exhaustion, vacuum/bloat/dead tuples, config audit, security, replication monitoring, self-monitoring.

### Tier 2 — LLM-Enhanced (optional)
Daily briefings, interactive diagnose (ReAct loop), explain narrative, index optimization, config advising. Pluggable LLM provider via OpenAI-compatible endpoint (covers Gemini, OpenAI, Groq, Ollama, etc).

### Tier 3 — Action Executor
Trust-ramped: OBSERVATION (day 0-7, findings only) → ADVISORY (day 8-30, SAFE actions) → AUTONOMOUS (day 31+, MODERATE actions). HIGH-risk always requires manual confirmation. All actions logged with rollback metadata.

### Fleet Mode (v0.8.0)
One sidecar binary monitors N databases. `fleet.DatabaseManager` wraps instances. Standalone mode auto-normalizes into `Databases[0]`. REST API + React dashboard on `:8080`.

### Safety
Circuit breakers for DB and LLM. Emergency stop halts all autonomous activity. Per-database token budgets in fleet mode.

## Tech Stack

- **Postgres driver:** `github.com/jackc/pgx/v5` + `pgxpool`
- **Testing:** `go test` (no testify — uses stdlib `testing` package)
- **Config:** `gopkg.in/yaml.v3` + env vars + runtime overrides via `sage.config` table
- **Logging:** `log/slog` (structured, stdlib)
- **Frontend:** React 19 + Vite + Tailwind CSS v4 + Recharts + lucide-react
- **Embedding:** `go:embed` for React build assets in Go binary
- **Release:** goreleaser (builds linux/darwin amd64/arm64)

## Project Structure

```
pg_sage/
├── sidecar/                          # Go sidecar (main codebase)
│   ├── cmd/pg_sage_sidecar/
│   │   └── main.go                   # Entry point, orchestration
│   ├── internal/
│   │   ├── advisor/                  # LLM config tuning (vacuum, WAL, memory)
│   │   ├── analyzer/                 # Tier 1 rules engine + findings
│   │   ├── api/                      # REST API (17 endpoints) + embedded dashboard
│   │   │   ├── dist/                 # React build output (go:embed)
│   │   │   └── *.go                  # Router, handlers, middleware, helpers
│   │   ├── briefing/                 # Tier 2 health briefings
│   │   ├── collector/                # Stats snapshot collection
│   │   ├── config/                   # Config structs, loading, fleet normalization
│   │   ├── executor/                 # Tier 3 trust-gated action execution
│   │   ├── fleet/                    # Multi-DB manager, health scores, budgets
│   │   ├── ha/                       # High availability monitoring
│   │   ├── llm/                      # LLM provider (OpenAI-compatible)
│   │   ├── optimizer/                # LLM index recommendations
│   │   ├── retention/                # Data retention + cleanup
│   │   ├── schema/                   # DB schema bootstrap (sage.*)
│   │   └── startup/                  # Startup validation checks
│   ├── web/                          # React dashboard
│   │   ├── src/
│   │   │   ├── components/           # 11 reusable components
│   │   │   ├── pages/                # 5 pages (Dashboard, Findings, Actions, DB, Settings)
│   │   │   └── hooks/useAPI.js       # Polling fetch hook
│   │   ├── vite.config.js            # Output → ../internal/api/dist/
│   │   └── package.json
│   ├── go.mod
│   ├── Dockerfile
│   └── .golangci.yml
├── sql/                              # SQL schema files
├── src/                              # C extension (legacy reference on master)
├── docs/                             # Specifications
├── .claude/                          # Claude Code config
│   ├── settings.json                 # Hooks + permissions
│   └── skills/                       # TDD, pgx patterns, LLM provider
├── .goreleaser.yml                   # Release config
├── docker-compose.yml
├── config.example.yaml
├── roadmap.md
└── LICENSE
```

## Build & Run

```bash
# From sidecar/ directory
cd sidecar

# Build (includes embedded React dashboard)
cd web && npm ci && npm run build && cd ..
go build ./cmd/pg_sage_sidecar/

# Run tests (762+ tests)
go test ./... -count=1 -timeout 300s

# Run with race detector
go test ./... -race -count=1

# Integration tests only
go test ./... -tags=integration -count=1

# Lint
go vet ./...
golangci-lint run ./...

# Release build
cd .. && goreleaser build --snapshot --clean
```

## Servers & Ports

| Server | Port | Purpose |
|--------|------|---------|
| Prometheus | `:9187` | Metrics endpoint (text format) |
| API + Dashboard | `:8080` | REST API (`/api/v1/*`) + React SPA + web UI |

## REST API Endpoints (17)

All under `/api/v1/`, all accept `?database=` filter in fleet mode:

| Method | Path | Purpose |
|--------|------|---------|
| GET | `/databases` | Fleet overview + health scores |
| GET | `/findings` | List findings (filter: severity, status, database) |
| GET | `/findings/:id` | Finding detail |
| POST | `/findings/:id/suppress` | Suppress finding |
| POST | `/findings/:id/unsuppress` | Unsuppress finding |
| GET | `/actions` | List executed actions |
| GET | `/actions/:id` | Action detail |
| GET | `/snapshots/latest` | Latest collector snapshot |
| GET | `/snapshots/history` | Snapshot time series |
| GET | `/config` | Current config |
| PUT | `/config` | Update config (hot reload) |
| GET | `/metrics` | JSON metrics |
| POST | `/emergency-stop` | Halt all autonomous actions |
| POST | `/resume` | Resume after emergency stop |
| GET | `/forecasts` | Forecaster predictions |
| GET | `/query-hints` | Active query hints from tuner |
| GET | `/alert-log` | Alert delivery history |

## Testing Strategy

1100+ tests using stdlib `testing` package. Three levels:

1. **Unit tests** (`*_test.go`) — rules, config parsing, fleet manager, API handlers, optimizer, advisor
2. **Integration tests** (`//go:build integration`) — real HTTP via httptest, fleet scenarios
3. **Schema tests** — require local PostgreSQL on port 5432 (bootstrap, idempotency)

## Testing Standards

### Core Principle
Tests exist to **find bugs**, not to prove code works. A test suite that always passes is suspicious. If you write tests and they all pass on the first run, you probably wrote weak tests.

### Rules — Violations Are Blockers

1. **No silent skips.** After every `go test` run, grep output for `SKIP`, `TODO`, and `PENDING`. Report skipped tests explicitly with reasons. A skipped test is NOT a passing test. Do not report "all tests pass" if any test was skipped.

2. **No cached results.** Always run tests with `-count=1` to defeat Go's test cache. A cached pass is not a real pass.

3. **Coverage is mandatory.** Run `go test -cover -count=1 ./...` and report per-package coverage. If any package with business logic falls below 70%, write additional tests before reporting completion. Utility/helper packages have a 50% floor.

4. **Assertions must be specific.** `assert.NoError(err)` alone is never sufficient. Every test must also assert the actual return value, state change, or side effect. If a test would still pass with the function body replaced by `return nil, nil`, the test is broken.

5. **Never modify a test to make it pass.** If a test fails, fix the implementation. The only reason to modify a test is if the test itself contains a logical error — and if you do, explain exactly what was wrong with the test before changing it.

### Test Writing Process (Two Phases — Do Not Combine)

**Phase 1: Write tests.** Write all tests based on the spec or feature description. Include every category below. Do NOT run them yet. Commit or stage them.

**Phase 2: Run and fix.** Run the full suite. Fix implementation bugs that tests reveal. Track what failed and why. Report a summary of bugs found.

If you write tests and run them in the same step, you will unconsciously write tests that pass. Separating the phases eliminates this bias.

### Required Test Categories

For every feature, function, or component, tests must cover:

| Category | What to test | Example |
|---|---|---|
| **Happy path** | Expected input → expected output | Valid query → correct index recommendation |
| **Invalid input** | Malformed, wrong-type, or out-of-range input | Empty string query, negative threshold, SQL injection strings |
| **Nil/empty/zero** | nil pointers, empty slices, zero-value structs, empty maps | `nil` config, `[]QueryStat{}`, zero `ConfidenceScore` |
| **Error propagation** | Caller receives a meaningful, distinguishable error | DB connection refused → error contains "connection" not just "failed" |
| **Boundary conditions** | Thresholds, limits, cutoffs, window edges | `unused_index_window_days=0` vs `=1` vs `=7`, trust level boundaries |
| **Concurrent access** | Race conditions under parallel execution | Two advisors writing recommendations simultaneously |
| **State transitions** | Trust ramp levels, executor gating, mode changes | `monitor` → `advisory` → `auto`, fleet mode vs single mode |
| **Integration** | Real or dockerized Postgres, actual SQL execution | Index created via `CREATE INDEX CONCURRENTLY`, vacuum actually runs |

If a category genuinely doesn't apply to a component, write a comment explaining why: `// No concurrent access tests: this function is stateless and takes no shared references`

### Negative Testing Is Not Optional

For every happy-path test, ask: "What should happen when this goes wrong?" Then write that test. Specifically:

- **LLM responses:** Malformed JSON, markdown-wrapped JSON, empty response, timeout, rate limit error
- **PostgreSQL errors:** Connection lost mid-query, permission denied, table doesn't exist, extension not installed
- **Config values:** Missing keys, zero values where defaults are expected, conflicting settings
- **Executor actions:** DDL that fails mid-execution, `CONCURRENTLY` that can't acquire lock, vacuum on a table in a transaction

### Post-Test Audit (Run After Every Test Session)

After all tests pass, answer these questions for each test file:

1. **What input would break this that I haven't tested?**
2. **What behavior is NOT covered by any assertion?**
3. **Are there assertions that would pass even if the feature was completely broken?** (e.g., only checking `err == nil` without checking the result)
4. **Are there any test doubles (mocks/stubs) that hide real failure modes?** (e.g., mocking the DB so you never test actual SQL)

Fix every gap found. Report what was added.

### Verification Checklist Pattern

For integration and end-to-end testing, use the numbered checklist format:

```
CHECK-01: [PASS/FAIL] Fleet mode discovers all 3 databases
CHECK-02: [PASS/FAIL] REST API /api/databases returns correct count
CHECK-03: [PASS/FAIL] Executor respects trust_level=monitor (no actions taken)
...
```

- Every check must be programmatically verifiable where possible
- Checks that require manual verification must be tagged: `// MANUAL: requires browser inspection of dashboard`
- A checklist with any FAIL is a failing test run, even if individual unit tests pass
- Do not report success until every CHECK is PASS or explicitly acknowledged as MANUAL

### Known Failure Patterns to Watch For

These are bugs Claude Code has introduced in the past. Test for them proactively:

- **Default value masking:** Config values defaulting to zero instead of their intended default (e.g., `unused_index_window_days` defaulting to 0 instead of 7). Write tests that verify defaults without any config file present.
- **Markdown-wrapped LLM responses:** Gemini wrapping JSON in ```json fences. Test that `stripToJSON` handles this everywhere LLM output is parsed.
- **Transaction scope errors:** Operations like VACUUM that cannot run inside a transaction. Test that these use dedicated connections.
- **Fleet mode leaks:** Values like `database_name` returning "all" instead of the actual database name. Test per-database context isolation.
- **Confidence score boundaries:** Verify the optimizer reaches advisory threshold (0.5) without HypoPG. Test with and without HypoPG available.

### Reporting Format

After any test run, report in this exact format:

```
## Test Results

**Command:** `go test -cover -count=1 ./...`
**Total:** X passed, Y failed, Z skipped
**Coverage:** [per-package breakdown]

### Skipped Tests (must be zero or justified)
- pkg/analyzer: TestComplexJoinDetection — SKIPPED: requires pg_stat_statements (CI limitation)

### Failures (if any)
- pkg/executor: TestVacuumDedicatedConn — FAIL: vacuum attempted inside transaction

### Coverage Gaps (packages below threshold)
- pkg/fleet: 58% — missing tests for DatabaseManager connection pooling

### Bugs Found This Session
1. [BUG] executor.go:142 — VACUUM not using dedicated connection
2. [BUG] config.go:87 — unused_index_window_days defaults to 0, not 7

### Manual Checks Remaining
- CHECK-12: MANUAL — Dashboard dark mode toggle (requires browser)
```

No test session is complete without this report. "All tests pass" is never an acceptable summary.

## Code Style & Conventions

- **Go version:** 1.24 (use range-over-func, slog, etc.)
- **Naming:** PascalCase exported, camelCase internal
- **Packages:** `internal/` for everything. No public library API.
- **Errors:** Wrap with `fmt.Errorf("context: %w", err)`. Sentinel errors for known conditions.
- **Logging:** `log/slog` with structured fields
- **Context:** First argument on all I/O functions. Respect cancellation.
- **Database:** `pgxpool` only. Parameterized queries (`$1`). Never string-concat SQL.
- **Config:** YAML + env vars. Validate at startup, fail fast.
- **Interfaces:** Define at consumer. Keep small (1-3 methods).

## Dependencies (keep minimal)

- `github.com/jackc/pgx/v5` — Postgres driver + pool
- `gopkg.in/yaml.v3` — config parsing
- `github.com/fsnotify/fsnotify` — config hot reload
- `github.com/google/uuid` — finding/action IDs
- No ORM. No web framework. No DI container. No testify.

## What NOT To Do

- Do not use `database/sql` — use `pgx` native interface
- Do not use global mutable state — pass dependencies explicitly
- Do not ignore `context.Context` cancellation
- Do not hardcode connection strings — always config/env
- Do not store secrets in source — env vars or secret manager
- Do not skip error wrapping — bare `return err` loses context
- Do not use `panic` for recoverable errors
- Do not add dependencies without justification — stdlib first
- Do not modify existing migration files — append new numbered ones
- Do not edit `internal/api/dist/` manually — it's generated by `npm run build`
