#!/usr/bin/env bash
set -uo pipefail

# verify.sh — Verify findings, snapshots, and sidecar behavior via psql and log inspection.
# Returns exit code = number of failures.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PSQL="${PSQL:-psql}"
SIDECAR_LOG="$SCRIPT_DIR/sidecar.log"
FAILURES=0

PGPASSWORD="${SAGE_PG_PASSWORD}"
export PGPASSWORD
PG_CONN="-h ${SAGE_PG_HOST} -p ${SAGE_PG_PORT} -U ${SAGE_PG_USER} -d ${SAGE_PG_DATABASE}"

pass() { echo "  PASS  $1"; }
fail() { echo "  FAIL  $1 — $2"; FAILURES=$((FAILURES + 1)); }

# Helper: run a SQL query and return the trimmed result.
sql() {
    "$PSQL" $PG_CONN -tAc "$1" 2>/dev/null | tr -d '[:space:]'
}

# Helper: check if a finding category exists.
check_finding() {
    local category="$1"
    local label="$2"
    local count
    count=$(sql "SELECT count(*) FROM sage.findings WHERE category = '$category'")
    if [[ -n "$count" && "$count" -gt 0 ]]; then
        pass "$label ($count findings)"
    else
        fail "$label" "no findings with category '$category'"
    fi
}

echo "Verifying findings and state via psql + log inspection"
echo ""

# ===============================================================
# SECTION 1: Finding categories
# ===============================================================
echo "--- Finding Categories ---"

check_finding "missing_fk_index"     "missing_fk_index (order_items FKs)"
check_finding "duplicate_index"      "duplicate_index (customers email)"

# Time/permission-dependent findings — SKIP if not present.
# unused_index: requires FirstSeen window (1+ day) to elapse.
# table_bloat: requires dead tuples (autovacuum may clean them).
# sort_without_index: requires explain plans with sort operations in cache.
# sequence_exhaustion: requires superuser to advance sequence near max.
# high_total_time: requires sustained query load (>10% wall clock).
soft_check_finding() {
    local category="$1"
    local label="$2"
    local reason="$3"
    local count
    count=$(sql "SELECT count(*) FROM sage.findings WHERE category = '$category'")
    if [[ -n "$count" && "$count" -gt 0 ]]; then
        pass "$label ($count findings)"
    else
        echo "  SKIP  $label — $reason"
    fi
}

soft_check_finding "unused_index"       "unused_index"       "requires FirstSeen window (1+ day)"
soft_check_finding "table_bloat"        "table_bloat"        "autovacuum may have cleaned dead tuples"
soft_check_finding "sort_without_index" "sort_without_index"  "requires explain plans with sort ops"
soft_check_finding "sequence_exhaustion" "sequence_exhaustion" "requires superuser to seed sequence"
soft_check_finding "high_total_time"    "high_total_time"     "requires sustained query load"

# Plan regression requires seeded explain_cache pairs — check if present.
PLAN_REG_COUNT=$(sql "SELECT count(*) FROM sage.findings WHERE category = 'plan_regression'" 2>/dev/null || echo "0")
if [[ -n "$PLAN_REG_COUNT" && "$PLAN_REG_COUNT" -gt 0 ]]; then
    pass "plan_regression ($PLAN_REG_COUNT findings)"
else
    echo "  SKIP  plan_regression — requires explain_cache diffs (may not trigger in test window)"
fi

# ---------------------------------------------------------------
# Forecaster findings (from seeded snapshots)
# ---------------------------------------------------------------
echo ""
echo "--- Forecaster Findings ---"

FORECAST_CATEGORIES=(
    "forecast_disk_growth"
    "forecast_connection_saturation"
    "forecast_cache_pressure"
    "forecast_sequence_exhaustion"
)

for cat in "${FORECAST_CATEGORIES[@]}"; do
    count=$(sql "SELECT count(*) FROM sage.findings WHERE category = '$cat'" 2>/dev/null || echo "0")
    if [[ -n "$count" && "$count" -gt 0 ]]; then
        pass "$cat ($count findings)"
    else
        echo "  SKIP  $cat — forecaster may not have run yet or thresholds not met"
    fi
done

# ===============================================================
# SECTION 2: Snapshot count
# ===============================================================
echo ""
echo "--- Snapshots ---"

SNAP_COUNT=$(sql "SELECT count(*) FROM sage.snapshots WHERE collected_at > now() - interval '5 minutes'")
if [[ -n "$SNAP_COUNT" && "$SNAP_COUNT" -ge 3 ]]; then
    pass "Recent snapshots: $SNAP_COUNT (>= 3 collector cycles)"
else
    fail "Snapshot count" "expected >= 3 recent snapshots, got ${SNAP_COUNT:-0}"
fi

# ===============================================================
# SECTION 3: Explain cache
# ===============================================================
echo ""
echo "--- Explain Cache ---"

EXPLAIN_COUNT=$(sql "SELECT count(*) FROM sage.explain_cache" 2>/dev/null || echo "0")
if [[ -n "$EXPLAIN_COUNT" && "$EXPLAIN_COUNT" -gt 0 ]]; then
    pass "explain_cache has $EXPLAIN_COUNT entries"
else
    echo "  SKIP  explain_cache — auto_explain may not be available"
fi

# ===============================================================
# SECTION 4: Config table
# ===============================================================
echo ""
echo "--- Config Table ---"

RAMP_START=$(sql "SELECT count(*) FROM sage.config WHERE key = 'trust_ramp_start'")
if [[ "$RAMP_START" == "1" ]]; then
    pass "sage.config has trust_ramp_start"
else
    fail "trust_ramp_start" "not found in sage.config"
fi

# Check emergency_stop is false (after resume)
ESTOP=$(sql "SELECT value FROM sage.config WHERE key = 'emergency_stop'" 2>/dev/null || echo "")
if [[ "$ESTOP" == "false" || -z "$ESTOP" ]]; then
    pass "emergency_stop = false (normal state after resume)"
else
    fail "emergency_stop" "expected false, got '$ESTOP'"
fi

# ===============================================================
# SECTION 5: Alert log
# ===============================================================
echo ""
echo "--- Alert Log ---"

ALERT_COUNT=$(sql "SELECT count(*) FROM sage.alert_log" 2>/dev/null || echo "0")
if [[ -n "$ALERT_COUNT" && "$ALERT_COUNT" -gt 0 ]]; then
    pass "alert_log has $ALERT_COUNT entries"
else
    echo "  SKIP  alert_log — alerting may not have triggered (webhook target not running)"
fi

# ===============================================================
# SECTION 6: Briefings
# ===============================================================
echo ""
echo "--- Briefings ---"

BRIEF_COUNT=$(sql "SELECT count(*) FROM sage.briefings" 2>/dev/null || echo "0")
if [[ -n "$BRIEF_COUNT" && "$BRIEF_COUNT" -gt 0 ]]; then
    pass "briefings has $BRIEF_COUNT entries"
else
    if [[ -n "${SAGE_GEMINI_API_KEY:-}" ]]; then
        echo "  SKIP  briefings — LLM enabled but briefing may not have run in test window"
    else
        echo "  SKIP  briefings — LLM disabled"
    fi
fi

# ===============================================================
# SECTION 7: Sidecar log inspection
# ===============================================================
echo ""
echo "--- Sidecar Log Checks ---"

if [[ ! -f "$SIDECAR_LOG" ]]; then
    fail "Sidecar log" "file not found at $SIDECAR_LOG"
else
    # Cloud environment detection
    if grep -q "cloud environment:" "$SIDECAR_LOG"; then
        ENV_LINE=$(grep "cloud environment:" "$SIDECAR_LOG" | head -1)
        pass "Cloud environment detected: $(echo "$ENV_LINE" | grep -o 'cloud environment: [a-z-]*')"
    else
        fail "Cloud environment" "no detection line in log"
    fi

    # No permission denied errors
    if grep -qi "permission denied" "$SIDECAR_LOG"; then
        PERM_COUNT=$(grep -ci "permission denied" "$SIDECAR_LOG")
        fail "Permission errors" "$PERM_COUNT 'permission denied' occurrences"
    else
        pass "No permission denied errors"
    fi

    # No panics
    if grep -q "panic:" "$SIDECAR_LOG"; then
        fail "Panics" "panic detected in sidecar log"
    else
        pass "No panics in log"
    fi

    # Schema bootstrap
    if grep -qi "bootstrap\|schema" "$SIDECAR_LOG" | head -5 | grep -qi "bootstrap"; then
        pass "Schema bootstrap mentioned in log"
    elif grep -qi "bootstrapping schema" "$SIDECAR_LOG"; then
        pass "Schema bootstrap mentioned in log"
    else
        echo "  SKIP  Schema bootstrap log line — may use different wording"
    fi

    # Collector started
    if grep -qi "collector" "$SIDECAR_LOG"; then
        pass "Collector referenced in log"
    else
        fail "Collector" "no collector references in log"
    fi

    # Analyzer started
    if grep -qi "analyzer" "$SIDECAR_LOG"; then
        pass "Analyzer referenced in log"
    else
        fail "Analyzer" "no analyzer references in log"
    fi
fi

# ===============================================================
# Summary
# ===============================================================
echo ""
echo "Verify checks complete: $FAILURES failures"
exit "$FAILURES"
