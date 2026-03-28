#!/usr/bin/env bash
set -uo pipefail

# test_api.sh — Test all 17 REST API endpoints.
# Returns exit code = number of failures.

BASE="http://localhost:${API_PORT:-18080}/api/v1"
FAILURES=0

pass() { echo "  PASS  $1"; }
fail() { echo "  FAIL  $1 — $2"; FAILURES=$((FAILURES + 1)); }

# Helper: GET request, check status code.
get_check() {
    local path="$1"
    local label="$2"
    local expect="${3:-200}"
    local resp
    resp=$(curl -s -w "\n%{http_code}" "$BASE$path" 2>/dev/null)
    local code
    code=$(echo "$resp" | tail -1)
    local body
    body=$(echo "$resp" | sed '$d')
    if [[ "$code" == "$expect" ]]; then
        pass "$label (HTTP $code)"
    else
        fail "$label" "expected $expect, got $code"
    fi
    echo "$body"
}

# Helper: POST request, check status code.
post_check() {
    local path="$1"
    local label="$2"
    local data="${3:-}"
    local expect="${4:-200}"
    local resp
    if [[ -n "$data" ]]; then
        resp=$(curl -s -w "\n%{http_code}" -X POST \
            -H "Content-Type: application/json" -d "$data" \
            "$BASE$path" 2>/dev/null)
    else
        resp=$(curl -s -w "\n%{http_code}" -X POST "$BASE$path" 2>/dev/null)
    fi
    local code
    code=$(echo "$resp" | tail -1)
    local body
    body=$(echo "$resp" | sed '$d')
    if [[ "$code" == "$expect" ]]; then
        pass "$label (HTTP $code)"
    else
        fail "$label" "expected $expect, got $code"
    fi
    echo "$body"
}

# Helper: PUT request, check status code.
put_check() {
    local path="$1"
    local label="$2"
    local data="$3"
    local expect="${4:-200}"
    local resp
    resp=$(curl -s -w "\n%{http_code}" -X PUT \
        -H "Content-Type: application/json" -d "$data" \
        "$BASE$path" 2>/dev/null)
    local code
    code=$(echo "$resp" | tail -1)
    local body
    body=$(echo "$resp" | sed '$d')
    if [[ "$code" == "$expect" ]]; then
        pass "$label (HTTP $code)"
    else
        fail "$label" "expected $expect, got $code"
    fi
    echo "$body"
}

echo "Testing API endpoints at $BASE"
echo ""

# ---------------------------------------------------------------
# 1. GET /databases
# ---------------------------------------------------------------
get_check "/databases" "GET /databases" > /dev/null

# ---------------------------------------------------------------
# 2. GET /findings
# ---------------------------------------------------------------
FINDINGS_BODY=$(get_check "/findings" "GET /findings")

# Verify response is a JSON array with findings
if echo "$FINDINGS_BODY" | grep -q '\['; then
    FINDING_COUNT=$(echo "$FINDINGS_BODY" | grep -o '"id"' | wc -l)
    if [[ "$FINDING_COUNT" -gt 0 ]]; then
        pass "GET /findings has $FINDING_COUNT findings"
    else
        fail "GET /findings content" "expected findings in response"
    fi
else
    fail "GET /findings content" "expected JSON array"
fi

# ---------------------------------------------------------------
# 3. GET /findings?severity=warning
# ---------------------------------------------------------------
get_check "/findings?severity=warning" "GET /findings?severity=warning" > /dev/null

# ---------------------------------------------------------------
# 4. GET /findings/{id}
# ---------------------------------------------------------------
FINDING_ID=$(echo "$FINDINGS_BODY" | grep -o '"id":"[0-9]*"' | head -1 | sed 's/"id":"//;s/"//')
if [[ -n "$FINDING_ID" ]]; then
    get_check "/findings/$FINDING_ID" "GET /findings/$FINDING_ID" > /dev/null
else
    fail "GET /findings/{id}" "no finding ID available to test"
fi

# ---------------------------------------------------------------
# 5. POST /findings/{id}/suppress
# ---------------------------------------------------------------
if [[ -n "$FINDING_ID" ]]; then
    post_check "/findings/$FINDING_ID/suppress" "POST /findings/{id}/suppress" > /dev/null
else
    fail "POST /findings/{id}/suppress" "no finding ID available"
fi

# ---------------------------------------------------------------
# 6. POST /findings/{id}/unsuppress
# ---------------------------------------------------------------
if [[ -n "$FINDING_ID" ]]; then
    post_check "/findings/$FINDING_ID/unsuppress" "POST /findings/{id}/unsuppress" > /dev/null
else
    fail "POST /findings/{id}/unsuppress" "no finding ID available"
fi

# ---------------------------------------------------------------
# 7. GET /actions
# ---------------------------------------------------------------
ACTIONS_BODY=$(get_check "/actions" "GET /actions")

# ---------------------------------------------------------------
# 8. GET /actions/{id}
# ---------------------------------------------------------------
ACTION_ID=$(echo "$ACTIONS_BODY" | grep -o '"id":"[0-9]*"' | head -1 | sed 's/"id":"//;s/"//')
if [[ -n "$ACTION_ID" ]]; then
    get_check "/actions/$ACTION_ID" "GET /actions/$ACTION_ID" > /dev/null
else
    echo "  SKIP  GET /actions/{id} — no actions recorded yet"
fi

# ---------------------------------------------------------------
# 9. GET /forecasts
# ---------------------------------------------------------------
get_check "/forecasts" "GET /forecasts" > /dev/null

# ---------------------------------------------------------------
# 10. GET /query-hints
# ---------------------------------------------------------------
get_check "/query-hints" "GET /query-hints" > /dev/null

# ---------------------------------------------------------------
# 11. GET /alert-log
# ---------------------------------------------------------------
get_check "/alert-log" "GET /alert-log" > /dev/null

# ---------------------------------------------------------------
# 12. GET /snapshots/latest
# ---------------------------------------------------------------
SNAP_BODY=$(get_check "/snapshots/latest" "GET /snapshots/latest")
if echo "$SNAP_BODY" | grep -q '"snapshot"'; then
    pass "GET /snapshots/latest has content"
else
    fail "GET /snapshots/latest content" "expected snapshot field"
fi

# ---------------------------------------------------------------
# 13. GET /snapshots/history
# ---------------------------------------------------------------
get_check "/snapshots/history" "GET /snapshots/history" > /dev/null

# ---------------------------------------------------------------
# 14. GET /config
# ---------------------------------------------------------------
CONFIG_BODY=$(get_check "/config" "GET /config")
if echo "$CONFIG_BODY" | grep -q '"mode"'; then
    pass "GET /config has mode field"
else
    fail "GET /config content" "expected mode field in response"
fi

# ---------------------------------------------------------------
# 15. PUT /config
# ---------------------------------------------------------------
put_check "/config" "PUT /config" \
    '{"analyzer":{"slow_query_threshold_ms":600}}' > /dev/null

# ---------------------------------------------------------------
# 16. GET /metrics
# ---------------------------------------------------------------
get_check "/metrics" "GET /metrics" > /dev/null

# ---------------------------------------------------------------
# 17. POST /emergency-stop
# ---------------------------------------------------------------
post_check "/emergency-stop" "POST /emergency-stop" > /dev/null

# ---------------------------------------------------------------
# 18. POST /resume
# ---------------------------------------------------------------
post_check "/resume" "POST /resume" > /dev/null

echo ""
echo "API tests complete: $FAILURES failures"
exit "$FAILURES"
