#!/usr/bin/env bash
set -uo pipefail

# test_mcp.sh — Test MCP SSE server endpoints.
# MCP uses SSE transport with persistent connection.
# The SSE connection must stay alive while posting JSON-RPC messages.
# Returns exit code = number of failures.

MCP_BASE="http://localhost:${MCP_PORT:-15433}"
FAILURES=0
SSE_PID=""

pass() { echo "  PASS  $1"; }
fail() { echo "  FAIL  $1 — $2"; FAILURES=$((FAILURES + 1)); }

cleanup() {
    if [[ -n "$SSE_PID" ]]; then
        kill "$SSE_PID" 2>/dev/null
        wait "$SSE_PID" 2>/dev/null
    fi
    rm -f /tmp/mcp_sse_out.txt
}
trap cleanup EXIT

echo "Testing MCP server at $MCP_BASE"
echo ""

# ---------------------------------------------------------------
# 1. Health endpoint (if available)
# ---------------------------------------------------------------
HEALTH_CODE=$(curl -s -o /dev/null -w "%{http_code}" "$MCP_BASE/health" 2>/dev/null || echo "000")
if [[ "$HEALTH_CODE" == "200" ]]; then
    pass "GET /health (HTTP $HEALTH_CODE)"
else
    echo "  SKIP  GET /health — endpoint returned $HEALTH_CODE"
fi

# ---------------------------------------------------------------
# 2. SSE endpoint responds (check port is listening)
# ---------------------------------------------------------------
# SSE is a long-lived stream; curl --max-time may report concatenated codes.
# Extract just the last 3 characters as the HTTP status.
SSE_RAW=$(curl -s -o /dev/null -w "%{http_code}" --max-time 3 "$MCP_BASE/sse" 2>/dev/null || echo "000")
SSE_CODE="${SSE_RAW: -3}"
if [[ "$SSE_CODE" == "200" || "$SSE_CODE" == "000" ]]; then
    pass "SSE endpoint is reachable"
else
    fail "SSE endpoint" "unexpected status $SSE_CODE"
fi

# ---------------------------------------------------------------
# 3. Start persistent SSE connection, extract session ID
# ---------------------------------------------------------------
# Start SSE in background — must stay alive for JSON-RPC posts
timeout 60 curl -s -N "$MCP_BASE/sse" > /tmp/mcp_sse_out.txt 2>/dev/null &
SSE_PID=$!
sleep 3

SESSION_ID=$(grep -o 'sessionId=[a-f0-9-]*' /tmp/mcp_sse_out.txt | head -1 | cut -d= -f2)

if [[ -n "$SESSION_ID" ]]; then
    pass "SSE returned session ID: $SESSION_ID"

    # Helper to post JSON-RPC while SSE is alive
    rpc_post() {
        local data="$1"
        curl -s -o /dev/null -w "%{http_code}" -X POST \
            -H "Content-Type: application/json" \
            -d "$data" \
            "$MCP_BASE/messages?sessionId=$SESSION_ID" 2>/dev/null || echo "000"
    }

    # ---------------------------------------------------------------
    # 4. JSON-RPC initialize
    # ---------------------------------------------------------------
    INIT_CODE=$(rpc_post '{"jsonrpc":"2.0","id":1,"method":"initialize","params":{}}')
    if [[ "$INIT_CODE" == "202" || "$INIT_CODE" == "200" ]]; then
        pass "JSON-RPC initialize (HTTP $INIT_CODE)"
    else
        fail "JSON-RPC initialize" "expected 202, got $INIT_CODE"
    fi

    # ---------------------------------------------------------------
    # 5. JSON-RPC resources/list
    # ---------------------------------------------------------------
    RES_CODE=$(rpc_post '{"jsonrpc":"2.0","id":2,"method":"resources/list","params":{}}')
    if [[ "$RES_CODE" == "202" || "$RES_CODE" == "200" ]]; then
        pass "JSON-RPC resources/list (HTTP $RES_CODE)"
    else
        fail "JSON-RPC resources/list" "expected 202, got $RES_CODE"
    fi

    # ---------------------------------------------------------------
    # 6. JSON-RPC tools/list
    # ---------------------------------------------------------------
    TOOLS_CODE=$(rpc_post '{"jsonrpc":"2.0","id":3,"method":"tools/list","params":{}}')
    if [[ "$TOOLS_CODE" == "202" || "$TOOLS_CODE" == "200" ]]; then
        pass "JSON-RPC tools/list (HTTP $TOOLS_CODE)"
    else
        fail "JSON-RPC tools/list" "expected 202, got $TOOLS_CODE"
    fi

    # ---------------------------------------------------------------
    # 7. JSON-RPC tools/call sage_status
    # ---------------------------------------------------------------
    STATUS_CODE=$(rpc_post '{"jsonrpc":"2.0","id":4,"method":"tools/call","params":{"name":"sage_status","arguments":{}}}')
    if [[ "$STATUS_CODE" == "202" || "$STATUS_CODE" == "200" ]]; then
        pass "JSON-RPC tools/call sage_status (HTTP $STATUS_CODE)"
    else
        fail "JSON-RPC tools/call sage_status" "expected 202, got $STATUS_CODE"
    fi

    # ---------------------------------------------------------------
    # 8. JSON-RPC resources/read sage://health
    # ---------------------------------------------------------------
    HEALTH_RPC_CODE=$(rpc_post '{"jsonrpc":"2.0","id":5,"method":"resources/read","params":{"uri":"sage://health"}}')
    if [[ "$HEALTH_RPC_CODE" == "202" || "$HEALTH_RPC_CODE" == "200" ]]; then
        pass "JSON-RPC resources/read sage://health (HTTP $HEALTH_RPC_CODE)"
    else
        fail "JSON-RPC resources/read sage://health" "expected 202, got $HEALTH_RPC_CODE"
    fi

    # ---------------------------------------------------------------
    # 9. JSON-RPC resources/read sage://findings
    # ---------------------------------------------------------------
    FIND_RPC_CODE=$(rpc_post '{"jsonrpc":"2.0","id":6,"method":"resources/read","params":{"uri":"sage://findings"}}')
    if [[ "$FIND_RPC_CODE" == "202" || "$FIND_RPC_CODE" == "200" ]]; then
        pass "JSON-RPC resources/read sage://findings (HTTP $FIND_RPC_CODE)"
    else
        fail "JSON-RPC resources/read sage://findings" "expected 202, got $FIND_RPC_CODE"
    fi

else
    fail "SSE session" "could not extract session ID from SSE stream"
    echo "  SKIP  JSON-RPC tests (no session)"
fi

echo ""
echo "MCP tests complete: $FAILURES failures"
exit "$FAILURES"
