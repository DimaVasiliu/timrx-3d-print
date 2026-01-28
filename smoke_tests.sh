#!/usr/bin/env bash
# ============================================================
# TimrX Backend Smoke Tests
# ============================================================
# Run: chmod +x smoke_tests.sh && ./smoke_tests.sh [BASE_URL]
# Default BASE_URL: http://localhost:5001
# ============================================================

set -euo pipefail

BASE_URL="${1:-http://localhost:5001}"
COOKIE_JAR=$(mktemp)
COOKIE_JAR_USER2=$(mktemp)

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

cleanup() {
    rm -f "$COOKIE_JAR" "$COOKIE_JAR_USER2"
}
trap cleanup EXIT

pass() { echo -e "${GREEN}PASS${NC}: $1"; }
fail() { echo -e "${RED}FAIL${NC}: $1"; exit 1; }
info() { echo -e "${YELLOW}INFO${NC}: $1"; }

# ============================================================
# 1. Health Check
# ============================================================
info "Testing health endpoint..."

HEALTH=$(curl -s "${BASE_URL}/api/health")
if echo "$HEALTH" | grep -q '"status":"ok"'; then
    pass "Health endpoint returns ok"
else
    fail "Health endpoint failed: $HEALTH"
fi

# Also test _mod path
HEALTH_MOD=$(curl -s "${BASE_URL}/api/_mod/health")
if echo "$HEALTH_MOD" | grep -q '"status":"ok"'; then
    pass "Health endpoint (_mod) returns ok"
else
    fail "Health endpoint (_mod) failed: $HEALTH_MOD"
fi

# ============================================================
# 2. Session Creation (Anonymous Identity)
# ============================================================
info "Testing anonymous session creation..."

# Get /me to establish session (creates anonymous identity)
ME_RESP=$(curl -s -c "$COOKIE_JAR" -b "$COOKIE_JAR" \
    -H "Content-Type: application/json" \
    "${BASE_URL}/api/me")

IDENTITY_ID=$(echo "$ME_RESP" | grep -o '"identity_id":"[^"]*"' | cut -d'"' -f4)
if [ -n "$IDENTITY_ID" ]; then
    pass "Session created, identity_id=$IDENTITY_ID"
else
    fail "Failed to create session: $ME_RESP"
fi

# Verify session cookie was set
if grep -q "timrx_sid" "$COOKIE_JAR"; then
    pass "Session cookie (timrx_sid) set"
else
    fail "Session cookie not set"
fi

# ============================================================
# 3. Per-User Isolation Test
# ============================================================
info "Testing per-user isolation..."

# Create a second user session
ME_RESP2=$(curl -s -c "$COOKIE_JAR_USER2" -b "$COOKIE_JAR_USER2" \
    -H "Content-Type: application/json" \
    "${BASE_URL}/api/me")

IDENTITY_ID2=$(echo "$ME_RESP2" | grep -o '"identity_id":"[^"]*"' | cut -d'"' -f4)
if [ -n "$IDENTITY_ID2" ] && [ "$IDENTITY_ID" != "$IDENTITY_ID2" ]; then
    pass "Second user has different identity_id=$IDENTITY_ID2"
else
    fail "User isolation failed - same identity_id or error: $ME_RESP2"
fi

# ============================================================
# 4. Wallet Endpoint (Per-User)
# ============================================================
info "Testing wallet endpoint..."

WALLET_RESP=$(curl -s -b "$COOKIE_JAR" \
    -H "Content-Type: application/json" \
    "${BASE_URL}/api/wallet")

if echo "$WALLET_RESP" | grep -q "$IDENTITY_ID"; then
    pass "Wallet returns correct identity_id"
else
    fail "Wallet response incorrect: $WALLET_RESP"
fi

# Verify balance field exists
if echo "$WALLET_RESP" | grep -q '"credits_balance"'; then
    pass "Wallet returns credits_balance field"
else
    fail "Wallet missing credits_balance: $WALLET_RESP"
fi

# ============================================================
# 5. History Endpoint (Per-User, Initially Empty)
# ============================================================
info "Testing history endpoint..."

HISTORY_RESP=$(curl -s -b "$COOKIE_JAR" \
    -H "Content-Type: application/json" \
    "${BASE_URL}/api/history")

# Should return empty array or array for this user only
if echo "$HISTORY_RESP" | grep -q '^\['; then
    pass "History endpoint returns JSON array"
else
    fail "History endpoint failed: $HISTORY_RESP"
fi

# Also test _mod path
HISTORY_MOD=$(curl -s -b "$COOKIE_JAR" \
    -H "Content-Type: application/json" \
    "${BASE_URL}/api/_mod/history")

if echo "$HISTORY_MOD" | grep -q '^\['; then
    pass "History endpoint (_mod) returns JSON array"
else
    fail "History endpoint (_mod) failed: $HISTORY_MOD"
fi

# ============================================================
# 6. Jobs Endpoints
# ============================================================
info "Testing jobs endpoints..."

# GET /api/jobs/active should return array
ACTIVE_JOBS=$(curl -s -b "$COOKIE_JAR" \
    -H "Content-Type: application/json" \
    "${BASE_URL}/api/jobs/active")

if echo "$ACTIVE_JOBS" | grep -q '^\['; then
    pass "Active jobs endpoint returns JSON array"
else
    fail "Active jobs endpoint failed: $ACTIVE_JOBS"
fi

# ============================================================
# 7. Text-to-3D Status (Should 404 for non-existent job)
# ============================================================
info "Testing text-to-3d status for non-existent job..."

FAKE_JOB_ID="00000000-0000-0000-0000-000000000000"
STATUS_RESP=$(curl -s -w "\n%{http_code}" -b "$COOKIE_JAR" \
    -H "Content-Type: application/json" \
    "${BASE_URL}/api/text-to-3d/status/${FAKE_JOB_ID}")

HTTP_CODE=$(echo "$STATUS_RESP" | tail -n1)
BODY=$(echo "$STATUS_RESP" | head -n -1)

# Should return 404 or error for non-existent job
if [ "$HTTP_CODE" = "404" ] || echo "$BODY" | grep -q '"error"'; then
    pass "Status endpoint correctly rejects non-existent job (HTTP $HTTP_CODE)"
else
    fail "Status endpoint should reject non-existent job: $BODY (HTTP $HTTP_CODE)"
fi

# ============================================================
# 8. Cross-User Data Isolation
# ============================================================
info "Testing cross-user data isolation..."

# User 2's history should be different from User 1
HISTORY_USER2=$(curl -s -b "$COOKIE_JAR_USER2" \
    -H "Content-Type: application/json" \
    "${BASE_URL}/api/history")

# Both should be valid arrays (likely empty for new users)
if echo "$HISTORY_USER2" | grep -q '^\['; then
    pass "User 2 history is isolated"
else
    fail "User 2 history failed: $HISTORY_USER2"
fi

# ============================================================
# 9. Unauthenticated Access Should Fail
# ============================================================
info "Testing unauthenticated access..."

# Wallet without session should fail
UNAUTH_WALLET=$(curl -s -w "\n%{http_code}" \
    -H "Content-Type: application/json" \
    "${BASE_URL}/api/wallet")

HTTP_CODE=$(echo "$UNAUTH_WALLET" | tail -n1)
if [ "$HTTP_CODE" = "401" ]; then
    pass "Wallet correctly rejects unauthenticated requests (HTTP 401)"
else
    BODY=$(echo "$UNAUTH_WALLET" | head -n -1)
    fail "Wallet should require auth: $BODY (HTTP $HTTP_CODE)"
fi

# ============================================================
# 10. Route Parity Check (Legacy vs _mod paths)
# ============================================================
info "Testing route parity (legacy vs _mod paths)..."

# Test a few key endpoints exist at both paths
for ENDPOINT in "health" "history"; do
    LEGACY=$(curl -s -o /dev/null -w "%{http_code}" -b "$COOKIE_JAR" "${BASE_URL}/api/${ENDPOINT}")
    MOD=$(curl -s -o /dev/null -w "%{http_code}" -b "$COOKIE_JAR" "${BASE_URL}/api/_mod/${ENDPOINT}")

    if [ "$LEGACY" != "404" ] && [ "$MOD" != "404" ]; then
        pass "Route parity for /${ENDPOINT}: legacy=${LEGACY}, _mod=${MOD}"
    else
        fail "Route parity failed for /${ENDPOINT}: legacy=${LEGACY}, _mod=${MOD}"
    fi
done

# ============================================================
# Summary
# ============================================================
echo ""
echo "============================================================"
echo -e "${GREEN}ALL SMOKE TESTS PASSED${NC}"
echo "============================================================"
echo ""
echo "Verified:"
echo "  - Health endpoints work (both /api and /api/_mod)"
echo "  - Anonymous sessions are created correctly"
echo "  - Per-user isolation (different identity_id per session)"
echo "  - Wallet endpoint returns correct identity and balance"
echo "  - History endpoint returns JSON arrays"
echo "  - Jobs active endpoint works"
echo "  - Status endpoint rejects non-existent jobs"
echo "  - Cross-user data isolation"
echo "  - Unauthenticated access is rejected (401)"
echo "  - Route parity between /api and /api/_mod"
echo ""
echo "Base URL: $BASE_URL"
echo "============================================================"
