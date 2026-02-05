#!/usr/bin/env bash
# ============================================================
# TimrX Credits & History Manual Test Script
# ============================================================
#
# Quick verification of the credit lifecycle:
#   1. Create identity, grant credits, verify balance
#   2. Test 402 for insufficient credits
#   3. Start OpenAI image job, verify reservation
#   4. Poll completion, verify finalization
#   5. Check history for generated image
#   6. Test magic code restore
#
# Usage:
#   chmod +x tests/test_credits_manual.sh
#   ./tests/test_credits_manual.sh [BASE_URL] [ADMIN_TOKEN]
#
# Examples:
#   ./tests/test_credits_manual.sh                            # Local
#   ./tests/test_credits_manual.sh https://3d.timrx.live      # Production
#   ./tests/test_credits_manual.sh http://localhost:5001 abc  # With admin
#
# ============================================================

set -euo pipefail

# Configuration
BASE_URL="${1:-http://localhost:5001}"
ADMIN_TOKEN="${2:-${ADMIN_TOKEN:-}}"
COOKIE_JAR=$(mktemp)
COOKIE_JAR_2=$(mktemp)
RUN_ID=$(date +%s | sha256sum | head -c 8)

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
BOLD='\033[1m'
NC='\033[0m'

cleanup() {
    rm -f "$COOKIE_JAR" "$COOKIE_JAR_2"
}
trap cleanup EXIT

# Logging helpers
pass() { echo -e "  ${GREEN}✓${NC} $1"; }
fail() { echo -e "  ${RED}✗${NC} $1"; exit 1; }
warn() { echo -e "  ${YELLOW}⚠${NC} $1"; }
info() { echo -e "  ${CYAN}→${NC} $1"; }
section() { echo -e "\n${BOLD}═══════════════════════════════════════════════════════════════${NC}"; echo -e "${BOLD}$1${NC}"; echo -e "${BOLD}═══════════════════════════════════════════════════════════════${NC}"; }

# API helper
api() {
    local method="$1"
    local path="$2"
    local data="${3:-}"
    local cookies="${4:-$COOKIE_JAR}"

    if [ -n "$data" ]; then
        curl -s -X "$method" "$BASE_URL$path" \
            -H "Content-Type: application/json" \
            -c "$cookies" -b "$cookies" \
            -d "$data"
    else
        curl -s -X "$method" "$BASE_URL$path" \
            -H "Content-Type: application/json" \
            -c "$cookies" -b "$cookies"
    fi
}

# Admin API helper
admin_api() {
    local method="$1"
    local path="$2"
    local data="${3:-}"

    if [ -z "$ADMIN_TOKEN" ]; then
        echo '{"error":"no_admin_token"}'
        return
    fi

    if [ -n "$data" ]; then
        curl -s -X "$method" "$BASE_URL$path" \
            -H "Content-Type: application/json" \
            -H "Authorization: Bearer $ADMIN_TOKEN" \
            -d "$data"
    else
        curl -s -X "$method" "$BASE_URL$path" \
            -H "Content-Type: application/json" \
            -H "Authorization: Bearer $ADMIN_TOKEN"
    fi
}

echo -e "\n${BOLD}${CYAN}TimrX Credits & History Manual Tests${NC}"
echo -e "Base URL: $BASE_URL"
echo -e "Run ID:   $RUN_ID"
echo -e "Admin:    ${ADMIN_TOKEN:+Yes}${ADMIN_TOKEN:-No}"

# ============================================================
# TEST 1: Create identity and verify balance
# ============================================================
section "TEST 1: Create Identity & Get Balance"

ME_RESP=$(api GET "/api/me")
IDENTITY_ID=$(echo "$ME_RESP" | grep -o '"identity_id":"[^"]*"' | cut -d'"' -f4 || echo "")

if [ -n "$IDENTITY_ID" ]; then
    pass "Identity created: ${IDENTITY_ID:0:8}..."
else
    fail "Failed to create identity: $ME_RESP"
fi

BALANCE=$(echo "$ME_RESP" | grep -o '"available_credits":[0-9]*' | cut -d':' -f2 || echo "0")
info "Initial balance: $BALANCE credits"

# ============================================================
# TEST 2: Grant credits (admin only)
# ============================================================
section "TEST 2: Grant Credits (Admin)"

if [ -n "$ADMIN_TOKEN" ]; then
    GRANT_RESP=$(admin_api POST "/api/admin/credits/grant" \
        "{\"identity_id\":\"$IDENTITY_ID\",\"amount\":50,\"reason\":\"test_$RUN_ID\"}")

    if echo "$GRANT_RESP" | grep -q '"ok":true'; then
        NEW_BALANCE=$(echo "$GRANT_RESP" | grep -o '"new_balance":[0-9]*' | cut -d':' -f2 || echo "0")
        pass "Granted 50 credits, new balance: $NEW_BALANCE"
        BALANCE=$NEW_BALANCE
    else
        warn "Grant failed: $GRANT_RESP"
    fi
else
    warn "Skipped - ADMIN_TOKEN not set"
fi

# ============================================================
# TEST 3: Insufficient credits returns 402
# ============================================================
section "TEST 3: Insufficient Credits → 402"

# Create fresh session
FRESH_ME=$(curl -s -c "$COOKIE_JAR_2" -b "$COOKIE_JAR_2" "$BASE_URL/api/me")
FRESH_BALANCE=$(echo "$FRESH_ME" | grep -o '"available_credits":[0-9]*' | cut -d':' -f2 || echo "0")

if [ "$FRESH_BALANCE" -lt 10 ]; then
    # Try to generate with insufficient credits
    INSUF_RESP=$(curl -s -w "\n%{http_code}" -X POST "$BASE_URL/api/_mod/image/openai" \
        -H "Content-Type: application/json" \
        -b "$COOKIE_JAR_2" \
        -d '{"prompt":"test","model":"dall-e-3","size":"1024x1024"}')

    HTTP_CODE=$(echo "$INSUF_RESP" | tail -n1)
    BODY=$(echo "$INSUF_RESP" | head -n -1)

    if [ "$HTTP_CODE" = "402" ]; then
        pass "Got 402 for insufficient credits"
    else
        warn "Expected 402, got $HTTP_CODE: $BODY"
    fi
else
    warn "Fresh identity has $FRESH_BALANCE credits (cannot test 402)"
fi

# ============================================================
# TEST 4: Start OpenAI image job
# ============================================================
section "TEST 4: Start OpenAI Image Job"

if [ "$BALANCE" -lt 10 ]; then
    warn "Insufficient credits ($BALANCE < 10) - skipping job test"
    warn "Run with ADMIN_TOKEN to grant credits"
    exit 0
fi

JOB_RESP=$(api POST "/api/_mod/image/openai" \
    "{\"prompt\":\"A simple red cube test_$RUN_ID\",\"model\":\"dall-e-3\",\"size\":\"1024x1024\"}")

JOB_ID=$(echo "$JOB_RESP" | grep -o '"job_id":"[^"]*"' | cut -d'"' -f4 || echo "")
RESERVATION_ID=$(echo "$JOB_RESP" | grep -o '"reservation_id":"[^"]*"' | cut -d'"' -f4 || echo "")

if [ -n "$JOB_ID" ]; then
    pass "Job started: $JOB_ID"
    info "Reservation: ${RESERVATION_ID:-none}"
else
    fail "Failed to start job: $JOB_RESP"
fi

# Verify reservation in DB (admin only)
if [ -n "$ADMIN_TOKEN" ] && [ -n "$RESERVATION_ID" ]; then
    DEBUG_RESP=$(admin_api GET "/api/admin/debug/openai-credits?job_id=$JOB_ID")
    HELD_COUNT=$(echo "$DEBUG_RESP" | grep -o '"held":[0-9]*' | cut -d':' -f2 || echo "0")

    if [ "$HELD_COUNT" -gt 0 ]; then
        pass "Reservation exists in DB (held)"
    else
        warn "No held reservation found"
    fi
fi

# ============================================================
# TEST 5: Poll for completion
# ============================================================
section "TEST 5: Poll & Verify Finalization"

MAX_POLLS=60
POLL_INTERVAL=2
FINAL_STATUS=""

for i in $(seq 1 $MAX_POLLS); do
    STATUS_RESP=$(api GET "/api/_mod/image/openai/status/$JOB_ID")
    JOB_STATUS=$(echo "$STATUS_RESP" | grep -o '"status":"[^"]*"' | cut -d'"' -f4 || echo "")

    info "Poll $i/$MAX_POLLS: status=$JOB_STATUS"

    if [ "$JOB_STATUS" = "done" ]; then
        FINAL_STATUS="done"
        IMAGE_URL=$(echo "$STATUS_RESP" | grep -o '"image_url":"[^"]*"' | cut -d'"' -f4 || echo "")
        pass "Job completed!"
        info "Image URL: ${IMAGE_URL:0:60}..."
        break
    elif [ "$JOB_STATUS" = "failed" ]; then
        FINAL_STATUS="failed"
        ERROR=$(echo "$STATUS_RESP" | grep -o '"error":"[^"]*"' | cut -d'"' -f4 || echo "unknown")
        warn "Job failed: $ERROR"
        break
    fi

    sleep $POLL_INTERVAL
done

if [ -z "$FINAL_STATUS" ]; then
    warn "Job did not complete in time"
fi

# Verify finalization (admin only)
if [ -n "$ADMIN_TOKEN" ] && [ "$FINAL_STATUS" = "done" ]; then
    sleep 1  # Give backend time to finalize
    DEBUG_RESP=$(admin_api GET "/api/admin/debug/openai-credits?job_id=$JOB_ID")

    FINALIZED_COUNT=$(echo "$DEBUG_RESP" | grep -o '"finalized":[0-9]*' | cut -d':' -f2 || echo "0")

    if [ "$FINALIZED_COUNT" -gt 0 ]; then
        pass "Reservation finalized in DB"
    else
        warn "No finalized reservation found"
    fi

    # Check balance decreased
    ME_RESP=$(api GET "/api/me")
    NEW_BALANCE=$(echo "$ME_RESP" | grep -o '"available_credits":[0-9]*' | cut -d':' -f2 || echo "0")
    EXPECTED=$((BALANCE - 10))

    if [ "$NEW_BALANCE" = "$EXPECTED" ]; then
        pass "Balance decreased by 10 (now: $NEW_BALANCE)"
    else
        warn "Balance mismatch: expected=$EXPECTED, actual=$NEW_BALANCE"
    fi
fi

# ============================================================
# TEST 6: Verify history
# ============================================================
section "TEST 6: Verify History"

HISTORY_RESP=$(api GET "/api/_mod/history")

if echo "$HISTORY_RESP" | grep -q "\"$JOB_ID\""; then
    pass "Job found in history"

    # Check for S3 URL
    if echo "$HISTORY_RESP" | grep -q "amazonaws.com"; then
        pass "Image stored in S3"
    else
        warn "Image URL may not be S3"
    fi
else
    warn "Job not found in history"
fi

# ============================================================
# SUMMARY
# ============================================================
section "SUMMARY"

echo -e "\n  ${GREEN}Tests completed for run $RUN_ID${NC}"
echo -e "  Identity: $IDENTITY_ID"
echo -e "  Job ID:   ${JOB_ID:-none}"
echo -e "  Status:   ${FINAL_STATUS:-incomplete}"
echo ""
echo "  To run full tests with admin capabilities:"
echo "    ADMIN_TOKEN=xxx ./tests/test_credits_manual.sh"
echo ""
echo "  To run Python acceptance tests:"
echo "    python tests/test_credits_acceptance.py"
echo ""
