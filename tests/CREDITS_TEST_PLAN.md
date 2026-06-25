# TimrX Credits & History Acceptance Test Plan

## Overview

This document describes the acceptance tests for the credit lifecycle and history persistence in TimrX. These tests verify that credits are properly deducted, history is saved, and account restore works correctly.

## Test Scenarios

### Test 1: Identity Creation & Balance Verification

**Goal:** Verify that anonymous users get an identity and can see their credit balance.

**Steps:**
1. Call `GET /api/me` with no session
2. Verify identity_id is returned
3. Verify balance fields are present (available_credits or credits_balance)

**Expected Results:**
- 200 OK response
- identity_id is a valid UUID
- Balance is numeric (may be 0 for new users without free credits)

---

### Test 2: Credit Granting (Admin)

**Goal:** Verify admins can grant credits and balance updates correctly.

**Prerequisites:** ADMIN_TOKEN environment variable set

**Steps:**
1. Call `POST /api/admin/credits/grant` with identity_id and amount
2. Verify new_balance in response
3. Call `GET /api/me` to verify balance matches

**Expected Results:**
- 200 OK response with ok: true
- new_balance = initial_balance + granted_amount
- /api/me shows same balance

---

### Test 3: Insufficient Credits → 402

**Goal:** Verify that insufficient credits blocks generation with proper error.

**Steps:**
1. Create fresh session with 0 credits
2. Call `POST /api/_mod/image/openai` with valid payload
3. Verify 402 response with INSUFFICIENT_CREDITS code

**Expected Results:**
- 402 Payment Required status
- Error code: "INSUFFICIENT_CREDITS"
- Response includes required/available/missing amounts

---

### Test 4: Start OpenAI Image Job

**Goal:** Verify job creation reserves credits immediately.

**Prerequisites:** Balance >= 10 credits

**Steps:**
1. Record balance before
2. Call `POST /api/_mod/image/openai`
3. Verify job_id returned
4. (Admin) Verify reservation row with status='held' in DB

**Expected Results:**
- 200 OK response
- job_id is returned
- reservation_id is returned
- DB shows credit_reservations row with status='held'

---

### Test 5: Job Completion & Credit Finalization

**Goal:** Verify credits are captured after successful job completion.

**Steps:**
1. Poll `GET /api/_mod/image/openai/status/{job_id}` until done/failed
2. For done: verify image_url returned
3. (Admin) Verify reservation status='finalized'
4. (Admin) Verify ledger_entries has negative debit
5. Verify /api/me shows balance decreased by 10

**Expected Results (Success):**
- status: "done"
- image_url present
- Reservation finalized
- Ledger entry: amount_credits = -10
- Balance decreased by 10

**Expected Results (Failure):**
- status: "failed"
- Reservation released (refunded)
- Balance unchanged

---

### Test 6: History Verification

**Goal:** Verify generated images appear in user history.

**Steps:**
1. Call `GET /api/_mod/history`
2. Find job_id in results
3. Verify image_url is S3 (not Meshy)
4. Verify required fields present

**Expected Results:**
- Job appears in history list
- image_url contains "amazonaws.com" or ".s3."
- status field is "finished" or "done"

---

### Test 7: Magic Code Restore

**Goal:** Verify account restore shows same credits and history on new device.

**Prerequisites:** ADMIN_TOKEN for setting email and generating test codes

**Steps:**
1. Set email on identity via admin API
2. Record current balance and history count
3. Request magic code
4. (Admin) Get plain-text code via debug endpoint
5. Create new session on "different device"
6. Redeem magic code
7. Verify identity_id matches original
8. Verify balance matches
9. Verify history count matches

**Expected Results:**
- New session linked to original identity
- Same identity_id after restore
- Same available_credits
- Same history items

---

## Running Tests

### Automated (Python)

```bash
# Install dependencies
pip install requests python-dotenv

# Run against local
python tests/test_credits_acceptance.py

# Run against production
API_BASE=https://3d.timrx.live python tests/test_credits_acceptance.py

# With admin capabilities
ADMIN_TOKEN=xxx python tests/test_credits_acceptance.py

# Verbose mode
VERBOSE=1 ADMIN_TOKEN=xxx python tests/test_credits_acceptance.py

# Quick mode (skip job completion wait)
QUICK=1 python tests/test_credits_acceptance.py

# Show curl commands
python tests/test_credits_acceptance.py --curl
```

### Manual (Bash)

```bash
# Make executable
chmod +x tests/test_credits_manual.sh

# Run locally
./tests/test_credits_manual.sh

# Run against production with admin
./tests/test_credits_manual.sh https://3d.timrx.live YOUR_ADMIN_TOKEN
```

### Manual (curl)

```bash
# Create session
curl -c cookies.txt -b cookies.txt "$BASE/api/me" | jq

# Start job
curl -X POST "$BASE/api/_mod/image/openai" \
  -H "Content-Type: application/json" \
  -b cookies.txt \
  -d '{"prompt":"test","model":"dall-e-3","size":"1024x1024"}' | jq

# Poll status
JOB_ID=xxx
curl -b cookies.txt "$BASE/api/_mod/image/openai/status/$JOB_ID" | jq

# Debug credits (admin)
curl "$BASE/api/admin/debug/openai-credits?job_id=$JOB_ID" \
  -H "Authorization: Bearer $ADMIN_TOKEN" | jq
```

---

## Debug Endpoints

### GET /api/admin/debug/openai-credits

Check credit flow for OpenAI image jobs.

**Query Parameters:**
- `identity_id` (optional): Filter by user
- `job_id` (optional): Filter by specific job
- `limit` (optional): Max results (default 20)

**Response includes:**
- reservations: Credit reservation rows
- ledger_entries: Wallet ledger entries
- jobs: OpenAI image jobs
- wallet: Current wallet state
- diagnosis: Automated analysis

### GET /api/admin/debug/magic-code

Generate and return a test magic code (testing only).

**Query Parameters:**
- `email` (required): Email address

**Response:**
- code: Plain-text 6-digit code
- expires_in_minutes: Code validity

---

## Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| API_BASE | Backend URL | http://localhost:5001 |
| ADMIN_TOKEN | Admin auth token | (none) |
| VERBOSE | Show debug output | false |
| QUICK | Skip slow tests | false |

---

## Database Verification

To manually verify in the database:

```sql
-- Check reservations
SELECT id, identity_id, action_code, cost_credits, status, created_at
FROM timrx_billing.credit_reservations
WHERE action_code = 'OPENAI_IMAGE'
ORDER BY created_at DESC LIMIT 10;

-- Check ledger entries
SELECT id, identity_id, entry_type, amount_credits, created_at
FROM timrx_billing.ledger_entries
WHERE entry_type = 'RESERVATION_FINALIZE'
ORDER BY created_at DESC LIMIT 10;

-- Check wallet balance
SELECT identity_id, balance_credits, updated_at
FROM timrx_billing.wallets
WHERE identity_id = 'YOUR_IDENTITY_ID';

-- Check history items
SELECT id, identity_id, type, status, created_at
FROM timrx_app.history_items
WHERE identity_id = 'YOUR_IDENTITY_ID'
ORDER BY created_at DESC LIMIT 10;
```

---

## Troubleshooting

### "INSUFFICIENT_CREDITS" when user should have credits

1. Check `/api/me` returns correct balance
2. Check reservations for 'held' status (credits may be reserved)
3. Run debug endpoint to see wallet state

### Job completes but credits not deducted

1. Check backend logs for `[CREDITS:DEBUG]` messages
2. Verify `finalize_job_credits` was called
3. Check reservation status in DB (should be 'finalized')
4. Check ledger for debit entry

### History not showing generated images

1. Check job status is 'done' (not 'failed')
2. Verify identity_id matches between session and history
3. Check S3 upload succeeded
4. Check `timrx_app.history_items` table

### Magic code restore fails

1. Verify email exists on identity
2. Check code hasn't expired (default 15 min)
3. Check max attempts not exceeded
4. Verify session update in `timrx_billing.sessions`
