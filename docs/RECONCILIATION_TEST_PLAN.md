# Reconciliation System Test Plan

This document outlines the testing strategy for the payment reconciliation system.

## Overview

The reconciliation system has two phases:
1. **Safety Reconciliation**: Database internal consistency checks
2. **Mollie Reconciliation**: Compare Mollie API payments to database state

## Pre-requisites

- Access to staging environment with test Mollie API key
- Admin API token for triggering reconciliation endpoints
- Database access for verification queries

## Test Categories

### 1. One-Time Purchase Reconciliation

#### 1.1 Missing Purchase Record

**Scenario**: Payment exists in Mollie but no purchase record in DB.

**Setup**:
```sql
-- Create a test identity
INSERT INTO timrx_billing.identities (id, email)
VALUES ('test-id-1', 'test@example.com');

-- Create wallet
INSERT INTO timrx_billing.wallets (identity_id, balance_credits)
VALUES ('test-id-1', 0);

-- Simulate: Mollie payment completed but purchase not created
-- (This would happen if webhook failed after Mollie confirmation)
```

**Mollie State**:
- Payment ID: `tr_test_purchase_001`
- Status: `paid`
- Metadata: `{"type": "one_time", "identity_id": "test-id-1", "credits": 100, "plan_code": "credits_100"}`

**Expected Behavior**:
1. Reconciliation detects missing purchase
2. Creates purchase record with `provider_payment_id = tr_test_purchase_001`
3. Creates ledger entry: +100 credits
4. Updates wallet balance: +100 credits
5. Logs fix in `reconciliation_fixes` table

**Verification**:
```sql
-- Check purchase created
SELECT * FROM timrx_billing.purchases WHERE provider_payment_id = 'tr_test_purchase_001';

-- Check ledger entry
SELECT * FROM timrx_billing.ledger_entries
WHERE reference_type = 'purchase' AND identity_id = 'test-id-1'
ORDER BY created_at DESC LIMIT 1;

-- Check wallet balance
SELECT balance_credits FROM timrx_billing.wallets WHERE identity_id = 'test-id-1';

-- Check fix logged
SELECT * FROM timrx_billing.reconciliation_fixes
WHERE provider_payment_id = 'tr_test_purchase_001';
```

#### 1.2 Purchase Exists but Missing Ledger Entry

**Scenario**: Purchase record exists but ledger entry missing.

**Setup**:
```sql
-- Create purchase without ledger entry
INSERT INTO timrx_billing.purchases (identity_id, plan_code, credits_granted, status, provider_payment_id)
VALUES ('test-id-1', 'credits_100', 100, 'completed', 'tr_test_purchase_002');
```

**Expected Behavior**:
1. Safety reconciliation detects missing ledger
2. Creates ledger entry: +100 credits
3. Updates wallet balance

---

### 2. Subscription Reconciliation (PERIOD-SAFE)

**Key Principles**:
- Credits are granted MONTHLY, even for yearly plans
- Cycles are keyed by (subscription_id, period_start) AND (provider, provider_payment_id)
- billing_day determines cycle boundaries (e.g., billing_day=18 → cycles run 18th to 18th)

#### 2.1 Paid Payment Grants Missing Cycle

**Scenario**: Subscription payment exists in Mollie but cycle credits not granted.

**Setup**:
```sql
-- Create subscription with billing_day = 18
INSERT INTO timrx_billing.subscriptions (
    id, identity_id, plan_code, status, billing_day, provider
)
VALUES (
    'sub-uuid-001', 'test-id-1', 'creator_monthly', 'active', 18, 'mollie'
);
```

**Mollie State**:
- Payment ID: `tr_test_sub_payment_001`
- Status: `paid`
- paidAt: `2026-02-18T10:00:00Z`
- Metadata: `{"type": "subscription", "identity_id": "test-id-1", "plan_code": "creator_monthly", "subscription_id": "sub-uuid-001"}`

**Expected Behavior**:
1. Reconciliation parses paidAt → 2026-02-18
2. Calculates period: 2026-02-18 → 2026-03-18 (billing_day=18)
3. Checks: no cycle exists for this period
4. Creates subscription_cycle with provider_payment_id = tr_test_sub_payment_001
5. Grants 500 credits (creator plan)
6. Logs fix with fix_type = 'subscription_cycle_granted'

**Verification**:
```sql
-- Check cycle created with correct period
SELECT id, period_start, period_end, credits_granted, provider_payment_id
FROM timrx_billing.subscription_cycles
WHERE subscription_id = 'sub-uuid-001';

-- Expected: period_start = 2026-02-18, period_end = 2026-03-18
-- Expected: provider_payment_id = 'tr_test_sub_payment_001'

-- Check fix logged
SELECT * FROM timrx_billing.reconciliation_fixes
WHERE provider_payment_id = 'tr_test_sub_payment_001'
AND fix_type = 'subscription_cycle_granted';
```

---

#### 2.2 Duplicate Webhook + Reconciliation Does NOT Double Grant

**Scenario**: Same payment processed by webhook AND reconciliation.

**Setup**:
```sql
-- Subscription exists
INSERT INTO timrx_billing.subscriptions (
    id, identity_id, plan_code, status, billing_day, provider
)
VALUES ('sub-uuid-002', 'test-id-2', 'creator_monthly', 'active', 1, 'mollie');
```

**Test Steps**:
1. Webhook receives payment `tr_test_dup_001` (paidAt: 2026-03-01) → grants cycle
2. Run reconciliation on same day
3. Reconciliation finds same payment `tr_test_dup_001`

**Expected Behavior**:
- Reconciliation checks: provider_payment_id = 'tr_test_dup_001' already exists
- Returns None (no fix needed)
- Total cycles for this subscription: 1 (not 2)

**Verification**:
```sql
-- Should return exactly 1 row
SELECT COUNT(*) FROM timrx_billing.subscription_cycles
WHERE subscription_id = 'sub-uuid-002';

-- Should have unique constraint violation if tried to insert again
-- The unique index uq_subscription_cycles_provider_payment prevents duplicates
```

---

#### 2.3 Yearly Plan Grants MONTHLY Cycle (Not Yearly)

**Scenario**: Yearly subscription payment should grant 1 month of credits, not 12.

**Setup**:
```sql
-- Create yearly subscription with 12 months remaining
INSERT INTO timrx_billing.subscriptions (
    id, identity_id, plan_code, status, billing_day, provider, credits_remaining_months
)
VALUES ('sub-uuid-003', 'test-id-3', 'creator_yearly', 'active', 15, 'mollie', 12);
```

**Mollie State**:
- Payment ID: `tr_test_yearly_001`
- Status: `paid`
- paidAt: `2026-01-15T12:00:00Z`
- Metadata: `{"type": "subscription", "identity_id": "test-id-3", "plan_code": "creator_yearly"}`

**Expected Behavior**:
1. Calculates period: 2026-01-15 → 2026-02-15 (1 MONTH, not 365 days)
2. Grants 500 credits (1 month's worth)
3. Decrements credits_remaining_months: 12 → 11
4. Does NOT grant 6000 credits (12 months at once)

**Verification**:
```sql
-- Check cycle is monthly, not yearly
SELECT period_start, period_end,
       (period_end - period_start) as duration
FROM timrx_billing.subscription_cycles
WHERE subscription_id = 'sub-uuid-003';

-- Expected duration: ~30 days (not 365)

-- Check remaining months decremented
SELECT credits_remaining_months FROM timrx_billing.subscriptions
WHERE id = 'sub-uuid-003';

-- Expected: 11
```

---

#### 2.4 Billing Day 31 in February Resolves to Last Day

**Scenario**: Subscription with billing_day=31, payment in February.

**Setup**:
```sql
-- Create subscription with billing_day = 31
INSERT INTO timrx_billing.subscriptions (
    id, identity_id, plan_code, status, billing_day, provider
)
VALUES ('sub-uuid-004', 'test-id-4', 'starter_monthly', 'active', 31, 'mollie');
```

**Mollie State**:
- Payment ID: `tr_test_feb_001`
- Status: `paid`
- paidAt: `2026-02-28T09:00:00Z` (Feb 28, 2026)
- Metadata: `{"type": "subscription", "identity_id": "test-id-4", "plan_code": "starter_monthly"}`

**Expected Behavior**:
1. billing_day=31, but February 2026 has 28 days
2. Resolves: period_start = 2026-02-28 (last day of Feb)
3. Resolves: period_end = 2026-03-31 (March has 31 days)
4. Grants starter credits (100)

**Verification**:
```sql
-- Check cycle period handles month-end correctly
SELECT period_start, period_end
FROM timrx_billing.subscription_cycles
WHERE subscription_id = 'sub-uuid-004';

-- Expected: period_start = 2026-02-28, period_end = 2026-03-31
```

---

#### 2.5 Subscription Lookup Priority

**Scenario**: Payment metadata has subscription_id vs. lookup by identity+plan.

**Test A: With subscription_id in metadata** (preferred):
```json
{
  "type": "subscription",
  "identity_id": "test-id-5",
  "plan_code": "creator_monthly",
  "subscription_id": "sub-uuid-exact"
}
```
- Reconciliation uses subscription_id directly
- Deterministic, no ambiguity

**Test B: Without subscription_id in metadata** (fallback):
```json
{
  "type": "subscription",
  "identity_id": "test-id-5",
  "plan_code": "creator_monthly"
}
```
- Reconciliation queries: `WHERE identity_id = ? AND plan_code = ? AND status IN ('active', 'past_due', 'cancelled') ORDER BY status, created_at DESC`
- Returns most recent active subscription
- Works but less deterministic if user has multiple subscriptions

---

### 3. Refund Reconciliation

#### 3.1 Missing Refund Entry

**Scenario**: Mollie shows refunded payment but credits not revoked.

**Setup**:
```sql
-- Create completed purchase
INSERT INTO timrx_billing.purchases (identity_id, plan_code, credits_granted, status, provider_payment_id)
VALUES ('test-id-1', 'credits_100', 100, 'completed', 'tr_test_refund_001');

-- Create ledger entry
INSERT INTO timrx_billing.ledger_entries (wallet_id, change_credits, reason, reference_type)
SELECT id, 100, 'purchase', 'purchase' FROM timrx_billing.wallets WHERE identity_id = 'test-id-1';

-- Update wallet
UPDATE timrx_billing.wallets SET balance_credits = 100 WHERE identity_id = 'test-id-1';
```

**Mollie State**:
- Payment ID: `tr_test_refund_001`
- Status: `refunded`
- Metadata: `{"type": "one_time", "identity_id": "test-id-1", "credits": 100}`

**Expected Behavior**:
1. Reconciliation detects refunded payment with credits still granted
2. Creates negative ledger entry: -100 credits
3. Updates wallet balance: -100 credits
4. Updates purchase status to 'refunded'

**Verification**:
```sql
-- Check refund ledger entry
SELECT * FROM timrx_billing.ledger_entries
WHERE reference_type = 'refund' AND identity_id = 'test-id-1';

-- Check wallet balance reverted
SELECT balance_credits FROM timrx_billing.wallets WHERE identity_id = 'test-id-1';

-- Check purchase status
SELECT status FROM timrx_billing.purchases WHERE provider_payment_id = 'tr_test_refund_001';
```

---

### 4. Idempotency Tests

#### 4.1 Duplicate Fix Prevention

**Scenario**: Same payment processed multiple times.

**Test**:
1. Run reconciliation with a missing purchase
2. Verify fix is applied
3. Run reconciliation again
4. Verify no duplicate fix is applied (unique constraint)

**Verification**:
```sql
-- Should return exactly 1 row
SELECT COUNT(*) FROM timrx_billing.reconciliation_fixes
WHERE provider_payment_id = 'tr_test_idempotent_001';
```

#### 4.2 Concurrent Reconciliation Runs

**Scenario**: Two reconciliation runs started simultaneously.

**Test**:
1. Start reconciliation run A
2. Start reconciliation run B before A completes
3. Both should complete without errors
4. No duplicate fixes should be created

---

### 5. Dry-Run Mode Tests

#### 5.1 Dry-Run Detection Only

**Test**:
1. Create scenario with missing purchase
2. Run: `POST /api/admin/reconcile/mollie?dry_run=true`
3. Verify response shows issues detected
4. Verify no database changes made

**Verification**:
```sql
-- Should be 0 after dry run
SELECT COUNT(*) FROM timrx_billing.reconciliation_fixes;
```

---

### 6. Error Handling Tests

#### 6.1 Invalid Identity ID

**Scenario**: Mollie payment has non-existent identity_id in metadata.

**Expected**: Error logged, other payments continue processing.

#### 6.2 Mollie API Failure

**Scenario**: Mollie API returns error during fetch.

**Expected**: Run marked as 'failed', error details saved.

#### 6.3 Database Transaction Failure

**Scenario**: DB connection lost mid-reconciliation.

**Expected**: Partial fixes committed (each in own transaction), run can be resumed.

---

### 7. Admin Endpoint Tests

#### 7.1 Manual Trigger

```bash
# Full reconciliation
curl -X POST "https://api.example.com/api/admin/reconcile/full?days=7" \
  -H "Authorization: Bearer $ADMIN_TOKEN"

# Mollie only
curl -X POST "https://api.example.com/api/admin/reconcile/mollie?days=30&dry_run=true" \
  -H "Authorization: Bearer $ADMIN_TOKEN"

# View recent runs
curl "https://api.example.com/api/admin/reconcile/runs?limit=10" \
  -H "Authorization: Bearer $ADMIN_TOKEN"

# View fixes for a run
curl "https://api.example.com/api/admin/reconcile/fixes?run_id=RUN_UUID" \
  -H "Authorization: Bearer $ADMIN_TOKEN"
```

---

### 8. Cron Script Tests

#### 8.1 Script Execution

```bash
# Dry run
cd /path/to/meshy
python scripts/daily_reconciliation.py --dry-run --verbose

# Full run with limited scope
python scripts/daily_reconciliation.py --days 7 --no-alert

# Mollie only
python scripts/daily_reconciliation.py --mollie-only
```

---

## Test Matrix

| Scenario | Payment Type | Mollie Status | DB State | Expected Fix |
|----------|--------------|---------------|----------|--------------|
| Missing purchase | one_time | paid | No purchase | Create purchase + ledger |
| Missing ledger | one_time | paid | Purchase exists | Create ledger only |
| Missing sub credits | subscription | paid | No cycle | Create cycle + ledger |
| Unprocessed refund | one_time | refunded | Credits granted | Revoke credits |
| Chargeback | one_time | charged_back | Credits granted | Revoke credits |
| Already processed | any | paid | Complete | No action |

---

## Monitoring Queries

```sql
-- Recent reconciliation runs
SELECT id, started_at, status, run_type,
       scanned_count, fixed_count, errors_count
FROM timrx_billing.reconciliation_runs
ORDER BY started_at DESC
LIMIT 10;

-- Fixes by type
SELECT fix_type, COUNT(*), SUM(credits_delta) as total_credits
FROM timrx_billing.reconciliation_fixes
GROUP BY fix_type;

-- Failed runs
SELECT * FROM timrx_billing.reconciliation_runs
WHERE status = 'failed'
ORDER BY started_at DESC;

-- Recent fixes
SELECT rf.*, rr.run_type
FROM timrx_billing.reconciliation_fixes rf
JOIN timrx_billing.reconciliation_runs rr ON rf.run_id = rr.id
ORDER BY rf.created_at DESC
LIMIT 20;
```

---

## Rollback Procedure

If reconciliation applies incorrect fixes:

1. Identify the run ID with bad fixes
2. Query fixes applied:
   ```sql
   SELECT * FROM timrx_billing.reconciliation_fixes WHERE run_id = 'BAD_RUN_ID';
   ```
3. Manually revert ledger entries (create opposite entries)
4. Update wallet balances
5. Mark fixes as reverted in notes (don't delete for audit trail)

---

## Sign-off

- [ ] All test scenarios pass
- [ ] Dry-run mode verified
- [ ] Idempotency confirmed
- [ ] Error handling validated
- [ ] Cron script tested
- [ ] Monitoring queries work
- [ ] Rollback procedure documented
