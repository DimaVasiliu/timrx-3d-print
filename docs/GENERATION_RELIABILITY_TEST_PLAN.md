# Generation Reliability Layer - Test Plan

This document outlines the test cases for verifying the generation reliability layer implementation.

## Overview

The generation reliability layer ensures that any generation in the app (text→3D, image→3D, text→image, image→video, etc.) never breaks if the user reloads, navigates away, closes the tab, or accidentally clicks another page.

## Covered Generation Types

| Type | Provider | Idempotency | Recovery |
|------|----------|-------------|----------|
| Text → 3D | Meshy | ✅ | ✅ |
| Image → 3D (upload) | Meshy | ✅ | ✅ |
| Image → 3D (history) | Meshy | ✅ | ✅ |
| Refine | Meshy | ✅ | ✅ |
| Remesh | Meshy | ✅ | ✅ |
| Texture | Meshy | ✅ | ✅ |
| Rig | Meshy | ✅ | ✅ |
| Text → Image (OpenAI) | OpenAI | ✅ | ✅ |
| Text → Image (Gemini) | Google | ✅ | ✅ |
| Text → Video | Gemini Veo | ✅ | ✅ |
| Image → Video | Gemini Veo | ✅ | ✅ |

## Prerequisites

1. Run the database migration: `017_jobs_idempotency.sql`
2. Ensure backend is running with the updated code
3. Have a test user with credits available

---

## Test Cases

### A. Idempotency - No Duplicate Jobs

#### A1. Double-click prevention
**Steps:**
1. Open the generation page
2. Enter a prompt
3. Rapidly double-click the "Generate" button

**Expected:**
- Only ONE job is created (check DB: `SELECT * FROM timrx_billing.jobs ORDER BY created_at DESC LIMIT 5`)
- Second click returns the same job_id as the first
- Only one credit reservation is made

#### A2. Page refresh during generation
**Steps:**
1. Start a text-to-3D generation
2. Note the job_id from console/network tab
3. Immediately refresh the page (F5) while still generating
4. Click "Generate" with the same prompt

**Expected:**
- The existing job is returned (was_existing: true in response)
- No new job is created
- Progress continues from where it was

#### A3. Network retry handling
**Steps:**
1. Throttle network in DevTools (Slow 3G)
2. Start a generation
3. Let the first request timeout/fail
4. Network auto-retries

**Expected:**
- Same idempotency key is sent
- Only one job exists in DB
- No duplicate charges

---

### B. Job Persistence - Survives Page Events

#### B1. Refresh during generation
**Steps:**
1. Start a text-to-3D generation
2. Wait until status shows "Processing" (progress > 0)
3. Refresh the page

**Expected:**
- Job continues on server (check `/api/jobs/active`)
- After page loads, job reappears in UI
- Progress indicator shows current progress
- Job completes successfully

#### B2. Navigate away and back
**Steps:**
1. Start a video generation
2. Navigate to a different page (e.g., /pricing)
3. Wait 30 seconds
4. Navigate back to the generation page

**Expected:**
- Job indicator shows active job
- Clicking indicator shows job in panel
- Job status is current (not stale)
- Job completes and appears in history

#### B3. Close tab and reopen
**Steps:**
1. Start an image generation
2. Close the browser tab (confirm warning dialog)
3. Reopen the same page in a new tab

**Expected:**
- `/api/jobs/active` returns the running job
- Job reattaches to UI
- Progress continues
- Final result appears in history

---

### C. Credit Safety - No Double Charges

#### C1. Single charge on success
**Steps:**
1. Check initial credit balance
2. Start a generation (cost: 20 credits for text-to-3D)
3. Let it complete successfully
4. Check final credit balance

**Expected:**
- Credits reduced by exactly 20
- One ledger entry for this job
- Reservation status = 'finalized'

#### C2. Refund on failure
**Steps:**
1. Check initial credit balance
2. Start a generation with invalid params (to force failure)
3. Wait for failure

**Expected:**
- Credits returned to original balance
- Reservation status = 'released'
- No charge in ledger

#### C3. Refresh doesn't double-charge
**Steps:**
1. Check initial credit balance
2. Start a generation
3. Refresh page 3 times during generation
4. Let it complete

**Expected:**
- Only ONE reservation in DB for this job
- Only ONE ledger entry on completion
- Credits reduced by exact cost once

---

### D. Backend Restart Recovery

#### D1. Job survives backend restart
**Steps:**
1. Start a text-to-3D generation
2. Note the job_id
3. Restart the backend server
4. Check job status

**Expected:**
- Job record exists in DB with status = 'pending' or 'processing'
- `upstream_job_id` is set
- Calling GET `/api/jobs/<job_id>` returns current status
- Job can be polled and completes

#### D2. Stale job recovery
**Steps:**
1. Start a generation
2. Stop the backend (simulate crash)
3. Wait 15 minutes (let job become stale)
4. Restart backend
5. Call `JobService.get_stale_jobs(stale_minutes=10)`

**Expected:**
- Stale job is returned
- Can poll provider for actual status
- Job status is updated correctly

---

### E. Before Unload Warning

#### E1. Warning shown with active jobs
**Steps:**
1. Start a generation
2. Try to close the tab

**Expected:**
- Browser shows confirmation dialog
- Message mentions "generation in progress"
- User can choose to stay or leave

#### E2. No warning without active jobs
**Steps:**
1. Ensure no generations running
2. Try to close the tab

**Expected:**
- Tab closes immediately
- No warning dialog

---

### F. Jobs Indicator UI

#### F1. Indicator appears when jobs active
**Steps:**
1. Start a generation
2. Look at bottom-left corner

**Expected:**
- Purple pulse indicator appears
- Shows count of active jobs
- "in progress" label visible

#### F2. Indicator click shows panel
**Steps:**
1. With active jobs, click the indicator

**Expected:**
- Modal panel opens
- Lists all active jobs
- Shows progress for each
- "Got it" button closes panel

#### F3. Indicator hides when no jobs
**Steps:**
1. Start a generation
2. Wait for completion
3. Check indicator

**Expected:**
- Indicator disappears when count = 0
- No visual artifact left

---

### G. Active Jobs Recovery Endpoint

#### G1. Returns running jobs
**Steps:**
1. Start 2 generations
2. Call GET `/api/jobs/active`

**Expected:**
```json
{
  "ok": true,
  "jobs": [
    {"job_id": "...", "status": "processing", "progress": 45, ...},
    {"job_id": "...", "status": "queued", "progress": 0, ...}
  ]
}
```

#### G2. Excludes completed jobs
**Steps:**
1. Complete a generation
2. Call GET `/api/jobs/active`

**Expected:**
- Completed job NOT in list
- Only queued/pending/processing returned

---

## Database Verification Queries

```sql
-- Check idempotency constraint
SELECT idempotency_key, COUNT(*)
FROM timrx_billing.jobs
WHERE idempotency_key IS NOT NULL
GROUP BY idempotency_key
HAVING COUNT(*) > 1;
-- Should return 0 rows

-- Check active jobs for a user
SELECT id, status, progress, created_at, updated_at
FROM timrx_billing.jobs
WHERE identity_id = '<user_id>'
  AND status IN ('queued', 'pending', 'processing')
ORDER BY created_at DESC;

-- Check stale jobs
SELECT id, status, upstream_job_id, updated_at,
       NOW() - updated_at AS stale_duration
FROM timrx_billing.jobs
WHERE status IN ('pending', 'processing')
  AND updated_at < NOW() - INTERVAL '10 minutes';

-- Check credit reservation for a job
SELECT j.id AS job_id, j.status AS job_status,
       r.id AS reservation_id, r.status AS reservation_status,
       r.credits_amount
FROM timrx_billing.jobs j
LEFT JOIN timrx_billing.credit_reservations r ON j.reservation_id = r.id
WHERE j.id = '<job_id>';
```

---

## Checklist

### Core Reliability
- [ ] A1: Double-click prevention works
- [ ] A2: Page refresh doesn't create duplicate job
- [ ] A3: Network retries handled correctly
- [ ] B1: Job survives page refresh
- [ ] B2: Job survives navigation
- [ ] B3: Job survives tab close/reopen

### Credits Safety
- [ ] C1: Single charge on success
- [ ] C2: Refund on failure
- [ ] C3: Refresh doesn't double-charge

### Backend Recovery
- [ ] D1: Job survives backend restart
- [ ] D2: Stale jobs recoverable

### UI Features
- [ ] E1: Before unload warning shown
- [ ] E2: No warning when no jobs
- [ ] F1: Jobs indicator appears
- [ ] F2: Jobs panel works
- [ ] F3: Indicator hides when done
- [ ] G1: Active jobs endpoint returns running
- [ ] G2: Active jobs excludes completed

### 3D Model Generation
- [ ] H1: Batch text-to-3D idempotency
- [ ] I1: Image-to-3D from upload idempotency
- [ ] I2: Image-to-3D from history idempotency
- [ ] J1: Remesh idempotency
- [ ] J2: Texture idempotency
- [ ] J3: Rig idempotency

### Image Generation
- [ ] K1: OpenAI image idempotency
- [ ] K2: Gemini image idempotency

### Video Generation
- [ ] L1: Text-to-video idempotency
- [ ] L2: Image-to-video idempotency

---

## Additional Test Cases by Generation Type

### H. Text-to-3D Specific

#### H1. Batch generation idempotency
**Steps:**
1. Start a batch of 3 text-to-3D generations
2. Refresh page immediately

**Expected:**
- Only 3 jobs created total
- All 3 reappear after refresh

### I. Image-to-3D Specific

#### I1. From upload idempotency
**Steps:**
1. Upload an image
2. Click "Generate 3D" twice quickly

**Expected:**
- Only one job created
- Second click returns existing job

#### I2. From history idempotency
**Steps:**
1. Select an image from history
2. Click "Convert to 3D"
3. Immediately click again

**Expected:**
- Single job created

### J. Mesh Operations (Remesh/Texture/Rig)

#### J1. Remesh idempotency
**Steps:**
1. Start remesh operation
2. Refresh page during processing

**Expected:**
- Remesh continues
- Recoverable on page load

#### J2. Texture idempotency
**Steps:**
1. Start texture operation
2. Navigate away and back

**Expected:**
- Texture job still running
- Progress visible

#### J3. Rig idempotency
**Steps:**
1. Start rig operation
2. Close and reopen tab

**Expected:**
- Rig job recoverable

### K. Image Generation (OpenAI & Gemini)

#### K1. OpenAI image idempotency
**Steps:**
1. Set provider to OpenAI
2. Start image generation
3. Quickly click generate again

**Expected:**
- Single job created
- Duplicate blocked by idempotency key

#### K2. Gemini image idempotency
**Steps:**
1. Set provider to Google/Gemini
2. Start image generation
3. Refresh during "Generating..."

**Expected:**
- Job continues
- Recoverable

### L. Video Generation

#### L1. Text-to-video idempotency
**Steps:**
1. Start text-to-video generation
2. Double-click the generate button

**Expected:**
- Single video job
- No duplicate charge

#### L2. Image-to-video idempotency
**Steps:**
1. Upload image for video
2. Start image-to-video
3. Refresh page

**Expected:**
- Video generation continues
- Progress visible after reload

---

## Known Limitations

1. **Synchronous API calls**: Some operations (like the initial API request) can't be recovered if the page refreshes during the HTTP call itself - but credits are protected via reservation

2. **Idempotency window**: Idempotency keys are kept for ~24 hours. Very old retries may create new jobs.

3. **Provider webhooks**: Not all providers support webhooks. Polling is used as fallback.

4. **Provider-specific behavior**: Each provider (Meshy, OpenAI, Gemini, Runway) has different polling intervals and timeout characteristics

---

## Rollback Plan

If issues are found:

1. Disable idempotency by removing `Idempotency-Key` header from frontend
2. Jobs will still be created but without duplicate prevention
3. Credit reservations still protect against double-charges
4. Migration can be rolled back: `DROP INDEX uq_jobs_identity_idempotency`
