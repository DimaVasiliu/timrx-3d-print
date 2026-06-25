# Video Providers Phase 4 Runbook

Last reviewed: 2026-06-04

## Purpose

Phase 4 verifies that video generation is production-ready after the provider and Reference-Guided changes. It covers configuration, migrations, billing, provider smoke tests, storage retention, frontend QA, and rollback.

## Before Deploy

1. Apply database migrations through:
   - `068_seedance_2ga_resolution_costs.sql`
   - `069_seedance_1080p_margin_protection.sql`
   - `072_seedance_reference_video_action_costs.sql`

2. Keep advanced Reference-Guided features private for initial launch:
   - `VIDEO_REFERENCE_VIDEO_REFS_PUBLIC=false`
   - `VIDEO_REFERENCE_AUDIO_REFS_PUBLIC=false`
   - `VIDEO_REFERENCE_1080P_PUBLIC=false`
   - `VIDEO_AUDIO_OUTPUT_PUBLIC=false`

3. Confirm required secrets/config are present:
   - Vertex service account/project/location/model config
   - `PIAPI_API_KEY`
   - S3 bucket and credentials
   - Admin auth: `ADMIN_TOKEN` or `ADMIN_EMAILS`

4. Configure storage lifecycle cleanup:
   - Public Reference-Guided inputs: delete after 24 hours
   - Failed-job temporary inputs: delete after 6 hours
   - Beta/admin Reference-Guided inputs: delete after 7 days

Current Reference-Guided data-URI uploads use the `video-input/{image|video|audio}` S3 prefix. The existing lifecycle script covers that prefix with 1-day deletion:

```bash
cd Backend/meshy
python scripts/setup_s3_lifecycle.py          # dry run
python scripts/setup_s3_lifecycle.py --apply  # apply
```

The app records retention windows in job metadata; object deletion must be enforced by the storage lifecycle or cleanup job. The existing `video-input/` lifecycle rule enforces the 24-hour public retention. Exact 6-hour failed-input cleanup and 7-day beta/admin retention require separate prefixes or a metadata-aware cleanup job if you need those windows enforced precisely.

## No-Spend Checks

Run these first. They should not call providers or reserve credits.

```bash
curl -sS -H "X-Admin-Token: $ADMIN_TOKEN" \
  "$PUBLIC_BASE_URL/api/video/admin/production-readiness"
```

Expected:
- `ok: true`
- all required provider configs marked configured
- Seedance Reference-Guided action costs count is `15/15`
- public gates are still false

```bash
curl -sS "$PUBLIC_BASE_URL/api/video/providers"
```

Expected:
- `seedance.reference_guided.image_refs: true`
- `seedance.reference_guided.video_refs: false`
- `seedance.reference_guided.audio_refs: false`
- `seedance.reference_guided.quality_1080p: false`

```bash
curl -sS -H "X-Admin-Token: $ADMIN_TOKEN" \
  "$PUBLIC_BASE_URL/api/video/admin/diagnostics"
```

Expected:
- endpoint returns current provider health, in-flight jobs, and recent failures
- no unexplained stuck jobs before launch

## Live Smoke Matrix

These tests spend provider/API credits. Use a test account with enough video credits and record the returned job IDs.

1. Cinematic text-to-video
2. Cinematic animate image
3. Cinematic image transition
4. Seedance Fast text-to-video
5. Seedance Quality text-to-video
6. Seedance animate image
7. Seedance image transition
8. Seedance Reference-Guided with image references

For each job, verify:
- credits reserve before dispatch
- status reaches `done`
- video URL is present and playable
- thumbnail/history card appears
- reservation finalizes exactly once
- `estimated_provider_cost_gbp` is stamped on the job row
- no unexpected provider fallback occurred unless the test was designed for fallback

## Negative Tests

Run these before enabling video/audio Reference-Guided publicly.

1. Public Reference-Guided with a video reference
   - Expected: `403 feature_not_enabled`

2. Public Reference-Guided with an audio reference
   - Expected: `403 feature_not_enabled`

3. Public Reference-Guided at 1080p
   - Expected: `403 feature_not_enabled`

4. Public Reference-Guided with more than 6 image refs
   - Expected: `400 invalid_params`

5. Public Reference-Guided with image over 10 MB
   - Expected: `400 invalid_params`

6. Failed provider dispatch
   - Expected: credits are released, job is failed, user can retry

## Billing Checks

For each successful smoke job:
- `credit_reservations.status` transitions from held to finalized
- wallet balance decreases by the exact expected video credits
- no duplicate ledger debit exists for the same reservation
- `jobs.meta.expected_cost` matches reserved credits
- Reference-Guided image-only jobs do not include input-video surcharge

For a beta Reference-Guided video-ref test:
- `jobs.meta.input_video_seconds` is present
- reserved credits include half-rate input-video surcharge
- provider cost estimate includes PiAPI input-video surcharge

## Frontend QA

Check desktop and mobile:
- provider labels: Cinematic, Fast / Quality, Legacy
- modes: Text, Animate, Reference-Guided
- Seedance Reference-Guided shows image refs publicly
- video/audio ref buttons are disabled unless beta/admin enabled
- 1080p Reference-Guided is hidden/disabled unless beta/admin enabled
- cost estimate matches backend `credits_reserved`
- upload limit errors are clear
- history, download, and share still work

## Launch Decision

Launch only when:
- readiness endpoint returns `ok: true`
- all live smoke jobs pass
- negative tests pass
- S3 lifecycle cleanup is configured
- support docs/FAQ are updated

## Rollback

If production issues appear:
- Set `VIDEO_PROVIDER=seedance` or `VIDEO_PROVIDER=vertex` to force the stable provider.
- Disable advanced Reference-Guided flags:
  - `VIDEO_REFERENCE_VIDEO_REFS_PUBLIC=false`
  - `VIDEO_REFERENCE_AUDIO_REFS_PUBLIC=false`
  - `VIDEO_REFERENCE_1080P_PUBLIC=false`
- Remove/disable the broken provider secret to make provider routing ignore it.
- Use `/api/video/admin/diagnostics` and `/api/video/admin/process-queue` to inspect and recover queued jobs.
