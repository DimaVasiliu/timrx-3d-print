# Video Providers Production Phases

Last reviewed: 2026-06-03

## Phase 1 - Safety and Accuracy

Implemented scope:
- Vertex image-transition jobs now fall back to Seedance 2 and then fal Seedance when Vertex is unavailable or fails at dispatch.
- Workspace and public docs use Seedance 2 "Quality" instead of the old "Preview" label.
- Public duration copy now matches the current workspace maximum of 15 seconds.

Non-code requirements:
- Confirm the production database has current video action-cost migrations through Seedance 2 GA.
- Confirm all video provider secrets are present in production: Vertex service account JSON, Google Cloud project/location, PiAPI key, and fal key if fal is enabled.
- Run one manual production smoke test per enabled provider after deployment.

## Phase 2 - Backend Capability Expansion

Implemented PiAPI Seedance scope:
- Hardened Seedance `omni_reference` / Reference Video routing for mixed image, video, and audio references.
- Added distinct Seedance Reference Video action codes.
- Added server-side input-video surcharge reservation that matches PiAPI billing: output cost plus half-rate input-video duration.
- Aligned frontend fallback credit estimates with Seedance 2 GA tier/resolution pricing and Reference Video surcharge.

Remaining recommended scope:
- Add Veo video extension as a separate endpoint and action code.
- Add provider capability metadata endpoint for video, matching the image-provider catalog pattern.
- Keep fal Seedance unchanged unless it is promoted again.

Non-code requirements:
- Decide which premium capabilities are user-facing versus admin-only: 1080p, audio, video references, less-restriction models, and video extension.
- Apply migration `072_seedance_reference_video_action_costs.sql` before exposing Reference Video in production.
- Approve the Reference Video surcharge policy. Current implementation reserves extra credits for input videos at half the selected output per-second credit rate.
- Check provider terms and safety policy for less-restriction variants before exposing them publicly.

## Phase 3 - Frontend Advanced UX

Implemented scope:
- Added `/video/providers` capability metadata for video provider availability, modes, user-facing names, Reference-Guided limits, and retention windows.
- Public Reference-Guided Video is image-reference first: up to 6 images, 10 MB each, 8 total public references, 75 MB total payload.
- Video references, audio references, and 1080p Reference-Guided output are private beta/admin-gated by default.
- Reference input retention is 24 hours for public jobs, failed-job temporary inputs are marked for 6 hours, and beta/admin jobs are marked for 7 days.
- Frontend labels use product names: Cinematic, Fast / Quality, Animate, Transition, Reference-Guided, Fast, and Quality.
- Frontend Reference-Guided validation now follows provider capabilities and limits.

Remaining recommended scope:
- Add Veo extension UI after the backend endpoint and action code exist.
- Replace any remaining hardcoded provider tables with the `/video/providers` catalog where practical.
- Add richer failure/retry messaging for provider fallback, refund state, and retry provider choice.

Non-code requirements:
- Leave `VIDEO_REFERENCE_VIDEO_REFS_PUBLIC=false`, `VIDEO_REFERENCE_AUDIO_REFS_PUBLIC=false`, and `VIDEO_REFERENCE_1080P_PUBLIC=false` for initial launch.
- Enable video/audio reference toggles only after live PiAPI smoke tests and cost review.
- Update support docs and customer-facing FAQ before public launch.

## Phase 4 - Production Verification

Implemented scope:
- Added `/video/admin/production-readiness` as a no-spend admin readiness report for config, provider availability, Reference-Guided action-cost rows, public gate safety, and the required smoke matrix.
- Added `VIDEO_PROVIDERS_PHASE4_RUNBOOK.md` with the exact no-spend checks, live smoke matrix, negative tests, billing checks, frontend QA, launch decision, and rollback steps.

Required live checks:
- Text-to-video, image-to-video, and image-transition smoke tests for every enabled provider.
- Seedance Reference-Guided smoke test with image references.
- Credit reservation, refund-on-failure, provider fallback, history card, thumbnail, download, and community share checks.
- Quota-exhaustion test in staging or via mocked provider response.
- Provider-cost dashboard review after the first real production jobs.

Rollback plan:
- Disable a provider by removing its secret or using the existing provider selection controls.
- Keep legacy provider aliases active until in-flight jobs from old deployments have completed.
