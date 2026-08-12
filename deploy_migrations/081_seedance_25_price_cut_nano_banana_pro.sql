-- Migration 081: Seedance 2.5 GA price cut + Nano Banana Pro
--
-- 1) PiAPI cut the Seedance 2.5 list price ~50% at GA (Aug 2026):
--    $0.30/s -> $0.15/s at 480p, $0.60/s -> $0.35/s at 720p.
--    User credit prices follow at the same 120 credits-per-$/s ratio
--    (mirrors pricing_service.SEEDANCE_CREDIT_COSTS["v25"]).
--
-- 2) Adds PiAPI Nano Banana Pro (gemini/nano-banana-pro) image actions:
--    list $0.105/img at 1K & 2K, $0.18/img at 4K -> 16c / 16c / 27c on the
--    same ~187 credits/$ ratio as the existing Nano Banana 2K/4K rows.
--
-- Note: the Aug 2026 mini/fast promo (ends 2026-09-07 06:00 UTC) is
-- deliberately NOT passed through to credit prices — it is tracked on the
-- provider-cost side only (backend/services/provider_costs.py).

INSERT INTO timrx_billing.action_costs (action_code, cost_credits, provider, updated_at) VALUES
  -- Seedance 2.5 — halved (480p: 18 c/s, 720p: 42 c/s)
  ('seedance_v25_text_generate_5s_480p',     90,  'seedance', NOW()),
  ('seedance_v25_text_generate_10s_480p',    180, 'seedance', NOW()),
  ('seedance_v25_text_generate_15s_480p',    270, 'seedance', NOW()),
  ('seedance_v25_text_generate_5s_720p',     210, 'seedance', NOW()),
  ('seedance_v25_text_generate_10s_720p',    420, 'seedance', NOW()),
  ('seedance_v25_text_generate_15s_720p',    630, 'seedance', NOW()),
  ('seedance_v25_image_animate_5s_480p',     90,  'seedance', NOW()),
  ('seedance_v25_image_animate_10s_480p',    180, 'seedance', NOW()),
  ('seedance_v25_image_animate_15s_480p',    270, 'seedance', NOW()),
  ('seedance_v25_image_animate_5s_720p',     210, 'seedance', NOW()),
  ('seedance_v25_image_animate_10s_720p',    420, 'seedance', NOW()),
  ('seedance_v25_image_animate_15s_720p',    630, 'seedance', NOW()),
  ('seedance_v25_image_transition_5s_480p',  90,  'seedance', NOW()),
  ('seedance_v25_image_transition_10s_480p', 180, 'seedance', NOW()),
  ('seedance_v25_image_transition_15s_480p', 270, 'seedance', NOW()),
  ('seedance_v25_image_transition_5s_720p',  210, 'seedance', NOW()),
  ('seedance_v25_image_transition_10s_720p', 420, 'seedance', NOW()),
  ('seedance_v25_image_transition_15s_720p', 630, 'seedance', NOW()),
  ('seedance_v25_reference_video_5s_480p',   90,  'seedance', NOW()),
  ('seedance_v25_reference_video_10s_480p',  180, 'seedance', NOW()),
  ('seedance_v25_reference_video_15s_480p',  270, 'seedance', NOW()),
  ('seedance_v25_reference_video_5s_720p',   210, 'seedance', NOW()),
  ('seedance_v25_reference_video_10s_720p',  420, 'seedance', NOW()),
  ('seedance_v25_reference_video_15s_720p',  630, 'seedance', NOW()),
  -- Nano Banana Pro (new)
  ('PIAPI_PRO_IMAGE',    16, 'nano_banana_pro', NOW()),
  ('PIAPI_PRO_IMAGE_2K', 16, 'nano_banana_pro', NOW()),
  ('PIAPI_PRO_IMAGE_4K', 27, 'nano_banana_pro', NOW())
ON CONFLICT (action_code) DO UPDATE SET
  cost_credits = EXCLUDED.cost_credits,
  provider     = EXCLUDED.provider,
  updated_at   = NOW();

-- Verify:
--   SELECT action_code, cost_credits FROM timrx_billing.action_costs
--   WHERE action_code LIKE 'seedance_v25_%' OR action_code LIKE 'PIAPI_PRO_%'
--   ORDER BY action_code;
