-- Migration 082: Seedance 2.5 long durations (20/25/30s)
--
-- Migration 081 raised the 2.5 duration ceiling to 30s but only priced
-- 5/10/15s. jobs.action_code and credit_reservations.action_code both have a
-- FOREIGN KEY to action_costs(action_code), so a 20/25/30s job fails the
-- reservation INSERT with an integrity error even though the per-second
-- fallback computes the price correctly. These rows close that gap.
--
-- Rates (matches 081): 480p 18 credits/s, 720p 42 credits/s.

INSERT INTO timrx_billing.action_costs (action_code, cost_credits, provider, updated_at) VALUES
  ('seedance_v25_text_generate_20s_480p',     360,  'seedance', NOW()),
  ('seedance_v25_text_generate_25s_480p',     450,  'seedance', NOW()),
  ('seedance_v25_text_generate_30s_480p',     540,  'seedance', NOW()),
  ('seedance_v25_text_generate_20s_720p',     840,  'seedance', NOW()),
  ('seedance_v25_text_generate_25s_720p',     1050, 'seedance', NOW()),
  ('seedance_v25_text_generate_30s_720p',     1260, 'seedance', NOW()),
  ('seedance_v25_image_animate_20s_480p',     360,  'seedance', NOW()),
  ('seedance_v25_image_animate_25s_480p',     450,  'seedance', NOW()),
  ('seedance_v25_image_animate_30s_480p',     540,  'seedance', NOW()),
  ('seedance_v25_image_animate_20s_720p',     840,  'seedance', NOW()),
  ('seedance_v25_image_animate_25s_720p',     1050, 'seedance', NOW()),
  ('seedance_v25_image_animate_30s_720p',     1260, 'seedance', NOW()),
  ('seedance_v25_image_transition_20s_480p',  360,  'seedance', NOW()),
  ('seedance_v25_image_transition_25s_480p',  450,  'seedance', NOW()),
  ('seedance_v25_image_transition_30s_480p',  540,  'seedance', NOW()),
  ('seedance_v25_image_transition_20s_720p',  840,  'seedance', NOW()),
  ('seedance_v25_image_transition_25s_720p',  1050, 'seedance', NOW()),
  ('seedance_v25_image_transition_30s_720p',  1260, 'seedance', NOW()),
  ('seedance_v25_reference_video_20s_480p',   360,  'seedance', NOW()),
  ('seedance_v25_reference_video_25s_480p',   450,  'seedance', NOW()),
  ('seedance_v25_reference_video_30s_480p',   540,  'seedance', NOW()),
  ('seedance_v25_reference_video_20s_720p',   840,  'seedance', NOW()),
  ('seedance_v25_reference_video_25s_720p',   1050, 'seedance', NOW()),
  ('seedance_v25_reference_video_30s_720p',   1260, 'seedance', NOW())
ON CONFLICT (action_code) DO UPDATE SET
  cost_credits = EXCLUDED.cost_credits,
  provider     = EXCLUDED.provider,
  updated_at   = NOW();

-- Verify:
--   SELECT action_code, cost_credits FROM timrx_billing.action_costs
--   WHERE action_code LIKE 'seedance_v25_%'
--   ORDER BY action_code;
