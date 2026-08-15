-- Deploy migration 085: Meshy Creative Lab + Meshy-native image action costs
--
-- Stage 7 of MESHY_API_PARITY_IMPLEMENTATION_PLAN.md (7.2 Creative Lab, 7.3
-- Meshy Text-to-Image / Image-to-Image).
--
-- PRICING BASIS: TimrX prices Meshy tasks 1:1 with Meshy's own credit costs.
-- That is the convention every existing Meshy row already follows (remesh 5=5,
-- convert 1=1, refine 10=10, retexture 10=10, rigging 5=5, animation 3=3), so
-- these rows use Meshy's published Creative Lab prices unchanged:
--   prototype 6 credits, build 30 credits; Keycap 12 / 50.
--   (https://docs.meshy.ai/en/api/pricing, checked 2026-08-15)
-- Meshy's individual product pages quote a lower build figure (20) than the
-- pricing table (30); the higher figure is used so a price change on Meshy's
-- side cannot put TimrX underwater. Adjust here or in admin if you want a
-- different markup.
--
-- Image generation is priced at the top of Meshy's per-model range so any
-- model choice stays covered: text-to-image 9 (nano-banana 3 … gpt-image-2 9),
-- image-to-image 12 (… gpt-image-2 12).

INSERT INTO timrx_billing.action_costs (action_code, cost_credits, provider, updated_at) VALUES
  ('MESHY_CL_KEYCHAIN_PROTOTYPE',       6, 'meshy', NOW()),   -- Keychain prototype
  ('MESHY_CL_KEYCHAIN_BUILD',          30, 'meshy', NOW()),   -- Keychain build
  ('MESHY_CL_FRIDGE_MAGNET_PROTOTYPE',  6, 'meshy', NOW()),   -- Fridge Magnet prototype
  ('MESHY_CL_FRIDGE_MAGNET_BUILD',     30, 'meshy', NOW()),   -- Fridge Magnet build
  ('MESHY_CL_FIGURE_PROTOTYPE',         6, 'meshy', NOW()),   -- Figure prototype
  ('MESHY_CL_FIGURE_BUILD',            30, 'meshy', NOW()),   -- Figure build
  ('MESHY_CL_VINYL_FIGURE_PROTOTYPE',   6, 'meshy', NOW()),   -- Vinyl Figure prototype
  ('MESHY_CL_VINYL_FIGURE_BUILD',      30, 'meshy', NOW()),   -- Vinyl Figure build
  ('MESHY_CL_BRICK_FIGURE_PROTOTYPE',   6, 'meshy', NOW()),   -- Brick Figure prototype
  ('MESHY_CL_BRICK_FIGURE_BUILD',      30, 'meshy', NOW()),   -- Brick Figure build
  ('MESHY_CL_LAMP_PROTOTYPE',           6, 'meshy', NOW()),   -- Lamp prototype
  ('MESHY_CL_LAMP_BUILD',              30, 'meshy', NOW()),   -- Lamp build
  ('MESHY_CL_KEYCAP_PROTOTYPE',        12, 'meshy', NOW()),   -- Keycap prototype
  ('MESHY_CL_KEYCAP_BUILD',            50, 'meshy', NOW()),   -- Keycap build
  ('MESHY_TEXT_TO_IMAGE',           9, 'meshy', NOW()),
  ('MESHY_IMAGE_TO_IMAGE',         12, 'meshy', NOW())
ON CONFLICT (action_code) DO UPDATE SET
  cost_credits = EXCLUDED.cost_credits,
  provider     = EXCLUDED.provider,
  updated_at   = NOW();

-- Verify:
--   SELECT action_code, cost_credits FROM timrx_billing.action_costs
--   WHERE action_code LIKE 'MESHY_CL_%' OR action_code IN ('MESHY_TEXT_TO_IMAGE','MESHY_IMAGE_TO_IMAGE')
--   ORDER BY action_code;
-- Expect 16 rows.
