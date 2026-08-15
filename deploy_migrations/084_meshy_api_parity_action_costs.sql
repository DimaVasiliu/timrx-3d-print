-- Deploy migration 084: Meshy API parity action costs (consolidated assert)
--
-- Restates all thirteen canonical Meshy base prices in one place. Pure upsert:
-- safe to run repeatedly and safe when the rows are already correct.
-- Provider-tier surcharges (8K texture +5, Ultra geometry +5) are applied at
-- reservation time in code, not stored here.

INSERT INTO timrx_billing.action_costs (action_code, cost_credits, provider, updated_at) VALUES
  ('MESHY_TEXT_TO_3D',         20, 'meshy', NOW()),
  ('MESHY_IMAGE_TO_3D',        30, 'meshy', NOW()),
  ('MESHY_REFINE',             10, 'meshy', NOW()),
  ('MESHY_RETEXTURE',          10, 'meshy', NOW()),
  ('MESHY_REMESH',              5, 'meshy', NOW()),
  ('MESHY_CONVERT',             1, 'meshy', NOW()),
  ('MESHY_RESIZE',              1, 'meshy', NOW()),
  ('MESHY_UV_UNWRAP',           5, 'meshy', NOW()),
  ('MESHY_PRINT_ANALYZE',       0, 'meshy', NOW()),
  ('MESHY_PRINT_REPAIR',       10, 'meshy', NOW()),
  ('MESHY_MULTI_COLOR_PRINT',  10, 'meshy', NOW()),
  ('MESHY_RIGGING',             5, 'meshy', NOW()),
  ('MESHY_ANIMATION',           3, 'meshy', NOW())
ON CONFLICT (action_code) DO UPDATE SET
  cost_credits = EXCLUDED.cost_credits,
  provider     = EXCLUDED.provider,
  updated_at   = NOW();

-- Verify:
--   SELECT action_code, cost_credits FROM timrx_billing.action_costs
--   WHERE action_code LIKE 'MESHY_%' ORDER BY action_code;
