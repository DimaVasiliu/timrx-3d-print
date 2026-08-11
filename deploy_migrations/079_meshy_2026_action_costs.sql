-- Deploy migration 079: Meshy 2026 API action costs

INSERT INTO timrx_billing.action_costs (action_code, cost_credits, provider, updated_at) VALUES
  ('MESHY_TEXT_TO_3D',       20, 'meshy', NOW()),
  ('MESHY_IMAGE_TO_3D',      30, 'meshy', NOW()),
  ('MESHY_REFINE',           10, 'meshy', NOW()),
  ('MESHY_REMESH',            5, 'meshy', NOW()),
  ('MESHY_RETEXTURE',        10, 'meshy', NOW()),
  ('MESHY_CONVERT',           1, 'meshy', NOW()),
  ('MESHY_RESIZE',            1, 'meshy', NOW()),
  ('MESHY_UV_UNWRAP',         5, 'meshy', NOW()),
  ('MESHY_PRINT_ANALYZE',     0, 'meshy', NOW()),
  ('MESHY_PRINT_REPAIR',     10, 'meshy', NOW())
ON CONFLICT (action_code) DO UPDATE SET
  cost_credits = EXCLUDED.cost_credits,
  provider     = EXCLUDED.provider,
  updated_at   = NOW();
