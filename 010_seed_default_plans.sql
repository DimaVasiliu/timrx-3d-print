-- Migration: Seed default credit plans
-- Uses credit_grant column for the number of credits to grant on purchase
-- Safe to run multiple times (idempotent via ON CONFLICT)

BEGIN;

-- Seed starter_80 plan (80 credits @ £7.99)
INSERT INTO timrx_billing.plans
    (code, name, description, price_gbp, currency, credit_grant, includes_priority, is_active, created_at)
VALUES
    ('starter_80', 'Starter', 'Try the tools. Great for a few generations.', 7.99, 'GBP', 80, FALSE, TRUE, NOW())
ON CONFLICT (code) DO UPDATE SET
    name = EXCLUDED.name,
    description = EXCLUDED.description,
    price_gbp = EXCLUDED.price_gbp,
    credit_grant = EXCLUDED.credit_grant,
    includes_priority = EXCLUDED.includes_priority,
    is_active = TRUE;

-- Seed creator_300 plan (300 credits @ £19.99)
INSERT INTO timrx_billing.plans
    (code, name, description, price_gbp, currency, credit_grant, includes_priority, is_active, created_at)
VALUES
    ('creator_300', 'Creator', 'Regular use. Better value bundle.', 19.99, 'GBP', 300, FALSE, TRUE, NOW())
ON CONFLICT (code) DO UPDATE SET
    name = EXCLUDED.name,
    description = EXCLUDED.description,
    price_gbp = EXCLUDED.price_gbp,
    credit_grant = EXCLUDED.credit_grant,
    includes_priority = EXCLUDED.includes_priority,
    is_active = TRUE;

-- Seed studio_600 plan (600 credits @ £34.99, includes priority queue)
INSERT INTO timrx_billing.plans
    (code, name, description, price_gbp, currency, credit_grant, includes_priority, is_active, created_at)
VALUES
    ('studio_600', 'Studio', 'Heavy use. Best value. Priority queue access.', 34.99, 'GBP', 600, TRUE, TRUE, NOW())
ON CONFLICT (code) DO UPDATE SET
    name = EXCLUDED.name,
    description = EXCLUDED.description,
    price_gbp = EXCLUDED.price_gbp,
    credit_grant = EXCLUDED.credit_grant,
    includes_priority = EXCLUDED.includes_priority,
    is_active = TRUE;

COMMIT;
