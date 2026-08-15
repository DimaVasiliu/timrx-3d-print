"""Meshy pricing parity contract (Stage 6 of the API parity plan).

Locks three things that silently drift apart otherwise:
  1. every Meshy action code TimrX productizes has the agreed base price,
  2. every route's ACTION_KEYS entry resolves to that code,
  3. the provider-tier surcharge rule the wallet charges is the same one the
     UI displays.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import flask
import pytest

from backend.config import ACTION_KEYS
from backend.services.meshy_service import MESHY_SURCHARGES, expected_meshy_platform_cost
from backend.services.pricing_service import (
    CANONICAL_TO_DB,
    DEFAULT_ACTION_COSTS,
    PricingService,
    normalize_action_key,
)

DB_TO_CANONICAL = PricingService.DB_TO_CANONICAL

MESHY_ROOT = Path(__file__).resolve().parents[2]

# The canonical price list from MESHY_API_PARITY_IMPLEMENTATION_PLAN.md, Stage 6.
EXPECTED_COSTS = {
    "MESHY_TEXT_TO_3D": 20,
    "MESHY_IMAGE_TO_3D": 30,
    "MESHY_REFINE": 10,
    "MESHY_REMESH": 5,
    "MESHY_RETEXTURE": 10,
    "MESHY_CONVERT": 1,
    "MESHY_RESIZE": 1,
    "MESHY_UV_UNWRAP": 5,
    "MESHY_PRINT_ANALYZE": 0,
    "MESHY_PRINT_REPAIR": 10,
    "MESHY_MULTI_COLOR_PRINT": 10,
    "MESHY_RIGGING": 5,
    "MESHY_ANIMATION": 3,
}

# Route-level action key -> DB action code.
ROUTE_ACTION_KEYS = {
    "text-to-3d-preview": "MESHY_TEXT_TO_3D",
    "text-to-3d-refine": "MESHY_REFINE",
    "image-to-3d": "MESHY_IMAGE_TO_3D",
    "remesh": "MESHY_REMESH",
    "retexture": "MESHY_RETEXTURE",
    "convert": "MESHY_CONVERT",
    "resize": "MESHY_RESIZE",
    "uv-unwrap": "MESHY_UV_UNWRAP",
    "print-analyze": "MESHY_PRINT_ANALYZE",
    "print-repair": "MESHY_PRINT_REPAIR",
    "rigging": "MESHY_RIGGING",
    "animation": "MESHY_ANIMATION",
}

SEEDED = {row["action_code"]: row["cost_credits"] for row in DEFAULT_ACTION_COSTS}


@pytest.mark.parametrize("action_code,expected", sorted(EXPECTED_COSTS.items()))
def test_seeded_defaults_match_the_agreed_price(action_code, expected):
    assert SEEDED.get(action_code) == expected


@pytest.mark.parametrize("route_key,db_code", sorted(ROUTE_ACTION_KEYS.items()))
def test_route_action_key_resolves_to_the_right_db_code(route_key, db_code):
    assert route_key in ACTION_KEYS, f"{route_key} missing from config ACTION_KEYS"
    canonical = normalize_action_key(ACTION_KEYS[route_key])
    assert CANONICAL_TO_DB.get(canonical) == db_code
    # Round-trips, so admin/debug views can map a DB code back to the action.
    assert DB_TO_CANONICAL.get(db_code) == canonical


def test_every_meshy_code_is_reachable_from_a_canonical_key():
    mapped = {code for code in CANONICAL_TO_DB.values() if code.startswith("MESHY_")}
    assert set(EXPECTED_COSTS).issubset(mapped)


def test_consolidated_migration_matches_the_expected_prices():
    """Migration 084 must restate exactly the agreed price list."""
    for folder in ("migrations", "deploy_migrations"):
        path = MESHY_ROOT / folder / "084_meshy_api_parity_action_costs.sql"
        assert path.exists(), f"{folder}/084_meshy_api_parity_action_costs.sql is missing"
        sql = path.read_text()
        body = "\n".join(line for line in sql.splitlines() if not line.strip().startswith("--"))
        found = {
            m.group(1): int(m.group(2))
            # Codes contain digits (…_TO_3D), so the class must allow them.
            for m in re.finditer(r"\('(MESHY_[A-Z0-9_]+)',\s*(\d+),", body)
        }
        assert found == EXPECTED_COSTS, f"{folder}/084 price list drifted: {found}"
        assert "ON CONFLICT (action_code) DO UPDATE" in sql, "migration must be an idempotent upsert"


# ── Provider-tier surcharges ─────────────────────────────────────

def test_surcharge_table_is_the_documented_one():
    assert MESHY_SURCHARGES == {"texture_8k": 5, "ultra_mode": 5}


@pytest.mark.parametrize("base,resolution,ultra,expected", [
    (30, "2k", False, 30),
    (30, "4k", False, 30),
    (30, "8k", False, 35),   # image-to-3d at 8K
    (30, "8k", True, 40),    # image-to-3d at 8K + Ultra
    (30, "2k", True, 35),    # Ultra only
    (10, "8k", False, 15),   # retexture / refine at 8K
    (10, "2k", False, 10),
    (20, None, False, 20),   # text preview: no texture tier
])
def test_expected_cost_matches_what_the_panels_display(base, resolution, ultra, expected):
    assert expected_meshy_platform_cost(base, texture_resolution=resolution, ultra_mode=ultra) == expected


def test_billing_fallback_meshy_prices_match_the_seeded_defaults():
    """The DB-outage fallback must not drift from DEFAULT_ACTION_COSTS."""
    from backend.routes import billing

    app = flask.Flask(__name__)
    app.register_blueprint(billing.bp, url_prefix="/api/billing")
    with app.test_client() as client:
        # No DATABASE_URL in tests, so this exercises the fallback branch.
        data = client.get("/api/billing/action-costs").get_json()

    served = {row["action_key"]: row["credits"] for row in data["action_costs"]}
    for db_code, expected in EXPECTED_COSTS.items():
        canonical = DB_TO_CANONICAL.get(db_code)
        assert canonical in served, f"{canonical} missing from the action-costs response"
        assert served[canonical] == expected, (
            f"{canonical} served as {served[canonical]}, seeded default is {expected}"
        )
    assert data.get("meshy_surcharges") == MESHY_SURCHARGES


def test_action_costs_endpoint_publishes_the_surcharges():
    """The UI reads the rule instead of hard-coding it."""
    from backend.routes import billing

    source = Path(billing.__file__).read_text()
    assert "meshy_surcharges" in source, "action-costs response must publish meshy_surcharges"
    assert "MESHY_SURCHARGES" in source, "it must come from the single backend table"
