"""Meshy Creative Lab + Meshy-native image routes (Stage 7.2 / 7.3)."""

from __future__ import annotations

import sys
from pathlib import Path
from unittest import mock

import flask
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from backend.config import ACTION_KEYS
from backend.routes import meshy_creative_lab as cl
from backend.routes import meshy_image as mi
from backend.services.credits_helper import PAID_GENERATION_ACTIONS
from backend.services.pricing_service import (
    CANONICAL_TO_DB,
    DEFAULT_ACTION_COSTS,
    normalize_action_key,
)

IDENTITY = "11111111-1111-1111-1111-111111111111"
SEEDED = {row["action_code"]: row["cost_credits"] for row in DEFAULT_ACTION_COSTS}

# Meshy's published Creative Lab prices; TimrX mirrors them 1:1.
EXPECTED_PRICES = {
    "keychain": (6, 30), "fridge-magnet": (6, 30), "figure": (6, 30),
    "vinyl-figure": (6, 30), "brick-figure": (6, 30), "lamp": (6, 30),
    "keycap": (12, 50),
}


def _run(module, view, body, **view_kwargs):
    captured = {}

    def _fake_mesh_post(path, payload):
        captured["path"] = path
        captured["payload"] = payload
        return {"result": "meshy-task-id"}

    def _fake_start_paid_job(identity_id, action_key, job_id, meta):
        captured["action_key"] = action_key
        captured["job_meta"] = meta
        return ("reservation-1", None)

    app = flask.Flask(__name__)
    with app.test_request_context(json=body, method="POST"):
        with mock.patch.object(module, "MESHY_API_KEY", "test-key"), \
             mock.patch.object(module, "require_identity", return_value=(IDENTITY, None)), \
             mock.patch.object(module, "get_job_metadata", return_value={}), \
             mock.patch.object(module, "load_store", return_value={}), \
             mock.patch.object(module, "save_store", lambda *a, **k: None), \
             mock.patch.object(module, "create_internal_job_row", lambda **k: True), \
             mock.patch.object(module, "get_job_by_idempotency_key", return_value=None), \
             mock.patch.object(module, "start_paid_job", _fake_start_paid_job), \
             mock.patch.object(module, "get_current_balance", return_value={"available": 500}), \
             mock.patch.object(module, "mesh_post", _fake_mesh_post), \
             mock.patch("backend.services.async_dispatch.update_job_with_upstream_id",
                        lambda *a, **k: None):
            response = view.__wrapped__(**view_kwargs)

    status = response[1] if isinstance(response, tuple) else 200
    payload_obj = response[0] if isinstance(response, tuple) else response
    return status, payload_obj.get_json(), captured


# ── Creative Lab ─────────────────────────────────────────────────

@pytest.mark.parametrize("product", sorted(EXPECTED_PRICES))
def test_prototype_hits_the_product_scoped_endpoint(product):
    status, data, captured = _run(
        cl, cl.creative_lab_prototype,
        {"image_url": "https://example.com/photo.jpg", "name": "My thing"},
        product=product,
    )

    assert status == 200
    assert captured["path"] == f"/openapi/creative-lab/{product}/v1/prototype"
    assert captured["payload"]["image_url"] == "https://example.com/photo.jpg"
    assert captured["payload"]["name"] == "My thing"
    assert captured["action_key"] == f"creative-lab-{product}-prototype"
    assert data["product"] == product and data["stage"] == "prototype"


@pytest.mark.parametrize("product", sorted(EXPECTED_PRICES))
def test_build_requires_and_forwards_the_prototype_task(product):
    status, _data, captured = _run(
        cl, cl.creative_lab_build,
        {"input_task_id": "proto-task", "output_format": "glb"},
        product=product,
    )

    assert status == 200
    assert captured["path"] == f"/openapi/creative-lab/{product}/v1/build"
    assert captured["payload"]["input_task_id"] == "proto-task"
    assert captured["payload"]["output"] == {"format": "glb"}
    assert captured["action_key"] == f"creative-lab-{product}-build"


def test_build_without_a_prototype_is_rejected():
    status, data, captured = _run(cl, cl.creative_lab_build, {}, product="keychain")
    assert status == 400
    assert "input_task_id" in data["error"]
    assert "payload" not in captured


def test_prototype_without_an_image_is_rejected():
    status, data, captured = _run(cl, cl.creative_lab_prototype, {}, product="figure")
    assert status == 400
    assert "image_url" in data["error"]
    assert "payload" not in captured


def test_unknown_product_is_rejected_before_any_charge():
    status, data, captured = _run(
        cl, cl.creative_lab_prototype,
        {"image_url": "https://example.com/a.png"}, product="spaceship",
    )
    assert status == 400
    assert data["error"] == "UNKNOWN_CREATIVE_LAB_PRODUCT"
    assert "action_key" not in captured  # no reservation attempted


def test_build_options_pass_through_untouched():
    _status, _data, captured = _run(
        cl, cl.creative_lab_build,
        {"input_task_id": "p", "options": {"size_mm": 60, "relief_height_mm": 3}},
        product="keychain",
    )
    assert captured["payload"]["options"] == {"size_mm": 60, "relief_height_mm": 3}


@pytest.mark.parametrize("product,prices", sorted(EXPECTED_PRICES.items()))
def test_prices_mirror_meshy_one_to_one(product, prices):
    prototype_cost, build_cost = prices
    for stage, expected in (("prototype", prototype_cost), ("build", build_cost)):
        canonical = normalize_action_key(ACTION_KEYS[f"creative-lab-{product}-{stage}"])
        code = CANONICAL_TO_DB[canonical]
        assert SEEDED[code] == expected
        # Paid actions must fail closed if their price row goes missing.
        assert canonical in PAID_GENERATION_ACTIONS


def test_every_product_is_covered_by_the_price_table():
    assert set(cl.CREATIVE_LAB_PRODUCTS) == set(EXPECTED_PRICES)


# ── Meshy-native images ──────────────────────────────────────────

def test_text_to_image_forwards_model_and_prompt():
    status, _data, captured = _run(
        mi, mi.meshy_text_to_image,
        {"prompt": "a brass owl", "ai_model": "nano-banana-pro", "aspect_ratio": "16:9"},
    )

    assert status == 200
    assert captured["path"] == "/openapi/v1/text-to-image"
    assert captured["payload"] == {
        "prompt": "a brass owl", "ai_model": "nano-banana-pro", "aspect_ratio": "16:9",
    }
    assert captured["action_key"] == ACTION_KEYS["meshy-text-to-image"]


def test_text_to_image_rejects_an_unknown_model():
    status, data, captured = _run(
        mi, mi.meshy_text_to_image, {"prompt": "x", "ai_model": "dall-e"},
    )
    assert status == 400 and "ai_model" in data["error"]
    assert "payload" not in captured


def test_text_to_image_rejects_a_bad_aspect_ratio():
    status, _data, captured = _run(
        mi, mi.meshy_text_to_image, {"prompt": "x", "aspect_ratio": "7:3"},
    )
    assert status == 400
    assert "payload" not in captured


def test_image_to_image_requires_one_to_five_references():
    status, _data, _c = _run(
        mi, mi.meshy_image_to_image, {"prompt": "x", "reference_image_urls": []},
    )
    assert status == 400

    status, _data, captured = _run(
        mi, mi.meshy_image_to_image,
        {"prompt": "x", "reference_image_urls": [f"https://e.com/{i}.png" for i in range(6)]},
    )
    assert status == 400
    assert "payload" not in captured

    status, _data, captured = _run(
        mi, mi.meshy_image_to_image,
        {"prompt": "weathered brass", "reference_image_urls": ["https://e.com/a.png"]},
    )
    assert status == 200
    assert captured["path"] == "/openapi/v1/image-to-image"
    assert captured["payload"]["reference_image_urls"] == ["https://e.com/a.png"]


def test_meshy_image_prices_cover_the_dearest_model():
    """Priced at the top of Meshy's per-model range so no model loses money."""
    assert SEEDED[CANONICAL_TO_DB[normalize_action_key(ACTION_KEYS["meshy-text-to-image"])]] == 9
    assert SEEDED[CANONICAL_TO_DB[normalize_action_key(ACTION_KEYS["meshy-image-to-image"])]] == 12


def test_routes_are_registered_under_the_modular_prefix():
    from app_modular import app

    paths = {str(r) for r in app.url_map.iter_rules()}
    assert "/api/_mod/creative-lab/<product>/prototype" in paths
    assert "/api/_mod/creative-lab/<product>/build" in paths
    assert "/api/_mod/creative-lab/<product>/<stage>/status/<job_id>" in paths
    assert "/api/_mod/meshy-image/text-to-image" in paths
    assert "/api/_mod/meshy-image/image-to-image" in paths
    assert "/api/_mod/meshy-image/<kind>/status/<job_id>" in paths
