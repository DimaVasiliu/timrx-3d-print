"""Retexture multiview style validation (Meshy 7 / latest).

Covers the four cases called out in the Meshy API parity plan, Stage 3.2:
valid multiview, unsupported ai_model, too many images, and mixed style modes.

The route is exercised end to end with credits, DB, store and the provider
dispatch stubbed, so the assertions are about the payload that would reach
Meshy — not about any live call.
"""

from __future__ import annotations

import sys
from pathlib import Path
from unittest import mock

import flask
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from backend.routes import mesh_operations as mo

IDENTITY = "11111111-1111-1111-1111-111111111111"
SOURCE_TASK = "meshy-source-task"
VIEWS = [
    "https://example.com/front.png",
    "https://example.com/left.png",
    "https://example.com/back.png",
    "https://example.com/right.png",
]


def call_retexture(body: dict):
    """Run POST /mesh/retexture, returning (status, json, meshy_payload)."""
    captured = {}

    def _fake_mesh_post(path, payload):
        captured["path"] = path
        captured["payload"] = payload
        return {"result": "meshy-retexture-task"}

    app = flask.Flask(__name__)
    with app.test_request_context(json=body, method="POST"):
        with mock.patch.object(mo, "MESHY_API_KEY", "test-key"), \
             mock.patch.object(mo, "require_identity", return_value=(IDENTITY, None)), \
             mock.patch.object(mo, "build_source_payload",
                               return_value=({"input_task_id": SOURCE_TASK}, None)), \
             mock.patch.object(mo, "_preflight_retexture_upstream",
                               return_value={"usable": True, "task_type": "image_to_3d"}), \
             mock.patch.object(mo, "_resolve_and_validate_source_task",
                               return_value=(SOURCE_TASK, None)), \
             mock.patch.object(mo, "get_job_metadata", return_value={}), \
             mock.patch.object(mo, "load_store", return_value={}), \
             mock.patch.object(mo, "save_store", lambda *a, **k: None), \
             mock.patch.object(mo, "create_internal_job_row", lambda **k: True), \
             mock.patch.object(mo, "update_job_with_upstream_id", lambda *a, **k: None), \
             mock.patch.object(mo, "start_paid_job", return_value=("reservation-1", None)), \
             mock.patch.object(mo, "get_current_balance", return_value={"available": 100}), \
             mock.patch.object(mo, "mesh_post", _fake_mesh_post):
            response = mo.mesh_retexture_mod.__wrapped__()

    status = response[1] if isinstance(response, tuple) else 200
    payload_obj = response[0] if isinstance(response, tuple) else response
    return status, payload_obj.get_json(), captured.get("payload")


def test_multiview_accepted_on_latest():
    status, _data, payload = call_retexture({
        "input_task_id": SOURCE_TASK,
        "ai_model": "latest",
        "multiview_image_urls": VIEWS[:3],
    })

    assert status == 200
    assert payload["multiview_image_urls"] == VIEWS[:3]
    # Order matters: Meshy treats the first image as the primary/front view.
    assert payload["multiview_image_urls"][0] == VIEWS[0]
    assert "text_style_prompt" not in payload
    assert "image_style_url" not in payload


def test_multiview_accepted_on_meshy_7():
    status, _data, payload = call_retexture({
        "input_task_id": SOURCE_TASK,
        "ai_model": "meshy-7",
        "multiview_image_urls": [VIEWS[0]],
    })

    assert status == 200
    assert payload["multiview_image_urls"] == [VIEWS[0]]


@pytest.mark.parametrize("ai_model", ["meshy-6", "meshy-5"])
def test_multiview_rejected_on_older_models(ai_model):
    status, data, payload = call_retexture({
        "input_task_id": SOURCE_TASK,
        "ai_model": ai_model,
        "multiview_image_urls": VIEWS[:2],
    })

    assert status == 400
    assert "meshy-7" in data["error"]
    assert payload is None  # nothing dispatched, no credits spent


def test_multiview_rejects_more_than_four_images():
    status, data, payload = call_retexture({
        "input_task_id": SOURCE_TASK,
        "ai_model": "latest",
        "multiview_image_urls": VIEWS + ["https://example.com/extra.png"],
    })

    assert status == 400
    assert "1-4" in data["error"]
    assert payload is None


def test_multiview_rejects_empty_list():
    status, data, payload = call_retexture({
        "input_task_id": SOURCE_TASK,
        "ai_model": "latest",
        "multiview_image_urls": [],
    })

    assert status == 400
    assert payload is None


def test_multiview_wins_over_mixed_style_inputs():
    """Meshy forbids combining multiview with text or single-image style."""
    status, _data, payload = call_retexture({
        "input_task_id": SOURCE_TASK,
        "ai_model": "latest",
        "multiview_image_urls": VIEWS[:2],
        "text_style_prompt": "brushed copper",
        "image_style_url": "https://example.com/style.png",
    })

    assert status == 200
    assert payload["multiview_image_urls"] == VIEWS[:2]
    assert "text_style_prompt" not in payload
    assert "image_style_url" not in payload


def test_single_image_style_beats_text_prompt():
    status, _data, payload = call_retexture({
        "input_task_id": SOURCE_TASK,
        "ai_model": "latest",
        "text_style_prompt": "brushed copper",
        "image_style_url": "https://example.com/style.png",
    })

    assert status == 200
    assert payload["image_style_url"] == "https://example.com/style.png"
    assert "text_style_prompt" not in payload


def test_text_only_style_still_works():
    status, _data, payload = call_retexture({
        "input_task_id": SOURCE_TASK,
        "ai_model": "latest",
        "text_style_prompt": "brushed copper",
    })

    assert status == 200
    assert payload["text_style_prompt"]
    assert "multiview_image_urls" not in payload


def test_no_style_input_is_rejected():
    status, data, payload = call_retexture({
        "input_task_id": SOURCE_TASK,
        "ai_model": "latest",
    })

    assert status == 400
    assert "required" in data["error"]
    assert payload is None


def test_remove_lighting_only_survives_on_meshy_6():
    _status, _data, payload_latest = call_retexture({
        "input_task_id": SOURCE_TASK,
        "ai_model": "latest",
        "text_style_prompt": "brushed copper",
        "remove_lighting": True,
    })
    _status6, _data6, payload_6 = call_retexture({
        "input_task_id": SOURCE_TASK,
        "ai_model": "meshy-6",
        "text_style_prompt": "brushed copper",
        "remove_lighting": True,
    })

    assert payload_latest["remove_lighting"] is False
    assert payload_6["remove_lighting"] is True
