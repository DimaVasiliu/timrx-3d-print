"""Meshy Analyze / Repair Printability routes (Stage 5 of the API parity plan).

Exercises the real routes with credits, store, DB and the provider call stubbed,
so the assertions cover what would be sent to Meshy, which action key is
reserved, and how the printability metrics are normalized.
"""

from __future__ import annotations

import sys
from pathlib import Path
from unittest import mock

import flask
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from backend.config import ACTION_KEYS
from backend.routes import meshy_printability as mp
from backend.services.job_service import JobService
from backend.services.pricing_service import CANONICAL_TO_DB, DEFAULT_ACTION_COSTS

IDENTITY = "11111111-1111-1111-1111-111111111111"
SOURCE_TASK = "meshy-source-task"

_SEED_COSTS = {row["action_code"]: row["cost_credits"] for row in DEFAULT_ACTION_COSTS}


def seeded_cost(canonical_key: str):
    return _SEED_COSTS.get(CANONICAL_TO_DB.get(canonical_key))


def call_start(route_fn, body: dict):
    """Run a printability create route; returns (status, json, captured)."""
    captured = {}

    def _fake_mesh_post(path, payload):
        captured["path"] = path
        captured["payload"] = payload
        return {"result": "meshy-print-task"}

    def _fake_start_paid_job(identity_id, action_key, job_id, meta):
        captured["action_key"] = action_key
        captured["job_meta"] = meta
        return ("reservation-1", None)

    app = flask.Flask(__name__)
    with app.test_request_context(json=body, method="POST"):
        with mock.patch.object(mp, "MESHY_API_KEY", "test-key"), \
             mock.patch.object(mp, "require_identity", return_value=(IDENTITY, None)), \
             mock.patch.object(mp, "build_source_payload",
                               return_value=({"input_task_id": SOURCE_TASK}, None)), \
             mock.patch.object(mp, "get_job_metadata", return_value={}), \
             mock.patch.object(mp, "load_store", return_value={}), \
             mock.patch.object(mp, "save_store", lambda *a, **k: None), \
             mock.patch.object(mp, "create_internal_job_row", lambda **k: True), \
             mock.patch.object(mp, "start_paid_job", _fake_start_paid_job), \
             mock.patch.object(mp, "get_current_balance", return_value={"available": 100}), \
             mock.patch.object(mp, "mesh_post", _fake_mesh_post), \
             mock.patch("backend.services.async_dispatch.update_job_with_upstream_id",
                        lambda *a, **k: None):
            response = route_fn.__wrapped__()

    status = response[1] if isinstance(response, tuple) else 200
    payload_obj = response[0] if isinstance(response, tuple) else response
    return status, payload_obj.get_json(), captured


# ── Analyze ──────────────────────────────────────────────────────

def test_analyze_posts_to_meshy_and_is_free():
    status, data, captured = call_start(mp.mesh_print_analyze_mod, {"input_task_id": SOURCE_TASK})

    assert status == 200
    assert captured["path"] == "/openapi/v1/print/analyze"
    assert captured["payload"] == {"input_task_id": SOURCE_TASK}
    assert captured["action_key"] == ACTION_KEYS["print-analyze"]
    assert seeded_cost(captured["action_key"]) == 0
    assert captured["job_meta"]["stage"] == "print_analyze"
    assert data["job_id"] == "meshy-print-task"


def test_analyze_requires_a_source():
    app = flask.Flask(__name__)
    with app.test_request_context(json={}, method="POST"):
        with mock.patch.object(mp, "MESHY_API_KEY", "test-key"), \
             mock.patch.object(mp, "require_identity", return_value=(IDENTITY, None)), \
             mock.patch.object(mp, "build_source_payload",
                               return_value=(None, "input_task_id or model_url required")):
            response = mp.mesh_print_analyze_mod.__wrapped__()

    assert response[1] == 400


# ── Repair ───────────────────────────────────────────────────────

def test_repair_posts_to_meshy_at_ten_credits():
    status, data, captured = call_start(mp.mesh_print_repair_mod, {"input_task_id": SOURCE_TASK})

    assert status == 200
    assert captured["path"] == "/openapi/v1/print/repair"
    assert captured["action_key"] == ACTION_KEYS["print-repair"]
    assert seeded_cost(captured["action_key"]) == 10
    assert captured["job_meta"]["stage"] == "print_repair"


def test_repair_forwards_alpha_thumbnail():
    _status, _data, captured = call_start(
        mp.mesh_print_repair_mod, {"input_task_id": SOURCE_TASK, "alpha_thumbnail": True}
    )
    assert captured["payload"]["alpha_thumbnail"] is True

    _status, _data, captured = call_start(mp.mesh_print_repair_mod, {"input_task_id": SOURCE_TASK})
    assert "alpha_thumbnail" not in captured["payload"]


def test_repair_sends_no_undocumented_fields():
    """Meshy documents only the source plus alpha_thumbnail for repair."""
    _status, _data, captured = call_start(
        mp.mesh_print_repair_mod,
        {"input_task_id": SOURCE_TASK, "target_formats": ["glb", "stl"], "topology": "quad"},
    )
    assert set(captured["payload"]) == {"input_task_id"}


# ── Metric normalization ─────────────────────────────────────────

def test_normalize_printability_reads_nested_metrics():
    out = mp.normalize_printability({
        "id": "task-1",
        "status": "SUCCEEDED",
        "printability": {
            "is_watertight": False,
            "volume": 0.000125,
            "holes": 3,
            "non_manifold_edges": 12,
            "degenerate_faces": 0,
            "status": "error",
            "error_count": 2,
            "warning_count": 1,
            "evaluated_at": 1755200000000,
        },
    })

    assert out["is_watertight"] is False
    assert out["volume"] == pytest.approx(0.000125)
    assert out["holes"] == 3
    assert out["non_manifold_edges"] == 12
    assert out["degenerate_faces"] == 0
    assert out["status"] == "error"
    assert out["error_count"] == 2
    assert out["warning_count"] == 1
    assert out["needs_repair"] is True


def test_normalize_printability_healthy_mesh_needs_no_repair():
    out = mp.normalize_printability({
        "printability": {
            "is_watertight": True,
            "volume": 0.002,
            "holes": 0,
            "non_manifold_edges": 0,
            "degenerate_faces": 0,
            "status": "healthy",
            "error_count": 0,
            "warning_count": 0,
        },
    })

    assert out["needs_repair"] is False
    assert out["status"] == "healthy"


def test_normalize_printability_accepts_top_level_metrics():
    out = mp.normalize_printability({
        "is_watertight": True, "holes": 0, "non_manifold_edges": 0,
        "degenerate_faces": 4, "volume": 0.5,
    })

    assert out["degenerate_faces"] == 4
    assert out["needs_repair"] is True  # degenerate faces alone justify a repair
    assert out["status"] == "unknown"


def test_normalize_printability_handles_missing_metrics():
    assert mp.normalize_printability({"id": "t", "status": "PENDING"}) is None
    assert mp.normalize_printability(None) is None


def test_normalize_printability_ignores_bad_types():
    out = mp.normalize_printability({
        "printability": {"is_watertight": True, "holes": "n/a", "volume": "x",
                         "non_manifold_edges": None, "degenerate_faces": 0},
    })

    assert out["holes"] is None
    assert out["volume"] is None
    assert out["needs_repair"] is False


# ── Recovery contract ────────────────────────────────────────────

def test_repair_job_resumes_with_the_meshy_task_id():
    job = {"id": "internal-1", "provider": "meshy", "action_code": "MESHY_PRINT_REPAIR",
           "stage": "print_repair", "upstream_job_id": "meshy-print-task", "meta": {}}
    _provider, resume_id, strategy = JobService._resolve_resume_fields(job)

    assert strategy == "meshy_print_repair"
    assert resume_id == "meshy-print-task"


def test_analyze_job_is_not_adopted_by_recovery():
    job = {"id": "internal-2", "provider": "meshy", "action_code": "MESHY_PRINT_ANALYZE",
           "stage": "print_analyze", "upstream_job_id": "meshy-print-task", "meta": {}}
    _provider, _resume_id, strategy = JobService._resolve_resume_fields(job)

    assert strategy == "skip"
