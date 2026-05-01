from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from backend.routes.multi_color_print import (
    _build_multi_color_payload,
    _normalize_multi_color_task,
)
from backend.services.history_service import _primary_model_url_key
from backend.services.meshy_service import extract_model_urls


def test_normalize_multi_color_extracts_3mf_url():
    out = _normalize_multi_color_task({
        "id": "task-1",
        "status": "SUCCEEDED",
        "progress": 87,
        "model_urls": {"3mf": "https://assets.meshy.ai/result"},
    })

    assert out["status"] == "done"
    assert out["pct"] == 100
    assert out["three_mf_url"] == "https://assets.meshy.ai/result"
    assert out["model_urls"] == {"3mf": "https://assets.meshy.ai/result"}


def test_build_payload_prefers_input_task_id():
    payload = _build_multi_color_payload(
        {"input_task_id": "meshy-task", "model_url": "https://example.com/model.glb"},
        8,
        5,
    )

    assert payload == {
        "input_task_id": "meshy-task",
        "max_colors": 8,
        "max_depth": 5,
    }


def test_build_payload_uses_model_url_without_task_id():
    payload = _build_multi_color_payload(
        {"model_url": "https://example.com/model.glb"},
        4,
        3,
    )

    assert payload == {
        "model_url": "https://example.com/model.glb",
        "max_colors": 4,
        "max_depth": 3,
    }


def test_history_model_url_key_preserves_3mf_format():
    assert _primary_model_url_key("model/3mf") == "3mf"
    assert _primary_model_url_key("application/vnd.ms-package.3dmanufacturing-3dmodel+xml") == "3mf"
    assert _primary_model_url_key("model/obj") == "obj"
    assert _primary_model_url_key("model/gltf-binary") == "glb"


def test_extract_model_urls_contract_includes_model_urls_tuple_slot():
    glb_url, model_urls, textured_model_urls, textured_glb_url, rigged_glb, rigged_fbx = extract_model_urls({
        "model_urls": {
            "glb": "https://example.com/model.glb",
            "3mf": "https://example.com/model.3mf",
        }
    })

    assert glb_url == "https://example.com/model.glb"
    assert model_urls["3mf"] == "https://example.com/model.3mf"
    assert textured_model_urls == {}
    assert textured_glb_url is None
    assert rigged_glb is None
    assert rigged_fbx is None
