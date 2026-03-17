"""
Rigging & Animation Service — wraps Meshy rigging + animation APIs.

Endpoints:
- POST /openapi/v1/rigging         — create rigging task
- GET  /openapi/v1/rigging/{id}    — poll rigging task
- POST /openapi/v1/animate         — create animation task
- GET  /openapi/v1/animate/{id}    — poll animation task
"""

from __future__ import annotations

from typing import Any

from backend.services.meshy_service import (
    MESHY_STATUS_MAP,
    _auth_headers,
    _pick_first,
    _task_containers,
    mesh_get,
    mesh_post,
)


# ─── Rigging ────────────────────────────────────────────────────────────────

def create_rigging_task(
    model_source: dict,
    height_meters: float = 1.7,
) -> dict:
    """
    Create a Meshy rigging task.

    Args:
        model_source: dict with either {"input_task_id": ...} or {"model_url": ...}
        height_meters: Character height in meters (default 1.7)

    Returns:
        Raw Meshy response dict (contains "result" or "id" for the task ID).
    """
    payload: dict[str, Any] = {**model_source, "height_meters": height_meters}
    return mesh_post("/openapi/v1/rigging", payload)


def get_rigging_task(task_id: str) -> dict:
    """Poll a Meshy rigging task by ID."""
    return mesh_get(f"/openapi/v1/rigging/{task_id}")


def normalize_rigging_response(ms: dict) -> dict:
    """
    Normalize a Meshy rigging response into the shape the frontend expects.

    Extracts rigged_character_glb_url, rigged_character_fbx_url, and
    basic_animations from the response containers.
    """
    containers = _task_containers(ms)

    st_raw = (_pick_first(containers, ["status", "task_status"]) or "").upper()
    status = MESHY_STATUS_MAP.get(st_raw, st_raw.lower() or "pending")

    try:
        pct = int(
            _pick_first(
                containers,
                ["progress", "progress_percentage", "progress_percent", "percent"],
            )
            or 0
        )
    except Exception:
        pct = 0

    rigged_glb = _pick_first(containers, ["rigged_character_glb_url", "glb_url"])
    rigged_fbx = _pick_first(containers, ["rigged_character_fbx_url", "fbx_url"])
    basic_animations = _pick_first(containers, ["basic_animations", "animations"])

    return {
        "id": _pick_first(containers, ["id", "task_id"]),
        "status": status,
        "pct": pct,
        "stage": "rig",
        "rigged_character_glb_url": rigged_glb,
        "rigged_character_fbx_url": rigged_fbx,
        "basic_animations": basic_animations,
        "thumbnail_url": _pick_first(containers, ["thumbnail_url", "cover_image_url", "image"]),
    }


# ─── Animation ──────────────────────────────────────────────────────────────

def create_animation_task(
    rigging_task_id: str,
    animation_action: str,
) -> dict:
    """
    Create a Meshy animation task from a previously rigged model.

    Args:
        rigging_task_id: ID of the completed rigging task
        animation_action: Animation type (e.g. "walk", "run", "idle", "dance")

    Returns:
        Raw Meshy response dict.
    """
    payload = {
        "input_task_id": rigging_task_id,
        "animation_action": animation_action,
    }
    return mesh_post("/openapi/v1/animate", payload)


def get_animation_task(task_id: str) -> dict:
    """Poll a Meshy animation task by ID."""
    return mesh_get(f"/openapi/v1/animate/{task_id}")


def normalize_animation_response(ms: dict) -> dict:
    """
    Normalize a Meshy animation response into the shape the frontend expects.

    Extracts animation_glb_url and animation_fbx_url from the response.
    """
    containers = _task_containers(ms)

    st_raw = (_pick_first(containers, ["status", "task_status"]) or "").upper()
    status = MESHY_STATUS_MAP.get(st_raw, st_raw.lower() or "pending")

    try:
        pct = int(
            _pick_first(
                containers,
                ["progress", "progress_percentage", "progress_percent", "percent"],
            )
            or 0
        )
    except Exception:
        pct = 0

    # Animation outputs may come as different keys
    animation_glb = _pick_first(
        containers,
        ["animation_glb_url", "glb_url", "output", "result", "animated_model_url", "output_url"],
    )
    animation_fbx = _pick_first(
        containers,
        ["animation_fbx_url", "fbx_url"],
    )

    # Fallback: if animation_glb is not a URL, clear it
    if animation_glb and not str(animation_glb).startswith("http"):
        animation_glb = None

    return {
        "id": _pick_first(containers, ["id", "task_id"]),
        "status": status,
        "pct": pct,
        "stage": "animate",
        "animation_glb_url": animation_glb,
        "animation_fbx_url": animation_fbx,
        "thumbnail_url": _pick_first(containers, ["thumbnail_url", "cover_image_url", "image"]),
    }
