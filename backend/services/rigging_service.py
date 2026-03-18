"""
Rigging & Animation Service — wraps Meshy rigging + animation APIs.

Endpoints (per Meshy public docs):
- POST /openapi/v1/rigging           — create rigging task
- GET  /openapi/v1/rigging/{id}      — poll rigging task
- GET  /openapi/v1/rigging/{id}/stream — SSE stream rigging task
- POST /openapi/v1/animations        — create animation task
- GET  /openapi/v1/animations/{id}   — poll animation task
- GET  /openapi/v1/animations/{id}/stream — SSE stream animation task
"""

from __future__ import annotations

from typing import Any

import requests

from backend.config import MESHY_API_BASE
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


def stream_rigging_task(task_id: str):
    """
    Open an SSE stream for a Meshy rigging task.

    Yields raw line bytes from the SSE stream.  The caller (route) is
    responsible for turning this into a Flask streaming response.
    """
    url = f"{MESHY_API_BASE.rstrip('/')}/openapi/v1/rigging/{task_id}/stream"
    resp = requests.get(url, headers=_auth_headers(), stream=True, timeout=300)
    resp.raise_for_status()
    for line in resp.iter_lines():
        yield line


def normalize_rigging_response(ms: dict) -> dict:
    """
    Normalize a Meshy rigging response into the shape the frontend expects.

    Extracts rigged_character_glb_url, rigged_character_fbx_url, and
    basic_animations from the response containers.

    Meshy returns basic_animations as an object:
      {walking_glb_url, walking_fbx_url, walking_armature_glb_url,
       running_glb_url, running_fbx_url, running_armature_glb_url}
    We normalize this into a frontend-friendly array while preserving all URLs.
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
    raw_animations = _pick_first(containers, ["basic_animations", "animations"])

    # Normalize basic_animations from Meshy's object format to array format
    basic_animations = _normalize_basic_animations(raw_animations)

    # Queue position (Meshy returns this for PENDING tasks)
    preceding = _pick_first(containers, ["preceding_tasks"])
    try:
        preceding = int(preceding) if preceding is not None else None
    except (TypeError, ValueError):
        preceding = None

    # Task error details (for failed tasks)
    task_error = _pick_first(containers, ["task_error"])
    error_msg = None
    if isinstance(task_error, dict):
        error_msg = task_error.get("message")
    elif isinstance(task_error, str) and task_error:
        error_msg = task_error

    result = {
        "id": _pick_first(containers, ["id", "task_id"]),
        "status": status,
        "pct": pct,
        "stage": "rig",
        "rigged_character_glb_url": rigged_glb,
        "rigged_character_fbx_url": rigged_fbx,
        "basic_animations": basic_animations,
        "basic_animations_raw": raw_animations,
        "thumbnail_url": _pick_first(containers, ["thumbnail_url", "cover_image_url", "image"]),
        "meshy_status": st_raw,
    }
    if preceding is not None:
        result["preceding_tasks"] = preceding
    if error_msg:
        result["message"] = error_msg
    return result


def _normalize_basic_animations(raw: Any) -> list[dict] | None:
    """
    Convert Meshy's basic_animations object into a frontend-friendly array.

    Meshy returns:
        {
            "walking_glb_url": "...", "walking_fbx_url": "...", "walking_armature_glb_url": "...",
            "running_glb_url": "...", "running_fbx_url": "...", "running_armature_glb_url": "..."
        }

    We produce:
        [
            {"name": "Walking", "action": "walking", "glb_url": "...", "fbx_url": "...", "armature_glb_url": "..."},
            {"name": "Running", "action": "running", "glb_url": "...", "fbx_url": "...", "armature_glb_url": "..."}
        ]
    """
    if raw is None:
        return None

    # Already an array — pass through
    if isinstance(raw, list):
        return raw

    # Object format from Meshy
    if isinstance(raw, dict):
        animations = []
        # Extract all animation types by scanning keys
        types_seen: set[str] = set()
        for key in raw:
            # Keys follow pattern: {type}_glb_url, {type}_fbx_url, {type}_armature_glb_url
            parts = key.rsplit("_", 2)
            if len(parts) >= 2:
                # e.g. "walking_glb_url" -> type="walking"
                anim_type = key.split("_glb_url")[0].split("_fbx_url")[0].split("_armature_glb_url")[0]
                if anim_type and anim_type != key:
                    types_seen.add(anim_type)

        for anim_type in sorted(types_seen):
            glb = raw.get(f"{anim_type}_glb_url")
            fbx = raw.get(f"{anim_type}_fbx_url")
            armature = raw.get(f"{anim_type}_armature_glb_url")
            if glb or fbx:
                animations.append({
                    "name": anim_type.replace("_", " ").title(),
                    "action": anim_type,
                    "glb_url": glb,
                    "fbx_url": fbx,
                    "armature_glb_url": armature,
                })

        return animations if animations else None

    return None


# ─── Animation ──────────────────────────────────────────────────────────────

def create_animation_task(
    rig_task_id: str,
    action_id: int,
    post_process: dict | None = None,
) -> dict:
    """
    Create a Meshy animation task from a previously rigged model.

    Args:
        rig_task_id: ID of the completed rigging task
        action_id: Integer animation ID from Meshy's animation library (0-584+)
        post_process: Optional post-processing config, e.g.
            {"operation_type": "change_fps", "fps": 30}
            {"operation_type": "fbx2usdz"}
            {"operation_type": "extract_armature"}

    Returns:
        Raw Meshy response dict.
    """
    payload: dict[str, Any] = {
        "rig_task_id": rig_task_id,
        "action_id": action_id,
    }
    if post_process:
        payload["post_process"] = post_process
    return mesh_post("/openapi/v1/animations", payload)


def get_animation_task(task_id: str) -> dict:
    """Poll a Meshy animation task by ID."""
    return mesh_get(f"/openapi/v1/animations/{task_id}")


def stream_animation_task(task_id: str):
    """
    Open an SSE stream for a Meshy animation task.

    Yields raw line bytes from the SSE stream.
    """
    url = f"{MESHY_API_BASE.rstrip('/')}/openapi/v1/animations/{task_id}/stream"
    resp = requests.get(url, headers=_auth_headers(), stream=True, timeout=300)
    resp.raise_for_status()
    for line in resp.iter_lines():
        yield line


def normalize_animation_response(ms: dict) -> dict:
    """
    Normalize a Meshy animation response into the shape the frontend expects.

    Extracts all animation output URLs including post-processed variants.
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

    animation_glb = _pick_first(
        containers,
        ["animation_glb_url", "glb_url"],
    )
    animation_fbx = _pick_first(
        containers,
        ["animation_fbx_url", "fbx_url"],
    )

    # Post-processed outputs (only present when post_process was used)
    processed_usdz = _pick_first(containers, ["processed_usdz_url"])
    processed_armature_fbx = _pick_first(containers, ["processed_armature_fbx_url"])
    processed_animation_fps_fbx = _pick_first(containers, ["processed_animation_fps_fbx_url"])

    # Fallback: if animation_glb is not a URL, clear it
    if animation_glb and not str(animation_glb).startswith("http"):
        animation_glb = None

    # Queue position
    preceding = _pick_first(containers, ["preceding_tasks"])
    try:
        preceding = int(preceding) if preceding is not None else None
    except (TypeError, ValueError):
        preceding = None

    # Task error
    task_error = _pick_first(containers, ["task_error"])
    error_msg = None
    if isinstance(task_error, dict):
        error_msg = task_error.get("message")
    elif isinstance(task_error, str) and task_error:
        error_msg = task_error

    result = {
        "id": _pick_first(containers, ["id", "task_id"]),
        "status": status,
        "pct": pct,
        "stage": "animate",
        "animation_glb_url": animation_glb,
        "animation_fbx_url": animation_fbx,
        "processed_usdz_url": processed_usdz,
        "processed_armature_fbx_url": processed_armature_fbx,
        "processed_animation_fps_fbx_url": processed_animation_fps_fbx,
        "thumbnail_url": _pick_first(containers, ["thumbnail_url", "cover_image_url", "image"]),
        "meshy_status": st_raw,
    }
    if preceding is not None:
        result["preceding_tasks"] = preceding
    if error_msg:
        result["message"] = error_msg
    return result
