"""
Meshy API service functions migrated from app.py.

This module intentionally mirrors the monolith's behavior so we can migrate
routes gradually without breaking the credit/identity pipeline.
"""

from __future__ import annotations

from typing import Any, Iterable

import requests

from backend.config import MESHY_API_BASE, MESHY_API_KEY
from backend.utils import normalize_epoch_ms


MESHY_STATUS_MAP = {
    "PENDING": "pending",
    "IN_PROGRESS": "running",
    "SUCCEEDED": "done",
    "FAILED": "failed",
    "COMPLETED": "done",
    "FINISHED": "done",
    "SUCCESS": "done",
    "CANCELED": "failed",
    "CANCELLED": "failed",
    "TIMEOUT": "failed",
}


def _filter_model_urls(urls: Any) -> dict:
    """Filter model URLs dict to only include glb and obj formats."""
    if not isinstance(urls, dict):
        return {}
    filtered: dict[str, str] = {}
    for key in ("glb", "obj"):
        val = urls.get(key)
        if val:
            filtered[key] = val
    return filtered


def _auth_headers() -> dict[str, str]:
    if not MESHY_API_KEY:
        raise RuntimeError("MESHY_API_KEY not set")
    return {
        "Authorization": f"Bearer {MESHY_API_KEY}",
        "Content-Type": "application/json",
    }


def mesh_post(path: str, payload: dict) -> dict:
    url = f"{MESHY_API_BASE.rstrip('/')}{path}"
    r = requests.post(url, headers=_auth_headers(), json=payload, timeout=60)
    if not r.ok:
        raise RuntimeError(f"POST {path} -> {r.status_code}: {r.text[:500]}")
    return r.json()


def mesh_get(path: str) -> dict:
    url = f"{MESHY_API_BASE.rstrip('/')}{path}"
    r = requests.get(url, headers=_auth_headers(), timeout=60)
    if not r.ok:
        raise RuntimeError(f"GET {path} -> {r.status_code}: {r.text[:500]}")
    return r.json()


def _task_containers(ms: dict) -> list[dict]:
    """
    Meshy responses may wrap the actual payload in `data`, `result`, or `task_result`.
    Return a list of dicts in priority order so lookups can scan through them.
    """
    containers: list[dict] = []
    if isinstance(ms, dict):
        containers.append(ms)
        for key in ("data", "result", "task_result"):
            val = ms.get(key)
            if isinstance(val, dict):
                containers.append(val)
            if isinstance(val, list):
                containers.extend([x for x in val if isinstance(x, dict)])
        for key in ("output", "outputs"):
            val = ms.get(key)
            if isinstance(val, dict):
                containers.append(val)
            if isinstance(val, list):
                containers.extend([x for x in val if isinstance(x, dict)])
    return containers or [{}]


def _pick_first(containers: Iterable[dict], keys: Iterable[str], default=None):
    for c in containers:
        if not isinstance(c, dict):
            continue
        for k in keys:
            val = c.get(k)
            if val not in (None, "", []):
                return val
    return default


def extract_model_urls(ms: dict):
    """
    Meshy responses sometimes return URLs in slightly different buckets or nesting.
    Surface the ones the frontend expects to see.
    """
    containers = _task_containers(ms)
    model_urls: dict = {}
    textured_model_urls: dict = {}
    textured_glb_url = None
    rigged_glb = None
    rigged_fbx = None
    glb_candidates: list[str] = []

    def pick_url(container: dict) -> str | None:
        if not isinstance(container, dict):
            return None
        return container.get("glb") or container.get("obj")

    for c in containers:
        if not isinstance(c, dict):
            continue
        if not model_urls and isinstance(c.get("model_urls"), dict):
            model_urls = _filter_model_urls(c.get("model_urls") or {})
        if not textured_model_urls and isinstance(c.get("textured_model_urls"), dict):
            textured_model_urls = _filter_model_urls(c.get("textured_model_urls") or {})
        if not model_urls and isinstance(c.get("output_model_urls"), dict):
            model_urls = _filter_model_urls(c.get("output_model_urls") or {})
        if not textured_model_urls and isinstance(c.get("output_textured_model_urls"), dict):
            textured_model_urls = _filter_model_urls(c.get("output_textured_model_urls") or {})
        if not textured_glb_url and c.get("textured_glb_url"):
            textured_glb_url = c.get("textured_glb_url")
        if not rigged_glb and c.get("rigged_character_glb_url"):
            rigged_glb = c.get("rigged_character_glb_url")
        if not rigged_fbx and c.get("rigged_character_fbx_url"):
            rigged_fbx = c.get("rigged_character_fbx_url")

        glb_candidates.extend(
            [
                url
                for url in [
                    c.get("textured_glb_url"),
                    c.get("textured_model_url"),
                    pick_url(c.get("textured_model_urls") or {}),
                    c.get("glb_url"),
                    c.get("model_url"),
                    c.get("output_model_url"),
                    c.get("mesh_url"),
                    c.get("mesh_download_url"),
                    c.get("gltf_url"),
                    c.get("gltf_download_url"),
                    pick_url(c.get("model_urls") or {}),
                    pick_url(c.get("output_model_urls") or {}),
                    c.get("rigged_character_glb_url"),
                ]
                if url
            ]
        )

    if not glb_candidates and isinstance(ms, dict) and isinstance(ms.get("result"), str) and ms["result"].startswith("http"):
        glb_candidates.append(ms["result"])

    glb_url = (
        textured_glb_url
        or textured_model_urls.get("glb")
        or model_urls.get("glb")
        or next((u for u in glb_candidates if u), None)
        or textured_model_urls.get("obj")
        or model_urls.get("obj")
        or rigged_glb
    )

    return glb_url, model_urls, textured_model_urls, textured_glb_url, rigged_glb, rigged_fbx


def log_status_summary(route: str, job_id: str, payload: dict):
    """Lightweight status logging for debugging stuck jobs without being spammy."""
    try:
        glb_url, model_urls, textured_model_urls, textured_glb_url, rigged_glb, _ = extract_model_urls(payload or {})
        has_model = bool(glb_url or textured_glb_url or rigged_glb)
        has_model = has_model or bool(
            (model_urls and isinstance(model_urls, dict) and any(model_urls.values()))
            or (textured_model_urls and isinstance(textured_model_urls, dict) and any(textured_model_urls.values()))
        )
        print(
            "[status] %s job=%s status=%s pct=%s has_model=%s glb=%s"
            % (
                route,
                job_id,
                payload.get("status") or payload.get("task_status"),
                payload.get("pct") or payload.get("progress") or payload.get("progress_percentage"),
                has_model,
                (glb_url or textured_glb_url or rigged_glb or "")[:128],
            )
        )
    except Exception as e:
        print(f"[status] {route} job={job_id} log-failed: {e}")


def normalize_status(ms: dict) -> dict:
    """Map Meshy task to the shape your frontend expects."""
    containers = _task_containers(ms)
    st_raw = (_pick_first(containers, ["status", "task_status"]) or "").upper()
    status = MESHY_STATUS_MAP.get(st_raw, st_raw.lower() or "pending")
    try:
        pct = int(_pick_first(containers, ["progress", "progress_percentage", "progress_percent", "percent"]) or 0)
    except Exception:
        pct = 0
    mode = (_pick_first(containers, ["mode", "stage"]) or "").strip().lower()
    stage = "refine" if mode == "refine" else (mode or "preview")

    glb_url, model_urls, textured_model_urls, textured_glb_url, rigged_glb, rigged_fbx = extract_model_urls(ms)

    return {
        "id": _pick_first(containers, ["id", "task_id"]),
        "status": status,
        "pct": pct,
        "stage": stage,
        "thumbnail_url": _pick_first(containers, ["thumbnail_url", "cover_image_url", "image"]),
        "glb_url": glb_url,
        "model_urls": model_urls,
        "textured_model_urls": textured_model_urls,
        "textured_glb_url": textured_glb_url,
        "rigged_character_glb_url": rigged_glb,
        "rigged_character_fbx_url": rigged_fbx,
        "created_at": normalize_epoch_ms(_pick_first(containers, ["created_at", "created_at_ts", "created_time"])),
        "preview_task_id": _pick_first(containers, ["preview_task_id", "preview_task"]),
    }


def normalize_meshy_task(ms: dict, *, stage: str) -> dict:
    containers = _task_containers(ms)
    st_raw = (_pick_first(containers, ["status", "task_status"]) or "").upper()
    status = MESHY_STATUS_MAP.get(st_raw, st_raw.lower() or "pending")
    try:
        pct = int(_pick_first(containers, ["progress", "progress_percentage", "progress_percent", "percent"]) or 0)
    except Exception:
        pct = 0

    glb_url, model_urls, textured_model_urls, textured_glb_url, rigged_glb, rigged_fbx = extract_model_urls(ms)

    return {
        "id": _pick_first(containers, ["id", "task_id"]),
        "status": status,
        "pct": pct,
        "stage": (_pick_first(containers, ["stage"]) or "").strip().lower() or stage,
        "thumbnail_url": _pick_first(containers, ["thumbnail_url", "cover_image_url", "image"]),
        "glb_url": glb_url,
        "model_urls": model_urls,
        "textured_model_urls": textured_model_urls,
        "textured_glb_url": textured_glb_url,
        "texture_urls": _pick_first(containers, ["texture_urls", "textures"]),
        "basic_animations": _pick_first(containers, ["basic_animations", "animations"]),
        "rigged_character_glb_url": rigged_glb,
        "rigged_character_fbx_url": rigged_fbx,
        "created_at": normalize_epoch_ms(_pick_first(containers, ["created_at", "created_at_ts", "created_time"])),
    }


def build_source_payload(body: dict, identity_id: str | None = None):
    """Validate and build the source payload for Meshy operations."""
    input_task_id = (body.get("input_task_id") or "").strip()
    model_url = (body.get("model_url") or "").strip()
    if input_task_id and model_url:
        return None, "Provide only one of input_task_id or model_url"
    if not input_task_id and not model_url:
        return None, "input_task_id or model_url required"

    if input_task_id:
        # Resolve internal IDs to original Meshy task IDs when needed.
        try:
            from backend.services.job_service import resolve_meshy_job_id, verify_job_ownership
            resolved = resolve_meshy_job_id(input_task_id)
            if identity_id and not verify_job_ownership(resolved, identity_id):
                return None, "Job not found or access denied"
            return {"input_task_id": resolved}, None
        except Exception:
            return {"input_task_id": input_task_id}, None

    # Normalize proxy URLs (our proxy includes `u` query param)
    if model_url and ("/api/proxy-glb" in model_url or "/api/_mod/proxy-glb" in model_url):
        try:
            from urllib.parse import urlparse, parse_qs, unquote

            parsed = urlparse(model_url)
            params = parse_qs(parsed.query)
            proxied = params.get("u", [None])[0]
            if proxied:
                model_url = unquote(proxied)
        except Exception:
            pass

    # If model_url is our S3 bucket, ensure identity owns it and sign for Meshy access.
    try:
        from backend.db import USE_DB, get_conn, dict_row, Tables
        from backend.services.s3_service import is_s3_url, parse_s3_key, presign_s3_url

        if model_url and is_s3_url(model_url) and identity_id and USE_DB:
            s3_key = parse_s3_key(model_url)
            with get_conn() as conn:
                with conn.cursor(row_factory=dict_row) as cur:
                    # Check ownership in MODELS table (direct columns)
                    # AND in HISTORY_ITEMS table (both direct glb_url column and payload->>'glb_url')
                    cur.execute(
                        f"""
                        SELECT 1
                        FROM {Tables.MODELS}
                        WHERE identity_id = %s AND (glb_s3_key = %s OR glb_url = %s)
                        UNION
                        SELECT 1
                        FROM {Tables.HISTORY_ITEMS}
                        WHERE identity_id = %s AND (
                            glb_url = %s
                            OR payload->>'glb_url' = %s
                            OR payload->>'model_url' = %s
                        )
                        LIMIT 1
                        """,
                        (identity_id, s3_key, model_url, identity_id, model_url, model_url, model_url),
                    )
                    row = cur.fetchone()
            if not row:
                return None, "Model URL not found or access denied"
            signed = presign_s3_url(model_url)
            if signed:
                model_url = signed
    except Exception as e:
        print(f"[Meshy] build_source_payload ownership/sign check failed: {e}")

    return {"model_url": model_url}, None
