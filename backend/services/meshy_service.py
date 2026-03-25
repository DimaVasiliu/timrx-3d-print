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


class MeshyTaskNotFoundError(RuntimeError):
    """Raised when Meshy returns 404 for a task that no longer exists."""
    pass


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
        detail = f"GET {path} -> {r.status_code}: {r.text[:500]}"
        if r.status_code == 404:
            raise MeshyTaskNotFoundError(detail)
        raise RuntimeError(detail)
    return r.json()


def terminalize_expired_meshy_job(job_id: str, identity_id: str | None = None):
    """
    Mark a Meshy job as failed when the provider returns 404 (task expired/deleted).
    Releases any held credit reservation. Idempotent — safe to call multiple times.

    job_id may be either the internal UUID (jobs.id) or the Meshy task ID
    (jobs.upstream_job_id). We match on both columns to handle either case.
    """
    try:
        from backend.db import USE_DB, execute, query_one
        from backend.db import Tables
        if not USE_DB:
            return
        # Find the job by either id or upstream_job_id
        row = query_one(
            f"""
            SELECT id::text AS internal_id FROM {Tables.JOBS}
            WHERE (id::text = %s OR upstream_job_id = %s)
              AND status IN ('queued', 'pending', 'processing',
                             'dispatched', 'provider_pending', 'provider_processing')
            LIMIT 1
            """,
            (job_id, job_id),
        )
        if not row:
            return  # Already terminal or not found
        internal_id = row["internal_id"]
        updated = execute(
            f"""
            UPDATE {Tables.JOBS}
            SET status = 'failed',
                error_message = 'Provider task expired (Meshy 404)',
                finished_at = NOW(),
                updated_at = NOW()
            WHERE id = %s
              AND status IN ('queued', 'pending', 'processing',
                             'dispatched', 'provider_pending', 'provider_processing')
            """,
            (internal_id,),
        )
        if updated:
            print(f"[MESHY] Terminalized expired job {internal_id} (lookup={job_id})")
            try:
                from backend.services.credits_helper import refund_failed_job
                refund_failed_job(internal_id)
            except Exception as e:
                print(f"[MESHY] Refund for expired job {internal_id} failed: {e}")
    except Exception as e:
        print(f"[MESHY] terminalize_expired_meshy_job error for {job_id}: {e}")


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
                ]
                if url
            ]
        )

    # Fallback: check top-level string fields that may contain direct URLs
    # (e.g. animation API returns output as a string URL, not a dict)
    if isinstance(ms, dict):
        for fallback_key in ("result", "output", "animated_model_url", "output_url"):
            val = ms.get(fallback_key)
            if isinstance(val, str) and val.startswith("http"):
                glb_candidates.append(val)

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

    # Human-readable status message for the frontend progress label
    if status == "done":
        message = "Generation complete"
    elif status == "failed":
        task_error = _pick_first(containers, ["task_error", "error"])
        if isinstance(task_error, dict):
            message = task_error.get("message") or task_error.get("detail") or "Generation failed"
        elif isinstance(task_error, str):
            message = task_error
        else:
            message = _pick_first(containers, ["message", "error_message", "fail_reason"]) or "Generation failed"
    elif status == "running":
        message = f"Generating 3D {stage}..." if stage != "preview" else "Generating 3D preview..."
    elif status == "pending":
        message = "Waiting for provider..."
    else:
        message = "Processing..."

    return {
        "id": _pick_first(containers, ["id", "task_id"]),
        "status": status,
        "pct": pct,
        "stage": stage,
        "message": message,
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

    # Extract error message from Meshy's response for failed tasks
    error_message = None
    if status == "failed":
        # Meshy returns errors in various formats
        task_error = _pick_first(containers, ["task_error", "error"])
        if isinstance(task_error, dict):
            error_message = task_error.get("message") or task_error.get("detail") or str(task_error)
        elif isinstance(task_error, str):
            error_message = task_error
        if not error_message:
            error_message = _pick_first(containers, ["message", "error_message", "fail_reason"])
        task_id = _pick_first(containers, ["id", "task_id"])
        print(f"[MESHY_TASK_FAILED] task_id={task_id} stage={stage} error={error_message} raw_status={st_raw}")

    result = {
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
        # Parent job reference for derived jobs (texture/remesh/rig) - used for metadata inheritance
        "original_job_id": _pick_first(containers, ["original_job_id", "source_task_id", "preview_task_id", "input_task_id"]),
    }
    if error_message:
        result["message"] = error_message
    return result


def build_source_payload(body: dict, identity_id: str | None = None, *, prefer: str = "input_task_id"):
    """Validate and build the source payload for Meshy operations.

    Returns a dict with EXACTLY ONE key: either {"input_task_id": ...} or
    {"model_url": ...}.  Never both — Meshy silently prefers input_task_id
    when both are present, and a stale/wrong one causes async failures even
    when model_url is valid.

    Args:
        prefer: Which source to prefer when both are available.
            "input_task_id" — best for retexture/refine (model is already
                in Meshy's system, preserves UV/geometry metadata).
            "model_url" — best for rigging/remesh or when task ID is
                expired/unavailable.
    """
    input_task_id = (body.get("input_task_id") or "").strip()
    model_url = (body.get("model_url") or "").strip()
    if input_task_id and model_url:
        print(f"[build_source_payload] Both provided, prefer={prefer} input_task_id={input_task_id}")
    if not input_task_id and not model_url:
        return None, "input_task_id or model_url required"

    # When caller prefers model_url and we have one, skip input_task_id entirely
    if prefer == "model_url" and model_url:
        input_task_id = ""  # go straight to model_url path below

    if input_task_id:
        # Resolve internal IDs to original Meshy task IDs when needed.
        try:
            from backend.services.job_service import resolve_meshy_job_id, verify_job_ownership
            resolved = resolve_meshy_job_id(input_task_id)
            if identity_id and not verify_job_ownership(resolved, identity_id):
                if model_url:
                    print(f"[build_source_payload] input_task_id={input_task_id} ownership check failed, falling back to model_url")
                else:
                    return None, "Job not found or access denied"
            else:
                print(f"[RIG_SOURCE] resolved: input_task_id={resolved} (original={input_task_id})")
                return {"input_task_id": resolved}, None
        except Exception as e:
            if model_url:
                print(f"[build_source_payload] input_task_id resolution failed ({e}), falling back to model_url")
            else:
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
            # Collect identity IDs to check (current + any that merged into current)
            identity_ids = [identity_id]
            try:
                with get_conn() as conn:
                    with conn.cursor(row_factory=dict_row) as cur:
                        cur.execute(
                            f"SELECT id::text FROM {Tables.IDENTITIES} WHERE merged_into_id = %s",
                            (identity_id,),
                        )
                        identity_ids.extend(r["id"] for r in cur.fetchall())
            except Exception:
                pass  # proceed with just the current identity

            with get_conn() as conn:
                with conn.cursor(row_factory=dict_row) as cur:
                    # Check ownership in MODELS and HISTORY_ITEMS tables
                    # Include merged identities so models from pre-merge still pass
                    cur.execute(
                        f"""
                        SELECT 1
                        FROM {Tables.MODELS}
                        WHERE identity_id = ANY(%s) AND (glb_s3_key = %s OR glb_url = %s)
                        UNION
                        SELECT 1
                        FROM {Tables.HISTORY_ITEMS}
                        WHERE identity_id = ANY(%s) AND (
                            glb_url = %s
                            OR payload->>'glb_url' = %s
                            OR payload->>'model_url' = %s
                        )
                        LIMIT 1
                        """,
                        (identity_ids, s3_key, model_url, identity_ids, model_url, model_url, model_url),
                    )
                    row = cur.fetchone()
            if not row:
                print(f"[build_source_payload] S3 ownership check failed: identity={identity_id} ids_checked={identity_ids} s3_key={s3_key}")
                return None, "Model URL not found or access denied"
            signed = presign_s3_url(model_url)
            if signed:
                model_url = signed
    except Exception as e:
        print(f"[Meshy] build_source_payload ownership/sign check failed: {e}")

    print(f"[RIG_SOURCE] fallback: model_url={'s3' if 's3.amazonaws' in model_url else 'other'}({model_url[:60]})")
    return {"model_url": model_url}, None