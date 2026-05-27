"""
STL repair routes.

POST /api/_mod/stl-repair/<job_id>
Repairs an existing owned model and returns a signed repaired STL URL.
"""

from __future__ import annotations

import hashlib
import re
import time
from collections import defaultdict

from flask import Blueprint, jsonify, request

from backend.db import USE_DB, get_conn, Tables
from backend.middleware import with_session
from backend.services import s3_service
from backend.services.identity_service import require_identity
from backend.services.stl_repair_service import StlRepairService

bp = Blueprint("stl_repair", __name__)

_repair_timestamps: dict[str, list[float]] = defaultdict(list)
STL_REPAIR_RATE_LIMIT = 2
STL_REPAIR_RATE_WINDOW = 60


def _check_rate_limit(identity_id: str) -> bool:
    now = time.time()
    timestamps = [t for t in _repair_timestamps[identity_id] if now - t < STL_REPAIR_RATE_WINDOW]
    if len(timestamps) >= STL_REPAIR_RATE_LIMIT:
        _repair_timestamps[identity_id] = timestamps
        return True
    timestamps.append(now)
    _repair_timestamps[identity_id] = timestamps
    return False


def _safe_filename_stem(value: str | None, fallback: str = "repaired-model") -> str:
    raw = str(value or "").strip() or fallback
    raw = re.sub(r"\.[a-zA-Z0-9]{1,8}$", "", raw)
    raw = re.sub(r"[^A-Za-z0-9._-]+", "-", raw).strip(".-_")
    return (raw or fallback)[:80]


def _resolve_owned_model_url(job_id: str, identity_id: str, fallback_model_url: str = "") -> tuple[str | None, str | None]:
    model_url = None
    title = None

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT COALESCE(
                    payload->'model_urls'->>'stl',
                    payload->'textured_model_urls'->>'stl',
                    glb_url,
                    payload->>'glb_url',
                    payload->>'textured_glb_url',
                    payload->'model_urls'->>'glb',
                    payload->'textured_model_urls'->>'glb'
                ) AS model_url,
                title
                FROM {Tables.HISTORY_ITEMS}
                WHERE (id = %s OR payload->>'original_job_id' = %s)
                  AND identity_id = %s
                ORDER BY created_at DESC
                LIMIT 1
                """,
                (job_id, job_id, identity_id),
            )
            row = cur.fetchone()
            if row:
                model_url = row[0] if isinstance(row, tuple) else row.get("model_url")
                title = row[1] if isinstance(row, tuple) else row.get("title")

            if not model_url:
                cur.execute(
                    f"""
                    SELECT COALESCE(
                        meta->'model_urls'->>'stl',
                        meta->'textured_model_urls'->>'stl',
                        glb_url,
                        meta->>'glb_url',
                        meta->>'textured_glb_url',
                        meta->'model_urls'->>'glb',
                        meta->'textured_model_urls'->>'glb'
                    ) AS model_url,
                    title
                    FROM {Tables.MODELS}
                    WHERE (id = %s OR upstream_job_id = %s)
                      AND identity_id = %s
                    ORDER BY created_at DESC
                    LIMIT 1
                    """,
                    (job_id, job_id, identity_id),
                )
                row = cur.fetchone()
                if row:
                    model_url = row[0] if isinstance(row, tuple) else row.get("model_url")
                    title = row[1] if isinstance(row, tuple) else row.get("title")

    if not model_url and fallback_model_url:
        model_url = fallback_model_url
    return model_url, title


@bp.route("/stl-repair/<job_id>", methods=["POST", "OPTIONS"])
@with_session
def repair_existing_model(job_id: str):
    if request.method == "OPTIONS":
        return ("", 204)

    identity_id, auth_error = require_identity()
    if auth_error:
        return auth_error

    if _check_rate_limit(identity_id):
        return jsonify({
            "ok": False,
            "error": "Rate limit exceeded. Please wait before repairing another model.",
        }), 429

    if not USE_DB:
        return jsonify({"ok": False, "error": "Database not configured"}), 503

    body = request.get_json(silent=True) or {}
    fallback_model_url = (body.get("model_url") or "").strip()

    try:
        model_url, title = _resolve_owned_model_url(job_id, identity_id, fallback_model_url=fallback_model_url)
    except Exception as exc:
        return jsonify({"ok": False, "error": f"Database lookup failed: {exc}"}), 500

    if not model_url:
        return jsonify({"ok": False, "error": "Model not found or no repairable model URL available"}), 404

    result = StlRepairService.repair_from_url(model_url)
    if not result.get("ok"):
        return jsonify({
            "ok": False,
            "error": result.get("error") or "STL repair failed",
            "suggestions": result.get("suggestions") or [],
            "report": {
                "repair_runtime_seconds": result.get("repair_runtime_seconds"),
            },
        }), 422

    stl_bytes = result.pop("stl_bytes", None)
    if not stl_bytes:
        return jsonify({"ok": False, "error": "Repair did not produce an STL file"}), 500

    stem = _safe_filename_stem(body.get("filename") or title)
    digest = hashlib.sha256(stl_bytes).hexdigest()[:12]
    key = f"models/stl-repairs/{identity_id}/{stem}-{digest}.stl"

    try:
        upload = s3_service.upload_bytes_to_s3(
            data_bytes=stl_bytes,
            content_type="model/stl",
            prefix="models",
            key=key,
            return_hash=True,
        )
        repaired_url = upload.get("url")
        s3_key = upload.get("key") or key
        download_url = s3_service.presign_s3_key(s3_key, expires_in=3600)
    except Exception as exc:
        return jsonify({"ok": False, "error": f"Could not store repaired STL: {exc}"}), 500

    return jsonify({
        "ok": True,
        "filename": f"{stem}-repaired.stl",
        "repaired_url": repaired_url,
        "download_url": download_url or repaired_url,
        "expires_in": 3600 if download_url else None,
        "report": result,
    })
