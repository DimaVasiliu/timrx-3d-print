"""
Print-Readiness Check Route
----------------------------
POST /api/_mod/print-check/<job_id>
Free analysis (0 credits) to encourage the print workflow.
"""

import logging
import time
from collections import defaultdict

from flask import Blueprint, jsonify, request

from backend.db import USE_DB, get_conn, Tables
from backend.middleware import with_session_readonly
from backend.services.identity_service import require_identity

# Simple per-identity rate limiter: max 10 print checks per minute
_print_check_timestamps: dict[str, list[float]] = defaultdict(list)
PRINT_CHECK_RATE_LIMIT = 10  # requests
PRINT_CHECK_RATE_WINDOW = 60  # seconds
_last_cleanup = time.time()


def _check_rate_limit(identity_id: str) -> bool:
    """Return True if rate limit exceeded."""
    global _last_cleanup
    now = time.time()

    # Periodic cleanup: every 5 minutes, purge stale entries
    if now - _last_cleanup > 300:
        stale_keys = [k for k, v in _print_check_timestamps.items()
                      if not v or now - max(v) > PRINT_CHECK_RATE_WINDOW * 2]
        for k in stale_keys:
            del _print_check_timestamps[k]
        _last_cleanup = now

    timestamps = _print_check_timestamps[identity_id]
    _print_check_timestamps[identity_id] = [t for t in timestamps if now - t < PRINT_CHECK_RATE_WINDOW]
    if len(_print_check_timestamps[identity_id]) >= PRINT_CHECK_RATE_LIMIT:
        return True
    _print_check_timestamps[identity_id].append(now)
    return False

bp = Blueprint("print_check", __name__)
logger = logging.getLogger(__name__)


@bp.route("/print-check/<job_id>", methods=["POST", "OPTIONS"])
@with_session_readonly
def print_check(job_id: str):
    """Analyze a completed model for 3D printing readiness."""
    if request.method == "OPTIONS":
        return ("", 204)

    identity_id, auth_error = require_identity()
    if auth_error:
        return auth_error

    if _check_rate_limit(identity_id):
        return jsonify({"error": "Rate limit exceeded. Please wait before running another print check."}), 429

    body = request.get_json(silent=True) or {}
    printer_type = body.get("printer_type", "fdm")  # "fdm" or "resin"

    if not USE_DB:
        return jsonify({"error": "Database not configured"}), 503

    # Prefer the durable stored model URL first, then fall back through other saved Meshy URLs.
    model_url = None
    history_stage = None
    history_item_type = None
    lineage_origin_id = None
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                # Check history_items first — also grab stage for model-state enrichment
                cur.execute(
                    f"""
                    SELECT COALESCE(
                        glb_url,
                        payload->>'glb_url',
                        payload->>'textured_glb_url',
                        payload->'model_urls'->>'glb',
                        payload->'textured_model_urls'->>'glb',
                        payload->'model_urls'->>'stl',
                        payload->'textured_model_urls'->>'stl'
                    ) AS model_url,
                    stage,
                    item_type,
                    lineage_origin_id
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
                    if isinstance(row, tuple):
                        model_url = row[0]
                        history_stage = row[1]
                        history_item_type = row[2]
                        lineage_origin_id = row[3]
                    else:
                        model_url = row.get("model_url")
                        history_stage = row.get("stage")
                        history_item_type = row.get("item_type")
                        lineage_origin_id = row.get("lineage_origin_id")

                # Fallback: check models table
                if not model_url:
                    cur.execute(
                        f"""
                        SELECT COALESCE(
                            glb_url,
                            meta->>'glb_url',
                            meta->>'textured_glb_url',
                            meta->'model_urls'->>'glb',
                            meta->'textured_model_urls'->>'glb',
                            meta->'model_urls'->>'stl',
                            meta->'textured_model_urls'->>'stl'
                        ) AS model_url
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
    except Exception as e:
        return jsonify({"error": f"Database lookup failed: {e}"}), 500

    if not model_url:
        return jsonify({"error": "Model not found or no printable model URL available"}), 404

    # Run analysis
    try:
        from backend.services.print_analysis_service import PrintAnalysisService
        result = PrintAnalysisService.analyze_from_url(model_url, printer_type=printer_type)

        # ── Enrich result with model-stage metadata ───────────────
        # Use the stage column from history_items (already fetched above)
        model_stage = "unknown"
        is_remeshed = False
        is_refined = False
        source_action = None
        try:
            stage_val = (history_stage or "").strip().lower()
            if stage_val:
                source_action = stage_val
                if stage_val in ("remeshed", "remesh"):
                    model_stage = "remeshed"
                    is_remeshed = True
                elif stage_val in ("refined", "refine"):
                    model_stage = "refined"
                    is_refined = True
                elif stage_val == "preview":
                    model_stage = "preview"
                elif stage_val in ("image3d", "image_to_3d"):
                    model_stage = "image3d"
                elif stage_val in ("retextured", "retexture"):
                    model_stage = "retextured"
                else:
                    model_stage = stage_val

            # If history_items didn't have a stage, try the billing jobs table as fallback
            if model_stage == "unknown" and USE_DB:
                with get_conn() as conn:
                    with conn.cursor() as cur:
                        cur.execute("""
                            SELECT meta->>'stage'  AS stage,
                                   action_key
                              FROM timrx_billing.jobs
                             WHERE upstream_job_id = %s
                             ORDER BY created_at DESC
                             LIMIT 1
                        """, (job_id,))
                        row = cur.fetchone()
                        if row:
                            jstage = (row[0] if isinstance(row, tuple) else row.get("stage")) or ""
                            jaction = (row[1] if isinstance(row, tuple) else row.get("action_key")) or ""
                            source_action = jaction or jstage
                            if "remesh" in jaction:
                                model_stage = "remeshed"
                                is_remeshed = True
                            elif "refine" in jaction or jstage == "refine":
                                model_stage = "refined"
                                is_refined = True
                            elif jstage == "preview":
                                model_stage = "preview"
                            elif jstage == "image3d":
                                model_stage = "image3d"
                            elif "retexture" in jaction:
                                model_stage = "retextured"
                            else:
                                model_stage = jstage or "unknown"
        except Exception:
            pass  # Non-critical enrichment; don't fail the response

        result["model_stage"] = model_stage
        result["is_remeshed"] = is_remeshed
        result["is_refined"] = is_refined
        result["source_action"] = source_action

        return jsonify(result)
    except ImportError:
        return jsonify({"error": "Print analysis not available (trimesh not installed)"}), 503
    except Exception as e:
        logger.exception("[PRINT_CHECK] Analysis failed for job_id=%s", job_id)
        return jsonify({"error": f"Analysis failed: {e}"}), 500
