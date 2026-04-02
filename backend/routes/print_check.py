"""
Print-Readiness Check Route
----------------------------
POST /api/_mod/print-check/<job_id>
Free analysis (0 credits) to encourage the print workflow.
"""

import logging

from flask import Blueprint, jsonify, request

from backend.db import USE_DB, get_conn, Tables
from backend.middleware import with_session_readonly
from backend.services.identity_service import require_identity

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

    if not USE_DB:
        return jsonify({"error": "Database not configured"}), 503

    # Prefer the durable stored model URL first, then fall back through other saved Meshy URLs.
    model_url = None
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                # Check history_items first
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
                    ) AS model_url
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
        result = PrintAnalysisService.analyze_from_url(model_url)
        return jsonify(result)
    except ImportError:
        return jsonify({"error": "Print analysis not available (trimesh not installed)"}), 503
    except Exception as e:
        logger.exception("[PRINT_CHECK] Analysis failed for job_id=%s", job_id)
        return jsonify({"error": f"Analysis failed: {e}"}), 500
