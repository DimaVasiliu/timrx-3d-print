"""
Print-Readiness Check Route
----------------------------
POST /api/_mod/print-check/<job_id>
Free analysis (0 credits) to encourage the print workflow.
"""

from flask import Blueprint, jsonify, g, request

from backend.db import USE_DB, get_conn, Tables
from backend.middleware import with_session_readonly
from backend.services.identity_service import require_identity

bp = Blueprint("print_check", __name__)


@bp.route("/print-check/<job_id>", methods=["POST", "OPTIONS"])
@with_session_readonly
def print_check(job_id: str):
    """Analyze a completed model for 3D printing readiness."""
    if request.method == "OPTIONS":
        return ("", 204)

    identity_id = require_identity()
    if not identity_id:
        return jsonify({"error": "Authentication required"}), 401

    if not USE_DB:
        return jsonify({"error": "Database not configured"}), 503

    # Look up the GLB URL for this job from history or models
    glb_url = None
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                # Check history_items first
                cur.execute(
                    f"""
                    SELECT payload->>'glb_url' as glb_url
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
                    glb_url = row[0] if isinstance(row, tuple) else row.get("glb_url")

                # Fallback: check models table
                if not glb_url:
                    cur.execute(
                        f"""
                        SELECT glb_url
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
                        glb_url = row[0] if isinstance(row, tuple) else row.get("glb_url")
    except Exception as e:
        return jsonify({"error": f"Database lookup failed: {e}"}), 500

    if not glb_url:
        return jsonify({"error": "Model not found or no GLB URL available"}), 404

    # Run analysis
    try:
        from backend.services.print_analysis_service import PrintAnalysisService
        result = PrintAnalysisService.analyze_from_url(glb_url)
        return jsonify(result)
    except ImportError:
        return jsonify({"error": "Print analysis not available (trimesh not installed)"}), 503
    except Exception as e:
        return jsonify({"error": f"Analysis failed: {e}"}), 500
