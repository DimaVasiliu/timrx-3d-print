"""
Prompt Enhancement Route Blueprint
-----------------------------------
Registered under /api/_mod.
"""

from __future__ import annotations

from flask import Blueprint, jsonify, request

from backend.middleware import with_session
from backend.services.prompt_enhance_service import (
    enhance_prompt,
    PromptEnhanceError,
)
from backend.services.prompt_safety_service import check_prompt_safety, get_safety_analytics

bp = Blueprint("prompt_enhance", __name__)

VALID_MODES = {"model", "image", "video", "texture"}


@bp.route("/prompt-enhance", methods=["POST", "OPTIONS"])
@with_session
def prompt_enhance():
    """
    Enhance a user prompt for a specific generation mode.

    Request body:
    {
        "prompt": "user raw prompt",
        "mode": "model" | "image" | "video"
    }

    Response (success):
    {
        "ok": true,
        "original_prompt": "...",
        "enhanced_prompt": "...",
        "mode": "model"
    }

    Response (error):
    {
        "ok": false,
        "error": "message"
    }
    """
    if request.method == "OPTIONS":
        return ("", 204)

    body = request.get_json(silent=True) or {}

    raw_prompt = (body.get("prompt") or "").strip()
    mode = (body.get("mode") or "").strip().lower()
    provider = (body.get("provider") or "vertex").strip().lower()

    # Validate mode
    if mode not in VALID_MODES:
        return jsonify({
            "ok": False,
            "error": f"Invalid mode. Must be one of: {', '.join(sorted(VALID_MODES))}",
        }), 400

    # Validate prompt
    if not raw_prompt:
        return jsonify({
            "ok": False,
            "error": "Prompt is empty",
        }), 400

    if len(raw_prompt) > 2000:
        return jsonify({
            "ok": False,
            "error": "Prompt exceeds 2000 character limit",
        }), 400

    # ── Prompt safety preflight ──
    from flask import g
    user_id = getattr(g, "identity_id", None)
    medium = "video" if mode == "video" else ("image" if mode == "image" else "text")
    safety = check_prompt_safety(raw_prompt, medium=medium, provider=provider, user_id=user_id)
    if safety["decision"] in ("block", "warn"):
        status_code = 451 if safety["decision"] == "block" else 422
        return jsonify({
            "ok": False,
            "error": "prompt_safety",
            "safety": safety,
        }), status_code

    try:
        enhanced = enhance_prompt(raw_prompt, mode, provider=provider)
        return jsonify({
            "ok": True,
            "original_prompt": raw_prompt,
            "enhanced_prompt": enhanced,
            "mode": mode,
        })
    except PromptEnhanceError as e:
        err_str = str(e)
        # Only surface user-input validation errors; hide provider details
        if any(kw in err_str for kw in ("empty", "exceeds", "Unsupported mode")):
            safe_msg = err_str
        else:
            print(f"[PROVIDER_ERROR] provider=openai context=prompt_enhance error={err_str}")
            safe_msg = "Prompt enhancement failed. Please try again."
        return jsonify({
            "ok": False,
            "error": safe_msg,
        }), 400
    except Exception as e:
        print(f"[PromptEnhance] Unexpected error: {e}")
        return jsonify({
            "ok": False,
            "error": "Prompt enhancement is temporarily unavailable. Please try again.",
        }), 503


@bp.route("/safety-check", methods=["POST", "OPTIONS"])
@with_session
def safety_check_dry_run():
    """
    Dry-run safety check endpoint.
    Evaluates a prompt against the safety rules and returns the full
    debug breakdown WITHOUT recording strikes or applying penalties.

    Request body:
    {
        "prompt": "...",
        "medium": "video" | "image" | "text",   (default: "video")
        "provider": "vertex" | "seedance" | ...  (default: "vertex")
    }

    Response:
    {
        "ok": true,
        "decision": "allow" | "warn" | "block",
        "categories": [...],
        "message": "...",
        "rewrite_hint": "...",
        "debug": {
            "scores": { "violence": 0, ... },
            "matched_rules": ["V01_shoot_person", ...],
            "matched_details": [...],
            "safe_context_count": 5,
            "has_harmful_verbs": false,
            "safe_reduced": true,
            "multiplier": 1.56,
            "thresholds_used": { ... }
        }
    }
    """
    if request.method == "OPTIONS":
        return ("", 204)

    body = request.get_json(silent=True) or {}

    raw_prompt = (body.get("prompt") or "").strip()
    if not raw_prompt:
        return jsonify({"ok": False, "error": "prompt is required"}), 400

    medium   = (body.get("medium") or "video").strip().lower()
    provider = (body.get("provider") or "vertex").strip().lower()

    result = check_prompt_safety(raw_prompt, medium=medium, provider=provider, dry_run=True)

    return jsonify({
        "ok": True,
        **result,
    })


@bp.route("/safety-analytics", methods=["GET", "OPTIONS"])
@with_session
def safety_analytics():
    """
    Admin/debug endpoint: aggregate safety data.

    Query params:
        hours  – lookback window (default 24)

    Response:
    {
        "ok": true,
        "period_hours": 24,
        "blocks_by_category": { "_total": 12 },
        "warns_by_category": { "_total": 5 },
        "false_negative_candidates": 3,
        "top_rejection_providers": { "vertex": 2 },
        "top_matched_rules": { "V01_shoot_person": 4 },
        "total_strikes": 17
    }
    """
    if request.method == "OPTIONS":
        return ("", 204)

    hours = int(request.args.get("hours", 24))
    data = get_safety_analytics(hours=hours)
    return jsonify({"ok": True, **data})
