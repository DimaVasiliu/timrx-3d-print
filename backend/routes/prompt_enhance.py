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

    try:
        enhanced = enhance_prompt(raw_prompt, mode)
        return jsonify({
            "ok": True,
            "original_prompt": raw_prompt,
            "enhanced_prompt": enhanced,
            "mode": mode,
        })
    except PromptEnhanceError as e:
        return jsonify({
            "ok": False,
            "error": str(e),
        }), 400
    except Exception as e:
        print(f"[PromptEnhance] Unexpected error: {e}")
        return jsonify({
            "ok": False,
            "error": "Prompt enhancement is temporarily unavailable. Please try again.",
        }), 503
