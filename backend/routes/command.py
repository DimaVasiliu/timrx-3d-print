"""Authenticated natural-language command bar: plan, quote, execute."""

from __future__ import annotations

import base64
import binascii
import hashlib
import hmac
import json
import os
import time
import uuid

import requests
from flask import Blueprint, g, jsonify, request

from backend.config import config
from backend.middleware import require_session
from backend.services.command_planner import (
    CommandPlanError,
    normalize_plan,
    provider_availability,
    quote_plan,
)
from backend.services.expense_guard import ExpenseGuard

bp = Blueprint("command", __name__)

_OPENAI_CHAT_URL = "https://api.openai.com/v1/chat/completions"
_PLANNER_MODEL = os.getenv("COMMAND_PLANNER_MODEL", "gpt-4o-mini")
_PLAN_TTL_SECONDS = 10 * 60


def _token_secret() -> bytes:
    return (config.CSRF_SECRET or "").encode("utf-8")


def _sign_token(payload: dict) -> str:
    raw = json.dumps(payload, separators=(",", ":"), sort_keys=True).encode("utf-8")
    encoded = base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")
    signature = hmac.new(_token_secret(), encoded.encode("ascii"), hashlib.sha256).digest()
    return encoded + "." + base64.urlsafe_b64encode(signature).decode("ascii").rstrip("=")


def _read_token(token: str, identity_id: str) -> dict:
    try:
        encoded, supplied_signature = token.split(".", 1)
        expected = hmac.new(_token_secret(), encoded.encode("ascii"), hashlib.sha256).digest()
        actual = base64.urlsafe_b64decode(supplied_signature + "=" * (-len(supplied_signature) % 4))
        if not hmac.compare_digest(expected, actual):
            raise ValueError("invalid signature")
        payload = json.loads(base64.urlsafe_b64decode(encoded + "=" * (-len(encoded) % 4)))
        if payload.get("identity_id") != identity_id or int(payload.get("exp", 0)) < int(time.time()):
            raise ValueError("expired plan")
        if not isinstance(payload.get("plan"), dict):
            raise ValueError("missing plan")
        return payload["plan"]
    except (ValueError, TypeError, KeyError, json.JSONDecodeError, binascii.Error, UnicodeDecodeError) as exc:
        raise CommandPlanError("This generation plan expired. Press Enter again to refresh it.") from exc


def _new_token(plan: dict, identity_id: str) -> str:
    return _sign_token({
        "identity_id": identity_id,
        "exp": int(time.time()) + _PLAN_TTL_SECONDS,
        "plan": plan,
    })


def _planner_messages(text: str) -> list[dict]:
    system = """You are the TimrX command planner. Return JSON only.
Choose exactly one intent: image, model, or video. Extract the user's requested
provider/model and settings into the settings object when present.
Provider aliases: Nano Banana -> nano_banana; OpenAI/GPT image -> openai;
Gemini/Imagen -> google; Veo/Google Veo -> vertex; Seedance 1.5 ->
fal_seedance; Seedance 2.0 -> seedance tier fast; Seedance 2.5 -> seedance
tier v25; Meshy 5 -> meshy-5; Meshy 6/latest -> latest.
Allowed settings keys: provider, model, image_size, aspect_ratio, duration,
duration_seconds, video_duration_seconds, resolution, video_resolution, tier,
video_tier, audio, video_mode, pose_mode, symmetry_mode, model_type.
Use values from the request. Do not invent a provider. Keep creative wording
in prompt and remove only technical settings from it.
Schema: {"intent":"image|model|video","prompt":"...","settings":{}}"""
    return [{"role": "system", "content": system}, {"role": "user", "content": text}]


def _extract_message_content(data: dict) -> str:
    choices = data.get("choices") or []
    if choices and isinstance(choices[0], dict):
        message = choices[0].get("message") or {}
        return str(message.get("content") or "")
    return ""


def _ai_plan(text: str) -> tuple[dict, str]:
    """Ask the server-side planner; use deterministic normalization if absent."""
    if not config.OPENAI_API_KEY:
        return {}, "local-fallback"
    response = requests.post(
        _OPENAI_CHAT_URL,
        headers={"Authorization": f"Bearer {config.OPENAI_API_KEY}", "Content-Type": "application/json"},
        json={
            "model": _PLANNER_MODEL,
            "messages": _planner_messages(text),
            "temperature": 0,
            "response_format": {"type": "json_object"},
        },
        timeout=(8, 25),
    )
    response.raise_for_status()
    content = _extract_message_content(response.json())
    try:
        return json.loads(content), "openai"
    except (TypeError, json.JSONDecodeError) as exc:
        raise CommandPlanError("The assistant returned an invalid generation plan. Please try again.") from exc


def _plan_from_text(text: str) -> tuple[dict, str]:
    try:
        raw, source = _ai_plan(text)
        return normalize_plan(text, raw), source
    except requests.RequestException as exc:
        # A temporary planner outage should not make the deterministic setting
        # recognizer unusable. The response identifies the fallback source.
        print(f"[COMMAND] planner request failed, using local parser: {exc}")
        return normalize_plan(text, {}), "local-fallback"


def _quote_payload(plan: dict) -> dict:
    quote = quote_plan(plan)
    available, availability_error = provider_availability(plan)
    quote["available"] = available
    quote["availability_error"] = availability_error
    return quote


def _error(exc: Exception, status: int = 422):
    return jsonify({"ok": False, "error": "command_plan_invalid", "message": str(exc)}), status


@bp.route("/command/plan", methods=["POST", "OPTIONS"])
@require_session
def command_plan():
    if request.method == "OPTIONS":
        return ("", 204)
    body = request.get_json(silent=True) or {}
    text = str(body.get("text") or body.get("prompt") or "").strip()
    if not 3 <= len(text) <= 2000:
        return _error(CommandPlanError("Enter a request between 3 and 2,000 characters."), 400)
    try:
        plan, source = _plan_from_text(text)
        quote = _quote_payload(plan)
        return jsonify({
            "ok": True,
            "plan": plan,
            "quote": quote,
            "plan_token": _new_token(plan, str(g.identity_id)),
            "planner": source,
            "expires_in": _PLAN_TTL_SECONDS,
        })
    except CommandPlanError as exc:
        return _error(exc)
    except requests.RequestException:
        return _error(CommandPlanError("The planning service is temporarily unavailable. Please try again."), 503)
    except Exception as exc:
        print(f"[COMMAND] plan failed: {type(exc).__name__}: {exc}")
        return _error(CommandPlanError("Could not prepare that generation."), 500)


@bp.route("/command/quote", methods=["POST", "OPTIONS"])
@require_session
def command_quote():
    if request.method == "OPTIONS":
        return ("", 204)
    body = request.get_json(silent=True) or {}
    try:
        if body.get("plan_token"):
            plan = _read_token(str(body["plan_token"]), str(g.identity_id))
        elif isinstance(body.get("plan"), dict):
            plan = normalize_plan(str(body["plan"].get("prompt") or ""), body["plan"])
        else:
            raise CommandPlanError("A generation plan is required.")
        quote = _quote_payload(plan)
        return jsonify({"ok": True, "plan": plan, "quote": quote, "plan_token": _new_token(plan, str(g.identity_id))})
    except CommandPlanError as exc:
        return _error(exc)
    except Exception as exc:
        print(f"[COMMAND] quote failed: {type(exc).__name__}: {exc}")
        return _error(CommandPlanError("Could not price that generation."), 500)


def _cacheable_response(response) -> dict | None:
    if not hasattr(response, "status_code") or response.status_code >= 400:
        return None
    data = response.get_json(silent=True)
    return data if isinstance(data, dict) and data.get("ok") else None


def _execute_image(plan: dict, idempotency_key: str):
    from backend.routes.image_gen import (
        _handle_flux_pro_image_generate,
        _handle_gemini_image_generate,
        _handle_google_nano_image_generate,
        _handle_ideogram_v3_image_generate,
        _handle_nano_banana_image_generate,
        _handle_openai_image_generate,
        _handle_recraft_v4_image_generate,
    )
    body = {
        "provider": plan["provider"], "prompt": plan["prompt"],
        "model": plan["model"], "image_size": plan["image_size"],
        "aspect_ratio": plan["aspect_ratio"], "size": plan["size"],
        "idempotency_key": idempotency_key,
    }
    handlers = {
        "nano_banana": _handle_nano_banana_image_generate,
        "google": _handle_gemini_image_generate,
        "google_nano": _handle_google_nano_image_generate,
        "flux_pro": _handle_flux_pro_image_generate,
        "ideogram_v3": _handle_ideogram_v3_image_generate,
        "recraft_v4": _handle_recraft_v4_image_generate,
        "openai": _handle_openai_image_generate,
    }
    return handlers[plan["provider"]](body)


def _execute_video(plan: dict, identity_id: str):
    from backend.routes.video import _dispatch_video_job
    task = "image2video" if plan.get("video_mode") in {"image2video", "animate_image"} else "text2video"
    return _dispatch_video_job(
        identity_id=identity_id,
        task=task,
        prompt=plan["prompt"],
        image_data=None,
        aspect_ratio=plan["aspect_ratio"],
        resolution=plan["video_resolution"],
        duration_seconds=plan["video_duration_seconds"],
        motion=plan.get("motion", ""),
        style_preset=plan.get("style_preset") or None,
        negative_prompt="",
        seed=None,
        provider=plan["provider"],
        seedance_variant=plan.get("seedance_variant"),
        seedance_tier=plan.get("video_tier", "fast"),
        seedance_less_restriction=plan.get("seedance_less_restriction", False),
        seedance_audio=plan.get("audio"),
        strict_provider=True,
    )


@bp.route("/command/execute", methods=["POST", "OPTIONS"])
@require_session
def command_execute():
    if request.method == "OPTIONS":
        return ("", 204)
    body = request.get_json(silent=True) or {}
    try:
        token = str(body.get("plan_token") or "")
        if not token:
            raise CommandPlanError("A generation plan is required.")
        plan = _read_token(token, str(g.identity_id))
        quote = _quote_payload(plan)
        if not quote["available"]:
            return jsonify({"ok": False, "error": "provider_unavailable", "message": quote["availability_error"], "quote": quote}), 409
        if quote["credits"] <= 0:
            return _error(CommandPlanError("This generation has no valid credit quote."), 422)

        client_key = str(request.headers.get("Idempotency-Key") or body.get("idempotency_key") or uuid.uuid4())
        idempotency_key = ExpenseGuard.compute_idempotency_key(
            str(g.identity_id), "command_execute", plan["prompt"],
            intent=plan["intent"], provider=plan["provider"], action_key=quote["action_key"], client_key=client_key,
        )
        cached = ExpenseGuard.is_duplicate_request(idempotency_key)
        if cached:
            return jsonify(cached)

        if plan["intent"] == "image":
            response = _execute_image(plan, idempotency_key)
        elif plan["intent"] == "model":
            from backend.routes.text_to_3d import text_to_3d_start_mod
            response = text_to_3d_start_mod.__wrapped__(
                body={"prompt": plan["prompt"], "model": plan["model"], "pose_mode": plan.get("pose_mode"),
                      "symmetry_mode": plan.get("symmetry_mode"), "model_type": plan.get("model_type")},
                identity_id=str(g.identity_id),
            )
        else:
            response = _execute_video(plan, str(g.identity_id))

        cached_response = _cacheable_response(response)
        if cached_response:
            ExpenseGuard.cache_response(idempotency_key, cached_response)
        return response
    except CommandPlanError as exc:
        return _error(exc)
    except Exception as exc:
        print(f"[COMMAND] execute failed: {type(exc).__name__}: {exc}")
        return jsonify({"ok": False, "error": "command_execute_failed", "message": "Generation could not be started."}), 500
