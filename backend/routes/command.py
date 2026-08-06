"""Authenticated natural-language command bar: plan, quote, execute."""

from __future__ import annotations

import base64
import binascii
import hashlib
import hmac
import json
import os
import time
import traceback
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
from backend.services.free_generation_service import (
    get_current_trial_state,
    has_paid_balance,
    mark_trial_failed,
    reserve_trial,
)
from backend.services.turnstile_service import is_turnstile_enabled, verify_turnstile_token

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
    quote["availability_error"] = _public_availability_message(plan, availability_error)
    return quote


def _generation_type(plan: dict) -> str:
    return "3d" if plan.get("intent") == "model" else str(plan.get("intent") or "image")


def _apply_free_offer(plan: dict) -> dict:
    """Pin an unused free entitlement to the advertised bounded-cost product."""
    generation_type = _generation_type(plan)
    state = get_current_trial_state(generation_type)
    if not state["entitlements"][generation_type].get("remaining") or not _free_type_enabled(generation_type):
        return plan
    pinned = dict(plan)
    if generation_type == "image":
        from backend.services.image_provider_registry import get_image_provider_spec

        spec = get_image_provider_spec("nano_banana")
        pinned.update({"provider": "nano_banana", "model": spec.model if spec else "nano-banana-2", "image_size": "2K"})
    elif generation_type == "video":
        pinned.update({
            "provider": "seedance", "model": "seedance-2", "video_tier": "fast",
            "video_duration_seconds": 5, "video_resolution": "480p",
            "seedance_variant": None, "video_mode": "text2video",
        })
    return pinned


def _free_cost_limit(generation_type: str) -> int:
    return int({
        "image": getattr(config, "HOMEPAGE_FREE_IMAGE_MAX_CREDITS", 12),
        "video": getattr(config, "HOMEPAGE_FREE_VIDEO_MAX_CREDITS", 80),
        "3d": getattr(config, "HOMEPAGE_FREE_3D_MAX_CREDITS", 20),
    }.get(generation_type, 0) or 0)


def _free_type_enabled(generation_type: str) -> bool:
    attr = {"image": "HOMEPAGE_FREE_ALLOW_IMAGE", "video": "HOMEPAGE_FREE_ALLOW_VIDEO", "3d": "HOMEPAGE_FREE_ALLOW_3D"}[generation_type]
    return bool(
        getattr(config, "HOMEPAGE_FREE_ENABLED", False)
        and getattr(config, attr, False)
        and getattr(config, "TURNSTILE_ENABLED", False)
        and str(getattr(config, "TURNSTILE_SECRET_KEY", "") or "").strip()
        and os.getenv("FREE_GENERATION_HASH_SALT", "").strip()
    )


def _access_payload(plan: dict, quote: dict) -> dict:
    generation_type = _generation_type(plan)
    state = get_current_trial_state(generation_type)
    entitlement = state["entitlements"][generation_type]
    credits = int(quote.get("credits") or 0)
    free_available = bool(
        quote.get("available") and entitlement.get("remaining") and _free_type_enabled(generation_type)
        and (_free_cost_limit(generation_type) <= 0 or credits <= _free_cost_limit(generation_type))
    )
    paid_available = bool(
        quote.get("available") and has_paid_balance(str(g.identity_id), str(quote.get("action_key") or ""), credits)
    )
    return {
        "mode": "free" if free_available else ("paid" if paid_available else "blocked"),
        "challenge_required": bool(free_available and is_turnstile_enabled()),
        "generation_type": generation_type,
        "entitlement": entitlement,
        "has_credits": paid_available,
    }


def _public_availability_message(plan: dict, detail: str | None) -> str | None:
    """Keep vendor credentials and internal provider ids out of the UI."""
    if not detail:
        return None
    labels = {
        "image": "Image generation is temporarily unavailable. Try again shortly.",
        "model": "3D model generation is temporarily unavailable. Try again shortly.",
        "video": "Video generation is temporarily unavailable. Try again shortly.",
    }
    return labels.get(plan.get("intent"), "This generation option is temporarily unavailable. Try again shortly.")


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
        plan = _apply_free_offer(plan)
        quote = _quote_payload(plan)
        access = _access_payload(plan, quote)
        return jsonify({
            "ok": True,
            "plan": plan,
            "quote": quote,
            "access": access,
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
        plan = _apply_free_offer(plan)
        quote = _quote_payload(plan)
        return jsonify({"ok": True, "plan": plan, "quote": quote, "access": _access_payload(plan, quote), "plan_token": _new_token(plan, str(g.identity_id))})
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


def _public_failed_response(response, plan: dict):
    """Normalize provider failures before they leave the command endpoint."""
    status = 200
    payload = {}
    target = response
    if isinstance(response, tuple):
        target = response[0] if response else None
        if len(response) > 1 and isinstance(response[1], int):
            status = response[1]
    if hasattr(target, "status_code"):
        status = target.status_code
    if hasattr(target, "get_json"):
        payload = target.get_json(silent=True) or {}
    if status < 400:
        return response

    code = str(payload.get("error") or "")
    if code in {"insufficient_credits", "not_enough_credits", "credits_insufficient"}:
        message = "You do not have enough credits for this generation."
    elif status in {401, 403}:
        message = "Your workspace session expired. Refresh and try again."
    elif code == "prompt_safety":
        message = "This request cannot be generated as written. Try a different prompt."
    elif plan.get("intent") == "image":
        message = "Image generation could not be started. Try again shortly."
    elif plan.get("intent") == "video":
        message = "Video generation could not be started. Try again shortly."
    else:
        message = "3D model generation could not be started. Try again shortly."
    return jsonify({"ok": False, "error": "command_generation_failed", "message": message}), status


def _execute_image(plan: dict, idempotency_key: str):
    from backend.routes.image_gen import dispatch_image_provider
    body = {
        "provider": plan["provider"], "prompt": plan["prompt"],
        "model": plan["model"], "image_size": plan["image_size"],
        "aspect_ratio": plan["aspect_ratio"], "size": plan["size"],
        "idempotency_key": idempotency_key,
    }
    return dispatch_image_provider(body)


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

        from backend.services.prompt_safety_service import check_prompt_safety
        safety = check_prompt_safety(
            plan["prompt"], medium=_generation_type(plan), provider=plan["provider"], user_id=str(g.identity_id)
        )
        if safety["decision"] in {"block", "warn"}:
            return jsonify({"ok": False, "error": "prompt_safety", "safety": safety}), 451 if safety["decision"] == "block" else 422

        access = _access_payload(plan, quote)
        if access["mode"] == "blocked":
            return jsonify({
                "ok": False,
                "error": "insufficient_credits",
                "message": "Your free generation for this service has been used and your credit balance is too low.",
                "access": access,
            }), 402

        trial = None
        if access["mode"] == "free":
            if is_turnstile_enabled():
                remote_ip = request.headers.get("CF-Connecting-IP") if getattr(config, "HOMEPAGE_FREE_TRUST_PROXY_HEADERS", False) else request.remote_addr
                verification = verify_turnstile_token(
                    body.get("turnstile_token"), remote_ip=remote_ip, expected_action="free_generation"
                )
                if not verification.ok:
                    return jsonify({
                        "ok": False, "error": "turnstile_required",
                        "message": "Verify you are human to claim this free generation.",
                        "reason": verification.reason,
                    }), 403
            decision = reserve_trial(
                plan["prompt"], access["generation_type"],
                idempotency_key=str(request.headers.get("Idempotency-Key") or body.get("idempotency_key") or ""),
                max_daily_total=int(getattr(config, "HOMEPAGE_FREE_MAX_DAILY_TOTAL", 50)),
                max_per_ip_per_day=int(getattr(config, "HOMEPAGE_FREE_MAX_PER_IP_PER_DAY", 6)),
                max_attempts_per_type_per_day=int(getattr(config, "HOMEPAGE_FREE_MAX_ATTEMPTS_PER_TYPE_PER_DAY", 2)),
                source="workspace_command",
            )
            if not decision.allowed:
                status = 429 if decision.blocked_reason in {"homepage_free_daily_limit", "homepage_free_ip_limit", "free_attempt_limit"} else 409
                return jsonify({"ok": False, "error": decision.blocked_reason, "message": "This free generation cannot be claimed."}), status
            trial = decision.trial
            g.homepage_free_trial_id = str(trial["id"])
            g.homepage_free_generation_type = access["generation_type"]

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

        response = _public_failed_response(response, plan)
        response_status = response[1] if isinstance(response, tuple) and len(response) > 1 else getattr(response, "status_code", 200)
        if trial and int(response_status or 200) >= 400:
            mark_trial_failed(str(trial["id"]), "command_dispatch_failed")
        cached_response = _cacheable_response(response)
        if cached_response:
            ExpenseGuard.cache_response(idempotency_key, cached_response)
        return response
    except CommandPlanError as exc:
        return _error(exc)
    except Exception as exc:
        request_id = str(uuid.uuid4())
        print(f"[COMMAND] execute failed request_id={request_id}: {type(exc).__name__}: {exc}")
        print(traceback.format_exc())
        return jsonify({"ok": False, "error": "command_execute_failed", "message": "Generation could not be started."}), 500
