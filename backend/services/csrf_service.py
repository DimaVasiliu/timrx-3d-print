"""
Stateless CSRF protection for browser requests.

We use a double-submit pattern:
- timrx_sid stays HttpOnly
- timrx_csrf is readable by JavaScript
- frontend echoes timrx_csrf in X-CSRF-Token on state-changing requests

The token is derived from the session ID with HMAC, so no database storage is
required and rotating sessions automatically rotates CSRF tokens.
"""

from __future__ import annotations

import hashlib
import hmac
from typing import Iterable

from flask import request

from backend.config import config


class CSRFService:
    EXEMPT_PREFIXES = (
        "/api/billing/webhook",
        "/api/webhooks/",
        "/api/jobs/callback",
        "/api/auth/restore/request",
        "/api/auth/restore/redeem",
    )
    STATE_CHANGING_METHODS = {"POST", "PUT", "PATCH", "DELETE"}

    @staticmethod
    def issue_token(session_id: str | None) -> str | None:
        if not session_id:
            return None
        secret = config.CSRF_SECRET.encode("utf-8")
        digest = hmac.new(secret, session_id.encode("utf-8"), hashlib.sha256).hexdigest()
        return digest
 
    @staticmethod
    def set_csrf_cookie(response, session_id: str | None) -> None:
        token = CSRFService.issue_token(session_id)
        if not token:
            return

        cookie_kwargs = {
            "max_age": config.SESSION_TTL_SECONDS,
            "httponly": False,
            "secure": config.SESSION_COOKIE_SECURE,
            "samesite": config.SESSION_COOKIE_SAMESITE,
            "path": config.SESSION_COOKIE_PATH,
        }
        if config.SESSION_COOKIE_DOMAIN:
            cookie_kwargs["domain"] = config.SESSION_COOKIE_DOMAIN

        response.set_cookie(config.CSRF_COOKIE_NAME, token, **cookie_kwargs)

    @staticmethod
    def clear_csrf_cookie(response) -> None:
        if config.IS_PROD:
            response.delete_cookie(config.CSRF_COOKIE_NAME, path=config.SESSION_COOKIE_PATH)
        if config.SESSION_COOKIE_DOMAIN:
            response.delete_cookie(
                config.CSRF_COOKIE_NAME,
                path=config.SESSION_COOKIE_PATH,
                domain=config.SESSION_COOKIE_DOMAIN,
            )
        else:
            response.delete_cookie(config.CSRF_COOKIE_NAME, path=config.SESSION_COOKIE_PATH)

    @staticmethod
    def _parse_session_candidates() -> list[str]:
        cookie_name = config.SESSION_COOKIE_NAME
        raw_cookie = request.headers.get("Cookie", "")
        candidates: list[str] = []

        for part in raw_cookie.split(";"):
            chunk = part.strip()
            if not chunk.startswith(f"{cookie_name}="):
                continue
            value = chunk.split("=", 1)[1].strip()
            if value and value not in candidates:
                candidates.append(value)

        fallback = request.cookies.get(cookie_name)
        if fallback and fallback not in candidates:
            candidates.append(fallback)

        return candidates

    @staticmethod
    def _has_session_cookie(candidates: Iterable[str]) -> bool:
        for candidate in candidates:
            if candidate:
                return True
        return False

    @staticmethod
    def request_requires_protection() -> bool:
        if not config.CSRF_PROTECT:
            return False
        if request.method not in CSRFService.STATE_CHANGING_METHODS:
            return False
        if request.method == "OPTIONS":
            return False
        for prefix in CSRFService.EXEMPT_PREFIXES:
            if request.path.startswith(prefix):
                return False
        return True

    @staticmethod
    def validate_request() -> tuple[bool, str | None]:
        if not CSRFService.request_requires_protection():
            return True, None

        candidates = CSRFService._parse_session_candidates()
        if not CSRFService._has_session_cookie(candidates):
            # No authenticated browser session yet. Allow the bootstrap request.
            return True, None

        provided = request.headers.get(config.CSRF_HEADER_NAME, "").strip()
        if not provided:
            return False, "missing_csrf_token"

        for session_id in candidates:
            expected = CSRFService.issue_token(session_id)
            if expected and hmac.compare_digest(provided, expected):
                return True, None

        return False, "invalid_csrf_token"
