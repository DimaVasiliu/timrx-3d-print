"""
/api/auth routes - Authentication and session management.

Handles:
- POST /api/auth/restore/request - Request magic code via email
- POST /api/auth/restore/redeem - Verify magic code and restore session
- POST /api/auth/request-code - Alias for restore/request
- POST /api/auth/verify-code - Alias for restore/redeem
- GET /api/auth/status - Get current auth status
- POST /api/auth/email/attach - Attach email to current identity
- POST /api/auth/email/verify - Verify email on current identity

Note: Logout is handled by /api/me/logout for consistency with other /me endpoints.
"""

import time
import random
from functools import wraps

from flask import Blueprint, request, jsonify, g

from backend.middleware import with_session, with_optional_session, require_session, require_admin
from backend.services.magic_code_service import MagicCodeService
from backend.services.identity_service import IdentityService
from backend.services.wallet_service import WalletService

bp = Blueprint("auth", __name__)


# ─────────────────────────────────────────────────────────────
# Per-IP rate limiter for email attach endpoint
# In-process sliding window — no external dependencies.
# Limits each IP to ATTACH_IP_MAX_REQUESTS within ATTACH_IP_WINDOW_SECS.
# ─────────────────────────────────────────────────────────────

ATTACH_IP_WINDOW_SECS = 60
ATTACH_IP_MAX_REQUESTS = 10
_ip_attach_log = {}  # { ip: [timestamp, ...] }
_ip_attach_last_cleanup = 0


def _check_ip_attach_rate(ip: str) -> bool:
    """
    Return True if the IP is within rate limits, False if over.
    Also piggyback-cleans stale entries every 60 seconds.
    """
    global _ip_attach_last_cleanup

    now = time.time()
    cutoff = now - ATTACH_IP_WINDOW_SECS

    # Periodic cleanup — purge IPs with only stale timestamps (max once per window)
    if now - _ip_attach_last_cleanup > ATTACH_IP_WINDOW_SECS:
        stale_ips = [k for k, v in _ip_attach_log.items() if not v or v[-1] < cutoff]
        for k in stale_ips:
            del _ip_attach_log[k]
        _ip_attach_last_cleanup = now

    # Get or create entry for this IP
    timestamps = _ip_attach_log.get(ip)
    if timestamps is None:
        timestamps = []
        _ip_attach_log[ip] = timestamps

    # Trim expired entries for this IP
    while timestamps and timestamps[0] < cutoff:
        timestamps.pop(0)

    # Check limit
    if len(timestamps) >= ATTACH_IP_MAX_REQUESTS:
        return False

    # Record this request
    timestamps.append(now)
    return True


# ─────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────

def _normalize_email(raw_email) -> str:
    """
    Safely normalize email: strip whitespace, lowercase, handle None/non-string.
    Used by both attach and verify to ensure consistency.
    """
    if not raw_email:
        return ""
    if not isinstance(raw_email, str):
        return ""
    return raw_email.strip().lower()


def _mask_email(email: str) -> str:
    """Mask email for safe logging: show first 3 chars + ***"""
    if not email or len(email) < 3:
        return "***"
    return f"{email[:3]}***"


def _mask_code(code: str) -> str:
    """Mask code for safe logging: show only last 2 digits"""
    if not code or len(code) < 2:
        return "**"
    return f"****{code[-2:]}"


def _get_redeem_error_response(message: str):
    """Build error response for redeem failures."""
    error_code = "INVALID_CODE"
    status_code = 400

    if "too many" in message.lower():
        error_code = "TOO_MANY_ATTEMPTS"
    elif "expired" in message.lower():
        error_code = "CODE_EXPIRED"
    elif "session" in message.lower():
        error_code = "SESSION_ERROR"
        status_code = 500

    return jsonify({
        "error": {
            "code": error_code,
            "message": message,
        }
    }), status_code


def _get_client_ip() -> str:
    """Get client IP address from request, handling proxies."""
    # Check X-Forwarded-For header (set by proxies/load balancers)
    forwarded_for = request.headers.get("X-Forwarded-For")
    if forwarded_for:
        # Take the first IP in the chain (original client)
        return forwarded_for.split(",")[0].strip()
    # Fall back to remote_addr
    return request.remote_addr or ""


# ─────────────────────────────────────────────────────────────
# IDENT-3: Anti-enumeration timing equalization
# ─────────────────────────────────────────────────────────────

# Minimum execution time (seconds) for auth endpoints.
# Set to cover the typical slow path (code generation + email send).
_AUTH_MIN_RESPONSE_SECS = 0.35


def anti_enumeration(min_secs=_AUTH_MIN_RESPONSE_SECS):
    """
    IDENT-3: Decorator that equalizes response timing to prevent
    email-existence enumeration via timing side-channels.

    Ensures every request takes at least `min_secs` plus random jitter,
    so fast-path (unknown email) and slow-path (known email, code sent)
    are indistinguishable from the outside.
    """
    def decorator(f):
        @wraps(f)
        def wrapper(*args, **kwargs):
            start = time.monotonic()
            try:
                return f(*args, **kwargs)
            finally:
                elapsed = time.monotonic() - start
                remaining = min_secs - elapsed
                # Random jitter 0–50ms prevents statistical averaging
                jitter = random.uniform(0, 0.05)
                pad = max(0, remaining) + jitter
                if pad > 0:
                    time.sleep(pad)
        return wrapper
    return decorator


@bp.route("/restore/request", methods=["POST"])
@anti_enumeration()
def request_restore():
    """
    Request a magic code to restore account access.
    Sends a 6-digit code to the provided email if it exists.

    Request body:
    {
        "email": "user@example.com"
    }

    Response (always 200 to prevent email enumeration):
    {
        "ok": true,
        "message": "If this email is registered, a code has been sent"
    }

    Rate limited:
    - Max 3 active codes per email
    - 60 second cooldown between requests
    """
    data = request.get_json() or {}
    email = data.get("email", "").strip()

    if not email:
        return jsonify({
            "error": {
                "code": "VALIDATION_ERROR",
                "message": "email is required",
            }
        }), 400

    # Basic email validation
    if "@" not in email or "." not in email:
        return jsonify({
            "error": {
                "code": "VALIDATION_ERROR",
                "message": "Invalid email format",
            }
        }), 400

    # Get client IP for rate limiting
    ip_address = _get_client_ip()

    # Request the code
    result = MagicCodeService.request_restore(email, ip_address)

    if not result.get("ok"):
        message = result.get("message", "Request failed")
        # Rate limit errors should return 429
        if "wait" in message.lower() or "too many" in message.lower():
            return jsonify({
                "error": {
                    "code": "RATE_LIMITED",
                    "message": message,
                }
            }), 429

        return jsonify({
            "error": {
                "code": "REQUEST_FAILED",
                "message": message,
            }
        }), 400

    # IDENT-3: Always include resolved_email and merge_redirected to
    # normalize response shape (prevents enumeration via field presence).
    return jsonify({
        "ok": True,
        "message": result.get("message", "Code sent"),
        "resolved_email": result.get("resolved_email", None),
        "merge_redirected": bool(result.get("merge_redirected")),
    })


@bp.route("/restore/redeem", methods=["POST"])
@with_optional_session
@anti_enumeration()
def redeem_restore():
    """
    Verify a magic code and restore session.
    Links the current browser session (timrx_sid) to the identity with that email.

    Request body:
    {
        "email": "user@example.com",
        "code": "123456"
    }

    Response (success - 200):
    {
        "ok": true,
        "message": "Account restored successfully",
        "me": {
            "identity_id": "uuid",
            "email": "user@example.com",
            "email_verified": true
        },
        "wallet": {
            "balance": 80,
            "reserved": 0,
            "available": 80
        }
    }

    Response (failure - 400):
    {
        "error": {
            "code": "INVALID_CODE",
            "message": "Invalid or expired code"
        }
    }

    Requires: Active session (from cookie)
    """
    data = request.get_json() or {}
    email = data.get("email", "").strip()
    code = data.get("code", "").strip()

    # Validation
    if not email:
        return jsonify({
            "error": {
                "code": "VALIDATION_ERROR",
                "message": "email is required",
            }
        }), 400

    if not code:
        return jsonify({
            "error": {
                "code": "VALIDATION_ERROR",
                "message": "code is required",
            }
        }), 400

    if len(code) != 6 or not code.isdigit():
        return jsonify({
            "error": {
                "code": "VALIDATION_ERROR",
                "message": "Code must be 6 digits",
            }
        }), 400

    # AUTH-3: Get session ID from middleware — may be None if no cookie existed.
    # @with_optional_session validates without creating throwaway identities.
    session_id = g.session_id  # None if no existing session

    # Verify the code and link session (handles session_id=None safely)
    success, identity_id, message, detail = MagicCodeService.redeem_restore(
        email=email,
        code=code,
        session_id=session_id,
    )

    if not success or not identity_id:
        # IDENT-2: structured blocked-restore response (data conflict)
        if detail and detail.get("error_code") == "RESTORE_BLOCKED_DATA_CONFLICT":
            return jsonify({
                "ok": False,
                "error": {
                    "code": "RESTORE_BLOCKED_DATA_CONFLICT",
                    "message": message,
                    "next_step": detail.get("next_step", "contact_support"),
                },
            }), 409

        # IDENT-1: merge was attempted but blocked
        if detail and detail.get("error_code") == "RESTORE_MERGE_BLOCKED":
            # IDENT-3: blocked_reason logged in MagicCodeService, NOT sent to client
            print(
                f"[RESTORE] Merge blocked: {detail.get('blocked_reason', 'unknown')}"
            )
            return jsonify({
                "ok": False,
                "error": {
                    "code": "RESTORE_MERGE_BLOCKED",
                    "message": message,
                    "next_step": detail.get("next_step", "contact_support"),
                },
            }), 409

        return _get_redeem_error_response(message)

    # AUTH-3: If there was no pre-existing session, create one directly
    # for the restore target identity (no throwaway identity needed).
    resp_needs_cookie = False

    if not session_id:
        new_session_id = IdentityService.create_session_for_identity(
            identity_id,
            ip_address=_get_client_ip(),
            user_agent=request.headers.get("User-Agent", ""),
        )
        if new_session_id:
            session_id = new_session_id
            resp_needs_cookie = True
            print(
                f"[RESTORE] AUTH-3: Created session directly for target "
                f"{identity_id[:8]}... (no throwaway identity)"
            )

    # Fetch updated identity and wallet data
    identity = IdentityService.get_identity(identity_id)
    wallet = WalletService.get_wallet(identity_id)

    balance = wallet.get("balance_credits", 0) if wallet else 0
    reserved = WalletService.get_reserved_credits(identity_id)
    available = max(0, balance - reserved)

    # Format created_at safely
    created_at = None
    if identity and identity.get("created_at"):
        created_at = identity["created_at"].isoformat()

    result = jsonify({
        "ok": True,
        "message": message,
        "me": {
            "identity_id": identity_id,
            "email": identity.get("email") if identity else email,
            "email_verified": identity.get("email_verified", True) if identity else True,
            "created_at": created_at,
        },
        "wallet": {
            "balance": balance,
            "reserved": reserved,
            "available": available,
            "currency": "GBP",
        },
    })

    # Set cookie on the response if a new session was created
    if resp_needs_cookie and session_id:
        IdentityService.set_session_cookie(result, session_id)

    return result


@bp.route("/status", methods=["GET"])
@with_session
def auth_status():
    """
    Get current authentication status.
    Returns session info and whether user has email attached.

    Response:
    {
        "ok": true,
        "has_session": true,
        "identity_id": "uuid",
        "has_email": true,
        "email": "user@example.com",
        "email_verified": true
    }
    """
    identity = g.identity

    return jsonify({
        "ok": True,
        "has_session": bool(g.session_id),
        "identity_id": g.identity_id,
        "has_email": bool(identity.get("email")) if identity else False,
        "email": identity.get("email") if identity else None,
        "email_verified": identity.get("email_verified", False) if identity else False,
    })


@bp.route("/email/attach", methods=["POST"])
@with_session
@anti_enumeration()
def attach_email():
    """
    Attach an email to the current identity and send verification code.

    This endpoint allows anonymous users to "secure" their credits by attaching
    an email address. The email is stored but marked unverified until the user
    completes the magic code verification.

    Request body:
    {
        "email": "user@example.com"
    }

    Response (always 200 to prevent email enumeration):
    {
        "ok": true,
        "message": "If valid, a verification code has been sent"
    }

    Behavior:
    - If email is free: attach to current identity, send verification code
    - If email belongs to another VERIFIED identity: return generic success,
      but user must use restore flow to take over that account
    - If email already on current identity: resend verification code

    Rate limited: same as restore/request (60s cooldown, max 3 active codes)
    """
    data = request.get_json() or {}
    email = _normalize_email(data.get("email"))

    if not email:
        return jsonify({
            "error": {
                "code": "VALIDATION_ERROR",
                "message": "email is required",
            }
        }), 400

    # Basic email validation
    if "@" not in email or "." not in email:
        return jsonify({
            "error": {
                "code": "VALIDATION_ERROR",
                "message": "Invalid email format",
            }
        }), 400

    identity_id = g.identity_id
    if not identity_id:
        return jsonify({
            "error": {
                "code": "SESSION_REQUIRED",
                "message": "Active session required",
            }
        }), 401

    ip_address = _get_client_ip()

    # Per-IP rate limit (defense against enumeration across many emails)
    if not _check_ip_attach_rate(ip_address):
        print(f"[AUTH] IP rate limited on email/attach: ip={ip_address}")
        return jsonify({
            "error": {
                "code": "RATE_LIMITED",
                "message": "Too many requests. Please try again later.",
            }
        }), 429

    # Attach email to current identity (unverified)
    # This method is anti-enumeration safe - it returns ok even if email belongs
    # to another identity (user must use restore flow, but we don't tell them explicitly)
    try:
        _, _, reason = IdentityService.attach_email(identity_id, email)
        # Log the reason internally but NEVER expose to users
        if reason == IdentityService.ATTACH_REASON_BELONGS_TO_OTHER:
            # Email belongs to another identity - silently skip attachment
            # User must use restore/redeem flow to take over that account
            pass  # Already logged in attach_email
    except ValueError as e:
        print(f"[AUTH] Failed to attach email: {e}")
        # Continue anyway to prevent enumeration

    # Check rate limits and send verification code
    rate_ok, rate_msg = MagicCodeService._check_rate_limits(email)
    if not rate_ok:
        if "wait" in rate_msg.lower() or "too many" in rate_msg.lower():
            return jsonify({
                "error": {
                    "code": "RATE_LIMITED",
                    "message": rate_msg,
                }
            }), 429
        return jsonify({
            "error": {
                "code": "REQUEST_FAILED",
                "message": rate_msg,
            }
        }), 400

    # Generate and send verification code (result ignored - always return generic success)
    MagicCodeService.request_restore(email, ip_address)

    # Always return generic success to prevent enumeration.
    # IDENT-4: Always include restore hint so frontend can show guidance
    # regardless of whether email is actually taken (anti-enum safe).
    return jsonify({
        "ok": True,
        "message": "If valid, a verification code has been sent",
        "hint": "restore_available",
    })


def _handle_verify_merge(code_id, email, source_id, target_id, session_id):
    """
    Handle cross-identity verify: source identity is trying to verify an email
    that belongs to a different canonical identity (target).

    Attempts automatic merge of source → target when safe, then completes
    verification on the canonical identity.

    Returns a Flask response (success or error).
    """
    from backend.db import transaction, Tables
    from backend.services.merge_service import MergeService

    # Check if source has meaningful data
    safety = MagicCodeService.check_restore_safety(session_id, target_id)
    source_has_data = not safety["safe"] and safety.get("reason") == "source_has_data"
    safety_error = not safety["safe"] and safety.get("reason") != "source_has_data"

    if safety_error:
        # Safety check itself failed → block, do NOT consume code
        print(
            f"[VERIFY] Cross-identity blocked (safety error): "
            f"source={source_id[:8]}..., target={target_id[:8]}..., "
            f"reason={safety.get('reason')}"
        )
        return jsonify({
            "ok": False,
            "error": {
                "code": "VERIFY_MERGE_BLOCKED",
                "message": "This email could not be verified automatically on this device.",
                "next_step": "contact_support",
            },
        }), 409

    if source_has_data:
        # Attempt automatic merge: source → canonical email owner
        print(
            f"[VERIFY] Source {source_id[:8]}... has data, "
            f"attempting merge → target {target_id[:8]}..."
        )
        merge_result = MergeService.execute_merge(
            source_id=source_id,
            target_id=target_id,
            merged_by="system",
            reason="verify_email",
            metadata={
                "trigger": "verify_email",
                "session_id": session_id,
                "email": email,
                "source_stats": safety.get("stats"),
            },
            skip_session_id=session_id,
        )

        if not merge_result["success"]:
            blocked = merge_result.get("blocked_reason", "unknown")
            print(
                f"[VERIFY] Merge BLOCKED: {source_id[:8]}... → "
                f"{target_id[:8]}...: {blocked}"
            )
            # Do NOT consume code — user can retry later
            # IDENT-3: blocked_reason logged above but NOT sent to client
            return jsonify({
                "ok": False,
                "error": {
                    "code": "VERIFY_MERGE_BLOCKED",
                    "message": "This email could not be verified automatically on this device.",
                    "next_step": MergeService._suggest_next_step(blocked),
                },
            }), 409

        print(
            f"[VERIFY] Merge OK: {source_id[:8]}... → {target_id[:8]}... | "
            f"tables={merge_result.get('tables_migrated', {})}, "
            f"sessions_revoked={merge_result.get('sessions_revoked', 0)}"
        )
    else:
        print(
            f"[VERIFY] Source {source_id[:8]}... has no data, "
            f"swinging session to canonical {target_id[:8]}..."
        )

    # Source has no data OR merge succeeded → consume code + swing session
    with transaction() as cur:
        # Consume code
        cur.execute(
            f"""
            UPDATE {Tables.MAGIC_CODES}
            SET consumed = TRUE, consumed_at = NOW()
            WHERE id = %s
            """,
            (code_id,),
        )

        # Swing session to canonical identity
        try:
            cur.execute(
                f"""
                UPDATE {Tables.SESSIONS}
                SET identity_id = %s, updated_at = NOW()
                WHERE id = %s
                RETURNING id
                """,
                (target_id, session_id),
            )
        except Exception as e:
            if "updated_at" in str(e):
                cur.execute(
                    f"""
                    UPDATE {Tables.SESSIONS}
                    SET identity_id = %s
                    WHERE id = %s
                    RETURNING id
                    """,
                    (target_id, session_id),
                )
            else:
                raise

        # NEW-4: Ensure email + email_verified are consistent on canonical.
        # The code was issued for `email`, so the canonical identity
        # must end with that exact email and email_verified = TRUE.
        cur.execute(
            f"""
            UPDATE {Tables.IDENTITIES}
            SET email = %s, email_verified = TRUE, last_seen_at = NOW()
            WHERE id = %s
            """,
            (email, target_id),
        )

    # Invalidate pending codes for this email
    MagicCodeService.invalidate_codes_for_email(email)

    # Resume subscriptions paused due to email_unverified on the canonical identity
    subscriptions_resumed = 0
    try:
        with transaction() as cur:
            cur.execute(
                f"""
                UPDATE {Tables.SUBSCRIPTIONS}
                SET pause_reason = NULL,
                    paused_at = NULL,
                    updated_at = NOW()
                WHERE identity_id::text = %s
                  AND pause_reason = 'email_unverified'
                RETURNING id
                """,
                (target_id,),
            )
            resumed = cur.fetchall() or []
            subscriptions_resumed = len(resumed)
            for row in resumed:
                print(f"[VERIFY] Resumed subscription {row['id']} after cross-identity verify")
    except Exception as e:
        print(f"[VERIFY] Error resuming subscriptions for {target_id}: {e}")

    print(
        f"[VERIFY] Cross-identity verify+merge complete: "
        f"{source_id[:8]}... → {target_id[:8]}..., email={_mask_email(email)}"
    )

    return jsonify({
        "ok": True,
        "message": "Email verified successfully",
        "email_verified": True,
        "email_attached": True,
        "identity_changed": True,
        "identity_id": target_id,
        "subscriptions_resumed": subscriptions_resumed,
    })


@bp.route("/email/verify", methods=["POST"])
@require_session
@anti_enumeration()
def verify_email():
    """
    Verify and attach email to current identity using magic code.

    Flow:
    1. User calls POST /api/auth/email/attach (sends code to email)
    2. User enters code and calls POST /api/auth/email/verify
    3. Code is verified, email is attached to identity idempotently

    For cross-identity cases (email belongs to another canonical identity),
    an automatic merge is attempted (IDENT-1) rather than just blocking.

    Request body:
    {
        "email": "user@example.com",
        "code": "123456"
    }

    Response (success - 200):
    {
        "ok": true,
        "message": "Email verified successfully",
        "email_verified": true,
        "email_attached": true,
        "identity_changed": false
    }

    Error responses:
    - 400 VALIDATION_ERROR: Missing/invalid email or code
    - 400 INVALID_CODE: Code doesn't match or expired
    - 400 CODE_EXPIRED: Code has expired
    - 400 TOO_MANY_ATTEMPTS: Max attempts exceeded
    - 401 NO_SESSION: No active session
    - 409 VERIFY_MERGE_BLOCKED: Cross-identity merge blocked (code preserved)
    """
    data = request.get_json() or {}

    # Normalize email (same as attach_email for consistency)
    email = _normalize_email(data.get("email"))

    # Safely normalize code
    raw_code = data.get("code")
    code = (raw_code.strip() if isinstance(raw_code, str) else "") if raw_code else ""

    # Validation
    if not email:
        return jsonify({
            "error": {
                "code": "VALIDATION_ERROR",
                "message": "email is required",
            }
        }), 400

    if "@" not in email or "." not in email:
        return jsonify({
            "error": {
                "code": "VALIDATION_ERROR",
                "message": "Invalid email format",
            }
        }), 400

    if not code:
        return jsonify({
            "error": {
                "code": "VALIDATION_ERROR",
                "message": "code is required",
            }
        }), 400

    if len(code) != 6 or not code.isdigit():
        return jsonify({
            "error": {
                "code": "VALIDATION_ERROR",
                "message": "Code must be 6 digits",
            }
        }), 400

    # Check session - return specific error code for frontend
    identity_id = g.identity_id
    identity = g.identity

    if not identity_id:
        print(f"[AUTH] verify_email failed: no session for email={_mask_email(email)}")
        return jsonify({
            "error": {
                "code": "NO_SESSION",
                "message": "No active session. Please refresh the page.",
            }
        }), 401

    # Log attempt (never log full code, only last 2 digits if needed)
    print(f"[AUTH] verify_email attempt: identity={identity_id}, email={_mask_email(email)}, code={_mask_code(code)}")

    # Verify the code against magic_codes table
    code_hash = MagicCodeService.hash_code(code)
    from backend.db import query_one, transaction, Tables
    from backend.config import config

    # Find matching active code for this email
    code_record = query_one(
        f"""
        SELECT id, attempts
        FROM {Tables.MAGIC_CODES}
        WHERE email = %s
          AND code_hash = %s
          AND consumed = FALSE
          AND expires_at > NOW()
        ORDER BY created_at DESC
        LIMIT 1
        """,
        (email, code_hash),
    )

    if not code_record:
        # Code not found or doesn't match - increment attempts counter
        MagicCodeService._increment_attempts(email)
        print(f"[AUTH] verify_email failed: invalid code for email={_mask_email(email)}")
        return _get_redeem_error_response("Invalid or expired code")

    # Check max attempts
    max_attempts = config.MAGIC_CODE_MAX_ATTEMPTS
    if code_record["attempts"] >= max_attempts:
        print(f"[AUTH] verify_email failed: too many attempts for email={_mask_email(email)}")
        return _get_redeem_error_response("Too many failed attempts. Please request a new code")

    code_id = str(code_record["id"])

    # ── Determine verification target ──
    current_email = None
    if identity and identity.get("email"):
        current_email = identity["email"].lower() if isinstance(identity["email"], str) else None

    # ── IDENT-1: Cross-identity verify+merge ──
    # If this email belongs to another canonical identity, attempt to merge
    # the current identity into the email owner rather than just blocking.
    if current_email != email:
        existing_owner = IdentityService.get_identity_by_email(email)
        if existing_owner and str(existing_owner["id"]) != identity_id:
            canonical_owner_id = str(existing_owner["id"])
            # Resolve to canonical if the email owner was itself merged
            if existing_owner.get("merged_into_id"):
                canonical_owner_id = IdentityService.resolve_canonical_id(
                    canonical_owner_id
                )
            # Only cross-identity if canonical differs from current session
            if canonical_owner_id != identity_id:
                print(
                    f"[AUTH] verify_email cross-identity: "
                    f"source={identity_id[:8]}... → "
                    f"canonical={canonical_owner_id[:8]}... "
                    f"email={_mask_email(email)}"
                )
                return _handle_verify_merge(
                    code_id=code_id,
                    email=email,
                    source_id=identity_id,
                    target_id=canonical_owner_id,
                    session_id=g.session_id,
                )

    # ── Same-identity case: attach email idempotently ──
    email_attached = False

    if current_email == email:
        # Email already on this identity - just verify it
        pass
    elif current_email is None:
        # Identity has no email, and email is free → attach it
        email_attached = True
    else:
        # Identity has a DIFFERENT email, and new email is free → change it
        email_attached = True
        print(f"[AUTH] Email change requested: identity {identity_id} from {_mask_email(current_email)} to {_mask_email(email)}")

    # ── Commit: mark code consumed & attach/verify email ──
    with transaction() as cur:
        # Mark code as consumed
        cur.execute(
            f"""
            UPDATE {Tables.MAGIC_CODES}
            SET consumed = TRUE, consumed_at = NOW()
            WHERE id = %s
            """,
            (code_id,),
        )

        # Attach email (if needed) and mark verified
        if email_attached:
            cur.execute(
                f"""
                UPDATE {Tables.IDENTITIES}
                SET email = %s, email_verified = TRUE, last_seen_at = NOW()
                WHERE id = %s
                """,
                (email, identity_id),
            )
        else:
            cur.execute(
                f"""
                UPDATE {Tables.IDENTITIES}
                SET email_verified = TRUE, last_seen_at = NOW()
                WHERE id = %s
                """,
                (identity_id,),
            )

    # Invalidate other pending codes for this email
    MagicCodeService.invalidate_codes_for_email(email)

    # Resume any subscriptions paused due to email_unverified
    subscriptions_resumed = 0
    try:
        with transaction() as cur:
            cur.execute(
                f"""
                UPDATE {Tables.SUBSCRIPTIONS}
                SET pause_reason = NULL,
                    paused_at = NULL,
                    updated_at = NOW()
                WHERE identity_id::text = %s
                  AND pause_reason = 'email_unverified'
                RETURNING id
                """,
                (identity_id,),
            )
            resumed = cur.fetchall() or []
            subscriptions_resumed = len(resumed)
            for row in resumed:
                print(f"[AUTH] Resumed subscription {row['id']} after email verification")
    except Exception as e:
        print(f"[AUTH] Error resuming subscriptions for {identity_id}: {e}")

    print(f"[AUTH] Email verified for identity {identity_id}: {_mask_email(email)} (attached={email_attached}, subs_resumed={subscriptions_resumed})")

    # IDENT-3: Always include identity_changed and identity_id to normalize
    # response shape (same keys as cross-identity path).
    return jsonify({
        "ok": True,
        "message": "Email verified successfully",
        "email_verified": True,
        "email_attached": email_attached,
        "identity_changed": False,
        "identity_id": identity_id,
        "subscriptions_resumed": subscriptions_resumed,
    })


# ─────────────────────────────────────────────────────────────
# Route Aliases (shorter paths for convenience)
# ─────────────────────────────────────────────────────────────

@bp.route("/request-code", methods=["POST"])
def request_code_alias():
    """Alias for /restore/request - Request a magic code via email."""
    return request_restore()


@bp.route("/verify-code", methods=["POST"])
@with_optional_session
def verify_code_alias():
    """Alias for /restore/redeem - Verify a magic code and restore session."""
    return redeem_restore()


# ─────────────────────────────────────────────────────────────
# Admin Debug Endpoint
# ─────────────────────────────────────────────────────────────

@bp.route("/health", methods=["GET"])
@require_admin
def auth_health():
    """
    Admin-only health check for auth system diagnostics.

    Checks:
    - SES/email configuration
    - DB schema (sessions.updated_at column)
    - magic_codes table exists

    Requires: X-Admin-Token header

    Response:
    {
        "ok": true,
        "checks": {
            "email": { "configured": true, "provider": "ses", "region": "eu-west-2", "from": "no-reply@timrx.live" },
            "db_sessions_updated_at": { "exists": true },
            "db_magic_codes": { "exists": true, "active_count": 5 }
        }
    }
    """
    from backend.config import config
    from backend.db import query_one, USE_DB

    checks = {}

    # ── Email configuration ──
    try:
        email_check = {
            "configured": config.EMAIL_CONFIGURED,
            "enabled": config.EMAIL_ENABLED,
            "provider": config.EMAIL_PROVIDER,
        }
        if config.EMAIL_PROVIDER == "ses":
            email_check["region"] = config.AWS_REGION
            email_check["from_email"] = config.SES_FROM_EMAIL or config.EMAIL_FROM_ADDRESS
        else:
            email_check["smtp_host"] = config.SMTP_HOST or "(not set)"
            name, addr = config.SMTP_FROM_PARSED
            email_check["from_email"] = addr
            email_check["from_name"] = name
        checks["email"] = email_check
    except Exception as e:
        checks["email"] = {"error": str(e)}

    # ── DB: sessions.updated_at column ──
    if USE_DB:
        try:
            result = query_one("""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = 'timrx_billing'
                  AND table_name = 'sessions'
                  AND column_name = 'updated_at'
            """)
            checks["db_sessions_updated_at"] = {"exists": result is not None}
        except Exception as e:
            checks["db_sessions_updated_at"] = {"exists": False, "error": str(e)}

        # ── DB: magic_codes table ──
        try:
            result = query_one("""
                SELECT COUNT(*) as cnt
                FROM timrx_billing.magic_codes
                WHERE consumed = FALSE AND expires_at > NOW()
            """)
            checks["db_magic_codes"] = {
                "exists": True,
                "active_codes": result["cnt"] if result else 0
            }
        except Exception as e:
            checks["db_magic_codes"] = {"exists": False, "error": str(e)}
    else:
        checks["db_sessions_updated_at"] = {"exists": False, "reason": "DB not configured"}
        checks["db_magic_codes"] = {"exists": False, "reason": "DB not configured"}

    # Overall status
    all_ok = (
        checks.get("email", {}).get("configured", False) and
        checks.get("db_sessions_updated_at", {}).get("exists", False) and
        checks.get("db_magic_codes", {}).get("exists", False)
    )

    return jsonify({
        "ok": all_ok,
        "checks": checks,
    })
