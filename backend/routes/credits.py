"""
/api/credits routes - Credit balance and charging.

Handles:
- GET /api/credits/wallet - Get current wallet balance for active identity
- POST /api/credits/charge - Charge credits for an action (idempotent)

This module provides a simple, stable interface for:
1. Frontend to fetch current balance
2. Backend services to charge credits for paid actions
"""

from flask import Blueprint, request, jsonify, g

from backend.middleware import require_session, no_cache
from backend.services.wallet_service import WalletService, LedgerEntryType
from backend.services.pricing_service import PricingService
from backend.db import transaction, fetch_one, Tables, DatabaseIntegrityError

bp = Blueprint("credits", __name__)


# ─────────────────────────────────────────────────────────────────────────────
# Action key mapping (frontend keys → DB action codes)
# ─────────────────────────────────────────────────────────────────────────────

# Maps frontend action names to DB action_code in timrx_billing.action_costs
ACTION_KEY_MAP = {
    # Core 3D generation
    "text_to_3d": "MESHY_TEXT_TO_3D",
    "image_to_3d": "MESHY_IMAGE_TO_3D",

    # Post-processing
    "texture": "MESHY_RETEXTURE",
    "remesh": "MESHY_REFINE",
    "refine": "MESHY_REFINE",
    "rig": "MESHY_REFINE",

    # Image generation
    "image_generate": "OPENAI_IMAGE",

    # Aliases for compatibility
    "preview": "MESHY_TEXT_TO_3D",  # Preview uses same cost as text_to_3d
    "upscale": "MESHY_REFINE",      # Upscale uses same cost as refine
}


def _resolve_action_cost(action: str) -> tuple[str, int]:
    """
    Resolve action key to (db_action_code, cost_credits).

    Args:
        action: Frontend action key (e.g., 'image_to_3d', 'texture')

    Returns:
        Tuple of (action_code, cost_credits)

    Raises:
        ValueError: If action is unknown or has no cost defined
    """
    # Try mapping frontend key to DB code
    action_code = ACTION_KEY_MAP.get(action.lower())

    if not action_code:
        # Try using action directly as DB code (for backward compat)
        action_code = action.upper()

    # Get cost from PricingService (which reads from action_costs table)
    cost = PricingService.get_action_cost(action)

    if cost == 0:
        # Try with the resolved action_code
        cost = PricingService.get_action_cost(action_code)

    if cost == 0:
        raise ValueError(f"Unknown action or zero cost: {action}")

    return action_code, cost


# ─────────────────────────────────────────────────────────────────────────────
# GET /api/credits/wallet - Fetch current balance
# ─────────────────────────────────────────────────────────────────────────────

@bp.route("/wallet", methods=["GET"])
@require_session
@no_cache
def get_wallet():
    """
    Get current wallet credits for the active identity.
    Returns balance, reserved, and available credits.

    Response (200):
    {
        "ok": true,
        "identity_id": "uuid",
        "credits_balance": 150,
        "reserved_credits": 20,
        "available_credits": 130
    }

    Response (401 - no session):
    {
        "error": {"code": "UNAUTHORIZED", "message": "No valid session"}
    }
    """
    try:
        balance = WalletService.get_balance(g.identity_id)
        reserved = WalletService.get_reserved_credits(g.identity_id)
        available = max(0, balance - reserved)

        print(f"[CREDITS] Wallet fetch: identity={g.identity_id}, balance={balance}, reserved={reserved}, available={available}")

        return jsonify({
            "ok": True,
            "identity_id": g.identity_id,
            "credits_balance": balance,
            "reserved_credits": reserved,
            "available_credits": available,
        })
    except Exception as e:
        print(f"[CREDITS] Error fetching wallet: {e}")
        return jsonify({
            "ok": False,
            "error": {
                "code": "INTERNAL_ERROR",
                "message": "Failed to fetch wallet balance",
            }
        }), 500


# ─────────────────────────────────────────────────────────────────────────────
# POST /api/credits/charge - Charge credits for an action (idempotent)
# ─────────────────────────────────────────────────────────────────────────────

@bp.route("/charge", methods=["POST"])
@require_session
def charge_credits():
    """
    Charge credits for a paid action. Idempotent via (identity_id, action, job_id/upstream_id).

    Request body:
    {
        "action": "image_to_3d" | "preview" | "text_to_3d" | "texture" | "remesh" | "rig" | "upscale" | "image_generate",
        "job_id": "unique-job-identifier",      // Primary idempotency key
        "upstream_id": "optional-provider-id",  // Alternative idempotency key (fallback)
        "metadata": {...}                       // Optional metadata to store
    }

    Idempotency:
    - Uses UNIQUE constraint on (identity_id, ref_type=action, ref_id=job_id)
    - If same (identity, action, job_id) is charged twice, second call returns existing result
    - Prevents double-charging for the same job

    Response (200 - success or idempotent replay):
    {
        "ok": true,
        "new_balance": 130,
        "charged": 20,
        "action": "image_to_3d",
        "idempotent": false  // true if this was a replay of existing charge
    }

    Response (402 - insufficient credits):
    {
        "ok": false,
        "error": {
            "code": "INSUFFICIENT_CREDITS",
            "message": "Not enough credits",
            "required": 20,
            "balance": 10
        }
    }

    Response (400 - validation error):
    {
        "ok": false,
        "error": {
            "code": "VALIDATION_ERROR",
            "message": "action is required"
        }
    }
    """
    data = request.get_json() or {}

    action = data.get("action", "").strip().lower()
    job_id = data.get("job_id", "").strip()
    upstream_id = data.get("upstream_id", "").strip()
    metadata = data.get("metadata") or {}

    # Validation
    if not action:
        return jsonify({
            "ok": False,
            "error": {
                "code": "VALIDATION_ERROR",
                "message": "action is required",
            }
        }), 400

    # Need at least one idempotency key
    idempotency_key = job_id or upstream_id
    if not idempotency_key:
        return jsonify({
            "ok": False,
            "error": {
                "code": "VALIDATION_ERROR",
                "message": "job_id or upstream_id is required for idempotency",
            }
        }), 400

    # Resolve action to DB code and cost
    try:
        action_code, cost_credits = _resolve_action_cost(action)
    except ValueError as e:
        return jsonify({
            "ok": False,
            "error": {
                "code": "INVALID_ACTION",
                "message": str(e),
            }
        }), 400

    identity_id = g.identity_id

    # Build ref_type and ref_id for idempotency
    # ref_type = action (e.g., "image_to_3d")
    # ref_id = job_id (or upstream_id as fallback)
    ref_type = action
    ref_id = idempotency_key

    # Store additional context in metadata
    charge_meta = {
        "action_code": action_code,
        "job_id": job_id or None,
        "upstream_id": upstream_id or None,
        **(metadata or {}),
    }

    try:
        result = _charge_credits_idempotent(
            identity_id=identity_id,
            action=action,
            ref_type=ref_type,
            ref_id=ref_id,
            cost_credits=cost_credits,
            meta=charge_meta,
        )

        return jsonify({
            "ok": True,
            "new_balance": result["new_balance"],
            "charged": result["charged"],
            "action": action,
            "idempotent": result["idempotent"],
        })

    except ValueError as e:
        error_msg = str(e)

        # Parse insufficient credits error
        if "INSUFFICIENT_CREDITS" in error_msg:
            parts = error_msg.split(":")
            error_data = {}
            for part in parts[1:]:
                if "=" in part:
                    key, val = part.split("=", 1)
                    error_data[key] = int(val)

            return jsonify({
                "ok": False,
                "error": {
                    "code": "INSUFFICIENT_CREDITS",
                    "message": "Not enough credits for this action",
                    "required": error_data.get("required", cost_credits),
                    "balance": error_data.get("balance", 0),
                }
            }), 402

        # Wallet not found
        if "Wallet not found" in error_msg:
            return jsonify({
                "ok": False,
                "error": {
                    "code": "WALLET_NOT_FOUND",
                    "message": "User wallet not initialized",
                }
            }), 400

        # Generic error
        return jsonify({
            "ok": False,
            "error": {
                "code": "CHARGE_ERROR",
                "message": str(e),
            }
        }), 400

    except Exception as e:
        print(f"[CREDITS] Error charging credits: {e}")
        return jsonify({
            "ok": False,
            "error": {
                "code": "INTERNAL_ERROR",
                "message": "Failed to charge credits",
            }
        }), 500


def _charge_credits_idempotent(
    identity_id: str,
    action: str,
    ref_type: str,
    ref_id: str,
    cost_credits: int,
    meta: dict = None,
) -> dict:
    """
    Charge credits with idempotency guarantee.

    Uses a unique constraint on (identity_id, ref_type, ref_id) where entry_type='charge'
    to ensure the same charge can only happen once.

    Args:
        identity_id: User identity
        action: Action being charged (for logging)
        ref_type: Reference type (typically the action name)
        ref_id: Reference ID (job_id or upstream_id)
        cost_credits: Amount to charge
        meta: Additional metadata

    Returns:
        {
            "new_balance": int,
            "charged": int,
            "idempotent": bool  # True if this was a replay
        }

    Raises:
        ValueError: If insufficient credits or wallet not found
    """
    import json

    meta_json = json.dumps(meta) if meta else None

    with transaction() as cur:
        # 1. Check for existing charge with same idempotency key
        cur.execute(
            f"""
            SELECT id, amount_credits
            FROM {Tables.LEDGER_ENTRIES}
            WHERE identity_id = %s
              AND ref_type = %s
              AND ref_id = %s
              AND entry_type = 'charge'
            """,
            (identity_id, ref_type, ref_id),
        )
        existing = fetch_one(cur)

        if existing:
            # Idempotent replay - return current balance without charging again
            cur.execute(
                f"""
                SELECT balance_credits
                FROM {Tables.WALLETS}
                WHERE identity_id = %s
                """,
                (identity_id,),
            )
            wallet = fetch_one(cur)
            current_balance = wallet.get("balance_credits", 0) if wallet else 0

            print(
                f"[CREDITS] Idempotent charge replay: identity={identity_id}, "
                f"action={action}, ref_id={ref_id}, balance={current_balance}"
            )

            return {
                "new_balance": current_balance,
                "charged": abs(existing["amount_credits"]),
                "idempotent": True,
            }

        # 2. Lock wallet and check balance
        cur.execute(
            f"""
            SELECT identity_id, balance_credits
            FROM {Tables.WALLETS}
            WHERE identity_id = %s
            FOR UPDATE
            """,
            (identity_id,),
        )
        wallet = fetch_one(cur)

        if not wallet:
            raise ValueError(f"Wallet not found for identity {identity_id}")

        current_balance = wallet.get("balance_credits", 0) or 0

        # 3. Check sufficient balance
        if current_balance < cost_credits:
            raise ValueError(
                f"INSUFFICIENT_CREDITS:required={cost_credits}:balance={current_balance}"
            )

        new_balance = current_balance - cost_credits

        # 4. Insert ledger entry (charge)
        try:
            cur.execute(
                f"""
                INSERT INTO {Tables.LEDGER_ENTRIES}
                (identity_id, entry_type, amount_credits, ref_type, ref_id, meta, created_at)
                VALUES (%s, 'charge', %s, %s, %s, %s, NOW())
                RETURNING id
                """,
                (identity_id, -cost_credits, ref_type, ref_id, meta_json),
            )
        except DatabaseIntegrityError:
            # Unique constraint violation - concurrent insert
            # This is fine, just return idempotent result
            cur.execute(
                f"""
                SELECT balance_credits
                FROM {Tables.WALLETS}
                WHERE identity_id = %s
                """,
                (identity_id,),
            )
            wallet = fetch_one(cur)
            current_balance = wallet.get("balance_credits", 0) if wallet else 0

            return {
                "new_balance": current_balance,
                "charged": cost_credits,
                "idempotent": True,
            }

        # 5. Update wallet balance
        cur.execute(
            f"""
            UPDATE {Tables.WALLETS}
            SET balance_credits = %s, updated_at = NOW()
            WHERE identity_id = %s
            """,
            (new_balance, identity_id),
        )

        print(
            f"[CREDITS] Charged: identity={identity_id}, action={action}, "
            f"credits={cost_credits}, balance: {current_balance} -> {new_balance}"
        )

        return {
            "new_balance": new_balance,
            "charged": cost_credits,
            "idempotent": False,
        }


# ─────────────────────────────────────────────────────────────────────────────
# Additional convenience endpoint: GET /api/wallet (alias)
# ─────────────────────────────────────────────────────────────────────────────

# Note: This is registered at /api/credits/wallet
# For a top-level /api/wallet endpoint, add a route in app.py or create
# a separate simple blueprint. The /api/credits/wallet path is preferred
# for namespace consistency.
