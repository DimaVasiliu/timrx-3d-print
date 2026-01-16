"""
Pricing Service - Manages plans and action costs.

Responsibilities:
- Get available purchase plans
- Get action costs for credit checks
- Validate plan codes

Stable frontend keys:
- text_to_3d_generate
- image_to_3d_generate
- refine
- remesh
- texture
- rig
- image_studio_generate
"""

from typing import Optional, Dict, Any, List

from ..db import query_one, query_all, Tables


class PricingService:
    """Service for managing pricing plans and action costs."""

    # Cache for action costs (refreshed on startup or manually)
    _action_costs_cache: Dict[str, int] = {}
    _db_to_frontend_map: Dict[str, str] = {}
    _frontend_to_db_map: Dict[str, str] = {}

    # Mapping from DB action_code to stable frontend keys
    # DB codes: MESHY_TEXT_TO_3D, MESHY_IMAGE_TO_3D, MESHY_REFINE, MESHY_RETEXTURE, OPENAI_IMAGE
    ACTION_CODE_MAP = {
        "MESHY_TEXT_TO_3D": "text_to_3d_generate",
        "MESHY_IMAGE_TO_3D": "image_to_3d_generate",
        "MESHY_REFINE": "refine",
        "MESHY_RETEXTURE": "texture",
        "OPENAI_IMAGE": "image_studio_generate",
    }

    # Additional frontend aliases that map to same DB codes
    FRONTEND_ALIASES = {
        "remesh": "MESHY_REFINE",  # remesh uses same cost as refine
        "rig": "MESHY_REFINE",     # rig uses same cost as refine
    }

    @staticmethod
    def get_plans(active_only: bool = True) -> List[Dict[str, Any]]:
        """
        Get available credit plans.
        Returns list of plans with code, name, price, credits.
        """
        if active_only:
            plans = query_all(
                f"""
                SELECT id, code, name, description, price_gbp, currency,
                       credit_grant, includes_priority, meta, created_at
                FROM {Tables.PLANS}
                WHERE is_active = TRUE
                ORDER BY price_gbp ASC
                """
            )
        else:
            plans = query_all(
                f"""
                SELECT id, code, name, description, price_gbp, currency,
                       credit_grant, includes_priority, is_active, meta, created_at
                FROM {Tables.PLANS}
                ORDER BY price_gbp ASC
                """
            )

        # Format for frontend
        return [
            {
                "id": str(plan["id"]),
                "code": plan["code"],
                "name": plan["name"],
                "description": plan.get("description"),
                "price": float(plan["price_gbp"]),
                "currency": plan.get("currency", "GBP"),
                "credits": plan["credit_grant"],
                "includes_priority": plan.get("includes_priority", False),
            }
            for plan in plans
        ]

    @staticmethod
    def get_plan_by_code(code: str) -> Optional[Dict[str, Any]]:
        """
        Get a specific plan by its code.
        Returns None if not found or inactive.
        """
        plan = query_one(
            f"""
            SELECT id, code, name, description, price_gbp, currency,
                   credit_grant, includes_priority, meta, created_at
            FROM {Tables.PLANS}
            WHERE code = %s AND is_active = TRUE
            """,
            (code,),
        )

        if not plan:
            return None

        return {
            "id": str(plan["id"]),
            "code": plan["code"],
            "name": plan["name"],
            "description": plan.get("description"),
            "price": float(plan["price_gbp"]),
            "currency": plan.get("currency", "GBP"),
            "credits": plan["credit_grant"],
            "includes_priority": plan.get("includes_priority", False),
        }

    @staticmethod
    def get_plans_with_perks(active_only: bool = True) -> List[Dict[str, Any]]:
        """
        Get available credit plans with perks object.
        Returns list of plans in frontend-friendly format.

        Response format:
        [
            {
                "id": "uuid",
                "code": "starter_80",
                "name": "Starter",
                "price_gbp": 7.99,
                "credits": 80,
                "perks": {
                    "priority": false,
                    "retention_days": 30
                }
            },
            ...
        ]
        """
        if active_only:
            plans = query_all(
                f"""
                SELECT id, code, name, description, price_gbp, currency,
                       credit_grant, includes_priority, meta, created_at
                FROM {Tables.PLANS}
                WHERE is_active = TRUE
                ORDER BY price_gbp ASC
                """
            )
        else:
            plans = query_all(
                f"""
                SELECT id, code, name, description, price_gbp, currency,
                       credit_grant, includes_priority, is_active, meta, created_at
                FROM {Tables.PLANS}
                ORDER BY price_gbp ASC
                """
            )

        # Default retention days (can be overridden via meta)
        DEFAULT_RETENTION_DAYS = 30

        # Format for frontend with perks
        return [
            {
                "id": str(plan["id"]),
                "code": plan["code"],
                "name": plan["name"],
                "price_gbp": float(plan["price_gbp"]),
                "credits": plan["credit_grant"],
                "perks": {
                    "priority": plan.get("includes_priority", False),
                    "retention_days": (plan.get("meta") or {}).get("retention_days", DEFAULT_RETENTION_DAYS),
                },
            }
            for plan in plans
        ]

    @staticmethod
    def get_action_costs() -> Dict[str, int]:
        """
        Get all action costs as a dict with stable frontend keys.
        Returns {frontend_key: cost_credits}.

        Example response:
        {
            "text_to_3d_generate": 20,
            "image_to_3d_generate": 30,
            "refine": 10,
            "remesh": 10,
            "texture": 10,
            "rig": 10,
            "image_studio_generate": 12
        }
        """
        # Use cache if available
        if PricingService._action_costs_cache:
            return PricingService._action_costs_cache.copy()

        # Fetch from DB
        rows = query_all(
            f"""
            SELECT action_code, cost_credits
            FROM {Tables.ACTION_COSTS}
            """
        )

        # Build cost lookup by DB code
        db_costs: Dict[str, int] = {}
        for row in rows:
            db_costs[row["action_code"]] = row["cost_credits"]

        # Map to frontend keys
        result: Dict[str, int] = {}

        # Primary mappings
        for db_code, frontend_key in PricingService.ACTION_CODE_MAP.items():
            if db_code in db_costs:
                result[frontend_key] = db_costs[db_code]

        # Aliases (remesh, rig point to same costs)
        for alias, db_code in PricingService.FRONTEND_ALIASES.items():
            if db_code in db_costs:
                result[alias] = db_costs[db_code]

        # Cache the result
        PricingService._action_costs_cache = result.copy()

        return result

    @staticmethod
    def get_action_costs_list() -> List[Dict[str, Any]]:
        """
        Get all action costs as a list of {action_key, credits} objects.
        Suitable for frontend caching.

        Response format:
        [
            {"action_key": "text_to_3d_generate", "credits": 20},
            {"action_key": "image_to_3d_generate", "credits": 30},
            ...
        ]
        """
        costs_dict = PricingService.get_action_costs()
        return [
            {"action_key": key, "credits": credits}
            for key, credits in costs_dict.items()
        ]

    @staticmethod
    def get_action_cost(action_key: str) -> int:
        """
        Get cost in credits for a specific action.
        Accepts both frontend keys (e.g., 'text_to_3d_generate')
        and DB codes (e.g., 'MESHY_TEXT_TO_3D').

        Returns 0 if action not found (should not happen in production).
        """
        costs = PricingService.get_action_costs()

        # Try direct frontend key lookup
        if action_key in costs:
            return costs[action_key]

        # Try mapping from DB code
        frontend_key = PricingService.ACTION_CODE_MAP.get(action_key)
        if frontend_key and frontend_key in costs:
            return costs[frontend_key]

        # Fallback - check aliases
        if action_key in PricingService.FRONTEND_ALIASES:
            db_code = PricingService.FRONTEND_ALIASES[action_key]
            frontend_key = PricingService.ACTION_CODE_MAP.get(db_code)
            if frontend_key and frontend_key in costs:
                return costs[frontend_key]

        print(f"[PRICING] Warning: Unknown action key '{action_key}', returning 0 cost")
        return 0

    @staticmethod
    def get_db_action_code(frontend_key: str) -> Optional[str]:
        """
        Convert frontend key to DB action_code.
        Returns None if not found.

        Example: 'text_to_3d_generate' -> 'MESHY_TEXT_TO_3D'
        """
        # Check aliases first
        if frontend_key in PricingService.FRONTEND_ALIASES:
            return PricingService.FRONTEND_ALIASES[frontend_key]

        # Reverse lookup in primary map
        for db_code, fe_key in PricingService.ACTION_CODE_MAP.items():
            if fe_key == frontend_key:
                return db_code

        return None

    @staticmethod
    def refresh_costs_cache() -> None:
        """
        Refresh the action costs cache from database.
        Called on startup and can be called to refresh.
        """
        PricingService._action_costs_cache = {}
        PricingService.get_action_costs()  # This repopulates the cache
        print(f"[PRICING] Refreshed action costs cache: {PricingService._action_costs_cache}")

    @staticmethod
    def map_job_type_to_action(job_type: str) -> str:
        """
        Map frontend job type names to DB action codes.
        E.g., 'text-to-3d' -> 'MESHY_TEXT_TO_3D'

        This is for backward compatibility with old job type strings.
        """
        job = (job_type or "").lower().replace("_", "-")
        mapping = {
            "text-to-3d": "MESHY_TEXT_TO_3D",
            "image-to-3d": "MESHY_IMAGE_TO_3D",
            "texture": "MESHY_RETEXTURE",
            "retexture": "MESHY_RETEXTURE",
            "remesh": "MESHY_REFINE",
            "refine": "MESHY_REFINE",
            "rigging": "MESHY_REFINE",
            "rig": "MESHY_REFINE",
            "openai-image": "OPENAI_IMAGE",
            "nano-image": "OPENAI_IMAGE",
            "image-studio": "OPENAI_IMAGE",
        }
        return mapping.get(job, "MESHY_TEXT_TO_3D")  # Default to text-to-3d cost
