"""
Pricing Service - Manages plans and action costs.

Responsibilities:
- Get available purchase plans
- Get action costs for credit checks
- Validate plan codes
- Normalize action keys to canonical form

CANONICAL ACTION KEYS (use these in new code):
- image_generate       (10c) - All 2D image providers (OpenAI, Gemini, etc.)
- text_to_3d_generate  (20c) - Text to 3D preview generation
- image_to_3d_generate (30c) - Image to 3D conversion
- refine               (10c) - Refine/upscale 3D model
- remesh               (10c) - Remesh 3D model (same cost as refine)
- retexture            (15c) - Apply new texture to 3D model
- rigging              (25c) - Add skeleton/rig to 3D model
- video_generate       (60c) - Generic video generation
- video_text_generate  (60c) - Text-to-video generation
- video_image_animate  (60c) - Image-to-video animation

LEGACY ALIASES (backwards compatibility only):
- preview, text-to-3d, text-to-3d-preview -> text_to_3d_generate
- image-to-3d -> image_to_3d_generate
- text-to-3d-refine, upscale -> refine
- texture -> retexture
- rig -> rigging
- image_studio_generate, openai-image, text-to-image -> image_generate
- video, video-generate -> video_generate
- text2video, video-text-generate -> video_text_generate
- image2video, video-image-animate -> video_image_animate
"""

from typing import Optional, Dict, Any, List

from backend.db import query_one, query_all, execute, Tables


# ─────────────────────────────────────────────────────────────────────────────
# CANONICAL ACTION KEYS - Single source of truth
# ─────────────────────────────────────────────────────────────────────────────

# Canonical action keys (use these in all new code)
class CanonicalActions:
    """Canonical action key constants."""
    IMAGE_GENERATE = "image_generate"
    TEXT_TO_3D_GENERATE = "text_to_3d_generate"
    IMAGE_TO_3D_GENERATE = "image_to_3d_generate"
    REFINE = "refine"
    REMESH = "remesh"
    RETEXTURE = "retexture"
    RIGGING = "rigging"
    VIDEO_GENERATE = "video_generate"
    VIDEO_TEXT_GENERATE = "video_text_generate"
    VIDEO_IMAGE_ANIMATE = "video_image_animate"
    GEMINI_VIDEO = "gemini_video"


# Canonical key -> DB action code mapping
CANONICAL_TO_DB = {
    CanonicalActions.IMAGE_GENERATE: "OPENAI_IMAGE",
    CanonicalActions.TEXT_TO_3D_GENERATE: "MESHY_TEXT_TO_3D",
    CanonicalActions.IMAGE_TO_3D_GENERATE: "MESHY_IMAGE_TO_3D",
    CanonicalActions.REFINE: "MESHY_REFINE",
    CanonicalActions.REMESH: "MESHY_REFINE",  # Remesh uses same cost as refine
    CanonicalActions.RETEXTURE: "MESHY_RETEXTURE",
    CanonicalActions.RIGGING: "MESHY_RIG",
    CanonicalActions.VIDEO_GENERATE: "VIDEO_GENERATE",
    CanonicalActions.VIDEO_TEXT_GENERATE: "VIDEO_TEXT_GENERATE",
    CanonicalActions.VIDEO_IMAGE_ANIMATE: "VIDEO_IMAGE_ANIMATE",
    CanonicalActions.GEMINI_VIDEO: "GEMINI_VIDEO",
}

# Alias -> Canonical key mapping (for backwards compatibility)
# All variations map to canonical keys
ALIAS_TO_CANONICAL = {
    # Image generation aliases
    "image_studio_generate": CanonicalActions.IMAGE_GENERATE,
    "openai-image": CanonicalActions.IMAGE_GENERATE,
    "text-to-image": CanonicalActions.IMAGE_GENERATE,
    "image-studio": CanonicalActions.IMAGE_GENERATE,
    "nano-image": CanonicalActions.IMAGE_GENERATE,

    # Text-to-3D aliases
    "preview": CanonicalActions.TEXT_TO_3D_GENERATE,
    "text-to-3d": CanonicalActions.TEXT_TO_3D_GENERATE,
    "text-to-3d-preview": CanonicalActions.TEXT_TO_3D_GENERATE,

    # Image-to-3D aliases
    "image-to-3d": CanonicalActions.IMAGE_TO_3D_GENERATE,

    # Refine aliases
    "text-to-3d-refine": CanonicalActions.REFINE,
    "upscale": CanonicalActions.REFINE,

    # Remesh aliases
    "text-to-3d-remesh": CanonicalActions.REMESH,

    # Retexture aliases
    "texture": CanonicalActions.RETEXTURE,

    # Rigging aliases
    "rig": CanonicalActions.RIGGING,

    # Video aliases
    "video": CanonicalActions.VIDEO_GENERATE,
    "video-generate": CanonicalActions.VIDEO_GENERATE,
    "text2video": CanonicalActions.VIDEO_TEXT_GENERATE,
    "video-text-generate": CanonicalActions.VIDEO_TEXT_GENERATE,
    "image2video": CanonicalActions.VIDEO_IMAGE_ANIMATE,
    "video-image-animate": CanonicalActions.VIDEO_IMAGE_ANIMATE,
}


def normalize_action_key(action_key: str) -> str:
    """
    Normalize any action key to its canonical form.

    Args:
        action_key: Any action key (canonical, alias, or legacy)

    Returns:
        Canonical action key

    Example:
        normalize_action_key("openai-image") -> "image_generate"
        normalize_action_key("image_generate") -> "image_generate"
        normalize_action_key("text-to-3d-preview") -> "text_to_3d_generate"
    """
    # Already canonical?
    if action_key in CANONICAL_TO_DB:
        return action_key

    # Check alias mapping
    if action_key in ALIAS_TO_CANONICAL:
        return ALIAS_TO_CANONICAL[action_key]

    # Normalize hyphens to underscores and try again
    normalized = action_key.replace("-", "_").lower()
    if normalized in CANONICAL_TO_DB:
        return normalized
    if normalized in ALIAS_TO_CANONICAL:
        return ALIAS_TO_CANONICAL[normalized]

    # Unknown - log warning and return as-is
    print(f"[PRICING] WARNING: Unknown action key '{action_key}', cannot normalize")
    return action_key


def get_db_action_code_from_canonical(canonical_key: str) -> Optional[str]:
    """
    Get DB action code from canonical key.

    Args:
        canonical_key: A canonical action key

    Returns:
        DB action code (e.g., "OPENAI_IMAGE") or None if not found
    """
    return CANONICAL_TO_DB.get(canonical_key)


# Default plans to seed into the database
DEFAULT_ACTION_COSTS = [
    {"action_code": "MESHY_TEXT_TO_3D", "cost_credits": 20, "provider": "meshy"},
    {"action_code": "MESHY_REFINE", "cost_credits": 10, "provider": "meshy"},
    {"action_code": "MESHY_RETEXTURE", "cost_credits": 15, "provider": "meshy"},
    {"action_code": "MESHY_IMAGE_TO_3D", "cost_credits": 30, "provider": "meshy"},
    {"action_code": "MESHY_RIG", "cost_credits": 25, "provider": "meshy"},
    {"action_code": "OPENAI_IMAGE", "cost_credits": 10, "provider": "openai"},
    {"action_code": "VIDEO_GENERATE", "cost_credits": 60, "provider": "video"},
    {"action_code": "VIDEO_TEXT_GENERATE", "cost_credits": 60, "provider": "video"},
    {"action_code": "VIDEO_IMAGE_ANIMATE", "cost_credits": 60, "provider": "video"},
    {"action_code": "GEMINI_VIDEO", "cost_credits": 80, "provider": "google"},
]

DEFAULT_PLANS = [
    {
        "code": "starter_80",
        "name": "Starter",
        "description": "Try the tools. Great for a few generations.",
        "price_gbp": 7.99,
        "credit_grant": 80,
        "includes_priority": False,
    },
    {
        "code": "creator_300",
        "name": "Creator",
        "description": "Regular use. Better value bundle.",
        "price_gbp": 19.99,
        "credit_grant": 300,
        "includes_priority": False,
    },
    {
        "code": "studio_600",
        "name": "Studio",
        "description": "Heavy use. Best value. Priority queue access.",
        "price_gbp": 34.99,
        "credit_grant": 600,
        "includes_priority": True,
    },
]


class PricingService:
    """Service for managing pricing plans and action costs."""

    # Cache for action costs (refreshed on startup or manually)
    _action_costs_cache: Dict[str, int] = {}

    # DB action code -> canonical key mapping (reverse of CANONICAL_TO_DB)
    DB_TO_CANONICAL = {v: k for k, v in CANONICAL_TO_DB.items()}

    # Legacy mapping for backwards compatibility (deprecated - use normalize_action_key)
    ACTION_CODE_MAP = {
        "MESHY_TEXT_TO_3D": CanonicalActions.TEXT_TO_3D_GENERATE,
        "MESHY_IMAGE_TO_3D": CanonicalActions.IMAGE_TO_3D_GENERATE,
        "MESHY_REFINE": CanonicalActions.REFINE,
        "MESHY_RETEXTURE": CanonicalActions.RETEXTURE,
        "MESHY_RIG": CanonicalActions.RIGGING,
        "OPENAI_IMAGE": CanonicalActions.IMAGE_GENERATE,
        "VIDEO_GENERATE": CanonicalActions.VIDEO_GENERATE,
        "VIDEO_TEXT_GENERATE": CanonicalActions.VIDEO_TEXT_GENERATE,
        "VIDEO_IMAGE_ANIMATE": CanonicalActions.VIDEO_IMAGE_ANIMATE,
    }

    # Legacy aliases (deprecated - use ALIAS_TO_CANONICAL)
    FRONTEND_ALIASES = {alias: CANONICAL_TO_DB.get(canonical, canonical)
                        for alias, canonical in ALIAS_TO_CANONICAL.items()}

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
        Get all action costs as a dict with canonical keys.
        Returns {canonical_key: cost_credits}.

        Example response:
        {
            "image_generate": 10,
            "text_to_3d_generate": 20,
            "image_to_3d_generate": 30,
            "refine": 10,
            "remesh": 10,
            "retexture": 15,
            "rigging": 25,
            "video_generate": 60,
            "video_text_generate": 60,
            "video_image_animate": 60
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

        # Map DB codes to canonical keys
        result: Dict[str, int] = {}
        for canonical_key, db_code in CANONICAL_TO_DB.items():
            if db_code in db_costs:
                result[canonical_key] = db_costs[db_code]

        # Also include all aliases for backwards compatibility
        for alias, canonical_key in ALIAS_TO_CANONICAL.items():
            if canonical_key in result:
                result[alias] = result[canonical_key]

        # Log what we're returning for debugging
        # if result:
        #     canonical_count = len(CANONICAL_TO_DB)
        #     print(f"[PRICING] Action costs loaded: {canonical_count} canonical keys + {len(result) - canonical_count} aliases")
        else:
            print("[PRICING] WARNING: No action costs found in database!")

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
        Accepts any action key (canonical, alias, or DB code).

        Args:
            action_key: Any action key

        Returns:
            Cost in credits, or 0 if not found
        """
        costs = PricingService.get_action_costs()

        # Direct lookup (handles canonical keys and cached aliases)
        if action_key in costs:
            return costs[action_key]

        # Try normalizing to canonical
        canonical = normalize_action_key(action_key)
        if canonical in costs:
            return costs[canonical]

        # Try as DB code (e.g., "MESHY_TEXT_TO_3D")
        canonical_from_db = PricingService.DB_TO_CANONICAL.get(action_key)
        if canonical_from_db and canonical_from_db in costs:
            return costs[canonical_from_db]

        print(f"[PRICING] Warning: Unknown action key '{action_key}', returning 0 cost")
        return 0

    @staticmethod
    def get_db_action_code(action_key: str) -> Optional[str]:
        """
        Convert any action key to DB action_code.

        Args:
            action_key: Any action key (canonical, alias, or legacy)

        Returns:
            DB action code (e.g., "OPENAI_IMAGE") or None if not found

        Example:
            get_db_action_code('image_generate') -> 'OPENAI_IMAGE'
            get_db_action_code('openai-image') -> 'OPENAI_IMAGE'
            get_db_action_code('text-to-3d-preview') -> 'MESHY_TEXT_TO_3D'
        """
        # Normalize to canonical first
        canonical = normalize_action_key(action_key)

        # Get DB code from canonical
        db_code = CANONICAL_TO_DB.get(canonical)
        if db_code:
            return db_code

        # Already a DB code?
        if action_key in PricingService.DB_TO_CANONICAL:
            return action_key

        return None

    @staticmethod
    def normalize_and_get_cost(action_key: str) -> tuple[str, str, int]:
        """
        Normalize action key and get its cost.
        Returns (canonical_key, db_code, cost).

        Useful for logging both requested and canonical action codes.

        Example:
            normalize_and_get_cost('openai-image') -> ('image_generate', 'OPENAI_IMAGE', 10)
        """
        canonical = normalize_action_key(action_key)
        db_code = CANONICAL_TO_DB.get(canonical, "UNKNOWN")
        cost = PricingService.get_action_cost(canonical)
        return (canonical, db_code, cost)

    @staticmethod
    def refresh_costs_cache() -> None:
        """
        Refresh the action costs cache from database.
        Called on startup and can be called to refresh.
        """
        PricingService._action_costs_cache = {}
        PricingService.get_action_costs()  # This repopulates the cache
        # print(f"[PRICING] Refreshed action costs cache: {PricingService._action_costs_cache}")

    @staticmethod
    def map_job_type_to_action(job_type: str) -> str:
        """
        Map any job type string to DB action code.
        Uses canonical normalization under the hood.

        Args:
            job_type: Any job type string (legacy or current)

        Returns:
            DB action code (e.g., 'MESHY_TEXT_TO_3D')

        Example:
            map_job_type_to_action('text-to-3d') -> 'MESHY_TEXT_TO_3D'
            map_job_type_to_action('openai-image') -> 'OPENAI_IMAGE'
        """
        if not job_type:
            return "MESHY_TEXT_TO_3D"  # Default

        # Normalize and get DB code
        canonical = normalize_action_key(job_type)
        db_code = CANONICAL_TO_DB.get(canonical)

        if db_code:
            return db_code

        # Fallback for unknown types
        print(f"[PRICING] Warning: Unknown job type '{job_type}', defaulting to MESHY_TEXT_TO_3D")
        return "MESHY_TEXT_TO_3D"

    @staticmethod
    def seed_plans() -> int:
        """
        Seed the default plans into the database.
        Uses INSERT ... ON CONFLICT DO UPDATE to ensure plans exist and are active.
        Safe to call multiple times (idempotent).

        Plans use the credit_grant column to store the number of credits to grant.

        Returns:
            Number of plans seeded/updated
        """
        from backend.db import is_available

        # print("[PRICING] Starting plan seed...")

        if not is_available():
            # print("[PRICING] Database not available, skipping plan seed")
            return 0

        # First verify the table exists
        try:
            result = query_one(
                f"SELECT COUNT(*) as cnt FROM {Tables.PLANS}"
            )
            existing_count = result["cnt"] if result else 0
            # print(f"[PRICING] Plans table exists, current count: {existing_count}")
        except Exception as e:
            print(f"[PRICING] Plans table check failed: {e}")
            print("[PRICING] Table may not exist - ensure migrations have run")
            return 0

        seeded = 0
        for plan in DEFAULT_PLANS:
            try:
                execute(
                    f"""
                    INSERT INTO {Tables.PLANS}
                        (code, name, description, price_gbp, currency, credit_grant, includes_priority, is_active, created_at)
                    VALUES
                        (%s, %s, %s, %s, 'GBP', %s, %s, TRUE, NOW())
                    ON CONFLICT (code) DO UPDATE SET
                        name = EXCLUDED.name,
                        description = EXCLUDED.description,
                        price_gbp = EXCLUDED.price_gbp,
                        credit_grant = EXCLUDED.credit_grant,
                        includes_priority = EXCLUDED.includes_priority,
                        is_active = TRUE
                    """,
                    (
                        plan["code"],
                        plan["name"],
                        plan["description"],
                        plan["price_gbp"],
                        plan["credit_grant"],
                        plan["includes_priority"],
                    ),
                )
                seeded += 1
                # print(f"[PRICING] Seeded plan: {plan['code']} ({plan['credit_grant']} credits @ £{plan['price_gbp']})")
            except Exception as e:
                print(f"[PRICING] Error seeding plan {plan['code']}: {e}")
                import traceback
                traceback.print_exc()

        # print(f"[PRICING] Plans seed complete: {seeded}/{len(DEFAULT_PLANS)}")
        return seeded

    @staticmethod
    def seed_action_costs() -> int:
        """
        Seed default action costs into the database.
        Uses INSERT ... ON CONFLICT DO NOTHING (idempotent).
        Safe to call multiple times — existing rows are never overwritten.

        Returns:
            Number of action costs seeded (new rows only)
        """
        from backend.db import is_available

        # print("[PRICING] Starting action_costs seed...")

        if not is_available():
            # print("[PRICING] Database not available, skipping action_costs seed")
            return 0

        try:
            result = query_one(
                f"SELECT COUNT(*) as cnt FROM {Tables.ACTION_COSTS}"
            )
            existing_count = result["cnt"] if result else 0
            # print(f"[PRICING] action_costs table exists, current count: {existing_count}")
        except Exception as e:
            print(f"[PRICING] action_costs table check failed: {e}")
            return 0

        seeded = 0
        for ac in DEFAULT_ACTION_COSTS:
            try:
                execute(
                    f"""
                    INSERT INTO {Tables.ACTION_COSTS}
                        (action_code, cost_credits, provider, updated_at)
                    VALUES
                        (%s, %s, %s, NOW())
                    ON CONFLICT (action_code) DO NOTHING
                    """,
                    (ac["action_code"], ac["cost_credits"], ac["provider"]),
                )
                seeded += 1
            except Exception as e:
                print(f"[PRICING] Error seeding action_cost {ac['action_code']}: {e}")

        # Clear cache so next lookup fetches fresh data
        PricingService._action_costs_cache = {}

        # print(f"[PRICING] Action costs seed complete: {seeded} checked, {existing_count} pre-existing")
        return seeded
