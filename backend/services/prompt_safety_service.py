"""
Prompt Safety Service
---------------------
Fast preflight safety check for image/video/text-enhancement prompts.

Two-level rule engine:
  HARD BLOCK  – graphic violence, explicit sexual, minors, self-harm, hate, real-person deepfakes, copyright
  SOFT WARN   – tense horror, weapons present, chase scenes, franchise-inspired, dark supernatural

Returns a structured dict so the frontend can show an instant modal without
ever hitting the upstream provider.

Usage:
    from backend.services.prompt_safety_service import check_prompt_safety

    result = check_prompt_safety(prompt, medium="video", provider="seedance", user_id="abc")
    if result["decision"] != "allow":
        return jsonify(result), 451  # or 422
"""

from __future__ import annotations

import re
import time
from typing import Dict, List, Optional

# ─────────────────────────────────────────────────────────────
# Category definitions
# ─────────────────────────────────────────────────────────────
CATEGORIES = {
    "violence",
    "sexual",
    "minors",
    "self_harm",
    "hate",
    "real_person",
    "copyright",
    "weapons",
    "horror",
    "dark_supernatural",
}

# ─────────────────────────────────────────────────────────────
# HARD BLOCK patterns  (decision = "block")
# Each entry: (compiled regex, category, weight)
# ─────────────────────────────────────────────────────────────
_HARD_BLOCK_RAW: List[tuple] = [
    # ── Graphic violence ──
    (r"\b(?:shooting|shoots?|shot)\b.*\b(?:person|people|man|woman|officer|soldier|figure|character|victim|target|body|head|chest)\b", "violence", 10),
    (r"\b(?:person|people|man|woman|officer|soldier|figure|character|victim)\b.*\b(?:shooting|shoots?|shot|fired?\b)", "violence", 10),
    (r"\bfiring\s+(?:controlled\s+)?shots?\b", "violence", 10),
    (r"\b(?:figure|person|body|victim)\s+(?:drops?|falls?|collapses?|slumps?)\b", "violence", 8),
    (r"\b(?:execution|execute[sd]?|executing)\b", "violence", 12),
    (r"\b(?:decapitat\w+|dismember\w+|disembowel\w+)\b", "violence", 12),
    (r"\b(?:torture[sd]?|torturing)\b", "violence", 12),
    (r"\b(?:gore|gory|goriest)\b", "violence", 10),
    (r"\b(?:blood\s*(?:splatter|spray|pool|gush|spurt|drip))\b", "violence", 10),
    (r"\bmassacre[sd]?\b", "violence", 12),
    (r"\bslaughter(?:s|ed|ing)?\b", "violence", 10),
    (r"\bstabb?(?:s|ed|ing)\b", "violence", 10),
    (r"\b(?:gunfire|gunshot|muzzle\s*flash)\b.*\b(?:hit|strike|impact|wound|drop|fall|kill)\b", "violence", 10),
    (r"\b(?:head\s*shot|kill\s*shot|lethal\s*shot)\b", "violence", 12),
    (r"\b(?:sniper|assassin)\b.*\b(?:shoot|fire|kill|target)\b", "violence", 10),
    (r"\bmurder(?:s|ed|ing)?\b", "violence", 10),
    (r"\b(?:beat(?:s|ing)?|bludgeon\w*)\s+(?:to\s+)?death\b", "violence", 12),
    (r"\b(?:strangl\w+|chok(?:e[sd]?|ing)\s+(?:to\s+)?death)\b", "violence", 10),

    # ── Explicit sexual content ──
    (r"\b(?:explicit\s+)?(?:sex(?:ual)?\s+(?:act|intercourse|scene|content))\b", "sexual", 12),
    (r"\b(?:pornograph\w+|hardcore\s+porn)\b", "sexual", 12),
    (r"\b(?:nude|naked)\s+(?:scene|body|figure|woman|man|girl|boy)\b", "sexual", 10),
    (r"\b(?:genitalia|genital|penis|vagina|erection|orgasm)\b", "sexual", 12),
    (r"\b(?:masturbat\w+)\b", "sexual", 12),
    (r"\b(?:hentai|rule\s*34)\b", "sexual", 12),

    # ── Minors in sexual or violent context ──
    (r"\b(?:child|minor|underage|kid|toddler|infant|baby)\b.*\b(?:sex\w*|nude|naked|erotic|pornograph\w*|explicit)\b", "minors", 20),
    (r"\b(?:sex\w*|nude|naked|erotic|pornograph\w*)\b.*\b(?:child|minor|underage|kid|toddler|infant|baby)\b", "minors", 20),
    (r"\b(?:child|minor|underage|kid)\b.*\b(?:torture|murder|gore|execution|kill|stab|shoot)\b", "minors", 18),
    (r"\bpedophil\w*\b", "minors", 20),
    (r"\b(?:loli|shota)\b", "minors", 20),

    # ── Self-harm / suicide promotion ──
    (r"\b(?:suicide|suicidal)\b.*\b(?:method|how\s+to|instruction|guide|step)\b", "self_harm", 15),
    (r"\b(?:cut(?:ting)?|slit(?:ting)?)\s+(?:wrist|vein|arm)\b", "self_harm", 12),
    (r"\b(?:hang(?:ing)?\s+(?:my|your|them)self)\b", "self_harm", 12),
    (r"\b(?:encourage|promot)\w*\s+(?:self[\s-]?harm|suicide)\b", "self_harm", 15),

    # ── Hate / extremist praise ──
    (r"\b(?:nazi|white\s*supremac\w+|ethnic\s*cleansing|racial\s*purity)\b", "hate", 15),
    (r"\b(?:heil\s+hitler|sieg\s+heil|1488|fourteen\s*words)\b", "hate", 15),
    (r"\b(?:ISIS|al[\s-]*qaeda|jihad(?:ist)?)\b.*\b(?:praise|glory|support|join)\b", "hate", 15),
    (r"\b(?:praise|glorif\w+|celebrat\w+)\b.*\b(?:nazi|hitler|terrorism|terrorist)\b", "hate", 15),

    # ── Real-person likeness in dangerous/deceptive scenes ──
    (r"\b(?:deepfake|face\s*swap)\b.*\b(?:real|actual|celebrity|politician|public\s*figure)\b", "real_person", 12),
    (r"\b(?:Trump|Biden|Obama|Putin|Xi\s*Jinping|Zelensky)\b.*\b(?:shoot|kill|murder|dead|nude|naked|sex|bomb|attack|stab|torture|execute)\b", "real_person", 15),
    (r"\b(?:shoot|kill|murder|dead|nude|naked|sex|bomb|attack|stab|torture|execute)\b.*\b(?:Trump|Biden|Obama|Putin|Xi\s*Jinping|Zelensky)\b", "real_person", 15),
    (r"\b(?:Taylor\s*Swift|Beyonce|Ariana\s*Grande|Billie\s*Eilish)\b.*\b(?:nude|naked|sex|pornograph\w*|deepfake)\b", "real_person", 15),
    (r"\b(?:nude|naked|sex|pornograph\w*|deepfake)\b.*\b(?:Taylor\s*Swift|Beyonce|Ariana\s*Grande|Billie\s*Eilish)\b", "real_person", 15),

    # ── Direct copyright recreation ──
    (r"\b(?:exact|identical|pixel[\s-]*perfect|frame[\s-]*by[\s-]*frame)\s+(?:copy|replica|recreation|reproduction)\b.*\b(?:scene|movie|film|show)\b", "copyright", 10),
    (r"\b(?:recreat\w+|reproduc\w+|replica\w*)\b.*\b(?:copyrighted|trademarked|Disney|Marvel|DC\s*Comics|Warner\s*Bros|Nintendo|Pixar)\b", "copyright", 10),
    (r"\b(?:Disney|Marvel|DC\s*Comics|Warner\s*Bros|Nintendo|Pixar)\b.*\b(?:exact\s+(?:copy|replica)|recreat\w+|reproduc\w+)\b", "copyright", 10),
]

# ─────────────────────────────────────────────────────────────
# SOFT WARN patterns  (decision = "warn")
# ─────────────────────────────────────────────────────────────
_SOFT_WARN_RAW: List[tuple] = [
    # ── Tense horror ──
    (r"\b(?:horror|terrif\w+|nightmar\w+)\b.*\b(?:scene|moment|creature)\b", "horror", 5),
    (r"\bjump\s*scare\b", "horror", 4),
    (r"\bcreepy\b", "horror", 3),
    (r"\bsinister\b", "horror", 3),
    (r"\beerie\b", "horror", 3),
    (r"\b(?:scream(?:s|ing)?|shriek\w*)\b.*\b(?:terror|fear|horror)\b", "horror", 5),

    # ── Weapons present but not used ──
    (r"\b(?:holding|brandish\w+|wield\w+|carry\w*)\s+(?:a\s+)?(?:gun|rifle|pistol|sword|knife|weapon|blade|axe)\b", "weapons", 5),
    (r"\b(?:gun|rifle|pistol|sword|knife|weapon|blade|axe)\s+(?:on\s+(?:table|desk|wall)|in\s+hand|strapped|holstered)\b", "weapons", 4),
    (r"\b(?:armed|armored|weaponized)\b", "weapons", 4),

    # ── Chase / threat scenes without explicit harm ──
    (r"\b(?:chas(?:e[sd]?|ing))\b.*\b(?:through|down|across|into)\b", "horror", 4),
    (r"\b(?:fleeing|running\s+(?:from|away))\b.*\b(?:threat|danger|attack\w*)\b", "horror", 4),
    (r"\b(?:menac\w+|threaten\w+|intimidat\w+)\b", "horror", 4),

    # ── "Inspired by" franchise prompts ──
    (r"\b(?:inspired\s+by|in\s+the\s+style\s+of|reminiscent\s+of)\b.*\b(?:Star\s*Wars|Lord\s+of\s+the\s+Rings|Harry\s*Potter|Game\s+of\s+Thrones|Marvel|DC|Stranger\s+Things)\b", "copyright", 5),
    (r"\b(?:looks?\s+like|similar\s+to|resembl\w+)\b.*\b(?:Darth\s*Vader|Spider[\s-]*Man|Batman|Superman|Iron[\s-]*Man|Gandalf|Yoda|Pikachu|Mario)\b", "copyright", 6),

    # ── Dark supernatural suspense ──
    (r"\b(?:demon(?:ic)?|possessed|exorcis\w+|occult|satanic)\b", "dark_supernatural", 5),
    (r"\b(?:ritual\s+sacrifice|blood\s+ritual|dark\s+ritual)\b", "dark_supernatural", 6),
    (r"\b(?:cursed|haunted|hex|voodoo)\b.*\b(?:scene|ritual|place)\b", "dark_supernatural", 4),

    # ── Borderline violence (not graphic enough to block) ──
    (r"\b(?:fight(?:ing)?|combat|battle|clash)\b", "weapons", 3),
    (r"\b(?:punch\w*|kick\w*|strike[sd]?|hit(?:ting)?)\b.*\b(?:person|opponent|enemy|foe)\b", "violence", 5),
    (r"\b(?:blood|bleed\w*)\b", "violence", 4),
    (r"\b(?:wound(?:ed)?|injur\w+|scar(?:red)?)\b", "violence", 4),
    (r"\b(?:gun|rifle|pistol|firearm|shotgun|revolver|sniper)\b", "weapons", 4),
    (r"\b(?:explosion|explod\w+|detonate|blast)\b", "violence", 5),
]

# ─────────────────────────────────────────────────────────────
# Compile patterns once at import time
# ─────────────────────────────────────────────────────────────
_HARD_BLOCKS = [(re.compile(p, re.IGNORECASE), cat, w) for p, cat, w in _HARD_BLOCK_RAW]
_SOFT_WARNS  = [(re.compile(p, re.IGNORECASE), cat, w) for p, cat, w in _SOFT_WARN_RAW]


# ─────────────────────────────────────────────────────────────
# Provider / medium strictness multipliers
# ─────────────────────────────────────────────────────────────
_PROVIDER_STRICTNESS: Dict[str, float] = {
    # Video providers (stricter)
    "seedance":      1.4,
    "fal_seedance":  1.3,
    "vertex":        1.3,
    "veo":           1.3,
    # Image providers
    "openai":        1.0,
    "google":        1.0,
    "gemini":        1.0,
    "nano_banana":   1.0,
    # Text enhancement
    "text":          0.8,
}

_MEDIUM_STRICTNESS: Dict[str, float] = {
    "video":          1.3,
    "image":          1.0,
    "text":           0.8,
    "text_enhancement": 0.8,
}

# Block threshold: total weighted score above this → block
_BLOCK_THRESHOLD  = 8
# Warn threshold: total weighted score above this → warn
_WARN_THRESHOLD   = 6


# ─────────────────────────────────────────────────────────────
# Rewrite hints by category
# ─────────────────────────────────────────────────────────────
_REWRITE_HINTS: Dict[str, str] = {
    "violence":         "Remove explicit combat, shooting, or injury details. Focus on suspense, atmosphere, or aftermath instead.",
    "sexual":           "Remove explicit sexual content. Describe emotion, connection, or artistry instead.",
    "minors":           "Content involving minors in harmful contexts is strictly prohibited.",
    "self_harm":        "Remove self-harm or suicide references. Focus on hope, resilience, or recovery themes.",
    "hate":             "Remove hate speech, extremist references, or supremacist ideology.",
    "real_person":      "Remove real person names from violent, sexual, or deceptive scenes. Use fictional characters.",
    "copyright":        "Avoid exact recreation of copyrighted scenes. Use 'inspired by' language and original characters.",
    "weapons":          "Consider removing or de-emphasizing weapons. Focus on character emotion and story tension.",
    "horror":           "Tone down graphic horror elements. Atmospheric suspense and mystery are usually safe.",
    "dark_supernatural": "Soften occult/demonic imagery. Mystical or ethereal alternatives often pass moderation.",
}

# ─────────────────────────────────────────────────────────────
# Strike tracking  (in-memory with DB persistence)
# ─────────────────────────────────────────────────────────────
# In-memory cache: { user_id: [(timestamp, decision), ...] }
_strike_cache: Dict[str, list] = {}

_STRIKE_WINDOW_SEC = 24 * 60 * 60  # 24 hours
_PENALTY_FREE_STRIKES = 2          # first 2 violations: no penalty
_PENALTY_PER_STRIKE = 2            # credits per strike after grace period
_MAX_PENALTY = 10                  # cap per single penalty event


def _get_strikes_24h(user_id: str) -> int:
    """Count blocked/warned prompts for user in trailing 24h window."""
    if not user_id:
        return 0

    cutoff = time.time() - _STRIKE_WINDOW_SEC
    entries = _strike_cache.get(user_id, [])

    # Prune old entries
    entries = [e for e in entries if e[0] > cutoff]
    _strike_cache[user_id] = entries

    return len(entries)


def _record_strike(user_id: str, decision: str) -> int:
    """Record a strike and return updated 24h count."""
    if not user_id:
        return 0

    now = time.time()
    if user_id not in _strike_cache:
        _strike_cache[user_id] = []

    _strike_cache[user_id].append((now, decision))

    # Also persist to DB if available
    _persist_strike_to_db(user_id, decision, now)

    return _get_strikes_24h(user_id)


def _persist_strike_to_db(user_id: str, decision: str, timestamp: float):
    """Persist strike to database (best-effort, non-blocking)."""
    try:
        from backend.db import USE_DB, transaction
        if not USE_DB:
            return
        with transaction() as cur:
            cur.execute("""
                INSERT INTO timrx_app.safety_strikes
                    (identity_id, decision, created_at)
                VALUES (%s, %s, to_timestamp(%s))
            """, (user_id, decision, timestamp))
    except Exception as e:
        print(f"[SAFETY] Warning: could not persist strike: {e}")


def _load_strikes_from_db(user_id: str) -> List[tuple]:
    """Load recent strikes from DB into cache. Returns list of (timestamp, decision)."""
    try:
        from backend.db import USE_DB, query_all
        if not USE_DB:
            return []
        cutoff_ts = time.time() - _STRIKE_WINDOW_SEC
        rows = query_all("""
            SELECT EXTRACT(EPOCH FROM created_at) AS ts, decision
            FROM timrx_app.safety_strikes
            WHERE identity_id = %s
              AND created_at > to_timestamp(%s)
            ORDER BY created_at DESC
        """, (user_id, cutoff_ts))
        return [(float(r["ts"]), r["decision"]) for r in rows]
    except Exception as e:
        print(f"[SAFETY] Warning: could not load strikes from DB: {e}")
        return []


def _compute_penalty(strike_count: int) -> int:
    """Compute credit penalty based on strike count."""
    if strike_count <= _PENALTY_FREE_STRIKES:
        return 0
    excess = strike_count - _PENALTY_FREE_STRIKES
    return min(excess * _PENALTY_PER_STRIKE, _MAX_PENALTY)


def _apply_credit_penalty(user_id: str, amount: int):
    """Deduct penalty credits from user wallet via the wallet service (best-effort)."""
    if amount <= 0 or not user_id:
        return
    try:
        from backend.db import USE_DB
        if not USE_DB:
            return
        from backend.services.wallet_service import WalletService
        WalletService.add_ledger_entry(
            identity_id=user_id,
            entry_type="safety_penalty",
            delta=-amount,
            credit_type="general",
            ref_type="safety",
            ref_id=None,
            meta={"penalty_credits": amount, "source": "prompt_safety"},
        )
        print(f"[SAFETY] Applied {amount} credit penalty to user {user_id}")
    except Exception as e:
        print(f"[SAFETY] Warning: could not apply credit penalty: {e}")


# ─────────────────────────────────────────────────────────────
# Main API
# ─────────────────────────────────────────────────────────────
def check_prompt_safety(
    prompt: str,
    medium: str = "image",
    provider: str = "openai",
    user_id: Optional[str] = None,
) -> Dict:
    """
    Fast preflight safety check for a generation prompt.

    Args:
        prompt:   The user's raw prompt text.
        medium:   "image", "video", or "text" / "text_enhancement".
        provider: Provider name (e.g. "openai", "seedance", "vertex").
        user_id:  Optional identity_id for strike tracking.

    Returns:
        {
            "decision":        "allow" | "warn" | "block",
            "categories":      ["violence", ...],
            "message":         "Human-readable explanation",
            "rewrite_hint":    "Suggestion for safer prompt",
            "strike_count_24h": int,
            "credit_penalty":  int,
            "penalty_notice":  str or None,
        }
    """
    if not prompt or not prompt.strip():
        return _allow_result()

    text = prompt.strip()
    provider_lower = (provider or "").lower()
    medium_lower = (medium or "image").lower()

    provider_mult = _PROVIDER_STRICTNESS.get(provider_lower, 1.0)
    medium_mult = _MEDIUM_STRICTNESS.get(medium_lower, 1.0)
    total_mult = provider_mult * medium_mult

    # ── Phase 1: Hard-block scan ──
    block_score = 0.0
    block_categories = set()

    for pattern, category, weight in _HARD_BLOCKS:
        if pattern.search(text):
            adjusted = weight * total_mult
            block_score += adjusted
            block_categories.add(category)

    if block_score >= _BLOCK_THRESHOLD or block_categories & {"minors", "self_harm"}:
        # Always block minors/self_harm regardless of score
        cats = sorted(block_categories)
        strike_count = 0
        penalty = 0
        penalty_notice = None

        if user_id:
            # Ensure cache is primed from DB
            if user_id not in _strike_cache:
                _strike_cache[user_id] = _load_strikes_from_db(user_id)
            strike_count = _record_strike(user_id, "block")
            penalty = _compute_penalty(strike_count)
            if penalty > 0:
                _apply_credit_penalty(user_id, penalty)
                penalty_notice = f"A {penalty}-credit penalty has been applied. Repeated blocked prompts can lead to increasing penalties."
            elif strike_count >= _PENALTY_FREE_STRIKES:
                penalty_notice = "Repeated blocked prompts may lead to small credit penalties."

        primary_cat = cats[0] if cats else "violence"
        return {
            "decision": "block",
            "categories": cats,
            "message": _block_message(cats),
            "rewrite_hint": _REWRITE_HINTS.get(primary_cat, _REWRITE_HINTS["violence"]),
            "strike_count_24h": strike_count,
            "credit_penalty": penalty,
            "penalty_notice": penalty_notice,
        }

    # ── Phase 2: Soft-warn scan ──
    warn_score = block_score  # carry over any partial block-pattern hits
    warn_categories = set(block_categories)

    for pattern, category, weight in _SOFT_WARNS:
        if pattern.search(text):
            adjusted = weight * total_mult
            warn_score += adjusted
            warn_categories.add(category)

    if warn_score >= _WARN_THRESHOLD:
        cats = sorted(warn_categories)
        strike_count = 0
        penalty_notice = None

        if user_id:
            if user_id not in _strike_cache:
                _strike_cache[user_id] = _load_strikes_from_db(user_id)
            strike_count = _record_strike(user_id, "warn")
            # No penalty for warns, but inform if close
            if strike_count >= _PENALTY_FREE_STRIKES:
                penalty_notice = "Repeated flagged prompts may lead to small credit penalties."

        primary_cat = cats[0] if cats else "violence"
        return {
            "decision": "warn",
            "categories": cats,
            "message": _warn_message(cats),
            "rewrite_hint": _REWRITE_HINTS.get(primary_cat, _REWRITE_HINTS["violence"]),
            "strike_count_24h": strike_count,
            "credit_penalty": 0,
            "penalty_notice": penalty_notice,
        }

    # ── Phase 3: Allow ──
    return _allow_result()


# ─────────────────────────────────────────────────────────────
# Response builders
# ─────────────────────────────────────────────────────────────
def _allow_result() -> Dict:
    return {
        "decision": "allow",
        "categories": [],
        "message": "",
        "rewrite_hint": "",
        "strike_count_24h": 0,
        "credit_penalty": 0,
        "penalty_notice": None,
    }


def _block_message(categories: list) -> str:
    parts = []
    if "minors" in categories:
        parts.append("content involving minors in harmful contexts")
    if "violence" in categories:
        parts.append("graphic violence or explicit harm")
    if "sexual" in categories:
        parts.append("explicit sexual content")
    if "self_harm" in categories:
        parts.append("self-harm or suicide promotion")
    if "hate" in categories:
        parts.append("hate speech or extremist content")
    if "real_person" in categories:
        parts.append("real-person likeness in dangerous or deceptive scenes")
    if "copyright" in categories:
        parts.append("likely copyright-infringing recreation")

    if not parts:
        parts.append("content that may violate provider safety policies")

    detail = ", ".join(parts)
    return (
        f"This prompt was blocked because it contains {detail}. "
        f"Sending this to the AI provider would likely be rejected and "
        f"could result in account penalties."
    )


def _warn_message(categories: list) -> str:
    parts = []
    if "horror" in categories:
        parts.append("intense horror elements")
    if "weapons" in categories:
        parts.append("weapons or armed characters")
    if "violence" in categories:
        parts.append("borderline violent content")
    if "copyright" in categories:
        parts.append("franchise-inspired elements that may trigger moderation")
    if "dark_supernatural" in categories:
        parts.append("dark supernatural or occult imagery")
    if "sexual" in categories:
        parts.append("suggestive content")
    if "real_person" in categories:
        parts.append("real-person references")

    if not parts:
        parts.append("elements that may trigger provider moderation")

    detail = ", ".join(parts)
    return (
        f"This prompt may need adjustment — it contains {detail}. "
        f"Some AI providers may reject or filter this content. "
        f"Consider revising before generating."
    )


# ─────────────────────────────────────────────────────────────
# DB schema for strikes
# ─────────────────────────────────────────────────────────────
def ensure_safety_schema():
    """
    Create the safety_strikes table if it doesn't exist.
    Called from db.ensure_schema() at app startup.
    """
    try:
        from backend.db import USE_DB, transaction
        if not USE_DB:
            return
        with transaction() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS timrx_app.safety_strikes (
                    id            BIGSERIAL PRIMARY KEY,
                    identity_id   UUID NOT NULL,
                    decision      TEXT NOT NULL CHECK (decision IN ('block', 'warn')),
                    created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
            """)
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_safety_strikes_identity_time
                ON timrx_app.safety_strikes (identity_id, created_at DESC)
            """)
        print("[SAFETY] safety_strikes table ensured")
    except Exception as e:
        print(f"[SAFETY] Warning: could not ensure safety schema: {e}")
