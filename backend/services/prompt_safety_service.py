"""
Prompt Safety Service  v3 – fast preflight safety check for generation prompts.

Category-bucketed scoring, safe-context reduction, per-category thresholds,
structured debug logging, provider outcome learning, analytics summary.

Call check_prompt_safety(prompt, medium, provider) for live checks.
Pass dry_run=True for evaluation without strikes or penalties.
"""

from __future__ import annotations

import hashlib
import json
import re
import time
from typing import Dict, List, Optional, Tuple

# ─────────────────────────────────────────────────────────────
# Negation stripping
# ─────────────────────────────────────────────────────────────
_NEGATION_RE = re.compile(
    r"(?:(?:non|not|no|without|never|isn'?t|aren'?t|wasn'?t|weren'?t|don'?t|doesn'?t)"
    r"[\s\-]+\w+(?:\s+\w+){0,2})",
    re.IGNORECASE,
)

def _strip_negations(text: str) -> str:
    """Remove negated phrases so 'non-threatening' doesn't match 'threatening'."""
    return _NEGATION_RE.sub(" ", text)


# ─────────────────────────────────────────────────────────────
# Pattern rules:  (regex, category, weight, rule_id)
# ─────────────────────────────────────────────────────────────
_RULES_RAW: List[Tuple[str, str, int, str]] = [
    # ═════════════════════════════════════════════════════════
    # VIOLENCE – explicit harmful actions against people
    # ═════════════════════════════════════════════════════════
    (r"\b(?:shoot(?:s|ing)?|shot)\b.*\b(?:person|people|man|woman|officer|soldier|figure|character|victim|body|head|chest|crowd)\b", "violence", 14, "V01_shoot_person"),
    (r"\b(?:person|people|man|woman|officer|soldier|figure|character|victim)\b.*\b(?:shoot(?:s|ing)?|shot|fire[sd]?\s+(?:at|on|upon))\b", "violence", 14, "V02_person_shot"),
    (r"\bfiring\s+(?:controlled\s+)?shots?\s+(?:at|into|toward)\b", "violence", 14, "V03_firing_shots_at"),
    (r"\b(?:shoots?|shoot(?:s|ing)?)\s+(?:multiple|several|many)\s+(?:people|victims|targets)\b", "violence", 16, "V04_mass_shooting"),
    (r"\bstabb?(?:s|ed|ing)\b.*\b(?:person|people|man|woman|victim|body)\b", "violence", 14, "V05_stab_person"),
    (r"\b(?:person|man|woman|victim)\b.*\bstabb?(?:s|ed|ing)\b", "violence", 14, "V06_person_stabbed"),
    (r"\bstabb?(?:s|ed|ing)\s+repeatedly\b", "violence", 16, "V07_stab_repeatedly"),
    (r"\b(?:kill(?:s|ed|ing)?|murder(?:s|ed|ing)?)\b.*\b(?:person|people|man|woman|victim|officer)\b", "violence", 14, "V08_kill_person"),
    (r"\b(?:person|man|woman|victim|officer)\b.*\b(?:kill(?:s|ed|ing)?|murder(?:s|ed|ing)?)\b", "violence", 14, "V09_person_killed"),
    (r"\b(?:execution|execute[sd]?|executing)\b", "violence", 16, "V10_execution"),
    (r"\b(?:torture[sd]?|torturing)\b", "violence", 16, "V11_torture"),
    (r"\b(?:decapitat\w+|dismember\w+|disembowel\w+)\b", "violence", 18, "V12_dismember"),
    (r"\b(?:massacre[sd]?|slaughter(?:s|ed|ing)?)\b", "violence", 16, "V13_massacre"),
    (r"\b(?:beat(?:s|ing)?|bludgeon\w*)\s+(?:to\s+)?death\b", "violence", 16, "V14_beat_death"),
    (r"\b(?:strangl\w+|chok(?:e[sd]?|ing)\s+(?:to\s+)?death)\b", "violence", 14, "V15_strangle"),
    (r"\b(?:gore|gory|goriest)\b", "violence", 12, "V16_gore"),
    (r"\bblood\s*(?:splatter|spray|pool|gush|spurt|drip|soaked|stained)\b", "violence", 12, "V17_blood_graphic"),
    (r"\b(?:head\s*shot|kill\s*shot|lethal\s*shot)\b", "violence", 14, "V18_headshot"),
    (r"\b(?:sniper|assassin)\b.*\b(?:shoot|fire|kill|target)\b", "violence", 12, "V19_sniper_kill"),
    (r"\b(?:gunfire|gunshot|muzzle\s*flash)\b.*\b(?:hit|strike|wound|drop|fall|kill|impact)\b", "violence", 12, "V20_gunfire_outcome"),
    (r"\b(?:tactical\s+raid|armed\s+assault|armed\s+attack)\b", "violence", 10, "V21_armed_conflict"),
    (r"\b(?:firing\s+squad|point[\s-]blank)\b", "violence", 14, "V22_firing_squad"),

    # ═════════════════════════════════════════════════════════
    # SEXUAL
    # ═════════════════════════════════════════════════════════
    (r"\b(?:explicit\s+)?(?:sex(?:ual)?\s+(?:act|intercourse|scene|content))\b", "sexual", 16, "S01_sexual_act"),
    (r"\b(?:pornograph\w+|hardcore\s+porn)\b", "sexual", 16, "S02_porn"),
    (r"\b(?:nude|naked)\s+(?:scene|body|figure|woman|man|girl|boy)\b", "sexual", 14, "S03_nude_person"),
    (r"\b(?:genitalia|genital|penis|vagina|erection|orgasm)\b", "sexual", 16, "S04_genitalia"),
    (r"\b(?:masturbat\w+)\b", "sexual", 16, "S05_masturbation"),
    (r"\b(?:hentai|rule\s*34)\b", "sexual", 16, "S06_hentai"),

    # ═════════════════════════════════════════════════════════
    # MINORS  (always block – threshold = 1)
    # ═════════════════════════════════════════════════════════
    (r"\b(?:child|minor|underage|kid|toddler|infant|baby)\b.*\b(?:sex\w*|nude|naked|erotic|pornograph\w*|explicit)\b", "minors", 30, "M01_minor_sexual"),
    (r"\b(?:sex\w*|nude|naked|erotic|pornograph\w*)\b.*\b(?:child|minor|underage|kid|toddler|infant|baby)\b", "minors", 30, "M02_sexual_minor"),
    (r"\b(?:child|minor|underage|kid)\b.*\b(?:torture|murder|gore|execution|kill|stab|shoot)\b", "minors", 25, "M03_minor_violence"),
    (r"\bpedophil\w*\b", "minors", 30, "M04_pedophilia"),
    (r"\b(?:loli|shota)\b", "minors", 30, "M05_loli"),

    # ════════════════════���════════════════════════════════════
    # SELF_HARM  (always block – threshold = 1)
    # ═════════════════════════════════════════════════════════
    (r"\b(?:suicide|suicidal)\b.*\b(?:method|how\s+to|instruction|guide|step)\b", "self_harm", 20, "H01_suicide_guide"),
    (r"\b(?:cut(?:ting)?|slit(?:ting)?)\s+(?:wrist|vein|arm)\b", "self_harm", 16, "H02_cutting"),
    (r"\b(?:hang(?:ing)?\s+(?:my|your|them)self)\b", "self_harm", 16, "H03_hanging"),
    (r"\b(?:encourage|promot)\w*\s+(?:self[\s-]?harm|suicide)\b", "self_harm", 20, "H04_promote_selfharm"),

    # ═════════════════════════════════════════════════════════
    # HATE
    # ═════════════════════════════════════════════════════════
    (r"\b(?:nazi|white\s*supremac\w+|ethnic\s*cleansing|racial\s*purity)\b", "hate", 18, "E01_nazi"),
    (r"\b(?:heil\s+hitler|sieg\s+heil|1488|fourteen\s*words)\b", "hate", 18, "E02_heil"),
    (r"\b(?:ISIS|al[\s-]*qaeda|jihad(?:ist)?)\b.*\b(?:praise|glory|support|join)\b", "hate", 18, "E03_terrorist_praise"),
    (r"\b(?:praise|glorif\w+|celebrat\w+)\b.*\b(?:nazi|hitler|terrorism|terrorist)\b", "hate", 18, "E04_glorify_hate"),

    # ═════════════════════════════════════════════════════════
    # REAL_PERSON
    # ═════════════════════════════════════════════════════════
    (r"\b(?:deepfake|face\s*swap)\b.*\b(?:real|actual|celebrity|politician|public\s*figure)\b", "real_person", 16, "R01_deepfake"),
    (r"\b(?:Trump|Biden|Obama|Putin|Xi\s*Jinping|Zelensky)\b.*\b(?:shoot|kill|murder|dead|nude|naked|sex|bomb|attack|stab|torture|execute)\b", "real_person", 18, "R02_politician_harm"),
    (r"\b(?:shoot|kill|murder|dead|nude|naked|sex|bomb|attack|stab|torture|execute)\b.*\b(?:Trump|Biden|Obama|Putin|Xi\s*Jinping|Zelensky)\b", "real_person", 18, "R03_harm_politician"),
    (r"\b(?:Taylor\s*Swift|Beyonce|Ariana\s*Grande|Billie\s*Eilish)\b.*\b(?:nude|naked|sex|pornograph\w*|deepfake)\b", "real_person", 18, "R04_celeb_sexual"),
    (r"\b(?:nude|naked|sex|pornograph\w*|deepfake)\b.*\b(?:Taylor\s*Swift|Beyonce|Ariana\s*Grande|Billie\s*Eilish)\b", "real_person", 18, "R05_sexual_celeb"),
    (r"\b(?:famous|real|actual)\s+(?:actor|actress|person|celebrity)\b.*\b(?:commit\w*|violent\s+crime|murder|kill|attack)\b", "real_person", 16, "R06_actor_crime"),
    (r"\b(?:realistic\s+video)\b.*\b(?:famous|real|actual)\s+(?:actor|actress|person|celebrity)\b.*\b(?:crime|murder|kill|attack|violent)\b", "real_person", 18, "R07_realistic_actor_crime"),

    # ═════════════════════════════════════════════════════════
    # COPYRIGHT
    # Exact recreation → block.  Franchise-inspired → warn only.
    # ═════════════════════════════════════════════════════════
    # -- Exact recreation (high weight → block) --
    (r"\b(?:exact|identical|pixel[\s-]*perfect|frame[\s-]*by[\s-]*frame)\s+(?:copy|replica|recreation|reproduction)\b.*\b(?:scene|movie|film|show)\b", "copyright", 16, "C01_exact_copy"),
    (r"\b(?:recreat\w+|reproduc\w+|replica\w*)\b.*\b(?:copyrighted|trademarked|Disney|Marvel|DC\s*Comics|Warner\s*Bros|Nintendo|Pixar)\b", "copyright", 16, "C02_recreate_brand"),
    (r"\b(?:Disney|Marvel|DC\s*Comics|Warner\s*Bros|Nintendo|Pixar)\b.*\b(?:exact\s+(?:copy|replica)|recreat\w+|reproduc\w+)\b", "copyright", 16, "C03_brand_recreate"),
    (r"\b(?:recreat\w*|exact\s+scene)\b.*\b(?:famous\s+(?:superhero|movie|film|show))\b", "copyright", 14, "C04_recreate_famous"),
    # -- Named character in original costume/style (mid weight → block) --
    (r"\b(?:Darth\s*Vader|Spider[\s-]*Man|Batman|Superman|Iron[\s-]*Man|Gandalf|Yoda|Pikachu|Mario|Mickey\s*Mouse|Elsa|Woody|Buzz\s*Lightyear)\b.*\b(?:in\s+(?:his|her|their|the)\s+(?:(?:original|iconic|classic|official)\s+)*(?:costume|suit|outfit|armor|style))\b", "copyright", 14, "C05_char_original_costume"),
    # -- Franchise-inspired (low weight → warn only, never block alone) --
    (r"\b(?:inspired\s+by|in\s+the\s+style\s+of|reminiscent\s+of)\b.*\b(?:Star\s*Wars|Lord\s+of\s+the\s+Rings|Harry\s*Potter|Game\s+of\s+Thrones|Marvel|DC|Stranger\s+Things)\b", "copyright", 4, "C06_franchise_inspired"),
    (r"\b(?:looks?\s+like|similar\s+to|resembl\w+)\b.*\b(?:Darth\s*Vader|Spider[\s-]*Man|Batman|Superman|Iron[\s-]*Man|Gandalf|Yoda|Pikachu|Mario)\b", "copyright", 4, "C07_looks_like_character"),

    # ═════════════════════════════════════════════════════════
    # WEAPONS  (low weight, never blocks alone)
    # ═════════════════════════════════════════════════════════
    (r"\b(?:holding|brandish\w+|wield\w+|carry\w*)\s+(?:a\s+)?(?:gun|rifle|pistol|sword|knife|weapon|blade|axe|machete)\b", "weapons", 5, "W01_holding_weapon"),
    (r"\b(?:armed|weaponized)\b", "weapons", 4, "W02_armed"),
    (r"\b(?:gun|rifle|pistol|firearm|shotgun|revolver)\b", "weapons", 4, "W03_gun_present"),
    (r"\b(?:fight(?:ing)?|combat|battle)\b.*\b(?:scene|sequence)\b", "weapons", 3, "W04_combat_scene"),

    # ═════════════════════════════════════════════════════════
    # HORROR  (low weight, never blocks alone)
    # ═════════════════════════════════════════════════════════
    (r"\b(?:horror|terrif\w+|nightmar\w+)\b.*\b(?:scene|moment|creature)\b", "horror", 4, "HR01_horror_scene"),
    (r"\bjump\s*scare\b", "horror", 4, "HR02_jumpscare"),
    (r"\b(?:scream(?:s|ing)?|shriek\w*)\b.*\b(?:terror|fear|horror)\b", "horror", 4, "HR03_scream_terror"),
    (r"\b(?:demon(?:ic)?|possessed|exorcis\w+|occult|satanic)\b", "horror", 4, "HR04_demonic"),
    (r"\b(?:ritual\s+sacrifice|blood\s+ritual|dark\s+ritual)\b", "horror", 5, "HR05_ritual"),

    # ═════════════════════════════════════════════════════════
    # LOW-SIGNAL (tiny scores, never block alone)
    # ═════════════════════════════════════════════════════════
    (r"\b(?:blood|bleed(?:s|ing)?)\b", "violence", 3, "LO01_blood"),
    (r"\b(?:wound(?:ed)?|injur\w+)\b", "violence", 3, "LO02_wound"),
    (r"\b(?:explosion|explod\w+|detonate|blast)\b", "violence", 3, "LO03_explosion"),
    (r"\b(?:punch\w*|kick\w*)\b.*\b(?:person|opponent|enemy|face)\b", "violence", 4, "LO04_punch"),
]

_RULES = [(re.compile(p, re.IGNORECASE), cat, w, rid) for p, cat, w, rid in _RULES_RAW]


# ─────────────────────────────────────────────────────────────
# Safe-context signals
# ─────────────────────────────────────────────────────────────
_SAFE_CONTEXT_WORDS = re.compile(
    r"\b(?:explore|explorer|walk(?:s|ing)?|wander|observe|observing|"
    r"flashlight|ambient|atmosphere|atmospheric|suspense|suspenseful|"
    r"corridor|hallway|tunnel|subway|passage|staircase|"
    r"curiosity|curious|silence|silent|quiet|calm|peaceful|"
    r"cinematic|camera\s+(?:dolly|pan|track|move|shift|angle)|"
    r"soft\s+light|warm\s+light|gentle|fading|drifting|"
    r"footsteps|echoes|reflections|shadows|dust|fog|mist|"
    r"abandoned|empty|desolate|solitary|lone|lonely|"
    r"music|ambient\s+music|soundtrack|resolve[sd]?)\b",
    re.IGNORECASE,
)

_HARMFUL_ACTION_VERBS = re.compile(
    r"\b(?:shoot(?:s|ing)|shooting|"  # "shot" removed — matches "macro shot"
    r"fire[sd]?\s+(?:at|on|upon|into)|"
    r"stab(?:s|bed|bing)?|kill(?:s|ed|ing)?|murder(?:s|ed|ing)?|"
    r"execut\w+|tortur\w+|slaughter\w*|massacr\w+|"
    r"decapitat\w+|dismember\w+|disembowel\w+|bludgeon\w*|strangl\w+|"
    r"attack(?:s|ed|ing)?|assault(?:s|ed|ing)?)\b",
    re.IGNORECASE,
)
# NOTE: "shot" alone was removed from harmful-verb detector because it
# matches photography terms ("macro shot", "cinematic shot").  The actual
# violence rules (V01-V02) still match "shot" when combined with human
# targets, so real violence is still caught.

_SAFE_SCORE_THRESHOLD = 4
_SAFE_DAMPEN_FACTOR   = 0.3


# ─────────────────────────────────────────────────────────────
# Non-human subject detector
# If the prompt is about animals / insects / nature / macro photography
# and contains NO human subject words, zero out violence and sexual scores.
# ─────────────────────────────────────────────────────────────
_NONHUMAN_SUBJECTS = re.compile(
    r"\b(?:spider|insect|beetle|butterfly|moth|ant|bee|wasp|dragonfly|"
    r"caterpillar|ladybug|mantis|cricket|fly|mosquito|"
    r"dog|cat|bird|fish|horse|deer|bear|wolf|fox|rabbit|squirrel|"
    r"lizard|snake|frog|turtle|gecko|chameleon|salamander|"
    r"owl|eagle|hawk|parrot|penguin|dolphin|whale|shark|octopus|"
    r"lion|tiger|elephant|giraffe|zebra|gorilla|monkey|"
    r"animal|wildlife|creature|specimen|organism|"
    r"flower|plant|moss|lichen|mushroom|fungus|coral|"
    r"macro\s+(?:shot|photo\w*|lens|detail|close[\s-]?up)|"
    r"macro\s+photography|close[\s-]?up\s+(?:shot|photo\w*)|"
    r"water\s+droplet|dew\s+drop|pollen|petal|leaf|leaves|branch|twig)\b",
    re.IGNORECASE,
)

_HUMAN_SUBJECTS = re.compile(
    r"\b(?:person|people|man|woman|girl|boy|child|kid|officer|soldier|"
    r"figure|character|victim|human|nude|naked|"
    r"actor|actress|celebrity|politician)\b",
    re.IGNORECASE,
)
# NOTE: "body" was removed from _HUMAN_SUBJECTS — it matches animal body
# references ("its body", "spider body") causing false positives.
# "body" only appears in rule patterns where it's already contextual
# (e.g. "nude body" in S03, "stab.*body" in V05).

# Photography / technical metadata — never risk-relevant
_PHOTO_METADATA = re.compile(
    r"\b(?:ultra[\s-]?realistic|hyper[\s-]?detailed|photo[\s-]?realistic|"
    r"macro|close[\s-]?up|bokeh|depth\s+of\s+field|lens|aperture|"
    r"8[kK]|4[kK]|HDR|RAW|DSLR|mirrorless|"
    r"f[\s/]?\d+(?:\.\d+)?|ISO\s*\d+|focal\s+length|"
    r"studio\s+lighting|natural\s+lighting|golden\s+hour|"
    r"product\s+photo\w*|stock\s+photo\w*)\b",
    re.IGNORECASE,
)

# High-confidence rule prefixes that justify penalties
_HIGH_CONFIDENCE_RULE_PREFIXES = {
    "V01", "V02", "V03", "V04", "V05", "V06", "V07", "V08", "V09",
    "V10", "V11", "V12", "V13", "V14", "V15", "V16", "V17", "V18",
    "S01", "S02", "S03", "S04", "S05", "S06",
    "M01", "M02", "M03", "M04", "M05",
    "H01", "H02", "H03", "H04",
    "E01", "E02", "E03", "E04",
    "R01", "R02", "R03", "R04", "R05", "R06", "R07",
}


# ─────────────────────────────────────────────────────────────
# Provider / medium strictness multipliers
# ─────────────────────────────────────────────────────────────
_PROVIDER_STRICTNESS: Dict[str, float] = {
    "seedance": 1.3, "fal_seedance": 1.2, "vertex": 1.3, "veo": 1.3,
    "openai": 1.0, "google": 1.0, "gemini": 1.0, "nano_banana": 1.0,
    "text": 0.8,
}

_MEDIUM_STRICTNESS: Dict[str, float] = {
    "video": 1.2, "image": 1.0, "text": 0.8, "text_enhancement": 0.8,
}

# ─────────────────────────────────────────────────────────────
# Per-category thresholds (after multiplier)
# ─────────────────────────────────────────────────────────────
_CATEGORY_THRESHOLDS: Dict[str, Dict[str, float]] = {
    "violence":    {"warn_at": 8,  "block_at": 12},
    "sexual":      {"warn_at": 8,  "block_at": 12},
    "minors":      {"warn_at": 1,  "block_at": 1},
    "self_harm":   {"warn_at": 1,  "block_at": 1},
    "hate":        {"warn_at": 8,  "block_at": 14},
    "real_person": {"warn_at": 8,  "block_at": 14},
    "copyright":   {"warn_at": 6,  "block_at": 14},
    "weapons":     {"warn_at": 8,  "block_at": 999},
    "horror":      {"warn_at": 10, "block_at": 999},
}
_DEFAULT_WARN_AT  = 8
_DEFAULT_BLOCK_AT = 14


# ─────────────────────────────────────────────────────────────
# Category-specific rewrite suggestions
# ─────────────────────────────────────────────────────────────
_REWRITE_HINTS: Dict[str, str] = {
    "violence":    "Remove explicit harm verbs (shoot, stab, kill) and focus on atmosphere, suspense, or the aftermath.",
    "sexual":      "Remove explicit nudity and sexual acts. Describe emotion, intimacy, or artistry instead.",
    "minors":      "Content involving minors in harmful contexts is strictly prohibited and cannot be rewritten.",
    "self_harm":   "Remove all self-harm references. Reframe around hope, recovery, or resilience.",
    "hate":        "Remove hate speech and extremist references entirely.",
    "real_person": "Replace real public figures with fictional characters to avoid deepfake / misuse flags.",
    "copyright":   "Describe original visual traits (silhouette, color palette, mood) instead of naming the franchise or character.",
    "weapons":     "De-emphasize weapons. Focus on character emotion and story tension instead.",
    "horror":      "Soften graphic horror into atmospheric suspense — fog, shadows, mystery.",
}

# Short category labels for the modal
_CATEGORY_LABELS: Dict[str, str] = {
    "violence":    "Explicit violence",
    "sexual":      "Sexual content",
    "minors":      "Minor safety",
    "self_harm":   "Self-harm",
    "hate":        "Hate / extremism",
    "real_person": "Real-person misuse",
    "copyright":   "Copyright concern",
    "weapons":     "Weapons present",
    "horror":      "Horror elements",
}


# ─────────────────────────────────────────────────────────────
# Strike tracking
# ─────────────────────────────────────────────────────────────
_strike_cache: Dict[str, list] = {}
_STRIKE_WINDOW_SEC    = 86400
_PENALTY_FREE_STRIKES = 2
_PENALTY_PER_STRIKE   = 2
_MAX_PENALTY          = 10

def _get_strikes_24h(user_id: str) -> int:
    if not user_id:
        return 0
    cutoff = time.time() - _STRIKE_WINDOW_SEC
    entries = _strike_cache.get(user_id, [])
    entries = [e for e in entries if e[0] > cutoff]
    _strike_cache[user_id] = entries
    return len(entries)

def _record_strike(user_id: str, decision: str,
                   categories: Optional[List[str]] = None,
                   matched_rules: Optional[List[str]] = None) -> int:
    if not user_id:
        return 0
    _strike_cache.setdefault(user_id, []).append((time.time(), decision))
    _persist_strike_to_db(user_id, decision, time.time(),
                          categories=categories, matched_rules=matched_rules)
    return _get_strikes_24h(user_id)

def _persist_strike_to_db(user_id, decision, timestamp,
                          categories=None, matched_rules=None):
    try:
        from backend.db import USE_DB, transaction
        if not USE_DB: return
        cats_json = json.dumps(categories or [])
        rules_json = json.dumps(matched_rules or [])
        with transaction() as cur:
            cur.execute(
                "INSERT INTO timrx_app.safety_strikes "
                "(identity_id, decision, categories, matched_rules, created_at) "
                "VALUES (%s, %s, %s, %s, to_timestamp(%s))",
                (user_id, decision, cats_json, rules_json, timestamp),
            )
    except Exception as e:
        print(f"[SAFETY] Warning: could not persist strike: {e}")

def _load_strikes_from_db(user_id):
    try:
        from backend.db import USE_DB, query_all
        if not USE_DB: return []
        cutoff = time.time() - _STRIKE_WINDOW_SEC
        rows = query_all("SELECT EXTRACT(EPOCH FROM created_at) AS ts, decision FROM timrx_app.safety_strikes WHERE identity_id = %s AND created_at > to_timestamp(%s) ORDER BY created_at DESC", (user_id, cutoff))
        return [(float(r["ts"]), r["decision"]) for r in rows]
    except Exception as e:
        print(f"[SAFETY] Warning: could not load strikes from DB: {e}")
        return []

def _get_db_strike_count_24h(user_id: str) -> int:
    """DB-confirmed 24h strike count — authoritative for penalty decisions."""
    try:
        from backend.db import USE_DB, query_one
        if not USE_DB:
            return _get_strikes_24h(user_id)  # fallback to cache
        row = query_one(
            "SELECT COUNT(*) AS cnt FROM timrx_app.safety_strikes "
            "WHERE identity_id = %s AND created_at > NOW() - INTERVAL '24 hours'",
            (user_id,),
        )
        return row["cnt"] if row else 0
    except Exception as e:
        print(f"[SAFETY] Warning: DB strike count failed, using cache: {e}")
        return _get_strikes_24h(user_id)

def _compute_penalty(strike_count):
    if strike_count <= _PENALTY_FREE_STRIKES: return 0
    return min((strike_count - _PENALTY_FREE_STRIKES) * _PENALTY_PER_STRIKE, _MAX_PENALTY)

def _apply_credit_penalty(user_id, amount):
    """
    Debit user wallet AND credit platform revenue in a single transaction.

    Both the user debit (ledger_entries) and platform credit (provider_ledger)
    share the same penalty_ref_id for cross-referencing.

    Idempotency: the ref_id includes a millisecond timestamp + user_id,
    so each penalty event produces a unique reference.  The wallet service
    allows negative balance for safety_penalty entries (system-imposed).

    Queryable via:
        SELECT * FROM timrx_billing.provider_ledger
        WHERE entry_type = 'safety_penalty_income'
        ORDER BY created_at DESC;
    """
    if amount <= 0 or not user_id:
        return
    try:
        from backend.db import USE_DB, get_conn, fetch_one, Tables
        if not USE_DB:
            return

        import uuid
        penalty_ref_id = f"safety_penalty_{uuid.uuid4().hex[:12]}"

        with get_conn() as conn:
            with conn.cursor() as cur:
                # ── 1. Debit user wallet (ledger_entries + wallet balance) ──
                # Lock wallet row
                cur.execute(f"""
                    SELECT identity_id, balance_credits
                    FROM {Tables.WALLETS}
                    WHERE identity_id = %s
                    FOR UPDATE
                """, (user_id,))
                wallet = fetch_one(cur)

                if not wallet:
                    print(f"[SAFETY] Warning: wallet not found for {user_id}, skipping penalty")
                    return

                old_balance = wallet.get("balance_credits", 0) or 0
                new_balance = old_balance - amount  # allowed to go negative

                # Insert user debit ledger entry
                cur.execute(f"""
                    INSERT INTO {Tables.LEDGER_ENTRIES}
                        (identity_id, entry_type, amount_credits, ref_type, ref_id,
                         meta, credit_type, created_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, NOW())
                """, (
                    user_id,
                    "safety_penalty",
                    -amount,
                    "safety",
                    penalty_ref_id,
                    json.dumps({"penalty_credits": amount, "source": "prompt_safety"}),
                    "general",
                ))

                # Update wallet balance
                cur.execute(f"""
                    UPDATE {Tables.WALLETS}
                    SET balance_credits = %s, updated_at = NOW()
                    WHERE identity_id = %s
                """, (new_balance, user_id))

                # ── 2. Credit platform revenue (provider_ledger) ──
                cur.execute(f"""
                    INSERT INTO {Tables.PROVIDER_LEDGER}
                        (provider, entry_type, amount_gbp, currency, description,
                         reference, metadata, recorded_by, created_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW())
                """, (
                    "platform",
                    "safety_penalty_income",
                    0,
                    "CREDITS",
                    f"Safety penalty: {amount} credits from {user_id}",
                    penalty_ref_id,
                    json.dumps({
                        "source_identity": user_id,
                        "penalty_credits": amount,
                        "credit_type": "general",
                    }),
                    "system:prompt_safety",
                ))

            # Both sides committed atomically
            conn.commit()

        print(
            f"[SAFETY] Penalty applied: user={user_id} debit=-{amount}cr "
            f"balance={old_balance}→{new_balance} ref={penalty_ref_id}"
        )
        print(
            f"[SAFETY] Platform revenue recorded: +{amount}cr "
            f"entry_type=safety_penalty_income ref={penalty_ref_id}"
        )

    except Exception as e:
        print(f"[SAFETY] Warning: could not apply credit penalty: {e}")


# ─────────────────────────────────────────────────────────────
# Provider outcome learning
# ─────────────────────────────────────────────────────────────
def record_provider_rejection(
    provider: str,
    medium: str,
    prompt: str,
    local_decision: str,
    matched_rules: List[str],
    cat_scores: Dict[str, float],
    rejection_code: str = "",
    rejection_message: str = "",
    job_id: str = "",
):
    """
    Record an upstream provider rejection for later analysis.
    Call this when a provider rejects a prompt that local safety allowed/warned.
    """
    prompt_hash = hashlib.sha256(prompt.encode()).hexdigest()[:16]

    # Always log — this is the primary signal for tuning
    print(
        f"[SAFETY_REJECTION] provider={provider} medium={medium} "
        f"local_decision={local_decision} prompt_hash={prompt_hash} "
        f"rejection_code={rejection_code} matched_rules={json.dumps(matched_rules)} "
        f"scores={json.dumps({k: round(v, 1) for k, v in cat_scores.items()})} "
        f"job_id={job_id}"
    )

    # Persist to DB
    try:
        from backend.db import USE_DB, transaction
        if not USE_DB:
            return
        with transaction() as cur:
            cur.execute("""
                INSERT INTO timrx_app.safety_rejections
                    (provider, medium, prompt_hash, local_decision, matched_rules,
                     category_scores, rejection_code, rejection_message, job_id, created_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
            """, (
                provider, medium, prompt_hash, local_decision,
                json.dumps(matched_rules),
                json.dumps({k: round(v, 1) for k, v in cat_scores.items()}),
                rejection_code[:200], rejection_message[:500], job_id,
            ))
    except Exception as e:
        print(f"[SAFETY] Warning: could not persist rejection: {e}")


# ─────────────────────────────────────────────────────────────
# Analytics summary
# ─────────────────────────────────────────────────────────────
def get_safety_analytics(hours: int = 24) -> Dict:
    """
    Aggregate safety data for admin/debug.
    Returns blocks/warns by category, false-negative candidates,
    top matched rules, providers with most rejections, penalty totals,
    and top penalized users.
    """
    result: Dict = {
        "period_hours": hours,
        "blocks_by_category": {},
        "warns_by_category": {},
        "false_negative_candidates": 0,
        "top_rejection_providers": {},
        "top_matched_rules": {},
        "total_strikes": 0,
        # New fields for admin dashboard
        "penalties_applied": 0,
        "penalty_credits_total": 0,
        "top_penalized_users": [],
        "category_breakdown": [],
    }
    try:
        from backend.db import USE_DB, query_all, query_one
        if not USE_DB:
            return result

        # ── Strikes by decision ──
        strikes = query_all("""
            SELECT decision, COUNT(*) AS cnt
            FROM timrx_app.safety_strikes
            WHERE created_at > NOW() - INTERVAL '%s hours'
            GROUP BY decision
        """, (hours,))
        for row in strikes:
            if row["decision"] == "block":
                result["blocks_by_category"]["_total"] = row["cnt"]
            else:
                result["warns_by_category"]["_total"] = row["cnt"]
            result["total_strikes"] += row["cnt"]

        # ── Category breakdown from new JSONB columns ──
        # Gracefully handles rows where categories is still '[]' (pre-migration)
        cat_rows = query_all("""
            SELECT cat.value AS category, decision, COUNT(*) AS cnt
            FROM timrx_app.safety_strikes,
                 jsonb_array_elements_text(categories) AS cat(value)
            WHERE created_at > NOW() - INTERVAL '%s hours'
              AND jsonb_array_length(categories) > 0
            GROUP BY cat.value, decision
            ORDER BY cnt DESC
        """, (hours,))
        cat_map: Dict[str, Dict] = {}
        for row in cat_rows:
            cat = row["category"]
            if cat not in cat_map:
                cat_map[cat] = {"category": cat, "blocks": 0, "warns": 0}
            if row["decision"] == "block":
                cat_map[cat]["blocks"] += row["cnt"]
                result["blocks_by_category"][cat] = result["blocks_by_category"].get(cat, 0) + row["cnt"]
            else:
                cat_map[cat]["warns"] += row["cnt"]
                result["warns_by_category"][cat] = result["warns_by_category"].get(cat, 0) + row["cnt"]
        result["category_breakdown"] = sorted(cat_map.values(), key=lambda x: x["blocks"], reverse=True)

        # ── Top matched rules from new JSONB column ──
        rule_rows = query_all("""
            SELECT rule.value AS rule_id, COUNT(*) AS cnt
            FROM timrx_app.safety_strikes,
                 jsonb_array_elements_text(matched_rules) AS rule(value)
            WHERE created_at > NOW() - INTERVAL '%s hours'
              AND jsonb_array_length(matched_rules) > 0
            GROUP BY rule.value
            ORDER BY cnt DESC
            LIMIT 20
        """, (hours,))
        for row in rule_rows:
            result["top_matched_rules"][row["rule_id"]] = row["cnt"]

        # ── Penalty totals from ledger ──
        penalty_row = query_one("""
            SELECT COUNT(*) AS cnt,
                   COALESCE(ABS(SUM(amount_credits)), 0) AS total
            FROM timrx_billing.ledger_entries
            WHERE entry_type = 'safety_penalty'
              AND created_at >= NOW() - make_interval(hours := %s)
        """, (hours,))
        if penalty_row:
            result["penalties_applied"] = penalty_row["cnt"]
            result["penalty_credits_total"] = int(penalty_row["total"])

        # ── Top penalized users ──
        top_users = query_all("""
            SELECT le.identity_id,
                   i.email,
                   COUNT(*) AS penalty_count,
                   ABS(SUM(le.amount_credits)) AS total_credits
            FROM timrx_billing.ledger_entries le
            LEFT JOIN timrx_billing.identities i ON i.id = le.identity_id
            WHERE le.entry_type = 'safety_penalty'
              AND le.created_at >= NOW() - make_interval(hours := %s)
            GROUP BY le.identity_id, i.email
            ORDER BY total_credits DESC
            LIMIT 10
        """, (hours,))
        for row in top_users:
            email = row.get("email") or ""
            # Mask email: show first 2 chars + *** + domain
            if "@" in email:
                local, domain = email.split("@", 1)
                masked = local[:2] + "***@" + domain
            else:
                masked = email[:3] + "***" if email else "unknown"
            result["top_penalized_users"].append({
                "identity_id": str(row["identity_id"]),
                "email_masked": masked,
                "penalty_count": row["penalty_count"],
                "penalty_credits": int(row["total_credits"]),
            })

        # ── Provider rejections (false-negative candidates) ──
        rejections = query_all("""
            SELECT provider, local_decision, COUNT(*) AS cnt, matched_rules
            FROM timrx_app.safety_rejections
            WHERE created_at > NOW() - INTERVAL '%s hours'
            GROUP BY provider, local_decision, matched_rules
            ORDER BY cnt DESC
            LIMIT 20
        """, (hours,))
        for row in rejections:
            prov = row["provider"]
            result["top_rejection_providers"][prov] = result["top_rejection_providers"].get(prov, 0) + row["cnt"]
            if row["local_decision"] == "allow":
                result["false_negative_candidates"] += row["cnt"]

            # Parse matched_rules to count (fallback for rules not yet in strikes table)
            if not rule_rows:
                try:
                    rules = json.loads(row.get("matched_rules", "[]"))
                    for r in rules:
                        result["top_matched_rules"][r] = result["top_matched_rules"].get(r, 0) + row["cnt"]
                except (json.JSONDecodeError, TypeError):
                    pass

    except Exception as e:
        print(f"[SAFETY] Warning: analytics query failed: {e}")

    return result


# ─────────────────────────────────────────────────────────────
# Main API
# ─────────────────────────────────────────────────────────────
def check_prompt_safety(
    prompt: str,
    medium: str = "image",
    provider: str = "openai",
    user_id: Optional[str] = None,
    dry_run: bool = False,
) -> Dict:
    if not prompt or not prompt.strip():
        return _allow_result()

    text = prompt.strip()
    text_cleaned = _strip_negations(text)

    provider_lower = (provider or "").lower()
    medium_lower   = (medium or "image").lower()
    provider_mult  = _PROVIDER_STRICTNESS.get(provider_lower, 1.0)
    medium_mult    = _MEDIUM_STRICTNESS.get(medium_lower, 1.0)
    total_mult     = provider_mult * medium_mult

    # Score every rule into per-category buckets
    cat_scores: Dict[str, float] = {}
    matched_rules: List[Dict] = []

    for pattern, category, weight, rule_id in _RULES:
        m = pattern.search(text_cleaned)
        if m:
            adjusted = weight * total_mult
            cat_scores[category] = cat_scores.get(category, 0.0) + adjusted
            matched_rules.append({
                "rule": rule_id, "category": category,
                "weight": weight, "adjusted": round(adjusted, 1),
                "matched": m.group()[:80],
            })

    # Safe-context reducer
    safe_count = len(_SAFE_CONTEXT_WORDS.findall(text_cleaned))
    has_harmful_verbs = bool(_HARMFUL_ACTION_VERBS.search(text_cleaned))
    safe_reduced = False

    if safe_count >= _SAFE_SCORE_THRESHOLD and not has_harmful_verbs:
        for c in ("violence", "horror", "weapons"):
            if c in cat_scores:
                cat_scores[c] *= _SAFE_DAMPEN_FACTOR
                safe_reduced = True

    # Non-human subject override:
    # If prompt is about animals/insects/nature/macro and has NO human subjects,
    # zero out violence and sexual scores entirely.
    has_nonhuman = bool(_NONHUMAN_SUBJECTS.search(text_cleaned))
    has_human = bool(_HUMAN_SUBJECTS.search(text_cleaned))
    nonhuman_override = False

    if has_nonhuman and not has_human and not has_harmful_verbs:
        for c in ("violence", "sexual", "horror", "weapons"):
            if c in cat_scores:
                cat_scores[c] = 0.0
                nonhuman_override = True

    # Determine decision per category
    block_cats = set()
    warn_cats  = set()
    for cat, score in cat_scores.items():
        th = _CATEGORY_THRESHOLDS.get(cat, {})
        if score >= th.get("block_at", _DEFAULT_BLOCK_AT):
            block_cats.add(cat)
        elif score >= th.get("warn_at", _DEFAULT_WARN_AT):
            warn_cats.add(cat)

    decision = "block" if block_cats else ("warn" if warn_cats else "allow")
    triggered_cats = sorted(block_cats | warn_cats)

    debug_info = {
        "scores": {k: round(v, 1) for k, v in sorted(cat_scores.items())},
        "matched_rules": [r["rule"] for r in matched_rules],
        "matched_details": matched_rules,
        "safe_context_count": safe_count,
        "has_harmful_verbs": has_harmful_verbs,
        "safe_reduced": safe_reduced,
        "nonhuman_override": nonhuman_override,
        "has_nonhuman_subject": has_nonhuman,
        "has_human_subject": has_human,
        "multiplier": round(total_mult, 2),
        "thresholds_used": {
            cat: _CATEGORY_THRESHOLDS.get(cat, {"warn_at": _DEFAULT_WARN_AT, "block_at": _DEFAULT_BLOCK_AT})
            for cat in cat_scores
        },
    }

    # Structured log
    print(
        f"[SAFETY] provider={provider_lower} medium={medium_lower} "
        f"decision={decision} categories={triggered_cats} "
        f"matched={json.dumps([r['rule'] for r in matched_rules])} "
        f"scores={json.dumps({k: round(v, 1) for k, v in cat_scores.items()})} "
        f"safe_ctx={safe_count} harmful_verbs={has_harmful_verbs} "
        f"reduced={safe_reduced} nonhuman={nonhuman_override} mult={total_mult:.2f}"
    )

    # False positive warning: log when a block/warn looks suspicious
    if decision != "allow" and not has_harmful_verbs and (safe_count >= 4 or nonhuman_override):
        print(
            f"[SAFETY_WARNING_FALSE_POSITIVE] decision={decision} categories={triggered_cats} "
            f"safe_ctx={safe_count} nonhuman={nonhuman_override} harmful_verbs=False "
            f"scores={json.dumps({k: round(v, 1) for k, v in cat_scores.items()})} "
            f"matched={json.dumps([r['rule'] for r in matched_rules])}"
        )

    if decision == "allow":
        r = _allow_result()
        r["debug"] = debug_info
        return r

    cats = sorted(block_cats) if block_cats else sorted(warn_cats)
    primary_cat = cats[0] if cats else "violence"

    strike_count = 0
    penalty = 0
    penalty_notice = None

    # Penalty guardrail: only penalize for high-confidence blocks
    # (explicit violence/sexual/hate/real_person rules, not ambiguous signals)
    matched_rule_ids = {r["rule"][:3] for r in matched_rules}
    is_high_confidence = bool(matched_rule_ids & _HIGH_CONFIDENCE_RULE_PREFIXES)

    # If non-human override was active, this prompt should never have been
    # blocked/warned — do NOT record strikes or apply penalties.
    if nonhuman_override:
        is_high_confidence = False

    if not dry_run and user_id:
        if user_id not in _strike_cache:
            _strike_cache[user_id] = _load_strikes_from_db(user_id)

        # Only record strike if it's NOT a non-human false positive
        if not nonhuman_override:
            strike_count = _record_strike(
                user_id, decision,
                categories=triggered_cats,
                matched_rules=[r["rule"] for r in matched_rules],
            )
        else:
            strike_count = _get_strikes_24h(user_id)

        if decision == "block" and is_high_confidence:
            # Use DB-confirmed count for penalty decisions (reliable across workers)
            strike_count = _get_db_strike_count_24h(user_id)
            penalty = _compute_penalty(strike_count)
            if penalty > 0:
                _apply_credit_penalty(user_id, penalty)
                penalty_notice = f"A {penalty}-credit penalty has been applied. Repeated blocked prompts can lead to increasing penalties."
            elif strike_count >= _PENALTY_FREE_STRIKES:
                penalty_notice = "Repeated blocked prompts may lead to small credit penalties."
        elif strike_count >= _PENALTY_FREE_STRIKES and not nonhuman_override:
            penalty_notice = "Repeated flagged prompts may lead to small credit penalties."

    return {
        "decision": decision,
        "categories": cats,
        "category_label": _CATEGORY_LABELS.get(primary_cat, primary_cat),
        "message": (_block_message if decision == "block" else _warn_message)(cats),
        "rewrite_hint": _REWRITE_HINTS.get(primary_cat, _REWRITE_HINTS["violence"]),
        "strike_count_24h": strike_count,
        "credit_penalty": penalty,
        "penalty_notice": penalty_notice,
        "debug": debug_info,
    }


# ─────────────────────────────────────────────────────────────
# Response helpers
# ─────────────────────────────────────────────────────────────
def _allow_result() -> Dict:
    return {
        "decision": "allow", "categories": [], "category_label": "",
        "message": "", "rewrite_hint": "",
        "strike_count_24h": 0, "credit_penalty": 0, "penalty_notice": None,
        "debug": {},
    }

def _block_message(categories):
    m = {
        "minors": "content involving minors in harmful contexts",
        "violence": "graphic violence or explicit harm",
        "sexual": "explicit sexual content",
        "self_harm": "self-harm or suicide promotion",
        "hate": "hate speech or extremist content",
        "real_person": "real-person likeness in dangerous or deceptive scenes",
        "copyright": "likely copyright-infringing recreation",
    }
    parts = [m[c] for c in categories if c in m] or ["content that may violate provider safety policies"]
    return f"This prompt was blocked because it contains {', '.join(parts)}."

def _warn_message(categories):
    m = {
        "horror": "intense horror elements",
        "weapons": "weapons or armed characters",
        "violence": "borderline violent content",
        "copyright": "franchise-inspired elements that may trigger moderation",
        "sexual": "suggestive content",
        "real_person": "real-person references",
    }
    parts = [m[c] for c in categories if c in m] or ["elements that may trigger provider moderation"]
    return f"This prompt contains {', '.join(parts)}. Some providers may filter this."


# ─────────────────────────────────────────────────────────────
# DB schema — startup sanity check
# Tables are created by migrations; these IF NOT EXISTS guards
# are no-ops under timrx_admin (no CREATE privilege on schemas).
# ─────────────────────────────────────────────────────────────
def ensure_safety_schema():
    try:
        from backend.db import USE_DB, transaction
        if not USE_DB: return
        with transaction() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS timrx_app.safety_strikes (
                    id            BIGSERIAL PRIMARY KEY,
                    identity_id   UUID NOT NULL,
                    decision      TEXT NOT NULL CHECK (decision IN ('block', 'warn')),
                    categories    JSONB NOT NULL DEFAULT '[]'::jsonb,
                    matched_rules JSONB NOT NULL DEFAULT '[]'::jsonb,
                    created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
            """)
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_safety_strikes_identity_time
                ON timrx_app.safety_strikes (identity_id, created_at DESC)
            """)
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_safety_strikes_categories
                ON timrx_app.safety_strikes USING gin (categories)
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS timrx_app.safety_rejections (
                    id                BIGSERIAL PRIMARY KEY,
                    provider          TEXT NOT NULL,
                    medium            TEXT NOT NULL,
                    prompt_hash       TEXT NOT NULL,
                    local_decision    TEXT NOT NULL,
                    matched_rules     JSONB,
                    category_scores   JSONB,
                    rejection_code    TEXT,
                    rejection_message TEXT,
                    job_id            TEXT,
                    created_at        TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
            """)
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_safety_rejections_provider_time
                ON timrx_app.safety_rejections (provider, created_at DESC)
            """)
        print("[SAFETY] safety schema ensured (strikes + rejections)")
    except Exception as e:
        print(f"[SAFETY] Warning: could not ensure safety schema: {e}")
