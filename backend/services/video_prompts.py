"""
Video prompt normalization and style presets.

C1: Auto-prepend cinematic instructions based on style_preset.
C2: Motion presets for image animation.

The presets enrich user prompts with camera/lighting/realism
hints that Veo 3.1 responds well to, without overriding the
user's creative intent.
"""

from __future__ import annotations

from typing import Optional


# ── Style presets (text → video) ──────────────────────────────
STYLE_PRESETS = {
    # Default / Auto - no style additions
    "auto": "",
    # Core presets matching UI dropdown
    "cinematic_realism": (
        "Cinematic lighting, natural motion, realistic textures, "
        "shallow depth of field, 35mm lens aesthetic, smooth camera movement, "
        "professional color grading, film grain."
    ),
    "cinematic": (
        "Cinematic film quality. Shallow depth of field, dramatic lighting, "
        "smooth camera movement, 24fps motion blur, color-graded."
    ),
    "product_ad": (
        "Professional product advertisement. Clean studio lighting, "
        "smooth rotation or reveal, sharp focus on product details, "
        "minimalist background, premium commercial quality."
    ),
    "product": (
        "Professional product showcase. Clean white or dark background, "
        "smooth 360-degree rotation, studio lighting, sharp focus."
    ),
    "anime_motion": (
        "Anime-style animation. Vibrant saturated colors, expressive character motion, "
        "dynamic camera angles, stylized cel-shading, dramatic speed lines."
    ),
    "anime": (
        "Anime-style animation. Vibrant colors, expressive motion, "
        "dynamic camera angles, stylized lighting."
    ),
    "documentary": (
        "Documentary style. Natural lighting, steady handheld camera, "
        "realistic colors, observational perspective, authentic feel."
    ),
    "dreamlike_surreal": (
        "Dreamlike surreal atmosphere. Soft ethereal lighting, "
        "fluid morphing transitions, impossible geometry, "
        "vivid saturated colors, slow hypnotic motion."
    ),
    "aerial": (
        "Aerial drone footage. Sweeping top-down or flyover perspective, "
        "wide landscape, smooth gliding motion, natural daylight."
    ),
    "timelapse": (
        "Timelapse style. Accelerated motion, fixed or slowly moving camera, "
        "dramatic lighting transitions, clouds and shadows moving."
    ),
    "slow_motion": (
        "Slow motion capture. Ultra-smooth 120fps feel, dramatic impact moments, "
        "fine details visible, shallow depth of field."
    ),
    "noir": (
        "Film noir aesthetic. High contrast black and white, dramatic shadows, "
        "venetian blind lighting, slow atmospheric camera movement."
    ),
}

# Fallback when no preset selected
DEFAULT_STYLE_SUFFIX = (
    "High quality, smooth camera motion, cinematic lighting, photorealistic."
)


# ── Motion presets (image → video) ────────────────────────────
MOTION_PRESETS = {
    "slow_pan": (
        "Slow smooth horizontal camera pan across the scene, "
        "steady movement, gentle parallax effect."
    ),
    "pan": (
        "Slow lateral camera pan from left to right, "
        "smooth horizontal movement, cinematic tracking."
    ),
    "parallax": (
        "Subtle parallax depth effect, foreground and background moving "
        "at different speeds, creating a 3D sense of depth."
    ),
    "zoom_in": (
        "Slow deliberate zoom into the center of the image, "
        "gradually revealing fine details, smooth motion."
    ),
    "zoom_out": (
        "Slow zoom out revealing the full scene, "
        "starting from a detail and expanding to the wider view."
    ),
    "orbit": (
        "Gentle orbit around subject, "
        "smooth circular camera path, slight 3D perspective shift."
    ),
    "dolly": (
        "Slow dolly-in toward the subject, "
        "smooth forward camera movement, increasing detail."
    ),
    "dolly_in": (
        "Slow dolly-in camera movement, "
        "smooth forward approach toward the subject."
    ),
    "dolly_out": (
        "Slow dolly-out camera movement, "
        "smooth backward reveal of the wider scene."
    ),
    "tilt": (
        "Slow vertical camera tilt, "
        "smooth up or down movement, cinematic reveal."
    ),
    "tilt_up": (
        "Slow vertical tilt from bottom to top, "
        "revealing the scene gradually, cinematic reveal."
    ),
    "tilt_down": (
        "Slow vertical tilt from top to bottom, "
        "descending view, smooth reveal."
    ),
    "crane": (
        "Crane shot rising above the scene, "
        "elevated perspective, dramatic reveal."
    ),
    "tracking": (
        "Tracking shot following the subject, "
        "smooth lateral movement matching subject motion."
    ),
    "static": (
        "Static camera with subtle motion in the scene, "
        "minimal camera movement, focus on subject animation."
    ),
    "breathing": (
        "Subtle breathing motion, very gentle scale oscillation "
        "creating a living, organic feel. Minimal camera movement."
    ),
    "handheld": (
        "Subtle handheld camera feel, "
        "organic micro-movements, documentary realism."
    ),
}

# ── Camera motion chip mappings for Luma ──────────────────────
# These map UI chip selections to prompt additions for Luma
# since Luma uses prompt-based camera control
CAMERA_MOTION_PROMPTS = {
    "pan": "slow lateral camera pan",
    "pan_left": "slow camera pan from right to left",
    "pan_right": "slow camera pan from left to right",
    "parallax": "parallax depth effect with layers moving at different speeds",
    "orbit": "gentle orbit around subject",
    "orbit_left": "gentle counter-clockwise orbit around subject",
    "orbit_right": "gentle clockwise orbit around subject",
    "dolly": "slow dolly-in toward subject",
    "dolly_in": "slow dolly-in toward subject",
    "dolly_out": "slow dolly-out revealing the scene",
    "tilt": "slow vertical camera tilt",
    "tilt_up": "slow upward camera tilt",
    "tilt_down": "slow downward camera tilt",
    "zoom_in": "slow zoom into the subject",
    "zoom_out": "slow zoom out revealing more of the scene",
    "crane": "crane shot rising above the scene",
    "crane_up": "crane shot rising upward",
    "crane_down": "crane shot descending downward",
    "tracking": "tracking shot following the subject",
    "static": "static camera with minimal movement",
    "handheld": "subtle handheld camera movement",
}


def normalize_text_prompt(
    user_prompt: str,
    style_preset: Optional[str] = None,
    duration_seconds: int = 6,
) -> str:
    """
    Normalize a text-to-video prompt with cinematic instructions.

    Prepends style instructions and appends duration/quality hints.
    The user's original prompt remains the creative core.
    """
    user_prompt = user_prompt.strip()
    if not user_prompt:
        return user_prompt

    parts = []

    # Style prefix (skip if "auto" or empty style)
    style_key = (style_preset or "").lower().replace(" ", "_").replace("-", "_")
    if style_key and style_key != "auto":
        style_hint = STYLE_PRESETS.get(style_key, "")
        if style_hint:
            parts.append(style_hint)
    elif not style_key:
        # Only add default if no style was specified at all
        parts.append(DEFAULT_STYLE_SUFFIX)

    # User's creative prompt (the core)
    parts.append(user_prompt)

    # Duration hint (helps Veo/Luma pace the action)
    if duration_seconds <= 4:
        parts.append("Short burst, fast-paced action compressed into a few seconds.")
    elif duration_seconds >= 8:
        parts.append("Extended take, unhurried pacing with time to breathe.")

    return " ".join(p for p in parts if p)


def normalize_motion_prompt(
    user_prompt: str,
    motion_preset: Optional[str] = None,
) -> str:
    """
    Normalize an image-to-video motion prompt.

    If a preset is selected, uses it as the base motion instruction
    and appends any user-provided text. If no preset, returns user
    prompt as-is (with a sensible default if empty).
    """
    user_prompt = (user_prompt or "").strip()

    if motion_preset and motion_preset in MOTION_PRESETS:
        preset_text = MOTION_PRESETS[motion_preset]
        if user_prompt:
            return f"{preset_text} {user_prompt}"
        return preset_text

    if user_prompt:
        return user_prompt

    return "Animate this image with natural, smooth motion and subtle camera movement."


def get_style_presets() -> list:
    """Return style presets for the frontend to display."""
    return [
        {"key": key, "label": key.replace("_", " ").title(), "description": desc}
        for key, desc in STYLE_PRESETS.items()
    ]


def get_motion_presets() -> list:
    """Return motion presets for the frontend to display."""
    return [
        {"key": key, "label": key.replace("_", " ").title(), "description": desc}
        for key, desc in MOTION_PRESETS.items()
    ]


def get_camera_motion_presets() -> list:
    """Return camera motion chip options for the frontend."""
    return [
        {"key": key, "label": key.replace("_", " ").title(), "prompt": prompt}
        for key, prompt in CAMERA_MOTION_PROMPTS.items()
    ]


def get_camera_motion_prompt(motion_key: Optional[str]) -> str:
    """
    Get the prompt addition for a camera motion chip selection.

    Args:
        motion_key: Key like "pan", "orbit", "dolly_in", etc.

    Returns:
        Prompt text to append, or empty string if not found.
    """
    if not motion_key:
        return ""
    key = motion_key.lower().replace(" ", "_").replace("-", "_")
    return CAMERA_MOTION_PROMPTS.get(key, "")
