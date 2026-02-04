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
    "cinematic": (
        "Cinematic film quality. Shallow depth of field, dramatic lighting, "
        "smooth camera movement, 24fps motion blur, color-graded."
    ),
    "documentary": (
        "Documentary style. Natural lighting, steady handheld camera, "
        "realistic colors, observational perspective."
    ),
    "product": (
        "Professional product showcase. Clean white or dark background, "
        "smooth 360-degree rotation, studio lighting, sharp focus."
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
    "anime": (
        "Anime-style animation. Vibrant colors, expressive motion, "
        "dynamic camera angles, stylized lighting."
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
        "Gentle orbital camera movement around the subject, "
        "slight 3D perspective shift, smooth circular path."
    ),
    "dolly": (
        "Forward dolly camera movement toward the subject, "
        "smooth continuous approach, increasing detail."
    ),
    "tilt_up": (
        "Slow vertical tilt from bottom to top, "
        "revealing the scene gradually, cinematic reveal."
    ),
    "breathing": (
        "Subtle breathing motion, very gentle scale oscillation "
        "creating a living, organic feel. Minimal camera movement."
    ),
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

    # Style prefix
    style_hint = STYLE_PRESETS.get(style_preset, DEFAULT_STYLE_SUFFIX)
    parts.append(style_hint)

    # User's creative prompt (the core)
    parts.append(user_prompt)

    # Duration hint (helps Veo pace the action)
    if duration_seconds <= 4:
        parts.append("Short burst, fast-paced action compressed into a few seconds.")
    elif duration_seconds >= 8:
        parts.append("Extended take, unhurried pacing with time to breathe.")

    return " ".join(parts)


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
