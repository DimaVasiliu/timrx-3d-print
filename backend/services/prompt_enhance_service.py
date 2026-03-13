"""Prompt enhancement service using OpenAI chat completions."""

from __future__ import annotations

import random
import time
import requests
from requests.exceptions import Timeout, ConnectionError as RequestsConnectionError

from backend.config import config

OPENAI_CHAT_URL = "https://api.openai.com/v1/chat/completions"
OPENAI_CHAT_TIMEOUT = (10, 30)  # (connect, read) seconds
MAX_RETRIES = 2
BASE_RETRY_DELAY = 1  # seconds
MAX_INPUT_LENGTH = 2000
MODEL = "gpt-4o-mini"

# ── Variation pools for the video enhance prompt ─────────────────
# The system prompt randomly selects from these to prevent repetitive outputs.
_CAMERA_VARIATIONS = [
    "slow cinematic tracking shot", "gentle orbit around the subject",
    "smooth dolly-in with shallow depth of field", "crane shot rising above the scene",
    "lateral tracking at eye level", "slow push-in toward the focal point",
    "steady wide establishing shot with subtle drift", "low-angle upward tilt",
]
_LIGHTING_VARIATIONS = [
    "dramatic cinematic lighting", "soft golden backlighting",
    "neon reflections and ambient glow", "moody contrast with rim light",
    "warm golden hour warmth", "cool blue twilight atmosphere",
    "high-contrast chiaroscuro", "natural overcast diffused light",
    "volumetric light rays through haze",
]
_STYLE_VARIATIONS = [
    "photorealistic cinematic quality", "filmic with subtle grain and color grading",
    "clean commercial aesthetic", "atmospheric and moody",
    "vivid and saturated", "muted and desaturated documentary feel",
    "dreamy soft-focus aesthetic", "sharp hyper-detailed realism",
]

# System prompts per mode
_SYSTEM_PROMPTS = {
    "model": (
        "You are a prompt enhancement engine for AI 3D model generation. "
        "Rewrite the user's prompt into a stronger prompt for text-to-3D generation. "
        "Preserve the original subject and intent. "
        "Add relevant visual detail: form, silhouette, materials, structure, and style cues. "
        "Favor 3D-generation-friendly wording and printable/modelable forms. "
        "Avoid overlong poetic fluff, camera language, and impossible scene complexity. "
        "Return ONLY the final enhanced prompt text. No explanations, no labels, no quotes."
    ),
    "image": (
        "You are a prompt enhancement engine for AI image generation. "
        "Rewrite the user's prompt into a stronger prompt for image generation. "
        "Make visual composition clearer. Add subject detail, style, lighting, material, and mood where appropriate. "
        "Keep it concise and strong. Avoid bloated prompt spam. "
        "Do not mention aspect ratio, resolution, or technical API arguments. "
        "Return ONLY the final enhanced prompt text. No explanations, no labels, no quotes."
    ),
    "texture": (
        "You are a prompt enhancement engine for AI 3D texture generation. "
        "Rewrite the user's prompt into a stronger prompt for 3D model texturing. "
        "Add material detail: surface properties, weathering, color variation, reflectivity. "
        "If a specific material is mentioned, elaborate on its physical characteristics. "
        "Keep it concise and descriptive. Avoid abstract or poetic language. "
        "Return ONLY the final enhanced prompt text. No explanations, no labels, no quotes."
    ),
}

# Provider-specific video system prompts
_VIDEO_SYSTEM_BASE = (
    "You are a cinematic prompt enhancement engine for AI video generation. "
    "Transform the user's simple prompt into a structured, cinematic video prompt. "
    "Your output MUST be a single flowing paragraph (NOT labeled sections). "
    "Weave together these elements naturally: "
    "(1) The core scene and subject action, "
    "(2) Camera movement — use: {camera}, "
    "(3) Lighting — use: {lighting}, "
    "(4) Visual style — use: {style}. "
    "Keep it to 2-4 sentences. Favor one clear scene over chaotic complexity. "
    "If the user already described motion, refine it rather than replace it. "
    "Vary your wording — never produce the same structure twice. "
    "{provider_rules}"
    "Return ONLY the final enhanced prompt text. No explanations, no labels, no quotes."
)

_PROVIDER_RULES = {
    "vertex": (
        "IMPORTANT provider rules: This prompt goes to Google Veo. "
        "Keep descriptions clean and cinematic. Do NOT include dialogue, audio instructions, "
        "text overlays, or references to real people. Avoid violent explosions — "
        "use 'burst of energy' or 'shockwave' instead. Avoid overly complex multi-scene descriptions. "
        "One clear, visually rich scene works best. "
    ),
    "seedance": (
        "Provider rules: This prompt goes to Seedance which handles complex cinematic motion well. "
        "You can use dynamic, dramatic language. Bold camera instructions and action sequences work. "
        "Stylized anime/fantasy language is acceptable. "
    ),
    "fal_seedance": (
        "Provider rules: This prompt goes to fal Seedance which works best with shorter instructions. "
        "Keep the prompt concise — 2 sentences max. Strong visual description, minimal technical directives. "
        "Do not overload with camera and lighting instructions simultaneously. "
    ),
}


class PromptEnhanceError(Exception):
    """Non-retryable prompt enhancement error."""
    pass


def _build_video_system_prompt(provider: str = "vertex") -> str:
    """Build a randomized, provider-specific video enhancement system prompt."""
    camera = random.choice(_CAMERA_VARIATIONS)
    lighting = random.choice(_LIGHTING_VARIATIONS)
    style = random.choice(_STYLE_VARIATIONS)
    rules = _PROVIDER_RULES.get(provider, _PROVIDER_RULES["vertex"])
    return _VIDEO_SYSTEM_BASE.format(
        camera=camera, lighting=lighting, style=style, provider_rules=rules,
    )


def _get_system_prompt(mode: str, provider: str = "vertex") -> str:
    """Get the system prompt for a given mode, with provider awareness for video."""
    if mode == "video":
        return _build_video_system_prompt(provider)
    prompt = _SYSTEM_PROMPTS.get(mode)
    if not prompt:
        raise PromptEnhanceError(f"Unsupported mode: {mode}")
    return prompt


def _call_openai(system_prompt: str, user_prompt: str) -> str:
    """Call OpenAI chat completions with retries. Returns enhanced text."""
    headers = {
        "Authorization": f"Bearer {config.OPENAI_API_KEY}",
        "Content-Type": "application/json",
    }
    payload = {
        "model": MODEL,
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
        "temperature": 0.7,
        "max_tokens": 400,
    }

    last_error = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = requests.post(
                OPENAI_CHAT_URL,
                headers=headers,
                json=payload,
                timeout=OPENAI_CHAT_TIMEOUT,
            )
            if not r.ok:
                if 400 <= r.status_code < 500:
                    raise PromptEnhanceError(
                        f"OpenAI error {r.status_code}: {r.text[:300]}"
                    )
                raise RuntimeError(f"OpenAI server error {r.status_code}")

            data = r.json()
            enhanced = (
                data.get("choices", [{}])[0]
                .get("message", {})
                .get("content", "")
                .strip()
            )
            if not enhanced:
                raise PromptEnhanceError("OpenAI returned empty response")

            # Strip wrapping quotes if the model added them
            if (enhanced.startswith('"') and enhanced.endswith('"')) or (
                enhanced.startswith("'") and enhanced.endswith("'")
            ):
                enhanced = enhanced[1:-1].strip()

            return enhanced

        except (Timeout, RequestsConnectionError) as e:
            last_error = e
            if attempt < MAX_RETRIES:
                time.sleep(BASE_RETRY_DELAY * attempt)
        except PromptEnhanceError:
            raise
        except RuntimeError as e:
            last_error = e
            if attempt < MAX_RETRIES:
                time.sleep(BASE_RETRY_DELAY * attempt)

    raise RuntimeError(f"Enhancement failed after {MAX_RETRIES} attempts: {last_error}")


def enhance_prompt(prompt: str, mode: str, provider: str = "vertex") -> str:
    """
    Enhance a user prompt using OpenAI chat completions.

    Args:
        prompt: The raw user prompt (already trimmed).
        mode: One of 'model', 'image', 'video', 'texture'.
        provider: Video provider hint ('vertex', 'seedance', 'fal_seedance').

    Returns:
        The enhanced prompt string.

    Raises:
        PromptEnhanceError: For validation or non-retryable API errors.
        RuntimeError: If all retries are exhausted.
    """
    if not config.OPENAI_API_KEY:
        raise PromptEnhanceError("OPENAI_API_KEY not set")

    prompt = prompt.strip()
    if not prompt:
        raise PromptEnhanceError("Prompt is empty")
    if len(prompt) > MAX_INPUT_LENGTH:
        raise PromptEnhanceError(f"Prompt exceeds {MAX_INPUT_LENGTH} character limit")

    system_prompt = _get_system_prompt(mode, provider)
    enhanced = _call_openai(system_prompt, prompt)
    print(f"[PromptEnhance] mode={mode} provider={provider} ok")
    return enhanced
