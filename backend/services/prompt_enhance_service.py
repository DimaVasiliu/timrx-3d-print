"""Prompt enhancement service using OpenAI chat completions."""

from __future__ import annotations

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
    "video": (
        "You are a prompt enhancement engine for AI video generation. "
        "Rewrite the user's prompt into a stronger prompt for text-to-video generation. "
        "Add subject, movement, environment, lighting, and cinematic clarity. "
        "Keep motion believable. Favor one clear scene over chaotic complexity. "
        "If the user already described motion, refine it rather than overwrite it. "
        "Do not inject unsupported technical claims or overstuff the scene. "
        "Return ONLY the final enhanced prompt text. No explanations, no labels, no quotes."
    ),
}


class PromptEnhanceError(Exception):
    """Non-retryable prompt enhancement error."""
    pass


def enhance_prompt(prompt: str, mode: str) -> str:
    """
    Enhance a user prompt using OpenAI chat completions.

    Args:
        prompt: The raw user prompt (already trimmed).
        mode: One of 'model', 'image', 'video'.

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

    system_prompt = _SYSTEM_PROMPTS.get(mode)
    if not system_prompt:
        raise PromptEnhanceError(f"Unsupported mode: {mode}")

    headers = {
        "Authorization": f"Bearer {config.OPENAI_API_KEY}",
        "Content-Type": "application/json",
    }
    payload = {
        "model": MODEL,
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": prompt},
        ],
        "temperature": 0.4,
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

            print(f"[PromptEnhance] mode={mode} ok (attempt {attempt})")
            return enhanced

        except (Timeout, RequestsConnectionError) as e:
            last_error = e
            if attempt < MAX_RETRIES:
                time.sleep(BASE_RETRY_DELAY * attempt)
            else:
                print(f"[PromptEnhance] All {MAX_RETRIES} attempts failed")
        except PromptEnhanceError:
            raise
        except RuntimeError as e:
            last_error = e
            if attempt < MAX_RETRIES:
                time.sleep(BASE_RETRY_DELAY * attempt)
            else:
                print(f"[PromptEnhance] All {MAX_RETRIES} attempts failed")

    raise RuntimeError(f"Prompt enhancement failed after {MAX_RETRIES} attempts: {last_error}")
