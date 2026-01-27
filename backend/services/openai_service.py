"""OpenAI image generation service migrated from app.py."""

from __future__ import annotations

import requests

from backend.config import config


def openai_image_generate(
    prompt: str,
    size: str = "1024x1024",
    model: str = "gpt-image-1",
    n: int = 1,
    response_format: str = "url",
) -> dict:
    if not config.OPENAI_API_KEY:
        raise RuntimeError("OPENAI_API_KEY not set")
    url = "https://api.openai.com/v1/images/generations"
    headers = {
        "Authorization": f"Bearer {config.OPENAI_API_KEY}",
        "Content-Type": "application/json",
    }
    payload = {
        "model": model,
        "prompt": prompt,
        "size": size,
        "n": max(1, min(4, int(n or 1))),
    }
    if model != "gpt-image-1":
        payload["response_format"] = response_format
    r = requests.post(url, headers=headers, json=payload, timeout=60)
    if not r.ok:
        raise RuntimeError(f"OpenAI image -> {r.status_code}: {r.text[:500]}")
    try:
        return r.json()
    except Exception as exc:
        raise RuntimeError(f"OpenAI image returned non-JSON: {r.text[:200]}") from exc
