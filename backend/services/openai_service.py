"""OpenAI image generation service migrated from app.py."""

from __future__ import annotations

import time
import requests
from requests.exceptions import Timeout, ConnectionError as RequestsConnectionError

from backend.config import config

# OpenAI image generation can take a while, especially for high-quality images
# gpt-image-1 can take 60-120s for complex prompts; allow generous timeout
OPENAI_TIMEOUT = (15, 180)  # (connect_timeout, read_timeout) - 3 minutes for generation
MAX_RETRIES = 3
BASE_RETRY_DELAY = 2  # seconds (exponential backoff: 2s, 4s, 8s)


class OpenAIServerError(Exception):
    """Raised for 5xx errors from OpenAI (retryable)."""
    def __init__(self, status_code: int, message: str):
        self.status_code = status_code
        super().__init__(message)


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

    last_error = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            print(f"[OpenAI] Attempt {attempt}/{MAX_RETRIES}: generating image (timeout={OPENAI_TIMEOUT[1]}s)")
            r = requests.post(url, headers=headers, json=payload, timeout=OPENAI_TIMEOUT)
            if not r.ok:
                # Don't retry 4xx errors (client errors) - they won't succeed
                if 400 <= r.status_code < 500:
                    raise RuntimeError(f"OpenAI image -> {r.status_code}: {r.text[:500]}")
                # For 5xx errors, raise retryable exception
                raise OpenAIServerError(r.status_code, f"OpenAI server error {r.status_code}: {r.text[:200]}")
            try:
                result = r.json()
                print(f"[OpenAI] Image generated successfully on attempt {attempt}")
                return result
            except Exception as exc:
                raise RuntimeError(f"OpenAI image returned non-JSON: {r.text[:200]}") from exc
        except (Timeout, RequestsConnectionError, OpenAIServerError) as e:
            last_error = e
            if attempt < MAX_RETRIES:
                # Exponential backoff: 2s, 4s, 8s
                delay = BASE_RETRY_DELAY * (2 ** (attempt - 1))
                error_type = type(e).__name__
                if isinstance(e, OpenAIServerError):
                    error_type = f"HTTP {e.status_code}"
                print(f"[OpenAI] Attempt {attempt} failed ({error_type}), retrying in {delay}s...")
                time.sleep(delay)
            else:
                print(f"[OpenAI] All {MAX_RETRIES} attempts failed after exponential backoff")
        except RuntimeError:
            # Re-raise RuntimeError (non-retryable errors like 4xx)
            raise

    # If we get here, all retries failed
    raise RuntimeError(f"OpenAI request failed after {MAX_RETRIES} attempts: {last_error}")
