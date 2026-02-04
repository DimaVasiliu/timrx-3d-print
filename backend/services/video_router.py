"""
Video Provider Router with Fallback.

Routes video generation requests to available providers in priority order.
Falls back to next provider on configuration or quota errors.

Supported providers (ordered by priority):
- google  (Gemini Veo 3.1) — primary
- runway  (Runway Gen-4 / Veo via Runway) — fallback
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

from backend.services.gemini_video_service import (
    GeminiAuthError,
    GeminiConfigError,
    GeminiValidationError,
    check_gemini_configured,
    download_video_bytes,
    extract_video_thumbnail,
    gemini_image_to_video,
    gemini_text_to_video,
    gemini_video_status,
)
from backend.services.runway_service import (
    RunwayAuthError,
    RunwayConfigError,
    RunwayQuotaError,
)
from backend.services.video_providers.runway_provider import RunwayProvider


# ── Errors ────────────────────────────────────────────────────
class QuotaExhaustedError(Exception):
    """Raised when all providers have exhausted their quota."""

    def __init__(self, provider: str, message: str = ""):
        self.provider = provider
        super().__init__(message or f"Quota exhausted for provider {provider}")


class ProviderUnavailableError(Exception):
    """Raised when no video providers are configured / available."""

    pass


# ── Provider base ─────────────────────────────────────────────
class VideoProvider:
    """Base interface every video provider must implement."""

    name: str = "unknown"

    def is_configured(self) -> Tuple[bool, Optional[str]]:
        return False, "Not implemented"

    def start_text_to_video(self, prompt: str, **params) -> Dict[str, Any]:
        raise NotImplementedError

    def start_image_to_video(self, image_data: str, prompt: str, **params) -> Dict[str, Any]:
        raise NotImplementedError

    def check_status(self, operation_name: str) -> Dict[str, Any]:
        raise NotImplementedError

    def download_video(self, video_url: str) -> Tuple[bytes, str]:
        raise NotImplementedError

    def extract_thumbnail(self, video_bytes: bytes, timestamp_sec: float = 1.0) -> Optional[bytes]:
        raise NotImplementedError


# ── Google Veo 3.1 ────────────────────────────────────────────
class GeminiVeoProvider(VideoProvider):
    """Google Gemini Veo 3.1 video generation provider."""

    name = "google"

    def is_configured(self) -> Tuple[bool, Optional[str]]:
        return check_gemini_configured()

    def start_text_to_video(self, prompt: str, **params) -> Dict[str, Any]:
        try:
            return gemini_text_to_video(
                prompt=prompt,
                aspect_ratio=params.get("aspect_ratio", "16:9"),
                resolution=params.get("resolution", "720p"),
                duration_seconds=params.get("duration_seconds", 6),
                negative_prompt=params.get("negative_prompt"),
                seed=params.get("seed"),
            )
        except RuntimeError as e:
            if _is_quota_error(str(e)):
                raise QuotaExhaustedError(self.name, str(e))
            raise

    def start_image_to_video(self, image_data: str, prompt: str, **params) -> Dict[str, Any]:
        try:
            return gemini_image_to_video(
                image_data=image_data,
                motion_prompt=prompt,
                aspect_ratio=params.get("aspect_ratio", "16:9"),
                resolution=params.get("resolution", "720p"),
                duration_seconds=params.get("duration_seconds", 6),
                negative_prompt=params.get("negative_prompt"),
                seed=params.get("seed"),
            )
        except RuntimeError as e:
            if _is_quota_error(str(e)):
                raise QuotaExhaustedError(self.name, str(e))
            raise

    def check_status(self, operation_name: str) -> Dict[str, Any]:
        return gemini_video_status(operation_name)

    def download_video(self, video_url: str) -> Tuple[bytes, str]:
        return download_video_bytes(video_url)

    def extract_thumbnail(self, video_bytes: bytes, timestamp_sec: float = 1.0) -> Optional[bytes]:
        return extract_video_thumbnail(video_bytes, timestamp_sec)


# ── Helpers ───────────────────────────────────────────────────
def _is_quota_error(msg: str) -> bool:
    """Detect quota / billing errors from error messages."""
    lower = msg.lower()
    return any(tok in lower for tok in ("quota", "billing", "resource_exhausted", "rate_limit", "429"))


# ── Provider registry (ordered by priority) ──────────────────
_PROVIDERS: List[VideoProvider] = [
    GeminiVeoProvider(),
    RunwayProvider(),
]


# ── Router ────────────────────────────────────────────────────
class VideoRouter:
    """
    Route video generation to available providers with automatic fallback.

    Tries providers in priority order.  Falls back on:
      - Configuration errors (provider not set up)
      - Quota exhaustion (daily limits)
      - Authentication failures

    Does NOT fall back on:
      - Validation errors (caller's fault)
    """

    def __init__(self, providers: List[VideoProvider] | None = None):
        self.providers = providers or _PROVIDERS

    # ── queries ───────────────────────────────────────────────
    def get_available_providers(self) -> List[VideoProvider]:
        return [p for p in self.providers if p.is_configured()[0]]

    def get_provider(self, name: str) -> Optional[VideoProvider]:
        for p in self.providers:
            if p.name == name:
                return p
        return None

    # ── routing ───────────────────────────────────────────────
    def route_text_to_video(self, prompt: str, **params) -> Tuple[Dict[str, Any], str]:
        """
        Route text-to-video to the best available provider.

        Returns:
            (api_response, provider_name)

        Raises:
            ProviderUnavailableError – no providers configured
            QuotaExhaustedError      – all providers quota-exhausted
            GeminiValidationError    – invalid parameters (not retryable)
        """
        return self._route("text2video", prompt=prompt, **params)

    def route_image_to_video(self, image_data: str, prompt: str, **params) -> Tuple[Dict[str, Any], str]:
        """Route image-to-video to the best available provider."""
        return self._route("image2video", image_data=image_data, prompt=prompt, **params)

    # ── internals ─────────────────────────────────────────────
    def _route(self, task: str, **kwargs) -> Tuple[Dict[str, Any], str]:
        last_error: Exception | None = None

        for provider in self.providers:
            configured, config_err = provider.is_configured()
            if not configured:
                print(f"[VideoRouter] Skipping {provider.name}: {config_err}")
                continue

            try:
                if task == "image2video":
                    result = provider.start_image_to_video(
                        image_data=kwargs["image_data"],
                        prompt=kwargs.get("prompt", ""),
                        **{k: v for k, v in kwargs.items() if k not in ("image_data", "prompt")},
                    )
                else:
                    result = provider.start_text_to_video(
                        prompt=kwargs["prompt"],
                        **{k: v for k, v in kwargs.items() if k != "prompt"},
                    )
                print(f"[VideoRouter] {task} routed to {provider.name}")
                return result, provider.name

            except QuotaExhaustedError as e:
                last_error = e
                print(f"[VideoRouter] {provider.name} quota exhausted, trying next…")
            except (GeminiAuthError, GeminiConfigError, RunwayAuthError, RunwayConfigError) as e:
                last_error = e
                print(f"[VideoRouter] {provider.name} auth/config error: {e}, trying next…")
            except RunwayQuotaError as e:
                last_error = QuotaExhaustedError(provider.name, str(e))
                print(f"[VideoRouter] {provider.name} quota exhausted, trying next…")
            # Let GeminiValidationError and other RuntimeErrors propagate

        if isinstance(last_error, QuotaExhaustedError):
            raise last_error
        raise ProviderUnavailableError("No video providers available")


# Singleton instance used by the rest of the app
video_router = VideoRouter()
