"""
Video Provider Router with Fallback.

Routes video generation requests to available providers in priority order.
Falls back to next provider on configuration or quota errors.

Supported providers for Veo (Google):
- vertex  (Vertex AI Veo) — production default
- google  (Gemini AI Studio Veo) — fallback

Runway is handled separately via dedicated endpoints.

Provider selection:
- VIDEO_PROVIDER=vertex (default in prod): Use Vertex AI first, AI Studio as fallback
- VIDEO_PROVIDER=aistudio: Use AI Studio first, Vertex as fallback
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

from backend.config import config
from backend.services.gemini_video_service import (
    GeminiAuthError,
    GeminiConfigError,
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
from backend.services.luma_service import (
    LumaAuthError,
    LumaConfigError,
    LumaQuotaError,
)
from backend.services.video_providers.runway_provider import RunwayProvider
from backend.services.video_providers.vertex_provider import VertexVeoProvider
from backend.services.video_providers.luma_provider import LumaProvider
from backend.services.vertex_video_service import (
    VertexAuthError,
    VertexConfigError,
    VertexQuotaError,
)


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


# ── Provider registry ─────────────────────────────────────────
# Provider instances (singletons)
_VERTEX_PROVIDER = VertexVeoProvider()
_AISTUDIO_PROVIDER = GeminiVeoProvider()
_RUNWAY_PROVIDER = RunwayProvider()
_LUMA_PROVIDER = LumaProvider()


def _get_ordered_providers() -> List[VideoProvider]:
    """
    Get Veo providers ordered by priority based on VIDEO_PROVIDER setting.

    VIDEO_PROVIDER=vertex (default): Vertex -> AI Studio
    VIDEO_PROVIDER=aistudio:         AI Studio -> Vertex

    Note: Runway is handled separately via dedicated endpoints.
    """
    video_provider = getattr(config, 'VIDEO_PROVIDER', 'vertex').lower()

    if video_provider == "aistudio":
        # AI Studio first, Vertex as fallback
        return [_AISTUDIO_PROVIDER, _VERTEX_PROVIDER]
    else:
        # Vertex first (production default), AI Studio as fallback
        return [_VERTEX_PROVIDER, _AISTUDIO_PROVIDER]


def get_runway_provider() -> RunwayProvider:
    """Get the Runway provider instance for direct access."""
    return _RUNWAY_PROVIDER


def get_luma_provider() -> LumaProvider:
    """Get the Luma provider instance for direct access."""
    return _LUMA_PROVIDER


def resolve_video_provider(provider_name: str):
    """
    Resolve a provider by name. Safe for import from any module.

    This function exists to avoid circular imports — async_dispatch and
    other modules can import this single resolver instead of individual
    provider getter functions.

    Args:
        provider_name: "runway", "luma", "google", "vertex"

    Returns:
        The provider instance, or None if not found.
    """
    name = (provider_name or "").lower()
    if name == "runway":
        return _RUNWAY_PROVIDER
    elif name == "luma":
        return _LUMA_PROVIDER
    elif name == "google":
        return _AISTUDIO_PROVIDER
    elif name == "vertex":
        return _VERTEX_PROVIDER
    else:
        # Try the router's provider lookup as fallback
        return video_router.get_provider(name)


# ── Router ────────────────────────────────────────────────────
class VideoRouter:
    """
    Route Veo video generation to available Google providers with automatic fallback.

    Tries providers in priority order based on VIDEO_PROVIDER setting:
      - VIDEO_PROVIDER=vertex (default): Vertex -> AI Studio
      - VIDEO_PROVIDER=aistudio:         AI Studio -> Vertex

    Note: Runway is handled separately via dedicated endpoints, not through this router.

    Falls back on:
      - Configuration errors (provider not set up)
      - Quota exhaustion (daily limits)
      - Authentication failures

    Does NOT fall back on:
      - Validation errors (caller's fault)
    """

    def __init__(self, providers: List[VideoProvider] | None = None):
        # Use dynamic provider ordering if not explicitly provided
        self.providers = providers if providers is not None else _get_ordered_providers()

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
            except (GeminiAuthError, GeminiConfigError, VertexAuthError, VertexConfigError, RunwayAuthError, RunwayConfigError, LumaAuthError, LumaConfigError) as e:
                last_error = e
                print(f"[VideoRouter] {provider.name} auth/config error: {e}, trying next…")
            except (RunwayQuotaError, VertexQuotaError, LumaQuotaError) as e:
                last_error = QuotaExhaustedError(provider.name, str(e))
                print(f"[VideoRouter] {provider.name} quota exhausted, trying next…")
            # Let GeminiValidationError and other RuntimeErrors propagate

        if isinstance(last_error, QuotaExhaustedError):
            raise last_error
        raise ProviderUnavailableError("No video providers available")


# Singleton instance used by the rest of the app
video_router = VideoRouter()
