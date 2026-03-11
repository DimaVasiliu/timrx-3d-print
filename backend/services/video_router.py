"""
Video Provider Router with Fallback.

Routes video generation requests to available providers.

Active providers:
- vertex   (Vertex AI Veo 3.1) — production default
- seedance (PiAPI Seedance 2.0)

Normalization:
- normalize_provider_name() is the single entry point for provider alias resolution.
  Legacy names ("veo", "google", "aistudio", "video") all resolve to "vertex".
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

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


# ── Helpers ───────────────────────────────────────────────────
def _is_quota_error(msg: str) -> bool:
    """Detect quota / billing errors from error messages."""
    lower = msg.lower()
    return any(tok in lower for tok in ("quota", "billing", "resource_exhausted", "rate_limit", "429"))


# ── Provider registry ─────────────────────────────────────────
from backend.services.video_providers.vertex_provider import VertexVeoProvider

_VERTEX_PROVIDER: Optional[VertexVeoProvider] = None
_SEEDANCE_PROVIDER = None


def _get_vertex_provider() -> VertexVeoProvider:
    """Lazy-load Vertex provider."""
    global _VERTEX_PROVIDER
    if _VERTEX_PROVIDER is None:
        _VERTEX_PROVIDER = VertexVeoProvider()
    return _VERTEX_PROVIDER


def _get_seedance_provider():
    """Lazy-load Seedance provider."""
    global _SEEDANCE_PROVIDER
    if _SEEDANCE_PROVIDER is None:
        from backend.services.video_providers.seedance_provider import SeedanceProvider
        _SEEDANCE_PROVIDER = SeedanceProvider()
    return _SEEDANCE_PROVIDER


def _get_ordered_providers() -> List[VideoProvider]:
    """
    Get video providers ordered by priority.

    Returns Vertex as the primary provider.
    """
    return [_get_vertex_provider()]


# Legacy provider aliases that all resolve to "vertex".
_VERTEX_ALIASES = frozenset({"google", "veo", "aistudio", "video"})

# The two canonical provider names accepted by the system.
CANONICAL_PROVIDERS = frozenset({"vertex", "seedance"})


def normalize_provider_name(raw: str | None) -> str:
    """
    Normalize any provider string to a canonical name.

    Returns "vertex" or "seedance".  Legacy aliases ("veo", "google",
    "aistudio", "video") are mapped to "vertex".  Unknown values
    default to "vertex".
    """
    name = (raw or "").strip().lower()
    if name in CANONICAL_PROVIDERS:
        return name
    if name in _VERTEX_ALIASES:
        return "vertex"
    return "vertex"


def resolve_video_provider(provider_name: str):
    """
    Resolve a provider instance by name.

    Accepts any raw provider string (canonical or legacy alias).
    Returns the provider instance, or None if lookup fails.
    """
    name = normalize_provider_name(provider_name)
    if name == "vertex":
        return _get_vertex_provider()
    if name == "seedance":
        return _get_seedance_provider()
    return video_router.get_provider(name)


# ── Router ────────────────────────────────────────────────────
class VideoRouter:
    """
    Route video generation to available providers.

    Uses Vertex AI as the primary provider.
    Falls back on configuration errors or quota exhaustion.
    Does NOT fall back on validation errors (caller's fault).
    """

    def __init__(self, providers: List[VideoProvider] | None = None):
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
            except (VertexAuthError, VertexConfigError) as e:
                last_error = e
                print(f"[VideoRouter] {provider.name} auth/config error: {e}, trying next…")
            except VertexQuotaError as e:
                last_error = QuotaExhaustedError(provider.name, str(e))
                print(f"[VideoRouter] {provider.name} quota exhausted, trying next…")

        if isinstance(last_error, QuotaExhaustedError):
            raise last_error
        raise ProviderUnavailableError("No video providers available")


# Singleton instance used by the rest of the app
video_router = VideoRouter()
