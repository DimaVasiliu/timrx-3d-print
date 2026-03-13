"""
Vertex AI Veo Video Provider.

Wraps the vertex_video_service to provide a consistent interface
for the VideoRouter.

Supported options:
- durations:    4, 6, 8 seconds
- aspect ratios: 16:9, 9:16  (NO 1:1 for video)
- resolutions:  720p, 1080p, 4k
- CONSTRAINT:   1080p / 4k require duration = 8
"""

from __future__ import annotations

from typing import Any, Dict, Optional, Tuple

from backend.services.vertex_video_service import (
    VertexAuthError,
    VertexConfigError,
    VertexQuotaError,
    VertexValidationError,
    check_vertex_configured,
    download_video_bytes,
    vertex_image_to_video,
    vertex_image_transition,
    vertex_text_to_video,
    vertex_video_status,
)
from backend.services.gemini_video_service import extract_video_thumbnail
from backend.services.video_errors import is_quota_error as _is_quota_error


# ── Vertex constraints ──────────────────────────────────────────
SUPPORTED_DURATIONS = frozenset({4, 6, 8})
SUPPORTED_ASPECTS = frozenset({"16:9", "9:16"})
SUPPORTED_RESOLUTIONS = frozenset({"720p", "1080p", "4k"})
HIGH_RES_REQUIRES_8S = frozenset({"1080p", "4k"})

DEFAULT_DURATION = 6
DEFAULT_ASPECT = "16:9"
DEFAULT_RESOLUTION = "720p"

# Duration mapping: snap UI values to nearest valid Vertex duration.
_DURATION_SNAP = {
    4: 4, 5: 4, 6: 6, 7: 6, 8: 8, 10: 8,
}


def normalize_vertex_params(
    duration_seconds: int | str = DEFAULT_DURATION,
    aspect_ratio: str = DEFAULT_ASPECT,
    resolution: str = DEFAULT_RESOLUTION,
) -> Dict[str, Any]:
    """
    Normalize and validate Vertex-specific parameters.

    Returns a clean dict with:
      duration_seconds (int), aspect_ratio (str), resolution (str)

    Falls back to safe defaults for invalid values.
    Enforces the 1080p/4k → duration=8 constraint.
    """
    # Duration
    try:
        dur = int(str(duration_seconds).replace("s", "").replace("sec", "").strip())
    except (ValueError, TypeError):
        dur = DEFAULT_DURATION
    dur = _DURATION_SNAP.get(dur, DEFAULT_DURATION)

    # Resolution
    res = (resolution or DEFAULT_RESOLUTION).strip().lower()
    if res == "4K":
        res = "4k"
    if res not in SUPPORTED_RESOLUTIONS:
        res = DEFAULT_RESOLUTION

    # Enforce: high-res requires 8s
    if res in HIGH_RES_REQUIRES_8S and dur != 8:
        dur = 8

    # Aspect ratio
    ar = (aspect_ratio or DEFAULT_ASPECT).strip()
    if ar not in SUPPORTED_ASPECTS:
        ar = DEFAULT_ASPECT

    return {
        "duration_seconds": dur,
        "aspect_ratio": ar,
        "resolution": res,
    }


class VertexVeoProvider:
    """
    Vertex AI Veo 3.1 video generation provider.

    Uses service account authentication and Vertex AI REST API.
    Production-grade provider with quota-based fallback to Seedance.
    """

    name = "vertex"

    def is_configured(self) -> Tuple[bool, Optional[str]]:
        """Check if Vertex AI is configured."""
        return check_vertex_configured()

    def start_text_to_video(self, prompt: str, **params) -> Dict[str, Any]:
        """Start text-to-video generation."""
        clean = normalize_vertex_params(
            duration_seconds=params.get("duration_seconds", DEFAULT_DURATION),
            aspect_ratio=params.get("aspect_ratio", DEFAULT_ASPECT),
            resolution=params.get("resolution", DEFAULT_RESOLUTION),
        )
        try:
            return vertex_text_to_video(
                prompt=prompt,
                aspect_ratio=clean["aspect_ratio"],
                resolution=clean["resolution"],
                duration_seconds=clean["duration_seconds"],
                negative_prompt=params.get("negative_prompt"),
                seed=params.get("seed"),
            )
        except VertexQuotaError as e:
            from backend.services.video_router import QuotaExhaustedError
            raise QuotaExhaustedError(self.name, str(e))
        except RuntimeError as e:
            if _is_quota_error(str(e)):
                from backend.services.video_router import QuotaExhaustedError
                raise QuotaExhaustedError(self.name, str(e))
            raise

    def start_image_to_video(self, image_data: str, prompt: str, **params) -> Dict[str, Any]:
        """Start image-to-video generation."""
        clean = normalize_vertex_params(
            duration_seconds=params.get("duration_seconds", DEFAULT_DURATION),
            aspect_ratio=params.get("aspect_ratio", DEFAULT_ASPECT),
            resolution=params.get("resolution", DEFAULT_RESOLUTION),
        )
        try:
            return vertex_image_to_video(
                image_data=image_data,
                motion_prompt=prompt,
                aspect_ratio=clean["aspect_ratio"],
                resolution=clean["resolution"],
                duration_seconds=clean["duration_seconds"],
                negative_prompt=params.get("negative_prompt"),
                seed=params.get("seed"),
            )
        except VertexQuotaError as e:
            from backend.services.video_router import QuotaExhaustedError
            raise QuotaExhaustedError(self.name, str(e))
        except RuntimeError as e:
            if _is_quota_error(str(e)):
                from backend.services.video_router import QuotaExhaustedError
                raise QuotaExhaustedError(self.name, str(e))
            raise

    def start_image_transition(self, start_image: str, end_image: str, prompt: str, **params) -> Dict[str, Any]:
        """Start image-to-image transition video generation (two images, first+last frame)."""
        clean = normalize_vertex_params(
            duration_seconds=params.get("duration_seconds", DEFAULT_DURATION),
            aspect_ratio=params.get("aspect_ratio", DEFAULT_ASPECT),
            resolution=params.get("resolution", DEFAULT_RESOLUTION),
        )
        try:
            return vertex_image_transition(
                start_image=start_image,
                end_image=end_image,
                prompt=prompt,
                aspect_ratio=clean["aspect_ratio"],
                resolution=clean["resolution"],
                duration_seconds=clean["duration_seconds"],
                negative_prompt=params.get("negative_prompt"),
                seed=params.get("seed"),
            )
        except VertexQuotaError as e:
            from backend.services.video_router import QuotaExhaustedError
            raise QuotaExhaustedError(self.name, str(e))
        except RuntimeError as e:
            if _is_quota_error(str(e)):
                from backend.services.video_router import QuotaExhaustedError
                raise QuotaExhaustedError(self.name, str(e))
            raise

    def check_status(self, operation_name: str) -> Dict[str, Any]:
        """Check status of a video generation operation."""
        return vertex_video_status(operation_name)

    def download_video(self, video_url: str) -> Tuple[bytes, str]:
        """Download video bytes from the generated URL."""
        return download_video_bytes(video_url)

    def extract_thumbnail(self, video_bytes: bytes, timestamp_sec: float = 1.0) -> Optional[bytes]:
        """Extract thumbnail from video (uses shared ffmpeg implementation)."""
        return extract_video_thumbnail(video_bytes, timestamp_sec)



# _is_quota_error imported from backend.services.video_errors
