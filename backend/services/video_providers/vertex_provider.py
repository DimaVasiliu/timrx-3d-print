"""
Vertex AI Veo Video Provider.

Wraps the vertex_video_service to provide a consistent interface
for the VideoRouter to use alongside AI Studio and Runway providers.
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
    vertex_text_to_video,
    vertex_video_status,
)
from backend.services.gemini_video_service import extract_video_thumbnail


class VertexVeoProvider:
    """
    Vertex AI Veo video generation provider.

    Uses service account authentication and Vertex AI REST API.
    Intended as the production provider with AI Studio as fallback.
    """

    name = "vertex"

    def is_configured(self) -> Tuple[bool, Optional[str]]:
        """Check if Vertex AI is configured."""
        return check_vertex_configured()

    def start_text_to_video(self, prompt: str, **params) -> Dict[str, Any]:
        """Start text-to-video generation."""
        try:
            return vertex_text_to_video(
                prompt=prompt,
                aspect_ratio=params.get("aspect_ratio", "16:9"),
                resolution=params.get("resolution", "720p"),
                duration_seconds=params.get("duration_seconds", 6),
                negative_prompt=params.get("negative_prompt"),
                seed=params.get("seed"),
            )
        except VertexQuotaError as e:
            # Re-raise as QuotaExhaustedError for router to handle
            from backend.services.video_router import QuotaExhaustedError
            raise QuotaExhaustedError(self.name, str(e))
        except RuntimeError as e:
            if _is_quota_error(str(e)):
                from backend.services.video_router import QuotaExhaustedError
                raise QuotaExhaustedError(self.name, str(e))
            raise

    def start_image_to_video(self, image_data: str, prompt: str, **params) -> Dict[str, Any]:
        """Start image-to-video generation."""
        try:
            return vertex_image_to_video(
                image_data=image_data,
                motion_prompt=prompt,
                aspect_ratio=params.get("aspect_ratio", "16:9"),
                resolution=params.get("resolution", "720p"),
                duration_seconds=params.get("duration_seconds", 6),
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


def _is_quota_error(msg: str) -> bool:
    """Detect quota/billing errors from error messages."""
    lower = msg.lower()
    return any(tok in lower for tok in ("quota", "billing", "resource_exhausted", "rate_limit", "429"))
