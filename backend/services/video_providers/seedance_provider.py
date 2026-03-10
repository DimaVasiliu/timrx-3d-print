"""
Seedance Video Provider (via PiAPI).

Wraps seedance_service to provide a consistent interface
for the VideoRouter to use alongside Veo providers.
"""

from __future__ import annotations

from typing import Any, Dict, Optional, Tuple

from backend.services.seedance_service import (
    SeedanceAuthError,
    SeedanceConfigError,
    SeedanceQuotaError,
    check_seedance_configured,
    check_seedance_status,
    create_seedance_task,
    download_seedance_video,
)
from backend.services.gemini_video_service import extract_video_thumbnail


class SeedanceProvider:
    """
    Seedance 2.0 video generation provider via PiAPI.

    Supports text-to-video and image-to-video with durations 5/10/15s
    and aspect ratios 16:9, 9:16, 1:1.

    Two task types (tiers):
    - seedance-2-fast-preview  (fast, lower cost)
    - seedance-2-preview       (higher quality, higher cost)
    """

    name = "seedance"

    def is_configured(self) -> Tuple[bool, Optional[str]]:
        """Check if PiAPI is configured."""
        return check_seedance_configured()

    def start_text_to_video(self, prompt: str, **params) -> Dict[str, Any]:
        """Start text-to-video generation via Seedance."""
        try:
            return create_seedance_task(
                prompt=prompt,
                duration=params.get("duration_seconds", 5),
                aspect_ratio=params.get("aspect_ratio", "16:9"),
                task_type=params.get("task_type", "seedance-2-fast-preview"),
            )
        except SeedanceQuotaError as e:
            from backend.services.video_router import QuotaExhaustedError
            raise QuotaExhaustedError(self.name, str(e))
        except RuntimeError as e:
            if _is_quota_error(str(e)):
                from backend.services.video_router import QuotaExhaustedError
                raise QuotaExhaustedError(self.name, str(e))
            raise

    def start_image_to_video(self, image_data: str, prompt: str, **params) -> Dict[str, Any]:
        """Start image-to-video generation via Seedance."""
        try:
            return create_seedance_task(
                prompt=prompt,
                duration=params.get("duration_seconds", 5),
                aspect_ratio=params.get("aspect_ratio", "16:9"),
                image_urls=[image_data] if image_data else None,
                task_type=params.get("task_type", "seedance-2-fast-preview"),
            )
        except SeedanceQuotaError as e:
            from backend.services.video_router import QuotaExhaustedError
            raise QuotaExhaustedError(self.name, str(e))
        except RuntimeError as e:
            if _is_quota_error(str(e)):
                from backend.services.video_router import QuotaExhaustedError
                raise QuotaExhaustedError(self.name, str(e))
            raise

    def check_status(self, task_id: str) -> Dict[str, Any]:
        """Check status of a Seedance task."""
        return check_seedance_status(task_id)

    def download_video(self, video_url: str) -> Tuple[bytes, str]:
        """Download video bytes from the Seedance result URL."""
        return download_seedance_video(video_url)

    def extract_thumbnail(self, video_bytes: bytes, timestamp_sec: float = 1.0) -> Optional[bytes]:
        """Extract thumbnail from video (uses shared ffmpeg implementation)."""
        return extract_video_thumbnail(video_bytes, timestamp_sec)


def _is_quota_error(msg: str) -> bool:
    """Detect quota/rate limit errors from error messages."""
    lower = msg.lower()
    return any(tok in lower for tok in ("quota", "rate_limit", "429", "too_many"))
