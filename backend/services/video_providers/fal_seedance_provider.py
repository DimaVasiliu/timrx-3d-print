"""
fal.ai Seedance 1.5 Pro Video Provider.

Wraps fal_seedance_service to provide a consistent interface
for the VideoRouter.

Supported options:
- durations:     5, 10 seconds
- aspect ratios: 16:9, 9:16, 1:1
- resolution:    720p (launch scope)
"""

from __future__ import annotations

from typing import Any, Dict, Optional, Tuple

from backend.services.fal_seedance_service import (
    FalSeedanceAuthError,
    FalSeedanceConfigError,
    FalSeedanceQuotaError,
    check_fal_seedance_configured,
    download_fal_seedance_video,
    submit_fal_seedance_task,
    check_fal_seedance_status,
)
from backend.services.gemini_video_service import extract_video_thumbnail
from backend.services.video_errors import is_quota_error as _is_quota_error


# ── fal Seedance constraints ──────────────────────────────────
SUPPORTED_DURATIONS = frozenset({5, 10})
SUPPORTED_ASPECTS = frozenset({"16:9", "9:16", "1:1"})
SUPPORTED_RESOLUTIONS = frozenset({"720p"})

DEFAULT_DURATION = 5
DEFAULT_ASPECT = "16:9"
DEFAULT_RESOLUTION = "720p"


def _ensure_public_image_url(image_data: str) -> str:
    """
    Ensure image_data is a public URL suitable for fal.ai image_url.

    If image_data is a base64 data URI, upload it to S3 first and return
    the public URL. If it's already an http(s) URL, return as-is.

    fal.ai requires image_url to be a publicly accessible URL.
    """
    if not image_data:
        return image_data

    # Already a public URL — pass through
    if image_data.startswith("http://") or image_data.startswith("https://"):
        print(f"[FAL_SEEDANCE] image-to-video input type=url")
        return image_data

    # Base64 data URI — upload to S3 first
    if image_data.startswith("data:"):
        print(f"[FAL_SEEDANCE] image-to-video input type=base64 ({len(image_data) // 1024}KB) -> uploading to S3")
        try:
            from backend.services.s3_service import upload_base64_to_s3
            result = upload_base64_to_s3(
                data_url=image_data,
                prefix="video-input",
                name="fal_seedance_ref",
                user_id="fal_seedance",
            )
            if isinstance(result, dict):
                url = result.get("url", "")
            else:
                url = str(result)
            if url:
                print(f"[FAL_SEEDANCE] image uploaded to S3: {url[:80]}...")
                return url
            else:
                print("[FAL_SEEDANCE] WARNING: S3 upload returned empty URL, falling back to raw data")
                return image_data
        except Exception as e:
            print(f"[FAL_SEEDANCE] ERROR uploading image to S3: {e} — falling back to raw data")
            return image_data

    # Unknown format — pass through and let fal reject if invalid
    print(f"[FAL_SEEDANCE] WARNING: unknown image_data format (len={len(image_data)}), passing as-is")
    return image_data


def normalize_fal_seedance_params(
    duration_seconds: int | str = DEFAULT_DURATION,
    aspect_ratio: str = DEFAULT_ASPECT,
    resolution: str = DEFAULT_RESOLUTION,
) -> Dict[str, Any]:
    """
    Normalize and validate fal Seedance parameters.

    Returns a clean dict with:
      duration_seconds (int), aspect_ratio (str), resolution (str)

    Falls back to safe defaults for invalid values.
    """
    # Duration
    try:
        dur = int(str(duration_seconds).replace("s", "").replace("sec", "").strip())
    except (ValueError, TypeError):
        dur = DEFAULT_DURATION
    if dur not in SUPPORTED_DURATIONS:
        # Snap to nearest supported duration
        dur = min(SUPPORTED_DURATIONS, key=lambda d: abs(d - dur))

    # Resolution (only 720p at launch)
    res = (resolution or DEFAULT_RESOLUTION).strip().lower()
    if res not in SUPPORTED_RESOLUTIONS:
        res = DEFAULT_RESOLUTION

    # Aspect ratio
    ar = (aspect_ratio or DEFAULT_ASPECT).strip()
    if ar not in SUPPORTED_ASPECTS:
        ar = DEFAULT_ASPECT

    return {
        "duration_seconds": dur,
        "aspect_ratio": ar,
        "resolution": res,
    }


class FalSeedanceProvider:
    """
    fal.ai Seedance 1.5 Pro video generation provider.

    Uses FAL_KEY authentication and fal.ai queue API.
    Primary Seedance provider with dispatch-time fallback to PiAPI Seedance.
    """

    name = "fal_seedance"

    def is_configured(self) -> Tuple[bool, Optional[str]]:
        """Check if fal.ai Seedance is configured."""
        return check_fal_seedance_configured()

    def start_text_to_video(self, prompt: str, **params) -> Dict[str, Any]:
        """Start text-to-video generation."""
        clean = normalize_fal_seedance_params(
            duration_seconds=params.get("duration_seconds", DEFAULT_DURATION),
            aspect_ratio=params.get("aspect_ratio", DEFAULT_ASPECT),
            resolution=params.get("resolution", DEFAULT_RESOLUTION),
        )
        try:
            return submit_fal_seedance_task(
                prompt=prompt,
                duration=clean["duration_seconds"],
                aspect_ratio=clean["aspect_ratio"],
                task="text2video",
            )
        except FalSeedanceQuotaError as e:
            from backend.services.video_router import QuotaExhaustedError
            raise QuotaExhaustedError(self.name, str(e))
        except RuntimeError as e:
            if _is_quota_error(str(e)):
                from backend.services.video_router import QuotaExhaustedError
                raise QuotaExhaustedError(self.name, str(e))
            raise

    def start_image_to_video(self, image_data: str, prompt: str, **params) -> Dict[str, Any]:
        """Start image-to-video generation."""
        clean = normalize_fal_seedance_params(
            duration_seconds=params.get("duration_seconds", DEFAULT_DURATION),
            aspect_ratio=params.get("aspect_ratio", DEFAULT_ASPECT),
            resolution=params.get("resolution", DEFAULT_RESOLUTION),
        )
        # fal.ai requires image_url to be a publicly accessible URL
        public_url = _ensure_public_image_url(image_data) if image_data else None
        try:
            return submit_fal_seedance_task(
                prompt=prompt,
                duration=clean["duration_seconds"],
                aspect_ratio=clean["aspect_ratio"],
                image_url=public_url,
                task="image2video",
            )
        except FalSeedanceQuotaError as e:
            from backend.services.video_router import QuotaExhaustedError
            raise QuotaExhaustedError(self.name, str(e))
        except RuntimeError as e:
            if _is_quota_error(str(e)):
                from backend.services.video_router import QuotaExhaustedError
                raise QuotaExhaustedError(self.name, str(e))
            raise

    def check_status(self, request_id: str, model_id: str | None = None) -> Dict[str, Any]:
        """Check status of a fal.ai Seedance task."""
        return check_fal_seedance_status(request_id, model_id=model_id)

    def download_video(self, video_url: str) -> Tuple[bytes, str]:
        """Download video bytes from the fal.ai result URL."""
        return download_fal_seedance_video(video_url)

    def extract_thumbnail(self, video_bytes: bytes, timestamp_sec: float = 1.0) -> Optional[bytes]:
        """Extract thumbnail from video (uses shared ffmpeg implementation)."""
        return extract_video_thumbnail(video_bytes, timestamp_sec)
