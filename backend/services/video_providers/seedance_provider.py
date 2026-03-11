"""
Seedance Video Provider (via PiAPI).

Wraps seedance_service to provide a consistent interface
for the VideoRouter to use alongside VertexVeoProvider.

Supported options:
- durations:     5, 10, 15 seconds
- aspect ratios: 16:9, 9:16, 1:1, 4:3, 3:4
- tiers:         fast  (seedance-2-fast-preview)
                 preview (seedance-2-preview)
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
from backend.services.video_errors import is_quota_error as _is_quota_error


def _ensure_public_image_url(image_data: str) -> str:
    """
    Ensure image_data is a public URL suitable for PiAPI image_urls.

    If image_data is a base64 data URI, upload it to S3 first and return
    the public URL. If it's already an http(s) URL, return as-is.

    Seedance (PiAPI) requires image_urls to be publicly accessible URLs —
    it does NOT accept inline base64 data.
    """
    if not image_data:
        return image_data

    # Already a public URL — pass through
    if image_data.startswith("http://") or image_data.startswith("https://"):
        print(f"[SEEDANCE] image-to-video input type=url")
        return image_data

    # Base64 data URI — upload to S3 first
    if image_data.startswith("data:"):
        print(f"[SEEDANCE] image-to-video input type=base64 ({len(image_data) // 1024}KB) -> uploading to S3")
        try:
            from backend.services.s3_service import upload_base64_to_s3
            result = upload_base64_to_s3(
                data_url=image_data,
                prefix="video-input",
                name="seedance_ref",
                user_id="seedance",
            )
            # upload_base64_to_s3 returns either a string URL or a dict with 'url'
            if isinstance(result, dict):
                url = result.get("url", "")
            else:
                url = str(result)
            if url:
                print(f"[SEEDANCE] image uploaded to S3: {url[:80]}...")
                return url
            else:
                print("[SEEDANCE] WARNING: S3 upload returned empty URL, falling back to raw data")
                return image_data
        except Exception as e:
            print(f"[SEEDANCE] ERROR uploading image to S3: {e} — falling back to raw data")
            return image_data

    # Unknown format — pass through and let PiAPI reject if invalid
    print(f"[SEEDANCE] WARNING: unknown image_data format (len={len(image_data)}), passing as-is")
    return image_data


# ── Seedance constraints ────────────────────────────────────────
SUPPORTED_DURATIONS = frozenset({5, 10, 15})
SUPPORTED_ASPECTS = frozenset({"16:9", "9:16", "1:1", "4:3", "3:4"})

# Maps user-facing tier name → PiAPI task_type string.
TIER_TO_TASK_TYPE = {
    "fast": "seedance-2-fast-preview",
    "preview": "seedance-2-preview",
}

# Maps the full seedance_variant string (from frontend) to (task_type, tier).
VARIANT_MAP = {
    "seedance-2-fast-preview": ("seedance-2-fast-preview", "fast"),
    "seedance-2-preview": ("seedance-2-preview", "preview"),
}

DEFAULT_DURATION = 5
DEFAULT_ASPECT = "16:9"
DEFAULT_TIER = "fast"
DEFAULT_TASK_TYPE = TIER_TO_TASK_TYPE[DEFAULT_TIER]


def normalize_seedance_params(
    duration_seconds: int | str = DEFAULT_DURATION,
    aspect_ratio: str = DEFAULT_ASPECT,
    tier: str | None = None,
    seedance_variant: str | None = None,
) -> Dict[str, Any]:
    """
    Normalize and validate Seedance-specific parameters.

    Accepts raw values from the request and returns a clean dict with:
      duration_seconds (int), aspect_ratio (str), task_type (str), tier (str)

    Falls back to safe defaults for invalid values.
    """
    # Duration
    try:
        dur = int(str(duration_seconds).replace("s", "").replace("sec", "").strip())
    except (ValueError, TypeError):
        dur = DEFAULT_DURATION
    if dur not in SUPPORTED_DURATIONS:
        dur = DEFAULT_DURATION

    # Aspect ratio
    ar = (aspect_ratio or DEFAULT_ASPECT).strip()
    if ar not in SUPPORTED_ASPECTS:
        ar = DEFAULT_ASPECT

    # Tier / task_type: prefer explicit tier, then seedance_variant, then default
    resolved_tier = DEFAULT_TIER
    resolved_task_type = DEFAULT_TASK_TYPE

    if tier and tier in TIER_TO_TASK_TYPE:
        resolved_tier = tier
        resolved_task_type = TIER_TO_TASK_TYPE[tier]
    elif seedance_variant and seedance_variant in VARIANT_MAP:
        resolved_task_type, resolved_tier = VARIANT_MAP[seedance_variant]
    # else: defaults

    return {
        "duration_seconds": dur,
        "aspect_ratio": ar,
        "task_type": resolved_task_type,
        "tier": resolved_tier,
    }


class SeedanceProvider:
    """
    Seedance 2.0 video generation provider via PiAPI.

    Supports text-to-video and image-to-video with durations 5/10/15s,
    aspect ratios 16:9 / 9:16 / 1:1, and two quality tiers (fast / preview).
    """

    name = "seedance"

    def is_configured(self) -> Tuple[bool, Optional[str]]:
        """Check if PiAPI is configured."""
        return check_seedance_configured()

    def start_text_to_video(self, prompt: str, **params) -> Dict[str, Any]:
        """Start text-to-video generation via Seedance."""
        clean = normalize_seedance_params(
            duration_seconds=params.get("duration_seconds", DEFAULT_DURATION),
            aspect_ratio=params.get("aspect_ratio", DEFAULT_ASPECT),
            tier=params.get("tier"),
            seedance_variant=params.get("task_type") or params.get("seedance_variant"),
        )
        try:
            return create_seedance_task(
                prompt=prompt,
                duration=clean["duration_seconds"],
                aspect_ratio=clean["aspect_ratio"],
                task_type=clean["task_type"],
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
        clean = normalize_seedance_params(
            duration_seconds=params.get("duration_seconds", DEFAULT_DURATION),
            aspect_ratio=params.get("aspect_ratio", DEFAULT_ASPECT),
            tier=params.get("tier"),
            seedance_variant=params.get("task_type") or params.get("seedance_variant"),
        )
        # PiAPI requires image_urls to be publicly accessible URLs.
        # If the client sent a base64 data URI, upload to S3 first.
        public_url = _ensure_public_image_url(image_data) if image_data else None
        try:
            return create_seedance_task(
                prompt=prompt,
                duration=clean["duration_seconds"],
                aspect_ratio=clean["aspect_ratio"],
                image_urls=[public_url] if public_url else None,
                task_type=clean["task_type"],
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



# _is_quota_error imported from backend.services.video_errors
