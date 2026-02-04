"""
Runway Video Provider — wraps the Runway API for the VideoRouter.

Implements the VideoProvider interface defined in video_router.py.

Supported tasks:
  - text_to_video  (models: veo3.1_fast, veo3.1, veo3)
  - image_to_video (models: gen4_turbo, veo3.1, gen3a_turbo, veo3.1_fast, veo3)

Runway API docs: https://docs.dev.runwayml.com/api
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

from backend.services.runway_service import (
    RunwayAuthError,
    RunwayConfigError,
    RunwayError,
    RunwayQuotaError,
    check_runway_configured,
    runway_download,
    runway_get,
    runway_post,
)


# ── Aspect-ratio mapping ─────────────────────────────────────
# Our UI uses friendly ratios; Runway requires pixel ratios.
_TEXT_RATIO_MAP: Dict[str, str] = {
    "16:9":  "1280:720",
    "9:16":  "720:1280",
    "1080:1920": "1080:1920",
    "1920:1080": "1920:1080",
    "1280:720":  "1280:720",
    "720:1280":  "720:1280",
}

_IMAGE_RATIO_MAP: Dict[str, str] = {
    "16:9":  "1280:720",
    "9:16":  "720:1280",
    "4:3":   "1104:832",
    "3:4":   "832:1104",
    "1:1":   "960:960",
    "1280:720":  "1280:720",
    "720:1280":  "720:1280",
    "1104:832":  "1104:832",
    "832:1104":  "832:1104",
    "960:960":   "960:960",
    "1584:672":  "1584:672",
}

# Allowed text-to-video ratios
_TEXT_RATIOS: set = set(_TEXT_RATIO_MAP.values())
# Allowed image-to-video ratios
_IMAGE_RATIOS: set = set(_IMAGE_RATIO_MAP.values())

# Model defaults
DEFAULT_TEXT_MODEL = "veo3.1_fast"
DEFAULT_IMAGE_MODEL = "gen4_turbo"

# Valid durations for text-to-video (seconds)
_TEXT_DURATIONS = {4, 6, 8}
# Image-to-video supports 2-10 s (integer)
_IMAGE_MIN_DURATION = 2
_IMAGE_MAX_DURATION = 10


# ── Helpers ──────────────────────────────────────────────────
def _map_ratio(ratio: str, task: str) -> str:
    """Convert a friendly ratio to Runway pixel ratio."""
    mapping = _IMAGE_RATIO_MAP if task == "image2video" else _TEXT_RATIO_MAP
    mapped = mapping.get(ratio, ratio)

    allowed = _IMAGE_RATIOS if task == "image2video" else _TEXT_RATIOS
    if mapped not in allowed:
        # Default to landscape
        mapped = "1280:720"
    return mapped


def _clamp_duration(seconds: int, task: str) -> int:
    """Clamp duration to provider-allowed values."""
    try:
        seconds = int(seconds)
    except (TypeError, ValueError):
        seconds = 6

    if task == "image2video":
        return max(_IMAGE_MIN_DURATION, min(_IMAGE_MAX_DURATION, seconds))

    # text2video: snap to nearest allowed value
    if seconds <= 4:
        return 4
    if seconds <= 6:
        return 6
    return 8


# ── Provider class ───────────────────────────────────────────
class RunwayProvider:
    """
    Runway video generation provider for the VideoRouter.

    Conforms to the VideoProvider interface (start_text_to_video,
    start_image_to_video, check_status, download_video, extract_thumbnail).
    """

    name = "runway"

    def is_configured(self) -> Tuple[bool, Optional[str]]:
        return check_runway_configured()

    # ── submit ───────────────────────────────────────────────
    def start_text_to_video(self, prompt: str, **params) -> Dict[str, Any]:
        """
        Submit a text-to-video task to Runway.

        Returns:
            {"task_id": "<uuid>"}  — used as upstream identifier for polling.
        """
        ratio = _map_ratio(params.get("aspect_ratio", "16:9"), "text2video")
        duration = _clamp_duration(params.get("duration_seconds", 6), "text2video")
        model = params.get("model") or DEFAULT_TEXT_MODEL

        body: Dict[str, Any] = {
            "model": model,
            "promptText": prompt[:1000],
            "ratio": ratio,
            "duration": duration,
        }

        # Audio defaults to True on Runway; let caller override
        if "audio" in params:
            body["audio"] = bool(params["audio"])

        try:
            resp = runway_post("/v1/text_to_video", body)
        except RunwayQuotaError:
            from backend.services.video_router import QuotaExhaustedError
            raise QuotaExhaustedError(self.name, "Runway rate limit / quota exhausted")
        except RunwayAuthError as e:
            raise RuntimeError(f"runway_auth_failed: {e.message}") from e
        except RunwayConfigError as e:
            raise RuntimeError(f"runway_not_configured: {e.message}") from e

        task_id = resp.get("id")
        if not task_id:
            raise RuntimeError(f"runway_submit_failed: No task id in response: {resp}")

        print(f"[Runway] text_to_video submitted → task_id={task_id}")
        return {"task_id": task_id}

    def start_image_to_video(self, image_data: str, prompt: str, **params) -> Dict[str, Any]:
        """
        Submit an image-to-video task to Runway.

        ``image_data`` must be an HTTPS URL, Runway URI, or data URI.

        Returns:
            {"task_id": "<uuid>"}
        """
        ratio = _map_ratio(params.get("aspect_ratio", "16:9"), "image2video")
        duration = _clamp_duration(params.get("duration_seconds", 6), "image2video")
        model = params.get("model") or DEFAULT_IMAGE_MODEL

        body: Dict[str, Any] = {
            "model": model,
            "promptImage": image_data,
            "ratio": ratio,
            "duration": duration,
        }

        if prompt:
            body["promptText"] = prompt[:1000]

        seed = params.get("seed")
        if seed is not None:
            try:
                body["seed"] = int(seed) % 4294967296  # 0..2^32-1
            except (TypeError, ValueError):
                pass

        try:
            resp = runway_post("/v1/image_to_video", body)
        except RunwayQuotaError:
            from backend.services.video_router import QuotaExhaustedError
            raise QuotaExhaustedError(self.name, "Runway rate limit / quota exhausted")
        except RunwayAuthError as e:
            raise RuntimeError(f"runway_auth_failed: {e.message}") from e
        except RunwayConfigError as e:
            raise RuntimeError(f"runway_not_configured: {e.message}") from e

        task_id = resp.get("id")
        if not task_id:
            raise RuntimeError(f"runway_submit_failed: No task id in response: {resp}")

        print(f"[Runway] image_to_video submitted → task_id={task_id}")
        return {"task_id": task_id}

    # ── poll ─────────────────────────────────────────────────
    def check_status(self, task_id: str) -> Dict[str, Any]:
        """
        Poll Runway task status.

        Returns a normalized dict matching the format expected by
        the async_dispatch polling loop:
            status:    "processing" | "done" | "failed" | "error"
            progress:  int (0-100, estimated)
            video_url: str (on done — first output URL)
            error:     str (on failed)
            message:   str (human-readable)
        """
        try:
            resp = runway_get(f"/v1/tasks/{task_id}")
        except RunwayError as e:
            if e.retryable:
                return {"status": "error", "error": "runway_server_error", "message": e.message}
            return {"status": "failed", "error": "runway_api_error", "message": e.message}

        raw_status = resp.get("status", "UNKNOWN").upper()

        if raw_status == "SUCCEEDED":
            outputs: List[str] = resp.get("output") or []
            video_url = outputs[0] if outputs else ""
            return {
                "status": "done",
                "progress": 100,
                "video_url": video_url,
                "runway_task": resp,
            }

        if raw_status == "FAILED":
            failure = resp.get("failure") or resp.get("error") or "Unknown failure"
            return {
                "status": "failed",
                "error": "runway_task_failed",
                "message": str(failure),
            }

        if raw_status == "RUNNING":
            return {
                "status": "processing",
                "progress": 50,  # Runway doesn't expose granular progress
                "message": "Runway is generating the video…",
            }

        if raw_status in ("PENDING", "THROTTLED"):
            return {
                "status": "processing",
                "progress": 0 if raw_status == "PENDING" else 10,
                "message": f"Task {raw_status.lower()}…",
            }

        # Unknown status — treat as transient
        return {
            "status": "error",
            "error": "runway_unknown_status",
            "message": f"Unknown Runway status: {raw_status}",
        }

    # ── download ─────────────────────────────────────────────
    def download_video(self, video_url: str) -> Tuple[bytes, str]:
        """Download video from a Runway ephemeral output URL."""
        return runway_download(video_url)

    def extract_thumbnail(self, video_bytes: bytes, timestamp_sec: float = 1.0) -> Optional[bytes]:
        """Reuse the ffmpeg-based thumbnail extractor from Gemini service."""
        from backend.services.gemini_video_service import extract_video_thumbnail
        return extract_video_thumbnail(video_bytes, timestamp_sec)
