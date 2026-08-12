"""
Seedance Video Provider (via PiAPI) — POLL-FIRST provider.

Wraps seedance_service to provide a consistent interface
for the VideoRouter to use alongside VertexVeoProvider.

Completion model: polling via durable job worker.
PiAPI ignores/strips webhook_config for Seedance models (confirmed March 2025).
Webhook config is still sent best-effort but all completion, failure, timeout,
and credit logic works correctly without webhook delivery.

Supported options:
- durations:     5, 10, 15 seconds (PiAPI accepts 4–15; UI exposes 5/10/15 for now)
- aspect ratios: 21:9, 16:9, 4:3, 1:1, 3:4, 9:16, plus `auto` in first_last_frames
                 (2.0 only — 2.5 has no `auto`)
- resolutions:   mini    → 480p, 720p   (defaults to 720p)
                 fast    → 480p, 720p
                 quality → 480p, 720p, 1080p
                 v25     → 480p, 720p   (defaults to 720p)
- tiers:         mini    (PiAPI task_type `seedance-2-mini`)    — cheapest, ~2× faster
                 fast    (PiAPI task_type `seedance-2-fast`)
                 quality (PiAPI task_type `seedance-2`)  — was "preview" pre-GA
                 v25     (PiAPI task_type `seedance-2.5`) — newer model, premium rate,
                         12 references (9 img / 3 vid / 3 audio), no LR variant
- modes:         text_to_video      (no image_urls)
                 first_last_frames  (1 image = animate; 2 images = native transition)
                 omni_reference     (multimodal — 9 combined refs on 2.0, 12 on 2.5)
- audio:         soundtrack generation is on by default; `audio=False` suppresses it
- moderation:    `less_restriction=True` switches to the `-less-restriction` twin of
                 the chosen tier (permissive review, +10% upstream cost, rejection is
                 final). Gated by config — see routes/video.py.

Legacy preview aliases (`seedance-2-preview` / `seedance-2-fast-preview` /
`-vip` variants) are still accepted by the underlying service and silently
upgraded to GA, so in-flight jobs polling here keep working.
"""

from __future__ import annotations

from typing import Any, Dict, Optional, Tuple

from backend.services.seedance_service import (
    SeedanceAuthError,
    SeedanceConfigError,
    SeedanceQuotaError,
    apply_less_restriction,
    is_v25,
    max_duration_for,
    reference_limits_for,
    check_seedance_configured,
    check_seedance_status,
    create_seedance_task,
    download_seedance_video,
)
from backend.services.gemini_video_service import extract_video_thumbnail
from backend.services.video_errors import is_quota_error as _is_quota_error


def _ensure_public_image_url(image_data: str) -> str:
    """Delegate to shared utility. Kept as local alias for backward compat."""
    from backend.services.video_providers.image_utils import ensure_public_image_url
    return ensure_public_image_url(image_data, provider_name="seedance")


def _ensure_public_media_url(media_data: str, kind: str) -> str:
    """Make any reference media (image/video/audio) publicly downloadable for PiAPI."""
    from backend.services.video_providers.image_utils import ensure_public_media_url
    return ensure_public_media_url(media_data, provider_name="seedance", kind=kind)


# omni_reference reference ceilings.
# Seedance 2.0: PiAPI's documented constraint is "1–9 combined references"; the
# earlier 12 we used came from marketing copy and produced 400s at 10+.
# Seedance 2.5: 12 combined, but budgeted per kind (9 images / 3 videos / 3 audio).
# Use reference_limits_for(task_type) rather than this constant when the model is known.
OMNI_MAX_TOTAL_REFS = 9           # images + videos + audios combined (Seedance 2.0)
OMNI_MAX_AUDIO_SECONDS = 15       # total audio duration ceiling
OMNI_MAX_INPUT_VIDEO_SECONDS = 15.4  # total reference-video duration ceiling


# ── Seedance constraints (GA) ───────────────────────────────────
# Backend is permissive: GA PiAPI accepts 4–15s. We don't reject 4/6/7/8/9/11/12/13/14
# even though the UI currently only exposes 5/10/15 — keeps the door open for future UX.
# The upper bound is per-model (see max_duration_for) so raising the 2.5 ceiling via
# SEEDANCE_25_MAX_DURATION doesn't need a second edit here.
MIN_SUPPORTED_DURATION = 4
SUPPORTED_DURATIONS = frozenset({4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15})
SUPPORTED_ASPECTS = frozenset({"21:9", "16:9", "4:3", "1:1", "3:4", "9:16", "auto"})

# Tier ↔ PiAPI GA task_type.
TIER_TO_TASK_TYPE = {
    "mini":    "seedance-2-mini",
    "fast":    "seedance-2-fast",
    "quality": "seedance-2",
    # Seedance 2.5 is a distinct model, not a 2.0 speed tier, but it slots into the
    # same tier→task_type indirection so pricing/quota/action codes stay uniform.
    "v25":     "seedance-2.5",
    # Legacy alias (user-facing copy was "Preview" pre-GA; the underlying PiAPI behaviour
    # was the same quality model). Keep it so any caller still passing "preview" works.
    "preview": "seedance-2",
}

# Maps any `seedance_variant` string we may receive (GA or legacy) to (task_type, tier).
# Both GA and legacy preview names are accepted — legacy maps to the same canonical tier.
# `-less-restriction` task types map to the same tier; the LR flag is carried separately
# so pricing/quota/action-code logic stays keyed on the three real tiers.
VARIANT_MAP = {
    # Seedance 2.5
    "seedance-2.5":              ("seedance-2.5",    "v25"),
    "seedance-2-5":              ("seedance-2.5",    "v25"),  # hyphen spelling, tolerated
    "seedance-2.5-less-restriction": ("seedance-2.5", "v25"),
    "seedance-2-5-less-restriction": ("seedance-2.5", "v25"),  # hyphen spelling, tolerated
    # GA — strict moderation
    "seedance-2-mini":           ("seedance-2-mini", "mini"),
    "seedance-2-fast":           ("seedance-2-fast", "fast"),
    "seedance-2":                ("seedance-2",      "quality"),
    # GA — permissive review (`-less-restriction`)
    "seedance-2-mini-less-restriction": ("seedance-2-mini", "mini"),
    "seedance-2-fast-less-restriction": ("seedance-2-fast", "fast"),
    "seedance-2-less-restriction":      ("seedance-2",      "quality"),
    # Legacy preview-era (still works upstream; seedance_service upgrades to GA before sending)
    "seedance-2-fast-preview":     ("seedance-2-fast", "fast"),
    "seedance-2-preview":          ("seedance-2",      "quality"),
    "seedance-2-fast-preview-vip": ("seedance-2-fast", "fast"),
    "seedance-2-preview-vip":      ("seedance-2",      "quality"),
}

# Per-tier allowed resolutions. PiAPI offers 1080p on the Pro tier only —
# both Fast and Mini cap at 720p.
TIER_TO_RESOLUTIONS = {
    "mini":    frozenset({"480p", "720p"}),
    "fast":    frozenset({"480p", "720p"}),
    "quality": frozenset({"480p", "720p", "1080p"}),
    # 2.5 tops out at 720p — it buys quality-per-second, not resolution.
    "v25":     frozenset({"480p", "720p"}),
}

# PiAPI's own per-tier default resolution. Mini is the odd one out at 720p.
TIER_DEFAULT_RESOLUTION = {
    "mini":    "720p",
    "fast":    "480p",
    "quality": "480p",
    "v25":     "720p",
}

DEFAULT_DURATION = 5
DEFAULT_ASPECT = "16:9"
DEFAULT_RESOLUTION = "480p"
DEFAULT_TIER = "fast"
DEFAULT_TASK_TYPE = TIER_TO_TASK_TYPE[DEFAULT_TIER]

# Soundtrack generation is on upstream by default.
DEFAULT_AUDIO = True


def _normalize_tier(raw: str | None) -> str:
    """Map any tier name (incl. legacy `preview`) to the canonical tier."""
    t = (raw or "").strip().lower()
    if t in ("quality", "preview"):
        return "quality"
    if t == "fast":
        return "fast"
    if t == "mini":
        return "mini"
    if t in ("v25", "2.5", "2_5", "seedance-2.5", "seedance-2-5"):
        return "v25"
    return DEFAULT_TIER


def _normalize_resolution(raw: str | None, tier: str) -> str:
    """Snap resolution to one supported by the chosen tier."""
    r = (raw or "").strip().lower()
    allowed = TIER_TO_RESOLUTIONS.get(tier, TIER_TO_RESOLUTIONS["fast"])
    if r in allowed:
        return r
    # If user asked for 1080p on a tier that lacks it (fast/mini), snap down to 720p.
    if r == "1080p" and "720p" in allowed:
        return "720p"
    return TIER_DEFAULT_RESOLUTION.get(tier, DEFAULT_RESOLUTION)


def normalize_seedance_params(
    duration_seconds: int | str = DEFAULT_DURATION,
    aspect_ratio: str = DEFAULT_ASPECT,
    tier: str | None = None,
    seedance_variant: str | None = None,
    resolution: str | None = None,
    less_restriction: bool = False,
    audio: bool | None = None,
) -> Dict[str, Any]:
    """
    Normalize and validate Seedance-specific parameters.

    Accepts raw values from the request and returns a clean dict with:
      duration_seconds (int), aspect_ratio (str), task_type (str), tier (str),
      resolution (str), less_restriction (bool), audio (bool)

    Resolution snap behaviour: 1080p on the `fast` or `mini` tier snaps down to 720p
    (PiAPI offers 1080p on `seedance-2` only). Unknown resolutions fall back to the
    tier's own default — 720p for `mini`, 480p for `fast`/`quality`.

    `less_restriction` selects the `-less-restriction` twin of the resolved task type
    (permissive content review, +10% upstream cost). It can also be implied by passing
    a `-less-restriction` task type in `seedance_variant`.
    """
    # Tier / task_type: prefer explicit tier, then seedance_variant, then default.
    # Resolved first because duration and aspect rules are model-dependent.
    resolved_tier = DEFAULT_TIER
    resolved_task_type = DEFAULT_TASK_TYPE
    resolved_lr = bool(less_restriction)

    variant_key = (seedance_variant or "").strip().lower()
    if variant_key in VARIANT_MAP and variant_key.endswith("-less-restriction"):
        resolved_lr = True

    if tier:
        resolved_tier = _normalize_tier(tier)
        resolved_task_type = TIER_TO_TASK_TYPE[resolved_tier]
    elif variant_key in VARIANT_MAP:
        resolved_task_type, resolved_tier = VARIANT_MAP[variant_key]
    # else: defaults

    # Duration — exposed durations are 5/10/15; backend tolerates the model's full
    # range (4–15 for 2.0, and whatever SEEDANCE_25_MAX_DURATION allows for 2.5).
    try:
        dur = int(str(duration_seconds).replace("s", "").replace("sec", "").strip())
    except (ValueError, TypeError):
        dur = DEFAULT_DURATION
    # Clamp into range rather than resetting to the 5s default — someone asking for
    # 30s should get the 15s maximum, not silently drop to the shortest clip.
    _ceiling = max_duration_for(resolved_task_type)
    if dur > _ceiling:
        print(f"[Seedance] duration {dur}s exceeds {resolved_task_type} max — clamping to {_ceiling}s")
        dur = _ceiling
    elif dur < MIN_SUPPORTED_DURATION:
        dur = DEFAULT_DURATION

    # Aspect ratio. `auto` is a 2.0-only value — 2.5 has no such option, so snap it
    # to the default here rather than letting the service silently rewrite it.
    ar = (aspect_ratio or DEFAULT_ASPECT).strip()
    if ar not in SUPPORTED_ASPECTS:
        ar = DEFAULT_ASPECT
    if ar == "auto" and is_v25(resolved_task_type):
        ar = DEFAULT_ASPECT

    if resolved_lr:
        resolved_task_type = apply_less_restriction(resolved_task_type)

    # Resolution (tier-aware snap)
    resolved_resolution = _normalize_resolution(resolution, resolved_tier)

    return {
        "duration_seconds": dur,
        "aspect_ratio": ar,
        "task_type": resolved_task_type,
        "tier": resolved_tier,
        "resolution": resolved_resolution,
        "less_restriction": resolved_lr,
        "audio": DEFAULT_AUDIO if audio is None else bool(audio),
    }


class SeedanceProvider:
    """
    Seedance 2.0 GA video generation provider via PiAPI.

    Supports:
      • text-to-video                (mode: text_to_video)
      • image-to-video, single image (mode: first_last_frames, image_urls=[ref])
      • image transition, two images (mode: first_last_frames, image_urls=[start, end])

    Durations 4–15s, aspect ratios 21:9/16:9/4:3/1:1/3:4/9:16 (+ `auto` on
    first_last_frames only).
    Resolutions: mini → 480p/720p (default 720p), fast → 480p/720p,
    quality → 480p/720p/1080p.

    omni_reference (multimodal: images + video + audio references, up to 9 combined)
    is exposed through `start_reference_video`.
    """

    name = "seedance"

    def is_configured(self) -> Tuple[bool, Optional[str]]:
        """Check if PiAPI is configured."""
        return check_seedance_configured()

    def start_text_to_video(self, prompt: str, **params) -> Dict[str, Any]:
        """Start text-to-video generation via Seedance (PiAPI mode=text_to_video)."""
        clean = normalize_seedance_params(
            duration_seconds=params.get("duration_seconds", DEFAULT_DURATION),
            aspect_ratio=params.get("aspect_ratio", DEFAULT_ASPECT),
            tier=params.get("tier"),
            seedance_variant=params.get("task_type") or params.get("seedance_variant"),
            resolution=params.get("resolution"),
            less_restriction=bool(params.get("less_restriction")),
            audio=params.get("audio"),
        )
        try:
            return create_seedance_task(
                prompt=prompt,
                duration=clean["duration_seconds"],
                aspect_ratio=clean["aspect_ratio"],
                task_type=clean["task_type"],
                mode="text_to_video",
                resolution=clean["resolution"],
                audio=clean["audio"],
                auto_upload_assets=params.get("auto_upload_assets"),
                asset_retention_hours=params.get("asset_retention_hours"),
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
        """
        Animate a single reference image (PiAPI mode=first_last_frames, image_urls=[ref]).

        Per the PiAPI Seedance 2 docs, single-image animation is done by
        first_last_frames with just one URL; text_to_video mode rejects image_urls.
        """
        clean = normalize_seedance_params(
            duration_seconds=params.get("duration_seconds", DEFAULT_DURATION),
            aspect_ratio=params.get("aspect_ratio", DEFAULT_ASPECT),
            tier=params.get("tier"),
            seedance_variant=params.get("task_type") or params.get("seedance_variant"),
            resolution=params.get("resolution"),
            less_restriction=bool(params.get("less_restriction")),
            audio=params.get("audio"),
        )
        # PiAPI requires image_urls to be publicly accessible URLs.
        # If the client sent a base64 data URI, upload to S3 first.
        public_url = _ensure_public_image_url(image_data) if image_data else None

        # PiAPI converts our @image1 marker to ByteDance's upstream 【@图片N】 form.
        i2v_prompt = prompt
        if public_url and "@image1" not in i2v_prompt:
            i2v_prompt = f"@image1 {prompt}"

        try:
            return create_seedance_task(
                prompt=i2v_prompt,
                duration=clean["duration_seconds"],
                aspect_ratio=clean["aspect_ratio"],
                image_urls=[public_url] if public_url else None,
                task_type=clean["task_type"],
                mode="first_last_frames",
                resolution=clean["resolution"],
                audio=clean["audio"],
                auto_upload_assets=params.get("auto_upload_assets"),
                asset_retention_hours=params.get("asset_retention_hours"),
            )
        except SeedanceQuotaError as e:
            from backend.services.video_router import QuotaExhaustedError
            raise QuotaExhaustedError(self.name, str(e))
        except RuntimeError as e:
            if _is_quota_error(str(e)):
                from backend.services.video_router import QuotaExhaustedError
                raise QuotaExhaustedError(self.name, str(e))
            raise

    def start_image_transition(self, start_image: str, end_image: str, prompt: str, **params) -> Dict[str, Any]:
        """
        Native first-to-last-frame interpolation via Seedance 2 GA.

        PiAPI mode=first_last_frames with image_urls=[start_url, end_url] —
        this is the real keyframe interpolation that replaces the legacy
        "experimental morph" hack we used pre-GA.
        """
        clean = normalize_seedance_params(
            duration_seconds=params.get("duration_seconds", DEFAULT_DURATION),
            aspect_ratio=params.get("aspect_ratio", DEFAULT_ASPECT),
            tier=params.get("tier"),
            seedance_variant=params.get("task_type") or params.get("seedance_variant"),
            resolution=params.get("resolution"),
            less_restriction=bool(params.get("less_restriction")),
            audio=params.get("audio"),
        )
        start_url = _ensure_public_image_url(start_image) if start_image else None
        end_url = _ensure_public_image_url(end_image) if end_image else None

        if not start_url or not end_url:
            raise RuntimeError("image_transition requires two valid images")

        # Prompt must reference both keyframes for multi-image input.
        transition_prompt = prompt
        if "@image1" not in transition_prompt and "@image2" not in transition_prompt:
            transition_prompt = f"@image1 smoothly transitions into @image2. {prompt}"

        print(
            f"[SEEDANCE] first_last_frames: tier={clean['tier']} "
            f"duration={clean['duration_seconds']}s resolution={clean['resolution']} "
            f"prompt_has_refs={'@image' in prompt}"
        )

        try:
            return create_seedance_task(
                prompt=transition_prompt,
                duration=clean["duration_seconds"],
                aspect_ratio=clean["aspect_ratio"],
                image_urls=[start_url, end_url],
                task_type=clean["task_type"],
                mode="first_last_frames",
                resolution=clean["resolution"],
                audio=clean["audio"],
                auto_upload_assets=params.get("auto_upload_assets"),
                asset_retention_hours=params.get("asset_retention_hours"),
            )
        except SeedanceQuotaError as e:
            from backend.services.video_router import QuotaExhaustedError
            raise QuotaExhaustedError(self.name, str(e))
        except RuntimeError as e:
            if _is_quota_error(str(e)):
                from backend.services.video_router import QuotaExhaustedError
                raise QuotaExhaustedError(self.name, str(e))
            raise

    # Back-compat alias — old callers that call `start_experimental_morph` keep working
    # but now route through the proper first_last_frames path.
    start_experimental_morph = start_image_transition

    def start_reference_video(
        self,
        prompt: str,
        image_data_list: Optional[list] = None,
        video_data_list: Optional[list] = None,
        audio_data_list: Optional[list] = None,
        **params,
    ) -> Dict[str, Any]:
        """
        Reference Video generation — PiAPI Seedance 2 GA `omni_reference` mode.

        Accepts up to 9 mixed references (images + videos + audio combined) and
        weaves them into the output. References are addressable in the prompt via
        @image1 / @video1 / @audio1 (1-based per media kind); PiAPI maps these to
        ByteDance's upstream 【@图片N / @视频N / @音频N】 form.

        Each input is normalised to a public URL (base64/data-URI → S3 presign).

        Cost note (caller's responsibility to surface): PiAPI bills
        ``unit_price × output_duration + (unit_price/2) × total_input_video_seconds``,
        so reference *videos* add to the upstream cost. The route layer computes
        and reserves the matching credit surcharge before dispatch.
        """
        clean = normalize_seedance_params(
            duration_seconds=params.get("duration_seconds", DEFAULT_DURATION),
            aspect_ratio=params.get("aspect_ratio", DEFAULT_ASPECT),
            tier=params.get("tier"),
            seedance_variant=params.get("task_type") or params.get("seedance_variant"),
            resolution=params.get("resolution"),
            less_restriction=bool(params.get("less_restriction")),
            audio=params.get("audio"),
        )

        image_data_list = image_data_list or []
        video_data_list = video_data_list or []
        audio_data_list = audio_data_list or []

        # Reference ceilings are model-specific: 2.0 allows 9 of any mix, while 2.5
        # allows 12 total but caps videos and audio at 3 each.
        caps = reference_limits_for(clean["task_type"])
        total_refs = len(image_data_list) + len(video_data_list) + len(audio_data_list)
        if total_refs == 0:
            raise RuntimeError("reference_video requires at least one image, video, or audio reference")
        if total_refs > caps["total"]:
            raise RuntimeError(
                f"reference_video accepts at most {caps['total']} combined references "
                f"(got {total_refs})"
            )
        for kind, items in (("image", image_data_list), ("video", video_data_list), ("audio", audio_data_list)):
            limit = caps[kind + "s"]
            if len(items) > limit:
                raise RuntimeError(
                    f"reference_video accepts at most {limit} {kind} reference(s) on "
                    f"{clean['task_type']} (got {len(items)})"
                )
        # Audio-only is not allowed by PiAPI — at least one image or video must accompany audio.
        if audio_data_list and not (image_data_list or video_data_list):
            raise RuntimeError("reference_video: audio references require at least one image or video reference")

        image_urls = [_ensure_public_media_url(x, "image") for x in image_data_list if x]
        video_urls = [_ensure_public_media_url(x, "video") for x in video_data_list if x]
        audio_urls = [_ensure_public_media_url(x, "audio") for x in audio_data_list if x]

        # If the caller didn't reference any media in the prompt, prepend a sensible
        # default mention chain so the model actually uses the supplied references.
        ref_prompt = prompt or ""
        if "@image" not in ref_prompt and "@video" not in ref_prompt and "@audio" not in ref_prompt:
            mentions = []
            mentions += [f"@image{i+1}" for i in range(len(image_urls))]
            mentions += [f"@video{i+1}" for i in range(len(video_urls))]
            mentions += [f"@audio{i+1}" for i in range(len(audio_urls))]
            if mentions:
                ref_prompt = f"{' '.join(mentions)} {ref_prompt}".strip()

        print(
            f"[SEEDANCE] omni_reference: tier={clean['tier']} duration={clean['duration_seconds']}s "
            f"resolution={clean['resolution']} images={len(image_urls)} videos={len(video_urls)} "
            f"audios={len(audio_urls)}"
        )

        try:
            return create_seedance_task(
                prompt=ref_prompt,
                duration=clean["duration_seconds"],
                aspect_ratio=clean["aspect_ratio"],
                image_urls=image_urls or None,
                video_urls=video_urls or None,
                audio_urls=audio_urls or None,
                task_type=clean["task_type"],
                mode="omni_reference",
                resolution=clean["resolution"],
                audio=clean["audio"],
                auto_upload_assets=params.get("auto_upload_assets"),
                asset_retention_hours=params.get("asset_retention_hours"),
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
