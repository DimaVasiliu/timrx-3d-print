"""
Canonical reference-media pipeline for video providers.

Every image (and video/audio) a user attaches to a video job — a single frame to
animate, a first/last frame pair, or a bag of omni_reference images — goes through
this module. One place to validate, normalize, transcode, downscale, dedupe and
publish, so a fix lands for every provider at once.

Why this exists (each point is a bug this module fixes):

  1. PiAPI's Seedance docs state plainly: "Use publicly accessible URLs (e.g.
     hosted on a CDN or cloud storage). Signed / expiring URLs may fail." The old
     helper handed PiAPI a *presigned* S3 URL with a 1-hour TTL, while Seedance
     Quality jobs can sit queued for up to an hour. Reference media now resolves
     to a stable, unsigned, non-expiring URL served by `/api/video/ref/<token>`.

  2. The old helper returned the raw base64 data URI whenever S3 failed. That
     string was then sent as an `image_url`, so the provider rejected it *after*
     credits had been reserved and the job row created. Failures now raise
     ReferenceMediaError so the route can refuse before charging anyone.

  3. Reference images were uploaded under the `video-input/*` prefix, which is not
     in IMAGE_PREFIXES, so the HEIC/MPO -> JPEG normalization step never ran. An
     iPhone photo reached the provider as undecodable HEIC bytes labelled
     image/jpeg. Normalization is now unconditional.

  4. Veo accepts image/jpeg and image/png ONLY. WEBP was passed straight through
     and rejected upstream. Formats are now transcoded per provider.

Provider contracts (verified against vendor docs, Aug 2026):

  vertex        image/jpeg, image/png                      inline base64
  seedance      image/jpeg, image/png, image/webp, bmp     public URL
  fal_seedance  image/jpeg, image/png, image/webp          public URL
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import io
from dataclasses import dataclass
from typing import Iterable, List, Optional, Tuple

from backend.config import config


class ReferenceMediaError(ValueError):
    """
    Raised when a user-supplied reference cannot be made provider-ready.

    Always surfaced to the caller as a 400 *before* credits are reserved — never
    swallowed into a silent fallback that fails upstream on the user's dime.
    """

    def __init__(self, message: str, *, field: str = "image_data", index: int | None = None):
        super().__init__(message)
        self.message = message
        self.field = field
        self.index = index


# ── Provider capability matrix ───────────────────────────────────────────────
# MIME types each provider will actually accept. Anything else is transcoded.
PROVIDER_IMAGE_FORMATS = {
    # Veo rejects everything except JPEG and PNG.
    "vertex":       frozenset({"image/jpeg", "image/png"}),
    # PiAPI Seedance: "jpg, jpeg, png, webp, bmp".
    "seedance":     frozenset({"image/jpeg", "image/png", "image/webp"}),
    "fal_seedance": frozenset({"image/jpeg", "image/png", "image/webp"}),
}
_DEFAULT_FORMATS = frozenset({"image/jpeg", "image/png"})

# What we transcode *to* when the source format isn't accepted. JPEG unless the
# image has meaningful alpha, in which case PNG (JPEG would flatten it to black).
_TRANSCODE_ALPHA = "image/png"
_TRANSCODE_OPAQUE = "image/jpeg"

# Longest edge we send upstream. Providers generate at 480p-1080p, so anything
# beyond this is pure upload cost and latency for zero quality gain. A 12MP phone
# photo drops from ~8MB to ~400KB here.
MAX_IMAGE_EDGE = 2048
JPEG_QUALITY = 92

# Hard ceiling on a single decoded reference, after normalization.
MAX_REFERENCE_BYTES = 20 * 1024 * 1024

# Cap on fetching a user-supplied remote URL, so a hostile link can't exhaust memory.
MAX_REMOTE_FETCH_BYTES = 25 * 1024 * 1024
REMOTE_FETCH_TIMEOUT = 20

_S3_REFERENCE_PREFIX = "video-input"


@dataclass(frozen=True)
class PreparedImage:
    """A reference image that is ready for a specific provider."""
    data: bytes
    mime: str
    width: int
    height: int
    content_hash: str

    @property
    def base64(self) -> str:
        """Inline base64 payload (Vertex `bytesBase64Encoded`)."""
        return base64.b64encode(self.data).decode("ascii")


# ── Stable public reference URLs ─────────────────────────────────────────────
# PiAPI may fetch a reference minutes-to-hours after task creation and warns that
# signed/expiring URLs may fail, so these tokens never expire. The HMAC makes the
# token unguessable (it is a capability URL): without the secret you cannot mint
# one, and the S3 key is not enumerable from outside.

_TOKEN_VERSION = "r1"


def _ref_secret() -> bytes:
    # Reuses the CSRF secret derivation so no new env var is required to deploy.
    return hashlib.sha256(f"{config.CSRF_SECRET}|video-ref-v1".encode("utf-8")).digest()


def _b64url(raw: bytes) -> str:
    return base64.urlsafe_b64encode(raw).rstrip(b"=").decode("ascii")


def _b64url_decode(text: str) -> bytes:
    return base64.urlsafe_b64decode(text + "=" * (-len(text) % 4))


def make_reference_token(s3_key: str) -> str:
    """Mint a stable, non-expiring capability token for an S3 key."""
    mac = hmac.new(_ref_secret(), f"{_TOKEN_VERSION}:{s3_key}".encode("utf-8"), hashlib.sha256).digest()
    return f"{_TOKEN_VERSION}.{_b64url(s3_key.encode('utf-8'))}.{_b64url(mac)[:24]}"


def resolve_reference_token(token: str) -> Optional[str]:
    """Verify a token and return its S3 key, or None if it is invalid."""
    try:
        version, key_part, sig_part = (token or "").split(".", 2)
    except ValueError:
        return None
    if version != _TOKEN_VERSION:
        return None
    try:
        s3_key = _b64url_decode(key_part).decode("utf-8")
    except Exception:
        return None
    expected = hmac.new(
        _ref_secret(), f"{_TOKEN_VERSION}:{s3_key}".encode("utf-8"), hashlib.sha256
    ).digest()
    if not hmac.compare_digest(_b64url(expected)[:24], sig_part):
        return None
    # Defensive: only ever serve from the reference prefix, never arbitrary keys.
    if not s3_key.startswith(f"{_S3_REFERENCE_PREFIX}/"):
        return None
    return s3_key


def reference_public_url(s3_key: str) -> str:
    """Stable public URL for a stored reference object."""
    base = (getattr(config, "PUBLIC_BASE_URL", "") or "").rstrip("/")
    if base:
        return f"{base}/api/video/ref/{make_reference_token(s3_key)}"

    # No PUBLIC_BASE_URL (local dev). Fall back to a long-lived presigned URL so
    # development still works. Production must set PUBLIC_BASE_URL — it already
    # does for PiAPI webhooks — because PiAPI warns that signed URLs may fail.
    from backend.services.s3_service import presign_s3_key

    presigned = presign_s3_key(s3_key, expires_in=24 * 3600)
    if not presigned:
        raise ReferenceMediaError(
            "Reference media could not be published to a provider-reachable URL. "
            "Set PUBLIC_BASE_URL.",
            field="server_config",
        )
    print(
        "[REF_MEDIA] WARNING: PUBLIC_BASE_URL is not set — falling back to a presigned "
        "S3 URL. PiAPI documents that signed/expiring URLs may fail; set "
        "PUBLIC_BASE_URL in this environment."
    )
    return presigned


# ── Decode / normalize ───────────────────────────────────────────────────────

def _fetch_remote(url: str) -> bytes:
    import requests

    try:
        with requests.get(url, timeout=REMOTE_FETCH_TIMEOUT, stream=True) as resp:
            resp.raise_for_status()
            declared = resp.headers.get("Content-Length")
            if declared and int(declared) > MAX_REMOTE_FETCH_BYTES:
                raise ReferenceMediaError("Reference image is too large to download.")
            buf = bytearray()
            for chunk in resp.iter_content(64 * 1024):
                buf.extend(chunk)
                if len(buf) > MAX_REMOTE_FETCH_BYTES:
                    raise ReferenceMediaError("Reference image is too large to download.")
            return bytes(buf)
    except ReferenceMediaError:
        raise
    except Exception as exc:
        raise ReferenceMediaError(f"Could not download the reference image: {exc}") from exc


def _decode_input(image_data: str) -> bytes:
    """Turn any accepted input form into raw image bytes."""
    if not image_data or not isinstance(image_data, str):
        raise ReferenceMediaError("No image supplied.")

    if image_data.startswith("data:"):
        from backend.utils.upload_validation import UploadValidationError, parse_data_url
        try:
            _mime, raw = parse_data_url(image_data)
        except UploadValidationError as exc:
            raise ReferenceMediaError(f"Invalid image upload: {exc}") from exc
        return raw

    if image_data.startswith("http://") or image_data.startswith("https://"):
        return _fetch_remote(image_data)

    # Bare base64 (some older clients post this). Validate strictly rather than
    # forwarding an arbitrary string to a provider as if it were an image.
    try:
        return base64.b64decode(image_data, validate=True)
    except Exception as exc:
        raise ReferenceMediaError("Unrecognised image format.") from exc


def _has_alpha(img) -> bool:
    if img.mode in ("RGBA", "LA", "PA"):
        alpha = img.getchannel("A")
        return alpha.getextrema()[0] < 255
    return "transparency" in getattr(img, "info", {})


def prepare_image(
    image_data: str,
    *,
    provider: str = "seedance",
    field: str = "image_data",
    index: int | None = None,
) -> PreparedImage:
    """
    Validate, normalize, transcode and downscale one reference image.

    Guarantees the returned bytes are a format the given provider accepts, are
    within the size ceiling, and actually decode as an image.
    """
    from PIL import Image

    raw = _decode_input(image_data)
    if not raw:
        raise ReferenceMediaError("Image upload was empty.", field=field, index=index)

    allowed = PROVIDER_IMAGE_FORMATS.get(provider, _DEFAULT_FORMATS)

    try:
        with Image.open(io.BytesIO(raw)) as img:
            img.load()
            src_format = (img.format or "").upper()
            width, height = img.size

            if width <= 0 or height <= 0:
                raise ReferenceMediaError("Image has no pixels.", field=field, index=index)

            from backend.utils.upload_validation import IMAGE_MIME_BY_FORMAT
            src_mime = IMAGE_MIME_BY_FORMAT.get(src_format)
            if src_mime is None:
                raise ReferenceMediaError(
                    f"Unsupported image format: {src_format or 'unknown'}. "
                    "Use JPG, PNG or WEBP.",
                    field=field, index=index,
                )

            keeps_alpha = _has_alpha(img)
            needs_resize = max(width, height) > MAX_IMAGE_EDGE
            # HEIC/MPO decode fine in Pillow but no provider accepts them, and
            # IMAGE_MIME_BY_FORMAT maps them onto image/jpeg — so the *source*
            # format, not the mapped MIME, decides whether a re-encode is needed.
            needs_transcode = src_mime not in allowed or src_format in ("HEIC", "HEIF", "MPO")

            if not needs_resize and not needs_transcode:
                digest = hashlib.sha256(raw).hexdigest()
                if len(raw) > MAX_REFERENCE_BYTES:
                    raise ReferenceMediaError("Image is too large.", field=field, index=index)
                return PreparedImage(raw, src_mime, width, height, digest)

            work = img
            if needs_resize:
                ratio = MAX_IMAGE_EDGE / float(max(width, height))
                work = img.resize(
                    (max(1, int(width * ratio)), max(1, int(height * ratio))),
                    Image.LANCZOS,
                )

            target = _TRANSCODE_ALPHA if keeps_alpha else _TRANSCODE_OPAQUE
            if target not in allowed:
                # Provider can't take our preferred target (e.g. a provider with no
                # PNG support and an alpha source) — fall back to anything it allows.
                target = "image/png" if "image/png" in allowed else sorted(allowed)[0]

            buf = io.BytesIO()
            if target == "image/png":
                work.convert("RGBA" if keeps_alpha else "RGB").save(buf, format="PNG", optimize=True)
            elif target == "image/webp":
                work.convert("RGBA" if keeps_alpha else "RGB").save(buf, format="WEBP", quality=JPEG_QUALITY)
            else:
                work.convert("RGB").save(buf, format="JPEG", quality=JPEG_QUALITY, optimize=True)
            out = buf.getvalue()
            out_w, out_h = work.size
    except ReferenceMediaError:
        raise
    except Exception as exc:
        raise ReferenceMediaError(
            "That file could not be read as an image. Use JPG, PNG or WEBP.",
            field=field, index=index,
        ) from exc

    if len(out) > MAX_REFERENCE_BYTES:
        raise ReferenceMediaError("Image is too large.", field=field, index=index)

    return PreparedImage(out, target, out_w, out_h, hashlib.sha256(out).hexdigest())


# ── Publishing ───────────────────────────────────────────────────────────────

def publish_reference(
    prepared: PreparedImage | Tuple[bytes, str],
    *,
    kind: str = "image",
    provider: str = "seedance",
) -> str:
    """
    Store a prepared reference in S3 under a content-addressed key and return a
    stable public URL.

    Content addressing means re-submitting the same image is a no-op upload and
    resolves to the same URL, which also lets providers cache it.
    """
    from backend.services.s3_service import s3_key_exists, upload_bytes_to_s3

    if isinstance(prepared, PreparedImage):
        data, mime, digest = prepared.data, prepared.mime, prepared.content_hash
    else:
        data, mime = prepared
        digest = hashlib.sha256(data).hexdigest()

    ext = {
        "image/jpeg": ".jpg",
        "image/png": ".png",
        "image/webp": ".webp",
        "video/mp4": ".mp4",
        "video/quicktime": ".mov",
        "audio/mpeg": ".mp3",
        "audio/wav": ".wav",
    }.get(mime, "")
    s3_key = f"{_S3_REFERENCE_PREFIX}/{kind}/{digest[:2]}/{digest}{ext}"

    try:
        if not s3_key_exists(s3_key):
            upload_bytes_to_s3(data, mime, prefix=_S3_REFERENCE_PREFIX, key=s3_key)
    except Exception as exc:
        raise ReferenceMediaError(
            f"Could not store the reference {kind} for the provider: {exc}",
            field=f"{kind}_urls",
        ) from exc

    return reference_public_url(s3_key)


def prepare_image_url(
    image_data: str,
    *,
    provider: str = "seedance",
    field: str = "image_data",
    index: int | None = None,
) -> str:
    """Full pipeline for URL-based providers (Seedance, fal): prepare then publish."""
    # An already-public reference URL of ours is idempotent — don't re-download and
    # re-upload an image we just published.
    base = (getattr(config, "PUBLIC_BASE_URL", "") or "").rstrip("/")
    if base and image_data.startswith(f"{base}/api/video/ref/"):
        return image_data

    prepared = prepare_image(image_data, provider=provider, field=field, index=index)
    return publish_reference(prepared, kind="image", provider=provider)


def prepare_image_urls(
    items: Iterable[str],
    *,
    provider: str = "seedance",
    field: str = "image_urls",
) -> List[str]:
    """Prepare and publish a list of image references, preserving order."""
    out: List[str] = []
    for i, item in enumerate(items or []):
        if not item:
            continue
        out.append(prepare_image_url(item, provider=provider, field=field, index=i))
    return out


def prepare_image_inline(
    image_data: str,
    *,
    provider: str = "vertex",
    field: str = "image_data",
    index: int | None = None,
) -> Tuple[str, str]:
    """
    Full pipeline for inline-base64 providers (Vertex Veo).

    Returns (base64_string, mime_type) with the MIME guaranteed to be one the
    provider accepts — Veo takes image/jpeg and image/png only.
    """
    prepared = prepare_image(image_data, provider=provider, field=field, index=index)
    return prepared.base64, prepared.mime


# ── Non-image references (video / audio for omni_reference) ──────────────────
# These are passed through byte-for-byte: PiAPI accepts mp4/mov and mp3/wav, and
# re-encoding them server-side would be expensive and lossy. They still get the
# content-addressed, stable-URL treatment so PiAPI never sees a signed URL.

_AV_MIME_BY_KIND = {
    "video": {"mp4": "video/mp4", "mov": "video/quicktime"},
    "audio": {"mp3": "audio/mpeg", "wav": "audio/wav"},
}


def prepare_av_url(
    media_data: str,
    *,
    kind: str,
    provider: str = "seedance",
    field: str | None = None,
    index: int | None = None,
) -> str:
    """Publish a video/audio reference to a stable public URL."""
    field = field or f"{kind}_urls"

    base = (getattr(config, "PUBLIC_BASE_URL", "") or "").rstrip("/")
    if base and media_data.startswith(f"{base}/api/video/ref/"):
        return media_data

    if media_data.startswith("data:"):
        from backend.utils.upload_validation import UploadValidationError, parse_data_url
        try:
            mime, raw = parse_data_url(media_data)
        except UploadValidationError as exc:
            raise ReferenceMediaError(f"Invalid {kind} upload: {exc}", field=field, index=index) from exc
    elif media_data.startswith("http://") or media_data.startswith("https://"):
        raw = _fetch_remote(media_data)
        mime = "video/mp4" if kind == "video" else "audio/mpeg"
    else:
        raise ReferenceMediaError(f"Unrecognised {kind} reference.", field=field, index=index)

    allowed = set(_AV_MIME_BY_KIND.get(kind, {}).values())
    if allowed and mime not in allowed:
        pretty = "MP4 or MOV" if kind == "video" else "MP3 or WAV"
        raise ReferenceMediaError(
            f"Unsupported {kind} format. Use {pretty}.", field=field, index=index
        )
    if len(raw) > MAX_REMOTE_FETCH_BYTES:
        raise ReferenceMediaError(f"{kind.title()} reference is too large.", field=field, index=index)

    return publish_reference((raw, mime), kind=kind, provider=provider)
