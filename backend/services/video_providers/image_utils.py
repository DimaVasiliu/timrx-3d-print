"""
Compatibility shim over the canonical reference-media pipeline.

The real implementation now lives in `reference_media.py`. This module keeps the
original function names so existing provider call sites keep working, but the
behaviour has changed in three ways that matter:

  * Output is a STABLE, UNSIGNED URL (`/api/video/ref/<token>`), not a presigned
    S3 link. PiAPI's Seedance docs state "Signed / expiring URLs may fail", and a
    Quality-tier job can be queued longer than the old 1-hour presign TTL.

  * Failures RAISE `ReferenceMediaError` instead of silently returning the raw
    base64 data URI. The old fallback guaranteed an upstream rejection *after*
    credits were reserved; callers now fail fast and refund nothing because
    nothing was charged.

  * Every image is validated, HEIC/MPO-normalized, transcoded into a format the
    target provider actually accepts, and downscaled to a sane upload size.

Prefer importing from `reference_media` directly in new code.
"""

from __future__ import annotations

from backend.services.video_providers.reference_media import (  # noqa: F401
    ReferenceMediaError,
    prepare_av_url,
    prepare_image_inline,
    prepare_image_url,
    prepare_image_urls,
)


def ensure_public_image_url(image_data: str, *, provider_name: str = "provider") -> str:
    """
    Make an image reference reachable by an external provider.

    Raises ReferenceMediaError if the image cannot be prepared — callers must let
    that propagate so the request fails before any credit reservation.
    """
    if not image_data:
        return image_data
    return prepare_image_url(image_data, provider=provider_name)


def ensure_public_media_url(
    media_data: str,
    *,
    provider_name: str = "provider",
    kind: str = "image",
) -> str:
    """
    Make any reference medium (image / video / audio) reachable by a provider.

    Images run the full validate-normalize-transcode-downscale pipeline; video and
    audio are stored byte-for-byte (re-encoding them would be lossy and slow) but
    still get a stable public URL.
    """
    if not media_data:
        return media_data
    if kind == "image":
        return prepare_image_url(media_data, provider=provider_name)
    return prepare_av_url(media_data, kind=kind, provider=provider_name)
