"""
Shared image URL utilities for video providers.

Both Seedance (PiAPI) and fal Seedance require image_url inputs to be
publicly downloadable.  Private S3 URLs and base64 data URIs must be
converted to presigned URLs before submission.

This module centralizes that logic so bug fixes apply to all providers.
"""

from __future__ import annotations


def ensure_public_image_url(image_data: str, *, provider_name: str = "provider") -> str:
    """
    Ensure image_data is a publicly accessible URL for an external provider API.

    Handles:
    - Public HTTP(S) URLs → pass through
    - Private S3 URLs (our bucket) → presign for 1h
    - Base64 data URIs → upload to S3, presign
    - Empty/None → return as-is

    Args:
        image_data: Image URL string, data URI, or empty.
        provider_name: Provider label for log messages (e.g., "seedance", "fal_seedance").

    Returns:
        A publicly accessible URL string.
    """
    from backend.services.s3_service import presign_s3_key, upload_base64_to_s3
    from backend.config import config as app_config

    tag = provider_name.upper()

    if not image_data:
        return image_data

    # ── Already a URL — check if it's our private S3 bucket ──
    if image_data.startswith("http://") or image_data.startswith("https://"):
        bucket = getattr(app_config, "AWS_BUCKET_MODELS", "")
        if bucket and bucket in image_data:
            try:
                key = image_data.split(f"{bucket}.s3.", 1)[1]
                key = key.split(".amazonaws.com/", 1)[1]
                presigned = presign_s3_key(key, expires_in=3600)
                if presigned:
                    print(f"[{tag}] presigned private S3 URL for provider access")
                    return presigned
            except (IndexError, Exception) as e:
                print(f"[{tag}] WARNING: failed to presign S3 URL: {e}")
        print(f"[{tag}] image-to-video input type=url (external)")
        return image_data

    # ── Base64 data URI — upload to S3, then presign ──
    if image_data.startswith("data:"):
        print(f"[{tag}] image-to-video input type=base64 ({len(image_data) // 1024}KB) -> uploading to S3")
        try:
            result = upload_base64_to_s3(
                data_url=image_data,
                prefix="video-input",
                name=f"{provider_name}_ref",
                user_id=provider_name,
            )
            s3_key = None
            if isinstance(result, dict):
                s3_key = result.get("key", "")
                url = result.get("url", "")
            else:
                url = str(result)

            # Try presigning from the returned S3 key
            if s3_key:
                presigned = presign_s3_key(s3_key, expires_in=3600)
                if presigned:
                    print(f"[{tag}] image uploaded + presigned for provider access")
                    return presigned

            # Fallback: try to presign from the URL if it's in our bucket
            if url:
                bucket = getattr(app_config, "AWS_BUCKET_MODELS", "")
                if bucket and bucket in url:
                    try:
                        key = url.split(f"{bucket}.s3.", 1)[1]
                        key = key.split(".amazonaws.com/", 1)[1]
                        presigned = presign_s3_key(key, expires_in=3600)
                        if presigned:
                            print(f"[{tag}] image uploaded + presigned (fallback)")
                            return presigned
                    except (IndexError, Exception):
                        pass
                print(f"[{tag}] WARNING: could not presign, returning raw URL: {url[:80]}...")
                return url
            else:
                print(f"[{tag}] WARNING: S3 upload returned empty URL, falling back to raw data")
                return image_data
        except Exception as e:
            print(f"[{tag}] ERROR uploading image to S3: {e} — falling back to raw data")
            return image_data

    # ── Unknown format — pass through ──
    print(f"[{tag}] WARNING: unknown image_data format (len={len(image_data)}), passing as-is")
    return image_data
