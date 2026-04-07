"""Byte-level validation helpers for user and provider uploads."""

from __future__ import annotations

import base64
import binascii
import io
import struct
from urllib.parse import unquote_to_bytes

from PIL import Image


IMAGE_PREFIXES = {"images", "thumbnails", "textures", "source_images"}
IMAGE_MIME_BY_FORMAT = {
    "PNG": "image/png",
    "JPEG": "image/jpeg",
    "WEBP": "image/webp",
}
MODEL_MIME_TYPES = {
    "glb": "model/gltf-binary",
    "gltf": "model/gltf+json",
    "obj": "model/obj",
    "stl": "model/stl",
    "fbx": "application/x-fbx",
}
MAX_IMAGE_BYTES = 25 * 1024 * 1024
MAX_MODEL_BYTES = 200 * 1024 * 1024
MAX_IMAGE_DIMENSION = 16384


class UploadValidationError(ValueError):
    """Raised when uploaded bytes do not match the expected file type."""


def _normalize_content_type(content_type: str | None) -> str:
    raw = str(content_type or "").split(";", 1)[0].strip().lower()
    return raw or "application/octet-stream"


def parse_data_url(data_url: str) -> tuple[str, bytes]:
    """Decode a data URL into a declared MIME type and raw bytes."""
    if not isinstance(data_url, str) or not data_url.startswith("data:"):
        raise UploadValidationError("Invalid data URL")

    try:
        header, payload = data_url.split(",", 1)
    except ValueError as exc:
        raise UploadValidationError("Malformed data URL") from exc

    meta = header[5:]
    parts = meta.split(";") if meta else []
    mime = _normalize_content_type(parts[0] if parts else "")
    is_base64 = any(part.strip().lower() == "base64" for part in parts[1:])

    try:
        if is_base64:
            data_bytes = base64.b64decode(payload, validate=True)
        else:
            data_bytes = unquote_to_bytes(payload)
    except (binascii.Error, ValueError) as exc:
        raise UploadValidationError("Invalid data URL encoding") from exc

    if not data_bytes:
        raise UploadValidationError("Empty upload")

    return mime, data_bytes


def sniff_image_content_type(data_bytes: bytes) -> str:
    """Validate image bytes and return the canonical MIME type."""
    if not data_bytes:
        raise UploadValidationError("Empty image upload")
    if len(data_bytes) > MAX_IMAGE_BYTES:
        raise UploadValidationError("Image upload exceeds maximum size")

    try:
        with Image.open(io.BytesIO(data_bytes)) as img:
            img.load()
            fmt = (img.format or "").upper()
            width, height = img.size
    except Exception as exc:
        raise UploadValidationError("Invalid image content") from exc

    if fmt not in IMAGE_MIME_BY_FORMAT:
        raise UploadValidationError(f"Unsupported image format: {fmt or 'unknown'}")
    if width <= 0 or height <= 0:
        raise UploadValidationError("Invalid image dimensions")
    if max(width, height) > MAX_IMAGE_DIMENSION:
        raise UploadValidationError("Image dimensions exceed maximum size")

    return IMAGE_MIME_BY_FORMAT[fmt]


def _looks_like_ascii_stl(text: str) -> bool:
    lowered = text.lower()
    return lowered.startswith("solid") and "facet normal" in lowered and "vertex " in lowered


def _looks_like_obj(text: str) -> bool:
    markers = ("v ", "vn ", "vt ", "f ", "o ", "g ", "mtllib ", "usemtl ")
    return any(f"\n{marker}" in f"\n{text}" for marker in markers)


def sniff_model_content_type(data_bytes: bytes) -> str:
    """Validate common 3D model bytes and return the canonical MIME type."""
    if not data_bytes:
        raise UploadValidationError("Empty model upload")
    if len(data_bytes) > MAX_MODEL_BYTES:
        raise UploadValidationError("Model upload exceeds maximum size")

    if data_bytes.startswith(b"glTF"):
        return MODEL_MIME_TYPES["glb"]

    if data_bytes.startswith(b"Kaydara FBX Binary  \x00\x1a\x00"):
        return MODEL_MIME_TYPES["fbx"]

    header_text = data_bytes[:8192].decode("utf-8", errors="ignore").strip()

    if header_text.startswith("{") and '"asset"' in header_text and ('"meshes"' in header_text or '"scenes"' in header_text):
        return MODEL_MIME_TYPES["gltf"]

    if _looks_like_ascii_stl(header_text):
        return MODEL_MIME_TYPES["stl"]

    if _looks_like_obj(header_text):
        return MODEL_MIME_TYPES["obj"]

    if len(data_bytes) >= 84:
        tri_count = struct.unpack("<I", data_bytes[80:84])[0]
        expected_size = 84 + (tri_count * 50)
        if expected_size == len(data_bytes):
            return MODEL_MIME_TYPES["stl"]

    raise UploadValidationError("Unsupported or invalid 3D model content")


def validate_and_normalize_upload_bytes(
    data_bytes: bytes,
    declared_type: str | None,
    prefix: str | None,
) -> str:
    """Validate uploaded bytes based on S3 prefix and return canonical MIME."""
    normalized_prefix = (prefix or "").strip().lower()
    normalized_type = _normalize_content_type(declared_type)

    if normalized_prefix in IMAGE_PREFIXES:
        return sniff_image_content_type(data_bytes)

    if normalized_prefix == "models":
        return sniff_model_content_type(data_bytes)

    if normalized_type.startswith("image/"):
        return sniff_image_content_type(data_bytes)

    return normalized_type
