"""
Central registry for TimrX image providers.

This keeps provider names, enable flags, action keys, and UI-facing constraints
in one place so new image providers do not need bespoke hardcoded branches in
every layer.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Optional

from backend.config import config


@dataclass(frozen=True)
class ImageProviderSpec:
    provider: str
    display_name: str
    model: str
    action_keys_by_size: Dict[str, str]
    image_sizes: tuple[str, ...]
    default_image_size: str
    output_modes: tuple[str, ...] = ("raster",)
    action_keys_by_output_mode: Optional[Dict[str, str]] = None
    feature_area: str = "image"
    provider_variant: Optional[str] = None
    enabled_by_default: bool = True
    enabled_flag_attr: Optional[str] = None
    api_key_attr: Optional[str] = None


IMAGE_PROVIDER_REGISTRY: Dict[str, ImageProviderSpec] = {
    "openai": ImageProviderSpec(
        provider="openai",
        display_name="OpenAI",
        model="gpt-image-1.5",
        action_keys_by_size={"1K": "image_generate", "2K": "image_generate_2k"},
        image_sizes=("1K", "2K"),
        default_image_size="1K",
        api_key_attr="OPENAI_API_KEY",
    ),
    "google": ImageProviderSpec(
        provider="google",
        display_name="Google (Imagen)",
        model="imagen-4.0-fast-generate-001",
        action_keys_by_size={"1K": "gemini_image_generate", "2K": "gemini_image_generate_2k"},
        image_sizes=("1K", "2K"),
        default_image_size="1K",
        api_key_attr="GEMINI_API_KEY",
    ),
    "nano_banana": ImageProviderSpec(
        provider="nano_banana",
        display_name="Nano Banana",
        model="gemini-2.5-flash-image",
        action_keys_by_size={
            "1K": "piapi_image_generate",
            "2K": "piapi_image_generate_2k",
            "4K": "piapi_image_generate_4k",
        },
        image_sizes=("1K", "2K", "4K"),
        default_image_size="1K",
        provider_variant="piapi",
        api_key_attr="PIAPI_API_KEY",
    ),
    "google_nano": ImageProviderSpec(
        provider="google_nano",
        display_name="Google Nano",
        model="gemini-2.5-flash-image",
        action_keys_by_size={"1K": "google_nano_image_generate"},
        image_sizes=("1K",),
        default_image_size="1K",
        provider_variant="direct_google",
        enabled_by_default=False,
        enabled_flag_attr="IMAGE_PROVIDER_GOOGLE_NANO_ENABLED",
        api_key_attr="GEMINI_API_KEY",
    ),
    "flux_pro": ImageProviderSpec(
        provider="flux_pro",
        display_name="FLUX.2 Pro",
        model="flux-2-pro",
        action_keys_by_size={"1K": "flux_pro_image_generate"},
        image_sizes=("1K",),
        default_image_size="1K",
        enabled_by_default=False,
        enabled_flag_attr="IMAGE_PROVIDER_FLUX_PRO_ENABLED",
        api_key_attr="BFL_API_KEY",
    ),
    "ideogram_v3": ImageProviderSpec(
        provider="ideogram_v3",
        display_name="Ideogram V3",
        model="ideogram-v3",
        action_keys_by_size={"1K": "ideogram_v3_image_generate"},
        image_sizes=("1K",),
        default_image_size="1K",
        enabled_by_default=False,
        enabled_flag_attr="IMAGE_PROVIDER_IDEOGRAM_V3_ENABLED",
        api_key_attr="IDEOGRAM_API_KEY",
    ),
    "recraft_v4": ImageProviderSpec(
        provider="recraft_v4",
        display_name="Recraft V4",
        model="recraftv4",
        action_keys_by_size={"1K": "recraft_v4_image_generate"},
        image_sizes=("1K",),
        default_image_size="1K",
        output_modes=("raster", "vector_svg"),
        action_keys_by_output_mode={
            "raster": "recraft_v4_image_generate",
            "vector_svg": "recraft_v4_vector_generate",
        },
        enabled_by_default=False,
        enabled_flag_attr="IMAGE_PROVIDER_RECRAFT_V4_ENABLED",
        api_key_attr="RECRAFT_API_KEY",
    ),
}


def get_image_provider_spec(provider: str) -> Optional[ImageProviderSpec]:
    return IMAGE_PROVIDER_REGISTRY.get((provider or "").lower())


def is_image_provider_enabled(provider: str) -> bool:
    spec = get_image_provider_spec(provider)
    if not spec:
        return False
    if spec.enabled_flag_attr:
        return bool(getattr(config, spec.enabled_flag_attr, False))
    return spec.enabled_by_default


def get_enabled_image_providers() -> list[str]:
    return [name for name in IMAGE_PROVIDER_REGISTRY if is_image_provider_enabled(name)]


def get_allowed_image_providers() -> list[str]:
    return sorted(IMAGE_PROVIDER_REGISTRY.keys())


def get_image_action_key(provider: str, image_size: str = "1K", output_mode: str = "raster") -> str:
    spec = get_image_provider_spec(provider) or IMAGE_PROVIDER_REGISTRY["openai"]
    normalized_output = (output_mode or "raster").lower()
    if spec.action_keys_by_output_mode:
        action_key = spec.action_keys_by_output_mode.get(normalized_output)
        if action_key:
            return action_key
    normalized_size = (image_size or spec.default_image_size).upper()
    return spec.action_keys_by_size.get(normalized_size, spec.action_keys_by_size[spec.default_image_size])

