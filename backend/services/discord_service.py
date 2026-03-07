"""
Discord Webhook Service — Posts generation notifications to Discord.
"""

import logging
import requests

from backend.config import DISCORD_WEBHOOK_URL

logger = logging.getLogger(__name__)

# Map action_code prefixes to Discord embed titles
_ACTION_TITLES = {
    "MESHY_TEXT_TO_3D": "🧊 New AI 3D Model Generated",
    "MESHY_IMAGE_TO_3D": "🧊 New AI 3D Model Generated",
    "MESHY_REFINE": "🧊 New AI 3D Model Refined",
    "MESHY_REMESH": "🔷 3D Model Remeshed",
    "MESHY_TEXTURE": "🎨 AI Texture Generated",
    "MESHY_RIG": "🦴 3D Model Auto-Rigged",
    "IMAGE_STUDIO": "🖼️ New AI Image Generated",
    "OPENAI_IMAGE": "🖼️ New AI Image Generated",
    "GEMINI_IMAGE": "🖼️ New AI Image Generated",
    "VIDEO": "🎬 New AI Video Generated",
}


def _get_user_email(identity_id: str) -> str:
    """Look up user email from identity_id. Returns empty string if not found."""
    if not identity_id:
        return ""
    try:
        from backend.services.identity_service import IdentityService
        identity = IdentityService.get_identity(identity_id)
        if identity and identity.get("email"):
            return identity["email"]
    except Exception:
        pass
    return ""


def send_to_discord(title: str, prompt: str = "", image_url: str = None, identity_id: str = None):
    """
    Post a generation notification embed to the configured Discord webhook.

    Args:
        title: Embed title (e.g. "🧊 New AI 3D Model Generated")
        prompt: The user's generation prompt
        image_url: Optional thumbnail/preview URL
        identity_id: Optional user identity ID (to include email in footer)
    """
    if not DISCORD_WEBHOOK_URL:
        return

    # Build footer with user email if available
    user_email = _get_user_email(identity_id) if identity_id else ""
    footer_text = f"TimrX 3D Print Hub | {user_email}" if user_email else "TimrX 3D Print Hub"

    embed = {
        "title": title,
        "color": 5814783,
        "footer": {"text": footer_text},
    }

    if prompt:
        embed["description"] = f"Prompt:\n{prompt[:200]}"

    if image_url:
        embed["image"] = {"url": image_url}

    payload = {
        "username": "TimrX Generator",
        "embeds": [embed],
    }

    try:
        resp = requests.post(DISCORD_WEBHOOK_URL, json=payload, timeout=5)
        if resp.status_code not in (200, 204):
            logger.warning("[Discord] Webhook returned %s: %s", resp.status_code, resp.text[:200])
    except Exception as e:
        logger.error("[Discord] Webhook error: %s", e)


def get_title_for_action(action_code: str) -> str:
    """Get the Discord embed title for a given action_code."""
    if not action_code:
        return "✨ New Creation on TimrX"
    upper = action_code.upper()
    for key, title in _ACTION_TITLES.items():
        if key in upper:
            return title
    return "✨ New Creation on TimrX"
