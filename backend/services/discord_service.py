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
    "IMAGE_STUDIO": "🖼️ New AI Image Generated",
    "OPENAI_IMAGE": "🖼️ New AI Image Generated",
    "GEMINI_IMAGE": "🖼️ New AI Image Generated",
    "VIDEO": "🎬 New AI Video Generated",
}


def _email_to_label(email: str) -> str:
    """Convert email to a privacy-safe display label (name part only, no domain)."""
    if not email or "@" not in email:
        return ""
    local = email.split("@")[0]
    # Clean up common separators to produce a readable name
    name = local.replace(".", " ").replace("_", " ").replace("-", " ")
    # Capitalize each word for readability
    return " ".join(w.capitalize() for w in name.split() if w)


def _get_user_label(identity_id: str) -> str:
    """Look up user display label from identity_id. Returns empty string if not found."""
    if not identity_id:
        return ""
    try:
        from backend.services.identity_service import IdentityService
        identity = IdentityService.get_identity(identity_id)
        if identity and identity.get("email"):
            return _email_to_label(identity["email"])
    except Exception:
        pass
    return ""


def _is_valid_embed_url(url) -> bool:
    """Check if a URL is valid for Discord embeds (must be HTTP/HTTPS string)."""
    return isinstance(url, str) and url.startswith(("http://", "https://"))


def _sanitize_embed(embed: dict) -> dict | None:
    """
    Validate and sanitize a Discord embed dict.

    Returns sanitized embed or None if the embed is unsalvageable.
    Discord embed constraints:
    - title: max 256 chars, must be non-empty string if present
    - description: max 4096 chars, must be non-empty string if present
    - footer.text: max 2048 chars, must be non-empty string if present
    - image.url: must be valid HTTP(S) URL
    - color: integer 0–16777215
    """
    if not isinstance(embed, dict):
        return None

    sanitized = {}

    # Title — required, non-empty string, max 256
    title = embed.get("title")
    if not isinstance(title, str) or not title.strip():
        return None
    sanitized["title"] = title[:256]

    # Color
    color = embed.get("color")
    if isinstance(color, int) and 0 <= color <= 16777215:
        sanitized["color"] = color

    # Description — optional, non-empty string, max 4096
    desc = embed.get("description")
    if isinstance(desc, str) and desc.strip():
        sanitized["description"] = desc[:4096]

    # Footer — optional, text must be non-empty string
    footer = embed.get("footer")
    if isinstance(footer, dict):
        ft = footer.get("text")
        if isinstance(ft, str) and ft.strip():
            sanitized["footer"] = {"text": ft[:2048]}

    # Image — optional, url must be valid HTTP(S)
    image = embed.get("image")
    if isinstance(image, dict):
        img_url = image.get("url")
        if _is_valid_embed_url(img_url):
            sanitized["image"] = {"url": img_url}
        else:
            logger.info("[Discord] Skipping invalid image URL (type=%s, prefix=%s)",
                        type(img_url).__name__, str(img_url)[:30] if img_url else "None")

    # URL — optional, must be valid HTTP(S)
    url = embed.get("url")
    if _is_valid_embed_url(url):
        sanitized["url"] = url

    return sanitized


def send_to_discord(title: str, prompt: str = "", image_url: str = None, identity_id: str = None):
    """
    Post a generation notification embed to the configured Discord webhook.

    Args:
        title: Embed title (e.g. "🧊 New AI 3D Model Generated")
        prompt: The user's generation prompt
        image_url: Optional thumbnail/preview URL (must be HTTP/HTTPS)
        identity_id: Optional user identity ID (to include email in footer)
    """
    if not DISCORD_WEBHOOK_URL:
        return

    # Build footer with user label if available
    user_label = _get_user_label(identity_id) if identity_id else ""
    footer_text = f"TimrX 3D Print Hub | {user_label}" if user_label else "TimrX 3D Print Hub"

    embed = {
        "title": title or "✨ New Creation on TimrX",
        "color": 5814783,
        "footer": {"text": footer_text},
    }

    if prompt and isinstance(prompt, str):
        embed["description"] = f"Prompt:\n{prompt[:200]}"

    if image_url and _is_valid_embed_url(image_url):
        embed["image"] = {"url": image_url}
    elif image_url:
        logger.info("[Discord] Dropping non-HTTP image_url (type=%s, prefix=%s)",
                     type(image_url).__name__, str(image_url)[:30])

    # Sanitize embed to catch any remaining invalid fields
    embed = _sanitize_embed(embed)
    if not embed:
        logger.warning("[Discord] Embed validation failed, falling back to plain content")
        embed = None

    if embed:
        payload = {
            "username": "TimrX Generator",
            "embeds": [embed],
        }
    else:
        # Fallback: plain content-only message (no embed)
        parts = [title or "New creation on TimrX"]
        if prompt and isinstance(prompt, str):
            parts.append(f"Prompt: {prompt[:200]}")
        payload = {
            "username": "TimrX Generator",
            "content": "\n".join(parts),
        }

    try:
        resp = requests.post(DISCORD_WEBHOOK_URL, json=payload, timeout=5)
        if resp.status_code not in (200, 204):
            logger.warning("[Discord] Webhook returned %s: %s | embed_keys=%s has_image=%s",
                           resp.status_code, resp.text[:500],
                           list(embed.keys()) if embed else "fallback",
                           "image" in embed if embed else False)
            # If embed was rejected, retry once with plain content fallback
            if embed and resp.status_code == 400:
                parts = [title or "New creation on TimrX"]
                if prompt and isinstance(prompt, str):
                    parts.append(f"Prompt: {prompt[:200]}")
                fallback = {
                    "username": "TimrX Generator",
                    "content": "\n".join(parts),
                }
                try:
                    fb_resp = requests.post(DISCORD_WEBHOOK_URL, json=fallback, timeout=5)
                    if fb_resp.status_code in (200, 204):
                        logger.info("[Discord] Fallback plain-content message succeeded")
                    else:
                        logger.warning("[Discord] Fallback also failed %s: %s",
                                       fb_resp.status_code, fb_resp.text[:200])
                except Exception as fb_err:
                    logger.error("[Discord] Fallback send error: %s", fb_err)
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
