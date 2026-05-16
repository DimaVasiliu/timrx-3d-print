"""
Prompt helpers for Meshy generation routes.

Meshy's current Text-to-3D task object still exposes ``negative_prompt`` for
backward compatibility, but the field is deprecated and does not influence
Meshy 5/6 outputs.  TimrX therefore keeps the user's avoid-list as metadata and
folds it into the active prompt as a short positive instruction.
"""

from __future__ import annotations

import re


MESHY_TEXT_PROMPT_LIMIT = 600
MESHY_TEXTURE_PROMPT_LIMIT = 600
MESHY_NEGATIVE_PROMPT_LIMIT = 240


def normalize_negative_prompt(value: object, *, limit: int = MESHY_NEGATIVE_PROMPT_LIMIT) -> str:
    """Return a compact avoid-list safe to append to a Meshy prompt."""
    text = str(value or "").strip()
    if not text:
        return ""
    text = re.sub(r"\s+", " ", text)
    # Keep this as plain language, not a second command block.
    text = re.sub(r"^(negative\s+prompt|avoid|do\s+not\s+include)\s*[:\-]\s*", "", text, flags=re.I)
    return text[:limit].rstrip(" ,.;:")


def merge_negative_prompt(
    prompt: str,
    negative_prompt: object,
    *,
    max_length: int = MESHY_TEXT_PROMPT_LIMIT,
) -> str:
    """
    Fold an avoid-list into a Meshy prompt without using deprecated API fields.

    The user's original prompt remains unchanged in TimrX metadata/history.  The
    returned value is only the provider-facing prompt.
    """
    base = re.sub(r"\s+", " ", str(prompt or "").strip())
    negative = normalize_negative_prompt(negative_prompt)
    if not base or not negative:
        return base[:max_length].rstrip()

    suffix = f" Avoid: {negative}."
    if len(base) + len(suffix) <= max_length:
        return f"{base}{suffix}"

    available = max_length - len(suffix)
    if available >= 40:
        return f"{base[:available].rstrip(' ,.;:')}{suffix}"

    # If the avoid-list itself is too long, preserve the main prompt and fit as
    # much of the avoid-list as possible.
    available_negative = max_length - len(base) - len(" Avoid: .")
    if available_negative > 12:
        trimmed_negative = negative[:available_negative].rstrip(" ,.;:")
        return f"{base} Avoid: {trimmed_negative}."

    return base[:max_length].rstrip()
