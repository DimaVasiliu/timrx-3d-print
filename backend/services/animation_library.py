"""
Animation Library Service — serves curated Meshy animation catalog.

The catalog is loaded from animation_library.json in the backend data directory.
Each entry maps to a Meshy action_id (integer) with category/subcategory metadata.
"""

from __future__ import annotations

import json
from functools import lru_cache
from pathlib import Path

# Catalog file lives next to this module's package
_DATA_DIR = Path(__file__).resolve().parent.parent / "data"
_CATALOG_PATH = _DATA_DIR / "animation_library.json"


@lru_cache(maxsize=1)
def get_animation_library() -> list[dict]:
    """Load and return the full animation library catalog."""
    if not _CATALOG_PATH.exists():
        print(f"[animation_library] catalog not found at {_CATALOG_PATH}")
        return []
    with open(_CATALOG_PATH, "r") as f:
        data = json.load(f)
    if isinstance(data, list):
        return data
    if isinstance(data, dict) and "items" in data:
        return data["items"]
    return []


def get_animation_by_id(action_id: int) -> dict | None:
    """Look up a single animation by action_id."""
    for anim in get_animation_library():
        if anim.get("action_id") == action_id:
            return anim
    return None


def get_categories() -> list[str]:
    """Return sorted unique category names."""
    cats = sorted({a.get("category", "") for a in get_animation_library() if a.get("category")})
    return cats
