"""Smoke tests for title derivation.

Run locally (optional):
    python -m backend.tests.test_titles
"""

from __future__ import annotations

import os

from backend.utils import derive_display_title, is_generic_title


def test_is_generic_title():
    """Test detection of generic/bad titles."""
    # Generic titles should return True
    assert is_generic_title(None) is True
    assert is_generic_title("") is True
    assert is_generic_title("  ") is True
    assert is_generic_title("Untitled") is True
    assert is_generic_title("untitled") is True
    assert is_generic_title("(untitled)") is True
    assert is_generic_title("Textured Model") is True
    assert is_generic_title("textured model") is True
    assert is_generic_title("Remeshed Model") is True
    assert is_generic_title("3D Model") is True
    assert is_generic_title("Image to 3D Model") is True
    assert is_generic_title("Model") is True
    assert is_generic_title("Image") is True
    assert is_generic_title("Video") is True

    # Hex IDs (24+ chars) should return True
    assert is_generic_title("25fdaf9ae4d89e1d12345678") is True
    assert is_generic_title("abcdef1234567890abcdef1234") is True
    assert is_generic_title("019576AF15877BE6A6B5BFEEE1C5B79E") is True

    # Good titles should return False
    assert is_generic_title("Spiderman in action") is False
    assert is_generic_title("A cool robot") is False
    assert is_generic_title("My awesome model") is False
    assert is_generic_title("abc123") is False  # Short, not hex ID


def test_derive_display_title():
    """Test title derivation with priority order."""
    # Basic cases
    assert derive_display_title("Spiderman in action", None) == "Spiderman in action"
    assert derive_display_title("  Spiderman in action  ", None) == "Spiderman in action"
    assert derive_display_title(None, "Custom Title") == "Custom Title"
    assert derive_display_title("", "Custom Title") == "Custom Title"
    assert derive_display_title(None, None) == "Untitled"

    # Generic titles should be ignored in favor of prompt
    assert derive_display_title("Cool robot", "Textured Model") == "Cool robot"
    assert derive_display_title("Cool robot", "(untitled)") == "Cool robot"
    assert derive_display_title("Cool robot", "25fdaf9ae4d89e1d12345678") == "Cool robot"

    # root_prompt fallback
    assert derive_display_title(None, None, root_prompt="Original prompt") == "Original prompt"
    assert derive_display_title("", "Textured Model", root_prompt="Original prompt") == "Original prompt"

    # Good explicit title takes precedence
    assert derive_display_title("prompt text", "My Custom Title") == "My Custom Title"
    assert derive_display_title(None, "My Custom Title", root_prompt="fallback") == "My Custom Title"


def smoke_db_title_roundtrip():
    """
    Optional DB smoke check (requires DATABASE_URL).
    Inserts a temporary model row and verifies title persistence.
    """
    if not os.getenv("DATABASE_URL"):
        print("[SKIP] DATABASE_URL not set; skipping DB smoke check.")
        return

    from backend.db import get_conn, Tables

    title = derive_display_title("Spiderman in action", None)
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("BEGIN")
            cur.execute(
                f"""
                INSERT INTO {Tables.MODELS} (title, prompt, provider, status)
                VALUES (%s, %s, %s, %s)
                RETURNING id, title
                """,
                (title, "Spiderman in action", "meshy", "ready"),
            )
            row = cur.fetchone()
            assert row[1] == "Spiderman in action"
            cur.execute("ROLLBACK")
    print("[OK] DB title persistence check passed.")


if __name__ == "__main__":
    test_is_generic_title()
    print("[OK] is_generic_title tests passed.")
    test_derive_display_title()
    print("[OK] derive_display_title tests passed.")
    smoke_db_title_roundtrip()
