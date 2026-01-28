"""Smoke tests for title derivation.

Run locally (optional):
    python -m backend.tests.test_titles
"""

from __future__ import annotations

import os

from backend.utils import derive_display_title


def test_derive_display_title():
    assert derive_display_title("Spiderman in action", None) == "Spiderman in action"
    assert derive_display_title("  Spiderman in action  ", None) == "Spiderman in action"
    assert derive_display_title(None, "Custom Title") == "Custom Title"
    assert derive_display_title("", "Custom Title") == "Custom Title"
    assert derive_display_title(None, None) == "Untitled"


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
    test_derive_display_title()
    smoke_db_title_roundtrip()
