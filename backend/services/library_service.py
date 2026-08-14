"""
Library organisation service — favorites, tags and collections.

Backs the Phase 2 features of the unified My Assets library on the tables
created by migration 083. Everything here is anchored on
``timrx_app.history_items`` so a single code path covers models, images and
videos.

Two rules hold throughout:

  1. **Ownership is verified, never assumed.** Every write first checks that
     the target history item belongs to the calling identity. Without that a
     user could favourite, tag, or collect somebody else's asset by guessing a
     UUID — the tables themselves only enforce referential integrity, not who
     may reference what.

  2. **Writes are idempotent.** The UI fires these from optimistic click
     handlers that may retry, so adding a favourite twice, or the same tag
     twice, must not raise.
"""

from __future__ import annotations

import re
from typing import Any, Dict, Iterable, List, Optional, Sequence

from backend.db import USE_DB, get_conn, dict_row, Tables

# Table names. These follow the Tables convention but the constants live here
# because migration 083 is library-specific and nothing else references them.
_APP = Tables.HISTORY_ITEMS.rsplit(".", 1)[0]
FAVORITES = f"{_APP}.asset_favorites"
TAGS = f"{_APP}.asset_tags"
COLLECTIONS = f"{_APP}.asset_collections"
COLLECTION_ITEMS = f"{_APP}.asset_collection_items"

# Guard rails. The DB enforces per-row shape (see migration 083); these bound
# how much one identity can accumulate, which a CHECK constraint cannot express.
MAX_TAG_LENGTH = 48
MAX_TAGS_PER_ASSET = 12
MAX_COLLECTIONS_PER_IDENTITY = 100
MAX_BULK_IDS = 200

_UUID_RE = re.compile(r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$")


class LibraryError(Exception):
    """Raised for user-correctable problems; carries an API error code."""

    def __init__(self, code: str, message: str, status: int = 400):
        super().__init__(message)
        self.code = code
        self.message = message
        self.status = status


def is_uuid(value: Any) -> bool:
    return isinstance(value, str) and bool(_UUID_RE.match(value.strip()))


def normalize_tag(raw: Any) -> str:
    """Lower-case, collapse whitespace, trim. Mirrors the DB CHECK constraint."""
    text = re.sub(r"\s+", " ", str(raw or "")).strip().lower()
    if not text:
        raise LibraryError("TAG_EMPTY", "A tag needs at least one character.")
    if len(text) > MAX_TAG_LENGTH:
        raise LibraryError("TAG_TOO_LONG", f"Tags are limited to {MAX_TAG_LENGTH} characters.")
    return text


def clean_ids(raw: Any) -> List[str]:
    """Accept a single id or a list; drop anything that is not a UUID."""
    if raw is None:
        return []
    values = raw if isinstance(raw, (list, tuple, set)) else [raw]
    out: List[str] = []
    seen = set()
    for v in values:
        s = str(v).strip()
        if is_uuid(s) and s not in seen:
            seen.add(s)
            out.append(s)
    if len(out) > MAX_BULK_IDS:
        raise LibraryError("TOO_MANY_ITEMS", f"Up to {MAX_BULK_IDS} assets can be changed at once.")
    return out


# ---------------------------------------------------------------------------
# Ownership
# ---------------------------------------------------------------------------
def owned_history_ids(identity_id: str, history_ids: Sequence[str]) -> List[str]:
    """Return the subset of history_ids that belong to this identity.

    Silently dropping unowned ids (rather than erroring) keeps bulk actions
    usable when a stale card is still on screen, while making it impossible to
    touch another account's assets.
    """
    if not USE_DB or not identity_id or not history_ids:
        return []
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT id::text FROM {Tables.HISTORY_ITEMS}
                 WHERE identity_id = %s AND id = ANY(%s::uuid[])
                """,
                (identity_id, list(history_ids)),
            )
            return [r[0] for r in cur.fetchall()]


def _require_owned(identity_id: str, history_id: str) -> str:
    owned = owned_history_ids(identity_id, [history_id])
    if not owned:
        raise LibraryError("NOT_FOUND", "That asset does not exist in your library.", status=404)
    return owned[0]


# ---------------------------------------------------------------------------
# Favorites
# ---------------------------------------------------------------------------
def set_favorite(identity_id: str, history_id: str, favorited: bool) -> bool:
    """Star or un-star one asset. Returns the resulting state."""
    _require_owned(identity_id, history_id)
    with get_conn() as conn:
        with conn.cursor() as cur:
            if favorited:
                cur.execute(
                    f"""
                    INSERT INTO {FAVORITES} (identity_id, history_id)
                    VALUES (%s, %s) ON CONFLICT DO NOTHING
                    """,
                    (identity_id, history_id),
                )
            else:
                cur.execute(
                    f"DELETE FROM {FAVORITES} WHERE identity_id = %s AND history_id = %s",
                    (identity_id, history_id),
                )
        conn.commit()
    return favorited


def toggle_favorite(identity_id: str, history_id: str) -> bool:
    """Flip the star in one round trip. Returns the new state."""
    _require_owned(identity_id, history_id)
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"DELETE FROM {FAVORITES} WHERE identity_id = %s AND history_id = %s RETURNING 1",
                (identity_id, history_id),
            )
            if cur.fetchone():
                conn.commit()
                return False
            cur.execute(
                f"INSERT INTO {FAVORITES} (identity_id, history_id) VALUES (%s, %s) ON CONFLICT DO NOTHING",
                (identity_id, history_id),
            )
        conn.commit()
    return True


def bulk_set_favorite(identity_id: str, history_ids: Sequence[str], favorited: bool) -> int:
    owned = owned_history_ids(identity_id, history_ids)
    if not owned:
        return 0
    with get_conn() as conn:
        with conn.cursor() as cur:
            if favorited:
                cur.execute(
                    f"""
                    INSERT INTO {FAVORITES} (identity_id, history_id)
                    SELECT %s, unnest(%s::uuid[])
                    ON CONFLICT DO NOTHING
                    """,
                    (identity_id, owned),
                )
            else:
                cur.execute(
                    f"DELETE FROM {FAVORITES} WHERE identity_id = %s AND history_id = ANY(%s::uuid[])",
                    (identity_id, owned),
                )
            # rowcount, not len(owned): re-favouriting something already
            # starred changes nothing, and the UI reports this number back to
            # the user as "n of m updated".
            changed = cur.rowcount
        conn.commit()
    return max(0, changed)


def list_favorite_ids(identity_id: str) -> List[str]:
    if not USE_DB or not identity_id:
        return []
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT history_id::text FROM {FAVORITES} WHERE identity_id = %s ORDER BY created_at DESC",
                (identity_id,),
            )
            return [r[0] for r in cur.fetchall()]


# ---------------------------------------------------------------------------
# Tags
# ---------------------------------------------------------------------------
def add_tag(identity_id: str, history_id: str, raw_tag: str) -> str:
    tag = normalize_tag(raw_tag)
    _require_owned(identity_id, history_id)
    label = re.sub(r"\s+", " ", str(raw_tag or "")).strip()[:MAX_TAG_LENGTH]
    with get_conn() as conn:
        with conn.cursor() as cur:
            # Exclude the tag being (re-)added: re-sending a tag the asset
            # already has is a no-op, and must not trip the cap. The UI fires
            # these from optimistic handlers that retry.
            cur.execute(
                f"SELECT COUNT(*) FROM {TAGS} WHERE identity_id = %s AND history_id = %s AND tag <> %s",
                (identity_id, history_id, tag),
            )
            existing = cur.fetchone()[0]
            if existing >= MAX_TAGS_PER_ASSET:
                raise LibraryError(
                    "TOO_MANY_TAGS",
                    f"An asset can carry up to {MAX_TAGS_PER_ASSET} tags.",
                )
            cur.execute(
                f"""
                INSERT INTO {TAGS} (identity_id, history_id, tag, label)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (identity_id, history_id, tag) DO NOTHING
                """,
                (identity_id, history_id, tag, label),
            )
        conn.commit()
    return tag


def remove_tag(identity_id: str, history_id: str, raw_tag: str) -> bool:
    tag = normalize_tag(raw_tag)
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"DELETE FROM {TAGS} WHERE identity_id = %s AND history_id = %s AND tag = %s RETURNING 1",
                (identity_id, history_id, tag),
            )
            removed = cur.fetchone() is not None
        conn.commit()
    return removed


def bulk_add_tag(identity_id: str, history_ids: Sequence[str], raw_tag: str) -> int:
    tag = normalize_tag(raw_tag)
    owned = owned_history_ids(identity_id, history_ids)
    if not owned:
        return 0
    label = re.sub(r"\s+", " ", str(raw_tag or "")).strip()[:MAX_TAG_LENGTH]
    with get_conn() as conn:
        with conn.cursor() as cur:
            # Only tag assets that are still under the per-asset cap, so a bulk
            # action can never push one asset past the limit a single add
            # would have refused.
            cur.execute(
                f"""
                INSERT INTO {TAGS} (identity_id, history_id, tag, label)
                SELECT %s, h.id, %s, %s
                  FROM unnest(%s::uuid[]) AS h(id)
                 WHERE (
                        SELECT COUNT(*) FROM {TAGS} t
                         WHERE t.identity_id = %s AND t.history_id = h.id
                       ) < %s
                ON CONFLICT (identity_id, history_id, tag) DO NOTHING
                """,
                (identity_id, tag, label, owned, identity_id, MAX_TAGS_PER_ASSET),
            )
            changed = cur.rowcount
        conn.commit()
    return max(0, changed)


def list_tags(identity_id: str) -> List[Dict[str, Any]]:
    """Distinct tags for this user with how many assets carry each."""
    if not USE_DB or not identity_id:
        return []
    with get_conn() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                f"""
                SELECT tag,
                       COALESCE(MAX(label), tag) AS label,
                       COUNT(*)::int            AS count
                  FROM {TAGS}
                 WHERE identity_id = %s
                 GROUP BY tag
                 ORDER BY count DESC, tag ASC
                """,
                (identity_id,),
            )
            return [dict(r) for r in cur.fetchall()]


def tags_by_history(identity_id: str) -> Dict[str, List[str]]:
    if not USE_DB or not identity_id:
        return {}
    out: Dict[str, List[str]] = {}
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT history_id::text, tag FROM {TAGS} WHERE identity_id = %s ORDER BY tag",
                (identity_id,),
            )
            for history_id, tag in cur.fetchall():
                out.setdefault(history_id, []).append(tag)
    return out


# ---------------------------------------------------------------------------
# Collections
# ---------------------------------------------------------------------------
def create_collection(identity_id: str, name: str, color: Optional[str] = None) -> Dict[str, Any]:
    clean_name = re.sub(r"\s+", " ", str(name or "")).strip()
    if not clean_name:
        raise LibraryError("NAME_REQUIRED", "Give the collection a name.")
    if len(clean_name) > 64:
        raise LibraryError("NAME_TOO_LONG", "Collection names are limited to 64 characters.")

    with get_conn() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                f"SELECT COUNT(*) AS n FROM {COLLECTIONS} WHERE identity_id = %s",
                (identity_id,),
            )
            if cur.fetchone()["n"] >= MAX_COLLECTIONS_PER_IDENTITY:
                raise LibraryError(
                    "TOO_MANY_COLLECTIONS",
                    f"You can keep up to {MAX_COLLECTIONS_PER_IDENTITY} collections.",
                )
            cur.execute(
                f"""
                INSERT INTO {COLLECTIONS} (identity_id, name, color)
                VALUES (%s, %s, %s)
                ON CONFLICT (identity_id, lower(btrim(name))) DO UPDATE
                    SET color = COALESCE(EXCLUDED.color, {COLLECTIONS}.color)
                RETURNING id::text, name, color, created_at, updated_at
                """,
                (identity_id, clean_name, color),
            )
            row = cur.fetchone()
            # ON CONFLICT DO UPDATE returns the pre-existing row, so "created"
            # would be a lie and item_count 0 would be wrong for a collection
            # that already holds assets.
            cur.execute(
                f"SELECT COUNT(*)::int AS n FROM {COLLECTION_ITEMS} WHERE collection_id = %s",
                (row["id"],),
            )
            item_count = cur.fetchone()["n"]
        conn.commit()
    created = item_count == 0 and str(row["name"]).strip().lower() == clean_name.lower()
    return {**dict(row), "item_count": item_count, "created": created}


def rename_collection(identity_id: str, collection_id: str, name: Optional[str], color: Optional[str]) -> Dict[str, Any]:
    if not is_uuid(collection_id):
        raise LibraryError("NOT_FOUND", "No such collection.", status=404)
    clean_name = re.sub(r"\s+", " ", str(name or "")).strip() if name is not None else None
    if name is not None and not clean_name:
        raise LibraryError("NAME_REQUIRED", "Give the collection a name.")
    # create_collection enforces this; without the same check here a rename
    # hits the column CHECK and surfaces as a 500 instead of a clean 400.
    if clean_name and len(clean_name) > 64:
        raise LibraryError("NAME_TOO_LONG", "Collection names are limited to 64 characters.")
    try:
      with get_conn() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                f"""
                UPDATE {COLLECTIONS}
                   SET name       = COALESCE(%s, name),
                       color      = COALESCE(%s, color),
                       updated_at = NOW()
                 WHERE id = %s AND identity_id = %s
                RETURNING id::text, name, color, created_at, updated_at
                """,
                (clean_name, color, collection_id, identity_id),
            )
            row = cur.fetchone()
        conn.commit()
    except LibraryError:
        raise
    except Exception as exc:  # unique index on (identity_id, lower(btrim(name)))
        if "unique" in str(exc).lower() or "duplicate" in str(exc).lower():
            raise LibraryError("NAME_TAKEN", "You already have a collection with that name.", status=409)
        raise
    if not row:
        raise LibraryError("NOT_FOUND", "No such collection.", status=404)
    return dict(row)


def delete_collection(identity_id: str, collection_id: str) -> bool:
    if not is_uuid(collection_id):
        return False
    with get_conn() as conn:
        with conn.cursor() as cur:
            # Membership rows cascade; the assets themselves are untouched.
            cur.execute(
                f"DELETE FROM {COLLECTIONS} WHERE id = %s AND identity_id = %s RETURNING 1",
                (collection_id, identity_id),
            )
            deleted = cur.fetchone() is not None
        conn.commit()
    return deleted


def list_collections(identity_id: str) -> List[Dict[str, Any]]:
    if not USE_DB or not identity_id:
        return []
    with get_conn() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                f"""
                SELECT c.id::text, c.name, c.color, c.created_at, c.updated_at,
                       COUNT(i.history_id)::int AS item_count
                  FROM {COLLECTIONS} c
                  LEFT JOIN {COLLECTION_ITEMS} i ON i.collection_id = c.id
                 WHERE c.identity_id = %s
                 GROUP BY c.id
                 ORDER BY c.updated_at DESC, c.name ASC
                """,
                (identity_id,),
            )
            return [dict(r) for r in cur.fetchall()]


def _owned_collection(cur, identity_id: str, collection_id: str) -> None:
    cur.execute(
        f"SELECT 1 FROM {COLLECTIONS} WHERE id = %s AND identity_id = %s",
        (collection_id, identity_id),
    )
    if not cur.fetchone():
        raise LibraryError("NOT_FOUND", "No such collection.", status=404)


def add_to_collection(identity_id: str, collection_id: str, history_ids: Sequence[str]) -> int:
    if not is_uuid(collection_id):
        raise LibraryError("NOT_FOUND", "No such collection.", status=404)
    owned = owned_history_ids(identity_id, history_ids)
    if not owned:
        return 0
    with get_conn() as conn:
        with conn.cursor() as cur:
            _owned_collection(cur, identity_id, collection_id)
            cur.execute(
                f"""
                INSERT INTO {COLLECTION_ITEMS} (collection_id, history_id, position)
                SELECT %s, h.id,
                       COALESCE((SELECT MAX(position) FROM {COLLECTION_ITEMS} WHERE collection_id = %s), 0)
                       + row_number() OVER ()
                  FROM unnest(%s::uuid[]) AS h(id)
                ON CONFLICT (collection_id, history_id) DO NOTHING
                """,
                (collection_id, collection_id, owned),
            )
            changed = cur.rowcount
        conn.commit()
    return max(0, changed)


def remove_from_collection(identity_id: str, collection_id: str, history_ids: Sequence[str]) -> int:
    if not is_uuid(collection_id):
        raise LibraryError("NOT_FOUND", "No such collection.", status=404)
    ids = clean_ids(history_ids)
    if not ids:
        return 0
    with get_conn() as conn:
        with conn.cursor() as cur:
            _owned_collection(cur, identity_id, collection_id)
            cur.execute(
                f"""
                DELETE FROM {COLLECTION_ITEMS}
                 WHERE collection_id = %s AND history_id = ANY(%s::uuid[])
                """,
                (collection_id, ids),
            )
            changed = cur.rowcount
        conn.commit()
    return max(0, changed)


def collections_by_history(identity_id: str) -> Dict[str, List[str]]:
    """{history_id: [collection_id, ...]} — drives the card menu's checkmarks."""
    if not USE_DB or not identity_id:
        return {}
    out: Dict[str, List[str]] = {}
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT i.history_id::text, i.collection_id::text
                  FROM {COLLECTION_ITEMS} i
                  JOIN {COLLECTIONS} c ON c.id = i.collection_id
                 WHERE c.identity_id = %s
                """,
                (identity_id,),
            )
            for history_id, collection_id in cur.fetchall():
                out.setdefault(history_id, []).append(collection_id)
    return out


# ---------------------------------------------------------------------------
# Overview
# ---------------------------------------------------------------------------
def get_overview(identity_id: str) -> Dict[str, Any]:
    """
    Everything the library needs to decorate a page of cards, in one call.

    The alternative — a request per card for its stars/tags/collections — would
    put dozens of round trips behind opening the modal.
    """
    if not USE_DB or not identity_id:
        return {"favorites": [], "tags": [], "tags_by_asset": {}, "collections": [], "collections_by_asset": {}}
    return {
        "favorites": list_favorite_ids(identity_id),
        "tags": list_tags(identity_id),
        "tags_by_asset": tags_by_history(identity_id),
        "collections": list_collections(identity_id),
        "collections_by_asset": collections_by_history(identity_id),
    }
