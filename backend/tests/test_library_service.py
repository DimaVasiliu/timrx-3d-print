"""
Behavioural tests for the library organisation service (migration 083).

These exercise real SQL — the constraints, cascades and ownership checks are
the whole point, and a mocked cursor would prove none of them. The suite
therefore needs a Postgres it may create and drop schemas in, and skips
itself when one is not configured.

Run against a scratch database:

    TIMRX_TEST_DSN="host=/tmp port=5433 user=postgres dbname=postgres" \
        pytest backend/tests/test_library_service.py

The fixture builds only the columns the service touches, then applies
migrations/083_library_favorites_tags_collections.sql verbatim, so a drift
between the migration and the service shows up here.
"""

from __future__ import annotations

import os
import sys
import types
import uuid
from pathlib import Path

import pytest

DSN = os.getenv("TIMRX_TEST_DSN")
psycopg = pytest.importorskip("psycopg", reason="psycopg is required for library service tests")
pytestmark = pytest.mark.skipif(not DSN, reason="set TIMRX_TEST_DSN to run library service tests")

REPO_ROOT = Path(__file__).resolve().parents[2]
MIGRATION = REPO_ROOT / "migrations" / "083_library_favorites_tags_collections.sql"

FIXTURE_SQL = """
CREATE EXTENSION IF NOT EXISTS pgcrypto;
DROP SCHEMA IF EXISTS timrx_app CASCADE;
DROP SCHEMA IF EXISTS timrx_billing CASCADE;
CREATE SCHEMA timrx_app;
CREATE SCHEMA timrx_billing;
CREATE TABLE timrx_billing.identities (id UUID PRIMARY KEY DEFAULT gen_random_uuid(), email TEXT);
CREATE TABLE timrx_app.history_items (
  id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  identity_id UUID REFERENCES timrx_billing.identities(id) ON DELETE SET NULL,
  item_type   TEXT NOT NULL,
  status      TEXT NOT NULL DEFAULT 'processing',
  created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
"""


@pytest.fixture(scope="module")
def lib():
    """Import the service with a stub backend.db pointed at the test DSN."""
    from psycopg.rows import dict_row as _dict_row

    stub = types.ModuleType("backend.db")
    stub.USE_DB = True
    stub.dict_row = _dict_row
    stub.get_conn = lambda: psycopg.connect(DSN)

    class _Tables:
        HISTORY_ITEMS = "timrx_app.history_items"

    stub.Tables = _Tables

    if str(REPO_ROOT) not in sys.path:
        sys.path.insert(0, str(REPO_ROOT))
    sys.modules["backend.db"] = stub

    from backend.services import library_service

    with psycopg.connect(DSN, autocommit=True) as conn:
        conn.execute(FIXTURE_SQL)
        conn.execute(MIGRATION.read_text(encoding="utf-8"))

    return library_service


@pytest.fixture()
def world(lib):
    """Two identities; two assets owned by `me`, one owned by `other`."""
    me, other = str(uuid.uuid4()), str(uuid.uuid4())
    a, b, theirs = str(uuid.uuid4()), str(uuid.uuid4()), str(uuid.uuid4())
    with psycopg.connect(DSN, autocommit=True) as conn:
        conn.execute("TRUNCATE timrx_app.history_items, timrx_billing.identities CASCADE")
        conn.execute(
            "INSERT INTO timrx_billing.identities (id, email) VALUES (%s,'me@t.local'),(%s,'other@t.local')",
            (me, other),
        )
        conn.execute(
            """INSERT INTO timrx_app.history_items (id, identity_id, item_type)
               VALUES (%s,%s,'model'),(%s,%s,'image'),(%s,%s,'model')""",
            (a, me, b, me, theirs, other),
        )
    return types.SimpleNamespace(me=me, other=other, a=a, b=b, theirs=theirs)


# --- favorites -------------------------------------------------------------
def test_toggle_favorite_round_trips(lib, world):
    assert lib.toggle_favorite(world.me, world.a) is True
    assert lib.toggle_favorite(world.me, world.a) is False


def test_set_favorite_is_idempotent(lib, world):
    lib.set_favorite(world.me, world.a, True)
    lib.set_favorite(world.me, world.a, True)
    assert lib.list_favorite_ids(world.me) == [world.a]


def test_cannot_favorite_another_identitys_asset(lib, world):
    with pytest.raises(lib.LibraryError) as exc:
        lib.toggle_favorite(world.me, world.theirs)
    assert exc.value.code == "NOT_FOUND"


def test_bulk_favorite_skips_unowned(lib, world):
    assert lib.bulk_set_favorite(world.me, [world.a, world.b, world.theirs], True) == 2
    assert sorted(lib.list_favorite_ids(world.me)) == sorted([world.a, world.b])


# --- tags ------------------------------------------------------------------
def test_tags_are_normalised_and_deduped(lib, world):
    assert lib.add_tag(world.me, world.a, "  Low   Poly ") == "low poly"
    assert lib.add_tag(world.me, world.a, "LOW POLY") == "low poly"
    assert lib.tags_by_history(world.me)[world.a] == ["low poly"]


@pytest.mark.parametrize("raw,code", [("   ", "TAG_EMPTY"), ("x" * 49, "TAG_TOO_LONG")])
def test_bad_tags_rejected(lib, world, raw, code):
    with pytest.raises(lib.LibraryError) as exc:
        lib.add_tag(world.me, world.a, raw)
    assert exc.value.code == code


def test_per_asset_tag_cap(lib, world):
    for i in range(lib.MAX_TAGS_PER_ASSET):
        lib.add_tag(world.me, world.a, f"tag{i}")
    with pytest.raises(lib.LibraryError) as exc:
        lib.add_tag(world.me, world.a, "one-too-many")
    assert exc.value.code == "TOO_MANY_TAGS"


def test_bulk_tag_respects_the_cap(lib, world):
    for i in range(lib.MAX_TAGS_PER_ASSET):
        lib.add_tag(world.me, world.a, f"tag{i}")
    # `a` is full, `b` is empty — only `b` should take the new tag.
    assert lib.bulk_add_tag(world.me, [world.a, world.b], "batch") == 1


def test_cannot_tag_another_identitys_asset(lib, world):
    with pytest.raises(lib.LibraryError) as exc:
        lib.add_tag(world.me, world.theirs, "mine")
    assert exc.value.code == "NOT_FOUND"


# --- collections -----------------------------------------------------------
def test_duplicate_collection_name_returns_the_same_row(lib, world):
    first = lib.create_collection(world.me, "  Print Queue ", "#7fc8c2")
    again = lib.create_collection(world.me, "print queue")
    assert first["name"] == "Print Queue"
    assert again["id"] == first["id"]


def test_membership_add_is_idempotent_and_ownership_scoped(lib, world):
    c = lib.create_collection(world.me, "Client X")
    assert lib.add_to_collection(world.me, c["id"], [world.a, world.b, world.theirs]) == 2
    assert lib.add_to_collection(world.me, c["id"], [world.a, world.b]) == 0
    assert [x for x in lib.list_collections(world.me) if x["id"] == c["id"]][0]["item_count"] == 2


def test_cannot_touch_another_identitys_collection(lib, world):
    c = lib.create_collection(world.me, "Mine")
    with pytest.raises(lib.LibraryError) as exc:
        lib.add_to_collection(world.other, c["id"], [world.theirs])
    assert exc.value.code == "NOT_FOUND"


def test_deleting_an_asset_cascades_to_organisation_rows(lib, world):
    c = lib.create_collection(world.me, "Temp")
    lib.set_favorite(world.me, world.b, True)
    lib.add_tag(world.me, world.b, "doomed")
    lib.add_to_collection(world.me, c["id"], [world.b])

    with psycopg.connect(DSN, autocommit=True) as conn:
        conn.execute("DELETE FROM timrx_app.history_items WHERE id = %s", (world.b,))

    assert world.b not in lib.list_favorite_ids(world.me)
    assert world.b not in lib.tags_by_history(world.me)
    assert world.b not in lib.collections_by_history(world.me)


def test_overview_returns_every_section(lib, world):
    overview = lib.get_overview(world.me)
    assert set(overview) == {
        "favorites", "tags", "tags_by_asset", "collections", "collections_by_asset",
    }


# --- regressions found in the post-ship review -----------------------------
def test_readding_an_existing_tag_at_the_cap_is_a_noop_not_an_error(lib, world):
    """The cap counted the tag being re-added, so re-sending a tag the asset
    already had raised TOO_MANY_TAGS once the asset was full — and the UI
    retries these from optimistic handlers."""
    for i in range(lib.MAX_TAGS_PER_ASSET):
        lib.add_tag(world.me, world.a, f"tag{i}")
    assert lib.add_tag(world.me, world.a, "tag0") == "tag0"


def test_bulk_favorite_reports_rows_actually_changed(lib, world):
    """It returned len(owned), so re-favouriting already-starred assets
    reported "2 of 2 updated" while changing nothing."""
    assert lib.bulk_set_favorite(world.me, [world.a, world.b], True) == 2
    assert lib.bulk_set_favorite(world.me, [world.a, world.b], True) == 0


def test_rename_rejects_an_overlong_name_cleanly(lib, world):
    """create_collection validated length; rename did not, so the column
    CHECK fired and the endpoint returned a 500 HTML page."""
    c = lib.create_collection(world.me, "Short")
    with pytest.raises(lib.LibraryError) as exc:
        lib.rename_collection(world.me, c["id"], "x" * 100, None)
    assert exc.value.code == "NAME_TOO_LONG"


def test_rename_into_an_existing_name_is_a_conflict_not_a_crash(lib, world):
    a = lib.create_collection(world.me, "Alpha")
    lib.create_collection(world.me, "Beta")
    with pytest.raises(lib.LibraryError) as exc:
        lib.rename_collection(world.me, a["id"], "beta", None)
    assert exc.value.code == "NAME_TAKEN"
    assert exc.value.status == 409


def test_create_collection_reports_whether_it_actually_created(lib, world):
    """ON CONFLICT DO UPDATE returns the pre-existing row, so the endpoint
    answered 201 "created" and item_count 0 for a collection that already
    existed and already held assets."""
    first = lib.create_collection(world.me, "Reused")
    lib.add_to_collection(world.me, first["id"], [world.a])
    again = lib.create_collection(world.me, "reused")
    assert again["created"] is False
    assert again["item_count"] == 1
