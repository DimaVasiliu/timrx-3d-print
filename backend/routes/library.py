"""
Library Routes Blueprint — favorites, tags and collections.

Phase 2 of the unified My Assets library. Every endpoint is scoped to the
calling identity; the service layer re-checks ownership on each write rather
than trusting the id in the URL.

Registered under both /api/_mod and /api (matching the existing history and
jobs blueprints), so the front end can call /api/library/... directly.
"""

from __future__ import annotations

from flask import Blueprint, g, jsonify, request

from backend.middleware import with_session, with_session_readonly
from backend.services import library_service as lib
from backend.services.library_service import LibraryError

bp = Blueprint("library", __name__)


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------
def _identity_or_401():
    identity_id = getattr(g, "identity_id", None)
    if not identity_id:
        return None, (
            jsonify({
                "ok": False,
                "error": {"code": "NO_SESSION", "message": "A valid session is required to use your library."},
            }),
            401,
        )
    return identity_id, None


def _fail(exc: LibraryError):
    return jsonify({"ok": False, "error": {"code": exc.code, "message": exc.message}}), exc.status


def _unavailable(where: str, exc: Exception):
    """Any non-LibraryError is a DB/infra problem, not the user's fault.

    Without this every endpoint returns a 500 HTML error page on a pool
    timeout, which the front end cannot parse and reports as a generic
    failure. 503 + JSON lets the UI say something true and retry.
    """
    print(f"[LIBRARY] {where} failed: {type(exc).__name__}: {exc}")
    return jsonify({
        "ok": False,
        "error": {"code": "LIBRARY_UNAVAILABLE", "message": "Your library is temporarily unavailable. Try again in a moment."},
    }), 503


def _body() -> dict:
    data = request.get_json(silent=True)
    return data if isinstance(data, dict) else {}


# ---------------------------------------------------------------------------
# overview — one call that decorates a whole page of cards
# ---------------------------------------------------------------------------
@bp.route("/library/overview", methods=["GET", "OPTIONS"])
@with_session_readonly
def library_overview():
    if request.method == "OPTIONS":
        return ("", 204)
    identity_id, err = _identity_or_401()
    if err:
        return err
    try:
        return jsonify({"ok": True, **lib.get_overview(identity_id)})
    except Exception as exc:  # noqa: BLE001
        # Overview is the one endpoint that must never fail the modal open:
        # an empty overview just means a library with no stars or tags yet.
        print(f"[LIBRARY] overview failed for {identity_id}: {type(exc).__name__}: {exc}")
        return jsonify({
            "ok": True,
            "favorites": [], "tags": [], "tags_by_asset": {},
            "collections": [], "collections_by_asset": {},
            "degraded": True,
        })


# ---------------------------------------------------------------------------
# favorites
# ---------------------------------------------------------------------------
@bp.route("/library/favorites", methods=["GET", "OPTIONS"])
@with_session_readonly
def library_favorites_list():
    if request.method == "OPTIONS":
        return ("", 204)
    identity_id, err = _identity_or_401()
    if err:
        return err
    try:
        return jsonify({"ok": True, "favorites": lib.list_favorite_ids(identity_id)})
    except Exception as exc:  # noqa: BLE001
        return _unavailable(request.path, exc)


@bp.route("/library/favorites/<history_id>", methods=["POST", "DELETE", "OPTIONS"])
@with_session
def library_favorite_item(history_id: str):
    if request.method == "OPTIONS":
        return ("", 204)
    identity_id, err = _identity_or_401()
    if err:
        return err
    if not lib.is_uuid(history_id):
        return jsonify({"ok": False, "error": {"code": "BAD_ID", "message": "Invalid asset id."}}), 400
    try:
        if request.method == "DELETE":
            state = lib.set_favorite(identity_id, history_id, False)
        else:
            # POST with an explicit body sets; POST without one toggles, so the
            # star works from a single optimistic click handler.
            body = _body()
            if "favorited" in body:
                state = lib.set_favorite(identity_id, history_id, bool(body["favorited"]))
            else:
                state = lib.toggle_favorite(identity_id, history_id)
        return jsonify({"ok": True, "history_id": history_id, "favorited": state})
    except LibraryError as exc:
        return _fail(exc)
    except Exception as exc:  # noqa: BLE001 — never leak a 500 HTML page
        return _unavailable(request.path, exc)


# ---------------------------------------------------------------------------
# tags
# ---------------------------------------------------------------------------
@bp.route("/library/tags", methods=["GET", "OPTIONS"])
@with_session_readonly
def library_tags_list():
    if request.method == "OPTIONS":
        return ("", 204)
    identity_id, err = _identity_or_401()
    if err:
        return err
    try:
        return jsonify({"ok": True, "tags": lib.list_tags(identity_id)})
    except Exception as exc:  # noqa: BLE001
        return _unavailable(request.path, exc)


@bp.route("/library/tags/<history_id>", methods=["POST", "DELETE", "OPTIONS"])
@with_session
def library_tag_item(history_id: str):
    if request.method == "OPTIONS":
        return ("", 204)
    identity_id, err = _identity_or_401()
    if err:
        return err
    if not lib.is_uuid(history_id):
        return jsonify({"ok": False, "error": {"code": "BAD_ID", "message": "Invalid asset id."}}), 400
    raw_tag = _body().get("tag") or request.args.get("tag")
    try:
        if request.method == "DELETE":
            removed = lib.remove_tag(identity_id, history_id, raw_tag)
            return jsonify({"ok": True, "history_id": history_id, "removed": removed})
        tag = lib.add_tag(identity_id, history_id, raw_tag)
        return jsonify({"ok": True, "history_id": history_id, "tag": tag})
    except LibraryError as exc:
        return _fail(exc)
    except Exception as exc:  # noqa: BLE001 — never leak a 500 HTML page
        return _unavailable(request.path, exc)


# ---------------------------------------------------------------------------
# collections
# ---------------------------------------------------------------------------
# Split GET and POST into separate views on purpose. Flask adds HEAD to every
# GET rule, so a single view that branches on `request.method == "GET"` sends
# HEAD requests — from monitors, proxies and link previewers — down the POST
# path, creating collections and running the write-session middleware.
@bp.route("/library/collections", methods=["GET", "OPTIONS"])
@with_session_readonly
def library_collections_list():
    if request.method == "OPTIONS":
        return ("", 204)
    identity_id, err = _identity_or_401()
    if err:
        return err
    try:
        return jsonify({"ok": True, "collections": lib.list_collections(identity_id)})
    except LibraryError as exc:
        return _fail(exc)
    except Exception as exc:  # noqa: BLE001 — never leak a 500 HTML page
        return _unavailable(request.path, exc)


@bp.route("/library/collections", methods=["POST"])
@with_session
def library_collections_create():
    identity_id, err = _identity_or_401()
    if err:
        return err
    try:
        body = _body()
        created = lib.create_collection(identity_id, body.get("name"), body.get("color"))
        return jsonify({"ok": True, "collection": created}), 201 if created.get("created") else 200
    except LibraryError as exc:
        return _fail(exc)
    except Exception as exc:  # noqa: BLE001 — never leak a 500 HTML page
        return _unavailable(request.path, exc)


@bp.route("/library/collections/<collection_id>", methods=["PATCH", "DELETE", "OPTIONS"])
@with_session
def library_collection_item(collection_id: str):
    if request.method == "OPTIONS":
        return ("", 204)
    identity_id, err = _identity_or_401()
    if err:
        return err
    try:
        if request.method == "DELETE":
            deleted = lib.delete_collection(identity_id, collection_id)
            if not deleted:
                return jsonify({"ok": False, "error": {"code": "NOT_FOUND", "message": "No such collection."}}), 404
            return jsonify({"ok": True, "deleted": collection_id})
        body = _body()
        updated = lib.rename_collection(identity_id, collection_id, body.get("name"), body.get("color"))
        return jsonify({"ok": True, "collection": updated})
    except LibraryError as exc:
        return _fail(exc)
    except Exception as exc:  # noqa: BLE001 — never leak a 500 HTML page
        return _unavailable(request.path, exc)


@bp.route("/library/collections/<collection_id>/items", methods=["POST", "DELETE", "OPTIONS"])
@with_session
def library_collection_items(collection_id: str):
    if request.method == "OPTIONS":
        return ("", 204)
    identity_id, err = _identity_or_401()
    if err:
        return err
    body = _body()
    try:
        ids = lib.clean_ids(body.get("history_ids") or body.get("history_id"))
        if not ids:
            return jsonify({"ok": False, "error": {"code": "NO_ITEMS", "message": "No assets given."}}), 400
        if request.method == "DELETE":
            changed = lib.remove_from_collection(identity_id, collection_id, ids)
        else:
            changed = lib.add_to_collection(identity_id, collection_id, ids)
        return jsonify({"ok": True, "collection_id": collection_id, "changed": changed})
    except LibraryError as exc:
        return _fail(exc)
    except Exception as exc:  # noqa: BLE001 — never leak a 500 HTML page
        return _unavailable(request.path, exc)


# ---------------------------------------------------------------------------
# bulk — one endpoint for the multi-select toolbar
# ---------------------------------------------------------------------------
@bp.route("/library/bulk", methods=["POST", "OPTIONS"])
@with_session
def library_bulk():
    if request.method == "OPTIONS":
        return ("", 204)
    identity_id, err = _identity_or_401()
    if err:
        return err
    body = _body()
    action = str(body.get("action") or "").strip().lower()
    try:
        ids = lib.clean_ids(body.get("history_ids"))
        if not ids:
            return jsonify({"ok": False, "error": {"code": "NO_ITEMS", "message": "Select at least one asset."}}), 400

        if action in ("favorite", "unfavorite"):
            changed = lib.bulk_set_favorite(identity_id, ids, action == "favorite")
        elif action == "tag":
            changed = lib.bulk_add_tag(identity_id, ids, body.get("tag"))
        elif action == "collect":
            changed = lib.add_to_collection(identity_id, body.get("collection_id") or "", ids)
        elif action == "uncollect":
            changed = lib.remove_from_collection(identity_id, body.get("collection_id") or "", ids)
        else:
            return jsonify({
                "ok": False,
                "error": {"code": "BAD_ACTION", "message": f"Unknown bulk action '{action}'."},
            }), 400

        return jsonify({"ok": True, "action": action, "changed": changed, "requested": len(ids)})
    except LibraryError as exc:
        return _fail(exc)
    except Exception as exc:  # noqa: BLE001 — never leak a 500 HTML page
        return _unavailable(request.path, exc)
