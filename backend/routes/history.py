"""
History Routes Blueprint (Modular, Real Logic)
---------------------------------------------
Registered under /api/_mod to avoid conflicts during migration.

This mirrors the monolith behavior but lives in the modular route file.
"""

from __future__ import annotations

import json
import uuid

from flask import Blueprint, jsonify, request, g

from backend.db import USE_DB, get_conn, dict_row, Tables
from backend.middleware import with_session
from backend.services.history_service import (
    _local_history_id,
    _lookup_asset_id_for_history,
    _validate_history_item_asset_ids,
    delete_history_local,
    delete_s3_objects,
    load_history_store,
    save_history_store,
    upsert_history_local,
)
from backend.services.s3_service import collect_s3_keys, ensure_s3_url_for_data_uri
from backend.utils import log_db_continue

bp = Blueprint("history", __name__)


@bp.route("/history", methods=["GET", "POST", "OPTIONS"])
def history_mod():
    @with_session
    def _inner():
        if request.method == "OPTIONS":
            return ("", 204)

        identity_id = getattr(g, "identity_id", None)
        if not identity_id:
            return jsonify({
                "ok": False,
                "error": {"code": "NO_SESSION", "message": "A valid session is required to access history."},
            }), 401

        if request.method == "GET":
            if USE_DB:
                conn = get_conn()
                if not conn:
                    return jsonify({"error": "db_unavailable"}), 503
                try:
                    with conn.cursor(row_factory=dict_row) as cur:
                        cur.execute(
                            f"""
                            SELECT id, item_type, status, stage, title, prompt,
                                   thumbnail_url, glb_url, image_url, payload, created_at
                            FROM {Tables.HISTORY_ITEMS}
                            WHERE identity_id = %s
                            ORDER BY created_at DESC;
                            """,
                            (identity_id,),
                        )
                        rows = cur.fetchall()
                    conn.close()
                    print(f"[History][mod] GET: Fetched {len(rows)} items from database")

                    items = []
                    for r in rows:
                        item = r["payload"] if r["payload"] else {}
                        item["id"] = str(r["id"])
                        item["type"] = r["item_type"]
                        item["status"] = r["status"]
                        if r["stage"]:
                            item["stage"] = r["stage"]
                        if r["title"]:
                            item["title"] = r["title"]
                        if r["prompt"]:
                            item["prompt"] = r["prompt"]
                        if r["thumbnail_url"]:
                            item["thumbnail_url"] = r["thumbnail_url"]
                        if r["glb_url"]:
                            item["glb_url"] = r["glb_url"]
                        if r["image_url"]:
                            item["image_url"] = r["image_url"]
                        if r["created_at"]:
                            item["created_at"] = int(r["created_at"].timestamp() * 1000)
                        items.append(item)

                    for i, item in enumerate(items[:3]):
                        thumb = item.get("thumbnail_url")
                        thumb_preview = (thumb[:60] + "...") if isinstance(thumb, str) else "None"
                        print(f"[History][mod] Item {i}: title={item.get('title')}, thumbnail={thumb_preview}")

                    save_history_store(items)
                    return jsonify(items)
                except Exception as e:
                    print(f"[History][mod] DB read failed: {e}")
                    try:
                        conn.close()
                    except Exception:
                        pass
                    return jsonify({"error": "db_query_failed"}), 503

            return jsonify(load_history_store())

        try:
            payload = request.get_json(silent=True) or []
            if not isinstance(payload, list):
                return jsonify({"error": "Payload must be a list"}), 400

            db_ok = None
            db_errors: list[dict[str, str]] = []
            updated_ids: list[str] = []
            inserted_ids: list[str] = []
            skipped_items: list[dict[str, str]] = []

            if USE_DB:
                conn = get_conn()
                if conn:
                    try:
                        with conn:
                            with conn.cursor() as cur:
                                for item in payload:
                                    item_id = item.get("id") or item.get("job_id")
                                    if not item_id:
                                        continue

                                    cur.execute(
                                        f"""
                                        SELECT id FROM {Tables.HISTORY_ITEMS}
                                        WHERE (id::text = %s
                                           OR payload->>'original_job_id' = %s
                                           OR payload->>'original_id' = %s
                                           OR payload->>'job_id' = %s)
                                          AND identity_id = %s
                                        LIMIT 1
                                        """,
                                        (str(item_id), str(item_id), str(item_id), str(item_id), identity_id),
                                    )
                                    existing = cur.fetchone()
                                    existing_id = existing[0] if existing else None

                                    item_type = item.get("type") or item.get("item_type") or "model"
                                    status = item.get("status") or "pending"
                                    stage = item.get("stage")
                                    title = item.get("title")
                                    prompt = item.get("prompt")
                                    root_prompt = item.get("root_prompt")
                                    thumbnail_url = item.get("thumbnail_url")
                                    glb_url = item.get("glb_url")
                                    image_url = item.get("image_url")

                                    if existing_id:
                                        use_id = str(existing_id)
                                    else:
                                        try:
                                            uuid.UUID(str(item_id))
                                            use_id = str(item_id)
                                        except (ValueError, TypeError, AttributeError):
                                            use_id = str(uuid.uuid4())
                                            item["original_id"] = item_id

                                    provider = "openai" if item_type == "image" else "meshy"
                                    s3_user_id = identity_id
                                    if thumbnail_url and isinstance(thumbnail_url, str) and thumbnail_url.startswith("data:"):
                                        thumbnail_url = ensure_s3_url_for_data_uri(
                                            thumbnail_url,
                                            "thumbnails",
                                            f"thumbnails/{s3_user_id}/{use_id}",
                                            user_id=identity_id,
                                            name="thumbnail",
                                            provider=provider,
                                        )
                                    if image_url and isinstance(image_url, str) and image_url.startswith("data:"):
                                        image_url = ensure_s3_url_for_data_uri(
                                            image_url,
                                            "images",
                                            f"images/{s3_user_id}/{use_id}",
                                            user_id=identity_id,
                                            name="image",
                                            provider=provider,
                                        )
                                    item["thumbnail_url"] = thumbnail_url
                                    item["image_url"] = image_url

                                    item["id"] = use_id

                                    if existing_id:
                                        cur.execute(
                                            f"""UPDATE {Tables.HISTORY_ITEMS}
                                               SET item_type = %s,
                                                   status = COALESCE(%s, status),
                                                   stage = COALESCE(%s, stage),
                                                   title = COALESCE(%s, title),
                                                   prompt = COALESCE(%s, prompt),
                                                   root_prompt = COALESCE(%s, root_prompt),
                                                   identity_id = COALESCE(%s, identity_id),
                                                   thumbnail_url = COALESCE(%s, thumbnail_url),
                                                   glb_url = COALESCE(%s, glb_url),
                                                   image_url = COALESCE(%s, image_url),
                                                   payload = %s,
                                                   updated_at = NOW()
                                               WHERE id = %s;""",
                                            (
                                                item_type,
                                                status,
                                                stage,
                                                title,
                                                prompt,
                                                root_prompt,
                                                identity_id,
                                                thumbnail_url,
                                                glb_url,
                                                image_url,
                                                json.dumps(item),
                                                use_id,
                                            ),
                                        )
                                        updated_ids.append(use_id)
                                    else:
                                        model_id = item.get("model_id")
                                        image_id = item.get("image_id")
                                        lookup_reason = None
                                        if not model_id and not image_id:
                                            model_id, image_id, lookup_reason = _lookup_asset_id_for_history(
                                                cur, item_type, item_id, glb_url, image_url, identity_id, provider
                                            )
                                        if not _validate_history_item_asset_ids(model_id, image_id, f"bulk_sync:{item_id}"):
                                            if lookup_reason:
                                                skip_reason = lookup_reason
                                            elif model_id and image_id:
                                                skip_reason = "xor_violation"
                                            else:
                                                skip_reason = "missing_asset_reference"
                                            print(f"[History][mod] Skipping item {item_id} - reason: {skip_reason}")
                                            skipped_items.append({"client_id": str(item_id), "reason": skip_reason})
                                            continue

                                        cur.execute(
                                            f"""INSERT INTO {Tables.HISTORY_ITEMS} (id, identity_id, item_type, status, stage, title, prompt,
                                                   root_prompt, thumbnail_url, glb_url, image_url, model_id, image_id, payload)
                                               VALUES (%s, %s, %s, %s, %s, %s, %s,
                                                   %s, %s, %s, %s, %s, %s, %s)
                                               ON CONFLICT (id) DO UPDATE
                                               SET item_type = EXCLUDED.item_type,
                                                   status = COALESCE(EXCLUDED.status, {Tables.HISTORY_ITEMS}.status),
                                                   stage = COALESCE(EXCLUDED.stage, {Tables.HISTORY_ITEMS}.stage),
                                                   title = COALESCE(EXCLUDED.title, {Tables.HISTORY_ITEMS}.title),
                                                   prompt = COALESCE(EXCLUDED.prompt, {Tables.HISTORY_ITEMS}.prompt),
                                                   root_prompt = COALESCE(EXCLUDED.root_prompt, {Tables.HISTORY_ITEMS}.root_prompt),
                                                   identity_id = COALESCE(EXCLUDED.identity_id, {Tables.HISTORY_ITEMS}.identity_id),
                                                   thumbnail_url = COALESCE(EXCLUDED.thumbnail_url, {Tables.HISTORY_ITEMS}.thumbnail_url),
                                                   glb_url = COALESCE(EXCLUDED.glb_url, {Tables.HISTORY_ITEMS}.glb_url),
                                                   image_url = COALESCE(EXCLUDED.image_url, {Tables.HISTORY_ITEMS}.image_url),
                                                   model_id = COALESCE(EXCLUDED.model_id, {Tables.HISTORY_ITEMS}.model_id),
                                                   image_id = COALESCE(EXCLUDED.image_id, {Tables.HISTORY_ITEMS}.image_id),
                                                   payload = EXCLUDED.payload,
                                                   updated_at = NOW();""",
                                            (
                                                use_id,
                                                identity_id,
                                                item_type,
                                                status,
                                                stage,
                                                title,
                                                prompt,
                                                root_prompt,
                                                thumbnail_url,
                                                glb_url,
                                                image_url,
                                                model_id,
                                                image_id,
                                                json.dumps(item),
                                            ),
                                        )
                                        inserted_ids.append(use_id)
                        conn.close()
                        print(
                            f"[History][mod] Bulk sync: updated={len(updated_ids)}, inserted={len(inserted_ids)}, skipped={len(skipped_items)}"
                        )
                        db_ok = True
                    except Exception as e:
                        log_db_continue("history_bulk_write", e)
                        db_errors.append({"op": "history_bulk_write", "error": str(e)})
                        import traceback

                        traceback.print_exc()
                        try:
                            conn.close()
                        except Exception:
                            pass
                        db_ok = False
                else:
                    db_ok = False
                    db_errors.append({"op": "history_bulk_connect", "error": "db_unavailable"})

            save_history_store(payload)
            return jsonify(
                {
                    "ok": True,
                    "count": len(payload),
                    "updated": updated_ids,
                    "inserted": inserted_ids,
                    "skipped": skipped_items if skipped_items else None,
                    "db": db_ok,
                    "db_errors": db_errors or None,
                    "source": "modular",
                }
            )
        except Exception as e:
            return jsonify({"error": str(e)}), 500

    return _inner()


@bp.route("/history/item", methods=["POST", "OPTIONS"])
def history_item_add_mod():
    @with_session
    def _inner():
        if request.method == "OPTIONS":
            return ("", 204)

        identity_id = getattr(g, "identity_id", None)
        if not identity_id:
            return jsonify(
                {
                    "ok": False,
                    "error": {"code": "NO_SESSION", "message": "A valid session is required to add history items."},
                }
            ), 401

        try:
            item = request.get_json(silent=True) or {}
            item_id = item.get("id") or item.get("job_id")
            if not item_id:
                return jsonify({"error": "Item ID required"}), 400

            item["id"] = item_id
            db_ok = False
            db_errors: list[dict[str, str]] = []

            if USE_DB:
                conn = get_conn()
                if conn:
                    try:
                        with conn:
                            with conn.cursor() as cur:
                                cur.execute(
                                    f"""
                                    SELECT id FROM {Tables.HISTORY_ITEMS}
                                    WHERE (id::text = %s
                                       OR payload->>'original_job_id' = %s
                                       OR payload->>'original_id' = %s
                                       OR payload->>'job_id' = %s)
                                      AND identity_id = %s
                                    LIMIT 1
                                    """,
                                    (str(item_id), str(item_id), str(item_id), str(item_id), identity_id),
                                )
                                existing = cur.fetchone()
                                existing_id = existing[0] if existing else None

                                item_type = item.get("type") or item.get("item_type") or "model"
                                status = item.get("status") or "pending"
                                stage = item.get("stage")
                                title = item.get("title")
                                prompt = item.get("prompt")
                                root_prompt = item.get("root_prompt")
                                thumbnail_url = item.get("thumbnail_url")
                                glb_url = item.get("glb_url")
                                image_url = item.get("image_url")

                                if existing_id:
                                    use_id = str(existing_id)
                                else:
                                    try:
                                        uuid.UUID(str(item_id))
                                        use_id = str(item_id)
                                    except (ValueError, TypeError, AttributeError):
                                        use_id = str(uuid.uuid4())
                                        item["original_id"] = item_id

                                provider = "openai" if item_type == "image" else "meshy"
                                s3_user_id = identity_id
                                if thumbnail_url and isinstance(thumbnail_url, str) and thumbnail_url.startswith("data:"):
                                    thumbnail_url = ensure_s3_url_for_data_uri(
                                        thumbnail_url,
                                        "thumbnails",
                                        f"thumbnails/{s3_user_id}/{use_id}",
                                        user_id=identity_id,
                                        name="thumbnail",
                                        provider=provider,
                                    )
                                if image_url and isinstance(image_url, str) and image_url.startswith("data:"):
                                    image_url = ensure_s3_url_for_data_uri(
                                        image_url,
                                        "images",
                                        f"images/{s3_user_id}/{use_id}",
                                        user_id=identity_id,
                                        name="image",
                                        provider=provider,
                                    )
                                item["thumbnail_url"] = thumbnail_url
                                item["image_url"] = image_url

                                item["id"] = use_id

                                if existing_id:
                                    cur.execute(
                                        f"""UPDATE {Tables.HISTORY_ITEMS}
                                           SET item_type = %s,
                                               status = COALESCE(%s, status),
                                               stage = COALESCE(%s, stage),
                                               title = COALESCE(%s, title),
                                               prompt = COALESCE(%s, prompt),
                                               root_prompt = COALESCE(%s, root_prompt),
                                               identity_id = COALESCE(%s, identity_id),
                                               thumbnail_url = COALESCE(%s, thumbnail_url),
                                               glb_url = COALESCE(%s, glb_url),
                                               image_url = COALESCE(%s, image_url),
                                               payload = %s,
                                               updated_at = NOW()
                                           WHERE id = %s;""",
                                        (
                                            item_type,
                                            status,
                                            stage,
                                            title,
                                            prompt,
                                            root_prompt,
                                            identity_id,
                                            thumbnail_url,
                                            glb_url,
                                            image_url,
                                            json.dumps(item),
                                            use_id,
                                        ),
                                    )
                                    db_ok = True
                                    item_id = use_id
                                else:
                                    model_id = item.get("model_id")
                                    image_id = item.get("image_id")
                                    lookup_reason = None
                                    if not model_id and not image_id:
                                        model_id, image_id, lookup_reason = _lookup_asset_id_for_history(
                                            cur, item_type, item_id, glb_url, image_url, identity_id, provider
                                        )
                                    if not _validate_history_item_asset_ids(model_id, image_id, f"item_add:{item_id}"):
                                        if lookup_reason:
                                            skip_reason = lookup_reason
                                        elif model_id and image_id:
                                            skip_reason = "xor_violation"
                                        else:
                                            skip_reason = "missing_asset_reference"

                                        if skip_reason == "xor_violation":
                                            print(f"[History][mod] Rejecting item {item_id} - reason: {skip_reason}")
                                            return jsonify(
                                                {
                                                    "ok": False,
                                                    "error": {
                                                        "code": "INVALID_ASSET_REFERENCE",
                                                        "message": "History item cannot have both model_id and image_id set",
                                                        "reason": skip_reason,
                                                        "client_id": str(item_id),
                                                    },
                                                }
                                            ), 400

                                        print(
                                            f"[History][mod] Skipped early insert for {item_id} - backend will create on job completion"
                                        )
                                        return jsonify(
                                            {
                                                "ok": True,
                                                "id": str(item_id),
                                                "db": False,
                                                "backend_handles": True,
                                                "message": "History will be created automatically when job completes and asset is saved",
                                                "source": "modular",
                                            }
                                        )

                                    cur.execute(
                                        f"""INSERT INTO {Tables.HISTORY_ITEMS} (id, identity_id, item_type, status, stage, title, prompt,
                                               root_prompt, thumbnail_url, glb_url, image_url, model_id, image_id, payload)
                                           VALUES (%s, %s, %s, %s, %s, %s, %s,
                                               %s, %s, %s, %s, %s, %s, %s)
                                           ON CONFLICT (id) DO UPDATE
                                           SET item_type = EXCLUDED.item_type,
                                               status = COALESCE(EXCLUDED.status, {Tables.HISTORY_ITEMS}.status),
                                               stage = COALESCE(EXCLUDED.stage, {Tables.HISTORY_ITEMS}.stage),
                                               title = COALESCE(EXCLUDED.title, {Tables.HISTORY_ITEMS}.title),
                                               prompt = COALESCE(EXCLUDED.prompt, {Tables.HISTORY_ITEMS}.prompt),
                                               root_prompt = COALESCE(EXCLUDED.root_prompt, {Tables.HISTORY_ITEMS}.root_prompt),
                                               identity_id = COALESCE(EXCLUDED.identity_id, {Tables.HISTORY_ITEMS}.identity_id),
                                               thumbnail_url = COALESCE(EXCLUDED.thumbnail_url, {Tables.HISTORY_ITEMS}.thumbnail_url),
                                               glb_url = COALESCE(EXCLUDED.glb_url, {Tables.HISTORY_ITEMS}.glb_url),
                                               image_url = COALESCE(EXCLUDED.image_url, {Tables.HISTORY_ITEMS}.image_url),
                                               model_id = COALESCE(EXCLUDED.model_id, {Tables.HISTORY_ITEMS}.model_id),
                                               image_id = COALESCE(EXCLUDED.image_id, {Tables.HISTORY_ITEMS}.image_id),
                                               payload = EXCLUDED.payload,
                                               updated_at = NOW();""",
                                        (
                                            use_id,
                                            identity_id,
                                            item_type,
                                            status,
                                            stage,
                                            title,
                                            prompt,
                                            root_prompt,
                                            thumbnail_url,
                                            glb_url,
                                            image_url,
                                            model_id,
                                            image_id,
                                            json.dumps(item),
                                        ),
                                    )
                                    db_ok = True
                                    item_id = use_id
                        conn.close()
                    except Exception as e:
                        log_db_continue("history_item_add", e)
                        db_errors.append({"op": "history_item_add", "error": str(e)})
                        try:
                            conn.close()
                        except Exception:
                            pass
                else:
                    db_errors.append({"op": "history_item_add_connect", "error": "db_unavailable"})

            local_ok = upsert_history_local(item, merge=False)
            return jsonify(
                {
                    "ok": db_ok or False,
                    "id": item_id,
                    "db": db_ok,
                    "db_errors": db_errors or None,
                    "local": local_ok,
                    "source": "modular",
                }
            )
        except Exception as e:
            return jsonify({"error": str(e)}), 500

    return _inner()


@bp.route("/history/item/<item_id>", methods=["PATCH", "DELETE", "OPTIONS"])
def history_item_update_mod(item_id: str):
    @with_session
    def _inner(item_id: str):
        if request.method == "OPTIONS":
            return ("", 204)

        identity_id = getattr(g, "identity_id", None)
        if not identity_id:
            return jsonify(
                {
                    "ok": False,
                    "error": {"code": "NO_SESSION", "message": "A valid session is required to modify history items."},
                }
            ), 401

        try:
            uuid.UUID(str(item_id))
        except (ValueError, TypeError):
            pass

        if request.method == "DELETE":
            db_ok = False
            db_errors: list[dict[str, str]] = []
            if USE_DB:
                conn = get_conn()
                if not conn:
                    db_errors.append({"op": "history_item_delete_connect", "error": "db_unavailable"})
                    delete_history_local(item_id)
                    return jsonify({"ok": False, "error": "db_unavailable"}), 503
                try:
                    with conn.cursor(row_factory=dict_row) as cur:
                        cur.execute(
                            f"""
                            SELECT id, item_type, model_id, image_id, thumbnail_url, glb_url, image_url, payload
                            FROM {Tables.HISTORY_ITEMS}
                            WHERE id::text = %s AND identity_id = %s
                            LIMIT 1
                            """,
                            (str(item_id), identity_id),
                        )
                        row = cur.fetchone()
                    if not row:
                        conn.close()
                        return jsonify({"ok": False, "error": "not_found"}), 404

                    model_id = row["model_id"]
                    image_id = row["image_id"]
                    payload = row["payload"] if row["payload"] else {}
                    if isinstance(payload, str):
                        try:
                            payload = json.loads(payload)
                        except Exception:
                            payload = {}

                    row["payload"] = payload
                    s3_keys = collect_s3_keys(row)

                    try:
                        with conn:
                            with conn.cursor() as cur:
                                cur.execute(
                                    f"""
                                    DELETE FROM {Tables.HISTORY_ITEMS}
                                    WHERE id::text = %s AND identity_id = %s
                                    """,
                                    (str(item_id), identity_id),
                                )

                                if model_id:
                                    cur.execute(f"DELETE FROM {Tables.MODELS} WHERE id = %s", (model_id,))
                                if image_id:
                                    cur.execute(f"DELETE FROM {Tables.IMAGES} WHERE id = %s", (image_id,))
                        db_ok = True
                    except Exception as e:
                        log_db_continue("history_item_delete_db", e)
                        db_errors.append({"op": "history_item_delete_db", "error": str(e)})
                        conn.close()
                        delete_history_local(item_id)
                        return jsonify({"ok": False, "error": "db_delete_failed"}), 500

                    if s3_keys:
                        try:
                            delete_s3_objects(s3_keys)
                        except Exception as e:
                            log_db_continue("history_item_delete_s3", e)
                            db_errors.append({"op": "history_item_delete_s3", "error": str(e)})
                            conn.close()
                            delete_history_local(item_id)
                            return jsonify({"ok": False, "error": "s3_delete_failed"}), 500

                    conn.close()
                except Exception as e:
                    log_db_continue("history_item_delete_lookup", e)
                    db_errors.append({"op": "history_item_delete_lookup", "error": str(e)})
                    try:
                        conn.close()
                    except Exception:
                        pass
                    delete_history_local(item_id)
                    return jsonify({"ok": False, "error": "lookup_failed"}), 500

            delete_history_local(item_id)
            return jsonify({"ok": True, "source": "modular"})

        if request.method == "PATCH":
            try:
                updates = request.get_json(silent=True) or {}
                db_ok = False
                db_errors: list[dict[str, str]] = []
                if USE_DB:
                    conn = get_conn()
                    if conn:
                        try:
                            with conn:
                                with conn.cursor(row_factory=dict_row) as cur:
                                    cur.execute(
                                        f"""SELECT id, payload FROM {Tables.HISTORY_ITEMS}
                                               WHERE (id::text = %s
                                                  OR payload->>'original_id' = %s
                                                  OR payload->>'job_id' = %s)
                                                 AND identity_id = %s
                                               LIMIT 1;""",
                                        (str(item_id), str(item_id), str(item_id), identity_id),
                                    )
                                    row = cur.fetchone()
                                    if not row:
                                        return jsonify({"error": "Item not found or access denied"}), 404

                                    existing = row["payload"] if row["payload"] else {}
                                    existing.update(updates)
                                    actual_id = row["id"]

                                    item_type = updates.get("type") or updates.get("item_type")
                                    status = updates.get("status")
                                    stage = updates.get("stage")
                                    title = updates.get("title")
                                    if isinstance(title, str):
                                        title_norm = title.strip().lower()
                                        if title_norm in ("", "untitled", "(untitled)"):
                                            title = None
                                    prompt = updates.get("prompt")
                                    thumbnail_url = updates.get("thumbnail_url")
                                    glb_url = updates.get("glb_url")
                                    image_url = updates.get("image_url")

                                    provider = "openai" if item_type == "image" else "meshy"
                                    s3_user_id = identity_id
                                    if thumbnail_url and isinstance(thumbnail_url, str) and thumbnail_url.startswith("data:"):
                                        thumbnail_url = ensure_s3_url_for_data_uri(
                                            thumbnail_url,
                                            "thumbnails",
                                            f"thumbnails/{s3_user_id}/{actual_id}",
                                            user_id=identity_id,
                                            name="thumbnail",
                                            provider=provider,
                                        )
                                        updates["thumbnail_url"] = thumbnail_url
                                    if image_url and isinstance(image_url, str) and image_url.startswith("data:"):
                                        image_url = ensure_s3_url_for_data_uri(
                                            image_url,
                                            "images",
                                            f"images/{s3_user_id}/{actual_id}",
                                            user_id=identity_id,
                                            name="image",
                                            provider=provider,
                                        )
                                        updates["image_url"] = image_url
                                    existing.update({k: v for k, v in updates.items() if k in {"thumbnail_url", "image_url"}})

                                    cur.execute(
                                        f"""UPDATE {Tables.HISTORY_ITEMS}
                                           SET item_type = COALESCE(%s, item_type),
                                               status = COALESCE(%s, status),
                                               stage = COALESCE(%s, stage),
                                               title = COALESCE(%s, title),
                                               prompt = COALESCE(%s, prompt),
                                               thumbnail_url = COALESCE(%s, thumbnail_url),
                                               glb_url = COALESCE(%s, glb_url),
                                               image_url = COALESCE(%s, image_url),
                                               payload = %s,
                                               updated_at = NOW()
                                           WHERE id = %s AND identity_id = %s;""",
                                        (
                                            item_type,
                                            status,
                                            stage,
                                            title,
                                            prompt,
                                            thumbnail_url,
                                            glb_url,
                                            image_url,
                                            json.dumps(existing),
                                            actual_id,
                                            identity_id,
                                        ),
                                    )
                            conn.close()
                            db_ok = True
                        except Exception as e:
                            log_db_continue("history_item_update", e)
                            db_errors.append({"op": "history_item_update", "error": str(e)})
                            try:
                                conn.close()
                            except Exception:
                                pass
                    else:
                        db_errors.append({"op": "history_item_update_connect", "error": "db_unavailable"})

                existing_local = None
                arr = load_history_store()
                if isinstance(arr, list):
                    for entry in arr:
                        if isinstance(entry, dict) and _local_history_id(entry) == item_id:
                            existing_local = entry
                            break
                merged_local = {**(existing_local or {}), **updates, "id": item_id}
                local_ok = upsert_history_local(merged_local, merge=True)

                return jsonify(
                    {
                        "ok": True,
                        "id": item_id,
                        "db": db_ok,
                        "db_errors": db_errors or None,
                        "local": local_ok,
                        "source": "modular",
                    }
                )
            except Exception as e:
                return jsonify({"error": str(e)}), 500

        return jsonify({"error": "Method not allowed"}), 405

    return _inner(item_id)
