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
from backend.utils import derive_display_title, log_db_continue

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
            import time
            start_time = time.time()

            items = []
            db_source = False

            # Pagination parameters
            limit = request.args.get("limit", type=int, default=100)
            offset = request.args.get("offset", type=int, default=0)
            limit = min(max(1, limit), 500)  # Clamp to 1-500
            offset = max(0, offset)

            print(f"[History][mod] GET: identity_id={identity_id}, USE_DB={USE_DB}, limit={limit}, offset={offset}")

            if USE_DB:
                try:
                    with get_conn() as conn:
                        with conn.cursor(row_factory=dict_row) as cur:
                            # Single optimized query with LEFT JOINs to avoid N+1 problem
                            # This replaces the previous per-item get_canonical_model_row/get_canonical_image_row calls
                            cur.execute(
                                f"""
                                SELECT
                                    h.id, h.item_type, h.status, h.stage, h.title, h.prompt,
                                    h.thumbnail_url, h.glb_url, h.image_url, h.video_url, h.payload, h.created_at,
                                    h.model_id, h.image_id, h.video_id,
                                    -- Model data from joined models table
                                    m.id AS m_id, m.title AS m_title, m.glb_url AS m_glb_url,
                                    m.thumbnail_url AS m_thumbnail_url, m.meta AS m_meta,
                                    -- Image data from joined images table
                                    i.id AS i_id, i.title AS i_title, i.image_url AS i_image_url,
                                    i.thumbnail_url AS i_thumbnail_url,
                                    -- Video data from joined videos table
                                    v.id AS v_id, v.title AS v_title, v.video_url AS v_video_url,
                                    v.thumbnail_url AS v_thumbnail_url, v.duration_seconds AS v_duration_seconds,
                                    v.resolution AS v_resolution, v.aspect_ratio AS v_aspect_ratio
                                FROM {Tables.HISTORY_ITEMS} h
                                LEFT JOIN {Tables.MODELS} m ON (
                                    m.identity_id = %s AND (
                                        -- Direct model_id lookup
                                        (h.model_id IS NOT NULL AND h.model_id = m.id)
                                        OR
                                        -- Fallback: upstream_job_id lookup when no model_id
                                        (h.model_id IS NULL AND m.upstream_job_id = COALESCE(
                                            h.payload->>'original_job_id',
                                            h.payload->>'preview_task_id',
                                            h.payload->>'source_task_id'
                                        ) AND COALESCE(
                                            h.payload->>'original_job_id',
                                            h.payload->>'preview_task_id',
                                            h.payload->>'source_task_id'
                                        ) IS NOT NULL)
                                    )
                                )
                                LEFT JOIN {Tables.IMAGES} i ON (
                                    i.identity_id = %s AND (
                                        -- Direct image_id lookup
                                        (h.image_id IS NOT NULL AND h.image_id = i.id)
                                        OR
                                        -- Fallback: upstream_id lookup when no image_id
                                        (h.image_id IS NULL AND i.upstream_id = COALESCE(
                                            h.payload->>'original_job_id',
                                            h.payload->>'preview_task_id',
                                            h.payload->>'source_task_id'
                                        ) AND COALESCE(
                                            h.payload->>'original_job_id',
                                            h.payload->>'preview_task_id',
                                            h.payload->>'source_task_id'
                                        ) IS NOT NULL)
                                    )
                                )
                                LEFT JOIN {Tables.VIDEOS} v ON (
                                    v.identity_id = %s AND (
                                        -- Direct video_id lookup
                                        (h.video_id IS NOT NULL AND h.video_id = v.id)
                                        OR
                                        -- Fallback: upstream_id lookup when no video_id
                                        (h.video_id IS NULL AND v.upstream_id = COALESCE(
                                            h.payload->>'original_id',
                                            h.payload->>'original_job_id'
                                        ) AND COALESCE(
                                            h.payload->>'original_id',
                                            h.payload->>'original_job_id'
                                        ) IS NOT NULL)
                                    )
                                )
                                WHERE h.identity_id = %s
                                ORDER BY h.created_at DESC
                                LIMIT %s OFFSET %s;
                                """,
                                (identity_id, identity_id, identity_id, identity_id, limit, offset),
                            )
                            rows = cur.fetchall()
                    query_time = time.time() - start_time
                    print(f"[History][mod] GET: Fetched {len(rows)} items from database in {query_time:.3f}s (single JOIN query)")
                    db_source = True

                    def _scrub_meshy_urls(value):
                        if isinstance(value, dict):
                            return {k: v for k, v in value.items() if not (isinstance(v, str) and "meshy.ai" in v)}
                        if isinstance(value, list):
                            return [v for v in value if not (isinstance(v, str) and "meshy.ai" in v)]
                        return value

                    def _enrich_model_meta(meta):
                        """Extract model_urls and textured_model_urls from meta JSONB."""
                        if not meta:
                            return {}, {}
                        if isinstance(meta, str):
                            try:
                                meta = json.loads(meta)
                            except Exception:
                                return {}, {}
                        if isinstance(meta, dict):
                            return meta.get("model_urls", {}), meta.get("textured_model_urls", {})
                        return {}, {}

                    for r in rows:
                        item = r["payload"] if r["payload"] else {}
                        payload = item if isinstance(item, dict) else {}
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
                        if r.get("video_url"):
                            item["video_url"] = r["video_url"]
                        if r["created_at"]:
                            item["created_at"] = int(r["created_at"].timestamp() * 1000)

                        # Apply joined model data (from LEFT JOIN)
                        if r.get("m_id"):
                            if r.get("m_glb_url"):
                                item["glb_url"] = r["m_glb_url"]
                                payload["glb_url"] = r["m_glb_url"]
                            if r.get("m_thumbnail_url"):
                                item["thumbnail_url"] = r["m_thumbnail_url"]
                                payload["thumbnail_url"] = r["m_thumbnail_url"]
                            model_urls, textured_model_urls = _enrich_model_meta(r.get("m_meta"))
                            if model_urls:
                                item["model_urls"] = model_urls
                                payload["model_urls"] = model_urls
                            if textured_model_urls:
                                item["textured_model_urls"] = textured_model_urls
                                payload["textured_model_urls"] = textured_model_urls
                            if r.get("m_title") and not item.get("title"):
                                item["title"] = r["m_title"]

                        # Apply joined image data (from LEFT JOIN)
                        if r.get("i_id"):
                            if r.get("i_image_url"):
                                item["image_url"] = r["i_image_url"]
                                payload["image_url"] = r["i_image_url"]
                            if r.get("i_thumbnail_url"):
                                item["thumbnail_url"] = r["i_thumbnail_url"]
                                payload["thumbnail_url"] = r["i_thumbnail_url"]
                            if r.get("i_title") and not item.get("title"):
                                item["title"] = r["i_title"]

                        # Apply joined video data (from LEFT JOIN)
                        if r.get("v_id"):
                            if r.get("v_video_url"):
                                item["video_url"] = r["v_video_url"]
                                payload["video_url"] = r["v_video_url"]
                            if r.get("v_thumbnail_url"):
                                item["thumbnail_url"] = r["v_thumbnail_url"]
                                payload["thumbnail_url"] = r["v_thumbnail_url"]
                            if r.get("v_title") and not item.get("title"):
                                item["title"] = r["v_title"]
                            if r.get("v_duration_seconds"):
                                item["duration_seconds"] = r["v_duration_seconds"]
                                payload["duration_seconds"] = r["v_duration_seconds"]
                            if r.get("v_resolution"):
                                item["resolution"] = r["v_resolution"]
                                payload["resolution"] = r["v_resolution"]
                            if r.get("v_aspect_ratio"):
                                item["aspect_ratio"] = r["v_aspect_ratio"]
                                payload["aspect_ratio"] = r["v_aspect_ratio"]

                        # Scrub any remaining meshy.ai URLs (we only want S3 URLs)
                        for key in ("glb_url", "thumbnail_url", "image_url"):
                            val = item.get(key)
                            if isinstance(val, str) and "meshy.ai" in val:
                                item[key] = None
                                if isinstance(payload, dict):
                                    payload[key] = None

                        for key in ("model_urls", "textured_model_urls", "texture_urls"):
                            if key in item:
                                item[key] = _scrub_meshy_urls(item.get(key))
                            if isinstance(payload, dict) and key in payload:
                                payload[key] = _scrub_meshy_urls(payload.get(key))

                        if not item.get("title"):
                            item["title"] = derive_display_title(item.get("prompt"), None)

                        items.append(item)

                    for i, item in enumerate(items[:3]):
                        thumb = item.get("thumbnail_url")
                        thumb_preview = (thumb[:60] + "...") if isinstance(thumb, str) else "None"
                        print(f"[History][mod] Item {i}: title={item.get('title')}, thumbnail={thumb_preview}")

                    save_history_store(items)
                except Exception as e:
                    print(f"[History][mod] DB read failed (returning local/empty): {e}")
                    import traceback
                    traceback.print_exc()
                    # Fall through to return local history or empty array

            # If DB wasn't used or failed, try local history
            if not db_source:
                local_items = load_history_store()
                if isinstance(local_items, list):
                    items = local_items
                    print(f"[History][mod] GET: Returning {len(items)} items from local store")

            # Log total time including N+1 queries
            total_time = time.time() - start_time
            print(f"[History][mod] GET: Total response time {total_time:.3f}s for {len(items)} items")

            # Always return 200 with bare array for frontend compatibility
            # Frontend expects: Array.isArray(result.data) to be true
            return jsonify(items)

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
                try:
                    with get_conn() as conn:
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
                                # Note: cursor returns dict due to connection's default row_factory=dict_row
                                existing_id = existing["id"] if existing else None

                                item_type = item.get("type") or item.get("item_type") or "model"
                                status = item.get("status") or "pending"
                                stage = item.get("stage")
                                title = item.get("title")
                                prompt = item.get("prompt")
                                if not title:
                                    title = derive_display_title(prompt, None)
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
                                               title = CASE
                                                   WHEN %s::text IS NOT NULL
                                                    AND %s::text <> ''
                                                    AND %s::text NOT IN ('3D Model', 'Untitled')
                                                   THEN %s::text
                                                   ELSE title
                                               END,
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
                                            title,
                                            title,
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
                                               title = CASE
                                                   WHEN EXCLUDED.title IS NOT NULL
                                                    AND EXCLUDED.title <> ''
                                                    AND EXCLUDED.title NOT IN ('3D Model', 'Untitled')
                                                   THEN EXCLUDED.title
                                                   ELSE {Tables.HISTORY_ITEMS}.title
                                               END,
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
                        conn.commit()
                        print(
                            f"[History][mod] Bulk sync: updated={len(updated_ids)}, inserted={len(inserted_ids)}, skipped={len(skipped_items)}"
                        )
                        db_ok = True
                except Exception as e:
                    log_db_continue("history_bulk_write", e)
                    db_errors.append({"op": "history_bulk_write", "error": str(e)})
                    import traceback
                    traceback.print_exc()
                    db_ok = False

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
                try:
                    with get_conn() as conn:
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
                            # Note: cursor returns dict due to connection's default row_factory=dict_row
                            existing_id = existing["id"] if existing else None

                            item_type = item.get("type") or item.get("item_type") or "model"
                            status = item.get("status") or "pending"
                            stage = item.get("stage")
                            title = item.get("title")
                            prompt = item.get("prompt")
                            if not title:
                                title = derive_display_title(prompt, None)
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
                                           title = CASE
                                               WHEN %s::text IS NOT NULL
                                                AND %s::text <> ''
                                                AND %s::text NOT IN ('3D Model', 'Untitled')
                                               THEN %s::text
                                               ELSE title
                                           END,
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
                                        title,
                                        title,
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
                                           title = CASE
                                               WHEN EXCLUDED.title IS NOT NULL
                                                AND EXCLUDED.title <> ''
                                                AND EXCLUDED.title NOT IN ('3D Model', 'Untitled')
                                               THEN EXCLUDED.title
                                               ELSE {Tables.HISTORY_ITEMS}.title
                                           END,
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
                        conn.commit()
                except Exception as e:
                    log_db_continue("history_item_add", e)
                    db_errors.append({"op": "history_item_add", "error": str(e)})

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
                try:
                    with get_conn() as conn:
                        with conn.cursor(row_factory=dict_row) as cur:
                            cur.execute(
                                f"""
                                SELECT id, item_type, model_id, image_id, video_id, thumbnail_url, glb_url, image_url, video_url, payload
                                FROM {Tables.HISTORY_ITEMS}
                                WHERE id::text = %s AND identity_id = %s
                                LIMIT 1
                                """,
                                (str(item_id), identity_id),
                            )
                            row = cur.fetchone()
                        if not row:
                            return jsonify({"ok": False, "error": "not_found"}), 404

                        model_id = row["model_id"]
                        image_id = row["image_id"]
                        video_id = row["video_id"]
                        payload = row["payload"] if row["payload"] else {}
                        if isinstance(payload, str):
                            try:
                                payload = json.loads(payload)
                            except Exception:
                                payload = {}

                        row["payload"] = payload
                        s3_keys = collect_s3_keys(row)

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
                            if video_id:
                                cur.execute(f"DELETE FROM {Tables.VIDEOS} WHERE id = %s", (video_id,))
                        conn.commit()
                        db_ok = True

                        if s3_keys:
                            try:
                                delete_s3_objects(s3_keys)
                            except Exception as e:
                                log_db_continue("history_item_delete_s3", e)
                                db_errors.append({"op": "history_item_delete_s3", "error": str(e)})
                except Exception as e:
                    log_db_continue("history_item_delete", e)
                    db_errors.append({"op": "history_item_delete", "error": str(e)})
                    delete_history_local(item_id)
                    return jsonify({"ok": False, "error": "delete_failed"}), 500

            delete_history_local(item_id)
            return jsonify({"ok": True, "source": "modular"})

        if request.method == "PATCH":
            try:
                updates = request.get_json(silent=True) or {}
                db_ok = False
                db_errors: list[dict[str, str]] = []
                if USE_DB:
                    try:
                        with get_conn() as conn:
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
                                video_url = updates.get("video_url")

                                provider = "google" if item_type == "video" else ("openai" if item_type == "image" else "meshy")
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
                                existing.update({k: v for k, v in updates.items() if k in {"thumbnail_url", "image_url", "video_url"}})

                                title_to_set = title
                                cur.execute(
                                    f"""UPDATE {Tables.HISTORY_ITEMS}
                                       SET item_type = COALESCE(%s, item_type),
                                           status = COALESCE(%s, status),
                                           stage = COALESCE(%s, stage),
                                           title = COALESCE(%s::text, title),
                                           prompt = COALESCE(%s, prompt),
                                           thumbnail_url = COALESCE(%s, thumbnail_url),
                                           glb_url = COALESCE(%s, glb_url),
                                           image_url = COALESCE(%s, image_url),
                                           video_url = COALESCE(%s, video_url),
                                           payload = %s,
                                           updated_at = NOW()
                                       WHERE id = %s AND identity_id = %s;""",
                                    (
                                        item_type,
                                        status,
                                        stage,
                                        title_to_set,
                                        prompt,
                                        thumbnail_url,
                                        glb_url,
                                        image_url,
                                        video_url,
                                        json.dumps(existing),
                                        actual_id,
                                        identity_id,
                                    ),
                                )
                            conn.commit()
                            db_ok = True
                    except Exception as e:
                        log_db_continue("history_item_update", e)
                        db_errors.append({"op": "history_item_update", "error": str(e)})

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
