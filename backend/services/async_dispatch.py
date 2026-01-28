"""
Async dispatch service.

This is a real migration of the async dispatch logic out of app.py.
To stay safe during the transition, dependencies that still live in the
monolith are accessed via lazy import.
"""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor

import json
import time
from typing import Optional

from backend.config import AWS_BUCKET_MODELS
from backend.db import USE_DB, get_conn, Tables
from backend.services.credits_helper import finalize_job_credits, release_job_credits
from backend.services.history_service import save_image_to_normalized_db
from backend.services.job_service import load_store, save_active_job_to_db, save_store
from backend.services.meshy_service import mesh_post
from backend.services.openai_service import openai_image_generate
from backend.services.s3_service import safe_upload_to_s3

# Shared executor for background tasks
_background_executor = ThreadPoolExecutor(max_workers=4, thread_name_prefix="job_worker")


def get_executor() -> ThreadPoolExecutor:
    return _background_executor




def dispatch_meshy_text_to_3d_async(
    internal_job_id: str,
    identity_id: str,
    reservation_id: Optional[str],
    payload: dict,
    store_meta: dict,
):
    start_time = time.time()
    print(f"[ASYNC] Starting Meshy text-to-3d dispatch for job {internal_job_id}")
    print(f"[JOB] provider_started job_id={internal_job_id} provider=meshy action=text-to-3d reservation_id={reservation_id}")

    try:
        resp = mesh_post("/openapi/v2/text-to-3d", payload)
        meshy_task_id = resp.get("result")

        duration_ms = int((time.time() - start_time) * 1000)
        print(f"[ASYNC] Meshy returned task_id={meshy_task_id} for job {internal_job_id} in {duration_ms}ms")
        print(f"[JOB] provider_done job_id={internal_job_id} duration_ms={duration_ms} upstream_id={meshy_task_id} status=accepted")

        if not meshy_task_id:
            print(f"[ASYNC] ERROR: No task_id from Meshy for job {internal_job_id}")
            if reservation_id:
                release_job_credits(reservation_id, "meshy_no_job_id", internal_job_id)
            update_job_status_failed(internal_job_id, "Meshy API returned no task ID")
            return

        update_job_with_upstream_id(internal_job_id, meshy_task_id)

        store = load_store()
        store_meta["upstream_job_id"] = meshy_task_id
        store[meshy_task_id] = store_meta
        store[internal_job_id] = {**store_meta, "meshy_task_id": meshy_task_id}
        save_store(store)

        save_active_job_to_db(
            meshy_task_id,
            "text-to-3d",
            store_meta.get("stage", "preview"),
            store_meta,
            identity_id,
        )

        print(f"[ASYNC] Job {internal_job_id} dispatched successfully, meshy_task_id={meshy_task_id}")

    except Exception as e:
        duration_ms = int((time.time() - start_time) * 1000)
        print(f"[ASYNC] ERROR: Meshy call failed for job {internal_job_id} after {duration_ms}ms: {e}")
        if reservation_id:
            release_job_credits(reservation_id, "meshy_api_error", internal_job_id)
        update_job_status_failed(internal_job_id, str(e))


def dispatch_meshy_refine_async(
    internal_job_id: str,
    identity_id: str,
    reservation_id: Optional[str],
    payload: dict,
    store_meta: dict,
):
    start_time = time.time()
    print(f"[ASYNC] Starting Meshy refine dispatch for job {internal_job_id}")
    print(f"[JOB] provider_started job_id={internal_job_id} provider=meshy action=refine reservation_id={reservation_id}")

    try:
        resp = mesh_post("/openapi/v2/text-to-3d", payload)
        meshy_task_id = resp.get("result")

        duration_ms = int((time.time() - start_time) * 1000)
        print(
            f"[ASYNC] Meshy refine returned task_id={meshy_task_id} "
            f"for job {internal_job_id} in {duration_ms}ms"
        )
        print(
            f"[JOB] provider_done job_id={internal_job_id} duration_ms={duration_ms} "
            f"upstream_id={meshy_task_id} status=accepted"
        )

        if not meshy_task_id:
            error_msg = "Meshy refine returned no task ID"
            print(f"[ASYNC] ERROR: {error_msg} for job {internal_job_id}")
            if reservation_id:
                release_job_credits(reservation_id, "meshy_no_job_id", internal_job_id)
            update_job_status_failed(internal_job_id, error_msg)
            return

        update_job_with_upstream_id(internal_job_id, meshy_task_id)

        store = load_store()
        store_meta["upstream_job_id"] = meshy_task_id
        store[meshy_task_id] = store_meta
        store[internal_job_id] = {**store_meta, "meshy_task_id": meshy_task_id}
        save_store(store)

        save_active_job_to_db(
            meshy_task_id,
            "text-to-3d",
            store_meta.get("stage", "refine"),
            store_meta,
            identity_id,
        )

        print(
            f"[ASYNC] Refine job {internal_job_id} dispatched successfully, "
            f"meshy_task_id={meshy_task_id}"
        )

    except Exception as e:
        duration_ms = int((time.time() - start_time) * 1000)
        err_text = str(e)
        print(
            f"[ASYNC] ERROR: Meshy refine call failed for job {internal_job_id} "
            f"after {duration_ms}ms: {err_text}"
        )
        if reservation_id:
            release_job_credits(reservation_id, "meshy_api_error", internal_job_id)
        update_job_status_failed(internal_job_id, err_text)


def dispatch_meshy_image_to_3d_async(
    internal_job_id: str,
    identity_id: str,
    reservation_id: Optional[str],
    payload: dict,
    store_meta: dict,
    image_url: str,
):
    start_time = time.time()
    print(f"[ASYNC] Starting Meshy image-to-3d dispatch for job {internal_job_id}")
    print(f"[JOB] provider_started job_id={internal_job_id} provider=meshy action=image-to-3d reservation_id={reservation_id}")

    try:
        resp = mesh_post("/openapi/v1/image-to-3d", payload)
        meshy_task_id = resp.get("result") or resp.get("id")

        duration_ms = int((time.time() - start_time) * 1000)
        print(f"[ASYNC] Meshy image-to-3d returned task_id={meshy_task_id} for job {internal_job_id} in {duration_ms}ms")
        print(f"[JOB] provider_done job_id={internal_job_id} duration_ms={duration_ms} upstream_id={meshy_task_id} status=accepted")

        if not meshy_task_id:
            print(f"[ASYNC] ERROR: No task_id from Meshy image-to-3d for job {internal_job_id}")
            if reservation_id:
                release_job_credits(reservation_id, "meshy_no_job_id", internal_job_id)
            update_job_status_failed(internal_job_id, "Meshy API returned no task ID")
            return

        update_job_with_upstream_id(internal_job_id, meshy_task_id)

        user_id = identity_id
        s3_image_url = image_url
        prompt = store_meta.get("prompt", "")
        s3_name = prompt if prompt else "image_to_3d_source"

        if AWS_BUCKET_MODELS:
            try:
                s3_image_url = safe_upload_to_s3(
                    image_url,
                    "image/png",
                    "source_images",
                    s3_name,
                    user_id=user_id,
                    key_base=f"source_images/{user_id or 'public'}/{meshy_task_id}",
                    provider="user",
                )
                print(f"[ASYNC] Uploaded source image to S3: {s3_image_url}")
            except Exception as e:
                print(f"[ASYNC] Failed to upload source image to S3: {e}, using original URL")

        store = load_store()
        store_meta["upstream_job_id"] = meshy_task_id
        store_meta["image_url"] = s3_image_url
        store[meshy_task_id] = store_meta
        store[internal_job_id] = {**store_meta, "meshy_task_id": meshy_task_id}
        save_store(store)

        save_active_job_to_db(
            meshy_task_id,
            "image-to-3d",
            "image3d",
            store_meta,
            identity_id,
        )

        print(f"[ASYNC] Image-to-3d job {internal_job_id} dispatched successfully, meshy_task_id={meshy_task_id}")

    except Exception as e:
        duration_ms = int((time.time() - start_time) * 1000)
        print(f"[ASYNC] ERROR: Meshy image-to-3d call failed for job {internal_job_id} after {duration_ms}ms: {e}")
        if reservation_id:
            release_job_credits(reservation_id, "meshy_api_error", internal_job_id)
        update_job_status_failed(internal_job_id, str(e))


def dispatch_openai_image_async(
    internal_job_id: str,
    identity_id: str,
    reservation_id: Optional[str],
    prompt: str,
    size: str,
    model: str,
    n: int,
    response_format: str,
    store_meta: dict,
):
    start_time = time.time()
    print(f"[ASYNC] Starting OpenAI image dispatch for job {internal_job_id}")
    print(f"[JOB] provider_started job_id={internal_job_id} provider=openai action=image-gen reservation_id={reservation_id}")

    try:
        resp = openai_image_generate(prompt=prompt, size=size, model=model, n=n, response_format=response_format)

        duration_ms = int((time.time() - start_time) * 1000)
        print(f"[ASYNC] OpenAI returned for job {internal_job_id} in {duration_ms}ms")
        print(f"[JOB] provider_done job_id={internal_job_id} duration_ms={duration_ms} status=complete")

        data_list = resp.get("data") or []
        urls: list[str] = []
        b64_first = None
        for item in data_list:
            if not isinstance(item, dict):
                continue
            if item.get("url"):
                urls.append(item["url"])
            elif item.get("b64_json"):
                if not b64_first:
                    b64_first = item["b64_json"]
                urls.append(f"data:image/png;base64,{item['b64_json']}")

        if not urls:
            print(f"[ASYNC] ERROR: No images from OpenAI for job {internal_job_id}")
            if reservation_id:
                release_job_credits(reservation_id, "openai_no_images", internal_job_id)
            update_job_status_failed(internal_job_id, "OpenAI returned no images")
            return

        save_image_to_normalized_db(
            image_id=internal_job_id,
            image_url=urls[0],
            prompt=prompt,
            ai_model=model,
            size=size,
            image_urls=urls,
            user_id=identity_id,
        )
        print(f"[JOB] asset_saved job_id={internal_job_id} image_id={internal_job_id}")

        store = load_store()
        store_meta["status"] = "done"
        store_meta["image_url"] = urls[0]
        store_meta["image_urls"] = urls
        store_meta["image_base64"] = b64_first
        store[internal_job_id] = store_meta
        save_store(store)

        if reservation_id:
            finalize_job_credits(reservation_id, internal_job_id)
            print(f"[ASYNC] Credits captured for OpenAI image job {internal_job_id}")

        update_job_status_ready(
            internal_job_id,
            upstream_job_id=None,
            image_id=internal_job_id,
            image_url=urls[0],
        )

        print(f"[ASYNC] OpenAI image job {internal_job_id} completed successfully")

    except Exception as e:
        duration_ms = int((time.time() - start_time) * 1000)
        print(f"[ASYNC] ERROR: OpenAI call failed for job {internal_job_id} after {duration_ms}ms: {e}")
        if reservation_id:
            release_job_credits(reservation_id, "openai_api_error", internal_job_id)
        update_job_status_failed(internal_job_id, str(e))


def update_job_with_upstream_id(job_id: str, upstream_job_id: str):
    if not USE_DB:
        return
    try:
        with get_conn() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    f"""
                    UPDATE {Tables.JOBS}
                    SET upstream_job_id = %s, status = 'processing', updated_at = NOW()
                    WHERE id = %s
                    """,
                    (upstream_job_id, job_id),
                )
            conn.commit()
            print(f"[ASYNC] Updated job {job_id} with upstream_job_id={upstream_job_id}")
    except Exception as e:
        print(f"[ASYNC] ERROR updating job {job_id}: {e}")


def update_job_status_failed(job_id: str, error_message: str):
    if not USE_DB:
        return
    try:
        with get_conn() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    f"""
                    UPDATE {Tables.JOBS}
                    SET status = 'failed', error_message = %s, updated_at = NOW()
                    WHERE id = %s
                    """,
                    (error_message[:500] if error_message else None, job_id),
                )
            conn.commit()
            print(
                "[JOB] Marked job %s as failed: %s"
                % (job_id, error_message[:100] if error_message else "no message")
            )
    except Exception as e:
        print(f"[JOB] ERROR marking job {job_id} as failed: {e}")


def update_job_status_ready(
    job_id: str,
    upstream_job_id: str = None,
    model_id: str = None,
    image_id: str = None,
    glb_url: str = None,
    image_url: str = None,
    progress: int = 100,
):
    if not USE_DB:
        return
    try:
        with get_conn() as conn:
            with conn.cursor() as cursor:
                meta_updates = {"progress": progress}
                if model_id:
                    meta_updates["model_id"] = model_id
                if image_id:
                    meta_updates["image_id"] = image_id
                if glb_url:
                    meta_updates["glb_url"] = glb_url
                if image_url:
                    meta_updates["image_url"] = image_url

                if upstream_job_id:
                    cursor.execute(
                        f"""
                        UPDATE {Tables.JOBS}
                        SET status = 'ready',
                            upstream_job_id = COALESCE(upstream_job_id, %s),
                            meta = COALESCE(meta, '{{}}'::jsonb) || %s::jsonb,
                            updated_at = NOW()
                        WHERE id = %s
                        """,
                        (upstream_job_id, json.dumps(meta_updates), job_id),
                    )
                else:
                    cursor.execute(
                        f"""
                        UPDATE {Tables.JOBS}
                        SET status = 'ready',
                            meta = COALESCE(meta, '{{}}'::jsonb) || %s::jsonb,
                            updated_at = NOW()
                        WHERE id = %s
                        """,
                        (json.dumps(meta_updates), job_id),
                    )
            conn.commit()
            print(f"[JOB] Marked job {job_id} as ready (model_id={model_id}, image_id={image_id})")
    except Exception as e:
        print(f"[JOB] ERROR marking job {job_id} as ready: {e}")

# --- Monolith-compatible adapter names (Phase 4) ---


def _dispatch_meshy_text_to_3d_async(internal_job_id, identity_id, reservation_id, payload, store_meta):
    return dispatch_meshy_text_to_3d_async(internal_job_id, identity_id, reservation_id, payload, store_meta)


def _dispatch_meshy_refine_async(internal_job_id, identity_id, reservation_id, payload, store_meta):
    return dispatch_meshy_refine_async(internal_job_id, identity_id, reservation_id, payload, store_meta)


def _dispatch_meshy_image_to_3d_async(internal_job_id, identity_id, reservation_id, payload, store_meta):
    image_url = (payload or {}).get("image_url") or (store_meta or {}).get("original_image_url") or ""
    return dispatch_meshy_image_to_3d_async(internal_job_id, identity_id, reservation_id, payload, store_meta, image_url)


def _dispatch_openai_image_async(internal_job_id, identity_id, reservation_id, payload, store_meta):
    payload = payload or {}
    prompt = payload.get("prompt") or store_meta.get("prompt") or ""
    size = payload.get("size") or store_meta.get("size") or "1024x1024"
    model = payload.get("model") or store_meta.get("model") or "gpt-image-1"
    n = int(payload.get("n") or store_meta.get("n") or 1)
    response_format = payload.get("response_format") or store_meta.get("response_format") or "url"
    return dispatch_openai_image_async(
        internal_job_id, identity_id, reservation_id, prompt, size, model, n, response_format, store_meta
    )
