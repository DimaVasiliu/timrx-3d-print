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
from backend.services.expense_guard import ExpenseGuard
from backend.services.history_service import save_image_to_normalized_db, save_video_to_normalized_db
from backend.services.job_service import load_store, save_active_job_to_db, save_store
from backend.services.meshy_service import mesh_post
from backend.services.openai_service import openai_image_generate
from backend.services.s3_service import safe_upload_to_s3
from backend.services.gemini_video_service import (
    gemini_video_status,
    download_video_bytes,
    extract_video_thumbnail,
    GeminiAuthError,
    GeminiConfigError,
    GeminiValidationError,
)
from backend.services.gemini_image_service import (
    gemini_generate_image,
    GeminiAuthError as GeminiImageAuthError,
    GeminiConfigError as GeminiImageConfigError,
    GeminiValidationError as GeminiImageValidationError,
)
from backend.services.video_router import (
    QuotaExhaustedError,
    ProviderUnavailableError,
    video_router,
)
from backend.services.video_queue import video_queue

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
    # print(f"[ASYNC] Starting Meshy text-to-3d dispatch for job {internal_job_id}")
    # print(f"[JOB] provider_started job_id={internal_job_id} provider=meshy action=text-to-3d reservation_id={reservation_id}")

    try:
        resp = mesh_post("/openapi/v2/text-to-3d", payload)
        meshy_task_id = resp.get("result")

        duration_ms = int((time.time() - start_time) * 1000)
        # print(f"[ASYNC] Meshy returned task_id={meshy_task_id} for job {internal_job_id} in {duration_ms}ms")
        # print(f"[JOB] provider_done job_id={internal_job_id} duration_ms={duration_ms} upstream_id={meshy_task_id} status=accepted")

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

        # print(f"[ASYNC] Job {internal_job_id} dispatched successfully, meshy_task_id={meshy_task_id}")

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
    # print(f"[ASYNC] Starting Meshy refine dispatch for job {internal_job_id}")
    # print(f"[JOB] provider_started job_id={internal_job_id} provider=meshy action=refine reservation_id={reservation_id}")

    try:
        resp = mesh_post("/openapi/v2/text-to-3d", payload)
        meshy_task_id = resp.get("result")

        duration_ms = int((time.time() - start_time) * 1000)
        # print(
        #     f"[ASYNC] Meshy refine returned task_id={meshy_task_id} "
        #     f"for job {internal_job_id} in {duration_ms}ms"
        # )
        # print(
        #     f"[JOB] provider_done job_id={internal_job_id} duration_ms={duration_ms} "
        #     f"upstream_id={meshy_task_id} status=accepted"
        # )

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

        # print(
        #     f"[ASYNC] Refine job {internal_job_id} dispatched successfully, "
        #     f"meshy_task_id={meshy_task_id}"
        # )

    except Exception as e:
        duration_ms = int((time.time() - start_time) * 1000)
        err_text = str(e)
        print(
            f"[ASYNC] ERROR: Meshy refine failed for job {internal_job_id} "
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
    # print(f"[ASYNC] Starting Meshy image-to-3d dispatch for job {internal_job_id}")
    # print(f"[JOB] provider_started job_id={internal_job_id} provider=meshy action=image-to-3d reservation_id={reservation_id}")

    try:
        user_id = identity_id
        s3_image_url = image_url
        prompt = store_meta.get("prompt", "")
        s3_name = prompt if prompt else "image_to_3d_source"

        # If image is a data URL, upload to S3 FIRST so Meshy gets an accessible URL
        is_data_url = isinstance(image_url, str) and image_url.startswith("data:")
        if is_data_url and AWS_BUCKET_MODELS:
            try:
                s3_image_url = safe_upload_to_s3(
                    image_url,
                    "image/png",
                    "source_images",
                    s3_name,
                    user_id=user_id,
                    key_base=f"source_images/{user_id or 'public'}/{internal_job_id}",
                    provider="user",
                )
                if s3_image_url:
                    print(f"[ASYNC] Uploaded data URL image to S3 before Meshy call: {s3_image_url}")
                    payload["image_url"] = s3_image_url
                else:
                    print(f"[ASYNC] S3 upload returned no URL, sending data URL to Meshy")
            except Exception as e:
                print(f"[ASYNC] Failed to pre-upload data URL to S3: {e}, sending data URL to Meshy")

        resp = mesh_post("/openapi/v1/image-to-3d", payload)
        meshy_task_id = resp.get("result") or resp.get("id")

        duration_ms = int((time.time() - start_time) * 1000)
        # print(f"[ASYNC] Meshy image-to-3d returned task_id={meshy_task_id} for job {internal_job_id} in {duration_ms}ms")
        # print(f"[JOB] provider_done job_id={internal_job_id} duration_ms={duration_ms} upstream_id={meshy_task_id} status=accepted")

        if not meshy_task_id:
            print(f"[ASYNC] ERROR: No task_id from Meshy image-to-3d for job {internal_job_id}")
            if reservation_id:
                release_job_credits(reservation_id, "meshy_no_job_id", internal_job_id)
            update_job_status_failed(internal_job_id, "Meshy API returned no task ID")
            return

        update_job_with_upstream_id(internal_job_id, meshy_task_id)

        # Upload source image to S3 for archival (if not already uploaded above)
        if not is_data_url and AWS_BUCKET_MODELS:
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
                pass  # print(f"[ASYNC] Uploaded source image to S3: {s3_image_url}")
            except Exception as e:
                print(f"[ASYNC] WARNING: Failed to upload source image to S3: {e}")

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

        # print(f"[ASYNC] Image-to-3d job {internal_job_id} dispatched successfully, meshy_task_id={meshy_task_id}")

    except Exception as e:
        duration_ms = int((time.time() - start_time) * 1000)
        print(f"[ASYNC] ERROR: Meshy image-to-3d failed for job {internal_job_id} after {duration_ms}ms: {e}")
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
    # print(f"[ASYNC] Starting OpenAI image dispatch for job {internal_job_id}")
    # print(f"[JOB] provider_started job_id={internal_job_id} provider=openai action=image-gen reservation_id={reservation_id}")

    try:
        resp = openai_image_generate(prompt=prompt, size=size, model=model, n=n, response_format=response_format)

        duration_ms = int((time.time() - start_time) * 1000)
        # print(f"[ASYNC] OpenAI returned for job {internal_job_id} in {duration_ms}ms")
        # print(f"[JOB] provider_done job_id={internal_job_id} duration_ms={duration_ms} status=complete")

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
            provider="openai",  # Explicit: saves to images/openai/{hash}.png
        )
        # print(f"[JOB] asset_saved job_id={internal_job_id} image_id={internal_job_id} provider=openai")

        store = load_store()
        store_meta["status"] = "done"
        store_meta["image_url"] = urls[0]
        store_meta["image_urls"] = urls
        store_meta["image_base64"] = b64_first
        store[internal_job_id] = store_meta
        save_store(store)

        # Finalize credits (handles missing reservation_id as ready_unbilled bug)
        # print(f"[OPENAI_IMAGE:DEBUG] >>> async_dispatch success path: reservation_id={reservation_id}, job_id={internal_job_id}")
        finalized = finalize_job_credits(reservation_id, internal_job_id, identity_id)
        # if finalized:
        #     print(f"[ASYNC] Credits captured for OpenAI image job {internal_job_id}")
        # elif reservation_id is None:
        #     print(f"[OPENAI_IMAGE:DEBUG] !!! NO RESERVATION_ID - job marked as ready_unbilled")

        update_job_status_ready(
            internal_job_id,
            upstream_job_id=None,
            image_id=internal_job_id,
            image_url=urls[0],
        )

        # Unregister active job
        ExpenseGuard.unregister_active_job(internal_job_id)

        # print(f"[ASYNC] OpenAI image job {internal_job_id} completed successfully")

    except Exception as e:
        duration_ms = int((time.time() - start_time) * 1000)
        print(f"[ASYNC] ERROR: OpenAI failed for job {internal_job_id} after {duration_ms}ms: {e}")
        if reservation_id:
            release_job_credits(reservation_id, "openai_api_error", internal_job_id)
        update_job_status_failed(internal_job_id, str(e))
        # Unregister active job on failure
        ExpenseGuard.unregister_active_job(internal_job_id)


def dispatch_gemini_image_async(
    internal_job_id: str,
    identity_id: str,
    reservation_id: Optional[str],
    prompt: str,
    aspect_ratio: str,
    image_size: str,
    sample_count: int,
    store_meta: dict,
):
    """
    Async dispatch for Gemini/Imagen image generation.

    This runs in a background thread to allow the endpoint to return immediately
    with job_id + reservation_id, so frontend can see the held credits.
    """
    start_time = time.time()
    # print(f"[ASYNC] Starting Gemini image dispatch for job {internal_job_id}")
    # print(f"[JOB] provider_started job_id={internal_job_id} provider=google action=image-gen reservation_id={reservation_id}")

    try:
        # Call Gemini API
        result = gemini_generate_image(
            prompt=prompt,
            aspect_ratio=aspect_ratio,
            image_size=image_size,
            sample_count=sample_count,
        )

        duration_ms = int((time.time() - start_time) * 1000)
        # print(f"[ASYNC] Gemini returned for job {internal_job_id} in {duration_ms}ms")
        # print(f"[JOB] provider_done job_id={internal_job_id} duration_ms={duration_ms} status=complete")

        # Extract image URLs
        image_url = result.get("image_url")
        image_urls = result.get("image_urls", [image_url] if image_url else [])
        image_base64 = result.get("image_base64")

        if not image_url and not image_urls:
            print(f"[ASYNC] ERROR: No images from Gemini for job {internal_job_id}")
            if reservation_id:
                release_job_credits(reservation_id, "gemini_no_images", internal_job_id)
            update_job_status_failed(internal_job_id, "Gemini returned no images")
            return

        # Save to normalized DB (creates images row + history_items row)
        save_image_to_normalized_db(
            image_id=internal_job_id,
            image_url=image_url or image_urls[0],
            prompt=prompt,
            ai_model="imagen-4.0",
            size=f"{aspect_ratio}@{image_size}",
            image_urls=image_urls,
            user_id=identity_id,
            provider="google",
        )
        # print(f"[JOB] asset_saved job_id={internal_job_id} image_id={internal_job_id} provider=google")

        # Update in-memory store
        store = load_store()
        store_meta["status"] = "done"
        store_meta["image_url"] = image_url or image_urls[0]
        store_meta["image_urls"] = image_urls
        store_meta["image_base64"] = image_base64
        store[internal_job_id] = store_meta
        save_store(store)

        # Finalize credits (handles missing reservation_id as ready_unbilled bug)
        # print(f"[GEMINI_IMAGE:DEBUG] >>> async_dispatch success path: reservation_id={reservation_id}, job_id={internal_job_id}")
        finalized = finalize_job_credits(reservation_id, internal_job_id, identity_id)
        # if finalized:
        #     print(f"[ASYNC] Credits captured for Gemini image job {internal_job_id}")
        # elif reservation_id is None:
        #     print(f"[GEMINI_IMAGE:DEBUG] !!! NO RESERVATION_ID - job marked as ready_unbilled")

        # Update job status to ready
        update_job_status_ready(
            internal_job_id,
            upstream_job_id=None,
            image_id=internal_job_id,
            image_url=image_url or image_urls[0],
        )

        # Unregister active job
        ExpenseGuard.unregister_active_job(internal_job_id)

        # print(f"[ASYNC] Gemini image job {internal_job_id} completed successfully")

    except GeminiImageConfigError as e:
        duration_ms = int((time.time() - start_time) * 1000)
        print(f"[ASYNC] ERROR: Gemini config error for job {internal_job_id} after {duration_ms}ms: {e}")
        if reservation_id:
            release_job_credits(reservation_id, "gemini_config_error", internal_job_id)
        update_job_status_failed(internal_job_id, f"gemini_config_error: {e}")
        ExpenseGuard.unregister_active_job(internal_job_id)

    except GeminiImageValidationError as e:
        duration_ms = int((time.time() - start_time) * 1000)
        print(f"[ASYNC] ERROR: Gemini validation error for job {internal_job_id} after {duration_ms}ms: {e.message}")
        if reservation_id:
            release_job_credits(reservation_id, "gemini_validation_error", internal_job_id)
        update_job_status_failed(internal_job_id, f"gemini_validation_error: {e.message}")
        ExpenseGuard.unregister_active_job(internal_job_id)

    except GeminiImageAuthError as e:
        duration_ms = int((time.time() - start_time) * 1000)
        print(f"[ASYNC] ERROR: Gemini auth error for job {internal_job_id} after {duration_ms}ms: {e}")
        if reservation_id:
            release_job_credits(reservation_id, "gemini_auth_error", internal_job_id)
        update_job_status_failed(internal_job_id, f"gemini_auth_error: {e}")
        ExpenseGuard.unregister_active_job(internal_job_id)

    except Exception as e:
        duration_ms = int((time.time() - start_time) * 1000)
        print(f"[ASYNC] ERROR: Gemini call failed for job {internal_job_id} after {duration_ms}ms: {e}")
        if reservation_id:
            release_job_credits(reservation_id, "gemini_api_error", internal_job_id)
        update_job_status_failed(internal_job_id, str(e))
        ExpenseGuard.unregister_active_job(internal_job_id)


def update_job_with_upstream_id(job_id: str, upstream_job_id: str):
    """Update job with upstream provider ID. Returns True if updated, False otherwise."""
    if not USE_DB:
        return False
    if not job_id or not upstream_job_id:
        print(f"[ASYNC] ERROR: update_job_with_upstream_id called with empty id(s): job_id={job_id}, upstream={upstream_job_id}")
        return False

    try:
        with get_conn() as conn:
            with conn.cursor() as cursor:
                # Use id::text for consistent comparison (job_id is always a string)
                cursor.execute(
                    f"""
                    UPDATE {Tables.JOBS}
                    SET upstream_job_id = %s, status = 'processing', updated_at = NOW()
                    WHERE id::text = %s
                    RETURNING id
                    """,
                    (upstream_job_id, job_id),
                )
                result = cursor.fetchone()
            conn.commit()

            if result:
                # print(f"[ASYNC] Updated job {job_id} with upstream_job_id={upstream_job_id}")
                return True
            else:
                print(f"[ASYNC] WARNING: No job row to update for job_id={job_id}")
                return False
    except Exception as e:
        print(f"[ASYNC] ERROR updating job {job_id}: {e}")
        return False


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
                    WHERE id::text = %s
                    """,
                    (error_message[:500] if error_message else None, job_id),
                )
            conn.commit()
            # print(
            #     "[JOB] Marked job %s as failed: %s"
            #     % (job_id, error_message[:100] if error_message else "no message")
            # )
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
                        WHERE id::text = %s
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
                        WHERE id::text = %s
                        """,
                        (json.dumps(meta_updates), job_id),
                    )
            conn.commit()
            # print(f"[JOB] Marked job {job_id} as ready (model_id={model_id}, image_id={image_id})")
    except Exception as e:
        print(f"[JOB] ERROR marking job {job_id} as ready: {e}")

def _update_job_meta(job_id: str, meta_patch: dict):
    """Merge a dict into the jobs.meta JSONB column."""
    if not USE_DB or not meta_patch:
        return
    try:
        with get_conn() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    f"""
                    UPDATE {Tables.JOBS}
                    SET meta = COALESCE(meta, '{{}}'::jsonb) || %s::jsonb,
                        updated_at = NOW()
                    WHERE id::text = %s
                    """,
                    (json.dumps(meta_patch), job_id),
                )
            conn.commit()
    except Exception as e:
        print(f"[JOB] ERROR updating meta for {job_id}: {e}")


def _redispatch_to_runway(
    internal_job_id: str,
    identity_id: str,
    reservation_id: str | None,
    store_meta: dict,
):
    """
    Re-dispatch a filtered Gemini job to Runway.

    Reuses the same job_id and credit reservation (no double-charge).
    The original payload is reconstructed from store_meta.
    """
    runway = video_router.get_provider("runway")
    if not runway:
        print(f"[ASYNC] Runway provider not found for fallback of {internal_job_id}")
        if reservation_id:
            release_job_credits(reservation_id, "provider_filtered_third_party", internal_job_id)
        update_job_status_failed(
            internal_job_id,
            "provider_filtered_third_party: Blocked by provider safety rules (third-party content). Try removing logos/faces/copyrighted characters.",
        )
        return

    task = store_meta.get("task", "text2video")
    route_params = dict(
        aspect_ratio=store_meta.get("aspect_ratio", "16:9"),
        resolution=store_meta.get("resolution", "720p"),
        duration_seconds=store_meta.get("duration_seconds", 6),
        negative_prompt=store_meta.get("negative_prompt", ""),
        seed=store_meta.get("seed"),
    )

    try:
        if task == "image2video":
            resp = runway.start_image_to_video(
                image_data=store_meta.get("image_data", ""),
                prompt=store_meta.get("motion") or store_meta.get("prompt", ""),
                **route_params,
            )
        else:
            resp = runway.start_text_to_video(
                prompt=store_meta.get("prompt", ""),
                **route_params,
            )

        upstream_id = resp.get("task_id") or resp.get("operation_name")
        if not upstream_id:
            raise RuntimeError("Runway returned no task_id")

        # print(f"[ASYNC] Runway fallback started for {internal_job_id}: upstream_id={upstream_id}")

        update_job_with_upstream_id(internal_job_id, upstream_id)

        store = load_store()
        store_meta["upstream_id"] = upstream_id
        store_meta["operation_name"] = upstream_id
        store_meta["status"] = "processing"
        store_meta["provider"] = "runway"
        store[internal_job_id] = store_meta
        save_store(store)

        _poll_video_completion(
            internal_job_id,
            identity_id,
            reservation_id,
            upstream_id,
            "runway",
            store_meta,
        )

    except Exception as e:
        print(f"[ASYNC] Runway fallback failed for {internal_job_id}: {e}")
        # Store user-friendly error in meta
        _update_job_meta(internal_job_id, {
            "error_code": "provider_filtered_third_party",
            "user_message": "Blocked by provider safety rules (third-party content). Try removing logos/faces/copyrighted characters.",
        })
        store = load_store()
        store_meta["status"] = "failed"
        store_meta["error"] = "Blocked by provider safety rules (third-party content). Try removing logos/faces/copyrighted characters."
        store_meta["error_code"] = "provider_filtered_third_party"
        store[internal_job_id] = store_meta
        save_store(store)
        if reservation_id:
            release_job_credits(reservation_id, "provider_filtered_third_party", internal_job_id)
        update_job_status_failed(
            internal_job_id,
            "provider_filtered_third_party: Blocked by provider safety rules (third-party content). Try removing logos/faces/copyrighted characters.",
        )


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


def dispatch_gemini_video_async(
    internal_job_id: str,
    identity_id: str,
    reservation_id: Optional[str],
    payload: dict,
    store_meta: dict,
):
    """
    Dispatch video generation asynchronously via the provider router.

    Uses VideoRouter for automatic provider fallback.  On quota
    exhaustion, the job is enqueued in VideoJobQueue for later retry
    instead of failing immediately.

    Payload parameters:
    - task: "text2video" or "image2video"
    - prompt: Text prompt for video generation
    - image_data: Base64 image for image2video
    - aspect_ratio: "16:9" or "9:16"
    - resolution: "720p", "1080p", or "4k"
    - duration_seconds: 4, 6, or 8 (integer, NOT string!)
    - negative_prompt: Optional things to avoid
    - seed: Optional random seed
    """
    start_time = time.time()
    task = payload.get("task", "text2video")
    # print(f"[ASYNC] Starting video {task} dispatch for job {internal_job_id}")
    # print(f"[JOB] provider_started job_id={internal_job_id} action={task} reservation_id={reservation_id}")

    try:
        # Extract parameters (use new names, fallback to old for compatibility)
        aspect_ratio = payload.get("aspect_ratio", "16:9")
        resolution = payload.get("resolution", "720p")
        duration_seconds = payload.get("duration_seconds") or payload.get("duration_sec", 6)
        negative_prompt = payload.get("negative_prompt", "")
        seed = payload.get("seed")

        # CRITICAL: Ensure duration_seconds is an integer (Gemini API requires number, not string!)
        try:
            if isinstance(duration_seconds, str):
                duration_seconds = int(duration_seconds.replace("s", "").replace("sec", "").strip())
            else:
                duration_seconds = int(duration_seconds)
        except (ValueError, TypeError):
            duration_seconds = 6  # Safe default

        # print(f"[ASYNC] Video params: aspect_ratio={aspect_ratio}, resolution={resolution}, duration_seconds={duration_seconds}")

        # Build common params for the router
        route_params = dict(
            aspect_ratio=aspect_ratio,
            resolution=resolution,
            duration_seconds=duration_seconds,
            negative_prompt=negative_prompt,
            seed=seed,
        )

        # Route through VideoRouter (handles provider fallback)
        if task == "image2video":
            prompt = payload.get("motion") or payload.get("prompt", "")
            resp, provider_used = video_router.route_image_to_video(
                image_data=payload.get("image_data", ""),
                prompt=prompt,
                **route_params,
            )
        else:  # text2video
            prompt = payload.get("prompt", "")
            resp, provider_used = video_router.route_text_to_video(
                prompt=prompt,
                **route_params,
            )

        # Track which provider actually handled the request
        store_meta["provider"] = provider_used

        duration_ms = int((time.time() - start_time) * 1000)

        # Providers return different upstream identifiers:
        #   Gemini → {"operation_name": "..."}
        #   Runway → {"task_id": "..."}
        upstream_id = resp.get("operation_name") or resp.get("task_id")

        if upstream_id:
            # Long-running operation - need to poll for status
            # print(f"[ASYNC] Video returned upstream_id={upstream_id} for job {internal_job_id} via {provider_used} in {duration_ms}ms")
            # print(f"[JOB] provider_done job_id={internal_job_id} duration_ms={duration_ms} upstream_id={upstream_id} provider={provider_used} status=processing")

            # Update job with upstream identifier
            update_job_with_upstream_id(internal_job_id, upstream_id)

            # Store upstream id for polling
            store = load_store()
            store_meta["operation_name"] = upstream_id  # kept for backward compat
            store_meta["upstream_id"] = upstream_id
            store_meta["status"] = "processing"
            store[internal_job_id] = store_meta
            save_store(store)

            # Poll for completion using provider-aware loop
            _poll_video_completion(
                internal_job_id,
                identity_id,
                reservation_id,
                upstream_id,
                provider_used,
                store_meta,
            )
        else:
            # Immediate result (unexpected for video)
            video_url = resp.get("video_url")
            if video_url:
                _finalize_video_success(
                    internal_job_id, identity_id, reservation_id, video_url, store_meta
                )
            else:
                raise RuntimeError("video_failed: No upstream task id or video_url in response")

    except QuotaExhaustedError as e:
        # All providers quota-exhausted — enqueue for later retry
        duration_ms = int((time.time() - start_time) * 1000)
        print(f"[ASYNC] Quota exhausted for job {internal_job_id} after {duration_ms}ms, enqueueing for retry")
        video_queue.enqueue({
            "internal_job_id": internal_job_id,
            "identity_id": identity_id,
            "reservation_id": reservation_id,
            "payload": payload,
            "store_meta": store_meta,
        })

    except ProviderUnavailableError as e:
        duration_ms = int((time.time() - start_time) * 1000)
        print(f"[ASYNC] ERROR: No video providers available for job {internal_job_id}: {e}")
        if reservation_id:
            release_job_credits(reservation_id, "no_provider_available", internal_job_id)
        update_job_status_failed(internal_job_id, f"no_provider_available: {e}")
        ExpenseGuard.unregister_active_job(internal_job_id)

    except GeminiConfigError as e:
        duration_ms = int((time.time() - start_time) * 1000)
        print(f"[ASYNC] ERROR: Provider not configured for job {internal_job_id}: {e}")
        if reservation_id:
            release_job_credits(reservation_id, "provider_not_configured", internal_job_id)
        update_job_status_failed(internal_job_id, f"provider_not_configured: {e}")
        ExpenseGuard.unregister_active_job(internal_job_id)

    except GeminiValidationError as e:
        duration_ms = int((time.time() - start_time) * 1000)
        print(f"[ASYNC] ERROR: Validation error for job {internal_job_id}: {e.message}")
        if reservation_id:
            release_job_credits(reservation_id, "validation_error", internal_job_id)
        update_job_status_failed(internal_job_id, f"invalid_params: {e.message}")
        ExpenseGuard.unregister_active_job(internal_job_id)

    except GeminiAuthError as e:
        duration_ms = int((time.time() - start_time) * 1000)
        print(f"[ASYNC] ERROR: Auth failed for job {internal_job_id}: {e}")
        if reservation_id:
            release_job_credits(reservation_id, "provider_auth_failed", internal_job_id)
        update_job_status_failed(internal_job_id, f"provider_auth_failed: {e}")
        ExpenseGuard.unregister_active_job(internal_job_id)

    except RuntimeError as e:
        duration_ms = int((time.time() - start_time) * 1000)
        error_str = str(e)
        print(f"[ASYNC] ERROR: Video {task} failed for job {internal_job_id} after {duration_ms}ms: {error_str}")
        if reservation_id:
            release_job_credits(reservation_id, "video_failed", internal_job_id)
        update_job_status_failed(internal_job_id, error_str)
        ExpenseGuard.unregister_active_job(internal_job_id)

    except Exception as e:
        duration_ms = int((time.time() - start_time) * 1000)
        print(f"[ASYNC] ERROR: Unexpected error for job {internal_job_id} after {duration_ms}ms: {e}")
        if reservation_id:
            release_job_credits(reservation_id, "video_error", internal_job_id)
        update_job_status_failed(internal_job_id, f"video_failed: {e}")
        ExpenseGuard.unregister_active_job(internal_job_id)


def _poll_gemini_video_completion(
    internal_job_id: str,
    identity_id: str,
    reservation_id: Optional[str],
    operation_name: str,
    store_meta: dict,
    max_polls: int = 200,  # 10 minutes at 3s intervals (Veo can take 1-5 min)
    poll_interval: int = 3,
):
    """
    Poll Gemini Veo for video generation completion.

    This runs in the background thread and updates job status.
    Veo typically completes in 1-2 minutes for standard videos.
    """
    # print(f"[ASYNC] Starting poll for Veo operation {operation_name}")

    consecutive_errors = 0
    max_consecutive_errors = 5
    zero_progress_polls = 0  # Track polls with 0% progress

    for poll_num in range(1, max_polls + 1):
        try:
            time.sleep(poll_interval)

            status_resp = gemini_video_status(operation_name)
            status = status_resp.get("status", "unknown")

            # print(f"[ASYNC] Poll {poll_num}/{max_polls} for job {internal_job_id}: status={status}")

            # Reset error counter on successful poll
            consecutive_errors = 0

            # Update progress in store
            if status == "processing":
                progress = status_resp.get("progress", 0)

                # Track stuck operations (0% progress for > 5 polls)
                if progress == 0:
                    zero_progress_polls += 1
                    if zero_progress_polls == 5:
                        print(f"[ASYNC] WARNING: Job {internal_job_id} stuck at 0% progress after {zero_progress_polls} polls")
                    elif zero_progress_polls > 5 and zero_progress_polls % 10 == 0:
                        print(f"[ASYNC] WARNING: Job {internal_job_id} still at 0% after {zero_progress_polls} polls")
                else:
                    zero_progress_polls = 0  # Reset if progress advances
                store = load_store()
                store_meta["progress"] = progress
                store_meta["status"] = "processing"
                store[internal_job_id] = store_meta
                save_store(store)

                # Update DB job status
                if USE_DB:
                    try:
                        with get_conn() as conn:
                            with conn.cursor() as cursor:
                                cursor.execute(
                                    f"""
                                    UPDATE {Tables.JOBS}
                                    SET status = 'processing',
                                        meta = COALESCE(meta, '{{}}'::jsonb) || %s::jsonb,
                                        updated_at = NOW()
                                    WHERE id::text = %s
                                    """,
                                    (json.dumps({"progress": progress}), internal_job_id),
                                )
                            conn.commit()
                    except Exception as e:
                        print(f"[ASYNC] Error updating progress for {internal_job_id}: {e}")

            elif status == "done":
                video_url = status_resp.get("video_url")
                if video_url:
                    _finalize_video_success(
                        internal_job_id, identity_id, reservation_id, video_url, store_meta
                    )
                else:
                    # Log missing video_url in response
                    print(f"[ASYNC] ERROR: Job {internal_job_id} status=done but video_url missing from response: {status_resp}")
                    raise RuntimeError("gemini_video_failed: Operation done but no video_url")
                return

            elif status in ("failed", "error"):
                error_code = status_resp.get("error", "gemini_video_failed")
                error_msg = status_resp.get("message", "Video generation failed")
                print(f"[ASYNC] Veo video failed for job {internal_job_id}: {error_code} - {error_msg}")

                # ── Provider content filtering — attempt Runway fallback ──
                if error_code == "provider_filtered_third_party" and not store_meta.get("fallback_attempted"):
                    runway_provider = video_router.get_provider("runway")
                    runway_ok = runway_provider and runway_provider.is_configured()[0] if runway_provider else False

                    if runway_ok:
                        print(f"[ASYNC] Gemini filtered for job {internal_job_id}, attempting Runway fallback…")
                        store_meta["fallback_attempted"] = True
                        store_meta["original_provider"] = "google"
                        store_meta["filter_reason"] = error_code

                        _update_job_meta(internal_job_id, {
                            "fallback_attempted": True,
                            "original_provider": "google",
                            "filter_reason": error_code,
                            "provider": "runway",
                        })

                        _redispatch_to_runway(
                            internal_job_id, identity_id, reservation_id, store_meta,
                        )
                        return

                # Store user-friendly error details in meta + store
                if error_code == "provider_filtered_third_party":
                    _update_job_meta(internal_job_id, {
                        "error_code": error_code,
                        "user_message": error_msg,
                    })
                    store = load_store()
                    store_meta["status"] = "failed"
                    store_meta["error"] = error_msg
                    store_meta["error_code"] = error_code
                    store[internal_job_id] = store_meta
                    save_store(store)

                if reservation_id:
                    release_job_credits(reservation_id, error_code, internal_job_id)
                update_job_status_failed(internal_job_id, f"{error_code}: {error_msg}")
                ExpenseGuard.unregister_active_job(internal_job_id)
                return

        except Exception as e:
            consecutive_errors += 1
            print(f"[ASYNC] Error polling Veo for job {internal_job_id} (attempt {consecutive_errors}): {e}")

            # If too many consecutive errors, fail the job
            if consecutive_errors >= max_consecutive_errors:
                print(f"[ASYNC] Too many consecutive poll errors for job {internal_job_id}, failing")
                if reservation_id:
                    release_job_credits(reservation_id, "gemini_poll_error", internal_job_id)
                update_job_status_failed(internal_job_id, f"gemini_poll_error: {e}")
                ExpenseGuard.unregister_active_job(internal_job_id)
                return

    # Timeout - max polls reached
    timeout_seconds = max_polls * poll_interval
    print(f"[ASYNC] Timeout: Veo video job {internal_job_id} did not complete after {timeout_seconds}s")
    if reservation_id:
        release_job_credits(reservation_id, "gemini_timeout", internal_job_id)
    update_job_status_failed(internal_job_id, f"gemini_timeout: Video generation did not complete within {timeout_seconds} seconds")
    ExpenseGuard.unregister_active_job(internal_job_id)


def _poll_video_completion(
    internal_job_id: str,
    identity_id: str,
    reservation_id: Optional[str],
    upstream_id: str,
    provider_name: str,
    store_meta: dict,
    max_polls: int = 200,
    poll_interval: int = 3,
):
    """
    Provider-aware video polling.

    For 'google' (Gemini), delegates to the existing _poll_gemini_video_completion.
    For 'runway' and future providers, uses the VideoProvider.check_status()
    interface which returns a normalized {status, progress, video_url, error} dict.
    """
    if provider_name == "google":
        # Existing Gemini-specific poll (uses Gemini API directly)
        return _poll_gemini_video_completion(
            internal_job_id, identity_id, reservation_id, upstream_id, store_meta,
            max_polls=max_polls, poll_interval=poll_interval,
        )

    # ── Generic provider poll (Runway, future providers) ─────
    provider = video_router.get_provider(provider_name)
    if not provider:
        print(f"[ASYNC] ERROR: Unknown provider {provider_name} for job {internal_job_id}")
        if reservation_id:
            release_job_credits(reservation_id, "unknown_provider", internal_job_id)
        update_job_status_failed(internal_job_id, f"unknown_provider: {provider_name}")
        ExpenseGuard.unregister_active_job(internal_job_id)
        return

    # print(f"[ASYNC] Starting {provider_name} poll for upstream_id={upstream_id}")

    consecutive_errors = 0
    max_consecutive_errors = 5
    zero_progress_polls = 0  # Track polls with 0% progress

    for poll_num in range(1, max_polls + 1):
        try:
            time.sleep(poll_interval)

            status_resp = provider.check_status(upstream_id)
            status = status_resp.get("status", "unknown")

            # print(f"[ASYNC] Poll {poll_num}/{max_polls} for job {internal_job_id} ({provider_name}): status={status}")

            # Reset error counter on successful poll
            consecutive_errors = 0

            # Processing — update progress
            if status == "processing":
                progress = status_resp.get("progress", 0)

                # Track stuck operations (0% progress for > 5 polls)
                if progress == 0:
                    zero_progress_polls += 1
                    if zero_progress_polls == 5:
                        print(f"[ASYNC] WARNING: Job {internal_job_id} ({provider_name}) stuck at 0% progress after {zero_progress_polls} polls")
                    elif zero_progress_polls > 5 and zero_progress_polls % 10 == 0:
                        print(f"[ASYNC] WARNING: Job {internal_job_id} ({provider_name}) still at 0% after {zero_progress_polls} polls")
                else:
                    zero_progress_polls = 0  # Reset if progress advances
                store = load_store()
                store_meta["progress"] = progress
                store_meta["status"] = "processing"
                store[internal_job_id] = store_meta
                save_store(store)

                if USE_DB:
                    try:
                        with get_conn() as conn:
                            with conn.cursor() as cursor:
                                cursor.execute(
                                    f"""
                                    UPDATE {Tables.JOBS}
                                    SET status = 'processing',
                                        meta = COALESCE(meta, '{{}}'::jsonb) || %s::jsonb,
                                        updated_at = NOW()
                                    WHERE id::text = %s
                                    """,
                                    (json.dumps({"progress": progress}), internal_job_id),
                                )
                            conn.commit()
                    except Exception as e:
                        print(f"[ASYNC] Error updating progress for {internal_job_id}: {e}")

            elif status == "done":
                video_url = status_resp.get("video_url")
                if video_url:
                    _finalize_video_success(
                        internal_job_id, identity_id, reservation_id,
                        video_url, store_meta, provider_name=provider_name,
                    )
                else:
                    # Log missing video_url in response
                    print(f"[ASYNC] ERROR: Job {internal_job_id} ({provider_name}) status=done but video_url missing from response: {status_resp}")
                    raise RuntimeError(f"{provider_name}_video_failed: Task done but no video_url")
                return

            elif status in ("failed", "error"):
                error_code = status_resp.get("error", f"{provider_name}_video_failed")
                print(f"[ASYNC] ERROR: Job {internal_job_id} ({provider_name}) failed: {error_code} - {status_resp.get('message', 'Unknown error')}")
                error_msg = status_resp.get("message", "Video generation failed")
                is_retryable = status == "error"

                print(f"[ASYNC] {provider_name} video failed for job {internal_job_id}: {error_code} - {error_msg}")

                if is_retryable and consecutive_errors < max_consecutive_errors:
                    consecutive_errors += 1
                    continue

                if reservation_id:
                    release_job_credits(reservation_id, error_code, internal_job_id)
                update_job_status_failed(internal_job_id, f"{error_code}: {error_msg}")
                ExpenseGuard.unregister_active_job(internal_job_id)
                return

        except Exception as e:
            consecutive_errors += 1
            print(f"[ASYNC] Error polling {provider_name} for job {internal_job_id} (attempt {consecutive_errors}): {e}")

            if consecutive_errors >= max_consecutive_errors:
                print(f"[ASYNC] Too many consecutive poll errors for job {internal_job_id}, failing")
                if reservation_id:
                    release_job_credits(reservation_id, f"{provider_name}_poll_error", internal_job_id)
                update_job_status_failed(internal_job_id, f"{provider_name}_poll_error: {e}")
                ExpenseGuard.unregister_active_job(internal_job_id)
                return

    # Timeout
    timeout_seconds = max_polls * poll_interval
    print(f"[ASYNC] Timeout: {provider_name} video job {internal_job_id} did not complete after {timeout_seconds}s")
    if reservation_id:
        release_job_credits(reservation_id, f"{provider_name}_timeout", internal_job_id)
    update_job_status_failed(
        internal_job_id,
        f"{provider_name}_timeout: Video generation did not complete within {timeout_seconds} seconds",
    )
    ExpenseGuard.unregister_active_job(internal_job_id)


def _finalize_video_success(
    internal_job_id: str,
    identity_id: str,
    reservation_id: Optional[str],
    video_url: str,
    store_meta: dict,
    provider_name: str = "google",
):
    """
    Finalize a successful video generation.

    1. Download video from provider (or ephemeral URL)
    2. Upload to S3 (if configured)
    3. Update job status and store
    4. Finalize credits

    Works for any provider — uses the VideoRouter to get the right
    download method.
    """
    # print(f"[ASYNC] Video completed for job {internal_job_id} ({provider_name}): {video_url[:100]}...")

    final_video_url = video_url
    s3_video_url = None
    s3_thumbnail_url = None

    # Try to download and upload to S3 for persistence
    if AWS_BUCKET_MODELS:
        try:
            provider = video_router.get_provider(provider_name)

            # print(f"[ASYNC] Downloading video from {provider_name} for S3 upload...")
            if provider:
                video_bytes, content_type = provider.download_video(video_url)
            else:
                # Fallback: plain HTTP download (works for CDN URLs)
                video_bytes, content_type = download_video_bytes(video_url)

            # Determine file extension
            ext = ".mp4"
            if "webm" in content_type:
                ext = ".webm"

            # S3 key uses provider prefix for organization:
            #   videos/google/<identity>/<job_id>.mp4
            #   videos/runway/<identity>/<job_id>.mp4
            s3_video_url = safe_upload_to_s3(
                f"data:{content_type};base64,{__import__('base64').b64encode(video_bytes).decode('utf-8')}",
                content_type,
                "videos",
                f"{provider_name}_{internal_job_id}",
                user_id=identity_id,
                key_base=f"videos/{provider_name}/{identity_id or 'public'}/{internal_job_id}{ext}",
                provider=provider_name,
            )

            if s3_video_url:
                # print(f"[ASYNC] Uploaded video to S3: {s3_video_url}")
                final_video_url = s3_video_url

                # Extract and upload thumbnail
                try:
                    if provider:
                        thumb_bytes = provider.extract_thumbnail(video_bytes, timestamp_sec=1.0)
                    else:
                        thumb_bytes = extract_video_thumbnail(video_bytes, timestamp_sec=1.0)

                    if thumb_bytes:
                        thumb_b64 = f"data:image/jpeg;base64,{__import__('base64').b64encode(thumb_bytes).decode('utf-8')}"
                        s3_thumbnail_url = safe_upload_to_s3(
                            thumb_b64,
                            "image/jpeg",
                            "thumbnails",
                            f"{provider_name}_thumb_{internal_job_id}",
                            user_id=identity_id,
                            key_base=f"thumbnails/{identity_id or 'public'}/{internal_job_id}.jpg",
                            provider=provider_name,
                        )
                        if s3_thumbnail_url:
                            pass  # print(f"[ASYNC] Uploaded thumbnail to S3: {s3_thumbnail_url}")
                    else:
                        # Fallback: Use video URL as thumbnail (browser will show first frame)
                        print(f"[ASYNC] WARNING: Thumbnail extraction returned None for {internal_job_id}, using video URL as fallback")
                        s3_thumbnail_url = s3_video_url
                except Exception as thumb_err:
                    # Fallback: Use video URL as thumbnail when extraction fails
                    print(f"[ASYNC] WARNING: Thumbnail extraction failed for {internal_job_id}: {thumb_err}, using video URL as fallback")
                    s3_thumbnail_url = s3_video_url
            else:
                print(f"[ASYNC] S3 upload returned no URL, using original {provider_name} URL")
                # Use original video URL as thumbnail fallback
                s3_thumbnail_url = video_url

        except Exception as e:
            print(f"[ASYNC] Failed to upload video to S3: {e}, using original {provider_name} URL")
            # Continue with original URL - video is still available

    # Update store
    store = load_store()
    store_meta["status"] = "done"
    store_meta["video_url"] = final_video_url
    if s3_video_url:
        store_meta["s3_video_url"] = s3_video_url
        store_meta["provider_video_url"] = video_url  # Keep ephemeral URL for reference
    if s3_thumbnail_url:
        store_meta["thumbnail_url"] = s3_thumbnail_url
    store[internal_job_id] = store_meta
    save_store(store)

    # Finalize credits (handles missing reservation_id as ready_unbilled bug)
    finalize_job_credits(reservation_id, internal_job_id, identity_id)
    # if finalized:
    #     print(f"[ASYNC] Credits captured for {provider_name} video job {internal_job_id}")
    # elif reservation_id is None:
    #     print(f"[ASYNC] !!! NO RESERVATION_ID for video job {internal_job_id} - marked as ready_unbilled")

    # Save to normalized tables (videos + history_items)
    # This creates the video row and history_items row with video_id
    prompt = store_meta.get("prompt", "")
    duration_seconds = store_meta.get("duration_seconds")
    if duration_seconds:
        try:
            duration_seconds = int(duration_seconds)
        except (ValueError, TypeError):
            duration_seconds = None

    save_video_to_normalized_db(
        video_id=internal_job_id,
        video_url=str(final_video_url) if final_video_url else "",
        prompt=prompt,
        duration_seconds=duration_seconds,
        resolution=store_meta.get("resolution"),
        aspect_ratio=store_meta.get("aspect_ratio"),
        thumbnail_url=str(s3_thumbnail_url) if s3_thumbnail_url else None,
        user_id=identity_id,
        provider=provider_name,
        s3_video_url=str(s3_video_url) if s3_video_url else None,
    )

    # Update job status
    update_job_status_ready(
        internal_job_id,
        upstream_job_id=store_meta.get("upstream_id") or store_meta.get("operation_name"),
    )

    # Unregister active job
    ExpenseGuard.unregister_active_job(internal_job_id)

    # Also update meta with video_url in jobs table
    if USE_DB:
        try:
            meta_update = {
                "video_url": final_video_url,
                "progress": 100,
                "provider": provider_name,
            }
            if s3_video_url:
                meta_update["s3_video_url"] = s3_video_url

            with get_conn() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(
                        f"""
                        UPDATE {Tables.JOBS}
                        SET meta = COALESCE(meta, '{{}}'::jsonb) || %s::jsonb
                        WHERE id::text = %s
                        """,
                        (json.dumps(meta_update), internal_job_id),
                    )
                conn.commit()
        except Exception as e:
            print(f"[ASYNC] Error updating video_url for {internal_job_id}: {e}")

    # print(f"[ASYNC] {provider_name} video job {internal_job_id} completed successfully")


def _dispatch_gemini_video_async(internal_job_id, identity_id, reservation_id, payload, store_meta):
    """Adapter for video dispatch (monolith-compatible name)."""
    return dispatch_gemini_video_async(internal_job_id, identity_id, reservation_id, payload, store_meta)
