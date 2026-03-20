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
from backend.services.discord_service import send_to_discord
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
# Video router imports moved to lazy-load inside video functions to avoid
# image pipeline depending on video dependencies at import time

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
            update_job_status_failed(internal_job_id, "3D model generation failed. Please try again.")
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
        print(f"[PROVIDER_ERROR] provider=meshy job_id={internal_job_id} duration_ms={duration_ms} error={e}")
        if reservation_id:
            release_job_credits(reservation_id, "meshy_api_error", internal_job_id)
        from backend.services.error_sanitizer import sanitize_job_error_message
        update_job_status_failed(internal_job_id, sanitize_job_error_message(str(e)) or "Generation failed. Please try again.")


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
        # Meshy has eventual consistency: a just-completed preview may not be
        # found for refine for a few seconds.  Retry once after a short delay.
        resp = None
        last_err = None
        for attempt in range(2):
            try:
                resp = mesh_post("/openapi/v2/text-to-3d", payload)
                break
            except Exception as e:
                last_err = e
                err_lower = str(e).lower()
                is_not_found = "preview task not found" in err_lower or "task not found" in err_lower
                if is_not_found and attempt == 0:
                    print(f"[ASYNC] Meshy refine: preview not found, retrying in 3s (job={internal_job_id})")
                    time.sleep(3)
                    continue
                raise

        if resp is None:
            raise last_err or RuntimeError("Refine dispatch failed")

        meshy_task_id = resp.get("result")

        duration_ms = int((time.time() - start_time) * 1000)

        if not meshy_task_id:
            print(f"[PROVIDER_ERROR] provider=meshy job_id={internal_job_id} error=refine_returned_no_task_id")
            if reservation_id:
                release_job_credits(reservation_id, "meshy_no_job_id", internal_job_id)
            update_job_status_failed(internal_job_id, "3D model generation failed. Please try again.")
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
            f"[PROVIDER_ERROR] provider=meshy job_id={internal_job_id} "
            f"action=refine duration_ms={duration_ms} error={err_text}"
        )
        if reservation_id:
            release_job_credits(reservation_id, "meshy_api_error", internal_job_id)
        from backend.services.error_sanitizer import sanitize_job_error_message
        update_job_status_failed(internal_job_id, sanitize_job_error_message(err_text) or "Generation failed. Please try again.")


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
            update_job_status_failed(internal_job_id, "3D model generation failed. Please try again.")
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
        print(f"[PROVIDER_ERROR] provider=meshy job_id={internal_job_id} action=image-to-3d duration_ms={duration_ms} error={e}")
        if reservation_id:
            release_job_credits(reservation_id, "meshy_api_error", internal_job_id)
        from backend.services.error_sanitizer import sanitize_job_error_message
        update_job_status_failed(internal_job_id, sanitize_job_error_message(str(e)) or "Generation failed. Please try again.")


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
            print(f"[PROVIDER_ERROR] provider=openai job_id={internal_job_id} error=no_images_returned")
            if reservation_id:
                release_job_credits(reservation_id, "openai_no_images", internal_job_id)
            update_job_status_failed(internal_job_id, "Image generation failed. Please try again.")
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

        # Post to Discord
        send_to_discord("🖼️ New AI Image Generated", prompt, urls[0], identity_id)

        # Unregister active job
        ExpenseGuard.unregister_active_job(internal_job_id)

        # print(f"[ASYNC] OpenAI image job {internal_job_id} completed successfully")

    except Exception as e:
        duration_ms = int((time.time() - start_time) * 1000)
        print(f"[PROVIDER_ERROR] provider=openai job_id={internal_job_id} duration_ms={duration_ms} error={e}")
        if reservation_id:
            release_job_credits(reservation_id, "openai_api_error", internal_job_id)
        from backend.services.error_sanitizer import sanitize_job_error_message
        update_job_status_failed(internal_job_id, sanitize_job_error_message(str(e)) or "Image generation failed. Please try again.")
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
            print(f"[PROVIDER_ERROR] provider=gemini job_id={internal_job_id} error=no_images_returned")
            if reservation_id:
                release_job_credits(reservation_id, "gemini_no_images", internal_job_id)
            update_job_status_failed(internal_job_id, "Image generation failed. Please try again.")
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

        # Post to Discord
        send_to_discord("🖼️ New AI Image Generated", prompt, image_url or image_urls[0], identity_id)

        # Unregister active job
        ExpenseGuard.unregister_active_job(internal_job_id)

        # print(f"[ASYNC] Gemini image job {internal_job_id} completed successfully")

    except GeminiImageConfigError as e:
        duration_ms = int((time.time() - start_time) * 1000)
        print(f"[PROVIDER_ERROR] provider=gemini job_id={internal_job_id} duration_ms={duration_ms} type=config error={e}")
        if reservation_id:
            release_job_credits(reservation_id, "gemini_config_error", internal_job_id)
        update_job_status_failed(internal_job_id, "Image generation temporarily unavailable. Please try again shortly.")
        ExpenseGuard.unregister_active_job(internal_job_id)

    except GeminiImageValidationError as e:
        duration_ms = int((time.time() - start_time) * 1000)
        print(f"[PROVIDER_ERROR] provider=gemini job_id={internal_job_id} duration_ms={duration_ms} type=validation error={e.message}")
        if reservation_id:
            release_job_credits(reservation_id, "gemini_validation_error", internal_job_id)
        update_job_status_failed(internal_job_id, "Image generation failed. Please try again.")
        ExpenseGuard.unregister_active_job(internal_job_id)

    except GeminiImageAuthError as e:
        duration_ms = int((time.time() - start_time) * 1000)
        print(f"[PROVIDER_ERROR] provider=gemini job_id={internal_job_id} duration_ms={duration_ms} type=auth error={e}")
        if reservation_id:
            release_job_credits(reservation_id, "gemini_auth_error", internal_job_id)
        update_job_status_failed(internal_job_id, "Image generation temporarily unavailable. Please try again shortly.")
        ExpenseGuard.unregister_active_job(internal_job_id)

    except Exception as e:
        duration_ms = int((time.time() - start_time) * 1000)
        print(f"[PROVIDER_ERROR] provider=gemini job_id={internal_job_id} duration_ms={duration_ms} error={e}")
        if reservation_id:
            release_job_credits(reservation_id, "gemini_api_error", internal_job_id)
        from backend.services.error_sanitizer import sanitize_job_error_message
        update_job_status_failed(internal_job_id, sanitize_job_error_message(str(e)) or "Image generation failed. Please try again.")
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

        # Stamp estimated provider cost (observability only)
        try:
            from backend.services.provider_costs import stamp_estimated_cost
            stamp_estimated_cost(job_id)
        except Exception:
            pass  # Cost stamping is best-effort

    except Exception as e:
        print(f"[JOB] ERROR marking job {job_id} as ready: {e}")

def _mark_job_for_worker(job_id: str, upstream_id: str, provider_name: str, store_meta: dict):
    """
    Transition a dispatched video job so the durable worker picks it up.

    Sets status='dispatched', stores upstream_id, and schedules next_poll_at
    so the worker claims and polls it. This replaces in-thread polling.
    """
    if not USE_DB:
        return
    try:
        meta_patch = {
            "upstream_id": upstream_id,
            "provider": provider_name,
            "dispatched_via": "async_dispatch",
        }
        # Persist fal metadata so reclaimed jobs can still poll correctly
        for fal_key in ("fal_model_id", "fal_status_url", "fal_response_url", "fal_cancel_url"):
            if store_meta.get(fal_key):
                meta_patch[fal_key] = store_meta[fal_key]
        with get_conn() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    f"""
                    UPDATE {Tables.JOBS}
                    SET status = 'dispatched',
                        upstream_job_id = %s,
                        last_provider_status = 'pending',
                        next_poll_at = NOW() + INTERVAL '5 seconds',
                        heartbeat_at = NULL,
                        claimed_by = NULL,
                        stage = COALESCE(stage, 'video'),
                        meta = COALESCE(meta, '{{}}'::jsonb) || %s::jsonb,
                        updated_at = NOW()
                    WHERE id::text = %s
                    """,
                    (upstream_id, json.dumps(meta_patch), job_id),
                )
            conn.commit()
        # Seedance: poll-first (PiAPI strips webhook_config). Vertex: poll-only.
        completion_note = "poll-first; webhook best-effort only" if provider_name == "seedance" else "poll-only"
        print(f"[ASYNC] Job {job_id} queued for durable worker (upstream={upstream_id}, provider={provider_name}, completion={completion_note})")

        # Update the early-created videos row with the provider's upstream_id
        video_uuid = store_meta.get("video_uuid")
        if video_uuid:
            from backend.services.history_service import update_video_record
            update_video_record(video_uuid, upstream_id=upstream_id, status="processing")
            print(f"[ASYNC] videos row updated: video_uuid={video_uuid} upstream_id={upstream_id} status=processing")
    except Exception as e:
        print(f"[ASYNC] ERROR marking job {job_id} for worker: {e}")


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


# --- Monolith-compatible adapter names (Phase 4) ---


def _dispatch_meshy_text_to_3d_async(internal_job_id, identity_id, reservation_id, payload, store_meta):
    return dispatch_meshy_text_to_3d_async(internal_job_id, identity_id, reservation_id, payload, store_meta)


def _dispatch_meshy_refine_async(internal_job_id, identity_id, reservation_id, payload, store_meta):
    return dispatch_meshy_refine_async(internal_job_id, identity_id, reservation_id, payload, store_meta)


def _dispatch_meshy_image_to_3d_async(internal_job_id, identity_id, reservation_id, payload, store_meta):
    image_url = (payload or {}).get("image_url") or (store_meta or {}).get("original_image_url") or ""
    return dispatch_meshy_image_to_3d_async(internal_job_id, identity_id, reservation_id, payload, store_meta, image_url)


def dispatch_meshy_multi_image_to_3d_async(
    internal_job_id: str,
    identity_id: str,
    reservation_id: Optional[str],
    payload: dict,
    store_meta: dict,
    image_urls: list,
):
    """Dispatch a multi-image-to-3D job to Meshy (POST /openapi/v1/multi-image-to-3d)."""
    start_time = time.time()

    try:
        user_id = identity_id
        prompt = store_meta.get("prompt", "")

        # Upload any data-URL images to S3 first so Meshy gets accessible URLs
        resolved_urls = []
        for idx, url in enumerate(image_urls):
            is_data_url = isinstance(url, str) and url.startswith("data:")
            if is_data_url and AWS_BUCKET_MODELS:
                try:
                    s3_name = prompt if prompt else f"multi_image_to_3d_source_{idx}"
                    s3_url = safe_upload_to_s3(
                        url,
                        "image/png",
                        "source_images",
                        s3_name,
                        user_id=user_id,
                        key_base=f"source_images/{user_id or 'public'}/{internal_job_id}_{idx}",
                        provider="user",
                    )
                    if s3_url:
                        resolved_urls.append(s3_url)
                        continue
                except Exception as e:
                    print(f"[ASYNC] Failed to pre-upload data URL {idx} to S3: {e}")
            resolved_urls.append(url)

        payload["image_urls"] = resolved_urls

        resp = mesh_post("/openapi/v1/multi-image-to-3d", payload)
        meshy_task_id = resp.get("result") or resp.get("id")

        duration_ms = int((time.time() - start_time) * 1000)

        if not meshy_task_id:
            print(f"[ASYNC] ERROR: No task_id from Meshy multi-image-to-3d for job {internal_job_id}")
            if reservation_id:
                release_job_credits(reservation_id, "meshy_no_job_id", internal_job_id)
            update_job_status_failed(internal_job_id, "3D model generation failed. Please try again.")
            return

        update_job_with_upstream_id(internal_job_id, meshy_task_id)

        # Upload non-data-URL source images to S3 for archival
        if AWS_BUCKET_MODELS:
            for idx, url in enumerate(resolved_urls):
                if not (isinstance(url, str) and url.startswith("data:")):
                    try:
                        s3_name = prompt if prompt else f"multi_image_to_3d_source_{idx}"
                        safe_upload_to_s3(
                            url,
                            "image/png",
                            "source_images",
                            s3_name,
                            user_id=user_id,
                            key_base=f"source_images/{user_id or 'public'}/{meshy_task_id}_{idx}",
                            provider="user",
                        )
                    except Exception as e:
                        print(f"[ASYNC] WARNING: Failed to upload source image {idx} to S3: {e}")

        store = load_store()
        store_meta["upstream_job_id"] = meshy_task_id
        store[meshy_task_id] = store_meta
        store[internal_job_id] = {**store_meta, "meshy_task_id": meshy_task_id}
        save_store(store)

        save_active_job_to_db(
            meshy_task_id,
            "multi-image-to-3d",
            "image3d",
            store_meta,
            identity_id,
        )

    except Exception as e:
        duration_ms = int((time.time() - start_time) * 1000)
        print(f"[PROVIDER_ERROR] provider=meshy job_id={internal_job_id} action=multi-image-to-3d duration_ms={duration_ms} error={e}")
        if reservation_id:
            release_job_credits(reservation_id, "meshy_api_error", internal_job_id)
        from backend.services.error_sanitizer import sanitize_job_error_message
        update_job_status_failed(internal_job_id, sanitize_job_error_message(str(e)) or "Generation failed. Please try again.")


def _dispatch_meshy_multi_image_to_3d_async(internal_job_id, identity_id, reservation_id, payload, store_meta):
    image_urls = (payload or {}).get("image_urls") or (store_meta or {}).get("original_image_urls") or []
    return dispatch_meshy_multi_image_to_3d_async(internal_job_id, identity_id, reservation_id, payload, store_meta, image_urls)


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


def _fail_video_record(store_meta: dict, error_message: str):
    """Update the early-created videos row to status='failed' and write history on dispatch errors."""
    video_uuid = store_meta.get("video_uuid")
    if not video_uuid:
        return
    try:
        from backend.services.history_service import update_video_record
        update_video_record(video_uuid, status="failed", error_message=error_message[:500])
        print(f"[ASYNC] videos row updated: video_uuid={video_uuid} status=failed")
    except Exception as e:
        print(f"[ASYNC] WARNING: failed to update videos row {video_uuid}: {e}")

    # Write history_items row so failed video appears in user history
    try:
        identity_id = store_meta.get("identity_id") or store_meta.get("user_id", "")
        job_id = store_meta.get("internal_job_id", "")
        if identity_id and job_id:
            from backend.services.history_service import save_failed_video_to_history
            save_failed_video_to_history(
                job_id=job_id,
                identity_id=identity_id,
                video_uuid=video_uuid,
                prompt=store_meta.get("prompt", ""),
                error_message=error_message[:500],
                provider=store_meta.get("provider", "unknown"),
                duration_seconds=store_meta.get("duration_seconds"),
                aspect_ratio=store_meta.get("aspect_ratio"),
                resolution=store_meta.get("resolution"),
            )
    except Exception as e:
        print(f"[ASYNC] WARNING: failed to write failed history for {video_uuid}: {e}")


def _safe_int_duration(raw) -> int:
    """Parse duration to int, stripping 's'/'sec' suffixes. Falls back to 6."""
    try:
        if isinstance(raw, str):
            return int(raw.replace("s", "").replace("sec", "").strip())
        return int(raw)
    except (ValueError, TypeError):
        return 6


def _dispatch_to_fal_seedance(
    internal_job_id: str,
    task: str,
    prompt: str,
    payload: dict,
    route_params: dict,
    store_meta: dict,
) -> tuple:
    """Route to fal Seedance with dispatch-time fallback to PiAPI Seedance."""
    from backend.services.video_router import (
        resolve_video_provider,
        ProviderUnavailableError,
    )
    from backend.config import config as _cfg

    fal = resolve_video_provider("fal_seedance")
    if fal:
        configured, err = fal.is_configured()
        if configured:
            try:
                if task == "image_transition":
                    # Two-image transition
                    fp = payload.get("motion") or prompt
                    resp = fal.start_image_transition(
                        start_image=payload.get("start_image", ""),
                        end_image=payload.get("end_image", ""),
                        prompt=fp,
                        **route_params,
                    )
                elif task == "image2video":
                    fp = payload.get("motion") or prompt
                    resp = fal.start_image_to_video(
                        image_data=payload.get("image_data", ""),
                        prompt=fp,
                        **route_params,
                    )
                else:
                    resp = fal.start_text_to_video(prompt=prompt, **route_params)
                return resp, "fal_seedance"
            except Exception as e:
                print(f"[ASYNC] fal_seedance failed for job {internal_job_id}: {e}")
                # Fall through to PiAPI Seedance fallback
        else:
            print(f"[ASYNC] fal_seedance not configured: {err}")

    # Dispatch-time fallback to PiAPI Seedance (if enabled).
    # PiAPI does NOT support image_transition — only fall back for text2video/image2video.
    fallback_enabled = getattr(_cfg, "FAL_SEEDANCE_FALLBACK_TO_PIAPI", True)
    if fallback_enabled and task != "image_transition":
        print(f"[ASYNC] Falling back to PiAPI Seedance for job {internal_job_id}")
        return _dispatch_to_seedance(
            internal_job_id, task, prompt, payload, route_params, store_meta,
        )

    reason = "image_transition not supported by PiAPI fallback" if task == "image_transition" else "fallback disabled"
    raise ProviderUnavailableError(f"fal Seedance not available and {reason}")


def _dispatch_to_seedance(
    internal_job_id: str,
    task: str,
    prompt: str,
    payload: dict,
    route_params: dict,
    store_meta: dict,
) -> tuple:
    """Route directly to Seedance (no Vertex fallback)."""
    from backend.services.video_router import (
        resolve_video_provider,
        ProviderUnavailableError,
    )

    seedance = resolve_video_provider("seedance")
    if not seedance:
        raise ProviderUnavailableError("Seedance provider not available")
    configured, err = seedance.is_configured()
    if not configured:
        raise ProviderUnavailableError(f"Seedance not configured: {err}")

    # Pass task_type so provider sends correct model tier
    seedance_variant = (
        payload.get("seedance_variant")
        or store_meta.get("seedance_variant")
        or "seedance-2-fast-preview"
    )
    route_params["task_type"] = seedance_variant

    if task == "experimental_morph":
        prompt = payload.get("motion") or prompt
        resp = seedance.start_experimental_morph(
            start_image=payload.get("start_image", ""),
            end_image=payload.get("end_image", ""),
            prompt=prompt,
            **route_params,
        )
    elif task == "image2video":
        prompt = payload.get("motion") or prompt
        resp = seedance.start_image_to_video(
            image_data=payload.get("image_data", ""),
            prompt=prompt,
            **route_params,
        )
    else:
        resp = seedance.start_text_to_video(prompt=prompt, **route_params)

    return resp, "seedance"


def _dispatch_to_vertex_with_fallback(
    internal_job_id: str,
    task: str,
    prompt: str,
    payload: dict,
    route_params: dict,
    router,
) -> tuple:
    """Route to Vertex via the router; fall back to Seedance on failure."""
    from backend.services.video_router import (
        resolve_video_provider,
        ProviderUnavailableError,
    )

    try:
        if task == "image_transition":
            # Two-image transition via Vertex (first-frame + last-frame conditioning)
            vertex = resolve_video_provider("vertex")
            if not vertex:
                raise ProviderUnavailableError("Vertex provider not available for transition")
            fp = payload.get("motion") or prompt
            resp = vertex.start_image_transition(
                start_image=payload.get("start_image", ""),
                end_image=payload.get("end_image", ""),
                prompt=fp,
                **route_params,
            )
            return resp, "vertex"
        elif task == "image2video":
            prompt = payload.get("motion") or prompt
            resp, provider_used = router.route_image_to_video(
                image_data=payload.get("image_data", ""),
                prompt=prompt,
                **route_params,
            )
        else:
            resp, provider_used = router.route_text_to_video(
                prompt=prompt,
                **route_params,
            )
        return resp, provider_used

    except (ProviderUnavailableError, RuntimeError) as vertex_err:
        # Transition jobs must NOT silently fall back — Seedance has no transition support
        if task == "image_transition":
            print(f"[ASYNC] Vertex transition failed for job {internal_job_id}: {vertex_err} — NO fallback available")
            raise vertex_err

        print(f"[ASYNC] Vertex failed for job {internal_job_id}: {vertex_err}, attempting Seedance failover")
        failover = resolve_video_provider("seedance")
        if failover:
            configured, _ = failover.is_configured()
            if configured:
                route_params["task_type"] = "seedance-2-fast-preview"
                if task == "image2video":
                    fp = payload.get("motion") or payload.get("prompt", "")
                    resp = failover.start_image_to_video(
                        image_data=payload.get("image_data", ""), prompt=fp, **route_params,
                    )
                else:
                    resp = failover.start_text_to_video(
                        prompt=payload.get("prompt", ""), **route_params,
                    )
                print(f"[ASYNC] Seedance failover succeeded for job {internal_job_id}")
                return resp, "seedance"
        raise vertex_err


def dispatch_gemini_video_async(
    internal_job_id: str,
    identity_id: str,
    reservation_id: Optional[str],
    payload: dict,
    store_meta: dict,
):
    """
    Dispatch video generation asynchronously via the provider router.

    Expects already-normalized parameters from video.py endpoints.
    Routes to Seedance or Vertex based on store_meta["provider"].
    On quota exhaustion, enqueues for later retry.
    """
    from backend.services.video_router import (
        video_router,
        normalize_provider_name,
        resolve_video_provider,
        QuotaExhaustedError,
        ProviderUnavailableError,
    )
    from backend.services.video_queue import video_queue
    from backend.services.video_limits import (
        acquire_video_worker,
        release_video_worker,
        record_video_request,
    )

    # Global worker limit: wait for a slot
    if not acquire_video_worker():
        print(f"[ASYNC] Worker limit reached for job {internal_job_id}, queuing")
        for _wait_attempt in range(5):
            time.sleep(10)
            if acquire_video_worker():
                break
        else:
            release_job_credits(reservation_id, "worker_limit_exceeded", internal_job_id)
            ExpenseGuard.unregister_active_job(internal_job_id)
            store = load_store()
            store_meta["status"] = "failed"
            store_meta["error"] = "Server busy — please try again shortly."
            store[internal_job_id] = store_meta
            save_store(store)
            _update_job_status(internal_job_id, "failed", error_message="worker_limit_exceeded: No worker slot available")
            _fail_video_record(store_meta, "worker_limit_exceeded: No worker slot available")
            return

    start_time = time.time()
    task = payload.get("task", "text2video")

    # Record for abuse detection
    record_video_request(identity_id, payload.get("prompt", ""))

    try:
        # Extract parameters (already normalized by video.py, but ensure int duration)
        aspect_ratio = payload.get("aspect_ratio", "16:9")
        resolution = payload.get("resolution", "720p")
        duration_seconds = _safe_int_duration(payload.get("duration_seconds") or payload.get("duration_sec", 6))
        negative_prompt = payload.get("negative_prompt", "")
        seed = payload.get("seed")

        route_params = dict(
            aspect_ratio=aspect_ratio,
            resolution=resolution,
            duration_seconds=duration_seconds,
            negative_prompt=negative_prompt,
            seed=seed,
        )

        # Resolve provider (normalized by caller, but safe to re-normalize)
        requested_provider = normalize_provider_name(store_meta.get("provider"))
        prompt = payload.get("prompt", "")

        if requested_provider == "fal_seedance":
            resp, provider_used = _dispatch_to_fal_seedance(
                internal_job_id, task, prompt, payload, route_params, store_meta,
            )
        elif requested_provider == "seedance":
            resp, provider_used = _dispatch_to_seedance(
                internal_job_id, task, prompt, payload, route_params, store_meta,
            )
        else:
            resp, provider_used = _dispatch_to_vertex_with_fallback(
                internal_job_id, task, prompt, payload, route_params, video_router,
            )

        # Track which provider actually handled the request
        store_meta["provider"] = provider_used

        # Upstream identifier: Vertex → operation_name, Seedance → task_id, fal → request_id
        upstream_id = resp.get("operation_name") or resp.get("task_id") or resp.get("request_id")

        if upstream_id:
            update_job_with_upstream_id(internal_job_id, upstream_id)

            store = load_store()
            store_meta["operation_name"] = upstream_id  # backward compat
            store_meta["upstream_id"] = upstream_id
            store_meta["status"] = "processing"
            # Preserve fal metadata for status polling
            if resp.get("fal_model_id"):
                store_meta["fal_model_id"] = resp["fal_model_id"]
            if resp.get("fal_status_url"):
                store_meta["fal_status_url"] = resp["fal_status_url"]
            if resp.get("fal_response_url"):
                store_meta["fal_response_url"] = resp["fal_response_url"]
            if resp.get("fal_cancel_url"):
                store_meta["fal_cancel_url"] = resp["fal_cancel_url"]
            store[internal_job_id] = store_meta
            save_store(store)

            # Log fal metadata persistence for debugging polling issues
            if provider_used == "fal_seedance":
                fal_keys = [k for k in ("fal_model_id", "fal_status_url", "fal_response_url", "fal_cancel_url") if store_meta.get(k)]
                print(f"[FAL_SEEDANCE] meta persisted job={internal_job_id} keys={fal_keys}")

            _mark_job_for_worker(internal_job_id, upstream_id, provider_used, store_meta)
        else:
            video_url = resp.get("video_url")
            if video_url:
                _finalize_video_success(
                    internal_job_id, identity_id, reservation_id, video_url, store_meta
                )
            else:
                raise RuntimeError("video_failed: No upstream task id or video_url in response")

    except QuotaExhaustedError:
        print(f"[ASYNC] Quota exhausted for job {internal_job_id}, enqueueing for retry")
        video_queue.enqueue({
            "internal_job_id": internal_job_id,
            "identity_id": identity_id,
            "reservation_id": reservation_id,
            "payload": payload,
            "store_meta": store_meta,
        })

    except ProviderUnavailableError as e:
        print(f"[ASYNC] ERROR: No video providers available for job {internal_job_id}: {e}")
        if reservation_id:
            release_job_credits(reservation_id, "no_provider_available", internal_job_id)
        update_job_status_failed(internal_job_id, f"no_provider_available: {e}")
        _fail_video_record(store_meta, f"no_provider_available: {e}")
        ExpenseGuard.unregister_active_job(internal_job_id)

    except GeminiConfigError as e:
        duration_ms = int((time.time() - start_time) * 1000)
        print(f"[ASYNC] ERROR: Provider not configured for job {internal_job_id}: {e}")
        if reservation_id:
            release_job_credits(reservation_id, "provider_not_configured", internal_job_id)
        update_job_status_failed(internal_job_id, f"provider_not_configured: {e}")
        _fail_video_record(store_meta, f"provider_not_configured: {e}")
        ExpenseGuard.unregister_active_job(internal_job_id)

    except GeminiValidationError as e:
        duration_ms = int((time.time() - start_time) * 1000)
        print(f"[ASYNC] ERROR: Validation error for job {internal_job_id}: {e.message}")
        if reservation_id:
            release_job_credits(reservation_id, "validation_error", internal_job_id)
        update_job_status_failed(internal_job_id, f"invalid_params: {e.message}")
        _fail_video_record(store_meta, f"invalid_params: {e.message}")
        ExpenseGuard.unregister_active_job(internal_job_id)

    except GeminiAuthError as e:
        duration_ms = int((time.time() - start_time) * 1000)
        print(f"[ASYNC] ERROR: Auth failed for job {internal_job_id}: {e}")
        if reservation_id:
            release_job_credits(reservation_id, "provider_auth_failed", internal_job_id)
        update_job_status_failed(internal_job_id, f"provider_auth_failed: {e}")
        _fail_video_record(store_meta, f"provider_auth_failed: {e}")
        ExpenseGuard.unregister_active_job(internal_job_id)

    except RuntimeError as e:
        duration_ms = int((time.time() - start_time) * 1000)
        error_str = str(e)
        print(f"[ASYNC] ERROR: Video {task} failed for job {internal_job_id} after {duration_ms}ms: {error_str}")
        if reservation_id:
            release_job_credits(reservation_id, "video_failed", internal_job_id)
        update_job_status_failed(internal_job_id, error_str)
        _fail_video_record(store_meta, error_str)
        ExpenseGuard.unregister_active_job(internal_job_id)

    except Exception as e:
        duration_ms = int((time.time() - start_time) * 1000)
        print(f"[ASYNC] ERROR: Unexpected error for job {internal_job_id} after {duration_ms}ms: {e}")
        if reservation_id:
            release_job_credits(reservation_id, "video_error", internal_job_id)
        update_job_status_failed(internal_job_id, f"video_failed: {e}")
        _fail_video_record(store_meta, f"video_failed: {e}")
        ExpenseGuard.unregister_active_job(internal_job_id)

    finally:
        release_video_worker()


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
    max_consecutive_errors = 1
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
    LEGACY — no longer called.  Retained for reference only.

    All video polling is now handled by the durable job_worker.py which uses
    DB-based claiming, heartbeats, and atomic CAS finalization.  The dispatch
    thread (dispatch_gemini_video_async) calls _mark_job_for_worker() and
    exits immediately — it never falls through to this function.

    If you are adding a new provider, add its polling logic to
    job_worker._poll_provider_once() instead.

    Previous responsibility:
    Provider-aware video polling with exponential backoff for Vertex.
    For 'vertex', uses exponential backoff (2s initial, 10s max, ~6min timeout).

    Status response may contain:
    - video_url: URL to download video from
    - video_bytes: Raw video bytes (base64 decoded) - used by Vertex
    """
    # ── DEAD CODE GUARD ──
    # This function is no longer called. All polling is handled by the durable
    # job_worker. If this executes, something is seriously wrong.
    print(
        f"[ASYNC] CRITICAL: legacy _poll_video_completion reached unexpectedly! "
        f"job={internal_job_id} provider={provider_name} — returning immediately"
    )
    return

    from backend.services.video_router import resolve_video_provider

    if provider_name == "google":
        # Existing Gemini-specific poll (uses Gemini API directly)
        return _poll_gemini_video_completion(
            internal_job_id, identity_id, reservation_id, upstream_id, store_meta,
            max_polls=max_polls, poll_interval=poll_interval,
        )

    # ── Provider lookup ─────
    # Use resolve_video_provider for all Veo providers (Vertex, Gemini AI Studio)
    provider = resolve_video_provider(provider_name)
    if not provider:
        print(f"[ASYNC] ERROR: Unknown provider {provider_name} for job {internal_job_id}")
        if reservation_id:
            release_job_credits(reservation_id, "unknown_provider", internal_job_id)
        update_job_status_failed(internal_job_id, f"unknown_provider: {provider_name}")
        ExpenseGuard.unregister_active_job(internal_job_id)
        return

    # ── Vertex-specific: exponential backoff polling ─────
    if provider_name == "vertex":
        _poll_vertex_with_backoff(
            internal_job_id, identity_id, reservation_id, upstream_id,
            provider, store_meta,
        )
        return

    # ── Seedance-aware poll with pending/processing split timeouts ─────
    if provider_name == "seedance":
        _poll_seedance_with_state_awareness(
            internal_job_id, identity_id, reservation_id, upstream_id,
            provider, store_meta,
        )
        return

    # ── Generic provider poll (future providers) ─────
    consecutive_errors = 0
    max_consecutive_errors = 1

    for poll_num in range(1, max_polls + 1):
        try:
            time.sleep(poll_interval)

            status_resp = provider.check_status(upstream_id)
            status = status_resp.get("status", "unknown")

            # Reset error counter on successful poll
            consecutive_errors = 0

            # Processing — update progress (log every 10 polls to reduce spam)
            if status in ("processing", "pending"):
                progress = status_resp.get("progress", 0)
                if poll_num == 1 or poll_num % 10 == 0:
                    print(f"[ASYNC] Poll {poll_num} for job {internal_job_id} ({provider_name}): status={status} progress={progress}%")

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
                # Check for video_bytes (base64 decoded) or video_url
                video_bytes = status_resp.get("video_bytes")
                video_url = status_resp.get("video_url")

                if video_bytes:
                    _finalize_video_success_with_bytes(
                        internal_job_id, identity_id, reservation_id,
                        video_bytes, status_resp.get("content_type", "video/mp4"),
                        store_meta, provider_name=provider_name,
                    )
                elif video_url:
                    _finalize_video_success(
                        internal_job_id, identity_id, reservation_id,
                        video_url, store_meta, provider_name=provider_name,
                    )
                else:
                    print(f"[ASYNC] ERROR: Job {internal_job_id} ({provider_name}) done but no video data: {list(status_resp.keys())}")
                    raise RuntimeError(f"{provider_name}_video_failed: Task done but no video data")
                return

            elif status in ("failed", "error"):
                error_code = status_resp.get("error", f"{provider_name}_video_failed")
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


# ── Seedance timeout constants (seconds) by task tier ────────────────────────
# Each tier has: (pending_soft, pending_hard, processing_soft, processing_hard)
#
# PiAPI observed timings (2026-03-10):
#   fast:    7-8 min total (queue + render)
#   preview: can sit Pending 20-100+ min, total up to 1h40m
#
# Strategy: preview gets a generous pending window, but if it stalls we
# fallback to fast tier (see _seedance_pending_fallback).
_SEEDANCE_TIMEOUTS = {
    "seedance-2-fast-preview": (5 * 60, 15 * 60, 10 * 60, 20 * 60),
    "seedance-2-preview":      (15 * 60, 30 * 60, 15 * 60, 30 * 60),
}
_SEEDANCE_DEFAULT_TIMEOUTS = (5 * 60, 15 * 60, 10 * 60, 20 * 60)

# Poll intervals
_SEEDANCE_POLL_NORMAL = 4       # seconds between polls while within soft timeout
_SEEDANCE_POLL_SLOW = 15        # seconds between polls after soft timeout hit


def _poll_seedance_with_state_awareness(
    internal_job_id: str,
    identity_id: str,
    reservation_id: Optional[str],
    upstream_id: str,
    provider,
    store_meta: dict,
):
    """
    LEGACY — no longer called.  Only reachable from _poll_video_completion
    which is itself dead code.  Retained for reference only.

    All Seedance polling is now handled by job_worker._poll_provider_once().
    """
    # ── DEAD CODE GUARD ──
    print(
        f"[ASYNC] CRITICAL: legacy _poll_seedance_with_state_awareness reached! "
        f"job={internal_job_id} — returning immediately"
    )
    return

    task_type = store_meta.get("task_type", "seedance-2-fast-preview")
    pend_soft, pend_hard, proc_soft, proc_hard = _SEEDANCE_TIMEOUTS.get(
        task_type, _SEEDANCE_DEFAULT_TIMEOUTS,
    )

    pending_elapsed = 0.0
    processing_elapsed = 0.0
    last_phase = "pending"       # "pending" or "processing"
    consecutive_errors = 0
    poll_num = 0
    did_fallback = False          # True if we already retried with fast tier

    while True:
        # ── Determine poll interval ──
        if last_phase == "pending":
            phase_elapsed = pending_elapsed
            past_soft = phase_elapsed >= pend_soft
        else:
            phase_elapsed = processing_elapsed
            past_soft = phase_elapsed >= proc_soft

        sleep_sec = _SEEDANCE_POLL_SLOW if past_soft else _SEEDANCE_POLL_NORMAL

        time.sleep(sleep_sec)
        poll_num += 1

        # ── Hard timeout check ──
        if last_phase == "pending" and pending_elapsed >= pend_hard:
            # FALLBACK: If this was a preview job, retry once with fast tier
            if task_type == "seedance-2-preview" and not did_fallback:
                fallback_result = _seedance_pending_fallback(
                    internal_job_id, identity_id, reservation_id,
                    provider, store_meta,
                )
                if fallback_result:
                    # Fallback created a new upstream task — reset poll state
                    upstream_id = fallback_result["upstream_id"]
                    task_type = "seedance-2-fast-preview"
                    pend_soft, pend_hard, proc_soft, proc_hard = _SEEDANCE_TIMEOUTS.get(
                        task_type, _SEEDANCE_DEFAULT_TIMEOUTS,
                    )
                    pending_elapsed = 0.0
                    processing_elapsed = 0.0
                    last_phase = "pending"
                    poll_num = 0
                    did_fallback = True
                    print(f"[SEEDANCE] Fallback to fast tier for job {internal_job_id}, new upstream={upstream_id}")
                    continue

            _seedance_fail(
                internal_job_id, reservation_id,
                "seedance_pending_timeout",
                f"Provider never started this job after {int(pending_elapsed)}s",
            )
            return
        if last_phase == "processing" and processing_elapsed >= proc_hard:
            _seedance_fail(
                internal_job_id, reservation_id,
                "seedance_processing_timeout",
                f"Provider did not finish rendering after {int(processing_elapsed)}s",
            )
            return

        # ── Poll provider ──
        try:
            status_resp = provider.check_status(upstream_id)
        except Exception as e:
            consecutive_errors += 1
            print(f"[SEEDANCE] Poll error #{consecutive_errors} for {internal_job_id}: {e}")
            if consecutive_errors >= 3:
                _seedance_fail(internal_job_id, reservation_id, "seedance_poll_error", str(e))
                return
            pending_elapsed += sleep_sec
            processing_elapsed += sleep_sec
            continue

        consecutive_errors = 0
        status = status_resp.get("status", "pending")
        provider_status = status_resp.get("provider_status", status)
        started_at = status_resp.get("started_at")
        ended_at = status_resp.get("ended_at")
        progress = status_resp.get("progress", 0)

        # ── Logging ──
        timeout_stage = "past_soft" if past_soft else "normal"
        if poll_num <= 3 or poll_num % 10 == 0 or status != last_phase:
            print(
                f"[SEEDANCE] Poll {poll_num} job={internal_job_id} "
                f"provider_status={provider_status} internal={status} "
                f"started_at={started_at} ended_at={ended_at} "
                f"pending={int(pending_elapsed)}s processing={int(processing_elapsed)}s "
                f"timeout_stage={timeout_stage}"
            )

        # ── Route by status ──
        if status == "done":
            video_url = status_resp.get("video_url")
            if video_url:
                _finalize_video_success(
                    internal_job_id, identity_id, reservation_id,
                    video_url, store_meta, provider_name="seedance",
                )
            else:
                _seedance_fail(
                    internal_job_id, reservation_id,
                    "seedance_no_video_url", "Completed but no video URL",
                )
            return

        if status == "failed":
            error_code = status_resp.get("error", "seedance_generation_failed")
            error_msg = status_resp.get("message", "Seedance generation failed")
            _seedance_fail(internal_job_id, reservation_id, error_code, error_msg)
            return

        # ── Pending (provider never started) ──
        if status == "pending":
            pending_elapsed += sleep_sec
            last_phase = "pending"
            db_status = "provider_pending"
            _update_job_progress(internal_job_id, store_meta, db_status, progress, {
                "provider_status": provider_status,
                "pending_seconds": int(pending_elapsed),
            })

        # ── Processing (actively rendering) ──
        elif status == "processing":
            processing_elapsed += sleep_sec
            # If we just transitioned from pending → processing, reset pending timer
            if last_phase == "pending":
                print(f"[SEEDANCE] Job {internal_job_id} transitioned pending→processing after {int(pending_elapsed)}s pending")
            last_phase = "processing"
            _update_job_progress(internal_job_id, store_meta, "processing", progress, {
                "provider_status": provider_status,
                "started_at": started_at,
            })

        else:
            # Unknown status — treat as pending
            pending_elapsed += sleep_sec


def _seedance_fail(internal_job_id, reservation_id, error_code, error_msg):
    """Helper: mark a Seedance job as failed, release credits, unregister.

    Uses 'provider_stalled' DB status for timeout errors where provider never
    started (pending_timeout) — distinct from explicit provider failure.
    """
    # Determine DB status: provider_stalled for queue timeouts, failed for everything else
    is_stalled = error_code in (
        "seedance_pending_timeout",
        "seedance_processing_timeout",
        "seedance_poll_error",
    )
    db_status = "provider_stalled" if is_stalled else "failed"

    print(f"[SEEDANCE] FAIL job={internal_job_id} code={error_code} db_status={db_status} msg={error_msg}")

    # Store structured error metadata in DB jsonb (for frontend to surface)
    _update_job_meta(internal_job_id, {
        "error_code": error_code,
        "error_message": error_msg,
        "failure_reason": _FAILURE_REASON_MAP.get(error_code, error_msg),
    })

    # Also update in-memory store with error details
    store = load_store()
    sm = store.get(internal_job_id)
    if sm:
        sm["status"] = db_status
        sm["error_code"] = error_code
        sm["error"] = _FAILURE_REASON_MAP.get(error_code, error_msg)
        store[internal_job_id] = sm
        save_store(store)

    if reservation_id:
        release_job_credits(reservation_id, error_code, internal_job_id)

    # Use provider_stalled or failed in DB
    if is_stalled:
        _update_job_db_status(internal_job_id, "provider_stalled", f"{error_code}: {error_msg}")
    else:
        update_job_status_failed(internal_job_id, f"{error_code}: {error_msg}")
    ExpenseGuard.unregister_active_job(internal_job_id)


# Human-readable failure reasons keyed by error_code
_FAILURE_REASON_MAP = {
    "seedance_pending_timeout": "Provider queue timed out — Seedance did not start this job in time",
    "seedance_processing_timeout": "Render timed out — Seedance started but did not finish in time",
    "seedance_poll_error": "Lost connection to provider during generation",
    "seedance_generation_failed": "Seedance rejected this generation",
    "seedance_no_video_url": "Generation completed but no video was returned",
    "seedance_auth_error": "Provider authentication failed",
}


def _update_job_db_status(job_id: str, status: str, error_message: str = None):
    """Update job status in DB to an arbitrary status string (e.g. provider_stalled)."""
    if not USE_DB:
        return
    try:
        with get_conn() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    f"""
                    UPDATE {Tables.JOBS}
                    SET status = %s, error_message = %s, updated_at = NOW()
                    WHERE id::text = %s
                    """,
                    (status, (error_message[:500] if error_message else None), job_id),
                )
            conn.commit()
    except Exception as e:
        print(f"[JOB] ERROR updating job {job_id} to status={status}: {e}")


def _seedance_pending_fallback(
    internal_job_id: str,
    identity_id: str,
    reservation_id: Optional[str],
    provider,
    store_meta: dict,
) -> Optional[dict]:
    """
    Attempt to retry a Seedance preview job with the fast tier.

    Called when a preview job times out in pending. Creates a new upstream task
    with seedance-2-fast-preview. The same internal_job_id, reservation, and
    credits are reused — no double charge.

    Returns {"upstream_id": "..."} on success, None on failure.
    """
    try:
        print(f"[SEEDANCE] Attempting fallback preview→fast for job {internal_job_id}")

        prompt = store_meta.get("prompt", "")
        duration = store_meta.get("duration_seconds", 5)
        aspect = store_meta.get("aspect_ratio", "16:9")
        image_urls = None
        if store_meta.get("task") == "image2video":
            img = store_meta.get("image_data") or ""
            if img:
                image_urls = [img]

        from backend.services.seedance_service import create_seedance_task
        resp = create_seedance_task(
            prompt=prompt,
            duration=duration,
            aspect_ratio=aspect,
            image_urls=image_urls,
            task_type="seedance-2-fast-preview",
        )

        new_upstream = resp.get("task_id")
        if not new_upstream:
            print(f"[SEEDANCE] Fallback failed: no task_id returned")
            return None

        # Update store and DB with new upstream id + fallback metadata
        store_meta["upstream_id"] = new_upstream
        store_meta["operation_name"] = new_upstream
        store_meta["task_type"] = "seedance-2-fast-preview"
        store_meta["seedance_variant"] = "seedance-2-fast-preview"
        store_meta["seedance_tier"] = "fast"
        store_meta["fallback_from"] = "seedance-2-preview"
        store_meta["status"] = "processing"

        store = load_store()
        store[internal_job_id] = store_meta
        save_store(store)

        _update_job_meta(internal_job_id, {
            "upstream_id": new_upstream,
            "task_type": "seedance-2-fast-preview",
            "fallback_from": "seedance-2-preview",
            "fallback_reason": "pending_timeout",
        })
        update_job_with_upstream_id(internal_job_id, new_upstream)

        return {"upstream_id": new_upstream}

    except Exception as e:
        print(f"[SEEDANCE] Fallback failed for job {internal_job_id}: {e}")
        return None


def _update_job_progress(internal_job_id, store_meta, db_status, progress, extra_meta=None):
    """Helper: update job store and DB with current progress/status."""
    store = load_store()
    store_meta["progress"] = progress
    store_meta["status"] = db_status
    if extra_meta:
        store_meta.update(extra_meta)
    store[internal_job_id] = store_meta
    save_store(store)

    if USE_DB:
        try:
            meta_json = {"progress": progress, "status_detail": db_status}
            if extra_meta:
                meta_json.update(extra_meta)
            with get_conn() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(
                        f"""
                        UPDATE {Tables.JOBS}
                        SET status = %s,
                            meta = COALESCE(meta, '{{}}'::jsonb) || %s::jsonb,
                            updated_at = NOW()
                        WHERE id::text = %s
                        """,
                        (db_status, json.dumps(meta_json), internal_job_id),
                    )
                conn.commit()
        except Exception as e:
            print(f"[SEEDANCE] DB update error for {internal_job_id}: {e}")


def _poll_vertex_with_backoff(
    internal_job_id: str,
    identity_id: str,
    reservation_id: Optional[str],
    upstream_id: str,
    provider,
    store_meta: dict,
):
    """
    Vertex-specific polling with exponential backoff.

    - Initial delay: 2s
    - Max delay: 10s
    - Total timeout: ~6 minutes (360s)
    - No progress warnings (Vertex often shows 0% until done)
    """
    INITIAL_DELAY = 2
    MAX_DELAY = 10
    TIMEOUT_SECONDS = 360  # 6 minutes for fast model

    consecutive_errors = 0
    max_consecutive_errors = 1
    current_delay = INITIAL_DELAY
    elapsed = 0
    poll_count = 0

    print(f"[ASYNC] Starting Vertex poll for job {internal_job_id} (timeout={TIMEOUT_SECONDS}s)")

    while elapsed < TIMEOUT_SECONDS:
        try:
            time.sleep(current_delay)
            elapsed += current_delay
            poll_count += 1

            status_resp = provider.check_status(upstream_id)
            status = status_resp.get("status", "unknown")

            # Reset error counter on successful poll
            consecutive_errors = 0

            # Log progress sparingly (every 30s or on status change)
            if poll_count == 1 or elapsed % 30 < current_delay:
                progress = status_resp.get("progress", 0)
                print(f"[ASYNC] Vertex poll #{poll_count} ({elapsed}s): status={status}, progress={progress}%")

            if status == "processing":
                # Update store (no warnings for 0% - Vertex doesn't report progress)
                progress = status_resp.get("progress", 0)
                store = load_store()
                store_meta["progress"] = progress
                store_meta["status"] = "processing"
                store[internal_job_id] = store_meta
                save_store(store)

                # Exponential backoff: increase delay up to max
                current_delay = min(current_delay * 1.5, MAX_DELAY)

            elif status == "done":
                # Check for video_bytes (base64 decoded) or video_url
                video_bytes = status_resp.get("video_bytes")
                video_url = status_resp.get("video_url")

                print(f"[ASYNC] Vertex job {internal_job_id} completed in {elapsed}s")

                if video_bytes:
                    _finalize_video_success_with_bytes(
                        internal_job_id, identity_id, reservation_id,
                        video_bytes, status_resp.get("content_type", "video/mp4"),
                        store_meta, provider_name="vertex",
                    )
                elif video_url:
                    _finalize_video_success(
                        internal_job_id, identity_id, reservation_id,
                        video_url, store_meta, provider_name="vertex",
                    )
                else:
                    print(f"[ASYNC] ERROR: Vertex job {internal_job_id} done but no video data: {list(status_resp.keys())}")
                    raise RuntimeError("vertex_video_failed: Task done but no video data")
                return

            elif status in ("failed", "error"):
                error_code = status_resp.get("error", "vertex_video_failed")
                error_msg = status_resp.get("message", "Video generation failed")

                print(f"[ASYNC] Vertex video failed for job {internal_job_id}: {error_code} - {error_msg}")

                if reservation_id:
                    release_job_credits(reservation_id, error_code, internal_job_id)
                update_job_status_failed(internal_job_id, f"{error_code}: {error_msg}")
                ExpenseGuard.unregister_active_job(internal_job_id)
                return

        except Exception as e:
            consecutive_errors += 1
            print(f"[ASYNC] Error polling Vertex for job {internal_job_id} (attempt {consecutive_errors}): {e}")

            if consecutive_errors >= max_consecutive_errors:
                print(f"[ASYNC] Too many Vertex poll errors for job {internal_job_id}, failing")
                if reservation_id:
                    release_job_credits(reservation_id, "vertex_poll_error", internal_job_id)
                update_job_status_failed(internal_job_id, f"vertex_poll_error: {e}")
                ExpenseGuard.unregister_active_job(internal_job_id)
                return

            # On error, use shorter delay for retry
            current_delay = INITIAL_DELAY

    # Timeout
    print(f"[ASYNC] Timeout: Vertex video job {internal_job_id} did not complete after {TIMEOUT_SECONDS}s")
    if reservation_id:
        release_job_credits(reservation_id, "vertex_timeout", internal_job_id)
    update_job_status_failed(
        internal_job_id,
        f"vertex_timeout: Video generation did not complete within {TIMEOUT_SECONDS} seconds",
    )
    ExpenseGuard.unregister_active_job(internal_job_id)


def _finalize_video_success_with_bytes(
    internal_job_id: str,
    identity_id: str,
    reservation_id: Optional[str],
    video_bytes: bytes,
    content_type: str,
    store_meta: dict,
    provider_name: str = "vertex",
):
    """
    Finalize a successful video generation when we already have the video bytes.

    Used by Vertex when it returns base64 encoded video directly (no URL to download).

    1. Upload video bytes to S3
    2. Extract and upload thumbnail
    3. Update job status and store
    4. Finalize credits (CAPTURE, not release)
    """
    import base64 as b64_module

    print(f"[ASYNC] Finalizing {provider_name} video with {len(video_bytes)} bytes for job {internal_job_id}")

    final_video_url = None
    s3_video_url = None
    s3_thumbnail_url = None

    # Upload to S3
    if AWS_BUCKET_MODELS:
        try:
            ext = ".mp4"
            if "webm" in content_type:
                ext = ".webm"

            # S3 key: videos/{provider}/{identity}/{job_id}.mp4
            video_b64 = f"data:{content_type};base64,{b64_module.b64encode(video_bytes).decode('utf-8')}"
            s3_video_url = safe_upload_to_s3(
                video_b64,
                content_type,
                "videos",
                f"{provider_name}_{internal_job_id}",
                user_id=identity_id,
                key_base=f"videos/{provider_name}/{identity_id or 'public'}/{internal_job_id}{ext}",
                provider=provider_name,
            )

            if s3_video_url:
                print(f"[ASYNC] Uploaded video to S3: {s3_video_url}")
                final_video_url = s3_video_url

                # Extract and upload thumbnail
                try:
                    thumb_bytes = extract_video_thumbnail(video_bytes, timestamp_sec=1.0)
                    if thumb_bytes:
                        thumb_b64 = f"data:image/jpeg;base64,{b64_module.b64encode(thumb_bytes).decode('utf-8')}"
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
                            print(f"[ASYNC] Uploaded thumbnail to S3: {s3_thumbnail_url}")
                    else:
                        print(f"[ASYNC] WARNING: Thumbnail extraction returned None for {internal_job_id}")
                        s3_thumbnail_url = s3_video_url
                except Exception as thumb_err:
                    print(f"[ASYNC] WARNING: Thumbnail extraction failed for {internal_job_id}: {thumb_err}")
                    s3_thumbnail_url = s3_video_url
            else:
                print(f"[ASYNC] ERROR: S3 upload returned no URL for {internal_job_id}")

        except Exception as e:
            print(f"[ASYNC] ERROR: Failed to upload video to S3 for {internal_job_id}: {e}")

    if not final_video_url:
        # This is a critical error - we have bytes but couldn't upload to S3
        print(f"[ASYNC] ERROR: No final video URL for {internal_job_id}, failing job")
        if reservation_id:
            release_job_credits(reservation_id, "s3_upload_failed", internal_job_id)
        update_job_status_failed(internal_job_id, "s3_upload_failed: Could not upload video to storage")
        _fail_video_record(store_meta, "s3_upload_failed: Could not upload video to storage")
        ExpenseGuard.unregister_active_job(internal_job_id)
        return

    # Update store
    store = load_store()
    store_meta["status"] = "done"
    store_meta["video_url"] = final_video_url
    if s3_video_url:
        store_meta["s3_video_url"] = s3_video_url
    if s3_thumbnail_url:
        store_meta["thumbnail_url"] = s3_thumbnail_url
    store[internal_job_id] = store_meta
    save_store(store)

    # Finalize credits (CAPTURE - charge the user)
    finalize_result = finalize_job_credits(reservation_id, internal_job_id, identity_id)
    new_balance = finalize_result.get("new_balance")

    if finalize_result.get("success"):
        print(f"[ASYNC] Credits CAPTURED for {provider_name} video job {internal_job_id} new_balance={new_balance}")
        store_meta["new_balance"] = new_balance
        store_meta["credits_charged"] = finalize_result.get("cost")
    elif reservation_id:
        print(f"[ASYNC] WARNING: Credits finalize returned False for job {internal_job_id}")

    # Save to normalized tables (videos + history_items)
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
        upstream_job_id=store_meta.get("upstream_id") or store_meta.get("operation_name") or "",
    )

    # Post to Discord
    prompt = store_meta.get("prompt", "")
    send_to_discord("🎬 New AI Video Generated", prompt, str(s3_thumbnail_url) if s3_thumbnail_url else None, identity_id)

    # Unregister active job
    ExpenseGuard.unregister_active_job(internal_job_id)

    # Update meta with video_url in jobs table
    if USE_DB:
        try:
            meta_update = {
                "video_url": final_video_url,
                "progress": 100,
                "provider": provider_name,
            }
            if s3_video_url:
                meta_update["s3_video_url"] = s3_video_url
            if s3_thumbnail_url:
                meta_update["thumbnail_url"] = s3_thumbnail_url
            if new_balance is not None:
                meta_update["new_balance"] = new_balance
            if store_meta.get("credits_charged") is not None:
                meta_update["credits_charged"] = store_meta.get("credits_charged")

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

    print(f"[ASYNC] {provider_name} video job {internal_job_id} completed successfully")

    return {
        "final_video_url": final_video_url,
        "s3_video_url": s3_video_url,
        "s3_thumbnail_url": s3_thumbnail_url,
    }


def _finalize_video_success(
    internal_job_id: str,
    identity_id: str,
    reservation_id: Optional[str],
    video_url: str,
    store_meta: dict,
    provider_name: str = "google",
) -> dict:
    """
    Finalize a successful video generation.

    1. Download video from provider (or ephemeral URL)
    2. Upload to S3 (if configured)
    3. Update job status and store
    4. Finalize credits

    Returns dict with final URLs:
        {"final_video_url": ..., "s3_video_url": ..., "s3_thumbnail_url": ...}

    Works for any provider — uses the VideoRouter to get the right
    download method.
    """
    from backend.services.video_router import video_router

    print(f"[ASYNC] Finalizing {provider_name} video for job {internal_job_id}: {video_url[:80]}...")

    final_video_url = video_url
    s3_video_url = None
    s3_thumbnail_url = None

    # Try to download and upload to S3 for persistence
    if AWS_BUCKET_MODELS:
        try:
            provider = video_router.get_provider(provider_name)

            print(f"[ASYNC] Downloading video from {provider_name} for S3 upload...")
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
            #   videos/seedance/<identity>/<job_id>.mp4
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

    # Finalize credits (capture permanently - MUST happen before any code that could throw)
    # This marks the reservation as 'finalized' in DB, which prevents release from refunding
    finalize_result = finalize_job_credits(reservation_id, internal_job_id, identity_id)
    new_balance = finalize_result.get("new_balance")

    if finalize_result.get("success"):
        print(f"[ASYNC] Credits CAPTURED for {provider_name} video job {internal_job_id} reservation_id={reservation_id} new_balance={new_balance}")
        # Store new_balance in metadata so status endpoint can return it to frontend
        store_meta["new_balance"] = new_balance
        store_meta["credits_charged"] = finalize_result.get("cost")
    elif reservation_id:
        print(f"[ASYNC] WARNING: Credits finalize returned False for job {internal_job_id} reservation_id={reservation_id}")
    else:
        print(f"[ASYNC] No reservation_id for video job {internal_job_id} - marked as ready_unbilled")

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
            # Include new_balance so frontend can update credits display
            if new_balance is not None:
                meta_update["new_balance"] = new_balance
            if store_meta.get("credits_charged") is not None:
                meta_update["credits_charged"] = store_meta.get("credits_charged")

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

    # Return final URLs so the caller can persist them in the jobs table
    return {
        "final_video_url": final_video_url,
        "s3_video_url": s3_video_url,
        "s3_thumbnail_url": s3_thumbnail_url,
    }


def _dispatch_gemini_video_async(internal_job_id, identity_id, reservation_id, payload, store_meta):
    """Adapter for video dispatch (monolith-compatible name)."""
    return dispatch_gemini_video_async(internal_job_id, identity_id, reservation_id, payload, store_meta)


