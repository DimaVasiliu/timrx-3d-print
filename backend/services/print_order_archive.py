"""
Print order archive — captures the model file (GLB), converts to STL,
and copies the thumbnail into a TimrX-controlled S3 location once payment
clears.  This ensures the operator can ALWAYS fulfill an order even if the
upstream provider URL (Meshy etc.) expires.

S3 layout (under the existing models bucket):
  models/print-orders/<order_number>/model.glb
  models/print-orders/<order_number>/model.stl
  thumbnails/print-orders/<order_number>/thumb.jpg
"""

from __future__ import annotations

import io
from datetime import datetime, timezone
from typing import Any, Dict, Optional, Tuple

import requests

from backend.db import get_conn, Tables
from backend.services import s3_service

try:
    import trimesh  # noqa: F401  (presence check)
    TRIMESH_OK = True
except Exception as _e:
    print(f"[PRINT-ARCHIVE] trimesh import failed: {_e}")
    TRIMESH_OK = False


def _download(url: str, timeout: int = 180) -> Tuple[bytes, str]:
    """Download bytes + return (data, content_type)."""
    resp = requests.get(url, timeout=timeout)
    resp.raise_for_status()
    return resp.content, resp.headers.get("Content-Type", "application/octet-stream")


def _glb_to_stl_bytes(glb_bytes: bytes) -> Optional[bytes]:
    """Convert GLB bytes to STL bytes using trimesh.  Returns None on failure."""
    if not TRIMESH_OK:
        return None
    try:
        import trimesh
        scene_or_mesh = trimesh.load(io.BytesIO(glb_bytes), file_type="glb")
        # Scenes need to be merged into a single mesh for STL.
        if isinstance(scene_or_mesh, trimesh.Scene):
            if not scene_or_mesh.geometry:
                return None
            mesh = trimesh.util.concatenate(
                [g for g in scene_or_mesh.geometry.values() if hasattr(g, "vertices")]
            )
        else:
            mesh = scene_or_mesh
        if mesh is None or not hasattr(mesh, "export"):
            return None
        return mesh.export(file_type="stl")
    except Exception as e:
        print(f"[PRINT-ARCHIVE] GLB→STL conversion failed: {e}")
        return None


def archive_for_order(order_id: str) -> Dict[str, Any]:
    """
    Idempotently archive an order's model + thumbnail and persist S3 keys.

    Safe to call multiple times — skips work if archived_at is already set.
    Returns a dict describing what happened (for logging / admin display).
    """
    # Load the order
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT id, order_number, model_glb_url, model_thumb_url,
                       archived_glb_key, archived_stl_key, archived_thumb_key,
                       archived_at
                FROM {Tables.PRINT_ORDERS}
                WHERE id = %s
                LIMIT 1
                """,
                (order_id,),
            )
            row = cur.fetchone()
    if not row:
        return {"ok": False, "error": "order not found"}
    r = dict(row)

    if r.get("archived_at"):
        return {"ok": True, "skipped": True, "reason": "already archived"}

    order_number = r["order_number"]
    glb_url   = r.get("model_glb_url") or ""
    thumb_url = r.get("model_thumb_url") or ""

    glb_key:   Optional[str] = None
    stl_key:   Optional[str] = None
    thumb_key: Optional[str] = None
    errors: list[str] = []

    # ── GLB ──────────────────────────────────────────────────────────
    glb_bytes: Optional[bytes] = None
    if glb_url:
        try:
            glb_bytes, _ct = _download(glb_url)
            key = f"models/print-orders/{order_number}/model.glb"
            res = s3_service.upload_bytes_to_s3(
                data_bytes=glb_bytes,
                content_type="model/gltf-binary",
                prefix="models",
                key=key,
            )
            glb_key = res.get("s3_key") if isinstance(res, dict) else key
            print(f"[PRINT-ARCHIVE] {order_number} GLB → s3:{glb_key}")
        except Exception as e:
            errors.append(f"glb: {e}")
            print(f"[PRINT-ARCHIVE] {order_number} GLB upload failed: {e}")

    # ── STL (derived from GLB) ───────────────────────────────────────
    if glb_bytes is not None:
        stl_bytes = _glb_to_stl_bytes(glb_bytes)
        if stl_bytes:
            try:
                key = f"models/print-orders/{order_number}/model.stl"
                res = s3_service.upload_bytes_to_s3(
                    data_bytes=stl_bytes,
                    content_type="model/stl",
                    prefix="models",
                    key=key,
                )
                stl_key = res.get("s3_key") if isinstance(res, dict) else key
                print(f"[PRINT-ARCHIVE] {order_number} STL → s3:{stl_key}")
            except Exception as e:
                errors.append(f"stl: {e}")
                print(f"[PRINT-ARCHIVE] {order_number} STL upload failed: {e}")
        else:
            errors.append("stl: conversion failed")

    # ── Thumbnail ────────────────────────────────────────────────────
    if thumb_url:
        try:
            thumb_bytes, thumb_ct = _download(thumb_url, timeout=30)
            ext = "jpg"
            if "png" in (thumb_ct or "").lower():
                ext = "png"
            elif "webp" in (thumb_ct or "").lower():
                ext = "webp"
            key = f"thumbnails/print-orders/{order_number}/thumb.{ext}"
            res = s3_service.upload_bytes_to_s3(
                data_bytes=thumb_bytes,
                content_type=thumb_ct or "image/jpeg",
                prefix="thumbnails",
                key=key,
            )
            thumb_key = res.get("s3_key") if isinstance(res, dict) else key
            print(f"[PRINT-ARCHIVE] {order_number} thumb → s3:{thumb_key}")
        except Exception as e:
            errors.append(f"thumb: {e}")
            print(f"[PRINT-ARCHIVE] {order_number} thumb upload failed: {e}")

    # ── Persist results on the order row ─────────────────────────────
    now = datetime.now(timezone.utc)
    err_str = "; ".join(errors)[:500] if errors else None
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                UPDATE {Tables.PRINT_ORDERS}
                SET archived_glb_key   = COALESCE(%s, archived_glb_key),
                    archived_stl_key   = COALESCE(%s, archived_stl_key),
                    archived_thumb_key = COALESCE(%s, archived_thumb_key),
                    archived_at        = %s,
                    archive_error      = %s
                WHERE id = %s
                """,
                (glb_key, stl_key, thumb_key, now, err_str, order_id),
            )
        conn.commit()

    return {
        "ok": not (errors and not (glb_key or stl_key or thumb_key)),
        "glb_key":   glb_key,
        "stl_key":   stl_key,
        "thumb_key": thumb_key,
        "errors":    errors,
    }


def get_admin_download_target(
    order_id_or_number: str,
    kind: str,
) -> Optional[Tuple[str, str, str]]:
    """
    Look up an archived file's S3 key for an admin download.

    Returns (s3_key, content_type, filename) or None.
    kind ∈ {'glb', 'stl', 'thumb'}.
    """
    col_map = {
        "glb":   ("archived_glb_key",   "model/gltf-binary", "glb"),
        "stl":   ("archived_stl_key",   "model/stl",         "stl"),
        "thumb": ("archived_thumb_key", "image/jpeg",        "jpg"),
    }
    if kind not in col_map:
        return None
    col, ct, ext = col_map[kind]

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT order_number, {col} AS key
                FROM {Tables.PRINT_ORDERS}
                WHERE id::text = %s OR order_number = %s
                LIMIT 1
                """,
                (order_id_or_number, order_id_or_number),
            )
            row = cur.fetchone()
    if not row:
        return None
    r = dict(row)
    key = r.get("key")
    if not key:
        return None
    filename = f"{r['order_number']}-model.{ext}" if kind != "thumb" else f"{r['order_number']}-thumb.{ext}"
    return (key, ct, filename)
