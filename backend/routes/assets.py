"""
Assets Routes Blueprint (Modular)
--------------------------------
Registered under /api/_mod.
"""

from __future__ import annotations

from urllib.parse import urlparse
import ipaddress
import socket

import boto3
import requests
from flask import Blueprint, Response, abort, jsonify, request

from backend.config import (
    AWS_ACCESS_KEY_ID,
    AWS_BUCKET_MODELS,
    AWS_REGION,
    AWS_SECRET_ACCESS_KEY,
    PROXY_ALLOWED_HOSTS,
)
from backend.db import USE_DB, get_conn, dict_row, Tables
from backend.middleware import with_session
from backend.services.identity_service import require_identity

bp = Blueprint("assets", __name__)

# Local S3 client for presigned URLs
_s3 = boto3.client(
    "s3",
    region_name=AWS_REGION,
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
)


def _extract_s3_key_from_url(url: str | None) -> str | None:
    if not url:
        return None
    parsed = urlparse(url)
    if ".s3." in parsed.netloc and ".amazonaws.com" in parsed.netloc:
        return parsed.path.lstrip("/") if parsed.path else None
    return None


def _extract_meshy_task_id(url: str | None) -> str | None:
    """Extract Meshy task ID from assets.meshy.ai URLs."""
    if not url:
        return None
    try:
        parsed = urlparse(url)
        path = parsed.path or ""
    except Exception:
        return None
    # Meshy URLs often include /tasks/<uuid>
    import re

    match = re.search(r"/tasks/([0-9a-fA-F-]{36})", path)
    if match:
        return match.group(1)
    return None


def _is_private_ip(host: str) -> bool:
    """Reject private/loopback/reserved IPs to prevent SSRF."""
    try:
        ip = ipaddress.ip_address(host)
        return ip.is_private or ip.is_loopback or ip.is_link_local or ip.is_reserved or ip.is_multicast
    except ValueError:
        return False


def _host_is_allowed(host: str) -> bool:
    allowed = {h.lower() for h in PROXY_ALLOWED_HOSTS}
    if host.lower() not in allowed:
        return False
    # Extra safety: if host is an IP, ensure it's not private
    if _is_private_ip(host):
        return False
    # Resolve DNS and block private ranges
    try:
        for res in socket.getaddrinfo(host, 443):
            ip = res[4][0]
            if _is_private_ip(ip):
                return False
    except Exception:
        # If DNS fails, treat as not allowed
        return False
    return True


@bp.route("/proxy-glb", methods=["GET", "OPTIONS", "HEAD"])
@with_session
def proxy_glb_mod():
    if request.method == "OPTIONS":
        return ("", 204)

    identity_id, auth_error = require_identity()
    if auth_error:
        return auth_error

    u = request.args.get("u", "").strip()
    if not u:
        return jsonify({"ok": False, "error": {"code": "MISSING_URL", "message": "u query param required"}}), 400

    p = urlparse(u)
    if p.scheme != "https":
        abort(400)
    host = (p.hostname or "").lower()
    if not _host_is_allowed(host):
        return jsonify({"ok": False, "error": {"code": "HOST_NOT_ALLOWED", "message": "Host not allowed"}}), 400

    if not USE_DB:
        return jsonify({"ok": False, "error": {"code": "DB_UNAVAILABLE", "message": "Database not configured"}}), 503

    s3_key = _extract_s3_key_from_url(u)
    meshy_task_id = _extract_meshy_task_id(u)
    if host == "assets.meshy.ai" and not meshy_task_id:
        return jsonify({
            "ok": False,
            "error": {"code": "INVALID_MESHY_URL", "message": "Meshy URL must include a task id"},
        }), 400
    try:
        with get_conn() as conn:
            with conn.cursor(row_factory=dict_row) as cur:
                if s3_key:
                    # For S3 URLs: check by key in models AND by full URL in both tables
                    # This handles cases where glb_s3_key may not be populated (older data)
                    cur.execute(
                        f"""
                        SELECT 1
                        FROM {Tables.MODELS}
                        WHERE identity_id = %s AND (
                            glb_s3_key = %s OR thumbnail_s3_key = %s
                            OR glb_url = %s OR thumbnail_url = %s
                        )
                        UNION
                        SELECT 1
                        FROM {Tables.HISTORY_ITEMS}
                        WHERE identity_id = %s AND (glb_url = %s OR thumbnail_url = %s)
                        LIMIT 1
                        """,
                        (identity_id, s3_key, s3_key, u, u, identity_id, u, u),
                    )
                elif meshy_task_id:
                    cur.execute(
                        f"""
                        SELECT 1
                        FROM {Tables.ACTIVE_JOBS}
                        WHERE identity_id = %s AND upstream_job_id = %s
                        UNION
                        SELECT 1
                        FROM {Tables.HISTORY_ITEMS}
                        WHERE identity_id = %s
                          AND (
                            payload->>'original_job_id' = %s
                            OR payload->>'preview_task_id' = %s
                            OR payload->>'source_task_id' = %s
                          )
                        LIMIT 1
                        """,
                        (
                            identity_id,
                            meshy_task_id,
                            identity_id,
                            meshy_task_id,
                            meshy_task_id,
                            meshy_task_id,
                        ),
                    )
                else:
                    cur.execute(
                        f"""
                        SELECT 1
                        FROM {Tables.MODELS}
                        WHERE identity_id = %s AND (glb_url = %s OR thumbnail_url = %s)
                        UNION
                        SELECT 1
                        FROM {Tables.HISTORY_ITEMS}
                        WHERE identity_id = %s AND (glb_url = %s OR thumbnail_url = %s)
                        LIMIT 1
                        """,
                        (identity_id, u, u, identity_id, u, u),
                    )
                row = cur.fetchone()
        if not row:
            print(f"[proxy-glb][mod] ownership check failed: no row found for identity={identity_id}, url={u[:80]}...")
            return jsonify({"ok": False, "error": {"code": "NOT_FOUND", "message": "Asset not found"}}), 404
    except Exception as e:
        print(f"[proxy-glb][mod] ownership check failed: {e}")
        return jsonify({"ok": False, "error": {"code": "DB_ERROR", "message": "Ownership check failed"}}), 500

    # If the URL is our S3 bucket, use a presigned URL to avoid 403 on private objects.
    if AWS_BUCKET_MODELS and host.startswith(AWS_BUCKET_MODELS.lower()) and s3_key:
        try:
            u = _s3.generate_presigned_url(
                "get_object",
                Params={"Bucket": AWS_BUCKET_MODELS, "Key": s3_key},
                ExpiresIn=3600,
            )
        except Exception as e:
            print(f"[proxy-glb][mod] Failed to presign S3 URL: {e}")

    timeout = (5, 30)
    max_bytes = 100 * 1024 * 1024  # 100MB cap

    if request.method == "HEAD":
        try:
            r = requests.head(u, allow_redirects=True, timeout=timeout)
        except Exception:
            abort(502)
        headers = {
            "Content-Type": r.headers.get("Content-Type", "application/octet-stream"),
            "Access-Control-Allow-Origin": "*",
            "Cache-Control": "public, max-age=3600",
        }
        return Response(status=r.status_code, headers=headers)

    try:
        r = requests.get(u, stream=True, timeout=timeout)
    except Exception:
        abort(502)

    def gen():
        total = 0
        for chunk in r.iter_content(chunk_size=8192):
            if not chunk:
                continue
            total += len(chunk)
            if total > max_bytes:
                print(f"[proxy-glb][mod] Aborting stream: exceeded {max_bytes} bytes")
                break
            yield chunk

    headers = {
        "Content-Type": r.headers.get("Content-Type", "application/octet-stream"),
        "Access-Control-Allow-Origin": "*",
        "Cache-Control": "public, max-age=3600",
    }
    return Response(gen(), status=r.status_code, headers=headers)


@bp.route("/assets/<asset_type>/<asset_id>/download", methods=["GET", "OPTIONS"])
@with_session
def asset_download_mod(asset_type: str, asset_id: str):
    if request.method == "OPTIONS":
        return ("", 204)

    identity_id, auth_error = require_identity()
    if auth_error:
        return auth_error

    if asset_type not in ("model", "image", "history"):
        return jsonify({
            "ok": False,
            "error": {"code": "INVALID_ASSET_TYPE", "message": "asset_type must be 'model', 'image', or 'history'"},
        }), 400

    if not USE_DB:
        return jsonify({"ok": False, "error": {"code": "DB_UNAVAILABLE", "message": "Database not configured"}}), 503

    if not AWS_BUCKET_MODELS:
        return jsonify({"ok": False, "error": {"code": "S3_NOT_CONFIGURED", "message": "S3 storage not configured"}}), 503

    file_type = request.args.get("file")

    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                if asset_type == "model":
                    file_type = file_type or "glb"
                    if file_type == "glb":
                        url_col = "glb_url"
                    elif file_type == "thumbnail":
                        url_col = "thumbnail_url"
                    else:
                        return jsonify({
                            "ok": False,
                            "error": {"code": "INVALID_FILE_TYPE", "message": "file must be 'glb' or 'thumbnail' for models"},
                        }), 400

                    cur.execute(
                        f"""
                        SELECT {url_col} AS url, title
                        FROM timrx_app.models
                        WHERE id = %s AND identity_id = %s
                        """,
                        (asset_id, identity_id),
                    )

                elif asset_type == "image":
                    file_type = file_type or "original"
                    if file_type == "original":
                        url_col = "image_url"
                    elif file_type == "thumbnail":
                        url_col = "thumbnail_url"
                    else:
                        return jsonify({
                            "ok": False,
                            "error": {"code": "INVALID_FILE_TYPE", "message": "file must be 'original' or 'thumbnail' for images"},
                        }), 400

                    cur.execute(
                        f"""
                        SELECT {url_col} AS url, filename AS title
                        FROM timrx_app.images
                        WHERE id = %s AND identity_id = %s
                        """,
                        (asset_id, identity_id),
                    )

                else:
                    file_type = file_type or "glb"
                    if file_type == "glb":
                        url_col = "glb_url"
                    elif file_type == "thumbnail":
                        url_col = "thumbnail_url"
                    elif file_type == "image":
                        url_col = "image_url"
                    else:
                        return jsonify({
                            "ok": False,
                            "error": {"code": "INVALID_FILE_TYPE", "message": "file must be 'glb', 'thumbnail', or 'image' for history items"},
                        }), 400

                    cur.execute(
                        f"""
                        SELECT {url_col} AS url, title
                        FROM timrx_app.history_items
                        WHERE id = %s AND identity_id = %s
                        """,
                        (asset_id, identity_id),
                    )

                row = cur.fetchone()

        if not row:
            return jsonify({
                "ok": False,
                "error": {"code": "ASSET_NOT_FOUND", "message": "Asset not found or you don't have permission to access it"},
            }), 404

        url = row.get("url") if isinstance(row, dict) else row[0]
        name = row.get("title") if isinstance(row, dict) else row[1]

        if not url:
            return jsonify({
                "ok": False,
                "error": {"code": "FILE_NOT_FOUND", "message": f"No {file_type} file available for this asset"},
            }), 404

        s3_key = _extract_s3_key_from_url(url)
        if not s3_key:
            return jsonify({
                "ok": True,
                "download_url": url,
                "filename": name or "download",
                "expires_in": None,
                "note": "External URL, not signed",
                "source": "modular",
            })

        expires_in = 3600
        try:
            presigned_url = _s3.generate_presigned_url(
                "get_object",
                Params={"Bucket": AWS_BUCKET_MODELS, "Key": s3_key},
                ExpiresIn=expires_in,
            )
        except Exception as e:
            print(f"[DOWNLOAD][mod] Error generating presigned URL: {e}")
            return jsonify({
                "ok": False,
                "error": {"code": "S3_ERROR", "message": "Failed to generate download URL"},
            }), 500

        filename = name or s3_key.split("/")[-1]
        if file_type == "glb" and not filename.endswith(".glb"):
            filename = f"{filename}.glb"
        elif file_type == "thumbnail" and not any(filename.endswith(ext) for ext in (".png", ".jpg", ".jpeg", ".webp")):
            filename = f"{filename}.png"

        return jsonify({
            "ok": True,
            "download_url": presigned_url,
            "filename": filename,
            "expires_in": expires_in,
            "source": "modular",
        })

    except Exception as e:
        print(f"[DOWNLOAD][mod] Error: {e}")
        return jsonify({"ok": False, "error": {"code": "SERVER_ERROR", "message": str(e)}}), 500
