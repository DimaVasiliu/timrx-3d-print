"""
/api/jobs routes - Job creation and management.

Handles:
- POST /api/jobs/create - Create a new job (reserve credits, dispatch to provider)
- GET /api/jobs/:id - Get job details
- GET /api/jobs - List jobs for current identity
- POST /api/jobs/:id/complete - Mark job as complete (user-initiated)
- POST /api/jobs/:id/cancel - Cancel a job (user: queued only, admin: any)
- POST /api/jobs/callback - Internal callback for existing pipeline (by upstream_job_id)
"""

from flask import Blueprint, request, jsonify, g

from backend.middleware import require_session
from backend.services.job_service import JobService, JobStatus, JobProvider
from backend.services.wallet_service import WalletService

bp = Blueprint("jobs", __name__)


@bp.route("/create", methods=["POST"])
@require_session
def create_job():
    """
    Create a new job.
    Reserves credits, dispatches to provider (Meshy/OpenAI), returns job_id.

    Request body:
    {
        "action_key": "text_to_3d_generate",
        "payload": {
            "prompt": "a cute robot",
            "art_style": "realistic",
            ...
        }
    }

    Response (success - 200):
    {
        "ok": true,
        "job_id": "uuid",
        "reservation_id": "uuid",
        "upstream_job_id": "meshy-task-id",
        "status": "pending",
        "provider": "meshy",
        "action_code": "MESHY_TEXT_TO_3D",
        "cost_credits": 20
    }

    Response (insufficient credits - 402):
    {
        "error": {
            "code": "INSUFFICIENT_CREDITS",
            "message": "Not enough credits for this action",
            "required": 20,
            "available": 10
        }
    }
    """
    data = request.get_json() or {}
    action_key = data.get("action_key")
    payload = data.get("payload") or {}

    # Validation
    if not action_key:
        return jsonify({
            "error": {
                "code": "VALIDATION_ERROR",
                "message": "action_key is required",
            }
        }), 400

    if not isinstance(payload, dict):
        return jsonify({
            "error": {
                "code": "VALIDATION_ERROR",
                "message": "payload must be an object",
            }
        }), 400

    try:
        result = JobService.create_job(
            identity_id=g.identity_id,
            action_key=action_key,
            payload=payload,
        )

        return jsonify({
            "ok": True,
            "job_id": result["job_id"],
            "reservation_id": result["reservation_id"],
            "upstream_job_id": result["upstream_job_id"],
            "status": result["status"],
            "provider": result["provider"],
            "action_code": result["action_code"],
            "cost_credits": result["cost_credits"],
        })

    except ValueError as e:
        error_msg = str(e)

        # Parse INSUFFICIENT_CREDITS error
        if "INSUFFICIENT_CREDITS" in error_msg:
            parts = error_msg.split(":")
            error_data = {}
            for part in parts[1:]:
                if "=" in part:
                    key, val = part.split("=", 1)
                    error_data[key] = int(val)

            return jsonify({
                "error": {
                    "code": "INSUFFICIENT_CREDITS",
                    "message": "Not enough credits for this action",
                    "required": error_data.get("required", 0),
                    "balance": error_data.get("balance", 0),
                    "available": error_data.get("available", 0),
                }
            }), 402

        # Unknown action
        if "Unknown action" in error_msg:
            return jsonify({
                "error": {
                    "code": "INVALID_ACTION",
                    "message": error_msg,
                }
            }), 400

        # No cost defined
        if "No cost defined" in error_msg:
            return jsonify({
                "error": {
                    "code": "INVALID_ACTION",
                    "message": error_msg,
                }
            }), 400

        # Provider dispatch failed
        if "Provider dispatch failed" in error_msg:
            return jsonify({
                "error": {
                    "code": "PROVIDER_ERROR",
                    "message": error_msg,
                }
            }), 502

        # Wallet not found
        if "Wallet not found" in error_msg:
            return jsonify({
                "error": {
                    "code": "WALLET_NOT_FOUND",
                    "message": "User wallet not initialized",
                }
            }), 400

        # Generic error
        return jsonify({
            "error": {
                "code": "JOB_ERROR",
                "message": error_msg,
            }
        }), 400

    except Exception as e:
        print(f"[JOBS] Error creating job: {e}")
        return jsonify({
            "error": {
                "code": "INTERNAL_ERROR",
                "message": "Failed to create job",
            }
        }), 500


@bp.route("/<job_id>", methods=["GET"])
@require_session
def get_job(job_id):
    """
    Get job details by ID.
    Only returns jobs belonging to the current identity.

    Response (success - 200):
    {
        "ok": true,
        "job_id": "uuid",
        "status": "queued|processing|ready|failed",
        "progress": 0-100,
        "error_message": null or "...",
        "model_id": null or "uuid",
        "image_id": null or "uuid",
        "glb_url": null or "https://...",
        "image_url": null or "https://..."
    }
    """
    try:
        job = JobService.get_job(job_id)

        if not job:
            return jsonify({
                "error": {
                    "code": "NOT_FOUND",
                    "message": "Job not found",
                }
            }), 404

        # Verify ownership
        if job.get("identity_id") != g.identity_id:
            return jsonify({
                "error": {
                    "code": "NOT_FOUND",
                    "message": "Job not found",
                }
            }), 404

        # Map internal status to frontend-expected status
        # Internal: queued, pending, succeeded, failed
        # Frontend: queued, processing, ready, failed
        internal_status = job.get("status", "queued")
        status_map = {
            "queued": "queued",
            "pending": "processing",
            "succeeded": "ready",
            "failed": "failed",
        }
        status = status_map.get(internal_status, internal_status)

        # Extract additional fields from meta
        meta = job.get("meta") or {}
        progress = meta.get("progress", 0)
        model_id = meta.get("model_id")
        image_id = meta.get("image_id")
        glb_url = meta.get("glb_url")
        image_url = meta.get("image_url")

        # Set progress to 100 if job is ready
        if status == "ready":
            progress = 100

        return jsonify({
            "ok": True,
            "job_id": job.get("id"),
            "status": status,
            "progress": progress,
            "error_message": job.get("error_message"),
            "model_id": model_id,
            "image_id": image_id,
            "glb_url": glb_url,
            "image_url": image_url,
        })

    except Exception as e:
        print(f"[JOBS] Error fetching job: {e}")
        return jsonify({
            "error": {
                "code": "INTERNAL_ERROR",
                "message": "Failed to fetch job",
            }
        }), 500


@bp.route("", methods=["GET"])
@require_session
def list_jobs():
    """
    List jobs for the current identity.

    Query params:
    - limit: Max entries to return (default 50, max 100)
    - offset: Pagination offset (default 0)
    - status: Filter by status (optional)

    Response:
    {
        "ok": true,
        "jobs": [...],
        "limit": 50,
        "offset": 0
    }
    """
    try:
        limit = min(request.args.get("limit", 50, type=int), 100)
        offset = request.args.get("offset", 0, type=int)
        status = request.args.get("status")

        jobs = JobService.get_jobs_for_identity(
            g.identity_id,
            limit=limit,
            offset=offset,
            status=status,
        )

        return jsonify({
            "ok": True,
            "jobs": jobs,
            "limit": limit,
            "offset": offset,
        })

    except Exception as e:
        print(f"[JOBS] Error listing jobs: {e}")
        return jsonify({
            "ok": True,
            "jobs": [],
            "limit": 50,
            "offset": 0,
        })


@bp.route("/<job_id>/complete", methods=["POST"])
@require_session
def complete_job(job_id):
    """
    Mark a job as complete (user-initiated).
    Handles reservation finalization (success) or release (failure).

    IDEMPOTENT: Safe to call multiple times with same success value.

    Request body:
    {
        "success": true,           // Required: whether job succeeded
        "error_message": "..."     // Optional: error message if failed
    }

    Response (success - 200):
    {
        "ok": true,
        "job": {...},
        "was_already_completed": false,
        "wallet": {
            "balance": 80,
            "reserved": 0,
            "available": 80
        }
    }
    """
    data = request.get_json() or {}
    success = data.get("success", True)
    error_message = data.get("error_message")

    try:
        # Get job first to verify ownership
        job = JobService.get_job(job_id)

        if not job:
            return jsonify({
                "error": {
                    "code": "NOT_FOUND",
                    "message": "Job not found",
                }
            }), 404

        # Verify ownership
        if job.get("identity_id") != g.identity_id:
            return jsonify({
                "error": {
                    "code": "NOT_FOUND",
                    "message": "Job not found",
                }
            }), 404

        # Complete the job (idempotent)
        result = JobService.complete_job(
            job_id=job_id,
            success=success,
            error_message=error_message,
        )

        # Get updated wallet balance
        wallet = WalletService.get_wallet(g.identity_id)
        balance = wallet.get("balance_credits", 0) if wallet else 0
        reserved = WalletService.get_reserved_credits(g.identity_id)
        available = max(0, balance - reserved)

        return jsonify({
            "ok": True,
            "job": result["job"],
            "was_already_completed": result["was_already_completed"],
            "wallet": {
                "balance": balance,
                "reserved": reserved,
                "available": available,
            },
        })

    except ValueError as e:
        error_msg = str(e)

        # Status conflict error
        if "already completed" in error_msg.lower():
            return jsonify({
                "error": {
                    "code": "STATUS_CONFLICT",
                    "message": error_msg,
                }
            }), 409

        return jsonify({
            "error": {
                "code": "JOB_ERROR",
                "message": error_msg,
            }
        }), 400

    except Exception as e:
        print(f"[JOBS] Error completing job: {e}")
        return jsonify({
            "error": {
                "code": "INTERNAL_ERROR",
                "message": "Failed to complete job",
            }
        }), 500


@bp.route("/<job_id>/cancel", methods=["POST"])
@require_session
def cancel_job(job_id):
    """
    Cancel a job and release its credit reservation.

    Users can cancel their own jobs if status is 'queued'.
    Admins can force cancel any job (including 'pending') by setting force=true.

    Request body:
    {
        "reason": "user_cancelled",  // Optional: cancellation reason
        "force": false               // Optional: force cancel (admin only)
    }

    Response (success - 200):
    {
        "ok": true,
        "job": {...},
        "credits_returned": 20,
        "wallet": {
            "balance": 100,
            "reserved": 0,
            "available": 100
        }
    }

    Response (not cancellable - 400):
    {
        "error": {
            "code": "NOT_CANCELLABLE",
            "message": "Job is in progress. Only admins can force cancel."
        }
    }
    """
    data = request.get_json() or {}
    reason = data.get("reason", "user_cancelled")
    force = data.get("force", False)

    try:
        # Get job first to verify ownership
        job = JobService.get_job(job_id)

        if not job:
            return jsonify({
                "error": {
                    "code": "NOT_FOUND",
                    "message": "Job not found",
                }
            }), 404

        # Verify ownership
        if job.get("identity_id") != g.identity_id:
            return jsonify({
                "error": {
                    "code": "NOT_FOUND",
                    "message": "Job not found",
                }
            }), 404

        # Check if user is admin (for force cancel)
        # For now, we check if identity has admin flag or if force is requested
        # TODO: Implement proper admin check via g.is_admin or similar
        is_admin = getattr(g, 'is_admin', False)

        # If force requested but not admin, deny
        if force and not is_admin:
            return jsonify({
                "error": {
                    "code": "FORBIDDEN",
                    "message": "Only admins can force cancel jobs",
                }
            }), 403

        # Check if cancellable (pre-flight check for better error messages)
        can_cancel, cancel_reason = JobService.is_cancellable(job, is_admin=is_admin or force)
        if not can_cancel:
            return jsonify({
                "error": {
                    "code": "NOT_CANCELLABLE",
                    "message": cancel_reason,
                }
            }), 400

        # Cancel the job
        result = JobService.cancel_job(
            job_id=job_id,
            reason=reason,
            force=force,
        )

        # Get updated wallet balance
        wallet = WalletService.get_wallet(g.identity_id)
        balance = wallet.get("balance_credits", 0) if wallet else 0
        reserved = WalletService.get_reserved_credits(g.identity_id)
        available = max(0, balance - reserved)

        return jsonify({
            "ok": True,
            "job": result["job"],
            "credits_returned": result["credits_returned"],
            "wallet": {
                "balance": balance,
                "reserved": reserved,
                "available": available,
            },
        })

    except ValueError as e:
        error_msg = str(e)

        if "Cannot cancel" in error_msg:
            return jsonify({
                "error": {
                    "code": "NOT_CANCELLABLE",
                    "message": error_msg,
                }
            }), 400

        return jsonify({
            "error": {
                "code": "JOB_ERROR",
                "message": error_msg,
            }
        }), 400

    except Exception as e:
        print(f"[JOBS] Error cancelling job: {e}")
        return jsonify({
            "error": {
                "code": "INTERNAL_ERROR",
                "message": "Failed to cancel job",
            }
        }), 500


@bp.route("/callback", methods=["POST"])
def job_callback():
    """
    Internal callback for existing pipeline to complete jobs by upstream_job_id.
    Called when Meshy/OpenAI job finishes (from status polling or webhook).

    NO AUTH REQUIRED - This is an internal endpoint called by the existing pipeline.
    Should be protected at network level (internal only) or via shared secret.

    IDEMPOTENT: Safe to call multiple times.

    Request body:
    {
        "provider": "meshy",           // Required: 'meshy' or 'openai'
        "upstream_job_id": "task-123", // Required: provider's job ID
        "success": true,               // Required: whether job succeeded
        "error_message": "..."         // Optional: error message if failed
    }

    Response (success - 200):
    {
        "ok": true,
        "job": {...},              // null if no matching job found
        "was_already_completed": false,
        "found": true              // whether a matching job was found
    }
    """
    data = request.get_json() or {}
    provider = data.get("provider", "").strip().lower()
    upstream_job_id = data.get("upstream_job_id", "").strip()
    success = data.get("success", True)
    error_message = data.get("error_message")

    # Validation
    if not provider:
        return jsonify({
            "error": {
                "code": "VALIDATION_ERROR",
                "message": "provider is required",
            }
        }), 400

    if provider not in [JobProvider.MESHY, JobProvider.OPENAI]:
        return jsonify({
            "error": {
                "code": "VALIDATION_ERROR",
                "message": f"Invalid provider: {provider}. Must be 'meshy' or 'openai'",
            }
        }), 400

    if not upstream_job_id:
        return jsonify({
            "error": {
                "code": "VALIDATION_ERROR",
                "message": "upstream_job_id is required",
            }
        }), 400

    try:
        result = JobService.complete_job_by_upstream_id(
            provider=provider,
            upstream_job_id=upstream_job_id,
            success=success,
            error_message=error_message,
        )

        if result is None:
            # No matching job found - this is OK for legacy jobs
            return jsonify({
                "ok": True,
                "job": None,
                "was_already_completed": False,
                "found": False,
            })

        return jsonify({
            "ok": True,
            "job": result["job"],
            "was_already_completed": result["was_already_completed"],
            "found": True,
        })

    except ValueError as e:
        error_msg = str(e)

        # Status conflict error
        if "already completed" in error_msg.lower():
            return jsonify({
                "error": {
                    "code": "STATUS_CONFLICT",
                    "message": error_msg,
                }
            }), 409

        return jsonify({
            "error": {
                "code": "JOB_ERROR",
                "message": error_msg,
            }
        }), 400

    except Exception as e:
        print(f"[JOBS] Error in callback: {e}")
        return jsonify({
            "error": {
                "code": "INTERNAL_ERROR",
                "message": "Failed to process callback",
            }
        }), 500
