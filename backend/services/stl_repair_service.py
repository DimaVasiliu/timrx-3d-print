"""
STL repair service.

Downloads an owned GLB/STL/OBJ, repairs mesh topology in an isolated child
process, and returns binary STL bytes plus a compact before/after report.
"""

from __future__ import annotations

import gc
import io
import logging
import os
from typing import Any, Dict

from backend.services.print_analysis_service import PrintAnalysisService

logger = logging.getLogger(__name__)


def _env_int(name: str, default: int, minimum: int | None = None) -> int:
    raw = os.getenv(name)
    if raw is None or str(raw).strip() == "":
        return default
    try:
        value = int(str(raw).strip())
    except (TypeError, ValueError):
        logger.warning("[STL_REPAIR] Invalid %s=%r; using default %s", name, raw, default)
        return default
    if minimum is not None and value < minimum:
        logger.warning("[STL_REPAIR] %s=%s is below minimum %s; using %s", name, value, minimum, minimum)
        return minimum
    return value


def _mesh_report(mesh) -> Dict[str, Any]:
    try:
        volume_cm3 = float(mesh.volume) / 1000.0 if bool(mesh.is_watertight) else None
    except Exception:
        volume_cm3 = None
    vertices = getattr(mesh, "vertices", [])
    faces = getattr(mesh, "faces", [])
    return {
        "vertices": int(len(vertices)),
        "faces": int(len(faces)),
        "is_watertight": bool(getattr(mesh, "is_watertight", False)),
        "is_winding_consistent": bool(getattr(mesh, "is_winding_consistent", False)),
        "volume_cm3": round(volume_cm3, 3) if volume_cm3 is not None else None,
    }


def _load_mesh(file_path: str, file_type: str | None):
    return PrintAnalysisService._load_mesh(file_path, file_type=file_type)


def _repair_with_pymeshfix(mesh):
    try:
        import pymeshfix
        import trimesh

        fixer = pymeshfix.MeshFix(mesh.vertices, mesh.faces)
        try:
            fixer.repair(joincomp=True, remove_smallest_components=False)
        except TypeError:
            fixer.repair()
        repaired = trimesh.Trimesh(vertices=fixer.v, faces=fixer.f, process=True)
        if len(repaired.faces) > 0:
            return repaired, "pymeshfix"
    except ImportError:
        return None, None
    except Exception as exc:
        logger.warning("[STL_REPAIR] pymeshfix failed, falling back to trimesh repair: %s", exc)
    return None, None


def _repair_with_trimesh(mesh):
    import trimesh

    repaired = mesh.copy()
    try:
        repaired.remove_duplicate_faces()
    except Exception:
        pass
    try:
        repaired.remove_degenerate_faces()
    except Exception:
        pass
    try:
        repaired.remove_unreferenced_vertices()
    except Exception:
        pass

    try:
        trimesh.repair.fix_winding(repaired)
    except Exception:
        pass
    try:
        trimesh.repair.fix_normals(repaired)
    except Exception:
        pass
    try:
        trimesh.repair.fill_holes(repaired)
    except Exception:
        pass
    try:
        trimesh.repair.fix_inversion(repaired)
    except Exception:
        pass
    try:
        repaired.process(validate=True)
    except Exception:
        pass
    return repaired, "trimesh"


def _repair_file(file_path: str, file_type: str | None) -> Dict[str, Any]:
    mesh = _load_mesh(file_path, file_type=file_type)
    before = _mesh_report(mesh)
    warnings = []

    repaired = None
    engine = None
    max_meshfix_faces = StlRepairService.PYMESHFIX_FACE_LIMIT
    if before["faces"] <= max_meshfix_faces:
        repaired, engine = _repair_with_pymeshfix(mesh)
    else:
        warnings.append(
            f"MeshFix skipped because this model has {before['faces']:,} faces. "
            f"Use Remesh with a lower polygon target for aggressive repair."
        )
    if repaired is None:
        repaired, engine = _repair_with_trimesh(mesh)

    after = _mesh_report(repaired)
    if not after["is_watertight"]:
        warnings.append("Fast repair completed, but the mesh is still not fully watertight.")
    stl_bytes = repaired.export(file_type="stl")
    if isinstance(stl_bytes, str):
        stl_bytes = stl_bytes.encode("utf-8")

    return {
        "ok": True,
        "engine": engine,
        "before": before,
        "after": after,
        "warnings": warnings,
        "stl_bytes": bytes(stl_bytes),
    }


def _repair_worker(conn, file_path: str, file_type: str | None, memory_mb: int) -> None:
    try:
        os.environ.setdefault("OMP_NUM_THREADS", "1")
        os.environ.setdefault("OPENBLAS_NUM_THREADS", "1")
        os.environ.setdefault("MKL_NUM_THREADS", "1")
        os.environ.setdefault("NUMEXPR_NUM_THREADS", "1")
        try:
            from backend.services.print_analysis_service import _set_analysis_child_limits

            _set_analysis_child_limits(memory_mb)
        except Exception:
            pass
        conn.send(_repair_file(file_path, file_type=file_type))
    except MemoryError:
        conn.send({
            "ok": False,
            "error": "Model is too complex for the STL repair memory budget.",
        })
    except BaseException as exc:
        logger.exception("[STL_REPAIR] Child repair failed")
        conn.send({"ok": False, "error": f"STL repair failed: {exc}"})
    finally:
        try:
            conn.close()
        except Exception:
            pass
        gc.collect()


class StlRepairService:
    MAX_DOWNLOAD_BYTES = _env_int("STL_REPAIR_MAX_DOWNLOAD_MB", 30, minimum=1) * 1024 * 1024
    REPAIR_TIMEOUT = _env_int("STL_REPAIR_TIMEOUT_SECONDS", 90, minimum=30)
    REPAIR_MEMORY_LIMIT_MB = _env_int("STL_REPAIR_MEMORY_MB", 1800, minimum=0)
    PYMESHFIX_FACE_LIMIT = _env_int("STL_REPAIR_MESHFIX_FACE_LIMIT", 180000, minimum=10000)
    USE_SUBPROCESS = os.getenv("STL_REPAIR_SUBPROCESS", "true").lower() not in ("0", "false", "no")

    @staticmethod
    def _repair_file_safely(file_path: str, file_type: str | None) -> Dict[str, Any]:
        if not StlRepairService.USE_SUBPROCESS:
            return _repair_file(file_path, file_type=file_type)

        import multiprocessing as mp
        import time

        start_method = os.getenv("STL_REPAIR_MP_START_METHOD", "spawn")
        try:
            ctx = mp.get_context(start_method)
        except ValueError:
            ctx = mp.get_context("spawn")

        parent_conn, child_conn = ctx.Pipe(duplex=False)
        proc = ctx.Process(
            target=_repair_worker,
            args=(child_conn, file_path, file_type, StlRepairService.REPAIR_MEMORY_LIMIT_MB),
            daemon=True,
        )

        started = time.monotonic()
        proc.start()
        child_conn.close()
        payload = None
        try:
            if parent_conn.poll(StlRepairService.REPAIR_TIMEOUT):
                payload = parent_conn.recv()
            else:
                logger.warning("[STL_REPAIR] Timed out after %ss; terminating child", StlRepairService.REPAIR_TIMEOUT)
                proc.terminate()
        except EOFError:
            payload = None
        finally:
            proc.join(timeout=3)
            if proc.is_alive():
                proc.kill()
                proc.join(timeout=2)
            parent_conn.close()

        elapsed = time.monotonic() - started
        if not payload:
            return {
                "ok": False,
                "error": "STL repair exceeded the safe memory or time budget for this server.",
                "suggestions": [
                    "Run Remesh with a lower polygon target, then try STL Repair again.",
                    "For high-detail AI geometry, use Bambu Studio's built-in repair after import.",
                ],
                "repair_runtime_seconds": round(elapsed, 2),
            }
        payload["repair_runtime_seconds"] = round(elapsed, 2)
        payload["repair_mode"] = "isolated"
        payload["repair_memory_limit_mb"] = StlRepairService.REPAIR_MEMORY_LIMIT_MB
        return payload

    @staticmethod
    def repair_from_url(url: str) -> Dict[str, Any]:
        import requests
        import tempfile

        url_error = PrintAnalysisService._validate_url(url)
        if url_error:
            return {"ok": False, "error": f"Invalid model URL: {url_error}"}

        tmp_path = None
        try:
            resp = requests.get(url, timeout=30, stream=True)
            resp.raise_for_status()

            content_length = int(resp.headers.get("content-length", 0))
            if content_length > StlRepairService.MAX_DOWNLOAD_BYTES:
                return {
                    "ok": False,
                    "error": (
                        f"Model file too large ({content_length / 1024 / 1024:.0f} MB). "
                        f"Maximum is {StlRepairService.MAX_DOWNLOAD_BYTES / 1024 / 1024:.0f} MB."
                    ),
                }

            file_type = PrintAnalysisService._detect_file_type(url, resp.headers.get("content-type"), b"")
            suffix = f".{file_type}" if file_type else ".bin"
            tmp = tempfile.NamedTemporaryFile(suffix=suffix, delete=False)
            tmp_path = tmp.name
            downloaded = 0
            head = b""
            for chunk in resp.iter_content(chunk_size=1024 * 1024):
                if not chunk:
                    continue
                if not head:
                    head = chunk[:16]
                downloaded += len(chunk)
                if downloaded > StlRepairService.MAX_DOWNLOAD_BYTES:
                    tmp.close()
                    return {
                        "ok": False,
                        "error": f"Model file exceeds {StlRepairService.MAX_DOWNLOAD_BYTES / 1024 / 1024:.0f} MB limit.",
                    }
                tmp.write(chunk)
            tmp.close()

            if not file_type:
                file_type = PrintAnalysisService._detect_file_type(url, resp.headers.get("content-type"), head)

            return StlRepairService._repair_file_safely(tmp_path, file_type=file_type)
        except requests.RequestException as exc:
            return {"ok": False, "error": f"Could not download model: {exc}"}
        except Exception as exc:
            logger.exception("[STL_REPAIR] Unexpected repair_from_url error")
            return {"ok": False, "error": f"STL repair failed: {exc}"}
        finally:
            if tmp_path:
                try:
                    os.unlink(tmp_path)
                except OSError:
                    pass
            gc.collect()

    @staticmethod
    def repair_from_bytes(data: bytes, file_type: str | None = None) -> Dict[str, Any]:
        import tempfile

        if len(data) > StlRepairService.MAX_DOWNLOAD_BYTES:
            return {
                "ok": False,
                "error": f"Model file exceeds {StlRepairService.MAX_DOWNLOAD_BYTES / 1024 / 1024:.0f} MB limit.",
            }

        tmp_path = None
        try:
            if not file_type:
                file_type = PrintAnalysisService._detect_file_type("", None, data[:16])
            suffix = f".{file_type}" if file_type else ".bin"
            tmp = tempfile.NamedTemporaryFile(suffix=suffix, delete=False)
            tmp_path = tmp.name
            tmp.write(data)
            tmp.close()
            return StlRepairService._repair_file_safely(tmp_path, file_type=file_type)
        finally:
            if tmp_path:
                try:
                    os.unlink(tmp_path)
                except OSError:
                    pass
