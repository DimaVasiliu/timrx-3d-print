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
    boundary_edges = None
    non_manifold_edges = None
    components = None
    try:
        import numpy as np

        edge_counts = np.bincount(mesh.edges_unique_inverse)
        boundary_edges = int((edge_counts == 1).sum())
        non_manifold_edges = int((edge_counts > 2).sum())
    except Exception:
        pass
    try:
        components = int(len(mesh.split(only_watertight=False)))
    except Exception:
        pass
    return {
        "vertices": int(len(vertices)),
        "faces": int(len(faces)),
        "is_watertight": bool(getattr(mesh, "is_watertight", False)),
        "is_winding_consistent": bool(getattr(mesh, "is_winding_consistent", False)),
        "boundary_edges": boundary_edges,
        "non_manifold_edges": non_manifold_edges,
        "components": components,
        "volume_cm3": round(volume_cm3, 3) if volume_cm3 is not None else None,
    }


def _repair_is_destructive(source, repaired) -> tuple[bool, str | None]:
    source_faces = int(len(getattr(source, "faces", [])))
    repaired_faces = int(len(getattr(repaired, "faces", [])))
    if source_faces <= 0 or repaired_faces <= 0:
        return True, "repair produced an empty mesh"

    face_ratio = repaired_faces / source_faces
    if face_ratio < StlRepairService.MIN_REPAIRED_FACE_RATIO:
        return (
            True,
            f"repair collapsed face count from {source_faces:,} to {repaired_faces:,}",
        )

    try:
        source_extents = list(map(float, source.extents))
        repaired_extents = list(map(float, repaired.extents))
        for source_extent, repaired_extent in zip(source_extents, repaired_extents):
            if source_extent <= 0:
                continue
            ratio = repaired_extent / source_extent
            if ratio < 0.55 or ratio > 1.8:
                return True, "repair changed model bounds too much"
    except Exception:
        pass

    return False, None


def _load_mesh(file_path: str, file_type: str | None):
    return PrintAnalysisService._load_mesh(file_path, file_type=file_type)


def _repair_with_pymeshfix(mesh):
    try:
        import pymeshfix
        import trimesh

        fixer = pymeshfix.MeshFix(mesh.vertices, mesh.faces)
        try:
            fixer.repair(joincomp=False, remove_smallest_components=False)
        except TypeError:
            fixer.repair()
        vertices = getattr(fixer, "points", None)
        faces = getattr(fixer, "faces", None)
        if vertices is None or faces is None:
            vertices = getattr(fixer, "v", None)
            faces = getattr(fixer, "f", None)
        if vertices is None or faces is None:
            raise RuntimeError("pymeshfix did not expose repaired vertices/faces")
        repaired = trimesh.Trimesh(vertices=vertices, faces=faces, process=True)
        destructive, reason = _repair_is_destructive(mesh, repaired)
        if destructive:
            logger.warning("[STL_REPAIR] pymeshfix result rejected: %s", reason)
            return None, None
        if len(repaired.faces) > 0:
            return repaired, "pymeshfix"
    except ImportError:
        return None, None
    except Exception as exc:
        logger.warning("[STL_REPAIR] pymeshfix failed, falling back to trimesh repair: %s", exc)
    return None, None


def _apply_pymeshlab_filter(ms, name: str, kwargs: Dict[str, Any]) -> None:
    try:
        ms.apply_filter(name, **kwargs)
    except TypeError:
        ms.apply_filter(name)
    except Exception as exc:
        logger.info("[STL_REPAIR] pymeshlab filter skipped %s: %s", name, exc)


def _final_manifold_cleanup(mesh):
    """Last-resort cleanup for meshes that come out of the main pipeline with a
    very small number of non-manifold edges still standing (typically 1-5).

    Strategy: use pymeshlab's `meshing_repair_non_manifold_edges` with method=0
    (remove the offending face — creates a tiny hole), then immediately
    `meshing_close_holes` to re-seal. This converges where split-vertex
    (method=1) won't, at the cost of replacing the bad triangle with a
    fresh patch. Detail loss is microscopic (a single triangle).

    Returns a trimesh.Trimesh if the cleanup improved manifoldness or kept the
    mesh structurally intact; None otherwise (caller keeps the pre-cleanup mesh).
    """
    try:
        import pymeshlab
        import trimesh
        import numpy as np

        ms = pymeshlab.MeshSet()
        ps_mesh = pymeshlab.Mesh(vertex_matrix=mesh.vertices, face_matrix=mesh.faces)
        ms.add_mesh(ps_mesh, "final-cleanup-input")

        cleanup_filters = [
            # Remove the offending face at each non-manifold edge.
            ("meshing_repair_non_manifold_edges", {"method": 0}),
            # Immediately re-close the tiny hole we just created.
            ("meshing_close_holes", {
                "maxholesize": 200,
                "selected": False,
                "newfaceselected": False,
                "selfintersection": True,
            }),
            # Tidy up any non-manifold vertices created in the process.
            ("meshing_repair_non_manifold_vertices", {}),
            ("meshing_remove_duplicate_faces", {}),
            ("meshing_remove_unreferenced_vertices", {}),
        ]
        for name, kwargs in cleanup_filters:
            _apply_pymeshlab_filter(ms, name, kwargs)

        current = ms.current_mesh()
        cleaned = trimesh.Trimesh(
            vertices=current.vertex_matrix(),
            faces=current.face_matrix(),
            process=True,
        )
        if len(cleaned.faces) == 0:
            return None
        # Safety: bail if the cleanup ate the mesh.
        if len(cleaned.faces) < len(mesh.faces) * 0.8:
            logger.warning(
                "[STL_REPAIR] final manifold cleanup rejected: collapsed %d -> %d faces",
                len(mesh.faces), len(cleaned.faces),
            )
            return None
        return cleaned
    except ImportError:
        return None
    except Exception as exc:
        logger.warning("[STL_REPAIR] final manifold cleanup failed: %s", exc)
        return None


def _stitch_components_with_pymeshlab(mesh):
    """Post-concatenation cleanup: welds component seams and eliminates the
    non-manifold edges that appear when independently-repaired components are
    concatenated. Uses split-vertex non-manifold repair (method=1) so we don't
    reopen any holes that the per-component pass already closed.
    Returns a trimesh.Trimesh on success, or None on failure (caller keeps the
    pre-stitch mesh).
    """
    try:
        import pymeshlab
        import trimesh

        ms = pymeshlab.MeshSet()
        ps_mesh = pymeshlab.Mesh(vertex_matrix=mesh.vertices, face_matrix=mesh.faces)
        ms.add_mesh(ps_mesh, "stitch-input")

        try:
            weld_threshold = pymeshlab.PercentageValue(0.0005)
        except Exception:
            try:
                weld_threshold = pymeshlab.Percentage(0.0005)
            except Exception:
                weld_threshold = 0.0005

        cleanup_filters = [
            # Merge near-duplicate vertices first — this stitches component
            # boundaries that ended up sharing the same edge after concatenation.
            ("meshing_merge_close_vertices", {"threshold": weld_threshold}),
            ("meshing_remove_duplicate_faces", {}),
            ("meshing_remove_null_faces", {}),
            ("meshing_remove_unreferenced_vertices", {}),
            # Split vertices at non-manifold edges (method=1) — preserves the
            # surface, no holes introduced.
            ("meshing_repair_non_manifold_edges", {"method": 1}),
            ("meshing_repair_non_manifold_vertices", {}),
            ("meshing_remove_unreferenced_vertices", {}),
        ]
        for name, kwargs in cleanup_filters:
            _apply_pymeshlab_filter(ms, name, kwargs)

        current = ms.current_mesh()
        cleaned = trimesh.Trimesh(
            vertices=current.vertex_matrix(),
            faces=current.face_matrix(),
            process=True,
        )
        if len(cleaned.faces) == 0:
            return None
        # Don't accept the stitch if it ate the mesh
        if len(cleaned.faces) < len(mesh.faces) * 0.5:
            logger.warning(
                "[STL_REPAIR] stitch cleanup rejected: collapsed %d -> %d faces",
                len(mesh.faces), len(cleaned.faces),
            )
            return None
        return cleaned
    except ImportError:
        return None
    except Exception as exc:
        logger.warning("[STL_REPAIR] stitch cleanup failed: %s", exc)
        return None


def _repair_with_pymeshlab(mesh):
    try:
        import pymeshlab
        import trimesh

        ms = pymeshlab.MeshSet()
        ps_mesh = pymeshlab.Mesh(vertex_matrix=mesh.vertices, face_matrix=mesh.faces)
        ms.add_mesh(ps_mesh, "repair-input")

        base_filters = [
            ("meshing_remove_duplicate_vertices", {}),
            ("meshing_remove_duplicate_faces", {}),
            ("meshing_remove_null_faces", {}),
            ("meshing_remove_unreferenced_vertices", {}),
        ]
        repair_filters = [
            ("meshing_repair_non_manifold_vertices", {}),
            ("meshing_repair_non_manifold_edges", {"method": 0}),
            ("meshing_close_holes", {
                "maxholesize": StlRepairService.PYMESHLAB_MAX_HOLE_SIZE,
                "selected": False,
                "newfaceselected": False,
                "selfintersection": True,
            }),
            ("meshing_repair_non_manifold_vertices", {}),
            ("meshing_repair_non_manifold_edges", {"method": 0}),
            ("meshing_remove_unreferenced_vertices", {}),
        ]

        for name, kwargs in base_filters:
            _apply_pymeshlab_filter(ms, name, kwargs)
        for _ in range(StlRepairService.PYMESHLAB_REPAIR_PASSES):
            for name, kwargs in repair_filters:
                _apply_pymeshlab_filter(ms, name, kwargs)

        current = ms.current_mesh()
        repaired = trimesh.Trimesh(
            vertices=current.vertex_matrix(),
            faces=current.face_matrix(),
            process=True,
        )
        destructive, reason = _repair_is_destructive(mesh, repaired)
        if destructive:
            logger.warning("[STL_REPAIR] pymeshlab result rejected: %s", reason)
            return None, None
        if len(repaired.faces) > 0:
            return repaired, "pymeshlab"
    except ImportError:
        return None, None
    except Exception as exc:
        logger.warning("[STL_REPAIR] pymeshlab failed, falling back: %s", exc)
    return None, None


def _solid_rebuild_with_pymeshlab(mesh):
    if not StlRepairService.ALLOW_SOLID_REBUILD:
        return None, None, []

    try:
        import pymeshlab
        import trimesh

        ms = pymeshlab.MeshSet()
        ps_mesh = pymeshlab.Mesh(vertex_matrix=mesh.vertices, face_matrix=mesh.faces)
        ms.add_mesh(ps_mesh, "solid-rebuild-input")

        try:
            cellsize = pymeshlab.Percentage(StlRepairService.SOLID_REBUILD_CELL_SIZE_PERCENT)
            offset = pymeshlab.Percentage(0.0)
        except Exception:
            cellsize = StlRepairService.SOLID_REBUILD_CELL_SIZE_PERCENT
            offset = 0.0

        filter_names = ("generate_resampled_uniform_mesh", "uniform_mesh_resampling")
        applied = False
        for filter_name in filter_names:
            try:
                ms.apply_filter(
                    filter_name,
                    cellsize=cellsize,
                    offset=offset,
                    mergeclosevert=True,
                    discretize=False,
                )
                applied = True
                break
            except TypeError:
                try:
                    ms.apply_filter(filter_name, cellsize=cellsize, offset=offset)
                    applied = True
                    break
                except Exception as exc:
                    logger.info("[STL_REPAIR] solid rebuild filter skipped %s: %s", filter_name, exc)
            except Exception as exc:
                logger.info("[STL_REPAIR] solid rebuild filter skipped %s: %s", filter_name, exc)

        if not applied:
            return None, None, ["Solid rebuild filter was unavailable in PyMeshLab."]

        cleanup_filters = [
            ("meshing_remove_duplicate_vertices", {}),
            ("meshing_remove_duplicate_faces", {}),
            ("meshing_remove_null_faces", {}),
            ("meshing_remove_unreferenced_vertices", {}),
            ("meshing_repair_non_manifold_vertices", {}),
            ("meshing_repair_non_manifold_edges", {"method": 0}),
            ("meshing_close_holes", {
                "maxholesize": StlRepairService.PYMESHLAB_MAX_HOLE_SIZE,
                "selected": False,
                "newfaceselected": False,
                "selfintersection": True,
            }),
        ]
        for name, kwargs in cleanup_filters:
            try:
                ms.apply_filter(name, **kwargs)
            except TypeError:
                ms.apply_filter(name)
            except Exception as exc:
                logger.info("[STL_REPAIR] solid rebuild cleanup skipped %s: %s", name, exc)

        current = ms.current_mesh()
        rebuilt = trimesh.Trimesh(
            vertices=current.vertex_matrix(),
            faces=current.face_matrix(),
            process=True,
        )

        try:
            source_extents = list(map(float, mesh.extents))
            rebuilt_extents = list(map(float, rebuilt.extents))
            for source_extent, rebuilt_extent in zip(source_extents, rebuilt_extents):
                if source_extent <= 0:
                    continue
                ratio = rebuilt_extent / source_extent
                if ratio < 0.45 or ratio > 2.2:
                    logger.warning("[STL_REPAIR] solid rebuild rejected: bounds changed too much")
                    return None, None, ["Solid rebuild changed model bounds too much."]
        except Exception:
            pass

        if len(rebuilt.faces) > 0:
            return rebuilt, "pymeshlab-solid-rebuild", [
                "Used solid rebuild fallback; fine surface detail may be reduced.",
            ]
    except ImportError:
        return None, None, []
    except Exception as exc:
        logger.warning("[STL_REPAIR] solid rebuild failed: %s", exc)
    return None, None, []


def _repair_components(mesh):
    import trimesh

    try:
        components = mesh.split(only_watertight=False)
    except Exception:
        return None, None, []

    if not components or len(components) <= 1:
        return None, None, []

    mesh_bounds = getattr(mesh, "bounds", None)
    try:
        diag = float(((mesh_bounds[1] - mesh_bounds[0]) ** 2).sum() ** 0.5) if mesh_bounds is not None else 0.0
    except Exception:
        diag = 0.0
    min_faces = max(12, StlRepairService.MIN_COMPONENT_FACES)
    min_extent = max(0.01, diag * StlRepairService.MIN_COMPONENT_EXTENT_RATIO) if diag > 0 else 0.01

    repaired_parts = []
    dropped = 0
    meshfix_parts = 0
    pymeshlab_parts = 0
    trimesh_parts = 0

    components = sorted(components, key=lambda part: int(len(getattr(part, "faces", []))), reverse=True)
    for index, part in enumerate(components):
        face_count = int(len(getattr(part, "faces", [])))
        try:
            extent = float(max(part.extents))
        except Exception:
            extent = 0.0

        if index > 0 and not bool(getattr(part, "is_watertight", False)) and (face_count < min_faces or extent < min_extent):
            dropped += 1
            continue

        fixed = None
        engine = None
        if face_count <= StlRepairService.PYMESHFIX_COMPONENT_FACE_LIMIT:
            fixed, engine = _repair_with_pymeshfix(part)
        if fixed is None and face_count <= StlRepairService.PYMESHLAB_COMPONENT_FACE_LIMIT:
            fixed, engine = _repair_with_pymeshlab(part)
        if fixed is None:
            fixed, engine = _repair_with_trimesh(part)

        if fixed is not None and len(fixed.faces) > 0:
            repaired_parts.append(fixed)
            if engine == "pymeshfix":
                meshfix_parts += 1
            elif engine == "pymeshlab":
                pymeshlab_parts += 1
            else:
                trimesh_parts += 1

    if not repaired_parts:
        return None, None, []

    try:
        repaired = trimesh.util.concatenate(repaired_parts)
        repaired.process(validate=True)
    except Exception:
        repaired = trimesh.util.concatenate(repaired_parts)

    warnings = []
    if dropped:
        warnings.append(f"Dropped {dropped} tiny open mesh fragment(s) during repair.")

    # Post-stitch: independently-repaired components, once concatenated, often
    # leave a small number of non-manifold edges at the seams. Run a cheap
    # pymeshlab weld + split-vertex pass to clean those up. Detail-preserving.
    stitched = _stitch_components_with_pymeshlab(repaired)
    if stitched is not None:
        repaired = stitched

    return repaired, f"components:pymeshfix={meshfix_parts},pymeshlab={pymeshlab_parts},trimesh={trimesh_parts}", warnings


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


def _repair_file(file_path: str, file_type: str | None, target_height_mm: float | None = None) -> Dict[str, Any]:
    mesh = _load_mesh(file_path, file_type=file_type)
    before = _mesh_report(mesh)
    warnings = []

    repaired, engine, component_warnings = _repair_components(mesh)
    warnings.extend(component_warnings)
    if repaired is None:
        max_meshfix_faces = StlRepairService.PYMESHFIX_FACE_LIMIT
        if before["faces"] <= max_meshfix_faces:
            repaired, engine = _repair_with_pymeshfix(mesh)
        else:
            warnings.append(
                f"MeshFix skipped because this model has {before['faces']:,} faces. "
                f"Use Remesh with a lower polygon target for aggressive repair."
            )
    if repaired is None and before["faces"] <= StlRepairService.PYMESHLAB_FACE_LIMIT:
        repaired, engine = _repair_with_pymeshlab(mesh)
    elif repaired is None:
        warnings.append(
            f"MeshLab skipped because this model has {before['faces']:,} faces, "
            f"above STL_REPAIR_MESHLAB_FACE_LIMIT={StlRepairService.PYMESHLAB_FACE_LIMIT:,}."
        )
    if repaired is None:
        repaired, engine = _repair_with_trimesh(mesh)

    after = _mesh_report(repaired)

    # Final aggressive pass: if we're left with no open holes but a small number
    # of stubborn non-manifold edges, run the remove-face + close-hole cleanup.
    # This catches the meshes where split-vertex repair couldn't converge.
    nm_after = int(after.get("non_manifold_edges") or 0)
    be_after = int(after.get("boundary_edges") or 0)
    if nm_after > 0 and be_after == 0 and nm_after <= 50:
        finalized = _final_manifold_cleanup(repaired)
        if finalized is not None:
            finalized_after = _mesh_report(finalized)
            finalized_nm = int(finalized_after.get("non_manifold_edges") or 0)
            finalized_be = int(finalized_after.get("boundary_edges") or 0)
            # Only adopt if it strictly improved manifoldness AND didn't
            # accidentally introduce open holes.
            if finalized_nm < nm_after and finalized_be <= be_after:
                repaired = finalized
                after = finalized_after
                engine = f"{engine}+final-cleanup"

    if not after["is_watertight"]:
        warnings.append("Fast repair completed, but the mesh is still not fully watertight.")
        rebuilt, rebuild_engine, rebuild_warnings = _solid_rebuild_with_pymeshlab(mesh)
        if rebuilt is not None:
            rebuilt_after = _mesh_report(rebuilt)
            if rebuilt_after["is_watertight"]:
                repaired = rebuilt
                engine = rebuild_engine
                after = rebuilt_after
                warnings.extend(rebuild_warnings)
            else:
                warnings.extend(rebuild_warnings)
                warnings.append("Solid rebuild also completed, but the mesh is still not fully watertight.")

    boundary_remaining = int(after.get("boundary_edges") or 0)
    non_manifold_remaining = int(after.get("non_manifold_edges") or 0)
    max_allowed_non_manifold = StlRepairService.MAX_NON_MANIFOLD_EDGES
    # Practical printability gate: most slicers (Cura, PrusaSlicer, Bambu, Orca)
    # tolerate a small number of non-manifold edges as long as there are no open
    # holes. Strict is_watertight is academically correct but rejects meshes that
    # would slice and print fine.
    printable_despite_non_watertight = (
        boundary_remaining == 0
        and non_manifold_remaining <= max_allowed_non_manifold
    )
    if not after["is_watertight"] and not printable_despite_non_watertight:
        return {
            "ok": False,
            "error": "STL repair could not close the mesh without damaging the model.",
            "engine": engine,
            "before": before,
            "after": after,
            "warnings": warnings,
            "suggestions": [
                "Use the slicer's repair tool for this model, or repair it in a dedicated service such as Formware.",
                "Try a lower-poly remesh before repairing if the model has many small disconnected details.",
            ],
        }
    if printable_despite_non_watertight and not after["is_watertight"]:
        warnings.append(
            f"Repair complete. Mesh has {non_manifold_remaining} non-manifold edge(s) "
            f"but no open holes — most slicers will accept it."
        )

    # Optional uniform scale so the exported STL matches the user's target print
    # height. Applied AFTER repair so topology metrics are unaffected. The
    # `after` report keeps the pre-scale dimensions; we add a scaled_dimensions
    # field separately so callers can show before/after.
    scaled_dimensions = None
    if target_height_mm and target_height_mm > 0:
        try:
            current_extents = list(map(float, repaired.extents))
            current_height = max(current_extents) if current_extents else 0.0
            if current_height > 1e-6:
                scale_factor = float(target_height_mm) / current_height
                # Clamp to a sane range to prevent absurd input from corrupting
                # the mesh. 0.001x .. 1000x covers any reasonable conversion.
                if 0.001 <= scale_factor <= 1000.0:
                    repaired.apply_scale(scale_factor)
                    new_extents = list(map(float, repaired.extents))
                    scaled_dimensions = {
                        "scale_factor": round(scale_factor, 6),
                        "width_mm": round(new_extents[0], 3) if len(new_extents) > 0 else None,
                        "height_mm": round(new_extents[1], 3) if len(new_extents) > 1 else None,
                        "depth_mm": round(new_extents[2], 3) if len(new_extents) > 2 else None,
                        "target_height_mm": float(target_height_mm),
                    }
                    warnings.append(
                        f"Scaled uniformly by {scale_factor:.4f}x so the longest axis is {target_height_mm:.1f} mm."
                    )
                else:
                    warnings.append(
                        f"Target height {target_height_mm:.2f} mm rejected: would require {scale_factor:.4f}x scale (out of safe range)."
                    )
        except Exception as exc:
            logger.warning("[STL_REPAIR] target_height_mm scaling failed: %s", exc)

    stl_bytes = repaired.export(file_type="stl")
    if isinstance(stl_bytes, str):
        stl_bytes = stl_bytes.encode("utf-8")

    return {
        "ok": True,
        "engine": engine,
        "before": before,
        "after": after,
        "warnings": warnings,
        "scaled_dimensions": scaled_dimensions,
        "stl_bytes": bytes(stl_bytes),
    }


def _repair_worker(conn, file_path: str, file_type: str | None, memory_mb: int, target_height_mm: float | None = None) -> None:
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
        conn.send(_repair_file(file_path, file_type=file_type, target_height_mm=target_height_mm))
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
    PYMESHFIX_FACE_LIMIT = _env_int("STL_REPAIR_MESHFIX_FACE_LIMIT", 900000, minimum=10000)
    PYMESHFIX_COMPONENT_FACE_LIMIT = _env_int("STL_REPAIR_MESHFIX_COMPONENT_FACE_LIMIT", 900000, minimum=1000)
    PYMESHLAB_FACE_LIMIT = _env_int("STL_REPAIR_MESHLAB_FACE_LIMIT", 900000, minimum=10000)
    PYMESHLAB_COMPONENT_FACE_LIMIT = _env_int("STL_REPAIR_MESHLAB_COMPONENT_FACE_LIMIT", 900000, minimum=1000)
    PYMESHLAB_MAX_HOLE_SIZE = _env_int("STL_REPAIR_MESHLAB_MAX_HOLE_SIZE", 5000, minimum=1)
    PYMESHLAB_REPAIR_PASSES = _env_int("STL_REPAIR_MESHLAB_REPAIR_PASSES", 3, minimum=1)
    ALLOW_SOLID_REBUILD = os.getenv("STL_REPAIR_ALLOW_SOLID_REBUILD", "true").lower() not in ("0", "false", "no")
    SOLID_REBUILD_CELL_SIZE_PERCENT = float(os.getenv("STL_REPAIR_SOLID_CELL_SIZE_PERCENT", "0.8") or "0.8")
    MIN_COMPONENT_FACES = _env_int("STL_REPAIR_MIN_COMPONENT_FACES", 18, minimum=1)
    MIN_COMPONENT_EXTENT_RATIO = float(os.getenv("STL_REPAIR_MIN_COMPONENT_EXTENT_RATIO", "0.0015") or "0.0015")
    MIN_REPAIRED_FACE_RATIO = float(os.getenv("STL_REPAIR_MIN_REPAIRED_FACE_RATIO", "0.35") or "0.35")
    # Practical printability: how many residual non-manifold edges we accept when
    # the mesh has no open holes. Slicers handle a small number trivially.
    MAX_NON_MANIFOLD_EDGES = _env_int("STL_REPAIR_MAX_NON_MANIFOLD_EDGES", 10, minimum=0)
    USE_SUBPROCESS = os.getenv("STL_REPAIR_SUBPROCESS", "true").lower() not in ("0", "false", "no")

    @staticmethod
    def _repair_file_safely(file_path: str, file_type: str | None, target_height_mm: float | None = None) -> Dict[str, Any]:
        if not StlRepairService.USE_SUBPROCESS:
            return _repair_file(file_path, file_type=file_type, target_height_mm=target_height_mm)

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
            args=(child_conn, file_path, file_type, StlRepairService.REPAIR_MEMORY_LIMIT_MB, target_height_mm),
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
    def repair_from_url(url: str, target_height_mm: float | None = None) -> Dict[str, Any]:
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

            return StlRepairService._repair_file_safely(tmp_path, file_type=file_type, target_height_mm=target_height_mm)
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
