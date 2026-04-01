"""
Print-readiness analysis service.
Analyzes 3D meshes for 3D printing compatibility.
Uses trimesh for geometry analysis.
"""

import logging
from urllib.parse import urlparse
from typing import Dict, Any

logger = logging.getLogger(__name__)


class PrintAnalysisService:
    """Analyzes GLB/STL meshes for 3D printing readiness."""

    MAX_FACE_COUNT = 500_000     # Very high-poly models are slow to slice
    MIN_WALL_THICKNESS_MM = 0.8  # Minimum for FDM printing

    @staticmethod
    def _failed_result(issue: str, suggestions: list[str] | None = None) -> Dict[str, Any]:
        return {
            "score": 0,
            "is_printable": False,
            "checks": {},
            "issues": [issue],
            "suggestions": suggestions or [],
        }

    @staticmethod
    def _detect_file_type(url: str, content_type: str | None, content: bytes) -> str | None:
        path = (urlparse(url).path or "").lower()
        content_type = (content_type or "").lower()
        head = content[:16]
        stripped = content.lstrip()[:16].lower()

        if path.endswith(".stl") or "model/stl" in content_type or stripped.startswith(b"solid"):
            return "stl"
        if path.endswith(".glb") or "model/gltf-binary" in content_type or head.startswith(b"glTF"):
            return "glb"
        if path.endswith(".gltf") or "model/gltf+json" in content_type:
            return "gltf"
        if path.endswith(".obj") or "text/plain" in content_type:
            return "obj"
        return None

    @staticmethod
    def _load_mesh(file_path: str, file_type: str | None = None):
        import trimesh

        loaded = trimesh.load(
            file_path,
            file_type=file_type,
            force="scene",
            process=False,
            skip_materials=True,
        )

        if isinstance(loaded, trimesh.Trimesh):
            return loaded

        if not isinstance(loaded, trimesh.Scene):
            raise TypeError(f"Unsupported geometry type: {type(loaded).__name__}")

        meshes = []
        for geometry in loaded.geometry.values():
            if not isinstance(geometry, trimesh.Trimesh):
                continue
            if geometry.is_empty or len(geometry.vertices) == 0 or len(geometry.faces) == 0:
                continue
            meshes.append(geometry.copy())

        if not meshes:
            raise ValueError("No valid mesh geometry found in file")

        if len(meshes) == 1:
            return meshes[0]

        try:
            return trimesh.util.concatenate(meshes)
        except Exception as exc:
            logger.warning("[PRINT_ANALYSIS] Mesh concatenate failed, using largest part: %s", exc)
            return max(meshes, key=lambda mesh: int(len(mesh.faces)))

    @staticmethod
    def analyze(file_path: str, file_type: str | None = None) -> Dict[str, Any]:
        """
        Analyze a mesh file for 3D printing readiness.

        Returns:
            {
                "score": 0-100,
                "is_printable": bool,
                "checks": { ... },
                "issues": ["..."],
                "suggestions": ["..."],
            }
        """
        import numpy as np

        try:
            mesh = PrintAnalysisService._load_mesh(file_path, file_type=file_type)
        except Exception as exc:
            logger.warning("[PRINT_ANALYSIS] Failed to parse mesh %s: %s", file_path, exc)
            return PrintAnalysisService._failed_result(
                f"Could not parse model geometry: {exc}",
                [
                    "Try Remesh before running the print check again",
                    "If available, export or download the model as STL for print analysis",
                ],
            )

        checks = {}
        issues = []
        suggestions = []
        score = 100

        try:
            # 1. Manifold check (watertight)
            checks["is_manifold"] = bool(mesh.is_watertight)
            if not mesh.is_watertight:
                score -= 30
                issues.append("Mesh is not watertight (has holes or open edges)")
                suggestions.append("Use Remesh to repair the mesh before printing")

            # 2. Face count
            checks["face_count"] = int(len(mesh.faces))
            checks["face_count_ok"] = len(mesh.faces) <= PrintAnalysisService.MAX_FACE_COUNT
            if not checks["face_count_ok"]:
                score -= 10
                issues.append(f"High polygon count ({len(mesh.faces):,} faces) may slow slicing software")
                suggestions.append("Use Remesh to reduce polygon count")

            # 3. Degenerate faces
            areas = mesh.area_faces
            degenerate_count = int(np.sum(areas < 1e-10))
            checks["has_degenerate_faces"] = degenerate_count > 0
            checks["degenerate_face_count"] = degenerate_count
            if degenerate_count > 0:
                score -= 10
                issues.append(f"{degenerate_count} degenerate (zero-area) faces found")

            # 4. Volume check (positive = proper normals)
            if mesh.is_watertight:
                checks["is_volume_positive"] = bool(mesh.volume > 0)
                if mesh.volume <= 0:
                    score -= 20
                    issues.append("Mesh normals may be inverted (negative volume)")
                    suggestions.append("Flip normals before printing")
            else:
                checks["is_volume_positive"] = None

            # 5. Bounding box
            extents = mesh.bounding_box.extents
            checks["bounding_box_mm"] = [round(float(x), 2) for x in extents]

            # 6. Volume estimate
            if mesh.is_watertight and mesh.volume > 0:
                checks["estimated_volume_cm3"] = round(float(mesh.volume / 1000), 2)
            else:
                checks["estimated_volume_cm3"] = None
        except Exception as exc:
            logger.exception("[PRINT_ANALYSIS] Mesh analysis failed for %s", file_path)
            return PrintAnalysisService._failed_result(
                f"Could not analyze model geometry: {exc}",
                [
                    "Try Remesh to simplify and repair the mesh",
                    "If the problem persists, export the model as STL and retry the print check",
                ],
            )

        score = max(0, min(100, score))

        return {
            "score": score,
            "is_printable": score >= 70,
            "checks": checks,
            "issues": issues,
            "suggestions": suggestions,
        }

    @staticmethod
    def analyze_from_url(url: str) -> Dict[str, Any]:
        """Download a GLB/STL from URL and analyze it."""
        import tempfile
        import requests

        try:
            resp = requests.get(url, timeout=30)
            resp.raise_for_status()
        except Exception as e:
            logger.error("[PRINT_ANALYSIS] Failed to download model: %s", e)
            return PrintAnalysisService._failed_result(f"Could not download model: {e}")

        file_type = PrintAnalysisService._detect_file_type(
            url,
            resp.headers.get("content-type"),
            resp.content,
        )
        suffix = f".{file_type}" if file_type else ".bin"
        with tempfile.NamedTemporaryFile(suffix=suffix, delete=True) as tmp:
            tmp.write(resp.content)
            tmp.flush()
            return PrintAnalysisService.analyze(tmp.name, file_type=file_type)
