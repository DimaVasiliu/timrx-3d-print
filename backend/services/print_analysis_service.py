"""
Print-readiness analysis service.
Analyzes 3D meshes for 3D printing compatibility.
Uses trimesh for geometry analysis.
"""

import logging
from typing import Dict, Any

logger = logging.getLogger(__name__)


class PrintAnalysisService:
    """Analyzes GLB/STL meshes for 3D printing readiness."""

    MAX_FACE_COUNT = 500_000     # Very high-poly models are slow to slice
    MIN_WALL_THICKNESS_MM = 0.8  # Minimum for FDM printing

    @staticmethod
    def analyze(file_path: str) -> Dict[str, Any]:
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
        import trimesh
        import numpy as np

        mesh = trimesh.load(file_path)

        # If it's a scene (GLB with multiple meshes), combine all meshes
        if isinstance(mesh, trimesh.Scene):
            geometries = [g for g in mesh.geometry.values() if isinstance(g, trimesh.Trimesh)]
            if not geometries:
                return {
                    "score": 0,
                    "is_printable": False,
                    "checks": {},
                    "issues": ["No valid mesh geometry found in file"],
                    "suggestions": ["Re-export the model as a single mesh"],
                }
            mesh = trimesh.util.concatenate(geometries)

        checks = {}
        issues = []
        suggestions = []
        score = 100

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
            return {
                "score": 0,
                "is_printable": False,
                "checks": {},
                "issues": [f"Could not download model: {e}"],
                "suggestions": [],
            }

        suffix = ".glb" if ".glb" in url.lower() else ".stl"
        with tempfile.NamedTemporaryFile(suffix=suffix, delete=True) as tmp:
            tmp.write(resp.content)
            tmp.flush()
            return PrintAnalysisService.analyze(tmp.name)
