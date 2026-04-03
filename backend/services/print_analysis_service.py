"""
Print-readiness analysis service.
Analyzes 3D meshes for 3D printing compatibility.
Uses trimesh for geometry analysis.
"""

import logging
from urllib.parse import urlparse
from typing import Dict, Any

from backend.config import AWS_BUCKET_MODELS, AWS_REGION

logger = logging.getLogger(__name__)


class PrintAnalysisService:
    """Analyzes GLB/STL meshes for 3D printing readiness."""

    MAX_FACE_COUNT = 500_000     # Very high-poly models are slow to slice
    MIN_WALL_THICKNESS_MM = 0.8  # Minimum for FDM printing

    BASE_ALLOWED_MODEL_DOMAINS = {
        "assets.meshy.ai",
        "cdn.meshy.ai",
    }
    MAX_DOWNLOAD_BYTES = 100 * 1024 * 1024  # 100 MB

    @staticmethod
    def _allowed_model_domains() -> set[str]:
        domains = set(PrintAnalysisService.BASE_ALLOWED_MODEL_DOMAINS)
        if AWS_BUCKET_MODELS:
            domains.add(f"{AWS_BUCKET_MODELS}.s3.{AWS_REGION}.amazonaws.com")
            domains.add(f"{AWS_BUCKET_MODELS}.s3.amazonaws.com")
        return domains

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
    def analyze(file_path: str, file_type: str | None = None, printer_type: str = "fdm") -> Dict[str, Any]:
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
        import trimesh

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

        # Set thresholds based on printer type
        if printer_type == "resin":
            min_wall = 0.4   # Resin can do thinner walls
            overhang_threshold = 30.0  # Resin needs supports earlier
        else:
            min_wall = PrintAnalysisService.MIN_WALL_THICKNESS_MM  # 0.8mm for FDM
            overhang_threshold = 45.0

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
                suggestions.append(
                    "Use Remesh (triangle topology) to close open edges and create "
                    "a watertight mesh — this is the most impactful fix for print quality"
                )

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

            # 5. Bounding box + unit detection
            extents = mesh.bounding_box.extents
            raw_extents = [round(float(x), 4) for x in extents]
            max_extent = max(raw_extents)

            # Detect likely unit system
            # glTF/GLB standard: meters. Meshy models: normalized -1 to 1 range.
            detected_unit = "mm"
            mm_multiplier = 1.0

            if max_extent > 0 and max_extent < 10:
                detected_unit = "meters"
                mm_multiplier = 1000.0
            elif max_extent >= 10 and max_extent < 100:
                detected_unit = "mm_or_cm"
                mm_multiplier = 1.0  # Assume mm unless user says otherwise

            extents_mm = [round(float(x) * mm_multiplier, 2) for x in extents]
            checks["bounding_box_raw"] = raw_extents
            checks["bounding_box_mm"] = extents_mm
            checks["detected_unit"] = detected_unit
            checks["mm_multiplier"] = mm_multiplier

            if detected_unit == "meters":
                suggestions.append(
                    f"Model appears to be in meters (max dimension: {max_extent:.2f}). "
                    f"Converted to mm for display: {extents_mm[0]} × {extents_mm[1]} × {extents_mm[2]} mm"
                )

            # 6. Volume estimate
            if mesh.is_watertight and mesh.volume > 0:
                checks["estimated_volume_cm3"] = round(float(mesh.volume / 1000), 2)
            else:
                checks["estimated_volume_cm3"] = None

            # 7. Wall thickness analysis (ray-based sampling)
            try:
                # Sample surface points for thickness measurement
                sample_count = min(2000, max(500, len(mesh.faces) // 10))
                points, face_indices = trimesh.sample.sample_surface(mesh, sample_count)

                # Compute inward normals at sample points
                face_normals = mesh.face_normals[face_indices]
                inward_normals = -face_normals  # Point inward

                # Cast rays inward from surface to find opposing wall
                ray_origins = points + inward_normals * 0.001  # Offset slightly to avoid self-intersection

                # Try the default ray engine (rtree-backed), fall back to
                # the slower embree-less engine that ships with trimesh.
                try:
                    locations, index_ray, index_tri = mesh.ray.intersects_location(
                        ray_origins=ray_origins,
                        ray_directions=inward_normals,
                    )
                except (ImportError, ModuleNotFoundError) as ray_dep_err:
                    logger.info(
                        "[PRINT_ANALYSIS] Default ray engine unavailable (%s), "
                        "falling back to triangle-based intersection",
                        ray_dep_err,
                    )
                    from trimesh.ray.ray_triangle import RayMeshIntersector
                    ray_engine = RayMeshIntersector(mesh)
                    locations, index_ray, index_tri = ray_engine.intersects_location(
                        ray_origins=ray_origins,
                        ray_directions=inward_normals,
                    )

                if len(locations) > 0:
                    # Compute distances from origin points to hit points
                    hit_distances = np.linalg.norm(
                        locations - ray_origins[index_ray], axis=1
                    )
                    # Filter out very long rays (> 100mm = likely through-shots, not walls)
                    valid_mask = hit_distances < 100.0
                    if np.any(valid_mask):
                        valid_distances = hit_distances[valid_mask]
                        min_thickness = float(np.min(valid_distances))
                        avg_thickness = float(np.mean(valid_distances))
                        pct_below_min = float(np.sum(valid_distances < min_wall) / len(valid_distances) * 100)

                        checks["min_wall_thickness_mm"] = round(min_thickness, 3)
                        checks["avg_wall_thickness_mm"] = round(avg_thickness, 3)
                        checks["pct_below_min_thickness"] = round(pct_below_min, 1)
                        checks["wall_thickness_ok"] = pct_below_min < 5.0  # Less than 5% of surface below minimum

                        if pct_below_min >= 20.0:
                            score -= 15
                            issues.append(
                                f"Significant thin walls detected: {pct_below_min:.0f}% of surface "
                                f"is below {min_wall}mm minimum "
                                f"(thinnest: {min_thickness:.2f}mm)"
                            )
                            suggestions.append(
                                "Thicken thin walls in a 3D editor before printing, "
                                "or increase infill to 100% for very thin sections"
                            )
                        elif pct_below_min >= 5.0:
                            score -= 5
                            issues.append(
                                f"Some thin walls detected: {pct_below_min:.0f}% of surface "
                                f"below {min_wall}mm "
                                f"(thinnest: {min_thickness:.2f}mm)"
                            )
                    else:
                        checks["min_wall_thickness_mm"] = None
                        checks["wall_thickness_ok"] = None
                else:
                    checks["min_wall_thickness_mm"] = None
                    checks["wall_thickness_ok"] = None
            except Exception as wall_exc:
                logger.error("[PRINT_ANALYSIS] Wall thickness check failed: %s", wall_exc, exc_info=True)
                checks["min_wall_thickness_mm"] = None
                checks["wall_thickness_ok"] = None

            # 8. Overhang detection (faces angled > threshold from vertical need support)
            fdm_overhang_pct = 0.0
            try:
                face_normals_arr = mesh.face_normals  # (N, 3) array
                # Build direction is +Y (up). Compute angle of each face from vertical.
                up = np.array([0.0, 1.0, 0.0])
                dots = np.dot(face_normals_arr, up)
                # Only consider downward-facing faces (dot < 0 means face points downward)
                downward_mask = dots < 0
                if np.any(downward_mask):
                    # Angle from vertical for downward faces
                    overhang_angles_deg = np.degrees(np.arccos(np.clip(np.abs(dots[downward_mask]), 0, 1)))
                    # Faces where angle from vertical > threshold are overhangs
                    fdm_overhang_threshold = 45.0
                    resin_overhang_threshold = 30.0

                    fdm_overhang_count = int(np.sum(overhang_angles_deg > fdm_overhang_threshold))
                    resin_overhang_count = int(np.sum(overhang_angles_deg > resin_overhang_threshold))

                    fdm_overhang_pct = round(fdm_overhang_count / len(mesh.faces) * 100, 1) if len(mesh.faces) > 0 else 0
                    resin_overhang_pct = round(resin_overhang_count / len(mesh.faces) * 100, 1) if len(mesh.faces) > 0 else 0

                    checks["overhang_fdm_pct"] = fdm_overhang_pct
                    checks["overhang_resin_pct"] = resin_overhang_pct
                    checks["overhang_fdm_faces"] = fdm_overhang_count
                    checks["overhang_resin_faces"] = resin_overhang_count

                    if fdm_overhang_pct > 20:
                        issues.append(
                            f"{fdm_overhang_pct}% of faces are steep overhangs (>45°) — "
                            f"FDM printing will require support material"
                        )
                        suggestions.append(
                            "Consider rotating the model for fewer overhangs, "
                            "or enable supports in your slicer (tree supports recommended)"
                        )
                    elif fdm_overhang_pct > 10:
                        suggestions.append(
                            f"{fdm_overhang_pct}% overhang faces detected — "
                            f"enable supports in slicer for best results"
                        )
                else:
                    checks["overhang_fdm_pct"] = 0.0
                    checks["overhang_resin_pct"] = 0.0
            except Exception as oh_exc:
                logger.warning("[PRINT_ANALYSIS] Overhang check failed: %s", oh_exc)
                checks["overhang_fdm_pct"] = None
                checks["overhang_resin_pct"] = None

            # 9. Printer-type-specific slicer guidance
            if score >= 70:
                max_dim_mm = max(extents_mm)
                suggestions.append(
                    f"FDM: Use 0.2mm layer height for standard quality, 0.12mm for fine detail. "
                    f"Set wall count to 3+ for structural strength."
                )
                suggestions.append(
                    f"Resin (SLA/DLP): Use 0.05mm layer height for maximum detail. "
                    f"Ensure wall thickness is ≥0.5mm (min for supported walls)."
                )
                if checks.get("overhang_fdm_pct") and checks["overhang_fdm_pct"] > 5:
                    suggestions.append(
                        "FDM: Enable tree supports in your slicer to reduce material waste on overhangs."
                    )
                if max_dim_mm > 200:
                    suggestions.append(
                        f"Model is {max_dim_mm:.0f}mm on its longest side — "
                        f"verify it fits your printer's build volume before slicing."
                    )

            # 10. Basic orientation suggestion
            try:
                best_orientation = "current"
                best_overhang_pct = fdm_overhang_pct if fdm_overhang_pct is not None else 100.0

                # Only run if overhangs are significant
                if best_overhang_pct > 15:
                    for axis_name, up_vec in [("rotate 90° around X (Z-up)", np.array([0, 0, 1.0])),
                                               ("rotate 90° around Z (X-up)", np.array([1.0, 0, 0]))]:
                        dots_alt = np.dot(face_normals_arr, up_vec)
                        down_alt = dots_alt < 0
                        if np.any(down_alt):
                            angles_alt = np.degrees(np.arccos(np.clip(np.abs(dots_alt[down_alt]), 0, 1)))
                            alt_pct = float(np.sum(angles_alt > 45.0) / len(mesh.faces) * 100)
                            if alt_pct < best_overhang_pct:
                                best_overhang_pct = alt_pct
                                best_orientation = axis_name

                    if best_orientation != "current":
                        checks["suggested_orientation"] = best_orientation
                        checks["suggested_orientation_overhang_pct"] = round(best_overhang_pct, 1)
                        suggestions.append(
                            f"Orientation tip: {best_orientation} would reduce overhangs "
                            f"from {fdm_overhang_pct:.0f}% to {best_overhang_pct:.0f}% of faces"
                        )
            except Exception as orient_exc:
                logger.warning("[PRINT_ANALYSIS] Orientation check failed: %s", orient_exc)

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

        # Add actionable suggestions for common issues
        if not mesh.is_watertight:
            suggestions.append(
                "Non-watertight meshes cannot be accurately measured for wall thickness "
                "or volume. Use Remesh with 'triangle' topology to close holes and repair the mesh."
            )

        if score < 90 and checks.get("is_manifold") and checks.get("min_wall_thickness_mm") is None:
            suggestions.append(
                "Wall thickness analysis requires a closed mesh. Remesh the model first, "
                "then re-run the print check for complete diagnostics."
            )

        return {
            "score": score,
            "is_printable": score >= 60,
            "checks": checks,
            "issues": issues,
            "suggestions": suggestions,
        }

    @staticmethod
    def _validate_url(url: str) -> str | None:
        """Return an error message if the URL is not safe to fetch, else None."""
        from urllib.parse import urlparse
        parsed = urlparse(url)

        # Scheme check
        if parsed.scheme not in ("https", "http"):
            return f"Unsupported URL scheme: {parsed.scheme}"

        # Domain allowlist — if configured, enforce it
        allowed_domains = PrintAnalysisService._allowed_model_domains()
        if allowed_domains:
            domain = parsed.hostname or ""
            if not any(domain == d or domain.endswith("." + d) for d in allowed_domains):
                return f"Domain not in allowlist: {domain}"

        # Block private/internal IPs
        import socket
        try:
            ip = socket.gethostbyname(parsed.hostname)
            import ipaddress
            addr = ipaddress.ip_address(ip)
            if addr.is_private or addr.is_loopback or addr.is_link_local:
                return f"Resolved to private IP: {ip}"
        except Exception:
            pass  # DNS resolution failed — requests.get will handle it

        return None

    @staticmethod
    def analyze_from_url(url: str, printer_type: str = "fdm") -> Dict[str, Any]:
        """Download a GLB/STL from URL and analyze it."""
        import tempfile
        import os
        import requests

        # Validate URL safety
        url_error = PrintAnalysisService._validate_url(url)
        if url_error:
            logger.warning("[PRINT_ANALYSIS] URL validation failed: %s — %s", url, url_error)
            return PrintAnalysisService._failed_result(f"Invalid model URL: {url_error}")

        tmp_path = None
        try:
            # Stream download with size limit
            resp = requests.get(url, timeout=30, stream=True)
            resp.raise_for_status()

            # Check Content-Length header if available
            content_length = int(resp.headers.get("content-length", 0))
            if content_length > PrintAnalysisService.MAX_DOWNLOAD_BYTES:
                return PrintAnalysisService._failed_result(
                    f"Model file too large ({content_length / 1024 / 1024:.0f} MB). "
                    f"Maximum is {PrintAnalysisService.MAX_DOWNLOAD_BYTES / 1024 / 1024:.0f} MB."
                )

            file_type = PrintAnalysisService._detect_file_type(
                url,
                resp.headers.get("content-type"),
                b"",  # We'll detect from content after download
            )
            suffix = f".{file_type}" if file_type else ".bin"

            # Write to temp file with size limit enforcement
            tmp = tempfile.NamedTemporaryFile(suffix=suffix, delete=False)
            tmp_path = tmp.name
            downloaded = 0
            for chunk in resp.iter_content(chunk_size=1024 * 1024):  # 1MB chunks
                downloaded += len(chunk)
                if downloaded > PrintAnalysisService.MAX_DOWNLOAD_BYTES:
                    tmp.close()
                    return PrintAnalysisService._failed_result(
                        f"Model file exceeds {PrintAnalysisService.MAX_DOWNLOAD_BYTES / 1024 / 1024:.0f} MB limit."
                    )
                tmp.write(chunk)
            tmp.close()

            # Re-detect file type from first bytes if needed
            if not file_type:
                with open(tmp_path, "rb") as f:
                    head = f.read(16)
                file_type = PrintAnalysisService._detect_file_type(url, resp.headers.get("content-type"), head)

            return PrintAnalysisService.analyze(tmp_path, file_type=file_type, printer_type=printer_type)

        except requests.RequestException as e:
            logger.error("[PRINT_ANALYSIS] Failed to download model: %s", e)
            return PrintAnalysisService._failed_result(f"Could not download model: {e}")
        finally:
            # Clean up temp file
            if tmp_path:
                try:
                    os.unlink(tmp_path)
                except OSError:
                    pass
