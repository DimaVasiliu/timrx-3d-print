import os
import sys
import unittest
from pathlib import Path

os.environ.setdefault("FLASK_ENV", "development")
os.environ.setdefault("ADMIN_TOKEN", "ci-3d-admin")

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from backend.utils.upload_validation import (
    UploadValidationError,
    parse_data_url,
    sniff_image_content_type,
    sniff_model_content_type,
)


ONE_BY_ONE_PNG = (
    b"\x89PNG\r\n\x1a\n\x00\x00\x00\rIHDR\x00\x00\x00\x01\x00\x00\x00\x01"
    b"\x08\x06\x00\x00\x00\x1f\x15\xc4\x89\x00\x00\x00\rIDATx\x9cc```\xf8\x0f"
    b"\x00\x01\x04\x01\x00\xa5\xf6E@\x00\x00\x00\x00IEND\xaeB`\x82"
)


class SecurityHardeningTests(unittest.TestCase):
    def test_s3_validation_prefix_comes_from_key(self):
        try:
            from backend.services.s3_service import _effective_validation_prefix
        except ModuleNotFoundError as exc:
            self.skipTest(f"S3 service dependencies unavailable: {exc}")

        self.assertEqual(
            _effective_validation_prefix("models", "images/provider/hash.png"),
            "images",
        )
        self.assertEqual(
            _effective_validation_prefix("models", "thumbnails/provider/hash.jpg"),
            "thumbnails",
        )

    def test_flux_upload_validation_maps_to_source_image_field(self):
        try:
            from backend.routes.image_gen import _upload_validation_field
        except ModuleNotFoundError as exc:
            self.skipTest(f"Image route dependencies unavailable: {exc}")

        field = _upload_validation_field(
            "flux_pro",
            {"source_image": "data:image/png;base64,ZmFrZQ=="},
        )
        self.assertEqual(field, "source_image")

    def test_health_endpoint_sets_frame_protection_headers(self):
        try:
            from app_modular import app
        except ModuleNotFoundError as exc:
            self.skipTest(f"Flask app dependencies unavailable: {exc}")

        response = app.test_client().get("/api/health")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.headers.get("X-Frame-Options"), "DENY")
        self.assertEqual(response.headers.get("X-Content-Type-Options"), "nosniff")
        self.assertIn("frame-ancestors 'none'", response.headers.get("Content-Security-Policy", ""))

    def test_parse_data_url_decodes_png(self):
        mime, data = parse_data_url(
            "data:image/png;base64,"
            "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAA"
            "DUlEQVR4nGNgYGD4DwABBAEApfZFQAAAAABJRU5ErkJggg=="
        )
        self.assertEqual(mime, "image/png")
        self.assertEqual(data, ONE_BY_ONE_PNG)

    def test_sniff_image_content_type_accepts_real_png(self):
        self.assertEqual(sniff_image_content_type(ONE_BY_ONE_PNG), "image/png")

    def test_sniff_image_content_type_rejects_invalid_bytes(self):
        with self.assertRaises(UploadValidationError):
            sniff_image_content_type(b"not-an-image")

    def test_sniff_model_content_type_accepts_glb_signature(self):
        glb_bytes = b"glTF\x02\x00\x00\x00\x0c\x00\x00\x00"
        self.assertEqual(sniff_model_content_type(glb_bytes), "model/gltf-binary")


if __name__ == "__main__":
    unittest.main()
