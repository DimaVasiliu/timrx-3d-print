import os
import sys
import unittest
from pathlib import Path

os.environ.setdefault("FLASK_ENV", "development")
os.environ.setdefault("ADMIN_TOKEN", "ci-3d-admin")

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from app_modular import app


class SmokeRouteTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.client = app.test_client()

    def test_health_endpoint(self):
        response = self.client.get("/api/health")
        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertTrue(payload["ok"])
        self.assertIn("database", payload)
        self.assertIn(payload["database"]["mode"], ("full", "degraded"))

    def test_modular_health_endpoint(self):
        response = self.client.get("/api/_mod/health")
        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertEqual(payload["source"], "modular")
        self.assertIn("database", payload)
        self.assertIn(payload["database"]["mode"], ("full", "degraded"))

    def test_status_endpoint(self):
        response = self.client.get("/api/status")
        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertTrue(payload["ok"])
        self.assertIn("database", payload)
        self.assertIn(payload["database"]["mode"], ("full", "degraded"))

    def test_ready_endpoint_exposes_readiness(self):
        response = self.client.get("/api/ready")
        self.assertIn(response.status_code, (200, 503))
        payload = response.get_json()
        self.assertEqual(payload["check"], "readiness")
        self.assertIn("readiness", payload)
        self.assertEqual(payload["readiness"]["ready"], payload["ok"])

    def test_action_costs_endpoint(self):
        response = self.client.get("/api/billing/action-costs")
        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertTrue(payload["ok"])
        self.assertGreater(len(payload["action_costs"]), 0)

    def test_admin_health_requires_auth(self):
        response = self.client.get("/api/admin/health")
        self.assertEqual(response.status_code, 401)
        payload = response.get_json()
        self.assertEqual(payload["error"]["code"], "UNAUTHORIZED")

    def test_admin_health_rejects_invalid_token(self):
        response = self.client.get(
            "/api/admin/health",
            headers={"X-Admin-Token": "wrong-token"},
        )
        self.assertEqual(response.status_code, 403)
        payload = response.get_json()
        self.assertEqual(payload["error"]["code"], "INVALID_ADMIN_TOKEN")

    def test_admin_health_accepts_valid_token(self):
        response = self.client.get(
            "/api/admin/health",
            headers={"X-Admin-Token": os.environ["ADMIN_TOKEN"]},
        )
        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertTrue(payload["ok"])
        self.assertEqual(payload["auth_method"], "token")


if __name__ == "__main__":
    unittest.main()
