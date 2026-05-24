"""
Tests for the STL pack storefront — checkout, webhook idempotency, and
entitlement gating.

These cover the money path that was previously untested:

  * Catalog integrity — prices are server-authoritative, slugs map to R2 keys.
  * Entitlement gating — has_entitlement() honours the All-Access pass ("*")
    and the /api/stl/* routes reject anonymous callers.
  * Webhook idempotency — a replayed Mollie webhook never double-grants.
  * Checkout — the amount sent to Mollie comes from the server catalog, so a
    tampered client cannot change the price.

Run:
    cd backend/tests && python -m unittest test_stl_packs -v
    # or
    python test_stl_packs.py

The service-layer tests mock the database and HTTP layers, so they need no
Postgres, no Mollie key and no R2 — they exercise pure logic. The route tests
spin up the Flask app via the test client (degraded DB mode is fine) and are
skipped automatically if the app cannot be imported.
"""

import os
import sys
import unittest
from pathlib import Path
from unittest.mock import patch, MagicMock

# ── Path / env setup (mirrors test_smoke_routes.py) ──────────────────────────
os.environ.setdefault("FLASK_ENV", "development")
os.environ.setdefault("ADMIN_TOKEN", "ci-3d-admin")

_HERE = Path(__file__).resolve()
_BACKEND_DIR = _HERE.parents[1]          # .../backend
_PROJECT_ROOT = _HERE.parents[2]         # parent of backend/  (app_modular lives here or in backend/)
for _p in (str(_PROJECT_ROOT), str(_BACKEND_DIR)):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from backend.services.stl_pack_service import (  # noqa: E402
    ALL_ACCESS,
    ALL_ACCESS_SLUG,
    STL_PACKS,
    StlPackService,
    get_pack,
)

# The Flask app is optional — only the route tests need it.
try:
    import app_modular  # noqa: E402
    _APP = app_modular.app
    _APP_ERROR = None
except Exception as exc:  # pragma: no cover - environment dependent
    _APP = None
    _APP_ERROR = exc

_SVC = "backend.services.stl_pack_service"


# ─────────────────────────────────────────────────────────────────────────────
# 1. Catalog integrity
# ─────────────────────────────────────────────────────────────────────────────
class TestStlCatalog(unittest.TestCase):
    """The catalog is the server-authoritative source of price and R2 key."""

    def test_get_pack_returns_known_pack(self):
        pack = get_pack("airplanes")
        self.assertIsNotNone(pack)
        self.assertEqual(pack["slug"], "airplanes")
        self.assertEqual(pack["r2_key"], "airplanes.zip")
        self.assertIn("title", pack)

    def test_get_pack_all_access(self):
        pack = get_pack(ALL_ACCESS_SLUG)
        self.assertIsNotNone(pack)
        self.assertEqual(pack["slug"], "*")
        self.assertEqual(pack["price_gbp"], ALL_ACCESS["price_gbp"])

    def test_get_pack_unknown_returns_none(self):
        self.assertIsNone(get_pack("not-a-real-pack"))

    def test_get_pack_empty_returns_none(self):
        self.assertIsNone(get_pack(""))
        self.assertIsNone(get_pack(None))

    def test_get_pack_returns_a_copy(self):
        # Mutating the returned dict must not corrupt the catalog.
        pack = get_pack("airplanes")
        pack["price_gbp"] = 999
        self.assertNotEqual(STL_PACKS["airplanes"]["price_gbp"], 999)

    def test_every_pack_has_price_and_matching_r2_key(self):
        self.assertGreater(len(STL_PACKS), 0)
        for slug, pack in STL_PACKS.items():
            self.assertIn("title", pack, f"{slug} missing title")
            self.assertIsInstance(pack["price_gbp"], (int, float),
                                  f"{slug} price not numeric")
            self.assertGreater(pack["price_gbp"], 0, f"{slug} price not positive")
            self.assertEqual(pack["r2_key"], f"{slug}.zip",
                             f"{slug} r2_key should be '{slug}.zip'")

    def test_all_access_pass_shape(self):
        self.assertEqual(ALL_ACCESS_SLUG, "*")
        self.assertGreater(ALL_ACCESS["price_gbp"], 0)
        # All-Access has no single file — it grants every pack instead.
        self.assertIsNone(ALL_ACCESS["r2_key"])

    def test_presign_download_rejects_all_access_and_unknown(self):
        # All-Access has no r2_key, so it cannot be presigned directly;
        # the route must request a concrete pack slug.
        with self.assertRaises(ValueError):
            StlPackService.presign_download(ALL_ACCESS_SLUG)
        with self.assertRaises(ValueError):
            StlPackService.presign_download("not-a-real-pack")


# ─────────────────────────────────────────────────────────────────────────────
# 2. Entitlement gating
# ─────────────────────────────────────────────────────────────────────────────
class TestEntitlementGating(unittest.TestCase):
    """has_entitlement() decides who may download a pack."""

    def test_returns_true_when_a_row_exists(self):
        with patch(f"{_SVC}.query_one", return_value={"?column?": 1}) as q:
            self.assertTrue(StlPackService.has_entitlement("id-1", "airplanes"))
            q.assert_called_once()

    def test_returns_false_when_no_row(self):
        with patch(f"{_SVC}.query_one", return_value=None):
            self.assertFalse(StlPackService.has_entitlement("id-1", "airplanes"))

    def test_query_always_checks_the_all_access_pass(self):
        # The security property: owning "*" must grant access to ANY pack.
        # has_entitlement does this by querying for (pack_slug OR "*").
        with patch(f"{_SVC}.query_one", return_value=None) as q:
            StlPackService.has_entitlement("id-7", "marvel")
        params = q.call_args.args[1]
        self.assertEqual(params[0], "id-7")
        self.assertEqual(params[1], "marvel")
        self.assertEqual(params[2], ALL_ACCESS_SLUG)


# ─────────────────────────────────────────────────────────────────────────────
# 3. Webhook idempotency
# ─────────────────────────────────────────────────────────────────────────────
class TestWebhookIdempotency(unittest.TestCase):
    """A Mollie webhook can be delivered more than once — it must never
    grant a second entitlement for the same payment."""

    def _payment(self, payment_id="tr_TEST123"):
        return {
            "id": payment_id,
            "amount": {"currency": "GBP", "value": "2.49"},
            "metadata": {
                "type": "stl_pack",
                "pack_slug": "marvel",
                "identity_id": "id-42",
                "email": "buyer@example.com",
            },
        }

    def test_record_entitlement_grants_on_first_insert(self):
        granted_row = {"id": "ent-1", "pack_slug": "marvel"}
        with patch(f"{_SVC}.execute_returning", return_value=granted_row) as ex:
            result = StlPackService.record_entitlement(
                "id-42", "marvel", "tr_TEST123", amount=2.49)
        self.assertEqual(result, granted_row)
        params = ex.call_args.args[1]
        # (uuid, identity_id, pack_slug, provider, provider_payment_id, amount, currency)
        self.assertEqual(params[1], "id-42")
        self.assertEqual(params[2], "marvel")
        self.assertEqual(params[3], "mollie")
        self.assertEqual(params[4], "tr_TEST123")

    def test_record_entitlement_is_a_noop_on_conflict(self):
        # ON CONFLICT (provider, provider_payment_id) DO NOTHING -> no row.
        # The second delivery of the same payment must return None, not raise.
        with patch(f"{_SVC}.execute_returning", return_value=None):
            result = StlPackService.record_entitlement(
                "id-42", "marvel", "tr_TEST123", amount=2.49)
        self.assertIsNone(result)

    def test_handle_payment_paid_grants_the_entitlement(self):
        with patch.object(StlPackService, "record_entitlement") as rec:
            rec.return_value = {"id": "ent-1"}
            out = StlPackService.handle_payment_paid(self._payment())
        self.assertEqual(out, {"ok": True, "stl_pack": True, "pack_slug": "marvel"})
        rec.assert_called_once()
        kwargs = rec.call_args.kwargs
        self.assertEqual(kwargs["identity_id"], "id-42")
        self.assertEqual(kwargs["pack_slug"], "marvel")
        self.assertEqual(kwargs["provider_payment_id"], "tr_TEST123")
        self.assertEqual(kwargs["amount"], 2.49)

    def test_handle_payment_paid_replay_keys_on_same_payment_id(self):
        # Deliver the same webhook twice. record_entitlement is keyed on the
        # payment id both times, so the DB unique index dedupes the 2nd one.
        with patch.object(StlPackService, "record_entitlement") as rec:
            rec.side_effect = [{"id": "ent-1"}, None]  # 1st grants, 2nd conflict
            payment = self._payment()
            first = StlPackService.handle_payment_paid(payment)
            second = StlPackService.handle_payment_paid(payment)
        self.assertEqual(first, second)
        self.assertEqual(rec.call_count, 2)
        ids = [c.kwargs["provider_payment_id"] for c in rec.call_args_list]
        self.assertEqual(ids, ["tr_TEST123", "tr_TEST123"])

    def test_handle_payment_paid_ignores_payment_with_no_metadata(self):
        bad = {"id": "tr_X", "amount": {"currency": "GBP", "value": "2.49"},
               "metadata": {}}
        with patch.object(StlPackService, "record_entitlement") as rec:
            out = StlPackService.handle_payment_paid(bad)
        self.assertIsNone(out)
        rec.assert_not_called()


# ─────────────────────────────────────────────────────────────────────────────
# 4. Checkout — price is server-authoritative
# ─────────────────────────────────────────────────────────────────────────────
class TestCheckout(unittest.TestCase):

    def test_unknown_pack_raises(self):
        with self.assertRaises(ValueError):
            StlPackService.create_checkout("id-1", "e@x.com", "not-a-real-pack")

    def test_amount_sent_to_mollie_comes_from_the_catalog(self):
        fake_response = MagicMock()
        fake_response.status_code = 201
        fake_response.json.return_value = {
            "id": "tr_NEW",
            "_links": {"checkout": {"href": "https://mollie.test/checkout/tr_NEW"}},
        }
        with patch(f"{_SVC}.requests") as req, \
             patch("backend.services.mollie_service.MollieService.is_available",
                   return_value=True), \
             patch("backend.services.mollie_service.MollieService._get_headers",
                   return_value={}):
            req.post.return_value = fake_response
            result = StlPackService.create_checkout("id-1", "e@x.com", "marvel")

        self.assertEqual(result["payment_id"], "tr_NEW")
        self.assertEqual(result["checkout_url"],
                         "https://mollie.test/checkout/tr_NEW")

        body = req.post.call_args.kwargs["json"]
        # The price is taken from STL_PACKS, never from client input.
        expected = f"{float(STL_PACKS['marvel']['price_gbp']):.2f}"
        self.assertEqual(body["amount"], {"currency": "GBP", "value": expected})
        self.assertEqual(body["metadata"]["type"], "stl_pack")
        self.assertEqual(body["metadata"]["pack_slug"], "marvel")
        self.assertEqual(body["metadata"]["identity_id"], "id-1")


# ─────────────────────────────────────────────────────────────────────────────
# 5. Route-level gating (Flask test client)
# ─────────────────────────────────────────────────────────────────────────────
@unittest.skipIf(_APP is None, f"app_modular import failed: {_APP_ERROR}")
class TestStlRoutes(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.client = _APP.test_client()

    def test_catalog_is_public_and_lists_every_pack(self):
        resp = self.client.get("/api/stl/catalog")
        self.assertEqual(resp.status_code, 200)
        payload = resp.get_json()
        self.assertTrue(payload["ok"])
        self.assertEqual(len(payload["packs"]), len(STL_PACKS))
        self.assertEqual(payload["all_access"]["slug"], "*")
        self.assertEqual(payload["all_access"]["price_gbp"], ALL_ACCESS["price_gbp"])
        for pack in payload["packs"]:
            self.assertIn(pack["slug"], STL_PACKS)
            self.assertEqual(pack["price_gbp"], STL_PACKS[pack["slug"]]["price_gbp"])

    def test_download_requires_a_session(self):
        resp = self.client.get("/api/stl/download?pack=airplanes")
        self.assertEqual(resp.status_code, 401)

    def test_my_packs_requires_a_session(self):
        resp = self.client.get("/api/stl/my-packs")
        self.assertEqual(resp.status_code, 401)

    def test_checkout_is_not_open_to_anonymous_callers(self):
        # No session (and no CSRF token) — either rejection proves the gate.
        resp = self.client.post("/api/stl/checkout", json={"pack_slug": "airplanes"})
        self.assertIn(resp.status_code, (401, 403))


if __name__ == "__main__":
    unittest.main(verbosity=2)
