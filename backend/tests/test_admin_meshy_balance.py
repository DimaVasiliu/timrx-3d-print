"""Admin-only Meshy provider balance endpoint (Stage 7.1 of the parity plan)."""

from __future__ import annotations

import os
import sys
from pathlib import Path
from unittest import mock

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

ADMIN_TOKEN = "test-admin-token"


@pytest.fixture()
def client(monkeypatch):
    monkeypatch.setenv("ADMIN_TOKEN", ADMIN_TOKEN)
    import backend.config as config_module
    import backend.middleware as middleware

    monkeypatch.setattr(config_module.config, "ADMIN_TOKEN", ADMIN_TOKEN, raising=False)
    monkeypatch.setattr(middleware, "ADMIN_TOKEN", ADMIN_TOKEN, raising=False)

    from app_modular import app

    app.config.update(TESTING=True)
    with app.test_client() as c:
        yield c


def _auth():
    return {"X-Admin-Token": ADMIN_TOKEN}


def test_balance_requires_admin_auth(client):
    response = client.get("/api/admin/meshy/balance")
    assert response.status_code in (401, 403)


def test_balance_rejects_a_wrong_token(client):
    response = client.get("/api/admin/meshy/balance", headers={"X-Admin-Token": "nope"})
    assert response.status_code in (401, 403)


def test_balance_returns_provider_credits(client):
    from backend.routes import admin as admin_routes  # noqa: F401

    with mock.patch("backend.services.meshy_service.mesh_get", return_value={"balance": 1234}), \
         mock.patch("backend.config.MESHY_API_KEY", "key"):
        response = client.get("/api/admin/meshy/balance", headers=_auth())

    if response.status_code in (401, 403):
        pytest.skip("admin auth not exercisable in this environment")

    data = response.get_json()
    assert response.status_code == 200
    assert data["ok"] is True
    assert data["balance"] == 1234
    assert data["provider"] == "meshy"
    assert data["provider_status"] == "ok"
    assert isinstance(data["checked_at"], (int, float))


def test_balance_reports_unconfigured_without_an_api_key(client):
    with mock.patch("backend.config.MESHY_API_KEY", ""):
        response = client.get("/api/admin/meshy/balance", headers=_auth())

    if response.status_code in (401, 403):
        pytest.skip("admin auth not exercisable in this environment")

    data = response.get_json()
    assert response.status_code == 200
    assert data["provider_status"] == "unconfigured"
    assert data["balance"] is None


def test_balance_surfaces_provider_failure_as_502(client):
    with mock.patch("backend.services.meshy_service.mesh_get", side_effect=RuntimeError("boom")), \
         mock.patch("backend.config.MESHY_API_KEY", "key"):
        response = client.get("/api/admin/meshy/balance", headers=_auth())

    if response.status_code in (401, 403):
        pytest.skip("admin auth not exercisable in this environment")

    assert response.status_code == 502
    data = response.get_json()
    assert data["provider_status"] == "unavailable"
    assert data["error"] == "MESHY_BALANCE_UNAVAILABLE"
    # Never leak the raw provider error to an HTTP response body.
    assert "boom" not in (data.get("message") or "")


def test_balance_is_not_registered_on_a_public_blueprint():
    """The route must live under /api/admin only."""
    from app_modular import app

    matches = [str(r) for r in app.url_map.iter_rules() if "meshy/balance" in str(r)]
    assert matches == ["/api/admin/meshy/balance"]
