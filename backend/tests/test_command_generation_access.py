from __future__ import annotations

from flask import Flask, g

from backend.routes import command


def _context():
    return Flask(__name__).test_request_context("/api/_mod/command/plan")


def _quote() -> dict:
    return {"available": True, "credits": 12, "action_key": "image.generate.nano_banana.2k"}


def test_access_prefers_unused_free_entitlement(monkeypatch):
    monkeypatch.setattr(command, "get_current_trial_state", lambda *_: {
        "entitlements": {"image": {"remaining": True, "status": "available"}}
    })
    monkeypatch.setattr(command, "_free_type_enabled", lambda *_: True)
    monkeypatch.setattr(command, "_free_cost_limit", lambda *_: 12)
    monkeypatch.setattr(command, "has_paid_balance", lambda *_: True)
    monkeypatch.setattr(command, "is_turnstile_enabled", lambda: True)
    with _context():
        g.identity_id = "identity-1"
        access = command._access_payload({"intent": "image"}, _quote())
    assert access["mode"] == "free"
    assert access["challenge_required"] is True


def test_access_falls_back_to_paid_credits_after_free_use(monkeypatch):
    monkeypatch.setattr(command, "get_current_trial_state", lambda *_: {
        "entitlements": {"image": {"remaining": False, "status": "completed"}}
    })
    monkeypatch.setattr(command, "_free_type_enabled", lambda *_: True)
    monkeypatch.setattr(command, "has_paid_balance", lambda *_: True)
    with _context():
        g.identity_id = "identity-1"
        access = command._access_payload({"intent": "image"}, _quote())
    assert access["mode"] == "paid"
    assert access["challenge_required"] is False


def test_access_blocks_when_entitlement_and_credits_are_exhausted(monkeypatch):
    monkeypatch.setattr(command, "get_current_trial_state", lambda *_: {
        "entitlements": {"image": {"remaining": False, "status": "completed"}}
    })
    monkeypatch.setattr(command, "_free_type_enabled", lambda *_: True)
    monkeypatch.setattr(command, "has_paid_balance", lambda *_: False)
    with _context():
        g.identity_id = "identity-1"
        access = command._access_payload({"intent": "image"}, _quote())
    assert access["mode"] == "blocked"
    assert access["has_credits"] is False


def test_free_offer_fails_closed_without_security_configuration(monkeypatch):
    monkeypatch.setattr(command.config, "HOMEPAGE_FREE_ENABLED", True)
    monkeypatch.setattr(command.config, "HOMEPAGE_FREE_ALLOW_IMAGE", True)
    monkeypatch.setattr(command.config, "TURNSTILE_ENABLED", True)
    monkeypatch.setattr(command.config, "TURNSTILE_SECRET_KEY", "")
    monkeypatch.setenv("FREE_GENERATION_HASH_SALT", "test-salt")
    assert command._free_type_enabled("image") is False

    monkeypatch.setattr(command.config, "TURNSTILE_SECRET_KEY", "turnstile-secret")
    monkeypatch.delenv("FREE_GENERATION_HASH_SALT")
    assert command._free_type_enabled("image") is False
