from __future__ import annotations

from contextlib import contextmanager

from flask import Flask, g

from backend.services import free_generation_service as service


def _request_context():
    app = Flask(__name__)
    return app.test_request_context(
        "/api/_mod/homepage/generate",
        method="POST",
        headers={"User-Agent": "entitlement-test"},
        environ_base={"REMOTE_ADDR": "203.0.113.8"},
    )


def test_entitlements_are_independent_per_service(monkeypatch):
    used = {"image": {"status": "completed", "generation_type": "image"}}
    monkeypatch.setattr(service, "_find_existing_trial", lambda fp, generation_type=None, **_: used.get(generation_type))
    with _request_context():
        g.identity_id = "identity-1"
        g.session_id = "session-1"
        state = service.get_current_trial_state()
    assert state["entitlements"]["image"]["remaining"] is False
    assert state["entitlements"]["video"]["remaining"] is True
    assert state["entitlements"]["3d"]["remaining"] is True


def test_unknown_generation_type_fails_without_database():
    with _request_context():
        decision = service.reserve_trial("test prompt", "audio")
    assert decision.allowed is False
    assert decision.blocked_reason == "unsupported_generation_type"


def test_failed_attempts_count_toward_attempt_limit(monkeypatch):
    class Cursor:
        description = None

        def __init__(self):
            self.sql = ""

        def execute(self, sql, params=()):
            self.sql = sql

        def fetchone(self):
            if "COUNT(*)" in self.sql:
                return {"count": 2}
            return None

    @contextmanager
    def fake_transaction(_source):
        yield Cursor()

    monkeypatch.setattr(service, "transaction", fake_transaction)
    with _request_context():
        g.identity_id = "identity-1"
        g.session_id = "session-1"
        decision = service.reserve_trial(
            "a safe image prompt", "image",
            max_attempts_per_type_per_day=2,
        )
    assert decision.allowed is False
    assert decision.blocked_reason == "free_attempt_limit"


def test_gate_errors_are_not_reported_as_used_trials(monkeypatch):
    @contextmanager
    def broken_transaction(_source):
        raise RuntimeError("database unavailable")
        yield

    monkeypatch.setattr(service, "transaction", broken_transaction)
    monkeypatch.setattr(service, "_find_existing_trial", lambda *args, **kwargs: None)
    with _request_context():
        g.identity_id = "identity-1"
        g.session_id = "session-1"
        decision = service.reserve_trial("a safe image prompt", "image")
    assert decision.allowed is False
    assert decision.blocked_reason == "free_gate_unavailable"

