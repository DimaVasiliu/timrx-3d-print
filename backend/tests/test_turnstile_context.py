from backend.services import turnstile_service as service


class Response:
    status_code = 200

    def __init__(self, payload):
        self.payload = payload

    def json(self):
        return self.payload


def _enable(monkeypatch):
    monkeypatch.setattr(service.config, "TURNSTILE_ENABLED", True)
    monkeypatch.setattr(service.config, "TURNSTILE_SECRET_KEY", "secret")
    monkeypatch.setattr(service.config, "TURNSTILE_EXPECTED_HOSTNAMES", "timrx.live")
    monkeypatch.setattr(service.config, "TURNSTILE_EXPECTED_ACTION", "free_generation")


def test_turnstile_sends_remote_ip_and_validates_context(monkeypatch):
    _enable(monkeypatch)
    captured = {}

    def post(_url, data, timeout):
        captured.update(data)
        return Response({"success": True, "hostname": "timrx.live", "action": "free_generation"})

    monkeypatch.setattr(service.requests, "post", post)
    result = service.verify_turnstile_token("token", remote_ip="203.0.113.8", expected_action="free_generation")
    assert result.ok is True
    assert captured["remoteip"] == "203.0.113.8"


def test_turnstile_rejects_wrong_hostname(monkeypatch):
    _enable(monkeypatch)
    monkeypatch.setattr(service.requests, "post", lambda *_args, **_kwargs: Response({
        "success": True, "hostname": "attacker.example", "action": "free_generation"
    }))
    assert service.verify_turnstile_token("token").reason == "hostname_mismatch"


def test_turnstile_rejects_wrong_action(monkeypatch):
    _enable(monkeypatch)
    monkeypatch.setattr(service.requests, "post", lambda *_args, **_kwargs: Response({
        "success": True, "hostname": "timrx.live", "action": "other_action"
    }))
    assert service.verify_turnstile_token("token").reason == "action_mismatch"

