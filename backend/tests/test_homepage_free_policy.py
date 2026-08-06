from __future__ import annotations

from backend.routes import homepage_generation as route


def test_free_generation_requires_turnstile_secret_and_hash_salt(monkeypatch):
    monkeypatch.setattr(route.config, "HOMEPAGE_FREE_ENABLED", True)
    monkeypatch.setattr(route.config, "TURNSTILE_ENABLED", True)
    monkeypatch.setattr(route.config, "TURNSTILE_SECRET_KEY", "")
    monkeypatch.setenv("FREE_GENERATION_HASH_SALT", "test-salt")
    assert route._free_generation_enabled() is False

    monkeypatch.setattr(route.config, "TURNSTILE_SECRET_KEY", "turnstile-secret")
    monkeypatch.delenv("FREE_GENERATION_HASH_SALT")
    assert route._free_generation_enabled() is False


def test_free_provider_policy_does_not_silently_fallback(monkeypatch):
    monkeypatch.setattr(route.config, "HOMEPAGE_FREE_IMAGE_PROVIDER", "nano_banana")
    monkeypatch.setattr(route.config, "HOMEPAGE_FREE_VIDEO_PROVIDER", "seedance")
    monkeypatch.setattr(route.config, "MESHY_API_KEY", "meshy-key")
    assert route._free_provider_ready("image", {"provider": "nano_banana"}) is True
    assert route._free_provider_ready("image", {"provider": "openai"}) is False
    assert route._free_provider_ready("video", {"provider": "seedance"}) is True
    assert route._free_provider_ready("video", {"provider": "vertex"}) is False
    assert route._free_provider_ready("3d", {}) is True
