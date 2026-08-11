from __future__ import annotations

from flask import Flask, g

from backend.routes import homepage_generation as route


def _preflight_context(query_string: dict):
    app = Flask(__name__)
    return app.test_request_context(
        "/api/_mod/homepage/preflight",
        method="GET",
        query_string=query_string,
        headers={"User-Agent": "homepage-policy-test", "Origin": "https://timrx.live"},
        environ_base={"REMOTE_ADDR": "203.0.113.9"},
    )


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


def test_homepage_intent_explicit_image_beats_3d_object_words():
    intent = route._parse_homepage_intent(
        "generate an image using nano banana in 4k of a robot keychain",
        {"requested_type": "3d"},
    )
    assert intent["generation_type"] == "image"
    assert intent["provider"] == "nano_banana"
    assert intent["image_size"] == "4K"


def test_homepage_intent_keeps_printable_mesh_as_3d():
    intent = route._parse_homepage_intent("make a printable STL robot keychain")
    assert intent["generation_type"] == "3d"


def test_homepage_intent_parses_video_provider_settings():
    intent = route._parse_homepage_intent("make a vertical Veo 8 second video in 4k")
    assert intent["generation_type"] == "video"
    assert intent["provider"] == "vertex"
    assert intent["strict_provider"] is True
    assert intent["aspect_ratio"] == "9:16"
    assert intent["duration_seconds"] == 8
    assert intent["resolution"] == "4k"


def test_preflight_respects_requested_output_type_without_prompt():
    assert route._parse_homepage_intent("", {"requested_type": "image"})["generation_type"] == "image"
    assert route._parse_homepage_intent("", {"requested_type": "video"})["generation_type"] == "video"
    assert route._parse_homepage_intent("", {"requested_type": "3d"})["generation_type"] == "3d"


def test_anonymous_preflight_allows_one_free_generation_per_type(monkeypatch):
    monkeypatch.setattr(route.config, "HOMEPAGE_FREE_ENABLED", True)
    monkeypatch.setattr(route.config, "TURNSTILE_ENABLED", True)
    monkeypatch.setattr(route.config, "TURNSTILE_SECRET_KEY", "turnstile-secret")
    monkeypatch.setattr(route.config, "HOMEPAGE_FREE_ALLOW_IMAGE", True)
    monkeypatch.setattr(route.config, "HOMEPAGE_FREE_ALLOW_VIDEO", True)
    monkeypatch.setattr(route.config, "HOMEPAGE_FREE_ALLOW_3D", True)
    monkeypatch.setattr(route.config, "HOMEPAGE_FREE_IMAGE_PROVIDER", "nano_banana")
    monkeypatch.setattr(route.config, "HOMEPAGE_FREE_VIDEO_PROVIDER", "fal_seedance")
    monkeypatch.setattr(route.config, "MESHY_API_KEY", "meshy-key")
    monkeypatch.setenv("FREE_GENERATION_HASH_SALT", "test-salt")
    monkeypatch.setattr(route, "is_turnstile_enabled", lambda: True)
    monkeypatch.setattr(route, "has_paid_balance", lambda *args: False)
    monkeypatch.setattr(route, "get_current_trial_state", lambda generation_type=None: {
        "entitlements": {
            "image": {"eligible": True, "remaining": True, "status": "available"},
            "video": {"eligible": True, "remaining": True, "status": "available"},
            "3d": {"eligible": True, "remaining": True, "status": "available"},
        }
    })

    def fake_action(generation_type, intent):
        params = {
            "image": ("piapi_image_generate_2k", 12, {"provider": "nano_banana", "image_size": "2K"}),
            "video": ("fal_seedance_text_generate_5s", 45, {"provider": "fal_seedance", "duration_seconds": 5, "resolution": "720p"}),
            "3d": ("text_to_3d_generate", 20, {"provider": "meshy"}),
        }[generation_type]
        return params

    monkeypatch.setattr(route, "_action_for_generation_type", fake_action)

    for requested_type in ("image", "video", "3d"):
        with _preflight_context({"requested_type": requested_type}):
            g.identity_id = None
            response = route.homepage_generation_preflight.__wrapped__()
            data = response.get_json()

        assert data["generation_type"] == requested_type
        assert data["mode"] == "free"
        assert data["challenge_required"] is True


def test_anonymous_preflight_blocks_when_type_entitlement_is_used(monkeypatch):
    monkeypatch.setattr(route.config, "HOMEPAGE_FREE_ENABLED", True)
    monkeypatch.setattr(route.config, "TURNSTILE_ENABLED", True)
    monkeypatch.setattr(route.config, "TURNSTILE_SECRET_KEY", "turnstile-secret")
    monkeypatch.setattr(route.config, "HOMEPAGE_FREE_ALLOW_IMAGE", True)
    monkeypatch.setattr(route.config, "HOMEPAGE_FREE_IMAGE_PROVIDER", "nano_banana")
    monkeypatch.setenv("FREE_GENERATION_HASH_SALT", "test-salt")
    monkeypatch.setattr(route, "has_paid_balance", lambda *args: False)
    monkeypatch.setattr(route, "_action_for_generation_type", lambda generation_type, intent: (
        "piapi_image_generate_2k",
        12,
        {"provider": "nano_banana", "image_size": "2K"},
    ))
    monkeypatch.setattr(route, "get_current_trial_state", lambda generation_type=None: {
        "entitlements": {
            "image": {"eligible": False, "remaining": False, "status": "completed"},
            "video": {"eligible": True, "remaining": True, "status": "available"},
            "3d": {"eligible": True, "remaining": True, "status": "available"},
        }
    })

    with _preflight_context({"requested_type": "image"}):
        g.identity_id = None
        data = route.homepage_generation_preflight.__wrapped__().get_json()

    assert data["generation_type"] == "image"
    assert data["mode"] == "blocked"
    assert data["entitlement"]["remaining"] is False
