"""Focused tests for command-bar provider and setting canonicalization."""

from backend.services.command_planner import CommandPlanError, normalize_plan


def test_nano_banana_image_alias_and_portrait_size():
    plan = normalize_plan("Generate a Nano Banana image of an ethereal human, portrait, 4K")
    assert plan["intent"] == "image"
    assert plan["provider"] == "nano_banana"
    assert plan["image_size"] == "4K"
    assert plan["aspect_ratio"] == "9:16"


def test_seedance_15_resolves_to_fal_provider():
    plan = normalize_plan("Create a Seedance 1.5 video of a glass human, 10 seconds")
    assert plan["provider"] == "fal_seedance"
    assert plan["video_duration_seconds"] == 10


def test_seedance_25_preserves_tier_and_resolution():
    plan = normalize_plan("Create a Seedance 2.5 video of a glass human, 15 seconds, 720p")
    assert plan["provider"] == "seedance"
    assert plan["video_tier"] == "v25"
    assert plan["video_duration_seconds"] == 15
    assert plan["video_resolution"] == "720p"


def test_meshy_model_aliases():
    assert normalize_plan("Create a Meshy 5 model of a robot")["model"] == "meshy-5"
    assert normalize_plan("Create a Meshy 6 model of a robot")["model"] == "latest"


def test_unknown_explicit_provider_is_rejected():
    try:
        normalize_plan("Create an image", {"intent": "image", "settings": {"provider": "unknown"}})
    except CommandPlanError as exc:
        assert "not supported" in str(exc)
    else:
        raise AssertionError("unknown provider should not silently become OpenAI")
