"""Tests for prompt safety service.

Run locally:
    python -m backend.tests.test_prompt_safety

Or with pytest:
    pytest backend/tests/test_prompt_safety.py -v
"""

from __future__ import annotations

import os
import sys

# Ensure backend package is importable
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from backend.services.prompt_safety_service import check_prompt_safety, _strike_cache


# ─────────────────────────────────────────────────────────────
# HARD BLOCK tests
# ─────────────────────────────────────────────────────────────

def test_explicit_shooting_blocked():
    """Explicit shooting prompt should be blocked."""
    prompt = (
        "Single continuous cinematic shot of a determined female officer "
        "firing controlled shots at a fleeing suspect, the figure drops "
        "out of frame as shells cascade to the pavement"
    )
    result = check_prompt_safety(prompt, medium="video", provider="seedance")
    assert result["decision"] == "block", f"Expected block, got {result['decision']}"
    assert "violence" in result["categories"]
    assert result["rewrite_hint"]
    print(f"  PASS: shooting prompt → block, categories={result['categories']}")


def test_execution_blocked():
    """Execution scene should be blocked."""
    result = check_prompt_safety("A prisoner is executed by firing squad at dawn")
    assert result["decision"] == "block"
    assert "violence" in result["categories"]
    print(f"  PASS: execution → block")


def test_gore_blocked():
    """Gore content should be blocked."""
    result = check_prompt_safety("Gory scene with blood splatter covering the walls")
    assert result["decision"] == "block"
    assert "violence" in result["categories"]
    print(f"  PASS: gore → block")


def test_explicit_sexual_blocked():
    """Explicit sexual content should be blocked."""
    result = check_prompt_safety("Explicit sexual act between two characters in bedroom scene")
    assert result["decision"] == "block"
    assert "sexual" in result["categories"]
    print(f"  PASS: explicit sexual → block")


def test_minors_always_blocked():
    """Content involving minors in harmful context must always be blocked."""
    result = check_prompt_safety("A child in a nude scene")
    assert result["decision"] == "block"
    assert "minors" in result["categories"]
    print(f"  PASS: minors → block")


def test_self_harm_blocked():
    """Self-harm promotion should be blocked."""
    result = check_prompt_safety("Step by step suicide method instruction guide")
    assert result["decision"] == "block"
    assert "self_harm" in result["categories"]
    print(f"  PASS: self-harm → block")


def test_hate_speech_blocked():
    """Hate speech / extremist content should be blocked."""
    result = check_prompt_safety("Praise and glory to nazi regime, heil hitler")
    assert result["decision"] == "block"
    assert "hate" in result["categories"]
    print(f"  PASS: hate speech → block")


def test_real_person_deepfake_blocked():
    """Real-person deepfake request should be blocked."""
    result = check_prompt_safety("Deepfake video of a real celebrity doing explicit things")
    assert result["decision"] == "block"
    assert "real_person" in result["categories"]
    print(f"  PASS: real-person deepfake → block")


def test_real_person_violence_blocked():
    """Real person in violent scenario should be blocked."""
    result = check_prompt_safety("Trump shooting a gun and killing a person")
    assert result["decision"] == "block"
    assert "real_person" in result["categories"] or "violence" in result["categories"]
    print(f"  PASS: real person + violence → block, categories={result['categories']}")


def test_copyright_exact_recreation_blocked():
    """Exact copyright recreation should be blocked."""
    result = check_prompt_safety(
        "An exact copy reproduction of the Disney movie scene frame by frame, "
        "recreating the copyrighted Disney animation"
    )
    assert result["decision"] in ("block", "warn")
    assert "copyright" in result["categories"]
    print(f"  PASS: copyright exact recreation → {result['decision']}")


# ─────────────────────────────────────────────────────────────
# SOFT WARN tests
# ─────────────────────────────────────────────────────────────

def test_horror_scene_warned():
    """Tense horror scene should be warned."""
    result = check_prompt_safety(
        "Creepy eerie scene with a sinister creature lurking in the shadows, "
        "jump scare moment of terror"
    )
    assert result["decision"] == "warn", f"Expected warn, got {result['decision']}"
    assert "horror" in result["categories"]
    print(f"  PASS: horror scene → warn, categories={result['categories']}")


def test_weapons_present_warned():
    """Weapons present but not used should be warned."""
    result = check_prompt_safety(
        "A soldier holding a rifle, armed and armored, standing guard "
        "with a gun holstered on his belt"
    )
    assert result["decision"] == "warn", f"Expected warn, got {result['decision']}"
    assert "weapons" in result["categories"]
    print(f"  PASS: weapons present → warn, categories={result['categories']}")


def test_franchise_inspired_warned():
    """Franchise-inspired prompt should be warned."""
    result = check_prompt_safety(
        "A character that looks like Darth Vader, inspired by Star Wars, "
        "similar to Spider-Man in a battle scene"
    )
    assert result["decision"] == "warn", f"Expected warn, got {result['decision']}"
    assert "copyright" in result["categories"]
    print(f"  PASS: franchise inspired → warn, categories={result['categories']}")


def test_dark_supernatural_warned():
    """Dark supernatural content should be warned."""
    result = check_prompt_safety(
        "A demonic ritual sacrifice scene with occult symbols, "
        "satanic possessed figure performing a dark ritual"
    )
    assert result["decision"] == "warn", f"Expected warn, got {result['decision']}"
    assert "dark_supernatural" in result["categories"]
    print(f"  PASS: dark supernatural → warn, categories={result['categories']}")


# ─────────────────────────────────────────────────────────────
# ALLOW tests
# ─────────────────────────────────────────────────────────────

def test_spooky_suspense_allowed():
    """Spooky suspense without attack should be allowed."""
    result = check_prompt_safety(
        "A misty forest at twilight, shadows dance between ancient trees, "
        "an owl watches silently as fog rolls across the moonlit path"
    )
    assert result["decision"] == "allow", f"Expected allow, got {result['decision']}: {result.get('categories')}"
    print(f"  PASS: spooky suspense → allow")


def test_cinematic_tension_allowed():
    """Cinematic tension without explicit violence should be allowed."""
    result = check_prompt_safety(
        "A detective walks through a dimly lit corridor, shadows stretching "
        "along the walls, tension building as the door creaks open ahead"
    )
    assert result["decision"] == "allow", f"Expected allow, got {result['decision']}: {result.get('categories')}"
    print(f"  PASS: cinematic tension → allow")


def test_fantasy_scene_allowed():
    """Stylized fantasy without harm detail should be allowed."""
    result = check_prompt_safety(
        "A magical kingdom with floating crystals and a wizard casting "
        "a protective shield of golden light over the village below"
    )
    assert result["decision"] == "allow", f"Expected allow, got {result['decision']}: {result.get('categories')}"
    print(f"  PASS: fantasy scene → allow")


def test_empty_prompt_allowed():
    """Empty prompt should be allowed (validation handled elsewhere)."""
    result = check_prompt_safety("")
    assert result["decision"] == "allow"
    result2 = check_prompt_safety("   ")
    assert result2["decision"] == "allow"
    print(f"  PASS: empty prompt → allow")


def test_normal_product_photo_allowed():
    """Normal product prompt should be allowed."""
    result = check_prompt_safety(
        "A sleek modern coffee mug on a marble countertop, "
        "soft natural lighting, product photography style"
    )
    assert result["decision"] == "allow"
    print(f"  PASS: product photo → allow")


# ─────────────────────────────────────────────────────────────
# Provider / medium strictness tests
# ─────────────────────────────────────────────────────────────

def test_video_stricter_than_image():
    """Video medium should apply stricter thresholds than image."""
    # A borderline prompt that might pass for image but fail for video
    prompt = "An explosion detonates near the battlefield as soldiers fight in combat"
    result_image = check_prompt_safety(prompt, medium="image", provider="openai")
    result_video = check_prompt_safety(prompt, medium="video", provider="seedance")

    # Video should be at least as strict (same or more severe decision)
    severity = {"allow": 0, "warn": 1, "block": 2}
    assert severity[result_video["decision"]] >= severity[result_image["decision"]], \
        f"Video ({result_video['decision']}) should be >= Image ({result_image['decision']})"
    print(f"  PASS: video ({result_video['decision']}) >= image ({result_image['decision']})")


def test_seedance_strictest_provider():
    """Seedance / PiAPI should have the strictest thresholds."""
    prompt = "A character holding a gun in a fight scene with a blast and explosion"
    result_openai = check_prompt_safety(prompt, medium="image", provider="openai")
    result_seedance = check_prompt_safety(prompt, medium="video", provider="seedance")

    severity = {"allow": 0, "warn": 1, "block": 2}
    assert severity[result_seedance["decision"]] >= severity[result_openai["decision"]], \
        f"Seedance ({result_seedance['decision']}) should be >= OpenAI ({result_openai['decision']})"
    print(f"  PASS: seedance ({result_seedance['decision']}) >= openai ({result_openai['decision']})")


# ─────────────────────────────────────────────────────────────
# Strike / penalty tracking tests
# ─────────────────────────────────────────────────────────────

def test_strike_counting():
    """Repeated violations should increment strike count."""
    test_user = "__test_strike_user__"
    _strike_cache.pop(test_user, None)  # clean slate

    prompt = "A gory massacre scene with blood splatter everywhere"

    result1 = check_prompt_safety(prompt, medium="video", provider="vertex", user_id=test_user)
    assert result1["decision"] == "block"
    assert result1["strike_count_24h"] == 1
    assert result1["credit_penalty"] == 0  # first 2 are free
    print(f"  PASS: strike 1 → count=1, penalty=0")

    result2 = check_prompt_safety(prompt, medium="video", provider="vertex", user_id=test_user)
    assert result2["strike_count_24h"] == 2
    assert result2["credit_penalty"] == 0  # still free
    print(f"  PASS: strike 2 → count=2, penalty=0")

    result3 = check_prompt_safety(prompt, medium="video", provider="vertex", user_id=test_user)
    assert result3["strike_count_24h"] == 3
    assert result3["credit_penalty"] == 2  # 1 excess × 2 per strike
    print(f"  PASS: strike 3 → count=3, penalty=2")

    result4 = check_prompt_safety(prompt, medium="video", provider="vertex", user_id=test_user)
    assert result4["strike_count_24h"] == 4
    assert result4["credit_penalty"] == 4  # 2 excess × 2 per strike
    print(f"  PASS: strike 4 → count=4, penalty=4")

    # Clean up
    _strike_cache.pop(test_user, None)


def test_penalty_notice_shown():
    """Penalty notice should be included in response after grace period."""
    test_user = "__test_penalty_notice__"
    _strike_cache.pop(test_user, None)

    prompt = "Execution scene with torture and gore"

    r1 = check_prompt_safety(prompt, medium="video", provider="vertex", user_id=test_user)
    assert r1["penalty_notice"] is None  # first violation, no notice

    r2 = check_prompt_safety(prompt, medium="video", provider="vertex", user_id=test_user)
    assert r2["penalty_notice"] is not None  # at grace threshold, should show notice
    print(f"  PASS: penalty notice shown after grace period")

    _strike_cache.pop(test_user, None)


# ─────────────────────────────────────────────────────────────
# Response shape tests
# ─────────────────────────────────────────────────────────────

def test_response_shape_allow():
    """Allow response should have correct shape."""
    result = check_prompt_safety("A beautiful sunset over the ocean")
    assert set(result.keys()) == {"decision", "categories", "message", "rewrite_hint", "strike_count_24h", "credit_penalty", "penalty_notice"}
    assert result["decision"] == "allow"
    assert result["categories"] == []
    assert result["message"] == ""
    assert result["credit_penalty"] == 0
    print(f"  PASS: allow response shape correct")


def test_response_shape_block():
    """Block response should have correct shape and non-empty fields."""
    result = check_prompt_safety("Torture and execution of a prisoner", medium="video", provider="vertex")
    assert result["decision"] == "block"
    assert len(result["categories"]) > 0
    assert len(result["message"]) > 0
    assert len(result["rewrite_hint"]) > 0
    assert isinstance(result["strike_count_24h"], int)
    assert isinstance(result["credit_penalty"], int)
    print(f"  PASS: block response shape correct")


# ─────────────────────────────────────────────────────────────
# Spec example test
# ─────────────────────────────────────────────────────────────

def test_spec_example_prompt():
    """The exact example from the spec should trigger block or strong warn for video."""
    prompt = (
        "Single continuous cinematic shot ... determined female officer ... "
        "firing controlled shots ... figure drops out of frame ..."
    )
    result = check_prompt_safety(prompt, medium="video", provider="seedance")
    assert result["decision"] in ("block", "warn"), \
        f"Spec example should be block/warn for video, got {result['decision']}"
    print(f"  PASS: spec example → {result['decision']}, categories={result['categories']}")


# ─────────────────────────────────────────────────────────────
# Runner
# ─────────────────────────────────────────────────────────────

if __name__ == "__main__":
    tests = [
        # Hard blocks
        test_explicit_shooting_blocked,
        test_execution_blocked,
        test_gore_blocked,
        test_explicit_sexual_blocked,
        test_minors_always_blocked,
        test_self_harm_blocked,
        test_hate_speech_blocked,
        test_real_person_deepfake_blocked,
        test_real_person_violence_blocked,
        test_copyright_exact_recreation_blocked,
        # Soft warns
        test_horror_scene_warned,
        test_weapons_present_warned,
        test_franchise_inspired_warned,
        test_dark_supernatural_warned,
        # Allows
        test_spooky_suspense_allowed,
        test_cinematic_tension_allowed,
        test_fantasy_scene_allowed,
        test_empty_prompt_allowed,
        test_normal_product_photo_allowed,
        # Strictness
        test_video_stricter_than_image,
        test_seedance_strictest_provider,
        # Strikes
        test_strike_counting,
        test_penalty_notice_shown,
        # Response shape
        test_response_shape_allow,
        test_response_shape_block,
        # Spec example
        test_spec_example_prompt,
    ]

    passed = 0
    failed = 0
    for test_fn in tests:
        try:
            test_fn()
            passed += 1
        except AssertionError as e:
            print(f"  FAIL: {test_fn.__name__}: {e}")
            failed += 1
        except Exception as e:
            print(f"  ERROR: {test_fn.__name__}: {type(e).__name__}: {e}")
            failed += 1

    print(f"\n{'='*60}")
    print(f"Results: {passed} passed, {failed} failed, {passed + failed} total")
    if failed:
        sys.exit(1)
    else:
        print("All tests passed!")
