"""Shared curation rules for public TimrX asset feeds.

The homepage, workspace asset stage, and Inspire overlay all consume the same
API. Keep public eligibility and deduplication here so a frontend cache or a
new surface cannot accidentally expose uncurated generations.
"""

from __future__ import annotations

import re
from datetime import datetime
from typing import Any, Dict, Iterable, List, Tuple
from urllib.parse import urlsplit


MODEL_SUBJECT_TERMS = {
    "animal", "architecture", "astronaut", "automaton", "bear", "bird",
    "building", "castle", "cat", "character", "creature", "dinosaur",
    "dog", "dragon", "drone", "fantasy", "figurine", "fox", "furniture",
    "helmet", "knight", "lantern", "machine", "mascot", "mech", "miniature",
    "monster", "orc", "owl", "prop", "robot", "rover", "sculpture",
    "spaceship", "statue", "sword", "temple", "tree", "vehicle", "warrior",
    "weapon", "wolf", "wyrm",
}

MODEL_QUALITY_TERMS = {
    "centered", "clean silhouette", "collectible", "detailed", "diorama",
    "game asset", "heroic", "high detail", "isolated", "ornate", "premium",
    "printable", "production", "sculpted", "stylized", "tabletop", "textured",
}

IMAGE_TO_3D_MARKERS = (
    "image-to-3d", "image_to_3d", "image2-3d", "image2_3d", "image3d",
    "img-to-3d", "img_to_3d", "img2-3d", "img2_3d", "photo-to-3d",
    "photo_to_3d", "reference-to-3d", "reference_to_3d",
)

REAL_PERSON_MARKERS = (
    "boy", "face", "face scan", "family photo", "female", "girl", "group photo",
    "human", "human scan", "man", "male", "my face", "people", "person",
    "photo of me", "portrait", "portrait scan", "real person", "selfie",
    "three people", "two people", "person from image", "person from photo",
    "photogrammetry", "woman",
)

GENERIC_MODEL_TITLES = {
    "3d model", "generated model", "image to 3d", "model", "new model",
    "preview", "refined model", "textured model", "untitled",
}

STAGE_SCORE = {
    "texture": 30,
    "textured": 30,
    "retexture": 28,
    "refined": 26,
    "refine": 26,
    "remesh": 22,
    "rigged": 20,
    "rig": 20,
    "preview": 10,
    "initial": 8,
}

_SPACE_RE = re.compile(r"\s+")
_NON_WORD_RE = re.compile(r"[^a-z0-9]+")


def normalize_text(value: Any) -> str:
    return _SPACE_RE.sub(" ", str(value or "").strip().lower())


def prompt_fingerprint(value: Any) -> str:
    return _NON_WORD_RE.sub(" ", normalize_text(value)).strip()


def canonical_media_key(value: Any) -> str:
    raw = str(value or "").strip()
    if not raw:
        return ""
    try:
        parts = urlsplit(raw)
        if parts.scheme and parts.netloc:
            return f"{parts.netloc.lower()}{parts.path.rstrip('/')}"
    except ValueError:
        pass
    return raw.split("?", 1)[0].split("#", 1)[0].rstrip("/").lower()


def _meta(item: Dict[str, Any]) -> Dict[str, Any]:
    value = item.get("meta") or item.get("payload") or {}
    return value if isinstance(value, dict) else {}


def _source_text(item: Dict[str, Any]) -> str:
    meta = _meta(item)
    values = (
        item.get("generation_action"),
        item.get("source_generation_action"),
        item.get("gen_action"),
        item.get("source_type"),
        item.get("stage"),
        meta.get("action"),
        meta.get("source_type"),
        meta.get("generation_type"),
        meta.get("mode"),
    )
    return " ".join(normalize_text(value) for value in values if value)


def is_image_to_3d(item: Dict[str, Any]) -> bool:
    source = _NON_WORD_RE.sub("_", _source_text(item)).strip("_")
    return any(marker.replace("-", "_") in source for marker in IMAGE_TO_3D_MARKERS)


def contains_real_person_marker(item: Dict[str, Any]) -> bool:
    text = prompt_fingerprint(" ".join((
        str(item.get("title") or ""),
        str(item.get("prompt") or ""),
        str(item.get("root_prompt") or ""),
    )))
    padded = f" {text} "
    return any(f" {prompt_fingerprint(marker)} " in padded for marker in REAL_PERSON_MARKERS)


def matched_model_subjects(item: Dict[str, Any]) -> List[str]:
    text = prompt_fingerprint(" ".join((
        str(item.get("title") or ""),
        str(item.get("prompt") or ""),
        str(item.get("root_prompt") or ""),
    )))
    padded = f" {text} "
    return sorted(term for term in MODEL_SUBJECT_TERMS if f" {term} " in padded)


def model_eligibility(item: Dict[str, Any]) -> Tuple[bool, str]:
    status = normalize_text(item.get("inspire_status") or "auto")
    if status == "rejected":
        return False, "moderation_rejected"
    if is_image_to_3d(item):
        return False, "image_to_3d"
    if contains_real_person_marker(item):
        return False, "real_person"

    prompt = normalize_text(item.get("prompt") or item.get("root_prompt"))
    title = normalize_text(item.get("title"))
    if not prompt or (title in GENERIC_MODEL_TITLES and len(prompt) < 12):
        return False, "missing_prompt"

    if status != "approved" and not matched_model_subjects(item):
        return False, "subject_not_curated"
    return True, "eligible"


def quality_score(item: Dict[str, Any]) -> int:
    stored = item.get("quality_score")
    score = int(stored or 0)
    status = normalize_text(item.get("inspire_status") or "auto")
    if status == "approved":
        score += 100

    stage = normalize_text(item.get("stage"))
    score += STAGE_SCORE.get(stage, 12)

    prompt = normalize_text(item.get("prompt") or item.get("root_prompt"))
    subjects = matched_model_subjects(item)
    score += min(24, len(subjects) * 8)
    score += min(18, sum(6 for term in MODEL_QUALITY_TERMS if term in prompt))
    if len(prompt) >= 40:
        score += 8
    if item.get("content_hash"):
        score += 4
    if item.get("thumb_refined"):
        score += 8
    return score


def _created_timestamp(item: Dict[str, Any]) -> float:
    value = item.get("created_at")
    if isinstance(value, datetime):
        return value.timestamp()
    if isinstance(value, str):
        try:
            return datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp()
        except ValueError:
            return 0.0
    return 0.0


def _dedupe_keys(item: Dict[str, Any], include_prompt: bool = True) -> List[str]:
    keys: List[str] = []
    for field in ("lineage_origin_id", "lineage_id"):
        if item.get(field):
            keys.append(f"lineage:{item[field]}")
            break
    if item.get("content_hash"):
        keys.append(f"content:{normalize_text(item['content_hash'])}")
    for field in ("glb_url", "video_url", "image_url", "thumb_preview", "thumbnail_url"):
        media_key = canonical_media_key(item.get(field))
        if media_key:
            keys.append(f"media:{media_key}")
    if include_prompt:
        fingerprint = prompt_fingerprint(item.get("prompt") or item.get("root_prompt"))
        if fingerprint:
            keys.append(f"prompt:{fingerprint}")
    return keys


def dedupe_assets(items: Iterable[Dict[str, Any]], *, models: bool = False) -> List[Dict[str, Any]]:
    candidates = list(items)
    if models:
        candidates.sort(
            key=lambda item: (quality_score(item), _created_timestamp(item)),
            reverse=True,
        )
    else:
        candidates.sort(key=_created_timestamp, reverse=True)

    seen: set[str] = set()
    selected: List[Dict[str, Any]] = []
    for item in candidates:
        keys = _dedupe_keys(item, include_prompt=models)
        if keys and any(key in seen for key in keys):
            continue
        seen.update(keys)
        selected.append(item)
    return selected


def curate_feed_assets(
    models: Iterable[Dict[str, Any]],
    images: Iterable[Dict[str, Any]],
    videos: Iterable[Dict[str, Any]],
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]], Dict[str, int]]:
    model_candidates = list(models)
    image_candidates = list(images)
    video_candidates = list(videos)
    eligible_models: List[Dict[str, Any]] = []
    rejected = 0
    for item in model_candidates:
        eligible, reason = model_eligibility(item)
        if not eligible:
            rejected += 1
            continue
        enriched = dict(item)
        enriched["curation_score"] = quality_score(enriched)
        enriched["curation_reason"] = reason
        eligible_models.append(enriched)

    curated_models = dedupe_assets(eligible_models, models=True)
    curated_images = dedupe_assets(image_candidates)
    curated_videos = dedupe_assets(video_candidates)
    stats = {
        "models_considered": len(model_candidates),
        "models_rejected": rejected,
        "models_deduplicated": len(eligible_models) - len(curated_models),
        "images_deduplicated": len(image_candidates) - len(curated_images),
        "videos_deduplicated": len(video_candidates) - len(curated_videos),
    }
    return curated_models, curated_images, curated_videos, stats
