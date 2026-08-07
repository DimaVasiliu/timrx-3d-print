from __future__ import annotations

import unittest
from datetime import datetime, timedelta, timezone

from backend.services.inspire_curation_service import (
    curate_feed_assets,
    model_eligibility,
)


def _model(item_id: str, prompt: str, **extra):
    item = {
        "id": item_id,
        "type": "model",
        "title": prompt,
        "prompt": prompt,
        "thumb_preview": f"https://cdn.example/{item_id}.jpg",
        "glb_url": f"https://cdn.example/{item_id}.glb",
        "stage": "preview",
        "created_at": datetime.now(timezone.utc),
        "inspire_status": "auto",
    }
    item.update(extra)
    return item


class InspireCurationTests(unittest.TestCase):
    def test_rejects_image_to_3d_even_when_subject_matches(self):
        item = _model(
            "face",
            "fantasy warrior character from my portrait",
            generation_action="image_to_3d_generate",
        )
        self.assertEqual(model_eligibility(item), (False, "image_to_3d"))

    def test_rejects_refined_derivative_of_image_to_3d_lineage(self):
        item = _model(
            "refined-face",
            "fantasy warrior character",
            generation_action="texture",
            source_generation_action="image_to_3d_generate",
        )
        self.assertEqual(model_eligibility(item), (False, "image_to_3d"))

    def test_rejects_real_person_photo_language(self):
        item = _model("family", "3D model of three people from a family photo")
        self.assertEqual(model_eligibility(item), (False, "real_person"))

    def test_auto_feed_requires_a_curated_subject(self):
        self.assertEqual(model_eligibility(_model("hook", "plastic lattice hook")), (False, "subject_not_curated"))
        self.assertEqual(model_eligibility(_model("dragon", "ornate printable crystal dragon creature")), (True, "eligible"))

    def test_manual_approval_can_expand_subjects_but_not_person_scans(self):
        approved = _model("vessel", "abstract ceramic vessel", inspire_status="approved")
        person = _model("selfie", "selfie converted to sculpture", inspire_status="approved")
        self.assertTrue(model_eligibility(approved)[0])
        self.assertFalse(model_eligibility(person)[0])

    def test_lineage_dedupe_prefers_finished_stage(self):
        now = datetime.now(timezone.utc)
        preview = _model("preview", "printable dragon creature", lineage_origin_id="lineage-1", created_at=now)
        textured = _model(
            "textured",
            "printable dragon creature",
            lineage_origin_id="lineage-1",
            stage="textured",
            created_at=now - timedelta(minutes=5),
            thumb_refined="https://cdn.example/textured-full.jpg",
        )
        models, _, _, stats = curate_feed_assets([preview, textured], [], [])
        self.assertEqual([item["id"] for item in models], ["textured"])
        self.assertEqual(stats["models_deduplicated"], 1)

    def test_exact_media_duplicates_are_removed_for_images_and_videos(self):
        now = datetime.now(timezone.utc)
        images = [
            {"id": "i1", "image_url": "https://cdn.example/image.png?sig=1", "created_at": now},
            {"id": "i2", "image_url": "https://cdn.example/image.png?sig=2", "created_at": now - timedelta(seconds=1)},
        ]
        videos = [
            {"id": "v1", "video_url": "https://cdn.example/video.mp4", "created_at": now},
            {"id": "v2", "video_url": "https://cdn.example/video.mp4", "created_at": now - timedelta(seconds=1)},
        ]
        _, curated_images, curated_videos, stats = curate_feed_assets([], images, videos)
        self.assertEqual(len(curated_images), 1)
        self.assertEqual(len(curated_videos), 1)
        self.assertEqual(stats["images_deduplicated"], 1)
        self.assertEqual(stats["videos_deduplicated"], 1)


if __name__ == "__main__":
    unittest.main()
