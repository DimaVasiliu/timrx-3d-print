from __future__ import annotations

from datetime import datetime, timezone

from flask import Flask

from backend.routes import inspire as inspire_module


def _make_items(kind: str, count: int):
    now = datetime.now(timezone.utc)
    return [
        {
            "id": f"{kind}-{idx}",
            "type": kind,
            "title": f"{kind} title {idx}",
            "prompt": f"{kind} prompt {idx}",
            "created_at": now,
            "thumb_preview": f"https://cdn.example/{kind}-{idx}.jpg",
            "thumbnail_url": f"https://cdn.example/{kind}-{idx}.jpg",
            "glb_url": f"https://cdn.example/{kind}-{idx}.glb" if kind == "model" else None,
            "video_url": f"https://cdn.example/{kind}-{idx}.mp4" if kind == "video" else None,
            "width": 1024 if kind == "image" else None,
            "height": 1024 if kind == "image" else None,
        }
        for idx in range(count)
    ]


class _DummyCursor:
    def close(self):
        return None


class _DummyConn:
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def cursor(self, row_factory=None):
        return _DummyCursor()


def _build_test_client(monkeypatch):
    app = Flask(__name__)
    app.register_blueprint(inspire_module.bp, url_prefix="/api/_mod")

    monkeypatch.setattr(inspire_module, "USE_DB", True)
    monkeypatch.setattr(inspire_module, "get_conn", lambda: _DummyConn())
    monkeypatch.setattr(
        inspire_module,
        "_get_prompt_of_the_day",
        lambda _cursor: {"prompt": "test", "category": "model"},
    )
    monkeypatch.setattr(inspire_module, "_fetch_models", lambda *_args, **_kwargs: _make_items("model", 120))
    monkeypatch.setattr(inspire_module, "_fetch_images", lambda *_args, **_kwargs: _make_items("image", 6))
    monkeypatch.setattr(inspire_module, "_fetch_videos", lambda *_args, **_kwargs: _make_items("video", 4))

    return app.test_client()


def test_inspire_balanced_mix_includes_images_and_videos(monkeypatch):
    client = _build_test_client(monkeypatch)

    res = client.get("/api/_mod/inspire/feed?type=all&limit=24&shuffle=false&mix=balanced")
    assert res.status_code == 200

    data = res.get_json()
    assert data["ok"] is True
    assert len(data["cards"]) == 24

    counts = {"model": 0, "image": 0, "video": 0}
    for card in data["cards"]:
        counts[card["type"]] += 1

    assert counts["image"] > 0
    assert counts["video"] > 0
    assert data["total_available"] == 130


def test_inspire_sequential_mix_can_be_model_only(monkeypatch):
    client = _build_test_client(monkeypatch)

    # Sequential + no shuffle keeps models first, so first page is all models.
    res = client.get("/api/_mod/inspire/feed?type=all&limit=24&shuffle=false&mix=sequential")
    assert res.status_code == 200

    data = res.get_json()
    assert data["ok"] is True
    assert len(data["cards"]) == 24
    assert all(card["type"] == "model" for card in data["cards"])
