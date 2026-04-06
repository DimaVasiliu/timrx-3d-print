from __future__ import annotations

from datetime import datetime, timezone

from flask import Flask

from backend.routes import community as community_module


class _DummyCursor:
    def __init__(self, executed_sql: list[tuple[str, tuple | None]]):
        self.executed_sql = executed_sql
        self._rows = []

    def execute(self, sql, params=None):
        normalized = " ".join(sql.split())
        normalized_params = tuple(params) if params is not None else None
        self.executed_sql.append((normalized, normalized_params))

        if "WITH published_posts AS" in normalized:
            self._rows = [(12, 4, 37)]
            return

        if (
            "SELECT COUNT(*)" in normalized
            and "FROM timrx_app.community_posts cp" in normalized
            and "LIMIT %s OFFSET %s" not in normalized
        ):
            self._rows = [(1,)]
            return

        if (
            "FROM timrx_app.community_posts cp" in normalized
            and "LIMIT %s OFFSET %s" in normalized
        ):
            self._rows = [(
                "post-1",
                "Dima",
                "robot prompt",
                True,
                datetime(2026, 4, 6, 21, 0, tzinfo=timezone.utc),
                None,
                None,
                "hist-1",
                None,
                None,
                None,
                None,
                None,
                "Robot Hero",
                "https://cdn.example/thumb.jpg",
                "https://cdn.example/model.glb",
                "https://cdn.example/image.png",
                "model",
                None,
                "image_to_3d_generate",
                None,
                5,
                12,
                4,
            )]
            return

        if (
            "FROM timrx_app.community_reactions" in normalized
            and "GROUP BY post_id, reaction" in normalized
        ):
            self._rows = [("post-1", "heart", 3), ("post-1", "fire", 2)]
            return

        raise AssertionError(f"Unexpected SQL: {normalized}")

    def fetchone(self):
        return self._rows[0] if self._rows else None

    def fetchall(self):
        return list(self._rows)

    def close(self):
        return None


class _DummyConn:
    def __init__(self, executed_sql: list[tuple[str, tuple | None]]):
        self.executed_sql = executed_sql

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def cursor(self, row_factory=None):
        return _DummyCursor(self.executed_sql)


def _build_test_client(monkeypatch, executed_sql: list[tuple[str, tuple | None]]):
    app = Flask(__name__)
    app.register_blueprint(community_module.bp, url_prefix="/api/_mod")

    monkeypatch.setattr(community_module, "USE_DB", True)
    monkeypatch.setattr(community_module, "_feed_cache", {})
    monkeypatch.setattr(community_module, "_stats_cache", None)
    monkeypatch.setattr(community_module, "get_conn", lambda *args, **kwargs: _DummyConn(executed_sql))

    return app.test_client()


def test_community_feed_returns_stats_and_honors_popular_search(monkeypatch):
    executed_sql: list[tuple[str, tuple | None]] = []
    client = _build_test_client(monkeypatch, executed_sql)

    res = client.get("/api/_mod/community/feed?limit=10&sort=popular&q=robot")
    assert res.status_code == 200

    data = res.get_json()
    assert data["ok"] is True
    assert data["stats"] == {
        "total_posts": 12,
        "total_creators": 4,
        "total_reactions": 37,
    }
    assert len(data["posts"]) == 1
    assert data["posts"][0]["tip_total"] == 12
    assert data["posts"][0]["comment_count"] == 4
    assert data["posts"][0]["reactions"] == {"heart": 3, "fire": 2}

    main_query = next(sql for sql, _ in executed_sql if "LIMIT %s OFFSET %s" in sql)
    main_params = next(params for sql, params in executed_sql if "LIMIT %s OFFSET %s" in sql)
    assert "ORDER BY (reaction_count + (comment_count * 2) + (tip_total * 0.2)) DESC" in main_query
    assert main_params == ("%robot%", "%robot%", "%robot%", "%robot%", 10, 0)


def test_community_feed_invalid_sort_falls_back_to_newest(monkeypatch):
    executed_sql: list[tuple[str, tuple | None]] = []
    client = _build_test_client(monkeypatch, executed_sql)

    res = client.get("/api/_mod/community/feed?sort=not-real")
    assert res.status_code == 200

    main_query = next(sql for sql, _ in executed_sql if "LIMIT %s OFFSET %s" in sql)
    main_params = next(params for sql, params in executed_sql if "LIMIT %s OFFSET %s" in sql)
    assert "ORDER BY cp.created_at DESC" in main_query
    assert main_params == (20, 0)


def test_community_stats_route_is_backward_compatible(monkeypatch):
    executed_sql: list[tuple[str, tuple | None]] = []
    client = _build_test_client(monkeypatch, executed_sql)

    res = client.get("/api/_mod/community/stats")
    assert res.status_code == 200

    data = res.get_json()
    assert data["ok"] is True
    assert data["stats"]["total_posts"] == 12
    assert data["stats"]["total_creators"] == 4
    assert data["stats"]["total_reactions"] == 37
    assert data["total_posts"] == 12
    assert data["total_reactions"] == 37
