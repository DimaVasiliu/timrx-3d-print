#!/usr/bin/env python3
import os
import sys
from typing import Iterable

try:
    import psycopg
    from psycopg.rows import dict_row
except Exception as exc:
    print(f"[dedupe_db] ERROR: psycopg not available: {exc}")
    sys.exit(1)


GROUP_BY_UPSTREAM_SQL = """
SELECT provider, upstream_id, COUNT(*) AS cnt
FROM timrx_app.images
WHERE upstream_id IS NOT NULL
GROUP BY provider, upstream_id
HAVING COUNT(*) > 1
ORDER BY cnt DESC;
""".strip()

GROUP_BY_IMAGE_URL_SQL = """
SELECT provider, image_url, COUNT(*) AS cnt
FROM timrx_app.images
WHERE upstream_id IS NULL AND image_url IS NOT NULL
GROUP BY provider, image_url
HAVING COUNT(*) > 1
ORDER BY cnt DESC;
""".strip()

FETCH_BY_UPSTREAM_SQL = """
SELECT id, image_url, thumbnail_url, created_at
FROM timrx_app.images
WHERE provider = %s AND upstream_id = %s
ORDER BY created_at DESC;
""".strip()

FETCH_BY_IMAGE_URL_SQL = """
SELECT id, image_url, thumbnail_url, created_at
FROM timrx_app.images
WHERE provider = %s AND image_url = %s AND upstream_id IS NULL
ORDER BY created_at DESC;
""".strip()

UPDATE_HISTORY_SQL = """
UPDATE timrx_app.history_items
SET image_id = %s
WHERE image_id = %s;
""".strip()

DELETE_IMAGE_SQL = """
DELETE FROM timrx_app.images
WHERE id = %s;
""".strip()


def is_s3_url(url: str) -> bool:
    return isinstance(url, str) and "s3." in url and "amazonaws.com" in url


def choose_keep(rows: Iterable[dict]) -> dict:
    s3_rows = [r for r in rows if is_s3_url(r.get("image_url")) or is_s3_url(r.get("thumbnail_url"))]
    if s3_rows:
        return max(s3_rows, key=lambda r: r.get("created_at") or 0)
    return max(rows, key=lambda r: r.get("created_at") or 0)


def dedupe_group(cur, rows: list[dict]) -> tuple[int, int]:
    if len(rows) <= 1:
        return (0, 0)
    keep = choose_keep(rows)
    keep_id = keep["id"]
    removed = 0
    updated = 0
    for row in rows:
        if row["id"] == keep_id:
            continue
        cur.execute(UPDATE_HISTORY_SQL, (keep_id, row["id"]))
        updated += cur.rowcount
        cur.execute(DELETE_IMAGE_SQL, (row["id"],))
        removed += cur.rowcount
    return updated, removed


def main() -> int:
    db_url = os.getenv("DATABASE_URL", "").strip()
    if not db_url:
        print("[dedupe_db] ERROR: DATABASE_URL not set")
        return 1
    try:
        from urllib.parse import urlparse
        parsed = urlparse(db_url)
        safe_netloc = parsed.hostname or parsed.netloc
        safe_db = (parsed.path or "").lstrip("/")
    except Exception:
        safe_netloc = "unknown"
        safe_db = "unknown"
    app_schema = os.getenv("APP_SCHEMA", "timrx_app")
    bucket = os.getenv("AWS_BUCKET_MODELS", "").strip() or "unset"
    print(f"[dedupe_db] Target DB: host={safe_netloc} db={safe_db} schema={app_schema}")
    print(f"[dedupe_db] S3 bucket={bucket} apply=always limit=n/a")

    print("[dedupe_db] Using SQL queries:")
    print(GROUP_BY_UPSTREAM_SQL)
    print(GROUP_BY_IMAGE_URL_SQL)
    print(UPDATE_HISTORY_SQL)
    print(DELETE_IMAGE_SQL)

    conn = psycopg.connect(db_url)
    total_groups = 0
    total_duplicates = 0
    total_updated = 0
    total_removed = 0

    try:
        with conn:
            with conn.cursor(row_factory=dict_row) as cur:
                cur.execute(GROUP_BY_UPSTREAM_SQL)
                upstream_groups = cur.fetchall()
                for group in upstream_groups:
                    total_groups += 1
                    provider = group["provider"]
                    upstream_id = group["upstream_id"]
                    cur.execute(FETCH_BY_UPSTREAM_SQL, (provider, upstream_id))
                    rows = cur.fetchall()
                    total_duplicates += max(len(rows) - 1, 0)
                    updated, removed = dedupe_group(cur, rows)
                    total_updated += updated
                    total_removed += removed

                cur.execute(GROUP_BY_IMAGE_URL_SQL)
                url_groups = cur.fetchall()
                for group in url_groups:
                    total_groups += 1
                    provider = group["provider"]
                    image_url = group["image_url"]
                    cur.execute(FETCH_BY_IMAGE_URL_SQL, (provider, image_url))
                    rows = cur.fetchall()
                    total_duplicates += max(len(rows) - 1, 0)
                    updated, removed = dedupe_group(cur, rows)
                    total_updated += updated
                    total_removed += removed
    finally:
        conn.close()

    print("[dedupe_db] Summary:")
    print(f"[dedupe_db] Duplicate groups found: {total_groups}")
    print(f"[dedupe_db] Duplicate rows found: {total_duplicates}")
    print(f"[dedupe_db] history_items.image_id updated: {total_updated}")
    print(f"[dedupe_db] Duplicate rows removed: {total_removed}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
