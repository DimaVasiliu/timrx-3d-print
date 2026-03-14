#!/usr/bin/env python3
"""
Deduplicate rows in normalized asset tables (images, models, videos).

For each table, finds duplicate groups by (provider, upstream_id) and by
(provider, asset_url), keeps the best row (prefers S3 URLs, then newest),
re-points history_items FK references, and deletes the duplicates.

Usage:
    DATABASE_URL=... python scripts/dedupe_db.py
    DATABASE_URL=... python scripts/dedupe_db.py --dry-run
"""
import argparse
import os
import sys
from typing import Iterable

try:
    import psycopg
    from psycopg.rows import dict_row
except Exception as exc:
    print(f"[dedupe_db] ERROR: psycopg not available: {exc}")
    sys.exit(1)


APP_SCHEMA = os.getenv("APP_SCHEMA", "timrx_app")

# ── Images ────────────────────────────────────────────────────
IMG_GROUP_BY_UPSTREAM = f"""
SELECT provider, upstream_id, COUNT(*) AS cnt
FROM {APP_SCHEMA}.images
WHERE upstream_id IS NOT NULL
GROUP BY provider, upstream_id
HAVING COUNT(*) > 1
ORDER BY cnt DESC;
""".strip()

IMG_GROUP_BY_URL = f"""
SELECT provider, image_url, COUNT(*) AS cnt
FROM {APP_SCHEMA}.images
WHERE upstream_id IS NULL AND image_url IS NOT NULL
GROUP BY provider, image_url
HAVING COUNT(*) > 1
ORDER BY cnt DESC;
""".strip()

IMG_FETCH_BY_UPSTREAM = f"""
SELECT id, image_url, thumbnail_url, created_at
FROM {APP_SCHEMA}.images
WHERE provider = %s AND upstream_id = %s
ORDER BY created_at DESC;
""".strip()

IMG_FETCH_BY_URL = f"""
SELECT id, image_url, thumbnail_url, created_at
FROM {APP_SCHEMA}.images
WHERE provider = %s AND image_url = %s AND upstream_id IS NULL
ORDER BY created_at DESC;
""".strip()

IMG_UPDATE_HISTORY = f"""
UPDATE {APP_SCHEMA}.history_items
SET image_id = %s
WHERE image_id = %s;
""".strip()

IMG_DELETE = f"""
DELETE FROM {APP_SCHEMA}.images
WHERE id = %s;
""".strip()

# ── Models ────────────────────────────────────────────────────
MDL_GROUP_BY_UPSTREAM = f"""
SELECT provider, upstream_id, COUNT(*) AS cnt
FROM {APP_SCHEMA}.models
WHERE upstream_id IS NOT NULL
GROUP BY provider, upstream_id
HAVING COUNT(*) > 1
ORDER BY cnt DESC;
""".strip()

MDL_GROUP_BY_URL = f"""
SELECT provider, glb_url, COUNT(*) AS cnt
FROM {APP_SCHEMA}.models
WHERE upstream_id IS NULL AND glb_url IS NOT NULL
GROUP BY provider, glb_url
HAVING COUNT(*) > 1
ORDER BY cnt DESC;
""".strip()

MDL_FETCH_BY_UPSTREAM = f"""
SELECT id, glb_url, thumbnail_url, created_at
FROM {APP_SCHEMA}.models
WHERE provider = %s AND upstream_id = %s
ORDER BY created_at DESC;
""".strip()

MDL_FETCH_BY_URL = f"""
SELECT id, glb_url, thumbnail_url, created_at
FROM {APP_SCHEMA}.models
WHERE provider = %s AND glb_url = %s AND upstream_id IS NULL
ORDER BY created_at DESC;
""".strip()

MDL_UPDATE_HISTORY = f"""
UPDATE {APP_SCHEMA}.history_items
SET model_id = %s
WHERE model_id = %s;
""".strip()

MDL_DELETE = f"""
DELETE FROM {APP_SCHEMA}.models
WHERE id = %s;
""".strip()

# ── Videos ────────────────────────────────────────────────────
VID_GROUP_BY_UPSTREAM = f"""
SELECT provider, upstream_id, COUNT(*) AS cnt
FROM {APP_SCHEMA}.videos
WHERE upstream_id IS NOT NULL
GROUP BY provider, upstream_id
HAVING COUNT(*) > 1
ORDER BY cnt DESC;
""".strip()

VID_GROUP_BY_URL = f"""
SELECT provider, video_url, COUNT(*) AS cnt
FROM {APP_SCHEMA}.videos
WHERE upstream_id IS NULL AND video_url IS NOT NULL
GROUP BY provider, video_url
HAVING COUNT(*) > 1
ORDER BY cnt DESC;
""".strip()

VID_FETCH_BY_UPSTREAM = f"""
SELECT id, video_url, thumbnail_url, created_at
FROM {APP_SCHEMA}.videos
WHERE provider = %s AND upstream_id = %s
ORDER BY created_at DESC;
""".strip()

VID_FETCH_BY_URL = f"""
SELECT id, video_url, thumbnail_url, created_at
FROM {APP_SCHEMA}.videos
WHERE provider = %s AND video_url = %s AND upstream_id IS NULL
ORDER BY created_at DESC;
""".strip()

VID_UPDATE_HISTORY = f"""
UPDATE {APP_SCHEMA}.history_items
SET video_id = %s
WHERE video_id = %s;
""".strip()

VID_DELETE = f"""
DELETE FROM {APP_SCHEMA}.videos
WHERE id = %s;
""".strip()


# ── Helpers ───────────────────────────────────────────────────
def is_s3_url(url: str) -> bool:
    return isinstance(url, str) and "s3." in url and "amazonaws.com" in url


def _has_s3(row: dict) -> bool:
    """Check if any URL-like field in the row points to S3."""
    for key in ("image_url", "glb_url", "video_url", "thumbnail_url"):
        if is_s3_url(row.get(key, "")):
            return True
    return False


def choose_keep(rows: Iterable[dict]) -> dict:
    """Pick the best row to keep: prefer S3 URLs, then newest created_at."""
    rows_list = list(rows)
    s3_rows = [r for r in rows_list if _has_s3(r)]
    if s3_rows:
        return max(s3_rows, key=lambda r: r.get("created_at") or 0)
    return max(rows_list, key=lambda r: r.get("created_at") or 0)


def dedupe_group(cur, rows: list[dict], update_sql: str, delete_sql: str, *, dry_run: bool = False) -> tuple[int, int]:
    """Dedupe a single group: re-point FKs and delete duplicates."""
    if len(rows) <= 1:
        return (0, 0)
    keep = choose_keep(rows)
    keep_id = keep["id"]
    removed = 0
    updated = 0
    for row in rows:
        if row["id"] == keep_id:
            continue
        if dry_run:
            print(f"  [dry-run] would re-point {row['id']} -> {keep_id} and delete {row['id']}")
            removed += 1
            continue
        cur.execute(update_sql, (keep_id, row["id"]))
        updated += cur.rowcount
        cur.execute(delete_sql, (row["id"],))
        removed += cur.rowcount
    return updated, removed


def dedupe_table(
    cur,
    label: str,
    group_upstream_sql: str,
    fetch_upstream_sql: str,
    group_url_sql: str,
    fetch_url_sql: str,
    url_column: str,
    update_history_sql: str,
    delete_sql: str,
    *,
    dry_run: bool = False,
) -> dict:
    """Run dedup for one asset table. Returns per-table stats."""
    stats = {"groups": 0, "duplicates": 0, "updated": 0, "removed": 0}

    # Pass 1: duplicates by (provider, upstream_id)
    cur.execute(group_upstream_sql)
    upstream_groups = cur.fetchall()
    for group in upstream_groups:
        stats["groups"] += 1
        cur.execute(fetch_upstream_sql, (group["provider"], group["upstream_id"]))
        rows = cur.fetchall()
        stats["duplicates"] += max(len(rows) - 1, 0)
        updated, removed = dedupe_group(cur, rows, update_history_sql, delete_sql, dry_run=dry_run)
        stats["updated"] += updated
        stats["removed"] += removed

    # Pass 2: duplicates by (provider, asset_url) where upstream_id IS NULL
    cur.execute(group_url_sql)
    url_groups = cur.fetchall()
    for group in url_groups:
        stats["groups"] += 1
        cur.execute(fetch_url_sql, (group["provider"], group[url_column]))
        rows = cur.fetchall()
        stats["duplicates"] += max(len(rows) - 1, 0)
        updated, removed = dedupe_group(cur, rows, update_history_sql, delete_sql, dry_run=dry_run)
        stats["updated"] += updated
        stats["removed"] += removed

    print(f"[dedupe_db] {label}: {stats['groups']} groups, {stats['duplicates']} duplicates, "
          f"{stats['updated']} FK updates, {stats['removed']} rows deleted")
    return stats


def main() -> int:
    parser = argparse.ArgumentParser(description="Deduplicate images, models, and videos tables.")
    parser.add_argument("--dry-run", action="store_true", help="Print what would be done without modifying the DB.")
    args = parser.parse_args()

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

    mode = "dry-run" if args.dry_run else "apply"
    print(f"[dedupe_db] Target DB: host={safe_netloc} db={safe_db} schema={APP_SCHEMA} mode={mode}")

    conn = psycopg.connect(db_url)
    all_stats = {}

    try:
        with conn:
            with conn.cursor(row_factory=dict_row) as cur:
                # ── Images ────────────────────────────
                all_stats["images"] = dedupe_table(
                    cur, "images",
                    IMG_GROUP_BY_UPSTREAM, IMG_FETCH_BY_UPSTREAM,
                    IMG_GROUP_BY_URL, IMG_FETCH_BY_URL,
                    "image_url",
                    IMG_UPDATE_HISTORY, IMG_DELETE,
                    dry_run=args.dry_run,
                )

                # ── Models ────────────────────────────
                all_stats["models"] = dedupe_table(
                    cur, "models",
                    MDL_GROUP_BY_UPSTREAM, MDL_FETCH_BY_UPSTREAM,
                    MDL_GROUP_BY_URL, MDL_FETCH_BY_URL,
                    "glb_url",
                    MDL_UPDATE_HISTORY, MDL_DELETE,
                    dry_run=args.dry_run,
                )

                # ── Videos ────────────────────────────
                all_stats["videos"] = dedupe_table(
                    cur, "videos",
                    VID_GROUP_BY_UPSTREAM, VID_FETCH_BY_UPSTREAM,
                    VID_GROUP_BY_URL, VID_FETCH_BY_URL,
                    "video_url",
                    VID_UPDATE_HISTORY, VID_DELETE,
                    dry_run=args.dry_run,
                )
    finally:
        conn.close()

    # ── Summary ───────────────────────────────────────────────
    print()
    print("[dedupe_db] ── Summary ──")
    total_groups = total_dupes = total_updated = total_removed = 0
    for table, s in all_stats.items():
        total_groups += s["groups"]
        total_dupes += s["duplicates"]
        total_updated += s["updated"]
        total_removed += s["removed"]
        if s["groups"]:
            print(f"  {table:8s}: {s['groups']} groups, {s['duplicates']} duplicates, "
                  f"{s['updated']} FK updates, {s['removed']} deleted")

    if total_groups == 0:
        print("  No duplicates found across any table.")
    else:
        print(f"  {'total':8s}: {total_groups} groups, {total_dupes} duplicates, "
              f"{total_updated} FK updates, {total_removed} deleted")

    if args.dry_run and total_groups:
        print()
        print("[dedupe_db] Dry-run complete. Re-run without --dry-run to apply changes.")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
