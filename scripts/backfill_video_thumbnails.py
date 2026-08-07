#!/usr/bin/env python3
"""
Backfill Video Thumbnails Script
---------------------------------
Generates thumbnails for all existing videos that don't have one.

Usage:
    # Dry-run (shows what would be processed):
    python scripts/backfill_video_thumbnails.py

    # Apply changes:
    python scripts/backfill_video_thumbnails.py --apply

    # Limit to N videos:
    python scripts/backfill_video_thumbnails.py --apply --limit 10
"""
import argparse
import base64
import json
import os
import sys

try:
    import psycopg
    from psycopg.rows import dict_row
except Exception as exc:
    print(f"[backfill_video_thumbnails] ERROR: psycopg not available: {exc}")
    sys.exit(1)

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

try:
    from backend.services.gemini_video_service import extract_video_thumbnail, download_video_bytes
    from backend.services.s3_service import safe_upload_to_s3, is_s3_url, get_s3_key_from_url
    from backend.config import AWS_BUCKET_MODELS
except Exception as exc:
    print(f"[backfill_video_thumbnails] ERROR: cannot import backend helpers: {exc}")
    sys.exit(1)

APP_SCHEMA = os.getenv("APP_SCHEMA", "timrx_app")

# Query videos that have video_url but no thumbnail_url
VIDEO_QUERY = f"""
SELECT v.id, v.identity_id, v.video_url, v.thumbnail_url, v.prompt, v.title
FROM {APP_SCHEMA}.videos v
WHERE v.video_url IS NOT NULL
  AND v.video_url != ''
  AND (v.thumbnail_url IS NULL OR v.thumbnail_url = '')
ORDER BY v.created_at DESC
""".strip()

# Also query history_items for videos
HISTORY_VIDEO_QUERY = f"""
SELECT h.id, h.identity_id, h.video_url, h.thumbnail_url, h.video_id, h.prompt, h.title
FROM {APP_SCHEMA}.history_items h
WHERE h.item_type = 'video'
  AND h.video_url IS NOT NULL
  AND h.video_url != ''
  AND (h.thumbnail_url IS NULL OR h.thumbnail_url = '')
ORDER BY h.created_at DESC
""".strip()


def backfill_video_thumbnails(cur, rows: list[dict], apply: bool, source: str = "videos") -> int:
    """
    Process videos and generate thumbnails.

    Args:
        cur: Database cursor
        rows: List of video rows
        apply: Whether to actually apply changes
        source: "videos" or "history_items" table

    Returns:
        Number of successfully processed videos
    """
    success_count = 0

    for idx, row in enumerate(rows, start=1):
        video_url = row.get("video_url")
        if not video_url:
            continue

        video_id = str(row["id"])
        user_id = row.get("identity_id")

        print(f"[backfill_video_thumbnails] {source} {idx}: id={video_id} url={video_url[:80]}...")

        if not apply:
            continue

        savepoint = f"bf_video_{source}_{idx}"
        cur.execute(f"SAVEPOINT {savepoint}")

        try:
            # Download video
            print(f"  Downloading video...")
            video_bytes, content_type = download_video_bytes(video_url)

            if not video_bytes:
                print(f"  Failed to download video, skipping")
                cur.execute(f"ROLLBACK TO SAVEPOINT {savepoint}")
                cur.execute(f"RELEASE SAVEPOINT {savepoint}")
                continue

            print(f"  Downloaded {len(video_bytes)} bytes")

            # Extract thumbnail
            print(f"  Extracting thumbnail...")
            thumb_bytes = extract_video_thumbnail(video_bytes, timestamp_sec=1.0)

            if not thumb_bytes:
                print(f"  Failed to extract thumbnail, skipping")
                cur.execute(f"ROLLBACK TO SAVEPOINT {savepoint}")
                cur.execute(f"RELEASE SAVEPOINT {savepoint}")
                continue

            print(f"  Extracted thumbnail: {len(thumb_bytes)} bytes")

            # Upload thumbnail to S3
            thumb_b64 = f"data:image/jpeg;base64,{base64.b64encode(thumb_bytes).decode('utf-8')}"
            user_folder = str(user_id) if user_id else "public"

            s3_thumbnail_url = safe_upload_to_s3(
                thumb_b64,
                "image/jpeg",
                "thumbnails",
                f"veo_thumb_{video_id}",
                user_id=user_id,
                key_base=f"thumbnails/{user_folder}/{video_id}.jpg",
                provider="google",
            )

            if not s3_thumbnail_url:
                print(f"  Failed to upload thumbnail to S3, skipping")
                cur.execute(f"ROLLBACK TO SAVEPOINT {savepoint}")
                cur.execute(f"RELEASE SAVEPOINT {savepoint}")
                continue

            print(f"  Uploaded thumbnail: {s3_thumbnail_url}")

            # Update database
            thumb_s3_key = get_s3_key_from_url(s3_thumbnail_url)

            if source == "videos":
                cur.execute(f"""
                    UPDATE {APP_SCHEMA}.videos
                    SET thumbnail_url = %s,
                        thumbnail_s3_key = %s,
                        updated_at = NOW()
                    WHERE id = %s
                """, (s3_thumbnail_url, thumb_s3_key, video_id))

                # Also update corresponding history_items
                cur.execute(f"""
                    UPDATE {APP_SCHEMA}.history_items
                    SET thumbnail_url = %s,
                        updated_at = NOW()
                    WHERE video_id = %s
                """, (s3_thumbnail_url, video_id))

            else:  # history_items
                cur.execute(f"""
                    UPDATE {APP_SCHEMA}.history_items
                    SET thumbnail_url = %s,
                        updated_at = NOW()
                    WHERE id = %s
                """, (s3_thumbnail_url, video_id))

                # Also update videos table if video_id exists
                video_fk = row.get("video_id")
                if video_fk:
                    cur.execute(f"""
                        UPDATE {APP_SCHEMA}.videos
                        SET thumbnail_url = %s,
                            thumbnail_s3_key = %s,
                            updated_at = NOW()
                        WHERE id = %s
                    """, (s3_thumbnail_url, thumb_s3_key, video_fk))

            cur.execute(f"RELEASE SAVEPOINT {savepoint}")
            success_count += 1
            print(f"  Updated database")

        except Exception as exc:
            cur.execute(f"ROLLBACK TO SAVEPOINT {savepoint}")
            cur.execute(f"RELEASE SAVEPOINT {savepoint}")
            print(f"  ERROR: {exc}")
            import traceback
            traceback.print_exc()

    return success_count


def main() -> int:
    parser = argparse.ArgumentParser(description="Backfill video thumbnails for existing videos.")
    parser.add_argument("--apply", action="store_true", help="Actually generate and upload thumbnails.")
    parser.add_argument("--limit", type=int, default=0, help="Limit rows per pass (0 = no limit).")
    parser.add_argument("--videos-only", action="store_true", help="Only process videos table.")
    parser.add_argument("--history-only", action="store_true", help="Only process history_items table.")
    args = parser.parse_args()

    db_url = os.getenv("DATABASE_URL", "").strip()
    if not db_url:
        print("[backfill_video_thumbnails] ERROR: DATABASE_URL not set")
        return 1

    if args.apply and not AWS_BUCKET_MODELS:
        print("[backfill_video_thumbnails] ERROR: AWS_BUCKET_MODELS not configured")
        return 1

    # Print connection info
    try:
        from urllib.parse import urlparse
        parsed = urlparse(db_url)
        safe_netloc = parsed.hostname or parsed.netloc
        safe_db = (parsed.path or "").lstrip("/")
    except Exception:
        safe_netloc = "unknown"
        safe_db = "unknown"

    print(f"[backfill_video_thumbnails] Target DB: host={safe_netloc} db={safe_db} schema={APP_SCHEMA}")
    print(f"[backfill_video_thumbnails] S3 bucket={AWS_BUCKET_MODELS or 'unset'} apply={args.apply} limit={args.limit}")

    conn = psycopg.connect(db_url)
    with conn.cursor() as cur:
        cur.execute("SET search_path TO timrx_app, timrx_billing, public;")

    total_success = 0

    with conn, conn.cursor(row_factory=dict_row) as cur:
        # Process videos table
        if not args.history_only:
            query = VIDEO_QUERY + (f" LIMIT {args.limit}" if args.limit else "")
            cur.execute(query)
            video_rows = cur.fetchall()
            print(f"[backfill_video_thumbnails] videos table candidates: {len(video_rows)}")
            total_success += backfill_video_thumbnails(cur, video_rows, args.apply, "videos")

        # Process history_items table
        if not args.videos_only:
            query = HISTORY_VIDEO_QUERY + (f" LIMIT {args.limit}" if args.limit else "")
            cur.execute(query)
            history_rows = cur.fetchall()
            print(f"[backfill_video_thumbnails] history_items candidates: {len(history_rows)}")
            total_success += backfill_video_thumbnails(cur, history_rows, args.apply, "history_items")

    conn.close()

    if not args.apply:
        print("[backfill_video_thumbnails] Dry-run complete. Re-run with --apply to generate thumbnails.")
    else:
        print(f"[backfill_video_thumbnails] Done. Successfully processed {total_success} videos.")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
