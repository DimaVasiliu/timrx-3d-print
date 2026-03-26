#!/usr/bin/env python3
"""
Backfill Image Thumbnails Script
---------------------------------
Generates 400px JPEG thumbnails for existing images that currently have
thumbnail_url == image_url (i.e., no real thumbnail).

Usage:
    # Dry-run (shows what would be processed):
    python scripts/backfill_image_thumbnails.py

    # Apply changes:
    python scripts/backfill_image_thumbnails.py --apply

    # Limit to N images:
    python scripts/backfill_image_thumbnails.py --apply --limit 50

    # Process only a specific provider:
    python scripts/backfill_image_thumbnails.py --apply --provider openai
"""
import argparse
import os
import sys
import time

try:
    import psycopg
    from psycopg.rows import dict_row
except Exception as exc:
    print(f"[backfill_image_thumbnails] ERROR: psycopg not available: {exc}")
    sys.exit(1)

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

try:
    import requests
    from backend.services.s3_service import (
        build_hash_s3_key,
        generate_image_thumbnail,
        get_s3_key_from_url,
        is_s3_url,
        upload_bytes_to_s3,
    )
    from backend.utils import compute_sha256, unpack_upload_result
    from backend.config import AWS_BUCKET_MODELS
except Exception as exc:
    print(f"[backfill_image_thumbnails] ERROR: cannot import backend helpers: {exc}")
    sys.exit(1)

APP_SCHEMA = os.getenv("APP_SCHEMA", "timrx_app")

# Images where thumbnail_url equals image_url (no real thumbnail)
IMAGES_QUERY = f"""
SELECT id, identity_id, provider, image_url, thumbnail_url, title
FROM {APP_SCHEMA}.images
WHERE image_url IS NOT NULL
  AND image_url != ''
  AND (thumbnail_url IS NULL OR thumbnail_url = '' OR thumbnail_url = image_url)
ORDER BY created_at DESC
""".strip()

IMAGES_QUERY_PROVIDER = f"""
SELECT id, identity_id, provider, image_url, thumbnail_url, title
FROM {APP_SCHEMA}.images
WHERE image_url IS NOT NULL
  AND image_url != ''
  AND (thumbnail_url IS NULL OR thumbnail_url = '' OR thumbnail_url = image_url)
  AND provider = %s
ORDER BY created_at DESC
""".strip()


def backfill_image_thumbnails(
    cur,
    conn,
    rows: list[dict],
    apply: bool,
    rate_limit: float = 0.1,
) -> tuple[int, int, int]:
    """
    Process images and generate thumbnails.

    Returns:
        (success_count, skip_count, error_count)
    """
    success_count = 0
    skip_count = 0
    error_count = 0

    for idx, row in enumerate(rows, start=1):
        image_id = str(row["id"])
        image_url = row["image_url"]
        provider = row.get("provider") or "unknown"
        title = (row.get("title") or "")[:40]

        print(f"\n[{idx}/{len(rows)}] image={image_id[:8]}... provider={provider} title={title}")

        if not image_url:
            print(f"  SKIP: no image_url")
            skip_count += 1
            continue

        if not apply:
            print(f"  DRY-RUN: would generate thumbnail from {image_url[:60]}...")
            skip_count += 1
            continue

        try:
            # Fetch image bytes
            r = requests.get(image_url, timeout=30)
            r.raise_for_status()
            image_bytes = r.content
            print(f"  Fetched {len(image_bytes)} bytes")

            # Generate thumbnail
            thumb_bytes = generate_image_thumbnail(
                image_bytes,
                max_size=400,
                quality=80,
                output_format="JPEG",
            )
            if not thumb_bytes:
                print(f"  SKIP: thumbnail generation returned None")
                skip_count += 1
                continue

            # Upload thumbnail to S3
            thumb_hash = compute_sha256(thumb_bytes)
            thumb_s3_key = build_hash_s3_key("thumbnails", provider, thumb_hash, "image/jpeg")
            thumb_result = upload_bytes_to_s3(
                thumb_bytes,
                content_type="image/jpeg",
                prefix="thumbnails",
                key=thumb_s3_key,
                return_hash=False,
            )
            thumb_url = unpack_upload_result(thumb_result)[0]

            if not thumb_url or not is_s3_url(thumb_url):
                print(f"  ERROR: upload returned non-S3 URL: {thumb_url}")
                error_count += 1
                continue

            thumb_s3_key_final = get_s3_key_from_url(thumb_url)
            print(f"  Thumbnail uploaded: {thumb_s3_key_final} ({len(thumb_bytes)} bytes)")

            # Update images table
            cur.execute(
                f"""
                UPDATE {APP_SCHEMA}.images
                SET thumbnail_url = %s,
                    thumbnail_s3_key = %s,
                    updated_at = NOW()
                WHERE id = %s
                """,
                (thumb_url, thumb_s3_key_final, image_id),
            )

            # Update history_items table
            cur.execute(
                f"""
                UPDATE {APP_SCHEMA}.history_items
                SET thumbnail_url = %s,
                    updated_at = NOW()
                WHERE image_id = %s
                """,
                (thumb_url, image_id),
            )

            conn.commit()
            success_count += 1
            print(f"  OK: DB updated")

            # Rate-limit to avoid S3 throttling
            time.sleep(rate_limit)

        except Exception as e:
            print(f"  ERROR: {e}")
            error_count += 1
            try:
                conn.rollback()
            except Exception:
                pass

    return success_count, skip_count, error_count


def main():
    parser = argparse.ArgumentParser(description="Backfill image thumbnails")
    parser.add_argument("--apply", action="store_true", help="Actually apply changes (default: dry-run)")
    parser.add_argument("--limit", type=int, default=0, help="Max images to process (0 = all)")
    parser.add_argument("--provider", type=str, default=None, help="Filter by provider (openai, google, nano_banana)")
    args = parser.parse_args()

    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        print("ERROR: DATABASE_URL not set")
        sys.exit(1)

    # Render compat
    if database_url.startswith("postgres://"):
        database_url = database_url.replace("postgres://", "postgresql://", 1)

    print(f"Backfill Image Thumbnails")
    print(f"  Mode: {'APPLY' if args.apply else 'DRY-RUN'}")
    print(f"  Limit: {args.limit or 'all'}")
    print(f"  Provider: {args.provider or 'all'}")
    print(f"  S3 Bucket: {AWS_BUCKET_MODELS or '(not configured)'}")
    print()

    if not AWS_BUCKET_MODELS:
        print("ERROR: AWS_BUCKET_MODELS not configured — cannot upload thumbnails")
        sys.exit(1)

    with psycopg.connect(database_url, row_factory=dict_row) as conn:
        with conn.cursor() as cur:
            if args.provider:
                cur.execute(IMAGES_QUERY_PROVIDER, (args.provider,))
            else:
                cur.execute(IMAGES_QUERY)

            rows = cur.fetchall()
            if args.limit:
                rows = rows[:args.limit]

            print(f"Found {len(rows)} images needing thumbnails")

            if not rows:
                print("Nothing to do.")
                return

            success, skipped, errors = backfill_image_thumbnails(
                cur, conn, rows, apply=args.apply
            )

    print(f"\n{'=' * 40}")
    print(f"Results: {success} succeeded, {skipped} skipped, {errors} errors")
    if not args.apply:
        print("(dry-run — no changes made. Use --apply to run for real)")


if __name__ == "__main__":
    main()
