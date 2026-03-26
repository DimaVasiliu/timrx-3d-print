#!/usr/bin/env python3
"""
S3 Orphan Detection Script
----------------------------
Scans S3 keys under configured prefixes and checks whether each key is
referenced by at least one DB row.  Keys that exist in S3 but have no
matching DB reference are reported as orphans.

This catches objects that slipped through compensating cleanup (edge cases,
historical data, code bugs, interrupted deploys).

Usage:
    # Dry-run: list orphans without deleting anything (default):
    python scripts/detect_s3_orphans.py

    # Scan only images:
    python scripts/detect_s3_orphans.py --prefix images

    # Scan only thumbnails:
    python scripts/detect_s3_orphans.py --prefix thumbnails

    # Limit S3 listing to N keys (faster for testing):
    python scripts/detect_s3_orphans.py --max-keys 500

    # Delete confirmed orphans (DANGEROUS — review dry-run output first):
    python scripts/detect_s3_orphans.py --delete

    # Write orphan keys to a file for review:
    python scripts/detect_s3_orphans.py --output orphans.txt
"""
import argparse
import os
import sys
import time

try:
    import psycopg
    from psycopg.rows import dict_row
except Exception as exc:
    print(f"[detect_s3_orphans] ERROR: psycopg not available: {exc}")
    sys.exit(1)

try:
    import boto3
except Exception as exc:
    print(f"[detect_s3_orphans] ERROR: boto3 not available: {exc}")
    sys.exit(1)

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

try:
    from backend.config import config
except Exception as exc:
    print(f"[detect_s3_orphans] ERROR: cannot import backend config: {exc}")
    sys.exit(1)

APP_SCHEMA = os.getenv("APP_SCHEMA", "timrx_app")

# Prefixes to scan and which DB columns reference keys under that prefix
PREFIX_DB_MAP = {
    "images": [
        (f"{APP_SCHEMA}.images", "image_s3_key"),
        (f"{APP_SCHEMA}.history_items", "image_url"),  # contains full S3 URL
    ],
    "thumbnails": [
        (f"{APP_SCHEMA}.images", "thumbnail_s3_key"),
        (f"{APP_SCHEMA}.models", "thumbnail_s3_key"),
        (f"{APP_SCHEMA}.videos", "thumbnail_s3_key"),
        (f"{APP_SCHEMA}.history_items", "thumbnail_url"),  # contains full S3 URL
    ],
    "models": [
        (f"{APP_SCHEMA}.models", "glb_s3_key"),
        (f"{APP_SCHEMA}.history_items", "glb_url"),  # contains full S3 URL
    ],
    "videos": [
        (f"{APP_SCHEMA}.videos", "video_s3_key"),
        (f"{APP_SCHEMA}.history_items", "video_url"),  # contains full S3 URL
    ],
    "textures": [
        # Textures are stored in model meta JSONB and history payload JSONB.
        # Direct column scan is not practical — skip for now.
    ],
}


def list_s3_keys(s3_client, bucket: str, prefix: str, max_keys: int = 0) -> list[str]:
    """List all S3 keys under a prefix using pagination."""
    keys = []
    continuation_token = None
    page_size = 1000

    while True:
        kwargs = {"Bucket": bucket, "Prefix": prefix + "/", "MaxKeys": page_size}
        if continuation_token:
            kwargs["ContinuationToken"] = continuation_token

        resp = s3_client.list_objects_v2(**kwargs)
        for obj in resp.get("Contents", []):
            keys.append(obj["Key"])

        if max_keys and len(keys) >= max_keys:
            keys = keys[:max_keys]
            break

        if not resp.get("IsTruncated"):
            break
        continuation_token = resp.get("NextContinuationToken")

    return keys


def check_key_referenced(cur, s3_key: str, bucket: str, table_columns: list[tuple[str, str]]) -> bool:
    """
    Check if an S3 key is referenced by any DB row.

    Checks both:
    - direct s3_key column match (e.g., image_s3_key = 'images/openai/abc.png')
    - full URL match (e.g., image_url LIKE '%images/openai/abc.png')
    """
    full_url = f"https://{bucket}.s3.{config.AWS_REGION}.amazonaws.com/{s3_key}"

    for table, column in table_columns:
        try:
            # Check by key suffix (for s3_key columns)
            if column.endswith("_s3_key"):
                cur.execute(
                    f"SELECT 1 FROM {table} WHERE {column} = %s LIMIT 1",
                    (s3_key,),
                )
            else:
                # Check by full URL (for url columns like image_url, thumbnail_url)
                cur.execute(
                    f"SELECT 1 FROM {table} WHERE {column} = %s LIMIT 1",
                    (full_url,),
                )

            if cur.fetchone():
                return True
        except Exception as e:
            # Table or column might not exist — skip silently
            print(f"  [WARN] Query failed on {table}.{column}: {e}")
            try:
                cur.connection.rollback()
            except Exception:
                pass

    return False


def main():
    parser = argparse.ArgumentParser(description="Detect orphaned S3 objects with no DB reference")
    parser.add_argument("--prefix", type=str, default=None,
                        help="Scan only this prefix (images, thumbnails, models, videos)")
    parser.add_argument("--max-keys", type=int, default=0,
                        help="Max S3 keys to scan per prefix (0 = all)")
    parser.add_argument("--delete", action="store_true",
                        help="Delete confirmed orphans (DANGEROUS — review dry-run first)")
    parser.add_argument("--output", type=str, default=None,
                        help="Write orphan keys to this file")
    args = parser.parse_args()

    bucket = config.AWS_BUCKET_MODELS
    if not bucket:
        print("ERROR: AWS_BUCKET_MODELS not configured")
        sys.exit(1)

    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        print("ERROR: DATABASE_URL not set")
        sys.exit(1)
    if database_url.startswith("postgres://"):
        database_url = database_url.replace("postgres://", "postgresql://", 1)

    s3 = boto3.client(
        "s3",
        region_name=config.AWS_REGION,
        aws_access_key_id=config.AWS_ACCESS_KEY_ID,
        aws_secret_access_key=config.AWS_SECRET_ACCESS_KEY,
    )

    prefixes_to_scan = [args.prefix] if args.prefix else list(PREFIX_DB_MAP.keys())

    print(f"S3 Orphan Detection")
    print(f"  Bucket: {bucket}")
    print(f"  Region: {config.AWS_REGION}")
    print(f"  Prefixes: {', '.join(prefixes_to_scan)}")
    print(f"  Max keys/prefix: {args.max_keys or 'all'}")
    print(f"  Mode: {'DELETE orphans' if args.delete else 'DRY-RUN (report only)'}")
    print()

    all_orphans: list[str] = []
    total_scanned = 0
    total_referenced = 0

    with psycopg.connect(database_url, row_factory=dict_row) as conn:
        with conn.cursor() as cur:
            for prefix in prefixes_to_scan:
                table_columns = PREFIX_DB_MAP.get(prefix, [])
                if not table_columns:
                    print(f"[{prefix}] Skipping — no DB columns to check against")
                    continue

                print(f"[{prefix}] Listing S3 keys...")
                keys = list_s3_keys(s3, bucket, prefix, max_keys=args.max_keys)
                print(f"[{prefix}] Found {len(keys)} keys")

                orphans_in_prefix = 0
                for i, key in enumerate(keys):
                    if (i + 1) % 100 == 0:
                        print(f"  Checked {i + 1}/{len(keys)}...")

                    referenced = check_key_referenced(cur, key, bucket, table_columns)
                    total_scanned += 1

                    if referenced:
                        total_referenced += 1
                    else:
                        all_orphans.append(key)
                        orphans_in_prefix += 1

                print(f"[{prefix}] {orphans_in_prefix} orphans / {len(keys)} total")
                print()

    print(f"{'=' * 50}")
    print(f"Total scanned:    {total_scanned}")
    print(f"Total referenced: {total_referenced}")
    print(f"Total orphans:    {len(all_orphans)}")
    print()

    if all_orphans:
        print("Orphan keys:")
        for key in all_orphans[:50]:
            print(f"  {key}")
        if len(all_orphans) > 50:
            print(f"  ... and {len(all_orphans) - 50} more")
        print()

    if args.output and all_orphans:
        with open(args.output, "w") as f:
            for key in all_orphans:
                f.write(key + "\n")
        print(f"Wrote {len(all_orphans)} orphan keys to {args.output}")

    if args.delete and all_orphans:
        print(f"\nDeleting {len(all_orphans)} orphan objects...")
        # Delete in batches of 1000 (S3 limit)
        deleted = 0
        for i in range(0, len(all_orphans), 1000):
            batch = all_orphans[i:i + 1000]
            try:
                s3.delete_objects(
                    Bucket=bucket,
                    Delete={"Objects": [{"Key": k} for k in batch], "Quiet": True},
                )
                deleted += len(batch)
                print(f"  Deleted batch {i // 1000 + 1}: {len(batch)} objects")
            except Exception as e:
                print(f"  ERROR deleting batch: {e}")
            time.sleep(0.5)  # Rate limit
        print(f"Deleted {deleted} orphan objects")
    elif args.delete and not all_orphans:
        print("No orphans to delete.")
    elif all_orphans:
        print("(dry-run — use --delete to remove orphans)")


if __name__ == "__main__":
    main()
