#!/usr/bin/env python3
"""
Setup S3 lifecycle rules for automatic cleanup of temporary objects.

This script configures lifecycle expiration rules on the TimrX S3 bucket
to automatically delete temporary objects that are only needed briefly.

Temporary prefixes cleaned up:
  - video-input/   — base64 images uploaded for Seedance/fal image-to-video.
                      Only needed while the provider downloads them via presigned
                      URL (~seconds). Presigned URLs expire after 1 hour.
  - meshy_input/   — normalized images uploaded for Meshy image-to-3D preflight.
                      Only needed while Meshy fetches them. Same lifecycle.

Permanent prefixes NOT affected:
  - models/        — 3D model GLB files (permanent user assets)
  - images/        — generated images (permanent user assets)
  - videos/        — generated videos (permanent user assets)
  - thumbnails/    — image/video/model thumbnails (permanent)
  - source_images/ — original user uploads for image-to-3D (permanent archival)
  - textures/      — model texture maps (permanent)

Usage:
    # Dry-run — show what rules would be set:
    python scripts/setup_s3_lifecycle.py

    # Apply the lifecycle rules:
    python scripts/setup_s3_lifecycle.py --apply

Notes:
    - This only needs to be run ONCE per bucket. Lifecycle rules persist on
      the bucket until explicitly changed or removed.
    - The 1-day expiry is conservative. Presigned URLs expire after 1 hour,
      and providers download images within seconds. 24 hours provides a large
      safety margin.
    - Re-running is safe: existing rules for other prefixes are preserved.
      Rules with the same ID are updated in place.
"""
import argparse
import os
import sys

# Ensure the backend package is importable when running from scripts/
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

try:
    import boto3
    from botocore.exceptions import ClientError
except ImportError:
    print("ERROR: boto3 not installed")
    sys.exit(1)

try:
    from backend.config import config
except ImportError:
    print("ERROR: cannot import backend config (run from meshy/ directory)")
    sys.exit(1)

# Rules to add/update — each is a standard S3 lifecycle rule dict
TIMRX_LIFECYCLE_RULES = [
    {
        "ID": "delete-video-input-after-1-day",
        "Filter": {"Prefix": "video-input/"},
        "Status": "Enabled",
        "Expiration": {"Days": 1},
    },
    {
        "ID": "delete-meshy-input-after-1-day",
        "Filter": {"Prefix": "meshy_input/"},
        "Status": "Enabled",
        "Expiration": {"Days": 1},
    },
]


def get_existing_rules(s3_client, bucket: str) -> list:
    """Fetch current lifecycle rules from the bucket, or empty list if none."""
    try:
        resp = s3_client.get_bucket_lifecycle_configuration(Bucket=bucket)
        return resp.get("Rules", [])
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code", "")
        if code == "NoSuchLifecycleConfiguration":
            return []
        raise


def merge_rules(existing: list, new_rules: list) -> list:
    """
    Merge new rules into existing rules by ID.

    Rules with the same ID are replaced (updated). Rules with different IDs
    are preserved unchanged. This prevents put_bucket_lifecycle_configuration
    from accidentally deleting unrelated rules.
    """
    new_ids = {r["ID"] for r in new_rules}
    # Keep existing rules that aren't being replaced
    merged = [r for r in existing if r.get("ID") not in new_ids]
    # Add all new/updated rules
    merged.extend(new_rules)
    return merged


def main():
    parser = argparse.ArgumentParser(
        description="Setup S3 lifecycle rules for temporary object cleanup"
    )
    parser.add_argument(
        "--apply", action="store_true",
        help="Actually apply the lifecycle rules (default: dry-run)"
    )
    args = parser.parse_args()

    bucket = config.AWS_BUCKET_MODELS
    if not bucket:
        print("ERROR: AWS_BUCKET_MODELS not configured")
        sys.exit(1)

    print(f"S3 Lifecycle Setup")
    print(f"  Bucket:  {bucket}")
    print(f"  Region:  {config.AWS_REGION}")
    print(f"  Mode:    {'APPLY' if args.apply else 'DRY-RUN'}")
    print()

    s3 = boto3.client(
        "s3",
        region_name=config.AWS_REGION,
        aws_access_key_id=config.AWS_ACCESS_KEY_ID,
        aws_secret_access_key=config.AWS_SECRET_ACCESS_KEY,
    )

    # Fetch existing rules
    existing = get_existing_rules(s3, bucket)
    print(f"Existing lifecycle rules: {len(existing)}")
    for r in existing:
        prefix = r.get("Filter", {}).get("Prefix", "(no prefix)")
        expiry = r.get("Expiration", {}).get("Days", "?")
        status = r.get("Status", "?")
        print(f"  [{r.get('ID', '?')}] prefix={prefix} expiry={expiry}d status={status}")

    # Merge
    merged = merge_rules(existing, TIMRX_LIFECYCLE_RULES)
    print(f"\nMerged lifecycle rules: {len(merged)}")
    for r in merged:
        prefix = r.get("Filter", {}).get("Prefix", "(no prefix)")
        expiry = r.get("Expiration", {}).get("Days", "?")
        status = r.get("Status", "?")
        is_new = r.get("ID") in {nr["ID"] for nr in TIMRX_LIFECYCLE_RULES}
        label = " (NEW/UPDATED)" if is_new else ""
        print(f"  [{r.get('ID', '?')}] prefix={prefix} expiry={expiry}d status={status}{label}")

    if not args.apply:
        print("\n(dry-run — use --apply to set these rules)")
        return

    # Apply
    print(f"\nApplying {len(merged)} lifecycle rules to {bucket}...")
    s3.put_bucket_lifecycle_configuration(
        Bucket=bucket,
        LifecycleConfiguration={"Rules": merged},
    )
    print("Done. Lifecycle rules applied successfully.")
    print()
    print("Verification: run this to check:")
    print(f"  aws s3api get-bucket-lifecycle-configuration --bucket {bucket}")


if __name__ == "__main__":
    main()
