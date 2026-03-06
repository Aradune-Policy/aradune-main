"""Sync the local data lake to/from S3.

Usage:
  python scripts/sync_lake.py upload          # local -> S3
  python scripts/sync_lake.py download        # S3 -> local
  python scripts/sync_lake.py upload --dry-run
  python scripts/sync_lake.py download --only dimension
"""

import argparse
import os
import sys
from pathlib import Path

try:
    import boto3
    from botocore.exceptions import ClientError
except ImportError:
    print("boto3 required: pip install boto3")
    sys.exit(1)

PROJECT_ROOT = Path(__file__).resolve().parent.parent
LAKE_DIR = PROJECT_ROOT / "data" / "lake"

S3_BUCKET = os.environ.get("ARADUNE_S3_BUCKET", "aradune-datalake")
S3_PREFIX = os.environ.get("ARADUNE_S3_PREFIX", "lake/")


def get_s3_client():
    return boto3.client("s3")


def upload(dry_run: bool = False, only: str = None):
    """Upload local lake Parquet files to S3."""
    s3 = get_s3_client()
    uploaded = 0

    for parquet in LAKE_DIR.rglob("*.parquet"):
        rel = parquet.relative_to(LAKE_DIR)
        if only and not str(rel).startswith(only):
            continue

        s3_key = S3_PREFIX + str(rel)
        size_mb = parquet.stat().st_size / (1024 * 1024)

        if dry_run:
            print(f"  [dry-run] {rel} ({size_mb:.1f} MB) -> s3://{S3_BUCKET}/{s3_key}")
        else:
            print(f"  Uploading {rel} ({size_mb:.1f} MB)...", end=" ", flush=True)
            s3.upload_file(str(parquet), S3_BUCKET, s3_key)
            print("done")
        uploaded += 1

    # Also upload metadata JSON files
    meta_dir = LAKE_DIR / "metadata"
    if meta_dir.exists():
        for f in meta_dir.glob("*.json"):
            rel = f.relative_to(LAKE_DIR)
            s3_key = S3_PREFIX + str(rel)
            if dry_run:
                print(f"  [dry-run] {rel} -> s3://{S3_BUCKET}/{s3_key}")
            else:
                print(f"  Uploading {rel}...", end=" ", flush=True)
                s3.upload_file(str(f), S3_BUCKET, s3_key)
                print("done")
            uploaded += 1

    print(f"\n{'Would upload' if dry_run else 'Uploaded'} {uploaded} files to s3://{S3_BUCKET}/{S3_PREFIX}")


def download(dry_run: bool = False, only: str = None):
    """Download lake Parquet files from S3 to local."""
    s3 = get_s3_client()
    downloaded = 0

    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=S3_PREFIX):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            rel = key[len(S3_PREFIX):]
            if not rel:
                continue
            if only and not rel.startswith(only):
                continue

            local_path = LAKE_DIR / rel
            size_mb = obj["Size"] / (1024 * 1024)

            if dry_run:
                print(f"  [dry-run] s3://{S3_BUCKET}/{key} ({size_mb:.1f} MB) -> {rel}")
            else:
                local_path.parent.mkdir(parents=True, exist_ok=True)
                print(f"  Downloading {rel} ({size_mb:.1f} MB)...", end=" ", flush=True)
                s3.download_file(S3_BUCKET, key, str(local_path))
                print("done")
            downloaded += 1

    print(f"\n{'Would download' if dry_run else 'Downloaded'} {downloaded} files from s3://{S3_BUCKET}/{S3_PREFIX}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Sync Aradune data lake to/from S3")
    parser.add_argument("action", choices=["upload", "download"])
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--only", help="Filter to specific subdirectory (e.g., 'dimension', 'fact/rate_comparison')")
    args = parser.parse_args()

    if args.action == "upload":
        upload(dry_run=args.dry_run, only=args.only)
    else:
        download(dry_run=args.dry_run, only=args.only)
