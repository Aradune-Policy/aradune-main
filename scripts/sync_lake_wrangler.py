#!/usr/bin/env python3
"""Sync local data lake to Cloudflare R2 using wrangler.

Bypasses boto3 SSL issues on macOS by using npx wrangler r2 object put.
Uploads files in parallel for speed.

Usage:
  python3 scripts/sync_lake_wrangler.py              # upload all
  python3 scripts/sync_lake_wrangler.py --dry-run     # preview only
  python3 scripts/sync_lake_wrangler.py --only fact    # upload only fact/
  python3 scripts/sync_lake_wrangler.py --workers 8    # 8 parallel uploads
"""

import argparse
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
LAKE_DIR = PROJECT_ROOT / "data" / "lake"
BUCKET = "aradune-datalake"
PREFIX = "lake/"


def upload_file(local_path: Path, r2_key: str) -> tuple[str, bool, str]:
    """Upload a single file to R2. Returns (key, success, message)."""
    try:
        result = subprocess.run(
            ["npx", "wrangler", "r2", "object", "put",
             f"{BUCKET}/{r2_key}",
             "--file", str(local_path)],
            capture_output=True, text=True, timeout=120
        )
        if result.returncode == 0:
            return (r2_key, True, "ok")
        else:
            return (r2_key, False, result.stderr[:200])
    except subprocess.TimeoutExpired:
        return (r2_key, False, "timeout")
    except Exception as e:
        return (r2_key, False, str(e)[:200])


def main():
    parser = argparse.ArgumentParser(description="Sync lake to R2 via wrangler")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--only", help="Filter subdirectory")
    parser.add_argument("--workers", type=int, default=4)
    args = parser.parse_args()

    # Collect all files to upload
    files = []
    for p in sorted(LAKE_DIR.rglob("*.parquet")):
        rel = p.relative_to(LAKE_DIR)
        if args.only and not str(rel).startswith(args.only):
            continue
        files.append((p, PREFIX + str(rel)))

    # Also include metadata JSON
    meta_dir = LAKE_DIR / "metadata"
    if meta_dir.exists():
        for f in sorted(meta_dir.glob("*.json")):
            rel = f.relative_to(LAKE_DIR)
            if args.only and not str(rel).startswith(args.only):
                continue
            files.append((f, PREFIX + str(rel)))

    total_size = sum(p.stat().st_size for p, _ in files) / (1024 * 1024)
    print(f"Found {len(files)} files ({total_size:.0f} MB) to upload")

    if args.dry_run:
        for p, key in files:
            size = p.stat().st_size / (1024 * 1024)
            print(f"  [dry-run] {key} ({size:.1f} MB)")
        print(f"\nWould upload {len(files)} files ({total_size:.0f} MB)")
        return

    succeeded = 0
    failed = 0

    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        futures = {executor.submit(upload_file, p, key): key for p, key in files}
        for i, future in enumerate(as_completed(futures), 1):
            key, ok, msg = future.result()
            if ok:
                succeeded += 1
                if i % 20 == 0 or i == len(files):
                    print(f"  [{i}/{len(files)}] {succeeded} uploaded, {failed} failed")
            else:
                failed += 1
                print(f"  FAILED: {key}: {msg}")

    print(f"\nDone: {succeeded} uploaded, {failed} failed out of {len(files)} files")
    if failed > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
