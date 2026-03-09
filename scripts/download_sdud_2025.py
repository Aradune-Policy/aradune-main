#!/usr/bin/env python3
"""Download SDUD 2025 data from data.medicaid.gov API."""
import subprocess, json, time, sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
OUT_FILE = PROJECT_ROOT / "data" / "raw" / "sdud_2025.json"

DATASET_ID = "158a1baa-5506-400a-8ec3-97756f0b0536"
PAGE_SIZE = 5000

all_rows = []
offset = 0

while True:
    url = f"https://data.medicaid.gov/api/1/datastore/query/{DATASET_ID}/0?limit={PAGE_SIZE}&offset={offset}"

    for attempt in range(3):
        try:
            result = subprocess.run(
                ["curl", "-s", "-H", "Accept: application/json",
                 "-H", "User-Agent: Aradune/1.0", url],
                capture_output=True, text=True, timeout=180,
            )
            data = json.loads(result.stdout)
            rows = data.get("results", [])
            break
        except Exception as e:
            print(f"  Retry {attempt+1}/3 at offset {offset}: {e}", flush=True)
            time.sleep(3 * (attempt + 1))
            rows = []
    else:
        print(f"  FAILED at offset {offset}", flush=True)
        break

    if not rows:
        break

    all_rows.extend(rows)
    offset += PAGE_SIZE

    if len(all_rows) % 50000 < PAGE_SIZE:
        print(f"  {len(all_rows):,} rows...", flush=True)

    if len(rows) < PAGE_SIZE:
        break

print(f"Total: {len(all_rows):,} rows")
with open(OUT_FILE, "w") as f:
    json.dump(all_rows, f)
print(f"Saved to {OUT_FILE} ({OUT_FILE.stat().st_size / 1024 / 1024:.1f} MB)")
