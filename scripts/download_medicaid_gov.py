#!/usr/bin/env python3
"""Download small datasets from data.medicaid.gov API."""
import subprocess
import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
RAW_DIR = PROJECT_ROOT / "data" / "raw"

DATASETS = {
    "marketplace_unwinding_transitions": "5636a78c-fe18-4229-aee1-e40fa910a8a0",
    "sbm_unwinding": "5670e72c-e44e-4282-ab67-4ebebaba3cbd",
    "exclusive_pediatric_drugs": "a54d7605-b780-4cf0-b53d-50313798f528",
    "clotting_factor_drugs": "f45f35c5-7aa4-4500-b196-ae7833717add",
}


def download_dataset(name: str, dataset_id: str) -> int:
    all_rows = []
    offset = 0
    page_size = 5000

    while True:
        url = (
            f"https://data.medicaid.gov/api/1/datastore/query/"
            f"{dataset_id}/0?limit={page_size}&offset={offset}"
        )
        result = subprocess.run(
            [
                "curl", "-s",
                "-H", "Accept: application/json",
                "-H", "User-Agent: Aradune/1.0",
                url,
            ],
            capture_output=True,
            text=True,
            timeout=120,
        )

        if result.returncode != 0:
            print(f"  curl failed at offset {offset}", flush=True)
            break

        try:
            data = json.loads(result.stdout)
        except json.JSONDecodeError:
            print(f"  JSON parse error at offset {offset}", flush=True)
            break

        rows = data.get("results", [])
        if not rows:
            break

        all_rows.extend(rows)
        offset += page_size

        if len(rows) < page_size:
            break

    out_path = RAW_DIR / f"{name}.json"
    with open(out_path, "w") as f:
        json.dump(all_rows, f)
    print(f"  {name}: {len(all_rows):,} rows -> {out_path.name}")
    return len(all_rows)


if __name__ == "__main__":
    total = 0
    for name, dataset_id in DATASETS.items():
        count = download_dataset(name, dataset_id)
        total += count
    print(f"\nTotal: {total:,} rows across {len(DATASETS)} datasets")
