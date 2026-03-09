#!/usr/bin/env python3
"""
build_lake_sdud_2025.py — Ingest State Drug Utilization Data 2025 into the lake.

Reads from: data/raw/sdud_2025.json (downloaded from data.medicaid.gov)
Writes to:  data/lake/fact/sdud_2025/

Usage:
  python3 scripts/build_lake_sdud_2025.py
  python3 scripts/build_lake_sdud_2025.py --dry-run
"""

import argparse
import json
import uuid
from datetime import date, datetime
from pathlib import Path

import duckdb

PROJECT_ROOT = Path(__file__).resolve().parent.parent
RAW_DIR = PROJECT_ROOT / "data" / "raw"
LAKE_DIR = PROJECT_ROOT / "data" / "lake"
FACT_DIR = LAKE_DIR / "fact"
META_DIR = LAKE_DIR / "metadata"

SNAPSHOT_DATE = date.today().isoformat()
RUN_ID = str(uuid.uuid4())


def write_parquet(con, table_name: str, out_path: Path, dry_run: bool) -> int:
    count = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
    if not dry_run and count > 0:
        out_path.parent.mkdir(parents=True, exist_ok=True)
        con.execute(f"COPY {table_name} TO '{out_path}' (FORMAT PARQUET, COMPRESSION ZSTD)")
        size_mb = out_path.stat().st_size / (1024 * 1024)
        print(f"  -> {out_path.relative_to(LAKE_DIR)} ({count:,} rows, {size_mb:.1f} MB)")
    elif dry_run:
        print(f"  [dry-run] {out_path.relative_to(LAKE_DIR)} ({count:,} rows)")
    return count


def _snapshot_path(fact_name: str) -> Path:
    return FACT_DIR / fact_name / f"snapshot={SNAPSHOT_DATE}" / "data.parquet"


def build_fact_sdud_2025(con, dry_run: bool) -> int:
    print("Building fact_sdud_2025...")
    json_path = RAW_DIR / "sdud_2025.json"
    if not json_path.exists():
        print(f"  SKIPPED — {json_path.name} not found")
        return 0

    # Check file isn't empty
    size = json_path.stat().st_size
    if size < 100:
        print(f"  SKIPPED — {json_path.name} too small ({size} bytes)")
        return 0

    con.execute(f"""
        CREATE OR REPLACE TABLE _fact_sdud25 AS
        SELECT
            state AS state_code,
            utilization_type,
            ndc,
            labeler_code,
            product_code,
            TRIM(product_name) AS product_name,
            CAST(year AS INTEGER) AS year,
            CAST(quarter AS INTEGER) AS quarter,
            CAST(suppression_used AS BOOLEAN) AS suppression_used,
            TRY_CAST(REPLACE(units_reimbursed, ',', '') AS DOUBLE) AS units_reimbursed,
            TRY_CAST(REPLACE(number_of_prescriptions, ',', '') AS INTEGER) AS number_of_prescriptions,
            TRY_CAST(REPLACE(total_amount_reimbursed, ',', '') AS DOUBLE) AS total_amount_reimbursed,
            TRY_CAST(REPLACE(medicaid_amount_reimbursed, ',', '') AS DOUBLE) AS medicaid_amount_reimbursed,
            TRY_CAST(REPLACE(non_medicaid_amount_reimbursed, ',', '') AS DOUBLE) AS non_medicaid_amount_reimbursed,
            'data.medicaid.gov' AS source,
            DATE '{SNAPSHOT_DATE}' AS snapshot_date
        FROM read_json_auto('{json_path}', maximum_object_size=536870912)
        WHERE state IS NOT NULL AND LENGTH(state) = 2
    """)

    count = write_parquet(con, "_fact_sdud25", _snapshot_path("sdud_2025"), dry_run)
    states = con.execute("SELECT COUNT(DISTINCT state_code) FROM _fact_sdud25").fetchone()[0]
    quarters = con.execute("SELECT DISTINCT quarter FROM _fact_sdud25 ORDER BY quarter").fetchall()
    total_rx = con.execute("SELECT SUM(number_of_prescriptions) FROM _fact_sdud25").fetchone()[0]
    total_cost = con.execute("SELECT SUM(total_amount_reimbursed) FROM _fact_sdud25").fetchone()[0]
    print(f"  {count:,} rows, {states} states, quarters: {[q[0] for q in quarters]}")
    print(f"  Total Rx: {total_rx:,.0f}, Total reimbursed: ${total_cost:,.0f}")
    con.execute("DROP TABLE IF EXISTS _fact_sdud25")
    return count


def main():
    parser = argparse.ArgumentParser(description="Ingest SDUD 2025 into Aradune lake")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    print(f"Snapshot: {SNAPSHOT_DATE}")
    print(f"Run ID:   {RUN_ID}")
    print()

    con = duckdb.connect()
    count = build_fact_sdud_2025(con, args.dry_run)
    con.close()

    print()
    print("=" * 60)
    print("SDUD 2025 LAKE INGESTION COMPLETE")
    print("=" * 60)
    print(f"  fact_sdud_2025                {count:>12,} rows")

    if not args.dry_run and count > 0:
        META_DIR.mkdir(parents=True, exist_ok=True)
        manifest = {
            "snapshot_date": SNAPSHOT_DATE,
            "pipeline_run_id": RUN_ID,
            "created_at": datetime.now().isoformat() + "Z",
            "tables": {"fact_sdud_2025": {"rows": count}},
            "total_rows": count,
        }
        manifest_file = META_DIR / f"manifest_sdud_2025_{SNAPSHOT_DATE}.json"
        with open(manifest_file, "w") as f:
            json.dump(manifest, f, indent=2)
        print(f"\n  Manifest: {manifest_file}")

    print("\nDone.")


if __name__ == "__main__":
    main()
