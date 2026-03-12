#!/usr/bin/env python3
"""
build_lake_hospice.py — Ingest hospice provider quality measures.

Reads from: data/raw/hospice_providers.csv (465K rows)
Writes to:  data/lake/fact/hospice_quality/

Table: fact_hospice_quality — Hospice provider quality measures from CMS Care Compare.
"""

import argparse
import json
import sys
import uuid
from datetime import date, datetime
from pathlib import Path

import duckdb

PROJECT_ROOT = Path(__file__).resolve().parent.parent
RAW_DIR = PROJECT_ROOT / "data" / "raw"
LAKE_DIR = PROJECT_ROOT / "data" / "lake"
FACT_DIR = LAKE_DIR / "fact"
META_DIR = LAKE_DIR / "metadata"

SOURCE_CSV = RAW_DIR / "hospice_providers.csv"
SNAPSHOT_DATE = date.today().isoformat()
RUN_ID = str(uuid.uuid4())


def main():
    parser = argparse.ArgumentParser(description="Ingest hospice provider quality data")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    if not SOURCE_CSV.exists():
        print(f"ERROR: {SOURCE_CSV} not found", file=sys.stderr)
        sys.exit(1)

    print(f"Snapshot: {SNAPSHOT_DATE}")
    print(f"Source:   {SOURCE_CSV} ({SOURCE_CSV.stat().st_size / 1e6:.0f}MB)")

    con = duckdb.connect()

    con.execute(f"""
        CREATE OR REPLACE TABLE _fact_hospice_quality AS
        SELECT
            "CMS Certification Number (CCN)" AS provider_ccn,
            "Facility Name" AS facility_name,
            "City/Town" AS city,
            "State" AS state_code,
            "ZIP Code" AS zip_code,
            "County/Parish" AS county,
            "CMS Region" AS cms_region,
            "Measure Code" AS measure_code,
            "Measure Name" AS measure_name,
            TRY_CAST("Score" AS DOUBLE) AS score,
            "Footnote" AS footnote,
            "Measure Date Range" AS measure_date_range,
            'cms.gov/care-compare/hospice' AS source,
            DATE '{SNAPSHOT_DATE}' AS snapshot_date
        FROM read_csv_auto('{SOURCE_CSV}')
        WHERE "State" IS NOT NULL
          AND LENGTH("State") = 2
    """)

    count = con.execute("SELECT COUNT(*) FROM _fact_hospice_quality").fetchone()[0]
    states = con.execute("SELECT COUNT(DISTINCT state_code) FROM _fact_hospice_quality").fetchone()[0]
    facilities = con.execute("SELECT COUNT(DISTINCT provider_ccn) FROM _fact_hospice_quality").fetchone()[0]
    measures = con.execute("SELECT COUNT(DISTINCT measure_code) FROM _fact_hospice_quality").fetchone()[0]

    out_path = FACT_DIR / "hospice_quality" / f"snapshot={SNAPSHOT_DATE}" / "data.parquet"
    if not args.dry_run:
        out_path.parent.mkdir(parents=True, exist_ok=True)
        con.execute(f"COPY _fact_hospice_quality TO '{out_path}' (FORMAT PARQUET, COMPRESSION ZSTD)")
        size_kb = out_path.stat().st_size / 1024
        print(f"  -> {out_path.relative_to(LAKE_DIR)} ({count:,} rows, {size_kb:.1f} KB)")
    else:
        print(f"  [dry-run] ({count:,} rows)")

    print(f"  {count:,} measure rows, {facilities:,} facilities, {states} states, {measures} distinct measures")

    print("\n  Top measures:")
    top = con.execute("""
        SELECT measure_code, measure_name, COUNT(*) AS cnt
        FROM _fact_hospice_quality
        GROUP BY 1, 2 ORDER BY 3 DESC LIMIT 8
    """).fetchall()
    for code, name, cnt in top:
        print(f"    {cnt:>8,}  {code or 'N/A'}: {(name or '')[:60]}")

    con.execute("DROP TABLE IF EXISTS _fact_hospice_quality")
    con.close()

    if not args.dry_run:
        META_DIR.mkdir(parents=True, exist_ok=True)
        manifest = {
            "snapshot_date": SNAPSHOT_DATE,
            "pipeline_run_id": RUN_ID,
            "created_at": datetime.now().isoformat() + "Z",
            "source_file": str(SOURCE_CSV),
            "tables": {"fact_hospice_quality": {"rows": count}},
        }
        with open(META_DIR / f"manifest_hospice_{SNAPSHOT_DATE}.json", "w") as f:
            json.dump(manifest, f, indent=2)

    print("\nDone.")


if __name__ == "__main__":
    main()
