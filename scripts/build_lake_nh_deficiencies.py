#!/usr/bin/env python3
"""
build_lake_nh_deficiencies.py — Ingest nursing home deficiency citation data.

Reads from: data/raw/nh_deficiencies.json (CMS Care Compare, 419K+ records)
Writes to:  data/lake/fact/nh_deficiency/

Table: fact_nh_deficiency — Individual deficiency citations from CMS nursing home surveys.
       Includes tag numbers, scope/severity codes, survey types, correction status.

Usage:
  python3 scripts/build_lake_nh_deficiencies.py
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

DEFICIENCY_JSON = RAW_DIR / "nh_deficiencies.json"

SNAPSHOT_DATE = date.today().isoformat()
RUN_ID = str(uuid.uuid4())


def main():
    parser = argparse.ArgumentParser(description="Ingest NH deficiency data")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    if not DEFICIENCY_JSON.exists():
        print(f"ERROR: {DEFICIENCY_JSON} not found", file=sys.stderr)
        sys.exit(1)

    print(f"Snapshot: {SNAPSHOT_DATE}")
    print(f"Source:   {DEFICIENCY_JSON} ({DEFICIENCY_JSON.stat().st_size / 1e6:.0f}MB)")

    # Load JSON
    print("Loading JSON...")
    with open(DEFICIENCY_JSON) as f:
        records = json.load(f)
    print(f"  {len(records):,} raw records")

    con = duckdb.connect()

    # Create table from records
    con.execute("CREATE TABLE _raw AS SELECT * FROM read_json_auto(?)", [str(DEFICIENCY_JSON)])

    con.execute(f"""
        CREATE OR REPLACE TABLE _fact_nh_deficiency AS
        SELECT
            cms_certification_number_ccn AS provider_ccn,
            provider_name,
            state AS state_code,
            citytown AS city,
            zip_code,
            TRY_CAST(survey_date AS DATE) AS survey_date,
            survey_type,
            deficiency_prefix,
            deficiency_category,
            deficiency_tag_number AS tag_number,
            deficiency_description AS description,
            scope_severity_code,
            -- Decode scope/severity into readable labels
            CASE scope_severity_code
                WHEN 'A' THEN 'Isolated / No actual harm, potential for minimal'
                WHEN 'B' THEN 'Pattern / No actual harm, potential for minimal'
                WHEN 'C' THEN 'Widespread / No actual harm, potential for minimal'
                WHEN 'D' THEN 'Isolated / No actual harm, potential for more than minimal'
                WHEN 'E' THEN 'Pattern / No actual harm, potential for more than minimal'
                WHEN 'F' THEN 'Widespread / No actual harm, potential for more than minimal'
                WHEN 'G' THEN 'Isolated / Actual harm'
                WHEN 'H' THEN 'Pattern / Actual harm'
                WHEN 'I' THEN 'Widespread / Actual harm'
                WHEN 'J' THEN 'Isolated / Immediate jeopardy'
                WHEN 'K' THEN 'Pattern / Immediate jeopardy'
                WHEN 'L' THEN 'Widespread / Immediate jeopardy'
            END AS severity_label,
            CASE
                WHEN scope_severity_code IN ('A','B','C') THEN 1
                WHEN scope_severity_code IN ('D','E','F') THEN 2
                WHEN scope_severity_code IN ('G','H','I') THEN 3
                WHEN scope_severity_code IN ('J','K','L') THEN 4
            END AS severity_level,
            deficiency_corrected AS correction_status,
            TRY_CAST(correction_date AS DATE) AS correction_date,
            TRY_CAST(inspection_cycle AS INTEGER) AS inspection_cycle,
            standard_deficiency = 'Y' AS is_standard,
            complaint_deficiency = 'Y' AS is_complaint,
            infection_control_inspection_deficiency = 'Y' AS is_infection_control,
            citation_under_idr = 'Y' AS is_idr,
            citation_under_iidr = 'Y' AS is_iidr,
            'cms.gov/care-compare' AS source,
            DATE '{SNAPSHOT_DATE}' AS snapshot_date
        FROM _raw
        WHERE state IS NOT NULL
          AND LENGTH(state) = 2
    """)

    count = con.execute("SELECT COUNT(*) FROM _fact_nh_deficiency").fetchone()[0]
    states = con.execute("SELECT COUNT(DISTINCT state_code) FROM _fact_nh_deficiency").fetchone()[0]
    facilities = con.execute("SELECT COUNT(DISTINCT provider_ccn) FROM _fact_nh_deficiency").fetchone()[0]
    severe = con.execute("SELECT COUNT(*) FROM _fact_nh_deficiency WHERE severity_level >= 3").fetchone()[0]

    out_path = FACT_DIR / "nh_deficiency" / f"snapshot={SNAPSHOT_DATE}" / "data.parquet"
    if not args.dry_run:
        out_path.parent.mkdir(parents=True, exist_ok=True)
        con.execute(f"COPY _fact_nh_deficiency TO '{out_path}' (FORMAT PARQUET, COMPRESSION ZSTD)")
        size_kb = out_path.stat().st_size / 1024
        print(f"  -> {out_path.relative_to(LAKE_DIR)} ({count:,} rows, {size_kb:.1f} KB)")
    else:
        print(f"  [dry-run] ({count:,} rows)")

    print(f"  {count:,} citations, {facilities:,} facilities, {states} states")
    print(f"  {severe:,} citations at severity level 3+ (actual harm or immediate jeopardy)")

    # Category breakdown
    print("\n  Top deficiency categories:")
    cats = con.execute("""
        SELECT deficiency_category, COUNT(*) AS cnt
        FROM _fact_nh_deficiency
        GROUP BY 1 ORDER BY 2 DESC LIMIT 10
    """).fetchall()
    for cat, cnt in cats:
        print(f"    {cnt:>8,}  {cat}")

    con.execute("DROP TABLE IF EXISTS _fact_nh_deficiency")
    con.execute("DROP TABLE IF EXISTS _raw")
    con.close()

    if not args.dry_run:
        META_DIR.mkdir(parents=True, exist_ok=True)
        manifest = {
            "snapshot_date": SNAPSHOT_DATE,
            "pipeline_run_id": RUN_ID,
            "created_at": datetime.now().isoformat() + "Z",
            "source_file": str(DEFICIENCY_JSON),
            "tables": {"fact_nh_deficiency": {"rows": count}},
        }
        with open(META_DIR / f"manifest_nh_deficiency_{SNAPSHOT_DATE}.json", "w") as f:
            json.dump(manifest, f, indent=2)

    print("\nDone.")


if __name__ == "__main__":
    main()
