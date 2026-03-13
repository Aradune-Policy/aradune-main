#!/usr/bin/env python3
"""
build_lake_mips_performance.py -- Ingest CMS MIPS Clinician Performance PY2023.

Reads from: data/raw/mips_2023_clinician_scores.csv (541K clinicians, 13 columns)
Writes to:  data/lake/fact/mips_performance/data.parquet

MIPS (Merit-based Incentive Payment System) scores individual clinicians across
4 categories: Quality, Promoting Interoperability, Improvement Activities, and Cost.
The final_mips_score determines payment adjustments.

Usage:
  python3 scripts/build_lake_mips_performance.py
  python3 scripts/build_lake_mips_performance.py --dry-run
"""

import argparse
import json
import re
import uuid
from datetime import date, datetime
from pathlib import Path

import duckdb
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent.parent
RAW_DIR = PROJECT_ROOT / "data" / "raw"
LAKE_DIR = PROJECT_ROOT / "data" / "lake"
FACT_DIR = LAKE_DIR / "fact"
META_DIR = LAKE_DIR / "metadata"

SNAPSHOT_DATE = date.today().isoformat()
RUN_ID = str(uuid.uuid4())


def to_snake_case(name) -> str:
    name = str(name).strip().lstrip("\ufeff")
    s = re.sub(r"[\s\-/]+", "_", name)
    s = re.sub(r"([a-z])([A-Z])", r"\1_\2", s)
    s = s.lower()
    s = re.sub(r"_+", "_", s)
    return s.strip("_")


def main():
    parser = argparse.ArgumentParser(description="Ingest MIPS PY2023 clinician scores")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    print("=" * 60)
    print("MIPS Clinician Performance PY2023 Ingestion")
    print(f"  Snapshot: {SNAPSHOT_DATE}")
    print(f"  Run ID:   {RUN_ID}")
    print()

    raw_path = RAW_DIR / "mips_2023_clinician_scores.csv"
    if not raw_path.exists():
        print("  ERROR: mips_2023_clinician_scores.csv not found")
        return

    print(f"  Source: {raw_path.name} ({raw_path.stat().st_size / (1024*1024):.1f} MB)")

    con = duckdb.connect()

    # Read with pandas (DuckDB has trouble with quoted headers in this file)
    print("  Reading CSV with pandas...")
    df = pd.read_csv(raw_path, encoding="utf-8-sig", low_memory=False)
    print(f"  Raw rows: {len(df):,}, columns: {len(df.columns)}")
    print(f"  Columns: {list(df.columns)}")

    # Rename columns to snake_case
    col_map = {col: to_snake_case(col) for col in df.columns}
    df.rename(columns=col_map, inplace=True)

    # Add metadata
    df["performance_year"] = 2023
    df["source_system"] = "CMS QPP MIPS"
    df["snapshot_date"] = SNAPSHOT_DATE

    # Register in DuckDB
    con.execute("CREATE TABLE fact_mips_performance AS SELECT * FROM df")

    count = con.execute("SELECT COUNT(*) FROM fact_mips_performance").fetchone()[0]
    cols = con.execute("PRAGMA table_info('fact_mips_performance')").fetchall()
    print(f"  Cleaned: {count:,} rows, {len(cols)} columns")

    # Summary stats
    stats = con.execute("""
        SELECT
            COUNT(DISTINCT npi) AS unique_npis,
            COUNT(*) FILTER (WHERE source = 'individual') AS individuals,
            COUNT(*) FILTER (WHERE source != 'individual' OR source IS NULL) AS groups,
            ROUND(AVG(final_mips_score), 2) AS avg_score,
            ROUND(MEDIAN(final_mips_score), 2) AS median_score,
            COUNT(*) FILTER (WHERE final_mips_score >= 75) AS above_75
        FROM fact_mips_performance
    """).fetchone()
    print(f"  Unique NPIs: {stats[0]:,}")
    print(f"  Individuals: {stats[1]:,}, Groups/Other: {stats[2]:,}")
    print(f"  Avg MIPS score: {stats[3]}, Median: {stats[4]}")
    print(f"  Scoring >= 75: {stats[5]:,}")

    # Score distribution
    print("\n  Score distribution:")
    dist = con.execute("""
        SELECT
            CASE
                WHEN final_mips_score IS NULL THEN 'NULL'
                WHEN final_mips_score = 0 THEN '0'
                WHEN final_mips_score < 25 THEN '1-24'
                WHEN final_mips_score < 50 THEN '25-49'
                WHEN final_mips_score < 75 THEN '50-74'
                WHEN final_mips_score < 90 THEN '75-89'
                ELSE '90-100'
            END AS bucket,
            COUNT(*) AS n
        FROM fact_mips_performance
        GROUP BY 1
        ORDER BY 1
    """).fetchall()
    for row in dist:
        print(f"    {row[0]:>8s}: {row[1]:>10,}")

    # Sample
    print("\n  Sample (5 rows):")
    sample = con.execute("""
        SELECT npi, provider_last_name, provider_first_name, source, final_mips_score
        FROM fact_mips_performance
        WHERE final_mips_score IS NOT NULL
        ORDER BY final_mips_score DESC
        LIMIT 5
    """).fetchall()
    for row in sample:
        print(f"    NPI={row[0]} | {row[2]} {row[1]} | {row[3]} | score={row[4]}")

    # Write parquet
    out_path = FACT_DIR / "mips_performance" / "data.parquet"
    if not args.dry_run:
        out_path.parent.mkdir(parents=True, exist_ok=True)
        con.execute(f"COPY fact_mips_performance TO '{out_path}' (FORMAT PARQUET, COMPRESSION ZSTD)")
        size_mb = out_path.stat().st_size / (1024 * 1024)
        print(f"\n  -> {out_path.relative_to(PROJECT_ROOT)} ({count:,} rows, {size_mb:.2f} MB)")
    else:
        print(f"\n  [dry-run] ({count:,} rows)")

    # Manifest
    if not args.dry_run:
        META_DIR.mkdir(parents=True, exist_ok=True)
        manifest = {
            "run_id": RUN_ID,
            "snapshot_date": SNAPSHOT_DATE,
            "script": "build_lake_mips_performance.py",
            "source": "https://data.cms.gov/provider-data/dataset/mips-clinicians",
            "tables": {
                "fact_mips_performance": {
                    "rows": count,
                    "columns": len(cols),
                    "path": str(out_path.relative_to(PROJECT_ROOT)),
                }
            },
            "completed_at": datetime.now().isoformat() + "Z",
        }
        manifest_path = META_DIR / f"manifest_mips_performance_{SNAPSHOT_DATE}.json"
        manifest_path.write_text(json.dumps(manifest, indent=2))
        print(f"  Manifest: {manifest_path.name}")

    con.close()

    print("\n" + "=" * 60)
    print("MIPS PERFORMANCE INGESTION COMPLETE")
    print(f"  fact_mips_performance: {count:,} rows, {len(cols)} columns")
    print("=" * 60)


if __name__ == "__main__":
    main()
