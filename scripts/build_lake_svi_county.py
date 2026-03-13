#!/usr/bin/env python3
"""
build_lake_svi_county.py -- Ingest CDC/ATSDR Social Vulnerability Index 2022 (county level).

Reads from: data/raw/svi_2022/SVI_2022_US_county.csv (3,144 counties, 158 columns)
Writes to:  data/lake/fact/svi_county/data.parquet

The SVI ranks each county on 16 social factors grouped into 4 themes:
  Theme 1: Socioeconomic Status
  Theme 2: Household Characteristics & Disability
  Theme 3: Racial & Ethnic Minority Status
  Theme 4: Housing Type & Transportation
RPL_THEMES is the overall percentile ranking (0-1, higher = more vulnerable).

Usage:
  python3 scripts/build_lake_svi_county.py
  python3 scripts/build_lake_svi_county.py --dry-run
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
    """Convert column name to snake_case."""
    name = str(name)
    # Remove BOM
    name = name.strip().lstrip("\ufeff")
    # Replace spaces, hyphens, slashes with underscores
    s = re.sub(r"[\s\-/]+", "_", name)
    # Insert underscore before uppercase letters preceded by lowercase
    s = re.sub(r"([a-z])([A-Z])", r"\1_\2", s)
    # Lowercase everything
    s = s.lower()
    # Collapse multiple underscores
    s = re.sub(r"_+", "_", s)
    # Strip leading/trailing underscores
    return s.strip("_")


def main():
    parser = argparse.ArgumentParser(description="Ingest CDC SVI 2022 county data")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    print("=" * 60)
    print("CDC SVI 2022 County-Level Ingestion")
    print(f"  Snapshot: {SNAPSHOT_DATE}")
    print(f"  Run ID:   {RUN_ID}")
    print()

    # Use the cleaner column-name version
    raw_path = RAW_DIR / "svi_2022" / "SVI_2022_US_county.csv"
    if not raw_path.exists():
        raw_path = RAW_DIR / "svi_2022_county.csv"
    if not raw_path.exists():
        print("  ERROR: No SVI county CSV found")
        return

    print(f"  Source: {raw_path.name}")

    con = duckdb.connect()

    # Read with pandas to handle BOM (utf-8-sig) and quoted LOCATION field
    print("  Reading CSV with pandas (BOM handling)...")
    df = pd.read_csv(raw_path, encoding="utf-8-sig", low_memory=False)
    print(f"  Raw rows: {len(df):,}, columns: {len(df.columns)}")

    # Rename columns to snake_case
    col_map = {col: to_snake_case(col) for col in df.columns}
    df.rename(columns=col_map, inplace=True)

    # Add metadata columns
    df["source"] = "CDC/ATSDR SVI 2022"
    df["snapshot_date"] = SNAPSHOT_DATE

    # Register in DuckDB
    con.execute("CREATE TABLE fact_svi_county AS SELECT * FROM df")

    count = con.execute("SELECT COUNT(*) FROM fact_svi_county").fetchone()[0]
    cols = con.execute("PRAGMA table_info('fact_svi_county')").fetchall()
    print(f"  Cleaned rows: {count:,}, columns: {len(cols)}")

    # Summary stats
    states = con.execute("SELECT COUNT(DISTINCT st_abbr) FROM fact_svi_county WHERE st_abbr IS NOT NULL").fetchone()[0]
    print(f"  States/territories: {states}")

    vuln = con.execute("""
        SELECT
            ROUND(AVG(rpl_themes), 4) AS avg_rpl,
            ROUND(MIN(rpl_themes), 4) AS min_rpl,
            ROUND(MAX(rpl_themes), 4) AS max_rpl
        FROM fact_svi_county
        WHERE rpl_themes >= 0
    """).fetchone()
    print(f"  RPL_THEMES: avg={vuln[0]}, min={vuln[1]}, max={vuln[2]}")

    # Sample
    print("\n  Sample (5 rows):")
    sample = con.execute("""
        SELECT st_abbr, county, fips, e_totpop, rpl_themes
        FROM fact_svi_county
        ORDER BY rpl_themes DESC
        LIMIT 5
    """).fetchall()
    for row in sample:
        print(f"    {row[0]} | {row[1]:30s} | FIPS={row[2]} | pop={row[3]:>10,} | RPL={row[4]:.4f}")

    # Write parquet
    out_path = FACT_DIR / "svi_county" / "data.parquet"
    if not args.dry_run:
        out_path.parent.mkdir(parents=True, exist_ok=True)
        con.execute(f"COPY fact_svi_county TO '{out_path}' (FORMAT PARQUET, COMPRESSION ZSTD)")
        size_mb = out_path.stat().st_size / (1024 * 1024)
        print(f"\n  -> {out_path.relative_to(PROJECT_ROOT)} ({count:,} rows, {size_mb:.2f} MB)")
    else:
        print(f"\n  [dry-run] {out_path.relative_to(PROJECT_ROOT)} ({count:,} rows)")

    # Manifest
    if not args.dry_run:
        META_DIR.mkdir(parents=True, exist_ok=True)
        manifest = {
            "run_id": RUN_ID,
            "snapshot_date": SNAPSHOT_DATE,
            "script": "build_lake_svi_county.py",
            "source": "https://www.atsdr.cdc.gov/placeandhealth/svi/data_documentation_download.html",
            "tables": {
                "fact_svi_county": {
                    "rows": count,
                    "columns": len(cols),
                    "path": str(out_path.relative_to(PROJECT_ROOT)),
                }
            },
            "completed_at": datetime.now().isoformat() + "Z",
        }
        manifest_path = META_DIR / f"manifest_svi_county_{SNAPSHOT_DATE}.json"
        manifest_path.write_text(json.dumps(manifest, indent=2))
        print(f"  Manifest: {manifest_path.name}")

    con.close()

    print("\n" + "=" * 60)
    print("SVI COUNTY INGESTION COMPLETE")
    print(f"  fact_svi_county: {count:,} rows, {len(cols)} columns")
    print("=" * 60)


if __name__ == "__main__":
    main()
