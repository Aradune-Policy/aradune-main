#!/usr/bin/env python3
"""
build_lake_sdud_historical.py — Download & ingest SDUD 1991-2019 into the Aradune data lake.

Reads from: data/raw/sdud_historical/sdud_{YEAR}.csv (bulk CSVs from download.medicaid.gov)
Writes to:  data/lake/fact/sdud_{YEAR}/data.parquet  (one per year)
            data/lake/fact/sdud_historical_combined/data.parquet  (all 1991-2019)

Download URLs discovered from data.medicaid.gov metastore API.
Source: https://www.medicaid.gov/medicaid/prescription-drugs/state-drug-utilization-data

Schema (standardized to sdud_2025 format):
  utilization_type, state_code, ndc, labeler_code, product_code, package_size,
  year, quarter, suppression_used, product_name, units_reimbursed,
  number_of_prescriptions, total_amount_reimbursed, medicaid_amount_reimbursed,
  non_medicaid_amount_reimbursed, source, snapshot_date
"""

import argparse
import json
import subprocess
import sys
import uuid
from datetime import date, datetime
from pathlib import Path

import duckdb

PROJECT_ROOT = Path(__file__).resolve().parent.parent
RAW_DIR = PROJECT_ROOT / "data" / "raw" / "sdud_historical"
LAKE_DIR = PROJECT_ROOT / "data" / "lake"
FACT_DIR = LAKE_DIR / "fact"
META_DIR = LAKE_DIR / "metadata"

SNAPSHOT_DATE = date.today().isoformat()
RUN_ID = str(uuid.uuid4())

# Bulk download URLs from data.medicaid.gov metastore
DOWNLOAD_URLS = {
    1991: "https://download.medicaid.gov/data/StateDrugUtilizationData1991.csv",
    1992: "https://download.medicaid.gov/data/StateDrugUtilizationData1992.csv",
    1993: "https://download.medicaid.gov/data/StateDrugUtilizationData1993.csv",
    1994: "https://download.medicaid.gov/data/StateDrugUtilizationData1994.csv",
    1995: "https://download.medicaid.gov/data/StateDrugUtilizationData1995.csv",
    1996: "https://download.medicaid.gov/data/StateDrugUtilizationData1996.csv",
    1997: "https://download.medicaid.gov/data/StateDrugUtilizationData1997.csv",
    1998: "https://download.medicaid.gov/data/StateDrugUtilizationData1998.csv",
    1999: "https://download.medicaid.gov/data/StateDrugUtilizationData1999.csv",
    2000: "https://download.medicaid.gov/data/StateDrugUtilizationData2000.csv",
    2001: "https://download.medicaid.gov/data/StateDrugUtilizationData2001.csv",
    2002: "https://download.medicaid.gov/data/StateDrugUtilizationData2002.csv",
    2003: "https://download.medicaid.gov/data/StateDrugUtilizationData2003.csv",
    2004: "https://download.medicaid.gov/data/StateDrugUtilizationData2004.csv",
    2005: "https://download.medicaid.gov/data/StateDrugUtilizationData2005.csv",
    2006: "https://download.medicaid.gov/data/StateDrugUtilizationData2006.csv",
    2007: "https://download.medicaid.gov/data/StateDrugUtilizationData2007.csv",
    2008: "https://download.medicaid.gov/data/StateDrugUtilizationData2008.csv",
    2009: "https://download.medicaid.gov/data/StateDrugUtilizationData2009.csv",
    2010: "https://download.medicaid.gov/data/StateDrugUtilizationData2010.csv",
    2011: "https://download.medicaid.gov/data/StateDrugUtilizationData2011.csv",
    2012: "https://download.medicaid.gov/data/StateDrugUtilizationData2012.csv",
    2013: "https://download.medicaid.gov/data/StateDrugUtilizationData2013.csv",
    2014: "https://download.medicaid.gov/data/StateDrugUtilizationData2014.csv",
    2015: "https://download.medicaid.gov/data/StateDrugUtilizationData2015.csv",
    2016: "https://download.medicaid.gov/data/StateDrugUtilizationData2016.csv",
    2017: "https://download.medicaid.gov/data/StateDrugUtilizationData2017.csv",
    2018: "https://download.medicaid.gov/data/StateDrugUtilizationData2018.csv",
    2019: "https://download.medicaid.gov/data/SDUD2019.csv",
}


def download_year(year: int, force: bool = False) -> Path:
    """Download a single year's CSV using curl."""
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    csv_path = RAW_DIR / f"sdud_{year}.csv"

    if csv_path.exists() and csv_path.stat().st_size > 1000 and not force:
        size_mb = csv_path.stat().st_size / (1024 * 1024)
        print(f"  Already downloaded: {csv_path.name} ({size_mb:.1f} MB)")
        return csv_path

    url = DOWNLOAD_URLS.get(year)
    if not url:
        print(f"  ERROR: No download URL for year {year}")
        return csv_path

    print(f"  Downloading {url} ...")
    result = subprocess.run(
        ["curl", "-L", "-o", str(csv_path), url],
        capture_output=True, text=True, timeout=600
    )
    if result.returncode != 0:
        print(f"  ERROR downloading {year}: {result.stderr[:200]}")
    else:
        size_mb = csv_path.stat().st_size / (1024 * 1024)
        print(f"  Downloaded: {csv_path.name} ({size_mb:.1f} MB)")

    return csv_path


def write_parquet(con, table_name: str, out_path: Path, dry_run: bool) -> int:
    """Write a DuckDB table to Parquet with ZSTD compression."""
    count = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
    if not dry_run and count > 0:
        out_path.parent.mkdir(parents=True, exist_ok=True)
        con.execute(f"COPY {table_name} TO '{out_path}' (FORMAT PARQUET, COMPRESSION ZSTD)")
        size_mb = out_path.stat().st_size / (1024 * 1024)
        print(f"  -> {out_path.relative_to(LAKE_DIR)} ({count:,} rows, {size_mb:.1f} MB)")
    elif dry_run:
        print(f"  [dry-run] {out_path.relative_to(LAKE_DIR)} ({count:,} rows)")
    return count


def _out_path(fact_name: str) -> Path:
    """Output path for a fact table (flat, no snapshot partition for simplicity)."""
    return FACT_DIR / fact_name / "data.parquet"


def build_sdud_year(con, year: int, csv_path: Path, dry_run: bool) -> int:
    """Build a single year's SDUD fact table."""
    fact_name = f"sdud_{year}"
    print(f"\nBuilding fact_{fact_name} from {csv_path.name}...")

    if not csv_path.exists() or csv_path.stat().st_size < 1000:
        print(f"  SKIPPED - {csv_path} not found or too small")
        return 0

    # Load raw CSV with all_varchar to handle zero-padded NDCs etc.
    con.execute(f"""
        CREATE OR REPLACE TABLE _sdud_raw AS
        SELECT * FROM read_csv_auto(
            '{csv_path}',
            sample_size=20000,
            ignore_errors=true,
            all_varchar=true
        )
    """)

    # Check available columns
    raw_cols = [r[0] for r in con.execute("DESCRIBE _sdud_raw").fetchall()]
    print(f"  Raw columns ({len(raw_cols)}): {raw_cols[:5]}...")

    # Step 1: Rename columns to snake_case (all files have consistent headers)
    con.execute("""
        CREATE OR REPLACE TABLE _sdud_renamed AS
        SELECT
            "Utilization Type"               AS utilization_type,
            "State"                          AS state,
            "NDC"                            AS ndc,
            "Labeler Code"                   AS labeler_code,
            "Product Code"                   AS product_code,
            "Package Size"                   AS package_size,
            "Year"                           AS year_raw,
            "Quarter"                        AS quarter_raw,
            "Suppression Used"               AS suppression_used,
            "Product Name"                   AS product_name,
            "Units Reimbursed"               AS units_raw,
            "Number of Prescriptions"        AS rx_raw,
            "Total Amount Reimbursed"        AS total_raw,
            "Medicaid Amount Reimbursed"     AS medicaid_raw,
            "Non Medicaid Amount Reimbursed" AS non_medicaid_raw
        FROM _sdud_raw
    """)

    # Step 2: Cast types and clean
    con.execute(f"""
        CREATE OR REPLACE TABLE _fact_sdud AS
        SELECT
            TRIM(utilization_type)                                              AS utilization_type,
            TRIM(state)                                                         AS state_code,
            TRIM(ndc)                                                           AS ndc,
            TRIM(labeler_code)                                                  AS labeler_code,
            TRIM(product_code)                                                  AS product_code,
            TRIM(package_size)                                                  AS package_size,
            TRY_CAST(year_raw AS INTEGER)                                       AS year,
            TRY_CAST(quarter_raw AS INTEGER)                                    AS quarter,
            TRIM(suppression_used)                                              AS suppression_used,
            TRIM(product_name)                                                  AS product_name,
            TRY_CAST(units_raw AS DOUBLE)                                       AS units_reimbursed,
            TRY_CAST(rx_raw AS INTEGER)                                         AS number_of_prescriptions,
            TRY_CAST(total_raw AS DOUBLE)                                       AS total_amount_reimbursed,
            TRY_CAST(medicaid_raw AS DOUBLE)                                    AS medicaid_amount_reimbursed,
            TRY_CAST(non_medicaid_raw AS DOUBLE)                                AS non_medicaid_amount_reimbursed,
            'data.medicaid.gov'                                                 AS source,
            DATE '{SNAPSHOT_DATE}'                                              AS snapshot_date
        FROM _sdud_renamed
        WHERE TRIM(state) IS NOT NULL
          AND LENGTH(TRIM(state)) = 2
        -- Note: 'state' refers to the raw column alias in _sdud_renamed,
        -- output column is 'state_code' to match sdud_2025 schema
    """)

    out = _out_path(fact_name)
    count = write_parquet(con, "_fact_sdud", out, dry_run)

    # Validation stats
    if count > 0:
        stats = con.execute("""
            SELECT
                COUNT(DISTINCT state_code) AS states,
                COUNT(DISTINCT quarter) AS quarters,
                COUNT(DISTINCT ndc) AS ndcs,
                SUM(total_amount_reimbursed) AS total_spent,
                SUM(number_of_prescriptions) AS total_rx
            FROM _fact_sdud
        """).fetchone()
        print(f"  {count:,} rows | {stats[0]} states | {stats[1]} quarters | {stats[2]:,} NDCs")
        if stats[3]:
            print(f"  Total reimbursed: ${stats[3]:,.0f}")
        if stats[4]:
            print(f"  Total prescriptions: {stats[4]:,}")

    con.execute("DROP TABLE IF EXISTS _sdud_raw")
    con.execute("DROP TABLE IF EXISTS _sdud_renamed")
    con.execute("DROP TABLE IF EXISTS _fact_sdud")
    return count


def build_historical_combined(con, years: list, dry_run: bool) -> int:
    """Build sdud_historical_combined from all 1991-2019 parquet files."""
    print("\n" + "=" * 60)
    print("Building fact_sdud_historical_combined (all 1991-2019)...")
    print("=" * 60)

    parts = []
    for y in years:
        parquet = _out_path(f"sdud_{y}")
        if parquet.exists():
            parts.append(f"SELECT * FROM '{parquet}'")
        else:
            print(f"  WARNING: sdud_{y} parquet not found, skipping")

    if not parts:
        print("  SKIPPED - no year tables found")
        return 0

    union_sql = " UNION ALL ".join(parts)
    con.execute(f"""
        CREATE OR REPLACE TABLE _sdud_hist_combined AS
        SELECT * FROM ({union_sql})
        ORDER BY year, quarter, state_code, ndc
    """)

    out = _out_path("sdud_historical_combined")
    count = write_parquet(con, "_sdud_hist_combined", out, dry_run)

    if count > 0:
        stats = con.execute("""
            SELECT
                MIN(year) AS min_year, MAX(year) AS max_year,
                COUNT(DISTINCT year) AS num_years,
                COUNT(DISTINCT state_code) AS states,
                COUNT(DISTINCT ndc) AS ndcs,
                SUM(total_amount_reimbursed) AS total_spent,
                SUM(number_of_prescriptions) AS total_rx
            FROM _sdud_hist_combined
        """).fetchone()
        print(f"  {count:,} rows | years {stats[0]}-{stats[1]} ({stats[2]} years)")
        print(f"  {stats[3]} states | {stats[4]:,} NDCs")
        if stats[5]:
            print(f"  Total reimbursed (all years): ${stats[5]:,.0f}")
        if stats[6]:
            print(f"  Total prescriptions (all years): {stats[6]:,}")

        # Per-year breakdown
        yearly = con.execute("""
            SELECT year, COUNT(*) AS rows, SUM(total_amount_reimbursed) AS spent
            FROM _sdud_hist_combined
            GROUP BY year ORDER BY year
        """).fetchall()
        print("\n  Year breakdown:")
        for row in yearly:
            spent_str = f"${row[2]:,.0f}" if row[2] else "$0"
            print(f"    {row[0]}: {row[1]:>10,} rows  {spent_str:>20}")

    con.execute("DROP TABLE IF EXISTS _sdud_hist_combined")
    return count


def main():
    parser = argparse.ArgumentParser(
        description="Download & ingest SDUD 1991-2019 into Aradune lake")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--years", type=str, default=None,
                        help="Comma-separated years (default: 1991-2019)")
    parser.add_argument("--skip-download", action="store_true",
                        help="Skip download, use existing CSVs")
    parser.add_argument("--skip-combined", action="store_true",
                        help="Skip building the combined table")
    parser.add_argument("--download-only", action="store_true",
                        help="Only download, don't ingest")
    parser.add_argument("--force-download", action="store_true",
                        help="Re-download even if file exists")
    args = parser.parse_args()

    if args.years:
        years = [int(y.strip()) for y in args.years.split(",")]
    else:
        years = list(range(2019, 1990, -1))  # 2019 down to 1991

    print(f"SDUD Historical Ingestion (1991-2019)")
    print(f"Snapshot: {SNAPSHOT_DATE}")
    print(f"Run ID:   {RUN_ID}")
    print(f"Years:    {years}")
    print()

    # Phase 1: Download
    if not args.skip_download:
        print("=" * 60)
        print("PHASE 1: DOWNLOADING RAW CSVs")
        print("=" * 60)
        for year in years:
            download_year(year, force=args.force_download)

    if args.download_only:
        print("\nDownload-only mode. Exiting.")
        return

    # Phase 2: Ingest
    print("\n" + "=" * 60)
    print("PHASE 2: INGESTING TO PARQUET")
    print("=" * 60)

    con = duckdb.connect()
    totals = {}

    for year in years:
        csv_path = RAW_DIR / f"sdud_{year}.csv"
        totals[f"sdud_{year}"] = build_sdud_year(con, year, csv_path, args.dry_run)

    # Phase 3: Combined table
    if not args.skip_combined:
        totals["sdud_historical_combined"] = build_historical_combined(
            con, years, args.dry_run)

    con.close()

    # Summary
    print("\n" + "=" * 60)
    print("SDUD HISTORICAL INGESTION COMPLETE")
    print("=" * 60)
    total_rows = sum(totals.values())
    for name, count in sorted(totals.items()):
        status = "written" if not args.dry_run else "dry-run"
        print(f"  fact_{name:35s} {count:>12,} rows  [{status}]")
    print(f"  {'TOTAL':40s} {total_rows:>12,} rows")

    # Manifest
    if not args.dry_run and total_rows > 0:
        META_DIR.mkdir(parents=True, exist_ok=True)
        manifest = {
            "snapshot_date": SNAPSHOT_DATE,
            "pipeline_run_id": RUN_ID,
            "created_at": datetime.now().isoformat() + "Z",
            "source_url": "https://download.medicaid.gov/data/",
            "source_api": "data.medicaid.gov metastore + bulk CSV download",
            "tables": {
                f"fact_{name}": {"rows": count}
                for name, count in totals.items()
            },
            "total_rows": total_rows,
            "years_covered": sorted([y for y in years if totals.get(f"sdud_{y}", 0) > 0]),
            "notes": (
                "SDUD: NDC-level Medicaid Rx utilization and spending by state, quarter, FFS/MCO. "
                "Pre-rebate amounts. Suppressed cells (Rx<11) retained with flag. "
                "Historical 1991-2019 bulk CSV downloads from download.medicaid.gov."
            ),
        }
        manifest_file = META_DIR / f"manifest_sdud_historical_1991_2019_{SNAPSHOT_DATE}.json"
        with open(manifest_file, "w") as f:
            json.dump(manifest, f, indent=2)
        print(f"\n  Manifest: {manifest_file}")

    print("\nDone.")


if __name__ == "__main__":
    main()
