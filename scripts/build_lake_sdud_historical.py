#!/usr/bin/env python3
"""
build_lake_sdud_historical.py — Ingest SDUD 2020-2023 into the Aradune data lake.

Reads from: data/raw/sdud_2020.csv through data/raw/sdud_2023.csv
Writes to:  data/lake/fact/sdud_{YEAR}/snapshot={DATE}/data.parquet
            data/lake/fact/sdud_combined/snapshot={DATE}/data.parquet

Matches the schema of existing fact_sdud_2024:
  utilization_type, state, ndc, year, quarter, product_name,
  units_reimbursed, num_prescriptions, total_reimbursed,
  medicaid_reimbursed, non_medicaid_reimbursed, snapshot_date, snapshot

Source: Medicaid.gov SDUD (https://www.medicaid.gov/medicaid/prescription-drugs/state-drug-utilization-data)
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

SNAPSHOT_DATE = date.today().isoformat()
RUN_ID = str(uuid.uuid4())

# Column mapping: raw CSV column names -> our schema
# SDUD CSVs may have varying column names across years
COLUMN_MAP = {
    "Utilization Type": "utilization_type",
    "State": "state",
    "NDC": "ndc",
    "Year": "year",
    "Quarter": "quarter",
    "Product Name": "product_name",
    "Units Reimbursed": "units_reimbursed",
    "Number of Prescriptions": "num_prescriptions",
    "Total Amount Reimbursed": "total_reimbursed",
    "Medicaid Amount Reimbursed": "medicaid_reimbursed",
    "Non Medicaid Amount Reimbursed": "non_medicaid_reimbursed",
    # Alternative names seen in some years
    "Labeler Code": None,  # Extractable from NDC, skip
    "Product Code": None,
    "Package Size": None,
    "Suppression Used": None,  # We keep suppressed rows but note it
}


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


def build_sdud_year(con, year: int, csv_path: Path, dry_run: bool) -> int:
    """Build a single year's SDUD fact table matching existing schema."""
    fact_name = f"sdud_{year}"
    print(f"Building fact_{fact_name} from {csv_path.name}...")

    if not csv_path.exists():
        print(f"  SKIPPED - {csv_path} not found")
        return 0

    # Load raw CSV - handle various column name patterns
    con.execute(f"""
        CREATE OR REPLACE TABLE _sdud_raw_{year} AS
        SELECT * FROM read_csv_auto(
            '{csv_path}',
            sample_size=20000,
            ignore_errors=true,
            all_varchar=true
        )
    """)

    # Check available columns
    raw_cols = [r[0] for r in con.execute(f"DESCRIBE _sdud_raw_{year}").fetchall()]
    print(f"  Raw columns: {len(raw_cols)}")

    # Map to canonical schema matching fact_sdud_2024
    # Handle column name variations across years
    def col_or_null(candidates, cast_type="VARCHAR"):
        for c in candidates:
            if c in raw_cols:
                if cast_type == "VARCHAR":
                    return f'"{c}"'
                return f'TRY_CAST("{c}" AS {cast_type})'
        return "NULL"

    util_col = col_or_null(["Utilization Type", "Utilization_Type", "utilization_type"])
    state_col = col_or_null(["State", "state"])
    ndc_col = col_or_null(["NDC", "ndc"])
    year_col = col_or_null(["Year", "year"], "INTEGER")
    quarter_col = col_or_null(["Quarter", "quarter"], "INTEGER")
    product_col = col_or_null(["Product Name", "Product_Name", "product_name"])
    units_col = col_or_null(["Units Reimbursed", "Units_Reimbursed", "units_reimbursed"], "DOUBLE")
    rx_col = col_or_null(["Number of Prescriptions", "Number_of_Prescriptions", "num_prescriptions"], "INTEGER")
    total_col = col_or_null(["Total Amount Reimbursed", "Total_Amount_Reimbursed", "total_reimbursed"], "DOUBLE")
    medicaid_col = col_or_null(["Medicaid Amount Reimbursed", "Medicaid_Amount_Reimbursed", "medicaid_reimbursed"], "DOUBLE")
    non_medicaid_col = col_or_null(["Non Medicaid Amount Reimbursed", "Non_Medicaid_Amount_Reimbursed", "non_medicaid_reimbursed"], "DOUBLE")

    con.execute(f"""
        CREATE OR REPLACE TABLE _fact_sdud_{year} AS
        SELECT
            {util_col}                      AS utilization_type,
            {state_col}                     AS state,
            {ndc_col}                       AS ndc,
            {year_col}                      AS year,
            {quarter_col}                   AS quarter,
            {product_col}                   AS product_name,
            {units_col}                     AS units_reimbursed,
            {rx_col}                        AS num_prescriptions,
            {total_col}                     AS total_reimbursed,
            {medicaid_col}                  AS medicaid_reimbursed,
            {non_medicaid_col}              AS non_medicaid_reimbursed,
            '{SNAPSHOT_DATE}'               AS snapshot_date,
            DATE '{SNAPSHOT_DATE}'           AS snapshot
        FROM _sdud_raw_{year}
        WHERE {year_col} IS NOT NULL
    """)

    count = write_parquet(con, f"_fact_sdud_{year}",
                          _snapshot_path(fact_name), dry_run)

    # Validation
    if count > 0:
        stats = con.execute(f"""
            SELECT
                COUNT(DISTINCT state) AS states,
                COUNT(DISTINCT quarter) AS quarters,
                COUNT(DISTINCT ndc) AS ndcs,
                SUM(total_reimbursed) AS total_spent,
                SUM(num_prescriptions) AS total_rx
            FROM _fact_sdud_{year}
        """).fetchone()
        print(f"  {count:,} rows, {stats[0]} states, {stats[1]} quarters, {stats[2]:,} NDCs")
        if stats[3]:
            print(f"  Total reimbursed: ${stats[3]:,.0f}")
        if stats[4]:
            print(f"  Total prescriptions: {stats[4]:,}")

        # Utilization type breakdown
        ut = con.execute(f"""
            SELECT utilization_type, COUNT(*) as cnt
            FROM _fact_sdud_{year}
            GROUP BY 1 ORDER BY 2 DESC
        """).fetchall()
        for row in ut:
            print(f"    {row[0]}: {row[1]:,}")

    con.execute(f"DROP TABLE IF EXISTS _sdud_raw_{year}")
    con.execute(f"DROP TABLE IF EXISTS _fact_sdud_{year}")
    return count


def build_sdud_combined(con, years: list, dry_run: bool) -> int:
    """Build a combined multi-year SDUD table from all available years."""
    print("Building fact_sdud_combined (all years)...")

    # Union all year tables from the lake
    parts = []
    for y in range(2020, 2026):
        parquet = _snapshot_path(f"sdud_{y}")
        # Check for either today's snapshot or any previous snapshot
        fact_path = FACT_DIR / f"sdud_{y}"
        if fact_path.exists():
            snapshots = sorted(fact_path.glob("snapshot=*/data.parquet"), reverse=True)
            if snapshots:
                parts.append(f"SELECT * FROM '{snapshots[0]}'")

    if not parts:
        print("  SKIPPED - no SDUD year tables found")
        return 0

    # Select only the canonical columns to handle schema differences across years
    canonical_cols = """
        utilization_type, state, ndc, year, quarter, product_name,
        units_reimbursed, num_prescriptions, total_reimbursed,
        medicaid_reimbursed, non_medicaid_reimbursed
    """
    select_parts = [f"SELECT {canonical_cols} FROM ({p})" for p in parts]
    union_sql = " UNION ALL ".join(select_parts)
    con.execute(f"""
        CREATE OR REPLACE TABLE _sdud_combined AS
        SELECT {canonical_cols},
            '{SNAPSHOT_DATE}' AS snapshot_date,
            DATE '{SNAPSHOT_DATE}' AS snapshot
        FROM ({union_sql})
        ORDER BY year, quarter, state, ndc
    """)

    count = write_parquet(con, "_sdud_combined",
                          _snapshot_path("sdud_combined"), dry_run)

    if count > 0:
        stats = con.execute("""
            SELECT
                MIN(year) AS min_year, MAX(year) AS max_year,
                COUNT(DISTINCT state) AS states,
                COUNT(DISTINCT ndc) AS ndcs,
                SUM(total_reimbursed) AS total_spent
            FROM _sdud_combined
        """).fetchone()
        print(f"  {count:,} rows, years {stats[0]}-{stats[1]}")
        print(f"  {stats[2]} states, {stats[3]:,} NDCs")
        if stats[4]:
            print(f"  Total reimbursed (all years): ${stats[4]:,.0f}")

    con.execute("DROP TABLE IF EXISTS _sdud_combined")
    return count


def main():
    parser = argparse.ArgumentParser(
        description="Ingest SDUD 2020-2023 into Aradune lake")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--years", type=str, default="2020,2021,2022,2023",
                        help="Comma-separated years to ingest")
    parser.add_argument("--skip-combined", action="store_true",
                        help="Skip building the combined table")
    args = parser.parse_args()

    years = [int(y.strip()) for y in args.years.split(",")]

    print(f"Snapshot: {SNAPSHOT_DATE}")
    print(f"Run ID:   {RUN_ID}")
    print(f"Years:    {years}")
    print()

    con = duckdb.connect()
    totals = {}

    for year in years:
        csv_path = RAW_DIR / f"sdud_{year}.csv"
        fact_name = f"sdud_{year}"
        totals[fact_name] = build_sdud_year(con, year, csv_path, args.dry_run)
        print()

    if not args.skip_combined:
        totals["sdud_combined"] = build_sdud_combined(con, years, args.dry_run)
        print()

    con.close()

    # Summary
    print("=" * 60)
    print("SDUD HISTORICAL INGESTION COMPLETE")
    print("=" * 60)
    total_rows = sum(totals.values())
    for name, count in totals.items():
        status = "written" if not args.dry_run else "dry-run"
        print(f"  fact_{name:25s} {count:>12,} rows  [{status}]")
    print(f"  {'TOTAL':30s} {total_rows:>12,} rows")

    if not args.dry_run:
        META_DIR.mkdir(parents=True, exist_ok=True)
        manifest = {
            "snapshot_date": SNAPSHOT_DATE,
            "pipeline_run_id": RUN_ID,
            "created_at": datetime.now().isoformat() + "Z",
            "source_files": [str(RAW_DIR / f"sdud_{y}.csv") for y in years],
            "source_url": "https://www.medicaid.gov/medicaid/prescription-drugs/state-drug-utilization-data",
            "tables": {f"fact_{name}": {"rows": count} for name, count in totals.items()},
            "total_rows": total_rows,
            "notes": "SDUD: NDC-level Medicaid Rx utilization and spending by state, quarter, FFS/MCO. "
                     "Pre-rebate amounts. Suppressed cells (Rx<11) retained with flag.",
        }
        manifest_file = META_DIR / f"manifest_sdud_historical_{SNAPSHOT_DATE}.json"
        with open(manifest_file, "w") as f:
            json.dump(manifest, f, indent=2)
        print(f"\n  Manifest: {manifest_file}")

    print("\nDone.")


if __name__ == "__main__":
    main()
