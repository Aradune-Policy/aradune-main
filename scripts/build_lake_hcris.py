#!/usr/bin/env python3
"""
build_lake_hcris.py — Ingest HCRIS hospital cost report data into the Aradune data lake.

Reads from: data/raw/hcris_{year}.csv (CMS Hospital Provider Cost Report)
            data/raw/hcris_snf_{year}.csv (SNF Cost Report)
Writes to:  data/lake/

Tables built:
  Facts:
    fact_hospital_cost    — Hospital financial data: costs, revenue, Medicaid days/charges, DSH, beds
                           Multi-year (FY2021-2023) for trend analysis

Usage:
  python3 scripts/build_lake_hcris.py
  python3 scripts/build_lake_hcris.py --dry-run
  python3 scripts/build_lake_hcris.py --years 2023     # Single year only
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

# Multi-year support — all available years
HCRIS_YEARS = [2021, 2022, 2023]
SNF_CSV = RAW_DIR / "hcris_snf_2023.csv"

SNAPSHOT_DATE = date.today().isoformat()
RUN_ID = str(uuid.uuid4())


def write_parquet(con, table_name: str, out_path: Path, dry_run: bool) -> int:
    count = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
    if not dry_run and count > 0:
        out_path.parent.mkdir(parents=True, exist_ok=True)
        con.execute(f"COPY {table_name} TO '{out_path}' (FORMAT PARQUET, COMPRESSION ZSTD)")
        size_kb = out_path.stat().st_size / 1024
        print(f"  -> {out_path.relative_to(LAKE_DIR)} ({count:,} rows, {size_kb:.1f} KB)")
    elif dry_run:
        print(f"  [dry-run] {out_path.relative_to(LAKE_DIR)} ({count:,} rows)")
    return count


def _snapshot_path(fact_name: str) -> Path:
    return FACT_DIR / fact_name / f"snapshot={SNAPSHOT_DATE}" / "data.parquet"


def _build_hospital_query(csv_path: str, report_year: int) -> str:
    """Generate the SELECT query for a single HCRIS year."""
    return f"""
        SELECT
            "Provider CCN" AS provider_ccn,
            "Hospital Name" AS hospital_name,
            "Street Address" AS street_address,
            "City" AS city,
            "State Code" AS state_code,
            "Zip Code" AS zip_code,
            "County" AS county,
            TRY_CAST("Medicare CBSA Number" AS VARCHAR) AS cbsa_code,
            "Rural Versus Urban" AS rural_urban,
            "CCN Facility Type" AS facility_type,
            "Provider Type" AS provider_type_code,
            "Type of Control" AS control_type_code,
            TRY_CAST("Fiscal Year Begin Date" AS DATE) AS fy_begin_date,
            TRY_CAST("Fiscal Year End Date" AS DATE) AS fy_end_date,
            TRY_CAST("Number of Beds" AS INTEGER) AS bed_count,
            TRY_CAST("Total Bed Days Available" AS BIGINT) AS bed_days_available,
            TRY_CAST("FTE - Employees on Payroll" AS DOUBLE) AS fte_employees,
            TRY_CAST("Number of Interns and Residents (FTE)" AS DOUBLE) AS fte_residents,
            -- Utilization
            TRY_CAST("Total Days Title XVIII" AS BIGINT) AS medicare_days,
            TRY_CAST("Total Days Title XIX" AS BIGINT) AS medicaid_days,
            TRY_CAST("Total Days (V + XVIII + XIX + Unknown)" AS BIGINT) AS total_days,
            TRY_CAST("Total Discharges Title XVIII" AS INTEGER) AS medicare_discharges,
            TRY_CAST("Total Discharges Title XIX" AS INTEGER) AS medicaid_discharges,
            TRY_CAST("Total Discharges (V + XVIII + XIX + Unknown)" AS INTEGER) AS total_discharges,
            -- Financials
            TRY_CAST("Total Costs" AS DOUBLE) AS total_costs,
            TRY_CAST("Total Salaries From Worksheet A" AS DOUBLE) AS total_salaries,
            TRY_CAST("Net Patient Revenue" AS DOUBLE) AS net_patient_revenue,
            TRY_CAST("Net Income" AS DOUBLE) AS net_income,
            TRY_CAST("Total Income" AS DOUBLE) AS total_income,
            TRY_CAST("Inpatient Revenue" AS DOUBLE) AS inpatient_revenue,
            TRY_CAST("Outpatient Revenue" AS DOUBLE) AS outpatient_revenue,
            TRY_CAST("Cost To Charge Ratio" AS DOUBLE) AS cost_to_charge_ratio,
            -- Medicaid-specific
            TRY_CAST("Net Revenue from Medicaid" AS DOUBLE) AS medicaid_net_revenue,
            TRY_CAST("Medicaid Charges" AS DOUBLE) AS medicaid_charges,
            TRY_CAST("Net Revenue from Stand-Alone CHIP" AS DOUBLE) AS chip_net_revenue,
            TRY_CAST("Stand-Alone CHIP Charges" AS DOUBLE) AS chip_charges,
            -- Safety net indicators
            TRY_CAST("Cost of Charity Care" AS DOUBLE) AS charity_care_cost,
            TRY_CAST("Total Bad Debt Expense" AS DOUBLE) AS bad_debt_expense,
            TRY_CAST("Cost of Uncompensated Care" AS DOUBLE) AS uncompensated_care_cost,
            TRY_CAST("Total Unreimbursed and Uncompensated Care" AS DOUBLE) AS total_unreimbursed_care,
            -- DSH / IME
            TRY_CAST("Disproportionate Share Adjustment" AS DOUBLE) AS dsh_adjustment,
            TRY_CAST("Allowable DSH Percentage" AS DOUBLE) AS dsh_pct,
            TRY_CAST("Total IME Payment" AS DOUBLE) AS ime_payment,
            -- Balance sheet
            TRY_CAST("Total Assets" AS DOUBLE) AS total_assets,
            TRY_CAST("Total Liabilities" AS DOUBLE) AS total_liabilities,
            -- Derived
            CASE
                WHEN TRY_CAST("Total Days (V + XVIII + XIX + Unknown)" AS BIGINT) > 0
                THEN ROUND(TRY_CAST("Total Days Title XIX" AS DOUBLE)
                     / TRY_CAST("Total Days (V + XVIII + XIX + Unknown)" AS DOUBLE) * 100, 2)
            END AS medicaid_day_pct,
            CASE
                WHEN TRY_CAST("Medicaid Charges" AS DOUBLE) > 0
                     AND TRY_CAST("Net Revenue from Medicaid" AS DOUBLE) IS NOT NULL
                THEN ROUND(TRY_CAST("Net Revenue from Medicaid" AS DOUBLE)
                     / TRY_CAST("Medicaid Charges" AS DOUBLE) * 100, 2)
            END AS medicaid_payment_to_charge_pct,
            'data.cms.gov/hcris' AS source,
            {report_year} AS report_year,
            DATE '{SNAPSHOT_DATE}' AS snapshot_date
        FROM read_csv_auto('{csv_path}')
        WHERE "State Code" IS NOT NULL
          AND LENGTH("State Code") = 2
    """


def build_fact_hospital_cost(con, years: list[int], dry_run: bool) -> int:
    print(f"Building fact_hospital_cost ({len(years)} years: {', '.join(map(str, years))})...")

    union_parts = []
    for year in years:
        csv_path = RAW_DIR / f"hcris_{year}.csv"
        if not csv_path.exists():
            print(f"  SKIPPED FY{year} — {csv_path.name} not found")
            continue
        union_parts.append(_build_hospital_query(str(csv_path), year))
        print(f"  Loading FY{year}...")

    if not union_parts:
        print("  ERROR: No HCRIS CSVs found")
        return 0

    full_query = " UNION ALL ".join(union_parts)
    con.execute(f"CREATE OR REPLACE TABLE _fact_hospital_cost AS {full_query}")

    count = write_parquet(con, "_fact_hospital_cost", _snapshot_path("hospital_cost"), dry_run)
    states = con.execute("SELECT COUNT(DISTINCT state_code) FROM _fact_hospital_cost").fetchone()[0]
    beds = con.execute("SELECT SUM(bed_count) FROM _fact_hospital_cost WHERE report_year = (SELECT MAX(report_year) FROM _fact_hospital_cost)").fetchone()[0]
    med_rev = con.execute("SELECT SUM(medicaid_net_revenue) FROM _fact_hospital_cost WHERE medicaid_net_revenue > 0").fetchone()[0]
    year_counts = con.execute("SELECT report_year, COUNT(*) FROM _fact_hospital_cost GROUP BY 1 ORDER BY 1").fetchall()
    print(f"  {count:,} total rows, {states} states, {beds:,} beds (latest year), ${med_rev/1e9:.1f}B Medicaid revenue")
    for yr, cnt in year_counts:
        print(f"    FY{yr}: {cnt:,} hospitals")
    con.execute("DROP TABLE IF EXISTS _fact_hospital_cost")
    return count


def _build_snf_query(csv_path: str, report_year: int) -> str:
    """Generate the SELECT query for a single SNF HCRIS year."""
    return f"""
        SELECT
            "Provider CCN" AS provider_ccn,
            "Facility Name" AS facility_name,
            "Street Address" AS street_address,
            "City" AS city,
            "State Code" AS state_code,
            "Zip Code" AS zip_code,
            "County" AS county,
            TRY_CAST("Medicare CBSA Number" AS VARCHAR) AS cbsa_code,
            "Rural versus Urban" AS rural_urban,
            "Type of Control" AS control_type_code,
            TRY_CAST("Fiscal Year Begin Date" AS DATE) AS fy_begin_date,
            TRY_CAST("Fiscal Year End Date" AS DATE) AS fy_end_date,
            TRY_CAST("Number of Beds" AS INTEGER) AS total_beds,
            TRY_CAST("SNF Number of Beds" AS INTEGER) AS snf_beds,
            TRY_CAST("NF Number of Beds" AS INTEGER) AS nf_beds,
            TRY_CAST("Total Bed Days Available" AS BIGINT) AS bed_days_available,
            TRY_CAST("Total Days Title XVIII" AS BIGINT) AS medicare_days,
            TRY_CAST("Total Days Title XIX" AS BIGINT) AS medicaid_days,
            TRY_CAST("Total Days Total" AS BIGINT) AS total_days,
            TRY_CAST("Total Discharges Title XVIII" AS INTEGER) AS medicare_discharges,
            TRY_CAST("Total Discharges Title XIX" AS INTEGER) AS medicaid_discharges,
            TRY_CAST("Total Discharges Total" AS INTEGER) AS total_discharges,
            TRY_CAST("NF Days Title XIX" AS BIGINT) AS nf_medicaid_days,
            TRY_CAST("NF Days Total" AS BIGINT) AS nf_total_days,
            TRY_CAST("NF Discharges Title XIX" AS INTEGER) AS nf_medicaid_discharges,
            TRY_CAST("NF Discharges Total" AS INTEGER) AS nf_total_discharges,
            TRY_CAST("SNF Days Title XVIII" AS BIGINT) AS snf_medicare_days,
            TRY_CAST("SNF Days Title XIX" AS BIGINT) AS snf_medicaid_days,
            TRY_CAST("SNF Days Total" AS BIGINT) AS snf_total_days,
            TRY_CAST("Total Costs" AS DOUBLE) AS total_costs,
            TRY_CAST("Total Salaries From Worksheet A" AS DOUBLE) AS total_salaries,
            TRY_CAST("Total Charges" AS DOUBLE) AS total_charges,
            TRY_CAST("Net Patient Revenue" AS DOUBLE) AS net_patient_revenue,
            TRY_CAST("Net Income" AS DOUBLE) AS net_income,
            TRY_CAST("Total Assets" AS DOUBLE) AS total_assets,
            TRY_CAST("Total liabilities" AS DOUBLE) AS total_liabilities,
            CASE
                WHEN TRY_CAST("Total Days Total" AS BIGINT) > 0
                THEN ROUND(TRY_CAST("Total Days Title XIX" AS DOUBLE)
                     / TRY_CAST("Total Days Total" AS DOUBLE) * 100, 2)
            END AS medicaid_day_pct,
            CASE
                WHEN TRY_CAST("Total Bed Days Available" AS BIGINT) > 0
                THEN ROUND(TRY_CAST("Total Days Total" AS DOUBLE)
                     / TRY_CAST("Total Bed Days Available" AS DOUBLE) * 100, 2)
            END AS occupancy_pct,
            'data.cms.gov/hcris-snf' AS source,
            {report_year} AS report_year,
            DATE '{SNAPSHOT_DATE}' AS snapshot_date
        FROM read_csv_auto('{csv_path}')
        WHERE "State Code" IS NOT NULL
          AND LENGTH("State Code") = 2
    """


def build_fact_snf_cost(con, years: list[int], dry_run: bool) -> int:
    print(f"Building fact_snf_cost ({len(years)} years)...")

    union_parts = []
    for year in years:
        csv_path = RAW_DIR / f"hcris_snf_{year}.csv"
        if not csv_path.exists():
            print(f"  SKIPPED FY{year} — {csv_path.name} not found")
            continue
        union_parts.append(_build_snf_query(str(csv_path), year))
        print(f"  Loading FY{year}...")

    if not union_parts:
        print("  ERROR: No SNF CSVs found")
        return 0

    full_query = " UNION ALL ".join(union_parts)
    con.execute(f"CREATE OR REPLACE TABLE _fact_snf_cost AS {full_query}")

    count = write_parquet(con, "_fact_snf_cost", _snapshot_path("snf_cost"), dry_run)
    states = con.execute("SELECT COUNT(DISTINCT state_code) FROM _fact_snf_cost").fetchone()[0]
    beds = con.execute("SELECT SUM(total_beds) FROM _fact_snf_cost WHERE report_year = (SELECT MAX(report_year) FROM _fact_snf_cost) AND total_beds > 0").fetchone()[0]
    med_days = con.execute("SELECT SUM(medicaid_days) FROM _fact_snf_cost WHERE medicaid_days > 0").fetchone()[0]
    year_counts = con.execute("SELECT report_year, COUNT(*) FROM _fact_snf_cost GROUP BY 1 ORDER BY 1").fetchall()
    print(f"  {count:,} total rows, {states} states, {beds:,} beds (latest year), {med_days:,} Medicaid days")
    for yr, cnt in year_counts:
        print(f"    FY{yr}: {cnt:,} facilities")
    con.execute("DROP TABLE IF EXISTS _fact_snf_cost")
    return count


def main():
    parser = argparse.ArgumentParser(description="Ingest HCRIS hospital cost report data into Aradune lake")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--years", type=str, default=None,
                        help="Comma-separated years to process (default: all available)")
    args = parser.parse_args()

    years = HCRIS_YEARS
    if args.years:
        years = [int(y.strip()) for y in args.years.split(",")]

    # Check at least one CSV exists
    available = [y for y in years if (RAW_DIR / f"hcris_{y}.csv").exists()]
    if not available:
        print(f"ERROR: No HCRIS CSVs found for years {years}", file=sys.stderr)
        print("Download from: https://data.cms.gov/provider-compliance/cost-report/hospital-provider-cost-report")
        sys.exit(1)

    print(f"Snapshot: {SNAPSHOT_DATE}")
    print(f"Run ID:   {RUN_ID}")
    print(f"Years:    {', '.join(map(str, available))}")
    print()

    con = duckdb.connect()
    totals = {}
    totals["fact_hospital_cost"] = build_fact_hospital_cost(con, available, args.dry_run)
    print()
    snf_available = [y for y in years if (RAW_DIR / f"hcris_snf_{y}.csv").exists()]
    totals["fact_snf_cost"] = build_fact_snf_cost(con, snf_available, args.dry_run)
    con.close()

    print()
    print("=" * 60)
    print("HCRIS LAKE INGESTION COMPLETE")
    print("=" * 60)
    total_rows = sum(totals.values())
    for name, count in totals.items():
        status = "written" if not args.dry_run else "dry-run"
        print(f"  {name:30s} {count:>10,} rows  [{status}]")
    print(f"  {'TOTAL':30s} {total_rows:>10,} rows")

    if not args.dry_run:
        META_DIR.mkdir(parents=True, exist_ok=True)
        manifest = {
            "snapshot_date": SNAPSHOT_DATE,
            "pipeline_run_id": RUN_ID,
            "created_at": datetime.now().isoformat() + "Z",
            "source_files": [str(RAW_DIR / f"hcris_{y}.csv") for y in available],
            "report_years": available,
            "tables": {name: {"rows": count} for name, count in totals.items()},
            "total_rows": total_rows,
        }
        manifest_file = META_DIR / f"manifest_hcris_{SNAPSHOT_DATE}.json"
        with open(manifest_file, "w") as f:
            json.dump(manifest, f, indent=2)
        print(f"\n  Manifest: {manifest_file}")

    print("\nDone.")


if __name__ == "__main__":
    main()
