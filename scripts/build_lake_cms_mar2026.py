#!/usr/bin/env python3
"""
build_lake_cms_mar2026.py
Ingest 31 new CMS/Medicaid CSV files into the Aradune data lake as Parquet.

Usage:
    python scripts/build_lake_cms_mar2026.py

Each file is read from data/raw/, cleaned, and written to
data/lake/fact/{table_name}/data.parquet with ZSTD compression.
Only files that exist on disk are processed.
"""

import os
import re
import sys
from pathlib import Path

import pandas as pd

# ── Paths ────────────────────────────────────────────────────────────────
BASE = Path(__file__).resolve().parent.parent
RAW_DIR = BASE / "data" / "raw"
LAKE_DIR = BASE / "data" / "lake" / "fact"

# ── File -> table name mapping (31 files) ────────────────────────────────
FILE_MAP = {
    "medicaid_enrollment_feb2026.csv": "enrollment_feb2026",
    "managed_care_enrollment_by_plan_2024.csv": "mc_enrollment_by_plan_2024",
    "medicaid_financial_management.csv": "cms64_financial_management",
    "medicaid_financial_management_national.csv": "cms64_financial_management_national",
    "cms64_new_adult_expenditures_feb2026.csv": "cms64_new_adult_expenditures",
    "cms64_caa_fmap_expenditures_feb2026.csv": "cms64_caa_fmap_expenditures",
    "medicaid_eligibility_processing_feb2026.csv": "eligibility_processing_feb2026",
    "major_eligibility_group_annual.csv": "major_eligibility_group_annual",
    "dual_status_annual.csv": "dual_status_annual",
    "program_info_annual.csv": "program_info_annual",
    "renewal_outcomes_oct2024.csv": "renewal_outcomes",
    "chip_enrollment_by_month_sep2025.csv": "chip_enrollment_monthly",
    "continuous_eligibility.csv": "continuous_eligibility_v2",
    "express_lane_eligibility.csv": "express_lane_eligibility_v2",
    "benefit_package_annual.csv": "benefit_package_annual",
    "medicaid_chip_eligibility_levels.csv": "medicaid_chip_eligibility_levels",
    "managed_care_programs_by_state_2023.csv": "mc_programs_by_state_2023",
    "managed_care_features_enrollment_pop_2024.csv": "mc_features_enrollment_2024",
    "share_medicaid_enrollees_mc_2024.csv": "mc_share_enrollees_2024",
    "managed_care_enrollment_pop_all_2024.csv": "mc_enrollment_pop_2024",
    "mltss_enrollment_2024.csv": "mltss_enrollment_2024",
    "managed_care_enrollment_summary_2024_v2.csv": "mc_enrollment_summary_2024_v2",
    "mlr_summary_reports_dec2025.csv": "mlr_summary_dec2025",
    "nadac_mar2026.csv": "nadac_mar2026",
    "nadac_comparison_mar2026.csv": "nadac_comparison_mar2026",
    "drug_amp_quarterly_q4_2025.csv": "drug_amp_q4_2025",
    "mdrp_drug_products_q4_2025.csv": "mdrp_drug_products_q4_2025",
    "dsh_reporting_latest.csv": "dsh_reporting_latest",
    "hcgov_transitions_unwinding_dec2024.csv": "hcgov_transitions_unwinding",
    "medicaid_enterprise_system.csv": "medicaid_enterprise_system",
    "1915c_waiver_participants_jan2025.csv": "1915c_waiver_participants_v2",
}


def clean_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Lowercase, replace spaces/special chars with underscores, strip."""
    df.columns = [
        re.sub(r"[^a-z0-9_]", "_", col.strip().lower()).strip("_")
        for col in df.columns
    ]
    # Collapse multiple underscores
    df.columns = [re.sub(r"_+", "_", col) for col in df.columns]
    return df


def drop_empty_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Drop columns that are entirely null/empty."""
    return df.dropna(axis=1, how="all")


def format_size(size_bytes: int) -> str:
    """Human-readable file size."""
    if size_bytes < 1024:
        return f"{size_bytes} B"
    elif size_bytes < 1024 * 1024:
        return f"{size_bytes / 1024:.1f} KB"
    else:
        return f"{size_bytes / (1024 * 1024):.1f} MB"


def main():
    total_tables = 0
    total_rows = 0
    total_bytes = 0
    skipped = []
    errors = []

    print("=" * 80)
    print("Aradune Data Lake Ingestion - CMS March 2026 Batch")
    print(f"Raw dir:  {RAW_DIR}")
    print(f"Lake dir: {LAKE_DIR}")
    print("=" * 80)
    print()

    for filename, table_name in FILE_MAP.items():
        raw_path = RAW_DIR / filename
        if not raw_path.exists():
            skipped.append(filename)
            print(f"  SKIP  {filename} (not found)")
            continue

        try:
            # Read CSV (on_bad_lines='warn' to handle occasional malformed rows)
            df = pd.read_csv(raw_path, low_memory=False, on_bad_lines="warn")

            # Clean column names
            df = clean_columns(df)

            # Drop completely empty columns
            df = drop_empty_columns(df)

            # Write to lake
            out_dir = LAKE_DIR / table_name
            out_dir.mkdir(parents=True, exist_ok=True)
            out_path = out_dir / "data.parquet"
            df.to_parquet(out_path, compression="zstd", index=False)

            # Stats
            size = out_path.stat().st_size
            total_tables += 1
            total_rows += len(df)
            total_bytes += size

            print(
                f"  OK    {filename}"
                f"  ->  {table_name}"
                f"  |  {len(df):>10,} rows"
                f"  |  {len(df.columns):>3} cols"
                f"  |  {format_size(size)}"
            )

        except Exception as e:
            errors.append((filename, str(e)))
            print(f"  ERR   {filename}: {e}")

    # ── Summary ──────────────────────────────────────────────────────────
    print()
    print("=" * 80)
    print(f"Tables created:  {total_tables}")
    print(f"Total rows:      {total_rows:,}")
    print(f"Total size:      {format_size(total_bytes)}")
    if skipped:
        print(f"Skipped:         {len(skipped)} (file not found)")
    if errors:
        print(f"Errors:          {len(errors)}")
        for fn, err in errors:
            print(f"  - {fn}: {err}")
    print("=" * 80)


if __name__ == "__main__":
    main()
