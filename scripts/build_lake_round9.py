#!/usr/bin/env python3
"""
Round 9: Ingest datasets downloaded by background agents.
- Medicare Enrollment (557K rows — county-level dual/demographic detail)
- CMS IPPS Impact File (3,160 hospitals — wage index, CMI, DSH%, VBP)
- Medicaid Opioid Prescribing Rates (539K rows)
- OTP Provider Directory (1,533 opioid treatment programs)
- CMS-64 FFCRA Enhanced FMAP (1,197 rows)
- Contraceptive Care Utilization (6,360 rows)
- Respiratory Conditions among Medicaid (28,620 rows)
- ESRD QIP Total Performance Scores (7,558 facilities)
- Program Enrollment by Month (13,356 rows)
- Managed Care Info Annual + Monthly (33,761 rows)
- CHIP Enrollment by Month (1,080 rows)
- CHIP Applications & Eligibility (5,567 rows)
- Performance Indicator Dataset (10,404 rows)
- Medicaid New Adult Enrollment (7,854 rows)
- Drug Rebate Products Q4 2025 (1.9M rows)
- SDUD 2024 (5.2M rows)
"""

import argparse
import json
from datetime import date
from pathlib import Path

import duckdb

RAW = Path(__file__).resolve().parent.parent / "data" / "raw"
LAKE = Path(__file__).resolve().parent.parent / "data" / "lake"
SNAP = str(date.today())


def _write_parquet(con, table: str, fact_name: str) -> int:
    out_dir = LAKE / "fact" / fact_name / f"snapshot={SNAP}"
    out_dir.mkdir(parents=True, exist_ok=True)
    out = out_dir / "data.parquet"
    con.execute(f"COPY {table} TO '{out}' (FORMAT PARQUET, COMPRESSION ZSTD)")
    n = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
    print(f"  -> {fact_name}: {n:,} rows -> {out}")
    return n


def build_medicare_enrollment(con, dry_run: bool) -> int:
    """Medicare enrollment with dual status, demographics by state/county/month."""
    csv = RAW / "medicare_enrollment.csv"
    if not csv.exists():
        print("  SKIP: medicare_enrollment.csv not found")
        return 0
    con.execute(f"""
        CREATE OR REPLACE TABLE _fact AS
        SELECT
            TRY_CAST(YEAR AS INTEGER) AS year,
            TRY_CAST(MONTH AS INTEGER) AS month,
            BENE_GEO_LVL AS geo_level,
            BENE_STATE_ABRVTN AS state,
            BENE_COUNTY_DESC AS county,
            BENE_FIPS_CD AS fips_code,
            TRY_CAST(TOT_BENES AS INTEGER) AS total_benes,
            TRY_CAST(ORGNL_MDCR_BENES AS INTEGER) AS original_medicare_benes,
            TRY_CAST(MA_AND_OTH_BENES AS INTEGER) AS ma_benes,
            TRY_CAST(AGED_TOT_BENES AS INTEGER) AS aged_benes,
            TRY_CAST(DSBLD_TOT_BENES AS INTEGER) AS disabled_benes,
            TRY_CAST(DUAL_TOT_BENES AS INTEGER) AS dual_total,
            TRY_CAST(FULL_DUAL_TOT_BENES AS INTEGER) AS full_dual,
            TRY_CAST(PART_DUAL_TOT_BENES AS INTEGER) AS partial_dual,
            TRY_CAST(QMB_ONLY_BENES AS INTEGER) AS qmb_only,
            TRY_CAST(QMB_PLUS_BENES AS INTEGER) AS qmb_plus,
            TRY_CAST(SLMB_ONLY_BENES AS INTEGER) AS slmb_only,
            TRY_CAST(SLMB_PLUS_BENES AS INTEGER) AS slmb_plus,
            TRY_CAST(A_B_TOT_BENES AS INTEGER) AS part_ab_benes,
            TRY_CAST(PRSCRPTN_DRUG_TOT_BENES AS INTEGER) AS part_d_benes,
            TRY_CAST(PRSCRPTN_DRUG_DEEMED_ELIGIBLE_FULL_LIS_BENES AS INTEGER) AS lis_full,
            TRY_CAST(PRSCRPTN_DRUG_PARTIAL_LIS_BENES AS INTEGER) AS lis_partial,
            '{SNAP}' AS snapshot_date
        FROM read_csv_auto('{csv}', header=true, ignore_errors=true)
        WHERE BENE_STATE_ABRVTN IS NOT NULL
          AND TRY_CAST(YEAR AS INTEGER) IS NOT NULL
    """)
    n = con.execute("SELECT COUNT(*) FROM _fact").fetchone()[0]
    print(f"  medicare_enrollment: {n:,} rows")
    if dry_run:
        return n
    return _write_parquet(con, "_fact", "medicare_enrollment")


def build_cms_impact(con, dry_run: bool) -> int:
    """CMS IPPS Impact File — hospital wage index, CMI, DSH%, VBP adjustments."""
    csv = RAW / "cms_impact_file.csv"
    if not csv.exists():
        print("  SKIP: cms_impact_file.csv not found")
        return 0
    con.execute(f"""
        CREATE OR REPLACE TABLE _fact AS
        SELECT
            "Provider Number" AS ccn,
            "Name" AS facility_name,
            "Geographic Labor Market Area" AS geo_lma,
            "Payment Labor Market Area" AS payment_lma,
            "FIPS County Code" AS fips_code,
            "Region" AS region,
            TRY_CAST("FY 2025 Wage Index" AS DOUBLE) AS wage_index,
            TRY_CAST("Resident to Bed Ratio" AS DOUBLE) AS resident_to_bed_ratio,
            TRY_CAST("RDAY" AS DOUBLE) AS resident_days,
            '{SNAP}' AS snapshot_date
        FROM read_csv_auto('{csv}', header=true, skip=1, ignore_errors=true)
        WHERE "Provider Number" IS NOT NULL
    """)
    n = con.execute("SELECT COUNT(*) FROM _fact").fetchone()[0]
    print(f"  cms_impact: {n:,} rows")
    if dry_run:
        return n
    return _write_parquet(con, "_fact", "cms_impact")


def build_opioid_prescribing(con, dry_run: bool) -> int:
    """Medicaid opioid prescribing rates by state/plan type."""
    csv = RAW / "medicaid_opioid_prescribing_rates.csv"
    if not csv.exists():
        print("  SKIP: medicaid_opioid_prescribing_rates.csv not found")
        return 0
    con.execute(f"""
        CREATE OR REPLACE TABLE _fact AS
        SELECT
            CAST(Year AS INTEGER) AS year,
            Geo_Lvl AS geo_level,
            Geo_Cd AS geo_code,
            Geo_Desc AS geo_desc,
            Plan_Type AS plan_type,
            TRY_CAST(Tot_Opioid_Clms AS BIGINT) AS opioid_claims,
            TRY_CAST(Tot_Clms AS BIGINT) AS total_claims,
            TRY_CAST(Opioid_Prscrbng_Rate AS DOUBLE) AS opioid_prescribing_rate,
            TRY_CAST(LA_Tot_Opioid_Clms AS BIGINT) AS long_acting_opioid_claims,
            TRY_CAST(LA_Opioid_Prscrbng_Rate AS DOUBLE) AS la_opioid_rate,
            '{SNAP}' AS snapshot_date
        FROM read_csv_auto('{csv}', header=true, ignore_errors=true)
        WHERE Geo_Cd IS NOT NULL
    """)
    n = con.execute("SELECT COUNT(*) FROM _fact").fetchone()[0]
    print(f"  opioid_prescribing: {n:,} rows")
    if dry_run:
        return n
    return _write_parquet(con, "_fact", "opioid_prescribing")


def build_otp_providers(con, dry_run: bool) -> int:
    """Opioid Treatment Program provider directory."""
    csv = RAW / "opioid_treatment_program_providers.csv"
    if not csv.exists():
        print("  SKIP: opioid_treatment_program_providers.csv not found")
        return 0
    r = con.execute(f"SELECT * FROM read_csv_auto('{csv}', header=true) LIMIT 0")
    cols = [d[0] for d in r.description]
    print(f"  otp_providers columns: {cols}")
    con.execute(f"""
        CREATE OR REPLACE TABLE _fact AS
        SELECT *,
            '{SNAP}' AS snapshot_date
        FROM read_csv_auto('{csv}', header=true, ignore_errors=true)
    """)
    n = con.execute("SELECT COUNT(*) FROM _fact").fetchone()[0]
    print(f"  otp_providers: {n:,} rows")
    if dry_run:
        return n
    return _write_parquet(con, "_fact", "otp_provider")


def build_cms64_ffcra(con, dry_run: bool) -> int:
    """CMS-64 FFCRA enhanced FMAP expenditures all quarters."""
    csv = RAW / "cms64_ffcra_all_quarters.csv"
    if not csv.exists():
        print("  SKIP: cms64_ffcra_all_quarters.csv not found")
        return 0
    con.execute(f"""
        CREATE OR REPLACE TABLE _fact AS
        SELECT *,
            '{SNAP}' AS snapshot_date
        FROM read_csv_auto('{csv}', header=true, ignore_errors=true, all_varchar=true)
    """)
    n = con.execute("SELECT COUNT(*) FROM _fact").fetchone()[0]
    print(f"  cms64_ffcra: {n:,} rows")
    if dry_run:
        return n
    return _write_parquet(con, "_fact", "cms64_ffcra")


def build_contraceptive_care(con, dry_run: bool) -> int:
    """Contraceptive care utilization among Medicaid beneficiaries."""
    csv = RAW / "contraceptive_care_medicaid.csv"
    if not csv.exists():
        print("  SKIP: contraceptive_care_medicaid.csv not found")
        return 0
    r = con.execute(f"SELECT * FROM read_csv_auto('{csv}', header=true) LIMIT 0")
    cols = [d[0] for d in r.description]
    print(f"  contraceptive_care columns: {cols}")
    con.execute(f"""
        CREATE OR REPLACE TABLE _fact AS
        SELECT *,
            '{SNAP}' AS snapshot_date
        FROM read_csv_auto('{csv}', header=true, ignore_errors=true, all_varchar=true)
    """)
    n = con.execute("SELECT COUNT(*) FROM _fact").fetchone()[0]
    print(f"  contraceptive_care: {n:,} rows")
    if dry_run:
        return n
    return _write_parquet(con, "_fact", "contraceptive_care")


def build_respiratory_conditions(con, dry_run: bool) -> int:
    """Respiratory conditions among Medicaid beneficiaries."""
    csv = RAW / "respiratory_conditions_medicaid.csv"
    if not csv.exists():
        print("  SKIP: respiratory_conditions_medicaid.csv not found")
        return 0
    r = con.execute(f"SELECT * FROM read_csv_auto('{csv}', header=true) LIMIT 0")
    cols = [d[0] for d in r.description]
    print(f"  respiratory_conditions columns: {cols}")
    con.execute(f"""
        CREATE OR REPLACE TABLE _fact AS
        SELECT *,
            '{SNAP}' AS snapshot_date
        FROM read_csv_auto('{csv}', header=true, ignore_errors=true, all_varchar=true)
    """)
    n = con.execute("SELECT COUNT(*) FROM _fact").fetchone()[0]
    print(f"  respiratory_conditions: {n:,} rows")
    if dry_run:
        return n
    return _write_parquet(con, "_fact", "respiratory_conditions")


def build_esrd_qip_tps(con, dry_run: bool) -> int:
    """ESRD QIP Total Performance Scores by facility."""
    csv = RAW / "esrd_qip_tps.csv"
    if not csv.exists():
        print("  SKIP: esrd_qip_tps.csv not found")
        return 0
    r = con.execute(f"SELECT * FROM read_csv_auto('{csv}', header=true) LIMIT 0")
    cols = [d[0] for d in r.description]
    print(f"  esrd_qip_tps columns: {cols}")
    con.execute(f"""
        CREATE OR REPLACE TABLE _fact AS
        SELECT *,
            '{SNAP}' AS snapshot_date
        FROM read_csv_auto('{csv}', header=true, ignore_errors=true, all_varchar=true)
    """)
    n = con.execute("SELECT COUNT(*) FROM _fact").fetchone()[0]
    print(f"  esrd_qip_tps: {n:,} rows")
    if dry_run:
        return n
    return _write_parquet(con, "_fact", "esrd_qip_tps")


def build_program_monthly(con, dry_run: bool) -> int:
    """Medicaid/CHIP enrollment by program type per month."""
    csv = RAW / "program_info_by_month.csv"
    if not csv.exists():
        print("  SKIP: program_info_by_month.csv not found")
        return 0
    r = con.execute(f"SELECT * FROM read_csv_auto('{csv}', header=true) LIMIT 0")
    cols = [d[0] for d in r.description]
    print(f"  program_monthly columns: {cols}")
    con.execute(f"""
        CREATE OR REPLACE TABLE _fact AS
        SELECT *,
            '{SNAP}' AS snapshot_date
        FROM read_csv_auto('{csv}', header=true, ignore_errors=true, all_varchar=true)
    """)
    n = con.execute("SELECT COUNT(*) FROM _fact").fetchone()[0]
    print(f"  program_monthly: {n:,} rows")
    if dry_run:
        return n
    return _write_parquet(con, "_fact", "program_monthly")


def build_mc_annual(con, dry_run: bool) -> int:
    """Managed care participation — annual."""
    csv = RAW / "managed_care_info_by_year.csv"
    if not csv.exists():
        print("  SKIP: managed_care_info_by_year.csv not found")
        return 0
    con.execute(f"""
        CREATE OR REPLACE TABLE _fact AS
        SELECT *,
            '{SNAP}' AS snapshot_date
        FROM read_csv_auto('{csv}', header=true, ignore_errors=true, all_varchar=true)
    """)
    n = con.execute("SELECT COUNT(*) FROM _fact").fetchone()[0]
    print(f"  mc_annual: {n:,} rows")
    if dry_run:
        return n
    return _write_parquet(con, "_fact", "mc_annual")


def build_mc_info_monthly(con, dry_run: bool) -> int:
    """Managed care participation — monthly."""
    csv = RAW / "managed_care_info_by_month.csv"
    if not csv.exists():
        print("  SKIP: managed_care_info_by_month.csv not found")
        return 0
    con.execute(f"""
        CREATE OR REPLACE TABLE _fact AS
        SELECT *,
            '{SNAP}' AS snapshot_date
        FROM read_csv_auto('{csv}', header=true, ignore_errors=true, all_varchar=true)
    """)
    n = con.execute("SELECT COUNT(*) FROM _fact").fetchone()[0]
    print(f"  mc_info_monthly: {n:,} rows")
    if dry_run:
        return n
    return _write_parquet(con, "_fact", "mc_info_monthly")


def build_chip_monthly(con, dry_run: bool) -> int:
    """CHIP enrollment by month."""
    csv = RAW / "chip_enrollment_by_month.csv"
    if not csv.exists():
        print("  SKIP: chip_enrollment_by_month.csv not found")
        return 0
    con.execute(f"""
        CREATE OR REPLACE TABLE _fact AS
        SELECT *,
            '{SNAP}' AS snapshot_date
        FROM read_csv_auto('{csv}', header=true, ignore_errors=true, all_varchar=true)
    """)
    n = con.execute("SELECT COUNT(*) FROM _fact").fetchone()[0]
    print(f"  chip_monthly: {n:,} rows")
    if dry_run:
        return n
    return _write_parquet(con, "_fact", "chip_monthly")


def build_chip_app_elig(con, dry_run: bool) -> int:
    """CHIP applications, eligibility, and enrollment."""
    csv = RAW / "chip_app_eligibility_enrollment.csv"
    if not csv.exists():
        print("  SKIP: chip_app_eligibility_enrollment.csv not found")
        return 0
    con.execute(f"""
        CREATE OR REPLACE TABLE _fact AS
        SELECT *,
            '{SNAP}' AS snapshot_date
        FROM read_csv_auto('{csv}', header=true, ignore_errors=true, all_varchar=true)
    """)
    n = con.execute("SELECT COUNT(*) FROM _fact").fetchone()[0]
    print(f"  chip_app_elig: {n:,} rows")
    if dry_run:
        return n
    return _write_parquet(con, "_fact", "chip_app_elig")


def build_performance_indicator(con, dry_run: bool) -> int:
    """Medicaid/CHIP Performance Indicator dataset (applications/determinations)."""
    csv = RAW / "pi_dataset_feb2026.csv"
    if not csv.exists():
        print("  SKIP: pi_dataset_feb2026.csv not found")
        return 0
    con.execute(f"""
        CREATE OR REPLACE TABLE _fact AS
        SELECT *,
            '{SNAP}' AS snapshot_date
        FROM read_csv_auto('{csv}', header=true, ignore_errors=true, all_varchar=true)
    """)
    n = con.execute("SELECT COUNT(*) FROM _fact").fetchone()[0]
    print(f"  performance_indicator: {n:,} rows")
    if dry_run:
        return n
    return _write_parquet(con, "_fact", "performance_indicator")


def build_new_adult_enrollment(con, dry_run: bool) -> int:
    """Medicaid new adult (expansion) enrollment by state/month."""
    csv = RAW / "medicaid_enrollment_new_adult.csv"
    if not csv.exists():
        print("  SKIP: medicaid_enrollment_new_adult.csv not found")
        return 0
    con.execute(f"""
        CREATE OR REPLACE TABLE _fact AS
        SELECT *,
            '{SNAP}' AS snapshot_date
        FROM read_csv_auto('{csv}', header=true, ignore_errors=true, all_varchar=true)
    """)
    n = con.execute("SELECT COUNT(*) FROM _fact").fetchone()[0]
    print(f"  new_adult_enrollment: {n:,} rows")
    if dry_run:
        return n
    return _write_parquet(con, "_fact", "new_adult_enrollment")


def build_drug_rebate(con, dry_run: bool) -> int:
    """Medicaid Drug Rebate Program product listing (Q4 2025)."""
    csv = RAW / "drug_rebate_products_2025q4.csv"
    if not csv.exists():
        print("  SKIP: drug_rebate_products_2025q4.csv not found")
        return 0
    con.execute(f"""
        CREATE OR REPLACE TABLE _fact AS
        SELECT *,
            '{SNAP}' AS snapshot_date
        FROM read_csv_auto('{csv}', header=true, ignore_errors=true, all_varchar=true)
    """)
    n = con.execute("SELECT COUNT(*) FROM _fact").fetchone()[0]
    print(f"  drug_rebate: {n:,} rows")
    if dry_run:
        return n
    return _write_parquet(con, "_fact", "drug_rebate_products")


def build_sdud_2024(con, dry_run: bool) -> int:
    """State Drug Utilization Data — 2024 update."""
    csv = RAW / "sdud_2024.csv"
    if not csv.exists():
        print("  SKIP: sdud_2024.csv not found")
        return 0
    con.execute(f"""
        CREATE OR REPLACE TABLE _fact AS
        SELECT
            "Utilization Type" AS utilization_type,
            State AS state,
            NDC AS ndc,
            CAST(Year AS INTEGER) AS year,
            CAST(Quarter AS INTEGER) AS quarter,
            "Product Name" AS product_name,
            TRY_CAST("Units Reimbursed" AS DOUBLE) AS units_reimbursed,
            TRY_CAST("Number of Prescriptions" AS INTEGER) AS num_prescriptions,
            TRY_CAST("Total Amount Reimbursed" AS DOUBLE) AS total_reimbursed,
            TRY_CAST("Medicaid Amount Reimbursed" AS DOUBLE) AS medicaid_reimbursed,
            TRY_CAST("Non Medicaid Amount Reimbursed" AS DOUBLE) AS non_medicaid_reimbursed,
            '{SNAP}' AS snapshot_date
        FROM read_csv_auto('{csv}', header=true, ignore_errors=true)
        WHERE State IS NOT NULL AND NDC IS NOT NULL
    """)
    n = con.execute("SELECT COUNT(*) FROM _fact").fetchone()[0]
    print(f"  sdud_2024: {n:,} rows")
    if dry_run:
        return n
    return _write_parquet(con, "_fact", "sdud_2024")


def main():
    parser = argparse.ArgumentParser(description="Round 9: Background agent downloads")
    parser.add_argument("--dry-run", action="store_true", help="Count rows only")
    parser.add_argument("--skip-large", action="store_true",
                        help="Skip SDUD and drug rebate (7M+ rows)")
    args = parser.parse_args()

    con = duckdb.connect()
    total = 0

    builders = [
        ("Medicare Enrollment", build_medicare_enrollment),
        ("CMS IPPS Impact File", build_cms_impact),
        ("Opioid Prescribing Rates", build_opioid_prescribing),
        ("OTP Provider Directory", build_otp_providers),
        ("CMS-64 FFCRA", build_cms64_ffcra),
        ("Contraceptive Care", build_contraceptive_care),
        ("Respiratory Conditions", build_respiratory_conditions),
        ("ESRD QIP TPS", build_esrd_qip_tps),
        ("Program Enrollment Monthly", build_program_monthly),
        ("MC Participation Annual", build_mc_annual),
        ("MC Participation Monthly", build_mc_info_monthly),
        ("CHIP Monthly", build_chip_monthly),
        ("CHIP App/Eligibility", build_chip_app_elig),
        ("Performance Indicator", build_performance_indicator),
        ("New Adult Enrollment", build_new_adult_enrollment),
    ]

    large_builders = [
        ("Drug Rebate Products", build_drug_rebate),
        ("SDUD 2024", build_sdud_2024),
    ]

    for name, builder in builders:
        print(f"\n{'='*60}\n{name}\n{'='*60}")
        n = builder(con, args.dry_run)
        total += n

    if not args.skip_large:
        for name, builder in large_builders:
            print(f"\n{'='*60}\n{name}\n{'='*60}")
            n = builder(con, args.dry_run)
            total += n
    else:
        print("\n(Skipping large datasets: drug_rebate_products, sdud_2024)")

    # Write manifest
    if not args.dry_run:
        manifest = {
            "script": "build_lake_round9.py",
            "snapshot": SNAP,
            "total_rows": total,
        }
        manifest_path = LAKE / "metadata" / f"manifest_round9_{SNAP}.json"
        manifest_path.parent.mkdir(parents=True, exist_ok=True)
        manifest_path.write_text(json.dumps(manifest, indent=2))
        print(f"\nManifest: {manifest_path}")

    con.close()
    print(f"\n{'='*60}")
    print(f"Round 9 complete: {total:,} total rows")
    if args.dry_run:
        print("(DRY RUN — no files written)")


if __name__ == "__main__":
    main()
