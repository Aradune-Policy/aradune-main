#!/usr/bin/env python3
"""
build_lake_medicare_puf.py — Download and ingest Medicare Provider Utilization PUFs.

Datasets:
  1. Medicare Part D Prescribers - by Provider (DY23)
     NPI-level prescribing aggregates: total claims, drug cost, beneficiary demographics,
     opioid/antibiotic/antipsychotic flags. ~1.2M rows.
     Table: fact_part_d_prescriber_provider

  2. Medicare Outpatient Hospitals - by Provider and Service (DY23)
     Hospital (CCN) x APC: beneficiary counts, services, charges, payments.
     Replaces truncated existing data (only had 5 states).
     Table: fact_medicare_outpatient_by_provider

Usage:
  python3 scripts/build_lake_medicare_puf.py
  python3 scripts/build_lake_medicare_puf.py --only part_d
  python3 scripts/build_lake_medicare_puf.py --only outpatient
  python3 scripts/build_lake_medicare_puf.py --dry-run
"""

import argparse
import json
import subprocess
import sys
import uuid
from datetime import date
from pathlib import Path

import duckdb

PROJECT_ROOT = Path(__file__).resolve().parent.parent
RAW_DIR = PROJECT_ROOT / "data" / "raw"
LAKE_DIR = PROJECT_ROOT / "data" / "lake"
FACT_DIR = LAKE_DIR / "fact"
META_DIR = LAKE_DIR / "metadata"

SNAPSHOT_DATE = date.today().isoformat()
RUN_ID = str(uuid.uuid4())

# Direct CSV download URLs from CMS data.json catalog
URLS = {
    "part_d_prescriber_provider": (
        "https://data.cms.gov/sites/default/files/2025-04/"
        "750769a3-bb0f-4f05-81dc-7dcb6e105cb0/MUP_DPR_RY25_P04_V10_DY23_NPI.csv"
    ),
    "outpatient_by_provider": (
        "https://data.cms.gov/sites/default/files/2025-08/"
        "bceaa5e1-e58c-4109-9f05-832fc5e6bbc8/MUP_OUT_RY25_P04_V10_DY23_Prov_Svc.csv"
    ),
}

results = {}


def write_parquet(con: duckdb.DuckDBPyConnection, table: str, path: Path) -> int:
    """Write a DuckDB table to ZSTD Parquet. Returns row count."""
    path.parent.mkdir(parents=True, exist_ok=True)
    con.execute(
        f"COPY {table} TO '{path}' (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 500000)"
    )
    count = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
    size_mb = path.stat().st_size / 1_048_576
    print(f"  -> {path.relative_to(PROJECT_ROOT)} ({count:,} rows, {size_mb:.1f} MB)")
    return count


def download_csv(name: str, url: str) -> Path:
    """Download a CSV file using curl (more reliable than urllib on macOS)."""
    out_path = RAW_DIR / f"medicare_puf_{name}.csv"
    if out_path.exists():
        size_mb = out_path.stat().st_size / 1_048_576
        print(f"  CSV already exists: {out_path.name} ({size_mb:.1f} MB), reusing")
        return out_path

    print(f"  Downloading {name} CSV...")
    result = subprocess.run(
        ["curl", "-L", "-s", "-o", str(out_path), url],
        capture_output=True,
        text=True,
        timeout=600,
    )
    if result.returncode != 0:
        print(f"  ERROR: curl failed: {result.stderr}")
        sys.exit(1)

    size_mb = out_path.stat().st_size / 1_048_576
    print(f"  Downloaded: {out_path.name} ({size_mb:.1f} MB)")
    return out_path


# ── 1. Part D Prescriber by Provider ─────────────────────────────────
def build_part_d_prescriber_provider(dry_run: bool) -> int:
    """
    Medicare Part D Prescribers - by Provider (DY23).
    NPI-level prescribing aggregates with demographics and drug class flags.
    ~1.2M rows, provider-level (not NPI x drug).
    """
    print("\n=== Medicare Part D Prescribers - by Provider (DY2023) ===")

    csv_path = download_csv("part_d_prescriber_provider", URLS["part_d_prescriber_provider"])

    con = duckdb.connect()
    con.execute(f"""
        CREATE TABLE fact_part_d_prescriber_provider AS
        SELECT
            TRIM(PRSCRBR_NPI) AS npi,
            TRIM(Prscrbr_Last_Org_Name) AS provider_last_org_name,
            TRIM(Prscrbr_First_Name) AS provider_first_name,
            TRIM(Prscrbr_Crdntls) AS credentials,
            TRIM(Prscrbr_Ent_Cd) AS entity_code,
            TRIM(Prscrbr_City) AS city,
            TRIM(Prscrbr_State_Abrvtn) AS state_code,
            TRIM(Prscrbr_State_FIPS) AS state_fips,
            TRIM(Prscrbr_zip5) AS zip5,
            TRIM(Prscrbr_RUCA) AS ruca_code,
            TRIM(Prscrbr_RUCA_Desc) AS ruca_description,
            TRIM(Prscrbr_Type) AS provider_type,
            TRIM(Prscrbr_Type_src) AS provider_type_source,
            -- Aggregate prescribing
            TRY_CAST(Tot_Clms AS BIGINT) AS total_claims,
            TRY_CAST(Tot_30day_Fills AS DOUBLE) AS total_30day_fills,
            TRY_CAST(Tot_Drug_Cst AS DOUBLE) AS total_drug_cost,
            TRY_CAST(Tot_Day_Suply AS BIGINT) AS total_day_supply,
            TRY_CAST(Tot_Benes AS BIGINT) AS total_beneficiaries,
            -- Age 65+ subset
            TRIM(GE65_Sprsn_Flag) AS ge65_suppression_flag,
            TRY_CAST(GE65_Tot_Clms AS BIGINT) AS ge65_total_claims,
            TRY_CAST(GE65_Tot_30day_Fills AS DOUBLE) AS ge65_total_30day_fills,
            TRY_CAST(GE65_Tot_Drug_Cst AS DOUBLE) AS ge65_total_drug_cost,
            TRY_CAST(GE65_Tot_Day_Suply AS BIGINT) AS ge65_total_day_supply,
            TRY_CAST(GE65_Tot_Benes AS BIGINT) AS ge65_total_beneficiaries,
            -- Brand/Generic/Other splits
            TRY_CAST(Brnd_Tot_Clms AS BIGINT) AS brand_total_claims,
            TRY_CAST(Brnd_Tot_Drug_Cst AS DOUBLE) AS brand_total_drug_cost,
            TRY_CAST(Gnrc_Tot_Clms AS BIGINT) AS generic_total_claims,
            TRY_CAST(Gnrc_Tot_Drug_Cst AS DOUBLE) AS generic_total_drug_cost,
            TRY_CAST(Othr_Tot_Clms AS BIGINT) AS other_total_claims,
            TRY_CAST(Othr_Tot_Drug_Cst AS DOUBLE) AS other_total_drug_cost,
            -- MAPD / PDP splits
            TRY_CAST(MAPD_Tot_Clms AS BIGINT) AS mapd_total_claims,
            TRY_CAST(MAPD_Tot_Drug_Cst AS DOUBLE) AS mapd_total_drug_cost,
            TRY_CAST(PDP_Tot_Clms AS BIGINT) AS pdp_total_claims,
            TRY_CAST(PDP_Tot_Drug_Cst AS DOUBLE) AS pdp_total_drug_cost,
            -- LIS / Non-LIS splits
            TRY_CAST(LIS_Tot_Clms AS BIGINT) AS lis_total_claims,
            TRY_CAST(LIS_Drug_Cst AS DOUBLE) AS lis_drug_cost,
            TRY_CAST(NonLIS_Tot_Clms AS BIGINT) AS nonlis_total_claims,
            TRY_CAST(NonLIS_Drug_Cst AS DOUBLE) AS nonlis_drug_cost,
            -- Opioid prescribing
            TRY_CAST(Opioid_Tot_Clms AS BIGINT) AS opioid_total_claims,
            TRY_CAST(Opioid_Tot_Drug_Cst AS DOUBLE) AS opioid_total_drug_cost,
            TRY_CAST(Opioid_Tot_Suply AS BIGINT) AS opioid_total_supply,
            TRY_CAST(Opioid_Tot_Benes AS BIGINT) AS opioid_total_beneficiaries,
            TRY_CAST(Opioid_Prscrbr_Rate AS DOUBLE) AS opioid_prescribing_rate,
            -- Long-acting opioids
            TRY_CAST(Opioid_LA_Tot_Clms AS BIGINT) AS opioid_la_total_claims,
            TRY_CAST(Opioid_LA_Tot_Drug_Cst AS DOUBLE) AS opioid_la_total_drug_cost,
            TRY_CAST(Opioid_LA_Tot_Suply AS BIGINT) AS opioid_la_total_supply,
            TRY_CAST(Opioid_LA_Tot_Benes AS BIGINT) AS opioid_la_total_beneficiaries,
            TRY_CAST(Opioid_LA_Prscrbr_Rate AS DOUBLE) AS opioid_la_prescribing_rate,
            -- Antibiotic
            TRY_CAST(Antbtc_Tot_Clms AS BIGINT) AS antibiotic_total_claims,
            TRY_CAST(Antbtc_Tot_Drug_Cst AS DOUBLE) AS antibiotic_total_drug_cost,
            TRY_CAST(Antbtc_Tot_Benes AS BIGINT) AS antibiotic_total_beneficiaries,
            -- Antipsychotic (65+)
            TRIM(Antpsyct_GE65_Sprsn_Flag) AS antipsychotic_ge65_suppression_flag,
            TRY_CAST(Antpsyct_GE65_Tot_Clms AS BIGINT) AS antipsychotic_ge65_total_claims,
            TRY_CAST(Antpsyct_GE65_Tot_Drug_Cst AS DOUBLE) AS antipsychotic_ge65_total_drug_cost,
            TRY_CAST(Antpsyct_GE65_Tot_Benes AS BIGINT) AS antipsychotic_ge65_total_beneficiaries,
            -- Beneficiary demographics
            TRY_CAST(Bene_Avg_Age AS DOUBLE) AS bene_avg_age,
            TRY_CAST(Bene_Age_LT_65_Cnt AS BIGINT) AS bene_age_lt65,
            TRY_CAST(Bene_Age_65_74_Cnt AS BIGINT) AS bene_age_65_74,
            TRY_CAST(Bene_Age_75_84_Cnt AS BIGINT) AS bene_age_75_84,
            TRY_CAST(Bene_Age_GT_84_Cnt AS BIGINT) AS bene_age_gt84,
            TRY_CAST(Bene_Feml_Cnt AS BIGINT) AS bene_female,
            TRY_CAST(Bene_Male_Cnt AS BIGINT) AS bene_male,
            TRY_CAST(Bene_Race_Wht_Cnt AS BIGINT) AS bene_race_white,
            TRY_CAST(Bene_Race_Black_Cnt AS BIGINT) AS bene_race_black,
            TRY_CAST(Bene_Race_Api_Cnt AS BIGINT) AS bene_race_api,
            TRY_CAST(Bene_Race_Hspnc_Cnt AS BIGINT) AS bene_race_hispanic,
            TRY_CAST(Bene_Race_Natind_Cnt AS BIGINT) AS bene_race_native,
            TRY_CAST(Bene_Race_Othr_Cnt AS BIGINT) AS bene_race_other,
            TRY_CAST(Bene_Dual_Cnt AS BIGINT) AS bene_dual_eligible,
            TRY_CAST(Bene_Ndual_Cnt AS BIGINT) AS bene_non_dual,
            TRY_CAST(Bene_Avg_Risk_Scre AS DOUBLE) AS bene_avg_risk_score,
            -- Metadata
            2023 AS data_year,
            'https://data.cms.gov/provider-summary-by-type-of-service/medicare-part-d-prescribers/medicare-part-d-prescribers-by-provider' AS source,
            '{SNAPSHOT_DATE}' AS snapshot_date
        FROM read_csv_auto('{csv_path}', all_varchar=true, ignore_errors=true)
        WHERE PRSCRBR_NPI IS NOT NULL
            AND TRIM(PRSCRBR_NPI) != ''
    """)

    count = con.execute("SELECT COUNT(*) FROM fact_part_d_prescriber_provider").fetchone()[0]
    states = con.execute(
        "SELECT COUNT(DISTINCT state_code) FROM fact_part_d_prescriber_provider"
    ).fetchone()[0]
    top_types = con.execute("""
        SELECT provider_type, COUNT(*) as cnt
        FROM fact_part_d_prescriber_provider
        GROUP BY provider_type ORDER BY cnt DESC LIMIT 5
    """).fetchall()
    total_cost = con.execute(
        "SELECT SUM(total_drug_cost) FROM fact_part_d_prescriber_provider"
    ).fetchone()[0]

    print(f"  {count:,} rows, {states} states")
    print(f"  Total drug cost: ${total_cost:,.0f}")
    print(f"  Top provider types: {[(t, c) for t, c in top_types]}")

    if dry_run:
        print("  [dry-run] Would write to lake")
        con.close()
        return count

    out_path = (
        FACT_DIR
        / "part_d_prescriber_provider"
        / f"snapshot={SNAPSHOT_DATE}"
        / "data.parquet"
    )
    row_count = write_parquet(con, "fact_part_d_prescriber_provider", out_path)
    con.close()
    return row_count


# ── 2. Outpatient by Provider and Service ─────────────────────────────
def build_outpatient_by_provider(dry_run: bool) -> int:
    """
    Medicare Outpatient Hospitals - by Provider and Service (DY23).
    Hospital (CCN) x APC: services, charges, payments.
    Replaces the truncated existing data that only had 5 states.
    """
    print("\n=== Medicare Outpatient Hospitals - by Provider and Service (DY2023) ===")

    csv_path = download_csv("outpatient_by_provider", URLS["outpatient_by_provider"])

    con = duckdb.connect()
    con.execute(f"""
        CREATE TABLE fact_medicare_outpatient_by_provider AS
        SELECT
            TRIM(Rndrng_Prvdr_CCN) AS ccn,
            TRIM(Rndrng_Prvdr_Org_Name) AS hospital_name,
            TRIM(Rndrng_Prvdr_City) AS city,
            TRIM(Rndrng_Prvdr_State_Abrvtn) AS state_code,
            TRIM(Rndrng_Prvdr_State_FIPS) AS state_fips,
            TRIM(Rndrng_Prvdr_Zip5) AS zip5,
            TRIM(Rndrng_Prvdr_RUCA) AS ruca_code,
            TRIM(Rndrng_Prvdr_RUCA_Desc) AS ruca_description,
            TRIM(APC_Cd) AS apc_code,
            TRIM(APC_Desc) AS apc_description,
            TRY_CAST(Bene_Cnt AS BIGINT) AS beneficiary_count,
            TRY_CAST(CAPC_Srvcs AS BIGINT) AS capc_services,
            TRY_CAST(Avg_Tot_Sbmtd_Chrgs AS DOUBLE) AS avg_submitted_charges,
            TRY_CAST(Avg_Mdcr_Alowd_Amt AS DOUBLE) AS avg_medicare_allowed,
            TRY_CAST(Avg_Mdcr_Pymt_Amt AS DOUBLE) AS avg_medicare_payment,
            TRY_CAST(Outlier_Srvcs AS BIGINT) AS outlier_services,
            TRY_CAST(Avg_Mdcr_Outlier_Amt AS DOUBLE) AS avg_medicare_outlier_amount,
            -- Metadata
            2023 AS data_year,
            'https://data.cms.gov/provider-summary-by-type-of-service/medicare-outpatient-hospitals/medicare-outpatient-hospitals-by-provider-and-service' AS source,
            '{SNAPSHOT_DATE}' AS snapshot_date
        FROM read_csv_auto('{csv_path}', all_varchar=true, ignore_errors=true)
        WHERE Rndrng_Prvdr_CCN IS NOT NULL
            AND TRIM(Rndrng_Prvdr_CCN) != ''
    """)

    count = con.execute("SELECT COUNT(*) FROM fact_medicare_outpatient_by_provider").fetchone()[0]
    hospitals = con.execute(
        "SELECT COUNT(DISTINCT ccn) FROM fact_medicare_outpatient_by_provider"
    ).fetchone()[0]
    states = con.execute(
        "SELECT COUNT(DISTINCT state_code) FROM fact_medicare_outpatient_by_provider"
    ).fetchone()[0]
    apcs = con.execute(
        "SELECT COUNT(DISTINCT apc_code) FROM fact_medicare_outpatient_by_provider"
    ).fetchone()[0]

    print(f"  {count:,} rows, {hospitals:,} hospitals, {states} states, {apcs} APCs")

    if dry_run:
        print("  [dry-run] Would write to lake")
        con.close()
        return count

    out_path = (
        FACT_DIR
        / "medicare_outpatient_by_provider"
        / f"snapshot={SNAPSHOT_DATE}"
        / "data.parquet"
    )
    row_count = write_parquet(con, "fact_medicare_outpatient_by_provider", out_path)
    con.close()
    return row_count


# ── Main ──────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="Ingest Medicare Provider Utilization PUFs")
    parser.add_argument("--dry-run", action="store_true", help="Parse and count but don't write")
    parser.add_argument(
        "--only",
        type=str,
        default="",
        help="Comma-separated list of datasets: part_d, outpatient",
    )
    args = parser.parse_args()

    targets = [t.strip() for t in args.only.split(",") if t.strip()] if args.only else []

    print(f"Medicare PUF Ingestion — {SNAPSHOT_DATE}")
    print(f"Run ID: {RUN_ID}")

    if not targets or "part_d" in targets:
        results["part_d_prescriber_provider"] = build_part_d_prescriber_provider(args.dry_run)

    if not targets or "outpatient" in targets:
        results["medicare_outpatient_by_provider"] = build_outpatient_by_provider(args.dry_run)

    # Write manifest
    print("\n=== Summary ===")
    total = 0
    for name, count in results.items():
        print(f"  {name}: {count:,} rows")
        total += count
    print(f"  TOTAL: {total:,} rows")

    if not args.dry_run:
        manifest = {
            "run_id": RUN_ID,
            "snapshot_date": SNAPSHOT_DATE,
            "script": "build_lake_medicare_puf.py",
            "tables": {
                name: {"rows": count, "path": f"fact/{name}/snapshot={SNAPSHOT_DATE}/data.parquet"}
                for name, count in results.items()
            },
            "total_rows": total,
        }
        manifest_path = META_DIR / f"manifest_medicare_puf_{SNAPSHOT_DATE}.json"
        manifest_path.parent.mkdir(parents=True, exist_ok=True)
        with open(manifest_path, "w") as f:
            json.dump(manifest, f, indent=2)
        print(f"\n  Manifest: {manifest_path.relative_to(PROJECT_ROOT)}")


if __name__ == "__main__":
    main()
