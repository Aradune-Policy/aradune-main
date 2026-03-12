#!/usr/bin/env python3
"""
build_lake_medicare_provider.py — Ingest Medicare Physician & Other Practitioners PUF.

Sources (CMS, CY2023):
  - By Provider: Provider-level aggregate utilization + demographics (1.26M providers)
  - Procedure Summary: Carrier × specialty × locality × code (14.4M rows)

Tables built:
  fact_medicare_provider — Provider-level utilization, charges, payments, beneficiary
                           demographics, and chronic condition prevalence.
  fact_medicare_procedure_summary — Carrier-level procedure utilization and payments
                                    by specialty and locality.

Usage:
  python3 scripts/build_lake_medicare_provider.py
"""

import json
import uuid
from datetime import date, datetime
from pathlib import Path

import duckdb

PROJECT_ROOT = Path(__file__).resolve().parent.parent
RAW_DIR = PROJECT_ROOT / "data" / "raw" / "medicare_physician"
LAKE_DIR = PROJECT_ROOT / "data" / "lake"
FACT_DIR = LAKE_DIR / "fact"
META_DIR = LAKE_DIR / "metadata"

SNAPSHOT_DATE = date.today().isoformat()
RUN_ID = str(uuid.uuid4())


def write_parquet(con: duckdb.DuckDBPyConnection, table: str, path: Path) -> int:
    path.parent.mkdir(parents=True, exist_ok=True)
    con.execute(
        f"COPY {table} TO '{path}' (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 500000)"
    )
    count = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
    size_mb = path.stat().st_size / 1_048_576
    print(f"  -> {path.relative_to(PROJECT_ROOT)} ({count:,} rows, {size_mb:.1f} MB)")
    return count


def build_provider():
    """Build fact_medicare_provider from by-provider PUF."""
    csv_path = RAW_DIR / "provider_2023.csv"
    if not csv_path.exists():
        raise FileNotFoundError(f"Provider CSV not found: {csv_path}")

    print(f"\nMedicare Provider Utilization — By Provider (CY2023)")
    print(f"  Source: {csv_path.name} ({csv_path.stat().st_size / 1e6:.0f} MB)")

    con = duckdb.connect()

    print("  Loading and transforming...")
    con.execute(f"""
        CREATE TABLE fact_medicare_provider AS
        SELECT
            Rndrng_NPI AS npi,
            Rndrng_Prvdr_Last_Org_Name AS last_name_org,
            Rndrng_Prvdr_First_Name AS first_name,
            Rndrng_Prvdr_Crdntls AS credentials,
            Rndrng_Prvdr_Ent_Cd AS entity_code,
            Rndrng_Prvdr_City AS city,
            Rndrng_Prvdr_State_Abrvtn AS state_code,
            Rndrng_Prvdr_State_FIPS AS state_fips,
            Rndrng_Prvdr_Zip5 AS zip5,
            CAST(Rndrng_Prvdr_RUCA AS INTEGER) AS ruca_code,
            Rndrng_Prvdr_Type AS provider_type,
            CAST(Rndrng_Prvdr_Mdcr_Prtcptg_Ind AS VARCHAR) AS medicare_participating,
            CAST(Tot_HCPCS_Cds AS INTEGER) AS total_hcpcs_codes,
            CAST(Tot_Benes AS INTEGER) AS total_beneficiaries,
            CAST(Tot_Srvcs AS DOUBLE) AS total_services,
            CAST(Tot_Sbmtd_Chrg AS DOUBLE) AS total_submitted_charges,
            CAST(Tot_Mdcr_Alowd_Amt AS DOUBLE) AS total_medicare_allowed,
            CAST(Tot_Mdcr_Pymt_Amt AS DOUBLE) AS total_medicare_payment,
            CAST(Tot_Mdcr_Stdzd_Amt AS DOUBLE) AS total_standardized_payment,
            CAST(Drug_Tot_HCPCS_Cds AS INTEGER) AS drug_hcpcs_codes,
            CAST(Drug_Mdcr_Pymt_Amt AS DOUBLE) AS drug_medicare_payment,
            CAST(Med_Tot_HCPCS_Cds AS INTEGER) AS med_hcpcs_codes,
            CAST(Med_Mdcr_Pymt_Amt AS DOUBLE) AS med_medicare_payment,
            CAST(Bene_Avg_Age AS DOUBLE) AS bene_avg_age,
            CAST(Bene_Age_LT_65_Cnt AS INTEGER) AS bene_age_lt65,
            CAST(Bene_Age_65_74_Cnt AS INTEGER) AS bene_age_65_74,
            CAST(Bene_Age_75_84_Cnt AS INTEGER) AS bene_age_75_84,
            CAST(Bene_Age_GT_84_Cnt AS INTEGER) AS bene_age_gt84,
            CAST(Bene_Feml_Cnt AS INTEGER) AS bene_female,
            CAST(Bene_Male_Cnt AS INTEGER) AS bene_male,
            CAST(Bene_Race_Wht_Cnt AS INTEGER) AS bene_race_white,
            CAST(Bene_Race_Black_Cnt AS INTEGER) AS bene_race_black,
            CAST(Bene_Race_API_Cnt AS INTEGER) AS bene_race_api,
            CAST(Bene_Race_Hspnc_Cnt AS INTEGER) AS bene_race_hispanic,
            CAST(Bene_Dual_Cnt AS INTEGER) AS bene_dual_eligible,
            CAST(Bene_Ndual_Cnt AS INTEGER) AS bene_non_dual,
            CAST(Bene_CC_PH_Diabetes_V2_Pct AS DOUBLE) AS bene_pct_diabetes,
            CAST(Bene_CC_PH_Hypertension_V2_Pct AS DOUBLE) AS bene_pct_hypertension,
            CAST(Bene_CC_PH_HF_NonIHD_V2_Pct AS DOUBLE) AS bene_pct_heart_failure,
            CAST(Bene_CC_PH_CKD_V2_Pct AS DOUBLE) AS bene_pct_ckd,
            CAST(Bene_CC_PH_COPD_V2_Pct AS DOUBLE) AS bene_pct_copd,
            CAST(Bene_CC_BH_Depress_V1_Pct AS DOUBLE) AS bene_pct_depression,
            CAST(Bene_CC_BH_Alz_NonAlzdem_V2_Pct AS DOUBLE) AS bene_pct_dementia,
            CAST(Bene_Avg_Risk_Scre AS DOUBLE) AS bene_avg_risk_score,
            2023 AS data_year,
            'https://data.cms.gov/provider-summary-by-type-of-service/medicare-physician-other-practitioners/medicare-physician-other-practitioners-by-provider' AS source,
            '{SNAPSHOT_DATE}' AS snapshot_date
        FROM read_csv_auto('{csv_path}', ignore_errors=true, sample_size=20000)
        WHERE Rndrng_NPI IS NOT NULL
    """)

    count = con.execute("SELECT COUNT(*) FROM fact_medicare_provider").fetchone()[0]
    states = con.execute("SELECT COUNT(DISTINCT state_code) FROM fact_medicare_provider WHERE state_code IS NOT NULL").fetchone()[0]
    types = con.execute("SELECT COUNT(DISTINCT provider_type) FROM fact_medicare_provider").fetchone()[0]
    total_pymt = con.execute("SELECT ROUND(SUM(total_medicare_payment)/1e9, 2) FROM fact_medicare_provider").fetchone()[0]
    duals = con.execute("SELECT SUM(bene_dual_eligible) FROM fact_medicare_provider WHERE bene_dual_eligible IS NOT NULL").fetchone()[0]

    print(f"  {count:,} providers, {states} states, {types} specialties")
    print(f"  ${total_pymt}B total Medicare payments")
    if duals:
        print(f"  {duals:,} dual-eligible beneficiaries served")

    # Top specialties
    print("\n  Top provider types by count:")
    top = con.execute("""
        SELECT provider_type, COUNT(*) as n, ROUND(SUM(total_medicare_payment)/1e9, 2) as pymt_B
        FROM fact_medicare_provider WHERE provider_type IS NOT NULL
        GROUP BY provider_type ORDER BY n DESC LIMIT 10
    """).fetchall()
    for row in top:
        print(f"    {row[0]}: {row[1]:,} providers, ${row[2]}B")

    out_path = FACT_DIR / "medicare_provider" / f"snapshot={SNAPSHOT_DATE}" / "data.parquet"
    row_count = write_parquet(con, "fact_medicare_provider", out_path)
    con.close()
    return row_count


def build_procedure_summary():
    """Build fact_medicare_procedure_summary from Physician/Supplier Procedure Summary."""
    csv_path = RAW_DIR / "procedure_summary_2024.csv"
    if not csv_path.exists():
        raise FileNotFoundError(f"Procedure Summary CSV not found: {csv_path}")

    print(f"\nMedicare Procedure Summary (CY2024)")
    print(f"  Source: {csv_path.name} ({csv_path.stat().st_size / 1e6:.0f} MB)")

    con = duckdb.connect()

    print("  Loading with ignore_errors (some rows have CSV issues)...")
    con.execute(f"""
        CREATE TABLE fact_medicare_procedure_summary AS
        SELECT
            HCPCS_CD AS hcpcs_code,
            HCPCS_INITIAL_MODIFIER_CD AS modifier,
            PROVIDER_SPEC_CD AS specialty_code,
            CARRIER_NUM AS carrier_number,
            PRICING_LOCALITY_CD AS pricing_locality,
            TYPE_OF_SERVICE_CD AS type_of_service,
            PLACE_OF_SERVICE_CD AS place_of_service,
            HCPCS_SECOND_MODIFIER_CD AS modifier_2,
            TRY_CAST(PSPS_SUBMITTED_SERVICE_CNT AS BIGINT) AS submitted_services,
            TRY_CAST(PSPS_SUBMITTED_CHARGE_AMT AS DOUBLE) AS submitted_charges,
            TRY_CAST(PSPS_ALLOWED_CHARGE_AMT AS DOUBLE) AS allowed_charges,
            TRY_CAST(PSPS_DENIED_SERVICES_CNT AS BIGINT) AS denied_services,
            TRY_CAST(PSPS_DENIED_CHARGE_AMT AS DOUBLE) AS denied_charges,
            TRY_CAST(PSPS_ASSIGNED_SERVICES_CNT AS BIGINT) AS assigned_services,
            TRY_CAST(PSPS_NCH_PAYMENT_AMT AS DOUBLE) AS nch_payment,
            PSPS_HCPCS_ASC_IND_CD AS asc_indicator,
            PSPS_ERROR_IND_CD AS error_indicator,
            HCPCS_BETOS_CD AS betos_code,
            2024 AS data_year,
            'https://data.cms.gov/provider-summary-by-type-of-service/medicare-physician-other-practitioners/physician-supplier-procedure-summary' AS source,
            '{SNAPSHOT_DATE}' AS snapshot_date
        FROM read_csv_auto('{csv_path}', all_varchar=true, ignore_errors=true, sample_size=20000)
        WHERE HCPCS_CD IS NOT NULL
    """)

    count = con.execute("SELECT COUNT(*) FROM fact_medicare_procedure_summary").fetchone()[0]
    codes = con.execute("SELECT COUNT(DISTINCT hcpcs_code) FROM fact_medicare_procedure_summary").fetchone()[0]
    total_pymt = con.execute("SELECT ROUND(SUM(nch_payment)/1e9, 2) FROM fact_medicare_procedure_summary").fetchone()[0]

    print(f"  {count:,} rows, {codes:,} unique HCPCS codes")
    print(f"  ${total_pymt}B total NCH payments")

    out_path = FACT_DIR / "medicare_procedure_summary" / f"snapshot={SNAPSHOT_DATE}" / "data.parquet"
    row_count = write_parquet(con, "fact_medicare_procedure_summary", out_path)
    con.close()
    return row_count


def write_manifest(provider_rows: int, procedure_rows: int):
    META_DIR.mkdir(parents=True, exist_ok=True)
    manifest = {
        "run_id": RUN_ID,
        "snapshot_date": SNAPSHOT_DATE,
        "script": "build_lake_medicare_provider.py",
        "tables": {
            "fact_medicare_provider": {
                "rows": provider_rows,
                "path": f"fact/medicare_provider/snapshot={SNAPSHOT_DATE}/data.parquet",
                "source": "Medicare Physician & Other Practitioners - by Provider (CY2023)",
            },
            "fact_medicare_procedure_summary": {
                "rows": procedure_rows,
                "path": f"fact/medicare_procedure_summary/snapshot={SNAPSHOT_DATE}/data.parquet",
                "source": "Physician/Supplier Procedure Summary (CY2024)",
            },
        },
        "completed_at": datetime.now().isoformat() + "Z",
    }
    manifest_path = META_DIR / f"manifest_medicare_provider_{SNAPSHOT_DATE}.json"
    manifest_path.write_text(json.dumps(manifest, indent=2))
    print(f"\n  Manifest: {manifest_path}")


def main():
    print("=" * 60)
    print("Medicare Physician & Other Practitioners PUF Ingestion")
    print(f"  Snapshot: {SNAPSHOT_DATE}")
    print(f"  Run ID:   {RUN_ID}")

    provider_rows = build_provider()
    procedure_rows = build_procedure_summary()
    write_manifest(provider_rows, procedure_rows)

    print("\n" + "=" * 60)
    print("MEDICARE PROVIDER INGESTION COMPLETE")
    print(f"  fact_medicare_provider:           {provider_rows:,} rows")
    print(f"  fact_medicare_procedure_summary:  {procedure_rows:,} rows")
    print(f"  TOTAL:                            {provider_rows + procedure_rows:,} rows")
    print("=" * 60)


if __name__ == "__main__":
    main()
