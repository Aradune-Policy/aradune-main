#!/usr/bin/env python3
"""
build_lake_gme_blockgrant.py — Ingest GME/IME and block grant data into the lake.

Sources:
  1. CMS Provider Specific File (PSF) — GME/IME/DSH operational data (68K providers)
  2. SAMHSA MHBG FY23 block grant allotments (scraped from HTML)
  3. MDS Quality Measures (250K nursing home QM rows)
  4. NH Provider Info (14.7K nursing homes with ratings/staffing)

Tables built:
  fact_provider_specific    — CMS PSF with GME/IME/DSH columns per provider
  fact_block_grant          — SAMHSA mental health block grant allotments
  fact_mds_quality          — MDS nursing home quality measures (already in BH script, skip if exists)
  fact_nh_provider_info     — NH provider info with ratings (already in BH script, skip if exists)

Usage:
  python3 scripts/build_lake_gme_blockgrant.py
  python3 scripts/build_lake_gme_blockgrant.py --dry-run
"""

import argparse
import json
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

STATE_NAME_TO_CODE = {
    "Alabama": "AL", "Alaska": "AK", "Arizona": "AZ", "Arkansas": "AR",
    "California": "CA", "Colorado": "CO", "Connecticut": "CT", "Delaware": "DE",
    "District of Columbia": "DC", "Florida": "FL", "Georgia": "GA", "Hawaii": "HI",
    "Idaho": "ID", "Illinois": "IL", "Indiana": "IN", "Iowa": "IA",
    "Kansas": "KS", "Kentucky": "KY", "Louisiana": "LA", "Maine": "ME",
    "Maryland": "MD", "Massachusetts": "MA", "Michigan": "MI", "Minnesota": "MN",
    "Mississippi": "MS", "Missouri": "MO", "Montana": "MT", "Nebraska": "NE",
    "Nevada": "NV", "New Hampshire": "NH", "New Jersey": "NJ", "New Mexico": "NM",
    "New York": "NY", "North Carolina": "NC", "North Dakota": "ND", "Ohio": "OH",
    "Oklahoma": "OK", "Oregon": "OR", "Pennsylvania": "PA", "Rhode Island": "RI",
    "South Carolina": "SC", "South Dakota": "SD", "Tennessee": "TN", "Texas": "TX",
    "Utah": "UT", "Vermont": "VT", "Virginia": "VA", "Washington": "WA",
    "West Virginia": "WV", "Wisconsin": "WI", "Wyoming": "WY",
    "Puerto Rico": "PR", "Virgin Islands": "VI", "Guam": "GU",
    "American Samoa": "AS", "Northern Mariana Islands": "MP",
    "Republic of Palau": "PW", "Marshall Islands": "MH",
    "Federated States of Micronesia": "FM",
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


def build_provider_specific(con, dry_run: bool) -> int:
    """Build fact_provider_specific from CMS Provider Specific File."""
    print("Building fact_provider_specific...")
    csv_path = RAW_DIR / "cms_provider_specific_file.csv"
    if not csv_path.exists():
        print("  SKIPPED — cms_provider_specific_file.csv not found")
        return 0

    con.execute(f"""
        CREATE OR REPLACE TABLE _fact_psf AS
        SELECT
            oscarNumber AS provider_ccn,
            nationalProviderIdentifier AS npi,
            state AS state_code,
            TRY_CAST(providerType AS INTEGER) AS provider_type,
            TRY_CAST(bedSize AS INTEGER) AS bed_size,
            TRY_CAST(caseMixIndex AS DOUBLE) AS case_mix_index,
            -- GME / IME
            TRY_CAST(internsToBedsRatio AS DOUBLE) AS interns_to_beds_ratio,
            TRY_CAST(passThroughAmountForDirectMedicalEducation AS DOUBLE) AS dgme_passthrough,
            TRY_CAST(passThroughAmountForDirectGraduateMedicalEducation AS DOUBLE) AS dgme_passthrough_alt,
            TRY_CAST(capitalIndirectMedicalEducationRatio AS DOUBLE) AS capital_ime_ratio,
            -- DSH
            TRY_CAST(operatingDsh AS DOUBLE) AS operating_dsh,
            TRY_CAST(supplementalSecurityIncomeRatio AS DOUBLE) AS ssi_ratio,
            TRY_CAST(medicaidRatio AS DOUBLE) AS medicaid_ratio,
            TRY_CAST(uncompensatedCareAmount AS DOUBLE) AS uncompensated_care_amount,
            -- Value-based programs
            TRY_CAST(valueBasedPurchasingAdjustment AS DOUBLE) AS vbp_adjustment,
            TRY_CAST(hospitalReadmissionsReductionAdjustment AS DOUBLE) AS hrrp_adjustment,
            hospitalAcquiredConditionReductionProgramParticipant AS hac_participant,
            -- Cost/charge
            TRY_CAST(operatingCostToChargeRatio AS DOUBLE) AS operating_ccr,
            TRY_CAST(capitalCostToChargeRatio AS DOUBLE) AS capital_ccr,
            -- Other
            TRY_CAST(lowVolumeAdjustmentFactor AS DOUBLE) AS low_volume_adjustment,
            TRY_CAST(medicarePerformanceAdjustment AS DOUBLE) AS medicare_perf_adjustment,
            effectiveDate AS effective_date,
            fiscalYearBeginningDate AS fy_begin,
            fiscalYearEnd AS fy_end,
            countyCode AS county_code,
            actualGeographicLocation_CBSA AS cbsa,
            'cms_provider_specific_file' AS source,
            DATE '{SNAPSHOT_DATE}' AS snapshot_date
        FROM read_csv_auto('{csv_path}', header=true, all_varchar=true)
        WHERE state IS NOT NULL
          AND LENGTH(state) <= 2
    """)

    count = write_parquet(con, "_fact_psf", _snapshot_path("provider_specific"), dry_run)
    states = con.execute("SELECT COUNT(DISTINCT state_code) FROM _fact_psf").fetchone()[0]

    stats = con.execute("""
        SELECT
            COUNT(*) FILTER (WHERE interns_to_beds_ratio > 0) AS teaching_hospitals,
            ROUND(SUM(dgme_passthrough) / 1e9, 2) AS total_dgme_bn,
            COUNT(*) FILTER (WHERE operating_dsh > 0) AS dsh_hospitals,
            ROUND(SUM(uncompensated_care_amount) / 1e6, 1) AS total_uc_m,
            ROUND(AVG(case_mix_index), 3) AS avg_cmi
        FROM _fact_psf
    """).fetchone()
    print(f"  {count:,} providers across {states} states")
    print(f"  Teaching hospitals (ITB>0): {stats[0]:,} | DGME total: ${stats[1]}B")
    print(f"  DSH hospitals: {stats[2]:,} | UC total: ${stats[3]:.0f}M")
    print(f"  Avg CMI: {stats[4]}")

    con.execute("DROP TABLE IF EXISTS _fact_psf")
    return count


def build_block_grants(con, dry_run: bool) -> int:
    """Build fact_block_grant from scraped SAMHSA allotment data."""
    print("Building fact_block_grant...")
    mhbg_path = RAW_DIR / "samhsa_mhbg_fy23.json"
    if not mhbg_path.exists():
        print("  SKIPPED — samhsa_mhbg_fy23.json not found")
        return 0

    with open(mhbg_path) as f:
        allotments = json.load(f)

    # Map state names to codes
    rows = []
    for a in allotments:
        state_name = a["state"].strip()
        code = STATE_NAME_TO_CODE.get(state_name)
        if not code:
            continue
        rows.append({
            "state_code": code,
            "program": a["program"],
            "fiscal_year": a["fiscal_year"],
            "allotment": a["amount"],
            "source": "samhsa_mhbg_fy23_allotments",
        })

    if not rows:
        print("  No valid rows")
        return 0

    con.execute("""
        CREATE OR REPLACE TABLE _fact_bg (
            state_code VARCHAR,
            program VARCHAR,
            fiscal_year INTEGER,
            allotment BIGINT,
            source VARCHAR,
            snapshot_date DATE
        )
    """)
    for r in rows:
        con.execute("INSERT INTO _fact_bg VALUES (?, ?, ?, ?, ?, ?)",
                     [r["state_code"], r["program"], r["fiscal_year"],
                      r["allotment"], r["source"], SNAPSHOT_DATE])

    count = write_parquet(con, "_fact_bg", _snapshot_path("block_grant"), dry_run)
    total = sum(r["allotment"] for r in rows)
    print(f"  {count} allotments totaling ${total / 1e9:.2f}B")

    # Top 5
    top = con.execute("""
        SELECT state_code, allotment FROM _fact_bg
        ORDER BY allotment DESC LIMIT 5
    """).fetchall()
    for s, a in top:
        print(f"    {s}: ${a:,}")

    con.execute("DROP TABLE IF EXISTS _fact_bg")
    return count


ALL_TABLES = {
    "provider_specific": ("fact_provider_specific", build_provider_specific),
    "block_grant": ("fact_block_grant", build_block_grants),
}


def main():
    parser = argparse.ArgumentParser(description="Ingest GME/block grant data into Aradune lake")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--table", choices=list(ALL_TABLES.keys()) + ["all"], default="all")
    args = parser.parse_args()

    print(f"\n{'='*60}")
    print(f"GME / Block Grant Data Ingestion — {SNAPSHOT_DATE}")
    print(f"{'='*60}")
    print(f"Run ID: {RUN_ID}\n")

    con = duckdb.connect()
    totals = {}

    tables_to_build = ALL_TABLES if args.table == "all" else {args.table: ALL_TABLES[args.table]}
    for key, (fact_name, builder) in tables_to_build.items():
        totals[fact_name] = builder(con, args.dry_run)
        print()

    con.close()

    print("=" * 60)
    print("GME / BLOCK GRANT LAKE INGESTION COMPLETE")
    print("=" * 60)
    total_rows = sum(totals.values())
    for name, count in totals.items():
        status = "written" if not args.dry_run else "dry-run"
        print(f"  {name:35s} {count:>12,} rows  [{status}]")
    print(f"  {'TOTAL':35s} {total_rows:>12,} rows")

    if not args.dry_run and total_rows > 0:
        META_DIR.mkdir(parents=True, exist_ok=True)
        manifest = {
            "snapshot_date": SNAPSHOT_DATE,
            "pipeline_run_id": RUN_ID,
            "created_at": datetime.now().isoformat() + "Z",
            "tables": {name: {"rows": count} for name, count in totals.items()},
            "total_rows": total_rows,
        }
        manifest_file = META_DIR / f"manifest_gme_blockgrant_{SNAPSHOT_DATE}.json"
        with open(manifest_file, "w") as f:
            json.dump(manifest, f, indent=2)
        print(f"\n  Manifest: {manifest_file}")

    print("\nDone.")


if __name__ == "__main__":
    main()
