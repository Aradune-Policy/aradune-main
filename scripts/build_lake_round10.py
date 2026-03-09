#!/usr/bin/env python3
"""
build_lake_round10.py — Ingest Round 10 datasets into the lake.

Datasets:
  1. Eligibility Processing (renewal/redetermination data) — 3,162 rows
  2. Marketplace Unwinding Transitions (HealthCare.gov) — 59,527 rows
  3. SBM Unwinding (State-Based Marketplace) — 128 rows
  4. Exclusive Pediatric Drugs — 262 rows
  5. Clotting Factor Drugs — 500 rows

Usage:
  python3 scripts/build_lake_round10.py
  python3 scripts/build_lake_round10.py --dry-run
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
REF_DIR = LAKE_DIR / "reference"
META_DIR = LAKE_DIR / "metadata"

SNAPSHOT_DATE = date.today().isoformat()
RUN_ID = str(uuid.uuid4())


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


def _fact_path(name: str) -> Path:
    return FACT_DIR / name / f"snapshot={SNAPSHOT_DATE}" / "data.parquet"


def _ref_path(name: str) -> Path:
    return REF_DIR / f"{name}.parquet"


# ── 1. Eligibility Processing ─────────────────────────────────────────

def build_eligibility_processing(con, dry_run: bool) -> int:
    print("Building fact_eligibility_processing...")
    path = RAW_DIR / "medicaid_eligibility_processing.json"
    if not path.exists():
        print("  SKIPPED — file not found")
        return 0

    con.execute(f"""
        CREATE OR REPLACE TABLE _elig_proc AS
        SELECT
            state_abbreviation AS state_code,
            state_name,
            CAST(reporting_period AS VARCHAR) AS reporting_period,
            original_or_updated,
            TRY_CAST(REPLACE(beneficiaries_with_a_renewal_initiated, ',', '') AS BIGINT) AS renewals_initiated,
            TRY_CAST(REPLACE(beneficiaries_with_a_renewal_due, ',', '') AS BIGINT) AS renewals_due,
            TRY_CAST(REPLACE(beneficiaries_whose_coverage_was_renewed_total, ',', '') AS BIGINT) AS renewals_completed,
            TRY_CAST(REPLACE(beneficiaries_whose_coverage_was_renewed_on_an_ex_parte_basis, ',', '') AS BIGINT) AS renewals_ex_parte,
            TRY_CAST(REPLACE(beneficiaries_whose_coverage_was_renewed_based_on_a_renewal_form, ',', '') AS BIGINT) AS renewals_form_based,
            TRY_CAST(REPLACE(beneficiaries_disenrolled_at_renewal_total, ',', '') AS BIGINT) AS disenrolled_total,
            TRY_CAST(REPLACE(beneficiaries_determined_ineligible_at_renewal, ',', '') AS BIGINT) AS disenrolled_ineligible,
            TRY_CAST(REPLACE(beneficiaries_disenrolled_for_procedural_reasons_at_renewal, ',', '') AS BIGINT) AS disenrolled_procedural,
            TRY_CAST(REPLACE(beneficiaries_with_a_pending_renewal, ',', '') AS BIGINT) AS renewals_pending,
            'data.medicaid.gov' AS source,
            DATE '{SNAPSHOT_DATE}' AS snapshot_date
        FROM read_json_auto('{path}', maximum_object_size=134217728)
        WHERE state_abbreviation IS NOT NULL
          AND LENGTH(state_abbreviation) = 2
    """)

    count = write_parquet(con, "_elig_proc", _fact_path("eligibility_processing"), dry_run)
    if count > 0:
        states = con.execute("SELECT COUNT(DISTINCT state_code) FROM _elig_proc").fetchone()[0]
        periods = con.execute("SELECT COUNT(DISTINCT reporting_period) FROM _elig_proc").fetchone()[0]
        print(f"  {count:,} rows, {states} states, {periods} reporting periods")
    con.execute("DROP TABLE IF EXISTS _elig_proc")
    return count


# ── 2. Marketplace Unwinding Transitions ──────────────────────────────

def build_marketplace_unwinding(con, dry_run: bool) -> int:
    print("Building fact_marketplace_unwinding...")
    path = RAW_DIR / "marketplace_unwinding_transitions.json"
    if not path.exists():
        print("  SKIPPED — file not found")
        return 0

    con.execute(f"""
        CREATE OR REPLACE TABLE _mkt_unwind AS
        SELECT
            state,
            metric,
            CAST(time_period AS VARCHAR) AS time_period,
            CAST(release_through AS VARCHAR) AS release_through,
            TRY_CAST(REPLACE(count_of_individuals_whose_medicaid_or_chip_coverage_was_te_dd11, ',', '') AS BIGINT) AS individual_count,
            TRIM(percentage_of_individuals_whose_medicaid_or_chip_coverage_w_da6e) AS individual_pct,
            TRY_CAST(REPLACE(cumulative_count_of_individuals_whose_medicaid_or_chip_cove_28d9, ',', '') AS BIGINT) AS cumulative_count,
            TRIM(cumulative_percentage_of_individuals_whose_medicaid_or_chip_6ac2) AS cumulative_pct,
            'data.medicaid.gov' AS source,
            DATE '{SNAPSHOT_DATE}' AS snapshot_date
        FROM read_json_auto('{path}', maximum_object_size=134217728)
        WHERE state IS NOT NULL
    """)

    count = write_parquet(con, "_mkt_unwind", _fact_path("marketplace_unwinding"), dry_run)
    if count > 0:
        states = con.execute("SELECT COUNT(DISTINCT state) FROM _mkt_unwind").fetchone()[0]
        metrics = con.execute("SELECT COUNT(DISTINCT metric) FROM _mkt_unwind").fetchone()[0]
        print(f"  {count:,} rows, {states} states/territories, {metrics} metrics")
    con.execute("DROP TABLE IF EXISTS _mkt_unwind")
    return count


# ── 3. SBM Unwinding ─────────────────────────────────────────────────

def build_sbm_unwinding(con, dry_run: bool) -> int:
    print("Building fact_sbm_unwinding...")
    path = RAW_DIR / "sbm_unwinding.json"
    if not path.exists():
        print("  SKIPPED — file not found")
        return 0

    con.execute(f"""
        CREATE OR REPLACE TABLE _sbm AS
        SELECT *,
            'data.medicaid.gov' AS source,
            DATE '{SNAPSHOT_DATE}' AS snapshot_date
        FROM read_json_auto('{path}', maximum_object_size=134217728)
    """)

    count = write_parquet(con, "_sbm", _fact_path("sbm_unwinding"), dry_run)
    print(f"  {count:,} rows")
    con.execute("DROP TABLE IF EXISTS _sbm")
    return count


# ── 4. Exclusive Pediatric Drugs (reference) ─────────────────────────

def build_pediatric_drugs(con, dry_run: bool) -> int:
    print("Building ref_pediatric_drugs...")
    path = RAW_DIR / "exclusive_pediatric_drugs.json"
    if not path.exists():
        print("  SKIPPED — file not found")
        return 0

    con.execute(f"""
        CREATE OR REPLACE TABLE _ped_drugs AS
        SELECT
            ndc_1 AS labeler_code,
            ndc_2 AS product_code,
            ndc_3 AS package_code,
            ndc_1 || ndc_2 || ndc_3 AS ndc,
            TRIM(labeler_name) AS labeler_name,
            TRIM(product_name) AS product_name,
            effective_quarter,
            termination_date
        FROM read_json_auto('{path}', maximum_object_size=134217728)
    """)

    count = write_parquet(con, "_ped_drugs", _ref_path("ref_pediatric_drugs"), dry_run)
    print(f"  {count:,} rows")
    con.execute("DROP TABLE IF EXISTS _ped_drugs")
    return count


# ── 5. Clotting Factor Drugs (reference) ─────────────────────────────

def build_clotting_factor_drugs(con, dry_run: bool) -> int:
    print("Building ref_clotting_factor_drugs...")
    path = RAW_DIR / "clotting_factor_drugs.json"
    if not path.exists():
        print("  SKIPPED — file not found")
        return 0

    con.execute(f"""
        CREATE OR REPLACE TABLE _clot_drugs AS
        SELECT *
        FROM read_json_auto('{path}', maximum_object_size=134217728)
    """)

    count = write_parquet(con, "_clot_drugs", _ref_path("ref_clotting_factor_drugs"), dry_run)
    print(f"  {count:,} rows")
    con.execute("DROP TABLE IF EXISTS _clot_drugs")
    return count


# ── Main ──────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Ingest Round 10 datasets")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    print(f"Snapshot: {SNAPSHOT_DATE}")
    print(f"Run ID:   {RUN_ID}")
    print()

    con = duckdb.connect()
    counts = {}

    counts["eligibility_processing"] = build_eligibility_processing(con, args.dry_run)
    counts["marketplace_unwinding"] = build_marketplace_unwinding(con, args.dry_run)
    counts["sbm_unwinding"] = build_sbm_unwinding(con, args.dry_run)
    counts["pediatric_drugs"] = build_pediatric_drugs(con, args.dry_run)
    counts["clotting_factor_drugs"] = build_clotting_factor_drugs(con, args.dry_run)

    con.close()

    print()
    print("=" * 60)
    print("ROUND 10 LAKE INGESTION COMPLETE")
    print("=" * 60)
    total = 0
    for name, count in counts.items():
        print(f"  {name:40s} {count:>10,} rows")
        total += count
    print(f"  {'TOTAL':40s} {total:>10,} rows")

    if not args.dry_run and total > 0:
        META_DIR.mkdir(parents=True, exist_ok=True)
        manifest = {
            "snapshot_date": SNAPSHOT_DATE,
            "pipeline_run_id": RUN_ID,
            "created_at": datetime.now().isoformat() + "Z",
            "tables": {k: {"rows": v} for k, v in counts.items()},
            "total_rows": total,
        }
        manifest_file = META_DIR / f"manifest_round10_{SNAPSHOT_DATE}.json"
        with open(manifest_file, "w") as f:
            json.dump(manifest, f, indent=2)
        print(f"\n  Manifest: {manifest_file}")

    print("\nDone.")


if __name__ == "__main__":
    main()
