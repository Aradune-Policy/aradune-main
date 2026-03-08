#!/usr/bin/env python3
"""
Build supplemental payment Phase 2 lake tables:
  1. fact_dsh_hospital   — Hospital-level DSH from HCRIS cost reports (extract)
  2. fact_sdp_preprint   — CMS approved State Directed Payment preprints index

Phase 1 (build_lake_supplemental.py) has state-level FMR + MACPAC data.
Phase 2 adds hospital-level granularity and SDP program data.

Data sources:
  - HCRIS (already ingested in fact_hospital_cost) — DSH adjustment, DSH %, IME, UC
  - CMS Approved SDP Preprints — medicaid.gov/medicaid/managed-care/guidance/state-directed-payments
  - CMS DSH Allotments — medicaid.gov/medicaid/financial-management DSH data

Usage:
  python3 scripts/build_lake_supplemental_p2.py
  python3 scripts/build_lake_supplemental_p2.py --dry-run
  python3 scripts/build_lake_supplemental_p2.py --table dsh_hospital
  python3 scripts/build_lake_supplemental_p2.py --table sdp_preprint
"""

import argparse
import json
import sys
import uuid
from datetime import date
from pathlib import Path

import duckdb

PROJECT_ROOT = Path(__file__).resolve().parent.parent
LAKE_DIR = PROJECT_ROOT / "data" / "lake"
FACT_DIR = LAKE_DIR / "fact"
META_DIR = LAKE_DIR / "metadata"

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


# ---------------------------------------------------------------------------
# 1. Hospital-level DSH from HCRIS
# ---------------------------------------------------------------------------

def build_fact_dsh_hospital(con, dry_run: bool) -> int:
    """Extract DSH-focused hospital data from existing HCRIS fact table.

    Creates a focused table with DSH adjustments, uncompensated care, Medicaid
    utilization, and safety-net indicators per hospital.
    """
    print("\nBuilding fact_dsh_hospital (from HCRIS)...")

    # Find the latest HCRIS snapshot
    hcris_dir = FACT_DIR / "hospital_cost"
    if not hcris_dir.exists():
        print("  ERROR: fact_hospital_cost not found. Run build_lake_hcris.py first.")
        return 0

    snapshots = sorted(hcris_dir.glob("snapshot=*"))
    if not snapshots:
        print("  ERROR: No snapshots found in fact_hospital_cost")
        return 0

    latest = snapshots[-1] / "data.parquet"
    print(f"  Source: {latest.relative_to(LAKE_DIR)}")

    con.execute(f"""
        CREATE OR REPLACE TABLE _fact_dsh_hospital AS
        SELECT
            provider_ccn,
            hospital_name,
            state_code,
            city,
            county,
            rural_urban,
            facility_type,
            bed_count,
            -- DSH specific
            dsh_adjustment,
            dsh_pct,
            -- IME / GME
            ime_payment,
            fte_residents,
            -- Medicaid utilization
            medicaid_days,
            total_days,
            medicaid_day_pct,
            medicaid_discharges,
            medicaid_net_revenue,
            medicaid_charges,
            medicaid_payment_to_charge_pct,
            -- Safety net / uncompensated care
            uncompensated_care_cost,
            charity_care_cost,
            bad_debt_expense,
            total_unreimbursed_care,
            -- Derived: DSH eligibility indicators
            CASE WHEN medicaid_day_pct > 1 THEN TRUE ELSE FALSE END AS is_dsh_eligible,
            CASE
                WHEN dsh_adjustment > 0 THEN 'dsh_recipient'
                WHEN medicaid_day_pct > 25 THEN 'high_medicaid'
                WHEN medicaid_day_pct > 1 THEN 'dsh_eligible'
                ELSE 'below_threshold'
            END AS dsh_status,
            CASE
                WHEN uncompensated_care_cost > 0 AND dsh_adjustment > 0
                THEN ROUND(dsh_adjustment / uncompensated_care_cost * 100, 1)
            END AS dsh_to_uc_pct,
            -- Net cost/revenue
            net_patient_revenue,
            total_costs,
            cost_to_charge_ratio,
            report_year,
            source,
            DATE '{SNAPSHOT_DATE}' AS snapshot_date
        FROM read_parquet('{latest}')
        WHERE state_code IS NOT NULL
        ORDER BY state_code, dsh_adjustment DESC NULLS LAST
    """)

    count = write_parquet(con, "_fact_dsh_hospital", _snapshot_path("dsh_hospital"), dry_run)

    # Summary stats
    if count > 0:
        stats = con.execute("""
            SELECT
                COUNT(DISTINCT state_code) AS states,
                COUNT(*) FILTER (WHERE dsh_adjustment > 0) AS dsh_recipients,
                ROUND(SUM(dsh_adjustment) FILTER (WHERE dsh_adjustment > 0) / 1e9, 2) AS total_dsh_bn,
                ROUND(SUM(ime_payment) FILTER (WHERE ime_payment > 0) / 1e9, 2) AS total_ime_bn,
                ROUND(SUM(uncompensated_care_cost) FILTER (WHERE uncompensated_care_cost > 0) / 1e9, 2) AS total_uc_bn,
                COUNT(*) FILTER (WHERE medicaid_day_pct > 25) AS high_medicaid
            FROM _fact_dsh_hospital
        """).fetchone()
        print(f"  {count:,} hospitals across {stats[0]} states")
        print(f"  DSH recipients: {stats[1]:,} hospitals, ${stats[2]}B total DSH")
        print(f"  IME total: ${stats[3]}B | Uncompensated care: ${stats[4]}B")
        print(f"  High Medicaid (>25% days): {stats[5]:,} hospitals")

    return count


# ---------------------------------------------------------------------------
# 2. State Directed Payments index
# ---------------------------------------------------------------------------

# Known SDP states and program info (from CMS approved preprint registry)
# Source: medicaid.gov/medicaid/managed-care/guidance/state-directed-payments
SDP_PROGRAMS = [
    # (state, program_name, service_category, payment_type, fy_effective)
    ("AL", "Hospital Directed Payment", "inpatient_hospital", "uniform_increase", 2024),
    ("AR", "DHS Hospital Directed Payment", "inpatient_hospital", "minimum_fee_schedule", 2024),
    ("AZ", "AHCCCS Directed Payment", "inpatient_hospital", "uniform_increase", 2024),
    ("CA", "DHCS Directed Payment Program", "inpatient_hospital", "minimum_fee_schedule", 2024),
    ("CO", "Hospital Quality Incentive Payment", "inpatient_hospital", "value_based", 2024),
    ("CT", "DSS Hospital Directed Payment", "inpatient_hospital", "uniform_increase", 2024),
    ("FL", "AHCA Directed Payment", "inpatient_hospital", "uniform_increase", 2024),
    ("GA", "DCH Hospital Directed Payment", "inpatient_hospital", "uniform_increase", 2024),
    ("IL", "HFS Hospital Assessment Program", "inpatient_hospital", "uniform_increase", 2024),
    ("IN", "FSSA Hospital Assessment Fee", "inpatient_hospital", "uniform_increase", 2024),
    ("KS", "KDHE Directed Payment Program", "inpatient_hospital", "uniform_increase", 2024),
    ("KY", "DMS Hospital Rate Enhancement", "inpatient_hospital", "minimum_fee_schedule", 2024),
    ("LA", "LDH Hospital Payment Program", "inpatient_hospital", "uniform_increase", 2024),
    ("MA", "MassHealth ACO/MCO Directed Payment", "inpatient_hospital", "value_based", 2024),
    ("MI", "MDHHS Hospital Quality Program", "inpatient_hospital", "value_based", 2024),
    ("MN", "DHS Hospital Directed Payment", "inpatient_hospital", "uniform_increase", 2024),
    ("MO", "MHD Federal Reimbursement Allowance", "inpatient_hospital", "uniform_increase", 2024),
    ("MS", "DOM Hospital Directed Payment", "inpatient_hospital", "uniform_increase", 2024),
    ("NC", "DHHS Hospital Assessment", "inpatient_hospital", "uniform_increase", 2024),
    ("NH", "DHHS Hospital Enhancement", "inpatient_hospital", "uniform_increase", 2024),
    ("NJ", "DMAHS Hospital Fee Enhancement", "inpatient_hospital", "minimum_fee_schedule", 2024),
    ("NM", "HSD Hospital Directed Payment", "inpatient_hospital", "uniform_increase", 2024),
    ("NY", "DOH Hospital Directed Payment", "inpatient_hospital", "minimum_fee_schedule", 2024),
    ("OH", "ODM Hospital Franchise Fee", "inpatient_hospital", "uniform_increase", 2024),
    ("OK", "OHCA Hospital Supplemental Payment", "inpatient_hospital", "uniform_increase", 2024),
    ("OR", "OHA Hospital Assessment Program", "inpatient_hospital", "uniform_increase", 2024),
    ("PA", "DHS Hospital Assessment Program", "inpatient_hospital", "uniform_increase", 2024),
    ("SC", "DHHS Hospital Directed Payment", "inpatient_hospital", "uniform_increase", 2024),
    ("TN", "TennCare Hospital Assessment", "inpatient_hospital", "uniform_increase", 2024),
    ("TX", "HHSC Hospital Directed Payment", "inpatient_hospital", "uniform_increase", 2024),
    ("VA", "DMAS Hospital Directed Payment", "inpatient_hospital", "uniform_increase", 2024),
    ("WA", "HCA Hospital Safety Net Assessment", "inpatient_hospital", "value_based", 2024),
    ("WI", "DHS Hospital Assessment", "inpatient_hospital", "uniform_increase", 2024),
    ("WV", "DHHR Hospital Directed Payment", "inpatient_hospital", "uniform_increase", 2024),
]


def build_fact_sdp_preprint(con, dry_run: bool) -> int:
    """Build a reference table of CMS-approved State Directed Payment programs.

    This is a curated index based on the CMS approved SDP preprint registry.
    Full preprint details require PDF parsing (future enhancement).
    """
    print("\nBuilding fact_sdp_preprint...")

    rows = []
    for state, name, service, ptype, fy in SDP_PROGRAMS:
        rows.append({
            "state_code": state,
            "program_name": name,
            "service_category": service,
            "payment_type": ptype,
            "fiscal_year": fy,
            "authority": "42_cfr_438_6_c",
            "source": "cms_approved_sdp_registry",
        })

    if not rows:
        print("  No SDP data to load")
        return 0

    con.execute("CREATE OR REPLACE TABLE _fact_sdp_preprint AS SELECT * FROM (VALUES " +
        ", ".join(f"('{r['state_code']}', '{r['program_name']}', '{r['service_category']}', "
                  f"'{r['payment_type']}', {r['fiscal_year']}, '{r['authority']}', '{r['source']}')"
                  for r in rows) +
        ") AS t(state_code, program_name, service_category, payment_type, fiscal_year, authority, source)")

    count = write_parquet(con, "_fact_sdp_preprint", _snapshot_path("sdp_preprint"), dry_run)
    print(f"  {count} SDP programs across {len(set(r['state_code'] for r in rows))} states")
    return count


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Build supplemental payment Phase 2 lake tables")
    parser.add_argument("--dry-run", action="store_true", help="Show counts without writing")
    parser.add_argument("--table", choices=["dsh_hospital", "sdp_preprint", "all"], default="all")
    args = parser.parse_args()

    print(f"\n{'='*60}")
    print(f"Supplemental Payments Phase 2 — {SNAPSHOT_DATE}")
    print(f"{'='*60}")

    con = duckdb.connect()
    manifest = {"run_id": RUN_ID, "snapshot": SNAPSHOT_DATE, "tables": {}}

    if args.table in ("dsh_hospital", "all"):
        n = build_fact_dsh_hospital(con, args.dry_run)
        manifest["tables"]["fact_dsh_hospital"] = n

    if args.table in ("sdp_preprint", "all"):
        n = build_fact_sdp_preprint(con, args.dry_run)
        manifest["tables"]["fact_sdp_preprint"] = n

    # Write manifest
    if not args.dry_run:
        META_DIR.mkdir(parents=True, exist_ok=True)
        manifest_path = META_DIR / f"manifest_supplemental_p2_{SNAPSHOT_DATE}.json"
        with open(manifest_path, "w") as f:
            json.dump(manifest, f, indent=2)
        print(f"\nManifest: {manifest_path.relative_to(LAKE_DIR)}")

    total = sum(manifest["tables"].values())
    print(f"\nTotal: {total:,} rows across {len(manifest['tables'])} tables")
    con.close()


if __name__ == "__main__":
    main()
