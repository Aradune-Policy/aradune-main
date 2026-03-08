#!/usr/bin/env python3
"""
Build computed Medicaid fee schedules for RBRVS-based states.

Some states set Medicaid rates as a formula applied to Medicare RVUs/GPCIs:
  - AK: Same RVUs + GPCIs, CF = $43.412 (vs Medicare CY2025 $32.3465)
  - MI: RBRVS with CF = $21.30, blended GPCI (60% Detroit + 40% rest of state)
  - NM: 150% of CY2024 Medicare (per Jan 2025 LOD 36-1/36-2)

This script computes Medicaid rates from existing Medicare state-level rates
and adds them to fact_medicaid_rate.

Usage:
  python3 scripts/build_lake_fee_schedules_computed.py
  python3 scripts/build_lake_fee_schedules_computed.py --dry-run
  python3 scripts/build_lake_fee_schedules_computed.py --state AK
"""

import argparse
import json
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

# --------------------------------------------------------------------------
# State configurations
# --------------------------------------------------------------------------
# Each state defines how to compute Medicaid rates from Medicare rates.

COMPUTED_STATES = {
    "AK": {
        "name": "Alaska",
        "method": "rbrvs_cf",
        "medicaid_cf": 43.412,
        "medicare_cf": 32.3465,  # CY2025 non-QPP
        "multiplier": 43.412 / 32.3465,  # ~1.342
        "effective_date": "2023-07-01",
        "source": "Alaska Medicaid RBRVS, 7 AAC 43.064, CF=$43.412",
        "notes": "AK uses Medicare RVUs + AK GPCI with state CF. Rate = Medicare × (43.412/32.3465)",
    },
    "MI": {
        "name": "Michigan",
        "method": "rbrvs_custom_gpci",
        "medicaid_cf": 21.30,
        "gpci_blend": {
            # SPA 23-0023: 60% Detroit + 40% rest of state
            "pw": 0.6 * 1.003 + 0.4 * 1.000,   # 1.0018
            "pe": 0.6 * 0.986 + 0.4 * 0.911,    # 0.9560
            "mp": 0.6 * 1.718 + 0.4 * 1.173,    # 1.5002
        },
        "multiplier": 21.30 / 32.3465,  # ~0.659 for simple display
        "effective_date": "2023-10-01",
        "source": "MI MDHHS SPA 23-0023, RBRVS CF=$21.30, 60/40 Detroit blend",
        "notes": "MI uses RBRVS with statewide CF=$21.30, blended GPCI (60% Detroit + 40% rest of MI)",
    },
    "NM": {
        "name": "New Mexico",
        "method": "pct_of_medicare",
        "multiplier": 1.50,
        "effective_date": "2025-01-01",
        "source": "NM HCA LOD 36-1/36-2, 150% of CY2024 Medicare PFS",
        "notes": "NM mandates MCOs pay minimum 150% of Medicare for physician services (LOD effective Jan 2025)",
    },
}


def _latest_snapshot(fact_name: str) -> Path | None:
    """Find the latest snapshot parquet for a fact table."""
    fact_dir = FACT_DIR / fact_name
    if not fact_dir.exists():
        return None
    snapshots = sorted(fact_dir.glob("snapshot=*/data.parquet"))
    return snapshots[-1] if snapshots else None


def build_computed_rates(con, state_code: str, config: dict, dry_run: bool) -> int:
    """Compute Medicaid rates for a single state from Medicare state-level rates."""
    print(f"\nComputing Medicaid rates for {state_code} ({config['name']})...")
    print(f"  Method: {config['method']}, multiplier: {config['multiplier']:.4f}")

    effective_date = config["effective_date"]
    source = config["source"]

    if config["method"] == "rbrvs_custom_gpci":
        # Compute from RVUs with custom GPCI blend + state CF
        proc_path = LAKE_DIR / "dimension" / "dim_procedure.parquet"
        if not proc_path.exists():
            print("  ERROR: dim_procedure.parquet not found.")
            return 0
        print(f"  Source: dim_procedure.parquet (RVU-based computation)")
        gpci = config["gpci_blend"]
        cf = config["medicaid_cf"]
        pw, pe, mp = gpci["pw"], gpci["pe"], gpci["mp"]
        print(f"  CF=${cf}, GPCI blend: PW={pw:.4f}, PE={pe:.4f}, MP={mp:.4f}")

        con.execute(f"""
            CREATE OR REPLACE TABLE _computed_{state_code} AS
            SELECT
                '{state_code}' AS state_code,
                procedure_code,
                '' AS modifier,
                ROUND((work_rvu * {pw} + pe_rvu_nonfacility * {pe} + mp_rvu * {mp}) * {cf}, 2) AS rate,
                ROUND((work_rvu * {pw} + pe_rvu_facility * {pe} + mp_rvu * {mp}) * {cf}, 2) AS rate_facility,
                ROUND((work_rvu * {pw} + pe_rvu_nonfacility * {pe} + mp_rvu * {mp}) * {cf}, 2) AS rate_nonfacility,
                DATE '{effective_date}' AS effective_date,
                NULL::DATE AS end_date,
                NULL AS billing_unit,
                NULL AS place_of_service,
                FALSE AS prior_auth,
                '{source}' AS source_file,
                DATE '{SNAPSHOT_DATE}' AS snapshot_date,
                '{RUN_ID}' AS pipeline_run_id,
                DATE '{SNAPSHOT_DATE}' AS snapshot
            FROM read_parquet('{proc_path}')
            WHERE (work_rvu > 0 OR pe_rvu_nonfacility > 0)
              AND (work_rvu * {pw} + pe_rvu_nonfacility * {pe} + mp_rvu * {mp}) * {cf} > 0
        """)
    else:
        # Simple multiplier on Medicare state-level rates
        medicare_path = _latest_snapshot("medicare_rate_state")
        if not medicare_path:
            print("  ERROR: fact_medicare_rate_state not found. Run cpra_engine.py first.")
            return 0
        print(f"  Medicare source: {medicare_path.relative_to(LAKE_DIR)}")

        multiplier = config["multiplier"]
        con.execute(f"""
            CREATE OR REPLACE TABLE _computed_{state_code} AS
            SELECT
                '{state_code}' AS state_code,
                procedure_code,
                '' AS modifier,
                ROUND(nonfac_rate * {multiplier}, 2) AS rate,
                ROUND(fac_rate * {multiplier}, 2) AS rate_facility,
                ROUND(nonfac_rate * {multiplier}, 2) AS rate_nonfacility,
                DATE '{effective_date}' AS effective_date,
                NULL::DATE AS end_date,
                NULL AS billing_unit,
                NULL AS place_of_service,
                FALSE AS prior_auth,
                '{source}' AS source_file,
                DATE '{SNAPSHOT_DATE}' AS snapshot_date,
                '{RUN_ID}' AS pipeline_run_id,
                DATE '{SNAPSHOT_DATE}' AS snapshot
            FROM read_parquet('{medicare_path}')
            WHERE state_code = '{state_code}'
              AND nonfac_rate > 0
        """)

    count = con.execute(f"SELECT COUNT(*) FROM _computed_{state_code}").fetchone()[0]

    if count == 0:
        print(f"  WARNING: No Medicare rates found for {state_code}")
        return 0

    # Spot-check: 99213
    spot = con.execute(f"""
        SELECT rate FROM _computed_{state_code}
        WHERE procedure_code = '99213'
    """).fetchone()
    if spot:
        print(f"  Spot-check 99213: ${spot[0]:.2f} Medicaid")

    # Stats
    stats = con.execute(f"""
        SELECT COUNT(*) AS n_codes,
               ROUND(AVG(rate), 2) AS avg_rate,
               ROUND(MIN(rate), 2) AS min_rate,
               ROUND(MAX(rate), 2) AS max_rate
        FROM _computed_{state_code}
    """).fetchone()
    print(f"  {stats[0]:,} codes, avg ${stats[1]}, range ${stats[2]}-${stats[3]}")

    return count


def merge_into_medicaid_rate(con, state_codes: list[str], dry_run: bool) -> int:
    """Merge computed rates into fact_medicaid_rate."""
    medicaid_path = _latest_snapshot("medicaid_rate")
    if not medicaid_path:
        print("\n  ERROR: fact_medicaid_rate not found.")
        return 0

    print(f"\nMerging into fact_medicaid_rate...")
    print(f"  Existing: {medicaid_path.relative_to(LAKE_DIR)}")

    # Load existing data
    con.execute(f"""
        CREATE OR REPLACE TABLE _existing_medicaid AS
        SELECT * FROM read_parquet('{medicaid_path}')
    """)
    existing_count = con.execute("SELECT COUNT(*) FROM _existing_medicaid").fetchone()[0]
    existing_states = con.execute("SELECT COUNT(DISTINCT state_code) FROM _existing_medicaid").fetchone()[0]
    print(f"  Existing: {existing_count:,} rows, {existing_states} states")

    # Remove any existing rows for these states (to avoid duplicates)
    state_list = ", ".join(f"'{s}'" for s in state_codes)
    con.execute(f"DELETE FROM _existing_medicaid WHERE state_code IN ({state_list})")
    after_delete = con.execute("SELECT COUNT(*) FROM _existing_medicaid").fetchone()[0]
    removed = existing_count - after_delete
    if removed > 0:
        print(f"  Removed {removed:,} existing rows for {state_list}")

    # Union with computed rates
    union_parts = ["SELECT * FROM _existing_medicaid"]
    for sc in state_codes:
        union_parts.append(f"SELECT * FROM _computed_{sc}")

    con.execute("CREATE OR REPLACE TABLE _merged_medicaid AS " + " UNION ALL ".join(union_parts))

    new_count = con.execute("SELECT COUNT(*) FROM _merged_medicaid").fetchone()[0]
    new_states = con.execute("SELECT COUNT(DISTINCT state_code) FROM _merged_medicaid").fetchone()[0]
    print(f"  Merged: {new_count:,} rows, {new_states} states")

    # Write output
    if not dry_run:
        out_path = FACT_DIR / "medicaid_rate" / f"snapshot={SNAPSHOT_DATE}" / "data.parquet"
        out_path.parent.mkdir(parents=True, exist_ok=True)
        con.execute(f"COPY _merged_medicaid TO '{out_path}' (FORMAT PARQUET, COMPRESSION ZSTD)")
        size_kb = out_path.stat().st_size / 1024
        print(f"  -> {out_path.relative_to(LAKE_DIR)} ({new_count:,} rows, {size_kb:.1f} KB)")
    else:
        print(f"  [dry-run] Would write {new_count:,} rows")

    return new_count


def main():
    parser = argparse.ArgumentParser(description="Build computed Medicaid fee schedules")
    parser.add_argument("--dry-run", action="store_true", help="Show counts without writing")
    parser.add_argument("--state", choices=list(COMPUTED_STATES.keys()) + ["all"], default="all")
    args = parser.parse_args()

    print(f"\n{'='*60}")
    print(f"Computed Fee Schedules — {SNAPSHOT_DATE}")
    print(f"{'='*60}")

    con = duckdb.connect()
    states_to_build = list(COMPUTED_STATES.keys()) if args.state == "all" else [args.state]
    manifest = {"run_id": RUN_ID, "snapshot": SNAPSHOT_DATE, "states": {}}

    for sc in states_to_build:
        cfg = COMPUTED_STATES[sc]
        n = build_computed_rates(con, sc, cfg, args.dry_run)
        manifest["states"][sc] = {"rows": n, "method": cfg["method"], "multiplier": cfg["multiplier"]}

    # Check if any computed table has columns that don't match existing schema
    medicaid_path = _latest_snapshot("medicaid_rate")
    if medicaid_path:
        existing_cols = [c[0] for c in con.execute(f"DESCRIBE SELECT * FROM read_parquet('{medicaid_path}')").fetchall()]
        print(f"\n  Existing medicaid_rate columns: {existing_cols}")

    # Merge into fact_medicaid_rate
    built_states = [sc for sc in states_to_build if manifest["states"][sc]["rows"] > 0]
    if built_states:
        merge_into_medicaid_rate(con, built_states, args.dry_run)

    # Write manifest
    if not args.dry_run:
        META_DIR.mkdir(parents=True, exist_ok=True)
        manifest_path = META_DIR / f"manifest_computed_fees_{SNAPSHOT_DATE}.json"
        with open(manifest_path, "w") as f:
            json.dump(manifest, f, indent=2)
        print(f"\nManifest: {manifest_path.relative_to(LAKE_DIR)}")

    total = sum(s["rows"] for s in manifest["states"].values())
    print(f"\nTotal: {total:,} computed rates across {len(built_states)} states")
    con.close()


if __name__ == "__main__":
    main()
