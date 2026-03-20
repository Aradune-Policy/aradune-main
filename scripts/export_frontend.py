#!/usr/bin/env python3
"""
export_frontend.py — Export from the data lake to public/data/ for the frontend.

Reads from:
  - data/lake/dimension/*.parquet
  - data/lake/fact/*/snapshot=LATEST/*.parquet

Writes to:
  - public/data/ (JSON + Parquet files consumed by the frontend)

This replaces the manual copy-paste workflow. It validates data before writing
and refuses to overwrite production files if quality gates fail.

Usage:
  python3 scripts/export_frontend.py
  python3 scripts/export_frontend.py --dry-run
  python3 scripts/export_frontend.py --skip-validation
"""

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import duckdb

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parent.parent
LAKE_DIR = PROJECT_ROOT / "data" / "lake"
DIM_DIR = LAKE_DIR / "dimension"
FACT_DIR = LAKE_DIR / "fact"
PUBLIC_DATA = PROJECT_ROOT / "public" / "data"


def latest_snapshot(fact_name: str) -> Path:
    """Find the most recent snapshot directory for a fact table."""
    fact_path = FACT_DIR / fact_name
    if not fact_path.exists():
        return None
    snapshots = sorted(fact_path.glob("snapshot=*"), reverse=True)
    if not snapshots:
        return None
    return snapshots[0] / "data.parquet"


def validate(con: duckdb.DuckDBPyConnection, paths: dict) -> list[str]:
    """Run quality gates. Returns list of failures (empty = all passed)."""
    failures = []

    # Gate 1: Rate comparison has enough states
    rc = paths.get("rate_comparison")
    if rc and rc.exists():
        states = con.execute(f"SELECT COUNT(DISTINCT state_code) FROM '{rc}'").fetchone()[0]
        if states < 30:
            failures.append(f"GATE FAIL: rate_comparison has only {states} states (need >= 30)")

    # Gate 2: Median pct_of_medicare is plausible
    if rc and rc.exists():
        median = con.execute(f"""
            SELECT MEDIAN(pct_of_medicare)
            FROM '{rc}'
            WHERE pct_of_medicare IS NOT NULL AND pct_of_medicare > 0 AND pct_of_medicare < 500
        """).fetchone()[0]
        if median and (median < 50 or median > 200):
            failures.append(f"GATE FAIL: median pct_of_medicare = {median:.1f}% (expected 50-200%)")

    # Gate 3: Medicaid rates table isn't empty
    mr = paths.get("medicaid_rate")
    if mr and mr.exists():
        rows = con.execute(f"SELECT COUNT(*) FROM '{mr}'").fetchone()[0]
        if rows < 100000:
            failures.append(f"GATE FAIL: medicaid_rate has only {rows:,} rows (expected 100K+)")

    # Gate 4: E/M codes present
    if rc and rc.exists():
        em_count = con.execute(f"SELECT COUNT(*) FROM '{rc}' WHERE em_category IS NOT NULL").fetchone()[0]
        if em_count < 1000:
            failures.append(f"GATE FAIL: only {em_count:,} E/M rate comparisons (expected 1000+)")

    # Gate 5: dim_procedure has correct CF
    dp = DIM_DIR / "dim_procedure.parquet"
    if dp.exists():
        cf = con.execute(f"SELECT DISTINCT conversion_factor FROM '{dp}' WHERE conversion_factor IS NOT NULL").fetchall()
        cfs = [float(c[0]) for c in cf]
        if cfs and abs(cfs[0] - 33.4009) > 0.01:
            failures.append(f"GATE FAIL: dim_procedure CF = {cfs[0]} (expected 33.4009)")

    return failures


def export_cpra_em(con: duckdb.DuckDBPyConnection, rc_path: Path, dry_run: bool):
    """Export cpra_em.json — E/M codes only from rate_comparison."""
    print("  Exporting cpra_em.json...")

    rows = con.execute(f"""
        SELECT state_code, procedure_code, medicaid_rate,
               medicare_nonfac_rate, medicare_fac_rate,
               pct_of_medicare, em_category, modifier,
               medicaid_rate_date
        FROM '{rc_path}'
        WHERE em_category IS NOT NULL AND pct_of_medicare < 500
        ORDER BY state_code, procedure_code
    """).fetchall()

    # Group by state
    result = {}
    for r in rows:
        state = r[0]
        if state not in result:
            result[state] = []
        result[state].append({
            "procedure_code": r[1],
            "medicaid_rate": round(r[2], 2) if r[2] else None,
            "medicare_nonfac_rate": round(r[3], 2) if r[3] else None,
            "medicare_fac_rate": round(r[4], 2) if r[4] else None,
            "pct_of_medicare": round(r[5], 2) if r[5] else None,
            "em_category": r[6],
            "modifier": r[7] if r[7] else None,
            "rate_effective_date": r[8],
        })

    count = sum(len(v) for v in result.values())
    print(f"    {count:,} rows across {len(result)} states")

    if not dry_run:
        out = PUBLIC_DATA / "cpra_em.json"
        with open(out, "w") as f:
            json.dump(result, f, separators=(",", ":"))
        print(f"    Wrote {out} ({out.stat().st_size / 1024:.0f} KB)")


def export_cpra_summary(con: duckdb.DuckDBPyConnection, rc_path: Path, dry_run: bool):
    """Export cpra_summary.json — state-level aggregates."""
    print("  Exporting cpra_summary.json...")

    # National summary
    nat = con.execute(f"""
        SELECT
            COUNT(DISTINCT state_code) AS state_count,
            COUNT(*) AS total_comparisons,
            AVG(pct_of_medicare) AS avg_pct,
            MEDIAN(pct_of_medicare) AS median_pct
        FROM '{rc_path}'
        WHERE pct_of_medicare IS NOT NULL AND pct_of_medicare > 0 AND pct_of_medicare < 500
    """).fetchone()

    # Per-state summaries
    states_data = con.execute(f"""
        SELECT
            state_code,
            COUNT(*) AS code_count,
            AVG(pct_of_medicare) AS avg_pct,
            MEDIAN(pct_of_medicare) AS median_pct,
            SUM(CASE WHEN pct_of_medicare < 80 THEN 1 ELSE 0 END) AS below_80_count,
            SUM(CASE WHEN pct_of_medicare < 50 THEN 1 ELSE 0 END) AS below_50_count,
            MIN(pct_of_medicare) AS min_pct,
            MAX(pct_of_medicare) AS max_pct
        FROM '{rc_path}'
        WHERE pct_of_medicare IS NOT NULL AND pct_of_medicare > 0 AND pct_of_medicare < 500
        GROUP BY state_code
        ORDER BY state_code
    """).fetchall()

    result = {
        "national": {
            "state_count": nat[0],
            "total_comparisons": nat[1],
            "avg_pct": round(nat[2], 2) if nat[2] else None,
            "median_pct": round(nat[3], 2) if nat[3] else None,
        },
        "states": {}
    }
    for s in states_data:
        result["states"][s[0]] = {
            "code_count": s[1],
            "avg_pct": round(s[2], 2) if s[2] else None,
            "median_pct": round(s[3], 2) if s[3] else None,
            "below_80_count": s[4],
            "below_50_count": s[5],
            "min_pct": round(s[6], 2) if s[6] else None,
            "max_pct": round(s[7], 2) if s[7] else None,
        }

    print(f"    National: {nat[0]} states, median {nat[3]:.1f}% of Medicare")

    if not dry_run:
        out = PUBLIC_DATA / "cpra_summary.json"
        with open(out, "w") as f:
            json.dump(result, f, indent=2)
        print(f"    Wrote {out}")


def export_dq_flags_em(con: duckdb.DuckDBPyConnection, dq_path: Path, rc_path: Path, dry_run: bool):
    """Export dq_flags_em.json — DQ flags scoped to E/M codes + state-level."""
    print("  Exporting dq_flags_em.json...")

    # Get E/M procedure codes from rate comparison
    em_codes = con.execute(f"""
        SELECT DISTINCT procedure_code
        FROM '{rc_path}'
        WHERE em_category IS NOT NULL
    """).fetchall()
    em_set = {r[0] for r in em_codes}

    # Get all flags, filter to E/M + state-level
    all_flags = con.execute(f"""
        SELECT state_code, entity_id, flag_type, severity, detail, entity_type
        FROM '{dq_path}'
    """).fetchall()

    em_flags = [f for f in all_flags if f[5] == "state" or f[1] in em_set]

    # Build summary
    summary = {}
    state_rollups = {}
    for f in em_flags:
        flag_type = f[2]
        state = f[0]
        summary[flag_type] = summary.get(flag_type, 0) + 1
        if state not in state_rollups:
            state_rollups[state] = {}
        state_rollups[state][flag_type] = state_rollups[state].get(flag_type, 0) + 1

    result = {
        "summary": summary,
        "total_flags": len(em_flags),
        "state_rollups": state_rollups,
    }

    print(f"    {len(em_flags):,} E/M-scoped flags")

    if not dry_run:
        out = PUBLIC_DATA / "dq_flags_em.json"
        with open(out, "w") as f:
            json.dump(result, f, separators=(",", ":"))
        print(f"    Wrote {out}")


def export_dim_447_codes(con: duckdb.DuckDBPyConnection, dry_run: bool):
    """Export dim_447_codes.json — 74 E/M codes per 42 CFR 447.203."""
    print("  Exporting dim_447_codes.json...")

    dp = DIM_DIR / "dim_procedure.parquet"
    rows = con.execute(f"""
        SELECT procedure_code, em_category, description
        FROM '{dp}'
        WHERE is_em_code = TRUE
        ORDER BY em_category, procedure_code
    """).fetchall()

    result = [
        {
            "cpt_code": r[0],
            "category": r[1],
            "description": r[2],
            "source": "dim_procedure (unified lake)",
        }
        for r in rows
    ]

    print(f"    {len(result)} E/M codes")

    if not dry_run:
        out = PUBLIC_DATA / "dim_447_codes.json"
        with open(out, "w") as f:
            json.dump(result, f, indent=2)
        print(f"    Wrote {out}")


def export_medicare_rates(con: duckdb.DuckDBPyConnection, mrs_path: Path, dry_run: bool):
    """Export medicare_rates.json — state-level Medicare rates."""
    print("  Exporting medicare_rates.json...")

    dp = DIM_DIR / "dim_procedure.parquet"

    rows = con.execute(f"""
        SELECT
            m.procedure_code,
            m.state_code,
            m.nonfac_rate,
            m.fac_rate,
            p.description,
            p.work_rvu
        FROM '{mrs_path}' m
        LEFT JOIN '{dp}' p ON m.procedure_code = p.procedure_code
        WHERE m.nonfac_rate > 0
        ORDER BY m.procedure_code, m.state_code
    """).fetchall()

    # Group by procedure code
    result = {}
    for r in rows:
        code = r[0]
        if code not in result:
            result[code] = {
                "rates": {},
                "d": r[4][:60] if r[4] else None,
                "w": round(r[5], 2) if r[5] else None,
            }
        result[code]["rates"][r[1]] = {
            "r": round(r[2], 2),
            "fr": round(r[3], 2) if r[3] else None,
        }

    print(f"    {len(result):,} codes with rates")

    if not dry_run:
        out = PUBLIC_DATA / "medicare_rates.json"
        with open(out, "w") as f:
            json.dump(result, f, separators=(",", ":"))
        print(f"    Wrote {out} ({out.stat().st_size / 1024:.0f} KB)")


def export_conversion_factors(con: duckdb.DuckDBPyConnection, dry_run: bool):
    """Export conversion_factors.json — state-level CF + methodology metadata."""
    print("  Exporting conversion_factors.json...")

    ds = DIM_DIR / "dim_state.parquet"
    rows = con.execute(f"""
        SELECT state_code, state_name, methodology, conversion_factor,
               cf_effective_date, rvu_source, update_frequency
        FROM '{ds}'
        ORDER BY state_code
    """).fetchall()

    result = {}
    for r in rows:
        result[r[0]] = {
            "name": r[1],
            "methodology": r[2],
            "cf": round(r[3], 4) if r[3] else None,
            "cf_date": str(r[4]) if r[4] else None,
            "rvu_source": r[5],
            "update_frequency": r[6],
        }

    print(f"    {len(result)} states")

    if not dry_run:
        out = PUBLIC_DATA / "conversion_factors.json"
        with open(out, "w") as f:
            json.dump(result, f, indent=2)
        print(f"    Wrote {out}")


def main():
    parser = argparse.ArgumentParser(description="Export data lake to frontend public/data/")
    parser.add_argument("--dry-run", action="store_true", help="Print what would be exported without writing")
    parser.add_argument("--skip-validation", action="store_true", help="Skip quality gates")
    args = parser.parse_args()

    # Resolve latest snapshots
    paths = {}
    print("Resolving latest snapshots:")
    for fact_name in ["medicaid_rate", "medicare_rate", "medicare_rate_state",
                       "rate_comparison", "dq_flag", "enrollment",
                       "quality_measure", "expenditure"]:
        p = latest_snapshot(fact_name)
        if p and p.exists():
            snapshot_dir = p.parent.name  # snapshot=2026-03-05
            paths[fact_name] = p
            print(f"  {fact_name:30s} -> {snapshot_dir}")
        else:
            print(f"  {fact_name:30s} -> NOT FOUND")

    print()

    con = duckdb.connect()

    # Quality gates
    if not args.skip_validation:
        print("Running quality gates...")
        failures = validate(con, paths)
        if failures:
            print()
            for f in failures:
                print(f"  {f}")
            print(f"\n{len(failures)} gate(s) failed. Use --skip-validation to override.")
            sys.exit(1)
        print("  All gates passed.\n")

    # Export each frontend file
    print("Exporting frontend files:")

    rc_path = paths.get("rate_comparison")
    dq_path = paths.get("dq_flag")
    mrs_path = paths.get("medicare_rate_state")

    if rc_path:
        export_cpra_em(con, rc_path, args.dry_run)
        export_cpra_summary(con, rc_path, args.dry_run)
    if dq_path and rc_path:
        export_dq_flags_em(con, dq_path, rc_path, args.dry_run)
    export_dim_447_codes(con, args.dry_run)
    if mrs_path:
        export_medicare_rates(con, mrs_path, args.dry_run)
    export_conversion_factors(con, args.dry_run)

    con.close()

    print("\nExport complete.")
    if not args.dry_run:
        print(f"\nFrontend files in {PUBLIC_DATA}/:")
        for name in ["cpra_em.json", "cpra_summary.json", "dq_flags_em.json",
                      "dim_447_codes.json", "medicare_rates.json", "conversion_factors.json"]:
            f = PUBLIC_DATA / name
            if f.exists():
                print(f"  {name:40s} {f.stat().st_size / 1024:>8.1f} KB")


if __name__ == "__main__":
    main()
