#!/usr/bin/env python3
"""
build_lake_rate_comparison_v2.py — Rebuild fact_rate_comparison_v2 from published fee schedule data.

Sources (priority order):
  1. State-specific fee schedule tables (fee_schedule_ca, fee_schedule_tx, etc.) — 18 tables
  2. fact_medicaid_rate (597K rows, 47 states) — scraped from state fee schedules
  3. CF × RVU fallback — only for states with NO published data at all

Joins with:
  - dim_procedure: medicare_rate_nonfac, total_rvu_nonfac, em_category, description
  - dim_state: conversion_factor (updated with derived CFs)

Outputs:
  - data/lake/fact/rate_comparison_v2/data.parquet
  - data/lake/dimension/dim_state.parquet (updated conversion_factors)

Usage:
  python3 scripts/build_lake_rate_comparison_v2.py
  python3 scripts/build_lake_rate_comparison_v2.py --dry-run
"""

import argparse
import os
import sys
from datetime import date
from pathlib import Path

import duckdb

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parent.parent
LAKE_DIR = PROJECT_ROOT / "data" / "lake"
DIM_DIR = LAKE_DIR / "dimension"
FACT_DIR = LAKE_DIR / "fact"

SNAPSHOT_DATE = date.today().isoformat()

# Fee schedule table directories
FEE_SCHEDULE_STATES = sorted([
    d.name.replace("fee_schedule_", "").upper()
    for d in (FACT_DIR).iterdir()
    if d.is_dir() and d.name.startswith("fee_schedule_")
])


def main():
    parser = argparse.ArgumentParser(
        description="Rebuild fact_rate_comparison_v2 from published fee schedule data"
    )
    parser.add_argument("--dry-run", action="store_true",
                        help="Print stats without writing files")
    args = parser.parse_args()

    print("=" * 70)
    print("BUILD fact_rate_comparison_v2")
    print("=" * 70)
    print(f"Snapshot date: {SNAPSHOT_DATE}")
    print(f"Fee schedule states: {FEE_SCHEDULE_STATES} ({len(FEE_SCHEDULE_STATES)})")
    print()

    # ------------------------------------------------------------------
    # 1. Connect DuckDB in-memory, load dimensions
    # ------------------------------------------------------------------
    con = duckdb.connect()

    dim_proc_path = DIM_DIR / "dim_procedure.parquet"
    dim_state_path = DIM_DIR / "dim_state.parquet"

    if not dim_proc_path.exists():
        print(f"ERROR: {dim_proc_path} not found", file=sys.stderr)
        sys.exit(1)
    if not dim_state_path.exists():
        print(f"ERROR: {dim_state_path} not found", file=sys.stderr)
        sys.exit(1)

    con.execute(f"""
        CREATE TABLE dim_procedure AS
        SELECT * FROM read_parquet('{dim_proc_path}')
    """)
    con.execute(f"""
        CREATE TABLE dim_state AS
        SELECT * FROM read_parquet('{dim_state_path}')
    """)

    proc_count = con.execute("SELECT COUNT(*) FROM dim_procedure").fetchone()[0]
    state_count = con.execute("SELECT COUNT(*) FROM dim_state").fetchone()[0]
    print(f"Loaded dim_procedure: {proc_count:,} codes")
    print(f"Loaded dim_state: {state_count} states/territories")
    print()

    # ------------------------------------------------------------------
    # 2. Load fact_medicaid_rate (all published rates from scraper)
    # ------------------------------------------------------------------
    print("Step 1: Loading fact_medicaid_rate...")

    medicaid_rate_path = FACT_DIR / "medicaid_rate"
    # Support both snapshot= partitioned and flat
    snap_glob = medicaid_rate_path / "snapshot=*" / "data.parquet"
    flat_file = medicaid_rate_path / "data.parquet"

    if list(medicaid_rate_path.glob("snapshot=*/data.parquet")):
        mr_path = str(snap_glob)
    elif flat_file.exists():
        mr_path = str(flat_file)
    else:
        print(f"ERROR: No data found in {medicaid_rate_path}", file=sys.stderr)
        sys.exit(1)

    # Filter out APC/facility/ASC/outpatient rates that contaminate physician comparisons.
    # These are facility fee schedules (include device costs, facility fees) and should NOT
    # be compared to Medicare non-facility physician rates. Key contaminated states:
    #   RI: 19,658 APC facility rates (cardiac device implantation $39K)
    #   CT: 6,696 addendum_b facility fee schedule rows (median $1,620)
    #   OK: 3,980 APC/ASC facility codes (median $2,078)
    #   DE: 5,442 ASC fee schedule rows
    #   NE: 2,589 ASC service rows
    #   AL: 360 outpatient hospital fee schedule rows
    con.execute(f"""
        CREATE TABLE _medicaid_rate_raw AS
        SELECT
            state_code,
            procedure_code,
            COALESCE(modifier, '') AS modifier,
            COALESCE(rate, rate_nonfacility) AS medicaid_rate,
            'published_direct' AS rate_source
        FROM read_parquet('{mr_path}')
        WHERE COALESCE(rate, rate_nonfacility) IS NOT NULL
          AND COALESCE(rate, rate_nonfacility) > 0
          AND (
              LOWER(COALESCE(source_file, '')) NOT LIKE '%apc%'
              AND LOWER(COALESCE(source_file, '')) NOT LIKE '%facility%'
              AND LOWER(COALESCE(source_file, '')) NOT LIKE '%outpatient%'
              AND LOWER(COALESCE(source_file, '')) NOT LIKE '%addendum_b%'
              AND LOWER(COALESCE(source_file, '')) NOT LIKE '%asc %'
              AND LOWER(COALESCE(source_file, '')) NOT LIKE '%asc_%'
          )
    """)

    mr_count = con.execute("SELECT COUNT(*) FROM _medicaid_rate_raw").fetchone()[0]
    mr_states = con.execute("SELECT COUNT(DISTINCT state_code) FROM _medicaid_rate_raw").fetchone()[0]
    print(f"  fact_medicaid_rate: {mr_count:,} rows, {mr_states} states (after APC/facility/ASC filter)")

    # Report how many rows were excluded by the facility filter
    total_before_filter = con.execute(f"""
        SELECT COUNT(*) FROM read_parquet('{mr_path}')
        WHERE COALESCE(rate, rate_nonfacility) IS NOT NULL
          AND COALESCE(rate, rate_nonfacility) > 0
    """).fetchone()[0]
    filtered_out = total_before_filter - mr_count
    print(f"  APC/facility/ASC rows excluded: {filtered_out:,} ({filtered_out/total_before_filter*100:.1f}%)")

    # Deduplicate: keep MAX rate per (state, code, modifier) to avoid dupes
    con.execute("""
        CREATE TABLE _medicaid_rates AS
        SELECT
            state_code,
            procedure_code,
            modifier,
            MAX(medicaid_rate) AS medicaid_rate,
            'published_direct' AS rate_source
        FROM _medicaid_rate_raw
        GROUP BY state_code, procedure_code, modifier
    """)
    con.execute("DROP TABLE _medicaid_rate_raw")

    dedup_count = con.execute("SELECT COUNT(*) FROM _medicaid_rates").fetchone()[0]
    print(f"  After dedup: {dedup_count:,} rows")
    print()

    # ------------------------------------------------------------------
    # 3. Load state-specific fee schedule tables
    # ------------------------------------------------------------------
    print("Step 2: Loading state fee schedule tables...")

    con.execute("""
        CREATE TABLE _fs_rates (
            state_code VARCHAR,
            procedure_code VARCHAR,
            modifier VARCHAR,
            medicaid_rate DOUBLE,
            rate_source VARCHAR
        )
    """)

    fs_total = 0
    for st in FEE_SCHEDULE_STATES:
        fs_dir = FACT_DIR / f"fee_schedule_{st.lower()}"
        # Find data file
        fs_file = fs_dir / "data.parquet"
        if not fs_file.exists():
            # Try snapshot pattern
            snaps = list(fs_dir.glob("snapshot=*/data.parquet"))
            if snaps:
                fs_file = snaps[0]
            else:
                print(f"  {st}: SKIPPED (no data.parquet)")
                continue

        try:
            # All fee schedule tables have: state_code, procedure_code, medicaid_rate
            # Some have modifier (may be VARCHAR or INTEGER), some don't
            col_info = {r[0]: r[1] for r in con.execute(
                f"DESCRIBE SELECT * FROM read_parquet('{fs_file}')"
            ).fetchall()}

            has_modifier = "modifier" in col_info

            if has_modifier:
                # modifier may be INTEGER in some tables - cast to VARCHAR
                mod_type = col_info["modifier"]
                if "INT" in mod_type.upper():
                    mod_expr = "COALESCE(CAST(modifier AS VARCHAR), '')"
                else:
                    mod_expr = "COALESCE(modifier, '')"

                con.execute(f"""
                    INSERT INTO _fs_rates
                    SELECT
                        state_code,
                        procedure_code,
                        {mod_expr} AS modifier,
                        medicaid_rate,
                        'published_state_fs' AS rate_source
                    FROM read_parquet('{fs_file}')
                    WHERE medicaid_rate IS NOT NULL AND medicaid_rate > 0
                """)
            else:
                con.execute(f"""
                    INSERT INTO _fs_rates
                    SELECT
                        state_code,
                        procedure_code,
                        '' AS modifier,
                        medicaid_rate,
                        'published_state_fs' AS rate_source
                    FROM read_parquet('{fs_file}')
                    WHERE medicaid_rate IS NOT NULL AND medicaid_rate > 0
                """)

            fs_count = con.execute(f"""
                SELECT COUNT(*) FROM read_parquet('{fs_file}')
                WHERE medicaid_rate IS NOT NULL AND medicaid_rate > 0
            """).fetchone()[0]
            fs_total += fs_count
            print(f"  {st}: {fs_count:>8,} rows")

        except Exception as e:
            print(f"  {st}: ERROR - {e}")

    # Deduplicate fee schedule rates
    con.execute("""
        CREATE TABLE _fs_rates_dedup AS
        SELECT
            state_code,
            procedure_code,
            modifier,
            MAX(medicaid_rate) AS medicaid_rate,
            'published_state_fs' AS rate_source
        FROM _fs_rates
        GROUP BY state_code, procedure_code, modifier
    """)
    con.execute("DROP TABLE _fs_rates")

    fs_dedup = con.execute("SELECT COUNT(*) FROM _fs_rates_dedup").fetchone()[0]
    print(f"  Total fee schedule rows: {fs_total:,} raw, {fs_dedup:,} after dedup")
    print()

    # ------------------------------------------------------------------
    # 4. Merge: fee schedule takes priority over fact_medicaid_rate
    #    (fee schedules are state-published originals; fact_medicaid_rate
    #     is the same data scraped into a unified table, but fee_schedule
    #     tables may cover MORE codes for those 18 states)
    # ------------------------------------------------------------------
    print("Step 3: Merging published rates (fee_schedule > fact_medicaid_rate)...")

    con.execute("""
        CREATE TABLE _all_published AS
        -- Start with fee schedule rates (higher priority)
        SELECT state_code, procedure_code, modifier, medicaid_rate, rate_source
        FROM _fs_rates_dedup

        UNION ALL

        -- Add fact_medicaid_rate rows that are NOT already covered by fee schedules
        SELECT
            m.state_code, m.procedure_code, m.modifier, m.medicaid_rate, m.rate_source
        FROM _medicaid_rates m
        LEFT JOIN _fs_rates_dedup f
            ON m.state_code = f.state_code
            AND m.procedure_code = f.procedure_code
            AND m.modifier = f.modifier
        WHERE f.state_code IS NULL
    """)

    # Final dedup (in case of overlap)
    con.execute("""
        CREATE TABLE _published_merged AS
        SELECT
            state_code,
            procedure_code,
            modifier,
            MAX(medicaid_rate) AS medicaid_rate,
            -- Prefer published_state_fs source label when both exist
            MAX(rate_source) AS rate_source
        FROM _all_published
        GROUP BY state_code, procedure_code, modifier
    """)
    con.execute("DROP TABLE _all_published")
    con.execute("DROP TABLE _fs_rates_dedup")
    con.execute("DROP TABLE _medicaid_rates")

    pub_count = con.execute("SELECT COUNT(*) FROM _published_merged").fetchone()[0]
    pub_states = con.execute("SELECT COUNT(DISTINCT state_code) FROM _published_merged").fetchone()[0]
    print(f"  Merged published rates: {pub_count:,} rows, {pub_states} states")

    # Source breakdown
    src_counts = con.execute("""
        SELECT rate_source, COUNT(*) c
        FROM _published_merged
        GROUP BY rate_source ORDER BY c DESC
    """).fetchall()
    for s, c in src_counts:
        print(f"    {s:30s} {c:>10,}")
    print()

    # ------------------------------------------------------------------
    # 5. Derive corrected conversion factors from published E/M rates
    # ------------------------------------------------------------------
    print("Step 4: Deriving corrected conversion factors from published rates...")

    con.execute("""
        CREATE TABLE _cf_derived AS
        SELECT
            p.state_code,
            MEDIAN(p.medicaid_rate / dp.total_rvu_nonfac) AS derived_cf
        FROM _published_merged p
        JOIN dim_procedure dp
            ON p.procedure_code = dp.procedure_code
        WHERE dp.is_em_code = true
          AND dp.total_rvu_nonfac IS NOT NULL
          AND dp.total_rvu_nonfac > 0
          AND p.medicaid_rate > 0
          AND p.modifier = ''
        GROUP BY p.state_code
        HAVING COUNT(*) >= 3
    """)

    cf_count = con.execute("SELECT COUNT(*) FROM _cf_derived").fetchone()[0]
    print(f"  Derived CFs for {cf_count} states (from E/M codes)")

    # Show a sample
    cf_sample = con.execute("""
        SELECT state_code, ROUND(derived_cf, 4) AS cf
        FROM _cf_derived
        ORDER BY state_code
        LIMIT 10
    """).fetchall()
    for st, cf in cf_sample:
        print(f"    {st}: ${cf}")
    if cf_count > 10:
        print(f"    ... and {cf_count - 10} more")
    print()

    # ------------------------------------------------------------------
    # 6. Identify states that need CF × RVU fallback
    # ------------------------------------------------------------------
    print("Step 5: Computing CF x RVU fallback for states without published rates...")

    # States in dim_state that have NO published rates at all
    con.execute("""
        CREATE TABLE _states_needing_fallback AS
        SELECT ds.state_code
        FROM dim_state ds
        LEFT JOIN (
            SELECT DISTINCT state_code FROM _published_merged
        ) pub ON ds.state_code = pub.state_code
        WHERE pub.state_code IS NULL
          AND ds.state_code NOT IN ('AS', 'GU', 'MP', 'PR', 'VI', 'US')
    """)

    fallback_states = [r[0] for r in con.execute(
        "SELECT state_code FROM _states_needing_fallback ORDER BY state_code"
    ).fetchall()]
    print(f"  States needing CF x RVU fallback ({len(fallback_states)}): {fallback_states}")

    # For fallback states, use dim_state.conversion_factor (if available)
    # or the median derived CF across all states
    median_cf = con.execute("SELECT MEDIAN(derived_cf) FROM _cf_derived").fetchone()[0]
    print(f"  Median derived CF (fallback): ${median_cf:.4f}" if median_cf else "  No median CF available")

    if fallback_states and median_cf:
        con.execute(f"""
            CREATE TABLE _fallback_rates AS
            SELECT
                fb.state_code,
                dp.procedure_code,
                '' AS modifier,
                ROUND(
                    COALESCE(ds.conversion_factor, cd.derived_cf, {median_cf}) * dp.total_rvu_nonfac,
                    2
                ) AS medicaid_rate,
                'cf_x_rvu_computed' AS rate_source
            FROM _states_needing_fallback fb
            CROSS JOIN dim_procedure dp
            LEFT JOIN dim_state ds ON fb.state_code = ds.state_code
            LEFT JOIN _cf_derived cd ON fb.state_code = cd.state_code
            WHERE dp.total_rvu_nonfac IS NOT NULL
              AND dp.total_rvu_nonfac > 0
              AND dp.medicare_rate_nonfac IS NOT NULL
              AND dp.medicare_rate_nonfac > 0
        """)
        fb_count = con.execute("SELECT COUNT(*) FROM _fallback_rates").fetchone()[0]
        fb_states = con.execute("SELECT COUNT(DISTINCT state_code) FROM _fallback_rates").fetchone()[0]
        print(f"  CF x RVU fallback: {fb_count:,} rows, {fb_states} states")
    else:
        con.execute("""
            CREATE TABLE _fallback_rates (
                state_code VARCHAR, procedure_code VARCHAR,
                modifier VARCHAR, medicaid_rate DOUBLE, rate_source VARCHAR
            )
        """)
        print("  No fallback rates needed (all states have published data)")
    print()

    # ------------------------------------------------------------------
    # 7. Combine all rates and join with dim_procedure for Medicare rates
    # ------------------------------------------------------------------
    print("Step 6: Building final rate_comparison_v2...")

    con.execute(f"""
        CREATE TABLE _combined AS
        SELECT state_code, procedure_code, modifier, medicaid_rate, rate_source
        FROM _published_merged

        UNION ALL

        SELECT state_code, procedure_code, modifier, medicaid_rate, rate_source
        FROM _fallback_rates
    """)

    # Join with dim_procedure for Medicare rate, em_category, and compute pct_of_medicare
    con.execute(f"""
        CREATE TABLE _rate_comparison_v2_raw AS
        SELECT
            c.state_code,
            c.procedure_code,
            c.modifier,
            ROUND(c.medicaid_rate, 2) AS medicaid_rate,
            ROUND(dp.medicare_rate_nonfac, 2) AS medicare_rate,
            CASE
                WHEN dp.medicare_rate_nonfac IS NOT NULL AND dp.medicare_rate_nonfac > 0
                THEN ROUND(c.medicaid_rate / dp.medicare_rate_nonfac * 100, 1)
                ELSE NULL
            END AS pct_of_medicare,
            dp.em_category,
            c.rate_source,
            DATE '{SNAPSHOT_DATE}' AS snapshot_date
        FROM _combined c
        LEFT JOIN dim_procedure dp
            ON c.procedure_code = dp.procedure_code
    """)

    raw_count = con.execute("SELECT COUNT(*) FROM _rate_comparison_v2_raw").fetchone()[0]
    print(f"  Raw combined: {raw_count:,} rows")

    # ------------------------------------------------------------------
    # 8. Filter out bad data
    # ------------------------------------------------------------------
    print("Step 7: Filtering bad rates...")

    # Cap pct_of_medicare at 500% — no legitimate physician rate is 5x Medicare.
    # Previous cap of 2000% was too permissive and let APC/facility rates through.
    con.execute("""
        CREATE TABLE fact_rate_comparison_v2 AS
        SELECT *
        FROM _rate_comparison_v2_raw
        WHERE medicaid_rate > 0
          AND (pct_of_medicare IS NULL OR pct_of_medicare <= 500)
    """)

    filtered_count = con.execute("SELECT COUNT(*) FROM fact_rate_comparison_v2").fetchone()[0]
    removed = raw_count - filtered_count
    print(f"  Removed {removed:,} rows (rate <= 0 or pct_of_medicare > 2000%)")
    print(f"  Final: {filtered_count:,} rows")
    print()

    # ------------------------------------------------------------------
    # 9. Summary stats
    # ------------------------------------------------------------------
    print("=" * 70)
    print("SUMMARY STATS")
    print("=" * 70)

    total = con.execute("SELECT COUNT(*) FROM fact_rate_comparison_v2").fetchone()[0]
    states = con.execute("SELECT COUNT(DISTINCT state_code) FROM fact_rate_comparison_v2").fetchone()[0]
    print(f"Total rows:     {total:,}")
    print(f"States covered: {states}")

    # By source
    print("\nRows by source:")
    sources = con.execute("""
        SELECT rate_source, COUNT(*) c, ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 1) AS pct
        FROM fact_rate_comparison_v2
        GROUP BY rate_source ORDER BY c DESC
    """).fetchall()
    for src, cnt, pct in sources:
        print(f"  {src:30s} {cnt:>10,} ({pct}%)")

    # Median pct_of_medicare overall and by source
    print("\nMedian pct_of_medicare:")
    overall = con.execute("""
        SELECT MEDIAN(pct_of_medicare)
        FROM fact_rate_comparison_v2
        WHERE pct_of_medicare IS NOT NULL
    """).fetchone()[0]
    print(f"  Overall: {overall:.1f}%")

    by_source = con.execute("""
        SELECT rate_source, MEDIAN(pct_of_medicare)
        FROM fact_rate_comparison_v2
        WHERE pct_of_medicare IS NOT NULL
        GROUP BY rate_source ORDER BY rate_source
    """).fetchall()
    for src, med in by_source:
        if med is not None:
            print(f"  {src:30s} {med:.1f}%")

    # Per-state medians for previously-contaminated states
    print("\nPer-state median pct_of_medicare (formerly contaminated states):")
    problem_states = con.execute("""
        SELECT state_code,
            ROUND(MEDIAN(pct_of_medicare), 1) AS median_pct,
            COUNT(*) AS rows,
            ROUND(MEDIAN(medicaid_rate), 2) AS median_rate
        FROM fact_rate_comparison_v2
        WHERE state_code IN ('RI', 'CT', 'OK', 'DE', 'NE', 'AL', 'KY')
          AND pct_of_medicare IS NOT NULL
        GROUP BY state_code
        ORDER BY state_code
    """).fetchall()
    for st, med, rows, rate in problem_states:
        print(f"  {st}: {med:.1f}% of Medicare ({rows:,} rows, median rate ${rate})")

    # E/M coverage
    em_count = con.execute("""
        SELECT COUNT(*) FROM fact_rate_comparison_v2 WHERE em_category IS NOT NULL
    """).fetchone()[0]
    print(f"\nE/M category rows: {em_count:,}")

    # States with published vs computed
    print("\nPer-state row counts (top 10):")
    state_rows = con.execute("""
        SELECT state_code, COUNT(*) c,
            SUM(CASE WHEN rate_source IN ('published_direct', 'published_state_fs') THEN 1 ELSE 0 END) AS published,
            SUM(CASE WHEN rate_source = 'cf_x_rvu_computed' THEN 1 ELSE 0 END) AS computed
        FROM fact_rate_comparison_v2
        GROUP BY state_code ORDER BY c DESC LIMIT 10
    """).fetchall()
    for st, c, pub, comp in state_rows:
        print(f"  {st}: {c:>8,} total ({pub:>7,} published, {comp:>7,} computed)")

    # ------------------------------------------------------------------
    # 10. Write output
    # ------------------------------------------------------------------
    if not args.dry_run:
        print("\n" + "=" * 70)
        print("WRITING OUTPUT")
        print("=" * 70)

        # Write fact_rate_comparison_v2
        out_dir = FACT_DIR / "rate_comparison_v2"
        out_dir.mkdir(parents=True, exist_ok=True)
        out_file = out_dir / "data.parquet"

        con.execute(f"""
            COPY fact_rate_comparison_v2 TO '{out_file}'
            (FORMAT PARQUET, COMPRESSION ZSTD)
        """)
        size_kb = out_file.stat().st_size / 1024
        print(f"  Wrote {out_file} ({size_kb:.1f} KB, {filtered_count:,} rows)")

        # Update dim_state with corrected conversion factors
        cf_updates = con.execute("""
            SELECT state_code, ROUND(derived_cf, 4) AS derived_cf
            FROM _cf_derived
        """).fetchall()

        if cf_updates:
            print(f"\n  Updating dim_state conversion factors ({len(cf_updates)} states)...")

            # Update in-memory dim_state table
            for st, cf in cf_updates:
                con.execute(f"""
                    UPDATE dim_state
                    SET conversion_factor = {cf}
                    WHERE state_code = '{st}'
                """)

            # Write updated dim_state
            con.execute(f"""
                COPY dim_state TO '{dim_state_path}'
                (FORMAT PARQUET, COMPRESSION ZSTD)
            """)
            ds_size = dim_state_path.stat().st_size / 1024
            print(f"  Wrote {dim_state_path} ({ds_size:.1f} KB)")

            # Show CF changes
            print("\n  Updated conversion factors:")
            cf_updates_sorted = sorted(cf_updates, key=lambda x: x[0])
            for st, cf in cf_updates_sorted[:15]:
                print(f"    {st}: ${cf}")
            if len(cf_updates_sorted) > 15:
                print(f"    ... and {len(cf_updates_sorted) - 15} more")
    else:
        print("\n[DRY RUN] No files written.")

    # Cleanup
    con.execute("DROP TABLE IF EXISTS _medicaid_rate_raw")
    con.execute("DROP TABLE IF EXISTS _medicaid_rates")
    con.execute("DROP TABLE IF EXISTS _fs_rates")
    con.execute("DROP TABLE IF EXISTS _fs_rates_dedup")
    con.execute("DROP TABLE IF EXISTS _all_published")
    con.execute("DROP TABLE IF EXISTS _published_merged")
    con.execute("DROP TABLE IF EXISTS _cf_derived")
    con.execute("DROP TABLE IF EXISTS _states_needing_fallback")
    con.execute("DROP TABLE IF EXISTS _fallback_rates")
    con.execute("DROP TABLE IF EXISTS _combined")
    con.execute("DROP TABLE IF EXISTS _rate_comparison_v2_raw")
    con.execute("DROP TABLE IF EXISTS fact_rate_comparison_v2")
    con.execute("DROP TABLE IF EXISTS dim_procedure")
    con.execute("DROP TABLE IF EXISTS dim_state")
    con.close()

    print("\nDone.")


if __name__ == "__main__":
    main()
