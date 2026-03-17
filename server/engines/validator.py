"""
Aradune Data Validation Engine (Layer 1)
Lightweight validation checks against the DuckDB lake.

Produces pass/fail results for key data quality assertions.
Future: migrate to Soda Core SodaCL format.
"""

import logging
from server.db import get_cursor

logger = logging.getLogger("aradune.validator")


def run_core_checks() -> list[dict]:
    """Run the core validation checks and return results."""
    results = []
    try:
        with get_cursor() as cur:
            checks = [
                # Row count checks
                ("fact_rate_comparison", "row_count", ">=", 300000),
                ("fact_enrollment", "row_count", ">=", 5000),
                ("fact_five_star", "row_count", ">=", 14000),
                ("fact_cms64_multiyear", "row_count", ">=", 100000),
                ("fact_sdud_2025", "row_count", ">=", 2000000),
                ("fact_quality_core_set_2024", "row_count", ">=", 5000),
                ("dim_state", "row_count", ">=", 51),
                ("fact_hpsa", "row_count", ">=", 60000),
                ("fact_mco_mlr", "row_count", ">=", 2000),
                ("fact_nadac", "row_count", ">=", 1500000),
            ]
            for table, check_type, op, expected in checks:
                try:
                    count = cur.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
                    passed = count >= expected
                    results.append({
                        "table": table, "check": f"row_count {op} {expected}",
                        "passed": passed, "actual": count, "expected": f"{op} {expected}",
                    })
                except Exception as e:
                    results.append({
                        "table": table, "check": f"row_count {op} {expected}",
                        "passed": False, "actual": f"ERROR: {e}", "expected": f"{op} {expected}",
                    })

            # Range checks
            range_checks = [
                ("dim_state", "fmap", 0.5, 0.83),
                ("fact_rate_comparison", "pct_of_medicare", 1, 1000),
                ("fact_mc_enrollment_summary", "mc_penetration_pct", 0, 100),
            ]
            for table, col, min_v, max_v in range_checks:
                try:
                    r = cur.execute(f"""
                        SELECT MIN({col}), MAX({col}),
                               COUNT(*) FILTER (WHERE {col} < {min_v} OR {col} > {max_v})
                        FROM {table} WHERE {col} IS NOT NULL
                    """).fetchone()
                    results.append({
                        "table": table, "check": f"{col} in [{min_v}, {max_v}]",
                        "passed": r[2] == 0, "actual": f"range [{r[0]}, {r[1]}], {r[2]} violations",
                        "expected": f"[{min_v}, {max_v}]",
                    })
                except Exception as e:
                    results.append({
                        "table": table, "check": f"{col} in [{min_v}, {max_v}]",
                        "passed": False, "actual": str(e)[:100], "expected": f"[{min_v}, {max_v}]",
                    })

            # Referential integrity
            ri_checks = [
                ("fact_rate_comparison", "state_code", "dim_state", "state_code"),
                ("fact_enrollment", "state_code", "dim_state", "state_code"),
            ]
            for table, col, ref_table, ref_col in ri_checks:
                try:
                    orphans = cur.execute(f"""
                        SELECT COUNT(DISTINCT t.{col})
                        FROM {table} t LEFT JOIN {ref_table} r ON t.{col} = r.{ref_col}
                        WHERE t.{col} IS NOT NULL AND r.{ref_col} IS NULL
                    """).fetchone()[0]
                    results.append({
                        "table": table, "check": f"{col} -> {ref_table}.{ref_col}",
                        "passed": orphans == 0, "actual": f"{orphans} orphans",
                        "expected": "0 orphans",
                    })
                except Exception as e:
                    results.append({
                        "table": table, "check": f"{col} -> {ref_table}.{ref_col}",
                        "passed": False, "actual": str(e)[:100], "expected": "0 orphans",
                    })

    except Exception as e:
        results.append({"table": "SYSTEM", "check": "db_connection", "passed": False,
                        "actual": str(e), "expected": "connected"})

    return results
