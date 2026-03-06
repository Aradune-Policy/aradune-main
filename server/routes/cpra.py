"""CPRA (Comparative Payment Rate Analysis) API routes.

Serves rate comparison data directly from the data lake views.
"""

from fastapi import APIRouter, HTTPException, Query
from server.db import get_cursor

router = APIRouter()


@router.get("/api/cpra/states")
async def cpra_states():
    """List all states with CPRA rate comparison data."""
    with get_cursor() as cur:
        rows = cur.execute("""
            SELECT
                state_code,
                COUNT(*) AS total_codes,
                SUM(CASE WHEN em_category IS NOT NULL THEN 1 ELSE 0 END) AS em_codes,
                ROUND(MEDIAN(pct_of_medicare), 2) AS median_pct,
                ROUND(AVG(pct_of_medicare), 2) AS avg_pct
            FROM fact_rate_comparison
            WHERE pct_of_medicare > 0 AND pct_of_medicare < 1000
            GROUP BY state_code
            ORDER BY state_code
        """).fetchall()
        columns = ["state_code", "total_codes", "em_codes", "median_pct", "avg_pct"]
        return [dict(zip(columns, r)) for r in rows]


@router.get("/api/cpra/rates/{state_code}")
async def cpra_rates(
    state_code: str,
    em_only: bool = Query(False, description="Filter to E/M codes only"),
):
    """Get rate comparison data for a specific state."""
    state_code = state_code.upper()
    if len(state_code) != 2:
        raise HTTPException(400, "state_code must be 2-letter abbreviation")

    em_filter = "AND rc.em_category IS NOT NULL" if em_only else ""
    with get_cursor() as cur:
        rows = cur.execute(f"""
            SELECT
                rc.procedure_code,
                rc.modifier,
                rc.medicaid_rate,
                rc.medicare_nonfac_rate,
                rc.medicare_fac_rate,
                rc.pct_of_medicare,
                rc.em_category,
                COALESCE(rc.category, dp.category) AS category,
                dp.description,
                rc.medicaid_rate_date
            FROM fact_rate_comparison rc
            LEFT JOIN dim_procedure dp ON rc.procedure_code = dp.procedure_code
            WHERE rc.state_code = $1 {em_filter}
            ORDER BY rc.pct_of_medicare ASC
        """, [state_code]).fetchall()

        columns = [
            "procedure_code", "modifier", "medicaid_rate",
            "medicare_nonfac_rate", "medicare_fac_rate", "pct_of_medicare",
            "em_category", "category", "description", "medicaid_rate_date",
        ]
        return [dict(zip(columns, r)) for r in rows]


@router.get("/api/cpra/dq/{state_code}")
async def cpra_dq_flags(state_code: str):
    """Get data quality flags for a specific state."""
    state_code = state_code.upper()
    with get_cursor() as cur:
        rows = cur.execute("""
            SELECT flag_type, severity, entity_type, entity_id, detail
            FROM fact_dq_flag
            WHERE state_code = $1
            ORDER BY severity DESC, flag_type
        """, [state_code]).fetchall()

        columns = ["flag_type", "severity", "entity_type", "entity_id", "detail"]
        return [dict(zip(columns, r)) for r in rows]


@router.get("/api/cpra/compare")
async def cpra_compare_codes(
    codes: str = Query(..., description="Comma-separated HCPCS codes"),
    states: str = Query(None, description="Comma-separated state codes (all if omitted)"),
):
    """Compare specific codes across states."""
    code_list = [c.strip() for c in codes.split(",")]
    placeholders = ", ".join(f"${i+1}" for i in range(len(code_list)))

    params = list(code_list)
    state_filter = ""
    if states:
        state_list = [s.strip().upper() for s in states.split(",")]
        state_placeholders = ", ".join(f"${len(params)+i+1}" for i in range(len(state_list)))
        state_filter = f"AND rc.state_code IN ({state_placeholders})"
        params.extend(state_list)

    with get_cursor() as cur:
        rows = cur.execute(f"""
            SELECT
                rc.state_code,
                rc.procedure_code,
                rc.medicaid_rate,
                rc.medicare_nonfac_rate,
                rc.pct_of_medicare,
                rc.em_category,
                dp.description,
                ds.state_name
            FROM fact_rate_comparison rc
            LEFT JOIN dim_procedure dp ON rc.procedure_code = dp.procedure_code
            LEFT JOIN dim_state ds ON rc.state_code = ds.state_code
            WHERE rc.procedure_code IN ({placeholders}) {state_filter}
            ORDER BY rc.procedure_code, rc.state_code
        """, params).fetchall()

        columns = [
            "state_code", "procedure_code", "medicaid_rate",
            "medicare_nonfac_rate", "pct_of_medicare", "em_category",
            "description", "state_name",
        ]
        return [dict(zip(columns, r)) for r in rows]
