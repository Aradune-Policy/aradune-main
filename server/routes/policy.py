"""Policy data routes — SPAs, waivers, managed care, FMAP, DSH."""

from fastapi import APIRouter, Query
from server.db import get_cursor
from server.utils.error_handler import safe_route

router = APIRouter()


@router.get("/api/policy/spas/{state_code}")
@safe_route(default_response={})
async def state_spas(state_code: str):
    """Get State Plan Amendments for a state."""
    state_code = state_code.upper()
    with get_cursor() as cur:
        rows = cur.execute("""
            SELECT spa_number, title, effective_date, approval_date,
                   topic, affects_rate_setting, pdf_url, summary,
                   conversion_factor, rvu_year, methodology_keywords
            FROM fact_spa
            WHERE state_code = $1
            ORDER BY approval_date DESC
        """, [state_code]).fetchall()
        columns = ["spa_number", "title", "effective_date", "approval_date",
                    "topic", "affects_rate_setting", "pdf_url", "summary",
                    "conversion_factor", "rvu_year", "methodology_keywords"]
        return [dict(zip(columns, r)) for r in rows]


@router.get("/api/policy/waivers/{state_code}")
@safe_route(default_response={})
async def state_waivers(state_code: str):
    """Get 1115 waivers for a state."""
    state_code = state_code.upper()
    with get_cursor() as cur:
        rows = cur.execute("""
            SELECT waiver_name, waiver_number, approval_date,
                   expiration_date, waiver_type, status, description
            FROM ref_1115_waivers
            WHERE state_code = $1
            ORDER BY waiver_number
        """, [state_code]).fetchall()
        columns = ["waiver_name", "waiver_number", "approval_date",
                    "expiration_date", "waiver_type", "status", "description"]
        return [dict(zip(columns, r)) for r in rows]


@router.get("/api/policy/managed-care/{state_code}")
@safe_route(default_response={})
async def managed_care(state_code: str, year: int = Query(None)):
    """Get managed care plan enrollment for a state."""
    state_code = state_code.upper()
    year_filter = "AND year = $2" if year else ""
    params = [state_code] + ([year] if year else [])

    with get_cursor() as cur:
        rows = cur.execute(f"""
            SELECT plan_name, plan_type, year, enrollment
            FROM fact_managed_care
            WHERE state_code = $1 {year_filter}
            ORDER BY year DESC, enrollment DESC
        """, params).fetchall()
        columns = ["plan_name", "plan_type", "year", "enrollment"]
        return [dict(zip(columns, r)) for r in rows]


@router.get("/api/policy/fmap")
@safe_route(default_response={})
async def fmap_rates():
    """Get FMAP for all states from fact_fmap_historical (MACPAC, authoritative)."""
    with get_cursor() as cur:
        rows = cur.execute("""
            WITH latest_fy AS (
                SELECT MAX(fiscal_year) AS fy FROM fact_fmap_historical WHERE rate_type = 'fmap'
            )
            SELECT
                f.state_code,
                f.fiscal_year,
                f.rate AS fmap_rate,
                e.rate AS efmap_rate
            FROM fact_fmap_historical f
            LEFT JOIN fact_fmap_historical e
                ON f.state_code = e.state_code
                AND e.rate_type = 'efmap'
                AND e.fiscal_year = f.fiscal_year
            WHERE f.rate_type = 'fmap'
              AND f.fiscal_year = (SELECT fy FROM latest_fy)
            ORDER BY f.state_code
        """).fetchall()
        columns = ["state_code", "fiscal_year", "fmap_rate", "efmap_rate"]
        return [dict(zip(columns, r)) for r in rows]


@router.get("/api/policy/dsh/{state_code}")
@safe_route(default_response={})
async def dsh_payments(state_code: str):
    """Get DSH payment data for a state."""
    state_code = state_code.upper()
    with get_cursor() as cur:
        rows = cur.execute("""
            SELECT year, hospital_name, dsh_payment, uncompensated_care_cost
            FROM fact_dsh_payment
            WHERE state_code = $1
            ORDER BY year DESC, dsh_payment DESC
        """, [state_code]).fetchall()
        columns = ["year", "hospital_name", "dsh_payment", "uncompensated_care_cost"]
        return [dict(zip(columns, r)) for r in rows]
