"""BLS wage data routes — OEWS wages for Medicaid-relevant occupations."""

from fastapi import APIRouter, Query
from server.db import get_cursor

router = APIRouter()


@router.get("/api/wages/{state_code}")
async def state_wages(state_code: str):
    """Get BLS OEWS wages for Medicaid occupations in a state."""
    state_code = state_code.upper()
    with get_cursor() as cur:
        rows = cur.execute("""
            SELECT soc_code, occupation_title, data_year,
                   total_employment, hourly_mean, annual_mean,
                   hourly_median, annual_median,
                   hourly_p10, hourly_p25, hourly_p75, hourly_p90,
                   annual_p10, annual_p25, annual_p75, annual_p90,
                   jobs_per_1000, location_quotient
            FROM fact_bls_wage
            WHERE state_code = $1
            ORDER BY soc_code
        """, [state_code]).fetchall()
        columns = ["soc_code", "occupation_title", "data_year",
                    "total_employment", "hourly_mean", "annual_mean",
                    "hourly_median", "annual_median",
                    "hourly_p10", "hourly_p25", "hourly_p75", "hourly_p90",
                    "annual_p10", "annual_p25", "annual_p75", "annual_p90",
                    "jobs_per_1000", "location_quotient"]
        return [dict(zip(columns, r)) for r in rows]


@router.get("/api/wages/compare/{soc_code}")
async def wage_comparison(soc_code: str):
    """Compare wages for a specific occupation across all states."""
    with get_cursor() as cur:
        rows = cur.execute("""
            SELECT state_code, occupation_title, data_year,
                   total_employment, hourly_mean, annual_mean,
                   hourly_median, annual_median, location_quotient
            FROM fact_bls_wage
            WHERE soc_code = $1
            ORDER BY hourly_median DESC
        """, [soc_code]).fetchall()
        columns = ["state_code", "occupation_title", "data_year",
                    "total_employment", "hourly_mean", "annual_mean",
                    "hourly_median", "annual_median", "location_quotient"]
        return [dict(zip(columns, r)) for r in rows]


@router.get("/api/wages/msa/{state_code}")
async def msa_wages(
    state_code: str,
    soc_code: str = Query(None, description="Filter by SOC code"),
):
    """Get MSA-level wages for Medicaid occupations in a state."""
    state_code = state_code.upper()
    soc_filter = "AND soc_code = $2" if soc_code else ""
    params = [state_code] + ([soc_code] if soc_code else [])

    with get_cursor() as cur:
        rows = cur.execute(f"""
            SELECT msa_code, msa_title, soc_code, occupation_title,
                   total_employment, hourly_mean, annual_mean,
                   hourly_median, annual_median
            FROM fact_bls_wage_msa
            WHERE state_code = $1 {soc_filter}
            ORDER BY msa_title, soc_code
        """, params).fetchall()
        columns = ["msa_code", "msa_title", "soc_code", "occupation_title",
                    "total_employment", "hourly_mean", "annual_mean",
                    "hourly_median", "annual_median"]
        return [dict(zip(columns, r)) for r in rows]


@router.get("/api/wages/national")
async def national_wages(
    search: str = Query(None, description="Search occupation title"),
    limit: int = Query(100, le=1000),
):
    """Get national-level wages for all occupations."""
    filters = []
    params = []
    idx = 1

    if search:
        filters.append(f"occupation_title ILIKE ${idx}")
        params.append(f"%{search}%")
        idx += 1

    where = "WHERE " + " AND ".join(filters) if filters else ""

    with get_cursor() as cur:
        rows = cur.execute(f"""
            SELECT soc_code, occupation_title, occ_group,
                   total_employment, hourly_mean, annual_mean,
                   hourly_median, annual_median,
                   hourly_p10, hourly_p90, annual_p10, annual_p90
            FROM fact_bls_wage_national
            {where}
            ORDER BY annual_median DESC
            LIMIT {limit}
        """, params).fetchall()
        columns = ["soc_code", "occupation_title", "occ_group",
                    "total_employment", "hourly_mean", "annual_mean",
                    "hourly_median", "annual_median",
                    "hourly_p10", "hourly_p90", "annual_p10", "annual_p90"]
        return [dict(zip(columns, r)) for r in rows]
