"""BLS wage data routes — OEWS wages for Medicaid-relevant occupations."""

from fastapi import APIRouter, Query
from server.db import get_cursor
from collections import defaultdict

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


@router.get("/api/wages/bulk")
async def bulk_wages():
    """All state + national wages in nested format for frontend WageAdequacy tool."""
    states: dict = defaultdict(dict)
    national: dict = {}

    with get_cursor() as cur:
        # State-level
        rows = cur.execute("""
            SELECT state_code, soc_code, occupation_title,
                   total_employment, hourly_mean,
                   hourly_median, hourly_p10, hourly_p25, hourly_p75, hourly_p90,
                   annual_median
            FROM fact_bls_wage
            ORDER BY state_code, soc_code
        """).fetchall()
        for r in rows:
            sc, soc, title, emp, h_mean, h_med, h10, h25, h75, h90, a_med = r
            states[sc][soc] = {
                "title": title, "emp": emp, "h_mean": h_mean,
                "h_median": h_med, "h_p10": h10, "h_p25": h25,
                "h_p75": h75, "h_p90": h90, "a_median": a_med,
            }

        # National-level
        nrows = cur.execute("""
            SELECT soc_code, occupation_title,
                   total_employment, hourly_mean,
                   hourly_median, hourly_p10, hourly_p90, annual_median
            FROM fact_bls_wage_national
            ORDER BY soc_code
        """).fetchall()
        for r in nrows:
            soc, title, emp, h_mean, h_med, h10, h90, a_med = r
            national[soc] = {
                "title": title, "emp": emp, "h_mean": h_mean,
                "h_median": h_med, "h_p10": h10, "h_p90": h90,
                "a_median": a_med,
            }

    return {
        "source": "BLS Occupational Employment and Wage Statistics (OEWS)",
        "states": dict(states),
        "national": national,
    }


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


# ── Workforce Supply ─────────────────────────────────────────────────────

@router.get("/api/workforce/projections")
async def workforce_projections(state: str = None):
    """HRSA workforce supply/demand projections 2023-2038.

    Filter by full state name (e.g. ?state=Florida).
    """
    with get_cursor() as cur:
        if state:
            rows = cur.execute("""
                SELECT year, profession_group, profession, state, rurality,
                       supply_fte, demand_fte, pct_adequacy, region
                FROM fact_workforce_projections
                WHERE state = $1
                ORDER BY year, profession
            """, [state]).fetchall()
        else:
            rows = cur.execute("""
                SELECT year, profession_group, profession, state, rurality,
                       supply_fte, demand_fte, pct_adequacy, region
                FROM fact_workforce_projections
                ORDER BY year, profession
                LIMIT 1000
            """).fetchall()
        columns = ["year", "profession_group", "profession", "state", "rurality",
                    "supply_fte", "demand_fte", "pct_adequacy", "region"]
        data = [dict(zip(columns, r)) for r in rows]
        return {"rows": data, "count": len(data)}


@router.get("/api/workforce/projections/summary")
async def workforce_projections_summary():
    """State-level workforce projection summary."""
    with get_cursor() as cur:
        rows = cur.execute("""
            SELECT state,
                   COUNT(DISTINCT profession) AS occupations,
                   MIN(year) AS min_year,
                   MAX(year) AS max_year,
                   COUNT(*) AS records
            FROM fact_workforce_projections
            GROUP BY state
            ORDER BY state
        """).fetchall()
        columns = ["state", "occupations", "min_year", "max_year", "records"]
        data = [dict(zip(columns, r)) for r in rows]
        return {"rows": data, "count": len(data)}


@router.get("/api/workforce/nursing")
async def nursing_workforce(state_name: str = None):
    """NSSRN nursing workforce demographics by state.

    Filter by full state name (e.g. ?state_name=Florida).
    """
    with get_cursor() as cur:
        if state_name:
            rows = cur.execute("""
                SELECT license_type, state_name, status, age, sex,
                       race_ethnicity, veteran_status, languages, weighted_count
                FROM fact_nursing_workforce
                WHERE state_name = $1
                ORDER BY license_type, status
            """, [state_name]).fetchall()
        else:
            rows = cur.execute("""
                SELECT license_type, state_name, status, age, sex,
                       race_ethnicity, veteran_status, languages, weighted_count
                FROM fact_nursing_workforce
                LIMIT 500
            """).fetchall()
        columns = ["license_type", "state_name", "status", "age", "sex",
                    "race_ethnicity", "veteran_status", "languages", "weighted_count"]
        data = [dict(zip(columns, r)) for r in rows]
        return {"rows": data, "count": len(data)}


@router.get("/api/workforce/nhsc")
async def nhsc_field_strength(state_code: str = None):
    """NHSC clinician counts by state and discipline."""
    with get_cursor() as cur:
        if state_code:
            rows = cur.execute("""
                SELECT state_name, discipline, fiscal_year, total_clinicians,
                       nhsc_lrp, nhsc_sud_lrp, nhsc_rc_lrp, nhsc_sp,
                       s2s_lrp, slrp, non_rural, rural
                FROM fact_nhsc_field_strength
                WHERE state_name = $1
            """, [state_code.upper()]).fetchall()
        else:
            rows = cur.execute("""
                SELECT state_name, discipline, fiscal_year, total_clinicians,
                       nhsc_lrp, nhsc_sud_lrp, nhsc_rc_lrp, nhsc_sp,
                       s2s_lrp, slrp, non_rural, rural
                FROM fact_nhsc_field_strength
                ORDER BY state_name
            """).fetchall()
        columns = ["state_name", "discipline", "fiscal_year", "total_clinicians",
                    "nhsc_lrp", "nhsc_sud_lrp", "nhsc_rc_lrp", "nhsc_sp",
                    "s2s_lrp", "slrp", "non_rural", "rural"]
        data = [dict(zip(columns, r)) for r in rows]
        return {"rows": data, "count": len(data)}
