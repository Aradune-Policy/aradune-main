"""Contextual data routes — Census, CDC, economic indicators, HPSA, Scorecard."""

from fastapi import APIRouter, Query
from server.db import get_cursor
from server.utils.error_handler import safe_route

router = APIRouter()


@router.get("/api/demographics/{state_code}")
@safe_route(default_response={})
async def demographics(state_code: str):
    """Get Census ACS demographics for a state."""
    state_code = state_code.upper()
    with get_cursor() as cur:
        rows = cur.execute("""
            SELECT state_code, state_name, total_population,
                   male_population, female_population,
                   white_alone, black_alone, hispanic_latino,
                   pct_poverty, pct_poverty_under18, pct_poverty_under5,
                   pct_poverty_65plus, pct_uninsured,
                   children_in_households, data_year
            FROM fact_acs_state
            WHERE state_code = $1
        """, [state_code]).fetchall()
        columns = [
            "state_code", "state_name", "total_population",
            "male_population", "female_population",
            "white_alone", "black_alone", "hispanic_latino",
            "pct_poverty", "pct_poverty_under18", "pct_poverty_under5",
            "pct_poverty_65plus", "pct_uninsured",
            "children_in_households", "data_year",
        ]
        return [dict(zip(columns, r)) for r in rows]


@router.get("/api/demographics")
@safe_route(default_response={})
async def all_demographics():
    """Get Census ACS demographics for all states."""
    with get_cursor() as cur:
        rows = cur.execute("""
            SELECT state_code, state_name, total_population,
                   pct_poverty, pct_uninsured, data_year
            FROM fact_acs_state
            ORDER BY state_code
        """).fetchall()
        columns = ["state_code", "state_name", "total_population",
                    "pct_poverty", "pct_uninsured", "data_year"]
        return [dict(zip(columns, r)) for r in rows]


@router.get("/api/scorecard/{state_code}")
@safe_route(default_response={})
async def scorecard_by_state(state_code: str):
    """Get Medicaid Scorecard measures for a state."""
    state_code = state_code.upper()
    with get_cursor() as cur:
        rows = cur.execute("""
            SELECT measure_id, data_period, population,
                   measure_value, median_value, mean_value,
                   strat_tier1_label, strat_tier1_value
            FROM fact_scorecard
            WHERE state_code = $1
            ORDER BY measure_id
        """, [state_code]).fetchall()
        columns = [
            "measure_id", "data_period", "population",
            "measure_value", "median_value", "mean_value",
            "strat_tier1_label", "strat_tier1_value",
        ]
        return [dict(zip(columns, r)) for r in rows]


@router.get("/api/economic/{state_code}")
@safe_route(default_response={})
async def economic_indicators(state_code: str):
    """Get economic indicators (GDP, population, unemployment, income) for a state."""
    state_code = state_code.upper()
    result = {}

    with get_cursor() as cur:
        # Unemployment
        rows = cur.execute("""
            SELECT observation_date, unemployment_rate
            FROM fact_unemployment
            WHERE state_code = $1
            ORDER BY observation_date DESC
            LIMIT 24
        """, [state_code]).fetchall()
        result["unemployment"] = [{"date": str(r[0]), "rate": r[1]} for r in rows]

        # Median income
        rows = cur.execute("""
            SELECT observation_date, median_household_income
            FROM fact_median_income
            WHERE state_code = $1
            ORDER BY observation_date DESC
        """, [state_code]).fetchall()
        result["median_income"] = [{"date": str(r[0]), "income": r[1]} for r in rows]

        # GDP
        rows = cur.execute("""
            SELECT observation_date, real_gdp_millions
            FROM fact_state_gdp
            WHERE state_code = $1
            ORDER BY observation_date DESC
        """, [state_code]).fetchall()
        result["gdp"] = [{"date": str(r[0]), "real_gdp_millions": r[1]} for r in rows]

        # Population
        rows = cur.execute("""
            SELECT observation_date, population_thousands
            FROM fact_state_population
            WHERE state_code = $1
            ORDER BY observation_date DESC
        """, [state_code]).fetchall()
        result["population"] = [{"date": str(r[0]), "pop_thousands": r[1]} for r in rows]

    return result


@router.get("/api/mortality/{state_code}")
@safe_route(default_response={})
async def mortality(state_code: str):
    """Get CDC mortality trends for a state (uses state name matching)."""
    # mortality_trend uses state names not codes — need to map
    with get_cursor() as cur:
        rows = cur.execute("""
            SELECT year, cause_name, deaths, age_adjusted_death_rate
            FROM fact_mortality_trend
            WHERE state_name = (
                SELECT state_name FROM fact_acs_state WHERE state_code = $1
            )
            ORDER BY cause_name, year
        """, [state_code.upper()]).fetchall()
        columns = ["year", "cause_name", "deaths", "age_adjusted_death_rate"]
        return [dict(zip(columns, r)) for r in rows]


# ── HUD Fair Market Rents ────────────────────────────────────────────────

@router.get("/api/housing/{state_code}")
@safe_route(default_response={})
async def housing_costs(state_code: str):
    """Get HUD Fair Market Rents for a state."""
    state_code = state_code.upper()
    with get_cursor() as cur:
        rows = cur.execute("""
            SELECT county_name, hud_area_name, is_metro, population_2022,
                   fmr_efficiency, fmr_1br, fmr_2br, fmr_3br, fmr_4br
            FROM fact_fair_market_rent
            WHERE state_code = $1
            ORDER BY population_2022 DESC
        """, [state_code]).fetchall()
        columns = ["county_name", "hud_area_name", "is_metro", "population_2022",
                    "fmr_efficiency", "fmr_1br", "fmr_2br", "fmr_3br", "fmr_4br"]
        return [dict(zip(columns, r)) for r in rows]


@router.get("/api/housing")
@safe_route(default_response={})
async def housing_summary():
    """Get state-level average FMR summary."""
    with get_cursor() as cur:
        rows = cur.execute("""
            SELECT state_code,
                   COUNT(*) AS area_count,
                   SUM(population_2022) AS total_population,
                   ROUND(AVG(fmr_1br)) AS avg_fmr_1br,
                   ROUND(AVG(fmr_2br)) AS avg_fmr_2br,
                   MIN(fmr_2br) AS min_fmr_2br,
                   MAX(fmr_2br) AS max_fmr_2br
            FROM fact_fair_market_rent
            GROUP BY state_code
            ORDER BY avg_fmr_2br DESC
        """).fetchall()
        columns = ["state_code", "area_count", "total_population",
                    "avg_fmr_1br", "avg_fmr_2br", "min_fmr_2br", "max_fmr_2br"]
        return [dict(zip(columns, r)) for r in rows]


# ── SNAP Enrollment ──────────────────────────────────────────────────────

@router.get("/api/snap/{state_code}")
@safe_route(default_response={})
async def snap_by_state(state_code: str):
    """Get SNAP monthly participation and benefit cost for a state."""
    state_code = state_code.upper()
    with get_cursor() as cur:
        rows = cur.execute("""
            SELECT year, month, households, persons, benefit_cost
            FROM fact_snap_enrollment
            WHERE state_code = $1
            ORDER BY year, month
        """, [state_code]).fetchall()
        columns = ["year", "month", "households", "persons", "benefit_cost"]
        return [dict(zip(columns, r)) for r in rows]


@router.get("/api/snap")
@safe_route(default_response={})
async def snap_summary():
    """Get SNAP enrollment summary — latest month totals by state."""
    with get_cursor() as cur:
        rows = cur.execute("""
            SELECT state_code, year, month, households, persons, benefit_cost
            FROM fact_snap_enrollment
            WHERE (year, month) = (
                SELECT (year, month) FROM fact_snap_enrollment
                ORDER BY year DESC, month DESC LIMIT 1
            )
            ORDER BY persons DESC
        """).fetchall()
        columns = ["state_code", "year", "month", "households", "persons", "benefit_cost"]
        return [dict(zip(columns, r)) for r in rows]


# ── TANF Enrollment ──────────────────────────────────────────────────────

@router.get("/api/tanf/{state_code}")
@safe_route(default_response={})
async def tanf_by_state(state_code: str):
    """Get TANF monthly families and recipients for a state."""
    state_code = state_code.upper()
    with get_cursor() as cur:
        rows = cur.execute("""
            SELECT year, month, measure, value
            FROM fact_tanf_enrollment
            WHERE state_code = $1
            ORDER BY measure, year, month
        """, [state_code]).fetchall()
        columns = ["year", "month", "measure", "value"]
        return [dict(zip(columns, r)) for r in rows]


@router.get("/api/tanf")
@safe_route(default_response={})
async def tanf_summary():
    """Get TANF enrollment summary — latest month families and recipients by state."""
    with get_cursor() as cur:
        rows = cur.execute("""
            SELECT state_code, year, month, measure, value
            FROM fact_tanf_enrollment
            WHERE measure IN ('total_families', 'total_recipients')
            AND (year, month) = (
                SELECT (year, month) FROM fact_tanf_enrollment
                ORDER BY year DESC, month DESC LIMIT 1
            )
            ORDER BY state_code, measure
        """).fetchall()
        columns = ["state_code", "year", "month", "measure", "value"]
        return [dict(zip(columns, r)) for r in rows]


@router.get("/api/cross-program/{state_code}")
@safe_route(default_response={})
async def cross_program_enrollment(state_code: str):
    """Get Medicaid, SNAP, and TANF enrollment side-by-side for a state."""
    state_code = state_code.upper()
    result = {"state_code": state_code, "medicaid": [], "snap": [], "tanf": []}
    with get_cursor() as cur:
        # Medicaid enrollment
        try:
            rows = cur.execute("""
                SELECT year, month, total_enrollment
                FROM fact_enrollment
                WHERE state_code = $1
                ORDER BY year, month
            """, [state_code]).fetchall()
            result["medicaid"] = [{"year": r[0], "month": r[1], "enrollment": r[2]} for r in rows]
        except Exception:
            pass

        # SNAP
        try:
            rows = cur.execute("""
                SELECT year, month, persons
                FROM fact_snap_enrollment
                WHERE state_code = $1
                ORDER BY year, month
            """, [state_code]).fetchall()
            result["snap"] = [{"year": r[0], "month": r[1], "persons": r[2]} for r in rows]
        except Exception:
            pass

        # TANF
        try:
            rows = cur.execute("""
                SELECT year, month, value
                FROM fact_tanf_enrollment
                WHERE state_code = $1 AND measure = 'total_recipients'
                ORDER BY year, month
            """, [state_code]).fetchall()
            result["tanf"] = [{"year": r[0], "month": r[1], "recipients": r[2]} for r in rows]
        except Exception:
            pass

    return result


# ── Eligibility Processing (Renewals/Redeterminations) ────────────────

@router.get("/api/eligibility-processing/{state_code}")
@safe_route(default_response={})
async def eligibility_processing(state_code: str):
    """Get Medicaid renewal/redetermination outcomes for a state."""
    state_code = state_code.upper()
    with get_cursor() as cur:
        rows = cur.execute("""
            SELECT reporting_period, original_or_updated,
                   renewals_initiated, renewals_due, renewals_completed,
                   renewals_ex_parte, renewals_form_based,
                   disenrolled_total, disenrolled_ineligible, disenrolled_procedural,
                   renewals_pending
            FROM fact_eligibility_processing
            WHERE state_code = $1
            ORDER BY reporting_period DESC
        """, [state_code]).fetchall()
        columns = [
            "reporting_period", "original_or_updated",
            "renewals_initiated", "renewals_due", "renewals_completed",
            "renewals_ex_parte", "renewals_form_based",
            "disenrolled_total", "disenrolled_ineligible", "disenrolled_procedural",
            "renewals_pending",
        ]
        return [dict(zip(columns, r)) for r in rows]


@router.get("/api/eligibility-processing")
@safe_route(default_response={})
async def eligibility_processing_summary():
    """Latest renewal outcomes by state — total renewed, disenrolled, pending."""
    with get_cursor() as cur:
        rows = cur.execute("""
            SELECT state_code,
                   MAX(reporting_period) AS latest_period,
                   SUM(renewals_completed) AS total_renewed,
                   SUM(renewals_ex_parte) AS total_ex_parte,
                   SUM(disenrolled_total) AS total_disenrolled,
                   SUM(disenrolled_procedural) AS total_procedural
            FROM fact_eligibility_processing
            WHERE original_or_updated = 'U' OR original_or_updated = 'O'
            GROUP BY state_code
            ORDER BY total_disenrolled DESC
        """).fetchall()
        columns = ["state_code", "latest_period", "total_renewed",
                    "total_ex_parte", "total_disenrolled", "total_procedural"]
        return [dict(zip(columns, r)) for r in rows]


# ── Marketplace Unwinding Transitions ──────────────────────────────────

@router.get("/api/marketplace-unwinding/{state_code}")
@safe_route(default_response={})
async def marketplace_unwinding(state_code: str):
    """Get HealthCare.gov marketplace transition data during Medicaid unwinding."""
    with get_cursor() as cur:
        rows = cur.execute("""
            SELECT state, metric, time_period,
                   individual_count, individual_pct,
                   cumulative_count, cumulative_pct
            FROM fact_marketplace_unwinding
            WHERE state ILIKE $1 || '%'
            ORDER BY time_period, metric
        """, [state_code.upper()]).fetchall()
        columns = ["state", "metric", "time_period",
                    "individual_count", "individual_pct",
                    "cumulative_count", "cumulative_pct"]
        return [dict(zip(columns, r)) for r in rows]


# ── HCBS Waiting Lists ────────────────────────────────────────────────

@router.get("/api/hcbs-waitlist")
@safe_route(default_response={})
async def hcbs_waitlist():
    """Get HCBS waiting list data for all states (KFF 2025 survey)."""
    with get_cursor() as cur:
        rows = cur.execute("""
            SELECT state_code, state_name, screens_eligibility,
                   idd_waiting, autism_waiting, seniors_physical_waiting,
                   medically_fragile_waiting, mental_health_waiting,
                   tbi_sci_waiting, other_waiting, total_waiting
            FROM fact_hcbs_waitlist
            ORDER BY total_waiting DESC
        """).fetchall()
        columns = [
            "state_code", "state_name", "screens_eligibility",
            "idd_waiting", "autism_waiting", "seniors_physical_waiting",
            "medically_fragile_waiting", "mental_health_waiting",
            "tbi_sci_waiting", "other_waiting", "total_waiting",
        ]
        return [dict(zip(columns, r)) for r in rows]


@router.get("/api/hcbs-waitlist/{state_code}")
@safe_route(default_response={})
async def hcbs_waitlist_by_state(state_code: str):
    """Get HCBS waiting list data for a specific state."""
    state_code = state_code.upper()
    with get_cursor() as cur:
        rows = cur.execute("""
            SELECT state_code, state_name, screens_eligibility,
                   idd_waiting, autism_waiting, seniors_physical_waiting,
                   medically_fragile_waiting, mental_health_waiting,
                   tbi_sci_waiting, other_waiting, total_waiting
            FROM fact_hcbs_waitlist
            WHERE state_code = $1
        """, [state_code]).fetchall()
        if not rows:
            return {"error": "State not found"}
        columns = [
            "state_code", "state_name", "screens_eligibility",
            "idd_waiting", "autism_waiting", "seniors_physical_waiting",
            "medically_fragile_waiting", "mental_health_waiting",
            "tbi_sci_waiting", "other_waiting", "total_waiting",
        ]
        return dict(zip(columns, rows[0]))


# ── LTSS Expenditure & Users ───────────────────────────────────────────

@router.get("/api/ltss/{state_code}")
@safe_route(default_response={})
async def ltss_by_state(state_code: str):
    """Get LTSS expenditure and users for a state (institutional vs HCBS)."""
    state_code = state_code.upper()
    result = {"state_code": state_code, "expenditure": [], "users": []}
    with get_cursor() as cur:
        try:
            rows = cur.execute("""
                SELECT year, ltss_total, institutional_total, institutional_pct,
                       hcbs_total, hcbs_pct
                FROM fact_ltss_expenditure
                WHERE state_code = $1
                ORDER BY year
            """, [state_code]).fetchall()
            columns = ["year", "ltss_total", "institutional_total", "institutional_pct",
                        "hcbs_total", "hcbs_pct"]
            result["expenditure"] = [dict(zip(columns, r)) for r in rows]
        except Exception:
            pass

        try:
            rows = cur.execute("""
                SELECT year, ltss_total, institutional_total, institutional_pct,
                       hcbs_total, hcbs_pct, both_total, both_pct
                FROM fact_ltss_users
                WHERE state_code = $1
                ORDER BY year
            """, [state_code]).fetchall()
            columns = ["year", "ltss_total", "institutional_total", "institutional_pct",
                        "hcbs_total", "hcbs_pct", "both_total", "both_pct"]
            result["users"] = [dict(zip(columns, r)) for r in rows]
        except Exception:
            pass
    return result


@router.get("/api/ltss")
@safe_route(default_response={})
async def ltss_summary():
    """Get LTSS summary — latest year expenditure by state with HCBS rebalancing."""
    with get_cursor() as cur:
        rows = cur.execute("""
            SELECT e.state_code, e.year,
                   e.ltss_total, e.institutional_total, e.hcbs_total, e.hcbs_pct,
                   u.ltss_total AS total_users, u.hcbs_total AS hcbs_users
            FROM fact_ltss_expenditure e
            LEFT JOIN fact_ltss_users u ON e.state_code = u.state_code AND e.year = u.year
            WHERE e.year = (SELECT MAX(year) FROM fact_ltss_expenditure)
            AND e.state_code != 'US'
            ORDER BY e.hcbs_pct DESC
            LIMIT 60
        """).fetchall()
        columns = ["state_code", "year", "ltss_total", "institutional_total",
                    "hcbs_total", "hcbs_pct", "total_users", "hcbs_users"]
        return [dict(zip(columns, r)) for r in rows]


@router.get("/api/ltss/rebalancing")
@safe_route(default_response={})
async def ltss_rebalancing():
    """Get HCBS rebalancing measures — % HCBS expenditure by state and demographics."""
    with get_cursor() as cur:
        rows = cur.execute("""
            SELECT state_code, year, demographic_group, subgroup, hcbs_pct
            FROM fact_ltss_rebalancing
            WHERE year = (SELECT MAX(year) FROM fact_ltss_rebalancing)
            AND state_code != 'US'
            ORDER BY state_code, demographic_group, subgroup
        """).fetchall()
        columns = ["state_code", "year", "demographic_group", "subgroup", "hcbs_pct"]
        return [dict(zip(columns, r)) for r in rows]


# ── CDC Vital Statistics / Maternal Mortality ──────────────────────────

@router.get("/api/vital-stats/{state_code}")
@safe_route(default_response={})
async def vital_stats_state(state_code: str):
    """Get CDC VSRR monthly births, deaths, infant deaths for a state."""
    state_code = state_code.upper()
    with get_cursor() as cur:
        rows = cur.execute("""
            SELECT year, month_name, indicator, value
            FROM fact_vital_stats_monthly
            WHERE state_code = $1
            ORDER BY year, indicator
        """, [state_code]).fetchall()
        columns = ["year", "month_name", "indicator", "value"]
        return [dict(zip(columns, r)) for r in rows]


@router.get("/api/ltss/spending-by-category/{state_code}")
@safe_route(default_response={})
async def ltss_spending_by_category(state_code: str):
    """Get FMR FY 2024 service category spending for a state."""
    state_code = state_code.upper()
    with get_cursor() as cur:
        rows = cur.execute("""
            SELECT service_category, total_computable, federal_share, state_share
            FROM fact_fmr_fy2024
            WHERE state_code = $1 AND report_type = 'MAP'
            AND total_computable IS NOT NULL AND total_computable != 0
            AND service_category NOT LIKE 'Total%'
            ORDER BY total_computable DESC
        """, [state_code]).fetchall()
        columns = ["service_category", "total_computable", "federal_share", "state_share"]
        return [dict(zip(columns, r)) for r in rows]


@router.get("/api/fmr/state-totals")
@safe_route(default_response={})
async def fmr_state_totals():
    """Get FY 2024 total Medicaid spending by state (MAP net expenditures)."""
    with get_cursor() as cur:
        rows = cur.execute("""
            SELECT state_code,
                   total_computable, federal_share, state_share
            FROM fact_fmr_fy2024
            WHERE report_type = 'MAP'
            AND service_category = 'Total Net Expenditures'
            AND state_code != 'US'
            ORDER BY total_computable DESC
        """).fetchall()
        columns = ["state_code", "total_computable", "federal_share", "state_share"]
        return [dict(zip(columns, r)) for r in rows]


@router.get("/api/maternal-mortality")
@safe_route(default_response={})
async def maternal_mortality_trends():
    """Get national maternal mortality trends by demographics."""
    with get_cursor() as cur:
        rows = cur.execute("""
            SELECT demographic_group, subgroup, year, month,
                   maternal_deaths, live_births, maternal_mortality_rate
            FROM fact_maternal_mortality_monthly
            WHERE jurisdiction = 'United States'
            ORDER BY demographic_group, subgroup, year, month
            LIMIT 500
        """).fetchall()
        columns = ["demographic_group", "subgroup", "year", "month",
                    "maternal_deaths", "live_births", "maternal_mortality_rate"]
        return [dict(zip(columns, r)) for r in rows]
