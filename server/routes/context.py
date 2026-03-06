"""Contextual data routes — Census, CDC, economic indicators, HPSA, Scorecard."""

from fastapi import APIRouter, Query
from server.db import get_cursor

router = APIRouter()


@router.get("/api/demographics/{state_code}")
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
