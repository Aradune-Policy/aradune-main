"""Maternal Health Deserts — mortality, access gaps, quality measures, infant mortality, and composite risk."""

from fastapi import APIRouter, HTTPException, Query
from server.db import get_cursor

router = APIRouter()


@router.get("/api/research/maternal-health/mortality")
async def maternal_mortality(state: str = Query(None), year: int = Query(None)):
    """Maternal mortality / SMM rates by state and year.

    Uses fact_maternal_morbidity which has state-level SMM data via geography column.
    Falls back to national fact_cdc_maternal_mortality_prov for national trends.
    """
    try:
        with get_cursor() as cur:
            # fact_maternal_morbidity has state-level data with geography = state name
            params: list = []
            conditions = ["geography != 'National'"]
            if state:
                params.append(state.upper())
                conditions.append(f"d.state_code = ${len(params)}")
            if year:
                params.append(year)
                conditions.append(f"m.year = ${len(params)}")
            where_clause = "WHERE " + " AND ".join(conditions)

            rows = cur.execute(f"""
                SELECT d.state_code, m.year,
                       MAX(CASE WHEN m.category = 'Deliveries with SMM per 10,000 live births'
                           THEN m.rate END) AS smm_rate_per_10k,
                       MAX(CASE WHEN m.category = 'Live births that were preterm'
                           THEN m.rate END) AS preterm_pct,
                       MAX(CASE WHEN m.category = 'Deliveries with multiple SMM conditions per 10,000 live births'
                           THEN m.rate END) AS multiple_smm_rate
                FROM fact_maternal_morbidity m
                JOIN dim_state d ON UPPER(m.geography) = UPPER(d.state_name)
                {where_clause}
                GROUP BY d.state_code, m.year
                ORDER BY m.year DESC, smm_rate_per_10k DESC NULLS LAST
            """, params).fetchall()
            columns = ["state_code", "year", "smm_rate_per_10k", "preterm_pct", "multiple_smm_rate"]
            return {"rows": [dict(zip(columns, r)) for r in rows], "count": len(rows)}
    except Exception as e:
        raise HTTPException(status_code=500, detail={"error": "Maternal mortality query failed", "detail": str(e)})


@router.get("/api/research/maternal-health/national-trend")
async def maternal_national_trend():
    """National maternal mortality monthly trend from CDC provisional data."""
    try:
        with get_cursor() as cur:
            rows = cur.execute("""
                SELECT year, month, time_period,
                       maternal_deaths, live_births,
                       ROUND(mortality_rate, 1) AS mortality_rate
                FROM fact_cdc_maternal_mortality_prov
                WHERE demographic_group = 'Total' AND subgroup = 'Total'
                ORDER BY year, month
            """).fetchall()
            columns = ["year", "month", "time_period", "maternal_deaths", "live_births", "mortality_rate"]
            return {"rows": [dict(zip(columns, r)) for r in rows], "count": len(rows)}
    except Exception as e:
        raise HTTPException(status_code=500, detail={"error": "National trend query failed", "detail": str(e)})


@router.get("/api/research/maternal-health/access")
async def maternal_access(state: str = Query(None)):
    """Maternal health access: HPSA shortage areas and social vulnerability index by state."""
    try:
        with get_cursor() as cur:
            params = []
            state_filter = ""
            if state:
                state_filter = "WHERE d.state_code = $1"
                params.append(state.upper())
            rows = cur.execute(f"""
                WITH hpsa_ob AS (
                    SELECT state_code, COUNT(DISTINCT hpsa_id) AS obgyn_hpsa_count
                    FROM fact_hpsa
                    WHERE discipline ILIKE '%primary%'
                       OR designation_type ILIKE '%primary%'
                    GROUP BY state_code
                ),
                svi AS (
                    SELECT st_abbr AS state_code,
                           ROUND(AVG(rpl_themes), 4) AS avg_svi_score,
                           COUNT(*) AS county_count
                    FROM fact_svi_county
                    WHERE rpl_themes >= 0
                    GROUP BY st_abbr
                )
                SELECT d.state_code,
                       COALESCE(h.obgyn_hpsa_count, 0) AS hpsa_count,
                       COALESCE(s.avg_svi_score, 0) AS avg_svi_score,
                       COALESCE(s.county_count, 0) AS county_count
                FROM dim_state d
                LEFT JOIN hpsa_ob h ON d.state_code = h.state_code
                LEFT JOIN svi s ON d.state_code = s.state_code
                {state_filter}
                ORDER BY avg_svi_score DESC
            """, params).fetchall()
            columns = ["state_code", "hpsa_count", "avg_svi_score", "county_count"]
            return {"rows": [dict(zip(columns, r)) for r in rows], "count": len(rows)}
    except Exception as e:
        raise HTTPException(status_code=500, detail={"error": "Maternal access query failed", "detail": str(e)})


@router.get("/api/research/maternal-health/quality")
async def maternal_quality(state: str = Query(None)):
    """Prenatal, postpartum, and maternal quality measures from Core Set 2024."""
    try:
        with get_cursor() as cur:
            params = []
            state_filter = ""
            if state:
                state_filter = "AND state_code = $1"
                params.append(state.upper())
            rows = cur.execute(f"""
                SELECT state_code, measure_id, measure_name, state_rate AS measure_rate
                FROM fact_quality_core_set_2024
                WHERE (measure_id ILIKE '%prenatal%'
                    OR measure_id ILIKE '%postpartum%'
                    OR measure_id ILIKE '%ppc%'
                    OR measure_id ILIKE '%pqa%'
                    OR measure_name ILIKE '%prenatal%'
                    OR measure_name ILIKE '%postpartum%'
                    OR measure_name ILIKE '%maternal%'
                    OR measure_name ILIKE '%cesarean%'
                    OR measure_name ILIKE '%low birth%')
                  AND state_rate IS NOT NULL
                  {state_filter}
                ORDER BY measure_id, state_code
            """, params).fetchall()
            columns = ["state_code", "measure_id", "measure_name", "measure_rate"]
            return {"rows": [dict(zip(columns, r)) for r in rows], "count": len(rows)}
    except Exception as e:
        raise HTTPException(status_code=500, detail={"error": "Maternal quality query failed", "detail": str(e)})


@router.get("/api/research/maternal-health/infant-mortality")
async def infant_mortality(state: str = Query(None)):
    """Infant mortality rates by state from CDC NCHS data."""
    try:
        with get_cursor() as cur:
            params: list = []
            conditions = [
                "i.subtopic = 'Total'",
                "i.classification = 'Geographic Characteristic'",
                "i.group_name = 'State or territory'",
                "i.estimate IS NOT NULL",
            ]
            if state:
                params.append(state.upper())
                conditions.append(f"d.state_code = ${len(params)}")
            where_clause = "WHERE " + " AND ".join(conditions)

            # state_fips is numeric; join to dim_state via a FIPS mapping subquery
            # Since dim_state doesn't have FIPS, we use a reference table approach
            # or hardcode the LPAD approach for 2-digit FIPS -> state lookup
            rows = cur.execute(f"""
                WITH state_fips_map AS (
                    SELECT st_abbr AS state_code, CAST(st AS INTEGER) AS state_fips
                    FROM fact_svi_county
                    GROUP BY st_abbr, st
                )
                SELECT d.state_code, i.time_period,
                       ROUND(i.estimate, 1) AS infant_mortality_rate,
                       ROUND(i.estimate_lci, 1) AS rate_lci,
                       ROUND(i.estimate_uci, 1) AS rate_uci
                FROM fact_infant_mortality_state i
                JOIN state_fips_map f ON CAST(i.state_fips AS INTEGER) = f.state_fips
                JOIN dim_state d ON f.state_code = d.state_code
                {where_clause}
                ORDER BY i.time_period DESC, infant_mortality_rate DESC
            """, params).fetchall()
            columns = ["state_code", "time_period", "infant_mortality_rate", "rate_lci", "rate_uci"]
            return {"rows": [dict(zip(columns, r)) for r in rows], "count": len(rows)}
    except Exception as e:
        raise HTTPException(status_code=500, detail={"error": "Infant mortality query failed", "detail": str(e)})


@router.get("/api/research/maternal-health/composite")
async def maternal_composite(state: str = Query(None)):
    """Composite maternal health risk: SMM rates, HPSAs, SVI, quality measures."""
    try:
        with get_cursor() as cur:
            params = []
            state_filter = ""
            if state:
                state_filter = "AND (d.state_code = $1)"
                params.append(state.upper())
            rows = cur.execute(f"""
                WITH smm AS (
                    SELECT d2.state_code,
                           MAX(CASE WHEN m.category = 'Deliveries with SMM per 10,000 live births'
                               THEN m.rate END) AS smm_rate
                    FROM fact_maternal_morbidity m
                    JOIN dim_state d2 ON UPPER(m.geography) = UPPER(d2.state_name)
                    WHERE m.year = (SELECT MAX(year) FROM fact_maternal_morbidity WHERE geography != 'National')
                      AND m.geography != 'National'
                    GROUP BY d2.state_code
                ),
                hpsas AS (
                    SELECT state_code, COUNT(DISTINCT hpsa_id) AS hpsa_count
                    FROM fact_hpsa
                    GROUP BY state_code
                ),
                svi AS (
                    SELECT st_abbr AS state_code, ROUND(AVG(rpl_themes), 4) AS avg_svi
                    FROM fact_svi_county
                    WHERE rpl_themes >= 0
                    GROUP BY st_abbr
                ),
                quality AS (
                    SELECT state_code, ROUND(AVG(state_rate), 2) AS avg_maternal_quality
                    FROM fact_quality_core_set_2024
                    WHERE (measure_id ILIKE '%prenatal%'
                       OR measure_id ILIKE '%postpartum%'
                       OR measure_id ILIKE '%ppc%')
                      AND state_rate IS NOT NULL
                    GROUP BY state_code
                )
                SELECT d.state_code,
                       COALESCE(smm.smm_rate, 0) AS maternal_mortality_rate,
                       COALESCE(h.hpsa_count, 0) AS hpsa_count,
                       COALESCE(s.avg_svi, 0) AS avg_svi_score,
                       COALESCE(q.avg_maternal_quality, 0) AS avg_maternal_quality
                FROM dim_state d
                LEFT JOIN smm ON d.state_code = smm.state_code
                LEFT JOIN hpsas h ON d.state_code = h.state_code
                LEFT JOIN svi s ON d.state_code = s.state_code
                LEFT JOIN quality q ON d.state_code = q.state_code
                WHERE (smm.smm_rate IS NOT NULL OR h.hpsa_count IS NOT NULL)
                {state_filter}
                ORDER BY smm.smm_rate DESC NULLS LAST
            """, params).fetchall()
            columns = [
                "state_code", "maternal_mortality_rate", "hpsa_count",
                "avg_svi_score", "avg_maternal_quality",
            ]
            return {"rows": [dict(zip(columns, r)) for r in rows], "count": len(rows)}
    except Exception as e:
        raise HTTPException(status_code=500, detail={"error": "Maternal composite query failed", "detail": str(e)})
