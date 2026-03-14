"""Maternal Health Deserts — mortality, access gaps, quality measures, infant mortality, and composite risk."""

from fastapi import APIRouter, HTTPException, Query
from server.db import get_cursor

router = APIRouter()


@router.get("/api/research/maternal-health/mortality")
async def maternal_mortality(state: str = Query(None), year: int = Query(None)):
    """Maternal mortality rates by state and year."""
    try:
        with get_cursor() as cur:
            params = []
            conditions = []
            if state:
                params.append(state.upper())
                conditions.append(f"state_code = ${len(params)}")
            if year:
                params.append(year)
                conditions.append(f"year = ${len(params)}")
            where_clause = f"WHERE {' AND '.join(conditions)}" if conditions else ""

            # Try fact_cdc_maternal_mortality_prov first, fall back to fact_maternal_mortality_national
            try:
                rows = cur.execute(f"""
                    SELECT state_code, year,
                           COALESCE(maternal_mortality_rate, 0) AS maternal_mortality_rate,
                           COALESCE(numerator, 0) AS maternal_deaths,
                           COALESCE(denominator, 0) AS live_births
                    FROM fact_cdc_maternal_mortality_prov
                    {where_clause}
                    ORDER BY year DESC, maternal_mortality_rate DESC
                """, params).fetchall()
                columns = ["state_code", "year", "maternal_mortality_rate", "maternal_deaths", "live_births"]
            except Exception:
                try:
                    rows = cur.execute(f"""
                        SELECT state_code, year,
                               COALESCE(maternal_mortality_rate, 0) AS maternal_mortality_rate,
                               COALESCE(numerator, deaths, 0) AS maternal_deaths,
                               COALESCE(denominator, births, 0) AS live_births
                        FROM fact_maternal_mortality_national
                        {where_clause}
                        ORDER BY year DESC, maternal_mortality_rate DESC
                    """, params).fetchall()
                    columns = ["state_code", "year", "maternal_mortality_rate", "maternal_deaths", "live_births"]
                except Exception:
                    # Last resort: discover schema
                    schema_rows = cur.execute("""
                        SELECT column_name FROM information_schema.columns
                        WHERE table_name IN ('fact_cdc_maternal_mortality_prov', 'fact_maternal_mortality_national')
                        LIMIT 20
                    """).fetchall()
                    available = [r[0] for r in schema_rows]
                    raise HTTPException(status_code=500, detail={
                        "error": "Maternal mortality table schema mismatch",
                        "available_columns": available,
                    })

            return {"rows": [dict(zip(columns, r)) for r in rows], "count": len(rows)}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail={"error": "Maternal mortality query failed", "detail": str(e)})


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
                    SELECT state_code, COUNT(*) AS obgyn_hpsa_count
                    FROM fact_hpsa
                    WHERE discipline_type ILIKE '%primary%'
                       OR hpsa_type ILIKE '%primary%'
                    GROUP BY state_code
                ),
                svi AS (
                    SELECT state_code,
                           ROUND(AVG(svi_score), 4) AS avg_svi_score,
                           COUNT(*) AS county_count
                    FROM fact_svi_county
                    GROUP BY state_code
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
                SELECT state_code, measure_id, measure_name, measure_rate
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
                  AND measure_rate IS NOT NULL
                  {state_filter}
                ORDER BY measure_id, state_code
            """, params).fetchall()
            columns = ["state_code", "measure_id", "measure_name", "measure_rate"]
            return {"rows": [dict(zip(columns, r)) for r in rows], "count": len(rows)}
    except Exception as e:
        raise HTTPException(status_code=500, detail={"error": "Maternal quality query failed", "detail": str(e)})


@router.get("/api/research/maternal-health/infant-mortality")
async def infant_mortality(state: str = Query(None), year: int = Query(None)):
    """Infant mortality rates by state and year."""
    try:
        with get_cursor() as cur:
            params = []
            conditions = []
            if state:
                params.append(state.upper())
                conditions.append(f"state_code = ${len(params)}")
            if year:
                params.append(year)
                conditions.append(f"year = ${len(params)}")
            where_clause = f"WHERE {' AND '.join(conditions)}" if conditions else ""

            try:
                rows = cur.execute(f"""
                    SELECT state_code, year,
                           COALESCE(infant_mortality_rate, 0) AS infant_mortality_rate
                    FROM fact_infant_mortality_state
                    {where_clause}
                    ORDER BY year DESC, infant_mortality_rate DESC
                """, params).fetchall()
                columns = ["state_code", "year", "infant_mortality_rate"]
            except Exception:
                # Fallback: discover schema
                schema_rows = cur.execute("SELECT column_name FROM information_schema.columns WHERE table_name = 'fact_infant_mortality_state'").fetchall()
                available_cols = [r[0] for r in schema_rows]
                raise HTTPException(status_code=500, detail={
                    "error": "Infant mortality table schema mismatch",
                    "available_columns": available_cols,
                })

            return {"rows": [dict(zip(columns, r)) for r in rows], "count": len(rows)}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail={"error": "Infant mortality query failed", "detail": str(e)})


@router.get("/api/research/maternal-health/composite")
async def maternal_composite(state: str = Query(None)):
    """Composite maternal health risk: mortality, HPSAs, SVI, quality measures."""
    try:
        with get_cursor() as cur:
            params = []
            state_filter = ""
            if state:
                state_filter = "AND (d.state_code = $1)"
                params.append(state.upper())
            rows = cur.execute(f"""
                WITH mortality AS (
                    SELECT state_code, MAX(maternal_mortality_rate) AS mmr
                    FROM fact_cdc_maternal_mortality_prov
                    WHERE year = (SELECT MAX(year) FROM fact_cdc_maternal_mortality_prov)
                    GROUP BY state_code
                ),
                hpsas AS (
                    SELECT state_code, COUNT(*) AS hpsa_count
                    FROM fact_hpsa
                    GROUP BY state_code
                ),
                svi AS (
                    SELECT state_code, ROUND(AVG(svi_score), 4) AS avg_svi
                    FROM fact_svi_county
                    GROUP BY state_code
                ),
                quality AS (
                    SELECT state_code, ROUND(AVG(measure_rate), 2) AS avg_maternal_quality
                    FROM fact_quality_core_set_2024
                    WHERE measure_id ILIKE '%prenatal%'
                       OR measure_id ILIKE '%postpartum%'
                       OR measure_id ILIKE '%ppc%'
                    GROUP BY state_code
                )
                SELECT d.state_code,
                       m.mmr AS maternal_mortality_rate,
                       COALESCE(h.hpsa_count, 0) AS hpsa_count,
                       COALESCE(s.avg_svi, 0) AS avg_svi_score,
                       q.avg_maternal_quality
                FROM dim_state d
                LEFT JOIN mortality m ON d.state_code = m.state_code
                LEFT JOIN hpsas h ON d.state_code = h.state_code
                LEFT JOIN svi s ON d.state_code = s.state_code
                LEFT JOIN quality q ON d.state_code = q.state_code
                WHERE (m.mmr IS NOT NULL OR h.hpsa_count IS NOT NULL)
                {state_filter}
                ORDER BY m.mmr DESC NULLS LAST
            """, params).fetchall()
            columns = [
                "state_code", "maternal_mortality_rate", "hpsa_count",
                "avg_svi_score", "avg_maternal_quality",
            ]
            return {"rows": [dict(zip(columns, r)) for r in rows], "count": len(rows)}
    except Exception as e:
        raise HTTPException(status_code=500, detail={"error": "Maternal composite query failed", "detail": str(e)})
