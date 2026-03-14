"""Rate-Quality Nexus — correlations between Medicaid rate levels and quality, access, and workforce outcomes."""

from fastapi import APIRouter, HTTPException, Query
from server.db import get_cursor

router = APIRouter()


@router.get("/api/research/rate-quality/correlation")
async def rate_quality_correlation(measure_id: str = Query(default="prenatal_care")):
    """Correlate average Medicaid-to-Medicare rate ratio with a quality measure by state."""
    try:
        with get_cursor() as cur:
            rows = cur.execute("""
                WITH rates AS (
                    SELECT state_code,
                           AVG(pct_of_medicare) AS avg_pct_medicare,
                           COUNT(*) AS procedure_count
                    FROM fact_rate_comparison
                    WHERE pct_of_medicare > 0 AND pct_of_medicare < 10
                    GROUP BY state_code
                ),
                quality AS (
                    SELECT state_code, state_rate
                    FROM fact_quality_core_set_2024
                    WHERE measure_id = $1
                      AND state_rate IS NOT NULL
                )
                SELECT r.state_code, r.avg_pct_medicare, r.procedure_count,
                       q.state_rate AS measure_rate
                FROM rates r
                INNER JOIN quality q ON r.state_code = q.state_code
                ORDER BY r.avg_pct_medicare
            """, [measure_id]).fetchall()
            columns = ["state_code", "avg_pct_medicare", "procedure_count", "measure_rate"]
            return {"rows": [dict(zip(columns, r)) for r in rows], "count": len(rows)}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"rate-quality correlation failed: {exc}")


@router.get("/api/research/rate-quality/measures")
async def rate_quality_measures():
    """List available quality measures from the Core Set 2024."""
    try:
        with get_cursor() as cur:
            rows = cur.execute("""
                SELECT DISTINCT measure_id, measure_name
                FROM fact_quality_core_set_2024
                WHERE state_rate IS NOT NULL
                ORDER BY measure_name
            """).fetchall()
            return {"measures": [{"id": r[0], "name": r[1]} for r in rows]}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"rate-quality measures failed: {exc}")


@router.get("/api/research/rate-quality/access")
async def rate_quality_access():
    """HPSA shortage area count per state vs average Medicaid rate level."""
    try:
        with get_cursor() as cur:
            rows = cur.execute("""
                WITH rates AS (
                    SELECT state_code, AVG(pct_of_medicare) AS avg_pct_medicare
                    FROM fact_rate_comparison
                    WHERE pct_of_medicare > 0 AND pct_of_medicare < 10
                    GROUP BY state_code
                ),
                hpsas AS (
                    SELECT state_code, COUNT(DISTINCT hpsa_id) AS hpsa_count
                    FROM fact_hpsa
                    GROUP BY state_code
                )
                SELECT r.state_code, r.avg_pct_medicare, COALESCE(h.hpsa_count, 0) AS hpsa_count
                FROM rates r
                LEFT JOIN hpsas h ON r.state_code = h.state_code
                ORDER BY h.hpsa_count DESC
            """).fetchall()
            columns = ["state_code", "avg_pct_medicare", "hpsa_count"]
            return {"rows": [dict(zip(columns, r)) for r in rows], "count": len(rows)}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"rate-quality access failed: {exc}")


@router.get("/api/research/rate-quality/workforce")
async def rate_quality_workforce():
    """BLS healthcare wages vs Medicaid rate levels by state."""
    try:
        with get_cursor() as cur:
            rows = cur.execute("""
                WITH rates AS (
                    SELECT state_code, AVG(pct_of_medicare) AS avg_pct_medicare
                    FROM fact_rate_comparison
                    WHERE pct_of_medicare > 0 AND pct_of_medicare < 10
                    GROUP BY state_code
                ),
                wages AS (
                    SELECT state_code, AVG(hourly_mean) AS avg_healthcare_wage
                    FROM fact_bls_wage
                    WHERE soc_code LIKE '29-%' OR soc_code LIKE '31-%'
                    GROUP BY state_code
                )
                SELECT r.state_code, r.avg_pct_medicare,
                       COALESCE(w.avg_healthcare_wage, 0) AS avg_healthcare_wage
                FROM rates r
                LEFT JOIN wages w ON r.state_code = w.state_code
                ORDER BY r.state_code
            """).fetchall()
            columns = ["state_code", "avg_pct_medicare", "avg_healthcare_wage"]
            return {"rows": [dict(zip(columns, r)) for r in rows], "count": len(rows)}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"rate-quality workforce failed: {exc}")


@router.get("/api/research/rate-quality/detail")
async def rate_quality_detail():
    """Full state detail: rates, quality, HPSAs, and managed care penetration."""
    try:
        with get_cursor() as cur:
            rows = cur.execute("""
                WITH rates AS (
                    SELECT state_code,
                           AVG(pct_of_medicare) AS avg_pct_medicare,
                           COUNT(*) AS procedure_count
                    FROM fact_rate_comparison
                    WHERE pct_of_medicare > 0 AND pct_of_medicare < 10
                    GROUP BY state_code
                ),
                quality_agg AS (
                    SELECT state_code,
                           AVG(state_rate) AS avg_quality_rate,
                           COUNT(DISTINCT measure_id) AS measures_reported
                    FROM fact_quality_core_set_2024
                    WHERE state_rate IS NOT NULL
                    GROUP BY state_code
                ),
                hpsas AS (
                    SELECT state_code, COUNT(*) AS hpsa_count
                    FROM fact_hpsa
                    GROUP BY state_code
                ),
                mc AS (
                    SELECT state_code, mc_penetration_pct
                    FROM fact_mc_enrollment_summary
                    WHERE year = (SELECT MAX(year) FROM fact_mc_enrollment_summary)
                )
                SELECT r.state_code, r.avg_pct_medicare, r.procedure_count,
                       qa.avg_quality_rate, qa.measures_reported,
                       COALESCE(h.hpsa_count, 0) AS hpsa_count,
                       mc.mc_penetration_pct
                FROM rates r
                LEFT JOIN quality_agg qa ON r.state_code = qa.state_code
                LEFT JOIN hpsas h ON r.state_code = h.state_code
                LEFT JOIN mc ON r.state_code = mc.state_code
                ORDER BY r.avg_pct_medicare
            """).fetchall()
            columns = [
                "state_code", "avg_pct_medicare", "procedure_count",
                "avg_quality_rate", "measures_reported", "hpsa_count",
                "mc_penetration_pct",
            ]
            return {"rows": [dict(zip(columns, r)) for r in rows], "count": len(rows)}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"rate-quality detail failed: {exc}")
