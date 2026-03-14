"""Managed Care Value Assessment — MC penetration vs spending, MCO financials, quality by tier, trends."""

from fastapi import APIRouter, HTTPException, Query
from server.db import get_cursor

router = APIRouter()


@router.get("/api/research/mc-value/penetration-spending")
async def mc_penetration_spending():
    """Managed care penetration vs per-enrollee spending by state."""
    try:
        with get_cursor() as cur:
            # Try direct join first; fall back to dim_state join if state_code missing
            try:
                rows = cur.execute("""
                    WITH mc AS (
                        SELECT state_code, mc_penetration_pct
                        FROM fact_mc_enrollment_summary
                        WHERE year = (SELECT MAX(year) FROM fact_mc_enrollment_summary)
                    ),
                    spending AS (
                        SELECT state_code, per_enrollee_spending
                        FROM fact_macpac_spending_per_enrollee
                    )
                    SELECT mc.state_code, mc.mc_penetration_pct,
                           s.per_enrollee_spending
                    FROM mc
                    LEFT JOIN spending s ON mc.state_code = s.state_code
                    WHERE mc.mc_penetration_pct IS NOT NULL
                    ORDER BY mc.mc_penetration_pct
                """).fetchall()
            except Exception:
                # Fallback: join through dim_state if spending table uses state_name
                rows = cur.execute("""
                    WITH mc AS (
                        SELECT state_code, mc_penetration_pct
                        FROM fact_mc_enrollment_summary
                        WHERE year = (SELECT MAX(year) FROM fact_mc_enrollment_summary)
                    ),
                    spending AS (
                        SELECT d.state_code, s.per_enrollee_spending
                        FROM fact_macpac_spending_per_enrollee s
                        JOIN dim_state d ON UPPER(TRIM(
                            REGEXP_REPLACE(s.state_name, '[0-9,]+$', '')
                        )) = UPPER(d.state_name)
                    )
                    SELECT mc.state_code, mc.mc_penetration_pct,
                           s.per_enrollee_spending
                    FROM mc
                    LEFT JOIN spending s ON mc.state_code = s.state_code
                    WHERE mc.mc_penetration_pct IS NOT NULL
                    ORDER BY mc.mc_penetration_pct
                """).fetchall()
            columns = ["state_code", "mc_penetration_pct", "per_enrollee_spending"]
            return {"rows": [dict(zip(columns, r)) for r in rows], "count": len(rows)}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"mc penetration-spending failed: {exc}")


@router.get("/api/research/mc-value/mco-financials")
async def mco_financials():
    """MCO MLR financial detail by state and plan."""
    try:
        with get_cursor() as cur:
            rows = cur.execute("""
                SELECT state_code, plan_name, program_name,
                       member_months, adjusted_mlr,
                       mlr_numerator, mlr_denominator,
                       remittance_amount
                FROM fact_mco_mlr
                ORDER BY state_code, adjusted_mlr
            """).fetchall()
            columns = [
                "state_code", "plan_name", "program_name",
                "member_months", "adjusted_mlr",
                "mlr_numerator", "mlr_denominator",
                "remittance_amount",
            ]
            return {"rows": [dict(zip(columns, r)) for r in rows], "count": len(rows)}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"mco-financials failed: {exc}")


@router.get("/api/research/mc-value/mco-summary")
async def mco_summary():
    """Aggregated MCO MLR statistics by state."""
    try:
        with get_cursor() as cur:
            rows = cur.execute("""
                SELECT state_code,
                       COUNT(*) AS plan_count,
                       SUM(member_months) AS total_member_months,
                       AVG(adjusted_mlr) AS avg_mlr,
                       MIN(adjusted_mlr) AS min_mlr,
                       MAX(adjusted_mlr) AS max_mlr,
                       SUM(remittance_amount) AS total_remittance
                FROM fact_mco_mlr
                GROUP BY state_code
                ORDER BY avg_mlr
            """).fetchall()
            columns = [
                "state_code", "plan_count", "total_member_months",
                "avg_mlr", "min_mlr", "max_mlr", "total_remittance",
            ]
            return {"rows": [dict(zip(columns, r)) for r in rows], "count": len(rows)}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"mco-summary failed: {exc}")


@router.get("/api/research/mc-value/quality-by-tier")
async def mc_quality_by_tier(measure_id: str = Query(default=None)):
    """Average quality measure rates grouped by managed care penetration tier."""
    try:
        with get_cursor() as cur:
            sql = """
                WITH mc AS (
                    SELECT state_code, mc_penetration_pct,
                           CASE
                               WHEN mc_penetration_pct >= 80 THEN 'High (80%+)'
                               WHEN mc_penetration_pct >= 50 THEN 'Medium (50-80%)'
                               ELSE 'Low (<50%)'
                           END AS mc_tier
                    FROM fact_mc_enrollment_summary
                    WHERE year = (SELECT MAX(year) FROM fact_mc_enrollment_summary)
                ),
                quality AS (
                    SELECT state_code, measure_id, measure_name, measure_rate
                    FROM fact_quality_core_set_2024
                    WHERE measure_rate IS NOT NULL
            """
            params: list = []
            if measure_id:
                sql += " AND measure_id = $1"
                params.append(measure_id)
            sql += """
                )
                SELECT mc.mc_tier, q.measure_id, q.measure_name,
                       AVG(q.measure_rate) AS avg_measure_rate,
                       COUNT(DISTINCT mc.state_code) AS state_count
                FROM mc
                INNER JOIN quality q ON mc.state_code = q.state_code
                GROUP BY mc.mc_tier, q.measure_id, q.measure_name
                ORDER BY q.measure_name, mc.mc_tier
            """
            rows = cur.execute(sql, params).fetchall()
            columns = ["mc_tier", "measure_id", "measure_name", "avg_measure_rate", "state_count"]
            return {"rows": [dict(zip(columns, r)) for r in rows], "count": len(rows)}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"mc quality-by-tier failed: {exc}")


@router.get("/api/research/mc-value/trend")
async def mc_trend():
    """Managed care penetration and total spending trends over time by state."""
    try:
        with get_cursor() as cur:
            rows = cur.execute("""
                WITH mc_trend AS (
                    SELECT year, state_code, mc_penetration_pct
                    FROM fact_mc_enrollment_summary
                ),
                spending_trend AS (
                    SELECT fiscal_year AS year, state_code,
                           SUM(total_computable) AS total_spending
                    FROM fact_cms64_multiyear
                    WHERE state_code != 'US'
                    GROUP BY fiscal_year, state_code
                )
                SELECT mt.year, mt.state_code,
                       mt.mc_penetration_pct,
                       st.total_spending
                FROM mc_trend mt
                LEFT JOIN spending_trend st ON mt.state_code = st.state_code AND mt.year = st.year
                ORDER BY mt.year, mt.state_code
            """).fetchall()
            columns = ["year", "state_code", "mc_penetration_pct", "total_spending"]
            return {"rows": [dict(zip(columns, r)) for r in rows], "count": len(rows)}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"mc trend failed: {exc}")
