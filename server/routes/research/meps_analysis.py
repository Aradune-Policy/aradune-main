"""MEPS Household Component Analysis — individual-level expenditure and utilization by insurance type."""

from fastapi import APIRouter, HTTPException, Query
from server.db import get_cursor
from server.utils.error_handler import safe_route

router = APIRouter()


@router.get("/api/research/meps/expenditure-by-insurance")
@safe_route(default_response={})
async def meps_expenditure_by_insurance():
    """Mean expenditure by insurance coverage type from MEPS HC-243 (2022)."""
    try:
        with get_cursor() as cur:
            rows = cur.execute("""
                SELECT
                    CASE
                        WHEN medicaid_months > 0 AND medicare_months > 0 THEN 'Dual (Medicaid+Medicare)'
                        WHEN medicaid_months > 0 THEN 'Medicaid'
                        WHEN medicare_months > 0 THEN 'Medicare Only'
                        WHEN uninsured_months >= 6 THEN 'Uninsured (6+ months)'
                        ELSE 'Private/Other'
                    END AS coverage_group,
                    COUNT(*) AS respondents,
                    ROUND(SUM(person_weight)) AS weighted_pop,
                    ROUND(AVG(total_expenditure), 0) AS mean_total_exp,
                    ROUND(AVG(out_of_pocket), 0) AS mean_oop,
                    ROUND(AVG(medicaid_paid), 0) AS mean_medicaid_paid,
                    ROUND(AVG(medicare_paid), 0) AS mean_medicare_paid,
                    ROUND(AVG(private_paid), 0) AS mean_private_paid,
                    ROUND(AVG(office_visits), 1) AS mean_office_visits,
                    ROUND(AVG(er_visits), 2) AS mean_er_visits,
                    ROUND(AVG(rx_fills), 1) AS mean_rx_fills,
                    ROUND(AVG(CASE WHEN total_expenditure > 0 THEN out_of_pocket * 100.0 / total_expenditure END), 1) AS oop_pct_of_total
                FROM fact_meps_hc_2022
                WHERE person_weight > 0
                GROUP BY coverage_group
                ORDER BY mean_total_exp DESC
            """).fetchall()
            columns = [
                "coverage_group", "respondents", "weighted_pop",
                "mean_total_exp", "mean_oop", "mean_medicaid_paid",
                "mean_medicare_paid", "mean_private_paid",
                "mean_office_visits", "mean_er_visits", "mean_rx_fills",
                "oop_pct_of_total",
            ]
            return {"rows": [dict(zip(columns, r)) for r in rows], "count": len(rows)}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"meps expenditure failed: {exc}")


@router.get("/api/research/meps/utilization-by-poverty")
@safe_route(default_response={})
async def meps_utilization_by_poverty():
    """Utilization patterns by poverty level from MEPS (relevant to Medicaid eligibility)."""
    try:
        with get_cursor() as cur:
            rows = cur.execute("""
                SELECT
                    CASE
                        WHEN poverty_level_pct < 100 THEN 'Below 100% FPL'
                        WHEN poverty_level_pct < 138 THEN '100-138% FPL (expansion)'
                        WHEN poverty_level_pct < 200 THEN '138-200% FPL'
                        WHEN poverty_level_pct < 400 THEN '200-400% FPL'
                        ELSE '400%+ FPL'
                    END AS poverty_group,
                    COUNT(*) AS respondents,
                    ROUND(AVG(total_expenditure), 0) AS mean_total_exp,
                    ROUND(AVG(out_of_pocket), 0) AS mean_oop,
                    ROUND(AVG(office_visits), 1) AS mean_office_visits,
                    ROUND(AVG(er_visits), 2) AS mean_er_visits,
                    ROUND(AVG(rx_fills), 1) AS mean_rx_fills,
                    ROUND(AVG(medicaid_months), 1) AS mean_medicaid_months,
                    ROUND(SUM(CASE WHEN medicaid_months > 0 THEN person_weight ELSE 0 END) * 100.0
                          / NULLIF(SUM(person_weight), 0), 1) AS pct_with_medicaid
                FROM fact_meps_hc_2022
                WHERE person_weight > 0 AND poverty_level_pct > 0
                GROUP BY poverty_group
                ORDER BY MIN(poverty_level_pct)
            """).fetchall()
            columns = [
                "poverty_group", "respondents", "mean_total_exp", "mean_oop",
                "mean_office_visits", "mean_er_visits", "mean_rx_fills",
                "mean_medicaid_months", "pct_with_medicaid",
            ]
            return {"rows": [dict(zip(columns, r)) for r in rows], "count": len(rows)}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"meps utilization failed: {exc}")


@router.get("/api/research/meps/medicaid-profile")
@safe_route(default_response={})
async def meps_medicaid_profile():
    """Profile of Medicaid enrollees vs other coverage types."""
    try:
        with get_cursor() as cur:
            rows = cur.execute("""
                WITH categorized AS (
                    SELECT *,
                        CASE
                            WHEN medicaid_months > 0 THEN 'Medicaid'
                            WHEN uninsured_months >= 6 THEN 'Uninsured'
                            ELSE 'Other Coverage'
                        END AS insurance_group
                    FROM fact_meps_hc_2022
                    WHERE person_weight > 0
                )
                SELECT insurance_group,
                       COUNT(*) AS n,
                       ROUND(AVG(age), 1) AS mean_age,
                       ROUND(AVG(total_expenditure), 0) AS mean_expenditure,
                       ROUND(AVG(out_of_pocket), 0) AS mean_oop,
                       ROUND(AVG(office_visits), 1) AS mean_office_visits,
                       ROUND(AVG(er_visits), 2) AS mean_er_visits,
                       ROUND(AVG(rx_fills), 1) AS mean_rx_fills,
                       ROUND(AVG(inpatient_discharges), 3) AS mean_ip_discharges,
                       ROUND(AVG(poverty_level_pct), 0) AS mean_poverty_pct
                FROM categorized
                GROUP BY insurance_group
                ORDER BY insurance_group
            """).fetchall()
            columns = [
                "insurance_group", "n", "mean_age", "mean_expenditure", "mean_oop",
                "mean_office_visits", "mean_er_visits", "mean_rx_fills",
                "mean_ip_discharges", "mean_poverty_pct",
            ]
            return {"rows": [dict(zip(columns, r)) for r in rows], "count": len(rows)}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"meps profile failed: {exc}")
