"""Pharmacy Spread Analysis — NADAC acquisition cost vs SDUD reimbursement spread by drug and state."""

from fastapi import APIRouter, HTTPException, Query
from server.db import get_cursor
from server.utils.error_handler import safe_route

router = APIRouter()


@router.get("/api/research/pharmacy-spread/overview")
@safe_route(default_response={})
async def pharmacy_spread_overview():
    """Join NADAC acquisition cost to SDUD reimbursement by NDC to compute per-drug spread."""
    try:
        with get_cursor() as cur:
            rows = cur.execute("""
                WITH latest_nadac AS (
                    SELECT ndc, ndc_description, nadac_per_unit, pricing_unit
                    FROM fact_nadac
                    QUALIFY ROW_NUMBER() OVER (PARTITION BY ndc ORDER BY effective_date DESC) = 1
                ),
                sdud_agg AS (
                    SELECT ndc, product_name,
                           SUM(total_amount_reimbursed) AS total_amount_reimbursed,
                           SUM(units_reimbursed) AS total_units,
                           SUM(number_of_prescriptions) AS total_prescriptions,
                           CASE WHEN SUM(units_reimbursed) > 0
                                THEN SUM(total_amount_reimbursed) / SUM(units_reimbursed)
                                ELSE NULL END AS reimbursement_per_unit
                    FROM fact_sdud_2025
                    WHERE state_code != 'XX' AND units_reimbursed > 0
                    GROUP BY ndc, product_name
                )
                SELECT s.ndc,
                       COALESCE(n.ndc_description, s.product_name) AS drug_name,
                       n.nadac_per_unit,
                       s.reimbursement_per_unit,
                       s.reimbursement_per_unit - n.nadac_per_unit AS spread_per_unit,
                       s.total_units,
                       s.total_amount_reimbursed,
                       (s.reimbursement_per_unit - n.nadac_per_unit) * s.total_units AS total_spread_dollars,
                       s.total_prescriptions
                FROM sdud_agg s
                INNER JOIN latest_nadac n ON s.ndc = n.ndc
                WHERE n.nadac_per_unit > 0 AND s.reimbursement_per_unit > 0
                ORDER BY total_spread_dollars DESC
                LIMIT 500
            """).fetchall()
            columns = [
                "ndc", "drug_name", "nadac_per_unit", "reimbursement_per_unit",
                "spread_per_unit", "total_units", "total_amount_reimbursed",
                "total_spread_dollars", "total_prescriptions",
            ]
            return {"rows": [dict(zip(columns, r)) for r in rows], "count": len(rows)}
    except Exception as e:
        raise HTTPException(status_code=500, detail={"error": "Pharmacy spread overview query failed", "detail": str(e)})


@router.get("/api/research/pharmacy-spread/by-state")
@safe_route(default_response={})
async def pharmacy_spread_by_state():
    """Spread aggregated by state: total reimbursement vs total acquisition cost."""
    try:
        with get_cursor() as cur:
            rows = cur.execute("""
                WITH latest_nadac AS (
                    SELECT ndc, nadac_per_unit
                    FROM fact_nadac
                    QUALIFY ROW_NUMBER() OVER (PARTITION BY ndc ORDER BY effective_date DESC) = 1
                ),
                sdud_state AS (
                    SELECT state_code, ndc,
                           SUM(total_amount_reimbursed) AS total_amount_reimbursed,
                           SUM(units_reimbursed) AS total_units
                    FROM fact_sdud_2025
                    WHERE state_code != 'XX' AND units_reimbursed > 0
                    GROUP BY state_code, ndc
                )
                SELECT s.state_code,
                       COUNT(DISTINCT s.ndc) AS drugs_matched,
                       SUM(s.total_amount_reimbursed) AS total_reimbursement,
                       SUM(n.nadac_per_unit * s.total_units) AS total_acquisition_cost,
                       SUM(s.total_amount_reimbursed) - SUM(n.nadac_per_unit * s.total_units) AS total_spread,
                       CASE WHEN SUM(n.nadac_per_unit * s.total_units) > 0
                            THEN ROUND((SUM(s.total_amount_reimbursed) - SUM(n.nadac_per_unit * s.total_units)) * 100.0
                                       / SUM(n.nadac_per_unit * s.total_units), 1)
                            ELSE NULL END AS spread_pct
                FROM sdud_state s
                INNER JOIN latest_nadac n ON s.ndc = n.ndc
                WHERE n.nadac_per_unit > 0
                GROUP BY s.state_code
                ORDER BY total_spread DESC
            """).fetchall()
            columns = [
                "state_code", "drugs_matched", "total_reimbursement",
                "total_acquisition_cost", "total_spread", "spread_pct",
            ]
            return {"rows": [dict(zip(columns, r)) for r in rows], "count": len(rows)}
    except Exception as e:
        raise HTTPException(status_code=500, detail={"error": "Pharmacy spread by-state query failed", "detail": str(e)})


@router.get("/api/research/pharmacy-spread/top-drugs")
@safe_route(default_response={})
async def pharmacy_spread_top_drugs(limit: int = Query(default=50, ge=1, le=500)):
    """Top drugs ranked by total overpayment (reimbursement above NADAC acquisition cost)."""
    try:
        with get_cursor() as cur:
            rows = cur.execute("""
                WITH latest_nadac AS (
                    SELECT ndc, ndc_description, nadac_per_unit
                    FROM fact_nadac
                    QUALIFY ROW_NUMBER() OVER (PARTITION BY ndc ORDER BY effective_date DESC) = 1
                ),
                sdud_agg AS (
                    SELECT ndc, product_name,
                           SUM(total_amount_reimbursed) AS total_amount_reimbursed,
                           SUM(units_reimbursed) AS total_units,
                           SUM(number_of_prescriptions) AS total_rx,
                           COUNT(DISTINCT state_code) AS state_count
                    FROM fact_sdud_2025
                    WHERE state_code != 'XX' AND units_reimbursed > 0
                    GROUP BY ndc, product_name
                )
                SELECT s.ndc,
                       COALESCE(n.ndc_description, s.product_name) AS drug_name,
                       n.nadac_per_unit,
                       ROUND(s.total_amount_reimbursed / NULLIF(s.total_units, 0), 4) AS avg_reimbursement_per_unit,
                       ROUND(s.total_amount_reimbursed / NULLIF(s.total_units, 0) - n.nadac_per_unit, 4) AS spread_per_unit,
                       s.total_units,
                       s.total_amount_reimbursed,
                       ROUND((s.total_amount_reimbursed / NULLIF(s.total_units, 0) - n.nadac_per_unit) * s.total_units, 2) AS total_overpayment,
                       s.total_rx,
                       s.state_count
                FROM sdud_agg s
                INNER JOIN latest_nadac n ON s.ndc = n.ndc
                WHERE n.nadac_per_unit > 0
                  AND s.total_amount_reimbursed / NULLIF(s.total_units, 0) > n.nadac_per_unit
                ORDER BY total_overpayment DESC
                LIMIT $1
            """, [limit]).fetchall()
            columns = [
                "ndc", "drug_name", "nadac_per_unit", "avg_reimbursement_per_unit",
                "spread_per_unit", "total_units", "total_amount_reimbursed",
                "total_overpayment", "total_rx", "state_count",
            ]
            return {"rows": [dict(zip(columns, r)) for r in rows], "count": len(rows)}
    except Exception as e:
        raise HTTPException(status_code=500, detail={"error": "Pharmacy spread top-drugs query failed", "detail": str(e)})


@router.get("/api/research/pharmacy-spread/stats")
@safe_route(default_response={})
async def pharmacy_spread_stats():
    """Summary statistics: average/median/p90 spread, total overpayment/underpayment, drug counts."""
    try:
        with get_cursor() as cur:
            row = cur.execute("""
                WITH spreads AS (
                    SELECT
                        s.total_amount_reimbursed / NULLIF(s.total_units, 0) - n.nadac_per_unit AS spread,
                        s.total_amount_reimbursed,
                        (s.total_amount_reimbursed / NULLIF(s.total_units, 0) - n.nadac_per_unit) * s.total_units AS total_spread_dollars
                    FROM (
                        SELECT ndc,
                               SUM(total_amount_reimbursed) AS total_amount_reimbursed,
                               SUM(units_reimbursed) AS total_units
                        FROM fact_sdud_2025
                        WHERE state_code != 'XX' AND units_reimbursed > 0
                        GROUP BY ndc
                    ) s
                    INNER JOIN (
                        SELECT ndc, nadac_per_unit
                        FROM fact_nadac
                        QUALIFY ROW_NUMBER() OVER (PARTITION BY ndc ORDER BY effective_date DESC) = 1
                    ) n ON s.ndc = n.ndc
                    WHERE n.nadac_per_unit > 0 AND s.total_units > 0
                )
                SELECT
                    COUNT(*) AS drugs_analyzed,
                    ROUND(AVG(spread), 4) AS avg_spread_per_unit,
                    ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY spread), 4) AS median_spread_per_unit,
                    ROUND(PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY spread), 4) AS p90_spread_per_unit,
                    ROUND(SUM(CASE WHEN spread > 0 THEN total_spread_dollars ELSE 0 END), 0) AS total_overpayment,
                    ROUND(SUM(CASE WHEN spread < 0 THEN total_spread_dollars ELSE 0 END), 0) AS total_underpayment,
                    COUNT(*) FILTER (WHERE spread > 0) AS drugs_overpaid,
                    COUNT(*) FILTER (WHERE spread < 0) AS drugs_underpaid
                FROM spreads
            """).fetchone()
            columns = [
                "drugs_analyzed", "avg_spread_per_unit", "median_spread_per_unit",
                "p90_spread_per_unit", "total_overpayment", "total_underpayment",
                "drugs_overpaid", "drugs_underpaid",
            ]
            return dict(zip(columns, row)) if row else {}
    except Exception as e:
        raise HTTPException(status_code=500, detail={"error": "Pharmacy spread stats query failed", "detail": str(e)})
