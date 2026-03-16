"""T-MSIS Claims vs Fee Schedule Calibration — how claims compare to published rates."""

from fastapi import APIRouter, HTTPException, Query
from server.db import get_cursor

router = APIRouter()


@router.get("/api/research/tmsis-calibration/state-summary")
async def tmsis_state_summary():
    """State-level T-MSIS claims vs fee schedule calibration factors."""
    try:
        with get_cursor() as cur:
            rows = cur.execute("""
                SELECT state_code, service_category,
                       n_codes,
                       ROUND(median_claims_to_fs * 100, 1) AS claims_pct_of_fs,
                       ROUND(median_claims_to_medicare * 100, 1) AS claims_pct_of_medicare,
                       ROUND(median_fs_to_medicare * 100, 1) AS fs_pct_of_medicare,
                       ROUND(p25_claims_to_fs * 100, 1) AS p25_pct,
                       ROUND(p75_claims_to_fs * 100, 1) AS p75_pct
                FROM fact_tmsis_calibration
                ORDER BY state_code, service_category
            """).fetchall()
            columns = [
                "state_code", "service_category", "n_codes",
                "claims_pct_of_fs", "claims_pct_of_medicare", "fs_pct_of_medicare",
                "p25_pct", "p75_pct",
            ]
            return {"rows": [dict(zip(columns, r)) for r in rows], "count": len(rows)}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"tmsis calibration failed: {exc}")


@router.get("/api/research/tmsis-calibration/tn-simulation")
async def tn_simulation(category: str = Query(default=None)):
    """Tennessee simulated fee schedule from T-MSIS claims calibrated via SE states."""
    try:
        with get_cursor() as cur:
            params = []
            cat_filter = ""
            if category:
                cat_filter = "AND service_category = $1"
                params.append(category)
            rows = cur.execute(f"""
                SELECT procedure_code, description, service_category,
                       claims_avg_paid, medicare_rate, claims_pct_medicare,
                       simulated_fs_mid, simulated_fs_low, simulated_fs_high,
                       simulated_pct_medicare, calibration_factor, months_observed
                FROM fact_tn_simulated_fee_schedule
                WHERE claims_avg_paid > 0 {cat_filter}
                ORDER BY procedure_code
                LIMIT 2000
            """, params).fetchall()
            columns = [
                "procedure_code", "description", "service_category",
                "claims_avg_paid", "medicare_rate", "claims_pct_medicare",
                "simulated_fs_mid", "simulated_fs_low", "simulated_fs_high",
                "simulated_pct_medicare", "calibration_factor", "months_observed",
            ]
            return {"rows": [dict(zip(columns, r)) for r in rows], "count": len(rows)}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"tn simulation failed: {exc}")


@router.get("/api/research/tmsis-calibration/effective-rates")
async def effective_rates(state: str = Query(default="TN"), limit: int = Query(default=100, le=500)):
    """T-MSIS effective paid rates for any state. Clearly labeled as claims-based, not fee schedule."""
    try:
        with get_cursor() as cur:
            rows = cur.execute("""
                SELECT state_code, procedure_code, description,
                       effective_paid_rate, medicare_rate, pct_of_medicare,
                       months_observed, total_paid_volume,
                       rate_source
                FROM fact_tmsis_effective_rates
                WHERE state_code = $1
                ORDER BY total_paid_volume DESC
                LIMIT $2
            """, [state.upper(), limit]).fetchall()
            columns = [
                "state_code", "procedure_code", "description",
                "effective_paid_rate", "medicare_rate", "pct_of_medicare",
                "months_observed", "total_paid_volume", "rate_source",
            ]
            return {"rows": [dict(zip(columns, r)) for r in rows], "count": len(rows)}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"effective rates failed: {exc}")
