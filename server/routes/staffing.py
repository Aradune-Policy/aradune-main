"""Nursing facility staffing routes — PBJ daily staffing data."""

from fastapi import APIRouter, Query
from server.db import get_cursor
from server.utils.error_handler import safe_route

router = APIRouter()


@router.get("/api/staffing/summary")
@safe_route(default_response=[])
async def staffing_summary():
    """Get state-level nursing facility staffing summary from PBJ data."""
    with get_cursor() as cur:
        rows = cur.execute("""
            SELECT
                state_code,
                COUNT(DISTINCT provider_ccn) AS facility_count,
                ROUND(AVG(nursing_hprd), 2) AS avg_nursing_hprd,
                ROUND(MEDIAN(nursing_hprd), 2) AS median_nursing_hprd,
                ROUND(AVG(CASE WHEN hrs_rn + hrs_lpn + hrs_cna > 0
                    THEN hrs_rn / (hrs_rn + hrs_lpn + hrs_cna) END) * 100, 1)
                    AS avg_rn_pct,
                ROUND(AVG(CASE WHEN hrs_rn + hrs_lpn + hrs_cna > 0
                    THEN hrs_cna_contract / (hrs_rn + hrs_lpn + hrs_cna) END) * 100, 1)
                    AS avg_contract_pct,
                SUM(mds_census) AS total_resident_days
            FROM fact_pbj_nurse_staffing
            WHERE nursing_hprd > 0 AND nursing_hprd < 20
            GROUP BY state_code
            ORDER BY avg_nursing_hprd
        """).fetchall()
        columns = ["state_code", "facility_count", "avg_nursing_hprd",
                    "median_nursing_hprd", "avg_rn_pct", "avg_contract_pct",
                    "total_resident_days"]
        return [dict(zip(columns, r)) for r in rows]


@router.get("/api/staffing/{state_code}")
@safe_route(default_response=[])
async def state_staffing(
    state_code: str,
    limit: int = Query(200, le=1000),
):
    """Get facility-level staffing averages for a state."""
    state_code = state_code.upper()
    with get_cursor() as cur:
        rows = cur.execute("""
            SELECT
                provider_ccn, facility_name, city, county,
                ROUND(AVG(nursing_hprd), 2) AS avg_nursing_hprd,
                ROUND(AVG(hrs_rn / NULLIF(mds_census, 0)), 2) AS avg_rn_hprd,
                ROUND(AVG(hrs_cna / NULLIF(mds_census, 0)), 2) AS avg_cna_hprd,
                ROUND(AVG(mds_census), 0) AS avg_census,
                ROUND(SUM(hrs_rn_contract) / NULLIF(SUM(hrs_rn), 0) * 100, 1)
                    AS rn_contract_pct,
                COUNT(*) AS days_reported
            FROM fact_pbj_nurse_staffing
            WHERE state_code = $1 AND nursing_hprd > 0
            GROUP BY provider_ccn, facility_name, city, county
            ORDER BY avg_nursing_hprd ASC
            LIMIT $2
        """, [state_code, limit]).fetchall()
        columns = ["provider_ccn", "facility_name", "city", "county",
                    "avg_nursing_hprd", "avg_rn_hprd", "avg_cna_hprd",
                    "avg_census", "rn_contract_pct", "days_reported"]
        return [dict(zip(columns, r)) for r in rows]
