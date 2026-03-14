"""Safety Net Stress Test — hospital financial stress, LTSS pressure, staffing crisis, and composite vulnerability index."""

from fastapi import APIRouter, HTTPException, Query
from server.db import get_cursor

router = APIRouter()


@router.get("/api/research/safety-net/hospital-stress")
async def hospital_stress(state: str = Query(None)):
    """Hospital financial stress by state: negative margins, uncompensated care, DSH, Medicaid days."""
    try:
        with get_cursor() as cur:
            params = []
            state_filter = ""
            if state:
                state_filter = "WHERE state_code = $1"
                params.append(state.upper())
            rows = cur.execute(f"""
                SELECT state_code,
                       COUNT(*) AS total_hospitals,
                       COUNT(*) FILTER (WHERE net_income < 0) AS negative_margin_count,
                       ROUND(COUNT(*) FILTER (WHERE net_income < 0) * 100.0 / NULLIF(COUNT(*), 0), 1) AS pct_negative_margin,
                       ROUND(AVG(CASE WHEN net_patient_revenue > 0 THEN net_income * 100.0 / net_patient_revenue END), 1) AS avg_operating_margin,
                       ROUND(AVG(uncompensated_care_cost), 0) AS avg_uncompensated_care,
                       ROUND(AVG(dsh_adjustment), 0) AS avg_dsh_payment,
                       ROUND(AVG(medicaid_day_pct), 1) AS avg_medicaid_day_pct
                FROM fact_hospital_cost
                {state_filter}
                GROUP BY state_code
                ORDER BY pct_negative_margin DESC
            """, params).fetchall()
            columns = [
                "state_code", "total_hospitals", "negative_margin_count",
                "pct_negative_margin", "avg_operating_margin", "avg_uncompensated_care",
                "avg_dsh_payment", "avg_medicaid_day_pct",
            ]
            return {"rows": [dict(zip(columns, r)) for r in rows], "count": len(rows)}
    except Exception as e:
        raise HTTPException(status_code=500, detail={"error": "Hospital stress query failed", "detail": str(e)})


@router.get("/api/research/safety-net/ltss-pressure")
async def ltss_pressure(state: str = Query(None)):
    """LTSS pressure: HCBS waitlists combined with nursing facility quality ratings."""
    try:
        with get_cursor() as cur:
            params = []
            state_filter_w = ""
            state_filter_n = ""
            state_filter_e = ""
            if state:
                state_filter_w = "WHERE state_code = $1"
                state_filter_n = "WHERE state_code = $1"
                state_filter_e = "AND state_code = $1"
                params = [state.upper(), state.upper(), state.upper()]
            rows = cur.execute(f"""
                WITH waitlists AS (
                    SELECT state_code,
                           SUM(waitlist_count) AS total_waitlist
                    FROM fact_hcbs_waitlist
                    {state_filter_w}
                    GROUP BY state_code
                ),
                nursing AS (
                    SELECT state_code,
                           ROUND(AVG(overall_rating), 2) AS avg_nursing_rating,
                           ROUND(AVG(staffing_rating), 2) AS avg_staffing_rating,
                           COUNT(*) AS facility_count
                    FROM fact_five_star
                    {state_filter_n}
                    GROUP BY state_code
                ),
                enrollment AS (
                    SELECT state_code, MAX(total_enrollment) AS total_enrollment
                    FROM fact_enrollment
                    WHERE year = (SELECT MAX(year) FROM fact_enrollment)
                    {state_filter_e}
                    GROUP BY state_code
                )
                SELECT COALESCE(w.state_code, n.state_code) AS state_code,
                       COALESCE(w.total_waitlist, 0) AS total_waitlist,
                       n.avg_nursing_rating,
                       n.avg_staffing_rating,
                       n.facility_count,
                       e.total_enrollment,
                       CASE WHEN e.total_enrollment > 0
                            THEN ROUND(COALESCE(w.total_waitlist, 0) * 1000.0 / e.total_enrollment, 1)
                            ELSE 0 END AS waitlist_per_1000
                FROM waitlists w
                FULL OUTER JOIN nursing n ON w.state_code = n.state_code
                LEFT JOIN enrollment e ON COALESCE(w.state_code, n.state_code) = e.state_code
                ORDER BY total_waitlist DESC
            """, params).fetchall()
            columns = [
                "state_code", "total_waitlist", "avg_nursing_rating",
                "avg_staffing_rating", "facility_count", "total_enrollment",
                "waitlist_per_1000",
            ]
            return {"rows": [dict(zip(columns, r)) for r in rows], "count": len(rows)}
    except Exception as e:
        raise HTTPException(status_code=500, detail={"error": "LTSS pressure query failed", "detail": str(e)})


@router.get("/api/research/safety-net/staffing-crisis")
async def staffing_crisis(state: str = Query(None)):
    """PBJ staffing analysis by state: hours per resident day, contract nurse %, below-minimum facilities."""
    try:
        with get_cursor() as cur:
            params = []
            state_filter = ""
            if state:
                state_filter = "WHERE state_code = $1"
                params.append(state.upper())

            # Try with pre-computed nursing_hprd first; fall back to computing from components
            try:
                rows = cur.execute(f"""
                    SELECT state_code,
                           ROUND(AVG(nursing_hprd), 2) AS avg_total_hprd,
                           ROUND(AVG(hrs_rn / NULLIF(mds_census, 0) * 24), 2) AS avg_rn_hprd,
                           ROUND(AVG(hrs_cna / NULLIF(mds_census, 0) * 24), 2) AS avg_cna_hprd,
                           ROUND(AVG(CASE WHEN nursing_hprd > 0
                                     THEN COALESCE(hrs_rn_contract, 0) * 100.0 / NULLIF(hrs_rn + 0.001, 0)
                                     END), 1) AS contract_rn_pct,
                           COUNT(DISTINCT provider_ccn) AS facilities_reporting,
                           COUNT(*) FILTER (WHERE nursing_hprd < 3.48) AS below_minimum_count
                    FROM fact_pbj_nurse_staffing
                    {state_filter}
                    GROUP BY state_code
                    ORDER BY avg_total_hprd
                """, params).fetchall()
            except Exception:
                # Fallback: compute nursing_hprd from total_nursing_hrs / mds_census
                rows = cur.execute(f"""
                    SELECT state_code,
                           ROUND(AVG(COALESCE(total_nursing_hrs, 0) / NULLIF(mds_census, 0)), 2) AS avg_total_hprd,
                           ROUND(AVG(hrs_rn / NULLIF(mds_census, 0) * 24), 2) AS avg_rn_hprd,
                           ROUND(AVG(hrs_cna / NULLIF(mds_census, 0) * 24), 2) AS avg_cna_hprd,
                           ROUND(AVG(CASE WHEN COALESCE(total_nursing_hrs, 0) / NULLIF(mds_census, 0) > 0
                                     THEN COALESCE(hrs_rn_contract, 0) * 100.0 / NULLIF(hrs_rn + 0.001, 0)
                                     END), 1) AS contract_rn_pct,
                           COUNT(DISTINCT provider_ccn) AS facilities_reporting,
                           COUNT(*) FILTER (WHERE COALESCE(total_nursing_hrs, 0) / NULLIF(mds_census, 0) < 3.48) AS below_minimum_count
                    FROM fact_pbj_nurse_staffing
                    {state_filter}
                    GROUP BY state_code
                    ORDER BY avg_total_hprd
                """, params).fetchall()

            columns = [
                "state_code", "avg_total_hprd", "avg_rn_hprd", "avg_cna_hprd",
                "contract_rn_pct", "facilities_reporting", "below_minimum_count",
            ]
            return {"rows": [dict(zip(columns, r)) for r in rows], "count": len(rows)}
    except Exception as e:
        raise HTTPException(status_code=500, detail={"error": "Staffing crisis query failed", "detail": str(e)})


@router.get("/api/research/safety-net/composite")
async def safety_net_composite(state: str = Query(None)):
    """Combined safety net stress index: hospital margins, HCBS waitlists, nursing quality deficit, FMAP."""
    try:
        with get_cursor() as cur:
            params = []
            state_filter = ""
            if state:
                state_filter = "WHERE d.state_code = $1"
                params.append(state.upper())
            rows = cur.execute(f"""
                WITH hospital AS (
                    SELECT state_code,
                           ROUND(COUNT(*) FILTER (WHERE net_income < 0) * 100.0 / NULLIF(COUNT(*), 0), 1) AS pct_negative_margin
                    FROM fact_hospital_cost
                    GROUP BY state_code
                ),
                waitlists AS (
                    SELECT w.state_code,
                           ROUND(SUM(w.waitlist_count) * 1000.0 / NULLIF(MAX(e.total_enrollment), 0), 1) AS waitlist_per_1000
                    FROM fact_hcbs_waitlist w
                    LEFT JOIN (
                        SELECT state_code, MAX(total_enrollment) AS total_enrollment
                        FROM fact_enrollment
                        WHERE year = (SELECT MAX(year) FROM fact_enrollment)
                        GROUP BY state_code
                    ) e ON w.state_code = e.state_code
                    GROUP BY w.state_code
                ),
                nursing AS (
                    SELECT state_code, AVG(overall_rating) AS avg_rating
                    FROM fact_five_star
                    GROUP BY state_code
                ),
                fmap AS (
                    SELECT state_code, fmap_rate
                    FROM fact_fmap_historical
                    WHERE fiscal_year = (SELECT MAX(fiscal_year) FROM fact_fmap_historical)
                )
                SELECT d.state_code,
                       COALESCE(h.pct_negative_margin, 0) AS hospital_stress,
                       COALESCE(w.waitlist_per_1000, 0) AS hcbs_pressure,
                       ROUND(COALESCE(5 - n.avg_rating, 0), 2) AS nursing_deficit,
                       COALESCE(f.fmap_rate, 0.5) AS fmap_rate
                FROM dim_state d
                LEFT JOIN hospital h ON d.state_code = h.state_code
                LEFT JOIN waitlists w ON d.state_code = w.state_code
                LEFT JOIN nursing n ON d.state_code = n.state_code
                LEFT JOIN fmap f ON d.state_code = f.state_code
                {state_filter}
                ORDER BY hospital_stress + hcbs_pressure + nursing_deficit DESC
            """, params).fetchall()
            columns = ["state_code", "hospital_stress", "hcbs_pressure", "nursing_deficit", "fmap_rate"]
            return {"rows": [dict(zip(columns, r)) for r in rows], "count": len(rows)}
    except Exception as e:
        raise HTTPException(status_code=500, detail={"error": "Safety net composite query failed", "detail": str(e)})
