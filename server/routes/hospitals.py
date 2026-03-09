"""Hospital cost report routes — HCRIS hospital and SNF financial data."""

from fastapi import APIRouter, HTTPException, Query
from server.db import get_cursor

router = APIRouter()


@router.get("/api/hospitals/search")
async def search_hospitals(
    q: str = Query(..., min_length=2, description="Search by name, city, or CCN"),
    state: str = Query(None, description="Optional state filter"),
    limit: int = Query(25, le=100),
):
    """Search hospitals by name, city, or CCN. Returns lightweight results for autocomplete."""
    q_like = f"%{q.strip().upper()}%"
    with get_cursor() as cur:
        state_filter = "AND state_code = $3" if state else ""
        params = [q_like, limit] + ([state.upper()] if state else [])
        rows = cur.execute(f"""
            SELECT provider_ccn, hospital_name, city, state_code, bed_count
            FROM fact_hospital_cost
            WHERE (
                UPPER(hospital_name) LIKE $1
                OR UPPER(city) LIKE $1
                OR provider_ccn LIKE $1
            )
            {state_filter}
            ORDER BY bed_count DESC NULLS LAST
            LIMIT $2
        """, params).fetchall()
        return [
            dict(zip(["ccn", "name", "city", "state", "beds"], r))
            for r in rows
        ]


@router.get("/api/hospitals/ccn/{ccn}")
async def hospital_by_ccn(ccn: str):
    """Get all available HCRIS data for a specific hospital CCN."""
    ccn = ccn.strip()
    with get_cursor() as cur:
        rows = cur.execute("""
            SELECT provider_ccn, hospital_name, state_code, city, county,
                   street_address, zip_code, cbsa_code,
                   facility_type, rural_urban, control_type_code,
                   bed_count, fte_employees, fte_residents,
                   medicare_days, medicaid_days, total_days,
                   medicare_discharges, medicaid_discharges, total_discharges,
                   net_patient_revenue, net_income, total_income,
                   inpatient_revenue, outpatient_revenue,
                   medicaid_net_revenue, medicaid_charges,
                   total_costs, total_salaries,
                   cost_to_charge_ratio,
                   charity_care_cost, bad_debt_expense, uncompensated_care_cost,
                   dsh_adjustment, dsh_pct, ime_payment,
                   total_assets, total_liabilities,
                   medicaid_day_pct, medicaid_payment_to_charge_pct,
                   report_year
            FROM fact_hospital_cost
            WHERE provider_ccn = $1
        """, [ccn]).fetchall()
        if not rows:
            raise HTTPException(status_code=404, detail={
                "found": False,
                "message": "CCN not found in HCRIS data",
            })
        columns = [
            "provider_ccn", "hospital_name", "state_code", "city", "county",
            "street_address", "zip_code", "cbsa_code",
            "facility_type", "rural_urban", "control_type_code",
            "bed_count", "fte_employees", "fte_residents",
            "medicare_days", "medicaid_days", "total_days",
            "medicare_discharges", "medicaid_discharges", "total_discharges",
            "net_patient_revenue", "net_income", "total_income",
            "inpatient_revenue", "outpatient_revenue",
            "medicaid_net_revenue", "medicaid_charges",
            "total_costs", "total_salaries",
            "cost_to_charge_ratio",
            "charity_care_cost", "bad_debt_expense", "uncompensated_care_cost",
            "dsh_adjustment", "dsh_pct", "ime_payment",
            "total_assets", "total_liabilities",
            "medicaid_day_pct", "medicaid_payment_to_charge_pct",
            "report_year",
        ]
        return [dict(zip(columns, r)) for r in rows]


@router.get("/api/hospitals/ccn/{ccn}/peers")
async def hospital_peers(ccn: str):
    """Get peer benchmark statistics for a hospital's designation and state."""
    ccn = ccn.strip()
    with get_cursor() as cur:
        # First get the hospital's state and rural/urban designation
        hosp = cur.execute("""
            SELECT state_code, rural_urban, facility_type
            FROM fact_hospital_cost WHERE provider_ccn = $1
        """, [ccn]).fetchone()
        if not hosp:
            raise HTTPException(status_code=404, detail={
                "found": False,
                "message": "CCN not found in HCRIS data",
            })
        state_code, rural_urban, facility_type = hosp

        # State peers — same state, same rural/urban designation
        state_peers = cur.execute("""
            SELECT
                COUNT(*) AS n,
                ROUND(MEDIAN(
                    CASE WHEN net_patient_revenue > 0
                    THEN (net_income::DOUBLE / net_patient_revenue) * 100 END
                ), 2) AS median_operating_margin,
                ROUND(PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY
                    CASE WHEN net_patient_revenue > 0
                    THEN (net_income::DOUBLE / net_patient_revenue) * 100 END
                ), 2) AS p25_operating_margin,
                ROUND(PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY
                    CASE WHEN net_patient_revenue > 0
                    THEN (net_income::DOUBLE / net_patient_revenue) * 100 END
                ), 2) AS p75_operating_margin,
                ROUND(MEDIAN(cost_to_charge_ratio), 4) AS median_ccr,
                ROUND(MEDIAN(bed_count), 0) AS median_beds,
                ROUND(MEDIAN(
                    CASE WHEN net_patient_revenue > 0
                    THEN (uncompensated_care_cost::DOUBLE / net_patient_revenue) * 100 END
                ), 2) AS median_uc_pct,
                ROUND(MEDIAN(
                    CASE WHEN net_patient_revenue > 0
                    THEN ((COALESCE(dsh_adjustment,0) + COALESCE(ime_payment,0))::DOUBLE
                          / net_patient_revenue) * 100 END
                ), 2) AS median_supplemental_pct,
                ROUND(MEDIAN(medicaid_day_pct), 2) AS median_medicaid_day_pct,
                ROUND(MEDIAN(
                    CASE WHEN total_days > 0
                    THEN (medicare_days::DOUBLE / total_days) * 100 END
                ), 2) AS median_medicare_day_pct
            FROM fact_hospital_cost
            WHERE state_code = $1
        """, [state_code]).fetchone()

        # National peers — same rural/urban designation
        national_peers = cur.execute("""
            SELECT
                COUNT(*) AS n,
                ROUND(MEDIAN(
                    CASE WHEN net_patient_revenue > 0
                    THEN (net_income::DOUBLE / net_patient_revenue) * 100 END
                ), 2) AS median_operating_margin,
                ROUND(PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY
                    CASE WHEN net_patient_revenue > 0
                    THEN (net_income::DOUBLE / net_patient_revenue) * 100 END
                ), 2) AS p25_operating_margin,
                ROUND(PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY
                    CASE WHEN net_patient_revenue > 0
                    THEN (net_income::DOUBLE / net_patient_revenue) * 100 END
                ), 2) AS p75_operating_margin,
                ROUND(MEDIAN(cost_to_charge_ratio), 4) AS median_ccr,
                ROUND(MEDIAN(bed_count), 0) AS median_beds,
                ROUND(MEDIAN(
                    CASE WHEN net_patient_revenue > 0
                    THEN (uncompensated_care_cost::DOUBLE / net_patient_revenue) * 100 END
                ), 2) AS median_uc_pct,
                ROUND(MEDIAN(
                    CASE WHEN net_patient_revenue > 0
                    THEN ((COALESCE(dsh_adjustment,0) + COALESCE(ime_payment,0))::DOUBLE
                          / net_patient_revenue) * 100 END
                ), 2) AS median_supplemental_pct,
                ROUND(MEDIAN(medicaid_day_pct), 2) AS median_medicaid_day_pct,
                ROUND(MEDIAN(
                    CASE WHEN total_days > 0
                    THEN (medicare_days::DOUBLE / total_days) * 100 END
                ), 2) AS median_medicare_day_pct
            FROM fact_hospital_cost
            WHERE rural_urban = $1
        """, [rural_urban]).fetchone()

        peer_cols = [
            "n", "median_operating_margin", "p25_operating_margin", "p75_operating_margin",
            "median_ccr", "median_beds", "median_uc_pct",
            "median_supplemental_pct", "median_medicaid_day_pct", "median_medicare_day_pct",
        ]
        return {
            "hospital": {"state_code": state_code, "rural_urban": rural_urban, "facility_type": facility_type},
            "state_peers": dict(zip(peer_cols, state_peers)) if state_peers else None,
            "national_peers": dict(zip(peer_cols, national_peers)) if national_peers else None,
        }


@router.get("/api/hospitals/{state_code}")
async def state_hospitals(
    state_code: str,
    min_beds: int = Query(0, description="Minimum bed count"),
    limit: int = Query(200, le=1000),
):
    """Get hospital cost report data for a state."""
    state_code = state_code.upper()
    with get_cursor() as cur:
        rows = cur.execute("""
            SELECT provider_ccn, hospital_name, city, county, cbsa_code,
                   rural_urban, bed_count, fte_employees,
                   medicare_days, medicaid_days, total_days,
                   total_costs, net_patient_revenue, net_income,
                   medicaid_net_revenue, medicaid_charges,
                   uncompensated_care_cost, dsh_adjustment, dsh_pct,
                   ime_payment, cost_to_charge_ratio,
                   medicaid_day_pct, medicaid_payment_to_charge_pct,
                   report_year
            FROM fact_hospital_cost
            WHERE state_code = $1 AND COALESCE(bed_count, 0) >= $2
            ORDER BY medicaid_net_revenue DESC NULLS LAST
            LIMIT $3
        """, [state_code, min_beds, limit]).fetchall()
        columns = ["provider_ccn", "hospital_name", "city", "county", "cbsa_code",
                    "rural_urban", "bed_count", "fte_employees",
                    "medicare_days", "medicaid_days", "total_days",
                    "total_costs", "net_patient_revenue", "net_income",
                    "medicaid_net_revenue", "medicaid_charges",
                    "uncompensated_care_cost", "dsh_adjustment", "dsh_pct",
                    "ime_payment", "cost_to_charge_ratio",
                    "medicaid_day_pct", "medicaid_payment_to_charge_pct",
                    "report_year"]
        return [dict(zip(columns, r)) for r in rows]


@router.get("/api/hospitals/summary")
async def hospital_summary():
    """Get state-level hospital financial summaries."""
    with get_cursor() as cur:
        rows = cur.execute("""
            SELECT
                state_code,
                COUNT(*) AS hospital_count,
                SUM(bed_count) AS total_beds,
                SUM(medicaid_days) AS total_medicaid_days,
                SUM(total_days) AS total_patient_days,
                ROUND(SUM(medicaid_days)::DOUBLE / NULLIF(SUM(total_days), 0) * 100, 1)
                    AS medicaid_day_pct,
                SUM(medicaid_net_revenue) AS total_medicaid_revenue,
                SUM(uncompensated_care_cost) AS total_uncompensated_care,
                SUM(dsh_adjustment) AS total_dsh,
                ROUND(MEDIAN(cost_to_charge_ratio), 4) AS median_ccr,
                report_year
            FROM fact_hospital_cost
            WHERE state_code IS NOT NULL
            GROUP BY state_code, report_year
            ORDER BY state_code
        """).fetchall()
        columns = ["state_code", "hospital_count", "total_beds",
                    "total_medicaid_days", "total_patient_days", "medicaid_day_pct",
                    "total_medicaid_revenue", "total_uncompensated_care",
                    "total_dsh", "median_ccr", "report_year"]
        return [dict(zip(columns, r)) for r in rows]


@router.get("/api/nursing-facilities/{state_code}")
async def state_nursing_facilities(
    state_code: str,
    limit: int = Query(200, le=1000),
):
    """Get nursing facility cost report data for a state."""
    state_code = state_code.upper()
    with get_cursor() as cur:
        rows = cur.execute("""
            SELECT provider_ccn, facility_name, city, county,
                   total_beds, snf_beds, nf_beds,
                   medicare_days, medicaid_days, total_days,
                   nf_medicaid_days, nf_total_days,
                   total_costs, net_patient_revenue, net_income,
                   medicaid_day_pct, occupancy_pct,
                   report_year
            FROM fact_snf_cost
            WHERE state_code = $1
            ORDER BY medicaid_days DESC NULLS LAST
            LIMIT $2
        """, [state_code, limit]).fetchall()
        columns = ["provider_ccn", "facility_name", "city", "county",
                    "total_beds", "snf_beds", "nf_beds",
                    "medicare_days", "medicaid_days", "total_days",
                    "nf_medicaid_days", "nf_total_days",
                    "total_costs", "net_patient_revenue", "net_income",
                    "medicaid_day_pct", "occupancy_pct",
                    "report_year"]
        return [dict(zip(columns, r)) for r in rows]
