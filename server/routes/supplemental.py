"""Supplemental payment endpoints — DSH, UPL, State Directed Payments."""
from fastapi import APIRouter, Query
from server.db import get_cursor

router = APIRouter()


@router.get("/api/supplemental/summary")
def supplemental_summary(fiscal_year: int = Query(default=2024)):
    """MACPAC Exhibit 24 — state-level hospital supplemental payment summary."""
    with get_cursor() as cur:
        rows = cur.execute("""
            SELECT state_code, fiscal_year,
                   total_hospital_payments_m, dsh_payments_m,
                   non_dsh_supplemental_m, sec_1115_waiver_m,
                   supplemental_pct
            FROM fact_macpac_supplemental
            WHERE fiscal_year = ?
            ORDER BY state_code
        """, [fiscal_year]).fetchall()
        cols = ["state", "fiscal_year", "total_hospital_payments_m",
                "dsh_payments_m", "non_dsh_supplemental_m",
                "sec_1115_waiver_m", "supplemental_pct"]
        return {
            "fiscal_year": fiscal_year,
            "states": [dict(zip(cols, r)) for r in rows],
            "count": len(rows),
        }


@router.get("/api/supplemental/fmr")
def supplemental_fmr(
    fiscal_year: int = Query(default=2024),
    state: str = Query(default=None),
    payment_type: str = Query(default=None),
):
    """CMS-64 FMR supplemental payment line items by state."""
    with get_cursor() as cur:
        sql = """
            SELECT state_code, fiscal_year, category, payment_type,
                   service, program, total_computable, federal_share, state_share
            FROM fact_fmr_supplemental
            WHERE fiscal_year = ?
        """
        params = [fiscal_year]
        if state:
            sql += " AND state_code = ?"
            params.append(state.upper())
        if payment_type:
            sql += " AND payment_type = ?"
            params.append(payment_type)
        sql += " ORDER BY state_code, service"

        rows = cur.execute(sql, params).fetchall()
        cols = ["state", "fiscal_year", "category", "payment_type",
                "service", "program", "total_computable", "federal_share", "state_share"]
        return {
            "fiscal_year": fiscal_year,
            "rows": [dict(zip(cols, r)) for r in rows],
            "count": len(rows),
        }


@router.get("/api/supplemental/fmr/totals")
def supplemental_fmr_totals(fiscal_year: int = Query(default=2024)):
    """Aggregate FMR supplemental payments by state and payment type."""
    with get_cursor() as cur:
        rows = cur.execute("""
            SELECT state_code, payment_type,
                   SUM(total_computable) AS total_computable,
                   SUM(federal_share) AS federal_share,
                   SUM(state_share) AS state_share
            FROM fact_fmr_supplemental
            WHERE fiscal_year = ?
            GROUP BY state_code, payment_type
            ORDER BY state_code, payment_type
        """, [fiscal_year]).fetchall()
        cols = ["state", "payment_type", "total_computable", "federal_share", "state_share"]
        return {
            "fiscal_year": fiscal_year,
            "rows": [dict(zip(cols, r)) for r in rows],
            "count": len(rows),
        }


@router.get("/api/supplemental/dsh/hospitals")
def supplemental_dsh_hospitals(
    state: str = Query(default=None),
    min_dsh: float = Query(default=0),
):
    """Hospital-level DSH data from HCRIS cost reports."""
    with get_cursor() as cur:
        sql = """
            SELECT provider_ccn, hospital_name, state_code, city, county,
                   rural_urban, bed_count, dsh_adjustment, dsh_pct,
                   ime_payment, medicaid_days, total_days, medicaid_day_pct,
                   medicaid_net_revenue, uncompensated_care_cost,
                   charity_care_cost, dsh_status, dsh_to_uc_pct
            FROM fact_dsh_hospital
            WHERE dsh_adjustment >= ?
        """
        params: list = [min_dsh]
        if state:
            sql += " AND state_code = ?"
            params.append(state.upper())
        sql += " ORDER BY dsh_adjustment DESC LIMIT 500"

        rows = cur.execute(sql, params).fetchall()
        cols = [
            "provider_ccn", "hospital_name", "state_code", "city", "county",
            "rural_urban", "bed_count", "dsh_adjustment", "dsh_pct",
            "ime_payment", "medicaid_days", "total_days", "medicaid_day_pct",
            "medicaid_net_revenue", "uncompensated_care_cost",
            "charity_care_cost", "dsh_status", "dsh_to_uc_pct",
        ]
        return {
            "rows": [dict(zip(cols, r)) for r in rows],
            "count": len(rows),
        }


@router.get("/api/supplemental/dsh/summary")
def supplemental_dsh_summary():
    """State-level DSH summary aggregated from hospital-level HCRIS data."""
    with get_cursor() as cur:
        rows = cur.execute("""
            SELECT
                state_code,
                COUNT(*) AS total_hospitals,
                COUNT(*) FILTER (WHERE dsh_adjustment > 0) AS dsh_recipients,
                ROUND(SUM(dsh_adjustment) / 1e6, 2) AS total_dsh_m,
                ROUND(SUM(ime_payment) / 1e6, 2) AS total_ime_m,
                ROUND(SUM(uncompensated_care_cost) / 1e6, 2) AS total_uc_m,
                ROUND(AVG(medicaid_day_pct), 1) AS avg_medicaid_day_pct,
                COUNT(*) FILTER (WHERE medicaid_day_pct > 25) AS high_medicaid_hospitals
            FROM fact_dsh_hospital
            GROUP BY state_code
            ORDER BY total_dsh_m DESC
        """).fetchall()
        cols = ["state", "total_hospitals", "dsh_recipients", "total_dsh_m",
                "total_ime_m", "total_uc_m", "avg_medicaid_day_pct", "high_medicaid_hospitals"]
        return {
            "rows": [dict(zip(cols, r)) for r in rows],
            "count": len(rows),
        }


@router.get("/api/supplemental/sdp")
def supplemental_sdp(state: str = Query(default=None)):
    """CMS-approved State Directed Payment programs."""
    with get_cursor() as cur:
        sql = """
            SELECT state_code, program_name, service_category,
                   payment_type, fiscal_year, authority
            FROM fact_sdp_preprint
        """
        params = []
        if state:
            sql += " WHERE state_code = ?"
            params.append(state.upper())
        sql += " ORDER BY state_code"

        rows = cur.execute(sql, params).fetchall()
        cols = ["state", "program_name", "service_category",
                "payment_type", "fiscal_year", "authority"]
        return {
            "rows": [dict(zip(cols, r)) for r in rows],
            "count": len(rows),
        }


@router.get("/api/supplemental/trend")
def supplemental_trend(state: str = Query(default=None)):
    """FMR supplemental payment trends across fiscal years."""
    with get_cursor() as cur:
        sql = """
            SELECT state_code, fiscal_year, payment_type,
                   SUM(total_computable) AS total_computable
            FROM fact_fmr_supplemental
        """
        params = []
        if state:
            sql += " WHERE state_code = ?"
            params.append(state.upper())
        sql += " GROUP BY state_code, fiscal_year, payment_type ORDER BY state_code, fiscal_year"

        rows = cur.execute(sql, params).fetchall()
        cols = ["state", "fiscal_year", "payment_type", "total_computable"]
        return {
            "rows": [dict(zip(cols, r)) for r in rows],
            "count": len(rows),
        }
