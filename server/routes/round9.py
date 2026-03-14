"""
Round 9 API endpoints — Medicare enrollment, opioid data, CMS impact,
CHIP/enrollment data, and more.
"""

from fastapi import APIRouter, Query
from typing import Optional
from server.db import get_cursor

router = APIRouter()


# ─── Medicare Enrollment (dual status, demographics) ───────────────────


@router.get("/api/medicare/enrollment")
async def medicare_enrollment(
    state: Optional[str] = Query(None),
    year: Optional[int] = Query(None),
    geo_level: Optional[str] = Query(None, description="National, State, or County"),
    limit: int = Query(1000, le=5000),
):
    """Medicare enrollment with dual status by state/county/month."""
    with get_cursor() as cur:
        sql = "SELECT * FROM fact_medicare_enrollment WHERE 1=1"
        params = []
        if state:
            sql += " AND state = ?"
            params.append(state)
        if year:
            sql += " AND year = ?"
            params.append(year)
        if geo_level:
            sql += " AND geo_level = ?"
            params.append(geo_level)
        sql += " ORDER BY year DESC, month DESC LIMIT ?"
        params.append(limit)
        cur.execute(sql, params)
        cols = [d[0] for d in cur.description]
        rows = cur.fetchall()
    return {"rows": [dict(zip(cols, r)) for r in rows], "count": len(rows)}


@router.get("/api/medicare/enrollment/duals")
async def medicare_duals_summary(
    year: Optional[int] = Query(None),
):
    """Dual-eligible summary by state — total, full, partial, QMB, SLMB."""
    with get_cursor() as cur:
        year_filter = ""
        params = []
        if year:
            year_filter = "AND year = ?"
            params.append(year)
        cur.execute(f"""
            SELECT state,
                   year,
                   SUM(total_benes) AS total_medicare,
                   SUM(dual_total) AS dual_total,
                   SUM(full_dual) AS full_dual,
                   SUM(partial_dual) AS partial_dual,
                   SUM(qmb_only) AS qmb_only,
                   SUM(qmb_plus) AS qmb_plus,
                   SUM(slmb_only) AS slmb_only,
                   ROUND(SUM(dual_total) * 100.0 / NULLIF(SUM(total_benes), 0), 1)
                       AS dual_pct
            FROM fact_medicare_enrollment
            WHERE geo_level = 'State' {year_filter}
            GROUP BY state, year
            ORDER BY year DESC, state
        """, params)
        cols = [d[0] for d in cur.description]
        rows = cur.fetchall()
    return {"rows": [dict(zip(cols, r)) for r in rows], "count": len(rows)}


# ─── CMS IPPS Impact File ──────────────────────────────────────────────


@router.get("/api/cms/ipps-impact")
async def cms_impact_file(
    state: Optional[str] = Query(None),
    ccn: Optional[str] = Query(None),
    limit: int = Query(1000, le=5000),
):
    """CMS IPPS Impact File — wage index, CMI, DSH%, VBP for hospitals."""
    with get_cursor() as cur:
        sql = "SELECT * FROM fact_cms_impact WHERE 1=1"
        params = []
        if ccn:
            sql += " AND ccn = ?"
            params.append(ccn)
        if state:
            # CCN first 2 digits encode state but not directly state code;
            # filter by geo_lma or payment_lma containing state info
            sql += " AND (geo_lma LIKE ? OR payment_lma LIKE ?)"
            params.extend([f"%{state}%", f"%{state}%"])
        sql += " LIMIT ?"
        params.append(limit)
        cur.execute(sql, params)
        cols = [d[0] for d in cur.description]
        rows = cur.fetchall()
    return {"rows": [dict(zip(cols, r)) for r in rows], "count": len(rows)}


# ─── Opioid Prescribing ────────────────────────────────────────────────


@router.get("/api/opioid/prescribing")
async def opioid_prescribing(
    state: Optional[str] = Query(None),
    year: Optional[int] = Query(None),
    plan_type: Optional[str] = Query(None),
    limit: int = Query(1000, le=5000),
):
    """Medicaid opioid prescribing rates by state/year/plan type."""
    with get_cursor() as cur:
        sql = "SELECT * FROM fact_opioid_prescribing WHERE geo_level = 'State'"
        params = []
        if state:
            sql += " AND geo_code = ?"
            params.append(state)
        if year:
            sql += " AND year = ?"
            params.append(year)
        if plan_type:
            sql += " AND plan_type = ?"
            params.append(plan_type)
        sql += " ORDER BY year DESC LIMIT ?"
        params.append(limit)
        cur.execute(sql, params)
        cols = [d[0] for d in cur.description]
        rows = cur.fetchall()
    return {"rows": [dict(zip(cols, r)) for r in rows], "count": len(rows)}


@router.get("/api/opioid/prescribing/summary")
async def opioid_prescribing_summary(
    year: Optional[int] = Query(None),
):
    """National opioid prescribing summary — rate by state, highest/lowest."""
    with get_cursor() as cur:
        year_filter = ""
        params = []
        if year:
            year_filter = "AND year = ?"
            params.append(year)
        cur.execute(f"""
            SELECT COALESCE(d.state_code, o.geo_code) AS state,
                   o.year,
                   o.plan_type,
                   o.opioid_prescribing_rate,
                   o.opioid_claims,
                   o.total_claims
            FROM fact_opioid_prescribing o
            LEFT JOIN dim_state d ON d.state_name = o.geo_desc
            WHERE o.geo_level = 'State' AND o.plan_type = 'All' {year_filter}
            ORDER BY o.year DESC, o.opioid_prescribing_rate DESC
        """, params)
        cols = [d[0] for d in cur.description]
        rows = cur.fetchall()
    return {"rows": [dict(zip(cols, r)) for r in rows], "count": len(rows)}


# ─── OTP Provider Directory ────────────────────────────────────────────


@router.get("/api/opioid/otp-providers")
async def otp_providers(
    state: Optional[str] = Query(None),
    limit: int = Query(500, le=2000),
):
    """Opioid Treatment Program provider directory."""
    with get_cursor() as cur:
        sql = "SELECT * FROM fact_otp_provider WHERE 1=1"
        params = []
        if state:
            sql += " AND STATE = ?"
            params.append(state)
        sql += " LIMIT ?"
        params.append(limit)
        cur.execute(sql, params)
        cols = [d[0] for d in cur.description]
        rows = cur.fetchall()
    return {"rows": [dict(zip(cols, r)) for r in rows], "count": len(rows)}


# ─── CMS-64 FFCRA ──────────────────────────────────────────────────────


@router.get("/api/financial/ffcra")
async def cms64_ffcra(
    state: Optional[str] = Query(None),
):
    """CMS-64 FFCRA enhanced FMAP expenditures by state and quarter."""
    with get_cursor() as cur:
        sql = "SELECT * FROM fact_cms64_ffcra WHERE 1=1"
        params = []
        if state:
            sql += " AND State = ?"
            params.append(state)
        sql += " ORDER BY State"
        cur.execute(sql, params)
        cols = [d[0] for d in cur.description]
        rows = cur.fetchall()
    return {"rows": [dict(zip(cols, r)) for r in rows], "count": len(rows)}


# ─── Contraceptive Care ────────────────────────────────────────────────


@router.get("/api/maternal/contraceptive-care")
async def contraceptive_care(
    state: Optional[str] = Query(None),
    year: Optional[int] = Query(None),
):
    """Contraceptive care utilization among Medicaid beneficiaries."""
    with get_cursor() as cur:
        sql = "SELECT * FROM fact_contraceptive_care WHERE 1=1"
        params = []
        if state:
            sql += " AND State = ?"
            params.append(state)
        if year:
            sql += " AND Year = ?"
            params.append(str(year))
        sql += " ORDER BY State, Year, Month"
        cur.execute(sql, params)
        cols = [d[0] for d in cur.description]
        rows = cur.fetchall()
    return {"rows": [dict(zip(cols, r)) for r in rows], "count": len(rows)}


# ─── Respiratory Conditions ────────────────────────────────────────────


@router.get("/api/health/respiratory")
async def respiratory_conditions(
    state: Optional[str] = Query(None),
    condition: Optional[str] = Query(None),
):
    """Respiratory conditions among Medicaid beneficiaries."""
    with get_cursor() as cur:
        sql = "SELECT * FROM fact_respiratory_conditions WHERE 1=1"
        params = []
        if state:
            sql += " AND State = ?"
            params.append(state)
        if condition:
            sql += " AND Condition LIKE ?"
            params.append(f"%{condition}%")
        sql += " ORDER BY State, Year, Month"
        cur.execute(sql, params)
        cols = [d[0] for d in cur.description]
        rows = cur.fetchall()
    return {"rows": [dict(zip(cols, r)) for r in rows], "count": len(rows)}


# ─── ESRD QIP Total Performance Scores ─────────────────────────────────


@router.get("/api/dialysis/esrd-qip-tps")
async def esrd_qip_tps(
    state: Optional[str] = Query(None),
    ccn: Optional[str] = Query(None),
    limit: int = Query(1000, le=5000),
):
    """ESRD QIP Total Performance Scores with payment reduction %."""
    with get_cursor() as cur:
        sql = "SELECT * FROM fact_esrd_qip_tps WHERE 1=1"
        params = []
        if state:
            sql += " AND State = ?"
            params.append(state)
        if ccn:
            sql += ' AND "CMS Certification Number (CCN)" = ?'
            params.append(ccn)
        sql += " LIMIT ?"
        params.append(limit)
        cur.execute(sql, params)
        cols = [d[0] for d in cur.description]
        rows = cur.fetchall()
    return {"rows": [dict(zip(cols, r)) for r in rows], "count": len(rows)}


# ─── Enrollment: Program, MC, CHIP, New Adult ──────────────────────────


@router.get("/api/enrollment/program-monthly")
async def program_monthly(
    state: Optional[str] = Query(None),
    program_type: Optional[str] = Query(None),
):
    """Medicaid/CHIP enrollment by program type per month."""
    with get_cursor() as cur:
        sql = "SELECT * FROM fact_program_monthly WHERE 1=1"
        params = []
        if state:
            sql += " AND State = ?"
            params.append(state)
        if program_type:
            sql += " AND ProgramType = ?"
            params.append(program_type)
        sql += " ORDER BY State, Month"
        cur.execute(sql, params)
        cols = [d[0] for d in cur.description]
        rows = cur.fetchall()
    return {"rows": [dict(zip(cols, r)) for r in rows], "count": len(rows)}


@router.get("/api/enrollment/mc-annual")
async def mc_annual(
    state: Optional[str] = Query(None),
):
    """Managed care participation — annual summary."""
    with get_cursor() as cur:
        sql = "SELECT * FROM fact_mc_annual WHERE 1=1"
        params = []
        if state:
            sql += " AND State = ?"
            params.append(state)
        sql += " ORDER BY State, Year"
        cur.execute(sql, params)
        cols = [d[0] for d in cur.description]
        rows = cur.fetchall()
    return {"rows": [dict(zip(cols, r)) for r in rows], "count": len(rows)}


@router.get("/api/enrollment/mc-info-monthly")
async def mc_info_monthly(
    state: Optional[str] = Query(None),
):
    """Managed care participation — monthly enrollment."""
    with get_cursor() as cur:
        sql = "SELECT * FROM fact_mc_info_monthly WHERE 1=1"
        params = []
        if state:
            sql += " AND State = ?"
            params.append(state)
        sql += " ORDER BY State, Month LIMIT 5000"
        cur.execute(sql, params)
        cols = [d[0] for d in cur.description]
        rows = cur.fetchall()
    return {"rows": [dict(zip(cols, r)) for r in rows], "count": len(rows)}


@router.get("/api/enrollment/chip-monthly")
async def chip_monthly(
    state: Optional[str] = Query(None),
):
    """CHIP enrollment by month."""
    with get_cursor() as cur:
        sql = "SELECT * FROM fact_chip_monthly WHERE 1=1"
        params = []
        if state:
            sql += ' AND "State Abbreviation" = ?'
            params.append(state)
        sql += " ORDER BY \"State Abbreviation\", \"Coverage Month\""
        cur.execute(sql, params)
        cols = [d[0] for d in cur.description]
        rows = cur.fetchall()
    return {"rows": [dict(zip(cols, r)) for r in rows], "count": len(rows)}


@router.get("/api/enrollment/chip-app-elig")
async def chip_app_elig(
    state: Optional[str] = Query(None),
):
    """CHIP applications, eligibility determinations, and enrollment."""
    with get_cursor() as cur:
        sql = "SELECT * FROM fact_chip_app_elig WHERE 1=1"
        params = []
        if state:
            sql += ' AND "State Abbreviation" = ?'
            params.append(state)
        sql += ' ORDER BY "State Abbreviation", "Report Date"'
        cur.execute(sql, params)
        cols = [d[0] for d in cur.description]
        rows = cur.fetchall()
    return {"rows": [dict(zip(cols, r)) for r in rows], "count": len(rows)}


@router.get("/api/enrollment/performance-indicator")
async def performance_indicator(
    state: Optional[str] = Query(None),
):
    """Medicaid/CHIP Performance Indicator (applications/determinations)."""
    with get_cursor() as cur:
        sql = "SELECT * FROM fact_performance_indicator WHERE 1=1"
        params = []
        if state:
            sql += " AND State = ?"
            params.append(state)
        sql += " ORDER BY State LIMIT 2000"
        cur.execute(sql, params)
        cols = [d[0] for d in cur.description]
        rows = cur.fetchall()
    return {"rows": [dict(zip(cols, r)) for r in rows], "count": len(rows)}


@router.get("/api/enrollment/new-adult")
async def new_adult_enrollment(
    state: Optional[str] = Query(None),
):
    """Medicaid new adult (expansion) enrollment by state/month."""
    with get_cursor() as cur:
        sql = "SELECT * FROM fact_new_adult_enrollment WHERE 1=1"
        params = []
        if state:
            sql += " AND State = ?"
            params.append(state)
        sql += " ORDER BY State LIMIT 2000"
        cur.execute(sql, params)
        cols = [d[0] for d in cur.description]
        rows = cur.fetchall()
    return {"rows": [dict(zip(cols, r)) for r in rows], "count": len(rows)}


# ─── Pharmacy: Drug Rebate + SDUD 2024 ─────────────────────────────────


@router.get("/api/pharmacy/drug-rebate")
async def drug_rebate_products(
    ndc: Optional[str] = Query(None),
    drug_name: Optional[str] = Query(None),
    limit: int = Query(100, le=1000),
):
    """Medicaid Drug Rebate Program product listing."""
    with get_cursor() as cur:
        sql = "SELECT * FROM fact_drug_rebate_products WHERE 1=1"
        params = []
        if ndc:
            sql += " AND NDC = ?"
            params.append(ndc)
        if drug_name:
            sql += ' AND "FDA Product Name" LIKE ?'
            params.append(f"%{drug_name.upper()}%")
        sql += " LIMIT ?"
        params.append(limit)
        cur.execute(sql, params)
        cols = [d[0] for d in cur.description]
        rows = cur.fetchall()
    return {"rows": [dict(zip(cols, r)) for r in rows], "count": len(rows)}


@router.get("/api/pharmacy/sdud-2024")
async def sdud_2024(
    state: Optional[str] = Query(None),
    ndc: Optional[str] = Query(None),
    year: Optional[int] = Query(None),
    quarter: Optional[int] = Query(None),
    limit: int = Query(100, le=1000),
):
    """State Drug Utilization Data — 2024 dataset."""
    with get_cursor() as cur:
        sql = "SELECT * FROM fact_sdud_2024 WHERE 1=1"
        params = []
        if state:
            sql += " AND state = ?"
            params.append(state)
        if ndc:
            sql += " AND ndc = ?"
            params.append(ndc)
        if year:
            sql += " AND year = ?"
            params.append(year)
        if quarter:
            sql += " AND quarter = ?"
            params.append(quarter)
        sql += " ORDER BY total_reimbursed DESC LIMIT ?"
        params.append(limit)
        cur.execute(sql, params)
        cols = [d[0] for d in cur.description]
        rows = cur.fetchall()
    return {"rows": [dict(zip(cols, r)) for r in rows], "count": len(rows)}


@router.get("/api/pharmacy/sdud-2024/top-drugs")
async def sdud_2024_top_drugs(
    state: Optional[str] = Query(None),
    year: Optional[int] = Query(None),
    limit: int = Query(25, le=100),
):
    """Top drugs by Medicaid spending from SDUD 2024."""
    with get_cursor() as cur:
        where = "WHERE 1=1"
        params = []
        if state:
            where += " AND state = ?"
            params.append(state)
        if year:
            where += " AND year = ?"
            params.append(year)
        cur.execute(f"""
            SELECT product_name,
                   ndc,
                   SUM(medicaid_reimbursed) AS total_medicaid_spend,
                   SUM(num_prescriptions) AS total_prescriptions,
                   SUM(units_reimbursed) AS total_units,
                   COUNT(DISTINCT state) AS state_count
            FROM fact_sdud_2024
            {where}
            GROUP BY product_name, ndc
            ORDER BY total_medicaid_spend DESC
            LIMIT ?
        """, params + [limit])
        cols = [d[0] for d in cur.description]
        rows = cur.fetchall()
    return {"rows": [dict(zip(cols, r)) for r in rows], "count": len(rows)}


# ─── SDUD 2025 ─────────────────────────────────────────────────────────


@router.get("/api/pharmacy/sdud-2025")
async def sdud_2025(
    state: Optional[str] = Query(None),
    ndc: Optional[str] = Query(None),
    quarter: Optional[int] = Query(None),
    limit: int = Query(100, le=1000),
):
    """State Drug Utilization Data — 2025 Q1-Q2 (2.64M rows)."""
    with get_cursor() as cur:
        sql = "SELECT * FROM fact_sdud_2025 WHERE 1=1"
        params = []
        if state:
            sql += " AND state_code = ?"
            params.append(state.upper())
        if ndc:
            sql += " AND ndc = ?"
            params.append(ndc)
        if quarter:
            sql += " AND quarter = ?"
            params.append(quarter)
        sql += " ORDER BY total_amount_reimbursed DESC LIMIT ?"
        params.append(limit)
        cur.execute(sql, params)
        cols = [d[0] for d in cur.description]
        rows = cur.fetchall()
    return {"rows": [dict(zip(cols, r)) for r in rows], "count": len(rows)}


@router.get("/api/pharmacy/sdud-2025/top-drugs")
async def sdud_2025_top_drugs(
    state: Optional[str] = Query(None),
    quarter: Optional[int] = Query(None),
    limit: int = Query(25, le=100),
):
    """Top drugs by Medicaid spending from SDUD 2025."""
    with get_cursor() as cur:
        where = "WHERE 1=1"
        params = []
        if state:
            where += " AND state_code = ?"
            params.append(state.upper())
        if quarter:
            where += " AND quarter = ?"
            params.append(quarter)
        cur.execute(f"""
            SELECT product_name,
                   ndc,
                   SUM(total_amount_reimbursed) AS total_spend,
                   SUM(medicaid_amount_reimbursed) AS total_medicaid_spend,
                   SUM(number_of_prescriptions) AS total_prescriptions,
                   SUM(units_reimbursed) AS total_units,
                   COUNT(DISTINCT state_code) AS state_count
            FROM fact_sdud_2025
            {where}
            GROUP BY product_name, ndc
            ORDER BY total_spend DESC
            LIMIT ?
        """, params + [limit])
        cols = [d[0] for d in cur.description]
        rows = cur.fetchall()
    return {"rows": [dict(zip(cols, r)) for r in rows], "count": len(rows)}


@router.get("/api/pharmacy/sdud-2025/state-summary")
async def sdud_2025_state_summary():
    """SDUD 2025 summary — total spend, Rx count, drug count by state."""
    with get_cursor() as cur:
        cur.execute("""
            SELECT state_code,
                   COUNT(DISTINCT ndc) AS drug_count,
                   SUM(number_of_prescriptions) AS total_prescriptions,
                   ROUND(SUM(total_amount_reimbursed), 2) AS total_reimbursed,
                   ROUND(SUM(medicaid_amount_reimbursed), 2) AS medicaid_reimbursed
            FROM fact_sdud_2025
            WHERE state_code != 'XX'
            GROUP BY state_code
            ORDER BY total_reimbursed DESC
        """)
        cols = [d[0] for d in cur.description]
        rows = cur.fetchall()
    return {"rows": [dict(zip(cols, r)) for r in rows], "count": len(rows)}


# ─── Medicare Provider Enrollment ──────────────────────────────────────


@router.get("/api/providers/medicare-enrollment")
async def medicare_provider_enrollment(
    state: Optional[str] = Query(None),
    provider_type: Optional[str] = Query(None),
    npi: Optional[str] = Query(None),
    limit: int = Query(100, le=1000),
):
    """Medicare FFS provider enrollment directory (2.96M providers)."""
    with get_cursor() as cur:
        sql = "SELECT * FROM fact_medicare_provider_enrollment WHERE 1=1"
        params = []
        if state:
            sql += " AND state = ?"
            params.append(state)
        if provider_type:
            sql += " AND provider_type LIKE ?"
            params.append(f"%{provider_type}%")
        if npi:
            sql += " AND npi = ?"
            params.append(npi)
        sql += " LIMIT ?"
        params.append(limit)
        cur.execute(sql, params)
        cols = [d[0] for d in cur.description]
        rows = cur.fetchall()
    return {"rows": [dict(zip(cols, r)) for r in rows], "count": len(rows)}


@router.get("/api/providers/medicare-enrollment/by-type")
async def medicare_provider_by_type(
    state: Optional[str] = Query(None),
):
    """Medicare provider enrollment counts by type."""
    with get_cursor() as cur:
        where = "WHERE 1=1"
        params = []
        if state:
            where += " AND state = ?"
            params.append(state)
        cur.execute(f"""
            SELECT provider_type,
                   provider_type_code,
                   COUNT(*) AS provider_count,
                   COUNT(DISTINCT state) AS state_count
            FROM fact_medicare_provider_enrollment
            {where}
            GROUP BY provider_type, provider_type_code
            ORDER BY provider_count DESC
        """, params)
        cols = [d[0] for d in cur.description]
        rows = cur.fetchall()
    return {"rows": [dict(zip(cols, r)) for r in rows], "count": len(rows)}


# ─── Program Integrity: LEIE, Open Payments, MFCU, PERM ─────────────


@router.get("/api/integrity/leie-summary")
async def leie_summary(
    state: Optional[str] = Query(None),
):
    """LEIE exclusion summary by state — counts by entity type and exclusion type."""
    with get_cursor() as cur:
        where = "WHERE state_code IS NOT NULL"
        params = []
        if state:
            where += " AND state_code = ?"
            params.append(state)
        cur.execute(f"""
            SELECT state_code,
                   entity_type,
                   exclusion_type,
                   COUNT(*) AS exclusion_count,
                   COUNT(DISTINCT npi) FILTER (WHERE npi IS NOT NULL) AS npi_count
            FROM fact_leie
            {where}
            GROUP BY state_code, entity_type, exclusion_type
            ORDER BY exclusion_count DESC
        """, params)
        cols = [d[0] for d in cur.description]
        rows = cur.fetchall()

        # Also get state-level totals
        cur.execute(f"""
            SELECT state_code,
                   COUNT(*) AS total_exclusions,
                   SUM(CASE WHEN entity_type = 'individual' THEN 1 ELSE 0 END) AS individual_count,
                   SUM(CASE WHEN entity_type = 'entity' THEN 1 ELSE 0 END) AS entity_count,
                   COUNT(DISTINCT npi) FILTER (WHERE npi IS NOT NULL) AS unique_npis
            FROM fact_leie
            {where}
            GROUP BY state_code
            ORDER BY total_exclusions DESC
        """, params)
        state_cols = [d[0] for d in cur.description]
        state_rows = cur.fetchall()

    return {
        "detail": [dict(zip(cols, r)) for r in rows],
        "by_state": [dict(zip(state_cols, r)) for r in state_rows],
        "count": len(rows),
    }


@router.get("/api/integrity/open-payments-summary")
async def open_payments_summary(
    state: Optional[str] = Query(None),
):
    """Open Payments state-level summary — total amounts, physician counts."""
    with get_cursor() as cur:
        where = "WHERE state_code IS NOT NULL AND LENGTH(TRIM(state_code)) = 2"
        params = []
        if state:
            where += " AND state_code = ?"
            params.append(state)
        cur.execute(f"""
            SELECT state_code,
                   SUM(payment_count) AS total_payments,
                   ROUND(SUM(total_amount), 2) AS total_amount,
                   ROUND(AVG(avg_amount), 2) AS avg_payment,
                   SUM(unique_physicians) AS unique_physicians,
                   SUM(unique_companies) AS unique_companies
            FROM fact_open_payments
            {where}
            GROUP BY state_code
            ORDER BY total_amount DESC
        """, params)
        cols = [d[0] for d in cur.description]
        rows = cur.fetchall()

        # Top payment types nationally
        cur.execute(f"""
            SELECT payment_nature,
                   SUM(payment_count) AS total_payments,
                   ROUND(SUM(total_amount), 2) AS total_amount
            FROM fact_open_payments
            {where}
            GROUP BY payment_nature
            ORDER BY total_amount DESC
            LIMIT 10
        """, params)
        type_cols = [d[0] for d in cur.description]
        type_rows = cur.fetchall()

        # Category breakdown (General/Research/Ownership)
        try:
            cur.execute(f"""
                SELECT payment_category,
                       SUM(payment_count) AS total_payments,
                       ROUND(SUM(total_amount), 2) AS total_amount
                FROM fact_open_payments
                {where}
                GROUP BY payment_category
                ORDER BY total_amount DESC
            """, params)
            cat_cols = [d[0] for d in cur.description]
            cat_rows = cur.fetchall()
            by_category = [dict(zip(cat_cols, r)) for r in cat_rows]
        except Exception:
            by_category = []

    return {
        "by_state": [dict(zip(cols, r)) for r in rows],
        "by_payment_type": [dict(zip(type_cols, r)) for r in type_rows],
        "by_category": by_category,
        "count": len(rows),
    }


@router.get("/api/integrity/mfcu")
async def mfcu_stats(
    state: Optional[str] = Query(None),
):
    """MFCU statistics — investigations, convictions, recoveries by state (FY 2024)."""
    with get_cursor() as cur:
        sql = "SELECT * FROM fact_mfcu_stats WHERE 1=1"
        params = []
        if state:
            sql += " AND state_code = ?"
            params.append(state)
        sql += " ORDER BY total_recoveries DESC"
        cur.execute(sql, params)
        cols = [d[0] for d in cur.description]
        rows = cur.fetchall()
    return {"rows": [dict(zip(cols, r)) for r in rows], "count": len(rows)}


@router.get("/api/integrity/perm")
async def perm_rates(
    program: Optional[str] = Query(None),
):
    """PERM payment error rates — Medicaid and CHIP (2020-2025)."""
    with get_cursor() as cur:
        sql = "SELECT * FROM fact_perm_rates WHERE 1=1"
        params = []
        if program:
            sql += " AND program = ?"
            params.append(program)
        sql += " ORDER BY program, year DESC"
        cur.execute(sql, params)
        cols = [d[0] for d in cur.description]
        rows = cur.fetchall()
    return {"rows": [dict(zip(cols, r)) for r in rows], "count": len(rows)}
