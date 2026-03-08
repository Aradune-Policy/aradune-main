from fastapi import APIRouter, HTTPException
from server.models import QueryMeta
from server.db import get_cursor
from server.presets import PRESETS

router = APIRouter()

# ── Table descriptions for the data catalog ────────────────────────────
TABLE_DESCRIPTIONS = {
    "fact_medicaid_rate": "Medicaid fee schedule rates by state, code, and modifier (47 states)",
    "fact_rate_comparison": "Medicaid vs Medicare rate comparison with pct_of_medicare (45 states)",
    "fact_enrollment": "Monthly Medicaid enrollment by state (total, CHIP, FFS, managed care)",
    "fact_claims": "T-MSIS claims aggregated by state, procedure code, and month",
    "fact_hospital_cost": "HCRIS hospital cost reports — financials, beds, payer mix, margins",
    "fact_bls_wage": "BLS OEWS healthcare occupation wages by state (16 occupations)",
    "fact_quality_measure": "Medicaid Adult/Child Core Set quality measures",
    "fact_expenditure": "CMS-64 Medicaid expenditure by state, category, and quarter",
    "fact_hpsa": "HRSA Health Professional Shortage Area designations (3 disciplines)",
    "fact_nsduh_prevalence": "SAMHSA behavioral health prevalence estimates by state",
    "fact_scorecard": "CMS Medicaid Scorecard measures with state and national benchmarks",
    "fact_unwinding": "PHE unwinding redetermination outcomes by state",
    "fact_fmap": "Federal Medical Assistance Percentages (FMAP/eFMAP) by state",
    "fact_drug_utilization": "State Drug Utilization Data (SDUD) — Medicaid prescriptions",
    "fact_nadac": "National Average Drug Acquisition Cost pharmacy pricing",
    "fact_dsh_payment": "Disproportionate Share Hospital payments by state",
    "fact_dsh_hospital": "Hospital-level DSH data (6,103 hospitals)",
    "fact_managed_care": "Managed care plan enrollment by state and plan type",
    "fact_five_star": "CMS Five-Star nursing facility quality ratings",
    "fact_hospital_rating": "Overall hospital quality star ratings from CMS",
    "fact_hospital_vbp": "Hospital Value-Based Purchasing program scores",
    "fact_hospital_hrrp": "Hospital Readmissions Reduction Program data",
    "fact_acs_state": "Census ACS demographics — population, poverty, income, insurance",
    "fact_unemployment": "Monthly state unemployment rates from BLS LAUS",
    "fact_opioid_prescribing": "Medicare Part D opioid prescribing rates by state",
    "fact_maternal_health": "Hospital-level maternal health quality measures",
    "fact_telehealth_services": "Telehealth utilization by state and service type",
    "fact_dental_services": "Dental services to Medicaid children under 19",
    "fact_chip_enrollment": "CHIP enrollment counts by state and month",
    "fact_block_grant": "SAMHSA Mental Health Block Grant allotments by state",
    "fact_mh_facility": "SAMHSA treatment facility directory with bed counts",
    "fact_epsdt": "Early and Periodic Screening, Diagnostic, and Treatment (CMS-416)",
    "fact_hospice_quality": "Hospice facility-level quality measures (4,948 hospices)",
    "fact_medicare_enrollment": "Medicare enrollment by state including MA penetration",
    "fact_sdud_2024": "State Drug Utilization Data — 2024 quarterly data",
    "fact_cms372_waiver": "CMS-372 waiver program records with expenditure data",
    "fact_bh_by_condition": "Behavioral health conditions by state from T-MSIS",
    "fact_irf_provider": "Inpatient rehabilitation facility quality measures",
    "fact_ltch_provider": "Long-term care hospital quality measures",
    "fact_home_health_agency": "Home health agency directory with quality ratings",
    "dim_state": "State dimension — codes, names, FMAP, methodology, enrollment",
    "dim_procedure": "HCPCS/CPT procedure codes with RVUs and Medicare rates",
    "dim_medicare_locality": "Medicare GPCI values by locality",
}


@router.get("/api/meta", response_model=QueryMeta)
async def meta():
    try:
        with get_cursor() as cur:
            states = [r[0] for r in cur.execute(
                "SELECT DISTINCT state FROM spending WHERE state IS NOT NULL ORDER BY state"
            ).fetchall()]

            categories = [r[0] for r in cur.execute(
                "SELECT DISTINCT category FROM spending WHERE category IS NOT NULL ORDER BY category"
            ).fetchall()]

            date_range = cur.execute(
                "SELECT MIN(CLAIM_FROM_MONTH), MAX(CLAIM_FROM_MONTH) FROM spending"
            ).fetchone()

            columns = [r[0] for r in cur.execute(
                "SELECT column_name FROM information_schema.columns WHERE table_name = 'spending' ORDER BY ordinal_position"
            ).fetchall()]

            total_rows = cur.execute("SELECT COUNT(*) FROM spending").fetchone()[0]

        return QueryMeta(
            states=states,
            categories=categories,
            date_min=str(date_range[0]) if date_range and date_range[0] else None,
            date_max=str(date_range[1]) if date_range and date_range[1] else None,
            columns=columns,
            total_rows=total_rows,
            presets=list(PRESETS.keys()),
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Meta query error: {e}")


@router.get("/api/catalog")
async def catalog():
    """Return metadata about all available tables — name, row count, columns, description."""
    try:
        with get_cursor() as cur:
            # Get all views (our registered tables)
            views = cur.execute(
                "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main' AND table_type = 'VIEW' ORDER BY table_name"
            ).fetchall()

            tables = []
            for (view_name,) in views:
                # Skip compat views
                if view_name in ("spending", "spending_providers"):
                    continue
                try:
                    row_count = cur.execute(f"SELECT COUNT(*) FROM {view_name}").fetchone()[0]
                    cols = cur.execute(
                        f"SELECT column_name, column_type FROM (DESCRIBE SELECT * FROM {view_name})"
                    ).fetchall()
                    tables.append({
                        "name": view_name,
                        "rows": row_count,
                        "columns": [{"name": c[0], "type": c[1]} for c in cols],
                        "description": TABLE_DESCRIPTIONS.get(view_name, ""),
                        "category": "dimension" if view_name.startswith("dim_") else "reference" if view_name.startswith("ref_") else "fact",
                    })
                except Exception:
                    continue

            total_rows = sum(t["rows"] for t in tables)
            return {
                "tables": tables,
                "total_tables": len(tables),
                "total_rows": total_rows,
            }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Catalog error: {e}")
