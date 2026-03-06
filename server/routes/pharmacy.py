"""Pharmacy data routes — SDUD, NADAC, drug rebate."""

from fastapi import APIRouter, Query
from server.db import get_cursor

router = APIRouter()


@router.get("/api/pharmacy/utilization/{state_code}")
async def drug_utilization(
    state_code: str,
    year: int = Query(None),
    limit: int = Query(100, le=1000),
):
    """Get State Drug Utilization Data for a state."""
    state_code = state_code.upper()
    year_filter = "AND year = $2" if year else ""
    params = [state_code] + ([year] if year else [])

    with get_cursor() as cur:
        rows = cur.execute(f"""
            SELECT ndc, product_name, year, quarter,
                   units_reimbursed, prescription_count,
                   total_reimbursed, medicaid_reimbursed
            FROM fact_drug_utilization
            WHERE state_code = $1 {year_filter}
            ORDER BY medicaid_reimbursed DESC
            LIMIT {limit}
        """, params).fetchall()
        columns = ["ndc", "product_name", "year", "quarter",
                    "units_reimbursed", "prescription_count",
                    "total_reimbursed", "medicaid_reimbursed"]
        return [dict(zip(columns, r)) for r in rows]


@router.get("/api/pharmacy/nadac")
async def nadac_pricing(
    ndc: str = Query(None, description="Filter by NDC code"),
    search: str = Query(None, description="Search drug name"),
    limit: int = Query(100, le=1000),
):
    """Get NADAC pharmacy pricing data."""
    filters = []
    params = []
    idx = 1

    if ndc:
        filters.append(f"ndc = ${idx}")
        params.append(ndc)
        idx += 1
    if search:
        filters.append(f"ndc_description ILIKE ${idx}")
        params.append(f"%{search}%")
        idx += 1

    where = "WHERE " + " AND ".join(filters) if filters else ""

    with get_cursor() as cur:
        rows = cur.execute(f"""
            SELECT ndc, ndc_description, nadac_per_unit,
                   effective_date, pricing_unit, is_otc
            FROM fact_nadac
            {where}
            ORDER BY effective_date DESC
            LIMIT {limit}
        """, params).fetchall()
        columns = ["ndc", "ndc_description", "nadac_per_unit",
                    "effective_date", "pricing_unit", "is_otc"]
        return [dict(zip(columns, r)) for r in rows]


@router.get("/api/pharmacy/top-drugs/{state_code}")
async def top_drugs(state_code: str, limit: int = Query(20, le=100)):
    """Get top drugs by Medicaid spending for a state."""
    state_code = state_code.upper()
    with get_cursor() as cur:
        rows = cur.execute("""
            SELECT
                du.ndc,
                du.product_name,
                SUM(du.medicaid_reimbursed) AS total_spending,
                SUM(du.prescription_count) AS total_rx,
                AVG(n.nadac_per_unit) AS avg_nadac
            FROM fact_drug_utilization du
            LEFT JOIN fact_nadac n ON du.ndc = n.ndc
            WHERE du.state_code = $1
            GROUP BY du.ndc, du.product_name
            ORDER BY total_spending DESC
            LIMIT $2
        """, [state_code, limit]).fetchall()
        columns = ["ndc", "product_name", "total_spending",
                    "total_rx", "avg_nadac"]
        return [dict(zip(columns, r)) for r in rows]
