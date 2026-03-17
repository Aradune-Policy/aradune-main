"""Pharmacy data routes — SDUD, NADAC, drug rebate, Orange Book."""

from fastapi import APIRouter, HTTPException, Query
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
    year_filter = "AND year = $2" if year is not None else ""
    params = [state_code] + ([year] if year is not None else [])
    limit_idx = len(params) + 1
    params.append(limit)

    try:
        with get_cursor() as cur:
            rows = cur.execute(f"""
                SELECT ndc, product_name, year, quarter,
                       units_reimbursed, prescription_count,
                       total_reimbursed, medicaid_reimbursed
                FROM fact_drug_utilization
                WHERE state_code = $1 {year_filter}
                ORDER BY medicaid_reimbursed DESC
                LIMIT ${limit_idx}
            """, params).fetchall()
            columns = ["ndc", "product_name", "year", "quarter",
                        "units_reimbursed", "prescription_count",
                        "total_reimbursed", "medicaid_reimbursed"]
            return [dict(zip(columns, r)) for r in rows]
    except Exception as e:
        raise HTTPException(500, {"error": "Drug utilization query failed", "detail": str(e)})


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

    try:
        with get_cursor() as cur:
            rows = cur.execute(f"""
                SELECT ndc, ndc_description, nadac_per_unit,
                       effective_date, pricing_unit, is_otc
                FROM fact_nadac
                {where}
                ORDER BY effective_date DESC
                LIMIT ${idx}
            """, params + [limit]).fetchall()
            columns = ["ndc", "ndc_description", "nadac_per_unit",
                        "effective_date", "pricing_unit", "is_otc"]
            return [dict(zip(columns, r)) for r in rows]
    except Exception as e:
        raise HTTPException(500, {"error": "NADAC query failed", "detail": str(e)})


@router.get("/api/pharmacy/top-drugs/{state_code}")
async def top_drugs(state_code: str, limit: int = Query(20, le=100)):
    """Get top drugs by Medicaid spending for a state."""
    state_code = state_code.upper()
    try:
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
    except Exception as e:
        raise HTTPException(500, {"error": "Top drugs query failed", "detail": str(e)})


# -- Orange Book / Generic Opportunity ----------------------------------------

@router.get("/api/pharmacy/generic-opportunity")
async def generic_opportunity(
    limit: int = Query(50, le=200),
):
    """Find Medicaid drugs where a generic is available (Orange Book TE code AB*)
    but the brand is still being dispensed at high cost.

    Joins FDA Orange Book products (ANDA approvals) to SDUD 2025 on drug name
    to surface generic substitution savings opportunities.
    """
    try:
        with get_cursor() as cur:
            rows = cur.execute("""
                WITH brand_spending AS (
                    SELECT
                        product_name,
                        SUM(total_amount_reimbursed) AS total_spending,
                        SUM(number_of_prescriptions) AS total_rx,
                        COUNT(DISTINCT state_code) AS states_dispensing
                    FROM fact_sdud_2025
                    WHERE state_code != 'XX'
                      AND suppression_used = false
                      AND total_amount_reimbursed > 0
                    GROUP BY product_name
                    HAVING SUM(total_amount_reimbursed) > 100000
                ),
                generics_available AS (
                    SELECT DISTINCT
                        Ingredient,
                        Trade_Name
                    FROM fact_fda_orange_book_products
                    WHERE Appl_Type = 'A'
                      AND TE_Code LIKE 'AB%'
                )
                SELECT
                    bs.product_name,
                    bs.total_spending,
                    bs.total_rx,
                    bs.states_dispensing,
                    ga.Ingredient AS generic_ingredient,
                    ga.Trade_Name AS generic_trade_name
                FROM brand_spending bs
                INNER JOIN generics_available ga
                    ON bs.product_name ILIKE '%' || ga.Ingredient || '%'
                ORDER BY bs.total_spending DESC
                LIMIT $1
            """, [limit]).fetchall()
            columns = [
                "product_name", "total_spending", "total_rx",
                "states_dispensing", "generic_ingredient", "generic_trade_name",
            ]
            return {
                "rows": [dict(zip(columns, r)) for r in rows],
                "count": len(rows),
                "source": "FDA Orange Book (AB-rated generics) x CMS SDUD 2025",
            }
    except Exception as e:
        raise HTTPException(500, {"error": "Generic opportunity query failed", "detail": str(e)})


@router.get("/api/pharmacy/patents")
async def drug_patents(
    search: str = Query(default=None, description="Search by drug/ingredient name"),
    limit: int = Query(100, le=500),
):
    """Browse FDA Orange Book products with patent expiration dates.

    Joins fact_fda_orange_book_products to fact_fda_orange_book_patent
    for patent landscape visibility.
    """
    filters = []
    params = []
    idx = 1

    if search:
        filters.append(
            f"(p.Trade_Name ILIKE ${idx} OR p.Ingredient ILIKE ${idx})"
        )
        params.append(f"%{search}%")
        idx += 1

    where = "WHERE " + " AND ".join(filters) if filters else ""

    try:
        with get_cursor() as cur:
            rows = cur.execute(f"""
                SELECT
                    p.Trade_Name,
                    p.Ingredient,
                    p.Appl_Type,
                    p.Appl_No,
                    p.Strength,
                    p."DF;Route" AS dosage_form_route,
                    p.TE_Code,
                    p.Approval_Date,
                    p.Applicant_Full_Name,
                    pt.Patent_No,
                    pt.Patent_Expire_Date_Text AS patent_expiration,
                    pt.Drug_Substance_Flag,
                    pt.Drug_Product_Flag
                FROM fact_fda_orange_book_products p
                LEFT JOIN fact_fda_orange_book_patent pt
                    ON p.Appl_Type = pt.Appl_Type
                    AND p.Appl_No = pt.Appl_No
                    AND p.Product_No = pt.Product_No
                {where}
                ORDER BY p.Trade_Name, pt.Patent_Expire_Date_Text DESC
                LIMIT ${idx}
            """, params + [limit]).fetchall()
            columns = [
                "trade_name", "ingredient", "appl_type", "appl_no",
                "strength", "dosage_form_route", "te_code", "approval_date",
                "applicant", "patent_no", "patent_expiration",
                "drug_substance_flag", "drug_product_flag",
            ]
            return {
                "rows": [dict(zip(columns, r)) for r in rows],
                "count": len(rows),
                "source": "FDA Orange Book (products + patents)",
            }
    except Exception as e:
        raise HTTPException(500, {"error": "Drug patent query failed", "detail": str(e)})
