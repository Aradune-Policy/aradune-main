"""Fiscal Cliff Analysis — spending vs revenue, FMAP impact, budget pressure, and composite vulnerability."""

from fastapi import APIRouter, HTTPException, Query
from server.db import get_cursor

router = APIRouter()


@router.get("/api/research/fiscal-cliff/spending-vs-revenue")
async def spending_vs_revenue(state: str = Query(None), fiscal_year: int = Query(None)):
    """Medicaid state share as percentage of state tax revenue, by state and fiscal year."""
    try:
        with get_cursor() as cur:
            params = []
            conditions = []
            if state:
                params.append(state.upper())
                conditions.append(f"s.state_code = ${len(params)}")
            if fiscal_year:
                params.append(fiscal_year)
                conditions.append(f"s.fiscal_year = ${len(params)}")
            extra_where = f"AND {' AND '.join(conditions)}" if conditions else ""

            rows = cur.execute(f"""
                WITH spending AS (
                    SELECT state_code, fiscal_year,
                           SUM(total_computable) AS total_spending,
                           SUM(federal_share) AS federal_share,
                           SUM(total_computable) - SUM(federal_share) AS state_share
                    FROM fact_cms64_multiyear
                    WHERE state_code != 'US'
                    GROUP BY state_code, fiscal_year
                ),
                revenue AS (
                    SELECT state_code, fiscal_year, amount_thousands * 1000.0 AS total_tax_revenue
                    FROM fact_census_state_finances
                    WHERE category = 'Total Taxes'
                )
                SELECT s.state_code, s.fiscal_year,
                       ROUND(s.total_spending, 0) AS total_spending,
                       ROUND(s.federal_share, 0) AS federal_share,
                       ROUND(s.state_share, 0) AS state_share,
                       COALESCE(ROUND(r.total_tax_revenue, 0), 0) AS total_tax_collections,
                       CASE WHEN COALESCE(r.total_tax_revenue, 0) > 0
                            THEN ROUND(s.state_share * 100.0 / r.total_tax_revenue, 1)
                            ELSE NULL END AS medicaid_pct_of_revenue
                FROM spending s
                LEFT JOIN revenue r ON s.state_code = r.state_code AND s.fiscal_year = r.fiscal_year
                WHERE 1=1 {extra_where}
                ORDER BY s.fiscal_year DESC, medicaid_pct_of_revenue DESC
            """, params).fetchall()
            columns = [
                "state_code", "fiscal_year", "total_spending", "federal_share",
                "state_share", "total_tax_collections", "medicaid_pct_of_revenue",
            ]
            return {"rows": [dict(zip(columns, r)) for r in rows], "count": len(rows)}
    except Exception as e:
        raise HTTPException(status_code=500, detail={"error": "Spending vs revenue query failed", "detail": str(e)})


@router.get("/api/research/fiscal-cliff/fmap-impact")
async def fmap_impact(state: str = Query(None)):
    """FMAP rates over time including enhanced FMAP where available."""
    try:
        with get_cursor() as cur:
            params = []
            state_filter = ""
            if state:
                state_filter = "WHERE state_code = $1"
                params.append(state.upper())
            rows = cur.execute(f"""
                WITH fmap_pivoted AS (
                    SELECT state_code, fiscal_year,
                           MAX(CASE WHEN rate_type = 'fmap' THEN rate END) AS fmap_rate,
                           MAX(CASE WHEN rate_type = 'efmap' THEN rate END) AS efmap_rate
                    FROM fact_fmap_historical
                    GROUP BY state_code, fiscal_year
                )
                SELECT state_code, fiscal_year,
                       ROUND(fmap_rate, 4) AS fmap_rate,
                       ROUND(COALESCE(efmap_rate, fmap_rate), 4) AS enhanced_fmap
                FROM fmap_pivoted
                {state_filter}
                ORDER BY fiscal_year, state_code
            """, params).fetchall()
            columns = ["state_code", "fiscal_year", "fmap_rate", "enhanced_fmap"]
            return {"rows": [dict(zip(columns, r)) for r in rows], "count": len(rows)}
    except Exception as e:
        raise HTTPException(status_code=500, detail={"error": "FMAP impact query failed", "detail": str(e)})


@router.get("/api/research/fiscal-cliff/budget-pressure")
async def budget_pressure(state: str = Query(None)):
    """Latest-year budget pressure: Medicaid state share vs tax revenue and GDP."""
    try:
        with get_cursor() as cur:
            params = []
            state_filter = ""
            if state:
                state_filter = "AND d.state_code = $1"
                params.append(state.upper())
            rows = cur.execute(f"""
                WITH latest_spending AS (
                    SELECT state_code,
                           SUM(total_computable) - SUM(federal_share) AS state_share
                    FROM fact_cms64_multiyear
                    WHERE fiscal_year = (SELECT MAX(fiscal_year) FROM fact_cms64_multiyear)
                      AND state_code != 'US'
                    GROUP BY state_code
                ),
                revenue AS (
                    SELECT state_code, amount_thousands * 1000.0 AS total_tax_revenue
                    FROM fact_census_state_finances
                    WHERE category = 'Total Taxes'
                      AND fiscal_year = (SELECT MAX(fiscal_year) FROM fact_census_state_finances WHERE category = 'Total Taxes')
                ),
                gdp AS (
                    SELECT state_code, real_gdp_millions
                    FROM fact_state_gdp
                    WHERE year = (SELECT MAX(year) FROM fact_state_gdp)
                ),
                fmap AS (
                    SELECT state_code, MAX(CASE WHEN rate_type = 'fmap' THEN rate END) AS fmap_rate
                    FROM fact_fmap_historical
                    WHERE fiscal_year = (SELECT MAX(fiscal_year) FROM fact_fmap_historical)
                    GROUP BY state_code
                )
                SELECT d.state_code,
                       ROUND(COALESCE(s.state_share, 0), 0) AS medicaid_state_share,
                       COALESCE(ROUND(r.total_tax_revenue, 0), 0) AS tax_revenue,
                       CASE WHEN COALESCE(r.total_tax_revenue, 0) > 0
                            THEN ROUND(s.state_share * 100.0 / r.total_tax_revenue, 1)
                            ELSE NULL END AS medicaid_pct_of_revenue,
                       COALESCE(g.real_gdp_millions, 0) AS state_gdp_millions,
                       ROUND(COALESCE(f.fmap_rate, 0.5), 4) AS fmap_rate
                FROM dim_state d
                LEFT JOIN latest_spending s ON d.state_code = s.state_code
                LEFT JOIN revenue r ON d.state_code = r.state_code
                LEFT JOIN gdp g ON d.state_code = g.state_code
                LEFT JOIN fmap f ON d.state_code = f.state_code
                WHERE s.state_share IS NOT NULL {state_filter}
                ORDER BY medicaid_pct_of_revenue DESC
            """, params).fetchall()
            columns = [
                "state_code", "medicaid_state_share", "tax_revenue",
                "medicaid_pct_of_revenue", "state_gdp_millions", "fmap_rate",
            ]
            return {"rows": [dict(zip(columns, r)) for r in rows], "count": len(rows)}
    except Exception as e:
        raise HTTPException(status_code=500, detail={"error": "Budget pressure query failed", "detail": str(e)})


@router.get("/api/research/fiscal-cliff/vulnerability")
async def fiscal_vulnerability(state: str = Query(None)):
    """Composite fiscal vulnerability: budget share, spending growth, state burden."""
    try:
        with get_cursor() as cur:
            params = []
            state_filter = ""
            if state:
                state_filter = "AND d.state_code = $1"
                params.append(state.upper())
            rows = cur.execute(f"""
                WITH spending_growth AS (
                    SELECT state_code,
                           MAX(CASE WHEN fiscal_year = (SELECT MAX(fiscal_year) FROM fact_cms64_multiyear)
                               THEN total_computable END) AS latest_spending,
                           MAX(CASE WHEN fiscal_year = (SELECT MAX(fiscal_year) FROM fact_cms64_multiyear) - 2
                               THEN total_computable END) AS prior_spending
                    FROM fact_cms64_multiyear
                    WHERE state_code != 'US'
                    GROUP BY state_code
                ),
                budget AS (
                    SELECT s.state_code,
                           SUM(s.total_computable) - SUM(s.federal_share) AS state_share,
                           MAX(r.amount_thousands * 1000.0) AS total_tax_revenue
                    FROM fact_cms64_multiyear s
                    LEFT JOIN fact_census_state_finances r
                        ON s.state_code = r.state_code
                        AND r.category = 'Total Taxes'
                        AND r.fiscal_year = (SELECT MAX(fiscal_year) FROM fact_census_state_finances WHERE category = 'Total Taxes')
                    WHERE s.fiscal_year = (SELECT MAX(fiscal_year) FROM fact_cms64_multiyear)
                      AND s.state_code != 'US'
                    GROUP BY s.state_code
                ),
                fmap AS (
                    SELECT state_code, MAX(CASE WHEN rate_type = 'fmap' THEN rate END) AS fmap_rate
                    FROM fact_fmap_historical
                    WHERE fiscal_year = (SELECT MAX(fiscal_year) FROM fact_fmap_historical)
                    GROUP BY state_code
                )
                SELECT d.state_code,
                       CASE WHEN COALESCE(b.total_tax_revenue, 0) > 0
                            THEN ROUND(b.state_share * 100.0 / b.total_tax_revenue, 1)
                            ELSE NULL END AS budget_share_pct,
                       CASE WHEN COALESCE(sg.prior_spending, 0) > 0
                            THEN ROUND((sg.latest_spending - sg.prior_spending) * 100.0 / sg.prior_spending, 1)
                            ELSE NULL END AS spending_growth_pct,
                       ROUND((1 - COALESCE(f.fmap_rate, 0.5)) * 100, 1) AS state_burden_pct
                FROM dim_state d
                LEFT JOIN budget b ON d.state_code = b.state_code
                LEFT JOIN spending_growth sg ON d.state_code = sg.state_code
                LEFT JOIN fmap f ON d.state_code = f.state_code
                WHERE b.state_share IS NOT NULL {state_filter}
                ORDER BY budget_share_pct DESC
            """, params).fetchall()
            columns = ["state_code", "budget_share_pct", "spending_growth_pct", "state_burden_pct"]
            return {"rows": [dict(zip(columns, r)) for r in rows], "count": len(rows)}
    except Exception as e:
        raise HTTPException(status_code=500, detail={"error": "Fiscal vulnerability query failed", "detail": str(e)})
