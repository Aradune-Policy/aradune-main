"""Validation API endpoints -- surfaces data quality check results."""

from fastapi import APIRouter, Query
from server.engines.validator import run_core_checks
from server.utils.error_handler import safe_route

router = APIRouter(prefix="/api/validation", tags=["validation"])


@router.get("/latest")
@safe_route(default_response={"checks": [], "total": 0, "passed": 0, "failed": 0, "pass_rate": 0})
async def validation_latest():
    """Run validation checks and return latest results."""
    results = run_core_checks()
    passed = sum(1 for r in results if r["passed"])
    failed = len(results) - passed
    return {
        "checks": results,
        "total": len(results),
        "passed": passed,
        "failed": failed,
        "pass_rate": round(passed / len(results) * 100, 1) if results else 0,
    }


@router.get("/results")
@safe_route(default_response={"checks": [], "total": 0})
async def validation_results(
    domain: str = Query(None, description="Filter by table prefix (e.g., 'fact_rate')"),
    failures_only: bool = Query(False),
):
    """Return detailed check results with optional filtering."""
    results = run_core_checks()
    if domain:
        results = [r for r in results if r["table"].startswith(domain)]
    if failures_only:
        results = [r for r in results if not r["passed"]]
    return {"checks": results, "total": len(results)}


@router.get("/domains")
@safe_route(default_response={"domains": []})
async def validation_domains():
    """Pass rates grouped by data domain (table prefix)."""
    results = run_core_checks()
    domains: dict = {}
    for r in results:
        # Extract domain from table name: fact_rate_comparison -> rates
        table = r["table"]
        if table.startswith("fact_rate") or table.startswith("fact_medicaid_rate"):
            domain = "rates"
        elif table.startswith("fact_enroll") or table.startswith("fact_mc_"):
            domain = "enrollment"
        elif table.startswith("fact_cms64") or table.startswith("fact_expenditure"):
            domain = "expenditure"
        elif table.startswith("fact_five_star") or table.startswith("fact_pbj"):
            domain = "nursing"
        elif table.startswith("fact_sdud") or table.startswith("fact_nadac"):
            domain = "pharmacy"
        elif table.startswith("fact_quality") or table.startswith("fact_scorecard"):
            domain = "quality"
        elif table.startswith("fact_hpsa") or table.startswith("fact_bls"):
            domain = "workforce"
        elif table.startswith("dim_"):
            domain = "dimensions"
        else:
            domain = "other"

        if domain not in domains:
            domains[domain] = {"total": 0, "passed": 0}
        domains[domain]["total"] += 1
        if r["passed"]:
            domains[domain]["passed"] += 1

    domain_list = [
        {
            "domain": d,
            "total": v["total"],
            "passed": v["passed"],
            "failed": v["total"] - v["passed"],
            "pass_rate": round(v["passed"] / v["total"] * 100, 1) if v["total"] else 0,
        }
        for d, v in sorted(domains.items())
    ]
    return {"domains": domain_list}
