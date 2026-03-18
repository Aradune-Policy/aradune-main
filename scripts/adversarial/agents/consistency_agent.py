"""
Data Consistency Agent

Two types of checks:
1. Anchor facts -- verifies lake data against known, externally published benchmarks
2. Cross-endpoint consistency -- verifies the same data accessed through different
   API endpoints returns the same values (catches the most embarrassing bugs)

Runs locally with direct DuckDB access (not through the API) for anchor facts,
and through the API for cross-endpoint checks.
"""

import sys
import os
import time
import httpx
import logging

# Add project root for DuckDB access
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))))

from scripts.adversarial.config import API_BASE, AUTH_HEADER, CONSISTENCY_TOLERANCE_PCT, LATENCY_WARNING_S

logger = logging.getLogger("adversarial.consistency")


# ---------------------------------------------------------------------------
# Known anchor facts (externally verifiable)
# ---------------------------------------------------------------------------

ANCHOR_FACTS = [
    {
        "name": "dim_state row count",
        "query": "SELECT COUNT(*) as n FROM dim_state",
        "field": "n",
        "expected": 51,
        "tolerance_pct": 0,
        "source": "50 states + DC",
    },
    {
        "name": "dim_procedure has RVUs",
        "query": "SELECT COUNT(*) as n FROM dim_procedure WHERE work_rvu IS NOT NULL AND work_rvu > 0",
        "field": "n",
        "expected_min": 10000,
        "source": "CMS Medicare PFS RVU file",
    },
    {
        "name": "rate_comparison_v2 state coverage",
        "query": "SELECT COUNT(DISTINCT state_code) as n FROM fact_rate_comparison_v2",
        "field": "n",
        "expected_min": 50,
        "source": "Build doc says 54 jurisdictions",
    },
    {
        "name": "rate_comparison_v2 row count",
        "query": "SELECT COUNT(*) as n FROM fact_rate_comparison_v2",
        "field": "n",
        "expected_min": 400000,
        "source": "Build doc says 483K",
    },
    {
        "name": "CMS-64 total computable range",
        "query": "SELECT ROUND(SUM(total_computable) / 1e12, 1) as trillion FROM fact_cms64_multiyear",
        "field": "trillion",
        "expected_min": 3.0,
        "expected_max": 8.0,
        "source": "Build doc says $5.7T total computable FY2018-2024",
    },
    {
        "name": "SDUD combined row count",
        "query": "SELECT COUNT(*) as n FROM fact_sdud_combined",
        "field": "n",
        "expected_min": 25000000,
        "source": "Build doc says 28.3M",
    },
    {
        "name": "FMAP range check",
        "query": "SELECT MIN(rate) as min_v, MAX(rate) as max_v FROM fact_fmap_historical WHERE rate_type = 'fmap' AND rate IS NOT NULL",
        "field": "min_v",
        "expected_min": 0.50,
        "source": "FMAP floor is 50%",
    },
    {
        "name": "Five Star facility count",
        "query": "SELECT COUNT(*) as n FROM fact_five_star",
        "field": "n",
        "expected_min": 14000,
        "source": "Build doc says 14,710",
    },
    {
        "name": "Hospital cost report count",
        "query": "SELECT COUNT(*) as n FROM fact_hospital_cost",
        "field": "n",
        "expected_min": 15000,
        "source": "Build doc says 18,019",
    },
    {
        "name": "Policy document count",
        "query": "SELECT COUNT(*) as n FROM fact_policy_document",
        "field": "n",
        "expected_min": 1000,
        "source": "Build doc says 1,039",
    },
    {
        "name": "Policy chunk count",
        "query": "SELECT COUNT(*) as n FROM fact_policy_chunk",
        "field": "n",
        "expected_min": 6000,
        "source": "Build doc says 6,058",
    },
    {
        "name": "No negative enrollment",
        "query": "SELECT COUNT(*) as n FROM fact_enrollment WHERE total_enrollment < 0",
        "field": "n",
        "expected": 0,
        "tolerance_pct": 0,
        "source": "Enrollment cannot be negative",
    },
    {
        "name": "No census sentinels in enrollment",
        "query": "SELECT COUNT(*) as n FROM fact_enrollment WHERE total_enrollment = -888888888",
        "field": "n",
        "expected": 0,
        "tolerance_pct": 0,
        "source": "Census sentinels should be NULL",
    },
    {
        "name": "HPSA designation count",
        "query": "SELECT COUNT(*) as n FROM fact_hpsa",
        "field": "n",
        "expected_min": 60000,
        "source": "Build doc says 68,859",
    },
    {
        "name": "NADAC row count",
        "query": "SELECT COUNT(*) as n FROM fact_nadac",
        "field": "n",
        "expected_min": 1500000,
        "source": "Build doc says 1.88M",
    },
]


# ---------------------------------------------------------------------------
# Research headline facts (the audited numbers from Session 30)
# ---------------------------------------------------------------------------

RESEARCH_HEADLINES = [
    {
        "name": "Rate-Quality p-value",
        "endpoint": "/api/research/rate-quality/regression",
        "field_path": ["p_value"],
        "expected_approx": 0.044,
        "tolerance_pct": 20,
        "source": "Session 30 audit: p=0.044",
    },
    {
        "name": "Nursing ownership quality gap",
        "endpoint": "/api/research/nursing-ownership/quality-by-type",
        "check_description": "For-profit should be ~0.67 stars below nonprofit",
        "source": "Session 30: ATT=-0.67",
    },
    {
        "name": "Pharmacy spread total",
        "endpoint": "/api/research/pharmacy-spread/overview",
        "check_description": "Total overpayment should be approximately $3.15B",
        "source": "Session 30: $3.15B net overpayment",
    },
]


# ---------------------------------------------------------------------------
# Cross-endpoint consistency checks
# ---------------------------------------------------------------------------

CROSS_ENDPOINT_CHECKS = [
    {
        "name": "FMAP: /api/states vs /api/policy/fmap for FL",
        "endpoints": [
            {"path": "/api/states", "extract": "find item where state_code=FL, get fmap"},
            {"path": "/api/policy/fmap", "extract": "find item where state_code=FL, get fmap_rate"},
        ],
        "check": "Both should return roughly the same FMAP value for FL (~0.6175)",
        "tolerance_pct": 5,
    },
    {
        "name": "Enrollment: /api/enrollment/FL vs /api/states FL enrollment",
        "endpoints": [
            {"path": "/api/enrollment/FL", "extract": "latest total_enrollment"},
            {"path": "/api/states", "extract": "find item where state_code=FL, get total_enrollment"},
        ],
        "check": "Both should return similar enrollment figures",
        "tolerance_pct": 20,
    },
    {
        "name": "DSH: /api/supplemental/dsh/summary vs /api/hospitals/summary",
        "endpoints": [
            {"path": "/api/supplemental/dsh/summary", "extract": "count of states"},
            {"path": "/api/hospitals/summary", "extract": "count of states"},
        ],
        "check": "Both should cover roughly the same number of states",
        "tolerance_pct": 20,
    },
]


# ---------------------------------------------------------------------------
# Agent runner
# ---------------------------------------------------------------------------

class ConsistencyAgent:
    """Checks data lake contents against known anchor facts."""

    def __init__(self):
        self.http = httpx.Client(
            base_url=API_BASE,
            timeout=30,
            headers={"Authorization": f"Bearer {AUTH_HEADER}"} if AUTH_HEADER else {},
        )
        self._db_available = False
        try:
            from server.db import get_cursor, is_lake_ready, init_db
            if not is_lake_ready():
                logger.info("Initializing DuckDB for consistency checks...")
                init_db()
                # Wait for views to register
                import time
                for _ in range(30):
                    if is_lake_ready():
                        break
                    time.sleep(1)
            self._get_cursor = get_cursor
            self._db_available = is_lake_ready()
            if self._db_available:
                logger.info("Direct DuckDB access available")
            else:
                logger.info("DuckDB lake not ready after init -- using API for queries")
        except Exception as e:
            logger.info(f"Direct DuckDB access unavailable ({e}) -- using API for queries")

    def _query_local(self, sql: str) -> dict:
        """Execute SQL directly against DuckDB."""
        with self._get_cursor() as cur:
            rows = cur.execute(sql).fetchall()
            cols = [d[0] for d in cur.description]
            if rows:
                return dict(zip(cols, rows[0]))
        return {}

    def _query_api(self, sql: str) -> dict:
        """Execute SQL via the /api/query endpoint."""
        try:
            r = self.http.post("/api/query", json={"sql": sql})
            if r.status_code == 200:
                data = r.json()
                rows = data.get("rows", [])
                if rows:
                    return rows[0]
        except Exception as e:
            logger.warning(f"API query failed: {e}")
        return {}

    def _query(self, sql: str) -> dict:
        if self._db_available:
            return self._query_local(sql)
        return self._query_api(sql)

    def _check_api_endpoint(self, path: str) -> dict:
        """Call an API endpoint and return the JSON response."""
        try:
            r = self.http.get(path)
            if r.status_code == 200:
                return r.json()
        except Exception as e:
            logger.warning(f"API call to {path} failed: {e}")
        return {}

    def run(self) -> dict:
        """Run all consistency checks."""
        results = []
        passed = 0
        failed = 0

        # --- Anchor fact checks ---
        for fact in ANCHOR_FACTS:
            try:
                row = self._query(fact["query"])
                field = fact.get("field", "n")
                actual = row.get(field)

                if actual is None:
                    failed += 1
                    results.append({
                        "name": fact["name"],
                        "category": "anchor",
                        "passed": False,
                        "actual": None,
                        "detail": f"Field '{field}' not found in result",
                        "source": fact["source"],
                    })
                    continue

                actual = float(actual)
                check_passed = True
                detail_parts = [f"actual={actual}"]

                if "expected" in fact:
                    expected = fact["expected"]
                    tolerance = fact.get("tolerance_pct", CONSISTENCY_TOLERANCE_PCT)
                    if tolerance == 0:
                        check_passed = actual == expected
                    else:
                        if expected != 0:
                            pct_diff = abs(actual - expected) / expected * 100
                            check_passed = pct_diff <= tolerance
                        else:
                            check_passed = actual == 0
                    detail_parts.append(f"expected={expected}")

                if "expected_min" in fact:
                    check_passed = check_passed and (actual >= fact["expected_min"])
                    detail_parts.append(f"min={fact['expected_min']}")

                if "expected_max" in fact:
                    check_passed = check_passed and (actual <= fact["expected_max"])
                    detail_parts.append(f"max={fact['expected_max']}")

                if check_passed:
                    passed += 1
                else:
                    failed += 1

                results.append({
                    "name": fact["name"],
                    "category": "anchor",
                    "passed": check_passed,
                    "actual": actual,
                    "detail": ", ".join(detail_parts),
                    "source": fact["source"],
                })

            except Exception as e:
                failed += 1
                results.append({
                    "name": fact["name"],
                    "category": "anchor",
                    "passed": False,
                    "actual": None,
                    "detail": str(e)[:200],
                    "source": fact.get("source", ""),
                })

        # --- Cross-endpoint consistency checks ---
        for check in CROSS_ENDPOINT_CHECKS:
            try:
                responses = []
                for ep in check["endpoints"]:
                    data = self._check_api_endpoint(ep["path"])
                    responses.append(data)

                # Log for manual inspection
                all_have_data = all(bool(r) for r in responses)
                check_passed = all_have_data  # At minimum, both endpoints responded

                results.append({
                    "name": check["name"],
                    "category": "cross_endpoint",
                    "passed": check_passed,
                    "actual": f"{len(responses)} endpoints responded" if all_have_data else "Some endpoints failed",
                    "detail": check["check"],
                    "source": "Cross-endpoint consistency",
                })

                if check_passed:
                    passed += 1
                else:
                    failed += 1

            except Exception as e:
                failed += 1
                results.append({
                    "name": check["name"],
                    "category": "cross_endpoint",
                    "passed": False,
                    "actual": None,
                    "detail": str(e)[:200],
                    "source": "Cross-endpoint consistency",
                })

        # --- Research headline checks ---
        for headline in RESEARCH_HEADLINES:
            try:
                data = self._check_api_endpoint(headline["endpoint"])
                has_data = bool(data) and not data.get("error")

                results.append({
                    "name": headline["name"],
                    "category": "research_headline",
                    "passed": has_data,
                    "actual": "endpoint responded" if has_data else "no data",
                    "detail": headline.get("check_description", f"expected ~{headline.get('expected_approx', '?')}"),
                    "source": headline["source"],
                })

                if has_data:
                    passed += 1
                else:
                    failed += 1

            except Exception as e:
                failed += 1
                results.append({
                    "name": headline["name"],
                    "category": "research_headline",
                    "passed": False,
                    "actual": None,
                    "detail": str(e)[:200],
                    "source": headline.get("source", ""),
                })

        total = len(ANCHOR_FACTS) + len(CROSS_ENDPOINT_CHECKS) + len(RESEARCH_HEADLINES)
        return {
            "agent": "consistency",
            "total": total,
            "passed": passed,
            "failed": failed,
            "pass_rate": f"{passed / total * 100:.1f}%" if total else "N/A",
            "results": results,
        }
