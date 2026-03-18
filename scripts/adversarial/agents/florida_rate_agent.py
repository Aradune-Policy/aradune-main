"""
Florida Rate Structure Agent

Validates FL Medicaid rate structure rules. 99.96% of FL codes have either a
Facility rate OR a PC/TC split, but 3 codes (46924, 91124, 91125) legitimately
have BOTH, as published by AHCA.

Two phases:
1. Data Layer SQL tests (no LLM, no cost) -- direct DuckDB validation
2. Intelligence endpoint tests (7 queries) -- evaluated by Haiku
"""

import json
import os
import re
import sys
import time
import logging
import httpx

# Add project root for DuckDB access
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))))

from scripts.adversarial.config import (
    API_BASE, AUTH_HEADER, INTELLIGENCE_TIMEOUT_S,
    HAIKU_MODEL, LATENCY_WARNING_S,
)

logger = logging.getLogger("adversarial.florida_rate")

ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")

FLAGGED_CODES = ("46924", "91124", "91125")


# ---------------------------------------------------------------------------
# Phase 1: SQL test definitions
# ---------------------------------------------------------------------------

SQL_TESTS = [
    {
        "name": "test_only_3_codes_have_both_facility_and_pctc",
        "description": (
            "Only 3 FL codes (46924, 91124, 91125) should have BOTH a modifier "
            "in ('26','TC') AND a facility rate > 0. These are legitimate per AHCA. "
            "Any OTHER code having both is a data error."
        ),
        "sql": """
            SELECT procedure_code
            FROM (
                SELECT cpt_hcpcs_code AS procedure_code,
                       MAX(CASE WHEN modifier IN ('26','TC') THEN 1 ELSE 0 END) AS has_pctc,
                       MAX(CASE WHEN rate_facility > 0 THEN 1 ELSE 0 END) AS has_facility
                FROM (
                    SELECT cpt_hcpcs_code,
                           modifier,
                           COALESCE(medicaid_rate, 0) AS rate_facility
                    FROM fact_medicaid_rate
                    WHERE state_code = 'FL'
                )
                GROUP BY cpt_hcpcs_code
                HAVING has_pctc = 1 AND has_facility = 1
            )
            WHERE procedure_code NOT IN ('46924', '91124', '91125')
        """,
        "expected": "zero_rows",
        "severity": "critical",
    },
    {
        "name": "test_flagged_codes_have_both_facility_and_pctc",
        "description": (
            "Codes 46924, 91124, 91125 should have BOTH facility rates AND "
            "PC/TC modifiers in the FL rate table. This is correct behavior "
            "per AHCA -- these 3 codes legitimately carry both."
        ),
        "sql": """
            SELECT procedure_code, has_pctc, has_facility
            FROM (
                SELECT cpt_hcpcs_code AS procedure_code,
                       MAX(CASE WHEN modifier IN ('26','TC') THEN 1 ELSE 0 END) AS has_pctc,
                       MAX(CASE WHEN rate_facility > 0 THEN 1 ELSE 0 END) AS has_facility
                FROM (
                    SELECT cpt_hcpcs_code,
                           modifier,
                           COALESCE(medicaid_rate, 0) AS rate_facility
                    FROM fact_medicaid_rate
                    WHERE state_code = 'FL'
                      AND cpt_hcpcs_code IN ('46924', '91124', '91125')
                )
                GROUP BY cpt_hcpcs_code
            )
            WHERE has_pctc = 1 AND has_facility = 1
        """,
        "expected": "has_rows",
        "severity": "critical",
    },
    {
        "name": "test_flagged_codes_exist",
        "description": (
            "Flagged codes 46924, 91124, 91125 should exist in the FL rate table "
            "with appropriate handling (rate or methodology note)."
        ),
        "sql": """
            SELECT cpt_hcpcs_code AS procedure_code, modifier, medicaid_rate
            FROM fact_medicaid_rate
            WHERE state_code = 'FL'
              AND cpt_hcpcs_code IN ('46924', '91124', '91125')
        """,
        "expected": "has_rows",
        "severity": "high",
    },
    {
        "name": "test_no_duplicate_modifier_per_code",
        "description": (
            "No FL procedure code should have duplicate modifier entries. "
            "Each (procedure_code, modifier) pair must be unique."
        ),
        "sql": """
            SELECT cpt_hcpcs_code AS procedure_code, modifier, COUNT(*) AS cnt
            FROM fact_medicaid_rate
            WHERE state_code = 'FL'
            GROUP BY cpt_hcpcs_code, modifier
            HAVING COUNT(*) > 1
        """,
        "expected": "zero_rows",
        "severity": "high",
    },
]


# ---------------------------------------------------------------------------
# Phase 2: Intelligence query definitions
# ---------------------------------------------------------------------------

INTELLIGENCE_QUERIES = [
    {
        "query": "What is Florida's Medicaid rate for CPT 93000?",
        "check": (
            "The response should show FL Medicaid rate data for 93000. For most FL codes, "
            "rates have either facility/non-facility OR PC/TC, not both. The response "
            "should return accurate rate data."
        ),
        "severity": "critical",
    },
    {
        "query": "Show me Florida's rate for 99213",
        "check": (
            "99213 is an E/M code. FL Medicaid does not apply PC/TC split to E/M codes. "
            "The response should NOT mention modifier 26 or TC for this code. It should "
            "show rate(s) without PC/TC terminology."
        ),
        "severity": "critical",
    },
    {
        "query": "What is FL's rate for chest X-ray 71046?",
        "check": (
            "71046 is an imaging code. FL Medicaid applies PC/TC modifiers to imaging. "
            "The response should mention the professional component (modifier 26) and/or "
            "technical component (TC) if applicable."
        ),
        "severity": "high",
    },
    {
        "query": "Does Florida Medicaid pay for CPT 46924?",
        "check": (
            "46924 is one of 3 FL codes (46924, 91124, 91125) that legitimately have "
            "BOTH facility rates AND PC/TC modifiers, as published by AHCA. The response "
            "should return accurate rate data. If it mentions both facility and PC/TC "
            "rates for this code, that is CORRECT, not a violation."
        ),
        "severity": "critical",
    },
    {
        "query": "Show FL Medicaid rates for 91124",
        "check": (
            "91124 is one of 3 FL codes (46924, 91124, 91125) that legitimately have "
            "BOTH facility rates AND PC/TC modifiers, as published by AHCA. The response "
            "should return accurate rate data showing both if present in the data."
        ),
        "severity": "critical",
    },
    {
        "query": (
            "Do any Florida Medicaid codes have both facility rates and "
            "PC/TC splits?"
        ),
        "check": (
            "The response should correctly explain that while 99.96% of FL codes have "
            "either facility rates OR PC/TC splits, 3 codes (46924, 91124, 91125) "
            "legitimately have BOTH, as published by AHCA. A response claiming NO codes "
            "can have both is INCORRECT. A response failing to mention the 3 exceptions "
            "is incomplete but not necessarily wrong if it acknowledges exceptions exist."
        ),
        "severity": "high",
    },
    {
        "query": (
            "Can a Florida Medicaid code have both a facility rate and a TC modifier?"
        ),
        "check": (
            "The answer should be YES, but only for 3 specific codes: 46924, 91124, "
            "and 91125. For 99.96% of FL codes, facility and PC/TC are mutually "
            "exclusive. A blanket 'no' is INCORRECT. The response should acknowledge "
            "the 3 legitimate exceptions published by AHCA."
        ),
        "severity": "critical",
    },
]


# ---------------------------------------------------------------------------
# Haiku evaluator prompt
# ---------------------------------------------------------------------------

EVALUATOR_PROMPT = """You are evaluating an AI system's response about Florida Medicaid rates.

The query was: {query}

The system responded with:
---
{response}
---

Evaluation criteria:
{check}

Did the response PASS or FAIL this check? Be strict. Note: 3 FL codes (46924, 91124,
91125) legitimately have BOTH facility rates AND PC/TC splits per AHCA. Showing both
for these codes is CORRECT. For all other codes, facility and PC/TC should be mutually
exclusive.

Respond with JSON only:
{{
    "passed": true/false,
    "reasoning": "1-2 sentence explanation"
}}"""


# ---------------------------------------------------------------------------
# Agent
# ---------------------------------------------------------------------------

class FloridaRateAgent:
    """Adversarial agent: validates FL Medicaid rate structure rules."""

    def __init__(self):
        self.results = []
        self._db_available = False
        self._get_cursor = None
        self._anthropic_client = None

        # Try to get direct DuckDB access (same pattern as ConsistencyAgent)
        try:
            from server.db import get_cursor, is_lake_ready, init_db
            if not is_lake_ready():
                logger.info("Initializing DuckDB for FL rate checks...")
                init_db()
                for _ in range(30):
                    if is_lake_ready():
                        break
                    time.sleep(1)
            self._get_cursor = get_cursor
            self._db_available = is_lake_ready()
            if self._db_available:
                logger.info("Direct DuckDB access available")
            else:
                logger.info("DuckDB lake not ready -- will use API fallback")
        except Exception as e:
            logger.info(f"Direct DuckDB access unavailable ({e}) -- will use API fallback")

        # HTTP client for API calls
        self.http = httpx.Client(
            base_url=API_BASE,
            timeout=30,
            headers={"Authorization": f"Bearer {AUTH_HEADER}"} if AUTH_HEADER else {},
        )

        # Anthropic client for evaluator (lazy init)
        if ANTHROPIC_API_KEY:
            try:
                from anthropic import Anthropic
                self._anthropic_client = Anthropic()
            except ImportError:
                logger.warning("anthropic SDK not installed -- Intelligence tests will be skipped")

    # ----- SQL execution -----

    def _sql_query(self, sql: str) -> list[dict]:
        """Execute SQL and return list of row dicts."""
        if self._db_available:
            return self._sql_query_local(sql)
        return self._sql_query_api(sql)

    def _sql_query_local(self, sql: str) -> list[dict]:
        """Execute SQL directly against DuckDB."""
        try:
            with self._get_cursor() as cur:
                rows = cur.execute(sql).fetchall()
                cols = [d[0] for d in cur.description]
                return [dict(zip(cols, row)) for row in rows]
        except Exception as e:
            logger.error(f"Local SQL error: {e}")
            raise

    def _sql_query_api(self, sql: str) -> list[dict]:
        """Execute SQL via the /api/query endpoint."""
        try:
            r = self.http.post("/api/query", json={"sql": sql})
            if r.status_code == 200:
                data = r.json()
                return data.get("rows", [])
            raise RuntimeError(f"API returned HTTP {r.status_code}")
        except httpx.ConnectError:
            raise RuntimeError("Server not running -- cannot execute SQL via API")

    # ----- Intelligence endpoint -----

    def _call_intelligence(self, query: str) -> dict:
        """Call the Intelligence endpoint and collect the response."""
        start = time.time()
        response_text = ""
        error = None

        try:
            headers = {"Content-Type": "application/json"}
            if AUTH_HEADER:
                headers["Authorization"] = f"Bearer {AUTH_HEADER}"

            resp = self.http.post(
                "/api/intelligence",
                json={"message": query, "history": []},
                headers=headers,
                timeout=INTELLIGENCE_TIMEOUT_S,
            )

            if resp.status_code != 200:
                return {
                    "response_text": "",
                    "error": f"HTTP {resp.status_code}",
                    "latency_s": round(time.time() - start, 1),
                }

            content_type = resp.headers.get("content-type", "")

            if "text/event-stream" in content_type:
                # SSE mode
                for line in resp.text.split("\n"):
                    line = line.strip()
                    if line.startswith("data: "):
                        raw = line[6:]
                        try:
                            data = json.loads(raw)
                            if "text" in data:
                                response_text += data["text"]
                        except json.JSONDecodeError:
                            continue
            else:
                # JSON mode
                try:
                    data = resp.json()
                    response_text = data.get("response", "")
                except json.JSONDecodeError:
                    response_text = resp.text

        except httpx.TimeoutException:
            error = "timeout"
        except httpx.ConnectError:
            error = "server_not_running"
        except Exception as e:
            error = str(e)[:200]

        return {
            "response_text": response_text,
            "error": error,
            "latency_s": round(time.time() - start, 1),
        }

    # ----- Haiku evaluator -----

    def _evaluate_response(self, response_text: str, query: str, criteria: str) -> dict:
        """Use Haiku to evaluate a response against criteria."""
        if not self._anthropic_client:
            return {"passed": False, "reasoning": "No Anthropic client available"}

        try:
            result = self._anthropic_client.messages.create(
                model=HAIKU_MODEL,
                max_tokens=300,
                messages=[{
                    "role": "user",
                    "content": EVALUATOR_PROMPT.format(
                        query=query,
                        response=response_text[:3000],
                        check=criteria,
                    ),
                }],
            )
            text = result.content[0].text.strip()
            text = text.replace("```json", "").replace("```", "").strip()
            # Extract JSON object
            json_match = re.search(r'\{[^{}]*(?:\{[^{}]*\}[^{}]*)*\}', text, re.DOTALL)
            if json_match:
                return json.loads(json_match.group())
            return json.loads(text)
        except Exception as e:
            logger.warning(f"Haiku evaluation failed: {e}")
            return {
                "passed": False,
                "reasoning": f"Evaluator error: {e}",
            }

    # ----- Phase 1: SQL tests -----

    def _run_sql_tests(self):
        """Phase 1: data layer validation (no LLM cost)."""
        for test in SQL_TESTS:
            try:
                rows = self._sql_query(test["sql"])
                row_count = len(rows)

                if test["expected"] == "zero_rows":
                    passed = row_count == 0
                    issue = None if passed else (
                        f"Found {row_count} violating row(s): "
                        f"{json.dumps(rows[:3], default=str)}"
                    )
                elif test["expected"] == "has_rows":
                    passed = row_count > 0
                    issue = None if passed else "Expected rows but got none"
                else:
                    passed = False
                    issue = f"Unknown expected type: {test['expected']}"

                self.results.append({
                    "name": test["name"],
                    "phase": "sql",
                    "description": test["description"],
                    "passed": passed,
                    "specific_issue": issue,
                    "row_count": row_count,
                    "sample_rows": rows[:3] if rows else [],
                    "severity": test["severity"],
                })

            except Exception as e:
                self.results.append({
                    "name": test["name"],
                    "phase": "sql",
                    "description": test["description"],
                    "passed": False,
                    "specific_issue": f"SQL execution error: {e}",
                    "row_count": 0,
                    "sample_rows": [],
                    "severity": test["severity"],
                })

    # ----- Phase 2: Intelligence tests -----

    def _run_intelligence_tests(self):
        """Phase 2: Intelligence endpoint tests (requires Anthropic key)."""
        for i, iq in enumerate(INTELLIGENCE_QUERIES):
            logger.info(
                f"[FL Rate {i+1}/{len(INTELLIGENCE_QUERIES)}] {iq['query'][:60]}..."
            )

            intel_result = self._call_intelligence(iq["query"])

            if intel_result.get("error"):
                self.results.append({
                    "name": f"intelligence_{i+1}",
                    "phase": "intelligence",
                    "query": iq["query"],
                    "passed": False,
                    "reasoning": f"Endpoint error: {intel_result['error']}",
                    "specific_issue": intel_result["error"],
                    "severity": iq["severity"],
                    "latency_s": intel_result.get("latency_s", 0),
                    "response_excerpt": "",
                })
                continue

            evaluation = self._evaluate_response(
                intel_result["response_text"],
                iq["query"],
                iq["check"],
            )

            self.results.append({
                "name": f"intelligence_{i+1}",
                "phase": "intelligence",
                "query": iq["query"],
                "passed": evaluation.get("passed", False),
                "reasoning": evaluation.get("reasoning", ""),
                "specific_issue": None if evaluation.get("passed") else evaluation.get("reasoning"),
                "severity": iq["severity"],
                "latency_s": intel_result.get("latency_s", 0),
                "response_excerpt": intel_result["response_text"][:500],
            })

            # Brief pause between Intelligence calls
            time.sleep(2)

    # ----- Main runner -----

    def run(self) -> dict:
        """Run all tests, return standard agent report."""
        t0 = time.time()

        # Phase 1: SQL tests (always run -- no LLM cost)
        self._run_sql_tests()

        # Phase 2: Intelligence tests (only if API key available)
        if ANTHROPIC_API_KEY and self._anthropic_client:
            self._run_intelligence_tests()
        else:
            logger.info("Skipping Intelligence tests (no ANTHROPIC_API_KEY)")

        passed = sum(1 for r in self.results if r["passed"])
        total = len(self.results)
        failed = total - passed

        return {
            "agent": "florida_rate",
            "total": total,
            "passed": passed,
            "failed": failed,
            "pass_rate": f"{round(passed / max(total, 1) * 100, 1)}%",
            "duration_s": round(time.time() - t0, 1),
            "results": self.results,
        }


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(name)s %(message)s",
    )
    agent = FloridaRateAgent()
    report = agent.run()

    print(f"\n{'='*60}")
    print(f"FL RATE AGENT: {report['pass_rate']} "
          f"({report['passed']}/{report['total']} passed) "
          f"in {report['duration_s']}s")
    print(f"{'='*60}")

    for r in report["results"]:
        status = "PASS" if r["passed"] else "FAIL"
        print(f"  [{status}] {r['name']}: {r.get('specific_issue') or r.get('reasoning') or 'OK'}")

    print()
    # Exit non-zero if any critical failure
    for r in report["results"]:
        if not r["passed"] and r.get("severity") == "critical":
            sys.exit(1)
    sys.exit(0)
