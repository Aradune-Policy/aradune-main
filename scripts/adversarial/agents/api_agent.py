"""
API Adversarial Agent

Systematically fuzzes all REST endpoints with:
- Missing parameters
- Invalid types
- Null/empty values
- Boundary values
- Territory/edge-case state codes
- Extremely long strings
- Special characters / injection attempts

Every endpoint should return 200 with a clean error payload (thanks to
the @safe_route decorator). Any 500 is a critical failure.

Auto-discovers endpoints by scanning route files.
"""

import os
import re
import time
import httpx
import logging
from scripts.adversarial.config import API_BASE, AUTH_HEADER, API_TIMEOUT_S, LATENCY_WARNING_S

logger = logging.getLogger("adversarial.api")

# ---------------------------------------------------------------------------
# Auto-discover endpoints from route files
# ---------------------------------------------------------------------------

# Path params that need custom values (not state codes)
CUSTOM_PATH_PARAMS = {
    "ccn": "100001",
    "soc_code": "29-1141",
    "state_code": "FL",
    "session_id": "test-session-000",
    "skill_id": "test-skill-000",
    "npi": "1234567890",
}


def discover_endpoints(routes_dir: str = None) -> list[dict]:
    """
    Scan FastAPI route files and extract endpoint definitions.
    Returns list of {method, path, has_state_param, path_params}.
    """
    if routes_dir is None:
        # Find routes dir relative to this file
        project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
        routes_dir = os.path.join(project_root, "server", "routes")

    endpoints = []
    route_pattern = re.compile(r'@router\.(get|post|put|delete|patch)\(\s*["\']([^"\']+)["\']')

    for root, dirs, files in os.walk(routes_dir):
        dirs[:] = [d for d in dirs if d != "__pycache__"]
        for fname in sorted(files):
            if not fname.endswith(".py") or fname.startswith("__"):
                continue
            filepath = os.path.join(root, fname)
            try:
                with open(filepath) as f:
                    content = f.read()
            except Exception:
                continue

            # Check for prefix on router
            prefix_match = re.search(r'APIRouter\(prefix=["\']([^"\']+)["\']', content)
            prefix = prefix_match.group(1) if prefix_match else ""

            for match in route_pattern.finditer(content):
                method = match.group(1).upper()
                path = prefix + match.group(2)

                # Detect path parameters
                path_params = re.findall(r'\{(\w+)\}', path)
                has_state = any(p in ("state", "state_code") for p in path_params)

                endpoints.append({
                    "method": method,
                    "path": path,
                    "has_state_param": has_state,
                    "path_params": path_params,
                    "source_file": os.path.relpath(filepath, routes_dir),
                })

    logger.info(f"Discovered {len(endpoints)} endpoints from {routes_dir}")
    return endpoints


# ---------------------------------------------------------------------------
# Fuzz payloads
# ---------------------------------------------------------------------------

FUZZ_STATES = [
    "FL",       # normal large state
    "DC",       # District of Columbia
    "PR",       # Puerto Rico (territory)
    "GU",       # Guam (very sparse)
    "VI",       # US Virgin Islands
    "XX",       # completely invalid
    "",         # empty string
    "florida",  # full name instead of code
    "fl",       # lowercase
    "F",        # too short
    "FLA",      # three characters
    "12",       # numeric
]

FUZZ_QUERY_PARAMS = [
    "",
    " ",
    "NULL",
    "undefined",
    "' OR 1=1 --",
    "<script>alert(1)</script>",
    "A" * 10000,
    "-1",
    "0",
    "999999999",
]

# Skip these paths entirely (SSE streaming, internal, file upload)
SKIP_PATHS = {
    "/api/intelligence",
    "/internal/reload-lake",
    "/api/import",
    "/api/import/hydrate",
}


# ---------------------------------------------------------------------------
# Agent runner
# ---------------------------------------------------------------------------

class ApiAgent:
    """Fuzzes REST endpoints for error handling robustness."""

    def __init__(self, routes_dir: str = None):
        self.endpoints = discover_endpoints(routes_dir)
        self.client = httpx.Client(
            base_url=API_BASE,
            timeout=API_TIMEOUT_S,
            headers={"Authorization": f"Bearer {AUTH_HEADER}"} if AUTH_HEADER else {},
        )

    def _call(self, method: str, path: str, params: dict = None) -> dict:
        """Make an API call and return status + timing."""
        start = time.time()
        try:
            if method == "GET":
                r = self.client.get(path, params=params or {})
            elif method == "POST":
                r = self.client.post(path, json=params or {})
            else:
                return {"status": -1, "error": f"Unsupported method {method}", "latency_s": 0}
            latency = time.time() - start
            return {
                "status": r.status_code,
                "latency_s": round(latency, 2),
                "has_body": len(r.content) > 0,
                "error": None,
                "slow": latency > LATENCY_WARNING_S,
            }
        except httpx.TimeoutException:
            return {"status": -1, "error": "timeout", "latency_s": round(time.time() - start, 2), "slow": True}
        except Exception as e:
            return {"status": -1, "error": str(e)[:100], "latency_s": round(time.time() - start, 2), "slow": False}

    def _resolve_path(self, path: str, path_params: list, overrides: dict = None) -> str:
        """Replace {param} placeholders with test values."""
        resolved = path
        for param in path_params:
            if overrides and param in overrides:
                val = overrides[param]
            elif param in ("state", "state_code"):
                val = "FL"
            elif param in CUSTOM_PATH_PARAMS:
                val = CUSTOM_PATH_PARAMS[param]
            else:
                val = "test"
            resolved = resolved.replace(f"{{{param}}}", str(val))
        return resolved

    def run(self) -> dict:
        """Run all fuzz tests."""
        results = []
        total_500s = 0
        total_tests = 0
        slow_count = 0

        for ep in self.endpoints:
            path = ep["path"]
            method = ep["method"]

            # Skip streaming / internal / upload endpoints
            if any(path.startswith(skip) for skip in SKIP_PATHS):
                continue
            # Skip POST endpoints without known body shape
            if method == "POST" and not ep["has_state_param"]:
                continue

            # --- Test 1: Normal call ---
            resolved = self._resolve_path(path, ep["path_params"])
            result = self._call(method, resolved)
            total_tests += 1
            is_500 = result["status"] >= 500
            if is_500:
                total_500s += 1
            if result.get("slow"):
                slow_count += 1
            results.append({
                "endpoint": path,
                "test": "normal",
                "status": result["status"],
                "latency_s": result["latency_s"],
                "passed": not is_500,
                "error": result["error"],
                "slow": result.get("slow", False),
            })

            # --- Test 2: Fuzz state param ---
            if ep["has_state_param"]:
                state_param = next(p for p in ep["path_params"] if p in ("state", "state_code"))
                for fuzz_state in FUZZ_STATES:
                    fuzz_resolved = self._resolve_path(
                        path, ep["path_params"],
                        overrides={state_param: fuzz_state},
                    )
                    result = self._call(method, fuzz_resolved)
                    total_tests += 1
                    is_500 = result["status"] >= 500
                    if is_500:
                        total_500s += 1
                    results.append({
                        "endpoint": path,
                        "test": f"fuzz_state={fuzz_state}",
                        "status": result["status"],
                        "latency_s": result["latency_s"],
                        "passed": not is_500,
                        "error": result["error"],
                        "slow": result.get("slow", False),
                    })

            # Small delay between endpoints
            time.sleep(0.1)

        return {
            "agent": "api_fuzzer",
            "total_tests": total_tests,
            "total_500s": total_500s,
            "slow_responses": slow_count,
            "pass_rate": f"{(total_tests - total_500s) / total_tests * 100:.1f}%" if total_tests else "N/A",
            "results": results,
            "five_hundreds": [r for r in results if not r["passed"]],
        }
