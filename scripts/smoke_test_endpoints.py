#!/usr/bin/env python3
"""
Smoke test all Aradune API endpoints.

Discovers endpoints by parsing route files, then hits each with HTTP requests.
Tests: no parameters, state=FL, state=DC (territory), state=XX (invalid).

Usage:
    python3 scripts/smoke_test_endpoints.py                          # default: http://localhost:8000
    python3 scripts/smoke_test_endpoints.py http://localhost:8000     # explicit base URL
    python3 scripts/smoke_test_endpoints.py https://aradune-api.fly.dev  # production

Options (via env vars):
    SMOKE_TIMEOUT=10        Per-request timeout in seconds (default: 10)
    SMOKE_CONCURRENCY=5     Max parallel requests (default: 5)
    SMOKE_SKIP_POST=1       Skip POST endpoints (default: test them)
    SMOKE_VERBOSE=1         Print every request/response (default: summary only)
"""

import json
import os
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field, asdict
from datetime import datetime
from pathlib import Path
from typing import Optional

import requests

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

BASE_URL = sys.argv[1].rstrip("/") if len(sys.argv) > 1 else "http://localhost:8000"
TIMEOUT = int(os.environ.get("SMOKE_TIMEOUT", "10"))
CONCURRENCY = int(os.environ.get("SMOKE_CONCURRENCY", "5"))
SKIP_POST = os.environ.get("SMOKE_SKIP_POST", "0") == "1"
VERBOSE = os.environ.get("SMOKE_VERBOSE", "0") == "1"

PROJECT_ROOT = Path(__file__).resolve().parent.parent
ROUTES_DIR = PROJECT_ROOT / "server" / "routes"
RESULTS_FILE = PROJECT_ROOT / "scripts" / "smoke_test_results.json"

# Valid state for testing (large state, should have data everywhere)
TEST_STATE = "FL"
# Territory for edge-case testing
TEST_TERRITORY = "DC"
# Invalid state code -- should return empty or 404, not 500
TEST_INVALID = "XX"

# ---------------------------------------------------------------------------
# Endpoint discovery
# ---------------------------------------------------------------------------

# Routers with prefix= in their APIRouter() constructor.
# Key: Python module filename (without .py) -> prefix string
ROUTER_PREFIXES = {
    "intelligence": "/api/intelligence",
    "nl2sql": "/api/nl2sql",
    "import_data": "/api/import",
}

# Endpoints that require specific query params to work (not just state)
# Map from endpoint path -> dict of query params to use
REQUIRED_PARAMS = {
    "/api/search": {"q": "hospital"},
    "/api/hospitals/search": {"q": "memorial"},
    "/api/rate-explorer": {"code": "99213"},
    "/api/rate-explorer/search": {"q": "office"},
    "/api/pharmacy/nadac": {"search": "metformin"},
    "/api/epsdt": {},  # no state param needed
    "/api/cpra/compare": {},  # has its own defaults
}

# Endpoints to skip entirely (require auth, mutate state, or are streaming)
SKIP_ENDPOINTS = {
    "/api/intelligence",           # POST, needs API key + Claude call
    "/api/intelligence/stream",    # SSE streaming
    "/api/intelligence/feedback",  # POST, needs body
    "/api/nl2sql",                 # POST, needs API key + Claude call
    "/api/import",                 # POST, file upload
    "/api/import/hydrate",         # POST, needs session_id
    "/api/forecast/generate",      # POST, needs complex body
    "/api/forecast/generate/csv",  # POST, needs complex body
    "/api/forecast/expenditure",   # POST, needs complex body
    "/api/forecast/expenditure/csv",  # POST, needs complex body
    "/api/forecast/expenditure-only",  # POST, needs complex body
    "/api/forecast/fiscal-impact",    # POST, needs complex body
    "/api/forecast/fiscal-impact/csv",  # POST, needs complex body
    "/api/cpra/upload/generate",       # POST, needs file data
    "/api/cpra/upload/generate/csv",   # POST, needs file data
    "/api/cpra/upload/generate/report",  # POST, needs file data
    "/api/query",                  # POST, needs SQL body
    "/api/skillbook/manual",       # POST, mutates
    "/api/pipeline/status",        # internal
    "/internal/reload-lake",       # internal POST
}

# Endpoints with path params that are NOT state codes
NON_STATE_PATH_PARAMS = {
    "/api/hospitals/ccn/{ccn}": {"ccn": "100001"},
    "/api/hospitals/ccn/{ccn}/peers": {"ccn": "100001"},
    "/api/wages/compare/{soc_code}": {"soc_code": "29-1141"},
    "/api/cpra/rates/{state_code}": {"state_code": "FL"},
    "/api/cpra/dq/{state_code}": {"state_code": "FL"},
    "/api/research/waiver-impact/enrollment/{state_code}": {"state_code": "FL"},
    "/api/research/waiver-impact/spending/{state_code}": {"state_code": "FL"},
    "/api/research/waiver-impact/quality/{state_code}": {"state_code": "FL"},
    "/api/import/sessions/{session_id}": None,  # skip -- needs real session
    "/api/import/sessions/{session_id}/quarantine": None,  # skip
}


@dataclass
class Endpoint:
    """Discovered API endpoint."""
    path: str                       # e.g., "/api/states"
    method: str                     # "GET" or "POST"
    source_file: str                # route file it came from
    has_state_param: bool = False   # path contains {state_code}
    path_params: dict = field(default_factory=dict)  # non-state path params


@dataclass
class TestResult:
    """Result of one smoke test request."""
    endpoint: str
    method: str
    variant: str           # "base", "FL", "DC", "XX"
    url: str
    status_code: int
    response_time_ms: float
    passed: bool
    error: Optional[str] = None
    row_count: Optional[int] = None  # len(response) if it's a list


def discover_endpoints() -> list[Endpoint]:
    """Parse all route files to discover endpoints."""
    endpoints = []
    route_files = list(ROUTES_DIR.glob("*.py")) + list((ROUTES_DIR / "research").glob("*.py"))

    for fpath in sorted(route_files):
        if fpath.name.startswith("__"):
            continue

        text = fpath.read_text()
        module_name = fpath.stem

        # Determine prefix for this module
        prefix = ROUTER_PREFIXES.get(module_name, "")

        # Find all @router.get("...") and @router.post("...") patterns
        # Also match empty-string routes like @router.post("")
        pattern = r'@router\.(get|post)\(\s*["\']([^"\']*)["\']'
        for match in re.finditer(pattern, text):
            method = match.group(1).upper()
            raw_path = match.group(2)

            # For prefixed routers, the path in the decorator is relative
            if prefix and (not raw_path or not raw_path.startswith("/api")):
                full_path = prefix + raw_path
            else:
                full_path = raw_path

            has_state = "{state_code}" in full_path

            ep = Endpoint(
                path=full_path,
                method=method,
                source_file=str(fpath.relative_to(PROJECT_ROOT)),
                has_state_param=has_state,
            )

            # Check if this has non-state path params
            if full_path in NON_STATE_PATH_PARAMS:
                params = NON_STATE_PATH_PARAMS[full_path]
                if params is None:
                    continue  # skip this endpoint
                ep.path_params = params

            endpoints.append(ep)

    return endpoints


def build_url(ep: Endpoint, variant: str) -> Optional[str]:
    """Build the full URL for a test variant."""
    path = ep.path

    # Handle path parameters
    if ep.has_state_param:
        if variant == "base":
            return None  # can't call without state
        path = path.replace("{state_code}", variant)
    elif ep.path_params:
        for param, value in ep.path_params.items():
            path = path.replace(f"{{{param}}}", value)

    url = f"{BASE_URL}{path}"

    # Add required query params if any
    base_path = ep.path
    if base_path in REQUIRED_PARAMS:
        params = REQUIRED_PARAMS[base_path]
        if params:
            qs = "&".join(f"{k}={v}" for k, v in params.items())
            url = f"{url}?{qs}"

    return url


def run_test(ep: Endpoint, variant: str) -> Optional[TestResult]:
    """Execute one smoke test request."""
    url = build_url(ep, variant)
    if url is None:
        return None

    # Skip endpoints in the skip list
    if ep.path in SKIP_ENDPOINTS:
        return None

    if ep.method == "POST" and SKIP_POST:
        return None

    start = time.time()
    error = None
    status_code = 0
    row_count = None

    try:
        if ep.method == "GET":
            resp = requests.get(url, timeout=TIMEOUT)
        else:
            resp = requests.post(url, json={}, timeout=TIMEOUT)

        status_code = resp.status_code
        elapsed_ms = (time.time() - start) * 1000

        # Try to get row count from response
        try:
            data = resp.json()
            if isinstance(data, list):
                row_count = len(data)
            elif isinstance(data, dict) and "data" in data and isinstance(data["data"], list):
                row_count = len(data["data"])
        except Exception:
            pass

    except requests.exceptions.ConnectionError:
        elapsed_ms = (time.time() - start) * 1000
        error = "CONNECTION_REFUSED"
    except requests.exceptions.Timeout:
        elapsed_ms = (time.time() - start) * 1000
        error = "TIMEOUT"
    except Exception as e:
        elapsed_ms = (time.time() - start) * 1000
        error = str(e)[:200]

    # Determine pass/fail
    if error:
        passed = False
    elif status_code == 500:
        passed = False
        error = "INTERNAL_SERVER_ERROR"
    elif variant == "XX" and status_code in (200, 404, 422):
        # Invalid state: 200 with empty data, 404, or 422 are all acceptable
        passed = True
    elif status_code in (200, 201):
        passed = True
    elif status_code == 422:
        # Validation error -- usually means missing required params
        passed = False
        error = "VALIDATION_ERROR (missing required params?)"
    else:
        passed = False
        error = f"UNEXPECTED_STATUS_{status_code}"

    result = TestResult(
        endpoint=ep.path,
        method=ep.method,
        variant=variant,
        url=url,
        status_code=status_code,
        response_time_ms=round(elapsed_ms, 1),
        passed=passed,
        error=error,
        row_count=row_count,
    )

    if VERBOSE:
        icon = "PASS" if passed else "FAIL"
        print(f"  [{icon}] {ep.method} {url} -> {status_code} ({elapsed_ms:.0f}ms)"
              + (f" [{row_count} rows]" if row_count is not None else "")
              + (f" ERROR: {error}" if error else ""))

    return result


def main():
    print("=" * 72)
    print("ARADUNE API SMOKE TEST")
    print(f"  Target:      {BASE_URL}")
    print(f"  Timeout:     {TIMEOUT}s per request")
    print(f"  Concurrency: {CONCURRENCY}")
    print(f"  Skip POST:   {SKIP_POST}")
    print(f"  Started:     {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 72)

    # Step 1: Check health
    print("\n[1/4] Checking server health...")
    try:
        resp = requests.get(f"{BASE_URL}/health", timeout=5)
        health = resp.json()
        print(f"  Server: {resp.status_code} -- lake_ready={health.get('lake_ready', '?')}")
        if not health.get("lake_ready"):
            print("  WARNING: Lake not ready. Some endpoints may fail.")
    except requests.exceptions.ConnectionError:
        print(f"  ERROR: Cannot connect to {BASE_URL}")
        print("  Make sure the server is running: cd server && uvicorn server.main:app")
        sys.exit(1)
    except Exception as e:
        print(f"  WARNING: Health check failed: {e}")

    # Step 2: Discover endpoints
    print("\n[2/4] Discovering endpoints from route files...")
    endpoints = discover_endpoints()
    print(f"  Found {len(endpoints)} endpoints across {len(set(e.source_file for e in endpoints))} route files")

    get_count = sum(1 for e in endpoints if e.method == "GET")
    post_count = sum(1 for e in endpoints if e.method == "POST")
    state_count = sum(1 for e in endpoints if e.has_state_param)
    skipped = sum(1 for e in endpoints if e.path in SKIP_ENDPOINTS)
    print(f"  GET: {get_count}  POST: {post_count}  State-parameterized: {state_count}  Skipped: {skipped}")

    # Step 3: Build test matrix
    print("\n[3/4] Running tests...")
    test_cases = []
    for ep in endpoints:
        if ep.path in SKIP_ENDPOINTS:
            continue

        if ep.has_state_param:
            # Test with FL, DC, XX (skip base since it requires state)
            for variant in [TEST_STATE, TEST_TERRITORY, TEST_INVALID]:
                test_cases.append((ep, variant))
        else:
            # Test base (no state param)
            test_cases.append((ep, "base"))

    print(f"  Total test cases: {len(test_cases)}")

    results: list[TestResult] = []
    with ThreadPoolExecutor(max_workers=CONCURRENCY) as pool:
        futures = {pool.submit(run_test, ep, v): (ep, v) for ep, v in test_cases}
        done_count = 0
        for future in as_completed(futures):
            done_count += 1
            result = future.result()
            if result:
                results.append(result)
            if not VERBOSE and done_count % 20 == 0:
                print(f"  ... {done_count}/{len(test_cases)} tests completed")

    if not VERBOSE:
        print(f"  ... {len(test_cases)}/{len(test_cases)} tests completed")

    # Step 4: Report
    print("\n[4/4] Results")
    print("=" * 72)

    total = len(results)
    passed = sum(1 for r in results if r.passed)
    failed = sum(1 for r in results if not r.passed)
    errors_500 = sum(1 for r in results if r.status_code == 500)
    conn_errors = sum(1 for r in results if r.error == "CONNECTION_REFUSED")
    timeouts = sum(1 for r in results if r.error == "TIMEOUT")
    avg_time = sum(r.response_time_ms for r in results) / total if total else 0
    max_time = max((r.response_time_ms for r in results), default=0)

    print(f"\n  Total tests:   {total}")
    print(f"  Passed:        {passed} ({100*passed/total:.1f}%)" if total else "  Passed: 0")
    print(f"  Failed:        {failed}")
    print(f"  500 errors:    {errors_500}")
    print(f"  Conn refused:  {conn_errors}")
    print(f"  Timeouts:      {timeouts}")
    print(f"  Avg latency:   {avg_time:.0f}ms")
    print(f"  Max latency:   {max_time:.0f}ms")

    # Group failures
    if failed > 0:
        print("\n  FAILURES:")
        print("  " + "-" * 68)

        # Sort failures: 500s first, then by endpoint
        fail_results = sorted(
            [r for r in results if not r.passed],
            key=lambda r: (0 if r.status_code == 500 else 1, r.endpoint, r.variant),
        )
        for r in fail_results:
            print(f"  {r.method:4s} {r.endpoint:55s} [{r.variant:4s}] "
                  f"-> {r.status_code:3d}  {r.error or ''}")

    # Slow endpoints (>2s)
    slow = sorted([r for r in results if r.response_time_ms > 2000], key=lambda r: -r.response_time_ms)
    if slow:
        print(f"\n  SLOW ENDPOINTS (>{2}s):")
        print("  " + "-" * 68)
        for r in slow[:15]:
            print(f"  {r.method:4s} {r.endpoint:55s} [{r.variant:4s}] "
                  f"-> {r.response_time_ms:.0f}ms")

    # Empty responses for FL (might indicate broken data pipeline)
    empty_fl = [r for r in results if r.variant == TEST_STATE and r.row_count == 0 and r.passed]
    if empty_fl:
        print(f"\n  EMPTY RESPONSES FOR {TEST_STATE} (possible data gaps):")
        print("  " + "-" * 68)
        for r in sorted(empty_fl, key=lambda r: r.endpoint):
            print(f"  {r.method:4s} {r.endpoint}")

    # Save results to JSON
    output = {
        "meta": {
            "base_url": BASE_URL,
            "timestamp": datetime.now().isoformat(),
            "total": total,
            "passed": passed,
            "failed": failed,
            "errors_500": errors_500,
            "avg_latency_ms": round(avg_time, 1),
            "max_latency_ms": round(max_time, 1),
        },
        "results": [asdict(r) for r in sorted(results, key=lambda r: (r.endpoint, r.variant))],
    }

    RESULTS_FILE.write_text(json.dumps(output, indent=2))
    print(f"\n  Results saved to: {RESULTS_FILE}")
    print("=" * 72)

    # Exit code: 0 if all pass, 1 if any fail
    sys.exit(0 if failed == 0 else 1)


if __name__ == "__main__":
    main()
