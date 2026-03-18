# Aradune Pre-Meeting Implementation Guide
## Priority fixes before Big 5 consulting firm demo (~April 2026)

> **Context for Claude Code:** This document describes the current state of Aradune, a Medicaid intelligence platform built on DuckDB + FastAPI + React/TypeScript + Claude API. The codebase lives in a single repo with `server/` (Python FastAPI backend), `src/` (React frontend), `scripts/` (ETL), `ontology/` (YAML entity registry), and `data/lake/` (Hive-partitioned Parquet). The owner is preparing for a high-stakes demo with a major consulting firm and needs specific fixes implemented. Work through these in the priority order listed. Ask clarifying questions if the codebase structure differs from what's described here.

---

## Priority 1: Replace Password Gate with Clerk Auth

### Current State
- `server/middleware/auth.py` (~100 lines) implements two auth modes
- Clerk integration exists but requires `CLERK_SECRET_KEY` env var to activate
- Fallback is a hardcoded password "mediquiad" checked via `X-Password` header
- Frontend `Platform.tsx` (~980 lines) contains a `PasswordGate` component that renders a password input field
- Clerk keys are not yet set: `VITE_CLERK_PUBLISHABLE_KEY` (Vercel) and `CLERK_SECRET_KEY` (Fly.io) need values

### What to Do

**Step 1: Verify Clerk scaffolding exists.** Look for:
- `server/middleware/auth.py` — find the Clerk JWT validation branch
- `Platform.tsx` — find any existing `ClerkProvider` or Clerk import
- `package.json` — check if `@clerk/clerk-react` is already a dependency

**Step 2: If `@clerk/clerk-react` is not installed:**
```bash
npm install @clerk/clerk-react
```

**Step 3: Update the frontend auth wrapper in `Platform.tsx`.**

Replace the `PasswordGate` component with Clerk's `SignIn` component. The structure should be:

```tsx
import { ClerkProvider, SignedIn, SignedOut, SignIn, UserButton } from '@clerk/clerk-react';

const CLERK_KEY = import.meta.env.VITE_CLERK_PUBLISHABLE_KEY;

// If no Clerk key is configured, fall back to open access (local dev only)
function AuthGate({ children }: { children: React.ReactNode }) {
  if (!CLERK_KEY) {
    return <>{children}</>;
  }
  return (
    <ClerkProvider publishableKey={CLERK_KEY}>
      <SignedIn>{children}</SignedIn>
      <SignedOut>
        <div style={{
          display: 'flex',
          justifyContent: 'center',
          alignItems: 'center',
          minHeight: '100vh',
          background: '#F5F7F5'
        }}>
          <SignIn routing="hash" />
        </div>
      </SignedOut>
    </ClerkProvider>
  );
}
```

- Remove the entire `PasswordGate` component and all references to "mediquiad"
- Remove any `X-Password` header logic from `apiFetch` or API utility functions
- Add `<UserButton />` in the nav bar (top-right) so authenticated users can see their session

**Step 4: Update backend auth middleware (`server/middleware/auth.py`).**

- Keep the Clerk JWT validation path as the primary auth
- Replace the password fallback with a simple "no auth in dev mode" check:

```python
import os

CLERK_SECRET = os.getenv("CLERK_SECRET_KEY")
DEV_MODE = os.getenv("ARADUNE_ENV", "development") == "development"

async def verify_auth(request):
    if CLERK_SECRET:
        # Validate Clerk JWT from Authorization header
        # (existing Clerk validation logic)
        pass
    elif DEV_MODE:
        # Local development: no auth required
        return {"user_id": "dev", "email": "dev@localhost"}
    else:
        raise HTTPException(401, "Authentication required")
```

- Remove all references to "mediquiad" and the `X-Password` header check
- Remove the hardcoded password string entirely from the codebase

**Step 5: Grep the entire codebase for "mediquiad" and remove every instance.**
```bash
grep -r "mediquiad" --include="*.py" --include="*.ts" --include="*.tsx" --include="*.js"
```

**Step 6: Add placeholder env vars.**
- In `.env.example` or equivalent, add:
  ```
  VITE_CLERK_PUBLISHABLE_KEY=pk_test_... 
  CLERK_SECRET_KEY=sk_test_...
  ```
- Add a comment noting that the owner needs to create a Clerk app at clerk.com and set these values on Vercel and Fly.io

### Acceptance Criteria
- [ ] No instance of "mediquiad" exists anywhere in the codebase
- [ ] No `X-Password` header logic exists anywhere
- [ ] With `VITE_CLERK_PUBLISHABLE_KEY` set, the app shows Clerk's SignIn component
- [ ] Without the key set (local dev), the app loads without any auth gate
- [ ] `UserButton` renders in the nav bar when authenticated
- [ ] Backend rejects unauthenticated requests when `CLERK_SECRET_KEY` is set

---

## Priority 2: Fix 500 Errors on Edge Cases

### Current State
- `server/routes/pharmacy.py`, `server/routes/enrollment.py`, and `server/routes/wages.py` return 500 errors on certain edge cases (specific states, missing data, null values)
- Other route files may have similar issues
- These are unhandled exceptions that crash the endpoint instead of returning clean error responses

### What to Do

**Step 1: Audit all route files for unprotected endpoints.**

There are 25 route files in `server/routes/`. For each file:
1. Check every endpoint function
2. Verify it has try/except wrapping
3. Verify it handles None/empty results from DuckDB queries gracefully

**Step 2: Create a shared error handler utility.**

Create `server/utils/error_handler.py`:

```python
"""
Shared error handling for all Aradune API routes.
Returns clean JSON error responses instead of 500 crashes.
"""

from fastapi.responses import JSONResponse
from functools import wraps
import logging
import traceback

logger = logging.getLogger("aradune.api")


def safe_route(default_response=None):
    """
    Decorator for route handlers. Catches all exceptions and returns
    a clean JSON response instead of a 500 error.
    
    Usage:
        @router.get("/api/pharmacy/summary")
        @safe_route(default_response={"states": [], "total": 0})
        async def pharmacy_summary(state: str = None):
            ...
    """
    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            try:
                result = await func(*args, **kwargs)
                if result is None:
                    return default_response or {"data": [], "message": "No data available"}
                return result
            except Exception as e:
                logger.error(f"{func.__name__} failed: {e}\n{traceback.format_exc()}")
                return JSONResponse(
                    status_code=200,  # 200 with error payload, not 500
                    content={
                        "data": [] if default_response is None else default_response,
                        "error": str(e),
                        "message": f"Data unavailable: {str(e)[:200]}"
                    }
                )
        return wrapper
    return decorator


def safe_query(conn, sql, params=None, fallback=None):
    """
    Execute a DuckDB query with error handling.
    Returns fallback value (default empty list) on any failure.
    """
    try:
        if params:
            result = conn.execute(sql, params).fetchdf()
        else:
            result = conn.execute(sql).fetchdf()
        if result.empty:
            return fallback if fallback is not None else []
        return result.to_dict(orient="records")
    except Exception as e:
        logger.warning(f"Query failed: {e}\nSQL: {sql[:200]}")
        return fallback if fallback is not None else []
```

**Step 3: Apply `@safe_route` to the three known problem files first:**
- `server/routes/pharmacy.py` (4 endpoints)
- `server/routes/enrollment.py` (6 endpoints)
- `server/routes/wages.py` (6 endpoints)

Then apply to all remaining route files. Every endpoint in the application should be wrapped.

**Step 4: Fix known null/empty patterns.**

Common failure patterns to check for in each route file:
- DuckDB query returns empty DataFrame, then code tries to access `.iloc[0]` or index into results
- State code parameter is None or invalid, passed directly into SQL without validation
- Division by zero in computed fields (enrollment = 0, rate = 0)
- String formatting on None values (f-string with None becomes "None" in SQL)

For each, add defensive checks:

```python
# Before any DuckDB query that filters by state:
if state and len(state) == 2 and state.isalpha():
    state = state.upper()
else:
    return {"data": [], "message": "Invalid state code"}

# Before any DataFrame index access:
if df.empty:
    return {"data": [], "message": f"No data available for {state}"}

# Before any division:
denominator = row.get("enrollment", 0)
result = numerator / denominator if denominator else None
```

**Step 5: Smoke test all 258+ endpoints.**

Write a quick test script that hits every registered endpoint with:
1. No parameters (should return empty data, not 500)
2. A valid state (FL)
3. An edge-case state (DC, PR, VI, GU, AS, MP — territories that may have sparse data)
4. An invalid state ("XX", "", None)

```python
"""Quick smoke test for all Aradune endpoints."""
import requests

BASE = "http://localhost:8000"
STATES = ["FL", "DC", "XX", ""]

# Collect all routes from the FastAPI app
# Or maintain a manual list from the 25 route files
ENDPOINTS = [
    "/api/pharmacy/summary",
    "/api/pharmacy/top-drugs",
    "/api/enrollment/monthly",
    "/api/wages/state",
    # ... add all 258+ endpoints
]

for endpoint in ENDPOINTS:
    for state in STATES:
        url = f"{BASE}{endpoint}"
        params = {"state": state} if state else {}
        try:
            r = requests.get(url, params=params, timeout=10)
            if r.status_code >= 500:
                print(f"FAIL [{r.status_code}] {endpoint} state={state}")
            else:
                print(f"OK   [{r.status_code}] {endpoint} state={state}")
        except Exception as e:
            print(f"ERR  {endpoint} state={state}: {e}")
```

Save this as `scripts/smoke_test_endpoints.py`. Run it. Fix every 500.

### Acceptance Criteria
- [ ] `@safe_route` decorator exists and is applied to all endpoints
- [ ] `safe_query` helper exists and is used for DuckDB calls
- [ ] Smoke test script exists at `scripts/smoke_test_endpoints.py`
- [ ] Zero 500 errors when smoke test runs against FL, DC, XX, and empty state
- [ ] All error responses return 200 with `{"data": [], "error": "...", "message": "..."}` structure

---

## Priority 3: Data Validation Layer (Layer 1)

### Current State
- 115+ ETL scripts in `scripts/` each have inline validation (schema checks, range validation, null handling)
- Hard stops and soft flags exist per the build doc but are implemented ad hoc in each script
- No centralized validation framework
- Soda Core, dbt, Pandera, and datacontract-cli are listed as "Planned" in the build doc
- The Dagster pipeline (`pipeline/dagster_pipeline.py`) has 13 assets, 3 checks, 3 jobs, 2 schedules

### What to Do

**We are NOT implementing the full Soda/dbt/Pandera stack before the meeting.** That's a multi-month effort. Instead, we're building a lightweight but real validation layer that:
1. Runs automatically after any ETL script
2. Produces a validation report stored in the lake
3. Surfaces validation status in the API and frontend
4. Gives the consulting firm something concrete to evaluate

**Step 1: Create the validation engine.**

Create `server/engines/validator.py`:

```python
"""
Aradune Data Validation Engine (Layer 1)
Centralized validation checks that run against the DuckDB lake.

This is the lightweight implementation that replaces ad-hoc inline validation
with a structured, auditable framework. Each check produces a pass/fail result
stored in fact_validation_results.

Future: migrate checks to Soda Core SodaCL format for full framework support.
"""

import duckdb
import uuid
import logging
from datetime import datetime
from typing import Optional
from pathlib import Path

logger = logging.getLogger("aradune.validator")


# ---------------------------------------------------------------------------
# Validation result storage
# ---------------------------------------------------------------------------

VALIDATION_SCHEMA = """
CREATE TABLE IF NOT EXISTS fact_validation_results (
    run_id          VARCHAR,
    check_id        VARCHAR,
    table_name      VARCHAR NOT NULL,
    domain          VARCHAR,
    check_type      VARCHAR NOT NULL,
    check_name      VARCHAR NOT NULL,
    description     VARCHAR,
    severity        VARCHAR DEFAULT 'error',
    passed          BOOLEAN NOT NULL,
    actual_value    VARCHAR,
    expected_value  VARCHAR,
    detail          VARCHAR,
    run_timestamp   TIMESTAMP DEFAULT current_timestamp
);

CREATE TABLE IF NOT EXISTS fact_validation_checks (
    check_id        VARCHAR PRIMARY KEY,
    table_name      VARCHAR NOT NULL,
    domain          VARCHAR,
    check_type      VARCHAR NOT NULL,
    check_name      VARCHAR NOT NULL,
    description     VARCHAR,
    severity        VARCHAR DEFAULT 'error',
    sql_check       VARCHAR,
    expected         VARCHAR,
    active          BOOLEAN DEFAULT true,
    created_at      TIMESTAMP DEFAULT current_timestamp
);
"""


def init_validation_tables(conn):
    """Create validation tables if they don't exist."""
    for stmt in VALIDATION_SCHEMA.strip().split(";"):
        stmt = stmt.strip()
        if stmt:
            conn.execute(stmt)


# ---------------------------------------------------------------------------
# Core check types
# ---------------------------------------------------------------------------

class ValidationCheck:
    """Single validation check against a DuckDB table."""
    
    def __init__(self, check_id, table_name, domain, check_type, 
                 check_name, description, severity="error"):
        self.check_id = check_id
        self.table_name = table_name
        self.domain = domain
        self.check_type = check_type
        self.check_name = check_name
        self.description = description
        self.severity = severity
    
    def run(self, conn) -> dict:
        raise NotImplementedError


class RowCountCheck(ValidationCheck):
    """Table has at least min_rows rows."""
    
    def __init__(self, table_name, domain, min_rows, **kwargs):
        super().__init__(
            check_id=f"rc_{table_name}",
            table_name=table_name,
            domain=domain,
            check_type="row_count",
            check_name=f"{table_name} row count >= {min_rows}",
            description=f"Verify {table_name} has at least {min_rows} rows",
            **kwargs
        )
        self.min_rows = min_rows
    
    def run(self, conn) -> dict:
        try:
            count = conn.execute(
                f"SELECT COUNT(*) FROM {self.table_name}"
            ).fetchone()[0]
            return {
                "passed": count >= self.min_rows,
                "actual_value": str(count),
                "expected_value": f">= {self.min_rows}",
                "detail": f"{count} rows found"
            }
        except Exception as e:
            return {
                "passed": False,
                "actual_value": "ERROR",
                "expected_value": f">= {self.min_rows}",
                "detail": str(e)[:500]
            }


class NotNullCheck(ValidationCheck):
    """Critical column has no NULL values."""
    
    def __init__(self, table_name, domain, column, **kwargs):
        super().__init__(
            check_id=f"nn_{table_name}_{column}",
            table_name=table_name,
            domain=domain,
            check_type="not_null",
            check_name=f"{table_name}.{column} not null",
            description=f"Verify {column} has no NULL values in {table_name}",
            **kwargs
        )
        self.column = column
    
    def run(self, conn) -> dict:
        try:
            result = conn.execute(f"""
                SELECT 
                    COUNT(*) as total,
                    COUNT(*) FILTER (WHERE {self.column} IS NULL) as null_count
                FROM {self.table_name}
            """).fetchone()
            total, null_count = result
            return {
                "passed": null_count == 0,
                "actual_value": f"{null_count} nulls / {total} rows",
                "expected_value": "0 nulls",
                "detail": f"{null_count / total * 100:.1f}% null" if total > 0 else "empty table"
            }
        except Exception as e:
            return {
                "passed": False,
                "actual_value": "ERROR",
                "expected_value": "0 nulls",
                "detail": str(e)[:500]
            }


class RangeCheck(ValidationCheck):
    """Numeric column falls within expected bounds."""
    
    def __init__(self, table_name, domain, column, 
                 min_val=None, max_val=None, **kwargs):
        super().__init__(
            check_id=f"rng_{table_name}_{column}",
            table_name=table_name,
            domain=domain,
            check_type="range",
            check_name=f"{table_name}.{column} in range [{min_val}, {max_val}]",
            description=f"Verify {column} values are within [{min_val}, {max_val}]",
            **kwargs
        )
        self.column = column
        self.min_val = min_val
        self.max_val = max_val
    
    def run(self, conn) -> dict:
        try:
            conditions = []
            if self.min_val is not None:
                conditions.append(f"{self.column} < {self.min_val}")
            if self.max_val is not None:
                conditions.append(f"{self.column} > {self.max_val}")
            where = " OR ".join(conditions) if conditions else "FALSE"
            
            result = conn.execute(f"""
                SELECT 
                    COUNT(*) as total,
                    COUNT(*) FILTER (WHERE {where}) as violations,
                    MIN({self.column}) as actual_min,
                    MAX({self.column}) as actual_max
                FROM {self.table_name}
                WHERE {self.column} IS NOT NULL
            """).fetchone()
            total, violations, actual_min, actual_max = result
            return {
                "passed": violations == 0,
                "actual_value": f"range [{actual_min}, {actual_max}], {violations} violations",
                "expected_value": f"[{self.min_val}, {self.max_val}]",
                "detail": f"{violations} / {total} rows out of range"
            }
        except Exception as e:
            return {
                "passed": False,
                "actual_value": "ERROR",
                "expected_value": f"[{self.min_val}, {self.max_val}]",
                "detail": str(e)[:500]
            }


class ReferentialIntegrityCheck(ValidationCheck):
    """Foreign key values exist in reference table."""
    
    def __init__(self, table_name, domain, column,
                 ref_table, ref_column, **kwargs):
        super().__init__(
            check_id=f"ri_{table_name}_{column}_{ref_table}",
            table_name=table_name,
            domain=domain,
            check_type="referential_integrity",
            check_name=f"{table_name}.{column} -> {ref_table}.{ref_column}",
            description=f"Verify all {column} values in {table_name} exist in {ref_table}.{ref_column}",
            **kwargs
        )
        self.column = column
        self.ref_table = ref_table
        self.ref_column = ref_column
    
    def run(self, conn) -> dict:
        try:
            result = conn.execute(f"""
                SELECT COUNT(DISTINCT t.{self.column}) as orphans
                FROM {self.table_name} t
                LEFT JOIN {self.ref_table} r 
                    ON t.{self.column} = r.{self.ref_column}
                WHERE t.{self.column} IS NOT NULL 
                    AND r.{self.ref_column} IS NULL
            """).fetchone()
            orphans = result[0]
            return {
                "passed": orphans == 0,
                "actual_value": f"{orphans} orphaned values",
                "expected_value": "0 orphans",
                "detail": f"{orphans} values in {self.table_name}.{self.column} not found in {self.ref_table}"
            }
        except Exception as e:
            return {
                "passed": False,
                "actual_value": "ERROR",
                "expected_value": "0 orphans",
                "detail": str(e)[:500]
            }


class UniqueCheck(ValidationCheck):
    """Column or column combination has no duplicates."""
    
    def __init__(self, table_name, domain, columns, **kwargs):
        cols = columns if isinstance(columns, list) else [columns]
        col_str = ", ".join(cols)
        super().__init__(
            check_id=f"uq_{table_name}_{'_'.join(cols)}",
            table_name=table_name,
            domain=domain,
            check_type="unique",
            check_name=f"{table_name} unique on ({col_str})",
            description=f"Verify no duplicate ({col_str}) combinations in {table_name}",
            **kwargs
        )
        self.columns = cols
    
    def run(self, conn) -> dict:
        try:
            col_str = ", ".join(self.columns)
            result = conn.execute(f"""
                SELECT COUNT(*) - COUNT(DISTINCT ({col_str})) as dupes
                FROM {self.table_name}
            """).fetchone()
            dupes = result[0]
            return {
                "passed": dupes == 0,
                "actual_value": f"{dupes} duplicates",
                "expected_value": "0 duplicates",
                "detail": f"{dupes} duplicate ({col_str}) combinations"
            }
        except Exception as e:
            return {
                "passed": False,
                "actual_value": "ERROR",
                "expected_value": "0 duplicates",
                "detail": str(e)[:500]
            }


class CrossSourceCheck(ValidationCheck):
    """Compare a metric across two tables that should agree."""
    
    def __init__(self, table_name, domain, check_name, description,
                 sql_query, tolerance_pct=5.0, **kwargs):
        super().__init__(
            check_id=f"xs_{table_name}_{check_name.replace(' ', '_')[:30]}",
            table_name=table_name,
            domain=domain,
            check_type="cross_source",
            check_name=check_name,
            description=description,
            **kwargs
        )
        self.sql_query = sql_query
        self.tolerance_pct = tolerance_pct
    
    def run(self, conn) -> dict:
        try:
            result = conn.execute(self.sql_query).fetchone()
            source_a, source_b = float(result[0]), float(result[1])
            if source_b == 0:
                pct_diff = 0 if source_a == 0 else 100
            else:
                pct_diff = abs(source_a - source_b) / source_b * 100
            return {
                "passed": pct_diff <= self.tolerance_pct,
                "actual_value": f"A={source_a:,.0f}, B={source_b:,.0f}, diff={pct_diff:.1f}%",
                "expected_value": f"within {self.tolerance_pct}%",
                "detail": f"Cross-source divergence: {pct_diff:.1f}%"
            }
        except Exception as e:
            return {
                "passed": False,
                "actual_value": "ERROR",
                "expected_value": f"within {self.tolerance_pct}%",
                "detail": str(e)[:500]
            }


# ---------------------------------------------------------------------------
# Validation runner
# ---------------------------------------------------------------------------

def run_validation_suite(conn, checks: list[ValidationCheck], 
                          run_id: str = None) -> dict:
    """
    Run a list of validation checks and store results.
    Returns summary with pass/fail counts.
    """
    init_validation_tables(conn)
    
    run_id = run_id or str(uuid.uuid4())[:12]
    timestamp = datetime.now().isoformat()
    results = []
    passed = 0
    failed = 0
    errors = 0
    
    for check in checks:
        try:
            outcome = check.run(conn)
            if outcome["passed"]:
                passed += 1
            else:
                failed += 1
            
            conn.execute("""
                INSERT INTO fact_validation_results
                (run_id, check_id, table_name, domain, check_type,
                 check_name, description, severity, passed,
                 actual_value, expected_value, detail, run_timestamp)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, [
                run_id, check.check_id, check.table_name, check.domain,
                check.check_type, check.check_name, check.description,
                check.severity, outcome["passed"], outcome.get("actual_value"),
                outcome.get("expected_value"), outcome.get("detail"),
                timestamp
            ])
            
            results.append({
                "check_id": check.check_id,
                "check_name": check.check_name,
                "severity": check.severity,
                **outcome
            })
        except Exception as e:
            errors += 1
            logger.error(f"Check {check.check_id} crashed: {e}")
            results.append({
                "check_id": check.check_id,
                "check_name": check.check_name,
                "severity": check.severity,
                "passed": False,
                "actual_value": "CRASH",
                "detail": str(e)[:500]
            })
    
    return {
        "run_id": run_id,
        "timestamp": timestamp,
        "total": len(checks),
        "passed": passed,
        "failed": failed,
        "errors": errors,
        "pass_rate": f"{passed / len(checks) * 100:.1f}%" if checks else "N/A",
        "results": results
    }
```

**Step 2: Define the core validation checks.**

Create `scripts/validation_checks.py`:

```python
"""
Aradune Validation Check Definitions
All checks that run against the data lake.

Organized by domain, matching ontology/domains/*.yaml structure.
Add new checks here when adding new datasets.

Run with: python scripts/run_validation.py
"""

from server.engines.validator import (
    RowCountCheck, NotNullCheck, RangeCheck,
    ReferentialIntegrityCheck, UniqueCheck, CrossSourceCheck
)


def get_all_checks() -> list:
    """Return all validation checks organized by domain."""
    checks = []
    
    # ===================================================================
    # RATES & FEE SCHEDULES
    # ===================================================================
    
    # Row counts (known minimums from build doc)
    checks.append(RowCountCheck(
        "fact_medicaid_rate", "rates", min_rows=500000,
        severity="error"
    ))
    checks.append(RowCountCheck(
        "fact_rate_comparison_v2", "rates", min_rows=400000,
        severity="error"
    ))
    checks.append(RowCountCheck(
        "dim_procedure", "rates", min_rows=15000,
        severity="error"
    ))
    
    # No null state codes in rate tables
    checks.append(NotNullCheck(
        "fact_medicaid_rate", "rates", "state_code"
    ))
    checks.append(NotNullCheck(
        "fact_rate_comparison_v2", "rates", "state_code"
    ))
    
    # Rate values are plausible (not $0, not >$50,000)
    checks.append(RangeCheck(
        "fact_medicaid_rate", "rates", "rate",
        min_val=0.01, max_val=50000,
        severity="warning"
    ))
    
    # State codes reference dim_state
    checks.append(ReferentialIntegrityCheck(
        "fact_medicaid_rate", "rates", "state_code",
        "dim_state", "state_code"
    ))
    checks.append(ReferentialIntegrityCheck(
        "fact_rate_comparison_v2", "rates", "state_code",
        "dim_state", "state_code"
    ))
    
    # Procedure codes reference dim_procedure
    checks.append(ReferentialIntegrityCheck(
        "fact_rate_comparison_v2", "rates", "cpt_hcpcs_code",
        "dim_procedure", "cpt_hcpcs_code",
        severity="warning"  # some state codes may not be in Medicare PFS
    ))
    
    # ===================================================================
    # ENROLLMENT
    # ===================================================================
    
    checks.append(RowCountCheck(
        "fact_enrollment", "enrollment", min_rows=5000
    ))
    checks.append(NotNullCheck(
        "fact_enrollment", "enrollment", "state_code"
    ))
    checks.append(RangeCheck(
        "fact_enrollment", "enrollment", "total_enrollment",
        min_val=0, max_val=20000000,  # no state > 20M
        severity="warning"
    ))
    checks.append(ReferentialIntegrityCheck(
        "fact_enrollment", "enrollment", "state_code",
        "dim_state", "state_code"
    ))
    
    # ===================================================================
    # EXPENDITURE & FISCAL
    # ===================================================================
    
    checks.append(RowCountCheck(
        "fact_cms64_multiyear", "expenditure", min_rows=100000
    ))
    checks.append(NotNullCheck(
        "fact_cms64_multiyear", "expenditure", "state_code"
    ))
    checks.append(RowCountCheck(
        "fact_fmap_historical", "expenditure", min_rows=600
    ))
    checks.append(RangeCheck(
        "fact_fmap_historical", "expenditure", "fmap_rate",
        min_val=0.50, max_val=0.90,  # FMAP ranges from ~50% to ~83%
        severity="warning"
    ))
    
    # Cross-source: enrollment totals should roughly agree
    # between fact_enrollment and fact_mc_enrollment_summary
    checks.append(CrossSourceCheck(
        "fact_enrollment", "enrollment",
        check_name="enrollment_vs_mc_enrollment_total",
        description="Total enrollment should roughly match MC enrollment summary totals",
        sql_query="""
            SELECT 
                (SELECT SUM(total_enrollment) FROM fact_enrollment 
                 WHERE state_code != 'US' 
                 AND total_enrollment IS NOT NULL) as enrollment_total,
                (SELECT SUM(total_enrollment) FROM fact_mc_enrollment_summary
                 WHERE total_enrollment IS NOT NULL) as mc_summary_total
        """,
        tolerance_pct=20  # loose tolerance, different time periods
    ))
    
    # ===================================================================
    # HOSPITALS
    # ===================================================================
    
    checks.append(RowCountCheck(
        "fact_hospital_cost", "hospitals", min_rows=15000
    ))
    checks.append(NotNullCheck(
        "fact_hospital_cost", "hospitals", "provider_ccn",
        severity="error"
    ))
    checks.append(RowCountCheck(
        "fact_five_star", "nursing", min_rows=14000
    ))
    
    # ===================================================================
    # PHARMACY
    # ===================================================================
    
    checks.append(RowCountCheck(
        "fact_sdud_combined", "pharmacy", min_rows=25000000
    ))
    checks.append(RowCountCheck(
        "fact_nadac", "pharmacy", min_rows=1500000
    ))
    checks.append(NotNullCheck(
        "fact_sdud_combined", "pharmacy", "state_code"
    ))
    
    # ===================================================================
    # QUALITY
    # ===================================================================
    
    checks.append(RowCountCheck(
        "fact_quality_core_set_2024", "quality", min_rows=10000
    ))
    checks.append(NotNullCheck(
        "fact_quality_core_set_2024", "quality", "state_code"
    ))
    checks.append(NotNullCheck(
        "fact_quality_core_set_2024", "quality", "measure_id"
    ))
    
    # ===================================================================
    # WORKFORCE & ACCESS
    # ===================================================================
    
    checks.append(RowCountCheck(
        "fact_hpsa", "workforce", min_rows=60000
    ))
    checks.append(RowCountCheck(
        "fact_bls_wage", "workforce", min_rows=700
    ))
    
    # ===================================================================
    # PROVIDER NETWORK
    # ===================================================================
    
    checks.append(RowCountCheck(
        "fact_nppes_provider", "provider_network", min_rows=9000000
    ))
    
    # ===================================================================
    # BEHAVIORAL HEALTH
    # ===================================================================
    
    checks.append(RowCountCheck(
        "fact_nsduh_prevalence", "behavioral_health", min_rows=5000
    ))
    checks.append(RowCountCheck(
        "fact_teds_admissions", "behavioral_health", min_rows=1000000
    ))
    
    # ===================================================================
    # DIMENSION TABLE INTEGRITY
    # ===================================================================
    
    checks.append(RowCountCheck(
        "dim_state", "reference", min_rows=51
    ))
    checks.append(UniqueCheck(
        "dim_state", "reference", "state_code"
    ))
    checks.append(RowCountCheck(
        "dim_procedure", "reference", min_rows=15000
    ))
    checks.append(UniqueCheck(
        "dim_procedure", "reference", "cpt_hcpcs_code"
    ))
    
    # ===================================================================
    # POLICY CORPUS (RAG)
    # ===================================================================
    
    checks.append(RowCountCheck(
        "fact_policy_document", "policy", min_rows=1000
    ))
    checks.append(RowCountCheck(
        "fact_policy_chunk", "policy", min_rows=6000
    ))
    
    return checks
```

**Step 3: Create the validation runner script.**

Create `scripts/run_validation.py`:

```python
"""
Run Aradune's full validation suite against the data lake.

Usage:
    python scripts/run_validation.py                    # Run all checks
    python scripts/run_validation.py --domain rates     # Run one domain
    python scripts/run_validation.py --summary          # Summary only
    python scripts/run_validation.py --export report.md # Export markdown
"""

import argparse
import sys
import os

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from server.db import init_db, get_connection
from server.engines.validator import run_validation_suite
from scripts.validation_checks import get_all_checks


def main():
    parser = argparse.ArgumentParser(description="Run Aradune validation suite")
    parser.add_argument("--domain", help="Run checks for specific domain only")
    parser.add_argument("--summary", action="store_true", help="Summary output only")
    parser.add_argument("--export", help="Export results to markdown file")
    args = parser.parse_args()
    
    # Initialize DuckDB with lake views
    init_db()
    conn = get_connection()
    
    # Get checks, optionally filtered by domain
    checks = get_all_checks()
    if args.domain:
        checks = [c for c in checks if c.domain == args.domain]
        print(f"Running {len(checks)} checks for domain: {args.domain}")
    else:
        print(f"Running {len(checks)} checks across all domains")
    
    # Run
    report = run_validation_suite(conn, checks)
    
    # Print summary
    print(f"\n{'=' * 60}")
    print(f"Validation Report: {report['run_id']}")
    print(f"{'=' * 60}")
    print(f"Timestamp: {report['timestamp']}")
    print(f"Total checks: {report['total']}")
    print(f"Passed: {report['passed']} ({report['pass_rate']})")
    print(f"Failed: {report['failed']}")
    print(f"Errors: {report['errors']}")
    print(f"{'=' * 60}\n")
    
    if not args.summary:
        # Print failures
        failures = [r for r in report['results'] if not r['passed']]
        if failures:
            print("FAILURES:\n")
            for f in failures:
                icon = "!!" if f['severity'] == 'error' else "W "
                print(f"  [{icon}] {f['check_name']}")
                print(f"       Expected: {f.get('expected_value', 'N/A')}")
                print(f"       Actual:   {f.get('actual_value', 'N/A')}")
                print(f"       Detail:   {f.get('detail', 'N/A')}")
                print()
        else:
            print("All checks passed.\n")
    
    # Export markdown
    if args.export:
        with open(args.export, "w") as f:
            f.write(f"# Aradune Validation Report\n\n")
            f.write(f"**Run ID:** {report['run_id']}  \n")
            f.write(f"**Timestamp:** {report['timestamp']}  \n")
            f.write(f"**Pass rate:** {report['pass_rate']}  \n\n")
            f.write(f"| Check | Domain | Severity | Status | Detail |\n")
            f.write(f"|-------|--------|----------|--------|--------|\n")
            for r in report['results']:
                status = "PASS" if r['passed'] else "FAIL"
                f.write(f"| {r['check_name'][:50]} | {r.get('domain', '')} | "
                        f"{r['severity']} | {status} | {r.get('detail', '')[:60]} |\n")
            f.write(f"\n**Total:** {report['total']} checks, "
                    f"{report['passed']} passed, {report['failed']} failed\n")
        print(f"Report exported to {args.export}")
    
    # Exit with failure code if any error-severity checks failed
    error_failures = [r for r in report['results'] 
                      if not r['passed'] and r['severity'] == 'error']
    sys.exit(1 if error_failures else 0)


if __name__ == "__main__":
    main()
```

**Step 4: Create the validation API endpoint.**

Create `server/routes/validation.py`:

```python
"""
Validation status API endpoints.
Surfaces data quality status in the platform.
"""

from fastapi import APIRouter
from server.db import get_connection
from server.engines.validator import init_validation_tables

router = APIRouter(prefix="/api/validation", tags=["validation"])


@router.get("/latest")
async def latest_validation_run():
    """Get the most recent validation run summary."""
    conn = get_connection()
    init_validation_tables(conn)
    
    try:
        summary = conn.execute("""
            SELECT 
                run_id,
                MAX(run_timestamp) as timestamp,
                COUNT(*) as total_checks,
                COUNT(*) FILTER (WHERE passed) as passed,
                COUNT(*) FILTER (WHERE NOT passed) as failed,
                COUNT(*) FILTER (WHERE NOT passed AND severity = 'error') as critical_failures
            FROM fact_validation_results
            WHERE run_id = (
                SELECT run_id FROM fact_validation_results 
                ORDER BY run_timestamp DESC LIMIT 1
            )
            GROUP BY run_id
        """).fetchdf().to_dict(orient="records")
        
        if not summary:
            return {"status": "no_runs", "message": "No validation runs found"}
        
        return summary[0]
    except Exception:
        return {"status": "not_initialized"}


@router.get("/results")
async def validation_results(run_id: str = None, domain: str = None, 
                              failures_only: bool = False):
    """Get detailed validation results."""
    conn = get_connection()
    init_validation_tables(conn)
    
    where = []
    params = []
    
    if run_id:
        where.append("run_id = ?")
        params.append(run_id)
    else:
        where.append("run_id = (SELECT run_id FROM fact_validation_results ORDER BY run_timestamp DESC LIMIT 1)")
    
    if domain:
        where.append("domain = ?")
        params.append(domain)
    
    if failures_only:
        where.append("NOT passed")
    
    where_clause = " AND ".join(where) if where else "TRUE"
    
    try:
        results = conn.execute(f"""
            SELECT check_id, table_name, domain, check_type, check_name,
                   severity, passed, actual_value, expected_value, detail
            FROM fact_validation_results
            WHERE {where_clause}
            ORDER BY passed ASC, severity DESC, table_name
        """, params).fetchdf().to_dict(orient="records")
        
        return {"results": results, "count": len(results)}
    except Exception:
        return {"results": [], "count": 0}


@router.get("/domains")
async def validation_by_domain():
    """Get pass rates by domain for the latest run."""
    conn = get_connection()
    init_validation_tables(conn)
    
    try:
        return conn.execute("""
            SELECT 
                domain,
                COUNT(*) as total,
                COUNT(*) FILTER (WHERE passed) as passed,
                ROUND(COUNT(*) FILTER (WHERE passed) * 100.0 / COUNT(*), 1) as pass_rate
            FROM fact_validation_results
            WHERE run_id = (
                SELECT run_id FROM fact_validation_results 
                ORDER BY run_timestamp DESC LIMIT 1
            )
            GROUP BY domain
            ORDER BY pass_rate ASC
        """).fetchdf().to_dict(orient="records")
    except Exception:
        return []
```

**Step 5: Wire validation routes into `server/main.py`.**

Add to the router imports:
```python
from server.routes.validation import router as validation_router
app.include_router(validation_router)
```

**Step 6: Add validation run to Dagster pipeline (optional but recommended).**

In `pipeline/dagster_pipeline.py`, add a validation job that runs after ETL completes:

```python
@job
def validate_lake():
    """Run full validation suite after ETL refresh."""
    from scripts.run_validation import main
    main()
```

Wire this to your existing schedules so validation runs automatically after data refreshes.

### Acceptance Criteria
- [ ] `server/engines/validator.py` exists with 6 check types (row count, not null, range, referential integrity, unique, cross-source)
- [ ] `scripts/validation_checks.py` defines 30+ checks across all major domains
- [ ] `scripts/run_validation.py` runs end-to-end and produces a report
- [ ] `fact_validation_results` table stores results in the lake
- [ ] `/api/validation/latest` returns the most recent run summary
- [ ] `/api/validation/results` returns detailed check results
- [ ] `/api/validation/domains` returns pass rates by domain
- [ ] `python scripts/run_validation.py --export report.md` produces a clean markdown report
- [ ] Running the suite against the current lake produces a pass rate >= 80% (fix any failing checks that indicate real data issues)

---

## Priority 4: Demo Stability Polish

### 4a: Pre-cache Demo Responses for Intelligence

The build doc mentions 27 pre-cached demo responses. Verify these work and add any that are missing.

**Check that `?demo=true` URL parameter works correctly:**
1. Visit `https://aradune.co/?demo=true`
2. Verify Intelligence shows pre-cached responses for starter prompts
3. Verify all 6 starter prompts produce clean, complete responses
4. Verify tables, charts, and citations render correctly in demo mode

**If demo responses are stale or broken, regenerate them:**
- Run each starter prompt through Intelligence
- Save the full response (markdown + tables + charts + queries) to the cache
- Verify the response cache (LRU 200, 6hr TTL) doesn't evict demo responses

### 4b: Loading States and Error Messages

Check every module for graceful loading and error states:
- Every module should show a loading indicator while API calls are in flight
- Every module should show a clean empty state ("No data available for this selection") instead of blank white space
- No module should show raw JSON or a stack trace to the user under any circumstance

Search the frontend for patterns like:
```tsx
// BAD: crashes if data is null
{data.map(item => ...)}

// GOOD: handles null/empty
{data && data.length > 0 ? data.map(item => ...) : <EmptyState />}
```

### 4c: Console Error Cleanup

Open the browser console while navigating through every module. Fix or suppress:
- Any `TypeError` or `undefined` errors
- Any failed network requests (404s, CORS errors)
- Any React key warnings
- Any deprecation warnings that might appear during a demo

### Acceptance Criteria
- [ ] `?demo=true` loads and all 6 starter prompts work
- [ ] No module shows blank white space on empty data
- [ ] No module shows raw JSON or stack traces
- [ ] Browser console is clean during a full walkthrough of all 15 core modules

---

## Priority 5: One-Page Architecture Document

Create a PDF-ready architecture summary for the meeting.

Create `docs/architecture-summary.md`:

```markdown
# Aradune: Medicaid Intelligence Platform
## Architecture Overview

### Three-Layer Architecture

**Layer 1 -- The Data Lake**
750+ tables | 400M+ rows | 20 domains | 90+ federal/state sources
DuckDB over Hive-partitioned Parquet | Medallion architecture (Bronze/Silver/Gold)
All 51 jurisdictions | Full fee schedule coverage

**Layer 2 -- The Ontology**  
16 entity types | 28 relationship edges | 19 named metrics
YAML-defined, auto-generates Intelligence system prompt + property graph
Add a dataset = add a YAML file + run a script

**Layer 3 -- Intelligence**
Claude-powered analytical engine with direct SQL access to the full lake
Tiered routing (Haiku/Sonnet/Opus) | RAG over 1,039 CMS policy documents
Natural language in, compliance-ready analysis out

### Key Capabilities

| Capability | Status | Regulatory Driver |
|-----------|--------|-------------------|
| CPRA Compliance Generator | Production | 42 CFR 447.203, July 2026 |
| Cross-State Rate Comparison | Production | 51 jurisdictions |
| Caseload & Expenditure Forecasting | Production | SARIMAX + ETS |
| Fiscal Impact Modeling | Production | OBBBA, SDP caps |
| Hospital AHEAD Readiness | Production | CMS AHEAD model |
| 12 Research Briefs (statistical) | Production | Academic methods |
| Self-Corrective Intelligence (Skillbook) | Architecture complete | -- |
| Data Validation Framework | Operational | -- |
| Network Adequacy Engine | Roadmap | Access rule, ~July 2027 |

### Infrastructure

Frontend: React + TypeScript (Vercel)
Backend: Python FastAPI + DuckDB (Fly.io)
Storage: Cloudflare R2 (890+ Parquet files)
Auth: Clerk
AI: Claude API (tiered: Haiku/Sonnet/Opus)
CI/CD: GitHub Actions

### Data Sensitivity

Currently Ring 0/0.5/1 only (public regulatory, economic, aggregated).
No PHI. No BAA required. SOC 2 Type II on roadmap for Ring 2+.

### Replication Cost Estimate

| Component | Time | Cost |
|-----------|------|------|
| Data lake assembly (90+ sources, 115+ ETL scripts) | 12-24 months | $500K-$1M |
| Domain logic + ontology | 6-12 months | $200-400K |
| Intelligence engine + RAG | 2-4 months | $100-200K |
| Regulatory expertise (encoded rules, caveats) | Ongoing | Irreplaceable |
```

Generate this as a clean PDF. Keep it to one page.

### Acceptance Criteria
- [ ] `docs/architecture-summary.md` exists
- [ ] Content fits on one printed page
- [ ] Can be exported as PDF

---

## Execution Order

1. **Password gate removal + Clerk setup** (2-4 hours)
2. **500 error fixes + safe_route decorator** (4-8 hours) 
3. **Validation engine + checks + runner + API** (6-10 hours)
4. **Demo stability polish** (2-4 hours)
5. **Architecture one-pager** (1 hour)

Total estimated: 15-27 hours of implementation work.

---

## Notes for Claude Code

- The DuckDB connection is managed in `server/db.py` via `get_connection()`. All new engines should use this, not create their own connections.
- Table names in SQL queries match the Parquet directory names under `data/lake/fact/` and `data/lake/dimension/`. They are registered as views in DuckDB at startup.
- The frontend uses inline styles exclusively (no CSS files, no Tailwind). Follow the existing `design.ts` tokens: `C.ink`, `C.brand`, `C.accent`, `C.surface`, `C.border`, `C.pos`, `C.neg`, `C.warn`.
- All route files live in `server/routes/` and are imported in `server/main.py`.
- FL Medicaid: Facility and PC/TC rates are typically mutually exclusive (99.96% of codes). Three codes (46924, 91124, 91125) legitimately carry both facility and PC/TC rates as published by AHCA.
- Never use em-dashes in any generated text. This is a codebase style rule.
- Test everything against the actual lake data. The DuckDB views should have 750+ tables registered when fully loaded.
