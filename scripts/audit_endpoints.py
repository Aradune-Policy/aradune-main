"""Quick health check against live Fly.io API. Run: python3 scripts/audit_endpoints.py"""
import urllib.request, json, time, sys

BASE = "https://aradune-api.fly.dev"
ENDPOINTS = [
    # Core
    ("GET", "/health"),
    ("GET", "/ready"),
    # Lake / States
    ("GET", "/api/states"),
    ("GET", "/api/enrollment/FL"),
    ("GET", "/api/quality/FL"),
    ("GET", "/api/expenditure/FL"),
    ("GET", "/api/lake/stats"),
    # CPRA
    ("GET", "/api/cpra/states"),
    ("GET", "/api/cpra/rates/FL"),
    ("GET", "/api/cpra/dq/FL"),
    ("GET", "/api/cpra/compare?codes=99213,99214"),
    ("GET", "/api/cpra/upload/states"),
    ("GET", "/api/cpra/upload/codes"),
    # Demographics / Context
    ("GET", "/api/demographics/FL"),
    ("GET", "/api/demographics"),
    ("GET", "/api/scorecard/FL"),
    ("GET", "/api/economic/FL"),
    ("GET", "/api/hcbs-waitlist"),
    ("GET", "/api/hcbs-waitlist/FL"),
    ("GET", "/api/ltss/FL"),
    ("GET", "/api/eligibility-processing"),
    ("GET", "/api/fmr/state-totals"),
    ("GET", "/api/snap/FL"),
    ("GET", "/api/tanf/FL"),
    # Enrollment
    ("GET", "/api/enrollment/eligibility/FL"),
    ("GET", "/api/enrollment/expansion/FL"),
    ("GET", "/api/enrollment/unwinding/FL"),
    ("GET", "/api/enrollment/managed-care-plans/FL"),
    # Hospitals
    ("GET", "/api/hospitals/search?q=shands"),
    ("GET", "/api/hospitals/summary"),
    # Policy
    ("GET", "/api/policy/fmap"),
    ("GET", "/api/policy/spas/FL"),
    ("GET", "/api/policy/waivers/FL"),
    ("GET", "/api/policy/managed-care/FL"),
    ("GET", "/api/policy/dsh/FL"),
    # Pharmacy
    ("GET", "/api/pharmacy/utilization/FL"),
    ("GET", "/api/pharmacy/nadac"),
    ("GET", "/api/pharmacy/top-drugs/FL"),
    # Wages
    ("GET", "/api/wages/FL"),
    ("GET", "/api/wages/national"),
    # Quality
    ("GET", "/api/five-star/summary"),
    ("GET", "/api/five-star/FL"),
    # Staffing
    ("GET", "/api/staffing/summary"),
    ("GET", "/api/staffing/FL"),
    # Bulk
    ("GET", "/api/bulk/states"),
    ("GET", "/api/bulk/quality-measures"),
    # Supplemental
    ("GET", "/api/supplemental/summary"),
    ("GET", "/api/supplemental/dsh/summary"),
    # Behavioral Health
    ("GET", "/api/behavioral-health/nsduh"),
    ("GET", "/api/behavioral-health/facilities"),
    # Medicare / Round9
    ("GET", "/api/medicare/enrollment"),
    ("GET", "/api/opioid/prescribing/summary"),
    # Forecast
    ("GET", "/api/forecast/public-enrollment"),
    # Search
    ("GET", "/api/search?q=florida"),
    # Meta
    ("GET", "/api/meta"),
    ("GET", "/api/presets"),
    # Insights
    ("GET", "/api/insights/FL"),
    # Catalog-style
    ("GET", "/api/bulk/fee-schedule-rates"),
]

ok, fail, err = [], [], []
for method, path in ENDPOINTS:
    url = BASE + path
    try:
        req = urllib.request.Request(url, method=method)
        req.add_header("User-Agent", "AraduneAudit/1.0")
        t0 = time.time()
        with urllib.request.urlopen(req, timeout=15) as resp:
            ms = int((time.time() - t0) * 1000)
            status = resp.status
            body = resp.read()
            size = len(body)
        if status == 200:
            ok.append((path, ms, size))
        else:
            fail.append((path, status, ms))
    except Exception as e:
        err.append((path, str(e)[:80]))

print(f"\n{'='*60}")
print(f"Endpoint Audit: {len(ok)} OK, {len(fail)} FAIL, {len(err)} ERROR")
print(f"{'='*60}\n")

if fail:
    print("FAILURES:")
    for path, status, ms in fail:
        print(f"  {status} {path} ({ms}ms)")
    print()

if err:
    print("ERRORS:")
    for path, e in err:
        print(f"  {path}: {e}")
    print()

# Show slowest 10
print("SLOWEST 10:")
for path, ms, size in sorted(ok, key=lambda x: -x[1])[:10]:
    print(f"  {ms:>5}ms  {size:>8} bytes  {path}")

print(f"\nAll {len(ok)} OK endpoints passed.")
