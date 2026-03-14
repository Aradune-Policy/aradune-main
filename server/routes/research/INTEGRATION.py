"""
Research Module Integration Guide
==================================
Add these lines to server/main.py to activate the research modules.
Do NOT modify this file — it is a reference only.

Step 1: Add to imports (near line 6 of main.py):
"""

# --- Add these imports ---
from server.routes.research import (
    rate_quality,
    mc_value,
    treatment_gap,
    safety_net,
    integrity_risk,
    fiscal_cliff,
    maternal_health,
    pharmacy_spread,
    nursing_ownership,
    waiver_impact,
)

"""
Step 2: Add router registrations (near line 54 of main.py):
"""

# --- Add these router registrations ---
# app.include_router(rate_quality.router)
# app.include_router(mc_value.router)
# app.include_router(treatment_gap.router)
# app.include_router(safety_net.router)
# app.include_router(integrity_risk.router)
# app.include_router(fiscal_cliff.router)
# app.include_router(maternal_health.router)
# app.include_router(pharmacy_spread.router)
# app.include_router(nursing_ownership.router)
# app.include_router(waiver_impact.router)

"""
Step 3: Verify endpoints are live:
    curl http://localhost:8000/api/research/rate-quality/measures
    curl http://localhost:8000/api/research/mc-value/mco-summary
    curl http://localhost:8000/api/research/treatment-gap/demand-supply
    curl http://localhost:8000/api/research/safety-net/hospital-stress
    curl http://localhost:8000/api/research/integrity-risk/composite
    curl http://localhost:8000/api/research/fiscal-cliff/spending-vs-revenue
    curl http://localhost:8000/api/research/maternal-health/mortality
    curl http://localhost:8000/api/research/pharmacy-spread/stats
    curl http://localhost:8000/api/research/nursing-ownership/quality-by-type
    curl http://localhost:8000/api/research/waiver-impact/catalog

Total: 45 new API endpoints across 10 modules.
"""
