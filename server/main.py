from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from server.config import settings
from server.db import init_db, close_db, is_lake_ready, reload_lake
from server.routes import query, meta, presets, cpra, lake, pipeline, pharmacy, policy, wages, hospitals, enrollment, staffing, quality, context, bulk, supplemental, behavioral_health, round9, forecast, nl2sql, intelligence, import_data, search, insights, rate_explorer
from server.routes.research import (
    rate_quality, mc_value, treatment_gap, safety_net,
    integrity_risk, fiscal_cliff, maternal_health,
    pharmacy_spread, nursing_ownership, waiver_impact,
    tmsis_calibration,
    meps_analysis,
)


@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()  # Creates connection immediately; views register in background
    yield
    close_db()


app = FastAPI(
    title="Aradune Query API",
    description="DuckDB-backed query API for Medicaid provider spending data",
    version="1.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(query.router)
app.include_router(meta.router)
app.include_router(presets.router)
app.include_router(cpra.router)
app.include_router(lake.router)
app.include_router(pipeline.router)
app.include_router(pharmacy.router)
app.include_router(policy.router)
app.include_router(wages.router)
app.include_router(hospitals.router)
app.include_router(enrollment.router)
app.include_router(staffing.router)
app.include_router(quality.router)
app.include_router(context.router)
app.include_router(bulk.router)
app.include_router(supplemental.router)
app.include_router(behavioral_health.router)
app.include_router(round9.router)
app.include_router(forecast.router)
app.include_router(nl2sql.router)
app.include_router(intelligence.router)
app.include_router(import_data.router)
app.include_router(search.router)
app.include_router(insights.router)
app.include_router(rate_explorer.router)
app.include_router(rate_quality.router)
app.include_router(mc_value.router)
app.include_router(treatment_gap.router)
app.include_router(safety_net.router)
app.include_router(integrity_risk.router)
app.include_router(fiscal_cliff.router)
app.include_router(maternal_health.router)
app.include_router(pharmacy_spread.router)
app.include_router(nursing_ownership.router)
app.include_router(waiver_impact.router)
app.include_router(tmsis_calibration.router)
app.include_router(meps_analysis.router)


@app.get("/health")
async def health():
    """Lightweight health check — returns 200 immediately, even before lake is ready.

    Fly.io hits this to decide whether the machine is alive. It must
    respond fast so the machine isn't killed during view registration.
    """
    return {"status": "ok", "lake_ready": is_lake_ready()}


@app.get("/ready")
async def ready():
    """Readiness probe — returns 200 only when all lake views are registered."""
    if is_lake_ready():
        return {"status": "ready"}
    return {"status": "initializing"}


@app.post("/internal/reload-lake")
async def internal_reload_lake():
    """Called by entrypoint.sh after background R2 sync completes.

    Re-scans the lake directory and registers any new Parquet files as views.
    Not exposed externally (Fly.io only routes /api and /health).
    """
    reload_lake()
    return {"status": "reloading"}
