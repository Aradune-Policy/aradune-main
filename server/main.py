import json
import logging
import time
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from starlette.middleware.base import BaseHTTPMiddleware

from server.config import settings
from server.db import init_db, close_db, is_lake_ready, reload_lake
from server.routes import query, meta, presets, cpra, lake, pipeline, pharmacy, policy, wages, hospitals, enrollment, staffing, quality, context, bulk, supplemental, behavioral_health, round9, forecast, nl2sql, intelligence, import_data, search, insights, rate_explorer, skillbook, validation, state_context, dynamics
from server.routes.research import (
    rate_quality, mc_value, treatment_gap, safety_net,
    integrity_risk, fiscal_cliff, maternal_health,
    pharmacy_spread, nursing_ownership, waiver_impact,
    tmsis_calibration,
    meps_analysis,
    network_adequacy,
)


class SecurityHeadersMiddleware(BaseHTTPMiddleware):
    """Add security headers to every response (OWASP best practices)."""
    async def dispatch(self, request: Request, call_next):
        response: Response = await call_next(request)
        response.headers["Strict-Transport-Security"] = "max-age=63072000; includeSubDomains; preload"
        response.headers["X-Content-Type-Options"] = "nosniff"
        response.headers["X-Frame-Options"] = "DENY"
        response.headers["Referrer-Policy"] = "strict-origin-when-cross-origin"
        response.headers["Permissions-Policy"] = "camera=(), microphone=(), geolocation=()"
        return response


class JSONFormatter(logging.Formatter):
    """JSON log formatter for production observability."""
    def format(self, record):
        log = {
            "ts": time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(record.created)),
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
        }
        if record.exc_info and record.exc_info[0]:
            log["error"] = self.formatException(record.exc_info)
        return json.dumps(log)

# Configure root logger for JSON output in production
_handler = logging.StreamHandler()
_handler.setFormatter(JSONFormatter())
logging.root.handlers = [_handler]
logging.root.setLevel(logging.INFO)


class RequestTimingMiddleware(BaseHTTPMiddleware):
    """Log request method, path, status, and duration."""
    async def dispatch(self, request: Request, call_next):
        t0 = time.time()
        response = await call_next(request)
        ms = int((time.time() - t0) * 1000)
        if request.url.path not in ("/healthz", "/ready"):
            logging.getLogger("aradune.http").info(
                f"{request.method} {request.url.path} {response.status_code} {ms}ms"
            )
        return response


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
app.add_middleware(SecurityHeadersMiddleware)
app.add_middleware(RequestTimingMiddleware)


@app.get("/healthz")
async def healthz():
    """Liveness probe. Pure function, no I/O."""
    return {"status": "ok"}


@app.get("/ready")
async def ready():
    """Readiness probe. Checks DuckDB connectivity."""
    from server.db import is_lake_ready, get_cursor
    try:
        with get_cursor() as cur:
            cur.execute("SELECT 1").fetchone()
        return {"status": "ready", "lake": is_lake_ready()}
    except Exception as e:
        from fastapi import Response
        return Response(content='{"status":"not_ready"}', status_code=503, media_type="application/json")


@app.get("/startup")
async def startup():
    """Startup probe. Confirms views registered."""
    from server.db import is_lake_ready
    if is_lake_ready():
        return {"status": "started", "lake": True}
    from fastapi import Response
    return Response(content='{"status":"starting"}', status_code=503, media_type="application/json")


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
app.include_router(skillbook.router)
app.include_router(validation.router)
app.include_router(state_context.router)
app.include_router(dynamics.router)
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
app.include_router(network_adequacy.router)


@app.get("/health")
async def health():
    """Lightweight health check — returns 200 immediately, even before lake is ready.

    Fly.io hits this to decide whether the machine is alive. It must
    respond fast so the machine isn't killed during view registration.
    """
    return {"status": "ok", "lake_ready": is_lake_ready()}


@app.post("/internal/reload-lake")
async def internal_reload_lake():
    """Called by entrypoint.sh after background R2 sync completes.

    Re-scans the lake directory and registers any new Parquet files as views.
    Not exposed externally (Fly.io only routes /api and /health).
    """
    reload_lake()
    return {"status": "reloading"}
