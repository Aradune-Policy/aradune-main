from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from server.config import settings
from server.db import init_db, close_db
from server.routes import query, meta, presets, cpra, lake, pipeline, pharmacy, policy, wages, hospitals, enrollment, staffing, quality, context, bulk, supplemental


@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
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


@app.get("/health")
async def health():
    return {"status": "ok"}
