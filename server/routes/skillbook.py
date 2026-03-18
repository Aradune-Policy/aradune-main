"""Skillbook admin endpoints -- view, add, retire skills."""

from fastapi import APIRouter, HTTPException, Query
from server.engines.skillbook import get_all_skills, add_skill, retire_skill, ensure_table
from server.db import get_cursor
from server.utils.error_handler import safe_route

router = APIRouter(tags=["skillbook"])


@router.get("/api/skillbook")
@safe_route(default_response={})
async def list_skills(domain: str = Query(None), active: bool = Query(True), limit: int = Query(50)):
    """List skills, optionally filtered by domain."""
    skills = get_all_skills(active_only=active, limit=limit)
    if domain:
        skills = [s for s in skills if s.get("domain") == domain]
    return {"skills": skills, "count": len(skills)}


@router.get("/api/skillbook/recent")
@safe_route(default_response={})
async def recent_skills(limit: int = Query(5)):
    """Get most recently added skills."""
    try:
        with get_cursor() as cur:
            rows = cur.execute("""
                SELECT skill_id, domain, category, content, source_type,
                       source_query, provenance, helpful_count, harmful_count,
                       times_retrieved, active, created_at
                FROM fact_skillbook
                ORDER BY created_at DESC
                LIMIT ?
            """, [limit]).fetchall()
            cols = ["skill_id", "domain", "category", "content", "source_type",
                    "source_query", "provenance", "helpful_count", "harmful_count",
                    "times_retrieved", "active", "created_at"]
            skills = []
            for row in rows:
                s = {}
                for k, v in zip(cols, row):
                    s[k] = v.isoformat() if hasattr(v, "isoformat") else v
                skills.append(s)
            return {"skills": skills, "count": len(skills)}
    except Exception:
        return {"skills": [], "count": 0}


@router.get("/api/skillbook/stats")
@safe_route(default_response={})
async def skillbook_stats():
    """Skillbook health metrics."""
    try:
        with get_cursor() as cur:
            row = cur.execute("""
                SELECT
                    COUNT(*) AS total_skills,
                    COUNT(*) FILTER (WHERE active) AS active_skills,
                    COUNT(*) FILTER (WHERE helpful_count - harmful_count > 0) AS validated_skills,
                    COUNT(*) FILTER (WHERE helpful_count - harmful_count < 0) AS suspect_skills,
                    COALESCE(SUM(times_retrieved), 0) AS total_retrievals,
                    ROUND(AVG(helpful_count - harmful_count), 2) AS avg_score,
                    COUNT(DISTINCT domain) AS domains_covered
                FROM fact_skillbook
            """).fetchone()
            return {
                "total_skills": row[0], "active_skills": row[1],
                "validated_skills": row[2], "suspect_skills": row[3],
                "total_retrievals": row[4], "avg_score": row[5],
                "domains_covered": row[6],
            }
    except Exception:
        return {"total_skills": 0, "active_skills": 0, "validated_skills": 0,
                "suspect_skills": 0, "total_retrievals": 0, "avg_score": 0, "domains_covered": 0}


@router.post("/api/skillbook/manual")
@safe_route(default_response={})
async def add_manual_skill(domain: str, category: str, content: str, provenance: str = Query(None)):
    """Manually add a skill (query params)."""
    skill_id = add_skill(domain=domain, category=category, content=content,
                         source_type="manual", provenance=provenance)
    return {"skill_id": skill_id}


@router.post("/api/skillbook/add")
@safe_route(default_response={})
async def add_skill_json(req: dict):
    """Add a skill via JSON body."""
    skill_id = add_skill(
        domain=req.get("domain", "general"),
        category=req.get("category", "learned"),
        content=req.get("content", ""),
        source_type=req.get("source_type", "manual"),
        provenance=req.get("provenance"),
    )
    return {"skill_id": skill_id}


@router.delete("/api/skillbook/{skill_id}")
@safe_route(default_response={})
async def deactivate_skill(skill_id: str):
    """Retire a skill."""
    retire_skill(skill_id)
    return {"status": "retired"}
