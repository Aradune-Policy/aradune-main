"""
Platform-wide search endpoint.

GET /api/search?q=...
Returns categorized results with relevance scoring across:
  - table names, descriptions, column names
  - (tools & states are handled client-side for instant results)
"""

from fastapi import APIRouter, Query
from server.db import get_cursor
from server.routes.meta import TABLE_DESCRIPTIONS
from server.utils.error_handler import safe_route

router = APIRouter()


def _score(query: str, text: str) -> int:
    """Simple relevance scoring. Higher = more relevant."""
    q = query.lower()
    t = text.lower()
    if q == t:
        return 100
    if t.startswith(q):
        return 80
    # whole-word match
    words = t.split()
    if q in words:
        return 60
    # substring match
    if q in t:
        return 40
    # fuzzy: all query words appear somewhere in text
    query_words = q.split()
    if all(w in t for w in query_words):
        return 30
    return 0


@router.get("/api/search")
@safe_route(default_response={"results": {"tables": [], "columns": [], "codes": []}, "query": "", "total": 0})
async def search(q: str = Query(..., min_length=1, max_length=200)):
    """Search across table names, column names, table descriptions, and HCPCS codes."""
    query = q.strip()
    if not query:
        return {"results": [], "query": query}

    results = {
        "tables": [],
        "columns": [],
        "codes": [],
    }

    # ── Search table names and descriptions ──────────────────────────
    for table_name, description in TABLE_DESCRIPTIONS.items():
        name_score = _score(query, table_name)
        # Also match the human-readable part (e.g. "enrollment" in "fact_enrollment")
        short_name = table_name.replace("fact_", "").replace("dim_", "").replace("ref_", "")
        short_score = _score(query, short_name)
        desc_score = _score(query, description) if description else 0
        best = max(name_score, short_score, desc_score)
        if best > 0:
            results["tables"].append({
                "name": table_name,
                "description": description,
                "score": best,
                "category": (
                    "dimension" if table_name.startswith("dim_") else
                    "reference" if table_name.startswith("ref_") else
                    "fact"
                ),
            })

    results["tables"].sort(key=lambda x: x["score"], reverse=True)
    results["tables"] = results["tables"][:20]

    # ── Search column names across all tables ────────────────────────
    try:
        with get_cursor() as cur:
            # Search columns from information_schema
            q_lower = query.lower()
            col_rows = cur.execute(
                """
                SELECT table_name, column_name, data_type
                FROM information_schema.columns
                WHERE table_schema = 'main'
                  AND (
                    LOWER(column_name) LIKE '%' || ? || '%'
                  )
                ORDER BY table_name, column_name
                LIMIT 50
                """,
                [q_lower],
            ).fetchall()

            seen = set()
            for table_name, column_name, data_type in col_rows:
                key = f"{table_name}.{column_name}"
                if key in seen:
                    continue
                seen.add(key)
                col_score = _score(query, column_name)
                if col_score > 0:
                    results["columns"].append({
                        "table": table_name,
                        "column": column_name,
                        "type": data_type,
                        "score": col_score,
                        "table_description": TABLE_DESCRIPTIONS.get(table_name, ""),
                    })

            results["columns"].sort(key=lambda x: x["score"], reverse=True)
            results["columns"] = results["columns"][:20]

            # ── Search HCPCS codes ───────────────────────────────────────
            # Check if dim_procedure exists and search it
            try:
                code_rows = cur.execute(
                    """
                    SELECT cpt_hcpcs_code, description, category_447
                    FROM dim_procedure
                    WHERE LOWER(cpt_hcpcs_code) LIKE '%' || ? || '%'
                       OR LOWER(description) LIKE '%' || ? || '%'
                    ORDER BY
                        CASE WHEN LOWER(cpt_hcpcs_code) = ? THEN 0
                             WHEN LOWER(cpt_hcpcs_code) LIKE ? || '%' THEN 1
                             ELSE 2
                        END,
                        cpt_hcpcs_code
                    LIMIT 20
                    """,
                    [q_lower, q_lower, q_lower, q_lower],
                ).fetchall()

                for code, desc, category in code_rows:
                    code_score = _score(query, code or "")
                    desc_score = _score(query, desc or "")
                    best = max(code_score, desc_score)
                    if best > 0:
                        results["codes"].append({
                            "code": code,
                            "description": desc or "",
                            "category": category or "",
                            "score": best,
                        })
            except Exception:
                # dim_procedure may not exist in some environments
                pass

    except Exception as e:
        # If DB is unavailable, return what we have (table name/desc matches)
        results["_error"] = str(e)

    return {
        "results": results,
        "query": query,
        "total": (
            len(results["tables"])
            + len(results["columns"])
            + len(results["codes"])
        ),
    }
