import time
from fastapi import APIRouter, HTTPException
from server.models import QueryRequest, QueryResponse
from server.query_builder import build_query
from server.db import get_cursor

router = APIRouter()


@router.post("/api/query", response_model=QueryResponse)
async def query(req: QueryRequest):
    try:
        sql, params = build_query(req)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Invalid query: {e}")

    t0 = time.perf_counter()
    try:
        with get_cursor() as cur:
            result = cur.execute(sql, params)
            columns = [desc[0] for desc in result.description]
            raw_rows = result.fetchall()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Query error: {e}")

    query_ms = (time.perf_counter() - t0) * 1000

    rows = [dict(zip(columns, row)) for row in raw_rows]

    return QueryResponse(
        rows=rows,
        total_rows=len(rows),
        query_ms=round(query_ms, 1),
        sql_preview=sql if len(sql) < 2000 else sql[:2000] + "...",
    )
