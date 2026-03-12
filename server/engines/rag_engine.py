"""
RAG Engine: Hybrid search over the Medicaid policy corpus.

Combines BM25 full-text search with optional vector similarity search
using DuckDB's fts and vss extensions. Falls back to BM25-only when
no embeddings are available.

Usage:
    from server.engines.rag_engine import hybrid_search
    results = hybrid_search("physician fee schedule methodology", states=["FL"])
"""

import json
import os
import re
from typing import Optional

from server.db import get_cursor

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

_VSS_AVAILABLE = False
_FTS_READY = False
_CORPUS_AVAILABLE = False

# Voyage API (optional — for query embedding when vector search is enabled)
VOYAGE_API_KEY = os.environ.get("VOYAGE_API_KEY", "")
VOYAGE_MODEL = "voyage-3-large"
VOYAGE_DIMS = 1024


def _check_corpus() -> bool:
    """Check if policy corpus tables exist and have data."""
    global _CORPUS_AVAILABLE
    if _CORPUS_AVAILABLE:
        return True
    try:
        with get_cursor() as cur:
            count = cur.execute(
                "SELECT COUNT(*) FROM fact_policy_chunk"
            ).fetchone()[0]
            _CORPUS_AVAILABLE = count > 0
            return _CORPUS_AVAILABLE
    except Exception:
        return False


def _check_vss() -> bool:
    """Check if vss extension is loaded and embeddings exist."""
    global _VSS_AVAILABLE
    try:
        with get_cursor() as cur:
            # Check if any chunk has embeddings
            row = cur.execute("""
                SELECT COUNT(*) FROM fact_policy_chunk
                WHERE embedding IS NOT NULL
            """).fetchone()
            _VSS_AVAILABLE = row[0] > 0
            return _VSS_AVAILABLE
    except Exception:
        return False


def _init_fts() -> bool:
    """Initialize full-text search index if not already done.

    DuckDB FTS requires a TABLE (not a view). We materialize the view
    into a table first, then build the FTS index on that table.
    """
    global _FTS_READY
    if _FTS_READY:
        return True
    try:
        with get_cursor() as cur:
            cur.execute("LOAD fts;")
            # Materialize view into table (FTS needs a real table)
            cur.execute("""
                CREATE OR REPLACE TABLE _fts_policy_chunk AS
                SELECT chunk_id, doc_id, doc_type, state_code,
                       chunk_index, text, section_title,
                       page_start, page_end, token_count
                FROM fact_policy_chunk
            """)
            cur.execute("""
                PRAGMA create_fts_index(
                    '_fts_policy_chunk', 'chunk_id',
                    'text', 'section_title',
                    overwrite=1
                )
            """)
            _FTS_READY = True
            return True
    except Exception:
        return False


# ---------------------------------------------------------------------------
# Embedding (optional — only when VOYAGE_API_KEY is set)
# ---------------------------------------------------------------------------

def _embed_query(text: str) -> Optional[list[float]]:
    """Embed a query string using Voyage API. Returns None if unavailable."""
    if not VOYAGE_API_KEY:
        return None
    try:
        import voyageai
        client = voyageai.Client(api_key=VOYAGE_API_KEY)
        result = client.embed([text], model=VOYAGE_MODEL, input_type="query")
        return result.embeddings[0]
    except Exception:
        return None


# ---------------------------------------------------------------------------
# Search methods
# ---------------------------------------------------------------------------

def _bm25_search(
    query: str,
    doc_types: Optional[list[str]] = None,
    states: Optional[list[str]] = None,
    top_k: int = 20,
) -> list[dict]:
    """Full-text BM25 search over policy chunks."""
    if not _init_fts():
        return []

    # Build WHERE clause for filters
    filters = []
    params = []
    if doc_types:
        placeholders = ", ".join(["?" for _ in doc_types])
        filters.append(f"pc.doc_type IN ({placeholders})")
        params.extend(doc_types)
    if states:
        placeholders = ", ".join(["?" for _ in states])
        filters.append(f"(pc.state_code IN ({placeholders}) OR pc.state_code IS NULL)")
        params.extend([s.upper() for s in states])

    where = " AND ".join(filters) if filters else "1=1"

    # Escape single quotes in query for FTS
    safe_query = query.replace("'", "''")

    try:
        with get_cursor() as cur:
            sql = f"""
                WITH ranked AS (
                    SELECT
                        pc.chunk_id,
                        pc.doc_id,
                        pc.doc_type,
                        pc.state_code,
                        pc.chunk_index,
                        pc.text,
                        pc.section_title,
                        pc.page_start,
                        pc.page_end,
                        pc.token_count,
                        fts_main__fts_policy_chunk.match_bm25(
                            chunk_id, '{safe_query}'
                        ) AS bm25_score
                    FROM _fts_policy_chunk pc
                    WHERE {where}
                )
                SELECT * FROM ranked
                WHERE bm25_score IS NOT NULL
                ORDER BY bm25_score DESC
                LIMIT {top_k}
            """
            rows = cur.execute(sql, params).fetchall()
            columns = [desc[0] for desc in cur.description]
            return [dict(zip(columns, row)) for row in rows]
    except Exception:
        return _fallback_search(query, doc_types, states, top_k)


def _fallback_search(
    query: str,
    doc_types: Optional[list[str]] = None,
    states: Optional[list[str]] = None,
    top_k: int = 20,
) -> list[dict]:
    """Simple ILIKE fallback when FTS index is not available."""
    filters = []
    params = []

    # Split query into keywords
    keywords = [w.strip() for w in query.split() if len(w.strip()) > 2]
    if not keywords:
        return []

    # Build ILIKE conditions — match any keyword
    kw_conditions = []
    for kw in keywords[:5]:
        kw_conditions.append("(text ILIKE ? OR section_title ILIKE ?)")
        params.extend([f"%{kw}%", f"%{kw}%"])
    filters.append(f"({' OR '.join(kw_conditions)})")

    if doc_types:
        placeholders = ", ".join(["?" for _ in doc_types])
        filters.append(f"doc_type IN ({placeholders})")
        params.extend(doc_types)
    if states:
        placeholders = ", ".join(["?" for _ in states])
        filters.append(f"(state_code IN ({placeholders}) OR state_code IS NULL)")
        params.extend([s.upper() for s in states])

    where = " AND ".join(filters)

    try:
        with get_cursor() as cur:
            sql = f"""
                SELECT
                    chunk_id, doc_id, doc_type, state_code,
                    chunk_index, text, section_title,
                    page_start, page_end, token_count,
                    0.5 AS bm25_score
                FROM fact_policy_chunk
                WHERE {where}
                LIMIT {top_k}
            """
            rows = cur.execute(sql, params).fetchall()
            columns = [desc[0] for desc in cur.description]
            return [dict(zip(columns, row)) for row in rows]
    except Exception:
        return []


def _vector_search(
    query_embedding: list[float],
    doc_types: Optional[list[str]] = None,
    states: Optional[list[str]] = None,
    top_k: int = 20,
) -> list[dict]:
    """Vector similarity search using DuckDB vss extension."""
    filters = []
    if doc_types:
        dt_list = ", ".join([f"'{d}'" for d in doc_types])
        filters.append(f"doc_type IN ({dt_list})")
    if states:
        st_list = ", ".join([f"'{s.upper()}'" for s in states])
        filters.append(f"(state_code IN ({st_list}) OR state_code IS NULL)")
    filters.append("embedding IS NOT NULL")

    where = " AND ".join(filters) if filters else "embedding IS NOT NULL"

    # Format embedding as DuckDB array literal
    emb_str = "[" + ",".join([str(v) for v in query_embedding]) + "]"

    try:
        with get_cursor() as cur:
            sql = f"""
                SELECT
                    chunk_id, doc_id, doc_type, state_code,
                    chunk_index, text, section_title,
                    page_start, page_end, token_count,
                    array_cosine_similarity(
                        embedding, {emb_str}::FLOAT[{VOYAGE_DIMS}]
                    ) AS vec_score
                FROM fact_policy_chunk
                WHERE {where}
                ORDER BY vec_score DESC
                LIMIT {top_k}
            """
            rows = cur.execute(sql).fetchall()
            columns = [desc[0] for desc in cur.description]
            return [dict(zip(columns, row)) for row in rows]
    except Exception:
        return []


# ---------------------------------------------------------------------------
# Reciprocal Rank Fusion
# ---------------------------------------------------------------------------

def _rrf_merge(
    bm25_results: list[dict],
    vec_results: list[dict],
    k: int = 60,
    top_k: int = 10,
) -> list[dict]:
    """Merge BM25 and vector results using Reciprocal Rank Fusion."""
    scores: dict[str, float] = {}
    chunk_map: dict[str, dict] = {}

    for rank, item in enumerate(bm25_results):
        cid = item["chunk_id"]
        scores[cid] = scores.get(cid, 0) + 1.0 / (k + rank + 1)
        chunk_map[cid] = item

    for rank, item in enumerate(vec_results):
        cid = item["chunk_id"]
        scores[cid] = scores.get(cid, 0) + 1.0 / (k + rank + 1)
        if cid not in chunk_map:
            chunk_map[cid] = item

    # Sort by RRF score
    ranked = sorted(scores.items(), key=lambda x: x[1], reverse=True)[:top_k]

    results = []
    for cid, score in ranked:
        item = chunk_map[cid]
        item["rrf_score"] = round(score, 6)
        # Remove embedding from result
        item.pop("embedding", None)
        item.pop("vec_score", None)
        item.pop("bm25_score", None)
        results.append(item)

    return results


# ---------------------------------------------------------------------------
# Enrich results with document metadata
# ---------------------------------------------------------------------------

def _enrich_with_doc_metadata(chunks: list[dict]) -> list[dict]:
    """Add document-level metadata (title, source_url, etc.) to each chunk."""
    if not chunks:
        return chunks

    doc_ids = list({c["doc_id"] for c in chunks})
    if not doc_ids:
        return chunks

    placeholders = ", ".join([f"'{did}'" for did in doc_ids])
    try:
        with get_cursor() as cur:
            rows = cur.execute(f"""
                SELECT doc_id, title, doc_number, effective_date,
                       publication_date, source_url, summary
                FROM fact_policy_document
                WHERE doc_id IN ({placeholders})
            """).fetchall()
            columns = [desc[0] for desc in cur.description]
            doc_meta = {
                row[0]: dict(zip(columns, row))
                for row in rows
            }
    except Exception:
        doc_meta = {}

    for chunk in chunks:
        meta = doc_meta.get(chunk["doc_id"], {})
        chunk["doc_title"] = meta.get("title", "")
        chunk["doc_number"] = meta.get("doc_number", "")
        chunk["source_url"] = meta.get("source_url", "")
        chunk["effective_date"] = str(meta.get("effective_date", "")) if meta.get("effective_date") else None
        chunk["publication_date"] = str(meta.get("publication_date", "")) if meta.get("publication_date") else None
        chunk["doc_summary"] = meta.get("summary", "")

    return chunks


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def hybrid_search(
    query: str,
    doc_types: Optional[list[str]] = None,
    states: Optional[list[str]] = None,
    top_k: int = 10,
) -> dict:
    """
    Search the policy corpus using hybrid BM25 + vector retrieval.

    Returns a dict with:
        - results: list of matching chunks with metadata
        - method: "hybrid", "bm25", or "fallback"
        - total: number of results
    """
    if not _check_corpus():
        return {
            "results": [],
            "method": "none",
            "total": 0,
            "note": "Policy corpus not yet ingested. Run scripts/build_lake_policy_corpus.py first.",
        }

    top_k = min(top_k, 20)

    # Always try BM25
    bm25_results = _bm25_search(query, doc_types, states, top_k=top_k * 2)

    # Try vector search if embeddings are available
    method = "bm25"
    vec_results = []
    if _check_vss() and VOYAGE_API_KEY:
        query_emb = _embed_query(query)
        if query_emb:
            vec_results = _vector_search(query_emb, doc_types, states, top_k=top_k * 2)
            method = "hybrid"

    # Merge results
    if vec_results:
        results = _rrf_merge(bm25_results, vec_results, top_k=top_k)
    elif bm25_results:
        results = bm25_results[:top_k]
        for r in results:
            r.pop("bm25_score", None)
    else:
        # BM25 returned nothing — try ILIKE fallback
        results = _fallback_search(query, doc_types, states, top_k)
        method = "fallback" if results else "none"

    # Enrich with document metadata
    results = _enrich_with_doc_metadata(results)

    return {
        "results": results,
        "method": method,
        "total": len(results),
        "query": query,
    }


def corpus_stats() -> dict:
    """Return stats about the policy corpus."""
    try:
        with get_cursor() as cur:
            doc_count = cur.execute(
                "SELECT COUNT(*) FROM fact_policy_document"
            ).fetchone()[0]
            chunk_count = cur.execute(
                "SELECT COUNT(*) FROM fact_policy_chunk"
            ).fetchone()[0]
            by_type = cur.execute("""
                SELECT doc_type, COUNT(*) AS cnt
                FROM fact_policy_document
                GROUP BY doc_type ORDER BY cnt DESC
            """).fetchall()
            try:
                has_embeddings = cur.execute("""
                    SELECT COUNT(*) FROM fact_policy_chunk
                    WHERE embedding IS NOT NULL
                """).fetchone()[0]
            except Exception:
                has_embeddings = 0

        return {
            "documents": doc_count,
            "chunks": chunk_count,
            "by_type": {row[0]: row[1] for row in by_type},
            "has_embeddings": has_embeddings,
            "vector_search": _VSS_AVAILABLE and bool(VOYAGE_API_KEY),
            "bm25_search": True,
        }
    except Exception:
        return {
            "documents": 0,
            "chunks": 0,
            "by_type": {},
            "has_embeddings": 0,
            "vector_search": False,
            "bm25_search": False,
            "note": "Policy corpus not available",
        }
