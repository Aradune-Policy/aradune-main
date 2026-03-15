# RAG Plan: Retrieval-Augmented Generation over the Medicaid Policy Corpus

> **Status:** Research & Planning (no code yet)
> **Created:** 2026-03-09, session 10
> **Author:** Planning document for implementation in a future session
> **Goal:** Enable natural-language search and AI-grounded analysis over SPAs, waivers, CIBs, SHO letters, and Federal Register notices

---

## 1. Architecture Overview

```
┌──────────────────────────────────────────────────────────────────────────┐
│                           FRONTEND                                       │
│   Intelligence Chat (/#/intelligence) ← new component                   │
│   "Ask about policy" ← routed to search_policy_corpus tool              │
│   Results: source excerpts + Claude synthesis + PDF links               │
└──────────────────────────────────────────────────┬───────────────────────┘
                                                   │
                                                   ▼
┌──────────────────────────────────────────────────────────────────────────┐
│                  server/routes/intelligence.py                           │
│                                                                          │
│   Existing tools:         New tool:                                      │
│   ├─ query_database       ├─ search_policy_corpus                       │
│   ├─ list_tables          │   ├─ query (str)                            │
│   ├─ describe_table       │   ├─ doc_types (optional filter)            │
│   └─ web_search           │   ├─ states (optional filter)               │
│                           │   └─ top_k (default 10)                     │
│                           │                                              │
│                           │   Returns: ranked chunks with               │
│                           │   source, page, score, text                  │
└──────────────────────────────────────────────────┬───────────────────────┘
                                                   │
                                                   ▼
┌──────────────────────────────────────────────────────────────────────────┐
│               server/engines/rag_engine.py (NEW)                        │
│                                                                          │
│   hybrid_search(query, filters) → ranked chunks                         │
│   ├─ 1. Embed query via Voyage-3-large API                              │
│   ├─ 2. Vector search in DuckDB (cosine similarity)                     │
│   ├─ 3. BM25 keyword search in DuckDB (full-text index)                │
│   ├─ 4. Reciprocal Rank Fusion (RRF) to merge results                  │
│   └─ 5. Return top_k chunks with metadata                              │
└──────────────────────────────────────────────────┬───────────────────────┘
                                                   │
                                                   ▼
┌──────────────────────────────────────────────────────────────────────────┐
│               DuckDB (existing in-memory connection)                     │
│                                                                          │
│   policy_document          │  policy_chunk                               │
│   ├─ doc_id (PK)           │  ├─ chunk_id (PK)                          │
│   ├─ doc_type              │  ├─ doc_id (FK)                            │
│   ├─ title                 │  ├─ chunk_index                            │
│   ├─ state_code            │  ├─ text                                   │
│   ├─ doc_number            │  ├─ embedding FLOAT[1024]                  │
│   ├─ effective_date        │  ├─ page_start                             │
│   ├─ approval_date         │  ├─ page_end                               │
│   ├─ source_url            │  ├─ section_title                          │
│   ├─ pdf_path              │  └─ token_count                            │
│   ├─ status                │                                             │
│   └─ summary               │  (stored as Parquet, loaded into DuckDB)   │
│                             │                                             │
│   ┌─────────────────────────┴─────────────────────────────────────┐      │
│   │  DuckDB vss extension (vector similarity search)              │      │
│   │  + DuckDB fts extension (full-text search / BM25)             │      │
│   └───────────────────────────────────────────────────────────────┘      │
└──────────────────────────────────────────────────────────────────────────┘
                                                   │
                                                   ▼
┌──────────────────────────────────────────────────────────────────────────┐
│            Embedding Pipeline (offline, run locally)                     │
│                                                                          │
│   scripts/build_lake_policy_corpus.py                                   │
│   ├─ 1. Download PDFs from CMS MACPro / medicaid.gov                   │
│   ├─ 2. Extract text via pdfplumber (+ Claude API for complex PDFs)    │
│   ├─ 3. Chunk documents (sliding window, section-aware)                │
│   ├─ 4. Embed chunks via Voyage-3-large API (batch)                    │
│   ├─ 5. Write Parquet: policy_document + policy_chunk                   │
│   └─ 6. Sync to R2 (same as all other lake tables)                     │
└──────────────────────────────────────────────────────────────────────────┘
```

---

## 2. Technology Choices

### 2a. Vector Store: DuckDB (in-process) — NOT pgvector

**Decision:** Use DuckDB with the `vss` (vector similarity search) extension, not pgvector.

**Rationale:**
- We already run DuckDB in-memory for 250+ tables. Adding vectors to the same connection avoids a new service.
- Fly.io is on a shared-CPU 2GB RAM instance. Adding a PostgreSQL service would require a second Fly.io machine or an external managed database ($15-50+/month).
- DuckDB's `vss` extension supports HNSW indexes on `FLOAT[]` arrays with cosine, L2, and inner product distance metrics.
- The policy corpus is bounded: ~5,000-20,000 documents across all types. Even at 10 chunks per doc and 1024-dim embeddings, that is ~200K chunks = ~800MB of embeddings. Fits in memory on the 2GB Fly instance if we keep chunk count reasonable.
- DuckDB also has a `fts` (full-text search) extension for BM25 keyword search, enabling hybrid retrieval in a single engine.
- **Tradeoff:** DuckDB `vss` is newer and less battle-tested than pgvector. If the corpus grows beyond 500K chunks or we need multi-tenant isolation, we would migrate to pgvector or Qdrant.

**Rejected alternatives:**
| Option | Why rejected |
|--------|-------------|
| pgvector (PostgreSQL) | Requires a new service on Fly.io ($7-15/month for Fly Postgres, or $15-50/month for managed). Adds operational complexity. Overkill for our corpus size. |
| ChromaDB | Python-native, easy to use, but no persistent storage story on Fly.io without volumes. Would need to rebuild index on every cold start. |
| Pinecone | Managed service, excellent DX, but $70+/month for production. Violates "keep costs low" constraint. Vendor lock-in. |
| Qdrant | Strong engine, but same problem as pgvector: new service to run. Docker sidecar on Fly.io adds complexity and cost. |
| SQLite + sqlite-vss | Would work but sqlite-vss is less mature than DuckDB vss. We are already DuckDB-native. |

### 2b. Embedding Model: Voyage-3-large

**Decision:** Voyage-3-large (1024 dimensions, 16K token context)

**Rationale:**
- CLAUDE.md already specifies `Voyage-3-large` as the target embedding model.
- Voyage-3-large ranks #1 or #2 on MTEB retrieval benchmarks for legal/regulatory text.
- 1024 dimensions is a good balance: smaller than OpenAI's 3072-dim `text-embedding-3-large` (saves 3x storage) while maintaining strong retrieval quality.
- Voyage offers a batch API for offline embedding (cheaper than one-at-a-time).
- Anthropic partnership means likely future integration improvements.
- 16K token context window handles long policy document chunks without truncation.

**Cost estimate:**
- Voyage-3-large: $0.06 per million tokens
- Typical SPA: ~15 pages, ~5,000 words = ~6,500 tokens
- 5,000 documents = ~32.5M tokens = **~$1.95** for initial corpus embedding
- Query embedding: negligible ($0.06 per 1M tokens, queries are ~50 tokens each)

**Alternative considered:**
- `Voyage-3-lite` ($0.02/M tokens, 512 dims): 3x cheaper but lower retrieval quality on legal text. Not worth the tradeoff for a corpus this small.

### 2c. Chunking Strategy

**Decision:** Section-aware sliding window with overlap

Policy documents (SPAs, waivers, CIBs) have a known structure:
- **SPAs:** Standard form with numbered sections (4.19-A, 4.19-B, etc.), attachments, rate tables
- **1115 Waivers:** STCs (Special Terms and Conditions) with numbered provisions
- **CIBs/SHOs:** Letter format with subject line, effective date, regulatory citations
- **Federal Register:** Standard FR format with preamble, regulatory text, tables

**Chunking parameters:**
```
chunk_size:     800 tokens (~600 words)
chunk_overlap:  200 tokens (~150 words)
min_chunk_size: 100 tokens (discard smaller fragments)
```

**Why 800 tokens:**
- Large enough to capture a complete policy provision or rate methodology section
- Small enough that 10 retrieved chunks (~8,000 tokens) fit comfortably in Claude's context alongside system prompt and conversation history
- Voyage-3-large's 16K window means we never truncate a chunk during embedding

**Section-aware splitting:**
1. First, split on document section boundaries (headers, numbered sections, page breaks)
2. Within each section, apply sliding window if section exceeds `chunk_size`
3. Preserve section title as metadata on every chunk (critical for retrieval context)
4. Never split mid-sentence — find nearest sentence boundary

**Metadata per chunk:**
```python
{
    "chunk_id": "spa-FL-2025-0003-c004",
    "doc_id": "spa-FL-2025-0003",
    "doc_type": "spa",              # spa | waiver | cib | sho | federal_register
    "state_code": "FL",
    "title": "FL SPA 25-0003: Physician Fee Schedule Update",
    "section_title": "Attachment 4.19-B: Rate-Setting Methodology",
    "chunk_index": 4,
    "page_start": 7,
    "page_end": 8,
    "text": "The Agency shall reimburse physicians...",
    "token_count": 743,
    "effective_date": "2025-07-01",
    "approval_date": "2025-06-15",
    "source_url": "https://www.medicaid.gov/...",
}
```

### 2d. Retrieval Strategy: Hybrid (Vector + BM25 + Metadata Filtering)

**Decision:** Reciprocal Rank Fusion (RRF) combining vector search and BM25 keyword search, with metadata pre-filtering.

**Why hybrid:**
- Vector search alone misses exact regulatory citations (e.g., "42 CFR 447.203" or "SPA 25-0003")
- BM25 alone misses semantic matches (e.g., query "rate adequacy" should match chunks about "payment sufficiency")
- RRF is simple, effective, and does not require training a re-ranker model

**Retrieval pipeline:**
```
User query: "What is Florida's physician fee schedule methodology?"
         │
         ▼
    ┌─ Pre-filter ─┐
    │ doc_type IN   │  (optional: user or Claude can specify)
    │ state_code =  │
    │ date range    │
    └───────┬───────┘
            │
    ┌───────┴────────┐
    │                │
    ▼                ▼
 Vector           BM25
 Search           Search
 (top 20)         (top 20)
    │                │
    └───────┬────────┘
            │
            ▼
    Reciprocal Rank
    Fusion (k=60)
            │
            ▼
    Top 10 chunks
    returned to Claude
```

**RRF formula:**
```
score(chunk) = sum over each ranking list:
    1 / (k + rank_in_list)

where k = 60 (standard constant that balances high-ranked and low-ranked results)
```

**Why not a re-ranker:**
- Re-rankers (like Cohere Rerank or cross-encoder models) add latency and cost
- For our corpus size (<200K chunks), RRF over vector+BM25 is sufficient
- Can add re-ranking later if retrieval quality is insufficient

---

## 3. Data Sources: What Documents to Ingest

### 3a. Existing Aradune Data

| Source | What we have | Rows | Location |
|--------|-------------|------|----------|
| SPAs | Metadata (title, date, state, topic) | Unknown (fact_spa table) | `data/lake/fact/spa/` |
| 1115 Waivers | Metadata (647 waivers) | 647 | `data/lake/reference/ref_1115_waivers.parquet` |
| SPA extracts | Scraped metadata | Unknown | `tools/mfs_scraper/spa_data/spa_tracker.csv` |
| SPA PDF extract | Extraction script exists | N/A | `tools/mfs_scraper/spa_pdf_extract.py` |

**What we do NOT have:** The actual full-text content of SPAs, waivers, CIBs, or SHO letters. We have metadata records pointing to documents, but not the document text or embeddings.

### 3b. Documents to Acquire

| Document Type | Source | Estimated Volume | Acquisition Method |
|---------------|--------|-----------------|-------------------|
| **State Plan Amendments (SPAs)** | CMS MACPro / medicaid.gov | ~3,000-5,000 active | Scrape medicaid.gov SPA search; download linked PDFs |
| **1115 Waivers (full text)** | medicaid.gov waiver portal | ~300-500 active + extensions | Download from waiver detail pages (have 647 metadata records) |
| **CMS Informational Bulletins (CIBs)** | medicaid.gov/federal-policy-guidance | ~200-400 (2010-present) | Scrape CIB listing page; most are HTML or short PDFs |
| **State Health Official (SHO) Letters** | medicaid.gov/federal-policy-guidance | ~200-400 (2010-present) | Same as CIBs — listed together |
| **State Medicaid Director (SMD) Letters** | medicaid.gov | ~100-200 | Historical, mostly pre-2015 |
| **Federal Register Medicaid rules** | federalregister.gov API | ~500-1,000 relevant | FR API is excellent (JSON, full text, free) |

**Total estimated corpus:** 5,000-7,000 documents, ~50,000-100,000 pages

**Priority order:**
1. **CIBs + SHO letters** (easiest: structured, mostly HTML, small volume, high value)
2. **1115 Waivers** (medium: have metadata, need full PDFs)
3. **SPAs** (hardest: high volume, need PDFs, many are scanned images)
4. **Federal Register** (easy acquisition via API but lower per-document value)

### 3c. CMS MACPro API

CMS has a MACPro API for State Plan and waiver data. Key endpoints:
- `https://www.medicaid.gov/search-api/spa` — SPA search (already used by `spa_scraper.py`)
- Waiver documents are linked from `medicaid.gov/medicaid/section-1115-demonstrations`
- CIBs/SHOs at `medicaid.gov/federal-policy-guidance/federal-policy-guidance`

The Federal Register has a proper REST API:
- `https://www.federalregister.gov/api/v1/documents.json?conditions[agencies][]=centers-for-medicare-medicaid-services&conditions[type][]=RULE&per_page=100`

---

## 4. Integration with Intelligence Endpoint

### 4a. New Tool: `search_policy_corpus`

Add to `CUSTOM_TOOLS` in `server/routes/intelligence.py`:

```python
{
    "name": "search_policy_corpus",
    "description": (
        "Search the Medicaid policy document corpus using semantic and keyword search. "
        "Covers State Plan Amendments (SPAs), 1115 waivers, CMS Informational Bulletins (CIBs), "
        "SHO letters, and Federal Register Medicaid rules. "
        "Returns relevant text excerpts with source citations. "
        "Use this when the user asks about Medicaid policy, regulations, methodology, "
        "or specific state plan provisions."
    ),
    "input_schema": {
        "type": "object",
        "properties": {
            "query": {
                "type": "string",
                "description": "Natural language search query about Medicaid policy"
            },
            "doc_types": {
                "type": "array",
                "items": {"type": "string", "enum": ["spa", "waiver", "cib", "sho", "federal_register"]},
                "description": "Optional: filter by document type"
            },
            "states": {
                "type": "array",
                "items": {"type": "string"},
                "description": "Optional: filter by state codes (e.g., ['FL', 'TX'])"
            },
            "top_k": {
                "type": "integer",
                "description": "Number of results to return (default 10, max 20)",
                "default": 10
            },
        },
        "required": ["query"],
    },
}
```

### 4b. Tool Execution

Add to `_execute_tool()` in `intelligence.py`:

```python
elif name == "search_policy_corpus":
    from server.engines.rag_engine import hybrid_search
    results = hybrid_search(
        query=inp["query"],
        doc_types=inp.get("doc_types"),
        states=inp.get("states"),
        top_k=inp.get("top_k", 10),
    )
    return json.dumps(results, default=str)
```

### 4c. System Prompt Addition

Add to the `SYSTEM_PROMPT` in `intelligence.py`:

```
## Policy Corpus Search
You have access to the full text of Medicaid policy documents:
- State Plan Amendments (SPAs): rate methodologies, covered services, eligibility
- 1115 Waivers: demonstration programs, STCs, evaluation designs
- CMS Informational Bulletins (CIBs): federal guidance on Medicaid topics
- SHO Letters: State Health Official directives from CMS
- Federal Register: Final rules and proposed rules affecting Medicaid

When a user asks about Medicaid policy, regulations, or state-specific methodology:
1. Use search_policy_corpus to find relevant document excerpts
2. Cross-reference with the data lake (e.g., look up the state's current rates alongside its rate methodology SPA)
3. Cite specific documents: "According to FL SPA 25-0003, Attachment 4.19-B..."
4. Note document dates — older SPAs may have been superseded
```

---

## 5. DuckDB Schema for Policy Corpus

### 5a. Tables (stored as Parquet in the lake)

```sql
-- Document-level metadata
-- Stored at: data/lake/fact/policy_document/snapshot=YYYY-MM-DD/data.parquet
CREATE TABLE fact_policy_document (
    doc_id          VARCHAR PRIMARY KEY,   -- e.g., "spa-FL-2025-0003"
    doc_type        VARCHAR NOT NULL,       -- spa, waiver, cib, sho, federal_register
    state_code      VARCHAR,                -- NULL for federal docs (CIBs, FRs)
    title           VARCHAR NOT NULL,
    doc_number      VARCHAR,                -- SPA number, waiver number, FR doc number
    effective_date  DATE,
    approval_date   DATE,
    publication_date DATE,
    status          VARCHAR,                -- active, superseded, withdrawn, pending
    source_url      VARCHAR,
    pdf_path        VARCHAR,                -- local path to downloaded PDF
    page_count      INTEGER,
    summary         VARCHAR,                -- Claude-generated 2-3 sentence summary
    topics          VARCHAR,                -- comma-separated topic tags
    chunk_count     INTEGER,
    ingested_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Chunk-level data with embeddings
-- Stored at: data/lake/fact/policy_chunk/snapshot=YYYY-MM-DD/data.parquet
CREATE TABLE fact_policy_chunk (
    chunk_id        VARCHAR PRIMARY KEY,   -- e.g., "spa-FL-2025-0003-c004"
    doc_id          VARCHAR NOT NULL,       -- FK to policy_document
    doc_type        VARCHAR NOT NULL,       -- denormalized for filter efficiency
    state_code      VARCHAR,                -- denormalized for filter efficiency
    chunk_index     INTEGER NOT NULL,
    text            VARCHAR NOT NULL,
    section_title   VARCHAR,
    page_start      INTEGER,
    page_end        INTEGER,
    token_count     INTEGER,
    embedding       FLOAT[1024]             -- Voyage-3-large embedding vector
);
```

### 5b. DuckDB Extensions Required

```sql
-- Vector similarity search
INSTALL vss;
LOAD vss;

-- Full-text search (BM25)
INSTALL fts;
LOAD fts;

-- Create HNSW index for vector search
CREATE INDEX policy_chunk_vec_idx ON fact_policy_chunk
USING HNSW (embedding) WITH (metric = 'cosine');

-- Create FTS index for keyword search
PRAGMA create_fts_index('fact_policy_chunk', 'chunk_id', 'text', 'section_title');
```

### 5c. Registration in db.py

Add to `FACT_NAMES` in `server/db.py`:
```python
"policy_document",
"policy_chunk",
```

And add extension loading in `_register_all_views()`:
```python
# Load extensions for RAG
try:
    _conn.execute("INSTALL vss; LOAD vss;")
    _conn.execute("INSTALL fts; LOAD fts;")
except Exception:
    pass  # Extensions may not be available in all environments
```

---

## 6. Embedding Pipeline Design

### 6a. Script: `scripts/build_lake_policy_corpus.py`

Follows the standard lake ingestion pattern (like `build_lake_cms.py`):

```python
# Pseudocode — actual implementation in a future session

def main():
    # Phase 1: Acquire documents
    docs = []
    docs += scrape_cibs_shos()        # ~400 docs, HTML/PDF
    docs += scrape_waivers()          # ~500 docs, PDF
    docs += scrape_spas()             # ~3,000 docs, PDF
    docs += fetch_federal_register()  # ~500 docs, JSON/HTML

    # Phase 2: Extract text
    for doc in docs:
        if doc.format == "html":
            doc.text = extract_html_text(doc.raw)
        elif doc.format == "pdf":
            doc.text = extract_pdf_text(doc.path)  # pdfplumber
            if doc.text_quality < 0.7:  # OCR/scanned
                doc.text = claude_extract(doc.path)  # Claude API

    # Phase 3: Chunk
    all_chunks = []
    for doc in docs:
        chunks = section_aware_chunk(
            doc.text,
            chunk_size=800,
            overlap=200,
            doc_metadata=doc.metadata,
        )
        all_chunks.extend(chunks)

    # Phase 4: Embed (batch)
    embeddings = voyage_embed_batch(
        texts=[c.text for c in all_chunks],
        model="voyage-3-large",
        batch_size=128,
    )
    for chunk, emb in zip(all_chunks, embeddings):
        chunk.embedding = emb

    # Phase 5: Write Parquet
    write_parquet("data/lake/fact/policy_document/", docs)
    write_parquet("data/lake/fact/policy_chunk/", all_chunks)

    # Phase 6: Sync to R2
    sync_to_r2()
```

### 6b. PDF Extraction Strategy

Three tiers:
1. **Clean digital PDFs** (most CIBs, recent SPAs): `pdfplumber` extracts text directly
2. **Complex layout PDFs** (rate tables, multi-column): `pdfplumber` + post-processing to reassemble tables
3. **Scanned/image PDFs** (older SPAs): Claude API vision for OCR + structure extraction

**Quality check:** If `pdfplumber` extracts <100 characters from a page that clearly has content, flag for Claude API extraction.

**Cost for Claude extraction:** ~$0.015 per page (Haiku for simple OCR, Sonnet for complex layout). Worst case 50,000 pages needing Claude = ~$750. Realistic: <10% of pages need Claude = ~$75.

### 6c. Voyage API Batch Embedding

```python
import voyageai

client = voyageai.Client(api_key=os.environ["VOYAGE_API_KEY"])

def embed_batch(texts: list[str], batch_size: int = 128) -> list[list[float]]:
    """Embed texts in batches. Voyage supports up to 128 texts per call."""
    all_embeddings = []
    for i in range(0, len(texts), batch_size):
        batch = texts[i:i + batch_size]
        result = client.embed(
            batch,
            model="voyage-3-large",
            input_type="document",  # "query" for search queries
        )
        all_embeddings.extend(result.embeddings)
    return all_embeddings
```

---

## 7. RAG Engine Design

### 7a. Core Search Function

`server/engines/rag_engine.py`:

```python
"""
Hybrid retrieval engine: vector search + BM25 keyword search + RRF fusion.
Operates on DuckDB policy_chunk table with vss and fts extensions.
"""

import os
import voyageai
from server.db import get_cursor

_voyage_client = None

def _get_voyage():
    global _voyage_client
    if _voyage_client is None:
        _voyage_client = voyageai.Client(api_key=os.environ["VOYAGE_API_KEY"])
    return _voyage_client


def hybrid_search(
    query: str,
    doc_types: list[str] | None = None,
    states: list[str] | None = None,
    top_k: int = 10,
) -> dict:
    """
    Hybrid search: vector + BM25 + RRF fusion.
    Returns top_k chunks with metadata and scores.
    """
    # 1. Embed the query
    voyage = _get_voyage()
    query_embedding = voyage.embed(
        [query], model="voyage-3-large", input_type="query"
    ).embeddings[0]

    # 2. Build metadata filters
    filters = []
    params = []
    if doc_types:
        placeholders = ", ".join(["?"] * len(doc_types))
        filters.append(f"doc_type IN ({placeholders})")
        params.extend(doc_types)
    if states:
        placeholders = ", ".join(["?"] * len(states))
        filters.append(f"state_code IN ({placeholders})")
        params.extend(states)
    where_clause = " AND ".join(filters) if filters else "1=1"

    retrieve_n = top_k * 3  # Over-retrieve for RRF

    with get_cursor() as cur:
        # 3. Vector search (cosine similarity via vss)
        vec_sql = f"""
            SELECT chunk_id, doc_id, text, section_title,
                   page_start, state_code, doc_type,
                   array_cosine_similarity(embedding, ?::FLOAT[1024]) AS score
            FROM fact_policy_chunk
            WHERE {where_clause}
            ORDER BY score DESC
            LIMIT {retrieve_n}
        """
        vec_results = cur.execute(vec_sql, [query_embedding] + params).fetchall()

        # 4. BM25 keyword search (fts extension)
        bm25_sql = f"""
            SELECT chunk_id, doc_id, text, section_title,
                   page_start, state_code, doc_type,
                   fts_main_fact_policy_chunk.match_bm25(chunk_id, ?) AS score
            FROM fact_policy_chunk
            WHERE {where_clause}
              AND score IS NOT NULL
            ORDER BY score DESC
            LIMIT {retrieve_n}
        """
        bm25_results = cur.execute(bm25_sql, [query] + params).fetchall()

    # 5. Reciprocal Rank Fusion
    rrf_scores = {}
    k = 60  # RRF constant

    for rank, row in enumerate(vec_results):
        chunk_id = row[0]
        rrf_scores[chunk_id] = rrf_scores.get(chunk_id, {"score": 0, "data": row})
        rrf_scores[chunk_id]["score"] += 1 / (k + rank + 1)

    for rank, row in enumerate(bm25_results):
        chunk_id = row[0]
        if chunk_id not in rrf_scores:
            rrf_scores[chunk_id] = {"score": 0, "data": row}
        rrf_scores[chunk_id]["score"] += 1 / (k + rank + 1)

    # 6. Sort by RRF score, return top_k
    ranked = sorted(rrf_scores.values(), key=lambda x: x["score"], reverse=True)[:top_k]

    chunks = []
    for item in ranked:
        row = item["data"]
        chunks.append({
            "chunk_id": row[0],
            "doc_id": row[1],
            "text": row[2],
            "section_title": row[3],
            "page": row[4],
            "state_code": row[5],
            "doc_type": row[6],
            "score": round(item["score"], 4),
        })

    return {
        "query": query,
        "chunks": chunks,
        "total_results": len(chunks),
        "filters_applied": {
            "doc_types": doc_types,
            "states": states,
        },
    }
```

---

## 8. Cost Estimate

### 8a. One-Time Setup Costs

| Item | Cost | Notes |
|------|------|-------|
| Voyage-3-large embedding (initial corpus) | ~$2-5 | ~50M tokens at $0.06/M |
| Claude PDF extraction (scanned pages) | ~$75 | ~5,000 pages needing OCR at ~$0.015/page |
| Developer time | N/A | Internal |
| **Total one-time** | **~$80** | |

### 8b. Ongoing Costs (monthly)

| Item | Cost/month | Notes |
|------|-----------|-------|
| Voyage-3-large query embeddings | ~$0.10 | ~1,000 queries/month at 50 tokens each |
| Fly.io (no change) | $0 incremental | Same 2GB instance, policy data fits in existing Parquet budget |
| R2 storage (incremental) | ~$0.05 | ~50MB additional Parquet for policy corpus |
| Claude API (Intelligence endpoint) | Already budgeted | search_policy_corpus is a tool call within existing Claude budget |
| New document ingestion | ~$1-2 | ~100 new docs/month, re-embedding |
| **Total incremental** | **~$2/month** | |

### 8c. Infrastructure Impact

| Resource | Current | After RAG | OK? |
|----------|---------|-----------|-----|
| Fly.io RAM | ~1.5GB used (250 tables) | ~1.8-2.0GB (+ embeddings + indexes) | Tight but OK at 2GB. Monitor. May need 4GB upgrade ($14/mo). |
| Fly.io disk | Pre-baked in Docker image | +50-200MB for policy Parquet | OK, Docker image grows slightly |
| R2 storage | ~785MB | +50-200MB | OK, R2 free tier is 10GB |
| Cold start time | ~5s (pre-baked) | +2-3s (load vss/fts extensions + indexes) | Acceptable |

---

## 9. Implementation Phases

### Phase 0: Prerequisites (1 session)
- [ ] Verify DuckDB `vss` extension works in our environment (macOS local + Linux Docker)
- [ ] Verify DuckDB `fts` extension works
- [ ] Get Voyage API key, test embedding a few documents
- [ ] Test end-to-end: embed 10 chunks, store in DuckDB, run vector search
- [ ] Measure memory usage of 10K embeddings in DuckDB

### Phase 1: CIBs + SHO Letters (1-2 sessions)
**Why first:** Smallest volume (~400 docs), mostly HTML (no PDF extraction needed), structured format, high value.

- [ ] Scrape CIB/SHO listing from medicaid.gov
- [ ] Extract full text (HTML parsing, minimal PDF)
- [ ] Chunk and embed
- [ ] Write Parquet to lake
- [ ] Add `search_policy_corpus` tool to Intelligence endpoint
- [ ] Test hybrid search quality
- [ ] Deploy to Fly.io

**Deliverable:** "Ask about any CMS guidance letter" works in the Intelligence chat.

### Phase 2: 1115 Waivers (1-2 sessions)
**Why second:** Have 647 metadata records, need to download PDFs and extract text.

- [ ] Download waiver PDFs from medicaid.gov links
- [ ] Extract text (pdfplumber + Claude for complex PDFs)
- [ ] Chunk with section awareness (STCs have numbered provisions)
- [ ] Embed and add to existing Parquet
- [ ] Test cross-referencing: "Compare FL's 1115 waiver HCBS provisions to TX"

**Deliverable:** Full-text search over all active 1115 waivers.

### Phase 3: State Plan Amendments (2-3 sessions)
**Why third:** Highest volume, many scanned PDFs, but highest value for the platform.

- [ ] Extend `spa_scraper.py` to download PDFs (not just metadata)
- [ ] Build PDF extraction pipeline with quality tiers
- [ ] Focus on rate-setting SPAs first (Attachment 4.19-B) — most relevant to CPRA
- [ ] Embed and add to corpus
- [ ] Test: "What is Florida's physician fee schedule methodology per its State Plan?"

**Deliverable:** Rate-methodology SPAs searchable for all states with fee schedules.

### Phase 4: Federal Register + Ongoing Ingestion (1 session)
- [ ] Fetch Medicaid-related FR notices via API
- [ ] Set up incremental ingestion (detect new documents, chunk, embed, append)
- [ ] Build a simple monitoring dashboard: corpus size, embedding coverage, search quality metrics
- [ ] Add "Policy Search" as a standalone tool in the frontend (not just Intelligence)

**Deliverable:** Continuously updated policy corpus. New CIBs/SPAs auto-ingested within 24 hours.

### Phase 5: Advanced Features (future)
- [ ] Citation linking: click a search result to view the original PDF at the right page
- [ ] Cross-reference: automatically link policy changes to rate impacts in the data lake
- [ ] Timeline view: show how a state's rate methodology evolved through successive SPAs
- [ ] Notification: alert when a new SPA/CIB affects a user's tracked states
- [ ] Re-ranker: add Cohere Rerank or cross-encoder if retrieval quality needs improvement

---

## 10. Risks and Mitigations

| Risk | Likelihood | Impact | Mitigation |
|------|-----------|--------|-----------|
| DuckDB `vss` extension not stable enough | Low | High | Tested in DuckDB 1.0+. Fallback: store embeddings in Parquet, do brute-force cosine in numpy (slower but works for <200K chunks). |
| Fly.io 2GB RAM not enough for embeddings | Medium | Medium | Monitor memory. 100K chunks at 1024 dims = ~400MB. If too tight, upgrade to 4GB ($14/mo) or use quantized embeddings (int8 = 4x smaller). |
| Scanned/image SPAs resist extraction | Medium | Medium | Claude API vision handles most scanned text. Budget $75 for OCR. Worst case: index metadata only for those docs. |
| CMS changes their website structure | Medium | Low | scraper already handles current structure. Pin URLs, add retry logic, alert on scrape failures. |
| Voyage API availability/pricing changes | Low | Medium | Embeddings are computed offline and stored. Only query embedding is real-time. Could switch to OpenAI embeddings or local model (e5-large) if needed. |
| Low retrieval quality for regulatory text | Low-Medium | High | Hybrid search (vector + BM25) mitigates this. If still insufficient, add a cross-encoder re-ranker (Phase 5). |

---

## 11. Dependencies and Environment Variables

### New Python packages (add to `server/requirements.txt`):
```
voyageai>=0.3.0       # Voyage embedding API client
pdfplumber>=0.11.0    # PDF text extraction (already used elsewhere)
```

### New environment variables:
```
VOYAGE_API_KEY=voyage-...    # Voyage AI API key for embeddings
```

Set on:
- Local `.env` (gitignored)
- Fly.io: `fly secrets set VOYAGE_API_KEY=...`
- Vercel: not needed (RAG runs server-side only)

### DuckDB extensions (auto-installed at runtime):
```
vss    — vector similarity search (HNSW indexes)
fts    — full-text search (BM25)
```

---

## 12. Key Design Decisions Summary

| Decision | Choice | Primary reason |
|----------|--------|---------------|
| Vector store | DuckDB (in-process) | No new service, fits existing architecture, 2GB RAM budget |
| Embedding model | Voyage-3-large (1024 dims) | CLAUDE.md specifies it; best retrieval quality for regulatory text |
| Chunking | 800 tokens, section-aware, 200 overlap | Balances context completeness vs. retrieval precision |
| Retrieval | Hybrid (vector + BM25 + RRF) | Handles both semantic and exact-match queries |
| First documents | CIBs + SHO letters | Smallest volume, highest signal, easiest acquisition |
| Integration point | Intelligence endpoint tool call | Leverages existing agentic loop; Claude decides when to search policy |
| Storage | Parquet in data lake | Same as all other data; syncs to R2; pre-baked in Docker image |

---

## 13. Open Questions (Resolve Before Implementation)

1. **DuckDB `vss` extension on Fly.io Linux:** Needs testing. The extension must be available in the `python:3.12-slim` Docker base image. May need `INSTALL vss` at startup or pre-install in Dockerfile.

2. **Memory budget for embeddings:** 100K chunks x 1024 dims x 4 bytes = 400MB. With DuckDB overhead (HNSW index), could reach 600MB. Current lake uses ~1.5GB. Total: ~2.1GB. Tight on 2GB Fly.io instance. Options: (a) upgrade to 4GB, (b) use quantized int8 embeddings (100MB instead of 400MB), (c) keep chunk count under 50K initially.

3. **Voyage API key cost management:** Voyage charges by token. Initial corpus embedding is cheap (~$5). But if we re-embed frequently (e.g., new chunking strategy), costs could add up. Solution: cache embeddings in Parquet, only embed new/changed documents.

4. **How to handle superseded SPAs:** A state may have 20+ SPAs that modify the same section (4.19-B). Should we index all versions or only the most recent? **Recommendation:** Index all, but add a `status` field (active/superseded) and boost active documents in retrieval scoring.

5. **Frontend integration:** The Intelligence endpoint exists but has no frontend component yet (known gap from session 9). RAG search should ship with the Intelligence frontend. Should we build the Intelligence chat UI as part of Phase 1, or add RAG to the existing Policy Analyst (`api/chat.js`)?
   **Recommendation:** Build as a tool in Intelligence (server-side Claude with DuckDB tools), not in the legacy Policy Analyst (Vercel serverless). The Intelligence endpoint already has the agentic loop, tool execution, and streaming.

---

*This plan positions RAG as a natural extension of the existing architecture: same DuckDB engine, same Parquet lake, same Intelligence endpoint, same deployment pipeline. The total incremental cost is under $100 for setup and under $5/month for operation. The primary investment is developer time for document acquisition and extraction.*
