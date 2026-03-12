"""
Aradune Intelligence — unified AI interface for Medicaid data analysis.

Claude Sonnet 4.6 with extended thinking, DuckDB tool access, web search,
smart routing (general knowledge vs data queries), response caching,
and real-time progress tracking via SSE.
"""

import json
import hashlib
import os
import re
import time
from collections import OrderedDict
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field

import anthropic

from server.db import get_cursor
from server.middleware.auth import require_clerk_auth

router = APIRouter(prefix="/api/intelligence", tags=["intelligence"])

# ---------------------------------------------------------------------------
# SQL safety
# ---------------------------------------------------------------------------

_FORBIDDEN = re.compile(
    r"\b(INSERT|UPDATE|DELETE|DROP|ALTER|TRUNCATE|CREATE|REPLACE|GRANT|REVOKE|ATTACH|COPY|EXPORT|IMPORT|LOAD|INSTALL)\b",
    re.IGNORECASE,
)


def _validate_sql(sql: str) -> str:
    sql = sql.strip().rstrip(";")
    sql = re.sub(r";.+", "", sql, flags=re.DOTALL)
    if not sql.upper().startswith("SELECT") and not sql.upper().startswith("WITH"):
        raise ValueError("Only SELECT/WITH queries allowed")
    if _FORBIDDEN.search(sql):
        raise ValueError("Forbidden SQL keyword")
    if "LIMIT" not in sql.upper():
        sql += " LIMIT 100"
    return sql


def _run_query(sql: str) -> dict:
    """Execute validated SQL, return {columns, rows, row_count, ms}."""
    sql = _validate_sql(sql)
    t0 = time.time()
    with get_cursor() as cur:
        result = cur.execute(sql).fetchall()
        columns = [desc[0] for desc in cur.description]
    ms = int((time.time() - t0) * 1000)
    rows = []
    for row in result:
        r = {}
        for k, v in zip(columns, row):
            if hasattr(v, "isoformat"):
                r[k] = v.isoformat()
            elif v is not None and not isinstance(v, (str, int, float, bool)):
                r[k] = str(v)
            else:
                r[k] = v
        rows.append(r)
    return {"columns": columns, "rows": rows, "row_count": len(rows), "query_ms": ms}


# ---------------------------------------------------------------------------
# Tool definitions for Claude
# ---------------------------------------------------------------------------

CUSTOM_TOOLS = [
    {
        "name": "query_database",
        "description": (
            "Run a SELECT-only SQL query against the Aradune DuckDB data lake. "
            "Contains 556+ Medicaid fact tables with 305M+ rows. "
            "Always include a LIMIT clause (max 200). "
            "Use DuckDB syntax (ILIKE for case-insensitive, :: for casts)."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "sql": {"type": "string", "description": "A SELECT query in DuckDB SQL dialect"},
                "purpose": {"type": "string", "description": "Brief note on what this query checks"},
            },
            "required": ["sql"],
        },
    },
    {
        "name": "list_tables",
        "description": (
            "List all available tables in the data lake with their row counts. "
            "Use this to discover what data is available before querying."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "filter": {"type": "string", "description": "Optional keyword to filter table names (e.g. 'hospital', 'drug', 'enrollment')"},
            },
        },
    },
    {
        "name": "describe_table",
        "description": "Get column names and types for a specific table.",
        "input_schema": {
            "type": "object",
            "properties": {
                "table_name": {"type": "string", "description": "Table name (e.g. fact_rate_comparison, dim_state)"},
            },
            "required": ["table_name"],
        },
    },
    {
        "name": "search_policy_corpus",
        "description": (
            "Search the Medicaid policy document corpus using semantic and keyword search. "
            "Covers CMS Informational Bulletins (CIBs), SHO letters, State Plan Amendments (SPAs), "
            "1115 waivers, and Federal Register Medicaid rules. "
            "Returns relevant text excerpts with source citations. "
            "Use this when the user asks about Medicaid policy, regulations, methodology, "
            "or specific state plan provisions."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "query": {
                    "type": "string",
                    "description": "Natural language search query about Medicaid policy",
                },
                "doc_types": {
                    "type": "array",
                    "items": {"type": "string", "enum": ["spa", "waiver", "cib", "sho", "federal_register"]},
                    "description": "Optional: filter by document type",
                },
                "states": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "Optional: filter by state codes (e.g., ['FL', 'TX'])",
                },
                "top_k": {
                    "type": "integer",
                    "description": "Number of results to return (default 10, max 20)",
                    "default": 10,
                },
            },
            "required": ["query"],
        },
    },
]

WEB_SEARCH_TOOL = {"type": "web_search_20250305", "name": "web_search", "max_uses": 3}
TOOLS = CUSTOM_TOOLS + [WEB_SEARCH_TOOL]


def _execute_tool(name: str, inp: dict) -> str:
    """Execute a tool call and return JSON string result."""
    try:
        if name == "query_database":
            result = _run_query(inp["sql"])
            if result["row_count"] > 50:
                result["rows"] = result["rows"][:50]
                result["truncated"] = True
                result["note"] = f"Showing first 50 of {result['row_count']} rows"
            return json.dumps(result, default=str)

        elif name == "list_tables":
            filt = inp.get("filter", "").lower()
            with get_cursor() as cur:
                rows = cur.execute("""
                    SELECT table_name, estimated_size
                    FROM duckdb_tables()
                    WHERE table_schema = 'main'
                    ORDER BY table_name
                """).fetchall()
                tables = []
                for name_val, size in rows:
                    if filt and filt not in name_val.lower():
                        continue
                    try:
                        cnt = cur.execute(f"SELECT COUNT(*) FROM {name_val}").fetchone()[0]
                    except Exception:
                        cnt = None
                    tables.append({"table": name_val, "rows": cnt})
            return json.dumps({"tables": tables, "total": len(tables)}, default=str)

        elif name == "describe_table":
            tbl = inp["table_name"]
            if not re.match(r"^[a-zA-Z_][a-zA-Z0-9_]*$", tbl):
                return json.dumps({"error": "Invalid table name"})
            with get_cursor() as cur:
                cols = cur.execute(f"DESCRIBE {tbl}").fetchall()
            return json.dumps({
                "table": tbl,
                "columns": [{"name": c[0], "type": c[1]} for c in cols],
            })

        elif name == "search_policy_corpus":
            from server.engines.rag_engine import hybrid_search
            results = hybrid_search(
                query=inp["query"],
                doc_types=inp.get("doc_types"),
                states=inp.get("states"),
                top_k=inp.get("top_k", 10),
            )
            return json.dumps(results, default=str)

        if name == "web_search":
            return json.dumps({"note": "Web search handled by API"})
        return json.dumps({"error": f"Unknown tool: {name}"})

    except Exception as e:
        return json.dumps({"error": str(e)})


# ---------------------------------------------------------------------------
# Response cache (in-memory, LRU)
# ---------------------------------------------------------------------------

_cache: OrderedDict[str, dict] = OrderedDict()
_CACHE_MAX = 200
_CACHE_TTL = 3600 * 6  # 6 hours

# Load pre-seeded cache from JSON (baked into Docker image)
_CACHE_SEED_PATH = os.path.join(os.path.dirname(__file__), "..", "cache_seeds.json")
try:
    if os.path.exists(_CACHE_SEED_PATH):
        with open(_CACHE_SEED_PATH) as f:
            _seeds = json.load(f)
        for entry in _seeds:
            _cache[entry["key"]] = {
                "response": entry["response"],
                "tool_calls": entry.get("tool_calls", []),
                "queries": entry.get("queries", []),
                "cached_at": time.time(),
                "hits": 0,
            }
        print(f"Loaded {len(_seeds)} pre-seeded cache entries.", flush=True)
except Exception as e:
    print(f"Cache seed load error (non-fatal): {e}", flush=True)


def _cache_key(message: str) -> str:
    """Normalize a question and return a stable cache key."""
    normalized = message.strip().lower()
    normalized = re.sub(r'[^\w\s]', '', normalized)
    normalized = re.sub(r'\s+', ' ', normalized).strip()
    return hashlib.sha256(normalized.encode()).hexdigest()[:20]


def _cache_get(key: str) -> dict | None:
    """Get a cached response, or None if expired/missing."""
    if key in _cache:
        entry = _cache[key]
        if time.time() - entry["cached_at"] < _CACHE_TTL:
            _cache.move_to_end(key)
            entry["hits"] = entry.get("hits", 0) + 1
            return entry
        else:
            del _cache[key]
    return None


def _cache_set(key: str, response: str, tool_calls: list, queries: list):
    """Store a response in the cache."""
    _cache[key] = {
        "response": response,
        "tool_calls": tool_calls,
        "queries": queries,
        "cached_at": time.time(),
        "hits": 0,
    }
    if len(_cache) > _CACHE_MAX:
        _cache.popitem(last=False)


# ---------------------------------------------------------------------------
# System prompt
# ---------------------------------------------------------------------------

_SYSTEM_PROMPT_STATIC = """You are Aradune, an AI analyst with direct access to the most comprehensive public Medicaid data lake ever assembled: 556+ tables, 305M+ rows, covering rates, enrollment, hospitals, quality, workforce, pharmacy, expenditure, behavioral health, and economics for all 54 jurisdictions, organized into 18 domains.

## How to Handle Questions

Assess each question before responding:

**General knowledge** -- If the question is about Medicaid policy, regulations, history, definitions, or concepts that don't require specific numbers from the data lake, answer directly from your knowledge. Do NOT use tools for questions like "What is EPSDT?", "How does the unwinding work?", or "Explain 42 CFR 447.203." Just answer clearly and thoroughly.

**Data-driven** -- If the question asks for specific numbers, state rankings, trends, comparisons, or anything that requires real data, use your tools to query the data lake. Examples: "Which states pay below 50% of Medicare?", "Show me Florida's enrollment trend."

**Hybrid** -- If the question needs both policy context and data, answer the conceptual part from knowledge, then use tools to provide supporting data. Example: "Tell me about school-based services in Florida vs the rest of the US" -- explain what school-based services are, then query for any relevant claims or enrollment data.

If the data needed to fully answer a question is not in the lake, say so clearly and provide what context you can from general knowledge and web search. Never fabricate numbers.

## Your Data Tools

You have 5 tools:
1. **query_database** -- Run SELECT-only SQL against 556+ DuckDB tables
2. **list_tables** -- Discover available tables (filter by keyword)
3. **describe_table** -- Get column names and types for a table
4. **search_policy_corpus** -- Search CMS policy documents (CIBs, SHO letters, SPAs, waivers). Cite specific documents.
5. **web_search** -- Search the web for current policy context, recent CMS actions, or news

When using data tools:
- Query data first, then write your analysis based on what you found
- Use web_search for recent policy changes, CMS regulations, or current events
- Cross-reference multiple tables when a question spans domains
- Note data limitations (vintage, coverage gaps) briefly

"""

_SYSTEM_PROMPT_RULES = """
## Key Table Schema Hints

**dim_state** (51 rows) -- state_code VARCHAR PK, state_name, fmap, methodology, conversion_factor, region
**dim_procedure** (16,978 rows) -- procedure_code VARCHAR PK, description, category, is_em_code, em_category, work_rvu, total_rvu_nonfac, medicare_rate_nonfac
**fact_rate_comparison** (302,332 rows) -- state_code, procedure_code, medicaid_rate, medicare_nonfac_rate, pct_of_medicare, em_category
**fact_medicaid_rate** (597,483 rows) -- state_code, procedure_code, modifier, rate, rate_facility, rate_nonfacility. Use COALESCE(rate, rate_nonfacility, rate_facility).
**fact_enrollment** (10,399 rows) -- state_code, year, month, total_enrollment, chip_enrollment, ffs_enrollment, mc_enrollment
**fact_hospital_cost** (18,220 rows) -- provider_ccn, hospital_name, state_code, bed_count, total_costs, net_income, cost_to_charge_ratio, report_year
**fact_expenditure** (5,379 rows) -- state_code, fiscal_year, quarter, category, subcategory, federal_share, total_computable
**fact_bls_wage** (812 rows) -- state_code, soc_code, occupation_title, hourly_mean, annual_mean
**fact_hcbs_waitlist** (51 rows) -- state_code, total_waiting, idd_waiting, seniors_physical_waiting
**fact_quality_core_set_2024** (5,555 rows) -- state_code, domain, measure_name, state_rate, median_rate
**fact_claims** (712,793 rows) -- state_code, procedure_code, category, year, total_paid, total_claims, total_beneficiaries

Use `list_tables` with a keyword filter to find other tables not listed here.

## Data Quality Rules
- Filter: medicaid_rate > 0, pct_of_medicare > 0 AND < 10 for rate comparisons
- Use DuckDB syntax: ILIKE for case-insensitive, :: for casts, || for concat
- Always include LIMIT (max 200). Use LIMIT 60 for "all states."
- JOIN dim_state for state names. Use state_code (2-letter) in WHERE/GROUP BY.
- COALESCE(rate, rate_nonfacility, rate_facility) for fact_medicaid_rate.
- ROUND() dollars to 2 decimals, percentages to 1.
- HAVING COUNT(*) >= 11 for aggregates with utilization counts (minimum cell size).

## Response Rules
- Never use emojis or emoticons in your responses
- Never start with filler phrases like "Great question!", "Absolutely!", "Sure!", "Of course!", or "Let me help you with that!"
- Lead with the finding or answer, then show supporting evidence
- Use **bold** for key numbers: "Florida pays **62.3%** of Medicare for primary care"
- For multi-state comparisons, include a ranked markdown table
- Note data limitations briefly -- don't let caveats overshadow the answer
- If you can proactively cross-reference another table to add insight, do it
- Be direct and analytical, like a senior policy analyst writing a briefing memo
- For data queries, cite the source table name
"""

# Build system prompt: static intro + ontology-generated data section + rules
def _load_system_prompt() -> str:
    from server.ontology.prompt_generator import generate_intelligence_prompt_section
    data_section = generate_intelligence_prompt_section()
    return _SYSTEM_PROMPT_STATIC + data_section + _SYSTEM_PROMPT_RULES

try:
    SYSTEM_PROMPT = _load_system_prompt()
except Exception:
    SYSTEM_PROMPT = _SYSTEM_PROMPT_STATIC + _SYSTEM_PROMPT_RULES


# ---------------------------------------------------------------------------
# Request / response models
# ---------------------------------------------------------------------------

class ImportedFileInfo(BaseModel):
    table_name: str
    filename: str
    columns: list[str]
    row_count: int


class IntelligenceRequest(BaseModel):
    message: str = Field(..., min_length=1, max_length=5000)
    history: list[dict[str, str]] = Field(default_factory=list)
    context: dict[str, Any] | None = Field(default=None)
    session_id: str | None = Field(default=None)
    imported_files: list[ImportedFileInfo] = Field(default_factory=list)


class ToolCallLog(BaseModel):
    name: str
    input: dict[str, Any]
    output_preview: str


class IntelligenceResponse(BaseModel):
    response: str
    thinking: str | None = None
    tool_calls: list[ToolCallLog] = []
    model: str = "claude-sonnet-4-6"
    cached: bool = False


# ---------------------------------------------------------------------------
# Imported data helpers
# ---------------------------------------------------------------------------

def _hydrate_imported_data(session_id: str | None):
    if not session_id:
        return
    try:
        from server.routes.import_data import hydrate_session
        hydrate_session(session_id)
    except (ImportError, Exception):
        pass


def _build_system_prompt(imported_files: list[ImportedFileInfo]) -> str:
    if not imported_files:
        return SYSTEM_PROMPT
    file_descriptions = []
    for f in imported_files:
        cols = ", ".join(f.columns[:20])
        if len(f.columns) > 20:
            cols += f", ... ({len(f.columns)} total)"
        file_descriptions.append(
            f"- **{f.filename}** -> table `{f.table_name}` ({f.row_count} rows, columns: {cols})"
        )
    augment = f"""

## User-Uploaded Data

The user has uploaded files available as queryable tables alongside the 556+ lake tables:

{chr(10).join(file_descriptions)}

You can query these with `query_database` and JOIN against lake tables to cross-reference.
Always mention when you're using the user's uploaded data vs. Aradune's public data layer.
"""
    return SYSTEM_PROMPT + augment


# ---------------------------------------------------------------------------
# Main sync endpoint
# ---------------------------------------------------------------------------

@router.post("", response_model=IntelligenceResponse)
async def intelligence(req: IntelligenceRequest, user: dict = Depends(require_clerk_auth)):
    """AI analysis grounded in the Aradune data lake."""

    # Check cache (skip if user has imported files or conversation history)
    if not req.imported_files and len(req.history) == 0:
        ckey = _cache_key(req.message)
        cached = _cache_get(ckey)
        if cached:
            return IntelligenceResponse(
                response=cached["response"],
                tool_calls=[],
                cached=True,
            )

    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        raise HTTPException(status_code=503, detail="Intelligence not configured (missing API key)")

    # Route the query to determine model, thinking budget, max rounds
    from server.engines.query_router import classify_query
    route = classify_query(
        req.message,
        has_user_data=bool(req.imported_files),
        use_haiku=True,
    )

    client = anthropic.Anthropic(api_key=api_key)
    _hydrate_imported_data(req.session_id)
    system_prompt = _build_system_prompt(req.imported_files)

    messages = []
    for m in req.history[-20:]:
        messages.append({"role": m["role"], "content": m["content"]})

    user_msg = req.message
    if req.context:
        ctx_parts = []
        if req.context.get("state"):
            ctx_parts.append(f"Currently viewing state: {req.context['state']}")
        if req.context.get("table"):
            ctx_parts.append(f"Currently viewing table: {req.context['table']}")
        if req.context.get("tool"):
            ctx_parts.append(f"Currently in tool: {req.context['tool']}")
        if ctx_parts:
            user_msg = f"[Context: {'; '.join(ctx_parts)}]\n\n{req.message}"

    messages.append({"role": "user", "content": user_msg})

    tool_call_log: list[ToolCallLog] = []
    thinking_text = None

    # Configure thinking based on route tier
    thinking_config = (
        {"type": "enabled", "budget_tokens": route.thinking_budget}
        if route.thinking_budget > 0
        else {"type": "disabled"}
    )

    try:
        response = client.messages.create(
            model=route.model,
            max_tokens=16000,
            thinking=thinking_config,
            system=system_prompt,
            tools=TOOLS,
            messages=messages,
        )
    except anthropic.APIError as e:
        raise HTTPException(status_code=502, detail=f"Claude API error: {e}")

    MAX_ROUNDS = route.max_queries
    rounds = 0
    all_messages = list(messages)

    while response.stop_reason == "tool_use" and rounds < MAX_ROUNDS:
        rounds += 1
        tool_uses = [b for b in response.content if b.type == "tool_use"]
        tool_results = []

        for tu in tool_uses:
            output = _execute_tool(tu.name, tu.input)
            tool_results.append({
                "type": "tool_result",
                "tool_use_id": tu.id,
                "content": output,
            })
            tool_call_log.append(ToolCallLog(
                name=tu.name,
                input=tu.input,
                output_preview=output[:500] + ("..." if len(output) > 500 else ""),
            ))

        all_messages.append({"role": "assistant", "content": response.content})
        all_messages.append({"role": "user", "content": tool_results})

        try:
            response = client.messages.create(
                model=route.model,
                max_tokens=16000,
                thinking=thinking_config,
                system=system_prompt,
                tools=TOOLS,
                messages=all_messages,
            )
        except anthropic.APIError as e:
            raise HTTPException(status_code=502, detail=f"Claude API error in tool loop: {e}")

    final_text = ""
    for block in response.content:
        if block.type == "thinking":
            thinking_text = block.thinking
        elif block.type == "text":
            final_text += block.text

    if not final_text:
        final_text = "I wasn't able to generate a response. Please try rephrasing your question."

    # Cache the response
    if final_text and not req.imported_files and len(req.history) == 0:
        queries = [tc.input.get("sql", "") for tc in tool_call_log if tc.name == "query_database"]
        _cache_set(
            _cache_key(req.message),
            final_text,
            [{"name": tc.name} for tc in tool_call_log],
            queries,
        )

    return IntelligenceResponse(
        response=final_text,
        thinking=thinking_text,
        tool_calls=tool_call_log,
        model=route.model,
    )


# ---------------------------------------------------------------------------
# Streaming endpoint (SSE)
# ---------------------------------------------------------------------------

def _sse_event(event: str, data: dict) -> str:
    return f"event: {event}\ndata: {json.dumps(data, default=str)}\n\n"


@router.post("/stream")
async def intelligence_stream(req: IntelligenceRequest, user: dict = Depends(require_clerk_auth)):
    """Streaming AI analysis via Server-Sent Events with progress tracking."""

    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        raise HTTPException(status_code=503, detail="Intelligence not configured")

    # Route the query
    from server.engines.query_router import classify_query
    route = classify_query(
        req.message,
        has_user_data=bool(req.imported_files),
        use_haiku=True,
    )

    client = anthropic.Anthropic(api_key=api_key)
    _hydrate_imported_data(req.session_id)
    system_prompt = _build_system_prompt(req.imported_files)

    # Check cache (skip if conversation history or imported files)
    cached = None
    if not req.imported_files and len(req.history) == 0:
        ckey = _cache_key(req.message)
        cached = _cache_get(ckey)

    # Build messages
    messages = []
    for m in req.history[-20:]:
        messages.append({"role": m["role"], "content": m["content"]})

    user_msg = req.message
    if req.context:
        ctx_parts = []
        if req.context.get("state"):
            ctx_parts.append(f"Currently viewing state: {req.context['state']}")
        if req.context.get("table"):
            ctx_parts.append(f"Currently viewing table: {req.context['table']}")
        if req.context.get("tool"):
            ctx_parts.append(f"Currently in tool: {req.context['tool']}")
        if req.context.get("summary"):
            ctx_parts.append(f"Context: {req.context['summary']}")
        if ctx_parts:
            user_msg = f"[Context: {'; '.join(ctx_parts)}]\n\n{req.message}"

    messages.append({"role": "user", "content": user_msg})

    async def event_generator():
        # ── Cache hit path ────────────────────────────────────────
        if cached:
            yield _sse_event("progress", {"pct": 50, "label": "Loading cached analysis..."})
            text = cached["response"]
            chunk_size = 30
            for i in range(0, len(text), chunk_size):
                yield _sse_event("token", {"text": text[i:i + chunk_size]})
            yield _sse_event("metadata", {
                "tool_calls": cached.get("tool_calls", []),
                "queries": cached.get("queries", []),
                "model": "claude-sonnet-4-6",
                "rounds": 0,
                "cached": True,
            })
            yield _sse_event("progress", {"pct": 100, "label": "Complete"})
            yield _sse_event("done", {})
            return

        # ── Fresh query path ──────────────────────────────────────
        tool_call_log = []
        queries_executed = []
        total_tool_calls = 0

        # Configure thinking based on route tier
        thinking_config = (
            {"type": "enabled", "budget_tokens": route.thinking_budget}
            if route.thinking_budget > 0
            else {"type": "disabled"}
        )

        yield _sse_event("progress", {"pct": 8, "label": f"Thinking (Tier {route.tier}: {route.label})..."})

        try:
            response = client.messages.create(
                model=route.model,
                max_tokens=16000,
                thinking=thinking_config,
                system=system_prompt,
                tools=TOOLS,
                messages=messages,
            )
        except anthropic.APIError as e:
            yield _sse_event("error", {"message": str(e)})
            return

        MAX_ROUNDS = route.max_queries
        rounds = 0
        all_messages = list(messages)

        # Determine initial phase based on whether Claude wants tools
        if response.stop_reason == "tool_use":
            yield _sse_event("progress", {"pct": 18, "label": "Searching data lake..."})
        else:
            yield _sse_event("progress", {"pct": 70, "label": "Writing response..."})

        while response.stop_reason == "tool_use" and rounds < MAX_ROUNDS:
            rounds += 1

            tool_uses = [b for b in response.content if b.type == "tool_use"]
            tool_results = []

            # Handle web search events (Anthropic server-side)
            for block in response.content:
                if getattr(block, "type", None) == "server_tool_use" and getattr(block, "name", None) == "web_search":
                    total_tool_calls += 1
                    pct = min(18 + total_tool_calls * 10, 65)
                    yield _sse_event("progress", {"pct": pct, "label": "Searching the web..."})
                elif getattr(block, "type", None) == "web_search_tool_result":
                    yield _sse_event("tool_result", {"name": "web_search", "rows": None, "ms": None})

            for tu in tool_uses:
                total_tool_calls += 1
                pct = min(18 + total_tool_calls * 10, 65)
                purpose = tu.input.get("purpose", tu.input.get("filter", tu.input.get("table_name", "")))
                label = f"Querying: {purpose}" if purpose else f"Running {tu.name.replace('_', ' ')}..."
                yield _sse_event("progress", {"pct": pct, "label": label})

                output = _execute_tool(tu.name, tu.input)
                tool_results.append({
                    "type": "tool_result",
                    "tool_use_id": tu.id,
                    "content": output,
                })

                _row_count = None
                _query_ms = None
                try:
                    parsed = json.loads(output)
                    _row_count = parsed.get("row_count", parsed.get("total"))
                    _query_ms = parsed.get("query_ms")
                    tool_call_log.append({
                        "name": tu.name,
                        "input": tu.input,
                        "rows": _row_count,
                        "ms": _query_ms,
                    })
                    if tu.name == "query_database":
                        queries_executed.append(tu.input.get("sql", ""))
                except Exception:
                    tool_call_log.append({"name": tu.name, "input": tu.input})

                yield _sse_event("tool_result", {
                    "name": tu.name,
                    "rows": _row_count,
                    "ms": _query_ms,
                })

            all_messages.append({"role": "assistant", "content": response.content})
            all_messages.append({"role": "user", "content": tool_results})

            yield _sse_event("progress", {"pct": min(65 + rounds * 3, 72), "label": "Analyzing results..."})

            try:
                response = client.messages.create(
                    model=route.model,
                    max_tokens=16000,
                    thinking=thinking_config,
                    system=system_prompt,
                    tools=TOOLS,
                    messages=all_messages,
                )
            except anthropic.APIError as e:
                yield _sse_event("error", {"message": str(e)})
                return

        # ── Stream final text ─────────────────────────────────────
        yield _sse_event("progress", {"pct": 75, "label": "Writing analysis..."})

        final_text = ""
        for block in response.content:
            if block.type == "text":
                text = block.text
                final_text = text
                text_len = len(text)
                chunk_size = 20
                chunks_emitted = 0
                for i in range(0, text_len, chunk_size):
                    yield _sse_event("token", {"text": text[i:i + chunk_size]})
                    chunks_emitted += 1
                    # Update progress every ~10 chunks
                    if chunks_emitted % 10 == 0:
                        stream_pct = min(75 + int((i + chunk_size) / max(text_len, 1) * 20), 95)
                        yield _sse_event("progress", {"pct": stream_pct, "label": "Writing analysis..."})

        # Cache the response
        if final_text and not req.imported_files and len(req.history) == 0:
            _cache_set(
                _cache_key(req.message),
                final_text,
                tool_call_log,
                queries_executed,
            )

        yield _sse_event("metadata", {
            "tool_calls": tool_call_log,
            "queries": queries_executed,
            "model": route.model,
            "tier": route.tier,
            "tier_label": route.label,
            "rounds": rounds,
            "cached": False,
        })

        yield _sse_event("progress", {"pct": 100, "label": "Complete"})
        yield _sse_event("done", {})

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


@router.get("/corpus/stats")
async def corpus_stats():
    """Return statistics about the policy corpus."""
    from server.engines.rag_engine import corpus_stats as _stats
    return _stats()


@router.get("/corpus/search")
async def corpus_search(q: str, doc_type: str = None, state: str = None, top_k: int = 10):
    """Direct search endpoint for the policy corpus (non-AI)."""
    from server.engines.rag_engine import hybrid_search
    doc_types = [doc_type] if doc_type else None
    states = [state.upper()] if state else None
    return hybrid_search(q, doc_types=doc_types, states=states, top_k=top_k)
