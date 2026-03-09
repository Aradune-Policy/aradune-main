"""
Aradune Intelligence — AI analysis grounded in 250+ Medicaid data tables.

Claude Sonnet 4.6 with extended thinking and DuckDB tool access.
This is the engine behind the Policy Analyst and the "Ask about this" buttons
throughout the platform.
"""

import json
import os
import re
import time
from typing import Any

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

import anthropic

from server.db import get_cursor

router = APIRouter(prefix="/api/intelligence", tags=["intelligence"])

# ---------------------------------------------------------------------------
# SQL safety (shared with nl2sql.py)
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

TOOLS = [
    {
        "name": "query_database",
        "description": (
            "Run a SELECT-only SQL query against the Aradune DuckDB data lake. "
            "Contains 250+ Medicaid fact tables with 115M+ rows. "
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
]


def _execute_tool(name: str, inp: dict) -> str:
    """Execute a tool call and return JSON string result."""
    try:
        if name == "query_database":
            result = _run_query(inp["sql"])
            # Truncate if too many rows for context
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
                # Get actual row count
                try:
                    cnt = cur.execute(f"SELECT COUNT(*) FROM {name_val}").fetchone()[0]
                except Exception:
                    cnt = None
                tables.append({"table": name_val, "rows": cnt})
            return json.dumps({"tables": tables, "total": len(tables)}, default=str)

        elif name == "describe_table":
            tbl = inp["table_name"]
            # Sanitize table name
            if not re.match(r"^[a-zA-Z_][a-zA-Z0-9_]*$", tbl):
                return json.dumps({"error": "Invalid table name"})
            with get_cursor() as cur:
                cols = cur.execute(f"DESCRIBE {tbl}").fetchall()
            return json.dumps({
                "table": tbl,
                "columns": [{"name": c[0], "type": c[1]} for c in cols],
            })

        return json.dumps({"error": f"Unknown tool: {name}"})

    except Exception as e:
        return json.dumps({"error": str(e)})


# ---------------------------------------------------------------------------
# System prompt
# ---------------------------------------------------------------------------

SYSTEM_PROMPT = """You are Aradune Intelligence, an AI analyst with direct access to the most comprehensive public Medicaid data lake ever assembled: 250+ tables, 115M+ rows, covering rates, enrollment, hospitals, quality, workforce, pharmacy, and economics for all 50 states.

## Your Core Advantage
You have something no other AI has: the ability to QUERY REAL DATA before answering. A regular AI chat would approximate or guess. You look it up.

When a user asks a question:
1. ALWAYS query the data first rather than relying on general knowledge
2. Cite specific numbers with their source table
3. Cross-reference multiple tables when the question spans domains
4. Note data limitations (vintage, coverage gaps, quality flags)

## Key Tables (use list_tables and describe_table for full catalog)

**Rates & Fees:**
- fact_rate_comparison: Medicaid vs Medicare rates (302K rows, 45 states, pct_of_medicare)
- fact_medicaid_rate: Raw fee schedule rates (597K rows, 47 states)
- dim_procedure: 16,978 HCPCS/CPT codes with Medicare RVUs

**Enrollment & Demographics:**
- fact_enrollment: Monthly Medicaid enrollment (total, FFS, MC)
- fact_acs_state: Census demographics, poverty, uninsured rates
- fact_unwinding: Redetermination outcomes by state

**Hospitals:**
- fact_hospital_cost: HCRIS cost reports (6,103 hospitals)
- fact_hospital_rating: CMS star ratings
- fact_dsh_hospital: DSH data

**Quality:**
- fact_quality_measure: CMS Core Set measures
- fact_quality_core_set_2024: Latest quality measures
- fact_scorecard: Medicaid Scorecard

**Workforce:**
- fact_bls_wage: BLS healthcare occupation wages by state
- fact_hpsa: Health Professional Shortage Areas (69K designations)
- fact_workforce_projections: HRSA 2023-2038 projections

**Pharmacy:**
- fact_drug_utilization: State Drug Utilization Data
- fact_sdud_2025: Latest SDUD (2.6M rows, Q1-Q2 2025)
- fact_nadac: National Average Drug Acquisition Cost

**LTSS & HCBS:**
- fact_hcbs_waitlist: 51 states, 606K people waiting
- fact_ltss_expenditure: LTSS spending by state
- fact_cms372_waiver: 553 HCBS waiver programs

**Economic:**
- fact_unemployment: Monthly state unemployment
- fact_fair_market_rent: HUD FMR by county
- fact_snap_enrollment, fact_tanf_enrollment: Cross-program data

**Behavioral Health:**
- fact_nsduh_prevalence: SAMHSA substance use/mental health
- fact_teds_admissions: Treatment admissions (1.6M)
- fact_block_grant: MHBG allocations

## State Reference
- dim_state: 51 rows with state_code (2-letter PK), state_name, fmap, methodology, managed care info
- Always use state_code (2-letter) in queries, JOIN dim_state for full names

## Data Quality Rules
- T-MSIS claims data has a 12-18 month lag. Never describe as "current."
- Filter: medicaid_rate > 0, pct_of_medicare > 0 AND < 10 for rate comparisons
- Minimum cell size: aggregates must have COUNT(*) >= 11
- Some states have incomplete data. Note when results show fewer than expected states.
- SD rates are per-15-minute (not per-service). DC has 45K+ codes including non-physician.

## Response Style
- Lead with the finding, then show the evidence
- When you query data, show key numbers inline (don't just say "I found that...")
- For multi-state comparisons, use ranked lists
- Note limitations and caveats, but don't let them overshadow the answer
- If you can answer a follow-up question by cross-referencing with another table, do it proactively
"""

# ---------------------------------------------------------------------------
# Request / response
# ---------------------------------------------------------------------------

class IntelligenceRequest(BaseModel):
    message: str = Field(..., min_length=1, max_length=5000)
    history: list[dict[str, str]] = Field(default_factory=list, description="Prior messages [{role, content}]")
    context: dict[str, Any] | None = Field(default=None, description="Current tool context (state, table, etc.)")


class ToolCallLog(BaseModel):
    name: str
    input: dict[str, Any]
    output_preview: str  # truncated for response size


class IntelligenceResponse(BaseModel):
    response: str
    thinking: str | None = None
    tool_calls: list[ToolCallLog] = []
    model: str = "claude-sonnet-4-6"


# ---------------------------------------------------------------------------
# Main endpoint
# ---------------------------------------------------------------------------

@router.post("", response_model=IntelligenceResponse)
async def intelligence(req: IntelligenceRequest):
    """AI analysis grounded in the Aradune data lake."""

    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        raise HTTPException(status_code=503, detail="Intelligence not configured (missing API key)")

    client = anthropic.Anthropic(api_key=api_key)

    # Build messages
    messages = []
    for m in req.history[-20:]:  # cap history
        messages.append({"role": m["role"], "content": m["content"]})

    # Add context if provided (e.g., user is on State Profile for FL)
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

    # Call Claude with tools and extended thinking
    tool_call_log: list[ToolCallLog] = []
    thinking_text = None

    try:
        response = client.messages.create(
            model="claude-sonnet-4-6",
            max_tokens=16000,
            thinking={
                "type": "enabled",
                "budget_tokens": 10000,
            },
            system=SYSTEM_PROMPT,
            tools=TOOLS,
            messages=messages,
        )
    except anthropic.APIError as e:
        raise HTTPException(status_code=502, detail=f"Claude API error: {e}")

    # Agentic tool-use loop
    MAX_ROUNDS = 10
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
            # Log for response
            tool_call_log.append(ToolCallLog(
                name=tu.name,
                input=tu.input,
                output_preview=output[:500] + ("..." if len(output) > 500 else ""),
            ))

        all_messages.append({"role": "assistant", "content": response.content})
        all_messages.append({"role": "user", "content": tool_results})

        try:
            response = client.messages.create(
                model="claude-sonnet-4-6",
                max_tokens=16000,
                thinking={
                    "type": "enabled",
                    "budget_tokens": 10000,
                },
                system=SYSTEM_PROMPT,
                tools=TOOLS,
                messages=all_messages,
            )
        except anthropic.APIError as e:
            raise HTTPException(status_code=502, detail=f"Claude API error in tool loop: {e}")

    # Extract final text and thinking
    final_text = ""
    for block in response.content:
        if block.type == "thinking":
            thinking_text = block.thinking
        elif block.type == "text":
            final_text += block.text

    if not final_text:
        final_text = "I wasn't able to generate a response. Please try rephrasing your question."

    return IntelligenceResponse(
        response=final_text,
        thinking=thinking_text,
        tool_calls=tool_call_log,
    )
