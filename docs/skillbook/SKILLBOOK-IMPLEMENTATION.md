# Aradune Self-Corrective Intelligence: Implementation Plan
## Session 31 Addendum | March 2026

---

## 0. What This Adds to Aradune

Right now, Aradune's Intelligence endpoint is stateless. Every query starts from zero: system prompt + ontology + RAG + DuckDB tools. If a user asks about Florida's dental fee schedule pricing methodology and Intelligence gives a wrong answer, that mistake evaporates. The next user asking the same question hits the same failure mode.

This plan adds a **Skillbook** -- a persistent, self-curating layer of Medicaid domain intelligence that sits between the ontology and Claude. It learns from every query: what reasoning worked, what failed, what domain rules matter. Over time, Aradune's Intelligence gets measurably better at answering Medicaid questions without any model fine-tuning, retraining, or manual prompt engineering.

The pattern is drawn from the ACE framework (ICLR 2026) and Dynamic Cheatsheet (ICLR 2026), adapted for a domain-specific regulatory analytics platform.

---

## 1. Architecture: Where the Skillbook Lives

### Current Intelligence Flow (from your build doc, Section 5)

```
User query
  -> Haiku classifier (Tier 1-4)
  -> System prompt (ontology/generated_prompt.md, ~33.7KB)
  -> Tools: query_database, list_tables, describe_table, web_search, search_policy
  -> Claude (Sonnet/Opus with extended thinking)
  -> SSE streaming response
```

### New Flow with Skillbook

```
User query
  -> Haiku classifier (Tier 1-4)
  -> System prompt (ontology/generated_prompt.md, ~33.7KB)
  -> SKILLBOOK INJECTION (retrieve relevant skills by domain + semantic match)
  -> Tools: query_database, list_tables, describe_table, web_search, search_policy
  -> Claude (Sonnet/Opus with extended thinking)
  -> SSE streaming response
  -> REFLECTION (async, non-blocking)
      -> Feedback signal (explicit or implicit)
      -> Reflector analyzes reasoning trace
      -> Curator proposes skill updates
      -> Skillbook updated in DuckDB
```

The key design decision: **the reflection loop runs asynchronously after the response is sent.** Users never wait for the learning step. This is critical -- your Tier 1 lookups need to stay under 1 second.

---

## 2. The Skillbook Schema

### New DuckDB Table: `fact_skillbook`

This lives in your data lake alongside everything else. It's a first-class fact table, registered in `db.py` like any other, queryable by Intelligence itself.

```sql
CREATE TABLE fact_skillbook (
    skill_id        VARCHAR PRIMARY KEY,   -- UUID
    domain          VARCHAR NOT NULL,      -- maps to ontology/domains/*.yaml
    category        VARCHAR NOT NULL,      -- 'strategy' | 'caveat' | 'failure_mode' | 'domain_rule' | 'query_pattern'
    content         VARCHAR NOT NULL,      -- the actual insight (1-3 sentences)
    source_type     VARCHAR,               -- 'reflection' | 'manual' | 'audit' | 'user_correction'
    source_query    VARCHAR,               -- the query that generated this skill
    source_trace    VARCHAR,               -- abbreviated reasoning trace
    provenance      VARCHAR,               -- CFR citation, CMS doc, SPA number, etc.
    helpful_count   INTEGER DEFAULT 0,     -- incremented when skill contributes to good answer
    harmful_count   INTEGER DEFAULT 0,     -- incremented when skill leads to error
    net_score       INTEGER GENERATED ALWAYS AS (helpful_count - harmful_count),
    times_retrieved INTEGER DEFAULT 0,     -- how often this skill is injected into prompts
    created_at      TIMESTAMP DEFAULT current_timestamp,
    updated_at      TIMESTAMP DEFAULT current_timestamp,
    superseded_by   VARCHAR,               -- points to newer skill_id if this one was refined
    active          BOOLEAN DEFAULT true   -- false = retired/superseded
);

-- FTS index for semantic retrieval
CREATE INDEX idx_skillbook_fts ON fact_skillbook 
    USING PRAGMA_FTS(skill_id, content, domain, category);

-- Domain + score index for fast retrieval
CREATE INDEX idx_skillbook_domain ON fact_skillbook(domain, active, net_score DESC);
```

### Why DuckDB and Not a Separate Vector Store

You already have DuckDB FTS working for your RAG engine (BM25 over `fact_policy_chunk`). The skillbook uses the same pattern. No new infrastructure. No Pinecone, no Chroma, no Redis. The skillbook is small (hundreds to low thousands of entries) -- DuckDB handles this trivially.

If you later want semantic/vector retrieval, you already have the `VOYAGE_API_KEY` optional path in your RAG engine. Same upgrade path applies here.

---

## 3. Skill Categories (Mapped to Aradune's Domains)

| Category | What It Captures | Example |
|----------|-----------------|---------|
| `strategy` | Effective reasoning patterns | "For novel CDT codes without FL precedent, use Section 1834A residual pricing methodology with CY2025 CF ($32.3465) as federal anchor" |
| `caveat` | Data quality warnings learned from experience | "T-MSIS encounter amounts for TN are simulated from claims averages, not actual FFS rates -- always flag this" |
| `failure_mode` | Reasoning paths that produced wrong answers | "Do NOT use fact_doge tables for state-level spending comparison -- OT-only, provider-state distortion makes MC states look artificially low" |
| `domain_rule` | Regulatory/policy rules that must be applied | "FL Medicaid rates cannot have both facility rate AND PC/TC split. Codes 46924, 91124, 91125 require special handling" |
| `query_pattern` | SQL patterns that work for common questions | "For per-enrollee spending by state: JOIN fact_cms64_multiyear ON state_code, filter service_category='total', divide by fact_enrollment excluding CHIP" |

### Seed Skills (Day 1)

You already have implicit skills scattered across your build. These become the initial skillbook:

**From your Response Rules (Section 5):**
1. "Specify data vintage (e.g., 'Based on CY2022 T-MSIS claims') -- never say 'current'" -> `domain_rule`
2. "T-MSIS encounter amounts unreliable for MCO-to-provider rates" -> `caveat`
3. "CPRA uses $32.3465 CF (CY2025). General: $33.4009 (CY2026)" -> `domain_rule`
4. "Census sentinels (-888888888) = NULL" -> `failure_mode`
5. "CHIP excluded from per-enrollee calculations" -> `domain_rule`

**From your DOGE Quarantine:**
6. "DOGE data is OT-only, uses provider state (not beneficiary state), MC states show misleadingly low paid amounts, Nov/Dec 2024 incomplete" -> `caveat`

**From your Research Audit findings:**
7. "Rate-Quality regression: drop SVI variable to avoid multicollinearity (VIF > 10 for SVI + poverty). Without this, p inflates from 0.044 to 0.178" -> `failure_mode`
8. "MAT spending calculation: use full NDC product names, not truncated. Truncation caused $0 match initially" -> `failure_mode`

**From your FL rate-setting rule:**
9. "FL Medicaid: no facility + PC/TC split on same code. Codes 46924, 91124, 91125 flagged." -> `domain_rule`

**From your Build Principles:**
10. "Never trust a single source. Triangulate: TAF + CMS-64 + supplemental for expenditure figures." -> `strategy`

You can seed ~20-30 skills from existing institutional knowledge on day one. The system then learns new ones organically.

---

## 4. File-Level Implementation

### New Files

```
server/
  engines/
    skillbook.py          # Skillbook engine (retrieval, injection, CRUD)
    reflector.py          # Async reflection + skill extraction
  routes/
    skillbook.py          # Admin endpoints for viewing/editing skills
ontology/
  domains/
    skillbook.yaml        # Register skillbook as a domain in the ontology
scripts/
  seed_skillbook.py       # One-time seed from existing rules
  export_skillbook.py     # Export skillbook as markdown (for audit)
```

### Modified Files

| File | Change | Why |
|------|--------|-----|
| `server/db.py` | Add `fact_skillbook` to `FACT_NAMES` or create real table at init | Skillbook needs to persist |
| `server/routes/intelligence.py` | Inject skills into system prompt; fire async reflection after response | Core integration point |
| `server/engines/query_router.py` | Pass domain classification to skillbook retriever | Skills are domain-filtered |
| `server/main.py` | Import skillbook routes | Wire up admin endpoints |

---

## 5. Engine Implementation

### `server/engines/skillbook.py`

```python
"""
Aradune Skillbook Engine
Persistent, self-curating domain intelligence layer.

Pattern: ACE (ICLR 2026) adapted for Medicaid regulatory analytics.
Skills are structured bullets with metadata, retrieved by domain + BM25
and injected into the Intelligence system prompt.
"""

import uuid
import duckdb
from datetime import datetime
from typing import Optional
from server.db import get_connection


# ---------------------------------------------------------------------------
# Retrieval
# ---------------------------------------------------------------------------

def retrieve_skills(
    domain: str,
    query: str,
    max_skills: int = 10,
    min_score: int = -2,    # allow slightly negative skills (still learning)
) -> list[dict]:
    """
    Retrieve relevant skills for injection into the Intelligence prompt.
    
    Two-stage retrieval:
    1. Domain-filtered by net_score (fast, indexed)
    2. BM25 text match against query (semantic relevance)
    
    Returns skills ordered by relevance, capped at max_skills.
    """
    conn = get_connection()
    
    # Stage 1: Domain rules always included (they're universal for the domain)
    domain_rules = conn.execute("""
        SELECT skill_id, domain, category, content, provenance,
               helpful_count, harmful_count, net_score
        FROM fact_skillbook
        WHERE domain = ? AND category = 'domain_rule' AND active = true
              AND net_score >= ?
        ORDER BY net_score DESC
        LIMIT ?
    """, [domain, min_score, max_skills // 2]).fetchall()
    
    # Stage 2: BM25 search for query-relevant skills
    # Uses same FTS pattern as your RAG engine
    relevant = conn.execute("""
        SELECT skill_id, domain, category, content, provenance,
               helpful_count, harmful_count, net_score,
               fts_main_fact_skillbook.match_bm25(skill_id, ?) AS relevance
        FROM fact_skillbook
        WHERE active = true AND net_score >= ?
              AND category != 'domain_rule'  -- already got these
        ORDER BY relevance DESC
        LIMIT ?
    """, [query, min_score, max_skills - len(domain_rules)]).fetchall()
    
    # Merge, deduplicate, increment retrieval counter
    all_skills = list(domain_rules) + list(relevant)
    skill_ids = [s[0] for s in all_skills]
    
    if skill_ids:
        placeholders = ",".join(["?"] * len(skill_ids))
        conn.execute(f"""
            UPDATE fact_skillbook 
            SET times_retrieved = times_retrieved + 1
            WHERE skill_id IN ({placeholders})
        """, skill_ids)
    
    return [_row_to_dict(s) for s in all_skills]


def format_skills_for_prompt(skills: list[dict]) -> str:
    """
    Format retrieved skills as a prompt section.
    Injected between the ontology system prompt and the user query.
    """
    if not skills:
        return ""
    
    lines = ["\n## Learned Domain Intelligence (Skillbook)\n"]
    lines.append("These are validated insights from prior analyses. Apply them when relevant.\n")
    
    for s in skills:
        prefix = {
            "strategy": "STRATEGY",
            "caveat": "CAUTION",
            "failure_mode": "AVOID",
            "domain_rule": "RULE",
            "query_pattern": "PATTERN",
        }.get(s["category"], "NOTE")
        
        score_note = ""
        if s["helpful_count"] > 3:
            score_note = f" [validated {s['helpful_count']}x]"
        
        provenance = f" (Source: {s['provenance']})" if s.get("provenance") else ""
        lines.append(f"- **{prefix}:** {s['content']}{provenance}{score_note}")
    
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# CRUD
# ---------------------------------------------------------------------------

def add_skill(
    domain: str,
    category: str,
    content: str,
    source_type: str = "reflection",
    source_query: str = None,
    source_trace: str = None,
    provenance: str = None,
) -> str:
    """Add a new skill to the skillbook. Returns skill_id."""
    conn = get_connection()
    skill_id = str(uuid.uuid4())[:12]
    
    conn.execute("""
        INSERT INTO fact_skillbook 
        (skill_id, domain, category, content, source_type, 
         source_query, source_trace, provenance)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """, [skill_id, domain, category, content, source_type,
          source_query, source_trace, provenance])
    
    return skill_id


def update_score(skill_id: str, helpful: bool):
    """Increment helpful or harmful counter."""
    conn = get_connection()
    col = "helpful_count" if helpful else "harmful_count"
    conn.execute(f"""
        UPDATE fact_skillbook 
        SET {col} = {col} + 1, updated_at = current_timestamp
        WHERE skill_id = ?
    """, [skill_id])


def retire_skill(skill_id: str, superseded_by: str = None):
    """Retire a skill (soft delete)."""
    conn = get_connection()
    conn.execute("""
        UPDATE fact_skillbook 
        SET active = false, superseded_by = ?, updated_at = current_timestamp
        WHERE skill_id = ?
    """, [superseded_by, skill_id])


def _row_to_dict(row) -> dict:
    cols = ["skill_id", "domain", "category", "content", "provenance",
            "helpful_count", "harmful_count", "net_score"]
    return dict(zip(cols, row[:len(cols)]))
```

### `server/engines/reflector.py`

```python
"""
Aradune Reflector
Async post-response analysis that extracts skills from Intelligence traces.

Runs AFTER the SSE response is complete. Never blocks user response.
Uses Haiku for cost efficiency (~$0.004 per reflection).
"""

import asyncio
import logging
from typing import Optional
from anthropic import Anthropic
from server.engines.skillbook import add_skill, update_score, retire_skill

logger = logging.getLogger("aradune.reflector")
client = Anthropic()

REFLECTOR_PROMPT = """You are Aradune's Reflector -- you analyze completed Medicaid intelligence queries to extract reusable domain knowledge.

Given:
- The user's original query
- The domain classification
- The SQL queries executed
- The final response text
- Any feedback signal (thumbs up/down, user correction, or none)

Your job: identify 0-3 reusable insights that would help answer SIMILAR future queries better.

For each insight, output JSON:
{
  "skills": [
    {
      "category": "strategy|caveat|failure_mode|domain_rule|query_pattern",
      "content": "1-3 sentence insight, specific and actionable",
      "provenance": "CFR section, CMS doc, or data source if applicable",
      "confidence": "high|medium|low"
    }
  ],
  "skill_updates": [
    {
      "skill_id": "existing skill ID",
      "helpful": true/false
    }
  ]
}

Rules:
- Only extract insights that generalize beyond this specific query
- "Florida's enrollment is 5.2M" is NOT a skill (it's a data point)
- "T-MSIS encounter amounts for managed care states undercount actual rates by 15-40%" IS a skill
- If the response was wrong and you know why, that's a failure_mode
- If the response used a clever join or calculation pattern, that's a query_pattern
- If there's no generalizable insight, return {"skills": [], "skill_updates": []}
- Be conservative. 0 skills is better than a bad skill.
"""


async def reflect_on_response(
    query: str,
    domain: str,
    sql_traces: list[str],
    response_text: str,
    feedback: Optional[str] = None,  # "positive", "negative", "correction:...", or None
    retrieved_skill_ids: list[str] = None,
):
    """
    Async reflection after Intelligence response.
    Called via asyncio.create_task() so it never blocks.
    """
    try:
        # Build reflection context
        sql_summary = "\n".join(sql_traces[:5])  # cap at 5 queries
        response_excerpt = response_text[:2000]   # cap context length
        
        feedback_text = "No explicit feedback yet."
        if feedback == "positive":
            feedback_text = "User gave thumbs up (positive signal)."
        elif feedback == "negative":
            feedback_text = "User gave thumbs down (negative signal)."
        elif feedback and feedback.startswith("correction:"):
            feedback_text = f"User corrected the response: {feedback[11:]}"
        
        user_msg = f"""Query: {query}
Domain: {domain}
SQL executed:
{sql_summary}

Response excerpt:
{response_excerpt}

Feedback: {feedback_text}

Retrieved skill IDs: {retrieved_skill_ids or 'none'}"""

        # Use Haiku for cost efficiency (~$0.004)
        response = client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=1000,
            system=REFLECTOR_PROMPT,
            messages=[{"role": "user", "content": user_msg}],
        )
        
        import json
        result = json.loads(response.content[0].text)
        
        # Process new skills
        for skill in result.get("skills", []):
            if skill.get("confidence") in ("high", "medium"):
                add_skill(
                    domain=domain,
                    category=skill["category"],
                    content=skill["content"],
                    source_type="reflection",
                    source_query=query[:500],
                    provenance=skill.get("provenance"),
                )
                logger.info(f"Skillbook: added {skill['category']} skill")
        
        # Process score updates for retrieved skills
        for update in result.get("skill_updates", []):
            update_score(update["skill_id"], update["helpful"])
        
        # If feedback was negative, mark all retrieved skills as potentially harmful
        if feedback == "negative" and retrieved_skill_ids:
            for sid in retrieved_skill_ids:
                update_score(sid, helpful=False)
        
        # If feedback was positive, mark retrieved skills as helpful
        if feedback == "positive" and retrieved_skill_ids:
            for sid in retrieved_skill_ids:
                update_score(sid, helpful=True)
                
    except Exception as e:
        logger.warning(f"Reflection failed (non-blocking): {e}")
```

---

## 6. Wiring Into Intelligence (`routes/intelligence.py`)

Your Intelligence endpoint is ~1,530 lines. The changes are surgical -- two insertion points.

### Insertion Point 1: Skill Retrieval (before Claude call)

In your system prompt construction, after the ontology-generated prompt and before the messages array:

```python
# --- EXISTING: build system prompt from ontology ---
system_prompt = load_ontology_prompt()  # your generated_prompt.md

# --- NEW: inject relevant skills ---
from server.engines.skillbook import retrieve_skills, format_skills_for_prompt

# domain comes from your Haiku classifier / query router
skills = retrieve_skills(domain=classified_domain, query=user_query)
skill_section = format_skills_for_prompt(skills)
retrieved_skill_ids = [s["skill_id"] for s in skills]

# Append skills to system prompt (after ontology, before response rules)
system_prompt = system_prompt + skill_section

# --- EXISTING: build messages, call Claude, stream SSE ---
```

### Insertion Point 2: Async Reflection (after SSE done event)

After you send the `event: done` SSE event:

```python
# --- EXISTING: send done event ---
yield f"event: done\ndata: {{}}\n\n"

# --- NEW: fire async reflection (non-blocking) ---
import asyncio
from server.engines.reflector import reflect_on_response

asyncio.create_task(reflect_on_response(
    query=user_query,
    domain=classified_domain,
    sql_traces=executed_queries,      # you already collect these for the query trace
    response_text=full_response_text,  # accumulate during streaming
    feedback=None,                     # initially none, updated on thumbs up/down
    retrieved_skill_ids=retrieved_skill_ids,
))
```

### Insertion Point 3: Feedback Endpoint (new)

```python
@router.post("/api/intelligence/feedback")
async def intelligence_feedback(
    conversation_id: str,
    feedback: str,           # "positive" | "negative" | "correction:..."
    skill_ids: list[str] = None,
):
    """
    Process user feedback on an Intelligence response.
    Updates skill scores and triggers targeted reflection.
    """
    from server.engines.reflector import reflect_on_response
    
    # Look up the conversation trace (you'd store this briefly in memory or cache)
    trace = get_conversation_trace(conversation_id)
    if not trace:
        return {"status": "trace_expired"}
    
    asyncio.create_task(reflect_on_response(
        query=trace["query"],
        domain=trace["domain"],
        sql_traces=trace["sql_traces"],
        response_text=trace["response_text"],
        feedback=feedback,
        retrieved_skill_ids=skill_ids or trace.get("skill_ids", []),
    ))
    
    return {"status": "feedback_received"}
```

---

## 7. Ontology Integration

### New file: `ontology/domains/skillbook.yaml`

```yaml
name: skillbook
description: >
  Learned domain intelligence from prior analyses. Skills are validated insights
  that improve Intelligence accuracy over time. Categories include strategies,
  caveats, failure modes, domain rules, and query patterns.
quality_tier: gold
primary_tables:
  - name: fact_skillbook
    rows: dynamic
    description: Self-curating skillbook of Medicaid domain intelligence
    columns:
      - skill_id: Unique identifier
      - domain: Ontology domain this skill applies to
      - category: strategy | caveat | failure_mode | domain_rule | query_pattern
      - content: The insight (1-3 sentences)
      - helpful_count: Times this skill contributed to a correct answer
      - harmful_count: Times this skill contributed to an incorrect answer
      - net_score: helpful_count - harmful_count
      - provenance: Source citation (CFR, CMS doc, SPA)
      - active: Whether this skill is currently in use
    known_issues:
      - "Skills are probabilistic -- net_score < 0 may indicate the skill needs revision, not deletion"
      - "New skills start unvalidated (score 0). Weight accordingly."
```

After creating this, run:
```bash
python scripts/validate_ontology.py
python scripts/generate_ontology.py
```

Intelligence now knows the skillbook exists and can query it directly ("What skills has Aradune learned about pharmacy pricing?").

---

## 8. Admin & Audit Interface

### `server/routes/skillbook.py`

```python
from fastapi import APIRouter
from server.db import get_connection

router = APIRouter(prefix="/api/skillbook", tags=["skillbook"])

@router.get("/")
async def list_skills(domain: str = None, active: bool = True, limit: int = 50):
    """List skills, optionally filtered by domain."""
    conn = get_connection()
    where = ["active = ?"]
    params = [active]
    if domain:
        where.append("domain = ?")
        params.append(domain)
    
    query = f"""
        SELECT * FROM fact_skillbook 
        WHERE {' AND '.join(where)}
        ORDER BY net_score DESC, created_at DESC
        LIMIT ?
    """
    params.append(limit)
    result = conn.execute(query, params).fetchdf()
    return result.to_dict(orient="records")

@router.get("/stats")
async def skillbook_stats():
    """Skillbook health metrics."""
    conn = get_connection()
    return conn.execute("""
        SELECT 
            COUNT(*) as total_skills,
            COUNT(*) FILTER (WHERE active) as active_skills,
            COUNT(*) FILTER (WHERE net_score > 0) as validated_skills,
            COUNT(*) FILTER (WHERE net_score < 0) as suspect_skills,
            SUM(times_retrieved) as total_retrievals,
            AVG(net_score) as avg_score,
            COUNT(DISTINCT domain) as domains_covered
        FROM fact_skillbook
    """).fetchdf().to_dict(orient="records")[0]

@router.post("/manual")
async def add_manual_skill(domain: str, category: str, content: str, provenance: str = None):
    """Manually add a skill (for seeding or expert input)."""
    from server.engines.skillbook import add_skill
    skill_id = add_skill(
        domain=domain, category=category, content=content,
        source_type="manual", provenance=provenance
    )
    return {"skill_id": skill_id}

@router.delete("/{skill_id}")
async def deactivate_skill(skill_id: str):
    """Retire a skill."""
    from server.engines.skillbook import retire_skill
    retire_skill(skill_id)
    return {"status": "retired"}
```

---

## 9. Frontend Integration

### Thumbs Up/Down on Intelligence Responses

In `IntelligenceChat.tsx`, after each response, add feedback buttons:

```tsx
// After the response markdown renders:
<div style={{ display: 'flex', gap: 8, marginTop: 8, opacity: 0.6 }}>
  <button 
    onClick={() => sendFeedback(conversationId, 'positive', skillIds)}
    style={{ background: 'none', border: 'none', cursor: 'pointer', fontSize: 16 }}
    title="This was helpful"
  >
    👍
  </button>
  <button
    onClick={() => sendFeedback(conversationId, 'negative', skillIds)}
    style={{ background: 'none', border: 'none', cursor: 'pointer', fontSize: 16 }}
    title="This was wrong or unhelpful"
  >
    👎
  </button>
</div>
```

The `sendFeedback` function calls `POST /api/intelligence/feedback`, which triggers a targeted reflection with the feedback signal.

### Skillbook Dashboard (Admin)

Add a route `/#/admin/skillbook` that shows:
- Total skills by domain (bar chart)
- Top validated skills (sorted by net_score)
- Suspect skills (net_score < 0, candidates for review)
- Recent additions (last 7 days)
- Retrieval frequency (which skills are being used most)

This is a Phase 2 feature, but the API endpoints above support it from day one.

---

## 10. Phased Rollout

### Phase 1: Seed + Retrieve (1-2 sessions)

**What:** Create `fact_skillbook` table. Seed ~25 skills from existing rules. Wire retrieval into Intelligence prompt. No reflection yet.

**Files to create:**
- `server/engines/skillbook.py` (retrieval + CRUD only)
- `scripts/seed_skillbook.py`
- `ontology/domains/skillbook.yaml`

**Files to modify:**
- `server/db.py` (register table)
- `server/routes/intelligence.py` (inject skills into prompt)

**Validation:** Run 10 test queries before/after. Do responses improve when domain rules are pre-loaded? Are caveats (DOGE quarantine, T-MSIS encounter limits) consistently applied?

**Cost:** $0 incremental per query (just string concatenation in prompt).

### Phase 2: Reflection Loop (1-2 sessions)

**What:** Add async Reflector. After each Intelligence response, Haiku analyzes the trace and proposes new skills or score updates.

**Files to create:**
- `server/engines/reflector.py`

**Files to modify:**
- `server/routes/intelligence.py` (fire async reflection after done event)

**Cost:** ~$0.004 per reflection (Haiku). At 100 queries/day = $0.40/day.

**Validation:** Monitor `fact_skillbook` growth. Are new skills being added? Are they high quality? Manually review first 50 auto-generated skills.

### Phase 3: Feedback Loop (1 session)

**What:** Add thumbs up/down. Wire feedback into Reflector. Add skill score decay (skills that haven't been retrieved in 30 days get `active = false`).

**Files to create:**
- `server/routes/skillbook.py` (admin endpoints)

**Files to modify:**
- `server/routes/intelligence.py` (feedback endpoint)
- `src/IntelligenceChat.tsx` (feedback buttons)

**Cost:** $0 incremental (feedback updates are DuckDB writes).

### Phase 4: CARE-ACE Governance (future)

**What:** Add audit trail to every Intelligence query: which skills were retrieved, what SQL was run, what reasoning path was followed, what confidence was assigned. This is the enterprise/consulting sell.

**New table:** `fact_intelligence_trace` with full query provenance.

**Value:** When a consulting firm asks "how did Aradune arrive at this CPRA figure?", you can show the complete reasoning chain including which learned skills influenced the answer.

---

## 11. Cost Model

| Component | Per Query | At 100/day | At 1000/day |
|-----------|-----------|------------|-------------|
| Skill retrieval | ~$0 (DuckDB read) | $0 | $0 |
| Prompt injection | ~$0.001 (extra tokens) | $0.10 | $1.00 |
| Async reflection | ~$0.004 (Haiku) | $0.40 | $4.00 |
| Score updates | ~$0 (DuckDB write) | $0 | $0 |
| **Total incremental** | **~$0.005** | **$0.50/day** | **$5.00/day** |

At your current Sonnet cost of ~$0.03-0.06 per query, the skillbook adds roughly 10% overhead. At Opus ($0.28), it's under 2%.

---

## 12. What This Means for Aradune's Value Proposition

### Pre-Skillbook
"Aradune has 750+ tables and Claude can query them."

### Post-Skillbook
"Aradune has 750+ tables, Claude can query them, AND the system has accumulated [N] validated Medicaid domain insights from [M] analyst queries. Every question makes the next answer better."

This is the moat from the research doc: **accumulated domain intelligence that compounds over time.** A competitor can replicate your data lake (expensively). They cannot replicate 6 months of learned Medicaid reasoning patterns.

### For the Valuation Conversation
The skillbook transforms Aradune from a "data + LLM" platform (commoditizable) into a **self-improving domain expert** (defensible). In the consulting market terminology: it's the difference between a data warehouse and an institutional knowledge base. The $3-5B Medicaid consulting market pays for the latter.

---

## 13. Build Principle Additions

Add to your 25 Rules:

**26. Intelligence learns from every query.** The Skillbook accumulates domain knowledge. Skills have provenance, scores, and audit trails. Bad skills get retired. Good skills compound.

**27. Reflection is async and non-blocking.** Users never wait for the learning step. Reflection runs on Haiku after the response is sent.

**28. Skills are not prompts.** A skill is a validated domain insight with a score, not a prompt engineering hack. Skills that don't help get retired.

---

*The data is the moat. Intelligence is the interface. The Skillbook is the compounding advantage. Build in that order.*
