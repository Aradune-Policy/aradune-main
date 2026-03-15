# ARADUNE BUILD GUIDE — FINAL
> The definitive build plan for the Aradune rebuild.
> Written 2026-03-09, updated 2026-03-12. Companion to CLAUDE.md and NEW_ARADUNE_BUILD_STRUCTURE.md.
> Hand this document to any Claude Code session alongside the current CLAUDE.md.

---

## Part 1: What We Have and What's Wrong

### What's strong

**The data lake.** 669 fact tables, 9 dimension tables, 9 reference tables, 2 compatibility views = 698 total views. 400M+ rows. 4.9 GB Parquet. Hive-partitioned, ZSTD-compressed, snapshot-versioned. DuckDB in-memory. 107 ETL scripts. Dagster orchestration. R2-synced. No consulting firm, no SaaS product, no competing platform has assembled this.

**The analytical engines.** CPRA upload (68 codes, 171 category pairs, $32.3465 CF, regulatory-correct, 821 lines). Caseload forecasting (SARIMAX+ETS, 650 lines). Expenditure modeling (cap rate/cost-per-eligible, 430 lines). All tested, all producing real outputs.

**The Intelligence backend.** `/api/intelligence` — Claude Sonnet 4.6 with extended thinking (10K budget) and direct DuckDB tool access to all 669 tables. Tools: `query_database`, `list_tables`, `describe_table`, `search_policy_corpus`, `web_search`. SSE streaming, 4-tier query routing, response caching, RAG over 1,039 CMS policy docs. Fully wired to IntelligenceChat.tsx (home page) and IntelligencePanel.tsx (sidebar from any module).

**The API layer.** 241 endpoints across 20 route files. 13 tested as returning 200 with real data. Coverage spans every data domain.

**State Profiles.** 727 lines, 18 parallel API fetches, 7 collapsible sections. The most valuable manual tool.

**CPRA dual-mode.** Pre-computed cross-state comparison (45 states, 302K rows) + user-upload compliance generator. PDF/Excel export. Both working.

### What's wrong

**1. 18 disconnected dashboards.** No tool knows about any other tool. State Profile doesn't link to Rate Analysis. Rate Analysis doesn't link to Forecasting. No connective tissue.

**2. Intelligence is not wired.** `/api/intelligence` has no frontend component. "Policy Analyst" (`/#/analyst`) hits old Vercel serverless `api/chat.js` with zero lake access. "Data Explorer" (`/#/ask`) does NL2SQL returning raw query results, not narrative. Neither is Intelligence.

**3. Arbitrary tool taxonomy.** Explore/Analyze/Build was chosen for a 3x3 grid, not because it reflects how people work.

**4. Heavy tool overlap.** 4 tools for browsing rates. 2 for AHEAD. "Rate Comparison" is actually TmsisExplorer which is actually "Spending Explorer." Data Explorer and Policy Analyst are both "ask a question" tools hitting different backends.

**5. Data quality issues.** Enrollment duplicates (FL: 204 rows, should be ~103). Census sentinels (-888888888.0). hospitals/summary returns empty. 10+ endpoints return 404.

**6. No report/export pipeline.** Every output is ephemeral. CPRA has PDF/Excel. Nothing else does beyond basic CSV. Consulting firms need documents, not chat transcripts.

**7. No data import path beyond CPRA.** CPRA upload works well. Caseload forecasting accepts CSV. But there's no general-purpose "bring your data, cross-reference it against the lake" capability. That's one of the most valuable things Aradune could offer.

**8. StateRateEngine.js disconnected.** 1,153 lines, 42/42 tests, not wired to Rate Builder.

**9. Fly.io cold start.** ~60s S3 sync on startup. Demo cannot depend on this.

### CLAUDE.md vs. reality

| CLAUDE.md says | Reality | Action |
|----------------|---------|--------|
| Section 2: 5-layer architecture (Interface → Claude Engine → Data → Ingestion → Raw) | Actual architecture is 3 layers. Diagram includes unbuilt things (RAG, pgvector, notifications). | Rewrite with 3-layer model |
| Section 3: "AI: Claude API via Vercel serverless (api/chat.js)" | Intelligence endpoint on Fly.io is the real AI layer. `api/chat.js` is legacy. | Update |
| Section 3: "pgvector + Voyage-3-large embeddings (target — for RAG)" | Not built. | Mark as future, not current |
| Section 4: "18 live tools" in Explore/Analyze/Build | Overlap, arbitrary taxonomy, disconnected | Replace with 6-module architecture |
| Section 7: Long resolved bug list mixed with open issues | Noise. Hard to find what's broken. | Open issues only. Resolved → CHANGELOG.md |
| Section 8: Tier 1-4 with ~~strikethrough~~ done items outnumbering open 3:1 | Can't scan for what's next | Replace with this build plan |
| Section 15: 3-tier AI routing (WASM / REST / RAG+Claude) | RAG doesn't exist. Intelligence combines middle + top tiers. | Rewrite to describe Intelligence as-is |
| Section 19 Track A: "NL2SQL Data Explorer is the demo closer" | Intelligence is the closer, not NL2SQL | Update |
| 400+ lines of "Recent Changes" session logs | Journal, not reference. Claude Code reads this every session. | Move to CHANGELOG.md |

### What's correct — keep as-is

Section 1 (What Aradune Is), Section 5 (CPRA — Two Systems), Section 6 (Terminal B), Section 10 (Policy Rules), Section 12 (T-MSIS Quality Rules), Section 13 (Data Ingestion Pipeline), Section 14 (Database Schema), Section 16 (Forecasting), Section 16b (Security/HIPAA), Section 17 (Build Principles), Section 18 (Build & Deploy), Section 19 (Monetization — minor update needed).

---

## Part 2: The Product — What Aradune Actually Is for Users

Before the architecture, the modules, and the build plan — what does a person experience when they come to Aradune?

### The core principle: meet people where they are

People come to Aradune with different things:

**Some come with a question and no data.** "How does Florida's Medicaid reimbursement compare to the Southeast?" They want Intelligence to query the lake and give them an answer with real numbers. They may want to export that answer as a report section, a table, or a chart.

**Some come with a specific workflow.** "I need to build a CPRA compliance report for my state." They want the Rate Analysis module, the CPRA tab, and a structured tool with inputs and outputs. They may not want AI at all — they want a form and a button.

**Some come with their own data.** "Here's our state's fee schedule — how does it compare to similar states? Where are we most out of line? What would it cost to bring our lowest codes up?" They want to upload a file and have Aradune cross-reference it against 669 tables of national data, layer in web-sourced context (CMS policy updates, recent federal register notices, state-specific regulatory context), and produce an analysis they can use.

**Some come to browse.** "Show me everything about Ohio." They want the State Profile — a dashboard with enrollment trends, rates, hospitals, quality, workforce, pharmacy, economics. They may drill into something and then want Intelligence to explain it.

**Some come to forecast.** "What will our caseload look like next year?" They may upload their own enrollment data or use Aradune's public enrollment to start.

**All of them may want to export what they find.** As a formatted report. As tables. As figures/charts. As raw CSV. As a slide-ready summary.

### The product loop

```
┌─────────────────────────────────────────────────────────┐
│                    INPUTS (any or all)                    │
│                                                          │
│  Ask a question ──────────────────────────┐              │
│  Browse a module ─────────────────────────┤              │
│  Upload your own data ────────────────────┤              │
│  Click "Ask about this" from any view ────┘              │
│                                                          │
└────────────────────────┬────────────────────────────────┘
                         ▼
┌─────────────────────────────────────────────────────────┐
│                 ARADUNE INTELLIGENCE                     │
│                                                          │
│  Queries the data lake (669 tables, 400M+ rows)          │
│  Cross-references user-uploaded data if present          │
│  Searches the web for current policy/regulatory context  │
│  Produces narrative analysis with real numbers            │
│  Generates tables and visualizations                     │
│                                                          │
└────────────────────────┬────────────────────────────────┘
                         ▼
┌─────────────────────────────────────────────────────────┐
│                    OUTPUTS (user's choice)                │
│                                                          │
│  Chat response (inline, conversational) ─────────────┐   │
│  Exportable report section (DOCX/PDF) ───────────────┤   │
│  Downloadable table (CSV/Excel) ─────────────────────┤   │
│  Exportable figure/chart (PNG/SVG) ──────────────────┤   │
│  Full formatted report (accumulated sections) ───────┘   │
│                                                          │
└─────────────────────────────────────────────────────────┘
```

The key insight: **Intelligence is not one mode among many. It's the layer that connects all modes.** You can interact with Intelligence directly (the home page chat), or you can interact with a structured module and invoke Intelligence when you need it. Either way, Intelligence is what turns data into answers.

And the modules aren't lesser — they're focused. A rate-setting actuary running their annual CPRA wants a structured tool, not a chat box. But when they get an unexpected result and want to understand why, they want Intelligence. Both paths are first-class.

---

## Part 3: The Architecture

### Three layers

```
┌─────────────────────────────────────────────────────────────────┐
│                    ARADUNE INTELLIGENCE                         │
│                                                                 │
│  Claude Sonnet 4.6 + extended thinking + DuckDB query access   │
│  + user-uploaded data cross-reference + web search for policy  │
│  context. The connective tissue of the platform.               │
│                                                                 │
│  Available everywhere: home page, sidebar from any module,     │
│  "Ask about this" buttons, State Profile questions.            │
│  Produces: narrative, tables, charts, exportable outputs.      │
└────────────────────────────┬────────────────────────────────────┘
                             │
┌────────────────────────────┴────────────────────────────────────┐
│                    INTELLIGENCE MODULES                          │
│                                                                 │
│  Structured workflows for recurring work. Organized by what    │
│  people DO, not what data they LOOK AT.                        │
│                                                                 │
│  5 modules: States / Rates / Forecast / Providers / Workforce  │
│  + Data Import available globally (not a separate module)      │
│  Every module has Intelligence access + export options.         │
└────────────────────────────┬────────────────────────────────────┘
                             │
┌────────────────────────────┴────────────────────────────────────┐
│                    THE DATA LAKE                                 │
│                                                                 │
│  669 fact tables · 9 dimensions · 9 references · 400M+ rows   │
│  + user session data (uploaded files, parsed and queryable)    │
│  Hive-partitioned Parquet · DuckDB in-memory · R2 sync         │
└─────────────────────────────────────────────────────────────────┘
```

### Navigation

```
ARADUNE  [⌕ Intelligence]  States  Rates  Forecast  Providers  Workforce  [↑ Import]
```

- **Intelligence** — Home page. Full-page chat. Starter prompts. The first thing anyone sees.
- **States** — State Profiles. Single state or comparison mode.
- **Rates** — Rate Analysis & Fee Setting. 5 tabs.
- **Forecast** — Caseload & Fiscal Forecasting.
- **Providers** — Provider & Facility Intelligence. 5 tabs.
- **Workforce** — Workforce & Quality. 5 tabs.
- **Import** — Data import. Always accessible. Upload files that become available to Intelligence and modules for the duration of the session.

**Responsive behavior:** On screens < 1024px, collapse module links into a hamburger menu. On screens >= 1024px, show all 6 items + Import button inline. The Intelligence search bar doubles as the nav search — no separate NavSearch component needed.

### Shared application state

```typescript
interface AraduneContext {
  // Navigation state
  selectedState: string | null;
  comparisonStates: string[];

  // Intelligence panel (sidebar mode)
  intelligencePanel: {
    open: boolean;
    context: IntelligenceContext | null;
  };

  // User-imported data (session-scoped)
  importedData: {
    files: ImportedFile[];        // parsed, validated, queryable
    activeFile: string | null;   // currently selected for cross-reference
  };

  // Report builder
  reportSections: ReportSection[];

  // Demo mode
  demoMode: boolean;
}

interface ImportedFile {
  id: string;
  name: string;
  type: 'csv' | 'xlsx' | 'json';
  columns: string[];
  rowCount: number;
  preview: Record<string, any>[];  // first 10 rows
  tableName: string;               // DuckDB temp table name for querying
  uploadedAt: Date;
}

interface ReportSection {
  id: string;
  prompt: string;
  response: string;          // markdown
  queries: string[];         // SQL queries Intelligence ran
  tables: TableData[];       // structured table data
  charts: ChartSpec[];       // chart specifications
  createdAt: Date;
}
```

---

## Part 4: Data Import — How It Works

### The principle

Data import is not a separate workflow. It's a capability that enhances every other workflow. A user can import data at any point and it becomes available everywhere — to Intelligence, to modules, to export. Or they can use Aradune without importing anything and it's fully functional.

### What users can import

| Format | Examples | How it's used |
|--------|----------|---------------|
| CSV / TSV | Fee schedules, enrollment data, utilization data, budget projections, any tabular data | Parsed, validated, loaded as a DuckDB temp table. Available to Intelligence and modules. |
| Excel (XLSX) | State reports, CMS workbooks, internal analytics | Sheet selector → parsed like CSV |
| JSON | API exports, structured data dumps | Parsed into tabular format if possible |

### The import flow

```
User clicks [↑ Import] or drags file onto any page
         │
         ▼
┌──────────────────────────────────────────────────┐
│  IMPORT PANEL                                     │
│                                                   │
│  ┌─────────────────────────────────────────────┐  │
│  │  Drop a file here or click to browse        │  │
│  │  CSV, Excel, JSON — up to 50MB              │  │
│  └─────────────────────────────────────────────┘  │
│                                                   │
│  Or start without data — Aradune has 669 tables  │
│  of public Medicaid data ready to query.          │
│                                                   │
└──────────────────────────────────────────────────┘
         │
         ▼ (after upload)
┌──────────────────────────────────────────────────┐
│  VALIDATION & PREVIEW                             │
│                                                   │
│  File: florida_fee_schedule_2026.csv              │
│  Rows: 6,676  Columns: 8                         │
│  Detected: procedure_code, rate, modifier, ...    │
│                                                   │
│  Preview:                                         │
│  | procedure_code | rate   | modifier | ...  |    │
│  | 99213          | 34.29  |          | ...  |    │
│  | 99214          | 50.14  |          | ...  |    │
│                                                   │
│  [✓ Looks good — make available to Intelligence]  │
│  [✎ Edit column names]  [✕ Cancel]                │
└──────────────────────────────────────────────────┘
         │
         ▼
Data loaded as DuckDB temp table: user_upload_1
Available to Intelligence, visible in modules,
persists for the session.
```

### Backend implementation

**Endpoint:** `POST /api/import`
- Accepts multipart file upload (CSV, XLSX, JSON)
- Parses and validates (column types, row counts, basic sanity checks)
- Loads into DuckDB as a session-scoped temp table (`user_upload_{id}`)
- Returns: table name, column schema, row count, preview rows
- **Security:** User data is session-scoped. Never persisted to the lake. Never visible to other users. Never mixed with Aradune's public data layer (Build Principle #14).

**Intelligence integration:** When user data is imported, Intelligence's tool access expands:
- `query_database` can now query `user_upload_{id}` alongside all 250 lake tables
- Intelligence's system prompt is augmented: "The user has uploaded a file called '{filename}' with {N} rows and columns: {columns}. This data is available as table '{tableName}'. You can query it and cross-reference it against any table in the Aradune data lake."
- Intelligence can JOIN user data against lake tables (e.g., join user's fee schedule on `procedure_code` to `fact_rate_comparison` to compare against national data)

**Module integration:** When user data is imported, relevant modules can use it:
- Rate Analysis: "Use my uploaded fee schedule as the comparison baseline"
- Forecasting: "Use my uploaded enrollment data instead of public data"
- CPRA: Already has this — the upload tool. Generalize the pattern.

### Session management

**Problem:** Fly.io machines can stop and restart between requests. DuckDB temp tables are in-memory and do not survive machine restarts. A user who uploads a file and then asks Intelligence a question 10 minutes later may hit a restarted machine with no temp tables.

**Solution:** Store uploaded file bytes in an in-memory session store on the server side. The session store is a Python `dict` keyed by `session_id`, with each entry holding the raw file bytes, parsed metadata, and a TTL of 2 hours. On each Intelligence request that references imported data, re-hydrate the DuckDB temp table from stored bytes before querying. This adds a few hundred milliseconds of latency on cache-miss but guarantees correctness across machine restarts.

**Session ID lifecycle:**
- A UUID `session_id` is generated at upload time by the `POST /api/import` endpoint and returned to the frontend in the response.
- The frontend stores this `session_id` in `AraduneContext` and passes it with every subsequent `/api/intelligence` request and any module API calls that reference imported data.
- The backend checks whether the temp table already exists in DuckDB. If not, it looks up the session store by `session_id` and re-creates the temp table from the stored bytes.

**Memory budget:**
- Cap at **50MB per session** (file bytes stored in memory).
- Cap at **500MB total** across all active sessions.
- If the total budget is exceeded, evict sessions using **LRU** (least recently used) ordering.
- Return a clear error to the frontend if a single upload exceeds the per-session cap.

**Future alternative:** For cross-restart persistence without memory pressure, consider Redis (store serialized bytes with TTL) or Fly.io persistent volumes (write uploaded files to disk). Either approach eliminates re-hydration latency and removes the memory cap constraint, but adds infrastructure complexity. Start with the in-memory approach; migrate if session volume demands it.

### What happens when there's no imported data

Everything works the same, minus the cross-reference. Intelligence queries the 250 lake tables. Modules use their standard data sources. The platform is fully functional without any import. Import is an enhancement, not a prerequisite.

### Web context layer

In addition to the data lake and user-uploaded data, Intelligence should be able to search the web for current policy and regulatory context when relevant. This means:

- When a user asks about a specific state's Medicaid policy, Intelligence can search for recent CMS guidance, state plan amendments, federal register notices, or news coverage
- When cross-referencing user data, Intelligence can search for relevant benchmarks, policy context, or regulatory requirements that aren't in the lake
- Web results are cited separately from lake data: "Based on Aradune's rate comparison data (CY2022 T-MSIS) and a February 2026 CMS informational bulletin..."

**Implementation:** Add web search to Intelligence's tool set using the Anthropic Messages API's built-in web search tool. Add to the tools array in the API call: `{"type": "web_search_20250305", "name": "web_search", "max_uses": 3}`. This is a server-side tool built into the Anthropic Messages API — no custom tool implementation is needed. The API handles search execution and returns results directly. Intelligence decides when to use it based on the query — most data questions hit the lake, but policy/regulatory questions benefit from current web context. Requires Anthropic SDK >=0.49.0 for web search support.

---

## Part 5: Export Pipeline — Getting Intelligence Out

### The principle

Every piece of intelligence Aradune produces — whether from a chat response, a module view, or a cross-reference analysis — should be exportable in the format the user needs. Some people want a quick answer in chat. Some want a formatted report for a client meeting. Some want the raw data table to drop into their own analysis. All of these are valid.

### Export options available everywhere

| Export type | What it produces | Available from |
|-------------|------------------|----------------|
| **Chat response** | Inline markdown with tables and narrative | Intelligence (default output) |
| **Report section** | Formatted analysis block saved to Report Builder | "Save to Report" button on any Intelligence response |
| **Table (CSV)** | Raw data table download | Any table display in any module or Intelligence response |
| **Table (Excel)** | Formatted Excel with headers | Any table display |
| **Figure (PNG/SVG)** | Chart or visualization download | Any chart in any module or Intelligence response |
| **Full report (DOCX)** | Accumulated report sections as a formatted Word document | Report Builder panel |
| **Full report (PDF)** | Same, as PDF | Report Builder panel |

### Report Builder

Not a separate module — a persistent panel accessible from the nav. Users accumulate sections over the course of a session and export when ready.

**How it works:**

1. User interacts with Intelligence or modules
2. Any Intelligence response has a "Save to Report" button
3. Any module view has an "Add to Report" button (captures the current view as a report section)
4. Saved sections appear in the Report Builder panel
5. User can reorder, annotate, delete sections
6. Export as DOCX or PDF with:
   - Cover page: "Medicaid Intelligence Report — [Topic] — Aradune — [Date]"
   - Each section: the prompt/context, the analysis, tables, chart placeholders, data citations
   - Footer: "Generated by Aradune · Data sources: [list of tables queried]"

**Implementation:**
- Report state lives in AraduneContext (session-scoped)
- Each section stores: source (Intelligence prompt + response, or module view), markdown content, structured tables, chart specs, queries executed
- DOCX generation via docx-js (same pattern as CPRA PDF export — already proven)
- PDF generation via the HTML-to-PDF path (existing in CPRA upload's `generate/report` endpoint)

### Intelligence response format

When Intelligence produces a response, it should structure the output so exports work cleanly:

```json
{
  "narrative": "Florida's E&M rates sit significantly below...",
  "tables": [
    {
      "title": "E&M Rate Comparison: FL vs Southeast vs Medicare",
      "columns": ["Code", "Description", "FL Rate", "SE Avg", "Medicare", "FL % MCR"],
      "rows": [["99213", "Office visit, est.", "$34.29", "$52.18", "$91.39", "37.5%"], ...]
    }
  ],
  "charts": [
    {
      "type": "bar",
      "title": "FL Medicaid Rates as % of Medicare",
      "data": [{"code": "99213", "pct": 37.5}, ...]
    }
  ],
  "queries": ["SELECT ... FROM fact_rate_comparison WHERE ..."],
  "citations": ["fact_rate_comparison (CY2022 T-MSIS)", "fact_bls_wage (BLS OEWS 2024)"],
  "web_sources": []
}
```

The frontend renders this as a rich response: narrative text + interactive table + chart + collapsible query trace. Each piece is independently exportable.

**Streaming approach:** Use SSE (Server-Sent Events) to deliver Intelligence responses. Stream the narrative text token-by-token as `event: token` messages. After the narrative completes, emit a single `event: metadata` message containing the structured JSON (tables, charts, queries, citations). The frontend renders narrative in real-time as it streams, then renders tables and charts when the metadata event arrives.

For tool use rounds, emit `event: tool_call` with the tool name and purpose so the frontend can show status updates like "Querying fact_rate_comparison..." while Intelligence works.

**Example SSE event sequence:**

```
event: status\ndata: {"status": "thinking"}\n\n
event: tool_call\ndata: {"name": "query_database", "purpose": "Looking up FL rates"}\n\n
event: tool_result\ndata: {"name": "query_database", "rows": 45, "ms": 23}\n\n
event: token\ndata: {"text": "Florida's"}\n\n
event: token\ndata: {"text": " E&M rates"}\n\n
...
event: metadata\ndata: {"tables": [...], "charts": [...], "queries": [...], "citations": [...]}\n\n
event: done\ndata: {}\n\n
```

---

## Part 6: Module Specifications

### Module 0: Aradune Intelligence (Home Page)

**Route:** `/#/`

**Replaces:** Policy Analyst (`/#/analyst`), Data Explorer (`/#/ask`). Data Catalog remains as a lightweight standalone page at `/#/catalog` for power users who want to browse table schemas quickly without a chat interaction.

**Backend:** `/api/intelligence` — already built. Needs: comprehensive system prompt, streaming (SSE), context injection, user-data awareness, web search tool, structured output format.

**Backend changes needed:**

1. **System prompt** — See Part 10 for full draft. Must include: lake summary, join keys, quality rules, policy rules, output format instructions, behavioral instructions (narrative + "so what" + citations).

2. **Streaming** — SSE via FastAPI `StreamingResponse`. Token-by-token display.

3. **Context injection** — When invoked from a module's "Ask about this" button, receives context:
```json
{
  "module": "state_profile",
  "state": "FL",
  "section": "rates",
  "context_summary": "User is viewing FL rate data. Avg E&M is 62% of Medicare."
}
```

4. **User data awareness** — When imported data exists, system prompt is augmented and `query_database` tool can access user temp tables alongside lake tables.

5. **Web search tool** — Add web search to Intelligence's tool set using the Anthropic Messages API's built-in web search tool. Add to the tools array in the API call: `{"type": "web_search_20250305", "name": "web_search", "max_uses": 3}`. This is a server-side tool built into the Anthropic Messages API — no custom tool implementation is needed. The API handles search execution and returns results directly. Requires Anthropic SDK >=0.49.0 for web search support.

6. **Structured output with streaming** — Response includes narrative, tables, charts, queries, citations as structured JSON that the frontend renders and that can be exported. Use SSE (Server-Sent Events) for delivery: stream narrative text token-by-token as `event: token` messages, emit `event: tool_call` with tool name/purpose for status updates during tool use rounds, and emit a final `event: metadata` message containing the structured JSON (tables, charts, queries, citations) after narrative completes. The frontend renders narrative in real-time as it streams, then renders tables/charts when the metadata event arrives. See Part 5 "Intelligence response format" for the full SSE event sequence.

7. **Conversation memory** — Frontend sends full messages array. Follow-ups work.

**Frontend components:**

| Component | Purpose |
|-----------|---------|
| `IntelligenceChat.tsx` | Full-page chat. Streams markdown. Renders tables and charts inline. Shows query trace. Export buttons on each response. |
| `StarterPrompts.tsx` | Grid of 6-8 prompts by persona. Disappears after first query. |
| `InputBar.tsx` | Fixed bottom. Auto-expanding textarea. File drop zone (triggers import). |
| `QueryTrace.tsx` | Collapsible "Queries executed (N)" showing SQL. |
| `ResponseExport.tsx` | Per-response buttons: Save to Report, Export Table (CSV/Excel), Export Chart (PNG). |
| `IntelligencePanel.tsx` | Right-side panel version for use inside modules. Same rendering, smaller viewport. |

**Starter prompt categories:**

Rate analysis:
- "Compare Florida's E&M rates to the Southeast average and Medicare"
- "Which states pay primary care providers above 80% of Medicare?"
- "What would it cost Florida to raise all rates below 50% of Medicare to 60%?"

State intelligence:
- "Give me a comprehensive profile of Ohio's Medicaid program"
- "Which states have the longest HCBS waitlists relative to their spending?"
- "How did the PHE unwinding affect enrollment in expansion vs non-expansion states?"

Hospital/provider:
- "Compare teaching hospitals in Florida by operating margin and Medicaid payer mix"
- "Which states have the highest nursing facility staffing deficiencies?"

Fiscal/forecasting:
- "What's driving enrollment growth in the Southeast?"
- "How does Florida's per-enrollee spending compare to states with similar FMAP?"

---

### Module 1: State Profiles

**Route:** `/#/state/{code}` | Comparison: `/#/state/FL+GA+TX`

**Current:** StateProfile.tsx — 727 lines, 18 parallel API fetches, 7 sections. Working.

**Changes:**

1. **Comparison mode.** Multi-state URL. Side-by-side columns. Comparison summary table at top. Internally, state is always an array (`["FL"]` or `["FL", "GA", "TX"]`).

2. **Cross-dataset insights ("So What").** After all fetches, call `/api/intelligence` with assembled data as context. Ask for 3-5 proactive observations. Cache per state/combination. Example: "FL has below-average rates (62% of Medicare) AND above-average HCBS waitlists (72,000 waiting) — this combination suggests workforce supply constraints driven by low reimbursement."

3. **"Ask about [state]" button.** Opens Intelligence sidebar with state context.

4. **Graceful degradation.** If a fetch 404s, show "Data unavailable" for that section. Don't break the page.

5. **Section-level export.** Each section: Export CSV, Export Chart, Add to Report.

6. **Import integration.** If user has imported data, show an "Overlay your data" option that compares their uploaded rates/enrollment/etc. against the state profile.

**Data sources:** dim_state, fact_enrollment, fact_rate_comparison, fact_hospital_cost, fact_quality_measure, fact_quality_core_set_2024, fact_bls_wage, fact_drug_utilization, fact_sdud_2025, fact_acs_state, fact_unemployment, fact_median_income, fact_state_gdp, fact_hpsa, fact_nsduh_prevalence, fact_hcbs_waitlist, fact_expenditure, fact_scorecard, fact_unwinding, fact_mc_enrollment_summary + Intelligence for anything not surfaced.

---

### Module 2: Rate Analysis & Fee Setting

**Route:** `/#/rates`

**5 tabs:**

| Tab | Default | Absorbs | Source |
|-----|---------|---------|--------|
| Browse & Compare | ✓ | TmsisExplorer, RateDecay, RateLookup | TmsisExplorer.tsx (3,332 lines) — wrap, don't rewrite |
| State Fee Schedules | | FeeScheduleDir | FeeScheduleDir.tsx (535 lines) |
| Rate Builder | | RateBuilder (unwired) | StateRateEngine.js (1,153 lines, 42/42 tests) — finally wire |
| CPRA Compliance | | CpraGenerator, ComplianceReport | CpraGenerator.tsx (1,096 lines) |
| Impact Analysis | | RateReductionAnalyzer | RateReductionAnalyzer.tsx (411 lines) |

**Design:**
- Tab navigation is persistent — switching tabs doesn't lose state
- Shared `selectedState` — if you're looking at FL in Browse & Compare, switching to CPRA defaults to FL
- "Ask Intelligence" button on every tab
- Old routes redirect: `/#/cpra` → `/#/rates?tab=cpra`, `/#/decay` → `/#/rates`, etc.
- **Import integration:** "Upload your fee schedule" option in Browse & Compare that cross-references against national data. In Rate Builder, uploaded rates can be used as baseline. In CPRA, this already works (CPRA upload tool).

**TmsisExplorer refactor:** Wrap in tab container. Don't rewrite. If time permits, decompose into sub-components. But wrapping works immediately.

**Data:** fact_medicaid_rate (597K), fact_rate_comparison (302K), fact_medicare_rate_state (417K), dim_procedure (16,978), fact_claims (713K), fact_bls_wage, fact_dq_flag (269K)

---

### Module 3: Caseload & Fiscal Forecasting

**Route:** `/#/forecast`

**Current:** CaseloadForecaster.tsx — 1,092 lines. Upload-driven. Tab toggle caseload/expenditure.

**Changes:**

1. **Dual-mode entry.** The forecaster should offer two paths up front:
   - "Use public data" — select a state, auto-populate from `fact_enrollment`. No upload needed.
   - "Upload your data" — the existing CSV upload path for custom/internal data.
   Both paths feed the same SARIMAX+ETS engine. Both produce the same outputs.

2. **Intelligence integration.** "What drove FL's enrollment spike in Q3 2024?" button → Intelligence queries enrollment + unwinding + economic data with forecast context.

3. **Historical accuracy overlay.** If previous forecasts exist and actuals are now available, show them overlaid. Build Principle #15.

4. **Export.** Forecast chart as PNG. Forecast data as CSV/Excel. Forecast narrative (from Intelligence) as report section.

**Engines:** caseload_forecast.py (SARIMAX+ETS), expenditure_model.py (cap rate/cost-per-eligible)

**Data:** fact_enrollment, fact_elig_group_monthly/annual, fact_unemployment, fact_expenditure, fact_fmr_supplemental

---

### Module 4: Provider & Facility Intelligence

**Route:** `/#/providers`

**5 tabs:**

| Tab | Absorbs | Source |
|-----|---------|--------|
| Hospital Search | Hospital Intelligence (search) | Existing hospital search endpoints |
| Hospital Detail | Hospital Intelligence (CCN detail + peers) | Existing CCN lookup + peer benchmarking |
| AHEAD Readiness | AheadReadiness + AheadCalculator (merged) | AheadReadiness.tsx + AheadCalculator.tsx |
| Nursing Facilities | Existing nursing-facilities endpoint | Five-Star, PBJ staffing, SNF cost |
| Facility Directory | New — uses existing fact tables | FQHCs, dialysis, hospice, HHA, IRF, LTCH |

**Design:**
- Hospital Search as default tab
- "Ask Intelligence" on every tab
- **Import integration:** Upload hospital financial data for custom peer benchmarking. Upload facility data for cross-referencing against national quality/staffing measures.

**Data:** fact_hospital_cost, fact_snf_cost, fact_five_star, fact_pbj_nurse_staffing, fact_hospital_rating, fact_hospital_vbp, fact_dsh_hospital, fact_fqhc_directory, fact_dialysis_facility, fact_hospice_provider, fact_home_health_agency, etc.

---

### Module 5: Workforce & Quality

**Route:** `/#/workforce`

**5 tabs:**

| Tab | Absorbs | Source |
|-----|---------|--------|
| Wage Comparison | WageAdequacy | WageAdequacy.tsx (546 lines) |
| Quality Measures | QualityLinkage | QualityLinkage.tsx (445 lines) |
| HCBS Pass-Through | HcbsCompTracker | HcbsCompTracker.tsx (414 lines) |
| Workforce Supply | New | fact_hpsa (69K), fact_workforce_projections, fact_nursing_workforce |
| Shortage Areas | New | fact_hpsa, fact_mua_designation (19.6K) — map visualization |

**Design:**
- Wage Comparison as default tab
- "Ask Intelligence" on every tab
- **Import integration:** Upload workforce survey data, wage data, or quality measure data for cross-referencing against national benchmarks.

**Data:** fact_bls_wage, fact_quality_measure, fact_quality_core_set_2024, fact_scorecard, fact_hpsa, fact_workforce_projections, fact_nursing_workforce, fact_nhsc_field_strength, fact_mua_designation, fact_hcbs_waitlist

---

### 18 → 6 Complete Mapping

| Old Tool | New Home | Action |
|----------|----------|--------|
| State Intelligence | **State Profiles** | Keep, enhance |
| Data Explorer (NL2SQL) | **Intelligence** | Replace |
| Rate Comparison (TmsisExplorer) | **Rates** → Browse & Compare | Consolidate |
| Data Catalog | Keep as lightweight standalone at `/#/catalog` | Keep as lightweight standalone at /#/catalog |
| Workforce & Quality | **Workforce** → Wage Comparison | Keep |
| CPRA Generator | **Rates** → CPRA Compliance | Move |
| Hospital Intelligence | **Providers** → Hospital Readiness | Move |
| Compliance Center | **Rates** → CPRA Compliance | Merge |
| Forecasting | **Forecast** | Keep |
| Policy Analyst | **Intelligence** | Replace |
| Spending Explorer | **Rates** → Browse & Compare | Consolidate |
| Rate Decay | **Rates** → Browse & Compare | Consolidate |
| Fee Schedule Directory | **Rates** → State Fee Schedules | Consolidate |
| Rate Lookup | **Rates** → Browse & Compare | Consolidate |
| Rate Builder | **Rates** → Rate Builder | Wire StateRateEngine |
| AHEAD Calculator | **Providers** → AHEAD Readiness | Merge |
| Quality Linkage | **Workforce** → Quality Measures | Consolidate |
| Rate Reduction Analyzer | **Rates** → Impact Analysis | Consolidate |
| HCBS Compensation Tracker | **Workforce** → HCBS Pass-Through | Consolidate |

---

## Part 7: Build Plan

No arbitrary timelines. Just the work, in order, with dependencies clear.

### Phase 0: Fix the Foundation

Everything in Phase 0 must be done before any new build work. These are bugs and deployment issues that will undermine everything else.

**Data quality:**
- [x] Deduplicate fact_enrollment (FL had 204 rows, now ~103 unique months — GROUP BY year,month + MAX() in lake.py query)
- [x] Replace Census sentinel values (-888888888.0 → NULL) in fact_acs_state (added _CENSUS_SENTINELS filter in build_lake_census.py, rebuilt Parquet, synced to R2)
- [x] Fix hospitals/summary endpoint (route ordering fix in hospitals.py)
- [x] Fix five-star/summary route ordering (quality.py)
- [x] Fix hpsa/summary route ordering (quality.py)
- [ ] Audit and fix the 10+ endpoints returning 404 — determine stale deploy vs code bug for each

**Deployment:**
- [x] Redeploy Fly.io with all 669 tables (4.9 GB lake baked into Docker image, verified at https://aradune-api.fly.dev/)
- [ ] Test top 20 most-used endpoints post-deploy
- [x] Address cold start: lake pre-baked into Docker image (4.9 GB, eliminates 60s S3 sync on startup)
- [ ] Confirm all 20 route files are registered and accessible

**Cleanup:**
- [ ] Delete `public/data/cpra_precomputed.json`
- [ ] Delete `scripts/build-cpra-data.mjs`
- [ ] Clean up `docs/` per Section 20 of CLAUDE.md

---

### Phase 1: Intelligence + Platform Restructure

Intelligence is the centerpiece. The platform restructure creates the 6-module navigation. Both happen together because Intelligence IS the home page.

**Intelligence frontend:**
- [x] Build `IntelligenceChat.tsx` — full-page chat, markdown rendering, table display, streaming (900 lines)
- [x] Build `StarterPrompts.tsx` — 6 prompts by persona (integrated into IntelligenceChat)
- [x] Build `InputBar.tsx` — fixed bottom, auto-expanding, file drop zone (integrated into IntelligenceChat)
- [x] Build `QueryTrace.tsx` — collapsible SQL trace (integrated into IntelligenceChat)
- [x] Build `ResponseExport.tsx` — Copy, Export CSV, Save to Report per response (integrated into IntelligenceChat)
- [x] Wire to `/api/intelligence` with streaming (SSE)
- [x] Conversation memory (messages array in state)

**Intelligence backend:**
- [x] Write comprehensive system prompt (auto-generated from ontology + static rules, ~1,100 words)
- [x] Add streaming support (SSE via FastAPI StreamingResponse — `/api/intelligence/stream`)
- [x] Add web search tool (Anthropic Messages API built-in web search)
- [x] Add context injection endpoint parameter
- [x] Add structured output format (narrative + tables + queries + citations via SSE metadata event)
- [x] Add user-data awareness (augment system prompt when imported data exists)
- [ ] Test with 15+ representative queries across all data domains
- [ ] Deprecate `api/chat.js` — keep it running but redirect PolicyAnalyst.tsx to use `/api/intelligence` instead. Migrate the FL methodology addendum and system_prompt.md content into Intelligence's system prompt. Once Intelligence is verified working in production, remove api/chat.js in a follow-up cleanup.

**Platform restructure:**
- [x] 6-module nav with Import button (States, Rates, Forecast, Providers, Workforce + Import + Report)
- [x] Intelligence as home page (root route `/#/` and `/#/intelligence`)
- [x] Lazy-load all modules
- [ ] Old routes redirect (e.g., `/#/cpra` → `/#/rates?tab=cpra`)
- [x] Build `AraduneContext` provider (selected state, intelligence panel, imported data, report sections — 124 lines)
- [x] Build `IntelligencePanel.tsx` — right-side sidebar version for use inside modules (271 lines)
- [x] Add "Ask Intelligence" button to all module shells

---

### Phase 2: Data Import + Export Pipeline

These are platform-level capabilities that enhance every module. Build them before the module consolidation so modules can integrate them.

**Data import:**
- [x] Build `POST /api/import` endpoint (CSV/XLSX/JSON → parsed → DuckDB temp table)
- [x] File validation: column type detection, row count, basic sanity checks
- [x] Build `ImportPanel.tsx` — drag-and-drop, file preview, column editor, confirmation (534 lines)
- [x] Session-scoped storage (temp tables, 2h TTL, LRU eviction, 500MB cap, never persisted)
- [x] Augment Intelligence system prompt when user data is present
- [x] Add user temp tables to Intelligence's `query_database` scope
- [ ] Test: upload a state fee schedule → ask Intelligence to compare it to national data → get cross-referenced analysis

**Export pipeline:**
- [x] Build `ReportBuilder.tsx` — panel for accumulated sections, delete, CSV export (372 lines)
- [x] "Save to Report" button on Intelligence responses → stores to AraduneContext
- [x] "Add to Report" button on module views (StateProfile, CaseloadForecaster done; remaining modules need wiring)
- [x] CSV export utility (shared, works on any table data)
- [x] Excel export utility (reportXlsx.ts — multi-sheet, per-table data sheets, via `xlsx` library)
- [x] Chart export (chartExport.ts — PNG 2x retina + SVG via Canvas API, ChartActions component)
- [x] DOCX report generation (reportDocx.ts — cover page, brand bar, sections, tables, via `docx` library)
- [x] PDF report generation (reportPdf.ts — branded PDF via jspdf + autotable)
- [x] Shared markdown parser (reportMarkdown.ts — converts Intelligence responses to typed blocks)
- [x] ChartActions wired into CaseloadForecaster (2 charts) and StateProfile (2 charts)

---

### Phase 3: Module Consolidation

All 5 modules get built. Each module wraps existing components into a tabbed container, adds Intelligence integration, adds import awareness, adds export buttons.

**Module 2 — Rate Analysis (highest complexity, most tool absorption):**
- [x] Build `RateAnalysis.tsx` — wrapper with 5-tab navigation (165 lines)
- [x] Browse & Compare tab: wrap TmsisExplorer.tsx with shared state
- [x] State Fee Schedules tab: wrap FeeScheduleDir.tsx
- [x] Rate Builder tab: WIRED StateRateEngine.js
- [x] CPRA Compliance tab: wrap CpraGenerator.tsx (both modes)
- [x] Impact Analysis tab: wrap RateReductionAnalyzer.tsx
- [x] Shared state: selected state persists across tabs
- [x] "Ask Intelligence" button on every tab with rate context
- [x] Route redirects for all old rate tool routes
- [ ] Import integration: "Upload your fee schedule" triggers import, then Browse & Compare shows it alongside national data
- [ ] Export: per-tab CSV/Excel/Chart export, per-tab "Add to Report"

**Module 1 — State Profiles:**
- [x] Comparison mode: multi-state URL (`/#/state/FL+GA+TX`), side-by-side rendering (1,640 lines)
- [x] Cross-dataset insights: 7 client + server insights merged and deduplicated
- [x] "Ask about [state]" button → opens Intelligence sidebar with state context
- [ ] Fix 404 fetches (graceful degradation per section)
- [x] Chart export (ChartActions on enrollment + rate distribution charts)
- [x] "+ Report" button on toolbar
- [ ] Section-level CSV export per card
- [ ] Import overlay: if user data exists, show comparison against state profile data

**Module 3 — Forecasting:**
- [x] Dual-mode entry: "Use public data" + "Upload your data" (1,100 lines)
- [x] Intelligence integration: "Ask Aradune" button with forecast context
- [x] Scenario builder: 4 sliders (unemployment, eligibility, rate change, MC shift)
- [ ] Historical accuracy overlay (forecasts vs actuals)
- [x] Chart export (ChartActions on caseload + expenditure charts)
- [x] "+ Report" button on toolbar
- [ ] Data CSV/Excel export from forecast results

**Module 4 — Provider Intelligence:**
- [x] Build wrapper with 5-tab navigation (ProviderIntelligence.tsx)
- [x] Hospital Readiness tab (AheadReadiness)
- [x] AHEAD Calculator tab (AheadCalculator)
- [x] Nursing Facilities tab (Five-Star ratings, PBJ staffing, SNF cost — NursingFacilities.tsx)
- [x] Facility Directory tab (FQHCs, dialysis, hospice, HHA, IRF, LTCH — FacilityDirectory.tsx)
- [x] Spending Explorer tab (TmsisExplorer)
- [x] "Ask Intelligence" on every tab
- [ ] Import integration: upload hospital/facility data for custom benchmarking

**Module 5 — Workforce & Quality:**
- [x] Build wrapper with 6-tab navigation (WorkforceQuality.tsx)
- [x] Wage Comparison tab (WageAdequacy.tsx)
- [x] Quality Measures tab (QualityLinkage.tsx)
- [x] HCBS Pass-Through tab (HcbsTracker.tsx)
- [x] Workforce Supply tab (BLS wages, NHSC clinicians, HRSA projections — WorkforceSupply.tsx)
- [x] Shortage Areas tab (HPSA designations, MUA/MUP — ShortageAreas.tsx)
- [x] Compliance tab (ComplianceReport.tsx)
- [x] "Ask Intelligence" on every tab
- [ ] Import integration: upload workforce/quality data for cross-referencing

---

### Phase 4: Demo Preparation

**Demo mode:**
- [x] `?demo=true` URL parameter activates demo mode (AraduneContext.demoMode)
- [x] Pre-cache key API responses (server/cache_seeds.json has 27 entries, loaded at startup)
- [x] Pre-cache 5-10 Intelligence responses for starter prompts (scripts/build_cache_seeds.py + server cache_seeds.json loader)
- [x] Subtle "DEMO" indicator in nav bar when demoMode active
- [ ] Ensure demo works without live Fly.io (all cached)

**Demo resilience:**
- [ ] Test with Fly.io cold start — confirm fallback works
- [ ] Test every module transition
- [ ] Test import → Intelligence cross-reference → export flow end-to-end

**Demo script:**
- [ ] Write the walkthrough: what to open, what to type, what to show
- [ ] Prepare 3-5 backup queries in case live ones fail
- [ ] Dry-run end-to-end with timing

**Visual polish:**
- [ ] Consistent loading states across all modules
- [ ] Error states that don't look like crashes
- [ ] Confirm Lottie sword loader works smoothly
- [ ] Mobile/laptop responsiveness (minimum: don't break on presenter's screen)

---

### Phase 5: Post-Demo / Ongoing

- [ ] User accounts (Clerk)
- [ ] Forecast accuracy dashboard (Build Principle #15)
- [ ] RAG over policy corpus (SPAs, waivers, CIBs — requires pgvector + Voyage embeddings)
- [ ] Hospital price transparency MRF ingestion
- [ ] Remaining fee schedules (KS, NJ, TN, WI)
- [ ] 340B covered entity data (browser automation needed)
- [ ] Historical HCRIS for 3-year trend sparklines
- [ ] UPL demonstration filings
- [ ] Stripe billing (Track B)
- [ ] White-label configuration (Track A)
- [ ] Public landing page redesign (Track B)

---

## Part 8: CLAUDE.md Restructuring Instructions

### For Claude Code: how to restructure the CLAUDE.md

The current CLAUDE.md is ~1,500 lines. It has become a session journal mixed with architecture reference. Claude Code reads this at the start of every session and most of it is resolved history. Here are the explicit instructions for restructuring:

**Step 1: Create CHANGELOG.md**

Move ALL of the following to a new `CHANGELOG.md` file:
- All "Recent Changes" sections (lines ~430-510 in current file — there are 6 of them spanning sessions 1-9)
- All resolved bugs from Section 7 (items marked ~~strikethrough~~)
- All ~~done~~ items from Section 8 tiers
- The "External Project: cpra-pipeline" subsection (keep a one-liner reference in CLAUDE.md)

**Step 2: Rewrite these sections in CLAUDE.md**

| Section | Current | New |
|---------|---------|-----|
| **2. System Architecture** | 5-layer aspirational diagram | 3-layer model from this document (Intelligence → Modules → Data Lake) |
| **3. Current Stack** | Mostly accurate | Update AI line: Intelligence endpoint is primary, not `api/chat.js`. Remove pgvector/RAG from "current" (mark as future). |
| **4. Live Tools** | 18-tool list in Explore/Analyze/Build | 6-module architecture. Include the 18→6 mapping table. List modules with their tabs. |
| **7. Known Bugs** | Mix of resolved and open | Open issues ONLY: enrollment dupes, Census sentinels, 404 endpoints, Intelligence not wired, StateRateEngine not wired, Fly.io cold start, hospitals/summary empty. Everything resolved goes to CHANGELOG.md. |
| **8. Next Steps** | Tier 1-4 with mostly-done items | Replace entirely with: "See ARADUNE_BUILD_GUIDE.md for the phased build plan. Current phase: [X]." Keep it to 10-15 lines pointing to this document. |
| **9. File Map** | Accurate but lists old tool files | Update `src/tools/` to show module structure. Note which old files are wrapped by which modules. |
| **15. AI Interface** | 3-tier routing diagram, model costs | Rewrite to describe Intelligence architecture: system prompt, tools (query_database, list_tables, describe_table, web_search), streaming, context injection, user data awareness, structured output format. Keep model cost table. |
| **19. Monetization** | Track A says "NL2SQL is demo closer" | Update Track A: Intelligence is the demo closer. Add data import + export as key value propositions. |
| **21. What Success Looks Like** | Current state + session-journal-style milestones | Update current state. Milestones should reference this build guide's phases, not ad-hoc task lists. |

**Step 3: Add these new sections to CLAUDE.md**

| New Section | Content | Placement |
|-------------|---------|-----------|
| **Intelligence System Prompt** | Summary of what Intelligence knows, its tools, behavioral rules (full draft in this build guide Part 10). Don't inline the full prompt — reference the actual prompt file. | After Section 15 |
| **Data Import Architecture** | How import works: session-scoped DuckDB temp tables, validation, Intelligence integration, security (never persisted, never shared). | After Section 16b (Security) |
| **Export Pipeline** | Report Builder, per-response exports, DOCX/PDF generation, chart export. | After Data Import |
| **Module Architecture** | The 6-module structure with tab specs, shared state, Intelligence integration pattern. Brief — point to this build guide for full specs. | Replace Section 4 |

**Step 4: Target length**

The restructured CLAUDE.md should be ~800-900 lines. The CHANGELOG.md absorbs the rest. Every line in CLAUDE.md should be currently accurate and actionable for a Claude Code session.

**Step 5: Add reference to this build guide**

At the top of CLAUDE.md, add:
```
> Build plan: See ARADUNE_BUILD_GUIDE.md for the comprehensive phased build plan,
> module specs, data import architecture, and export pipeline design.
```

---

## Part 9: Technical Decisions

### Stay with Vite + React 18 + TypeScript
No migration to Next.js. Hash-based routing is fine. Static Vercel hosting is free and fast.

### Keep FastAPI + DuckDB on Fly.io
Sound architecture. Solve cold start via pre-baked Docker image or persistent volume.

### Streaming for Intelligence
Server-Sent Events (SSE) from FastAPI. Frontend reads with `fetch()` + `ReadableStream`. Simpler than WebSockets, works through Vercel proxy.

### Data import via DuckDB temp tables
User uploads are parsed server-side and loaded as session-scoped DuckDB temp tables. Intelligence can query them alongside lake tables. No persistence. No sharing. No HIPAA concerns (Ring 0 data only unless user brings their own, in which case it's their data and their session).

### Component wrapping strategy
Don't rewrite big components. Wrap them. TmsisExplorer.tsx (3,332 lines) becomes one tab in RateAnalysis — unchanged internally, wrapped in a tab container. Same for CpraGenerator.tsx, CaseloadForecaster.tsx, etc. New code is the module wrappers, the Intelligence layer, the import pipeline, and the export pipeline.

### Web search in Intelligence
Use the Anthropic Messages API's built-in web search tool. Add to the tools array in the API call: `{"type": "web_search_20250305", "name": "web_search", "max_uses": 3}`. This is a server-side tool built into the Anthropic Messages API — no custom tool implementation is needed. The API handles search execution and returns results directly. Intelligence decides when to use it — most data queries hit the lake, but policy questions benefit from current web context. This is what makes Intelligence more than "Claude with a database" — it's Claude with a database AND the internet AND the user's data. Requires Anthropic SDK >=0.49.0 for web search support.

---

## Part 10: Intelligence System Prompt (Draft)

```
You are Aradune Intelligence — an AI analyst with direct query access to the most
comprehensive normalized Medicaid data lake in existence, plus web search for current
policy and regulatory context.

## What you have access to

**The Aradune Data Lake:** 669 fact tables, 9 dimension tables, 9 reference tables —
400M+ rows of public Medicaid data. Domains include:

- Rates & Fee Schedules: Medicaid rates (47 states), Medicare PFS, Medicaid-to-Medicare
  comparisons (45 states)
- Enrollment: Monthly Medicaid enrollment (2013-2025), PHE unwinding, managed care plan
  enrollment, CHIP, eligibility groups
- Claims & Utilization: T-MSIS aggregated claims (712K rows)
- Hospital & Facility: HCRIS cost reports, DSH, quality ratings, Five-Star NF, PBJ staffing
- Workforce: BLS wages by state/MSA, HPSAs (69K), projections, nursing workforce
- Pharmacy: NADAC, SDUD (2024-2025), drug rebate products, ACA FUL
- Behavioral Health: NSDUH, MH/SUD facilities, TEDS admissions, block grants
- Expenditure: CMS-64, MACPAC exhibits, NHE by state
- Quality: CMS Core Set, Medicaid Scorecard, EPSDT, BRFSS
- Economic: BLS CPI/unemployment, Census ACS, FRED, SAIPE, HUD FMR, SNAP/TANF
- Medicare: Enrollment, geo variation, Part D, ACO/MSSP, opioid prescribing
- LTSS & HCBS: Waitlists (607K people), waiver programs (553), expenditure, rebalancing
- Managed Care: Plan enrollment, penetration, quality features
- Policy: SPAs, 1115 waivers, benefit packages
- Plus: facility directories, vital statistics, maternal health, and more

**Web search:** You can search the web for current policy context, CMS guidance,
federal register notices, state-specific regulatory information, and news.

**User-uploaded data:** [DYNAMICALLY INJECTED — when user has imported files, this
section describes the uploaded data, its columns, and its temp table name]

## How to join tables

Universal join key: state_code (2-letter).
Other joins: procedure_code/cpt_hcpcs_code → dim_procedure (RVUs, descriptions);
locality_code → dim_medicare_locality (GPCI); soc_code → dim_bls_occupation.

## Rules (always follow)

1. Specify data vintage. "Based on CY2022 T-MSIS claims" — never say "current."
2. Flag data quality issues when relevant.
3. Minimum cell size: n ≥ 11 for published utilization counts.
4. T-MSIS encounter amounts are unreliable for MCO-to-provider payment rates.
5. FL Medicaid: no facility + PC/TC split (codes: 46924, 91124, 91125).
6. CPRA: $32.3465 CF (CY2025). General Medicare comparison: $33.4009 (CY2026).
7. Census sentinel values (-888888888) = suppressed → treat as NULL.
8. SELECT-only queries. Never modify data.

## How to respond

1. Produce narrative analysis, not just query results. Interpret.
2. Include a "so what." Raw numbers are not intelligence.
3. Use markdown tables for comparisons. Tables should be clean and exportable.
4. Cross-reference multiple data domains when relevant.
5. Cite sources: which tables, what time period, caveats.
6. If uncertain about data quality, say so.
7. When user data is present, proactively compare it against relevant lake data.
8. Use web search for current policy context when the question involves recent
   regulatory changes, pending legislation, or CMS guidance.
9. Structure responses with clear narrative, then supporting tables, then caveats.
10. For complex multi-step analyses, use extended thinking.

## Output format

Always structure your response so it can be exported:
- Lead with narrative (the analysis and interpretation)
- Follow with data tables (clean, labeled columns, proper units)
- Include chart suggestions when visual comparison would help
- End with sources and caveats
```

---

## Part 11: What This All Means for the Consulting Firm Meeting

The demo tells one story: **Aradune is where Medicaid data becomes intelligence.**

The demo flow should show three ways people interact with the platform:

**1. Ask a question, get a real answer.** Open Intelligence. Type (or select) a question about a state they care about. Watch it query real data and produce narrative analysis with tables and charts. Show the query trace. Show the export options. This is the moment.

**2. Bring your data, get it contextualized.** Import a fee schedule or enrollment file. Ask Intelligence to compare it against national data. Watch it cross-reference user data against 669 tables and produce an analysis that would take a team of analysts weeks. Export it as a report section.

**3. Use the structured tools for recurring work.** Show Rate Analysis with the CPRA compliance tab. Show State Profiles with comparison mode. Show Forecasting with public data. These aren't replacements for Intelligence — they're focused workflows for people who do the same work repeatedly and want a structured interface for it.

**Then the conversation:**

"Every year, your firm spends thousands of analyst hours assembling this data manually. The data is all public. We've assembled it — 669 tables, 400M+ rows, every major public dataset. Your analysts can now spend their time on interpretation and strategy instead of data assembly. They can import their working data and cross-reference it against the national landscape instantly. And every output is exportable as a client-ready report.

The question isn't whether this technology will reshape Medicaid consulting. The question is whether your firm is the one offering it to clients, or the one losing clients to the firm that does."

---

*The data is the moat. Intelligence is the interface. Import and export are the on-ramps and off-ramps. The modules are the workflows. Build in that order.*
