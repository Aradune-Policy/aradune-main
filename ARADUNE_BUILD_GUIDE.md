# ARADUNE BUILD GUIDE
> The definitive build plan for Aradune's transformation from data lake to Medicaid operating system.
> Written 2026-03-10, updated 2026-03-11. Companion to CLAUDE.md.
> Hand this document to any Claude Code session alongside CLAUDE.md.
> For data ingestion work, also read COMPLETE-DATA-REFERENCE-FOR-ARADUNE.md (per-dataset quality, validation stack, adversarial testing).
> For entity registry work, also read ONTOLOGY_SPEC.md.

---

## Part 1: The Vision

### The problem

Medicaid is an $880 billion system run on Excel, SAS scripts, and fragmented legacy databases. State agencies use "hundreds of models and estimates" for forecasting (WA JLARC audit), staff list "advanced Excel" as their primary tool, and NAMD calls workforce challenges an "existential threat." Meanwhile federal requirements are escalating: CPRA rate transparency (July 2026), mandatory Core Set quality reporting (FFY 2024+), network adequacy standards, OBBBA work requirements (January 2027), SDP caps, provider tax restrictions, and six-month redetermination cycles.

No integrated system connects rate-setting to network adequacy to quality to fiscal forecasting to compliance artifacts.

### What we're building

An operating system for Medicaid intelligence. Not a dashboard. Not a chatbot with a database. An operational platform where:

- A rate-setter asks "What would it cost to bring behavioral health E/M rates to 80% of Medicare?" and gets a fiscal impact analysis with federal match calculations, UPL headroom check, and draft SPA language
- A compliance officer generates a submission-ready CPRA by uploading their fee schedule
- A consulting firm imports client data and cross-references it against 50-state benchmarks in minutes
- Every output is exportable as a client-ready report with full data lineage

### Design philosophy

**Bloomberg** became indispensable through workflow integration, not data. $12.5B revenue from mostly public financial data. IB messaging created network effects. API made it connective tissue of finance.

**Palantir Foundry** introduced the ontology: real-world entities mapped to digital objects with typed relationships. Critical insight: separate data layer (integration) from operational layer (applications, actions). "Action types" create closed-loop operations.

**ThoughtSpot** proved domain experts adopt AI when they ask questions in their language. Spotter reasons about drivers and generates visuals for the "why." Custom Actions connect insights to operational systems.

**Hex** bridges analyst-to-business-user. Context Studio grounds AI in organizational knowledge (database descriptions, business rules, analytical logic) — directly applicable to Aradune's policy corpus.

**The pattern:** Platforms become indispensable at Level 4-5 (prescriptive/operational), not Level 1-2 (descriptive/diagnostic). Only 9% of healthcare analytics addresses prescriptive. The gap is enormous.

---

## Part 2: The Product — What Users Experience

### Five personas, one platform

**The State Rate-Setter** comes with a fee schedule and a July deadline. Starts with CPRA tool (verifies data), then asks Intelligence for cross-state analysis.

**The Consulting Actuary** comes with client data and a 6-month rate development cycle. Imports data, cross-references against lake, generates report sections for certification.

**The Medicaid Director** needs an early warning system and ad hoc answers without waiting for IT. State Profiles + Intelligence.

**The MCO Analyst** needs network adequacy data, quality benchmarking, rate competitiveness. Provider Intelligence + Workforce tools.

**The Advocate/Researcher** needs accessible data. Free tier, Intelligence, export.

### The product loop

```
     STRUCTURED TOOLS                    INTELLIGENCE
     (trust-building on-ramps)           (the connective tissue)
     ┌─────────────────────┐             ┌─────────────────────────────┐
     │ State Profiles      │──"Ask ───→  │ Natural language query       │
     │ Rate Analysis       │  about      │ + 300+ table DuckDB access  │
     │ CPRA Compliance     │  this"──→   │ + RAG policy corpus         │
     │ Forecasting         │             │ + Web search for policy     │
     │ AHEAD Readiness     │             │ + User data cross-reference │
     │ Providers           │             │                             │
     │ Workforce & HCBS    │             │ Outputs:                    │
     │ Rate Lookup         │             │ Narrative · Tables · Charts │
     └─────────────────────┘             │ Compliance artifacts        │
              ↕                          │ Fiscal impact models        │
     ┌─────────────────────┐             │ Action recommendations      │
     │ DATA IMPORT         │──────────→  └──────────────┬──────────────┘
     │ (any page, any time)│                            │
     └─────────────────────┘             ┌──────────────┴──────────────┐
                                         │ EXPORT PIPELINE              │
                                         │ Save to Report · CSV/Excel  │
                                         │ Chart PNG · DOCX/PDF report │
                                         │ Compliance filings          │
                                         └─────────────────────────────┘
```

---

## Part 3: Architecture

### Navigation
```
ARADUNE  [⌕ Intelligence]  States  Rates  Forecast  Providers  Workforce  [↑ Import]
```

### Entity Registry (Ontology Layer)

The entity registry is a set of YAML files that define what every data entity is, how it connects to other entities, and what metrics can be computed from it. It sits between Intelligence and the raw DuckDB tables.

**Three outputs from the same YAML files:**
1. **Intelligence system prompt** — auto-generated data lake description organized by domain, with entity relationships and named metrics. Replaces hand-maintained table lists.
2. **DuckPGQ property graph** — `CREATE PROPERTY GRAPH medicaid` statement enabling SQL/PGQ graph pattern matching over the same underlying tables. No data migration needed.
3. **Validation rules** — CI checks that every table is assigned to an entity, every relationship references a real table and column, and every metric has valid source tables.

**Why this matters:** At 300+ tables, manually maintaining the Intelligence system prompt is error-prone. At 500+ (where we're heading), it's untenable. The registry makes data additions mechanical: add YAML, run script, Intelligence knows. It also ensures named metrics (pct_of_medicare, cpra_ratio) are calculated identically every time, regardless of how the question is phrased.

See `ONTOLOGY_SPEC.md` for the full build specification.

### Shared application state
```typescript
interface AraduneContext {
  selectedState: string | null;
  comparisonStates: string[];
  intelligencePanel: { open: boolean; context: IntelligenceContext | null };
  importedData: { files: ImportedFile[]; activeFile: string | null };
  reportSections: ReportSection[];
  demoMode: boolean;
}

interface ImportedFile {
  id: string; name: string; type: 'csv' | 'xlsx' | 'json';
  columns: string[]; rowCount: number;
  preview: Record<string, any>[];  // first 10 rows
  tableName: string;               // DuckDB temp table name
  uploadedAt: Date;
}

interface ReportSection {
  id: string; prompt: string; response: string;
  queries: string[]; tables: TableData[]; charts: ChartSpec[];
  createdAt: Date;
}
```

State persists across tool navigation. FL in States → FL pre-selected in Rates.

### Tool specifications

**State Profiles** (`/#/state/{code}` or `/#/state/FL+GA+TX`)
- Current: StateProfile.tsx (~1,000 lines, 20 parallel API fetches, 7 sections)
- Enhance with comparison mode (multi-state URL, side-by-side columns)
- Post-fetch Intelligence call for 3-5 proactive cross-dataset insights (cached)
- "Ask about [state]" → Intelligence sidebar with full state context
- Section-level export (CSV, chart, Add to Report)
- Import overlay: compare user data against state profile
- Data: dim_state, fact_enrollment, fact_rate_comparison, fact_hospital_cost, fact_quality_measure, fact_bls_wage, fact_drug_utilization, fact_acs_state, fact_hpsa, fact_hcbs_waitlist, fact_expenditure, fact_scorecard, fact_unwinding, fact_mc_enrollment_summary

**Rate Analysis & Fee Setting** (`/#/rates`)
- 4 tabs: Browse & Compare (wrap TmsisExplorer 2,400 lines), CPRA Compliance (wrap CpraGenerator 734 lines), Rate Builder (WIRE StateRateEngine 1,153 lines, 42/42 tests), Rate Lookup & Directory (wrap FeeScheduleDir 535 lines + RateLookup)
- Shared selected state across tabs. Tab state persists (switching doesn't lose work).
- Import: upload fee schedule → Browse & Compare shows alongside national data
- Old routes redirect: `/#/decay` → `/#/rates`, `/#/cpra` → `/#/cpra` (keep standalone too)
- API: all existing rate, CPRA, comparison endpoints

**CPRA Compliance** (`/#/cpra` — standalone route AND `/#/rates?tab=cpra`)
- Pre-computed cross-state (45 states, 302K rows) + user-upload compliance generator
- Keep as prominent standalone AND as Rates tab
- Target: one-button submission-ready CPRA compliance document
- Two CSVs in → full regulatory-correct CPRA out in <2 seconds
- Export: PDF (cpraPdf.ts), Excel (cpraXlsx.ts), DOCX report section

**Caseload & Fiscal Forecasting** (`/#/forecast`)
- Dual-mode: "Use public data" (auto-populate from fact_enrollment) + "Upload your data"
- 3 tabs: Caseload (SARIMAX+ETS), Expenditure (cap rate/cost-per-eligible), Scenario Builder (4 sliders)
- Target: 4th tab — Fiscal Impact Engine (rate change → FMAP match → UPL headroom → SDP cap → budget)
- Intelligence: "What drove this trend?" with enrollment + unwinding + economic context
- Historical accuracy overlay (Build Principle #15)
- Engines: caseload_forecast.py (~650 lines), expenditure_model.py (~430 lines)

**AHEAD Readiness** (`/#/ahead`)
- Merge AheadReadiness.tsx + AheadCalculator.tsx into single workflow
- Hospital search → AHEAD scoring → peer benchmarks → readiness report
- HCRIS fields: margins, payer mix, uncompensated care, bed count, CCR (see CLAUDE.md §13)
- Intelligence: "What should this hospital focus on?"
- 6 participating states: MD (Cohort 1, live), CT/HI/VT (Cohort 2, 2028), RI/NY (Cohort 3, 2028)

**Provider & Hospital Intelligence** (`/#/providers`)
- 3 tabs: Hospital Search/Detail, Nursing Facilities, Facility Directory
- Hospital: CCN lookup, HCRIS financials, quality ratings, DSH/VBP, peer benchmarking
- Nursing: Five-Star, PBJ staffing (65M+ rows), deficiencies, ownership, SNF cost
- Directory: FQHCs (8,121 sites), dialysis (7,557), hospice, HHA, IRF, LTCH
- Import: upload facility data for custom peer benchmarking

**Workforce & HCBS** (`/#/workforce`)
- 4 tabs: Wage Adequacy (wrap 546 lines), Quality Linkage (wrap 445 lines), HCBS Waitlists & Compensation (wrap 414 lines), Shortage Areas (new — HPSA + MUA map)
- HCBS pass-through tracking: 80% compensation requirement by July 2028
- Workforce Supply: HPSA (69K designations), projections (121 professions to 2038), nursing workforce
- Shortage Areas: HPSA + MUA (19,645 areas) map visualization
- Import: upload workforce/quality data for cross-referencing

**Rate Lookup & Directory** (`/#/lookup`)
- Code-level Medicaid rate lookup across 47 states
- State fee schedule directory with download links
- Quick trust-building tool: "look up a code you know, verify the data"
- Wraps: FeeScheduleDir.tsx + RateLookup.tsx

### Old tool → New location mapping

| Old Tool | New Home | Action |
|----------|----------|--------|
| Intelligence Chat | **Intelligence** (home) | Enhance |
| Policy Analyst | **Intelligence** | Replace |
| Data Explorer (NL2SQL) | **Intelligence** | Replace |
| State Profile | **States** | Enhance + comparison |
| TmsisExplorer / Spending Explorer | **Rates** → Browse & Compare | Wrap |
| Rate Decay / Medicare Comparison | **Rates** → Browse & Compare | Wrap |
| CPRA Generator | **CPRA** (standalone + Rates tab) | Wrap |
| Rate Builder | **Rates** → Rate Builder | Wire StateRateEngine |
| Fee Schedule Directory | **Rate Lookup** → Directory | Wrap |
| Rate Lookup | **Rate Lookup** | Wrap |
| Compliance Report | **CPRA** | Merge |
| Rate Reduction Analyzer | **Rates** → Browse & Compare | Integrate |
| AHEAD Calculator + Readiness | **AHEAD** | Merge |
| Caseload Forecaster | **Forecast** | Enhance |
| Wage Adequacy | **Workforce** → Wages | Wrap |
| Quality Linkage | **Workforce** → Quality | Wrap |
| HCBS Compensation Tracker | **Workforce** → HCBS | Wrap |
| Data Catalog | Keep standalone `/#/catalog` | Keep |

---

## Part 4: Intelligence System Prompt (Full Draft)

```
You are Aradune Intelligence — an AI analyst with direct query access to the most
comprehensive normalized Medicaid data lake in existence, plus web search for current
policy and regulatory context.

## What you have access to

**The Aradune Data Lake:** 300+ fact tables, 9 dimension tables, 9 reference tables —
115M+ rows of public Medicaid data. Domains:

- Rates & Fee Schedules: Medicaid rates (47 states, 597K rows), Medicare PFS (16,978
  codes, 858K locality rates), Medicaid-to-Medicare comparisons (45 states, 302K rows)
- Enrollment: Monthly Medicaid (2013-2025), CHIP, managed care, eligibility groups,
  unwinding, new adult expansion
- Claims & Utilization: T-MSIS aggregated (712K), SDUD pharmacy (2.64M rows, $108.8B)
- Hospital: HCRIS cost reports (6,103 hospitals), DSH, quality ratings, VBP, HRRP
- Nursing: Five-Star, PBJ staffing (65M+ rows), SNF cost, deficiency citations
- Workforce: BLS wages by state/MSA, HPSAs (69K), projections (121 professions to 2038)
- Pharmacy: NADAC, SDUD, drug rebate, ACA FUL, opioid prescribing
- Behavioral Health: NSDUH, TEDS (1.6M admissions), MH/SUD facilities, block grants
- LTSS/HCBS: Waitlists (607K people, 41 states), waivers (553), CMS-372, expenditure
- Expenditure: CMS-64 ($909B FY2024), MACPAC exhibits, FMAP
- Quality: Core Sets 2023-2024 (57 measures, 51 states), Scorecard, EPSDT
- Economic: BLS CPI/unemployment, Census ACS, FRED, SAIPE, HUD FMR, SNAP/TANF
- Medicare: Enrollment, geographic variation (2014-2023), ACOs (511, PY2026)
- Post-Acute: HHA, hospice, dialysis, IRF, LTCH
- Public Health: CDC PLACES (3,144 counties), overdose deaths, vital stats, BRFSS
- Maternal: SMM rates, pregnancy outcomes, NAS
- Policy: 1,039 CMS documents, 6,058 searchable chunks (CIBs, SHOs, SMDs)

**Web search:** Current CMS guidance, federal register notices, state regulatory info.

**User-uploaded data:** [DYNAMICALLY INJECTED when present]

## How to join tables

Universal: state_code (2-letter).
Codes: procedure_code/cpt_hcpcs_code → dim_procedure (RVUs, descriptions).
Locality: locality_code → dim_medicare_locality (GPCI).
Workforce: soc_code → dim_bls_occupation.
Provider: NPI for linkage.
Geography: FIPS codes for county-level data.

## Rules (always follow)

1. Specify data vintage. "Based on CY2022 T-MSIS claims" — never "current."
2. Flag data quality issues. Check DQ Atlas for any state used.
3. Minimum cell size: n >= 11 for utilization counts.
4. T-MSIS encounter amounts are unreliable for MCO-to-provider rates.
5. FL Medicaid: Facility and PC/TC rates are typically mutually exclusive (99.96% of codes). Three codes (46924, 91124, 91125) legitimately carry both as published by AHCA.
6. CPRA: $32.3465 CF (CY2025). General comparison: $33.4009 (CY2026).
7. Census sentinels (-888888888) = suppressed → NULL.
8. SELECT-only queries. Never modify data.
9. CHIP excluded from per-enrollee calculations.
10. No em-dashes. No "plain English."

## How to respond

1. Lead with narrative. Interpret. Include a "so what."
2. Cross-reference multiple domains. The power is in connections.
3. Clean markdown tables for comparisons.
4. Cite sources: tables, time period, caveats.
5. Flag uncertainty and quality issues explicitly.
6. When user data present, proactively compare against lake.
7. Web search for current policy context when relevant.
8. Reference specific CFR sections and deadlines for compliance.
9. Structure for export: narrative → tables → charts → sources → caveats.
10. When a finding implies action, state it: "This suggests [state] should consider [step]."
11. Use extended thinking for complex multi-step analyses.
```

---

## Part 5: The Build Plan

### Phase 0: Fix the Foundation
Before any new build work.

- [ ] Audit and fix 10+ endpoints returning 404
- [ ] Test top 20 most-used endpoints post-deploy
- [ ] Address Fly.io cold start: pre-bake lake into Docker image (~800MB)
- [ ] Delete `public/data/cpra_precomputed.json` and `scripts/build-cpra-data.mjs`
- [ ] Create CHANGELOG.md from session history in old CLAUDE.md

### Phase 0.5: Entity Registry (Ontology)

**This phase produces the scaffolding that makes every subsequent phase better.** Hand `ONTOLOGY_SPEC.md` to a Claude Code session alongside the codebase. It will:

- [ ] Run `scripts/introspect_lake.py` — connect to DuckDB, DESCRIBE every registered table, output `ontology/raw_inventory.json`
- [ ] Infer entity types from dimension tables and column patterns (state_code → State, cpt_hcpcs_code → Procedure, NPI → Provider, etc.)
- [ ] Generate ~16 entity YAML files in `ontology/entities/` with properties, relationships, and fact table references
- [ ] Generate ~13 domain YAML files in `ontology/domains/` with table assignments and intelligence context
- [ ] Define 10+ named metrics in `ontology/metrics/` with deterministic formulas (pct_of_medicare, cpra_ratio, per_enrollee_spending, etc.)
- [ ] Build `scripts/generate_ontology.py` — reads YAML, outputs system prompt section + DuckPGQ SQL
- [ ] Build `scripts/validate_ontology.py` — CI validation (broken references, orphan tables, schema conformance)
- [ ] Build `server/ontology/registry.py` — Python module loaded at startup, provides entity/metric lookup
- [ ] Build `server/ontology/prompt_generator.py` — generates Intelligence system prompt section from registry
- [ ] Generate `sql/property_graph.sql` — CREATE PROPERTY GRAPH statement for DuckPGQ
- [ ] Wire `intelligence.py` to use auto-generated prompt section
- [ ] Add `validate_ontology.py` to CI pipeline

**Decision points (Claude Code will ask you):**
- Orphan tables with no detectable entity join
- Overlapping tables (e.g., 10+ managed care enrollment variants)
- Ambiguous column names
- Cross-domain table assignments
- Metric calculation method choices (simple avg vs weighted avg)
- DuckPGQ edge limitations for composite-key tables

**After this phase:** Every subsequent data addition follows the pattern: ingest → add YAML → validate → generate → Intelligence knows about it automatically.

### Phase 0.5: Entity Registry + Query Router

See `ONTOLOGY_AND_ROUTING_SPEC.md` for the full execution spec. This builds the scaffolding that every subsequent phase depends on.

- [ ] Inventory all lake tables (DESCRIBE + COUNT via DuckDB)
- [ ] Generate entity YAML files from dimension tables + implicit entities
- [ ] Generate domain YAML files grouping fact tables
- [ ] Generate metrics YAML files with canonical definitions
- [ ] Build `scripts/generate_ontology.py` (system prompt + DuckPGQ + validation)
- [ ] Build `server/engines/query_router.py` (Tier 1-4 classification + resource allocation)
- [ ] Integrate router with Intelligence endpoint
- [ ] Install DuckPGQ, generate and test property graph
- [ ] Update Intelligence system prompt to use auto-generated data section
- [ ] Test: 20+ queries across all tiers, verify routing and accuracy

### Phase 1: Intelligence + Platform Restructure

**Intelligence backend:**
- [ ] Comprehensive system prompt (Part 4 of this doc — include lake summary, all join keys, 10 quality rules, output format instructions, behavioral rules)
- [ ] SSE streaming (FastAPI StreamingResponse). Event types: `status` (thinking/searching), `tool_call` (name + purpose for UI status), `tool_result` (name + rows + ms), `token` (text fragment), `metadata` (tables/charts/queries/citations JSON), `done` (empty)
- [ ] Web search: add `{"type": "web_search_20250305", "name": "web_search", "max_uses": 3}` to tools array in Anthropic API call. No custom implementation needed. Requires SDK >=0.49.0.
- [ ] Context injection: accept optional `context` parameter with `{module, state, section, context_summary}`. When present, prepend to system prompt: "The user is viewing {module} for {state}, section: {section}. Context: {context_summary}."
- [ ] Structured output: after narrative streaming completes, emit single `event: metadata` with JSON containing tables, charts, queries, citations arrays
- [ ] User data awareness: when `session_id` provided and imported data exists, augment system prompt with file metadata and add temp table to query_database scope
- [ ] Conversation memory: frontend sends full messages array. Follow-ups work naturally.
- [ ] Test 15+ queries: rate comparison across states, fiscal impact, quality trends, workforce shortages, pharmacy spending, HCBS waitlists, cross-domain (rates + workforce + quality), policy questions hitting RAG, web search for current CMS guidance, user data cross-reference
- [ ] Deprecate api/chat.js — keep running but redirect PolicyAnalyst to /api/intelligence. Migrate FL methodology addendum into Intelligence system prompt. Remove api/chat.js after verification.

**Intelligence frontend:**
- [ ] IntelligenceChat.tsx — full-page chat, markdown, inline tables/charts, streaming
- [ ] StarterPrompts.tsx — 6-8 prompts by persona, disappear after first query
- [ ] InputBar.tsx — fixed bottom, auto-expanding, file drop zone
- [ ] QueryTrace.tsx — collapsible SQL trace per response
- [ ] ResponseExport.tsx — Save to Report, Export Table, Export Chart
- [ ] IntelligencePanel.tsx — right sidebar for use inside tools

**Platform restructure:**
- [ ] Rewrite Platform.tsx: new nav (Intelligence, States, Rates, Forecast, Providers, Workforce, Import)
- [ ] Intelligence as home page (`/#/`)
- [ ] Lazy-load all tool modules
- [ ] Old route redirects (`/#/analyst` → `/#/`, `/#/decay` → `/#/rates`, etc.)
- [ ] AraduneContext provider
- [ ] "Ask Intelligence" button on all tool shells

### Phase 2: Data Import + Export Pipeline

**Data import:**
- [ ] `POST /api/import` (CSV/XLSX/JSON → DuckDB temp table)
- [ ] Validation: column types, row counts, sanity checks
- [ ] ImportPanel.tsx — drag-and-drop, preview, column editor, confirmation
- [ ] Session-scoped storage (never persisted, never shared)
- [ ] Augment Intelligence prompt when data present
- [ ] Add user temp tables to query_database scope
- [ ] Test: upload fee schedule → Intelligence cross-reference → analysis

**Export pipeline:**
- [ ] ReportBuilder.tsx — persistent panel (not a module). Accumulated sections list with drag-to-reorder, inline annotation, delete.
- [ ] "Save to Report" button on every Intelligence response → creates ReportSection in AraduneContext with: prompt, response markdown, tables, chart specs, queries executed
- [ ] "Add to Report" on tool views → captures current view as report section (snapshot of visible data + charts)
- [ ] Shared CSV export utility: accepts any `{columns, rows}` data → downloads formatted CSV
- [ ] Shared Excel export utility: formatted headers, auto-width columns, branded color scheme
- [ ] Chart export: Recharts `toDataURL()` on canvas → PNG/SVG download
- [ ] DOCX generation via docx-js (same pattern proven in CPRA PDF export):
  - Cover page: "Medicaid Intelligence Report — [Topic] — Aradune — [Date]"
  - Table of contents
  - Each section: the prompt or context, the analysis narrative, tables, chart image placeholders, data citations
  - Footer: "Generated by Aradune. Sources: [list of tables queried]"
  - Branded: #2E6B4A headers, SF Mono for data, logo-wordmark.png in header
- [ ] PDF generation: HTML-to-PDF path or from DOCX (evaluate both approaches)
- [ ] Compliance artifact generation: CPRA outputs as standalone formatted documents (already proven in cpra_upload.py → extend pattern)

### Phase 3: Tool Consolidation

Wrap existing components. Don't rewrite big components — wrap them in tab containers.

**Rates module:**
- [ ] RateAnalysis.tsx — 4-tab wrapper with persistent tab state (switching tabs doesn't lose work)
- [ ] Browse & Compare: wrap TmsisExplorer.tsx (2,400 lines) in tab container. Don't rewrite internals. Add shared state connection (selectedState propagates from AraduneContext).
- [ ] CPRA Compliance: wrap CpraGenerator.tsx (734 lines). Keep standalone `/#/cpra` route AND `/#/rates?tab=cpra`. Both render same component.
- [ ] Rate Builder: WIRE StateRateEngine.js (1,153 lines, 42/42 tests passing). Build the UI connection to the Engine Analysis card. FL Tier 3 engine, multi-state CF comparison, implied CF reverse-engineering.
- [ ] Rate Lookup & Directory: wrap FeeScheduleDir.tsx (535 lines) + RateLookup into combined tab
- [ ] Shared selectedState across tabs. "Ask Intelligence" with rate context per tab.
- [ ] Import: "Upload your fee schedule" in Browse & Compare cross-references against national data
- [ ] Export: per-tab CSV/Excel/Chart, per-tab "Add to Report"
- [ ] Route redirects: `/#/decay` → `/#/rates`, `/#/compliance` → `/#/cpra`, `/#/reduction` → `/#/rates`

**State Profiles:**
- [ ] Comparison mode: multi-state URL (`/#/state/FL+GA+TX`). Internally, state is always an array.
- [ ] Side-by-side columns. Comparison summary table at top.
- [ ] Cross-dataset insights: after all 20 fetches, call `/api/intelligence` with assembled data. Ask for 3-5 proactive observations. Cache per state combo. Example: "FL has below-average rates (62% of Medicare) AND above-average HCBS waitlists (72,000) — suggesting workforce supply constraints from low reimbursement."
- [ ] "Ask about [state]" → opens Intelligence sidebar with full state context
- [ ] Graceful 404 degradation: if a fetch fails, show "Data unavailable" for that section, don't break page
- [ ] Section-level export (CSV, chart PNG, Add to Report)
- [ ] Import overlay: if user data exists, show comparison option

**Forecast:**
- [ ] Dual-mode entry: "Use public data" (select state, auto-populate from fact_enrollment) + "Upload your data" (existing CSV path). Both feed same SARIMAX+ETS engine.
- [ ] Intelligence: "What drove FL's enrollment spike in Q3 2024?" → queries enrollment + unwinding + economic data with forecast context
- [ ] Historical accuracy overlay: previous forecasts overlaid with actuals (Build Principle #15)
- [ ] Export: forecast chart PNG, data CSV/Excel, narrative as report section
- [ ] Target: Fiscal Impact tab — rate increase % → FMAP match → UPL headroom → SDP cap → budget impact

**AHEAD:**
- [ ] Merge AheadReadiness + AheadCalculator into single flow
- [ ] Hospital search → AHEAD scoring → peer benchmarks → readiness report
- [ ] HCRIS fields: margins (Worksheet G), payer mix (S-3), uncompensated care (S-10), DSH/IME (E), CCR (D-1/D-4)
- [ ] Intelligence: "What should this hospital focus on?"
- [ ] 6 states: MD (live), CT/HI/VT (2028), RI/NY (2028). 2 more from July 2026.

**Providers:**
- [ ] 3-tab wrapper (Hospitals, Nursing Facilities, Directory)
- [ ] Hospital: CCN lookup, HCRIS financials, quality ratings, DSH/VBP, peer benchmarking
- [ ] Nursing: Five-Star, PBJ staffing (65M+), deficiency citations, ownership, SNF cost/VBP
- [ ] Directory: FQHCs (8,121), dialysis (7,557), hospice, HHA, IRF, LTCH
- [ ] Import: upload facility data for custom peer benchmarking
- [ ] Intelligence + export per tab

**Workforce:**
- [ ] 4-tab wrapper (Wage Adequacy, Quality Linkage, HCBS, Shortage Areas)
- [ ] Wage Adequacy (wrap 546 lines): BLS wages × state × occupation vs Medicaid rates
- [ ] Quality Linkage (wrap 445 lines): rate competitiveness → quality correlations
- [ ] HCBS (wrap 414 lines): waitlists (607K, 41 states), compensation tracking toward 80% (July 2028)
- [ ] Shortage Areas (NEW): HPSA (69K) + MUA (19,645) map. Color-coded severity. Layer with enrollment.
- [ ] Import: upload workforce/quality data for cross-referencing
- [ ] Intelligence + export per tab

### Phase 4: Demo Preparation

**Demo mode:**
- [ ] `?demo=true` URL parameter activates demo mode in AraduneContext
- [ ] Pre-cache key API responses in `public/demo/` (State Profile for 3-5 states, rate comparisons, enrollment trends)
- [ ] Pre-cache 5-10 Intelligence responses for starter prompts (store as JSON, render as if streaming)
- [ ] Subtle "DEMO MODE" indicator (small badge, not intrusive)
- [ ] Ensure demo works without live Fly.io (all cached responses)

**Demo resilience:**
- [ ] Test with Fly.io cold start — confirm fallback to cached responses
- [ ] Test every tool transition (Intelligence → States → Rates → CPRA → Forecast → Providers → Workforce)
- [ ] Test import → Intelligence cross-reference → export flow end-to-end
- [ ] Test Intelligence sidebar from inside each tool
- [ ] Test Report Builder: save 3+ sections → reorder → export DOCX

**Demo script (write this out with timing):**
- [ ] Act 1 (~3 min): Intelligence home. Type question. Watch streaming + query trace + tables. Export.
- [ ] Act 2 (~3 min): Import fee schedule CSV. Ask Intelligence to compare. Cross-reference output.
- [ ] Act 3 (~3 min): CPRA compliance (2 CSVs → regulatory-correct output in <2s). State Profile comparison. Caseload forecast.
- [ ] Prepare 3-5 backup queries per act in case primary fails
- [ ] Dry-run end-to-end with timing — target 12-15 minutes total

**Visual polish:**
- [ ] Consistent loading states: Lottie sword loader for Intelligence, skeleton screens for tools
- [ ] Error states that don't look like crashes (friendly messages, retry buttons)
- [ ] Mobile/laptop responsive: minimum = don't break on presenter's screen size
- [ ] Verify brand consistency: #0A2540 ink, #2E6B4A brand, SF Mono for numbers

### Phase 5: Data Quality Infrastructure + Expansion Sprint

**First: build the validation infrastructure** (see `COMPLETE-DATA-REFERENCE-FOR-ARADUNE.md` for full architecture)

- [ ] Install and configure **Soda Core v4** with DuckDB connector. Write initial SodaCL checks for top 20 tables (row counts, nulls, ranges, freshness).
- [ ] Set up **dbt-duckdb** adapter + **dbt-expectations** package. Port existing Dagster validation logic to dbt tests.
- [ ] Implement **Pandera** schemas for Python pipeline DataFrames (rate parsing, fee schedule ingestion).
- [ ] Set up **datacontract-cli** with ODCS v3.1.0 contracts for the 5 most critical tables. Add `datacontract test` to CI.
- [ ] Build 4-layer adversarial test suite: `tests/unit/` (Hypothesis), `tests/integration/` (dbt), `tests/chaos/` (schema drift, null injection, encoding), `tests/adversarial/` (invalid codes, outliers).
- [ ] Implement **medallion architecture**: restructure `data/lake/` into `bronze/` (raw) → `silver/` (normalized) → `gold/` (analytics-ready). Add metadata columns to bronze.
- [ ] Implement **SCD Type 2** for all reference tables (ICD-10, CPT, NDC, GPCIs, CFs, FMAP) with `effective_date` + `termination_date`.
- [ ] Build **Illinois-specific dedup pipeline** as first-class concern (incremental credit/debit logic, not void/replace).
- [ ] Implement **NDC normalization chain**: raw → 11-digit 5-4-2 → RxNorm CUI → therapeutic classification (ATC/USP).
- [ ] Add **DQ Atlas quality metadata** as a carried field through all T-MSIS-derived tables.
- [ ] Implement **user upload quarantine pattern**: schema profiling → column mapping → validation → clean load + quarantine with rejection reasons.

**Then: ingest priority datasets** (each addition follows ontology-first pattern: YAML entity/domain → validate → generate → ingest)

- [ ] **Medicare PFS RVU files** (annual + quarterly) — CPRA denominator. Full RVU decomposition + locality GPCIs.
- [ ] **MCO MLR reports** (data.Medicaid.gov) — first public MCO profitability by state × plan × period.
- [ ] **AHRQ SDOH Database** (44 sources, county/tract/ZIP) — pre-integrated SDOH enrichment.
- [ ] **CDC SVI** (census tract) + **USDA RUCA** (tract/ZIP) — social vulnerability + rural classification.
- [ ] **Area Deprivation Index** (block group) — CMS equity adjustments.
- [ ] **OIG LEIE exclusion list** (monthly CSV, ~70K records) — program integrity.
- [ ] **CMS Open Payments** (16M records, $13.18B) — conflict detection.
- [ ] **CLFS + DMEPOS + Ambulance** — complete Medicare benchmark stack.
- [ ] **Federal Register API** — full Medicaid rulemaking text since 1994.
- [ ] **HRSA AHRF** (6,000 variables/county) — county-level health resource analysis.
- [ ] **HHS DOGE Provider Spending** (when re-released) — 227M rows. **CAUTION:** OT only, no beneficiary state, suppresses <12 claims, MC states show misleading paid amounts. See COMPLETE-DATA-REFERENCE for full handling rules.
- [ ] **PHI Direct Care Worker data** — DSW wages, demographics, poverty rates.

### Phase 6: Post-Demo / Future

- [ ] User accounts (Clerk) + Stripe (Track B)
- [ ] Early warning heat map (50-state, 6 categories, color-coded)
- [ ] Forecast accuracy dashboard
- [ ] Network adequacy engine (directory vs claims vs availability reconciliation)
- [ ] Compliance countdown dashboard (CPRA, MCPAR, Core Set, work requirements)
- [ ] Ghost network detection (auto-flag directory-listed providers with zero claims)
- [ ] Shared analytical workspaces (state + actuary + CMS reviewer)
- [ ] Hospital price transparency MRF ingestion
- [ ] Remaining fee schedules (KS, NJ, TN, WI)
- [ ] 340B covered entity data (HRSA OPAIS)
- [ ] SOC 2 Type II certification
- [ ] State procurement pathway (APD, SMC)

---

## Part 6: Technical Decisions

**Stay with current stack.** Vite + React 18 + TypeScript. FastAPI + DuckDB on Fly.io. Hash routing. Vercel hosting. No Next.js migration.

**Streaming via SSE.** FastAPI StreamingResponse. Frontend reads `fetch()` + ReadableStream. Simpler than WebSockets, works through Vercel proxy.

**Component wrapping, not rewriting.** TmsisExplorer (2,400 lines) → one tab in Rates. CpraGenerator (734 lines) → CPRA tab + standalone. New code = wrappers + Intelligence + import + export.

**Web search in Intelligence.** Anthropic built-in: `{"type": "web_search_20250305", "name": "web_search", "max_uses": 3}`. No custom implementation. SDK >=0.49.0.

**Data import via DuckDB temp tables.** Parse server-side → session-scoped temp table → Intelligence queries alongside lake. No persistence, no sharing. 50MB/session, 500MB total, LRU. User uploads go through quarantine pattern: schema profiling → column mapping → validation → load clean rows + route invalid rows to `_quarantine` with rejection reasons.

**Medallion architecture for the lake.** Bronze (raw Parquet, append-only, metadata columns `_source_file`/`_ingestion_timestamp`/`_batch_id`, never modify). Silver (normalized: ICD→CCSR, NDC→RxNorm, NPI enriched, IL-specific dedup, void/replacement logic, temporal alignment with FFY/SFY/CY columns). Gold (pre-computed PMPM, utilization metrics, inflation-adjusted via Medical Care CPI). Use `union_by_name=true` for schema-tolerant Parquet reads. Target Iceberg or DuckLake for managed schema evolution. Full detail in `COMPLETE-DATA-REFERENCE-FOR-ARADUNE.md`.

**Validation stack: Soda Core + dbt-duckdb + Pandera + datacontract-cli.** Soda Core v4 has the best native DuckDB support (50+ SodaCL checks). dbt-duckdb adapter (maintained under DuckDB org) with dbt-expectations package ports 60+ Great Expectations tests. Pandera for DataFrame validation with statistical hypothesis testing (distribution shifts). datacontract-cli for CI/CD contract testing and `datacontract diff` for breaking change detection. See `COMPLETE-DATA-REFERENCE-FOR-ARADUNE.md` Part 2 for full evaluation.

**Adversarial testing: Hypothesis + SDV + chaos engineering.** Four-layer test suite: `tests/unit/` (Hypothesis property tests), `tests/integration/` (schema contracts, referential integrity), `tests/chaos/` (schema drift, null injection at graduated rates, encoding chaos, volume spikes), `tests/adversarial/` (invalid codes, outlier values, boundary conditions). Use SDV for realistic synthetic data, Mimesis for volume testing (12-15x faster than Faker), CMS SynPUF for ETL validation. Key properties: row count preservation, schema stability, idempotency, referential integrity, null propagation.

**Reference data as SCD Type 2.** All reference tables (ICD-10, CPT, NDC, RxNorm, NUCC Taxonomy, FMAP, GPCIs, conversion factors) stored with `effective_date` + `termination_date` for point-in-time historical joins.

**DuckDB extensions required:** `httpfs`, `cache_httpfs` (60%+ S3 cost reduction), `iceberg`, `icu` (Unicode), `json`, `excel`, `spatial`, `duckpgq`.

### Data import session management (detail)

**Session ID lifecycle:**
- UUID `session_id` generated at upload by `POST /api/import`, returned to frontend
- Frontend stores in AraduneContext, passes with every `/api/intelligence` and module API call
- Backend checks if temp table exists in DuckDB; if not, looks up session store, re-creates from bytes
- Adds ~200ms latency on cache miss but guarantees correctness across Fly.io restarts

**Memory budget:**
- 50MB per session (file bytes in memory)
- 500MB total across all active sessions
- LRU eviction when total exceeded
- Clear error to frontend if single upload exceeds per-session cap
- 2-hour TTL per session

**Future alternative:** Redis (serialized bytes with TTL) or Fly.io persistent volumes for cross-restart persistence without memory pressure.

### Intelligence structured output format

When Intelligence produces a response, it structures for export:

```json
{
  "narrative": "Florida's E&M rates sit significantly below...",
  "tables": [{
    "title": "E&M Rate Comparison: FL vs Southeast vs Medicare",
    "columns": ["Code", "Description", "FL Rate", "SE Avg", "Medicare", "FL % MCR"],
    "rows": [["99213", "Office visit, est.", "$34.29", "$52.18", "$91.39", "37.5%"]]
  }],
  "charts": [{
    "type": "bar",
    "title": "FL Medicaid Rates as % of Medicare",
    "data": [{"code": "99213", "pct": 37.5}]
  }],
  "queries": ["SELECT ... FROM fact_rate_comparison WHERE ..."],
  "citations": ["fact_rate_comparison (CY2022 T-MSIS)", "fact_bls_wage (BLS OEWS 2024)"],
  "web_sources": []
}
```

Frontend renders narrative in real-time as it streams, then renders tables and charts when metadata event arrives. Each piece is independently exportable.

### Responsive behavior

On screens < 1024px: collapse module links into hamburger menu.
On screens >= 1024px: show all nav items + Import button inline.
Intelligence search bar doubles as nav search — no separate NavSearch component.

**Target AWS GovCloud or Azure Government** from day one. Inherits 46+ FedRAMP controls, reducing SOC 2 / GovRAMP burden.

---

## Part 7: The Demo Script

Three acts:

**Act 1: Ask a question, get a real answer.**
Open Intelligence. Type a question about a state they care about. Watch it query real data, cross-reference datasets, produce narrative with tables, charts, citations. Show query trace. Show export. *This is the moment.*

**Act 2: Bring your data.**
Import a fee schedule. Ask Intelligence to compare against national data. Watch cross-reference against 300+ tables. Export as report section. *This would take a team weeks.*

**Act 3: Structured tools for recurring work.**
CPRA compliance: upload two CSVs, regulatory-correct analysis in <2 seconds. State Profiles in comparison mode. Caseload forecasting with public data.

**Then:** "Your analysts spend thousands of hours assembling data that already exists in one place. Aradune turns it into compliance-ready documents, fiscal impact models, and early warning signals — through a conversation, not a spreadsheet. The question is whether your firm offers this to clients, or loses clients to the firm that does."

---

## Part 8: Regulatory Context Driving Urgency

| Deadline | Requirement | Aradune capability |
|----------|------------|-------------------|
| **July 1, 2026** | CPRA rate transparency (§447.203) | CPRA tool — ready now |
| **July 1, 2026** | Publish all FFS rates on public website | Rate Lookup + Directory |
| **July 1, 2026** | HCBS hourly rate disclosure | Workforce & HCBS tool |
| **January 1, 2027** | OBBBA work requirements | Enrollment forecasting + fiscal impact |
| **~July 2027** | Appointment wait times (90% compliance) | Network adequacy (future) |
| **~July 2028** | 80% HCBS compensation pass-through | HCBS tracking |
| **FY 2030** | 3% eligibility error rate penalty | Program integrity (future) |

### OBBBA fiscal impact
- Work requirements: -5.3M enrollees by 2034
- SDP caps: 100% Medicare (expansion) / 110% (non-expansion), phase-down Jan 2028
- Provider tax: safe harbor 6% → 4% for expansion states
- FMAP floor elimination: $467.7B impact to 10 states + DC
- **Total federal savings: $911B (14%) over a decade**

States need tools to model these impacts. That's Aradune.

### Competitive landscape

No standalone Medicaid analytics SaaS exists. The market is served by:
- **Consulting-embedded analytics:** Milliman MedInsight (2.5B+ records, all-payer), Guidehouse GuideIQ, Accenture AIP4Health. None Medicaid-specific.
- **MMIS-embedded analytics:** Gainwell Genius, Conduent CMdS, Acentra evoBrix X. Bundled in multi-year enterprise contracts, not standalone.
- **CMS's own tools:** MACBIS dashboards, T-MSIS DQ Atlas. Not analytics platforms.
- **No CPRA-specific compliance tools** found in public searches.
- **TAF Research Files:** ~$88K/year + $35K/seat + 6-8 month application. Commercial data (Merative, IQVIA, Optum): $100K-$1M+/year for incomplete Medicaid coverage.

Aradune's 300+ table assembly would cost ~$500K-$1M and 12-24 months to replicate.

### Federal funding pathway

States can procure Aradune at **10-25 cents on the dollar** through CMS's Decision Support System & Data Warehouse (DSSDW) module designation. FFP rates: 90% for design/development, 75% for operations. Requires state IAPD submission → CMS approval → Streamlined Modular Certification (7 artifacts, down from 29). Multiple states already have active DSS/DW modules: AR (Optum), CO (BIDM), FL (Data Warehouse), NC (active RFI for replacement).

---

## Part 9: Future Architecture

These build on the entity registry foundation (Phase 0.5) but are not Phase 1-4 deliverables.

### Medicaid Ontology (evolution of entity registry)
The YAML entity registry is the foundation. The evolution path: Layer 1 (YAML + auto-generated prompt) → Layer 2 (DuckPGQ graph queries for relationship traversal) → Layer 3 (full semantic layer with deterministic metric compilation). Each layer is additive. Layer 1 is built in Phase 0.5. Layer 2 is enabled by installing DuckPGQ and running the generated SQL. Layer 3 follows the dbt MetricFlow pattern if/when DuckDB gets a MetricFlow adapter or we build a lightweight equivalent.

### Early Warning System
50-state dashboard modeled on OFR Financial Vulnerabilities Monitor. 6 categories × 5-10 indicators each:

**Enrollment health:** Application volume trends, renewal/redetermination outcomes, procedural vs substantive denial rates, CHIP enrollment, expansion enrollment velocity, unwinding trajectory.

**Spending/fiscal health:** Per-enrollee spending trends, CMS-64 quarterly growth vs budget, MCO MLR trajectories, supplemental payment growth, provider tax revenue sensitivity.

**Access/network:** Medicaid-to-Medicare rate ratios by specialty, provider enrollment/disenrollment velocity, HPSA designations trending, MCO network adequacy compliance rates, appointment availability (when data available).

**Quality/outcomes:** Core Set measure trajectories, HEDIS trends, ED utilization for non-emergent conditions, avoidable hospitalizations, maternal/infant mortality.

**Workforce/capacity:** BLS wage growth vs Medicaid rate growth, HCBS waitlist trends, nursing facility staffing levels, provider age distribution, HPSA score changes.

**Program integrity:** PERM improper payment rates, excluded provider matches, spending outlier trends, billing pattern anomalies.

Color-coded green→yellow→orange→red based on peer-group-adjusted trajectories. Alert tiers: routine → elevated → critical. Updated as new data arrives.

### Compliance Engine
Map every table to specific regulatory requirements. Auto-generate CPRAs, MCPARs, NAAAR reports, Core Set submissions. Countdown dashboard per state per deadline.

### Network Adequacy Intelligence
Reconcile: directory (MCO reports) vs claims (who actually billed) vs availability (appointment scheduling). Ghost network auto-detection: directory-listed + zero Medicaid claims = flagged. Geo-access with road network drive-time, not straight-line distance.

### Fiscal Impact Simulator
Rate increase % → federal match at FMAP → UPL headroom → SDP cap under OBBBA → MCO capitation impact → budget cycle projection. All connected.

### Shared Workspaces
State + actuary + CMS reviewer on same data, same queries, same compliance status. Role-based access. Audit trail. Network effects.

---

## Part 10: For Claude Code Sessions

### Starting a session
1. Read CLAUDE.md (target architecture)
2. Read this BUILD_GUIDE (phased plan)
3. Read ONTOLOGY_SPEC.md if the session involves data ingestion or Intelligence changes
4. Read COMPLETE-DATA-REFERENCE-FOR-ARADUNE.md if the session involves data ingestion, validation, or new datasets
5. Check CHANGELOG.md for recent history
6. Identify which phase the work falls into
7. Build toward CLAUDE.md

### File mapping
| Current file | Target location | Action |
|-------------|----------------|--------|
| **(new)** ontology/entities/*.yaml | Entity registry | Generate via ONTOLOGY_SPEC.md |
| **(new)** ontology/domains/*.yaml | Domain registry | Generate via ONTOLOGY_SPEC.md |
| **(new)** ontology/metrics/*.yaml | Named metrics | Generate via ONTOLOGY_SPEC.md |
| **(new)** server/ontology/registry.py | Registry module | Generate via ONTOLOGY_SPEC.md |
| **(new)** server/ontology/prompt_generator.py | Prompt generation | Generate via ONTOLOGY_SPEC.md |
| IntelligenceChat.tsx | Intelligence home | Enhance |
| TmsisExplorer.tsx | Rates → Browse & Compare | Wrap |
| CpraGenerator.tsx | CPRA Compliance | Wrap (keep standalone) |
| CaseloadForecaster.tsx | Forecast | Enhance |
| StateProfile.tsx | States | Enhance + comparison |
| AheadReadiness + Calculator | AHEAD | Merge |
| WageAdequacy.tsx | Workforce → Wages | Wrap |
| QualityLinkage.tsx | Workforce → Quality | Wrap |
| HcbsCompTracker.tsx | Workforce → HCBS | Wrap |
| FeeScheduleDir.tsx | Rate Lookup → Directory | Wrap |
| RateLookup.tsx | Rate Lookup | Wrap |
| PolicyAnalyst.tsx | DEPRECATED | Replace with Intelligence |
| DataExplorer.tsx | DEPRECATED | Replace with Intelligence |
| StateRateEngine.js | Rates → Rate Builder | Wire (finally) |
| intelligence.py | Intelligence backend | Enhance |
| cpra_upload.py | CPRA backend | Keep |
| caseload_forecast.py | Forecast backend | Keep |
| expenditure_model.py | Forecast backend | Keep |
| rag_engine.py | Intelligence RAG | Keep |

### The rule
Every session should advance the current phase. If it can't complete a phase item, don't create work that conflicts with CLAUDE.md target architecture. Build toward the target. Ship fast.

### Critical operational note: Florida ethics
The founder's AHCA employment creates exposure under Florida Code of Ethics §112.313. §112.313(8) prohibits using non-public information gained through official position for personal gain. §112.313(7)(a) prohibits employment with entities doing business with AHCA — meaning Aradune cannot sell to Florida AHCA while founder is employed there. **Action required:** Request formal advisory opinion from Florida Commission on Ethics (free), proactively disclose to AHCA, document all data sources as publicly available, maintain strict firewall between AHCA work and Aradune.

---

*The data is the moat. Intelligence is the interface. Compliance is the wedge. Structured tools are the on-ramps. Build in that order.*
