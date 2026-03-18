# CLAUDE.md — Aradune
> **The operating system for Medicaid intelligence.**
> Read this file at the start of every session. It defines what Aradune is, how it's built, and the rules for building it.
> Build plan: See ARADUNE_BUILD_GUIDE.md for the phased build plan, module specs, and data import architecture.
> Last updated: 2026-03-18 (Session 34) · Live: https://www.aradune.co
> Research audit: RESEARCH_AUDIT_GUIDE.md (v1) + RESEARCH_AUDIT_GUIDE_v2.md (verification-first). Advanced methods: scripts/research_advanced_methods.py
> Adversarial testing: docs/ADVERSARIAL_TESTING_IMPL.md (7-agent suite). All 7 agents built. Run: `python -m scripts.adversarial.runner`
> Complete reference: ARADUNE-COMPLETE-REFERENCE.md — data catalog, module inventory, audit test catalog (hand to another Claude session for autonomous auditing)

---

## 1. What Aradune Is

Aradune is a **Medicaid intelligence operating system**. It ingests, normalizes, and cross-references every available public Medicaid dataset into a unified data layer, then uses Claude-powered analytics to turn that data into intelligence: compliance-ready documents, fiscal impact models, early warning signals, and actionable recommendations.

Aradune is not a dashboard with AI bolted on. It is **the platform where Medicaid data becomes decisions** — for state agencies, consulting firms, MCOs, hospitals, providers, researchers, journalists, advocates, and legislators.

**Core identity:**
- **The data layer is the moat.** 750+ tables, 400M+ rows, 4.9 GB. Curated, normalized, cross-referenced public Medicaid data no one else has assembled.
- **Intelligence is the interface.** Claude is the primary interaction model for complex work. Natural language in, compliance-ready analysis out.
- **Structured tools are on-ramps.** Fifteen purpose-built modules build trust, demonstrate data quality, and pull users into Intelligence naturally.
- **Compliance automation is the adoption wedge.** Auto-generate CPRAs, rate transparency filings, MCPARs, Core Set submissions. The July 2026 CPRA deadline is less than 4 months away.
- **Bring your own data.** Users upload files and cross-reference them against the national data layer within their session.
- **Closed-loop operations.** Every analysis connects to an action: a rate gap finding generates a SPA template, a network gap produces a corrective action plan, a spending anomaly creates a program integrity referral.

Named after Brad McQuaid's EverQuest paladin character. Domain: aradune.co. Florida LLC filed. Federal trademark pending (Class 42, §1(b)).

---

## 2. System Architecture

Aradune has three layers. Intelligence connects everything.

```
┌─────────────────────────────────────────────────────────────────────────┐
│                       ARADUNE INTELLIGENCE                              │
│                                                                         │
│  Claude Sonnet/Opus + extended thinking + DuckDB query access           │
│  + RAG over policy corpus + web search for current regulatory context   │
│  + user-uploaded data cross-reference + structured output format        │
│                                                                         │
│  Available everywhere: home page chat, sidebar from any structured      │
│  tool, "Ask about this" buttons, State Profile questions.               │
│  Produces: narrative, tables, charts, exportable compliance documents.  │
│                                                                         │
│  15 CORE MODULES + 13 RESEARCH BRIEFS (on-ramps to Intelligence):       │
│  State Profiles · Rate Analysis · CPRA Compliance · Forecasting         │
│  AHEAD Readiness · Providers · Workforce & HCBS · Rate Lookup           │
│  Every tool has: Intelligence sidebar, export, data import              │
└────────────────────────────┬────────────────────────────────────────────┘
                             │
┌────────────────────────────┴────────────────────────────────────────────┐
│                      ENTITY REGISTRY (Ontology)                         │
│                                                                         │
│  YAML-defined entities: State, Procedure, Provider, Hospital, MCO,     │
│  Rate Cell, Drug, Quality Measure, Policy Document, Geographic Area    │
│  Properties · Relationships · Named metrics · Domain groupings         │
│  Auto-generates: Intelligence system prompt + DuckPGQ property graph   │
│  Add a dataset = add a YAML file + run a script                        │
└────────────────────────────┬────────────────────────────────────────────┘
                             │
┌────────────────────────────┴────────────────────────────────────────────┐
│                        THE DATA LAKE                                    │
│                                                                         │
│  750+ tables · 400M+ rows · Hive-partitioned Parquet · DuckDB          │
│  Medallion architecture: Bronze (raw) → Silver (normalized) → Gold     │
│  + DuckPGQ property graph (SQL/PGQ queries over same tables)           │
│  + User session data (uploaded files, parsed and queryable)            │
│  + Policy corpus (1,039+ CMS docs, 6,058+ searchable chunks)          │
│  R2 sync · Dagster orchestration · Source-provenant · Versioned        │
│  Validated: 15-check engine (row count, range, referential integrity) + validator API  │
│  See COMPLETE-DATA-REFERENCE-FOR-ARADUNE.md for per-dataset quality    │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## 3. Current Stack

```
Frontend:       React 18 + TypeScript + Vite (Vercel Pro, aradune.co)
Visualization:  Recharts
Routing:        Hash-based in Platform.tsx
Data store:     DuckDB-WASM (browser-side client queries)
Data lake:      Hive-partitioned Parquet (data/lake/) — 400M+ rows, 750+ views
                DuckDB in-memory views over Parquet files, 4.9 GB on disk
                S3/R2 sync (scripts/sync_lake_wrangler.py --remote, Cloudflare R2 bucket: aradune-datalake)
Backend:        Python FastAPI (server/) — ~345 endpoints across 40+ route files, DuckDB-backed
AI:             Intelligence (server/routes/intelligence.py) — Claude Sonnet 4.6 + SSE streaming
                + extended thinking + DuckDB tools + RAG policy corpus + web search
                Haiku for routing · Sonnet for analysis · Opus for complex reasoning
Skillbook:      Skillbook (self-improving): server/engines/skillbook.py + reflector.py, CRUSP lifecycle, score decay, graph expansion
Adversarial:    Adversarial testing: 7-agent suite (scripts/adversarial/)
Production:     DuckDB memory_limit=900MB, 2 threads, object cache, disk spill
Dynamics:       System dynamics engine (server/engines/system_dynamics.py) — stock-flow ODE modeling
                scipy.integrate.solve_ivp, 12 stocks, 6 feedback loops, lake-calibrated parameters
                Policy Simulator module (/#/policy-simulator) + 4 embedded widgets in modules
                Security headers (HSTS, nosniff, DENY, referrer, permissions)
                Gunicorn 2 workers + preload + max-requests recycling
                Rate limiting: 15 Intelligence queries/min/user
                Health probes: /healthz (liveness), /ready (readiness), /startup
                JSON structured logging + request timing middleware
                Dependabot (npm, pip, GitHub Actions) + supply chain auditing in CI
                Schemathesis API contract testing (auto-tests 340+ endpoints)
RAG:            DuckDB FTS over policy corpus (1,039 docs, 6,058 chunks from medicaid.gov)
                BM25 full-text search with ILIKE fallback (server/engines/rag_engine.py)
Search:         Platform-wide Cmd+K search (PlatformSearch.tsx + /api/search)
Auth:           Clerk integration (ClerkProvider.tsx + server/middleware/auth.py)
                Falls back to password gate ("mediquiad") when Clerk not configured
Pipeline:       Python build scripts (scripts/build_*.py) — 115+ ETL scripts
                Python (cpra_engine.py) — CPRA/DuckDB analytical layer
                R (tmsis_pipeline_duckdb.R) — T-MSIS processing
Orchestration:  Dagster (pipeline/dagster_pipeline.py) — 13 assets, 3 checks, 3 jobs, 2 schedules
CI/CD:          GitHub Actions (.github/workflows/ci.yml) — TypeScript check + Vercel + Fly.io deploy
Deployment:     Vercel (frontend) · Fly.io (FastAPI, server/fly.toml + Dockerfile)
                Pre-baked lake in Docker image for fast cold starts
Design:         #0A2540 ink · #2E6B4A brand · #C4590A accent · #F5F7F5 surface
                SF Mono for numbers · Helvetica Neue for body · No Google Fonts
Context:        Universal state context (server/routes/state_context.py) — 12-query endpoint, 1hr cache
                StateContextBar component (compact + expanded) deployed across all 12 modules
Access:         Clerk auth (when configured) OR password gate ("mediquiad")
```

---

## 4. Navigation & Structured Tools

```
ARADUNE  [⌕ Intelligence]  States  Rates  Forecast  Providers  Workforce  [↑ Import]
```

**Intelligence** is the home page (`/#/`). Full-page Claude-powered chat. Every question starts here. Structured tools are accessible from the nav and linked from Intelligence responses.

### Structured Tools (14 on-ramps)

Purpose-built workflows for recurring work. Each has "Ask Intelligence" (opens sidebar with full context), export (CSV, Excel, PDF, DOCX), and accepts imported user data.

| Tool | Route | What it does | Key data |
|------|-------|-------------|----------|
| **State Profiles** | `/#/state/{code}` | 7-section dashboard. Comparison mode: `/#/state/FL+GA+TX`. 20 parallel API fetches. Cross-dataset insights. | dim_state, enrollment, rates, hospitals, quality, workforce, pharmacy, economic |
| **Rate Analysis & Fee Setting** | `/#/rates` | 4 tabs: Browse & Compare, CPRA Compliance, Rate Builder (StateRateEngine), Rate Lookup & Directory | fact_medicaid_rate (597K), fact_rate_comparison (302K), dim_procedure (16,978) |
| **CPRA Compliance** | `/#/cpra` | Pre-computed cross-state (45 states) + user-upload generator. Dual-mode. PDF/Excel. Regulatory-correct (68 codes, $32.3465 CF, many-to-many). | fact_rate_comparison, CPRA reference data |
| **Caseload & Fiscal Forecasting** | `/#/forecast` | SARIMAX+ETS forecasting, expenditure modeling, scenario builder. Dual-mode: public data or upload. | fact_enrollment, fact_expenditure, FMAP, economic |
| **Spending Efficiency** | `/#/spending` | 3 tabs: Per-Enrollee Spending (MACPAC), Total Expenditure (CMS-64 FY2018-2024), Efficiency Metrics (scatter: spending vs MC penetration). | fact_cms64_multiyear (118K), fact_macpac_spending_per_enrollee |
| **AHEAD Readiness** | `/#/ahead` | Hospital readiness scoring + calculator. HCRIS financials, payer mix, peer benchmarks. | fact_hospital_cost, fact_dsh_hospital, fact_hospital_rating |
| **Provider & Hospital Intelligence** | `/#/providers` | 3 tabs: Hospitals (search + CCN detail + peers), Nursing Facilities (Five-Star, PBJ, deficiencies), Directory (FQHCs, dialysis, hospice, HHA, IRF, LTCH). | HCRIS, Five-Star, PBJ (65M+), SNF cost, facility dirs |
| **Hospital Rate Setting** | `/#/hospital-rates` | 3 tabs: Hospital Financials (HCRIS cost reports), DSH & Supplemental (MACPAC Exhibit 24), State Directed Payments (34 states). | fact_hospital_cost (18K), fact_dsh_hospital (6K), fact_macpac_supplemental, fact_sdp_preprint |
| **Nursing Facility** | `/#/nursing` | 3 tabs: Quality Ratings (Five-Star summary), Staffing (PBJ nurse staffing), State Detail (facility-level). | fact_five_star (14.7K), fact_pbj_nurse_staffing (1.3M) |
| **Behavioral Health & SUD** | `/#/behavioral-health` | 4 tabs: Prevalence (NSDUH 26 measures), Treatment Network (facilities/beds, IPF quality, block grants), Opioid Crisis (prescribing rates), Conditions & Services. | fact_nsduh_prevalence (5.9K), fact_mh_facility (28K), fact_opioid_prescribing (539K), fact_bh_by_condition (4.2K) |
| **Pharmacy Intelligence** | `/#/pharmacy` | 3 tabs: Spending Overview (SDUD 2025 state summary), Top Drugs (by spending, filterable by state), NADAC Pricing (drug name search). | fact_sdud_2025 (2.6M), fact_nadac (1.9M) |
| **Program Integrity** | `/#/integrity` | 3 tabs: Exclusions (LEIE 82K), Open Payments ($13B), MFCU & PERM (error rates 2020-2025). | fact_leie (83K), fact_open_payments (36K), fact_mfcu_stats, fact_perm_rates |
| **Workforce & HCBS** | `/#/workforce` | 4 tabs: Wage Adequacy, Quality Linkage, HCBS Waitlists & Compensation (80% pass-through tracking), Shortage Areas (HPSA + MUA map). | fact_bls_wage, fact_hpsa (69K), fact_hcbs_waitlist (607K), quality |
| **Policy Simulator** | `/#/policy-simulator` | System dynamics: model downstream effects of rate changes, wage increases, HCBS funding, economic shocks through interconnected feedback loops. 5 presets. Baseline vs scenario comparison. | fact_enrollment, fact_rate_comparison_v2, fact_bls_wage, fact_hcbs_waitlist, fact_hpsa |
| **Rate Lookup & Directory** | `/#/lookup` | Code-level Medicaid rate lookup across 47 states. State fee schedule directory with download links. Quick trust-building tool. | fact_medicaid_rate, fee schedule files |

**Data Catalog** remains standalone at `/#/catalog` for power users browsing table schemas.

### How tools connect to Intelligence

Every tool view has **"Ask Intelligence about this"** → opens sidebar with current state, section, and data pre-loaded. Tools build trust (user verifies data they know); Intelligence delivers the insight they couldn't get from a spreadsheet.

### Responsive behavior

On screens < 768px (`BP.mobile`): hamburger menu, reduced container padding (12px), single-column grids, all tables horizontally scrollable.
On screens >= 768px: full horizontal nav with dropdowns, standard 20px padding, multi-column grids.
Shared `useIsMobile()` hook exported from `design.ts` — used by Platform, StateProfile, CaseloadForecaster, CpraGenerator, AheadReadiness. AheadCalculator has its own `wW`-based breakpoint at 900px.
All `<table>` elements wrapped with `overflowX: "auto"` containers. Recharts charts use `ResponsiveContainer`.

### Cross-Dataset Context

Every module shows cross-dataset context when a state is selected. The `StateContextBar` component fetches from `/api/state-context/{state_code}` and displays: FMAP, enrollment (total + MC%), HPSA counts, quality measures below median, rate adequacy (median % Medicare), CMS-64 expenditure, workforce wages (CNA/HHA/RN), HCBS waitlist, LTSS rebalancing, T-MSIS claims-based rates, and supplemental payments (DSH + SDP). Compact mode shows a single-row summary; expanded mode shows a grid with named sections.

Intelligence integration: every "Ask Aradune" button passes the full cross-dataset context summary, enriching Intelligence queries automatically.

### Key tool API endpoints

**State Profiles:** `/api/states`, `/api/enrollment/{state}`, `/api/rates/{state}`, `/api/hospitals/{state}`, `/api/quality/{state}`, `/api/wages/{state}`, `/api/pharmacy/{state}`, `/api/economic/{state}`, `/api/insights/{state}`

**CPRA:** `/api/cpra/states`, `/api/cpra/rates/{state}`, `/api/cpra/dq/{state}`, `/api/cpra/compare`, `/api/cpra/upload/generate`, `/api/cpra/upload/generate/csv`, `/api/cpra/upload/generate/report`

**Forecast:** `/api/forecast/templates/caseload`, `/api/forecast/templates/events`, `/api/forecast/generate`, `/api/forecast/generate/csv`, `/api/forecast/enrollment/public`, `/api/forecast/expenditure/*`

**Spending:** `/api/spending/by-state` (CMS-64 multiyear), `/api/spending/per-enrollee` (MACPAC)

**Hospital Rates:** `/api/hospitals/summary`, `/api/supplemental/dsh/summary`, `/api/supplemental/summary`, `/api/supplemental/sdp`

**Nursing:** `/api/five-star/summary`, `/api/five-star/{state}`, `/api/staffing/summary`, `/api/staffing/{state}`

**Behavioral Health:** `/api/behavioral-health/nsduh/measures`, `/api/behavioral-health/nsduh/ranking`, `/api/behavioral-health/facilities/summary`, `/api/behavioral-health/ipf-facility/summary`, `/api/behavioral-health/block-grants`, `/api/behavioral-health/conditions/summary`, `/api/behavioral-health/services/summary`, `/api/opioid/prescribing/summary`

**Pharmacy:** `/api/pharmacy/sdud-2025/state-summary`, `/api/pharmacy/sdud-2025/top-drugs`, `/api/pharmacy/nadac`

**Integrity:** `/api/integrity/leie-summary`, `/api/integrity/open-payments-summary`, `/api/integrity/mfcu`, `/api/integrity/perm`

**Intelligence:** `/api/intelligence` (POST, SSE streaming)

**NL2SQL:** `/api/nl2sql` (POST, returns SQL + results)

---

## 5. Intelligence Architecture

Intelligence is powered by the **Entity Registry** — a YAML-defined ontology that auto-generates the system prompt and DuckPGQ graph definition. See ONTOLOGY_SPEC.md for the full build specification.

```
User query (natural language, from chat or "Ask about this")
    │
    ├── Intelligence receives: query + conversation history
    │   + context (if from tool: module, state, section, data summary)
    │   + user data metadata (if imported files exist)
    │
    ├── System prompt includes (auto-generated from ontology/):
    │   ├── Entity types with properties and relationships
    │   ├── Domain groupings with table descriptions and row counts
    │   ├── Named metrics with deterministic formulas
    │   └── Domain-specific intelligence context and caveats
    │
    ├── Tools available to Intelligence:
    │   ├── query_database    → SELECT-only DuckDB over all lake + user temp tables
    │   ├── list_tables       → Browse tables by domain (reads from entity registry)
    │   ├── describe_table    → Schema, row counts, sample data
    │   ├── web_search        → Current policy/regulatory context (Anthropic built-in)
    │   └── search_policy     → RAG over 1,039+ CMS docs (BM25 + FTS)
    │
    ├── Intelligence executes multi-step analysis
    │   (for relationship-heavy queries, can use DuckPGQ graph pattern matching)
    │
    └── Output (streamed via SSE):
        ├── narrative     → analysis + interpretation (token-by-token)
        ├── tables        → clean, labeled, exportable (JSON metadata event)
        ├── charts        → specs for frontend rendering
        ├── queries       → SQL trace (collapsible, auditable)
        ├── citations     → sources with vintage + caveats
        └── web_sources   → policy/regulatory URLs
```

### SSE event sequence
```
event: status\ndata: {"status": "thinking"}\n\n
event: tool_call\ndata: {"name": "query_database", "purpose": "Looking up FL rates"}\n\n
event: tool_result\ndata: {"name": "query_database", "rows": 45, "ms": 23}\n\n
event: token\ndata: {"text": "Florida's"}\n\n
...
event: metadata\ndata: {"tables": [...], "charts": [...], "queries": [...], "citations": [...]}\n\n
event: done\ndata: {}\n\n
```

### Programmatic Enforcement (Session 34)

Intelligence enforces data quality and safety rules at **code level**, not just via prompt:
- **DOGE quarantine:** Injected programmatically into system prompt by `intelligence.py` (code-level, not just prompt text). OT-only, provider state, MC distortion, Nov/Dec 2024 incomplete.
- **IL T-MSIS caveats:** Injected programmatically when IL is detected in query context.
- **Territory-aware fallback:** Guam, PR, VI get appropriate caveats when territories lack data coverage.
- **DuckDB 30s `statement_timeout`** prevents runaway queries. Anthropic API has 120s timeout.
- **`_postprocess_response`:** Em-dash removal (U+2014 → " - ") applied to all Intelligence output.
- **`fact_intelligence_trace`:** Every Intelligence interaction logged for audit trail (query, response hash, model, tokens, cost, trace_id).
- **`trace_id` in SSE metadata events:** Every streamed response includes a trace_id for end-to-end audit correlation.

### Entity Registry and Ontology

Intelligence's knowledge of the data lake is auto-generated from YAML entity definitions in `data/ontology/`. See `ONTOLOGY_AND_ROUTING_SPEC.md` for the full spec. The system prompt data section is generated by `scripts/generate_ontology.py --system-prompt` and includes entity types, properties, relationships, join paths, and named metrics. When a new dataset is added, add a YAML file and regenerate. DuckPGQ property graph (SQL/PGQ, SQL:2023 standard) provides graph pattern matching over entity relationships for cross-domain questions.

### Intelligent Query Router

Questions are classified into 4 tiers by a Haiku classifier (~100ms, ~$0.001). The system always errs up (borderline medium → high). See `server/engines/query_router.py`.

| Tier | Type | Model | Thinking | Queries | Tools | Target |
|------|------|-------|----------|---------|-------|--------|
| 1 | Lookup | Sonnet | No | 1-2 | DuckDB | <1s |
| 2 | Comparison | Sonnet | No | 1-4 | DuckDB | 1-3s |
| 3 | Analysis | Sonnet | 5K budget | 3-8 | DuckDB + RAG | 5-15s |
| 4 | Synthesis | Sonnet/Opus | 10K budget | 5-15 | DuckDB + RAG + Web | 15-45s |

Bump-up rules: user data present → minimum Tier 3. Compliance terms detected → Tier 4.

### Model costs

| Query type | Model | Cost/query |
|---|---|---|
| Classification, routing, PDF extraction | claude-haiku-4-5-20251001 | ~$0.004 |
| NL2SQL, RAG, standard analysis | claude-sonnet-4-6 | ~$0.03-0.06 |
| SPA drafting, CPRA narrative, AHEAD, complex reasoning | claude-opus-4-6 | ~$0.28 |

Use **prompt caching** (90% input cost reduction) and **batch API** (50% discount for non-real-time).

### Intelligence System Prompt

The system prompt has two parts: an **auto-generated section** (produced by `scripts/generate_ontology.py` from the entity registry YAML files) and a **hand-written section** (rules, response format, behavioral instructions). The auto-generated section replaces any manual table/join documentation and stays in sync with the actual data lake automatically.

**Auto-generated section** includes: entity types with properties and relationships, domain groupings with table descriptions and row counts, named metrics with formulas and caveats, domain-specific intelligence context. Regenerated by running `python scripts/generate_ontology.py`.

**Hand-written section** (keep synced with server/routes/intelligence.py):

```
You are Aradune Intelligence — an AI analyst with direct query access to the most
comprehensive normalized Medicaid data lake in existence, plus web search for current
policy and regulatory context.

## What you have access to

**The Aradune Data Lake:** 750+ tables (fact + dimension + reference) —
400M+ rows of public Medicaid data across 20 domains:

- Rates & Fee Schedules: Medicaid rates (47 states, 597K rows), Medicare PFS (16,978
  codes, 858K locality rates), Medicaid-to-Medicare comparisons (45 states, 302K rows)
- Enrollment: Monthly Medicaid (2013-2025), CHIP, managed care, eligibility groups,
  unwinding, new adult expansion, Medicare monthly (557K)
- Claims & Utilization: T-MSIS aggregated (712K), SDUD pharmacy (2020-2025, 28.3M rows, $1.05T pre-rebate),
  DOGE provider spending (190M rows, 5 aggregated tables)
- Hospital: HCRIS cost reports (6,103 hospitals), DSH, quality, VBP, HRRP, HCAHPS,
  Care Compare quality, ownership
- Nursing: Five-Star, PBJ staffing (65M+), SNF cost, deficiency citations, MDS (29.2M)
- Workforce: BLS wages by state/MSA, HPSAs (69K), projections (121 professions to 2038),
  HRSA awards, BH workforce projections, NHSC, NSSRN
- Pharmacy: NADAC, SDUD (2020-2025, 28.3M rows, $1.05T pre-rebate), drug rebate, ACA FUL, opioid prescribing, Part B/D drug spending
- Behavioral Health: NSDUH, TEDS (1.6M+ admissions), MH/SUD facilities, block grants,
  SAMHSA TEDS detail, NSDUH 2024 SAE
- LTSS/HCBS: Waitlists (607K people, 41 states), waivers (553), CMS-372, expenditure
- Expenditure: CMS-64 (FY2018-2024, 118K rows, $5.7T total computable), MACPAC exhibits, FMAP, NHE/NHE projections
- Quality: Core Sets 2023-2024 (57 measures, 51 states), Scorecard, EPSDT, HAC measures
- Economic: BLS CPI/unemployment, Census ACS/state finances, FRED, SAIPE, HUD FMR/SAFMR,
  SNAP/TANF, BEA income/GDP, Tax Foundation rankings, county health rankings
- Medicare: Enrollment, geographic variation (2014-2023), ACOs (511, PY2026), chronic
  conditions, MCBS, Part D prescriber, outpatient by provider, program stats
- Post-Acute: HHA, hospice, dialysis, IRF, LTCH, PAC casemix
- Public Health: CDC PLACES (3,144 counties), overdose deaths, vital stats, BRFSS,
  natality, immunization, food environment
- Maternal & Child: SMM rates, pregnancy outcomes, NAS, WIC, foster care, Title V
- State Fiscal: Census state finances, Tax Foundation rankings, FMAP historical, NHE
- Program Integrity: LEIE, Open Payments, MFCU stats, PERM rates, Federal Register CMS
- Insurance Market: MLR, risk adjustment, MA stars
- Provider Network: NPPES (9.37M), PECOS, affiliations, CAHs, GME
- Policy: 1,039 CMS documents, 6,058 searchable chunks (CIBs, SHOs, SMDs)

**Web search:** Current CMS guidance, federal register notices, state regulatory info.
**User-uploaded data:** [DYNAMICALLY INJECTED when present]

## How to join tables

Universal: state_code (2-letter).
Codes: procedure_code/cpt_hcpcs_code -> dim_procedure (RVUs, descriptions).
Locality: locality_code -> dim_medicare_locality (GPCI).
Workforce: soc_code -> dim_bls_occupation.
Provider: NPI for linkage. Geography: FIPS for county-level.

## Rules

1. Specify data vintage. "Based on CY2022 T-MSIS claims" — never "current."
2. Flag data quality issues. Check DQ Atlas for any state.
3. Minimum cell size: n >= 11 for utilization counts.
4. T-MSIS encounter amounts unreliable for MCO-to-provider rates.
5. FL Medicaid: Facility and PC/TC rates are typically mutually exclusive (99.96% of codes). Three codes (46924, 91124, 91125) legitimately carry both facility and PC/TC rates as published by AHCA.
6. CPRA: $32.3465 CF (CY2025). General: $33.4009 (CY2026).
7. Census sentinels (-888888888) = NULL.
8. SELECT-only. Never modify.
9. CHIP excluded from per-enrollee.
10. No em-dashes. No "plain English."

## How to respond

1. Lead with narrative. Interpret. Include a "so what."
2. Cross-reference multiple domains. Power is in connections.
3. Clean markdown tables. Cite sources with vintage and caveats.
4. When user data present, proactively compare against lake.
5. Web search for current policy when relevant.
6. Structure for export: narrative -> tables -> charts -> sources -> caveats.
7. When finding implies action, state it.
8. Extended thinking for complex multi-step analyses.
```

### Starter prompt examples (by persona)

**Rate analysis:**
- "Compare Florida's E&M rates to the Southeast average and Medicare"
- "Which states pay primary care above 80% of Medicare?"
- "What would it cost Florida to raise all rates below 50% of Medicare to 60%?"

**State intelligence:**
- "Give me a comprehensive profile of Ohio's Medicaid program"
- "Which states have the longest HCBS waitlists relative to spending?"
- "How did the PHE unwinding affect expansion vs non-expansion states?"

**Hospital/provider:**
- "Compare teaching hospitals in Florida by margin and Medicaid payer mix"
- "Which states have the highest nursing facility staffing deficiencies?"

**Fiscal:**
- "What's driving enrollment growth in the Southeast?"
- "How does Florida's per-enrollee spending compare to similar-FMAP states?"

---

## 6. Data Import Architecture

Data import enhances every workflow. Not a separate tool.

### Import flow
```
User drags file or clicks [↑ Import]
         │
         ▼
┌──────────────────────────────────────────────────┐
│  IMPORT PANEL                                     │
│  Drop a file here or click to browse              │
│  CSV, Excel, JSON — up to 50MB                    │
│  Or start without data — 750+ tables ready         │
└──────────────────────────────────────────────────┘
         │ (after upload)
         ▼
┌──────────────────────────────────────────────────┐
│  VALIDATION & PREVIEW                             │
│  File: florida_fee_schedule_2026.csv              │
│  Rows: 6,676  Columns: 8                         │
│  Detected: procedure_code, rate, modifier, ...    │
│  Preview: first 10 rows in table                  │
│  [✓ Make available] [✎ Edit columns] [✕ Cancel]   │
└──────────────────────────────────────────────────┘
         │
         ▼
Data loaded as DuckDB temp table: user_upload_1
Available to Intelligence and all tools for session.
```

### Backend implementation
**Endpoint:** `POST /api/import` — multipart file upload (CSV, XLSX, JSON) → parse, validate, load as session-scoped DuckDB temp table (`user_upload_{id}`) → return table name, column schema, row count, preview rows.

**Intelligence integration:** When user data imported, `query_database` can query `user_upload_{id}` alongside all lake tables. System prompt augmented: "The user has uploaded '{filename}' with {N} rows and columns: {columns}. Available as table '{tableName}'."

**Module integration:** Rate Analysis: "Use my fee schedule as comparison baseline." Forecasting: "Use my enrollment data instead of public." CPRA: already has this pattern — generalize.

### Session management
- UUID `session_id` generated at upload, returned to frontend
- Frontend stores in AraduneContext, passes with Intelligence and module API calls
- Backend checks if temp table exists; if not, re-hydrates from in-memory session store
- 50MB/session, 500MB total, LRU eviction, 2-hour TTL
- Future: Redis or Fly.io persistent volumes if session volume demands it

### Without imported data
Everything works the same, minus cross-reference. Intelligence queries 560+ lake tables. Tools use standard sources. Import is enhancement, not prerequisite.

### User Upload Validation (from COMPLETE-DATA-REFERENCE)
User-uploaded files go through: schema profiling (DuckDB `SUMMARIZE`), column mapping (fuzzy match to canonical schema), validation (code format regex, date range plausibility, referential integrity), and a **quarantine pattern** routing invalid records to a `_quarantine` temp table with rejection reason codes. Users see: "{N} of {M} rows loaded. {K} rows quarantined — [View issues]."

---

## 7. Export Pipeline

Every Intelligence response and tool view is exportable.

| Export type | What it produces | Available from |
|-------------|------------------|----------------|
| Chat response | Inline markdown with tables and narrative | Intelligence (default) |
| Report section | Formatted block saved to Report Builder | "Save to Report" button |
| Table (CSV/Excel) | Raw data download | Any table anywhere |
| Figure (PNG/SVG) | Chart download (Recharts `toDataURL()`) | Any chart |
| Full report (DOCX/PDF) | Accumulated sections, cover page, citations, branding | Report Builder panel |
| Compliance artifact | Submission-ready CPRA, rate transparency filing | CPRA tool + Intelligence |

### Report Builder
Not a separate module — a persistent panel accessible from the nav.

**How it works:**
1. User interacts with Intelligence or tools
2. Any Intelligence response has "Save to Report" button
3. Any tool view has "Add to Report" (captures current view as section)
4. Saved sections appear in Report Builder panel
5. User can reorder, annotate, delete sections
6. Export as DOCX or PDF with:
   - Cover page: "Medicaid Intelligence Report — [Topic] — Aradune — [Date]"
   - Each section: prompt/context, analysis, tables, chart placeholders, data citations
   - Footer: "Generated by Aradune. Data sources: [list of tables queried]"

**Implementation (built):**
- Report state in AraduneContext (session-scoped). Each section stores: prompt, response (markdown), queries, createdAt.
- `ReportBuilder.tsx` — slide-out panel, section list with preview, 4 export buttons (DOCX/PDF/Excel/CSV), clear.
- `reportMarkdown.ts` — shared markdown parser, converts Intelligence responses to typed blocks (heading, paragraph, table, list, code, hr).
- `reportDocx.ts` — branded DOCX via `docx` library. Cover page with brand bar, numbered sections, shaded prompt boxes, tables with branded headers.
- `reportPdf.ts` — branded PDF via `jspdf` + `jspdf-autotable`. Page breaks, autoTable for markdown tables.
- `reportXlsx.ts` — multi-sheet Excel via `xlsx`. Overview + per-table data sheets + queries + notes.
- `chartExport.ts` — SVG serialization + Canvas PNG (2x retina). `ChartActions.tsx` component adds overlay PNG/SVG buttons.
- All export libraries lazy-loaded (reportDocx 356KB, jspdf 358KB, xlsx 429KB — only loaded on export click).

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

interface IntelligenceContext {
  module: string;           // "state_profile", "rates", "forecast", etc.
  state: string | null;
  section: string | null;   // "rates", "enrollment", "quality", etc.
  contextSummary: string;   // "User is viewing FL rate data. Avg E&M is 62% of Medicare."
}

interface ImportedFile {
  id: string; name: string; type: 'csv' | 'xlsx' | 'json';
  columns: string[]; rowCount: number;
  preview: Record<string, any>[];
  tableName: string;
  uploadedAt: Date;
}

interface ReportSection {
  id: string; prompt: string; response: string;
  queries: string[]; tables: TableData[]; charts: ChartSpec[];
  createdAt: Date;
}
```

---

## 8. CPRA — Two Separate Systems

### 8a. CPRA Frontend (Pre-Computed Cross-State Comparison)
`src/tools/CpraGenerator.tsx` (734 lines). Pre-computed from `fact_rate_comparison` (302K rows, 45 states).
**Data flow:** lake → `cpra_em.json` (2,742 rows/34 states) → frontend.
**Export:** PDF · Excel · HTML.
**API:** `GET /api/cpra/states`, `/api/cpra/rates/{state_code}`, `/api/cpra/dq/{state_code}`, `/api/cpra/compare`

### 8b. CPRA Upload Tool (42 CFR 447.203 Compliance Generator)
`server/engines/cpra_upload.py` (821 lines). Upload two CSVs → full CPRA in <2 seconds. **Regulatory-correct.**

| Aspect | Pre-Computed (8a) | Upload Tool (8b) |
|--------|-------------------|-------------------|
| E/M codes | 74 (old list) | **68** (CMS CY 2025) |
| Code-category mapping | 1:1 | **Many-to-many** (171 pairs) |
| Conversion factor | $33.4009 (CY2026) | **$32.3465** (CY2025) |
| Medicare rates | State-level averages | **Per-locality** |

**Reference data** (`data/reference/cpra/`): `em_codes.csv` (68), `code_categories.csv` (171), `GPCI2025.csv` (109 localities).
**API:** `GET /api/cpra/upload/states`, `/upload/codes`, `/upload/templates/*`, `POST /upload/generate`, `/upload/generate/csv`, `/upload/generate/report`

### 8c. Terminal B — CPRA Data Pipeline
**File:** `tools/mfs_scraper/cpra_engine.py` (~968 lines)
```bash
python cpra_engine.py --all --cpra-em --output-dir ../../public/data/
```
**Steps:** `--init` → `--em-codes` → `--medicare-rates` (858K) → `--cpra` (242K) → `--dq` (258K flags) → `--export` → `--cpra-em` → `--stats`
**DuckDB (`aradune_cpra.duckdb`):** 1.87M rows across 8 tables.
**Summary:** Median 84.8% of Medicare. PC E/M avg 81.4%, MH/SUD 99.6%, OB/GYN 132.9%.

### CPRA Compliance Rules (always enforce)
- **68 codes** from CMS CY 2025 E/M Code List
- **$32.3465** CY 2025 CF (non-QPP) for CPRA; **$33.4009** CY 2026 for general comparison
- **Many-to-many categories**; base rates only (no supplementals); non-facility Medicare benchmark
- Small cell suppression: beneficiary counts 1-10 suppressed
- Published by July 1, 2026; updated biennially

---

## 9. Known Policy Rules (Always Enforce)

- **FL Medicaid: Facility and PC/TC rates are typically mutually exclusive (99.96% of codes).** Three codes (**46924, 91124, 91125**) legitimately carry both facility and PC/TC rates as published by AHCA.
- **FL conversion factors:** Regular `$24.9779582769` · Lab `$26.1689186096`. Ad hoc CF $24.9876 is stale.
- **FL has 8 schedule types.**
- **Medicare baseline:** Non-facility rate (not facility), per 42 CFR 447.203.
- **CHIP excluded** from per-enrollee calculations.
- **Minimum cell size:** n >= 11.
- **No em-dashes.** No "plain English."

---

## 10. T-MSIS Data Quality Rules

**Non-negotiable. Full detail in `COMPLETE-DATA-REFERENCE-FOR-ARADUNE.md` (Part 1: T-MSIS/TAF section) and `docs/TMSIS_DATA_GUIDE.md`.**

### What the data is
- **227M rows** = OT (Other Services) only. **Excludes** IP, LT, RX.
- Preliminary TAF: ~12-18 month lag. Final: ~24 months. **2024 incomplete** (Nov-Dec dropoff).
- **T-MSIS DuckDB (17.5GB) currently empty** — R pipeline must run first.

### 12 non-negotiable rules
1. Always specify service year ("CY2022 T-MSIS data").
2. Check DQ Atlas. Flag "Unusable"/"High Concern."
3. Apply OT claims filters: `MDCD_PD_AMT > 0 AND < 50000`, exclude voided/adjusted/denied (`ADJSTMT_IND NOT IN ('1','V')`), final claims only (`CLM_STUS_CTGRY_CD IN ('F1','F2','F3')`), require procedure code and service date.
4. Validate NPIs: 10 digits, not null/zeros. Use `SRVC_PRVDR_NPI`.
5. Separate FFS from encounters (`CLM_TYPE_CD`). Encounter amounts unreliable.
6. Surface MCO penetration context.
7. Use ASPE HCBS taxonomy.
8. T-MSIS does NOT capture MCO-to-provider payment rates.
9. SCD Type 2 logic for fee schedule temporal joins.
10. Document data vintage in every output.
11. Minimum cell size: n >= 11.
12. Never mix MAX and TAF without formal crosswalk.
13. **Illinois claims require custom dedup logic.** IL captures adjustments as incremental credits/debits, not void/replace. Standard TAF final-action algorithm fails. See CMS "How to Use Illinois Claims Data" documentation.
14. **DOGE T-MSIS release (Feb 2026):** OT claims only (no IP/LT/RX), no beneficiary state variable, suppresses <12 claims, Nov/Dec 2024 incomplete, high-MC states show misleadingly low paid amounts. Dataset was taken offline. Use with extreme caution if re-released.

### Per-source quality gates
**Rates:** Flag $0.00, >$10K E/M, unchanged 24+ months, >2x RVU-derived expected.
**Codes:** Verify expected E/M, track CPT additions/deletions.
**Medicare matching:** Validate locality, confirm CF, flag undocumented weighting.
**Cross-state:** Flag >3 SD from national mean.

---

## 11. Data Ingestion Pipeline

**READ FIRST: `COMPLETE-DATA-REFERENCE-FOR-ARADUNE.md` documents every known quality issue across all datasets, the validation stack, adversarial testing patterns, and medallion architecture. It is required reading before any data ingestion work.**

### Medallion Architecture

```
Bronze (raw, append-only)       Silver (normalized/cleaned)       Gold (analytics-ready)
├── Parquet as received         ├── ICD: GEMs + CCSR groupings    ├── Pre-computed PMPM
├── state/year/month partition  ├── NDC: 5-4-2 → RxNorm mapped   ├── Utilization metrics
├── _source_file                ├── NPI: taxonomy enriched        ├── Quality indicators
│   _ingestion_timestamp        ├── IL-specific claim dedup       ├── Pre-joined dimensions
│   _source_state, _batch_id    ├── Void/replacement logic        ├── Medical Care CPI adjusted
├── union_by_name=true          ├── Temporal: FFY/SFY/CY cols     └── State-level adj factors
├── Never modify (audit trail)  ├── DQ Atlas metadata carried     
└── ZSTD, 500K-1M row groups   └── DuckDB union_by_name for schema evolution  
```

### Core Ingestion Pattern
```python
def fetch_raw(source_config) -> bytes | Path:    # HTTP HEAD + ETag
def parse(raw) -> list[dict]:                     # Per-source; PDF → pdfplumber → Claude
def validate(parsed) -> ValidationResult:         # validator.py (15 checks)
def normalize(validated) -> list[dict]:            # Unified schema + URL + date
def load(normalized, db_conn) -> LoadResult:       # Upsert + version + S3 snapshot
```
**Hard stops:** Rate changed >90% · Code count dropped >20% · Schema mismatch
**Soft flags:** Rate unchanged >24 months · New codes without description · Rate >3 SDs

### Validation Stack
**Deployed:** `server/engines/validator.py` — 15 operational checks across 3 types (row count, range, referential integrity). API: /api/validation/latest, /results, /domains.
**Future (not yet implemented):**
- Soda Core v4 — native DuckDB support, ML anomaly detection
- dbt-duckdb + dbt-expectations — SQL-first validation macros
- Pandera — DataFrame validation with hypothesis testing
- datacontract-cli — CI/CD contract testing

### Adversarial Testing Layers
```
tests/unit/          — Hypothesis property tests, code validators
tests/integration/   — Schema contracts, referential integrity, dbt-expectations
tests/chaos/         — Schema drift, null injection, encoding, duplicates, volume spikes
tests/adversarial/   — Invalid codes, outlier values, boundary conditions
```
Key patterns: null injection at 1/5/10/25/50%, encoding chaos (smart quotes, null bytes), date format mixing in same column, 10x volume spikes, near-duplicate injection. Use **SDV** for realistic synthetic data, **Mimesis** (12-15x faster than Faker) for volume testing.

### Critical Per-Dataset Rules (summary)
- **T-MSIS:** Illinois custom dedup (incremental credits/debits, not void/replace). MC encounters may show $0. Check DQ Atlas per state/year. Final-release TAF only (12+ month runout). Store TAF version as metadata.
- **SDUD:** NDC 11-digit left-padding (5-4-2). Suppression <11 scripts. All amounts pre-rebate. Link via RxNorm for canonical drug ID.
- **CMS-64 vs T-MSIS:** Will never reconcile (payment date vs service date). CMS-64 = totals authority. TAF = service detail. Maintain both, show the gap.
- **HCRIS:** Not audited, not GAAP. Winsorize outliers. Multiple reports per provider (collapse duplicates, weight by fiscal year fraction). Two form versions need crosswalk.
- **NPPES:** 9.37M providers ingested (28 key columns + unpivoted taxonomy detail). 8.2% updated within past year. Taxonomy self-reported, unverified. Cross-reference PECOS + state licensing.
- **RBRVS/PFS:** CY2026 has -2.5% efficiency adjustment + separate APM CFs. Versioned annual snapshots required.
- **Hospital MRFs:** 21% compliance. Format chaos. No standardized payer naming. Outlier rates <$1 to >$1M.

### Reference Data: SCD Type 2
All reference tables (ICD-10, CPT, NDC, RxNorm, NUCC Taxonomy, FMAP, GPCIs, CFs) stored with `effective_date` + `termination_date` for point-in-time historical joins.

### Scheduling
| Frequency | Sources |
|-----------|---------|
| Weekly | NPPES, NADAC, Federal Register/CIBs/SHOs, LEIE |
| Monthly | BLS unemployment/CPI/FRED, MC enrollment, RxNorm |
| Quarterly | T-MSIS/SDUD/MBES-CBES, MCO MLR |
| Annual | Medicare PFS RVU, state fee schedules, HCRIS, BLS OEWS, ACS, AHRF, SVI |

### DuckDB Extensions
**Loaded at startup:** `json` (built-in), `fts` (full-text search for RAG), `vss` (vector similarity, loaded if available)
**Used by sync scripts:** `httpfs` (R2/S3 access, used by sync_lake.py)
**Available but not loaded:** `icu` (Unicode), `excel` (state uploads), `spatial` (geographic)
**Not implemented:** `iceberg` (table format), `duckpgq` (property graph — referenced in ontology spec, not loaded at runtime), `cache_httpfs`

---

## 12. Database Schema

```sql
CREATE TABLE dim_state (
    state_code VARCHAR(2) PRIMARY KEY, state_name VARCHAR, region VARCHAR,
    expansion_status BOOLEAN, fmap DECIMAL(5,4),
    managed_care_model VARCHAR, medicaid_agency_name VARCHAR, agency_url VARCHAR
);

CREATE TABLE fact_medicaid_rate (
    state_code VARCHAR(2), cpt_hcpcs_code VARCHAR(10), modifier VARCHAR(10),
    provider_type VARCHAR, population VARCHAR, geographic_area VARCHAR,
    effective_date DATE, medicaid_rate DECIMAL(10,2),
    source_file VARCHAR, last_updated DATE, dq_flag VARCHAR,
    PRIMARY KEY (state_code, cpt_hcpcs_code, modifier, provider_type,
                 population, geographic_area, effective_date)
);

CREATE TABLE fact_rate_comparison (
    state_code VARCHAR(2), cpt_hcpcs_code VARCHAR(10), category_447 VARCHAR,
    population VARCHAR, provider_type VARCHAR, geographic_area VARCHAR, year INTEGER,
    medicaid_rate DECIMAL(10,2), medicare_nonfac_rate DECIMAL(10,2),
    pct_of_medicare DECIMAL(6,4), claim_count INTEGER, beneficiary_count INTEGER,
    PRIMARY KEY (state_code, cpt_hcpcs_code, category_447, population,
                 provider_type, geographic_area, year)
);

CREATE TABLE forecast_enrollment (
    state_code VARCHAR(2), eligibility_group VARCHAR,
    forecast_date DATE, run_date DATE, model_id VARCHAR,
    point_estimate INTEGER, lower_80 INTEGER, upper_80 INTEGER,
    lower_95 INTEGER, upper_95 INTEGER,
    PRIMARY KEY (state_code, eligibility_group, forecast_date, model_id)
);
```

**DuckDB notes:** `DECIMAL(10,2)` for rates (never FLOAT). `DATE` for dates (never string). Skip PKs during bulk load. DuckDB-WASM for browser queries <5M rows.

---

## 13. Data Universe

### Current Lake by Domain

| Domain | Key tables | Scale |
|--------|-----------|-------|
| **Rates** | fact_medicaid_rate, fact_rate_comparison, dim_procedure | 597K rates (47 states), 302K comparisons, 16,978 codes |
| **Enrollment** | fact_enrollment, fact_mc_enrollment_summary, fact_elig_group_*, fact_unwinding, fact_medicare_monthly_enrollment | Monthly 2013-2025, MC penetration %, unwinding outcomes, Medicare monthly (557K) |
| **Claims** | fact_claims, fact_drug_utilization, fact_sdud_2020-2025, fact_sdud_combined (28.3M), fact_doge_* | 712K claims, SDUD 2020-2025 28.3M rows ($1.05T pre-rebate), DOGE 190M rows (5 aggregated tables) |
| **Hospitals** | fact_hospital_cost, fact_hospital_rating/vbp/hrrp, fact_dsh_hospital, fact_hcahps, fact_hospital_ownership | 6,103 hospitals, financials, quality, DSH, HCAHPS, ownership |
| **Nursing** | fact_five_star, fact_pbj_nurse/nonnurse_staffing, fact_snf_cost/vbp, fact_mds_facility_level | 65M+ PBJ, MDS 29.2M, Five-Star, deficiencies |
| **LTSS/HCBS** | fact_hcbs_waitlist, fact_ltss_expenditure, fact_cms372_waiver | 607K waiting (41 states), waivers |
| **Workforce** | fact_bls_wage/_msa/_national, fact_hpsa, fact_workforce_projections, fact_health_center_awards, fact_hrsa_awarded_grants (71K), fact_hrsa_active_grants (19K), fact_bh_workforce_projections | 69K HPSAs, 121 professions to 2038, HRSA grants (awarded + active), BH projections |
| **Pharmacy** | fact_nadac, fact_sdud_2020-2025, fact_sdud_combined (28.3M rows), fact_aca_ful, fact_drug_rebate, fact_drug_spending_* | NADAC, SDUD 2020-2025 ($1.05T pre-rebate), opioid prescribing, Part B/D drug spending |
| **BH** | fact_nsduh*, fact_teds_admissions, fact_mh_facility, fact_block_grant, fact_teds_detail, fact_nsduh_2024_sae | 1.6M+ TEDS, BH facilities, SAMHSA detail, NSDUH 2024 |
| **Quality** | fact_quality_core_set_2023/2024, fact_scorecard, fact_epsdt, fact_hac_measures | 57 measures, 51 states, HAC |
| **Expenditure** | fact_expenditure, fact_cms64_multiyear (FY2018-2024, 118K rows), fact_fmr_fy2024, fact_macpac_spending*, fact_nhe, fact_nhe_projections | CMS-64 multi-year ($5.7T, 7 FYs), MACPAC, NHE historical + projections |
| **Economic** | fact_acs_state, fact_unemployment, fact_cpi, fact_saipe, fact_snap/tanf, fact_county_health_rankings, fact_bea_*, fact_census_state_finances, fact_tax_foundation_* | BLS, Census, FRED, BEA, Tax Foundation, county health, HUD FMR/SAFMR |
| **Medicare** | fact_medicare_enrollment, fact_medicare_geo_variation, fact_mssp_*, fact_chronic_conditions_*, fact_mcbs, fact_cms_program_stats_* | Geo variation 2014-2023, 511 ACOs, chronic conditions, MCBS, program stats |
| **Policy** | fact_policy_document, fact_policy_chunk, fact_federal_register_cms | 1,039 docs, 6,058 chunks, Federal Register rules |
| **Post-Acute** | fact_pac_*, fact_home_health_agency, fact_hospice_*, fact_dialysis_* | HHA, hospice, dialysis, IRF, LTCH, PAC casemix |
| **Public Health** | fact_places_county, fact_cdc_overdose_deaths, fact_vital_stats, fact_cdc_natality, fact_immunization, fact_food_environment | 3,144 counties, overdose, natality, immunization, food environment |
| **Maternal & Child** | fact_maternal_morbidity, fact_pregnancy_outcomes, fact_nas_rates, fact_wic, fact_foster_care, fact_title_v | SMM, NAS, WIC, foster care, Title V |
| **State Fiscal** | fact_census_state_finances, fact_tax_foundation_*, fact_fmap_historical, fact_nhe | Census finances (all 50 states), tax rankings |
| **Program Integrity** | fact_leie, fact_open_payments, fact_mfcu_stats, fact_perm_rates, fact_federal_register_cms | LEIE exclusions, Open Payments ($13B), MFCU, PERM |
| **Insurance Market** | fact_mlr_*, fact_risk_adjustment, fact_ma_stars | MLR reports, risk adjustment, MA star ratings |
| **Provider Network** | fact_nppes, fact_nppes_taxonomy, fact_pecos, fact_affiliations, fact_cah, fact_gme, fact_provider_reassignment (3.5M) | NPPES 9.37M providers, PECOS, CAHs, GME, NPI reassignment |
| **ACO/VBC** | fact_mssp_aco/participants/financial, fact_aco_reach_results/providers/beneficiaries | 511 ACOs PY2026, REACH 2026, beneficiaries |
| **KFF Medicaid** | fact_kff_total_spending, fact_kff_spending_per_enrollee, ... (28 tables) | 28 KFF Medicaid policy/spending tables |
| **Medicaid.gov** | fact_drug_amp, fact_mlr_summary, fact_mc_programs, ... (17 tables) | Drug AMP (5.5M), MLR, MC programs, DSH annual |

**Total:** 750+ registered views, 400M+ rows, 4.9 GB Parquet. 20 ontology domains, 16 entities, 19 named metrics.

### Category Completion Summary

| Category | Status | Key gaps |
|----------|--------|----------|
| Hospital Quality & VBP | **Done** | HCAHPS, Care Compare, ownership all added |
| Behavioral Health | **Done** | TEDS detail, NSDUH 2024, SAMHSA facilities |
| Children's/CHIP/EPSDT | **Done** | WIC, foster care, Title V, natality, immunization added |
| LTSS/HCBS | **Partially done** | CMS-64 Schedule B, 1915(c) utilization, HCBS quality, DSW workforce, 1915(k) |
| Eligibility & Unwinding | **Done** | — |
| Pharmacy | **Done** | SDUD 2020-2025 (28.3M rows), 340B blocked (Blazor app), State MAC not started |
| Economic/Contextual | **Done** | Census state finances, BEA, Tax Foundation, county health rankings all added |
| Rates & Fee Schedules | **Core done** | CLFS done, DMEPOS and Ambulance in raw but not ingested |
| Medicare | **Done** | Chronic conditions, MCBS, Part D prescriber, outpatient by provider, program stats added |
| SDOH | **Substantially done** | CDC PLACES, food environment, county health rankings, RUCA done. SVI, ADI, AHRQ SDOH blocked (WAF) |
| Program Integrity | **Done** | LEIE, Open Payments, MFCU stats, PERM rates, Federal Register all added |
| Provider Network | **Done** | NPPES (9.37M), PECOS, affiliations, CAHs, GME, provider reassignment (3.5M NPI mappings) |
| Insurance Market | **Done** | MLR, risk adjustment, MA stars |
| State Fiscal | **Done** | Census finances, Tax Foundation, FMAP historical |
| KFF Medicaid Policy | **Done** | 28 KFF tables covering spending, enrollment, eligibility, benefits |
| Maternal & Child Health | **Done** | Natality, immunization, WIC, foster care, Title V |

### AHEAD Readiness — HCRIS Field Map
```
Operating/total margin   → net_income / net_patient_revenue (Worksheet G)
Current ratio            → total_assets / total_liabilities (Worksheet G)
Cost-to-charge ratio     → cost_to_charge_ratio (Worksheet D-1/D-4)
Payer mix (days)         → medicare_days, medicaid_days, total_days (Worksheet S-3)
Uncompensated care       → uncompensated_care_cost (Worksheet S-10)
Medicare DSH / IME       → dsh_adjustment, dsh_pct, ime_payment (Worksheet E)
Discharges, bed count    → total_discharges, bed_count (Worksheet S-3)
```
**Gaps:** No hospital-level UPL/SDP. No Maryland peers. No Medicaid FFS/MC split. Only FY2023.

### Supplemental Payments
**Done:** CMS DSH Allotment, CMS-64 FMR, SDP preprint (34 states), MACPAC supplemental.
**Not started:** UPL demonstrations, HRSA GME, OIG DSH audits.
**Partial:** 1115 waiver financials (647 waivers, metadata only).

### Priority Data Additions

| # | Dataset | Status | Notes |
|---|---------|--------|-------|
| 1 | HHS DOGE Medicaid Provider Spending (190M rows) | **DONE** | 5 aggregated tables in lake |
| 2 | Medicare PFS RVU files (annual + quarterly) | **DONE** | CY2025 RVU, quarterly updates |
| 3 | MCO MLR reports | **DONE** | MLR summary from Medicaid.gov |
| 4 | AHRQ SDOH Database | **Blocked** | WAF prevents download |
| 5 | Area Deprivation Index (block group) | **Blocked** | WAF prevents download |
| 6 | Medicare Provider Utilization PUFs | **DONE** | Part D prescriber, outpatient by provider, procedure summary |
| 7 | OIG LEIE exclusion list | **DONE** | In lake |
| 8 | CMS Open Payments ($13.18B) | **DONE** | Aggregated to state x specialty x payment type |
| 9 | CDC SVI + USDA RUCA | **Partial** | RUCA done, SVI blocked (WAF) |
| 10 | Federal Register API | **DONE** | CMS rules via FR API |
| 11 | CLFS + DMEPOS + Ambulance schedules | Open | Raw files in data/raw/ (clfs/, dmepos/, ambulance/), not yet ingested |
| 12 | HRSA AHRF (6,000 variables/county) | **DONE** | Processed from raw |
| 13 | NPPES full registry (9.37M NPIs) | **DONE** | 28 key columns + taxonomy detail |
| 14 | KFF Medicaid data (28 tables) | **DONE** | Via Google Sheets CSV export |
| 15 | Census state finances | **DONE** | Revenue, expenditure, debt |
| 16 | BEA economic data | **DONE** | Personal income, transfer payments |
| 17 | PECOS + GME + CAHs | **DONE** | Provider network tables |
| 18 | Medicare provider reassignment (568 MB) | **DONE** | `fact_provider_reassignment` 3.49M rows |
| 19 | HRSA awarded grants (83 MB) | **DONE** | `fact_hrsa_awarded_grants` 70,902 rows |
| 20 | BLS OEWS full occupation x area 2024 (78 MB) | **In lake** | `fact_bls_oews` 38K rows (state-level); MSA detail incremental |
| 21 | HRSA active grants (12 MB) | **DONE** | `fact_hrsa_active_grants` 18,641 rows |
| 22 | Promoting Interoperability (3.8 MB) | **In raw** | `pi_dataset_feb2026.csv`, verify vs existing pi_performance |
| 23 | ACA effectuated enrollment | **In raw** | `effectuated_enrollment.xlsx`, marketplace enrollment by state |
| 24 | SAMHSA NSDUH 2022 | **In raw** | `samhsa_nsduh_2022.csv`, fills gap (lake has 2023+2024) |
| 25 | HCRIS full worksheet ZIPs (260 MB) | **In raw** | Deeper cost report detail beyond flat CSVs already ingested |

### Remaining Fee Schedules
4 states: KS/NJ (portal login), TN (MC only), WI (manual).

### Data Sensitivity Rings

| Ring | Data | HIPAA | Status |
|------|------|-------|--------|
| **0** | Public regulatory: fee schedules, RVUs, SPAs | None | Here now |
| **0.5** | Economic/contextual: BLS, FRED, Census, SDOH | None | Here now |
| **1** | Aggregated/de-identified: T-MSIS open data, DOGE | Minimal | Here now |
| **2** | Provider-level: billing volumes, network data | Low — BAA | Future |
| **3** | Claims/encounter: T-MSIS/TAF, state MMIS | Full HIPAA | After BAA + HITRUST |

**Stay in Ring 0/0.5/1 until BAA, SOC 2 Type II, and HITRUST in place.**

---

## 14. Caseload & Expenditure Forecasting

### Phase 1 — DONE: Caseload
**Engine:** `server/engines/caseload_forecast.py` (~650 lines). SARIMAX + ETS model competition, intervention variables, economic covariates, holdout MAPE.
**Frontend:** `src/tools/CaseloadForecaster.tsx` (~830 lines) at `/#/forecast`.
**API:** 6 endpoints — templates, generate (JSON + CSV), public enrollment.

### Phase 2 — DONE: Expenditure
**Engine:** `server/engines/expenditure_model.py` (~430 lines). Caseload forecast + params CSV. Trend, admin load, risk margin, policy adjustments.
**API:** 4 endpoints — template, pipeline, CSV, expenditure-only.

### Phase 3 — DONE: Scenario Builder
4 sliders (unemployment, eligibility, rate change, MC shift). Presets. Baseline vs scenario chart.

### Phase 4 — Target: Fiscal Impact Engine
Adjust rate increase % → federal match at FMAP → UPL headroom → SDP cap under OBBBA → budget impact across biennium. Connects fee schedule, FMAP, CMS-64, actuarial trends.

### Phase 5 — DONE: System Dynamics
**Engine:** `server/engines/system_dynamics.py` (~512 lines). Stock-flow ODE modeling via scipy.integrate.solve_ivp. 12 stocks in integrated model, 6 cross-domain feedback loops, lake-calibrated parameters with fallback chain.
**Models:** Enrollment (eligible→processing→enrolled→disenrolled), Provider Participation (rate→providers→access), Workforce Pipeline (wage→recruitment→retention→staffing), HCBS Rebalancing (funding→transition→community vs institutional).
**Integrated model:** Connects all 4 through coupling: rate→providers→access→enrollment, wages→workforce→capacity, HCBS funding→spending shift, enrollment→spending→budget pressure.
**Frontend:** PolicySimulator.tsx (~500 lines) at `/#/policy-simulator`. Intervention builder, 5 presets, baseline vs scenario charts, feedback loops panel. 4 embedded DynamicsWidget instances in CaseloadForecaster, WageAdequacy, HcbsTracker, RateBrowse.
**API:** 5 POST endpoints at /api/dynamics/ (enrollment, provider, workforce, hcbs, policy-simulator).

---

## 15. Security & HIPAA

### Controls
- AES-256 at rest, TLS 1.2+ in transit, no plaintext secrets
- RBAC, MFA admin, tenant isolation, expiring sessions
- Immutable logs (6-10 year retention). Secrets via env vars only.
- User data session-scoped, never persisted, never shared.

### Certifications (priority order)
1. **SOC 2 Type II** — $30K-80K, 6-12 months. Minimum for enterprise.
2. **GovRAMP Ready** — $50K-125K, 6-12 months. 27 states recognize.
3. **HITRUST i1** — $70K, 5-8 months. MCO sales differentiator.
4. **FedRAMP** — $250K+, 12-24 months. Federal only. Deprioritize.

### AI Governance (always enforce)
- Every output: source attribution, confidence, data quality warnings
- No beneficiary-affecting output without human confirmation
- Bias monitoring across race, ethnicity, geography, disability
- NIST AI RMF alignment. CMS AI Playbook v4. Full audit trail.

### Never
- Host raw PHI without DUA + HITRUST
- Build fraud detection before BAA
- Allow cross-provider visibility without opt-in
- Use T-MSIS outside ResDAC terms
- Publish counts below n=11
- Log user query content that could reveal PHI

### Regulatory deadlines driving urgency

| Deadline | Requirement | Aradune capability |
|----------|------------|-------------------|
| **July 1, 2026** | CPRA rate transparency (§447.203) | CPRA tool — ready now |
| **July 1, 2026** | Publish all FFS rates publicly | Rate Lookup + Directory |
| **July 1, 2026** | HCBS hourly rate disclosure | Workforce & HCBS |
| **January 1, 2027** | OBBBA work requirements | Forecasting + fiscal impact |
| **~July 2027** | Appointment wait times (90% compliance) | Network adequacy (future) |
| **~July 2028** | 80% HCBS compensation pass-through | HCBS tracking |
| **FY 2030** | 3% eligibility error rate penalty | Program integrity (future) |

---

## 16. File Map

```
Aradune/
├── CLAUDE.md                        ← THIS FILE
├── ARADUNE_BUILD_GUIDE.md           ← Phased build plan
├── ONTOLOGY_SPEC.md                 ← Entity registry build specification
├── COMPLETE-DATA-REFERENCE-FOR-ARADUNE.md ← Dataset quality, validation, adversarial testing
├── CHANGELOG.md                     ← Session history, resolved issues
├── package.json / vite.config.ts / tsconfig.json / vercel.json / index.html
│
├── ontology/                        ← ENTITY REGISTRY (auto-generates system prompt + DuckPGQ)
│   ├── schema.yaml                  ← Master schema definition
│   ├── generated_prompt.md          ← Auto-generated system prompt (33.7KB, 722 tables)
│   ├── entities/                    ← One YAML per entity type (16 files)
│   │   ├── state.yaml, procedure.yaml, provider.yaml, hospital.yaml,
│   │   ├── mco.yaml, rate_cell.yaml, drug.yaml, quality_measure.yaml,
│   │   └── nursing_facility.yaml, workforce.yaml, hcbs_program.yaml, ...
│   ├── domains/                     ← One YAML per data domain (20 files)
│   │   ├── rates.yaml, enrollment.yaml, hospitals.yaml, quality.yaml,
│   │   ├── state_fiscal.yaml, insurance_market.yaml, program_integrity.yaml, ...
│   └── metrics/                     ← Named deterministic metric definitions (5 files)
│       ├── rate_metrics.yaml, enrollment_metrics.yaml, fiscal_metrics.yaml, ...
│
├── src/
│   ├── Platform.tsx                 ← ~980 lines. Router, tool registry, PasswordGate
│   ├── design.ts                    ← Design tokens (C, FONT, SHADOW, BP, useIsMobile)
│   ├── tools/
│   │   ├── IntelligenceChat.tsx     ← ~850 lines. Intelligence home, SSE, file upload
│   │   ├── TmsisExplorer.tsx        ← ~2,400 lines. → Rates: Browse & Compare
│   │   ├── CpraGenerator.tsx        ← 734 lines. → CPRA Compliance
│   │   ├── CaseloadForecaster.tsx   ← ~830 lines. → Forecast
│   │   ├── StateProfile.tsx         ← ~1,000 lines. → States (add comparison)
│   │   ├── AheadReadiness.tsx       ← → AHEAD (merge with Calculator)
│   │   ├── AheadCalculator.tsx      ← → AHEAD (merge)
│   │   ├── WageAdequacy.tsx         ← 546 lines. → Workforce: Wages
│   │   ├── QualityLinkage.tsx       ← 445 lines. → Workforce: Quality
│   │   ├── HcbsCompTracker.tsx      ← 414 lines. → Workforce: HCBS
│   │   ├── PolicySimulator.tsx      ← ~500 lines. → Policy Simulator (system dynamics)
│   │   ├── FeeScheduleDir.tsx       ← 535 lines. → Rate Lookup: Directory
│   │   ├── RateLookup.tsx           ← → Rate Lookup
│   │   ├── RateReductionAnalyzer.tsx ← 411 lines. → Rates (integrate)
│   │   ├── BehavioralHealth.tsx      ← 627 lines. → BH/SUD (4 tabs: prevalence, treatment, opioid, conditions)
│   │   ├── PharmacyIntelligence.tsx  ← 408 lines. → Pharmacy (3 tabs: spending, top drugs, NADAC)
│   │   ├── NursingFacility.tsx      ← 662 lines. → Nursing (3 tabs: quality, staffing, detail)
│   │   ├── SpendingEfficiency.tsx   ← 752 lines. → Spending (3 tabs: per-enrollee, total, efficiency)
│   │   ├── HospitalRateSetting.tsx  ← 436 lines. → Hospital Rates (3 tabs: financials, DSH, SDP)
│   │   ├── ProgramIntegrity.tsx     ← 654 lines. → Integrity (3 tabs: LEIE, Open Payments, MFCU/PERM)
│   │   ├── PolicyAnalyst.tsx        ← 378 lines. DEPRECATED → Intelligence
│   │   └── DataExplorer.tsx         ← DEPRECATED → Intelligence
│   ├── components/
│   │   ├── StateContextBar.tsx    ← Reusable cross-dataset context panel (compact + expanded)
│   ├── hooks/
│   │   └── useStateContext.ts     ← Shared state context fetch hook with client cache
│   ├── engine/
│   │   └── StateRateEngine.js       ← 1,153 lines. 42/42 tests. → Rates: Rate Builder
│   ├── utils/                       ← 16 files: reportDocx/Pdf/Xlsx/Markdown, chartExport, cpraPdf/Xlsx, aheadPdf/Xlsx/Scoring, pdfReport, exportCsv, compliancePdf, ccbhcPdf, rateBuilderPdf/Xlsx
│   │   ├── formatContext.ts       ← Shared format helpers (fmtB, fmtPct, fmtDollar, SYM)
│   ├── lib/                         ← api.ts, duckdb.ts, queryEngine.ts
│   └── data/states.ts               ← STATE_NAMES, STATES_LIST
│
├── public/data/                     ← JSON/Parquet served to frontend
│   ├── cpra_em.json (615KB), dq_flags_em.json, dim_447_codes.json, cpra_summary.json
│   ├── hcpcs.json, states.json, trends.json, medicare_rates.json, fee_schedules.json
│   ├── system_prompt.md, fl_methodology_addendum.md
│   └── [external] claims_monthly.parquet (82MB) via VITE_MONTHLY_PARQUET_URL
│
├── public/assets/                   ← Brand: logo-full.png, logo-mark.png, logo-wordmark.png, icon-bot.png
│
├── api/chat.js                      ← 515 lines. LEGACY — deprecate after Intelligence verified
│
├── server/                          ← FastAPI backend
│   ├── main.py / db.py / config.py / query_builder.py
│   ├── Dockerfile / fly.toml / entrypoint.sh
│   ├── ontology/                    ← Python registry loaded at startup
│   │   ├── registry.py              ← Loads YAML, provides entity/metric lookup API
│   │   └── prompt_generator.py      ← Generates Intelligence prompt section from registry
│   ├── engines/
│   │   ├── cpra_upload.py           ← 821 lines. CPRA upload engine
│   │   ├── caseload_forecast.py     ← ~650 lines. SARIMAX+ETS
│   │   ├── expenditure_model.py     ← ~430 lines. Expenditure projection
│   │   ├── system_dynamics.py       ← Stock-flow ODE models (enrollment, provider, workforce, HCBS, integrated)
│   │   ├── rag_engine.py            ← ~460 lines. BM25 + FTS policy search
│   │   └── query_router.py          ← Tier 1-4 classification + resource allocation
│   └── routes/                      ← 40+ files (27 top-level + 13 research), ~345 endpoints
│       ├── intelligence.py          ← Claude + SSE + DuckDB + RAG
│       ├── cpra.py                  ← Pre-computed + upload CPRA
│       ├── lake.py                  ← /api/states, enrollment, quality, expenditure
│       ├── nl2sql.py                ← NL2SQL for Data Explorer
│       ├── forecast.py              ← 10 caseload + expenditure endpoints
│       ├── dynamics.py              ← 5 system dynamics API endpoints
│       └── [17 more: query, meta, presets, pharmacy, policy, wages, hospitals,
│            enrollment, staffing, quality, context, bulk, supplemental,
│            behavioral_health, round9, insights, corpus]
│
├── tools/mfs_scraper/               ← Terminal B CPRA pipeline
│   ├── cpra_engine.py               ← 968 lines
│   └── aradune_cpra.duckdb          ← 1.87M rows
│
├── pipeline/
│   ├── dagster_pipeline.py          ← 13 assets, 3 checks, 3 jobs, 2 schedules
│   └── tmsis_pipeline_duckdb.R      ← 71KB. T-MSIS (227M rows).
│
├── data/
│   ├── lake/                        ← 400M+ rows, 750+ views, 4.9 GB
│   ├── ontology/                    ← Entity registry (auto-generates system prompt + DuckPGQ)
│   │   ├── entities/                ← One YAML per entity type (state, procedure, hospital, etc.)
│   │   ├── domains/                 ← One YAML per data domain (rates, enrollment, hospitals, etc.)
│   │   ├── metrics/                 ← Named metric definitions with canonical formulas
│   │   └── lake_inventory.json      ← Auto-generated schema inventory of all tables
│   ├── reference/cpra/              ← em_codes.csv, code_categories.csv, GPCI2025.csv
│   └── raw/                         ← T-MSIS DuckDB (17.5GB), NPPES (11.2GB), DOGE DuckDB (17.5GB)
│
├── scripts/                         ← 105 build_lake_*.py + utility scripts, sync_lake_wrangler.py
│   ├── generate_ontology.py         ← Reads YAML → generates system prompt + DuckPGQ SQL
│   ├── introspect_lake.py           ← DuckDB introspection → raw_inventory.json
│   ├── validate_ontology.py         ← CI: validates YAML against schema + lake
│   ├── sync_lake_wrangler.py        ← R2 upload via wrangler --remote (bypasses boto3 SSL)
│   ├── sync_lake.py                 ← R2 download via boto3 (used by Fly.io entrypoint, incremental)
│   ├── build_cache_seeds.py         ← Populates server/cache_seeds.json for demo mode
│   └── build_lake_*.py              ← 115+ ETL scripts across all data domains
├── scripts/adversarial/             ← 7-agent adversarial testing suite
│   ├── runner.py                    ← Test orchestrator (runs all agents, reports results)
│   ├── skillbook_import.py          ← Converts test failures → Skillbook skills (closed feedback loop)
│   ├── config.py                    ← Agent configuration and thresholds
│   ├── agents/                      ← 7 agent modules (intelligence, api_fuzzer, consistency, persona, florida_rate, skillbook, browser)
│   └── fixtures/known_facts.json    ← 28 ground-truth anchor facts across 11 domains
├── scripts/prune_skillbook.py       ← Skillbook maintenance: prune low-score/stale skills
│
├── .github/workflows/ci.yml        ← Build, lint, deploy Vercel + Fly.io
├── .github/workflows/adversarial.yml ← Weekly adversarial test run + auto-import to Skillbook + issue on failure
├── docs/
│   ├── adr/                         ← Architecture Decision Records
│   │   ├── 001-duckdb-over-postgresql.md
│   │   ├── 002-parquet-over-delta-iceberg.md
│   │   ├── 003-claude-api-with-skillbook.md
│   │   ├── 004-data-partitioning-strategy.md
│   │   └── 005-auth-architecture.md
│   ├── ARADUNE_MASTER.md / TMSIS_DATA_GUIDE.md / AraduneMockup.jsx
│   ├── SESSION-34-DEPLOY-GUIDE.md   ← Session 34 deployment guide
└── ...
```

**db.py critical note:** Only facts in `fact_names` (currently 750+ entries) are registered as views. Always update when adding lake tables. `_latest_snapshot()` supports both `data.parquet` and `snapshot=*/data.parquet` formats.

---

## 17. Build Principles

1. Always build to the unified schema.
2. Validation is not optional.
3. Source provenance is not optional. Every record → URL + download date.
4. Ship ugly. 50 states working > 5 states beautiful.
5. Coverage > polish.
6. Federal data first (covers all states).
7. Florida pipeline is the template.
8. PDF parsing prompts are versioned.
9. FL Medicaid: Facility and PC/TC rates are typically mutually exclusive (99.96% of codes). Three codes (46924, 91124, 91125) legitimately carry both as published by AHCA.
10. Data layer is the moat. Every session: add data, improve quality, or make adding easier.
11. Don't be CPRA-forward. Build for the platform.
12. Economic/contextual data matters.
13. Forecasting models are never deleted.
14. User data never mixed with public layer.
15. Log predictions. Compare to actuals. Publish accuracy.
16. No em-dashes. No "plain English."
17. Upload data in context, not standalone.
18. Intelligence is the connective tissue. Every tool → Intelligence.
19. Compliance artifacts are first-class outputs. Not dashboards — submission-ready documents.
20. Closed-loop: analysis → recommendation → action template → execution.
21. **Ontology-first data additions.** Every new dataset gets a YAML entity/domain definition before or alongside ingestion. Run `validate_ontology.py` and `generate_ontology.py` after every addition. Intelligence's system prompt and the DuckPGQ graph update automatically.
22. **Named metrics are deterministic.** Key calculations (pct_of_medicare, per_enrollee_spending, cpra_ratio) are defined once in `ontology/metrics/` with explicit formulas, source tables, and caveats. Intelligence references these by name. Same question always produces same number.
23. **Never trust a single source.** Expenditures: triangulate TAF + CMS-64 + supplemental. Providers: cross-reference NPPES + PECOS + HCRIS. Drugs: link SDUD + FDA NDC + RxNorm + NADAC. Build validation checks that compare sources against each other.
24. **State-level variation is the dominant quality dimension.** Illinois needs custom dedup. Arkansas may be missing. MC encounter completeness ranges from excellent to absent. Every pipeline starts with DQ Atlas lookup and carries state-quality metadata through.
25. **Test adversarially, not just defensively.** Soda Core/dbt catch known issues. Hypothesis + chaos engineering catch unknown ones. Generate realistic test data with SDV. Test at 10x volume. See `COMPLETE-DATA-REFERENCE-FOR-ARADUNE.md` Part 2.
26. **Update Sections 1-16 in place, not just the changelog.** When you change a count (endpoints, tables, modules), a capability (new engine, new tool), or a status (fixed issue, new known issue), update the relevant summary section AND the changelog. The summary sections are what the next session reads first. Stale summaries cause compounding errors across sessions. Also reconcile ARADUNE_FULL_BUILD.md if the change affects architecture, engines, modules, or routes.

---

## 18. Monetization

### Track A: Partnership / Acquisition (active)
Demo pitch: "Your analysts spend thousands of hours assembling data that already exists in one place. Aradune turns that data into compliance-ready documents, fiscal impact models, and early warning signals — through a conversation, not a spreadsheet."
Target acquirers: Gainwell/Veritas (natural fit), Nordic Capital, Merative. Valuations: 5-9x revenue.

### Track B: Independent SaaS (future)
| Tier | Price | Who |
|---|---|---|
| Free | $0 | Journalists, advocates, students |
| Analyst | $99/mo | Individual analysts |
| Pro | $299/mo | Consulting teams |
| State Agency | $50-200K/yr | State agencies (75% FFP eligible) |
| Enterprise | $50-500K/yr | Firms, MCOs, hospitals |

Pricing in CLAUDE.md, **not on live site**. Both tracks use same codebase.

---

## 19. Build & Deploy

```bash
cd ~/Desktop/Aradune
npm install && npm run dev                    # localhost:5173
cd tools/mfs_scraper/ && python cpra_engine.py --all --cpra-em --output-dir ../../public/data/
npm run build && npx vercel --prod
curl -s -o /dev/null -w "%{http_code}" https://www.aradune.co/data/cpra_summary.json
git add . && git commit -m "describe change" && git push

# Ontology (run after any data lake changes)
python scripts/validate_ontology.py           # CI check — must pass
python scripts/generate_ontology.py           # Regenerates system prompt + DuckPGQ SQL

# R2 sync (upload new lake data to Cloudflare R2 for Fly.io)
python3 scripts/sync_lake_wrangler.py                       # upload all (uses --remote)
python3 scripts/sync_lake_wrangler.py --only "fact/my_table" # upload specific table
python3 scripts/sync_lake_wrangler.py --dry-run              # preview only

# Fly.io deploy (from project root, NOT from server/)
fly deploy --remote-only --config server/fly.toml --dockerfile server/Dockerfile
```

**R2 sync notes:**
- `sync_lake_wrangler.py` uses `npx wrangler r2 object put --remote`. The `--remote` flag is **critical** -- without it wrangler uploads to a local emulator.
- `sync_lake.py` (used by Fly.io entrypoint) downloads via boto3. Skips existing files with matching size (incremental).
- Fly.io entrypoint always runs background R2 sync + sends reload signal via Python urllib after download completes.
- After uploading new tables to R2, restart Fly.io machines or trigger reload: `fly ssh console --app aradune-api --command "python3 -c \"import urllib.request; urllib.request.urlopen(urllib.request.Request('http://localhost:8000/internal/reload-lake', method='POST'))\""`

**Env vars:** Vercel: `ANTHROPIC_API_KEY`, `VITE_MONTHLY_PARQUET_URL`. Fly.io: `ANTHROPIC_API_KEY`, R2 creds (`ARADUNE_S3_BUCKET`, `ARADUNE_S3_ENDPOINT`, `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`).

---

## 20. Reference Documents

| File | Purpose |
|------|---------|
| `ARADUNE_BUILD_GUIDE.md` | Phased build plan, tool specs, demo script |
| `ONTOLOGY_SPEC.md` | Entity registry build spec — **hand to Claude Code for ontology generation** |
| `COMPLETE-DATA-REFERENCE-FOR-ARADUNE.md` | **Per-dataset quality issues, validation stack, adversarial testing, medallion architecture — READ BEFORE ANY DATA WORK** |
| `CHANGELOG.md` | Session history, resolved issues, milestones |
| `docs/ARADUNE_MASTER.md` | Full strategy/architecture reference |
| `docs/TMSIS_DATA_GUIDE.md` | T-MSIS operational guide — read before any T-MSIS work |
| `docs/AraduneMockup.jsx` | Landing page + nav reference |

---

## 20. Known Issues

| # | Issue | Status |
|---|-------|--------|
| 1 | R2 credentials need rotation | Open |
| 2 | db.py fact_names must match filesystem (750+ entries synced) | Synced |
| 3 | api/chat.js is legacy | Deprecate after Intelligence verified |
| 4 | Password gate is client-side only | Not a security boundary |
| 5 | GitHub CI secrets (VERCEL_TOKEN, FLY_API_TOKEN) not set | **Fixed** -- Session 34. All 6 secrets set. |
| 6 | Clerk auth needs env vars (VITE_CLERK_PUBLISHABLE_KEY, CLERK_SECRET_KEY) | **Fixed** -- Session 34. Keys in GitHub. Confirm live vs test before demo. |
| 7 | AHRQ SDOH / CDC SVI blocked by WAF | Cannot download programmatically |
| 8 | 4 state fee schedules missing (KS, NJ, TN, WI) | **Fixed** -- Session 30. KS/NJ/WI added. TN excluded (94% MC, no FFS). |
| 9 | Remaining raw files: HCRIS full worksheets (260 MB), MACPAC exhibits (small), SAMHSA NSDUH 2022 (HTML/blocked) | Session 19 -- major gaps closed. HCRIS summary tables already in lake. |
| 10 | 17 empty/broken raw files in data/raw/ (header-only stubs, WAF failures) | Cleanup candidate |
| 11 | Duplicate raw files in data/raw/ (_v2 pairs, dme26a=dmepos) | Cleanup candidate |
| 12 | HPSA count shows row count not unique HPSA count | Minor -- cosmetic |
| 13 | pharmacy/enrollment/wages routes lack error handling | **Fixed** -- Session 30. @safe_route on all ~345 endpoints. |
| 14 | AHEAD module hardcoded to 6 states/12 hospitals | Save for last per Scott |
| 15 | R2 has ~253/760 parquet files | Need full `sync_lake_wrangler.py` run (865 files, 4.8 GB) |
| 16 | "Ask Aradune" homepage button was broken in dev (StrictMode) | **Fixed** -- Session 27 |
| 17 | Mobile: tables overflowed on small screens | **Fixed** -- Session 27, all tables wrapped |
| 18 | sync_lake_wrangler.py missing --remote flag | **Fixed** -- Session 28 |
| 19 | entrypoint.sh used curl (not in slim image) | **Fixed** -- Session 28, uses Python urllib |
| 20 | FL "mutual exclusion rule" was false | **Fixed** -- Session 34. Corrected across 13+ files. |
| 21 | Intelligence: Guam/territory fallback returned generic 74-char error | **Fixed** -- Session 34. Territory-aware fallback. |
| 22 | Intelligence: DOGE quarantine caveats inconsistent (prompt-only) | **Fixed** -- Session 34. Programmatic injection in _execute_tool. |
| 23 | Intelligence: IL T-MSIS caveat not triggering | **Fixed** -- Session 34. Programmatic injection. |
| 24 | Intelligence: em-dashes in responses | **Fixed** -- Session 34. _postprocess_response strips all dash types. |
| 25 | Intelligence: no DuckDB query timeout | **Fixed** -- Session 34. 30s statement_timeout + 120s API timeout. |
| 26 | Cache seeds stale (contain old responses with em-dashes) | Open -- need regeneration with updated prompt |
| 27 | ANTHROPIC_API_KEY not in GitHub secrets | Open -- needed for adversarial workflow |
| 28 | 31 broken/empty raw files in data/raw/ (7 empty, 2 HTML/WAF) | Identified, not deleted. Cleanup when ready. |
| 29 | 11 duplicate _v2 raw file pairs (~51.5MB) | Identified, not deleted. Safe to remove _v2 copies. |

---

## 21. What Success Looks Like

**Now (March 2026):** 750+ views, 400M+ rows, 4.9 GB, ~345 endpoints across 40+ route files, 10 engines, 20 ontology domains with 28 relationship edges, Intelligence with SSE + DuckDB + RAG + web search + Skillbook v2 (CRUSP lifecycle, score decay, graph expansion, trace storage) + programmatic DOGE/IL/territory enforcement + FL Medicaid context (rule corrected), 28 standalone modules (15 core + 13 research), CPRA regulatory-correct both modes. 115+ ETL scripts. Export pipeline: DOCX/PDF/Excel/CSV + chart PNG/SVG. Demo mode with 27 pre-cached Intelligence responses. @safe_route on all ~345 endpoints. 7-agent adversarial suite (all built) with adversarial-to-Skillbook closed feedback loop. GitHub Actions weekly adversarial workflow.

**Session 34 (2026-03-18) — Intelligence hardening + adversarial completion + FL rule correction:**
- Intelligence fixes: programmatic DOGE quarantine injection (code-level, not just prompt), IL T-MSIS caveat injection, territory-aware fallback (Guam/PR/VI), em-dash post-processing, DuckDB 30s statement_timeout, Anthropic API 120s timeout.
- Adversarial suite completed: 7/7 agents built (was 4/7). Florida Rate agent (4 SQL + 7 Intelligence tests), Skillbook agent (5 poisoning + 2 compounding + 4 integrity), Browser agent (8 Playwright UI tests).
- known_facts.json: 28 ground-truth anchor facts across 11 domains for consistency validation.
- Adversarial-to-Skillbook pipeline: skillbook_import.py converts test failures to learnable skills. Closes the loop: adversarial tests find weaknesses, skills are created, Intelligence improves.
- GitHub Actions adversarial workflow: weekly scheduled run + auto-import to Skillbook + issue creation on failure.
- Skillbook API: added /api/skillbook/recent and /api/skillbook/add endpoints.
- **FL "mutual exclusion rule" corrected.** The rule was fabricated by a prior Claude session. AHCA-published data shows 3 codes (46924, 91124, 91125) legitimately carry both facility and PC/TC rates. Fixed across 13+ files.
- Known issues audit: 8 issues resolved (CI secrets, Clerk keys, fee schedules, error handling, Intelligence bugs). 2 new open (cache seeds stale, ANTHROPIC_API_KEY not in GitHub).
- Cross-dataset enrichment: universal state context endpoint (/api/state-context/{state_code}, 12 queries, 1hr cache) + StateContextBar component deployed across all 12 modules. Every module shows FMAP, enrollment, HPSAs, quality, rates, workforce, HCBS, CMS-64, T-MSIS alongside its domain data.
- Rates & Compliance redesign: new Rate Browse & Compare tool (RateBrowse.tsx, 1,230 lines) with Dashboard, Code Lookup, State Compare views. Replaced 5 overlapping tools. Backend: /api/rates/state-summary + /api/rates/compare-states + /api/rates/context/{state}.
- Shared frontend infrastructure: formatContext.ts (format helpers), StateContextData type, useStateContext hook.
- Production hardening: DuckDB memory config (900MB limit, 2 threads, object cache), security headers (5 OWASP headers on all responses), Gunicorn (2 workers, preload, max-requests), rate limiting (15 queries/min/user), health probes (/healthz, /ready, /startup), JSON structured logging, request timing middleware.
- System dynamics engine: stock-flow ODE modeling (scipy.integrate.solve_ivp). 4 individual models (enrollment, provider participation, workforce, HCBS) + 1 integrated model with 12 stocks and 6 cross-domain feedback loops. Lake-calibrated parameters. Policy Simulator standalone module at /#/policy-simulator with intervention builder, 5 presets, baseline vs scenario comparison. 4 embedded DynamicsWidget instances in CaseloadForecaster, WageAdequacy, HcbsTracker, RateBrowse.
- Supply chain security: Dependabot for npm/pip/GitHub Actions, pip-audit + npm audit in CI, Schemathesis API contract testing.
- 5 Architecture Decision Records: DuckDB, Parquet, Skillbook, partitioning, auth.
- Legacy cleanup: api/chat.js deprecated. Raw file audit: 31 broken files + 11 duplicate pairs identified.

**Session 32 (2026-03-17) — Post-review fixes + adversarial testing framework:**
- @safe_route on all 336/336 endpoints (was 176). safe_route updated to re-raise HTTPException.
- Created validation API (server/routes/validation.py: 3 endpoints) + CLI runner (scripts/run_validation.py).
- Build doc (ARADUNE_FULL_BUILD.md) fully reconciled: architecture diagram, Section 15, route table (39 files), Skillbook subsection, auth references, 51/54 clarified, open items cleaned.
- Adversarial testing framework built: 4 agents (Intelligence, API Fuzzer, Consistency, Persona) in scripts/adversarial/. API fuzzer: 100% pass. Consistency: 85.7%.
- Intelligence system prompt overhauled: dash elimination (em/en/double-hyphen all banned), data vintage enforcement, per-state mandatory caveats (IL T-MSIS, HCRIS, TN, territories), strengthened DOGE quarantine, AI filler phrases banned.
- 3 more agents designed (Florida Rate, Skillbook, Browser). Implementation guide: docs/ADVERSARIAL_TESTING_IMPL.md.

**Session 30 (2026-03-15 through 2026-03-17) — Research audit + fee schedule expansion + data ingestion:**
- Full 8-prompt research audit (V1 + V2): 25 bugs fixed, all 46 endpoints pass, 10/10 data accuracy checks pass.
- NARRATIVE CHANGE: Rate-quality p=0.044 (was 0.178). SVI multicollinearity fixed. Sensitivity note: depends on N=41 (AK/CT COALESCE'd).
- Corrected numbers: pharmacy $3.15B, MCO $120B, quality -1.2pp/yr, MAT $1.16B, Cohen's d=0.50.
- 7 advanced methods: IV/2SLS, VIF, PSM (10,737 pairs), CHOW (4,952 transfers), RF (CV R²=0.622), quantile regression, K-means (26 desert states).
- FEE SCHEDULE COMPLETION: All 51 jurisdictions now have official published fee schedule data. 17 new state fee schedule tables scraped (CA 11.4K, TX 10.2K, NY 7.1K, VA 17.5K, KS 21.3K, WI 8.2K, NJ 22K, IA 10.4K, IL 10.6K, MT 9K, OR 12.4K, WA 8.8K, OH 8.8K, NC 8.1K, ND 9.1K, PA 2.3K, VT 7.8K). TN excluded (94% MC, no FFS).
- rate_comparison_v2: 483,154 rows, 54 states. Published rates 88%, CF×RVU 11%, T-MSIS 1.1%.
- T-MSIS Calibration module: claims vs fee schedule analysis, TN simulated rates, state-level discount factors.
- MEPS Expenditure module: 22,431 respondents, Medicaid vs private vs uninsured spending/utilization.
- Data ingestion: ADI (240K block groups), AHRQ SDOH (44K county-years, 14 years), FMAP FY2011-2023 (663 rows), MCPAR (300 PDF reports), MEPS HC-243, expansion dates in dim_state (41 expanded, 10 not).
- Deep data dive: dental HPSA (43K), MH HPSA (38K), MUA/MUP (20K), FQHC sites (19K), food access atlas (72K tracts), FDA Orange Book (48K products + 21K patents), NHE 30-year Medicaid/Medicare/private spending series (1991-2020), HUD housing data.
- State Profile SDOH section: new collapsible section with ADI, food deserts, dental/MH HPSAs, MUA/MUP per state. Backend /api/sdoh/{state_code}.
- Intelligence: repetition detection guard (truncates looping model output). Chat colors: Aradune=green, You=orange.
- Ontology updated: 722 tables across 20 domains. Regenerated system prompt (33.7K chars).
- About page rewritten: origin story, 750+ tables, 90+ sources, research overview, roadmap. No emdashes.
- UI: nav 10→5, chat box dark green, module grid 3-column flat, architecture visual full-width. Data lake pipeline: Ingest→Normalize→Query. Infrastructure box blue to match.
- Homepage: "54 jurisdictions" (not "50 states"), official data source names, 750+ tables everywhere, footer links to About + Data Catalog.
- Doc consolidation: 12 archived, 8 audit reports organized, ARADUNE-COMPLETE-REFERENCE.md (1,030 lines).
- 13 research modules (was 10): + T-MSIS Calibration + MEPS Expenditure + Network Adequacy.
- Rate Explorer tool: search any HCPCS code, see rates across all 54 jurisdictions. Registered in Rates & Compliance nav.
- CDC Natality 2023: 3.6M births parsed. 41.1% Medicaid. State-level payer mix + national clinical outcomes by payer.
- Compliance Countdown: days to July 2026 CPRA deadline with linked tools per subsection.
- Skillbook (self-improving Intelligence): 24 seed skills from audit findings + DOGE quarantine + FL rules + query patterns. Async Haiku reflector learns from every query. Thumbs up/down feedback buttons. server/engines/skillbook.py + reflector.py.
- Clerk auth: test keys active (pk_test/sk_test). ClerkProvider scaffolding activated. Switch to live keys before demo.
- Error handling: @safe_route decorator on ALL 336 endpoints across 39 route files. Re-raises HTTPException (400/404/413 pass through), catches all other exceptions -> graceful JSON 200. Validator engine + /api/validation/latest, /results, /domains.
- Architecture one-pager: docs/architecture-summary.md (for Big 5 demo).
- Smoke test script: scripts/smoke_test_endpoints.py (331 endpoints, multi-variant testing).
- MCPAR deeper extraction: 300 reports with 21 columns (appeals, grievances, program types, overpayment standards).
- Pharmacy enhancements: generic opportunity endpoint (Orange Book join), patent search endpoint.
- Workforce enhancements: comprehensive access designations endpoint (PC + dental + MH HPSAs + MUA/MUP + FQHCs).
- R2 synced. Vercel + Fly.io deployed. Clerk keys set.
- 750+ lake tables (was 697). 55+ new fact tables. ~55 commits pushed.

**Session 29 (2026-03-14) — Full forensic audit + research integration:**
- 8-prompt forensic audit completed (ARADUNE_AUDIT_GUIDE.md): data integrity sweep, Gold table spot-check, DOGE quarantine, ontology graph, tool functional audit, AHEAD validation, Intelligence regression (30 queries, 22 pass), end-to-end workflow smoke test (6/6 pass).
- 30+ bugs fixed: 17 critical ETL bugs, 18 stale snapshot cleanups, SDUD schema standardization, HCRIS CHOW dedup, FMAP dynamic header detection, Medicare PUF Socrata discovery, scorecard explicit columns, CCW validation, eligibility API pagination.
- AHEAD calculator architectural rework: 3-year 10/30/60 CMS baseline (was single-year + synthetic growth), ±2% volume corridor, commercial payer engine (PY2+), TIA PY1/PY2 limited, TCOC PY4 upside-only.
- DOGE quarantine: 3-layer controls (Intelligence system prompt, ontology QUARANTINE tags, ETL docstring). OT-only, provider state, MC distortion, Nov/Dec 2024 incomplete.
- Ontology overhaul: 28 relationship edges, entity blast-lists pruned (5 entities: 107→10-19 tables each), MCO entity connected, 680 tables covered (was 632).
- 10 research modules built and integrated as academic briefs.
- UX: search parser (67 synonyms), ExplainButton, QuickStart landing cards, AHEAD overview + interpretation guide.
- Open Payments: $2.2B → $10.83B (all 3 CMS categories).
- R2 fully synced: 826 files, ~5GB.
- Intelligence: FL Medicaid context (SMMC 3.0/9 regions, MPIP, rate stacking), Tier 4 → Opus, Tier 3 max_queries 8→12.
- Wage adequacy: fee schedule rates preferred over T-MSIS per-claim averages.
- 16 commits deployed. All live at aradune.co + aradune-api.fly.dev.

**Previous session 28 fixes:** CMS-64 FY2016→FY2024, MACPAC footnote cleanup, opioid FIPS→state codes, SDUD XX filter, R2 sync --remote flag, 14 crash risks fixed, data accuracy verified across 5 states, mobile-responsive across all tools.

**Demo milestone (~April 2026):** End-to-end demo flow tested. Import → cross-reference → export polished. Visual polish pass. Demo walkthrough script written.

**3-6 months:** First external CPRA user. Revenue conversation.
**6-12 months:** Early warning dashboard. Forecast accuracy published. Revenue covers infra.
**1-3 years:** Default Medicaid reference. CMS links to it. Seven figures.

---

*The data is the moat. Intelligence is the interface. Compliance is the wedge. Build in that order.*
