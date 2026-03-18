# CLAUDE.md — Aradune
> **The ONE source for Medicaid data intelligence.**
> Read this file at the start of every session. It defines what Aradune is, how it's built, and the rules for building it.
> Last updated: 2026-03-10 (session 12) · Live: https://www.aradune.co
> Session history moved to CHANGELOG.md.

---

## 1. What Aradune Is

Aradune (aradune.co) is a **Medicaid data intelligence platform** — the Bloomberg Terminal of Medicaid. It ingests, normalizes, and cross-references every available public Medicaid dataset into a single queryable infrastructure, then layers Claude-powered analytics on top so that **anyone working with Medicaid** — state agencies, consultants, MCOs, hospitals, providers, researchers, journalists, advocates, legislators — can find answers that currently require million-dollar consulting engagements or months of manual data assembly.

Aradune is **not** a CPRA compliance tool that also does other things. It is **the platform where all Medicaid data lives**, and CPRA compliance, AHEAD modeling, fee schedule comparison, caseload forecasting, fraud detection, and policy intelligence are all *applications* that sit on top of that data layer.

**Core identity:**
- **Data layer is the moat.** Curated, normalized, cross-referenced public Medicaid data no one else has assembled.
- **AI-native from day one.** Claude is the primary interaction model for complex work — not a dashboard with AI bolted on.
- **Anyone working with Medicaid.** The free tier should be genuinely useful; the paid tier indispensable.
- **Bring your own data.** Users upload their own data and connect it to Aradune's normalized public layer.

Named after Brad McQuaid's EverQuest paladin character. Domain: aradune.co.

---

## 2. System Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                          INTERFACE LAYER                                │
│   Free: Dashboards · Lookups · Rate Builder · State Profiles           │
│   Paid: Claude Policy Analyst · Structured Reports · API Access        │
│   Institutional: White-Label · Custom Integrations · DaaS Licensing    │
│   [ USER DATA UPLOAD / CONNECT ]                                        │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
┌─────────────────────────────────────────────────────────────────────────┐
│                     CLAUDE-NATIVE ANALYTICS ENGINE                      │
│   NL2SQL · RAG over policy corpus · Cross-state comparisons            │
│   Rate adequacy · Caseload & expenditure forecasting                   │
│   AHEAD modeling · SPA impact · MCO gap · ML model tracking            │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
┌─────────────────────────────────────────────────────────────────────────┐
│                        UNIFIED DATA LAYER                               │
│   Normalized schema · Versioned · Timestamped · Source-provenant       │
│   Hive-partitioned Parquet on S3 · pgvector for RAG corpus             │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
┌─────────────────────────────────────────────────────────────────────────┐
│                     DATA INGESTION PIPELINES                            │
│   Dagster orchestration · Per-source ETL (fetch→parse→validate→load)  │
│   Change detection · Versioned snapshots · Rollback · Pause/resume     │
│   PDF extraction (pdfplumber → Claude API) · Notification system       │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
┌─────────────────────────────────────────────────────────────────────────┐
│                          RAW DATA UNIVERSE                              │
│   MEDICAID: T-MSIS/TAF · Fee Schedules (51 states) · CMS-64           │
│   MANAGED CARE: MCO contracts · Rate certifications · MLR reports      │
│   HOSPITALS: HCRIS · DSH/UPL · Price transparency MRFs                │
│   POLICY: SPAs · Waivers · CIBs · SHO letters · Federal Register      │
│   ECONOMIC: BLS wages/CPI/unemployment · FRED · ACS · SNAP/TANF       │
│   QUALITY: Medicaid Scorecard · HEDIS · Core Sets · DQ Atlas          │
│   PHARMACY: NADAC · SDUD · State MAC prices                           │
│   USER-UPLOADED: Any data the user brings to the platform              │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## 3. Current Stack

```
Frontend:       React 18 + TypeScript + Vite (Vercel Pro, aradune.co)
Visualization:  Recharts
Routing:        Hash-based in Platform.tsx
Data store:     DuckDB-WASM (browser-side client queries)
Data lake:      Hive-partitioned Parquet (data/lake/) — 115M+ rows, 270 views (250 fact + 9 dim + 9 ref + 2 compat)
                DuckDB in-memory views over Parquet files, 785MB on disk
                S3/R2 sync (scripts/sync_lake.py, Cloudflare R2 bucket: aradune-datalake)
Backend:        Python FastAPI (server/) — 258+ endpoints across 22 route files, DuckDB-backed
                3 Vercel serverless functions in api/ (legacy)
AI:             Intelligence (server/routes/intelligence.py) — Claude Sonnet 4.6 + SSE streaming
                + extended thinking + DuckDB tools + RAG policy corpus + web search
                Haiku for routing · Sonnet for analysis · Opus for complex reasoning
RAG:            DuckDB FTS over policy corpus (1,039 docs, 6,058 chunks from medicaid.gov)
                BM25 full-text search with ILIKE fallback (server/engines/rag_engine.py)
Search:         Platform-wide Cmd+K search (PlatformSearch.tsx + /api/search)
Auth:           Clerk integration (ClerkProvider.tsx + server/middleware/auth.py)
                Falls back to password gate ("mediquiad") when Clerk not configured
Pipeline:       Python build scripts (scripts/build_*.py) — lake ETL
                Python (cpra_engine.py) — CPRA/DuckDB analytical layer
                R (tmsis_pipeline_duckdb.R) — T-MSIS processing
Orchestration:  Dagster (pipeline/dagster_pipeline.py) — 13 assets, 3 checks, 3 jobs, 2 schedules
CI/CD:          GitHub Actions (.github/workflows/ci.yml) — TypeScript check + Vercel + Fly.io deploy
Deployment:     Vercel (frontend) · Fly.io (FastAPI, server/fly.toml + Dockerfile)
                Lazy DuckDB view loading + /health endpoint for fast cold starts
Vector store:   DuckDB vss extension ready (optional Voyage-3-large embeddings for hybrid search)
NL2SQL:         Claude Sonnet via Anthropic SDK — schema-in-prompt, SELECT-only validation
Design:         #0A2540 ink · #2E6B4A brand · #C4590A accent
                SF Mono for numbers · Helvetica Neue for body · No Google Fonts
Access:         Clerk auth (when VITE_CLERK_PUBLISHABLE_KEY set) OR password gate ("mediquiad")
```

---

## 4. Live Tools (18 total)

**Site is behind a password gate** (`PasswordGate` component in `Platform.tsx`). Password: `mediquiad`. Stored in `sessionStorage` — clears on tab close. All tools are lazy-loaded and code-split per route.

| Group | Tool | Route | Status |
|-------|------|-------|--------|
| **Home** | **Aradune Intelligence** | **`/#/`** | **live — SSE streaming chat, RAG + DuckDB tools, contextual file upload** |
| **Explore** | **Data Explorer** | **`/#/ask`** | **live — NL2SQL via Claude Sonnet** |
| **Explore** | **Data Catalog** | **`/#/catalog`** | **live — browsable table index** |
| **Explore** | **State Profile** | **`/#/state` or `/#/state/{code}`** | **live — 20 parallel API fetches, 7 sections, cross-dataset insights** |
| Explore | Spending Explorer | `/#/explorer` | live |
| Explore | Medicare Comparison | `/#/decay` | live |
| Explore | State Fee Schedule Directory | `/#/fees` | live |
| Explore | Rate Lookup | `/#/lookup` | live |
| Explore | Compliance Report | `/#/compliance` | live |
| **Explore** | **CPRA Generator** | **`/#/cpra`** | **live — wedge product** |
| Analyze | Rate & Wage Comparison | `/#/wages` | live |
| Analyze | Quality Linkage | `/#/quality` | live |
| Analyze | Rate Reduction Analyzer | `/#/reduction` | live |
| Analyze | HCBS Compensation Tracker | `/#/hcbs8020` | live |
| Build | Rate Builder | `/#/builder` | live |
| Build | AHEAD Calculator | `/#/ahead` | live |
| Build | AHEAD Readiness Score | `/#/ahead-readiness` | live |
| **Build** | **Caseload Forecaster** | **`/#/forecast`** | **live — caseload + expenditure** |
| Build | Policy Analyst | `/#/analyst` | beta — has Bearer token auth |

**Target nav structure:**
```
Aradune  [⌕ search]  Explore▾  Analyze▾  Build▾  About
```
See `docs/AraduneMockup.jsx` as the definitive landing page + nav reference.

---

## 5. CPRA — Two Separate Systems

### 5a. CPRA Frontend (Pre-Computed Cross-State Comparison)

`src/tools/CpraGenerator.tsx` (734 lines). Pre-computed rate comparisons from `fact_rate_comparison` (302K rows, 45 states, all HCPCS codes).

**Data flow:** `fact_rate_comparison` (lake) → `cpra_em.json` (2,742 rows/34 states) → frontend. Also: `dim_447_codes.json`, `cpra_summary.json`, `dq_flags_em.json`.

**Export:** PDF (`cpraPdf.ts`) · Excel (`cpraXlsx.ts`) · HTML.

**API:** `GET /api/cpra/states`, `/api/cpra/rates/{state_code}`, `/api/cpra/dq/{state_code}`, `/api/cpra/compare`

### 5b. CPRA Upload Tool (42 CFR 447.203 Compliance Generator)

`server/engines/cpra_upload.py` (821 lines). User uploads two CSVs, gets full CPRA in <2 seconds. **This is the regulatory-correct implementation.**

| Aspect | Pre-Computed (5a) | Upload Tool (5b) |
|--------|-------------------|-------------------|
| E/M codes | 74 (old list) | **68** (official CMS CY 2025) |
| Code-category mapping | 1:1 | **Many-to-many** (171 pairs) |
| Conversion factor | $33.4009 (CY2026) | **$32.3465** (CY2025) |
| Medicare rates | State-level averages | **Per-locality** |
| Data source | Pre-computed lake table | **User-uploaded** |

**Reference data** (`data/reference/cpra/`): `em_codes.csv` (68 codes), `code_categories.csv` (171 rows), `GPCI2025.csv` (109 localities).

**API:** `GET /api/cpra/upload/states`, `/upload/codes`, `/upload/templates/fee-schedule`, `/upload/templates/utilization`, `POST /upload/generate`, `/upload/generate/csv`, `/upload/generate/report`

**Frontend:** CpraGenerator.tsx — "Cross-State Comparison" (default) + "Bring Your Own Data" tabs.

### CPRA Compliance Rules (always enforce)

- **68 codes** from the official CMS CY 2025 E/M Code List — not 74
- **$32.3465** CY 2025 CF (non-QPP) — not $33.4009 (that's CY2026)
- **Many-to-many categories** — a code can appear in multiple categories
- Base rates only — do not include supplemental payments in the % calculation
- Non-facility Medicare rate is the benchmark (not facility), per 42 CFR 447.203
- Medicaid rates effective July 1, 2025 vs CY2025 Medicare PFS
- Published by July 1, 2026; updated biennially thereafter
- Small cell suppression: beneficiary counts 1-10 suppressed

---

## 6. Terminal B — CPRA Data Pipeline

**Primary file:** `tools/mfs_scraper/cpra_engine.py` (~48KB, 968 lines)

**Single-command pipeline:**
```bash
python cpra_engine.py --all --cpra-em --output-dir ../public/data/
```

**Individual steps:** `--init` (DuckDB schema + ETL) → `--em-codes` (load 74 E/M codes) → `--medicare-rates` (858K locality rates) → `--cpra` (match Medicaid→Medicare, 242K rows) → `--dq` (11 rules, 258K flags) → `--export` (7 output files) → `--cpra-em` (slim E/M extract) → `--stats` (print counts)

**DuckDB (`aradune_cpra.duckdb`) — 1.87M rows:** dim_state (51), dim_procedure (16,978), dim_medicare_locality (109), dim_em_447_codes (74), xwalk_locality_to_state (109), fact_medicare_rate (858,593), fact_medicare_rate_state (417,481), fact_rate_comparison (302,332), fact_dq_flags (269,475).

**45 states in rate_comparison (40 with E/M in cpra_em.json):** AK AL AR AZ CA CO CT DC DE FL GA HI ID IL IN KY LA MA MD ME MI MN MO MS MT NC ND NE NH NM NV NY OH OK OR PA RI SC SD TX UT VA WA WV WY

**CPRA summary:** Median 84.8% of Medicare. PC E/M avg 81.4%, MH/SUD 99.6%, OB/GYN 132.9%.

---

## 7. Known Bugs & Issues

### Open

| # | Bug | Location | Status |
|---|-----|----------|--------|
| 9 | **R2 credentials need rotation** | Infrastructure | Shared in plain text during session. |
| 10 | **db.py fact_names must match filesystem** | `server/db.py` | Only facts in `fact_names` (line 41-148) are registered as views. Always update when adding lake tables. Currently 250 entries. |
| 11 | **Fly.io cold start slow** | Infrastructure | S3 sync downloads 270+ files on startup (~60s). Consider pre-baking lake into Docker image or using persistent volumes. |

### Data Quality — Investigated

All outlier states investigated. Root causes in `public/data/dq_state_notes.json` (42 states, 11 flagged). Key: SD (per-15-min unit rates, 3%), CT/KY (bundled facility, 106%), RI (bundled facility, 278%), DC (45K+ codes, 28%).

### Minor / Cleanup

| Item | Action |
|------|--------|
| `scripts/build-cpra-data.mjs` | Delete — superseded by cpra_engine.py |
| Locality weighting is equal, not population-weighted | Acceptable for v1; fix with Census CBSA data later |
| `StateRateEngine.js` | 1,153 lines, 42/42 tests passing, wired to Rate Builder (Engine Analysis card) |
| Password gate is client-side only | `sessionStorage` check in Platform.tsx — not a security boundary |

See CHANGELOG.md for all resolved bugs (#1-8, #12-18).

**#18 State Profile field mismatches (RESOLVED session 12):** Demographics API returns `pct_poverty`/`pct_uninsured` (already percentages, not decimals); FMAP returns `fmap_rate` not `fmap`; CPRA rates use `procedure_code`/`em_category` not `cpt_hcpcs_code`/`category_447`; hospital `medicaid_day_pct` is already a percentage; scorecard uses `measure_value`/`median_value`. All fixed. Region now fetched from `/api/states`.

---

## 8. Immediate Next Steps

### Tier 1 — Ship-blocking: ALL DONE
All 4 items completed. See CHANGELOG.md for details.

### Tier 2 — Platform completeness

**Done:** Nav redesign, DQ state notes, Bar cap, FL methodology addendum, CPRA upload frontend + Fly.io deploy, cpra_engine.py update, Caseload Forecaster frontend, Expenditure modeling engine + API + frontend.

**Still open:**
1. ~~**Landing page redesign**~~ — Done (session 10-12). Hero, stats bar, tabbed start card, workflow steps, Intelligence section, state profiles grid, Ensuring Access callout.
2. **Platform architecture redesign** — Consolidate 18 tools into 6 workflow-based modules. See `docs/NEW_ARADUNE_BUILD_STRUCTURE.md`. User reviewing direction.
3. ~~**Wire `StateRateEngine.js` into Rate Builder**~~ — Done. Engine Analysis card with FL Tier 3 engine, multi-state CF comparison, implied CF reverse-engineering.
4. ~~**Wire Intelligence endpoint to frontend**~~ — Done (session 10). IntelligenceChat.tsx at `/#/`, SSE streaming, contextual file upload, 3 starter prompts.

### Tier 2b — Critical platform gaps

**Done:** State Profile pages (fixed session 12), Data Catalog, Landing page update, Export for all tools, NL2SQL search, Intelligence wired to frontend, RAG over policy corpus.
**Still open:** User accounts (F) — Can't monetize with shared password gate. Need email + magic link auth, saved workspaces, usage tracking.

### Tier 3 — Data expansion (standing instruction)

The data layer is the moat. Every session: add data, improve quality, or make adding data easier.

**Current:** 250 fact tables, 115M+ rows, 80+ federal sources. Full list in `server/db.py` lines 41-148.

**Highest-value datasets not yet ingested:**

| # | Dataset | Source | Status |
|---|---------|--------|--------|
| 1 | **Hospital price transparency MRFs** | CMS MRF index | Not started — massive, requires targeted extraction |
| 2 | **340B covered entity data** | hrsa.gov | Blocked — Blazor app needs browser automation |
| 3 | **SPA/waiver policy corpus** | CMS MACPro / medicaid.gov | Partial — 1,039 CMS guidance docs ingested (CIBs/SHOs/SMDs/guidance), 6,058 chunks. Waivers not yet scraped. |
| 4 | **MCO contract terms** | State portals | Not started |
| 5 | **More state fee schedules** | State portals | 4 remaining: KS/NJ (login), TN (MC only), WI (manual) |
| 6 | **UPL demonstrations** | CMS MACPro | Not started |
| 7 | **Full SDP preprint parsing** | CMS | Index done (34 states), parsing not started |
| 8 | **Historical HCRIS (FY2021-2022)** | cms.gov | Not started — enables 3-year trend sparklines |

**Improve existing data:**
- Add `weighted_avg_pct` to `cpra_summary.json` using CY2023 FFS claim volume weights
- Add category-level breakdowns to `cpra_summary.json` per state
- Build reusable ingestion pattern (fetch→parse→validate→normalize→load)

### Tier 4 — Analytical features

**Done:** Caseload forecasting, Expenditure modeling, Scenario builder, NL2SQL, RAG over policy corpus, Cross-dataset insights.

**Still open:**
| # | Feature | Status |
|---|---------|--------|
| 5 | ~~**RAG over policy corpus**~~ | Done (session 11). DuckDB FTS + BM25 over 1,039 docs / 6,058 chunks from medicaid.gov (CIBs, SHOs, SMDs, guidance). `server/engines/rag_engine.py`. |
| 6 | **Forecast accuracy dashboard** | Not started — log predictions, compare to actuals, publish accuracy. |
| 7 | ~~**Cross-dataset insights**~~ | Done (session 10). Server-side `/api/insights/{state}` + client-side `computeInsights()` in StateProfile. Merged, deduped, capped at 7. |

---

## 9. File Map

```
Aradune/
├── CLAUDE.md                        ← THIS FILE
├── CHANGELOG.md                     ← Session history, resolved issues, completed milestones
├── README.md / SETUP.md
├── package.json / vite.config.ts / tsconfig.json / vercel.json / index.html
│
├── src/
│   ├── Platform.tsx                 ← ~980 lines. Main router, tool registry, landing page,
│   │                                   ToolErrorBoundary, PasswordGate ("mediquiad")
│   ├── design.ts                    ← Design tokens (C, FONT, SHADOW)
│   ├── tools/                       ← 19 tool components (see Section 4 for routes)
│   │   ├── IntelligenceChat.tsx     ← ~850 lines. AI chat home (/#/), SSE, contextual file upload
│   │   ├── TmsisExplorer.tsx        ← ~2,400 lines. Spending Explorer.
│   │   ├── CpraGenerator.tsx        ← 734 lines. CPRA wedge product.
│   │   ├── CaseloadForecaster.tsx   ← ~830 lines. Caseload + expenditure (#/forecast)
│   │   ├── StateProfile.tsx         ← ~1,000 lines. State Profile (#/state/{code}), 20 API fetches
│   │   ├── PolicyAnalyst.tsx        ← 378 lines. AI chat (#/analyst)
│   │   └── [13 more — see Section 4]
│   ├── engine/
│   │   └── StateRateEngine.js       ← 1,153 lines. 42/42 tests passing. Wired to Rate Builder.
│   ├── utils/                       ← cpraPdf.ts, cpraXlsx.ts, pdfReport.ts, aheadScoring.ts
│   ├── lib/                         ← api.ts, duckdb.ts, queryEngine.ts
│   └── data/states.ts               ← STATE_NAMES, STATES_LIST
│
├── public/data/                     ← All JSON/Parquet served to frontend
│   ├── cpra_em.json (615KB), dq_flags_em.json (79KB), dim_447_codes.json (14KB), cpra_summary.json (7KB)
│   ├── hcpcs.json, states.json, trends.json, medicare_rates.json, fee_schedules.json
│   ├── bls_wages.json, quality_measures.json, soc_hcpcs_crosswalk.json, conversion_factors.json
│   ├── system_prompt.md, fl_methodology_addendum.md
│   └── [external] claims_monthly.parquet (82MB) via VITE_MONTHLY_PARQUET_URL
│
├── api/chat.js                      ← 515 lines. Vercel serverless.
│
├── server/                          ← FastAPI backend (DuckDB over lake Parquet)
│   ├── main.py / db.py / config.py / query_builder.py
│   ├── Dockerfile / fly.toml / entrypoint.sh
│   ├── engines/
│   │   ├── cpra_upload.py           ← 821 lines. CPRA upload engine
│   │   ├── caseload_forecast.py     ← ~650 lines. SARIMAX+ETS forecasting
│   │   ├── expenditure_model.py     ← ~430 lines. Expenditure projection
│   │   └── rag_engine.py            ← ~460 lines. BM25 + vector hybrid search over policy corpus
│   └── routes/                      ← 22 route files, 258+ endpoints
│       ├── cpra.py                  ← Pre-computed + upload CPRA routes
│       ├── lake.py                  ← /api/states, enrollment, quality, expenditure
│       ├── nl2sql.py                ← /api/nl2sql — NL2SQL for Data Explorer
│       ├── intelligence.py          ← Claude Sonnet + SSE + DuckDB tools + RAG — wired to IntelligenceChat
│       ├── forecast.py              ← 10 endpoints: caseload + expenditure pipeline
│       └── [17 more: query, meta, presets, pharmacy, policy, wages, hospitals,
│            enrollment, staffing, quality, context, bulk, supplemental,
│            behavioral_health, round9, insights, corpus]
│
├── tools/mfs_scraper/
│   ├── cpra_engine.py               ← 968 lines. Terminal B CPRA pipeline.
│   ├── cms_data.py / ncci_scraper.py / export_data.py / db_import.py
│   └── aradune_cpra.duckdb          ← 1.79M rows. Analytical layer.
│
├── pipeline/
│   ├── dagster_pipeline.py          ← 6 assets, 3 checks, 3 jobs, 2 schedules
│   └── tmsis_pipeline_duckdb.R      ← 71KB. T-MSIS processing (227M rows).
│
├── data/
│   ├── lake/                        ← 115M+ rows, 250 fact + 9 dim + 9 ref tables, 785MB
│   ├── reference/cpra/              ← em_codes.csv, code_categories.csv, GPCI2025.csv
│   └── raw/                         ← T-MSIS DuckDB (17.5GB), NPPES (11.2GB), fee schedules, BLS, etc.
│
├── scripts/                         ← 30+ build_lake_*.py scripts, build_lake_policy_corpus.py, sync_lake.py
│
├── .github/workflows/ci.yml        ← Build, lint, deploy to Vercel + Fly.io
│
└── docs/
    ├── ARADUNE_MASTER.md            ← Full strategy reference
    ├── TMSIS_DATA_GUIDE.md          ← T-MSIS operational guide
    ├── UX_FEATURES_SPEC.md          ← Implementation spec
    ├── NEW_ARADUNE_BUILD_STRUCTURE.md ← Architecture redesign proposal (session 9)
    └── AraduneMockup.jsx            ← Definitive landing page + nav reference
```

### Brand Assets

All transparent PNGs — green on transparent. `logo-full.png` (navbar), `logo-mark.png` (favicon), `logo-wordmark.png` (PDF headers), `icon-bot.png` (chat avatar). All in `public/assets/`.

**Brand colors:** `#0A2540` ink · `#2E6B4A` brand green · `#C4590A` accent · `#F5F7F5` surface.

## 10. Known Policy Rules (Always Enforce)

- **FL Medicaid: Facility and PC/TC rates are typically mutually exclusive (99.96% of codes).** Three codes (**46924, 91124, 91125**) legitimately carry both facility and PC/TC rates as published by AHCA.
- **FL production conversion factors:** Regular `$24.9779582769` · Lab `$26.1689186096`. The ad hoc CF of $24.9876 is stale — do not use for CY2026.
- **FL has 8 schedule types** in the fee schedule.
- **Medicare comparison baseline:** Always use the non-facility rate (not facility), per 42 CFR 447.203.
- **Medicare conversion factors:** `$33.4009` (CY2026, non-QPP) for general fee-to-Medicare comparison. `$32.3465` (CY2025, non-QPP) for CPRA compliance (July 2026 deadline compares CY2025 rates). Both are correct for their respective uses.
- **CPRA base rates only:** Do not include supplemental payments in the Medicaid-to-Medicare percentage.
- **CHIP excluded** from per-enrollee Medicaid calculations.
- **Minimum cell size:** n >= 11 for any published utilization count.

---

## 11. Data Universe

### Ring 0: Public Regulatory Data (No HIPAA, no DUA — build here first)

| Dataset | Status |
|---------|--------|
| State Medicaid fee schedules (all 51) | **47 states** (597K rows). 4 remaining: KS/NJ (portal login), TN (MC only), WI (manual). |
| Medicare Physician Fee Schedule | **Done** — 16,978 codes, 858K locality rates |
| T-MSIS HHS open data (227M rows) | **Done** — ingested into lake |
| NPPES NPI Registry | **Done** — 11.2GB raw file downloaded |
| CMS-64 expenditure reports | **Done** — fact_expenditure + fact_fmr_supplemental |
| HCRIS hospital cost reports | **Done** — fact_hospital_cost + fact_dsh_hospital (6,103 hospitals) |
| Provider of Services (POS) | **Done** — fact_pos_hospital + fact_pos_other |
| NADAC pharmacy pricing | **Done** |
| SDUD | **Done** — fact_drug_utilization (incl. 2024 + 2025) |
| FMAP rates | **Done** |
| Adult/Child Core Set quality | **Done** — fact_quality_measure |
| MACPAC Exhibits | **Done** — Exhibits 14/16/17/22/24 |
| Medicaid Provider Enrollment | Not started |
| MBES/CBES enrollment/expenditure | Partial — enrollment data ingested |

### AHEAD Readiness Score — HCRIS Field Map

```
Operating/total margin   → net_income / net_patient_revenue (HCRIS Worksheet G)
Current ratio            → total_assets / total_liabilities (HCRIS Worksheet G)
Cost-to-charge ratio     → cost_to_charge_ratio (HCRIS Worksheet D-1/D-4)
Payer mix (days)         → medicare_days, medicaid_days, total_days (HCRIS Worksheet S-3)
Uncompensated care       → uncompensated_care_cost (HCRIS Worksheet S-10)
Medicare DSH / IME       → dsh_adjustment, dsh_pct, ime_payment (HCRIS Worksheet E)
Discharges, bed count    → total_discharges, bed_count (HCRIS Worksheet S-3)
```

**AHEAD data gaps:** Medicaid UPL/SDP not at hospital level. No Maryland peer benchmarks. No Medicaid FFS/MC split in HCRIS. No service line margins. Only FY2023 HCRIS (need historical). No days cash on hand/DSCR/days AR directly.

### Supplemental Payment Programs — PARTIALLY ADDRESSED

**Done:** CMS DSH Allotment, CMS-64 FMR, SDP preprint (34 states), MACPAC supplemental.
**Not started:** UPL demonstrations, HRSA GME payments, OIG DSH audits.
**Partial:** 1115 waiver financial terms (647 waivers, metadata only).

### Category Completion Summary

| Category | Status | Key tables |
|----------|--------|------------|
| Hospital Quality & VBP | **Done** | fact_hospital_rating, fact_hospital_vbp, fact_hospital_hrrp, fact_hac_measure |
| Behavioral Health | **Substantially done** | fact_nsduh_prevalence, fact_block_grant, fact_mh_facility, fact_ipf_quality, fact_bh_by_condition, fact_mh_sud_recipients |
| Children's/CHIP/EPSDT | **Substantially done** | fact_chip_enrollment (+ monthly/annual/unwinding), fact_chip_eligibility, fact_epsdt, fact_well_child_visits, fact_blood_lead_screening, fact_vaccinations |
| LTSS/HCBS | **Partially done** | fact_hcbs_waitlist (51 states), fact_five_star, fact_pbj (65M+), fact_snf_cost, fact_cms372_waiver. Not started: CMS-64 Schedule B, 1915(c) utilization, HCBS quality, DSW workforce, 1915(k) |
| Eligibility & Unwinding | **Done** | fact_unwinding, fact_eligibility_levels |
| Pharmacy | **Mostly done** | fact_nadac, fact_drug_utilization. Blocked: 340B (Blazor app). Not started: State MAC. |
| Economic/Contextual | **Done** | BLS wages, Census ACS, CDC mortality/overdose, FRED, SNAP, TANF, HUD FMR, SAIPE, CDC PLACES |

### Ring 0.5: Economic & Contextual Data

All key datasets ingested: BLS (unemployment, CPI, OEWS), Census/ACS, FRED (GDP, income), SNAP/TANF, HUD FMR, CDC (mortality, overdose, PLACES), SAIPE poverty. Remaining: state revenue/budget data (NASBO).

### Ring 1-3 (HIPAA sensitivity increases)
- **Ring 1:** Aggregated/de-identified — state-published utilization counts, HHS open data
- **Ring 2:** Provider-level — may need BAA; build when state relationships develop
- **Ring 3:** Claims/encounter data — full HIPAA; only when BAA + HITRUST in place

## 12. T-MSIS Data Quality Rules

**Non-negotiable for every feature, pipeline, or analysis that touches T-MSIS data. See also: `docs/TMSIS_DATA_GUIDE.md` for full field-level detail.**

### What the data is and isn't
- **227M rows** = T-MSIS OT (Other Services) file only — physician, outpatient, clinic, HCBS claims
- **Excludes** inpatient (IP), long-term care (LT), pharmacy (RX)
- **No real-time data.** Preliminary TAF: ~12-18 month lag. Final: ~24 months.
- **2024 data is incomplete.** Sharp Nov-Dec dropoff — flag in every output using 2024
- **T-MSIS DuckDB (17.5GB) is currently empty** — R pipeline must run first

### 12 non-negotiable rules

1. **Always specify the service year.** Say "CY2022 T-MSIS data" — never "current."
2. **Check DQ Atlas before using any state.** Flag "Unusable"/"High Concern" states.
3. **Always apply OT claims filters:**
   ```sql
   WHERE MDCD_PD_AMT > 0 AND MDCD_PD_AMT < 50000
   AND ADJSTMT_IND NOT IN ('1', 'V')
   AND CLM_STUS_CTGRY_CD IN ('F1', 'F2', 'F3')
   AND PRCDR_CD IS NOT NULL AND SRVC_BGNG_DT IS NOT NULL
   ```
4. **Validate NPIs:** 10 digits, not null, not '0000000000'. Use `SRVC_PRVDR_NPI` over `BLNG_PRVDR_NPI`.
5. **Separate FFS from encounter claims** using `CLM_TYPE_CD`. Encounter amounts are unreliable.
6. **Surface MCO penetration context** alongside every utilization metric.
7. **Use ASPE HCBS taxonomy** — not `BNFT_TYPE_CD` alone.
8. **Never imply T-MSIS captures MCO-to-provider payment rates.** It does not.
9. **Use SCD Type 2 logic** for fee schedule temporal joins.
10. **Document data vintage in every output** — service year, TAF release, DQ Atlas rating.
11. **Minimum cell size: n >= 11** for any published utilization count.
12. **Do not mix MAX and TAF** in the same time series without a formal crosswalk.

### Per-source data quality gates

**Rate validity:** Flag $0.00 rates, rates >$10K for E/M, rates unchanged 24+ months, rates >2x RVU-derived expected.
**Code coverage:** Verify expected E/M codes, identify missing codes, track CPT additions/deletions.
**Medicare matching:** Validate locality mapping, confirm CF, flag undocumented multi-locality weighting.
**Cross-state:** Flag >3 SD from national mean, compare to KFF/MACPAC benchmarks, flag uniform-CF errors.

---

## 13. Data Ingestion Pipeline

### Core Pattern (every source implements all five steps)
```python
def fetch_raw(source_config) -> bytes | Path:    # HTTP HEAD + ETag for change detection
def parse(raw) -> list[dict]:                     # Per-source; PDF → pdfplumber → Claude
def validate(parsed) -> ValidationResult:         # Hard stops vs. soft flags
def normalize(validated) -> list[dict]:            # Unified schema + URL + download date
def load(normalized, db_conn) -> LoadResult:       # Upsert + version tracking + S3 snapshot
```

**Hard stops:** Rate changed >90% · Code count dropped >20% · Schema mismatch
**Soft flags:** Rate unchanged >24 months · New codes without description · Rate >3 SDs from mean

### Scheduling
| Frequency | Sources |
|-----------|---------|
| Weekly | NPPES, NADAC, Federal Register/CIBs/SHOs |
| Monthly | BLS unemployment/CPI/FRED, MC enrollment |
| Quarterly | T-MSIS/SDUD/MBES-CBES |
| Annual | Medicare PFS RVU, state fee schedules, HCRIS, BLS OEWS, ACS |

---

## 14. Database Schema (Core Tables)

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
    PRIMARY KEY (state_code, cpt_hcpcs_code, modifier, provider_type, population, geographic_area, effective_date)
);

CREATE TABLE fact_rate_comparison (
    state_code VARCHAR(2), cpt_hcpcs_code VARCHAR(10), category_447 VARCHAR,
    population VARCHAR, provider_type VARCHAR, geographic_area VARCHAR, year INTEGER,
    medicaid_rate DECIMAL(10,2), medicare_nonfac_rate DECIMAL(10,2),
    pct_of_medicare DECIMAL(6,4), claim_count INTEGER, beneficiary_count INTEGER,
    PRIMARY KEY (state_code, cpt_hcpcs_code, category_447, population, provider_type, geographic_area, year)
);

CREATE TABLE forecast_enrollment (
    state_code VARCHAR(2), eligibility_group VARCHAR,
    forecast_date DATE, run_date DATE, model_id VARCHAR,
    point_estimate INTEGER, lower_80 INTEGER, upper_80 INTEGER,
    lower_95 INTEGER, upper_95 INTEGER,
    PRIMARY KEY (state_code, eligibility_group, forecast_date, model_id)
);
```

**DuckDB notes:** `DECIMAL(10,2)` for rates (never FLOAT) · `DATE` for dates (never string) · Skip PKs during bulk load · DuckDB-WASM for browser queries <5M rows.

Additional schemas: `dim_procedure`, `economic_indicators`, `model_performance`, `ingestion_log`, `pipeline_alerts` — see previous versions or code.

---

## 15. AI Interface

```
User query
    ├── Pre-computed lookup → DuckDB-WASM + CDN Parquet (sub-second)
    ├── Analytical query   → Server-side DuckDB REST API (seconds)
    └── Policy reasoning   → RAG + Claude pipeline (5–15 seconds)
```

| Query type | Model | Cost/query |
|---|---|---|
| Classification, routing, PDF extraction | claude-haiku-4-5-20251001 | ~$0.004 |
| NL2SQL, RAG, standard analysis | claude-sonnet-4-6 | ~$0.03-0.06 |
| SPA drafting, CPRA narrative, AHEAD | claude-opus-4-6 | ~$0.28 |

Use **prompt caching** (90% input cost reduction) and **batch API** (50% discount for non-real-time).

---

## 16. Caseload & Expenditure Forecasting

### Phase 1 — DONE: Caseload Forecasting
**Engine:** `server/engines/caseload_forecast.py` (~650 lines). SARIMAX + ETS model competition per category with intervention variables. Economic covariate enrichment. Holdout MAPE validation.
**Frontend:** `src/tools/CaseloadForecaster.tsx` (~830 lines) at `/#/forecast`.
**API:** 6 endpoints — templates (caseload + events), generate (JSON + CSV), public enrollment.

### Phase 2 — DONE: Expenditure Modeling
**Engine:** `server/engines/expenditure_model.py` (~430 lines). Takes caseload forecast + user expenditure params CSV. Applies trend, admin load, risk margin, policy adjustments.
**API:** 4 endpoints — template, full pipeline, CSV download, expenditure-only.

### Phase 3 — DONE: Scenario Builder
Third tab in CaseloadForecaster. 4 sliders (unemployment, eligibility, rate change, MC shift) with preset scenarios. Client-side adjustment with baseline vs scenario chart.

### Phase 4 — FUTURE: ML Models + Ensemble
Gradient boosting / random forest with public performance tracking. Model leaderboard. Ensemble forecasts.

---

## 16b. Security & HIPAA Architecture

### Data sensitivity rings (always follow)

| Ring | Data | HIPAA | Current |
|------|------|-------|---------|
| **0** | Public regulatory: fee schedules, RVUs, SPAs | None | Here now |
| **0.5** | Economic/contextual: BLS, FRED, Census | None | Here now |
| **1** | Aggregated/de-identified: DOGE data, state utilization | Minimal | Here now |
| **2** | Provider-level: billing volumes, network data | Low — BAA | When state relationships develop |
| **3** | Claims/encounter data: T-MSIS/TAF, state MMIS | Full HIPAA | Only after BAA + HITRUST |

**Stay in Ring 0/0.5/1 until BAA, SOC 2 Type II, and HITRUST CSF are in place.**

### Technical controls
- **Encryption:** AES-256 at rest, TLS 1.2+ in transit, no plaintext secrets
- **Access:** RBAC, MFA for admin, strict tenant isolation, expiring session tokens
- **Audit:** Immutable logs (6-10 year retention), pipeline runs to `ingestion_log`
- **Secrets:** Env vars only (ANTHROPIC_API_KEY, VITE_MONTHLY_PARQUET_URL). Never commit to git.
- **User data:** Session-scoped by default. Private workspace encrypted, user-only.

### Certifications to pursue (in order)
1. **SOC 2 Type II** — minimum for enterprise sales
2. **HITRUST CSF** — preferred for state contracts
3. **StateRAMP** — some state cloud deployments
4. **FedRAMP** — federal contracts only

### Never do these things
- Never host raw PHI without DUA + HITRUST
- Never build fraud detection before BAA relationships exist
- Never allow Provider A to see Provider B's data without opt-in
- Never use T-MSIS outside ResDAC DUA terms
- Never publish utilization counts below n=11
- Never log user query content that could reveal PHI

---

## 17. Build Principles for Every Session

1. **Always build to the unified schema.** No one-off scripts dumping to random formats.
2. **Validation is not optional.** Every parser validates before loading.
3. **Source provenance is not optional.** Every record traces to URL + download date.
4. **Ship ugly.** Working data for 50 states beats beautiful UI for 5 states.
5. **Coverage > polish.**
6. **Federal data first.** Federal sources cover all states at once.
7. **Florida pipeline is the template.** Abstract, parameterize, replicate.
8. **PDF parsing prompts are versioned.** Build test suite with known-correct outputs.
9. **FL Medicaid: Facility and PC/TC rates typically mutually exclusive (99.96% of codes).** Three codes (46924, 91124, 91125) legitimately carry both per AHCA.
10. **Data layer is the moat.** Every session: add data, improve quality, or make adding data easier.
11. **Don't be CPRA-forward.** CPRA is one use case. Build for the platform.
12. **Economic/contextual data matters.** Ingest data that informs Medicaid, not just from Medicaid.
13. **Forecasting models are never deleted.** Always append; track performance over time.
14. **User data is never mixed with Aradune's public layer** without explicit opt-in.
15. **Log predictions. Compare to actuals. Publish accuracy.**
16. **No em-dashes in user-facing copy.** Use commas, colons, or periods instead.
17. **No "plain English" or "in plain English."** Say "natural language" or just omit.
18. **Upload data in context, not standalone.** File upload belongs in the chat or tool where it will be used.

---

## 18. Build & Deploy

```bash
# Development
cd ~/Desktop/Aradune
npm install && npm run dev       # localhost:5173

# CPRA pipeline (Terminal B)
cd tools/mfs_scraper/
python cpra_engine.py --all --cpra-em --output-dir ../../public/data/

# T-MSIS pipeline
cd pipeline/
Rscript tmsis_pipeline_duckdb.R medicaid-provider-spending.csv npidata_pfile_*.csv

# Build & deploy
npm run build && npx vercel --prod

# Verify deploy
curl -s -o /dev/null -w "%{http_code}" https://www.aradune.co/data/cpra_summary.json

# Git
git add . && git commit -m "describe change" && git push
```

**Required env vars in Vercel dashboard:**
- `ANTHROPIC_API_KEY` — for Policy Analyst (api/chat.js)
- `VITE_MONTHLY_PARQUET_URL` — external URL for claims_monthly.parquet (82MB, not deployed to Vercel)

---

## 19. Monetization — Two Tracks

Aradune has **two parallel go-to-market paths**. Both use the same codebase and data layer.

### Track A: Partnership / Acquisition (active — demo build)
A major consulting firm meeting is upcoming. The current build is optimized for this:
- **Pricing removed from the site** — kept flexible for negotiation
- **Password gate remains** — controlled access, "exclusive" positioning
- **Claude features front and center** — NL2SQL Data Explorer is the demo closer
- **Data depth is the pitch** — 250 tables, 115M rows, every public Medicaid dataset assembled
- Goal: partnership, licensing deal, or acquisition.

### Track B: Independent SaaS (future — full public build)
If Track A doesn't materialize, Aradune launches independently with a freemium model:

| Tier | Price | Who |
|---|---|---|
| Free (Aradune Open) | $0 | Journalists, advocates, aides, students |
| Analyst | $99/mo | Individual analysts, small consultants |
| Pro | $299/mo | Power users, consulting teams |
| State Agency | $50-200K/yr | State Medicaid agencies (may qualify for 75% FFP) |
| Enterprise | $50-500K/yr | Consulting firms, MCOs, hospital systems |
| Data as a Service | $25-100K/yr per dataset | Firms wanting bulk normalized data |

Requires: user accounts (Clerk recommended), usage tracking, Stripe billing, landing page redesign.

### Do not lose either option
- Keep pricing in CLAUDE.md but **not on the live site**
- ProGate component stays in code with generic "contact us" messaging
- All tools stay fully functional behind password gate

---

## 20. Reference Documents (docs/)

| File | Purpose |
|------|---------|
| `ARADUNE_MASTER.md` | Full strategy/architecture reference |
| `TMSIS_DATA_GUIDE.md` | T-MSIS operational guide — read before any T-MSIS work |
| `UX_FEATURES_SPEC.md` | Implementation spec for UX features |
| `NEW_ARADUNE_BUILD_STRUCTURE.md` | Architecture redesign proposal (session 9) |
| `AraduneMockup.jsx` | Definitive landing page + nav reference |

---

## 21. What Success Looks Like

**Current state (March 10, 2026 — session 12):**
- 250+ tables live in production (Fly.io pre-baked), 115M+ rows queryable
- 270 views (250 fact + 9 dim + 9 ref + 2 compat), 785MB Parquet, synced to R2
- 258+ API endpoints, 19 tools, 4 engines (CPRA, Caseload Forecast, Expenditure, RAG)
- Intelligence wired to frontend — SSE streaming chat with DuckDB + RAG tools
- Policy corpus: 1,039 docs, 6,058 chunks (CIBs, SHOs, SMDs, guidance from medicaid.gov)
- Cross-dataset insights live in State Profiles
- State Profile fully functional — 20 parallel API fetches, all field mappings correct
- Landing page polished — no em-dashes, no "plain English", contextual upload in chat
- Deployed: Fly.io (API + data, pre-baked ~1GB image) + Vercel (frontend)
- Behind password gate ("mediquiad")

**Next milestone — Consulting firm demo (Track A):**
- Polish NL2SQL prompts and example queries
- More data — every table strengthens the pitch
- Dry-run the demo end-to-end

**Next milestone — Public beta (Track B, if needed):**
- User accounts (Clerk — 4-8 hours), Landing page redesign, Stripe billing
- At least one person outside the team has used it

**3-6 months:** SPA/waiver search live. First external citation. Revenue conversation.
**6-12 months:** Cited in MACPAC report. Forecast accuracy dashboard published. Revenue covers infrastructure.
**1-3 years:** Default reference for Medicaid data. CMS links to it. Seven-figure revenue.

---

*The data is the moat. Build the moat. Ship fast. Iterate in public.*
