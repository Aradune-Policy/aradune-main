# CLAUDE.md — Aradune
> **The ONE source for Medicaid data intelligence.**
> Read this file at the start of every session. It defines what Aradune is, how it's built, and the rules for building it.
> Last updated: 2026-03-09 · Live: https://www.aradune.co

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
Backend:        Python FastAPI (server/) — 237+ endpoints across 20 route files, DuckDB-backed
                3 Vercel serverless functions in api/ (legacy)
AI:             Claude API via Vercel serverless (api/chat.js)
                Haiku for routing · Sonnet for analysis · Opus for complex reasoning
Pipeline:       Python build scripts (scripts/build_*.py) — lake ETL
                Python (cpra_engine.py) — CPRA/DuckDB analytical layer
                Python + pdfplumber — PDF fee schedule extraction
                R (tmsis_pipeline_duckdb.R) — T-MSIS processing
Orchestration:  Dagster (pipeline/dagster_pipeline.py) — 13 assets, 3 checks, 3 jobs, 2 schedules
CI/CD:          GitHub Actions (.github/workflows/ci.yml) — Vercel + Fly.io deploy
Deployment:     Vercel (frontend) · Fly.io (FastAPI, server/fly.toml + Dockerfile)
Vector store:   pgvector + Voyage-3-large embeddings (target — for RAG)
NL2SQL:         Claude Sonnet via Anthropic SDK — schema-in-prompt, SELECT-only validation
Design:         #0A2540 ink · #2E6B4A brand · #C4590A accent
                SF Mono for numbers · Helvetica Neue for body · No Google Fonts
Access:         Password gate ("mediquiad") via sessionStorage in Platform.tsx
                "Coming Soon" splash — entire site hidden until code entered
```

---

## 4. Live Tools (18 total)

**Site is behind a password gate** (`PasswordGate` component in `Platform.tsx`). Password: `mediquiad`. Stored in `sessionStorage` — clears on tab close. All tools are lazy-loaded and code-split per route.

| Group | Tool | Route | Status |
|-------|------|-------|--------|
| **Explore** | **Data Explorer** | **`/#/ask`** | **live — NL2SQL via Claude Sonnet** |
| **Explore** | **Data Catalog** | **`/#/catalog`** | **live — browsable table index** |
| **Explore** | **State Profile** | **`/#/state` or `/#/state/{code}`** | **live — 18 parallel API fetches, 7 sections** |
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

The CPRA exists as **two architecturally distinct systems** that serve different purposes:

### 5a. CPRA Frontend (Pre-Computed Cross-State Comparison)

`src/tools/CpraGenerator.tsx` (734 lines). Displays pre-computed rate comparisons from `fact_rate_comparison` (302K rows, 45 states, all HCPCS codes). This is a **general fee-to-Medicare comparison tool** — not limited to the 68 E/M codes.

**Data flow (pre-computed, read-only):**
```
fact_rate_comparison (lake)           → all codes, 45 states, pre-computed pct_of_medicare
cpra_em.json (2,742 rows/34 states)  → slim E/M extract for frontend
dim_447_codes.json (74 codes)        → old code list (⚠️ should be 68 — see 5b)
cpra_summary.json (7KB)              → pipeline aggregates (median, national context)
dq_flags_em.json (771 flags)         → data quality warnings per code/state
```

**Export formats:** PDF (`src/utils/cpraPdf.ts`) · Excel (`src/utils/cpraXlsx.ts`) · HTML inline.

**API endpoints (pre-computed):**
- `GET /api/cpra/states` — states with rate comparison data
- `GET /api/cpra/rates/{state_code}` — all rate comparisons for a state
- `GET /api/cpra/dq/{state_code}` — data quality flags
- `GET /api/cpra/compare` — compare specific codes across states

### 5b. CPRA Upload Tool (42 CFR 447.203 Compliance Generator)

`server/engines/cpra_upload.py` (821 lines). A **user-upload, stateless CPRA generator** ported from the standalone `cpra-pipeline/` project. Any state uploads two CSVs and gets the full CPRA computed in <2 seconds.

**This is the regulatory-correct implementation.** Key differences from the pre-computed system:

| Aspect | Pre-Computed (5a) | Upload Tool (5b) |
|--------|-------------------|-------------------|
| E/M codes | 74 (old list) | **68** (official CMS CY 2025 E/M Code List) |
| Code-category mapping | 1:1 (74 rows) | **Many-to-many** (171 pairs: all 68 in PC, 52 in OB-GYN, 51 in MH/SUD) |
| Conversion factor | $33.4009 (CY2026) | **$32.3465** (CY2025 — correct for July 2026 deadline) |
| Medicare rates | State-level averages | **Per-locality** (FL=3, CA=29, AL=1 localities) |
| Data source | Pre-computed lake table | **User-uploaded fee schedule + utilization** |
| Categories | "primary_care", "obgyn", "mhsud" | "Primary Care", "OB-GYN", "Outpatient MH/SUD" |

**Reference data** (in `data/reference/cpra/`):
- `em_codes.csv` — 68 codes with RVUs from official CMS CY 2025 E/M Code List
- `code_categories.csv` — 171 rows, many-to-many code→category mapping
- `GPCI2025.csv` — 109 Medicare localities across 53 states

**API endpoints (upload):**
- `GET /api/cpra/upload/states` — 53 states with locality counts
- `GET /api/cpra/upload/codes` — 68 codes + 171 category mappings
- `GET /api/cpra/upload/templates/fee-schedule` — blank CSV template (68 rows)
- `GET /api/cpra/upload/templates/utilization` — blank CSV template (171 rows)
- `POST /api/cpra/upload/generate` — upload 2 CSVs → full CPRA JSON
- `POST /api/cpra/upload/generate/csv` — → CSV download
- `POST /api/cpra/upload/generate/report` — → self-contained HTML report

**FL spot-check (verified against R pipeline):**
- 99213 PC: Medicaid $34.29 / Medicare $91.39 = **37.5%** of Medicare
- 42 of 68 codes have FL rates; 26 are not on the AHCA fee schedule
- Category weighted averages: PC 61.2%, OB-GYN 61.1%, MH/SUD 59.0%

**Frontend:** CpraGenerator.tsx has two modes:
- **Cross-State Comparison** (default) — pre-computed rate comparisons from `fact_rate_comparison`
- **Bring Your Own Data** — upload fee schedule + utilization CSVs, generates full CPRA via `/api/cpra/upload/generate`

**Source project:** `/Users/jamestori/Desktop/cpra-pipeline/` — contains the R pipeline (publication-quality figures/tables via Quarto), Python engine, and standalone FastAPI server. See `cpra-pipeline/CPRA_TOOL_HANDOFF.md` for full details.

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

**Individual steps:**
```bash
python cpra_engine.py --init            # DuckDB schema + ETL from SQLite
python cpra_engine.py --em-codes        # Load 74 E/M codes
python cpra_engine.py --medicare-rates  # Calculate 858K locality rates
python cpra_engine.py --cpra            # Match Medicaid→Medicare (242K rows)
python cpra_engine.py --dq              # Run 11 DQ rules (258K flags)
python cpra_engine.py --export          # Export all 7 output files
python cpra_engine.py --cpra-em         # Export slim E/M extract
python cpra_engine.py --stats           # Print table counts
```

**DuckDB (`aradune_cpra.duckdb`) — 1.87M rows:**

| Table | Rows | Description |
|-------|------|-------------|
| dim_state | 51 | State dimension |
| dim_procedure | 16,978 | Medicare PFS codes + RVUs |
| dim_medicare_locality | 109 | GPCI values by locality |
| dim_em_447_codes | 74 | E/M codes per 42 CFR 447.203 |
| xwalk_locality_to_state | 109 | Equal-weighted locality→state crosswalk |
| fact_medicare_rate | 858,593 | Medicare rates × 109 localities |
| fact_medicare_rate_state | 417,481 | State-level Medicare averages |
| fact_rate_comparison | 302,332 | CPRA: Medicaid vs Medicare (45 states) |
| fact_dq_flags | 269,475 | Data quality flags |

**Output files (what the frontend reads):**

| File | Size | Frontend? | Notes |
|------|------|-----------|-------|
| `cpra_em.json` | 495KB | ✅ Primary CPRA data | 40 states, 2,216 E/M rows |
| `dq_flags_em.json` | 81KB | ✅ DQ warnings | 789 flags, 6 types |
| `dim_447_codes.json` | 14KB | ✅ Code definitions | 74 E/M codes |
| `cpra_summary.json` | 7KB | ✅ State aggregates | median, national context |
| `cpra_all.json` | 47.5MB | ❌ gitignored/vercelignored | too large for client |
| `dq_flags.json` | 27.2MB | ❌ gitignored/vercelignored | full 258K flags |
| `medicare_rates_locality.parquet` | 10MB | ❌ gitignored/vercelignored | locality-level rates |

**45 states in rate_comparison (40 with E/M codes in cpra_em.json):** AK, AL, AR, AZ, CA, CO, CT, DC, DE, FL, GA, HI, ID, IL, IN, KY, LA, MA, MD, ME, MI, MN, MO, MS, MT, NC, ND, NE, NH, NM, NV, NY, OH, OK, OR, PA, RI, SC, SD, TX, UT, VA, WA, WV, WY

**States with limited CPRA data:**
- IA: No FFS physician codes (HCBS-only fee schedule)
- NY: 13 codes (HCPCS Level II only, physician services via managed care)
- NC: 3,156 codes but only 1 E/M match

**CPRA summary numbers:**
- Median pct_of_medicare: **84.8%**
- Primary care E/M avg: **81.4%** (below Medicare)
- MH/SUD avg: **99.6%** (near parity)
- OB/GYN avg: **132.9%** (above Medicare)
- Spot-check CA 99213: Medicaid $82.02 / Medicare $104.31 = **78.6%**

**DQ flag inventory:**

| Flag | Count | Notes |
|------|-------|-------|
| BELOW_50PCT | 55,284 (214 E/M) | Most common — red |
| STALE_RATE | 52,483 (383 E/M) | Effective dates before 2023 — amber |
| MISSING_MEDICARE | 124,476 (23 E/M) | State-specific codes with no PFS match |
| ABOVE_MEDICARE | 25,859 (128 E/M) | Often OB/GYN, lab — green |
| METHODOLOGY_RISK | 10 states | Pct-of-charge pricing — amber |
| LOW_COVERAGE | 13 states | <1000 matched codes — amber |
| HIGH_RATE | 782 | Rates >$10K (surgical/anesthesia) |

**Phase 1 CMS data (prior sessions):** 15 datasets, 9.3M rows across `cms_data.py` (8 datasets: FMAP, enrollment, NADAC, SDUD, CMS-64, managed care, quality measures, DSH) and `ncci_scraper.py` (NCCI edits 2.5M pairs, HCPCS Level II 8,623 codes, 1115 waivers 647 via Playwright, drug rebate products).

---

## 7. Known Bugs & Issues

### Critical (fix first)

| # | Bug | Location | Status |
|---|-----|----------|--------|
| 1 | ~~White page on CPRA~~ | `/#/cpra` | **Resolved** — ErrorBoundary added. |
| 2 | ~~T-MSIS DuckDB empty~~ | | **Resolved** — T-MSIS data ingested into lake. |
| 3 | ~~6 states missing from CPRA~~ | | **Fixed** — COALESCE in cpra_engine.py. 34→45 states (AK/MI/NM computed from RBRVS). |
| 4 | ~~**Frontend not wired to FastAPI**~~ | All 13 tools | **Resolved** — All tools wired with JSON fallback. |
| 5 | ~~**Policy Analyst no auth**~~ | `api/chat.js` | **Mitigated** — Has Bearer token auth (PREVIEW_TOKEN + Stripe + rate limiting at 30/hr). Set PREVIEW_TOKEN env var on Vercel to restrict beyond site password. |
| 6 | ~~**Old CPRA uses wrong code list**~~ | `cpra_engine.py` | **Fixed** — Updated to 68 codes from reference CSVs, $32.3465 CF, many-to-many (171 pairs). Re-run `--em-codes --cpra --export` to regenerate `fact_rate_comparison`. |
| 7 | ~~FL rates not in CPRA display~~ | `fact_rate_comparison` | **Fixed** — Re-ran cpra_engine.py. FL now in rate_comparison. Also added AK/MI/NM computed fee schedules (RBRVS). 45 states, 302K rows. |
| 8 | ~~**CPRA upload not deployed to Fly.io**~~ | `server/Dockerfile` | **Fixed** — Dockerfile updated to COPY `data/reference/cpra/`. Deployed to Fly.io. |
| 9 | **R2 credentials need rotation** | Infrastructure | Shared in plain text during session. |
| 10 | **db.py fact_names must match filesystem** | `server/db.py` | Only facts listed in `fact_names` array (line 41-148) are registered as views. When adding new lake tables, always update this list. Currently 250 entries = 250 filesystem directories. |
| 11 | **Fly.io cold start slow** | Infrastructure | S3 sync downloads 270+ files on startup (~60s). Health check fails during sync. Consider pre-baking lake into Docker image or using persistent volumes. |
| 13 | ~~**Fly.io needs redeployment**~~ | Infrastructure | **Done** — Deployed session 8. 232 tables live, NL2SQL working. |
| 14 | ~~**ANTHROPIC_API_KEY on Fly.io**~~ | Infrastructure | **Done** — Set on Fly.io, Vercel, and local .env. |
| 12 | ~~**Forecast engine needs frontend**~~ | `/#/forecast` | **Done.** Full UI: upload form, caseload forecast (fan chart + model table + interventions), expenditure projection (summary, chart, category table, MC/FFS breakdown bar). Tab toggle between caseload and expenditure views. |

### Data Quality — Investigated

All outlier states investigated. Root causes documented in `public/data/dq_state_notes.json` (42 states, 11 flagged).

| State | Root Cause | E/M Median |
|-------|-----------|-----------|
| SD | Per-15-minute unit rates, not per-service | 3% (needs unit conversion) |
| CT | Non-E/M codes include bundled facility costs | 106% (E/M is reasonable) |
| KY | Similar bundling on surgical codes | 106% (E/M is reasonable) |
| RI | Bundled facility rates inflate averages | 278% |
| DC | 45K+ codes from multiple fee schedule types | 28% |

### Minor / Cleanup

| Item | Action |
|------|--------|
| `scripts/build-cpra-data.mjs` | Delete — superseded by cpra_engine.py |
| Locality weighting is equal, not population-weighted | Acceptable for v1; fix with Census CBSA data later |
| `StateRateEngine.js` not wired | 1,153 lines, 42/42 tests passing, but not connected to Rate Builder UI |
| Password gate is client-side only | `sessionStorage` check in Platform.tsx — not a security boundary, just a preview wall |
| No export on most tools | Only CPRA has PDF/Excel. Other tools need CSV/PDF export buttons. |
| `cpra_precomputed.json` still in public/data | Listed as "⚠️ DELETE" in file map — actually delete it |

---

## 8. Immediate Next Steps

### Tier 1 — Ship-blocking (before removing password gate)
1. ~~**Wire frontend to FastAPI endpoints**~~ — **Done.** All 13 tools wired. CPRA Generator, WageAdequacy, HcbsTracker use per-endpoint API calls. RateDecay, RateBuilder, ComplianceReport, QualityLinkage, RateLookup, FeeScheduleDir use bulk API endpoints (`/api/bulk/*`) with static JSON fallback. RateReduction uses DuckDB-WASM (no API needed). New `server/routes/bulk.py` serves 7 bulk endpoints matching frontend JSON shapes.
2. ~~**Auth on Policy Analyst**~~ — **Done.** Preview token (`mediquiad`) accepted in `api/chat.js`. Password gate auto-populates analyst token in localStorage. Three auth paths: ADMIN_KEY, PREVIEW_TOKEN, ANALYST_TOKENS (env vars).
3. ~~**Confirm CPRA in production**~~ — **Build verified.** ErrorBoundary in place, TypeScript clean, production build succeeds. Needs visual verification on aradune.co.
4. ~~**Reconcile conversion factor**~~ — **Done.** `medicare_pfs.py` updated from $32.3465 (QPP) to $33.4009 (non-QPP). Frontend and cpra_engine.py already used correct value.

### Tier 2 — Platform completeness (done items collapsed)

**Done:** ~~Nav redesign~~, ~~DQ state notes~~, ~~Bar cap~~, ~~FL methodology addendum~~, ~~CPRA upload frontend~~, ~~CPRA upload Fly.io deploy~~, ~~cpra_engine.py update~~, ~~Caseload Forecaster frontend~~, ~~Expenditure modeling engine + API + frontend~~.

**Still open:**
5. **Landing page redesign** from `docs/AraduneMockup.jsx` — password gate restyled but full post-gate landing page not built.
7. **Wire `StateRateEngine.js` into Rate Builder** — 42/42 tests passing, not connected to UI.

### Tier 2b — Critical platform gaps (what's actually blocking public launch)

These are the things that would make a user say "this is a real product":

| # | Gap | Why it matters | Effort |
|---|-----|----------------|--------|
| A | ~~**State Profile pages**~~ | ~~Primary entry point for 80% of users.~~ **Done.** `StateProfile.tsx` (~470 lines), 18 parallel API fetches, 7 collapsible sections (overview, enrollment, rates, hospitals, quality, workforce, pharmacy, economic). Hash routing: `/#/state/{code}`. | **Done** |
| B | ~~**Search / discovery**~~ | **Partially done.** Data Explorer (`/#/ask`) provides NL2SQL search. NavSearch exists. Full-text search across all tools still TODO. | **Partial** |
| C | ~~**Landing page**~~ | **Updated.** Hero copy, stats, "Find a state" → State Profile, "Ask a Question" CTA. | **Done** |
| D | ~~**Data catalog**~~ | **Done.** `DataCatalog.tsx` at `/#/catalog` — browsable index of all tables with row counts, column schemas, descriptions. | **Done** |
| E | ~~**Export for all tools**~~ | **Done.** All tools now have CSV export buttons. CPRA also has PDF/Excel. | **Done** |
| F | **User accounts** | Can't monetize with a shared password gate. Need at minimum email + magic link auth, saved workspaces, usage tracking. | Large |

### Tier 3 — Data expansion (standing instruction)

The data layer is the moat. Every session: add data, improve quality, or make adding data easier.

**Completed federal datasets (115M+ rows, 250 fact tables):**
- T-MSIS claims (227M source) · CPRA rates (45 states) · CMS-64 · NADAC · SDUD + SDUD 2024 (5.2M) + SDUD 2025 (2.64M)
- BLS wages (state/MSA/national) · HCRIS hospitals + SNFs · Hospital quality (ratings/VBP/HRRP/HAC)
- Five-Star NF · POS · PBJ staffing (65M+) · EPSDT · Enrollment/unwinding/MC plans
- Census ACS · BRFSS · CDC mortality/overdose · FRED economic (GDP/pop/unemployment/income)
- HPSA · Scorecard · HAI · NH ownership/penalties/deficiencies · HCAHPS · Imaging
- MLTSS · Financial mgmt · Eligibility levels · ACA FUL · DQ Atlas · 1115 waivers · NCCI edits
- SAMHSA: NSDUH (5,865 + 2024 update), N-SUMHSS (27,957 MH/SUD facilities), Block Grants ($0.95B), TEDS-A (49 states, 1.6M admissions)
- CHIP: enrollment, unwinding, monthly/annual, eligibility, continuous eligibility
- Behavioral health: BH by condition, MH/SUD recipients, IPF quality, BRFSS behavioral
- Managed care: enrollment by plan (7,804), MLTSS enrollment, PACE (201 orgs), MC quality features, MC enrollment summary (2016-2024), MC dashboard (AZ/MI/NV/NM)
- Hospice: quality (331K), provider, directory, CAHPS · Maternal health · ASC quality
- Medicare: enrollment (557K), provider enrollment, IPPS impact, opioid prescribing (539K), telehealth (32K), geo variation (state/county), MA geo variation, Part D (geo + quarterly spending + opioid geo + spending by drug), Medicare program stats, physician geo (269K), hospital service area (1.16M)
- Drug rebate products (1.9M) · AHRF county · Physician Compare · ESRD QIP · ESRD ETC results
- Home health agencies · IRF providers · LTCH providers · Dialysis facilities (v2 with quality measures)
- SNF VBP · SNF quality · Nursing home state averages · NH penalties v2 · NH survey summary · FQHC directory + hypertension + quality badges
- Vital stats (monthly) · Maternal mortality (monthly) · Pregnancy outcomes · Well-child visits
- Telehealth services · Dental services · Contraceptive care · Respiratory conditions
- SNAP enrollment (3,920) · TANF enrollment (9,072) · HUD Fair Market Rents (4,764)
- HCBS waitlists (51 states, 606K people) · LTSS expenditure/users/rebalancing · Quality Core Sets 2023 & 2024
- Eligibility processing · Marketplace unwinding (59K) · SBM unwinding · FMR FY2024 ($909B) · New adult spending
- SAIPE poverty (3,196) · CDC PLACES county health (3,144 counties) · HRSA health center sites (8,121)
- Marketplace OEP · MUA designations (19,645) · Workforce projections (121 professions) · Food environment (304 vars)
- Medicaid drug spending (brand/generic, 2019-2023) · NHE by state (1991-2020, 117K)
- ACO/MSSP: orgs (511), participants (15,370), beneficiaries by county (135K), REACH results, financial results (476 ACOs)
- NHSC field strength · MACPAC enrollment (Exhibit 14) + spending per enrollee (Exhibit 22) + spending by state (Exhibit 16) + benefit spending (Exhibit 17)
- Nursing workforce demographics (17.6K) + earnings (41.8K) · Post-acute care (HHA/IRF/LTCH utilization)
- Market saturation by county (962K) · HHA cost reports (10,715) · CDC overdose deaths (81K) · CDC leading causes of death (10.8K)
- Part D opioid geo (329K) · Part D spending by drug (14,309)

**Highest-value datasets not yet ingested:**

| # | Dataset | Why it matters | Source | Status |
|---|---------|---------------|--------|--------|
| 1 | **Hospital price transparency MRFs** | Only way to see what MCOs actually pay providers. Covers ~70% of Medicaid (MC). Unique competitive advantage — no one has assembled this for Medicaid. | CMS MRF index | Not started — massive dataset, requires targeted extraction |
| 2 | **HCBS waitlist data** | 700K+ people waiting for HCBS services nationally. No public database aggregates this. Genuinely differentiated. | KFF / state reports | **✓** fact_hcbs_waitlist (51 states, 606,895 people, 8 population categories) |
| 3 | **340B covered entity data** | HRSA quarterly. Drug pricing intersection with Medicaid. | hrsa.gov | Blocked — Blazor Server app, needs browser automation. JSON/Excel export available at 340bopais.hrsa.gov/Reports |
| 4 | **SPA/waiver policy corpus** | The text of State Plan Amendments, 1115 waivers, CIBs, SHO letters. Central to "AI-native policy intelligence." | CMS MACPro / medicaid.gov | Not started (have 647 waiver metadata records, but not the actual documents) |
| 5 | **MCO contract terms** | Rate certifications, MLR reports, network adequacy standards. ~70% of Medicaid flows through MCOs. | State portals | Not started |
| 6 | **SNAP/TANF enrollment** | Cross-program correlation for caseload forecasting. | fns.usda.gov / ACF | **✓** fact_snap_enrollment (3,920), fact_tanf_enrollment (9,072) |
| 7 | **More state fee schedules** | 45/51 states in CPRA. Remaining: KS (portal login), NJ (portal login), TN (MC only), WI (manual). IA/VT in medicaid_rate but not rate_comparison. | State portals | 4 remaining |
| 8 | **UPL demonstrations** | Upper payment limit filings — key to understanding supplemental payment structure. | CMS MACPro | Not started |
| 9 | **Full SDP preprint parsing** | Have 34 state index entries. Need actual preprint PDF content via Claude API + pdfplumber. | CMS | Index done, parsing not started |
| 10 | **Historical HCRIS (FY2021-2022)** | Enables 3-year trend sparklines for AHEAD Readiness. | cms.gov | Not started |

**Improve existing data:**
- Add `weighted_avg_pct` to `cpra_summary.json` using CY2023 FFS claim volume weights
- Add category-level breakdowns to `cpra_summary.json` per state
- Build reusable ingestion pattern (fetch→parse→validate→normalize→load) to accelerate new sources

### Tier 4 — Analytical features

| # | Feature | Status | Next action |
|---|---------|--------|-------------|
| 1 | ~~**Caseload forecasting**~~ | **Done.** Engine + API (10 endpoints) + full frontend UI with fan charts, model comparison, intervention effects. | Scenario builder (Phase 3) |
| 2 | ~~**Expenditure modeling**~~ | **Done.** Engine (`expenditure_model.py`) + 4 API endpoints + frontend UI (summary, chart, per-category table, MC/FFS breakdown bar). Tab toggle with caseload view. | — |
| 3 | ~~**Scenario builder**~~ | **Done.** Third tab in CaseloadForecaster. 4 sliders (unemployment, eligibility, rate change, MC shift) with preset scenarios. Client-side adjustment of forecast with baseline vs scenario chart. | — |
| 4 | ~~**NL2SQL over the data lake**~~ | **Done.** `DataExplorer.tsx` at `/#/ask`. Claude Sonnet generates DuckDB SQL from natural language, validates (SELECT-only, LIMIT, forbidden keywords), executes with timeout. 10 example queries. Backend: `server/routes/nl2sql.py`. | — |
| 5 | **RAG over policy corpus** | Not started | pgvector + Voyage-3-large embeddings over SPAs, waivers, CIBs. Requires ingesting the policy documents first (Tier 3 #4). |
| 6 | **Forecast accuracy dashboard** | Not started | Principle #15: "Log predictions. Compare to actuals. Publish accuracy." Unique credibility signal — no Medicaid analytics firm publishes their forecast accuracy. |
| 7 | **Cross-dataset insights** | Not started | The moat's real value: "States with lowest rates AND highest uninsured AND longest HCBS waitlists." Requires cross-table joins that the current tool-per-table architecture doesn't support. State Profile pages (Tier 2b-A) are the natural home for this. |

### Recent Changes (2026-03-09, session 8 — deploy + demo prep)
- **Fly.io deployed** — All 250 fact tables registered in code, 232 live in production (109M rows). NL2SQL endpoint working with ANTHROPIC_API_KEY.
- **ANTHROPIC_API_KEY set** — Fly.io (`fly secrets set`), Vercel (`vercel env add`), local `server/.env` (gitignored). NL2SQL and Policy Analyst both functional.
- **Vercel deployed** — Frontend live at aradune.co with all changes below.
- **Pricing removed from site** — Deleted `Pricing()` component and `/pricing` route from Platform.tsx. Removed "See pricing →" footer link. Updated ProGateModal to generic "contact us" text. Updated PolicyAnalyst auth screen to remove subscription language. Pricing kept in CLAUDE.md (Section 19) for Track B reference.
- **Two-track strategy documented** — Track A: partnership/acquisition demo build (active). Track B: independent SaaS with freemium model (future fallback). Both use same codebase.
- **Brand assets migrated to SVG** — Navbar: logo-wordmark.svg (was logo-full.png). Chat icon: helmet.svg (was icon-bot.png). PDF reports: logo-wordmark.svg.
- **Lottie sword loader** — `sword-animation.json` (10.8MB) fetched at runtime via `SwordLoader` component. Used as loading fallback for lazy-loaded tools. Kept small (80x140px).
- **Landing page stats updated** — "250 fact tables", "115M+ rows", "80+ federal sources".
- **lottie-react** dependency added to package.json.

### Recent Changes (2026-03-09, sessions 4-7 — data expansion sprint)
- **65 new fact tables ingested** across 4 sessions, bringing total from 185 → 250 fact tables
- **Session 4** (round 10): SNAP enrollment (3,920), TANF enrollment (9,072), HUD Fair Market Rents (4,764 counties)
- **Session 5** (round 10-11): SDUD 2025 (2.64M rows, $108.8B), HCBS waitlists (51 states, 606K people), Quality Core Sets 2023 & 2024, eligibility processing, marketplace/SBM unwinding, LTSS expenditure/users/rebalancing, vital stats monthly, maternal mortality monthly, FMR FY2024 ($909B), new adult spending, NSDUH 2024, MC enrollment summary
- **Session 6** (round 12): SAIPE poverty (3,196), CDC PLACES county (3,144 counties, 40 measures), HRSA health center sites (8,121), marketplace OEP, MUA designations (19,645), workforce projections (121 professions), food environment (304 variables), Medicare telehealth (32K), Medicare/MA geo variation, Medicaid drug spending, MC dashboard, NHE by state (117K)
- **Session 7** (round 13 + inline): MSSP ACO orgs (511) + participants (15,370) + beneficiaries by county (135K), ACO REACH results, Part D geo (116K) + quarterly spending (28K) + opioid geo (329K) + spending by drug (14,309), NHSC field strength, FQHC hypertension + quality badges, MACPAC Exhibits 14/16/17/22, nursing workforce (17.6K) + earnings (41.8K), TEDS-A admissions (49 states, 1.6M), Medicare program stats, hospital service area (1.16M), HHA cost reports (10,715), ESRD ETC results, PAC utilization (HHA/IRF/LTCH), market saturation by county (962K), Medicare physician geo (269K), MSSP financial results (476 ACOs), NH penalties v2 (17.4K) + survey summary (44K), dialysis facility v2 (7,557), CDC overdose deaths (81K) + leading causes (10.8K), MACPAC spending by state + benefit spending
- **db.py** — 250 fact_names entries, verified matching all 250 filesystem directories
- **meta.py** — TABLE_DESCRIPTIONS updated with all 250 fact tables + 9 dims + 9 refs
- **nl2sql.py** — Key schema entries added for MSSP, Part D, NHSC, MACPAC, TEDS, CDC, ACO tables
- **All 65 new Parquet files synced to R2** via wrangler — ready for Fly.io deployment
- **4 new reference tables** added: ref_pediatric_drugs (262), ref_clotting_factor (500), + 2 more

### Recent Changes (2026-03-08, session 2)
- **Expenditure Modeling Engine** — `server/engines/expenditure_model.py` (~430 lines). Takes caseload forecast output + user-uploaded expenditure parameters CSV (cap rates for MC, cost-per-eligible for FFS). Applies compound monthly trend, admin load, risk margin, policy adjustments. Returns per-category and aggregate projections with CI bands. Key classes: `ExpenditureModeler`, `CategoryExpenditure`, `ExpenditureResult`.
- **Expenditure API routes** — 4 new endpoints added to `server/routes/forecast.py` (now 10 total): `GET /api/forecast/templates/expenditure-params`, `POST /api/forecast/expenditure` (full pipeline), `POST /api/forecast/expenditure/csv`, `POST /api/forecast/expenditure-only`.
- **Caseload Forecaster frontend** — `src/tools/CaseloadForecaster.tsx` (~830 lines). Full upload UI: state selector, horizon dropdown, caseload/events/expenditure-params file inputs with template download links, seasonality/economic checkboxes. Caseload view: summary metrics, category pills, fan chart with 80/95% CI bands, event markers, intervention effects panel, model comparison table. Expenditure view: summary card (total/MC/FFS), expenditure fan chart (orange accent), per-category table (9 columns + totals row), MC vs FFS horizontal breakdown bar. Tab toggle between views.
- **Platform.tsx updated** — CaseloadForecaster registered as lazy-loaded tool at `/#/forecast` in Build group. Tool count: 15.
- **CLAUDE.md overhauled** — Sections 4, 7, 8, 9, 16 updated. Section 8 restructured with Tier 2b (critical platform gaps: State Profiles, search, landing page, data catalog, export, user accounts) and Tier 3/4 tables with clear-eyed gap analysis.

### Recent Changes (2026-03-08, session 1)
- **Caseload Forecasting Engine (Phase 1)** — `server/engines/caseload_forecast.py` (~650 lines). SARIMAX + ETS model competition per category with intervention variables (COVID PHE, unwinding, MC launches, eligibility changes). Economic covariate enrichment from Aradune's public unemployment data. Holdout MAPE validation. Template-driven CSV upload pattern (same as CPRA). Tested with synthetic FL data: 8 categories, 96 months, all SARIMAX, <1% MAPE. Key fixes: event deduplication for multicollinearity, future exog construction for step functions. Dependencies added: `statsmodels>=0.14.0`, `pmdarima>=2.0.0`, `pandas>=2.1.0`, `numpy>=1.26.0`.
- **Forecast API routes** — `server/routes/forecast.py` (original 6 endpoints): template downloads (caseload + events CSVs), generate forecast (JSON + CSV), public enrollment time series, enrollment by eligibility group.
- **Round 9 data ingestion** — `scripts/build_lake_round9.py` (17 datasets): Medicare Enrollment (557K), Opioid Prescribing (539K), SDUD 2024 (5.2M), Drug Rebate Products (1.9M), CMS IPPS Impact (3,152), AHRF County, Physician Compare, ESRD QIP, OTP providers, CMS-64 FFCRA, contraceptive care, respiratory conditions, program monthly, MC annual/info monthly, CHIP monthly/app-elig, performance indicator, new adult enrollment, Medicare provider enrollment. Total: ~8.3M new rows.
- **Round 9 API routes** — `server/routes/round9.py` (22 endpoints): Medicare enrollment/duals, opioid prescribing summary, SDUD 2024 top drugs, CMS IPPS impact, Medicare provider enrollment by type, and more.
- **Rounds 4-8 data ingestion** — Multiple build scripts ingested ~80+ additional fact tables across sessions: hospital directories, MC programs, CHIP enrollment/unwinding, medicaid applications, vaccinations, blood lead screening, dual status, benefit packages, NAS rates, SNF VBP/quality, FQHC directory, vital stats, HHCAHPS, hospice directory/CAHPS, VHA providers, pregnancy outcomes, and more.
- **db.py expanded** — Now registers 250 fact tables (up from ~70). Fixed duplicate `imaging_hospital` entry. All 250 lake directories matched.
- **Data lake milestone** — 115M+ rows across 250 fact tables, 9 dimensions, 9 references, 2 compat views = 270 total views. 237+ API endpoints across 20 route files. Deployed to Fly.io (needs redeployment for sessions 4-7 data).
- **Platform.tsx updated** — Stats now show "115M+" rows and "250" fact tables.

### Recent Changes (2026-03-07)
- **3 new computed fee schedules** — AK (RBRVS CF=$43.412, 138.6% MCR), MI (RBRVS CF=$21.30, 66.7% MCR), NM (150% of Medicare, 154.9% MCR). Script: `scripts/build_lake_fee_schedules_computed.py`. Added to both Parquet lake and SQLite.
- **CPRA coverage expanded** — 42→45 states in `fact_rate_comparison` (302,332 rows), 39→40 states with E/M data in `cpra_em.json`.
- **cpra_engine.py regenerated** — Updated to 68 codes, $32.3465 CF, many-to-many (171 pairs) from reference CSVs. All exports refreshed.
- **Supplemental Payments Phase 2** — `build_lake_supplemental_p2.py` created: hospital-level DSH (6,103 hospitals) + SDP preprint (34 states). 3 new API endpoints.
- **CpraGenerator.tsx upload tab** — "Bring Your Own Data" mode added, POSTs to `/api/cpra/upload/generate`.
- **Dockerfile updated** — `COPY data/reference/cpra/` for upload tool. Ready for `fly deploy`.

### Recent Changes (2026-03-06, session 2)
- **CPRA Upload Tool ported** — User-upload CPRA generator from `cpra-pipeline/` integrated into Aradune. Engine at `server/engines/cpra_upload.py` (821 lines), 7 new endpoints under `/api/cpra/upload/`. Uses the **correct** CMS CY 2025 E/M code list (68 codes, 171 code-category pairs, $32.3465 CF). Reference data in `data/reference/cpra/` (3 CSVs: em_codes, code_categories, GPCI2025). Existing pre-computed comparison routes unchanged — general fee-to-Medicare comparison is a separate tool from the compliance-specific CPRA upload. Tested end-to-end with FL data (99213 PC: $34.29/$91.39 = 37.5% — exact match with R pipeline).
- **Password gate redesigned** — Replaced tiny logo PNG with large text "ARADUNE" (32px, brand green, letterspaced). Content now left-justified in a centered 400px block instead of scattered center-aligned text. Added "Access code" label above input. Deployed to Vercel.
- **FL Practitioner Fee Schedule added** — Downloaded from AHCA, parsed 6,676 codes into `fact_medicaid_rate` (573,853 total rows). FL went from 3,773 to 10,449 rows. Handles "BR" (By Report) non-numeric values. Note: these rates are in `fact_medicaid_rate` but NOT yet in `fact_rate_comparison` (which the CPRA frontend reads). The upload tool computes FL correctly from CSVs.
- **Supplemental payment data ingested** — CMS-64 FMR (1,553 rows, FY 2019-2024, DSH/supplemental/GME by service x state) + MACPAC Exhibit 24 (102 rows, FY 2023-2024). 4 API endpoints in `server/routes/supplemental.py`, 2 new lake tables. Synced to R2 and live on Fly.io. Key findings: TN 98.2% supplemental, TX 92.5%, VA 90.2%. TX $9.7B/yr in supplemental payments.
- **Nav groups renamed** — Transparency→Explore, Adequacy→Analyze, Modeling→Build.

### Recent Changes (2026-03-06, session 1)
- **Frontend→API wiring (complete)** — All 13 tools use API-first with static JSON fallback. 7 bulk endpoints in `server/routes/bulk.py`.
- **Policy Analyst auth fixed** — Preview token (`mediquiad`) in `api/chat.js`.
- **Conversion factor reconciled** — `medicare_pfs.py` updated to $33.4009 (non-QPP) for general comparison. Note: CPRA compliance uses $32.3465 (CY2025).
- **CPRA DQ panel enhanced** — `dq_state_notes.json` wired.
- **Bar visualization capped** — Capped at 200%.
- **FL methodology addendum** — Appended to Policy Analyst system prompt.
- **AHEAD Readiness Score** — New tool at `/#/ahead-readiness`.
- **Password gate added** — `PasswordGate` in Platform.tsx.
- **Brand assets integrated** — navbar, favicon, chat avatar, PDF headers.
- **.gitignore overhauled** — Prevents 45GB+ data from git.
- **Pushed to main** — Commit `95f5a34`.

### External Project: cpra-pipeline

Located at `/Users/jamestori/Desktop/cpra-pipeline/`. A standalone CPRA pipeline built separately, now partially ported into Aradune. Contains:
- **R pipeline** (`R/01-07`) — Publication-quality CPRA report via Quarto. Real Medicare rates + FL Medicaid rates + simulated utilization. Produces 6 figures, gt tables, PDF report.
- **Python engine** (`python/cpra_generator.py`) — The engine ported into Aradune as `server/engines/cpra_upload.py`.
- **Python API** (`python/cpra_api.py`) — Standalone FastAPI server (port 8100). Routes ported into `server/routes/cpra.py`.
- **Reference data** (`data/raw/`) — em_codes.csv, code_categories.csv, GPCI2025.csv, PPRRVU25_JAN.csv, fl_practitioner_fee_schedule_2025.xlsx. The first 3 copied to `data/reference/cpra/`.
- **FL test data** (`data/simulated/`) — Real FL Medicaid rates + simulated utilization CSVs. Useful for testing the upload tool.
- **HCBS templates** (`deliverables/data_requests/`, `data/hcbs/`) — Templates for the separate HCBS disclosure required by 447.203(b)(2)(iv). Not yet built.
- **Key regulatory notes:** Deadline July 1, 2026. CY 2025 rates. 68 codes (not 74). $32.3465 CF. Only utilization data is simulated (no FMMIS access on personal laptop). When moved to AHCA work machine, replace simulated utilization with real FMMIS extract.

---

## 9. File Map

```
Aradune/
├── CLAUDE.md                        ← THIS FILE (auto-loaded by Claude Code)
├── README.md
├── SETUP.md
├── package.json / package-lock.json
├── vite.config.ts / tsconfig.json
├── vercel.json                      ← SPA fallback, cache headers, build config
├── index.html
│
├── src/
│   ├── Platform.tsx                 ← ~980 lines. Main router, tool registry,
│   │                                   landing page, ToolErrorBoundary, PasswordGate
│   │                                   Password: "mediquiad" (sessionStorage)
│   ├── design.ts                    ← Design tokens (C, FONT, SHADOW)
│   ├── tools/
│   │   ├── TmsisExplorer.tsx        ← ~2,400 lines. Spending Explorer.
│   │   ├── CpraGenerator.tsx        ← 734 lines. CPRA wedge product.
│   │   ├── FeeScheduleDir.tsx       ← State fee schedule directory (#/fees)
│   │   ├── RateLookup.tsx           ← Search any HCPCS code cross-state (#/lookup)
│   │   ├── WageAdequacy.tsx         ← 512 lines. BLS vs Medicaid (#/wages)
│   │   ├── QualityLinkage.tsx       ← 446 lines. Outcomes vs rates (#/quality)
│   │   ├── RateDecay.tsx            ← 431 lines. % of Medicare (#/decay)
│   │   ├── RateBuilder.tsx          ← 499 lines. Rate calculator (#/builder)
│   │   ├── AheadReadiness.tsx       ← AHEAD Readiness Score (#/ahead-readiness)
│   │   │                              CCN lookup → 4 scored dimensions → self-report → peers
│   │   ├── CaseloadForecaster.tsx    ← ~830 lines. Caseload forecast + expenditure (#/forecast)
│   │   │                              Upload form, fan chart, model table, expenditure projection,
│   │   │                              MC/FFS breakdown bar. Tab toggle caseload↔expenditure.
│   │   ├── PolicyAnalyst.tsx        ← 378 lines. AI chat (#/analyst)
│   │   ├── ComplianceReport.tsx     ← (#/compliance)
│   │   ├── RateReductionAnalyzer.tsx ← (#/reduction)
│   │   ├── StateProfile.tsx          ← ~470 lines. State Profile (#/state, #/state/{code})
│   │   │                              18 parallel API fetches, 7 collapsible sections, hash-based deep linking
│   │   ├── HcbsCompTracker.tsx      ← (#/hcbs8020)
│   │   └── AheadCalculator.tsx      ← (#/ahead)
│   ├── engine/
│   │   └── StateRateEngine.js       ← 1,153 lines. 42/42 tests passing.
│   │                                   ⚠️ NOT YET WIRED INTO Rate Builder.
│   ├── utils/
│   │   ├── cpraPdf.ts               ← 239 lines. CPRA PDF export.
│   │   ├── cpraXlsx.ts              ← 173 lines. CPRA Excel export.
│   │   ├── pdfReport.ts             ← ~100 lines. Shared PDF utilities.
│   │   └── aheadScoring.ts         ← AHEAD scoring functions (4 dimensions + self-report + composite)
│   ├── lib/
│   │   ├── api.ts                   ← Shared API client (VITE_API_URL + fallback)
│   │   ├── duckdb.ts                ← ~110 lines. DuckDB-WASM singleton.
│   │   └── queryEngine.ts           ← ~200 lines. resolveHcpcsCodes(), pickTable().
│   └── data/
│       └── states.ts                ← STATE_NAMES, STATES_LIST
│
├── public/data/                     ← All JSON/Parquet served to frontend
│   ├── cpra_em.json                 ← 615KB. Primary CPRA data. (Terminal B)
│   ├── dq_flags_em.json             ← 79KB. DQ warnings. (Terminal B)
│   ├── dim_447_codes.json           ← 14KB. 74 E/M codes. (Terminal B)
│   ├── cpra_summary.json            ← 7KB. State aggregates. (Terminal B)
│   ├── cpra_precomputed.json        ← 384KB. ⚠️ DELETE — superseded by cpra_em.json
│   ├── hcpcs.json                   ← T-MSIS claims data
│   ├── states.json                  ← State aggregates
│   ├── trends.json                  ← Year-over-year trends
│   ├── medicare_rates.json          ← Medicare rates (may be incomplete)
│   ├── providers.json               ← Provider counts
│   ├── specialties.json             ← Specialty breakdown
│   ├── fee_schedules.json           ← State fee schedule rates (45 states)
│   ├── bls_wages.json               ← BLS wages
│   ├── quality_measures.json        ← CMS Core Set
│   ├── soc_hcpcs_crosswalk.json     ← SOC-HCPCS mapping
│   ├── conversion_factors.json      ← State methodology metadata
│   ├── system_prompt.md             ← AI tier system prompt
│   ├── fl_methodology_addendum.md   ← Loaded in api/chat.js system prompt
│   └── [external]                   ← claims_monthly.parquet (82MB) via VITE_MONTHLY_PARQUET_URL
│
├── api/
│   └── chat.js                      ← 515 lines. Vercel serverless. ⚠️ NO AUTH.
│
├── server/                          ← FastAPI backend (DuckDB over lake Parquet)
│   ├── main.py                      ← FastAPI app, lifespan hooks, router includes
│   ├── db.py                        ← In-memory DuckDB, registers lake Parquet as views
│   ├── config.py                    ← Settings (lake_dir, CORS, max_rows)
│   ├── query_builder.py             ← Parameterized SQL builder for /api/query
│   ├── Dockerfile                   ← Python 3.12, S3 sync on startup
│   ├── fly.toml                     ← Fly.io deployment config
│   ├── entrypoint.sh                ← Downloads lake from S3, starts uvicorn
│   ├── engines/
│   │   ├── cpra_upload.py           ← 821 lines. CPRA upload engine (68 codes, 171 pairs, DuckDB)
│   │   ├── caseload_forecast.py     ← ~650 lines. SARIMAX+ETS forecasting engine (template-driven upload)
│   │   └── expenditure_model.py     ← ~430 lines. Expenditure projection engine (cap rates + cost-per-eligible)
│   └── routes/
│       ├── query.py                 ← POST /api/query (backward-compatible spending queries)
│       ├── meta.py                  ← GET /api/meta (dataset metadata)
│       ├── presets.py               ← GET /api/presets
│       ├── cpra.py                  ← CPRA: pre-computed (/api/cpra/states, rates, dq, compare)
│       │                              + upload tool (/api/cpra/upload/generate, templates, etc.)
│       ├── lake.py                  ← /api/states, enrollment, quality, expenditure, lake/stats
│       ├── pharmacy.py              ← /api/pharmacy/utilization, nadac, top-drugs
│       ├── policy.py                ← /api/policy/spas, waivers, managed-care, fmap, dsh
│       ├── wages.py                 ← /api/wages/{state}, compare/{soc}, msa/{state}, national
│       ├── hospitals.py             ← /api/hospitals/{state}, summary, nursing-facilities/{state},
│       │                              ccn/{ccn}, ccn/{ccn}/peers (AHEAD Readiness)
│       ├── enrollment.py            ← /api/enrollment/eligibility, expansion, unwinding, mc-plans
│       ├── staffing.py              ← /api/staffing/summary, staffing/{state}
│       ├── quality.py              ← /api/five-star/, hac/, pos/, hospital-ratings/, vbp/, hrrp/, epsdt, hpsa/
│       ├── context.py             ← /api/demographics/, scorecard/, economic/, mortality/
│       ├── bulk.py                ← /api/bulk/* — 7 bulk endpoints matching frontend JSON shapes
│       ├── supplemental.py       ← /api/supplemental/* — 7 endpoints (FMR + MACPAC + DSH hospital + SDP)
│       ├── behavioral_health.py  ← /api/behavioral-health/*, /api/gme/*, rounds 2-8 data (109 endpoints)
│       ├── round9.py             ← /api/medicare/*, /api/opioid/*, /api/pharmacy/sdud-2024/* (22 endpoints)
│       ├── forecast.py           ← /api/forecast/* — caseload templates + generate + expenditure pipeline (10 endpoints)
│       └── pipeline.py              ← /api/pipeline/status
│
├── tools/
│   ├── ahead/                       ← AHEAD/Meridian calculator (standalone)
│   └── mfs_scraper/
│       ├── cpra_engine.py           ← 968 lines. Full CPRA pipeline. (Terminal B)
│       ├── cms_data.py              ← 8 CMS dataset scrapers
│       ├── ncci_scraper.py          ← NCCI, HCPCS, waivers, drug rebate
│       ├── export_data.py           ← JSON export for CMS supplemental data
│       ├── db_import.py             ← --import-cms flag, pg_dump
│       ├── schema.sql               ← 12 new tables for CMS data
│       └── aradune_cpra.duckdb      ← 1.79M rows. Terminal B analytical layer.
│
├── pipeline/
│   ├── dagster_pipeline.py          ← Dagster definitions: 6 assets, 3 checks, 3 jobs, 2 schedules
│   ├── __init__.py
│   ├── tmsis_pipeline_duckdb.R      ← 71KB. T-MSIS processing (227M rows).
│   ├── process_data.R               ← 15KB.
│   ├── hcpcs_reference.R            ← 16KB. HCPCS reference data processing.
│   └── tmsis_sample_generator.R     ← 18KB. Sample/dev data generation.
│
├── data/
│   ├── lake/                        ← Unified Parquet data lake (115M+ rows, 250 fact tables, 785MB)
│   │   ├── dimension/               ← 9 tables: dim_state, dim_procedure, dim_hcpcs, dim_bls_occupation,
│   │   │                              dim_medicare_locality, dim_time, dim_provider_taxonomy,
│   │   │                              dim_pace_organization, dim_scorecard_measure
│   │   ├── fact/                    ← 250 Hive-partitioned tables: fact/{name}/snapshot=YYYY-MM-DD/data.parquet
│   │   │                              Full list: see `ls data/lake/fact/` or `server/db.py` lines 41-148
│   │   ├── reference/               ← 9 tables: ref_drug_rebate, ref_ncci_edits, ref_1115_waivers,
│   │   │                              ref_poverty_guidelines, ref_presumptive_eligibility,
│   │   │                              ref_pediatric_drugs, ref_clotting_factor, + 2 more
│   │   └── metadata/                ← manifest_*.json (pipeline run metadata, ~25 manifests)
│   ├── reference/
│   │   └── cpra/                    ← em_codes.csv (68), code_categories.csv (171), GPCI2025.csv (109 localities)
│   ├── raw/
│   │   ├── medicaid-provider-spending.duckdb   ← 17.57GB. T-MSIS DuckDB. ⚠️ EMPTY — R pipeline must run first.
│   │   ├── medicaid-provider-spending.csv      ← 11.09GB. Source T-MSIS data.
│   │   ├── npidata_pfile_20050523-20260208.csv ← 11.21GB. Full NPPES registry.
│   │   ├── NPPES_Data_Dissemination_February_2026/ ← Extracted NPPES folder.
│   │   ├── NPPES_Data_Dissemination_February_2026.zip ← 1.11GB.
│   │   ├── PPRR VU2026_Jan_nonQPP.csv          ← 2.6MB. CY2026 Medicare PFS RVU file.
│   │   ├── Florida_2026 Practitioner Fee Schedule.xlsx          ← 274KB.
│   │   ├── Florida_2026 Practitioner Laboratory Fee Schedule.xlsx ← 72KB.
│   │   ├── medicaid_fee_schedule_directory_v2.xlsx ← 14KB. State fee schedule registry.
│   │   ├── hcpc2025_oct_anweb_v4.zip           ← 2.5MB. HCPCS Level II codes.
│   │   ├── bls_medicaid_occupations.csv        ← 2.2MB. BLS wage data.
│   │   ├── 2024-child-and-adult-core-quality-measures.csv ← 9.1MB.
│   │   ├── all_data_M_2024.xlsx                ← 81.6MB. (identify and document)
│   │   ├── data-02-25-2026-10_06am.csv         ← 33KB.
│   │   ├── EXHIBIT-14.-Medicaid-...-Status-FY-2023.xlsx ← 29KB.
│   │   ├── EXHIBIT-22.-Medicaid-...-Group-FY-2023.xlsx  ← 38KB.
│   │   └── dme26a/                             ← DME fee schedule folder.
│   ├── processed/                   ← Pipeline outputs (to build)
│   └── sources/                     ← State YAML registry (to build)
│
├── scripts/
│   ├── build_dimensions.py          ← Build dimension Parquet from SQLite + CPRA DuckDB
│   ├── build_facts.py               ← Build CPRA fact tables (medicaid_rate, rate_comparison, etc.)
│   ├── build_facts_tmsis.py         ← Ingest T-MSIS Parquet into the lake
│   ├── build_lake_cms.py            ← Migrate CMS supplemental data (SDUD, NADAC, SPAs, etc.)
│   ├── build_lake_bls.py            ← Ingest BLS OEWS wage data (16 Medicaid occupations)
│   ├── build_lake_hcris.py          ← Ingest HCRIS hospital + SNF cost reports
│   ├── build_lake_enrollment.py     ← Ingest Medicaid enrollment, unwinding, MC plan data
│   ├── build_lake_pbj.py            ← Ingest PBJ nursing facility staffing (65M+ rows)
│   ├── build_lake_quality.py        ← Ingest Five-Star, HAC, POS facility data (162K rows)
│   ├── build_lake_hospital_quality.py ← Ingest hospital ratings, VBP, HRRP, EPSDT (26K rows)
│   ├── build_lake_care_compare.py   ← Ingest Care Compare state-level quality (6 tables, 3.8K rows)
│   ├── build_lake_medicaid_supplemental.py ← MLTSS, Financial Mgmt, Eligibility, ACA FUL, DQ Atlas (2.3M rows)
│   ├── build_lake_economic.py       ← CPI, unemployment, median income, MSPB hospital (8.9K rows)
│   ├── build_lake_hpsa.py           ← HRSA HPSA designations (69K rows, 3 disciplines)
│   ├── build_lake_scorecard.py      ← Medicaid Scorecard + eligibility groups + HAI + MC enrollment (130K rows)
│   ├── build_lake_hai_ownership.py  ← HAI hospital infections + NH ownership (316K rows)
│   ├── build_lake_census.py         ← Census ACS demographics, poverty, insurance (52 states)
│   ├── build_lake_cdc.py            ← CDC drug overdose + mortality trends (13.6K rows)
│   ├── build_lake_supplemental.py   ← CMS-64 FMR + MACPAC Exhibit 24 supplemental payments (1,655 rows)
│   ├── build_lake_supplemental_p2.py ← Hospital-level DSH (6,103) + SDP preprint (34 states)
│   ├── build_lake_behavioral_health.py ← NSDUH, N-SUMHSS, IPF quality, BRFSS behavioral
│   ├── build_lake_gme_blockgrant.py ← GME/CMS PSF (68K providers) + MHBG block grants
│   ├── build_lake_chip_hcbs.py      ← CHIP eligibility + continuous eligibility policies
│   ├── build_lake_fee_schedules_computed.py ← AK/MI/NM RBRVS-computed fee schedules
│   ├── build_lake_providers_demographics.py ← Provider demographics + facility data
│   ├── build_lake_round2.py         ← Hospice, maternal health, ASC, home health, HCBS
│   ├── build_lake_round2b.py        ← CMS-372 waivers, MC enrollment by plan, MLTSS
│   ├── build_lake_round2c.py        ← MC enrollment by population, PACE directory
│   ├── build_lake_round3.py         ← BH by condition, dental, telehealth, IRF, LTCH, HHA
│   ├── build_lake_round3b.py        ← MC share, MC monthly, dialysis facility, IPF facility
│   ├── build_lake_round3c.py        ← Hospital directory, MC programs
│   ├── build_lake_round4.py         ← CHIP unwinding, medicaid applications, vaccinations
│   ├── build_lake_round5.py         ← Dual status, benefit package, NAS rates, SMM extended
│   ├── build_lake_round6.py         ← HAI hospital2, complications/timely/unplanned hosp-level
│   ├── build_lake_round7.py         ← SNF VBP/quality, FQHC directory, vital stats, HHCAHPS
│   ├── build_lake_round8.py         ← Hospice directory/CAHPS, Medicare spending, VHA providers
│   ├── build_lake_round9.py         ← Medicare enrollment, opioid, SDUD 2024, drug rebate, CMS impact
│   ├── build_lake_round10.py        ← SDUD 2025, Core Set 2023/2024, HCBS waitlist, eligibility processing
│   ├── build_lake_round11.py        ← LTSS, vital stats monthly, maternal mortality, FMR FY2024, NSDUH 2024
│   ├── build_lake_round12.py        ← SAIPE poverty, CDC PLACES, health center sites, marketplace OEP, MUA, workforce, food environment, Medicare telehealth/geo variation, drug spending, NHE
│   ├── build_lake_round13.py        ← MSSP/ACO, Part D, NHSC, FQHC hypertension/badges
│   ├── export_frontend.py           ← Export validated lake data to public/data/ JSON
│   ├── sync_lake.py                 ← Upload/download lake to/from S3
│   ├── sync-fee-schedules.py
│   ├── update-rvu.mjs
│   ├── build_reference_data.py
│   └── build-cpra-data.mjs          ← ⚠️ Superseded by cpra_engine.py — delete
│
└── docs/
    ├── ARADUNE_MASTER.md            ← Full strategy/architecture reference
    ├── ARADUNE_PRODUCT_STRATEGY.md
    ├── ARADUNE_VISION.md            ← T-MSIS resource library, verified URLs
    ├── aradune-implementation-plan.md
    ├── aradune-market-gaps.md
    ├── TMSIS_DATA_GUIDE.md
    ├── UX_FEATURES_SPEC.md          ← Full implementation spec: glossary, search, state profiles
    └── AraduneMockup.jsx            ← DEFINITIVE landing page + nav reference
```

├── .github/workflows/
│   └── ci.yml                       ← GitHub Actions: build, lint, deploy to Vercel + Fly.io

### Brand Assets

All assets are transparent PNGs — green elements on transparent background. Render correctly on any surface.

| File | Path | Usage |
|------|------|-------|
| `logo-full.png` | `public/assets/logo-full.png` | Primary logo (A lettermark + ARADUNE wordmark stacked). Navbar top-left. |
| `logo-mark.png` | `public/assets/logo-mark.png` | A lettermark alone. Favicon, small spaces, app icon. |
| `logo-wordmark.png` | `public/assets/logo-wordmark.png` | ARADUNE text only. PDF report headers, tight horizontal spaces. |
| `icon-bot.png` | `public/assets/icon-bot.png` | Winged helmet. Policy Analyst AI chat avatar (32×32, border-radius 50%). |

**Brand colors:** `#0A2540` ink · `#2E6B4A` brand green · `#C4590A` accent · `#F5F7F5` surface.

**Files to delete:**
- ~~`public/data/cpra_precomputed.json`~~ — **Deleted**
- `docs/HANDOFF_CLAUDE_CODE.md` — merged into CLAUDE.md
- `docs/TERMINAL_A_HANDOFF.md` — merged into CLAUDE.md
- `scripts/build-cpra-data.mjs` — superseded by cpra_engine.py

## 10. Known Policy Rules (Always Enforce)

- **FL Medicaid: rates cannot have both a facility rate AND a PC/TC split.** Codes requiring special handling: **46924, 91124, 91125.**
- **FL production conversion factors:** Regular `$24.9779582769` · Lab `$26.1689186096`. The ad hoc CF of $24.9876 is stale — do not use for CY2026.
- **FL has 8 schedule types** in the fee schedule.
- **Medicare comparison baseline:** Always use the non-facility rate (not facility), per 42 CFR 447.203.
- **Medicare conversion factors:** `$33.4009` (CY2026, non-QPP) for general fee-to-Medicare comparison. `$32.3465` (CY2025, non-QPP) for CPRA compliance (July 2026 deadline compares CY2025 rates). Both are correct for their respective uses.
- **CPRA base rates only:** Do not include supplemental payments in the Medicaid-to-Medicare percentage.
- **CHIP excluded** from per-enrollee Medicaid calculations.
- **Minimum cell size:** n ≥ 11 for any published utilization count.

---

## 11. Data Universe

### Ring 0: Public Regulatory Data (No HIPAA, no DUA — build here first)

| Dataset | Source | Format | Cadence | Priority | Status |
|---|---|---|---|---|---|
| State Medicaid fee schedules (all 51) | State agency websites | CSV/XLSX/PDF | Annual/quarterly | **P0** | **47 states** (597K rows). 4 remaining: KS/NJ (portal login), TN (MC only), WI (manual). |
| Medicare Physician Fee Schedule | cms.gov PFS RVU files | ZIP/CSV | Annual + quarterly | **P0** | **✓** 16,978 codes, 858K locality rates, 417K state rates |
| T-MSIS HHS open data (227M rows) | HHS/Hugging Face | Parquet | Done (Feb 2026) | **P0** | **✓** Ingested into lake |
| NPPES NPI Registry | download.cms.gov | CSV | Weekly | **P0** | **✓** 11.2GB raw file downloaded |
| Medicaid Provider Enrollment Files | State portals | State portals | Annual | **P0** | Not started |
| CMS-64 expenditure reports | medicaid.gov | Excel/CSV | Quarterly | **P1** | **✓** fact_expenditure + fact_fmr_supplemental |
| MBES/CBES enrollment/expenditure | medicaid.gov | Excel | Quarterly | **P1** | **Partial** — enrollment data ingested |
| HCRIS hospital cost reports | cms.gov | CSV | Quarterly (2–4yr lag) | **P1** | **✓** fact_hospital_cost + fact_dsh_hospital (6,103 hospitals) |
| Provider of Services (POS) File | cms.gov | CSV | Quarterly | **P1** | **✓** fact_pos_hospital + fact_pos_other |
| NADAC pharmacy pricing | medicaid.gov | CSV | Weekly | **P1** | **✓** fact_nadac |
| State Drug Utilization Data (SDUD) | data.medicaid.gov | CSV + API | Quarterly | **P1** | **✓** fact_drug_utilization |
| FMAP rates | medicaid.gov/kff.org | Web | Annual | **P1** | **✓** fact_fmap |
| Adult/Child Core Set quality measures | medicaid.gov | Web/Excel | Annual | **P1** | **✓** fact_quality_measure |
| MACStats / MACPAC Exhibits | macpac.gov | PDF/Excel | Annual | **P2** | **✓** fact_macpac_supplemental (Exhibit 24) |

### AHEAD Readiness Score — HCRIS Field Map

```
Operating/total margin   → net_income / net_patient_revenue (HCRIS Worksheet G)
Current ratio            → total_assets / total_liabilities (HCRIS Worksheet G)
Cost-to-charge ratio     → cost_to_charge_ratio (HCRIS Worksheet D-1/D-4)
Payer mix (days)         → medicare_days, medicaid_days, total_days (HCRIS Worksheet S-3)
Inpatient/outpatient rev → inpatient_revenue, outpatient_revenue (HCRIS Worksheet G-3)
Uncompensated care       → uncompensated_care_cost (HCRIS Worksheet S-10)
Medicare DSH             → dsh_adjustment, dsh_pct (HCRIS Worksheet E)
IME payment              → ime_payment (HCRIS Worksheet E)
Discharges, bed count    → total_discharges, bed_count (HCRIS Worksheet S-3)
```

**AHEAD Readiness Score — Data Gaps (as of March 2026):**
1. **Medicaid UPL/SDP** — Not available at hospital level. Only state-aggregate in CMS-64.
2. **Maryland AHEAD peer benchmarks** — No public dataset. Static MedPAC/MACPAC benchmarks as placeholder.
3. **Medicaid FFS vs MC split** — HCRIS S-3 reports combined Medicaid days. Estimate from MBES/CBES penetration rate.
4. **Service line margins** — Internal data. Self-report unlock only.
5. **3-year trends** — HCRIS only has FY2023 in lake. Multi-year would need historical HCRIS downloads.
6. **Days cash on hand, DSCR, days AR** — Not directly in HCRIS extract. Derived from balance sheet where possible.

### Supplemental Payment Programs ← PARTIALLY ADDRESSED

For safety net hospitals, supplemental payments can exceed base rates by 2–5x. All Ring 0.

| Dataset | Program | Source | Cadence | Priority |
|---|---|---|---|---|
| CMS DSH Allotment Reports | DSH | medicaid.gov | Annual | **P1** | **✓** fact_dsh_payment + fact_dsh_hospital |
| CMS-64 Schedule A/B | UPL/DSH | medicaid.gov | Quarterly | **P1** | **✓** fact_fmr_supplemental (state-level) |
| UPL Demonstration filings (state SPAs) | UPL/IGT/CPE | CMS MACPro | Ongoing | **P1** | Not started |
| State Directed Payment filings (42 CFR 438.6(c)) | SDP | CMS | Annual | **P1** | **✓** fact_sdp_preprint (34 states, curated index) |
| HRSA GME payment data | GME (direct + indirect) | hrsa.gov | Annual | **P1** | Not started |
| 1115 waiver financial terms | LIP/DSRIP/UC pools | medicaid.gov | Ongoing | **P1** | **Partial** — ref_1115_waivers (647 waivers) |
| MACPAC supplemental payment reports | All programs | macpac.gov | Annual | **P2** | **✓** fact_macpac_supplemental |
| OIG DSH audit reports | DSH | oig.hhs.gov | Ongoing | **P2** | Not started |

**Programs to model:** DSH (disproportionate share) · UPL/IGT/CPE (upper payment limit + intergovernmental transfers) · State Directed Payments (managed care) · LIP (Low Income Pool, FL + others) · DSRIP · GME direct + indirect · Uncompensated Care pools. The "all-in Medicaid rate" = base rate + all supplemental programs. No platform shows this today.

### LTSS / HCBS ← PARTIALLY ADDRESSED (~40% of Medicaid spending)

| Dataset | Source | Cadence | Priority | Status |
|---|---|---|---|---|
| CMS-64 Schedule B (HCBS expenditure by waiver) | medicaid.gov | Quarterly | **P1** | Not started |
| 1915(c) Waiver Utilization & Expenditure | CMS waiver reports | Annual | **P1** | Not started |
| HCBS Quality Measures (CMS national framework) | medicaid.gov | Annual | **P1** | Not started |
| HCBS Waitlist Data (700K+ people waiting nationally) | KFF / state reports | Annual | **P1** | **✓** fact_hcbs_waitlist (51 states, 606K, KFF 2025) |
| Nursing Facility Cost Reports (CMS-2540) | cms.gov | Annual | **P1** | **✓** fact_snf_cost |
| Five-Star Quality Rating (Care Compare, NF) | cms.gov API | Monthly | **P1** | **✓** fact_five_star |
| Payroll-Based Journal (PBJ) NF staffing | cms.gov | Quarterly | **P1** | **✓** fact_pbj_nurse/nonnurse/employee (65M+ rows) |
| Direct Support Workforce data (wages, vacancy, turnover) | PHI / ANCOR | Annual | **P1** | Not started |
| MDS facility-level aggregates | cms.gov | Quarterly | **P2** | **✓** fact_mds_quality (250K rows) |
| 1915(k) Community First Choice utilization | CMS reports | Annual | **P2** | Not started |
| PACE enrollment & spending | CMS reports | Annual | **P2** | **Partial** — dim_pace_organization (201 orgs), fact_mc_enrollment_plan has PACE |
| CMS-372 Waiver data | CMS reports | Annual | **P2** | **✓** fact_cms372_waiver (553 programs, $55.75B) |
| HCBS authority measures | Medicaid Scorecard | Annual | **P2** | **✓** fact_hcbs_authority (59 measures) |

### Hospital Quality & Value-Based Programs ← DONE

| Dataset | Source | Cadence | Priority | Status |
|---|---|---|---|---|
| Care Compare — hospital ratings | cms.gov API | Quarterly | **P1** | **✓** fact_hospital_rating |
| Inpatient Quality Reporting (IQR) | cms.gov API | Quarterly | **P1** | **✓** (via Care Compare) |
| Hospital Value-Based Purchasing (VBP) scores | cms.gov | Annual | **P1** | **✓** fact_hospital_vbp |
| Hospital Readmissions Reduction Program (HRRP) | cms.gov | Annual | **P1** | **✓** fact_hospital_hrrp |
| Hospital-Acquired Condition (HAC) Reduction | cms.gov | Annual | **P2** | **✓** fact_hac_measure |

### Behavioral Health ← SUBSTANTIALLY ADDRESSED

| Dataset | Source | Cadence | Priority | Status |
|---|---|---|---|---|
| SAMHSA NSDUH (MH/SUD prevalence by state) | samhsa.gov | Annual | **P1** | **✓** fact_nsduh_prevalence (5,865 rows, 2023-2024) |
| SAMHSA Block Grant expenditure reports | samhsa.gov | Annual | **P1** | **✓** fact_block_grant (55 state allotments, $0.95B) |
| Psychiatric bed capacity by state | samhsa.gov | Annual | **P1** | **✓** fact_mh_facility (27,957 MH/SUD treatment facilities) |
| IPF Quality (psychiatric facility quality) | cms.gov | Annual | **P1** | **✓** fact_ipf_quality_state + fact_ipf_quality_facility (1,474 rows) |
| BH by condition (T-MSIS) | CMS | Annual | **P1** | **✓** fact_bh_by_condition (4,240 rows, 16 conditions) |
| MH/SUD service recipients | CMS | Annual | **P1** | **✓** fact_mh_sud_recipients (216 rows, 2020-2022) |
| Physical conditions among BH beneficiaries | CMS | Annual | **P1** | **✓** fact_physical_among_mh + fact_physical_among_sud (11,130 rows) |
| 1115 IMD waiver utilization | CMS reports | Annual | **P1** | Not started |
| BH-specific HRSA HPSA designations | hrsa.gov API | Ongoing | **P1** | **✓** fact_hpsa (69K rows, 3 disciplines incl. MH) |

### Children's Health / CHIP / EPSDT ← SUBSTANTIALLY ADDRESSED

| Dataset | Source | Cadence | Priority | Status |
|---|---|---|---|---|
| CHIP enrollment & expenditure (separate from Medicaid) | CMS/MBES | Quarterly | **P1** | **✓** fact_chip_enrollment, fact_chip_monthly, fact_chip_program_monthly/annual, fact_chip_app_elig, fact_chip_enrollment_unwinding |
| CHIP eligibility thresholds | medicaid.gov | Annual | **P1** | **✓** fact_chip_eligibility (51 states, income thresholds by age) |
| EPSDT Participation Reports (CMS-416) | medicaid.gov | Annual | **P1** | **✓** fact_epsdt |
| Children's Core Set measures | medicaid.gov | Annual | **P1** | **✓** fact_quality_measure |
| Well-child visits | CMS | Annual | **P1** | **✓** fact_well_child_visits |
| Blood lead screening | CMS | Annual | **P1** | **✓** fact_blood_lead_screening |
| Vaccinations | CMS | Annual | **P1** | **✓** fact_vaccinations |

### Eligibility & Unwinding

| Dataset | Source | Cadence | Priority | Status |
|---|---|---|---|---|
| Medicaid unwinding / redetermination outcomes | CMS dashboard | Monthly | **P1** | **✓** fact_unwinding |
| KFF Medicaid eligibility policy tracker | kff.org | Ongoing | **P1** | **✓** fact_eligibility_levels |

### Pharmacy (Deeper)

| Dataset | Source | Cadence | Priority | Status |
|---|---|---|---|---|
| NADAC | medicaid.gov | Weekly | **P1** | **✓** fact_nadac |
| SDUD | data.medicaid.gov | Quarterly | **P1** | **✓** fact_drug_utilization |
| 340B covered entity data | hrsa.gov | Quarterly | **P1** | Blocked — needs browser (Blazor app at 340bopais.hrsa.gov/Reports) |
| State MAC prices | State portals | Varies | **P2** | Not started |

### Ring 0.5: Economic & Contextual Data (Essential for Forecasting)

| Dataset | Source | Use Case | Cadence |
|---|---|---|---|
| State unemployment rates (LAUS) | bls.gov | Primary caseload driver | Monthly |
| BLS CPI (medical care, all items) | bls.gov | Rate decay / real-value analysis | Monthly |
| BLS OEWS (healthcare occupation wages) | bls.gov | Wage adequacy analysis | Annual |
| Census/ACS demographics | census.gov | Population denominators, poverty rates | Annual |
| BEA GDP by state | bea.gov | Economic context for forecasting | Quarterly |
| FRED economic series | fred.stlouisfed.org | Poverty, income, enrollment correlates | Monthly |
| SNAP/TANF enrollment | fns.usda.gov / ACF | Cross-program enrollment correlation | Monthly | **✓** |
| Federal poverty guidelines | ASPE/HHS | Eligibility threshold context | Annual |
| Housing cost indices (HUD) | hud.gov | Cost-of-living / HCBS wage adequacy | Annual | **✓** fact_fair_market_rent (4,764 counties) |
| Maternal/infant mortality | CDC WONDER | Outcome context for rate adequacy | Annual |
| Opioid prescribing / overdose rates | CDC | SUD service demand forecasting | Annual |
| State revenue/budget data | NASBO | State fiscal capacity for match | Annual |

### Ring 1–3 (HIPAA sensitivity increases)
- **Ring 1:** Aggregated/de-identified — state-published utilization counts, HHS open data
- **Ring 2:** Provider-level data — may need BAA; build when state relationships develop
- **Ring 3:** Claims/encounter data — full HIPAA; only when BAA + HITRUST in place


## 12. T-MSIS Data Quality Rules

**Non-negotiable for every feature, pipeline, or analysis that touches T-MSIS data. See also: `docs/TMSIS_DATA_GUIDE.md` for full field-level detail.**

### What the data is and isn't
- **227M rows ingested** = T-MSIS OT (Other Services) file only — physician, outpatient, clinic, HCBS claims
- **Excludes inpatient (IP), long-term care (LT), pharmacy (RX)** — those files are not ingested yet
- **No real-time data.** Preliminary TAF: ~12–18 month lag. Final TAF: ~24 months. Never describe T-MSIS as "current."
- **2024 data is incomplete.** Sharp Nov–Dec 2024 dropoff in DOGE dataset — flag in every output using 2024
- **T-MSIS DuckDB (17.5GB) is currently empty** — R pipeline (`tmsis_pipeline_duckdb.R`) must run to populate it
- **10 of 20 largest "providers"** in the DOGE dataset are state/local government agencies, not healthcare providers

### 12 non-negotiable rules

1. **Always specify the service year.** Never say "current" — say "CY2022 T-MSIS data" or "based on 2022 claims."
2. **Check DQ Atlas before using any state.** medicaid.gov/dq-atlas. Flag states with "Unusable" or "High Concern" for the relevant topic. Do not suppress this warning.
3. **Always apply OT claims filters:**
   ```sql
   WHERE MDCD_PD_AMT > 0                          -- exclude $0 / denied
   AND MDCD_PD_AMT < 50000                         -- exclude obvious errors
   AND ADJSTMT_IND NOT IN ('1', 'V')               -- exclude voids/adjustments
   AND CLM_STUS_CTGRY_CD IN ('F1', 'F2', 'F3')    -- paid claims only
   AND PRCDR_CD IS NOT NULL                         -- require procedure code
   AND SRVC_BGNG_DT IS NOT NULL                    -- require service date
   ```
4. **Validate NPIs:** 10 digits, not null, not '0000000000'. Use `SRVC_PRVDR_NPI` (rendering) over `BLNG_PRVDR_NPI` (billing) for rate analysis.
5. **Separate FFS from encounter claims** using `CLM_TYPE_CD` before any payment analysis. Encounter amounts are unreliable — many states submit $0 or capitation rate, not actual payment.
6. **Surface MCO penetration context** alongside every utilization metric. In highly managed care states, FFS-only data systematically undercounts utilization.
7. **Use ASPE HCBS taxonomy** for HCBS classification (procedure + revenue codes + bill type in combination). Do NOT rely on `BNFT_TYPE_CD` alone — it's sparsely populated.
8. **Never imply T-MSIS captures MCO-to-provider payment rates.** It does not. Encounter amounts are the capitation rate, not the service-level payment.
9. **Use SCD Type 2 logic** for fee schedule temporal joins — match service date to the rate in effect at that time.
10. **Document data vintage in every output** — service year, TAF release (preliminary/final), and DQ Atlas rating for the topic and state.
11. **Minimum cell size: n ≥ 11** for any published utilization count. Never publish smaller.
12. **Do not mix MAX and TAF** in the same time series without a formal crosswalk. MAX (pre-2014) and TAF use different field names, enrollment groups, and payment definitions.

### Key field-level pitfalls (OT file)

| Field | Issue |
|-------|-------|
| `SRVC_BGNG_DT` | Missing in 5–15% of records; some states submit claim date instead |
| `PRCDR_CD` | Missing for HCBS, transportation, dental in some states |
| `MDCD_PD_AMT` | Includes $0 (denied), negatives (adjustments), very high values (errors) |
| `SRVC_PLC_CD` | Needed to distinguish office vs. facility rates; sometimes missing |
| `RNDG_PRVDR_NPI` | Preferred; sometimes missing — fall back to `BLNG_PRVDR_NPI` |
| `BILL_TYPE_CD` | Inconsistent across states; needed to distinguish professional vs. institutional |
| `BNFT_TYPE_CD` | States populate inconsistently — don't classify HCBS from this alone |

### What T-MSIS cannot answer

| Question | Better source |
|----------|--------------|
| What did MCOs actually pay providers? | Hospital price transparency MRFs; managed care contracts |
| Current-year utilization trends? | State MMIS (not public) |
| Provider charges? | HCRIS cost reports (facilities) |
| HCBS worker wages? | BLS OES; PHI workforce data |
| Actual Medicaid underpayment per hospital? | HCRIS Worksheet D-1/D-4 + Worksheet S-2 |

### Per-source data quality gates (all sources, not just T-MSIS)

**Rate validity (fee schedules):**
- Flag $0.00 rates — distinguish non-covered from data error
- Flag rates above $10,000 for a single E/M code
- Flag rates unchanged for 24+ months (stale detection)
- Cross-check: if `|medicaid_rate - (total_rvu × $33.4009)| / medicaid_rate > 2.0` → flag for review

**Code coverage:**
- Verify state covers expected E/M codes from the CMS 447.203 list
- Identify codes in Medicare PFS but missing from state schedule
- Track annual CPT additions/deletions against CMS PFS updates

**Medicare matching:**
- Validate locality-to-state mapping
- Confirm conversion factor = $33.4009 (non-QPP) for comparison year
- Flag multi-locality states where weighting methodology isn't documented

**Modifier consistency:**
- Flag states where 26/TC splits are inconsistent
- Ensure modifier adjustments don't produce rates exceeding global rate
- FL rule: rates cannot have both facility rate AND PC/TC split (codes: 46924, 91124, 91125)

**Cross-state validation:**
- Flag states where average primary care rates differ >3 SDs from national mean
- Compare computed Medicaid-to-Medicare ratios against KFF/MACPAC benchmarks
- Flag impossible patterns (all codes at exactly same %-of-Medicare may indicate uniform CF error)

**CPRA-specific outlier states requiring investigation:**
- SD: median 5.83% MCR — likely per-15-min vs per-hour unit mismatch
- DC: avg 40.32% with 45K+ codes — possibly non-E/M codes included
- CT/KY/RI: averages 666%/341%/418% — likely conversion factor or unit mismatch


## 13. Data Ingestion Pipeline

### Core Pattern (every source implements all five steps)
```python
def fetch_raw(source_config) -> bytes | Path:
    """Use HTTP HEAD + ETag/Last-Modified for change detection."""

def parse(raw) -> list[dict]:
    """Per-source parsers. PDF → pdfplumber → Claude API normalization."""

def validate(parsed) -> ValidationResult:
    """Hard stops vs. soft flags."""

def normalize(validated) -> list[dict]:
    """Map to unified schema. URL + download date on every record."""

def load(normalized, db_conn) -> LoadResult:
    """Upsert with version tracking. Create S3 snapshot. Record hash."""
```

### Validation Gates
**Hard stops:** Rate changed >90% · Code count dropped >20% · Schema mismatch
**Soft flags:** Rate unchanged >24 months · New codes without description · Rate >3 SDs from national mean

### Operational Controls
```yaml
# data/pipeline_config.yaml
pipeline_paused: false
sources:
  FL_fee_schedule:
    paused: false
```
- Rollback: `aradune rollback --source FL_fee_schedule --version 2026-01-15`
- Snapshots: `s3://aradune-datalake/snapshots/{source}/{YYYY-MM-DD}/`
- Forecasting models: **never deleted** — always append, track performance over time

### Scheduling
| Source | Frequency |
|--------|-----------|
| NPPES NPI Registry | Weekly |
| NADAC Pharmacy | Weekly |
| Federal Register / CIBs / SHOs | Continuous |
| SPAs / Waivers | Continuous |
| BLS Unemployment, CPI, FRED | Monthly |
| Managed Care Enrollment | Monthly |
| T-MSIS / SDUD / MBES-CBES | Quarterly |
| Medicare PFS RVU | Annual + quarterly |
| State Fee Schedules | Annual (change-detected) |
| HCRIS Cost Reports, BLS OEWS, ACS | Annual |

---

## 14. Database Schema (Core Tables)

```sql
CREATE TABLE dim_state (
    state_code VARCHAR(2) PRIMARY KEY, state_name VARCHAR, region VARCHAR,
    expansion_status BOOLEAN, fmap DECIMAL(5,4),
    managed_care_model VARCHAR, medicaid_agency_name VARCHAR, agency_url VARCHAR
);

CREATE TABLE dim_procedure (
    cpt_hcpcs_code VARCHAR(10) PRIMARY KEY, description VARCHAR,
    category_447 VARCHAR, betos_code VARCHAR, is_em_code BOOLEAN,
    work_rvu DECIMAL(8,4), nonfac_pe_rvu DECIMAL(8,4),
    fac_pe_rvu DECIMAL(8,4), mp_rvu DECIMAL(8,4), status_indicator VARCHAR(2)
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

CREATE TABLE economic_indicators (
    state_code VARCHAR(2), indicator_name VARCHAR, reference_area VARCHAR,
    period_date DATE, value DECIMAL(12,4), source VARCHAR,
    PRIMARY KEY (state_code, indicator_name, reference_area, period_date)
);

CREATE TABLE forecast_enrollment (
    state_code VARCHAR(2), eligibility_group VARCHAR,
    forecast_date DATE, run_date DATE, model_id VARCHAR,
    point_estimate INTEGER, lower_80 INTEGER, upper_80 INTEGER,
    lower_95 INTEGER, upper_95 INTEGER,
    PRIMARY KEY (state_code, eligibility_group, forecast_date, model_id)
);

CREATE TABLE model_performance (
    model_id VARCHAR, state_code VARCHAR(2), target_variable VARCHAR,
    forecast_horizon_months INTEGER, evaluation_date DATE,
    mape DECIMAL(8,4), rmse DECIMAL(15,2), bias DECIMAL(15,2), n_observations INTEGER,
    PRIMARY KEY (model_id, state_code, target_variable, forecast_horizon_months, evaluation_date)
);

CREATE TABLE ingestion_log (
    id SERIAL PRIMARY KEY, source_id VARCHAR, run_timestamp TIMESTAMP,
    rows_loaded INTEGER, content_hash VARCHAR, version_label VARCHAR,
    status VARCHAR, validation_flags TEXT, snapshot_path VARCHAR
);

CREATE TABLE pipeline_alerts (
    id SERIAL PRIMARY KEY, source_id VARCHAR,
    alert_type VARCHAR, alert_message TEXT,
    run_timestamp TIMESTAMP, resolved BOOLEAN
);
```

**DuckDB notes:** `DECIMAL(10,2)` for rates (never FLOAT) · `DATE` for dates (never string) · Skip PKs during bulk load · DuckDB-WASM for browser queries <5M rows.

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
| NL2SQL, RAG, standard analysis | claude-sonnet-4-6 | ~$0.03–0.06 |
| SPA drafting, CPRA narrative, AHEAD | claude-opus-4-6 | ~$0.28 |

Use **prompt caching** (90% input cost reduction) and **batch API** (50% discount for non-real-time).

---

## 16. Caseload & Expenditure Forecasting

### Phase 1 — DONE: Template-Driven Caseload Forecasting

**Engine:** `server/engines/caseload_forecast.py` (~650 lines)
**Frontend:** `src/tools/CaseloadForecaster.tsx` (~830 lines)
**API:** `server/routes/forecast.py` (10 endpoints total — 6 caseload + 4 expenditure)
**Dependencies:** `statsmodels>=0.14.0`, `pmdarima>=2.0.0`, `pandas>=2.1.0`, `numpy>=1.26.0`

**How it works:**
1. User downloads CSV templates (caseload + optional events + optional expenditure params)
2. Fills in monthly enrollment by category (e.g., SSI Aged, TANF Children, MMA Managed Care)
3. Uploads → engine runs SARIMAX + ETS model competition per category
4. Returns per-category forecasts with 80/95% confidence intervals, model metadata, intervention effects

**Frontend UI (`/#/forecast`):**
- Upload form: state selector, horizon (12-60 months), caseload CSV, events CSV (optional), expenditure params CSV (optional)
- Caseload view: summary card, category pills, fan chart with CI bands, event markers, intervention effects, model comparison table
- Expenditure view: summary card (total/MC/FFS), expenditure fan chart, per-category table (type, base rate, trend, admin, risk, total), MC vs FFS breakdown bar
- Tab toggle between caseload and expenditure views when both exist

**Key class:** `CaseloadForecaster` with methods:
- `load_caseload_bytes(content)` — validates CSV (min 24 months, required columns)
- `load_events_bytes(content)` — optional structural events (MC launches, eligibility changes)
- `load_economic_data(db_cursor)` — enriches with Aradune's public unemployment data
- `forecast(horizon_months, include_seasonality, include_economic)` — runs model competition

**Model competition:**
- Tests multiple SARIMAX orders with intervention variables (step functions for PHE, unwinding, etc.)
- Tests ETS (exponential smoothing) as baseline
- Picks best model per category by AIC, validates on holdout MAPE
- Falls back to naive (last-12-month average) if all models fail

**Known events (built-in):** COVID-19 PHE start (2020-03), PHE unwinding start (2023-04), unwinding peak (2023-07)
**User events:** MC launches, eligibility changes, with affected categories and magnitude

**Caseload API endpoints:**
- `GET /api/forecast/templates/caseload` — blank CSV template (9 FL-style categories)
- `GET /api/forecast/templates/events` — events template with examples
- `POST /api/forecast/generate` — upload → forecast JSON
- `POST /api/forecast/generate/csv` — upload → forecast CSV download
- `GET /api/forecast/public-enrollment` — Aradune's public enrollment by state
- `GET /api/forecast/public-enrollment/by-group` — enrollment by eligibility group

**Tested:** Synthetic FL data (8 categories, 96 months, 2016-2024). All categories selected SARIMAX with <1% MAPE. Meaningful intervention effects detected (unwinding: MMA -248K, TANF Children -265K).

### Phase 2 — DONE: Expenditure Modeling

**Engine:** `server/engines/expenditure_model.py` (~430 lines)
**Class:** `ExpenditureModeler` — takes caseload forecast output + expenditure parameters CSV

**How it works:**
1. Runs caseload forecast (Phase 1) to get per-category enrollment projections
2. User uploads expenditure parameters CSV: cap_rate_pmpm (MC) or cost_per_eligible (FFS) per category
3. Applies annual trend (compound monthly), admin load, risk margin, policy adjustments
4. Returns per-category and aggregate expenditure projections with CI bands

**Expenditure API endpoints:**
- `GET /api/forecast/templates/expenditure-params` — blank params CSV template (8 FL-style categories)
- `POST /api/forecast/expenditure` — full pipeline: caseload + params → forecast + expenditure JSON
- `POST /api/forecast/expenditure/csv` — → expenditure CSV download
- `POST /api/forecast/expenditure-only` — apply params to existing forecast CSV (skip re-forecasting)

**Key data classes:**
- `CategoryExpenditure`: per-category projection with base_rate, trend, admin_load, risk_margin, monthly projections with CI
- `ExpenditureResult`: aggregate with total_projected, total_mc_projected, total_ffs_projected, per-category list

**Verified:** FL with 2M MC enrollees × $850 PMPM = ~$23.4B/yr MC + 300K FFS × $1,200/eligible = ~$4.5B/yr FFS = ~$27.9B/yr total.

### Phase 3 — FUTURE: Scenario Builder

- "What if unemployment rises 2 percentage points?"
- "What if we launch a new MC program covering TANF Adults?"
- "What if we expand eligibility to 138% FPL?"
- Hypothetical intervention variables injected into existing model

### Phase 4 — FUTURE: ML Models + Ensemble

- Gradient boosting / random forest with public performance tracking
- Model leaderboard: best algorithm per state over rolling 12/24/36-month windows
- Ensemble forecasts combining SARIMAX + ML predictions

Schema tables: `forecast_enrollment`, `forecast_expenditure`, `model_performance`, `economic_indicators` — see Section 14.

---

## 16b. Security & HIPAA Architecture

**This is non-negotiable. Build it right the first time.**

### Data sensitivity rings (always follow)

| Ring | Data | HIPAA | Current Aradune |
|------|------|-------|-----------------|
| **Ring 0** | Public regulatory: fee schedules, RVUs, SPAs, waivers, provider directories | None | ✅ Here now |
| **Ring 0.5** | Economic/contextual: BLS, FRED, Census — informs Medicaid, isn't Medicaid | None | ✅ Here now |
| **Ring 1** | Aggregated/de-identified: DOGE open data, state-published utilization counts | Minimal — verify de-identification | ✅ Here now |
| **Ring 2** | Provider-level: billing volumes, network participation (no patient info) | Low — may need BAA | When state relationships develop |
| **Ring 3** | Claims/encounter data: T-MSIS/TAF, state claims warehouses | Full HIPAA | Only after BAA + HITRUST in place |

**Stay in Ring 0/0.5/1 until BAA infrastructure, SOC 2 Type II, and HITRUST CSF certification are in place. Never enter Ring 3 without explicit authorization.**

### Technical controls (apply when any HIPAA-adjacent data is handled)

**Encryption:**
- AES-256 at rest for any stored data
- TLS 1.2+ in transit (enforce HTTPS everywhere)
- No plaintext secrets anywhere — use environment variables, never hardcode

**Access controls:**
- Role-based access control (RBAC) — principle of least privilege
- MFA required for any admin or data access
- Provider A must never be able to see Provider B's data (strict tenant isolation)
- Session tokens expire; no permanent API keys with broad access

**Audit logging:**
- Immutable logs of all data access
- Retain per state contract requirements (typically 6–10 years)
- Log: who accessed what, when, from where, what query was run
- Pipeline runs logged to `ingestion_log` table

**Secrets management:**
- Required env vars in Vercel: `ANTHROPIC_API_KEY`, `VITE_MONTHLY_PARQUET_URL`
- Never commit API keys or credentials to git
- Rotate keys if any accidental exposure occurs

**Data isolation:**
- User-uploaded data is session-scoped by default — not persisted, not shared
- Persistent private workspace: encrypted, user-only access
- Aggregate benchmarks use only Aradune's public layer — never user's private data

### When BAA is required

A Business Associate Agreement is required before handling any Ring 2/3 data from a covered entity (state Medicaid agency, hospital, MCO).

BAA template must cover:
- Permitted uses and disclosures
- Minimum necessary standard
- Safeguard requirements
- Breach notification (60 days federal HIPAA; many state contracts require **24–72 hours**)
- Data retention and destruction
- Right to audit

### Certifications to pursue (in order)

1. **SOC 2 Type II** — Minimum for enterprise sales. Covers security, availability, processing integrity, confidentiality, privacy.
2. **HITRUST CSF** — Preferred for state contracts. Increasingly required in state RFPs.
3. **StateRAMP** — Required for some state cloud deployments.
4. **FedRAMP** — Only needed if pursuing federal contracts directly.

### Never do these things

- Never host raw PHI (patient-level claims) on Aradune infrastructure without a DUA and HITRUST certification
- Never build fraud detection features before BAA relationships with states exist
- Never allow Provider A to see Provider B's self-reported data without explicit opt-in
- Never use T-MSIS data outside the ResDAC DUA terms (no re-hosting, no re-identification)
- Never publish utilization counts below n=11
- Never log or store user query content in a way that could reveal PHI

### The de-identified public layer is always safe

The February 2026 HHS/DOGE dataset, state fee schedules, Medicare PFS, NPPES, and all aggregated federal datasets are Ring 0/1 — **zero HIPAA overhead**. This is where we operate now. Build credibility and revenue here before touching Ring 2/3.


## 17. Build Principles for Every Session

1. **Always build to the unified schema.** No one-off scripts dumping to random formats.
2. **Validation is not optional.** Every parser validates before loading.
3. **Source provenance is not optional.** Every record traces to URL + download date.
4. **Ship ugly.** Working data for 50 states beats beautiful UI for 5 states.
5. **Coverage > polish.**
6. **Federal data first.** Federal sources cover all states at once.
7. **Florida pipeline is the template.** Abstract, parameterize, replicate.
8. **PDF parsing prompts are versioned.** Build test suite with known-correct outputs.
9. **FL rate rule always enforced.** No facility + PC/TC split. Special: 46924, 91124, 91125.
10. **Data layer is the moat.** Every session: add data, improve quality, or make adding data easier.
11. **Don't be CPRA-forward.** CPRA is one use case. Build for the platform.
12. **Economic/contextual data matters.** Ingest data that informs Medicaid, not just from Medicaid.
13. **Forecasting models are never deleted.** Always append; track performance over time.
14. **User data is never mixed with Aradune's public layer** without explicit opt-in.
15. **Log predictions. Compare to actuals. Publish accuracy.**

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
- Goal: partnership, licensing deal, or acquisition. Could be white-label, data licensing, or full platform sale.

### Track B: Independent SaaS (future — full public build)
If Track A doesn't materialize, Aradune launches independently with a freemium model:

| Tier | Price | Who |
|---|---|---|
| Free (Aradune Open) | $0 | Journalists, advocates, aides, students |
| Analyst | $99/mo | Individual analysts, small consultants |
| Pro | $299/mo | Power users, consulting teams |
| State Agency | $50–200K/yr | State Medicaid agencies (may qualify for 75% FFP) |
| Enterprise | $50–500K/yr | Consulting firms, MCOs, hospital systems |
| Data as a Service | $25–100K/yr per dataset | Firms wanting bulk normalized data |

This requires: user accounts (Clerk recommended), usage tracking, Stripe billing, landing page redesign, removing password gate.

### Do not lose either option
- Keep pricing information in CLAUDE.md (this file) but **not on the live site**
- ProGate component and token system remain in code but with generic "contact us" messaging
- All tools stay fully functional behind the password gate
- The data layer and AI features serve both tracks equally

---

## 20. Reference Documents (docs/)

| File | Purpose | Action |
|------|---------|--------|
| `ARADUNE_MASTER.md` | Full strategy, data universe, analytical products, monetization | **Keep — primary strategy reference** |
| `TMSIS_DATA_GUIDE.md` | Operational T-MSIS guide: file types, field issues, DQ rules, SQL patterns, HCBS taxonomy | **Keep — Claude Code must read before any T-MSIS work** |
| `TMSIS_RESOURCES.md` | Verified URL library for T-MSIS docs, ResDAC, DQ Atlas, DOGE dataset, research orgs | **Keep — reference when looking up T-MSIS resources** |
| `UX_FEATURES_SPEC.md` | Full implementation spec for 5 UX features with code: glossary tooltips, nav search, state profiles, explain buttons, data story generator | **Keep — code spec for Claude Code** |
| `AraduneMockup.jsx` | Definitive landing page + nav design reference (React, design tokens, layout) | **Keep — UI reference for all frontend work** |
| `ARADUNE_PRODUCT_STRATEGY.md` | ~~Strategy and data landscape~~ | **DELETE — merged into ARADUNE_MASTER.md** |
| `aradune-implementation-plan.md` | ~~Build roadmap~~ | **DELETE — superseded by CLAUDE.md sections 13/17/18** |
| `aradune-market-gaps.md` | ~~Market gaps, HIPAA architecture~~ | **DELETE — incorporated into ARADUNE_MASTER.md and Section 16b above** |
| `ARADUNE_VISION.md` | ~~Vision doc~~ | **RENAME to TMSIS_RESOURCES.md** |


## 21. What Success Looks Like

**Current state (March 9, 2026):**
- 232 tables live in production (Fly.io), 109M rows queryable, NL2SQL working
- 250 fact tables on disk (270 views total), 115M+ rows, 785MB Parquet, all synced to R2
- 237+ API endpoints, 18 tools, 3 upload-driven engines (CPRA, Caseload Forecast, Expenditure Modeling)
- Deployed: Fly.io (API + data) + Vercel (frontend) — both current as of session 8
- ANTHROPIC_API_KEY set on Fly.io, Vercel, and local .env
- Pricing removed from site (kept flexible for partnership conversations)
- Brand assets updated: SVG logos, helmet chat icon, Lottie sword loader
- Behind password gate ("mediquiad")

**Completed milestones:**
- ~~State Profile pages~~ **Done**
- ~~NL2SQL Data Explorer~~ **Done** (working in production)
- ~~Data Catalog~~ **Done**
- ~~All tools have CSV export~~ **Done**
- ~~Deploy to Fly.io + set ANTHROPIC_API_KEY~~ **Done**
- ~~Remove pricing from site~~ **Done**

**Next milestone — Consulting firm demo (Track A):**
- Polish NL2SQL prompts and example queries
- Cross-dataset insights in State Profiles (the "so what" moment)
- More data — every table strengthens the pitch
- Dry-run the demo end-to-end
- Prepare talking points: data depth, AI capabilities, competitive landscape

**Next milestone — Public beta (Track B, if needed):**
- User accounts (Clerk recommended — 4-8 hours integration)
- Landing page redesign from `docs/AraduneMockup.jsx`
- Stripe billing integration
- At least one person outside the team has used it and given feedback

**3–6 months:** SPA/waiver search live. First external citation. Revenue conversation (either track).

**6–12 months:** Cited in a MACPAC report or state filing. Forecast accuracy dashboard published. Revenue covers infrastructure. Hospital price transparency MRFs ingested.

**1–3 years:** Default reference for Medicaid data. CMS links to it. Firms license the data. State agencies use it for CPRA compliance. Seven-figure revenue. Aradune is where Medicaid professionals start their day.

---

*The data is the moat. Build the moat. Ship fast. Iterate in public.*
