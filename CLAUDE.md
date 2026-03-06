# CLAUDE.md — Aradune
> **The ONE source for Medicaid data intelligence.**
> Read this file at the start of every session. It defines what Aradune is, how it's built, and the rules for building it.
> Last updated: 2026-03-06 · Last commit: `95f5a34` · Live: https://www.aradune.co

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
Data lake:      Hive-partitioned Parquet (data/lake/) — 89.5M rows, 83 tables
                DuckDB in-memory views over Parquet files
                S3 sync ready (scripts/sync_lake.py)
Backend:        Python FastAPI (server/) — 77 endpoints, DuckDB-backed
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
NL2SQL:         Vanna (target — DuckDB native, open-source)
Design:         #0A2540 ink · #2E6B4A brand · #C4590A accent
                SF Mono for numbers · Helvetica Neue for body · No Google Fonts
Access:         Password gate ("mediquiad") via sessionStorage in Platform.tsx
                "Coming Soon" splash — entire site hidden until code entered
```

---

## 4. Live Tools (14 total)

**Site is behind a password gate** (`PasswordGate` component in `Platform.tsx`). Password: `mediquiad`. Stored in `sessionStorage` — clears on tab close. All tools are lazy-loaded and code-split per route.

| Group | Tool | Route | Status |
|-------|------|-------|--------|
| Transparency | Spending Explorer | `/#/explorer` | live |
| Transparency | Medicare Comparison | `/#/decay` | live |
| Transparency | State Fee Schedule Directory | `/#/fees` | live |
| Transparency | Rate Lookup | `/#/lookup` | live |
| Transparency | Compliance Report | `/#/compliance` | live |
| **Transparency** | **CPRA Generator** | **`/#/cpra`** | **live — wedge product** |
| Adequacy | Rate & Wage Comparison | `/#/wages` | live |
| Adequacy | Quality Linkage | `/#/quality` | live |
| Adequacy | Rate Reduction Analyzer | `/#/reduction` | live |
| Adequacy | HCBS Compensation Tracker | `/#/hcbs8020` | live |
| Modeling | Rate Builder | `/#/builder` | live |
| Modeling | AHEAD Calculator | `/#/ahead` | live |
| Modeling | AHEAD Readiness Score | `/#/ahead-readiness` | live |
| Modeling | Policy Analyst | `/#/analyst` | beta — **NO AUTH, publicly accessible** |

**Target nav structure:**
```
Aradune  [⌕ search]  Explore▾  Analyze▾  Build▾  About
```
See `docs/AraduneMockup.jsx` as the definitive landing page + nav reference.

---

## 5. CPRA — Two Separate Systems

The CPRA exists as **two architecturally distinct systems** that serve different purposes:

### 5a. CPRA Frontend (Pre-Computed Cross-State Comparison)

`src/tools/CpraGenerator.tsx` (734 lines). Displays pre-computed rate comparisons from `fact_rate_comparison` (278K rows, 42 states, all HCPCS codes). This is a **general fee-to-Medicare comparison tool** — not limited to the 68 E/M codes.

**Data flow (pre-computed, read-only):**
```
fact_rate_comparison (lake)           → all codes, 42 states, pre-computed pct_of_medicare
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
| fact_rate_comparison | 278,702 | CPRA: Medicaid vs Medicare (42 states) |
| fact_dq_flags | 270,000 | Data quality flags |

**Output files (what the frontend reads):**

| File | Size | Frontend? | Notes |
|------|------|-----------|-------|
| `cpra_em.json` | 709KB | ✅ Primary CPRA data | 39 states, 3,169 E/M rows |
| `dq_flags_em.json` | 81KB | ✅ DQ warnings | 789 flags, 6 types |
| `dim_447_codes.json` | 14KB | ✅ Code definitions | 74 E/M codes |
| `cpra_summary.json` | 7KB | ✅ State aggregates | median, national context |
| `cpra_all.json` | 47.5MB | ❌ gitignored/vercelignored | too large for client |
| `dq_flags.json` | 27.2MB | ❌ gitignored/vercelignored | full 258K flags |
| `medicare_rates_locality.parquet` | 10MB | ❌ gitignored/vercelignored | locality-level rates |

**42 states in rate_comparison (39 with E/M codes in cpra_em.json):** AL, AR, AZ, CA, CO, CT, DC, DE, FL, GA, HI, ID, IL, IN, KY, LA, MA, MD, ME, MN, MO, MS, MT, NC, ND, NE, NH, NV, NY, OH, OK, OR, PA, RI, SC, SD, TX, UT, VA, WA, WV, WY

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
| 3 | ~~6 states missing from CPRA~~ | | **Fixed** — COALESCE in cpra_engine.py. 34→42 states. |
| 4 | ~~**Frontend not wired to FastAPI**~~ | All 13 tools | **Resolved** — All tools wired with JSON fallback. |
| 5 | **Policy Analyst no auth** | `api/chat.js` | Publicly accessible — anyone can burn Anthropic API credits. Site password gate helps but is client-side only. |
| 6 | **Old CPRA uses wrong code list** | `cpra_engine.py`, `fact_rate_comparison` | Uses 74 codes + $33.4009 CF + 1:1 categories. Should be 68 codes + $32.3465 CF + many-to-many (171 pairs). Upload tool (5b) has the correct values. Pre-computed data not yet updated. |
| 7 | **FL rates not in CPRA display** | `fact_rate_comparison` | FL Practitioner Fee Schedule (6,676 codes) added to `fact_medicaid_rate` but NOT reflected in `fact_rate_comparison` (which CPRA frontend reads). The upload tool (5b) computes FL correctly from user-uploaded CSVs. |
| 8 | **CPRA upload not deployed to Fly.io** | `server/engines/cpra_upload.py` | Engine + routes ported and tested locally. Needs: reference CSVs in Docker image, `python-multipart` in Dockerfile, deploy. |
| 9 | **R2 credentials need rotation** | Infrastructure | Shared in plain text during session. |

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
| ~~`public/data/cpra_precomputed.json`~~ | **Deleted** |
| `scripts/build-cpra-data.mjs` | Delete — superseded by cpra_engine.py |
| ~~Bar visualization clips at outliers (CT 666%)~~ | **Fixed** — Capped at 200% |
| ~~Conversion factor discrepancy~~ | **Fixed** — Updated to $33.4009 (non-QPP) for general comparison. CPRA compliance uses $32.3465 (CY2025). |
| Locality weighting is equal, not population-weighted | Acceptable for v1; fix with Census CBSA data later |
| ~~`fl_methodology_addendum.md` not loaded~~ | **Fixed** — `api/chat.js` now appends it to system prompt |
| `StateRateEngine.js` not wired | 1,153 lines, 42/42 tests passing, but not connected to Rate Builder UI |
| Password gate is client-side only | `sessionStorage` check in Platform.tsx — not a security boundary, just a preview wall |
| Frontend `CpraGenerator.tsx` not wired to upload tool | Needs "Bring Your Own Data" tab/mode that POSTs to `/api/cpra/upload/generate` |

---

## 8. Immediate Next Steps

### Tier 1 — Ship-blocking (before removing password gate)
1. ~~**Wire frontend to FastAPI endpoints**~~ — **Done.** All 13 tools wired. CPRA Generator, WageAdequacy, HcbsTracker use per-endpoint API calls. RateDecay, RateBuilder, ComplianceReport, QualityLinkage, RateLookup, FeeScheduleDir use bulk API endpoints (`/api/bulk/*`) with static JSON fallback. RateReduction uses DuckDB-WASM (no API needed). New `server/routes/bulk.py` serves 7 bulk endpoints matching frontend JSON shapes.
2. ~~**Auth on Policy Analyst**~~ — **Done.** Preview token (`mediquiad`) accepted in `api/chat.js`. Password gate auto-populates analyst token in localStorage. Three auth paths: ADMIN_KEY, PREVIEW_TOKEN, ANALYST_TOKENS (env vars).
3. ~~**Confirm CPRA in production**~~ — **Build verified.** ErrorBoundary in place, TypeScript clean, production build succeeds. Needs visual verification on aradune.co.
4. ~~**Reconcile conversion factor**~~ — **Done.** `medicare_pfs.py` updated from $32.3465 (QPP) to $33.4009 (non-QPP). Frontend and cpra_engine.py already used correct value.

### Tier 2 — Platform completeness
5. **Landing page redesign** from `docs/AraduneMockup.jsx` — password gate redesigned (text logo, left-justified centered block). Full landing page after gate still needs work.
6. ~~**Nav redesign**~~ — **Done.** Grouped dropdowns (Explore / Analyze / Build).
7. **Wire `StateRateEngine.js` into Rate Builder** — 42/42 tests passing, not connected to UI
8. ~~**Wire `dq_state_notes.json`**~~ — **Done.**
9. ~~**Cap bar visualization**~~ — **Done.**
10. ~~**Append `fl_methodology_addendum.md`**~~ — **Done.**
11. **Wire CPRA upload tool to frontend** — Add "Bring Your Own Data" mode to `CpraGenerator.tsx` that POSTs to `/api/cpra/upload/generate`. JSON output shape designed to match existing table component.
12. **Deploy CPRA upload to Fly.io** — Include `data/reference/cpra/` CSVs in Docker image, add `python-multipart` to requirements, redeploy.
13. **Update old cpra_engine.py** — Align with correct 68 codes, $32.3465 CF, many-to-many categories from `data/reference/cpra/` files. Or deprecate in favor of upload tool for CPRA-specific use cases.

### Tier 3 — Data expansion (standing instruction)
The data layer is the moat. Every session: add data, improve quality, or make adding data easier.

**Completed federal datasets (89.5M rows, 83 tables):**
- T-MSIS claims (227M source) · CPRA rates (42 states) · CMS-64 · NADAC · SDUD
- BLS wages (state/MSA/national) · HCRIS hospitals + SNFs · Hospital quality (ratings/VBP/HRRP/HAC)
- Five-Star NF · POS · PBJ staffing (65M+) · EPSDT · Enrollment/unwinding/MC plans
- Census ACS · BRFSS · CDC mortality/overdose · FRED economic (GDP/pop/unemployment/income)
- HPSA · Scorecard · HAI · NH ownership/penalties/deficiencies · HCAHPS · Imaging
- MLTSS · Financial mgmt · Eligibility levels · ACA FUL · DQ Atlas · 1115 waivers · NCCI edits

**Next datasets to ingest:**
11. ~~**Supplemental payment programs**~~ — **Done (Phase 1).** Ingested CMS-64 FMR supplemental payments (1,553 rows, FY 2019-2024, 51 states, DSH/supplemental/GME by service category) + MACPAC Exhibit 24 (102 rows, FY 2023-2024, state-level DSH/non-DSH/1115 waiver summary). 4 API endpoints in `server/routes/supplemental.py`. Still needed: hospital-level DSH data, State Directed Payment preprint parsing, UPL demonstrations.
12. **More state fee schedules** — Currently 42/51 states in CPRA. Remaining 9 need manual extraction.
13. **SAMHSA behavioral health** — Block grants, psychiatric beds. NSDUH requires manual extraction (no bulk API).
14. **340B covered entity data** — HRSA quarterly.
15. **HCBS deeper** — Waiver utilization, waitlists (700K+ nationally), DSW workforce data.
16. **CHIP enrollment/expenditure** — Separate from Medicaid, CMS/MBES quarterly.

**Improve existing data:**
17. Add `weighted_avg_pct` to `cpra_summary.json` using CY2023 FFS claim volume weights
18. Add category-level breakdowns to `cpra_summary.json` per state
19. Build reusable ingestion pattern (fetch→parse→validate→normalize→load) to accelerate new sources
20. **AHEAD Readiness: historical HCRIS** — Ingest FY2021–2022 HCRIS for 3-year trend sparklines
21. **AHEAD Readiness: hospital-level supplemental** — CMS-64 Schedule A/B at hospital level for UPL/SDP

### Tier 4 — Analytical features
20. **Caseload forecasting** — ARIMA/ETS per state with economic covariates
21. **RAG over policy corpus** — pgvector + Voyage-3-large embeddings
22. **NL2SQL** via Vanna (DuckDB native)

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
│   │   ├── PolicyAnalyst.tsx        ← 378 lines. AI chat (#/analyst) — ⚠️ NO AUTH
│   │   ├── ComplianceReport.tsx     ← (#/compliance)
│   │   ├── RateReductionAnalyzer.tsx ← (#/reduction)
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
│   ├── fee_schedules.json           ← State fee schedule rates (42 states)
│   ├── bls_wages.json               ← BLS wages
│   ├── quality_measures.json        ← CMS Core Set
│   ├── soc_hcpcs_crosswalk.json     ← SOC-HCPCS mapping
│   ├── conversion_factors.json      ← State methodology metadata
│   ├── system_prompt.md             ← AI tier system prompt
│   ├── fl_methodology_addendum.md   ← ⚠️ NOT YET LOADED in api/chat.js
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
│   │   └── cpra_upload.py           ← 821 lines. CPRA upload engine (68 codes, 171 pairs, DuckDB)
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
│       ├── supplemental.py       ← /api/supplemental/* — 4 endpoints (FMR DSH/supplemental/GME + MACPAC summary)
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
│   ├── lake/                        ← Unified Parquet data lake (89.5M rows, 83 tables)
│   │   ├── dimension/               ← dim_state, dim_procedure, dim_hcpcs, dim_bls_occupation, dim_medicare_locality, dim_time, dim_provider_taxonomy
│   │   ├── fact/                    ← Hive-partitioned: fact/{name}/snapshot=YYYY-MM-DD/data.parquet
│   │   │   └── (medicaid_rate, medicare_rate, medicare_rate_state, rate_comparison, dq_flag,
│   │   │       enrollment, quality_measure, expenditure, claims, claims_monthly, claims_categories,
│   │   │       provider, drug_utilization, nadac, managed_care, dsh_payment, fmap, spa,
│   │   │       bls_wage, bls_wage_msa, bls_wage_national,
│   │   │       hospital_cost, snf_cost, eligibility, new_adult, unwinding, mc_enrollment,
│   │   │       pbj_nurse_staffing, pbj_nonnurse_staffing, pbj_employee,
│   │   │       five_star, hac_measure, pos_hospital, pos_other,
│   │   │       hospital_rating, hospital_vbp, hospital_hrrp, epsdt,
│   │   │       mspb_state, timely_effective, complications, unplanned_visits,
│   │   │       dialysis_state, home_health_state,
│   │   │       mltss, financial_mgmt, eligibility_levels, aca_ful, dq_atlas,
│   │   │       cpi, unemployment, median_income, mspb_hospital,
│   │   │       hpsa, scorecard, elig_group_monthly, elig_group_annual,
│   │   │       cms64_new_adult, ffcra_fmap, mc_enroll_pop, mc_enroll_duals,
│   │   │       hai_state, hai_hospital, nh_ownership,
│   │   │       acs_state, drug_overdose, mortality_trend,
│   │   │       state_gdp, state_population, nh_penalties)
│   │   ├── reference/               ← ref_drug_rebate, ref_ncci_edits, ref_1115_waivers
│   │   └── metadata/                ← manifest_*.json (pipeline run metadata)
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

| Dataset | Source | Format | Cadence | Priority |
|---|---|---|---|---|
| State Medicaid fee schedules (all 51) | State agency websites | CSV/XLSX/PDF | Annual/quarterly | **P0** |
| Medicare Physician Fee Schedule | cms.gov PFS RVU files | ZIP/CSV | Annual + quarterly | **P0** |
| T-MSIS HHS open data (227M rows) | HHS/Hugging Face | Parquet | Done (Feb 2026) | **P0** ✓ |
| NPPES NPI Registry | download.cms.gov | CSV | Weekly | **P0** ✓ |
| Medicaid Provider Enrollment Files | State portals — actively enrolled providers | State portals | Annual | **P0** |
| CMS-64 expenditure reports | medicaid.gov | Excel/CSV | Quarterly | **P1** |
| MBES/CBES enrollment/expenditure | medicaid.gov | Excel | Quarterly | **P1** |
| HCRIS hospital cost reports | cms.gov | CSV | Quarterly (2–4yr lag) | **P1** |
| Provider of Services (POS) File | cms.gov | CSV | Quarterly | **P1** |
| NADAC pharmacy pricing | medicaid.gov | CSV | Weekly | **P1** |
| State Drug Utilization Data (SDUD) | data.medicaid.gov | CSV + API | Quarterly | **P1** |
| FMAP rates | medicaid.gov/kff.org | Web | Annual | **P1** |
| Adult/Child Core Set quality measures | medicaid.gov | Web/Excel | Annual | **P1** |
| MACStats | macpac.gov | PDF/Excel | Annual | **P2** |

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

### Supplemental Payment Programs ← MAJOR GAP

For safety net hospitals, supplemental payments can exceed base rates by 2–5x. All Ring 0.

| Dataset | Program | Source | Cadence | Priority |
|---|---|---|---|---|
| CMS DSH Allotment Reports | DSH | medicaid.gov | Annual | **P1** |
| CMS-64 Schedule A/B | UPL/DSH | medicaid.gov | Quarterly | **P1** |
| UPL Demonstration filings (state SPAs) | UPL/IGT/CPE | CMS MACPro | Ongoing | **P1** |
| State Directed Payment filings (42 CFR 438.6(c)) | SDP | CMS | Annual | **P1** |
| HRSA GME payment data | GME (direct + indirect) | hrsa.gov | Annual | **P1** |
| 1115 waiver financial terms | LIP/DSRIP/UC pools | medicaid.gov | Ongoing | **P1** |
| MACPAC supplemental payment reports | All programs | macpac.gov | Annual | **P2** |
| OIG DSH audit reports | DSH | oig.hhs.gov | Ongoing | **P2** |

**Programs to model:** DSH (disproportionate share) · UPL/IGT/CPE (upper payment limit + intergovernmental transfers) · State Directed Payments (managed care) · LIP (Low Income Pool, FL + others) · DSRIP · GME direct + indirect · Uncompensated Care pools. The "all-in Medicaid rate" = base rate + all supplemental programs. No platform shows this today.

### LTSS / HCBS ← SIGNIFICANT GAP (~40% of Medicaid spending)

| Dataset | Source | Cadence | Priority |
|---|---|---|---|
| CMS-64 Schedule B (HCBS expenditure by waiver) | medicaid.gov | Quarterly | **P1** |
| 1915(c) Waiver Utilization & Expenditure | CMS waiver reports | Annual | **P1** |
| HCBS Quality Measures (CMS national framework) | medicaid.gov | Annual | **P1** |
| HCBS Waitlist Data (700K+ people waiting nationally) | KFF / state reports | Annual | **P1** |
| Nursing Facility Cost Reports (CMS-2540) | cms.gov | Annual | **P1** |
| Five-Star Quality Rating (Care Compare, NF) | cms.gov API | Monthly | **P1** |
| Payroll-Based Journal (PBJ) NF staffing | cms.gov | Quarterly | **P1** |
| Direct Support Workforce data (wages, vacancy, turnover) | PHI / ANCOR | Annual | **P1** |
| MDS facility-level aggregates | cms.gov | Quarterly | **P2** |
| 1915(k) Community First Choice utilization | CMS reports | Annual | **P2** |
| PACE enrollment & spending | CMS reports | Annual | **P2** |

### Hospital Quality & Value-Based Programs ← NOT IN ARADUNE YET

| Dataset | Source | Cadence | Priority |
|---|---|---|---|
| Care Compare — hospital ratings | cms.gov API | Quarterly | **P1** |
| Inpatient Quality Reporting (IQR) | cms.gov API | Quarterly | **P1** |
| Hospital Value-Based Purchasing (VBP) scores | cms.gov | Annual | **P1** |
| Hospital Readmissions Reduction Program (HRRP) | cms.gov | Annual | **P1** |
| Hospital-Acquired Condition (HAC) Reduction | cms.gov | Annual | **P2** |

### Behavioral Health ← UNDERREPRESENTED

| Dataset | Source | Cadence | Priority |
|---|---|---|---|
| SAMHSA NSDUH (MH/SUD prevalence by state) | samhsa.gov | Annual | **P1** |
| SAMHSA Block Grant expenditure reports | samhsa.gov | Annual | **P1** |
| Psychiatric bed capacity by state | samhsa.gov | Annual | **P1** |
| 1115 IMD waiver utilization | CMS reports | Annual | **P1** |
| BH-specific HRSA HPSA designations | hrsa.gov API | Ongoing | **P1** |

### Children's Health / CHIP / EPSDT

| Dataset | Source | Cadence | Priority |
|---|---|---|---|
| CHIP enrollment & expenditure (separate from Medicaid) | CMS/MBES | Quarterly | **P1** |
| EPSDT Participation Reports (CMS-416) | medicaid.gov | Annual | **P1** |
| Children's Core Set measures | medicaid.gov | Annual | **P1** ✓ Terminal B |

### Eligibility & Unwinding

| Dataset | Source | Cadence | Priority |
|---|---|---|---|
| Medicaid unwinding / redetermination outcomes | CMS dashboard | Monthly | **P1** |
| KFF Medicaid eligibility policy tracker | kff.org | Ongoing | **P1** |

### Pharmacy (Deeper)

| Dataset | Source | Cadence | Priority |
|---|---|---|---|
| NADAC | medicaid.gov | Weekly | **P1** ✓ Terminal B |
| SDUD | data.medicaid.gov | Quarterly | **P1** ✓ Terminal B |
| 340B covered entity data | hrsa.gov | Quarterly | **P1** |
| State MAC prices | State portals | Varies | **P2** |

### Ring 0.5: Economic & Contextual Data (Essential for Forecasting)

| Dataset | Source | Use Case | Cadence |
|---|---|---|---|
| State unemployment rates (LAUS) | bls.gov | Primary caseload driver | Monthly |
| BLS CPI (medical care, all items) | bls.gov | Rate decay / real-value analysis | Monthly |
| BLS OEWS (healthcare occupation wages) | bls.gov | Wage adequacy analysis | Annual |
| Census/ACS demographics | census.gov | Population denominators, poverty rates | Annual |
| BEA GDP by state | bea.gov | Economic context for forecasting | Quarterly |
| FRED economic series | fred.stlouisfed.org | Poverty, income, enrollment correlates | Monthly |
| SNAP/TANF enrollment | fns.usda.gov / ACF | Cross-program enrollment correlation | Monthly |
| Federal poverty guidelines | ASPE/HHS | Eligibility threshold context | Annual |
| Housing cost indices (HUD) | hud.gov | Cost-of-living / HCBS wage adequacy | Annual |
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

**Phase 1:** ARIMA/ETS per state — display forecast + confidence intervals on state profiles.
**Phase 2:** Driver-based with economic covariates (unemployment, poverty, population). Publish elasticity estimates.
**Phase 3:** ML models (gradient boosting / random forest) with public performance tracking. Model leaderboard: best algorithm per state over rolling 12/24/36-month windows.
**Phase 4:** Ensemble + scenario builder ("What if unemployment rises 2 points?").

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

## 19. Monetization

| Tier | Price | Who |
|---|---|---|
| Free (Aradune Open) | $0 | Journalists, advocates, aides, students |
| Analyst | $99/mo | Individual analysts, small consultants |
| Pro | $299/mo | Power users, consulting teams |
| State Agency | $50–200K/yr | State Medicaid agencies (may qualify for 75% FFP) |
| Enterprise | $50–500K/yr | Consulting firms, MCOs, hospital systems |
| Data as a Service | $25–100K/yr per dataset | Firms wanting bulk normalized data |

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

**1–3 months:** All 50 states have rate data. Cross-state comparisons work. CPRA tool confirmed working in production. A journalist or staffer cites Aradune.

**3–6 months:** Caseload forecasting for top 15 states. SPA search live. Rate adequacy reports generating. First paying institutional client. User data upload in beta.

**6–12 months:** Cited in a MACPAC report or state filing. Forecast accuracy dashboard published. Multiple institutional clients. Revenue covers infrastructure costs. ML model leaderboard public.

**1–3 years:** Default reference for Medicaid data. CMS links to it. Firms license the data. State agencies use it for compliance. Seven-figure revenue. Aradune is where Medicaid professionals start their day.

---

*The data is the moat. Build the moat. Ship fast. Iterate in public.*
