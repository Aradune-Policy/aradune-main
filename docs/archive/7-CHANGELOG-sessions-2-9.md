# CHANGELOG — Aradune

> Session history, resolved issues, and completed milestones. Extracted from CLAUDE.md to keep the main reference file concise.

---

## Recent Changes (2026-03-09, session 9 — data audit + architecture)
- **Comprehensive platform audit** — Three parallel agents audited live API (25 endpoints), data layer (250 tables, cross-references), and frontend (18 tools). Identified 5 data quality / routing bugs, all fixed.
- **Route ordering fixes (3 files)** — `hospitals.py`: moved `/api/hospitals/summary` before `/{state_code}`. `quality.py`: moved `/api/five-star/summary` before `/{state_code}` and `/api/hpsa/summary` before `/{state_code}`. All were returning empty results because FastAPI matched the parameterized route first.
- **Census sentinel values fixed** — `_CENSUS_SENTINELS` set added to `build_lake_census.py`. Rebuilt `fact_acs_state` Parquet (52 rows, 0 sentinel values). Synced to R2.
- **Enrollment deduplication** — `lake.py` enrollment query now uses `GROUP BY year, month` + `MAX()` to deduplicate preliminary/final reports. FL: 204→103 rows.
- **Architecture document created** — `docs/NEW_ARADUNE_BUILD_STRUCTURE.md` — comprehensive assessment and proposed ground-up redesign. Three-layer architecture (Intelligence → 6 Modules → Data Lake). Modules organized by user workflow, not data domain. AHEAD/CPRA absorbed into larger modules rather than standalone. User reviewing for brainstorm in extended thinking session.
- **Intelligence endpoint built but not wired** — `server/routes/intelligence.py` exists (Claude Sonnet 4.6 + extended thinking + 3 DuckDB tools) but has no frontend component yet. Key finding from the audit.
- **Fly.io redeployed** — All 5 fixes deployed. Census Parquet synced to R2. Production verified.

---

## Recent Changes (2026-03-09, session 8 — deploy + demo prep)
- **Fly.io deployed** — All 250 fact tables registered in code, 232 live in production (109M rows). NL2SQL endpoint working with ANTHROPIC_API_KEY.
- **ANTHROPIC_API_KEY set** — Fly.io (`fly secrets set`), Vercel (`vercel env add`), local `server/.env` (gitignored). NL2SQL and Policy Analyst both functional.
- **Vercel deployed** — Frontend live at aradune.co with all changes below.
- **Pricing removed from site** — Deleted `Pricing()` component and `/pricing` route from Platform.tsx. Removed "See pricing →" footer link. Updated ProGateModal to generic "contact us" text. Updated PolicyAnalyst auth screen to remove subscription language. Pricing kept in CLAUDE.md (Section 19) for Track B reference.
- **Two-track strategy documented** — Track A: partnership/acquisition demo build (active). Track B: independent SaaS with freemium model (future fallback). Both use same codebase.
- **Brand assets migrated to SVG** — Navbar: logo-wordmark.svg (was logo-full.png). Chat icon: helmet.svg (was icon-bot.png). PDF reports: logo-wordmark.svg.
- **Lottie sword loader** — `sword-animation.json` (10.8MB) fetched at runtime via `SwordLoader` component. Used as loading fallback for lazy-loaded tools. Kept small (80x140px).
- **Landing page stats updated** — "250 fact tables", "115M+ rows", "80+ federal sources".
- **lottie-react** dependency added to package.json.

---

## Recent Changes (2026-03-09, sessions 4-7 — data expansion sprint)
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

---

## Recent Changes (2026-03-08, session 2)
- **Expenditure Modeling Engine** — `server/engines/expenditure_model.py` (~430 lines). Takes caseload forecast output + user-uploaded expenditure parameters CSV (cap rates for MC, cost-per-eligible for FFS). Applies compound monthly trend, admin load, risk margin, policy adjustments. Returns per-category and aggregate projections with CI bands. Key classes: `ExpenditureModeler`, `CategoryExpenditure`, `ExpenditureResult`.
- **Expenditure API routes** — 4 new endpoints added to `server/routes/forecast.py` (now 10 total): `GET /api/forecast/templates/expenditure-params`, `POST /api/forecast/expenditure` (full pipeline), `POST /api/forecast/expenditure/csv`, `POST /api/forecast/expenditure-only`.
- **Caseload Forecaster frontend** — `src/tools/CaseloadForecaster.tsx` (~830 lines). Full upload UI: state selector, horizon dropdown, caseload/events/expenditure-params file inputs with template download links, seasonality/economic checkboxes. Caseload view: summary metrics, category pills, fan chart with 80/95% CI bands, event markers, intervention effects panel, model comparison table. Expenditure view: summary card (total/MC/FFS), expenditure fan chart (orange accent), per-category table (9 columns + totals row), MC vs FFS horizontal breakdown bar. Tab toggle between views.
- **Platform.tsx updated** — CaseloadForecaster registered as lazy-loaded tool at `/#/forecast` in Build group. Tool count: 15.
- **CLAUDE.md overhauled** — Sections 4, 7, 8, 9, 16 updated. Section 8 restructured with Tier 2b (critical platform gaps: State Profiles, search, landing page, data catalog, export, user accounts) and Tier 3/4 tables with clear-eyed gap analysis.

---

## Recent Changes (2026-03-08, session 1)
- **Caseload Forecasting Engine (Phase 1)** — `server/engines/caseload_forecast.py` (~650 lines). SARIMAX + ETS model competition per category with intervention variables (COVID PHE, unwinding, MC launches, eligibility changes). Economic covariate enrichment from Aradune's public unemployment data. Holdout MAPE validation. Template-driven CSV upload pattern (same as CPRA). Tested with synthetic FL data: 8 categories, 96 months, all SARIMAX, <1% MAPE. Key fixes: event deduplication for multicollinearity, future exog construction for step functions. Dependencies added: `statsmodels>=0.14.0`, `pmdarima>=2.0.0`, `pandas>=2.1.0`, `numpy>=1.26.0`.
- **Forecast API routes** — `server/routes/forecast.py` (original 6 endpoints): template downloads (caseload + events CSVs), generate forecast (JSON + CSV), public enrollment time series, enrollment by eligibility group.
- **Round 9 data ingestion** — `scripts/build_lake_round9.py` (17 datasets): Medicare Enrollment (557K), Opioid Prescribing (539K), SDUD 2024 (5.2M), Drug Rebate Products (1.9M), CMS IPPS Impact (3,152), AHRF County, Physician Compare, ESRD QIP, OTP providers, CMS-64 FFCRA, contraceptive care, respiratory conditions, program monthly, MC annual/info monthly, CHIP monthly/app-elig, performance indicator, new adult enrollment, Medicare provider enrollment. Total: ~8.3M new rows.
- **Round 9 API routes** — `server/routes/round9.py` (22 endpoints): Medicare enrollment/duals, opioid prescribing summary, SDUD 2024 top drugs, CMS IPPS impact, Medicare provider enrollment by type, and more.
- **Rounds 4-8 data ingestion** — Multiple build scripts ingested ~80+ additional fact tables across sessions: hospital directories, MC programs, CHIP enrollment/unwinding, medicaid applications, vaccinations, blood lead screening, dual status, benefit packages, NAS rates, SNF VBP/quality, FQHC directory, vital stats, HHCAHPS, hospice directory/CAHPS, VHA providers, pregnancy outcomes, and more.
- **db.py expanded** — Now registers 250 fact tables (up from ~70). Fixed duplicate `imaging_hospital` entry. All 250 lake directories matched.
- **Data lake milestone** — 115M+ rows across 250 fact tables, 9 dimensions, 9 references, 2 compat views = 270 total views. 237+ API endpoints across 20 route files. Deployed to Fly.io (needs redeployment for sessions 4-7 data).
- **Platform.tsx updated** — Stats now show "115M+" rows and "250" fact tables.

---

## Recent Changes (2026-03-07)
- **3 new computed fee schedules** — AK (RBRVS CF=$43.412, 138.6% MCR), MI (RBRVS CF=$21.30, 66.7% MCR), NM (150% of Medicare, 154.9% MCR). Script: `scripts/build_lake_fee_schedules_computed.py`. Added to both Parquet lake and SQLite.
- **CPRA coverage expanded** — 42→45 states in `fact_rate_comparison` (302,332 rows), 39→40 states with E/M data in `cpra_em.json`.
- **cpra_engine.py regenerated** — Updated to 68 codes, $32.3465 CF, many-to-many (171 pairs) from reference CSVs. All exports refreshed.
- **Supplemental Payments Phase 2** — `build_lake_supplemental_p2.py` created: hospital-level DSH (6,103 hospitals) + SDP preprint (34 states). 3 new API endpoints.
- **CpraGenerator.tsx upload tab** — "Bring Your Own Data" mode added, POSTs to `/api/cpra/upload/generate`.
- **Dockerfile updated** — `COPY data/reference/cpra/` for upload tool. Ready for `fly deploy`.

---

## Recent Changes (2026-03-06, session 2)
- **CPRA Upload Tool ported** — User-upload CPRA generator from `cpra-pipeline/` integrated into Aradune. Engine at `server/engines/cpra_upload.py` (821 lines), 7 new endpoints under `/api/cpra/upload/`. Uses the **correct** CMS CY 2025 E/M code list (68 codes, 171 code-category pairs, $32.3465 CF). Reference data in `data/reference/cpra/` (3 CSVs: em_codes, code_categories, GPCI2025). Existing pre-computed comparison routes unchanged — general fee-to-Medicare comparison is a separate tool from the compliance-specific CPRA upload. Tested end-to-end with FL data (99213 PC: $34.29/$91.39 = 37.5% — exact match with R pipeline).
- **Password gate redesigned** — Replaced tiny logo PNG with large text "ARADUNE" (32px, brand green, letterspaced). Content now left-justified in a centered 400px block instead of scattered center-aligned text. Added "Access code" label above input. Deployed to Vercel.
- **FL Practitioner Fee Schedule added** — Downloaded from AHCA, parsed 6,676 codes into `fact_medicaid_rate` (573,853 total rows). FL went from 3,773 to 10,449 rows. Handles "BR" (By Report) non-numeric values. Note: these rates are in `fact_medicaid_rate` but NOT yet in `fact_rate_comparison` (which the CPRA frontend reads). The upload tool computes FL correctly from CSVs.
- **Supplemental payment data ingested** — CMS-64 FMR (1,553 rows, FY 2019-2024, DSH/supplemental/GME by service x state) + MACPAC Exhibit 24 (102 rows, FY 2023-2024). 4 API endpoints in `server/routes/supplemental.py`, 2 new lake tables. Synced to R2 and live on Fly.io. Key findings: TN 98.2% supplemental, TX 92.5%, VA 90.2%. TX $9.7B/yr in supplemental payments.
- **Nav groups renamed** — Transparency→Explore, Adequacy→Analyze, Modeling→Build.

---

## Recent Changes (2026-03-06, session 1)
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

---

## Resolved Issues (from Section 7 — Known Bugs & Issues)

### Critical — Resolved

| # | Bug | Location | Resolution |
|---|-----|----------|------------|
| 1 | ~~White page on CPRA~~ | `/#/cpra` | **Resolved** — ErrorBoundary added. |
| 2 | ~~T-MSIS DuckDB empty~~ | | **Resolved** — T-MSIS data ingested into lake. |
| 3 | ~~6 states missing from CPRA~~ | | **Fixed** — COALESCE in cpra_engine.py. 34→45 states (AK/MI/NM computed from RBRVS). |
| 4 | ~~**Frontend not wired to FastAPI**~~ | All 13 tools | **Resolved** — All tools wired with JSON fallback. |
| 5 | ~~**Policy Analyst no auth**~~ | `api/chat.js` | **Mitigated** — Has Bearer token auth (PREVIEW_TOKEN + Stripe + rate limiting at 30/hr). Set PREVIEW_TOKEN env var on Vercel to restrict beyond site password. |
| 6 | ~~**Old CPRA uses wrong code list**~~ | `cpra_engine.py` | **Fixed** — Updated to 68 codes from reference CSVs, $32.3465 CF, many-to-many (171 pairs). Re-run `--em-codes --cpra --export` to regenerate `fact_rate_comparison`. |
| 7 | ~~FL rates not in CPRA display~~ | `fact_rate_comparison` | **Fixed** — Re-ran cpra_engine.py. FL now in rate_comparison. Also added AK/MI/NM computed fee schedules (RBRVS). 45 states, 302K rows. |
| 8 | ~~**CPRA upload not deployed to Fly.io**~~ | `server/Dockerfile` | **Fixed** — Dockerfile updated to COPY `data/reference/cpra/`. Deployed to Fly.io. |
| 12 | ~~**Forecast engine needs frontend**~~ | `/#/forecast` | **Done.** Full UI: upload form, caseload forecast (fan chart + model table + interventions), expenditure projection (summary, chart, category table, MC/FFS breakdown bar). Tab toggle between caseload and expenditure views. |
| 13 | ~~**Fly.io needs redeployment**~~ | Infrastructure | **Done** — Deployed session 8. 232 tables live, NL2SQL working. |
| 14 | ~~**ANTHROPIC_API_KEY on Fly.io**~~ | Infrastructure | **Done** — Set on Fly.io, Vercel, and local .env. |
| 15 | ~~**Route ordering bugs (3 files)**~~ | `hospitals.py`, `quality.py` | **Fixed** — `/api/hospitals/summary`, `/api/five-star/summary`, `/api/hpsa/summary` were unreachable because parameterized `/{state_code}` routes were declared first. Moved all summary routes before parameterized routes. |
| 16 | ~~**Census sentinel values in lake**~~ | `build_lake_census.py` | **Fixed** — Census suppression codes (-888888888, -666666666, -999999999) leaked into `fact_acs_state` as real values. Added `_CENSUS_SENTINELS` filter in `_try_float()`. Rebuilt Parquet (52 rows, 0 sentinels), synced to R2. |
| 17 | ~~**Enrollment duplication**~~ | `lake.py` enrollment query | **Fixed** — data.medicaid.gov publishes both preliminary (P) and final (U) reports per month. API query now deduplicates with `GROUP BY year, month` + `MAX()`. FL went from 204→103 rows. Underlying Parquet still has dupes (fix at ETL layer later). |

### Minor — Resolved

| Item | Resolution |
|------|------------|
| ~~No export on most tools~~ | **Done** — All tools have CSV export. CPRA also has PDF/Excel. |

---

## Completed Milestones (from Section 8 — Immediate Next Steps)

### Tier 1 — Ship-blocking (all completed)

1. ~~**Wire frontend to FastAPI endpoints**~~ — **Done.** All 13 tools wired. CPRA Generator, WageAdequacy, HcbsTracker use per-endpoint API calls. RateDecay, RateBuilder, ComplianceReport, QualityLinkage, RateLookup, FeeScheduleDir use bulk API endpoints (`/api/bulk/*`) with static JSON fallback. RateReduction uses DuckDB-WASM (no API needed). New `server/routes/bulk.py` serves 7 bulk endpoints matching frontend JSON shapes.
2. ~~**Auth on Policy Analyst**~~ — **Done.** Preview token (`mediquiad`) accepted in `api/chat.js`. Password gate auto-populates analyst token in localStorage. Three auth paths: ADMIN_KEY, PREVIEW_TOKEN, ANALYST_TOKENS (env vars).
3. ~~**Confirm CPRA in production**~~ — **Build verified.** ErrorBoundary in place, TypeScript clean, production build succeeds. Needs visual verification on aradune.co.
4. ~~**Reconcile conversion factor**~~ — **Done.** `medicare_pfs.py` updated from $32.3465 (QPP) to $33.4009 (non-QPP). Frontend and cpra_engine.py already used correct value.

### Tier 2 — Platform completeness (completed items)

**Done:** ~~Nav redesign~~, ~~DQ state notes~~, ~~Bar cap~~, ~~FL methodology addendum~~, ~~CPRA upload frontend~~, ~~CPRA upload Fly.io deploy~~, ~~cpra_engine.py update~~, ~~Caseload Forecaster frontend~~, ~~Expenditure modeling engine + API + frontend~~.

### Tier 2b — Critical platform gaps (completed items)

| # | Gap | Resolution |
|---|-----|------------|
| A | ~~**State Profile pages**~~ | **Done.** `StateProfile.tsx` (~470 lines), 18 parallel API fetches, 7 collapsible sections (overview, enrollment, rates, hospitals, quality, workforce, pharmacy, economic). Hash routing: `/#/state/{code}`. |
| B | ~~**Search / discovery**~~ | **Partially done.** Data Explorer (`/#/ask`) provides NL2SQL search. NavSearch exists. Full-text search across all tools still TODO. |
| C | ~~**Landing page**~~ | **Updated.** Hero copy, stats, "Find a state" → State Profile, "Ask a Question" CTA. |
| D | ~~**Data catalog**~~ | **Done.** `DataCatalog.tsx` at `/#/catalog` — browsable index of all tables with row counts, column schemas, descriptions. |
| E | ~~**Export for all tools**~~ | **Done.** All tools now have CSV export buttons. CPRA also has PDF/Excel. |

### Tier 4 — Analytical features (completed items)

| # | Feature | Resolution |
|---|---------|------------|
| 1 | ~~**Caseload forecasting**~~ | **Done.** Engine + API (10 endpoints) + full frontend UI with fan charts, model comparison, intervention effects. |
| 2 | ~~**Expenditure modeling**~~ | **Done.** Engine (`expenditure_model.py`) + 4 API endpoints + frontend UI (summary, chart, per-category table, MC/FFS breakdown bar). Tab toggle with caseload view. |
| 3 | ~~**Scenario builder**~~ | **Done.** Third tab in CaseloadForecaster. 4 sliders (unemployment, eligibility, rate change, MC shift) with preset scenarios. Client-side adjustment of forecast with baseline vs scenario chart. |
| 4 | ~~**NL2SQL over the data lake**~~ | **Done.** `DataExplorer.tsx` at `/#/ask`. Claude Sonnet generates DuckDB SQL from natural language, validates (SELECT-only, LIMIT, forbidden keywords), executes with timeout. 10 example queries. Backend: `server/routes/nl2sql.py`. |

---

## External Project: cpra-pipeline

Located at `/Users/jamestori/Desktop/cpra-pipeline/`. A standalone CPRA pipeline built separately, now partially ported into Aradune. Contains:
- **R pipeline** (`R/01-07`) — Publication-quality CPRA report via Quarto. Real Medicare rates + FL Medicaid rates + simulated utilization. Produces 6 figures, gt tables, PDF report.
- **Python engine** (`python/cpra_generator.py`) — The engine ported into Aradune as `server/engines/cpra_upload.py`.
- **Python API** (`python/cpra_api.py`) — Standalone FastAPI server (port 8100). Routes ported into `server/routes/cpra.py`.
- **Reference data** (`data/raw/`) — em_codes.csv, code_categories.csv, GPCI2025.csv, PPRRVU25_JAN.csv, fl_practitioner_fee_schedule_2025.xlsx. The first 3 copied to `data/reference/cpra/`.
- **FL test data** (`data/simulated/`) — Real FL Medicaid rates + simulated utilization CSVs. Useful for testing the upload tool.
- **HCBS templates** (`deliverables/data_requests/`, `data/hcbs/`) — Templates for the separate HCBS disclosure required by 447.203(b)(2)(iv). Not yet built.
- **Key regulatory notes:** Deadline July 1, 2026. CY 2025 rates. 68 codes (not 74). $32.3465 CF. Only utilization data is simulated (no FMMIS access on personal laptop). When moved to AHCA work machine, replace simulated utilization with real FMMIS extract.
