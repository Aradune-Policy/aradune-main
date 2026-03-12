# Changelog

## Session 17 (2026-03-12): Overnight Data Marathon + Deploy

### Data Collection (largest session ever)
- **294 new fact tables** added in one session (from ~250 to 544)
- **~137M new rows** ingested across all domains
- **3.0 GB** total lake size (was 785 MB)
- **77 ETL scripts** (was ~30)
- **19 new build scripts** written by parallel agents

### New Data Domains
- **KFF Medicaid** (28 tables): Total spending, spending per enrollee, eligibility, benefits, enrollment by category, Medicaid-to-Medicare fee index, work requirements, expansion status, managed care penetration, and more. All sourced from KFF Google Sheets.
- **State Fiscal** (11 tables): Census state finances (revenue, expenditure, debt), Tax Foundation rankings (income, corporate, sales, property, overall), FMAP historical.
- **Insurance Market** (3 tables): MLR reports, risk adjustment, MA star ratings.
- **Program Integrity** (5 tables): Federal Register CMS rules, MFCU stats, NHE, NHE projections, PERM rates.
- **Provider Network** (6+ tables): NPPES full registry (9.37M NPIs, 28 columns + taxonomy detail), PECOS enrollment, affiliations, CAHs, GME.
- **Maternal & Child Health** (10 tables): CDC natality, immunization, WIC participation, foster care, Title V block grants.
- **Medicaid.gov datasets** (17 tables): Drug AMP (5.5M rows), MLR summary, managed care programs by state, DSH annual, CoreSet measures, plus more from data.medicaid.gov API.
- **DOGE Provider Spending**: 190M raw rows from pre-built DuckDB, aggregated into 5 analytical tables (state x HCPCS, state x taxonomy, state x monthly, state x category, top providers).

### Major Additions to Existing Domains
- **NPPES**: Full 9.37M provider registry from 11 GB CSV in data/raw/. Selected 28 key columns + unpivoted 15 taxonomy slots.
- **Medicare**: Chronic conditions (CCW), MCBS, Part D prescriber, outpatient by provider, CMS program stats (utilization by service type), monthly enrollment.
- **Economic**: BEA personal income/transfers, SAFMR ZIP-level, county health rankings, RUCA rural classification, Census state finances, Tax Foundation.
- **Quality**: HCAHPS, Care Compare timely/effective care, OAS CAHPS, HAC measures.
- **Hospital**: Ownership data, Care Compare facility quality.
- **Behavioral Health**: TEDS detail, NSDUH 2024 state-level SAE, SAMHSA v2.
- **Nursing**: MDS facility-level (29.2M rows, largest single table).

### Infrastructure
- **R2 sync via wrangler**: Created `scripts/sync_lake_wrangler.py` to bypass boto3 SSL issues on macOS. Uses `npx wrangler r2 object put` with ThreadPoolExecutor parallel uploads. Successfully synced all 591 parquet files.
- **Docker deploy**: Rebuilt and deployed to Fly.io with full 3.0 GB lake baked into image. Both machines updated with rolling strategy.
- **Ontology update**: All 18 domains updated, 3 new domains created (state_fiscal, insurance_market, program_integrity). 288 new table entries. Generated prompt: 26,624 chars, 557 tables.
- **Integration tests**: 49 new tests for session 17 data (row counts, state codes, NPI format, FIPS codes, spending ranges).
- **db.py synced**: 544 fact_names entries matching 544 lake directories.

### Session 16 (2026-03-11): Data Run (~46M new rows)
- MDS facility-level (29.2M), providers missing DCI (3.8M), revalidation clinic group (3.3M)
- FISS attending/rendering (2M), order referring v2 (2M), Medicare physician provider (1.3M)
- CLIA (672K), Medicare monthly enrollment (557K), Medicaid opioid geo (539K), QPP experience (525K)
- HPSA dental + mental, enrollment files (FQHC/HHA/hospice/SNF/RHC/hospital)
- All-owners (FQHC/HHA/hospice/RHC), PAC casemix (HHA/IRF/SNF)
- Drug spending (Part B/D/Medicaid), ACO (REACH 2026, beneficiaries county v2)
- Plus: OTP providers, MA geo variation, quarterly spending, innovation models, HAC measures, LTC characteristics, AHRQ PSI11, MDPP suppliers, MC dashboard v2
- 2 reference tables: RBCS taxonomy 2025, taxonomy crosswalk 2025

### Session 15 (2026-03-11): ~1.27M rows, 39+2 tables
- HCRIS multi-year, ACO REACH providers, hospital/SNF ownership
- BEA GDP, scorecard detail, POS IQIES, BH services, CHIP, MC updates, BLS, CDC

### Session 14: ~774K rows, 21 tables
- Opioid geo, drug spending, MC enrollment, NSUMHSS, Medicare inpatient/outpatient, SNF owners

### Session 13b: ~15.8M rows
- PFS RVU, LEIE, Open Payments, MCO MLR, Medicare Provider PUFs

### Session 13 (2026-03-11): Build Plan Execution
- **Entity Registry (Ontology)**: 16 entities, 15 domains, 5 metric files, 274 tables. CI wired.
- **Data Quality Infrastructure**: 4-layer test suite, 86 tests passing in 1s.
- **Upload Quarantine Pattern**: Validates codes, rates. Splits invalid rows with rejection_reason.
- **Query Router**: 4-tier classification (Lookup/Comparison/Analysis/Synthesis).
- **Export Utility**: Shared CSV download + markdown extraction.

### Session 12 (2026-03-10): Intelligence Redesign
- Unified Intelligence interface at `/#/ask`
- Smart routing (general knowledge vs data queries vs hybrid)
- Response cache (LRU 200, 6hr TTL, 27 pre-seeded questions)
- Docker pre-bake (785MB lake in image, 10s cold start)
- Mobile fixes (100dvh, viewport-fit, safe-area)

### Session 11 (2026-03-10): State Rate Engine + RAG
- StateRateEngine wired to Rate Builder
- RAG engine: BM25 + vector hybrid over 1,039 policy docs, 6,058 chunks
- Intelligence wired to RAG search
- State Profile cross-dataset insights (11 generators)

### Session 9-10: Architecture Rebuild
- Intelligence as home page with SSE streaming
- AraduneContext shared state
- IntelligencePanel sidebar
- Nav restructured (6 modules)

### Earlier Sessions
- Sessions 1-8: Initial build, CPRA engine, T-MSIS pipeline, state fee schedule ingestion (47 states), forecasting engines, AHEAD readiness, provider/workforce tools.
