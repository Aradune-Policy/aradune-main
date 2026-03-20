# Aradune: Complete Build Document

> **The operating system for Medicaid intelligence.**
> Last updated: 2026-03-18 (Session 34)
> Live: https://www.aradune.co | API: https://aradune-api.fly.dev
> Florida LLC filed. Federal trademark pending (Class 42, Section 1(b)).

---

## Table of Contents

1. [What Aradune Is](#1-what-aradune-is)
2. [System Architecture](#2-system-architecture)
3. [The Data Lake](#3-the-data-lake)
4. [The Ontology Layer](#4-the-ontology-layer)
5. [Intelligence Engine](#5-intelligence-engine)
6. [Backend (FastAPI + DuckDB)](#6-backend)
7. [Frontend (React + TypeScript)](#7-frontend)
8. [Structured Modules (28 Total)](#8-structured-modules)
9. [Research Modules (13 Academic Briefs)](#9-research-modules)
10. [ETL Pipeline & Data Ingestion](#10-etl-pipeline)
11. [CPRA Compliance System](#11-cpra-compliance-system)
12. [Forecasting Engines](#12-forecasting-engines)
13. [Data Import & Export Pipeline](#13-data-import--export-pipeline)
14. [Infrastructure & Deployment](#14-infrastructure--deployment)
15. [Data Quality & Validation](#15-data-quality--validation)
16. [Security & Compliance](#16-security--compliance)
17. [Build History & Current State](#17-build-history--current-state)
18. [What Remains](#18-what-remains)

---

## 1. What Aradune Is

Aradune is a Medicaid intelligence operating system. It ingests, normalizes, and cross-references every available public Medicaid dataset into a unified data layer, then uses Claude-powered analytics to turn that data into intelligence: compliance-ready documents, fiscal impact models, early warning signals, and actionable recommendations.

Named after Brad McQuaid's EverQuest paladin character.

### The Problem

Medicaid is an $880B system run on Excel, SAS, and fragmented legacy databases. State agencies use "hundreds of models and estimates" for forecasting. No integrated system connects rate-setting to network adequacy to quality to fiscal forecasting to compliance artifacts. Federal requirements are escalating: CPRA rate transparency (July 2026), mandatory Core Set quality reporting (FFY 2024+), OBBBA work requirements (January 2027), SDP caps, provider tax restrictions, six-month redetermination cycles.

### Core Identity

- **The data layer is the moat.** 750+ tables, 400M+ rows, 4.9 GB. Curated, normalized, cross-referenced public Medicaid data no one else has assembled.
- **Intelligence is the interface.** Claude is the primary interaction model. Natural language in, compliance-ready analysis out.
- **Structured tools are on-ramps.** Fifteen purpose-built modules build trust, demonstrate data quality, and pull users into Intelligence naturally.
- **Compliance automation is the adoption wedge.** Auto-generate CPRAs, rate transparency filings, MCPARs, Core Set submissions. The July 2026 CPRA deadline is less than 4 months away.
- **Bring your own data.** Users upload files and cross-reference them against the national data layer within their session.
- **Closed-loop operations.** Every analysis connects to an action: a rate gap finding generates a SPA template, a network gap produces a corrective action plan, a spending anomaly creates a program integrity referral.

### Target Users

| Persona | Entry Point | Value |
|---------|-------------|-------|
| State Rate-Setter | CPRA tool, then Intelligence | Fee schedule + July deadline = compliance-ready CPRA in <2 seconds |
| Consulting Actuary | Import client data, cross-reference | Generate report sections for actuarial certification |
| Medicaid Director | Intelligence home page | Ad hoc answers without waiting for IT |
| MCO Analyst | State Profiles, Rate Comparison | Network adequacy, quality benchmarking, rate competitiveness |
| Advocate/Researcher | Intelligence, Data Catalog | Accessible data, free tier, export |

### Competitive Landscape

No standalone Medicaid analytics SaaS exists. The market is served by:
- Consulting-embedded analytics (Milliman MedInsight, Guidehouse, Accenture) -- none Medicaid-specific
- MMIS-embedded analytics (Gainwell, Conduent, Acentra) -- bundled in multi-year contracts
- CMS's own tools (MACBIS, DQ Atlas) -- not analytics platforms

No CPRA-specific compliance tools found. TAF Research Files cost ~$88K/year + $35K/seat + 6-8 month application. Aradune's 750+ table assembly would cost ~$500K-$1M and 12-24 months to replicate.

---

## 2. System Architecture

Aradune has three layers. Intelligence connects everything.

```
+-----------------------------------------------------------------------+
|                       ARADUNE INTELLIGENCE                            |
|                                                                       |
|  Claude Sonnet/Opus + extended thinking + DuckDB query access         |
|  + RAG over policy corpus + web search + Skillbook injection          |
|  + user-uploaded data cross-reference + structured output format      |
|                                                                       |
|  Produces: narrative, tables, charts, exportable compliance docs.     |
+-------------------+---------------------------+-----------------------+
                    |                           |
                    |                    +------+--------+
                    |                    |   REFLECTOR   |
                    |                    | Async Haiku   |
                    |                    | ~$0.004/query |
                    |                    | Skills + score|
                    |                    +------+--------+
                    |                           |
+-------------------+---------------------------+-----------------------+
|                      ENTITY REGISTRY + SKILLBOOK v2                   |
|                                                                       |
|  Ontology: 16 entities, 20 domains, 28 edges, 19 named metrics      |
|  Skillbook v2: CRUSP lifecycle, score decay (half-life 30 days),     |
|    graph expansion (1-hop related_skills), prune automation.         |
|    24+ validated domain insights (strategies, caveats, failure       |
|    modes, rules, query patterns). Self-curating, scored.             |
|  Reflector v2: Async Haiku, proposes links + split candidates.       |
|  fact_intelligence_trace: Audit trail with trace_id per query.       |
|  Auto-generates: Intelligence system prompt + DuckPGQ property graph |
+-------------------------------+---------------------------------------+
                                |
+-------------------------------+---------------------------------------+
|                        THE DATA LAKE                                  |
|                                                                       |
|  750+ tables, 400M+ rows, Hive-partitioned Parquet, DuckDB           |
|  Medallion architecture: Bronze -> Silver -> Gold                     |
|  + Policy corpus (1,039 docs, 6,058 chunks)                         |
|  + Validation engine (15+ checks, 3 types, stored results)          |
|  R2 sync, Dagster orchestration, source-provenant, versioned         |
+-----------------------------------------------------------------------+
```

### Technology Stack

```
Frontend:       React 18 + TypeScript + Vite (Vercel Pro, aradune.co)
Visualization:  Recharts
Routing:        Hash-based in Platform.tsx
Data store:     DuckDB-WASM (browser-side client queries)
Data lake:      Hive-partitioned Parquet (data/lake/) -- 400M+ rows, 750+ views
                DuckDB in-memory views over Parquet files, 4.9 GB on disk
                S3/R2 sync (scripts/sync_lake_wrangler.py --remote)
Backend:        Python FastAPI (server/) -- ~345 endpoints across 40+ route files
AI:             Claude Sonnet 4.6 + extended thinking + DuckDB tools + RAG + web search
                Haiku for routing, Sonnet for analysis, Opus for complex reasoning
                Programmatic caveat injection (DOGE, IL T-MSIS, territory fallback)
                DuckDB 30s statement_timeout + Anthropic API 120s timeout
                _postprocess_response em-dash/en-dash stripping on all output
Skillbook:      Self-improving Intelligence layer (server/engines/skillbook.py + reflector.py)
                CRUSP lifecycle, score decay (half-life 30 days), graph expansion (1-hop)
                fact_intelligence_trace audit trail with trace_id in SSE metadata
Adversarial:    7-agent adversarial testing suite (scripts/adversarial/)
                28 ground-truth anchor facts (known_facts.json)
                Adversarial-to-Skillbook pipeline (skillbook_import.py)
                GitHub Actions weekly run + auto-import + issue creation
RAG:            DuckDB FTS over policy corpus (1,039 docs, 6,058 chunks)
                BM25 full-text search with ILIKE fallback
Search:         Platform-wide Cmd+K search (PlatformSearch.tsx + /api/search)
Auth:           Clerk (JWT validation, test keys active -- switch to production before demo)
Pipeline:       115+ Python ETL scripts (scripts/build_*.py)
Orchestration:  Dagster (13 assets, 3 checks, 3 jobs, 2 schedules)
CI/CD:          GitHub Actions (TypeScript check + Vercel + Fly.io deploy + weekly adversarial)
Deployment:     Vercel (frontend) + Fly.io (FastAPI backend)
Design:         #0A2540 ink, #2E6B4A brand, #C4590A accent, #F5F7F5 surface
                SF Mono for numbers, Helvetica Neue for body, no Google Fonts
```

---

## 3. The Data Lake

### Scale

| Metric | Value |
|--------|-------|
| Total views registered | 750+ (669 fact + 9 dimension + 22 reference + auto-discovered) |
| Total rows | 400M+ |
| Parquet on disk | 4.9 GB |
| Ontology domains | 20 |
| Entity types | 16 |
| Relationship edges | 28 |
| Named metrics | 19 |
| ETL scripts | 115+ |
| Data sources | 90+ official federal/state sources |
| R2 parquet files | 890+ on Cloudflare R2 |

### Medallion Architecture

**Bronze (Raw, Append-Only)**
- Parquet as received from source
- Hive-partitioned by state/year/month
- Metadata columns: `_source_file` (URL), `_ingestion_timestamp`, `_source_state`, `_batch_id`
- `union_by_name=true` for schema-tolerant reads
- ZSTD compression, 500K-1M row groups
- Never modified after write (audit trail)

**Silver (Normalized)**
- ICD GEMs + CCSR groupings applied
- NDC 5-4-2 format normalized to RxNorm
- NPI taxonomy enriched from NPPES
- Illinois-specific claim dedup (incremental credits/debits, not void/replace)
- Void/replacement logic for T-MSIS
- Temporal FFY/SFY/CY columns added
- DQ Atlas quality metadata carried through

**Gold (Analytics-Ready)**
- Pre-computed PMPM, utilization metrics, quality indicators
- Pre-joined dimension tables
- Medical Care CPI adjusted (not general CPI)
- State-level adjustment factors from MACPAC

### Parquet Organization on Disk

```
data/lake/
  dimension/          9 .parquet files (884 KB)
    dim_state.parquet
    dim_procedure.parquet
    dim_medicare_locality.parquet
    dim_bls_occupation.parquet
    ...
  fact/               669+ directories (4.6 GB)
    medicaid_rate/
      data.parquet                            (modern format)
      snapshot=2026-03-17/data.parquet        (versioned format)
    rate_comparison_v2/
    enrollment/
    cms64_multiyear/
    sdud_combined/
    hospital_cost/
    five_star/
    pbj_nurse_staffing/
    ...
  reference/          22 files (11 MB)
    ref_1115_waivers.parquet
    ref_icd10.parquet
    ref_ndc.parquet
    ...
  metadata/
```

### View Registration (db.py)

The database layer (`server/db.py`) creates an in-memory DuckDB connection at startup and registers all Parquet files as views:

1. `init_db()` creates the DuckDB connection, spawns background thread
2. `_register_all_views()` iterates dimensions (9), facts (667 in `FACT_NAMES`), auto-discovered facts, and reference tables (22)
3. `_delayed_rescan()` polls every 30 seconds for up to 15 minutes until 650+ views found -- handles cold starts where R2 sync is still downloading
4. Thread-safe via `threading.Lock()`, idempotent `CREATE VIEW IF NOT EXISTS`
5. `_latest_snapshot()` supports both `data.parquet` and `snapshot=*/data.parquet` formats

Two backward-compatibility views are also created:
- `spending` remaps `fact_claims` columns to legacy schema
- `spending_providers` remaps `fact_provider` columns to legacy schema

After R2 background sync completes, the server receives `POST /internal/reload-lake` which clears the registered set and re-scans.

### Complete Data Universe by Domain

#### Rates & Fee Schedules
| Table | Rows | Source |
|-------|------|--------|
| fact_medicaid_rate | 597,483 | 47 state fee schedule PDFs/CSVs |
| fact_rate_comparison_v2 | 483,154 | 54 states: 88% published, 11% CF x RVU, 1.1% T-MSIS |
| fact_fee_schedule_ca | 11,454 | California Medi-Cal |
| fact_fee_schedule_tx | 10,224 | Texas HHSC |
| fact_fee_schedule_ny | 7,111 | New York eMedNY |
| fact_fee_schedule_va | 17,527 | Virginia DMAS |
| fact_fee_schedule_ks | 21,316 | Kansas KMAP |
| fact_fee_schedule_wi | 8,245 | Wisconsin ForwardHealth |
| fact_fee_schedule_nj | 22,017 | New Jersey DMAHS |
| fact_fee_schedule_ia | 10,384 | Iowa IME |
| fact_fee_schedule_il | 10,552 | Illinois HFS |
| fact_fee_schedule_mt | 9,047 | Montana DPHHS |
| fact_fee_schedule_or | 12,432 | Oregon OHP |
| fact_fee_schedule_wa | 8,812 | Washington HCA |
| fact_fee_schedule_oh | 8,847 | Ohio ODM |
| fact_fee_schedule_nc | 8,112 | North Carolina NCDHHS |
| fact_fee_schedule_nd | 9,118 | North Dakota HHS |
| fact_fee_schedule_pa | 2,301 | Pennsylvania DHS |
| fact_fee_schedule_vt | 7,827 | Vermont DVHA (computed from CFs: $35.99 PC / $28.71 std) |
| dim_procedure | 17,081 | HCPCS/CPT codes with CY2026 Medicare PFS RVUs |
| dim_medicare_locality | 109 | Medicare GPCI locality definitions |

**Coverage:** All 54 jurisdictions (50 states + DC + PR + GU + VI). 51 have published fee schedules; 3 territories use T-MSIS claims-based rates. TN excluded from FFS rate comparison (94% managed care, no published FFS schedule; simulated rates from T-MSIS claims available). 17 new fee schedule tables scraped in Session 30.

#### Enrollment & Managed Care
| Table | Rows | Source |
|-------|------|--------|
| fact_enrollment | 5,250-10,399 | Monthly 2013-2025, data.medicaid.gov |
| fact_mc_enrollment_summary | 513 | MC penetration by state |
| fact_mc_enrollment | 7,804 | Plan-level MC enrollment |
| fact_unwinding | 57,759 | Post-PHE unwinding 2023-2025 |
| fact_medicare_monthly_enrollment | 557,282 | Medicare monthly by state |
| fact_chip_eligibility | varies | CHIP enrollment |
| fact_continuous_eligibility | varies | 12-month continuous |
| + 48 more tables | | PACE, managed care programs, new adult expansion |

#### Claims & Utilization
| Table | Rows | Source |
|-------|------|--------|
| fact_claims | 712,793 | T-MSIS aggregated OT claims |
| fact_sdud_combined | 28,300,000 | SDUD 2020-2025 ($1.05T pre-rebate) |
| fact_sdud_2025 | 2,637,009 | Q1-Q4 2025 drug utilization |
| fact_drug_utilization | 2,369,659 | SDUD legacy multi-year |
| fact_doge_* (5 tables) | 190M (raw) | DOGE provider spending (QUARANTINED) |

**DOGE Quarantine:** Three-layer controls -- Intelligence system prompt, ontology QUARANTINE tags, and ETL docstring. DOGE data is OT-only, uses provider state (not beneficiary state), MC states show misleadingly low paid amounts, Nov/Dec 2024 incomplete. The dataset was taken offline by CMS.

#### Hospitals & Acute Care
| Table | Rows | Source |
|-------|------|--------|
| fact_hospital_cost | 18,019 | HCRIS cost reports (6,103 hospitals) |
| fact_hospital_rating | 5,426 | CMS Care Compare quality |
| fact_hospital_vbp | 2,455 | CMS Value-Based Purchasing |
| fact_hospital_hrrp | 18,330 | CMS HRRP readmissions |
| fact_hcahps | 325,000+ | Patient satisfaction |
| fact_hac_measure | 12,120 | Hospital-Acquired Conditions |
| fact_hospital_ownership | varies | Ownership type and chain |
| fact_dsh_hospital | 6,000+ | DSH allotments and payments |

#### Nursing Facilities
| Table | Rows | Source |
|-------|------|--------|
| fact_five_star | 14,710 | CMS Five-Star ratings, ownership, deficiencies, turnover |
| fact_pbj_nurse_staffing | 1,332,436 | PBJ daily staffing (summary) |
| fact_pbj_nonnurse_staffing | varies | Non-nurse staffing |
| fact_snf_cost | 42,810 | HCRIS SNF cost reports |
| fact_nh_deficiency | 419,452 | Deficiency citations |
| fact_mds_facility_level | varies | MDS quality (29.2M assessments underlying) |

**Note:** PBJ raw data exceeds 65M records. Summary table is 1.3M rows.

#### Workforce & Access
| Table | Rows | Source |
|-------|------|--------|
| fact_bls_wage | 812 | State-level wages |
| fact_bls_wage_national | 831 | National wages |
| fact_hpsa | 68,859 | HRSA HPSA designations |
| fact_hpsa_dental | 43,000 | Dental HPSAs |
| fact_hpsa_mh | 38,000 | Mental Health HPSAs |
| fact_mua_mup | 20,000 | Medically Underserved Areas/Populations |
| fact_workforce_projections | 102,528 | 121 professions to 2038 |
| fact_hrsa_awarded_grants | 70,902 | HRSA grants |
| fact_hrsa_active_grants | 18,641 | Active HRSA grants |
| fact_nhsc_field_strength | 222 | NHSC field strength |
| fact_bh_workforce_projections | varies | Behavioral health workforce |
| fact_fqhc_sites | 19,000 | FQHC locations |

#### Pharmacy & Drug Spending
| Table | Rows | Source |
|-------|------|--------|
| fact_sdud_combined | 28,300,000 | SDUD 1991-2025, $1.05T pre-rebate |
| fact_nadac | 1,882,296 | NADAC drug pricing |
| fact_aca_ful | 2,100,000 | ACA Federal Upper Limit |
| fact_opioid_prescribing | 539,181 | Part D opioid rates |
| fact_open_payments | 39,640 | Open Payments ($13.18B, all 3 CMS categories) |
| fact_drug_spending_partb | varies | Part B drug spending |
| fact_drug_spending_partd | varies | Part D drug spending |
| fact_fda_orange_book | 48,000 | FDA Orange Book products |
| fact_fda_orange_book_patents | 21,000 | Drug patents |

#### Behavioral Health & SUD
| Table | Rows | Source |
|-------|------|--------|
| fact_nsduh_prevalence | 5,865 | SAMHSA NSDUH (26 measures) |
| fact_nsduh_2024_sae | varies | NSDUH 2024 Small Area Estimates |
| fact_teds_admissions | 1,600,000+ | TEDS treatment admissions |
| fact_teds_detail | varies | TEDS detailed records |
| fact_mh_facility | 27,957 | Mental health facilities |
| fact_block_grant | 55 | SAMHSA block grants |
| fact_ipf_facility | 1,400 | Inpatient Psychiatric Facilities |
| fact_bh_by_condition | 4,200 | Behavioral health by condition |

#### LTSS/HCBS
| Table | Rows | Source |
|-------|------|--------|
| fact_hcbs_waitlist | 51 (state-level) | 607K people waiting (41 states reporting) |
| fact_section_1115_waivers | 665 | Section 1115 waivers catalog |
| fact_cms372_waiver | varies | CMS-372 waiver data |
| fact_ltss_expenditure | varies | LTSS spending |
| fact_mltss | varies | Managed LTSS enrollment |

#### Quality Measures
| Table | Rows | Source |
|-------|------|--------|
| fact_quality_core_set_2024 | 10,972 | 57 measures, 51 states |
| fact_quality_core_set_2023 | varies | Prior year |
| fact_quality_core_set_combined | 35,993 | 2017-2024 longitudinal |
| fact_scorecard | varies | CMS Scorecard |
| fact_epsdt | 54 | CMS-416 EPSDT |
| fact_hac_measures | varies | Hospital-Acquired Conditions |

#### Expenditure & Fiscal
| Table | Rows | Source |
|-------|------|--------|
| fact_cms64_multiyear | 117,936 | CMS-64 FY2018-2024, $5.7T total computable |
| fact_expenditure | 5,379 | Legacy expenditure |
| fact_macpac_spending_per_enrollee | 63 | MACPAC per-enrollee by state |
| fact_macpac_supplemental | varies | Supplemental payments |
| fact_fmap_historical | 663 | FMAP FY2011-2023 (extended from 4 to 13 years) |
| fact_nhe | 3,400 | National Health Expenditures 1991-2020 |
| fact_nhe_projections | varies | NHE projections to 2029 |
| fact_census_state_finances | 16,435 | All 50 states fiscal data |
| fact_sdp_preprint | varies | State Directed Payments (34 states) |

#### Economic & Contextual
| Table | Rows | Source |
|-------|------|--------|
| fact_acs_state | varies | Census ACS demographic data |
| fact_unemployment | varies | BLS unemployment rates |
| fact_cpi | varies | BLS CPI (Medical Care index) |
| fact_saipe | varies | Small Area Income and Poverty |
| fact_snap | varies | SNAP participation |
| fact_tanf | varies | TANF caseloads |
| fact_county_health_rankings | 263,000 | Robert Wood Johnson rankings |
| fact_bea_state_gdp | 13,440 | BEA state GDP |
| fact_bea_state_income | varies | BEA personal income |
| fact_tax_foundation_* | varies | Tax Foundation state rankings |
| fact_food_access_atlas | 72,000 | USDA food access by census tract |
| fact_hud_fmr | varies | HUD Fair Market Rents |

#### Medicare & ACOs
| Table | Rows | Source |
|-------|------|--------|
| fact_medicare_enrollment | varies | Medicare monthly enrollment |
| fact_medicare_monthly_enrollment | 557,282 | Monthly by state |
| fact_medicare_geo_variation | 34,000 | Geographic variation 2014-2023 |
| fact_mssp_aco | varies | 511 ACOs PY2026 |
| fact_aco_reach_* | varies | REACH 2026 |
| fact_chronic_conditions_* | varies | Medicare chronic conditions |
| fact_mcbs | varies | Medicare Current Beneficiary Survey |
| fact_cms_program_stats_* | varies | CMS program statistics |

#### Program Integrity
| Table | Rows | Source |
|-------|------|--------|
| fact_leie | 82,749 | OIG exclusions |
| fact_open_payments | 39,640 | Open Payments ($13.18B) |
| fact_mfcu_stats | 53 | MFCU performance 2020-2025 |
| fact_perm_rates | 12 | PERM error rates |
| fact_federal_register_cms | varies | Federal Register CMS rules |

#### Provider Network
| Table | Rows | Source |
|-------|------|--------|
| fact_nppes_provider | 9,370,000 | Full NPPES registry (28 key columns) |
| fact_nppes_taxonomy | varies | Provider taxonomy |
| fact_pecos | varies | Medicare enrollment/revalidation |
| fact_provider_reassignment | 3,490,000 | NPI reassignment |
| fact_cah | varies | Critical Access Hospitals |
| fact_gme | 62,000 | Graduate Medical Education |
| fact_affiliations | varies | Provider affiliations |

#### Social Determinants (New Session 30)
| Table | Rows | Source |
|-------|------|--------|
| fact_adi | 240,000 | Area Deprivation Index (block groups) |
| fact_ahrq_sdoh | 44,000 | AHRQ SDOH (county-years, 14 years) |
| fact_svi_county | 3,144 | CDC Social Vulnerability Index |
| fact_food_access_atlas | 72,000 | USDA food access by tract |

#### Policy Corpus
| Table | Rows | Source |
|-------|------|--------|
| fact_policy_document | 1,039 | CMS documents (CIBs, SHOs, SMDs) |
| fact_policy_chunk | 6,058 | Searchable text chunks |

#### KFF Medicaid (28 tables)
Covering spending, enrollment, eligibility, benefits, fee indexes, and policy trackers from Kaiser Family Foundation.

#### Medicaid.gov (17 tables)
Including fact_drug_amp (5.5M), fact_mlr_summary, fact_mc_programs, fact_dsh_annual.

### Dimension Tables

| Table | Rows | Description |
|-------|------|-------------|
| dim_state | 51 | States + DC with FMAP, enrollment, expansion_status, expansion_date, conversion_factor, agency |
| dim_procedure | 17,081 | HCPCS/CPT codes with CY2026 work/PE/MP RVUs |
| dim_medicare_locality | 109 | Medicare GPCI locality definitions |
| dim_bls_occupation | 16 | Medicaid-relevant BLS occupation codes |
| + 5 more | | Various reference dimensions |

### Reference Tables (22)

Including: ref_1115_waivers, ICD-9/ICD-10 crosswalks, NDC mappings, FIPS codes, NUCC taxonomy codes, CCSR groupings, RxNorm mappings. All stored as SCD Type 2 with `effective_date` + `termination_date` for point-in-time historical joins.

### Universal Join Keys

| Key | Links | Example |
|-----|-------|---------|
| `state_code` (2-letter) | Everything to dim_state | `WHERE state_code = 'FL'` |
| `cpt_hcpcs_code` / `procedure_code` | Rates, claims to dim_procedure | JOIN on RVUs, descriptions |
| `provider_ccn` / `ccn` | HCRIS, Five-Star, PBJ, SNF, deficiencies | Hospital/nursing linkage |
| `npi` | NPPES, PECOS, claims, affiliations | Provider-level linkage |
| `ndc` | SDUD, NADAC, drug_utilization, FDA | Drug-level linkage (11-digit 5-4-2) |
| `locality_code` | Medicare rates to dim_medicare_locality | GPCI adjustment |
| `fips_code` / `county_fips` | County-level data, SVI, AHRF | Geographic analysis |
| `soc_code` | BLS wages to dim_bls_occupation | Workforce analysis |
| `measure_id` | Quality Core Set measures | Quality analysis |

### Data Sensitivity Rings

| Ring | Type | HIPAA | Status |
|------|------|-------|--------|
| 0 | Public regulatory (fee schedules, RVUs, SPAs) | None | Here now |
| 0.5 | Economic/contextual (BLS, FRED, Census, SDOH) | None | Here now |
| 1 | Aggregated/de-identified (T-MSIS open data, DOGE) | Minimal | Here now |
| 2 | Provider-level (billing volumes, network data) | Low, BAA required | Future |
| 3 | Claims/encounter (T-MSIS/TAF, state MMIS) | Full HIPAA | After BAA + HITRUST |

**Directive:** Stay in Ring 0/0.5/1 until BAA, SOC 2 Type II, and HITRUST in place.

---

## 4. The Ontology Layer

The ontology is a YAML-based Entity Registry that sits between the raw data lake and Intelligence. It auto-generates the system prompt (what Intelligence knows about the data) and the DuckPGQ property graph definition. Adding a new dataset means adding a YAML file and running two scripts.

### Directory Structure

```
ontology/
  schema.yaml                 Master schema definition
  generated_prompt.md         Auto-generated system prompt (33.7KB, 722 tables)
  entities/                   16 YAML files (one per entity type)
    state.yaml
    procedure.yaml
    provider.yaml
    hospital.yaml
    nursing_facility.yaml
    mco.yaml
    drug.yaml
    quality_measure.yaml
    rate_cell.yaml
    geographic_area.yaml
    economic_indicator.yaml
    enrollment_record.yaml
    expenditure_record.yaml
    hcbs_program.yaml
    occupation.yaml
    policy_document.yaml
  domains/                    20 YAML files (one per data domain)
    rates.yaml
    enrollment.yaml
    hospitals.yaml
    nursing.yaml
    quality.yaml
    workforce.yaml
    pharmacy.yaml
    behavioral_health.yaml
    ltss_hcbs.yaml
    expenditure.yaml
    economic.yaml
    medicare.yaml
    policy.yaml
    public_health.yaml
    provider_network.yaml
    program_integrity.yaml
    state_fiscal.yaml
    insurance_market.yaml
    maternal_child.yaml
    post_acute.yaml
  metrics/                    6 YAML files with named metric definitions
    rate_metrics.yaml
    enrollment_metrics.yaml
    fiscal_metrics.yaml
    pharmacy_metrics.yaml
    quality_metrics.yaml
    access_metrics.yaml
```

### 16 Entity Types

| Entity | Canonical Table | Primary Key | Fact Tables |
|--------|-----------------|-------------|-------------|
| State | dim_state | state_code | 485+ |
| Procedure | dim_procedure | cpt_hcpcs_code | 81 |
| Hospital | fact_hospital_directory | provider_ccn | 45+ |
| Nursing Facility | fact_nh_provider_info | provider_id | 20+ |
| Provider | fact_nppes_provider | npi | 15+ |
| MCO | fact_mc_enrollment_plan | plan_id | 10+ |
| Drug | fact_sdud_combined | ndc | 12 |
| Quality Measure | fact_quality_core_set_2024 | measure_id | 8 |
| Rate Cell | fact_rate_comparison | composite (7 cols) | 3 |
| Geographic Area | county/state FIPS | fips_code | 50+ |
| Economic Indicator | fact_unemployment | (state, month) | 15+ |
| Enrollment Record | fact_enrollment | (state, month) | 5 |
| Expenditure Record | fact_cms64_multiyear | (state, service, FY) | 3 |
| HCBS Program | fact_hcbs_waitlist | (state, waiver_id) | 4 |
| Occupation | dim_bls_occupation | soc_code | 8 |
| Policy Document | fact_policy_document | doc_id | 2 |

### 28 Relationship Edges

Relationships connect entity types through fact tables. Examples:

- State `has_rates` -> Procedure via fact_rate_comparison (join: state_code, cpt_hcpcs_code)
- State `has_enrollment` -> Enrollment via fact_enrollment (join: state_code)
- Hospital `reported_in_hcris` -> State via fact_hospital_cost (join: provider_ccn, state_code)
- Drug `has_nadac_price` -> Drug pricing via fact_nadac (join: ndc)
- Provider `operates_in_state` -> State via fact_nppes_provider (join: npi, state_code)

### Named Metrics (Deterministic)

Key calculations defined once in `ontology/metrics/` with explicit formulas:

| Metric | Formula | Source Table |
|--------|---------|-------------|
| pct_of_medicare | medicaid_rate / medicare_nonfac_rate | fact_rate_comparison |
| cpra_pct_of_medicare | Claim-weighted, $32.3465 CF, 68 CMS codes, 3 categories | fact_rate_comparison + utilization |
| per_enrollee_spending | CMS-64 total / enrollment (CHIP excluded) | fact_cms64_multiyear + fact_enrollment |
| managed_care_penetration | mc_enrollment / total_enrollment | fact_mc_enrollment_summary |
| enrollment_change_pct | (current - prior) / prior | fact_enrollment |
| implied_conversion_factor | medicaid_rate / total_rvu | fact_medicaid_rate + dim_procedure |
| rate_decay_index | current_pct_of_medicare / baseline_pct_of_medicare | Longitudinal rate comparison |

**Design principle:** Same question always produces the same number. Named metrics eliminate ambiguity in Intelligence responses.

### Auto-Generated System Prompt

`scripts/generate_ontology.py` reads all YAML files and produces a ~33.7KB system prompt section containing:
- Entity types with properties and relationships
- Domain groupings with table descriptions, row counts, and quality tier badges
- Named metrics with formulas and caveats
- Data quality notes surfaced directly to Intelligence
- Domain-specific intelligence context (how to answer domain questions)

This replaces any hand-maintained table documentation and stays in sync with the actual data lake automatically.

### DuckPGQ Property Graph

The same script generates a `CREATE PROPERTY GRAPH medicaid` SQL statement for DuckPGQ:
- Vertex tables = canonical entity tables (deduplicated)
- Edge tables = fact tables connecting entities (with SOURCE KEY / DESTINATION KEY)
- Enables graph pattern matching via SQL/PGQ (SQL:2023 standard)

### Validation

`scripts/validate_ontology.py` runs in CI and performs 8 checks:
1. Every entity YAML conforms to master schema
2. Every relationship references a valid target entity
3. Every `via_table` exists in the DuckDB lake
4. Every `join_key` exists as a column in the `via_table`
5. Every metric references a valid `source_table`
6. Every domain references valid entities and tables
7. No orphan tables (in lake but not in any domain)
8. No orphan entities (with zero fact tables)

### How to Add a New Dataset

1. Run ETL (fetch -> parse -> validate -> normalize -> load)
2. Add table to `db.py` `FACT_NAMES`
3. Create/update entity YAML -- add table to `fact_tables` and new relationships
4. Update domain YAML -- add to primary/supporting tables with quality_tier, known_issues
5. Add new metrics to `ontology/metrics/` if applicable
6. Run `python scripts/validate_ontology.py` -- catches broken references
7. Run `python scripts/generate_ontology.py` -- regenerates system prompt + DuckPGQ
8. Intelligence immediately knows about the new dataset

---

## 5. Intelligence Engine

Intelligence is Aradune's primary interface. It is a Claude-powered analytical assistant with direct query access to the entire data lake, RAG over policy documents, and web search for current regulatory context.

### How Intelligence Works

```
User query (natural language, from chat or "Ask about this")
    |
    +-- Intelligence receives: query + conversation history
    |   + context (if from tool: module, state, section, data summary)
    |   + user data metadata (if imported files exist)
    |
    +-- System prompt includes (auto-generated from ontology/):
    |     Entity types with properties and relationships
    |     Domain groupings with table descriptions and row counts
    |     Named metrics with deterministic formulas
    |     Domain-specific intelligence context and caveats
    |
    +-- Tools available to Intelligence:
    |     query_database    -> SELECT-only DuckDB over all lake + user temp tables
    |     list_tables       -> Browse tables by domain (reads from entity registry)
    |     describe_table    -> Schema, row counts, sample data
    |     web_search        -> Current policy/regulatory context (Anthropic built-in)
    |     search_policy     -> RAG over 1,039+ CMS docs (BM25 + FTS)
    |
    +-- Intelligence executes multi-step analysis
    |   (for relationship-heavy queries, can use DuckPGQ graph pattern matching)
    |
    +-- Output (streamed via SSE):
          narrative     -> analysis + interpretation (token-by-token)
          tables        -> clean, labeled, exportable (JSON metadata event)
          charts        -> specs for frontend rendering
          queries       -> SQL trace (collapsible, auditable)
          citations     -> sources with vintage + caveats
          web_sources   -> policy/regulatory URLs
```

### Query Router (4 Tiers)

Questions are classified by a Haiku classifier (~100ms, ~$0.001). The system always errs up.

| Tier | Type | Model | Thinking | Max Queries | Tools | Target |
|------|------|-------|----------|-------------|-------|--------|
| 1 | Lookup | Sonnet 4.6 | No | 2 | DuckDB | <1s |
| 2 | Comparison | Sonnet 4.6 | No | 4 | DuckDB | 1-3s |
| 3 | Analysis | Sonnet 4.6 | 5K budget | 12 | DuckDB + RAG | 5-15s |
| 4 | Synthesis | Opus 4.6 | 10K budget | 15 | DuckDB + RAG + Web | 15-45s |

**Bump-up rules:**
- User data present -> minimum Tier 3
- Compliance terms detected (CPRA, 42 CFR, SPA, rate transparency, AHEAD, fiscal impact) -> Tier 4

**Classification method:**
1. Haiku classifier (fast, ~$0.001) if API key available
2. Heuristic keyword fallback (compliance terms -> Tier 4, analysis terms -> Tier 3, comparison -> Tier 2)
3. System takes `max(haiku, heuristic)`

### RAG Engine (Policy Corpus)

**Corpus:** 1,039 CMS documents, 6,058 searchable chunks (CIBs, SHOs, SMDs, federal register rules)

**Search method hierarchy:**
1. Hybrid BM25 + vector (if embeddings + VOYAGE_API_KEY available)
2. BM25 only via DuckDB FTS (default)
3. ILIKE keyword fallback (if FTS unavailable)

**Initialization:** `_init_fts()` materializes `fact_policy_chunk` view into real table `_fts_policy_chunk`, builds FTS index on chunk_id, text, section_title. Lazy on first use.

**Result enrichment:** Joins chunks with document metadata (title, doc_number, source_url, effective_date, publication_date, summary). Returns top 10 results.

### SSE Event Sequence

```
event: status\ndata: {"status": "thinking"}
event: tool_call\ndata: {"name": "query_database", "purpose": "Looking up FL rates"}
event: tool_result\ndata: {"name": "query_database", "rows": 45, "ms": 23}
event: token\ndata: {"text": "Florida's"}
...
event: metadata\ndata: {"tables": [...], "charts": [...], "queries": [...], "citations": [...]}
event: done\ndata: {}
```

### Safety Controls

**SQL validation:** All queries must start with SELECT/WITH. 15 forbidden keywords (INSERT, UPDATE, DELETE, DROP, ALTER, TRUNCATE, CREATE, REPLACE, GRANT, REVOKE, ATTACH, COPY, EXPORT, IMPORT, LOAD) blocked by regex.

**Auto-LIMIT:** Appends `LIMIT 100` if not specified.

**Minimum cell size:** n >= 11 for utilization counts.

**DuckDB query timeout:** `SET statement_timeout=30000` executed before every `_run_query()` call. Prevents runaway queries from consuming resources.

**Anthropic API timeout:** `httpx.Timeout(120.0, connect=10.0)` -- 120 seconds total, 10 seconds connect.

**DOGE quarantine (Session 34 -- code-level enforcement):** `_detect_doge_tables()` checks SQL for any of the 5 DOGE table names. When detected, `MANDATORY_CAVEATS` array is injected into the tool result JSON, ensuring Claude sees the caveats as part of the data response (not just the system prompt). All 5 caveats included: OT-only, provider state, MC distortion, Nov/Dec incomplete, dataset offline.

**IL T-MSIS caveat (Session 34 -- code-level enforcement):** `_detect_il_claims()` checks for IL state code + claims/tmsis table references in SQL. Injects the incremental credits/debits dedup warning into tool results.

**Territory-aware fallback (Session 34):** When Intelligence generates an empty response and the query mentions a territory (GU/PR/VI/AS/MP), returns a helpful message about territory data limitations instead of a generic 74-char fallback.

**Response post-processing (Session 34):** `_postprocess_response()` strips em-dashes (U+2014), en-dashes (U+2013), and double-hyphen dashes from all responses. Also cleans up double commas from replacements.

**Repetition guard:** Detects looping model output (same token repeated 10+ times) and truncates.

**Trace storage:** `fact_intelligence_trace` table stores every query with trace_id, query_text, domain, tier, skill_ids_retrieved, sql_queries, model_used, response_length, response_time_ms, feedback. Trace_id included in SSE metadata events so frontend can reference specific queries for feedback.

### Model Costs

| Query type | Model | Cost/query |
|---|---|---|
| Classification, routing, PDF extraction | claude-haiku-4-5-20251001 | ~$0.004 |
| NL2SQL, RAG, standard analysis | claude-sonnet-4-6 | ~$0.03-0.06 |
| SPA drafting, CPRA narrative, AHEAD, complex reasoning | claude-opus-4-6 | ~$0.28 |

### Response Rules (Intelligence Always Follows)

1. Specify data vintage ("Based on CY2022 T-MSIS claims" -- never "current")
2. Flag data quality issues (DQ Atlas for any state)
3. Minimum cell size n >= 11
4. T-MSIS encounter amounts unreliable for MCO-to-provider rates
5. FL Medicaid: Facility and PC/TC rates are typically mutually exclusive (99.96% of codes). Three codes (46924, 91124, 91125) legitimately carry both as published by AHCA.
6. CPRA: $32.3465 CF (CY2025). General: $33.4009 (CY2026)
7. Census sentinels (-888888888) = NULL
8. SELECT-only. Never modify data.
9. CHIP excluded from per-enrollee calculations
10. No em-dashes. No "plain English."
11. Lead with narrative and "so what." Cross-reference multiple domains.
12. Clean markdown tables. Cite sources with vintage and caveats.
13. When finding implies action, state it.

### Skillbook v2 (Self-Corrective Intelligence Layer)

The Skillbook is a persistent, self-curating layer of Medicaid domain intelligence that sits between the ontology and Claude. It learns from every query: what reasoning worked, what failed, what domain rules matter. Session 34 upgraded it to v2 with CRUSP lifecycle, score decay, graph expansion, and automated pruning.

**Table:** `fact_skillbook` in DuckDB (same lake, same query access)

**Skill categories:**
- `strategy` -- effective reasoning patterns for common query types
- `caveat` -- data quality warnings learned from experience
- `failure_mode` -- reasoning paths that produced wrong answers
- `domain_rule` -- regulatory/policy rules that must always be applied
- `query_pattern` -- SQL patterns that work for common questions
- `factual_accuracy` -- ground-truth facts that must be correct
- `caveat_enforcement` -- quarantine and data quality rules that must trigger

**Current state:** 24+ seed skills from existing build rules, audit findings, and adversarial test anchors (28 known_facts.json entries importable via `--anchors`). Auto-learning active via async Reflector v2 (Haiku, ~$0.004/reflection, non-blocking).

**Schema (v2 additions):**
- `last_validated_at` -- reset on any feedback (refreshes decay clock)
- `decay_half_life_days` -- default 30 days
- `related_skills` -- VARCHAR array for graph edges (bidirectional linking)
- `prune_reason` -- why a skill was retired (stored for audit)

**Score decay:** `effective_score = net_score * pow(2, -(days_elapsed / half_life_days))`. Default half-life 30 days. Negative scores do not get decay benefit. All retrieval now sorted by effective_score descending.

**Graph expansion:** After initial retrieval (domain rules + keyword match), reads `related_skills` arrays and does 1-hop fetch for linked skills. Linked skills flagged with `via_link=True` so Intelligence knows they are contextually related, not directly matched.

**Retrieval:** Domain-filtered by effective_score + BM25 text match against query. Injected into Intelligence system prompt between ontology and user query. Graph-expanded skills included with provenance flag.

**Scoring:** Each skill has helpful_count, harmful_count, net_score. `update_score` resets `last_validated_at` (refreshes decay clock). Skills with sustained negative scores are retired. Skills validated 3+ times are marked as high-confidence.

**CRUSP Lifecycle (Create, Retrieve, Update, Score, Prune):**
- **Create:** Manual add, adversarial import (skillbook_import.py), or reflector extraction
- **Retrieve:** Domain + keyword + graph expansion, sorted by effective_score
- **Update:** Feedback (thumbs up/down), score adjustment, link management
- **Score:** Decay-adjusted scoring with half-life, feedback resets clock
- **Prune:** `scripts/prune_skillbook.py` implements automated cleanup:
  - Harmful: net_score < -2 for 14+ days
  - Decayed+unused: effective_score < 0.5, times_retrieved == 0, 60+ days old
  - Oversized: >500 chars, logged for split consideration
  - Dry-run by default, `--apply` to execute

**Skill linking:** `link_skills(a, b)` creates bidirectional edges via JSON arrays in `related_skills` column. Used by graph expansion during retrieval.

**Reflector v2:** Async Haiku post-response analysis now proposes `proposed_links` between related skills and flags `split_candidates` (oversized skills >500 chars that should be decomposed).

**Feedback endpoint enhanced (Session 34):** Now accepts trace_id, looks up the trace from `fact_intelligence_trace`, passes recovered context (query, SQL, response) to reflector for targeted re-reflection.

**Pattern:** ACE framework (ICLR 2026) adapted for domain-specific regulatory analytics.

**Engines:**
- `server/engines/skillbook.py` (278+ lines) -- retrieval with graph expansion, injection, CRUD, decay scoring, skill linking
- `server/engines/reflector.py` (133+ lines) -- async post-response analysis, skill extraction, link proposals, split candidates

**API endpoints (6 total):**
- GET /api/skillbook -- list skills, optional domain filter
- GET /api/skillbook/stats -- health metrics (total, active, validated, suspect)
- GET /api/skillbook/recent -- most recent skills, ordered by created_at DESC
- POST /api/skillbook/manual -- manually add a skill
- POST /api/skillbook/add -- add a skill (JSON body, used by adversarial pipeline)
- DELETE /api/skillbook/{skill_id} -- retire a skill with optional prune_reason

### Adversarial Testing Suite (7 Agents)

Aradune's adversarial testing framework validates Intelligence quality, API reliability, data consistency, and UI behavior through 7 specialized agents. All agents are in `scripts/adversarial/`. Run the full suite with `python -m scripts.adversarial.runner`.

**Agent 1: Intelligence Agent** (existing from Session 32)
Scripted + LLM-generated queries testing Intelligence response quality: style checks (no em-dashes, proper citations, data vintage), factual accuracy against known_facts.json, caveat enforcement (DOGE quarantine, IL dedup, territory warnings), and edge case handling.

**Agent 2: API Fuzzer Agent** (existing from Session 32)
Tests all ~345 endpoints for 500 errors with invalid inputs, missing parameters, malformed requests, boundary values, and injection attempts. Session 32 result: 100% pass rate across all endpoints.

**Agent 3: Consistency Agent** (existing from Session 32)
Cross-checks data consistency across related tables and endpoints. Loads ground-truth facts from `known_facts.json` (28 facts across 11 domains) and validates that Intelligence and API responses match. Session 32 result: 85.7% pass rate.

**Agent 4: Persona Agent** (existing from Session 32)
Tests Intelligence from different user personas (state Medicaid analyst, consulting actuary, investigative journalist, legislative staffer) to verify responses are appropriately contextualized and persona-appropriate.

**Agent 5: Florida Rate Agent** (new in Session 34)
4 SQL data-layer tests validating FL rate structure directly against the lake, plus 7 Intelligence endpoint tests validating that Intelligence correctly handles FL rate questions. Key validation: 99.96% of codes have either facility OR PC/TC rates, but 3 codes (46924, 91124, 91125) legitimately carry both facility and PC/TC rates as published by AHCA. Cost: ~$0.50 per run.

**Agent 6: Skillbook Agent** (new in Session 34)
5 poisoning resistance tests (false facts injected as skills should not contaminate Intelligence responses), 2 compounding tests (valid skills should measurably improve response quality), 4 integrity checks (schema validation, score range enforcement, contamination scan for known-bad patterns, domain distribution balance).

**Agent 7: Browser Agent** (new in Session 34)
8 Playwright end-to-end UI tests: homepage load, JS error-free navigation across 7 routes, mobile viewport (390x844), rapid state switching in State Profiles, Intelligence SSE rendering, Cmd+K search functionality, export during active data load. Cost: $0 (no LLM calls).

**Ground truth:** `scripts/adversarial/fixtures/known_facts.json` contains 28 anchor facts across 11 domains (rates, enrollment, claims, hospitals, nursing, pharmacy, quality, expenditure, workforce, behavioral health, program integrity). These are verified against actual data and serve as the consistency baseline.

**Runner:** `python -m scripts.adversarial.runner` with flags:
- `--agent <name>` -- run specific agent only
- `--quick` -- skip expensive LLM-based tests
- `--export <path>` -- save results to JSON file
- `--json` -- output results as JSON to stdout

**Cost per run:**
| Suite | Cost |
|-------|------|
| Full (all 7 agents) | $7-13 |
| Quick (--quick flag) | $5-9 |
| Florida Rate only | ~$0.50 |
| Browser only | $0 (no LLM) |

### Adversarial-to-Skillbook Pipeline

The adversarial testing suite is connected to the Skillbook via `scripts/adversarial/skillbook_import.py`, creating a closed feedback loop: adversarial tests find weaknesses, failures are converted to learnable skills, and Intelligence improves automatically.

**How it works:**
1. Adversarial runner produces JSON report with test results
2. `skillbook_import.py` reads the report and extracts lessons from failures
3. Each failure is converted to a Skillbook entry with:
   - Domain inferred from query keywords (17 domain keyword sets covering rates, enrollment, claims, hospitals, nursing, workforce, pharmacy, behavioral health, quality, expenditure, economic, medicare, policy, ltss_hcbs, public_health, maternal_child, program_integrity)
   - Category mapped from failure type (hallucination -> factual_accuracy, quarantine_bypass -> caveat_enforcement, style_violation -> strategy, etc.)
   - Content derived from the test assertion and expected behavior
4. Deduplication: checks existing skills before adding (substring match on first 80 chars)
5. Skills are inserted with initial score of 1.0 and source="adversarial"

**Anchor import:** `--anchors` flag imports all 28 known_facts.json entries as baseline Skillbook skills with category `factual_accuracy` and domain derived from each fact's domain field.

**GitHub Actions workflow:** `.github/workflows/adversarial.yml` runs weekly (Sunday 2AM UTC):
1. Runs full adversarial suite against deployed Fly.io instance
2. Auto-imports lessons from failures to Skillbook via skillbook_import.py
3. Creates a GitHub issue on critical failure (any agent with >20% failure rate)
4. Uploads JSON report as workflow artifact for audit trail

---

## 6. Backend

### FastAPI Application (server/)

**Entry point:** `server/main.py`
**Framework:** Python FastAPI with uvicorn
**Total:** ~345 endpoints across 40+ route files (27 top-level + 13 research modules). All endpoints protected by @safe_route error handler (except SSE streaming and file import validation endpoints which have their own error handling).

### Server Startup Sequence

1. `init_db()` creates in-memory DuckDB connection
2. Background thread `_register_all_views()` scans `/data/lake/` and registers 750+ Parquet views
3. Background thread `_delayed_rescan()` polls every 30s for new files
4. FastAPI app boots; `/health` responds immediately (before lake ready)
5. `/ready` returns 200 only once 650+ views registered

### Key Files

| File | Lines | Purpose |
|------|-------|---------|
| server/main.py | ~200 | FastAPI app, lifespan, CORS, router imports |
| server/db.py | ~400 | DuckDB connection, view registration (667 FACT_NAMES) |
| server/config.py | ~50 | Settings (lake_dir, CORS, max_rows, port) |
| server/query_builder.py | ~150 | Safe SQL construction |
| server/middleware/auth.py | 214 | Clerk JWT validation (RS256 via JWKS) |

### Engines

| Engine | File | Lines | Purpose |
|--------|------|-------|---------|
| Intelligence | routes/intelligence.py | 1,530 | SSE streaming, Claude integration, tool execution |
| Query Router | engines/query_router.py | ~250 | Tier 1-4 classification via Haiku + heuristics |
| RAG | engines/rag_engine.py | ~460 | BM25 + FTS over 1,039 policy docs |
| Caseload Forecast | engines/caseload_forecast.py | ~650 | SARIMAX + ETS model competition |
| Expenditure Model | engines/expenditure_model.py | ~430 | Enrollment -> expenditure projection |
| CPRA Upload | engines/cpra_upload.py | 821 | 42 CFR 447.203 compliant CPRA from file upload |
| Skillbook v2 | engines/skillbook.py | 278+ | Domain skill retrieval with graph expansion, injection, CRUD, decay scoring, skill linking, CRUSP lifecycle |
| Reflector v2 | engines/reflector.py | 133+ | Async post-response skill extraction via Haiku, link proposals, split candidates |
| Validator | engines/validator.py | 98 | 15+ data quality checks (row count, range, RI) |
| System Dynamics | engines/system_dynamics.py | ~512 | Stock-flow ODE modeling (enrollment, provider, workforce, HCBS, integrated) |

### Route Files (40+ total: 27 top-level + 13 research)

| File | Endpoints | Purpose |
|------|-----------|---------|
| intelligence.py | 5 | SSE chat, status, feedback (POST /api/intelligence) |
| behavioral_health.py | 109 | NSDUH, TEDS, facilities, block grants, opioid, conditions, services |
| round9.py | 29 | Medicare enrollment, opioid prescribing, CHIP, SDUD 2024/2025, integrity |
| context.py | 24 | Demographics, economic, housing, SNAP, TANF, eligibility, LTSS, maternal |
| forecast.py | 12 | Caseload + expenditure + fiscal impact + scenario |
| cpra.py | 11 | CPRA states, rates, DQ, compare, upload generate |
| wages.py | 10 | BLS wages, HPSAs, MUAs, shortage areas |
| quality.py | 13 | Core Set measures, state detail, HAC |
| lake.py | 7 | State data, enrollment, quality, expenditure, spending |
| bulk.py | 7 | Pre-computed Medicare/Medicaid rates, GPCI, quality, states |
| supplemental.py | 7 | DSH, SDP, FMR supplemental payments |
| hospitals.py | 6 | Summary, state list, CCN detail, peers, hospital rates |
| import_data.py | 5 | User file upload, parse, sessions, quarantine, hydrate |
| policy.py | 5 | SPAs, waivers, managed care, FMAP, DSH |
| pharmacy.py | 5 | SDUD state summary, top drugs, NADAC |
| enrollment.py | 4 | Monthly, unwinding, managed care |
| skillbook.py | 6 | Skillbook CRUD, stats, manual add, recent, add (JSON body) |
| validation.py | 3 | Validation latest, results, domains |
| meta.py | 3 | Table schema, catalog, stats |
| rate_explorer.py | 2 | Rate search across jurisdictions |
| nl2sql.py | 2 | Natural language to SQL (Haiku) |
| insights.py | 2 | Pre-computed insights |
| staffing.py | 2 | PBJ summary and state |
| search.py | 1 | Platform-wide Cmd+K search |
| query.py | 1 | Generic DuckDB query builder |
| presets.py | 1 | Saved query presets |
| pipeline.py | 1 | Data pipeline triggers |
| dynamics.py | 5 | System dynamics API (enrollment, provider, workforce, HCBS, policy-simulator) |
| state_context.py | 1 | Universal state context (12 queries, 1hr cache) |
| research/ (13 files) | 55 | 13 research module endpoints |

### Authentication

Two modes:
1. **Clerk** (when `CLERK_SECRET_KEY` set): RS256 JWT validation from `Authorization: Bearer` header or `__session` cookie, returns user metadata via JWKS
2. **Open mode** (fallback when Clerk not configured): Anonymous stub user returned, all endpoints accessible. Frontend shows client-side PasswordGate component (not a security boundary)

### Configuration

```python
lake_dir = "/app/data/lake"       # Fly.io; locally ~/Desktop/Aradune/data/lake/
cors_origins = ["http://localhost:5173", "https://aradune.co", "https://www.aradune.co"]
max_rows = 10_000
port = 8000
```

**Environment variables:**
- `ANTHROPIC_API_KEY` -- required for Intelligence
- `VOYAGE_API_KEY` -- optional, for vector search RAG
- `ARADUNE_S3_BUCKET`, `ARADUNE_S3_ENDPOINT`, `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` -- R2 credentials
- `CLERK_SECRET_KEY` -- Clerk auth (if configured)

---

## 7. Frontend

### React Application (src/)

**Framework:** React 18 + TypeScript + Vite
**Routing:** Hash-based in Platform.tsx (no React Router)
**Styling:** Inline styles only (no CSS files, no Tailwind)
**Charts:** Recharts with ResponsiveContainer
**State:** Single AraduneContext (React Context, not Redux)

### Platform Shell

```
Platform (top-level with auth)
  +-- ClerkAuthProvider (when configured) OR PasswordGate (client-side fallback)
  +-- PlatformInner (the app shell)
      +-- PlatformNav (sticky, 48px, 5 nav groups)
      +-- renderRoute() (switch on window.location.hash)
      +-- IntelligencePanel (sidebar, position: fixed, 420px)
      +-- ReportBuilder (floating button + panel)
      +-- Footer
```

### Navigation (5 Groups)

| Group | Tools |
|-------|-------|
| States | State Profiles (comparison mode supported) |
| Rates & Compliance | Rate Comparison, CPRA, Rate Lookup, Compliance Center |
| Finance | Caseload/Fiscal Impact, Spending Efficiency, Hospital Rates, AHEAD, Policy Simulator |
| Clinical | Pharmacy, Behavioral Health, Nursing, Program Integrity |
| Research | 13 research briefs |

Desktop: horizontal dropdown nav. Mobile (<768px): hamburger menu.

### Application State (AraduneContext)

```typescript
{
  selectedState: string | null,
  comparisonStates: string[],
  intelligenceOpen: boolean,
  intelligenceContext: IntelligenceContext | null,
  importedFiles: ImportedFile[],
  reportSections: ReportSection[],
  demoMode: boolean   // ?demo=true URL parameter
}
```

Session-scoped. Cleared on page refresh. No localStorage persistence. State persists across tool navigation (FL in States = FL pre-selected in Rates).

### Design System (design.ts)

```typescript
C = {
  ink: "#0A2540",        brand: "#2E6B4A",      accent: "#C4590A",
  surface: "#F5F7F5",    border: "#E4EAE4",     pos: "#2E6B4A",
  neg: "#A4262C",        warn: "#B8860B"
}
FONT = { mono: "SF Mono, Menlo, Consolas, monospace", body: "Helvetica Neue, Arial" }
BP = { mobile: 768, tablet: 1024 }
useIsMobile(breakpoint = 768) -> boolean
```

### Key Files

| File | Lines | Purpose |
|------|-------|---------|
| Platform.tsx | ~980 | Router, tool registry, PasswordGate, landing page |
| design.ts | 46 | Color/font/breakpoint tokens, useIsMobile() |
| types.ts | 418 | All TypeScript interfaces |
| IntelligenceChat.tsx | ~850 | Full-page Intelligence chat |
| IntelligencePanel.tsx | 270 | Sidebar chat from any tool |
| ReportBuilder.tsx | 200+ | Export panel, 4 formats |
| PlatformSearch.tsx | 700 | Cmd+K search (67 synonyms) |
| StateRateEngine.js | 1,153 | Rate Builder engine (42/42 tests) |

### Lazy Loading

All 27+ tools are `lazy(() => import(...))` + code-split. `<Suspense fallback={<SwordLoader />}>` wraps each with error boundary. Export libraries (docx 356KB, jspdf 358KB, xlsx 429KB) only loaded on export click.

### Intelligence Integration Points

1. **Full-page chat** (`/#/intelligence`): IntelligenceChat.tsx, 6 starter prompts, streaming markdown, query trace
2. **Sidebar panel**: IntelligencePanel.tsx, opens from any tool via `openIntelligence(context)`, context-aware
3. **"Ask Aradune" buttons**: On every tool, pre-fills question with current state/section

### API Integration

All tools communicate via `apiFetch<T>(path, fallbackPath?)`:
1. Try API endpoint (VITE_API_URL, defaults to Fly.io)
2. If API unreachable, try static JSON fallback in `/public/data/`
3. If both fail, throw error

---

## 8. Structured Modules (28 Total)

### 15 Core Modules

#### 1. Intelligence (Home Page)
- **Route:** `/#/` and `/#/intelligence`
- **File:** IntelligenceChat.tsx (~850 lines)
- **Function:** Full-page Claude chat with direct DuckDB access to all 750+ tables. 6 starter prompts by persona. Streaming markdown. Query trace. CSV export.

#### 2. State Profiles
- **Route:** `/#/state/{code}` (supports `/#/state/FL+GA+TX` for comparison)
- **File:** StateProfile.tsx (~1,000 lines)
- **Function:** 7-section dashboard (enrollment, rates, hospitals, quality, workforce, pharmacy, economic + SDOH). 20 parallel API fetches. Comparison mode with side-by-side columns. Cross-dataset insights (cached). SDOH section: ADI, food deserts, dental/MH HPSAs, MUA/MUP.

#### 3. Rate Comparison (Browse & Compare)
- **Route:** `/#/rates`
- **File:** TmsisExplorer.tsx (~2,400 lines)
- **Function:** Browse Medicaid rates across 47 states + Medicare benchmark. Category breakdown, trend analysis, state-level detail. Wraps the largest component in the app.

#### 4. CPRA Compliance
- **Route:** `/#/cpra`
- **File:** CpraGenerator.tsx (734 lines)
- **Function:** Pre-computed cross-state comparison (45 states, 302K rows) + user-upload generator (two CSVs -> full 42 CFR 447.203 CPRA in <2 seconds). PDF/Excel/HTML export. See Section 11 for full detail.

#### 5. Rate Lookup & Directory
- **Route:** `/#/lookup`
- **File:** RateLookup.tsx + FeeScheduleDir.tsx (535 lines)
- **Function:** Code-level Medicaid rate lookup across 47 states. State fee schedule directory with download links. Quick trust-building tool.

#### 6. Caseload & Fiscal Forecasting
- **Route:** `/#/forecast`
- **File:** CaseloadForecaster.tsx (~830 lines)
- **Function:** SARIMAX + ETS model competition. Dual-mode: public data or user upload. 3 tabs: Caseload, Expenditure, Scenario Builder (4 sliders). See Section 12 for full detail.

#### 7. Spending Efficiency
- **Route:** `/#/spending`
- **File:** SpendingEfficiency.tsx (752 lines)
- **Function:** 3 tabs: Per-Enrollee Spending (MACPAC), Total Expenditure (CMS-64 FY2018-2024), Efficiency Metrics (scatter: spending vs MC penetration). Data: 118K rows CMS-64, $5.7T total computable.

#### 8. AHEAD Readiness
- **Route:** `/#/ahead`
- **Files:** AheadReadiness.tsx + AheadCalculator.tsx
- **Function:** Hospital readiness scoring for CMS AHEAD model. 3-year 10/30/60 CMS baseline, +/-2% volume corridor, commercial payer engine (PY2+), TIA PY1/PY2 limited, TCOC PY4 upside-only. HCRIS financials, payer mix, peer benchmarks. 6 states: MD (live), CT/HI/VT (2028), RI/NY (2028).

#### 9. Hospital Rate Setting
- **Route:** `/#/hospital-rates`
- **File:** HospitalRateSetting.tsx (436 lines)
- **Function:** 3 tabs: Hospital Financials (HCRIS cost reports, 18K rows), DSH & Supplemental (MACPAC Exhibit 24, 6K rows), State Directed Payments (34 states).

#### 10. Nursing Facility
- **Route:** `/#/nursing`
- **File:** NursingFacility.tsx (662 lines)
- **Function:** 3 tabs: Quality Ratings (Five-Star summary, 14.7K facilities), Staffing (PBJ nurse staffing, 1.3M rows), State Detail (facility-level drilldown).

#### 11. Behavioral Health & SUD
- **Route:** `/#/behavioral-health`
- **File:** BehavioralHealth.tsx (627 lines)
- **Function:** 4 tabs: Prevalence (NSDUH 26 measures), Treatment Network (facilities/beds, IPF quality, block grants), Opioid Crisis (prescribing rates, 539K rows), Conditions & Services.

#### 12. Pharmacy Intelligence
- **Route:** `/#/pharmacy`
- **File:** PharmacyIntelligence.tsx (408 lines)
- **Function:** 3 tabs: Spending Overview (SDUD 2025 state summary), Top Drugs (by spending, filterable by state), NADAC Pricing (drug name search, 1.9M rows).

#### 13. Program Integrity
- **Route:** `/#/integrity`
- **File:** ProgramIntegrity.tsx (654 lines)
- **Function:** 3 tabs: Exclusions (LEIE 82K), Open Payments ($13.18B, all 3 CMS categories), MFCU & PERM (error rates 2020-2025).

#### 14. Workforce & HCBS
- **Route:** `/#/workforce`
- **Files:** WageAdequacy.tsx (546), QualityLinkage.tsx (445), HcbsCompTracker.tsx (414)
- **Function:** 4 tabs: Wage Adequacy (BLS market wages vs Medicaid), Quality Linkage (spending vs outcomes), HCBS Waitlists & Compensation (607K waiting, 80% pass-through tracking for July 2028 deadline), Shortage Areas (HPSA 69K + MUA map).

#### 15. Policy Simulator
- **Route:** `/#/policy-simulator`
- **File:** PolicySimulator.tsx (~500 lines)
- **Function:** System dynamics: model downstream effects of rate changes, wage increases, HCBS funding, economic shocks through interconnected feedback loops. Stock-flow ODE modeling (scipy.integrate.solve_ivp), 12 stocks, 6 cross-domain feedback loops, lake-calibrated parameters. 5 presets. Baseline vs scenario comparison. 4 embedded DynamicsWidget instances in CaseloadForecaster, WageAdequacy, HcbsTracker, RateBrowse.

### Additional Modules

- **Compliance Center** (`/#/compliance`): 42 CFR 447.203 requirements dashboard
- **Data Catalog** (`/#/catalog`): Power user table browser with schema inspection

---

## 9. Research Modules (13 Academic Briefs)

Research modules are findings-forward briefs with collapsible methods and replication sections. They are NOT interactive dashboards. Each runs statistical analysis against the data lake and presents results in academic paper format.

### Advanced Statistical Methods

Implemented in `scripts/research_advanced_methods.py` (~650 lines):

| Method | Purpose |
|--------|---------|
| OLS with HC1 Robust SE | Standard regression with heteroskedasticity correction |
| IV/2SLS | Instrumental variables for endogeneity (GPCI as instrument) |
| VIF Diagnostics | Multicollinearity detection (VIF > 10 = severe) |
| Propensity Score Matching | Causal inference from observational data (matched pairs) |
| CHOW Event Study | Quality changes around ownership transitions |
| Random Forest | Feature importance with cross-validation R-squared |
| Quantile Regression | Heterogeneous effects at p10/p25/p50/p75/p90 |
| Difference-in-Differences | Expansion treatment effects with leads + lags |
| K-means Clustering | Treatment desert identification |

### Module 1: Rate-Quality Nexus

**Question:** Does paying Medicaid providers more improve quality?

**Finding:** p=0.044 -- rates DO predict quality (beta=0.070, robust SE). N=41 states.

**Data:** fact_rate_comparison, fact_quality_core_set_2024, fact_hpsa, fact_bls_wage, fact_mc_enrollment_summary

**Methods:** OLS, IV/2SLS (GPCI as instrument), VIF diagnostics. Original p=0.178 was wrong due to SVI multicollinearity bug (VIF > 10 for SVI + poverty). After dropping collinear SVI variable, p=0.044.

**Sensitivity note:** Result depends on N=41 (AK/CT COALESCE'd). Not robust to all specifications. This is the single most important research finding and went through extensive audit.

**Endpoints:** 5 (overview, scatter, regression, panel, replication)

### Module 2: Managed Care Value Assessment

**Question:** Is managed care saving money or retaining margin?

**Finding:** -$9.2/enrollee (p=0.337, NOT significant). Quality declines with MC penetration. $120B MCO retention (revenue minus medical spending).

**Data:** fact_mc_enrollment_summary, fact_macpac_spending_per_enrollee, fact_mco_mlr, fact_cms64_multiyear

**Methods:** OLS on spending vs MC penetration, MLR analysis, retention = (1 - MLR) * MCO revenue

**Endpoints:** 5 (overview, spending, quality, mlr, retention)

### Module 3: Nursing Ownership & Quality

**Question:** Does for-profit ownership reduce nursing home quality?

**Finding:** -0.67 stars for for-profit (Cohen's d=0.50 raw, p<0.0001). PSM confirms: ATT=-0.67, 10,737 matched pairs.

**Data:** fact_five_star (14,710 facilities), fact_nh_deficiency

**Methods:** T-test, Cohen's d, PSM with balance diagnostics (standardized mean differences < 0.1), CHOW event study (4,952 ownership transfers)

**Endpoints:** 5 (overview, quality-by-type, deficiencies, staffing, ownership-changes)

### Module 4: Pharmacy Spread Analysis

**Question:** How much are states overpaying for drugs vs. acquisition cost?

**Finding:** $3.15B net overpayment (NADAC vs SDUD). Random Forest R-squared=0.75 (cross-validated). Low-cost generics drive 60% of overpayment.

**Data:** fact_sdud_combined (28.3M rows), fact_nadac (1.9M rows)

**Methods:** Latest NADAC per-unit price (ROW_NUMBER window) joined to SDUD 2025 aggregated reimbursement by NDC. Random Forest for feature importance.

**Endpoints:** 4 (overview, by-state, top-drugs, predictors)

### Module 5: Opioid Treatment Gap

**Question:** Where does OUD prevalence outstrip treatment capacity?

**Finding:** $1.16B MAT spending (was $0 -> $978M -> $1.16B -- progressively fixed truncated product names). Spatial Mismatch Index = 0.164. 26 Treatment Desert states identified via K-means clustering.

**Data:** fact_nsduh_prevalence (measure_id = 'oud_past_year'), fact_teds_admissions, fact_mh_facility, SDUD (MAT drugs: buprenorphine, suboxone, naloxone, naltrexone, vivitrol, sublocade)

**Methods:** K-means clustering (k chosen by elbow/silhouette), spatial mismatch index, treatment gap = prevalence - capacity

**Endpoints:** 4 (overview, mat-spending, mismatch, deserts)

### Module 6: Safety Net Stress Test

**Question:** Which states are under compound safety-net failure?

**Finding:** 20 states show compound failure across 4 dimensions.

**Formula:** Composite = hospital_stress + hcbs_pressure + nursing_deficit + fmap_rate

**Data:** fact_hospital_cost, fact_hcbs_waitlist (51 rows, state-level), fact_five_star, fact_fmap_historical

**Endpoints:** 4 (overview, components, rankings, states)

### Module 7: Integrity Risk Index

**Question:** Where is program integrity risk highest?

**Finding:** Composite scoring across Open Payments ($13.18B) + LEIE (82K exclusions) + PERM (error rates) + MFCU (investigations/convictions)

**Data:** fact_open_payments, fact_leie, fact_perm_rates, fact_mfcu_stats

**Endpoints:** 4 (overview, components, rankings, states)

### Module 8: Fiscal Cliff Analysis

**Question:** Which states hit the fiscal wall first?

**Finding:** ~$489/enrollee/yr spending growth. States with high Medicaid as % of revenue + high enrollment growth + low FMAP hit fiscal cliffs earliest.

**Formula:** medicaid_pct_of_revenue = state_share * 100 / total_tax_revenue

**Data:** fact_cms64_multiyear, fact_enrollment, fact_fmap_historical, fact_census_state_finances

**Endpoints:** 4 (overview, growth, exposure, rankings)

### Module 9: Maternal Health Deserts

**Question:** Where do mortality, access gaps, SVI, and quality compound?

**Finding:** Compound maternal health deserts identified via SMM x HPSA x SVI x quality composite

**Data:** fact_maternal_morbidity (435 rows), fact_hpsa, fact_svi_county (3,144 counties), fact_quality_core_set_2024, fact_infant_mortality_state

**Join complexity:** maternal_morbidity.geography -> dim_state.state_name via UPPER(), SVI via st_abbr, infant mortality via FIPS through fact_svi_county

**Endpoints:** 6 (overview, mortality, access, vulnerability, quality, composite)

### Module 10: Section 1115 Waiver Impact

**Question:** Do Section 1115 waivers improve outcomes?

**Finding:** 647 waivers cataloged. Before/after framework using enrollment, CMS-64, and quality trends around waiver approval dates.

**Data:** fact_section_1115_waivers (665 rows), fact_enrollment, fact_cms64_multiyear, fact_quality_core_set_combined

**Endpoints:** 5 (overview, catalog, enrollment-impact, spending-impact, quality-impact)

### Module 11: T-MSIS Calibration (New Session 30)

**Question:** How well do T-MSIS per-claim averages match published fee schedules?

**Finding:** T-MSIS claims systematically undercount actual Medicaid rates due to MC encounter reporting. State-level discount factors computed. TN simulated rates from claims (only MC state without published FFS schedule).

**Data:** fact_claims, fact_medicaid_rate, fact_rate_comparison_v2

### Module 12: MEPS Expenditure Analysis (New Session 30)

**Question:** How does individual-level spending differ by insurance type?

**Finding:** 22,431 MEPS respondents analyzed. Medicaid vs private vs uninsured spending and utilization patterns.

**Data:** fact_meps_hc243 (22,431 respondents from MEPS HC-243)

### Module 13: Network Adequacy (New Session 30)

**Question:** Where do provider supply gaps create access barriers for Medicaid enrollees?

**Finding:** Provider network analysis combining NPPES supply data, HPSA designations, enrollment demand, and geographic access metrics.

**Data:** fact_nppes_provider (9.37M), fact_hpsa (69K), fact_enrollment, fact_fqhc_sites (19K)

### Research Audit

Two audit guides govern research quality:
- **RESEARCH_AUDIT_GUIDE.md (v1):** 8 prompts to discover problems from scratch. Used in Session 29.
- **RESEARCH_AUDIT_GUIDE_v2.md:** 8 prompts to verify fixes and stress-test remaining. Used in Session 30.

**43 formal tests** across 6 categories:
- Schema Validation (SV-01 through SV-08)
- Data Accuracy Benchmarks (DA-01 through DA-10)
- Statistical Method Verification (SM-01 through SM-06)
- Frontend-Backend Contract Tests (FB-01 through FB-06)
- Edge Case Tests (EC-01 through EC-10)
- Performance Tests (PT-01 through PT-06)
- Cross-Module Consistency Tests (CM-01 through CM-07)

**Corrected headline numbers (authoritative after Session 30 audit):**
- Rate-Quality: p=0.044 (was 0.178)
- Pharmacy spread: $3.15B net overpayment
- MCO retention: $120B
- MAT spending: $1.16B
- Nursing ownership: -0.67 stars (d=0.50)
- Quality trend: -1.2pp/yr
- Spending growth: ~$489/enrollee/yr

---

## 10. ETL Pipeline

### Core Pattern

All 115+ ETL scripts follow the same 5-step pattern:

```python
def build_lake_table():
    raw = fetch_raw(source_url)        # HTTP GET + ETag caching
    parsed = parse(raw)                # CSV/JSON/XLSX/PDF parsing
    validated = validate(parsed)       # Schema + quality checks
    normalized = normalize(validated)  # Unified schema + metadata
    load(normalized, lake_path)        # Parquet + snapshot versioning
```

Every record gets: `_ingestion_timestamp`, `_source_file` (URL), `_batch_id` (UUID).

### Hard Stops (Fail Immediately)

- Rate changed >90% from prior snapshot
- Code count dropped >20%
- Schema mismatch (expected columns missing)
- NULL state_code or invalid 2-letter code

### Soft Flags (Warn, Continue)

- Rate unchanged >24 months
- New codes without description
- Rate >3 standard deviations from national mean
- Cell size n <11

### Scripts by Domain (~115 scripts in scripts/)

| Domain | Count | Examples |
|--------|-------|---------|
| Rates | 8 | build_lake_medicaid_rate, build_lake_pfs_rvu, build_lake_clfs |
| Enrollment | 6 | build_lake_enrollment, build_lake_unwinding, build_lake_mc_enrollment |
| Claims | 7 | build_lake_sdud_*, build_lake_doge_*, build_lake_claims |
| Hospitals | 10 | build_lake_hcris, build_lake_dsh, build_lake_hospital_rating |
| Nursing | 6 | build_lake_five_star, build_lake_pbj_staffing, build_lake_mds |
| Workforce | 8 | build_lake_bls_wage, build_lake_hpsa, build_lake_workforce_projections |
| Pharmacy | 8 | build_lake_nadac, build_lake_sdud_combined, build_lake_opioid |
| Behavioral Health | 6 | build_lake_nsduh, build_lake_teds, build_lake_mh_facility |
| Quality | 6 | build_lake_quality_core_set, build_lake_scorecard |
| Expenditure | 8 | build_lake_cms64_multiyear, build_lake_fmap, build_lake_nhe |
| Economic | 12 | build_lake_acs, build_lake_bls_cpi, build_lake_bea_gdp |
| Medicare | 8 | build_lake_medicare_enrollment, build_lake_mssp_aco |
| Provider Network | 6 | build_lake_nppes, build_lake_pecos |
| Other | 16+ | Batch sessions, data gap fills, Medicaid.gov, KFF, SDOH |

### Orchestration (Dagster)

**File:** `pipeline/dagster_pipeline.py`
- 13 assets (data sources)
- 3 checks (quality validation)
- 3 jobs (ETL pipelines)
- 2 schedules (weekly/monthly refresh)

### R2 Sync

**Upload (local -> R2):** `python3 scripts/sync_lake_wrangler.py`
- Uses `npx wrangler r2 object put --remote` (the `--remote` flag is critical -- without it, wrangler uploads to a local emulator)
- `--only "fact/my_table"` for specific tables
- `--dry-run` for preview

**Download (R2 -> Fly.io):** `python3 scripts/sync_lake.py`
- Uses boto3 for download
- Skips existing files with matching size (incremental)
- Runs in Fly.io entrypoint background

### Critical Per-Dataset Rules

**T-MSIS:**
- Illinois custom dedup logic required (incremental credits/debits, not void/replace)
- MC encounters may show $0 paid amounts
- Check DQ Atlas quality tier before analysis
- Final-release TAF only (12+ month runout)
- Store TAF version metadata

**SDUD:**
- NDC 11-digit left-padding (5-4-2 format)
- Suppression <11 prescriptions
- All amounts pre-rebate (before manufacturer rebates)
- Link via RxNorm for canonical drug ID
- Filter out 'XX' (national total, double-count risk)

**CMS-64 vs T-MSIS:**
- Will never reconcile (payment date vs service date)
- CMS-64 = totals authority, TAF = service detail
- Maintain both, show the gap

**HCRIS:**
- Not audited, not GAAP
- Winsorize outliers
- Multiple reports per provider (collapse, weight by fiscal year fraction)
- Two form versions need crosswalk (CMS-2552-96 and CMS-2552-10)

**NPPES:**
- 9.37M providers, 28 key columns + taxonomy
- Only 8.2% updated within past year
- Taxonomy self-reported, unverified
- Cross-reference with PECOS + state licensing

### Fee Schedule Pipeline

**CPRA Engine:** `tools/mfs_scraper/cpra_engine.py` (~968 lines)
```
python cpra_engine.py --all --cpra-em --output-dir ../../public/data/
```
Steps: --init -> --em-codes -> --medicare-rates (858K) -> --cpra (242K) -> --dq (258K flags) -> --export -> --cpra-em -> --stats

DuckDB (`aradune_cpra.duckdb`): 1.87M rows across 8 tables.

**Individual State Scrapers:** 17 new fee schedule tables built in Session 30, each with custom parsing for the state's publication format (CSV, Excel, PDF, portal scraping).

### Scheduling

| Frequency | Sources |
|-----------|---------|
| Weekly | NPPES, NADAC, Federal Register (CIBs/SHOs), LEIE |
| Monthly | BLS unemployment/CPI/FRED, MC enrollment, RxNorm |
| Quarterly | T-MSIS/SDUD/MBES-CBES, MCO MLR |
| Annual | Medicare PFS RVU, state fee schedules, HCRIS, BLS OEWS, ACS, AHRF, SVI |

---

## 11. CPRA Compliance System

The CPRA (Comparative Payment Rate Analysis) is Aradune's regulatory compliance wedge. 42 CFR Section 447.203 requires states to publish Medicaid-to-Medicare rate comparisons by July 1, 2026.

### Two Separate Systems

#### 11a. Pre-Computed Cross-State Comparison (Frontend)

**File:** CpraGenerator.tsx (734 lines)
**Data:** Pre-computed from fact_rate_comparison (302K rows, 45 states)
**Pipeline:** lake -> cpra_em.json (2,742 rows / 34 states) -> frontend
**Exports:** PDF, Excel, HTML
**API:** GET /api/cpra/states, /api/cpra/rates/{state}, /api/cpra/dq/{state}, /api/cpra/compare

**Summary results:** Median 84.8% of Medicare. PC E&M avg 81.4%, MH/SUD 99.6%, OB/GYN 132.9%.

#### 11b. Upload Tool (42 CFR 447.203 Compliance Generator)

**File:** server/engines/cpra_upload.py (821 lines)
**Input:** Two CSVs (fee schedule + utilization) -> full regulatory CPRA in <2 seconds
**API:** POST /api/cpra/upload/generate, /upload/generate/csv, /upload/generate/report

**Reference data** (data/reference/cpra/):
- em_codes.csv: 68 E&M codes (CMS CY 2025 list)
- code_categories.csv: 171 many-to-many code-category pairs (3 categories: Primary Care, MH/SUD, OB/GYN)
- GPCI2025.csv: 109 Medicare localities

**Processing:**
1. Load reference data (codes, RVUs, GPCIs)
2. Cross-join fee schedule x localities (compute Medicare rates per locality)
3. Merge with utilization by code x category
4. Compute pct_of_medicare = medicaid_rate / (RVU x CF x GPCI)
5. Suppress cells with beneficiary count < 11
6. Aggregate: per-code x category, per-category, per-category x locality

### CPRA Compliance Rules (Always Enforced)

| Rule | Value |
|------|-------|
| Code set | 68 codes from CMS CY 2025 E/M Code List |
| Conversion factor (CPRA) | $32.3465 (CY 2025, non-QPP) |
| Conversion factor (general comparison) | $33.4009 (CY 2026) |
| Category mapping | Many-to-many (171 pairs across 3 categories) |
| Medicare benchmark | Non-facility rate (NOT facility) per 42 CFR 447.203 |
| Supplementals | Base rates only (exclude DSH, UPL, directed payments) |
| Small cell suppression | Beneficiary counts 1-10 suppressed |
| Deadline | Published by July 1, 2026; updated biennially |

### CPRA Data Pipeline (Terminal B)

**File:** tools/mfs_scraper/cpra_engine.py (~968 lines)
**DuckDB:** aradune_cpra.duckdb (1.87M rows, 8 tables)

Command sequence:
```bash
python cpra_engine.py --init          # Create DuckDB, build schemas
python cpra_engine.py --em-codes      # Load 68 E/M codes
python cpra_engine.py --medicare-rates # Fetch Medicare PFS (858K rows)
python cpra_engine.py --cpra          # Compute comparisons (242K rows)
python cpra_engine.py --dq            # Generate DQ flags (258K)
python cpra_engine.py --export        # Export JSON for frontend
python cpra_engine.py --cpra-em       # E/M subset for CpraGenerator
python cpra_engine.py --stats         # Summary statistics
```

---

## 12. Forecasting Engines

### Caseload Forecasting (SARIMAX + ETS)

**Engine:** server/engines/caseload_forecast.py (~650 lines)
**Frontend:** CaseloadForecaster.tsx (~830 lines)
**Route:** `/#/forecast`

**Input:**
- User-uploaded caseload CSV: month, category, enrollment
- Optional events CSV: date, event_type, description, affected_categories, magnitude, direction
- Or public enrollment data from the lake

**Processing:**
1. Validate (minimum 24 months history per category)
2. Fit SARIMAX (various (p,d,q)(P,D,Q,12) orders) and ETS (additive/multiplicative variants)
3. Model competition: select by holdout MAPE (last 6 months)
4. Auto-detect structural breaks: PHE start (2020-03), unwinding start (2023-04), unwinding peak (2023-06)
5. Forecast with confidence intervals (80%, 95%)

**Output per category:** Model used, AIC, MAPE, history months, forecasts (point + CI), intervention effects, warnings.

### Expenditure Modeling

**Engine:** server/engines/expenditure_model.py (~430 lines)

**Input:** Caseload forecast + expenditure parameters CSV with per-category rates

**Processing:**
- Capitation: enrollment x cap_rate x (1 + trend/12) x (1 + admin_load) x (1 + risk_margin)
- FFS: enrollment x cost_per_eligible x (1 + trend/12)
- Policy adjustment applied when effective

### Scenario Builder

4 sliders for sensitivity analysis:
- Unemployment rate change
- Eligibility threshold shift
- Rate change percentage
- Managed care shift

Presets for common scenarios. Baseline vs scenario comparison chart.

### Fiscal Impact (Future Phase 4)

Target: Rate increase % -> federal match at FMAP -> UPL headroom -> SDP cap under OBBBA -> MCO capitation impact -> budget cycle projection. Connects fee schedule, FMAP, CMS-64, and actuarial trends.

---

## 13. Data Import & Export Pipeline

### Data Import

**Endpoint:** POST /api/import (multipart file upload)
**Formats:** CSV, XLSX, JSON (up to 50 MB)

**Flow:**
1. File parsed (Papa Parse for CSV, XLSX library for Excel)
2. Schema profiling via DuckDB SUMMARIZE
3. Fuzzy column mapping to canonical schema
4. Validation: code format regex, date range plausibility, referential integrity
5. Quarantine pattern: invalid records routed to _quarantine temp table with rejection reason codes
6. Valid records loaded as DuckDB temp table: user_upload_{uuid}
7. Available to Intelligence and all tools for session

**Session management:**
- 50 MB/session, 500 MB total, LRU eviction, 2-hour TTL
- UUID session_id generated at upload, returned to frontend
- Frontend stores in AraduneContext, passes with Intelligence and module API calls

**Intelligence integration:** System prompt augmented: "The user has uploaded '{filename}' with {N} rows and columns: {columns}. Available as table '{tableName}'."

### Export Pipeline (6 Formats)

| Format | Library | Size | Trigger |
|--------|---------|------|---------|
| CSV | Built-in | tiny | Any table download |
| Excel (multi-sheet) | xlsx | 429 KB | Report export |
| DOCX (branded) | docx | 356 KB | Report export |
| PDF (branded) | jspdf + autotable | 358 KB | Report export |
| Chart PNG (2x retina) | Canvas | n/a | Chart overlay button |
| Chart SVG | SVG serialization | n/a | Chart overlay button |

All export libraries lazy-loaded (only loaded when user clicks export button).

### Report Builder

**Not a separate module** -- a persistent panel accessible from any tool.

**Workflow:**
1. Use Intelligence or any tool
2. Click "Save to Report" / "Add to Report"
3. Section appears in Report Builder panel (floating button, bottom-right, shows count badge)
4. Reorder, annotate, delete sections
5. Export as DOCX, PDF, Excel, or CSV

**DOCX format:** Branded cover page (Aradune logo, date, topic), numbered sections, shaded prompt boxes, branded table headers, footer with data citations.

**PDF format:** Page breaks, numbered sections, auto-table for data, branded headings.

**Excel format:** Multi-sheet (Overview + per-section data + Queries + Notes).

---

## 14. Infrastructure & Deployment

### Vercel (Frontend)

- React app deployed via CI (GitHub Actions)
- Domain: aradune.co + www.aradune.co
- Vercel Pro plan
- Token rotated 2026-03-17
- Env vars: ANTHROPIC_API_KEY, VITE_MONTHLY_PARQUET_URL

### Fly.io (Backend)

- 1 machine: shared-cpu-1x, 2GB RAM
- Persistent 10GB volume at /app/data/lake
- min_machines_running=1
- Pre-baked lake in Docker image for fast cold starts

**Dockerfile:**
```
FROM python:3.11-slim
WORKDIR /app
COPY . .
RUN pip install -r requirements.txt
EXPOSE 8000
```

**fly.toml:**
```toml
[app]
name = "aradune-api"

[mounts]
source = "aradune-data"
destination = "/app/data/lake"
size_gb = 10
```

**Entrypoint sequence:**
1. Start background R2 sync (`python3 scripts/sync_lake.py` -- incremental download)
2. Start FastAPI: `python -m uvicorn server.main:app --host 0.0.0.0 --port 8000`
3. After sync completes: POST /internal/reload-lake (triggers view re-registration)

**Health checks:**
- GET /health -- lightweight, immediate (machine alive)
- GET /ready -- readiness probe, 200 only when lake fully loaded (650+ views)

### Cloudflare R2

- Bucket: aradune-datalake
- 890+ parquet files, ~5 GB
- Upload: `sync_lake_wrangler.py` (local -> R2 via wrangler CLI)
- Download: `sync_lake.py` (R2 -> Fly.io via boto3)
- Incremental: skips files with matching size

### CI/CD (GitHub Actions)

**Files:**
- `.github/workflows/ci.yml` -- build, lint, deploy
- `.github/workflows/adversarial.yml` -- weekly adversarial testing + Skillbook import

**CI Pipeline (ci.yml):**
1. TypeScript type check
2. Vercel frontend deploy
3. Fly.io backend deploy

Both Vercel and Fly.io deploying successfully as of 2026-03-18. All 6 GitHub CI secrets confirmed set (VERCEL_TOKEN, FLY_API_TOKEN, CLERK_SECRET_KEY, VITE_CLERK_PUBLISHABLE_KEY, VERCEL_ORG_ID, VERCEL_PROJECT_ID).

**Adversarial Pipeline (adversarial.yml -- Session 34):**
1. Runs weekly, Sunday 2AM UTC
2. Executes full 7-agent adversarial suite against deployed Fly.io instance
3. Auto-imports lessons from failures to Skillbook via `skillbook_import.py`
4. Creates GitHub issue on critical failure (any agent with >20% failure rate)
5. Uploads JSON report as workflow artifact for audit trail
6. Cost: $7-13 per full run

### Self-Healing View Registration

db.py implements a self-healing pattern for cold starts:
1. Primary registration scans all Parquet files immediately
2. Background thread rescans every 30s until 650+ views found
3. reload_lake() endpoint clears _registered set and re-scans
4. Handles R2 sync lag (files still downloading when server starts)

### Build & Deploy Commands

```bash
# Local development
npm install && npm run dev                    # Frontend on localhost:5173

# CPRA engine
cd tools/mfs_scraper/
python cpra_engine.py --all --cpra-em --output-dir ../../public/data/

# Frontend deploy
npm run build && npx vercel --prod

# Ontology (after any data lake changes)
python scripts/validate_ontology.py           # CI check, must pass
python scripts/generate_ontology.py           # Regenerate system prompt + DuckPGQ

# R2 sync (upload to Cloudflare R2)
python3 scripts/sync_lake_wrangler.py                           # All tables
python3 scripts/sync_lake_wrangler.py --only "fact/my_table"    # Specific table
python3 scripts/sync_lake_wrangler.py --dry-run                 # Preview

# Fly.io deploy (run from project root, NOT server/)
fly deploy --remote-only --config server/fly.toml --dockerfile server/Dockerfile

# Fly.io reload (after R2 upload)
fly ssh console --app aradune-api --command "python3 -c \"import urllib.request; urllib.request.urlopen(urllib.request.Request('http://localhost:8000/internal/reload-lake', method='POST'))\""
```

---

## 15. Data Quality & Validation

### Operational Validation Layer

Aradune runs a centralized validation engine (`server/engines/validator.py`) with 3 check types across major data domains. Results are surfaced via API.

| Check Type | What It Validates | Count |
|-----------|-------------------|-------|
| Row Count | Table has minimum expected rows (fact_rate_comparison >= 300K, etc.) | 10 |
| Range | Numeric values within plausible bounds (FMAP 0.5-0.83, pct_of_medicare 1-1000, MC penetration 0-100) | 3 |
| Referential Integrity | Foreign keys resolve to dim_state (rate_comparison, enrollment) | 2 |

**API endpoints:**
- GET /api/validation/latest -- run checks and return summary with pass rate
- GET /api/validation/results -- detailed check results (filterable by domain, failures only)
- GET /api/validation/domains -- pass rates grouped by data domain

**Runner:**
```
python3 scripts/run_validation.py                    # Full suite
python3 scripts/run_validation.py --domain fact_rate # Single domain prefix
python3 scripts/run_validation.py --export report.md # Markdown report
python3 scripts/run_validation.py --failures-only    # Show only failures
```

### ETL Inline Validation (Layer 0)

All 115+ ETL scripts include inline validation with hard stops and soft flags:

**Hard stops (fail immediately):** Rate changed >90% from prior snapshot. Code count dropped >20%. Schema mismatch. NULL or invalid state_code.

**Soft flags (warn, continue):** Rate unchanged >24 months. New codes without description. Rate >3 standard deviations from national mean. Cell size n < 11.

### Future Validation Stack (Phase 2)

| Tool | Purpose | Status |
|------|---------|--------|
| Soda Core v4 | SodaCL check language, ML anomaly detection | Phase 2 |
| dbt-duckdb + dbt-expectations | SQL-first validation macros | Phase 2 |
| Pandera | DataFrame validation with statistical hypothesis testing | Phase 2 |
| datacontract-cli | CI/CD contract testing, breaking change detection | Phase 2 |

The operational validation layer covers the critical path. The formal framework tools are planned for implementation with engineering resources and will extend, not replace, the existing checks.

### Per-Source Quality Gates

**Rates:**
- Flag $0.00 rates, >$10K E&M rates, unchanged >24 months, >2x RVU-derived expected
- Verify expected E/M codes present, track CPT additions/deletions
- Validate locality for Medicare matching, confirm conversion factor

**Cross-state:**
- Flag >3 standard deviations from national mean
- Check DQ Atlas tier for every state before analysis

### Known Data Quality Issues

| Issue | Impact | Mitigation |
|-------|--------|------------|
| T-MSIS Illinois dedup | Wrong claim counts for IL | Custom incremental credit/debit logic |
| DOGE OT-only | Missing 3 of 4 claim types | 3-layer quarantine controls |
| HCRIS unaudited | Noisy hospital financials | Winsorize outliers |
| NPPES self-reported | 57% not updated in 5+ years | Cross-reference PECOS |
| SDUD NDC format | Different padding by source | Normalize to 5-4-2 via RxNorm |
| Census sentinels | -888888888 values | Treat as NULL |
| CMS-64 vs T-MSIS | Never reconcile | Maintain both, show gap |

---

## 16. Security & Compliance

### Current Controls

- AES-256 at rest, TLS 1.2+ in transit, no plaintext secrets
- RBAC, MFA admin, tenant isolation, expiring sessions
- Immutable logs, secrets via env vars only
- User data session-scoped, never persisted, never shared
- SELECT-only SQL enforcement (15 forbidden keywords)
- Minimum cell size n >= 11

### Certification Roadmap

| Cert | Cost | Timeline | Priority |
|------|------|----------|----------|
| SOC 2 Type II | $30-80K | 6-12 months | Minimum for enterprise |
| GovRAMP Ready | $50-125K | 6-12 months | 27 states recognize |
| HITRUST i1 | $70K | 5-8 months | MCO sales differentiator |
| FedRAMP | $250K+ | 12-24 months | Federal only, deprioritized |

### AI Governance (Always Enforced)

- Source attribution, confidence scores, data quality warnings on every output
- No beneficiary-affecting output without human confirmation
- Bias monitoring across race, ethnicity, geography, disability
- NIST AI RMF alignment, CMS AI Playbook v4, full audit trail

### Hard Rules

- No raw PHI without DUA + HITRUST
- No fraud detection before BAA
- No cross-provider visibility without opt-in
- No T-MSIS outside ResDAC terms
- No counts below n=11
- No logging user query content that could reveal PHI

### Regulatory Deadlines Aradune Serves

| Deadline | Requirement | Aradune Capability |
|----------|-------------|-------------------|
| July 1, 2026 | CPRA rate transparency (42 CFR 447.203) | CPRA tool -- ready now |
| July 1, 2026 | Publish all FFS rates publicly | Rate Lookup + Directory |
| July 1, 2026 | HCBS hourly rate disclosure | Workforce & HCBS |
| January 1, 2027 | OBBBA work requirements | Forecasting + fiscal impact |
| ~July 2027 | Appointment wait times (90% compliance) | Network adequacy (future) |
| ~July 2028 | 80% HCBS compensation pass-through | HCBS tracking |
| FY 2030 | 3% eligibility error rate penalty | Program integrity (future) |

---

## 17. Build History & Current State

### Session Timeline (34 Sessions, Feb-Mar 2026)

**Sessions 1-8 (Feb 2026): Foundation**
Initial build. CPRA engine. T-MSIS pipeline. State fee schedule ingestion (47 states). Forecasting engines. AHEAD readiness. Provider/workforce tools.

**Sessions 9-10: Architecture Rebuild**
Intelligence as home page with SSE streaming. AraduneContext shared state. IntelligencePanel sidebar. Nav restructured.

**Session 11: RAG + Rate Engine**
StateRateEngine wired to Rate Builder (1,153 lines, 42/42 tests). RAG engine: BM25 + FTS over 1,039 policy docs, 6,058 chunks. State Profile cross-dataset insights (11 generators).

**Session 12: Intelligence Redesign**
Unified Intelligence interface. Smart routing (4 tiers). Response cache (LRU 200, 6hr TTL, 27 pre-seeded demos). Docker pre-bake (785MB lake, 10s cold start).

**Session 13: Entity Registry / Ontology**
16 entities, 15 domains, 274 tables. CI wired. 4-layer test suite (86 tests). Upload quarantine pattern. Query router. Export utility.

**Session 17: Overnight Data Marathon (Largest Session)**
294 new fact tables (from ~250 to 544). ~137M new rows. Lake grew from 785 MB to 3.0 GB. 77 ETL scripts. 19 new build scripts. New domains: KFF (28 tables), State Fiscal (11), Insurance Market (3), Program Integrity (5), Provider Network (6+ including NPPES 9.37M), Maternal & Child Health (10), Medicaid.gov (17), DOGE (190M raw, 5 aggregated). Major additions: NPPES (11 GB CSV), Medicare chronic conditions/MCBS/Part D, economic data, quality (HCAHPS, Care Compare), behavioral health (TEDS, NSDUH 2024), nursing (MDS 29.2M).

**Session 27: Comprehensive Audit + Mobile**
12 bug fixes across 10 files. FMAP rebuilt (204 rows, 51 states, 38 corrected). Mobile: shared useIsMobile(), responsive grids, all tables scrollable.

**Session 28: 6 New Modules + Data Accuracy**
Behavioral Health, Pharmacy, Nursing Facility, Spending Efficiency, Hospital Rate Setting, Program Integrity modules built. CMS-64 corrected to FY2018-2024 multiyear (118K rows). R2 sync --remote flag fixed.

**Session 29: Full Forensic Audit + Research**
8-prompt forensic audit. 30+ bugs fixed (17 critical ETL, 18 stale snapshots). SDUD schema standardized. AHEAD calculator reworked (3-year 10/30/60 CMS baseline). DOGE quarantine (3-layer). 10 research modules built. Ontology: 28 edges, 680 tables. Open Payments: $2.2B -> $10.83B. R2 fully synced (826 files, ~5GB). 16 commits deployed.

**Session 30: Research Audit V2 + Fee Schedule Completion + Data Ingestion**
Full 8-prompt research audit (V1 + V2): 25 bugs fixed, all 46 endpoints pass. Rate-Quality p-value corrected: 0.044 (was 0.178, SVI multicollinearity fixed). All 51 jurisdictions fee schedules complete (17 new state tables scraped). rate_comparison_v2: 483K rows, 54 states. 2 new research modules (T-MSIS Calibration, MEPS Expenditure). Data ingestion: ADI (240K), AHRQ SDOH (44K), FMAP FY2011-2023, MCPAR (300 PDFs), MEPS HC-243, expansion dates, dental/MH HPSAs, MUA/MUP, food access atlas, FDA Orange Book, NHE 30-year series. Ontology: 722 tables across 20 domains. Nav consolidated 10 groups -> 5. About page rewritten. R2 synced (49 new parquets). ~45 commits.

**Session 32 (2026-03-17): Post-Review Fixes + Adversarial Testing Framework**
@safe_route on all 336/336 endpoints (was 176). safe_route updated to re-raise HTTPException. Created validation API (server/routes/validation.py: 3 endpoints) + CLI runner (scripts/run_validation.py). Build doc (ARADUNE_FULL_BUILD.md) fully reconciled. Adversarial testing framework built: 4 agents (Intelligence, API Fuzzer, Consistency, Persona) in scripts/adversarial/. API fuzzer: 100% pass. Consistency: 85.7%. Intelligence system prompt overhauled: dash elimination, data vintage enforcement, per-state mandatory caveats, strengthened DOGE quarantine. 3 more agents designed (Florida Rate, Skillbook, Browser). Implementation guide: docs/ADVERSARIAL_TESTING_IMPL.md.

**Session 34 (2026-03-18): Intelligence Hardening + Adversarial Completion + Skillbook v2 + FL Rule Correction**

This was the most significant Intelligence and quality assurance session. Four major workstreams:

*Intelligence Hardening:*
- Programmatic DOGE quarantine: `_detect_doge_tables()` checks SQL for any of the 5 DOGE table names. When detected, `MANDATORY_CAVEATS` array is injected into the tool result JSON, ensuring Claude sees the caveats as part of the data response, not just the system prompt. All 5 caveats: OT-only, provider state, MC distortion, Nov/Dec incomplete, dataset offline.
- Programmatic IL T-MSIS caveat: `_detect_il_claims()` checks for IL state code + claims/tmsis table references in SQL. Injects the incremental credits/debits dedup warning.
- Territory-aware fallback: When Intelligence generates an empty response and the query mentions a territory (GU/PR/VI/AS/MP), returns a helpful message about territory data limitations instead of a generic 74-char fallback.
- Response post-processing: `_postprocess_response()` strips em-dashes (U+2014), en-dashes (U+2013), and double-hyphen dashes from all responses. Also cleans up double commas from replacements.
- DuckDB statement_timeout: 30 seconds per query via `SET statement_timeout=30000` before each `_run_query()` execution.
- Anthropic API timeout: 120 seconds total, 10 seconds connect, via `httpx.Timeout(120.0, connect=10.0)`.
- Trace storage: `fact_intelligence_trace` table stores every query with trace_id, query_text, domain, tier, skill_ids_retrieved, sql_queries, model_used, response_length, response_time_ms, feedback. Trace_id included in SSE metadata events so frontend can reference specific queries for feedback.

*Adversarial Testing Completion (7/7 agents):*
- Florida Rate Agent: 4 SQL data-layer tests + 7 Intelligence endpoint tests validating FL rate structure. Confirms 99.96% codes have either facility OR PC/TC, and 3 codes (46924, 91124, 91125) legitimately have both. Cost: ~$0.50.
- Skillbook Agent: 5 poisoning resistance tests (false facts should not contaminate skills), 2 compounding tests (skills should improve responses), 4 integrity checks (schema, score range, contamination scan, domain distribution).
- Browser Agent: 8 Playwright end-to-end tests (homepage load, JS error-free navigation across 7 routes, mobile viewport 390x844, rapid state switching, Intelligence SSE rendering, Cmd+K search, export during load). Cost: $0 (no LLM).
- known_facts.json: 28 ground-truth anchor facts across 11 domains.
- Runner: `python -m scripts.adversarial.runner` with --agent, --quick, --export, --json flags.
- Cost: Full suite $7-13, Quick $5-9, Florida Rate ~$0.50, Browser $0.

*Adversarial-to-Skillbook Pipeline:*
- `skillbook_import.py`: Reads adversarial JSON reports, extracts lessons from failures, converts to Skillbook entries.
- Failure-to-skill conversion: domain inference from query keywords (17 domain keyword sets), category mapping (hallucination -> factual_accuracy, quarantine_bypass -> caveat_enforcement, etc.).
- Anchor import: `--anchors` flag imports all 28 known_facts.json entries as baseline skills.
- Deduplication: Checks existing skills before adding (substring match on first 80 chars).
- GitHub Actions workflow (`.github/workflows/adversarial.yml`): Weekly Sunday 2AM UTC run, tests against deployed Fly.io, auto-imports lessons to Skillbook, creates GitHub issue on critical failure.

*Skillbook v2 Upgrades:*
- Score decay: `effective_score = net_score * pow(2, -(days_elapsed / half_life_days))`. Default half-life 30 days. Negative scores don't get decay benefit. All retrieval now sorted by effective_score.
- Schema additions: `last_validated_at` (reset on any feedback), `decay_half_life_days` (default 30), `related_skills` (VARCHAR array for graph edges), `prune_reason` (why retired).
- Graph expansion: After initial retrieval (domain rules + keyword match), reads `related_skills` arrays and does 1-hop fetch for linked skills. Linked skills flagged with `via_link=True`.
- `link_skills(a, b)`: Bidirectional linking via JSON arrays.
- `update_score` now resets `last_validated_at` (refreshes decay clock).
- `retire_skill` accepts `reason` parameter stored in `prune_reason`.
- Reflector v2: Now proposes `proposed_links` between related skills and flags `split_candidates` (oversized skills >500 chars).
- CRUSP Prune: `scripts/prune_skillbook.py` implements automated cleanup. Rules: harmful (net_score < -2 for 14+ days), decayed+unused (effective_score < 0.5, times_retrieved == 0, 60+ days), oversized (>500 chars, logged for split). Dry-run by default, `--apply` to execute.
- New API endpoints: `/api/skillbook/recent` (most recent skills), `/api/skillbook/add` (POST with JSON body).
- Feedback endpoint enhanced: Now accepts trace_id, looks up the trace from fact_intelligence_trace, passes recovered context to reflector for targeted re-reflection.

*FL "Mutual Exclusion Rule" Correction:*
- The rule was fabricated by a prior Claude session in the very first CLAUDE.md commit. It claimed FL Medicaid rates were "mutually exclusive" -- a code could only have facility OR PC/TC, never both.
- AHCA-published Practitioner Fee Schedule shows 3 codes (46924, 91124, 91125) legitimately carry both facility AND PC/TC rates.
- Corrected across 13+ files: CLAUDE.md, intelligence.py system prompt, ARADUNE_BUILD_GUIDE.md, ONTOLOGY_SPEC.md, fl_methodology_addendum.md, adversarial agents, known_facts.json, etc.
- Adversarial agents updated to test FOR the correct behavior (3 codes should have both).

*Data Quality Fix:*
- fact_rate_comparison_v2 rebuilt from actual published fee schedules. APC/facility rates were contaminating physician-level comparisons, inflating pct_of_medicare for RI (275%→107%) and CT (435%→110%).
- Corrected dim_state conversion factors for 47 states against CMS published CFs.
- Repeatable build script: `scripts/build_lake_rate_comparison_v2.py`. Facility rates filtered so they are never compared to non-facility Medicare rates.
- known_facts.json expanded to 32 facts (added 4 state-level rate quality checks).
- New build principle #27: validate data against external benchmarks (KFF, MACPAC, CMS).

*Infrastructure:*
- GitHub CI secrets confirmed set: VERCEL_TOKEN, FLY_API_TOKEN, CLERK_SECRET_KEY, VITE_CLERK_PUBLISHABLE_KEY, VERCEL_ORG_ID, VERCEL_PROJECT_ID.
- Known issues audit: 8 issues resolved, 2 new (cache seeds stale, ANTHROPIC_API_KEY not in GitHub).
- Deploy guide: docs/SESSION-34-DEPLOY-GUIDE.md with step-by-step for remaining manual steps.

### Cross-Dataset Enrichment Architecture (Session 34 continued)

Two-tier context system deployed across all 12 core modules:

**Tier 1: Universal State Context**
- Backend: `GET /api/state-context/{state_code}` (server/routes/state_context.py)
- 12 independent try/except queries: fiscal (FMAP + CMS-64), enrollment, access (HPSA), quality (Core Set), demographics, rate adequacy (fact_rate_comparison_v2), workforce (BLS CNA/HHA/RN), HCBS waitlist, LTSS rebalancing, T-MSIS effective rates, supplemental (DSH + SDP)
- 1-hour in-process cache per state. ~20-30ms uncached.

**Tier 2: Module-Specific Enrichment**
- Rate Browse: inline StateContext with T-MSIS gap analysis
- Hospital Rate Setting: ownership, GME, MSSP ACO, VBP (planned)
- Behavioral Health: TEDS detail, NSDUH age-stratified, overdose (planned)

**Shared Frontend Infrastructure:**
- `src/components/StateContextBar.tsx`: compact (single-row metrics) + expanded (grid with sections)
- `src/hooks/useStateContext.ts`: shared fetch hook, 10-min client cache
- `src/utils/formatContext.ts`: fmtB, fmtPct, fmtDollar, fmtNum, SYM constants
- `src/types.ts`: StateContextData interface (11 domain sections)

**Module Deployment:**
| Module | Mode | State Source |
|--------|------|-------------|
| StateProfile | compact | URL hash |
| RateBrowse | expanded (inline) | row click |
| SpendingEfficiency | expanded | dropdown |
| BehavioralHealth | compact | NEW dropdown + chart click |
| NursingFacility | compact | existing dropdown |
| HospitalRateSetting | expanded | NEW dropdown + table/chart click |
| PharmacyIntelligence | compact | existing filter |
| ProgramIntegrity | compact | NEW dropdown + table click |
| WageAdequacy | expanded | existing dropdown |
| QualityLinkage | compact | existing dropdown |
| HcbsTracker | compact | existing dropdown |
| CaseloadForecaster | compact | existing dropdown |

**Extensibility:** Any future module gets cross-dataset context with:
  import StateContextBar from "../components/StateContextBar";
  <StateContextBar stateCode={selectedState} />

### Rates & Compliance Redesign (Session 34 continued)

- New Rate Browse & Compare tool (RateBrowse.tsx, 1,230 lines) with Dashboard, Code Lookup, State Compare views.
- Replaced 5 overlapping tools.
- Backend: /api/rates/state-summary + /api/rates/compare-states + /api/rates/context/{state}.

### Deep Data Lake Audit (Session 34 continued)

Comprehensive audit of 722 fact tables found 6 critical and 8 significant data quality issues:

**Critical (fixed):**
1. CMS-64 CHIP subtotal double-counting: Subtotal rows ('C-Total Net', 'C-Balance', 'T-%') inflated state spending 3-7%. CA over-reported by $10.5B. Fixed column name (category→service_category) and added exclusion filter to all 10 CMS-64 query locations.
2. rate_comparison v1 facility rate contamination: 10,772 rows with pct_of_medicare > 500%. Capped at <500% in all API queries.
3. dim_state CF errors: OK corrected from $103.59 to $27.35. SD set to NULL (pct-of-medicare methodology).
4. MACPAC footnote contamination: Up to 19.4% of rows are footnotes. Guards added to NL2SQL and Intelligence prompt.

**Significant (known, monitored):**
5. Published FS rates diverge from actual claims-based rates (AL: FS=151% vs claims=59% of Medicare).
6. 5 duplicate opioid tables with identical 539K rows.
7. TEDS coded values (-9 = missing, not negative).
8. SD rate data uses raw multipliers instead of dollars.

### System Dynamics Engine (Session 34 continued)

Stock-flow ODE modeling for Medicaid policy analysis. No other Medicaid analytics platform offers causal feedback loop modeling.

**Backend Engine** (`server/engines/system_dynamics.py`, 512 lines):
- Core: Stock, Flow, Parameter, Intervention, SDModel, SDResult dataclasses
- Solver: scipy.integrate.solve_ivp (RK45, fallback Radau for stiff systems)
- 4 individual models calibrated from lake data:
  1. Enrollment: eligible_pool → processing → enrolled → disenrolled (unemployment elasticity, policy shocks)
  2. Provider Participation: rate_attractiveness → provider entry/exit → access score (logistic curve, 12-month lag)
  3. Workforce Pipeline: wage_ratio → recruitment → retention → staffing HPRD (turnover dynamics)
  4. HCBS Rebalancing: funding_ratio → transition_rate → institutional/community/waitlist stocks
- 1 integrated model: 12 stocks in single ODE system, 6 cross-domain feedback loops
- Calibration: state data from lake → national average → literature default. Source logged in results.

**API** (`server/routes/dynamics.py`, 290 lines):
- POST /api/dynamics/enrollment, /provider, /workforce, /hcbs, /policy-simulator
- Policy simulator runs baseline + scenario, returns impact deltas and active feedback loops
- In-process cache (1hr TTL, 100 entries)

**Policy Simulator** (`src/tools/PolicySimulator.tsx`, ~500 lines):
- Intervention builder: rate changes, wage increases, HCBS funding, unemployment shocks
- 5 presets: Rate Parity, Recession, HCBS Expansion, Austerity, Workforce Investment
- 4 impact cards, 5 chart tabs, feedback loops panel
- StateContextBar + "Ask Aradune" + CSV export
- Registered at /#/policy-simulator under Finance nav

**Embedded Widgets** (4 modules):
- CaseloadForecaster: Enrollment Dynamics (unemployment slider)
- WageAdequacy: Workforce Pipeline (wage slider)
- HcbsTracker: Rebalancing Trajectory (funding slider)
- RateBrowse: Provider Participation (rate change slider)
- All collapsible, 400ms debounce, Recharts charts, calibration sources

### Current State (March 18, 2026)

| Metric | Value |
|--------|-------|
| Lake views | 750+ |
| Total rows | 400M+ |
| Parquet size | 4.9 GB |
| Ontology domains | 20 (722 tables mapped) |
| Entity types | 16 |
| Relationship edges | 28 |
| Named metrics | 19 (deterministic, defined in ontology/metrics/) |
| ETL scripts | 115+ |
| Backend endpoints | ~345 across 40+ route files (27 top-level + 13 research) |
| Engines | 11 (Intelligence, Query Router, RAG, Caseload, Expenditure, CPRA Upload, CPRA Engine, Skillbook v2, Reflector v2, Validator, System Dynamics) |
| Frontend modules | 28 standalone (15 core + 13 research) |
| Export formats | 6 (CSV, Excel, DOCX, PDF, PNG, SVG) |
| R2 parquet files | 890+ |
| Demo responses | 27 pre-cached |
| CI/CD | Vercel + Fly.io deploying + weekly adversarial testing workflow |
| Auth | Clerk (JWT, test keys active -- switch to production before demo) |
| Fee schedule coverage | All 54 jurisdictions (50 states + DC + PR/GU/VI; 51 published, 3 T-MSIS) |
| Rate comparison rows | 483,154 across 54 jurisdictions |
| Skillbook | v2: CRUSP lifecycle, score decay (30-day half-life), graph expansion (1-hop), 24+ seed skills, auto-learning from every query |
| Adversarial agents | 7/7 built (Intelligence, API Fuzzer, Consistency, Persona, Florida Rate, Skillbook, Browser) |
| Known facts | 28 ground-truth anchor facts across 11 domains (known_facts.json) |
| Intelligence trace | fact_intelligence_trace audit trail with trace_id in SSE metadata |
| Research modules | 13 (Rate-Quality, MC Value, Treatment Gap, Safety Net, Integrity, Fiscal Cliff, Maternal Health, Pharmacy Spread, Nursing Ownership, Waiver Impact, T-MSIS Calibration, MEPS Expenditure, Network Adequacy) |

---

## 18. What Remains

### Open Items

| # | Item | Status |
|---|------|--------|
| 1 | Clerk auth -- switch to production keys | Test keys active. Create Clerk production instance, deploy pk_live/sk_live to Vercel + Fly.io. |
| 2 | R2 credentials rotation | James needs new Cloudflare token |
| 3 | AHRQ SDOH + CDC SVI blocked by WAF | Cannot automate download (manual refresh only) |
| 4 | Formal validation stack (Soda Core, dbt, Pandera) | Phase 2. Operational validation layer deployed with 15 checks across all domains. |
| 5 | ETL re-runs for audit fixes | FMAP dynamic headers, eligibility pagination, SDUD schema |
| 6 | AHEAD hardcoded to 6 states / 12 hospitals | Deferred |
| 7 | Duplicate raw files in data/raw/ | Cleanup candidate |

### Phase 6 (Post-Demo / Future)

- User accounts (Clerk) + Stripe billing
- Early warning heat map (50-state, 6 categories)
- Forecast accuracy dashboard (log predictions, compare to actuals)
- Network adequacy engine (directory vs claims vs availability, ghost network detection)
- Compliance countdown dashboard (per state per deadline)
- Hospital price transparency MRF ingestion
- Shared analytical workspaces (role-based, audit trail)
- SOC 2 Type II certification
- State procurement pathway (APD, SMC)

### Roadmap

| Milestone | Target | Key Deliverables |
|-----------|--------|-----------------|
| Demo ready | ~April 2026 | End-to-end demo flow, visual polish, walkthrough script |
| First external CPRA user | 3-6 months | Revenue conversation |
| Early warning + accuracy | 6-12 months | Revenue covers infrastructure |
| Default Medicaid reference | 1-3 years | CMS links to it, seven figures |

### Monetization

**Track A (Active): Partnership / Acquisition**
- Target: Gainwell/Veritas, Nordic Capital, Merative
- Valuations: 5-9x revenue

**Track B (Future): Independent SaaS**

| Tier | Price | Audience |
|------|-------|---------|
| Free | $0 | Journalists, advocates, students |
| Analyst | $99/mo | Individual analysts |
| Pro | $299/mo | Consulting teams |
| State Agency | $50-200K/yr | State agencies (75% FFP eligible) |
| Enterprise | $50-500K/yr | Firms, MCOs, hospitals |

**Federal Funding Pathway:** States can procure Aradune at 10-25 cents on the dollar through CMS's DSSDW module designation. 90% FFP for design/development, 75% for operations.

---

## Build Principles (25 Rules)

1. Build to the unified schema
2. Validation is not optional
3. Source provenance is not optional (every record has URL + download date)
4. Ship ugly -- 50 states working beats 5 states beautiful
5. Coverage over polish
6. Federal data first (covers all states)
7. Florida pipeline is the template
8. PDF parsing prompts are versioned
9. FL Medicaid: Facility and PC/TC rates are typically mutually exclusive (99.96% of codes). Three codes (46924, 91124, 91125) legitimately carry both as published by AHCA.
10. Data layer is the moat -- every session adds data, improves quality, or makes adding easier
11. Don't be CPRA-forward -- build for the platform
12. Economic/contextual data matters
13. Forecasting models are never deleted
14. User data never mixed with public layer
15. Log predictions, compare to actuals, publish accuracy
16. No em-dashes, no "plain English"
17. Upload data in context, not standalone
18. Intelligence is the connective tissue -- every tool connects to Intelligence
19. Compliance artifacts are first-class outputs (submission-ready, not dashboards)
20. Closed-loop: analysis -> recommendation -> action template -> execution
21. Ontology-first data additions (YAML before ingestion, validate + generate after)
22. Named metrics are deterministic (same question -> same number)
23. Never trust a single source (triangulate: TAF + CMS-64 + supplemental for expenditures)
24. State-level variation is the dominant quality dimension (DQ Atlas first)
25. Test adversarially (Hypothesis + chaos + SDV + 10x volume)
26. Intelligence learns from every query. The Skillbook accumulates domain knowledge. Skills have provenance, scores, and audit trails.
27. Reflection is async and non-blocking. Users never wait for the learning step.
28. Skills are not prompts. A skill is a validated domain insight with a score, not a prompt engineering hack.

---

*The data is the moat. Intelligence is the interface. The Skillbook is the compounding advantage. Adversarial testing keeps it honest. Build in that order.*
