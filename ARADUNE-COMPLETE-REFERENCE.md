# ARADUNE-COMPLETE-REFERENCE.md
> **The single document another Claude session needs to run audits, understand the data, and test the system.**
> Updated: 2026-03-17 (Session 30). Source of truth for architecture, data lake, endpoints, research modules, and audit tests.
> Live: https://www.aradune.co | API: https://aradune-api.fly.dev

---

## 1. Architecture Overview

### What Aradune Is

Aradune is a Medicaid intelligence operating system. It ingests, normalizes, and cross-references every available public Medicaid dataset into a unified data layer, then uses Claude-powered analytics to turn that data into intelligence: compliance-ready documents, fiscal impact models, early warning signals, and actionable recommendations.

**Core identity:** 750+ registered views, 400M+ rows, 5.1 GB Parquet. 20 ontology domains (722 tables mapped), 16 entities. 325+ API endpoints. Official fee schedules for all 54 jurisdictions. 12 cross-domain research modules. 90+ federal data sources.

### Tech Stack

| Layer | Technology |
|-------|-----------|
| **Frontend** | React 18 + TypeScript + Vite (Vercel Pro, aradune.co) |
| **Visualization** | Recharts |
| **Routing** | Hash-based in Platform.tsx (`/#/state/FL`, `/#/rates`, etc.) |
| **Backend** | Python FastAPI (server/) -- 258+ endpoints across 25 route files, DuckDB-backed |
| **Data Lake** | Hive-partitioned Parquet (data/lake/) -- DuckDB in-memory views over Parquet files |
| **AI** | Claude Sonnet 4.6 + extended thinking + DuckDB tools + RAG policy corpus + web search |
| **RAG** | DuckDB FTS over policy corpus (1,039 docs, 6,058 chunks from medicaid.gov) |
| **Auth** | Clerk integration; falls back to password gate ("mediquiad") when Clerk not configured |
| **CI/CD** | GitHub Actions -- TypeScript check + Vercel + Fly.io deploy |
| **Deployment** | Vercel (frontend) + Fly.io (FastAPI backend, shared-cpu-1x, 2GB RAM, 10GB persistent volume) |
| **R2** | Cloudflare R2 (aradune-datalake bucket) -- 816 parquet files |

### Data Flow

```
Data Sources (CMS, BLS, Census, KFF, SAMHSA, CDC, etc.)
    |
    v
ETL Scripts (scripts/build_lake_*.py, 115+ scripts)
    |
    v
Parquet Files on Disk (data/lake/fact/, dimension/, reference/)
    |
    v
DuckDB In-Memory Views (server/db.py, background thread registration)
    |
    v
FastAPI Endpoints (server/routes/*.py, 258+ endpoints)
    |
    v
React Frontend (src/Platform.tsx, 25 modules) <-> Claude Intelligence (SSE streaming)
```

### Deployment Architecture

- **Vercel**: Frontend (aradune.co). Deployed via `npx vercel --prod` or GitHub Actions.
- **Fly.io**: Backend (aradune-api.fly.dev). 1 machine, persistent 10GB volume at /app/data/lake, min_machines_running=1. Pre-baked lake in Docker image for fast cold starts. Self-healing view registration: db.py auto-rescans every 30s until 650+ views found.
- **R2**: Cloudflare R2 bucket (aradune-datalake). `scripts/sync_lake_wrangler.py` for upload; `scripts/sync_lake.py` for download (used by Fly.io entrypoint).

---

## 2. Data Lake Structure

### Medallion Architecture

```
Bronze (raw, append-only)       Silver (normalized/cleaned)       Gold (analytics-ready)
+-- Parquet as received         +-- ICD: GEMs + CCSR groupings    +-- Pre-computed PMPM
+-- state/year/month partition  +-- NDC: 5-4-2 -> RxNorm mapped   +-- Utilization metrics
+-- _source_file metadata       +-- NPI: taxonomy enriched         +-- Quality indicators
+-- Never modify (audit trail)  +-- IL-specific claim dedup        +-- Pre-joined dimensions
+-- ZSTD, 500K-1M row groups   +-- Temporal: FFY/SFY/CY cols      +-- Medical Care CPI adjusted
```

### Parquet Organization on Disk

```
data/lake/
+-- dimension/     (9 .parquet files, 884 KB)
|   +-- dim_state.parquet
|   +-- dim_procedure.parquet
|   +-- dim_medicare_locality.parquet
|   +-- dim_bls_occupation.parquet
|   +-- ...
+-- fact/          (669 directories, 4.6 GB)
|   +-- enrollment/data.parquet
|   +-- medicaid_rate/data.parquet
|   +-- rate_comparison/data.parquet
|   +-- hospital_cost/data.parquet
|   +-- ...
+-- reference/     (22 files, 11 MB)
|   +-- ref_1115_waivers/data.parquet
|   +-- ...
+-- metadata/
```

### How Views Are Registered (db.py)

1. `init_db()` creates an in-memory DuckDB connection, then spawns a background thread to register views.
2. `_register_all_views()` iterates:
   - **Dimensions**: `dim_dir/*.parquet` -> view name = file stem (e.g., `dim_state`)
   - **Facts**: For each name in `FACT_NAMES` (667 entries), finds `_latest_snapshot()` -> `data.parquet` or `snapshot=*/data.parquet` -> view name = `fact_{name}`
   - **Auto-discover**: Any fact dir not in `FACT_NAMES` is also registered
   - **Reference**: `ref_dir/*.parquet` -> stem, and `ref_dir/{subdir}/data.parquet` -> `ref_{name}`
   - **Compatibility views**: `spending` and `spending_providers` (aliased from fact_claims and fact_provider)
3. `_delayed_rescan()` polls every 30s for up to 15 minutes until 650+ views found (handles cold starts when R2 data is still downloading).
4. Thread-safe via `threading.Lock()`. Views are idempotent (CREATE VIEW IF NOT EXISTS).

### Current Lake Statistics

| Metric | Value |
|--------|-------|
| **Total registered views** | 695 |
| **Fact table directories** | 669 |
| **Dimension tables** | 9 |
| **Reference tables** | 22 |
| **Total disk size** | 4.7 GB |
| **Estimated total rows** | 400M+ |
| **FACT_NAMES entries in db.py** | 667 |

---

## 3. Complete Table Catalog

### Dimension Tables

| Table | Rows | Key Columns | Description |
|-------|------|-------------|-------------|
| `dim_state` | 51 | `state_code` (PK), `state_name`, `region`, `fmap`, `efmap`, `total_enrollment`, `fee_index`, `conversion_factor` | US states/territories with Medicaid program metadata |
| `dim_procedure` | 17,081 | `procedure_code` (PK), `description`, `work_rvu`, `total_rvu_nonfac`, `medicare_rate_nonfac`, `em_category`, `category` | HCPCS/CPT codes with CY2026 Medicare PFS RVUs |
| `dim_medicare_locality` | 109 | `locality_code` (PK) | Medicare GPCI locality definitions |
| `dim_bls_occupation` | 16 | `soc_code` (PK) | Medicaid-relevant BLS occupation codes |

### Core Fact Tables (by Domain)

#### Rates & Fee Schedules

| Table | Rows | Key Columns | Source | Join Keys |
|-------|------|-------------|--------|-----------|
| `fact_medicaid_rate` | 597,483 | `state_code`, `cpt_hcpcs_code`, `modifier`, `medicaid_rate`, `effective_date` | State fee schedule PDFs/CSVs (47 states) | state_code -> dim_state; cpt_hcpcs_code -> dim_procedure |
| `fact_rate_comparison` | 302,332 | `state_code`, `procedure_code`, `medicaid_rate`, `medicare_nonfac_rate`, `pct_of_medicare`, `em_category` | CY2022 T-MSIS + CY2025 Medicare PFS (45 states) | state_code; procedure_code -> dim_procedure |

#### Enrollment

| Table | Rows | Key Columns | Source | Time Coverage |
|-------|------|-------------|--------|---------------|
| `fact_enrollment` | 5,250 | `state_code`, `year`, `month`, `total_enrollment`, `chip_enrollment`, `ffs_enrollment`, `mc_enrollment` | CMS monthly reports | 2013-2025 |
| `fact_mc_enrollment_summary` | 513 | `state_code`, `year`, `mc_penetration_pct`, `total_mc_enrollment` | CMS managed care data | Multi-year |
| `fact_mc_enrollment` | 7,804 | `state_code`, `program_name`, `plan_name`, `total_enrollment`, `year` | CMS MC enrollment | Multi-year |
| `fact_unwinding` | 57,759 | `state_code`, `metric`, `time_period`, `terminated_count`, `terminated_pct` | CMS post-PHE unwinding | 2023-2025 |

#### Claims & Utilization

| Table | Rows | Key Columns | Source | Notes |
|-------|------|-------------|--------|-------|
| `fact_claims` | 712,793 | `state_code`, `procedure_code`, `year`, `month`, `total_paid`, `total_claims` | T-MSIS aggregated | OT claims only |
| `fact_sdud_2025` | 2,637,009 | `state_code`, `ndc`, `product_name`, `units_reimbursed`, `number_of_prescriptions`, `total_amount_reimbursed` | data.medicaid.gov | Q1-Q4 2025 |
| `fact_drug_utilization` | 2,369,659 | `state_code`, `ndc`, `product_name`, `year`, `quarter`, `prescription_count`, `medicaid_reimbursed` | SDUD legacy | Multi-year |

#### Hospitals

| Table | Rows | Key Columns | Source |
|-------|------|-------------|--------|
| `fact_hospital_cost` | 18,019 | `provider_ccn`, `hospital_name`, `state_code`, `bed_count`, `net_income`, `net_patient_revenue`, `medicaid_days`, `uncompensated_care_cost`, `dsh_adjustment`, `cost_to_charge_ratio` | HCRIS cost reports |
| `fact_hospital_rating` | 5,426 | `provider_id`, `state_code`, `overall_rating`, `ownership_type`, `has_emergency` | CMS Care Compare |
| `fact_hospital_vbp` | 2,455 | `provider_id`, `state_code`, `total_performance_score`, `clinical_outcomes_score` | CMS VBP |
| `fact_hospital_hrrp` | 18,330 | `provider_id`, `state_code`, `measure_name`, `excess_readmission_ratio` | CMS HRRP |
| `fact_hac_measure` | 12,120 | `provider_id`, `measure_name`, `rate` | CMS HAC |

#### Nursing Facilities

| Table | Rows | Key Columns | Source |
|-------|------|-------------|--------|
| `fact_five_star` | 14,710 | `provider_ccn`, `state_code`, `overall_rating`, `ownership_type`, `hprd_total`, `hprd_rn`, `deficiency_count`, `fine_total_dollars`, `chain_name`, `turnover_rn_pct` | CMS Care Compare Five-Star |
| `fact_pbj_nurse_staffing` | 1,332,436 | `provider_ccn`, `state_code`, `nursing_hprd`, `hrs_rn`, `hrs_cna`, `mds_census` | CMS PBJ daily staffing |
| `fact_snf_cost` | 42,810 | `provider_ccn`, `state_code`, `medicaid_days`, `total_costs`, `net_income` | HCRIS SNF cost reports |
| `fact_nh_deficiency` | 419,452 | `provider_ccn`, `tag_number`, `description`, `severity_level` | CMS deficiency citations |

#### Pharmacy

| Table | Rows | Key Columns | Source |
|-------|------|-------------|--------|
| `fact_nadac` | 1,882,296 | `ndc`, `ndc_description`, `nadac_per_unit`, `effective_date`, `pricing_unit` | CMS NADAC |
| `fact_opioid_prescribing` | 539,181 | `geo_code`, `geo_desc`, `geo_level`, `year`, `opioid_prescribing_rate` | CMS Part D opioid |

#### Behavioral Health

| Table | Rows | Key Columns | Source |
|-------|------|-------------|--------|
| `fact_nsduh_prevalence` | 5,865 | `state_code`, `measure_id`, `measure_name`, `age_group`, `estimate_pct` | SAMHSA NSDUH |
| `fact_mh_facility` | 27,957 | `facility_id`, `state_code`, `offers_mh`, `offers_su`, `hospital_beds`, `residential_beds` | SAMHSA N-MHSS |
| `fact_block_grant` | 55 | `state_code`, `allotment` | SAMHSA block grants |

#### Quality

| Table | Rows | Key Columns | Source |
|-------|------|-------------|--------|
| `fact_quality_core_set_2024` | 10,972 | `state_code`, `measure_id`, `measure_name`, `state_rate`, `median_rate`, `domain` | CMS Core Set 2024 |
| `fact_quality_core_set_combined` | 35,993 | `state_code`, `core_set_year`, `measure_id`, `state_rate` | CMS Core Sets 2017-2024 |
| `fact_epsdt` | 54 | `state_code`, `fiscal_year`, `screening_ratio`, `participant_ratio` | CMS-416 |

#### Expenditure & Fiscal

| Table | Rows | Key Columns | Source |
|-------|------|-------------|--------|
| `fact_cms64_multiyear` | 117,936 | `state_code`, `fiscal_year`, `service_category`, `total_computable`, `federal_share`, `state_share` | CMS-64 FY2018-2024 ($5.7T total computable) |
| `fact_expenditure` | 5,379 | `state_code` | CMS-64 summary |
| `fact_macpac_spending_per_enrollee` | 63 | `state_name`, `fiscal_year`, `total_all` | MACPAC exhibits |
| `fact_fmap_historical` | 612 | `state_code`, `fiscal_year`, `rate_type`, `rate` | CMS FMAP (fmap, efmap) |
| `fact_census_state_finances` | 16,435 | `state_code`, `fiscal_year`, `category`, `amount_thousands` | Census Bureau |
| `fact_bea_state_gdp` | 13,440 | `geo_name`, `year`, `line_code`, `value` | BEA |

#### Workforce

| Table | Rows | Key Columns | Source |
|-------|------|-------------|--------|
| `fact_bls_wage` | 812 | `state_code`, `soc_code`, `occupation_title`, `hourly_mean`, `annual_mean`, `hourly_median` | BLS OEWS |
| `fact_bls_wage_national` | 831 | `soc_code`, `occupation_title`, `total_employment`, `hourly_median` | BLS OEWS national |
| `fact_hpsa` | 68,859 | `hpsa_id`, `state_code`, `discipline`, `hpsa_score`, `designation_population`, `shortage` | HRSA HPSA |
| `fact_workforce_projections` | 102,528 | `year`, `profession`, `state`, `supply_fte`, `demand_fte`, `pct_adequacy` | HRSA projections |
| `fact_nhsc_field_strength` | 222 | `state_name`, `discipline`, `total_clinicians` | HRSA NHSC |

#### LTSS/HCBS

| Table | Rows | Key Columns | Source |
|-------|------|-------------|--------|
| `fact_hcbs_waitlist` | 51 | `state_code`, `total_waiting`, `idd_waiting`, `seniors_physical_waiting` | KFF survey |
| `fact_section_1115_waivers` | 665 | `state_code`, `waiver_name`, `status`, `approval_date`, `expiration_date` | CMS/KFF |

#### Program Integrity

| Table | Rows | Key Columns | Source |
|-------|------|-------------|--------|
| `fact_leie` | 82,749 | `state_code`, `npi`, `exclusion_type`, `exclusion_date`, `reinstatement_date` | OIG LEIE |
| `fact_open_payments` | 39,640 | `state_code`, `specialty`, `payment_category`, `total_amount`, `payment_count`, `unique_physicians` | CMS Open Payments |
| `fact_mfcu_stats` | 53 | `state_code`, `fiscal_year`, `total_investigations`, `total_convictions`, `total_recoveries` | OIG MFCU |
| `fact_perm_rates` | 12 | `year`, `overall_rate_pct`, `ffs_rate_pct`, `mc_rate_pct`, `eligibility_rate_pct` | CMS PERM |

#### Maternal & Child Health

| Table | Rows | Key Columns | Source |
|-------|------|-------------|--------|
| `fact_maternal_morbidity` | 435 | `year`, `geography`, `category`, `rate`, `delivery_count` | CMS Maternal Health |
| `fact_infant_mortality_state` | 1,167 | `state_fips`, `time_period`, `estimate` | CDC NCHS |
| `fact_svi_county` | 3,144 | `st_abbr`, `county`, `fips`, `rpl_themes`, `e_totpop` | CDC SVI (county-level) |

#### Economic

| Table | Rows | Key Columns | Source |
|-------|------|-------------|--------|
| `fact_pos_hospital` | 13,510 | `provider_id`, `state_code`, `facility_name`, `total_beds`, `teaching_status` | CMS POS |
| `fact_mco_mlr` | 2,282 | `state_code`, `plan_name`, `adjusted_mlr`, `member_months`, `remittance_amount` | CMS MCO MLR |

### Reference Tables

22 reference Parquet files in `data/lake/reference/`, including:
- `ref_1115_waivers` -- Section 1115 waiver catalog
- ICD crosswalks, NDC mappings, FIPS codes, taxonomy codes, etc.

---

## 4. Module Inventory

### 15 Core Structured Modules

| # | Module | Route | Key Endpoints | Primary Tables |
|---|--------|-------|---------------|----------------|
| 1 | **Intelligence** (home) | `/#/` or `/#/intelligence` | `POST /api/intelligence/stream` | All lake tables via DuckDB |
| 2 | **State Profiles** | `/#/state/{code}` | `/api/states`, `/api/enrollment/{st}`, `/api/rates/{st}`, `/api/quality/{st}`, `/api/wages/{st}`, `/api/pharmacy/{st}`, `/api/economic/{st}`, `/api/insights/{st}` | dim_state, enrollment, rates, hospitals, quality, workforce, pharmacy, economic |
| 3 | **Rate Comparison** | `/#/rates` | `/api/bulk/medicaid-rates`, `/api/bulk/medicare-rates`, `/api/bulk/hcpcs-rates` | fact_medicaid_rate, fact_rate_comparison, dim_procedure |
| 4 | **CPRA Generator** | `/#/cpra` | `/api/cpra/states`, `/api/cpra/rates/{st}`, `/api/cpra/dq/{st}`, `/api/cpra/compare`, `POST /api/cpra/upload/generate` | fact_rate_comparison, CPRA reference data |
| 5 | **Rate Lookup** | `/#/lookup` | `/api/bulk/fee-schedule-rates` | fact_medicaid_rate |
| 6 | **Caseload & Expenditure Forecasting** | `/#/forecast` | `/api/forecast/generate`, `/api/forecast/expenditure`, `/api/forecast/public-enrollment` | fact_enrollment, fact_expenditure, FMAP |
| 7 | **Fiscal Impact** | `/#/fiscal-impact` | `POST /api/forecast/fiscal-impact` | fact_cms64_multiyear, fact_fmap_historical |
| 8 | **Spending Efficiency** | `/#/spending` | `/api/spending/by-state`, `/api/spending/per-enrollee` | fact_cms64_multiyear, fact_macpac_spending_per_enrollee |
| 9 | **Hospital Intelligence** | `/#/hospitals` | `/api/hospitals/search`, `/api/hospitals/ccn/{ccn}`, `/api/hospitals/ccn/{ccn}/peers`, `/api/hospitals/summary` | fact_hospital_cost |
| 10 | **AHEAD Calculator** | `/#/ahead` | Hospital readiness scoring | fact_hospital_cost, fact_dsh_hospital, fact_hospital_rating |
| 11 | **Hospital Rate Setting** | `/#/hospital-rates` | `/api/hospitals/summary`, `/api/supplemental/dsh/summary`, `/api/supplemental/sdp` | fact_hospital_cost, fact_dsh_hospital, fact_sdp_preprint |
| 12 | **Nursing Facility** | `/#/nursing` | `/api/five-star/summary`, `/api/five-star/{st}`, `/api/staffing/summary`, `/api/staffing/{st}` | fact_five_star, fact_pbj_nurse_staffing |
| 13 | **Behavioral Health & SUD** | `/#/behavioral-health` | `/api/behavioral-health/nsduh/measures`, `/api/behavioral-health/facilities/summary`, `/api/opioid/prescribing/summary` | fact_nsduh_prevalence, fact_mh_facility, fact_opioid_prescribing |
| 14 | **Pharmacy Intelligence** | `/#/pharmacy` | `/api/pharmacy/sdud-2025/state-summary`, `/api/pharmacy/sdud-2025/top-drugs`, `/api/pharmacy/nadac` | fact_sdud_2025, fact_nadac |
| 15 | **Program Integrity** | `/#/integrity` | `/api/integrity/leie-summary`, `/api/integrity/open-payments-summary`, `/api/integrity/mfcu`, `/api/integrity/perm` | fact_leie, fact_open_payments, fact_mfcu_stats, fact_perm_rates |

### Additional Modules: Workforce, Compliance, Data Catalog

| Module | Route | Key Endpoints |
|--------|-------|---------------|
| Wage Adequacy | `/#/wages` | `/api/wages/{st}`, `/api/wages/bulk`, `/api/wages/national` |
| Compliance Center | `/#/compliance` | Rate transparency, HCBS pass-through |
| Data Catalog | `/#/catalog` | `/api/catalog` |

---

## 5. Research Module Detail

### Research Module 1: Rate-Quality Nexus

**Research question:** Does paying Medicaid providers more improve quality, access, and workforce outcomes?

**Key finding:** p=0.178 -- rates do not predict quality after controlling for state characteristics.

**Endpoints:**
| Endpoint | Parameters | Tables Used |
|----------|-----------|-------------|
| `GET /api/research/rate-quality/correlation` | `measure_id` (default: "prenatal_care") | fact_rate_comparison, fact_quality_core_set_2024 |
| `GET /api/research/rate-quality/measures` | none | fact_quality_core_set_2024 |
| `GET /api/research/rate-quality/access` | none | fact_rate_comparison, fact_hpsa |
| `GET /api/research/rate-quality/workforce` | none | fact_rate_comparison, fact_bls_wage |
| `GET /api/research/rate-quality/detail` | none | fact_rate_comparison, fact_quality_core_set_2024, fact_hpsa, fact_mc_enrollment_summary |

**Key columns referenced:** `pct_of_medicare` (range filter: 10-500, AUDIT FIX from prior >0 AND <10), `state_rate`, `hpsa_id`, `hourly_mean`, `mc_penetration_pct`

**Statistical methods:** OLS correlation, scatter plot with controls, cross-domain join

---

### Research Module 2: Managed Care Value Assessment

**Research question:** Is managed care saving Medicaid money or just retaining margin?

**Key finding:** -$16/enrollee (p=0.058), quality declines in high-MC states, $113B MCO retention

**Endpoints:**
| Endpoint | Parameters | Tables Used |
|----------|-----------|-------------|
| `GET /api/research/mc-value/penetration-spending` | none | fact_mc_enrollment_summary, fact_macpac_spending_per_enrollee, dim_state |
| `GET /api/research/mc-value/mco-financials` | none | fact_mco_mlr |
| `GET /api/research/mc-value/mco-summary` | none | fact_mco_mlr |
| `GET /api/research/mc-value/quality-by-tier` | `measure_id` (optional) | fact_mc_enrollment_summary, fact_quality_core_set_2024 |
| `GET /api/research/mc-value/trend` | none | fact_mc_enrollment_summary, fact_cms64_multiyear |

**Key columns:** `mc_penetration_pct`, `adjusted_mlr`, `member_months`, `remittance_amount`, `total_computable`

---

### Research Module 3: Nursing Ownership & Quality

**Research question:** Does for-profit ownership reduce nursing home quality?

**Key finding:** -0.67 stars for for-profit (Cohen's d=0.59, p<0.0001) across 14,710 facilities

**Endpoints:**
| Endpoint | Parameters | Tables Used |
|----------|-----------|-------------|
| `GET /api/research/nursing-ownership/quality-by-type` | none | fact_five_star |
| `GET /api/research/nursing-ownership/chain-vs-independent` | none | fact_five_star |
| `GET /api/research/nursing-ownership/deficiency-patterns` | none | fact_nh_deficiency, fact_five_star |
| `GET /api/research/nursing-ownership/chain-scoreboard` | `limit` (default 50) | fact_five_star |
| `GET /api/research/nursing-ownership/state-breakdown` | none | fact_five_star |

**Key columns:** `ownership_type`, `overall_rating`, `chain_name`, `hprd_total`, `hprd_rn`, `deficiency_count`, `fine_total_dollars`, `turnover_rn_pct`, `tag_number`, `severity_level`

**Join key:** `provider_ccn` links fact_five_star to fact_nh_deficiency

---

### Research Module 4: Pharmacy Spread Analysis

**Research question:** How much are states overpaying for drugs relative to acquisition cost?

**Key finding:** $3.15B net overpayment (NADAC vs SDUD)

**Endpoints:**
| Endpoint | Parameters | Tables Used |
|----------|-----------|-------------|
| `GET /api/research/pharmacy-spread/overview` | none | fact_nadac, fact_sdud_2025 |
| `GET /api/research/pharmacy-spread/by-state` | none | fact_nadac, fact_sdud_2025 |
| `GET /api/research/pharmacy-spread/top-drugs` | `limit` (default 50) | fact_nadac, fact_sdud_2025 |
| `GET /api/research/pharmacy-spread/stats` | none | fact_nadac, fact_sdud_2025 |

**Key columns:** `ndc` (join key), `nadac_per_unit`, `total_amount_reimbursed`, `units_reimbursed`, `number_of_prescriptions`

**Method:** Join NADAC latest price per NDC (ROW_NUMBER OVER PARTITION BY ndc ORDER BY effective_date DESC) to SDUD aggregated reimbursement. Spread = reimbursement_per_unit - nadac_per_unit. Excludes state_code = 'XX' and zero-unit records.

---

### Research Module 5: Opioid Treatment Gap

**Research question:** Where does OUD prevalence outstrip treatment capacity and funding?

**Key finding:** MAT spending misaligned with OUD prevalence

**Endpoints:**
| Endpoint | Parameters | Tables Used |
|----------|-----------|-------------|
| `GET /api/research/treatment-gap/demand-supply` | none | fact_nsduh_prevalence, fact_mh_facility, fact_enrollment |
| `GET /api/research/treatment-gap/mat-utilization` | none | fact_sdud_2025 |
| `GET /api/research/treatment-gap/prescribing` | none | fact_opioid_prescribing, dim_state |
| `GET /api/research/treatment-gap/funding` | none | fact_nsduh_prevalence, fact_block_grant, fact_enrollment |

**Key columns:** `measure_id` (tries: oud_past_year, opioid_misuse_past_year, sud_past_year), `estimate_pct`, `offers_su`, `offers_detox`, `residential_beds`, `allotment`, `product_name` (ILIKE for buprenorphine/suboxone/naloxone/naltrexone/vivitrol/sublocade)

**Note:** NSDUH measure_id for OUD is `oud_past_year`, not `opioid_use_disorder` (AUDIT FIX).

---

### Research Module 6: Safety Net Stress Test

**Research question:** Which states are under compound safety-net failure?

**Key finding:** 20 states show compound failure across hospital margins, HCBS waitlists, nursing quality, and FMAP exposure

**Endpoints:**
| Endpoint | Parameters | Tables Used |
|----------|-----------|-------------|
| `GET /api/research/safety-net/hospital-stress` | `state` (optional) | fact_hospital_cost |
| `GET /api/research/safety-net/ltss-pressure` | `state` (optional) | fact_hcbs_waitlist, fact_five_star, fact_enrollment |
| `GET /api/research/safety-net/staffing-crisis` | `state` (optional) | fact_pbj_nurse_staffing |
| `GET /api/research/safety-net/composite` | `state` (optional) | fact_hospital_cost, fact_hcbs_waitlist, fact_five_star, fact_fmap_historical, dim_state, fact_enrollment |

**Composite formula:** hospital_stress (% negative margin) + hcbs_pressure (waitlist per 1,000) + nursing_deficit (5 - avg_rating) + fmap_rate

---

### Research Module 7: Integrity Risk Index

**Research question:** Where is program integrity risk highest?

**Key finding:** Composite scoring: Open Payments + LEIE + PERM + MFCU

**Endpoints:**
| Endpoint | Parameters | Tables Used |
|----------|-----------|-------------|
| `GET /api/research/integrity-risk/composite` | `state` (optional) | fact_open_payments, fact_leie, fact_enrollment, dim_state |
| `GET /api/research/integrity-risk/open-payments` | `state` (optional) | fact_open_payments |
| `GET /api/research/integrity-risk/enforcement` | `state`, `fiscal_year` (both optional) | fact_mfcu_stats |
| `GET /api/research/integrity-risk/perm` | `fiscal_year` (optional) | fact_perm_rates |

**Key columns:** `total_amount`, `payment_count`, `unique_physicians` (Open Payments); `exclusion_count` (LEIE, WHERE reinstatement_date IS NULL); `total_investigations`, `total_convictions`, `total_recoveries`, `mfcu_grant_expenditures` (MFCU); `overall_rate_pct`, `ffs_rate_pct`, `mc_rate_pct`, `eligibility_rate_pct` (PERM)

**AUDIT FIX notes:** MFCU columns corrected from generic names to actual schema (total_investigations, total_convictions, civil_settlements_judgments, total_recoveries, mfcu_grant_expenditures). PERM column names corrected: fiscal_year->year, improper_payment_rate_pct->overall_rate_pct, managed_care_rate_pct->mc_rate_pct, eligibility_error_rate_pct->eligibility_rate_pct.

---

### Research Module 8: Fiscal Cliff Analysis

**Research question:** Which states hit the fiscal wall first as enhanced FMAP expires?

**Key finding:** $489/enrollee/yr spending growth, FMAP exposure varies widely

**Endpoints:**
| Endpoint | Parameters | Tables Used |
|----------|-----------|-------------|
| `GET /api/research/fiscal-cliff/spending-vs-revenue` | `state`, `fiscal_year` (both optional) | fact_cms64_multiyear, fact_census_state_finances |
| `GET /api/research/fiscal-cliff/fmap-impact` | `state` (optional) | fact_fmap_historical |
| `GET /api/research/fiscal-cliff/budget-pressure` | `state` (optional) | fact_cms64_multiyear, fact_census_state_finances, fact_bea_state_gdp, fact_fmap_historical, dim_state |
| `GET /api/research/fiscal-cliff/vulnerability` | `state` (optional) | fact_cms64_multiyear, fact_census_state_finances, fact_fmap_historical, dim_state |

**Key formula:** medicaid_pct_of_revenue = state_share * 100 / total_tax_revenue. Census finances joined on state_code + fiscal_year where category = 'Total Taxes'. GDP joined via dim_state.state_name = bea.geo_name.

---

### Research Module 9: Maternal Health Deserts

**Research question:** Where do mortality, access gaps, SVI, and quality compound?

**Key finding:** Mortality x SVI x HPSA x quality composite identifies compound maternal health deserts

**Endpoints:**
| Endpoint | Parameters | Tables Used |
|----------|-----------|-------------|
| `GET /api/research/maternal-health/mortality` | `state`, `year` (both optional) | fact_maternal_morbidity, dim_state |
| `GET /api/research/maternal-health/national-trend` | none | fact_cdc_maternal_mortality_prov |
| `GET /api/research/maternal-health/access` | `state` (optional) | fact_hpsa, fact_svi_county, dim_state |
| `GET /api/research/maternal-health/quality` | `state` (optional) | fact_quality_core_set_2024 |
| `GET /api/research/maternal-health/infant-mortality` | `state` (optional) | fact_infant_mortality_state, fact_svi_county, dim_state |
| `GET /api/research/maternal-health/composite` | `state` (optional) | fact_maternal_morbidity, fact_hpsa, fact_svi_county, fact_quality_core_set_2024, dim_state |

**Key joins:** fact_maternal_morbidity.geography -> dim_state.state_name (UPPER match). fact_svi_county.st_abbr -> state_code. fact_infant_mortality_state via FIPS from fact_svi_county.

---

### Research Module 10: Section 1115 Waiver Impact

**Research question:** Do Section 1115 waivers actually improve outcomes?

**Key finding:** 647 waivers cataloged across 54 jurisdictions; before/after framework

**Endpoints:**
| Endpoint | Parameters | Tables Used |
|----------|-----------|-------------|
| `GET /api/research/waiver-impact/catalog` | `state`, `status`, `search` (all optional) | ref_1115_waivers OR fact_section_1115_waivers OR fact_kff_1115_waivers (tries in priority order) |
| `GET /api/research/waiver-impact/enrollment/{state_code}` | none (path param) | fact_enrollment |
| `GET /api/research/waiver-impact/spending/{state_code}` | none (path param) | fact_cms64_multiyear |
| `GET /api/research/waiver-impact/quality/{state_code}` | none (path param) | fact_quality_core_set_combined (falls back to 2024) |
| `GET /api/research/waiver-impact/compare` | `waiver_type` (default: "expansion") | waiver table, fact_enrollment, fact_cms64_multiyear, dim_state |

**Schema discovery:** The waiver catalog endpoint dynamically discovers which waiver table is available and which columns exist (waiver_type vs authority_type vs request_type, etc.).

---

## 6. API Endpoint Reference

### System Endpoints

| Method | Path | Description |
|--------|------|-------------|
| GET | `/health` | Lightweight health check (always 200) |
| GET | `/ready` | Readiness probe (200 only when lake ready) |
| POST | `/internal/reload-lake` | Re-scan lake directory (internal, Fly.io only) |

### Intelligence & AI

| Method | Path | Description |
|--------|------|-------------|
| POST | `/api/intelligence/stream` | SSE streaming intelligence chat |
| GET | `/api/intelligence/corpus/stats` | Policy corpus statistics |
| GET | `/api/intelligence/corpus/search` | BM25 search over policy corpus |
| POST | `/api/query` | Raw DuckDB query execution |
| POST | `/api/nl2sql` | Natural language to SQL |
| GET | `/api/search` | Platform-wide Cmd+K search |

### State & Enrollment

| Method | Path | Description |
|--------|------|-------------|
| GET | `/api/states` | All states with metadata |
| GET | `/api/enrollment/{state_code}` | Monthly enrollment for a state |
| GET | `/api/enrollment/eligibility/{state_code}` | Eligibility/determination data |
| GET | `/api/enrollment/expansion/{state_code}` | ACA expansion (Group VIII) |
| GET | `/api/enrollment/unwinding/{state_code}` | Post-PHE unwinding outcomes |
| GET | `/api/enrollment/managed-care-plans/{state_code}` | MC plan-level enrollment |
| GET | `/api/enrollment/applications` | Medicaid application data |
| GET | `/api/enrollment/renewals` | Renewal processing |
| GET | `/api/enrollment/dual-status` | Dual eligible status |

### Rates & CPRA

| Method | Path | Description |
|--------|------|-------------|
| GET | `/api/cpra/states` | CPRA state list |
| GET | `/api/cpra/rates/{state_code}` | Pre-computed CPRA rates |
| GET | `/api/cpra/dq/{state_code}` | DQ flags for state |
| GET | `/api/cpra/compare` | Cross-state CPRA comparison |
| POST | `/api/cpra/upload/generate` | Upload fee schedule, generate CPRA |
| GET | `/api/bulk/medicaid-rates` | All Medicaid rates |
| GET | `/api/bulk/medicare-rates` | All Medicare rates |
| GET | `/api/bulk/hcpcs-rates` | HCPCS-level rates |
| GET | `/api/bulk/fee-schedule-rates` | Fee schedule rate lookup |

### Hospitals & Providers

| Method | Path | Description |
|--------|------|-------------|
| GET | `/api/hospitals/search` | Search by name/city/CCN |
| GET | `/api/hospitals/ccn/{ccn}` | Full HCRIS data for CCN |
| GET | `/api/hospitals/ccn/{ccn}/peers` | Peer benchmark statistics |
| GET | `/api/hospitals/summary` | State-level hospital summary |
| GET | `/api/hospitals/{state_code}` | Hospital list for a state |
| GET | `/api/nursing-facilities/{state_code}` | SNF cost reports for state |
| GET | `/api/hospitals/directory` | Hospital directory |

### Quality & Ratings

| Method | Path | Description |
|--------|------|-------------|
| GET | `/api/five-star/summary` | State-level nursing home ratings |
| GET | `/api/five-star/{state_code}` | Facility-level Five-Star for state |
| GET | `/api/quality/{state_code}` | Core Set quality measures |
| GET | `/api/hospital-ratings/{state_code}` | Hospital overall ratings |
| GET | `/api/vbp/{state_code}` | VBP scores |
| GET | `/api/hrrp/{state_code}` | Readmission reduction data |
| GET | `/api/hac/{state_code}` | HAC measure rates |
| GET | `/api/epsdt` | EPSDT participation (all states) |
| GET | `/api/hpsa/summary` | HPSA summary by state/discipline |
| GET | `/api/hpsa/{state_code}` | HPSA designations for state |
| GET | `/api/mua/summary` | MUA/MUP summary |

### Workforce

| Method | Path | Description |
|--------|------|-------------|
| GET | `/api/wages/{state_code}` | BLS wages for state |
| GET | `/api/wages/compare/{soc_code}` | Cross-state wage comparison |
| GET | `/api/wages/msa/{state_code}` | MSA-level wages |
| GET | `/api/wages/bulk` | All state + national wages (nested) |
| GET | `/api/wages/national` | National-level wages |
| GET | `/api/workforce/projections` | HRSA supply/demand projections |
| GET | `/api/workforce/nursing` | NSSRN nursing workforce |
| GET | `/api/workforce/nhsc` | NHSC clinician counts |

### Pharmacy

| Method | Path | Description |
|--------|------|-------------|
| GET | `/api/pharmacy/sdud-2025/state-summary` | SDUD 2025 by state |
| GET | `/api/pharmacy/sdud-2025/top-drugs` | Top drugs by spending |
| GET | `/api/pharmacy/nadac` | NADAC pricing (search by NDC or name) |
| GET | `/api/pharmacy/utilization/{state_code}` | Drug utilization for state |
| GET | `/api/pharmacy/top-drugs/{state_code}` | Top drugs by state |

### Behavioral Health

| Method | Path | Description |
|--------|------|-------------|
| GET | `/api/behavioral-health/nsduh/measures` | NSDUH measure list |
| GET | `/api/behavioral-health/nsduh/ranking` | State ranking by measure |
| GET | `/api/behavioral-health/facilities/summary` | MH/SUD facility summary |
| GET | `/api/behavioral-health/conditions/summary` | BH condition summary |
| GET | `/api/behavioral-health/services/summary` | BH service utilization |
| GET | `/api/opioid/prescribing/summary` | Opioid prescribing summary |

### Spending & Fiscal

| Method | Path | Description |
|--------|------|-------------|
| GET | `/api/spending/by-state` | CMS-64 multiyear by state |
| GET | `/api/spending/per-enrollee` | MACPAC per-enrollee spending |
| GET | `/api/expenditure/{state_code}` | Expenditure for state |
| GET | `/api/economic/{state_code}` | Economic context for state |
| POST | `/api/forecast/generate` | Caseload forecast |
| POST | `/api/forecast/expenditure` | Expenditure projection |
| POST | `/api/forecast/fiscal-impact` | Fiscal impact model |

### Supplemental Payments

| Method | Path | Description |
|--------|------|-------------|
| GET | `/api/supplemental/summary` | MACPAC supplemental summary |
| GET | `/api/supplemental/dsh/summary` | DSH allotment summary |
| GET | `/api/supplemental/dsh/hospitals` | Hospital-level DSH |
| GET | `/api/supplemental/sdp` | State Directed Payments |

### Program Integrity

| Method | Path | Description |
|--------|------|-------------|
| GET | `/api/integrity/leie-summary` | LEIE exclusion summary |
| GET | `/api/integrity/open-payments-summary` | Open Payments summary |
| GET | `/api/integrity/mfcu` | MFCU enforcement stats |
| GET | `/api/integrity/perm` | PERM error rates |

### Research Endpoints (10 modules, 44 endpoints)

See Section 5 above for complete research endpoint documentation. All research endpoints are prefixed with `/api/research/`.

### Staffing

| Method | Path | Description |
|--------|------|-------------|
| GET | `/api/staffing/summary` | State-level PBJ staffing summary |
| GET | `/api/staffing/{state_code}` | Facility-level staffing for state |

---

## 7. Data Relationships

### Universal Join Key: `state_code`

Nearly every fact table contains `state_code` (2-letter abbreviation). This is the universal join path through `dim_state`.

### Key Join Paths

```
dim_state.state_code
    +-- fact_enrollment.state_code
    +-- fact_rate_comparison.state_code
    +-- fact_hospital_cost.state_code
    +-- fact_five_star.state_code
    +-- fact_quality_core_set_2024.state_code
    +-- fact_cms64_multiyear.state_code
    +-- fact_sdud_2025.state_code
    +-- fact_bls_wage.state_code
    +-- fact_hpsa.state_code
    +-- fact_nsduh_prevalence.state_code
    +-- fact_open_payments.state_code
    +-- fact_leie.state_code
    +-- [nearly every other fact table]
```

### Procedure Code Joins

```
dim_procedure.procedure_code
    +-- fact_medicaid_rate.cpt_hcpcs_code
    +-- fact_rate_comparison.procedure_code
    +-- fact_claims.procedure_code
```

### Provider/Facility Joins

```
fact_hospital_cost.provider_ccn
    +-- fact_hospital_rating.provider_id (different naming!)
    +-- fact_hospital_vbp.provider_id
    +-- fact_hospital_hrrp.provider_id
    +-- fact_hac_measure.provider_id

fact_five_star.provider_ccn
    +-- fact_nh_deficiency.provider_ccn
    +-- fact_pbj_nurse_staffing.provider_ccn
    +-- fact_snf_cost.provider_ccn
```

### Drug/NDC Joins

```
fact_sdud_2025.ndc
    +-- fact_nadac.ndc
    +-- fact_drug_utilization.ndc
```

### Geographic Joins

```
fact_svi_county.st_abbr -> dim_state.state_code
fact_svi_county.fips -> county-level tables
fact_bea_state_gdp: JOIN dim_state ON state_name = geo_name
fact_maternal_morbidity: JOIN dim_state ON UPPER(geography) = UPPER(state_name)
fact_census_state_finances: JOIN ON state_code + fiscal_year
fact_infant_mortality_state: JOIN via fact_svi_county (FIPS mapping)
```

### Time Dimensions

- **Calendar Year:** fact_enrollment.year, fact_rate_comparison.snapshot
- **Federal Fiscal Year (Oct-Sep):** fact_cms64_multiyear.fiscal_year, fact_fmap_historical.fiscal_year
- **Quarterly:** fact_sdud_2025.quarter, fact_drug_utilization.quarter
- **Monthly:** fact_enrollment.month, fact_pbj_nurse_staffing (daily)

### Cross-Module Join Examples

```sql
-- Rate-quality nexus: rates + quality + workforce
fact_rate_comparison r
JOIN fact_quality_core_set_2024 q ON r.state_code = q.state_code
JOIN fact_bls_wage w ON r.state_code = w.state_code

-- Fiscal cliff: spending + revenue + FMAP
fact_cms64_multiyear s
JOIN fact_census_state_finances r ON s.state_code = r.state_code AND s.fiscal_year = r.fiscal_year
JOIN fact_fmap_historical f ON s.state_code = f.state_code AND s.fiscal_year = f.fiscal_year

-- Maternal composite: mortality + HPSA + SVI + quality
fact_maternal_morbidity m
JOIN dim_state d ON UPPER(m.geography) = UPPER(d.state_name)
JOIN fact_hpsa h ON d.state_code = h.state_code
JOIN fact_svi_county s ON d.state_code = s.st_abbr
JOIN fact_quality_core_set_2024 q ON d.state_code = q.state_code
```

---

## 8. Known Data Gaps

### Missing States

| Dataset | Missing States | Reason |
|---------|---------------|--------|
| Medicaid fee schedules | KS, NJ, TN, WI | KS/NJ portal login; TN MC-only; WI manual |
| Rate comparison (T-MSIS) | ~6 states | DQ Atlas quality issues |
| HCBS waitlists | ~10 states | Not all states report |

### Data Approximations

| Item | Approximation | Impact |
|------|--------------|--------|
| SDUD amounts | Pre-rebate | Overstates net Medicaid drug costs |
| SDUD suppression | Cells <11 Rx suppressed | ~6 Rx mean per suppressed cell (Urban Institute FOIA) |
| T-MSIS MC encounters | Payment amounts often $0 | Cannot derive MCO-to-provider rates |
| HCRIS cost reports | Not audited, not GAAP | Outlier values common; winsorize |
| NPPES taxonomy | Self-reported, unverified | 8.2% updated within past year |
| AHEAD calculator | Years 1-2 estimated backward from Y3 | Actual y1/y2 data not available |
| MACPAC per-enrollee | State-name-based join | Requires REGEXP cleanup for matching |

### Suppression Rules

- **SDUD:** Prescriptions <11 per NDC-state-quarter-utilization_type are suppressed. Secondary suppression prevents back-calculation.
- **T-MSIS:** Minimum cell size n >= 11 for utilization counts.
- **General Aradune rule:** Never publish counts below n=11.

### Blocked Data Sources

| Source | Reason |
|--------|--------|
| AHRQ SDOH Database | WAF prevents download |
| Area Deprivation Index (ADI) | Registration/WAF |
| 340B covered entities detail | Blazor app, not scrapable |
| State MAC pricing (beyond NY/TX) | Per-state portals |

### Known Schema Issues

- **SDUD schema variation:** Older years use different column names (state vs state_code, num_prescriptions vs number_of_prescriptions). Standardized in fact_sdud_2025.
- **Census sentinel values:** -888888888 = NULL. Must filter.
- **DOGE data quarantined:** 3-layer quarantine (system prompt, ontology, ETL docstring). OT claims only, no beneficiary state, suppresses <12, Nov/Dec 2024 incomplete.

---

## 9. Audit Test Catalog

### Schema Validation Tests

| Test ID | Test | SQL/Logic | Expected |
|---------|------|-----------|----------|
| SV-01 | dim_state has all 50 states + DC | `SELECT COUNT(*) FROM dim_state WHERE LENGTH(state_code) = 2` | >= 51 |
| SV-02 | fact_enrollment has state_code, year, month, total_enrollment | DESCRIBE check | All columns present |
| SV-03 | fact_rate_comparison.pct_of_medicare is numeric and reasonable | `SELECT COUNT(*) FROM fact_rate_comparison WHERE pct_of_medicare < 0 OR pct_of_medicare > 1000` | 0 or near 0 |
| SV-04 | fact_sdud_2025 uses standardized column names | `DESCRIBE fact_sdud_2025` | state_code (not state), number_of_prescriptions (not num_prescriptions) |
| SV-05 | All fact tables have snapshot_date or snapshot column | DESCRIBE check per table | Present |
| SV-06 | No duplicate primary keys in dim tables | `SELECT state_code, COUNT(*) FROM dim_state GROUP BY state_code HAVING COUNT(*) > 1` | 0 rows |
| SV-07 | fact_five_star.overall_rating in range 1-5 | `SELECT COUNT(*) FROM fact_five_star WHERE overall_rating NOT BETWEEN 1 AND 5` | 0 |
| SV-08 | fact_cms64_multiyear.total_computable > 0 for non-zero records | Spot check | Positive values |

### Data Accuracy Benchmarks

| Test ID | Test | Expected Benchmark | Source |
|---------|------|-------------------|--------|
| DA-01 | Total CMS-64 computable across all states/years | ~$5.7T | CMS published totals |
| DA-02 | Open Payments total | ~$10.83B (3 CMS categories) | CMS Open Payments |
| DA-03 | LEIE active exclusions | ~82K | OIG published count |
| DA-04 | Nursing facilities (Five-Star) | ~14,700 | CMS published count |
| DA-05 | HPSA designations | ~69K | HRSA published count |
| DA-06 | Section 1115 waivers | ~647-665 | CMS/KFF catalog |
| DA-07 | SDUD 2025 total_amount_reimbursed national total | Cross-check with published CMS figures | CMS SDUD totals |
| DA-08 | Enrollment totals match CMS published dashboard | Within 5% | CMS Enrollment Dashboard |
| DA-09 | Medicaid rate count per state | Reasonable distribution (>100 per state for most) | State fee schedule publications |
| DA-10 | PBJ nurse staffing row count | ~1.3M (quarterly, ~15K facilities x ~90 days) | CMS PBJ |

### Statistical Method Verification

| Test ID | Test | Method |
|---------|------|--------|
| SM-01 | Rate-quality correlation replicable | Run OLS on fact_rate_comparison AVG(pct_of_medicare) vs fact_quality_core_set_2024 state_rate. Verify p ~0.178 |
| SM-02 | Nursing ownership effect size | Compare AVG(overall_rating) for-profit vs non-profit from fact_five_star. Verify d ~0.59 |
| SM-03 | Pharmacy spread total | Sum (reimbursement_per_unit - nadac_per_unit) * total_units across all matched NDCs. Verify ~$3.15B |
| SM-04 | MC value per-enrollee differential | Regress per-enrollee spending on mc_penetration_pct. Verify coefficient ~-$16 |
| SM-05 | Fiscal cliff spending growth | (latest_spending - prior_spending) / prior_spending for 2-year window. Verify ~$489/enrollee/yr |
| SM-06 | Pharmacy spread: filter validation | Verify NADAC join uses latest price per NDC (ROW_NUMBER window function) |

### Frontend-Backend Contract Tests

| Test ID | Test | Method |
|---------|------|--------|
| FB-01 | State Profile fetches all 8 APIs | Hit /api/states, /api/enrollment/FL, /api/rates/FL, /api/hospitals/FL, /api/quality/FL, /api/wages/FL, /api/pharmacy/FL, /api/economic/FL | All return 200 |
| FB-02 | Research endpoints return expected shape | Each research endpoint returns `{rows: [...], count: N}` | Verify structure |
| FB-03 | CPRA compare returns data for 45+ states | `GET /api/cpra/compare` | count >= 40 |
| FB-04 | Intelligence SSE stream returns valid events | `POST /api/intelligence/stream` | Events: status, token, metadata, done |
| FB-05 | Pharmacy NADAC search works | `GET /api/pharmacy/nadac?search=metformin` | Returns results |
| FB-06 | Hospital CCN lookup returns data | `GET /api/hospitals/ccn/100001` | Returns hospital or 404 |

### Edge Case Tests

| Test ID | Test | Risk |
|---------|------|------|
| EC-01 | State code 'XX' excluded from SDUD aggregations | XX = national total, would double-count |
| EC-02 | Census sentinel -888888888 not treated as real value | Would corrupt aggregations |
| EC-03 | pct_of_medicare filter range 10-500 (not >0 AND <10) | Old filter captured only 4.5% of data |
| EC-04 | Illinois T-MSIS claims not naively aggregated | Incremental credit/debit adjustment pattern |
| EC-05 | NADAC join uses latest effective_date per NDC | Multiple prices per NDC over time |
| EC-06 | HCBS waitlist state-level (51 rows, not individual) | fact_hcbs_waitlist has only 51 rows |
| EC-07 | MFCU stats have corrected column names | total_investigations, not cases_opened |
| EC-08 | PERM rates use 'year' not 'fiscal_year' | Schema mismatch audit fix |
| EC-09 | Maternal morbidity geography -> state join uses UPPER() | Case-sensitive join would miss matches |
| EC-10 | Infant mortality requires FIPS mapping through fact_svi_county | No direct state_code in fact_infant_mortality_state |

### Performance Tests

| Test ID | Test | Threshold |
|---------|------|-----------|
| PT-01 | Health check response time | < 100ms |
| PT-02 | Intelligence first token latency | < 3s for Tier 1 |
| PT-03 | State Profile full load (8 parallel API calls) | < 5s total |
| PT-04 | Pharmacy spread overview (NADAC x SDUD join) | < 10s |
| PT-05 | PBJ staffing summary (1.3M rows aggregation) | < 15s |
| PT-06 | View registration on cold start | < 60s for 650+ views |

### Cross-Module Consistency Tests

| Test ID | Test | Method |
|---------|------|--------|
| CM-01 | Enrollment totals consistent across modules | Compare fact_enrollment totals with fact_mc_enrollment_summary totals for same state/year |
| CM-02 | State codes consistent across all fact tables | All state_codes should appear in dim_state |
| CM-03 | Spending per enrollee = CMS-64 total / enrollment | Cross-check fact_cms64_multiyear with fact_enrollment |
| CM-04 | Hospital count consistent | fact_hospital_cost COUNT(*) by state should roughly match fact_pos_hospital COUNT(*) |
| CM-05 | Quality measures across years | fact_quality_core_set_2024 measures should be subset of fact_quality_core_set_combined |
| CM-06 | FMAP rates plausible | fact_fmap_historical rates should be between 0.50 and 0.83 (standard FMAP range) |
| CM-07 | MC penetration + FFS should sum to ~100% | fact_mc_enrollment_summary mc_penetration_pct should not exceed 100 |

---

## 10. Corrected Research Findings (Current as of 2026-03-14)

These are the authoritative numbers from the 8-prompt forensic audit completed in Session 29.

### Rate-Quality Nexus
- **Correlation:** p=0.178. Medicaid rate levels do not predict quality outcomes after controlling for state characteristics.
- **Data:** 45 states with both rate comparison and Core Set 2024 data
- **AUDIT FIX applied:** pct_of_medicare filter changed from `>0 AND <10` to `BETWEEN 10 AND 500` (old filter captured only 4.5% of data)

### Managed Care Value Assessment
- **Per-enrollee differential:** -$16 (p=0.058) -- MC states spend slightly less but not statistically significant
- **Quality:** Quality declines in highest-MC-penetration tier
- **MCO retention:** $113B in MCO administrative/profit retention
- **Data:** fact_mco_mlr (2,282 plan-level MLR records)

### Nursing Ownership & Quality
- **For-profit penalty:** -0.67 stars (Cohen's d=0.59, p<0.0001)
- **Sample:** 14,710 facilities in fact_five_star
- **Chain effect:** Chain-affiliated facilities underperform independents within each ownership type
- **Worst chains:** Identifiable via chain_scoreboard endpoint (minimum 5 facilities)

### Pharmacy Spread Analysis
- **Net overpayment:** $3.15B (NADAC vs SDUD)
- **Method:** Latest NADAC per-unit price joined to SDUD 2025 aggregated reimbursement by NDC
- **Coverage:** Excludes state_code='XX', zero-unit records, zero-price NADAC records
- **Note:** All SDUD amounts are pre-rebate

### Opioid Treatment Gap
- **Finding:** MAT spending misaligned with OUD prevalence
- **NSDUH measure:** `oud_past_year` (not `opioid_use_disorder` -- AUDIT FIX)
- **MAT drugs tracked:** buprenorphine, suboxone, naloxone, naltrexone, vivitrol, sublocade

### Safety Net Stress
- **Finding:** 20 states show compound failure
- **Dimensions:** Hospital margins (% negative), HCBS waitlists per 1,000, nursing quality deficit (5 - avg_rating), FMAP rate
- **Data limitation:** fact_hcbs_waitlist has only 51 rows (state-level, not individual waitlist records)

### Integrity Risk Index
- **Open Payments:** $10.83B across 3 CMS categories
- **LEIE active exclusions:** ~82K (WHERE reinstatement_date IS NULL)
- **PERM:** Historical error rates (12 data points, 2020-2025)
- **MFCU:** 53 state-year observations

### Fiscal Cliff
- **Spending growth:** ~$489/enrollee/yr
- **Exposure:** Varies by state FMAP and tax revenue base
- **Key metric:** Medicaid state share as % of total state tax revenue

### Maternal Health Deserts
- **Composite:** SMM rate x HPSA count x SVI score x maternal quality measures
- **Data source:** fact_maternal_morbidity (435 rows, state-level by year and category)
- **SVI:** County-level (3,144 counties), aggregated to state via AVG(rpl_themes)

### Waiver Impact
- **Catalog:** 647-665 Section 1115 waivers
- **Framework:** Before/after comparison using enrollment, CMS-64 spending, and Core Set quality trends
- **Quality over time:** fact_quality_core_set_combined (2017-2024, 35,993 rows)

---

## Appendix A: Intelligence System Prompt Tools

The Intelligence endpoint (`POST /api/intelligence/stream`) provides Claude with these tools:

1. **query_database** -- SELECT-only DuckDB over all 695 views + user temp tables. Max LIMIT 200. DuckDB syntax (ILIKE, :: casts).
2. **list_tables** -- Browse tables by domain (reads from entity registry)
3. **describe_table** -- Schema, row counts, sample data for any table
4. **web_search** -- Current policy/regulatory context (Anthropic built-in)
5. **search_policy** -- RAG over 1,039+ CMS docs (BM25 + FTS)

SQL safety: Only SELECT/WITH allowed. Forbidden keywords: INSERT, UPDATE, DELETE, DROP, ALTER, TRUNCATE, CREATE, REPLACE, GRANT, REVOKE, ATTACH, COPY, EXPORT, IMPORT, LOAD, INSTALL.

---

## Appendix B: CPRA Compliance Rules

- **68 codes** from CMS CY 2025 E/M Code List
- **$32.3465** CY 2025 CF (non-QPP) for CPRA calculations
- **$33.4009** CY 2026 CF for general comparison
- **Many-to-many code-category mapping** (171 pairs)
- Base rates only (no supplemental payments)
- Non-facility Medicare benchmark per 42 CFR 447.203
- Small cell suppression: beneficiary counts 1-10 suppressed
- Published by July 1, 2026; updated biennially

---

## Appendix C: Florida-Specific Rules (Always Enforce)

- **Facility and PC/TC rates are typically mutually exclusive (99.96% of codes).** Three codes (46924, 91124, 91125) legitimately carry both facility and PC/TC rates as published by AHCA.
- **Conversion factors:** Regular $24.9779582769; Lab $26.1689186096
- **8 schedule types** in the Florida Medicaid fee schedule
- Ad hoc CF $24.9876 is stale -- do not use

---

## Appendix D: Key File Paths

| File | Path | Description |
|------|------|-------------|
| Master reference | `/Users/jamestori/Desktop/Aradune/CLAUDE.md` | Project operating manual |
| Database module | `/Users/jamestori/Desktop/Aradune/server/db.py` | View registration, FACT_NAMES |
| Main app | `/Users/jamestori/Desktop/Aradune/server/main.py` | FastAPI app, router registration |
| Intelligence | `/Users/jamestori/Desktop/Aradune/server/routes/intelligence.py` | AI chat endpoint |
| Platform router | `/Users/jamestori/Desktop/Aradune/src/Platform.tsx` | Frontend tool registry |
| Ontology spec | `/Users/jamestori/Desktop/Aradune/ONTOLOGY_SPEC.md` | Entity/edge definitions |
| Data reference | `/Users/jamestori/Desktop/Aradune/COMPLETE-DATA-REFERENCE-FOR-ARADUNE.md` | Per-dataset quality issues |
| Data lake | `/Users/jamestori/Desktop/Aradune/data/lake/` | 4.7 GB Parquet files |
| ETL scripts | `/Users/jamestori/Desktop/Aradune/scripts/build_lake_*.py` | 115+ ingestion scripts |
| Research routes | `/Users/jamestori/Desktop/Aradune/server/routes/research/` | 10 research module backends |

---

## Appendix E: Quick Reference -- DuckDB Patterns

```sql
-- Register a view from parquet
CREATE VIEW IF NOT EXISTS fact_enrollment AS SELECT * FROM read_parquet('/path/to/data.parquet');

-- Check table exists
SELECT 1 FROM fact_enrollment LIMIT 1;

-- Get schema
DESCRIBE fact_enrollment;

-- Row count
SELECT COUNT(*) FROM fact_enrollment;

-- State-level aggregation pattern
SELECT state_code, AVG(metric) AS avg_metric
FROM fact_table
GROUP BY state_code
ORDER BY avg_metric DESC;

-- Latest snapshot pattern (NADAC example)
SELECT ndc, nadac_per_unit
FROM fact_nadac
QUALIFY ROW_NUMBER() OVER (PARTITION BY ndc ORDER BY effective_date DESC) = 1;

-- Cross-domain join (rates + quality)
WITH rates AS (
    SELECT state_code, AVG(pct_of_medicare) AS avg_pct
    FROM fact_rate_comparison
    WHERE pct_of_medicare BETWEEN 10 AND 500
    GROUP BY state_code
)
SELECT r.state_code, r.avg_pct, q.state_rate
FROM rates r
JOIN fact_quality_core_set_2024 q ON r.state_code = q.state_code
WHERE q.measure_id = 'prenatal_care';
```
