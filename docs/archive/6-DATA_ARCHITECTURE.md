# Aradune Data Architecture v1.0

> Canonical reference for the unified data layer. All pipelines write to this schema; all tools read from it.
> Created: 2026-03-05

---

## 1. Design Principles

1. **One schema, many writers.** CPRA engine, T-MSIS pipeline, CMS scrapers, and future sources all produce Parquet files conforming to the same dimensional model.
2. **DuckDB is the engine.** Local analytics use DuckDB (both server-side and browser-side via WASM). No Postgres required for the core data layer.
3. **Parquet is the format.** All persistent data is Hive-partitioned Parquet on S3 (or local disk during development). DuckDB reads Parquet natively.
4. **Immutable snapshots.** Every pipeline run produces a timestamped snapshot. Production reads from the latest validated snapshot. Bad data never overwrites good data.
5. **Dimensions are shared.** `dim_state`, `dim_procedure`, `dim_time` are maintained once and referenced everywhere. No per-pipeline copies.
6. **Validate before publish.** Every pipeline run must pass quality gates before its output is promoted to production.

---

## 2. Current State (What Exists)

Three disconnected pipelines, no shared schema:

| Pipeline | Language | Storage | Output | Status |
|----------|----------|---------|--------|--------|
| CPRA Engine (`cpra_engine.py`) | Python | `aradune_cpra.duckdb` | 7 JSON files to `public/data/` | Production (34 states) |
| T-MSIS Pipeline (`tmsis_pipeline_duckdb.R`) | R | `medicaid-provider-spending.duckdb` | 4 Parquet + 6 JSON to `public/data/` | Infrastructure ready, DuckDB empty |
| CMS Scrapers (`cms_data.py`, `ncci_scraper.py`) | Python | SQLite (`aradune.db`) | 12 tables | Populated but not exported to frontend |

**Problems:**
- No shared dimension tables (state metadata duplicated 3 ways)
- Manual copy-paste from pipeline output to `public/data/`
- No versioning — bad pipeline run overwrites production data
- T-MSIS claims can't weight CPRA averages (no join path)
- CMS supplemental data (enrollment, FMAP, quality, CMS-64) trapped in SQLite

---

## 3. Target Architecture

```
                       S3 / Local Disk
                   (Hive-partitioned Parquet)
                            |
           +----------------+----------------+
           |                |                |
     [CPRA Engine]   [T-MSIS Pipeline]  [CMS Scrapers]
      (Python)           (R/Python)       (Python)
           |                |                |
           v                v                v
    +----------------------------------------------+
    |           Dagster Orchestrator                |
    |  ingest -> validate -> transform -> publish   |
    +----------------------------------------------+
           |                                |
           v                                v
    +--------------+              +-----------------+
    | DuckDB       |              | public/data/    |
    | (server-side |              | (Parquet + JSON |
    |  analytics)  |              |  for frontend)  |
    +--------------+              +-----------------+
           |                                |
           v                                v
    +--------------+              +-----------------+
    | FastAPI      |              | DuckDB-WASM     |
    | /api/query   |              | (browser-side)  |
    +--------------+              +-----------------+
```

---

## 4. Unified Schema

### 4.1 Dimension Tables

All dimensions live in `s3://aradune-datalake/dimension/` (or `data/lake/dimension/` locally).

#### dim_state

Single source of truth for state metadata. Replaces: `states` (SQLite), `dim_state` (CPRA DuckDB), state data in R pipeline.

```sql
CREATE TABLE dim_state (
    state_code          VARCHAR(2)    PRIMARY KEY,  -- USPS abbreviation
    state_name          VARCHAR(50)   NOT NULL,
    region              VARCHAR(20),                -- Northeast, South, Midwest, West
    -- Fee schedule methodology
    methodology         VARCHAR(30),                -- rbrvs, pct_of_medicare, state_developed, hybrid
    conversion_factor   DECIMAL(10,4),
    cf_effective_date   DATE,
    rvu_source          VARCHAR(30),                -- medicare_pfs_cy2025, state_modified
    update_frequency    VARCHAR(20),                -- annual_jan, quarterly, as_needed
    fee_schedule_url    TEXT,
    -- Managed care context
    pct_managed_care    DECIMAL(5,2),               -- % beneficiaries in MCO
    ffs_relevance_note  TEXT,                        -- interpretation guidance
    -- Enrollment & spending (latest available)
    total_enrollment    INTEGER,
    ffs_enrollment      INTEGER,
    mc_enrollment       INTEGER,
    total_spending      DECIMAL(15,2),
    per_enrollee        DECIMAL(10,2),
    -- Federal match
    fmap                DECIMAL(5,4),               -- current FY FMAP rate
    efmap               DECIMAL(5,4),               -- enhanced FMAP
    -- Data quality
    dq_atlas_rating     VARCHAR(20),                -- low_concern, medium_concern, high_concern, unusable
    dq_notes            TEXT,                        -- per-state quality narrative
    -- Metadata
    data_vintage        VARCHAR(20),                -- e.g., "CY2025", "FY2026"
    last_updated        DATE
);
```

**Partitioning:** None (51 rows, single file).
**File:** `dimension/dim_state.parquet`

---

#### dim_procedure

All procedure codes with Medicare RVU components and classification.

```sql
CREATE TABLE dim_procedure (
    procedure_code      VARCHAR(7)    PRIMARY KEY,  -- CPT/HCPCS code
    description         TEXT,
    short_description   VARCHAR(50),
    -- Classification
    category            VARCHAR(30),                -- E&M, Surgery, Radiology, Lab, etc.
    subcategory         VARCHAR(50),
    betos_code          VARCHAR(5),
    is_em_code          BOOLEAN       DEFAULT FALSE,
    em_category         VARCHAR(20),                -- primary_care, obgyn, mh_sud (NULL if not E/M)
    -- Medicare RVU components (CY2026 PFS)
    work_rvu            DECIMAL(8,4),
    pe_rvu_facility     DECIMAL(8,4),
    pe_rvu_nonfacility  DECIMAL(8,4),
    mp_rvu              DECIMAL(8,4),
    total_rvu_facility  DECIMAL(8,4),
    total_rvu_nonfac    DECIMAL(8,4),
    -- Code metadata
    is_add_on           BOOLEAN       DEFAULT FALSE,
    global_days         VARCHAR(4),
    pc_tc_indicator     VARCHAR(2),                 -- 0=physician, 1=technical, 2=professional, 9=N/A
    status_indicator    VARCHAR(2),
    -- Medicare national rate (no locality adjustment)
    medicare_rate_nonfac DECIMAL(10,2),             -- work*1 + pe_nf*1 + mp*1 × CF
    medicare_rate_fac    DECIMAL(10,2),
    conversion_factor    DECIMAL(8,4),              -- $33.4009 for CY2026 non-QPP
    -- Metadata
    pfs_year            INTEGER,                    -- 2026
    last_updated        DATE
);
```

**Partitioning:** None (~17K rows, single file).
**File:** `dimension/dim_procedure.parquet`

**Source mapping:**
- `procedure_codes` (SQLite) -> Medicare PFS data
- `dim_procedure` (CPRA DuckDB) -> same source
- `dim_em_447_codes` (CPRA DuckDB) -> `is_em_code` + `em_category` columns
- `hcpcs_codes` (NCCI scraper) -> `description`, `short_description`, code metadata

---

#### dim_medicare_locality

Medicare GPCI values by geographic locality. Needed for locality-adjusted rate calculations.

```sql
CREATE TABLE dim_medicare_locality (
    locality_id         INTEGER       PRIMARY KEY,
    mac_locality        VARCHAR(10)   NOT NULL,
    locality_name       VARCHAR(200),
    state_code          VARCHAR(2),                 -- FK to dim_state
    gpci_work           DECIMAL(8,4),
    gpci_pe             DECIMAL(8,4),
    gpci_mp             DECIMAL(8,4),
    -- Pre-computed weight for state-level aggregation
    state_weight        DECIMAL(8,6),               -- 1/COUNT(localities in state)
    pfs_year            INTEGER,
    last_updated        DATE
);
```

**Partitioning:** None (109 rows).
**File:** `dimension/dim_medicare_locality.parquet`

---

#### dim_provider_taxonomy

Provider type classification from NUCC taxonomy.

```sql
CREATE TABLE dim_provider_taxonomy (
    taxonomy_code       VARCHAR(20)   PRIMARY KEY,
    classification      VARCHAR(200),
    specialization      VARCHAR(200),
    category            VARCHAR(50),                -- physician, facility, non_physician, supplier
    display_name        VARCHAR(100),               -- human-readable short name
    last_updated        DATE
);
```

**Partitioning:** None (~800 rows).
**File:** `dimension/dim_provider_taxonomy.parquet`

---

#### dim_time

Date dimension for consistent temporal joins.

```sql
CREATE TABLE dim_time (
    date_key            INTEGER       PRIMARY KEY,  -- YYYYMMDD
    full_date           DATE          NOT NULL,
    year                INTEGER,
    quarter             INTEGER,                    -- 1-4
    month               INTEGER,                    -- 1-12
    month_name          VARCHAR(10),
    fiscal_year         INTEGER,                    -- federal FY (Oct-Sep)
    fiscal_quarter      INTEGER,
    is_month_start      BOOLEAN,
    is_quarter_start    BOOLEAN,
    is_fy_start         BOOLEAN
);
```

**Partitioning:** None (~3,650 rows for 10 years).
**File:** `dimension/dim_time.parquet`

---

### 4.2 Fact Tables

All facts live in `s3://aradune-datalake/fact/` (or `data/lake/fact/` locally).

#### fact_medicaid_rate

State Medicaid fee schedule rates. The core "what does Medicaid pay" table.

```sql
CREATE TABLE fact_medicaid_rate (
    state_code          VARCHAR(2)    NOT NULL,     -- FK to dim_state
    procedure_code      VARCHAR(7)    NOT NULL,     -- FK to dim_procedure
    modifier            VARCHAR(4)    DEFAULT '',
    -- Rate values
    rate                DECIMAL(10,2),              -- primary rate (generic/single)
    rate_facility       DECIMAL(10,2),
    rate_nonfacility    DECIMAL(10,2),
    -- Effective period (SCD Type 2)
    effective_date      DATE          NOT NULL,
    end_date            DATE,                       -- NULL = currently active
    -- Metadata
    billing_unit        VARCHAR(20),
    place_of_service    VARCHAR(4),
    prior_auth          BOOLEAN,
    source_file         TEXT,
    -- Pipeline metadata
    snapshot_date       DATE          NOT NULL,     -- when this data was captured
    pipeline_run_id     VARCHAR(36)                 -- FK to pipeline_run
);
```

**Partitioning:** `state_code` / `snapshot_date`
```
fact/medicaid_rate/state=FL/snapshot=2026-03-05/*.parquet
fact/medicaid_rate/state=CA/snapshot=2026-03-05/*.parquet
```

**Source:** SQLite `rates` table (mfs_scraper) + state-specific fee schedule parsers.
**Estimated rows:** ~500K (51 states x ~10K codes avg, active rates only)

---

#### fact_medicare_rate

Medicare rates by locality. Pre-computed from RVU components x GPCI x CF.

```sql
CREATE TABLE fact_medicare_rate (
    procedure_code      VARCHAR(7)    NOT NULL,     -- FK to dim_procedure
    locality_id         INTEGER       NOT NULL,     -- FK to dim_medicare_locality
    state_code          VARCHAR(2)    NOT NULL,     -- FK to dim_state (denormalized)
    -- Computed rates
    nonfac_rate         DECIMAL(10,2),
    fac_rate            DECIMAL(10,2),
    -- Component values (for audit)
    work_rvu            DECIMAL(8,4),
    pe_rvu_nonfac       DECIMAL(8,4),
    pe_rvu_fac          DECIMAL(8,4),
    mp_rvu              DECIMAL(8,4),
    gpci_work           DECIMAL(8,4),
    gpci_pe             DECIMAL(8,4),
    gpci_mp             DECIMAL(8,4),
    conversion_factor   DECIMAL(8,4),               -- $33.4009
    -- Metadata
    pfs_year            INTEGER,
    snapshot_date       DATE          NOT NULL
);
```

**Partitioning:** `pfs_year`
```
fact/medicare_rate/pfs_year=2026/*.parquet
```

**Derived view:** `fact_medicare_rate_state` (weighted average across localities per state) materialized as a separate Parquet file for fast joins.

**Source:** CPRA engine `--medicare-rates` step.
**Rows:** ~860K (locality) / ~420K (state-level)

---

#### fact_rate_comparison

Medicaid vs Medicare rate comparison. The core CPRA output.

```sql
CREATE TABLE fact_rate_comparison (
    state_code          VARCHAR(2)    NOT NULL,
    procedure_code      VARCHAR(7)    NOT NULL,
    modifier            VARCHAR(4)    DEFAULT '',
    -- Rate comparison
    medicaid_rate       DECIMAL(10,2),
    medicare_nonfac_rate DECIMAL(10,2),
    medicare_fac_rate   DECIMAL(10,2),
    pct_of_medicare     DECIMAL(7,2),               -- (medicaid / medicare_nonfac) x 100
    -- Classification (denormalized for fast filtering)
    em_category         VARCHAR(20),                -- primary_care, obgyn, mh_sud, NULL
    category            VARCHAR(30),                -- E&M, Surgery, etc.
    -- T-MSIS volume context (joined from claims)
    claim_count         INTEGER,                    -- CY2023 FFS claims for this code+state
    beneficiary_count   INTEGER,
    total_paid          DECIMAL(15,2),              -- actual Medicaid spending
    -- Metadata
    medicaid_rate_date  DATE,
    comparison_year     INTEGER,                    -- CY of comparison
    snapshot_date       DATE          NOT NULL,
    pipeline_run_id     VARCHAR(36)
);
```

**Partitioning:** `comparison_year`
```
fact/rate_comparison/comparison_year=2025/*.parquet
```

**Source:** CPRA engine `--cpra` step, enriched with T-MSIS claim volumes.
**Rows:** ~242K (all codes) / ~2,742 (E/M subset for frontend)

---

#### fact_claims

T-MSIS claims aggregated by state, code, and time period. This is what the browser queries via DuckDB-WASM.

```sql
CREATE TABLE fact_claims (
    state_code          VARCHAR(2)    NOT NULL,
    procedure_code      VARCHAR(7)    NOT NULL,
    category            VARCHAR(30),
    -- Time
    year                INTEGER       NOT NULL,
    month               INTEGER,                    -- NULL for yearly aggregation
    -- Measures
    total_paid          DECIMAL(15,2),
    total_claims        INTEGER,
    total_beneficiaries INTEGER,
    avg_paid_per_claim  DECIMAL(10,2),              -- total_paid / total_claims
    -- Metadata
    claim_type          VARCHAR(10)   DEFAULT 'FFS', -- FFS, encounter, all
    snapshot_date       DATE          NOT NULL,
    pipeline_run_id     VARCHAR(36)
);
```

**Partitioning:** `year` / `state_code`
```
fact/claims/year=2024/state=FL/*.parquet
fact/claims/year=2024/state=CA/*.parquet
```

**Source:** R pipeline (`tmsis_pipeline_duckdb.R`).
**Rows:** ~713K (yearly) / ~6.3M (monthly)

---

#### fact_provider

Provider-level aggregates from T-MSIS + NPPES enrichment.

```sql
CREATE TABLE fact_provider (
    npi                 VARCHAR(10)   NOT NULL,
    state_code          VARCHAR(2)    NOT NULL,
    -- Provider info (from NPPES)
    provider_name       VARCHAR(200),
    zip3                VARCHAR(3),
    taxonomy_code       VARCHAR(20),                -- FK to dim_provider_taxonomy
    specialty           VARCHAR(100),
    -- Measures (aggregated from claims)
    total_paid          DECIMAL(15,2),
    total_claims        INTEGER,
    total_beneficiaries INTEGER,
    code_count          INTEGER,                    -- distinct HCPCS codes billed
    -- Period
    year                INTEGER       NOT NULL,
    -- Metadata
    snapshot_date       DATE          NOT NULL,
    pipeline_run_id     VARCHAR(36)
);
```

**Partitioning:** `year` / `state_code`
```
fact/provider/year=2024/state=FL/*.parquet
```

**Source:** R pipeline + NPPES enrichment (currently in `bootstrap_db.py`).
**Rows:** ~584K

---

#### fact_enrollment

Medicaid enrollment by state and month.

```sql
CREATE TABLE fact_enrollment (
    state_code          VARCHAR(2)    NOT NULL,
    year                INTEGER       NOT NULL,
    month               INTEGER,
    -- Enrollment counts
    total_enrollment    INTEGER,
    chip_enrollment     INTEGER,
    ffs_enrollment      INTEGER,
    mc_enrollment       INTEGER,
    -- Metadata
    source              VARCHAR(50),                -- medicaid.gov, KFF, etc.
    snapshot_date       DATE          NOT NULL
);
```

**Partitioning:** `year`
**Source:** `cms_data.py` FMAP/enrollment scraper.
**Rows:** ~3K

---

#### fact_quality_measure

CMS Core Set quality measures by state and year.

```sql
CREATE TABLE fact_quality_measure (
    state_code          VARCHAR(2)    NOT NULL,
    measure_id          VARCHAR(50)   NOT NULL,
    year                INTEGER       NOT NULL,
    -- Measure values
    rate                DECIMAL(10,4),
    numerator           INTEGER,
    denominator         INTEGER,
    -- Context
    measure_name        VARCHAR(200),
    domain              VARCHAR(50),
    national_median     DECIMAL(10,4),
    percentile_rank     INTEGER,
    -- Metadata
    source              VARCHAR(50),
    snapshot_date       DATE          NOT NULL
);
```

**Partitioning:** `year`
**Source:** `cms_data.py` quality scraper.
**Rows:** ~2.5K

---

#### fact_expenditure

CMS-64 federal/state Medicaid expenditures.

```sql
CREATE TABLE fact_expenditure (
    state_code          VARCHAR(2)    NOT NULL,
    fiscal_year         INTEGER       NOT NULL,
    quarter             INTEGER,
    category            VARCHAR(50),                -- inpatient, outpatient, physician, drugs, etc.
    subcategory         VARCHAR(100),
    -- Amounts
    federal_share       DECIMAL(15,2),
    total_computable    DECIMAL(15,2),
    -- Metadata
    source              VARCHAR(50),
    snapshot_date       DATE          NOT NULL
);
```

**Partitioning:** `fiscal_year`
**Source:** `cms_data.py` CMS-64 scraper.
**Rows:** ~1K

---

#### fact_dq_flag

Data quality flags generated by validation rules.

```sql
CREATE TABLE fact_dq_flag (
    state_code          VARCHAR(2)    NOT NULL,
    entity_type         VARCHAR(20)   NOT NULL,     -- procedure, state, provider
    entity_id           VARCHAR(20),                -- procedure_code, '*' for state-level
    -- Flag details
    flag_type           VARCHAR(30)   NOT NULL,     -- BELOW_50PCT, STALE_RATE, MISSING_MEDICARE, etc.
    severity            VARCHAR(10)   NOT NULL,     -- info, warning, error
    detail              TEXT,
    -- Scope
    source_pipeline     VARCHAR(30),                -- cpra, tmsis, cms
    -- Metadata
    snapshot_date       DATE          NOT NULL,
    pipeline_run_id     VARCHAR(36)
);
```

**Partitioning:** `source_pipeline`
**Source:** All pipelines generate DQ flags.
**Rows:** ~260K (CPRA) + future T-MSIS flags

---

### 4.3 Reference Tables

Reference data that doesn't change often. Lives in `s3://aradune-datalake/reference/`.

```
reference/
  bls_wages.parquet              -- BLS OEWS wage data (~50 occupations)
  soc_hcpcs_crosswalk.parquet    -- SOC code <-> HCPCS code mapping
  ncci_edits.parquet             -- NCCI PTP/MUE edits (~2.5M pairs)
  hcpcs_descriptions.parquet     -- HCPCS Level II descriptions (~8.6K codes)
  nadac_pricing.parquet          -- Drug pricing (~900K NDCs)
  drug_utilization.parquet       -- SDUD quarterly claims (~2.5M rows)
  drug_rebate_products.parquet   -- Rebate program products (~100K)
  section_1115_waivers.parquet   -- Active waivers (~647)
  dsh_payments.parquet           -- DSH hospital payments (~1K)
  fee_schedule_directory.parquet -- State FS URLs and metadata (51 states)
  gpci.parquet                   -- alias for dim_medicare_locality
```

---

### 4.4 Pipeline Metadata

```sql
CREATE TABLE pipeline_run (
    run_id              VARCHAR(36)   PRIMARY KEY,  -- UUID
    pipeline_name       VARCHAR(50)   NOT NULL,     -- cpra, tmsis, cms_enrollment, etc.
    started_at          TIMESTAMP     NOT NULL,
    completed_at        TIMESTAMP,
    status              VARCHAR(20),                -- running, success, failed, partial
    rows_produced       INTEGER,
    rows_rejected       INTEGER,
    dq_flags_generated  INTEGER,
    version_label       VARCHAR(50),                -- git hash or manual version
    config              TEXT,                        -- JSON of pipeline parameters
    error_message       TEXT
);
```

**File:** `metadata/pipeline_runs.parquet` (append-only log)

---

## 5. Directory Layout

### Development (local disk)

```
data/
  lake/
    dimension/
      dim_state.parquet
      dim_procedure.parquet
      dim_medicare_locality.parquet
      dim_provider_taxonomy.parquet
      dim_time.parquet
    fact/
      medicaid_rate/
        state=FL/snapshot=2026-03-05/part-0.parquet
        state=CA/snapshot=2026-03-05/part-0.parquet
      medicare_rate/
        pfs_year=2026/part-0.parquet
      rate_comparison/
        comparison_year=2025/part-0.parquet
      claims/
        year=2024/state=FL/part-0.parquet
        year=2024/state=CA/part-0.parquet
      provider/
        year=2024/state=FL/part-0.parquet
      enrollment/
        year=2025/part-0.parquet
      quality_measure/
        year=2024/part-0.parquet
      expenditure/
        fiscal_year=2025/part-0.parquet
      dq_flag/
        source_pipeline=cpra/part-0.parquet
        source_pipeline=tmsis/part-0.parquet
    reference/
      bls_wages.parquet
      ncci_edits.parquet
      ... (see 4.3)
    metadata/
      pipeline_runs.parquet
    snapshots/
      2026-03-05T14:00:00Z/
        manifest.json        -- what was produced, row counts, checksums
        dim_state.parquet    -- frozen copy
        ... (full snapshot)

  raw/                        -- untouched source files (existing)
    medicaid-provider-spending.csv
    npidata_pfile_*.csv
    dme26a/
    NPPES_Data_Dissemination_February_2026/
    ...
```

### Production (S3)

Same layout under `s3://aradune-datalake/`. Frontend reads from CDN-cached copies in `public/data/`.

---

## 6. Frontend Export Layer

The browser cannot query the full data lake. Pipelines produce **optimized exports** for the frontend:

| Frontend File | Source | Rows | Purpose |
|---------------|--------|------|---------|
| `cpra_em.json` | `fact_rate_comparison` WHERE `em_category IS NOT NULL` | ~2,742 | CPRA Generator |
| `cpra_summary.json` | Aggregated from `fact_rate_comparison` | ~38 | CPRA overview |
| `dq_flags_em.json` | `fact_dq_flag` WHERE E/M scope | ~771 | CPRA DQ panel |
| `dim_447_codes.json` | `dim_procedure` WHERE `is_em_code = TRUE` | 74 | CPRA code list |
| `claims.parquet` | `fact_claims` grouped yearly | ~713K | DuckDB-WASM Spending Explorer |
| `claims_monthly.parquet` | `fact_claims` with month | ~6.3M | DuckDB-WASM monthly queries |
| `categories.parquet` | `fact_claims` grouped by category | ~8.1K | DuckDB-WASM fast rollups |
| `providers.parquet` | `fact_provider` | ~584K | DuckDB-WASM provider search |
| `hcpcs.json` | `dim_procedure` + `fact_claims` aggregates | ~2K | Explorer code reference |
| `states.json` | `dim_state` + `fact_claims` aggregates | 51 | Explorer state overview |
| `fee_schedules.json` | `fact_medicaid_rate` latest snapshot | ~50K | Fee Schedule Directory |
| `medicare_rates.json` | `fact_medicare_rate` state-level | ~10K | Rate comparisons |
| `bls_wages.json` | `reference/bls_wages.parquet` | ~50 | Wage Adequacy tool |
| `quality_measures.json` | `fact_quality_measure` latest year | ~60 | Quality Linkage tool |

**Export script:** `scripts/export_frontend.py` (to be built) reads from the lake, validates, and writes to `public/data/`.

---

## 7. Dagster Pipeline Design

### 7.1 Pipeline Graph

```
@asset: dim_state           -- from SQLite states table + CMS enrollment + FMAP
@asset: dim_procedure       -- from Medicare PFS RVU file + NCCI HCPCS
@asset: dim_medicare_locality -- from GPCI file
@asset: dim_provider_taxonomy -- from NUCC taxonomy
@asset: dim_time            -- generated (2014-2030)

@asset: fact_medicaid_rate  -- from SQLite rates table, depends on dim_state, dim_procedure
@asset: fact_medicare_rate  -- from RVU x GPCI x CF, depends on dim_procedure, dim_medicare_locality
@asset: fact_rate_comparison -- from fact_medicaid_rate JOIN fact_medicare_rate, depends on both
@asset: fact_claims         -- from T-MSIS CSV, depends on dim_state, dim_procedure
@asset: fact_provider       -- from T-MSIS + NPPES, depends on fact_claims
@asset: fact_enrollment     -- from CMS enrollment API
@asset: fact_quality_measure -- from CMS Core Set
@asset: fact_expenditure    -- from CMS-64

@asset: fact_dq_flag        -- runs validation rules on ALL fact tables

@asset: export_frontend     -- reads validated lake, writes public/data/
```

### 7.2 Schedules

| Pipeline | Trigger | Frequency | Notes |
|----------|---------|-----------|-------|
| dim_* | Manual / on source update | As needed | Dimensions rarely change |
| fact_medicaid_rate | State fee schedule update | Per state, as published | Some states quarterly, most annual |
| fact_medicare_rate | CMS PFS release | Annual (January) | CY2026 PFS |
| fact_rate_comparison | After either rate table updates | After upstream | Always rebuilds full comparison |
| fact_claims | T-MSIS release | Quarterly / on new data | Next: when R pipeline runs |
| fact_enrollment | medicaid.gov update | Monthly | Lag: ~2 months |
| fact_quality_measure | CMS Core Set release | Annual | ~November |
| fact_dq_flag | After ANY fact table update | After upstream | Quality gate |
| export_frontend | After dq passes | After validation | Deploys to public/data/ |

### 7.3 Quality Gates

Every pipeline run must pass these checks before export_frontend promotes data:

```python
# Gate 1: Row count sanity
assert new_rows >= 0.9 * previous_rows, "Row count dropped >10%"

# Gate 2: State coverage
assert len(states_with_data) >= 34, "Lost state coverage"

# Gate 3: Value bounds
assert fact_rate_comparison.pct_of_medicare.median() > 50, "Median MCR implausibly low"
assert fact_rate_comparison.pct_of_medicare.median() < 200, "Median MCR implausibly high"

# Gate 4: No null keys
assert fact_rate_comparison.state_code.notnull().all(), "Null state codes"
assert fact_rate_comparison.procedure_code.notnull().all(), "Null procedure codes"

# Gate 5: Conversion factor check
assert abs(medicare_cf - 33.4009) < 0.01, "Medicare CF changed — verify"

# Gate 6: Snapshot exists
assert snapshot_manifest_written, "No snapshot manifest"
```

If any gate fails: pipeline status = `failed`, no export, alert to operator.

---

## 8. Migration Plan

### Phase 1: Shared dimensions + local lake (this session)
1. Create `data/lake/` directory structure
2. Build `scripts/build_dimensions.py` — reads existing SQLite + CPRA DuckDB, writes unified dimension Parquet files
3. Modify `cpra_engine.py` to read dimensions from `data/lake/dimension/` and write facts to `data/lake/fact/`
4. Modify `export_frontend.py` to read from lake and produce `public/data/` files
5. Validate: full round-trip from raw data to frontend JSON

### Phase 2: T-MSIS integration
1. Port R pipeline to Python (or wrap R script in Python orchestrator)
2. Write T-MSIS output to `data/lake/fact/claims/` and `data/lake/fact/provider/`
3. Enrich `fact_rate_comparison` with claim volumes from `fact_claims`
4. Run full pipeline: dimensions -> CPRA -> T-MSIS -> comparison -> export

### Phase 3: Dagster orchestration
1. Define Dagster assets for each dimension and fact table
2. Implement quality gates as Dagster checks
3. Implement schedules for recurring data sources
4. Deploy Dagster locally (single-process mode)

### Phase 4: S3 + FastAPI
1. Sync `data/lake/` to S3 with immutable snapshots
2. Wire FastAPI to read from S3/local DuckDB
3. Replace static JSON serving with API endpoints
4. Keep DuckDB-WASM for browser queries (no change to frontend)

### Phase 5: Production automation
1. CI/CD: push to main triggers pipeline validation
2. Dagster sensors watch for new source data (CMS releases, state fee schedule updates)
3. Alerting on pipeline failures and DQ regressions
4. Dashboard for pipeline health and data freshness

---

## 9. Naming Conventions

| Entity | Convention | Example |
|--------|-----------|---------|
| Dimension tables | `dim_<entity>` | `dim_state`, `dim_procedure` |
| Fact tables | `fact_<measurement>` | `fact_claims`, `fact_medicaid_rate` |
| Reference tables | `ref_<dataset>` or filename | `bls_wages`, `ncci_edits` |
| Parquet partition keys | `key=value` (Hive style) | `state=FL`, `year=2024` |
| Snapshot dirs | ISO 8601 | `2026-03-05T14:00:00Z` |
| Pipeline run IDs | UUID v4 | `a1b2c3d4-...` |
| Column names | `snake_case` | `procedure_code`, `pct_of_medicare` |
| Date columns | `_date` suffix for dates, `_at` for timestamps | `effective_date`, `created_at` |
| Amount columns | `_rate`, `_paid`, `_amount` | `medicaid_rate`, `total_paid` |
| Count columns | `_count`, `total_` prefix | `claim_count`, `total_enrollment` |
| Boolean columns | `is_`, `has_` prefix | `is_em_code`, `has_upl_supplement` |

---

## 10. Key Decisions & Rationale

| Decision | Choice | Why |
|----------|--------|-----|
| DuckDB over Postgres | DuckDB | Columnar analytics, Parquet-native, zero-ops, runs in browser via WASM |
| Parquet over CSV/JSON | Parquet | Columnar compression, schema enforcement, partition pruning, DuckDB reads natively |
| Hive partitioning | state + year/snapshot | Most queries filter by state and time; partition pruning eliminates I/O |
| Immutable snapshots | Yes | Bad data never overwrites good data; easy rollback; audit trail |
| SCD Type 2 for rates | effective_date + end_date | Fee schedules change over time; need to join claims to correct rate period |
| Denormalized em_category | In fact_rate_comparison | Avoids join to dim_procedure on every CPRA query; 74 codes rarely change |
| Separate monthly/yearly claims | Two Parquet files | Yearly (713K rows) loads fast in browser; monthly (6.3M) is optional/external |
| Frontend exports as separate step | export_frontend.py | Decouples lake schema from frontend needs; validates before publish |
| $33.4009 CF hardcoded | Non-QPP 2026 | Per 42 CFR 447.203 CPRA rules; update annually with CMS PFS release |
