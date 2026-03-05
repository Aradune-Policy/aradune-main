# Aradune: Product Strategy & Medicaid Data Landscape
> **Guiding document for Claude Code** — Use this as primary context when building any Aradune feature, data pipeline, schema, or analytical module. This document describes what Aradune is, what it's trying to become, what data it works with, who it serves, and how it should be built.

---

## 1. What Aradune Is

Aradune (aradune.co) is a **Medicaid policy transparency and intelligence platform** — the goal is to become the Bloomberg Terminal of Medicaid. It aggregates public and DUA-accessible Medicaid data, connects dots across datasets that no individual analyst can connect manually, and delivers structured analytics and AI-powered policy intelligence to every major Medicaid stakeholder.

**Current stack:**
- React frontend
- DuckDB backend (server-side and browser-side via DuckDB-WASM)
- Python data pipelines (pdfplumber for PDF extraction)
- 227M rows of T-MSIS claims data already ingested
- State Medicaid fee schedules ingested via PDF extraction across 18+ states (target: 51)
- Hive-partitioned Parquet files on S3 for large datasets
- Deployed at aradune.co

**Current features:**
- Wage-adequacy analysis
- MCO gap analysis
- Quality ↔ rate linkage
- Rate decay visualization
- Border arbitrage analysis
- Reverse cash flows
- Fee schedule lookups and dashboards
- Comparative rate analysis (Medicaid vs. Medicare)

**Tier structure:**
- Free tier: lookups, basic dashboards, limited exports
- Paid AI tier ($99–299/month): unlimited AI queries, CPRA auto-generation, SPA tracking, API access, custom reports

---

## 2. The Regulatory Forcing Function: 42 CFR 447.203

### What it is
The **CMS Ensuring Access to Medicaid Services Final Rule (CMS-2442-F)**, published May 10, 2024, created a hard **July 1, 2026 deadline** requiring every state to publish:

1. **All Medicaid FFS fee schedule payment rates** on a publicly accessible website, organized by CPT/HCPCS code, provider type, population (pediatric/adult), and geography. Must be updated within **30 days** of any rate change.

2. **Comparative Payment Rate Analysis (CPRA)** — for three service categories (primary care, OB/GYN, outpatient MH/SUD), express every Medicaid base FFS rate as a **percentage of the Medicare non-facility rate** for the same code. Must include claim counts and beneficiary counts. Updated biennially (next due July 2028).

3. **HCBS payment rate disclosure** — for personal care, home health aide, homemaker, and habilitation services: convert all rates to **average hourly equivalents** for cross-state comparability. No Medicare comparable exists for HCBS — the disclosure is designed for state-to-state benchmarking instead.

4. **Interested Parties Advisory Group (IPAG)** — advises on HCBS rates. First meeting deadline: **January 1, 2029** (enforcement discretion extended Dec 2025).

### Rate reduction SPA procedures (already in effect since July 9, 2024)
For any SPA reducing or restructuring rates that could diminish access, states must demonstrate:
- Aggregate Medicaid rates (base + supplemental) ≥ **80% of Medicare** for each affected benefit category
- Rate reduction + all other reductions in the state fiscal year ≤ **4% of aggregate FFS expenditures**
- Public process produced no significant unresolved access concerns

If any condition fails (Tier 2), states must submit extensive documentation including 3-year provider participation trends, 3-year utilization trends, and forward-looking impact estimates.

### Compliance status
- **No legal challenge, no injunction, no proposed delay** as of March 2026
- Rule survived the Trump administration regulatory freeze (was already effective July 9, 2024)
- The One Big Beautiful Bill Act (July 4, 2025) imposed moratoriums on other Medicaid rules but **explicitly excluded CMS-2442-F**
- CMS retains authority to **defer FFP** (federal matching payments) for noncompliant states
- This is real, enforceable, and the July 2026 deadline has not moved

### Why this matters for Aradune
This rule creates:
- **Immediate, universal, penalty-backed demand** for CPRA automation tools
- **The first standardized, public, cross-state Medicaid fee schedule dataset** (after July 2026)
- A go-to-market forcing function: states are actively looking for compliance help RIGHT NOW
- Aradune's CPRA auto-generation feature is the wedge product into every other capability

---

## 3. The Full Medicaid Data Landscape

### Tier 1: Core claims and enrollment data (T-MSIS TAF)

| Dataset | Description | Access | Format | Update Cadence |
|---|---|---|---|---|
| T-MSIS TAF (DE) | Demographic & eligibility for 90M+ beneficiaries | DUA via ResDAC | SAS/Parquet | Quarterly |
| T-MSIS TAF (OT) | Other services claims (physician, outpatient, clinic) | DUA via ResDAC | SAS/Parquet | Quarterly |
| T-MSIS TAF (IP) | Inpatient hospital claims | DUA via ResDAC | SAS/Parquet | Quarterly |
| T-MSIS TAF (RX) | Pharmacy claims | DUA via ResDAC | SAS/Parquet | Quarterly |
| T-MSIS TAF (LT) | Long-term care claims | DUA via ResDAC | SAS/Parquet | Quarterly |
| T-MSIS TAF (APL) | Plan participation/managed care enrollment | DUA via ResDAC | SAS/Parquet | Quarterly |
| T-MSIS TAF (APR) | Annual provider file | DUA via ResDAC | SAS/Parquet | Annual |

**Access mechanism:** ResDAC DUA (resdac.org). VRDC (cloud environment): ~$35K first year, ~$23K renewal. Physical extract: $1K–$5K per file per year. For-profit entities must use VRDC. DUA approval: 6–8+ months.

**T-MSIS Data Quality Atlas:** free, public, interactive — medicaid.gov/dq-atlas. Review this before relying on any state's data for a given topic. Significant state-level variation in data quality, especially for encounter data completeness and race/ethnicity.

### Tier 2: Publicly available CMS datasets (no DUA required)

| Dataset | URL | Description | Format | Cadence |
|---|---|---|---|---|
| State Drug Utilization Data (SDUD) | data.medicaid.gov | State × quarter × NDC × units × amounts (pre-rebate) | CSV + API | Quarterly |
| MBES/CBES Expenditure Reports | medicaid.gov | State-level FFS + MCO expenditures and enrollment counts | Excel | Quarterly |
| MACStats | macpac.gov/macstats | 40+ exhibits: enrollment, spending, FMAP, supplemental payments | PDF + Excel | Annual (Feb) |
| Managed Care MLR Reports | medicaid.gov | Per-MCO MLR %, numerator, denominator, remittance | PDF | Annual |
| MCPAR (Managed Care Program Annual Reports) | medicaid.gov | Plan-level enrollment, network adequacy, grievances, quality | PDF | Annual |
| Managed Care Enrollment Report | data.medicaid.gov | Plan-specific enrollment by entity type and geography | CSV + API | Monthly |
| 1115/1915 Waiver List | medicaid.gov | All current and concluded waivers with documents | Web/PDF | Continuous |
| Adult/Child Core Set Quality Measures | medicaid.gov | State-level quality measures for Medicaid MCOs | Web/Excel | Annual |
| Medicare PFS RVU Files | cms.gov/medicare/payment/fee-schedules/physician/pfs-relative-value-files | Work/PE/MP RVUs, GPCIs, status indicators for all CPT/HCPCS codes | ZIP/CSV | Annual + quarterly |
| Medicare PFS National Payment Amounts | cms.gov (PFALLyyA.ZIP) | Pre-calculated Medicare payment rates by locality | ZIP/CSV | Annual + quarterly |
| CMS Open Payments | openpaymentsdata.cms.gov | Manufacturer payments to physicians — 16M+ records/year | CSV + API | Annual |
| NPPES NPI Registry | download.cms.gov/nppes/NPI_Files.html | Full provider directory with taxonomy, address, name | ZIP/CSV + API | Weekly |
| Provider of Services (POS) File | cms.gov | Facility characteristics: bed size, ownership, CCN | CSV | Quarterly |
| Medicare Cost Reports (HCRIS) | cms.gov/data-research/statistics-trends-and-reports/cost-reports | Hospital/SNF/HH/hospice cost reports — DSH, cost-to-charge, uncompensated care | CSV | Quarterly (lags 2–4 yrs) |
| DSH Audits & Allotments | medicaid.gov | Hospital-level DSH limits, utilization rates, total payments | PDF | Annual (3-yr lag) |
| KFF State Health Facts | kff.org/statedata | 800+ indicators: enrollment, FMAP, managed care, fee index | Web/CSV | Varies |

### Tier 3: State-published data (heterogeneous, requires scraping)

Every state publishes Medicaid data on its own website with no standardization. Common sources:
- **Fee schedules**: PDF, Excel, CSV, or searchable web portals. ~60% machine-readable, ~30% PDF, ~10% scrape-only.
- **Managed care contracts**: Usually public PDF, some states post online. Contain capitation rates, quality requirements, and network standards.
- **Rate certifications**: Actuarial certification letters for MCO capitation rates. Posted publicly in many states.
- **Annual reports**: Program statistics, enrollment trends, spending summaries.
- **HCBS waiver rate tables**: States post 1915(c) waiver rate schedules, often by service category and geographic area.

Key state-specific data sources worth prioritizing:
- **New York SPARCS** (health.data.ny.gov): All-payer hospital discharge data with API
- **Texas TMHP** (public.tmhp.com/FeeSchedules): Public fee schedule lookup
- **California Medi-Cal**: Rates portal updated monthly
- **Colorado CIVHC APCD** (civhc.org): All-payer claims including Medicaid

### Tier 4: Supplementary population health and provider data

| Dataset | URL | Description |
|---|---|---|
| AHRQ HCUP SID/NIS | ahrq.gov/data/hcup | All-payer hospital discharge data with Medicaid flag. DUA + fee ($200–$4K). |
| HRSA UDS | data.hrsa.gov | FQHC patient/payer mix, services, quality measures |
| HRSA AHRF | hrsa.gov | 6,000+ county-level health professions and facility variables |
| HRSA HPSA designations | hrsa.gov | Health Professional Shortage Areas — trigger enhanced Medicaid rates |
| Hospital Price Transparency MRFs | Individual hospital websites | Medicaid MCO negotiated rates — unprecedented but not centrally aggregated |
| State APCDs | ~21 states | All-payer claims databases including Medicaid |

---

## 4. The CPRA Pipeline: Core Technical Specification

The Comparative Payment Rate Analysis is Aradune's wedge product. Here is the full pipeline specification:

### Step 1: Ingest the CMS CY 2025 E/M Code List
- Source: `medicaid.gov/medicaid/access-care/downloads/comp-pay-rate-analysis-cy-2025-em-code-list.pdf`
- This is the **authoritative list of CPT/HCPCS codes** subject to the 447.203 CPRA requirement
- Maps each code to its service category: `primary_care`, `obgyn`, or `mh_sud`
- Parse and store as a lookup table: `dim_447_code_list(cpt_hcpcs_code, category_447, description)`

### Step 2: Ingest Medicare PFS non-facility rates
- Source: `cms.gov/medicare/payment/fee-schedules/physician/pfs-relative-value-files`
- Download annual RVU ZIP (e.g., RVU26A.ZIP) and GPCI file
- Non-facility payment rate formula:
  ```
  rate = (Work_RVU × Work_GPCI + NonFac_PE_RVU × PE_GPCI + MP_RVU × MP_GPCI) × CF
  ```
- CY 2026 conversion factors: **$33.57** (QP) / **$33.40** (non-QP). Use $33.40 as the benchmark.
- Pre-calculated national amounts available in `PFALLyyA.ZIP` — use as validation check
- 109 Medicare localities; county-to-locality crosswalk at `cms.gov/medicare/payment/fee-schedules/physician-fee-schedule/locality-key`
- PFS API available at `pfs.data.cms.gov`

### Step 3: Ingest state Medicaid fee schedule
- Normalize to standard schema: `(state_code, cpt_hcpcs_code, modifier, provider_type, population, geographic_area, effective_date, rate, source_file, last_updated)`
- Three-tier extraction approach:
  - **Tier 1** (~60% of states): direct CSV/Excel ingestion
  - **Tier 2** (~30%): pdfplumber with per-state custom parsing
  - **Tier 3** (~10%): OCR + LLM-assisted extraction for scanned PDFs

### Step 4: Match Medicaid rates to Medicare rates
- Join on: `cpt_hcpcs_code`, Medicare locality (mapped from state geographic area), modifier
- For multi-locality states: use population-weighted average Medicare rate
- Apply modifier logic crosswalk (state-specific adjustments)

### Step 5: Calculate and validate
- `pct_of_medicare = medicaid_rate / medicare_nonfac_rate`
- Append claim counts and beneficiary counts from T-MSIS OT file
- Data quality validation gates (see Section 6)

### Step 6: Generate compliant CPRA output
- Organized by service category (primary_care, obgyn, mh_sud)
- Required columns: CPT/HCPCS code, Medicaid base rate, Medicare non-facility rate, Medicaid as % of Medicare, claim count, beneficiary count
- Segmented by population (pediatric/adult), provider type, geographic area where rates vary
- Output formats: PDF (branded report), Excel workbook, HTML table (for state website publication)

### CPRA compliance rules
- **Base rates only** (not supplemental) for the percentage calculation
- **Non-facility Medicare rate** is the benchmark (not facility)
- Data lag: compare Medicaid rates effective **July 1, 2025** to **CY 2025 Medicare PFS** rates
- Must be published by **July 1, 2026**; updated biennially (July 1, 2028 next)

---

## 5. DuckDB Schema

```sql
-- Dimension tables
CREATE TABLE dim_state (
    state_code VARCHAR(2) PRIMARY KEY,
    state_name VARCHAR,
    region VARCHAR,
    expansion_status BOOLEAN,
    fmap DECIMAL(5,4)
);

CREATE TABLE dim_procedure (
    cpt_hcpcs_code VARCHAR(10),
    description VARCHAR,
    category_447 VARCHAR,      -- 'primary_care', 'obgyn', 'mh_sud', 'hcbs', 'other'
    betos_code VARCHAR,
    is_em_code BOOLEAN,
    work_rvu DECIMAL(8,4),
    nonfac_pe_rvu DECIMAL(8,4),
    fac_pe_rvu DECIMAL(8,4),
    mp_rvu DECIMAL(8,4),
    status_indicator VARCHAR(2),
    PRIMARY KEY (cpt_hcpcs_code)
);

CREATE TABLE dim_medicare_locality (
    locality_id VARCHAR(10),
    carrier VARCHAR,
    state_code VARCHAR(2),
    locality_name VARCHAR,
    gpci_work DECIMAL(6,4),
    gpci_pe DECIMAL(6,4),
    gpci_mp DECIMAL(6,4),
    urban_rural VARCHAR,
    PRIMARY KEY (locality_id)
);

CREATE TABLE dim_time (
    effective_date DATE PRIMARY KEY,
    year INTEGER,
    quarter INTEGER,
    state_fiscal_year INTEGER
);

CREATE TABLE dim_provider_type (
    type_code VARCHAR,
    description VARCHAR,
    taxonomy_code VARCHAR,
    PRIMARY KEY (type_code)
);

-- Fact tables
CREATE TABLE fact_medicaid_rate (
    state_code VARCHAR(2),
    cpt_hcpcs_code VARCHAR(10),
    modifier VARCHAR(10),
    provider_type VARCHAR,
    population VARCHAR,          -- 'adult', 'pediatric', 'all'
    geographic_area VARCHAR,
    effective_date DATE,
    medicaid_rate DECIMAL(10,2),
    source_file VARCHAR,
    last_updated DATE,
    dq_flag VARCHAR,             -- data quality flags
    PRIMARY KEY (state_code, cpt_hcpcs_code, modifier, provider_type, population, geographic_area, effective_date)
);

CREATE TABLE fact_medicare_rate (
    locality_id VARCHAR(10),
    cpt_hcpcs_code VARCHAR(10),
    modifier VARCHAR(10),
    effective_year INTEGER,
    non_facility_rate DECIMAL(10,2),
    facility_rate DECIMAL(10,2),
    conversion_factor DECIMAL(8,4),
    status_indicator VARCHAR(2),
    PRIMARY KEY (locality_id, cpt_hcpcs_code, modifier, effective_year)
);

CREATE TABLE fact_rate_comparison (
    state_code VARCHAR(2),
    cpt_hcpcs_code VARCHAR(10),
    category_447 VARCHAR,
    population VARCHAR,
    provider_type VARCHAR,
    geographic_area VARCHAR,
    year INTEGER,
    medicaid_rate DECIMAL(10,2),
    medicare_nonfac_rate DECIMAL(10,2),
    pct_of_medicare DECIMAL(6,4),
    claim_count INTEGER,
    beneficiary_count INTEGER,
    PRIMARY KEY (state_code, cpt_hcpcs_code, category_447, population, provider_type, geographic_area, year)
);

CREATE TABLE fact_hcbs_rate (
    state_code VARCHAR(2),
    hcbs_category VARCHAR,       -- 'personal_care', 'home_health_aide', 'homemaker', 'habilitation'
    provider_mode VARCHAR,       -- 'individual', 'agency'
    population VARCHAR,
    geographic_area VARCHAR,
    avg_hourly_rate DECIMAL(10,2),
    includes_facility_costs BOOLEAN,
    claim_count INTEGER,
    beneficiary_count INTEGER,
    year INTEGER,
    PRIMARY KEY (state_code, hcbs_category, provider_mode, population, geographic_area, year)
);

-- Crosswalk tables
CREATE TABLE xwalk_cpt_to_447_category (
    cpt_hcpcs_code VARCHAR(10) PRIMARY KEY,
    category_447 VARCHAR,
    source_document VARCHAR,     -- 'CMS_CY2025_EM_Code_List'
    effective_year INTEGER
);

CREATE TABLE xwalk_locality_to_state (
    locality_id VARCHAR(10),
    state_code VARCHAR(2),
    county_fips VARCHAR(5),
    population_weight DECIMAL(6,4),
    PRIMARY KEY (locality_id, state_code, county_fips)
);

CREATE TABLE xwalk_modifier_logic (
    state_code VARCHAR(2),
    modifier VARCHAR(10),
    adjustment_type VARCHAR,     -- 'percentage', 'fixed', 'not_covered'
    adjustment_value DECIMAL(8,4),
    description VARCHAR,
    PRIMARY KEY (state_code, modifier)
);
```

**DuckDB optimization notes:**
- Use `DECIMAL(10,2)` for rates, not `FLOAT`
- Use `DATE` for effective dates, not string
- Avoid primary keys during bulk load — add after
- Query Parquet directly: `SELECT * FROM read_parquet('s3://aradune-datalake/rates/state=FL/year=2025/*.parquet', hive_partitioning=true)`
- Pre-compute aggregated rollup tables for common dashboard queries

---

## 6. Data Quality Gates

Before any rate comparison is published, every record must pass:

**Rate validity:**
- Flag `$0.00` rates (distinguish non-covered from data error)
- Flag rates above $10,000 for a single E/M code
- Flag rates unchanged for 24+ months (stale detection)
- Cross-check: if `|medicaid_rate - (total_rvu × $33.40)| / medicaid_rate > 2.0` → flag for review

**Code coverage:**
- Verify state covers expected E/M codes from the CMS list
- Identify codes in Medicare PFS but missing from state schedule
- Track annual CPT additions/deletions against CMS PFS updates

**Medicare matching:**
- Validate locality-to-state mapping is correct
- Confirm conversion factor matches published CMS CF for the comparison year
- Verify GPCI factors against the published GPCI file
- For multi-locality states: document weighting methodology

**Modifier consistency:**
- Flag states where modifier logic diverges from Medicare standard
- Ensure 26/TC splits are handled properly
- Verify modifier adjustments don't produce rates exceeding global rate

**Cross-state validation:**
- Flag states where average primary care rates differ >3 SDs from national mean
- Compare computed Medicaid-to-Medicare ratios against KFF/MACPAC benchmarks as reasonableness check
- Flag impossible patterns (all codes at exactly same %-of-Medicare may indicate uniform CF error)

**Florida-specific rule (AHCA internal):**
- Rates **cannot** have both a facility rate AND a PC/TC split — must be one or the other
- Flagged codes requiring review: 46924, 91124, 91125

---

## 7. Data Ingestion Architecture

### Orchestration: Dagster
Use Dagster (not Airflow) as the primary orchestration framework. Reasons:
- Asset-centric model maps naturally to datasets-as-assets
- Built-in data lineage tracking
- Partitioning model maps to `state × year × quarter` data structure
- Strong DuckDB integration

### Automated CMS data pulls
```python
# Pattern for scheduled CMS bulk file ingestion
# Run annually (PFS), quarterly (SDUD, MBES), weekly (NPPES)
import httpx
import hashlib

def check_file_changed(url: str, stored_hash: str) -> bool:
    """Use HTTP HEAD + ETag/Last-Modified for change detection."""
    response = httpx.head(url)
    current_etag = response.headers.get('ETag', '')
    return current_etag != stored_hash

def ingest_pfs_rvu_file(year: int, release: str = 'A'):
    """
    Download Medicare PFS RVU file.
    URL pattern: https://www.cms.gov/medicare/payment/fee-schedules/physician/pfs-relative-value-files
    File naming: RVU{YY}{RELEASE}.ZIP (e.g., RVU26A.ZIP)
    """
    url = f"https://downloads.cms.gov/medicare/physicianfeeschedule/rvu/{year}/RVU{str(year)[2:]}{release}.ZIP"
    # download, extract, validate, load to DuckDB staging
```

### State fee schedule scraping registry
Maintain a YAML registry of all 51 jurisdictions:
```yaml
states:
  FL:
    fee_schedule_url: "https://ahca.myflorida.com/..."
    format: "pdf"
    parser: "florida_fee_schedule_parser"
    update_frequency: "annual"
    last_checked: "2026-03-01"
  TX:
    fee_schedule_url: "https://public.tmhp.com/FeeSchedules"
    format: "excel"
    parser: "texas_tmhp_parser"
    update_frequency: "monthly"
```

### PDF extraction pipeline
```python
# Multi-tool PDF extraction with LLM cleaning
import pdfplumber
import anthropic

def extract_fee_schedule_pdf(pdf_path: str, state_code: str) -> list[dict]:
    """
    Tier 1: pdfplumber for text-layer PDFs with structured tables
    Tier 2: Camelot/Tabula for complex table layouts
    Tier 3: LLM-assisted column normalization for ambiguous structures
    """
    with pdfplumber.open(pdf_path) as pdf:
        tables = []
        for page in pdf.pages:
            table = page.extract_table()
            if table:
                tables.extend(table)
    
    # LLM normalization of column headers
    client = anthropic.Anthropic()
    normalized = normalize_columns_with_llm(tables, state_code, client)
    return normalized

def normalize_columns_with_llm(raw_tables, state_code, client):
    """Map heterogeneous state column headers to standard schema."""
    prompt = f"""
    You are normalizing a Medicaid fee schedule from state {state_code}.
    Map these column headers to our standard schema:
    Standard columns: cpt_hcpcs_code, modifier, rate, provider_type, population, geographic_area, effective_date
    
    Raw headers: {raw_tables[0] if raw_tables else []}
    
    Return a JSON mapping of raw_header -> standard_column (or 'ignore' if not relevant).
    Return ONLY valid JSON, no other text.
    """
    response = client.messages.create(
        model="claude-haiku-4-5-20251001",
        max_tokens=500,
        messages=[{"role": "user", "content": prompt}]
    )
    return json.loads(response.content[0].text)
```

---

## 8. Serving Architecture

### Query routing
```
User query
    │
    ├── Pre-computed lookup? (rate for a specific code/state) 
    │       → DuckDB-WASM + CDN Parquet (sub-second, zero server cost)
    │
    ├── Analytical query? ("which states pay < 60% Medicare for primary care?")
    │       → Server-side DuckDB REST API (seconds)
    │
    └── Policy reasoning? ("explain how this SPA affects access")
            → RAG + LLM pipeline (5–15 seconds)
```

### DuckDB-WASM optimization
- Pre-compute all aggregate tables server-side, export as small Parquet (~50–200MB)
- Host on CloudFront CDN (24-hour TTL)
- DuckDB-WASM fetches only required columns via HTTP Range requests
- Practical limit: ~5–10M rows per WASM query; larger analyses route server-side

### API design (FastAPI)
```python
# Key endpoints
GET  /api/rates/{state_code}/{cpt_code}          # single rate lookup
GET  /api/cpra/{state_code}?year=2025            # full CPRA for a state
GET  /api/compare?states=FL,TX,CA&category=primary_care  # cross-state comparison
GET  /api/threshold/80pct?state={state_code}     # codes below 80% Medicare
POST /api/spa-analysis                            # rate reduction SPA analysis
GET  /api/hcbs/{state_code}?category=personal_care  # HCBS hourly rates
```

### Storage tiers
| Tier | Storage | Use Case | Cost |
|---|---|---|---|
| Hot | DuckDB in-memory/SSD | Pre-computed aggregates, sub-second queries | ~$150–200/mo (EC2 r6g.xlarge) |
| Warm | S3 Standard + Parquet/Iceberg | Full datasets, seconds-to-minutes queries | ~$0.023/GB/month |
| Cold | S3 Glacier | Raw source files, archived PDFs | ~$0.004/GB/month |

**Table format recommendation:** Apache Iceberg over Delta Lake — engine-agnostic, works with DuckDB extension, strong AWS native support via S3 Tables.

---

## 9. AI/LLM Layer

### Model routing strategy
| Query type | Model | Cost/query | Use case |
|---|---|---|---|
| Classification, simple lookup | claude-haiku-4-5-20251001 | ~$0.004 | Route queries, extract structured data |
| RAG + NL2SQL, analysis | claude-sonnet-4-6 | ~$0.03–0.06 | Policy questions, rate analysis |
| Complex policy reasoning | claude-opus-4-6 | ~$0.28 | SPA drafting, CPRA narrative generation |

Use **prompt caching** (90% input cost reduction for repeated context) and **batch API** (50% discount for non-real-time work) to keep LLM costs under $10K/month at 1,000 active users.

### RAG pipeline for Medicaid policy documents
**Corpus to index:**
- All published SPAs (from MACPro/OneMAC scraping)
- 1115 and 1915 waiver approvals and special terms
- CMS informational bulletins (CIBs) and State Health Official (SHO) letters
- Federal Register notices for Medicaid rules
- State Medicaid manuals
- MACPAC reports and issue briefs

**Technical stack:**
- Embedding model: Voyage-3-large (outperforms OpenAI by 9–20% on retrieval benchmarks)
- Vector database: pgvector (PostgreSQL extension) — hybrid BM25 + semantic search
- Chunking: 500–1,000 tokens at logical section boundaries, 100–200 token overlap
- Metadata: `{document_type, state_code, effective_date, regulatory_citation, source_url}`

### NL2SQL for the data warehouse
- Framework: Vanna (open-source, MIT license, DuckDB native support)
- Provide 200+ curated Medicaid-specific NL→SQL examples
- Include full schema documentation in system prompt
- Auto-correct pattern: execute → if error → retry up to 3 times with error feedback
- Target accuracy: 85%+ with domain-specific training

---

## 10. User Segments and Their Core Needs

### State Medicaid agencies
**Primary pain:** 447.203 compliance by July 2026. Rate-setting analysts use Excel + SAS + MMIS. Manual, labor-intensive, error-prone.
**What they need from Aradune:**
- CPRA auto-generation (upload fee schedule → get compliant output)
- 447.203 compliance checklist with pass/fail indicators
- 80% Medicare threshold flagging for rate reduction SPA analysis
- 4% SFY expenditure cap tracker
- Rate change monitoring with 30-day update deadline alerts
- Historical rate decay visualization for their own schedule

**Pricing:** $50K–$200K/year. Critical: if tool qualifies for **75% Federal Financial Participation** as Medicaid IT expenditure, effective state cost drops to $12.5K–$50K/year.

### Medicaid consulting firms (Milliman, HMA, Myers & Stauffer, Mathematica, Mercer, Guidehouse, Manatt, Sellers Dorsey)
**Primary pain:** Cross-state rate comparison requires assembling 51 different fee schedules manually. No standardized national dataset exists.
**What they need:**
- All 50 states' fee schedules in a standardized, queryable database
- Cross-state rate benchmarking by code and service category
- SPA/waiver tracking and precedent language search
- CPRA auto-generation as a deliverable for state clients
- Rate reduction impact modeling (80% threshold + 4% cap)
- Exportable, branded PDF reports for client presentations
- T-MSIS-based utilization and access metrics for program evaluations

**Pricing:** $50K–$250K/year enterprise license. HMA's HMAIS subscription is the closest existing product.

### MCOs / managed care organizations (Centene, Elevance, UnitedHealthcare, Molina, CVS/Aetna)
**Primary pain:** Rate underfunding (post-unwinding MLR 89→93%+) + network adequacy compliance across 40+ state portfolios.
**What they need:**
- Cross-state capitation rate benchmarking
- SPA/waiver policy change alerts (affects MCO contract terms)
- Network adequacy analytics (provider-to-beneficiary ratios, appointment wait times)
- MCO financial benchmarking (MLR, administrative ratio, medical costs by category)
- Quality measure tracking (HEDIS, CAHPS, Core Set) across their states
- Medicaid-to-Medicare rate parity analysis (affects their FFS carve-out payments)

**Pricing:** $100K–$500K/year depending on state footprint.

### Hospitals and providers
**Primary pain:** Medicaid underpayment quantification ($130B nationally in 2023, AHA data).
**What they need:**
- Medicaid-to-cost comparison using HCRIS cost-to-charge ratios
- Medicaid-to-Medicare rate comparison for advocacy
- DSH and supplemental payment tracking
- Fee schedule monitoring with rate change alerts
- FFS vs. MCO rate comparison for the same services

**Pricing:** $500–$2,000/month.

### Journalists, legislative aides, and advocacy organizations
**Primary pain:** Limited to FOIA requests and manually compiled KFF/MACPAC data.
**What they need:**
- Free, easy-to-use dashboards comparing states on key Medicaid metrics
- Data download capability for custom analysis
- Enrollment tracking (especially post-unwinding)
- MCO financial performance data (MLR, profitability)
- Rate adequacy visualization (% of Medicare by state and service category)

**Pricing:** Free tier. Premium institutional tier at $29–$49/month.

---

## 11. Competitive Whitespace

No existing platform combines:
1. Comprehensive public Medicaid data aggregation (fee schedules, enrollment, expenditures, quality, managed care)
2. Cross-state analytical capabilities
3. Multi-stakeholder access (states + consultants + MCOs + providers + journalists)
4. Real-time policy and rate change intelligence
5. AI-powered analysis and report generation

**Key competitive advantages Aradune must defend:**
- **No conflict of interest**: Unlike Optum (owned by UnitedHealthcare, the largest MCO), Aradune is independent
- **Public data moat**: Aggregating standardized public data requires no permissions, just engineering excellence — the value is curation, standardization, linkage, and AI analysis
- **AI-native from day one**: Not bolting AI onto systems designed in the 2000s
- **447.203 timing**: Every state needs compliance help RIGHT NOW; no incumbent has built this product

**HMAIS (Health Management Associates Information Services)** is the closest existing product — a subscription service for cross-state Medicaid market data. Aradune should study HMAIS positioning carefully and differentiate on data depth (code-level vs. program-level), AI capability, and pricing accessibility.

---

## 12. Priority Build Sequence

1. **Medicare PFS ingestion pipeline** — automated annual + quarterly pulls, non-facility rate calculation engine, locality crosswalk
2. **CPT-to-447 service category crosswalk** — anchored on CMS CY 2025 E/M Code List
3. **State fee schedule adapters** — prioritize the 15 largest states by Medicaid enrollment (covers ~70% of national enrollment)
4. **CPRA output generator** — compliant publication output in PDF, Excel, and HTML
5. **447.203 compliance checklist module** — binary pass/fail dashboard for each regulatory requirement
6. **80% threshold and 4% cap analytics** — for rate reduction SPA analysis (paid tier)
7. **National rate comparison dashboard** — free tier hook, state heatmap by service category
8. **HCBS hourly rate disclosure module** — follow-on after July 2026 FFS launch
9. **RAG pipeline over Medicaid policy corpus** — SPA/waiver/CIB/SHO letter search
10. **NL2SQL interface** — natural language queries against the data warehouse

---

## 13. Infrastructure and Costs

**Initial stack (current scale, ~227M rows):**
- EC2 r6g.xlarge (32GB RAM): $150–200/month
- S3 Standard (~100GB): $2–5/month
- CloudFront CDN: $10–20/month
- Dagster Cloud starter: $0–100/month
- RDS PostgreSQL t3.micro (pgvector): $15–30/month
- **Total: ~$200–400/month**

**Growth stage (1–5B rows):**
- EC2 r6g.4xlarge (128GB RAM) or MotherDuck: $400–600/month
- S3 (~1TB): $23/month
- **Total: ~$900–1,700/month**

**Cost optimizations:**
- Spot instances for batch pipeline processing: 60–80% savings
- Reserved instances for always-on API server: ~40% savings
- DuckDB-WASM offloads dashboard queries to zero server cost
- Apply for AWS Activate startup credits ($10K–$100K available)

---

## 14. Monetization

| Tier | Price | Who | Key Features |
|---|---|---|---|
| Free (Aradune Open) | $0 | Journalists, advocates, legislative aides | Basic dashboards, 50 lookups/day, 2yr data, watermarked exports, 447.203 status tracker |
| Analyst | $99/month | Individual analysts, small consultants | Full data access, 200 AI queries/month, CSV exports, NL2SQL, rate comparison tools |
| Pro | $299/month | Power users, consulting teams | Unlimited AI queries, CPRA auto-generation, SPA tracking alerts, API access (1K calls/month), branded PDF reports |
| State Agency | $50K–$200K/year | State Medicaid agencies | 447.203 compliance suite, white-label output, SSO, SLA, may qualify for 75% FFP |
| Enterprise | $50K–$500K/year | Consulting firms, MCOs | Bulk API, custom integrations, white-label dashboards, dedicated account management, DaaS licensing |

**Data as a Service (DaaS):** Sell cleaned/linked datasets via API or bulk license — all 50 states' fee schedules standardized ($25K–$100K/year per dataset), SPA tracking database, HCBS rate comparison dataset.

---

*Last updated: March 2026. Sources: CMS-2442-F final rule, 42 CFR 447.203 (eCFR), CMS Guide for States (July 2024), MACPAC reports, ResDAC documentation, KFF State Health Facts, Myers and Stauffer client alert (November 2025), Georgetown CCF analysis.*
