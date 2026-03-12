# Bulletproof Medicaid data: a complete reference for the Aradune platform

**Building a production-grade Medicaid analytics platform requires navigating a minefield of inconsistent state reporting, suppressed fields, schema drift, and datasets that were never designed to work together.** This reference documents every known data quality issue across the ten core datasets feeding Aradune, alongside the validation frameworks, adversarial testing patterns, and architectural decisions needed to build a DuckDB/Parquet data lake that handles real-world healthcare data gracefully. The core finding: no single Medicaid dataset is authoritative for any dimension of analysis—expenditures, enrollment, and utilization each require triangulating across multiple sources with different organizing principles, temporal frameworks, and quality levels.

---

## Part 1: Dataset-specific quality issues

---

### T-MSIS/TAF is improving but remains deeply uneven across states

T-MSIS (Transformed Medicaid Statistical Information System) is the backbone of Medicaid claims and enrollment analytics. CMS runs **~4,400 automated checks** on each state's monthly submission, and the DQ Atlas (built by Mathematica) classifies quality into four tiers: low concern, medium concern, high concern, and unusable. The Outcomes Based Assessment (OBA) framework tracks **500+ high/critical priority checks** across three scoring tiers: critical priority (100% target), high priority (99% target), and expenditures (95% target). States failing targets for two consecutive months face escalating compliance actions.

**The TAF file structure** consists of seven file types. Annual files include the DE (Demographic & Eligibility), APR (Annual Provider), and APL (Annual Managed Care Plan). Monthly claims files include IP (Inpatient), LT (Long-Term Care), OT (Other Services—the largest and most diverse file), and RX (Pharmacy). Each claims file has separate header and line-level files linked by `DA_RUN_ID` + `*_LINK_KEY`. The `PAYMENT-LEVEL-IND` field indicates whether payment is determined at header level (1) or line level (2).

**Critical state-level issues** demand per-state handling logic. **Illinois** is the single most dangerous edge case: it captures adjustments as incremental credits/debits rather than voiding and replacing claims, meaning the standard TAF final-action algorithm fails. All claim versions are included, and naïve analysis will overcount utilization. CMS publishes separate "How to Use Illinois Claims Data" documentation that must be implemented. **Arkansas** was excluded entirely from the 2016 TAF RIF. **Idaho** showed a **38% drop in outpatient claims** between preliminary and final 2021 TAF (5.8M to 3.6M), demonstrating why preliminary data should never be treated as final. **Utah** shows anomalously low paid amounts despite high enrollment due to managed care encounter data gaps.

**Managed care encounter data remains the weakest area.** GAO found **30 states** failed to submit acceptable inpatient managed care encounter data. Payment amounts on managed care encounters are frequently **$0** or redacted in TAF RIFs (proprietary MCO payment information). With 55+ million Medicaid beneficiaries in managed care, this gap affects the majority of the program. Fee-for-service data is consistently higher quality than managed care encounter data.

**Race/ethnicity data quality** has improved but remains problematic: in 2022, **14 states** were rated "high concern" and **1 state** remained "unusable." DQ Atlas benchmarks against ACS five-year estimates. Connecticut once reported 0% Hispanic enrollment despite ACS showing 33%.

**Void/adjusted claims** require careful handling using `ADJUSTMENT_IND` and `LINE_ADJUSTMENT_IND` fields (0=original, 1=void, 4=replacement, 5/6=service tracking). The five record-key elements—Submitting State Code, ICN-ORIG, ICN-ADJ, Adjustment Indicator, and Adjudication Date—link claim families. TAF includes only final-action, non-voided, non-denied claims, but some states cannot reliably identify final-action claims.

**Crossover claims** for dual eligibles (identified via `XOVR_IND`) present a specific trap: some states report the full Medicare coinsurance/deductible amount rather than the Medicaid-paid amount, inflating Medicaid spending figures. Dual eligibility codes 02, 04, and 08 indicate full duals; 01, 03, 05, and 06 indicate partial duals.

**The DOGE-released TMSIS data** (February 13, 2026) was a provider-level aggregation covering outpatient/professional claims from January 2018–December 2024. Each of the **227+ million rows** represented a unique combination of Billing Provider NPI × Service Provider NPI × HCPCS Code × Month. It excluded inpatient, pharmacy, and long-term care claims entirely, contained no beneficiary state variable, suppressed rows with fewer than 12 claims, and showed sharply incomplete November/December 2024 data consistent with claims lag. McDermott+ analysis demonstrated that states with high managed care penetration (e.g., Utah at 77.1%) showed anomalously low paid amounts compared to FFS-dominant states (e.g., Alaska), making interstate comparisons misleading. The dataset was taken offline within weeks ("temporarily unavailable while we make improvements").

**MSIS to T-MSIS transition** considerations for longitudinal analysis: MSIS submitted quarterly with ~400 data elements; T-MSIS submits monthly with ~1,400 elements. The research extract changed from MAX to TAF with different adjustment algorithms, populations (TAF includes S-CHIP), file organization, and eligibility coding. CMS provides bridge files for analysis spanning both eras. All states cut over by 2016; for 2014–2015, researchers may need MAX for some states and TAF for others.

**Recommended handling for Aradune:**
- Always consult DQ Atlas before including any state/year combination
- Implement Illinois-specific deduplication logic as a first-class pipeline concern
- Use `CLM_TYPE_CD` to separate FFS from managed care analysis paths
- Benchmark TAF expenditures against CMS-64 totals
- Use only final-release TAF files (12+ months runout) for production analytics
- Store the TAF version (preliminary vs. Release 1 vs. Release 2) as metadata

---

### SDUD drug utilization data requires careful NDC normalization and unit handling

The State Drug Utilization Data is available as CSV files on Data.Medicaid.gov, reported quarterly by states within 60 days of rebate period end. Each record represents a unique NDC × State × Quarter × Utilization Type (FFS or MCO) combination.

**NDC coding is the primary quality hazard.** The 11-digit format (5-4-2) requires left-padding the three underlying FDA components (which can be 4-4-2, 5-3-2, or 5-4-1). Invalid or improperly formatted NDCs appear when states fail to pad correctly. The FDA allows **NDC reuse**—discontinued NDCs can be reassigned to entirely different drugs, creating historical ambiguity. CMS's Medicaid Drug Programs system cannot maintain data for different products sharing the same 9-digit NDC, so the earlier product's entire history gets deleted upon reuse.

**Suppression rules** hide prescriptions counts **under 11** per NDC-state-quarter-utilization type. CMS also applies secondary suppression to prevent back-calculation. For rare drugs like buprenorphine for opioid addiction treatment, suppression significantly affects analysis. Urban Institute researchers obtained unsuppressed data via FOIA and found the mean for suppressed cells was approximately **6 prescriptions**.

**Unit type inconsistencies** make cross-state comparisons unreliable. Tablets/capsules use "EACH," liquids use "MILLILITER," and topicals use "GRAM," but states may report differently for compound products, kits, and bulk chemicals. Aggregating "units" across drug types without normalization by dosage form produces meaningless results.

**All amounts are pre-rebate**, overstating net Medicaid drug costs. MCO records may show prescriptions dispensed but $0 for amount reimbursed. States inconsistently exclude **340B claims** before submission, potentially inflating utilization figures. Pre-2010 MCO prescription data was not required and is incomplete.

**Recommended linking strategy:** Use the FDA NDC Directory for drug identification, normalize to 5-4-2 format, map through **RxNorm** (updated monthly by NLM) for canonical drug identification, and add therapeutic classification via ATC or USP systems. For pricing context, cross-reference NADAC (National Average Drug Acquisition Cost) published by CMS.

---

### MBES/CMS-64 and T-MSIS expenditures will never perfectly reconcile

MBES (Medicaid Budget and Expenditure System) captures CMS-64 financial data organized by **payment date** and quarter, while TAF organizes by **date of service**. This fundamental difference means the same expenditures appear in files covering different time periods—an inpatient stay in September 2018 paid in January 2019 appears in CY 2018 TAF data but FFY 2019 CMS-64 data.

**Prior period adjustments** in CMS-64 are submitted as marginal adjustments in the quarter made and **cannot be allocated to the original expenditure period**. States have up to 2 years (sometimes longer) to report PPAs. TAF consolidates adjustments into a single final-action record assigned to the original service date. This is a structural incompatibility, not a data quality error.

**Non-claim financial transactions** further diverge the sources. CMS-64 uniquely includes collections (third-party liability, fraud recoveries), Medicare premium payments for duals, and drug rebate offsets. T-MSIS was not historically expected to capture some of these transactions. DSH payments, UPL supplemental payments, and GME payments appear on service tracking claims in TAF, but completeness is unverified.

**MACPAC's reconciliation methodology** adjusts T-MSIS benefit spending to match CMS-64 totals by creating state-level adjustment factors per service category, distributing drug rebates proportionally across enrollees with drug spending, and allocating Medicare premiums across dually eligible enrollees. This is the current best practice but involves significant assumptions.

**Multiple FMAP rates** may apply within a single state/quarter: standard FMAP, enhanced for ACA expansion adults, 100% for COVID testing groups, and temporary increases under FFCRA. States report on separate CMS-64 lines for different FMAP-eligible populations, but misclassification occurs.

**For the Aradune pipeline:** Treat CMS-64 as the authoritative source for total expenditures and TAF as the authoritative source for service-level detail. Maintain both in parallel. Never expect exact reconciliation—build dashboards that show the gap and its known causes.

---

### HCRIS hospital cost reports are notoriously noisy but essential

HCRIS contains three flat files per fiscal year (RPT, NMRC, ALPHNMRC) linked by `RPT_REC_NUM`. The numeric file stores each cell as a row identified by worksheet code, line number, and column number—producing **200,000+ unique combinations** for the CMS-2552-10 form. Two form versions coexist: CMS-2552-96 (through ~2010) and CMS-2552-10 (2010 onward), requiring crosswalk logic.

**The data is "notoriously noisy and mis-measured"** (per academic researchers). Cost reports are **not audited, not standardized across hospitals, and do not follow GAAP**. Negative values appear where positive values are expected. Some hospitals report obviously incorrect figures. **Trimming/winsorizing outliers** is strongly advised.

**Multiple reports per provider** are common due to fiscal year changes. Hospitals may file overlapping reports, two short-period reports, or one long-period report during transitions. The Ian McCarthy and Adam Sacarny GitHub repositories implement specific rules: collapsing duplicates, creating synthetic calendar-year data by weighting reports by the fraction of the year they cover, and handling negative values.

**Key worksheets** for Medicaid analytics: S-2 (hospital characteristics/beds), S-3 (utilization), A (cost allocation), C (ratio of costs to charges), D (Medicare apportionment), E (settlement), G (balance sheet), S-10 (uncompensated care). S-10 definitions have changed over time, requiring harmonization.

**Open tools:** NBER provides SAS/Stata extract code and crosswalks. RAND Hospital Data Tool offers enhanced panel datasets with outlier correction. GitHub repositories from [imccart/HCRIS](https://github.com/imccart/HCRIS) (R) and [asacarny/hospital-cost-reports](https://github.com/asacarny/hospital-cost-reports) (R, outputs Parquet) handle common cleanup tasks.

---

### NPPES provider data is shockingly stale and minimally verified

As of recent analysis, **only 8.2% of 6.2 million NPIs had been updated within the past year**, and **57% had not been updated in over 5 years**. All NPPES data is self-reported. CMS only verifies SSN and address validity—**not** the provider's specialty/taxonomy code, which may not reflect actual training or board certification.

**Deactivated NPIs** are included in the monthly full replacement file since ~2018. The R package `npi` (ropensci) provides validation but does not check activation status. **Taxonomy codes** are self-reported and unverified; providers can list one primary and two secondary taxonomy codes that may be outdated.

**PECOS** (Provider Enrollment, Chain, and Ownership System) has overlapping but independently collected data with a 5-year revalidation requirement. PECOS uses different specialty codes than NPPES taxonomy codes. Cross-reference both systems plus AMA Masterfile and state licensing boards for reliable provider identification. The NPPES Version 2 file format became required March 3, 2026, with extended field lengths.

---

### Hospital price transparency MRFs are a parsing nightmare with only 21% compliance

PatientRightsAdvocate.org found only **21.1% of 2,000 hospitals** fully compliant as of November 2024, and only **6.7%** posting sufficient dollars-and-cents pricing data. CMS has fined only **15–18 hospitals** total. Even among compliant hospitals, data quality is severe:

- **Format chaos:** JSON, CSV, XLSX, pipe-delimited, nested JSON, XML, and proprietary formats all appear, even after CMS mandated a standard template effective January 2025
- **Extreme outlier values:** Negotiated rates for hip/knee replacement range from <$1 to >$1,000,000
- **Percentage vs. dollar confusion:** Some "rates" represent percentages of charges, appearing as values less than $1
- **No standardized payer naming:** Each hospital uses its own notation for insurance plans
- **Missing standard codes:** Many entries have only internal hospital codes with no CPT/HCPCS/DRG

**Parsing tools:** Turquoise Health (commercial, >1B records from 5,100+ hospitals), DoltHub (open-source, ~300M prices from ~1,800 hospitals with `mrfutils` Python utility), CMS GitHub (official templates and validator), and Postman Open Technologies (community API project).

---

### Supplemental payments, RBRVS, MCPARs, and other datasets each carry distinct quality concerns

**Supplemental payment data (DSH, UPL, IGT/CPE)** is the least standardized category. GAO has flagged it as a High Risk Issue. There is **no reliable public tracking mechanism for IGT/CPE** funding sources. The new Section 1903(bb) reporting system (effective October 2021) is in early stages of standardization. DSH audit data showed **228 of 2,953 audits** with reliability/documentation issues. UPL supplemental payments for hospital services totaled approximately **$15.8 billion** in FY 2022 per MACPAC.

**RBRVS/Medicare Fee Schedule** data requires meticulous versioning. The CY 2026 updates introduced a **-2.5% efficiency adjustment** to work RVUs for most procedures and a site-of-service PE methodology change—both break year-over-year comparability. Conversion factor instability is severe: CY 2024 had two different CFs due to mid-year legislation, and CY 2026 introduced separate CFs for Qualifying APM Participants vs. non-QPs for the first time. GPCIs still use **2006-based MEI cost share weights**. Maintain annual versioned snapshots of RVU files, GPCI files, and conversion factors with effective date ranges.

**MCPARs** (Managed Care Program Annual Reports) are very new—first submitted in 2022, first posted on Medicaid.gov in July 2024, and available **only as PDFs** (not machine-readable). CMS is still prioritizing data quality and technical assistance before analytics. Beginning June 2026, states must report plan-level prior authorization data; contract rating periods beginning July 2027 will include enrollee experience survey results.

**Other important datasets** for Medicaid analytics include the ACS (systematically undercounts Medicaid enrollment; useful for demographic benchmarking), PECOS (5-year revalidation; different specialty coding from NPPES), AHRF (county-level health workforce data with 6,000+ variables), HEDIS (proprietary quality measures; Medicaid-commercial gap worsening 2017–2022), and KFF curated state-level compilations.

---

## Part 2: Adversarial testing and smart ingestion architecture

---

### The optimal DuckDB/Parquet validation stack combines four tools

After evaluating eight frameworks against DuckDB/Parquet compatibility, healthcare applicability, and maturity, the recommended stack is:

**Soda Core** (v4) provides the **best native DuckDB support** of any validation framework. It connects directly to DuckDB databases, in-memory databases, and raw Parquet files. SodaCL (Soda Checks Language) offers human-readable YAML syntax for 50+ built-in checks including missing, duplicate, invalid, freshness, schema validation, reference checks, and custom SQL. The Python API integrates into any pipeline. Soda Cloud adds ML-powered anomaly detection but requires subscription (~$500/month).

**dbt-duckdb with dbt-expectations** provides SQL-first validation tightly integrated with the transformation layer. The dbt-duckdb adapter is mature (maintained under the DuckDB GitHub org, supports DuckDB 1.1.x+). The dbt-expectations package ports 60+ Great Expectations tests to dbt macros with confirmed DuckDB support, including regex matching for code validation, range checks for amounts, and cross-column comparisons (discharge_date > admission_date).

**Pandera** (v0.29.0) handles DataFrame-level validation for Python pipelines with native Polars and Ibis/DuckDB integration. Its unique statistical hypothesis testing capability (t-tests, chi-square on data distributions) is valuable for detecting distribution shifts in claims data across states or time periods. Type-safe schema definitions integrate with mypy for compile-time checking.

**datacontract-cli** provides contract testing in CI/CD, internally using DuckDB and Soda Core. It supports the Open Data Contract Standard (ODCS v3.1.0), maintained by Bitol under the LF AI & Data Foundation. Commands include `datacontract test`, `datacontract lint`, `datacontract diff` for breaking change detection.

**Emerging tools worth monitoring:** Pointblank (v0.20.0, first-class DuckDB support via Ibis, beautiful HTML reports, LLM-powered validation plan generation) and DuckDQ (CWI/Amsterdam, purpose-built for DuckDB, benchmarked at **4x faster** than Great Expectations via scan sharing optimization). Great Expectations remains the largest community but lacks native DuckDB support, requiring SQLAlchemy workarounds with friction. Monte Carlo, Elementary, and other enterprise observability tools **do not support DuckDB**.

---

### Property-based and adversarial testing catches what unit tests miss

**Hypothesis** (Python's property-based testing library) generates hundreds of random inputs—including edge cases—to find violations of declared properties. For healthcare pipelines, custom strategies generate realistic but adversarial data:

```python
icd10_strategy = st.from_regex(r'[A-Z][0-9]{2}(\.[0-9]{1,4})?', fullmatch=True)
ndc_strategy = st.from_regex(r'[0-9]{5}-[0-9]{4}-[0-9]{2}', fullmatch=True)
npi_strategy = st.from_regex(r'[12][0-9]{9}', fullmatch=True)
```

Key properties to test: **row count preservation** (transformations don't silently drop records), **schema stability** (output matches contract), **idempotency** (running twice yields identical results), **referential integrity** (FK relationships survive joins), and **null propagation** (NULLs aren't silently converted to defaults).

**Synthetic data generation** should use different tools for different purposes. **SDV (Synthetic Data Vault)** is the top pick for realistic stress testing—it learns statistical patterns from real data using GaussianCopula or CTGAN models and generates privacy-preserving synthetic records that preserve multi-table relationships. A 2025 Nature Digital Medicine paper validated SDV achieving **83.1% confidence interval overlap** with real healthcare data distributions. **Synthea** (MITRE) generates complete patient medical histories using 120+ disease modules informed by CDC/NIH statistics—ideal for clinical scenario testing. **CMS SynPUF** provides 2.33 million synthetic Medicare beneficiaries whose variable names match actual CMS Limited Data Sets—the gold standard for validating ETL logic. **Mimesis** generates test data **12–15x faster than Faker** and should be used for volume spike testing (10M+ rows). The `faker-healthcare-system` package adds NPI, taxonomy, and organization generation.

**Chaos engineering for data pipelines** requires controlled failure injection. **lakeFS** provides Git-like branching for data lakes, enabling zero-risk chaos experiments—create a branch (metadata-only, milliseconds), inject failures, run the pipeline, and instantly revert. Key failure injection patterns for Aradune include:

- **Schema drift:** Add/remove/rename columns in Parquet files mid-pipeline
- **Null injection at graduated rates** (1%, 5%, 10%, 25%, 50%) on critical fields like member_id, diagnosis_code, and service_date
- **Encoding chaos:** Smart quotes, em-dashes, accented characters, null bytes in provider names
- **Duplicate injection:** Exact duplicates and near-duplicates (e.g., same claim with slightly different processed_date or uppercased provider name)
- **Volume spikes:** 10x normal data volume to test memory management
- **File corruption:** Truncated Parquet files, byte-flipped data
- **Invalid codes:** ICD-10 codes with trailing spaces, lowercase, extra decimal places, or completely invalid formats
- **Date format mixing:** MM/DD/YYYY, YYYY-MM-DD, DD-Mon-YYYY in the same column

Structure the test suite in layers: `tests/unit/` (Hypothesis property tests, code validators), `tests/integration/` (schema contracts, referential integrity, dbt-expectations), `tests/chaos/` (schema drift, null injection, encoding, duplicates, volume spikes, corrupt files), and `tests/adversarial/` (invalid codes, outlier values, boundary conditions).

---

### Healthcare code normalization requires versioned crosswalks and temporal awareness

**ICD-9 to ICD-10 crosswalks** via CMS General Equivalence Mappings (GEMs) are the standard, but CMS explicitly warns "there is no simple crosswalk." Only **40% of ICD-9 codes have exact one-to-one ICD-10 matches**. Many-to-many mappings are common, and Jaccard similarity between GEM-derived and clinician-derived crosswalks ranges from 0.06 to 1.00 depending on the condition. For longitudinal analysis spanning the 2015 transition, create **AHRQ CCSR groupings** or CCS categories as a common abstraction layer above both ICD versions. Store both the original and mapped codes with GEM flag metadata (approximate, no-map, combination, scenario, choice-list).

**NDC normalization** requires converting all codes to consistent 11-digit 5-4-2 format by left-padding components with zeros. Use **RxNorm** (NLM, updated monthly) as the canonical drug identifier. The linking chain is: raw NDC → normalized NDC-11 → RxCUI → therapeutic classification (ATC/USP) + pricing (NADAC/ASP/AWP). For Part B drugs, use the CMS NDC-to-HCPCS crosswalk via ASP Drug Pricing Files.

**Temporal normalization** must handle three frameworks simultaneously: Federal Fiscal Year (October–September), State Fiscal Year (varies, most use July–June), and Calendar Year. TAF uses service date and calendar year; CMS-64 uses payment date and federal fiscal year. Store all three dates (service, payment, adjudication) as separate columns and create computed FFY/SFY/CY columns. For cost normalization over time, use the **BLS Medical Care CPI** (FRED series CPIMEDSL) rather than general CPI—medical prices have outpaced general prices by 35+ percentage points since 2000. For geographic normalization, apply CMS Area Wage Index and GPCIs.

All reference data tables—ICD-10 (updated October 1), CPT (updated January 1), NDC (rolling), RxNorm (monthly), NUCC Taxonomy (January and July), FMAP rates (annual), GPCIs, conversion factors—should be stored as **SCD Type 2** Parquet files with `effective_date` and `termination_date` columns, enabling point-in-time joins for historical accuracy.

---

### DuckDB/Parquet architecture should follow medallion patterns with Iceberg for schema evolution

**The recommended table format is Apache Iceberg** for production workloads. The DuckDB Iceberg extension now supports both read and write operations, handles schema evolution (add columns, rename, reorder) via field ID mapping, provides time travel via snapshots, and integrates with REST catalogs (AWS Glue, Polaris, LakeKeeper). **DuckLake** (2025) is a simpler alternative that stores metadata in a SQL database rather than JSON/Avro files—worth evaluating if infrastructure simplicity is prioritized over ecosystem compatibility.

**DuckDB's `union_by_name` parameter** is the primary mechanism for handling schema evolution in Parquet files. When reading files with different columns, missing columns are filled with NULLs. Combined with Hive partitioning (`hive_partitioning=true`), this enables automatic partition pruning and schema-tolerant reads across years of evolving state data submissions.

The **medallion architecture** for Aradune should work as follows:

**Bronze layer** (raw, append-only): Store data exactly as received in Parquet format, partitioned by `state/year/month`. Include metadata columns `_source_file`, `_ingestion_timestamp`, `_source_state`, `_batch_id`. Never modify—this is the audit trail. Use `union_by_name=true` for seamless schema evolution.

**Silver layer** (normalized/cleaned): Apply ICD code normalization via GEMs and CCSR, NDC → RxNorm mapping, NPI → taxonomy enrichment, state-specific → national code mapping, temporal alignment (FFY/SFY/CY columns), deduplication via the TAF final-action algorithm (with Illinois exception), and void/replacement claim logic. Partition by state/year using Iceberg or DuckLake for schema management.

**Gold layer** (analytics-ready): Pre-computed PMPM cost summaries (inflation-adjusted via Medical Care CPI), utilization metrics by state/service category, quality performance indicators, and pre-joined dimensional models for BI consumption.

**Partitioning guidelines:** Target **minimum 100 MB per partition**. Use ZSTD compression (best balance of ratio and speed for healthcare data). Target row groups of 500K–1M rows for optimal compression, with file sizes of 100 MB–1 GB. Use `PER_THREAD_OUTPUT` for parallel writes.

**User-uploaded data** should flow through a landing zone with schema profiling (DuckDB's `SUMMARIZE`), column mapping (UI-driven mapping tool for uploaded column names → canonical schema), validation rules (code format regex, date range plausibility, referential integrity), and a **quarantine pattern** routing invalid records to a `_quarantine` table with rejection reason codes.

**Data lineage** should use the **duck_lineage DuckDB extension** which automatically captures and emits OpenLineage events for every query, paired with **Marquez** as the lineage backend. This provides table-level and column-level lineage tracking—critical for tracing code normalization transformations and demonstrating data provenance for regulatory compliance.

**Essential DuckDB extensions** for Aradune: `httpfs` (S3/GCS/Azure Parquet access), `cache_httpfs` (local caching reducing S3 costs by 60%+), `iceberg` (table format), `icu` (Unicode for healthcare text), `json` (semi-structured data), `excel` (state agency uploads), `spatial` (geographic cost adjustments).

---

## Conclusion: five architectural principles for bulletproof Medicaid data

This research reveals that Medicaid data quality is not a problem to be solved but a condition to be managed. Five principles should guide Aradune's architecture:

**Never trust a single source.** Expenditure analysis requires triangulating TAF, CMS-64, and supplemental payment data. Provider analysis requires cross-referencing NPPES, PECOS, and HCRIS. Drug analysis requires linking SDUD, FDA NDC Directory, RxNorm, and NADAC. Build validation checks that compare sources against each other, not just against internal consistency rules.

**State-level variation is the dominant quality dimension.** Illinois needs custom deduplication. Arkansas data may be missing entirely for early years. Managed care encounter completeness ranges from excellent to absent depending on the state. Every analysis pipeline should begin with a DQ Atlas lookup and carry state-quality metadata through every transformation.

**Schema evolution is inevitable—design for it from day one.** T-MSIS added fields moving from MSIS. Hospital MRFs have no stable schema. CMS-64 categories evolve. NPPES changed to Version 2 format in March 2026. Use Iceberg (or DuckLake) for managed schema evolution, `union_by_name` for Parquet reads, and SCD Type 2 patterns for all reference data.

**Test adversarially, not just defensively.** Soda Core checks and dbt tests catch known issues. Hypothesis property-based testing and chaos engineering catch unknown issues—schema drift mid-pipeline, encoding corruption, volume spikes, near-duplicate records that slip past exact-match deduplication. Generate realistic test data with SDV trained on actual claims distributions, and test at 10x expected volume.

**Version everything, including the context.** Store the TAF release version, RBRVS conversion factor effective date, FMAP rate, Medical Care CPI value, and DQ Atlas quality rating alongside every analytical result. When someone asks "why did spending in State X jump 30% in 2024?" the answer may be a data quality change, not a policy change—and the metadata to distinguish between the two must be readily available.