# Aradune Data Run — Session 18: Gap-Closing Sprint

**Purpose:** Close the highest-value data gaps identified in the Session 17 review. These datasets fill holes in fiscal analysis, drug utilization, managed care, workforce/access, and social determinants — all critical for consulting firm demos and cross-cutting Medicaid intelligence queries.

**Estimated total new rows:** ~5–6M  
**Estimated raw CSV size:** ~2–3 GB  
**Estimated parquet size:** ~200–400 MB  
**Priority order:** Work top-to-bottom. Each tier is ordered by analytical value per byte.

---

## Tier 1: Must-Have (Demo-Critical)

### 1.1 State Drug Utilization Data (SDUD)
- **What:** NDC-level Medicaid prescription drug utilization and spending by state, quarter, and FFS/MCO split.
- **Why:** Enables rebate-adjusted drug cost analysis, opioid tracking, drug class comparisons across states. Only public source with this granularity.
- **Source:** data.medicaid.gov
- **URLs:**
  - 2024: https://data.medicaid.gov/dataset/61729e5a-7aa8-448c-8903-ba3e0cd0ea3c
  - 2023: https://data.medicaid.gov/dataset/d890d3a9-6b00-43fd-8b31-fcba4c8e2909
  - 2022: https://data.medicaid.gov/dataset/200c2cba-e58d-4a95-aa60-14b99736808d
  - 2021: https://catalog.data.gov/dataset/state-drug-utilization-data-2021-f9419
  - 2020: https://catalog.data.gov/dataset/state-drug-utilization-data-2020
  - All years also browsable at: https://www.medicaid.gov/medicaid/prescription-drugs/state-drug-utilization-data
- **Format:** CSV via API or direct download
- **Expected size:** ~600K–800K rows/year × 5 years = ~3.5–4M rows. Largest dataset in this run.
- **Key columns:** State, NDC (labeler + product + package), Drug Name, Quarter, Year, Utilization Type (FFSU/MCOU), Number of Prescriptions, Total Amount Reimbursed, Units Reimbursed, Suppression Flag
- **ETL notes:**
  - Download each year separately. The API supports JSON/CSV export.
  - Suppressed cells (Rx count < 11) are marked — keep the flag, don't drop rows.
  - Consider creating an aggregate table: state × therapeutic_class × year with total Rx and total spending (requires NDC-to-class crosswalk — RED BOOK is proprietary, but FDA's NDC directory + first-databank-free alternatives can approximate).
  - Store as: `data/lake/drug_utilization/sdud_YYYY/` per year, plus `sdud_combined/` rolled up.
- **Domain:** `drug_utilization` (new or merge into existing drug tables)

### 1.2 CMS-64 Medicaid Financial Management Data
- **What:** Quarterly actual expenditures by state and service category, used to compute federal financial participation (FFP). The definitive source for how much each state actually spends on Medicaid.
- **Why:** Enables state spending comparisons, expansion vs. non-expansion cost analysis, service category breakdowns. Critical denominator for rate adequacy work.
- **Source:** Medicaid.gov + data.medicaid.gov
- **URLs:**
  - By State: https://data.medicaid.gov (search "Medicaid Financial Management Data")
  - National Totals: https://catalog.data.gov/dataset/medicaid-financial-management-data-national-totals-bce32
  - New Adult Group Expenditures: https://catalog.data.gov/dataset/medicaid-cms-64-new-adult-group-expenditures-d89ee
  - Historical Excel zips (FY 1997–2024): https://www.medicaid.gov/medicaid/financial-management/state-budget-expenditure-reporting-for-medicaid-and-chip/expenditure-reports-mbes/cbes
- **Format:** CSV on data.medicaid.gov; Excel zips for historical
- **Expected size:** ~50 states × ~30 service categories × 10 fiscal years = ~15K–20K rows. Tiny but essential.
- **Key columns:** State, Fiscal Year, Quarter, Service Category, Total Computable, Federal Share, State Share
- **ETL notes:**
  - The data.medicaid.gov version is cleanest. Use API if possible.
  - Historical Excel files need parsing — each FY is a separate zip containing spreadsheets with fixed layouts.
  - Prioritize FY 2016–2024 for trend analysis. Go back further only if time allows.
  - Create both a raw quarterly table and an annual rollup by state × service category.
  - Store as: `data/lake/state_fiscal/cms64_expenditures/`
- **Domain:** `state_fiscal` (existing)

### 1.3 Medicare PFS National Rates (for Medicare-to-Medicaid Rate Comparison)
- **What:** Medicare Physician Fee Schedule national payment amounts by HCPCS code.
- **Why:** Combined with your 47-state Medicaid fee schedules, this enables code-level Medicare-to-Medicaid rate ratios — the single most asked-for metric in Medicaid rate adequacy work. KFF's Medicaid-to-Medicare fee index is the headline number; this lets you build the detail underneath it.
- **Source:** CMS
- **URLs:**
  - 2025 PFS: https://www.cms.gov/medicare/payment/fee-schedules/physician/pfs-relative-value-files
  - Also: https://data.cms.gov (search "Physician Fee Schedule")
- **Format:** CSV / ZIP
- **Expected size:** ~15K–16K HCPCS codes × a handful of pricing columns. Under 5 MB.
- **Key columns:** HCPCS, Modifier, Non-Facility Price, Facility Price, Locality, GPCI, Work RVU, PE RVU, MP RVU, Conversion Factor
- **ETL notes:**
  - You may already have RVU data from Session 13b (PFS RVU). Verify overlap.
  - If RVU file exists, you just need the national payment amount file (PFREVL + GPCI applied).
  - The key deliverable is a JOIN table: HCPCS × state with Medicaid rate, Medicare rate, and ratio.
  - This is more of an analytics build than a data ingestion — the raw Medicare file is tiny.
  - Store as: `data/lake/medicare/pfs_national_rates_2025/`
- **Domain:** `medicare` (existing)

### 1.4 Medicaid Managed Care Enrollment by Program and Plan
- **What:** Plan-specific enrollment statistics including plan name, parent organization, MCO/PIHP/PAHP type, geographic area, Medicaid-only and dual-eligible enrollment counts.
- **Why:** Over 75% of Medicaid beneficiaries are in managed care. This fills the biggest hole in the managed care domain — who's enrolled where.
- **Source:** data.medicaid.gov
- **URLs:**
  - All years: https://data.medicaid.gov (search "Managed Care Enrollment by Program and Plan")
  - Filtered 2024 data: https://data.medicaid.gov/dataset/0bef7b8a-c663-5b14-9a46-0b5c2b86b0fe/data
  - Landing page: https://www.medicaid.gov/medicaid/managed-care/enrollment-report
- **Format:** CSV via data.medicaid.gov API
- **Expected size:** ~2K–5K rows per year (all plans × all states). Multiple years available (2014–2024). Total maybe 20K–40K rows.
- **Key columns:** State, Program Name, Program Type, Plan Name, Parent Organization, Managed Care Entity Type, Geographic Area, Medicaid-Only Enrollment, Dual Eligible Enrollment, Total Enrollment, Reimbursement Arrangement
- **ETL notes:**
  - Pull all available years for trend analysis.
  - Also grab Table 1 (state summary), Table 2 (enrollment by program type), and Table 4 (share in MC).
  - Watch for suppression: "0" may mean suppressed (<10), not actually zero.
  - Store as: `data/lake/managed_care/mc_enrollment_by_plan/`
- **Domain:** `managed_care` (may need to create or expand from existing MC tables)

---

## Tier 2: High-Value Reference Data (Small but Powerful)

### 2.1 HRSA Area Health Resource File (AHRF)
- **What:** County-level data on 6,000+ variables covering health professions, facilities, population, economics, training, utilization, and environment. Sourced from 50+ federal datasets.
- **Why:** One-stop shop for workforce and access analysis at county level. Replaces needing to pull AMA, ADA, Census, and BLS data separately.
- **Source:** HRSA
- **URL:** https://data.hrsa.gov/data/download (look for "Area Health Resources Files")
- **Format:** CSV and SAS. The county-level file is the primary target.
- **Expected size:** ~3,230 rows (counties) × 6,000+ columns = very wide but only ~3,230 rows. File is ~17–30 MB.
- **ETL notes:**
  - This is a WIDE file. You'll want to either:
    - (a) Ingest as-is and let DuckDB handle column projection, or
    - (b) Select key variable families: physician supply, dentist supply, hospital beds, population, poverty, insurance coverage, etc.
  - Comes with a data dictionary Excel file — parse it for column name mappings (columns are coded like F08921-13).
  - The 2024-2025 release is the most current.
  - Store as: `data/lake/workforce/ahrf_county/`
- **Domain:** `workforce` (new or expand existing provider domain)

### 2.2 CDC/ATSDR Social Vulnerability Index (SVI)
- **What:** Census tract-level composite index ranking communities on 16 social factors (poverty, housing, transportation, minority status, etc.) grouped into 4 themes.
- **Why:** Essential for any analysis connecting Medicaid policy to population vulnerability. Every consulting deck on access or equity uses SVI.
- **Source:** CDC/ATSDR
- **URL:** https://www.atsdr.cdc.gov/place-health/php/svi/data-documentation-download.html
- **Format:** CSV
- **Expected size:** ~74,000 census tracts × ~50 columns. About 30–50 MB.
- **ETL notes:**
  - Also available at county level (~3,200 rows) — grab both.
  - Key columns: FIPS, RPL_THEMES (overall ranking), RPL_THEME1-4 (theme rankings), plus the raw indicator values.
  - 2022 is the most recent release.
  - Store as: `data/lake/social_determinants/svi_tract/` and `svi_county/`
- **Domain:** `social_determinants` (new)

### 2.3 USDA Food Access Research Atlas
- **What:** Census tract-level indicators of food access including low-access flags at various distance thresholds, vehicle availability, SNAP participation, and poverty rates.
- **Why:** SDOH layer for maternal health, pediatric, and chronic disease analyses. 
- **Source:** USDA Economic Research Service
- **URL:** https://www.ers.usda.gov/data-products/food-access-research-atlas/download-the-data/
- **Format:** Excel/CSV
- **Expected size:** ~74,000 tracts × ~30 columns. ~50 MB.
- **ETL notes:**
  - Straightforward flat file. Join on FIPS tract code.
  - Store as: `data/lake/social_determinants/food_access/`
- **Domain:** `social_determinants` (new)

### 2.4 Section 1115 Waiver Tracker
- **What:** Status of Section 1115 Medicaid waivers by state — approved, pending, expired — with effective dates, key provisions, and topics (work requirements, block grants, DSH, HCBS, etc.).
- **Why:** Policy context layer for any state comparison. Essential for understanding why states differ.
- **Sources:**
  - KFF: https://www.kff.org/medicaid/issue-brief/medicaid-waiver-tracker/ (may need scraping)
  - Medicaid.gov: https://www.medicaid.gov/medicaid/section-1115-demonstrations/demonstrations-and-waivers (official but less structured)
  - MACPAC: https://www.macpac.gov/subtopic/section-1115-waivers/
- **Format:** Varies — KFF is HTML tables, Medicaid.gov has PDFs and a state search.
- **Expected size:** ~100–200 rows (state × waiver combinations). Trivial size.
- **ETL notes:**
  - KFF's tracker is the most analytically useful but may require scraping or manual extraction.
  - Focus on: state, waiver name, status, approval date, expiration date, key topics/provisions.
  - Store as: `data/lake/policy/section_1115_waivers/`
- **Domain:** `policy` (new or merge into existing KFF domain)

---

## Tier 3: Nice-to-Have (If Time Allows)

### 3.1 Area Deprivation Index (ADI)
- **What:** Block-group-level socioeconomic deprivation ranking (1–100 national, 1–10 state decile) based on 17 ACS indicators.
- **Source:** University of Wisconsin Neighborhood Atlas
- **URL:** https://www.neighborhoodatlas.medicine.wisc.edu/
- **Format:** CSV (requires free registration)
- **Expected size:** ~220,000 block groups × ~10 columns. ~30 MB.
- **Store as:** `data/lake/social_determinants/adi_block_group/`

### 3.2 Medicaid MBES/CBES Expenditure Detail
- **What:** More granular than CMS-64 summary — breaks spending into specific benefit categories.
- **Note:** If CMS-64 summary data from Tier 1 is sufficient, skip this. This is the deeper cut.
- **Source:** data.medicaid.gov
- **Expected size:** Small (state × category × quarter).

### 3.3 Historical SDUD Backfill (2015–2019)
- **What:** Extends SDUD to 10 years for long-term trend analysis.
- **Expected size:** ~3–4M additional rows.
- **Note:** Only do this if Tier 1 SDUD ingestion goes smoothly and you have time.

### 3.4 MCPAR Data (Managed Care Program Annual Reports)
- **What:** Plan-level data on prior authorizations, grievances, appeals, quality measures, network adequacy.
- **Source:** https://www.medicaid.gov/medicaid/managed-care/managed-care-program-annual-report
- **Note:** 2023 and 2024 reports are now available on Medicaid.gov. Data is in PDF/structured report format — may need manual extraction or targeted scraping. High value but high ETL effort.

---

## Infrastructure Notes

### Ontology Updates
New domains to register:
- `managed_care` — if not already a top-level domain, elevate it
- `social_determinants` — new domain for SVI, food access, ADI
- `policy` — for waiver tracker and any future SPA/regulatory data

Existing domains to update:
- `drug_utilization` — add SDUD tables
- `state_fiscal` — add CMS-64 expenditure tables
- `medicare` — add PFS national rates (if not already covered by RVU data)
- `workforce` — add AHRF

### Integration Tests to Add
- SDUD: row counts per year, state code validation, NDC format (5-4-2), no negative Rx counts
- CMS-64: all 50 states + DC present per fiscal year, spending > 0 for major categories
- AHRF: exactly ~3,230 county rows, FIPS code format validation
- SVI: tract FIPS 11-digit format, RPL values between 0–1
- MC Enrollment: plan enrollment counts non-negative, state codes valid

### db.py Registration
Each new table needs a `fact_name` entry. Expected new entries: ~15–25 depending on how you split SDUD years and summary tables.

### Docker Size Impact
This run adds maybe 200–400 MB of parquet to the lake. Total would go from 3.0 GB to ~3.2–3.4 GB. Well within current Docker image approach limits. No architecture change needed yet.

---

## Execution Order (Recommended)

1. **SDUD 2020–2024** — largest volume, start downloads first
2. **CMS-64 financials** — small, fast, critical for fiscal analysis
3. **MC Enrollment by Plan** — small, fills the managed care gap
4. **Medicare PFS rates** — tiny download, then build the comparison join
5. **AHRF county file** — download + selective column extraction
6. **SVI tract + county** — straightforward CSV ingest
7. **Food Access Atlas** — same pattern as SVI
8. **Section 1115 waiver tracker** — may need scraping, save for last
9. **Tier 3 items** — only if time remains

---

## Success Criteria

After this run, Aradune should be able to answer:
- "How does Florida's Medicaid spending per enrollee compare to the Southeast region?" (CMS-64)
- "What are the top 10 drugs by Medicaid spending in Florida vs. Texas?" (SDUD)
- "Which MCOs have the largest market share in states with Medicaid expansion?" (MC Enrollment)
- "How do Medicaid rates compare to Medicare for E&M codes across all 47 states?" (PFS + fee schedules)
- "Which Florida counties have the worst provider-to-population ratios?" (AHRF)
- "Show me the most socially vulnerable counties in states that haven't expanded Medicaid." (SVI + KFF expansion status)
- "What Section 1115 waivers are active in Florida and what do they cover?" (Waiver tracker)
