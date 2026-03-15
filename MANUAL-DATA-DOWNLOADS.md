# Aradune — Manual Data Downloads Required

> These datasets cannot be automated due to registration requirements, WAF blocks, or portal logins.
> Priority-ordered by analytical impact. Total estimated time: ~45-60 minutes.

---

## Priority 1: Medicaid Expansion Dates (5 min)

**What:** Which states expanded Medicaid, when, and current status (40 expansion states + 10 non-expansion).

**Why:** Essential instrument for difference-in-differences and staggered adoption designs. Currently missing from `dim_state` — blocks the strongest causal inference methods.

**Where:** https://www.kff.org/medicaid/issue-brief/status-of-state-medicaid-expansion-decisions-interactive-map/

**What to do:**
1. Open the KFF page
2. Scroll to the table listing all states with expansion status and dates
3. Copy the full table into a CSV or just paste it into a text file
4. Save to: `data/raw/policy/medicaid_expansion_dates.csv`
5. Columns needed: `state`, `expansion_status` (Adopted/Not Adopted), `expansion_date` (month/year), `expansion_type` (ACA, 1115 waiver, etc.)

**I will then:** Add `expansion_date`, `expansion_status`, and `expansion_type` columns to `dim_state` and build an ETL script.

---

## Priority 2: Historical FMAP Rates (10 min)

**What:** Federal Medical Assistance Percentage by state, FY2010-2022 (we currently only have FY2023-2026).

**Why:** Needed for pre-2023 fiscal analysis, PHE enhanced FMAP impact studies, and the rate-quality DiD design using FMAP as treatment variable.

**Where:** https://www.macpac.gov/subtopic/matching-rates/

**What to do:**
1. Open the MACPAC matching rates page
2. Look for the "Exhibit 6" or "Federal Medical Assistance Percentages" tables
3. Download the Excel/PDF for FY2010-2022 (or as far back as available)
4. Also check: https://aspe.hhs.gov/topics/health-health-care/medicaid/federal-medical-assistance-percentages-fmap
5. Save to: `data/raw/state_fiscal/fmap_historical/`

**I will then:** Parse into parquet, extend `fact_fmap_historical` from 4 years to 15+ years.

---

## Priority 3: Area Deprivation Index (5 min)

**What:** Block-group-level socioeconomic deprivation ranking (1-100 national, 1-10 state decile) based on 17 ACS indicators. ~220,000 block groups.

**Why:** Granular SDOH layer for maternal health deserts, equity analysis, provider access scoring. More granular than SVI (which we already have at county level).

**Where:** https://www.neighborhoodatlas.medicine.wisc.edu/

**What to do:**
1. Click "Get Started" or "Download ADI Data"
2. Register for a free account (requires name, email, institution — use "Aradune Policy" or personal)
3. After registration, download the national ADI CSV
4. Save to: `data/raw/social_determinants/adi_2024.csv`

**I will then:** Build `scripts/build_lake_adi.py`, create `fact_adi_block_group` table.

---

## Priority 4: State Fee Schedules — KS, NJ, TN, WI (15-20 min)

These 4 states require portal logins or manual navigation. Currently have 42/47 states; these would bring us to 46/47.

### Kansas
**Where:** Kansas MMIS Provider Portal — https://portal.kmap.state.ks.us/
**What to do:**
1. Navigate to provider resources → fee schedules
2. Look for the physician/professional fee schedule (HCPCS-based)
3. Download the current fee schedule (PDF or Excel)
4. Save to: `data/raw/fee_schedules/KS/`

### New Jersey
**Where:** NJ Medicaid FFS Fee Schedule — https://www.nj.gov/humanservices/dmahs/info/resources/
**What to do:**
1. Look for "Fee-for-Service Fee Schedules" or "Provider Billing Supplements"
2. Download the physician/professional services fee schedule
3. Save to: `data/raw/fee_schedules/NJ/`

### Tennessee
**Where:** TennCare — https://www.tn.gov/tenncare/providers.html
**What to do:**
1. Navigate to provider information → fee schedules
2. TennCare is mostly managed care (97%+), so FFS fee schedule may be limited
3. Look for any published rate schedules or billing guides
4. Save to: `data/raw/fee_schedules/TN/`
5. Note: TN may not have a traditional FFS fee schedule — if so, skip

### Wisconsin
**Where:** ForwardHealth Portal — https://www.forwardhealth.wi.gov/
**What to do:**
1. Navigate to Provider → Max Fee Schedules
2. Download the physician/professional max fee schedule
3. Save to: `data/raw/fee_schedules/WI/`

**I will then:** Run the fee schedule scraper/parser to extract rates and add to `fact_medicaid_rate`.

---

## Priority 5: AHRQ SDOH Database (5 min)

**What:** County-level social determinants of health — education, employment, housing, transportation, food access. Compiled from ACS, USDA, Census.

**Why:** Richer SDOH layer than SVI alone. Enables multivariate vulnerability scoring.

**Where:** https://www.ahrq.gov/sdoh/data-analytics/sdoh-data.html

**What to do:**
1. Open the page in your browser (the download link is blocked by a WAF for automated tools)
2. Click "Download SDOH Data" or navigate to the data files section
3. Download the county-level CSV (latest year available, likely 2020 or 2021)
4. Save to: `data/raw/social_determinants/ahrq_sdoh_county.csv`

**I will then:** Build `scripts/build_lake_ahrq_sdoh.py`, create `fact_ahrq_sdoh_county`.

---

## Priority 6: MCPAR Reports (30+ min, lowest priority)

**What:** Managed Care Program Annual Reports — plan-level data on prior authorizations, grievances, appeals, quality measures, network adequacy, encounter data validation.

**Why:** The only source of plan-level operational data. High value but high extraction effort (PDF format).

**Where:** https://www.medicaid.gov/medicaid/managed-care/managed-care-program-annual-report

**What to do:**
1. Download the 2023 and/or 2024 MCPAR reports (PDFs)
2. There may be a structured data file — check if CMS has released an Excel/CSV version
3. Save to: `data/raw/managed_care/mcpar/`

**I will then:** Use Claude PDF extraction to parse into structured tables.

**Note:** Only do this if you have time. The PDFs are complex multi-page reports and extraction is labor-intensive.

---

## After Downloads

Once you've saved the files to the paths above, let me know and I'll:
1. Build ETL scripts for each new dataset
2. Register them as lake views in `db.py`
3. Add ontology YAML definitions
4. Update `dim_state` with expansion dates
5. Extend `fact_fmap_historical` with pre-2023 data
6. Sync to R2 and redeploy

The expansion dates (#1) and historical FMAP (#2) are by far the highest leverage — they unlock causal inference methods that are currently blocked.
