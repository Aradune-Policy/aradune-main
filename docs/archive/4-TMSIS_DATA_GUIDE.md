# T-MSIS Data: Characteristics, Limitations, and Quality Guide
> **Guiding document for Claude Code** — Use this when writing any pipeline, query, analysis, or feature that touches T-MSIS (Transformed Medicaid Statistical Information System) data. Aradune already has 227M rows ingested. This document describes what the data actually is, where it breaks down, and how to work with it reliably.

---

## 1. What T-MSIS Is

T-MSIS (Transformed Medicaid Statistical Information System) is the national Medicaid/CHIP data system replacing the legacy MSIS. States submit claims, enrollment, and provider data to CMS monthly. CMS transforms and publishes the **Analytic Files (TAF)** — the cleaned, research-ready version — through ResDAC.

T-MSIS is the **only national, person-level Medicaid dataset**. It covers approximately **90+ million unique beneficiaries** and spans service years 2014 to present. It is the foundation of virtually all serious Medicaid research and program evaluation.

**Aradune currently has 227M rows of T-MSIS data ingested.** This is likely the OT (Other Services) file, which is the most analytically central file for physician/outpatient claims work.

---

## 2. The Seven TAF File Types

| File | Abbreviation | Description | Key Use Cases |
|---|---|---|---|
| Demographic & Eligibility | DE | One record per beneficiary per month: demographics, eligibility group, enrollment status, managed care plan | Enrollment counts, eligibility analysis, population demographics |
| Other Services | OT | Physician, outpatient hospital, clinic, home health, HCBS claims | Rate comparison, access analysis, utilization trends, CPRA claim counts |
| Inpatient | IP | Inpatient hospital stays | Hospital underpayment, readmissions, DRG analysis |
| Long-Term Care | LT | Nursing facility, ICF/IID, PACE claims | LTSS spending, SNF rate analysis |
| Pharmacy | RX | Drug claims with NDC codes | Drug utilization, formulary analysis, rebate estimation |
| Plan Participation | APL | Managed care plan enrollment by beneficiary-month | MCO enrollment, carve-out tracking |
| Annual Provider | APR | Provider-level summary: enrollment, specialty, location | Provider-to-beneficiary ratios, network adequacy, access metrics |

**For 447.203 CPRA work:** The **OT file** is the primary source for claim counts and beneficiary counts by CPT/HCPCS code. The **DE file** provides the denominator for access metrics. The **APR file** provides provider counts for network adequacy analysis.

---

## 3. Access and Data Lag

### Access requirements
- Requires a **Data Use Agreement (DUA)** approved by CMS through ResDAC (resdac.org)
- DUA approval: typically **6–8+ months**
- Access modes:
  - **VRDC (Virtual Research Data Center)**: secure cloud environment; ~$35K first year, ~$23K renewal; required for for-profit entities
  - **Physical extract**: $1K–$5K per file per year; for academic/nonprofit with approved DUA
- Beneficiary PII (exact DOB, full name, SSN) is redacted; quasi-identifiers (state, county, age, sex, race) are in RIF versions

### Data lag
- **Preliminary TAF data**: released approximately 12–18 months after service year end
- **Final TAF data**: released approximately 24 months after service year end
- Current availability (as of March 2026): Final data through 2022; preliminary data through 2023
- **There is no real-time T-MSIS.** Do not promise or imply current-year claims data from T-MSIS.

### Implication for Aradune
Aradune's 227M rows are likely from a specific service year range. When building features:
- Always surface the service year of the data, not just a retrieval date
- Never describe T-MSIS data as "current" without specifying the vintage year
- For 447.203 CPRA claim counts: use the most recent available year (likely 2022 or 2023 depending on DUA)

---

## 4. The T-MSIS Data Quality Atlas — Use It Before Trusting Any State's Data

The **T-MSIS Data Quality Atlas** (medicaid.gov/dq-atlas) is CMS's own public assessment of data quality for each state, for each topic, for each year. It rates data quality as:

- ✅ **Low Concern** — data is generally reliable for analysis
- ⚠️ **Medium Concern** — use with caution; may need adjustments
- ❌ **High Concern** — significant issues; limit use
- 🚫 **Unusable** — do not use for this topic/state/year

**This is critical.** Before using any state's T-MSIS data for a given analysis, check the DQ Atlas for the relevant topic (claims volume, enrollment, service use, expenditures, etc.).

### Known quality problem areas

**Managed care encounter data completeness** is the single largest quality problem in T-MSIS. States are required to submit encounter claims from MCOs, but:
- Submission completeness varies enormously — some states submit near-complete encounter data; others submit almost none
- Even when submitted, encounter amounts (what the MCO paid the provider) are often missing, set to $0, or set to the capitation rate rather than the actual service-level payment
- The DQ Atlas rates encounter data completeness separately from FFS data completeness — always check both
- **Implication:** For any analysis mixing FFS and managed care claims, encounter data incompleteness will systematically undercount utilization in highly managed care states

**Race and ethnicity data** has significant missingness and inconsistency across states. Do not use T-MSIS race/ethnicity for disparities analysis without extensive QC.

**Provider identifiers** are inconsistently populated. NPI is the preferred identifier, but many states also use state-specific billing provider IDs. NPIs can be missing, invalid (wrong length), or contain placeholder values.

**Diagnosis and procedure codes** in OT claims may be missing for claim types where they're not required (e.g., HCBS claims, transportation, dental in some states). The absence of a procedure code ≠ the absence of a service.

**Payment amounts** can include $0 (denied or placeholder records), negative amounts (adjustments/voids), or extremely high amounts (data entry errors). Always filter for positive, non-zero paid amounts before rate analysis.

---

## 5. File-Specific Data Quality Issues

### OT (Other Services) — primary file for CPRA work

| Field | Common Issues |
|---|---|
| `SRVC_BGNG_DT` (service begin date) | Missing in ~5–15% of records depending on state; some states submit claim date instead |
| `BILL_TYPE_CD` (bill type code) | Inconsistent coding across states; needed to distinguish professional vs. institutional claims |
| `PRCDR_CD` (procedure code) | Missing for HCBS, transportation, dental in some states; may be state-specific code not in CPT/HCPCS |
| `MDCD_PD_AMT` (Medicaid paid amount) | Includes $0 (denied), negatives (adjustments), very high values (errors); filter aggressively |
| `TOT_MDCR_DDCTBL_AMT` | Medicare cost-sharing amounts often zero for dual-eligible claims even when Medicare paid |
| `BNFT_TYPE_CD` (benefit type) | Used to classify claim type but states populate inconsistently |
| `SRVC_PLC_CD` (place of service) | Needed to distinguish office vs. facility rates; sometimes missing |
| `RNDG_PRVDR_NPI` (rendering provider NPI) | Preferred for provider matching; sometimes missing, use `BLNG_PRVDR_NPI` as fallback |

**Critical filtering rules for OT analysis:**
```python
# Always apply these filters before any OT claims analysis
valid_ot_claims = """
    WHERE MDCD_PD_AMT > 0                          -- exclude $0 and denied claims
    AND MDCD_PD_AMT < 50000                         -- exclude obvious data entry errors
    AND ADJSTMT_IND NOT IN ('1', 'V')               -- exclude void/adjustment records (keep original claims)
    AND CLM_STUS_CTGRY_CD IN ('F1', 'F2', 'F3')    -- paid claims only
    AND PRCDR_CD IS NOT NULL                         -- require procedure code
    AND SRVC_BGNG_DT IS NOT NULL                    -- require service date
"""
```

### DE (Demographic & Eligibility) — primary file for enrollment denominators

| Field | Common Issues |
|---|---|
| `ELGBLTY_GRP_CD` (eligibility group) | State-to-federal crosswalk is imperfect; some states use custom groups |
| `CHIP_CD` (CHIP indicator) | Needed to separate Medicaid from CHIP; sometimes missing or mismapped |
| `MNGD_CARE_PLAN_ID` | Used to identify MCO enrollment; may not match plan IDs in APL file |
| `DUAL_ELGBL_CD` (dual eligibility) | Critical for any Medicare-Medicaid analysis; accuracy varies |
| Race/ethnicity fields | High missingness; do not use for disparities analysis without extensive QC |

**For enrollment denominators in CPRA beneficiary counts:**
- Use the DE file to count distinct `MSIS_ID` values with at least one month of Medicaid eligibility in the analysis year
- Filter `CHIP_CD NOT IN ('2', '3')` to exclude CHIP-only enrollees if analysis is Medicaid-only
- Do not double-count beneficiaries who appear in multiple states due to interstate moves

### APR (Annual Provider) — for network adequacy analysis

| Field | Common Issues |
|---|---|
| `PRVDR_NPI` | ~10–15% of records have invalid or missing NPIs |
| `PRVDR_LCTN_*` (location fields) | Addresses are often billing addresses, not service locations |
| `PRVDR_SPCLTY_CD` (specialty) | State-specific specialty codes require crosswalk to standard taxonomy |
| Active/inactive status | File includes historical providers; filter by active enrollment dates |

---

## 6. Known Structural Limitations

### T-MSIS does NOT contain
- **What MCOs actually pay providers** — MCO negotiated rates are proprietary and not in T-MSIS. Encounter amounts reflect what the state paid the MCO (capitation), not what the MCO paid the provider. This is the most significant analytical limitation for any rate-adequacy work in managed care states.
- **Supplemental payments at the provider level** — Base FFS rates are in fee schedules; DSH, UPL, and directed payments are aggregate state-level data not attached to individual claims.
- **Real-time data** — T-MSIS is 12–24 months behind the current service period.
- **Charges** — Claims contain paid amounts, not provider charges. Cost-to-charge analysis requires linking to HCRIS cost reports.
- **Employer-sponsored insurance data** — T-MSIS covers only Medicaid/CHIP. For Marketplace or commercial comparisons, need IQVIA/MarketScan/Merative.

### The FFS vs. managed care data divide
As of 2024, approximately **71% of Medicaid beneficiaries** are enrolled in comprehensive managed care. T-MSIS has **much better data quality for FFS claims** than for managed care encounter data. For states with high MCO penetration (California, Texas, New York — all above 75%), the OT file reflects primarily FFS carve-outs, transition periods, and populations not yet enrolled in MCOs. This matters enormously for:

- **CPRA claim counts**: In a 90% managed care state, the OT file claim counts will represent only ~10% of actual service utilization. This is what 447.203 requires — but users must understand this context.
- **Provider-to-beneficiary ratios**: Using FFS-only claims to calculate access metrics in high-MCO states dramatically understates provider participation (providers billing FFS carve-outs only ≠ all Medicaid-participating providers).
- **Utilization trends**: Declining FFS utilization may reflect MCO enrollment growth, not reduced care access.

**Always surface MCO penetration rate alongside any T-MSIS utilization metric** so users understand the FFS/MCO split.

### HCBS classification is unreliable in raw T-MSIS
The standard T-MSIS HCBS taxonomy codes are sparsely populated — states do not consistently use them. Do NOT rely on `BNFT_TYPE_CD` alone to classify HCBS claims. Use the **ASPE/Mathematica TAF HCBS Taxonomy** instead (documented at aspe.hhs.gov/reports/identifying-classifying-medicaid-hcbs-t-msis), which classifies claims into 20 HCBS service categories using procedure codes + revenue codes + bill type codes + claim type in combination.

---

## 7. The Legacy MSIS and MAX Files

The **Medicaid Statistical Information System (MSIS)** is T-MSIS's predecessor, covering approximately **1999–2015**. The researcher-accessible version was the **Medicaid Analytic eXtract (MAX)** files (calendar years 1999–2013 final, 2014–2015 transitional).

MAX files have simpler structure than TAF but fewer variables and different field names. They are still accessible through ResDAC for historical research. **Do not mix MAX and TAF data in the same time series without careful crosswalk work** — enrollment groups, claim categories, and payment fields are not directly comparable.

For Aradune purposes: T-MSIS TAF is the current standard. MAX is only relevant for multi-decade trend analyses going back before 2014.

---

## 8. Working with T-MSIS in DuckDB

### Partitioning strategy
Parquet files should be organized as:
```
s3://aradune-datalake/tmsis/
  file_type=OT/
    state=FL/
      year=2022/
        quarter=1/
          part-0001.parquet
          part-0002.parquet
```

Query with partition pruning:
```sql
SELECT 
    prcdr_cd,
    COUNT(*) as claim_count,
    COUNT(DISTINCT msis_id) as beneficiary_count,
    AVG(mdcd_pd_amt) as avg_paid
FROM read_parquet(
    's3://aradune-datalake/tmsis/file_type=OT/state=FL/year=2022/**/*.parquet',
    hive_partitioning=true
)
WHERE mdcd_pd_amt > 0
AND prcdr_cd IS NOT NULL
GROUP BY prcdr_cd
ORDER BY claim_count DESC;
```

### Joining OT claims to fee schedule rates
The critical temporal join — match the claim's service date to the fee schedule rate in effect at that time:
```sql
SELECT 
    c.prcdr_cd,
    c.mdcd_pd_amt as paid_amount,
    r.medicaid_rate as scheduled_rate,
    c.mdcd_pd_amt / NULLIF(r.medicaid_rate, 0) as pct_of_scheduled
FROM ot_claims c
JOIN fact_medicaid_rate r
    ON c.prcdr_cd = r.cpt_hcpcs_code
    AND c.state_code = r.state_code
    AND c.srvc_bgng_dt >= r.effective_date
    AND (c.srvc_bgng_dt < r.end_date OR r.end_date IS NULL)
WHERE c.mdcd_pd_amt > 0
AND c.adjstmt_ind NOT IN ('1', 'V');
```

### Provider-to-beneficiary ratio calculation
```sql
-- Count unique participating providers per county per specialty
-- Uses APR file for providers, DE file for beneficiaries
SELECT 
    p.state_code,
    p.county_fips,
    p.prvdr_spclty_cd,
    COUNT(DISTINCT p.prvdr_npi) as provider_count,
    b.beneficiary_count,
    COUNT(DISTINCT p.prvdr_npi) * 1000.0 / NULLIF(b.beneficiary_count, 0) as providers_per_1000
FROM apr_providers p
JOIN (
    SELECT state_code, county_fips, COUNT(DISTINCT msis_id) as beneficiary_count
    FROM de_eligibility
    WHERE year = 2022
    GROUP BY state_code, county_fips
) b ON p.state_code = b.state_code AND p.county_fips = b.county_fips
WHERE p.prvdr_npi IS NOT NULL
AND LENGTH(p.prvdr_npi) = 10
GROUP BY p.state_code, p.county_fips, p.prvdr_spclty_cd, b.beneficiary_count;
```

### Data quality validation queries
Run these on any newly ingested T-MSIS data:
```sql
-- Check for suspicious payment amounts
SELECT 
    'zero_payments' as check_name, 
    COUNT(*) as count,
    COUNT(*) * 100.0 / (SELECT COUNT(*) FROM ot_claims) as pct
FROM ot_claims WHERE mdcd_pd_amt = 0
UNION ALL
SELECT 'negative_payments', COUNT(*), COUNT(*) * 100.0 / (SELECT COUNT(*) FROM ot_claims)
FROM ot_claims WHERE mdcd_pd_amt < 0
UNION ALL
SELECT 'extremely_high_payments', COUNT(*), COUNT(*) * 100.0 / (SELECT COUNT(*) FROM ot_claims)
FROM ot_claims WHERE mdcd_pd_amt > 50000
UNION ALL
SELECT 'missing_procedure_code', COUNT(*), COUNT(*) * 100.0 / (SELECT COUNT(*) FROM ot_claims)
FROM ot_claims WHERE prcdr_cd IS NULL
UNION ALL
SELECT 'missing_service_date', COUNT(*), COUNT(*) * 100.0 / (SELECT COUNT(*) FROM ot_claims)
FROM ot_claims WHERE srvc_bgng_dt IS NULL;

-- Check NPI validity
SELECT 
    COUNT(*) as total_providers,
    COUNT(CASE WHEN prvdr_npi IS NULL THEN 1 END) as missing_npi,
    COUNT(CASE WHEN LENGTH(prvdr_npi) != 10 THEN 1 END) as invalid_length_npi,
    COUNT(CASE WHEN prvdr_npi = '0000000000' THEN 1 END) as placeholder_npi
FROM apr_providers;
```

---

## 9. The HCBS Data Gap

**Home and community-based services (HCBS) are the hardest Medicaid services to analyze in T-MSIS.** Problems include:

1. **Service taxonomy unreliable**: Standard T-MSIS HCBS service type codes sparsely populated. Use the ASPE/Mathematica HCBS taxonomy crosswalk instead.

2. **Rate structure inconsistency**: States pay HCBS on per-hour, per-visit, per-day, and per-unit bases. Direct rate comparison across states requires converting to a common unit — 447.203 mandates average hourly equivalent rates for this reason.

3. **Provider type heterogeneity**: Individual self-directed workers, consumer-directed employees, homemaker agencies, and certified home health agencies are all in the same data — must be separated for meaningful rate analysis.

4. **No Medicare comparable**: Unlike physician services, CMS's Personal Care benefit is minimal and not an appropriate benchmark for state HCBS rates. Cross-state comparison using the 447.203 hourly rate disclosure is the best available method.

5. **Encounter data gaps especially severe**: In managed care states, HCBS is frequently carved in to MCO contracts. Encounter data completeness for HCBS is among the worst in T-MSIS — some states have near-zero HCBS encounter claims despite substantial program spending.

**Aradune's HCBS module should:**
- Use the ASPE HCBS taxonomy for claim classification
- Always surface the FFS/MCO caveat for HCBS utilization data
- Convert all rates to per-hour equivalents
- Separate individual vs. agency providers
- Display hourly rate data alongside the state's FMAP to contextualize the federal-state cost split

---

## 10. Key Identifiers and Crosswalks

### Beneficiary identifier
- `MSIS_ID`: state-assigned Medicaid beneficiary ID. **Not consistent across states.** Do not compare MSIS_IDs between states.
- For cross-state analysis, use demographic matching (age + sex + state + county) or linked datasets
- CMS's synthetic/encrypted beneficiary ID (`BENE_ID` in some files) allows linking across T-MSIS file types within a state and year

### Provider identifier
- `PRVDR_NPI`: the preferred provider identifier (10-digit; use for cross-dataset linking)
- `SRVC_PRVDR_NPI` (rendering provider) is preferred over `BLNG_PRVDR_NPI` (billing provider) for rate analysis
- Link to NPPES for provider type, specialty, address, and taxonomy code
- Link to POS file via CCN for facility characteristics
- Link to PECOS for Medicare enrollment status (relevant for dual-eligible analysis)
- Link to Open Payments via NPI for conflict-of-interest analysis

### Geographic identifiers
- `STATE_CD`: 2-character FIPS state code (note: this is the *beneficiary's* state, not necessarily where service was rendered)
- `COUNTY_CD`: 3-digit county FIPS — use with STATE_CD for full 5-digit FIPS
- ZIP code is often beneficiary's home ZIP, not service site
- For Medicare locality matching: use `county_fips` → CMS locality crosswalk

### Claim type and adjustment indicators
- `ADJSTMT_IND`: '0' = original claim, '1' = void/cancellation, '2' = adjustment. **Always filter to '0' for analysis** or apply voiding logic (if an '0' record is later voided by a '1' record with the same claim ID, exclude both).
- `CLM_TYPE_CD`: identifies FFS vs. encounter (managed care) claim origin

---

## 11. What T-MSIS Cannot Answer (and Where to Go Instead)

| Question | T-MSIS limitation | Better source |
|---|---|---|
| What did MCOs pay providers? | Encounter amounts unreliable | Hospital price transparency MRFs (for hospitals); managed care contracts (rarely public) |
| What are current-year utilization trends? | 12–24 month lag | State MMIS data (not publicly available) |
| What are provider charges? | Not in T-MSIS | HCRIS cost reports (facilities); charge masters (hospitals) |
| How do Medicaid rates compare to commercial rates? | No commercial data | IQVIA, MarketScan, Merative (expensive); hospital price transparency MRFs |
| What are provider-reported quality outcomes? | Not in T-MSIS | HEDIS data (NCQA), Core Set measures (CMS), HRSA UDS |
| What is the actual Medicaid underpayment per hospital? | No cost data | HCRIS Worksheet D-1/D-4 (cost-to-charge) + Worksheet S-2 (Medicaid days) |
| What drugs did a patient take? | RX file exists but formulary context missing | State PDL/formulary documents + SDUD |
| What are HCBS worker wages? | Not captured | BLS OES, PHI (Paraprofessional Healthcare Institute) workforce data |

---

## 12. Summary: Rules for Any Aradune Feature Using T-MSIS

1. **Always specify the service year.** Never describe T-MSIS data as "current."
2. **Check the DQ Atlas** for any state × topic × year before relying on the data.
3. **Always filter:** `mdcd_pd_amt > 0`, `adjstmt_ind = '0'`, `prcdr_cd IS NOT NULL`.
4. **Validate NPIs** before any provider-level analysis: must be 10 digits, not null, not placeholder.
5. **Surface MCO penetration context** alongside any utilization metric.
6. **Separate FFS from encounter claims** (`clm_type_cd`) before any payment analysis.
7. **Use the ASPE HCBS taxonomy** (not raw T-MSIS service type codes) for HCBS classification.
8. **Never imply T-MSIS captures MCO-to-provider payment rates** — it does not.
9. **Use SCD Type 2 logic** for fee schedule temporal joins (match service date to rate effective date).
10. **Document data vintage in every output** — which service year, which TAF release (preliminary or final), and the DQ Atlas rating for the topic.

---

*Sources: ResDAC TAF documentation (resdac.org), T-MSIS Data Quality Atlas (medicaid.gov/dq-atlas), ASPE HCBS Taxonomy report, MACPAC reports on T-MSIS data quality, CMS T-MSIS Analytic Files release notes.*
