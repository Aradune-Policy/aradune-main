# Aradune Data Integrity Audit Report (Prompt 1)
## Bronze -> Silver -> Gold ETL Sweep

**Audit Date:** 2026-03-13
**Auditor:** Claude Opus 4.6 (forensic mode)
**Scope:** 107 ETL scripts across 19 domains, 720+ parquet files, 702 DuckDB views
**Anchor Facts:** FL FY2025-26 FMAP 57.22%, FL enrollment ~4.2M, CPT 99202 base $55.15

---

## Executive Summary

| Severity | Count | Fixed This Session | Remaining |
|----------|-------|--------------------|-----------|
| **CRITICAL** | 17 | 9 | 8 |
| **WARNING** | 52 | 0 | 52 (most are hardcoded-value or schema-fragility issues) |
| **INFO** | 44 | 0 | 44 (documentation/cosmetic) |

### Critical Issues Fixed (9)
1. fact_mc_enrollment state_code: full names with footnotes -> 2-letter codes
2. NH ownership_pct: 100% NULL from '%' suffix in TRY_CAST -> REPLACE fix
3. FMR category names: lstrip("TCM-") char-stripping bug -> regex fix
4. TX NDC: BIGINT losing leading zeros -> LPAD VARCHAR(11)
5. SDUD XX: national total rows leaking into 4 state-level endpoints
6. Opioid insights: FIPS codes in state_code column -> dim_state JOIN
7. ZIP-to-state: PR/VI overlap in SAFMR mapping -> separated ranges
8. NPPES: missing snapshot_date/source columns -> added
9. Provider network: duplicate fact_provider_affiliation -> deprecated

### Critical Issues Remaining (8) - Need Decision or Manual Action
1. **Eligibility enrollment snapshot: only 25/51 states** - API pagination not implemented. Need to decide: implement pagination or document limitation.
2. **66.9% of fact_medicaid_rate missing effective_date** - Source data lacks dates for most states. Structural gap, not fixable without better source files.
3. **SDUD schema inconsistency across 3 vintages** - Different column names (state/state_code, num_prescriptions/number_of_prescriptions). Need to decide: rename columns or document.
4. **CCW chronic conditions data hardcoded from PDF** - 550+ data points manually transcribed. Need to decide: find machine-readable source or add validation checksums.
5. **FMAP column positions hardcoded against MACPAC Excel** - Authoritative source. Need to decide: add dynamic header detection or document risk.
6. **Medicare PUF download URLs date-stamped** - Will 404 on next CMS release. Need to decide: add URL discovery or accept manual update.
7. **HCRIS duplicate CCNs from CHOW** - 197 hospital + 2,114 SNF duplicate CCN+year combos. Need to decide: deduplicate (keep latest report period) or flag.
8. **Scorecard tables use SELECT * from raw CSVs** - Raw column names flow through to gold. Need to decide: add explicit column lists or accept.

---

## Domain-by-Domain Findings

### 1. ENROLLMENT (5 scripts)

| # | Finding | Severity | Script | Status |
|---|---------|----------|--------|--------|
| E1 | fact_mc_enrollment state_code has full names + footnotes | CRITICAL | build_lake_enrollment.py | **FIXED** |
| E2 | fact_eligibility: 49.5% duplicate rows (Preliminary+Updated coexist) | WARNING | build_lake_enrollment.py | Document |
| E3 | fact_new_adult/unwinding: INNER JOIN silently drops unmatched states | WARNING | build_lake_enrollment.py | Document |
| E4 | fact_unwinding: hardcoded CMS column names are fragile | WARNING | build_lake_enrollment.py | Accept |
| E5 | Eligibility enrollment snapshot: only 25/51 states (no pagination) | CRITICAL | build_lake_eligibility_enrollment_snapshot.py | Decision needed |
| E6 | Two enrollment tables disagree by ~550K for FL | CRITICAL | Multiple | Document (methodology difference) |
| E7 | reporting_period type mismatch (BIGINT vs VARCHAR) | INFO | Multiple | Low priority |

### 2. FMAP / FISCAL (3 scripts)

| # | Finding | Severity | Script | Status |
|---|---------|----------|--------|--------|
| F1 | FL FY2026 FMAP = 57.22% confirmed across all 3 sources | PASS | All FMAP scripts | Verified |
| F2 | snapshot_date stored as VARCHAR not DATE | WARNING | build_lake_macpac_fmap_multiyear.py | Low priority |
| F3 | E-FMAP data only through FY2024 | INFO | build_lake_macpac_fmap_multiyear.py | Source limitation |
| F4 | FMAP column positions hardcoded against MACPAC Excel | CRITICAL | build_lake_state_fiscal.py | Decision needed |
| F5 | CMS-64 data row start hardcoded at index 7 | WARNING | build_lake_cms64_multiyear.py | Accept |
| F6 | "Mass. Blind" maps to MA, creating duplicate rows | WARNING | build_lake_cms64_historical.py | Low priority |

### 3. RATES / FEE SCHEDULES (4 scripts)

| # | Finding | Severity | Script | Status |
|---|---------|----------|--------|--------|
| R1 | TX NDC stored as BIGINT, leading zeros lost | CRITICAL | build_lake_state_mac.py | **FIXED** |
| R2 | 66.9% of fact_medicaid_rate missing effective_date | CRITICAL | build_facts.py | Decision needed |
| R3 | FL has 70 duplicate (code, modifier) combos across fee schedules | WARNING | build_facts.py | Decision needed |
| R4 | dim_procedure has 10 triplicated codes | WARNING | build_lake_fee_schedules_computed.py | Low priority |
| R5 | FL Regular CF inconsistency: CLAUDE.md vs sync-fee-schedules.py | WARNING | sync-fee-schedules.py | Documentation |
| R6 | PFS RVU parser uses positional indexing | WARNING | build_lake_pfs_rvu.py | Accept |
| R7 | State MAC tables lack snapshot partitioning | WARNING | build_lake_state_mac.py | Low priority |
| R8 | 99202 base rate: $58.45 (CY2026), not $55.15 (CY2025) | WARNING | Data vintage | Document |

### 4. HOSPITALS (6 scripts)

| # | Finding | Severity | Script | Status |
|---|---------|----------|--------|--------|
| H1 | NH ownership_pct 100% NULL (% suffix in TRY_CAST) | CRITICAL | build_lake_hai_ownership.py | **FIXED** |
| H2 | FMR category names mangled (lstrip bug) | CRITICAL | build_lake_supplemental.py | **FIXED** |
| H3 | HCRIS: 197 duplicate CCN+year combos (CHOW) | WARNING | build_lake_hcris.py | Decision needed |
| H4 | SNF: 2,114 duplicate CCN+year combos | WARNING | build_lake_hcris.py | Decision needed |
| H5 | Inconsistent provider key naming (provider_id vs provider_ccn) | WARNING | Multiple | Document |
| H6 | SDP preprint data 100% hardcoded | WARNING | build_lake_supplemental_p2.py | Accept |
| H7 | POS Other: incompatible category codes in UNION | WARNING | build_lake_quality.py | Low priority |

### 5. NURSING FACILITIES (4 scripts)

| # | Finding | Severity | Script | Status |
|---|---------|----------|--------|--------|
| N1 | PBJ extreme HPRD outliers (census=1) | WARNING | build_lake_pbj.py | Low priority |
| N2 | PBJ total_nursing_hrs NULL propagation (missing COALESCE) | WARNING | build_lake_pbj.py | Low priority |
| N3 | 6% of Five-Star facilities have no SNF cost data | WARNING | Cross-table | Expected |

### 6. PHARMACY (2 scripts)

| # | Finding | Severity | Script | Status |
|---|---------|----------|--------|--------|
| P1 | SDUD XX national total not filtered (160K rows, $56B) | CRITICAL | build_lake_sdud_2025.py | **FIXED** (route level) |
| P2 | SDUD XX not filtered in historical (1.1M rows) | CRITICAL | build_lake_sdud_historical.py | **FIXED** (route level) |
| P3 | 3 different schemas across SDUD family | CRITICAL | Multiple | Decision needed |
| P4 | 50% of SDUD 2025 rows have null spending (suppression) | WARNING | build_lake_sdud_2025.py | Expected |

### 7. BEHAVIORAL HEALTH (5 scripts)

| # | Finding | Severity | Script | Status |
|---|---------|----------|--------|--------|
| B1 | Opioid prescribing: state_code has FIPS, not 2-letter codes | CRITICAL | build_lake_opioid_prescribing.py | **FIXED** (route level) |
| B2 | NSDUH US aggregate rows included (inflates state counts) | WARNING | build_lake_nsduh_2024/2022.py | Low priority |
| B3 | NSDUH CSV parsed with split(",") not csv module | WARNING | build_lake_behavioral_health.py | Low priority |
| B4 | TEDS sex field fallback order reversed between A and D | WARNING | build_lake_samhsa_v2.py | Low priority |
| B5 | MDS/NH tables in behavioral_health script (wrong location) | WARNING | build_lake_behavioral_health.py | Cosmetic |

### 8. WORKFORCE (4 scripts)

| # | Finding | Severity | Script | Status |
|---|---------|----------|--------|--------|
| W1 | HPSA: 43% of rows are "Proposed For Withdrawal" | WARNING | build_lake_hpsa.py | Document |
| W2 | NP/PA supply: headcount conflated with FTE | WARNING | build_lake_hrsa_workforce.py | Document |
| W3 | BLS national table requires spatial extension (silent skip) | WARNING | build_lake_bls.py | Accept |
| W4 | BLS OEWS MSA uses different tech stack (pandas vs DuckDB) | WARNING | build_lake_bls_oews_msa.py | Cosmetic |

### 9. ECONOMIC (5 scripts)

| # | Finding | Severity | Script | Status |
|---|---------|----------|--------|--------|
| EC1 | ZIP-to-state PR/VI overlap | CRITICAL | build_lake_economic_v2.py | **FIXED** |
| EC2 | Census API column name dependency | WARNING | build_lake_census.py | Accept |
| EC3 | SNAP/TANF positional column parsing fragile | WARNING | build_lake_snap_tanf.py | Accept |
| EC4 | Tax Foundation abbreviation map brittleness | WARNING | build_lake_state_fiscal.py | Accept |

### 10. QUALITY (3 scripts)

| # | Finding | Severity | Script | Status |
|---|---------|----------|--------|--------|
| Q1 | Scorecard tables use SELECT * from raw CSVs | CRITICAL | build_lake_scorecard.py | Decision needed |
| Q2 | Quality Core Set: silent drop of unmapped states | WARNING | build_lake_quality_core_set_historical.py | Low priority |
| Q3 | HAC measure missing state_code column | WARNING | build_lake_quality.py | Low priority |

### 11. PROGRAM INTEGRITY (3 scripts)

| # | Finding | Severity | Script | Status |
|---|---------|----------|--------|--------|
| PI1 | LEIE date parsing only handles YYYYMMDD format | WARNING | build_lake_leie.py | Accept |
| PI2 | MFCU column positions hardcoded | WARNING | build_lake_mfcu_fy2024.py | Accept |
| PI3 | Federal Register pagination edge case | WARNING | build_lake_federal_register.py | Low priority |

### 12. LTSS/HCBS (3 scripts)

| # | Finding | Severity | Script | Status |
|---|---------|----------|--------|--------|
| LH1 | HCBS waiver uses state_name not state_code | WARNING | build_lake_chip_hcbs.py | Low priority |
| LH2 | Section 1115 waivers: no schema enforcement | WARNING | build_lake_section_1115_waivers.py | Accept |
| LH3 | MACPAC HCBS column positions hardcoded | WARNING | build_lake_macpac_hcbs_payment_scan.py | Accept |

### 13. MEDICARE (2 scripts)

| # | Finding | Severity | Script | Status |
|---|---------|----------|--------|--------|
| M1 | Medicare PUF download URLs date-stamped | CRITICAL | build_lake_medicare_puf.py | Decision needed |
| M2 | CCW chronic conditions: 550+ data points hardcoded from PDF | CRITICAL | build_lake_chronic_conditions.py | Decision needed |
| M3 | MCBS code-to-label mapping hardcoded | WARNING | build_lake_chronic_conditions.py | Accept |

### 14. PROVIDER NETWORK (3 scripts)

| # | Finding | Severity | Script | Status |
|---|---------|----------|--------|--------|
| PN1 | Duplicate provider_reassignment tables | CRITICAL | build_lake_provider_network.py | **FIXED** |
| PN2 | NPPES missing snapshot_date/source columns | WARNING | build_lake_nppes.py | **FIXED** |
| PN3 | NPPES hardcoded source filename with date range | WARNING | build_lake_nppes.py | Accept |
| PN4 | GME CCN-to-state mapping hardcoded | WARNING | build_lake_provider_network.py | Accept |

---

## Cross-Cutting Patterns

### Pattern 1: Hardcoded Column Positions (7 scripts)
Scripts that parse Excel/CSV by positional index rather than header name:
- build_lake_state_fiscal.py (FMAP, Tax Foundation)
- build_lake_mfcu_fy2024.py (statistical chart)
- build_lake_macpac_hcbs_payment_scan.py (19 columns)
- build_lake_chronic_conditions.py (Exhibits 17/21/29)
- build_lake_snap_tanf.py (SNAP)
- build_lake_pfs_rvu.py (RVU)
- build_lake_cms64_multiyear.py (FMR)

**Risk:** All fragile to source layout changes. When CMS/MACPAC/BLS publish updated files, any column addition/removal breaks parsing silently.

### Pattern 2: Hardcoded Data Years (8 scripts)
Scripts with hardcoded fiscal_year, data_year, or reference_year values that must be manually updated.

### Pattern 3: Missing State Count Validation
Only 2 of 50+ scripts validate that the expected number of states (50+DC+territories) appears in output. Most scripts can silently produce partial data.

### Pattern 4: snapshot_date Type Inconsistency
Some scripts use DATE, others VARCHAR, one omits entirely (NPPES, now fixed). Cross-table metadata queries may fail.

---

## Anchor Fact Verification Results

| Anchor Fact | Expected | Actual | Status |
|---|---|---|---|
| FL FMAP FY2025-26 | 57.22% | 57.22% (all 3 sources agree) | **PASS** |
| FL enrollment FY2024-25 | ~4,226,347 | ~4,149,281 (fact_new_adult avg) | **PASS** (-1.8%) |
| FL enrollment (eligibility table) | ~4,226,347 | ~3,585,834 | **WARNING** (-15.2%, different methodology) |
| CPT 99202 base rate (Jan 2025) | $55.15 | $58.45 (CY2026 schedule loaded) | **WARNING** (data vintage is CY2026) |
| FL: no Facility+PC/TC dual coding | 0 violations | 0 violations | **PASS** |
| FL CMS-64 implied FMAP FY2024 | ~57-58% | 58.81% blended | **PASS** (blended > base expected) |
| Hospital count (FY2023) | ~6,100 | 6,040 unique CCNs | **PASS** |
| Nursing facility count | 14,500-16,000 | 14,710 (Five-Star) | **PASS** |
| LEIE total exclusions | 80,000-85,000 | 82,749 | **PASS** |
| Open Payments total | ~$10-13B | $10.83B | **PASS** |
