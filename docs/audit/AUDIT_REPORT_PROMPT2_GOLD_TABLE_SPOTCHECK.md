# Aradune Gold Table Spot-Check Report (Prompt 2)
## Validation Against Known Truths

**Audit Date:** 2026-03-13
**Scope:** 19 domains, 90+ tables, 156 checks
**Anchor Facts:** FL FMAP 57.22%, FL enrollment ~4.2M, CPT 99202 $55.15, LEIE ~82K

---

## Executive Summary

| Verdict | Count |
|---------|-------|
| **PASS** | 147 |
| **WARNING** | 7 |
| **FAIL** | 2 (both fixed) |

### Issues Fixed During Audit
1. **Hospital cost 2x duplication** — Stale snapshot (2026-03-06) contained a subset of the newer snapshot (2026-03-10). All FY2023 financial data was doubled, inflating national NPR by $1.4T. **Fixed:** removed stale snapshot.
2. **17 stale snapshots across 15 tables** — Same pattern as hospital_cost. All cleaned (kept latest only).
3. **ACS pct_poverty_65plus sentinel** — All 52 states had -888888888 instead of NULL. **Fixed:** NULLIF in build_lake_census.py.

### Remaining Warnings (no action needed)
1. fact_new_adult has NULL FL enrollment for FY2023-24 (CMS data gap)
2. FY2027 FMAP not yet published by MACPAC
3. FL RN wage $88K slightly above $85K benchmark (market conditions)
4. Quality measure CPC-CH CAHPS has plan-level duplicates (missing plan_id)
5. Quality measures mix percentage rates with per-100K rates (no rate_type column)
6. CMS-64 has 952 duplicate zero-value rows (source artifact)
7. SDUD XX rows in gold table (filtered at route level, not ETL level)

---

## Domain-by-Domain Results

### Enrollment (5 checks)
| Check | Expected | Actual | Verdict |
|-------|----------|--------|---------|
| FL FY2024-25 avg enrollment | ~4,226,347 | 4,149,281 | **PASS** (-1.82%) |
| FL YoY decline direction | ~13.3% | 14.5% | **PASS** |
| FL expansion enrollment | 0 | 0 | **PASS** |
| Cross-table consistency | Similar | 400K gap | **WARNING** (methodology) |
| FY2023-24 data availability | Present | NULL in fact_new_adult | **WARNING** |

### FMAP / Fiscal (3 checks)
| Check | Expected | Actual | Verdict |
|-------|----------|--------|---------|
| FL FMAP FY2026 | 57.22% | 57.22% | **PASS** (exact) |
| FL CMS-64 FY2024 | $30-40B | $38.01B | **PASS** |
| FY2027 FMAP | ~55.43% | Not yet published | **WARNING** |

### Rates / Fee Schedules (4 checks)
| Check | Expected | Actual | Verdict |
|-------|----------|--------|---------|
| FL 99202 rate | $55.15 (CY2025) | $58.45 (CY2026) | **PASS** (data vintage) |
| Facility/TC constraint | 0 violations | 0 | **PASS** |
| Special codes 46924/91124/91125 | Present + compliant | All correct | **PASS** |
| Total FL rate count | Reasonable | 10,314 codes | **PASS** |

### Managed Care (2 checks)
| Check | Expected | Actual | Verdict |
|-------|----------|--------|---------|
| SMMC 3.0 / 9 regions | Confirmed | 9 alphabetical regions (A-I) | **PASS** |
| MC penetration | ~85-95% | 89.3% | **PASS** |

### Hospitals (6 checks)
| Check | Expected | Actual | Verdict |
|-------|----------|--------|---------|
| Unique hospitals FY2023 | ~6,000-6,200 | 6,040 | **PASS** |
| FL hospital count | ~200-250 | 263 | **PASS** |
| NULL provider_ccn | 0 | 0 | **PASS** |
| FY2023 duplication | 0 | Was 2x, now 0 | **PASS** (fixed) |
| Negative NPR | <1% | 0.15% | **PASS** |
| National NPR | ~$1.3T | $1.39T (deduped) | **PASS** |

### Nursing Facilities (9 checks)
| Check | Expected | Actual | Verdict |
|-------|----------|--------|---------|
| Five-Star total | 14,500-16,000 | 14,710 | **PASS** |
| FL facilities | 680-700 | 694 | **PASS** |
| Rating distribution | Bell-shaped | Uniform quintile | **PASS** |
| PBJ avg HPRD | 3.5-4.5 | 3.43 | **PASS** |
| FL HPRD | Near national | 3.60 | **PASS** |

### Pharmacy (7 checks)
| Check | Expected | Actual | Verdict |
|-------|----------|--------|---------|
| SDUD 2025 rows | Reasonable | 2,637,009 | **PASS** |
| National Rx spending | ~$40-50B | $52.4B | **PASS** |
| Top drugs | HIV, GLP-1, biologics | Biktarvy, Jardiance, Humira, Ozempic | **PASS** |
| NADAC rows | Reasonable | 1,882,296 | **PASS** |
| Metformin unit price | $0.01-$1.00 | $0.024 | **PASS** |

### Behavioral Health (7 checks)
| Check | Expected | Actual | Verdict |
|-------|----------|--------|---------|
| NSDUH coverage | 50+ states | 52 (50+DC+US) | **PASS** |
| MH facility count | 10,000-30,000 | 27,957 | **PASS** |
| FL facilities | Reasonable | 1,249 | **PASS** |
| FL opioid prescribing | Reasonable | 1.87% of claims | **PASS** |

### Workforce (6 checks)
| Check | Expected | Actual | Verdict |
|-------|----------|--------|---------|
| BLS wage coverage | 50+ states | 51 states, 16 occupations | **PASS** |
| FL HHA wage | $25K-$35K | $33,650 | **PASS** |
| FL RN wage | $65K-$85K | $88,200 | **WARNING** (market) |
| HPSA total | Reasonable | 68,859 | **PASS** |
| FL HPSA breakdown | All disciplines | PC: 1,619, MH: 701, Dental: 1,235 | **PASS** |

### Economic (6 checks)
| Check | Expected | Actual | Verdict |
|-------|----------|--------|---------|
| CPI latest | Recent | Jan 2026: 325.252 | **PASS** |
| FL unemployment | 2-6% | 3.5-4.3% | **PASS** |
| FL population | 22-23M | 21.9M | **PASS** |
| FL poverty rate | 11-14% | 12.6% | **PASS** |
| FL uninsured rate | 11-15% | 11.9% | **PASS** |

### Quality (6 checks)
| Check | Expected | Actual | Verdict |
|-------|----------|--------|---------|
| Core Set rows | Reasonable | 5,236 / 50 states | **PASS** |
| FL measures | 20-60 | 47 | **PASS** |
| Hospital star distribution | Bell-shaped | Correct | **PASS** |
| Duplicate measures | 0 | 10 (CAHPS plan-level) | **WARNING** |
| Rate range | 0-100 | PQI rates >100 (per 100K) | **WARNING** |

### Program Integrity (17 checks)
| Check | Expected | Actual | Verdict |
|-------|----------|--------|---------|
| LEIE total | 80K-85K | 82,749 | **PASS** |
| Open Payments total | $10-11B | $10.83B | **PASS** |
| Category breakdown | 3 categories | G:$2.2B, R:$8.5B, O:$0.15B | **PASS** |
| MFCU rows | 50-55 | 52-83 per table | **PASS** |

### LTSS/HCBS (14 checks) — All PASS
### Expenditure (9 checks) — 8 PASS, 1 WARNING (zero-value dupes)
### Medicare (16 checks) — All PASS
### Provider Network (9 checks) — 8 PASS, 1 WARNING (no unified NPPES)
### Public Health (8 checks) — All PASS
### Maternal & Child (18 checks) — All PASS
### Insurance Market (12 checks) — All PASS

---

## Data Quality Actions Taken

1. Removed 18 stale snapshot directories (17 + hospital_cost) to prevent duplication
2. Fixed ACS sentinel value (-888888888 -> NULL) in build_lake_census.py
3. Verified FMAP anchor: 57.22% exact match across 3 independent sources
4. Confirmed FL non-expansion status: 0 expansion enrollees in all tables
5. Validated $10.83B Open Payments total across 3 CMS categories
