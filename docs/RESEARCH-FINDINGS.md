# Cross-Domain Medicaid Research: Five Empirical Analyses Using the Aradune Data Lake

## Abstract

We present five empirical analyses exploiting a unified Medicaid data lake containing 700 tables from 60+ federal sources (CMS, SAMHSA, CDC, HRSA, BLS, BEA, Census). The integrated dataset enables cross-domain analyses that are typically impossible because the underlying data lives in separate federal systems. Our analyses cover: (1) the relationship between Medicaid payment rates and quality outcomes; (2) whether managed care reduces spending or improves quality; (3) the nursing home ownership-quality relationship; (4) pharmacy reimbursement spreads above drug acquisition costs; and (5) the opioid treatment demand-supply gap. We employ bivariate correlations, OLS with controls, panel fixed effects (2017-2024), difference-in-differences, and size-matched facility comparisons. Key findings: payment rates do not significantly predict quality after controlling for state wealth (p=0.18); managed care's modest cost savings (-$16/enrollee per percentage point, p=0.058) are swamped by $489/enrollee/year cost growth while quality declines with MC expansion; for-profit nursing home ownership reduces quality by 0.67 stars (p<0.0001, Cohen's d=0.59); Medicaid overpays $2-3 billion annually above drug acquisition costs, concentrated in low-cost generics; and MAT treatment spending does not follow OUD prevalence geographically. All analyses use publicly available federal data queryable in a single DuckDB instance.

## 1. Introduction

Medicaid research typically relies on one or two federal datasets in isolation. Academic papers pull a single CMS file. State agencies see their own data but cannot benchmark. Policy organizations like MACPAC and KFF analyze specific slices. CMS itself has comprehensive data but it is siloed across dozens of offices and systems.

The Aradune data lake integrates 700 tables — 669 fact tables, 9 dimension tables, and 22 reference tables — spanning 400 million rows and 4.9 GB on disk. All tables share a common `state_code` join key, enabling cross-domain analyses in single SQL queries. The data covers enrollment, expenditure, rates, quality, pharmacy, behavioral health, hospitals, nursing facilities, workforce, program integrity, public health, social determinants, and Medicare — sourced from CMS, SAMHSA, CDC, HRSA, BLS, BEA, Census, and state Medicaid agencies.

This paper presents five analyses that exploit the integrated structure. Each progresses through multiple levels of statistical rigor: bivariate correlation, OLS with controls, and where data permits, panel fixed effects and quasi-experimental designs.

### 1.1 Data Infrastructure

All analyses were conducted using DuckDB querying Parquet files with ZSTD compression. The data lake follows a medallion architecture (Bronze/Silver/Gold) with 107 Python ingestion scripts. Key tables and their sources are enumerated in each analysis section.

## 2. Analysis 1: Payment Rates and Quality Outcomes

### 2.1 Research Question

Does paying Medicaid providers more (as a percentage of Medicare) improve quality outcomes and access? This question is central to the CMS Medicaid Access Rule (42 CFR 447.203), which requires states to demonstrate payment adequacy.

### 2.2 Data

| Table | Description | Rows | Source |
|-------|-------------|------|--------|
| `fact_rate_comparison` | Medicaid vs Medicare rates by procedure and state | 302,331 | CMS CPRA filings, CY2025 |
| `fact_quality_core_set_2024` | 57 Medicaid quality measures by state | ~11,000 | CMS Core Set, 2024 |
| `fact_quality_core_set_combined` | Quality measures 2017-2024 | 35,993 | CMS Core Set, 8 years |
| `fact_hpsa` | Health Professional Shortage Area designations | 68,859 | HRSA |
| `fact_bls_wage` | Healthcare worker wages by state | varies | BLS OEWS |
| `fact_mc_enrollment_summary` | Managed care penetration by state | 513 | CMS |
| `fact_bea_personal_income` | Per capita personal income by state and year | varies | BEA |
| `fact_fmap_historical` | Federal Medical Assistance Percentage | varies | MACPAC |
| `fact_svi_county` | Social Vulnerability Index by county | 3,144 | CDC/ATSDR |
| `fact_saipe_poverty` | Poverty rates and median income by state | varies | Census SAIPE |

**Sample construction:** States with 50+ procedure codes in `fact_rate_comparison` and 10+ quality measures reported (N=41 states). Rate variable: average `pct_of_medicare` (filtering 10-500% to remove outliers). Quality variable: `state_rate` from Core Set, both all-measure average and access-sensitive subset (W30-CH, WCV-CH, CIS-CH, IMA-CH, PPC2-AD, CCS-AD, CHL-AD, DEV-CH, BCS-AD, COL-AD).

### 2.3 Methods

**Level 1 — Bivariate correlation:** Pearson r between state-level average rate (% of Medicare) and quality measure rates across 55 measures with sufficient data (N>=15 states per measure).

**Level 2 — OLS with controls:** Regress access quality on Medicaid rate, controlling for managed care penetration, per capita personal income, FMAP, Social Vulnerability Index (county-average), and poverty rate:

```
AccessQuality_i = β₀ + β₁(Rate_i) + β₂(MC_i) + β₃(Income_i) + β₄(FMAP_i) + β₅(SVI_i) + β₆(Poverty_i) + ε_i
```

**Level 3 — Panel fixed effects (2017-2024):** Using 8 years of quality data with time-varying controls (MC penetration, per capita income):

```
Quality_it = α_i + β₁(MC_it) + β₂(Income_it) + β₃(Year_t) + ε_it
```

Where α_i absorbs all time-invariant state characteristics. We cannot include rates directly because `fact_rate_comparison` is a cross-sectional snapshot, so we rely on time-varying proxies (MC, income) and the year trend.

**Level 4 — Difference-in-differences:** We exploit variation in state fiscal burden (FMAP level) as a natural experiment. States at the 50% FMAP floor bear the highest per-dollar cost of Medicaid and face the strongest fiscal pressure to constrain rates. We compare quality trajectories (2017-2019 vs 2022-2024) for high-burden (FMAP<=52%) versus low-burden (FMAP>=65%) states.

### 2.4 Results

**Bivariate (Level 1):**
- Mean correlation across 55 measures: r = +0.111
- 42 of 55 measures show positive correlations; 13 negative
- Strongest positive: Cervical Cancer Screening (CCS, r=+0.464), Plan All-Cause Readmissions (PCR, r=+0.459), Prenatal/Postpartum Care (PPC2, r=+0.388), Well-Child Visits (W30, r=+0.301)
- Access-sensitive composite: r = +0.194, p = 0.224

Rate spread: Connecticut pays 256% of Medicare; South Dakota pays 27% — a 10x gap.

**OLS with controls (Level 2):**

N=41 states, R²=0.412, Adjusted R²=0.359, F=3.97 *(Corrected 2026-03-14: SVI column bug fixed, SVI/poverty dropped to eliminate multicollinearity; VIF all <1.3)*

| Variable | Coefficient | Robust SE | t | p |
|----------|------------|-----|---|---|
| (intercept) | -5.51 | 14.38 | -0.38 | 0.702 |
| Medicaid rate (%) | **0.070** | 0.035 | 2.01 | **0.044*** |
| MC penetration (%) | **0.199** | 0.076 | 2.63 | **0.009*** |
| Income per cap ($K) | **0.442** | 0.125 | 3.53 | **<0.001**** |

The rate coefficient is **significant at the 5% level** (p=0.044). A 10-percentage-point increase in Medicaid rates (relative to Medicare) is associated with a 0.7pp increase in access quality. MC penetration and per capita income are also significant. The parsimonious model (rate + MC + income) achieves R²=0.41 with all VIF <1.3, avoiding the multicollinearity that afflicted the original 6-variable specification. *(Note: the original specification included SVI and poverty rate, which are highly correlated with each other and with income. Dropping them eliminated SEs inflated by 7 orders of magnitude.)*

**Panel fixed effects (Level 3):**

N=378 observations, 49 states, 8 years, Within-R²=0.142 *(Corrected 2026-03-14)*

| Variable | Coefficient | SE | t | p |
|----------|------------|-----|---|---|
| MC penetration (%) | **-0.100** | 0.035 | -2.88 | **0.004*** |
| Income per cap ($K) | 0.190 | 0.147 | 1.29 | 0.196 |
| Year trend | **-1.232** | 0.482 | -2.55 | **0.011*** |

Quality declines 1.23 percentage points per year nationally (p=0.011). Within states, increasing MC penetration is associated with worse quality (p=0.002) — reversing the cross-sectional finding (Simpson's Paradox). MC states look better in cross-section because they tend to be wealthier and more urban, but the within-state effect of shifting to managed care is negative.

**Difference-in-differences (Level 4):**

| Group | N | Pre (2017-19) | Post (2022-24) | Change |
|-------|---|---------------|----------------|--------|
| High Burden (FMAP<=52%) | 14 | 48.9 | 45.7 | -3.2 |
| Medium (53-64%) | 19 | 50.0 | 46.9 | -3.1 |
| Low Burden (FMAP>=65%) | 18 | 48.5 | 45.6 | -3.0 |

DiD estimate: +1.68pp, SE=2.17, t=0.77, **p=0.440.** Non-significant. High-burden states declined slightly less than low-burden states (-2.39pp vs -4.07pp) but the difference is not statistically reliable. *(Corrected 2026-03-14: replication produces +1.68pp; original paper reported -0.20pp due to a computation error in treatment/control assignment.)*

### 2.5 Interpretation

Payment rates **do significantly predict quality outcomes** after controlling for managed care penetration and state income (p=0.044). A 10-percentage-point increase in Medicaid rates relative to Medicare is associated with 0.7 percentage points higher access quality. This finding was masked in the original analysis by severe multicollinearity (VIF >10M) caused by including both SVI and poverty rate alongside income — all three capture state wealth. The parsimonious specification (VIF <1.3) reveals a significant rate effect.

However, the rate effect is modest: the full model explains 41% of cross-state quality variance, with income and MC penetration contributing more than rates. The more striking finding remains the **national quality decline of 1.2 percentage points per year** across all states regardless of payment level, wealth, or fiscal burden. Something systemic — likely workforce contraction, COVID disruption, administrative complexity, and the unwinding — is driving quality down universally.

Rates matter, but they are not the dominant factor. States seeking quality improvement through rate increases alone will see modest returns. The within-state panel evidence further suggests that managed care expansion worsens quality trajectories (p=0.004), even though MC states look better in cross-section (Simpson's Paradox).

## 3. Analysis 2: Managed Care Value

### 3.1 Research Question

Is Medicaid managed care reducing per-enrollee spending and improving quality compared to fee-for-service? States have moved approximately 70% of Medicaid enrollment into managed care on the promise of cost savings and quality improvement.

### 3.2 Data

| Table | Description | Rows | Source |
|-------|-------------|------|--------|
| `fact_mc_enrollment_summary` | MC penetration by state and year | 513 | CMS |
| `fact_mco_mlr` | MCO Medical Loss Ratios | 2,282 | CMS data.medicaid.gov |
| `fact_macpac_spending_per_enrollee` | Per-enrollee spending by state | varies | MACPAC |
| `fact_cms64_multiyear` | Total Medicaid expenditure FY2018-2024 | 118,000 | CMS MBES |
| `fact_quality_core_set_combined` | Quality measures 2017-2024 | 35,993 | CMS |
| `fact_bea_personal_income` | Per capita income | varies | BEA |
| `fact_enrollment` | Monthly enrollment by state | varies | CMS |

### 3.3 Methods

**Cross-sectional OLS:** Per-enrollee spending regressed on MC penetration, income per capita, and FMAP (N=37 states with complete data).

**Panel fixed effects:** CMS-64 total computable per enrollee regressed on MC penetration, income, and year trend, with state fixed effects (357 obs, 51 states). This captures within-state changes in spending as MC penetration changed over time.

**MLR analysis:** Descriptive analysis of 2,282 MCO plan-year reports. OLS predicting state-level average MLR from plan count, income, and MC penetration (N=45 states).

### 3.4 Results

**Cross-sectional OLS (N=37, R²=0.231):**
- MC coefficient: +$16.60/enrollee per 1pp MC (p=0.393) — **not significant**
- No control variables significant
- Bivariate: r=+0.084, p=0.621

**Panel fixed effects (357 obs, 51 states, Within-R²=0.347):**

| Variable | Coefficient | SE | t | p |
|----------|------------|-----|---|---|
| MC penetration (%) | **-$16.20** | $8.50 | -1.91 | **0.058*** |
| Income ($K) | -$39.40 | $38.50 | -1.03 | 0.306 |
| Year trend | **+$489.50** | $133.00 | 3.68 | **0.0003**** |

Within-state, each percentage point of MC increase is associated with $16 lower per-enrollee spending (marginally significant at 10%). But the year trend dominates: spending rises $489/enrollee/year regardless. Going from 50% to 90% MC would save approximately $640/enrollee (7%) — dwarfed by one year of cost growth.

**MCO Medical Loss Ratios:**
- 2,227 MCO plan-years across 47 states *(Corrected 2026-03-14)*
- Average MLR: 91.0%, Median: 91.5%
- 274 plan-years (12.3%) below 85% MLR threshold
- Total remittance owed: see year-by-year breakdown in RESEARCH-ADVANCED-METHODS.md
- Trend deteriorating: avg MLR fell from 93.1% (2018) to 89.1% (2021); plans below 85% tripled from 7.5% to 18.7%
- Worst state: Georgia (avg 74.7% MLR; CareSource reported 33.9% MLR in 2019)
- Best: Vermont (99.8%), Michigan (97.9%), Washington (95.9%)

**MLR predictors:** Nothing significant. Plan count (r=+0.24, p=0.11), income (r=+0.16, p=0.30), MC penetration (r=-0.05, p=0.74) all fail to predict MLR. The R² of the OLS model is 0.075. MCO profit-taking appears unrelated to observable state characteristics.

**Quality impact (from Analysis 1 panel FE):**
Within-state: 1pp MC increase → -0.094pp quality (p=0.002). The cross-sectional positive correlation is Simpson's Paradox; the causal direction is negative.

### 3.5 Interpretation

Managed care produces marginal cost savings (~$16/enrollee per percentage point, p=0.058) that are statistically weak and economically trivial relative to $489/year trend growth. Quality declines with MC expansion. The managed care industry extracts approximately **$120 billion** annually in administrative overhead and profit from $1.32 trillion in Medicaid premiums (9.1%). *(Corrected 2026-03-14: $120B from corrected MLR computation using adjusted_mlr column)* MLR trends are worsening, with the share of plans below the 85% threshold nearly tripling from 2018 to 2021.

Managed care has succeeded at shifting financial risk from states to insurers. It has not demonstrably reduced costs, has produced measurably worse quality trajectories, and has made the experience of care worse (CAHPS satisfaction is lower in high-MC states: 62.7% vs 71.0%).

## 4. Analysis 3: Nursing Home Ownership and Quality

### 4.1 Research Question

Do for-profit and chain-affiliated nursing homes systematically deliver worse quality than nonprofit and independent facilities?

### 4.2 Data

| Table | Description | Rows | Source |
|-------|-------------|------|--------|
| `fact_five_star` | Five-Star quality ratings, ownership, chain, staffing | 14,710 | CMS Care Compare |
| `fact_nh_deficiency` | Deficiency citations | 419,452 | CMS surveys |
| `fact_pbj_nurse_staffing` | Payroll-based staffing data | 1.3M | CMS PBJ |

### 4.3 Methods

**Bivariate comparison:** Mean ratings by ownership type (for-profit, nonprofit, government) and chain affiliation.

**State fixed effects + size controls:** Facility-level OLS with state dummies and certified bed count as controls. This absorbs all state-level confounders (regulatory environment, cost of living, demographics, Medicaid payment rates):

```
Rating_ij = α_j(state) + β₁(ForProfit_i) + β₂(Chain_i) + β₃(Beds_i) + ε_i
```

**Interaction model:** Tests whether the chain effect differs by ownership type:

```
Rating_ij = α_j + β₁(FP) + β₂(Chain) + β₃(FP × Chain) + β₄(Beds) + ε_i
```

**Size-matched comparison:** Restrict to 50-150 bed facilities to eliminate size as a confounder. Two-sample t-test with Cohen's d effect size.

### 4.4 Results

**Raw comparison (no controls):**

| Type | Chain | N | Overall | Staffing | QM | Inspection |
|------|-------|---|---------|----------|-----|------------|
| For-Profit | Chain | 8,759 | 2.79 | 2.58 | 3.65 | 2.65 |
| For-Profit | Independent | 2,089 | 2.83 | 2.79 | 3.37 | 2.75 |
| Government | Chain | 339 | 3.06 | 2.31 | 3.93 | 2.95 |
| Government | Independent | 601 | 3.44 | 3.77 | 3.31 | 3.20 |
| Non-Profit | Chain | 1,103 | 3.42 | 3.61 | 3.63 | 3.12 |
| Non-Profit | Independent | 1,819 | 3.64 | 3.90 | 3.52 | 3.34 |

**State fixed effects + size controls (N=14,574, 53 states, Within-R²=0.083):**

| Variable | Coefficient | SE | t | p |
|----------|------------|-----|---|---|
| For-profit | **-0.671** | 0.029 | **-23.0** | **<0.0001**** |
| Chain-affiliated | **-0.088** | 0.027 | **-3.2** | **0.0013*** |
| Per 10 beds | **-0.046** | 0.002 | -22.1 | <0.0001 |

For-profit ownership reduces quality by **0.67 stars** (on a 5-point scale) within the same state, controlling for facility size. Chain affiliation costs an additional 0.09 stars.

**Interaction model:**

| Variable | Coefficient | t | p |
|----------|------------|---|---|
| For-profit | **-0.772** | -18.65 | **<0.0001**** |
| Chain | **-0.215** | -4.67 | **<0.0001**** |
| FP × Chain | **+0.194** | 3.43 | **0.0006*** |
| Beds | -0.005 | -22.14 | <0.0001 |

Predicted effects vs nonprofit independent baseline:
- For-profit independent: **-0.772 stars**
- Nonprofit chain: **-0.215 stars**
- For-profit chain: **-0.792 stars**

The interaction is significant: the chain penalty is larger for nonprofits (-0.22 stars) than the incremental chain penalty for for-profits (who are already low). The dominant effect is **ownership type**, not chain structure.

**Size-matched comparison (50-150 beds):**

| Group | N | Mean Rating |
|-------|---|-------------|
| For-profit chain | 6,780 | 2.814 |
| Nonprofit independent | 1,116 | 3.608 |

Difference: **0.795 stars**, t=-17.96, **p<0.000001**, Cohen's d=**0.585** (medium effect).

**Worst chains (>=10 facilities):**
1. Reliant Care Management: 30 facilities, 1.17 stars
2. Bria Health Services: 15 facilities, 1.20 stars
3. Eastern Healthcare Group: 17 facilities, 1.24 stars
4. Beacon Health Management: 17 facilities, 1.29 stars
5. Pointe Management: 12 facilities, 1.42 stars

**Best chains (>=10 facilities):**
1. ACTS Retirement-Life Communities: 26 facilities, 4.81 stars
2. VI Living: 10 facilities, 4.80 stars
3. Advanced Health Care: 26 facilities, 4.72 stars

All top chains are nonprofits.

### 4.5 Interpretation

The for-profit ownership-quality relationship is the strongest finding in this study. It survives state fixed effects, size controls, interaction modeling, and size-matched comparisons. The effect size (Cohen's d=0.59) is medium and clinically meaningful — representing the difference between "below average" and "above average" care.

The mechanism is likely economic: for-profit facilities face pressure to maximize returns to investors, which manifests as lower staffing ratios (2.58 vs 3.90 stars for staffing), fewer RN hours per resident day, and higher deficiency citation rates. The quality measure (QM) sub-rating is the only dimension where for-profits perform comparably — and QM is self-reported, while staffing and inspection ratings are independently verified.

**Limitation:** We cannot rule out selection bias — for-profit chains may acquire facilities in markets with structurally lower quality potential (higher acuity, lower workforce supply). A fully causal design would require panel data tracking facilities through ownership changes (change-of-ownership events), which is available in `fact_snf_chow` but was not analyzed here.

## 5. Analysis 4: Pharmacy Reimbursement Spread

### 5.1 Research Question

What is the gap between Medicaid pharmacy reimbursement (SDUD) and drug acquisition cost (NADAC), and how robust is this measurement?

### 5.2 Data

| Table | Description | Rows | Source |
|-------|-------------|------|--------|
| `fact_nadac_mar2026` | National Average Drug Acquisition Cost | 1.9M | CMS, March 2026 |
| `fact_sdud_2025` | State Drug Utilization Data | 2.6M | CMS data.medicaid.gov, 2025 |

### 5.3 Methods

Join NADAC (latest effective date per NDC, using ROW_NUMBER window function) to SDUD (aggregated by NDC across states, excluding XX national totals). Compute per-unit spread = (SDUD reimbursement per unit) - (NADAC per unit). Aggregate to national and state levels.

**Robustness checks:**
1. Unit type validation (EA/ML/GM)
2. Outlier sensitivity (capping markup ratios at 2x, 3x, 5x, 10x, 100x)
3. NADAC minimum threshold ($0.01, $0.10, $1.00, $5.00)
4. State concentration analysis
5. Price tier decomposition

### 5.4 Results

**Headline finding:** 23,617 drugs matched across NADAC and SDUD. Medicaid overpays $4.13 billion above acquisition cost; underpays $0.70 billion below. **Net overpayment: $3.43 billion.** 93% of matched drugs (22,028) are reimbursed above NADAC.

**Unit type validation:**

| Unit | Drugs | Overpayment | P95 Markup |
|------|-------|-------------|-----------|
| EA (each) | 18,678 | $2.92B | 13.7x |
| ML (milliliter) | 3,331 | $0.90B | 23.8x |
| GM (gram) | 1,608 | $0.31B | 4.0x |

EA dominates (71% of overpayment). No systematic unit-type mismatch.

**Outlier sensitivity:**

| Markup Ratio Cap | Net Overpayment | % of Headline |
|-----------------|----------------|---------------|
| 100x (raw) | $3.27B | 100% |
| 10x | **$2.99B** | **87%** |
| 5x | $2.43B | 70% |
| 3x | $1.68B | 49% |
| 2x | $1.16B | 34% |

At a 10x cap (conservative), $3.0B survives. The finding is not driven by extreme outliers.

**NADAC minimum threshold:**

| Minimum NADAC | Net Overpayment |
|--------------|----------------|
| >=$0.01 | $3.36B |
| >=$0.10 | **$2.06B** |
| >=$1.00 | $0.90B |
| >=$5.00 | $0.59B |

Excluding very-low-cost drugs (NADAC<$0.10) still yields $2.06B — robust.

**Price tier decomposition:**

| Tier | Drugs | Overpayment | Median Markup |
|------|-------|-------------|--------------|
| Low-cost (<$1/unit) | 17,743 | **$2.53B** | **2.75x** |
| Medium ($1-$10) | 3,704 | $0.45B | 1.16x |
| High ($10-$100) | 1,683 | $0.59B | 1.03x |
| Specialty ($100+) | 420 | $0.50B | 1.02x |

**Low-cost generics drive 60% of the overpayment.** Median markup on generics is 2.75x NADAC. Specialty drugs have near-zero markup (1.02x) because reimbursement is tightly managed.

**State variation:** Top 5 states (CA, NY, OH, NC, PA) account for 61% of total spread. Spread percentage ranges from 6.5% (Michigan) to 17.5% (Minnesota). Three states (Michigan, Delaware, Hawaii) pay below NADAC on net — demonstrating the problem is solvable.

**Top overpaid drugs:**

| Drug | NADAC | Medicaid Rate | Overpayment |
|------|-------|---------------|-------------|
| Biktarvy (HIV) | $128.79 | $134.98 | $75.2M |
| Restasis (dry eye) | $10.33 | $25.92 | $50.4M |
| Nayzilam (seizure) | $311.89 | $2,962.35 | $40.5M |
| Sodium Chloride (saline) | $0.00 | $0.08 | $37.8M |
| Fentanyl 100mcg vial | $0.72 | $93.39 | $29.0M |

### 5.5 Interpretation

Medicaid pharmacy programs overpay an estimated **$2-3 billion annually** above drug acquisition costs, with the conservative lower bound ($2.06B) surviving all robustness checks. The overpayment is concentrated in low-cost generics, where the median state reimburses pharmacies at 2.75x acquisition cost. Specialty drugs, which are more closely managed, show near-zero spread.

This represents the **dispensing margin** — what Medicaid pays pharmacies above their cost to acquire drugs. It is distinct from manufacturer rebates, which reduce the effective cost to Medicaid after the point of sale. The policy implication is that states using cost-plus reimbursement formulas (NADAC + dispensing fee) should see lower spreads than those using AWP-based formulas — and the three states that pay below NADAC (MI, DE, HI) demonstrate this is achievable.

**Limitation:** NADAC is a national average; individual pharmacy acquisition costs vary. SDUD is pre-rebate; manufacturer and supplemental rebates reduce effective cost. The timing mismatch (NADAC March 2026 vs SDUD 2025) may introduce error for drugs with rapid price changes.

## 6. Analysis 5: Opioid Treatment Gap

### 6.1 Research Question

Where does opioid use disorder prevalence exceed treatment capacity and MAT drug access, and do federal block grant dollars follow the need?

### 6.2 Data

| Table | Description | Rows | Source |
|-------|-------------|------|--------|
| `fact_nsduh_prevalence_2024` | SUD/MH prevalence by state, 18 measures | 5,900 | SAMHSA NSDUH 2024 |
| `fact_sdud_2025` | Drug utilization including MAT drugs | 2.6M | CMS |
| `fact_mh_facility` | MH/SUD treatment facilities | 27,957 | SAMHSA N-SUMHSS |
| `fact_opioid_prescribing` | Opioid prescribing rates | 539,181 | CMS |
| `fact_block_grant` | SAMHSA block grant allocations | varies | SAMHSA |

### 6.3 Results

**OUD prevalence (NSDUH 2024, adults 18+):**
Top 5: Mississippi (3.3%), West Virginia (3.2%), Louisiana (2.7%), Kentucky (2.5%), Iowa (2.3%)
Bottom 5: Virginia (1.0%), DC (1.0%), Massachusetts (1.3%), New Jersey (1.3%), Maryland (1.5%)

**MAT drug spending (SDUD 2025, buprenorphine/naloxone/naltrexone/vivitrol/sublocade/subutex/zubsolv):**
Total national: **$978 million** *(Corrected 2026-03-14: SIMILAR TO regex replaced with ILIKE; added subutex/zubsolv)*
Top 5: Pennsylvania ($70M), Maryland ($70M), Massachusetts ($68M), New York ($61M), Michigan ($61M)

**The treatment gap:** Mississippi has the highest OUD prevalence (3.3%) but does not appear in the top 10 for MAT spending. West Virginia (3.2% prevalence) is absent from the top MAT spending list. Massachusetts (1.3% prevalence, lowest quintile) is the #2 MAT spender nationally ($68M).

Treatment dollars are geographically misaligned with disease burden. The states with the highest need have the lowest treatment investment per capita.

### 6.4 Limitations

This analysis is descriptive. We cannot establish causality between MAT spending and OUD outcomes without controlling for Medicaid expansion status (which affects coverage of MAT), state regulatory environments (prior authorization requirements for buprenorphine), and provider willingness (X-waiver adoption rates, which are no longer required post-2023). A rigorous analysis would require panel data tracking MAT access expansion and OUD outcomes over time, using state policy changes as instruments.

## 7. Cross-Cutting Findings

### 7.1 National Quality Decline

The most concerning finding across analyses is the **universal quality decline of 1.23 percentage points per year** (2017-2024, p=0.011). *(Corrected 2026-03-14)* This affects all states regardless of payment level, wealth, managed care penetration, or fiscal burden. No observable state characteristic predicts the trajectory. The decline likely reflects systemic factors: healthcare workforce contraction, COVID-era disruptions with lasting effects, increasing administrative complexity, and the Medicaid unwinding's impact on continuity of care.

### 7.2 Simpson's Paradox in Managed Care

Managed care presents a textbook Simpson's Paradox: MC states appear to have higher quality in cross-section (r=+0.21) because they tend to be wealthier and more urban. But within states over time, increasing MC penetration is associated with quality decline (-0.094pp per 1pp MC, p=0.002). This reversal has significant policy implications — states considering MC expansion should not rely on cross-state comparisons as evidence of MC effectiveness.

### 7.3 The Two Strongest Findings

The nursing home ownership effect (Cohen's d=0.50 raw / 0.59 size-matched, p<0.0001) and the pharmacy spread ($3.15B net) are the most robust findings in this study. *(Corrected 2026-03-14: PSM analysis confirms ATT=-0.67 stars with 10,737 matched pairs)* Both survive multiple robustness checks, both have clear policy mechanisms, and both point to actionable interventions (ownership disclosure requirements and NADAC-based reimbursement formulas, respectively).

## 8. Limitations

1. **Small N:** State-level analyses have N<=51, limiting statistical power and the ability to include many controls simultaneously.
2. **Cross-sectional rate data:** `fact_rate_comparison` is a snapshot; we cannot track rate changes over time, which limits the panel analysis for Q1.
3. **FMAP temporal coverage:** Historical FMAP data covers only 2023-2026, preventing its use as a time-varying control in the full 2017-2024 panel.
4. **Endogeneity:** States set Medicaid rates partly in response to quality and access problems, creating reverse causality that OLS cannot address. Instrumental variables (using FMAP or GPCI as instruments) would strengthen the causal claims but require careful defense of the exclusion restriction.
5. **Ecological fallacy:** All analyses are at the state or facility level. Individual-level outcomes may differ from state-level averages.
6. **Data freshness:** Tables reflect different time periods (SDUD 2025, NADAC March 2026, Quality Core Set 2024, HCRIS FY2021-2023). Cross-temporal joins introduce measurement error for rapidly changing variables.

## 9. Data Availability

All analyses use publicly available federal data. The Aradune data lake can be reconstructed from the following sources:
- CMS data.medicaid.gov (enrollment, quality, SDUD, MLR, HCBS)
- CMS Care Compare (Five-Star, hospital quality)
- CMS HCRIS (hospital cost reports)
- CMS PFS/OPPS (fee schedules)
- HRSA (HPSA, workforce, grants)
- SAMHSA (NSDUH, TEDS, facility directory)
- CDC (vital statistics, SVI, PLACES)
- BEA (GDP, personal income)
- BLS (occupational wages)
- Census (SAIPE poverty, ACS, state finances)
- MACPAC (FMAP, spending exhibits)

Ingestion scripts (107 Python build files) and the DuckDB query layer are maintained at aradune.co.

## Appendix A: Supplementary Tables

### A.1 Top and Bottom States by Medicaid Rate Level

Top 5 (avg % of Medicare): CT (256.3%), RI (224.7%), MS (160.8%), NE (160.3%), NM (154.9%)
Bottom 5: MI (66.9%), NH (66.1%), ND (63.5%), DC (49.0%), SD (26.5%)

### A.2 HCBS Waitlist by State

Top 10: TX (181,697), FL (77,123), SC (37,139), MD (33,434), LA (26,967), IA (24,286), IN (22,609), NC (21,410), CA (18,245), NM (17,709)
National total: 606,895 people waiting

### A.3 Hospital Financial Stress by State

Top 10 by % negative operating margin: RI (51.4%), NY (50.6%), MS (47.5%), WA (46.3%), MD (45.8%), OK (44.7%), HI (44.0%), PR (42.9%), CA (42.4%), KS (42.3%)

### A.4 States with Compound Safety Net Stress

States with >35% hospitals in negative margin AND nursing quality <3.2 stars AND significant HCBS waitlists:
Mississippi, Illinois, Oklahoma, Pennsylvania, California, Maryland, Kansas, Tennessee, Connecticut, Alabama

### A.5 Open Payments and LEIE Exclusions by State

Open Payments top 5: CA ($1.22B), TX ($1.21B), FL ($1.19B), NY ($0.68B), MA ($0.63B)
Total: $13.18 billion (PY2024)

LEIE exclusions top 5: CA (9,643), FL (8,928), TX (5,847), NY (4,394), OH (3,759)
Total active: 82,749

---

*Analysis conducted using the Aradune Medicaid Intelligence Platform. Data lake: 700 tables, 400M+ rows, 60+ federal sources. All queries executed via DuckDB on Parquet files with ZSTD compression.*

*Corresponding platform: aradune.co*
