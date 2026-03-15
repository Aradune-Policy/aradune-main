# Aradune Cross-Domain Research: Advanced Methods Report

**Generated:** 2026-03-14 21:40

**Data Lake:** /Users/jamestori/Desktop/Aradune/data/lake

**Methods:** IV/2SLS, VIF, Propensity Score Matching, CHOW Event Study, 
Random Forest, Quantile Regression, K-Means Clustering, Spatial Mismatch Index

**Supplements:** research_replication.py (OLS, Panel FE, DiD, Cohen's d)

---

# Analysis 1 Enhanced: Rate-Quality with IV, VIF, Quantile Regression

Merged sample: **N=41** states

## 1A. OLS with Robust (HC1) Standard Errors

*Dropping SVI and poverty to reduce collinearity; keeping rate, MC, income.*

| Variable | Coefficient | Robust SE | t | p |
| --- | --- | --- | --- | --- |
| (intercept) | -5.5098 | 14.3838 | -0.38 | 0.7017 |
| Medicaid rate (%) | 0.0704 | 0.0351 | 2.01 | 0.0445* |
| MC penetration (%) | 0.1985 | 0.0756 | 2.63 | 0.0087** |
| Income ($K) | 0.4420 | 0.1251 | 3.53 | 0.0004*** |

R² = 0.407, Adjusted R² = 0.359, N = 41

## 1B. Variance Inflation Factors

| Variable | VIF | Flag |
| --- | --- | --- |
| Medicaid rate (%) | 1.07 |  |
| MC penetration (%) | 1.25 |  |
| Income ($K) | 1.19 |  |

*VIF > 5 suggests moderate collinearity; VIF > 10 suggests severe.*

## 1C. Instrumental Variables (2SLS): GPCI as Instrument

**Identification strategy:** Geographic Practice Cost Index (GPCI) affects Medicare rates, 
which influence Medicaid rates (many states peg to Medicare). GPCI should not directly 
affect Medicaid quality except through rates (exclusion restriction).


IV sample: **N=41** states with GPCI data

**First-stage F-statistic: 0.0**
 ⚠️ Weak instrument (F < 10). GPCI does not predict Medicaid rates 
in the first stage — most states don't peg to Medicare. 
IV estimates below are unreliable. Alternative instruments needed 
(e.g., neighboring state rates, historical rate shocks, legislative mandates).

| Variable | IV Coefficient | SE | t | p |
| --- | --- | --- | --- | --- |
| (intercept) | 754.5439 | 6443.6458 | 0.12 | 0.9068 |
| MC penetration (%) | -1.6112 | 15.3733 | -0.10 | 0.9165 |
| Income ($K) | -1.9189 | 20.2126 | -0.09 | 0.9244 |
| Medicaid rate (%, IV) | -4.0294 | 34.7309 | -0.12 | 0.9076 |

IV R² = -374.147


**OLS rate coefficient: 0.0704**

**IV rate coefficient: -4.0294**

*IV coefficient is substantially larger than OLS, suggesting OLS has attenuation bias 
(measurement error in rates) or downward omitted variable bias.*

## 1D. Quantile Regression: Rate Effect Across Quality Distribution

*Does the rate effect differ for states at the bottom vs top of quality?*

| Quantile | Rate Coefficient | SE (bootstrap) | p |
| --- | --- | --- | --- |
| τ=0.10 | 0.0074 | 0.0520 | 0.8875 |
| τ=0.25 | 0.0037 | 0.0520 | 0.9432 |
| τ=0.50 | 0.0745 | 0.0520 | 0.1514 |
| τ=0.75 | 0.0857 | 0.0520 | 0.0993† |
| τ=0.90 | 0.0810 | 0.0520 | 0.1190 |

*If the coefficient is larger at lower quantiles, rate increases help struggling states more.*


---

# Analysis 2 Enhanced: Managed Care Value — Dynamic Panel + Trend Analysis

## 2A. MC Transition Event Study

*Identifying states with large MC penetration increases for event study analysis.*


States with MC penetration change > 15pp: **21**


### Top MC Transition States (Synthetic Control Candidates)

| State | Min MC% | Max MC% | Change | From | To |
| --- | --- | --- | --- | --- | --- |
| ND | 23% | 82% | +58pp | 2016 | 2024 |
| VT | 53% | 100% | +47pp | 2016 | 2024 |
| AR | 50% | 97% | +47pp | 2016 | 2024 |
| SC | 60% | 100% | +40pp | 2016 | 2024 |
| OK | 65% | 98% | +34pp | 2016 | 2024 |
| VA | 67% | 100% | +33pp | 2016 | 2024 |
| MT | 58% | 91% | +32pp | 2016 | 2024 |
| MS | 43% | 74% | +31pp | 2016 | 2024 |
| SD | 61% | 89% | +28pp | 2016 | 2024 |
| NH | 65% | 92% | +27pp | 2016 | 2024 |

## 2B. Dynamic Panel: Spending Growth Decomposition

Panel with lagged DV: **284 obs, 48 states**


### Spending Growth Rate (%) ~ MC + Income + Year

| Variable | Coefficient | Robust SE | t | p |
| --- | --- | --- | --- | --- |
| (intercept) | -6584.822 | 977.806 | -6.73 | 0.0000*** |
| MC penetration (%) | 0.035 | 0.052 | 0.67 | 0.5006 |
| Income ($K) | -0.043 | 0.055 | -0.79 | 0.4322 |
| Year | 3.260 | 0.484 | 6.73 | 0.0000*** |

R² = 0.219, N = 284


### State-Level Spending Growth (CAGR)

Median CAGR: **4.9%/year**

Top 5: KY (9.8%), IA (9.6%), ID (8.6%), WA (8.5%), SD (7.8%)

Bottom 5: MO (2.1%), NE (1.7%), WI (1.3%), ME (1.2%), HI (0.2%)


## 2C. MLR Trend Decomposition by Year


### MCO MLR Trend by Year

| Year | Plans | Avg MLR | Median MLR | % Below 85% | Total Premium | Remittance |
| --- | --- | --- | --- | --- | --- | --- |
| 2017 | 12 | 95.4% | 92.2% | 0.0% | $6B | $nanM |
| 2018 | 500 | 92.5% | 92.6% | 7.6% | $254B | $214M |
| 2019 | 565 | 92.4% | 92.9% | 8.8% | $307B | $176M |
| 2020 | 571 | 90.0% | 90.3% | 14.2% | $367B | $247M |
| 2021 | 561 | 89.0% | 90.0% | 18.7% | $389B | $1065M |

---

# Analysis 3 Enhanced: Nursing Ownership — PSM + CHOW Event Study

## 3A. Propensity Score Matching: For-Profit vs Nonprofit

*Matching for-profit to nonprofit facilities on beds, urban/rural, state, acuity.*


Matched pairs: **10737** (caliper=0.05)


### PSM Average Treatment Effect on Treated

| Outcome | For-Profit | Nonprofit (matched) | ATT | p-value |
| --- | --- | --- | --- | --- |
| Overall Rating | 2.80 | 3.47 | -0.67 | 0.000000*** |
| Staffing Rating | 2.62 | 3.60 | -0.98 | nan |
| Inspection Rating | 2.67 | 3.19 | -0.53 | 0.000000*** |
| QM Rating | 3.60 | 3.63 | -0.03 | — |
| Avg Deficiencies | 10.4 | 7.7 | +2.8 | — |

### Covariate Balance (Post-Matching)

- beds: SMD = -0.178 ⚠️ imbalanced

- avg_residents: SMD = -0.155 ⚠️ imbalanced


## 3B. Change-of-Ownership (CHOW) Event Study

*Tracking quality changes around ownership transfers using fact_snf_chow.*


SNF ownership transfers matched to Five Star: **4952**


### Ownership Type After Transfer

| Post-CHOW Ownership | Count |
| --- | --- |
| For profit - Limited Liability company | 2384 |
| For profit - Corporation | 1767 |
| For profit - Individual | 233 |
| Non profit - Corporation | 221 |
| For profit - Partnership | 133 |
| Government - Hospital district | 130 |
| Non profit - Other | 35 |
| Government - County | 25 |
| Non profit - Church related | 12 |
| Government - Federal | 4 |
| Government - City/county | 3 |
| Government - City | 3 |
| Government - State | 2 |

**CHOW facility avg rating: 2.58** vs national avg: 2.98
 (difference: -0.40)

| CHOW Type | N | Avg Overall | Avg Staffing |
| --- | --- | --- | --- |
| ACQUISITION/MERGER | 2 | 3.50 | 1.00 |
| CHANGE OF OWNERSHIP | 4950 | 2.58 | 2.43 |

*NOTE: Full event study requires historical Five-Star snapshots (pre/post transfer). 
Current data is point-in-time only. The CHOW dates + current quality allow cross-sectional 
analysis but not pre/post comparison. Historical quarterly Five-Star archives from CMS 
would enable a proper event study design.*


---

# Analysis 4 Enhanced: Pharmacy Spread — ML + Policy Analysis

## 4A. Random Forest: Drivers of Drug-Level Overpayment


Drugs for ML analysis: **30,558**

**Random Forest R² (in-sample): 0.750**


### Top 10 Features Predicting Drug Overpayment

| Feature | Importance | % Total |
| --- | --- | --- |
| log_rx | 0.4867 | 48.7% |
| log_nadac | 0.3086 | 30.9% |
| n_states | 0.1122 | 11.2% |
| log_units | 0.0797 | 8.0% |
| unit_ML | 0.0079 | 0.8% |
| unit_GM | 0.0036 | 0.4% |
| tier_Medium ($1-$10) | 0.0006 | 0.1% |
| tier_Low (<$1) | 0.0004 | 0.0% |
| tier_Specialty ($100+) | 0.0002 | 0.0% |

### Overpayment by Price Tier

| Price Tier | Drugs | Total Spread | Median Markup |
| --- | --- | --- | --- |
| Low (<$1) | 22,305 | $2.61B | 2.61x |
| High ($10-$100) | 2,581 | $0.89B | 1.00x |
| Medium ($1-$10) | 4,769 | $0.84B | 1.15x |
| Specialty ($100+) | 903 | $-1.19B | 0.97x |

## 4B. State-Level Spread Variation Analysis


States analyzed: **52**

Spread % range: -32.7% to 62.6%


**States paying BELOW NADAC (net underpayment):** NH, MI, HI, DE

*These states demonstrate that below-acquisition-cost reimbursement is achievable.*


### Highest Spread States

- SD: 62.6% spread ($0.04B)

- NM: 29.2% spread ($0.06B)

- PR: 18.1% spread ($0.07B)

- MN: 18.0% spread ($0.10B)

- AR: 17.1% spread ($0.03B)


### Lowest Spread States

- DC: 0.7% spread ($0.00B)

- NH: -1.0% spread ($-0.00B)

- MI: -3.5% spread ($-0.06B)

- HI: -25.3% spread ($-0.04B)

- DE: -32.7% spread ($-0.04B)


---

# Analysis 5 Enhanced: Treatment Gap — Spatial Mismatch + Clustering

## 5A. Treatment Gap Composite

States with OUD prevalence data: **51**

National MAT spending: **$978M**

**Spatial Mismatch Index: 0.164** (0=perfect alignment, 0.5=maximum mismatch)


### Top 10 Treatment Gap States (High Need, Low Treatment)

| State | OUD Prev | MAT $ | MAT $/1K Enrollees | Facilities/100K | Gap Score |
| --- | --- | --- | --- | --- | --- |
| MS | 3.3% | $2M | $4047/1K | 46 | 0.954 |
| IA | 2.3% | $1M | $2310/1K | 63 | 0.669 |
| LA | 2.7% | $19M | $13627/1K | 26 | 0.649 |
| SC | 2.2% | $3M | $3566/1K | 24 | 0.630 |
| NV | 2.2% | $3M | $4315/1K | 35 | 0.628 |
| KS | 2.1% | $0M | $869/1K | 76 | 0.622 |
| AR | 2.3% | $5M | $6224/1K | 42 | 0.618 |
| OK | 2.2% | $3M | $3637/1K | 31 | 0.614 |
| OR | 2.1% | $3M | $2525/1K | 36 | 0.609 |
| WY | 2.2% | $0M | $6110/1K | 133 | 0.601 |

## 5B. State Typology Clustering


### State Typology (K-Means, 4 clusters)

| Cluster | N States | Avg OUD% | MAT $/1K | Fac/100K | Sample States |
| --- | --- | --- | --- | --- | --- |
| 🟡 High Need / Responding | 9 | 1.9% | $35482/1K | 104 | KY, MD, ME, MT, ND... |
| 🔴 Treatment Desert | 26 | 2.1% | $6707/1K | 40 | AL, AR, AZ, CA, DE... |
| 🟢 Low Need / Well-Resourced | 15 | 1.6% | $21431/1K | 48 | AK, CO, CT, DC, ID... |
| 🟡 High Need / Responding | 1 | 3.2% | $87951/1K | 47 | WV |

---
