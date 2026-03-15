# Aradune Cross-Domain Research: Replication Results

**Generated:** 2026-03-14 21:40

**Data Lake:** /Users/jamestori/Desktop/Aradune/data/lake

**Methods:** OLS, Panel Fixed Effects (within-transformation), Difference-in-Differences, Cohen's d

**Replication:** Every SQL query is embedded. Run `python3 scripts/research_replication.py` to reproduce.

---

# Analysis 1: Rate-Quality Nexus

**Question:** Does paying Medicaid providers more (as % of Medicare) improve quality?

## Step 1: State-Level Rate Construction

```sql
-- Replication query: average % of Medicare by state


        SELECT state_code,
               AVG(pct_of_medicare) AS avg_pct_medicare,
               COUNT(*) AS n_codes
        FROM fact_rate_comparison
        WHERE pct_of_medicare BETWEEN 10 AND 500
        GROUP BY state_code
        HAVING COUNT(*) >= 50
        ORDER BY avg_pct_medicare DESC
    ```

States with 50+ codes: **42**

Rate range: 26.5% to 256.3%

## Step 2: Quality Measure Construction

```sql

        SELECT state_code,
               AVG(state_rate) AS avg_access_quality,
               COUNT(DISTINCT measure_id) AS n_measures
        FROM fact_quality_core_set_2024
        WHERE measure_id IN ('W30-CH','WCV-CH','CIS-CH','IMA-CH','PPC2-AD','CCS-AD','CHL-AD','DEV-CH','BCS-AD','COL-AD')
          AND state_rate IS NOT NULL AND state_rate > 0
        GROUP BY state_code
        HAVING COUNT(DISTINCT measure_id) >= 3
    
```

States with 3+ access measures: **51**

## Step 3: Control Variables

```sql

        SELECT
            d.state_code,
            d.fmap,
            COALESCE(mc.mc_penetration_pct, 0) AS mc_pct,
            bea.per_capita_personal_income / 1000.0 AS income_k,
            svi.avg_svi,
            pov.poverty_rate
        FROM dim_state d
        LEFT JOIN (
            SELECT state_code AS mc_st, mc_penetration_pct
            FROM fact_mc_enrollment_summary
            WHERE year = (SELECT MAX(year) FROM fact_mc_enrollment_summary)
        ) mc ON d.state_code = mc.mc_st
        LEFT JOIN (
            SELECT state_code AS bea_st, per_capita_personal_income
            FROM fact_bea_personal_income
            WHERE year = (SELECT MAX(year) FROM fact_bea_personal_income)
              AND per_capita_personal_income IS NOT NULL
        ) bea ON d.state_code = bea.bea_st
        LEFT JOIN (
            SELECT st_abbr AS svi_st, AVG(rpl_themes) AS avg_svi  -- AUDIT FIX: state_code -> st_abbr (column doesn't exist in fact_svi_county)
            FROM fact_svi_county
            WHERE rpl_themes IS NOT NULL AND rpl_themes >= 0
            GROUP BY st_abbr
        ) svi ON d.state_code = svi.svi_st
        LEFT JOIN (
            SELECT state_code AS pov_st, pct_poverty AS poverty_rate
            FROM fact_acs_state
            WHERE data_year = (SELECT MAX(data_year) FROM fact_acs_state)
        ) pov ON d.state_code = pov.pov_st
        WHERE d.fmap IS NOT NULL
    
```

## Step 4: OLS with Controls

Merged sample: **N=41** states

**R² = 0.412, Adjusted R² = 0.287, F = 3.97**


### OLS Results: Access Quality ~ Rate + Controls

| Variable | Coefficient | SE | t | p |
| --- | --- | --- | --- | --- |
| (intercept) | -23.855 | 45.507 | -0.52 | 0.600 |
| Medicaid rate (%) | 0.067 | 0.030 | 2.25 | 0.025* |
| MC penetration (%) | 0.196 | 0.050 | 3.91 | 0.000*** |
| Income per cap ($K) | 0.559 | 0.322 | 1.74 | 0.082† |
| FMAP (%) | 23.202 | 44.545 | 0.52 | 0.602 |
| SVI (%) | -0.376 | 8.327 | -0.05 | 0.964 |
| Poverty rate (%) | -0.243 | 0.799 | -0.30 | 0.761 |

**Key finding:** Medicaid rate coefficient = 0.067 (p=0.025). Significant at 5% level. MC penetration coefficient = 0.196 (p=0.000).

## Step 5: Panel Fixed Effects (2017-2024)

```sql

        SELECT q.state_code,
               q.core_set_year AS year,
               AVG(q.state_rate) AS avg_quality,
               mc.mc_penetration_pct AS mc_pct,
               bea.per_capita_personal_income / 1000.0 AS income_k
        FROM fact_quality_core_set_combined q
        LEFT JOIN fact_mc_enrollment_summary mc
            ON q.state_code = mc.state_code AND q.core_set_year = mc.year
        LEFT JOIN fact_bea_personal_income bea
            ON q.state_code = bea.state_code AND q.core_set_year = bea.year
        WHERE q.state_rate IS NOT NULL AND q.state_rate > 0
          AND q.core_set_year BETWEEN 2017 AND 2024
        GROUP BY q.state_code, q.core_set_year, mc.mc_penetration_pct, bea.per_capita_personal_income
        HAVING COUNT(*) >= 5
    
```

Panel: **378 observations, 49 states**


### Panel FE Results (Within-Transformation)

| Variable | Coefficient | SE | t | p |
| --- | --- | --- | --- | --- |
| MC penetration (%) | -0.100 | 0.035 | -2.88 | 0.0040** |
| Income ($K) | 0.190 | 0.147 | 1.29 | 0.1961 |
| Year trend | -1.232 | 0.482 | -2.55 | 0.0107* |

N=378, groups=49, within-R²=0.142


**Year trend = -1.232pp/year** — quality is declining nationally.

## Step 6: Difference-in-Differences (FMAP burden)

Treatment: high fiscal burden states (FMAP ≤ 52%). Control: low burden (FMAP ≥ 65%).

Pre: 2017-2019. Post: 2022-2024.

DiD estimate: **1.68pp**, SE=2.17, t=0.77, p=0.440

Treatment change: -2.39pp. Control change: -4.07pp.


---

# Analysis 2: Managed Care Value Assessment

## Panel Fixed Effects: Per-Enrollee Spending ~ MC Penetration

```sql

        SELECT c.state_code, c.fiscal_year AS year,
               SUM(c.total_computable) / NULLIF(e.total_enrollment, 0) AS per_enrollee,
               mc.mc_penetration_pct AS mc_pct,
               bea.per_capita_personal_income / 1000.0 AS income_k
        FROM fact_cms64_multiyear c
        JOIN (
            SELECT state_code, year, MAX(total_enrollment) AS total_enrollment
            FROM fact_enrollment
            GROUP BY state_code, year
        ) e ON c.state_code = e.state_code AND c.fiscal_year = e.year
        LEFT JOIN fact_mc_enrollment_summary mc
            ON c.state_code = mc.state_code AND c.fiscal_year = mc.year
        LEFT JOIN fact_bea_personal_income bea
            ON c.state_code = bea.state_code AND c.fiscal_year = bea.year
        WHERE c.state_code != 'US' AND c.fiscal_year BETWEEN 2018 AND 2024
          AND e.total_enrollment > 100000
        GROUP BY c.state_code, c.fiscal_year, mc.mc_penetration_pct,
                 bea.per_capita_personal_income, e.total_enrollment
        HAVING SUM(c.total_computable) > 0
    
```

Panel: **332 observations, 48 states**


### Panel FE: Per-Enrollee Spending

| Variable | Coefficient | SE | t | p |
| --- | --- | --- | --- | --- |
| MC penetration (%) | $-9.2 | $9.6 | -0.96 | 0.3370 |
| Income ($K) | $-13.8 | $41.8 | -0.33 | 0.7421 |
| Year trend | $432.9 | $142.2 | 3.04 | 0.0023** |

N=332, groups=48, within-R²=0.385

## MCO Medical Loss Ratio Analysis

```sql

        SELECT state_code,
               COUNT(*) AS n_plans,
               ROUND(AVG(adjusted_mlr), 1) AS avg_mlr,
               ROUND(MIN(adjusted_mlr), 1) AS min_mlr,
               SUM(CASE WHEN adjusted_mlr < 85 THEN 1 ELSE 0 END) AS below_85,
               ROUND(SUM(mlr_denominator) / 1e9, 2) AS total_premium_B,
               ROUND(SUM(mlr_denominator) * (1 - AVG(adjusted_mlr)/100) / 1e9, 2) AS admin_profit_B
        FROM fact_mco_mlr
        WHERE adjusted_mlr IS NOT NULL AND adjusted_mlr > 0 AND adjusted_mlr < 120
        GROUP BY state_code
        ORDER BY avg_mlr ASC
    
```

Total MCO premiums: **$1324B**

Average MLR: **91.0%**

Plans below 85% MLR: **274** of 2227 (12.3%)

Estimated admin/profit retention: **$120B/year**


---

# Analysis 3: Nursing Home Ownership & Quality

## Raw Comparison (No Controls)

```sql

        SELECT
            CASE WHEN ownership_type ILIKE '%profit%' AND ownership_type NOT ILIKE '%non%'
                 THEN 'For-Profit'
                 WHEN ownership_type ILIKE '%non%profit%' THEN 'Non-Profit'
                 WHEN ownership_type ILIKE '%gov%' THEN 'Government'
                 ELSE 'Other' END AS ownership,
            CASE WHEN chain_name IS NOT NULL AND chain_name != '' AND chain_name != 'N/A'
                 THEN 'Chain' ELSE 'Independent' END AS affiliation,
            COUNT(*) AS n,
            ROUND(AVG(overall_rating), 2) AS avg_overall,
            ROUND(AVG(staffing_rating), 2) AS avg_staffing,
            ROUND(AVG(qm_rating), 2) AS avg_qm,
            ROUND(AVG(health_inspection_rating), 2) AS avg_inspection
        FROM fact_five_star
        WHERE overall_rating IS NOT NULL
        GROUP BY ownership, affiliation
        ORDER BY avg_overall ASC
    
```


### Five-Star Ratings by Ownership Type

| Ownership | Affiliation | N | Overall | Staffing | QM | Inspection |
| --- | --- | --- | --- | --- | --- | --- |
| For-Profit | Chain | 8674 | 2.79 | 2.58 | 3.66 | 2.64 |
| For-Profit | Independent | 2063 | 2.83 | 2.79 | 3.37 | 2.75 |
| Government | Chain | 337 | 3.06 | 2.31 | 3.93 | 2.95 |
| Non-Profit | Chain | 1101 | 3.42 | 3.61 | 3.63 | 3.12 |
| Government | Independent | 591 | 3.44 | 3.77 | 3.31 | 3.20 |
| Non-Profit | Independent | 1808 | 3.64 | 3.91 | 3.52 | 3.34 |

## State Fixed Effects + Size Controls

```sql

        SELECT state_code,
               overall_rating,
               CASE WHEN ownership_type ILIKE '%profit%' AND ownership_type NOT ILIKE '%non%'
                    THEN 1 ELSE 0 END AS is_for_profit,
               CASE WHEN chain_name IS NOT NULL AND chain_name != '' AND chain_name != 'N/A'
                    THEN 1 ELSE 0 END AS is_chain,
               COALESCE(certified_beds, 0) / 10.0 AS beds_10  -- AUDIT FIX: number_of_certified_beds -> certified_beds
        FROM fact_five_star
        WHERE overall_rating IS NOT NULL
          AND state_code IS NOT NULL AND LENGTH(state_code) = 2
    
```

Sample: **14574 facilities, 53 states**


### State FE + Size Controls

| Variable | Coefficient | SE | t | p |
| --- | --- | --- | --- | --- |
| For-Profit | -0.671 | 0.029 | -23.0 | 0.000000*** |
| Chain-Affiliated | -0.088 | 0.027 | -3.2 | 0.001294** |
| Per 10 Beds | -0.046 | 0.002 | -22.1 | 0.000000*** |

N=14574, groups=53, within-R²=0.083


**Cohen's d = 0.50** (for-profit vs non-for-profit, within-state)


## Worst Chains (≥10 facilities)

```sql

        SELECT chain_name, COUNT(*) AS n,
               ROUND(AVG(overall_rating), 2) AS avg_rating,
               ROUND(AVG(staffing_rating), 2) AS avg_staffing
        FROM fact_five_star
        WHERE chain_name IS NOT NULL AND chain_name != '' AND chain_name != 'N/A'
          AND overall_rating IS NOT NULL
        GROUP BY chain_name HAVING COUNT(*) >= 10
        ORDER BY avg_rating ASC LIMIT 10
    
```


### 10 Worst Chains by Quality

| Chain | Facilities | Avg Rating | Avg Staffing |
| --- | --- | --- | --- |
| RELIANT CARE MANAGEMENT | 30 | 1.17 | 1.00 |
| BRIA HEALTH SERVICES | 15 | 1.20 | 1.00 |
| EASTERN HEALTHCARE GROUP | 17 | 1.24 | 1.35 |
| BEACON HEALTH MANAGEMENT | 17 | 1.29 | 1.71 |
| POINTE MANAGEMENT | 12 | 1.42 | 1.33 |
| PLANTATION MANAGEMENT COMPANY | 16 | 1.44 | 1.25 |
| EVERCARE SKILLED NURSING | 11 | 1.45 | 1.55 |
| SABA HEALTHCARE | 11 | 1.45 | 1.36 |
| AVID HEALTHCARE GROUP | 10 | 1.50 | 2.70 |
| ALLIANCE HEALTH GROUP | 10 | 1.50 | 1.80 |

---

# Analysis 4: Pharmacy Reimbursement Spread (NADAC vs SDUD)

```sql

        WITH latest_nadac AS (
            SELECT ndc, nadac_per_unit, pricing_unit,
                   ROW_NUMBER() OVER (PARTITION BY ndc ORDER BY effective_date DESC) AS rn
            FROM fact_nadac
            WHERE nadac_per_unit IS NOT NULL AND nadac_per_unit > 0
        ),
        sdud_agg AS (
            SELECT ndc,
                   SUM(total_amount_reimbursed) AS total_reimbursed,
                   SUM(units_reimbursed) AS total_units,
                   SUM(number_of_prescriptions) AS total_rx
            FROM fact_sdud_2025
            WHERE state_code != 'XX'
              AND total_amount_reimbursed > 0 AND units_reimbursed > 0
            GROUP BY ndc
        )
        SELECT
            COUNT(*) AS n_drugs,
            SUM(CASE WHEN s.total_reimbursed/s.total_units > n.nadac_per_unit THEN 1 ELSE 0 END) AS n_overpaid,
            ROUND(SUM(CASE WHEN s.total_reimbursed/s.total_units > n.nadac_per_unit
                       THEN (s.total_reimbursed/s.total_units - n.nadac_per_unit) * s.total_units
                       ELSE 0 END) / 1e9, 2) AS overpayment_B,
            ROUND(SUM(CASE WHEN s.total_reimbursed/s.total_units < n.nadac_per_unit
                       THEN (n.nadac_per_unit - s.total_reimbursed/s.total_units) * s.total_units
                       ELSE 0 END) / 1e9, 2) AS underpayment_B,
            ROUND(MEDIAN((s.total_reimbursed/s.total_units) / n.nadac_per_unit), 2) AS median_markup
        FROM sdud_agg s
        JOIN latest_nadac n ON s.ndc = n.ndc AND n.rn = 1
        WHERE s.total_units > 0
    
```

Drugs matched: **23,530**

Drugs overpaid: **21,420** (91%)

Total overpayment: **$4.82B**

Total underpayment: **$1.67B**

Net overpayment: **$3.15B**

Median markup ratio: **2.27x** NADAC


## Robustness: Outlier Sensitivity

Markup cap 100x: **$4.75B**

Markup cap 10x: **$4.32B**

Markup cap 5x: **$3.68B**

Markup cap 3x: **$2.74B**

Markup cap 2x: **$1.93B**


---

# Analysis 5: Opioid Treatment Gap

## MAT Drug Spending by State

```sql

        SELECT state_code,
               SUM(total_amount_reimbursed) / 1e6 AS mat_spending_M,
               SUM(number_of_prescriptions) AS mat_rx
        FROM fact_sdud_2025
        WHERE state_code != 'XX'
          AND (product_name ILIKE '%buprenorphine%' OR product_name ILIKE '%suboxone%'  -- AUDIT FIX: SIMILAR TO returned 0 rows; ILIKE matches correctly
               OR product_name ILIKE '%naloxone%' OR product_name ILIKE '%naltrexone%'
               OR product_name ILIKE '%vivitrol%' OR product_name ILIKE '%sublocade%'
               OR product_name ILIKE '%zubsolv%' OR product_name ILIKE '%subutex%'
               OR product_name ILIKE '%sublocade%')
          AND total_amount_reimbursed > 0
        GROUP BY state_code
        ORDER BY mat_spending_M DESC
    
```

National MAT Medicaid spending: **$978M**

Top 5 states: PA ($70M), MD ($70M), MA ($68M), NY ($61M), MI ($61M)


---
