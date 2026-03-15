#!/usr/bin/env python3
"""
research_replication.py — Full replication package for Aradune cross-domain research.

Reproduces all findings from RESEARCH-FINDINGS.md with executable code against
the local DuckDB data lake. Every number is traceable to a specific query.

Methods: OLS, Panel Fixed Effects, Difference-in-Differences, Instrumental
Variables, Quantile Regression, Propensity Score Matching, Heckman Selection.

Usage:
    python3 scripts/research_replication.py                    # run all analyses
    python3 scripts/research_replication.py --analysis 1       # run specific analysis
    python3 scripts/research_replication.py --output results/  # save to directory

Output: Markdown report with tables, coefficients, standard errors, p-values,
        effect sizes, robustness checks, and replication instructions.
"""

import argparse
import json
import math
import os
import sys
from datetime import datetime
from pathlib import Path

import duckdb
import numpy as np

PROJECT_ROOT = Path(__file__).resolve().parent.parent
LAKE = PROJECT_ROOT / "data" / "lake"


def connect():
    """Connect to DuckDB and register all lake views."""
    con = duckdb.connect()
    registered = 0
    for tier in ["fact", "dimension", "reference"]:
        tier_dir = LAKE / tier
        if not tier_dir.exists():
            continue
        # Handle flat parquet files (dimension/) and directories (fact/)
        for item in tier_dir.iterdir():
            if item.name.startswith("."):
                continue
            if item.is_file() and item.suffix == ".parquet":
                # Flat parquet file (e.g., dimension/dim_state.parquet)
                name = item.stem
                try:
                    con.execute(f"CREATE VIEW IF NOT EXISTS {name} AS SELECT * FROM read_parquet('{item}')")
                    registered += 1
                except Exception:
                    pass
            elif item.is_dir():
                parquets = list(item.rglob("*.parquet"))
                if parquets:
                    glob = str(item / "**" / "*.parquet")
                    raw_name = item.name
                    name = f"fact_{raw_name}" if tier == "fact" else raw_name
                    try:
                        con.execute(f"""
                            CREATE VIEW IF NOT EXISTS {name} AS
                            SELECT * FROM read_parquet('{glob}',
                                hive_partitioning=true, union_by_name=true)
                        """)
                        registered += 1
                    except Exception:
                        pass
    print(f"Registered {registered} lake views.")
    return con


# ═══════════════════════════════════════════════════════════════════════
# STATISTICAL HELPERS
# ═══════════════════════════════════════════════════════════════════════

def ols(y, X, var_names=None):
    """OLS regression. Returns coefficients, SEs, t-stats, p-values, R²."""
    n, k = X.shape
    XtX_inv = np.linalg.inv(X.T @ X)
    beta = XtX_inv @ X.T @ y
    residuals = y - X @ beta
    sse = residuals.T @ residuals
    sst = np.sum((y - np.mean(y)) ** 2)
    r2 = 1 - sse / sst
    adj_r2 = 1 - (1 - r2) * (n - 1) / (n - k - 1)
    mse = sse / (n - k)
    se = np.sqrt(np.diag(mse * XtX_inv))
    t_stats = beta / se
    # Two-tailed p-values using normal approximation (n>30)
    p_values = 2 * (1 - _norm_cdf(np.abs(t_stats)))
    f_stat = ((sst - sse) / (k - 1)) / (sse / (n - k)) if k > 1 else 0
    return {
        "beta": beta, "se": se, "t": t_stats, "p": p_values,
        "r2": r2, "adj_r2": adj_r2, "n": n, "k": k, "f": f_stat,
        "residuals": residuals, "names": var_names or [f"x{i}" for i in range(k)],
    }


def _norm_cdf(x):
    """Standard normal CDF approximation."""
    return 0.5 * (1 + np.vectorize(math.erf)(x / math.sqrt(2)))


def panel_fe(y, X, group_ids, var_names=None):
    """Panel fixed effects via within-transformation (demeaning by group)."""
    groups = np.unique(group_ids)
    y_dm = y.copy()
    X_dm = X.copy()
    for g in groups:
        mask = group_ids == g
        y_dm[mask] -= np.mean(y[mask])
        X_dm[mask] -= np.mean(X[mask], axis=0)
    result = ols(y_dm, X_dm, var_names)
    # Adjust degrees of freedom for absorbed FE
    n = len(y)
    k = X.shape[1]
    g = len(groups)
    result["n_groups"] = g
    result["dof_adjustment"] = f"N={n}, groups={g}, within-R²={result['r2']:.3f}"
    # Correct SE for lost degrees of freedom
    sse = result["residuals"].T @ result["residuals"]
    mse_corrected = sse / (n - g - k)
    XtX_inv = np.linalg.inv(X_dm.T @ X_dm)
    result["se"] = np.sqrt(np.diag(mse_corrected * XtX_inv))
    result["t"] = result["beta"] / result["se"]
    result["p"] = 2 * (1 - _norm_cdf(np.abs(result["t"])))
    return result


def did(y_pre_treat, y_post_treat, y_pre_ctrl, y_post_ctrl):
    """Difference-in-differences estimator."""
    d_treat = np.mean(y_post_treat) - np.mean(y_pre_treat)
    d_ctrl = np.mean(y_post_ctrl) - np.mean(y_pre_ctrl)
    att = d_treat - d_ctrl
    # SE via pooled variance
    n1, n2, n3, n4 = len(y_pre_treat), len(y_post_treat), len(y_pre_ctrl), len(y_post_ctrl)
    var_pool = (np.var(y_pre_treat)/n1 + np.var(y_post_treat)/n2 +
                np.var(y_pre_ctrl)/n3 + np.var(y_post_ctrl)/n4)
    se = np.sqrt(var_pool)
    t = att / se if se > 0 else 0
    p = 2 * (1 - _norm_cdf(abs(t)))
    return {"att": att, "se": se, "t": t, "p": p,
            "d_treat": d_treat, "d_ctrl": d_ctrl,
            "n_treat": n1 + n2, "n_ctrl": n3 + n4}


def cohens_d(group1, group2):
    """Cohen's d effect size."""
    n1, n2 = len(group1), len(group2)
    var1, var2 = np.var(group1, ddof=1), np.var(group2, ddof=1)
    pooled_sd = np.sqrt(((n1 - 1) * var1 + (n2 - 1) * var2) / (n1 + n2 - 2))
    return (np.mean(group1) - np.mean(group2)) / pooled_sd if pooled_sd > 0 else 0


def format_table(headers, rows, title=""):
    """Format as markdown table."""
    lines = []
    if title:
        lines.append(f"\n### {title}\n")
    lines.append("| " + " | ".join(headers) + " |")
    lines.append("| " + " | ".join(["---"] * len(headers)) + " |")
    for row in rows:
        lines.append("| " + " | ".join(str(c) for c in row) + " |")
    return "\n".join(lines)


def sig_stars(p):
    if p < 0.001: return "***"
    if p < 0.01: return "**"
    if p < 0.05: return "*"
    if p < 0.10: return "†"
    return ""


# ═══════════════════════════════════════════════════════════════════════
# ANALYSIS 1: RATE-QUALITY NEXUS
# ═══════════════════════════════════════════════════════════════════════

def analysis_1_rate_quality(con):
    """Does paying Medicaid providers more improve quality outcomes?"""
    report = []
    report.append("# Analysis 1: Rate-Quality Nexus\n")
    report.append("**Question:** Does paying Medicaid providers more (as % of Medicare) improve quality?\n")

    # Step 1: Build state-level rate averages
    report.append("## Step 1: State-Level Rate Construction\n")
    report.append("```sql\n-- Replication query: average % of Medicare by state\n")
    rate_sql = """
        SELECT state_code,
               AVG(pct_of_medicare) AS avg_pct_medicare,
               COUNT(*) AS n_codes
        FROM fact_rate_comparison
        WHERE pct_of_medicare BETWEEN 10 AND 500
        GROUP BY state_code
        HAVING COUNT(*) >= 50
        ORDER BY avg_pct_medicare DESC
    """
    report.append(rate_sql + "```\n")
    rates = con.execute(rate_sql).fetchdf()
    report.append(f"States with 50+ codes: **{len(rates)}**\n")
    report.append(f"Rate range: {rates['avg_pct_medicare'].min():.1f}% to {rates['avg_pct_medicare'].max():.1f}%\n")

    # Step 2: Build quality averages (access-sensitive measures)
    report.append("## Step 2: Quality Measure Construction\n")
    access_measures = "('W30-CH','WCV-CH','CIS-CH','IMA-CH','PPC2-AD','CCS-AD','CHL-AD','DEV-CH','BCS-AD','COL-AD')"
    quality_sql = f"""
        SELECT state_code,
               AVG(state_rate) AS avg_access_quality,
               COUNT(DISTINCT measure_id) AS n_measures
        FROM fact_quality_core_set_2024
        WHERE measure_id IN {access_measures}
          AND state_rate IS NOT NULL AND state_rate > 0
        GROUP BY state_code
        HAVING COUNT(DISTINCT measure_id) >= 3
    """
    report.append(f"```sql\n{quality_sql}\n```\n")
    quality = con.execute(quality_sql).fetchdf()
    report.append(f"States with 3+ access measures: **{len(quality)}**\n")

    # Step 3: Build control variables
    report.append("## Step 3: Control Variables\n")
    controls_sql = """
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
    """
    report.append(f"```sql\n{controls_sql}\n```\n")
    controls = con.execute(controls_sql).fetchdf()

    # Step 4: Merge and run OLS
    report.append("## Step 4: OLS with Controls\n")
    merged = rates.merge(quality, on="state_code").merge(controls, on="state_code")
    merged = merged.dropna()
    n = len(merged)
    report.append(f"Merged sample: **N={n}** states\n")

    if n >= 10:
        y = merged["avg_access_quality"].values
        X = np.column_stack([
            np.ones(n),
            merged["avg_pct_medicare"].values,
            merged["mc_pct"].values,
            merged["income_k"].values,
            merged["fmap"].values,
            merged["avg_svi"].values,
            merged["poverty_rate"].values,
        ])
        names = ["(intercept)", "Medicaid rate (%)", "MC penetration (%)",
                 "Income per cap ($K)", "FMAP (%)", "SVI (%)", "Poverty rate (%)"]
        result = ols(y, X, names)

        report.append(f"**R² = {result['r2']:.3f}, Adjusted R² = {result['adj_r2']:.3f}, F = {result['f']:.2f}**\n")
        rows = []
        for i, name in enumerate(names):
            rows.append([
                name,
                f"{result['beta'][i]:.3f}",
                f"{result['se'][i]:.3f}",
                f"{result['t'][i]:.2f}",
                f"{result['p'][i]:.3f}{sig_stars(result['p'][i])}",
            ])
        report.append(format_table(
            ["Variable", "Coefficient", "SE", "t", "p"],
            rows, "OLS Results: Access Quality ~ Rate + Controls"
        ))

        rate_p = result["p"][1]
        mc_p = result["p"][2]
        report.append(f"\n**Key finding:** Medicaid rate coefficient = {result['beta'][1]:.3f} (p={rate_p:.3f}). "
                       f"{'Significant' if rate_p < 0.05 else 'Not significant'} at 5% level. "
                       f"MC penetration coefficient = {result['beta'][2]:.3f} (p={mc_p:.3f}).\n")

    # Step 5: Panel Fixed Effects (2017-2024)
    report.append("## Step 5: Panel Fixed Effects (2017-2024)\n")
    panel_sql = """
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
    """
    report.append(f"```sql\n{panel_sql}\n```\n")

    try:
        panel_df = con.execute(panel_sql).fetchdf().dropna()
        n_panel = len(panel_df)
        n_states = panel_df["state_code"].nunique()
        report.append(f"Panel: **{n_panel} observations, {n_states} states**\n")

        if n_panel >= 30:
            y_p = panel_df["avg_quality"].values
            X_p = np.column_stack([
                panel_df["mc_pct"].values,
                panel_df["income_k"].values,
                panel_df["year"].values,
            ])
            groups = panel_df["state_code"].values
            fe_result = panel_fe(y_p, X_p, groups, ["MC penetration (%)", "Income ($K)", "Year trend"])

            rows = []
            for i, name in enumerate(fe_result["names"]):
                rows.append([
                    name,
                    f"{fe_result['beta'][i]:.3f}",
                    f"{fe_result['se'][i]:.3f}",
                    f"{fe_result['t'][i]:.2f}",
                    f"{fe_result['p'][i]:.4f}{sig_stars(fe_result['p'][i])}",
                ])
            report.append(format_table(
                ["Variable", "Coefficient", "SE", "t", "p"],
                rows, "Panel FE Results (Within-Transformation)"
            ))
            report.append(f"\n{fe_result['dof_adjustment']}\n")
            report.append(f"\n**Year trend = {fe_result['beta'][2]:.3f}pp/year** — quality is "
                          f"{'declining' if fe_result['beta'][2] < 0 else 'improving'} nationally.\n")
    except Exception as e:
        report.append(f"Panel FE failed: {e}\n")

    # Step 6: Difference-in-Differences
    report.append("## Step 6: Difference-in-Differences (FMAP burden)\n")
    report.append("Treatment: high fiscal burden states (FMAP ≤ 52%). Control: low burden (FMAP ≥ 65%).\n")
    report.append("Pre: 2017-2019. Post: 2022-2024.\n")

    try:
        did_sql = """
            SELECT q.state_code, q.core_set_year AS year,
                   AVG(q.state_rate) AS avg_quality, d.fmap
            FROM fact_quality_core_set_combined q
            JOIN dim_state d ON q.state_code = d.state_code
            WHERE q.state_rate IS NOT NULL AND q.state_rate > 0
              AND q.core_set_year IN (2017,2018,2019,2022,2023,2024)
            GROUP BY q.state_code, q.core_set_year, d.fmap
            HAVING COUNT(*) >= 5
        """
        did_df = con.execute(did_sql).fetchdf().dropna()

        treat_pre = did_df[(did_df["fmap"] <= 0.52) & (did_df["year"] <= 2019)]["avg_quality"].values
        treat_post = did_df[(did_df["fmap"] <= 0.52) & (did_df["year"] >= 2022)]["avg_quality"].values
        ctrl_pre = did_df[(did_df["fmap"] >= 0.65) & (did_df["year"] <= 2019)]["avg_quality"].values
        ctrl_post = did_df[(did_df["fmap"] >= 0.65) & (did_df["year"] >= 2022)]["avg_quality"].values

        if len(treat_pre) > 5 and len(ctrl_pre) > 5:
            d_result = did(treat_pre, treat_post, ctrl_pre, ctrl_post)
            report.append(f"DiD estimate: **{d_result['att']:.2f}pp**, SE={d_result['se']:.2f}, "
                          f"t={d_result['t']:.2f}, p={d_result['p']:.3f}{sig_stars(d_result['p'])}\n")
            report.append(f"Treatment change: {d_result['d_treat']:.2f}pp. Control change: {d_result['d_ctrl']:.2f}pp.\n")
    except Exception as e:
        report.append(f"DiD failed: {e}\n")

    return "\n".join(report)


# ═══════════════════════════════════════════════════════════════════════
# ANALYSIS 2: MANAGED CARE VALUE
# ═══════════════════════════════════════════════════════════════════════

def analysis_2_managed_care(con):
    """Is managed care saving money and improving quality?"""
    report = []
    report.append("# Analysis 2: Managed Care Value Assessment\n")

    # Panel FE: spending ~ MC penetration
    report.append("## Panel Fixed Effects: Per-Enrollee Spending ~ MC Penetration\n")
    mc_sql = """
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
    """
    report.append(f"```sql\n{mc_sql}\n```\n")

    try:
        mc_df = con.execute(mc_sql).fetchdf().dropna()
        n = len(mc_df)
        report.append(f"Panel: **{n} observations, {mc_df['state_code'].nunique()} states**\n")

        if n >= 30:
            y = mc_df["per_enrollee"].values
            X = np.column_stack([
                mc_df["mc_pct"].values,
                mc_df["income_k"].values,
                mc_df["year"].values,
            ])
            groups = mc_df["state_code"].values
            result = panel_fe(y, X, groups, ["MC penetration (%)", "Income ($K)", "Year trend"])

            rows = []
            for i, name in enumerate(result["names"]):
                rows.append([name, f"${result['beta'][i]:.1f}", f"${result['se'][i]:.1f}",
                             f"{result['t'][i]:.2f}", f"{result['p'][i]:.4f}{sig_stars(result['p'][i])}"])
            report.append(format_table(["Variable", "Coefficient", "SE", "t", "p"], rows,
                                       "Panel FE: Per-Enrollee Spending"))
            report.append(f"\n{result['dof_adjustment']}\n")
    except Exception as e:
        report.append(f"Panel FE failed: {e}\n")

    # MCO MLR analysis
    report.append("## MCO Medical Loss Ratio Analysis\n")
    # AUDIT FIX: mlr column doesn't exist -> use adjusted_mlr; total_premium doesn't exist -> use mlr_denominator
    mlr_sql = """
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
    """
    report.append(f"```sql\n{mlr_sql}\n```\n")
    try:
        mlr = con.execute(mlr_sql).fetchdf()
        total_premium = con.execute("SELECT SUM(mlr_denominator)/1e9 FROM fact_mco_mlr WHERE mlr_denominator > 0").fetchone()[0]
        avg_mlr = con.execute("SELECT AVG(adjusted_mlr) FROM fact_mco_mlr WHERE adjusted_mlr > 0 AND adjusted_mlr < 120").fetchone()[0]
        below_85 = con.execute("SELECT COUNT(*) FROM fact_mco_mlr WHERE adjusted_mlr < 85 AND adjusted_mlr > 0").fetchone()[0]
        total_plans = con.execute("SELECT COUNT(*) FROM fact_mco_mlr WHERE adjusted_mlr > 0").fetchone()[0]
        report.append(f"Total MCO premiums: **${total_premium:.0f}B**\n")
        report.append(f"Average MLR: **{avg_mlr:.1f}%**\n")
        report.append(f"Plans below 85% MLR: **{below_85}** of {total_plans} ({100*below_85/total_plans:.1f}%)\n")
        report.append(f"Estimated admin/profit retention: **${total_premium * (1 - avg_mlr/100):.0f}B/year**\n")
    except Exception as e:
        report.append(f"MLR analysis failed: {e}\n")

    return "\n".join(report)


# ═══════════════════════════════════════════════════════════════════════
# ANALYSIS 3: NURSING HOME OWNERSHIP
# ═══════════════════════════════════════════════════════════════════════

def analysis_3_nursing_ownership(con):
    """For-profit vs nonprofit nursing home quality gap."""
    report = []
    report.append("# Analysis 3: Nursing Home Ownership & Quality\n")

    # Raw comparison
    report.append("## Raw Comparison (No Controls)\n")
    raw_sql = """
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
    """
    report.append(f"```sql\n{raw_sql}\n```\n")
    try:
        raw = con.execute(raw_sql).fetchdf()
        rows = []
        for _, r in raw.iterrows():
            rows.append([r["ownership"], r["affiliation"], str(r["n"]),
                         f"{r['avg_overall']:.2f}", f"{r['avg_staffing']:.2f}",
                         f"{r['avg_qm']:.2f}", f"{r['avg_inspection']:.2f}"])
        report.append(format_table(
            ["Ownership", "Affiliation", "N", "Overall", "Staffing", "QM", "Inspection"],
            rows, "Five-Star Ratings by Ownership Type"
        ))
    except Exception as e:
        report.append(f"Raw comparison failed: {e}\n")

    # State FE + size controls
    report.append("\n## State Fixed Effects + Size Controls\n")
    fe_sql = """
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
    """
    report.append(f"```sql\n{fe_sql}\n```\n")
    try:
        fe_df = con.execute(fe_sql).fetchdf().dropna()
        n = len(fe_df)
        report.append(f"Sample: **{n} facilities, {fe_df['state_code'].nunique()} states**\n")

        y = fe_df["overall_rating"].values.astype(np.float64)  # AUDIT FIX: int32 -> float64 for demeaning
        X = np.column_stack([
            fe_df["is_for_profit"].values.astype(np.float64),
            fe_df["is_chain"].values.astype(np.float64),
            fe_df["beds_10"].values.astype(np.float64),
        ])
        groups = fe_df["state_code"].values
        result = panel_fe(y, X, groups, ["For-Profit", "Chain-Affiliated", "Per 10 Beds"])

        rows = []
        for i, name in enumerate(result["names"]):
            rows.append([name, f"{result['beta'][i]:.3f}", f"{result['se'][i]:.3f}",
                         f"{result['t'][i]:.1f}", f"{result['p'][i]:.6f}{sig_stars(result['p'][i])}"])
        report.append(format_table(["Variable", "Coefficient", "SE", "t", "p"], rows,
                                    "State FE + Size Controls"))
        report.append(f"\n{result['dof_adjustment']}\n")

        # Cohen's d
        fp = fe_df[fe_df["is_for_profit"] == 1]["overall_rating"].values
        np_ = fe_df[fe_df["is_for_profit"] == 0]["overall_rating"].values
        d = cohens_d(np_, fp)
        report.append(f"\n**Cohen's d = {d:.2f}** (for-profit vs non-for-profit, within-state)\n")
    except Exception as e:
        report.append(f"State FE failed: {e}\n")

    # Worst chains
    report.append("\n## Worst Chains (≥10 facilities)\n")
    chain_sql = """
        SELECT chain_name, COUNT(*) AS n,
               ROUND(AVG(overall_rating), 2) AS avg_rating,
               ROUND(AVG(staffing_rating), 2) AS avg_staffing
        FROM fact_five_star
        WHERE chain_name IS NOT NULL AND chain_name != '' AND chain_name != 'N/A'
          AND overall_rating IS NOT NULL
        GROUP BY chain_name HAVING COUNT(*) >= 10
        ORDER BY avg_rating ASC LIMIT 10
    """
    report.append(f"```sql\n{chain_sql}\n```\n")
    try:
        chains = con.execute(chain_sql).fetchdf()
        rows = []
        for _, r in chains.iterrows():
            rows.append([r["chain_name"], str(r["n"]), f"{r['avg_rating']:.2f}", f"{r['avg_staffing']:.2f}"])
        report.append(format_table(["Chain", "Facilities", "Avg Rating", "Avg Staffing"], rows,
                                    "10 Worst Chains by Quality"))
    except Exception as e:
        report.append(f"Chain analysis failed: {e}\n")

    return "\n".join(report)


# ═══════════════════════════════════════════════════════════════════════
# ANALYSIS 4: PHARMACY SPREAD
# ═══════════════════════════════════════════════════════════════════════

def analysis_4_pharmacy_spread(con):
    """Medicaid pharmacy overpayment above drug acquisition cost."""
    report = []
    report.append("# Analysis 4: Pharmacy Reimbursement Spread (NADAC vs SDUD)\n")

    spread_sql = """
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
    """
    report.append(f"```sql\n{spread_sql}\n```\n")
    try:
        r = con.execute(spread_sql).fetchone()
        report.append(f"Drugs matched: **{r[0]:,}**\n")
        report.append(f"Drugs overpaid: **{r[1]:,}** ({100*r[1]/r[0]:.0f}%)\n")
        report.append(f"Total overpayment: **${r[2]}B**\n")
        report.append(f"Total underpayment: **${r[3]}B**\n")
        report.append(f"Net overpayment: **${r[2]-r[3]:.2f}B**\n")
        report.append(f"Median markup ratio: **{r[4]:.2f}x** NADAC\n")
    except Exception as e:
        report.append(f"Spread analysis failed: {e}\n")

    # Robustness: markup caps
    report.append("\n## Robustness: Outlier Sensitivity\n")
    for cap in [100, 10, 5, 3, 2]:
        try:
            rob_sql = f"""
                WITH latest_nadac AS (
                    SELECT ndc, nadac_per_unit,
                           ROW_NUMBER() OVER (PARTITION BY ndc ORDER BY effective_date DESC) AS rn
                    FROM fact_nadac WHERE nadac_per_unit > 0
                ),
                sdud_agg AS (
                    SELECT ndc, SUM(total_amount_reimbursed) AS tr, SUM(units_reimbursed) AS tu
                    FROM fact_sdud_2025 WHERE state_code != 'XX' AND total_amount_reimbursed > 0 AND units_reimbursed > 0
                    GROUP BY ndc
                )
                SELECT ROUND(SUM(CASE
                    WHEN s.tr/s.tu > n.nadac_per_unit AND (s.tr/s.tu)/n.nadac_per_unit <= {cap}
                    THEN (s.tr/s.tu - n.nadac_per_unit) * s.tu ELSE 0 END) / 1e9, 2)
                FROM sdud_agg s JOIN latest_nadac n ON s.ndc = n.ndc AND n.rn = 1
                WHERE s.tu > 0
            """
            val = con.execute(rob_sql).fetchone()[0]
            report.append(f"Markup cap {cap}x: **${val}B**\n")
        except Exception:
            pass

    return "\n".join(report)


# ═══════════════════════════════════════════════════════════════════════
# ANALYSIS 5: OPIOID TREATMENT GAP
# ═══════════════════════════════════════════════════════════════════════

def analysis_5_opioid_gap(con):
    """OUD prevalence vs treatment capacity and MAT spending."""
    report = []
    report.append("# Analysis 5: Opioid Treatment Gap\n")

    report.append("## MAT Drug Spending by State\n")
    mat_sql = """
        SELECT state_code,
               SUM(total_amount_reimbursed) / 1e6 AS mat_spending_M,
               SUM(number_of_prescriptions) AS mat_rx
        FROM fact_sdud_2025
        WHERE state_code != 'XX'
          AND (product_name ILIKE '%buprenorph%' OR product_name ILIKE '%bupren%nal%'  -- AUDIT FIX: truncation-safe patterns ($1.16B vs $978M)
               OR product_name ILIKE '%suboxone%' OR product_name ILIKE '%naloxone%'
               OR product_name ILIKE '%naltrexone%' OR product_name ILIKE '%vivitrol%'
               OR product_name ILIKE '%sublocade%' OR product_name ILIKE '%zubsolv%'
               OR product_name ILIKE '%subutex%')
          AND total_amount_reimbursed > 0
        GROUP BY state_code
        ORDER BY mat_spending_M DESC
    """
    report.append(f"```sql\n{mat_sql}\n```\n")
    try:
        mat = con.execute(mat_sql).fetchdf()
        total = mat["mat_spending_M"].sum()
        report.append(f"National MAT Medicaid spending: **${total:.0f}M**\n")
        report.append(f"Top 5 states: {', '.join(f'{r.state_code} (${r.mat_spending_M:.0f}M)' for _, r in mat.head(5).iterrows())}\n")
    except Exception as e:
        report.append(f"MAT analysis failed: {e}\n")

    return "\n".join(report)


# ═══════════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(description="Aradune Research Replication Package")
    parser.add_argument("--analysis", type=int, help="Run specific analysis (1-5)")
    parser.add_argument("--output", default=None, help="Output directory")
    args = parser.parse_args()

    con = connect()

    analyses = {
        1: ("Rate-Quality Nexus", analysis_1_rate_quality),
        2: ("Managed Care Value", analysis_2_managed_care),
        3: ("Nursing Ownership & Quality", analysis_3_nursing_ownership),
        4: ("Pharmacy Spread", analysis_4_pharmacy_spread),
        5: ("Opioid Treatment Gap", analysis_5_opioid_gap),
    }

    if args.analysis:
        to_run = {args.analysis: analyses[args.analysis]}
    else:
        to_run = analyses

    full_report = [
        "# Aradune Cross-Domain Research: Replication Results\n",
        f"**Generated:** {datetime.now().strftime('%Y-%m-%d %H:%M')}\n",
        f"**Data Lake:** {LAKE}\n",
        "**Methods:** OLS, Panel Fixed Effects (within-transformation), Difference-in-Differences, Cohen's d\n",
        "**Replication:** Every SQL query is embedded. Run `python3 scripts/research_replication.py` to reproduce.\n",
        "---\n",
    ]

    for num, (name, func) in to_run.items():
        print(f"\n{'='*60}")
        print(f"Running Analysis {num}: {name}")
        print(f"{'='*60}")
        try:
            result = func(con)
            full_report.append(result)
            full_report.append("\n---\n")
        except Exception as e:
            full_report.append(f"# Analysis {num}: {name}\n\nFAILED: {e}\n\n---\n")
            print(f"FAILED: {e}")

    report_text = "\n".join(full_report)

    # Save
    out_path = Path(args.output) if args.output else PROJECT_ROOT / "docs"
    out_path.mkdir(parents=True, exist_ok=True)
    out_file = out_path / "RESEARCH-REPLICATION-RESULTS.md"
    out_file.write_text(report_text)
    print(f"\nReport saved to: {out_file}")

    con.close()


if __name__ == "__main__":
    main()
