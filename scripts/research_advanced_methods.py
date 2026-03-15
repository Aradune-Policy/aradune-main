#!/usr/bin/env python3
"""
research_advanced_methods.py — Advanced causal inference and ML methods for Aradune research.

Supplements research_replication.py with stronger identification strategies:
- Instrumental Variables (2SLS) using GPCI as instrument for Medicaid rates
- Variance Inflation Factor diagnostics for multicollinearity
- Propensity Score Matching for nursing home ownership
- Change-of-Ownership (CHOW) event study for nursing quality
- Random Forest feature importance for pharmacy spread drivers
- Quantile regression for heterogeneous rate-quality effects
- Enhanced DiD with proper state-level clustering
- Synthetic control preparation for MC expansion analysis

Usage:
    python3 scripts/research_advanced_methods.py                    # run all
    python3 scripts/research_advanced_methods.py --analysis 1       # specific analysis
    python3 scripts/research_advanced_methods.py --output results/  # output dir

Requires: numpy, scipy, sklearn, statsmodels, duckdb, pandas
"""

import argparse
import math
import warnings
from datetime import datetime
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd
from scipy import stats as sp_stats

warnings.filterwarnings("ignore", category=FutureWarning)

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
        for item in tier_dir.iterdir():
            if item.name.startswith("."):
                continue
            if item.is_file() and item.suffix == ".parquet":
                try:
                    con.execute(f"CREATE VIEW IF NOT EXISTS {item.stem} AS SELECT * FROM read_parquet('{item}')")
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

def _norm_cdf(x):
    return 0.5 * (1 + np.vectorize(math.erf)(x / math.sqrt(2)))


def sig_stars(p):
    if p < 0.001: return "***"
    if p < 0.01: return "**"
    if p < 0.05: return "*"
    if p < 0.10: return "†"
    return ""


def fmt_table(headers, rows, title=""):
    lines = []
    if title:
        lines.append(f"\n### {title}\n")
    lines.append("| " + " | ".join(headers) + " |")
    lines.append("| " + " | ".join(["---"] * len(headers)) + " |")
    for row in rows:
        lines.append("| " + " | ".join(str(c) for c in row) + " |")
    return "\n".join(lines)


def vif(X, var_names=None):
    """Compute Variance Inflation Factors for each predictor column."""
    n, k = X.shape
    vifs = []
    names = var_names or [f"x{i}" for i in range(k)]
    for j in range(k):
        y_j = X[:, j]
        X_other = np.delete(X, j, axis=1)
        # Add intercept to the auxiliary regression
        X_aug = np.column_stack([np.ones(n), X_other])
        try:
            beta = np.linalg.lstsq(X_aug, y_j, rcond=None)[0]
            resid = y_j - X_aug @ beta
            ss_res = resid @ resid
            ss_tot = np.sum((y_j - np.mean(y_j)) ** 2)
            r2_j = 1 - ss_res / ss_tot if ss_tot > 0 else 0
            vif_j = 1 / (1 - r2_j) if r2_j < 1 else float("inf")
        except Exception:
            vif_j = float("inf")
        vifs.append((names[j], vif_j))
    return vifs


def ols_full(y, X, var_names=None):
    """OLS with full diagnostics including VIF, heteroskedasticity-robust SEs."""
    n, k = X.shape
    beta = np.linalg.lstsq(X, y, rcond=None)[0]
    resid = y - X @ beta
    sse = resid @ resid
    sst = np.sum((y - np.mean(y)) ** 2)
    r2 = 1 - sse / sst
    adj_r2 = 1 - (1 - r2) * (n - 1) / (n - k)
    mse = sse / (n - k)

    # Classical SEs
    XtX_inv = np.linalg.inv(X.T @ X)
    se_classical = np.sqrt(np.diag(mse * XtX_inv))

    # HC1 (White) robust SEs
    meat = X.T @ np.diag(resid ** 2 * n / (n - k)) @ X
    V_robust = XtX_inv @ meat @ XtX_inv
    se_robust = np.sqrt(np.diag(V_robust))

    t_stats = beta / se_robust
    p_values = 2 * (1 - _norm_cdf(np.abs(t_stats)))
    f_stat = ((sst - sse) / (k - 1)) / (sse / (n - k)) if k > 1 else 0

    names = var_names or [f"x{i}" for i in range(k)]

    # VIF (skip intercept column if present)
    vif_results = vif(X[:, 1:], names[1:]) if k > 1 else []

    return {
        "beta": beta, "se": se_robust, "se_classical": se_classical,
        "t": t_stats, "p": p_values,
        "r2": r2, "adj_r2": adj_r2, "n": n, "k": k, "f": f_stat,
        "residuals": resid, "names": names, "vif": vif_results,
    }


def iv_2sls(y, X_endog, X_exog, Z, endog_name="endog", exog_names=None, iv_names=None):
    """Two-Stage Least Squares IV estimation.

    y: dependent variable (n,)
    X_endog: endogenous regressors (n, p)
    X_exog: exogenous controls including intercept (n, q)
    Z: excluded instruments (n, m) — must have m >= p
    """
    n = len(y)
    if X_endog.ndim == 1:
        X_endog = X_endog.reshape(-1, 1)
    if Z.ndim == 1:
        Z = Z.reshape(-1, 1)
    p = X_endog.shape[1]
    q = X_exog.shape[1]
    m = Z.shape[1]

    # Stage 1: regress endogenous on instruments + exogenous
    W = np.column_stack([X_exog, Z])
    beta_1 = np.linalg.lstsq(W, X_endog, rcond=None)[0]
    X_hat = W @ beta_1  # fitted values

    # First-stage F-statistic
    resid_1 = X_endog - X_hat
    ss_res_1 = np.sum(resid_1 ** 2, axis=0)
    X_exog_only = np.linalg.lstsq(X_exog, X_endog, rcond=None)[0]
    X_hat_restricted = X_exog @ X_exog_only
    ss_res_restricted = np.sum((X_endog - X_hat_restricted) ** 2, axis=0)
    f_first = ((ss_res_restricted - ss_res_1) / m) / (ss_res_1 / (n - q - m))
    f_first_val = float(f_first[0]) if f_first.ndim > 0 else float(f_first)

    # Stage 2: regress y on fitted endogenous + exogenous
    X_2 = np.column_stack([X_exog, X_hat])
    beta_2 = np.linalg.lstsq(X_2, y, rcond=None)[0]

    # Correct residuals use actual X, not fitted X
    X_actual = np.column_stack([X_exog, X_endog])
    resid_2 = y - X_actual @ beta_2
    sse = resid_2 @ resid_2
    sst = np.sum((y - np.mean(y)) ** 2)
    mse = sse / (n - q - p)

    # SE from stage 2 design matrix
    X2tX2_inv = np.linalg.inv(X_2.T @ X_2)
    se = np.sqrt(np.diag(mse * X2tX2_inv))
    t_stats = beta_2 / se
    p_values = 2 * (1 - _norm_cdf(np.abs(t_stats)))

    all_names = (exog_names or [f"exog_{i}" for i in range(q)]) + \
                ([endog_name] if p == 1 else [f"{endog_name}_{i}" for i in range(p)])

    return {
        "beta": beta_2, "se": se, "t": t_stats, "p": p_values,
        "n": n, "r2": 1 - sse / sst, "names": all_names,
        "f_first_stage": f_first_val,
        "first_stage_beta": beta_1,
    }


def quantile_reg(y, X, tau=0.5, var_names=None, max_iter=100):
    """Quantile regression via iteratively reweighted least squares.

    tau: quantile (0.1 = bottom 10%, 0.5 = median, 0.9 = top 10%)
    """
    n, k = X.shape
    # Initialize with OLS
    beta = np.linalg.lstsq(X, y, rcond=None)[0]

    for _ in range(max_iter):
        resid = y - X @ beta
        # Asymmetric weights
        weights = np.where(resid >= 0, tau, 1 - tau)
        weights = np.maximum(weights / np.maximum(np.abs(resid), 1e-6), 1e-6)
        W = np.diag(weights)
        try:
            beta_new = np.linalg.solve(X.T @ W @ X, X.T @ W @ y)
        except np.linalg.LinAlgError:
            break
        if np.max(np.abs(beta_new - beta)) < 1e-6:
            beta = beta_new
            break
        beta = beta_new

    # Bootstrap SEs
    n_boot = 200
    betas_boot = []
    rng = np.random.RandomState(42)
    for _ in range(n_boot):
        idx = rng.choice(n, n, replace=True)
        try:
            b = np.linalg.lstsq(X[idx], y[idx], rcond=None)[0]
            betas_boot.append(b)
        except Exception:
            pass
    se = np.std(betas_boot, axis=0) if betas_boot else np.zeros(k)
    t = beta / np.maximum(se, 1e-10)
    p = 2 * (1 - _norm_cdf(np.abs(t)))

    return {
        "beta": beta, "se": se, "t": t, "p": p,
        "tau": tau, "n": n,
        "names": var_names or [f"x{i}" for i in range(k)],
    }


# ═══════════════════════════════════════════════════════════════════════
# ANALYSIS 1: RATE-QUALITY WITH IV, VIF, QUANTILE REGRESSION
# ═══════════════════════════════════════════════════════════════════════

def analysis_1_enhanced(con):
    """Rate-Quality with IV (GPCI instrument), VIF diagnostics, quantile regression."""
    report = []
    report.append("# Analysis 1 Enhanced: Rate-Quality with IV, VIF, Quantile Regression\n")

    # Build the merged dataset
    rate_sql = """
        SELECT state_code, AVG(pct_of_medicare) AS avg_pct_medicare, COUNT(*) AS n_codes
        FROM fact_rate_comparison
        WHERE pct_of_medicare BETWEEN 10 AND 500
        GROUP BY state_code HAVING COUNT(*) >= 50
    """
    quality_sql = """
        SELECT state_code, AVG(state_rate) AS avg_access_quality, COUNT(DISTINCT measure_id) AS n_measures
        FROM fact_quality_core_set_2024
        WHERE measure_id IN ('W30-CH','WCV-CH','CIS-CH','IMA-CH','PPC2-AD','CCS-AD','CHL-AD','DEV-CH','BCS-AD','COL-AD')
          AND state_rate IS NOT NULL AND state_rate > 0
        GROUP BY state_code HAVING COUNT(DISTINCT measure_id) >= 3
    """
    controls_sql = """
        SELECT d.state_code, d.fmap,
               COALESCE(mc.mc_penetration_pct, 0) AS mc_pct,
               bea.per_capita_personal_income / 1000.0 AS income_k,
               svi.avg_svi, pov.poverty_rate
        FROM dim_state d
        LEFT JOIN (SELECT state_code AS mc_st, mc_penetration_pct FROM fact_mc_enrollment_summary
                   WHERE year = (SELECT MAX(year) FROM fact_mc_enrollment_summary)) mc ON d.state_code = mc.mc_st
        LEFT JOIN (SELECT state_code AS bea_st, per_capita_personal_income FROM fact_bea_personal_income
                   WHERE year = (SELECT MAX(year) FROM fact_bea_personal_income)
                     AND per_capita_personal_income IS NOT NULL) bea ON d.state_code = bea.bea_st
        LEFT JOIN (SELECT st_abbr AS svi_st, AVG(rpl_themes) AS avg_svi FROM fact_svi_county
                   WHERE rpl_themes IS NOT NULL AND rpl_themes >= 0 GROUP BY st_abbr) svi ON d.state_code = svi.svi_st
        LEFT JOIN (SELECT state_code AS pov_st, pct_poverty AS poverty_rate FROM fact_acs_state
                   WHERE data_year = (SELECT MAX(data_year) FROM fact_acs_state)) pov ON d.state_code = pov.pov_st
        WHERE d.fmap IS NOT NULL
    """

    rates = con.execute(rate_sql).fetchdf()
    quality = con.execute(quality_sql).fetchdf()
    controls = con.execute(controls_sql).fetchdf()
    merged = rates.merge(quality, on="state_code").merge(controls, on="state_code").dropna()
    n = len(merged)
    report.append(f"Merged sample: **N={n}** states\n")

    if n < 15:
        report.append("Insufficient sample for advanced methods.\n")
        return "\n".join(report)

    y = merged["avg_access_quality"].values
    X_vars = merged[["avg_pct_medicare", "mc_pct", "income_k"]].values
    X = np.column_stack([np.ones(n), X_vars])
    var_names = ["(intercept)", "Medicaid rate (%)", "MC penetration (%)", "Income ($K)"]

    # ─── OLS with robust SEs and VIF ───
    report.append("## 1A. OLS with Robust (HC1) Standard Errors\n")
    report.append("*Dropping SVI and poverty to reduce collinearity; keeping rate, MC, income.*\n")
    result = ols_full(y, X, var_names)

    rows = []
    for i, name in enumerate(result["names"]):
        rows.append([name, f"{result['beta'][i]:.4f}", f"{result['se'][i]:.4f}",
                      f"{result['t'][i]:.2f}", f"{result['p'][i]:.4f}{sig_stars(result['p'][i])}"])
    report.append(fmt_table(["Variable", "Coefficient", "Robust SE", "t", "p"], rows))
    report.append(f"\nR² = {result['r2']:.3f}, Adjusted R² = {result['adj_r2']:.3f}, N = {n}\n")

    # VIF report
    report.append("## 1B. Variance Inflation Factors\n")
    vif_rows = []
    for name, v in result["vif"]:
        flag = " ⚠️ HIGH" if v > 5 else " 🔴 CRITICAL" if v > 10 else ""
        vif_rows.append([name, f"{v:.2f}", flag])
    report.append(fmt_table(["Variable", "VIF", "Flag"], vif_rows))
    report.append("\n*VIF > 5 suggests moderate collinearity; VIF > 10 suggests severe.*\n")

    # ─── Instrumental Variables: GPCI as instrument for Medicaid rates ───
    report.append("## 1C. Instrumental Variables (2SLS): GPCI as Instrument\n")
    report.append("**Identification strategy:** Geographic Practice Cost Index (GPCI) affects Medicare rates, ")
    report.append("which influence Medicaid rates (many states peg to Medicare). GPCI should not directly ")
    report.append("affect Medicaid quality except through rates (exclusion restriction).\n")

    try:
        # Get state-level average GPCI (work component)
        gpci_sql = """
            SELECT state_code,
                   AVG(gpci_work) AS avg_work_gpci,
                   AVG(gpci_pe) AS avg_pe_gpci
            FROM ref_pfs_gpci
            WHERE state_code IS NOT NULL AND LENGTH(state_code) = 2
            GROUP BY state_code
        """
        gpci = con.execute(gpci_sql).fetchdf()

        if len(gpci) > 0:
            merged_iv = merged.merge(gpci, on="state_code").dropna()
            n_iv = len(merged_iv)
            report.append(f"\nIV sample: **N={n_iv}** states with GPCI data\n")

            if n_iv >= 15:
                y_iv = merged_iv["avg_access_quality"].values
                X_endog = merged_iv["avg_pct_medicare"].values
                X_exog = np.column_stack([
                    np.ones(n_iv),
                    merged_iv["mc_pct"].values,
                    merged_iv["income_k"].values,
                ])
                Z = merged_iv["avg_work_gpci"].values

                iv_result = iv_2sls(
                    y_iv, X_endog, X_exog, Z,
                    endog_name="Medicaid rate (%, IV)",
                    exog_names=["(intercept)", "MC penetration (%)", "Income ($K)"],
                    iv_names=["Work GPCI"],
                )

                report.append(f"**First-stage F-statistic: {iv_result['f_first_stage']:.1f}**")
                if iv_result['f_first_stage'] < 10:
                    report.append(" ⚠️ Weak instrument (F < 10). GPCI does not predict Medicaid rates ")
                    report.append("in the first stage — most states don't peg to Medicare. ")
                    report.append("IV estimates below are unreliable. Alternative instruments needed ")
                    report.append("(e.g., neighboring state rates, historical rate shocks, legislative mandates).\n")
                else:
                    report.append(" ✓ Strong instrument (F ≥ 10)\n")

                iv_rows = []
                for i, name in enumerate(iv_result["names"]):
                    iv_rows.append([name, f"{iv_result['beta'][i]:.4f}", f"{iv_result['se'][i]:.4f}",
                                    f"{iv_result['t'][i]:.2f}", f"{iv_result['p'][i]:.4f}{sig_stars(iv_result['p'][i])}"])
                report.append(fmt_table(["Variable", "IV Coefficient", "SE", "t", "p"], iv_rows))
                report.append(f"\nIV R² = {iv_result['r2']:.3f}\n")

                # Compare OLS vs IV
                ols_rate_beta = result["beta"][1]
                iv_rate_beta = iv_result["beta"][-1]
                report.append(f"\n**OLS rate coefficient: {ols_rate_beta:.4f}**\n")
                report.append(f"**IV rate coefficient: {iv_rate_beta:.4f}**\n")
                if abs(iv_rate_beta) > abs(ols_rate_beta) * 1.5:
                    report.append("*IV coefficient is substantially larger than OLS, suggesting OLS has attenuation bias ")
                    report.append("(measurement error in rates) or downward omitted variable bias.*\n")
                elif abs(iv_rate_beta) < abs(ols_rate_beta) * 0.5:
                    report.append("*IV coefficient is smaller than OLS, suggesting OLS may have upward bias from ")
                    report.append("reverse causality (states with better quality attract more providers, enabling higher rates).*\n")
                else:
                    report.append("*IV and OLS coefficients are similar, suggesting limited endogeneity concern.*\n")
        else:
            report.append("Insufficient GPCI matches for IV estimation.\n")
    except Exception as e:
        report.append(f"IV estimation failed: {e}\n")

    # ─── Quantile Regression ───
    report.append("## 1D. Quantile Regression: Rate Effect Across Quality Distribution\n")
    report.append("*Does the rate effect differ for states at the bottom vs top of quality?*\n")

    qr_rows = []
    for tau in [0.10, 0.25, 0.50, 0.75, 0.90]:
        qr = quantile_reg(y, X, tau=tau, var_names=var_names)
        rate_beta = qr["beta"][1]
        rate_se = qr["se"][1]
        rate_p = qr["p"][1]
        qr_rows.append([
            f"τ={tau:.2f}",
            f"{rate_beta:.4f}",
            f"{rate_se:.4f}",
            f"{rate_p:.4f}{sig_stars(rate_p)}",
        ])
    report.append(fmt_table(["Quantile", "Rate Coefficient", "SE (bootstrap)", "p"], qr_rows))
    report.append("\n*If the coefficient is larger at lower quantiles, rate increases help struggling states more.*\n")

    return "\n".join(report)


# ═══════════════════════════════════════════════════════════════════════
# ANALYSIS 2: MC VALUE WITH SYNTHETIC CONTROL PREP + ARIMA-X
# ═══════════════════════════════════════════════════════════════════════

def analysis_2_enhanced(con):
    """Managed care value with enhanced panel methods and trend decomposition."""
    report = []
    report.append("# Analysis 2 Enhanced: Managed Care Value — Dynamic Panel + Trend Analysis\n")

    # Monthly enrollment data for state-level MC transition analysis
    report.append("## 2A. MC Transition Event Study\n")
    report.append("*Identifying states with large MC penetration increases for event study analysis.*\n")

    try:
        mc_transitions = con.execute("""
            SELECT state_code,
                   MIN(mc_penetration_pct) AS min_mc,
                   MAX(mc_penetration_pct) AS max_mc,
                   MAX(mc_penetration_pct) - MIN(mc_penetration_pct) AS mc_change,
                   MIN(year) AS first_year, MAX(year) AS last_year,
                   COUNT(DISTINCT year) AS n_years
            FROM fact_mc_enrollment_summary
            WHERE mc_penetration_pct IS NOT NULL
            GROUP BY state_code
            HAVING COUNT(DISTINCT year) >= 5
            ORDER BY mc_change DESC
        """).fetchdf()

        big_changers = mc_transitions[mc_transitions["mc_change"] > 15]
        report.append(f"\nStates with MC penetration change > 15pp: **{len(big_changers)}**\n")

        if len(big_changers) > 0:
            rows = []
            for _, r in big_changers.head(10).iterrows():
                rows.append([r.state_code, f"{r.min_mc:.0f}%", f"{r.max_mc:.0f}%",
                             f"+{r.mc_change:.0f}pp", str(int(r.first_year)), str(int(r.last_year))])
            report.append(fmt_table(["State", "Min MC%", "Max MC%", "Change", "From", "To"], rows,
                                     "Top MC Transition States (Synthetic Control Candidates)"))
    except Exception as e:
        report.append(f"MC transition analysis failed: {e}\n")

    # Enhanced spending panel with lagged DV
    report.append("\n## 2B. Dynamic Panel: Spending Growth Decomposition\n")
    try:
        panel_sql = """
            SELECT c.state_code, c.fiscal_year AS year,
                   SUM(c.total_computable) / NULLIF(e.total_enrollment, 0) AS per_enrollee,
                   mc.mc_penetration_pct AS mc_pct,
                   bea.per_capita_personal_income / 1000.0 AS income_k
            FROM fact_cms64_multiyear c
            JOIN (SELECT state_code, year, MAX(total_enrollment) AS total_enrollment
                  FROM fact_enrollment GROUP BY state_code, year) e
              ON c.state_code = e.state_code AND c.fiscal_year = e.year
            LEFT JOIN fact_mc_enrollment_summary mc
              ON c.state_code = mc.state_code AND c.fiscal_year = mc.year
            LEFT JOIN fact_bea_personal_income bea
              ON c.state_code = bea.state_code AND c.fiscal_year = bea.year
            WHERE c.state_code != 'US' AND c.fiscal_year BETWEEN 2018 AND 2024
              AND e.total_enrollment > 100000
            GROUP BY c.state_code, c.fiscal_year, mc.mc_penetration_pct,
                     bea.per_capita_personal_income, e.total_enrollment
            HAVING SUM(c.total_computable) > 0
            ORDER BY c.state_code, c.fiscal_year
        """
        panel = con.execute(panel_sql).fetchdf().dropna()

        # Add lagged spending
        panel = panel.sort_values(["state_code", "year"])
        panel["per_enrollee_lag"] = panel.groupby("state_code")["per_enrollee"].shift(1)
        panel["spending_growth_pct"] = (panel["per_enrollee"] - panel["per_enrollee_lag"]) / panel["per_enrollee_lag"] * 100
        panel_clean = panel.dropna()

        n = len(panel_clean)
        n_states = panel_clean["state_code"].nunique()
        report.append(f"Panel with lagged DV: **{n} obs, {n_states} states**\n")

        if n >= 30:
            # Spending growth ~ MC change + income growth + year
            y = panel_clean["spending_growth_pct"].values
            X = np.column_stack([
                np.ones(n),
                panel_clean["mc_pct"].values,
                panel_clean["income_k"].values,
                panel_clean["year"].values,
            ])
            result = ols_full(y, X, ["(intercept)", "MC penetration (%)", "Income ($K)", "Year"])
            rows = []
            for i, name in enumerate(result["names"]):
                rows.append([name, f"{result['beta'][i]:.3f}", f"{result['se'][i]:.3f}",
                             f"{result['t'][i]:.2f}", f"{result['p'][i]:.4f}{sig_stars(result['p'][i])}"])
            report.append(fmt_table(["Variable", "Coefficient", "Robust SE", "t", "p"], rows,
                                     "Spending Growth Rate (%) ~ MC + Income + Year"))
            report.append(f"\nR² = {result['r2']:.3f}, N = {n}\n")

            # Spending CAGR by state
            report.append("\n### State-Level Spending Growth (CAGR)\n")
            cagr = panel.groupby("state_code").apply(
                lambda g: ((g["per_enrollee"].iloc[-1] / g["per_enrollee"].iloc[0]) ** (1 / max(len(g) - 1, 1)) - 1) * 100
                if g["per_enrollee"].iloc[0] > 0 else 0
            ).reset_index(name="cagr_pct")
            cagr = cagr.sort_values("cagr_pct", ascending=False)
            report.append(f"Median CAGR: **{cagr['cagr_pct'].median():.1f}%/year**\n")
            report.append(f"Top 5: {', '.join(f'{r.state_code} ({r.cagr_pct:.1f}%)' for _, r in cagr.head(5).iterrows())}\n")
            report.append(f"Bottom 5: {', '.join(f'{r.state_code} ({r.cagr_pct:.1f}%)' for _, r in cagr.tail(5).iterrows())}\n")

    except Exception as e:
        report.append(f"Dynamic panel failed: {e}\n")

    # MLR trend decomposition
    report.append("\n## 2C. MLR Trend Decomposition by Year\n")
    try:
        mlr_trend = con.execute("""
            SELECT report_year,
                   COUNT(*) AS n_plans,
                   ROUND(AVG(adjusted_mlr), 1) AS avg_mlr,
                   ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY adjusted_mlr), 1) AS median_mlr,
                   ROUND(SUM(CASE WHEN adjusted_mlr < 85 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) AS pct_below_85,
                   ROUND(SUM(mlr_denominator) / 1e9, 1) AS total_premium_B,
                   ROUND(SUM(remittance_amount) / 1e6, 0) AS total_remittance_M
            FROM fact_mco_mlr
            WHERE adjusted_mlr > 0 AND adjusted_mlr < 120
              AND report_year IS NOT NULL
            GROUP BY report_year
            ORDER BY report_year
        """).fetchdf()

        if len(mlr_trend) > 0:
            rows = []
            for _, r in mlr_trend.iterrows():
                rows.append([str(int(r.report_year)), str(int(r.n_plans)),
                             f"{r.avg_mlr:.1f}%", f"{r.median_mlr:.1f}%",
                             f"{r.pct_below_85:.1f}%", f"${r.total_premium_B:.0f}B",
                             f"${r.total_remittance_M:.0f}M"])
            report.append(fmt_table(
                ["Year", "Plans", "Avg MLR", "Median MLR", "% Below 85%", "Total Premium", "Remittance"],
                rows, "MCO MLR Trend by Year"
            ))
    except Exception as e:
        report.append(f"MLR trend failed: {e}\n")

    return "\n".join(report)


# ═══════════════════════════════════════════════════════════════════════
# ANALYSIS 3: NURSING OWNERSHIP WITH PSM + CHOW EVENT STUDY
# ═══════════════════════════════════════════════════════════════════════

def analysis_3_enhanced(con):
    """Nursing ownership with Propensity Score Matching + CHOW event study."""
    report = []
    report.append("# Analysis 3 Enhanced: Nursing Ownership — PSM + CHOW Event Study\n")

    # ─── Propensity Score Matching ───
    report.append("## 3A. Propensity Score Matching: For-Profit vs Nonprofit\n")
    report.append("*Matching for-profit to nonprofit facilities on beds, urban/rural, state, acuity.*\n")

    try:
        from sklearn.linear_model import LogisticRegression
        from sklearn.neighbors import NearestNeighbors

        facility_sql = """
            SELECT provider_ccn, state_code, overall_rating,
                   CASE WHEN ownership_type ILIKE '%profit%' AND ownership_type NOT ILIKE '%non%'
                        THEN 1 ELSE 0 END AS is_for_profit,
                   COALESCE(certified_beds, 0) AS beds,
                   COALESCE(avg_residents_per_day, 0) AS avg_residents,
                   COALESCE(hprd_total, 0) AS hprd,
                   CASE WHEN in_hospital THEN 1 ELSE 0 END AS in_hospital,
                   staffing_rating, qm_rating, health_inspection_rating,
                   COALESCE(deficiency_count, 0) AS deficiency_count
            FROM fact_five_star
            WHERE overall_rating IS NOT NULL
              AND state_code IS NOT NULL AND LENGTH(state_code) = 2
              AND certified_beds > 0
        """
        facilities = con.execute(facility_sql).fetchdf().dropna(subset=["is_for_profit", "beds", "avg_residents"])

        # State dummies for matching
        state_dummies = pd.get_dummies(facilities["state_code"], prefix="st", drop_first=True)
        X_match = pd.concat([
            facilities[["beds", "avg_residents", "in_hospital"]],
            state_dummies
        ], axis=1).values.astype(np.float64)
        treatment = facilities["is_for_profit"].values.astype(int)

        # Fit propensity score
        ps_model = LogisticRegression(max_iter=1000, C=1.0)
        ps_model.fit(X_match, treatment)
        pscore = ps_model.predict_proba(X_match)[:, 1]
        facilities["pscore"] = pscore

        # Nearest-neighbor matching (1:1)
        fp_idx = np.where(treatment == 1)[0]
        np_idx = np.where(treatment == 0)[0]

        nn = NearestNeighbors(n_neighbors=1, metric="euclidean")
        nn.fit(pscore[np_idx].reshape(-1, 1))
        distances, indices = nn.kneighbors(pscore[fp_idx].reshape(-1, 1))

        matched_fp = facilities.iloc[fp_idx].copy()
        matched_np = facilities.iloc[np_idx[indices.ravel()]].copy()

        # Caliper: drop matches with distance > 0.05
        caliper = 0.05
        good_matches = distances.ravel() < caliper
        matched_fp = matched_fp[good_matches]
        matched_np = matched_np.iloc[np.where(good_matches)[0]]

        n_matched = len(matched_fp)
        report.append(f"\nMatched pairs: **{n_matched}** (caliper={caliper})\n")

        if n_matched > 100:
            att_overall = matched_fp["overall_rating"].mean() - matched_np["overall_rating"].mean()
            att_staffing = matched_fp["staffing_rating"].mean() - matched_np["staffing_rating"].mean()
            att_inspection = matched_fp["health_inspection_rating"].mean() - matched_np["health_inspection_rating"].mean()
            att_qm = matched_fp["qm_rating"].mean() - matched_np["qm_rating"].mean()
            att_deficiency = matched_fp["deficiency_count"].mean() - matched_np["deficiency_count"].mean()

            # T-tests
            _, p_overall = sp_stats.ttest_ind(matched_fp["overall_rating"], matched_np["overall_rating"])
            _, p_staffing = sp_stats.ttest_ind(matched_fp["staffing_rating"], matched_np["staffing_rating"])
            _, p_inspection = sp_stats.ttest_ind(matched_fp["health_inspection_rating"], matched_np["health_inspection_rating"])

            rows = [
                ["Overall Rating", f"{matched_fp['overall_rating'].mean():.2f}",
                 f"{matched_np['overall_rating'].mean():.2f}", f"{att_overall:.2f}", f"{p_overall:.6f}{sig_stars(p_overall)}"],
                ["Staffing Rating", f"{matched_fp['staffing_rating'].mean():.2f}",
                 f"{matched_np['staffing_rating'].mean():.2f}", f"{att_staffing:.2f}", f"{p_staffing:.6f}{sig_stars(p_staffing)}"],
                ["Inspection Rating", f"{matched_fp['health_inspection_rating'].mean():.2f}",
                 f"{matched_np['health_inspection_rating'].mean():.2f}", f"{att_inspection:.2f}", f"{p_inspection:.6f}{sig_stars(p_inspection)}"],
                ["QM Rating", f"{matched_fp['qm_rating'].mean():.2f}",
                 f"{matched_np['qm_rating'].mean():.2f}", f"{att_qm:.2f}", "—"],
                ["Avg Deficiencies", f"{matched_fp['deficiency_count'].mean():.1f}",
                 f"{matched_np['deficiency_count'].mean():.1f}", f"{att_deficiency:+.1f}", "—"],
            ]
            report.append(fmt_table(["Outcome", "For-Profit", "Nonprofit (matched)", "ATT", "p-value"], rows,
                                     "PSM Average Treatment Effect on Treated"))

            # Balance check
            report.append("\n### Covariate Balance (Post-Matching)\n")
            for var in ["beds", "avg_residents"]:
                smd = (matched_fp[var].mean() - matched_np[var].mean()) / matched_fp[var].std()
                report.append(f"- {var}: SMD = {smd:.3f} {'✓' if abs(smd) < 0.1 else '⚠️ imbalanced'}\n")

    except Exception as e:
        report.append(f"PSM failed: {e}\n")

    # ─── CHOW Event Study ───
    report.append("\n## 3B. Change-of-Ownership (CHOW) Event Study\n")
    report.append("*Tracking quality changes around ownership transfers using fact_snf_chow.*\n")

    try:
        # Check CHOW data
        chow_sql = """
            SELECT c.buyer_ccn AS provider_ccn,
                   c.effective_date,
                   c.chow_type,
                   f.overall_rating,
                   f.staffing_rating,
                   f.ownership_type,
                   f.state_code,
                   f.certified_beds
            FROM fact_snf_chow c
            JOIN fact_five_star f ON c.buyer_ccn = f.provider_ccn
            WHERE c.effective_date IS NOT NULL
              AND f.overall_rating IS NOT NULL
        """
        chow = con.execute(chow_sql).fetchdf()
        report.append(f"\nSNF ownership transfers matched to Five Star: **{len(chow)}**\n")

        if len(chow) > 0:
            # Ownership type distribution post-CHOW
            type_dist = chow["ownership_type"].value_counts()
            rows = [[t, str(c)] for t, c in type_dist.items()]
            report.append(fmt_table(["Post-CHOW Ownership", "Count"], rows,
                                     "Ownership Type After Transfer"))

            # Current quality of CHOW facilities vs all
            all_avg = con.execute("SELECT AVG(overall_rating) FROM fact_five_star WHERE overall_rating IS NOT NULL").fetchone()[0]
            chow_avg = chow["overall_rating"].mean()
            report.append(f"\n**CHOW facility avg rating: {chow_avg:.2f}** vs national avg: {all_avg:.2f}")
            report.append(f" (difference: {chow_avg - all_avg:+.2f})\n")

            # CHOW type breakdown
            if "chow_type" in chow.columns:
                chow_types = chow.groupby("chow_type").agg(
                    n=("overall_rating", "count"),
                    avg_rating=("overall_rating", "mean"),
                    avg_staffing=("staffing_rating", "mean"),
                ).reset_index()
                rows = []
                for _, r in chow_types.iterrows():
                    rows.append([r.chow_type, str(int(r.n)), f"{r.avg_rating:.2f}", f"{r.avg_staffing:.2f}"])
                report.append(fmt_table(["CHOW Type", "N", "Avg Overall", "Avg Staffing"], rows))

            report.append("\n*NOTE: Full event study requires historical Five-Star snapshots (pre/post transfer). ")
            report.append("Current data is point-in-time only. The CHOW dates + current quality allow cross-sectional ")
            report.append("analysis but not pre/post comparison. Historical quarterly Five-Star archives from CMS ")
            report.append("would enable a proper event study design.*\n")

    except Exception as e:
        report.append(f"CHOW analysis failed: {e}\n")

    return "\n".join(report)


# ═══════════════════════════════════════════════════════════════════════
# ANALYSIS 4: PHARMACY SPREAD WITH ML FEATURE IMPORTANCE
# ═══════════════════════════════════════════════════════════════════════

def analysis_4_enhanced(con):
    """Pharmacy spread with Random Forest feature importance + state policy analysis."""
    report = []
    report.append("# Analysis 4 Enhanced: Pharmacy Spread — ML + Policy Analysis\n")

    # ─── Random Forest: what predicts the largest spreads? ───
    report.append("## 4A. Random Forest: Drivers of Drug-Level Overpayment\n")

    try:
        from sklearn.ensemble import RandomForestRegressor

        drug_sql = """
            WITH latest_nadac AS (
                SELECT ndc, ndc_description, nadac_per_unit, pricing_unit
                FROM fact_nadac
                QUALIFY ROW_NUMBER() OVER (PARTITION BY ndc ORDER BY effective_date DESC) = 1
            ),
            sdud_agg AS (
                SELECT ndc, product_name,
                       SUM(total_amount_reimbursed) AS total_reimbursed,
                       SUM(units_reimbursed) AS total_units,
                       SUM(number_of_prescriptions) AS total_rx,
                       COUNT(DISTINCT state_code) AS n_states
                FROM fact_sdud_2025
                WHERE state_code != 'XX' AND units_reimbursed > 0
                  AND total_amount_reimbursed > 0
                GROUP BY ndc, product_name
            )
            SELECT s.ndc, s.product_name, n.nadac_per_unit, n.pricing_unit,
                   s.total_reimbursed / NULLIF(s.total_units, 0) AS reimb_per_unit,
                   (s.total_reimbursed / NULLIF(s.total_units, 0) - n.nadac_per_unit) * s.total_units AS total_spread,
                   s.total_units, s.total_rx, s.n_states,
                   CASE WHEN n.nadac_per_unit > 0
                        THEN (s.total_reimbursed / NULLIF(s.total_units, 0)) / n.nadac_per_unit
                        ELSE NULL END AS markup_ratio,
                   CASE WHEN n.nadac_per_unit < 1 THEN 'Low (<$1)'
                        WHEN n.nadac_per_unit < 10 THEN 'Medium ($1-$10)'
                        WHEN n.nadac_per_unit < 100 THEN 'High ($10-$100)'
                        ELSE 'Specialty ($100+)' END AS price_tier
            FROM sdud_agg s
            JOIN latest_nadac n ON s.ndc = n.ndc
            WHERE n.nadac_per_unit > 0 AND s.total_units > 0
              AND s.total_reimbursed / NULLIF(s.total_units, 0) > 0
        """
        drugs = con.execute(drug_sql).fetchdf().dropna()
        report.append(f"\nDrugs for ML analysis: **{len(drugs):,}**\n")

        if len(drugs) > 100:
            # Features
            drugs["log_nadac"] = np.log1p(drugs["nadac_per_unit"])
            drugs["log_units"] = np.log1p(drugs["total_units"])
            drugs["log_rx"] = np.log1p(drugs["total_rx"])
            tier_dummies = pd.get_dummies(drugs["price_tier"], prefix="tier", drop_first=True)
            unit_dummies = pd.get_dummies(drugs["pricing_unit"], prefix="unit", drop_first=True)

            feature_cols = ["log_nadac", "log_units", "log_rx", "n_states"]
            X_ml = pd.concat([drugs[feature_cols], tier_dummies, unit_dummies], axis=1).values
            y_ml = np.log1p(np.maximum(drugs["total_spread"].values, 0))

            feature_names = feature_cols + list(tier_dummies.columns) + list(unit_dummies.columns)

            rf = RandomForestRegressor(n_estimators=200, max_depth=10, random_state=42, n_jobs=-1)
            rf.fit(X_ml, y_ml)

            # Feature importance
            importances = rf.feature_importances_
            sorted_idx = np.argsort(importances)[::-1]

            report.append(f"**Random Forest R² (in-sample): {rf.score(X_ml, y_ml):.3f}**\n")
            rows = []
            for i in sorted_idx[:10]:
                rows.append([feature_names[i], f"{importances[i]:.4f}", f"{importances[i]*100:.1f}%"])
            report.append(fmt_table(["Feature", "Importance", "% Total"], rows,
                                     "Top 10 Features Predicting Drug Overpayment"))

            # Price tier analysis
            report.append("\n### Overpayment by Price Tier\n")
            tier_stats = drugs.groupby("price_tier").agg(
                n_drugs=("ndc", "count"),
                total_spread=("total_spread", "sum"),
                avg_markup=("markup_ratio", "median"),
            ).reset_index()
            tier_stats["total_spread_B"] = tier_stats["total_spread"] / 1e9
            tier_stats = tier_stats.sort_values("total_spread_B", ascending=False)
            rows = []
            for _, r in tier_stats.iterrows():
                rows.append([r.price_tier, f"{int(r.n_drugs):,}", f"${r.total_spread_B:.2f}B", f"{r.avg_markup:.2f}x"])
            report.append(fmt_table(["Price Tier", "Drugs", "Total Spread", "Median Markup"], rows))

    except Exception as e:
        report.append(f"ML analysis failed: {e}\n")

    # ─── State Policy Natural Experiment ───
    report.append("\n## 4B. State-Level Spread Variation Analysis\n")
    try:
        state_spread = con.execute("""
            WITH latest_nadac AS (
                SELECT ndc, nadac_per_unit
                FROM fact_nadac
                QUALIFY ROW_NUMBER() OVER (PARTITION BY ndc ORDER BY effective_date DESC) = 1
            ),
            sdud_state AS (
                SELECT state_code, ndc,
                       SUM(total_amount_reimbursed) AS total_reimbursed,
                       SUM(units_reimbursed) AS total_units
                FROM fact_sdud_2025
                WHERE state_code != 'XX' AND units_reimbursed > 0
                GROUP BY state_code, ndc
            )
            SELECT s.state_code,
                   COUNT(DISTINCT s.ndc) AS drugs,
                   SUM(s.total_reimbursed) / 1e9 AS total_reimb_B,
                   SUM(n.nadac_per_unit * s.total_units) / 1e9 AS total_nadac_B,
                   (SUM(s.total_reimbursed) - SUM(n.nadac_per_unit * s.total_units)) / 1e9 AS spread_B,
                   ROUND((SUM(s.total_reimbursed) - SUM(n.nadac_per_unit * s.total_units)) * 100.0
                         / NULLIF(SUM(n.nadac_per_unit * s.total_units), 0), 1) AS spread_pct
            FROM sdud_state s
            JOIN latest_nadac n ON s.ndc = n.ndc
            WHERE n.nadac_per_unit > 0
            GROUP BY s.state_code
            ORDER BY spread_pct DESC
        """).fetchdf()

        if len(state_spread) > 0:
            report.append(f"\nStates analyzed: **{len(state_spread)}**\n")
            report.append(f"Spread % range: {state_spread['spread_pct'].min():.1f}% to {state_spread['spread_pct'].max():.1f}%\n")

            # States paying below NADAC
            below_nadac = state_spread[state_spread["spread_B"] < 0]
            if len(below_nadac) > 0:
                report.append(f"\n**States paying BELOW NADAC (net underpayment):** {', '.join(below_nadac['state_code'].tolist())}\n")
                report.append("*These states demonstrate that below-acquisition-cost reimbursement is achievable.*\n")

            # Top/bottom 5
            report.append("\n### Highest Spread States\n")
            for _, r in state_spread.head(5).iterrows():
                report.append(f"- {r.state_code}: {r.spread_pct:.1f}% spread (${r.spread_B:.2f}B)\n")
            report.append("\n### Lowest Spread States\n")
            for _, r in state_spread.tail(5).iterrows():
                report.append(f"- {r.state_code}: {r.spread_pct:.1f}% spread (${r.spread_B:.2f}B)\n")

    except Exception as e:
        report.append(f"State spread analysis failed: {e}\n")

    return "\n".join(report)


# ═══════════════════════════════════════════════════════════════════════
# ANALYSIS 5: TREATMENT GAP WITH EXPANSION ANALYSIS + CLUSTERING
# ═══════════════════════════════════════════════════════════════════════

def analysis_5_enhanced(con):
    """Treatment gap with spatial mismatch index + ML clustering."""
    report = []
    report.append("# Analysis 5 Enhanced: Treatment Gap — Spatial Mismatch + Clustering\n")

    # ─── Build comprehensive state profile ───
    report.append("## 5A. Treatment Gap Composite\n")
    try:
        # OUD prevalence
        oud = con.execute("""
            SELECT state_code, estimate_pct AS oud_prevalence
            FROM fact_nsduh_prevalence
            WHERE measure_id = 'oud_past_year' AND age_group = '18+'
        """).fetchdf()

        # MAT spending
        mat = con.execute("""
            SELECT state_code,
                   SUM(total_amount_reimbursed) / 1e6 AS mat_spending_M,
                   SUM(number_of_prescriptions) AS mat_rx
            FROM fact_sdud_2025
            WHERE state_code != 'XX'
              AND (product_name ILIKE '%buprenorphine%' OR product_name ILIKE '%suboxone%'
                   OR product_name ILIKE '%naloxone%' OR product_name ILIKE '%naltrexone%'
                   OR product_name ILIKE '%vivitrol%' OR product_name ILIKE '%sublocade%'
                   OR product_name ILIKE '%zubsolv%' OR product_name ILIKE '%subutex%')
              AND total_amount_reimbursed > 0
            GROUP BY state_code
        """).fetchdf()

        # Enrollment
        enrollment = con.execute("""
            SELECT state_code, MAX(total_enrollment) AS enrollment
            FROM fact_enrollment
            WHERE year = (SELECT MAX(year) FROM fact_enrollment)
            GROUP BY state_code
        """).fetchdf()

        # Facility count
        facilities = con.execute("""
            SELECT state_code, COUNT(*) AS facility_count
            FROM fact_mh_facility
            GROUP BY state_code
        """).fetchdf()

        # Merge
        gap = oud.merge(mat, on="state_code", how="left") \
                  .merge(enrollment, on="state_code", how="left") \
                  .merge(facilities, on="state_code", how="left")
        gap["mat_spending_M"] = gap["mat_spending_M"].fillna(0)
        gap["mat_rx"] = gap["mat_rx"].fillna(0)
        gap["facility_count"] = gap["facility_count"].fillna(0)
        gap = gap.dropna(subset=["enrollment", "oud_prevalence"])

        # Per capita metrics
        gap["mat_per_1000"] = gap["mat_spending_M"] * 1e6 / gap["enrollment"] * 1000
        gap["facilities_per_100k"] = gap["facility_count"] / gap["enrollment"] * 100000
        gap["mat_rx_per_1000"] = gap["mat_rx"] / gap["enrollment"] * 1000

        # Spatial Mismatch Index
        # Gini-like: if spending perfectly tracked prevalence, mismatch = 0
        gap_sorted = gap.sort_values("oud_prevalence")
        cum_prev = np.cumsum(gap_sorted["oud_prevalence"]) / gap_sorted["oud_prevalence"].sum()
        cum_spend = np.cumsum(gap_sorted["mat_spending_M"]) / max(gap_sorted["mat_spending_M"].sum(), 1)
        # np.trapezoid in numpy 2.x (was np.trapz in older versions)
        _trapz = getattr(np, "trapezoid", getattr(np, "trapz", None))
        mismatch_index = _trapz(np.abs(cum_prev - cum_spend), cum_prev)

        report.append(f"States with OUD prevalence data: **{len(gap)}**\n")
        report.append(f"National MAT spending: **${gap['mat_spending_M'].sum():.0f}M**\n")
        report.append(f"**Spatial Mismatch Index: {mismatch_index:.3f}** (0=perfect alignment, 0.5=maximum mismatch)\n")

        # Worst mismatches: high prevalence, low treatment
        gap["gap_score"] = gap["oud_prevalence"] / gap["oud_prevalence"].max() - \
                           gap["mat_per_1000"] / max(gap["mat_per_1000"].max(), 1)
        gap_sorted = gap.sort_values("gap_score", ascending=False)

        rows = []
        for _, r in gap_sorted.head(10).iterrows():
            rows.append([r.state_code, f"{r.oud_prevalence:.1f}%",
                         f"${r.mat_spending_M:.0f}M", f"${r.mat_per_1000:.0f}/1K",
                         f"{r.facilities_per_100k:.0f}", f"{r.gap_score:.3f}"])
        report.append(fmt_table(
            ["State", "OUD Prev", "MAT $", "MAT $/1K Enrollees", "Facilities/100K", "Gap Score"],
            rows, "Top 10 Treatment Gap States (High Need, Low Treatment)"
        ))

    except Exception as e:
        report.append(f"Treatment gap composite failed: {e}\n")

    # ─── K-Means Clustering ───
    report.append("\n## 5B. State Typology Clustering\n")
    try:
        from sklearn.cluster import KMeans
        from sklearn.preprocessing import StandardScaler

        cluster_features = gap[["oud_prevalence", "mat_per_1000", "facilities_per_100k"]].dropna()
        if len(cluster_features) >= 10:
            scaler = StandardScaler()
            X_scaled = scaler.fit_transform(cluster_features)

            kmeans = KMeans(n_clusters=4, random_state=42, n_init=10)
            labels = kmeans.fit_predict(X_scaled)
            gap_clustered = gap.loc[cluster_features.index].copy()
            gap_clustered["cluster"] = labels

            # Characterize clusters
            cluster_profiles = gap_clustered.groupby("cluster").agg(
                n=("state_code", "count"),
                avg_oud=("oud_prevalence", "mean"),
                avg_mat=("mat_per_1000", "mean"),
                avg_facilities=("facilities_per_100k", "mean"),
                states=("state_code", lambda x: ", ".join(sorted(x)[:5]) + ("..." if len(x) > 5 else "")),
            ).reset_index()

            # Label clusters
            for _, r in cluster_profiles.iterrows():
                high_need = r.avg_oud > gap["oud_prevalence"].median()
                high_treat = r.avg_mat > gap["mat_per_1000"].median()
                if high_need and not high_treat:
                    label = "🔴 Treatment Desert"
                elif high_need and high_treat:
                    label = "🟡 High Need / Responding"
                elif not high_need and high_treat:
                    label = "🟢 Low Need / Well-Resourced"
                else:
                    label = "⚪ Low Need / Low Treatment"
                cluster_profiles.loc[cluster_profiles["cluster"] == r.cluster, "label"] = label

            rows = []
            for _, r in cluster_profiles.iterrows():
                rows.append([r.label, str(int(r.n)), f"{r.avg_oud:.1f}%",
                             f"${r.avg_mat:.0f}/1K", f"{r.avg_facilities:.0f}", r.states])
            report.append(fmt_table(
                ["Cluster", "N States", "Avg OUD%", "MAT $/1K", "Fac/100K", "Sample States"],
                rows, "State Typology (K-Means, 4 clusters)"
            ))

    except Exception as e:
        report.append(f"Clustering failed: {e}\n")

    return "\n".join(report)


# ═══════════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(description="Aradune Advanced Research Methods")
    parser.add_argument("--analysis", type=int, help="Run specific analysis (1-5)")
    parser.add_argument("--output", default=None, help="Output directory")
    args = parser.parse_args()

    con = connect()

    analyses = {
        1: ("Rate-Quality Enhanced (IV + VIF + Quantile)", analysis_1_enhanced),
        2: ("MC Value Enhanced (Dynamic Panel + Trends)", analysis_2_enhanced),
        3: ("Nursing Enhanced (PSM + CHOW)", analysis_3_enhanced),
        4: ("Pharmacy Enhanced (ML + Policy)", analysis_4_enhanced),
        5: ("Treatment Gap Enhanced (Mismatch + Clustering)", analysis_5_enhanced),
    }

    if args.analysis:
        to_run = {args.analysis: analyses[args.analysis]}
    else:
        to_run = analyses

    full_report = [
        "# Aradune Cross-Domain Research: Advanced Methods Report\n",
        f"**Generated:** {datetime.now().strftime('%Y-%m-%d %H:%M')}\n",
        f"**Data Lake:** {LAKE}\n",
        "**Methods:** IV/2SLS, VIF, Propensity Score Matching, CHOW Event Study, ",
        "Random Forest, Quantile Regression, K-Means Clustering, Spatial Mismatch Index\n",
        "**Supplements:** research_replication.py (OLS, Panel FE, DiD, Cohen's d)\n",
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

    out_path = Path(args.output) if args.output else PROJECT_ROOT / "docs"
    out_path.mkdir(parents=True, exist_ok=True)
    out_file = out_path / "RESEARCH-ADVANCED-METHODS.md"
    out_file.write_text(report_text)
    print(f"\nReport saved to: {out_file}")

    con.close()


if __name__ == "__main__":
    main()
