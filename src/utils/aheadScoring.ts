// ── AHEAD Readiness Scoring Functions ────────────────────────────────────
// Each dimension scores 0–25 points. Composite = sum of 4 dimensions (0–100).
// Scoring functions are pure and unit-testable.

export interface HospitalMetrics {
  net_patient_revenue: number | null;
  net_income: number | null;
  total_income: number | null;
  total_costs: number | null;
  total_assets: number | null;
  total_liabilities: number | null;
  total_salaries: number | null;
  inpatient_revenue: number | null;
  outpatient_revenue: number | null;
  medicare_days: number | null;
  medicaid_days: number | null;
  total_days: number | null;
  total_discharges: number | null;
  uncompensated_care_cost: number | null;
  charity_care_cost: number | null;
  bad_debt_expense: number | null;
  dsh_adjustment: number | null;
  ime_payment: number | null;
  cost_to_charge_ratio: number | null;
  bed_count: number | null;
  medicaid_day_pct: number | null;
}

export interface PeerBenchmarks {
  median_operating_margin: number | null;
  p25_operating_margin: number | null;
  p75_operating_margin: number | null;
  median_supplemental_pct: number | null;
  median_medicaid_day_pct: number | null;
  median_medicare_day_pct: number | null;
  median_uc_pct: number | null;
  n: number | null;
}

export interface DimensionResult {
  score: number;
  maxScore: number;
  details: { label: string; value: number | string | null; points: number; maxPoints: number; interpretation: string; source: string }[];
}

// ── Derived metric helpers ──────────────────────────────────────────────

export function operatingMarginPct(m: HospitalMetrics): number | null {
  if (!m.net_patient_revenue || m.net_patient_revenue <= 0 || m.net_income == null) return null;
  return (m.net_income / m.net_patient_revenue) * 100;
}

export function currentRatio(m: HospitalMetrics): number | null {
  if (!m.total_assets || !m.total_liabilities || m.total_liabilities <= 0) return null;
  return m.total_assets / m.total_liabilities;
}

export function supplementalTotal(m: HospitalMetrics): number {
  return (m.dsh_adjustment ?? 0) + (m.ime_payment ?? 0);
}

export function supplementalPctOfRevenue(m: HospitalMetrics): number | null {
  if (!m.net_patient_revenue || m.net_patient_revenue <= 0) return null;
  return (supplementalTotal(m) / m.net_patient_revenue) * 100;
}

export function govPayerPct(m: HospitalMetrics): number | null {
  if (!m.total_days || m.total_days <= 0) return null;
  return (((m.medicare_days ?? 0) + (m.medicaid_days ?? 0)) / m.total_days) * 100;
}

export function ucPctOfRevenue(m: HospitalMetrics): number | null {
  if (!m.net_patient_revenue || m.net_patient_revenue <= 0) return null;
  return ((m.uncompensated_care_cost ?? 0) / m.net_patient_revenue) * 100;
}

// ── Dimension 1: Financial Stability (0–25) ─────────────────────────────

export function scoreFinancialStability(m: HospitalMetrics, _peers?: PeerBenchmarks): DimensionResult {
  const details: DimensionResult["details"] = [];
  let total = 0;

  // Operating Margin (0–8)
  const opMgn = operatingMarginPct(m);
  let opPts = 0;
  let opInterp = "";
  if (opMgn == null) { opInterp = "Operating margin not available in public data."; }
  else if (opMgn >= 3) { opPts = 8; opInterp = `Operating margin of ${opMgn.toFixed(1)}% is above the 3% threshold. Low risk.`; }
  else if (opMgn >= 1) { opPts = 5; opInterp = `Operating margin of ${opMgn.toFixed(1)}% is positive but below 3%. Moderate risk.`; }
  else if (opMgn >= 0) { opPts = 2; opInterp = `Operating margin of ${opMgn.toFixed(1)}% is near breakeven. Elevated risk for global budget transition.`; }
  else { opPts = 0; opInterp = `Operating margin of ${opMgn.toFixed(1)}% is negative. High risk — hospital is losing money on operations.`; }
  details.push({ label: "Operating Margin %", value: opMgn != null ? `${opMgn.toFixed(1)}%` : null, points: opPts, maxPoints: 8, interpretation: opInterp, source: "HCRIS FY2023" });
  total += opPts;

  // Current Ratio (0–5)
  const cr = currentRatio(m);
  let crPts = 0;
  let crInterp = "";
  if (cr == null) { crInterp = "Current ratio not calculable from available data."; }
  else if (cr >= 1.5) { crPts = 5; crInterp = `Current ratio of ${cr.toFixed(2)} exceeds the 1.5 benchmark. Strong liquidity.`; }
  else if (cr >= 1.0) { crPts = 3; crInterp = `Current ratio of ${cr.toFixed(2)} is adequate but below preferred 1.5 threshold.`; }
  else { crPts = 0; crInterp = `Current ratio of ${cr.toFixed(2)} is below 1.0. Significant liquidity risk.`; }
  details.push({ label: "Current Ratio", value: cr != null ? cr.toFixed(2) : null, points: crPts, maxPoints: 5, interpretation: crInterp, source: "HCRIS FY2023" });
  total += crPts;

  // Cost-to-Charge Ratio (0–5) — lower is more efficient
  const ccr = m.cost_to_charge_ratio;
  let ccrPts = 0;
  let ccrInterp = "";
  if (ccr == null) { ccrInterp = "Cost-to-charge ratio not available."; }
  else if (ccr <= 0.35) { ccrPts = 5; ccrInterp = `CCR of ${ccr.toFixed(3)} indicates strong cost efficiency.`; }
  else if (ccr <= 0.50) { ccrPts = 3; ccrInterp = `CCR of ${ccr.toFixed(3)} is in the typical range.`; }
  else { ccrPts = 0; ccrInterp = `CCR of ${ccr.toFixed(3)} is elevated — costs are high relative to charges.`; }
  details.push({ label: "Cost-to-Charge Ratio", value: ccr != null ? ccr.toFixed(3) : null, points: ccrPts, maxPoints: 5, interpretation: ccrInterp, source: "HCRIS FY2023" });
  total += ccrPts;

  // Net Income positive (0–7)
  const ni = m.net_income;
  const npr = m.net_patient_revenue;
  let niPts = 0;
  let niInterp = "";
  if (ni == null || npr == null || npr <= 0) { niInterp = "Net income data not available."; }
  else {
    const totalMgn = (ni / npr) * 100;
    if (totalMgn >= 5) { niPts = 7; niInterp = `Total margin of ${totalMgn.toFixed(1)}% is healthy. Strong financial position for global budget.`; }
    else if (totalMgn >= 2) { niPts = 4; niInterp = `Total margin of ${totalMgn.toFixed(1)}% is positive but thin.`; }
    else if (totalMgn >= 0) { niPts = 2; niInterp = `Total margin of ${totalMgn.toFixed(1)}% is near breakeven.`; }
    else { niPts = 0; niInterp = `Total margin of ${totalMgn.toFixed(1)}% is negative. Hospital is operating at a loss.`; }
    details.push({ label: "Total Margin %", value: `${totalMgn.toFixed(1)}%`, points: niPts, maxPoints: 7, interpretation: niInterp, source: "HCRIS FY2023" });
  }
  if (ni == null || npr == null || npr <= 0) {
    details.push({ label: "Total Margin %", value: null, points: 0, maxPoints: 7, interpretation: niInterp, source: "HCRIS FY2023" });
  }
  total += niPts;

  return { score: Math.min(total, 25), maxScore: 25, details };
}

// ── Dimension 2: Revenue Concentration Risk (0–25) ──────────────────────

export function scoreRevenueConcentration(m: HospitalMetrics, _peers?: PeerBenchmarks): DimensionResult {
  const details: DimensionResult["details"] = [];
  let total = 0;

  // Government payer concentration (0–10)
  const gPct = govPayerPct(m);
  let gPts = 0;
  let gInterp = "";
  if (gPct == null) { gInterp = "Payer mix not calculable from available day data."; }
  else if (gPct < 60) { gPts = 10; gInterp = `Government payer days at ${gPct.toFixed(1)}% — diversified payer mix. Low concentration risk.`; }
  else if (gPct < 75) { gPts = 6; gInterp = `Government payer days at ${gPct.toFixed(1)}% — moderate concentration. Some cost-shifting capacity remains.`; }
  else if (gPct < 85) { gPts = 3; gInterp = `Government payer days at ${gPct.toFixed(1)}% — high government dependence. Limited commercial cost-shifting.`; }
  else { gPts = 0; gInterp = `Government payer days at ${gPct.toFixed(1)}% — very high concentration. Virtually no commercial payer cushion.`; }
  details.push({ label: "Medicare + Medicaid Day %", value: gPct != null ? `${gPct.toFixed(1)}%` : null, points: gPts, maxPoints: 10, interpretation: gInterp, source: "HCRIS FY2023" });
  total += gPts;

  // Uncompensated care / self-pay burden (0–8)
  const ucPct = ucPctOfRevenue(m);
  let ucPts = 0;
  let ucInterp = "";
  if (ucPct == null) { ucInterp = "Uncompensated care data not available."; }
  else if (ucPct < 5) { ucPts = 8; ucInterp = `Uncompensated care at ${ucPct.toFixed(1)}% of revenue. Low burden.`; }
  else if (ucPct < 10) { ucPts = 5; ucInterp = `Uncompensated care at ${ucPct.toFixed(1)}% of revenue. Moderate burden.`; }
  else if (ucPct < 15) { ucPts = 2; ucInterp = `Uncompensated care at ${ucPct.toFixed(1)}% of revenue. Significant safety-net burden.`; }
  else { ucPts = 0; ucInterp = `Uncompensated care at ${ucPct.toFixed(1)}% of revenue. Very high — global budget baseline may not account for this volume.`; }
  details.push({ label: "Uncompensated Care % of Revenue", value: ucPct != null ? `${ucPct.toFixed(1)}%` : null, points: ucPts, maxPoints: 8, interpretation: ucInterp, source: "HCRIS FY2023" });
  total += ucPts;

  // Inpatient vs outpatient revenue balance (0–7)
  const ipRev = m.inpatient_revenue ?? 0;
  const opRev = m.outpatient_revenue ?? 0;
  const totalRev = ipRev + opRev;
  let balPts = 0;
  let balInterp = "";
  if (totalRev <= 0) { balInterp = "Revenue split data not available."; }
  else {
    const ipPct = (ipRev / totalRev) * 100;
    if (ipPct >= 30 && ipPct <= 60) { balPts = 7; balInterp = `Inpatient revenue at ${ipPct.toFixed(0)}% — balanced IP/OP mix. Favorable for global budget.`; }
    else if (ipPct >= 20 && ipPct <= 70) { balPts = 4; balInterp = `Inpatient revenue at ${ipPct.toFixed(0)}% — somewhat concentrated. Monitor volume shifts.`; }
    else { balPts = 0; balInterp = `Inpatient revenue at ${ipPct.toFixed(0)}% — highly skewed. Volume shifts pose risk under fixed budget.`; }
  }
  details.push({ label: "Inpatient Revenue %", value: totalRev > 0 ? `${((ipRev / totalRev) * 100).toFixed(0)}%` : null, points: balPts, maxPoints: 7, interpretation: balInterp, source: "HCRIS FY2023" });
  total += balPts;

  return { score: Math.min(total, 25), maxScore: 25, details };
}

// ── Dimension 3: Supplemental Payment Exposure (0–25) ───────────────────

export function scoreSupplementalExposure(m: HospitalMetrics, _peers?: PeerBenchmarks): DimensionResult {
  const details: DimensionResult["details"] = [];

  const suppPct = supplementalPctOfRevenue(m);
  const suppTotal = supplementalTotal(m);
  let pts = 0;
  let interp = "";

  if (suppPct == null) {
    interp = "Supplemental payment data not calculable.";
  } else if (suppPct < 10) {
    pts = 25;
    interp = `Supplemental payments are ${suppPct.toFixed(1)}% of revenue. Minimal exposure — global budget transition poses low supplemental risk.`;
  } else if (suppPct < 20) {
    pts = 18;
    interp = `Supplemental payments are ${suppPct.toFixed(1)}% of revenue. Moderate exposure. Negotiate preservation in early transition years.`;
  } else if (suppPct < 35) {
    pts = 10;
    interp = `Supplemental payments are ${suppPct.toFixed(1)}% of revenue. Significant exposure. Loss of supplementals would materially impact financial position.`;
  } else if (suppPct < 50) {
    pts = 4;
    interp = `Supplemental payments are ${suppPct.toFixed(1)}% of revenue. High exposure — supplemental preservation is critical to viability under global budget.`;
  } else {
    pts = 0;
    interp = `Supplemental payments are ${suppPct.toFixed(1)}% of revenue. Extremely high dependence. Global budget without supplemental protection would be unsustainable.`;
  }

  details.push({
    label: "Supplemental % of Revenue",
    value: suppPct != null ? `${suppPct.toFixed(1)}%` : null,
    points: pts, maxPoints: 25,
    interpretation: interp,
    source: "HCRIS FY2023",
  });

  // Show the breakdown
  if (m.dsh_adjustment) {
    details.push({ label: "Medicare DSH Adjustment", value: fmtDollar(m.dsh_adjustment), points: 0, maxPoints: 0, interpretation: "", source: "HCRIS Worksheet E" });
  }
  if (m.ime_payment) {
    details.push({ label: "IME Payment", value: fmtDollar(m.ime_payment), points: 0, maxPoints: 0, interpretation: "", source: "HCRIS Worksheet E" });
  }
  if (suppTotal > 0) {
    details.push({ label: "Total Identifiable Supplemental", value: fmtDollar(suppTotal), points: 0, maxPoints: 0, interpretation: "Includes DSH + IME only. Medicaid UPL/SDP not yet available at hospital level.", source: "HCRIS FY2023" });
  }

  return { score: Math.min(pts, 25), maxScore: 25, details };
}

// ── Dimension 4: Volume Stability (0–25) ────────────────────────────────

export function scoreVolumeStability(m: HospitalMetrics, _peers?: PeerBenchmarks): DimensionResult {
  const details: DimensionResult["details"] = [];
  let total = 0;

  // Discharge volume relative to beds (utilization proxy)
  const discharges = m.total_discharges;
  const beds = m.bed_count;
  let volPts = 0;
  let volInterp = "";
  if (discharges == null || beds == null || beds <= 0) {
    volInterp = "Discharge/bed data not available.";
  } else {
    const dPerBed = discharges / beds;
    // Typical range: 30–50 discharges per bed per year
    if (dPerBed >= 35) { volPts = 10; volInterp = `${dPerBed.toFixed(0)} discharges per bed — strong utilization. Stable volume base for global budget.`; }
    else if (dPerBed >= 20) { volPts = 5; volInterp = `${dPerBed.toFixed(0)} discharges per bed — moderate utilization. Some volume risk.`; }
    else { volPts = 0; volInterp = `${dPerBed.toFixed(0)} discharges per bed — low utilization. High volume risk under fixed budget.`; }
  }
  details.push({ label: "Discharges per Bed", value: discharges && beds && beds > 0 ? (discharges / beds).toFixed(0) : null, points: volPts, maxPoints: 10, interpretation: volInterp, source: "HCRIS FY2023" });
  total += volPts;

  // Bed occupancy estimate
  const totalDays = m.total_days;
  let occPts = 0;
  let occInterp = "";
  if (totalDays == null || beds == null || beds <= 0) {
    occInterp = "Occupancy data not available.";
  } else {
    const occupancy = (totalDays / (beds * 365)) * 100;
    if (occupancy >= 60) { occPts = 8; occInterp = `Estimated occupancy ${occupancy.toFixed(0)}% — healthy utilization.`; }
    else if (occupancy >= 40) { occPts = 4; occInterp = `Estimated occupancy ${occupancy.toFixed(0)}% — moderate. Some excess capacity risk.`; }
    else { occPts = 0; occInterp = `Estimated occupancy ${occupancy.toFixed(0)}% — low utilization. Excess capacity is a risk under global budgets.`; }
  }
  details.push({ label: "Estimated Occupancy %", value: totalDays && beds && beds > 0 ? `${((totalDays / (beds * 365)) * 100).toFixed(0)}%` : null, points: occPts, maxPoints: 8, interpretation: occInterp, source: "HCRIS FY2023" });
  total += occPts;

  // Cost efficiency — total cost per discharge
  let costPts = 0;
  let costInterp = "";
  if (!m.total_costs || !discharges || discharges <= 0) {
    costInterp = "Cost per discharge not calculable.";
  } else {
    const cpd = m.total_costs / discharges;
    if (cpd <= 15000) { costPts = 7; costInterp = `Cost per discharge $${(cpd / 1000).toFixed(0)}K — efficient. Favorable for global budget margins.`; }
    else if (cpd <= 25000) { costPts = 4; costInterp = `Cost per discharge $${(cpd / 1000).toFixed(0)}K — typical range.`; }
    else { costPts = 0; costInterp = `Cost per discharge $${(cpd / 1000).toFixed(0)}K — high. Cost reduction needed before global budget transition.`; }
  }
  details.push({ label: "Cost per Discharge", value: m.total_costs && discharges && discharges > 0 ? `$${((m.total_costs / discharges) / 1000).toFixed(0)}K` : null, points: costPts, maxPoints: 7, interpretation: costInterp, source: "HCRIS FY2023" });
  total += costPts;

  return { score: Math.min(total, 25), maxScore: 25, details };
}

// ── Self-report bonus scoring ───────────────────────────────────────────

export interface SelfReportAnswers {
  downsideRisk: boolean | null;      // +3
  positiveMargins: boolean | null;   // +2
  reinsurance: "yes" | "no" | "in_progress" | null;  // +2/0/1
  costAccounting: "yes" | "no" | "in_progress" | null; // +2/0/1
  costReports: "yes" | "no" | "partial" | null;  // +2/0/1
  mssp: boolean | null;             // +2
  legalActuarial: "internal" | "vendor" | "none" | null; // +1/1/0
  serviceLineMargins: boolean | null; // +1
}

export function scoreSelfReport(answers: SelfReportAnswers): number {
  let pts = 0;
  if (answers.downsideRisk) pts += 3;
  if (answers.positiveMargins) pts += 2;
  if (answers.reinsurance === "yes") pts += 2;
  else if (answers.reinsurance === "in_progress") pts += 1;
  if (answers.costAccounting === "yes") pts += 2;
  else if (answers.costAccounting === "in_progress") pts += 1;
  if (answers.costReports === "yes") pts += 2;
  else if (answers.costReports === "partial") pts += 1;
  if (answers.mssp) pts += 2;
  if (answers.legalActuarial === "internal" || answers.legalActuarial === "vendor") pts += 1;
  if (answers.serviceLineMargins) pts += 1;
  return Math.min(pts, 15);
}

// ── Composite score ─────────────────────────────────────────────────────

export function compositeScore(d1: number, d2: number, d3: number, d4: number, selfReport = 0): number {
  return Math.min(d1 + d2 + d3 + d4 + selfReport, 100);
}

export function scoreColor(score: number): string {
  if (score >= 70) return "#059669"; // green
  if (score >= 40) return "#D97706"; // amber
  return "#D93025"; // red
}

export function scoreLabel(score: number): string {
  if (score >= 70) return "Low Risk";
  if (score >= 40) return "Moderate Risk";
  return "High Risk";
}

function fmtDollar(n: number): string {
  const a = Math.abs(n);
  if (a >= 1e9) return (n < 0 ? "-" : "") + "$" + (a / 1e9).toFixed(2) + "B";
  if (a >= 1e6) return (n < 0 ? "-" : "") + "$" + (a / 1e6).toFixed(1) + "M";
  if (a >= 1e3) return (n < 0 ? "-" : "") + "$" + (a / 1e3).toFixed(0) + "K";
  return "$" + n.toFixed(0);
}
