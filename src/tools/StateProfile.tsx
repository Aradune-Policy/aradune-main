/**
 * State Profile — Everything Aradune knows about a state, in one view.
 * Fetches from ~12 endpoints in parallel on state selection.
 * Comparison mode: /#/state/FL+GA+TX renders states side-by-side.
 */
import { useState, useEffect, useCallback, useMemo } from "react";
import {
  ComposedChart, Area, Line, Bar, XAxis, YAxis, CartesianGrid,
  Tooltip, ResponsiveContainer, Legend, BarChart, PieChart, Pie, Cell,
} from "recharts";
import { STATE_NAMES } from "../data/states";
import { API_BASE } from "../lib/api";
import { useAradune } from "../context/AraduneContext";
import ChartActions from "../components/ChartActions";
import StateContextBar from "../components/StateContextBar";
import { useIsMobile } from "../design";
import { stateContextSummary } from "../utils/formatContext";
import { useStateContext } from "../hooks/useStateContext";

// ── Design tokens ───────────────────────────────────────────────────────
const A = "#0A2540", AL = "#425A70", POS = "#2E6B4A", NEG = "#A4262C", WARN = "#B8860B";
const SF = "#F5F7F5", BD = "#E4EAE4", WH = "#fff", cB = "#2E6B4A", ACC = "#C4590A";
const FM = "'SF Mono',Menlo,Consolas,monospace";
const FB = "'Helvetica Neue',Arial,sans-serif";
const SH = "0 1px 3px rgba(0,0,0,.04),0 4px 12px rgba(0,0,0,.03)";

// Comparison colors — one per state column
const COMP_COLORS = [cB, ACC, "#3B82F6", "#8B5CF6", "#EC4899", "#F59E0B"];

// ── Local UI components ─────────────────────────────────────────────────
const Card = ({ children, accent, compact }: { children: React.ReactNode; accent?: string; compact?: boolean }) => (
  <div style={{
    background: WH, borderRadius: 10, padding: compact ? "16px 14px" : "20px 24px", marginBottom: 16,
    boxShadow: SH, borderTop: accent ? `3px solid ${accent}` : undefined,
  }}>{children}</div>
);

const CH = ({ title, sub }: { title: string; sub?: string }) => (
  <div style={{ marginBottom: 12 }}>
    <h3 style={{ margin: 0, fontSize: 15, fontWeight: 700, color: A, letterSpacing: -0.2 }}>{title}</h3>
    {sub && <p style={{ margin: "3px 0 0", fontSize: 11, color: AL }}>{sub}</p>}
  </div>
);

const Met = ({ label, value, color, mono, small }: { label: string; value: string | number; color?: string; mono?: boolean; small?: boolean }) => (
  <div style={{ textAlign: "center", minWidth: small ? 60 : 80 }}>
    <div style={{ fontSize: small ? 14 : 18, fontWeight: 700, color: color || A, fontFamily: mono ? FM : FB, letterSpacing: -0.5 }}>{value}</div>
    <div style={{ fontSize: small ? 9 : 10, color: AL, marginTop: 2 }}>{label}</div>
  </div>
);

const Pill = ({ label, active, onClick, color }: { label: string; active: boolean; onClick: () => void; color?: string }) => (
  <button onClick={onClick} style={{
    padding: "6px 14px", borderRadius: 20, border: `1px solid ${active ? (color || cB) : BD}`,
    background: active ? (color || cB) : WH, color: active ? WH : AL,
    fontSize: 11, fontWeight: 600, fontFamily: FB, cursor: "pointer", transition: "all .15s",
  }}>{label}</button>
);

const SectionToggle = ({ label, open, onClick }: { label: string; open: boolean; onClick: () => void }) => (
  <button onClick={onClick} style={{
    display: "flex", alignItems: "center", gap: 8, width: "100%", padding: "12px 0",
    background: "none", border: "none", cursor: "pointer", fontFamily: FB,
  }}>
    <span style={{ fontSize: 12, color: AL, transition: "transform .2s", transform: open ? "rotate(90deg)" : "none" }}>▶</span>
    <span style={{ fontSize: 14, fontWeight: 700, color: A, letterSpacing: -0.2 }}>{label}</span>
  </button>
);

// ── Helpers ──────────────────────────────────────────────────────────────
const STATES = Object.keys(STATE_NAMES).sort();
const fmtNum = (n: number) => n >= 1_000_000 ? (n / 1_000_000).toFixed(2) + "M" : n >= 1_000 ? (n / 1_000).toFixed(1) + "K" : n.toFixed(0);
const fmtDollars = (n: number) => n >= 1e9 ? "$" + (n / 1e9).toFixed(2) + "B" : n >= 1e6 ? "$" + (n / 1e6).toFixed(1) + "M" : n >= 1e3 ? "$" + (n / 1e3).toFixed(1) + "K" : "$" + n.toFixed(0);
/** Count unique HPSAs — the raw API returns multiple rows per HPSA ID (by provider type, population type). */
const uniqueHpsaCount = (hpsa: any[]): number => new Set(hpsa.map((h: any) => h.hpsa_id).filter(Boolean)).size || hpsa.length;
const fmtPct = (n: number) => (n * 100).toFixed(1) + "%";
const PIE_COLORS = [cB, ACC, "#3B82F6", "#8B5CF6", "#EC4899", "#F59E0B", "#6366F1", "#14B8A6"];

// ── Insight types and computation ────────────────────────────────────────
type InsightSeverity = "info" | "warning" | "alert";

interface Insight {
  icon: string;
  title: string;
  description: string;
  severity: InsightSeverity;
}

const SEVERITY_COLORS: Record<InsightSeverity, { bg: string; border: string; icon: string }> = {
  info:    { bg: "#F0F7F4", border: "#C2D9CC", icon: cB },
  warning: { bg: "#FFF8ED", border: "#F0DDB8", icon: WARN },
  alert:   { bg: "#FDF2F2", border: "#F0C2C2", icon: NEG },
};

const DOMAIN_ICONS: Record<string, string> = {
  rates: "\u25C7",      // ◇ diamond outline
  workforce: "\u25B3",  // △ triangle
  enrollment: "\u25CE", // ◎ bullseye
  hospitals: "\u25A1",  // □ square
  quality: "\u25C9",    // ◉ filled circle
  pharmacy: "\u2295",   // ⊕ circled plus
  economic: "\u25CA",   // ◊ lozenge
};

/** Normalize server-side insights ({type, title, text, domains}) into client Insight format. */
function normalizeServerInsights(serverInsights: any[]): Insight[] {
  return (serverInsights || []).map((si: any) => ({
    icon: DOMAIN_ICONS[si.domains?.[0]] || "\u25CB",
    title: si.title,
    description: si.text,
    severity: (si.type === "warning" ? "warning" : "info") as InsightSeverity,
  }));
}

/** Merge server + client insights, dedup by title similarity, cap at 7. */
function mergeInsights(server: Insight[], client: Insight[]): Insight[] {
  const merged = [...server];
  const serverTitlesLower = new Set(server.map(s => s.title.toLowerCase()));
  for (const ci of client) {
    if (!serverTitlesLower.has(ci.title.toLowerCase())) merged.push(ci);
  }
  const order: Record<InsightSeverity, number> = { alert: 0, warning: 1, info: 2 };
  merged.sort((a, b) => order[a.severity] - order[b.severity]);
  return merged.slice(0, 7);
}

function computeInsights(d: any, state: string): Insight[] {
  const insights: Insight[] = [];

  // ── 1. Rate-to-outcome: Medicaid rate adequacy signal ──────────────
  if (d.cpraSummary?.medianPctMcr && d.cpraSummary.medianPctMcr > 0) {
    const pctMcr = d.cpraSummary.medianPctMcr;
    const pctStr = (pctMcr * 100).toFixed(0);
    const emCount = d.cpraSummary.emCount || 0;
    const hpsaCount = d.hpsa?.length ? uniqueHpsaCount(d.hpsa) : 0;

    if (pctMcr < 0.70) {
      insights.push({
        icon: "\u25C7",
        title: "Low Rate Adequacy",
        description: `${STATE_NAMES[state]} pays a median of ${pctStr}% of Medicare across ${d.cpraSummary.count} matched codes${emCount > 0 ? ` (${emCount} E/M)` : ""}. States paying below 70% of Medicare face higher provider opt-out rates and reduced appointment availability for Medicaid beneficiaries.`,
        severity: "alert",
      });
    } else if (pctMcr < 0.85) {
      insights.push({
        icon: "\u25BD",
        title: "Below-Average Rate Adequacy",
        description: `Medicaid rates are ${pctStr}% of Medicare (national median ~85%). This gap may limit provider participation, particularly for primary care and specialty services${hpsaCount > 20 ? `, compounded by ${hpsaCount} health professional shortage areas in the state` : ""}.`,
        severity: "warning",
      });
    } else if (pctMcr >= 1.0) {
      insights.push({
        icon: "\u25C6",
        title: "Strong Rate Adequacy",
        description: `Medicaid rates are at ${pctStr}% of Medicare, at or above parity. This positions ${STATE_NAMES[state]} favorably for provider recruitment and network adequacy.`,
        severity: "info",
      });
    } else {
      insights.push({
        icon: "\u25CE",
        title: "Rate Adequacy Near National Median",
        description: `Medicaid-to-Medicare ratio of ${pctStr}% is near the national median of ~85%. Across ${d.cpraSummary.count} matched procedure codes${emCount > 0 ? ` (${emCount} E/M)` : ""}.`,
        severity: "info",
      });
    }
  }

  // ── 2. Workforce gap: CNA / home health aide wage vs market ────────
  if (d.wages?.length > 0) {
    const cnaOccs = d.wages.filter((w: any) => {
      const title = (w.occ_title || w.occupation_title || w.occupation || "").toLowerCase();
      const soc = w.soc_code || "";
      return title.includes("nursing assist") || title.includes("home health aide")
        || soc === "31-1131" || soc === "31-1121" || title.includes("personal care aide");
    });

    if (cnaOccs.length > 0) {
      const cna = cnaOccs[0];
      const medianHourly = cna.h_median || cna.hourly_median || cna.median_hourly || 0;
      const p90Hourly = cna.h_pct90 || cna.hourly_p90 || cna.pct90_hourly || 0;
      const title = cna.occ_title || cna.occupation_title || cna.occupation || "Direct Care Worker";

      if (medianHourly > 0) {
        const retailBenchmark = 16.0;
        const premium = ((medianHourly - retailBenchmark) / retailBenchmark * 100);

        if (premium < 10) {
          insights.push({
            icon: "\u25B3",
            title: "Direct Care Workforce Pressure",
            description: `${title}s earn a median of $${medianHourly.toFixed(2)}/hr in ${STATE_NAMES[state]}, only ${premium > 0 ? premium.toFixed(0) + "% above" : Math.abs(premium).toFixed(0) + "% below"} entry-level retail wages ($${retailBenchmark.toFixed(2)}/hr). This narrow gap drives turnover and vacancy in nursing facilities and HCBS programs.${p90Hourly > 0 ? ` Top earners reach $${p90Hourly.toFixed(2)}/hr.` : ""}`,
            severity: premium < 0 ? "alert" : "warning",
          });
        } else {
          insights.push({
            icon: "\u25B3",
            title: "Direct Care Workforce",
            description: `${title}s earn $${medianHourly.toFixed(2)}/hr median in ${STATE_NAMES[state]}, ${premium.toFixed(0)}% above entry-level retail.${p90Hourly > 0 ? ` 90th percentile: $${p90Hourly.toFixed(2)}/hr.` : ""} Higher pay helps retention, but workforce shortages persist nationally.`,
            severity: "info",
          });
        }
      }
    }
  }

  // ── 3. Access risk: HPSAs + rate adequacy ─────────────────────────
  if (d.hpsa?.length > 0) {
    const hpsaCount = uniqueHpsaCount(d.hpsa);
    const pctMcr = d.cpraSummary?.medianPctMcr || 0;
    const byDiscipline: Record<string, number> = {};
    d.hpsa.forEach((h: any) => {
      const disc = h.discipline_type || h.hpsa_discipline || "Unknown";
      byDiscipline[disc] = (byDiscipline[disc] || 0) + 1;
    });
    const discSummary = Object.entries(byDiscipline)
      .sort((a, b) => b[1] - a[1])
      .map(([name, count]) => `${count} ${name}`)
      .join(", ");

    if (hpsaCount > 50 && pctMcr > 0 && pctMcr < 0.85) {
      insights.push({
        icon: "\u25A1",
        title: "Elevated Access Risk",
        description: `${hpsaCount} health professional shortage area designations (${discSummary}). Combined with Medicaid rates at ${(pctMcr * 100).toFixed(0)}% of Medicare, providers in underserved areas face compounding financial pressure that may limit beneficiary access.`,
        severity: "alert",
      });
    } else if (hpsaCount > 30) {
      insights.push({
        icon: "\u25A1",
        title: "Provider Shortage Areas",
        description: `${hpsaCount} HPSA designations across the state (${discSummary}). These areas face documented shortages in primary care, dental, or mental health providers.`,
        severity: hpsaCount > 80 ? "warning" : "info",
      });
    }
  }

  // ── 4. Enrollment trajectory ──────────────────────────────────────
  if (d.enrollment?.length >= 12) {
    const recent = d.enrollment.slice(-12);
    const older = d.enrollment.slice(-24, -12);

    if (older.length >= 6) {
      const latestEnroll = recent[recent.length - 1]?.total_enrollment || 0;
      const yearAgoEnroll = older[older.length - 1]?.total_enrollment || 0;

      if (latestEnroll > 0 && yearAgoEnroll > 0) {
        const yoyChange = ((latestEnroll - yearAgoEnroll) / yearAgoEnroll) * 100;
        const direction = yoyChange > 0 ? "up" : "down";
        const absChange = Math.abs(yoyChange);

        const unwindingNote = d.unwinding?.length > 0
          ? ` PHE unwinding is actively affecting enrollment.`
          : "";

        if (absChange > 5) {
          insights.push({
            icon: yoyChange > 0 ? "\u25B2" : "\u25BD",
            title: `Enrollment ${direction === "up" ? "Growing" : "Declining"} Sharply`,
            description: `Medicaid enrollment is ${direction} ${absChange.toFixed(1)}% year-over-year (${fmtNum(yearAgoEnroll)} \u2192 ${fmtNum(latestEnroll)}).${unwindingNote} Changes of this magnitude impact state budgets, provider networks, and managed care plan capacity.`,
            severity: absChange > 10 ? "warning" : "info",
          });
        } else {
          insights.push({
            icon: "\u25CE",
            title: "Enrollment Trend",
            description: `Enrollment is ${direction} ${absChange.toFixed(1)}% year-over-year at ${fmtNum(latestEnroll)} total enrollees.${unwindingNote}`,
            severity: "info",
          });
        }
      }
    }
  }

  // ── 5. Hospital financial stress + Medicaid dependency ────────────
  if (d.hospitals?.length > 0) {
    const highMedicaid = d.hospitals.filter((h: any) => (h.medicaid_day_pct || 0) > 25);
    const totalHospitals = d.hospitals.length;
    const highMcdPct = totalHospitals > 0 ? (highMedicaid.length / totalHospitals * 100) : 0;

    const negMargin = d.hospitals.filter((h: any) => {
      const rev = h.net_patient_revenue || 0;
      const income = h.net_income || 0;
      return rev > 0 && (income / rev) < 0;
    });

    if (highMedicaid.length > 3 && highMcdPct > 15) {
      const dshTotal = d.hospitalSummary?.total_dsh || 0;
      insights.push({
        icon: "\u25A1",
        title: "Safety Net Hospital Dependency",
        description: `${highMedicaid.length} of ${totalHospitals} hospitals (${highMcdPct.toFixed(0)}%) have Medicaid representing >25% of patient days.${negMargin.length > 0 ? ` ${negMargin.length} hospitals operate at negative margins.` : ""}${dshTotal > 0 ? ` DSH payments total ${fmtDollars(dshTotal)}.` : ""} Rate adequacy directly impacts these safety net facilities.`,
        severity: highMcdPct > 30 ? "warning" : "info",
      });
    }
  }

  // ── 6. Demographics + coverage gap ────────────────────────────────
  if (d.demographics) {
    const demo = d.demographics;
    // pct_poverty/pct_uninsured are already in percentage form (e.g. 12.6 = 12.6%)
    // Convert to decimal (0-1) for consistent math
    const rawPov = demo.poverty_rate || demo.pct_poverty || 0;
    const rawUni = demo.uninsured_rate || demo.pct_uninsured || 0;
    const povertyRate = rawPov > 1 ? rawPov / 100 : rawPov;
    const uninsuredRate = rawUni > 1 ? rawUni / 100 : rawUni;
    const pop = demo.total_population || 0;

    if (povertyRate > 0.15 && uninsuredRate > 0.10) {
      const estUninsuredPoor = Math.round(pop * povertyRate * uninsuredRate);
      insights.push({
        icon: "\u25CA",
        title: "Coverage Gap Signal",
        description: `${(povertyRate * 100).toFixed(1)}% poverty rate combined with ${(uninsuredRate * 100).toFixed(1)}% uninsured rate suggests a significant coverage gap. An estimated ${fmtNum(estUninsuredPoor)} residents are both below the poverty line and uninsured.`,
        severity: "alert",
      });
    } else if (uninsuredRate > 0.08) {
      insights.push({
        icon: "\u25CA",
        title: "Uninsured Population",
        description: `${(uninsuredRate * 100).toFixed(1)}% of residents lack insurance (national avg ~8%). ${pop > 0 ? `Approximately ${fmtNum(Math.round(pop * uninsuredRate))} uninsured individuals.` : ""}`,
        severity: "warning",
      });
    }
  }

  // ── 7. Nursing facility quality signal ─────────────────────────────
  if (d.fiveStarSummary?.avg_overall_rating && d.staffingSummary?.avg_nursing_hprd) {
    const avgRating = d.fiveStarSummary.avg_overall_rating;
    const avgHprd = d.staffingSummary.avg_nursing_hprd;
    const facilityCount = d.fiveStarSummary.facility_count || 0;

    if (avgRating < 3.0 && avgHprd < 3.5) {
      insights.push({
        icon: "\u25C9",
        title: "Nursing Facility Quality Concern",
        description: `${facilityCount > 0 ? facilityCount + " nursing facilities average" : "Average rating is"} ${avgRating.toFixed(1)} out of 5 stars with ${avgHprd.toFixed(2)} nursing hours per resident day. Both metrics fall below national benchmarks (3.3 stars, 3.8 HPRD), indicating systemic quality pressure.`,
        severity: "alert",
      });
    } else if (avgRating < 3.3) {
      insights.push({
        icon: "\u25C9",
        title: "Nursing Facility Quality",
        description: `Nursing facilities average ${avgRating.toFixed(1)} stars (${facilityCount > 0 ? facilityCount + " facilities" : ""}), below the national average of 3.3. Staffing: ${avgHprd.toFixed(2)} nursing hours per resident day.`,
        severity: "warning",
      });
    }
  }

  const severityOrder: Record<InsightSeverity, number> = { alert: 0, warning: 1, info: 2 };
  insights.sort((a, b) => severityOrder[a.severity] - severityOrder[b.severity]);
  return insights.slice(0, 5);
}

// ── Insight Card component ───────────────────────────────────────────────
const InsightCard = ({ insight }: { insight: Insight }) => {
  const colors = SEVERITY_COLORS[insight.severity];
  return (
    <div style={{
      background: colors.bg,
      border: `1px solid ${colors.border}`,
      borderRadius: 8,
      padding: "14px 16px",
      display: "flex",
      gap: 12,
      alignItems: "flex-start",
      minHeight: 80,
    }}>
      <div style={{
        fontSize: 20,
        lineHeight: 1,
        flexShrink: 0,
        marginTop: 1,
      }}>{insight.icon}</div>
      <div style={{ flex: 1, minWidth: 0 }}>
        <div style={{
          fontSize: 12,
          fontWeight: 700,
          color: colors.icon,
          marginBottom: 4,
          letterSpacing: -0.1,
        }}>{insight.title}</div>
        <div style={{
          fontSize: 11,
          color: A,
          lineHeight: 1.5,
          opacity: 0.85,
        }}>{insight.description}</div>
      </div>
    </div>
  );
};

// ── CSV Export ──────────────────────────────────────────────────────────
function downloadCSV(headers: string[], rows: (string | number)[][], filename: string) {
  const csv = [headers.join(","), ...rows.map(r => r.map(c => {
    const s = String(c ?? "");
    return s.includes(",") || s.includes('"') ? `"${s.replace(/"/g, '""')}"` : s;
  }).join(","))].join("\n");
  const a = document.createElement("a");
  const url = URL.createObjectURL(new Blob([csv], { type: "text/csv" }));
  a.href = url;
  a.download = filename;
  a.click();
  setTimeout(() => URL.revokeObjectURL(url), 1000);
}

// ── Safe fetch helper with timeout ──────────────────────────────────────
async function safeFetch(url: string, timeoutMs = 12000) {
  try {
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), timeoutMs);
    const res = await fetch(url, { signal: controller.signal });
    clearTimeout(timer);
    if (!res.ok) return null;
    return await res.json();
  } catch {
    return null;
  }
}

// ── Data loading for a single state ─────────────────────────────────────
async function loadStateData(code: string): Promise<any> {
  const [
    demographics, economic, enrollment, enrollmentByGroup,
    cpraRates, hospitalSummary, hospitals,
    fmap, hpsa, wages, quality, scorecard,
    fiveStarSummary, staffingSummary, topDrugs,
    supplementalSummary, unwinding, spas, insightsData,
    statesData, sdoh,
  ] = await Promise.all([
    safeFetch(`${API_BASE}/api/demographics/${code}`),
    safeFetch(`${API_BASE}/api/economic/${code}`),
    safeFetch(`${API_BASE}/api/enrollment/${code}`),
    safeFetch(`${API_BASE}/api/forecast/public-enrollment/by-group?state=${STATE_NAMES[code] || code}`),
    safeFetch(`${API_BASE}/api/cpra/rates/${code}`),
    safeFetch(`${API_BASE}/api/hospitals/summary`),
    safeFetch(`${API_BASE}/api/hospitals/${code}`),
    safeFetch(`${API_BASE}/api/policy/fmap`),
    safeFetch(`${API_BASE}/api/hpsa/${code}`),
    safeFetch(`${API_BASE}/api/wages/${code}`),
    safeFetch(`${API_BASE}/api/quality/${code}`),
    safeFetch(`${API_BASE}/api/scorecard/${code}`),
    safeFetch(`${API_BASE}/api/five-star/summary`),
    safeFetch(`${API_BASE}/api/staffing/summary`),
    safeFetch(`${API_BASE}/api/pharmacy/top-drugs/${code}`),
    safeFetch(`${API_BASE}/api/supplemental/summary`),
    safeFetch(`${API_BASE}/api/enrollment/unwinding/${code}`),
    safeFetch(`${API_BASE}/api/policy/spas/${code}`),
    safeFetch(`${API_BASE}/api/insights/${code}`),
    safeFetch(`${API_BASE}/api/states`),
    safeFetch(`${API_BASE}/api/sdoh/${code}`),
  ]);

  const allResults = [demographics, economic, enrollment, enrollmentByGroup, cpraRates, hospitalSummary, hospitals, fmap, hpsa, wages, quality, scorecard, fiveStarSummary, staffingSummary, topDrugs, supplementalSummary, unwinding, spas, insightsData, statesData, sdoh];
  const successCount = allResults.filter(r => r !== null).length;
  if (successCount === 0) return null;

  const rows = (d: any): any[] => {
    if (!d) return [];
    if (Array.isArray(d)) return d;
    if (d.rows && Array.isArray(d.rows)) return d.rows;
    if (d.states && Array.isArray(d.states)) return d.states;
    return [];
  };
  const first = (d: any): any => {
    const r = rows(d);
    return r.length > 0 ? r[0] : null;
  };

  const stateInfo = rows(statesData).find((r: any) => r.state_code === code) || null;
  const stateFmap = rows(fmap).find((r: any) => r.state_code === code) || null;
  const stateHospSummary = rows(hospitalSummary).find((r: any) => r.state_code === code) || null;
  const stateFsSummary = rows(fiveStarSummary).find((r: any) => r.state_code === code) || null;
  const stateStaffSummary = rows(staffingSummary).find((r: any) => r.state_code === code) || null;
  const stateSuppSummary = rows(supplementalSummary).find((r: any) => (r.state_code || r.state) === code) || null;

  const cpraRows = rows(cpraRates);
  const emRows = cpraRows.filter((r: any) => r.is_em === true || r.is_em === 1 || r.em_category || r.category_447);
  const allPctMcr = cpraRows.filter((r: any) => r.pct_of_medicare > 0).map((r: any) => r.pct_of_medicare);
  const medianPctMcrRaw = allPctMcr.length > 0
    ? allPctMcr.sort((a: number, b: number) => a - b)[Math.floor(allPctMcr.length / 2)]
    : null;
  const medianPctMcr = medianPctMcrRaw != null ? medianPctMcrRaw / 100 : null;

  return {
    stateInfo,
    demographics: first(demographics),
    economic: rows(economic),
    enrollment: rows(enrollment),
    enrollmentByGroup: rows(enrollmentByGroup),
    cpraRates: cpraRows,
    cpraEmRows: emRows,
    cpraSummary: { count: cpraRows.length, emCount: emRows.length, medianPctMcr },
    hospitalSummary: stateHospSummary,
    hospitals: rows(hospitals),
    fmap: stateFmap,
    hpsa: rows(hpsa),
    wages: rows(wages),
    quality: rows(quality),
    scorecard: rows(scorecard),
    fiveStarSummary: stateFsSummary,
    staffingSummary: stateStaffSummary,
    topDrugs: rows(topDrugs),
    supplementalSummary: stateSuppSummary,
    unwinding: rows(unwinding),
    spas: rows(spas),
    insights: insightsData?.insights || [],
    sdoh: sdoh || null,
  };
}

// ── Parse state codes from URL hash ─────────────────────────────────────
function parseStatesFromHash(): string[] {
  const hash = window.location.hash;
  const m = hash.match(/state\/([A-Za-z+]+)/);
  if (!m) return ["FL"];
  const codes = m[1].toUpperCase().split("+").filter(c => STATE_NAMES[c]);
  return codes.length > 0 ? codes.slice(0, 6) : ["FL"]; // max 6 states
}

// ═══════════════════════════════════════════════════════════════════════
// Comparison Summary Table
// ═══════════════════════════════════════════════════════════════════════
function ComparisonTable({ states, dataMap }: { states: string[]; dataMap: Record<string, any> }) {
  const metrics = [
    {
      label: "Population",
      get: (d: any) => d?.demographics?.total_population ? fmtNum(d.demographics.total_population) : "\u2014",
    },
    {
      label: "Medicaid Enrollment",
      get: (d: any) => {
        if (!d?.enrollment?.length) return "\u2014";
        const latest = d.enrollment[d.enrollment.length - 1];
        return fmtNum(latest.total_enrollment || 0);
      },
    },
    {
      label: "FMAP",
      get: (d: any) => d?.fmap ? `${((d.fmap.fmap_rate || d.fmap.fmap || 0) * 100).toFixed(2)}%` : "\u2014",
    },
    {
      label: "Median % of Medicare",
      get: (d: any) => d?.cpraSummary?.medianPctMcr ? `${(d.cpraSummary.medianPctMcr * 100).toFixed(1)}%` : "\u2014",
      color: (d: any) => {
        const pct = d?.cpraSummary?.medianPctMcr;
        if (!pct) return AL;
        return pct < 0.8 ? NEG : pct > 1.0 ? POS : A;
      },
    },
    {
      label: "Matched Codes",
      get: (d: any) => d?.cpraSummary?.count ? String(d.cpraSummary.count) : "\u2014",
    },
    {
      label: "Hospitals",
      get: (d: any) => d?.hospitals?.length ? String(d.hospitals.length) : "\u2014",
    },
    {
      label: "Median CCR",
      get: (d: any) => {
        const ccr = d?.hospitalSummary?.median_ccr || d?.hospitalSummary?.median_cost_to_charge;
        return ccr ? ccr.toFixed(3) : "\u2014";
      },
    },
    {
      label: "Avg NF Rating",
      get: (d: any) => {
        const r = d?.fiveStarSummary?.avg_overall || d?.fiveStarSummary?.avg_overall_rating;
        return r ? `${r.toFixed(1)} / 5` : "\u2014";
      },
    },
    {
      label: "HPSAs",
      get: (d: any) => d?.hpsa?.length ? String(uniqueHpsaCount(d.hpsa)) : "\u2014",
    },
    {
      label: "Poverty Rate",
      get: (d: any) => {
        const p = d?.demographics?.pct_poverty || d?.demographics?.poverty_rate;
        return p ? `${Number(p).toFixed(1)}%` : "\u2014";
      },
    },
    {
      label: "Uninsured Rate",
      get: (d: any) => {
        const u = d?.demographics?.pct_uninsured || d?.demographics?.uninsured_rate;
        return u ? `${Number(u).toFixed(1)}%` : "\u2014";
      },
    },
    {
      label: "Region",
      get: (d: any) => d?.stateInfo?.region || "\u2014",
    },
  ];

  return (
    <Card accent={cB}>
      <CH title="State Comparison" sub={`${states.length} states side-by-side`} />
      <div style={{ overflowX: "auto" }}>
        <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 11, fontFamily: FB }}>
          <thead>
            <tr style={{ borderBottom: `2px solid ${BD}` }}>
              <th style={{ padding: "8px 10px", textAlign: "left", fontWeight: 600, color: AL, fontSize: 10, minWidth: 130 }}>Metric</th>
              {states.map((s, i) => (
                <th key={s} style={{
                  padding: "8px 10px", textAlign: "right", fontWeight: 700, fontSize: 12,
                  color: COMP_COLORS[i % COMP_COLORS.length],
                  borderBottom: `3px solid ${COMP_COLORS[i % COMP_COLORS.length]}`,
                }}>
                  {s} {STATE_NAMES[s] ? `\u2014 ${STATE_NAMES[s]}` : ""}
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {metrics.map((m, mi) => (
              <tr key={m.label} style={{ background: mi % 2 === 0 ? WH : SF, borderBottom: `1px solid ${BD}` }}>
                <td style={{ padding: "6px 10px", fontWeight: 500, color: A, fontSize: 10 }}>{m.label}</td>
                {states.map((s, si) => {
                  const d = dataMap[s];
                  const col = m.color ? m.color(d) : A;
                  return (
                    <td key={s} style={{ padding: "6px 10px", textAlign: "right", fontFamily: FM, fontWeight: 600, color: col, fontSize: 11 }}>
                      {m.get(d)}
                    </td>
                  );
                })}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </Card>
  );
}

// ═══════════════════════════════════════════════════════════════════════
// Comparison Enrollment Chart — overlays enrollment trends for all states
// ═══════════════════════════════════════════════════════════════════════
function ComparisonEnrollmentChart({ states, dataMap }: { states: string[]; dataMap: Record<string, any> }) {
  // Build unified timeline data — merge all states' enrollment arrays
  const combined = useMemo(() => {
    const byDate: Record<string, Record<string, number>> = {};
    for (const s of states) {
      const enroll = dataMap[s]?.enrollment || [];
      for (const row of enroll.slice(-60)) {
        const key = row.month || row.year || "";
        if (!key) continue;
        if (!byDate[key]) byDate[key] = { _date: key } as any;
        (byDate[key] as any)._date = key;
        byDate[key][s] = row.total_enrollment || 0;
      }
    }
    return Object.values(byDate).sort((a: any, b: any) => a._date < b._date ? -1 : 1);
  }, [states, dataMap]);

  if (combined.length < 3) return null;

  return (
    <Card>
      <CH title="Enrollment Trends" sub={`${states.join(" vs ")} — last 60 data points`} />
      <ChartActions filename={`${states.join("-")}-enrollment`}>
      <ResponsiveContainer width="100%" height={300}>
        <ComposedChart data={combined} margin={{ left: 10, right: 20, top: 5, bottom: 5 }}>
          <CartesianGrid strokeDasharray="3 3" stroke={BD} vertical={false} />
          <XAxis dataKey="_date" tick={{ fontSize: 10, fill: AL, fontFamily: FM }} interval="preserveStartEnd" />
          <YAxis tick={{ fontSize: 10, fill: AL, fontFamily: FM }} tickFormatter={(v: number) => fmtNum(v)} width={60} />
          {states.map((s, i) => (
            <Line key={s} type="monotone" dataKey={s} stroke={COMP_COLORS[i % COMP_COLORS.length]}
              strokeWidth={2} dot={false} name={`${s} — ${STATE_NAMES[s]}`} />
          ))}
          <Tooltip contentStyle={{ fontSize: 11, fontFamily: FM, borderRadius: 6, border: `1px solid ${BD}` }}
            formatter={(v: number) => fmtNum(v)} />
          <Legend wrapperStyle={{ fontSize: 10, fontFamily: FB }} />
        </ComposedChart>
      </ResponsiveContainer>
      </ChartActions>
    </Card>
  );
}

// ═══════════════════════════════════════════════════════════════════════
// Comparison Rate Distribution — bar chart per state
// ═══════════════════════════════════════════════════════════════════════
function ComparisonRateChart({ states, dataMap }: { states: string[]; dataMap: Record<string, any> }) {
  const chartData = useMemo(() => {
    const buckets = [0, 25, 50, 75, 100, 125, 150, 200, 99999];
    const labels = ["<25%", "25-50%", "50-75%", "75-100%", "100-125%", "125-150%", "150-200%", ">200%"];

    return labels.map((label, li) => {
      const row: Record<string, any> = { range: label };
      for (const s of states) {
        const rates = dataMap[s]?.cpraRates || [];
        let count = 0;
        for (const r of rates) {
          if (!r.pct_of_medicare || r.pct_of_medicare <= 0) continue;
          if (r.pct_of_medicare >= buckets[li] && r.pct_of_medicare < buckets[li + 1]) count++;
        }
        row[s] = count;
      }
      return row;
    });
  }, [states, dataMap]);

  const anyData = states.some(s => (dataMap[s]?.cpraRates?.length || 0) > 0);
  if (!anyData) return null;

  return (
    <Card>
      <CH title="Rate Distribution Comparison" sub="% of Medicare — code distribution by bucket" />
      <ChartActions filename={`${states.join("-")}-rate-distribution`}>
      <ResponsiveContainer width="100%" height={260}>
        <BarChart data={chartData} margin={{ left: 5, right: 5, top: 5, bottom: 5 }}>
          <CartesianGrid strokeDasharray="3 3" stroke={BD} vertical={false} />
          <XAxis dataKey="range" tick={{ fontSize: 9, fill: AL, fontFamily: FM }} />
          <YAxis tick={{ fontSize: 10, fill: AL, fontFamily: FM }} width={40} />
          {states.map((s, i) => (
            <Bar key={s} dataKey={s} name={`${s}`} fill={COMP_COLORS[i % COMP_COLORS.length]}
              radius={[2, 2, 0, 0]} />
          ))}
          <Tooltip contentStyle={{ fontSize: 11, fontFamily: FM, borderRadius: 6 }} />
          <Legend wrapperStyle={{ fontSize: 10, fontFamily: FB }} />
        </BarChart>
      </ResponsiveContainer>
      </ChartActions>
      <div style={{ display: "flex", gap: 20, justifyContent: "center", marginTop: 8, fontSize: 10, color: AL }}>
        {states.map((s, i) => {
          const d = dataMap[s];
          return (
            <span key={s} style={{ color: COMP_COLORS[i % COMP_COLORS.length] }}>
              <strong>{s}</strong>: {d?.cpraSummary?.medianPctMcr ? `${(d.cpraSummary.medianPctMcr * 100).toFixed(1)}%` : "\u2014"} median ({d?.cpraSummary?.count || 0} codes)
            </span>
          );
        })}
      </div>
    </Card>
  );
}

// ═══════════════════════════════════════════════════════════════════════
// Comparison Hospital/Quality summary
// ═══════════════════════════════════════════════════════════════════════
function ComparisonHospitalCard({ states, dataMap }: { states: string[]; dataMap: Record<string, any> }) {
  const anyData = states.some(s => (dataMap[s]?.hospitals?.length || 0) > 0);
  if (!anyData) return null;

  return (
    <Card>
      <CH title="Healthcare Infrastructure" sub="Hospitals, nursing facilities, and shortage areas" />
      <div style={{ overflowX: "auto" }}>
        <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 10, fontFamily: FB }}>
          <thead>
            <tr style={{ borderBottom: `2px solid ${BD}` }}>
              <th style={{ padding: "6px 8px", textAlign: "left", fontWeight: 600, color: AL, fontSize: 9 }}>Metric</th>
              {states.map((s, i) => (
                <th key={s} style={{ padding: "6px 8px", textAlign: "right", fontWeight: 700, color: COMP_COLORS[i % COMP_COLORS.length], fontSize: 10 }}>{s}</th>
              ))}
            </tr>
          </thead>
          <tbody>
            {[
              { label: "Hospitals", get: (d: any) => d?.hospitals?.length || 0 },
              { label: "Total Beds", get: (d: any) => d?.hospitalSummary?.total_beds ? fmtNum(d.hospitalSummary.total_beds) : "\u2014" },
              { label: "Median CCR", get: (d: any) => { const c = d?.hospitalSummary?.median_ccr; return c ? c.toFixed(3) : "\u2014"; } },
              { label: "Avg NF Rating", get: (d: any) => { const r = d?.fiveStarSummary?.avg_overall || d?.fiveStarSummary?.avg_overall_rating; return r ? r.toFixed(1) : "\u2014"; } },
              { label: "Avg NF HPRD", get: (d: any) => d?.staffingSummary?.avg_nursing_hprd ? d.staffingSummary.avg_nursing_hprd.toFixed(2) : "\u2014" },
              { label: "HPSAs", get: (d: any) => d?.hpsa?.length ? uniqueHpsaCount(d.hpsa) : 0 },
              { label: "High-Medicaid Hospitals (>25%)", get: (d: any) => { const h = (d?.hospitals || []).filter((h: any) => (h.medicaid_day_pct || 0) > 25); return h.length; } },
            ].map((m, mi) => (
              <tr key={m.label} style={{ background: mi % 2 === 0 ? WH : SF, borderBottom: `1px solid ${BD}` }}>
                <td style={{ padding: "5px 8px", fontWeight: 500, color: A }}>{m.label}</td>
                {states.map(s => (
                  <td key={s} style={{ padding: "5px 8px", textAlign: "right", fontFamily: FM, fontWeight: 600 }}>{m.get(dataMap[s])}</td>
                ))}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </Card>
  );
}

// ═══════════════════════════════════════════════════════════════════════
// Comparison Workforce summary
// ═══════════════════════════════════════════════════════════════════════
function ComparisonWorkforceCard({ states, dataMap }: { states: string[]; dataMap: Record<string, any> }) {
  // Find CNA/HHA wages for each state
  const getCnaWage = (d: any) => {
    if (!d?.wages?.length) return null;
    const cna = d.wages.find((w: any) => {
      const title = (w.occ_title || w.occupation_title || "").toLowerCase();
      const soc = w.soc_code || "";
      return title.includes("nursing assist") || title.includes("home health aide") || soc === "31-1131" || soc === "31-1121";
    });
    return cna ? (cna.h_median || cna.hourly_median || cna.median_hourly || 0) : null;
  };

  const getRnWage = (d: any) => {
    if (!d?.wages?.length) return null;
    const rn = d.wages.find((w: any) => {
      const title = (w.occ_title || w.occupation_title || "").toLowerCase();
      const soc = w.soc_code || "";
      return title.includes("registered nurse") || soc === "29-1141";
    });
    return rn ? (rn.h_median || rn.hourly_median || rn.median_hourly || 0) : null;
  };

  const anyData = states.some(s => (dataMap[s]?.wages?.length || 0) > 0);
  if (!anyData) return null;

  return (
    <Card>
      <CH title="Workforce Wages" sub="BLS median hourly wages for key healthcare occupations" />
      <div style={{ overflowX: "auto" }}>
        <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 10, fontFamily: FB }}>
          <thead>
            <tr style={{ borderBottom: `2px solid ${BD}` }}>
              <th style={{ padding: "6px 8px", textAlign: "left", fontWeight: 600, color: AL, fontSize: 9 }}>Occupation</th>
              {states.map((s, i) => (
                <th key={s} style={{ padding: "6px 8px", textAlign: "right", fontWeight: 700, color: COMP_COLORS[i % COMP_COLORS.length], fontSize: 10 }}>{s}</th>
              ))}
            </tr>
          </thead>
          <tbody>
            {[
              { label: "CNA / Home Health Aide", get: getCnaWage, fmt: (v: number | null) => v ? `$${v.toFixed(2)}/hr` : "\u2014" },
              { label: "Registered Nurse", get: getRnWage, fmt: (v: number | null) => v ? `$${v.toFixed(2)}/hr` : "\u2014" },
            ].map((m, mi) => (
              <tr key={m.label} style={{ background: mi % 2 === 0 ? WH : SF, borderBottom: `1px solid ${BD}` }}>
                <td style={{ padding: "5px 8px", fontWeight: 500, color: A }}>{m.label}</td>
                {states.map(s => (
                  <td key={s} style={{ padding: "5px 8px", textAlign: "right", fontFamily: FM, fontWeight: 600 }}>{m.fmt(m.get(dataMap[s]))}</td>
                ))}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </Card>
  );
}

// ═══════════════════════════════════════════════════════════════════════
// Multi-State Comparison View
// ═══════════════════════════════════════════════════════════════════════
function ComparisonView({ states, dataMap, loading, onChangeStates }: {
  states: string[];
  dataMap: Record<string, any>;
  loading: boolean;
  onChangeStates: (states: string[]) => void;
}) {
  const isMobile = useIsMobile();
  const [addState, setAddState] = useState("");

  const removeState = (code: string) => {
    const next = states.filter(s => s !== code);
    if (next.length > 0) onChangeStates(next);
  };

  const handleAdd = () => {
    if (addState && !states.includes(addState) && states.length < 6) {
      onChangeStates([...states, addState]);
      setAddState("");
    }
  };

  return (
    <div style={{ maxWidth: 1080, margin: "0 auto", padding: isMobile ? "0 12px 32px" : "0 20px 48px", fontFamily: FB }}>
      {/* Header */}
      <div style={{ padding: isMobile ? "20px 0 12px" : "28px 0 20px" }}>
        <div style={{ display: "flex", justifyContent: "space-between", alignItems: "flex-start", flexWrap: "wrap", gap: 12 }}>
          <div>
            <h2 style={{ margin: 0, fontSize: 22, fontWeight: 700, color: A, letterSpacing: -0.3 }}>
              State Comparison
            </h2>
            <p style={{ margin: "4px 0 0", fontSize: 12, color: AL }}>
              Side-by-side analysis of {states.map(s => STATE_NAMES[s] || s).join(", ")}
            </p>
          </div>
          <div style={{ display: "flex", gap: 8, alignItems: "center", flexWrap: "wrap" }}>
            {/* State pills */}
            {states.map((s, i) => (
              <span key={s} style={{
                display: "inline-flex", alignItems: "center", gap: 6,
                padding: "5px 12px", borderRadius: 20,
                background: `${COMP_COLORS[i % COMP_COLORS.length]}12`,
                border: `1px solid ${COMP_COLORS[i % COMP_COLORS.length]}40`,
                fontSize: 11, fontWeight: 600, color: COMP_COLORS[i % COMP_COLORS.length],
              }}>
                {s}
                {states.length > 1 && (
                  <button onClick={() => removeState(s)} style={{
                    background: "none", border: "none", cursor: "pointer", padding: 0,
                    fontSize: 13, color: COMP_COLORS[i % COMP_COLORS.length], lineHeight: 1, opacity: 0.6,
                  }} title={`Remove ${s}`}>&times;</button>
                )}
              </span>
            ))}
            {/* Add state */}
            {states.length < 6 && (
              <div style={{ display: "flex", gap: 4 }}>
                <select value={addState} onChange={e => setAddState(e.target.value)} style={{
                  padding: "5px 8px", borderRadius: 6, border: `1px solid ${BD}`,
                  fontSize: 11, fontFamily: FB, color: addState ? A : AL, background: WH, minWidth: 100,
                }}>
                  <option value="">+ Add state</option>
                  {STATES.filter(s => !states.includes(s)).map(s => (
                    <option key={s} value={s}>{s} — {STATE_NAMES[s]}</option>
                  ))}
                </select>
                {addState && (
                  <button onClick={handleAdd} style={{
                    padding: "5px 12px", borderRadius: 6, border: "none",
                    background: cB, color: WH, fontSize: 11, fontWeight: 600, cursor: "pointer",
                  }}>Add</button>
                )}
              </div>
            )}
            {/* Export */}
            <button onClick={() => {
              const headers = ["Metric", ...states];
              const metrics = [
                { label: "Population", get: (d: any) => d?.demographics?.total_population || "" },
                { label: "Enrollment", get: (d: any) => { const e = d?.enrollment; return e?.length ? (e[e.length - 1].total_enrollment || "") : ""; } },
                { label: "FMAP", get: (d: any) => d?.fmap ? ((d.fmap.fmap_rate || d.fmap.fmap || 0) * 100).toFixed(2) : "" },
                { label: "Median % of Medicare", get: (d: any) => d?.cpraSummary?.medianPctMcr ? (d.cpraSummary.medianPctMcr * 100).toFixed(1) : "" },
                { label: "Hospitals", get: (d: any) => d?.hospitals?.length || "" },
                { label: "HPSAs", get: (d: any) => d?.hpsa?.length ? uniqueHpsaCount(d.hpsa) : "" },
              ];
              const csvRows = metrics.map(m => [m.label, ...states.map(s => m.get(dataMap[s]))]);
              downloadCSV(headers, csvRows, `state_comparison_${states.join("_")}.csv`);
            }} style={{
              padding: "6px 12px", borderRadius: 8, border: `1px solid ${BD}`,
              background: WH, color: AL, fontSize: 11, cursor: "pointer", fontFamily: FM,
            }}>Export CSV</button>
          </div>
        </div>
      </div>

      {loading && (
        <Card>
          <div style={{ textAlign: "center", padding: 40 }}>
            <div style={{ fontSize: 13, color: AL, marginBottom: 8 }}>
              Loading data for {states.map(s => STATE_NAMES[s]).join(", ")}...
            </div>
            <div style={{ fontSize: 11, color: AL, opacity: 0.6 }}>
              Querying {states.length * 18} data sources in parallel
            </div>
          </div>
        </Card>
      )}

      {!loading && Object.keys(dataMap).length > 0 && (
        <>
          <ComparisonTable states={states} dataMap={dataMap} />
          <ComparisonEnrollmentChart states={states} dataMap={dataMap} />
          <ComparisonRateChart states={states} dataMap={dataMap} />
          <ComparisonHospitalCard states={states} dataMap={dataMap} />
          <ComparisonWorkforceCard states={states} dataMap={dataMap} />

          {/* Footer links */}
          <div style={{ display: "flex", gap: 8, flexWrap: "wrap", padding: "8px 0" }}>
            <a href="/#/cpra" style={{ fontSize: 11, color: cB, textDecoration: "none", padding: "6px 12px", borderRadius: 6, border: `1px solid ${BD}`, background: WH }}>
              CPRA Generator
            </a>
            <a href="/#/wages" style={{ fontSize: 11, color: cB, textDecoration: "none", padding: "6px 12px", borderRadius: 6, border: `1px solid ${BD}`, background: WH }}>
              Wage Comparison
            </a>
            <a href="/#/forecast" style={{ fontSize: 11, color: cB, textDecoration: "none", padding: "6px 12px", borderRadius: 6, border: `1px solid ${BD}`, background: WH }}>
              Caseload Forecaster
            </a>
            {/* Switch to single-state for each */}
            {states.map(s => (
              <a key={s} href={`/#/state/${s}`} style={{ fontSize: 11, color: cB, textDecoration: "none", padding: "6px 12px", borderRadius: 6, border: `1px solid ${BD}`, background: WH }}>
                {s} Profile
              </a>
            ))}
          </div>
        </>
      )}
    </div>
  );
}

// ═══════════════════════════════════════════════════════════════════════
// Main component
// ═══════════════════════════════════════════════════════════════════════
export default function StateProfile() {
  const { openIntelligence, addReportSection } = useAradune();
  const isMobile = useIsMobile();
  const [stateCodes, setStateCodes] = useState<string[]>(parseStatesFromHash);
  const stateCtx = useStateContext(stateCodes.length === 1 ? stateCodes[0] : null);
  const [loading, setLoading] = useState(false);
  const [dataMap, setDataMap] = useState<Record<string, any>>({});
  const [apiError, setApiError] = useState(false);

  // Determine mode
  const isComparison = stateCodes.length > 1;
  const singleState = stateCodes[0];

  // Section visibility (single-state only)
  const [sections, setSections] = useState<Record<string, boolean>>({
    enrollment: true, rates: true, hospitals: true,
    quality: true, workforce: false, pharmacy: false, sdoh: true, economic: true,
  });
  const toggle = (key: string) => setSections(s => ({ ...s, [key]: !s[key] }));

  // ── Fetch data for all states in parallel ─────────────────────────
  const loadStates = useCallback(async (codes: string[]) => {
    setLoading(true);
    setDataMap({});
    setApiError(false);
    window.location.hash = `#/state/${codes.join("+")}`;

    const results = await Promise.all(codes.map(c => loadStateData(c)));
    const map: Record<string, any> = {};
    let anySuccess = false;
    codes.forEach((c, i) => {
      if (results[i]) {
        map[c] = results[i];
        anySuccess = true;
      }
    });

    if (!anySuccess) {
      setApiError(true);
    } else {
      setDataMap(map);
    }
    setLoading(false);
  }, []);

  useEffect(() => { loadStates(stateCodes); }, [stateCodes, loadStates]);

  // Listen for hash changes (e.g., user navigates to a different state comparison)
  useEffect(() => {
    const handler = () => {
      const parsed = parseStatesFromHash();
      setStateCodes(prev => {
        if (parsed.length === prev.length && parsed.every((c, i) => c === prev[i])) return prev;
        return parsed;
      });
    };
    window.addEventListener("hashchange", handler);
    return () => window.removeEventListener("hashchange", handler);
  }, []);

  // ── Comparison mode ───────────────────────────────────────────────
  if (isComparison) {
    return (
      <ComparisonView
        states={stateCodes}
        dataMap={dataMap}
        loading={loading}
        onChangeStates={setStateCodes}
      />
    );
  }

  // ═══ SINGLE-STATE VIEW ═════════════════════════════════════════════
  const state = singleState;
  const d = dataMap[state] || null;

  return (
    <div style={{ maxWidth: 1080, margin: "0 auto", padding: isMobile ? "0 12px 32px" : "0 20px 48px", fontFamily: FB }}>
      {/* Header */}
      <div style={{ padding: isMobile ? "20px 0 12px" : "28px 0 20px", display: "flex", justifyContent: "space-between", alignItems: isMobile ? "flex-start" : "flex-end", flexWrap: "wrap", gap: 8 }}>
        <div>
          <h2 style={{ margin: 0, fontSize: 22, fontWeight: 700, color: A, letterSpacing: -0.3 }}>
            State Profile
          </h2>
          <p style={{ margin: "4px 0 0", fontSize: 12, color: AL }}>
            Everything Aradune knows about a state: enrollment, rates, quality, workforce, and economy.
          </p>
        </div>
        <div style={{ display: "flex", gap: 8, alignItems: "center" }}>
          <select value={state} onChange={e => setStateCodes([e.target.value])} style={{
            padding: "8px 14px", borderRadius: 8, border: `1px solid ${BD}`,
            fontSize: 13, fontFamily: FB, color: A, background: WH, fontWeight: 600, minWidth: 220,
          }}>
            {STATES.map(s => <option key={s} value={s}>{s} — {STATE_NAMES[s]}</option>)}
          </select>
          {/* Compare button */}
          <button onClick={() => {
            const other = STATES.filter(s => s !== state).slice(0, 2);
            setStateCodes([state, ...other.slice(0, 1)]);
          }} style={{
            padding: "8px 14px", borderRadius: 8, border: `1px solid ${BD}`,
            background: WH, color: cB, fontSize: 12, cursor: "pointer", fontFamily: FB, fontWeight: 600,
          }}>+ Compare</button>
          <button onClick={() => openIntelligence({ state, summary: `User is viewing State Profile for ${STATE_NAMES[state] || state}.${stateCtx.data ? " " + stateContextSummary(stateCtx.data) : ""}` })} style={{
            padding: "8px 14px", borderRadius: 8, border: "none",
            background: cB, color: WH, fontSize: 12, cursor: "pointer", fontFamily: FB, fontWeight: 600,
          }}>Ask Aradune</button>
          {d && <button onClick={() => {
            const rows: (string | number)[][] = [];
            if (d.cpraRates.length > 0) {
              for (const r of d.cpraRates) {
                rows.push([r.cpt_hcpcs_code || r.code || "", r.description || r.desc || "", r.medicaid_rate?.toFixed(2) || "", r.medicare_nonfac_rate?.toFixed(2) || "", r.pct_of_medicare ? r.pct_of_medicare.toFixed(1) : ""]);
              }
            }
            if (rows.length > 0) {
              downloadCSV(["HCPCS Code", "Description", "Medicaid Rate", "Medicare Rate", "% of Medicare"], rows, `state_profile_rates_${state}.csv`);
            } else {
              const hospRows = (d.hospitals || []).map((h: any) => [
                h.hospital_name || h.provider_name || "", h.city || "", h.bed_count || h.beds || "",
                h.medicaid_days || "", h.medicaid_day_pct ? (h.medicaid_day_pct * 100).toFixed(1) : "",
                h.cost_to_charge_ratio?.toFixed(3) || "",
              ]);
              if (hospRows.length) downloadCSV(["Hospital", "City", "Beds", "Medicaid Days", "Medicaid %", "CCR"], hospRows, `state_profile_hospitals_${state}.csv`);
            }
          }} style={{
            padding: "8px 14px", borderRadius: 8, border: `1px solid ${BD}`,
            background: WH, color: AL, fontSize: 12, cursor: "pointer", fontFamily: FM,
          }}>Export CSV</button>}
          {d && <button onClick={() => {
            const summary = [
              `State Profile: ${STATE_NAMES[state] || state}`,
              d.enrollment?.total ? `Enrollment: ${d.enrollment.total.toLocaleString()}` : null,
              d.cpraRates?.length ? `Rate codes: ${d.cpraRates.length}` : null,
              d.hospitals?.length ? `Hospitals: ${d.hospitals.length}` : null,
            ].filter(Boolean).join(". ");
            addReportSection({
              id: crypto.randomUUID(),
              prompt: `State Profile for ${STATE_NAMES[state] || state}`,
              response: summary,
              queries: [],
              createdAt: new Date(),
            });
          }} style={{
            padding: "8px 14px", borderRadius: 8, border: `1px solid ${BD}`,
            background: WH, color: AL, fontSize: 12, cursor: "pointer", fontFamily: FM,
          }}>+ Report</button>}
        </div>
      </div>

      {loading && (
        <Card>
          <div style={{ textAlign: "center", padding: 40 }}>
            <div style={{ fontSize: 13, color: AL, marginBottom: 8 }}>
              Loading data for {STATE_NAMES[state]}...
            </div>
            <div style={{ fontSize: 11, color: AL, opacity: 0.6 }}>
              Querying 18 data sources in parallel
            </div>
          </div>
        </Card>
      )}

      {apiError && !loading && (
        <Card accent={WARN}>
          <div style={{ textAlign: "center", padding: "32px 20px" }}>
            <div style={{ fontSize: 24, marginBottom: 12 }}>&#9201;</div>
            <div style={{ fontSize: 15, fontWeight: 700, color: A, marginBottom: 8 }}>
              Server is warming up
            </div>
            <div style={{ fontSize: 12, color: AL, lineHeight: 1.6, maxWidth: 440, margin: "0 auto 20px" }}>
              The data server scales to zero when idle and takes about 30 seconds to load 250+ datasets from storage.
              It's probably ready now. Try again.
            </div>
            <button onClick={() => loadStates(stateCodes)} style={{
              padding: "10px 24px", borderRadius: 8, border: "none",
              background: cB, color: WH, fontSize: 13, fontWeight: 600,
              cursor: "pointer", fontFamily: FB,
            }}>
              Retry
            </button>
          </div>
        </Card>
      )}

      {d && !loading && (() => {
        const demo = d.demographics;
        const fmapVal = d.fmap;

        return (
          <>
            {/* ─── Overview Card ──────────────────────────────────────── */}
            <Card accent={cB}>
              <div style={{ display: "flex", justifyContent: "space-between", alignItems: "flex-start" }}>
                <div>
                  <h2 style={{ margin: 0, fontSize: 24, fontWeight: 800, color: A, letterSpacing: -0.5 }}>
                    {STATE_NAMES[state]}
                  </h2>
                  <p style={{ margin: "2px 0 0", fontSize: 11, color: AL }}>
                    {state} | {d.stateInfo?.region || "\u2014"}
                  </p>
                </div>
                {fmapVal && (
                  <div style={{ textAlign: "right" }}>
                    <div style={{ fontSize: 10, color: AL }}>FMAP (FY{fmapVal.fiscal_year})</div>
                    <div style={{ fontSize: 20, fontWeight: 700, color: cB, fontFamily: FM }}>{((fmapVal.fmap_rate || fmapVal.fmap || 0) * 100).toFixed(2)}%</div>
                  </div>
                )}
              </div>

              <div style={{ display: "flex", gap: 16, flexWrap: "wrap", justifyContent: "space-around", padding: "16px 0 8px", borderTop: `1px solid ${BD}`, marginTop: 12 }}>
                {demo && <>
                  <Met label="Population" value={fmtNum(demo.total_population || 0)} mono />
                  <Met label="Poverty Rate" value={demo.pct_poverty ? `${Number(demo.pct_poverty).toFixed(1)}%` : "\u2014"} color={demo.pct_poverty > 15 ? NEG : AL} mono />
                  <Met label="Uninsured" value={demo.pct_uninsured ? `${Number(demo.pct_uninsured).toFixed(1)}%` : "\u2014"} color={demo.pct_uninsured > 10 ? NEG : AL} mono />
                </>}
                {d.enrollment.length > 0 && (() => {
                  const latest = d.enrollment[d.enrollment.length - 1];
                  const totalEnroll = latest.total_enrollment || latest.total_medicaid_enrollment || 0;
                  const mcEnroll = latest.mc_enrollment || latest.managed_care_enrollment || 0;
                  // If mc_enrollment is missing, fall back to stateInfo managed care % or mc_enrollment_summary
                  let mcPct: string | null = null;
                  if (mcEnroll > 0 && totalEnroll > 0) {
                    mcPct = `${((mcEnroll / totalEnroll) * 100).toFixed(0)}%`;
                  } else if (d.stateInfo?.pct_managed_care && !isNaN(d.stateInfo.pct_managed_care)) {
                    mcPct = `${Number(d.stateInfo.pct_managed_care).toFixed(0)}%`;
                  } else if (d.stateInfo?.mc_penetration_pct && !isNaN(d.stateInfo.mc_penetration_pct)) {
                    mcPct = `${Number(d.stateInfo.mc_penetration_pct).toFixed(0)}%`;
                  }
                  return <>
                    <Met label="Medicaid Enrollment" value={fmtNum(totalEnroll)} color={cB} mono />
                    <Met label="Managed Care %" value={mcPct || "\u2014"} mono />
                  </>;
                })()}
                {d.cpraSummary.count > 0 && (
                  <Met label="Median % of Medicare" value={d.cpraSummary.medianPctMcr ? `${(d.cpraSummary.medianPctMcr * 100).toFixed(1)}%` : "\u2014"}
                    color={d.cpraSummary.medianPctMcr < 0.8 ? NEG : d.cpraSummary.medianPctMcr > 1.0 ? POS : AL} mono />
                )}
                {d.hospitalSummary && (
                  <Met label="Hospitals" value={d.hospitalSummary.hospital_count || d.hospitals.length || "\u2014"} mono />
                )}
                {d.hpsa.length > 0 && (
                  <Met label="HPSA Designations" value={uniqueHpsaCount(d.hpsa)} mono />
                )}
              </div>
            </Card>

            <StateContextBar stateCode={state} mode="compact" />

            {/* ─── Cross-Dataset Insights ──────────────────────────────── */}
            {(() => {
              const serverInsights = normalizeServerInsights(d.insights);
              const clientInsights = computeInsights(d, state);
              const insights = mergeInsights(serverInsights, clientInsights);
              if (insights.length === 0) return null;
              return (
                <div style={{ marginBottom: 16 }}>
                  <div style={{
                    display: "flex", alignItems: "center", gap: 8,
                    padding: "12px 0 8px",
                  }}>
                    <span style={{ fontSize: 14, fontWeight: 700, color: A, letterSpacing: -0.2 }}>
                      Cross-Dataset Insights
                    </span>
                    <span style={{
                      fontSize: 9, fontWeight: 600, color: WH, background: cB,
                      padding: "2px 8px", borderRadius: 10,
                    }}>
                      {insights.length} signal{insights.length !== 1 ? "s" : ""}
                    </span>
                  </div>
                  <div style={{
                    display: "grid",
                    gridTemplateColumns: "repeat(auto-fit, minmax(280px, 1fr))",
                    gap: 10,
                  }}>
                    {insights.map((insight, i) => (
                      <InsightCard key={i} insight={insight} />
                    ))}
                  </div>
                </div>
              );
            })()}

            {/* ─── Enrollment ─────────────────────────────────────────── */}
            <SectionToggle label="Enrollment & Eligibility" open={sections.enrollment} onClick={() => toggle("enrollment")} />
            {sections.enrollment && d.enrollment.length > 0 && (
              <Card>
                <CH title="Enrollment Trends" sub={`${d.enrollment.length} data points`} />
                <ResponsiveContainer width="100%" height={280}>
                  <ComposedChart data={d.enrollment.slice(-60)} margin={{ left: 10, right: 20, top: 5, bottom: 5 }}>
                    <CartesianGrid strokeDasharray="3 3" stroke={BD} vertical={false} />
                    <XAxis dataKey={d.enrollment[0]?.month ? "month" : "year"} tick={{ fontSize: 10, fill: AL, fontFamily: FM }} interval="preserveStartEnd" />
                    <YAxis tick={{ fontSize: 10, fill: AL, fontFamily: FM }} tickFormatter={(v: number) => fmtNum(v)} width={60} />
                    <Area type="monotone" dataKey="total_enrollment" fill={`${cB}15`} stroke={cB} strokeWidth={2} name="Total Enrollment" />
                    {d.enrollment[0]?.mc_enrollment !== undefined && (
                      <Line type="monotone" dataKey="mc_enrollment" stroke={ACC} strokeWidth={1.5} dot={false} name="Managed Care" strokeDasharray="4 2" />
                    )}
                    {d.enrollment[0]?.managed_care_enrollment !== undefined && (
                      <Line type="monotone" dataKey="managed_care_enrollment" stroke={ACC} strokeWidth={1.5} dot={false} name="Managed Care" strokeDasharray="4 2" />
                    )}
                    <Tooltip contentStyle={{ fontSize: 11, fontFamily: FM, borderRadius: 6, border: `1px solid ${BD}` }}
                      formatter={(v: number) => fmtNum(v)} />
                    <Legend wrapperStyle={{ fontSize: 10, fontFamily: FB }} />
                  </ComposedChart>
                </ResponsiveContainer>

                {/* Unwinding impact */}
                {d.unwinding.length > 0 && (
                  <div style={{ marginTop: 12, paddingTop: 12, borderTop: `1px solid ${BD}` }}>
                    <div style={{ fontSize: 11, fontWeight: 600, color: A, marginBottom: 6 }}>PHE Unwinding Impact</div>
                    <div style={{ display: "flex", gap: 16, flexWrap: "wrap" }}>
                      {d.unwinding.slice(-6).map((r: any, i: number) => (
                        <div key={i} style={{ fontSize: 10, color: AL }}>
                          <span style={{ fontFamily: FM }}>{r.month || r.reporting_period}</span>:{" "}
                          <span style={{ color: NEG, fontWeight: 600 }}>{fmtNum(r.total_disenrolled || r.terminated_count || 0)}</span> disenrolled
                        </div>
                      ))}
                    </div>
                  </div>
                )}
              </Card>
            )}

            {/* ─── Rates & CPRA ───────────────────────────────────────── */}
            <SectionToggle label="Rate Adequacy & Fee Schedule" open={sections.rates} onClick={() => toggle("rates")} />
            {sections.rates && (
              <Card>
                <CH title="Medicaid-to-Medicare Rate Comparison" sub={`${d.cpraRates.length} codes matched | ${d.cpraSummary.emCount} E/M codes`} />
                {d.cpraRates.length < 200 && d.cpraRates.length > 0 && (
                  <div style={{ margin: "0 16px 12px", padding: "8px 12px", borderRadius: 6, background: "#FEF3C7", border: "1px solid #F59E0B", fontSize: 11, color: "#92400E", lineHeight: 1.5 }}>
                    Limited published fee schedule data for this state ({d.cpraRates.length} codes). This state may use bundled payments (DRG/APG) or be primarily managed care.{" "}
                    <a href="#/research/tmsis-calibration" style={{ color: "#92400E", fontWeight: 600 }}>View T-MSIS claims-based rate analysis</a> for additional coverage.
                  </div>
                )}
                {d.cpraRates.length === 0 && (
                  <div style={{ margin: "0 16px 12px", padding: "8px 12px", borderRadius: 6, background: "#FEF3C7", border: "1px solid #F59E0B", fontSize: 11, color: "#92400E", lineHeight: 1.5 }}>
                    No published FFS fee schedule available for this state.{" "}
                    <a href="#/research/tmsis-calibration" style={{ color: "#92400E", fontWeight: 600 }}>View T-MSIS claims-based rate estimates</a> (labeled as claims data, not fee schedule).
                  </div>
                )}
                {d.cpraRates.length > 0 ? (() => {
                  // Build distribution histogram (pct_of_medicare is already percentage, e.g. 76.83)
                  const buckets = [0, 25, 50, 75, 100, 125, 150, 200, 99999];
                  const labels = ["<25%", "25-50%", "50-75%", "75-100%", "100-125%", "125-150%", "150-200%", ">200%"];
                  const hist = labels.map(() => 0);
                  d.cpraRates.forEach((r: any) => {
                    if (!r.pct_of_medicare || r.pct_of_medicare <= 0) return;
                    for (let i = 0; i < buckets.length - 1; i++) {
                      if (r.pct_of_medicare >= buckets[i] && r.pct_of_medicare < buckets[i + 1]) { hist[i]++; break; }
                    }
                  });
                  const histData = labels.map((l, i) => ({ range: l, count: hist[i] }));
                  const barColors = ["#DC2626", "#EA580C", "#D97706", "#CA8A04", POS, "#059669", "#0891B2", "#6366F1"];

                  return (
                    <>
                      <ResponsiveContainer width="100%" height={200}>
                        <BarChart data={histData} margin={{ left: 5, right: 5, top: 5, bottom: 5 }}>
                          <CartesianGrid strokeDasharray="3 3" stroke={BD} vertical={false} />
                          <XAxis dataKey="range" tick={{ fontSize: 9, fill: AL, fontFamily: FM }} />
                          <YAxis tick={{ fontSize: 10, fill: AL, fontFamily: FM }} width={40} />
                          <Bar dataKey="count" name="Codes" radius={[4, 4, 0, 0]}>
                            {histData.map((_, i) => <Cell key={i} fill={barColors[i]} />)}
                          </Bar>
                          <Tooltip contentStyle={{ fontSize: 11, fontFamily: FM, borderRadius: 6 }} />
                        </BarChart>
                      </ResponsiveContainer>
                      <div style={{ display: "flex", gap: 20, justifyContent: "center", marginTop: 8, fontSize: 10, color: AL }}>
                        <span>Median: <strong style={{ color: A, fontFamily: FM }}>
                          {d.cpraSummary.medianPctMcr ? `${(d.cpraSummary.medianPctMcr * 100).toFixed(1)}%` : "\u2014"}
                        </strong></span>
                        <span>Codes matched: <strong style={{ color: A, fontFamily: FM }}>{d.cpraRates.length}</strong></span>
                        <span>E/M codes: <strong style={{ color: A, fontFamily: FM }}>{d.cpraSummary.emCount}</strong></span>
                      </div>
                    </>
                  );
                })() : (
                  <div style={{ textAlign: "center", padding: 20, fontSize: 12, color: AL }}>No rate comparison data available for {state}.</div>
                )}

                {/* SPAs */}
                {d.spas.length > 0 && (
                  <div style={{ marginTop: 16, paddingTop: 12, borderTop: `1px solid ${BD}` }}>
                    <div style={{ fontSize: 11, fontWeight: 600, color: A, marginBottom: 6 }}>Recent State Plan Amendments ({d.spas.length} total)</div>
                    <div style={{ display: "flex", flexDirection: "column", gap: 4 }}>
                      {d.spas.slice(0, 5).map((spa: any, i: number) => (
                        <div key={i} style={{ display: "flex", gap: 8, fontSize: 10, color: AL, padding: "4px 0", borderBottom: i < 4 ? `1px solid ${SF}` : undefined }}>
                          <span style={{ fontFamily: FM, minWidth: 80, color: A }}>{spa.spa_id || spa.spa_number || "\u2014"}</span>
                          <span style={{ flex: 1 }}>{spa.title || spa.description || "\u2014"}</span>
                          <span style={{ fontFamily: FM, color: AL }}>{spa.effective_date || spa.approval_date || ""}</span>
                        </div>
                      ))}
                    </div>
                  </div>
                )}

                {/* Supplemental payments */}
                {d.supplementalSummary && (
                  <div style={{ marginTop: 16, paddingTop: 12, borderTop: `1px solid ${BD}` }}>
                    <div style={{ fontSize: 11, fontWeight: 600, color: A, marginBottom: 6 }}>Supplemental Payments</div>
                    <div style={{ display: "flex", gap: 20, flexWrap: "wrap" }}>
                      {d.supplementalSummary.total_hospital_payments && (
                        <Met label="Total Hospital" value={fmtDollars(d.supplementalSummary.total_hospital_payments)} mono small />
                      )}
                      {d.supplementalSummary.dsh_payments && (
                        <Met label="DSH" value={fmtDollars(d.supplementalSummary.dsh_payments)} mono small />
                      )}
                      {d.supplementalSummary.supplemental_pct && (
                        <Met label="Supplemental %" value={`${(d.supplementalSummary.supplemental_pct * 100).toFixed(1)}%`} mono small />
                      )}
                    </div>
                  </div>
                )}
              </Card>
            )}

            {/* ─── Hospitals & Infrastructure ─────────────────────────── */}
            <SectionToggle label="Healthcare Infrastructure" open={sections.hospitals} onClick={() => toggle("hospitals")} />
            {sections.hospitals && (
              <Card>
                <CH title="Hospitals & Facilities" sub={`${d.hospitals.length} hospitals${d.fiveStarSummary ? ` | ${d.fiveStarSummary.facility_count || 0} nursing facilities` : ""}`} />

                <div style={{ display: "flex", gap: 16, flexWrap: "wrap", justifyContent: "space-around", padding: "8px 0", marginBottom: 12 }}>
                  <Met label="Hospitals" value={d.hospitals.length} mono small />
                  {d.hospitalSummary?.total_beds && <Met label="Total Beds" value={fmtNum(d.hospitalSummary.total_beds)} mono small />}
                  {(d.hospitalSummary?.median_ccr || d.hospitalSummary?.median_cost_to_charge) && <Met label="Median CCR" value={(d.hospitalSummary.median_ccr || d.hospitalSummary.median_cost_to_charge).toFixed(3)} mono small />}
                  {(d.fiveStarSummary?.avg_overall || d.fiveStarSummary?.avg_overall_rating) && <Met label="Avg NF Rating" value={`${(d.fiveStarSummary.avg_overall || d.fiveStarSummary.avg_overall_rating).toFixed(1)}`} mono small />}
                  {d.staffingSummary?.avg_nursing_hprd && <Met label="Avg NF HPRD" value={d.staffingSummary.avg_nursing_hprd.toFixed(2)} mono small />}
                  {d.hpsa.length > 0 && <Met label="HPSAs" value={uniqueHpsaCount(d.hpsa)} color={uniqueHpsaCount(d.hpsa) > 50 ? NEG : AL} mono small />}
                </div>

                {/* HPSA breakdown by discipline */}
                {d.hpsa.length > 0 && (() => {
                  const byDiscipline: Record<string, number> = {};
                  d.hpsa.forEach((h: any) => {
                    const disc = h.discipline_type || h.hpsa_discipline || "Unknown";
                    byDiscipline[disc] = (byDiscipline[disc] || 0) + 1;
                  });
                  const pieData = Object.entries(byDiscipline).map(([name, value]) => ({ name, value }));
                  return (
                    <div style={{ display: "flex", gap: 20, alignItems: "center", marginTop: 8, paddingTop: 8, borderTop: `1px solid ${BD}` }}>
                      <div style={{ fontSize: 11, fontWeight: 600, color: A }}>Shortage Areas by Discipline</div>
                      <div style={{ display: "flex", gap: 12 }}>
                        {pieData.map((d, i) => (
                          <span key={d.name} style={{ fontSize: 10, color: AL }}>
                            <span style={{ display: "inline-block", width: 8, height: 8, borderRadius: 4, background: PIE_COLORS[i % PIE_COLORS.length], marginRight: 4 }} />
                            {d.name}: <strong>{d.value}</strong>
                          </span>
                        ))}
                      </div>
                    </div>
                  );
                })()}

                {/* Top hospitals by Medicaid days */}
                {d.hospitals.length > 0 && (
                  <div style={{ marginTop: 12, paddingTop: 12, borderTop: `1px solid ${BD}` }}>
                    <div style={{ fontSize: 11, fontWeight: 600, color: A, marginBottom: 6 }}>Top Hospitals by Medicaid Volume</div>
                    <div style={{ overflowX: "auto" }}>
                      <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 10, fontFamily: FB }}>
                        <thead>
                          <tr style={{ borderBottom: `2px solid ${BD}` }}>
                            {["Hospital", "City", "Beds", "Medicaid Days", "Medicaid %", "CCR"].map(h => (
                              <th key={h} style={{ padding: "6px 8px", textAlign: "left", fontWeight: 600, color: A, fontSize: 9, letterSpacing: 0.3 }}>{h}</th>
                            ))}
                          </tr>
                        </thead>
                        <tbody>
                          {d.hospitals
                            .sort((a: any, b: any) => (b.medicaid_days || 0) - (a.medicaid_days || 0))
                            .slice(0, 10)
                            .map((h: any, i: number) => (
                              <tr key={i} style={{ background: i % 2 === 0 ? WH : SF, borderBottom: `1px solid ${BD}` }}>
                                <td style={{ padding: "5px 8px", maxWidth: 200, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>{h.hospital_name || h.provider_name || "\u2014"}</td>
                                <td style={{ padding: "5px 8px", color: AL }}>{h.city || "\u2014"}</td>
                                <td style={{ padding: "5px 8px", fontFamily: FM }}>{h.bed_count || h.beds || "\u2014"}</td>
                                <td style={{ padding: "5px 8px", fontFamily: FM }}>{h.medicaid_days ? fmtNum(h.medicaid_days) : "\u2014"}</td>
                                <td style={{ padding: "5px 8px", fontFamily: FM, color: (h.medicaid_day_pct || 0) > 25 ? POS : AL }}>
                                  {h.medicaid_day_pct ? `${Number(h.medicaid_day_pct).toFixed(1)}%` : "\u2014"}
                                </td>
                                <td style={{ padding: "5px 8px", fontFamily: FM }}>{h.cost_to_charge_ratio?.toFixed(3) || "\u2014"}</td>
                              </tr>
                            ))
                          }
                        </tbody>
                      </table>
                    </div>
                  </div>
                )}
              </Card>
            )}

            {/* ─── Quality ────────────────────────────────────────────── */}
            <SectionToggle label="Quality Measures & Outcomes" open={sections.quality} onClick={() => toggle("quality")} />
            {sections.quality && (
              <Card>
                <CH title="Quality & Scorecard" sub={`${d.quality.length} quality measures | ${d.scorecard.length} scorecard items`} />

                {d.scorecard.length > 0 && (
                  <div style={{ overflowX: "auto" }}>
                    <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 10, fontFamily: FB }}>
                      <thead>
                        <tr style={{ borderBottom: `2px solid ${BD}` }}>
                          {["Measure", "Period", "Value", "Median"].map(h => (
                            <th key={h} style={{ padding: "6px 8px", textAlign: "left", fontWeight: 600, color: A, fontSize: 9 }}>{h}</th>
                          ))}
                        </tr>
                      </thead>
                      <tbody>
                        {d.scorecard.slice(0, 15).map((s: any, i: number) => {
                          const val = s.measure_value ?? s.value;
                          const med = s.median_value ?? s.median;
                          return (
                          <tr key={i} style={{ background: i % 2 === 0 ? WH : SF, borderBottom: `1px solid ${BD}` }}>
                            <td style={{ padding: "5px 8px", maxWidth: 300, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>
                              {s.measure_name || s.measure_id || "\u2014"}
                            </td>
                            <td style={{ padding: "5px 8px", fontFamily: FM, color: AL }}>{s.data_period || "\u2014"}</td>
                            <td style={{ padding: "5px 8px", fontFamily: FM, fontWeight: 600 }}>
                              {val != null ? (typeof val === "number" ? val.toFixed(1) : val) : "\u2014"}
                            </td>
                            <td style={{ padding: "5px 8px", fontFamily: FM, color: AL }}>
                              {med != null ? (typeof med === "number" ? med.toFixed(1) : med) : "\u2014"}
                            </td>
                          </tr>
                          );
                        })}
                      </tbody>
                    </table>
                    {d.scorecard.length > 15 && (
                      <div style={{ textAlign: "center", fontSize: 10, color: AL, padding: "8px 0" }}>
                        Showing 15 of {d.scorecard.length} scorecard measures
                      </div>
                    )}
                  </div>
                )}

                {d.quality.length > 0 && d.scorecard.length === 0 && (
                  <div style={{ overflowX: "auto" }}>
                    <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 10, fontFamily: FB }}>
                      <thead>
                        <tr style={{ borderBottom: `2px solid ${BD}` }}>
                          {["Measure", "Domain", "Year", "Rate"].map(h => (
                            <th key={h} style={{ padding: "6px 8px", textAlign: "left", fontWeight: 600, color: A, fontSize: 9 }}>{h}</th>
                          ))}
                        </tr>
                      </thead>
                      <tbody>
                        {d.quality.slice(0, 15).map((q: any, i: number) => (
                          <tr key={i} style={{ background: i % 2 === 0 ? WH : SF }}>
                            <td style={{ padding: "5px 8px" }}>{q.measure_name || q.measure_id || "\u2014"}</td>
                            <td style={{ padding: "5px 8px", color: AL }}>{q.domain || "\u2014"}</td>
                            <td style={{ padding: "5px 8px", fontFamily: FM }}>{q.year || "\u2014"}</td>
                            <td style={{ padding: "5px 8px", fontFamily: FM, fontWeight: 600 }}>{q.rate != null ? q.rate.toFixed(1) : "\u2014"}</td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                )}

                {d.quality.length === 0 && d.scorecard.length === 0 && (
                  <div style={{ textAlign: "center", padding: 20, fontSize: 12, color: AL }}>No quality data available.</div>
                )}
              </Card>
            )}

            {/* ─── Workforce ──────────────────────────────────────────── */}
            <SectionToggle label="Workforce & Wages" open={sections.workforce} onClick={() => toggle("workforce")} />
            {sections.workforce && (
              <Card>
                <CH title="Healthcare Workforce Wages" sub={`BLS OEWS data | ${d.wages.length} occupation records`} />
                {d.wages.length > 0 ? (
                  <div style={{ overflowX: "auto" }}>
                    <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 10, fontFamily: FB }}>
                      <thead>
                        <tr style={{ borderBottom: `2px solid ${BD}` }}>
                          {["Occupation", "Employment", "Median Hourly", "Mean Hourly", "90th Pctl"].map(h => (
                            <th key={h} style={{ padding: "6px 8px", textAlign: "left", fontWeight: 600, color: A, fontSize: 9 }}>{h}</th>
                          ))}
                        </tr>
                      </thead>
                      <tbody>
                        {d.wages
                          .sort((a: any, b: any) => (b.tot_emp || b.employment || 0) - (a.tot_emp || a.employment || 0))
                          .slice(0, 20)
                          .map((w: any, i: number) => (
                            <tr key={i} style={{ background: i % 2 === 0 ? WH : SF, borderBottom: `1px solid ${BD}` }}>
                              <td style={{ padding: "5px 8px", maxWidth: 250, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>
                                {w.occ_title || w.occupation || "\u2014"}
                              </td>
                              <td style={{ padding: "5px 8px", fontFamily: FM }}>{fmtNum(w.tot_emp || w.employment || 0)}</td>
                              <td style={{ padding: "5px 8px", fontFamily: FM }}>${(w.h_median || w.median_hourly || 0).toFixed(2)}</td>
                              <td style={{ padding: "5px 8px", fontFamily: FM, color: AL }}>${(w.h_mean || w.mean_hourly || 0).toFixed(2)}</td>
                              <td style={{ padding: "5px 8px", fontFamily: FM, color: AL }}>${(w.h_pct90 || w.pct90_hourly || 0).toFixed(2)}</td>
                            </tr>
                          ))
                        }
                      </tbody>
                    </table>
                  </div>
                ) : (
                  <div style={{ textAlign: "center", padding: 20, fontSize: 12, color: AL }}>No wage data available.</div>
                )}
              </Card>
            )}

            {/* ─── Pharmacy ───────────────────────────────────────────── */}
            <SectionToggle label="Top Drugs by Medicaid Spending" open={sections.pharmacy} onClick={() => toggle("pharmacy")} />
            {sections.pharmacy && (
              <Card>
                <CH title="Top Drugs" sub="By total Medicaid reimbursement" />
                {d.topDrugs.length > 0 ? (
                  <div style={{ overflowX: "auto" }}>
                    <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 10, fontFamily: FB }}>
                      <thead>
                        <tr style={{ borderBottom: `2px solid ${BD}` }}>
                          {["Drug", "Total Spending", "Rx Count", "Avg NADAC"].map(h => (
                            <th key={h} style={{ padding: "6px 8px", textAlign: "left", fontWeight: 600, color: A, fontSize: 9 }}>{h}</th>
                          ))}
                        </tr>
                      </thead>
                      <tbody>
                        {d.topDrugs.slice(0, 15).map((drug: any, i: number) => (
                          <tr key={i} style={{ background: i % 2 === 0 ? WH : SF, borderBottom: `1px solid ${BD}` }}>
                            <td style={{ padding: "5px 8px" }}>{drug.product_name || drug.ndc_description || "\u2014"}</td>
                            <td style={{ padding: "5px 8px", fontFamily: FM, fontWeight: 600, color: ACC }}>{fmtDollars(drug.total_spending || drug.total_amount_reimbursed || 0)}</td>
                            <td style={{ padding: "5px 8px", fontFamily: FM }}>{fmtNum(drug.total_prescriptions || drug.total_rx || 0)}</td>
                            <td style={{ padding: "5px 8px", fontFamily: FM, color: AL }}>{drug.avg_nadac ? `$${drug.avg_nadac.toFixed(2)}` : "\u2014"}</td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                ) : (
                  <div style={{ textAlign: "center", padding: 20, fontSize: 12, color: AL }}>No drug spending data available.</div>
                )}
              </Card>
            )}

            {/* ─── Social Determinants ──────────────────────────────── */}
            <SectionToggle label="Social Determinants of Health" open={sections.sdoh} onClick={() => toggle("sdoh")} />
            {sections.sdoh && d.sdoh && (
              <Card>
                <CH title="Social Determinants" sub="ADI, food access, shortage areas, underserved designations" />
                <div style={{ display: "grid", gridTemplateColumns: isMobile ? "1fr" : "repeat(auto-fit, minmax(180px, 1fr))", gap: 12, padding: "8px 0" }}>
                  {/* ADI */}
                  {d.sdoh.adi?.avg_national_rank != null && (
                    <div style={{ background: SF, borderRadius: 8, padding: "14px 16px" }}>
                      <div style={{ fontSize: 10, color: AL, marginBottom: 4 }}>Area Deprivation Index</div>
                      <div style={{ fontSize: 20, fontWeight: 700, color: d.sdoh.adi.avg_national_rank > 60 ? NEG : d.sdoh.adi.avg_national_rank > 40 ? WARN : POS, fontFamily: FM }}>
                        {d.sdoh.adi.avg_national_rank}
                      </div>
                      <div style={{ fontSize: 9, color: AL, marginTop: 2 }}>
                        avg national rank (1-100, higher = more deprived)
                      </div>
                      <div style={{ fontSize: 9, color: AL }}>
                        {d.sdoh.adi.block_group_count?.toLocaleString()} block groups
                      </div>
                    </div>
                  )}

                  {/* Food Access */}
                  {d.sdoh.food_access?.total_tracts > 0 && (
                    <div style={{ background: SF, borderRadius: 8, padding: "14px 16px" }}>
                      <div style={{ fontSize: 10, color: AL, marginBottom: 4 }}>Food Desert Tracts</div>
                      <div style={{ fontSize: 20, fontWeight: 700, color: d.sdoh.food_access.food_desert_tracts > 200 ? NEG : WARN, fontFamily: FM }}>
                        {d.sdoh.food_access.food_desert_tracts?.toLocaleString()}
                      </div>
                      <div style={{ fontSize: 9, color: AL, marginTop: 2 }}>
                        of {d.sdoh.food_access.total_tracts?.toLocaleString()} total tracts (LILA 1mi/10mi)
                      </div>
                      {d.sdoh.food_access.avg_poverty_rate != null && (
                        <div style={{ fontSize: 9, color: AL }}>
                          avg tract poverty rate: {d.sdoh.food_access.avg_poverty_rate}%
                        </div>
                      )}
                    </div>
                  )}

                  {/* Dental HPSA */}
                  {d.sdoh.dental_hpsa?.designated_count > 0 && (
                    <div style={{ background: SF, borderRadius: 8, padding: "14px 16px" }}>
                      <div style={{ fontSize: 10, color: AL, marginBottom: 4 }}>Dental Shortage Areas</div>
                      <div style={{ fontSize: 20, fontWeight: 700, color: d.sdoh.dental_hpsa.designated_count > 100 ? NEG : AL, fontFamily: FM }}>
                        {d.sdoh.dental_hpsa.designated_count}
                      </div>
                      <div style={{ fontSize: 9, color: AL, marginTop: 2 }}>designated dental HPSAs</div>
                      {d.sdoh.dental_hpsa.avg_score != null && (
                        <div style={{ fontSize: 9, color: AL }}>avg score: {d.sdoh.dental_hpsa.avg_score}</div>
                      )}
                    </div>
                  )}

                  {/* Mental Health HPSA */}
                  {d.sdoh.mental_health_hpsa?.designated_count > 0 && (
                    <div style={{ background: SF, borderRadius: 8, padding: "14px 16px" }}>
                      <div style={{ fontSize: 10, color: AL, marginBottom: 4 }}>Mental Health Shortage Areas</div>
                      <div style={{ fontSize: 20, fontWeight: 700, color: d.sdoh.mental_health_hpsa.designated_count > 100 ? NEG : AL, fontFamily: FM }}>
                        {d.sdoh.mental_health_hpsa.designated_count}
                      </div>
                      <div style={{ fontSize: 9, color: AL, marginTop: 2 }}>designated MH HPSAs</div>
                      {d.sdoh.mental_health_hpsa.avg_score != null && (
                        <div style={{ fontSize: 9, color: AL }}>avg score: {d.sdoh.mental_health_hpsa.avg_score}</div>
                      )}
                    </div>
                  )}

                  {/* MUA/MUP */}
                  {d.sdoh.mua_mup?.designated_count > 0 && (
                    <div style={{ background: SF, borderRadius: 8, padding: "14px 16px" }}>
                      <div style={{ fontSize: 10, color: AL, marginBottom: 4 }}>Medically Underserved Areas</div>
                      <div style={{ fontSize: 20, fontWeight: 700, color: d.sdoh.mua_mup.designated_count > 100 ? NEG : AL, fontFamily: FM }}>
                        {d.sdoh.mua_mup.designated_count}
                      </div>
                      <div style={{ fontSize: 9, color: AL, marginTop: 2 }}>designated MUA/MUP areas</div>
                      {d.sdoh.mua_mup.avg_imu_score != null && (
                        <div style={{ fontSize: 9, color: AL }}>avg IMU score: {d.sdoh.mua_mup.avg_imu_score}</div>
                      )}
                    </div>
                  )}
                </div>
              </Card>
            )}

            {/* ─── Economic Context ───────────────────────────────────── */}
            <SectionToggle label="Economic Context" open={sections.economic} onClick={() => toggle("economic")} />
            {sections.economic && d.economic.length > 0 && (
              <Card>
                <CH title="Economic Indicators" sub="BLS unemployment, BEA GDP, Census income" />
                {(() => {
                  const byIndicator: Record<string, any[]> = {};
                  d.economic.forEach((r: any) => {
                    const key = r.indicator_name || r.indicator || "unknown";
                    if (!byIndicator[key]) byIndicator[key] = [];
                    byIndicator[key].push(r);
                  });
                  const indicators = Object.entries(byIndicator);

                  return (
                    <div style={{ display: "flex", gap: 20, flexWrap: "wrap", justifyContent: "space-around", padding: "8px 0" }}>
                      {indicators.slice(0, 6).map(([name, rows]) => {
                        const latest = rows[rows.length - 1];
                        const value = latest.value || latest.rate || 0;
                        const isRate = name.toLowerCase().includes("unemployment") || name.toLowerCase().includes("rate");
                        return (
                          <Met key={name} label={name.replace(/_/g, " ")}
                            value={isRate ? `${value.toFixed(1)}%` : (value > 1000 ? fmtDollars(value) : value.toFixed(1))}
                            mono small />
                        );
                      })}
                    </div>
                  );
                })()}

                {/* Unemployment trend chart if available */}
                {(() => {
                  const unemploymentData = d.economic.filter((r: any) =>
                    (r.indicator_name || r.indicator || "").toLowerCase().includes("unemployment")
                  ).slice(-24);
                  if (unemploymentData.length < 3) return null;
                  return (
                    <div style={{ marginTop: 12 }}>
                      <div style={{ fontSize: 11, fontWeight: 600, color: A, marginBottom: 4 }}>Unemployment Rate (24 months)</div>
                      <ResponsiveContainer width="100%" height={160}>
                        <ComposedChart data={unemploymentData} margin={{ left: 5, right: 10, top: 5, bottom: 5 }}>
                          <CartesianGrid strokeDasharray="3 3" stroke={BD} vertical={false} />
                          <XAxis dataKey="period_date" tick={{ fontSize: 9, fill: AL, fontFamily: FM }} interval="preserveStartEnd"
                            tickFormatter={(v: string) => v ? v.slice(5, 7) + "/" + v.slice(2, 4) : ""} />
                          <YAxis tick={{ fontSize: 9, fill: AL, fontFamily: FM }} domain={["auto", "auto"]} width={35}
                            tickFormatter={(v: number) => `${v}%`} />
                          <Line type="monotone" dataKey="value" stroke={ACC} strokeWidth={2} dot={false} />
                          <Tooltip contentStyle={{ fontSize: 10, fontFamily: FM, borderRadius: 6 }}
                            formatter={(v: number) => `${v.toFixed(1)}%`} />
                        </ComposedChart>
                      </ResponsiveContainer>
                    </div>
                  );
                })()}
              </Card>
            )}

            {/* ─── Footer / Links ─────────────────────────────────────── */}
            <div style={{ display: "flex", gap: 8, flexWrap: "wrap", padding: "8px 0" }}>
              <a href={`/#/cpra`} style={{ fontSize: 11, color: cB, textDecoration: "none", padding: "6px 12px", borderRadius: 6, border: `1px solid ${BD}`, background: WH }}>
                CPRA Generator
              </a>
              <a href={`/#/wages`} style={{ fontSize: 11, color: cB, textDecoration: "none", padding: "6px 12px", borderRadius: 6, border: `1px solid ${BD}`, background: WH }}>
                Wage Comparison
              </a>
              <a href={`/#/forecast`} style={{ fontSize: 11, color: cB, textDecoration: "none", padding: "6px 12px", borderRadius: 6, border: `1px solid ${BD}`, background: WH }}>
                Caseload Forecaster
              </a>
              <a href={`/#/ahead-readiness`} style={{ fontSize: 11, color: cB, textDecoration: "none", padding: "6px 12px", borderRadius: 6, border: `1px solid ${BD}`, background: WH }}>
                AHEAD Readiness
              </a>
            </div>
          </>
        );
      })()}
    </div>
  );
}
