import { useState, useCallback, useEffect, useRef } from "react";
import {
  BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer,
  ScatterChart, Scatter, Cell, ReferenceLine,
} from "recharts";
import { C, FONT, SHADOW } from "../design";
import { useAradune } from "../context/AraduneContext";
import {
  scoreFinancialStability, scoreRevenueConcentration,
  scoreSupplementalExposure, scoreVolumeStability,
  compositeScore, scoreColor, scoreLabel, scoreSelfReport,
  operatingMarginPct, supplementalPctOfRevenue, govPayerPct,
  type HospitalMetrics, type PeerBenchmarks, type DimensionResult, type SelfReportAnswers,
} from "../utils/aheadScoring";
import { createAradunePDF, addSection, addFooter, loadJsPDF } from "../utils/pdfReport";
import { API_BASE } from "../lib/api";

// ── Constants ───────────────────────────────────────────────────────────
const BD = C.border, WH = C.white, SH = SHADOW, AL = C.inkLight;
const API = API_BASE;

// ── Formatting helpers ──────────────────────────────────────────────────
const fmtD = (n: number | null | undefined): string => {
  if (n == null) return "—";
  const a = Math.abs(n);
  return (n < 0 ? "-" : "") + (a >= 1e9 ? "$" + (a / 1e9).toFixed(2) + "B" : a >= 1e6 ? "$" + (a / 1e6).toFixed(1) + "M" : a >= 1e3 ? "$" + (a / 1e3).toFixed(0) + "K" : "$" + a.toFixed(0));
};

// ── Types ───────────────────────────────────────────────────────────────
interface HospitalData {
  provider_ccn: string; hospital_name: string; state_code: string;
  city: string; county: string; rural_urban: string;
  facility_type: string; bed_count: number | null;
  report_year: number;
  [key: string]: unknown;
}

// ── Card component ──────────────────────────────────────────────────────
function Card({ children, style }: { children: React.ReactNode; style?: React.CSSProperties }) {
  return (
    <div style={{ background: WH, borderRadius: 10, border: `1px solid ${BD}`, boxShadow: SH, padding: "20px 24px", ...style }}>
      {children}
    </div>
  );
}

function MetricCard({ label, value, points, maxPoints, interpretation, source }: {
  label: string; value: string | number | null; points: number; maxPoints: number;
  interpretation: string; source: string;
}) {
  const pctFill = maxPoints > 0 ? (points / maxPoints) * 100 : 0;
  return (
    <div style={{ padding: "14px 16px", background: C.surface, borderRadius: 8, marginBottom: 8 }}>
      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 6 }}>
        <span style={{ fontSize: 12, fontWeight: 600, color: C.ink }}>{label}</span>
        <span style={{ fontSize: 18, fontWeight: 700, fontFamily: FONT.mono, color: value != null ? C.ink : AL }}>
          {value ?? "Not found in public data"}
        </span>
      </div>
      {maxPoints > 0 && (
        <div style={{ display: "flex", alignItems: "center", gap: 8, marginBottom: 6 }}>
          <div style={{ flex: 1, height: 4, background: BD, borderRadius: 2, overflow: "hidden" }}>
            <div style={{ width: `${pctFill}%`, height: "100%", background: pctFill >= 60 ? C.pos : pctFill >= 30 ? C.warn : C.neg, borderRadius: 2 }} />
          </div>
          <span style={{ fontSize: 10, fontWeight: 600, fontFamily: FONT.mono, color: AL, whiteSpace: "nowrap" }}>{points}/{maxPoints} pts</span>
        </div>
      )}
      {interpretation && <div style={{ fontSize: 11, color: AL, lineHeight: 1.5, marginBottom: 4 }}>{interpretation}</div>}
      <div style={{ fontSize: 9, color: C.border, fontFamily: FONT.mono }}>{source}</div>
    </div>
  );
}

// ── Designation badges ──────────────────────────────────────────────────
function DesignationBadges({ rural, facilityType }: { rural: string | null; facilityType: string | null }) {
  const badges: string[] = [];
  if (rural?.toLowerCase().includes("rural")) badges.push("Rural");
  if (facilityType?.includes("CAH") || facilityType === "14") badges.push("CAH");
  if (facilityType?.includes("SCH")) badges.push("SCH");
  if (facilityType?.includes("REH")) badges.push("REH");
  if (!badges.length && rural?.toLowerCase().includes("urban")) badges.push("Urban");
  if (!badges.length) badges.push("PPS");
  return (
    <div style={{ display: "flex", gap: 6, flexWrap: "wrap" }}>
      {badges.map(b => (
        <span key={b} style={{
          fontSize: 10, fontWeight: 600, padding: "2px 8px", borderRadius: 4,
          background: b === "CAH" ? "#EDE9FE" : b === "Rural" ? "#FEF3C7" : C.surface,
          color: b === "CAH" ? "#5B21B6" : b === "Rural" ? "#92400E" : C.ink,
          border: `1px solid ${b === "CAH" ? "#C4B5FD" : b === "Rural" ? "#FCD34D" : BD}`,
        }}>{b}</span>
      ))}
    </div>
  );
}

// ── Dimension Section (collapsible) ─────────────────────────────────────
function DimensionSection({ title, result, color, defaultOpen, extraContent }: {
  title: string; result: DimensionResult; color: string; defaultOpen?: boolean;
  extraContent?: React.ReactNode;
}) {
  const [open, setOpen] = useState(defaultOpen ?? false);
  return (
    <Card style={{ marginBottom: 12 }}>
      <div
        onClick={() => setOpen(!open)}
        style={{ display: "flex", justifyContent: "space-between", alignItems: "center", cursor: "pointer" }}
      >
        <div style={{ display: "flex", alignItems: "center", gap: 10 }}>
          <div style={{ width: 4, height: 28, borderRadius: 2, background: color }} />
          <span style={{ fontSize: 14, fontWeight: 700, color: C.ink }}>{title}</span>
        </div>
        <div style={{ display: "flex", alignItems: "center", gap: 10 }}>
          <span style={{ fontSize: 22, fontWeight: 800, fontFamily: FONT.mono, color }}>{result.score}</span>
          <span style={{ fontSize: 11, color: AL }}>/ {result.maxScore}</span>
          <span style={{ fontSize: 14, color: AL, transition: "transform 0.2s", transform: open ? "rotate(180deg)" : "none" }}>▼</span>
        </div>
      </div>
      {open && (
        <div style={{ marginTop: 16 }}>
          {result.details.map((d, i) => (
            <MetricCard key={i} {...d} />
          ))}
          {extraContent}
        </div>
      )}
    </Card>
  );
}

// ── Self-Report Panel ───────────────────────────────────────────────────
function SelfReportPanel({ answers, onChange }: {
  answers: SelfReportAnswers;
  onChange: (a: SelfReportAnswers) => void;
}) {
  const [open, setOpen] = useState(false);
  const bonus = scoreSelfReport(answers);

  const radio = (label: string, key: keyof SelfReportAnswers, options: { label: string; value: string | boolean }[]) => (
    <div style={{ marginBottom: 12 }}>
      <div style={{ fontSize: 12, fontWeight: 600, color: C.ink, marginBottom: 6 }}>{label}</div>
      <div style={{ display: "flex", gap: 8, flexWrap: "wrap" }}>
        {options.map(o => {
          const selected = answers[key] === o.value;
          return (
            <button key={String(o.value)} onClick={() => onChange({ ...answers, [key]: o.value })} style={{
              padding: "4px 12px", fontSize: 11, borderRadius: 4, cursor: "pointer",
              border: `1px solid ${selected ? C.brand : BD}`,
              background: selected ? C.brand : WH, color: selected ? WH : C.ink,
              fontWeight: selected ? 600 : 400, fontFamily: FONT.body,
            }}>{o.label}</button>
          );
        })}
      </div>
    </div>
  );

  return (
    <Card style={{ marginBottom: 12 }}>
      <div onClick={() => setOpen(!open)} style={{ display: "flex", justifyContent: "space-between", alignItems: "center", cursor: "pointer" }}>
        <div>
          <div style={{ fontSize: 13, fontWeight: 700, color: C.ink }}>Refine Your Score</div>
          <div style={{ fontSize: 11, color: AL }}>Answer 8 questions to add up to +15 points with internal data Aradune can't see.</div>
        </div>
        <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
          {bonus > 0 && <span style={{ fontSize: 13, fontWeight: 700, fontFamily: FONT.mono, color: C.pos }}>+{bonus}</span>}
          <span style={{ fontSize: 14, color: AL, transition: "transform 0.2s", transform: open ? "rotate(180deg)" : "none" }}>▼</span>
        </div>
      </div>
      {open && (
        <div style={{ marginTop: 16 }}>
          {radio("1. Has your hospital participated in any downside risk model in the last 3 years?", "downsideRisk", [{ label: "Yes", value: true }, { label: "No", value: false }])}
          {radio("2. Did your hospital achieve positive margins in both of the last 2 fiscal years?", "positiveMargins", [{ label: "Yes", value: true }, { label: "No", value: false }])}
          {radio("3. Do you have a formal reinsurance or stop-loss policy?", "reinsurance", [{ label: "Yes", value: "yes" }, { label: "No", value: "no" }, { label: "In Progress", value: "in_progress" }])}
          {radio("4. Does your hospital have an internal cost accounting system by service line?", "costAccounting", [{ label: "Yes", value: "yes" }, { label: "No", value: "no" }, { label: "In Progress", value: "in_progress" }])}
          {radio("5. Can your team generate cost/utilization reports by payer and patient cohort?", "costReports", [{ label: "Yes", value: "yes" }, { label: "No", value: "no" }, { label: "Partial", value: "partial" }])}
          {radio("6. MSSP or other value-based program participation?", "mssp", [{ label: "Yes", value: true }, { label: "No", value: false }])}
          {radio("7. Legal and actuarial resources available?", "legalActuarial", [{ label: "Internal", value: "internal" }, { label: "Vendor", value: "vendor" }, { label: "None", value: "none" }])}
          {radio("8. Service line margin tracking system in place?", "serviceLineMargins", [{ label: "Yes", value: true }, { label: "No", value: false }])}
        </div>
      )}
    </Card>
  );
}

// ── Peer Comparison Panel ───────────────────────────────────────────────
function PeerPanel({ hospital, peers, d1, d2, d3, d4 }: {
  hospital: HospitalData;
  peers: { state_peers: PeerBenchmarks | null; national_peers: PeerBenchmarks | null } | null;
  d1: number; d2: number; d3: number; d4: number;
}) {
  if (!peers?.state_peers) return null;

  // Build scatter data from peer median + this hospital
  const hospOM = operatingMarginPct(hospital as unknown as HospitalMetrics);
  const hospSP = supplementalPctOfRevenue(hospital as unknown as HospitalMetrics);
  const peerOM = peers.state_peers.median_operating_margin;
  const peerSP = peers.state_peers.median_supplemental_pct;

  const scatterData: { name: string; x: number; y: number; highlight: boolean }[] = [];
  if (peerOM != null && peerSP != null) {
    scatterData.push({ name: "State Peer Median", x: peerSP, y: peerOM, highlight: false });
  }
  if (hospOM != null && hospSP != null) {
    scatterData.push({ name: hospital.hospital_name, x: hospSP, y: hospOM, highlight: true });
  }

  // Maryland AHEAD benchmarks (static, from MedPAC/MACPAC)
  const mdBenchmarks = { financial: 17, revenue: 16, supplemental: 14, volume: 18, composite: 65 };

  return (
    <Card style={{ marginBottom: 12 }}>
      <div style={{ fontSize: 14, fontWeight: 700, color: C.ink, marginBottom: 16 }}>Peer Comparison</div>

      {scatterData.length >= 2 && (
        <div style={{ marginBottom: 20 }}>
          <div style={{ fontSize: 12, fontWeight: 600, color: C.ink, marginBottom: 8 }}>Operating Margin vs. Supplemental Exposure</div>
          <ResponsiveContainer width="100%" height={240}>
            <ScatterChart margin={{ top: 10, right: 20, bottom: 20, left: 20 }}>
              <CartesianGrid strokeDasharray="3 3" stroke={BD} />
              <XAxis dataKey="x" type="number" name="Supplemental %" unit="%" tick={{ fontSize: 10, fill: AL }} label={{ value: "Supplemental % of Revenue", position: "bottom", fontSize: 10, fill: AL }} />
              <YAxis dataKey="y" type="number" name="Operating Margin %" unit="%" tick={{ fontSize: 10, fill: AL }} label={{ value: "Op. Margin %", angle: -90, position: "left", fontSize: 10, fill: AL }} />
              <Tooltip cursor={{ strokeDasharray: "3 3" }} formatter={(v: number) => `${v.toFixed(1)}%`} />
              <Scatter data={scatterData} dataKey="y">
                {scatterData.map((e, i) => (
                  <Cell key={i} fill={e.highlight ? C.brand : BD} r={e.highlight ? 8 : 5} stroke={e.highlight ? C.ink : AL} strokeWidth={e.highlight ? 2 : 1} />
                ))}
              </Scatter>
              {peerOM != null && <ReferenceLine y={peerOM} stroke={AL} strokeDasharray="3 3" />}
            </ScatterChart>
          </ResponsiveContainer>
          <div style={{ display: "flex", gap: 16, justifyContent: "center", fontSize: 10, color: AL }}>
            <span><span style={{ display: "inline-block", width: 8, height: 8, borderRadius: "50%", background: C.brand, marginRight: 4 }} />Your Hospital</span>
            <span><span style={{ display: "inline-block", width: 8, height: 8, borderRadius: "50%", background: BD, marginRight: 4 }} />State Peer Median</span>
          </div>
        </div>
      )}

      <div style={{ fontSize: 12, fontWeight: 600, color: C.ink, marginBottom: 8 }}>Dimension Score Comparison</div>
      <div style={{ fontSize: 10, color: AL, marginBottom: 8 }}>* AHEAD benchmark based on published MedPAC/MACPAC reports for Maryland model hospitals. Indicative only.</div>
      <table style={{ width: "100%", fontSize: 11, borderCollapse: "collapse" }}>
        <thead>
          <tr style={{ borderBottom: `2px solid ${BD}` }}>
            <th style={{ textAlign: "left", padding: "6px 8px", color: AL, fontWeight: 600 }}>Dimension</th>
            <th style={{ textAlign: "right", padding: "6px 8px", color: AL, fontWeight: 600 }}>Your Score</th>
            <th style={{ textAlign: "right", padding: "6px 8px", color: AL, fontWeight: 600 }}>AHEAD Benchmark*</th>
          </tr>
        </thead>
        <tbody>
          {[
            { name: "Financial Stability", yours: d1, bench: mdBenchmarks.financial },
            { name: "Revenue Concentration", yours: d2, bench: mdBenchmarks.revenue },
            { name: "Supplemental Exposure", yours: d3, bench: mdBenchmarks.supplemental },
            { name: "Volume Stability", yours: d4, bench: mdBenchmarks.volume },
          ].map(r => (
            <tr key={r.name} style={{ borderBottom: `1px solid ${BD}` }}>
              <td style={{ padding: "6px 8px", color: C.ink }}>{r.name}</td>
              <td style={{ padding: "6px 8px", textAlign: "right", fontFamily: FONT.mono, fontWeight: 700, color: r.yours >= r.bench ? C.pos : C.neg }}>{r.yours}/25</td>
              <td style={{ padding: "6px 8px", textAlign: "right", fontFamily: FONT.mono, color: AL }}>{r.bench}/25</td>
            </tr>
          ))}
        </tbody>
      </table>
    </Card>
  );
}

// ── Supplemental Cliff Chart ────────────────────────────────────────────
function SupplementalCliffChart({ m }: { m: HospitalMetrics }) {
  const npr = m.net_patient_revenue ?? 0;
  const supp = (m.dsh_adjustment ?? 0) + (m.ime_payment ?? 0);
  if (npr <= 0 || supp <= 0) return null;
  const base = npr - supp;
  const data = [
    { name: "Current Revenue", value: npr },
    { name: "Base (excl. supplemental)", value: base },
    { name: "Est. Global Budget", value: base * 0.98 },
  ];
  const delta = npr - base * 0.98;
  return (
    <div style={{ marginTop: 12 }}>
      <div style={{ fontSize: 12, fontWeight: 600, color: C.ink, marginBottom: 8 }}>Supplemental Payment Cliff Analysis</div>
      <ResponsiveContainer width="100%" height={140}>
        <BarChart data={data} layout="vertical" margin={{ left: 160, right: 20 }}>
          <CartesianGrid strokeDasharray="3 3" stroke={BD} horizontal={false} />
          <XAxis type="number" tickFormatter={v => fmtD(v)} tick={{ fontSize: 10, fill: AL }} />
          <YAxis dataKey="name" type="category" tick={{ fontSize: 10, fill: C.ink }} width={150} />
          <Tooltip formatter={(v: number) => fmtD(v)} />
          <Bar dataKey="value" radius={[0, 4, 4, 0]}>
            {data.map((_, i) => (
              <Cell key={i} fill={i === 0 ? C.brand : i === 1 ? C.warn : C.neg} />
            ))}
          </Bar>
        </BarChart>
      </ResponsiveContainer>
      <div style={{ padding: "10px 14px", background: "#FEF3C7", borderRadius: 8, fontSize: 11, color: "#92400E", lineHeight: 1.6, marginTop: 8 }}>
        If supplemental payments ({fmtD(supp)}) are restructured at transition, the estimated revenue impact is approximately <strong>{fmtD(delta)}</strong> ({((delta / npr) * 100).toFixed(1)}% of current revenue).
        Hospitals entering global budget models typically negotiate supplemental payment preservation in the first 2–3 years.
      </div>
    </div>
  );
}

// ── Revenue Mix Chart ───────────────────────────────────────────────────
function RevenueMixChart({ m }: { m: HospitalMetrics }) {
  const mcr = m.medicare_days ?? 0;
  const mcd = m.medicaid_days ?? 0;
  const tot = m.total_days ?? 0;
  if (tot <= 0) return null;
  const other = Math.max(0, tot - mcr - mcd);
  const data = [
    { name: "Medicare", value: mcr, pct: ((mcr / tot) * 100).toFixed(1) },
    { name: "Medicaid", value: mcd, pct: ((mcd / tot) * 100).toFixed(1) },
    { name: "Other", value: other, pct: ((other / tot) * 100).toFixed(1) },
  ];
  const colors = [C.brand, C.accent, BD];
  return (
    <div style={{ marginTop: 12 }}>
      <div style={{ fontSize: 12, fontWeight: 600, color: C.ink, marginBottom: 8 }}>Payer Mix (Patient Days)</div>
      <ResponsiveContainer width="100%" height={60}>
        <BarChart data={[{ medicare: mcr, medicaid: mcd, other }]} layout="horizontal" barSize={24}>
          <XAxis type="number" hide domain={[0, tot]} />
          <Bar dataKey="medicare" stackId="a" fill={colors[0]} />
          <Bar dataKey="medicaid" stackId="a" fill={colors[1]} />
          <Bar dataKey="other" stackId="a" fill={colors[2]} />
        </BarChart>
      </ResponsiveContainer>
      <div style={{ display: "flex", gap: 16, fontSize: 10, color: AL, marginTop: 4 }}>
        {data.map((d, i) => (
          <span key={d.name}><span style={{ display: "inline-block", width: 8, height: 8, borderRadius: 2, background: colors[i], marginRight: 4 }} />{d.name} {d.pct}%</span>
        ))}
      </div>
      <div style={{ padding: "10px 14px", background: C.surface, borderRadius: 8, fontSize: 11, color: AL, lineHeight: 1.6, marginTop: 8 }}>
        Under a global budget, your revenue is fixed at historical spending levels. High government payer concentration means limited ability to shift costs to commercial payers if the global budget is set below your cost base.
      </div>
    </div>
  );
}

// ── CSV Export ──────────────────────────────────────────────────────────
function exportCSV(hospital: HospitalData, composite: number, dims: { title: string; result: DimensionResult }[]) {
  const rows: (string | number)[][] = [
    ["Hospital", String(hospital.hospital_name)],
    ["CCN", String(hospital.provider_ccn)],
    ["State", String(hospital.state_code)],
    ["Composite Score", composite],
    ["Risk Level", scoreLabel(composite)],
    [],
  ];
  for (const dim of dims) {
    rows.push([dim.title, `${dim.result.score}/${dim.result.maxScore}`]);
    for (const d of dim.result.details) {
      rows.push(["", d.label, String(d.value ?? "N/A"), `${d.points}/${d.maxPoints}`, d.interpretation]);
    }
  }
  const csv = rows.map(r => r.map(c => {
    const s = String(c ?? "");
    return s.includes(",") || s.includes('"') || s.includes("\n") ? `"${s.replace(/"/g, '""')}"` : s;
  }).join(",")).join("\n");
  const a = document.createElement("a");
  const url = URL.createObjectURL(new Blob([csv], { type: "text/csv" }));
  a.href = url;
  a.download = `AHEAD_Readiness_${hospital.provider_ccn}.csv`;
  a.click();
  setTimeout(() => URL.revokeObjectURL(url), 1000);
}

// ── PDF Export ───────────────────────────────────────────────────────────
async function exportPDF(hospital: HospitalData, composite: number, dims: { title: string; result: DimensionResult }[]) {
  const doc = await createAradunePDF(`AHEAD Readiness Score — ${hospital.hospital_name}`);
  let y = 90;

  // Composite score
  doc.setFontSize(36);
  doc.setTextColor(...(composite >= 70 ? [5, 150, 105] as [number, number, number] : composite >= 40 ? [217, 119, 6] as [number, number, number] : [217, 48, 37] as [number, number, number]));
  doc.text(String(composite), 28, y);
  doc.setFontSize(12);
  doc.setTextColor(66, 90, 112);
  doc.text(`/ 100 — ${scoreLabel(composite)}`, 80, y);
  y += 10;

  doc.setFontSize(9);
  doc.text(`${hospital.city}, ${hospital.state_code} | CCN: ${hospital.provider_ccn} | ${hospital.bed_count ?? "?"} beds | HCRIS FY${hospital.report_year}`, 28, y);
  y += 20;

  for (const dim of dims) {
    if (y > 680) { doc.addPage(); y = 40; }
    y = addSection(doc, `${dim.title} — ${dim.result.score}/${dim.result.maxScore}`, y);
    for (const d of dim.result.details) {
      if (y > 720) { doc.addPage(); y = 40; }
      doc.setFont("helvetica", "bold");
      doc.setFontSize(9);
      doc.setTextColor(10, 37, 64);
      doc.text(`${d.label}: ${d.value ?? "N/A"}`, 38, y);
      if (d.maxPoints > 0) {
        doc.setFont("helvetica", "normal");
        doc.text(`(${d.points}/${d.maxPoints} pts)`, 350, y);
      }
      y += 12;
      if (d.interpretation) {
        doc.setFont("helvetica", "normal");
        doc.setFontSize(8);
        doc.setTextColor(66, 90, 112);
        const lines = doc.splitTextToSize(d.interpretation, 520);
        doc.text(lines, 38, y);
        y += lines.length * 10 + 4;
      }
    }
    y += 8;
  }

  addFooter(doc, [
    "All data sourced from publicly available CMS data (HCRIS, DSH allotment reports, Care Compare).",
    "Aradune makes no representation as to the accuracy of pre-populated values.",
    "Verify against your own financial records before use in any regulatory filing.",
  ]);

  doc.save(`AHEAD_Readiness_${hospital.provider_ccn}_${new Date().toISOString().slice(0, 10)}.pdf`);
}

// ── Excel Export ─────────────────────────────────────────────────────────
async function exportExcel(hospital: HospitalData, composite: number, dims: { title: string; result: DimensionResult }[]) {
  const XLSX = await import("xlsx");
  const wb = XLSX.utils.book_new();

  // Summary sheet
  const summaryData = [
    ["AHEAD Readiness Score", hospital.hospital_name],
    ["CCN", hospital.provider_ccn],
    ["State", hospital.state_code],
    ["Composite Score", composite],
    ["Risk Level", scoreLabel(composite)],
    ["Report Date", new Date().toISOString().slice(0, 10)],
    ["Data Source", `HCRIS FY${hospital.report_year}`],
    [],
    ...dims.map(d => [d.title, `${d.result.score}/${d.result.maxScore}`]),
  ];
  XLSX.utils.book_append_sheet(wb, XLSX.utils.aoa_to_sheet(summaryData), "Summary");

  // One sheet per dimension
  for (const dim of dims) {
    const rows = [["Metric", "Value", "Points", "Max Points", "Interpretation", "Source"]];
    for (const d of dim.result.details) {
      rows.push([d.label, String(d.value ?? "N/A"), String(d.points), String(d.maxPoints), d.interpretation, d.source]);
    }
    XLSX.utils.book_append_sheet(wb, XLSX.utils.aoa_to_sheet(rows), dim.title.slice(0, 31));
  }

  XLSX.writeFile(wb, `AHEAD_Readiness_${hospital.provider_ccn}_${new Date().toISOString().slice(0, 10)}.xlsx`);
}

// ═════════════════════════════════════════════════════════════════════════
// MAIN COMPONENT
// ═════════════════════════════════════════════════════════════════════════

export default function AheadReadiness() {
  const { openIntelligence } = useAradune();
  const [ccn, setCcn] = useState("");
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [hospital, setHospital] = useState<HospitalData | null>(null);
  const [peers, setPeers] = useState<{ state_peers: PeerBenchmarks | null; national_peers: PeerBenchmarks | null } | null>(null);
  const [selfReport, setSelfReport] = useState<SelfReportAnswers>({
    downsideRisk: null, positiveMargins: null, reinsurance: null,
    costAccounting: null, costReports: null, mssp: null,
    legalActuarial: null, serviceLineMargins: null,
  });

  const fetchHospital = useCallback(async () => {
    if (!ccn.trim()) return;
    setLoading(true);
    setError(null);
    setHospital(null);
    setPeers(null);

    try {
      const [hospRes, peerRes] = await Promise.allSettled([
        fetch(`${API}/api/hospitals/ccn/${ccn.trim()}`),
        fetch(`${API}/api/hospitals/ccn/${ccn.trim()}/peers`),
      ]);

      if (hospRes.status === "fulfilled" && hospRes.value.ok) {
        const data = await hospRes.value.json();
        if (Array.isArray(data) && data.length > 0) {
          setHospital(data[0] as HospitalData);
        } else {
          setError("CCN not found in HCRIS data. You can still explore with manual entry.");
        }
      } else {
        setError("CCN not found in HCRIS data. Check the number and try again.");
      }

      if (peerRes.status === "fulfilled" && peerRes.value.ok) {
        setPeers(await peerRes.value.json());
      }
    } catch {
      setError("Unable to reach Aradune API. Please try again.");
    } finally {
      setLoading(false);
    }
  }, [ccn]);

  // ── Compute scores ──────────────────────────────────────────────────
  const metrics: HospitalMetrics | null = hospital ? {
    net_patient_revenue: hospital.net_patient_revenue as number | null,
    net_income: hospital.net_income as number | null,
    total_income: hospital.total_income as number | null,
    total_costs: hospital.total_costs as number | null,
    total_assets: hospital.total_assets as number | null,
    total_liabilities: hospital.total_liabilities as number | null,
    total_salaries: hospital.total_salaries as number | null,
    inpatient_revenue: hospital.inpatient_revenue as number | null,
    outpatient_revenue: hospital.outpatient_revenue as number | null,
    medicare_days: hospital.medicare_days as number | null,
    medicaid_days: hospital.medicaid_days as number | null,
    total_days: hospital.total_days as number | null,
    total_discharges: hospital.total_discharges as number | null,
    uncompensated_care_cost: hospital.uncompensated_care_cost as number | null,
    charity_care_cost: hospital.charity_care_cost as number | null,
    bad_debt_expense: hospital.bad_debt_expense as number | null,
    dsh_adjustment: hospital.dsh_adjustment as number | null,
    ime_payment: hospital.ime_payment as number | null,
    cost_to_charge_ratio: hospital.cost_to_charge_ratio as number | null,
    bed_count: hospital.bed_count as number | null,
    medicaid_day_pct: hospital.medicaid_day_pct as number | null,
  } : null;

  const peerBench: PeerBenchmarks | undefined = peers?.state_peers ?? undefined;
  const d1 = metrics ? scoreFinancialStability(metrics, peerBench) : null;
  const d2 = metrics ? scoreRevenueConcentration(metrics, peerBench) : null;
  const d3 = metrics ? scoreSupplementalExposure(metrics, peerBench) : null;
  const d4 = metrics ? scoreVolumeStability(metrics, peerBench) : null;

  const selfBonus = scoreSelfReport(selfReport);
  const comp = d1 && d2 && d3 && d4 ? compositeScore(d1.score, d2.score, d3.score, d4.score, selfBonus) : null;
  const compBase = d1 && d2 && d3 && d4 ? compositeScore(d1.score, d2.score, d3.score, d4.score, 0) : null;

  const dims = d1 && d2 && d3 && d4 ? [
    { title: "Financial Stability", result: d1, color: C.brand },
    { title: "Revenue Concentration", result: d2, color: C.accent },
    { title: "Supplemental Exposure", result: d3, color: "#5B21B6" },
    { title: "Volume Stability", result: d4, color: C.teal },
  ] : [];

  // ── Hospital name search ──────────────────────────────────────────
  const [searchQ, setSearchQ] = useState("");
  const [searchResults, setSearchResults] = useState<{ ccn: string; name: string; city: string; state: string; beds: number | null }[]>([]);
  const [searching, setSearching] = useState(false);
  const [showResults, setShowResults] = useState(false);
  const searchTimer = useRef<ReturnType<typeof setTimeout> | null>(null);
  const searchRef = useRef<HTMLDivElement>(null);

  // Debounced search
  useEffect(() => {
    if (searchQ.length < 2) { setSearchResults([]); return; }
    if (searchTimer.current) clearTimeout(searchTimer.current);
    searchTimer.current = setTimeout(async () => {
      setSearching(true);
      try {
        const res = await fetch(`${API}/api/hospitals/search?q=${encodeURIComponent(searchQ)}&limit=15`);
        if (res.ok) { setSearchResults(await res.json()); setShowResults(true); }
      } catch { /* silent */ }
      setSearching(false);
    }, 250);
    return () => { if (searchTimer.current) clearTimeout(searchTimer.current); };
  }, [searchQ]);

  // Close dropdown on outside click
  useEffect(() => {
    const handler = (e: MouseEvent) => {
      if (searchRef.current && !searchRef.current.contains(e.target as Node)) setShowResults(false);
    };
    document.addEventListener("mousedown", handler);
    return () => document.removeEventListener("mousedown", handler);
  }, []);

  // ── Entry screen ──────────────────────────────────────────────────
  if (!hospital) {
    return (
      <div style={{ maxWidth: 560, margin: "0 auto", padding: "80px 20px", fontFamily: FONT.body }}>
        <div style={{ textAlign: "center", marginBottom: 32 }}>
          <div style={{ fontSize: 22, fontWeight: 800, color: C.ink, letterSpacing: -0.5, marginBottom: 8 }}>AHEAD Readiness Score</div>
          <div style={{ fontSize: 13, color: AL, lineHeight: 1.6, marginBottom: 12 }}>
            Search for your hospital by name, city, or CCN. Aradune scores readiness from public CMS data.
          </div>
          <button onClick={() => openIntelligence({ summary: "User is viewing AHEAD Readiness scoring tool" })} style={{
            padding: "8px 14px", borderRadius: 8, border: "none",
            background: C.brand, color: "#fff", fontSize: 12, cursor: "pointer", fontWeight: 600,
          }}>Ask Aradune</button>
        </div>

        {/* Hospital name search */}
        <div ref={searchRef} style={{ position: "relative", marginBottom: 16 }}>
          <input
            type="text"
            value={searchQ}
            onChange={e => { setSearchQ(e.target.value); setShowResults(true); }}
            onFocus={() => { if (searchResults.length) setShowResults(true); }}
            placeholder="Search hospital name, city, or CCN..."
            autoFocus
            style={{
              width: "100%", padding: "12px 14px", fontSize: 14, fontFamily: FONT.body,
              borderRadius: 8, border: `1px solid ${BD}`, outline: "none",
              boxSizing: "border-box",
            }}
          />
          {searching && (
            <div style={{ position: "absolute", right: 12, top: 14, fontSize: 11, color: AL }}>Searching...</div>
          )}
          {showResults && searchResults.length > 0 && (
            <div style={{
              position: "absolute", top: "100%", left: 0, right: 0, zIndex: 50,
              background: WH, border: `1px solid ${BD}`, borderRadius: 8,
              boxShadow: SHADOW, maxHeight: 320, overflowY: "auto", marginTop: 4,
            }}>
              {searchResults.map(r => (
                <div
                  key={r.ccn}
                  onClick={() => {
                    setCcn(r.ccn);
                    setSearchQ(r.name);
                    setShowResults(false);
                    // Auto-fetch
                    setTimeout(() => {
                      setLoading(true);
                      setError(null);
                      Promise.allSettled([
                        fetch(`${API}/api/hospitals/ccn/${r.ccn}`),
                        fetch(`${API}/api/hospitals/ccn/${r.ccn}/peers`),
                      ]).then(async ([hospRes, peerRes]) => {
                        if (hospRes.status === "fulfilled" && hospRes.value.ok) {
                          const data = await hospRes.value.json();
                          if (Array.isArray(data) && data.length > 0) setHospital(data[0] as HospitalData);
                          else setError("Hospital data not available in HCRIS.");
                        } else setError("Hospital not found in HCRIS data.");
                        if (peerRes.status === "fulfilled" && peerRes.value.ok) setPeers(await peerRes.value.json());
                      }).catch(() => setError("Unable to reach Aradune API."))
                        .finally(() => setLoading(false));
                    }, 0);
                  }}
                  style={{
                    padding: "10px 14px", cursor: "pointer", borderBottom: `1px solid ${C.surface}`,
                    transition: "background 0.1s",
                  }}
                  onMouseEnter={e => { e.currentTarget.style.background = C.surface; }}
                  onMouseLeave={e => { e.currentTarget.style.background = WH; }}
                >
                  <div style={{ fontSize: 13, fontWeight: 600, color: C.ink }}>{r.name}</div>
                  <div style={{ fontSize: 11, color: AL, marginTop: 2 }}>
                    {r.city}, {r.state} · CCN {r.ccn}{r.beds ? ` · ${r.beds} beds` : ""}
                  </div>
                </div>
              ))}
            </div>
          )}
          {showResults && searchQ.length >= 2 && !searching && searchResults.length === 0 && (
            <div style={{
              position: "absolute", top: "100%", left: 0, right: 0, zIndex: 50,
              background: WH, border: `1px solid ${BD}`, borderRadius: 8,
              boxShadow: SHADOW, padding: "16px", marginTop: 4, textAlign: "center",
            }}>
              <div style={{ fontSize: 12, color: AL }}>No hospitals found. Try a different search term.</div>
            </div>
          )}
        </div>

        {/* CCN direct entry fallback */}
        <div style={{ textAlign: "center", fontSize: 11, color: AL, marginBottom: 16 }}>
          Or enter CCN directly:
        </div>
        <form onSubmit={e => { e.preventDefault(); fetchHospital(); }} style={{ display: "flex", gap: 8, marginBottom: 12 }}>
          <input
            type="text"
            value={ccn}
            onChange={e => setCcn(e.target.value.replace(/\D/g, "").slice(0, 6))}
            placeholder="6-digit CCN"
            style={{
              flex: 1, padding: "10px 14px", fontSize: 15, fontFamily: FONT.mono,
              borderRadius: 8, border: `1px solid ${BD}`, outline: "none",
              letterSpacing: 1,
            }}
          />
          <button type="submit" disabled={ccn.length < 6 || loading} style={{
            padding: "10px 24px", fontSize: 13, fontWeight: 700,
            background: ccn.length >= 6 ? C.brand : BD,
            color: ccn.length >= 6 ? WH : AL,
            border: "none", borderRadius: 8, cursor: ccn.length >= 6 ? "pointer" : "default",
            fontFamily: FONT.body,
          }}>{loading ? "Loading..." : "Score"}</button>
        </form>

        {error && (
          <div style={{ marginTop: 16, padding: "10px 14px", background: "#FEF3C7", borderRadius: 8, border: "1px solid #FCD34D", fontSize: 12, color: "#92400E" }}>
            {error}
          </div>
        )}

        {loading && (
          <div style={{ marginTop: 24, textAlign: "center" }}>
            {[0, 1, 2, 3].map(i => (
              <div key={i} style={{ height: 20, background: C.surface, borderRadius: 6, marginBottom: 8, animation: "pulse 1.5s ease-in-out infinite", animationDelay: `${i * 0.15}s` }} />
            ))}
          </div>
        )}
      </div>
    );
  }

  // ── Dashboard ─────────────────────────────────────────────────────
  return (
    <div style={{ maxWidth: 900, margin: "0 auto", padding: "24px 20px 60px", fontFamily: FONT.body }}>
      {/* Header */}
      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "flex-start", flexWrap: "wrap", gap: 12, marginBottom: 20 }}>
        <div>
          <div style={{ fontSize: 20, fontWeight: 800, color: C.ink, letterSpacing: -0.3, marginBottom: 4 }}>{hospital.hospital_name}</div>
          <div style={{ fontSize: 12, color: AL, marginBottom: 6 }}>{hospital.city}, {hospital.state_code} · CCN {hospital.provider_ccn} · {hospital.bed_count ?? "?"} beds · HCRIS FY{hospital.report_year}</div>
          <DesignationBadges rural={hospital.rural_urban} facilityType={hospital.facility_type} />
        </div>
        <div style={{ display: "flex", gap: 8 }}>
          <button onClick={() => { setHospital(null); setCcn(""); }} style={{
            padding: "6px 14px", fontSize: 11, fontWeight: 600,
            background: WH, color: C.ink, border: `1px solid ${BD}`,
            borderRadius: 6, cursor: "pointer", fontFamily: FONT.body,
          }}>← New Search</button>
          <button onClick={() => exportCSV(hospital, comp ?? 0, dims)} style={{
            padding: "6px 14px", fontSize: 11, fontWeight: 600,
            background: WH, color: C.ink, border: `1px solid ${BD}`,
            borderRadius: 6, cursor: "pointer", fontFamily: FONT.body,
          }}>Export CSV</button>
          <button onClick={() => exportPDF(hospital, comp ?? 0, dims)} style={{
            padding: "6px 14px", fontSize: 11, fontWeight: 600,
            background: C.brand, color: WH, border: "none",
            borderRadius: 6, cursor: "pointer", fontFamily: FONT.body,
          }}>Export PDF</button>
          <button onClick={() => exportExcel(hospital, comp ?? 0, dims)} style={{
            padding: "6px 14px", fontSize: 11, fontWeight: 600,
            background: C.ink, color: WH, border: "none",
            borderRadius: 6, cursor: "pointer", fontFamily: FONT.body,
          }}>Export Excel</button>
        </div>
      </div>

      {/* Composite Score */}
      {comp != null && (
        <Card style={{ marginBottom: 20, textAlign: "center", padding: "28px 24px" }}>
          <div style={{ fontSize: 11, fontWeight: 600, color: AL, textTransform: "uppercase", letterSpacing: 1, marginBottom: 8 }}>AHEAD Readiness Score</div>
          <div style={{ fontSize: 64, fontWeight: 900, fontFamily: FONT.mono, color: scoreColor(comp), lineHeight: 1 }}>{comp}</div>
          <div style={{ fontSize: 14, fontWeight: 700, color: scoreColor(comp), marginTop: 4 }}>{scoreLabel(comp)}</div>
          {selfBonus > 0 && (
            <div style={{ fontSize: 11, color: AL, marginTop: 8 }}>
              Base score: {compBase} + {selfBonus} self-reported = {comp} (Refined Score)
            </div>
          )}
          <div style={{
            marginTop: 12, height: 8, borderRadius: 4, overflow: "hidden",
            background: `linear-gradient(to right, #D93025 0%, #D93025 39%, #D97706 40%, #D97706 69%, #059669 70%, #059669 100%)`,
          }}>
            <div style={{
              width: 3, height: 16, background: C.ink, borderRadius: 2,
              marginLeft: `${comp}%`, marginTop: -4, position: "relative",
            }} />
          </div>
          <div style={{ display: "flex", justifyContent: "space-between", fontSize: 9, color: AL, marginTop: 4 }}>
            <span>High Risk (0–39)</span><span>Moderate (40–69)</span><span>Low Risk (70–100)</span>
          </div>
        </Card>
      )}

      {/* 2×2 Dimension Score Grid */}
      {dims.length === 4 && (
        <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 12, marginBottom: 20 }}>
          {dims.map(d => (
            <Card key={d.title} style={{ textAlign: "center", padding: 16 }}>
              <div style={{ width: 4, height: 4, borderRadius: "50%", background: d.color, margin: "0 auto 6px" }} />
              <div style={{ fontSize: 28, fontWeight: 900, fontFamily: FONT.mono, color: d.color }}>{d.result.score}</div>
              <div style={{ fontSize: 10, color: AL }}>/ {d.result.maxScore}</div>
              <div style={{ fontSize: 11, fontWeight: 600, color: C.ink, marginTop: 4 }}>{d.title}</div>
            </Card>
          ))}
        </div>
      )}

      {/* Dimension Details (collapsible) */}
      {dims.map((d, i) => (
        <DimensionSection key={d.title} title={d.title} result={d.result} color={d.color} defaultOpen={i === 0} extraContent={
          d.title === "Revenue Concentration" && metrics ? <RevenueMixChart m={metrics} /> :
          d.title === "Supplemental Exposure" && metrics ? <SupplementalCliffChart m={metrics} /> :
          null
        } />
      ))}

      {/* Self-Report Unlock */}
      <SelfReportPanel answers={selfReport} onChange={setSelfReport} />

      {/* Peer Comparison */}
      {hospital && d1 && d2 && d3 && d4 && (
        <PeerPanel hospital={hospital} peers={peers} d1={d1.score} d2={d2.score} d3={d3.score} d4={d4.score} />
      )}

      {/* Disclaimer */}
      <div style={{ marginTop: 24, padding: "12px 16px", background: C.surface, borderRadius: 8, fontSize: 10, color: AL, lineHeight: 1.6 }}>
        All data sourced from publicly available CMS data (HCRIS, DSH allotment reports, Care Compare).
        Aradune makes no representation as to the accuracy of pre-populated values. Verify against your own
        financial records before use in any regulatory filing. This tool is not a substitute for actuarial review.
      </div>
    </div>
  );
}
