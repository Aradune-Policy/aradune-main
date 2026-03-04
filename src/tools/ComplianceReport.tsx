/**
 * Rate Transparency Compliance Package
 * Unified compliance report combining rate transparency, Medicare parity,
 * access rule analysis, and methodology documentation.
 * Targets 42 CFR §447.203 (CMS Ensuring Access Final Rule, July 2026 deadline).
 */
import { useState, useEffect, useMemo } from "react";
import { STATE_NAMES } from "../data/states";
import { query as duckQuery } from "../lib/duckdb";
import { getPreset } from "../lib/presets";

// ── Design tokens ───────────────────────────────────────────────────────
const A = "#0A2540", AL = "#425A70", POS = "#2E6B4A", NEG = "#A4262C", WARN = "#B8860B";
const SF = "#F5F7F5", BD = "#E4EAE4", WH = "#fff", cB = "#2E6B4A";
const FM = "'SF Mono',Menlo,Consolas,monospace";
const FB = "'Helvetica Neue',Arial,sans-serif";
const SH = "0 1px 3px rgba(0,0,0,.04),0 4px 12px rgba(0,0,0,.03)";

// ── Local UI primitives ─────────────────────────────────────────────────
const Card = ({ children, accent }: { children: React.ReactNode; accent?: string }) => (
  <div style={{ background: WH, borderRadius: 12, boxShadow: SH, padding: "20px 24px",
    borderTop: accent ? `3px solid ${accent}` : undefined, marginBottom: 20 }}>{children}</div>
);
const CH = ({ title, sub, right }: { title: string; sub?: string; right?: React.ReactNode }) => (
  <div style={{ display: "flex", justifyContent: "space-between", alignItems: "baseline", marginBottom: 14 }}>
    <div>
      <h3 style={{ margin: 0, fontSize: 15, fontWeight: 700, color: A, fontFamily: FB }}>{title}</h3>
      {sub && <p style={{ margin: "2px 0 0", fontSize: 12, color: AL }}>{sub}</p>}
    </div>
    {right}
  </div>
);
const Met = ({ label, value, color, sub }: { label: string; value: string; color?: string; sub?: string }) => (
  <div style={{ textAlign: "center", minWidth: 90 }}>
    <div style={{ fontSize: 22, fontWeight: 700, color: color || A, fontFamily: FM }}>{value}</div>
    <div style={{ fontSize: 11, color: AL, marginTop: 2 }}>{label}</div>
    {sub && <div style={{ fontSize: 10, color: AL, marginTop: 1 }}>{sub}</div>}
  </div>
);

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

const f$ = (n: number) =>
  n >= 1e9 ? `$${(n / 1e9).toFixed(1)}B`
  : n >= 1e6 ? `$${(n / 1e6).toFixed(1)}M`
  : n >= 1e3 ? `$${(n / 1e3).toFixed(0)}K`
  : `$${n.toFixed(2)}`;

const fPct = (n: number) => `${n.toFixed(1)}%`;

const STATES = Object.keys(STATE_NAMES).sort();

// ── Access Rule thresholds per 42 CFR §447.203 ─────────────────────────
const REVIEW_THRESHOLD = 4;   // % reduction triggering access review
const ANALYSIS_THRESHOLD = 6; // % reduction requiring independent analysis
const PARITY_WARN = 80;       // % of Medicare — access concern
const PARITY_CRITICAL = 50;   // % of Medicare — critical access risk

// ── Types ───────────────────────────────────────────────────────────────
interface MedicareEntry { r: number; fr?: number; rvu?: number; w?: number; d?: string }
interface MedicareData { rates: Record<string, MedicareEntry>; cf: number; year: number }
interface DirEntry { state: string; agency: string; url: string; format: string; access: string; methodology: string; verified: boolean }
interface SpendRow { hcpcs_code: string; total_paid: number; total_claims: number; total_bene: number }
interface StateSpending { state: string; total_spend: number; total_claims: number; total_bene: number; n_providers: number; fmap: number }

interface CodeAnalysis {
  hcpcs: string; desc: string;
  medicaidRate: number; medicareRate: number; pctMedicare: number;
  totalPaid: number; totalClaims: number;
  flag: "ok" | "warning" | "critical";
}

type CheckStatus = "pass" | "warn" | "fail" | "na";

interface CheckItem {
  label: string; status: CheckStatus; detail: string;
  regulation?: string;
}

// ═════════════════════════════════════════════════════════════════════════
export default function ComplianceReport() {
  const [st, setSt] = useState("FL");
  const [medicare, setMedicare] = useState<MedicareData | null>(null);
  const [directory, setDirectory] = useState<DirEntry[]>([]);
  const [statesData, setStatesData] = useState<StateSpending[]>([]);
  const [spendData, setSpendData] = useState<SpendRow[]>([]);
  const [loading, setLoading] = useState(true);
  const [duckLoading, setDuckLoading] = useState(false);
  const [reductionPct, setReductionPct] = useState(0);
  const [catFilter, setCatFilter] = useState("all");

  // Load static data
  useEffect(() => {
    Promise.all([
      fetch("/data/medicare_rates.json").then(r => r.json()),
      fetch("/data/fee_schedule_directory.json").then(r => r.json()),
      fetch("/data/states.json").then(r => r.json()),
    ]).then(([med, dir, states]) => {
      setMedicare(med as MedicareData);
      setDirectory((dir as { directory: DirEntry[] }).directory.filter((d: DirEntry) => d.agency));
      setStatesData(states as StateSpending[]);
      setLoading(false);
    });
  }, []);

  // Load DuckDB spending data for selected state
  useEffect(() => {
    if (loading) return;
    setDuckLoading(true);
    duckQuery(`
      SELECT hcpcs_code,
             SUM(total_paid) AS total_paid,
             SUM(total_claims) AS total_claims,
             SUM(total_beneficiaries) AS total_bene
      FROM 'claims.parquet'
      WHERE state = '${st}' AND year = 2023
      GROUP BY hcpcs_code
      ORDER BY total_paid DESC
      LIMIT 500
    `).then(res => {
      setSpendData(res.rows as unknown as SpendRow[]);
      setDuckLoading(false);
    }).catch(() => setDuckLoading(false));
  }, [st, loading]);

  // Map state name → abbreviation
  const nameToAbbr = useMemo(() => {
    const m = new Map<string, string>();
    for (const [abbr, name] of Object.entries(STATE_NAMES)) m.set(name, abbr);
    m.set("District of Columbia", "DC");
    return m;
  }, []);

  // State fee schedule info
  const stateDir = useMemo(() => {
    const fullName = STATE_NAMES[st] || st;
    return directory.find(d => d.state === fullName || nameToAbbr.get(d.state) === st);
  }, [directory, st, nameToAbbr]);

  // State spending summary
  const stateSummary = useMemo(() =>
    statesData.find(s => s.state === st),
  [statesData, st]);

  // Machine-readable check
  const isMachineReadable = useMemo(() => {
    if (!stateDir) return false;
    const fmt = stateDir.format.toLowerCase();
    return fmt.includes("excel") || fmt.includes("csv") || fmt.includes("xls");
  }, [stateDir]);

  // Code-level Medicare parity analysis
  const codeAnalysis = useMemo((): CodeAnalysis[] => {
    if (!medicare || !spendData.length) return [];

    let codes = spendData;
    if (catFilter !== "all") {
      const preset = getPreset(catFilter);
      if (preset?.codes?.length) {
        const set = new Set(preset.codes);
        codes = codes.filter(r => set.has(r.hcpcs_code));
      }
    }

    return codes.map(row => {
      const med = medicare.rates[row.hcpcs_code];
      const medicareRate = med?.r || 0;
      const medicaidRate = row.total_claims > 0 ? row.total_paid / row.total_claims : 0;
      const pctMedicare = medicareRate > 0 ? (medicaidRate / medicareRate) * 100 : 0;
      return {
        hcpcs: row.hcpcs_code,
        desc: med?.d || row.hcpcs_code,
        medicaidRate,
        medicareRate,
        pctMedicare,
        totalPaid: row.total_paid,
        totalClaims: row.total_claims,
        flag: pctMedicare > 0 && pctMedicare < PARITY_CRITICAL ? "critical" as const
          : pctMedicare > 0 && pctMedicare < PARITY_WARN ? "warning" as const
          : "ok" as const,
      };
    }).filter(c => c.medicareRate > 0);
  }, [medicare, spendData, catFilter]);

  // Reduction impact analysis
  const reductionAnalysis = useMemo(() => {
    if (reductionPct === 0) return null;
    const factor = 1 - reductionPct / 100;
    let totalImpact = 0;
    let codesAffected = 0;
    let belowWarnAfter = 0;
    let belowCritAfter = 0;

    const details = codeAnalysis.map(c => {
      const newRate = c.medicaidRate * factor;
      const newPctMed = c.medicareRate > 0 ? (newRate / c.medicareRate) * 100 : 0;
      const impact = c.totalPaid * (reductionPct / 100);
      totalImpact += impact;
      codesAffected++;
      if (newPctMed > 0 && newPctMed < PARITY_WARN) belowWarnAfter++;
      if (newPctMed > 0 && newPctMed < PARITY_CRITICAL) belowCritAfter++;
      return { ...c, newRate, newPctMed, impact };
    });

    return { details, totalImpact, codesAffected, belowWarnAfter, belowCritAfter };
  }, [codeAnalysis, reductionPct]);

  // Summary stats
  const summary = useMemo(() => {
    if (!codeAnalysis.length) return null;
    const withParity = codeAnalysis.filter(c => c.pctMedicare > 0);
    const sorted = [...withParity].sort((a, b) => a.pctMedicare - b.pctMedicare);
    const median = sorted.length > 0 ? sorted[Math.floor(sorted.length / 2)].pctMedicare : 0;
    const belowWarn = withParity.filter(c => c.pctMedicare < PARITY_WARN).length;
    const belowCrit = withParity.filter(c => c.pctMedicare < PARITY_CRITICAL).length;
    const totalSpend = codeAnalysis.reduce((s, c) => s + c.totalPaid, 0);
    return { median, belowWarn, belowCrit, total: withParity.length, totalSpend };
  }, [codeAnalysis]);

  // Compliance checklist
  const checklist = useMemo((): CheckItem[] => {
    const items: CheckItem[] = [
      {
        label: "FFS rates published in machine-readable format",
        status: isMachineReadable ? "pass" : "fail",
        detail: isMachineReadable
          ? `Published in ${stateDir?.format || "machine-readable format"}`
          : `Currently published as ${stateDir?.format || "unknown format"} — must convert to Excel or CSV`,
        regulation: "42 CFR §447.203(b)(1)",
      },
      {
        label: "Fee schedule publicly accessible",
        status: stateDir
          ? stateDir.access.toLowerCase().includes("public") ? "pass" : "warn"
          : "na",
        detail: stateDir?.access || "No fee schedule URL on file",
        regulation: "42 CFR §447.203(b)(1)",
      },
      {
        label: "Rate-setting methodology documented",
        status: stateDir?.methodology ? "pass" : "fail",
        detail: stateDir?.methodology || "No methodology documentation on file",
        regulation: "42 CFR §447.203(b)(2)",
      },
      {
        label: "Medicare parity analysis completed",
        status: summary
          ? summary.belowCrit > 0 ? "fail" : summary.belowWarn > 0 ? "warn" : "pass"
          : "na",
        detail: summary
          ? `Median: ${fPct(summary.median)} of Medicare. ${summary.belowCrit} codes below 50%, ${summary.belowWarn} codes below 80%.`
          : "Loading parity data...",
        regulation: "42 CFR §447.203(b)(3)",
      },
    ];

    if (reductionPct > 0 && reductionAnalysis) {
      items.push({
        label: `Rate reduction (${reductionPct}%) access review`,
        status: reductionPct >= ANALYSIS_THRESHOLD ? "fail" : reductionPct >= REVIEW_THRESHOLD ? "warn" : "pass",
        detail: reductionPct >= ANALYSIS_THRESHOLD
          ? `${reductionPct}% reduction requires independent access analysis`
          : reductionPct >= REVIEW_THRESHOLD
          ? `${reductionPct}% reduction triggers access review`
          : `${reductionPct}% reduction below review threshold`,
        regulation: reductionPct >= ANALYSIS_THRESHOLD
          ? "42 CFR §447.203(b)(6)"
          : "42 CFR §447.203(b)(5)",
      });
    }

    return items;
  }, [isMachineReadable, stateDir, summary, reductionPct, reductionAnalysis]);

  const statusIcon = (s: CheckStatus) =>
    s === "pass" ? { sym: "\u2713", color: POS }
    : s === "warn" ? { sym: "!", color: WARN }
    : s === "fail" ? { sym: "\u2717", color: NEG }
    : { sym: "—", color: AL };

  const categories = [
    { id: "all", label: "All Codes" },
    { id: "em", label: "E&M" },
    { id: "behavioral_health", label: "Behavioral" },
    { id: "hcbs_waiver", label: "HCBS" },
    { id: "dental", label: "Dental" },
    { id: "rehabilitation", label: "Rehab" },
    { id: "telehealth", label: "Telehealth" },
  ];

  // Full export
  const handleExport = () => {
    const rows: (string | number)[][] = [];

    // Section 1: Compliance Checklist
    rows.push(["=== COMPLIANCE CHECKLIST ===", "", "", ""]);
    rows.push(["Check", "Status", "Detail", "Regulation"]);
    checklist.forEach(c => rows.push([c.label, c.status.toUpperCase(), c.detail, c.regulation || ""]));
    rows.push([]);

    // Section 2: State Info
    rows.push(["=== STATE INFORMATION ===", "", "", ""]);
    rows.push(["State", STATE_NAMES[st] || st, "", ""]);
    rows.push(["Agency", stateDir?.agency || "—", "", ""]);
    rows.push(["Methodology", stateDir?.methodology || "—", "", ""]);
    rows.push(["Format", stateDir?.format || "—", "", ""]);
    rows.push(["Fee Schedule URL", stateDir?.url || "—", "", ""]);
    rows.push(["Total Medicaid Spend", stateSummary ? f$(stateSummary.total_spend) : "—", "", ""]);
    rows.push(["FMAP", stateSummary ? `${stateSummary.fmap}%` : "—", "", ""]);
    rows.push([]);

    // Section 3: Medicare Parity
    rows.push(["=== MEDICARE PARITY ANALYSIS ===", "", "", ""]);
    rows.push(["HCPCS", "Description", "Medicaid Rate", "Medicare Rate", "% of Medicare", "Annual Spend", "Flag"]);
    codeAnalysis.forEach(c => rows.push([
      c.hcpcs, c.desc, c.medicaidRate.toFixed(2), c.medicareRate.toFixed(2),
      c.pctMedicare.toFixed(1), c.totalPaid.toFixed(0), c.flag,
    ]));

    if (reductionPct > 0 && reductionAnalysis) {
      rows.push([]);
      rows.push([`=== RATE REDUCTION IMPACT (${reductionPct}%) ===`, "", "", ""]);
      rows.push(["HCPCS", "Description", "Current Rate", "New Rate", "New % Medicare", "Annual Impact", "Flag"]);
      reductionAnalysis.details.forEach(c => rows.push([
        c.hcpcs, c.desc, c.medicaidRate.toFixed(2), c.newRate.toFixed(2),
        c.newPctMed.toFixed(1), c.impact.toFixed(0), c.newPctMed < 50 ? "critical" : c.newPctMed < 80 ? "warning" : "ok",
      ]));
    }

    downloadCSV(
      ["Field 1", "Field 2", "Field 3", "Field 4", "Field 5", "Field 6", "Field 7"],
      rows,
      `compliance_report_${st}_${new Date().toISOString().split("T")[0]}.csv`,
    );
  };

  // ── Render ────────────────────────────────────────────────────────────
  return (
    <div style={{ maxWidth: 1100, margin: "0 auto", padding: "24px 16px", fontFamily: FB }}>
      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "baseline", flexWrap: "wrap", gap: 12 }}>
        <div>
          <h2 style={{ fontSize: 22, fontWeight: 800, color: A, margin: "0 0 4px" }}>Rate Transparency Compliance</h2>
          <p style={{ fontSize: 13, color: AL, margin: "0 0 20px" }}>
            42 CFR §447.203 — CMS Ensuring Access Final Rule — Deadline: July 1, 2026
          </p>
        </div>
        <div style={{ display: "flex", gap: 8, alignItems: "center" }}>
          <select value={st} onChange={e => setSt(e.target.value)} style={{
            padding: "6px 10px", borderRadius: 6, border: `1px solid ${BD}`,
            fontSize: 13, fontFamily: FM, background: WH,
          }}>
            {STATES.map(s => <option key={s} value={s}>{s} — {STATE_NAMES[s]}</option>)}
          </select>
          <button onClick={handleExport} style={{
            padding: "6px 14px", borderRadius: 6, background: WH, color: cB,
            border: `1px solid ${BD}`, fontSize: 12, fontWeight: 600, cursor: "pointer", fontFamily: FB,
          }}>Export CSV</button>
          <button onClick={async () => {
            const { generateCompliancePdf } = await import("../utils/compliancePdf");
            const withParity = codeAnalysis.filter(c => c.pctMedicare > 0);
            const sorted = [...withParity].sort((a, b) => a.pctMedicare - b.pctMedicare);
            const median = sorted.length > 0 ? sorted[Math.floor(sorted.length / 2)].pctMedicare : 0;
            generateCompliancePdf({
              state: st,
              stateName: STATE_NAMES[st] || st,
              agency: stateDir?.agency || "",
              methodology: stateDir?.methodology || "",
              format: stateDir?.format || "",
              feeScheduleUrl: stateDir?.url || "",
              totalSpend: stateSummary?.total_spend || 0,
              fmap: stateSummary?.fmap || 0,
              checklist,
              codeAnalysis,
              medianPctMedicare: median,
              belowWarn: withParity.filter(c => c.pctMedicare < 80).length,
              belowCrit: withParity.filter(c => c.pctMedicare < 50).length,
              reductionPct,
              reductionDetails: reductionAnalysis?.details || [],
              reductionTotalImpact: reductionAnalysis?.totalImpact || 0,
            });
          }} style={{
            padding: "6px 14px", borderRadius: 6, background: cB, color: WH,
            border: "none", fontSize: 12, fontWeight: 600, cursor: "pointer", fontFamily: FB,
          }}>Export PDF</button>
        </div>
      </div>

      {loading ? (
        <Card><p style={{ color: AL, fontSize: 13, textAlign: "center", padding: 40 }}>Loading compliance data...</p></Card>
      ) : (
        <>
          {/* Compliance Checklist */}
          <Card accent={checklist.some(c => c.status === "fail") ? NEG : checklist.some(c => c.status === "warn") ? WARN : POS}>
            <CH title="Compliance Checklist" sub={`${STATE_NAMES[st]} — ${checklist.filter(c => c.status === "pass").length} of ${checklist.length} items passing`} />
            <div style={{ display: "flex", flexDirection: "column", gap: 10 }}>
              {checklist.map((item, i) => {
                const icon = statusIcon(item.status);
                return (
                  <div key={i} style={{
                    display: "flex", gap: 12, padding: "10px 12px", borderRadius: 8,
                    background: item.status === "fail" ? "#FFF5F5" : item.status === "warn" ? "#FFFDF0" : SF,
                    alignItems: "flex-start",
                  }}>
                    <div style={{
                      width: 24, height: 24, borderRadius: "50%", display: "flex",
                      alignItems: "center", justifyContent: "center", flexShrink: 0,
                      background: `${icon.color}18`, color: icon.color, fontWeight: 800, fontSize: 14,
                    }}>{icon.sym}</div>
                    <div style={{ flex: 1 }}>
                      <div style={{ fontSize: 13, fontWeight: 600, color: A }}>{item.label}</div>
                      <div style={{ fontSize: 12, color: AL, marginTop: 2 }}>{item.detail}</div>
                      {item.regulation && (
                        <div style={{ fontSize: 10, color: AL, marginTop: 3, fontFamily: FM }}>{item.regulation}</div>
                      )}
                    </div>
                  </div>
                );
              })}
            </div>
          </Card>

          {/* State Overview */}
          <Card>
            <CH title="State Overview" sub={STATE_NAMES[st]} />
            <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 16, fontSize: 12 }}>
              <div>
                <div style={{ fontWeight: 700, color: A, marginBottom: 8 }}>Medicaid Agency</div>
                <div style={{ color: AL, marginBottom: 12 }}>{stateDir?.agency || "—"}</div>
                <div style={{ fontWeight: 700, color: A, marginBottom: 8 }}>Rate-Setting Methodology</div>
                <div style={{ color: AL, marginBottom: 12 }}>{stateDir?.methodology || "—"}</div>
                <div style={{ fontWeight: 700, color: A, marginBottom: 8 }}>Fee Schedule Format</div>
                <div style={{ color: AL }}>
                  {stateDir?.format || "—"}
                  {isMachineReadable
                    ? <span style={{ marginLeft: 8, color: POS, fontWeight: 600 }}>Machine-Readable</span>
                    : <span style={{ marginLeft: 8, color: NEG, fontWeight: 600 }}>Not Machine-Readable</span>}
                </div>
              </div>
              <div>
                <div style={{ display: "flex", flexWrap: "wrap", gap: 20, justifyContent: "center" }}>
                  <Met label="Total Spend" value={stateSummary ? f$(stateSummary.total_spend) : "—"} />
                  <Met label="Total Claims" value={stateSummary ? f$(stateSummary.total_claims).replace("$", "") : "—"} />
                  <Met label="Providers" value={stateSummary?.n_providers?.toLocaleString() || "—"} />
                  <Met label="FMAP" value={stateSummary ? `${stateSummary.fmap}%` : "—"} color={cB} />
                </div>
                {stateDir?.url && (
                  <div style={{ textAlign: "center", marginTop: 12 }}>
                    <a href={stateDir.url} target="_blank" rel="noopener noreferrer"
                      style={{ color: cB, fontSize: 12, fontWeight: 600, textDecoration: "none" }}>
                      View Fee Schedule &rarr;
                    </a>
                  </div>
                )}
              </div>
            </div>
          </Card>

          {/* Medicare Parity Analysis */}
          <Card accent={cB}>
            <CH title="Medicare Parity Analysis"
              sub={duckLoading ? "Loading spending data..." : `${codeAnalysis.length} codes with Medicare comparison`}
              right={
                <div style={{ display: "flex", gap: 6, flexWrap: "wrap" }}>
                  {categories.map(c => (
                    <button key={c.id} onClick={() => setCatFilter(c.id)} style={{
                      padding: "3px 10px", borderRadius: 14, border: `1px solid ${catFilter === c.id ? cB : BD}`,
                      background: catFilter === c.id ? cB : WH, color: catFilter === c.id ? WH : AL,
                      fontSize: 11, fontWeight: 600, cursor: "pointer", fontFamily: FB,
                    }}>{c.label}</button>
                  ))}
                </div>
              }
            />

            {summary && (
              <div style={{ display: "flex", justifyContent: "space-around", flexWrap: "wrap", gap: 16, marginBottom: 16,
                padding: "12px 0", borderBottom: `1px solid ${BD}` }}>
                <Met label="Median % Medicare" value={fPct(summary.median)}
                  color={summary.median < 80 ? WARN : POS} />
                <Met label="Below 80% MCR" value={`${summary.belowWarn}`}
                  color={summary.belowWarn > 0 ? WARN : POS} sub="access concern" />
                <Met label="Below 50% MCR" value={`${summary.belowCrit}`}
                  color={summary.belowCrit > 0 ? NEG : POS} sub="critical risk" />
                <Met label="Codes Analyzed" value={`${summary.total}`} />
                <Met label="Total Spend" value={f$(summary.totalSpend)} />
              </div>
            )}

            {/* Parity bar chart — top 20 codes by spend, sorted by % Medicare */}
            {codeAnalysis.length > 0 && (
              <div style={{ marginBottom: 16 }}>
                <div style={{ fontSize: 11, color: AL, marginBottom: 8 }}>Top codes by spending — sorted by Medicare parity</div>
                {[...codeAnalysis].sort((a, b) => a.pctMedicare - b.pctMedicare).slice(0, 20).map(c => (
                  <div key={c.hcpcs} style={{ display: "flex", alignItems: "center", gap: 8, marginBottom: 4 }}>
                    <div style={{ width: 55, fontSize: 10, fontFamily: FM, color: A, textAlign: "right" }}>{c.hcpcs}</div>
                    <div style={{ flex: 1, height: 16, background: SF, borderRadius: 3, overflow: "hidden", position: "relative" }}>
                      <div style={{
                        width: `${Math.min(c.pctMedicare, 150)}%`,
                        maxWidth: "100%",
                        height: "100%", borderRadius: 3,
                        background: c.pctMedicare < PARITY_CRITICAL ? NEG
                          : c.pctMedicare < PARITY_WARN ? WARN : POS,
                      }} />
                      {/* 100% reference line */}
                      <div style={{
                        position: "absolute", left: "66.7%", top: 0, bottom: 0,
                        width: 1, background: A, opacity: 0.3,
                      }} />
                    </div>
                    <div style={{ width: 45, fontSize: 10, fontFamily: FM, textAlign: "right",
                      color: c.pctMedicare < PARITY_CRITICAL ? NEG : c.pctMedicare < PARITY_WARN ? WARN : POS,
                      fontWeight: 600 }}>{fPct(c.pctMedicare)}</div>
                  </div>
                ))}
                <div style={{ display: "flex", gap: 12, marginTop: 6, fontSize: 10, color: AL }}>
                  <span><span style={{ display: "inline-block", width: 8, height: 8, borderRadius: 2, background: NEG, marginRight: 3 }} />Below 50%</span>
                  <span><span style={{ display: "inline-block", width: 8, height: 8, borderRadius: 2, background: WARN, marginRight: 3 }} />50-80%</span>
                  <span><span style={{ display: "inline-block", width: 8, height: 8, borderRadius: 2, background: POS, marginRight: 3 }} />80%+</span>
                  <span style={{ marginLeft: "auto" }}>| = 100% Medicare</span>
                </div>
              </div>
            )}

            {/* Code table */}
            {codeAnalysis.length > 0 && (
              <div style={{ overflowX: "auto" }}>
                <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 11, fontFamily: FM }}>
                  <thead>
                    <tr style={{ borderBottom: `2px solid ${BD}` }}>
                      {["HCPCS", "Description", "Medicaid", "Medicare", "% MCR", "Annual $", "Flag"].map(h => (
                        <th key={h} style={{ padding: "6px 5px", textAlign: h === "Description" ? "left" : "right",
                          color: AL, fontWeight: 600, fontSize: 10, whiteSpace: "nowrap" }}>{h}</th>
                      ))}
                    </tr>
                  </thead>
                  <tbody>
                    {[...codeAnalysis].sort((a, b) => a.pctMedicare - b.pctMedicare).slice(0, 50).map(c => (
                      <tr key={c.hcpcs} style={{
                        borderBottom: `1px solid ${BD}`,
                        background: c.flag === "critical" ? "#FFF5F5" : c.flag === "warning" ? "#FFFDF0" : undefined,
                      }}>
                        <td style={{ padding: "5px", fontWeight: 600, color: A }}>{c.hcpcs}</td>
                        <td style={{ padding: "5px", color: AL, maxWidth: 200, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap", textAlign: "left" }}>{c.desc}</td>
                        <td style={{ padding: "5px", textAlign: "right" }}>${c.medicaidRate.toFixed(2)}</td>
                        <td style={{ padding: "5px", textAlign: "right" }}>${c.medicareRate.toFixed(2)}</td>
                        <td style={{ padding: "5px", textAlign: "right", fontWeight: 600,
                          color: c.flag === "critical" ? NEG : c.flag === "warning" ? WARN : POS }}>
                          {fPct(c.pctMedicare)}
                        </td>
                        <td style={{ padding: "5px", textAlign: "right" }}>{f$(c.totalPaid)}</td>
                        <td style={{ padding: "5px", textAlign: "center" }}>
                          {c.flag === "critical" && <span style={{ color: NEG, fontWeight: 700 }}>&#9679;</span>}
                          {c.flag === "warning" && <span style={{ color: WARN, fontWeight: 700 }}>&#9679;</span>}
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
                {codeAnalysis.length > 50 && (
                  <div style={{ fontSize: 11, color: AL, padding: "8px 0", textAlign: "center" }}>
                    Showing 50 of {codeAnalysis.length} codes (sorted by % Medicare, lowest first). Export CSV for full list.
                  </div>
                )}
              </div>
            )}
          </Card>

          {/* Rate Reduction Impact Simulator */}
          <Card accent={reductionPct >= ANALYSIS_THRESHOLD ? NEG : reductionPct >= REVIEW_THRESHOLD ? WARN : BD}>
            <CH title="Rate Reduction Impact" sub="Model a proposed rate reduction against Access Rule thresholds" />
            <div style={{ display: "flex", gap: 16, alignItems: "center", marginBottom: 16, flexWrap: "wrap" }}>
              <div>
                <label style={{ fontSize: 11, color: AL, display: "block", marginBottom: 4 }}>Proposed Reduction</label>
                <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
                  <input type="range" min={0} max={30} step={1} value={reductionPct}
                    onChange={e => setReductionPct(Number(e.target.value))}
                    style={{ width: 200 }} />
                  <span style={{
                    fontSize: 18, fontWeight: 700, fontFamily: FM, minWidth: 40, textAlign: "right",
                    color: reductionPct >= ANALYSIS_THRESHOLD ? NEG : reductionPct >= REVIEW_THRESHOLD ? WARN : A,
                  }}>{reductionPct}%</span>
                </div>
              </div>
              {reductionPct > 0 && (
                <div style={{ display: "flex", gap: 20, flexWrap: "wrap" }}>
                  <div style={{ padding: "6px 12px", borderRadius: 8,
                    background: reductionPct >= REVIEW_THRESHOLD ? "#FFF5F5" : SF }}>
                    <div style={{ fontSize: 10, color: AL }}>4% Review Threshold</div>
                    <div style={{ fontSize: 13, fontWeight: 700, fontFamily: FM,
                      color: reductionPct >= REVIEW_THRESHOLD ? NEG : POS }}>
                      {reductionPct >= REVIEW_THRESHOLD ? "TRIGGERED" : "Clear"}
                    </div>
                  </div>
                  <div style={{ padding: "6px 12px", borderRadius: 8,
                    background: reductionPct >= ANALYSIS_THRESHOLD ? "#FFF5F5" : SF }}>
                    <div style={{ fontSize: 10, color: AL }}>6% Analysis Threshold</div>
                    <div style={{ fontSize: 13, fontWeight: 700, fontFamily: FM,
                      color: reductionPct >= ANALYSIS_THRESHOLD ? NEG : POS }}>
                      {reductionPct >= ANALYSIS_THRESHOLD ? "TRIGGERED" : "Clear"}
                    </div>
                  </div>
                </div>
              )}
            </div>

            {reductionPct > 0 && reductionAnalysis && (
              <>
                <div style={{ display: "flex", justifyContent: "space-around", flexWrap: "wrap", gap: 16, padding: "12px 0", borderTop: `1px solid ${BD}` }}>
                  <Met label="Annual Impact" value={f$(reductionAnalysis.totalImpact)} color={NEG} />
                  <Met label="Codes Affected" value={`${reductionAnalysis.codesAffected}`} />
                  <Met label="Below 80% MCR After" value={`${reductionAnalysis.belowWarnAfter}`}
                    color={reductionAnalysis.belowWarnAfter > 0 ? WARN : POS} />
                  <Met label="Below 50% MCR After" value={`${reductionAnalysis.belowCritAfter}`}
                    color={reductionAnalysis.belowCritAfter > 0 ? NEG : POS} />
                </div>

                {/* Top impact codes */}
                <div style={{ overflowX: "auto", marginTop: 12 }}>
                  <div style={{ fontSize: 11, color: AL, marginBottom: 6 }}>Top 15 codes by fiscal impact</div>
                  <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 11, fontFamily: FM }}>
                    <thead>
                      <tr style={{ borderBottom: `2px solid ${BD}` }}>
                        {["HCPCS", "Description", "Current", "New Rate", "% MCR After", "Impact", "Flag"].map(h => (
                          <th key={h} style={{ padding: "5px", textAlign: h === "Description" ? "left" : "right",
                            color: AL, fontWeight: 600, fontSize: 10 }}>{h}</th>
                        ))}
                      </tr>
                    </thead>
                    <tbody>
                      {[...reductionAnalysis.details].sort((a, b) => b.impact - a.impact).slice(0, 15).map(c => (
                        <tr key={c.hcpcs} style={{
                          borderBottom: `1px solid ${BD}`,
                          background: c.newPctMed > 0 && c.newPctMed < 50 ? "#FFF5F5" : c.newPctMed > 0 && c.newPctMed < 80 ? "#FFFDF0" : undefined,
                        }}>
                          <td style={{ padding: "5px", fontWeight: 600, color: A }}>{c.hcpcs}</td>
                          <td style={{ padding: "5px", color: AL, maxWidth: 180, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap", textAlign: "left" }}>{c.desc}</td>
                          <td style={{ padding: "5px", textAlign: "right" }}>${c.medicaidRate.toFixed(2)}</td>
                          <td style={{ padding: "5px", textAlign: "right" }}>${c.newRate.toFixed(2)}</td>
                          <td style={{ padding: "5px", textAlign: "right", fontWeight: 600,
                            color: c.newPctMed > 0 && c.newPctMed < 50 ? NEG : c.newPctMed > 0 && c.newPctMed < 80 ? WARN : POS }}>
                            {c.newPctMed > 0 ? fPct(c.newPctMed) : "—"}
                          </td>
                          <td style={{ padding: "5px", textAlign: "right", color: NEG }}>-{f$(c.impact)}</td>
                          <td style={{ padding: "5px", textAlign: "center" }}>
                            {c.newPctMed > 0 && c.newPctMed < 50 && <span style={{ color: NEG, fontWeight: 700 }}>&#9679;</span>}
                            {c.newPctMed >= 50 && c.newPctMed < 80 && <span style={{ color: WARN, fontWeight: 700 }}>&#9679;</span>}
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </>
            )}

            {reductionPct === 0 && (
              <div style={{ color: AL, fontSize: 12, textAlign: "center", padding: 16 }}>
                Adjust the slider to model a proposed rate reduction. Reductions of 4%+ trigger CMS access review;
                6%+ require independent analysis.
              </div>
            )}
          </Card>

          {/* Regulatory Reference */}
          <Card>
            <CH title="Regulatory Reference" sub="42 CFR §447.203 — Ensuring Access Final Rule" />
            <div style={{ fontSize: 12, color: AL, lineHeight: 1.7 }}>
              <p style={{ margin: "0 0 8px" }}>
                <strong>§447.203(b)(1):</strong> States must publish FFS Medicaid payment rates in a machine-readable format
                and update them at least annually. Deadline: July 1, 2026.
              </p>
              <p style={{ margin: "0 0 8px" }}>
                <strong>§447.203(b)(2):</strong> States must document rate-setting methodology and make it publicly available.
              </p>
              <p style={{ margin: "0 0 8px" }}>
                <strong>§447.203(b)(3):</strong> States must conduct a comparative payment rate analysis (including comparison
                to Medicare) and make findings publicly available.
              </p>
              <p style={{ margin: "0 0 8px" }}>
                <strong>§447.203(b)(5):</strong> Rate reductions of 4% or more require an access review demonstrating that
                the reduction will not diminish provider participation or beneficiary access.
              </p>
              <p style={{ margin: 0 }}>
                <strong>§447.203(b)(6):</strong> Rate reductions of 6% or more require an independent access analysis
                conducted by an entity not involved in rate-setting.
              </p>
            </div>
          </Card>
        </>
      )}
    </div>
  );
}
