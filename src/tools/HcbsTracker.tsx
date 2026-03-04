/**
 * HCBS Compensation Tracker
 * How much of Medicaid HCBS spending reaches direct care workers?
 * The 80/20 pass-through standard is the benchmark.
 */
import { useState, useEffect, useMemo } from "react";
import {
  BarChart, Bar, XAxis, YAxis, Tooltip, ResponsiveContainer, Cell, ReferenceLine,
} from "recharts";
import { STATES_LIST, STATE_NAMES } from "../data/states";

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
  <div style={{ textAlign: "center", minWidth: 100 }}>
    <div style={{ fontSize: 22, fontWeight: 700, color: color || A, fontFamily: FM }}>{value}</div>
    <div style={{ fontSize: 11, color: AL, marginTop: 2 }}>{label}</div>
    {sub && <div style={{ fontSize: 10, color: AL, marginTop: 1 }}>{sub}</div>}
  </div>
);
const Pill = ({ label, on, onClick }: { label: string; on: boolean; onClick: () => void }) => (
  <button onClick={onClick} aria-pressed={on} style={{
    padding: "5px 14px", borderRadius: 20, border: `1px solid ${on ? cB : BD}`,
    background: on ? cB : WH, color: on ? WH : AL, fontSize: 12, fontWeight: 600,
    cursor: "pointer", fontFamily: FB, marginRight: 6, marginBottom: 6,
  }}>{label}</button>
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

const ExportBtn = ({ label, onClick }: { label: string; onClick: () => void }) => (
  <button onClick={onClick} style={{
    padding: "6px 14px", borderRadius: 6, border: `1px solid ${BD}`, background: WH,
    color: AL, fontSize: 12, cursor: "pointer", fontFamily: FM,
  }}>{label}</button>
);

const f$ = (n: number) => `$${n.toFixed(2)}`;

// ── Types ───────────────────────────────────────────────────────────────
interface CrosswalkCode { hcpcs: string; desc: string; units_per_hour: number | null; unit: string }
interface CrosswalkCat {
  id: string; name: string; soc: string;
  codes: CrosswalkCode[];
  overhead_default: number; overhead_range: [number, number]; overhead_note: string;
}
interface BlsEntry { title: string; h_mean: number; h_median: number; h_p10: number; h_p25: number; h_p75: number; h_p90: number; a_median: number; emp?: number }
interface BlsData { national: Record<string, BlsEntry>; states: Record<string, Record<string, BlsEntry>> }
interface HcpcsEntry { code?: string; c?: string; rates?: Record<string, number>; r?: Record<string, number> }

// ── Tooltip ─────────────────────────────────────────────────────────────
interface TipPayload { value?: number; payload?: Record<string, unknown> }
const SafeTip = ({ active, payload }: { active?: boolean; payload?: TipPayload[] }) => {
  if (!active || !payload?.length) return null;
  const d = payload[0]?.payload as Record<string, unknown> | undefined;
  if (!d) return null;
  return (
    <div style={{ background: WH, border: `1px solid ${BD}`, borderRadius: 8, padding: "8px 12px", fontSize: 12, fontFamily: FM }}>
      <div style={{ fontWeight: 700, color: A }}>{String(d.name ?? d.st ?? "")}</div>
      <div style={{ color: AL }}>Worker share: {Number(d.workerPct ?? 0).toFixed(1)}%</div>
      <div style={{ color: AL }}>Rate: ${Number(d.rate ?? 0).toFixed(2)}/hr</div>
    </div>
  );
};

// ═════════════════════════════════════════════════════════════════════════
export default function HcbsTracker() {
  const [st, setSt] = useState("FL");
  const [catId, setCatId] = useState("hcbs");
  const [overhead, setOverhead] = useState(35);
  const [crosswalk, setCrosswalk] = useState<CrosswalkCat[]>([]);
  const [blsData, setBlsData] = useState<BlsData | null>(null);
  const [hcpcsData, setHcpcsData] = useState<HcpcsEntry[]>([]);
  const [loading, setLoading] = useState(true);

  // Load data on mount
  useEffect(() => {
    let cancelled = false;
    Promise.all([
      fetch("/data/soc_hcpcs_crosswalk.json").then(r => { if (!r.ok) throw new Error("Failed"); return r.json(); }),
      fetch("/data/bls_wages.json").then(r => { if (!r.ok) throw new Error("Failed"); return r.json(); }),
      fetch("/data/hcpcs.json").then(r => { if (!r.ok) throw new Error("Failed"); return r.json(); }),
    ]).then(([xw, bls, hcpcs]) => {
      if (cancelled) return;
      setCrosswalk((xw as { categories: CrosswalkCat[] }).categories);
      setBlsData(bls as BlsData);
      setHcpcsData(Array.isArray(hcpcs) ? hcpcs : []);
      setLoading(false);
    }).catch(() => { if (!cancelled) setLoading(false); });
    return () => { cancelled = true; };
  }, []);

  const curCat = useMemo(() => crosswalk.find(c => c.id === catId), [crosswalk, catId]);

  // Update overhead when category changes
  useEffect(() => {
    if (curCat) setOverhead(curCat.overhead_default);
  }, [curCat]);

  // Get T-MSIS rate for a code in a state
  const getRate = (code: string, state: string): number => {
    for (const h of hcpcsData) {
      const c = h.code ?? h.c;
      if (c === code) {
        const rates = h.rates ?? h.r;
        return rates?.[state] ?? 0;
      }
    }
    return 0;
  };

  // Code-level analysis for selected state + category
  const codeAnalysis = useMemo(() => {
    if (!curCat || !blsData) return [];
    const blsState = blsData.states[st]?.[curCat.soc];
    const blsNational = blsData.national[curCat.soc];
    const blsMedian = blsState?.h_median ?? blsNational?.h_median ?? 0;

    return curCat.codes.filter(c => c.units_per_hour).map(code => {
      const rate = getRate(code.hcpcs, st);
      const grossHourly = rate * (code.units_per_hour ?? 1);
      const workerHourly = grossHourly * (1 - overhead / 100);
      const workerPct = grossHourly > 0 ? (workerHourly / grossHourly) * 100 : 0;
      const meets80 = overhead <= 20;
      const gapVsBls = workerHourly - blsMedian;
      return {
        hcpcs: code.hcpcs,
        desc: code.desc,
        unit: code.unit,
        units_per_hour: code.units_per_hour ?? 0,
        medicaid_rate: rate,
        gross_hourly: grossHourly,
        worker_hourly: workerHourly,
        worker_pct: workerPct,
        bls_median: blsMedian,
        gap: gapVsBls,
        meets80,
        adequate: workerHourly >= blsMedian,
      };
    });
  }, [curCat, blsData, st, overhead, hcpcsData]);

  // All-state comparison for primary HCBS code
  const allStates = useMemo(() => {
    if (!curCat || !blsData) return [];
    const primaryCode = curCat.codes.find(c => c.units_per_hour);
    if (!primaryCode) return [];

    return STATES_LIST.map(s => {
      const rate = getRate(primaryCode.hcpcs, s);
      const grossHourly = rate * (primaryCode.units_per_hour ?? 1);
      const workerHourly = grossHourly * (1 - overhead / 100);
      const blsState = blsData.states[s]?.[curCat.soc];
      const blsMedian = blsState?.h_median ?? blsData.national[curCat.soc]?.h_median ?? 0;
      const workerPct = grossHourly > 0 ? ((1 - overhead / 100) * 100) : 0;
      return {
        st: s,
        name: STATE_NAMES[s] ?? s,
        rate: grossHourly,
        workerHourly,
        workerPct,
        bls_median: blsMedian,
        gap: workerHourly - blsMedian,
        adequate: workerHourly >= blsMedian,
      };
    }).filter(s => s.rate > 0).sort((a, b) => b.workerHourly - a.workerHourly);
  }, [curCat, blsData, st, overhead, hcpcsData]);

  // Summary stats
  const summary = useMemo(() => {
    if (codeAnalysis.length === 0) return { avgRate: 0, avgWorker: 0, avgBls: 0, pctAdequate: 0, workerPct: 100 - overhead };
    const avgRate = codeAnalysis.reduce((s, c) => s + c.gross_hourly, 0) / codeAnalysis.length;
    const avgWorker = codeAnalysis.reduce((s, c) => s + c.worker_hourly, 0) / codeAnalysis.length;
    const avgBls = codeAnalysis.reduce((s, c) => s + c.bls_median, 0) / codeAnalysis.length;
    const pctAdequate = (codeAnalysis.filter(c => c.adequate).length / codeAnalysis.length) * 100;
    return { avgRate, avgWorker, avgBls, pctAdequate, workerPct: 100 - overhead };
  }, [codeAnalysis, overhead]);

  // Chart data (all states, top 30)
  const chartData = useMemo(() =>
    allStates.slice(0, 30).map(s => ({
      st: s.st, name: s.name, rate: s.rate, workerHourly: s.workerHourly,
      workerPct: s.workerPct, bls: s.bls_median, isHighlight: s.st === st,
    })), [allStates, st]);

  // ── Render ────────────────────────────────────────────────────────────
  return (
    <div style={{ maxWidth: 1100, margin: "0 auto", padding: "24px 16px", fontFamily: FB }}>
      <h2 style={{ fontSize: 22, fontWeight: 800, color: A, margin: "0 0 4px" }}>HCBS Compensation Tracker</h2>
      <p style={{ fontSize: 13, color: AL, margin: "0 0 20px" }}>
        How much of Medicaid HCBS spending reaches direct care workers? The 80/20 pass-through standard is the benchmark.
      </p>

      {loading ? (
        <Card><p style={{ color: AL, fontSize: 13, textAlign: "center", padding: 40 }}>Loading wage and rate data...</p></Card>
      ) : (
        <>
          {/* Controls */}
          <Card>
            <div style={{ display: "flex", gap: 24, flexWrap: "wrap", alignItems: "flex-end" }}>
              <div>
                <label style={{ fontSize: 11, color: AL, fontWeight: 600, display: "block", marginBottom: 4 }}>State</label>
                <select value={st} onChange={e => setSt(e.target.value)}
                  style={{ padding: "6px 10px", borderRadius: 6, border: `1px solid ${BD}`, fontSize: 13, fontFamily: FM }}>
                  {STATES_LIST.map(s => <option key={s} value={s}>{s} — {STATE_NAMES[s]}</option>)}
                </select>
              </div>
              <div style={{ flex: 1, minWidth: 200 }}>
                <label style={{ fontSize: 11, color: AL, fontWeight: 600, display: "block", marginBottom: 4 }}>
                  Agency Overhead: <span style={{ color: overhead <= 20 ? POS : WARN, fontFamily: FM }}>{overhead}%</span>
                </label>
                <input type="range" min={10} max={55} value={overhead} onChange={e => setOverhead(Number(e.target.value))}
                  style={{ width: "100%", accentColor: overhead <= 20 ? POS : WARN }} />
                <div style={{ display: "flex", justifyContent: "space-between", fontSize: 10, color: AL }}>
                  <span>10%</span>
                  <span style={{ color: POS, fontWeight: 600 }}>20% (80/20 target)</span>
                  <span>55%</span>
                </div>
              </div>
            </div>
            <div style={{ marginTop: 12 }}>
              {crosswalk.map(c => <Pill key={c.id} label={c.name} on={catId === c.id} onClick={() => setCatId(c.id)} />)}
            </div>
            {curCat && (
              <p style={{ fontSize: 11, color: AL, margin: "8px 0 0", fontStyle: "italic" }}>{curCat.overhead_note}</p>
            )}
          </Card>

          {/* 80/20 Gauge + KPIs */}
          <Card accent={overhead <= 20 ? POS : WARN}>
            <CH title="Pass-Through Summary" sub={`${STATE_NAMES[st] ?? st} — ${curCat?.name ?? ""}`} />
            <div style={{ display: "flex", justifyContent: "space-around", flexWrap: "wrap", gap: 16, marginBottom: 20 }}>
              <Met label="Avg Gross Rate" value={`$${summary.avgRate.toFixed(2)}/hr`} />
              <Met label="Implied Worker Pay" value={`$${summary.avgWorker.toFixed(2)}/hr`} color={summary.avgWorker >= summary.avgBls ? POS : NEG} />
              <Met label="BLS Median Wage" value={`$${summary.avgBls.toFixed(2)}/hr`} color={AL} />
              <Met label="Worker Share" value={`${summary.workerPct}%`} color={summary.workerPct >= 80 ? POS : WARN} />
            </div>

            {/* Visual gauge */}
            <div style={{ position: "relative", height: 32, background: SF, borderRadius: 16, overflow: "hidden", border: `1px solid ${BD}` }}>
              <div style={{
                width: `${Math.min(summary.workerPct, 100)}%`, height: "100%",
                background: summary.workerPct >= 80 ? POS : summary.workerPct >= 60 ? WARN : NEG,
                borderRadius: 16, transition: "width 0.3s ease",
              }} />
              <div style={{
                position: "absolute", left: "80%", top: 0, bottom: 0, width: 2,
                background: A, opacity: 0.5,
              }} />
              <div style={{
                position: "absolute", left: "80%", top: -18, transform: "translateX(-50%)",
                fontSize: 10, color: AL, fontWeight: 600,
              }}>80%</div>
            </div>
            <div style={{ display: "flex", justifyContent: "space-between", fontSize: 11, color: AL, marginTop: 4 }}>
              <span>{summary.workerPct}% to workers</span>
              <span>{overhead}% overhead</span>
            </div>
            <div style={{ marginTop: 10, padding: "8px 12px", background: summary.workerPct >= 80 ? "#E8F5E9" : "#FFF3E0",
              borderRadius: 8, fontSize: 12, fontWeight: 600,
              color: summary.workerPct >= 80 ? POS : WARN }}>
              {summary.workerPct >= 80
                ? `✓ At ${overhead}% overhead, ${STATE_NAMES[st]}'s HCBS rates meet the 80/20 pass-through standard`
                : `⚠ At ${overhead}% overhead, only ${summary.workerPct}% reaches workers — below the 80% pass-through target`}
            </div>
          </Card>

          {/* Code-Level Table */}
          {codeAnalysis.length === 0 && !loading && (
            <Card>
              <div style={{ padding: "24px 14px", textAlign: "center", color: AL, fontSize: 11, lineHeight: 1.7 }}>
                No per-unit codes available for {curCat?.name ?? "this category"} in {STATE_NAMES[st] ?? st}.
                This category may not have unit-based billing codes with rate data for this state.
              </div>
            </Card>
          )}
          {codeAnalysis.length > 0 && (
            <Card>
              <CH title="Code-Level Analysis" sub={`${curCat?.name ?? ""} codes in ${STATE_NAMES[st] ?? st}`} right={
                <ExportBtn label="Export CSV" onClick={() => {
                  downloadCSV(
                    ["HCPCS", "Description", "Unit", "Rate", "Gross $/hr", "Worker $/hr", "BLS Median", "Gap", "Adequate"],
                    codeAnalysis.map(c => [c.hcpcs, c.desc, c.unit, c.medicaid_rate.toFixed(2),
                      c.gross_hourly.toFixed(2), c.worker_hourly.toFixed(2), c.bls_median.toFixed(2),
                      c.gap.toFixed(2), c.adequate ? "Yes" : "No"]),
                    `hcbs_tracker_${st}_${catId}.csv`,
                  );
                }} />
              } />
              <div style={{ overflowX: "auto" }}>
              <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 12, fontFamily: FM }}>
                <thead>
                  <tr style={{ borderBottom: `2px solid ${BD}` }}>
                    {["Code", "Description", "Unit", "Rate", "Gross $/hr", "Worker $/hr", "BLS Median", "Gap", "Adequate"].map(h => (
                      <th key={h} style={{ padding: "8px 6px", textAlign: "left", color: AL, fontWeight: 600, fontSize: 11, whiteSpace: "nowrap" }}>{h}</th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {codeAnalysis.map(c => (
                    <tr key={c.hcpcs} style={{ borderBottom: `1px solid ${BD}`,
                      background: c.adequate ? undefined : "#FEE2E2" }}>
                      <td style={{ padding: "6px", fontWeight: 600, color: A }}>{c.hcpcs}</td>
                      <td style={{ padding: "6px", color: AL }}>{c.desc}</td>
                      <td style={{ padding: "6px", color: AL }}>{c.unit}</td>
                      <td style={{ padding: "6px", textAlign: "right" }}>{f$(c.medicaid_rate)}</td>
                      <td style={{ padding: "6px", textAlign: "right" }}>{f$(c.gross_hourly)}</td>
                      <td style={{ padding: "6px", textAlign: "right", fontWeight: 600,
                        color: c.adequate ? POS : NEG }}>{f$(c.worker_hourly)}</td>
                      <td style={{ padding: "6px", textAlign: "right", color: AL }}>{f$(c.bls_median)}</td>
                      <td style={{ padding: "6px", textAlign: "right", fontWeight: 600,
                        color: c.gap >= 0 ? POS : NEG }}>
                        {c.gap >= 0 ? "+" : ""}{f$(c.gap)}
                      </td>
                      <td style={{ padding: "6px", textAlign: "center" }}>
                        {c.adequate
                          ? <span style={{ color: POS, fontWeight: 700 }}>✓</span>
                          : <span style={{ color: NEG, fontWeight: 700 }}>✗</span>}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
              </div>
            </Card>
          )}

          {/* All-State Comparison */}
          {chartData.length > 0 && (
            <Card>
              <CH title="Cross-State Comparison"
                sub={`Implied worker hourly for ${curCat?.codes.find(c => c.units_per_hour)?.hcpcs ?? ""} at ${overhead}% overhead`} />
              <ResponsiveContainer width="100%" height={Math.max(400, chartData.length * 22)}>
                <BarChart data={chartData} layout="vertical" margin={{ left: 35, right: 20, top: 5, bottom: 5 }}>
                  <XAxis type="number" tickFormatter={v => `$${v}`} style={{ fontSize: 11 }} />
                  <YAxis type="category" dataKey="st" width={30} style={{ fontSize: 11, fontFamily: FM }} />
                  <Tooltip content={<SafeTip />} />
                  <ReferenceLine x={summary.avgBls} stroke={AL} strokeDasharray="3 3" label={{ value: `BLS $${summary.avgBls.toFixed(0)}`, position: "top", fontSize: 10 }} />
                  <Bar dataKey="workerHourly" radius={[0, 4, 4, 0]}>
                    {chartData.map((d, i) => (
                      <Cell key={i} fill={d.isHighlight ? cB : d.workerHourly >= d.bls ? POS : NEG}
                        fillOpacity={d.isHighlight ? 1 : 0.6} />
                    ))}
                  </Bar>
                </BarChart>
              </ResponsiveContainer>
            </Card>
          )}

          {/* Methodology */}
          <Card>
            <CH title="Methodology" />
            <div style={{ fontSize: 12, color: AL, lineHeight: 1.7 }}>
              <p style={{ margin: "0 0 8px" }}>
                <strong>Implied worker hourly</strong> = Medicaid rate × units per hour × (1 − overhead%).
                Overhead accounts for payroll taxes, workers' comp, admin/billing, benefits, and margin.
              </p>
              <p style={{ margin: "0 0 8px" }}>
                <strong>80/20 standard:</strong> The Ensuring Access final rule requires that at least 80% of Medicaid HCBS
                payments for personal care and home health services go to direct care worker compensation.
                States must report compliance with monthly updates starting July 2026.
              </p>
              <p style={{ margin: 0 }}>
                Medicaid rates from T-MSIS (effective rates). BLS wages from OES May 2024.
                SOC-to-HCPCS crosswalk maps service categories to billing codes.
              </p>
            </div>
          </Card>
        </>
      )}
    </div>
  );
}
