/**
 * Rate Reduction Analyzer
 * Model the impact of proposed Medicaid rate reductions against
 * access thresholds, Medicare ratios, and total spending.
 */
import { useState, useEffect, useMemo } from "react";
import {
  BarChart, Bar, XAxis, YAxis, Tooltip, ResponsiveContainer, Cell, ReferenceLine,
} from "recharts";
import { query } from "../lib/duckdb";
import { getPreset } from "../lib/presets";
import { STATES_LIST, STATE_NAMES } from "../data/states";

// ── Constants ───────────────────────────────────────────────────────────
const DATA_YEAR = 2023; // T-MSIS data year in claims.parquet
const MCR_CF = 33.4009; // CY2026 Medicare conversion factor ($/RVU)

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

// ── Formatters ──────────────────────────────────────────────────────────
const f$ = (n: number) =>
  n >= 1e9 ? `$${(n / 1e9).toFixed(1)}B`
  : n >= 1e6 ? `$${(n / 1e6).toFixed(1)}M`
  : n >= 1e3 ? `$${(n / 1e3).toFixed(1)}K`
  : `$${n.toFixed(2)}`;
const fN = (n: number) =>
  n >= 1e6 ? `${(n / 1e6).toFixed(1)}M`
  : n >= 1e3 ? `${(n / 1e3).toFixed(0)}K`
  : String(Math.round(n));

// ── Filter presets ──────────────────────────────────────────────────────
const FILTERS = [
  { id: "all", label: "All Codes" },
  { id: "em", label: "E&M" },
  { id: "behavioral_health", label: "Behavioral" },
  { id: "hcbs_waiver", label: "HCBS" },
  { id: "dental", label: "Dental" },
  { id: "rehabilitation", label: "Rehab" },
  { id: "telehealth", label: "Telehealth" },
  { id: "maternity", label: "Maternity" },
];

// ── Tooltip ─────────────────────────────────────────────────────────────
interface TipPayload { value?: number; name?: string; payload?: Record<string, unknown> }
const SafeTip = ({ active, payload }: { active?: boolean; payload?: TipPayload[] }) => {
  if (!active || !payload?.length) return null;
  const d = payload[0]?.payload as Record<string, unknown> | undefined;
  if (!d) return null;
  return (
    <div style={{ background: WH, border: `1px solid ${BD}`, borderRadius: 8, padding: "8px 12px", fontSize: 12, fontFamily: FM }}>
      <div style={{ fontWeight: 700, color: A }}>{String(d.code ?? "")}</div>
      <div style={{ color: AL }}>Impact: {f$(Number(d.impact ?? 0))}</div>
      <div style={{ color: AL }}>New % Medicare: {Number(d.pctMed ?? 0).toFixed(1)}%</div>
    </div>
  );
};

// ── Types ───────────────────────────────────────────────────────────────
interface RawRow { hcpcs_code: string; total_paid: number; total_claims: number; total_bene: number }
interface MedRates { rates: Record<string, { r?: number; rvu?: number; d?: string }> }

// ═════════════════════════════════════════════════════════════════════════
export default function RateReduction() {
  const [st, setSt] = useState("FL");
  const [cat, setCat] = useState("all");
  const [pct, setPct] = useState(5);
  const [rawData, setRawData] = useState<RawRow[]>([]);
  const [descMap, setDescMap] = useState<Record<string, string>>({});
  const [medMap, setMedMap] = useState<Record<string, number>>({});
  const [medDescs, setMedDescs] = useState<Record<string, string>>({});
  const [loading, setLoading] = useState(true);

  // Load static data
  useEffect(() => {
    let cancelled = false;
    Promise.all([
      fetch("/data/hcpcs_descriptions.json").then(r => { if (!r.ok) throw new Error("Failed to load descriptions"); return r.json(); }),
      fetch("/data/medicare_rates.json").then(r => { if (!r.ok) throw new Error("Failed to load Medicare rates"); return r.json(); }),
    ]).then(([descs, med]) => {
      if (cancelled) return;
      setDescMap(descs as Record<string, string>);
      const mRates: Record<string, number> = {};
      const mDescs: Record<string, string> = {};
      const rates = (med as MedRates).rates;
      for (const [code, entry] of Object.entries(rates)) {
        mRates[code] = entry.r ?? (entry.rvu ? entry.rvu * MCR_CF : 0);
        if (entry.d) mDescs[code] = entry.d;
      }
      setMedMap(mRates);
      setMedDescs(mDescs);
    }).catch(() => { /* Static data failed — tool still works with DuckDB data */ });
    return () => { cancelled = true; };
  }, []);

  // Query DuckDB for state spending
  useEffect(() => {
    setLoading(true);
    const esc = st.replace(/'/g, "''");
    const sql = `
      SELECT hcpcs_code,
             SUM(total_paid) AS total_paid,
             SUM(total_claims) AS total_claims,
             SUM(total_beneficiaries) AS total_bene
      FROM 'claims.parquet'
      WHERE state = '${esc}' AND year = ${DATA_YEAR}
      GROUP BY hcpcs_code
      ORDER BY total_paid DESC
      LIMIT 500
    `;
    query(sql).then(result => {
      setRawData(result.rows.map(r => ({
        hcpcs_code: String(r.hcpcs_code ?? ""),
        total_paid: Number(r.total_paid ?? 0),
        total_claims: Number(r.total_claims ?? 0),
        total_bene: Number(r.total_bene ?? 0),
      })));
      setLoading(false);
    }).catch(() => setLoading(false));
  }, [st]);

  // Filtered + reduction analysis
  const analysis = useMemo(() => {
    let filtered = rawData;
    if (cat !== "all") {
      const preset = getPreset(cat);
      if (preset && preset.codes.length > 0) {
        const codes = new Set(preset.codes);
        filtered = rawData.filter(r => codes.has(r.hcpcs_code));
      }
    }
    const factor = 1 - pct / 100;
    return filtered.map(r => {
      const effRate = r.total_claims > 0 ? r.total_paid / r.total_claims : 0;
      const newRate = effRate * factor;
      const medRate = medMap[r.hcpcs_code] ?? 0;
      const curPctMed = medRate > 0 ? (effRate / medRate) * 100 : 0;
      const newPctMed = medRate > 0 ? (newRate / medRate) * 100 : 0;
      return {
        hcpcs: r.hcpcs_code,
        desc: descMap[r.hcpcs_code] || medDescs[r.hcpcs_code] || r.hcpcs_code,
        total_paid: r.total_paid,
        total_claims: r.total_claims,
        total_bene: r.total_bene,
        eff_rate: effRate,
        new_rate: newRate,
        reduction: r.total_paid * (pct / 100),
        med_rate: medRate,
        cur_pct_med: curPctMed,
        new_pct_med: newPctMed,
        flag: medRate > 0 && newPctMed < 50 ? "critical" as const
            : medRate > 0 && newPctMed < 80 ? "warning" as const
            : "ok" as const,
      };
    });
  }, [rawData, cat, pct, medMap, descMap, medDescs]);

  const stats = useMemo(() => {
    const totalImpact = analysis.reduce((s, r) => s + r.reduction, 0);
    const totalSpending = analysis.reduce((s, r) => s + r.total_paid, 0);
    const critical = analysis.filter(r => r.flag === "critical").length;
    const warning = analysis.filter(r => r.flag === "warning").length;
    const withMed = analysis.filter(r => r.med_rate > 0);
    const avgPctBefore = withMed.length > 0
      ? withMed.reduce((s, r) => s + r.cur_pct_med, 0) / withMed.length : 0;
    const avgPctAfter = withMed.length > 0
      ? withMed.reduce((s, r) => s + r.new_pct_med, 0) / withMed.length : 0;
    return { totalImpact, totalSpending, critical, warning, count: analysis.length, avgPctBefore, avgPctAfter };
  }, [analysis]);

  // Chart: top 15 by reduction impact
  const chartData = useMemo(() =>
    analysis.slice(0, 15).map(r => ({
      code: r.hcpcs, impact: r.reduction, pctMed: r.new_pct_med, flag: r.flag,
    })), [analysis]);

  const flagColor = (f: string) => f === "critical" ? NEG : f === "warning" ? WARN : POS;

  // ── Render ────────────────────────────────────────────────────────────
  return (
    <div style={{ maxWidth: 1100, margin: "0 auto", padding: "24px 16px", fontFamily: FB }}>
      <h2 style={{ fontSize: 22, fontWeight: 800, color: A, margin: "0 0 4px" }}>Rate Reduction Analyzer</h2>
      <p style={{ fontSize: 13, color: AL, margin: "0 0 20px" }}>
        Model the impact of proposed Medicaid rate changes on spending, access, and Medicare comparability
      </p>

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
              Rate Reduction: <span style={{ color: NEG, fontFamily: FM }}>{pct}%</span>
            </label>
            <input type="range" min={1} max={30} value={pct} onChange={e => setPct(Number(e.target.value))}
              style={{ width: "100%", accentColor: NEG }} />
            <div style={{ display: "flex", justifyContent: "space-between", fontSize: 10, color: AL }}>
              <span>1%</span>
              <span style={{ color: WARN }}>4% review</span>
              <span style={{ color: NEG }}>6% analysis</span>
              <span>30%</span>
            </div>
          </div>
        </div>
        <div style={{ marginTop: 12 }}>
          {FILTERS.map(f => <Pill key={f.id} label={f.label} on={cat === f.id} onClick={() => setCat(f.id)} />)}
        </div>
      </Card>

      {loading ? (
        <Card><p style={{ color: AL, fontSize: 13, textAlign: "center", padding: 40 }}>Loading spending data for {STATE_NAMES[st] ?? st}...</p></Card>
      ) : (
        <>
          {/* KPI Summary */}
          <Card accent={pct >= 6 ? NEG : pct >= 4 ? WARN : cB}>
            <CH title="Impact Summary" sub={`${pct}% uniform reduction across ${stats.count} codes in ${STATE_NAMES[st] ?? st}`} />
            <div style={{ display: "flex", justifyContent: "space-around", flexWrap: "wrap", gap: 16 }}>
              <Met label="Annual Impact" value={f$(stats.totalImpact)} color={NEG} />
              <Met label="Codes Analyzed" value={fN(stats.count)} />
              <Met label="Below 80% Medicare" value={String(stats.warning + stats.critical)} color={stats.critical > 0 ? NEG : stats.warning > 0 ? WARN : POS} />
              <Met label="Below 50% Medicare" value={String(stats.critical)} color={stats.critical > 0 ? NEG : POS} />
              <Met label="Avg % Medicare" value={`${stats.avgPctBefore.toFixed(0)}% → ${stats.avgPctAfter.toFixed(0)}%`} color={AL} />
            </div>
          </Card>

          {/* Access Rule Flags */}
          {(pct >= 4 || stats.critical > 0 || stats.warning > 0) && (
            <Card accent={NEG}>
              <CH title="Access Rule Compliance Flags" sub="CMS Ensuring Access to Medicaid Services (May 2024)" />
              <div style={{ fontSize: 13, lineHeight: 1.7 }}>
                {pct >= 6 && (
                  <div style={{ color: NEG, fontWeight: 600 }}>
                    ▸ {pct}% reduction requires independent access analysis per 42 CFR §447.203(b)(6)
                  </div>
                )}
                {pct >= 4 && pct < 6 && (
                  <div style={{ color: WARN, fontWeight: 600 }}>
                    ▸ {pct}% reduction triggers access review requirements per 42 CFR §447.203(b)(5)
                  </div>
                )}
                {stats.critical > 0 && (
                  <div style={{ color: NEG }}>
                    ▸ {stats.critical} code{stats.critical > 1 ? "s" : ""} would fall below 50% of Medicare — high risk of provider withdrawal
                  </div>
                )}
                {stats.warning > 0 && (
                  <div style={{ color: WARN }}>
                    ▸ {stats.warning} code{stats.warning > 1 ? "s" : ""} would fall below 80% of Medicare — access review recommended
                  </div>
                )}
              </div>
            </Card>
          )}

          {/* Top Impact Chart */}
          {chartData.length > 0 && (
            <Card>
              <CH title="Top Codes by Spending Impact" sub="Largest dollar reduction" right={
                <ExportBtn label="Export CSV" onClick={() => {
                  downloadCSV(
                    ["HCPCS", "Description", "Annual $", "Claims", "Eff Rate", "New Rate", "Reduction $", "Medicare Rate", "Current % Med", "New % Med", "Flag"],
                    analysis.map(r => [r.hcpcs, r.desc, r.total_paid.toFixed(2), r.total_claims,
                      r.eff_rate.toFixed(2), r.new_rate.toFixed(2), r.reduction.toFixed(2),
                      r.med_rate.toFixed(2), r.cur_pct_med.toFixed(1), r.new_pct_med.toFixed(1), r.flag]),
                    `rate_reduction_${st}_${pct}pct.csv`,
                  );
                }} />
              } />
              <ResponsiveContainer width="100%" height={Math.max(300, chartData.length * 28)}>
                <BarChart data={chartData} layout="vertical" margin={{ left: 60, right: 20, top: 5, bottom: 5 }}>
                  <XAxis type="number" tickFormatter={v => f$(v as number)} style={{ fontSize: 11 }} />
                  <YAxis type="category" dataKey="code" width={55} style={{ fontSize: 11, fontFamily: FM }} />
                  <Tooltip content={<SafeTip />} />
                  <ReferenceLine x={0} stroke={BD} />
                  <Bar dataKey="impact" radius={[0, 4, 4, 0]}>
                    {chartData.map((d, i) => (
                      <Cell key={i} fill={flagColor(d.flag)} fillOpacity={0.8} />
                    ))}
                  </Bar>
                </BarChart>
              </ResponsiveContainer>
            </Card>
          )}

          {/* Detail Table */}
          <Card>
            <CH title="Code-Level Analysis" sub={`${analysis.length} codes — sorted by spending`} />
            <div style={{ overflowX: "auto" }}>
              <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 12, fontFamily: FM }}>
                <thead>
                  <tr style={{ borderBottom: `2px solid ${BD}` }}>
                    {["Code", "Description", "Annual $", "Claims", "Current Rate", "New Rate", "Reduction $", "% Medicare", "→ New %", "Flag"].map(h => (
                      <th key={h} style={{ padding: "8px 6px", textAlign: "left", color: AL, fontWeight: 600, fontSize: 11, whiteSpace: "nowrap" }}>{h}</th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {analysis.length === 0 && <tr><td colSpan={10} style={{ padding: "20px 8px", textAlign: "center", color: AL, fontSize: 11 }}>No codes found for this category in {STATE_NAMES[st] ?? st}.</td></tr>}
                  {analysis.slice(0, 100).map(r => (
                    <tr key={r.hcpcs} style={{
                      borderBottom: `1px solid ${BD}`,
                      background: r.flag === "critical" ? "#FEE2E2" : r.flag === "warning" ? "#FEF3CD" : undefined,
                    }}>
                      <td style={{ padding: "6px", fontWeight: 600, color: A }}>{r.hcpcs}</td>
                      <td style={{ padding: "6px", color: AL, maxWidth: 200, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>{r.desc}</td>
                      <td style={{ padding: "6px", textAlign: "right" }}>{f$(r.total_paid)}</td>
                      <td style={{ padding: "6px", textAlign: "right" }}>{fN(r.total_claims)}</td>
                      <td style={{ padding: "6px", textAlign: "right" }}>${r.eff_rate.toFixed(2)}</td>
                      <td style={{ padding: "6px", textAlign: "right", color: NEG }}>${r.new_rate.toFixed(2)}</td>
                      <td style={{ padding: "6px", textAlign: "right", color: NEG }}>{f$(r.reduction)}</td>
                      <td style={{ padding: "6px", textAlign: "right", color: r.cur_pct_med < 80 ? WARN : AL }}>
                        {r.med_rate > 0 ? `${r.cur_pct_med.toFixed(0)}%` : "—"}
                      </td>
                      <td style={{ padding: "6px", textAlign: "right", fontWeight: 600,
                        color: r.new_pct_med < 50 ? NEG : r.new_pct_med < 80 ? WARN : POS }}>
                        {r.med_rate > 0 ? `${r.new_pct_med.toFixed(0)}%` : "—"}
                      </td>
                      <td style={{ padding: "6px", textAlign: "center" }}>
                        {r.flag === "critical" ? <span style={{ color: NEG, fontWeight: 700 }}>●</span>
                          : r.flag === "warning" ? <span style={{ color: WARN, fontWeight: 700 }}>●</span>
                          : <span style={{ color: POS }}>○</span>}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
            {analysis.length > 100 && (
              <p style={{ fontSize: 11, color: AL, marginTop: 8 }}>Showing 100 of {analysis.length} codes</p>
            )}
          </Card>

          {/* Methodology note */}
          <Card>
            <CH title="Methodology" />
            <div style={{ fontSize: 12, color: AL, lineHeight: 1.7 }}>
              <p style={{ margin: "0 0 8px" }}>
                Effective Medicaid rates are derived from T-MSIS claims (total paid / total claims) for {STATE_NAMES[st] ?? st}, CY 2023.
                Medicare rates are from the CMS Physician Fee Schedule. Access Rule thresholds per 42 CFR §447.203.
              </p>
              <p style={{ margin: 0 }}>
                <strong>4% threshold:</strong> Rate reductions of 4%+ require states to demonstrate that access will not be materially impaired.{" "}
                <strong>6% threshold:</strong> Rate reductions of 6%+ require an independent access analysis before implementation.
              </p>
            </div>
          </Card>
        </>
      )}
    </div>
  );
}
