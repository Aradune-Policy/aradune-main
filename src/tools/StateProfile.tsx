/**
 * State Profile — Everything Aradune knows about a state, in one view.
 * Fetches from ~12 endpoints in parallel on state selection.
 */
import { useState, useEffect, useCallback } from "react";
import {
  ComposedChart, Area, Line, Bar, XAxis, YAxis, CartesianGrid,
  Tooltip, ResponsiveContainer, Legend, BarChart, PieChart, Pie, Cell,
} from "recharts";
import { STATE_NAMES } from "../data/states";
import { API_BASE } from "../lib/api";

// ── Design tokens ───────────────────────────────────────────────────────
const A = "#0A2540", AL = "#425A70", POS = "#2E6B4A", NEG = "#A4262C", WARN = "#B8860B";
const SF = "#F5F7F5", BD = "#E4EAE4", WH = "#fff", cB = "#2E6B4A", ACC = "#C4590A";
const FM = "'SF Mono',Menlo,Consolas,monospace";
const FB = "'Helvetica Neue',Arial,sans-serif";
const SH = "0 1px 3px rgba(0,0,0,.04),0 4px 12px rgba(0,0,0,.03)";

// ── Local UI components ─────────────────────────────────────────────────
const Card = ({ children, accent }: { children: React.ReactNode; accent?: string }) => (
  <div style={{
    background: WH, borderRadius: 10, padding: "20px 24px", marginBottom: 16,
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
const fmtPct = (n: number) => (n * 100).toFixed(1) + "%";
const PIE_COLORS = [cB, ACC, "#3B82F6", "#8B5CF6", "#EC4899", "#F59E0B", "#6366F1", "#14B8A6"];

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

// ── Safe fetch helper ────────────────────────────────────────────────────
async function safeFetch(url: string) {
  try {
    const res = await fetch(url);
    if (!res.ok) return null;
    return await res.json();
  } catch {
    return null;
  }
}

// ═══════════════════════════════════════════════════════════════════════
// Main component
// ═══════════════════════════════════════════════════════════════════════
export default function StateProfile() {
  const [state, setState] = useState(() => {
    const hash = window.location.hash;
    const m = hash.match(/state\/([A-Z]{2})/i);
    return m ? m[1].toUpperCase() : "FL";
  });
  const [loading, setLoading] = useState(false);
  const [data, setData] = useState<any>(null);

  // Section visibility
  const [sections, setSections] = useState<Record<string, boolean>>({
    enrollment: true, rates: true, hospitals: true,
    quality: true, workforce: false, pharmacy: false, economic: true,
  });
  const toggle = (key: string) => setSections(s => ({ ...s, [key]: !s[key] }));

  // ── Fetch all data in parallel ──────────────────────────────────────
  const loadState = useCallback(async (code: string) => {
    setLoading(true);
    setData(null);
    window.location.hash = `#/state/${code}`;

    const [
      demographics, economic, enrollment, enrollmentByGroup,
      cpraRates, hospitalSummary, hospitals,
      fmap, hpsa, wages, quality, scorecard,
      fiveStarSummary, staffingSummary, topDrugs,
      supplementalSummary, unwinding, spas,
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
    ]);

    // Extract FMAP for this state
    const fmapRows = fmap?.rows || [];
    const stateFmap = fmapRows.find((r: any) => r.state_code === code);

    // Extract hospital summary for this state
    const hospSummaryRows = hospitalSummary?.rows || [];
    const stateHospSummary = hospSummaryRows.find((r: any) => r.state_code === code);

    // Extract five-star summary for this state
    const fsSummaryRows = fiveStarSummary?.rows || [];
    const stateFsSummary = fsSummaryRows.find((r: any) => r.state_code === code);

    // Extract staffing summary for this state
    const staffRows = staffingSummary?.rows || [];
    const stateStaffSummary = staffRows.find((r: any) => r.state_code === code);

    // Extract supplemental summary for this state
    const suppRows = supplementalSummary?.rows || [];
    const stateSuppSummary = suppRows.find((r: any) => r.state_code === code);

    // Compute CPRA summary from rates
    const cpraRows = cpraRates?.rows || [];
    const emRows = cpraRows.filter((r: any) => r.is_em === true || r.is_em === 1 || r.category_447);
    const allPctMcr = cpraRows.filter((r: any) => r.pct_of_medicare > 0).map((r: any) => r.pct_of_medicare);
    const medianPctMcr = allPctMcr.length > 0
      ? allPctMcr.sort((a: number, b: number) => a - b)[Math.floor(allPctMcr.length / 2)]
      : null;

    setData({
      demographics: demographics?.rows?.[0] || null,
      economic: economic?.rows || [],
      enrollment: enrollment?.rows || [],
      enrollmentByGroup: enrollmentByGroup?.rows || [],
      cpraRates: cpraRows,
      cpraEmRows: emRows,
      cpraSummary: { count: cpraRows.length, emCount: emRows.length, medianPctMcr },
      hospitalSummary: stateHospSummary,
      hospitals: hospitals?.rows || [],
      fmap: stateFmap,
      hpsa: hpsa?.rows || [],
      wages: wages?.rows || [],
      quality: quality?.rows || [],
      scorecard: scorecard?.rows || [],
      fiveStarSummary: stateFsSummary,
      staffingSummary: stateStaffSummary,
      topDrugs: topDrugs?.rows || [],
      supplementalSummary: stateSuppSummary,
      unwinding: unwinding?.rows || [],
      spas: spas?.rows || [],
    });
    setLoading(false);
  }, []);

  useEffect(() => { loadState(state); }, [state, loadState]);

  // ═══ RENDER ═══════════════════════════════════════════════════════════
  const d = data;

  return (
    <div style={{ maxWidth: 1080, margin: "0 auto", padding: "0 20px 48px", fontFamily: FB }}>
      {/* Header */}
      <div style={{ padding: "28px 0 20px", display: "flex", justifyContent: "space-between", alignItems: "flex-end" }}>
        <div>
          <h2 style={{ margin: 0, fontSize: 22, fontWeight: 700, color: A, letterSpacing: -0.3 }}>
            State Profile
          </h2>
          <p style={{ margin: "4px 0 0", fontSize: 12, color: AL }}>
            Everything Aradune knows about a state — enrollment, rates, quality, workforce, and economy.
          </p>
        </div>
        <div style={{ display: "flex", gap: 8, alignItems: "center" }}>
          <select value={state} onChange={e => setState(e.target.value)} style={{
            padding: "8px 14px", borderRadius: 8, border: `1px solid ${BD}`,
            fontSize: 13, fontFamily: FB, color: A, background: WH, fontWeight: 600, minWidth: 220,
          }}>
            {STATES.map(s => <option key={s} value={s}>{s} — {STATE_NAMES[s]}</option>)}
          </select>
          {d && <button onClick={() => {
            const rows: (string | number)[][] = [];
            // Rate comparison data
            if (d.cpraRates.length > 0) {
              for (const r of d.cpraRates) {
                rows.push([r.cpt_hcpcs_code || r.code || "", r.description || r.desc || "", r.medicaid_rate?.toFixed(2) || "", r.medicare_nonfac_rate?.toFixed(2) || "", r.pct_of_medicare ? (r.pct_of_medicare * 100).toFixed(1) : ""]);
              }
            }
            if (rows.length > 0) {
              downloadCSV(["HCPCS Code", "Description", "Medicaid Rate", "Medicare Rate", "% of Medicare"], rows, `state_profile_rates_${state}.csv`);
            } else {
              // Export hospitals if no rate data
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
        </div>
      </div>

      {loading && (
        <Card>
          <div style={{ textAlign: "center", padding: 40, fontSize: 13, color: AL }}>
            Loading data for {STATE_NAMES[state]}...
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
                    {state} | {demo?.region || "—"}
                  </p>
                </div>
                {fmapVal && (
                  <div style={{ textAlign: "right" }}>
                    <div style={{ fontSize: 10, color: AL }}>FMAP (FY{fmapVal.fiscal_year})</div>
                    <div style={{ fontSize: 20, fontWeight: 700, color: cB, fontFamily: FM }}>{(fmapVal.fmap * 100).toFixed(2)}%</div>
                  </div>
                )}
              </div>

              <div style={{ display: "flex", gap: 16, flexWrap: "wrap", justifyContent: "space-around", padding: "16px 0 8px", borderTop: `1px solid ${BD}`, marginTop: 12 }}>
                {demo && <>
                  <Met label="Population" value={fmtNum(demo.total_population || 0)} mono />
                  <Met label="Poverty Rate" value={demo.poverty_rate ? `${(demo.poverty_rate * 100).toFixed(1)}%` : "—"} color={demo.poverty_rate > 0.15 ? NEG : AL} mono />
                  <Met label="Uninsured" value={demo.uninsured_rate ? `${(demo.uninsured_rate * 100).toFixed(1)}%` : "—"} color={demo.uninsured_rate > 0.10 ? NEG : AL} mono />
                </>}
                {d.enrollment.length > 0 && (() => {
                  const latest = d.enrollment[d.enrollment.length - 1];
                  const totalEnroll = latest.total_enrollment || latest.total_medicaid_enrollment || 0;
                  const mcEnroll = latest.mc_enrollment || latest.managed_care_enrollment || 0;
                  return <>
                    <Met label="Medicaid Enrollment" value={fmtNum(totalEnroll)} color={cB} mono />
                    <Met label="Managed Care %" value={totalEnroll > 0 ? `${((mcEnroll / totalEnroll) * 100).toFixed(0)}%` : "—"} mono />
                  </>;
                })()}
                {d.cpraSummary.count > 0 && (
                  <Met label="Median % of Medicare" value={d.cpraSummary.medianPctMcr ? `${(d.cpraSummary.medianPctMcr * 100).toFixed(1)}%` : "—"}
                    color={d.cpraSummary.medianPctMcr < 0.8 ? NEG : d.cpraSummary.medianPctMcr > 1.0 ? POS : AL} mono />
                )}
                {d.hospitalSummary && (
                  <Met label="Hospitals" value={d.hospitalSummary.hospital_count || d.hospitals.length || "—"} mono />
                )}
                {d.hpsa.length > 0 && (
                  <Met label="HPSA Designations" value={d.hpsa.length} mono />
                )}
              </div>
            </Card>

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
                {d.cpraRates.length > 0 ? (() => {
                  // Build distribution histogram
                  const buckets = [0, 0.25, 0.5, 0.75, 1.0, 1.25, 1.5, 2.0, 999];
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
                          {d.cpraSummary.medianPctMcr ? `${(d.cpraSummary.medianPctMcr * 100).toFixed(1)}%` : "—"}
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
                          <span style={{ fontFamily: FM, minWidth: 80, color: A }}>{spa.spa_id || spa.spa_number || "—"}</span>
                          <span style={{ flex: 1 }}>{spa.title || spa.description || "—"}</span>
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
                  {d.hospitalSummary?.median_cost_to_charge && <Met label="Median CCR" value={d.hospitalSummary.median_cost_to_charge.toFixed(3)} mono small />}
                  {d.fiveStarSummary?.avg_overall_rating && <Met label="Avg NF Rating" value={`${d.fiveStarSummary.avg_overall_rating.toFixed(1)}★`} mono small />}
                  {d.staffingSummary?.avg_nursing_hprd && <Met label="Avg NF HPRD" value={d.staffingSummary.avg_nursing_hprd.toFixed(2)} mono small />}
                  {d.hpsa.length > 0 && <Met label="HPSAs" value={d.hpsa.length} color={d.hpsa.length > 50 ? NEG : AL} mono small />}
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
                                <td style={{ padding: "5px 8px", maxWidth: 200, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>{h.hospital_name || h.provider_name || "—"}</td>
                                <td style={{ padding: "5px 8px", color: AL }}>{h.city || "—"}</td>
                                <td style={{ padding: "5px 8px", fontFamily: FM }}>{h.bed_count || h.beds || "—"}</td>
                                <td style={{ padding: "5px 8px", fontFamily: FM }}>{h.medicaid_days ? fmtNum(h.medicaid_days) : "—"}</td>
                                <td style={{ padding: "5px 8px", fontFamily: FM, color: (h.medicaid_day_pct || 0) > 0.25 ? POS : AL }}>
                                  {h.medicaid_day_pct ? `${(h.medicaid_day_pct * 100).toFixed(1)}%` : "—"}
                                </td>
                                <td style={{ padding: "5px 8px", fontFamily: FM }}>{h.cost_to_charge_ratio?.toFixed(3) || "—"}</td>
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
                        {d.scorecard.slice(0, 15).map((s: any, i: number) => (
                          <tr key={i} style={{ background: i % 2 === 0 ? WH : SF, borderBottom: `1px solid ${BD}` }}>
                            <td style={{ padding: "5px 8px", maxWidth: 300, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>
                              {s.measure_name || s.measure_id || "—"}
                            </td>
                            <td style={{ padding: "5px 8px", fontFamily: FM, color: AL }}>{s.data_period || "—"}</td>
                            <td style={{ padding: "5px 8px", fontFamily: FM, fontWeight: 600 }}>
                              {s.value != null ? (typeof s.value === "number" ? s.value.toFixed(1) : s.value) : "—"}
                            </td>
                            <td style={{ padding: "5px 8px", fontFamily: FM, color: AL }}>
                              {s.median != null ? s.median.toFixed(1) : "—"}
                            </td>
                          </tr>
                        ))}
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
                            <td style={{ padding: "5px 8px" }}>{q.measure_name || q.measure_id || "—"}</td>
                            <td style={{ padding: "5px 8px", color: AL }}>{q.domain || "—"}</td>
                            <td style={{ padding: "5px 8px", fontFamily: FM }}>{q.year || "—"}</td>
                            <td style={{ padding: "5px 8px", fontFamily: FM, fontWeight: 600 }}>{q.rate != null ? q.rate.toFixed(1) : "—"}</td>
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
                                {w.occ_title || w.occupation || "—"}
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
                            <td style={{ padding: "5px 8px" }}>{drug.product_name || drug.ndc_description || "—"}</td>
                            <td style={{ padding: "5px 8px", fontFamily: FM, fontWeight: 600, color: ACC }}>{fmtDollars(drug.total_spending || drug.total_amount_reimbursed || 0)}</td>
                            <td style={{ padding: "5px 8px", fontFamily: FM }}>{fmtNum(drug.total_prescriptions || drug.total_rx || 0)}</td>
                            <td style={{ padding: "5px 8px", fontFamily: FM, color: AL }}>{drug.avg_nadac ? `$${drug.avg_nadac.toFixed(2)}` : "—"}</td>
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

            {/* ─── Economic Context ───────────────────────────────────── */}
            <SectionToggle label="Economic Context" open={sections.economic} onClick={() => toggle("economic")} />
            {sections.economic && d.economic.length > 0 && (
              <Card>
                <CH title="Economic Indicators" sub="BLS unemployment, BEA GDP, Census income" />
                {(() => {
                  // Group economic data by indicator
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
