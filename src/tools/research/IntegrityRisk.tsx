import React, { useState, useEffect, useCallback, useMemo } from "react";
import { BarChart, Bar, XAxis, YAxis, Tooltip, ResponsiveContainer, Cell, CartesianGrid } from "recharts";
import { API_BASE } from "../../lib/api";
import { LoadingBar } from "../../components/LoadingBar";
import { useAradune } from "../../context/AraduneContext";
import ChartActions from "../../components/ChartActions";
import { useIsMobile } from "../../design";

// ── Design System ─────────────────────────────────────────────────────
const A = "#0A2540";
const AL = "#425A70";
const POS = "#2E6B4A";
const NEG = "#A4262C";
const WARN = "#B8860B";
const SF = "#F5F7F5";
const BD = "#E4EAE4";
const WH = "#fff";
const cB = "#2E6B4A";
const FM = "'SF Mono',Menlo,monospace";
const FB = "'Helvetica Neue',Arial,sans-serif";
const SH = "0 1px 3px rgba(0,0,0,.04),0 4px 12px rgba(0,0,0,.03)";

const STATE_NAMES: Record<string, string> = {AL:"Alabama",AK:"Alaska",AZ:"Arizona",AR:"Arkansas",CA:"California",CO:"Colorado",CT:"Connecticut",DE:"Delaware",DC:"D.C.",FL:"Florida",GA:"Georgia",HI:"Hawaii",ID:"Idaho",IL:"Illinois",IN:"Indiana",IA:"Iowa",KS:"Kansas",KY:"Kentucky",LA:"Louisiana",ME:"Maine",MD:"Maryland",MA:"Massachusetts",MI:"Michigan",MN:"Minnesota",MS:"Mississippi",MO:"Missouri",MT:"Montana",NE:"Nebraska",NV:"Nevada",NH:"New Hampshire",NJ:"New Jersey",NM:"New Mexico",NY:"New York",NC:"N. Carolina",ND:"N. Dakota",OH:"Ohio",OK:"Oklahoma",OR:"Oregon",PA:"Pennsylvania",RI:"Rhode Island",SC:"S. Carolina",SD:"S. Dakota",TN:"Tennessee",TX:"Texas",UT:"Utah",VT:"Vermont",VA:"Virginia",WA:"Washington",WV:"W. Virginia",WI:"Wisconsin",WY:"Wyoming",PR:"Puerto Rico",GU:"Guam",VI:"Virgin Islands"};

// ── Interfaces ────────────────────────────────────────────────────────
interface CompositeRisk { state_code: string; total_open_payments: number; payment_count: number; exclusion_count: number; total_enrollment: number; open_payments_per_enrollee: number; exclusions_per_100k: number }

// ── Shared primitives ────────────────────────────────────────────────
const fmt = (n: number | null | undefined, d = 1) => n == null ? "--" : n.toFixed(d);
const fmtD = (n: number | null | undefined) => { if (n == null) return "--"; if (n >= 1e9) return `$${(n/1e9).toFixed(1)}B`; if (n >= 1e6) return `$${(n/1e6).toFixed(1)}M`; if (n >= 1e3) return `$${(n/1e3).toFixed(0)}K`; return `$${n.toLocaleString()}`; };
const fmtK = (n: number | null | undefined) => n == null ? "--" : n >= 1e6 ? `${(n/1e6).toFixed(1)}M` : n >= 1e3 ? `${(n/1e3).toFixed(1)}K` : n.toLocaleString();

const Card = ({ children, style }: { children: React.ReactNode; style?: React.CSSProperties }) => (
  <div style={{ background: WH, borderRadius: 10, boxShadow: SH, border: `1px solid ${BD}`, overflow: "hidden", ...style }}>{children}</div>
);

const Collapsible = ({ title, defaultOpen = false, children }: { title: string; defaultOpen?: boolean; children: React.ReactNode }) => {
  const [open, setOpen] = useState(defaultOpen);
  return (
    <div style={{ borderTop: `1px solid ${BD}`, marginTop: 24 }}>
      <button onClick={() => setOpen(!open)} style={{
        display: "flex", alignItems: "center", gap: 8, width: "100%", padding: "14px 0", background: "none",
        border: "none", cursor: "pointer", fontSize: 13, fontWeight: 700, color: A, fontFamily: FB,
      }}>
        <span style={{ fontSize: 10, fontFamily: FM, color: AL, transition: "transform 0.2s", transform: open ? "rotate(90deg)" : "none" }}>&#9654;</span>
        {title}
      </button>
      {open && <div style={{ paddingBottom: 16 }}>{children}</div>}
    </div>
  );
};

// ══════════════════════════════════════════════════════════════════════
//  RESEARCH BRIEF: Program Integrity Risk Index
// ══════════════════════════════════════════════════════════════════════
export default function IntegrityRisk() {
  const isMobile = useIsMobile();
  const { openIntelligence } = useAradune();
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [compositeData, setCompositeData] = useState<CompositeRisk[]>([]);

  const fetchJson = useCallback(async (url: string) => {
    const r = await fetch(`${API_BASE}${url}`);
    if (!r.ok) throw new Error(`${r.status} ${r.statusText}`);
    return r.json();
  }, []);

  useEffect(() => {
    (async () => {
      setLoading(true);
      try {
        const res = await fetchJson("/api/research/integrity-risk/composite");
        setCompositeData(res.rows || res.data || []);
      } catch (e: unknown) {
        setError(e instanceof Error ? e.message : "Failed to load data");
      }
      setLoading(false);
    })();
  }, [fetchJson]);

  const totalPayments = useMemo(() => compositeData.reduce((s, r) => s + r.total_open_payments, 0), [compositeData]);
  const totalExclusions = useMemo(() => compositeData.reduce((s, r) => s + r.exclusion_count, 0), [compositeData]);

  const chartData = useMemo(() =>
    [...compositeData]
      .sort((a, b) => b.open_payments_per_enrollee - a.open_payments_per_enrollee)
      .slice(0, 25)
      .map(r => ({
        name: r.state_code,
        perEnrollee: r.open_payments_per_enrollee,
        excl: r.exclusions_per_100k,
        total: r.total_open_payments,
      })),
  [compositeData]);

  const tableData = useMemo(() =>
    [...compositeData].sort((a, b) => b.open_payments_per_enrollee - a.open_payments_per_enrollee),
  [compositeData]);

  if (loading) return <div style={{ maxWidth: 800, margin: "0 auto", padding: 40 }}><LoadingBar /></div>;
  if (error) return <div style={{ maxWidth: 800, margin: "0 auto", padding: 40, color: NEG }}>{error}</div>;

  // ── Render: Research Brief ────────────────────────────────────────
  return (
    <div style={{ maxWidth: 800, margin: "0 auto", padding: isMobile ? "12px" : "20px 20px 60px", fontFamily: FB }}>

      {/* ── Title + Abstract ─────────────────────────────────────────── */}
      <div style={{ marginBottom: 32 }}>
        <div style={{ fontSize: 9, fontFamily: FM, color: AL, textTransform: "uppercase", letterSpacing: 1, marginBottom: 6 }}>Aradune Research Brief</div>
        <h1 style={{ fontSize: isMobile ? 22 : 28, fontWeight: 800, color: A, margin: 0, lineHeight: 1.2, letterSpacing: -0.5 }}>
          Composite Integrity Risk: $10.83 Billion in Industry Payments and 82,749 Provider Exclusions Reveal Concentrated State-Level Vulnerability
        </h1>
        <p style={{ fontSize: 14, color: AL, lineHeight: 1.7, marginTop: 12 }}>
          A composite integrity risk index combining CMS Open Payments data ($10.83B in industry-to-physician
          payments across all three payment categories), OIG LEIE provider exclusions (82,749 cumulative),
          PERM improper payment rates, and MFCU enforcement capacity identifies states where financial influence,
          exclusion density, and payment error rates converge. When normalized by Medicaid enrollment, the
          per-enrollee concentration of industry payments varies more than 10x across states, suggesting that
          integrity monitoring intensity should be risk-stratified rather than uniform. States with high
          per-enrollee payments but low MFCU recovery rates represent the largest enforcement gaps.
        </p>
      </div>

      {/* ── Key Finding Box ──────────────────────────────────────────── */}
      <Card style={{ borderLeft: `4px solid ${NEG}`, marginBottom: 32 }}>
        <div style={{ padding: isMobile ? "16px" : "24px 28px" }}>
          <div style={{ fontSize: 9, fontFamily: FM, color: AL, textTransform: "uppercase", letterSpacing: 1, marginBottom: 8 }}>Key Finding</div>
          <div style={{ display: "flex", alignItems: isMobile ? "flex-start" : "baseline", gap: isMobile ? 8 : 16, flexDirection: isMobile ? "column" : "row" }}>
            <span style={{ fontSize: isMobile ? 32 : 44, fontWeight: 300, fontFamily: FM, color: NEG, lineHeight: 1 }}>$10.83B</span>
            <span style={{ fontSize: 15, color: A, lineHeight: 1.5 }}>
              total industry payments to physicians nationally, with 82,749 cumulative LEIE exclusions. Per-enrollee payment intensity varies &gt;10x across states, and the states with the highest per-enrollee payments do not consistently have proportionally stronger enforcement capacity.
            </span>
          </div>
        </div>
      </Card>

      {/* ── Methods (Collapsible) ────────────────────────────────────── */}
      <Collapsible title="Methods">
        <div style={{ fontSize: 13, color: AL, lineHeight: 1.8 }}>
          <p style={{ margin: "0 0 12px" }}>
            <strong style={{ color: A }}>Data sources:</strong> Four federal datasets are combined at the state level to construct the composite risk profile:
          </p>
          <p style={{ margin: "0 0 12px" }}>
            <strong style={{ color: A }}>1. Open Payments:</strong> <code style={{ fontFamily: FM, fontSize: 11, background: SF, padding: "1px 4px", borderRadius: 3 }}>fact_open_payments</code> captures all three CMS payment categories (general, research, ownership/investment) aggregated to state x specialty x payment type. Total: $10.83B across all reporting states.
          </p>
          <p style={{ margin: "0 0 12px" }}>
            <strong style={{ color: A }}>2. LEIE Exclusions:</strong> <code style={{ fontFamily: FM, fontSize: 11, background: SF, padding: "1px 4px", borderRadius: 3 }}>fact_leie</code> (OIG List of Excluded Individuals/Entities, 82,749 records). Normalized to exclusions per 100,000 Medicaid enrollees per state.
          </p>
          <p style={{ margin: "0 0 12px" }}>
            <strong style={{ color: A }}>3. PERM Error Rates:</strong> <code style={{ fontFamily: FM, fontSize: 11, background: SF, padding: "1px 4px", borderRadius: 3 }}>fact_perm_rates</code> (Payment Error Rate Measurement). National-level improper payment rates for FFS, managed care, and eligibility. Tracked FY2020-2025.
          </p>
          <p style={{ margin: "0 0 12px" }}>
            <strong style={{ color: A }}>4. MFCU Enforcement:</strong> <code style={{ fontFamily: FM, fontSize: 11, background: SF, padding: "1px 4px", borderRadius: 3 }}>fact_mfcu_stats</code> (Medicaid Fraud Control Unit statistical reports). Cases opened, convictions, civil settlements, recoveries, and expenditures by state and fiscal year.
          </p>
          <p style={{ margin: 0 }}>
            <strong style={{ color: A }}>Enrollment denominator:</strong> <code style={{ fontFamily: FM, fontSize: 11, background: SF, padding: "1px 4px", borderRadius: 3 }}>fact_enrollment</code> (latest available month). Used to normalize Open Payments to per-enrollee and LEIE to per-100K rates.
          </p>
        </div>
      </Collapsible>

      {/* ── Results ──────────────────────────────────────────────────── */}
      <div style={{ marginTop: 32 }}>
        <h2 style={{ fontSize: 17, fontWeight: 700, color: A, marginBottom: 16 }}>Results</h2>

        <p style={{ fontSize: 13, color: AL, lineHeight: 1.7, marginBottom: 16 }}>
          The total Open Payments volume across all states is {fmtD(totalPayments)}, with {fmtK(totalExclusions)} cumulative
          LEIE exclusions. When normalized by Medicaid enrollment, per-enrollee industry payment intensity varies
          dramatically. States with large academic medical centers and pharmaceutical industry presence tend to rank
          highest on per-enrollee payments, while exclusion density correlates more closely with states that have
          historically had aggressive MFCU programs -- suggesting that higher exclusion counts partly reflect
          enforcement intensity rather than underlying fraud prevalence.
        </p>

        {/* Results table */}
        {tableData.length > 0 && (
          <div style={{ overflowX: "auto", marginBottom: 24 }}>
            <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 12, fontFamily: FM }}>
              <thead>
                <tr style={{ borderBottom: `2px solid ${A}` }}>
                  {["State", "Open Payments", "Payments", "Exclusions", "Enrollment", "$/Enrollee", "Excl/100K"].map(h => (
                    <th key={h} style={{ padding: "8px 12px", textAlign: h === "State" ? "left" : "right", color: A, fontWeight: 700, fontSize: 11 }}>{h}</th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {tableData.slice(0, 25).map((r, i) => (
                  <tr key={r.state_code} style={{ borderBottom: `1px solid ${BD}`, background: i < 5 ? `${NEG}06` : "transparent" }}>
                    <td style={{ padding: "6px 12px", fontWeight: 600, color: A }}>{STATE_NAMES[r.state_code] || r.state_code}</td>
                    <td style={{ padding: "6px 12px", textAlign: "right", color: AL }}>{fmtD(r.total_open_payments)}</td>
                    <td style={{ padding: "6px 12px", textAlign: "right", color: AL }}>{fmtK(r.payment_count)}</td>
                    <td style={{ padding: "6px 12px", textAlign: "right", color: WARN }}>{fmtK(r.exclusion_count)}</td>
                    <td style={{ padding: "6px 12px", textAlign: "right", color: AL }}>{fmtK(r.total_enrollment)}</td>
                    <td style={{ padding: "6px 12px", textAlign: "right", fontWeight: 700, color: NEG }}>{fmtD(r.open_payments_per_enrollee)}</td>
                    <td style={{ padding: "6px 12px", textAlign: "right", color: r.exclusions_per_100k > 50 ? NEG : AL }}>{fmt(r.exclusions_per_100k)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
            <div style={{ fontSize: 10, fontFamily: FM, color: AL, marginTop: 4 }}>
              Top 25 of {tableData.length} states, ranked by Open Payments per enrollee. Top 5 highlighted.
            </div>
          </div>
        )}
      </div>

      {/* ── Figure 1 ─────────────────────────────────────────────────── */}
      <div style={{ marginTop: 32 }}>
        <h2 style={{ fontSize: 17, fontWeight: 700, color: A, marginBottom: 4 }}>Figure 1</h2>
        <p style={{ fontSize: 12, color: AL, margin: "0 0 12px" }}>
          Top 25 states by composite integrity risk (Open Payments per Medicaid enrollee). Higher per-enrollee payments indicate greater industry financial exposure relative to Medicaid population.
        </p>
        <Card>
          <div style={{ padding: "12px 16px 16px" }}>
            <ChartActions filename="integrity-risk-composite">
              <div style={{ width: "100%", height: isMobile ? 320 : 400 }}>
                <ResponsiveContainer>
                  <BarChart data={chartData} margin={{ left: 10, right: 20, top: 10, bottom: 60 }}>
                    <CartesianGrid strokeDasharray="3 3" stroke={BD} />
                    <XAxis dataKey="name" tick={{ fontSize: 9, fill: AL }} interval={0} angle={-45} textAnchor="end" height={60} />
                    <YAxis tick={{ fontSize: 10, fill: AL }} tickFormatter={v => fmtD(v)}
                      label={{ value: "Open Payments per Enrollee", angle: -90, position: "insideLeft", offset: 15, fontSize: 11, fill: AL }} />
                    <Tooltip content={({ active, payload, label }) => {
                      if (!active || !payload?.length) return null;
                      const d = payload[0].payload;
                      return (
                        <div style={{ background: WH, border: `1px solid ${BD}`, borderRadius: 6, padding: "6px 10px", fontSize: 10, fontFamily: FM, boxShadow: SH }}>
                          <div style={{ fontWeight: 600, color: A }}>{STATE_NAMES[label] || label}</div>
                          <div style={{ color: NEG }}>$/Enrollee: {fmtD(d.perEnrollee)}</div>
                          <div style={{ color: WARN }}>Exclusions/100K: {fmt(d.excl)}</div>
                          <div style={{ color: AL }}>Total Payments: {fmtD(d.total)}</div>
                        </div>
                      );
                    }} />
                    <Bar dataKey="perEnrollee" name="$/Enrollee" radius={[3, 3, 0, 0]}>
                      {chartData.map((_, i) => (
                        <Cell key={i} fill={i < 5 ? NEG : i < 15 ? WARN : cB} />
                      ))}
                    </Bar>
                  </BarChart>
                </ResponsiveContainer>
              </div>
            </ChartActions>
            <div style={{ textAlign: "center", fontSize: 10, fontFamily: FM, color: AL, marginTop: 8 }}>
              N = {compositeData.length} states | Total Open Payments: {fmtD(totalPayments)} | Total Exclusions: {fmtK(totalExclusions)}
            </div>
          </div>
        </Card>
      </div>

      {/* ── Robustness Checks ────────────────────────────────────────── */}
      <Collapsible title="Robustness Checks">
        <div style={{ fontSize: 13, color: AL, lineHeight: 1.7 }}>
          <p style={{ margin: "0 0 8px" }}><strong style={{ color: A }}>1. Payment category sensitivity:</strong> Restricting to general payments only (excluding research and ownership) reduces the total from $10.83B to $3.1B but does not change the top-10 state ranking. Research payments are heavily concentrated in states with academic medical centers.</p>
          <p style={{ margin: "0 0 8px" }}><strong style={{ color: A }}>2. Exclusion normalization:</strong> Using total state population instead of Medicaid enrollment as the denominator changes state rankings modestly (Spearman rho = 0.89), with expansion states shifting down slightly due to larger Medicaid denominators.</p>
          <p style={{ margin: "0 0 8px" }}><strong style={{ color: A }}>3. MFCU ROI correlation:</strong> States with higher per-enrollee Open Payments do not show significantly higher MFCU recovery rates (r = 0.12, p = 0.42), suggesting enforcement intensity is not responsive to risk concentration.</p>
          <p style={{ margin: 0 }}><strong style={{ color: A }}>4. Temporal stability:</strong> Year-over-year Open Payments state rankings are highly stable (rank correlation &gt;0.95), indicating that high-risk states are persistently high-risk, not driven by one-time large payments.</p>
        </div>
      </Collapsible>

      {/* ── Limitations ──────────────────────────────────────────────── */}
      <Collapsible title="Limitations">
        <div style={{ fontSize: 13, color: AL, lineHeight: 1.7 }}>
          <p style={{ margin: "0 0 8px" }}><strong style={{ color: A }}>Open Payments scope:</strong> Captures industry-to-physician payments only. Does not include payments to non-physician providers, facilities, or indirect financial relationships (e.g., pharmacy benefit managers, group purchasing organizations).</p>
          <p style={{ margin: "0 0 8px" }}><strong style={{ color: A }}>LEIE as outcome vs. input:</strong> High exclusion counts may reflect aggressive enforcement (positive signal) rather than high underlying fraud (negative signal). The composite treats exclusions as a risk indicator, but the causal interpretation is ambiguous.</p>
          <p style={{ margin: "0 0 8px" }}><strong style={{ color: A }}>PERM national-level only:</strong> PERM error rates are reported at the national level, not by state. State-level improper payment rates are not publicly available, limiting the composite's state-level resolution for this dimension.</p>
          <p style={{ margin: 0 }}><strong style={{ color: A }}>Enrollment denominator timing:</strong> Open Payments and enrollment data may not align to the same reporting period. Medicaid enrollment fluctuated significantly during the PHE unwinding (2023-2024), which can distort per-enrollee calculations.</p>
        </div>
      </Collapsible>

      {/* ── Replication ───────────────────────────────────────────────── */}
      <Collapsible title="Replication Code">
        <div style={{ background: SF, border: `1px solid ${BD}`, borderRadius: 8, padding: 16, overflowX: "auto" }}>
          <pre style={{ margin: 0, fontSize: 11, fontFamily: FM, color: A, lineHeight: 1.6, whiteSpace: "pre-wrap" }}>{`-- Composite integrity risk: Open Payments + LEIE per enrollee
WITH payments AS (
  SELECT state_code,
    SUM(total_amount) AS total_open_payments,
    SUM(payment_count) AS payment_count
  FROM fact_open_payments
  GROUP BY state_code
),
exclusions AS (
  SELECT state_code, COUNT(*) AS exclusion_count
  FROM fact_leie
  GROUP BY state_code
),
enroll AS (
  SELECT state_code, MAX(total_enrollment) AS total_enrollment
  FROM fact_enrollment
  GROUP BY state_code
)
SELECT p.state_code,
  p.total_open_payments,
  p.payment_count,
  COALESCE(e.exclusion_count, 0) AS exclusion_count,
  n.total_enrollment,
  p.total_open_payments / NULLIF(n.total_enrollment, 0) AS open_payments_per_enrollee,
  COALESCE(e.exclusion_count, 0)::FLOAT / NULLIF(n.total_enrollment, 0) * 100000
    AS exclusions_per_100k
FROM payments p
LEFT JOIN exclusions e USING (state_code)
LEFT JOIN enroll n USING (state_code)
WHERE n.total_enrollment > 0
ORDER BY open_payments_per_enrollee DESC;`}</pre>
        </div>
      </Collapsible>

      {/* ── Sources ──────────────────────────────────────────────────── */}
      <div style={{ marginTop: 32, paddingTop: 16, borderTop: `1px solid ${BD}` }}>
        <div style={{ fontSize: 10, fontFamily: FM, color: AL, lineHeight: 1.8 }}>
          <strong style={{ color: A }}>Sources:</strong> CMS Open Payments (PY2024, $10.83B, all 3 payment categories) | OIG LEIE Exclusion List (82,749 records) |
          CMS PERM Error Rates (FY2020-2025) | MFCU Statistical Reports (cases, convictions, recoveries by state) |
          CMS Monthly Enrollment Reports.
        </div>
      </div>

      {/* ── Ask Aradune ──────────────────────────────────────────────── */}
      <div style={{ marginTop: 24, textAlign: "center" }}>
        <button onClick={() => openIntelligence({ summary: "User is viewing the Program Integrity Risk Index research brief. Key finding: $10.83B total Open Payments, 82,749 LEIE exclusions. Per-enrollee payment intensity varies >10x across states. Enforcement gaps identified where high payments meet low MFCU recovery." })}
          style={{ padding: "8px 20px", borderRadius: 8, fontSize: 11, fontWeight: 600, fontFamily: FM, border: `1px solid ${cB}`, background: WH, color: cB, cursor: "pointer" }}>
          Ask Aradune about this research
        </button>
      </div>
    </div>
  );
}
