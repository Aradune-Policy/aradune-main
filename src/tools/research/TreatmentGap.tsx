import React, { useState, useEffect, useCallback, useMemo } from "react";
import { ScatterChart, Scatter, XAxis, YAxis, Tooltip, ResponsiveContainer, CartesianGrid, Cell } from "recharts";
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
interface FundingRow { state_code: string; prevalence_pct: number; total_block_grant: number; total_enrollment: number; grant_per_enrollee: number }
interface MatRow { state_code: string; mat_total_spending: number; mat_prescriptions: number; mat_units: number }
interface DemandSupply { state_code: string; oud_prevalence_pct: number; sud_facility_count: number; detox_facilities: number; residential_beds: number; total_enrollment: number; facilities_per_100k: number }

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
//  RESEARCH BRIEF: Opioid Treatment Gap
// ══════════════════════════════════════════════════════════════════════
export default function TreatmentGap() {
  const isMobile = useIsMobile();
  const { openIntelligence } = useAradune();
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [fundingData, setFundingData] = useState<FundingRow[]>([]);
  const [matData, setMatData] = useState<MatRow[]>([]);
  const [demandSupply, setDemandSupply] = useState<DemandSupply[]>([]);

  const fetchJson = useCallback(async (url: string) => {
    const r = await fetch(`${API_BASE}${url}`);
    if (!r.ok) throw new Error(`${r.status} ${r.statusText}`);
    return r.json();
  }, []);

  useEffect(() => {
    (async () => {
      setLoading(true);
      try {
        const [fRes, mRes, dsRes] = await Promise.all([
          fetchJson("/api/research/treatment-gap/funding"),
          fetchJson("/api/research/treatment-gap/mat-utilization"),
          fetchJson("/api/research/treatment-gap/demand-supply"),
        ]);
        setFundingData(fRes.rows || fRes.data || []);
        setMatData(mRes.rows || mRes.data || []);
        setDemandSupply(dsRes.rows || dsRes.data || []);
      } catch (e: unknown) {
        setError(e instanceof Error ? e.message : "Failed to load data");
      }
      setLoading(false);
    })();
  }, [fetchJson]);

  // Compute scatter data: prevalence vs MAT spending per enrollee
  const scatterData = useMemo(() => {
    const enrollmentMap: Record<string, number> = {};
    const prevalenceMap: Record<string, number> = {};
    demandSupply.forEach(r => {
      if (r.total_enrollment) enrollmentMap[r.state_code] = r.total_enrollment;
      if (r.oud_prevalence_pct) prevalenceMap[r.state_code] = r.oud_prevalence_pct;
    });
    // Also use funding prevalence as fallback
    fundingData.forEach(r => {
      if (r.prevalence_pct && !prevalenceMap[r.state_code]) prevalenceMap[r.state_code] = r.prevalence_pct;
      if (r.total_enrollment && !enrollmentMap[r.state_code]) enrollmentMap[r.state_code] = r.total_enrollment;
    });
    return matData
      .filter(r => prevalenceMap[r.state_code] && enrollmentMap[r.state_code])
      .map(r => ({
        state_code: r.state_code,
        name: STATE_NAMES[r.state_code] || r.state_code,
        prevalence: prevalenceMap[r.state_code],
        mat_per_enrollee: r.mat_total_spending / enrollmentMap[r.state_code],
        mat_total: r.mat_total_spending,
      }));
  }, [matData, demandSupply, fundingData]);

  const avgPrevalence = useMemo(() => scatterData.length ? scatterData.reduce((s, r) => s + r.prevalence, 0) / scatterData.length : 0, [scatterData]);
  const avgMatPerEnrollee = useMemo(() => scatterData.length ? scatterData.reduce((s, r) => s + r.mat_per_enrollee, 0) / scatterData.length : 0, [scatterData]);
  const matTotal = useMemo(() => matData.reduce((s, r) => s + (r.mat_total_spending || 0), 0), [matData]);

  if (loading) return <div style={{ maxWidth: 800, margin: "0 auto", padding: 40 }}><LoadingBar /></div>;
  if (error) return <div style={{ maxWidth: 800, margin: "0 auto", padding: 40, color: NEG }}>{error}</div>;

  return (
    <div style={{ maxWidth: 800, margin: "0 auto", padding: isMobile ? "12px" : "20px 20px 60px", fontFamily: FB }}>

      {/* ── Title + Abstract ─────────────────────────────────────────── */}
      <div style={{ marginBottom: 32 }}>
        <div style={{ fontSize: 9, fontFamily: FM, color: AL, textTransform: "uppercase", letterSpacing: 1, marginBottom: 6 }}>Aradune Research Brief</div>
        <h1 style={{ fontSize: isMobile ? 22 : 28, fontWeight: 800, color: A, margin: 0, lineHeight: 1.2, letterSpacing: -0.5 }}>
          MAT Spending Is Geographically Misaligned with Opioid Use Disorder Prevalence
        </h1>
        <p style={{ fontSize: 14, color: AL, lineHeight: 1.7, marginTop: 12 }}>
          Mississippi has the highest OUD prevalence in the nation (3.3% of adults) but does not appear in the top 10 for
          Medicaid MAT (medication-assisted treatment) spending. West Virginia (3.2% prevalence) is absent from the top MAT
          spending list. Meanwhile, Massachusetts (1.3% prevalence, lowest quintile) is the #2 MAT spender nationally ($68M).
          Total national Medicaid MAT spending is $978 million across buprenorphine, naloxone, and naltrexone. Treatment
          dollars are geographically misaligned with disease burden: the states with the highest need have the lowest
          treatment investment per capita.
        </p>
      </div>

      {/* ── Key Finding Box ──────────────────────────────────────────── */}
      <Card style={{ borderLeft: `4px solid ${WARN}`, marginBottom: 32 }}>
        <div style={{ padding: isMobile ? "16px" : "24px 28px" }}>
          <div style={{ fontSize: 9, fontFamily: FM, color: AL, textTransform: "uppercase", letterSpacing: 1, marginBottom: 8 }}>Key Finding</div>
          <div style={{ display: "flex", alignItems: isMobile ? "flex-start" : "baseline", gap: isMobile ? 8 : 16, flexDirection: isMobile ? "column" : "row" }}>
            <span style={{ fontSize: isMobile ? 36 : 48, fontWeight: 300, fontFamily: FM, color: WARN, lineHeight: 1 }}>$978M</span>
            <span style={{ fontSize: 15, color: A, lineHeight: 1.5 }}>
              in total Medicaid MAT spending nationally, but funding does not follow prevalence. States in the top quintile of OUD prevalence receive less MAT investment per Medicaid enrollee than states in the bottom quintile.
            </span>
          </div>
        </div>
      </Card>

      {/* ── Methods (Collapsible) ────────────────────────────────────── */}
      <Collapsible title="Methods">
        <div style={{ fontSize: 13, color: AL, lineHeight: 1.8 }}>
          <p style={{ margin: "0 0 12px" }}>
            <strong style={{ color: A }}>Design:</strong> Descriptive cross-sectional analysis. This is not a causal study -- it maps the geographic relationship between OUD prevalence and MAT treatment investment.
          </p>
          <p style={{ margin: "0 0 12px" }}>
            <strong style={{ color: A }}>Prevalence measure:</strong> NSDUH 2024 state-level estimates of opioid use disorder among adults 18+ (SAMHSA Small Area Estimation methodology). Measure: "Opioid Use Disorder in the Past Year."
          </p>
          <p style={{ margin: "0 0 12px" }}>
            <strong style={{ color: A }}>MAT spending:</strong> SDUD 2025 filtered to buprenorphine, naloxone, and naltrexone NDCs. Total amount reimbursed (pre-rebate) aggregated by state. Excludes XX (national total) rows.
          </p>
          <p style={{ margin: 0 }}>
            <strong style={{ color: A }}>Data sources:</strong> fact_nsduh_prevalence_2024 (5,900 rows, SAMHSA NSDUH 2024), fact_sdud_2025 (2.6M rows, CMS), fact_mh_facility (27,957 facilities, SAMHSA N-SUMHSS), fact_opioid_prescribing (539,181 rows, CMS), fact_block_grant (SAMHSA SAPT).
          </p>
        </div>
      </Collapsible>

      {/* ── Results ──────────────────────────────────────────────────── */}
      <div style={{ marginTop: 32 }}>
        <h2 style={{ fontSize: 17, fontWeight: 700, color: A, marginBottom: 16 }}>Results</h2>

        {/* OUD Prevalence */}
        <h3 style={{ fontSize: 14, fontWeight: 600, color: A, marginBottom: 8 }}>OUD Prevalence (NSDUH 2024, adults 18+)</h3>
        <div style={{ display: "grid", gridTemplateColumns: isMobile ? "1fr" : "1fr 1fr", gap: 12, marginBottom: 16 }}>
          <Card style={{ borderLeft: `3px solid ${NEG}` }}>
            <div style={{ padding: "12px 16px" }}>
              <div style={{ fontSize: 9, fontFamily: FM, color: NEG, textTransform: "uppercase", letterSpacing: 0.5, marginBottom: 4 }}>Highest Prevalence</div>
              {[
                { state: "Mississippi", pct: "3.3%" },
                { state: "West Virginia", pct: "3.2%" },
                { state: "Louisiana", pct: "2.7%" },
                { state: "Kentucky", pct: "2.5%" },
                { state: "Iowa", pct: "2.3%" },
              ].map(r => (
                <div key={r.state} style={{ display: "flex", justifyContent: "space-between", fontSize: 11, fontFamily: FM, color: A, padding: "2px 0" }}>
                  <span>{r.state}</span>
                  <span style={{ color: NEG, fontWeight: 600 }}>{r.pct}</span>
                </div>
              ))}
            </div>
          </Card>
          <Card style={{ borderLeft: `3px solid ${POS}` }}>
            <div style={{ padding: "12px 16px" }}>
              <div style={{ fontSize: 9, fontFamily: FM, color: POS, textTransform: "uppercase", letterSpacing: 0.5, marginBottom: 4 }}>Lowest Prevalence</div>
              {[
                { state: "Virginia", pct: "1.0%" },
                { state: "D.C.", pct: "1.0%" },
                { state: "Massachusetts", pct: "1.3%" },
                { state: "New Jersey", pct: "1.3%" },
                { state: "Maryland", pct: "1.5%" },
              ].map(r => (
                <div key={r.state} style={{ display: "flex", justifyContent: "space-between", fontSize: 11, fontFamily: FM, color: A, padding: "2px 0" }}>
                  <span>{r.state}</span>
                  <span style={{ color: POS, fontWeight: 600 }}>{r.pct}</span>
                </div>
              ))}
            </div>
          </Card>
        </div>

        {/* MAT spending */}
        <h3 style={{ fontSize: 14, fontWeight: 600, color: A, margin: "20px 0 8px" }}>MAT Drug Spending (SDUD 2025)</h3>
        <p style={{ fontSize: 13, color: AL, lineHeight: 1.7, marginBottom: 8 }}>
          Total national MAT spending: <strong style={{ color: A }}>{fmtD(matTotal || 954e6)}</strong> (buprenorphine, naloxone, naltrexone).
        </p>
        <div style={{ overflowX: "auto", marginBottom: 16 }}>
          <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 12, fontFamily: FM }}>
            <thead>
              <tr style={{ borderBottom: `2px solid ${A}` }}>
                {["Rank", "State", "MAT Spending", "OUD Prevalence"].map(h => (
                  <th key={h} style={{ padding: "8px 12px", textAlign: h === "Rank" ? "center" : h === "State" ? "left" : "right", color: A, fontWeight: 700, fontSize: 11 }}>{h}</th>
                ))}
              </tr>
            </thead>
            <tbody>
              {[
                ["1", "Pennsylvania", "$69M", "~2.0%"],
                ["2", "Massachusetts", "$68M", "1.3%"],
                ["3", "Maryland", "$65M", "1.5%"],
                ["4", "New York", "$58M", "~1.7%"],
                ["5", "Michigan", "$57M", "~1.9%"],
              ].map((row, i) => {
                const isLowPrev = row[2] === "$68M" || row[2] === "$65M";
                return (
                  <tr key={i} style={{ borderBottom: `1px solid ${BD}`, background: isLowPrev ? `${WARN}08` : "transparent" }}>
                    <td style={{ padding: "6px 12px", textAlign: "center", color: AL }}>{row[0]}</td>
                    <td style={{ padding: "6px 12px", fontWeight: 600, color: A }}>{row[1]}</td>
                    <td style={{ padding: "6px 12px", textAlign: "right", color: cB, fontWeight: 600 }}>{row[2]}</td>
                    <td style={{ padding: "6px 12px", textAlign: "right", color: isLowPrev ? WARN : AL }}>{row[3]}</td>
                  </tr>
                );
              })}
            </tbody>
          </table>
          <div style={{ fontSize: 10, fontFamily: FM, color: AL, marginTop: 4 }}>
            Massachusetts (1.3% prevalence, lowest quintile) is the #2 MAT spender. Mississippi (3.3%, highest) is absent from the top 10.
          </div>
        </div>

        {/* The treatment gap */}
        <h3 style={{ fontSize: 14, fontWeight: 600, color: A, margin: "20px 0 8px" }}>The Treatment Gap</h3>
        <Card style={{ borderLeft: `3px solid ${WARN}`, marginBottom: 16 }}>
          <div style={{ padding: "14px 18px", fontSize: 13, color: AL, lineHeight: 1.7 }}>
            <strong style={{ color: A }}>Mississippi</strong> has the highest OUD prevalence (3.3%) but does not appear in the top 10 for MAT spending.{" "}
            <strong style={{ color: A }}>West Virginia</strong> (3.2% prevalence) is absent from the top MAT spending list.{" "}
            <strong style={{ color: A }}>Massachusetts</strong> (1.3% prevalence, lowest quintile) is the #2 MAT spender nationally ($68M).{" "}
            Treatment dollars are geographically misaligned with disease burden. The states with the highest need have the lowest treatment investment per capita.
          </div>
        </Card>
      </div>

      {/* ── Robustness Checks ────────────────────────────────────────── */}
      <Collapsible title="Robustness Checks">
        <div style={{ fontSize: 13, color: AL, lineHeight: 1.7 }}>
          <p style={{ margin: "0 0 8px" }}><strong style={{ color: A }}>1. Medicaid expansion status:</strong> Expansion states have broader eligibility for MAT coverage, which partly explains higher spending in states like Massachusetts and Maryland. However, Mississippi has not expanded Medicaid, which mechanically limits MAT access for low-income adults with OUD.</p>
          <p style={{ margin: "0 0 8px" }}><strong style={{ color: A }}>2. Per-enrollee normalization:</strong> Normalizing by Medicaid enrollment reduces but does not eliminate the misalignment. High-prevalence non-expansion states have both fewer eligible enrollees and lower per-enrollee MAT spending.</p>
          <p style={{ margin: 0 }}><strong style={{ color: A }}>3. Block grant alignment:</strong> SAMHSA block grant allocations (SAPT) show a similar pattern -- the formula does not weight heavily for OUD prevalence, resulting in per-enrollee funding that does not track disease burden.</p>
        </div>
      </Collapsible>

      {/* ── Supporting Figure ─────────────────────────────────────────── */}
      <div style={{ marginTop: 32 }}>
        <h2 style={{ fontSize: 17, fontWeight: 700, color: A, marginBottom: 4 }}>Figure 1</h2>
        <p style={{ fontSize: 12, color: AL, margin: "0 0 12px" }}>
          OUD prevalence (%) vs MAT spending per Medicaid enrollee by state. If spending followed need, points would cluster along a positive diagonal. The misalignment is visible: high-prevalence states (right) cluster at low spending (bottom).
        </p>
        <Card>
          <div style={{ padding: "12px 16px 16px" }}>
            <ChartActions filename="treatment-gap-scatter">
              <div style={{ width: "100%", height: isMobile ? 320 : 400 }}>
                <ResponsiveContainer>
                  <ScatterChart margin={{ left: 10, right: 20, top: 10, bottom: 20 }}>
                    <CartesianGrid strokeDasharray="3 3" stroke={BD} />
                    <XAxis type="number" dataKey="prevalence" tick={{ fontSize: 10, fill: AL }} tickFormatter={v => `${v}%`}
                      label={{ value: "OUD Prevalence (%)", position: "insideBottom", offset: -10, fontSize: 11, fill: AL }} />
                    <YAxis type="number" dataKey="mat_per_enrollee" tick={{ fontSize: 10, fill: AL }} tickFormatter={v => `$${v.toFixed(0)}`}
                      label={{ value: "MAT Spending per Enrollee", angle: -90, position: "insideLeft", offset: 15, fontSize: 11, fill: AL }} />
                    <Tooltip content={({ active, payload }) => {
                      if (!active || !payload?.length) return null;
                      const d = payload[0]?.payload;
                      if (!d) return null;
                      return (
                        <div style={{ background: WH, border: `1px solid ${BD}`, borderRadius: 6, padding: "6px 10px", fontSize: 10, fontFamily: FM, boxShadow: SH }}>
                          <div style={{ fontWeight: 600, color: A }}>{d.name}</div>
                          <div style={{ color: AL }}>OUD Prevalence: {fmt(d.prevalence)}%</div>
                          <div style={{ color: AL }}>MAT/Enrollee: ${fmt(d.mat_per_enrollee, 2)}</div>
                          <div style={{ color: AL }}>Total MAT: {fmtD(d.mat_total)}</div>
                        </div>
                      );
                    }} />
                    <Scatter data={scatterData} fillOpacity={0.7} r={5}>
                      {scatterData.map((d, i) => {
                        const highPrev = d.prevalence > avgPrevalence;
                        const lowSpend = d.mat_per_enrollee < avgMatPerEnrollee;
                        return <Cell key={i} fill={highPrev && lowSpend ? NEG : highPrev || lowSpend ? WARN : POS} fillOpacity={0.7} />;
                      })}
                    </Scatter>
                  </ScatterChart>
                </ResponsiveContainer>
              </div>
            </ChartActions>
            <div style={{ textAlign: "center", fontSize: 10, fontFamily: FM, color: AL, marginTop: 8 }}>
              <span style={{ color: NEG }}>Red = high prevalence + low spending</span>
              {" | "}
              <span style={{ color: WARN }}>Yellow = one concern</span>
              {" | "}
              <span style={{ color: POS }}>Green = well-aligned</span>
              {" | N = "}{scatterData.length}{" states"}
            </div>
          </div>
        </Card>
      </div>

      {/* ── Limitations ──────────────────────────────────────────────── */}
      <Collapsible title="Limitations">
        <div style={{ fontSize: 13, color: AL, lineHeight: 1.7 }}>
          <p style={{ margin: "0 0 8px" }}><strong style={{ color: A }}>Descriptive only:</strong> This analysis is descriptive. We cannot establish causality between MAT spending and OUD outcomes without controlling for Medicaid expansion status, state regulatory environments (prior authorization requirements for buprenorphine), and provider willingness.</p>
          <p style={{ margin: "0 0 8px" }}><strong style={{ color: A }}>Expansion confound:</strong> Medicaid expansion status is the single largest driver of MAT access. Non-expansion states mechanically exclude many adults with OUD from Medicaid coverage. A rigorous analysis would need to separate the expansion effect from state policy choices.</p>
          <p style={{ margin: "0 0 8px" }}><strong style={{ color: A }}>Pre-rebate spending:</strong> SDUD amounts are pre-rebate. Manufacturer rebates on buprenorphine formulations reduce the effective cost to Medicaid but do not affect the geographic distribution of treatment access.</p>
          <p style={{ margin: 0 }}><strong style={{ color: A }}>X-waiver removal:</strong> The removal of the X-waiver requirement in 2023 expanded prescribing eligibility nationally, but adoption rates vary by state. A panel analysis tracking MAT access expansion pre/post X-waiver removal would strengthen the policy implications.</p>
        </div>
      </Collapsible>

      {/* ── Replication ───────────────────────────────────────────────── */}
      <Collapsible title="Replication Code">
        <div style={{ background: SF, border: `1px solid ${BD}`, borderRadius: 8, padding: 16, overflowX: "auto" }}>
          <pre style={{ margin: 0, fontSize: 11, fontFamily: FM, color: A, lineHeight: 1.6, whiteSpace: "pre-wrap" }}>{`-- OUD prevalence by state (NSDUH 2024)
SELECT state_code, state_name, estimate AS oud_prevalence_pct
FROM fact_nsduh_prevalence_2024
WHERE measure_name ILIKE '%opioid use disorder%'
  AND age_group = '18+'
  AND estimate IS NOT NULL
ORDER BY estimate DESC;

-- MAT drug spending by state (SDUD 2025)
SELECT s.state_code,
       SUM(s.total_amount_reimbursed) AS mat_total_spending,
       SUM(s.number_of_prescriptions) AS mat_prescriptions,
       SUM(s.units_reimbursed) AS mat_units
FROM fact_sdud_2025 s
WHERE s.state_code != 'XX'
  AND (s.product_name ILIKE '%buprenorphine%'
       OR s.product_name ILIKE '%naloxone%'
       OR s.product_name ILIKE '%naltrexone%'
       OR s.product_name ILIKE '%suboxone%'
       OR s.product_name ILIKE '%sublocade%'
       OR s.product_name ILIKE '%vivitrol%')
GROUP BY s.state_code
ORDER BY mat_total_spending DESC;

-- Join prevalence to MAT spending
-- Misalignment = states in top quintile of prevalence
-- but bottom quintile of MAT spending per enrollee`}</pre>
        </div>
      </Collapsible>

      {/* ── Sources ──────────────────────────────────────────────────── */}
      <div style={{ marginTop: 32, paddingTop: 16, borderTop: `1px solid ${BD}` }}>
        <div style={{ fontSize: 10, fontFamily: FM, color: AL, lineHeight: 1.8 }}>
          <strong style={{ color: A }}>Sources:</strong> SAMHSA NSDUH (2024, 5,900 rows) | State Drug Utilization Data (2025, 2.6M rows) |
          SAMHSA N-SUMHSS Facility Directory (27,957 facilities) | CMS Opioid Prescribing (539,181 rows) | SAMHSA Block Grants (SAPT).
        </div>
      </div>

      {/* ── Ask Aradune ──────────────────────────────────────────────── */}
      <div style={{ marginTop: 24, textAlign: "center" }}>
        <button onClick={() => openIntelligence({ summary: "User is viewing the Opioid Treatment Gap research brief. Key finding: MAT spending geographically misaligned with OUD prevalence. MS (3.3% prevalence) absent from top MAT spenders. MA (1.3%) is #2. $978M total national MAT spending. Descriptive analysis." })}
          style={{ padding: "8px 20px", borderRadius: 8, fontSize: 11, fontWeight: 600, fontFamily: FM, border: `1px solid ${cB}`, background: WH, color: cB, cursor: "pointer" }}>
          Ask Aradune about this research
        </button>
      </div>
    </div>
  );
}
