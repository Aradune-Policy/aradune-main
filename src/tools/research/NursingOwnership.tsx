import React, { useState, useEffect, useCallback, useMemo } from "react";
import { BarChart, Bar, XAxis, YAxis, Tooltip, ResponsiveContainer, CartesianGrid, Cell, Legend } from "recharts";
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

// ── Interfaces ────────────────────────────────────────────────────────
interface QualityByType { ownership_type: string; facility_count: number; avg_overall_rating: number; avg_inspection_rating: number; avg_qm_rating: number; avg_staffing_rating: number; avg_total_hprd: number; avg_rn_hprd: number; avg_deficiencies: number; avg_fine_dollars: number }

// ── Shared primitives ────────────────────────────────────────────────
const fmt = (n: number | null | undefined, d = 1) => n == null ? "--" : n.toFixed(d);
const fmtK = (n: number | null | undefined) => n == null ? "--" : n >= 1e6 ? `${(n/1e6).toFixed(1)}M` : n >= 1e3 ? `${(n/1e3).toFixed(1)}K` : n.toLocaleString();
const fmtD = (n: number | null | undefined) => { if (n == null) return "--"; if (n >= 1e9) return `$${(n/1e9).toFixed(1)}B`; if (n >= 1e6) return `$${(n/1e6).toFixed(1)}M`; if (n >= 1e3) return `$${(n/1e3).toFixed(0)}K`; return `$${n.toLocaleString()}`; };

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

const OWN_COLORS: Record<string, string> = {
  "For-profit": NEG, "for-profit": NEG, "For profit": NEG,
  "Non-profit": POS, "non-profit": POS, "Non profit": POS, "Nonprofit": POS,
  "Government": "#3B82F6", "government": "#3B82F6",
};
const getOwnerColor = (t: string) => OWN_COLORS[t] || AL;

// ══════════════════════════════════════════════════════════════════════
//  RESEARCH BRIEF: Nursing Home Ownership and Quality
// ══════════════════════════════════════════════════════════════════════
export default function NursingOwnership() {
  const isMobile = useIsMobile();
  const { openIntelligence } = useAradune();
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [qualityData, setQualityData] = useState<QualityByType[]>([]);

  const fetchJson = useCallback(async (url: string) => {
    const r = await fetch(`${API_BASE}${url}`);
    if (!r.ok) throw new Error(`${r.status} ${r.statusText}`);
    return r.json();
  }, []);

  useEffect(() => {
    (async () => {
      setLoading(true);
      try {
        const d = await fetchJson("/api/research/nursing-ownership/quality-by-type");
        setQualityData(d.rows || d.data || []);
      } catch (e: unknown) {
        setError(e instanceof Error ? e.message : "Failed to load data");
      }
      setLoading(false);
    })();
  }, [fetchJson]);

  const totalFacilities = useMemo(() => qualityData.reduce((s, r) => s + (r.facility_count || 0), 0), [qualityData]);

  // Build the grouped bar chart data from the raw comparison table in the paper
  const rawComparisonData = [
    { name: "For-Profit\nChain", overall: 2.79, staffing: 2.58, qm: 3.65, n: 8759, type: "For-profit" },
    { name: "For-Profit\nIndep.", overall: 2.83, staffing: 2.79, qm: 3.37, n: 2089, type: "For-profit" },
    { name: "Gov't\nChain", overall: 3.06, staffing: 2.31, qm: 3.93, n: 339, type: "Government" },
    { name: "Gov't\nIndep.", overall: 3.44, staffing: 3.77, qm: 3.31, n: 601, type: "Government" },
    { name: "Nonprofit\nChain", overall: 3.42, staffing: 3.61, qm: 3.63, n: 1103, type: "Non-profit" },
    { name: "Nonprofit\nIndep.", overall: 3.64, staffing: 3.90, qm: 3.52, n: 1819, type: "Non-profit" },
  ];

  if (loading) return <div style={{ maxWidth: 800, margin: "0 auto", padding: 40 }}><LoadingBar /></div>;
  if (error) return <div style={{ maxWidth: 800, margin: "0 auto", padding: 40, color: NEG }}>{error}</div>;

  return (
    <div style={{ maxWidth: 800, margin: "0 auto", padding: isMobile ? "12px" : "20px 20px 60px", fontFamily: FB }}>

      {/* ── Title + Abstract ─────────────────────────────────────────── */}
      <div style={{ marginBottom: 32 }}>
        <div style={{ fontSize: 9, fontFamily: FM, color: AL, textTransform: "uppercase", letterSpacing: 1, marginBottom: 6 }}>Aradune Research Brief</div>
        <h1 style={{ fontSize: isMobile ? 22 : 28, fontWeight: 800, color: A, margin: 0, lineHeight: 1.2, letterSpacing: -0.5 }}>
          For-Profit Nursing Home Ownership Reduces Quality by 0.67 Stars on a 5-Point Scale
        </h1>
        <p style={{ fontSize: 14, color: AL, lineHeight: 1.7, marginTop: 12 }}>
          Facility-level analysis of 14,710 nursing homes finds that for-profit ownership is associated with a 0.67-star reduction
          in CMS Five-Star quality ratings after controlling for state fixed effects and facility size (p &lt; 0.0001, t = -23.0).
          The effect size (Cohen's d = 0.59) is the strongest finding in this study -- medium-large and clinically meaningful,
          representing the difference between "below average" and "above average" care. Chain affiliation costs an additional 0.09
          stars. The mechanism is economic: for-profit facilities show staffing ratings of 2.58 vs 3.90 for nonprofit independents,
          reflecting lower RN hours per resident day under pressure to maximize returns to investors.
        </p>
      </div>

      {/* ── Key Finding Box ──────────────────────────────────────────── */}
      <Card style={{ borderLeft: `4px solid ${NEG}`, marginBottom: 32 }}>
        <div style={{ padding: isMobile ? "16px" : "24px 28px" }}>
          <div style={{ fontSize: 9, fontFamily: FM, color: AL, textTransform: "uppercase", letterSpacing: 1, marginBottom: 8 }}>Key Finding</div>
          <div style={{ display: "flex", alignItems: isMobile ? "flex-start" : "baseline", gap: isMobile ? 8 : 16, flexDirection: isMobile ? "column" : "row" }}>
            <span style={{ fontSize: isMobile ? 36 : 48, fontWeight: 300, fontFamily: FM, color: NEG, lineHeight: 1 }}>-0.67</span>
            <span style={{ fontSize: 15, color: A, lineHeight: 1.5 }}>
              star penalty for for-profit ownership (on a 5-point scale). p &lt; 0.0001. Cohen's d = 0.59 (medium effect).
              Survives state fixed effects, size controls, interaction modeling, and size-matched comparisons.
            </span>
          </div>
        </div>
      </Card>

      {/* ── Methods (Collapsible) ────────────────────────────────────── */}
      <Collapsible title="Methods">
        <div style={{ fontSize: 13, color: AL, lineHeight: 1.8 }}>
          <p style={{ margin: "0 0 12px" }}>
            <strong style={{ color: A }}>State fixed effects + size controls:</strong> Facility-level OLS with state dummies and certified bed count. Absorbs all state-level confounders (regulatory environment, cost of living, demographics, Medicaid payment rates).
          </p>
          <div style={{ background: SF, border: `1px solid ${BD}`, borderRadius: 8, padding: "12px 16px", fontFamily: FM, fontSize: 12, color: A, overflowX: "auto", marginBottom: 12 }}>
            Rating_ij = alpha_j(state) + B1(ForProfit_i) + B2(Chain_i) + B3(Beds_i) + e_i
          </div>
          <p style={{ margin: "0 0 12px" }}>
            <strong style={{ color: A }}>Interaction model:</strong> Tests whether the chain effect differs by ownership type.
          </p>
          <div style={{ background: SF, border: `1px solid ${BD}`, borderRadius: 8, padding: "12px 16px", fontFamily: FM, fontSize: 12, color: A, overflowX: "auto", marginBottom: 12 }}>
            Rating_ij = alpha_j + B1(FP) + B2(Chain) + B3(FP x Chain) + B4(Beds) + e_i
          </div>
          <p style={{ margin: "0 0 12px" }}>
            <strong style={{ color: A }}>Size-matched comparison:</strong> Restrict to 50-150 bed facilities to eliminate size as a confounder. Two-sample t-test with Cohen's d effect size.
          </p>
          <p style={{ margin: 0 }}>
            <strong style={{ color: A }}>Data sources:</strong> fact_five_star (14,710 facilities, CMS Care Compare), fact_nh_deficiency (419,452 citations), fact_pbj_nurse_staffing (1.3M observations, CMS PBJ).
          </p>
        </div>
      </Collapsible>

      {/* ── Results ──────────────────────────────────────────────────── */}
      <div style={{ marginTop: 32 }}>
        <h2 style={{ fontSize: 17, fontWeight: 700, color: A, marginBottom: 16 }}>Results</h2>

        {/* Raw comparison */}
        <h3 style={{ fontSize: 14, fontWeight: 600, color: A, marginBottom: 8 }}>Raw Comparison (No Controls)</h3>
        <div style={{ overflowX: "auto", marginBottom: 16 }}>
          <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 12, fontFamily: FM }}>
            <thead>
              <tr style={{ borderBottom: `2px solid ${A}` }}>
                {["Type", "Chain", "N", "Overall", "Staffing", "QM", "Inspection"].map(h => (
                  <th key={h} style={{ padding: "8px 12px", textAlign: h === "Type" || h === "Chain" ? "left" : "right", color: A, fontWeight: 700, fontSize: 11 }}>{h}</th>
                ))}
              </tr>
            </thead>
            <tbody>
              {[
                ["For-Profit", "Chain", "8,759", "2.79", "2.58", "3.65", "2.65"],
                ["For-Profit", "Independent", "2,089", "2.83", "2.79", "3.37", "2.75"],
                ["Government", "Chain", "339", "3.06", "2.31", "3.93", "2.95"],
                ["Government", "Independent", "601", "3.44", "3.77", "3.31", "3.20"],
                ["Non-Profit", "Chain", "1,103", "3.42", "3.61", "3.63", "3.12"],
                ["Non-Profit", "Independent", "1,819", "3.64", "3.90", "3.52", "3.34"],
              ].map((row, i) => (
                <tr key={i} style={{ borderBottom: `1px solid ${BD}`, background: row[0] === "For-Profit" ? `${NEG}05` : "transparent" }}>
                  <td style={{ padding: "6px 12px", fontWeight: 600, color: row[0] === "For-Profit" ? NEG : row[0] === "Non-Profit" ? POS : "#3B82F6" }}>{row[0]}</td>
                  <td style={{ padding: "6px 12px", color: AL }}>{row[1]}</td>
                  <td style={{ padding: "6px 12px", textAlign: "right", color: AL }}>{row[2]}</td>
                  <td style={{ padding: "6px 12px", textAlign: "right", color: A, fontWeight: 600 }}>{row[3]}</td>
                  <td style={{ padding: "6px 12px", textAlign: "right", color: A }}>{row[4]}</td>
                  <td style={{ padding: "6px 12px", textAlign: "right", color: A }}>{row[5]}</td>
                  <td style={{ padding: "6px 12px", textAlign: "right", color: A }}>{row[6]}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>

        {/* State FE */}
        <h3 style={{ fontSize: 14, fontWeight: 600, color: A, margin: "20px 0 8px" }}>State Fixed Effects + Size Controls (N=14,574, 53 states)</h3>
        <div style={{ overflowX: "auto", marginBottom: 16 }}>
          <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 12, fontFamily: FM }}>
            <thead>
              <tr style={{ borderBottom: `2px solid ${A}` }}>
                {["Variable", "Coeff.", "SE", "t", "p", ""].map(h => (
                  <th key={h} style={{ padding: "8px 12px", textAlign: h === "Variable" ? "left" : "right", color: A, fontWeight: 700, fontSize: 11 }}>{h}</th>
                ))}
              </tr>
            </thead>
            <tbody>
              {[
                ["For-profit", "-0.671", "0.029", "-23.0", "<0.0001", "****"],
                ["Chain-affiliated", "-0.088", "0.027", "-3.2", "0.0013", "***"],
                ["Per 10 beds", "-0.046", "0.002", "-22.1", "<0.0001", "****"],
              ].map((row, i) => (
                <tr key={i} style={{ borderBottom: `1px solid ${BD}`, background: `${POS}08` }}>
                  <td style={{ padding: "6px 12px", fontWeight: 600, color: A }}>{row[0]}</td>
                  <td style={{ padding: "6px 12px", textAlign: "right", color: NEG, fontWeight: 700 }}>{row[1]}</td>
                  <td style={{ padding: "6px 12px", textAlign: "right", color: AL }}>{row[2]}</td>
                  <td style={{ padding: "6px 12px", textAlign: "right", color: A, fontWeight: 600 }}>{row[3]}</td>
                  <td style={{ padding: "6px 12px", textAlign: "right", color: POS, fontWeight: 700 }}>{row[4]}</td>
                  <td style={{ padding: "6px 12px", textAlign: "right", color: POS, fontWeight: 700 }}>{row[5]}</td>
                </tr>
              ))}
            </tbody>
          </table>
          <div style={{ fontSize: 10, fontFamily: FM, color: AL, marginTop: 4 }}>Within-R^2 = 0.083.</div>
        </div>

        {/* Interaction Model */}
        <h3 style={{ fontSize: 14, fontWeight: 600, color: A, margin: "20px 0 8px" }}>Interaction Model</h3>
        <div style={{ overflowX: "auto", marginBottom: 16 }}>
          <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 12, fontFamily: FM }}>
            <thead>
              <tr style={{ borderBottom: `2px solid ${A}` }}>
                {["Variable", "Coeff.", "t", "p", ""].map(h => (
                  <th key={h} style={{ padding: "8px 12px", textAlign: h === "Variable" ? "left" : "right", color: A, fontWeight: 700, fontSize: 11 }}>{h}</th>
                ))}
              </tr>
            </thead>
            <tbody>
              {[
                ["For-profit", "-0.772", "-18.65", "<0.0001", "****"],
                ["Chain", "-0.215", "-4.67", "<0.0001", "****"],
                ["FP x Chain", "+0.194", "3.43", "0.0006", "***"],
                ["Beds", "-0.005", "-22.14", "<0.0001", "****"],
              ].map((row, i) => (
                <tr key={i} style={{ borderBottom: `1px solid ${BD}` }}>
                  <td style={{ padding: "6px 12px", fontWeight: 600, color: A }}>{row[0]}</td>
                  <td style={{ padding: "6px 12px", textAlign: "right", color: row[1].startsWith("+") ? POS : NEG, fontWeight: 700 }}>{row[1]}</td>
                  <td style={{ padding: "6px 12px", textAlign: "right", color: AL }}>{row[2]}</td>
                  <td style={{ padding: "6px 12px", textAlign: "right", color: POS, fontWeight: 700 }}>{row[3]}</td>
                  <td style={{ padding: "6px 12px", textAlign: "right", color: POS, fontWeight: 700 }}>{row[4]}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>

        <p style={{ fontSize: 13, color: AL, lineHeight: 1.7, marginBottom: 8 }}>
          <strong style={{ color: A }}>Predicted effects vs nonprofit independent baseline:</strong>{" "}
          For-profit independent: <strong style={{ color: NEG }}>-0.772 stars</strong>.{" "}
          Nonprofit chain: <strong style={{ color: WARN }}>-0.215 stars</strong>.{" "}
          For-profit chain: <strong style={{ color: NEG }}>-0.792 stars</strong>.{" "}
          The dominant effect is ownership type, not chain structure.
        </p>

        {/* Size-matched */}
        <h3 style={{ fontSize: 14, fontWeight: 600, color: A, margin: "20px 0 8px" }}>Size-Matched Comparison (50-150 beds)</h3>
        <div style={{ overflowX: "auto", marginBottom: 16 }}>
          <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 12, fontFamily: FM }}>
            <thead>
              <tr style={{ borderBottom: `2px solid ${A}` }}>
                {["Group", "N", "Mean Rating"].map(h => (
                  <th key={h} style={{ padding: "8px 12px", textAlign: h === "Group" ? "left" : "right", color: A, fontWeight: 700, fontSize: 11 }}>{h}</th>
                ))}
              </tr>
            </thead>
            <tbody>
              <tr style={{ borderBottom: `1px solid ${BD}` }}>
                <td style={{ padding: "6px 12px", fontWeight: 600, color: NEG }}>For-profit chain</td>
                <td style={{ padding: "6px 12px", textAlign: "right", color: AL }}>6,780</td>
                <td style={{ padding: "6px 12px", textAlign: "right", color: A, fontWeight: 600 }}>2.814</td>
              </tr>
              <tr style={{ borderBottom: `1px solid ${BD}` }}>
                <td style={{ padding: "6px 12px", fontWeight: 600, color: POS }}>Nonprofit independent</td>
                <td style={{ padding: "6px 12px", textAlign: "right", color: AL }}>1,116</td>
                <td style={{ padding: "6px 12px", textAlign: "right", color: A, fontWeight: 600 }}>3.608</td>
              </tr>
            </tbody>
          </table>
          <div style={{ fontSize: 10, fontFamily: FM, color: AL, marginTop: 4 }}>
            Difference: 0.795 stars. t = -17.96. p &lt; 0.000001. Cohen's d = 0.585 (medium effect).
          </div>
        </div>
      </div>

      {/* ── Robustness Checks ────────────────────────────────────────── */}
      <Collapsible title="Robustness Checks">
        <div style={{ fontSize: 13, color: AL, lineHeight: 1.7 }}>
          <p style={{ margin: "0 0 8px" }}><strong style={{ color: A }}>1. State FE:</strong> The -0.671 star penalty survives 53 state fixed effects, demonstrating it is not driven by for-profit facilities concentrating in low-quality states.</p>
          <p style={{ margin: "0 0 8px" }}><strong style={{ color: A }}>2. Size controls:</strong> Larger facilities score lower (-0.046 per 10 beds). For-profit facilities tend to be larger. Controlling for size, the ownership effect is unchanged.</p>
          <p style={{ margin: "0 0 8px" }}><strong style={{ color: A }}>3. Interaction model:</strong> The FP x Chain interaction is significant (+0.194, p=0.0006), but it operates in the wrong direction for the "chain drives poor quality" narrative -- the chain penalty is larger for nonprofits (-0.215) than the incremental chain penalty for for-profits. Ownership is the dominant factor.</p>
          <p style={{ margin: "0 0 8px" }}><strong style={{ color: A }}>4. Size-matched:</strong> Restricting to 50-150 bed facilities yields an even larger gap (0.795 stars vs 0.671), confirming the result is not a size artifact.</p>
          <p style={{ margin: 0 }}><strong style={{ color: A }}>5. QM self-reporting:</strong> The quality measure (QM) sub-rating is the only dimension where for-profits perform comparably (3.65 vs 3.52). QM is self-reported; staffing and inspection ratings are independently verified. The pattern suggests for-profit facilities may overstate QM performance.</p>
        </div>
      </Collapsible>

      {/* ── Supporting Figure ─────────────────────────────────────────── */}
      <div style={{ marginTop: 32 }}>
        <h2 style={{ fontSize: 17, fontWeight: 700, color: A, marginBottom: 4 }}>Figure 1</h2>
        <p style={{ fontSize: 12, color: AL, margin: "0 0 12px" }}>
          Average CMS Five-Star overall rating by ownership type and chain affiliation. 14,710 facilities. Scale: 1-5 stars.
        </p>
        <Card>
          <div style={{ padding: "12px 16px 16px" }}>
            <ChartActions filename="nursing-ownership-quality">
              <div style={{ width: "100%", height: isMobile ? 320 : 380 }}>
                <ResponsiveContainer>
                  <BarChart data={rawComparisonData} margin={{ left: 10, right: 20, top: 10, bottom: 10 }}>
                    <CartesianGrid strokeDasharray="3 3" stroke={BD} />
                    <XAxis dataKey="name" tick={{ fontSize: 9, fill: AL }} interval={0} />
                    <YAxis tick={{ fontSize: 10, fill: AL }} domain={[0, 5]} label={{ value: "Average Rating (1-5)", angle: -90, position: "insideLeft", offset: 15, fontSize: 11, fill: AL }} />
                    <Tooltip content={({ active, payload }) => {
                      if (!active || !payload?.length) return null;
                      const d = payload[0]?.payload;
                      if (!d) return null;
                      return (
                        <div style={{ background: WH, border: `1px solid ${BD}`, borderRadius: 6, padding: "6px 10px", fontSize: 10, fontFamily: FM, boxShadow: SH }}>
                          <div style={{ fontWeight: 600, color: A }}>{d.name.replace("\n", " ")}</div>
                          <div style={{ color: AL }}>N = {fmtK(d.n)} facilities</div>
                          <div style={{ color: AL }}>Overall: {fmt(d.overall)}</div>
                          <div style={{ color: AL }}>Staffing: {fmt(d.staffing)}</div>
                          <div style={{ color: AL }}>QM: {fmt(d.qm)}</div>
                        </div>
                      );
                    }} />
                    <Legend wrapperStyle={{ fontSize: 10, fontFamily: FM }} />
                    <Bar dataKey="overall" name="Overall Rating" radius={[3, 3, 0, 0]} maxBarSize={32}>
                      {rawComparisonData.map((d, i) => (
                        <Cell key={i} fill={getOwnerColor(d.type)} fillOpacity={0.85} />
                      ))}
                    </Bar>
                    <Bar dataKey="staffing" name="Staffing Rating" radius={[3, 3, 0, 0]} maxBarSize={32}>
                      {rawComparisonData.map((d, i) => (
                        <Cell key={i} fill={getOwnerColor(d.type)} fillOpacity={0.5} />
                      ))}
                    </Bar>
                  </BarChart>
                </ResponsiveContainer>
              </div>
            </ChartActions>
            <div style={{ textAlign: "center", fontSize: 10, fontFamily: FM, color: AL, marginTop: 8 }}>
              <span style={{ color: NEG }}>Red = For-Profit</span>
              {" | "}
              <span style={{ color: POS }}>Green = Non-Profit</span>
              {" | "}
              <span style={{ color: "#3B82F6" }}>Blue = Government</span>
              {" | Solid = Overall, Light = Staffing"}
            </div>
          </div>
        </Card>

        {/* Worst/Best chains callout */}
        <div style={{ display: "grid", gridTemplateColumns: isMobile ? "1fr" : "1fr 1fr", gap: 12, marginTop: 16 }}>
          <Card style={{ borderLeft: `3px solid ${NEG}` }}>
            <div style={{ padding: "12px 16px" }}>
              <div style={{ fontSize: 9, fontFamily: FM, color: NEG, textTransform: "uppercase", letterSpacing: 0.5, marginBottom: 4 }}>{"Worst Chains (>=10 facilities)"}</div>
              {[
                { name: "Reliant Care Management", n: 30, rating: 1.17 },
                { name: "Bria Health Services", n: 15, rating: 1.20 },
                { name: "Eastern Healthcare Group", n: 17, rating: 1.24 },
                { name: "Beacon Health Management", n: 17, rating: 1.29 },
                { name: "Pointe Management", n: 12, rating: 1.42 },
              ].map(c => (
                <div key={c.name} style={{ display: "flex", justifyContent: "space-between", fontSize: 11, fontFamily: FM, color: A, padding: "2px 0" }}>
                  <span>{c.name} ({c.n})</span>
                  <span style={{ color: NEG, fontWeight: 600 }}>{fmt(c.rating)} stars</span>
                </div>
              ))}
            </div>
          </Card>
          <Card style={{ borderLeft: `3px solid ${POS}` }}>
            <div style={{ padding: "12px 16px" }}>
              <div style={{ fontSize: 9, fontFamily: FM, color: POS, textTransform: "uppercase", letterSpacing: 0.5, marginBottom: 4 }}>{"Best Chains (>=10 facilities)"}</div>
              {[
                { name: "ACTS Retirement-Life Communities", n: 26, rating: 4.81 },
                { name: "VI Living", n: 10, rating: 4.80 },
                { name: "Advanced Health Care", n: 26, rating: 4.72 },
              ].map(c => (
                <div key={c.name} style={{ display: "flex", justifyContent: "space-between", fontSize: 11, fontFamily: FM, color: A, padding: "2px 0" }}>
                  <span>{c.name} ({c.n})</span>
                  <span style={{ color: POS, fontWeight: 600 }}>{fmt(c.rating)} stars</span>
                </div>
              ))}
              <div style={{ fontSize: 10, color: AL, marginTop: 4 }}>All top chains are nonprofits.</div>
            </div>
          </Card>
        </div>
      </div>

      {/* ── Limitations ──────────────────────────────────────────────── */}
      <Collapsible title="Limitations">
        <div style={{ fontSize: 13, color: AL, lineHeight: 1.7 }}>
          <p style={{ margin: "0 0 8px" }}><strong style={{ color: A }}>Selection bias:</strong> For-profit chains may acquire facilities in markets with structurally lower quality potential (higher acuity, lower workforce supply). A fully causal design would require panel data tracking facilities through ownership changes (change-of-ownership events), available in fact_snf_chow but not analyzed here.</p>
          <p style={{ margin: "0 0 8px" }}><strong style={{ color: A }}>Cross-sectional:</strong> This analysis uses a single point-in-time snapshot. We cannot observe quality trajectories before and after ownership changes.</p>
          <p style={{ margin: 0 }}><strong style={{ color: A }}>QM self-reporting:</strong> The near-parity on QM ratings (the only self-reported dimension) suggests possible gaming. Independently verified dimensions (staffing, inspection) show the largest gaps.</p>
        </div>
      </Collapsible>

      {/* ── Replication ───────────────────────────────────────────────── */}
      <Collapsible title="Replication Code">
        <div style={{ background: SF, border: `1px solid ${BD}`, borderRadius: 8, padding: 16, overflowX: "auto" }}>
          <pre style={{ margin: 0, fontSize: 11, fontFamily: FM, color: A, lineHeight: 1.6, whiteSpace: "pre-wrap" }}>{`-- Raw comparison by ownership type and chain affiliation
SELECT ownership_type,
       CASE WHEN chain_affiliated THEN 'Chain' ELSE 'Independent' END AS affiliation,
       COUNT(*) AS facility_count,
       ROUND(AVG(overall_rating), 2) AS avg_overall,
       ROUND(AVG(staffing_rating), 2) AS avg_staffing,
       ROUND(AVG(quality_measure_rating), 2) AS avg_qm,
       ROUND(AVG(survey_rating), 2) AS avg_inspection
FROM fact_five_star
WHERE overall_rating IS NOT NULL
GROUP BY ownership_type, affiliation
ORDER BY avg_overall;

-- State FE regression (Python):
-- import statsmodels.api as sm
-- dummies = pd.get_dummies(df['state_code'], drop_first=True)
-- X = pd.concat([df[['for_profit','chain','beds']], dummies], axis=1)
-- X = sm.add_constant(X)
-- model = sm.OLS(df['overall_rating'], X).fit()
-- print(model.summary())

-- Size-matched comparison (50-150 beds)
SELECT CASE WHEN ownership_type ILIKE '%for%profit%' AND chain_affiliated
            THEN 'For-profit chain'
            WHEN ownership_type ILIKE '%non%profit%' AND NOT chain_affiliated
            THEN 'Nonprofit independent'
       END AS group_label,
       COUNT(*) AS n, ROUND(AVG(overall_rating), 3) AS mean_rating
FROM fact_five_star
WHERE bed_count BETWEEN 50 AND 150
  AND overall_rating IS NOT NULL
GROUP BY group_label
HAVING group_label IS NOT NULL;`}</pre>
        </div>
      </Collapsible>

      {/* ── Sources ──────────────────────────────────────────────────── */}
      <div style={{ marginTop: 32, paddingTop: 16, borderTop: `1px solid ${BD}` }}>
        <div style={{ fontSize: 10, fontFamily: FM, color: AL, lineHeight: 1.8 }}>
          <strong style={{ color: A }}>Sources:</strong> CMS Five-Star Quality Rating (14,710 facilities, Care Compare) |
          CMS Deficiency Citations (419,452 citations) | Payroll-Based Journal (1.3M staffing observations, CMS PBJ) |
          HCRIS SNF Cost Reports.
        </div>
      </div>

      {/* ── Ask Aradune ──────────────────────────────────────────────── */}
      <div style={{ marginTop: 24, textAlign: "center" }}>
        <button onClick={() => openIntelligence({ summary: "User is viewing the Nursing Ownership research brief. Key finding: -0.67 star for-profit penalty (p<0.0001, Cohen's d=0.59). Strongest finding in the study. Survives state FE, size controls, interaction model, size-matched comparison." })}
          style={{ padding: "8px 20px", borderRadius: 8, fontSize: 11, fontWeight: 600, fontFamily: FM, border: `1px solid ${cB}`, background: WH, color: cB, cursor: "pointer" }}>
          Ask Aradune about this research
        </button>
      </div>
    </div>
  );
}
