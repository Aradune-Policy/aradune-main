/**
 * Methodology Library
 * How each state sets Medicaid rates: methodology type,
 * conversion factors, fee schedule sources, and spending context.
 */
import { useState, useEffect, useMemo } from "react";
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
const Met = ({ label, value, color }: { label: string; value: string; color?: string }) => (
  <div style={{ textAlign: "center", minWidth: 80 }}>
    <div style={{ fontSize: 22, fontWeight: 700, color: color || A, fontFamily: FM }}>{value}</div>
    <div style={{ fontSize: 11, color: AL, marginTop: 2 }}>{label}</div>
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
  a.href = URL.createObjectURL(new Blob([csv], { type: "text/csv" }));
  a.download = filename;
  a.click();
}

const ExportBtn = ({ label, onClick }: { label: string; onClick: () => void }) => (
  <button onClick={onClick} style={{
    padding: "6px 14px", borderRadius: 6, border: `1px solid ${BD}`, background: WH,
    color: AL, fontSize: 12, cursor: "pointer", fontFamily: FM,
  }}>{label}</button>
);

const f$ = (n: number) =>
  n >= 1e9 ? `$${(n / 1e9).toFixed(1)}B`
  : n >= 1e6 ? `$${(n / 1e6).toFixed(1)}M`
  : n >= 1e3 ? `$${(n / 1e3).toFixed(0)}K`
  : `$${n.toFixed(0)}`;

// ── Methodology Classification ──────────────────────────────────────────
type MethodType = "RBRVS" | "% Medicare" | "Custom CF" | "Cost-Based" | "Negotiated" | "Mixed" | "Unknown";

function classifyMethodology(text: string): MethodType {
  const t = text.toLowerCase();
  if (!t.trim()) return "Mixed";

  // Detect individual signals
  const hasRbrvs = t.includes("rbrvs") || t.includes("rvu") || t.includes("resource-based");
  const hasCostBased = t.includes("cost-based") || t.includes("cost based") || t.includes("cost report");
  const hasNegotiated = t.includes("negotiat");
  const hasMultiple = t.includes("multiple methodolog");
  const hasStateDeveloped = t.includes("state-developed") && !hasRbrvs;
  // "% of Medicare" = explicit percentage reference (not just mentioning "medicare" in passing)
  const hasPctMedicare = /\d+%?\s*of\s*(prior.year\s*)?medicare/i.test(text)
    || t.includes("% of medicare") || t.includes("percentage of medicare");

  // Mixed: multiple distinct methodologies mentioned, or explicit "multiple"
  if (hasMultiple || hasStateDeveloped) return "Mixed";
  if (hasRbrvs && hasCostBased) return "Mixed";
  if (hasPctMedicare && hasCostBased) return "Mixed";

  // Single methodology
  if (hasPctMedicare && !hasRbrvs) return "% Medicare";
  if (hasRbrvs) return "RBRVS";
  if (hasCostBased) return "Cost-Based";
  if (hasNegotiated) return "Negotiated";

  // Weak Medicare reference (e.g., "some services linked to Medicare")
  if (t.includes("medicare")) return "% Medicare";

  return "Mixed";
}

const METHOD_COLORS: Record<MethodType, string> = {
  "RBRVS": "#2E6B4A",
  "% Medicare": "#3A7D5C",
  "Custom CF": "#C4590A",
  "Cost-Based": "#B8860B",
  "Negotiated": "#6B4A8C",
  "Mixed": "#425A70",
  "Unknown": "#999",
};

// ── Types ───────────────────────────────────────────────────────────────
interface DirEntry {
  state: string; agency: string; url: string; format: string;
  access: string; methodology: string; verified: boolean;
}
interface StateSpending {
  state: string; total_spend: number; total_claims: number; total_bene: number;
  n_providers: number; fmap: number;
}

// ═════════════════════════════════════════════════════════════════════════
export default function MethodologyLibrary() {
  const [directory, setDirectory] = useState<DirEntry[]>([]);
  const [statesData, setStatesData] = useState<StateSpending[]>([]);
  const [search, setSearch] = useState("");
  const [methodFilter, setMethodFilter] = useState<MethodType | "All">("All");
  const [expanded, setExpanded] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    let cancelled = false;
    Promise.all([
      fetch("/data/fee_schedule_directory.json").then(r => r.json()),
      fetch("/data/states.json").then(r => r.json()),
    ]).then(([dir, states]) => {
      if (cancelled) return;
      // Filter out reference notes (entries without an agency are not real states)
      setDirectory((dir as { directory: DirEntry[] }).directory.filter((d: DirEntry) => d.agency));
      setStatesData(states as StateSpending[]);
      setLoading(false);
    });
    return () => { cancelled = true; };
  }, []);

  // Map state name to abbreviation
  const nameToAbbr = useMemo(() => {
    const m = new Map<string, string>();
    for (const [abbr, name] of Object.entries(STATE_NAMES)) m.set(name, abbr);
    // Also add some variations
    m.set("District of Columbia", "DC");
    return m;
  }, []);

  // Enrich directory with classification + spending
  const enriched = useMemo(() => {
    const spendMap = new Map(statesData.map(s => [s.state, s]));
    return directory.map(d => {
      const abbr = nameToAbbr.get(d.state) ?? STATES_LIST.find(s => STATE_NAMES[s] === d.state) ?? "";
      const spend = spendMap.get(abbr);
      return {
        ...d,
        abbr,
        method_type: classifyMethodology(d.methodology),
        total_spend: spend?.total_spend ?? 0,
        total_claims: spend?.total_claims ?? 0,
        n_providers: spend?.n_providers ?? 0,
        fmap: spend?.fmap ?? 0,
      };
    });
  }, [directory, statesData, nameToAbbr]);

  // Filter
  const filtered = useMemo(() => {
    let list = enriched;
    if (search) {
      const q = search.toLowerCase();
      list = list.filter(d =>
        d.state.toLowerCase().includes(q) ||
        d.abbr.toLowerCase().includes(q) ||
        d.methodology.toLowerCase().includes(q) ||
        d.agency.toLowerCase().includes(q)
      );
    }
    if (methodFilter !== "All") {
      list = list.filter(d => d.method_type === methodFilter);
    }
    return list.sort((a, b) => a.state.localeCompare(b.state));
  }, [enriched, search, methodFilter]);

  // Summary stats
  const summary = useMemo(() => {
    const counts: Record<string, number> = {};
    for (const d of enriched) {
      counts[d.method_type] = (counts[d.method_type] ?? 0) + 1;
    }
    const verified = enriched.filter(d => d.verified).length;
    return { counts, total: enriched.length, verified };
  }, [enriched]);

  const methodTypes: MethodType[] = ["RBRVS", "% Medicare", "Custom CF", "Cost-Based", "Mixed"];

  // ── Render ────────────────────────────────────────────────────────────
  return (
    <div style={{ maxWidth: 1100, margin: "0 auto", padding: "24px 16px", fontFamily: FB }}>
      <h2 style={{ fontSize: 22, fontWeight: 800, color: A, margin: "0 0 4px" }}>Methodology Library</h2>
      <p style={{ fontSize: 13, color: AL, margin: "0 0 20px" }}>
        How each state sets Medicaid rates: methodology type, fee schedule sources, and spending context
      </p>

      {loading ? (
        <Card><p style={{ color: AL, fontSize: 13, textAlign: "center", padding: 40 }}>Loading methodology data...</p></Card>
      ) : (
        <>
          {/* Summary KPIs */}
          <Card accent={cB}>
            <CH title="Methodology Landscape" sub={`${summary.total} states & territories — ${summary.verified} verified`} />
            <div style={{ display: "flex", justifyContent: "space-around", flexWrap: "wrap", gap: 16 }}>
              {methodTypes.map(m => (
                <div key={m} style={{ textAlign: "center", cursor: "pointer" }}
                  onClick={() => setMethodFilter(methodFilter === m ? "All" : m)}>
                  <div style={{ fontSize: 24, fontWeight: 700, color: METHOD_COLORS[m], fontFamily: FM }}>
                    {summary.counts[m] ?? 0}
                  </div>
                  <div style={{ fontSize: 11, color: AL }}>{m}</div>
                  {methodFilter === m && <div style={{ height: 2, background: METHOD_COLORS[m], borderRadius: 1, marginTop: 2 }} />}
                </div>
              ))}
              <Met label="Verified" value={`${summary.verified}`} color={POS} />
            </div>
          </Card>

          {/* Search + Filters */}
          <Card>
            <div style={{ display: "flex", gap: 16, flexWrap: "wrap", alignItems: "center" }}>
              <input
                placeholder="Search states, agencies, or methods..."
                value={search} onChange={e => setSearch(e.target.value)}
                style={{
                  flex: 1, minWidth: 200, padding: "8px 12px", borderRadius: 8,
                  border: `1px solid ${BD}`, fontSize: 13, fontFamily: FB, outline: "none",
                }}
              />
              <Pill label="All" on={methodFilter === "All"} onClick={() => setMethodFilter("All")} />
              {methodTypes.map(m => (
                <Pill key={m} label={m} on={methodFilter === m} onClick={() => setMethodFilter(methodFilter === m ? "All" : m)} />
              ))}
              <ExportBtn label="Export CSV" onClick={() => {
                downloadCSV(
                  ["State", "Agency", "Methodology Type", "Methodology Detail", "Format", "URL", "Verified", "Total Spend", "FMAP"],
                  filtered.map(d => [d.state, d.agency, d.method_type, d.methodology, d.format, d.url, d.verified ? "Yes" : "No", d.total_spend.toFixed(0), d.fmap.toFixed(1)]),
                  "methodology_library.csv",
                );
              }} />
            </div>
          </Card>

          {/* State Table */}
          <Card>
            <CH title={`${filtered.length} States`} sub={methodFilter !== "All" ? `Filtered: ${methodFilter}` : "All methodology types"} />
            <div style={{ overflowX: "auto" }}>
              <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 12, fontFamily: FM }}>
                <thead>
                  <tr style={{ borderBottom: `2px solid ${BD}` }}>
                    {["State", "Agency", "Methodology", "Total Spend", "FMAP", "Format", "Verified"].map(h => (
                      <th key={h} style={{ padding: "8px 6px", textAlign: "left", color: AL, fontWeight: 600, fontSize: 11, whiteSpace: "nowrap" }}>{h}</th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {filtered.map(d => (
                    <>
                      <tr key={d.abbr || d.state}
                        onClick={() => setExpanded(expanded === d.state ? null : d.state)}
                        style={{
                          borderBottom: `1px solid ${BD}`, cursor: "pointer",
                          background: expanded === d.state ? SF : undefined,
                        }}>
                        <td style={{ padding: "8px 6px", fontWeight: 600, color: A, whiteSpace: "nowrap" }}>
                          <span style={{ marginRight: 6, fontSize: 10 }}>{expanded === d.state ? "▾" : "▸"}</span>
                          {d.abbr ? `${d.abbr} — ` : ""}{d.state}
                        </td>
                        <td style={{ padding: "6px", color: AL, maxWidth: 180, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>{d.agency}</td>
                        <td style={{ padding: "6px" }}>
                          <span style={{
                            display: "inline-block", padding: "2px 8px", borderRadius: 10, fontSize: 11,
                            fontWeight: 600, color: WH, background: METHOD_COLORS[d.method_type],
                          }}>{d.method_type}</span>
                        </td>
                        <td style={{ padding: "6px", textAlign: "right" }}>{d.total_spend > 0 ? f$(d.total_spend) : "—"}</td>
                        <td style={{ padding: "6px", textAlign: "right" }}>{d.fmap > 0 ? `${d.fmap.toFixed(1)}%` : "—"}</td>
                        <td style={{ padding: "6px", color: AL }}>{d.format}</td>
                        <td style={{ padding: "6px", textAlign: "center" }}>
                          {d.verified ? <span style={{ color: POS }}>✓</span> : <span style={{ color: AL }}>—</span>}
                        </td>
                      </tr>
                      {expanded === d.state && (
                        <tr key={`${d.state}-detail`}>
                          <td colSpan={7} style={{ padding: "12px 16px 16px", background: SF, borderBottom: `1px solid ${BD}` }}>
                            <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 16, fontSize: 12 }}>
                              <div>
                                <div style={{ fontWeight: 700, color: A, marginBottom: 6 }}>Methodology Detail</div>
                                <p style={{ margin: 0, color: AL, lineHeight: 1.6 }}>{d.methodology}</p>
                              </div>
                              <div>
                                <div style={{ fontWeight: 700, color: A, marginBottom: 6 }}>Fee Schedule Access</div>
                                <p style={{ margin: "0 0 8px", color: AL, lineHeight: 1.6 }}>{d.access}</p>
                                {d.url && (
                                  <a href={d.url} target="_blank" rel="noopener noreferrer"
                                    style={{ color: cB, fontWeight: 600, fontSize: 12, textDecoration: "none" }}>
                                    View Fee Schedule →
                                  </a>
                                )}
                              </div>
                            </div>
                            {d.total_spend > 0 && (
                              <div style={{ display: "flex", gap: 24, marginTop: 12, paddingTop: 12, borderTop: `1px solid ${BD}` }}>
                                <div><span style={{ color: AL }}>Total Spend:</span> <span style={{ fontWeight: 600, color: A }}>{f$(d.total_spend)}</span></div>
                                <div><span style={{ color: AL }}>Claims:</span> <span style={{ fontWeight: 600, color: A }}>{d.total_claims > 0 ? f$(d.total_claims).replace("$", "") : "—"}</span></div>
                                <div><span style={{ color: AL }}>Providers:</span> <span style={{ fontWeight: 600, color: A }}>{d.n_providers > 0 ? d.n_providers.toLocaleString() : "—"}</span></div>
                                <div><span style={{ color: AL }}>FMAP:</span> <span style={{ fontWeight: 600, color: A }}>{d.fmap > 0 ? `${d.fmap.toFixed(1)}%` : "—"}</span></div>
                              </div>
                            )}
                          </td>
                        </tr>
                      )}
                    </>
                  ))}
                </tbody>
              </table>
            </div>
          </Card>

          {/* Notes */}
          <Card>
            <CH title="Notes" />
            <div style={{ fontSize: 12, color: AL, lineHeight: 1.7 }}>
              <p style={{ margin: "0 0 8px" }}>
                Methodology classifications are derived from published fee schedule documentation and state plan amendments.
                States may use different methodologies for different service categories (e.g., RBRVS for physician services,
                cost-based for facilities).
              </p>
              <p style={{ margin: "0 0 8px" }}>
                <strong>RBRVS:</strong> Resource-Based Relative Value Scale. Multiplies CMS RVUs by a state conversion factor.{" "}
                <strong>% Medicare:</strong> Sets rates as a percentage of the Medicare Physician Fee Schedule.{" "}
                <strong>Custom CF:</strong> State-developed conversion factors applied to RVUs with state-specific adjustments.{" "}
                <strong>Cost-Based:</strong> Rates derived from provider cost reports (common for facilities).
              </p>
              <p style={{ margin: 0 }}>
                Fee schedule URLs verified as of March 2026. Spending data from T-MSIS (2018–2024 aggregated).
              </p>
            </div>
          </Card>
        </>
      )}
    </div>
  );
}
