/**
 * State Fee Schedule Directory
 * Merged tool: fee schedule access + methodology classification + spending context.
 * Every state's published Medicaid fee schedule in one table with expandable detail rows.
 */
import React, { useState, useEffect, useMemo } from "react";
import { STATES_LIST, STATE_NAMES } from "../data/states";
import { API_BASE } from "../lib/api";

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
const Met = ({ label, value, color, onClick }: { label: string; value: string; color?: string; onClick?: () => void }) => (
  <div style={{ textAlign: "center", minWidth: 80, cursor: onClick ? "pointer" : undefined }} onClick={onClick}>
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

const f$ = (n: number) =>
  n >= 1e9 ? `$${(n / 1e9).toFixed(1)}B`
  : n >= 1e6 ? `$${(n / 1e6).toFixed(1)}M`
  : n >= 1e3 ? `$${(n / 1e3).toFixed(0)}K`
  : `$${n.toFixed(0)}`;

// ── Format / Access classification ──────────────────────────────────────
type FormatTag = "Excel" | "PDF" | "CSV" | "Web" | "Text";
type AccessTag = "Public" | "Click-through" | "Login" | "Portal";

const FORMAT_COLORS: Record<FormatTag, string> = {
  Excel: "#217346", PDF: "#B30B00", CSV: "#0070C0", Web: "#6B4A8C", Text: "#666",
};
const ACCESS_COLORS: Record<AccessTag, { bg: string; fg: string }> = {
  "Public": { bg: "#E8F5E9", fg: POS },
  "Click-through": { bg: "#FFF8E1", fg: WARN },
  "Login": { bg: "#FFEBEE", fg: NEG },
  "Portal": { bg: "#E3F2FD", fg: "#1565C0" },
};

function parseFormats(raw: string): FormatTag[] {
  const tags: FormatTag[] = [];
  const t = raw.toLowerCase();
  if (t.includes("excel") || t.includes("xls")) tags.push("Excel");
  if (t.includes("pdf")) tags.push("PDF");
  if (t.includes("csv")) tags.push("CSV");
  if (t.includes("web") || t.includes("lookup") || t.includes("search")) tags.push("Web");
  if (t.includes("text") || t.includes("txt")) tags.push("Text");
  return tags.length ? tags : ["PDF"];
}

function classifyAccess(raw: string): AccessTag {
  const t = raw.toLowerCase();
  if (t.includes("login") || t.includes("credentials") || t.includes("provider login")) return "Login";
  if (t.includes("click-through") || t.includes("license acceptance") || t.includes("cpt") || t.includes("cdt")) return "Click-through";
  if (t.includes("portal") || t.includes("navigation") || t.includes("navigate")) return "Portal";
  return "Public";
}

function isMachineReadable(formats: FormatTag[]): boolean {
  return formats.includes("Excel") || formats.includes("CSV");
}

// ── Methodology Classification ──────────────────────────────────────────
type MethodType = "RBRVS" | "% Medicare" | "Custom CF" | "Cost-Based" | "Negotiated" | "Mixed" | "Unknown";

function classifyMethodology(text: string): MethodType {
  const t = text.toLowerCase();
  if (!t.trim()) return "Mixed";
  const hasRbrvs = t.includes("rbrvs") || t.includes("rvu") || t.includes("resource-based");
  const hasCostBased = t.includes("cost-based") || t.includes("cost based") || t.includes("cost report");
  const hasNegotiated = t.includes("negotiat");
  const hasMultiple = t.includes("multiple methodolog");
  const hasStateDeveloped = t.includes("state-developed") && !hasRbrvs;
  const hasPctMedicare = /\d+%?\s*of\s*(prior.year\s*)?medicare/i.test(text)
    || t.includes("% of medicare") || t.includes("percentage of medicare");
  if (hasMultiple || hasStateDeveloped) return "Mixed";
  if (hasRbrvs && hasCostBased) return "Mixed";
  if (hasPctMedicare && hasCostBased) return "Mixed";
  if (hasPctMedicare && !hasRbrvs) return "% Medicare";
  if (hasRbrvs) return "RBRVS";
  if (hasCostBased) return "Cost-Based";
  if (hasNegotiated) return "Negotiated";
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
interface CfValue { name: string; value: number; }
interface CfEntry {
  name: string; methodology: string; methodology_detail: string;
  conversion_factors: CfValue[];
  cf_notes: string;
  update_frequency: string; gpci_approach: string; fee_schedule_type: string;
}

// ═════════════════════════════════════════════════════════════════════════
export default function FeeScheduleDir() {
  const [directory, setDirectory] = useState<DirEntry[]>([]);
  const [statesData, setStatesData] = useState<StateSpending[]>([]);
  const [cfData, setCfData] = useState<Record<string, CfEntry>>({});
  const [search, setSearch] = useState("");
  const [formatFilter, setFormatFilter] = useState<FormatTag | "All">("All");
  const [accessFilter, setAccessFilter] = useState<AccessTag | "All">("All");
  const [methodFilter, setMethodFilter] = useState<MethodType | "All">("All");
  const [expanded, setExpanded] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    let cancelled = false;
    const tryApi = async (apiPath: string, fallback: string, dflt: any = null) => {
      if (API_BASE) { try { const r = await fetch(`${API_BASE}${apiPath}`); if (r.ok) return r.json(); } catch {} }
      const r = await fetch(fallback);
      if (!r.ok) { if (dflt !== null) return dflt; throw new Error(`${fallback}: ${r.status}`); }
      return r.json();
    };
    Promise.all([
      fetch("/data/fee_schedule_directory.json").then(r => { if (!r.ok) throw new Error(`fee_schedule_directory: ${r.status}`); return r.json(); }),
      tryApi("/api/bulk/states", "/data/states.json"),
      fetch("/data/conversion_factors.json").then(r => r.ok ? r.json() : {}).catch(() => ({})),
    ]).then(([dir, states, cf]) => {
      if (cancelled) return;
      const dirArr = dir?.directory ?? dir;
      const entries = Array.isArray(dirArr) ? dirArr.filter((d: DirEntry) => d.agency) : [];
      setDirectory(entries);
      setStatesData(Array.isArray(states) ? states : []);
      if (cf && typeof cf === "object") setCfData(cf as Record<string, CfEntry>);
      setLoading(false);
    }).catch((err) => {
      console.error("FeeScheduleDir load error:", err);
      if (!cancelled) { setError(String(err)); setLoading(false); }
    });
    return () => { cancelled = true; };
  }, []);

  const nameToAbbr = useMemo(() => {
    const m = new Map<string, string>();
    for (const [abbr, name] of Object.entries(STATE_NAMES)) m.set(name, abbr);
    m.set("District of Columbia", "DC");
    return m;
  }, []);

  // Enrich with parsed formats, access, methodology, spending, and CF
  const enriched = useMemo(() => {
    const spendMap = new Map(statesData.map(s => [s.state, s]));
    return directory.map(d => {
      const abbr = nameToAbbr.get(d.state) ?? STATES_LIST.find(s => STATE_NAMES[s] === d.state) ?? "";
      const formats = parseFormats(d.format);
      const accessTag = classifyAccess(d.access);
      const machineReadable = isMachineReadable(formats);
      const methodType = classifyMethodology(d.methodology);
      const spend = spendMap.get(abbr);
      const cf = cfData[abbr];
      return {
        ...d, abbr, formats, accessTag, machineReadable, methodType,
        total_spend: spend?.total_spend ?? 0,
        total_claims: spend?.total_claims ?? 0,
        n_providers: spend?.n_providers ?? 0,
        fmap: spend?.fmap ?? 0,
        conversion_factors: cf?.conversion_factors ?? [],
        cf_notes: cf?.cf_notes ?? "",
        update_frequency: cf?.update_frequency ?? "",
      };
    });
  }, [directory, statesData, nameToAbbr, cfData]);

  // Filter
  const filtered = useMemo(() => {
    let list = enriched;
    if (search) {
      const q = search.toLowerCase();
      list = list.filter(d =>
        d.state.toLowerCase().includes(q) ||
        d.abbr.toLowerCase().includes(q) ||
        d.agency.toLowerCase().includes(q) ||
        d.methodology.toLowerCase().includes(q)
      );
    }
    if (formatFilter !== "All") list = list.filter(d => d.formats.includes(formatFilter));
    if (accessFilter !== "All") list = list.filter(d => d.accessTag === accessFilter);
    if (methodFilter !== "All") list = list.filter(d => d.methodType === methodFilter);
    return list.sort((a, b) => a.state.localeCompare(b.state));
  }, [enriched, search, formatFilter, accessFilter, methodFilter]);

  // Summary stats
  const summary = useMemo(() => {
    const verified = enriched.filter(d => d.verified).length;
    const machineReadable = enriched.filter(d => d.machineReadable).length;
    const pdfOnly = enriched.filter(d => d.formats.length === 1 && d.formats[0] === "PDF").length;
    const methodCounts: Record<string, number> = {};
    for (const d of enriched) methodCounts[d.methodType] = (methodCounts[d.methodType] ?? 0) + 1;
    const withCf = enriched.filter(d => d.conversion_factors.length > 0).length;
    return { total: enriched.length, verified, machineReadable, pdfOnly, methodCounts, withCf };
  }, [enriched]);

  const formatTags: FormatTag[] = ["Excel", "PDF", "CSV", "Web", "Text"];
  const accessTags: AccessTag[] = ["Public", "Click-through", "Login", "Portal"];
  const methodTypes: MethodType[] = ["RBRVS", "% Medicare", "Custom CF", "Cost-Based", "Mixed"];

  const activeFilters = [
    formatFilter !== "All" ? formatFilter : "",
    accessFilter !== "All" ? accessFilter : "",
    methodFilter !== "All" ? methodFilter : "",
  ].filter(Boolean).join(", ");

  return (
    <div style={{ maxWidth: 1100, margin: "0 auto", padding: "24px 16px", fontFamily: FB }}>
      <h2 style={{ fontSize: 22, fontWeight: 800, color: A, margin: "0 0 4px" }}>State Fee Schedule Directory</h2>
      <p style={{ fontSize: 13, color: AL, margin: "0 0 20px" }}>
        Every state's Medicaid fee schedule: methodology, format, access requirements, spending context, and compliance readiness
      </p>

      {error && (
        <Card accent={NEG}><p style={{ color: NEG, fontSize: 13, padding: 12 }}>Error loading data: {error}</p></Card>
      )}
      {loading ? (
        <Card><p style={{ color: AL, fontSize: 13, textAlign: "center", padding: 40 }}>Loading directory...</p></Card>
      ) : (
        <>
          {/* KPI Summary */}
          <Card accent={cB}>
            <CH title="Directory Overview" sub={`${summary.total} states & territories`} />
            <div style={{ display: "flex", justifyContent: "space-around", flexWrap: "wrap", gap: 16 }}>
              <Met label="Total States" value={`${summary.total}`} color={A} />
              {methodTypes.map(m => (
                <Met key={m} label={m} value={`${summary.methodCounts[m] ?? 0}`} color={METHOD_COLORS[m]}
                  onClick={() => setMethodFilter(methodFilter === m ? "All" : m)} />
              ))}
              <Met label="Machine-Readable" value={`${summary.machineReadable}`} color={POS} />
              <Met label="Verified" value={`${summary.verified}`} color={cB} />
            </div>
          </Card>

          {/* CMS Compliance Banner */}
          <Card accent={WARN}>
            <CH title="CMS Machine-Readable Deadline" sub="42 CFR 447.203: July 1, 2026" />
            <div style={{ display: "flex", gap: 24, alignItems: "center", flexWrap: "wrap" }}>
              <div style={{ flex: 1, minWidth: 240 }}>
                <p style={{ margin: "0 0 8px", fontSize: 13, color: AL, lineHeight: 1.6 }}>
                  The CMS Ensuring Access Final Rule requires states to publish FFS rates in machine-readable format.{" "}
                  <strong>{summary.machineReadable}</strong> of {summary.total} states already publish in Excel or CSV.{" "}
                  <strong>{summary.pdfOnly}</strong> states publish PDF only and will need to convert.
                </p>
              </div>
              <div style={{ minWidth: 140, textAlign: "center" }}>
                <div style={{ fontSize: 28, fontWeight: 800, fontFamily: FM, color: POS }}>
                  {summary.total > 0 ? Math.round((summary.machineReadable / summary.total) * 100) : 0}%
                </div>
                <div style={{ fontSize: 11, color: AL }}>Already compliant</div>
                <div style={{ width: 120, height: 8, background: BD, borderRadius: 4, marginTop: 6, overflow: "hidden" }}>
                  <div style={{
                    width: `${summary.total > 0 ? (summary.machineReadable / summary.total) * 100 : 0}%`,
                    height: "100%", background: POS, borderRadius: 4,
                  }} />
                </div>
              </div>
            </div>
          </Card>

          {/* Search + Filters */}
          <Card>
            <div style={{ display: "flex", gap: 12, flexWrap: "wrap", alignItems: "center" }}>
              <input
                placeholder="Search states, agencies, or methodologies..."
                value={search} onChange={e => setSearch(e.target.value)}
                style={{
                  flex: 1, minWidth: 200, padding: "8px 12px", borderRadius: 8,
                  border: `1px solid ${BD}`, fontSize: 13, fontFamily: FB, outline: "none",
                }}
              />
              <ExportBtn label="Export CSV" onClick={() => {
                downloadCSV(
                  ["State", "Abbr", "Agency", "Methodology", "Method Type", "Conversion Factors", "Format", "Access", "Machine-Readable", "Verified", "Total Spend", "FMAP", "URL"],
                  filtered.map(d => [
                    d.state, d.abbr, d.agency, d.methodology, d.methodType,
                    d.conversion_factors.map(c => `${c.name}: $${c.value.toFixed(4)}`).join("; "),
                    d.formats.join("; "), d.accessTag, d.machineReadable ? "Yes" : "No",
                    d.verified ? "Yes" : "No", d.total_spend.toFixed(0), d.fmap.toFixed(1), d.url,
                  ]),
                  "state_fee_schedule_directory.csv",
                );
              }} />
            </div>
            <div style={{ marginTop: 10 }}>
              <div style={{ fontSize: 11, color: AL, marginBottom: 4 }}>Methodology</div>
              <Pill label="All" on={methodFilter === "All"} onClick={() => setMethodFilter("All")} />
              {methodTypes.map(m => (
                <Pill key={m} label={m} on={methodFilter === m} onClick={() => setMethodFilter(methodFilter === m ? "All" : m)} />
              ))}
            </div>
            <div style={{ marginTop: 8 }}>
              <div style={{ fontSize: 11, color: AL, marginBottom: 4 }}>Format</div>
              <Pill label="All" on={formatFilter === "All"} onClick={() => setFormatFilter("All")} />
              {formatTags.map(f => (
                <Pill key={f} label={f} on={formatFilter === f} onClick={() => setFormatFilter(formatFilter === f ? "All" : f)} />
              ))}
            </div>
            <div style={{ marginTop: 8 }}>
              <div style={{ fontSize: 11, color: AL, marginBottom: 4 }}>Access</div>
              <Pill label="All" on={accessFilter === "All"} onClick={() => setAccessFilter("All")} />
              {accessTags.map(t => (
                <Pill key={t} label={t} on={accessFilter === t} onClick={() => setAccessFilter(accessFilter === t ? "All" : t)} />
              ))}
            </div>
          </Card>

          {/* State Directory Table */}
          <Card>
            <CH title={`${filtered.length} States`}
              sub={activeFilters ? `Filtered: ${activeFilters}` : "All states"} />
            <div style={{ overflowX: "auto" }}>
              <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 12, fontFamily: FM }}>
                <thead>
                  <tr style={{ borderBottom: `2px solid ${BD}` }}>
                    {["State", "Agency", "Methodology", "Format", "Access", "Total Spend", ""].map(h => (
                      <th key={h} style={{ padding: "8px 6px", textAlign: h === "Total Spend" ? "right" : "left", color: AL, fontWeight: 600, fontSize: 11, whiteSpace: "nowrap" }}>{h}</th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {filtered.length === 0 && (
                    <tr><td colSpan={7} style={{ padding: "20px 8px", textAlign: "center", color: AL, fontSize: 11 }}>No states match your filters.</td></tr>
                  )}
                  {filtered.map(d => (
                    <React.Fragment key={d.abbr || d.state}>
                      <tr
                        onClick={() => setExpanded(expanded === d.state ? null : d.state)}
                        style={{
                          borderBottom: `1px solid ${BD}`, cursor: "pointer",
                          background: expanded === d.state ? SF : undefined,
                        }}
                      >
                        <td style={{ padding: "10px 6px", fontWeight: 600, color: A, whiteSpace: "nowrap", minWidth: 130 }}>
                          <span style={{ marginRight: 6, fontSize: 10, color: AL }}>{expanded === d.state ? "\u25BE" : "\u25B8"}</span>
                          {d.abbr ? `${d.abbr}` : ""}{" "}
                          <span style={{ fontWeight: 400, color: AL }}>{d.state}</span>
                          {d.verified && <span title="Verified Mar 2026" style={{ marginLeft: 4, color: POS, fontSize: 10 }}>&#10003;</span>}
                        </td>
                        <td style={{ padding: "8px 6px", color: AL, maxWidth: 180, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>
                          {d.agency}
                        </td>
                        <td style={{ padding: "8px 6px" }}>
                          <span style={{
                            display: "inline-block", padding: "2px 8px", borderRadius: 10, fontSize: 10,
                            fontWeight: 600, color: WH, background: METHOD_COLORS[d.methodType],
                          }}>{d.methodType}</span>
                        </td>
                        <td style={{ padding: "8px 6px" }}>
                          <div style={{ display: "flex", gap: 4, flexWrap: "wrap" }}>
                            {d.formats.map(f => (
                              <span key={f} style={{
                                display: "inline-block", padding: "2px 7px", borderRadius: 4, fontSize: 10,
                                fontWeight: 700, color: WH, background: FORMAT_COLORS[f], letterSpacing: 0.3,
                              }}>{f}</span>
                            ))}
                          </div>
                        </td>
                        <td style={{ padding: "8px 6px" }}>
                          <span style={{
                            display: "inline-block", padding: "2px 8px", borderRadius: 10, fontSize: 10,
                            fontWeight: 600, color: ACCESS_COLORS[d.accessTag].fg,
                            background: ACCESS_COLORS[d.accessTag].bg,
                          }}>{d.accessTag}</span>
                        </td>
                        <td style={{ padding: "8px 6px", textAlign: "right", fontFamily: FM }}>
                          {d.total_spend > 0 ? f$(d.total_spend) : "\u2014"}
                        </td>
                        <td style={{ padding: "8px 6px" }}>
                          {d.url && (
                            <a href={d.url} target="_blank" rel="noopener noreferrer"
                              onClick={e => e.stopPropagation()}
                              style={{
                                display: "inline-block", padding: "4px 12px", borderRadius: 6,
                                background: cB, color: WH, fontSize: 11, fontWeight: 600,
                                textDecoration: "none", whiteSpace: "nowrap", fontFamily: FB,
                              }}>
                              Open &rarr;
                            </a>
                          )}
                        </td>
                      </tr>
                      {expanded === d.state && (
                        <tr>
                          <td colSpan={7} style={{ padding: "12px 16px 16px", background: SF, borderBottom: `1px solid ${BD}` }}>
                            <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 16, fontSize: 12 }}>
                              <div>
                                <div style={{ fontWeight: 700, color: A, marginBottom: 6 }}>Methodology Detail</div>
                                <p style={{ margin: 0, color: AL, lineHeight: 1.6 }}>{d.methodology}</p>
                              </div>
                              <div>
                                <div style={{ fontWeight: 700, color: A, marginBottom: 6 }}>Fee Schedule Access</div>
                                <p style={{ margin: "0 0 4px", color: AL, lineHeight: 1.6 }}>{d.access}</p>
                                <div style={{ marginTop: 4 }}>
                                  <span style={{ fontSize: 11, color: AL }}>Machine-readable: </span>
                                  {d.machineReadable
                                    ? <span style={{ color: POS, fontWeight: 600, fontSize: 11 }}>Yes (Excel/CSV)</span>
                                    : <span style={{ color: NEG, fontWeight: 600, fontSize: 11 }}>No (PDF only)</span>}
                                </div>
                                {d.url && (
                                  <a href={d.url} target="_blank" rel="noopener noreferrer"
                                    style={{ color: cB, fontWeight: 600, fontSize: 12, textDecoration: "none", display: "inline-block", marginTop: 6 }}>
                                    View Fee Schedule →
                                  </a>
                                )}
                              </div>
                            </div>
                            {(d.total_spend > 0 || d.fmap > 0 || d.conversion_factors.length > 0) && (
                              <div style={{ display: "flex", gap: 24, marginTop: 12, paddingTop: 12, borderTop: `1px solid ${BD}`, flexWrap: "wrap" }}>
                                {d.conversion_factors.map(c => (
                                  <div key={c.name}><span style={{ color: AL }}>{c.name} CF:</span> <span style={{ fontWeight: 600, color: cB, fontFamily: FM }}>${c.value.toFixed(4)}</span></div>
                                ))}
                                {d.total_spend > 0 && <div><span style={{ color: AL }}>Total Spend:</span> <span style={{ fontWeight: 600, color: A }}>{f$(d.total_spend)}</span></div>}
                                {d.total_claims > 0 && <div><span style={{ color: AL }}>Claims:</span> <span style={{ fontWeight: 600, color: A }}>{f$(d.total_claims).replace("$", "")}</span></div>}
                                {d.n_providers > 0 && <div><span style={{ color: AL }}>Providers:</span> <span style={{ fontWeight: 600, color: A }}>{d.n_providers.toLocaleString()}</span></div>}
                                {d.fmap > 0 && <div><span style={{ color: AL }}>FMAP:</span> <span style={{ fontWeight: 600, color: A }}>{d.fmap.toFixed(1)}%</span></div>}
                              </div>
                            )}
                            {d.cf_notes && (
                              <div style={{ marginTop: 8, fontSize: 11, color: AL, fontStyle: "italic", lineHeight: 1.5 }}>
                                {d.cf_notes}
                              </div>
                            )}
                          </td>
                        </tr>
                      )}
                    </React.Fragment>
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
                <strong>Methodology types:</strong>{" "}
                <em>RBRVS</em> = Resource-Based Relative Value Scale (CMS RVUs x state conversion factor).{" "}
                <em>% Medicare</em> = rates set as a percentage of Medicare PFS.{" "}
                <em>Custom CF</em> = state-developed conversion factors with state-specific adjustments.{" "}
                <em>Cost-Based</em> = rates derived from provider cost reports (common for facilities).{" "}
                <em>Mixed</em> = multiple methodologies for different service categories.
              </p>
              <p style={{ margin: "0 0 8px" }}>
                <strong>Machine-Readable:</strong> States publishing in Excel or CSV format. The CMS Ensuring Access
                Final Rule (42 CFR 447.203) requires machine-readable FFS rate publication by July 1, 2026.
              </p>
              <p style={{ margin: "0 0 8px" }}>
                <strong>Access types:</strong>{" "}
                <em>Public</em> = freely downloadable.{" "}
                <em>Click-through</em> = requires CPT/CDT license acceptance.{" "}
                <em>Login</em> = requires provider credentials.{" "}
                <em>Portal</em> = requires navigating a state portal.
              </p>
              <p style={{ margin: "0 0 8px" }}>
                URLs verified as of March 2026. Spending data from T-MSIS (2018-2024 aggregated).
                State websites change frequently; if a link is broken, check the state Medicaid agency's main provider page.
              </p>
              <p style={{ margin: 0 }}>
                This directory covers physician/professional fee schedules. Most states publish separate schedules for hospital
                outpatient, DME, lab, dental, and pharmacy. In managed care states, FFS rates typically serve as a floor or
                reference. MCO contracted rates may differ.
              </p>
            </div>
          </Card>
        </>
      )}
      <div style={{ fontSize: 10, color: AL, marginTop: 8 }}>Aradune State Fee Schedule Directory v1.0 · 42 CFR 447.203 · T-MSIS + State Agency Data</div>
    </div>
  );
}
