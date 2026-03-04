/**
 * Fee Schedule Directory
 * Central directory of every state's published Medicaid fee schedule.
 * Focus: quick access to URLs, format badges, access requirements,
 * and CMS machine-readable compliance readiness (July 2026 deadline).
 */
import { useState, useEffect, useMemo } from "react";
import { STATE_NAMES } from "../data/states";

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

/** Machine-readable = Excel or CSV (vs PDF-only or Web-lookup-only) */
function isMachineReadable(formats: FormatTag[]): boolean {
  return formats.includes("Excel") || formats.includes("CSV");
}

// ── Types ───────────────────────────────────────────────────────────────
interface DirEntry {
  state: string; agency: string; url: string; format: string;
  access: string; methodology: string; verified: boolean;
}

// ═════════════════════════════════════════════════════════════════════════
export default function FeeScheduleDir() {
  const [directory, setDirectory] = useState<DirEntry[]>([]);
  const [search, setSearch] = useState("");
  const [formatFilter, setFormatFilter] = useState<FormatTag | "All">("All");
  const [accessFilter, setAccessFilter] = useState<AccessTag | "All">("All");
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    let cancelled = false;
    fetch("/data/fee_schedule_directory.json").then(r => { if (!r.ok) throw new Error("Failed"); return r.json(); }).then(data => {
      if (cancelled) return;
      // Filter out reference notes (no agency = not a real state entry)
      const real = (data as { directory: DirEntry[] }).directory.filter(d => d.agency);
      setDirectory(real);
      setLoading(false);
    }).catch(() => { if (!cancelled) setLoading(false); });
    return () => { cancelled = true; };
  }, []);

  // Map state name → abbreviation
  const nameToAbbr = useMemo(() => {
    const m = new Map<string, string>();
    for (const [abbr, name] of Object.entries(STATE_NAMES)) m.set(name, abbr);
    m.set("District of Columbia", "DC");
    return m;
  }, []);

  // Enrich with parsed formats + access classification
  const enriched = useMemo(() =>
    directory.map(d => {
      const abbr = nameToAbbr.get(d.state) ?? "";
      const formats = parseFormats(d.format);
      const accessTag = classifyAccess(d.access);
      const machineReadable = isMachineReadable(formats);
      return { ...d, abbr, formats, accessTag, machineReadable };
    }),
  [directory, nameToAbbr]);

  // Filter
  const filtered = useMemo(() => {
    let list = enriched;
    if (search) {
      const q = search.toLowerCase();
      list = list.filter(d =>
        d.state.toLowerCase().includes(q) ||
        d.abbr.toLowerCase().includes(q) ||
        d.agency.toLowerCase().includes(q)
      );
    }
    if (formatFilter !== "All") {
      list = list.filter(d => d.formats.includes(formatFilter));
    }
    if (accessFilter !== "All") {
      list = list.filter(d => d.accessTag === accessFilter);
    }
    return list.sort((a, b) => a.state.localeCompare(b.state));
  }, [enriched, search, formatFilter, accessFilter]);

  // Summary stats
  const summary = useMemo(() => {
    const verified = enriched.filter(d => d.verified).length;
    const machineReadable = enriched.filter(d => d.machineReadable).length;
    const pdfOnly = enriched.filter(d => d.formats.length === 1 && d.formats[0] === "PDF").length;
    const publicAccess = enriched.filter(d => d.accessTag === "Public").length;
    const clickThrough = enriched.filter(d => d.accessTag === "Click-through").length;
    return { total: enriched.length, verified, machineReadable, pdfOnly, publicAccess, clickThrough };
  }, [enriched]);

  const formatTags: FormatTag[] = ["Excel", "PDF", "CSV", "Web", "Text"];
  const accessTags: AccessTag[] = ["Public", "Click-through", "Login", "Portal"];

  // ── Render ────────────────────────────────────────────────────────────
  return (
    <div style={{ maxWidth: 1100, margin: "0 auto", padding: "24px 16px", fontFamily: FB }}>
      <h2 style={{ fontSize: 22, fontWeight: 800, color: A, margin: "0 0 4px" }}>Fee Schedule Directory</h2>
      <p style={{ fontSize: 13, color: AL, margin: "0 0 20px" }}>
        Direct links to every state's published Medicaid fee schedule: format, access requirements, and compliance readiness
      </p>

      {loading ? (
        <Card><p style={{ color: AL, fontSize: 13, textAlign: "center", padding: 40 }}>Loading directory...</p></Card>
      ) : (
        <>
          {/* Summary KPIs */}
          <Card accent={cB}>
            <CH title="Directory Overview" sub={`${summary.total} states & territories`} />
            <div style={{ display: "flex", justifyContent: "space-around", flexWrap: "wrap", gap: 16 }}>
              <Met label="Total States" value={`${summary.total}`} color={A} />
              <Met label="Machine-Readable" value={`${summary.machineReadable}`} color={POS} />
              <Met label="PDF Only" value={`${summary.pdfOnly}`} color={NEG} />
              <Met label="Public Access" value={`${summary.publicAccess}`} color={POS} />
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
              {/* Compliance gauge */}
              <div style={{ minWidth: 140, textAlign: "center" }}>
                <div style={{ fontSize: 28, fontWeight: 800, fontFamily: FM, color: POS }}>
                  {summary.total > 0 ? Math.round((summary.machineReadable / summary.total) * 100) : 0}%
                </div>
                <div style={{ fontSize: 11, color: AL }}>Already compliant</div>
                <div style={{
                  width: 120, height: 8, background: BD, borderRadius: 4, marginTop: 6, overflow: "hidden",
                }}>
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
                placeholder="Search states or agencies..."
                value={search} onChange={e => setSearch(e.target.value)}
                style={{
                  flex: 1, minWidth: 200, padding: "8px 12px", borderRadius: 8,
                  border: `1px solid ${BD}`, fontSize: 13, fontFamily: FB, outline: "none",
                }}
              />
              <ExportBtn label="Export CSV" onClick={() => {
                downloadCSV(
                  ["State", "Abbr", "Agency", "URL", "Formats", "Access", "Machine-Readable", "Verified"],
                  filtered.map(d => [
                    d.state, d.abbr, d.agency, d.url, d.formats.join("; "),
                    d.accessTag, d.machineReadable ? "Yes" : "No", d.verified ? "Yes" : "No",
                  ]),
                  "fee_schedule_directory.csv",
                );
              }} />
            </div>
            <div style={{ marginTop: 10 }}>
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
              sub={formatFilter !== "All" || accessFilter !== "All"
                ? `Filtered: ${[formatFilter !== "All" ? formatFilter : "", accessFilter !== "All" ? accessFilter : ""].filter(Boolean).join(", ")}`
                : "All states"} />
            <div style={{ overflowX: "auto" }}>
              <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 12, fontFamily: FM }}>
                <thead>
                  <tr style={{ borderBottom: `2px solid ${BD}` }}>
                    {["State", "Agency", "Format", "Access", "MR", ""].map(h => (
                      <th key={h} style={{ padding: "8px 6px", textAlign: "left", color: AL, fontWeight: 600, fontSize: 11, whiteSpace: "nowrap" }}>
                        {h === "MR" ? <span title="Machine-Readable (Excel/CSV)">MR</span> : h}
                      </th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {filtered.length === 0 && <tr><td colSpan={6} style={{ padding: "20px 8px", textAlign: "center", color: AL, fontSize: 11 }}>No states match your filters.</td></tr>}
                  {filtered.map(d => (
                    <tr key={d.abbr || d.state} style={{ borderBottom: `1px solid ${BD}` }}>
                      <td style={{ padding: "10px 6px", fontWeight: 600, color: A, whiteSpace: "nowrap", minWidth: 120 }}>
                        {d.abbr ? `${d.abbr}` : ""}{" "}
                        <span style={{ fontWeight: 400, color: AL }}>{d.state}</span>
                        {d.verified && <span title="URL verified Feb 2026" style={{ marginLeft: 4, color: POS, fontSize: 10 }}>&#10003;</span>}
                      </td>
                      <td style={{ padding: "8px 6px", color: AL, maxWidth: 200, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>
                        {d.agency}
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
                      <td style={{ padding: "8px 6px", textAlign: "center" }}>
                        {d.machineReadable
                          ? <span style={{ color: POS, fontWeight: 700 }}>&#10003;</span>
                          : <span style={{ color: NEG, fontWeight: 700 }}>&#10007;</span>}
                      </td>
                      <td style={{ padding: "8px 6px" }}>
                        {d.url && (
                          <a href={d.url} target="_blank" rel="noopener noreferrer"
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
                <strong>Machine-Readable (MR):</strong> States publishing in Excel or CSV format. The CMS Ensuring Access
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
                URLs verified as of February 2026. State websites change frequently. If a link is broken, check the state
                Medicaid agency's main provider page.
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
    </div>
  );
}
