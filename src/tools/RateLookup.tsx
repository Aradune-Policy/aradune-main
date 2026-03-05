/**
 * Rate Lookup
 * Type a HCPCS code, see every state's fee schedule rate vs Medicare in one table.
 * Uses fee_schedule_rates.json (code-centric, 16K+ codes across 40+ states).
 */
import { useState, useEffect, useMemo, useCallback } from "react";

// ── Design tokens ───────────────────────────────────────────────────────
const A = "#0A2540", AL = "#425A70", POS = "#2E6B4A", NEG = "#A4262C", WARN = "#B8860B";
const SF = "#F5F7F5", BD = "#E4EAE4", WH = "#fff", cB = "#2E6B4A";
const FM = "'SF Mono',Menlo,Consolas,monospace";
const FB = "'Helvetica Neue',Arial,sans-serif";
const SH = "0 1px 3px rgba(0,0,0,.04),0 4px 12px rgba(0,0,0,.03)";

const STATE_NAMES: Record<string, string> = {AL:"Alabama",AK:"Alaska",AZ:"Arizona",AR:"Arkansas",CA:"California",CO:"Colorado",CT:"Connecticut",DE:"Delaware",DC:"D.C.",FL:"Florida",GA:"Georgia",HI:"Hawaii",ID:"Idaho",IL:"Illinois",IN:"Indiana",IA:"Iowa",KS:"Kansas",KY:"Kentucky",LA:"Louisiana",ME:"Maine",MD:"Maryland",MA:"Massachusetts",MI:"Michigan",MN:"Minnesota",MS:"Mississippi",MO:"Missouri",MT:"Montana",NE:"Nebraska",NV:"Nevada",NH:"New Hampshire",NJ:"New Jersey",NM:"New Mexico",NY:"New York",NC:"N. Carolina",ND:"N. Dakota",OH:"Ohio",OK:"Oklahoma",OR:"Oregon",PA:"Pennsylvania",RI:"Rhode Island",SC:"S. Carolina",SD:"S. Dakota",TN:"Tennessee",TX:"Texas",UT:"Utah",VT:"Vermont",VA:"Virginia",WA:"Washington",WV:"W. Virginia",WI:"Wisconsin",WY:"Wyoming",PR:"Puerto Rico",GU:"Guam",VI:"Virgin Islands"};

// ── UI primitives ───────────────────────────────────────────────────────
const Card = ({ children, accent }: { children: React.ReactNode; accent?: string }) => (
  <div style={{ background: WH, borderRadius: 12, boxShadow: SH, padding: "20px 24px",
    borderTop: accent ? `3px solid ${accent}` : undefined, marginBottom: 20 }}>{children}</div>
);
const CH = ({ title, sub, right }: { title: string; sub?: string; right?: React.ReactNode }) => (
  <div style={{ display: "flex", justifyContent: "space-between", alignItems: "baseline", marginBottom: 14, flexWrap: "wrap", gap: 8 }}>
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

// ── Types ───────────────────────────────────────────────────────────────
interface CodeEntry {
  states: Record<string, number>;
  desc: string;
  medicare?: number;
}

type SortKey = "state" | "rate" | "pct";

// ═════════════════════════════════════════════════════════════════════════
export default function RateLookup() {
  const [data, setData] = useState<Record<string, CodeEntry> | null>(null);
  const [loading, setLoading] = useState(true);
  const [query, setQuery] = useState("");
  const [selectedCode, setSelectedCode] = useState<string | null>(null);
  const [sortKey, setSortKey] = useState<SortKey>("pct");
  const [sortAsc, setSortAsc] = useState(true);

  useEffect(() => {
    fetch("/data/fee_schedule_rates.json")
      .then(r => r.json())
      .then(d => { setData(d); setLoading(false); })
      .catch(() => setLoading(false));
  }, []);

  // Search results: match code or description
  const searchResults = useMemo(() => {
    if (!data || query.length < 2) return [];
    const q = query.toUpperCase().trim();
    const results: { code: string; desc: string; states: number; medicare: number | null }[] = [];
    for (const [code, entry] of Object.entries(data)) {
      if (results.length >= 50) break;
      if (code.includes(q) || entry.desc.toUpperCase().includes(q)) {
        results.push({
          code,
          desc: entry.desc,
          states: Object.keys(entry.states).length,
          medicare: entry.medicare ?? null,
        });
      }
    }
    // Exact match first, then by state count
    results.sort((a, b) => {
      if (a.code === q) return -1;
      if (b.code === q) return 1;
      return b.states - a.states;
    });
    return results;
  }, [data, query]);

  // Auto-select on exact match
  useEffect(() => {
    if (searchResults.length > 0 && searchResults[0].code === query.toUpperCase().trim()) {
      setSelectedCode(searchResults[0].code);
    }
  }, [searchResults, query]);

  // Selected code analysis
  const analysis = useMemo(() => {
    if (!data || !selectedCode || !data[selectedCode]) return null;
    const entry = data[selectedCode];
    const medicare = entry.medicare ?? null;

    const stateRows = Object.entries(entry.states)
      .map(([st, rate]) => ({
        st,
        name: STATE_NAMES[st] || st,
        rate,
        pctMedicare: medicare && medicare > 0 ? (rate / medicare) * 100 : null,
      }));

    // Sort
    stateRows.sort((a, b) => {
      let cmp = 0;
      if (sortKey === "state") cmp = a.name.localeCompare(b.name);
      else if (sortKey === "rate") cmp = a.rate - b.rate;
      else cmp = (a.pctMedicare ?? 999) - (b.pctMedicare ?? 999);
      return sortAsc ? cmp : -cmp;
    });

    const rates = stateRows.map(s => s.rate);
    const pcts = stateRows.filter(s => s.pctMedicare != null).map(s => s.pctMedicare!);
    const sorted = [...rates].sort((a, b) => a - b);
    const medianRate = sorted[Math.floor(sorted.length / 2)] || 0;
    const sortedPcts = [...pcts].sort((a, b) => a - b);
    const medianPct = sortedPcts.length > 0 ? sortedPcts[Math.floor(sortedPcts.length / 2)] : null;
    const below80 = pcts.filter(p => p < 80).length;

    return {
      code: selectedCode,
      desc: entry.desc,
      medicare,
      states: stateRows,
      stats: {
        count: stateRows.length,
        min: sorted[0] || 0,
        max: sorted[sorted.length - 1] || 0,
        median: medianRate,
        medianPct,
        below80,
      },
    };
  }, [data, selectedCode, sortKey, sortAsc]);

  const handleSort = useCallback((key: SortKey) => {
    if (sortKey === key) setSortAsc(!sortAsc);
    else { setSortKey(key); setSortAsc(key === "state"); }
  }, [sortKey, sortAsc]);

  const totalCodes = data ? Object.keys(data).length : 0;

  return (
    <div style={{ maxWidth: 1000, margin: "0 auto", padding: "24px 16px", fontFamily: FB }}>
      <h2 style={{ fontSize: 22, fontWeight: 800, color: A, margin: "0 0 4px" }}>Rate Lookup</h2>
      <p style={{ fontSize: 13, color: AL, margin: "0 0 20px" }}>
        Search any HCPCS code to compare fee schedule rates across every state with Medicare as the benchmark
      </p>

      {loading ? (
        <Card><p style={{ color: AL, fontSize: 13, textAlign: "center", padding: 40 }}>Loading fee schedule data...</p></Card>
      ) : !data ? (
        <Card><p style={{ color: AL, fontSize: 13, textAlign: "center", padding: 40 }}>Fee schedule data not available.</p></Card>
      ) : (
        <>
          {/* Search */}
          <Card accent={cB}>
            <div style={{ display: "flex", gap: 12, alignItems: "center", flexWrap: "wrap" }}>
              <input
                value={query}
                onChange={e => { setQuery(e.target.value); if (e.target.value.length < 2) setSelectedCode(null); }}
                placeholder="Enter HCPCS code or description (e.g., 99213, office visit)..."
                style={{
                  flex: 1, minWidth: 260, padding: "10px 14px", borderRadius: 8,
                  border: `1px solid ${BD}`, fontSize: 14, fontFamily: FM, outline: "none",
                  color: A,
                }}
                onKeyDown={e => {
                  if (e.key === "Enter" && searchResults.length > 0) {
                    setSelectedCode(searchResults[0].code);
                    setQuery(searchResults[0].code);
                  }
                }}
              />
              <span style={{ fontSize: 11, color: AL }}>{totalCodes.toLocaleString()} codes available</span>
            </div>

            {/* Search results dropdown */}
            {query.length >= 2 && !selectedCode && searchResults.length > 0 && (
              <div style={{ marginTop: 8, maxHeight: 240, overflowY: "auto", border: `1px solid ${BD}`, borderRadius: 8, background: WH }}>
                {searchResults.slice(0, 20).map(r => (
                  <div key={r.code}
                    onClick={() => { setSelectedCode(r.code); setQuery(r.code); }}
                    style={{
                      padding: "8px 12px", cursor: "pointer", borderBottom: `1px solid ${SF}`,
                      display: "flex", gap: 12, alignItems: "center",
                    }}
                    onMouseEnter={e => e.currentTarget.style.background = SF}
                    onMouseLeave={e => e.currentTarget.style.background = WH}
                  >
                    <span style={{ fontFamily: FM, fontWeight: 700, color: cB, minWidth: 60 }}>{r.code}</span>
                    <span style={{ flex: 1, fontSize: 12, color: AL, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>{r.desc}</span>
                    <span style={{ fontSize: 10, color: AL, flexShrink: 0 }}>{r.states} states</span>
                    {r.medicare && <span style={{ fontSize: 10, color: AL, flexShrink: 0, fontFamily: FM }}>MCR ${r.medicare.toFixed(2)}</span>}
                  </div>
                ))}
                {searchResults.length > 20 && (
                  <div style={{ padding: "6px 12px", fontSize: 11, color: AL, textAlign: "center" }}>
                    {searchResults.length - 20} more results...
                  </div>
                )}
              </div>
            )}
            {query.length >= 2 && searchResults.length === 0 && (
              <div style={{ marginTop: 8, fontSize: 12, color: AL }}>No matching codes found.</div>
            )}
          </Card>

          {/* Analysis */}
          {analysis && (
            <>
              {/* Summary */}
              <Card>
                <CH title={`${analysis.code}`} sub={analysis.desc}
                  right={analysis.medicare ? (
                    <span style={{ fontSize: 13, fontFamily: FM, color: A }}>
                      Medicare: <strong>${analysis.medicare.toFixed(2)}</strong>
                    </span>
                  ) : undefined}
                />
                <div style={{ display: "flex", justifyContent: "space-around", flexWrap: "wrap", gap: 16 }}>
                  <Met label="States" value={`${analysis.stats.count}`} />
                  <Met label="Lowest" value={`$${analysis.stats.min.toFixed(2)}`} color={NEG} />
                  <Met label="Median" value={`$${analysis.stats.median.toFixed(2)}`} />
                  <Met label="Highest" value={`$${analysis.stats.max.toFixed(2)}`} color={POS} />
                  {analysis.stats.medianPct != null && (
                    <Met label="Median % MCR" value={`${analysis.stats.medianPct.toFixed(0)}%`}
                      color={analysis.stats.medianPct < 80 ? WARN : POS} />
                  )}
                  {analysis.stats.below80 > 0 && (
                    <Met label="Below 80% MCR" value={`${analysis.stats.below80}`} color={WARN}
                      sub={`of ${analysis.stats.count} states`} />
                  )}
                </div>
              </Card>

              {/* Bar visualization */}
              {analysis.medicare && analysis.medicare > 0 && (
                <Card>
                  <CH title="State Rates vs Medicare" sub={`Medicare: $${analysis.medicare.toFixed(2)}`} />
                  <div style={{ maxHeight: 500, overflowY: "auto" }}>
                    {[...analysis.states].sort((a, b) => (a.pctMedicare ?? 0) - (b.pctMedicare ?? 0)).map(s => (
                      <div key={s.st} style={{ display: "flex", alignItems: "center", gap: 8, marginBottom: 3 }}>
                        <div style={{ width: 28, fontSize: 10, fontFamily: FM, color: A, textAlign: "right", fontWeight: 600 }}>{s.st}</div>
                        <div style={{ flex: 1, height: 14, background: SF, borderRadius: 3, overflow: "hidden", position: "relative" }}>
                          <div style={{
                            width: `${Math.min((s.pctMedicare ?? 0) / 1.5, 100)}%`,
                            height: "100%", borderRadius: 3,
                            background: (s.pctMedicare ?? 0) < 50 ? NEG : (s.pctMedicare ?? 0) < 80 ? WARN : POS,
                          }} />
                          <div style={{
                            position: "absolute", left: `${100 / 1.5}%`, top: 0, bottom: 0,
                            width: 1.5, background: A, opacity: 0.4,
                          }} />
                        </div>
                        <div style={{ width: 50, fontSize: 10, fontFamily: FM, textAlign: "right", color: AL }}>${s.rate.toFixed(2)}</div>
                        <div style={{ width: 40, fontSize: 10, fontFamily: FM, textAlign: "right", fontWeight: 600,
                          color: (s.pctMedicare ?? 0) < 50 ? NEG : (s.pctMedicare ?? 0) < 80 ? WARN : POS,
                        }}>{s.pctMedicare != null ? `${s.pctMedicare.toFixed(0)}%` : ""}</div>
                      </div>
                    ))}
                  </div>
                  <div style={{ display: "flex", gap: 12, marginTop: 8, fontSize: 10, color: AL }}>
                    <span><span style={{ display: "inline-block", width: 8, height: 8, borderRadius: 2, background: NEG, marginRight: 3 }} />Below 50%</span>
                    <span><span style={{ display: "inline-block", width: 8, height: 8, borderRadius: 2, background: WARN, marginRight: 3 }} />50-80%</span>
                    <span><span style={{ display: "inline-block", width: 8, height: 8, borderRadius: 2, background: POS, marginRight: 3 }} />80%+</span>
                    <span style={{ marginLeft: "auto" }}>| = Medicare parity</span>
                  </div>
                </Card>
              )}

              {/* State table */}
              <Card>
                <CH title={`${analysis.states.length} States`}
                  right={
                    <button onClick={() => {
                      downloadCSV(
                        ["State", "Abbr", "Fee Schedule Rate", "Medicare Rate", "% of Medicare"],
                        analysis.states.map(s => [
                          s.name, s.st, s.rate.toFixed(2),
                          analysis.medicare?.toFixed(2) ?? "",
                          s.pctMedicare != null ? s.pctMedicare.toFixed(1) : "",
                        ]),
                        `rate_lookup_${analysis.code}.csv`,
                      );
                    }} style={{
                      padding: "5px 12px", borderRadius: 6, border: `1px solid ${BD}`,
                      background: WH, color: AL, fontSize: 11, cursor: "pointer", fontFamily: FM,
                    }}>Export CSV</button>
                  }
                />
                <div style={{ overflowX: "auto" }}>
                  <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 12, fontFamily: FM }}>
                    <thead>
                      <tr style={{ borderBottom: `2px solid ${BD}` }}>
                        <th onClick={() => handleSort("state")} style={{ padding: "8px 6px", textAlign: "left", color: AL, fontWeight: 600, fontSize: 11, cursor: "pointer", userSelect: "none" }}>
                          State {sortKey === "state" ? (sortAsc ? "\u25B4" : "\u25BE") : ""}
                        </th>
                        <th onClick={() => handleSort("rate")} style={{ padding: "8px 6px", textAlign: "right", color: AL, fontWeight: 600, fontSize: 11, cursor: "pointer", userSelect: "none" }}>
                          Fee Schedule Rate {sortKey === "rate" ? (sortAsc ? "\u25B4" : "\u25BE") : ""}
                        </th>
                        {analysis.medicare && (
                          <>
                            <th style={{ padding: "8px 6px", textAlign: "right", color: AL, fontWeight: 600, fontSize: 11 }}>Medicare</th>
                            <th onClick={() => handleSort("pct")} style={{ padding: "8px 6px", textAlign: "right", color: AL, fontWeight: 600, fontSize: 11, cursor: "pointer", userSelect: "none" }}>
                              % of MCR {sortKey === "pct" ? (sortAsc ? "\u25B4" : "\u25BE") : ""}
                            </th>
                            <th style={{ padding: "8px 6px", textAlign: "right", color: AL, fontWeight: 600, fontSize: 11 }}>Gap</th>
                          </>
                        )}
                      </tr>
                    </thead>
                    <tbody>
                      {analysis.states.map(s => (
                        <tr key={s.st} style={{ borderBottom: `1px solid ${BD}` }}>
                          <td style={{ padding: "8px 6px", fontWeight: 600, color: A }}>
                            {s.st} <span style={{ fontWeight: 400, color: AL }}>{s.name}</span>
                          </td>
                          <td style={{ padding: "8px 6px", textAlign: "right", fontWeight: 600 }}>${s.rate.toFixed(2)}</td>
                          {analysis.medicare && (
                            <>
                              <td style={{ padding: "8px 6px", textAlign: "right", color: AL }}>${analysis.medicare.toFixed(2)}</td>
                              <td style={{ padding: "8px 6px", textAlign: "right", fontWeight: 700,
                                color: s.pctMedicare == null ? AL : s.pctMedicare < 50 ? NEG : s.pctMedicare < 80 ? WARN : POS,
                              }}>{s.pctMedicare != null ? `${s.pctMedicare.toFixed(1)}%` : "\u2014"}</td>
                              <td style={{ padding: "8px 6px", textAlign: "right",
                                color: s.pctMedicare != null && s.pctMedicare < 100 ? NEG : POS,
                              }}>{analysis.medicare ? `${s.rate >= analysis.medicare ? "+" : ""}$${(s.rate - analysis.medicare).toFixed(2)}` : ""}</td>
                            </>
                          )}
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </Card>

              {/* Notes */}
              <Card>
                <div style={{ fontSize: 12, color: AL, lineHeight: 1.7 }}>
                  <p style={{ margin: "0 0 8px" }}>
                    <strong>Fee schedule rates</strong> are official state-published rates, not T-MSIS actual-paid averages.
                    Where a state publishes facility/non-facility rates, the non-facility (higher) rate is shown.
                    Rates reflect base codes without modifiers.
                  </p>
                  <p style={{ margin: 0 }}>
                    <strong>Medicare rate</strong> is the CY2026 Medicare Physician Fee Schedule non-facility rate.
                    % of Medicare = (state fee schedule rate / Medicare rate) x 100.
                  </p>
                </div>
              </Card>
            </>
          )}

          {/* Quick codes when nothing selected */}
          {/* Data Sources */}
          {selectedCode && (
            <Card>
              <CH title="Data Sources & Methodology" />
              <div style={{ padding: "0 0 4px", fontSize: 11, color: AL, lineHeight: 1.8 }}>
                <strong>Fee schedule rates</strong> compiled from state Medicaid agency publications (42 state agencies as of March 2026).
                Medicare rates from CY2026 Medicare Physician Fee Schedule.
                Rates shown are base rates without modifiers. Actual reimbursement may vary by modifier, place of service, and locality.
                <br/><br/>
                For state-level methodology and conversion factor details, see the{" "}
                <a href="#/fees" style={{ color: cB }}>State Fee Schedule Directory</a>.
              </div>
            </Card>
          )}

          {!selectedCode && (
            <Card>
              <CH title="Common Codes" sub="Click a code to view cross-state comparison" />
              <div style={{ display: "flex", flexWrap: "wrap", gap: 6 }}>
                {["99213", "99214", "99215", "99203", "99204", "90834", "90837", "97110", "92507", "99385", "99394", "36415", "90460", "97530", "10060"].map(code => {
                  const entry = data?.[code];
                  return entry ? (
                    <button key={code} onClick={() => { setSelectedCode(code); setQuery(code); }}
                      style={{
                        padding: "6px 14px", borderRadius: 8, border: `1px solid ${BD}`,
                        background: WH, cursor: "pointer", fontFamily: FM, fontSize: 11,
                        display: "flex", flexDirection: "column", alignItems: "center", gap: 2,
                        minWidth: 80,
                      }}
                      onMouseEnter={e => e.currentTarget.style.borderColor = cB}
                      onMouseLeave={e => e.currentTarget.style.borderColor = BD}
                    >
                      <span style={{ fontWeight: 700, color: cB }}>{code}</span>
                      <span style={{ fontSize: 9, color: AL }}>{Object.keys(entry.states).length} states</span>
                    </button>
                  ) : null;
                })}
              </div>
            </Card>
          )}
        </>
      )}
      <div style={{ fontSize: 10, color: AL, marginTop: 8 }}>Aradune Rate Lookup v1.0 · State Fee Schedules (42 states) + CY2026 Medicare PFS</div>
    </div>
  );
}
