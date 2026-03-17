/**
 * Rate Explorer
 * Search a HCPCS/CPT code, see every state's Medicaid rate vs Medicare in a
 * horizontal bar chart and sortable table. Data from fact_rate_comparison_v2.
 */
import { useState, useEffect, useMemo, useCallback, useRef } from "react";
import { BarChart, Bar, XAxis, YAxis, Tooltip, ResponsiveContainer, Cell, ReferenceLine } from "recharts";
import { API_BASE } from "../lib/api";
import { useAradune } from "../context/AraduneContext";
import { C, FONT, SHADOW, useIsMobile } from "../design";

// ── Design tokens ───────────────────────────────────────────────────────
const A = C.ink, AL = C.inkLight, POS = C.pos, NEG = C.neg, WARN = C.warn;
const SF = C.surface, BD = C.border, WH = C.white, cB = C.brand;
const FM = FONT.mono, FB = FONT.body, SH = SHADOW;

const STATE_NAMES: Record<string, string> = {AL:"Alabama",AK:"Alaska",AZ:"Arizona",AR:"Arkansas",CA:"California",CO:"Colorado",CT:"Connecticut",DE:"Delaware",DC:"D.C.",FL:"Florida",GA:"Georgia",HI:"Hawaii",ID:"Idaho",IL:"Illinois",IN:"Indiana",IA:"Iowa",KS:"Kansas",KY:"Kentucky",LA:"Louisiana",ME:"Maine",MD:"Maryland",MA:"Massachusetts",MI:"Michigan",MN:"Minnesota",MS:"Mississippi",MO:"Missouri",MT:"Montana",NE:"Nebraska",NV:"Nevada",NH:"New Hampshire",NJ:"New Jersey",NM:"New Mexico",NY:"New York",NC:"N. Carolina",ND:"N. Dakota",OH:"Ohio",OK:"Oklahoma",OR:"Oregon",PA:"Pennsylvania",RI:"Rhode Island",SC:"S. Carolina",SD:"S. Dakota",TN:"Tennessee",TX:"Texas",UT:"Utah",VT:"Vermont",VA:"Virginia",WA:"Washington",WV:"W. Virginia",WI:"Wisconsin",WY:"Wyoming",PR:"Puerto Rico",GU:"Guam",VI:"Virgin Islands"};

// ── Types ───────────────────────────────────────────────────────────────
interface SearchResult {
  procedure_code: string;
  description: string;
  category: string;
  is_em_code: boolean;
  medicare_rate_nonfac: number | null;
}

interface RateRow {
  state_code: string;
  medicaid_rate: number;
  medicare_rate: number;
  pct_of_medicare: number;
  rate_source: string;
}

type SortKey = "state" | "medicaid" | "pct";

// ── Bar color by pct_of_medicare ────────────────────────────────────────
function barColor(pct: number): string {
  if (pct < 60) return NEG;
  if (pct < 80) return "#D97706";
  if (pct <= 120) return POS;
  return "#2563EB";
}

// ── Primitives ──────────────────────────────────────────────────────────
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
const Met = ({ label, value, color }: { label: string; value: string; color?: string }) => (
  <div style={{ textAlign: "center", minWidth: 90 }}>
    <div style={{ fontSize: 22, fontWeight: 700, color: color || A, fontFamily: FM }}>{value}</div>
    <div style={{ fontSize: 11, color: AL, marginTop: 2 }}>{label}</div>
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

// ═════════════════════════════════════════════════════════════════════════
export default function RateExplorer() {
  const { openIntelligence } = useAradune();
  const mobile = useIsMobile();

  const [query, setQuery] = useState("");
  const [searchResults, setSearchResults] = useState<SearchResult[]>([]);
  const [searchLoading, setSearchLoading] = useState(false);
  const [selectedCode, setSelectedCode] = useState<SearchResult | null>(null);
  const [rates, setRates] = useState<RateRow[]>([]);
  const [ratesLoading, setRatesLoading] = useState(false);
  const [sortKey, setSortKey] = useState<SortKey>("pct");
  const [sortAsc, setSortAsc] = useState(false);
  const [showDropdown, setShowDropdown] = useState(false);

  const debounceRef = useRef<ReturnType<typeof setTimeout> | null>(null);

  // ── Search as user types ─────────────────────────────────────────────
  useEffect(() => {
    if (query.length < 2) {
      setSearchResults([]);
      setShowDropdown(false);
      return;
    }
    setSearchLoading(true);
    if (debounceRef.current) clearTimeout(debounceRef.current);
    debounceRef.current = setTimeout(async () => {
      try {
        const res = await fetch(`${API_BASE}/api/rate-explorer/search?q=${encodeURIComponent(query)}`);
        if (res.ok) {
          const data = await res.json();
          setSearchResults(data.results || []);
          setShowDropdown(true);
        }
      } catch { /* ignore */ }
      setSearchLoading(false);
    }, 250);
    return () => { if (debounceRef.current) clearTimeout(debounceRef.current); };
  }, [query]);

  // ── Fetch rates when code selected ───────────────────────────────────
  useEffect(() => {
    if (!selectedCode) { setRates([]); return; }
    setRatesLoading(true);
    (async () => {
      try {
        const res = await fetch(`${API_BASE}/api/rate-explorer?code=${encodeURIComponent(selectedCode.procedure_code)}`);
        if (res.ok) {
          const data = await res.json();
          setRates(data.rows || []);
        }
      } catch { /* ignore */ }
      setRatesLoading(false);
    })();
  }, [selectedCode]);

  // ── Select a code from dropdown ──────────────────────────────────────
  const handleSelect = useCallback((r: SearchResult) => {
    setSelectedCode(r);
    setQuery(r.procedure_code);
    setShowDropdown(false);
    setSortKey("pct");
    setSortAsc(false);
  }, []);

  // ── Sorted rates ────────────────────────────────────────────────────
  const sortedRates = useMemo(() => {
    const arr = [...rates];
    arr.sort((a, b) => {
      let cmp = 0;
      if (sortKey === "state") cmp = (STATE_NAMES[a.state_code] || a.state_code).localeCompare(STATE_NAMES[b.state_code] || b.state_code);
      else if (sortKey === "medicaid") cmp = a.medicaid_rate - b.medicaid_rate;
      else cmp = a.pct_of_medicare - b.pct_of_medicare;
      return sortAsc ? cmp : -cmp;
    });
    return arr;
  }, [rates, sortKey, sortAsc]);

  // ── Chart data (always sorted by pct desc for the chart) ────────────
  const chartData = useMemo(() =>
    [...rates].sort((a, b) => b.pct_of_medicare - a.pct_of_medicare)
      .map(r => ({ ...r, name: r.state_code, pct: Math.round(r.pct_of_medicare) })),
    [rates],
  );

  // ── Summary stats ───────────────────────────────────────────────────
  const stats = useMemo(() => {
    if (rates.length === 0) return null;
    const pcts = rates.map(r => r.pct_of_medicare).sort((a, b) => a - b);
    const meds = rates.map(r => r.medicaid_rate).sort((a, b) => a - b);
    return {
      states: rates.length,
      medianPct: pcts[Math.floor(pcts.length / 2)],
      minRate: meds[0],
      maxRate: meds[meds.length - 1],
      below60: pcts.filter(p => p < 60).length,
      above120: pcts.filter(p => p > 120).length,
    };
  }, [rates]);

  const handleSort = useCallback((key: SortKey) => {
    if (sortKey === key) setSortAsc(!sortAsc);
    else { setSortKey(key); setSortAsc(key === "state"); }
  }, [sortKey, sortAsc]);

  const chartHeight = Math.max(300, chartData.length * 22 + 40);

  return (
    <div style={{ maxWidth: 1000, margin: "0 auto", padding: mobile ? "16px 12px" : "24px 16px", fontFamily: FB }}>
      {/* Header */}
      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "flex-start", marginBottom: 20 }}>
        <div>
          <h2 style={{ fontSize: 22, fontWeight: 800, color: A, margin: "0 0 4px" }}>Rate Explorer</h2>
          <p style={{ fontSize: 13, color: AL, margin: 0 }}>
            Look up any procedure code to see Medicaid rates across all jurisdictions, ranked by % of Medicare
          </p>
        </div>
        <button onClick={() => openIntelligence({ summary: selectedCode
          ? `User is viewing Rate Explorer for ${selectedCode.procedure_code} (${selectedCode.description}). ${rates.length} states loaded.`
          : "User is viewing Rate Explorer" })}
          style={{
            padding: "8px 14px", borderRadius: 8, border: "none", flexShrink: 0,
            background: cB, color: WH, fontSize: 12, cursor: "pointer", fontWeight: 600,
          }}>Ask Aradune</button>
      </div>

      {/* Search */}
      <Card accent={cB}>
        <div style={{ position: "relative" }}>
          <div style={{ display: "flex", gap: 12, alignItems: "center", flexWrap: "wrap" }}>
            <input
              value={query}
              onChange={e => {
                setQuery(e.target.value);
                if (e.target.value.length < 2) { setSelectedCode(null); setShowDropdown(false); }
              }}
              placeholder="Enter a HCPCS/CPT code or description (e.g., 99213, office visit)..."
              style={{
                flex: 1, minWidth: 260, padding: "10px 14px", borderRadius: 8,
                border: `1px solid ${BD}`, fontSize: 14, fontFamily: FM, outline: "none", color: A,
              }}
              onFocus={() => { if (searchResults.length > 0 && !selectedCode) setShowDropdown(true); }}
              onKeyDown={e => {
                if (e.key === "Enter" && searchResults.length > 0) handleSelect(searchResults[0]);
              }}
            />
            {searchLoading && <span style={{ fontSize: 11, color: AL }}>Searching...</span>}
          </div>

          {/* Dropdown */}
          {showDropdown && searchResults.length > 0 && (
            <div style={{
              position: "absolute", left: 0, right: 0, top: "100%", zIndex: 10,
              marginTop: 4, maxHeight: 280, overflowY: "auto",
              border: `1px solid ${BD}`, borderRadius: 8, background: WH, boxShadow: SH,
            }}>
              {searchResults.map(r => (
                <div key={r.procedure_code}
                  onClick={() => handleSelect(r)}
                  style={{
                    padding: "8px 12px", cursor: "pointer", borderBottom: `1px solid ${SF}`,
                    display: "flex", gap: 12, alignItems: "center",
                  }}
                  onMouseEnter={e => (e.currentTarget.style.background = SF)}
                  onMouseLeave={e => (e.currentTarget.style.background = WH)}
                >
                  <span style={{ fontFamily: FM, fontWeight: 700, color: cB, minWidth: 60 }}>{r.procedure_code}</span>
                  <span style={{ flex: 1, fontSize: 12, color: AL, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>
                    {r.description}
                  </span>
                  {r.category && <span style={{ fontSize: 10, color: AL, flexShrink: 0 }}>{r.category}</span>}
                  {r.medicare_rate_nonfac != null && (
                    <span style={{ fontSize: 10, color: AL, flexShrink: 0, fontFamily: FM }}>MCR ${r.medicare_rate_nonfac.toFixed(2)}</span>
                  )}
                </div>
              ))}
            </div>
          )}
          {showDropdown && query.length >= 2 && searchResults.length === 0 && !searchLoading && (
            <div style={{ marginTop: 8, fontSize: 12, color: AL }}>No matching codes found.</div>
          )}
        </div>
      </Card>

      {/* Loading state */}
      {ratesLoading && (
        <Card><p style={{ color: AL, fontSize: 13, textAlign: "center", padding: 40 }}>Loading rate data...</p></Card>
      )}

      {/* Results */}
      {selectedCode && !ratesLoading && rates.length > 0 && stats && (
        <>
          {/* Summary stats */}
          <Card>
            <CH title={selectedCode.procedure_code} sub={selectedCode.description}
              right={selectedCode.medicare_rate_nonfac != null ? (
                <span style={{ fontSize: 13, fontFamily: FM, color: A }}>
                  Medicare: <strong>${selectedCode.medicare_rate_nonfac.toFixed(2)}</strong>
                </span>
              ) : undefined}
            />
            <div style={{ display: "flex", justifyContent: "space-around", flexWrap: "wrap", gap: 16 }}>
              <Met label="Jurisdictions" value={`${stats.states}`} />
              <Met label="Lowest Rate" value={`$${stats.minRate.toFixed(2)}`} color={NEG} />
              <Met label="Highest Rate" value={`$${stats.maxRate.toFixed(2)}`} color={POS} />
              <Met label="Median % MCR" value={`${stats.medianPct.toFixed(0)}%`}
                color={stats.medianPct < 80 ? WARN : POS} />
              {stats.below60 > 0 && (
                <Met label="Below 60% MCR" value={`${stats.below60}`} color={NEG} />
              )}
              {stats.above120 > 0 && (
                <Met label="Above 120% MCR" value={`${stats.above120}`} color="#2563EB" />
              )}
            </div>
          </Card>

          {/* Horizontal bar chart */}
          <Card>
            <CH title="Rates by Jurisdiction" sub="Ranked by % of Medicare" />
            <div style={{ overflowY: "auto", maxHeight: mobile ? 400 : 600 }}>
              <ResponsiveContainer width="100%" height={chartHeight}>
                <BarChart data={chartData} layout="vertical" margin={{ top: 4, right: 40, left: 4, bottom: 4 }}>
                  <XAxis type="number" domain={[0, (dm: number) => Math.max(dm, 150)]}
                    tick={{ fontSize: 10, fontFamily: FM, fill: AL }}
                    tickFormatter={(v: number) => `${v}%`} />
                  <YAxis type="category" dataKey="name" width={32}
                    tick={{ fontSize: 10, fontFamily: FM, fill: A, fontWeight: 600 }} />
                  <Tooltip
                    content={({ active, payload }) => {
                      if (!active || !payload?.[0]) return null;
                      const d = payload[0].payload as RateRow & { name: string; pct: number };
                      return (
                        <div style={{ background: WH, border: `1px solid ${BD}`, borderRadius: 8, padding: "10px 14px", fontSize: 12, fontFamily: FM, boxShadow: SH }}>
                          <div style={{ fontWeight: 700, marginBottom: 4 }}>{d.name} - {STATE_NAMES[d.state_code] || d.state_code}</div>
                          <div>Medicaid: ${d.medicaid_rate.toFixed(2)}</div>
                          <div>Medicare: ${d.medicare_rate.toFixed(2)}</div>
                          <div style={{ fontWeight: 700, color: barColor(d.pct_of_medicare) }}>
                            {d.pct_of_medicare.toFixed(1)}% of Medicare
                          </div>
                          {d.rate_source && <div style={{ fontSize: 10, color: AL, marginTop: 4 }}>Source: {d.rate_source}</div>}
                        </div>
                      );
                    }}
                  />
                  <ReferenceLine x={100} stroke={A} strokeDasharray="4 3" strokeWidth={1.5} opacity={0.5} />
                  <Bar dataKey="pct" radius={[0, 3, 3, 0]} barSize={16}>
                    {chartData.map((d, i) => (
                      <Cell key={i} fill={barColor(d.pct_of_medicare)} />
                    ))}
                  </Bar>
                </BarChart>
              </ResponsiveContainer>
            </div>
            <div style={{ display: "flex", gap: 14, marginTop: 10, fontSize: 10, color: AL, flexWrap: "wrap" }}>
              <span><span style={{ display: "inline-block", width: 8, height: 8, borderRadius: 2, background: NEG, marginRight: 3 }} />Below 60%</span>
              <span><span style={{ display: "inline-block", width: 8, height: 8, borderRadius: 2, background: "#D97706", marginRight: 3 }} />60-80%</span>
              <span><span style={{ display: "inline-block", width: 8, height: 8, borderRadius: 2, background: POS, marginRight: 3 }} />80-120%</span>
              <span><span style={{ display: "inline-block", width: 8, height: 8, borderRadius: 2, background: "#2563EB", marginRight: 3 }} />Above 120%</span>
              <span style={{ marginLeft: "auto" }}>Dashed line = Medicare parity</span>
            </div>
          </Card>

          {/* Sortable table */}
          <Card>
            <CH title={`${rates.length} Jurisdictions`}
              right={
                <button onClick={() => {
                  downloadCSV(
                    ["State", "State Name", "Medicaid Rate", "Medicare Rate", "% of Medicare", "Rate Source"],
                    sortedRates.map(r => [
                      r.state_code,
                      STATE_NAMES[r.state_code] || r.state_code,
                      r.medicaid_rate.toFixed(2),
                      r.medicare_rate.toFixed(2),
                      r.pct_of_medicare.toFixed(1),
                      r.rate_source || "",
                    ]),
                    `rate_explorer_${selectedCode.procedure_code}.csv`,
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
                    <th onClick={() => handleSort("medicaid")} style={{ padding: "8px 6px", textAlign: "right", color: AL, fontWeight: 600, fontSize: 11, cursor: "pointer", userSelect: "none" }}>
                      Medicaid Rate {sortKey === "medicaid" ? (sortAsc ? "\u25B4" : "\u25BE") : ""}
                    </th>
                    <th style={{ padding: "8px 6px", textAlign: "right", color: AL, fontWeight: 600, fontSize: 11 }}>Medicare Rate</th>
                    <th onClick={() => handleSort("pct")} style={{ padding: "8px 6px", textAlign: "right", color: AL, fontWeight: 600, fontSize: 11, cursor: "pointer", userSelect: "none" }}>
                      % of MCR {sortKey === "pct" ? (sortAsc ? "\u25B4" : "\u25BE") : ""}
                    </th>
                    <th style={{ padding: "8px 6px", textAlign: "right", color: AL, fontWeight: 600, fontSize: 11 }}>Gap</th>
                    <th style={{ padding: "8px 6px", textAlign: "left", color: AL, fontWeight: 600, fontSize: 11 }}>Source</th>
                  </tr>
                </thead>
                <tbody>
                  {sortedRates.map(r => {
                    const gap = r.medicaid_rate - r.medicare_rate;
                    return (
                      <tr key={r.state_code} style={{ borderBottom: `1px solid ${BD}` }}>
                        <td style={{ padding: "8px 6px", fontWeight: 600, color: A }}>
                          {r.state_code} <span style={{ fontWeight: 400, color: AL }}>{STATE_NAMES[r.state_code] || ""}</span>
                        </td>
                        <td style={{ padding: "8px 6px", textAlign: "right", fontWeight: 600 }}>${r.medicaid_rate.toFixed(2)}</td>
                        <td style={{ padding: "8px 6px", textAlign: "right", color: AL }}>${r.medicare_rate.toFixed(2)}</td>
                        <td style={{ padding: "8px 6px", textAlign: "right", fontWeight: 700,
                          color: barColor(r.pct_of_medicare),
                        }}>{r.pct_of_medicare.toFixed(1)}%</td>
                        <td style={{ padding: "8px 6px", textAlign: "right",
                          color: gap < 0 ? NEG : POS,
                        }}>{gap >= 0 ? "+" : ""}${gap.toFixed(2)}</td>
                        <td style={{ padding: "8px 6px", textAlign: "left", fontSize: 10, color: AL }}>{r.rate_source || "--"}</td>
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            </div>
          </Card>

          {/* Data source note */}
          <Card>
            <div style={{ fontSize: 12, color: AL, lineHeight: 1.7 }}>
              <p style={{ margin: "0 0 8px" }}>
                <strong>Medicaid rates</strong> from fact_rate_comparison_v2 (483,154 rows, 54 jurisdictions).
                Rate sources include state-published fee schedules (88%), conversion factor x RVU (11%),
                and T-MSIS paid amounts (1.1%).
              </p>
              <p style={{ margin: 0 }}>
                <strong>Medicare rate</strong> is the CY2026 Medicare Physician Fee Schedule non-facility rate.
                % of Medicare = (Medicaid rate / Medicare rate) x 100.
              </p>
            </div>
          </Card>
        </>
      )}

      {/* No results for selected code */}
      {selectedCode && !ratesLoading && rates.length === 0 && (
        <Card>
          <p style={{ color: AL, fontSize: 13, textAlign: "center", padding: 40 }}>
            No rate data found for {selectedCode.procedure_code} in fact_rate_comparison_v2.
          </p>
        </Card>
      )}

      {/* Common codes when nothing selected */}
      {!selectedCode && !ratesLoading && (
        <Card>
          <CH title="Common Codes" sub="Select a code to see rates across all jurisdictions" />
          <div style={{ display: "flex", flexWrap: "wrap", gap: 6 }}>
            {["99213", "99214", "99215", "99203", "99204", "90834", "90837", "97110", "92507", "99385"].map(code => (
              <button key={code}
                onClick={() => {
                  setQuery(code);
                  // Trigger search & auto-select
                  (async () => {
                    try {
                      const res = await fetch(`${API_BASE}/api/rate-explorer/search?q=${encodeURIComponent(code)}`);
                      if (res.ok) {
                        const data = await res.json();
                        const match = (data.results || []).find((r: SearchResult) => r.procedure_code === code);
                        if (match) handleSelect(match);
                      }
                    } catch { /* ignore */ }
                  })();
                }}
                style={{
                  padding: "6px 14px", borderRadius: 8, border: `1px solid ${BD}`,
                  background: WH, cursor: "pointer", fontFamily: FM, fontSize: 11,
                  display: "flex", flexDirection: "column", alignItems: "center", gap: 2,
                  minWidth: 80,
                }}
                onMouseEnter={e => (e.currentTarget.style.borderColor = cB)}
                onMouseLeave={e => (e.currentTarget.style.borderColor = BD)}
              >
                <span style={{ fontWeight: 700, color: cB }}>{code}</span>
              </button>
            ))}
          </div>
        </Card>
      )}

      <div style={{ fontSize: 10, color: AL, marginTop: 8 }}>
        Aradune Rate Explorer v1.0 -- fact_rate_comparison_v2 (483K rows, 54 jurisdictions) + CY2026 Medicare PFS
      </div>
    </div>
  );
}
