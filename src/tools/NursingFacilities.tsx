/**
 * NursingFacilities — Tab in Provider Intelligence module.
 * Five-Star ratings, PBJ staffing, SNF cost reports.
 * Uses existing /api/five-star, /api/staffing, /api/nursing-facilities endpoints.
 */

import { useState, useEffect, useCallback } from "react";
import { C, FONT, SHADOW } from "../design";
import { API_BASE } from "../lib/api";

interface FiveStarRow {
  provider_ccn?: string;
  facility_name?: string;
  overall_rating?: number;
  health_inspection_rating?: number;
  staffing_rating?: number;
  quality_rating?: number;
  deficiency_count?: number;
  fine_count?: number;
  abuse_icon?: string;
  special_focus?: string;
  [k: string]: unknown;
}

interface StateSummary {
  state_code: string;
  facility_count?: number;
  avg_overall_rating?: number;
  avg_staffing_rating?: number;
  avg_hprd?: number;
  [k: string]: unknown;
}

const STATES = [
  "AL","AK","AZ","AR","CA","CO","CT","DE","FL","GA","HI","ID","IL","IN","IA",
  "KS","KY","LA","ME","MD","MA","MI","MN","MS","MO","MT","NE","NV","NH","NJ",
  "NM","NY","NC","ND","OH","OK","OR","PA","RI","SC","SD","TN","TX","UT","VT",
  "VA","WA","WV","WI","WY","DC",
];

function stars(n: number | undefined): string {
  if (!n) return "-";
  return "\u2605".repeat(Math.round(n)) + "\u2606".repeat(5 - Math.round(n));
}

function fmt(v: unknown, digits = 1): string {
  if (v === null || v === undefined) return "-";
  const n = Number(v);
  return isNaN(n) ? String(v) : n.toFixed(digits);
}

export default function NursingFacilities() {
  const [state, setState] = useState("FL");
  const [view, setView] = useState<"summary" | "facilities">("summary");
  const [summary, setSummary] = useState<StateSummary[]>([]);
  const [facilities, setFacilities] = useState<FiveStarRow[]>([]);
  const [loading, setLoading] = useState(false);
  const [sortCol, setSortCol] = useState<string>("overall_rating");
  const [sortAsc, setSortAsc] = useState(false);
  const [error, setError] = useState<string | null>(null);

  // Load national summary
  useEffect(() => {
    fetch(`${API_BASE}/api/five-star/summary`)
      .then((r) => r.json())
      .then((d) => { setSummary(d.rows || []); setError(null); })
      .catch(() => { setError("Unable to load summary data"); });
  }, []);

  // Load state facilities
  const loadFacilities = useCallback(async (st: string) => {
    setLoading(true);
    try {
      const res = await fetch(`${API_BASE}/api/five-star/${st}`);
      const data = await res.json();
      setFacilities(data.rows || []);
      setView("facilities");
    } catch {
      setFacilities([]);
      setError("Unable to load facility data");
    }
    setLoading(false);
  }, []);

  const sorted = [...facilities].sort((a, b) => {
    const av = Number(a[sortCol]) || 0;
    const bv = Number(b[sortCol]) || 0;
    return sortAsc ? av - bv : bv - av;
  });

  const handleSort = (col: string) => {
    if (col === sortCol) setSortAsc(!sortAsc);
    else { setSortCol(col); setSortAsc(false); }
  };

  const thStyle = (col: string): React.CSSProperties => ({
    padding: "6px 8px", fontSize: 10, fontWeight: 600, color: C.white,
    background: C.brand, cursor: "pointer", whiteSpace: "nowrap",
    textAlign: "left",
    borderBottom: sortCol === col ? "2px solid #C4590A" : undefined,
  });

  return (
    <div style={{ padding: "16px 20px", fontFamily: FONT.body }}>
      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 12 }}>
        <div>
          <h3 style={{ margin: 0, fontSize: 15, color: C.ink, fontWeight: 700 }}>
            Nursing Facility Quality
          </h3>
          <p style={{ margin: "2px 0 0", fontSize: 11, color: C.inkLight }}>
            CMS Five-Star ratings, PBJ staffing, deficiency tracking
          </p>
        </div>
        <div style={{ display: "flex", gap: 8, alignItems: "center" }}>
          {view === "facilities" && (
            <button
              onClick={() => setView("summary")}
              style={{
                background: "none", border: `1px solid ${C.border}`, borderRadius: 4,
                padding: "4px 10px", fontSize: 11, color: C.inkLight, cursor: "pointer",
              }}
            >
              Back to Summary
            </button>
          )}
          <select
            value={state}
            onChange={(e) => { setState(e.target.value); loadFacilities(e.target.value); }}
            style={{
              padding: "5px 8px", fontSize: 11, borderRadius: 4, border: `1px solid ${C.border}`,
              fontFamily: FONT.body,
            }}
          >
            {STATES.map((s) => <option key={s} value={s}>{s}</option>)}
          </select>
        </div>
      </div>

      {/* ── National Summary ──────────────────────────────────── */}
      {view === "summary" && (
        <div style={{ overflowX: "auto" }}>
          <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 11 }}>
            <thead>
              <tr style={{ background: C.brand }}>
                <th style={thStyle("state_code")}>State</th>
                <th style={thStyle("facility_count")}>Facilities</th>
                <th style={thStyle("avg_overall_rating")}>Avg Rating</th>
                <th style={thStyle("avg_staffing_rating")}>Avg Staff Rating</th>
                <th style={thStyle("avg_hprd")}>Avg HPRD</th>
              </tr>
            </thead>
            <tbody>
              {summary.map((row, i) => (
                <tr
                  key={row.state_code}
                  onClick={() => { setState(row.state_code); loadFacilities(row.state_code); }}
                  style={{
                    background: i % 2 ? C.surface : C.white, cursor: "pointer",
                    transition: "background 0.1s",
                  }}
                  onMouseEnter={(e) => (e.currentTarget.style.background = "#e8f0e8")}
                  onMouseLeave={(e) => (e.currentTarget.style.background = i % 2 ? C.surface : C.white)}
                >
                  <td style={{ padding: "6px 8px", fontWeight: 600 }}>{row.state_code}</td>
                  <td style={{ padding: "6px 8px" }}>{row.facility_count ?? "-"}</td>
                  <td style={{ padding: "6px 8px" }}>
                    {stars(row.avg_overall_rating)} ({fmt(row.avg_overall_rating)})
                  </td>
                  <td style={{ padding: "6px 8px" }}>{fmt(row.avg_staffing_rating)}</td>
                  <td style={{ padding: "6px 8px" }}>{fmt(row.avg_hprd)}</td>
                </tr>
              ))}
            </tbody>
          </table>
          {summary.length === 0 && !loading && (
            <div style={{ padding: 24, textAlign: "center", color: error ? C.neg : C.inkLight, fontSize: 12 }}>
              {error || "Loading summary data..."}
            </div>
          )}
        </div>
      )}

      {/* ── State Facilities Detail ──────────────────────────── */}
      {view === "facilities" && (
        <>
          <div style={{ marginBottom: 8, fontSize: 12, color: C.inkLight }}>
            {loading ? "Loading..." : `${sorted.length} facilities in ${state}`}
          </div>
          <div style={{ overflowX: "auto", border: `1px solid ${C.border}`, borderRadius: 6, boxShadow: SHADOW }}>
            <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 11 }}>
              <thead>
                <tr>
                  <th style={thStyle("facility_name")} onClick={() => handleSort("facility_name")}>Facility</th>
                  <th style={thStyle("overall_rating")} onClick={() => handleSort("overall_rating")}>Overall</th>
                  <th style={thStyle("health_inspection_rating")} onClick={() => handleSort("health_inspection_rating")}>Health Insp</th>
                  <th style={thStyle("staffing_rating")} onClick={() => handleSort("staffing_rating")}>Staffing</th>
                  <th style={thStyle("quality_rating")} onClick={() => handleSort("quality_rating")}>Quality</th>
                  <th style={thStyle("deficiency_count")} onClick={() => handleSort("deficiency_count")}>Deficiencies</th>
                  <th style={thStyle("fine_count")} onClick={() => handleSort("fine_count")}>Fines</th>
                </tr>
              </thead>
              <tbody>
                {sorted.slice(0, 200).map((row, i) => (
                  <tr key={row.provider_ccn || i} style={{ background: i % 2 ? C.surface : C.white }}>
                    <td style={{ padding: "5px 8px", maxWidth: 240, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>
                      {row.facility_name || "-"}
                    </td>
                    <td style={{ padding: "5px 8px", textAlign: "center" }}>{stars(row.overall_rating)}</td>
                    <td style={{ padding: "5px 8px", textAlign: "center" }}>{row.health_inspection_rating ?? "-"}</td>
                    <td style={{ padding: "5px 8px", textAlign: "center" }}>{row.staffing_rating ?? "-"}</td>
                    <td style={{ padding: "5px 8px", textAlign: "center" }}>{row.quality_rating ?? "-"}</td>
                    <td style={{ padding: "5px 8px", textAlign: "center", color: (row.deficiency_count ?? 0) > 10 ? C.neg : C.ink }}>
                      {row.deficiency_count ?? "-"}
                    </td>
                    <td style={{ padding: "5px 8px", textAlign: "center", color: (row.fine_count ?? 0) > 0 ? C.neg : C.ink }}>
                      {row.fine_count ?? "-"}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
          {sorted.length > 200 && (
            <div style={{ fontSize: 10, color: C.inkLight, marginTop: 4 }}>
              Showing first 200 of {sorted.length} facilities. Use Intelligence for deeper analysis.
            </div>
          )}
        </>
      )}
    </div>
  );
}
