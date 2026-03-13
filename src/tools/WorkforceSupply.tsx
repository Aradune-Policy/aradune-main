/**
 * WorkforceSupply — Tab in Workforce & Quality module.
 * BLS wages, HRSA projections, NHSC field strength, nursing workforce.
 * Uses /api/wages, /api/workforce/* endpoints.
 */

import { useState, useEffect } from "react";
import { C, FONT, SHADOW } from "../design";
import { API_BASE } from "../lib/api";

interface WageRow {
  occ_title?: string;
  soc_code?: string;
  h_mean?: number;
  a_mean?: number;
  h_median?: number;
  tot_emp?: number;
  [k: string]: unknown;
}

interface NhscRow {
  state_code?: string;
  discipline?: string;
  clinician_count?: number;
  [k: string]: unknown;
}

const STATES = [
  "AL","AK","AZ","AR","CA","CO","CT","DE","FL","GA","HI","ID","IL","IN","IA",
  "KS","KY","LA","ME","MD","MA","MI","MN","MS","MO","MT","NE","NV","NH","NJ",
  "NM","NY","NC","ND","OH","OK","OR","PA","RI","SC","SD","TN","TX","UT","VT",
  "VA","WA","WV","WI","WY","DC",
];

function fmtDollar(v: unknown): string {
  if (v === null || v === undefined) return "-";
  const n = Number(v);
  return isNaN(n) ? String(v) : `$${n.toLocaleString("en-US", { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`;
}

function fmtNum(v: unknown): string {
  if (v === null || v === undefined) return "-";
  const n = Number(v);
  return isNaN(n) ? String(v) : n.toLocaleString("en-US");
}

type SubView = "wages" | "nhsc" | "projections";

export default function WorkforceSupply() {
  const [state, setState] = useState("FL");
  const [subView, setSubView] = useState<SubView>("wages");
  const [wages, setWages] = useState<WageRow[]>([]);
  const [nhsc, setNhsc] = useState<NhscRow[]>([]);
  const [projections, setProjections] = useState<Record<string, unknown>[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  // Load wages for state
  useEffect(() => {
    if (subView !== "wages") return;
    setLoading(true);
    setError(null);
    fetch(`${API_BASE}/api/wages/${state}`)
      .then((r) => r.json())
      .then((d) => { setWages(d.rows || []); setError(null); })
      .catch(() => { setWages([]); setError("Unable to load wage data"); })
      .finally(() => setLoading(false));
  }, [state, subView]);

  // Load NHSC data
  useEffect(() => {
    if (subView !== "nhsc") return;
    setLoading(true);
    setError(null);
    fetch(`${API_BASE}/api/workforce/nhsc?state_code=${state}`)
      .then((r) => r.json())
      .then((d) => { setNhsc(d.rows || []); setError(null); })
      .catch(() => { setNhsc([]); setError("Unable to load NHSC data"); })
      .finally(() => setLoading(false));
  }, [state, subView]);

  // Load projections
  useEffect(() => {
    if (subView !== "projections") return;
    setLoading(true);
    setError(null);
    fetch(`${API_BASE}/api/workforce/projections?state_code=${state}`)
      .then((r) => r.json())
      .then((d) => { setProjections(d.rows || []); setError(null); })
      .catch(() => { setProjections([]); setError("Unable to load projection data"); })
      .finally(() => setLoading(false));
  }, [state, subView]);

  const pillStyle = (active: boolean): React.CSSProperties => ({
    padding: "4px 10px", fontSize: 11, fontFamily: FONT.body, fontWeight: 500,
    borderRadius: 4, cursor: "pointer", transition: "all 0.1s",
    background: active ? C.brand : C.white,
    color: active ? C.white : C.inkLight,
    border: `1px solid ${active ? C.brand : C.border}`,
  });

  const thStyle: React.CSSProperties = {
    padding: "6px 8px", fontSize: 10, fontWeight: 600, color: C.white,
    background: C.brand, textAlign: "left", whiteSpace: "nowrap",
  };

  return (
    <div style={{ padding: "16px 20px", fontFamily: FONT.body }}>
      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 12 }}>
        <div>
          <h3 style={{ margin: 0, fontSize: 15, color: C.ink, fontWeight: 700 }}>
            Workforce Supply
          </h3>
          <p style={{ margin: "2px 0 0", fontSize: 11, color: C.inkLight }}>
            BLS wages, NHSC clinician coverage, HRSA supply/demand projections
          </p>
        </div>
        <select
          value={state}
          onChange={(e) => setState(e.target.value)}
          style={{
            padding: "5px 8px", fontSize: 11, borderRadius: 4, border: `1px solid ${C.border}`,
            fontFamily: FONT.body,
          }}
        >
          {STATES.map((s) => <option key={s} value={s}>{s}</option>)}
        </select>
      </div>

      {/* Sub-view pills */}
      <div style={{ display: "flex", gap: 4, marginBottom: 12 }}>
        <button onClick={() => setSubView("wages")} style={pillStyle(subView === "wages")}>
          BLS Wages
        </button>
        <button onClick={() => setSubView("nhsc")} style={pillStyle(subView === "nhsc")}>
          NHSC Clinicians
        </button>
        <button onClick={() => setSubView("projections")} style={pillStyle(subView === "projections")}>
          Projections
        </button>
      </div>

      {loading && (
        <div style={{ padding: 16, fontSize: 12, color: C.inkLight }}>Loading...</div>
      )}
      {!loading && error && (
        <div style={{ padding: 16, fontSize: 12, color: C.neg }}>{error}</div>
      )}

      {/* ── BLS Wages ──────────────────────────────────────── */}
      {subView === "wages" && !loading && (
        <div style={{ overflowX: "auto", border: `1px solid ${C.border}`, borderRadius: 6, boxShadow: SHADOW }}>
          <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 11 }}>
            <thead>
              <tr>
                <th style={thStyle}>Occupation</th>
                <th style={thStyle}>SOC</th>
                <th style={{ ...thStyle, textAlign: "right" }}>Hourly Mean</th>
                <th style={{ ...thStyle, textAlign: "right" }}>Annual Mean</th>
                <th style={{ ...thStyle, textAlign: "right" }}>Hourly Median</th>
                <th style={{ ...thStyle, textAlign: "right" }}>Employment</th>
              </tr>
            </thead>
            <tbody>
              {wages.map((row, i) => (
                <tr key={row.soc_code || i} style={{ background: i % 2 ? C.surface : C.white }}>
                  <td style={{ padding: "5px 8px", maxWidth: 220, overflow: "hidden", textOverflow: "ellipsis" }}>
                    {row.occ_title || "-"}
                  </td>
                  <td style={{ padding: "5px 8px", fontFamily: FONT.mono, fontSize: 10 }}>
                    {row.soc_code || "-"}
                  </td>
                  <td style={{ padding: "5px 8px", textAlign: "right" }}>{fmtDollar(row.h_mean)}</td>
                  <td style={{ padding: "5px 8px", textAlign: "right" }}>{fmtDollar(row.a_mean)}</td>
                  <td style={{ padding: "5px 8px", textAlign: "right" }}>{fmtDollar(row.h_median)}</td>
                  <td style={{ padding: "5px 8px", textAlign: "right" }}>{fmtNum(row.tot_emp)}</td>
                </tr>
              ))}
            </tbody>
          </table>
          {wages.length === 0 && (
            <div style={{ padding: 20, textAlign: "center", color: C.inkLight, fontSize: 12 }}>
              No wage data available for {state}
            </div>
          )}
        </div>
      )}

      {/* ── NHSC Clinicians ────────────────────────────────── */}
      {subView === "nhsc" && !loading && (
        <div style={{ overflowX: "auto", border: `1px solid ${C.border}`, borderRadius: 6, boxShadow: SHADOW }}>
          <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 11 }}>
            <thead>
              <tr>
                {Object.keys(nhsc[0] || {}).slice(0, 8).map((k) => (
                  <th key={k} style={thStyle}>{k.replace(/_/g, " ")}</th>
                ))}
              </tr>
            </thead>
            <tbody>
              {nhsc.map((row, i) => (
                <tr key={i} style={{ background: i % 2 ? C.surface : C.white }}>
                  {Object.keys(nhsc[0] || {}).slice(0, 8).map((k) => (
                    <td key={k} style={{ padding: "5px 8px" }}>
                      {row[k] != null ? String(row[k]) : "-"}
                    </td>
                  ))}
                </tr>
              ))}
            </tbody>
          </table>
          {nhsc.length === 0 && (
            <div style={{ padding: 20, textAlign: "center", color: C.inkLight, fontSize: 12 }}>
              No NHSC data available for {state}. Endpoint may not be configured yet.
            </div>
          )}
        </div>
      )}

      {/* ── Projections ────────────────────────────────────── */}
      {subView === "projections" && !loading && (
        <div style={{ overflowX: "auto", border: `1px solid ${C.border}`, borderRadius: 6, boxShadow: SHADOW }}>
          <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 11 }}>
            <thead>
              <tr>
                {Object.keys(projections[0] || {}).slice(0, 8).map((k) => (
                  <th key={k} style={thStyle}>{k.replace(/_/g, " ")}</th>
                ))}
              </tr>
            </thead>
            <tbody>
              {projections.slice(0, 200).map((row, i) => (
                <tr key={i} style={{ background: i % 2 ? C.surface : C.white }}>
                  {Object.keys(projections[0] || {}).slice(0, 8).map((k) => (
                    <td key={k} style={{ padding: "5px 8px" }}>
                      {row[k] != null ? String(row[k]) : "-"}
                    </td>
                  ))}
                </tr>
              ))}
            </tbody>
          </table>
          {projections.length === 0 && (
            <div style={{ padding: 20, textAlign: "center", color: C.inkLight, fontSize: 12 }}>
              No projection data available for {state}. Endpoint may not be configured yet.
            </div>
          )}
        </div>
      )}
    </div>
  );
}
