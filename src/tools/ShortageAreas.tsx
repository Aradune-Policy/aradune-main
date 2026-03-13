/**
 * ShortageAreas — Tab in Workforce & Quality module.
 * HPSA designations + MUA/MUP data by state.
 * Uses /api/hpsa and /api/mua endpoints.
 */

import { useState, useEffect } from "react";
import { C, FONT, SHADOW } from "../design";
import { API_BASE } from "../lib/api";

interface HpsaRow {
  hpsa_name?: string;
  hpsa_id?: string;
  discipline?: string;
  designation_type?: string;
  hpsa_score?: number;
  hpsa_status?: string;
  metro_indicator?: string;
  degree_of_shortage?: number;
  fte?: number;
  population?: number;
  poverty_pct?: number;
  county?: string;
  [k: string]: unknown;
}

interface HpsaSummary {
  state_code: string;
  total_hpsas?: number;
  avg_hpsa_score?: number;
  total_population?: number;
  [k: string]: unknown;
}

const STATES = [
  "AL","AK","AZ","AR","CA","CO","CT","DE","FL","GA","HI","ID","IL","IN","IA",
  "KS","KY","LA","ME","MD","MA","MI","MN","MS","MO","MT","NE","NV","NH","NJ",
  "NM","NY","NC","ND","OH","OK","OR","PA","RI","SC","SD","TN","TX","UT","VT",
  "VA","WA","WV","WI","WY","DC",
];

const DISCIPLINES = ["", "Primary Care", "Mental Health", "Dental"];

function fmt(v: unknown, digits = 1): string {
  if (v === null || v === undefined) return "-";
  const n = Number(v);
  return isNaN(n) ? String(v) : n.toLocaleString("en-US", { maximumFractionDigits: digits });
}

function scoreColor(score: number | undefined): string {
  if (!score) return C.ink;
  if (score >= 18) return C.neg;
  if (score >= 12) return C.warn;
  return C.ink;
}

type ViewMode = "summary" | "hpsa" | "mua";

export default function ShortageAreas() {
  const [state, setState] = useState("FL");
  const [view, setView] = useState<ViewMode>("summary");
  const [discipline, setDiscipline] = useState("");
  const [summary, setSummary] = useState<HpsaSummary[]>([]);
  const [hpsaRows, setHpsaRows] = useState<HpsaRow[]>([]);
  const [muaRows, setMuaRows] = useState<Record<string, unknown>[]>([]);
  const [loading, setLoading] = useState(false);

  // National summary
  useEffect(() => {
    fetch(`${API_BASE}/api/hpsa/summary`)
      .then((r) => r.json())
      .then((d) => setSummary(d.rows || []))
      .catch(() => {});
  }, []);

  // HPSA detail
  useEffect(() => {
    if (view !== "hpsa") return;
    setLoading(true);
    fetch(`${API_BASE}/api/hpsa/${state}`)
      .then((r) => r.json())
      .then((d) => setHpsaRows(d.rows || []))
      .catch(() => setHpsaRows([]))
      .finally(() => setLoading(false));
  }, [state, view]);

  // MUA detail
  useEffect(() => {
    if (view !== "mua") return;
    setLoading(true);
    fetch(`${API_BASE}/api/mua/${state}`)
      .then((r) => r.json())
      .then((d) => setMuaRows(d.rows || []))
      .catch(() => setMuaRows([]))
      .finally(() => setLoading(false));
  }, [state, view]);

  const filteredHpsa = discipline
    ? hpsaRows.filter((r) => String(r.discipline || "").toLowerCase().includes(discipline.toLowerCase()))
    : hpsaRows;

  const pillStyle = (active: boolean): React.CSSProperties => ({
    padding: "4px 10px", fontSize: 11, fontFamily: FONT.body, fontWeight: 500,
    borderRadius: 4, cursor: "pointer",
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
            Shortage Areas
          </h3>
          <p style={{ margin: "2px 0 0", fontSize: 11, color: C.inkLight }}>
            HPSA designations and Medically Underserved Areas/Populations
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

      {/* View pills */}
      <div style={{ display: "flex", gap: 4, marginBottom: 12 }}>
        <button onClick={() => setView("summary")} style={pillStyle(view === "summary")}>
          National Summary
        </button>
        <button onClick={() => setView("hpsa")} style={pillStyle(view === "hpsa")}>
          HPSA Detail
        </button>
        <button onClick={() => setView("mua")} style={pillStyle(view === "mua")}>
          MUA/MUP
        </button>
      </div>

      {/* ── National Summary ──────────────────────────────── */}
      {view === "summary" && (
        <div style={{ overflowX: "auto" }}>
          <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 11 }}>
            <thead>
              <tr>
                <th style={thStyle}>State</th>
                <th style={thStyle}>Total HPSAs</th>
                <th style={thStyle}>Avg HPSA Score</th>
                <th style={thStyle}>Population Designated</th>
              </tr>
            </thead>
            <tbody>
              {summary.map((row, i) => (
                <tr
                  key={row.state_code}
                  onClick={() => { setState(row.state_code); setView("hpsa"); }}
                  style={{ background: i % 2 ? C.surface : C.white, cursor: "pointer" }}
                  onMouseEnter={(e) => (e.currentTarget.style.background = "#e8f0e8")}
                  onMouseLeave={(e) => (e.currentTarget.style.background = i % 2 ? C.surface : C.white)}
                >
                  <td style={{ padding: "6px 8px", fontWeight: 600 }}>{row.state_code}</td>
                  <td style={{ padding: "6px 8px" }}>{fmt(row.total_hpsas, 0)}</td>
                  <td style={{ padding: "6px 8px", color: scoreColor(row.avg_hpsa_score as number) }}>
                    {fmt(row.avg_hpsa_score)}
                  </td>
                  <td style={{ padding: "6px 8px" }}>{fmt(row.total_population, 0)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}

      {/* ── HPSA Detail ──────────────────────────────────── */}
      {view === "hpsa" && (
        <>
          <div style={{ display: "flex", gap: 8, marginBottom: 8, alignItems: "center" }}>
            <select
              value={discipline}
              onChange={(e) => setDiscipline(e.target.value)}
              style={{
                padding: "4px 8px", fontSize: 11, borderRadius: 4, border: `1px solid ${C.border}`,
                fontFamily: FONT.body,
              }}
            >
              <option value="">All Disciplines</option>
              {DISCIPLINES.filter(Boolean).map((d) => <option key={d} value={d}>{d}</option>)}
            </select>
            <span style={{ fontSize: 10, color: C.inkLight, fontFamily: FONT.mono }}>
              {loading ? "Loading..." : `${filteredHpsa.length} designations`}
            </span>
          </div>
          <div style={{ overflowX: "auto", border: `1px solid ${C.border}`, borderRadius: 6, boxShadow: SHADOW }}>
            <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 11 }}>
              <thead>
                <tr>
                  <th style={thStyle}>HPSA Name</th>
                  <th style={thStyle}>Discipline</th>
                  <th style={thStyle}>Type</th>
                  <th style={thStyle}>Score</th>
                  <th style={thStyle}>Shortage</th>
                  <th style={thStyle}>Population</th>
                  <th style={thStyle}>Poverty %</th>
                  <th style={thStyle}>County</th>
                </tr>
              </thead>
              <tbody>
                {filteredHpsa.slice(0, 300).map((row, i) => (
                  <tr key={row.hpsa_id || i} style={{ background: i % 2 ? C.surface : C.white }}>
                    <td style={{ padding: "5px 8px", maxWidth: 200, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>
                      {row.hpsa_name || "-"}
                    </td>
                    <td style={{ padding: "5px 8px" }}>{row.discipline || "-"}</td>
                    <td style={{ padding: "5px 8px" }}>{row.designation_type || "-"}</td>
                    <td style={{ padding: "5px 8px", fontWeight: 600, color: scoreColor(row.hpsa_score) }}>
                      {row.hpsa_score ?? "-"}
                    </td>
                    <td style={{ padding: "5px 8px" }}>{row.degree_of_shortage ?? "-"}</td>
                    <td style={{ padding: "5px 8px", textAlign: "right" }}>{fmt(row.population, 0)}</td>
                    <td style={{ padding: "5px 8px", textAlign: "right" }}>{fmt(row.poverty_pct)}%</td>
                    <td style={{ padding: "5px 8px" }}>{row.county || "-"}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
          {filteredHpsa.length > 300 && (
            <div style={{ fontSize: 10, color: C.inkLight, marginTop: 4 }}>
              Showing first 300 of {filteredHpsa.length}. Filter by discipline to narrow.
            </div>
          )}
        </>
      )}

      {/* ── MUA/MUP ──────────────────────────────────────── */}
      {view === "mua" && (
        <>
          <div style={{ marginBottom: 8, fontSize: 11, color: C.inkLight }}>
            {loading ? "Loading..." : `${muaRows.length} MUA/MUP designations in ${state}`}
          </div>
          <div style={{ overflowX: "auto", border: `1px solid ${C.border}`, borderRadius: 6, boxShadow: SHADOW }}>
            <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 11 }}>
              <thead>
                <tr>
                  {Object.keys(muaRows[0] || {}).slice(0, 8).map((k) => (
                    <th key={k} style={thStyle}>{k.replace(/_/g, " ")}</th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {muaRows.slice(0, 300).map((row, i) => (
                  <tr key={i} style={{ background: i % 2 ? C.surface : C.white }}>
                    {Object.keys(muaRows[0] || {}).slice(0, 8).map((k) => (
                      <td key={k} style={{ padding: "5px 8px" }}>
                        {row[k] != null ? String(row[k]) : "-"}
                      </td>
                    ))}
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
          {muaRows.length === 0 && !loading && (
            <div style={{ padding: 20, textAlign: "center", color: C.inkLight, fontSize: 12 }}>
              No MUA data for {state}. Endpoint may not be configured yet.
            </div>
          )}
        </>
      )}
    </div>
  );
}
