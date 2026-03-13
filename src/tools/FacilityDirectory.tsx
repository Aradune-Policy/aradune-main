/**
 * FacilityDirectory — Tab in Provider Intelligence module.
 * Searchable directory: FQHCs, dialysis, hospice, home health, IRF, LTCH.
 * Uses existing /api/providers/fqhc, /api/dialysis, /api/hospice, /api/home-health, /api/irf, /api/ltch endpoints.
 */

import { useState, useEffect, useCallback } from "react";
import { C, FONT, SHADOW } from "../design";
import { API_BASE } from "../lib/api";

type FacilityType = "fqhc" | "dialysis" | "hospice" | "home_health" | "irf" | "ltch";

interface FacilityConfig {
  key: FacilityType;
  label: string;
  endpoint: string;
  summaryEndpoint: string;
  nameField: string;
  columns: { key: string; label: string; width?: number }[];
}

const FACILITY_TYPES: FacilityConfig[] = [
  {
    key: "fqhc",
    label: "FQHCs",
    endpoint: "/api/providers/fqhc",
    summaryEndpoint: "/api/providers/fqhc/summary",
    nameField: "site_name",
    columns: [
      { key: "site_name", label: "Site Name", width: 200 },
      { key: "health_center_name", label: "Health Center" },
      { key: "center_type", label: "Type" },
      { key: "state_code", label: "State", width: 50 },
      { key: "city", label: "City" },
    ],
  },
  {
    key: "dialysis",
    label: "Dialysis",
    endpoint: "/api/dialysis/facilities",
    summaryEndpoint: "/api/dialysis/summary",
    nameField: "facility_name",
    columns: [
      { key: "facility_name", label: "Facility", width: 200 },
      { key: "state_code", label: "State", width: 50 },
      { key: "five_star_rating", label: "Rating" },
      { key: "total_stations", label: "Stations" },
      { key: "ownership_type", label: "Ownership" },
    ],
  },
  {
    key: "hospice",
    label: "Hospice",
    endpoint: "/api/hospice/directory",
    summaryEndpoint: "/api/hospice/summary",
    nameField: "facility_name",
    columns: [
      { key: "facility_name", label: "Facility", width: 200 },
      { key: "state_code", label: "State", width: 50 },
      { key: "city", label: "City" },
      { key: "ownership_type", label: "Ownership" },
      { key: "certification_date", label: "Certified" },
    ],
  },
  {
    key: "home_health",
    label: "Home Health",
    endpoint: "/api/home-health/agencies",
    summaryEndpoint: "/api/home-health/agencies/summary",
    nameField: "provider_name",
    columns: [
      { key: "provider_name", label: "Agency", width: 200 },
      { key: "state_code", label: "State", width: 50 },
      { key: "quality_of_patient_care_star_rating", label: "Quality" },
      { key: "offers_nursing_care_services", label: "Nursing" },
      { key: "offers_physical_therapy_services", label: "PT" },
    ],
  },
  {
    key: "irf",
    label: "IRF",
    endpoint: "/api/irf/quality",
    summaryEndpoint: "/api/irf/summary",
    nameField: "facility_name",
    columns: [
      { key: "facility_name", label: "Facility", width: 200 },
      { key: "state_code", label: "State", width: 50 },
      { key: "measure_name", label: "Measure" },
      { key: "score", label: "Score" },
    ],
  },
  {
    key: "ltch",
    label: "LTCH",
    endpoint: "/api/ltch/quality",
    summaryEndpoint: "/api/ltch/summary",
    nameField: "facility_name",
    columns: [
      { key: "facility_name", label: "Facility", width: 200 },
      { key: "state_code", label: "State", width: 50 },
      { key: "measure_name", label: "Measure" },
      { key: "score", label: "Score" },
    ],
  },
];

const STATES = [
  "","AL","AK","AZ","AR","CA","CO","CT","DE","FL","GA","HI","ID","IL","IN","IA",
  "KS","KY","LA","ME","MD","MA","MI","MN","MS","MO","MT","NE","NV","NH","NJ",
  "NM","NY","NC","ND","OH","OK","OR","PA","RI","SC","SD","TN","TX","UT","VT",
  "VA","WA","WV","WI","WY","DC",
];

export default function FacilityDirectory() {
  const [facilityType, setFacilityType] = useState<FacilityType>("fqhc");
  const [state, setState] = useState("");
  const [rows, setRows] = useState<Record<string, unknown>[]>([]);
  const [summaryRows, setSummaryRows] = useState<Record<string, unknown>[]>([]);
  const [loading, setLoading] = useState(false);
  const [search, setSearch] = useState("");

  const config = FACILITY_TYPES.find((f) => f.key === facilityType)!;

  // Load summary on type change
  useEffect(() => {
    fetch(`${API_BASE}${config.summaryEndpoint}`)
      .then((r) => r.json())
      .then((d) => setSummaryRows(d.rows || []))
      .catch(() => setSummaryRows([]));
  }, [config.summaryEndpoint]);

  // Load facilities
  const loadFacilities = useCallback(async () => {
    setLoading(true);
    try {
      const url = state
        ? `${API_BASE}${config.endpoint}?state_code=${state}`
        : `${API_BASE}${config.endpoint}`;
      const res = await fetch(url);
      const data = await res.json();
      setRows(data.rows || []);
    } catch {
      setRows([]);
    }
    setLoading(false);
  }, [config.endpoint, state]);

  useEffect(() => {
    loadFacilities();
  }, [loadFacilities]);

  const filtered = search
    ? rows.filter((r) => {
        const name = String(r[config.nameField] || "").toLowerCase();
        return name.includes(search.toLowerCase());
      })
    : rows;

  const displayed = filtered.slice(0, 300);

  return (
    <div style={{ padding: "16px 20px", fontFamily: FONT.body }}>
      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 12 }}>
        <div>
          <h3 style={{ margin: 0, fontSize: 15, color: C.ink, fontWeight: 700 }}>
            Facility Directory
          </h3>
          <p style={{ margin: "2px 0 0", fontSize: 11, color: C.inkLight }}>
            FQHCs, dialysis, hospice, home health, rehab, and long-term care facilities
          </p>
        </div>
      </div>

      {/* Controls row */}
      <div style={{ display: "flex", gap: 8, marginBottom: 12, flexWrap: "wrap", alignItems: "center" }}>
        {/* Facility type pills */}
        <div style={{ display: "flex", gap: 4 }}>
          {FACILITY_TYPES.map((ft) => (
            <button
              key={ft.key}
              onClick={() => { setFacilityType(ft.key); setSearch(""); }}
              style={{
                padding: "4px 10px", fontSize: 11, fontFamily: FONT.body, fontWeight: 500,
                borderRadius: 4, cursor: "pointer", transition: "all 0.1s",
                background: facilityType === ft.key ? C.brand : C.white,
                color: facilityType === ft.key ? C.white : C.inkLight,
                border: `1px solid ${facilityType === ft.key ? C.brand : C.border}`,
              }}
            >
              {ft.label}
            </button>
          ))}
        </div>

        {/* State filter */}
        <select
          value={state}
          onChange={(e) => setState(e.target.value)}
          style={{
            padding: "4px 8px", fontSize: 11, borderRadius: 4, border: `1px solid ${C.border}`,
            fontFamily: FONT.body,
          }}
        >
          <option value="">All States</option>
          {STATES.filter(Boolean).map((s) => <option key={s} value={s}>{s}</option>)}
        </select>

        {/* Search */}
        <input
          type="text"
          placeholder="Search by name..."
          value={search}
          onChange={(e) => setSearch(e.target.value)}
          style={{
            padding: "4px 10px", fontSize: 11, borderRadius: 4, border: `1px solid ${C.border}`,
            fontFamily: FONT.body, width: 180,
          }}
        />

        <span style={{ fontSize: 10, color: C.inkLight, fontFamily: FONT.mono }}>
          {loading ? "Loading..." : `${filtered.length} results`}
        </span>
      </div>

      {/* Summary cards */}
      {summaryRows.length > 0 && !state && (
        <div style={{
          display: "grid", gridTemplateColumns: "repeat(auto-fill, minmax(120px, 1fr))",
          gap: 8, marginBottom: 12,
        }}>
          {summaryRows.slice(0, 10).map((row, i) => (
            <div
              key={i}
              onClick={() => setState(String(row.state_code || ""))}
              style={{
                background: C.white, border: `1px solid ${C.border}`, borderRadius: 6,
                padding: "8px 10px", cursor: "pointer", boxShadow: SHADOW,
                transition: "border-color 0.1s",
              }}
              onMouseEnter={(e) => (e.currentTarget.style.borderColor = C.brand)}
              onMouseLeave={(e) => (e.currentTarget.style.borderColor = C.border)}
            >
              <div style={{ fontSize: 13, fontWeight: 700, color: C.brand }}>
                {String(row.state_code || "")}
              </div>
              <div style={{ fontSize: 10, color: C.inkLight }}>
                {String(row.facility_count || row.site_count || row.designations || "—")} facilities
              </div>
            </div>
          ))}
        </div>
      )}

      {/* Results table */}
      <div style={{ overflowX: "auto", border: `1px solid ${C.border}`, borderRadius: 6, boxShadow: SHADOW }}>
        <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 11 }}>
          <thead>
            <tr>
              {config.columns.map((col) => (
                <th
                  key={col.key}
                  style={{
                    padding: "6px 8px", fontSize: 10, fontWeight: 600, color: C.white,
                    background: C.brand, textAlign: "left", whiteSpace: "nowrap",
                    maxWidth: col.width,
                  }}
                >
                  {col.label}
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {displayed.map((row, i) => (
              <tr key={i} style={{ background: i % 2 ? C.surface : C.white }}>
                {config.columns.map((col) => (
                  <td
                    key={col.key}
                    style={{
                      padding: "5px 8px", maxWidth: col.width || 160,
                      overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap",
                    }}
                  >
                    {row[col.key] != null ? String(row[col.key]) : "-"}
                  </td>
                ))}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      {filtered.length > 300 && (
        <div style={{ fontSize: 10, color: C.inkLight, marginTop: 4 }}>
          Showing first 300 of {filtered.length}. Filter by state or search to narrow results.
        </div>
      )}
      {displayed.length === 0 && !loading && (
        <div style={{ padding: 24, textAlign: "center", color: C.inkLight, fontSize: 12 }}>
          No facilities found. Try a different state or facility type.
        </div>
      )}
    </div>
  );
}
