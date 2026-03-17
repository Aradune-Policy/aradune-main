/**
 * Data Catalog — Browse all 750+ tables in the Aradune data lake.
 * Shows table names, row counts, columns, and descriptions.
 */
import { useState, useEffect, useMemo } from "react";
import { API_BASE } from "../lib/api";
import { LoadingBar } from "../components/LoadingBar";

// ── Design tokens ───────────────────────────────────────────────────────
const A = "#0A2540", AL = "#425A70", POS = "#2E6B4A";
const SF = "#F5F7F5", BD = "#E4EAE4", WH = "#fff";
const FM = "'SF Mono',Menlo,Consolas,monospace";
const FB = "'Helvetica Neue',Arial,sans-serif";
const SH = "0 1px 3px rgba(0,0,0,.04),0 4px 12px rgba(0,0,0,.03)";

interface Column { name: string; type: string }
interface Table { name: string; rows: number; columns: Column[]; description: string; category: string }

export default function DataCatalog() {
  const [tables, setTables] = useState<Table[]>([]);
  const [totalRows, setTotalRows] = useState(0);
  const [loading, setLoading] = useState(true);
  const [search, setSearch] = useState("");
  const [categoryFilter, setCategoryFilter] = useState<string>("all");
  const [expanded, setExpanded] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    fetch(`${API_BASE}/api/catalog`)
      .then(r => {
        if (!r.ok) throw new Error(`API returned ${r.status}`);
        return r.json();
      })
      .then(d => {
        setTables(d.tables || []);
        setTotalRows(d.total_rows || 0);
        setError(null);
      })
      .catch(err => {
        setError(`Unable to load data catalog: ${err.message}`);
        setTables([]);
      })
      .finally(() => setLoading(false));
  }, []);

  const filtered = useMemo(() => {
    let t = tables;
    if (categoryFilter !== "all") t = t.filter(tb => tb.category === categoryFilter);
    if (search) {
      const q = search.toLowerCase();
      t = t.filter(tb =>
        tb.name.toLowerCase().includes(q) ||
        tb.description.toLowerCase().includes(q) ||
        tb.columns.some(c => c.name.toLowerCase().includes(q))
      );
    }
    return t;
  }, [tables, search, categoryFilter]);

  const counts = useMemo(() => ({
    fact: tables.filter(t => t.category === "fact").length,
    dimension: tables.filter(t => t.category === "dimension").length,
    reference: tables.filter(t => t.category === "reference").length,
  }), [tables]);

  if (loading) return <LoadingBar text="Loading catalog" detail="Table schemas and row counts" />;

  return (
    <div style={{ maxWidth: 1100, margin: "0 auto", padding: "32px 16px", fontFamily: FB }}>
      {/* Header */}
      <div style={{ marginBottom: 24 }}>
        <h1 style={{ fontSize: 22, fontWeight: 700, color: A, margin: 0, letterSpacing: -0.3 }}>
          Data Catalog
        </h1>
        <p style={{ fontSize: 13, color: AL, margin: "6px 0 0", lineHeight: 1.5 }}>
          {tables.length} tables &middot; {totalRows.toLocaleString()} total rows &middot; Updated daily from federal sources
        </p>
      </div>

      {/* Stats */}
      <div style={{ display: "flex", gap: 16, marginBottom: 20 }}>
        {[
          { label: "Fact tables", count: counts.fact, color: POS },
          { label: "Dimensions", count: counts.dimension, color: "#3A7D5C" },
          { label: "Reference", count: counts.reference, color: "#5B6E8A" },
        ].map(s => (
          <div key={s.label} style={{
            background: WH, borderRadius: 8, padding: "12px 16px", boxShadow: SH,
            borderTop: `2px solid ${s.color}`, minWidth: 100,
          }}>
            <div style={{ fontSize: 20, fontWeight: 700, color: s.color, fontFamily: FM }}>{s.count}</div>
            <div style={{ fontSize: 11, color: AL, marginTop: 2 }}>{s.label}</div>
          </div>
        ))}
      </div>

      {/* Search + Filter */}
      <div style={{ display: "flex", gap: 10, marginBottom: 16, flexWrap: "wrap" }}>
        <input
          value={search}
          onChange={e => setSearch(e.target.value)}
          placeholder="Search tables or columns..."
          style={{
            flex: 1, minWidth: 200, padding: "8px 12px", fontSize: 13, fontFamily: FB,
            border: `1px solid ${BD}`, borderRadius: 6, outline: "none", color: A, background: WH,
          }}
        />
        <div style={{ display: "flex", gap: 4 }}>
          {(["all", "fact", "dimension", "reference"] as const).map(cat => (
            <button
              key={cat}
              onClick={() => setCategoryFilter(cat)}
              style={{
                padding: "6px 12px", fontSize: 11, fontWeight: categoryFilter === cat ? 600 : 400,
                background: categoryFilter === cat ? `${POS}12` : "none",
                color: categoryFilter === cat ? POS : AL,
                border: `1px solid ${categoryFilter === cat ? POS : BD}`, borderRadius: 6,
                cursor: "pointer", fontFamily: FB, textTransform: "capitalize",
              }}
            >
              {cat === "all" ? `All (${tables.length})` : `${cat} (${counts[cat]})`}
            </button>
          ))}
        </div>
      </div>

      {/* Error state */}
      {error && (
        <div style={{
          padding: "16px", marginBottom: 16, borderRadius: 8,
          background: "#FEE2E2", border: "1px solid #A4262C",
          color: "#A4262C", fontSize: 13,
        }}>
          {error}
        </div>
      )}

      {/* Table list */}
      <div style={{ display: "flex", flexDirection: "column", gap: 6 }}>
        {filtered.map(table => {
          const isExpanded = expanded === table.name;
          return (
            <div
              key={table.name}
              style={{
                background: WH, borderRadius: 8, boxShadow: SH, overflow: "hidden",
                border: isExpanded ? `1px solid ${POS}40` : `1px solid transparent`,
              }}
            >
              <div
                onClick={() => setExpanded(isExpanded ? null : table.name)}
                style={{
                  padding: "12px 16px", cursor: "pointer", display: "flex",
                  alignItems: "center", gap: 12,
                }}
              >
                <span style={{
                  fontSize: 10, fontWeight: 600, fontFamily: FM, color: WH, padding: "2px 6px",
                  borderRadius: 4, flexShrink: 0, textTransform: "uppercase",
                  background: table.category === "fact" ? POS : table.category === "dimension" ? "#3A7D5C" : "#5B6E8A",
                }}>
                  {table.category.slice(0, 3)}
                </span>
                <div style={{ flex: 1, minWidth: 0 }}>
                  <div style={{ fontSize: 13, fontWeight: 600, color: A, fontFamily: FM }}>{table.name}</div>
                  {table.description && (
                    <div style={{ fontSize: 11, color: AL, marginTop: 2, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>
                      {table.description}
                    </div>
                  )}
                </div>
                <div style={{ textAlign: "right", flexShrink: 0 }}>
                  <div style={{ fontSize: 12, fontWeight: 600, color: A, fontFamily: FM }}>
                    {table.rows.toLocaleString()}
                  </div>
                  <div style={{ fontSize: 10, color: AL }}>rows</div>
                </div>
                <span style={{
                  fontSize: 10, color: AL, flexShrink: 0, transition: "transform .15s",
                  transform: isExpanded ? "rotate(180deg)" : "none",
                }}>
                  ▼
                </span>
              </div>

              {/* Expanded column list */}
              {isExpanded && (
                <div style={{
                  padding: "0 16px 14px", borderTop: `1px solid ${BD}`,
                }}>
                  <div style={{
                    display: "grid", gridTemplateColumns: "repeat(auto-fill, minmax(200px, 1fr))",
                    gap: 4, marginTop: 10,
                  }}>
                    {table.columns.filter(c => !["snapshot", "snapshot_date", "pipeline_run_id"].includes(c.name)).map(col => (
                      <div key={col.name} style={{ display: "flex", gap: 6, alignItems: "baseline", fontSize: 11 }}>
                        <span style={{ fontFamily: FM, color: A, fontWeight: 500 }}>{col.name}</span>
                        <span style={{ fontFamily: FM, color: AL, fontSize: 10 }}>{col.type}</span>
                      </div>
                    ))}
                  </div>
                  <div style={{ marginTop: 10 }}>
                    <button
                      onClick={(e) => {
                        e.stopPropagation();
                        window.location.hash = `/ask`;
                        // Pre-populate with a query about this table
                      }}
                      style={{
                        background: "none", border: `1px solid ${BD}`, borderRadius: 4,
                        padding: "4px 10px", fontSize: 11, color: POS, cursor: "pointer",
                        fontFamily: FM, fontWeight: 600,
                      }}
                    >
                      Query this table →
                    </button>
                  </div>
                </div>
              )}
            </div>
          );
        })}
      </div>

      {filtered.length === 0 && (
        <div style={{ textAlign: "center", padding: 40, color: AL, fontSize: 13 }}>
          No tables match your search.
        </div>
      )}
    </div>
  );
}
