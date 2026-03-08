/**
 * Data Explorer — Ask questions about Medicaid data in plain English.
 * NL2SQL: user question → Claude Sonnet → DuckDB SQL → results table.
 */
import { useState, useEffect, useRef, useCallback } from "react";
import { API_BASE } from "../lib/api";

// ── Design tokens ───────────────────────────────────────────────────────
const A = "#0A2540", AL = "#425A70", POS = "#2E6B4A";
const SF = "#F5F7F5", BD = "#E4EAE4", WH = "#fff";
const ACC = "#2E6B4A";
const FM = "'SF Mono',Menlo,Consolas,monospace";
const FB = "'Helvetica Neue',Arial,sans-serif";
const SH = "0 1px 3px rgba(0,0,0,.04),0 4px 12px rgba(0,0,0,.03)";

// ── Types ────────────────────────────────────────────────────────────────
interface QueryResult {
  sql: string;
  explanation: string;
  rows: Record<string, unknown>[];
  total_rows: number;
  query_ms: number;
}

interface HistoryEntry {
  query: string;
  result: QueryResult;
  timestamp: number;
}

// ── Local UI components ──────────────────────────────────────────────────
const Card = ({ children, accent }: { children: React.ReactNode; accent?: string }) => (
  <div style={{
    background: WH, borderRadius: 10, padding: "20px 24px", marginBottom: 16,
    boxShadow: SH, borderTop: accent ? `3px solid ${accent}` : undefined,
  }}>{children}</div>
);

// ── Helpers ──────────────────────────────────────────────────────────────
function formatCell(val: unknown): string {
  if (val === null || val === undefined) return "—";
  if (typeof val === "number") {
    if (Number.isInteger(val) && Math.abs(val) > 999) return val.toLocaleString();
    if (!Number.isInteger(val)) {
      // If it looks like a percentage (0-10 range), show more decimals
      if (Math.abs(val) <= 10) return val.toFixed(2);
      return val.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 });
    }
    return String(val);
  }
  return String(val);
}

function isNumericColumn(rows: Record<string, unknown>[], col: string): boolean {
  return rows.some(r => typeof r[col] === "number");
}

function downloadCSV(rows: Record<string, unknown>[], filename: string) {
  if (!rows.length) return;
  const cols = Object.keys(rows[0]);
  const header = cols.join(",");
  const body = rows.map(r => cols.map(c => {
    const v = r[c];
    if (v === null || v === undefined) return "";
    if (typeof v === "string" && (v.includes(",") || v.includes('"') || v.includes("\n")))
      return `"${v.replace(/"/g, '""')}"`;
    return String(v);
  }).join(",")).join("\n");
  const blob = new Blob([header + "\n" + body], { type: "text/csv" });
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = filename;
  a.click();
  URL.revokeObjectURL(url);
}

// ── Main Component ───────────────────────────────────────────────────────
export default function DataExplorer() {
  const [query, setQuery] = useState("");
  const [result, setResult] = useState<QueryResult | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [examples, setExamples] = useState<string[]>([]);
  const [history, setHistory] = useState<HistoryEntry[]>([]);
  const [showSQL, setShowSQL] = useState(false);
  const inputRef = useRef<HTMLTextAreaElement>(null);

  // Load examples on mount
  useEffect(() => {
    fetch(`${API_BASE}/api/nl2sql/examples`)
      .then(r => r.ok ? r.json() : null)
      .then(d => d?.examples && setExamples(d.examples))
      .catch(() => {});
  }, []);

  // Focus textarea on mount
  useEffect(() => { inputRef.current?.focus(); }, []);

  const runQuery = useCallback(async (q: string) => {
    if (!q.trim()) return;
    setLoading(true);
    setError(null);
    setResult(null);
    setShowSQL(false);

    try {
      const res = await fetch(`${API_BASE}/api/nl2sql`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ query: q.trim(), limit: 200 }),
      });
      const data = await res.json();
      if (!res.ok) {
        throw new Error(data.detail || "Query failed");
      }
      setResult(data);
      setHistory(prev => [{ query: q.trim(), result: data, timestamp: Date.now() }, ...prev].slice(0, 20));
    } catch (err: unknown) {
      setError(err instanceof Error ? err.message : "Unknown error");
    } finally {
      setLoading(false);
    }
  }, []);

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    runQuery(query);
  };

  const handleKeyDown = (e: React.KeyboardEvent) => {
    if (e.key === "Enter" && (e.metaKey || e.ctrlKey)) {
      e.preventDefault();
      runQuery(query);
    }
  };

  const columns = result?.rows?.length ? Object.keys(result.rows[0]) : [];

  return (
    <div style={{ maxWidth: 1100, margin: "0 auto", padding: "32px 16px", fontFamily: FB }}>
      {/* Header */}
      <div style={{ marginBottom: 24 }}>
        <h1 style={{ fontSize: 22, fontWeight: 700, color: A, margin: 0, letterSpacing: -0.3 }}>
          Data Explorer
        </h1>
        <p style={{ fontSize: 13, color: AL, margin: "6px 0 0", lineHeight: 1.5 }}>
          Ask questions about Medicaid data in plain English. Queries run against 185 tables and 101M+ rows.
        </p>
      </div>

      {/* Input */}
      <Card accent={ACC}>
        <form onSubmit={handleSubmit}>
          <textarea
            ref={inputRef}
            value={query}
            onChange={e => setQuery(e.target.value)}
            onKeyDown={handleKeyDown}
            placeholder="What states pay the lowest primary care rates as a percentage of Medicare?"
            disabled={loading}
            rows={3}
            style={{
              width: "100%", padding: 12, fontSize: 14, fontFamily: FB,
              border: `1px solid ${BD}`, borderRadius: 6, resize: "vertical",
              outline: "none", color: A, background: SF, lineHeight: 1.5,
              boxSizing: "border-box",
            }}
          />
          <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", marginTop: 10 }}>
            <span style={{ fontSize: 11, color: AL }}>
              {loading ? "Thinking..." : "Cmd+Enter to run"}
            </span>
            <button
              type="submit"
              disabled={loading || !query.trim()}
              style={{
                background: loading ? AL : ACC, color: WH, border: "none",
                borderRadius: 6, padding: "8px 20px", fontSize: 13, fontWeight: 600,
                cursor: loading ? "wait" : "pointer", opacity: !query.trim() ? 0.5 : 1,
              }}
            >
              {loading ? "Running..." : "Run Query"}
            </button>
          </div>
        </form>
      </Card>

      {/* Examples */}
      {!result && !error && examples.length > 0 && (
        <div style={{ marginBottom: 24 }}>
          <div style={{ fontSize: 11, fontWeight: 600, color: AL, marginBottom: 8, textTransform: "uppercase", letterSpacing: 0.5 }}>
            Try these
          </div>
          <div style={{ display: "flex", flexWrap: "wrap", gap: 6 }}>
            {examples.map((ex, i) => (
              <button
                key={i}
                onClick={() => { setQuery(ex); runQuery(ex); }}
                style={{
                  background: SF, border: `1px solid ${BD}`, borderRadius: 16,
                  padding: "6px 12px", fontSize: 12, color: A, cursor: "pointer",
                  fontFamily: FB, lineHeight: 1.3,
                }}
              >
                {ex}
              </button>
            ))}
          </div>
        </div>
      )}

      {/* Error */}
      {error && (
        <Card>
          <div style={{ color: "#A4262C", fontSize: 13, fontWeight: 600, marginBottom: 4 }}>Query failed</div>
          <div style={{ fontSize: 12, color: AL, fontFamily: FM, whiteSpace: "pre-wrap" }}>{error}</div>
        </Card>
      )}

      {/* Results */}
      {result && (
        <>
          {/* Explanation + metadata */}
          <Card>
            <div style={{ display: "flex", justifyContent: "space-between", alignItems: "flex-start", flexWrap: "wrap", gap: 8 }}>
              <div style={{ flex: 1, minWidth: 200 }}>
                <div style={{ fontSize: 13, color: A, lineHeight: 1.5 }}>{result.explanation}</div>
              </div>
              <div style={{ display: "flex", gap: 12, alignItems: "center", flexShrink: 0 }}>
                <span style={{ fontSize: 11, color: AL, fontFamily: FM }}>
                  {result.total_rows} row{result.total_rows !== 1 ? "s" : ""} &middot; {result.query_ms}ms
                </span>
                <button
                  onClick={() => setShowSQL(!showSQL)}
                  style={{
                    background: "none", border: `1px solid ${BD}`, borderRadius: 4,
                    padding: "3px 8px", fontSize: 11, color: AL, cursor: "pointer", fontFamily: FM,
                  }}
                >
                  {showSQL ? "Hide SQL" : "Show SQL"}
                </button>
                <button
                  onClick={() => downloadCSV(result.rows, "aradune_query.csv")}
                  style={{
                    background: "none", border: `1px solid ${BD}`, borderRadius: 4,
                    padding: "3px 8px", fontSize: 11, color: ACC, cursor: "pointer", fontFamily: FM, fontWeight: 600,
                  }}
                >
                  CSV
                </button>
              </div>
            </div>

            {/* SQL block */}
            {showSQL && (
              <pre style={{
                marginTop: 12, padding: 12, background: "#0A2540", color: "#E0E6EC",
                borderRadius: 6, fontSize: 12, fontFamily: FM, overflow: "auto",
                lineHeight: 1.5, whiteSpace: "pre-wrap", wordBreak: "break-word",
              }}>
                {result.sql}
              </pre>
            )}
          </Card>

          {/* Results table */}
          {result.rows.length > 0 && (
            <div style={{
              background: WH, borderRadius: 10, boxShadow: SH, overflow: "auto",
              marginBottom: 16,
            }}>
              <table style={{
                width: "100%", borderCollapse: "collapse", fontSize: 12,
                fontFamily: FM,
              }}>
                <thead>
                  <tr>
                    {columns.map(col => (
                      <th key={col} style={{
                        padding: "10px 12px", textAlign: isNumericColumn(result.rows, col) ? "right" : "left",
                        borderBottom: `2px solid ${BD}`, color: AL, fontSize: 11,
                        fontWeight: 600, whiteSpace: "nowrap", position: "sticky", top: 0,
                        background: WH, textTransform: "uppercase", letterSpacing: 0.3,
                      }}>
                        {col.replace(/_/g, " ")}
                      </th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {result.rows.map((row, i) => (
                    <tr key={i} style={{ background: i % 2 === 0 ? WH : SF }}>
                      {columns.map(col => (
                        <td key={col} style={{
                          padding: "8px 12px",
                          textAlign: isNumericColumn(result.rows, col) ? "right" : "left",
                          borderBottom: `1px solid ${BD}`,
                          color: A, whiteSpace: "nowrap",
                        }}>
                          {formatCell(row[col])}
                        </td>
                      ))}
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}

          {result.rows.length === 0 && (
            <Card>
              <div style={{ textAlign: "center", padding: 20, color: AL, fontSize: 13 }}>
                No rows returned. Try rephrasing your question.
              </div>
            </Card>
          )}
        </>
      )}

      {/* History */}
      {history.length > 1 && !loading && (
        <div style={{ marginTop: 24 }}>
          <div style={{ fontSize: 11, fontWeight: 600, color: AL, marginBottom: 8, textTransform: "uppercase", letterSpacing: 0.5 }}>
            Recent queries
          </div>
          {history.slice(1).map((h, i) => (
            <button
              key={i}
              onClick={() => { setQuery(h.query); setResult(h.result); }}
              style={{
                display: "block", width: "100%", textAlign: "left",
                background: "none", border: `1px solid ${BD}`, borderRadius: 6,
                padding: "8px 12px", marginBottom: 4, cursor: "pointer",
                fontSize: 12, color: A, fontFamily: FB,
              }}
            >
              {h.query}
              <span style={{ fontSize: 10, color: AL, marginLeft: 8 }}>
                {h.result.total_rows} rows &middot; {h.result.query_ms}ms
              </span>
            </button>
          ))}
        </div>
      )}
    </div>
  );
}
