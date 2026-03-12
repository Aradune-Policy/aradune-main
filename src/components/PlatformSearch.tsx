import { useState, useEffect, useRef, useCallback } from "react";
import { C, FONT, SHADOW_LG } from "../design";
import { STATES_LIST, STATE_NAMES } from "../data/states";
import { API_BASE } from "../lib/api";
import type { ToolDef } from "../types";

// ── Types ──────────────────────────────────────────────────────────────
interface SearchResult {
  type: "tool" | "state" | "table" | "code" | "column";
  label: string;
  sublabel?: string;
  route: string;
  score: number;
  icon?: string;
  color?: string;
}

interface CatalogTable {
  name: string;
  rows: number;
  description: string;
  category: string;
  columns: { name: string; type: string }[];
}

interface ApiSearchResults {
  tables: { name: string; description: string; score: number; category: string }[];
  columns: { table: string; column: string; type: string; score: number; table_description: string }[];
  codes: { code: string; description: string; category: string; score: number }[];
}

// ── Constants ──────────────────────────────────────────────────────────
const RECENT_KEY = "aradune_recent_searches";
const MAX_RECENT = 8;
const CATALOG_CACHE_KEY = "__platformSearchCatalogCache";

// Category labels and colors
const CATEGORY_META: Record<string, { label: string; color: string; icon: string }> = {
  tool: { label: "Tools", color: C.brand, icon: "◧" },
  state: { label: "States", color: "#3478F6", icon: "◉" },
  table: { label: "Data Tables", color: C.accent, icon: "◇" },
  code: { label: "HCPCS Codes", color: C.teal, icon: "⌗" },
  column: { label: "Columns", color: "#8B5CF6", icon: "≡" },
};

// ── Helper: format table name for display ──────────────────────────────
function humanTableName(name: string): string {
  return name
    .replace(/^fact_/, "")
    .replace(/^dim_/, "")
    .replace(/^ref_/, "")
    .replace(/_/g, " ");
}

// ── Helpers: recent searches ───────────────────────────────────────────
function getRecent(): string[] {
  try {
    const raw = localStorage.getItem(RECENT_KEY);
    return raw ? JSON.parse(raw) : [];
  } catch {
    return [];
  }
}

function addRecent(q: string): void {
  try {
    const list = getRecent().filter(s => s !== q);
    list.unshift(q);
    localStorage.setItem(RECENT_KEY, JSON.stringify(list.slice(0, MAX_RECENT)));
  } catch {
    // localStorage unavailable
  }
}

// ── Component ──────────────────────────────────────────────────────────
interface PlatformSearchProps {
  tools: ToolDef[];
}

export default function PlatformSearch({ tools }: PlatformSearchProps) {
  const [open, setOpen] = useState(false);
  const [q, setQ] = useState("");
  const [selected, setSelected] = useState(0);
  const [catalogCache, setCatalogCache] = useState<CatalogTable[] | null>(null);
  const [apiResults, setApiResults] = useState<ApiSearchResults | null>(null);
  const [apiLoading, setApiLoading] = useState(false);
  const inputRef = useRef<HTMLInputElement>(null);
  const listRef = useRef<HTMLDivElement>(null);
  const debounceRef = useRef<ReturnType<typeof setTimeout> | null>(null);

  // ── Keyboard shortcut: Cmd+K / Ctrl+K ──────────────────────────────
  useEffect(() => {
    const handler = (e: KeyboardEvent) => {
      if ((e.metaKey || e.ctrlKey) && e.key === "k") {
        e.preventDefault();
        setOpen(prev => !prev);
      }
      if (e.key === "Escape" && open) {
        setOpen(false);
      }
    };
    window.addEventListener("keydown", handler);
    return () => window.removeEventListener("keydown", handler);
  }, [open]);

  // ── Focus input when modal opens ───────────────────────────────────
  useEffect(() => {
    if (open) {
      setQ("");
      setSelected(0);
      setApiResults(null);
      setTimeout(() => inputRef.current?.focus(), 50);
      // Prefetch catalog on first open
      if (!catalogCache) {
        fetchCatalog();
      }
    }
  }, [open]);

  // ── Fetch catalog (once, cached) ───────────────────────────────────
  const fetchCatalog = useCallback(async () => {
    // Check in-memory cache
    if ((window as any)[CATALOG_CACHE_KEY]) {
      setCatalogCache((window as any)[CATALOG_CACHE_KEY]);
      return;
    }
    try {
      const res = await fetch(`${API_BASE}/api/catalog`);
      if (res.ok) {
        const data = await res.json();
        const tables = data.tables || [];
        setCatalogCache(tables);
        (window as any)[CATALOG_CACHE_KEY] = tables;
      }
    } catch {
      // Catalog unavailable — search will still work for tools/states
    }
  }, []);

  // ── Debounced API search for tables/columns/codes ──────────────────
  useEffect(() => {
    if (debounceRef.current) clearTimeout(debounceRef.current);
    if (q.length < 2) {
      setApiResults(null);
      setApiLoading(false);
      return;
    }
    setApiLoading(true);
    debounceRef.current = setTimeout(async () => {
      try {
        const res = await fetch(`${API_BASE}/api/search?q=${encodeURIComponent(q)}`);
        if (res.ok) {
          const data = await res.json();
          setApiResults(data.results);
        }
      } catch {
        // API unavailable — rely on client-side results
      }
      setApiLoading(false);
    }, 250);
    return () => { if (debounceRef.current) clearTimeout(debounceRef.current); };
  }, [q]);

  // ── Build results ──────────────────────────────────────────────────
  const results: SearchResult[] = [];
  const lower = q.toLowerCase().trim();

  if (lower.length >= 1) {
    // Tools (client-side, instant)
    for (const t of tools) {
      const nameMatch = t.name.toLowerCase().includes(lower);
      const tagMatch = t.tagline.toLowerCase().includes(lower);
      const descMatch = t.desc.toLowerCase().includes(lower);
      if (nameMatch || tagMatch || descMatch) {
        results.push({
          type: "tool",
          label: t.name,
          sublabel: t.tagline,
          route: `#/${t.id}`,
          score: nameMatch ? 90 : tagMatch ? 60 : 40,
          icon: t.icon,
          color: t.color,
        });
      }
    }

    // Also match "Data Explorer" / "ask" / "NL2SQL"
    if ("data explorer".includes(lower) || "nl2sql".includes(lower) || "ask a question".includes(lower) || "natural language".includes(lower)) {
      if (!results.find(r => r.route === "#/ask")) {
        results.push({
          type: "tool",
          label: "Data Explorer",
          sublabel: "Ask questions in natural language (NL2SQL)",
          route: "#/ask",
          score: 85,
          icon: "⌕",
          color: C.brand,
        });
      }
    }
    if ("data catalog".includes(lower) || "catalog".includes(lower) || "browse tables".includes(lower)) {
      if (!results.find(r => r.route === "#/catalog")) {
        results.push({
          type: "tool",
          label: "Data Catalog",
          sublabel: "Browse all data tables with schemas and row counts",
          route: "#/catalog",
          score: 85,
          icon: "◇",
          color: C.brand,
        });
      }
    }

    // States (client-side, instant)
    for (const code of STATES_LIST) {
      const name = STATE_NAMES[code];
      const codeMatch = code.toLowerCase() === lower;
      const nameMatch = name.toLowerCase().includes(lower);
      if (codeMatch || nameMatch) {
        results.push({
          type: "state",
          label: `${name} (${code})`,
          sublabel: "State profile",
          route: `#/state/${code}`,
          score: codeMatch ? 95 : nameMatch ? 70 : 50,
          icon: "◉",
          color: "#3478F6",
        });
      }
    }

    // API results: tables
    if (apiResults) {
      for (const t of apiResults.tables) {
        // Skip if already from catalog client-side
        results.push({
          type: "table",
          label: humanTableName(t.name),
          sublabel: t.description,
          route: `#/catalog`,
          score: t.score,
          icon: t.category === "dimension" ? "D" : t.category === "reference" ? "R" : "F",
          color: C.accent,
        });
      }

      // API results: codes
      for (const c of apiResults.codes) {
        results.push({
          type: "code",
          label: c.code,
          sublabel: c.description,
          route: `#/lookup`,
          score: c.score,
          icon: "⌗",
          color: C.teal,
        });
      }

      // API results: columns (show fewer, they're supplemental)
      for (const col of apiResults.columns.slice(0, 8)) {
        results.push({
          type: "column",
          label: col.column,
          sublabel: `in ${humanTableName(col.table)}`,
          route: `#/catalog`,
          score: col.score - 5, // slightly lower priority
          icon: "≡",
          color: "#8B5CF6",
        });
      }
    } else if (lower.length >= 2 && catalogCache) {
      // Fallback: search catalog cache client-side if API is not available
      for (const t of catalogCache) {
        const nameMatch = t.name.toLowerCase().includes(lower);
        const descMatch = t.description?.toLowerCase().includes(lower);
        const colMatch = t.columns?.some(c => c.name.toLowerCase().includes(lower));
        if (nameMatch || descMatch || colMatch) {
          results.push({
            type: "table",
            label: humanTableName(t.name),
            sublabel: t.description,
            route: `#/catalog`,
            score: nameMatch ? 60 : descMatch ? 40 : 30,
            icon: t.category === "dimension" ? "D" : t.category === "reference" ? "R" : "F",
            color: C.accent,
          });
        }
      }
    }
  }

  // Deduplicate and sort by score
  const seen = new Set<string>();
  const deduped = results.filter(r => {
    const key = `${r.type}:${r.label}`;
    if (seen.has(key)) return false;
    seen.add(key);
    return true;
  });
  deduped.sort((a, b) => b.score - a.score);

  // Group by category
  const grouped: Record<string, SearchResult[]> = {};
  for (const r of deduped) {
    if (!grouped[r.type]) grouped[r.type] = [];
    grouped[r.type].push(r);
  }

  // Flatten for keyboard navigation
  const flat: SearchResult[] = [];
  const categoryOrder = ["tool", "state", "table", "code", "column"];
  for (const cat of categoryOrder) {
    if (grouped[cat]) flat.push(...grouped[cat].slice(0, 10));
  }

  // ── Navigate result ────────────────────────────────────────────────
  const navigate = (result: SearchResult) => {
    addRecent(q);
    setOpen(false);
    window.location.hash = result.route.replace("#", "");
  };

  // ── Keyboard navigation ───────────────────────────────────────────
  const onKeyDown = (e: React.KeyboardEvent) => {
    if (e.key === "ArrowDown") {
      e.preventDefault();
      setSelected(prev => Math.min(prev + 1, flat.length - 1));
    } else if (e.key === "ArrowUp") {
      e.preventDefault();
      setSelected(prev => Math.max(prev - 1, 0));
    } else if (e.key === "Enter" && flat[selected]) {
      e.preventDefault();
      navigate(flat[selected]);
    }
  };

  // Scroll selected into view
  useEffect(() => {
    if (listRef.current) {
      const el = listRef.current.querySelector(`[data-idx="${selected}"]`);
      if (el) el.scrollIntoView({ block: "nearest" });
    }
  }, [selected]);

  // Reset selection when results change
  useEffect(() => { setSelected(0); }, [q, apiResults]);

  const recent = getRecent();

  // ── Trigger button (in nav) ────────────────────────────────────────
  if (!open) {
    return (
      <button
        onClick={() => setOpen(true)}
        style={{
          display: "flex",
          alignItems: "center",
          gap: 6,
          background: C.surface,
          border: `1px solid transparent`,
          borderRadius: 6,
          padding: "4px 10px",
          cursor: "pointer",
          transition: "all .2s ease",
          height: 28,
        }}
        onMouseEnter={e => { e.currentTarget.style.borderColor = C.border; }}
        onMouseLeave={e => { e.currentTarget.style.borderColor = "transparent"; }}
      >
        <span style={{ fontSize: 12, color: C.inkLight, flexShrink: 0 }}>&#x2315;</span>
        <span style={{ fontSize: 11, color: C.inkLight, fontFamily: FONT.body, whiteSpace: "nowrap" }}>
          Search
        </span>
        <kbd style={{
          fontSize: 9,
          fontFamily: FONT.mono,
          color: C.inkLight,
          background: C.white,
          border: `1px solid ${C.border}`,
          borderRadius: 3,
          padding: "1px 4px",
          marginLeft: 4,
          lineHeight: 1.4,
        }}>
          {navigator.platform?.includes("Mac") ? "\u2318K" : "Ctrl+K"}
        </kbd>
      </button>
    );
  }

  // ── Modal overlay ──────────────────────────────────────────────────
  return (
    <>
      {/* Backdrop */}
      <div
        onClick={() => setOpen(false)}
        style={{
          position: "fixed",
          inset: 0,
          background: "rgba(10,37,64,0.35)",
          backdropFilter: "blur(4px)",
          WebkitBackdropFilter: "blur(4px)",
          zIndex: 9998,
        }}
      />
      {/* Modal */}
      <div style={{
        position: "fixed",
        top: "min(20vh, 120px)",
        left: "50%",
        transform: "translateX(-50%)",
        width: "min(580px, calc(100vw - 32px))",
        maxHeight: "min(70vh, 520px)",
        background: C.white,
        borderRadius: 14,
        boxShadow: "0 16px 70px rgba(0,0,0,0.15), 0 2px 8px rgba(0,0,0,0.08)",
        zIndex: 9999,
        display: "flex",
        flexDirection: "column",
        overflow: "hidden",
      }}>
        {/* Search input */}
        <div style={{
          display: "flex",
          alignItems: "center",
          gap: 10,
          padding: "14px 18px",
          borderBottom: `1px solid ${C.border}`,
        }}>
          <span style={{ fontSize: 16, color: C.inkLight, flexShrink: 0 }}>&#x2315;</span>
          <input
            ref={inputRef}
            value={q}
            onChange={e => setQ(e.target.value)}
            onKeyDown={onKeyDown}
            placeholder="Search tools, states, data tables, HCPCS codes..."
            style={{
              flex: 1,
              border: "none",
              outline: "none",
              fontSize: 14,
              color: C.ink,
              fontFamily: FONT.body,
              background: "transparent",
            }}
          />
          {apiLoading && (
            <span style={{ fontSize: 10, color: C.inkLight, fontFamily: FONT.mono, flexShrink: 0 }}>...</span>
          )}
          <kbd
            onClick={() => setOpen(false)}
            style={{
              fontSize: 10,
              fontFamily: FONT.mono,
              color: C.inkLight,
              background: C.surface,
              border: `1px solid ${C.border}`,
              borderRadius: 4,
              padding: "2px 6px",
              cursor: "pointer",
              flexShrink: 0,
            }}
          >
            ESC
          </kbd>
        </div>

        {/* Results area */}
        <div ref={listRef} style={{ overflowY: "auto", flex: 1 }}>
          {/* Empty state: show recent searches */}
          {lower.length < 1 && (
            <div style={{ padding: "12px 18px" }}>
              {recent.length > 0 && (
                <>
                  <div style={{
                    fontSize: 9, fontWeight: 700, color: C.inkLight,
                    textTransform: "uppercase", letterSpacing: 1,
                    fontFamily: FONT.mono, marginBottom: 8,
                  }}>
                    Recent searches
                  </div>
                  {recent.map((r, i) => (
                    <div
                      key={i}
                      onClick={() => setQ(r)}
                      style={{
                        padding: "6px 10px",
                        fontSize: 12,
                        color: C.ink,
                        cursor: "pointer",
                        borderRadius: 6,
                        display: "flex",
                        alignItems: "center",
                        gap: 8,
                      }}
                      onMouseEnter={e => { e.currentTarget.style.background = C.surface; }}
                      onMouseLeave={e => { e.currentTarget.style.background = "transparent"; }}
                    >
                      <span style={{ fontSize: 10, color: C.inkLight }}>&#x21B3;</span>
                      <span>{r}</span>
                    </div>
                  ))}
                </>
              )}
              {recent.length === 0 && (
                <div style={{ fontSize: 12, color: C.inkLight, padding: "20px 0", textAlign: "center" }}>
                  Type to search across tools, states, data tables, and HCPCS codes.
                </div>
              )}
              {/* Quick links */}
              <div style={{
                fontSize: 9, fontWeight: 700, color: C.inkLight,
                textTransform: "uppercase", letterSpacing: 1,
                fontFamily: FONT.mono, marginTop: 16, marginBottom: 8,
              }}>
                Quick links
              </div>
              {[
                { label: "Data Explorer", sub: "Ask questions in plain English", route: "#/ask", icon: "⌕" },
                { label: "Data Catalog", sub: "Browse all 250+ tables", route: "#/catalog", icon: "◇" },
                { label: "State Profiles", sub: "Select a state to explore", route: "#/state", icon: "◉" },
              ].map((link, i) => (
                <div
                  key={i}
                  onClick={() => { setOpen(false); window.location.hash = link.route.replace("#", ""); }}
                  style={{
                    padding: "8px 10px",
                    fontSize: 12,
                    color: C.ink,
                    cursor: "pointer",
                    borderRadius: 6,
                    display: "flex",
                    alignItems: "center",
                    gap: 10,
                  }}
                  onMouseEnter={e => { e.currentTarget.style.background = C.surface; }}
                  onMouseLeave={e => { e.currentTarget.style.background = "transparent"; }}
                >
                  <span style={{
                    fontSize: 12, width: 24, height: 24, display: "flex",
                    alignItems: "center", justifyContent: "center",
                    borderRadius: 6, background: `${C.brand}0D`, color: C.brand,
                    flexShrink: 0,
                  }}>{link.icon}</span>
                  <div>
                    <div style={{ fontWeight: 500 }}>{link.label}</div>
                    <div style={{ fontSize: 10, color: C.inkLight, marginTop: 1 }}>{link.sub}</div>
                  </div>
                </div>
              ))}
            </div>
          )}

          {/* Results */}
          {lower.length >= 1 && flat.length === 0 && !apiLoading && (
            <div style={{ padding: "32px 18px", textAlign: "center" }}>
              <div style={{ fontSize: 13, color: C.inkLight }}>
                No results for &ldquo;{q}&rdquo;
              </div>
              <div style={{ fontSize: 11, color: C.inkLight, marginTop: 6 }}>
                Try a state name, HCPCS code, or table name.
              </div>
            </div>
          )}

          {lower.length >= 1 && flat.length > 0 && (
            <div style={{ padding: "6px 0" }}>
              {categoryOrder.map(cat => {
                const items = grouped[cat];
                if (!items || items.length === 0) return null;
                const meta = CATEGORY_META[cat];
                return (
                  <div key={cat}>
                    <div style={{
                      padding: "8px 18px 4px",
                      fontSize: 9,
                      fontWeight: 700,
                      color: meta.color,
                      textTransform: "uppercase",
                      letterSpacing: 1,
                      fontFamily: FONT.mono,
                    }}>
                      {meta.label}
                    </div>
                    {items.slice(0, 10).map(r => {
                      const flatIdx = flat.indexOf(r);
                      const isSelected = flatIdx === selected;
                      return (
                        <div
                          key={`${r.type}:${r.label}`}
                          data-idx={flatIdx}
                          onClick={() => navigate(r)}
                          style={{
                            padding: "8px 18px",
                            display: "flex",
                            alignItems: "center",
                            gap: 10,
                            cursor: "pointer",
                            background: isSelected ? `${meta.color}0D` : "transparent",
                            transition: "background 0.1s",
                          }}
                          onMouseEnter={e => {
                            setSelected(flatIdx);
                            e.currentTarget.style.background = `${meta.color}0D`;
                          }}
                          onMouseLeave={e => {
                            if (flatIdx !== selected) e.currentTarget.style.background = "transparent";
                          }}
                        >
                          <span style={{
                            fontSize: 11,
                            width: 24,
                            height: 24,
                            display: "flex",
                            alignItems: "center",
                            justifyContent: "center",
                            borderRadius: 6,
                            background: `${r.color || meta.color}0D`,
                            color: r.color || meta.color,
                            flexShrink: 0,
                            fontWeight: 600,
                          }}>
                            {r.icon || meta.icon}
                          </span>
                          <div style={{ flex: 1, minWidth: 0 }}>
                            <div style={{
                              fontSize: 12,
                              fontWeight: 500,
                              color: C.ink,
                              overflow: "hidden",
                              textOverflow: "ellipsis",
                              whiteSpace: "nowrap",
                            }}>
                              {r.label}
                            </div>
                            {r.sublabel && (
                              <div style={{
                                fontSize: 10,
                                color: C.inkLight,
                                marginTop: 1,
                                overflow: "hidden",
                                textOverflow: "ellipsis",
                                whiteSpace: "nowrap",
                              }}>
                                {r.sublabel}
                              </div>
                            )}
                          </div>
                          {isSelected && (
                            <span style={{
                              fontSize: 10,
                              color: C.inkLight,
                              fontFamily: FONT.mono,
                              flexShrink: 0,
                            }}>
                              &#x21B5;
                            </span>
                          )}
                        </div>
                      );
                    })}
                  </div>
                );
              })}
            </div>
          )}
        </div>

        {/* Footer hint */}
        <div style={{
          padding: "8px 18px",
          borderTop: `1px solid ${C.border}`,
          display: "flex",
          gap: 16,
          alignItems: "center",
          fontSize: 10,
          color: C.inkLight,
          fontFamily: FONT.mono,
        }}>
          <span>&#x2191;&#x2193; navigate</span>
          <span>&#x21B5; open</span>
          <span>esc close</span>
          {flat.length > 0 && (
            <span style={{ marginLeft: "auto" }}>
              {flat.length} result{flat.length !== 1 ? "s" : ""}
            </span>
          )}
        </div>
      </div>
    </>
  );
}
