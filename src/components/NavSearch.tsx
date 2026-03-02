import { useState, useRef } from "react";
import { C, FONT, SHADOW_LG } from "../design";
import type { ToolDef } from "../types";

interface NavSearchProps {
  tools: ToolDef[];
}

export default function NavSearch({ tools }: NavSearchProps) {
  const [q, setQ] = useState("");
  const [focused, setFocused] = useState(false);
  const ref = useRef<HTMLDivElement>(null);

  const results: { label: string; route: string }[] = [];
  if (q.length >= 2) {
    const lower = q.toLowerCase();
    for (const t of tools) {
      if (t.name.toLowerCase().includes(lower) || t.tagline.toLowerCase().includes(lower)) {
        results.push({ label: t.name, route: `#/${t.id}` });
      }
    }
  }

  return (
    <div style={{ position: "relative" }} ref={ref}>
      <div style={{
        display: "flex",
        alignItems: "center",
        gap: 6,
        background: focused ? C.white : C.surface,
        border: `1px solid ${focused ? C.brand : "transparent"}`,
        borderRadius: 6,
        padding: "3px 8px",
        transition: "all .2s ease",
        width: focused || q ? 180 : 28,
        overflow: "hidden",
      }}>
        <span
          style={{ fontSize: 12, color: C.inkLight, flexShrink: 0, cursor: "pointer" }}
          onClick={() => { setFocused(true); ref.current?.querySelector("input")?.focus(); }}
        >
          ⌕
        </span>
        <input
          value={q}
          onChange={e => setQ(e.target.value)}
          onFocus={() => setFocused(true)}
          onBlur={() => setTimeout(() => setFocused(false), 200)}
          placeholder="Search tools..."
          style={{
            border: "none",
            outline: "none",
            background: "transparent",
            fontSize: 11,
            color: C.ink,
            fontFamily: FONT.body,
            width: "100%",
            opacity: focused || q ? 1 : 0,
          }}
        />
      </div>
      {focused && results.length > 0 && (
        <div style={{
          position: "absolute",
          top: "calc(100% + 6px)",
          right: 0,
          width: 260,
          maxWidth: "calc(100vw - 24px)",
          background: C.white,
          border: `1px solid ${C.border}`,
          borderRadius: 10,
          boxShadow: SHADOW_LG,
          padding: "4px 0",
          zIndex: 200,
        }}>
          {results.map((r, i) => (
            <a
              key={i}
              href={r.route}
              onClick={() => { setQ(""); setFocused(false); }}
              style={{
                display: "block",
                padding: "10px 14px",
                textDecoration: "none",
                transition: "background .1s",
              }}
              onMouseEnter={e => { e.currentTarget.style.background = C.surface; }}
              onMouseLeave={e => { e.currentTarget.style.background = "transparent"; }}
            >
              <div style={{ fontSize: 12, fontWeight: 500, color: C.ink }}>{r.label}</div>
            </a>
          ))}
        </div>
      )}
    </div>
  );
}
