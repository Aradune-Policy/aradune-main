// ── ExplainButton ───────────────────────────────────────────────────────
// Small inline button that expands an explanation box below it.
// Uses the explain engine to interpolate templates with data.
//
// Usage:
//   <ExplainButton
//     template="stateRateComparison"
//     data={{ state: "Florida", code: "99213", medicaidRate: "45.20", ... }}
//   />
//   -- or with a raw text string --
//   <ExplainButton text="Florida pays 62% of Medicare for this code." />

import { useState, useCallback } from "react";
import { C, FONT } from "../design";
import { explain } from "../explain";

interface Props {
  /** Named template key from TEMPLATES, or a raw template string with {{key}} placeholders */
  template?: string;
  /** Data to interpolate into the template */
  data?: Record<string, string | number>;
  /** Pre-computed explanation text (skips template engine) */
  text?: string;
}

export default function ExplainButton({ template, data, text }: Props) {
  const [open, setOpen] = useState(false);
  const [copied, setCopied] = useState(false);

  const explanation = text || (template && data ? explain(template, data) : "");
  if (!explanation) return null;

  const handleCopy = useCallback(async () => {
    try {
      await navigator.clipboard.writeText(explanation);
      setCopied(true);
      setTimeout(() => setCopied(false), 1500);
    } catch {
      // Fallback for older browsers
      const ta = document.createElement("textarea");
      ta.value = explanation;
      ta.style.position = "fixed";
      ta.style.opacity = "0";
      document.body.appendChild(ta);
      ta.select();
      document.execCommand("copy");
      document.body.removeChild(ta);
      setCopied(true);
      setTimeout(() => setCopied(false), 1500);
    }
  }, [explanation]);

  return (
    <span style={{ display: "inline-block", verticalAlign: "middle" }}>
      <button
        onClick={() => setOpen(o => !o)}
        style={{
          fontSize: 9,
          fontFamily: FONT.mono,
          fontWeight: 600,
          color: C.inkLight,
          background: "none",
          border: "none",
          padding: "1px 4px",
          cursor: "pointer",
          opacity: 0.7,
          transition: "opacity 0.15s",
          textDecoration: open ? "underline" : "none",
        }}
        onMouseEnter={e => (e.currentTarget.style.opacity = "1")}
        onMouseLeave={e => (e.currentTarget.style.opacity = "0.7")}
        title={open ? "Hide explanation" : "Show explanation"}
      >
        Explain
      </button>

      {open && (
        <div
          style={{
            marginTop: 4,
            padding: "8px 10px",
            borderLeft: `3px solid ${C.brand}`,
            background: C.surface,
            borderRadius: "0 4px 4px 0",
            fontSize: 11,
            fontFamily: FONT.body,
            color: C.ink,
            lineHeight: 1.55,
            maxWidth: 520,
          }}
        >
          <div>{explanation}</div>
          <div style={{ marginTop: 6, textAlign: "right" }}>
            <button
              onClick={handleCopy}
              style={{
                fontSize: 9,
                fontFamily: FONT.mono,
                fontWeight: 600,
                color: copied ? C.pos : C.accent,
                background: "none",
                border: "none",
                cursor: "pointer",
                padding: "1px 4px",
                transition: "color 0.15s",
              }}
            >
              {copied ? "Copied" : "Copy"}
            </button>
          </div>
        </div>
      )}
    </span>
  );
}
