/**
 * ChartActions — overlay PNG/SVG download buttons on a chart container.
 *
 * Usage:
 *   <ChartActions filename="fl-enrollment">
 *     <ResponsiveContainer ...>
 *       <LineChart .../>
 *     </ResponsiveContainer>
 *   </ChartActions>
 */
import { useRef, useState } from "react";
import type { ReactNode } from "react";
import { C, FONT } from "../design";

interface Props {
  children: ReactNode;
  filename?: string;
}

export default function ChartActions({ children, filename }: Props) {
  const containerRef = useRef<HTMLDivElement>(null);
  const [busy, setBusy] = useState(false);

  const download = async (format: "png" | "svg") => {
    if (!containerRef.current || busy) return;
    setBusy(true);
    try {
      const { downloadChartPNG, downloadChartSVG } = await import("../utils/chartExport");
      const fn = filename ? `aradune-${filename}` : undefined;
      if (format === "png") await downloadChartPNG(containerRef.current, fn ? `${fn}.png` : undefined);
      else downloadChartSVG(containerRef.current, fn ? `${fn}.svg` : undefined);
    } finally {
      setBusy(false);
    }
  };

  const btnStyle: React.CSSProperties = {
    fontSize: 10, fontFamily: FONT.mono, fontWeight: 600,
    color: C.inkLight, background: "rgba(255,255,255,0.85)",
    border: `1px solid ${C.border}`, borderRadius: 3,
    padding: "2px 6px", cursor: "pointer", lineHeight: 1,
  };

  return (
    <div style={{ position: "relative" }} ref={containerRef}>
      {children}
      <div style={{
        position: "absolute", top: 4, right: 4,
        display: "flex", gap: 3, opacity: 0.6,
        transition: "opacity 0.15s",
      }} onMouseEnter={e => (e.currentTarget.style.opacity = "1")}
         onMouseLeave={e => (e.currentTarget.style.opacity = "0.6")}>
        <button style={btnStyle} onClick={() => download("png")} disabled={busy} title="Download PNG">PNG</button>
        <button style={btnStyle} onClick={() => download("svg")} disabled={busy} title="Download SVG">SVG</button>
      </div>
    </div>
  );
}
