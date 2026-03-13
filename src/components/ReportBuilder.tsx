import { useState, useCallback } from "react";
import { C, FONT, SHADOW_LG } from "../design";
import { useAradune } from "../context/AraduneContext";
import type { ReportSection } from "../context/AraduneContext";

function formatTimestamp(date: Date): string {
  const d = date instanceof Date ? date : new Date(date);
  return d.toLocaleString("en-US", {
    month: "short",
    day: "numeric",
    hour: "numeric",
    minute: "2-digit",
    hour12: true,
  });
}

function truncate(text: string, max: number): string {
  if (!text) return "";
  return text.length > max ? text.slice(0, max) + "..." : text;
}

function exportCSV(sections: { id: string; prompt: string; response: string; createdAt: Date }[]) {
  const escape = (s: string) => '"' + s.replace(/"/g, '""') + '"';
  const header = "section_number,prompt,response";
  const rows = sections.map(
    (s, i) => `${i + 1},${escape(s.prompt)},${escape(s.response)}`
  );
  const csv = [header, ...rows].join("\n");
  const blob = new Blob([csv], { type: "text/csv;charset=utf-8;" });
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = `aradune-report-${new Date().toISOString().slice(0, 10)}.csv`;
  a.click();
  URL.revokeObjectURL(url);
}

async function exportDOCX(sections: ReportSection[]) {
  const { generateReportDocx } = await import("../utils/reportDocx");
  await generateReportDocx(sections);
}

async function exportPDF(sections: ReportSection[]) {
  const { generateReportPdf } = await import("../utils/reportPdf");
  await generateReportPdf(sections);
}

async function exportXLSX(sections: ReportSection[]) {
  const { generateReportXlsx } = await import("../utils/reportXlsx");
  await generateReportXlsx(sections);
}

export default function ReportBuilder() {
  const { reportSections, removeReportSection, clearReport } = useAradune();
  const [reportBuilderOpen, setReportBuilderOpen] = useState(false);
  const [confirmClear, setConfirmClear] = useState(false);
  const [exporting, setExporting] = useState<string | null>(null);

  const handleClear = useCallback(() => {
    if (confirmClear) {
      clearReport();
      setConfirmClear(false);
    } else {
      setConfirmClear(true);
      setTimeout(() => setConfirmClear(false), 3000);
    }
  }, [confirmClear, clearReport]);

  return (
    <>
      {/* Floating "Report" button — always visible */}
      <button
        onClick={() => setReportBuilderOpen((o) => !o)}
        style={{
          position: "fixed",
          bottom: 24,
          right: 24,
          zIndex: 199,
          background: C.brand,
          color: C.white,
          border: "none",
          borderRadius: 8,
          padding: "10px 18px",
          fontSize: 13,
          fontWeight: 600,
          fontFamily: FONT.body,
          cursor: "pointer",
          boxShadow: SHADOW_LG,
          display: "flex",
          alignItems: "center",
          gap: 8,
        }}
      >
        Report
        {reportSections.length > 0 && (
          <span
            style={{
              background: C.white,
              color: C.brand,
              borderRadius: 10,
              padding: "1px 7px",
              fontSize: 11,
              fontWeight: 700,
              fontFamily: FONT.mono,
              lineHeight: "18px",
              minWidth: 18,
              textAlign: "center",
            }}
          >
            {reportSections.length}
          </span>
        )}
      </button>

      {/* Slide-out panel */}
      {reportBuilderOpen && (
        <div
          style={{
            position: "fixed",
            top: 0,
            right: 0,
            bottom: 0,
            width: 420,
            background: C.white,
            borderLeft: `1px solid ${C.border}`,
            boxShadow: SHADOW_LG,
            zIndex: 200,
            display: "flex",
            flexDirection: "column",
          }}
        >
          {/* Header */}
          <div
            style={{
              padding: "12px 16px",
              borderBottom: `1px solid ${C.border}`,
              display: "flex",
              alignItems: "center",
              justifyContent: "space-between",
            }}
          >
            <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
              <div
                style={{
                  fontSize: 13,
                  fontWeight: 700,
                  color: C.brand,
                  fontFamily: FONT.body,
                }}
              >
                Report Builder
              </div>
              {reportSections.length > 0 && (
                <span
                  style={{
                    background: C.brand,
                    color: C.white,
                    borderRadius: 10,
                    padding: "1px 7px",
                    fontSize: 10,
                    fontWeight: 700,
                    fontFamily: FONT.mono,
                    lineHeight: "16px",
                    minWidth: 16,
                    textAlign: "center",
                  }}
                >
                  {reportSections.length}
                </span>
              )}
            </div>
            <button
              onClick={() => setReportBuilderOpen(false)}
              style={{
                background: "none",
                border: "none",
                cursor: "pointer",
                fontSize: 18,
                color: C.inkLight,
                padding: "4px 8px",
              }}
            >
              x
            </button>
          </div>

          {/* Section list */}
          <div style={{ flex: 1, overflowY: "auto", padding: "12px 16px" }}>
            {reportSections.length === 0 && (
              <div
                style={{
                  fontSize: 12,
                  color: C.inkLight,
                  textAlign: "center",
                  padding: "40px 16px",
                  lineHeight: 1.7,
                  fontFamily: FONT.body,
                }}
              >
                No sections yet. Use "Add to Report" on any Aradune
                response to build your report.
              </div>
            )}

            {reportSections.map((section, idx) => (
              <div
                key={section.id}
                style={{
                  marginBottom: 12,
                  border: `1px solid ${C.border}`,
                  borderRadius: 6,
                  padding: "10px 12px",
                  background: C.white,
                }}
              >
                {/* Top row: drag handle + section number + delete */}
                <div
                  style={{
                    display: "flex",
                    alignItems: "center",
                    justifyContent: "space-between",
                    marginBottom: 6,
                  }}
                >
                  <div
                    style={{
                      display: "flex",
                      alignItems: "center",
                      gap: 8,
                    }}
                  >
                    {/* Drag handle indicator (visual only) */}
                    <span
                      style={{
                        color: C.border,
                        fontSize: 14,
                        cursor: "grab",
                        userSelect: "none",
                        lineHeight: 1,
                      }}
                      title="Reordering coming soon"
                    >
                      &#x2630;
                    </span>
                    <span
                      style={{
                        fontSize: 9,
                        fontWeight: 600,
                        color: C.inkLight,
                        textTransform: "uppercase",
                        letterSpacing: 1,
                        fontFamily: FONT.mono,
                      }}
                    >
                      Section {idx + 1}
                    </span>
                  </div>
                  <button
                    onClick={() => removeReportSection(section.id)}
                    style={{
                      background: "none",
                      border: "none",
                      cursor: "pointer",
                      fontSize: 14,
                      color: C.inkLight,
                      padding: "2px 6px",
                      lineHeight: 1,
                    }}
                    title="Remove section"
                  >
                    x
                  </button>
                </div>

                {/* Prompt */}
                <div
                  style={{
                    fontSize: 12,
                    fontWeight: 600,
                    color: C.ink,
                    fontFamily: FONT.body,
                    marginBottom: 4,
                    lineHeight: 1.4,
                  }}
                >
                  {truncate(section.prompt, 80)}
                </div>

                {/* Response preview */}
                <div
                  style={{
                    fontSize: 11,
                    color: C.inkLight,
                    fontFamily: FONT.body,
                    lineHeight: 1.5,
                    marginBottom: 6,
                  }}
                >
                  {truncate(section.response, 100)}
                </div>

                {/* Timestamp */}
                <div
                  style={{
                    fontSize: 9,
                    color: C.inkLight,
                    fontFamily: FONT.mono,
                  }}
                >
                  {formatTimestamp(section.createdAt)}
                </div>
              </div>
            ))}
          </div>

          {/* Footer actions */}
          {reportSections.length > 0 && (
            <div
              style={{
                padding: "12px 16px",
                borderTop: `1px solid ${C.border}`,
                display: "flex",
                gap: 6,
                flexWrap: "wrap",
                alignItems: "center",
              }}
            >
              {/* Export DOCX */}
              <button
                onClick={async () => {
                  setExporting("docx");
                  try { await exportDOCX(reportSections); } finally { setExporting(null); }
                }}
                disabled={!!exporting}
                style={{
                  background: C.brand,
                  color: C.white,
                  border: "none",
                  borderRadius: 6,
                  padding: "7px 12px",
                  fontSize: 12,
                  fontWeight: 600,
                  cursor: exporting ? "wait" : "pointer",
                  fontFamily: FONT.body,
                  opacity: exporting && exporting !== "docx" ? 0.5 : 1,
                }}
              >
                {exporting === "docx" ? "..." : "DOCX"}
              </button>

              {/* Export PDF */}
              <button
                onClick={async () => {
                  setExporting("pdf");
                  try { await exportPDF(reportSections); } finally { setExporting(null); }
                }}
                disabled={!!exporting}
                style={{
                  background: C.brand,
                  color: C.white,
                  border: "none",
                  borderRadius: 6,
                  padding: "7px 12px",
                  fontSize: 12,
                  fontWeight: 600,
                  cursor: exporting ? "wait" : "pointer",
                  fontFamily: FONT.body,
                  opacity: exporting && exporting !== "pdf" ? 0.5 : 1,
                }}
              >
                {exporting === "pdf" ? "..." : "PDF"}
              </button>

              {/* Export Excel */}
              <button
                onClick={async () => {
                  setExporting("xlsx");
                  try { await exportXLSX(reportSections); } finally { setExporting(null); }
                }}
                disabled={!!exporting}
                style={{
                  background: C.white,
                  color: C.brand,
                  border: `1px solid ${C.brand}`,
                  borderRadius: 6,
                  padding: "7px 12px",
                  fontSize: 12,
                  fontWeight: 600,
                  cursor: exporting ? "wait" : "pointer",
                  fontFamily: FONT.body,
                  opacity: exporting && exporting !== "xlsx" ? 0.5 : 1,
                }}
              >
                {exporting === "xlsx" ? "..." : "Excel"}
              </button>

              {/* Export CSV */}
              <button
                onClick={() => exportCSV(reportSections)}
                disabled={!!exporting}
                style={{
                  background: C.white,
                  color: C.brand,
                  border: `1px solid ${C.brand}`,
                  borderRadius: 6,
                  padding: "7px 12px",
                  fontSize: 12,
                  fontWeight: 600,
                  cursor: exporting ? "wait" : "pointer",
                  fontFamily: FONT.body,
                  opacity: exporting ? 0.5 : 1,
                }}
              >
                CSV
              </button>

              {/* Clear All */}
              <button
                onClick={handleClear}
                style={{
                  background: "none",
                  color: confirmClear ? C.neg : C.inkLight,
                  border: `1px solid ${confirmClear ? C.neg : C.border}`,
                  borderRadius: 6,
                  padding: "7px 12px",
                  fontSize: 12,
                  fontWeight: 600,
                  cursor: "pointer",
                  fontFamily: FONT.body,
                  marginLeft: "auto",
                }}
              >
                {confirmClear ? "Confirm" : "Clear"}
              </button>
            </div>
          )}
        </div>
      )}
    </>
  );
}
