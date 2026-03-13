/**
 * PDF Report Export
 * Generates a branded PDF from accumulated report sections.
 * Uses existing pdfReport.ts utilities (jsPDF + autoTable).
 */

import { createAradunePDF, addSection, addFooter, BRAND, INK, INK_LIGHT } from "./pdfReport";
import { parseMarkdown, extractAllTables } from "./reportMarkdown";
import type { Block } from "./reportMarkdown";
import type jsPDF from "jspdf";

interface ReportSection {
  id: string;
  prompt: string;
  response: string;
  queries: string[];
  createdAt: Date;
}

const PAGE_W = 612;
const MARGIN_L = 28;
const MARGIN_R = 584;
const CONTENT_W = MARGIN_R - MARGIN_L;
const PAGE_BOTTOM = 740; // leave room for footer

function ensureSpace(doc: jsPDF, y: number, needed: number): number {
  if (y + needed > PAGE_BOTTOM) {
    doc.addPage();
    return 56; // top margin on new page
  }
  return y;
}

function renderBlocks(doc: jsPDF, blocks: Block[], startY: number): number {
  let y = startY;

  for (const block of blocks) {
    switch (block.type) {
      case "heading": {
        y = ensureSpace(doc, y, 28);
        const fontSize = block.level === 1 ? 14 : block.level === 2 ? 12 : 11;
        doc.setFont("helvetica", "bold");
        doc.setFontSize(fontSize);
        doc.setTextColor(...BRAND);
        doc.text(block.text, MARGIN_L, y + fontSize);
        y += fontSize + 10;
        break;
      }

      case "paragraph": {
        doc.setFont("helvetica", "normal");
        doc.setFontSize(9.5);
        doc.setTextColor(...INK);
        // Combine runs into text, handle bold segments via splitTextToSize
        const fullText = block.runs.map((r) => r.text).join("");
        const lines: string[] = doc.splitTextToSize(fullText, CONTENT_W);
        for (const line of lines) {
          y = ensureSpace(doc, y, 13);
          doc.text(line, MARGIN_L, y);
          y += 13;
        }
        y += 4;
        break;
      }

      case "table": {
        y = ensureSpace(doc, y, 40);
        const tableBody = block.rows.map((row) =>
          row.map((cell) => cell)
        );
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        (doc as any).autoTable({
          startY: y,
          head: [block.columns],
          body: tableBody,
          margin: { left: MARGIN_L, right: PAGE_W - MARGIN_R },
          styles: {
            font: "helvetica",
            fontSize: 8,
            cellPadding: 3,
            textColor: [10, 37, 64],
            lineColor: [228, 234, 228],
            lineWidth: 0.5,
          },
          headStyles: {
            fillColor: [46, 107, 74],
            textColor: [255, 255, 255],
            fontStyle: "bold",
            fontSize: 8,
          },
          alternateRowStyles: {
            fillColor: [245, 247, 245],
          },
          didDrawPage: () => {
            // Reset y after page break within table
          },
        });
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        y = (doc as any).lastAutoTable?.finalY ?? y + 20;
        y += 10;
        break;
      }

      case "list": {
        doc.setFont("helvetica", "normal");
        doc.setFontSize(9.5);
        doc.setTextColor(...INK);
        for (let i = 0; i < block.items.length; i++) {
          const bullet = block.ordered ? `${i + 1}. ` : "- ";
          const itemText = bullet + block.items[i];
          const lines: string[] = doc.splitTextToSize(itemText, CONTENT_W - 12);
          for (const line of lines) {
            y = ensureSpace(doc, y, 13);
            doc.text(line, MARGIN_L + 12, y);
            y += 13;
          }
        }
        y += 4;
        break;
      }

      case "hr": {
        y = ensureSpace(doc, y, 16);
        doc.setDrawColor(228, 234, 228);
        doc.line(MARGIN_L, y, MARGIN_R, y);
        y += 16;
        break;
      }

      case "code": {
        y = ensureSpace(doc, y, 30);
        doc.setFont("courier", "normal");
        doc.setFontSize(8);
        doc.setTextColor(...INK);
        const codeLines: string[] = doc.splitTextToSize(block.text, CONTENT_W - 16);
        // Background rect
        const codeHeight = codeLines.length * 11 + 8;
        y = ensureSpace(doc, y, codeHeight);
        doc.setFillColor(245, 247, 245);
        doc.rect(MARGIN_L, y - 4, CONTENT_W, codeHeight, "F");
        for (const line of codeLines) {
          doc.text(line, MARGIN_L + 8, y + 6);
          y += 11;
        }
        y += 8;
        doc.setFont("helvetica", "normal");
        break;
      }
    }
  }

  return y;
}

export async function generateReportPdf(
  sections: ReportSection[],
  title?: string
): Promise<void> {
  const reportTitle = title || "Medicaid Intelligence Report";
  const doc = await createAradunePDF(reportTitle);

  // Collect data sources from queries
  const allQueries = sections.flatMap((s) => s.queries || []);
  const tableNames = [
    ...new Set(
      allQueries
        .join(" ")
        .match(/(?:FROM|JOIN)\s+(fact_\w+|dim_\w+|ref_\w+)/gi)
        ?.map((m) => m.replace(/^(?:FROM|JOIN)\s+/i, "")) || []
    ),
  ];

  // Subtitle: section count + date
  let y = 88;
  doc.setFont("helvetica", "normal");
  doc.setFontSize(10);
  doc.setTextColor(...INK_LIGHT);
  doc.text(
    `${sections.length} analysis section${sections.length !== 1 ? "s" : ""} | ${new Date().toLocaleDateString("en-US", { year: "numeric", month: "long", day: "numeric" })}`,
    MARGIN_L,
    y
  );
  y += 24;

  // Data sources on cover
  if (tableNames.length > 0) {
    doc.setFont("helvetica", "bold");
    doc.setFontSize(9);
    doc.setTextColor(...BRAND);
    doc.text("Data Sources:", MARGIN_L, y);
    y += 12;
    doc.setFont("helvetica", "normal");
    doc.setFontSize(8);
    doc.setTextColor(...INK_LIGHT);
    const sourceLines: string[] = doc.splitTextToSize(tableNames.join(", "), CONTENT_W);
    for (const line of sourceLines) {
      doc.text(line, MARGIN_L, y);
      y += 10;
    }
    y += 12;
  }

  // ── Render each section ──────────────────────────────────────────────

  for (let idx = 0; idx < sections.length; idx++) {
    const section = sections[idx];

    // Section divider
    y = ensureSpace(doc, y, 60);
    doc.setDrawColor(228, 234, 228);
    doc.line(MARGIN_L, y, MARGIN_R, y);
    y += 16;

    // Section label
    doc.setFont("helvetica", "bold");
    doc.setFontSize(8);
    doc.setTextColor(...INK_LIGHT);
    doc.text(`SECTION ${idx + 1}`, MARGIN_L, y);
    y += 14;

    // User prompt (shaded box)
    doc.setFont("helvetica", "italic");
    doc.setFontSize(10);
    doc.setTextColor(...INK);
    const promptLines: string[] = doc.splitTextToSize(section.prompt, CONTENT_W - 16);
    const promptHeight = promptLines.length * 14 + 8;
    y = ensureSpace(doc, y, promptHeight);
    doc.setFillColor(245, 247, 245);
    doc.rect(MARGIN_L, y - 6, CONTENT_W, promptHeight, "F");
    for (const line of promptLines) {
      doc.text(line, MARGIN_L + 8, y + 4);
      y += 14;
    }
    y += 12;

    // Parse and render response
    const blocks = parseMarkdown(section.response);
    y = renderBlocks(doc, blocks, y);
  }

  // Footer on all pages
  addFooter(doc, tableNames.length > 0 ? tableNames : ["Aradune Data Lake"]);

  doc.save(`aradune-report-${new Date().toISOString().slice(0, 10)}.pdf`);
}
