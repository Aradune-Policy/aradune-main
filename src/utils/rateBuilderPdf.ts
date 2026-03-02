// ── Rate Builder PDF Report ──────────────────────────────────────────
import type { RateResult, RateComponent } from "../types";
import { createAradunePDF, addSection, addFooter, BRAND, INK, INK_LIGHT } from "./pdfReport";

interface StateRate { st: string; rate: number; name: string; }

interface RateBuilderPdfInput {
  code: string;
  desc: string | null;
  medicareRate: number | null;
  methodology: string;
  formula: string;
  components: RateComponent[];
  rate: number;
  stateRates: StateRate[];
  nStates: number;
}

export async function generateRateBuilderPdf(input: RateBuilderPdfInput): Promise<void> {
  const { code, desc, medicareRate, methodology, formula, components, rate, stateRates, nStates } = input;

  const doc = await createAradunePDF(`Rate Calculation — ${code}`);

  // Subtitle / description
  if (desc) {
    doc.setFont("helvetica", "normal");
    doc.setFontSize(10);
    doc.setTextColor(...INK_LIGHT);
    doc.text(desc, 28, 88);
  }

  let y = desc ? 106 : 92;

  // ── Page 1: Methodology & Components ──
  y = addSection(doc, "Methodology", y);
  doc.setFont("helvetica", "normal");
  doc.setFontSize(10);
  doc.setTextColor(...INK);
  doc.text(`Method: ${methodology}`, 38, y + 4);
  y += 16;
  doc.text(`Formula: ${formula}`, 38, y + 4);
  y += 20;

  // Component breakdown table
  y = addSection(doc, "Component Breakdown", y);
  const compRows = components.map((c: RateComponent) => [
    c.label,
    c.value,
    c.note || "",
  ]);

  (doc as any).autoTable({
    startY: y + 4,
    head: [["Component", "Value", "Note"]],
    body: compRows,
    margin: { left: 28, right: 28 },
    styles: { fontSize: 9, cellPadding: 5, textColor: INK },
    headStyles: { fillColor: BRAND, textColor: [255, 255, 255], fontStyle: "bold" },
    alternateRowStyles: { fillColor: [245, 247, 245] },
    columnStyles: {
      1: { fontStyle: "bold", halign: "right" },
    },
  });

  y = (doc as any).lastAutoTable.finalY + 16;

  // Medicare comparison
  y = addSection(doc, "Medicare Comparison", y);
  const mcrData = [
    ["Calculated Rate", `$${rate.toFixed(2)}`],
    ["Medicare CY2025", medicareRate ? `$${medicareRate.toFixed(2)}` : "N/A"],
    ["% of Medicare", medicareRate ? `${(rate / medicareRate * 100).toFixed(1)}%` : "N/A"],
    ["States with T-MSIS data", String(nStates)],
  ];
  if (stateRates.length > 0) {
    const median = stateRates[Math.floor(stateRates.length / 2)].rate;
    mcrData.push(["T-MSIS Median", `$${median.toFixed(2)}`]);
  }

  (doc as any).autoTable({
    startY: y + 4,
    body: mcrData,
    margin: { left: 28, right: 28 },
    styles: { fontSize: 9, cellPadding: 5, textColor: INK },
    columnStyles: {
      0: { fontStyle: "bold", cellWidth: 200 },
      1: { halign: "right" },
    },
    alternateRowStyles: { fillColor: [245, 247, 245] },
  });

  // ── Page 2: State Comparison Table ──
  if (stateRates.length > 0) {
    doc.addPage();

    // Re-add header bar on page 2
    doc.setFillColor(...BRAND);
    doc.rect(0, 0, 612, 44, "F");
    doc.setFont("helvetica", "bold");
    doc.setFontSize(14);
    doc.setTextColor(255, 255, 255);
    doc.text("ARADUNE", 28, 28);
    doc.setFontSize(9);
    doc.setFont("helvetica", "normal");
    doc.text(`${code} State Comparison`, 584, 28, { align: "right" });

    let y2 = 64;
    y2 = addSection(doc, `State Rates — ${code}`, y2);

    const stateRows = stateRates.map(s => {
      const diff = rate - s.rate;
      const pctChg = ((rate / s.rate) - 1) * 100;
      return [
        s.name,
        `$${s.rate.toFixed(2)}`,
        `$${rate.toFixed(2)}`,
        `${diff >= 0 ? "+" : ""}$${diff.toFixed(2)}`,
        `${pctChg >= 0 ? "+" : ""}${pctChg.toFixed(1)}%`,
      ];
    });

    (doc as any).autoTable({
      startY: y2 + 4,
      head: [["State", "T-MSIS Rate", "Calculated Rate", "Change", "Change %"]],
      body: stateRows,
      margin: { left: 28, right: 28 },
      styles: { fontSize: 8, cellPadding: 4, textColor: INK },
      headStyles: { fillColor: BRAND, textColor: [255, 255, 255], fontStyle: "bold" },
      alternateRowStyles: { fillColor: [245, 247, 245] },
      columnStyles: {
        1: { halign: "right" },
        2: { halign: "right", fontStyle: "bold" },
        3: { halign: "right" },
        4: { halign: "right" },
      },
      didParseCell: (data: any) => {
        if (data.section === "body" && (data.column.index === 3 || data.column.index === 4)) {
          const raw = String(data.cell.raw);
          if (raw.startsWith("+")) data.cell.styles.textColor = [46, 107, 74];
          else if (raw.startsWith("-")) data.cell.styles.textColor = [164, 38, 44];
        }
      },
    });
  }

  addFooter(doc, ["CMS T-MSIS", "Medicare PFS CY2025"]);
  doc.save(`rate_report_${code}_${new Date().toISOString().split("T")[0]}.pdf`);
}
