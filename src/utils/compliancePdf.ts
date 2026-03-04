/**
 * Compliance Report PDF Export
 * Generates a branded PDF compliance package for 42 CFR §447.203.
 */
import { createAradunePDF, addSection, addFooter, BRAND, INK, INK_LIGHT } from "./pdfReport";
import type jsPDF from "jspdf";

interface CheckItem {
  label: string;
  status: "pass" | "warn" | "fail" | "na";
  detail: string;
  regulation?: string;
}

interface CodeRow {
  hcpcs: string;
  desc: string;
  medicaidRate: number;
  medicareRate: number;
  pctMedicare: number;
  totalPaid: number;
  flag: "ok" | "warning" | "critical";
}

interface ReductionRow {
  hcpcs: string;
  desc: string;
  medicaidRate: number;
  newRate: number;
  newPctMed: number;
  impact: number;
}

export interface CompliancePdfInput {
  state: string;
  stateName: string;
  agency: string;
  methodology: string;
  format: string;
  feeScheduleUrl: string;
  totalSpend: number;
  fmap: number;
  checklist: CheckItem[];
  codeAnalysis: CodeRow[];
  medianPctMedicare: number;
  belowWarn: number;
  belowCrit: number;
  reductionPct: number;
  reductionDetails: ReductionRow[];
  reductionTotalImpact: number;
}

const STATUS_LABELS: Record<string, string> = {
  pass: "PASS", warn: "WARNING", fail: "FAIL", na: "N/A",
};
const STATUS_COLORS: Record<string, [number, number, number]> = {
  pass: [46, 107, 74], warn: [184, 134, 11], fail: [164, 38, 44], na: [150, 150, 150],
};

function f$(n: number): string {
  if (n >= 1e9) return `$${(n / 1e9).toFixed(1)}B`;
  if (n >= 1e6) return `$${(n / 1e6).toFixed(1)}M`;
  if (n >= 1e3) return `$${(n / 1e3).toFixed(0)}K`;
  return `$${n.toFixed(2)}`;
}

function checkPage(doc: jsPDF, y: number, need: number): number {
  if (y + need > 740) {
    doc.addPage();
    return 50;
  }
  return y;
}

export async function generateCompliancePdf(input: CompliancePdfInput): Promise<void> {
  const doc = await createAradunePDF(`Rate Transparency Compliance: ${input.stateName}`);
  let y = 90;

  // Subtitle
  doc.setFont("helvetica", "normal");
  doc.setFontSize(10);
  doc.setTextColor(...INK_LIGHT);
  doc.text("42 CFR §447.203 | CMS Ensuring Access Final Rule | Deadline: July 1, 2026", 28, y);
  y += 24;

  // ── Section 1: Compliance Checklist ────────────────────────────────────
  y = addSection(doc, "Compliance Checklist", y);

  (doc as unknown as Record<string, unknown>).autoTable?.call?.(doc) ||
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  (doc as any).autoTable({
    startY: y,
    margin: { left: 28, right: 28 },
    headStyles: { fillColor: BRAND, font: "helvetica", fontStyle: "bold", fontSize: 8 },
    bodyStyles: { font: "helvetica", fontSize: 8, textColor: INK },
    columnStyles: {
      0: { cellWidth: 40, halign: "center", fontStyle: "bold" },
      1: { cellWidth: 200 },
      2: { cellWidth: 220 },
    },
    head: [["Status", "Requirement", "Finding"]],
    body: input.checklist.map(c => [
      { content: STATUS_LABELS[c.status], styles: { textColor: STATUS_COLORS[c.status] || INK } },
      c.label + (c.regulation ? `\n${c.regulation}` : ""),
      c.detail,
    ]),
  });
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  y = (doc as any).lastAutoTable.finalY + 16;

  // ── Section 2: State Overview ──────────────────────────────────────────
  y = checkPage(doc, y, 80);
  y = addSection(doc, "State Overview", y);

  const infoRows = [
    ["State", `${input.state} — ${input.stateName}`],
    ["Agency", input.agency || "—"],
    ["Methodology", input.methodology || "—"],
    ["Fee Schedule Format", input.format || "—"],
    ["Total Medicaid Spend (FFS)", f$(input.totalSpend)],
    ["FMAP", input.fmap > 0 ? `${input.fmap.toFixed(1)}%` : "—"],
  ];
  if (input.feeScheduleUrl) infoRows.push(["Fee Schedule URL", input.feeScheduleUrl]);

  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  (doc as any).autoTable({
    startY: y,
    margin: { left: 28, right: 28 },
    headStyles: { fillColor: [240, 243, 240] as [number, number, number], textColor: INK, font: "helvetica", fontStyle: "bold", fontSize: 8 },
    bodyStyles: { font: "helvetica", fontSize: 8, textColor: INK },
    columnStyles: { 0: { cellWidth: 140, fontStyle: "bold" } },
    head: [["Field", "Value"]],
    body: infoRows,
  });
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  y = (doc as any).lastAutoTable.finalY + 16;

  // ── Section 3: Medicare Parity Analysis ────────────────────────────────
  y = checkPage(doc, y, 60);
  y = addSection(doc, "Medicare Parity Analysis", y);

  // Summary line
  doc.setFont("helvetica", "normal");
  doc.setFontSize(9);
  doc.setTextColor(...INK);
  doc.text(
    `Median: ${input.medianPctMedicare.toFixed(1)}% of Medicare  |  ${input.belowCrit} codes below 50%  |  ${input.belowWarn} codes below 80%  |  ${input.codeAnalysis.length} codes analyzed`,
    28, y + 4,
  );
  y += 16;

  // Top 40 codes by lowest % Medicare
  const sorted = [...input.codeAnalysis].sort((a, b) => a.pctMedicare - b.pctMedicare).slice(0, 40);
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  (doc as any).autoTable({
    startY: y,
    margin: { left: 28, right: 28 },
    headStyles: { fillColor: BRAND, font: "helvetica", fontStyle: "bold", fontSize: 7 },
    bodyStyles: { font: "helvetica", fontSize: 7, textColor: INK },
    columnStyles: {
      0: { cellWidth: 45 },
      1: { cellWidth: 180 },
      2: { halign: "right", cellWidth: 55 },
      3: { halign: "right", cellWidth: 55 },
      4: { halign: "right", cellWidth: 50 },
      5: { halign: "right", cellWidth: 60 },
    },
    head: [["HCPCS", "Description", "Medicaid", "Medicare", "% MCR", "Annual $"]],
    body: sorted.map(c => [
      c.hcpcs,
      c.desc.slice(0, 45),
      `$${c.medicaidRate.toFixed(2)}`,
      `$${c.medicareRate.toFixed(2)}`,
      { content: `${c.pctMedicare.toFixed(1)}%`, styles: { textColor: c.flag === "critical" ? STATUS_COLORS.fail : c.flag === "warning" ? STATUS_COLORS.warn : INK } },
      f$(c.totalPaid),
    ]),
  });
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  y = (doc as any).lastAutoTable.finalY + 16;

  // ── Section 4: Rate Reduction Impact (if applicable) ───────────────────
  if (input.reductionPct > 0 && input.reductionDetails.length > 0) {
    y = checkPage(doc, y, 60);
    y = addSection(doc, `Rate Reduction Impact (${input.reductionPct}%)`, y);

    doc.setFont("helvetica", "normal");
    doc.setFontSize(9);
    doc.setTextColor(...INK);
    const threshold = input.reductionPct >= 6 ? "REQUIRES INDEPENDENT ACCESS ANALYSIS (§447.203(b)(6))"
      : input.reductionPct >= 4 ? "TRIGGERS ACCESS REVIEW (§447.203(b)(5))"
      : "Below review threshold";
    doc.text(`Total Annual Impact: ${f$(input.reductionTotalImpact)}  |  ${threshold}`, 28, y + 4);
    y += 16;

    const topImpact = [...input.reductionDetails].sort((a, b) => b.impact - a.impact).slice(0, 25);
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    (doc as any).autoTable({
      startY: y,
      margin: { left: 28, right: 28 },
      headStyles: { fillColor: BRAND, font: "helvetica", fontStyle: "bold", fontSize: 7 },
      bodyStyles: { font: "helvetica", fontSize: 7, textColor: INK },
      head: [["HCPCS", "Description", "Current", "New Rate", "% MCR After", "Impact"]],
      body: topImpact.map(c => [
        c.hcpcs,
        c.desc.slice(0, 40),
        `$${c.medicaidRate.toFixed(2)}`,
        `$${c.newRate.toFixed(2)}`,
        { content: `${c.newPctMed.toFixed(1)}%`, styles: { textColor: c.newPctMed < 50 ? STATUS_COLORS.fail : c.newPctMed < 80 ? STATUS_COLORS.warn : INK } },
        `-${f$(c.impact)}`,
      ]),
    });
  }

  // Footer
  addFooter(doc, ["CMS T-MSIS (2018-2024)", "Medicare PFS CY2026", "42 CFR §447.203"]);

  doc.save(`compliance_report_${input.state}_${new Date().toISOString().split("T")[0]}.pdf`);
}
