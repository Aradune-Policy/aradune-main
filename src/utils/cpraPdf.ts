/**
 * CPRA PDF Export
 * Generates a branded PDF for the Comparative Payment Rate Analysis.
 */
import { createAradunePDF, addSection, addFooter, BRAND, INK, INK_LIGHT } from "./pdfReport";
import type jsPDF from "jspdf";

interface CpraCodeRow {
  hcpcs: string;
  desc: string;
  category: string;
  categoryLabel: string;
  medicaidRate: number;
  rateSource: "fee_schedule" | "precomputed";
  medicareRate: number | null;
  pctMedicare: number | null;
  claims: number;
  bene: number;
  flag: "pass" | "warn" | "critical" | "na";
}

interface CatSummary {
  id: string;
  label: string;
  totalCodes: number;
  parityCodes: number;
  weightedAvg: number;
  below80: number;
  below50: number;
  totalClaims: number;
  totalBene: number;
}

interface ConvFactorEntry {
  methodology?: string;
  methodology_detail?: string;
  update_frequency?: string;
  gpci_approach?: string;
  fee_schedule_type?: string;
}

export interface CpraPdfInput {
  state: string;
  stateName: string;
  rows: CpraCodeRow[];
  stats: {
    totalCodes: number;
    below80: number;
    below50: number;
    totalClaims: number;
    totalBene: number;
    weightedAvg: number;
    overallStatus: string;
    fsCodes: number;
    precomputedCodes: number;
  };
  catSummary: CatSummary[];
  stateConv?: ConvFactorEntry;
  ffsShare: number | null;
}

const NEG_RGB: [number, number, number] = [164, 38, 44];
const WARN_RGB: [number, number, number] = [184, 134, 11];

function fN(n: number): string {
  if (n >= 1e6) return `${(n / 1e6).toFixed(1)}M`;
  if (n >= 1e3) return `${(n / 1e3).toFixed(1)}K`;
  return String(n);
}

function checkPage(doc: jsPDF, y: number, need: number): number {
  if (y + need > 740) {
    doc.addPage();
    return 50;
  }
  return y;
}

export async function generateCpraPdf(input: CpraPdfInput): Promise<void> {
  const doc = await createAradunePDF(`Comparative Payment Rate Analysis: ${input.stateName}`);
  let y = 90;

  // Subtitle
  doc.setFont("helvetica", "normal");
  doc.setFontSize(10);
  doc.setTextColor(...INK_LIGHT);
  doc.text("42 CFR §447.203 | CMS Ensuring Access Final Rule | Deadline: July 1, 2026", 28, y);
  y += 24;

  // ── Page 1: Summary ─────────────────────────────────────────────────
  y = addSection(doc, "Category Summary", y);

  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  (doc as any).autoTable({
    startY: y,
    margin: { left: 28, right: 28 },
    headStyles: { fillColor: BRAND, font: "helvetica", fontStyle: "bold", fontSize: 8 },
    bodyStyles: { font: "helvetica", fontSize: 8, textColor: INK },
    columnStyles: {
      0: { cellWidth: 90 },
      1: { halign: "right", cellWidth: 50 },
      2: { halign: "right", cellWidth: 70 },
      3: { halign: "right", cellWidth: 60 },
      4: { halign: "right", cellWidth: 60 },
      5: { halign: "right", cellWidth: 60 },
      6: { halign: "right", cellWidth: 60 },
    },
    head: [["Category", "Codes", "Wtd Avg % MCR", "Below 80%", "Below 50%", "Claims", "Bene"]],
    body: input.catSummary.map(c => [
      c.label,
      String(c.totalCodes),
      {
        content: `${c.weightedAvg.toFixed(1)}%`,
        styles: { textColor: c.weightedAvg < 50 ? NEG_RGB : c.weightedAvg < 80 ? WARN_RGB : INK },
      },
      String(c.below80),
      String(c.below50),
      fN(c.totalClaims),
      fN(c.totalBene),
    ]),
    didParseCell: function(data: { section: string; column: { index: number }; cell: { styles: { textColor: [number, number, number] } }; row: { raw: unknown[] } }) {
      if (data.section === "body") {
        if (data.column.index === 3) {
          const val = Number(data.row.raw[3]);
          if (val > 0) data.cell.styles.textColor = WARN_RGB;
        }
        if (data.column.index === 4) {
          const val = Number(data.row.raw[4]);
          if (val > 0) data.cell.styles.textColor = NEG_RGB;
        }
      }
    },
  });
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  y = (doc as any).lastAutoTable.finalY + 16;

  // Overall summary line
  y = checkPage(doc, y, 40);
  doc.setFont("helvetica", "normal");
  doc.setFontSize(9);
  doc.setTextColor(...INK);
  doc.text(
    `Overall: ${input.stats.totalCodes} codes | Wtd avg ${input.stats.weightedAvg.toFixed(1)}% of Medicare | ${input.stats.below80} below 80% | ${input.stats.below50} below 50%`,
    28, y + 4,
  );
  y += 12;
  doc.text(
    `Rate sources: ${input.stats.fsCodes} fee schedule, ${input.stats.precomputedCodes} T-MSIS effective | FFS share: ${input.ffsShare !== null && !isNaN(Number(input.ffsShare)) ? (Number(input.ffsShare) * 100).toFixed(0) + "%" : "N/A"}`,
    28, y + 4,
  );
  y += 20;

  // ── Per-category detail tables ──────────────────────────────────────
  const categories = [
    { id: "primary_care", label: "Primary Care" },
    { id: "obgyn", label: "OB/GYN" },
    { id: "mh_sud", label: "MH/SUD" },
  ];

  for (const cat of categories) {
    const catRows = input.rows
      .filter(r => r.category === cat.id)
      .sort((a, b) => (a.pctMedicare ?? 999) - (b.pctMedicare ?? 999));

    if (catRows.length === 0) continue;

    y = checkPage(doc, y, 60);
    y = addSection(doc, cat.label, y);

    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    (doc as any).autoTable({
      startY: y,
      margin: { left: 28, right: 28 },
      headStyles: { fillColor: BRAND, font: "helvetica", fontStyle: "bold", fontSize: 7 },
      bodyStyles: { font: "helvetica", fontSize: 7, textColor: INK },
      columnStyles: {
        0: { cellWidth: 40 },
        1: { cellWidth: 140 },
        2: { halign: "right", cellWidth: 55 },
        3: { halign: "center", cellWidth: 40 },
        4: { halign: "right", cellWidth: 55 },
        5: { halign: "right", cellWidth: 50 },
        6: { halign: "right", cellWidth: 45 },
        7: { halign: "right", cellWidth: 45 },
      },
      head: [["HCPCS", "Description", "Medicaid", "Source", "Medicare", "% MCR", "Claims", "Bene"]],
      body: catRows.map(r => [
        r.hcpcs,
        r.desc.slice(0, 40),
        `$${r.medicaidRate.toFixed(2)}`,
        r.rateSource === "fee_schedule" ? "FS" : "T-MSIS",
        r.medicareRate !== null ? `$${r.medicareRate.toFixed(2)}` : "N/A",
        r.pctMedicare !== null
          ? {
              content: `${r.pctMedicare.toFixed(1)}%`,
              styles: { textColor: r.flag === "critical" ? NEG_RGB : r.flag === "warn" ? WARN_RGB : INK },
            }
          : "N/A",
        r.claims > 0 ? fN(r.claims) : "-",
        r.bene > 0 ? fN(r.bene) : "-",
      ]),
    });
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    y = (doc as any).lastAutoTable.finalY + 16;
  }

  // ── Data notes page ─────────────────────────────────────────────────
  y = checkPage(doc, y, 200);
  y = addSection(doc, "Data Notes and Methodology", y);

  const notes = [
    `Service year: 2023 T-MSIS (FFS claims only)`,
    `FFS share: ${input.ffsShare !== null && !isNaN(Number(input.ffsShare)) ? (Number(input.ffsShare) * 100).toFixed(0) + "% of Medicaid in " + input.state + " is FFS" : "FFS share unavailable"}`,
    `Rate sources: ${input.stats.fsCodes} codes from state fee schedule, ${input.stats.precomputedCodes} from T-MSIS effective rates`,
    `Medicare benchmark: CY2026 PFS national non-facility rates (locality adjustment pending)`,
    `Beneficiary counts represent patient-service events, not unique individuals`,
    `H-codes (behavioral health) excluded from parity calculations; no Medicare equivalent`,
    ``,
    `State methodology: ${input.stateConv?.methodology_detail || input.stateConv?.methodology || "Not available"}`,
    `Update frequency: ${input.stateConv?.update_frequency || "Not available"}`,
    ``,
    `Regulatory reference: 42 CFR §447.203 -- Ensuring Access to Medicaid Services Final Rule`,
    `Compliance deadline: July 1, 2026`,
  ];

  doc.setFont("helvetica", "normal");
  doc.setFontSize(8);
  doc.setTextColor(...INK);
  for (const line of notes) {
    if (y > 740) { doc.addPage(); y = 50; }
    doc.text(line, 28, y);
    y += 12;
  }

  // Footer
  addFooter(doc, ["CMS T-MSIS (2018-2024)", "Medicare PFS CY2026", "State fee schedules", "42 CFR §447.203"]);

  doc.save(`cpra_${input.state}_${new Date().toISOString().split("T")[0]}.pdf`);
}
