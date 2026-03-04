// ── AHEAD Calculator PDF Report ──────────────────────────────────────
import { createAradunePDF, addSection, addFooter, BRAND, INK, INK_LIGHT } from "./pdfReport";

interface WaterfallStep { n: string; v: number; c: number; }
interface McrResult { py: number; cY: number; wT: number; fin: number; ffs: number; delta: number; pct: number; wf: WaterfallStep[]; [k: string]: any; }
interface McdResult { py: number; cY: number; wT: number; fin: number; ffs: number; delta: number; pct: number; wf: WaterfallStep[]; [k: string]: any; }
interface McYearResult { py: number; cY: number; cp10: number; cp25: number; cp50: number; cp75: number; cp90: number; cffs: number; [k: string]: any; }
interface SensItem { nm: string; lo: number; hi: number; p: string; }
interface HospInfo { nm: string; st: string; beds: number; ty: string; }

const fmt = (n: number): string => {
  const a = Math.abs(n);
  return (n < 0 ? "-" : "") + (a >= 1e9 ? "$" + (a / 1e9).toFixed(2) + "B" : a >= 1e6 ? "$" + (a / 1e6).toFixed(1) + "M" : a >= 1e3 ? "$" + (a / 1e3).toFixed(0) + "K" : "$" + a.toFixed(0));
};
const fP = (n: number): string => (n >= 0 ? "+" : "") + (n * 100).toFixed(2) + "%";

export async function generateAheadPdf(input: {
  hospital: HospInfo;
  mr: McrResult;
  dr: McdResult;
  mp: McrResult[];
  dp: McdResult[];
  mc: McYearResult[];
  sens: SensItem[];
}): Promise<void> {
  const { hospital, mr, dr, mp, dp, mc, sens } = input;

  const doc = await createAradunePDF(`AHEAD Analysis: ${hospital.nm}`);

  // Subtitle
  doc.setFont("helvetica", "normal");
  doc.setFontSize(10);
  doc.setTextColor(...INK_LIGHT);
  doc.text(`${hospital.st} · ${hospital.beds} beds · ${hospital.ty}`, 28, 88);

  let y = 106;

  // ── Page 1: Hospital Info + Medicare HGB ──
  y = addSection(doc, "Medicare Hospital Global Budget", y);

  const mcrWf = mr.wf;
  const wfRows = mcrWf.map(s => [s.n, fmt(s.v), fmt(s.c || s.v)]);

  (doc as any).autoTable({
    startY: y + 4,
    head: [["Step", "Amount", "Cumulative"]],
    body: wfRows,
    margin: { left: 28, right: 28 },
    styles: { fontSize: 9, cellPadding: 5, textColor: INK },
    headStyles: { fillColor: BRAND, textColor: [255, 255, 255], fontStyle: "bold" },
    alternateRowStyles: { fillColor: [245, 247, 245] },
    columnStyles: { 1: { halign: "right" }, 2: { halign: "right" } },
  });

  y = (doc as any).lastAutoTable.finalY + 12;

  // Medicare summary row
  const mcrSummary = [
    ["HGB Final", fmt(mr.fin)],
    ["FFS Counterfactual", fmt(mr.ffs)],
    ["Delta", fmt(mr.delta)],
    ["% Change", fP(mr.pct)],
  ];
  (doc as any).autoTable({
    startY: y,
    body: mcrSummary,
    margin: { left: 28, right: 28 },
    styles: { fontSize: 9, cellPadding: 4, textColor: INK },
    columnStyles: { 0: { fontStyle: "bold", cellWidth: 200 }, 1: { halign: "right" } },
    alternateRowStyles: { fillColor: [245, 247, 245] },
  });

  y = (doc as any).lastAutoTable.finalY + 16;

  // Medicaid HGB summary
  y = addSection(doc, "Medicaid Hospital Global Budget", y);
  const mcdSummary = [
    ["HGB Final", fmt(dr.fin)],
    ["FFS Counterfactual", fmt(dr.ffs)],
    ["Delta", fmt(dr.delta)],
    ["% Change", fP(dr.pct)],
    ["Federal Share", fmt(dr.fSh)],
    ["State Share", fmt(dr.stS)],
  ];
  (doc as any).autoTable({
    startY: y + 4,
    body: mcdSummary,
    margin: { left: 28, right: 28 },
    styles: { fontSize: 9, cellPadding: 4, textColor: INK },
    columnStyles: { 0: { fontStyle: "bold", cellWidth: 200 }, 1: { halign: "right" } },
    alternateRowStyles: { fillColor: [245, 247, 245] },
  });

  // ── Page 2: Multi-year Projections ──
  doc.addPage();
  doc.setFillColor(...BRAND);
  doc.rect(0, 0, 612, 44, "F");
  doc.setFont("helvetica", "bold");
  doc.setFontSize(14);
  doc.setTextColor(255, 255, 255);
  doc.text("ARADUNE", 28, 28);
  doc.setFontSize(9);
  doc.setFont("helvetica", "normal");
  doc.text("Multi-Year Projections", 584, 28, { align: "right" });

  let y2 = 64;
  y2 = addSection(doc, "Monte Carlo Projection Summary", y2);

  const projRows = mc.map((r, i) => [
    `PY${r.py}`,
    fmt(r.cp10),
    fmt(r.cp25),
    fmt(r.cp50),
    fmt(r.cp75),
    fmt(r.cp90),
    fmt(r.cffs),
    fmt(r.cp50 - r.cffs),
  ]);

  (doc as any).autoTable({
    startY: y2 + 4,
    head: [["Year", "P10", "P25", "P50", "P75", "P90", "FFS", "Δ Median"]],
    body: projRows,
    margin: { left: 28, right: 28 },
    styles: { fontSize: 8, cellPadding: 4, textColor: INK },
    headStyles: { fillColor: BRAND, textColor: [255, 255, 255], fontStyle: "bold" },
    alternateRowStyles: { fillColor: [245, 247, 245] },
    columnStyles: {
      1: { halign: "right" }, 2: { halign: "right" }, 3: { halign: "right" },
      4: { halign: "right" }, 5: { halign: "right" }, 6: { halign: "right" },
      7: { halign: "right" },
    },
  });

  y2 = (doc as any).lastAutoTable.finalY + 16;

  // Deterministic projections
  y2 = addSection(doc, "Deterministic Projections", y2);
  const detRows = mp.map((r, i) => [
    `PY${r.py}`,
    fmt(r.fin),
    fmt(dp[i].fin),
    fmt(r.fin + dp[i].fin),
    fmt(r.ffs + dp[i].ffs),
    fmt((r.fin + dp[i].fin) - (r.ffs + dp[i].ffs)),
  ]);

  (doc as any).autoTable({
    startY: y2 + 4,
    head: [["Year", "MCR HGB", "MCD HGB", "Combined", "FFS", "Delta"]],
    body: detRows,
    margin: { left: 28, right: 28 },
    styles: { fontSize: 8, cellPadding: 4, textColor: INK },
    headStyles: { fillColor: BRAND, textColor: [255, 255, 255], fontStyle: "bold" },
    alternateRowStyles: { fillColor: [245, 247, 245] },
    columnStyles: {
      1: { halign: "right" }, 2: { halign: "right" }, 3: { halign: "right" },
      4: { halign: "right" }, 5: { halign: "right" },
    },
  });

  // ── Page 3: Sensitivity Analysis ──
  doc.addPage();
  doc.setFillColor(...BRAND);
  doc.rect(0, 0, 612, 44, "F");
  doc.setFont("helvetica", "bold");
  doc.setFontSize(14);
  doc.setTextColor(255, 255, 255);
  doc.text("ARADUNE", 28, 28);
  doc.setFontSize(9);
  doc.setFont("helvetica", "normal");
  doc.text("Sensitivity Analysis", 584, 28, { align: "right" });

  let y3 = 64;
  y3 = addSection(doc, "Sensitivity Analysis", y3);

  const sensRows = sens.map(s => [
    s.nm,
    s.p,
    fmt(s.lo),
    fmt(s.hi),
    fmt(Math.abs(s.hi - s.lo)),
  ]);

  (doc as any).autoTable({
    startY: y3 + 4,
    head: [["Variable", "Program", "Low Impact", "High Impact", "Range"]],
    body: sensRows,
    margin: { left: 28, right: 28 },
    styles: { fontSize: 9, cellPadding: 5, textColor: INK },
    headStyles: { fillColor: BRAND, textColor: [255, 255, 255], fontStyle: "bold" },
    alternateRowStyles: { fillColor: [245, 247, 245] },
    columnStyles: {
      2: { halign: "right" }, 3: { halign: "right" }, 4: { halign: "right" },
    },
    didParseCell: (data: any) => {
      if (data.section === "body" && (data.column.index === 2 || data.column.index === 3)) {
        const raw = typeof data.cell.raw === "string" ? data.cell.raw : "";
        if (raw.startsWith("-")) data.cell.styles.textColor = [164, 38, 44];
        else if (raw.startsWith("$") && !raw.startsWith("$0")) data.cell.styles.textColor = [46, 107, 74];
      }
    },
  });

  addFooter(doc, ["CMS AHEAD Model Parameters", "T-MSIS", "Medicare Cost Reports"]);
  doc.save(`ahead_report_${hospital.nm.replace(/\s+/g, "_")}_${new Date().toISOString().split("T")[0]}.pdf`);
}
