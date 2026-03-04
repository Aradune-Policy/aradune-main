// ── CCBHC Rate Development Analysis PDF Report ──────────────────────
import { createAradunePDF, addSection, addFooter, BRAND, INK, INK_LIGHT } from "./pdfReport";
import type { CcbhcAnalysisResult } from "../lib/ccbhcAnalysis";
import { MILLIMAN_ESTIMATES, SAMHSA_CATEGORY_NAMES } from "../lib/ccbhcAnalysis";

const fmt = (n: number): string => {
  if (n == null || isNaN(n) || !isFinite(n)) return "$0";
  const a = Math.abs(n);
  const s = n < 0 ? "-" : "";
  if (a >= 1e9) return `${s}$${(a / 1e9).toFixed(1)}B`;
  if (a >= 1e6) return `${s}$${(a / 1e6).toFixed(1)}M`;
  if (a >= 1e3) return `${s}$${(a / 1e3).toFixed(0)}K`;
  if (a < 10) return `${s}$${a.toFixed(2)}`;
  return `${s}$${a.toFixed(0)}`;
};
const fNu = (v: number): string => {
  if (v >= 1e6) return `${(v / 1e6).toFixed(1)}M`;
  if (v >= 1e3) return `${(v / 1e3).toFixed(0)}K`;
  return `${v}`;
};

const NEG: [number, number, number] = [164, 38, 44];
const POS: [number, number, number] = [46, 107, 74];

function addPageHeader(doc: any, subtitle: string) {
  doc.setFillColor(...BRAND);
  doc.rect(0, 0, 612, 44, "F");
  doc.setFont("helvetica", "bold");
  doc.setFontSize(14);
  doc.setTextColor(255, 255, 255);
  doc.text("ARADUNE", 28, 28);
  doc.setFontSize(9);
  doc.setFont("helvetica", "normal");
  doc.text(subtitle, 584, 28, { align: "right" });
}

function checkPage(doc: any, y: number, need: number): number {
  if (y + need > 730) {
    doc.addPage();
    addPageHeader(doc, "CCBHC Analysis (cont.)");
    return 64;
  }
  return y;
}

export async function generateCcbhcPdf(r: CcbhcAnalysisResult): Promise<void> {
  const doc = await createAradunePDF(`CCBHC Rate Development: ${r.state}`);

  // Subtitle
  doc.setFont("helvetica", "normal");
  doc.setFontSize(10);
  doc.setTextColor(...INK_LIGHT);
  doc.text(`SPA FL-25-0007 · ${r.utilization.length} CCBHC codes · ${r.providers.length} taxonomy providers`, 28, 88);
  doc.text(`Analysis run: ${new Date(r.run_at).toLocaleDateString()}`, 28, 100);

  let y = 118;

  // ── Section 1: Status Quo Summary ──
  y = addSection(doc, "Status Quo Spending", y);
  (doc as any).autoTable({
    startY: y + 4,
    body: [
      ["Total CCBHC Spending", fmt(r.status_quo.grand_total_paid)],
      ["Core Services", fmt(r.status_quo.core_total_paid)],
      ["Expanded Services", fmt(r.status_quo.expanded_total_paid)],
      ["Total Claims", fNu(r.status_quo.grand_total_claims)],
      ["Total Beneficiaries", fNu(r.status_quo.grand_total_beneficiaries)],
      ["Milliman Range", `${fmt(MILLIMAN_ESTIMATES.status_quo_low)} – ${fmt(MILLIMAN_ESTIMATES.status_quo_high)}`],
      ["LBR Appropriation", fmt(MILLIMAN_ESTIMATES.lbr_appropriation)],
      ["Net New Spending", fmt(r.status_quo.net_new_spending)],
    ],
    margin: { left: 28, right: 28 },
    styles: { fontSize: 9, cellPadding: 4, textColor: INK },
    columnStyles: { 0: { fontStyle: "bold", cellWidth: 200 }, 1: { halign: "right" } },
    alternateRowStyles: { fillColor: [245, 247, 245] },
  });
  y = (doc as any).lastAutoTable.finalY + 16;

  // ── Section 2: Spending by SAMHSA Category ──
  y = checkPage(doc, y, 120);
  y = addSection(doc, "Spending by SAMHSA Category", y);
  (doc as any).autoTable({
    startY: y + 4,
    head: [["Category", "Total Paid", "Claims", "Benes"]],
    body: r.status_quo.by_category.map(c => [c.category, fmt(c.total_paid), fNu(c.total_claims), fNu(c.total_beneficiaries)]),
    margin: { left: 28, right: 28 },
    styles: { fontSize: 8, cellPadding: 4, textColor: INK },
    headStyles: { fillColor: BRAND, textColor: [255, 255, 255], fontStyle: "bold" },
    alternateRowStyles: { fillColor: [245, 247, 245] },
    columnStyles: { 1: { halign: "right" }, 2: { halign: "right" }, 3: { halign: "right" } },
  });
  y = (doc as any).lastAutoTable.finalY + 16;

  // ── Section 3: Refined Rate Estimates ──
  if (r.refined_rates?.length && r.provider_totals) {
    y = checkPage(doc, y, 140);
    y = addSection(doc, `Refined Rate Estimates (${r.provider_totals.provider_count} providers)`, y);

    doc.setFont("helvetica", "normal");
    doc.setFontSize(8);
    doc.setTextColor(...INK_LIGHT);
    doc.text(`Annualized: ${fmt(r.provider_totals.annualized_paid)}/yr paid, ${fNu(r.provider_totals.annualized_claims)}/yr claims (${r.provider_totals.years_in_data}yr avg)`, 28, y + 4);
    y += 14;

    (doc as any).autoTable({
      startY: y,
      head: [["Scenario", "Numerator", "Per Claim", "SQ /Claim", "Increment"]],
      body: r.refined_rates.map(rr => [rr.label, fmt(rr.numerator), fmt(rr.per_claim), fmt(rr.status_quo_per_claim), `+${fmt(rr.increment)}`]),
      margin: { left: 28, right: 28 },
      styles: { fontSize: 9, cellPadding: 5, textColor: INK },
      headStyles: { fillColor: BRAND, textColor: [255, 255, 255], fontStyle: "bold" },
      alternateRowStyles: { fillColor: [245, 247, 245] },
      columnStyles: { 1: { halign: "right" }, 2: { halign: "right" }, 3: { halign: "right" }, 4: { halign: "right" } },
      didParseCell: (data: any) => {
        if (data.section === "body" && data.column.index === 4) {
          data.cell.styles.textColor = POS;
          data.cell.styles.fontStyle = "bold";
        }
        if (data.section === "body" && data.column.index === 2) {
          data.cell.styles.fontStyle = "bold";
        }
      },
    });
    y = (doc as any).lastAutoTable.finalY + 8;
    doc.setFontSize(7);
    doc.setTextColor(...INK_LIGHT);
    doc.text("Per-claim estimates. After daily-visit dedup (1.3-1.8 claims/visit), PPS daily rates would be higher.", 28, y);
    y += 16;
  }

  // ── Page 2: Provider & Geographic Analysis ──
  doc.addPage();
  addPageHeader(doc, "Provider & Geographic Analysis");
  y = 64;

  // Provider-scoped peer comparison
  if (r.provider_benchmarks?.length) {
    y = addSection(doc, "Peer State CCBHC Provider Comparison (Taxonomy-Scoped)", y);
    (doc as any).autoTable({
      startY: y + 4,
      head: [["State", "Providers", "Total Paid", "Per Provider", "Per Claim"]],
      body: r.provider_benchmarks.map(pb => [pb.state, pb.provider_count, fmt(pb.total_paid), fmt(pb.per_provider), fmt(pb.per_claim)]),
      margin: { left: 28, right: 28 },
      styles: { fontSize: 9, cellPadding: 5, textColor: INK },
      headStyles: { fillColor: BRAND, textColor: [255, 255, 255], fontStyle: "bold" },
      alternateRowStyles: { fillColor: [245, 247, 245] },
      columnStyles: { 1: { halign: "right" }, 2: { halign: "right" }, 3: { halign: "right" }, 4: { halign: "right" } },
      didParseCell: (data: any) => {
        if (data.section === "body") {
          const stateVal = r.provider_benchmarks![data.row.index]?.state;
          if (stateVal === r.state) {
            data.cell.styles.fontStyle = "bold";
          }
        }
      },
    });
    y = (doc as any).lastAutoTable.finalY + 16;
  }

  // Geographic distribution
  if (r.geography?.length) {
    y = checkPage(doc, y, 180);
    y = addSection(doc, "Geographic Distribution (ZIP3)", y);
    const deserts = r.geography.filter(g => g.is_desert);
    const top15 = r.geography.slice(0, 15);

    if (deserts.length > 0) {
      doc.setFont("helvetica", "bold");
      doc.setFontSize(8);
      doc.setTextColor(...NEG);
      doc.text(`CCBHC Deserts (${deserts.length}): ${deserts.map(d => `ZIP3 ${d.zip3}`).join(", ")}`, 28, y + 4);
      y += 14;
    }

    (doc as any).autoTable({
      startY: y,
      head: [["ZIP3", "CCBHC Provs", "Total Provs", "CCBHC %", "CCBHC Paid", "Status"]],
      body: top15.map(g => [
        g.zip3,
        g.ccbhc_providers,
        g.total_providers,
        g.total_providers > 0 ? ((g.ccbhc_providers / g.total_providers) * 100).toFixed(1) + "%" : "—",
        g.ccbhc_paid > 0 ? fmt(g.ccbhc_paid) : "—",
        g.is_desert ? "DESERT" : g.ccbhc_providers <= 3 ? "Thin" : g.ccbhc_providers >= 10 ? "High" : "",
      ]),
      margin: { left: 28, right: 28 },
      styles: { fontSize: 8, cellPadding: 4, textColor: INK },
      headStyles: { fillColor: BRAND, textColor: [255, 255, 255], fontStyle: "bold" },
      alternateRowStyles: { fillColor: [245, 247, 245] },
      columnStyles: { 1: { halign: "right" }, 2: { halign: "right" }, 3: { halign: "right" }, 4: { halign: "right" } },
      didParseCell: (data: any) => {
        if (data.section === "body" && data.column.index === 5) {
          const val = String(data.cell.raw);
          if (val === "DESERT") data.cell.styles.textColor = NEG;
          else if (val === "High") data.cell.styles.textColor = POS;
        }
      },
    });
    y = (doc as any).lastAutoTable.finalY + 16;
  }

  // Top providers table
  if (r.providers.length > 0) {
    y = checkPage(doc, y, 200);
    y = addSection(doc, `Top Providers (${r.providers.length} total)`, y);
    const top20 = r.providers.slice(0, 20);
    (doc as any).autoTable({
      startY: y + 4,
      head: [["NPI", "Provider Name", "ZIP3", "Total Paid", "Claims"]],
      body: top20.map(p => [p.npi, p.provider_name.slice(0, 40), p.zip3, fmt(p.total_paid), fNu(p.total_claims)]),
      margin: { left: 28, right: 28 },
      styles: { fontSize: 7, cellPadding: 3, textColor: INK },
      headStyles: { fillColor: BRAND, textColor: [255, 255, 255], fontStyle: "bold" },
      alternateRowStyles: { fillColor: [245, 247, 245] },
      columnStyles: { 3: { halign: "right" }, 4: { halign: "right" } },
    });
    y = (doc as any).lastAutoTable.finalY + 16;
  }

  // ── Page 3: Service Utilization & Quality ──
  doc.addPage();
  addPageHeader(doc, "Service Utilization & Quality");
  y = 64;

  // Service utilization (top codes)
  y = addSection(doc, "Service Utilization (Top 20 Codes)", y);
  const activeCodes = r.utilization.filter(u => u.total_claims > 0).sort((a, b) => b.total_paid - a.total_paid).slice(0, 20);
  (doc as any).autoTable({
    startY: y + 4,
    head: [["HCPCS", "Description", "Scope", "Claims", "Paid", "Avg Rate"]],
    body: activeCodes.map(u => [u.hcpcs_code, u.description.slice(0, 35), u.scope, fNu(u.total_claims), fmt(u.total_paid), fmt(u.avg_rate)]),
    margin: { left: 28, right: 28 },
    styles: { fontSize: 7, cellPadding: 3, textColor: INK },
    headStyles: { fillColor: BRAND, textColor: [255, 255, 255], fontStyle: "bold" },
    alternateRowStyles: { fillColor: [245, 247, 245] },
    columnStyles: { 3: { halign: "right" }, 4: { halign: "right" }, 5: { halign: "right" } },
  });
  y = (doc as any).lastAutoTable.finalY + 6;
  const zeroCodes = r.utilization.filter(u => u.total_claims === 0);
  if (zeroCodes.length > 0) {
    doc.setFont("helvetica", "normal");
    doc.setFontSize(8);
    doc.setTextColor(...INK_LIGHT);
    doc.text(`${zeroCodes.length} codes with zero claims: ${zeroCodes.map(z => z.hcpcs_code).join(", ")}`, 28, y + 4);
    y += 14;
  }
  y += 8;

  // Quality gaps
  if (r.enhanced?.quality_gaps.length) {
    y = checkPage(doc, y, 160);
    y = addSection(doc, "Quality Gap Analysis (CMS Core Set)", y);
    const gaps = [...r.enhanced.quality_gaps].sort((a, b) => a.gap - b.gap);
    (doc as any).autoTable({
      startY: y + 4,
      head: [["Measure", "FL Rate", "Median", "Gap"]],
      body: gaps.map(q => [q.name, `${q.fl_rate}%`, `${q.median}%`, `${q.gap >= 0 ? "+" : ""}${q.gap.toFixed(1)}pp`]),
      margin: { left: 28, right: 28 },
      styles: { fontSize: 8, cellPadding: 4, textColor: INK },
      headStyles: { fillColor: BRAND, textColor: [255, 255, 255], fontStyle: "bold" },
      alternateRowStyles: { fillColor: [245, 247, 245] },
      columnStyles: { 1: { halign: "right" }, 2: { halign: "right" }, 3: { halign: "right" } },
      didParseCell: (data: any) => {
        if (data.section === "body" && data.column.index === 3) {
          const raw = String(data.cell.raw);
          if (raw.startsWith("-")) data.cell.styles.textColor = NEG;
          else if (raw.startsWith("+")) data.cell.styles.textColor = POS;
        }
      },
    });
    y = (doc as any).lastAutoTable.finalY + 16;
  }

  // Trends (yearly)
  if (r.trends.length > 1) {
    y = checkPage(doc, y, 140);
    y = addSection(doc, "Year-over-Year Trends", y);
    (doc as any).autoTable({
      startY: y + 4,
      head: [["Year", "Total Paid", "Claims", "Benes", "YoY Growth"]],
      body: r.trends.map(t => [t.year, fmt(t.total_paid), fNu(t.total_claims), fNu(t.total_beneficiaries), t.yoy_growth != null ? `${t.yoy_growth >= 0 ? "+" : ""}${t.yoy_growth.toFixed(1)}%` : "—"]),
      margin: { left: 28, right: 28 },
      styles: { fontSize: 8, cellPadding: 4, textColor: INK },
      headStyles: { fillColor: BRAND, textColor: [255, 255, 255], fontStyle: "bold" },
      alternateRowStyles: { fillColor: [245, 247, 245] },
      columnStyles: { 1: { halign: "right" }, 2: { halign: "right" }, 3: { halign: "right" }, 4: { halign: "right" } },
      didParseCell: (data: any) => {
        if (data.section === "body" && data.column.index === 4) {
          const raw = String(data.cell.raw);
          if (raw.startsWith("-")) data.cell.styles.textColor = NEG;
          else if (raw.startsWith("+")) data.cell.styles.textColor = POS;
        }
      },
    });
    y = (doc as any).lastAutoTable.finalY + 16;
  }

  // ── Page 4: Data Limitations ──
  doc.addPage();
  addPageHeader(doc, "Data Limitations & Next Steps");
  y = 64;

  y = addSection(doc, "Data Limitations", y);
  doc.setFont("helvetica", "normal");
  doc.setFontSize(9);
  doc.setTextColor(...INK);

  const limitations = [
    "ILLUSTRATIVE ONLY. Rate estimates use aggregate T-MSIS claim counts as proxy.",
    "Actual PPS rate requires beneficiary x date daily visit deduplication from claim-level data.",
    "T-MSIS data is FFS-adjudicated only (FL is 77% managed care).",
    "No beneficiary demographics, diagnoses, or eligibility-level data.",
    "No billing vs rendering NPI distinction for DCO validation.",
    "Telehealth identification limited to telehealth-specific HCPCS codes only.",
    "",
    "To complete the full analysis, request:",
    "  1. Claim-level T-MSIS extract with beneficiary IDs, service dates, modifiers, POS",
    "  2. Milliman provider-level cost survey microdata",
    "  3. SAMHSA CCBHC grantee list for provider cross-reference",
  ];

  for (const line of limitations) {
    doc.text(line, 28, y + 4);
    y += 12;
  }

  addFooter(doc, ["T-MSIS (2018-2024)", "Milliman/AHCA Appendix I", "CMS Medicaid Core Set", "BLS OES"]);
  doc.save(`ccbhc_analysis_${r.state}_${new Date().toISOString().split("T")[0]}.pdf`);
}
