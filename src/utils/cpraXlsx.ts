/**
 * CPRA Excel Export
 * Generates a multi-sheet XLSX workbook for the Comparative Payment Rate Analysis.
 */

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

export interface CpraXlsxInput {
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

export async function generateCpraXlsx(input: CpraXlsxInput): Promise<void> {
  const XLSX = await import("xlsx");
  const wb = XLSX.utils.book_new();
  const date = new Date().toISOString().split("T")[0];

  // ── Summary sheet ───────────────────────────────────────────────────
  const summaryData: (string | number)[][] = [
    ["Comparative Payment Rate Analysis"],
    [`${input.stateName} (${input.state})`],
    [`Generated: ${date}`],
    ["42 CFR §447.203 | CMS Ensuring Access Final Rule | Deadline: July 1, 2026"],
    [],
    ["Category Summary"],
    ["Category", "Codes", "Parity Codes", "Wtd Avg % MCR", "Below 80%", "Below 50%", "Claims", "Beneficiaries"],
    ...input.catSummary.map(c => [
      c.label,
      c.totalCodes,
      c.parityCodes,
      +c.weightedAvg.toFixed(1),
      c.below80,
      c.below50,
      c.totalClaims,
      c.totalBene,
    ]),
    [],
    ["Overall Statistics"],
    ["Total codes analyzed", input.stats.totalCodes],
    ["Weighted average % of Medicare", +input.stats.weightedAvg.toFixed(1)],
    ["Codes below 80% Medicare", input.stats.below80],
    ["Codes below 50% Medicare", input.stats.below50],
    ["Total claims (CY2023 FFS)", input.stats.totalClaims],
    ["Total beneficiary-service events", input.stats.totalBene],
    ["Fee schedule rate codes", input.stats.fsCodes],
    ["T-MSIS effective rate codes", input.stats.precomputedCodes],
    ["FFS share", input.ffsShare !== null && !isNaN(Number(input.ffsShare)) ? +((Number(input.ffsShare)) * 100).toFixed(1) : "N/A"],
  ];
  const wsSummary = XLSX.utils.aoa_to_sheet(summaryData);
  wsSummary["!cols"] = [{ wch: 30 }, { wch: 14 }, { wch: 14 }, { wch: 16 }, { wch: 12 }, { wch: 12 }, { wch: 14 }, { wch: 14 }];
  XLSX.utils.book_append_sheet(wb, wsSummary, "Summary");

  // ── Per-category sheets ─────────────────────────────────────────────
  const categories = [
    { id: "primary_care", label: "Primary Care", sheetName: "Primary Care" },
    { id: "obgyn", label: "OB/GYN", sheetName: "OB-GYN" },
    { id: "mh_sud", label: "MH/SUD", sheetName: "MH-SUD" },
  ];

  for (const cat of categories) {
    const catRows = input.rows
      .filter(r => r.category === cat.id)
      .sort((a, b) => (a.pctMedicare ?? 999) - (b.pctMedicare ?? 999));

    const sheetData: (string | number)[][] = [
      ["HCPCS", "Description", "Category", "Medicaid Rate", "Rate Source", "Medicare Rate", "% of Medicare", "Claims", "Beneficiaries", "Status"],
      ...catRows.map(r => [
        r.hcpcs,
        r.desc,
        r.categoryLabel,
        +r.medicaidRate.toFixed(2),
        r.rateSource === "fee_schedule" ? "Fee Schedule" : "T-MSIS",
        r.medicareRate !== null ? +r.medicareRate.toFixed(2) : "N/A" as string | number,
        r.pctMedicare !== null ? +r.pctMedicare.toFixed(1) : "N/A" as string | number,
        r.claims,
        r.bene,
        r.flag === "critical" ? "CRITICAL" : r.flag === "warn" ? "WARNING" : r.flag === "pass" ? "PASS" : "N/A",
      ]),
    ];
    const ws = XLSX.utils.aoa_to_sheet(sheetData);
    ws["!cols"] = [
      { wch: 10 }, { wch: 40 }, { wch: 14 }, { wch: 14 },
      { wch: 14 }, { wch: 14 }, { wch: 14 }, { wch: 12 }, { wch: 14 }, { wch: 10 },
    ];
    XLSX.utils.book_append_sheet(wb, ws, cat.sheetName);
  }

  // ── Data Notes sheet ────────────────────────────────────────────────
  const notesData: (string | number)[][] = [
    ["Data Notes"],
    [],
    ["Coverage"],
    ["Service year", "2023 T-MSIS"],
    ["Coverage", "FFS claims only"],
    ["FFS share", input.ffsShare !== null && !isNaN(Number(input.ffsShare)) ? `${(Number(input.ffsShare) * 100).toFixed(0)}%` : "N/A"],
    ["Missing from analysis", "Inpatient, pharmacy, LTSS, DSH payments"],
    [],
    ["Rate Sources"],
    ["Fee schedule rates", `${input.stats.fsCodes} codes`],
    ["T-MSIS effective rates", `${input.stats.precomputedCodes} codes`],
    ["Medicare benchmark", "CY2026 PFS national non-facility rates"],
    ["Locality adjustment", "Pending (GPCI data available)"],
    [],
    ["Known Limitations"],
    ["Beneficiary counts", "Patient-service events, not unique individuals (inflated ~10x)"],
    ["H-codes", "Behavioral health codes excluded from parity; no Medicare equivalent"],
    ["T-MSIS rates", "May differ from published fee schedules"],
    [],
    ["State Methodology"],
    ["Methodology", input.stateConv?.methodology || "N/A"],
    ["Detail", input.stateConv?.methodology_detail || "N/A"],
    ["Update frequency", input.stateConv?.update_frequency || "N/A"],
    ["GPCI approach", input.stateConv?.gpci_approach || "N/A"],
    [],
    ["Regulatory Reference"],
    ["Rule", "42 CFR §447.203 -- Ensuring Access to Medicaid Services"],
    ["Deadline", "July 1, 2026"],
    [],
    ["Generated by Aradune | aradune.co"],
  ];
  const wsNotes = XLSX.utils.aoa_to_sheet(notesData);
  wsNotes["!cols"] = [{ wch: 24 }, { wch: 60 }];
  XLSX.utils.book_append_sheet(wb, wsNotes, "Data Notes");

  XLSX.writeFile(wb, `cpra_${input.state}_${date}.xlsx`);
}
