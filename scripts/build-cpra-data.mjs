#!/usr/bin/env node
/**
 * CPRA Data Pipeline (Terminal B)
 * Generates three deliverables from CMS RVU CSV + existing JSON data:
 *   1. dim_447_codes.json   -- Authoritative CMS code list with CPRA category mapping
 *   2. cpra_precomputed.json -- Locality-adjusted Medicare rates per state
 *   3. dq_flags.json        -- Per-code and per-state data quality flags
 *
 * Usage:
 *   node scripts/build-cpra-data.mjs
 *
 * Reads:
 *   data/PPRRVU*nonQPP*.csv       (CMS PFS RVU file)
 *   public/data/gpci.json          (Medicare GPCI factors)
 *   public/data/medicaid_rates.json (state Medicaid fee schedules)
 *   public/data/medicare_rates.json (national Medicare rates)
 *   public/data/states.json         (state metadata)
 *
 * Outputs:
 *   public/data/dim_447_codes.json
 *   public/data/cpra_precomputed.json
 *   public/data/dq_flags.json
 */

import fs from "fs";
import path from "path";
import { createReadStream } from "fs";
import { createInterface } from "readline";
import { fileURLToPath } from "url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const DATA_DIR = path.join(ROOT, "data");
const PUB_DATA = path.join(ROOT, "public", "data");

// ── CSV parser (same as update-rvu.mjs) ───────────────────────────────
function parseCSVLine(line) {
  const result = [];
  let current = "";
  let inQuotes = false;
  for (let i = 0; i < line.length; i++) {
    const ch = line[i];
    if (inQuotes) {
      if (ch === '"' && line[i + 1] === '"') { current += '"'; i++; }
      else if (ch === '"') { inQuotes = false; }
      else { current += ch; }
    } else {
      if (ch === '"') { inQuotes = true; }
      else if (ch === ",") { result.push(current); current = ""; }
      else { current += ch; }
    }
  }
  result.push(current);
  return result;
}

// ── CPRA Category Definitions (42 CFR 447.203) ─────────────────────────
// These are the three service categories the rule requires:
//   1. Primary care (E/M office visits, preventive, chronic care management)
//   2. OB/GYN (maternity, delivery, reproductive health)
//   3. Outpatient mental health & SUD
const CPRA_CATEGORIES = {
  primary_care: {
    label: "Primary Care",
    description: "Office/outpatient E/M visits, preventive care, chronic care management",
    codeRanges: [
      // Office/outpatient visits (new + established)
      ["99202", "99215"],
      // Preventive visits (new + established)
      ["99381", "99397"],
      // Chronic care management
      ["99490", "99491"],
      // Annual wellness visits
      ["G0438", "G0439"],
      // Transitional care management
      ["99495", "99496"],
      // Prolonged services
      ["99354", "99358"],
    ],
  },
  obgyn: {
    label: "OB/GYN",
    description: "Maternity, delivery, and reproductive health services",
    codeRanges: [
      // Maternity care & delivery
      ["59000", "59899"],
    ],
  },
  mh_sud: {
    label: "MH/SUD",
    description: "Outpatient mental health, psychiatry, psychotherapy, and substance use disorder services",
    codeRanges: [
      // Psychiatry & psychotherapy
      ["90785", "90899"],
      // Behavioral health H-codes (no Medicare equivalent)
      ["H0001", "H2037"],
    ],
  },
};

function expandRange(start, end) {
  const prefix = start.replace(/\d+$/, "");
  const sNum = parseInt(start.replace(/\D/g, ""), 10);
  const eNum = parseInt(end.replace(/\D/g, ""), 10);
  const pad = start.length - prefix.length;
  const out = [];
  for (let i = sNum; i <= eNum; i++) out.push(prefix + String(i).padStart(pad, "0"));
  return out;
}

function buildCodeSet() {
  const all = new Set();
  const codeToCategory = new Map();
  for (const [catId, cat] of Object.entries(CPRA_CATEGORIES)) {
    for (const [start, end] of cat.codeRanges) {
      for (const code of expandRange(start, end)) {
        all.add(code);
        codeToCategory.set(code, catId);
      }
    }
  }
  return { all, codeToCategory };
}

// ── Find CMS CSV ───────────────────────────────────────────────────────
function findRvuFile() {
  const files = fs.readdirSync(DATA_DIR).filter(f =>
    f.startsWith("PPRRVU") && f.includes("nonQPP") && f.endsWith(".csv")
  );
  if (!files.length) {
    console.error("No PPRRVU*nonQPP*.csv found in data/");
    process.exit(1);
  }
  files.sort().reverse();
  return path.join(DATA_DIR, files[0]);
}

// ── Parse RVU CSV with component breakdown ─────────────────────────────
// Columns from CMS PPRRVU CSV:
//   0: HCPCS, 1: MOD, 2: DESCRIPTION, 3: STATUS CODE
//   5: WORK RVU
//   6: NON-FACILITY PE RVU
//   8: FACILITY PE RVU
//  10: MP RVU
//  11: NON-FACILITY TOTAL RVU
//  12: FACILITY TOTAL RVU
//  25: CONVERSION FACTOR
async function parseRvuComponents(csvPath) {
  console.log(`Parsing RVU components from: ${path.basename(csvPath)}`);
  const codes = {};
  let cf = null;
  let headerFound = false;

  const rl = createInterface({ input: createReadStream(csvPath, "utf-8") });

  for await (const rawLine of rl) {
    const line = rawLine.replace(/^\uFEFF/, "");
    const row = parseCSVLine(line);

    if (!headerFound) {
      if (row.length > 0 && row[0].trim() === "HCPCS") headerFound = true;
      continue;
    }

    if (row.length < 13) continue;

    const hcpcs = row[0].trim();
    const mod = row[1].trim();
    const desc = row[2].trim();
    const status = row[3].trim();
    const workRvu = parseFloat(row[5]) || 0;
    const nfPeRvu = parseFloat(row[6]) || 0;
    const facPeRvu = parseFloat(row[8]) || 0;
    const mpRvu = parseFloat(row[10]) || 0;
    const nfTotal = parseFloat(row[11]) || 0;
    const facTotal = parseFloat(row[12]) || 0;

    if (!cf && row.length > 25) {
      const rawCf = parseFloat(row[25]);
      if (rawCf > 20 && rawCf < 50) cf = rawCf;
    }

    // Skip non-payable statuses
    if (["B", "T", "C", "N"].includes(status)) continue;
    // Prefer base code (no modifier)
    if (mod && hcpcs in codes) continue;

    if (nfTotal > 0 || facTotal > 0) {
      codes[hcpcs] = {
        desc: desc.slice(0, 60),
        work_rvu: Math.round(workRvu * 1000) / 1000,
        nf_pe_rvu: Math.round(nfPeRvu * 1000) / 1000,
        fac_pe_rvu: Math.round(facPeRvu * 1000) / 1000,
        mp_rvu: Math.round(mpRvu * 1000) / 1000,
        nf_total_rvu: Math.round(nfTotal * 1000) / 1000,
        fac_total_rvu: Math.round(facTotal * 1000) / 1000,
      };
    }
  }

  if (!cf) cf = 33.4009;
  console.log(`  CF: $${cf}, codes: ${Object.keys(codes).length}`);
  return { codes, cf };
}

// ── Build dim_447_codes.json ────────────────────────────────────────────
function buildDim447Codes(rvuCodes, codeToCategory, allCodes) {
  console.log("\nBuilding dim_447_codes.json...");

  const codes = {};
  let found = 0;
  let missing = 0;

  for (const code of allCodes) {
    const catId = codeToCategory.get(code);
    const rvu = rvuCodes[code];
    const isHCode = code.startsWith("H");

    codes[code] = {
      code,
      description: rvu?.desc || (isHCode ? `BH service ${code}` : code),
      category: catId,
      category_label: CPRA_CATEGORIES[catId].label,
      has_medicare: !isHCode && !!rvu,
      work_rvu: rvu?.work_rvu || null,
      nf_pe_rvu: rvu?.nf_pe_rvu || null,
      mp_rvu: rvu?.mp_rvu || null,
      nf_total_rvu: rvu?.nf_total_rvu || null,
    };

    if (rvu || isHCode) found++;
    else missing++;
  }

  const output = {
    metadata: {
      source: "CMS PFS CY2026 + 42 CFR 447.203 category mapping",
      generated: new Date().toISOString().split("T")[0],
      total_codes: allCodes.size,
      codes_with_rvu: found,
      codes_missing_rvu: missing,
    },
    categories: Object.fromEntries(
      Object.entries(CPRA_CATEGORIES).map(([id, cat]) => [id, {
        label: cat.label,
        description: cat.description,
        code_count: [...allCodes].filter(c => codeToCategory.get(c) === id).length,
      }])
    ),
    codes,
  };

  const outPath = path.join(PUB_DATA, "dim_447_codes.json");
  fs.writeFileSync(outPath, JSON.stringify(output));
  console.log(`  Written: ${outPath} (${Object.keys(codes).length} codes)`);
  return output;
}

// ── Build cpra_precomputed.json ─────────────────────────────────────────
function buildCpraPrecomputed(rvuCodes, cf, gpci, codeToCategory, allCodes) {
  console.log("\nBuilding cpra_precomputed.json...");

  // Group GPCI entries by state
  const gpciByState = {};
  for (const entry of gpci) {
    if (!gpciByState[entry.state]) gpciByState[entry.state] = [];
    gpciByState[entry.state].push(entry);
  }

  const byState = {};

  for (const [st, localities] of Object.entries(gpciByState)) {
    // For CPRA, use the statewide or "rest of state" locality.
    // If only one locality, use it. If multiple, pick locality "99" (rest of state)
    // or the first one (statewide "00").
    let primary = localities.find(l => l.locality === "99")
      || localities.find(l => l.locality === "00")
      || localities[0];

    const rates = {};
    let computed = 0;

    for (const code of allCodes) {
      const rvu = rvuCodes[code];
      if (!rvu) continue; // H-codes and missing codes
      if (!codeToCategory.has(code)) continue;

      // Locality-adjusted rate:
      // Payment = (Work_RVU * PW_GPCI + NF_PE_RVU * PE_GPCI + MP_RVU * MP_GPCI) * CF
      const adjustedRate = Math.round(
        (rvu.work_rvu * primary.pw_gpci_floor
         + rvu.nf_pe_rvu * primary.pe_gpci
         + rvu.mp_rvu * primary.mp_gpci) * cf * 100
      ) / 100;

      const nationalRate = Math.round(rvu.nf_total_rvu * cf * 100) / 100;

      rates[code] = {
        adjusted_rate: adjustedRate,
        national_rate: nationalRate,
        gpci_effect: nationalRate > 0 ? Math.round((adjustedRate / nationalRate) * 1000) / 1000 : 1,
      };
      computed++;
    }

    byState[st] = {
      locality_used: primary.locality,
      locality_name: primary.locality_name,
      pw_gpci: primary.pw_gpci_floor,
      pe_gpci: primary.pe_gpci,
      mp_gpci: primary.mp_gpci,
      n_localities: localities.length,
      codes_computed: computed,
      rates,
    };
  }

  const output = {
    metadata: {
      source: "CMS PFS CY2026 RVU × GPCI locality adjustment",
      cf,
      year: 2026,
      generated: new Date().toISOString().split("T")[0],
      states: Object.keys(byState).length,
      note: "Uses pw_gpci_floor (1.0 floor) for work component per CMS policy",
    },
    by_state: byState,
  };

  const outPath = path.join(PUB_DATA, "cpra_precomputed.json");
  const json = JSON.stringify(output);
  fs.writeFileSync(outPath, json);
  const sizeMb = (Buffer.byteLength(json) / 1024 / 1024).toFixed(2);
  console.log(`  Written: ${outPath} (${Object.keys(byState).length} states, ${sizeMb} MB)`);
  return output;
}

// ── Build dq_flags.json ─────────────────────────────────────────────────
function buildDqFlags(dim447, precomputed, medicaidRates, statesData) {
  console.log("\nBuilding dq_flags.json...");

  const byCode = {};
  const byState = {};

  // Per-code flags
  for (const [code, info] of Object.entries(dim447.codes)) {
    const flags = [];

    // No Medicare equivalent
    if (!info.has_medicare) {
      flags.push({ flag: "no_medicare_equivalent", severity: "info", detail: "H-code or non-Medicare service; excluded from parity calculations" });
    }

    // Missing RVU components
    if (info.has_medicare && info.work_rvu === null) {
      flags.push({ flag: "missing_rvu", severity: "warn", detail: "Code in scope but no RVU data in CMS PFS" });
    }

    // Check Medicaid coverage across states
    let statesWithRate = 0;
    for (const [st, rates] of Object.entries(medicaidRates)) {
      if (rates[code] && rates[code][0] > 0) statesWithRate++;
    }

    if (statesWithRate === 0 && info.has_medicare) {
      flags.push({ flag: "no_medicaid_rates", severity: "warn", detail: "No state has a fee schedule rate for this code" });
    } else if (statesWithRate < 10 && info.has_medicare) {
      flags.push({ flag: "sparse_medicaid", severity: "info", detail: `Only ${statesWithRate} states have fee schedule rates` });
    }

    // Check claim volume (rough proxy via precomputed state count)
    let statesInPrecomputed = 0;
    for (const stData of Object.values(precomputed.by_state)) {
      if (stData.rates[code]) statesInPrecomputed++;
    }

    byCode[code] = {
      category: info.category,
      flags,
      medicaid_states: statesWithRate,
      precomputed_states: statesInPrecomputed,
    };
  }

  // Per-state flags
  const statesMap = {};
  for (const s of statesData) statesMap[s.state] = s;

  for (const [st, stData] of Object.entries(precomputed.by_state)) {
    const flags = [];
    const stInfo = statesMap[st];

    // Locality coverage
    if (stData.n_localities > 1) {
      flags.push({ flag: "multi_locality", severity: "info", detail: `${stData.n_localities} Medicare localities; using "${stData.locality_name}" as primary` });
    }

    // FFS share
    if (stInfo && stInfo.ffs_share !== undefined) {
      const ffs = Number(stInfo.ffs_share);
      if (ffs < 0.2) {
        flags.push({ flag: "low_ffs_share", severity: "warn", detail: `Only ${(ffs * 100).toFixed(0)}% FFS; T-MSIS data covers minority of Medicaid population` });
      }
    }

    // Medicaid rate coverage
    const stMedicaid = medicaidRates[st] || {};
    const totalCpra = Object.keys(dim447.codes).length;
    const hasFsRate = Object.keys(dim447.codes).filter(c => stMedicaid[c] && stMedicaid[c][0] > 0).length;
    const fsCoverage = hasFsRate / totalCpra;

    if (fsCoverage < 0.1) {
      flags.push({ flag: "minimal_fee_schedule", severity: "warn", detail: `Only ${hasFsRate}/${totalCpra} CPRA codes in fee schedule` });
    } else if (fsCoverage < 0.5) {
      flags.push({ flag: "partial_fee_schedule", severity: "info", detail: `${hasFsRate}/${totalCpra} CPRA codes in fee schedule (${(fsCoverage * 100).toFixed(0)}%)` });
    }

    byState[st] = {
      locality_used: stData.locality_name,
      n_localities: stData.n_localities,
      cpra_codes_in_fs: hasFsRate,
      cpra_codes_total: totalCpra,
      fs_coverage_pct: Math.round(fsCoverage * 100),
      flags,
    };
  }

  const output = {
    metadata: {
      generated: new Date().toISOString().split("T")[0],
      total_codes: Object.keys(byCode).length,
      total_states: Object.keys(byState).length,
    },
    by_code: byCode,
    by_state: byState,
  };

  const outPath = path.join(PUB_DATA, "dq_flags.json");
  fs.writeFileSync(outPath, JSON.stringify(output));
  console.log(`  Written: ${outPath}`);
  return output;
}

// ── Main ────────────────────────────────────────────────────────────────
async function main() {
  console.log("=".repeat(60));
  console.log("  CPRA Data Pipeline (Terminal B) -- Aradune");
  console.log("=".repeat(60));

  // Load inputs
  const csvPath = findRvuFile();
  const gpci = JSON.parse(fs.readFileSync(path.join(PUB_DATA, "gpci.json"), "utf-8"));
  const medicaidRates = JSON.parse(fs.readFileSync(path.join(PUB_DATA, "medicaid_rates.json"), "utf-8"));
  const statesData = JSON.parse(fs.readFileSync(path.join(PUB_DATA, "states.json"), "utf-8"));

  // Parse CMS CSV with full component breakdown
  const { codes: rvuCodes, cf } = await parseRvuComponents(csvPath);

  // Build code set
  const { all: allCodes, codeToCategory } = buildCodeSet();
  console.log(`\nCPRA scope: ${allCodes.size} codes across ${Object.keys(CPRA_CATEGORIES).length} categories`);

  // 1. dim_447_codes.json
  const dim447 = buildDim447Codes(rvuCodes, codeToCategory, allCodes);

  // 2. cpra_precomputed.json
  const precomputed = buildCpraPrecomputed(rvuCodes, cf, gpci, codeToCategory, allCodes);

  // 3. dq_flags.json
  buildDqFlags(dim447, precomputed, medicaidRates, statesData);

  console.log("\n  All Terminal B deliverables built successfully.\n");
}

main().catch(err => {
  console.error("Error:", err.message);
  process.exit(1);
});
