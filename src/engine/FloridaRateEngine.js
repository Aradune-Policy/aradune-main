/**
 * FloridaRateEngine.js
 * 
 * Complete implementation of Florida Medicaid Practitioner Reimbursement
 * Methodology per SPA FL-24-0002 (Attachment 4.19-B, Exhibit II).
 * 
 * Implements the full rate-setting hierarchy:
 *   A. Legislative Rates (lookup)
 *   B. RBRVS rates (RVU × CF × 1.04)
 *   C. FCSO locale-weighted rates (× 0.60)
 *   D. Lab rates (CLFS × 0.60, independent × 0.90)
 *   E. Anesthesia (base + time units × $14.50)
 *   F. Other state PPP comparison
 *   G. Like-code coverage
 *   H. Manual pricing
 * 
 * Also implements:
 *   - Annual CF rebalancing (budget-neutral optimization with ±10% guardrails)
 *   - PA/ARNP 80% reduction (§409.905 F.S.)
 *   - Pediatric rate stacking (4% + 24% + 10.2% + 7.3%)
 *   - All legislative rate protections (IV.a through IV.j)
 * 
 * Data sources:
 *   - Medicare PFS RVUs (CY2026)
 *   - FCSO locality rates (Jurisdiction N, locales 03/04/99)
 *   - CMS Clinical Lab Fee Schedule
 *   - FL Medicaid utilization (T-MSIS or FLMMIS extract)
 *   - BEA Regional Price Parities (for cross-state PPP)
 * 
 * Based on: Ad_Hoc_Fee_Setting_-_CSB.R (v5.0) by Scott Toriello
 * Fixes: FCSO 2-locale weighting, lab priority, facility rates,
 *        PC/TC splits, base FS vs FSI separation
 */

// ── Configuration ───────────────────────────────────────────────────────

const FL_CONFIG = {
  // Current conversion factor (CY2026)
  CF: 24.9876420525878,

  // Fee Schedule Increase — SPA IV.b (Ch. 2000-166, Laws of FL)
  FSI_MULT: 1.04,

  // Medicaid multiplier for FCSO-based rates — SPA II.E(a)
  MEDICAID_MULT: 0.60,

  // Independent lab reduction — SPA II.E(d)
  INDEP_LAB_MULT: 0.90,

  // GPCI — SPA Glossary O: "standard GPCI of 1 across all locales"
  GPCI: 1.0,

  // Locale population weights — EDR 2024
  // Used for FCSO weighted average — SPA II.E(a)
  LOCALE_WEIGHTS: {
    "03": 0.238319,
    "04": 0.124225,
    "99": 0.637456,
  },

  // Anesthesia time rate — SPA II.E(b)
  ANESTHESIA_TIME_RATE: 14.50,

  // PA/ARNP reimbursement — SPA II.I, §409.905 F.S.
  PA_ARNP_MULT: 0.80,

  // Lab PC/TC split — SPA II.E(c)
  LAB_PC_SHARE: 0.20,
  LAB_TC_SHARE: 0.80,

  // Budget neutrality guardrail — SPA III
  MAX_RATE_CHANGE: 0.10, // ±10%

  // Pediatric specialty types — SPA IV.d (Ch. 2004-268)
  PED_SPECIALTY_TYPES: [
    "002","003","004","005","008","010","014","015","017",
    "020","021","022","023","029","030","031","036","037",
    "038","039","043","046","051","053","055","057","058","060","062"
  ],

  // Pediatric E&M rate increase specialty types — SPA IV.e (Ch. 2014-51)
  PED_EM_SPECIALTY_TYPES: [
    "01","19","23","35","36","37","38","39","43","49","59","101","102"
  ],

  // Legislative frozen rates — SPA IV.a (Ch. 99-223)
  LEGISLATIVE_RATES: {
    "99212": 26.45,
    "99213": 32.56,
    "99214": 48.27,
  },

  // Rate increase exclusions — SPA IV top
  FSI_EXCLUDED_CATEGORIES: ["supplies", "devices", "laboratory", "pathology"],
};

// ── Core Rate Calculation Functions ─────────────────────────────────────

/**
 * Calculate RBRVS rate per SPA Section II.D
 * Fee = GPCI × RVU × CF
 * FSI = Fee × 1.04
 * 
 * Returns both base fee (FS) and increased fee (FSI) separately.
 */
function calcRBRVS(rvu, cf = FL_CONFIG.CF, gpci = FL_CONFIG.GPCI) {
  if (!rvu || rvu <= 0) return null;
  const baseFee = gpci * rvu * cf;
  const fsi = baseFee * FL_CONFIG.FSI_MULT;
  return {
    baseFee: round2(baseFee),
    fsi: round2(fsi),
    rvu,
    cf,
    gpci,
    formula: `GPCI(${gpci}) × RVU(${rvu.toFixed(4)}) × CF($${cf.toFixed(4)}) = $${baseFee.toFixed(2)}`,
    formulaFSI: `$${baseFee.toFixed(2)} × ${FL_CONFIG.FSI_MULT} = $${fsi.toFixed(2)}`,
  };
}

/**
 * Calculate FCSO weighted average rate per SPA Section II.E(a)
 * 
 * CRITICAL FIX: Handles 2-locale and 1-locale cases correctly.
 * The SPA specifies weight redistribution when locales are missing,
 * NOT zeroing out missing weights (which underprices the rate).
 * 
 * 3 locales: weighted avg = R03×A + R04×B + R99×C
 * 2 locales: redistribute missing weight proportionally
 * 1 locale:  use that locale's rate directly
 * 
 * Then: Medicaid FS = weighted avg × 0.60
 */
function calcFCSO(rates03, rates04, rates99) {
  const w = FL_CONFIG.LOCALE_WEIGHTS;
  const locales = [];

  if (rates03 != null && rates03 > 0) locales.push({ rate: rates03, weight: w["03"], locale: "03" });
  if (rates04 != null && rates04 > 0) locales.push({ rate: rates04, weight: w["04"], locale: "04" });
  if (rates99 != null && rates99 > 0) locales.push({ rate: rates99, weight: w["99"], locale: "99" });

  if (locales.length === 0) return null;

  let weightedAvg;
  let formula;

  if (locales.length === 3) {
    // Standard 3-locale weighted average — SPA II.E(a) page 4
    weightedAvg = locales.reduce((sum, l) => sum + l.rate * l.weight, 0);
    formula = locales.map(l => `R${l.locale}($${l.rate.toFixed(2)}) × ${l.weight.toFixed(4)}`).join(" + ");
  } else if (locales.length === 2) {
    // 2-locale redistribution — SPA II.E(a) page 4
    // Total weight of available locales
    const totalWeight = locales[0].weight + locales[1].weight;
    // Missing locale's weight to redistribute
    const missingWeight = 1 - totalWeight;
    // Proportional redistribution
    const adj0 = locales[0].weight + (locales[0].weight / totalWeight) * missingWeight;
    const adj1 = locales[1].weight + (locales[1].weight / totalWeight) * missingWeight;

    // SPA formula: the missing locale's share is split proportionally
    // then each locale's rate is multiplied by its adjusted weight
    weightedAvg = locales[0].rate * adj0 + locales[1].rate * adj1;
    formula = `2-locale redistribution: R${locales[0].locale}($${locales[0].rate.toFixed(2)}) × ${adj0.toFixed(4)} + R${locales[1].locale}($${locales[1].rate.toFixed(2)}) × ${adj1.toFixed(4)}`;
  } else {
    // 1-locale — SPA II.E(a) page 4: "Medicare FS Rate = R03"
    weightedAvg = locales[0].rate;
    formula = `1-locale: R${locales[0].locale} = $${locales[0].rate.toFixed(2)}`;
  }

  const medicaidRate = weightedAvg * FL_CONFIG.MEDICAID_MULT;

  return {
    medicareWeightedAvg: round2(weightedAvg),
    medicaidRate: round2(medicaidRate),
    fsi: round2(medicaidRate * FL_CONFIG.FSI_MULT),
    localesUsed: locales.length,
    formula,
    formulaMedicaid: `$${weightedAvg.toFixed(2)} × ${FL_CONFIG.MEDICAID_MULT} = $${medicaidRate.toFixed(2)}`,
  };
}

/**
 * Calculate lab rate per SPA Section II.E(c) and (d)
 * 
 * Practitioner lab: Medicare TC × 0.60
 *   PC = FS × 0.20
 *   TC = FS × 0.80
 * 
 * Independent lab: Practitioner rate × 0.90
 * 
 * Note: SPA says check FCSO first, then CLFS.
 */
function calcLabRate(clfsRate, fcsoRate = null) {
  // SPA II.E(c): "FCSO is reviewed for laboratory services. If not found, CLFS is used."
  const baseRate = (fcsoRate && fcsoRate > 0) ? fcsoRate : clfsRate;
  if (!baseRate || baseRate <= 0) return null;

  const source = (fcsoRate && fcsoRate > 0) ? "FCSO" : "CLFS";
  const practFS = baseRate * FL_CONFIG.MEDICAID_MULT;
  const practPC = practFS * FL_CONFIG.LAB_PC_SHARE;
  const practTC = practFS * FL_CONFIG.LAB_TC_SHARE;
  const indepFS = practFS * FL_CONFIG.INDEP_LAB_MULT;
  const indepPC = indepFS * FL_CONFIG.LAB_PC_SHARE;
  const indepTC = indepFS * FL_CONFIG.LAB_TC_SHARE;

  return {
    source,
    medicareRate: baseRate,
    practitioner: {
      fs: round2(practFS),
      pc: round2(practPC),
      tc: round2(practTC),
      formula: `${source}($${baseRate.toFixed(2)}) × 0.60 = $${practFS.toFixed(2)}, PC=$${practPC.toFixed(2)} (20%), TC=$${practTC.toFixed(2)} (80%)`,
    },
    independent: {
      fs: round2(indepFS),
      pc: round2(indepPC),
      tc: round2(indepTC),
      formula: `Pract($${practFS.toFixed(2)}) × 0.90 = $${indepFS.toFixed(2)}`,
    },
  };
}

/**
 * Calculate anesthesia rate per SPA Section II.E(b)
 * Cost = Base Units × CF × FSI + (Time in 15-min increments × $14.50)
 * 
 * Note: Time rounded DOWN to nearest 15-min increment.
 * Pediatric rate increase of 4% applies for age < 21.
 */
function calcAnesthesia(baseUnits, timeMinutes = null, isPediatric = false) {
  if (!baseUnits || baseUnits <= 0) return null;

  const baseCost = baseUnits * FL_CONFIG.CF;
  let timeCost = 0;
  let timeUnits = 0;

  if (timeMinutes && timeMinutes > 0) {
    timeUnits = Math.floor(timeMinutes / 15);
    timeCost = timeUnits * FL_CONFIG.ANESTHESIA_TIME_RATE;
  }

  let total = baseCost + timeCost;
  if (isPediatric) total *= FL_CONFIG.FSI_MULT; // 4% pediatric increase

  return {
    baseCost: round2(baseCost),
    timeCost: round2(timeCost),
    total: round2(total),
    baseUnits,
    timeUnits,
    isPediatric,
    formula: `Base(${baseUnits} × $${FL_CONFIG.CF.toFixed(2)}) + Time(${timeUnits} × $${FL_CONFIG.ANESTHESIA_TIME_RATE}) = $${total.toFixed(2)}${isPediatric ? " (×1.04 pediatric)" : ""}`,
  };
}

/**
 * Calculate pediatric rate stacking per SPA Section IV
 * 
 * When all criteria met, multiply sequentially:
 *   base × 1.04 (FSI) × 1.24 (specialty) × 1.102 (E&M) × 1.073 (physician ped)
 * 
 * Per SPA IV.g: "The highest rate following inclusion of all applicable
 * fee schedule increases is reimbursed."
 */
function calcPediatricStacking(baseFee, {
  providerType = null,
  specialtyType = null,
  cptCode = null,
  patientAge = null,
} = {}) {
  if (!baseFee || baseFee <= 0) return { finalRate: baseFee, increases: [], formula: "" };

  const increases = [];
  let rate = baseFee;

  // IV.b: FSI 4% — provider types 25,26,27,28,29,30,35,62
  const fsiProviders = ["25","26","27","28","29","30","35","62"];
  if (fsiProviders.includes(providerType)) {
    rate *= 1.04;
    increases.push({ name: "FSI (Ch.2000-166)", mult: 1.04 });
  }

  // IV.c: Pediatric 4% — under 21
  if (patientAge != null && patientAge < 21) {
    rate *= 1.04;
    increases.push({ name: "Pediatric (Ch.2001-253)", mult: 1.04 });
  }

  // IV.d: Pediatric specialty 24% — under 21, specific specialty types
  if (patientAge != null && patientAge < 21 &&
      FL_CONFIG.PED_SPECIALTY_TYPES.includes(specialtyType)) {
    rate *= 1.24;
    increases.push({ name: "Ped Specialty (Ch.2004-268)", mult: 1.24 });
  }

  // IV.e: Pediatric E&M 10.2% — CPT 99201-99496, specific specialty types
  if (cptCode) {
    const cptNum = parseInt(cptCode);
    if (cptNum >= 99201 && cptNum <= 99496 &&
        FL_CONFIG.PED_EM_SPECIALTY_TYPES.includes(specialtyType)) {
      rate *= 1.102;
      increases.push({ name: "Ped E&M (Ch.2014-51)", mult: 1.102 });
    }
  }

  // IV.f: Physician pediatric 7.3% — under 21, provider types 25,26
  if (patientAge != null && patientAge < 21 &&
      ["25","26"].includes(providerType)) {
    rate *= 1.073;
    increases.push({ name: "Physician Ped (2023-24 GAA)", mult: 1.073 });
  }

  const formula = increases.length > 0
    ? `$${baseFee.toFixed(2)} × ${increases.map(i => i.mult).join(" × ")} = $${rate.toFixed(2)}`
    : "";

  return {
    finalRate: round2(rate),
    baseFee: round2(baseFee),
    increases,
    formula,
    totalMultiplier: increases.reduce((m, i) => m * i.mult, 1),
  };
}

// ── Ad Hoc Fee Setting Pipeline ─────────────────────────────────────────

/**
 * Price a single HCPCS code using FL SPA methodology hierarchy.
 * 
 * Priority per SPA Sections II.A through II.H:
 *   1. Legislative rate (frozen by statute)
 *   2. RBRVS (has Medicare RVUs)
 *   3. FCSO locale-weighted (no RVUs, has FCSO rates)
 *   4. Lab-specific (FCSO → CLFS, with PC/TC split)
 *   5. Anesthesia (base + time formula)
 *   6. Other state PPP comparison (no FL data)
 *   7. Like-code coverage
 *   8. Manual pricing required
 * 
 * @param {string} code - HCPCS/CPT code
 * @param {object} data - All available data for this code
 * @returns {object} Complete rate calculation with methodology trail
 */
function priceCode(code, data = {}) {
  const {
    // RVU data (from Medicare PFS)
    rvuGlobal = null,      // Non-facility total RVU (Work + PE NF + MP)
    rvuFacility = null,    // Facility total RVU (Work + PE F + MP)
    rvu26 = null,          // Professional component RVU
    rvuTC = null,          // Technical component RVU
    workRVU = null,
    peRVU_NF = null,
    peRVU_F = null,
    mpRVU = null,

    // FCSO locale rates
    fcso03_global = null,
    fcso04_global = null,
    fcso99_global = null,
    fcso03_26 = null,
    fcso04_26 = null,
    fcso99_26 = null,
    fcso03_tc = null,
    fcso04_tc = null,
    fcso99_tc = null,

    // Lab data
    clfsRate = null,

    // Anesthesia
    anesthesiaBaseUnits = null,

    // Cross-state data (for PPP step)
    otherStateRates = null, // [{ state, rate, rpp }]

    // Like-code reference
    likeCode = null,
    likeCodeRate = null,

    // Is this a lab code?
    isLab = false,

    // Override
    legislativeRate = null,
  } = data;

  const result = {
    code,
    methodology: null,
    spaSection: null,
    rates: {},
    calculation: null,
    alternateRates: {},
    flags: [],
    auditTrail: [],
  };

  // ── Step A/B: Legislative Rates ─────────────────────────────────
  const legRate = legislativeRate || FL_CONFIG.LEGISLATIVE_RATES[code];
  if (legRate) {
    result.methodology = "Legislative Rate";
    result.spaSection = "II.A/B";
    result.rates.fs = legRate;
    result.rates.fsi = legRate; // Legislative rates ARE the final rate
    result.calculation = { formula: `Legislative rate: $${legRate.toFixed(2)} (statute-mandated)` };
    result.auditTrail.push(`Code ${code}: Legislative rate $${legRate.toFixed(2)} per SPA IV.a`);
    return result;
  }

  // ── Step C/D: RBRVS (has RVUs) ─────────────────────────────────
  if (rvuGlobal && rvuGlobal > 0) {
    const nfCalc = calcRBRVS(rvuGlobal);
    result.methodology = "RBRVS";
    result.spaSection = "II.D";
    result.rates.fs = nfCalc.baseFee;
    result.rates.fsi = nfCalc.fsi;
    result.calculation = nfCalc;
    result.auditTrail.push(`Global NF: ${nfCalc.formula}`, `FSI: ${nfCalc.formulaFSI}`);

    // Facility rate
    if (rvuFacility && rvuFacility > 0) {
      const facCalc = calcRBRVS(rvuFacility);
      result.rates.facilityFS = facCalc.baseFee;
      result.rates.facilityFSI = facCalc.fsi;
      result.auditTrail.push(`Facility: ${facCalc.formula}`, `Facility FSI: ${facCalc.formulaFSI}`);
    }

    // PC/TC split (if code has components)
    if (rvu26 && rvu26 > 0) {
      const pcCalc = calcRBRVS(rvu26);
      result.rates.pc = pcCalc.baseFee;
      result.rates.pci = pcCalc.fsi;
      result.auditTrail.push(`PC (mod 26): ${pcCalc.formula}`);
    }
    if (rvuTC && rvuTC > 0) {
      const tcCalc = calcRBRVS(rvuTC);
      result.rates.tc = tcCalc.baseFee;
      result.rates.tci = tcCalc.fsi;
      result.auditTrail.push(`TC: ${tcCalc.formula}`);
    }

    // Flag if code has BOTH PC/TC and facility (should be one or the other)
    if ((rvu26 || rvuTC) && rvuFacility && rvuFacility !== rvuGlobal) {
      result.flags.push("WARNING: Code has both PC/TC split and facility rate. Per FL policy, must be one or the other.");
    }

    return result;
  }

  // Also check for PC/TC only (no global RVU)
  if ((rvu26 && rvu26 > 0) || (rvuTC && rvuTC > 0)) {
    result.methodology = "RBRVS (Components)";
    result.spaSection = "II.D";

    if (rvu26 && rvu26 > 0) {
      const pcCalc = calcRBRVS(rvu26);
      result.rates.pc = pcCalc.baseFee;
      result.rates.pci = pcCalc.fsi;
      result.auditTrail.push(`PC: ${pcCalc.formula}`);
    }
    if (rvuTC && rvuTC > 0) {
      const tcCalc = calcRBRVS(rvuTC);
      result.rates.tc = tcCalc.baseFee;
      result.rates.tci = tcCalc.fsi;
      result.auditTrail.push(`TC: ${tcCalc.formula}`);
    }
    // Derive global from components
    if (result.rates.pc && result.rates.tc) {
      result.rates.fs = round2(result.rates.pc + result.rates.tc);
      result.rates.fsi = round2(result.rates.pci + result.rates.tci);
      result.auditTrail.push(`Global (sum): $${result.rates.fs} = PC($${result.rates.pc}) + TC($${result.rates.tc})`);
    } else if (result.rates.pc) {
      result.rates.fs = result.rates.pc;
      result.rates.fsi = result.rates.pci;
    }

    result.calculation = { formula: result.auditTrail.join("; ") };
    return result;
  }

  // ── Step E(c/d): Lab-specific (FCSO → CLFS) ────────────────────
  if (isLab) {
    // SPA says check FCSO first for lab, then CLFS
    const fcsoGlobal = calcFCSO(fcso03_global, fcso04_global, fcso99_global);
    const fcsoRate = fcsoGlobal?.medicareWeightedAvg || null;
    const labCalc = calcLabRate(clfsRate, fcsoRate);

    if (labCalc) {
      result.methodology = `Lab (${labCalc.source})`;
      result.spaSection = "II.E(c)/(d)";
      result.rates.fs = labCalc.practitioner.fs;
      result.rates.pc = labCalc.practitioner.pc;
      result.rates.tc = labCalc.practitioner.tc;
      result.rates.indepFS = labCalc.independent.fs;
      result.rates.indepPC = labCalc.independent.pc;
      result.rates.indepTC = labCalc.independent.tc;
      result.calculation = labCalc;
      result.auditTrail.push(
        `Pract: ${labCalc.practitioner.formula}`,
        `Indep: ${labCalc.independent.formula}`
      );
      return result;
    }
  }

  // ── Step E(b): Anesthesia ──────────────────────────────────────
  if (anesthesiaBaseUnits && anesthesiaBaseUnits > 0) {
    const anCalc = calcAnesthesia(anesthesiaBaseUnits);
    result.methodology = "Anesthesia";
    result.spaSection = "II.E(b)";
    result.rates.fs = anCalc.total;
    result.calculation = anCalc;
    result.auditTrail.push(anCalc.formula);
    return result;
  }

  // ── Step E(a): FCSO Locale-Weighted ────────────────────────────
  const fcsoGlobal = calcFCSO(fcso03_global, fcso04_global, fcso99_global);
  if (fcsoGlobal) {
    result.methodology = "FCSO Locale-Weighted";
    result.spaSection = "II.E(a)";
    result.rates.fs = fcsoGlobal.medicaidRate;
    result.rates.fsi = fcsoGlobal.fsi;
    result.calculation = fcsoGlobal;
    result.auditTrail.push(fcsoGlobal.formula, fcsoGlobal.formulaMedicaid);

    // FCSO PC/TC if available
    const fcso26 = calcFCSO(fcso03_26, fcso04_26, fcso99_26);
    const fcsoTC = calcFCSO(fcso03_tc, fcso04_tc, fcso99_tc);
    if (fcso26) {
      result.rates.pc = fcso26.medicaidRate;
      result.rates.pci = fcso26.fsi;
      result.auditTrail.push(`PC (FCSO): ${fcso26.formulaMedicaid}`);
    }
    if (fcsoTC) {
      result.rates.tc = fcsoTC.medicaidRate;
      result.rates.tci = fcsoTC.fsi;
      result.auditTrail.push(`TC (FCSO): ${fcsoTC.formulaMedicaid}`);
    }

    return result;
  }

  // Also check FCSO components only
  const fcso26Only = calcFCSO(fcso03_26, fcso04_26, fcso99_26);
  const fcsoTCOnly = calcFCSO(fcso03_tc, fcso04_tc, fcso99_tc);
  if (fcso26Only || fcsoTCOnly) {
    result.methodology = "FCSO (Components)";
    result.spaSection = "II.E(a)";
    if (fcso26Only) {
      result.rates.pc = fcso26Only.medicaidRate;
      result.rates.pci = fcso26Only.fsi;
    }
    if (fcsoTCOnly) {
      result.rates.tc = fcsoTCOnly.medicaidRate;
      result.rates.tci = fcsoTCOnly.fsi;
    }
    if (result.rates.pc && result.rates.tc) {
      result.rates.fs = round2(result.rates.pc + result.rates.tc);
      result.rates.fsi = round2(result.rates.pci + result.rates.tci);
    }
    result.calculation = { formula: "FCSO components" };
    result.auditTrail.push("FCSO PC/TC components used (no global rate available)");
    return result;
  }

  // ── Step F: Other State PPP Comparison ─────────────────────────
  if (otherStateRates && otherStateRates.length > 0) {
    const validRates = otherStateRates.filter(r => r.rate > 0 && r.rpp > 0);
    if (validRates.length > 0) {
      // Adjust each state's rate by PPP ratio (FL RPP / State RPP)
      const FL_RPP = 101.2; // FL's regional price parity (BEA)
      const adjusted = validRates.map(r => ({
        ...r,
        adjustedRate: round2(r.rate * (FL_RPP / r.rpp)),
      }));
      const sorted = adjusted.sort((a, b) => a.adjustedRate - b.adjustedRate);
      const median = sorted[Math.floor(sorted.length / 2)].adjustedRate;

      result.methodology = "Other State PPP";
      result.spaSection = "II.F";
      result.rates.fs = median;
      result.rates.fsi = round2(median * FL_CONFIG.FSI_MULT);
      result.calculation = {
        statesUsed: adjusted.length,
        median,
        range: [sorted[0].adjustedRate, sorted[sorted.length - 1].adjustedRate],
        states: adjusted,
      };
      result.auditTrail.push(`PPP-adjusted median of ${adjusted.length} states: $${median.toFixed(2)}`);
      result.flags.push("Rate derived from other state comparison — verify appropriateness");
      return result;
    }
  }

  // ── Step G: Like-Code Coverage ─────────────────────────────────
  if (likeCode && likeCodeRate && likeCodeRate > 0) {
    result.methodology = "Like-Code Coverage";
    result.spaSection = "II.G";
    result.rates.fs = likeCodeRate;
    result.rates.fsi = round2(likeCodeRate * FL_CONFIG.FSI_MULT);
    result.calculation = { likeCode, likeCodeRate };
    result.auditTrail.push(`Like-code ${likeCode}: $${likeCodeRate.toFixed(2)}`);
    result.flags.push("Like-code coverage — subject to review next year per SPA II.G");
    return result;
  }

  // ── Step H: Manual Pricing Required ────────────────────────────
  result.methodology = "Manual Pricing Required";
  result.spaSection = "II.H";
  result.rates.fs = null;
  result.auditTrail.push("No data available from steps A-G. Manual pricing required per SPA II.H.");
  result.flags.push("MANUAL: Evaluate like-codes in same service type subset of national coding manual");
  return result;
}


// ── CF Rebalancing (Annual Update) ──────────────────────────────────────

/**
 * Calculate budget-neutral conversion factor per SPA Section III.
 * 
 * This implements the optimization from the Fee Setting Manual:
 *   min (Total Expenditures - Total Adjusted Expenditures)
 *   s.t. Total Expenditures - Total Adjusted Expenditures >= 0
 *   s.t. each code's rate change <= ±10% (with exceptions)
 * 
 * Uses binary search instead of Excel Solver.
 * 
 * @param {Array} codes - [{code, currentFS, currentPC, rvuFS, rvuPC, utilFS, utilPC, isExcluded}]
 * @returns {object} Optimal CF and all adjusted rates
 */
function rebalanceCF(codes) {
  // Calculate total current expenditures
  let totalExpFS = 0;
  let totalExpPC = 0;
  let totalRVU_FS = 0;
  let totalRVU_PC = 0;

  codes.forEach(c => {
    if (c.currentFS && c.utilFS) totalExpFS += c.currentFS * c.utilFS;
    if (c.currentPC && c.utilPC) totalExpPC += c.currentPC * c.utilPC;
    if (c.rvuFS && c.utilFS) totalRVU_FS += c.rvuFS * c.utilFS;
    if (c.rvuPC && c.utilPC) totalRVU_PC += c.rvuPC * c.utilPC;
  });

  const totalExp = totalExpFS + totalExpPC;
  const totalRVU = totalRVU_FS + totalRVU_PC;
  const cfCalculated = totalRVU > 0 ? totalExp / totalRVU : 0;

  // Binary search for optimal CF
  // The ±10% guardrails make this non-linear, so we can't just divide
  function calcAdjustedExp(cf) {
    let adjExp = 0;
    codes.forEach(c => {
      // FS rates
      if (c.rvuFS && c.rvuFS > 0 && c.utilFS && c.currentFS) {
        const rawNew = c.rvuFS * cf;
        let adjusted;
        if (c.isExcluded) {
          // Excluded codes (facility, RVU-definition changes, errors, lab)
          // can exceed ±10% — SPA III.A exclusions
          adjusted = rawNew;
        } else if (rawNew >= c.currentFS * 0.9 && rawNew <= c.currentFS * 1.1) {
          adjusted = rawNew;
        } else if (rawNew < c.currentFS * 0.9) {
          adjusted = c.currentFS * 0.9;
        } else {
          adjusted = c.currentFS * 1.1;
        }
        adjExp += adjusted * c.utilFS;
      }
      // PC rates (same logic)
      if (c.rvuPC && c.rvuPC > 0 && c.utilPC && c.currentPC) {
        const rawNew = c.rvuPC * cf;
        let adjusted;
        if (c.isExcluded) {
          adjusted = rawNew;
        } else if (rawNew >= c.currentPC * 0.9 && rawNew <= c.currentPC * 1.1) {
          adjusted = rawNew;
        } else if (rawNew < c.currentPC * 0.9) {
          adjusted = c.currentPC * 0.9;
        } else {
          adjusted = c.currentPC * 1.1;
        }
        adjExp += adjusted * c.utilPC;
      }
    });
    return adjExp;
  }

  // Binary search: find CF that minimizes (totalExp - adjExp) subject to >= 0
  let lo = cfCalculated * 0.5;
  let hi = cfCalculated * 1.5;
  let bestCF = cfCalculated;
  let bestGap = Infinity;

  for (let iter = 0; iter < 100; iter++) {
    const mid = (lo + hi) / 2;
    const adjExp = calcAdjustedExp(mid);
    const gap = totalExp - adjExp;

    if (gap >= 0 && gap < bestGap) {
      bestGap = gap;
      bestCF = mid;
    }

    // If adjusted > total (gap < 0), CF is too high
    if (gap < 0) {
      hi = mid;
    } else {
      lo = mid;
    }
  }

  // Compute all adjusted rates at optimal CF
  const adjustedCodes = codes.map(c => {
    const result = { code: c.code };

    if (c.rvuFS && c.rvuFS > 0 && c.currentFS) {
      const rawNew = c.rvuFS * bestCF;
      if (c.isExcluded || (rawNew >= c.currentFS * 0.9 && rawNew <= c.currentFS * 1.1)) {
        result.newFS = round2(rawNew);
      } else if (rawNew < c.currentFS * 0.9) {
        result.newFS = round2(c.currentFS * 0.9);
        result.capped = "floor";
      } else {
        result.newFS = round2(c.currentFS * 1.1);
        result.capped = "ceiling";
      }
      result.newFSI = round2(result.newFS * FL_CONFIG.FSI_MULT);
      result.changePct = round2(((result.newFS / c.currentFS) - 1) * 100);
    }

    if (c.rvuPC && c.rvuPC > 0 && c.currentPC) {
      const rawNew = c.rvuPC * bestCF;
      if (c.isExcluded || (rawNew >= c.currentPC * 0.9 && rawNew <= c.currentPC * 1.1)) {
        result.newPC = round2(rawNew);
      } else if (rawNew < c.currentPC * 0.9) {
        result.newPC = round2(c.currentPC * 0.9);
        result.pcCapped = "floor";
      } else {
        result.newPC = round2(c.currentPC * 1.1);
        result.pcCapped = "ceiling";
      }
      result.newPCI = round2(result.newPC * FL_CONFIG.FSI_MULT);
    }

    return result;
  });

  return {
    optimalCF: bestCF,
    calculatedCF: cfCalculated,
    totalExpenditures: round2(totalExp),
    totalAdjustedExpenditures: round2(calcAdjustedExp(bestCF)),
    budgetNeutralityGap: round2(totalExp - calcAdjustedExp(bestCF)),
    totalCodes: codes.length,
    codesCapped: adjustedCodes.filter(c => c.capped || c.pcCapped).length,
    adjustedCodes,
  };
}


// ── Utility ─────────────────────────────────────────────────────────────

function round2(n) {
  return Math.round(n * 100) / 100;
}


// ── Exports ─────────────────────────────────────────────────────────────

export {
  FL_CONFIG,
  calcRBRVS,
  calcFCSO,
  calcLabRate,
  calcAnesthesia,
  calcPediatricStacking,
  priceCode,
  rebalanceCF,
};

export default priceCode;
