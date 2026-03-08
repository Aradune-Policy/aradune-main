// =============================================================================
// ARADUNE STATE RATE ENGINE v2.0
// =============================================================================
// Generic Medicaid fee schedule calculation engine with multi-state support.
//
// Architecture:
//   - Universal RBRVS core (CF optimization, guardrails, component splits)
//   - Per-state configuration with schedule-type granularity
//   - Automatic validation against published fee schedules
//   - Confidence tiering based on gap analysis
//
// Confidence Tiers (per state):
//   Every state: SPA math → compare to published schedule → gap distribution
//   Tier 1: Gaps flagged but unexplained
//   Tier 2: Patterns detected, systematic corrections inferred
//   Tier 3: Full operational reproduction (FL reference implementation)
//
// Built from:
//   - SPA FL-24-0002 (legal methodology)
//   - Florida 2026 production R scripts (operational ground truth)
//   - Ted Webb's Fee Setting Manual (process documentation)
//
// =============================================================================

// =============================================================================
// SECTION 1: UNIVERSAL RBRVS CORE
// =============================================================================
// These functions are state-agnostic. Any RBRVS-based Medicaid program uses
// some variant of this math.

/**
 * Apply ±X% guardrail constraint to a target rate.
 * Identical logic across all 8 FL schedule scripts.
 * Universal to any RBRVS state with rate change limits.
 *
 * @param {number} target    - New calculated rate
 * @param {number} prior     - Prior year rate
 * @param {boolean} isNew    - New codes are unconstrained
 * @param {number} maxChange - Guardrail percentage (default 0.10 = ±10%)
 * @returns {number}
 */
function applyConstraint(target, prior, isNew = false, maxChange = 0.10) {
  if (isNew) return target;
  if (prior <= 0) return target;

  const ceiling = prior * (1 + maxChange);
  const floor   = prior * (1 - maxChange);

  if (target > ceiling) return ceiling;
  if (target < floor)   return floor;
  return target;
}

/**
 * Calculate RBRVS base fee from RVU, CF, and optional multipliers.
 *
 * @param {number} rvu        - Relative Value Unit (non-facility or facility)
 * @param {number} cf         - Conversion factor
 * @param {number} gpci       - Geographic Practice Cost Index (1.0 for FL)
 * @param {number} fsiMult    - Fee Schedule Increase multiplier (1.04 for FL Prac/Rad, 1.0 for Hearing/Visual)
 * @returns {number}
 */
function calcRBRVS(rvu, cf, gpci = 1.0, fsiMult = 1.0) {
  return gpci * rvu * cf * fsiMult;
}

/**
 * Calculate PC/TC components using the remainder method.
 *
 * CRITICAL RULE (proven in all FL production scripts):
 *   1. Round FSI/FS to 2 decimals FIRST
 *   2. Calculate PC from ratio, round to 2 decimals
 *   3. TC = rounded_global - rounded_PC (NO additional rounding)
 *   This guarantees PC + TC = Global EXACTLY.
 *
 * @param {number} globalFee     - Rounded global fee (FSI or FS)
 * @param {Object} componentInfo - Component calculation parameters
 * @returns {{ pc: number, tc: number }}
 */
function calcComponents(globalFee, componentInfo) {
  const {
    priorPC = 0,
    priorTC = 0,
    pcRVU = 0,
    tcRVU = 0,
    globalRVU = 0,
    isNew = false,
    defaultPCRatio = 0.20,
    defaultTCRatio = 0.80,
  } = componentInfo;

  const hadBoth    = priorPC > 0 && priorTC > 0;
  const hadOnlyPC  = priorPC > 0 && priorTC === 0;
  const hadOnlyTC  = priorPC === 0 && priorTC > 0;
  const hadNeither = priorPC === 0 && priorTC === 0;
  const hasBothRVUs = pcRVU > 0 && tcRVU > 0 && globalRVU > 0;

  // Determine if code should have components
  const shouldHavePC = hadBoth || hadOnlyPC || (hasBothRVUs && (isNew || hadNeither));
  const shouldHaveTC = hadBoth || hadOnlyTC || (hasBothRVUs && (isNew || hadNeither));

  if (!shouldHavePC && !shouldHaveTC) {
    return { pc: 0, tc: 0 };
  }

  // Determine PC ratio
  let pcRatio;
  if (hasBothRVUs) {
    pcRatio = pcRVU / globalRVU;
  } else if (hadBoth) {
    const sum = priorPC + priorTC;
    pcRatio = sum > 0 ? priorPC / sum : defaultPCRatio;
  } else if (hadOnlyPC) {
    pcRatio = 1.0;
  } else {
    pcRatio = defaultPCRatio;
  }

  // Calculate PC (rounded)
  let pc = 0;
  if (shouldHavePC && !hadOnlyTC) {
    pc = round2(globalFee * pcRatio);
  }

  // TC = remainder (NOT independently calculated — this is the key insight)
  let tc = 0;
  if (shouldHaveTC && !hadOnlyPC) {
    if (hadBoth || (isNew && hasBothRVUs)) {
      tc = globalFee - pc; // Remainder method — guarantees exact sum
    } else if (hadOnlyTC) {
      // TCI-only codes: use TC ratio
      let tcRatio = hasBothRVUs ? tcRVU / globalRVU : defaultTCRatio;
      tc = round2(globalFee * tcRatio);
    } else {
      tc = globalFee - pc;
    }
  }

  return { pc, tc };
}

/**
 * Binary search CF optimizer with ±X% guardrails.
 *
 * Replicates Excel Solver / R binary search from FL production scripts.
 * Given utilization-weighted codes with current fees and new RVUs,
 * find CF such that total adjusted expenditures ≈ total current expenditures.
 *
 * @param {Array} codes       - Array of { rvu, currentFee, units, isFixed }
 * @param {number} targetMoney - Total current expenditures to match
 * @param {Object} options
 * @returns {{ cf: number, adjustedMoney: number, iterations: number, detail: Array }}
 */
function optimizeCF(codes, targetMoney, options = {}) {
  const {
    maxChange = 0.10,
    cfLow     = 15.0,
    cfHigh    = 45.0,
    tolerance = 1.0,
    maxIter   = 60,
  } = options;

  // Separate fixed codes (no RVU — their money doesn't change with CF)
  const variable = codes.filter(c => c.rvu > 0 && !c.isFixed);
  const fixedMoney = codes
    .filter(c => c.rvu === 0 || c.isFixed)
    .reduce((sum, c) => sum + (c.currentFee * c.units), 0);

  function calcAdjustedMoney(cf) {
    let total = fixedMoney;
    for (const code of variable) {
      const target = code.rvu * cf;
      const ceiling = code.currentFee * (1 + maxChange);
      const floor   = code.currentFee * (1 - maxChange);
      const adjusted = Math.max(Math.min(target, ceiling), floor);
      total += adjusted * code.units;
    }
    return total;
  }

  let lo = cfLow;
  let hi = cfHigh;
  let mid, adjusted, diff;
  let i;

  for (i = 0; i < maxIter; i++) {
    mid = (lo + hi) / 2;
    adjusted = calcAdjustedMoney(mid);
    diff = adjusted - targetMoney;

    if (Math.abs(diff) < tolerance) break;

    if (diff > 0) {
      hi = mid;
    } else {
      lo = mid;
    }
  }

  // Generate per-code detail
  const detail = variable.map(code => {
    const target = code.rvu * mid;
    const ceiling = code.currentFee * (1 + maxChange);
    const floor   = code.currentFee * (1 - maxChange);
    const adjustedFee = Math.max(Math.min(target, ceiling), floor);
    let status = 'unconstrained';
    if (Math.abs(adjustedFee - floor) < 0.01) status = 'at_floor';
    if (Math.abs(adjustedFee - ceiling) < 0.01) status = 'at_ceiling';
    return { ...code, targetFee: target, adjustedFee, status };
  });

  return {
    cf: mid,
    adjustedMoney: adjusted,
    budgetDiff: diff,
    iterations: i + 1,
    fixedMoney,
    variableCodes: variable.length,
    detail,
  };
}

/**
 * FCSO locale-weighted average with proportional redistribution.
 * When a locale has no rate, redistribute its weight proportionally
 * among locales that DO have rates. (SPA FL-24-0002 page 4)
 *
 * @param {Object} rates   - { locale_code: rate } (e.g., { '03': 45.00, '04': null, '99': 50.00 })
 * @param {Object} weights - { locale_code: weight } (must sum to ~1.0)
 * @returns {{ weightedAvg: number, localesUsed: number, redistributed: boolean }}
 */
function calcFCSOWeightedAvg(rates, weights) {
  const available = Object.entries(rates)
    .filter(([, r]) => r != null && r > 0)
    .map(([locale, rate]) => ({ locale, rate, weight: weights[locale] || 0 }));

  if (available.length === 0) return { weightedAvg: 0, localesUsed: 0, redistributed: false };
  if (available.length === 1) return { weightedAvg: available[0].rate, localesUsed: 1, redistributed: false };

  // Proportional redistribution of missing locale weights
  const totalAvailableWeight = available.reduce((s, l) => s + l.weight, 0);
  const redistributed = available.length < Object.keys(weights).length;

  let weightedSum = 0;
  for (const loc of available) {
    const adjustedWeight = totalAvailableWeight > 0 ? loc.weight / totalAvailableWeight : 1 / available.length;
    weightedSum += loc.rate * adjustedWeight;
  }

  return { weightedAvg: weightedSum, localesUsed: available.length, redistributed };
}

/**
 * Calculate anesthesia fee per SPA methodology.
 *
 * @param {number} baseUnits
 * @param {number} timeMinutes
 * @param {number} timeRate      - Per 15-min increment rate (FL: $14.50)
 * @param {boolean} isPediatric  - Under 21: 4% increase
 * @returns {{ fee: number, timeUnits: number }}
 */
function calcAnesthesia(baseUnits, timeMinutes, timeRate, isPediatric = false) {
  const timeUnits = Math.floor(timeMinutes / 15); // Rounded DOWN per SPA
  let fee = (baseUnits * timeRate) + (timeUnits * timeRate);
  if (isPediatric) fee *= 1.04;
  return { fee: round2(fee), timeUnits };
}


// =============================================================================
// SECTION 2: GAP ANALYSIS & CONFIDENCE ENGINE
// =============================================================================
// This is the core differentiator. Every state gets validated against
// published fee schedules. The gap distribution determines confidence.

/**
 * Validate calculated rates against a published fee schedule.
 * Returns gap analysis with confidence metrics.
 *
 * @param {Array} calculated  - Array of { code, mod?, calculatedRate, rateField }
 * @param {Array} published   - Array of { code, mod?, publishedRate, rateField }
 * @param {Object} options
 * @returns {Object} Gap analysis results
 */
function validateAgainstPublished(calculated, published, options = {}) {
  const {
    tolerance  = 0.01,  // $0.01 = exact match
    nearMatch  = 0.50,  // $0.50 = close enough to not worry
    rateField  = 'fsi', // Which rate to compare
  } = options;

  // Build lookup from published
  const pubLookup = new Map();
  for (const p of published) {
    const key = `${p.code}|${p.mod || ''}|${rateField}`;
    pubLookup.set(key, p.publishedRate || p[rateField] || 0);
  }

  const results = [];
  let exactMatches = 0;
  let nearMatches = 0;
  let totalCompared = 0;
  const gapValues = [];
  const gapPercents = [];

  for (const calc of calculated) {
    const key = `${calc.code}|${calc.mod || ''}|${rateField}`;
    const pubRate = pubLookup.get(key);

    if (pubRate == null || pubRate === 0) continue;
    if (calc.calculatedRate == null || calc.calculatedRate === 0) continue;

    totalCompared++;
    const gap = calc.calculatedRate - pubRate;
    const gapPct = pubRate > 0 ? (gap / pubRate) * 100 : 0;

    gapValues.push(gap);
    gapPercents.push(gapPct);

    const isExact = Math.abs(gap) <= tolerance;
    const isNear  = Math.abs(gap) <= nearMatch;
    if (isExact) exactMatches++;
    if (isNear) nearMatches++;

    results.push({
      code: calc.code,
      mod: calc.mod || '',
      calculated: calc.calculatedRate,
      published: pubRate,
      gap: round2(gap),
      gapPct: Math.round(gapPct * 1000) / 1000,
      match: isExact ? 'exact' : isNear ? 'near' : 'divergent',
    });
  }

  // Statistical analysis of gaps
  const stats = calcGapStatistics(gapPercents);

  // Pattern detection
  const patterns = detectGapPatterns(results, stats);

  // Confidence score (0-100)
  const confidence = calcConfidenceScore(exactMatches, nearMatches, totalCompared, stats);

  // Infer tier
  const tier = confidence >= 95 ? 3 : confidence >= 80 ? 2 : 1;

  return {
    summary: {
      totalCompared,
      exactMatches,
      nearMatches,
      exactMatchRate: totalCompared > 0 ? round4(exactMatches / totalCompared) : 0,
      nearMatchRate:  totalCompared > 0 ? round4(nearMatches / totalCompared) : 0,
      confidence,
      tier,
      tierLabel: tier === 3 ? 'operational' : tier === 2 ? 'validated' : 'spa_derived',
    },
    statistics: stats,
    patterns,
    divergentCodes: results.filter(r => r.match === 'divergent'),
    allResults: results,
  };
}

/**
 * Calculate gap distribution statistics.
 */
function calcGapStatistics(gapPercents) {
  if (gapPercents.length === 0) return { mean: 0, median: 0, stdDev: 0, skew: 0 };

  const sorted = [...gapPercents].sort((a, b) => a - b);
  const n = sorted.length;
  const mean = sorted.reduce((s, v) => s + v, 0) / n;
  const median = n % 2 === 0
    ? (sorted[n / 2 - 1] + sorted[n / 2]) / 2
    : sorted[Math.floor(n / 2)];

  const variance = sorted.reduce((s, v) => s + (v - mean) ** 2, 0) / n;
  const stdDev = Math.sqrt(variance);

  const skew = n > 2 && stdDev > 0
    ? sorted.reduce((s, v) => s + ((v - mean) / stdDev) ** 3, 0) / n
    : 0;

  const p5  = sorted[Math.floor(n * 0.05)] || sorted[0];
  const p25 = sorted[Math.floor(n * 0.25)] || sorted[0];
  const p75 = sorted[Math.floor(n * 0.75)] || sorted[n - 1];
  const p95 = sorted[Math.floor(n * 0.95)] || sorted[n - 1];

  return { mean, median, stdDev, skew, p5, p25, p75, p95, n };
}

/**
 * Detect patterns in gap distribution that suggest specific methodology differences.
 *
 * Key patterns from FL experience:
 *   - Uniform ~4% offset → missing FSI multiplier (Hearing/Visual)
 *   - Bimodal distribution → two code populations treated differently
 *   - Scattered outliers → legislative overrides
 *   - Systematic offset in one code range → separate CF (Lab)
 */
function detectGapPatterns(results, stats) {
  const patterns = [];

  // Pattern 1: Uniform offset (suggests missing/extra multiplier)
  if (stats.stdDev < 1.0 && Math.abs(stats.mean) > 1.0) {
    const inferredMultiplier = 1 + (stats.mean / 100);
    patterns.push({
      type: 'uniform_offset',
      description: `Systematic ${stats.mean > 0 ? 'over' : 'under'}pricing of ~${Math.abs(stats.mean).toFixed(1)}%`,
      inference: `Possible missing/extra multiplier of ~${inferredMultiplier.toFixed(4)}`,
      confidence: 'high',
      correction: { type: 'multiplier', value: 1 / inferredMultiplier },
    });
  }

  // Pattern 2: Bimodal (two distinct populations)
  if (stats.stdDev > 3.0 && Math.abs(stats.skew) < 0.5) {
    const nearZero = results.filter(r => Math.abs(r.gapPct) < 2);
    const farGroup = results.filter(r => Math.abs(r.gapPct) >= 2);
    if (nearZero.length > results.length * 0.3 && farGroup.length > results.length * 0.3) {
      patterns.push({
        type: 'bimodal',
        description: `Two distinct code populations: ${nearZero.length} matching, ${farGroup.length} divergent`,
        inference: 'Likely schedule-type or code-range specific methodology difference',
        confidence: 'medium',
        nearZeroCount: nearZero.length,
        divergentCount: farGroup.length,
      });
    }
  }

  // Pattern 3: Outlier cluster (legislative overrides)
  const outliers = results.filter(r => Math.abs(r.gapPct) > 15);
  if (outliers.length > 0 && outliers.length < results.length * 0.05) {
    patterns.push({
      type: 'outlier_cluster',
      description: `${outliers.length} codes with >15% gap (likely legislative overrides)`,
      inference: 'These codes probably have statute-mandated rates',
      confidence: 'high',
      codes: outliers.map(o => ({ code: o.code, mod: o.mod, gap: o.gapPct })),
    });
  }

  // Pattern 4: Code-range specific offset (separate CF)
  const codeRanges = {};
  for (const r of results) {
    const prefix = r.code.substring(0, 2);
    if (!codeRanges[prefix]) codeRanges[prefix] = [];
    codeRanges[prefix].push(r.gapPct);
  }
  for (const [prefix, gaps] of Object.entries(codeRanges)) {
    if (gaps.length < 10) continue;
    const rangeMean = gaps.reduce((s, v) => s + v, 0) / gaps.length;
    const rangeStd  = Math.sqrt(gaps.reduce((s, v) => s + (v - rangeMean) ** 2, 0) / gaps.length);
    if (rangeStd < 2.0 && Math.abs(rangeMean - stats.mean) > 3.0) {
      patterns.push({
        type: 'range_specific_offset',
        description: `Codes ${prefix}xxx have distinct offset of ~${rangeMean.toFixed(1)}% (vs overall ${stats.mean.toFixed(1)}%)`,
        inference: `Possible separate conversion factor or methodology for ${prefix}xxx range`,
        confidence: 'medium',
        codePrefix: prefix,
        rangeMean,
        rangeStd,
        codeCount: gaps.length,
      });
    }
  }

  return patterns;
}

/**
 * Calculate overall confidence score (0-100).
 */
function calcConfidenceScore(exactMatches, nearMatches, total, stats) {
  if (total === 0) return 0;

  const exactRate = exactMatches / total;
  const nearRate  = nearMatches / total;

  // Weighted score: exact matches matter most, then near, then low variance
  let score = (exactRate * 70) + (nearRate * 20) + (stats.stdDev < 2 ? 10 : 0);
  return Math.min(100, Math.round(score));
}


// =============================================================================
// SECTION 3: SCHEDULE-TYPE PRICING
// =============================================================================

/**
 * Process a single code through a schedule's methodology.
 */
function priceCodeForSchedule(code, scheduleConfig, stateConfig) {
  const audit = [];
  const flags = [];
  const result = {
    code: code.procedure,
    mod: code.mod || '',
    schedule: scheduleConfig.name,
    methodology: null,
    spaSection: null,
    rates: {},
  };

  const cf   = scheduleConfig.cf || stateConfig.cf;
  const fsi  = scheduleConfig.applyFSI ? (stateConfig.fsiMultiplier || 1.04) : 1.0;
  const gpci = stateConfig.gpci || 1.0;
  const maxChange = stateConfig.maxChange || 0.10;

  // Step 0: Legislative overrides
  if (scheduleConfig.legislativeOverrides) {
    const overrideKey = `${code.procedure}|${code.mod || ''}`;
    const override = scheduleConfig.legislativeOverrides[overrideKey]
                  || scheduleConfig.legislativeOverrides[code.procedure];
    if (override) {
      result.methodology = 'legislative_override';
      result.spaSection = override.spaSection || 'Statutory';
      result.rates = { ...override.rates };
      audit.push(`Legislative override: ${override.statute || 'statute-mandated'}`);
      return { ...result, audit, flags };
    }
  }

  // Derived schedules (Midwife, Indep Lab)
  if (scheduleConfig.type === 'derived') {
    return priceDerivedCode(code, scheduleConfig, stateConfig, audit, flags);
  }

  // Flat-rate schedules (Dental)
  if (scheduleConfig.type === 'flat') {
    result.methodology = 'flat_rate';
    result.spaSection = scheduleConfig.spaSection || 'Statutory';
    result.rates.fs = code.priorFS || 0;
    audit.push('Flat rate schedule — maintained from prior year');
    return { ...result, audit, flags };
  }

  // Lab schedule has its own path logic
  if (scheduleConfig.type === 'lab') {
    return priceLabCode(code, scheduleConfig, stateConfig, audit, flags);
  }

  // RBRVS calculation
  const hasRVU = (code.nonFacRVU || code.globalRVU || 0) > 0;
  const hasFacRVU = (code.facRVU || 0) > 0;

  if (hasRVU) {
    const rvu = code.nonFacRVU || code.globalRVU;
    const targetFS = calcRBRVS(rvu, cf, gpci, fsi);
    const priorFS = code.priorFSI || code.priorFS || 0;

    const constrainedFS = applyConstraint(targetFS, priorFS, code.isNew, maxChange);
    const roundedFS = round2(constrainedFS);

    result.methodology = 'RBRVS';
    result.spaSection = 'II.D';
    result.rates.fs = roundedFS;
    audit.push(`${scheduleConfig.applyFSI ? 'FSI' : 'FS'} = RVU(${rvu}) × CF($${cf.toFixed(4)})${fsi !== 1.0 ? ` × ${fsi}` : ''}${gpci !== 1.0 ? ` × GPCI(${gpci})` : ''} = $${targetFS.toFixed(4)}`);

    if (roundedFS !== round2(targetFS)) {
      const status = roundedFS > priorFS ? 'ceiling' : 'floor';
      audit.push(`Constrained to ±${maxChange * 100}% ${status}: $${priorFS.toFixed(2)} → $${roundedFS.toFixed(2)}`);
    }

    // Facility fee
    if (scheduleConfig.facilityRules) {
      const shouldHaveFacility = scheduleConfig.facilityRules.onlyIfPrior
        ? (code.priorFacility || 0) > 0
        : hasFacRVU;

      if (shouldHaveFacility && hasFacRVU) {
        const facFSI = scheduleConfig.facilityRules.applyFSI !== false ? fsi : 1.0;
        const targetFac = calcRBRVS(code.facRVU, cf, gpci, facFSI);
        const constrainedFac = applyConstraint(targetFac, code.priorFacility || 0, code.isNew, maxChange);
        result.rates.facility = round2(constrainedFac);
        audit.push(`Facility = FacRVU(${code.facRVU}) × CF × ${facFSI} = $${targetFac.toFixed(4)} → $${result.rates.facility}`);
      }
    }

    // PC/TC components
    if (scheduleConfig.componentRules) {
      const components = calcComponents(roundedFS, {
        priorPC: code.priorPCI || code.priorPC || 0,
        priorTC: code.priorTCI || code.priorTC || 0,
        pcRVU: code.pcRVU || 0,
        tcRVU: code.tcRVU || 0,
        globalRVU: rvu,
        isNew: code.isNew,
        defaultPCRatio: scheduleConfig.componentRules.defaultPCRatio || 0.20,
        defaultTCRatio: scheduleConfig.componentRules.defaultTCRatio || 0.80,
      });

      if (components.pc > 0) {
        if (!code.isNew && (code.priorPCI || code.priorPC || 0) > 0 && scheduleConfig.componentRules.constrainComponents) {
          result.rates.pc = round2(applyConstraint(components.pc, code.priorPCI || code.priorPC, false, maxChange));
          // Recalculate TC as remainder after constrained PC
          if (components.tc > 0) {
            result.rates.tc = roundedFS - result.rates.pc;
          }
        } else {
          result.rates.pc = components.pc;
          if (components.tc > 0) result.rates.tc = components.tc;
        }
      } else if (components.tc > 0) {
        if (!code.isNew && (code.priorTCI || code.priorTC || 0) > 0 && scheduleConfig.componentRules.constrainComponents) {
          result.rates.tc = round2(applyConstraint(components.tc, code.priorTCI || code.priorTC, false, maxChange));
        } else {
          result.rates.tc = components.tc;
        }
      }

      if (result.rates.pc > 0 || result.rates.tc > 0) {
        audit.push(`Components: PC=$${(result.rates.pc || 0).toFixed(2)}, TC=$${(result.rates.tc || 0).toFixed(2)} (sum=$${((result.rates.pc || 0) + (result.rates.tc || 0)).toFixed(2)})`);
      }
    }

    return { ...result, audit, flags };
  }

  // FCSO locale-weighted (if configured)
  if (scheduleConfig.fcsoRules && code.fcsoRates) {
    const { weightedAvg, localesUsed, redistributed } = calcFCSOWeightedAvg(
      code.fcsoRates,
      stateConfig.localeWeights || {}
    );

    if (weightedAvg > 0) {
      const medicaidRate = weightedAvg * (stateConfig.medicaidMultiplier || 0.60);
      const fsiRate = medicaidRate * fsi;
      const priorFS = code.priorFSI || code.priorFS || 0;
      const constrained = applyConstraint(fsiRate, priorFS, code.isNew, maxChange);

      result.methodology = 'FCSO_locale_weighted';
      result.spaSection = 'II.E(a)';
      result.rates.fs = round2(constrained);
      audit.push(`FCSO: weighted avg=$${weightedAvg.toFixed(2)} × ${stateConfig.medicaidMultiplier} × ${fsi} = $${fsiRate.toFixed(2)}`);
      if (redistributed) {
        audit.push(`Weight redistributed (only ${localesUsed} of ${Object.keys(stateConfig.localeWeights).length} locales had rates)`);
      }
      return { ...result, audit, flags };
    }
  }

  // Maintain prior rate
  result.methodology = 'prior_maintained';
  result.spaSection = 'N/A';
  result.rates.fs = code.priorFSI || code.priorFS || 0;
  flags.push('NO_SOURCE: No RVU, no FCSO — maintaining prior year rate');
  audit.push('No pricing source available — maintaining prior year rate');
  return { ...result, audit, flags };
}

/**
 * Price a lab code (4-path methodology from FL Practitioner Lab script).
 */
function priceLabCode(code, scheduleConfig, stateConfig, audit, flags) {
  const labCF = scheduleConfig.cf || stateConfig.labCF;
  const medicareMult = stateConfig.medicaidMultiplier || 0.60;
  const maxChange = stateConfig.maxChange || 0.10;

  const result = {
    code: code.procedure,
    mod: code.mod || '',
    schedule: scheduleConfig.name,
    methodology: null,
    spaSection: null,
    rates: {},
  };

  const hasRVU = (code.rvu || code.globalRVU || 0) > 0;
  const hasMedicare = (code.medicareRate || 0) > 0;
  const inUtil = code.inUtilization !== false;
  const rvu = code.rvu || code.globalRVU || 0;

  // Path determination
  let path;
  if (!inUtil) path = 'maintain';
  else if (hasRVU) path = 'rvu';
  else if (hasMedicare) path = 'medicare';
  else path = 'maintain';

  let constrainedFS;

  switch (path) {
    case 'rvu': {
      const targetFS = labCF * rvu;
      constrainedFS = applyConstraint(targetFS, code.priorFS || 0, code.isNew, maxChange);
      if (hasMedicare && constrainedFS > code.medicareRate) {
        constrainedFS = code.medicareRate;
        audit.push(`Capped at Medicare rate: $${code.medicareRate.toFixed(2)}`);
      }
      result.methodology = 'lab_RBRVS';
      result.spaSection = 'II.E(c)';
      audit.push(`Lab RBRVS: RVU(${rvu}) × LabCF($${labCF.toFixed(4)}) = $${targetFS.toFixed(4)}`);
      break;
    }
    case 'medicare': {
      const targetFS = code.medicareRate * medicareMult;
      constrainedFS = applyConstraint(targetFS, code.priorFS || 0, code.isNew, maxChange);
      result.methodology = 'lab_medicare';
      result.spaSection = 'II.E(c)';
      audit.push(`Lab Medicare: $${code.medicareRate.toFixed(2)} × ${medicareMult} = $${targetFS.toFixed(2)}`);
      break;
    }
    default:
      constrainedFS = code.priorFS || 0;
      result.methodology = inUtil ? 'lab_no_source' : 'lab_no_utilization';
      result.spaSection = 'II.E(c)';
      audit.push(inUtil ? 'No RVU or Medicare rate — maintaining prior' : 'Not in utilization — maintaining prior');
      break;
  }

  result.rates.fs = round2(constrainedFS);

  // PC/TC for lab codes
  if (scheduleConfig.componentRules) {
    const priorPC = code.priorPC || 0;
    const priorTC = code.priorTC || 0;
    const hadBoth = priorPC > 0 && priorTC > 0;
    const isZeroSeries = code.procedure.startsWith('0');
    const isStandardCLFS = code.isStandardCLFS || false;
    const shouldHavePCTC = hadBoth || priorPC > 0 || priorTC > 0 ||
      (code.isNew && isStandardCLFS && !isZeroSeries);

    if (shouldHavePCTC) {
      const fsRounded = result.rates.fs;
      const pcRatio = hadBoth && priorPC + priorTC > 0
        ? priorPC / (priorPC + priorTC)
        : (scheduleConfig.componentRules.defaultPCRatio || 0.20);

      result.rates.pc = round2(fsRounded * pcRatio);
      result.rates.tc = fsRounded - result.rates.pc;
      audit.push(`Lab PC/TC: ${round2(pcRatio * 100)}%/${round2((1 - pcRatio) * 100)}% → PC=$${result.rates.pc}, TC=$${result.rates.tc}`);
    }

    if (code.isNew && isZeroSeries) {
      audit.push('0-series PLA code: no PC/TC (not on standard CLFS)');
    }
  }

  return { ...result, audit, flags };
}

/**
 * Price a derived-schedule code (Midwife from Practitioner, Indep Lab from Pract Lab).
 */
function priceDerivedCode(code, scheduleConfig, stateConfig, audit, flags) {
  const result = {
    code: code.procedure,
    mod: code.mod || '',
    schedule: scheduleConfig.name,
    methodology: `derived_from_${scheduleConfig.sourceSchedule}`,
    spaSection: scheduleConfig.spaSection || 'II.I',
    rates: {},
  };

  const sourceRate = code.sourceFS || code.sourceFSI || 0;
  const multiplier = scheduleConfig.derivedMultiplier || 1.0;
  const removeFSI  = scheduleConfig.removeFSIBeforeDerivation || false;
  const fsiMult    = stateConfig.fsiMultiplier || 1.04;

  if (sourceRate > 0) {
    const base = removeFSI ? sourceRate / fsiMult : sourceRate;
    result.rates.fs = round2(base * multiplier);
    audit.push(`Derived: ${removeFSI ? `($${sourceRate.toFixed(2)} / ${fsiMult})` : `$${sourceRate.toFixed(2)}`} × ${multiplier} = $${result.rates.fs}`);

    if (code.sourceFacility > 0 && scheduleConfig.deriveFacility) {
      const facBase = removeFSI ? code.sourceFacility / fsiMult : code.sourceFacility;
      result.rates.facility = round2(facBase * multiplier);
    }
  } else {
    result.rates.fs = code.priorFS || code.priorFC || 0;
    flags.push('NO_SOURCE: Source schedule rate not found — maintaining prior');
  }

  return { ...result, audit, flags };
}


// =============================================================================
// SECTION 4: CROSSWALK ENGINE
// =============================================================================

/**
 * Apply utilization crosswalks to historical utilization data.
 */
function applyCrosswalks(utilization, crosswalks, options = {}) {
  const { mode = 'expanded' } = options;

  const utilMap = new Map();
  for (const u of utilization) {
    const existing = utilMap.get(u.code) || { code: u.code, units: 0, originalUnits: 0, action: 'unchanged' };
    existing.units += u.units;
    existing.originalUnits += u.units;
    utilMap.set(u.code, existing);
  }

  const actions = [];

  for (const xw of crosswalks) {
    switch (xw.type) {
      case 'redistribute': {
        const deletedUtil = xw.deletedCodes.reduce((sum, c) => sum + (utilMap.get(c)?.units || 0), 0);
        const allCodes = [...xw.deletedCodes, ...xw.newCodes];
        const perCode = deletedUtil / allCodes.length;

        for (const c of allCodes) {
          if (!utilMap.has(c)) utilMap.set(c, { code: c, units: 0, originalUnits: 0, action: 'new' });
          const entry = utilMap.get(c);
          entry.units = perCode;
          entry.action = xw.deletedCodes.includes(c) ? `${xw.name}_redistributed` : `${xw.name}_new`;
        }
        actions.push({ crosswalk: xw.name, type: 'redistribute', unitsRedistributed: deletedUtil, codesAffected: allCodes.length });
        break;
      }
      case 'one_to_one': {
        for (const mapping of xw.mappings) {
          const sourceEntry = utilMap.get(mapping.source);
          const sourceUtil = sourceEntry ? sourceEntry.units : 0;
          if (sourceUtil > 0) {
            if (!utilMap.has(mapping.target)) utilMap.set(mapping.target, { code: mapping.target, units: 0, originalUnits: 0, action: 'new' });
            utilMap.get(mapping.target).units += sourceUtil;
            utilMap.get(mapping.target).action = `${xw.name}_target`;
            sourceEntry.units = 0;
            sourceEntry.action = `${xw.name}_source`;
          }
        }
        actions.push({ crosswalk: xw.name, type: 'one_to_one', mappings: xw.mappings.length });
        break;
      }
      case 'proportional': {
        if (mode === 'ghost') {
          for (const c of xw.deletedCodes) { const e = utilMap.get(c); if (e) e.action = 'ghost'; }
          actions.push({ crosswalk: xw.name, type: 'ghost', codes: xw.deletedCodes.length });
        } else {
          const deletedUtil = xw.deletedCodes.reduce((sum, c) => sum + (utilMap.get(c)?.units || 0), 0);
          const baseTotalUtil = xw.baseCodes.reduce((sum, c) => sum + (utilMap.get(c)?.units || 0), 0);
          for (const c of xw.baseCodes) {
            const entry = utilMap.get(c);
            if (entry && baseTotalUtil > 0) {
              entry.units += deletedUtil * (entry.units / baseTotalUtil);
              entry.action = `${xw.name}_target`;
            }
          }
          for (const c of xw.deletedCodes) { const e = utilMap.get(c); if (e) { e.units = 0; e.action = `${xw.name}_source`; } }
          actions.push({ crosswalk: xw.name, type: 'proportional', unitsRedistributed: deletedUtil });
        }
        break;
      }
      case 'exclude': {
        const label = mode === 'ghost' ? 'ghost' : 'excluded';
        for (const c of xw.codes) { const e = utilMap.get(c); if (e) e.action = label; }
        actions.push({ crosswalk: xw.name, type: 'exclude', codes: xw.codes.length });
        break;
      }
    }
  }

  const adjusted = Array.from(utilMap.values()).filter(u => u.units > 0 && u.action !== 'excluded');

  return {
    adjusted,
    summary: {
      originalCodes: utilization.length,
      adjustedCodes: adjusted.length,
      originalUnits: utilization.reduce((s, u) => s + u.units, 0),
      adjustedUnits: adjusted.reduce((s, u) => s + u.units, 0),
      actions,
    },
  };
}


// =============================================================================
// SECTION 5: FLORIDA TIER 3 CONFIGURATION
// =============================================================================

const FL_CONFIG = {
  state: 'FL',
  spaReference: 'FL-24-0002',
  effectiveDate: '2026-01-01',
  tier: 3,
  confidence: 98,

  cf: 24.9779582769,
  labCF: 26.1689186096,
  priorCF: 24.550422,
  priorLabCF: 25.5292372731359,
  gpci: 1.0,
  fsiMultiplier: 1.04,
  medicaidMultiplier: 0.60,
  indepLabMultiplier: 0.90,
  maxChange: 0.10,

  localeWeights: { '03': 0.238319, '04': 0.124225, '99': 0.637456 },

  schedules: {
    practitioner: {
      name: 'Practitioner', type: 'rbrvs', applyFSI: true, cf: null, spaSection: 'II.D',
      facilityRules: { onlyIfPrior: true, applyFSI: true },
      componentRules: { method: 'remainder', defaultPCRatio: 0.20, defaultTCRatio: 0.80, constrainComponents: true },
      legislativeOverrides: {
        '99212|':   { rates: { fs: 28.14, facility: 19.23 }, statute: 'Ch.99-223 + stacked' },
        '99212|TH': { rates: { fs: 28.14 }, statute: 'Ch.99-223 + stacked' },
        '99213|':   { rates: { fs: 34.29, facility: 32.58 }, statute: 'Ch.99-223 + stacked' },
        '99213|TH': { rates: { fs: 34.29 }, statute: 'Ch.99-223 + stacked' },
        '99214|':   { rates: { fs: 53.43, facility: 50.77 }, statute: 'Ch.99-223 + stacked' },
        '99214|TH': { rates: { fs: 53.43 }, statute: 'Ch.99-223 + stacked' },
        '99468': { rates: { fs: 650.55 }, statute: 'Legislative' },
        '99469': { rates: { fs: 261.62 }, statute: 'Legislative' },
        '99471': { rates: { fs: 565.82 }, statute: 'Legislative' },
        '99472': { rates: { fs: 269.94 }, statute: 'Legislative' },
        '99477': { rates: { fs: 231.52 }, statute: 'Legislative' },
        '99478': { rates: { fs: 91.90 }, statute: 'Legislative' },
        '99479': { rates: { fs: 81.73 }, statute: 'Legislative' },
        '99480': { rates: { fs: 78.95 }, statute: 'Legislative' },
        'H1000':    { rates: { fs: 52.00 }, statute: 'Legislative' },
        'H1001|':   { rates: { fs: 114.40 }, statute: 'Legislative (V3 corrected)' },
        'H1001|TG': { rates: { fs: 140.40 }, statute: 'Legislative (V3 corrected)' },
        '01967': { rates: { fs: 102.34 }, statute: 'Legislative' },
        '01968': { rates: { fs: 73.07 }, statute: 'Legislative' },
        '01969': { rates: { fs: 102.34 }, statute: 'Legislative' },
        'Q4186': { rates: { fs: 179.00 }, statute: 'Legislative' },
        'Q4187': { rates: { fs: 179.00 }, statute: 'Legislative' },
        'S8415': { rates: { fs: 247.33 }, statute: 'Legislative' },
        '59409|*':  { rates: { fs: 1029.60 }, statute: 'Milliman CY25' },
        '59409|':   { rates: { fs: 772.20 }, statute: 'Milliman CY25' },
        '59410|*':  { rates: { fs: 1144.00 }, statute: 'Milliman CY25' },
        '59410|TH': { rates: { fs: 1029.60 }, statute: 'Milliman CY25' },
        '59410|':   { rates: { fs: 915.20 }, statute: 'Milliman CY25' },
        '59514|*':  { rates: { fs: 1029.60 }, statute: 'Milliman CY25' },
        '59514|':   { rates: { fs: 623.00 }, statute: 'Milliman CY25' },
        '59515|*':  { rates: { fs: 1029.60 }, statute: 'Milliman CY25' },
        '59515|':   { rates: { fs: 963.73 }, statute: 'Milliman CY25' },
        '59612|*':  { rates: { fs: 1029.60 }, statute: 'Milliman CY25' },
        '59612|':   { rates: { fs: 772.20 }, statute: 'Milliman CY25' },
        '59614|*':  { rates: { fs: 1029.60 }, statute: 'Milliman CY25' },
        '59614|':   { rates: { fs: 870.20 }, statute: 'Milliman CY25' },
        '59622|*':  { rates: { fs: 1029.60 }, statute: 'Milliman CY25' },
        '59622|':   { rates: { fs: 988.11 }, statute: 'Milliman CY25' },
        '59430|':   { rates: { fs: 45.76, facility: 47.82 }, statute: 'Milliman CY25' },
      },
      outputColumns: ['Procedure', 'Mod', 'FSI', 'Facility', 'PCI', 'TCI', 'PA'],
      expectedCodeCount: 6677,
    },
    radiology: {
      name: 'Radiology', type: 'rbrvs', applyFSI: true, cf: null, spaSection: 'II.D',
      facilityRules: null,
      componentRules: { method: 'remainder', defaultPCRatio: 0.20, defaultTCRatio: 0.80, constrainComponents: false },
      outputColumns: ['Procedure', 'Mod', 'FSI', 'PCI', 'TCI', 'PA'],
    },
    hearing: {
      name: 'Hearing', type: 'rbrvs', applyFSI: false, cf: null, spaSection: 'II.D',
      facilityRules: { onlyIfPrior: true, applyFSI: false },
      componentRules: { method: 'remainder', defaultPCRatio: 0.20, defaultTCRatio: 0.80, constrainComponents: false },
      outputColumns: ['Procedure', 'Mod', 'FS', 'Facility', 'PC', 'TC', 'PA'],
    },
    visual: {
      name: 'Visual', type: 'rbrvs', applyFSI: false, cf: null, spaSection: 'II.D',
      facilityRules: { onlyIfPrior: false, applyFSI: false },
      componentRules: null,
      outputColumns: ['Procedure', 'Mod', 'FS', 'Facility', 'PA'],
    },
    dental: {
      name: 'Dental', type: 'flat', spaSection: 'Statutory',
      outputColumns: ['Procedure', 'FS'],
    },
    practitionerLab: {
      name: 'Practitioner Lab', type: 'lab', cf: null, spaSection: 'II.E(c)',
      componentRules: { method: 'remainder', defaultPCRatio: 0.20, defaultTCRatio: 0.80 },
      legislativeOverrides: {
        '81539': { rates: { fs: 456.00 }, statute: 'Legislative freeze' },
        '81435': { rates: { fs: 350.94 }, statute: 'Legislative freeze' },
        '81436': { rates: { fs: 350.94 }, statute: 'Legislative freeze' },
        '80406': { rates: { fs: 46.96, pc: 9.39, tc: 37.57 }, statute: 'Legislative freeze' },
        '80439': { rates: { fs: 40.33, pc: 8.07, tc: 32.26 }, statute: 'Legislative freeze' },
        '81176': { rates: { fs: 145.14 }, statute: 'Legislative freeze' },
        '81432': { rates: { fs: 407.43, pc: 81.49, tc: 325.94 }, statute: 'Legislative freeze' },
      },
      outputColumns: ['Procedure', 'FS', 'PC', 'TC'],
    },
    licensedMidwife: {
      name: 'Licensed Midwife', type: 'derived', sourceSchedule: 'practitioner',
      derivedMultiplier: 0.80, removeFSIBeforeDerivation: true, deriveFacility: true,
      spaSection: 'II.I',
      outputColumns: ['Procedure', 'Mod', 'FC', 'Facility'],
    },
    independentLab: {
      name: 'Independent Lab', type: 'derived', sourceSchedule: 'practitionerLab',
      derivedMultiplier: 0.90, removeFSIBeforeDerivation: false, deriveFacility: false,
      spaSection: 'II.E(d)',
      outputColumns: ['Procedure', 'FS'],
    },
  },

  crosswalks: [
    { name: 'LER', type: 'redistribute', deletedCodes: Array.from({ length: 16 }, (_, i) => String(37220 + i)), newCodes: Array.from({ length: 46 }, (_, i) => String(37254 + i)) },
    { name: 'ProstateBiopsy', type: 'redistribute', deletedCodes: ['55700'], newCodes: Array.from({ length: 9 }, (_, i) => String(55707 + i)) },
    { name: 'AnorectalMotility', type: 'one_to_one', mappings: [{ source: '91120', target: '91124' }, { source: '91122', target: '91125' }] },
    { name: 'RadiationTherapy', type: 'proportional', deletedCodes: ['77014', '77385', '77386', '77401'], baseCodes: ['77402', '77412'] },
    { name: 'BundledExcluded', type: 'exclude', codes: ['92921','92925','92929','92934','92938','92944','75956','75957','75958','75959','33884','33889','33891','D9248','27445','27468','37500','52647','92975','92977','94662','75842'] },
  ],

  paArnpMultiplier: 0.80,
  anesthesia: { timeRate: 14.50, pediatricIncrease: 0.04 },

  pediatricStacking: {
    fsi:          { mult: 1.04, statute: 'Ch.2000-166' },
    pediatric:    { mult: 1.04, statute: 'Ch.2001-253' },
    pedSpecialty: { mult: 1.24, statute: 'Ch.2004-268' },
    pedEM:        { mult: 1.102, statute: 'Ch.2014-51' },
    physPed:      { mult: 1.073, statute: '2023-24 GAA' },
  },

  notes: [
    'Tier 3: Built from production R scripts run for CY2026 fee setting',
    'V3 corrections: Facility × 1.04, H1001 swap fix',
    'Hearing/Visual: NO 1.04 baked in — applied at claim time by provider type',
    'Two separate CFs: Regular ($24.9780) and Lab ($26.1689)',
  ],
  sources: [
    'SPA FL-24-0002', 'Ted Webb Fee Setting Manual (Jan 2016)',
    'Milliman CY2025 Annual Rate Report', 'AHCA 2026 Production Scripts (1a-9)',
  ],
};


// =============================================================================
// SECTION 6: TIER 1 CONFIG FACTORY
// =============================================================================

function createTier1Config(params) {
  const {
    state, spaReference, effectiveDate,
    cf, fsiMultiplier = 1.0, gpci = 1.0,
    medicaidMultiplier = 0.60, maxChange = 0.10,
    methodologyType = 'rbrvs', pctMedicare = null,
    notes = [],
  } = params;

  return {
    state, spaReference, effectiveDate,
    tier: 1, confidence: 0,
    cf, gpci, fsiMultiplier, medicaidMultiplier, maxChange, methodologyType, pctMedicare,
    schedules: {
      primary: {
        name: `${state} Primary`, type: methodologyType,
        applyFSI: fsiMultiplier !== 1.0, cf: null,
        componentRules: { method: 'remainder', defaultPCRatio: 0.20, defaultTCRatio: 0.80 },
      },
    },
    localeWeights: {}, crosswalks: [], legislativeOverrides: {},
    validation: null,
    notes: [`Tier 1: SPA-derived only (${spaReference})`, ...notes],
    sources: [spaReference],
  };
}


// =============================================================================
// SECTION 7: MAIN ENGINE CLASS
// =============================================================================

class StateRateEngine {
  constructor(stateConfig) {
    this.config = stateConfig;
    this.validationResults = null;
  }

  priceCode(code, scheduleName = null) {
    const schedule = scheduleName
      ? this.config.schedules[scheduleName]
      : this._inferSchedule(code);

    if (!schedule) {
      return { code: code.procedure, error: `No schedule found`, flags: ['UNKNOWN_SCHEDULE'] };
    }
    return priceCodeForSchedule(code, schedule, this.config);
  }

  priceSchedule(codes, scheduleName) {
    return codes.map(code => this.priceCode(code, scheduleName));
  }

  calculateCF(codes, options = {}) {
    const targetMoney = codes.reduce((sum, c) => sum + ((c.currentFee || 0) * (c.units || 0)), 0);
    return optimizeCF(codes, targetMoney, { maxChange: this.config.maxChange, ...options });
  }

  applyCrosswalks(utilization, options = {}) {
    return applyCrosswalks(utilization, this.config.crosswalks, options);
  }

  validate(calculated, published, options = {}) {
    this.validationResults = validateAgainstPublished(calculated, published, options);
    this.config.confidence = this.validationResults.summary.confidence;
    this.config.tier = this.validationResults.summary.tier;
    this.config.validation = this.validationResults.summary;
    return this.validationResults;
  }

  getDiagnostics() {
    return {
      state: this.config.state,
      tier: this.config.tier,
      confidence: this.config.confidence,
      scheduleTypes: Object.keys(this.config.schedules),
      parameters: {
        cf: this.config.cf, labCF: this.config.labCF,
        fsiMultiplier: this.config.fsiMultiplier,
        gpci: this.config.gpci, maxChange: this.config.maxChange,
      },
      validation: this.validationResults?.summary || null,
      patterns: this.validationResults?.patterns || [],
    };
  }

  _inferSchedule(code) {
    const proc = code.procedure || '';
    if (proc.startsWith('D')) return this.config.schedules.dental;
    if (/^8[0-9]/.test(proc) || /U$/.test(proc)) return this.config.schedules.practitionerLab;
    if (/^7[0-9]/.test(proc) && parseInt(proc) >= 70010) return this.config.schedules.radiology;
    if (/^92[5-6]/.test(proc)) return this.config.schedules.hearing;
    if (/^920[0-4]/.test(proc) || /^6[5-8]/.test(proc)) return this.config.schedules.visual;
    return this.config.schedules.practitioner || this.config.schedules.primary;
  }
}


// =============================================================================
// UTILITIES
// =============================================================================

function round2(n) { return Math.round(n * 100) / 100; }
function round4(n) { return Math.round(n * 10000) / 10000; }


// =============================================================================
// EXPORTS
// =============================================================================

export {
  StateRateEngine,
  applyConstraint, calcRBRVS, calcComponents, optimizeCF,
  calcFCSOWeightedAvg, calcAnesthesia, applyCrosswalks,
  validateAgainstPublished, detectGapPatterns,
  createTier1Config, FL_CONFIG,
  round2, round4,
};
