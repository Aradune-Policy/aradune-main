/**
 * CCBHC Rate Development Analysis Engine
 * Runs pre-built SQL queries against T-MSIS Parquet data via DuckDB-WASM
 * to support FL SPA FL-25-0007 rate development.
 */

import { query } from "./duckdb";

// ── Types ─────────────────────────────────────────────────────────────

export interface CcbhcCode {
  hcpcs: string;
  description: string;
  samhsa_category: string; // "1"–"9" for core categories, "expanded" for expanded-only
  scope: "core" | "expanded";
  policy_flag?: string;
}

export interface ServiceUtilRow {
  hcpcs_code: string;
  description: string;
  samhsa_category: string;
  scope: "core" | "expanded";
  total_paid: number;
  total_claims: number;
  total_beneficiaries: number;
  avg_rate: number;
  policy_flag?: string;
}

export interface StatusQuoResult {
  grand_total_paid: number;
  grand_total_claims: number;
  grand_total_beneficiaries: number;
  core_total_paid: number;
  core_total_claims: number;
  expanded_total_paid: number;
  expanded_total_claims: number;
  by_category: { category: string; total_paid: number; total_claims: number; total_beneficiaries: number }[];
  variance_vs_milliman_low: number;
  variance_vs_milliman_high: number;
  net_new_spending: number;
}

export interface ProviderRow {
  npi: string;
  provider_name: string;
  zip3: string;
  taxonomy: string;
  total_paid: number;
  total_claims: number;
  total_beneficiaries: number;
}

export interface TrendRow {
  year: number;
  total_paid: number;
  total_claims: number;
  total_beneficiaries: number;
  yoy_growth?: number;
}

export interface BenchmarkRow {
  state: string;
  total_paid: number;
  total_claims: number;
  total_beneficiaries: number;
  per_claim: number;
  per_bene: number;
}

export interface RateScenario {
  label: string;
  numerator: number;
  numerator_label: string;
  denominator: number;
  denominator_label: string;
  per_claim: number;
}

// ── Enhanced Analysis Types ───────────────────────────────────────

export interface DailyVisitRow {
  month: string;
  claims: number;
  benes: number;
  paid: number;
  working_days: number;
  daily_claims: number;
  daily_paid: number;
}

export interface VisitFrequencyRow {
  hcpcs_code: string;
  description: string;
  claims: number;
  benes: number;
  claims_per_bene: number;
  avg_rate: number;
  intensity: "high" | "medium" | "low";
}

export interface QualityGap {
  id: string;
  name: string;
  domain: string;
  fl_rate: number;
  median: number;
  gap: number;
  linked_codes: string[];
  direction: "higher_better" | "lower_better";
}

export interface WorkforceEntry {
  soc: string;
  title: string;
  fl_hourly: number;
  national_hourly: number;
  fl_vs_national_pct: number;
  linked_codes: string[];
  overhead_pct: number;
  implied_rate_per_15min: number;
}

export interface TelehealthTrend {
  month: string;
  phone_claims: number;
  phone_paid: number;
  digital_claims: number;
  digital_paid: number;
  total_claims: number;
  total_paid: number;
}

export interface EnhancedAnalysisResult {
  daily_visits: DailyVisitRow[];
  visit_frequency: VisitFrequencyRow[];
  quality_gaps: QualityGap[];
  workforce: WorkforceEntry[];
  telehealth_trends: TelehealthTrend[];
  ffs_share: number;
  implied_total_with_mc: number;
  enrollment_mix: Record<string, number>;
  provider_readiness: { total: number; broad_service: number; narrow_service: number; avg_codes: number };
}

export interface GeographyRow {
  zip3: string;
  region_name: string;
  ccbhc_providers: number;
  total_providers: number;
  ccbhc_paid: number;
  ccbhc_claims: number;
  ccbhc_benes: number;
  is_desert: boolean;
}

export interface ProviderScopedTotals {
  provider_count: number;
  total_paid: number;
  total_claims: number;
  total_benes: number;
  annualized_paid: number;
  annualized_claims: number;
  years_in_data: number;
}

export interface RefinedRateScenario {
  label: string;
  numerator: number;
  numerator_label: string;
  annual_claims: number;
  per_claim: number;
  status_quo_per_claim: number;
  increment: number;
}

export interface MonthlyTrendRow {
  month: string;
  total_paid: number;
  total_claims: number;
  total_beneficiaries: number;
}

export interface ProviderBenchmarkRow {
  state: string;
  provider_count: number;
  total_paid: number;
  total_claims: number;
  total_benes: number;
  per_provider: number;
  per_claim: number;
}

export interface CcbhcAnalysisResult {
  state: string;
  run_at: string;
  utilization: ServiceUtilRow[];
  status_quo: StatusQuoResult;
  providers: ProviderRow[];
  trends: TrendRow[];
  monthly_trends?: MonthlyTrendRow[];
  benchmarks: BenchmarkRow[];
  provider_benchmarks?: ProviderBenchmarkRow[];
  rate_estimates: RateScenario[];
  refined_rates?: RefinedRateScenario[];
  provider_totals?: ProviderScopedTotals;
  geography?: GeographyRow[];
  enhanced?: EnhancedAnalysisResult;
}

// ── Code Definitions (SAMHSA 9 Categories + Expanded) ─────────────────

export const CCBHC_CODES: CcbhcCode[] = [
  // Category 1: Screening, Assessment, and Diagnosis
  { hcpcs: "H2000", description: "Comprehensive MH assessment", samhsa_category: "1", scope: "core" },
  { hcpcs: "H0031", description: "MH health assessment", samhsa_category: "1", scope: "core" },
  { hcpcs: "H0001", description: "Alcohol/drug assessment", samhsa_category: "1", scope: "core" },
  { hcpcs: "96110", description: "Developmental screening", samhsa_category: "1", scope: "core" },
  { hcpcs: "H2010", description: "Comprehensive medication services", samhsa_category: "1", scope: "expanded" },
  { hcpcs: "96160", description: "Health risk assessment", samhsa_category: "1", scope: "expanded" },
  { hcpcs: "96130", description: "Psychological testing eval, first hour", samhsa_category: "1", scope: "expanded" },
  { hcpcs: "96131", description: "Psychological testing eval, addl hour", samhsa_category: "1", scope: "expanded" },
  { hcpcs: "96132", description: "Neuropsychological testing eval, first hour", samhsa_category: "1", scope: "expanded" },
  { hcpcs: "96133", description: "Neuropsychological testing eval, addl hour", samhsa_category: "1", scope: "expanded" },

  // Category 2: Person-Centered and Family-Centered Treatment Planning
  { hcpcs: "H0032", description: "MH service plan development", samhsa_category: "2", scope: "core" },
  { hcpcs: "T1007", description: "Treatment plan development/modification", samhsa_category: "2", scope: "core" },

  // Category 3: Outpatient Mental Health and Substance Use Services
  { hcpcs: "H0015", description: "Intensive outpatient (group)", samhsa_category: "3", scope: "core" },
  { hcpcs: "H0020", description: "Methadone administration", samhsa_category: "3", scope: "core" },
  { hcpcs: "H2019", description: "Therapeutic behavioral services", samhsa_category: "3", scope: "core" },
  { hcpcs: "H0040", description: "Assertive community treatment", samhsa_category: "3", scope: "core" },
  { hcpcs: "H2033", description: "Multi-systemic therapy", samhsa_category: "3", scope: "core" },
  { hcpcs: "H0046", description: "Mental health services NOS", samhsa_category: "3", scope: "expanded" },
  { hcpcs: "H0047", description: "Alcohol/drug abuse NOS (less intensive)", samhsa_category: "3", scope: "expanded" },
  { hcpcs: "H0048", description: "Alcohol/drug abuse NOS (more intensive)", samhsa_category: "3", scope: "expanded" },
  { hcpcs: "H2012", description: "Behavioral health day treatment, per hour", samhsa_category: "3", scope: "expanded" },
  { hcpcs: "T1015", description: "Clinic visit/encounter, all-inclusive", samhsa_category: "3", scope: "expanded" },
  { hcpcs: "T1023", description: "Screening to determine SUD risk", samhsa_category: "3", scope: "expanded" },
  { hcpcs: "S9480", description: "Intensive outpatient psych services", samhsa_category: "3", scope: "expanded" },

  // Category 4: Primary Care Screening and Monitoring
  { hcpcs: "99385", description: "Preventive visit new 18-39", samhsa_category: "4", scope: "core" },
  { hcpcs: "99386", description: "Preventive visit new 40-64", samhsa_category: "4", scope: "core" },
  { hcpcs: "99387", description: "Preventive visit new 65+", samhsa_category: "4", scope: "core" },
  { hcpcs: "99395", description: "Preventive visit est 18-39", samhsa_category: "4", scope: "core" },
  { hcpcs: "99396", description: "Preventive visit est 40-64", samhsa_category: "4", scope: "core" },
  { hcpcs: "99397", description: "Preventive visit est 65+", samhsa_category: "4", scope: "core" },

  // Category 5: Targeted Case Management
  { hcpcs: "T1017", description: "Targeted case management", samhsa_category: "5", scope: "core" },

  // Category 6: Psychiatric Rehabilitation Services
  { hcpcs: "H2017", description: "Psychosocial rehab, per 15 min", samhsa_category: "6", scope: "core" },
  { hcpcs: "H2030", description: "Mental health clubhouse services", samhsa_category: "6", scope: "expanded" },

  // Category 7: Peer Supports and Family/Caregiver Supports
  { hcpcs: "H0018", description: "Behavioral health short-term residential", samhsa_category: "7", scope: "core" },
  { hcpcs: "H0038", description: "Self-help/peer services, per 15 min", samhsa_category: "7", scope: "core" },
  { hcpcs: "S5102", description: "Day care services, adult, per 15 min", samhsa_category: "7", scope: "expanded" },

  // Category 8: Crisis Services
  { hcpcs: "H2011", description: "Crisis intervention, per 15 min", samhsa_category: "8", scope: "core" },
  { hcpcs: "S9484", description: "Crisis intervention, per hour", samhsa_category: "8", scope: "core" },

  // Category 9: Other Medical Items or Services
  { hcpcs: "H0035", description: "MH partial hospitalization", samhsa_category: "9", scope: "expanded" },
  { hcpcs: "S9475", description: "Ambulatory surgical center", samhsa_category: "9", scope: "expanded" },
  { hcpcs: "T1015", description: "Clinic visit/encounter, all-inclusive", samhsa_category: "9", scope: "expanded" },
];

// Deduplicate (T1015 appears in both Cat 3 and 9 — keep first occurrence)
const _seen = new Set<string>();
export const CCBHC_CODES_DEDUPED: CcbhcCode[] = CCBHC_CODES.filter(c => {
  if (_seen.has(c.hcpcs)) return false;
  _seen.add(c.hcpcs);
  return true;
});

export const ALL_HCPCS = CCBHC_CODES_DEDUPED.map(c => c.hcpcs);
const CODE_MAP = new Map(CCBHC_CODES_DEDUPED.map(c => [c.hcpcs, c]));

export const SAMHSA_CATEGORY_NAMES: Record<string, string> = {
  "1": "Screening, Assessment & Diagnosis",
  "2": "Treatment Planning",
  "3": "Outpatient MH & SU Services",
  "4": "Primary Care Screening",
  "5": "Targeted Case Management",
  "6": "Psychiatric Rehabilitation",
  "7": "Peer & Family Supports",
  "8": "Crisis Services",
  "9": "Other Medical Services",
};

export const MILLIMAN_ESTIMATES = {
  core_low: 87_300_000,
  core_high: 105_300_000,
  core_mid: 96_300_000,
  expanded_low: 107_100_000,
  expanded_high: 141_100_000,
  expanded_mid: 124_100_000,
  status_quo_low: 57_000_000,
  status_quo_high: 95_000_000,
  lbr_appropriation: 141_000_000,
} as const;

export const CCBHC_TAXONOMY_CODES = ["261QM0801X", "324500000X", "261QR0405X"];
export const PEER_STATES = ["FL", "GA", "TX", "NY", "CA", "OH"];

// ── SQL helpers ──────────────────────────────────────────────────────

function esc(s: string): string {
  return s.replace(/'/g, "''");
}

function inList(arr: string[]): string {
  return arr.map(s => `'${esc(s)}'`).join(",");
}

// ── Analysis Functions ──────────────────────────────────────────────

export async function analyzeServiceUtilization(state = "FL"): Promise<ServiceUtilRow[]> {
  const sql = `
    SELECT hcpcs_code,
           SUM(total_paid) AS total_paid,
           SUM(total_claims) AS total_claims,
           SUM(total_beneficiaries) AS total_beneficiaries,
           CASE WHEN SUM(total_claims) > 0
                THEN SUM(total_paid) / SUM(total_claims)
                ELSE 0 END AS avg_rate
    FROM 'claims.parquet'
    WHERE state = '${esc(state)}'
      AND hcpcs_code IN (${inList(ALL_HCPCS)})
    GROUP BY hcpcs_code
    ORDER BY total_paid DESC
  `;
  const result = await query(sql);
  const dbMap = new Map(result.rows.map(r => [r.hcpcs_code as string, r]));

  // Return all 54 codes, filling in zeros for those not in DB
  return CCBHC_CODES_DEDUPED.map(code => {
    const row = dbMap.get(code.hcpcs);
    return {
      hcpcs_code: code.hcpcs,
      description: code.description,
      samhsa_category: code.samhsa_category,
      scope: code.scope,
      total_paid: Number(row?.total_paid ?? 0),
      total_claims: Number(row?.total_claims ?? 0),
      total_beneficiaries: Number(row?.total_beneficiaries ?? 0),
      avg_rate: Number(row?.avg_rate ?? 0),
      policy_flag: code.policy_flag,
    };
  });
}

export async function analyzeStatusQuo(state = "FL"): Promise<StatusQuoResult> {
  const sql = `
    SELECT hcpcs_code,
           SUM(total_paid) AS total_paid,
           SUM(total_claims) AS total_claims,
           SUM(total_beneficiaries) AS total_beneficiaries
    FROM 'claims.parquet'
    WHERE state = '${esc(state)}'
      AND hcpcs_code IN (${inList(ALL_HCPCS)})
    GROUP BY hcpcs_code
  `;
  const result = await query(sql);

  let grand_total_paid = 0, grand_total_claims = 0, grand_total_beneficiaries = 0;
  let core_total_paid = 0, core_total_claims = 0;
  let expanded_total_paid = 0, expanded_total_claims = 0;
  const catAccum: Record<string, { total_paid: number; total_claims: number; total_beneficiaries: number }> = {};

  for (const row of result.rows) {
    const code = CODE_MAP.get(row.hcpcs_code as string);
    const paid = Number(row.total_paid ?? 0);
    const claims = Number(row.total_claims ?? 0);
    const benes = Number(row.total_beneficiaries ?? 0);

    grand_total_paid += paid;
    grand_total_claims += claims;
    grand_total_beneficiaries += benes;

    if (code?.scope === "core") {
      core_total_paid += paid;
      core_total_claims += claims;
    } else {
      expanded_total_paid += paid;
      expanded_total_claims += claims;
    }

    const cat = code?.samhsa_category ?? "unknown";
    if (!catAccum[cat]) catAccum[cat] = { total_paid: 0, total_claims: 0, total_beneficiaries: 0 };
    catAccum[cat].total_paid += paid;
    catAccum[cat].total_claims += claims;
    catAccum[cat].total_beneficiaries += benes;
  }

  const by_category = Object.entries(catAccum)
    .map(([category, v]) => ({ category: SAMHSA_CATEGORY_NAMES[category] || category, ...v }))
    .sort((a, b) => b.total_paid - a.total_paid);

  return {
    grand_total_paid,
    grand_total_claims,
    grand_total_beneficiaries,
    core_total_paid,
    core_total_claims,
    expanded_total_paid,
    expanded_total_claims,
    by_category,
    variance_vs_milliman_low: grand_total_paid - MILLIMAN_ESTIMATES.status_quo_low,
    variance_vs_milliman_high: grand_total_paid - MILLIMAN_ESTIMATES.status_quo_high,
    net_new_spending: MILLIMAN_ESTIMATES.lbr_appropriation - grand_total_paid,
  };
}

export async function analyzeProviders(state = "FL"): Promise<ProviderRow[]> {
  const sql = `
    SELECT npi, provider_name, zip3, taxonomy,
           total_paid, total_claims, total_beneficiaries
    FROM 'providers.parquet'
    WHERE state = '${esc(state)}'
      AND taxonomy IN (${inList(CCBHC_TAXONOMY_CODES)})
    ORDER BY total_paid DESC
    LIMIT 200
  `;
  const result = await query(sql);
  return result.rows.map(r => ({
    npi: String(r.npi ?? ""),
    provider_name: String(r.provider_name ?? ""),
    zip3: String(r.zip3 ?? ""),
    taxonomy: String(r.taxonomy ?? ""),
    total_paid: Number(r.total_paid ?? 0),
    total_claims: Number(r.total_claims ?? 0),
    total_beneficiaries: Number(r.total_beneficiaries ?? 0),
  }));
}

export async function analyzeTrends(state = "FL"): Promise<TrendRow[]> {
  const sql = `
    SELECT year,
           SUM(total_paid) AS total_paid,
           SUM(total_claims) AS total_claims,
           SUM(total_beneficiaries) AS total_beneficiaries
    FROM 'claims.parquet'
    WHERE state = '${esc(state)}'
      AND hcpcs_code IN (${inList(ALL_HCPCS)})
    GROUP BY year
    ORDER BY year
  `;
  const result = await query(sql);
  const rows: TrendRow[] = result.rows.map(r => ({
    year: Number(r.year),
    total_paid: Number(r.total_paid ?? 0),
    total_claims: Number(r.total_claims ?? 0),
    total_beneficiaries: Number(r.total_beneficiaries ?? 0),
  }));

  // Calculate YoY growth
  for (let i = 1; i < rows.length; i++) {
    const prev = rows[i - 1].total_paid;
    if (prev > 0) {
      rows[i].yoy_growth = ((rows[i].total_paid - prev) / prev) * 100;
    }
  }
  return rows;
}

export async function analyzeCrossState(): Promise<BenchmarkRow[]> {
  const sql = `
    SELECT state,
           SUM(total_paid) AS total_paid,
           SUM(total_claims) AS total_claims,
           SUM(total_beneficiaries) AS total_beneficiaries
    FROM 'claims.parquet'
    WHERE state IN (${inList(PEER_STATES)})
      AND hcpcs_code IN (${inList(ALL_HCPCS)})
    GROUP BY state
    ORDER BY total_paid DESC
  `;
  const result = await query(sql);
  return result.rows.map(r => {
    const paid = Number(r.total_paid ?? 0);
    const claims = Number(r.total_claims ?? 0);
    const benes = Number(r.total_beneficiaries ?? 0);
    return {
      state: String(r.state),
      total_paid: paid,
      total_claims: claims,
      total_beneficiaries: benes,
      per_claim: claims > 0 ? paid / claims : 0,
      per_bene: benes > 0 ? paid / benes : 0,
    };
  });
}

export function calculateRateEstimates(totalClaims: number, _totalBenes: number): RateScenario[] {
  if (totalClaims <= 0) return [];
  return [
    {
      label: "Scenario A: Core PPS",
      numerator: MILLIMAN_ESTIMATES.core_mid,
      numerator_label: "Milliman Core midpoint ($96.3M)",
      denominator: totalClaims,
      denominator_label: "FL CCBHC claims (T-MSIS)",
      per_claim: MILLIMAN_ESTIMATES.core_mid / totalClaims,
    },
    {
      label: "Scenario B: Core+ (adjusted)",
      numerator: 106_000_000,
      numerator_label: "Core + 10% adjustment ($106.0M)",
      denominator: totalClaims,
      denominator_label: "FL CCBHC claims (T-MSIS)",
      per_claim: 106_000_000 / totalClaims,
    },
    {
      label: "Scenario C: Expanded PPS",
      numerator: MILLIMAN_ESTIMATES.expanded_mid,
      numerator_label: "Milliman Expanded midpoint ($124.1M)",
      denominator: totalClaims,
      denominator_label: "FL CCBHC claims (T-MSIS)",
      per_claim: MILLIMAN_ESTIMATES.expanded_mid / totalClaims,
    },
    {
      label: "Scenario D: LBR Appropriation",
      numerator: MILLIMAN_ESTIMATES.lbr_appropriation,
      numerator_label: "Legislative appropriation ($141.0M)",
      denominator: totalClaims,
      denominator_label: "FL CCBHC claims (T-MSIS)",
      per_claim: MILLIMAN_ESTIMATES.lbr_appropriation / totalClaims,
    },
  ];
}

// ── Enhanced Analysis Functions ──────────────────────────────────

const WORKING_DAYS: Record<string, number> = {
  "01":22,"02":20,"03":22,"04":21,"05":22,"06":21,"07":22,"08":22,"09":21,"10":23,"11":20,"12":22
};

export async function analyzeDailyVisits(state = "FL"): Promise<DailyVisitRow[]> {
  const sql = `
    SELECT claim_month, SUM(total_claims) AS claims, SUM(total_beneficiaries) AS benes, SUM(total_paid) AS paid
    FROM 'claims_monthly.parquet'
    WHERE state = '${esc(state)}' AND year IN (2023, 2024)
      AND hcpcs_code IN (${inList(ALL_HCPCS)})
    GROUP BY claim_month ORDER BY claim_month
  `;
  try {
    const result = await query(sql);
    return result.rows.map(r => {
      const month = String(r.claim_month);
      const mm = month.slice(5, 7);
      const wd = WORKING_DAYS[mm] || 21;
      const claims = Number(r.claims ?? 0);
      const paid = Number(r.paid ?? 0);
      return {
        month,
        claims,
        benes: Number(r.benes ?? 0),
        paid,
        working_days: wd,
        daily_claims: Math.round(claims / wd),
        daily_paid: paid / wd,
      };
    });
  } catch {
    return []; // Monthly data not available
  }
}

export async function analyzeVisitFrequency(state = "FL"): Promise<VisitFrequencyRow[]> {
  const sql = `
    SELECT hcpcs_code, SUM(total_claims) AS claims, SUM(total_beneficiaries) AS benes,
      SUM(total_paid) AS paid,
      SUM(total_claims)*1.0/NULLIF(SUM(total_beneficiaries),0) AS claims_per_bene,
      SUM(total_paid)*1.0/NULLIF(SUM(total_claims),0) AS avg_rate
    FROM 'claims.parquet'
    WHERE state = '${esc(state)}' AND year = 2023
      AND hcpcs_code IN (${inList(ALL_HCPCS)})
    GROUP BY hcpcs_code ORDER BY claims_per_bene DESC
  `;
  const result = await query(sql);
  return result.rows.map(r => {
    const cpb = Number(r.claims_per_bene ?? 0);
    return {
      hcpcs_code: String(r.hcpcs_code),
      description: CODE_MAP.get(String(r.hcpcs_code))?.description ?? "",
      claims: Number(r.claims ?? 0),
      benes: Number(r.benes ?? 0),
      claims_per_bene: cpb,
      avg_rate: Number(r.avg_rate ?? 0),
      intensity: cpb >= 10 ? "high" : cpb >= 3 ? "medium" : "low",
    };
  });
}

const TELEHEALTH_CODES = ["99441","99442","99443","98966","98967","98968","99421","99422","99423","G2012","G2010"];
const TELEHEALTH_PHONE = ["99441","99442","99443"];

export async function analyzeTelehealth(state = "FL"): Promise<TelehealthTrend[]> {
  const sql = `
    SELECT claim_month,
      SUM(CASE WHEN hcpcs_code IN (${inList(TELEHEALTH_PHONE)}) THEN total_claims ELSE 0 END) AS phone_claims,
      SUM(CASE WHEN hcpcs_code IN (${inList(TELEHEALTH_PHONE)}) THEN total_paid ELSE 0 END) AS phone_paid,
      SUM(CASE WHEN hcpcs_code NOT IN (${inList(TELEHEALTH_PHONE)}) THEN total_claims ELSE 0 END) AS digital_claims,
      SUM(CASE WHEN hcpcs_code NOT IN (${inList(TELEHEALTH_PHONE)}) THEN total_paid ELSE 0 END) AS digital_paid,
      SUM(total_claims) AS total_claims, SUM(total_paid) AS total_paid
    FROM 'claims_monthly.parquet'
    WHERE state = '${esc(state)}' AND hcpcs_code IN (${inList(TELEHEALTH_CODES)})
    GROUP BY claim_month ORDER BY claim_month
  `;
  try {
    const result = await query(sql);
    return result.rows.map(r => ({
      month: String(r.claim_month),
      phone_claims: Number(r.phone_claims ?? 0),
      phone_paid: Number(r.phone_paid ?? 0),
      digital_claims: Number(r.digital_claims ?? 0),
      digital_paid: Number(r.digital_paid ?? 0),
      total_claims: Number(r.total_claims ?? 0),
      total_paid: Number(r.total_paid ?? 0),
    }));
  } catch {
    return [];
  }
}

// FFS share constants (from KFF/CMS 2023)
const FFS_SHARE: Record<string, number> = {
  FL:0.23,NY:0.24,TX:0.27,CA:0.18,OH:0.16,GA:0.28,PA:0.22,IL:0.35,MN:0.28,AZ:0.15
};

// Quality measures — statically defined from data file
const QUALITY_GAPS_DATA: Omit<QualityGap, "direction">[] = [
  { id:"IET-AD", name:"SUD Treatment Initiation (18+)", domain:"Behavioral Health", fl_rate:6.8, median:41.8, gap:-35.0, linked_codes:["H0004","H0015","90834","90837"] },
  { id:"AMM-AD", name:"Antidepressant Medication Mgmt (18+)", domain:"Behavioral Health", fl_rate:39.9, median:61.1, gap:-21.2, linked_codes:[] },
  { id:"MSC-AD", name:"Smoking/Tobacco Cessation (18+)", domain:"Behavioral Health", fl_rate:46.9, median:74.5, gap:-27.6, linked_codes:[] },
  { id:"DEV-CH", name:"Developmental Screening (0-3)", domain:"Preventive", fl_rate:24.7, median:37.4, gap:-12.7, linked_codes:["96110"] },
  { id:"APM-CH", name:"Metabolic Monitoring, Antipsychotics (1-17)", domain:"Behavioral Health", fl_rate:37.9, median:43.6, gap:-5.7, linked_codes:[] },
  { id:"FUM-AD", name:"Post-ED Follow-Up, Mental Illness (18+)", domain:"Behavioral Health", fl_rate:33.6, median:35.3, gap:-1.7, linked_codes:["90834","90837","99213","99214"] },
  { id:"FUA-AD", name:"Post-ED Follow-Up, Substance Use (18+)", domain:"Behavioral Health", fl_rate:24.3, median:25.8, gap:-1.5, linked_codes:["99213","99214","H0004","H0015"] },
  { id:"FUH-CH", name:"Post-Hospitalization Follow-Up (6-17)", domain:"Behavioral Health", fl_rate:64.5, median:44.8, gap:19.7, linked_codes:["90834","90837","90832"] },
  { id:"FUH-AD", name:"Post-Hospitalization Follow-Up (18+)", domain:"Behavioral Health", fl_rate:40.7, median:32.3, gap:8.4, linked_codes:["90834","90837","90832","90847"] },
  { id:"OUD-AD", name:"Pharmacotherapy for Opioid Use Disorder", domain:"Behavioral Health", fl_rate:50.4, median:40.0, gap:10.4, linked_codes:["H0020","J0571","J0572","J0573","J0574","J0575"] },
  { id:"ADD-CH", name:"ADHD Medication Follow-Up (6-12)", domain:"Behavioral Health", fl_rate:57.9, median:47.4, gap:10.5, linked_codes:["99213","99214","99215"] },
  { id:"SAA-AD", name:"Antipsychotic Adherence, Schizophrenia", domain:"Behavioral Health", fl_rate:60.8, median:61.2, gap:-0.4, linked_codes:["J2426","J1631","H0033"] },
];

// Workforce data — statically defined from BLS data file
const WORKFORCE_DATA: Omit<WorkforceEntry, "implied_rate_per_15min">[] = [
  { soc:"21-1023", title:"MH & SUD Social Workers", fl_hourly:26.98, national_hourly:32.83, fl_vs_national_pct:-17.8, linked_codes:["H0031","H0032","H0038"], overhead_pct:30 },
  { soc:"21-1021", title:"Child/Family Social Workers", fl_hourly:27.28, national_hourly:30.25, fl_vs_national_pct:-9.8, linked_codes:["H2019","T1017"], overhead_pct:30 },
  { soc:"29-1141", title:"Registered Nurses", fl_hourly:42.40, national_hourly:47.32, fl_vs_national_pct:-10.4, linked_codes:["T1015","99385","99395"], overhead_pct:30 },
];

// Enrollment mix — statically from risk_adj.json
const FL_ENROLLMENT_MIX: Record<string, number> = { child:47.6, new_adult:0.0, other_adult:28.2, disabled:10.8, aged:13.4 };

export function buildEnhancedAnalysis(
  dailyVisits: DailyVisitRow[],
  visitFreq: VisitFrequencyRow[],
  teleTrends: TelehealthTrend[],
  providers: ProviderRow[],
  grandTotalPaid: number,
  state: string,
): EnhancedAnalysisResult {
  const ffsShare = FFS_SHARE[state] ?? 0.23;
  const broadService = providers.filter(p => (p as ProviderRow & { code_count?: number }).total_claims > 1000).length;
  const narrowService = providers.length - broadService;
  const avgCodes = providers.length > 0
    ? providers.reduce((a, p) => a + ((p as unknown as Record<string, number>).code_count ?? 1), 0) / providers.length
    : 0;

  const qualityGaps: QualityGap[] = QUALITY_GAPS_DATA.map(q => ({
    ...q,
    direction: ["COB-AD","OHD-AD"].includes(q.id) ? "lower_better" as const : "higher_better" as const,
  }));

  const workforce: WorkforceEntry[] = WORKFORCE_DATA.map(w => ({
    ...w,
    implied_rate_per_15min: (w.fl_hourly / 4) * (1 + w.overhead_pct / 100),
  }));

  return {
    daily_visits: dailyVisits,
    visit_frequency: visitFreq,
    quality_gaps: qualityGaps,
    workforce,
    telehealth_trends: teleTrends,
    ffs_share: ffsShare,
    implied_total_with_mc: ffsShare > 0 ? grandTotalPaid / ffsShare : grandTotalPaid,
    enrollment_mix: FL_ENROLLMENT_MIX,
    provider_readiness: { total: providers.length, broad_service: broadService, narrow_service: narrowService, avg_codes: avgCodes },
  };
}

// ── Monthly Trends ──────────────────────────────────────────────────

export async function analyzeMonthlyTrends(state = "FL"): Promise<MonthlyTrendRow[]> {
  const sql = `
    SELECT claim_month,
           SUM(total_paid) AS total_paid,
           SUM(total_claims) AS total_claims,
           SUM(total_beneficiaries) AS total_beneficiaries
    FROM 'claims_monthly.parquet'
    WHERE state = '${esc(state)}'
      AND hcpcs_code IN (${inList(ALL_HCPCS)})
    GROUP BY claim_month ORDER BY claim_month
  `;
  try {
    const result = await query(sql);
    return result.rows.map(r => ({
      month: String(r.claim_month),
      total_paid: Number(r.total_paid ?? 0),
      total_claims: Number(r.total_claims ?? 0),
      total_beneficiaries: Number(r.total_beneficiaries ?? 0),
    }));
  } catch {
    return []; // Monthly data not available
  }
}

// ── Provider-Scoped Peer Benchmarks ─────────────────────────────────

export async function analyzeProviderBenchmarks(): Promise<ProviderBenchmarkRow[]> {
  const sql = `
    SELECT state, COUNT(DISTINCT npi) AS provider_count,
           SUM(total_paid) AS total_paid,
           SUM(total_claims) AS total_claims,
           SUM(total_beneficiaries) AS total_benes
    FROM 'providers.parquet'
    WHERE state IN (${inList(PEER_STATES)})
      AND taxonomy IN (${inList(CCBHC_TAXONOMY_CODES)})
    GROUP BY state ORDER BY total_paid DESC
  `;
  const result = await query(sql);
  return result.rows.map(r => {
    const paid = Number(r.total_paid ?? 0);
    const count = Number(r.provider_count ?? 0);
    const claims = Number(r.total_claims ?? 0);
    return {
      state: String(r.state),
      provider_count: count,
      total_paid: paid,
      total_claims: claims,
      total_benes: Number(r.total_benes ?? 0),
      per_provider: count > 0 ? paid / count : 0,
      per_claim: claims > 0 ? paid / claims : 0,
    };
  });
}

// ── Geographic Analysis ──────────────────────────────────────────────

export async function analyzeGeography(state = "FL"): Promise<GeographyRow[]> {
  // Get CCBHC-taxonomy providers by ZIP3
  const ccbhcSql = `
    SELECT zip3, COUNT(DISTINCT npi) AS ccbhc_providers,
           SUM(total_paid) AS ccbhc_paid, SUM(total_claims) AS ccbhc_claims,
           SUM(total_beneficiaries) AS ccbhc_benes
    FROM 'providers.parquet'
    WHERE state = '${esc(state)}' AND taxonomy IN (${inList(CCBHC_TAXONOMY_CODES)})
    GROUP BY zip3
  `;
  // Get ALL providers by ZIP3 (for desert detection)
  const allSql = `
    SELECT zip3, COUNT(DISTINCT npi) AS total_providers
    FROM 'providers.parquet'
    WHERE state = '${esc(state)}'
    GROUP BY zip3
  `;

  const [ccbhcResult, allResult] = await Promise.all([query(ccbhcSql), query(allSql)]);

  const ccbhcMap = new Map(ccbhcResult.rows.map(r => [String(r.zip3), r]));
  const rows: GeographyRow[] = [];

  for (const ar of allResult.rows) {
    const z = String(ar.zip3);
    // Skip non-state ZIP3s (data anomalies from out-of-state providers)
    if (state === "FL" && !z.startsWith("3")) continue;
    const cr = ccbhcMap.get(z);
    rows.push({
      zip3: z,
      region_name: z, // Will be enriched by UI if regions.json is available
      ccbhc_providers: Number(cr?.ccbhc_providers ?? 0),
      total_providers: Number(ar.total_providers ?? 0),
      ccbhc_paid: Number(cr?.ccbhc_paid ?? 0),
      ccbhc_claims: Number(cr?.ccbhc_claims ?? 0),
      ccbhc_benes: Number(cr?.ccbhc_benes ?? 0),
      is_desert: !cr,
    });
  }

  return rows.sort((a, b) => b.ccbhc_providers - a.ccbhc_providers);
}

// ── Provider-Scoped Rate Refinement ─────────────────────────────────

export async function analyzeProviderScopedTotals(state = "FL"): Promise<ProviderScopedTotals> {
  const sql = `
    SELECT COUNT(DISTINCT npi) AS provider_count,
           SUM(total_paid) AS total_paid,
           SUM(total_claims) AS total_claims,
           SUM(total_beneficiaries) AS total_benes
    FROM 'providers.parquet'
    WHERE state = '${esc(state)}'
      AND taxonomy IN (${inList(CCBHC_TAXONOMY_CODES)})
  `;
  const result = await query(sql);
  const row = result.rows[0] || {};
  const totalPaid = Number(row.total_paid ?? 0);
  const totalClaims = Number(row.total_claims ?? 0);
  // Data spans ~7 years (2018-2024)
  const years = 7;
  return {
    provider_count: Number(row.provider_count ?? 0),
    total_paid: totalPaid,
    total_claims: totalClaims,
    total_benes: Number(row.total_benes ?? 0),
    annualized_paid: totalPaid / years,
    annualized_claims: totalClaims / years,
    years_in_data: years,
  };
}

export function calculateRefinedRates(provTotals: ProviderScopedTotals): RefinedRateScenario[] {
  if (provTotals.annualized_claims <= 0) return [];
  const ac = provTotals.annualized_claims;
  const sqPerClaim = provTotals.annualized_paid / ac;

  return [
    {
      label: "Core PPS (mid)",
      numerator: MILLIMAN_ESTIMATES.core_mid,
      numerator_label: "Milliman Core $96.3M",
      annual_claims: ac,
      per_claim: MILLIMAN_ESTIMATES.core_mid / ac,
      status_quo_per_claim: sqPerClaim,
      increment: (MILLIMAN_ESTIMATES.core_mid / ac) - sqPerClaim,
    },
    {
      label: "Expanded PPS (mid)",
      numerator: MILLIMAN_ESTIMATES.expanded_mid,
      numerator_label: "Milliman Expanded $124.1M",
      annual_claims: ac,
      per_claim: MILLIMAN_ESTIMATES.expanded_mid / ac,
      status_quo_per_claim: sqPerClaim,
      increment: (MILLIMAN_ESTIMATES.expanded_mid / ac) - sqPerClaim,
    },
    {
      label: "LBR Appropriation",
      numerator: MILLIMAN_ESTIMATES.lbr_appropriation,
      numerator_label: "Legislative $141.0M",
      annual_claims: ac,
      per_claim: MILLIMAN_ESTIMATES.lbr_appropriation / ac,
      status_quo_per_claim: sqPerClaim,
      increment: (MILLIMAN_ESTIMATES.lbr_appropriation / ac) - sqPerClaim,
    },
  ];
}

// ── Master runner ───────────────────────────────────────────────────

export async function runFullCcbhcAnalysis(state = "FL"): Promise<CcbhcAnalysisResult> {
  const [utilization, status_quo, providers, trends, monthlyTrends, benchmarks, provBenchmarks, dailyVisits, visitFreq, teleTrends, geography, providerTotals] = await Promise.all([
    analyzeServiceUtilization(state),
    analyzeStatusQuo(state),
    analyzeProviders(state),
    analyzeTrends(state),
    analyzeMonthlyTrends(state),
    analyzeCrossState(),
    analyzeProviderBenchmarks(),
    analyzeDailyVisits(state),
    analyzeVisitFrequency(state),
    analyzeTelehealth(state),
    analyzeGeography(state),
    analyzeProviderScopedTotals(state),
  ]);

  const rate_estimates = calculateRateEstimates(
    status_quo.grand_total_claims,
    status_quo.grand_total_beneficiaries,
  );

  const refined_rates = calculateRefinedRates(providerTotals);

  const enhanced = buildEnhancedAnalysis(
    dailyVisits, visitFreq, teleTrends, providers,
    status_quo.grand_total_paid, state,
  );

  return {
    state,
    run_at: new Date().toISOString(),
    utilization,
    status_quo,
    providers,
    trends,
    monthly_trends: monthlyTrends,
    benchmarks,
    provider_benchmarks: provBenchmarks,
    rate_estimates,
    refined_rates,
    provider_totals: providerTotals,
    geography,
    enhanced,
  };
}

// ── CSV Export ───────────────────────────────────────────────────────

export function exportAnalysisCSV(result: CcbhcAnalysisResult): string {
  const lines: string[] = [];
  const esc = (v: string | number) => {
    const s = String(v ?? "");
    return s.includes(",") || s.includes('"') ? `"${s.replace(/"/g, '""')}"` : s;
  };

  // Section 1: Service Utilization
  lines.push("=== SERVICE UTILIZATION ===");
  lines.push("HCPCS,Description,SAMHSA Category,Scope,Total Paid,Claims,Beneficiaries,Avg Rate");
  for (const r of result.utilization) {
    lines.push([r.hcpcs_code, esc(r.description), r.samhsa_category, r.scope,
      r.total_paid.toFixed(2), r.total_claims, r.total_beneficiaries, r.avg_rate.toFixed(2)].join(","));
  }
  lines.push("");

  // Section 2: Status Quo
  lines.push("=== STATUS QUO SPENDING ===");
  lines.push(`Grand Total Paid,${result.status_quo.grand_total_paid.toFixed(2)}`);
  lines.push(`Core Subtotal,${result.status_quo.core_total_paid.toFixed(2)}`);
  lines.push(`Expanded Subtotal,${result.status_quo.expanded_total_paid.toFixed(2)}`);
  lines.push(`Variance vs Milliman Low ($57M),${result.status_quo.variance_vs_milliman_low.toFixed(2)}`);
  lines.push(`Variance vs Milliman High ($95M),${result.status_quo.variance_vs_milliman_high.toFixed(2)}`);
  lines.push(`Net New Spending ($141M - status quo),${result.status_quo.net_new_spending.toFixed(2)}`);
  lines.push("");
  lines.push("Category,Total Paid,Claims,Beneficiaries");
  for (const c of result.status_quo.by_category) {
    lines.push([esc(c.category), c.total_paid.toFixed(2), c.total_claims, c.total_beneficiaries].join(","));
  }
  lines.push("");

  // Section 3: Providers
  lines.push("=== PROVIDER LANDSCAPE ===");
  lines.push("NPI,Provider Name,ZIP3,Taxonomy,Total Paid,Claims,Beneficiaries");
  for (const p of result.providers) {
    lines.push([p.npi, esc(p.provider_name), p.zip3, p.taxonomy,
      p.total_paid.toFixed(2), p.total_claims, p.total_beneficiaries].join(","));
  }
  lines.push("");

  // Section 4: Trends
  lines.push("=== TRENDS ===");
  lines.push("Year,Total Paid,Claims,Beneficiaries,YoY Growth %");
  for (const t of result.trends) {
    lines.push([t.year, t.total_paid.toFixed(2), t.total_claims, t.total_beneficiaries,
      t.yoy_growth != null ? t.yoy_growth.toFixed(1) : ""].join(","));
  }
  lines.push("");

  // Section 5: Cross-State Benchmarks
  lines.push("=== CROSS-STATE BENCHMARKS ===");
  lines.push("State,Total Paid,Claims,Beneficiaries,Per Claim,Per Bene");
  for (const b of result.benchmarks) {
    lines.push([b.state, b.total_paid.toFixed(2), b.total_claims, b.total_beneficiaries,
      b.per_claim.toFixed(2), b.per_bene.toFixed(2)].join(","));
  }
  lines.push("");

  // Section 6: Rate Estimates
  lines.push("=== ILLUSTRATIVE RATE ESTIMATES (statewide) ===");
  lines.push("Scenario,Numerator ($),Denominator (claims),Per Claim Rate ($)");
  for (const r of result.rate_estimates) {
    lines.push([esc(r.label), r.numerator.toFixed(2), r.denominator, r.per_claim.toFixed(2)].join(","));
  }
  lines.push("");

  // Section 7: Refined Rate Estimates (provider-scoped)
  if (result.refined_rates?.length) {
    lines.push("=== REFINED RATE ESTIMATES (267-provider scope) ===");
    if (result.provider_totals) {
      lines.push(`Providers,${result.provider_totals.provider_count}`);
      lines.push(`Total Paid (cumulative),${result.provider_totals.total_paid.toFixed(2)}`);
      lines.push(`Total Claims (cumulative),${result.provider_totals.total_claims}`);
      lines.push(`Annualized Paid,${result.provider_totals.annualized_paid.toFixed(2)}`);
      lines.push(`Annualized Claims,${result.provider_totals.annualized_claims.toFixed(0)}`);
    }
    lines.push("Scenario,Numerator ($),Annual Claims,Per Claim ($),Status Quo Per Claim ($),Increment ($)");
    for (const r of result.refined_rates) {
      lines.push([esc(r.label), r.numerator.toFixed(2), r.annual_claims.toFixed(0),
        r.per_claim.toFixed(2), r.status_quo_per_claim.toFixed(2), r.increment.toFixed(2)].join(","));
    }
    lines.push("");
  }

  // Section 8: Geographic Analysis
  if (result.geography?.length) {
    lines.push("=== GEOGRAPHIC ANALYSIS ===");
    lines.push("ZIP3,CCBHC Providers,Total Providers,CCBHC Desert,CCBHC Paid,CCBHC Claims,CCBHC Benes");
    for (const g of result.geography) {
      lines.push([g.zip3, g.ccbhc_providers, g.total_providers, g.is_desert ? "YES" : "",
        g.ccbhc_paid.toFixed(2), g.ccbhc_claims, g.ccbhc_benes].join(","));
    }
  }

  return lines.join("\n");
}
