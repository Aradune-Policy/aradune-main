// ── Aradune Type System ─────────────────────────────────────────────────
// Organized types for the entire frontend. Imported where needed.

import type React from "react";

// ── Recharts Tooltip ────────────────────────────────────────────────────
// Recharts injects these props into <Tooltip content={...}/> children.
export interface TooltipEntry<T = Record<string, unknown>> {
  payload: T;
  value?: number | string;
  name?: string;
  dataKey?: string | number;
  color?: string;
}

export interface SafeTipProps<T = Record<string, unknown>> {
  active?: boolean;
  payload?: TooltipEntry<T>[];
  render: (d: T) => React.ReactNode;
}

// ── T-MSIS Explorer: State Data ─────────────────────────────────────────
export interface StateData {
  name: string;
  spend: number;
  enroll: number;
  pe: number;
  fmap: number;
  mc: number;
  provs: number;
  em: number;
  hcbs: number;
  bh: number;
  dn: number;
  pi?: number;
  mi?: number;
}

// ── T-MSIS Explorer: HCPCS Code ─────────────────────────────────────────
export interface TrendPoint {
  y: number;
  v: number;
}

export interface Concentration {
  t1: number;
  t5: number;
  t10: number;
  gi: number;
}

export interface HcpcsCode {
  c: string;
  d: string;
  cat: string;
  na: number;
  nc: number;
  ns?: number;
  nst?: number;
  np?: number;
  r: Record<string, number>;
  tr?: TrendPoint[] | null;
  cn?: Concentration | null;
}

// ── T-MSIS Explorer: National Trends ────────────────────────────────────
export interface NatlTrend {
  y: number;
  s: number;
  e: number;
  pe: number;
}

// ── T-MSIS Explorer: Category Accumulator ───────────────────────────────
export interface CatAccumulator {
  cat: string;
  s1W: number;
  s2W: number;
  s3W: number;
  naW: number;
  w: number;
  n: number;
  s2n: number;
  s3n: number;
}

// ── T-MSIS Explorer: Raw Pipeline Data ──────────────────────────────────
export interface RawState {
  state: string;
  total_spend?: number;
  est_enrollment?: number;
  per_enrollee?: number;
  fmap?: number;
  n_providers?: number;
  em_provs?: number;
  hcbs_provs?: number;
  bh_provs?: number;
  dental_provs?: number;
  price_index?: number;
  mix_index?: number;
}

export interface RawHcpcs {
  code?: string;
  desc?: string;
  category?: string;
  national_avg?: number;
  national_claims?: number;
  national_spend?: number;
  n_states?: number;
  n_providers?: number;
  rates?: Record<string, number>;
  trend?: { year: number; avg_rate: number }[];
  concentration?: {
    top1_pct: number;
    top5_pct: number;
    top10_pct: number;
    gini: number;
  };
}

export interface RawTrend {
  year: number;
  total_spend?: number;
  total_bene?: number;
}

export interface PipelineMeta {
  live: boolean;
  source?: string;
  [key: string]: unknown;
}

// ── T-MSIS Explorer: Medicare Reference Data ────────────────────────────
export interface MedicareRateEntry {
  r: number;
  fr?: number;
  rvu?: number;
  w?: number;
  d?: string;
}

export interface MedicareRates {
  rates: Record<string, MedicareRateEntry>;
  cf?: number;
  year?: number;
}

export interface RiskAdjState {
  factor: number;
  adjusted_pe?: number;
  mix?: number;
}

export interface RiskAdjData {
  states: Record<string, RiskAdjState>;
}

export interface FeeScheduleState {
  rates: Record<string, number | { r: number }>;
}

export interface FeeScheduleData {
  states: Record<string, FeeScheduleState>;
}

export interface FeeScheduleDirectory {
  directory: unknown[];
  compiled?: string;
  count?: number;
}

// ── T-MSIS Explorer: Provider / Specialty Data ──────────────────────────
export interface ProviderRecord {
  npi: string;
  name: string;
  specialty?: string;
  taxonomy?: string;
  state?: string;
  claims?: number;
  paid?: number;
  [key: string]: unknown;
}

export interface SpecialtyRecord {
  taxonomy: string;
  description: string;
  count?: number;
  claims?: number;
  [key: string]: unknown;
}

// ── Quality Linkage ─────────────────────────────────────────────────────
export interface MeasureHcpcsInfo {
  codes: string[];
  desc?: string;
  [key: string]: unknown;
}

export interface MeasureMeta {
  domain: string;
  name: string;
  rate_def?: string;
  median?: number;
  n_states?: number;
  [key: string]: unknown;
}

export interface QualData {
  measure_hcpcs: Record<string, MeasureHcpcsInfo>;
  measures: Record<string, MeasureMeta>;
  rates: Record<string, Record<string, number>>;
}

export interface LinkedMeasure {
  id: string;
  codes: string[];
  domain: string;
  name: string;
  desc?: string;
  rate_def?: string;
  median?: number;
  n_states?: number;
  [key: string]: unknown;
}

export interface QualHcpcsRecord {
  code?: string;
  c?: string;
  r?: Record<string, number>;
  rates_by_state?: { state: string; avg_rate: number }[];
  [key: string]: unknown;
}

// ── Rate Builder ────────────────────────────────────────────────────────
export interface MethodField {
  id: string;
  label: string;
  type: string;
  min: number;
  max: number;
  default: number;
  step: number;
  unit: string;
}

export interface RateComponent {
  label: string;
  value: string;
  note?: string;
  bold?: boolean;
}

export interface RateResult {
  rate: number;
  formula: string;
  components: RateComponent[];
}

export interface ComputeContext {
  medicareRate?: number;
  rvu?: number;
  peerRates?: { st: string; rate: number }[];
  stateRates?: number[];
}

export interface Methodology {
  id: string;
  name: string;
  desc: string;
  fields: MethodField[];
  compute: (inputs: Record<string, number>, ctx: ComputeContext) => RateResult | null;
}

export interface BenchmarkCode {
  code: string;
  desc: string;
  category: string;
}

export interface RateBuilderHcpcs {
  code?: string;
  c?: string;
  desc?: string;
  d?: string;
  rates?: Record<string, number>;
  r?: Record<string, number>;
  rates_by_state?: { state: string; avg_rate: number }[];
  [key: string]: unknown;
}

export interface RateBuilderMedicare {
  rates: Record<string, {
    r: number;
    fr?: number;
    rvu?: number;
    w?: number;
    d?: string;
  }>;
  cf?: number;
  year?: number;
}

// ── Rate Decay ──────────────────────────────────────────────────────────
export interface DecayHcpcs {
  code?: string;
  c?: string;
  desc?: string;
  d?: string;
  category?: string;
  cat?: string;
  rates?: Record<string, number>;
  r?: Record<string, number>;
  national_avg?: number;
  na?: number;
  national_claims?: number;
  nc?: number;
  trend?: { year: number; avg_rate: number }[];
  tr?: TrendPoint[];
  [key: string]: unknown;
}

// ── Wage Adequacy ───────────────────────────────────────────────────────
export interface WageCategory {
  label: string;
  occ_code: string;
  bls_wage: number;
  overhead_range: [number, number];
  hcpcs: string[];
  unit_minutes: number;
  desc?: string;
}

// ── Policy Analyst ──────────────────────────────────────────────────────
export interface ChatMessage {
  role: "user" | "assistant";
  content: string;
}

export interface ChatUsage {
  tool_rounds?: number;
  input_tokens?: number;
  output_tokens?: number;
  remaining?: number;
}

// ── Platform ────────────────────────────────────────────────────────────
export interface ToolDef {
  id: string;
  name: string;
  tagline: string;
  desc: string;
  status: string;
  icon: string;
  color: string;
}
