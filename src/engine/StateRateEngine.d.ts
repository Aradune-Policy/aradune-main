export function calcRBRVS(rvu: number, cf: number, gpci?: number, fsiMult?: number): number;
export function applyConstraint(target: number, prior: number, isNew?: boolean, maxChange?: number): number;
export function calcComponents(globalFee: number, componentInfo: Record<string, unknown>): { pc: number; tc: number };
export function optimizeCF(codes: unknown[], targetMoney: number, options?: Record<string, unknown>): {
  cf: number; adjustedMoney: number; budgetDiff: number; iterations: number;
  fixedMoney: number; variableCodes: number; detail: unknown[];
};
export function calcFCSOWeightedAvg(rates: Record<string, number | null>, weights: Record<string, number>): {
  weightedAvg: number; localesUsed: number; redistributed: boolean;
};
export function calcAnesthesia(baseUnits: number, timeMinutes: number, timeRate: number, isPediatric?: boolean): {
  fee: number; timeUnits: number;
};
export function applyCrosswalks(utilization: unknown[], crosswalks: unknown[], options?: Record<string, unknown>): unknown;
export function validateAgainstPublished(calculated: unknown[], published: unknown[], options?: Record<string, unknown>): unknown;
export function detectGapPatterns(results: unknown[], stats: unknown): unknown;
export function createTier1Config(params: Record<string, unknown>): unknown;
export function round2(n: number): number;
export function round4(n: number): number;

export const FL_CONFIG: {
  state: string;
  cf: number;
  labCF: number;
  gpci: number;
  fsiMultiplier: number;
  medicaidMultiplier: number;
  maxChange: number;
  schedules: {
    practitioner: {
      legislativeOverrides: Record<string, { rates: { fs?: number; facility?: number; pc?: number; tc?: number }; statute: string }>;
      [key: string]: unknown;
    };
    [key: string]: unknown;
  };
  [key: string]: unknown;
};

export class StateRateEngine {
  constructor(stateConfig: unknown);
  priceCode(code: unknown, scheduleName?: string): unknown;
  priceSchedule(codes: unknown[], scheduleName: string): unknown[];
  calculateCF(codes: unknown[], options?: Record<string, unknown>): unknown;
  applyCrosswalks(utilization: unknown[], options?: Record<string, unknown>): unknown;
  validate(calculated: unknown[], published: unknown[], options?: Record<string, unknown>): unknown;
  getDiagnostics(): unknown;
}
