// ── Aradune Search Parser ───────────────────────────────────────────────
// Parses natural-language queries into structured route + params results.
// Used by PlatformSearch, Intelligence routing, and nav bar.

import { STATE_NAMES, STATES_LIST } from "./data/states";
import { SYNONYMS } from "./data/synonyms";

// ── Types ──────────────────────────────────────────────────────────────
export interface SearchResult {
  route: string;
  params: Record<string, string>;
  label: string;
  priority: number;
}

// ── State lookup helpers ───────────────────────────────────────────────
// Build reverse map: lowercase name → code, lowercase abbreviation → code
const NAME_TO_CODE: Record<string, string> = {};
const ABBR_TO_CODE: Record<string, string> = {};

for (const [code, name] of Object.entries(STATE_NAMES)) {
  NAME_TO_CODE[name.toLowerCase()] = code;
  ABBR_TO_CODE[code.toLowerCase()] = code;
}

// ── Intent keywords → route mapping ────────────────────────────────────
const INTENT_MAP: Record<string, { route: string; label: string }> = {
  rates:       { route: "rates",             label: "Rate analysis" },
  rate:        { route: "rates",             label: "Rate analysis" },
  spending:    { route: "spending",          label: "Spending analysis" },
  expenditure: { route: "spending",          label: "Spending analysis" },
  quality:     { route: "workforce",         label: "Quality linkage" },
  wages:       { route: "workforce",         label: "Wage adequacy" },
  wage:        { route: "workforce",         label: "Wage adequacy" },
  workforce:   { route: "workforce",         label: "Workforce analysis" },
  forecast:    { route: "forecast",          label: "Caseload forecast" },
  forecasting: { route: "forecast",          label: "Caseload forecast" },
  hospitals:   { route: "providers",         label: "Hospital intelligence" },
  hospital:    { route: "providers",         label: "Hospital intelligence" },
  pharmacy:    { route: "pharmacy",          label: "Pharmacy intelligence" },
  drug:        { route: "pharmacy",          label: "Pharmacy intelligence" },
  drugs:       { route: "pharmacy",          label: "Pharmacy intelligence" },
  behavioral:  { route: "behavioral-health", label: "Behavioral health" },
  "behavioral health": { route: "behavioral-health", label: "Behavioral health" },
  "mental health":     { route: "behavioral-health", label: "Behavioral health" },
  nursing:     { route: "nursing",           label: "Nursing facility" },
  integrity:   { route: "integrity",         label: "Program integrity" },
  fraud:       { route: "integrity",         label: "Program integrity" },
  cpra:        { route: "cpra",              label: "CPRA compliance" },
  compliance:  { route: "cpra",              label: "CPRA compliance" },
  enrollment:  { route: "forecast",          label: "Enrollment data" },
  providers:   { route: "providers",         label: "Provider intelligence" },
  provider:    { route: "providers",         label: "Provider intelligence" },
};

// ── HCPCS code pattern ─────────────────────────────────────────────────
// Matches standard HCPCS/CPT patterns: 5 digits, or letter + 4 digits
const HCPCS_RE = /\b([A-Z]\d{4}|\d{5})\b/gi;

// ── Fuzzy state extraction ─────────────────────────────────────────────
function extractState(query: string): { code: string; name: string } | null {
  const lower = query.toLowerCase();
  const words = lower.split(/\s+/);

  // Check exact abbreviation match (2-letter words)
  for (const w of words) {
    const cleaned = w.replace(/[^a-z]/g, "");
    if (cleaned.length === 2 && ABBR_TO_CODE[cleaned]) {
      return { code: ABBR_TO_CODE[cleaned], name: STATE_NAMES[ABBR_TO_CODE[cleaned]] };
    }
  }

  // Check full state name match (longest match first for "New York", "West Virginia", etc.)
  const sortedNames = Object.entries(NAME_TO_CODE).sort(
    (a, b) => b[0].length - a[0].length,
  );
  for (const [name, code] of sortedNames) {
    if (lower.includes(name)) {
      return { code, name: STATE_NAMES[code] };
    }
  }

  // Fuzzy: check if any word starts with a state name (e.g., "flor" → Florida)
  for (const w of words) {
    if (w.length < 3) continue;
    for (const [name, code] of sortedNames) {
      if (name.startsWith(w) && w.length >= 3) {
        return { code, name: STATE_NAMES[code] };
      }
    }
  }

  return null;
}

// ── Detect HCPCS codes in query ────────────────────────────────────────
function extractCodes(query: string): string[] {
  const matches = query.match(HCPCS_RE);
  if (!matches) return [];
  return [...new Set(matches.map(m => m.toUpperCase()))];
}

// ── Detect service synonyms ───────────────────────────────────────────
function extractServiceCodes(query: string): { service: string; codes: string[] }[] {
  const lower = query.toLowerCase().trim();
  const results: { service: string; codes: string[] }[] = [];

  // Sort by key length descending so "physical therapy" matches before "therapy"
  const sorted = Object.entries(SYNONYMS).sort(
    (a, b) => b[0].length - a[0].length,
  );

  const matched = new Set<string>();
  for (const [term, codes] of sorted) {
    if (lower.includes(term) && !matched.has(term)) {
      matched.add(term);
      // Filter to actual HCPCS-like codes (skip category labels)
      const hcpcs = codes.filter(c => /^[A-Z0-9]{2,5}$/i.test(c) || /^[A-Z]\d{4}$/i.test(c));
      if (hcpcs.length > 0) {
        results.push({ service: term, codes: hcpcs });
      }
    }
  }

  return results;
}

// ── Detect intent keywords ─────────────────────────────────────────────
function extractIntents(query: string): { route: string; label: string }[] {
  const lower = query.toLowerCase();
  const results: { route: string; label: string }[] = [];
  const seen = new Set<string>();

  // Check multi-word intents first (longest match)
  const sorted = Object.entries(INTENT_MAP).sort(
    (a, b) => b[0].length - a[0].length,
  );

  for (const [keyword, info] of sorted) {
    if (lower.includes(keyword) && !seen.has(info.route)) {
      seen.add(info.route);
      results.push(info);
    }
  }

  return results;
}

// ── Main parser ────────────────────────────────────────────────────────
export function parseSearch(query: string): SearchResult[] {
  if (!query || query.trim().length < 2) return [];

  const results: SearchResult[] = [];
  const state = extractState(query);
  const codes = extractCodes(query);
  const services = extractServiceCodes(query);
  const intents = extractIntents(query);
  const stateLabel = state ? ` in ${state.name}` : "";

  // 1. Direct HCPCS code lookups (highest priority)
  for (const code of codes) {
    results.push({
      route: "rates",
      params: { code, ...(state ? { state: state.code } : {}) },
      label: `Rate lookup: ${code}${stateLabel}`,
      priority: 1,
    });
  }

  // 2. Service synonym → code lookup
  for (const { service, codes: serviceCodes } of services) {
    const codeStr = serviceCodes.slice(0, 3).join(", ");
    results.push({
      route: "rates",
      params: {
        q: service,
        ...(state ? { state: state.code } : {}),
      },
      label: `${capitalize(service)} rates${stateLabel} (${codeStr}...)`,
      priority: 2,
    });
  }

  // 3. Intent-based routing
  for (const intent of intents) {
    // Skip "rates" intent if we already have code/service results pointing there
    if (intent.route === "rates" && results.some(r => r.route === "rates")) continue;

    results.push({
      route: intent.route,
      params: state ? { state: state.code } : {},
      label: `${intent.label}${state ? ` for ${state.name}` : ""}`,
      priority: 3,
    });
  }

  // 4. State profile if state detected but no other results, or as fallback
  if (state && results.length === 0) {
    results.push({
      route: `state/${state.code}`,
      params: { state: state.code },
      label: `${state.name} state profile`,
      priority: 2,
    });
  } else if (state) {
    // Add state profile as lower-priority option
    results.push({
      route: `state/${state.code}`,
      params: { state: state.code },
      label: `${state.name} state profile`,
      priority: 5,
    });
  }

  // Sort by priority (lower = higher priority), deduplicate by route+params
  const seen = new Set<string>();
  return results
    .sort((a, b) => a.priority - b.priority)
    .filter(r => {
      const key = `${r.route}|${JSON.stringify(r.params)}`;
      if (seen.has(key)) return false;
      seen.add(key);
      return true;
    });
}

// ── Utility ────────────────────────────────────────────────────────────
function capitalize(s: string): string {
  return s.charAt(0).toUpperCase() + s.slice(1);
}
