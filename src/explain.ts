// ── Aradune Explain Engine ──────────────────────────────────────────────
// Template-based explanation generator for ExplainButton.
// Interpolates {{key}} placeholders with data values.

// ── Templates ──────────────────────────────────────────────────────────
export const TEMPLATES: Record<string, string> = {
  stateRateComparison:
    "{{state}}'s Medicaid rate for {{code}} is ${{medicaidRate}}, which is {{pctMedicare}}% of the Medicare non-facility rate (${{medicareRate}}). " +
    "The national median Medicaid-to-Medicare ratio for this code is {{nationalMedian}}%. " +
    "{{state}} ranks #{{rank}} among {{totalStates}} states for this code. " +
    "A ratio below 80% may indicate access concerns under 42 CFR 447.203.",

  codeStateRate:
    "Code {{code}} ({{description}}) has a Medicaid rate of ${{medicaidRate}} in {{state}}. " +
    "The Medicare non-facility rate is ${{medicareRate}} (CY{{medicareYear}}, CF ${{conversionFactor}}). " +
    "This gives a Medicaid-to-Medicare ratio of {{pctMedicare}}%. " +
    "Based on {{claimCount}} claims and {{beneficiaryCount}} beneficiaries.",

  wageAdequacy:
    "In {{state}}, the median hourly wage for {{occupation}} is ${{stateWage}}, compared to the national median of ${{nationalWage}}. " +
    "This represents {{pctNational}}% of the national median. " +
    "The Medicaid reimbursement-implied wage is approximately ${{impliedWage}}/hr, " +
    "suggesting a {{gap}} gap between what Medicaid pays and market wages.",

  qualitySummary:
    "{{state}} reports a {{measure}} rate of {{value}}{{unit}} for {{year}}. " +
    "The national median is {{nationalMedian}}{{unit}}. " +
    "{{state}} ranks #{{rank}} out of {{totalStates}} reporting states. " +
    "{{direction}} values indicate better performance for this measure.",

  perEnrolleeSpending:
    "{{state}}'s per-enrollee Medicaid spending is ${{perEnrollee}} ({{year}}), " +
    "compared to the national average of ${{nationalAvg}}. " +
    "This is {{pctNational}}% of the national average. " +
    "Total Medicaid spending in {{state}} was ${{totalSpending}}B, " +
    "with {{enrollmentK}}K enrollees and an FMAP of {{fmap}}%.",

  enrollmentTrend:
    "{{state}}'s Medicaid enrollment {{direction}} from {{startEnrollment}} to {{endEnrollment}} " +
    "between {{startDate}} and {{endDate}}, a change of {{changePct}}%. " +
    "Key drivers: {{drivers}}. " +
    "The national enrollment trend over the same period was {{nationalChangePct}}%.",
};

// ── Interpolation engine ───────────────────────────────────────────────
export function explain(
  template: string,
  data: Record<string, string | number>,
): string {
  let text = TEMPLATES[template];
  if (!text) {
    // If template name not found, treat the first arg as raw template string
    text = template;
  }

  return text.replace(/\{\{(\w+)\}\}/g, (match, key) => {
    const val = data[key];
    if (val === undefined || val === null) return match;
    return String(val);
  });
}
