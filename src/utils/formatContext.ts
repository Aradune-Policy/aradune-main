/**
 * Shared format helpers and symbols for cross-dataset context panels.
 * Used by StateContextBar and module-specific enrichment components.
 */

// Number formatting
export const fmtB = (n: number | null | undefined): string =>
  n == null ? "--" : Math.abs(n) >= 1e9 ? `$${(n / 1e9).toFixed(1)}B`
  : Math.abs(n) >= 1e6 ? `$${(n / 1e6).toFixed(1)}M`
  : `$${n.toLocaleString()}`;

export const fmtPct = (n: number | null | undefined): string =>
  n == null ? "--" : `${Number(n).toFixed(1)}%`;

export const fmtDollar = (n: number | null | undefined): string =>
  n == null ? "--" : `$${Number(n).toFixed(2)}`;

export const fmtNum = (n: number | null | undefined): string =>
  n == null ? "--" : Number(n) >= 1e6 ? `${(Number(n) / 1e6).toFixed(1)}M`
  : Number(n) >= 1e3 ? `${(Number(n) / 1e3).toFixed(1)}K`
  : Number(n).toLocaleString();

export const fmtK = (n: number | null | undefined): string =>
  n == null ? "--" : Number(n) >= 1e6 ? `${(Number(n) / 1e6).toFixed(1)}M`
  : Number(n) >= 1e3 ? `${(Number(n) / 1e3).toFixed(0)}K`
  : Number(n).toLocaleString();

// Section symbols (geometric, consistent across all modules)
export const SYM = {
  fiscal: "\u25C6",       // ◆ filled diamond
  access: "\u25B2",       // ▲ filled triangle
  workforce: "\u25CF",    // ● filled circle
  enrollment: "\u25A0",   // ■ filled square
  quality: "\u25C9",      // ◉ fisheye
  rates: "\u25C7",        // ◇ diamond outline
  pharmacy: "\u2295",     // ⊕ circled plus
  hospitals: "\u25A1",    // □ square outline
  hcbs: "\u25C8",         // ◈ diamond in square
  demographic: "\u25CA",  // ◊ lozenge
  integrity: "\u25D0",    // ◐ half circle
  tmsis: "\u2297",        // ⊗ circled x
  supplemental: "\u25C6", // ◆ filled diamond
  nursing: "\u25CB",      // ○ circle outline
  behavioral: "\u25B3",   // △ triangle outline
} as const;

// Compact one-line summary for a state's context
export function stateContextSummary(ctx: Record<string, any>): string {
  const parts: string[] = [];
  if (ctx.fiscal?.fmap != null) parts.push(`FMAP ${fmtPct(ctx.fiscal.fmap * 100)}`);
  if (ctx.enrollment?.total) parts.push(`${fmtNum(ctx.enrollment.total)} enrolled`);
  if (ctx.enrollment?.mc_pct != null) parts.push(`${ctx.enrollment.mc_pct.toFixed(0)}% MC`);
  if (ctx.access?.hpsa_total) parts.push(`${ctx.access.hpsa_total} HPSAs`);
  if (ctx.rate_adequacy?.median_pct_medicare != null) parts.push(`${ctx.rate_adequacy.median_pct_medicare.toFixed(0)}% MCR`);
  if (ctx.fiscal?.cms64_total) parts.push(fmtB(ctx.fiscal.cms64_total));
  return parts.join(" \u00B7 "); // middle dot separator
}

// T-MSIS styling constants (amber background for claims-based data)
export const TMSIS_BG = "#FFF8ED";
export const TMSIS_BD = "#F0DDB8";
