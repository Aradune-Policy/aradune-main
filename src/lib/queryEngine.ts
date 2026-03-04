/**
 * Query engine: replaces the FastAPI client with local DuckDB-WASM execution.
 * Builds SQL from QueryRequest and runs it against Parquet files in the browser.
 */

import { query, getDuckDB, hasMonthlyData } from "./duckdb";
import { getPreset, listPresets } from "./presets";
import type { QueryRequest, QueryResponse, QueryMeta, PresetInfo } from "../types";

// ── Column mapping (mirrors server/query_builder.py) ─────────────────
const GROUP_COLUMNS: Record<string, string> = {
  state: "state",
  hcpcs_code: "hcpcs_code",
  category: "category",
  claim_month: "claim_month",
  claim_year: "year",
  zip3: "zip3",
  billing_npi: "npi",
  taxonomy: "taxonomy",
};

// Resolve HCPCS codes including preset expansion
function resolveHcpcsCodes(req: QueryRequest): string[] {
  let codes = [...(req.hcpcs_codes || [])];
  if (req.preset) {
    const preset = getPreset(req.preset);
    if (preset?.codes?.length && preset.filter_type === "hcpcs_codes") {
      codes = [...new Set([...codes, ...preset.codes])];
    }
  }
  return codes;
}

// Which Parquet file to query based on group-by and filters
function pickTable(req: QueryRequest, resolvedCodes: string[]): string {
  const gb = req.group_by || [];
  // Provider-level queries (NPI, ZIP3, Taxonomy all live in providers.parquet)
  if (gb.includes("billing_npi") || gb.includes("zip3") || gb.includes("taxonomy") ||
      req.npi?.length || req.provider_name || req.zip3?.length) {
    return "'providers.parquet'";
  }
  // Monthly granularity — requires claims_monthly.parquet
  if (gb.includes("claim_month")) {
    if (hasMonthlyData()) return "'claims_monthly.parquet'";
    // Monthly data unavailable — fall back to yearly via claims.parquet
    // (buildSQL will remap claim_month → year)
    return "'claims.parquet'";
  }
  // Category-only rollups (faster, smaller file) — but only if no code-level filters
  if (
    gb.every(g => ["state", "category", "claim_year"].includes(g)) &&
    !resolvedCodes.length
  ) {
    return "'categories.parquet'";
  }
  // Default: claims (state × code × year)
  return "'claims.parquet'";
}

function buildSQL(req: QueryRequest): string {
  const where: string[] = [];
  const hcpcsCodes = resolveHcpcsCodes(req);
  const table = pickTable(req, hcpcsCodes);

  // WHERE clauses
  if (req.states?.length) {
    where.push(`state IN (${req.states.map(s => `'${esc(s)}'`).join(",")})`);
  }
  if (hcpcsCodes.length && table !== "'providers.parquet'") {
    where.push(`hcpcs_code IN (${hcpcsCodes.map(c => `'${esc(c)}'`).join(",")})`);
  }
  if (req.categories?.length) {
    where.push(`category IN (${req.categories.map(c => `'${esc(c)}'`).join(",")})`);
  }

  // Provider-specific filters
  if (table === "'providers.parquet'") {
    if (req.npi?.length) {
      where.push(`npi IN (${req.npi.map(n => `'${esc(n)}'`).join(",")})`);
    }
    if (req.taxonomy?.length) {
      where.push(`taxonomy IN (${req.taxonomy.map(t => `'${esc(t)}'`).join(",")})`);
    }
    if (req.provider_name) {
      where.push(`provider_name ILIKE '%${esc(req.provider_name)}%'`);
    }
    if (req.zip3?.length) {
      where.push(`zip3 IN (${req.zip3.map(z => `'${esc(z)}'`).join(",")})`);
    }
  }

  // Date range
  if (req.date_from && table !== "'providers.parquet'") {
    if (table === "'claims_monthly.parquet'") {
      where.push(`claim_month >= '${esc(req.date_from)}'`);
    } else {
      const yearFrom = parseInt(req.date_from.substring(0, 4));
      if (!isNaN(yearFrom)) where.push(`year >= ${yearFrom}`);
    }
  }
  if (req.date_to && table !== "'providers.parquet'") {
    if (table === "'claims_monthly.parquet'") {
      where.push(`claim_month <= '${esc(req.date_to)}'`);
    } else {
      const yearTo = parseInt(req.date_to.substring(0, 4));
      if (!isNaN(yearTo)) where.push(`year <= ${yearTo}`);
    }
  }

  // SELECT + GROUP BY
  const selectParts: string[] = [];
  const groupParts: string[] = [];

  for (const gb of req.group_by || []) {
    const col = GROUP_COLUMNS[gb];
    if (!col) continue;
    if (gb === "claim_year") {
      selectParts.push(`${col} AS claim_year`);
      groupParts.push(col);
    } else if (gb === "claim_month") {
      if (table === "'claims_monthly.parquet'") {
        selectParts.push(`${col} AS claim_month`);
        groupParts.push(col);
      } else {
        // Monthly data unavailable — fall back to year grouping
        selectParts.push("year AS claim_year");
        groupParts.push("year");
      }
    } else if (gb === "billing_npi") {
      selectParts.push("npi");
      if (table === "'providers.parquet'") selectParts.push("provider_name");
      groupParts.push("npi");
      if (table === "'providers.parquet'") groupParts.push("provider_name");
    } else {
      selectParts.push(col);
      groupParts.push(col);
    }
  }

  // Metrics
  selectParts.push("SUM(total_paid) AS total_paid");
  selectParts.push("SUM(total_claims) AS total_claims");
  selectParts.push("SUM(total_beneficiaries) AS total_beneficiaries");
  selectParts.push("COUNT(*) AS row_count");

  if (req.include_avg_rate) {
    selectParts.push(
      "CASE WHEN SUM(total_claims) > 0 THEN SUM(total_paid) / SUM(total_claims) ELSE 0 END AS avg_rate"
    );
  }
  if (req.include_per_bene) {
    selectParts.push(
      "CASE WHEN SUM(total_beneficiaries) > 0 THEN SUM(total_paid) / SUM(total_beneficiaries) ELSE 0 END AS per_bene"
    );
  }

  // HAVING
  const having: string[] = [];
  if (req.min_claims != null) {
    having.push(`SUM(total_claims) >= ${Number(req.min_claims)}`);
  }
  if (req.min_beneficiaries != null) {
    having.push(`SUM(total_beneficiaries) >= ${Number(req.min_beneficiaries)}`);
  }

  // ORDER BY
  let orderCol = req.order_by || "total_paid";
  let orderDir = (req.order_dir || "desc").toUpperCase();
  if (orderDir !== "ASC") orderDir = "DESC";
  if (req.preset === "top_spending") {
    orderCol = "total_paid";
    orderDir = "DESC";
  }

  const limit = Math.min(req.limit || 100, 10000);
  const offset = Math.max(req.offset || 0, 0);

  const sql = [
    `SELECT ${selectParts.join(", ")}`,
    `FROM ${table}`,
    where.length ? `WHERE ${where.join(" AND ")}` : "",
    groupParts.length ? `GROUP BY ${groupParts.join(", ")}` : "",
    having.length ? `HAVING ${having.join(" AND ")}` : "",
    `ORDER BY ${orderCol} ${orderDir}`,
    `LIMIT ${limit} OFFSET ${offset}`,
  ]
    .filter(Boolean)
    .join(" ");

  return sql;
}

/** Escape single quotes in SQL string literals. */
function esc(s: string): string {
  return s.replace(/'/g, "''");
}

// ── Public API (mirrors old queryClient.ts) ──────────────────────────

export async function executeQuery(req: QueryRequest): Promise<QueryResponse> {
  const sql = buildSQL(req);
  const result = await query(sql);
  return {
    rows: result.rows,
    total_rows: result.rowCount,
    query_ms: result.durationMs,
    sql_preview: sql.length > 200 ? sql.substring(0, 200) + "..." : sql,
  };
}

export async function fetchMeta(): Promise<QueryMeta> {
  const [statesResult, categoriesResult, dateResult] = await Promise.all([
    query("SELECT DISTINCT state FROM 'claims.parquet' ORDER BY state"),
    query("SELECT DISTINCT category FROM 'claims.parquet' ORDER BY category"),
    query("SELECT MIN(year) AS date_min, MAX(year) AS date_max FROM 'claims.parquet'"),
  ]);

  const states = statesResult.rows.map(r => String(r.state));
  const categories = categoriesResult.rows.map(r => String(r.category));
  const dateRow = dateResult.rows[0] || {};

  return {
    states,
    categories,
    date_min: String(dateRow.date_min || ""),
    date_max: String(dateRow.date_max || ""),
    columns: ["state", "hcpcs_code", "category", "year", "total_paid", "total_claims", "total_beneficiaries"],
    total_rows: 0, // Will be set from meta.json
    presets: Object.keys(listPresets()),
  };
}

export function fetchPresets(): PresetInfo[] {
  return listPresets();
}

/**
 * Initialize DuckDB-WASM. Call on component mount.
 * Returns true if successful, throws on failure.
 */
export async function initEngine(): Promise<boolean> {
  await getDuckDB();
  return true;
}

export async function searchProviders(
  name: string,
  state?: string,
  limit = 50
): Promise<Record<string, unknown>[]> {
  const where: string[] = [];
  if (name) where.push(`provider_name ILIKE '%${esc(name)}%'`);
  if (state) where.push(`state = '${esc(state)}'`);

  const sql = `
    SELECT npi, provider_name, state, zip3, taxonomy,
           total_paid, total_claims, total_beneficiaries, code_count
    FROM 'providers.parquet'
    ${where.length ? "WHERE " + where.join(" AND ") : ""}
    ORDER BY total_paid DESC
    LIMIT ${limit}
  `;
  const result = await query(sql);
  return result.rows;
}
