/**
 * DuckDB-WASM singleton: initializes the database, registers remote Parquet
 * files, and exposes a typed query() function for the rest of the app.
 */

import * as duckdb from "@duckdb/duckdb-wasm";

let dbInstance: duckdb.AsyncDuckDB | null = null;
let connInstance: duckdb.AsyncDuckDBConnection | null = null;
let initPromise: Promise<duckdb.AsyncDuckDBConnection> | null = null;

export type DuckDBRow = Record<string, unknown>;

export interface DuckDBResult {
  rows: DuckDBRow[];
  rowCount: number;
  durationMs: number;
}

const PARQUET_BASE =
  typeof window !== "undefined" ? `${window.location.origin}/data` : "/data";

// External URL for the large monthly file (hosted on R2 or similar).
// Falls back to same-origin /data/ for local dev.
const MONTHLY_URL = import.meta.env.VITE_MONTHLY_PARQUET_URL || `${PARQUET_BASE}/claims_monthly.parquet`;

// Core files always deployed with the app
const CORE_FILES = [
  { name: "claims", file: "claims.parquet" },
  { name: "categories", file: "categories.parquet" },
  { name: "providers", file: "providers.parquet" },
] as const;

// Track whether the monthly file is available
let monthlyAvailable = false;
export function hasMonthlyData(): boolean { return monthlyAvailable; }

async function init(): Promise<duckdb.AsyncDuckDBConnection> {
  // Pick the best bundle for the browser
  const JSDELIVR_BUNDLES = duckdb.getJsDelivrBundles();
  const bundle = await duckdb.selectBundle(JSDELIVR_BUNDLES);

  const worker_url = URL.createObjectURL(
    new Blob([`importScripts("${bundle.mainWorker!}");`], {
      type: "text/javascript",
    })
  );

  const worker = new Worker(worker_url);
  const logger = new duckdb.ConsoleLogger();
  const db = new duckdb.AsyncDuckDB(logger, worker);
  await db.instantiate(bundle.mainModule, bundle.pthreadWorker);
  URL.revokeObjectURL(worker_url);

  const conn = await db.connect();

  // Register core Parquet files (always available)
  for (const pf of CORE_FILES) {
    await db.registerFileURL(
      pf.file,
      `${PARQUET_BASE}/${pf.file}`,
      duckdb.DuckDBDataProtocol.HTTP,
      false
    );
  }

  // Try to register the monthly file (may be externally hosted or unavailable)
  try {
    const probe = await fetch(MONTHLY_URL, { method: "HEAD" });
    if (probe.ok) {
      await db.registerFileURL(
        "claims_monthly.parquet",
        MONTHLY_URL,
        duckdb.DuckDBDataProtocol.HTTP,
        false
      );
      monthlyAvailable = true;
    }
  } catch {
    // Monthly file not available — month grouping will fall back to yearly
  }

  dbInstance = db;
  connInstance = conn;
  return conn;
}

/**
 * Returns the singleton DuckDB connection, initializing on first call.
 * Concurrent callers share the same initialization promise.
 */
export async function getDuckDB(): Promise<duckdb.AsyncDuckDBConnection> {
  if (connInstance) return connInstance;
  if (!initPromise) initPromise = init();
  return initPromise;
}

/**
 * Run a SQL query against the registered Parquet files and return typed rows.
 */
export async function query(sql: string): Promise<DuckDBResult> {
  const conn = await getDuckDB();
  const t0 = performance.now();
  const result = await conn.query(sql);
  const durationMs = performance.now() - t0;

  // Convert Arrow table to plain JS objects
  const rows: DuckDBRow[] = result.toArray().map((row: Record<string, unknown>) => {
    const obj: DuckDBRow = {};
    for (const key of Object.keys(row)) {
      const val = row[key];
      // Convert BigInt to Number for JSON-safe handling
      obj[key] = typeof val === "bigint" ? Number(val) : val;
    }
    return obj;
  });

  return { rows, rowCount: rows.length, durationMs };
}

/**
 * Check if DuckDB-WASM is ready (already initialized).
 */
export function isReady(): boolean {
  return connInstance !== null;
}

/**
 * Close the connection and database (cleanup).
 */
export async function close(): Promise<void> {
  if (connInstance) {
    await connInstance.close();
    connInstance = null;
  }
  if (dbInstance) {
    await dbInstance.terminate();
    dbInstance = null;
  }
  initPromise = null;
}
