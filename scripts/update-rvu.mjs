#!/usr/bin/env node
/**
 * RVU Update Pipeline
 * Downloads CMS Physician Fee Schedule RVU data and rebuilds medicare_rates.json.
 *
 * Usage:
 *   node scripts/update-rvu.mjs                  # Uses latest downloaded file in data/
 *   node scripts/update-rvu.mjs --download 26a   # Downloads RVU26A release, then builds
 *   node scripts/update-rvu.mjs --year 2026      # Specify calendar year for metadata
 *
 * The CMS RVU files follow this URL pattern:
 *   https://www.cms.gov/files/zip/rvu{YY}{a|b|c|d}-updated-{MM}-{DD}-{YYYY}.zip
 *
 * Quarterly releases: a=January, b=April, c=July, d=October
 *
 * Output: public/data/medicare_rates.json
 */

import fs from "fs";
import path from "path";
import { createReadStream } from "fs";
import { createInterface } from "readline";
import { execSync } from "child_process";
import { fileURLToPath } from "url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const DATA_DIR = path.join(ROOT, "data");
const OUTPUT = path.join(ROOT, "public", "data", "medicare_rates.json");

// ── CLI Args ────────────────────────────────────────────────────────────
const args = process.argv.slice(2);
let downloadRelease = null;
let calendarYear = null;

for (let i = 0; i < args.length; i++) {
  if (args[i] === "--download" && args[i + 1]) downloadRelease = args[++i];
  if (args[i] === "--year" && args[i + 1]) calendarYear = parseInt(args[++i]);
}

// ── Download (optional) ─────────────────────────────────────────────────
async function downloadRvu(release) {
  // CMS URL pattern: https://www.cms.gov/medicare/payment/fee-schedules/physician/pfs-relative-value-files/rvu{release}
  // The actual ZIP URL requires scraping or known patterns. We use the landing page approach.
  const landingUrl = `https://www.cms.gov/medicare/payment/fee-schedules/physician/pfs-relative-value-files/rvu${release}`;

  console.log(`\nFetching RVU release page: ${landingUrl}`);
  console.log("Note: CMS ZIP URLs change with each update. If auto-download fails,");
  console.log("manually download from the URL above and place the CSV in data/\n");

  // Try known URL patterns for recent releases
  const year = `20${release.slice(0, 2)}`;
  const knownUrls = [
    `https://www.cms.gov/files/zip/rvu${release}.zip`,
    `https://www.cms.gov/files/zip/rvu${release}-updated.zip`,
  ];

  if (!fs.existsSync(DATA_DIR)) fs.mkdirSync(DATA_DIR, { recursive: true });

  for (const url of knownUrls) {
    console.log(`Trying: ${url}`);
    try {
      const zipPath = path.join(DATA_DIR, `rvu${release}.zip`);
      execSync(`curl -fsSL -o "${zipPath}" "${url}"`, { timeout: 60000 });
      console.log(`Downloaded: ${zipPath}`);

      // Extract CSV
      execSync(`unzip -o "${zipPath}" "PPRRVU*nonQPP*.csv" -d "${DATA_DIR}"`, { timeout: 30000 });
      console.log("Extracted PPRRVU CSV successfully");
      return year;
    } catch {
      // Try next URL
    }
  }

  console.error(`\nCould not auto-download. Please manually download from:`);
  console.error(`  ${landingUrl}`);
  console.error(`Extract the PPRRVU*nonQPP*.csv file into: ${DATA_DIR}/`);
  process.exit(1);
}

// ── Find RVU CSV ────────────────────────────────────────────────────────
function findRvuFile() {
  if (!fs.existsSync(DATA_DIR)) {
    console.error(`Data directory not found: ${DATA_DIR}`);
    console.error("Run with --download or place PPRRVU*nonQPP*.csv in data/");
    process.exit(1);
  }

  const files = fs.readdirSync(DATA_DIR).filter(f =>
    f.startsWith("PPRRVU") && f.includes("nonQPP") && f.endsWith(".csv")
  );

  if (!files.length) {
    console.error("No PPRRVU*nonQPP*.csv found in data/");
    console.error("Run with --download or manually place the file.");
    process.exit(1);
  }

  // Use the most recent file
  files.sort().reverse();
  return path.join(DATA_DIR, files[0]);
}

// ── Parse RVU CSV ───────────────────────────────────────────────────────
async function parseRvuFile(csvPath) {
  console.log(`\nParsing: ${path.basename(csvPath)}`);

  const rates = {};
  let cf = null;
  let headerFound = false;
  let rowCount = 0;

  const rl = createInterface({ input: createReadStream(csvPath, "utf-8") });

  for await (const rawLine of rl) {
    // Strip BOM
    const line = rawLine.replace(/^\uFEFF/, "");

    // Parse CSV (handles quoted fields)
    const row = parseCSVLine(line);

    // Find header row
    if (!headerFound) {
      if (row.length > 0 && row[0].trim() === "HCPCS") {
        headerFound = true;
      }
      continue;
    }

    if (row.length < 13) continue;

    const hcpcs = row[0].trim();
    const mod = row[1].trim();
    const desc = row[2].trim();
    const status = row[3].trim();
    const work = parseFloat(row[5]) || 0;
    const nfTotal = parseFloat(row[11]) || 0;
    const facTotal = parseFloat(row[12]) || 0;

    // Extract conversion factor from column 25 (first valid row)
    if (!cf && row.length > 25) {
      const rawCf = parseFloat(row[25]);
      if (rawCf > 20 && rawCf < 50) cf = rawCf; // Sanity check (CF is ~$33)
    }

    // Skip bundled (B), tracking (T), carrier-priced (C), non-payable (N)
    if (["B", "T", "C", "N"].includes(status)) continue;

    // Prefer base code (no modifier); skip modifier duplicates
    if (mod && hcpcs in rates) continue;

    const nfRate = nfTotal > 0 ? Math.round(nfTotal * (cf || 33.4009) * 100) / 100 : 0;
    const facRate = facTotal > 0 ? Math.round(facTotal * (cf || 33.4009) * 100) / 100 : 0;

    if (nfRate > 0 || facRate > 0) {
      const entry = {
        r: nfRate,
        fr: facRate,
        rvu: Math.round(nfTotal * 10000) / 10000,
        w: Math.round(work * 100) / 100,
      };
      if (desc) entry.d = desc.slice(0, 60);
      rates[hcpcs] = entry;
      rowCount++;
    }
  }

  if (!cf) cf = 33.4009; // Fallback to CY2026

  console.log(`  Conversion factor: $${cf}`);
  console.log(`  Codes parsed: ${rowCount}`);

  return { rates, cf };
}

// ── CSV line parser (handles quoted fields) ─────────────────────────────
function parseCSVLine(line) {
  const result = [];
  let current = "";
  let inQuotes = false;

  for (let i = 0; i < line.length; i++) {
    const ch = line[i];
    if (inQuotes) {
      if (ch === '"' && line[i + 1] === '"') {
        current += '"';
        i++;
      } else if (ch === '"') {
        inQuotes = false;
      } else {
        current += ch;
      }
    } else {
      if (ch === '"') {
        inQuotes = true;
      } else if (ch === ",") {
        result.push(current);
        current = "";
      } else {
        current += ch;
      }
    }
  }
  result.push(current);
  return result;
}

// ── Detect year from filename ───────────────────────────────────────────
function detectYear(csvPath) {
  const match = path.basename(csvPath).match(/PPRRVU(\d{4})/);
  return match ? parseInt(match[1]) : new Date().getFullYear();
}

// ── Main ────────────────────────────────────────────────────────────────
async function main() {
  console.log("═══════════════════════════════════════════════════");
  console.log("  RVU Update Pipeline — Aradune");
  console.log("═══════════════════════════════════════════════════");

  if (downloadRelease) {
    await downloadRvu(downloadRelease);
  }

  const csvPath = findRvuFile();
  const year = calendarYear || detectYear(csvPath);
  const { rates, cf } = await parseRvuFile(csvPath);

  // Build output
  const output = { rates, cf, year };
  const json = JSON.stringify(output);

  // Read current file for comparison
  let prevCount = 0;
  if (fs.existsSync(OUTPUT)) {
    try {
      const prev = JSON.parse(fs.readFileSync(OUTPUT, "utf-8"));
      prevCount = Object.keys(prev.rates || {}).length;
    } catch { /* ignore */ }
  }

  fs.writeFileSync(OUTPUT, json);

  const codeCount = Object.keys(rates).length;
  const sizeMb = (Buffer.byteLength(json) / 1024 / 1024).toFixed(2);

  console.log(`\n  Output: ${OUTPUT}`);
  console.log(`  Year: CY${year}`);
  console.log(`  Conversion Factor: $${cf}`);
  console.log(`  Codes: ${codeCount}${prevCount ? ` (prev: ${prevCount}, Δ${codeCount - prevCount})` : ""}`);
  console.log(`  Size: ${sizeMb} MB`);
  console.log("\n  Done.\n");
}

main().catch(err => {
  console.error("Error:", err.message);
  process.exit(1);
});
