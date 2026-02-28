# Aradune · Medicaid Policy

Cross-state Medicaid fee schedule benchmarking, rate analysis, provider intelligence, and policy impact simulation.

**Live at [aradune.co](https://aradune.co)**

> **First time?** See `SETUP.md` for the complete step-by-step guide including R/Node install, data downloads, Porkbun domain setup, and troubleshooting.

## Quick Deploy (5 minutes)

### Option A: Vercel (recommended)
1. Install Node.js from https://nodejs.org (LTS version)
2. Open terminal in this folder
3. Run:
   ```bash
   npm install
   npm run build
   ```
4. Go to https://vercel.com → Sign up with GitHub/email
5. Click "Add New Project" → "Upload" → drag the `dist` folder
6. Your site is live at `your-project.vercel.app`

### Option B: Vercel CLI (even faster after first time)
```bash
npm install
npm run build
npx vercel --prod
```
Follow the prompts. Done.

## Local Development
```bash
npm install
npm run dev
```
Opens at http://localhost:5173

## Connecting Real T-MSIS Data

### Prerequisites
```bash
# Install R packages (one-time)
Rscript -e 'install.packages(c("duckdb", "jsonlite", "readxl", "data.table"), repos="https://cloud.r-project.org")'
```

### Test with sample data
```bash
cd data
Rscript tmsis_sample_generator.R                    # creates sample CSVs
Rscript tmsis_pipeline_duckdb.R sample_spending.csv  # processes into JSON
```

### Run with real T-MSIS data
1. Move your data files into `data/`:
   - `medicaid-provider-spending.csv` (from opendata.hhs.gov)
   - `npidata_pfile_20050523-20260208.csv` (from download.cms.gov/nppes — **required** for T-MSIS data: provides state, ZIP3, provider names, and taxonomy)
2. Run:
   ```bash
   cd data
   Rscript tmsis_pipeline_duckdb.R medicaid-provider-spending.csv npidata_pfile_20050523-20260208.csv
   ```
   Or without NPPES (only works if your spending file already has a STATE column):
   ```bash
   Rscript tmsis_pipeline_duckdb.R medicaid-provider-spending.csv
   ```
   **Note:** The HHS T-MSIS file has no state column — NPPES is required to map NPIs to states.
3. Outputs JSON to `public/data/` (~5-50MB depending on code count)
4. Rebuild: `npm run build && vercel --prod`

### Why DuckDB?
The full dataset is 227M rows. Loading that into memory with data.table
would need ~13GB RAM. DuckDB streams from disk — 16GB laptop is fine.
It processes ALL 7,550 HCPCS codes, not a subset.

### Adding code descriptions (recommended)
Without reference files, you get ~200 named codes from the built-in reference.
To name all ~7,550, drop one or both of these into `data/`:

**HCPCS Level II (A-V codes: T1019, J3490, D0120, etc.)**
1. Go to https://www.hhs.gov/guidance/document/hcpcs-quarterly-update
2. Download the latest "Alpha-Numeric HCPCS File (ZIP)"
3. Unzip it into `data/` — the pipeline auto-detects the `.txt` file inside

**CPT codes (5-digit numeric: 99213, 90834, etc.)**
1. Go to https://www.cms.gov/medicare/payment/fee-schedules/physician/pfs-relative-value-files
2. Download the current year RVU file (Addendum B, usually Excel)
3. Save/copy it into `data/` — the pipeline reads it directly
4. Or: export the HCPCS+Description columns as CSV, name it `hcpcs_codes.csv`

The pipeline auto-detects all of these and merges them. Later sources override earlier ones.

## Stack
- React 18 + Vite
- Recharts for visualization
- DuckDB + R for data processing (offline, no backend)
- Zero backend — static site, free hosting

## Pipeline outputs
- `states.json` — state-level spending, enrollment, provider counts, FMAP, case mix indices
- `hcpcs.json` — ALL codes with per-state rates, national avg, categories, trends, concentration
- `trends.json` — national yearly spending/claims
- `regions.json` — sub-state ZIP3 geography with spending breakdowns
- `providers.json` — top 200 providers per state with case mix, trends, peer comparison
- `specialties.json` — taxonomy × state cross-comparison (top 200 specialties)
- `meta.json` — pipeline metadata and stats
