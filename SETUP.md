# Aradune Setup Guide
## From Zero to Live Dashboard on a MacBook Pro (16GB)

---

## What You're Building

A live, public Medicaid policy dashboard at **aradune.co** that lets anyone search any HCPCS code and compare rates across all 50 states, drill into sub-state regions, look up individual providers with case mix profiles, compare specialties across states, simulate rate change impacts, and track spending trends — powered by the real T-MSIS dataset from HHS.

---

## Part 1: Install Tools (One-Time, ~10 Minutes)

### 1A. Node.js (builds the website)

Open **Terminal** (Cmd + Space → type "Terminal" → Enter).

```bash
node --version
```

If you see a version number (v18+), skip ahead. If "command not found":

1. Go to **https://nodejs.org**
2. Click the green **LTS** button
3. Double-click the downloaded `.pkg`, click through installer
4. **Close and reopen Terminal**
5. Verify: `node --version`

### 1B. R (runs the data pipeline)

```bash
Rscript --version
```

If not installed:

1. Go to **https://cran.r-project.org/bin/macosx/**
2. Download the `.pkg` for **Apple Silicon** (if your MacBook is 2021 or later) or **Intel**
   - Not sure? Click Apple menu → About This Mac → look for "Apple M1/M2/M3" vs "Intel"
3. Double-click the `.pkg`, install

### 1C. R Packages (one-time)

```bash
Rscript -e 'install.packages(c("duckdb", "jsonlite", "readxl", "data.table"), repos="https://cloud.r-project.org")'
```

This takes 1-2 minutes. You'll see a lot of text — that's normal.

---

## Part 2: Deploy the Prototype (~5 Minutes)

This gets a working dashboard live immediately with simulated data so you can share the URL and get feedback while you set up the real data.

### 2A. Unzip and Build

```bash
cd ~/Desktop/Aradune
unzip aradune-app.zip
cd aradune-app
npm install
npm run build
```

`npm install` takes 30-60 seconds (downloads React, Recharts, Vite).
`npm run build` takes ~5 seconds (creates a `dist` folder).

### 2B. Preview Locally (Optional)

```bash
npm run preview
```

Open **http://localhost:4173** in your browser. You should see the full Aradune dashboard with 7 tabs, 20 states, 10 codes. Press **Ctrl+C** in Terminal to stop.

### 2C. Deploy to Vercel

**Option A: Drag and Drop (easiest first time)**

1. Go to **https://vercel.com** → Sign up (email or GitHub)
2. Click **"Add New..."** → **"Project"**
3. Look for **"Upload"** (ignore the Git import options)
4. Open Finder, navigate to `~/Desktop/Aradune/aradune-app/dist`
5. Drag the entire **`dist`** folder into the Vercel upload area
6. Wait ~10 seconds
7. Your site is live at something like `aradune-app-abc.vercel.app` (you'll point `aradune.co` to this)

**Option B: Vercel CLI (faster for future deploys)**

```bash
npm install -g vercel
cd ~/Desktop/Aradune/aradune-app
vercel --prod
```

First time: it opens a browser to log in, then asks a few questions — hit Enter for all defaults. Gives you a URL when done. Future deploys are just:

```bash
npm run build && vercel --prod
```

**You now have a live, shareable URL.** It shows simulated data with a "PROTOTYPE" badge. The rest of this guide makes it show real data.

---

## Part 2.5: Connect Your Domain via Porkbun (~10 Minutes)

Skip this if you're fine with the default `*.vercel.app` URL. Come back to it anytime.

### Buy the Domain (if you haven't already)

1. Go to **https://porkbun.com**
2. You already have `aradune.co` — skip to step 3
3. Add to cart → checkout. Porkbun includes free WHOIS privacy and free SSL.

### Point the Domain to Vercel

**Option A: Vercel manages DNS (simplest)**

This tells Porkbun to let Vercel handle everything.

1. **In Vercel:**
   - Go to your project → **Settings** → **Domains**
   - Click **"Add"** → type `aradune.co` → click **Add**
   - Vercel will show you the nameservers it wants. They'll look like:
     ```
     ns1.vercel-dns.com
     ns2.vercel-dns.com
     ```

2. **In Porkbun:**
   - Log in → **Domain Management** → click your domain
   - Click **"Nameservers"** (in the left sidebar or under Details)
   - Delete the default Porkbun nameservers
   - Add the two Vercel nameservers from above
   - Click **"Submit"**

3. **Wait 5-30 minutes** for DNS propagation. Vercel will show a green checkmark when it's ready and auto-provisions SSL.

**Option B: Keep Porkbun DNS, add records manually**

Use this if you also use Porkbun for email or other services on the same domain.

1. **In Vercel:**
   - Go to your project → **Settings** → **Domains**
   - Click **"Add"** → type your domain → click **Add**
   - Vercel will show DNS records you need to add. Typically:
     - **A record:** `@` → `76.76.21.21`
     - **CNAME record:** `www` → `cname.vercel-dns.com`

2. **In Porkbun:**
   - Log in → **Domain Management** → click your domain
   - Click **"DNS"** in the left sidebar
   - Add the records:
     - **Type:** A · **Host:** (leave blank for root) · **Answer:** `76.76.21.21` · **TTL:** 600
     - **Type:** CNAME · **Host:** `www` · **Answer:** `cname.vercel-dns.com` · **TTL:** 600
   - Click **"Add"** for each

3. **Wait 5-30 minutes.** Vercel auto-provisions SSL once DNS resolves.

### Verify

After DNS propagates, visit your domain — you should see Aradune. Vercel handles HTTPS automatically. If Vercel still shows "Invalid Configuration," wait a bit longer or double-check the DNS records in Porkbun match exactly.

### Redeploys

Once the domain is connected, every future `vercel --prod` deploy automatically goes live at your custom domain. Nothing extra needed.

---

## Part 3: Set Up Your Data Files

You already have the two core data files. Move them into the project's `data/` folder:

```
~/Desktop/Aradune/
└── aradune-app/
    └── data/
        ├── tmsis_pipeline_duckdb.R              (already here from ZIP)
        ├── tmsis_sample_generator.R             (already here)
        ├── hcpcs_reference.R                    (already here)
        ├── medicaid-provider-spending.csv       ← move here
        └── npidata_pfile_20050523-20260208.csv  ← move here
```

You can leave `medicaid-provider-spending.duckdb` wherever it is — the pipeline reads the CSV.

### Optional: Code Description Files

Without these, ~200 common codes have names from the built-in reference. The other ~7,350 show as "HCPCS 43239" — functional but not labeled. With them, virtually every code gets a human-readable description.

**HCPCS Level II (A-V codes: T1019, J3490, D0120, etc.)**
1. Go to https://www.hhs.gov/guidance/document/hcpcs-quarterly-update
2. Download the latest "Alpha-Numeric HCPCS File (ZIP)"
3. Save the ZIP directly to `data/` — the pipeline auto-unzips it
4. Covers ~6,000 Level II codes

**CPT codes (5-digit numeric: 99213, 90834, etc.)**
1. Go to https://www.cms.gov/medicare/payment/fee-schedules/physician/pfs-relative-value-files
2. Download the current year RVU file (Addendum B, usually Excel .xlsx) — you already have this for RBRVS work
3. Save to `data/`
4. Covers ~10,000 CPT codes

The pipeline auto-detects all reference files and merges them.

---

## Part 4: Run the Pipeline (~5 Minutes)

### 4A. Test with Sample Data First

```bash
cd ~/Desktop/Aradune/aradune-app/data
Rscript tmsis_sample_generator.R
```

This creates 3 small CSV files in ~2 seconds. Then:

```bash
Rscript tmsis_pipeline_duckdb.R sample_spending.csv
```

You should see output like:
```
═══════════════════════════════════════════════════
  Aradune · T-MSIS Pipeline (DuckDB)
═══════════════════════════════════════════════════

Step 1: Locating files...
  [ref] Built-in reference: 200 codes
  ...
Step 11: Exporting JSON...
  → states.json ( 20 states)
  → hcpcs.json ( 33 codes)
  → trends.json ( 7 years)
  → meta.json

  Pipeline complete!
```

Check that `~/Desktop/Aradune/aradune-app/public/data/` now has JSON files.

### 4B. Run with Real Data

```bash
cd ~/Desktop/Aradune/aradune-app/data
Rscript tmsis_pipeline_duckdb.R medicaid-provider-spending.csv npidata_pfile_20050523-20260208.csv
```

The spending CSV goes first. NPPES goes second — no separate providers file needed (the pipeline checks whether the spending file already has a state column).

**Or even simpler:** just drop both files in `data/` and run with only the spending file — the pipeline auto-detects `npidata_pfile_*.csv` in the same directory:

```bash
Rscript tmsis_pipeline_duckdb.R medicaid-provider-spending.csv
```

**Expected runtime on 16GB MacBook:** 2-5 minutes for the full 227M rows. DuckDB streams from disk — you won't run out of memory.

**Expected output:**
```
  States: 51
  HCPCS codes: 7,550 (every code in the dataset)
  Descriptions: 6,200 of 7,550 codes named
  Trends: 500 codes × 7 years
  Concentration: 200 codes
  Regions: 4,200 sub-state areas
  Total spend: $862.4B
```

The JSON files land in `~/Desktop/Aradune/aradune-app/public/data/`. Total size is roughly 5-50 MB depending on how many codes and regions.

---

## Part 5: Connect Data and Redeploy (~2 Minutes)

### 5A. Flip the Switch

The dashboard auto-detects the JSON files. No code change needed — it tries to load `/data/meta.json` on startup and if it finds it, switches from simulated to live data automatically.

If for some reason it doesn't detect the files, you can force it: open `~/Desktop/Aradune/aradune-app/src/App.jsx` in any text editor (TextEdit, VS Code, nano), and the data loading logic is at the top. But this should not be necessary.

### 5B. Rebuild and Redeploy

```bash
cd ~/Desktop/Aradune/aradune-app
npm run build
```

Check locally:

```bash
npm run preview
```

Open http://localhost:4173 — you should see "LIVE DATA" badge instead of "PROTOTYPE", and the code count should match your pipeline output. Ctrl+C to stop.

Deploy:

```bash
vercel --prod
```

Or drag the `dist` folder to Vercel again.

**That's it. Your dashboard is live with real T-MSIS data.**

---

## Part 6: Connect GitHub for Version Control + Auto-Deploy (~10 Minutes)

Without this, every update is manual: edit → build → `vercel --prod`. With GitHub connected, you push a change and Vercel rebuilds automatically in ~30 seconds. You also get full version history so you can roll back anything.

### 6A. Create the Repo

1. Go to **https://github.com** → Sign up or log in
2. Click **"+"** (top right) → **"New repository"**
3. Name: `aradune` (or whatever you want)
4. Set to **Private** (your data JSON files shouldn't be public)
5. Do NOT check "Add a README" (you already have one)
6. Click **"Create repository"**

### 6B. Push Your Project

GitHub shows you the commands. In Terminal:

```bash
cd ~/Desktop/Aradune/aradune-app

# One-time Git setup (skip if you've used Git before)
git config --global user.name "Your Name"
git config --global user.email "your@email.com"

# Initialize and push
git init
git add .
git commit -m "Initial commit"
git branch -M main
git remote add origin https://github.com/aradune-policy/aradune.git
git push -u origin main
```

If it asks for a password, you need a **Personal Access Token** instead:
- GitHub → Settings → Developer settings → Personal access tokens → Tokens (classic)
- Generate new token, check `repo` scope, copy it
- Use the token as your password when Git asks

### 6C. Connect Vercel to GitHub

1. Go to **https://vercel.com** → your project → **Settings** → **Git**
2. Click **"Connect Git Repository"**
3. Select your GitHub account → select the `aradune` repo
4. Vercel auto-detects the Vite framework. Defaults are fine.
5. Click **"Connect"**

From now on, every `git push` triggers an automatic deploy. You'll see the build status in your Vercel dashboard.

### 6D. Daily Workflow After Setup

**To update the site (code changes):**
```bash
cd ~/Desktop/Aradune/aradune-app
# ... make your edits ...
git add .
git commit -m "describe what changed"
git push
```
Vercel auto-deploys in ~30 seconds. Done.

**To refresh the data:**
```bash
cd ~/Desktop/Aradune/aradune-app/data
# (download updated T-MSIS and/or NPPES files here)
Rscript tmsis_pipeline_duckdb.R medicaid-provider-spending.csv npidata_pfile_20050523-20260208.csv
cd ..
git add public/data/
git commit -m "Data refresh: Feb 2026"
git push
```

**To roll back a bad deploy:**
- Vercel dashboard → Deployments → find the last good one → click "..." → "Promote to Production"
- Or: `git revert HEAD && git push`

---

## Quick Reference: All Commands

```bash
# ── ONE-TIME SETUP ──
# Install Node.js from https://nodejs.org
# Install R from https://cran.r-project.org/bin/macosx/
Rscript -e 'install.packages(c("duckdb","jsonlite","readxl","data.table"), repos="https://cloud.r-project.org")'

# ── PROJECT SETUP ──
cd ~/Desktop/Aradune
unzip aradune-app.zip
cd aradune-app
npm install

# ── TEST PIPELINE ──
cd data
Rscript tmsis_sample_generator.R
Rscript tmsis_pipeline_duckdb.R sample_spending.csv

# ── REAL DATA ──
# (move data files into data/ folder first)
Rscript tmsis_pipeline_duckdb.R medicaid-provider-spending.csv npidata_pfile_20050523-20260208.csv

# ── BUILD & DEPLOY ──
cd ~/Desktop/Aradune/aradune-app
npm run build
npm run preview              # check at localhost:4173
vercel --prod                # deploy to production

# ── CUSTOM DOMAIN (Porkbun) ──
# 1. Buy domain at porkbun.com
# 2. In Vercel: Settings → Domains → Add your domain
# 3. In Porkbun: set nameservers to ns1.vercel-dns.com + ns2.vercel-dns.com
#    OR add A record (76.76.21.21) + CNAME www (cname.vercel-dns.com)
# 4. Wait 5-30 min for DNS, Vercel auto-provisions SSL

# ── GITHUB (one-time) ──
git init && git add . && git commit -m "Initial commit"
git branch -M main
git remote add origin https://github.com/aradune-policy/aradune.git
git push -u origin main
# Then: Vercel → Settings → Git → Connect repo

# ── UPDATES (after GitHub is connected) ──
git add . && git commit -m "describe change" && git push
# Vercel auto-deploys in ~30 seconds
```

---

## Files in the ZIP

| File | What It Does |
|------|-------------|
| `SETUP.md` | Full step-by-step guide (this document) |
| `src/App.jsx` | The entire dashboard (React + Recharts, 7 tabs) |
| `data/tmsis_pipeline_duckdb.R` | Processes T-MSIS → JSON via DuckDB |
| `data/tmsis_sample_generator.R` | Creates fake test data for pipeline testing |
| `data/hcpcs_reference.R` | Built-in descriptions for ~200 common codes |
| `package.json` | Node dependencies (React, Recharts, Vite) |
| `vite.config.js` | Build configuration |
| `index.html` | Entry point |
| `src/main.jsx` | React mount |
| `vercel.json` | Vercel routing (SPA fallback) |
| `public/favicon.svg` | Aradune logo |
| `README.md` | Condensed version of this guide |

---

## Troubleshooting

**"command not found: node"** → Close Terminal, reopen, try again. If still fails, reinstall Node.js.

**"command not found: Rscript"** → R isn't in your PATH. Try `/usr/local/bin/Rscript --version` or reinstall R.

**Pipeline crashes with memory error** → Shouldn't happen with DuckDB, but if it does: close other apps, or add `dbExecute(con, "SET memory_limit='6GB'")` to use less RAM.

**Pipeline says "No provider file found"** → This is OK if the spending file already has a state column. The pipeline will use it directly.

**Dashboard still shows "PROTOTYPE" after pipeline** → The JSON files need to be in `public/data/` before you run `npm run build`. Check that `public/data/meta.json` exists, then rebuild.

**Vercel deploy fails** → Make sure you're dragging the `dist` folder, not the project root.

**Excel RVU file won't read** → Install readxl: `Rscript -e 'install.packages("readxl")'`. Or save the Excel file as CSV first.
