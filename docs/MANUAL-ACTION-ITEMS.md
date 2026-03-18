# Aradune Manual Action Items

Updated: Session 34 (2026-03-18). Three items require your credentials. Each has exact commands and verification steps.

---

## 1. R2 Full Data Sync (HIGHEST PRIORITY)

**What:** Upload all 865 local data lake files (~4.8 GB) to Cloudflare R2 so production has complete data.

**Why this matters:** Only ~253 of 760 parquet files are in R2. Every module on production shows incomplete data. The Rate Browse Dashboard shows states with 1-5 codes instead of thousands. The state context panels are missing HPSA and demographics sections. Intelligence queries return partial results. This is the single biggest quality gap on the live site.

**Time:** ~5 minutes of your time + 30-60 minutes of upload time.

### Step 1: Get Cloudflare credentials

1. Go to https://dash.cloudflare.com
2. Click **R2** in the left sidebar
3. Copy your **Account ID** (shown on the R2 overview page, top-right)
4. Click **Manage R2 API Tokens**
5. Click **Create API token** (or find existing)
   - Token name: `aradune-sync`
   - Permissions: **Object Read & Write**
   - Specify bucket: `aradune-datalake`
6. After creating, copy:
   - **Access Key ID** (looks like `a1b2c3d4e5f6...`)
   - **Secret Access Key** (looks like `abc123def456...`)

### Step 2: Run the sync

```bash
cd ~/Desktop/aradune

# Set credentials (this terminal session only)
export CLOUDFLARE_ACCOUNT_ID="paste-account-id-here"
export AWS_ACCESS_KEY_ID="paste-access-key-here"
export AWS_SECRET_ACCESS_KEY="paste-secret-key-here"

# Preview what will upload (no actual upload)
python3 scripts/sync_lake_wrangler.py --dry-run
# Expected: "Found 865 files (4847 MB) to upload"

# Run the full sync (30-60 min depending on connection speed)
python3 scripts/sync_lake_wrangler.py --workers 8
# Each file prints: ✓ lake/fact/medicaid_rate/... (12.3 MB)
# Watch for failures: ✗ lake/fact/... FAILED: (error message)
```

### Step 3: Restart Fly.io to pick up new files

```bash
fly machines restart -a aradune-api
# Wait ~2 minutes for download to complete
```

### Step 4: Verify

```bash
# Check the state context endpoint returns all sections
curl -s https://aradune-api.fly.dev/api/state-context/FL | python3 -c "
import sys, json
d = json.load(sys.stdin)
sections = [k for k in d if k not in ('state_code','state_name')]
print(f'FL context: {len(sections)} sections ({', '.join(sections)})')
missing = {'fiscal','enrollment','access','quality','demographics','rate_adequacy','workforce','hcbs_waitlist','ltss','tmsis_claims','supplemental'} - set(sections)
if missing: print(f'MISSING: {missing}')
else: print('All sections present')
"

# Check rate summary has full data
curl -s https://aradune-api.fly.dev/api/rates/state-summary | python3 -c "
import sys, json
d = json.load(sys.stdin)
print(f'{len(d)} states')
big = [s for s in d if s['total_codes'] > 100]
small = [s for s in d if s['total_codes'] < 10]
print(f'{len(big)} states with 100+ codes, {len(small)} with <10 codes')
if small: print(f'Small states: {[s[\"state_code\"] for s in small[:5]]}')
"
```

### Optional: Rotate R2 credentials (recommended)

After sync works, rotate the credentials to address known issue #1:

1. Go back to Cloudflare R2 > Manage API Tokens
2. Delete the old token
3. Create a new one with same permissions
4. Update Fly.io:

```bash
fly secrets set \
  AWS_ACCESS_KEY_ID="new-access-key" \
  AWS_SECRET_ACCESS_KEY="new-secret-key" \
  ARADUNE_S3_ENDPOINT="https://your-account-id.r2.cloudflarestorage.com" \
  ARADUNE_S3_BUCKET="aradune-datalake" \
  --app aradune-api
```

---

## 2. Fix Clerk Authentication (UNBLOCKS INTELLIGENCE)

**What:** Fix the "Authentication error" that prevents Intelligence from working on the live site.

**Why this matters:** When users ask a question on the Intelligence page, they get `{"detail":"Authentication error"}`. This is because `CLERK_SECRET_KEY` is set on Fly.io, which activates JWT verification, but the key doesn't match the frontend's publishable key (different Clerk app instances or expired keys).

**Time:** ~2 minutes.

### Option A: Quick unblock (remove Clerk, fall back to password gate)

This disables JWT verification and falls back to the client-side password gate ("mediquiad"). Intelligence will work immediately.

```bash
fly secrets unset CLERK_SECRET_KEY --app aradune-api
```

Verify:
```bash
# Should return a response instead of auth error
curl -s -X POST https://aradune-api.fly.dev/api/intelligence \
  -H "Content-Type: application/json" \
  -d '{"message":"What is EPSDT?"}' | python3 -c "
import sys, json
d = json.load(sys.stdin)
if 'response' in d: print(f'Intelligence works! ({len(d[\"response\"])} chars)')
elif 'detail' in d: print(f'Still broken: {d[\"detail\"]}')
else: print(str(d)[:100])
"
```

### Option B: Fix Clerk keys properly (for real auth)

1. Go to https://clerk.com and sign in
2. Find your **Aradune** application (or create one)
3. Go to **API Keys** in the left sidebar
4. Copy both keys:
   - **Publishable key**: starts with `pk_test_` (dev) or `pk_live_` (production)
   - **Secret key**: starts with `sk_test_` (dev) or `sk_live_` (production)

5. Update Fly.io (backend):
```bash
fly secrets set CLERK_SECRET_KEY="sk_test_YOUR_KEY_HERE" --app aradune-api
```

6. Update Vercel (frontend):
   - Go to https://vercel.com > Aradune project > Settings > Environment Variables
   - Set `VITE_CLERK_PUBLISHABLE_KEY` = `pk_test_YOUR_KEY_HERE`
   - Redeploy: `npx vercel --prod` or push a commit

7. Update GitHub secrets (for CI/CD):
```bash
gh secret set CLERK_SECRET_KEY
# paste: sk_test_YOUR_KEY_HERE

gh secret set VITE_CLERK_PUBLISHABLE_KEY
# paste: pk_test_YOUR_KEY_HERE
```

**Important:** Both keys MUST be from the SAME Clerk application. Mismatched keys cause the auth error.

---

## 3. ANTHROPIC_API_KEY in GitHub (ENABLES WEEKLY TESTING)

**What:** Add the Anthropic API key as a GitHub secret so the weekly adversarial testing workflow can run.

**Why this matters:** The adversarial workflow (`.github/workflows/adversarial.yml`) runs every Sunday at 2 AM UTC. It tests Intelligence with adversarial queries, evaluates responses with Haiku, and automatically imports lessons into the Skillbook. Without this secret, the workflow fails silently. Cost: ~$5-9/week.

**Time:** ~1 minute.

### How to do it

```bash
cd ~/Desktop/aradune
gh secret set ANTHROPIC_API_KEY
# When prompted, paste your Anthropic API key (sk-ant-api03-...)
# Press Enter, then Ctrl+D
```

### Where to find your API key

- Go to https://console.anthropic.com/settings/keys
- Use the same key that's on Fly.io. To check:
```bash
fly secrets list --app aradune-api | grep ANTHROPIC
# Should show ANTHROPIC_API_KEY with a timestamp
```

### Verify

```bash
# Trigger a manual run to test
gh workflow run adversarial.yml

# Check status after ~5 minutes
gh run list --workflow=adversarial.yml --limit 1
```

---

## 4. Cache Seed Regeneration (AFTER items 1 + 2)

**What:** Re-generate the 27 pre-cached Intelligence demo responses with the updated system prompt.

**Why this matters:** The current `server/cache_seeds.json` was generated with the old prompt. Cached responses may contain em-dashes, the false FL mutual exclusion rule, missing DOGE caveats, and missing data vintages. Users hitting cached queries see stale answers.

**Prerequisite:** Items 1 (R2 sync) and 2 (Clerk fix) must be done first.

**Time:** ~5 minutes + ~$1 in API costs.

```bash
cd ~/Desktop/aradune
export ANTHROPIC_API_KEY="sk-ant-api03-..."

# Regenerate against the deployed server
python3 scripts/build_cache_seeds.py --api-url https://aradune-api.fly.dev

# Verify the seeds are clean
python3 -c "
import json
seeds = json.load(open('server/cache_seeds.json'))
issues = 0
for s in seeds:
    text = s.get('response', '')
    q = s.get('question', s.get('query', ''))[:50]
    probs = []
    if '\u2014' in text: probs.append('em-dash')
    if 'mutual exclusion' in text.lower(): probs.append('false FL rule')
    if probs:
        issues += 1
        print(f'  ISSUE: {q}... -> {probs}')
    else:
        print(f'  OK: {q}...')
print(f'\n{len(seeds)} seeds, {issues} with issues')
"

# Commit and deploy
git add server/cache_seeds.json
git commit -m 'Regenerate cache seeds with corrected Intelligence prompt'
git push
fly deploy --remote-only --config server/fly.toml --dockerfile server/Dockerfile
```

---

## 5. Raw File Cleanup (OPTIONAL, LOW PRIORITY)

**What:** Delete broken and duplicate files from `data/raw/` to reclaim ~52MB and reduce confusion.

**Why:** Session 34 identified 31 broken files (7 truly empty, 2 HTML/WAF masqueraders) and 11 duplicate `_v2` file pairs. None of these are in the lake or used by any ETL script.

### Safe to delete (duplicates with identical non-v2 counterparts):

```bash
cd ~/Desktop/aradune/data/raw

# These 11 _v2 files are byte-for-byte identical to their non-v2 counterparts
rm medicaid_acute_care_v2.csv
rm part_d_quarterly_spending_v2.csv
rm medicaid_mc_dashboard_v2.csv
rm revoked_providers_v2.csv
rm medicaid_spending_by_drug_v2.csv
rm optout_providers_v2.csv
rm cms64_new_adult_expenditures_v2.csv
rm medicaid_health_screenings_v2.csv
rm ma_geo_variation_v2.csv
rm medicare_telehealth_trends_v2.csv
rm medicaid_1915c_waiver_participants_v2.csv
```

### Safe to delete (empty/broken files):

```bash
# Truly empty or near-empty (0-37 bytes)
rm svi_2022/SVI_2022_US_tract.csv        # 0 bytes
rm bls_state_unemployment.json             # 2 bytes
rm fred_state_poverty.json                 # 2 bytes
rm test_pediatric.json                     # 2 bytes
rm ipf_quality_national_2025.json          # 37 bytes
rm mssp_financial_2024.json                # 37 bytes
rm mssp_financial_py2024.json              # 37 bytes (duplicate of above)

# HTML masquerading as CSV (WAF block pages)
rm svi_county_2022.csv
rm hud_subsidized_housing_state.csv
```

---

## Recommended Order

1. **Clerk fix** (Option A: 30 seconds, unblocks Intelligence immediately)
2. **R2 sync** (5 min setup + 30-60 min upload, fixes all data gaps)
3. **ANTHROPIC_API_KEY** (1 minute, enables weekly quality testing)
4. **Cache seeds** (after 1+2, regenerates demo responses)
5. **Raw cleanup** (optional, when you have a moment)
