# Session 34 Deployment Guide

Reference for the manual steps needed after Session 34's commit (`7f7204d`).

---

## Step 1: Push + Deploy (Claude can do this)

If Claude hasn't already done this, run:

```bash
cd ~/Desktop/aradune
git push origin main
```

This triggers Vercel frontend deploy via CI. Then deploy the backend:

```bash
fly deploy --remote-only --config server/fly.toml --dockerfile server/Dockerfile
```

Verify the deploy succeeded:

```bash
# Check the app is running
fly status --app aradune-api

# Test an endpoint
curl -s https://aradune-api.fly.dev/api/states | python3 -c "
import sys, json
d = json.load(sys.stdin)
print(f'{len(d)} states returned - deploy OK')
"
```

---

## Step 2: R2 Full Sync (Manual - needs Cloudflare credentials)

### Why this matters

The Fly.io backend downloads the data lake from Cloudflare R2 on cold start. Only ~253 of 760 parquet tables are currently in R2. This means Intelligence, State Profiles, and every structured tool returns incomplete data on production. Locally everything works because the full 4.8 GB lake is on disk.

### Get your credentials

1. Go to https://dash.cloudflare.com
2. Click **R2** in the left sidebar
3. Note your **Account ID** (shown on the R2 overview page, top-right area)
4. Click **Manage R2 API Tokens**
5. Create a new API token (or find your existing one):
   - **Token name:** `aradune-sync` (or whatever you like)
   - **Permissions:** Object Read & Write
   - **Bucket:** `aradune-datalake`
6. After creating, you'll see:
   - **Access Key ID** (looks like: `a1b2c3d4e5f6...`)
   - **Secret Access Key** (looks like: `abc123def456...`)

### Set credentials and run sync

```bash
# Set credentials for this terminal session
export CLOUDFLARE_ACCOUNT_ID="your-account-id-from-step-3"
export AWS_ACCESS_KEY_ID="your-access-key-id-from-step-6"
export AWS_SECRET_ACCESS_KEY="your-secret-access-key-from-step-6"

# Preview what will be uploaded (no actual upload)
cd ~/Desktop/aradune
python3 scripts/sync_lake_wrangler.py --dry-run

# You should see output like:
# Found 865 files (4847 MB) to upload
# (list of files)

# Run the full sync (30-60 minutes, uploads 4.8 GB)
python3 scripts/sync_lake_wrangler.py --workers 8

# Watch for any failures in the output. Each file prints:
# ✓ lake/fact/medicaid_rate/snapshot=2026-03-17/data.parquet (12.3 MB)
# or
# ✗ lake/fact/some_table/data.parquet - FAILED: (error message)
```

### After sync completes

```bash
# Restart Fly.io so it picks up the new files from R2
fly machines restart -a aradune-api

# Wait ~2 minutes for the download to complete on Fly.io, then verify
curl -s https://aradune-api.fly.dev/api/states | python3 -c "
import sys, json
d = json.load(sys.stdin)
print(f'{len(d)} states returned')
"
```

### Credential rotation (optional but recommended)

Since R2 credentials are listed as a known issue (#1), this is a good time to rotate. After the sync works:

1. Go back to Cloudflare R2 > Manage API Tokens
2. Delete the old token
3. Create a new one with the same permissions
4. Update Fly.io with the new credentials:

```bash
fly secrets set \
  AWS_ACCESS_KEY_ID="new-access-key" \
  AWS_SECRET_ACCESS_KEY="new-secret-key" \
  ARADUNE_S3_ENDPOINT="https://your-account-id.r2.cloudflarestorage.com" \
  ARADUNE_S3_BUCKET="aradune-datalake" \
  --app aradune-api
```

---

## Step 3: ANTHROPIC_API_KEY in GitHub Secrets (Manual)

### Why this matters

The new `.github/workflows/adversarial.yml` runs weekly (Sunday 2 AM UTC). It tests the deployed Intelligence endpoint with adversarial queries, evaluates responses with Haiku, and feeds lessons back into the Skillbook. Without this secret, the workflow will fail.

**Cost:** ~$5-9 per weekly run in quick mode = ~$25-40/month.

### How to set it

**Option A: CLI (recommended)**
```bash
cd ~/Desktop/aradune

# This will prompt you to paste the key
gh secret set ANTHROPIC_API_KEY

# When prompted, paste your Anthropic API key (starts with sk-ant-api03-)
# Press Enter, then Ctrl+D (or Enter on a blank line)
```

**Option B: GitHub web UI**
1. Go to https://github.com/Aradune-Policy/aradune-main/settings/secrets/actions
2. Click **New repository secret**
3. **Name:** `ANTHROPIC_API_KEY`
4. **Value:** paste your Anthropic API key (the same one used on Fly.io)
5. Click **Add secret**

### Where to find your API key

If you don't remember your key:
1. Go to https://console.anthropic.com/settings/keys
2. Your existing key is shown (masked). If you can't reveal it, create a new one.
3. Use the same key that's set as a Fly.io secret. To check what's on Fly.io:

```bash
# This won't show the full key, but confirms one is set
fly secrets list --app aradune-api | grep ANTHROPIC
```

### Verify the workflow can run

After setting the secret:

```bash
# Trigger a manual run
gh workflow run adversarial.yml

# Check status
gh run list --workflow=adversarial.yml --limit 1
```

---

## Step 4: Cache Seed Regeneration (Manual - needs running server + API key)

### Why this matters

The 27 pre-cached Intelligence responses in `server/cache_seeds.json` were generated with the OLD system prompt. They may contain:
- Em-dashes (now banned and post-processed out of fresh responses)
- The false FL mutual exclusion rule (now corrected)
- Missing DOGE quarantine caveats (now programmatically enforced)
- Missing data vintages (now strictly enforced in prompt)

In demo mode, or for any first-time query matching a seed, users see these stale responses. The cache is checked BEFORE the LLM runs, so prompt fixes alone don't fix cached answers.

### How to regenerate

**Option A: Against the deployed server (recommended, after Steps 1-2)**
```bash
cd ~/Desktop/aradune

# Set API key for the build script (it calls the Intelligence endpoint)
export ANTHROPIC_API_KEY="sk-ant-api03-..."

# Regenerate (runs 27 queries sequentially, ~5 minutes, ~$1 in API costs)
python3 scripts/build_cache_seeds.py --api-url https://aradune-api.fly.dev

# The script outputs progress like:
# [1/27] "Compare Florida E&M rates to Medicare" ... OK (2,341 chars)
# [2/27] "What is the national Medicaid enrollment?" ... OK (1,892 chars)
# ...
# Wrote 27 seeds to server/cache_seeds.json
```

**Option B: Against a local server**
```bash
# Terminal 1: start the server
cd ~/Desktop/aradune
export ANTHROPIC_API_KEY="sk-ant-api03-..."
cd server
uvicorn main:app --port 8000

# Terminal 2: regenerate seeds
cd ~/Desktop/aradune
export ANTHROPIC_API_KEY="sk-ant-api03-..."
python3 scripts/build_cache_seeds.py --api-url http://localhost:8000
```

### Verify the seeds are clean

```bash
cd ~/Desktop/aradune
python3 -c "
import json

seeds = json.load(open('server/cache_seeds.json'))
issues_found = 0

for s in seeds:
    text = s.get('response', '')
    question = s.get('question', s.get('query', '?'))[:60]
    problems = []

    # Check for em-dashes
    if '\u2014' in text:
        problems.append('em-dash (\u2014)')
    if '\u2013' in text:
        problems.append('en-dash (\u2013)')

    # Check for the false FL rule
    if 'mutual exclusion' in text.lower():
        problems.append('false FL mutual exclusion rule')
    if 'cannot have both' in text.lower() and 'florida' in text.lower():
        problems.append('false FL rule variant')

    # Check for missing data vintage (rough heuristic)
    if len(text) > 200 and not any(y in text for y in ['FY20', 'CY20', '2023', '2024', '2025', '2026']):
        problems.append('possibly missing data vintage')

    if problems:
        issues_found += 1
        print(f'  ISSUE: {question}...')
        for p in problems:
            print(f'         -> {p}')
    else:
        print(f'  OK:    {question}...')

print(f'\n{len(seeds)} seeds total, {issues_found} with issues')
"
```

### Commit and deploy the new seeds

```bash
cd ~/Desktop/aradune
git add server/cache_seeds.json
git commit -m "Regenerate cache seeds with corrected Intelligence prompt

Co-Authored-By: Claude Opus 4.6 (1M context) <noreply@anthropic.com>"
git push

# Redeploy so Fly.io gets the new seeds baked into the Docker image
fly deploy --remote-only --config server/fly.toml --dockerfile server/Dockerfile
```

---

## Step 5: Clerk Live Keys (Manual - needs Clerk dashboard)

### Why this matters

Clerk test keys (prefixed `pk_test_` / `sk_test_`) let the app run but:
- Show a Clerk development mode banner in the UI
- Don't enforce real authentication
- Don't persist user accounts
- Look unprofessional in any demo

For any external demo, Big 5 meeting, or user-facing deployment, you need live keys.

### Check current key status

```bash
# Check what's on Fly.io
fly ssh console --app aradune-api --command "printenv CLERK_SECRET_KEY" 2>/dev/null | head -c 10
# If output starts with "sk_test_" -> test keys
# If output starts with "sk_live_" -> live keys, you're done
```

### Get live keys from Clerk

1. Go to https://clerk.com and sign in
2. Select your Aradune application
3. In the left sidebar, click **API Keys**
4. You'll see two keys:
   - **Publishable key** (starts with `pk_live_...`)
   - **Secret key** (starts with `sk_live_...`)
5. Copy both

### Before switching: configure Clerk sign-in

If you haven't already, make sure your Clerk app has sign-in methods configured:
1. In Clerk dashboard, go to **User & Authentication > Email, Phone, Username**
2. Enable at least one: Email (recommended), Google OAuth, etc.
3. Go to **Customization > Branding** and set your Aradune brand colors/logo if desired

### Update all three deployment targets

**Fly.io (backend):**
```bash
fly secrets set CLERK_SECRET_KEY="sk_live_your_key_here" --app aradune-api
```

**GitHub Secrets (for CI/CD):**
```bash
# Secret key (backend)
gh secret set CLERK_SECRET_KEY
# paste: sk_live_your_key_here

# Publishable key (frontend, injected at build time)
gh secret set VITE_CLERK_PUBLISHABLE_KEY
# paste: pk_live_your_key_here
```

**Vercel (frontend):**
1. Go to https://vercel.com
2. Select the Aradune project
3. Go to **Settings > Environment Variables**
4. Find or create `VITE_CLERK_PUBLISHABLE_KEY`
5. Set value to `pk_live_your_key_here`
6. Make sure it applies to **Production** environment
7. Click **Save**

### Trigger a redeploy

```bash
# Push a no-op change or trigger manually
git push  # if there are pending changes

# Or trigger Vercel manually from the dashboard
# Or:
npx vercel --prod
```

### Verify

1. Go to https://www.aradune.co
2. You should see a proper Clerk sign-in modal (no development banner)
3. Sign up with an email or OAuth
4. Confirm you can access the app after signing in

---

## Recommended Order of Operations

```
1. git push                          <- pushes code to GitHub, triggers Vercel
2. fly deploy                       <- deploys backend with new code
3. R2 full sync                     <- uploads all 865 lake files
4. fly machines restart             <- backend picks up new R2 data
5. Cache seed regeneration          <- clean demo responses
6. gh secret set ANTHROPIC_API_KEY  <- enables weekly adversarial testing
7. Clerk live keys                  <- when ready for external demo (not urgent)
```

Steps 1-2 are the minimum to get Session 34 changes live.
Steps 3-5 make the production data complete and demo-ready.
Steps 6-7 are operational improvements for ongoing quality.
