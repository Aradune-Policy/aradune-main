# Aradune Deployment Guide

This document covers how to deploy Aradune's frontend (Vercel) and API (Fly.io), set up CI/CD secrets, and handle rollbacks.

---

## Architecture Overview

```
                 ┌──────────────────┐
  Users ────────>│  Vercel (CDN)    │  React SPA at aradune.co
                 │  + Serverless    │  api/chat.js (Policy Analyst)
                 └────────┬─────────┘
                          │ VITE_API_URL
                 ┌────────▼─────────┐
                 │  Fly.io          │  FastAPI at aradune-api.fly.dev
                 │  (iad region)    │  DuckDB over Parquet lake
                 └────────┬─────────┘
                          │ S3-compatible API
                 ┌────────▼─────────┐
                 │  Cloudflare R2   │  Parquet data lake (785MB)
                 │  aradune-datalake│  Synced on container startup
                 └──────────────────┘
```

---

## 1. Prerequisites

- **Node.js 20+** and **npm**
- **Python 3.12+**
- **Vercel CLI**: `npm i -g vercel`
- **Fly.io CLI**: `brew install flyctl` (macOS) or see https://fly.io/docs/getting-started/installing-flyctl/
- **GitHub repo** with push access to `main`

---

## 2. Setting Up GitHub Secrets

The CI workflow (`.github/workflows/ci.yml`) requires four secrets. Set them in your GitHub repo under **Settings > Secrets and variables > Actions > New repository secret**.

| Secret Name | Where to get it | Used by |
|---|---|---|
| `VERCEL_TOKEN` | https://vercel.com/account/tokens -- create a new token | Vercel deploy step |
| `VERCEL_ORG_ID` | Run `vercel link` locally, then check `.vercel/project.json` for `orgId` | Vercel deploy step |
| `VERCEL_PROJECT_ID` | Same `.vercel/project.json` file, `projectId` field | Vercel deploy step |
| `FLY_API_TOKEN` | Run `fly tokens create deploy -x 999999h` or visit https://fly.io/dashboard > Tokens | Fly.io deploy step |

### Step-by-step

1. Go to https://github.com/<your-org>/Aradune/settings/secrets/actions
2. Click **New repository secret**
3. Enter the secret name (e.g., `VERCEL_TOKEN`) and paste the value
4. Repeat for all four secrets

### Getting Vercel IDs

```bash
cd ~/Desktop/Aradune
vercel link                       # follow prompts to link project
cat .vercel/project.json          # shows orgId and projectId
```

### Getting Fly.io Token

```bash
fly tokens create deploy -x 999999h
# Copy the token value (starts with "FlyV1 ...")
```

---

## 3. Environment Variables on Hosting Platforms

Beyond CI secrets, the running applications need env vars set directly on Vercel and Fly.io.

### Vercel Environment Variables

Set via https://vercel.com > Project > Settings > Environment Variables, or CLI:

```bash
# Required for Policy Analyst (api/chat.js)
vercel env add ANTHROPIC_API_KEY           # paste your key

# Required for claims Parquet loading
vercel env add VITE_MONTHLY_PARQUET_URL    # external Parquet URL

# Optional — Policy Analyst auth
vercel env add PREVIEW_TOKEN               # defaults to "mediquiad"
vercel env add ADMIN_KEY                   # master admin key

# Optional — Stripe (Track B only)
vercel env add STRIPE_SECRET_KEY
vercel env add STRIPE_PRICE_ID_INDIVIDUAL
vercel env add STRIPE_PRICE_ID_ORG
```

### Fly.io Secrets

Set via `fly secrets set` (automatically restarts the app):

```bash
cd ~/Desktop/Aradune

# Required for NL2SQL and Intelligence endpoints
fly secrets set ANTHROPIC_API_KEY=sk-ant-...

# R2 credentials for lake sync on startup
fly secrets set AWS_ACCESS_KEY_ID=...
fly secrets set AWS_SECRET_ACCESS_KEY=...

# Verify secrets are set (values are redacted)
fly secrets list
```

Non-secret env vars are in `server/fly.toml` under `[env]` and do not need `fly secrets set`.

---

## 4. CI/CD Pipeline

The GitHub Actions workflow (`.github/workflows/ci.yml`) runs automatically:

### On every push and PR to `main`:
1. **Frontend Build & Typecheck** -- `npm ci`, `npx tsc --noEmit`, `npm run build`
2. **API Lint & Test** -- installs Python deps, verifies FastAPI app loads

### On push to `main` only (after checks pass):
3. **Deploy Frontend (Vercel)** -- builds and deploys to Vercel production
4. **Deploy API (Fly.io)** -- builds Docker image and deploys to Fly.io

Vercel and Fly.io deploys run in parallel since they are independent.

---

## 5. Manual Deployment

### Frontend (Vercel)

```bash
cd ~/Desktop/Aradune

# Build locally first to catch errors
npm ci
npx tsc --noEmit
npm run build

# Deploy to production
npx vercel --prod
```

### API (Fly.io)

```bash
cd ~/Desktop/Aradune

# Deploy (builds Docker image remotely on Fly.io)
fly deploy --config server/fly.toml --remote-only

# Or with local Docker build
fly deploy --config server/fly.toml --local-only
```

### Data Lake Sync (R2)

After adding new Parquet files to `data/lake/`:

```bash
# Upload new/changed files to R2
# Uses npx wrangler because Python boto3 has SSL issues on macOS
npx wrangler r2 object put aradune-datalake/lake/fact/<table>/snapshot=<date>/data.parquet \
  --file data/lake/fact/<table>/snapshot=<date>/data.parquet --remote

# Or use the sync script (requires AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY)
ARADUNE_S3_BUCKET=aradune-datalake \
ARADUNE_S3_ENDPOINT=https://<account-id>.r2.cloudflarestorage.com \
python3 scripts/sync_lake.py upload
```

After syncing to R2, redeploy Fly.io so the container picks up new files on startup:

```bash
fly deploy --config server/fly.toml --remote-only
```

---

## 6. Verifying Deployments

### Frontend (Vercel)

```bash
# Check HTTP status
curl -s -o /dev/null -w "%{http_code}" https://www.aradune.co

# Verify static data files are accessible
curl -s -o /dev/null -w "%{http_code}" https://www.aradune.co/data/cpra_summary.json

# Open in browser
open https://www.aradune.co
```

### API (Fly.io)

```bash
# Health check
curl -s https://aradune-api.fly.dev/api/meta | python3 -m json.tool | head -20

# Check table count
curl -s https://aradune-api.fly.dev/api/lake/stats | python3 -m json.tool

# Check a specific endpoint
curl -s https://aradune-api.fly.dev/api/states | python3 -m json.tool | head -10

# Check NL2SQL is working (requires ANTHROPIC_API_KEY)
curl -s -X POST https://aradune-api.fly.dev/api/nl2sql \
  -H "Content-Type: application/json" \
  -d '{"question": "How many states are in the database?"}' | python3 -m json.tool

# Check Fly.io app status
fly status --config server/fly.toml
fly logs --config server/fly.toml
```

### Smoke Test Checklist

After any deployment, verify these pages load correctly:

- [ ] Landing page: https://www.aradune.co (enter password "mediquiad")
- [ ] Data Explorer: https://www.aradune.co/#/ask
- [ ] Data Catalog: https://www.aradune.co/#/catalog
- [ ] State Profile: https://www.aradune.co/#/state/FL
- [ ] CPRA Generator: https://www.aradune.co/#/cpra
- [ ] Caseload Forecaster: https://www.aradune.co/#/forecast

---

## 7. Rollback Instructions

### Vercel Rollback

Vercel keeps every deployment. Roll back from the dashboard or CLI:

```bash
# List recent deployments
vercel list

# Promote a previous deployment to production
vercel promote <deployment-url>
```

Or use the Vercel dashboard: **Project > Deployments > click the three dots on any deployment > Promote to Production**.

### Fly.io Rollback

Fly.io keeps recent releases:

```bash
# List recent releases
fly releases --config server/fly.toml

# Roll back to the previous release
fly deploy --config server/fly.toml --image <previous-image-ref>
```

Alternatively, revert the git commit and redeploy:

```bash
git revert HEAD
git push origin main
# CI will automatically redeploy the reverted state
```

### Data Lake Rollback

Parquet files are Hive-partitioned with snapshot dates (`snapshot=YYYY-MM-DD`). To roll back a table:

1. Delete the bad snapshot directory from R2
2. Re-upload the previous snapshot (if you still have it locally)
3. Redeploy Fly.io to pick up the change

```bash
# Delete bad snapshot from R2
npx wrangler r2 object delete aradune-datalake/lake/fact/<table>/snapshot=<bad-date>/data.parquet --remote

# Re-upload previous snapshot
npx wrangler r2 object put aradune-datalake/lake/fact/<table>/snapshot=<good-date>/data.parquet \
  --file data/lake/fact/<table>/snapshot=<good-date>/data.parquet --remote

# Redeploy to pick up the change
fly deploy --config server/fly.toml --remote-only
```

---

## 8. Troubleshooting

### Fly.io cold start is slow (~60 seconds)

The container downloads 250+ Parquet files from R2 on startup. This is expected. The health check may fail during sync. Mitigations:

- `auto_start_machines = true` in `fly.toml` means machines spin up on request
- `min_machines_running = 0` saves cost but means the first request after idle is slow
- Set `min_machines_running = 1` if cold start latency is unacceptable

### Vercel deploy fails with "missing token"

The `VERCEL_TOKEN` secret is missing or expired. Generate a new one at https://vercel.com/account/tokens and update the GitHub secret.

### Fly.io deploy fails with "authentication required"

The `FLY_API_TOKEN` secret is missing or expired. Generate a new one:

```bash
fly tokens create deploy -x 999999h
```

Update the GitHub secret with the new value.

### NL2SQL returns "API key not configured"

`ANTHROPIC_API_KEY` is not set on Fly.io:

```bash
fly secrets set ANTHROPIC_API_KEY=sk-ant-...
```

### Python boto3 SSL errors on macOS

Use `npx wrangler` for R2 uploads instead of `python3 scripts/sync_lake.py upload`:

```bash
npx wrangler r2 object put aradune-datalake/lake/... --file ... --remote
```

### CI builds pass but deploy is skipped

Deploys only run on pushes to `main`, not on pull requests. This is intentional. Merge the PR to trigger deployment.
