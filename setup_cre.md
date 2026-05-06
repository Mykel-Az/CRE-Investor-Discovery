# CRE Investor Discovery MCP — Setup Manual (Railway)

This guide walks you through deploying the project on [Railway](https://railway.app). Railway handles hosting, HTTPS, and managed databases — no server configuration required.

---

## Overview

The setup has two parts:

1. **Railway** — hosts the live Node.js app, PostgreSQL database, and Redis
2. **Your local machine** — runs the one-time data pipeline (download + load ~18M parcel records into the Railway database)

---

## What You Need Before Starting

| Requirement | Where to get it |
|-------------|-----------------|
| Node.js 22+ | [nodejs.org](https://nodejs.org/) |
| Python 3.8+ | [python.org/downloads](https://www.python.org/downloads/) |
| Railway account | [railway.app](https://railway.app) — free to sign up |
| Railway CLI | `npm install -g @railway/cli` |
| API keys | Proxycurl, Hunter.io, ATTOM (see Step 3) |

---

## Part A — Railway Setup

### Step 1 — Create a Railway Project

1. Log in to [railway.app](https://railway.app)
2. Click **New Project**
3. Choose **Empty Project**
4. Name it `cre-investor-discovery`

---

### Step 2 — Add PostgreSQL and Redis

Inside your Railway project:

**Add PostgreSQL:**
1. Click **+ Add Service** → **Database** → **PostgreSQL**
2. Railway provisions a PostgreSQL instance automatically
3. Click on the PostgreSQL service → **Variables** tab
4. Copy the `DATABASE_URL` value — you will need this shortly

**Add Redis:**
1. Click **+ Add Service** → **Database** → **Redis**
2. Click on the Redis service → **Variables** tab
3. Copy the `REDIS_URL` value

> Railway's PostgreSQL supports PostGIS — the init script enables it automatically.

---

### Step 3 — Deploy the App

**Option A — Via GitHub (recommended):**
1. Push the project folder to a private GitHub repository
2. In Railway, click **+ Add Service** → **GitHub Repo**
3. Select your repository
4. Railway auto-detects Node.js and uses `railway.toml` to build and start

**Option B — Via Railway CLI:**
```bash
# Install CLI and log in
npm install -g @railway/cli
railway login

# From inside the project folder
railway link        # select your project
railway up          # deploys the code
```

---

### Step 4 — Set Environment Variables on Railway

In Railway, click on your app service → **Variables** tab → add each of the following:

| Variable | Value |
|----------|-------|
| `DATABASE_URL` | Paste from the PostgreSQL service |
| `REDIS_URL` | Paste from the Redis service |
| `PROXYCURL_API_KEY` | Your Proxycurl key ([nubela.co/proxycurl](https://nubela.co/proxycurl/)) |
| `HUNTER_API_KEY` | Your Hunter.io key ([hunter.io/api](https://hunter.io/api)) |
| `ATTOM_API_KEY` | Your ATTOM key ([api.attomdata.com](https://api.attomdata.com/)) |
| `PORT` | `3000` |
| `OPENADDRESSES_DATA_DIR` | `./data/openaddresses` |
| `SOS_DATA_DIR` | `./data/sos` |
| `CORRIDOR_SEARCH_RADIUS_METERS` | `400` |

Railway will automatically redeploy after you save the variables.

---

### Step 5 — Get Your Public URL

Once deployed:
1. Click on your app service → **Settings** tab
2. Under **Networking**, click **Generate Domain**
3. Railway assigns a public HTTPS URL like `https://cre-investor-discovery-production.up.railway.app`

Test it:
```
GET https://your-app.up.railway.app/health
```
Expected response: `{"status":"ok","service":"cre-investor-discovery-mcp","version":"1.0.0"}`

---

## Part B — Load Parcel Data (Run Locally, Targets Railway DB)

The parcel data (~18M rows for NJ + NY) is loaded from your local machine directly into the Railway database. This only needs to be done once.

### Step 6 — Install Local Dependencies

```bash
cd cre-investor-discovery-mcp
npm install
```

### Step 7 — Create a Local .env Pointing at Railway

```bash
cp .env.example .env
```

Open `.env` and set `DATABASE_URL` and `REDIS_URL` to the values copied from Railway in Step 2. Leave everything else as-is for now.

```env
DATABASE_URL=postgresql://postgres:xxxxx@monorail.proxy.rlwy.net:12345/railway
REDIS_URL=redis://default:xxxxx@monorail.proxy.rlwy.net:67890
```

> Railway exposes a public proxy URL for each database — use that, not the internal URL.

### Step 8 — Initialize the Database Schema

```bash
npm run init-db
```

Expected output:
```
[init-db] Connected to postgresql://***@monorail.proxy.rlwy.net:12345/railway
[init-db] PostGIS extension enabled.
[init-db] Schema applied successfully.
```

### Step 9 — Download Parcel Data

Downloads address and geometry data from Overture Maps. Requires Python — the `overturemaps` package installs automatically.

```bash
npm run download-parcels -- nj,ny
```

**Available state codes:** `nj` `ny` `fl` `tx` `ca` `il` `pa` `ga` `co` `wa` `az` `ma` `nc` `oh` `mi`

Estimated download sizes:
| State | Size |
|-------|------|
| NJ | ~80 MB |
| NY | ~350 MB |
| FL | ~450 MB |
| TX | ~600 MB |
| CA | ~700 MB |

> Downloads retry automatically up to 4 times on network failure.

### Step 10 — Load Parcels into the Railway Database

```bash
npm run load-parcels -- nj,ny
```

This streams ~18M rows over the network into Railway's PostgreSQL. It takes **15–30 minutes** depending on your internet connection.

Expected output:
```
[parcels] parcels_staging truncated.
[parcels] Loading nj_addresses.geojsonseq...
[parcels] nj_addresses.geojsonseq complete — 4,894,219 loaded, 201 skipped
[parcels] Loading ny_addresses.geojsonseq...
[parcels] ny_addresses.geojsonseq complete — 13,847,127 loaded, 3428 skipped
[parcels] Upserting parcels_staging → parcels...
[parcels] Done. 18,741,346 total address rows loaded into parcels.
[parcels] Spatial corridor queries are now enabled.
```

Once this completes, the app on Railway is fully live and ready to handle queries.

---

## Troubleshooting

### App deploys but crashes immediately
- Check Railway logs: click your app service → **Deployments** → click the latest deployment → **View Logs**
- Most likely cause: a missing environment variable. Double-check all variables in Step 4.

### `init-db` fails with PostGIS error
Railway's PostgreSQL supports PostGIS but it must be enabled per-database. The `init-db` script does this automatically. If it still fails, connect to the database manually and run:
```sql
CREATE EXTENSION IF NOT EXISTS postgis;
```

### `load-parcels` connection times out
Railway databases have connection limits on free plans. If you hit this, reduce `BATCH_SIZE` in `scripts/load-parcels.ts` from `500` to `100` and retry.

### Download fails with `AWS Error NETWORK_CONNECTION`
Transient network issue. The script retries automatically. If all 4 attempts fail, wait a few minutes and re-run — already-downloaded files are skipped automatically.

### Health check returns 502
The app is still starting up. Wait 30 seconds and try again. If it persists, check the deployment logs.

---

## Summary Checklist

**Railway (one-time):**
- [ ] Create Railway project
- [ ] Add PostgreSQL service → copy `DATABASE_URL`
- [ ] Add Redis service → copy `REDIS_URL`
- [ ] Deploy app (GitHub or CLI)
- [ ] Set all environment variables in Railway dashboard
- [ ] Generate public domain for the app

**Local machine (one-time data load):**
- [ ] `npm install`
- [ ] Set `DATABASE_URL` and `REDIS_URL` in `.env` to Railway proxy URLs
- [ ] `npm run init-db`
- [ ] `npm run download-parcels -- nj,ny`
- [ ] `npm run load-parcels -- nj,ny`

**Verify:**
- [ ] `GET https://your-app.up.railway.app/health` returns `{"status":"ok",...}`
