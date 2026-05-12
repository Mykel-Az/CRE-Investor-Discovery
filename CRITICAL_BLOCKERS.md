# Fixing Critical Blockers — CRE Investor Discovery

The tool currently returns `status: empty` for all real queries because three data layers
are missing. Fix them in order — each layer depends on the one before it.

---

## Blocker 1 — Roads table is empty

**Why it matters:** Every corridor query runs a PostGIS `ST_DWithin` spatial join between
parcels and roads. If the `roads` table has no rows, the join returns nothing regardless
of how many parcels are loaded.

**Fix:** Load OSM road data via the free Overpass API.

```powershell
npm run load-roads -- nj,ny
```

Add more states as needed (comma-separated, no spaces):

```powershell
npm run load-roads -- nj,ny,fl,tx,ca
```

**Valid state codes:** `nj ny fl tx ca il pa ga co wa az ma nc oh mi`

**Expected output:**
```
[roads] NJ — 12,400 segments inserted, 0 skipped
[roads] NY — 38,200 segments inserted, 0 skipped
```

**If you get 406 / connection errors:** The script automatically tries three Overpass
mirrors. If all fail, wait 5 minutes and retry — Overpass enforces rate limits by IP.

---

## Blocker 2 — Parcel data not loaded

**Why it matters:** Without parcels in the database, there are no properties to match
against road corridors. Requires Python 3.8+.

### Step 2a — Download parcel files (Overture Maps, free)

```powershell
npm run download-parcels -- nj
```

Approximate download sizes per state:

| State | Size |
|-------|------|
| NJ    | ~80 MB |
| NY    | ~350 MB |
| FL    | ~450 MB |
| TX    | ~600 MB |
| CA    | ~700 MB |

Files are saved to `./data/openaddresses/` as `{state}_addresses.geojsonseq`.
Already-downloaded files are skipped automatically.

### Step 2b — Load parcels into the database

```powershell
npm run load-parcels -- nj
```

This streams the GeoJSON file into `parcels_staging` then upserts into `parcels`.
Takes 5–10 minutes per state. Loads address + geometry — **owner names are not
included** (Overture Maps does not provide ownership data — fixed in Blocker 3).

**Expected output:**
```
[parcels] nj_addresses.geojsonseq complete — 1,240,000 loaded, 320 skipped
[parcels] Upserted 1,240,000 rows into parcels.
[parcels] Spatial corridor queries are now enabled.
```

---

## Blocker 3 — owner_name_raw is NULL (no owner data)

**Why it matters:** `owner_name_raw` is the field that drives all entity resolution and
portfolio graph building. Without it, `portfolio_edges` stays empty and the tool returns
owners: [] even when parcels are found along a corridor.

**Root cause:** Overture Maps (used in Blocker 2) provides addresses and geometry only —
no ownership data. Owner names come from ATTOM or county assessor records.

**Fix:** Enrich parcels with owner data from the ATTOM API.

### Step 3a — Start small to validate your ATTOM key

```powershell
npm run enrich-owners -- nj 100
```

This enriches 100 NJ parcels. Check the output:

```
Enriched with owner:  82
Not found in ATTOM:   15
Errors:               3
```

If "Enriched with owner" > 0, your key is working. Scale up:

```powershell
npm run enrich-owners -- nj 2000
```

### Step 3b — Check enrichment cost

ATTOM charges approximately **$0.02 per record**. 2,000 parcels = ~$40.
Start with corridors you care about most before doing bulk enrichment.

### Step 3c — Enrich multiple states

```powershell
npm run enrich-owners -- nj,ny 5000
```

### What gets populated per parcel:
- `owner_name_raw` — raw legal name from deed/tax record
- `property_type` — Retail, Office, Industrial, Multifamily, etc.
- `lot_size_acres` — converted from ATTOM sqft value
- `building_sqft` — gross building area
- `last_sale_date` — most recent recorded sale date
- `last_sale_price` — most recent recorded sale price

---

## After all three blockers are fixed

Once roads, parcels, and owner names are loaded, the background jobs handle the rest
automatically on their nightly schedule:

| Job | Time | What it does |
|-----|------|--------------|
| Entity resolution | 3 AM daily | Matches `owner_name_raw` → SOS entities via exact + fuzzy match |
| Portfolio graph | 4 AM daily | Builds `portfolio_edges` (owner → parcel links) |
| Contact pre-warm | 5 AM daily | Calls Proxycurl + Hunter.io for top-queried owners |

**To trigger entity resolution immediately** without waiting for 3 AM, run:

```powershell
npm run init-db    # safe to re-run — uses CREATE TABLE IF NOT EXISTS
npm run dev        # restart server; jobs run on schedule from boot
```

Or manually verify the data pipeline is working by checking row counts:

```sql
SELECT COUNT(*) FROM roads;           -- should be > 0
SELECT COUNT(*) FROM parcels;         -- should be > 0
SELECT COUNT(*) FROM parcels
WHERE owner_name_raw IS NOT NULL;     -- should be > 0 after ATTOM enrichment
SELECT COUNT(*) FROM entities;        -- should be > 0 after 3 AM job
SELECT COUNT(*) FROM portfolio_edges; -- should be > 0 after 4 AM job
```

Run these against your Railway database using any PostgreSQL client (TablePlus, psql, etc.)
with the `DATABASE_URL` from your `.env`.

---

## Quick reference — full run order for NJ

```powershell
# 1. Roads
npm run load-roads -- nj

# 2. Parcels
npm run download-parcels -- nj
npm run load-parcels -- nj

# 3. Owner enrichment (validate first with 100, then scale)
npm run enrich-owners -- nj 100
npm run enrich-owners -- nj 5000

# 4. Seed smoke test cache (Upstash Redis)
node --env-file=.env --import tsx scripts/seed-smoke-tests.ts

# 5. Wait for nightly jobs (3–5 AM) or restart server and wait for cron
npm run dev
```

After the 3 AM and 4 AM jobs complete, a query like:

```json
{
  "corridor": "Highway 9, NJ",
  "property_type": "Retail",
  "lot_size_min_acres": 0
}
```

will return real owner records with contact data instead of `status: empty`.
