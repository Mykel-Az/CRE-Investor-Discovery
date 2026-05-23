// scripts/load-pluto.ts
//
// Downloads NYC PLUTO (Primary Land Use Tax Lot Output) and loads commercial
// parcels into the parcels table with owner_name_raw populated.
//
// PLUTO is free from NYC Open Data — no API key required.
// Commercial/industrial properties only (LandUse 04-07).
//
// Usage:
//   npm run load-pluto

import * as fs from 'fs';
import * as path from 'path';
import * as readline from 'readline';
import * as https from 'https';
import pg from 'pg';

const DATA_DIR   = path.resolve('./data/pluto');
const PLUTO_FILE = path.join(DATA_DIR, 'nyc_pluto.csv');
const DB_URL     = process.env.DATABASE_URL ?? 'postgresql://localhost:5432/cre_investor';
const BATCH_SIZE = 500;

// PLUTO v25v4 CSV download
const PLUTO_URL = 'https://data.cityofnewyork.us/api/views/64uk-42ks/rows.csv?accessType=DOWNLOAD';

const pool = new pg.Pool({ connectionString: DB_URL, max: 3 });
pool.on('error', (err) => console.error('[pluto] Pool idle error:', err.message));

// ─── BldgClass → property_type ────────────────────────────────────────────────

function mapBldgClass(cls?: string): string {
  if (!cls) return 'Unknown';
  const c = cls[0]?.toUpperCase();
  if (c === 'O') return 'Office';
  if (c === 'K') return 'Retail';
  if (c === 'L') return 'Retail';        // Loft buildings — often retail/mixed
  if (c === 'E') return 'Industrial';    // Warehouses
  if (c === 'F') return 'Industrial';    // Factories
  if (c === 'G') return 'Industrial';    // Garages
  if (c === 'H') return 'Hospitality';   // Hotels
  if (c === 'D') return 'Multifamily';   // Elevator apartments
  if (c === 'C') return 'Multifamily';   // Walk-up apartments
  if (c === 'R') return 'Multifamily';   // Condos
  if (c === 'S') return 'Mixed-Use';     // Various
  return 'Unknown';
}

// Commercial land-use codes to include (PLUTO stores as single digits: 4=Mixed, 5=Commercial, 6=Industrial, 7=Transportation)
const COMMERCIAL_LAND_USE = new Set(['4', '5', '6', '7']);

// ─── Download ─────────────────────────────────────────────────────────────────

async function downloadPluto(): Promise<void> {
  if (fs.existsSync(PLUTO_FILE)) {
    const stat = fs.statSync(PLUTO_FILE);
    if (stat.size > 10_000_000) {
      console.log(`[pluto] Using cached file (${(stat.size / 1e6).toFixed(0)}MB)`);
      return;
    }
  }

  console.log('[pluto] Downloading NYC PLUTO from NYC Open Data...');
  console.log('[pluto] This may take a few minutes (~150MB)...');
  fs.mkdirSync(DATA_DIR, { recursive: true });

  await new Promise<void>((resolve, reject) => {
    const file = fs.createWriteStream(PLUTO_FILE);
    let downloaded = 0;

    function doGet(url: string): void {
      https.get(url, (res) => {
        if (res.statusCode === 301 || res.statusCode === 302) {
          doGet(res.headers.location!);
          return;
        }
        if (res.statusCode !== 200) {
          reject(new Error(`HTTP ${res.statusCode}`));
          return;
        }
        res.on('data', (chunk: Buffer) => {
          downloaded += chunk.length;
          if (downloaded % 10_000_000 < chunk.length) {
            process.stdout.write(`  ${(downloaded / 1e6).toFixed(0)}MB downloaded...\r`);
          }
        });
        res.pipe(file);
        file.on('finish', () => { file.close(); resolve(); });
      }).on('error', reject);
    }

    doGet(PLUTO_URL);
  });

  const size = fs.statSync(PLUTO_FILE).size;
  console.log(`\n[pluto] Download complete — ${(size / 1e6).toFixed(1)}MB`);
}

// ─── Retry helper ─────────────────────────────────────────────────────────────

async function withRetry<T>(fn: (client: pg.PoolClient) => Promise<T>, maxRetries = 5): Promise<T> {
  for (let attempt = 0; attempt <= maxRetries; attempt++) {
    const client = await pool.connect();
    client.setMaxListeners(100);
    client.once('error', () => {});
    try {
      const result = await fn(client);
      client.release();
      return result;
    } catch (err: unknown) {
      client.release(true);
      const msg = err instanceof Error ? err.message : String(err);
      if (attempt >= maxRetries) throw err;
      const wait = 3000 * (attempt + 1);
      console.error(`[pluto] DB retry ${attempt + 1}: ${msg}, waiting ${wait / 1000}s...`);
      await new Promise(r => setTimeout(r, wait));
    }
  }
  throw new Error('unreachable');
}

// ─── Insert batch ─────────────────────────────────────────────────────────────

async function insertBatch(batch: {
  ids: string[]; addrs: string[]; cities: string[]; states: string[];
  zips: string[]; lngs: number[]; lats: number[];
  owners: (string | null)[]; types: string[]; lots: (number | null)[];
}): Promise<void> {
  // Deduplicate by parcel_id within the batch — PLUTO has duplicate BBLs
  const seen = new Map<string, number>();
  const dedup = { ids: [] as string[], addrs: [] as string[], cities: [] as string[],
    states: [] as string[], zips: [] as string[], lngs: [] as number[], lats: [] as number[],
    owners: [] as (string | null)[], types: [] as string[], lots: [] as (number | null)[] };
  for (let i = 0; i < batch.ids.length; i++) {
    if (!seen.has(batch.ids[i])) {
      seen.set(batch.ids[i], i);
      dedup.ids.push(batch.ids[i]);   dedup.addrs.push(batch.addrs[i]);
      dedup.cities.push(batch.cities[i]); dedup.states.push(batch.states[i]);
      dedup.zips.push(batch.zips[i]);  dedup.lngs.push(batch.lngs[i]);
      dedup.lats.push(batch.lats[i]);  dedup.owners.push(batch.owners[i]);
      dedup.types.push(batch.types[i]); dedup.lots.push(batch.lots[i]);
    }
  }
  if (dedup.ids.length === 0) return;

  await withRetry((client) => client.query(`
    INSERT INTO parcels
      (parcel_id, address, city, state, zip, geom, source,
       owner_name_raw, property_type, lot_size_acres)
    SELECT
      unnest($1::text[]), unnest($2::text[]), unnest($3::text[]),
      unnest($4::text[]), unnest($5::text[]),
      ST_SetSRID(ST_MakePoint(unnest($6::float8[]), unnest($7::float8[])), 4326)::geography,
      'pluto',
      unnest($8::text[]),
      unnest($9::text[]),
      unnest($10::float8[])
    ON CONFLICT (parcel_id) DO UPDATE SET
      owner_name_raw = COALESCE(EXCLUDED.owner_name_raw, parcels.owner_name_raw),
      property_type  = CASE WHEN EXCLUDED.property_type != 'Unknown'
                            THEN EXCLUDED.property_type ELSE parcels.property_type END,
      lot_size_acres = COALESCE(EXCLUDED.lot_size_acres, parcels.lot_size_acres),
      updated_at     = NOW()
  `, [
    dedup.ids, dedup.addrs, dedup.cities, dedup.states, dedup.zips,
    dedup.lngs, dedup.lats, dedup.owners, dedup.types, dedup.lots,
  ]));

  batch.ids = []; batch.addrs = []; batch.cities = []; batch.states = [];
  batch.zips = []; batch.lngs = []; batch.lats = [];
  batch.owners = []; batch.types = []; batch.lots = [];
}

// ─── Main ─────────────────────────────────────────────────────────────────────

await downloadPluto();

console.log('\n[pluto] Parsing CSV and loading commercial parcels...');

// Parse header row first
const headerStream = fs.createReadStream(PLUTO_FILE);
const headerRl = readline.createInterface({ input: headerStream, crlfDelay: Infinity });
let headers: string[] = [];
for await (const line of headerRl) {
  headers = line.split(',').map(h => h.replace(/^"|"$/g, '').toLowerCase().trim());
  headerRl.close();
  break;
}
headerStream.destroy();

// Column indices
const col = (name: string) => headers.indexOf(name);
const iAddr     = col('address');
const iZip      = col('postcode');
const iOwner    = col('ownername');
const iBBL      = col('bbl');
const iLandUse  = col('landuse');
const iBldgCls  = col('bldgclass');
const iLotArea  = col('lotarea');
const iLat      = col('latitude');
const iLng      = col('longitude');
const iBorough  = col('borough');

console.log('[pluto] Header columns found:', { iAddr, iZip, iOwner, iBBL, iLandUse, iLat, iLng });

let totalLoaded = 0;
let skipped = 0;
let commercialOnly = 0;

const batch = {
  ids: [] as string[], addrs: [] as string[], cities: [] as string[],
  states: [] as string[], zips: [] as string[],
  lngs: [] as number[], lats: [] as number[],
  owners: [] as (string | null)[], types: [] as string[], lots: [] as (number | null)[],
};

// Borough → city name
const boroughCity: Record<string, string> = {
  MN: 'Manhattan', BX: 'Bronx', BK: 'Brooklyn', QN: 'Queens', SI: 'Staten Island',
};

const rl = readline.createInterface({
  input: fs.createReadStream(PLUTO_FILE),
  crlfDelay: Infinity,
});

let lineNum = 0;
for await (const line of rl) {
  lineNum++;
  if (lineNum === 1) continue; // skip header

  // Parse CSV (simple — PLUTO fields don't contain commas in practice)
  const fields = line.split(',').map(f => f.replace(/^"|"$/g, '').trim());
  if (fields.length < headers.length - 5) { skipped++; continue; }

  const landUse = fields[iLandUse]?.trim();
  if (!COMMERCIAL_LAND_USE.has(landUse)) { skipped++; continue; }

  commercialOnly++;

  const lat = parseFloat(fields[iLat]);
  const lng = parseFloat(fields[iLng]);
  if (isNaN(lat) || isNaN(lng) || lat === 0 || lng === 0) { skipped++; continue; }

  const bbl     = fields[iBBL]?.trim();
  const address = fields[iAddr]?.trim();
  const zip     = fields[iZip]?.trim() ?? '';
  const owner   = fields[iOwner]?.trim() || null;
  const borough = fields[iBorough]?.trim() ?? '';
  const city    = boroughCity[borough] ?? 'New York';
  const lotSqft = parseFloat(fields[iLotArea]);
  const lotAcres = isNaN(lotSqft) || lotSqft <= 0 ? null : lotSqft / 43_560;
  const propType = mapBldgClass(fields[iBldgCls]);

  if (!bbl || !address) { skipped++; continue; }

  const parcelId = `ny_bbl_${bbl}`;

  batch.ids.push(parcelId);
  batch.addrs.push(address);
  batch.cities.push(city);
  batch.states.push('NY');
  batch.zips.push(zip);
  batch.lngs.push(lng);
  batch.lats.push(lat);
  batch.owners.push(owner);
  batch.types.push(propType);
  batch.lots.push(lotAcres);

  if (batch.ids.length >= BATCH_SIZE) {
    await insertBatch(batch);
    totalLoaded += BATCH_SIZE;
    if (totalLoaded % 10_000 === 0) {
      process.stdout.write(`  ${totalLoaded.toLocaleString()} commercial parcels loaded...\r`);
    }
  }
}

if (batch.ids.length > 0) {
  const rem = batch.ids.length;
  await insertBatch(batch);
  totalLoaded += rem;
}

console.log(`\n[pluto] Done.`);
console.log(`  Commercial parcels loaded: ${totalLoaded.toLocaleString()}`);
console.log(`  Non-commercial skipped:    ${skipped.toLocaleString()}`);
console.log(`  Total PLUTO rows scanned:  ${(lineNum - 1).toLocaleString()}`);
console.log('\n[pluto] owner_name_raw is now populated for NYC commercial parcels.');
console.log('[pluto] Run npm run resolve-entities to match owners to SOS entities.');

await pool.end();
