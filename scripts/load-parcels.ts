// scripts/load-parcels.ts
//
// Streams *.geojsonseq files from ./data/openaddresses/ into parcels_staging,
// then upserts staging → parcels.
//
// Usage:
//   npm run load-parcels              # loads all *.geojsonseq files
//   npm run load-parcels -- nj,ny    # loads only nj_addresses.geojsonseq and ny_addresses.geojsonseq

import * as fs from 'fs';
import * as path from 'path';
import * as readline from 'readline';
import pg from 'pg';

const DATA_DIR   = path.resolve(process.env.OPENADDRESSES_DATA_DIR ?? './data/openaddresses');
const DB_URL     = process.env.DATABASE_URL ?? 'postgresql://localhost:5432/cre_investor';
const BATCH_SIZE = 500;
const MAX_RETRIES = 10;

// ─── GeoJSON feature types ─────────────────────────────────────────────────

interface AddressProperties {
  id?:            string;
  number?:        string;
  street?:        string;
  postcode?:      string;
  postal_city?:   string;
  address_levels?: Array<{ value?: string; short_code?: string }>;
}

interface GeoJSONFeature {
  type:       string;
  id?:        string;
  geometry:   { type: string; coordinates: [number, number] };
  properties: AddressProperties;
}

// ─── Helpers ──────────────────────────────────────────────────────────────────

function parseState(levels: AddressProperties['address_levels']): string {
  const top = levels?.[0];
  if (top?.short_code) return top.short_code.replace(/^US-/, '');
  if (top?.value)      return top.value.slice(0, 2).toUpperCase();
  return '';
}

function parseCity(levels: AddressProperties['address_levels'], postalCity?: string): string {
  const last = levels?.[levels.length - 1];
  return last?.value ?? postalCity ?? '';
}

function buildAddress(num?: string, street?: string): string {
  return [num, street].filter(Boolean).join(' ');
}

function sleep(ms: number): Promise<void> {
  return new Promise(r => setTimeout(r, ms));
}

// ─── Per-operation client helper ──────────────────────────────────────────────
//
// Creates a FRESH pg.Client for every operation — no pool reuse, no listener
// accumulation. Railway's proxy (viaduct.proxy.rlwy.net) drops connections and
// briefly takes its DNS offline when it resets; longer retry delays let it recover.

async function withClient<T>(
  label: string,
  fn: (client: pg.Client) => Promise<T>,
): Promise<T> {
  for (let attempt = 0; attempt <= MAX_RETRIES; attempt++) {
    const client = new pg.Client({ connectionString: DB_URL });
    // Suppress unhandled error events — fresh client per operation means
    // this handler is registered exactly once per client object.
    client.on('error', () => {});
    try {
      await client.connect();
      const result = await fn(client);
      await client.end().catch(() => {});
      return result;
    } catch (err: unknown) {
      await client.end().catch(() => {});
      const msg = err instanceof Error ? err.message : String(err);
      if (attempt >= MAX_RETRIES) {
        console.error(`[parcels] ${label} failed after ${MAX_RETRIES + 1} attempts: ${msg}`);
        throw err;
      }
      // Exponential backoff: 10s, 20s, 30s … capped at 60s
      // Gives Railway's proxy time to fully recover DNS + TCP stack.
      const wait = Math.min(10_000 * (attempt + 1), 60_000);
      console.error(`[parcels] ${label} attempt ${attempt + 1} failed (${msg}), retrying in ${wait / 1000}s...`);
      await sleep(wait);
    }
  }
  throw new Error('unreachable');
}

// ─── Load one file ─────────────────────────────────────────────────────────────

async function insertBatch(batch: {
  ids: string[]; addrs: string[]; cities: string[];
  states: string[]; zips: string[]; lngs: number[]; lats: number[];
}): Promise<void> {
  const { ids, addrs, cities, states, zips, lngs, lats } = batch;
  await withClient('insertBatch', (client) => client.query(`
    INSERT INTO parcels_staging
      (parcel_id, address, city, state, zip, geom, source)
    SELECT
      unnest($1::text[]), unnest($2::text[]), unnest($3::text[]),
      unnest($4::text[]), unnest($5::text[]),
      ST_SetSRID(ST_MakePoint(unnest($6::float8[]), unnest($7::float8[])), 4326)::geography,
      'overture'
    ON CONFLICT DO NOTHING
  `, [ids, addrs, cities, states, zips, lngs, lats]));
  batch.ids = []; batch.addrs = []; batch.cities = [];
  batch.states = []; batch.zips = []; batch.lngs = []; batch.lats = [];
}

async function loadFile(filePath: string): Promise<number> {
  const label = path.basename(filePath);
  console.log(`\n[parcels] Loading ${label}...`);

  let totalWritten = 0;
  let skipped = 0;

  const batch = {
    ids: [] as string[], addrs: [] as string[], cities: [] as string[],
    states: [] as string[], zips: [] as string[], lngs: [] as number[], lats: [] as number[],
  };

  const rl = readline.createInterface({
    input: fs.createReadStream(filePath),
    crlfDelay: Infinity,
  });

  for await (const line of rl) {
    const trimmed = line.trim();
    const json = trimmed.startsWith('\x1e') ? trimmed.slice(1) : trimmed;
    if (!json) continue;

    let feature: GeoJSONFeature;
    try { feature = JSON.parse(json) as GeoJSONFeature; }
    catch { skipped++; continue; }

    const { geometry, properties: p, id: featureId } = feature;
    if (geometry?.type !== 'Point' || !geometry.coordinates) { skipped++; continue; }

    const [lng, lat] = geometry.coordinates;
    if (typeof lng !== 'number' || typeof lat !== 'number') { skipped++; continue; }

    const parcelId = p.id ?? featureId ?? `ov_${lng.toFixed(6)}_${lat.toFixed(6)}`;
    const address  = buildAddress(p.number, p.street);
    const city     = parseCity(p.address_levels, p.postal_city);
    const state    = parseState(p.address_levels);
    const zip      = p.postcode ?? '';

    if (!address || !state) { skipped++; continue; }

    batch.ids.push(parcelId);
    batch.addrs.push(address);
    batch.cities.push(city);
    batch.states.push(state);
    batch.zips.push(zip);
    batch.lngs.push(lng);
    batch.lats.push(lat);

    if (batch.ids.length >= BATCH_SIZE) {
      await insertBatch(batch);
      totalWritten += BATCH_SIZE;
      if (totalWritten % 100_000 === 0) {
        process.stdout.write(`  ${totalWritten.toLocaleString()} rows loaded...\r`);
      }
    }
  }

  // Flush remainder
  if (batch.ids.length > 0) {
    const rem = batch.ids.length;
    await insertBatch(batch);
    totalWritten += rem;
  }

  console.log(
    `[parcels] ${label} complete — ${totalWritten.toLocaleString()} loaded, ${skipped} skipped`
  );
  return totalWritten;
}

// ─── Staging → parcels upsert ─────────────────────────────────────────────────

async function upsertFromStaging(): Promise<void> {
  console.log('\n[parcels] Upserting parcels_staging → parcels (batched)...');

  await withClient('add _rid column', (c) =>
    c.query('ALTER TABLE parcels_staging ADD COLUMN IF NOT EXISTS _rid SERIAL')
  );

  const countRes = await withClient('count staging rows', (c) =>
    c.query('SELECT MAX(_rid) AS max FROM parcels_staging')
  );
  const maxRid = (countRes.rows[0] as Record<string, unknown>).max as number;
  const BATCH  = 50_000;
  let upserted = 0;

  for (let lo = 1; lo <= maxRid; lo += BATCH) {
    const hi = lo + BATCH - 1;
    const r = await withClient(`upsert ${lo}-${hi}`, (c) => c.query(`
      INSERT INTO parcels (parcel_id, address, city, state, zip, geom, source)
      SELECT parcel_id, address, city, state, zip, geom, source
      FROM   parcels_staging
      WHERE  _rid BETWEEN $1 AND $2
      ON CONFLICT (parcel_id) DO UPDATE SET
        address    = EXCLUDED.address,
        city       = EXCLUDED.city,
        state      = EXCLUDED.state,
        zip        = EXCLUDED.zip,
        geom       = EXCLUDED.geom,
        source     = EXCLUDED.source,
        updated_at = NOW()
    `, [lo, hi]));
    upserted += r.rowCount ?? 0;
    process.stdout.write(`  ${upserted.toLocaleString()} rows upserted...\r`);
  }

  await withClient('drop _rid column', (c) =>
    c.query('ALTER TABLE parcels_staging DROP COLUMN IF EXISTS _rid')
  );
  console.log(`\n[parcels] Upserted ${upserted.toLocaleString()} rows into parcels.`);
}

// ─── Main ─────────────────────────────────────────────────────────────────────

const argFilter = process.argv[2]?.toLowerCase().split(',').map(s => s.trim()).filter(Boolean);

let files = fs.readdirSync(DATA_DIR)
  .filter(f => f.endsWith('.geojsonseq'))
  .map(f => path.join(DATA_DIR, f));

if (argFilter?.length) {
  files = files.filter(f => argFilter.some(code => path.basename(f).startsWith(code)));
}

if (files.length === 0) {
  console.error(`[parcels] No *.geojsonseq files found in ${DATA_DIR}`);
  console.error('[parcels] Run npm run download-parcels first.');
  process.exit(1);
}

console.log(`[parcels] Found ${files.length} file(s) to load:`);
files.forEach(f => console.log(`  ${path.basename(f)}`));

await withClient('truncate staging', (c) => c.query('TRUNCATE parcels_staging'));
console.log('[parcels] parcels_staging truncated.\n');

let totalRows = 0;
for (const file of files) {
  totalRows += await loadFile(file);
}

await upsertFromStaging();

console.log(`\n[parcels] Done. ${totalRows.toLocaleString()} total address rows loaded into parcels.`);
console.log('[parcels] Spatial corridor queries are now enabled.');
console.log('[parcels] NOTE: owner_name_raw is NULL — supplement with ATTOM or county GIS for owner resolution.');
