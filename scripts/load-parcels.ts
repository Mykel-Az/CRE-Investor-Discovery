// scripts/load-parcels.ts
//
// Streams *.geojsonseq files from ./data/openaddresses/ into parcels_staging,
// then upserts staging → parcels (same logic as the weekly cron job).
//
// Usage:
//   npm run load-parcels              # loads all *.geojsonseq files
//   npm run load-parcels -- nj,ny    # loads only nj_addresses.geojsonseq and ny_addresses.geojsonseq
//
// Each GeoJSONSeq line is one Overture Maps address feature:
//   { geometry: { type: "Point", coordinates: [lng, lat] },
//     properties: { id, number, street, postcode, postal_city,
//                   address_levels: [{value, short_code}, ...] } }
//
// NOTE: Overture addresses do NOT include owner_name_raw, lot_size_acres,
// property_type, or sale data. Corridor spatial queries will work after this
// load, but owner entity resolution requires supplementing with ATTOM or
// county assessor data.

import * as fs from 'fs';
import * as path from 'path';
import * as readline from 'readline';
import pg from 'pg';

const DATA_DIR   = path.resolve(process.env.OPENADDRESSES_DATA_DIR ?? './data/openaddresses');
const DB_URL     = process.env.DATABASE_URL ?? 'postgresql://localhost:5432/cre_investor';
const BATCH_SIZE = 500; // rows per INSERT — keeps params well under pg's 32767 limit

const pool = new pg.Pool({ connectionString: DB_URL, max: 5 });

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
  // address_levels[0] is most general (state). short_code is "US-NJ" → "NJ"
  const top = levels?.[0];
  if (top?.short_code) return top.short_code.replace(/^US-/, '');
  if (top?.value)      return top.value.slice(0, 2).toUpperCase();
  return '';
}

function parseCity(levels: AddressProperties['address_levels'], postalCity?: string): string {
  // Last address_level is most specific (city/locality)
  const last = levels?.[levels.length - 1];
  return last?.value ?? postalCity ?? '';
}

function buildAddress(num?: string, street?: string): string {
  return [num, street].filter(Boolean).join(' ');
}

// ─── Bulk insert via UNNEST ───────────────────────────────────────────────────

async function flushBatch(
  client: pg.PoolClient,
  batch: {
    ids:   string[];
    addrs: string[];
    cities:string[];
    states:string[];
    zips:  string[];
    lngs:  number[];
    lats:  number[];
  }
): Promise<number> {
  if (batch.ids.length === 0) return 0;

  await client.query(`
    INSERT INTO parcels_staging
      (parcel_id, address, city, state, zip, geom, source)
    SELECT
      unnest($1::text[]),
      unnest($2::text[]),
      unnest($3::text[]),
      unnest($4::text[]),
      unnest($5::text[]),
      ST_SetSRID(
        ST_MakePoint(unnest($6::float8[]), unnest($7::float8[])),
        4326
      )::geography,
      'overture'
    ON CONFLICT DO NOTHING
  `, [batch.ids, batch.addrs, batch.cities, batch.states, batch.zips, batch.lngs, batch.lats]);

  const count = batch.ids.length;
  batch.ids   = []; batch.addrs  = []; batch.cities = [];
  batch.states= []; batch.zips   = []; batch.lngs   = []; batch.lats = [];
  return count;
}

// ─── Load one file ─────────────────────────────────────────────────────────────

async function loadFile(filePath: string): Promise<number> {
  const label = path.basename(filePath);
  console.log(`\n[parcels] Loading ${label}...`);

  const client = await pool.connect();
  let totalWritten = 0;
  let skipped = 0;

  const batch = {
    ids: [] as string[], addrs:  [] as string[], cities: [] as string[],
    states: [] as string[], zips: [] as string[], lngs: [] as number[], lats: [] as number[],
  };

  try {
    const rl = readline.createInterface({
      input: fs.createReadStream(filePath),
      crlfDelay: Infinity,
    });

    for await (const line of rl) {
      const trimmed = line.trim();
      // GeoJSONSeq uses RS (0x1E) prefix per RFC 8142; strip it if present
      const json = trimmed.startsWith('\x1e') ? trimmed.slice(1) : trimmed;
      if (!json) continue;

      let feature: GeoJSONFeature;
      try {
        feature = JSON.parse(json) as GeoJSONFeature;
      } catch {
        skipped++;
        continue;
      }

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
        totalWritten += await flushBatch(client, batch);
        if (totalWritten % 100_000 === 0) {
          process.stdout.write(`  ${totalWritten.toLocaleString()} rows loaded...\r`);
        }
      }
    }

    // Flush remainder
    totalWritten += await flushBatch(client, batch);

  } finally {
    client.release();
  }

  console.log(
    `[parcels] ${label} complete — ${totalWritten.toLocaleString()} loaded, ${skipped} skipped`
  );
  return totalWritten;
}

// ─── Staging → parcels upsert ─────────────────────────────────────────────────

async function upsertFromStaging(): Promise<void> {
  console.log('\n[parcels] Upserting parcels_staging → parcels...');
  const client = await pool.connect();
  try {
    const result = await client.query(`
      INSERT INTO parcels
        (parcel_id, address, city, state, zip, geom, source)
      SELECT DISTINCT ON (parcel_id) parcel_id, address, city, state, zip, geom, source
      FROM   parcels_staging
      ORDER BY parcel_id
      ON CONFLICT (parcel_id) DO UPDATE SET
        address    = EXCLUDED.address,
        city       = EXCLUDED.city,
        state      = EXCLUDED.state,
        zip        = EXCLUDED.zip,
        geom       = EXCLUDED.geom,
        source     = EXCLUDED.source,
        updated_at = NOW()
    `);
    console.log(`[parcels] Upserted ${result.rowCount?.toLocaleString() ?? 0} rows into parcels.`);
  } finally {
    client.release();
  }
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

// Clear staging before each run to avoid duplicate rows accumulating
const truncClient = await pool.connect();
try {
  await truncClient.query('TRUNCATE parcels_staging');
  console.log('[parcels] parcels_staging truncated.\n');
} finally {
  truncClient.release();
}

let totalRows = 0;
for (const file of files) {
  totalRows += await loadFile(file);
}

await upsertFromStaging();

console.log(`\n[parcels] Done. ${totalRows.toLocaleString()} total address rows loaded into parcels.`);
console.log('[parcels] Spatial corridor queries are now enabled.');
console.log('[parcels] NOTE: owner_name_raw is NULL — supplement with ATTOM or county GIS for owner resolution.');

await pool.end();
