// scripts/run-entity-resolution.ts
// One-time manual trigger for the entity resolution pipeline.
// Reads SOS CSV files from SOS_DATA_DIR and writes to the entities table.
// Usage: node --env-file=.env --import tsx scripts/run-entity-resolution.ts

import * as fsPromises from 'fs/promises';
import * as fsSync from 'fs';
import * as readline from 'readline';
import * as path from 'path';
import pg from 'pg';
import * as fuzzball from 'fuzzball';

const DB_URL  = process.env.DATABASE_URL ?? 'postgresql://localhost:5432/cre_investor';
const SOS_DIR = process.env.SOS_DATA_DIR ?? './data/sos';

const pool = new pg.Pool({
  connectionString: DB_URL,
  max: 2,
  idleTimeoutMillis: 120_000,
  connectionTimeoutMillis: 15_000,
  keepAlive: true,
  keepAliveInitialDelayMillis: 10_000,
});

function parseCSVLine(line: string): string[] {
  const result: string[] = [];
  let current = '';
  let inQuotes = false;
  for (const ch of line) {
    if (ch === '"')             { inQuotes = !inQuotes; }
    else if (ch === ',' && !inQuotes) { result.push(current.trim()); current = ''; }
    else                        { current += ch; }
  }
  result.push(current.trim());
  return result;
}

function mapEntityType(raw: string): string {
  const t = raw.toLowerCase();
  if (t.includes('llc') || t.includes('limited liability')) return 'LLC';
  if (t.includes('trust'))                                   return 'Trust';
  if (t.includes('corp') || t.includes('inc'))               return 'Corporation';
  if (t.includes('individual') || t.includes('sole prop'))   return 'Individual';
  return 'Unknown';
}

// ─── Step 1: SOS CSV ingest ───────────────────────────────────────────────────

async function ingestSOS(): Promise<number> {
  let total = 0;

  const files = (await fsPromises.readdir(SOS_DIR)).filter(f => f.endsWith('_entities.csv'));
  if (files.length === 0) {
    console.log('[entity-resolution] No *_entities.csv files found in', SOS_DIR);
    return 0;
  }

  for (const file of files) {
    const state    = file.replace('_entities.csv', '').toUpperCase();
    const filePath = path.join(SOS_DIR, file);
    console.log(`[entity-resolution] Ingesting ${file} (${state})...`);

    const rl = readline.createInterface({
      input: fsSync.createReadStream(filePath),
      crlfDelay: Infinity,
    });

    let headers: string[] = [];
    let isFirst = true;
    let count   = 0;
    let batch: unknown[][] = [];

    // Phase 1: read entire file into memory (no DB connection held during disk I/O)
    const rows: unknown[][] = [];
    const statusMap: Record<string, string> = {
      'active': 'active', 'good standing': 'active',
      'dissolved': 'dissolved', 'cancelled': 'dissolved',
      'suspended': 'suspended', 'revoked': 'suspended',
    };

    for await (const line of rl) {
      if (isFirst) {
        headers = line.split(',').map(h => h.trim().toLowerCase());
        isFirst = false;
        continue;
      }
      if (!line.trim()) continue;
      const cols = parseCSVLine(line);
      const row  = Object.fromEntries(headers.map((h, i) => [h, cols[i] ?? '']));
      if (!row['filing_number'] || !row['name']) continue;
      rows.push([
        `sos_${state.toLowerCase()}_${row['filing_number']}`,
        row['name'].trim(),
        mapEntityType(row['entity_type'] ?? ''),
        `US-${state}`,
        statusMap[(row['status'] ?? '').toLowerCase()] ?? 'unknown',
        row['incorporated_date'] || null,
        row['registered_agent_name'] || null,
        row['registered_agent_address'] || null,
        row['filing_url'] || null,
      ]);
    }

    console.log(`[entity-resolution] ${file} — ${rows.length.toLocaleString()} rows read, inserting...`);

    // Phase 2: insert in batches of 1000, refreshing the connection every 100k rows
    const BATCH  = 1000;
    const RENEW  = 100; // renew connection every 100 batches (100k rows)
    let client   = await pool.connect();
    let batchNum = 0;
    try {
      for (let i = 0; i < rows.length; i += BATCH) {
        if (batchNum > 0 && batchNum % RENEW === 0) {
          client.release();
          client = await pool.connect();
        }
        const batch = rows.slice(i, i + BATCH);
        await client.query(`
          INSERT INTO entities
            (entity_id, canonical_name, entity_type, jurisdiction,
             status, incorporated_at, registered_agent_name,
             registered_agent_address, sos_filing_url, source, updated_at)
          SELECT
            unnest($1::text[]), unnest($2::text[]), unnest($3::text[]),
            unnest($4::text[]), unnest($5::text[]),
            NULLIF(unnest($6::text[]), '')::date,
            unnest($7::text[]), unnest($8::text[]), unnest($9::text[]),
            'sos', NOW()
          ON CONFLICT (entity_id) DO UPDATE SET
            canonical_name           = EXCLUDED.canonical_name,
            entity_type              = EXCLUDED.entity_type,
            status                   = EXCLUDED.status,
            registered_agent_name    = EXCLUDED.registered_agent_name,
            registered_agent_address = EXCLUDED.registered_agent_address,
            sos_filing_url           = EXCLUDED.sos_filing_url,
            updated_at               = NOW()
        `, [
          batch.map(r => r[0]), batch.map(r => r[1]), batch.map(r => r[2]),
          batch.map(r => r[3]), batch.map(r => r[4]), batch.map(r => r[5]),
          batch.map(r => r[6]), batch.map(r => r[7]), batch.map(r => r[8]),
        ]);
        count += batch.length;
        batchNum++;
        if (count % 50_000 === 0) process.stdout.write(`  ${count.toLocaleString()} rows inserted...\r`);
      }
    } finally {
      client.release();
    }

    console.log(`[entity-resolution] ${file} — ${count.toLocaleString()} entities upserted`);
    total += count;
  }

  return total;
}

// ─── Step 2: Fuzzy match owner names → entities (server-side via pg_trgm) ────

async function runFuzzyMatch(): Promise<number> {
  console.log('\n[entity-resolution] Running fuzzy match on parcel owner names (pg_trgm)...');

  // Enable pg_trgm and create GIN index for fast similarity search
  {
    const c = await pool.connect();
    c.setMaxListeners(50); c.once('error', () => {});
    try {
      await c.query('CREATE EXTENSION IF NOT EXISTS pg_trgm');
      await c.query(`
        CREATE INDEX IF NOT EXISTS idx_entities_canonical_trgm
        ON entities USING gin(upper(canonical_name) gin_trgm_ops)
      `);
      console.log('[entity-resolution] pg_trgm extension and GIN index ready.');
    } catch (e) {
      console.warn('[entity-resolution] pg_trgm setup warning:', (e as Error).message);
    } finally { c.release(); }
  }

  // Get distinct unmatched owner names (Railway-safe: small result set)
  const unmatchedRes = await pool.query(`
    SELECT DISTINCT owner_name_raw FROM parcels
    WHERE owner_name_raw IS NOT NULL
      AND NOT EXISTS (
        SELECT 1 FROM entity_aliases ea
        WHERE upper(ea.alias_name) = upper(parcels.owner_name_raw)
      )
    LIMIT 5000
  `);

  const names = unmatchedRes.rows.map(r => (r as Record<string, string>).owner_name_raw);
  if (names.length === 0) {
    console.log('[entity-resolution] No unmatched owner names found.');
    return 0;
  }
  console.log(`[entity-resolution] Found ${names.length} unmatched owner names to match.`);

  // Process in batches of 50 names — each batch is one DB query using pg_trgm
  let matched = 0;
  const BATCH = 50;

  for (let i = 0; i < names.length; i += BATCH) {
    const chunk = names.slice(i, i + BATCH);
    // Retry both pool.connect() AND query — wrap both in the try block
    for (let attempt = 0; attempt <= 5; attempt++) {
      let c: pg.PoolClient | null = null;
      try {
        c = await pool.connect();
        c.setMaxListeners(50); c.once('error', () => {});
        const r = await c.query(`
          INSERT INTO entity_aliases (entity_id, alias_name, match_score, source)
          SELECT DISTINCT ON (name) best.entity_id, name, best.sim, 'trgm'
          FROM unnest($1::text[]) AS name
          CROSS JOIN LATERAL (
            SELECT entity_id,
                   similarity(upper(canonical_name), upper(name)) AS sim
            FROM   entities
            WHERE  upper(canonical_name) % upper(name)
            ORDER  BY sim DESC
            LIMIT  1
          ) best
          WHERE best.sim >= 0.70
          ON CONFLICT DO NOTHING
        `, [chunk]);
        matched += r.rowCount ?? 0;
        c.release();
        break; // success — move to next batch
      } catch (err) {
        if (c) c.release(true);
        if (attempt >= 5) {
          console.warn(`[entity-resolution] Batch ${i}-${i + BATCH} failed after retries:`, (err as Error).message);
          break;
        }
        const wait = 5_000 * (attempt + 1);
        console.warn(`[entity-resolution] Batch ${i}-${i + BATCH} retry ${attempt + 1} in ${wait / 1000}s: ${(err as Error).message}`);
        await new Promise(r => setTimeout(r, wait));
      }
    }

    if (i % 500 === 0 && i > 0) {
      process.stdout.write(`  ${i}/${names.length} names processed, ${matched} matches so far\r`);
    }
  }

  console.log(`\n[entity-resolution] Fuzzy match complete — ${matched} aliases created`);
  return matched;
}

// ─── Step 3: Portfolio graph recompute ───────────────────────────────────────

async function recomputePortfolio(): Promise<void> {
  console.log('\n[entity-resolution] Recomputing portfolio edges...');

  const c1 = await pool.connect();
  c1.setMaxListeners(50); c1.once('error', () => {});
  let exactCount = 0;
  try {
    const r = await c1.query(`
      INSERT INTO portfolio_edges (entity_id, parcel_id, confidence, method)
      SELECT e.entity_id, p.parcel_id, 1.0, 'exact'
      FROM parcels p
      JOIN entities e ON UPPER(TRIM(p.owner_name_raw)) = UPPER(TRIM(e.canonical_name))
      WHERE p.owner_name_raw IS NOT NULL
      ON CONFLICT (entity_id, parcel_id) DO UPDATE SET
        confidence = GREATEST(portfolio_edges.confidence, EXCLUDED.confidence),
        method     = CASE WHEN EXCLUDED.confidence > portfolio_edges.confidence
                         THEN EXCLUDED.method ELSE portfolio_edges.method END
    `);
    exactCount = r.rowCount ?? 0;
    c1.release();
  } catch (e) { c1.release(true); console.warn('[entity-resolution] Exact match warning:', (e as Error).message); }

  const c2 = await pool.connect();
  c2.setMaxListeners(50); c2.once('error', () => {});
  let fuzzyCount = 0;
  try {
    const r = await c2.query(`
      INSERT INTO portfolio_edges (entity_id, parcel_id, confidence, method)
      SELECT DISTINCT ON (ea.entity_id, p.parcel_id)
             ea.entity_id, p.parcel_id, ea.match_score, 'fuzzy'
      FROM parcels p
      JOIN entity_aliases ea ON UPPER(TRIM(p.owner_name_raw)) = UPPER(TRIM(ea.alias_name))
      WHERE p.owner_name_raw IS NOT NULL
      ORDER BY ea.entity_id, p.parcel_id, ea.match_score DESC
      ON CONFLICT (entity_id, parcel_id) DO UPDATE SET
        confidence = GREATEST(portfolio_edges.confidence, EXCLUDED.confidence),
        method     = CASE WHEN EXCLUDED.confidence > portfolio_edges.confidence
                         THEN EXCLUDED.method ELSE portfolio_edges.method END
    `);
    fuzzyCount = r.rowCount ?? 0;
    c2.release();
  } catch (e) { c2.release(true); console.warn('[entity-resolution] Fuzzy edge warning:', (e as Error).message); }

  console.log(`[entity-resolution] Portfolio edges — exact: ${exactCount}, fuzzy: ${fuzzyCount}`);
}

// ─── Main ─────────────────────────────────────────────────────────────────────

console.log('[entity-resolution] Starting manual entity resolution run...');
console.log(`[entity-resolution] SOS_DATA_DIR: ${path.resolve(SOS_DIR)}`);
console.log(`[entity-resolution] DATABASE_URL:  ${DB_URL.replace(/:\/\/.*@/, '://***@')}\n`);

// Skip SOS ingest if entities already loaded (avoids 15-min re-ingest on retries)
const entityCountRes = await pool.query('SELECT COUNT(*)::int AS n FROM entities');
const existingEntities = (entityCountRes.rows[0] as Record<string, number>).n;
let sosCount = existingEntities;
if (existingEntities < 100_000) {
  sosCount = await ingestSOS();
} else {
  console.log(`[entity-resolution] Skipping SOS ingest — ${existingEntities.toLocaleString()} entities already in DB.`);
}

const fuzzyCount = await runFuzzyMatch();
await recomputePortfolio();

const final = await pool.query('SELECT COUNT(*)::int AS n FROM entities');
console.log(`\n[entity-resolution] Done.`);
console.log(`  SOS entities upserted : ${sosCount.toLocaleString()}`);
console.log(`  Fuzzy aliases created : ${fuzzyCount}`);
console.log(`  Total entities in DB  : ${(final.rows[0] as Record<string, unknown>).n}`);

await pool.end();
process.exit(0);
