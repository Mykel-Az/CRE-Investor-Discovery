// scripts/recompute-portfolio.ts
// One-shot: rebuild portfolio_edges from exact + fuzzy owner name matches.
// Usage: node --env-file=.env --import tsx scripts/recompute-portfolio.ts

import pg from 'pg';

const DB_URL = process.env.DATABASE_URL ?? 'postgresql://localhost:5432/cre_investor';

const pool = new pg.Pool({
  connectionString: DB_URL,
  max: 2,
  idleTimeoutMillis: 30_000,
  connectionTimeoutMillis: 15_000,
});

console.log('[portfolio] Recomputing portfolio edges...');

// Exact match
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
} catch (e) { c1.release(true); console.error('[portfolio] Exact match error:', (e as Error).message); }

// Fuzzy match — DISTINCT ON prevents duplicate (entity_id, parcel_id) pairs
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
} catch (e) { c2.release(true); console.error('[portfolio] Fuzzy match error:', (e as Error).message); }

const total = await pool.query('SELECT COUNT(*)::int AS n FROM portfolio_edges');

console.log(`[portfolio] Done.`);
console.log(`  Exact edges  : ${exactCount.toLocaleString()}`);
console.log(`  Fuzzy edges  : ${fuzzyCount.toLocaleString()}`);
console.log(`  Total edges  : ${(total.rows[0] as Record<string, number>).n.toLocaleString()}`);

await pool.end();
process.exit(0);
