// scripts/load-roads.ts
//
// Downloads named road geometries from OpenStreetMap via Overpass API
// and loads them into the roads table for corridor spatial queries.
//
// Usage:
//   node --env-file=.env --import tsx scripts/load-roads.ts
//   node --env-file=.env --import tsx scripts/load-roads.ts nj,ny
//
// Free — no API key required. Filters to major named roads to avoid timeouts.

import pg from 'pg';

const DB_URL = process.env.DATABASE_URL ?? 'postgresql://cre:cre@localhost:5432/cre_investor';
const pool   = new pg.Pool({ connectionString: DB_URL, max: 3 });

const STATE_ISO: Record<string, string> = {
  nj: 'US-NJ', ny: 'US-NY', fl: 'US-FL', tx: 'US-TX', ca: 'US-CA',
  il: 'US-IL', pa: 'US-PA', ga: 'US-GA', co: 'US-CO', wa: 'US-WA',
  az: 'US-AZ', ma: 'US-MA', nc: 'US-NC', oh: 'US-OH', mi: 'US-MI',
};

interface OverpassNode { lat: number; lon: number; }
interface OverpassWay {
  type:     'way';
  id:       number;
  tags:     Record<string, string>;
  geometry: OverpassNode[];
}
interface OverpassResponse { elements: Array<{ type: string } & Partial<OverpassWay>> }

async function fetchRoads(state: string, iso: string): Promise<void> {
  console.log(`\n[roads] Fetching ${state.toUpperCase()} from Overpass API...`);

  // Filter to major named roads — avoids timeout and fetches CRE-relevant corridors
  const query = `
    [out:json][timeout:180];
    area["ISO3166-2"="${iso}"]->.s;
    way["highway"~"motorway|trunk|primary|secondary|tertiary"]["name"](area.s);
    out geom;
  `;

  const endpoints = [
    'https://overpass-api.de/api/interpreter',
    'https://overpass.kumi.systems/api/interpreter',
    'https://overpass.openstreetmap.ru/api/interpreter',
  ];

  let res: Response | null = null;
  for (const endpoint of endpoints) {
    const url = `${endpoint}?data=${encodeURIComponent(query)}`;
    try {
      res = await fetch(url, { headers: { Accept: 'application/json' } });
      if (res.ok) break;
      console.warn(`[roads] ${endpoint} returned ${res.status}, trying next...`);
    } catch {
      console.warn(`[roads] ${endpoint} unreachable, trying next...`);
    }
  }

  if (!res || !res.ok) throw new Error(`All Overpass endpoints failed`);

  const data = await res.json() as OverpassResponse;
  const ways = data.elements.filter(
    (e): e is OverpassWay => e.type === 'way' && Array.isArray((e as OverpassWay).geometry) && (e as OverpassWay).geometry.length >= 2
  );

  console.log(`[roads] Got ${ways.length} segments for ${state.toUpperCase()}`);

  const client = await pool.connect();
  let inserted = 0;
  let skipped  = 0;

  try {
    for (const way of ways) {
      const name = way.tags?.name;
      if (!name || way.geometry.length < 2) { skipped++; continue; }

      const coords = way.geometry.map(n => `${n.lon} ${n.lat}`).join(', ');
      const wkt    = `LINESTRING(${coords})`;

      try {
        await client.query(
          `INSERT INTO roads (road_name, state, geom, source) VALUES ($1, $2, ST_GeogFromText($3), 'osm')`,
          [name, state.toUpperCase(), wkt]
        );
        inserted++;
      } catch {
        skipped++;
      }

      if (inserted % 5_000 === 0 && inserted > 0) {
        process.stdout.write(`  ${inserted.toLocaleString()} segments inserted...\r`);
      }
    }
  } finally {
    client.release();
  }

  console.log(`[roads] ${state.toUpperCase()} — ${inserted} segments inserted, ${skipped} skipped`);
}

// ─── Main ─────────────────────────────────────────────────────────────────────

const argStates = (process.argv[2] ?? 'nj,ny').toLowerCase().split(',').map(s => s.trim()).filter(Boolean);

console.log('[roads] OSM road loader starting...');
console.log(`[roads] Target states: ${argStates.join(', ').toUpperCase()}`);
console.log('[roads] Source: OpenStreetMap via Overpass API (free, no key required)\n');

for (let i = 0; i < argStates.length; i++) {
  const state = argStates[i];
  const iso   = STATE_ISO[state];

  if (!iso) {
    console.warn(`[roads] Unknown state code: ${state} — skipping. Valid: ${Object.keys(STATE_ISO).join(', ')}`);
    continue;
  }

  try {
    await fetchRoads(state, iso);
  } catch (err) {
    console.error(`[roads] Failed ${state.toUpperCase()}:`, (err as Error).message);
  }

  if (i < argStates.length - 1) {
    console.log('[roads] Waiting 15s before next state (Overpass rate limit)...');
    await new Promise(r => setTimeout(r, 15_000));
  }
}

console.log('\n[roads] Done. Corridor spatial queries are now enabled.');
await pool.end();
