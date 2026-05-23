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
    'https://overpass.private.coffee/api/interpreter',
    'https://overpass.openstreetmap.ru/api/interpreter',
  ];

  let res: Response | null = null;
  for (const endpoint of endpoints) {
    try {
      // Use POST — GET URLs get too long for some proxies (→ 406)
      res = await fetch(endpoint, {
        method:  'POST',
        headers: {
          'Content-Type': 'application/x-www-form-urlencoded',
          'Accept':       'application/json',
          'User-Agent':   'CRE-Investor-Discovery/1.0',
        },
        body: `data=${encodeURIComponent(query)}`,
        signal: AbortSignal.timeout(200_000),
      });
      if (res.ok) break;
      if (res.status === 429) {
        console.warn(`[roads] ${endpoint} rate-limited (429), waiting 30s...`);
        await new Promise(r => setTimeout(r, 30_000));
        // retry same endpoint once after back-off
        res = await fetch(endpoint, {
          method:  'POST',
          headers: { 'Content-Type': 'application/x-www-form-urlencoded', 'Accept': 'application/json', 'User-Agent': 'CRE-Investor-Discovery/1.0' },
          body: `data=${encodeURIComponent(query)}`,
          signal: AbortSignal.timeout(200_000),
        });
        if (res.ok) break;
      }
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

  let inserted = 0;
  let skipped  = 0;
  const ROAD_BATCH = 500;

  for (let i = 0; i < ways.length; i += ROAD_BATCH) {
    const chunk = ways.slice(i, i + ROAD_BATCH);
    const names: string[] = [];
    const states: string[] = [];
    const wkts: string[] = [];

    for (const way of chunk) {
      const name = way.tags?.name;
      if (!name || way.geometry.length < 2) { skipped++; continue; }
      names.push(name);
      states.push(state.toUpperCase());
      wkts.push(`LINESTRING(${way.geometry.map(n => `${n.lon} ${n.lat}`).join(', ')})`);
    }

    if (names.length === 0) continue;

    for (let attempt = 0; attempt <= 5; attempt++) {
      const client = new pg.Client({ connectionString: DB_URL });
      client.on('error', () => {});
      try {
        await client.connect();
        await client.query(
          `INSERT INTO roads (road_name, state, geom, source)
           SELECT unnest($1::text[]), unnest($2::text[]),
                  ST_GeogFromText(unnest($3::text[])), 'osm'
           ON CONFLICT DO NOTHING`,
          [names, states, wkts]
        );
        inserted += names.length;
        await client.end().catch(() => {});
        break;
      } catch (err) {
        await client.end().catch(() => {});
        if (attempt >= 5) { skipped += names.length; break; }
        await new Promise(r => setTimeout(r, 5_000 * (attempt + 1)));
      }
    }

    if (inserted % 5_000 === 0 && inserted > 0) {
      process.stdout.write(`  ${inserted.toLocaleString()} segments inserted...\r`);
    }
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
