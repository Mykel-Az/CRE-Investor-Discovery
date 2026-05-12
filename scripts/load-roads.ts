process.env.NODE_TLS_REJECT_UNAUTHORIZED = '0';

import * as fs from 'fs';
import * as path from 'path';
import * as readline from 'readline';
import pg from 'pg';

const DB_URL = process.env.DATABASE_URL ?? 'postgresql://localhost:5432/cre_investor';
const ROADS_FILE = path.resolve('./data/roads/nj_roads.geojsonseq');
const BATCH_SIZE = 200;

const pool = new pg.Pool({ connectionString: DB_URL, max: 5 });

interface RoadFeature {
  type: string;
  geometry: { type: string; coordinates: unknown[] };
  properties: {
    id?: string;
    names?: { primary?: string };
    class?: string;
    subtype?: string;
  };
}

async function loadRoads(): Promise<void> {
  console.log('[roads] Starting road load...');

  const client = await pool.connect();

  try {
    const rl = readline.createInterface({
      input: fs.createReadStream(ROADS_FILE),
      crlfDelay: Infinity,
    });

    let batch = {
      ids: [] as string[],
      names: [] as string[],
      states: [] as string[],
      geoms: [] as string[],
    };

    let loaded = 0;
    let skipped = 0;

    for await (const line of rl) {
      const trimmed = line.trim().replace(/^\x1e/, '');
      if (!trimmed) continue;

      let feature: RoadFeature;
      try {
        feature = JSON.parse(trimmed);
      } catch {
        skipped++;
        continue;
      }

      if (feature.geometry?.type !== 'LineString') { skipped++; continue; }

      const id = feature.properties?.id ?? `road_${loaded}`;
      const name = feature.properties?.names?.primary ?? '';
      if (!name) { skipped++; continue; }

      batch.ids.push(id);
      batch.names.push(name);
      batch.states.push('NJ');
      batch.geoms.push(JSON.stringify(feature.geometry));

      if (batch.ids.length >= BATCH_SIZE) {
        await client.query(`
          INSERT INTO roads (road_name, state, geom, source)
          SELECT
            unnest($1::text[]),
            unnest($2::text[]),
            ST_SetSRID(ST_GeomFromGeoJSON(unnest($3::text[])), 4326)::geography,
            'overture'
          ON CONFLICT DO NOTHING
        `, [batch.names, batch.states, batch.geoms]);

        loaded += batch.ids.length;
        batch = { ids: [], names: [], states: [], geoms: [] };

        if (loaded % 10000 === 0) {
          process.stdout.write(`  ${loaded.toLocaleString()} roads loaded...\r`);
        }
      }
    }

    // Flush remainder
    if (batch.ids.length > 0) {
      await client.query(`
        INSERT INTO roads (road_name, state, geom, source)
        SELECT
          unnest($1::text[]),
          unnest($2::text[]),
          ST_SetSRID(ST_GeomFromGeoJSON(unnest($3::text[])), 4326)::geography,
          'overture'
        ON CONFLICT DO NOTHING
      `, [batch.names, batch.states, batch.geoms]);
      loaded += batch.ids.length;
    }

    console.log(`\n[roads] Done. ${loaded.toLocaleString()} roads loaded, ${skipped} skipped.`);

  } finally {
    client.release();
    await pool.end();
  }
}

loadRoads().catch(err => {
  console.error('[roads] Failed:', err.message);
  process.exit(1);
});