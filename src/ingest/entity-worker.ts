// src/ingest/entity-worker.ts
import { workerData, parentPort } from 'worker_threads';
import * as fuzzball from 'fuzzball';
import { query } from '../db/client.js';

// Run the fuzzy match in a worker thread so it never blocks the main event loop
async function runFuzzyMatch() {
  const unmatchedResult = await query(`
    SELECT DISTINCT owner_name_raw
    FROM parcels p
    WHERE owner_name_raw IS NOT NULL
      AND NOT EXISTS (
        SELECT 1 FROM entity_aliases ea
        WHERE ea.alias_name = p.owner_name_raw
      )
    LIMIT 1000
  `);

  const entitiesResult = await query('SELECT entity_id, canonical_name FROM entities');
  const entityNames = entitiesResult.rows.map((r: any) => ({
    id: r.entity_id as string,
    name: r.canonical_name as string,
  }));

  let fuzzyMatches = 0;
  for (const row of unmatchedResult.rows) {
    const rawName = (row as any).owner_name_raw as string;
    let bestScore = 0;
    let bestEntityId = '';

    for (const entity of entityNames) {
      const score = fuzzball.token_set_ratio(rawName, entity.name);
      if (score > bestScore) {
        bestScore = score;
        bestEntityId = entity.id;
      }
    }

    if (bestScore >= 90 && bestEntityId) {
      await query(`
        INSERT INTO entity_aliases (entity_id, alias_name, match_score, source)
        VALUES ($1, $2, $3, 'fuzzball')
        ON CONFLICT DO NOTHING
      `, [bestEntityId, rawName, bestScore / 100]);
      fuzzyMatches++;
    }
  }

  parentPort?.postMessage({ done: true, fuzzyMatches });
}

runFuzzyMatch().catch(err => {
  parentPort?.postMessage({ error: err.message });
});