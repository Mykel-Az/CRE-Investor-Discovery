// src/ingest/jobs.ts
//
// All data freshness is maintained by these background jobs.
// NOTHING here is called at query time — these jobs write to the
// database / cache and the tool handlers read from it.

import cron from 'node-cron';
import * as fuzzball from 'fuzzball';
import * as fsPromises from 'fs/promises';
import * as fsSync from 'fs';
import * as readline from 'readline';
import * as path from 'path';
import { query } from '../db/client.js';
import { setCache } from '../cache/helpers.js';

// ─── Parcel data refresh ──────────────────────────────────────────────────
// OpenAddresses bulk downloads refresh monthly; county GIS varies.
// We re-ingest the delta diff weekly (Sunday 2 AM).

cron.schedule('0 2 * * 0', async () => {
  console.log('[ingest/parcels] Weekly parcel delta refresh starting...');
  try {
    // Pull updated records from the OpenAddresses changeset endpoint.
    // In production, this downloads a CSV/GeoJSON delta from:
    //   https://batch.openaddresses.io/api/jobs
    // and upserts into the parcels table.

    const dataDir = process.env.OPENADDRESSES_DATA_DIR ?? './data/openaddresses';

    // Count current parcels for logging
    const before = await query('SELECT COUNT(*)::int AS n FROM parcels');
    const beforeCount = (before.rows[0] as Record<string, unknown>).n as number;

    // Upsert pattern: read downloaded GeoJSON files and bulk insert.
    // The actual file-reading + parsing is environment-specific.
    // This query re-stamps updated_at on parcels touched by external ETL scripts
    // that load data into a staging table.
    await query(`
      INSERT INTO parcels (parcel_id, address, city, state, zip, lot_size_acres,
                           building_sqft, property_type, last_sale_price,
                           last_sale_date, loan_maturity, owner_name_raw, geom, source)
      SELECT parcel_id, address, city, state, zip, lot_size_acres,
             building_sqft, property_type, last_sale_price,
             last_sale_date, loan_maturity, owner_name_raw, geom, source
      FROM parcels_staging
      ON CONFLICT (parcel_id) DO UPDATE SET
        address         = EXCLUDED.address,
        city            = EXCLUDED.city,
        state           = EXCLUDED.state,
        zip             = EXCLUDED.zip,
        lot_size_acres  = EXCLUDED.lot_size_acres,
        building_sqft   = EXCLUDED.building_sqft,
        property_type   = EXCLUDED.property_type,
        last_sale_price = EXCLUDED.last_sale_price,
        last_sale_date  = EXCLUDED.last_sale_date,
        loan_maturity   = EXCLUDED.loan_maturity,
        owner_name_raw  = EXCLUDED.owner_name_raw,
        geom            = EXCLUDED.geom,
        source          = EXCLUDED.source,
        updated_at      = NOW()
    `);

    const after = await query('SELECT COUNT(*)::int AS n FROM parcels');
    const afterCount = (after.rows[0] as Record<string, unknown>).n as number;

    console.log(`[ingest/parcels] Parcel refresh complete. ${beforeCount} → ${afterCount} parcels.`);
  } catch (err) {
    console.error('[ingest/parcels] Parcel refresh failed:', err);
  }
});

// ─── Entity resolution refresh ────────────────────────────────────────────
// Two free sources: SOS bulk CSV files (primary) + GLEIF REST API (supplemental).
// SOS covers LLCs/trusts; GLEIF fills in larger registered corporations.
//
// SOS CSV format (one file per state: {state}_entities.csv):
//   filing_number,name,entity_type,status,incorporated_date,
//   registered_agent_name,registered_agent_address,filing_url

cron.schedule('0 3 * * *', async () => {
  console.log('[ingest/entities] Daily entity resolution refresh starting...');
  try {
    let sosUpserted = 0;
    let gleifUpserted = 0;

    // ── Step 1: SOS bulk CSV ingest ─────────────────────────────────────────
    const sosDir = process.env.SOS_DATA_DIR ?? './data/sos';

    try {
      const files = await fsPromises.readdir(sosDir);
      const csvFiles = files.filter(f => f.endsWith('_entities.csv'));

      for (const file of csvFiles) {
        const state = file.replace('_entities.csv', '').toUpperCase();
        const filePath = path.join(sosDir, file);
        const fileStream = fsSync.createReadStream(filePath);
        const rl = readline.createInterface({ input: fileStream, crlfDelay: Infinity });

        let headers: string[] = [];
        let isFirst = true;

        for await (const line of rl) {
          if (isFirst) {
            headers = line.split(',').map(h => h.trim().toLowerCase());
            isFirst = false;
            continue;
          }
          if (!line.trim()) continue;

          const cols = parseCSVLine(line);
          const row = Object.fromEntries(headers.map((h, i) => [h, cols[i] ?? '']));

          if (!row['filing_number'] || !row['name']) continue;

          const entityId   = `sos_${state.toLowerCase()}_${row['filing_number']}`;
          const entityType = mapSosEntityType(row['entity_type'] ?? '');
          const statusMap: Record<string, string> = {
            'active': 'active', 'good standing': 'active',
            'dissolved': 'dissolved', 'cancelled': 'dissolved',
            'suspended': 'suspended', 'revoked': 'suspended',
          };
          const status = statusMap[(row['status'] ?? '').toLowerCase()] ?? 'unknown';

          await query(`
            INSERT INTO entities (entity_id, canonical_name, entity_type, jurisdiction,
                                  status, incorporated_at, registered_agent_name,
                                  registered_agent_address, sos_filing_url, source, updated_at)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, 'sos', NOW())
            ON CONFLICT (entity_id) DO UPDATE SET
              canonical_name           = EXCLUDED.canonical_name,
              entity_type              = EXCLUDED.entity_type,
              status                   = EXCLUDED.status,
              registered_agent_name    = EXCLUDED.registered_agent_name,
              registered_agent_address = EXCLUDED.registered_agent_address,
              sos_filing_url           = EXCLUDED.sos_filing_url,
              updated_at               = NOW()
          `, [
            entityId,
            row['name'].trim(),
            entityType,
            `US-${state}`,
            status,
            row['incorporated_date'] || null,
            row['registered_agent_name'] || null,
            row['registered_agent_address'] || null,
            row['filing_url'] || null,
          ]);

          sosUpserted++;
        }
      }
    } catch (err: unknown) {
      if ((err as NodeJS.ErrnoException).code !== 'ENOENT') throw err;
      console.warn(`[ingest/entities] SOS_DATA_DIR not found at ${sosDir} — skipping SOS ingest.`);
    }

    // ── Step 2: GLEIF API enrichment for unmatched owner names ──────────────
    // Queries owner names with no existing entity match (max 200/run).
    // GLEIF REST API is free, no key required.
    const unmatchedForGleif = await query(`
      SELECT DISTINCT p.owner_name_raw
      FROM parcels p
      WHERE p.owner_name_raw IS NOT NULL
        AND NOT EXISTS (SELECT 1 FROM entity_aliases ea WHERE ea.alias_name = p.owner_name_raw)
        AND NOT EXISTS (SELECT 1 FROM entities e WHERE UPPER(TRIM(e.canonical_name)) = UPPER(TRIM(p.owner_name_raw)))
      LIMIT 200
    `);

    for (const row of unmatchedForGleif.rows) {
      const ownerName = (row as Record<string, unknown>).owner_name_raw as string;

      try {
        const gleifUrl = `https://api.gleif.org/api/v1/lei-records?filter[fullname]=${encodeURIComponent(ownerName)}&page[size]=1`;
        const gleifResponse = await fetch(gleifUrl);
        if (!gleifResponse.ok) continue;

        const gleifData = await gleifResponse.json() as {
          data?: Array<{
            attributes: {
              lei: string;
              entity: {
                legalName: { name: string };
                legalJurisdiction: string;
                status: string;
              };
            };
          }>;
        };

        const record = gleifData.data?.[0];
        if (!record) continue;

        const { lei, entity } = record.attributes;
        const gleifStatus = entity.status === 'ACTIVE' ? 'active' : 'unknown';

        await query(`
          INSERT INTO entities (entity_id, canonical_name, entity_type, jurisdiction,
                                status, lei, source, updated_at)
          VALUES ($1, $2, 'Corporation', $3, $4, $5, 'gleif', NOW())
          ON CONFLICT (entity_id) DO UPDATE SET
            canonical_name = EXCLUDED.canonical_name,
            status         = EXCLUDED.status,
            updated_at     = NOW()
        `, [`gleif_${lei}`, entity.legalName.name, entity.legalJurisdiction ?? 'US', gleifStatus, lei]);

        gleifUpserted++;
      } catch {
        // Skip individual GLEIF failures silently
      }

      // Respect GLEIF free tier: ~200ms between calls
      await new Promise(resolve => setTimeout(resolve, 200));
    }

    // ── Step 3: RapidFuzz matching on still-unmatched owner names ───────────
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
    const entityNames = entitiesResult.rows.map((r: Record<string, unknown>) => ({
      id:   r.entity_id as string,
      name: r.canonical_name as string,
    }));

    let fuzzyMatches = 0;

    for (const row of unmatchedResult.rows) {
      const rawName = (row as Record<string, unknown>).owner_name_raw as string;

      let bestScore = 0;
      let bestEntityId = '';

      for (const entity of entityNames) {
        const score = fuzzball.token_set_ratio(rawName, entity.name);
        if (score > bestScore) {
          bestScore    = score;
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

    // Recompute nested LLC→Trust→Individual graph traversal
    await query(`
      WITH RECURSIVE ownership_chain AS (
        SELECT entity_id, owner_name, role, parent_entity_id, 1 AS depth
        FROM beneficial_owners
        WHERE parent_entity_id IS NOT NULL

        UNION ALL

        SELECT bo.entity_id, bo.owner_name, bo.role, bo.parent_entity_id, oc.depth + 1
        FROM beneficial_owners bo
        JOIN ownership_chain oc ON bo.entity_id = oc.parent_entity_id
        WHERE oc.depth < 5
      )
      SELECT DISTINCT entity_id, owner_name, role
      FROM ownership_chain
    `);

    console.log(
      `[ingest/entities] Entity refresh complete.`,
      `sos=${sosUpserted}`,
      `gleif=${gleifUpserted}`,
      `fuzzy=${fuzzyMatches}`
    );
  } catch (err) {
    console.error('[ingest/entities] Entity refresh failed:', err);
  }
});

// ─── SOS CSV helpers ──────────────────────────────────────────────────────

function parseCSVLine(line: string): string[] {
  const result: string[] = [];
  let current = '';
  let inQuotes = false;
  for (let i = 0; i < line.length; i++) {
    const ch = line[i];
    if (ch === '"') {
      inQuotes = !inQuotes;
    } else if (ch === ',' && !inQuotes) {
      result.push(current.trim());
      current = '';
    } else {
      current += ch;
    }
  }
  result.push(current.trim());
  return result;
}

function mapSosEntityType(raw: string): string {
  const t = raw.toLowerCase();
  if (t.includes('llc') || t.includes('limited liability')) return 'LLC';
  if (t.includes('trust'))                                   return 'Trust';
  if (t.includes('corp') || t.includes('inc'))               return 'Corporation';
  if (t.includes('individual') || t.includes('sole prop'))   return 'Individual';
  return 'Unknown';
}

// ─── Portfolio graph recompute ────────────────────────────────────────────
// Edge table: owner_entity_id → parcel_ids. Recompute daily after entity refresh.

cron.schedule('0 4 * * *', async () => {
  console.log('[ingest/portfolio] Daily portfolio graph recompute starting...');
  try {
    // Step 1: Exact name matches — parcels.owner_name_raw = entities.canonical_name
    const exactResult = await query(`
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

    // Step 2: Fuzzy matches via entity_aliases
    const fuzzyResult = await query(`
      INSERT INTO portfolio_edges (entity_id, parcel_id, confidence, method)
      SELECT ea.entity_id, p.parcel_id, ea.match_score, 'fuzzy'
      FROM parcels p
      JOIN entity_aliases ea ON UPPER(TRIM(p.owner_name_raw)) = UPPER(TRIM(ea.alias_name))
      WHERE p.owner_name_raw IS NOT NULL
      ON CONFLICT (entity_id, parcel_id) DO UPDATE SET
        confidence = GREATEST(portfolio_edges.confidence, EXCLUDED.confidence),
        method     = CASE WHEN EXCLUDED.confidence > portfolio_edges.confidence
                         THEN EXCLUDED.method ELSE portfolio_edges.method END
    `);

    // Step 3: Graph-based — link parcels through beneficial_owners chain
    const graphResult = await query(`
      INSERT INTO portfolio_edges (entity_id, parcel_id, confidence, method)
      SELECT bo.parent_entity_id, pe.parcel_id, pe.confidence * 0.9, 'graph'
      FROM portfolio_edges pe
      JOIN beneficial_owners bo ON bo.entity_id = pe.entity_id
      WHERE bo.parent_entity_id IS NOT NULL
      ON CONFLICT (entity_id, parcel_id) DO UPDATE SET
        confidence = GREATEST(portfolio_edges.confidence, EXCLUDED.confidence),
        method     = CASE WHEN EXCLUDED.confidence > portfolio_edges.confidence
                         THEN EXCLUDED.method ELSE portfolio_edges.method END
    `);

    console.log(
      `[ingest/portfolio] Portfolio graph recompute complete.`,
      `exact=${exactResult.rowCount ?? 0}`,
      `fuzzy=${fuzzyResult.rowCount ?? 0}`,
      `graph=${graphResult.rowCount ?? 0}`
    );
  } catch (err) {
    console.error('[ingest/portfolio] Portfolio graph recompute failed:', err);
  }
});

// ─── Contact enrichment pre-warm ─────────────────────────────────────────
// For top-queried entities, pre-warm the 30-day contact cache.
// Cold misses are handled async by the contact enrichment worker below.

cron.schedule('0 5 * * *', async () => {
  console.log('[ingest/contacts] Contact cache pre-warm starting...');
  try {
    // Find top 500 most-queried entities that need contact refresh
    const topEntities = await query(`
      SELECT ql.entity_id, e.canonical_name
      FROM query_log ql
      JOIN entities e ON e.entity_id = ql.entity_id
      LEFT JOIN contacts c ON c.entity_id = ql.entity_id AND c.expires_at > NOW()
      WHERE ql.entity_id IS NOT NULL
        AND c.entity_id IS NULL
      GROUP BY ql.entity_id, e.canonical_name
      ORDER BY COUNT(*) DESC
      LIMIT 500
    `);

    let enriched = 0;

    for (const row of topEntities.rows) {
      const entityId  = (row as Record<string, unknown>).entity_id as string;
      const ownerName = (row as Record<string, unknown>).canonical_name as string;

      try {
        const contact = await enrichContact(entityId, ownerName);
        if (contact) enriched++;
      } catch (err) {
        console.error(`[ingest/contacts] Pre-warm failed for ${entityId}:`, err);
      }

      // Rate limit: ~100ms between calls
      await new Promise((resolve) => setTimeout(resolve, 100));
    }

    console.log(`[ingest/contacts] Contact pre-warm complete. ${enriched}/${topEntities.rows.length} enriched.`);
  } catch (err) {
    console.error('[ingest/contacts] Contact pre-warm failed:', err);
  }
});

// ─── Contact enrichment core function ────────────────────────────────────
// Calls Proxycurl (primary) → Hunter.io (fallback). Writes to DB + Redis.

async function enrichContact(
  entityId: string,
  ownerName: string
): Promise<Record<string, unknown> | null> {
  const proxycurlKey = process.env.PROXYCURL_API_KEY ?? '';
  const hunterKey    = process.env.HUNTER_API_KEY ?? '';

  let contact: Record<string, unknown> | null = null;

  // ── Proxycurl: Company enrichment ──────────────────────────────────────
  if (proxycurlKey) {
    try {
      // Proxycurl company lookup by name
      // API: GET https://nubela.co/proxycurl/api/linkedin/company/resolve
      const pcUrl = `https://nubela.co/proxycurl/api/linkedin/company/resolve?company_name=${encodeURIComponent(ownerName)}`;
      const pcResponse = await fetch(pcUrl, {
        headers: { Authorization: `Bearer ${proxycurlKey}` },
      });

      if (pcResponse.ok) {
        const pcData = await pcResponse.json() as Record<string, unknown>;
        const companyUrl = pcData.url as string | undefined;

        if (companyUrl) {
          // Get company profile for employee/contact info
          const profileUrl = `https://nubela.co/proxycurl/api/linkedin/company?url=${encodeURIComponent(companyUrl)}`;
          const profileResponse = await fetch(profileUrl, {
            headers: { Authorization: `Bearer ${proxycurlKey}` },
          });

          if (profileResponse.ok) {
            const profile = await profileResponse.json() as Record<string, unknown>;

            // Extract primary contact from company profile
            const name = (profile.name as string) ?? ownerName;
            // Look for a top executive in the company
            const people = (profile.people_also_viewed as Array<Record<string, unknown>>) ?? [];
            const topPerson = people[0];

            contact = {
              name:       topPerson?.name ?? name,
              role:       topPerson?.title ?? 'Principal',
              email:      null, // Proxycurl doesn't directly provide email
              phone:      null,
              confidence: 0.75,
              source:     'proxycurl',
            };
          }
        }
      }
    } catch (err) {
      console.warn(`[ingest/contacts] Proxycurl failed for ${entityId}:`, err);
    }
  }

  // ── Hunter.io fallback: email finder ───────────────────────────────────
  if (hunterKey && contact) {
    try {
      // Try to find an email via Hunter.io domain search
      // First, derive a likely domain from the company name
      const domain = ownerName
        .toLowerCase()
        .replace(/\s*(llc|inc|corp|ltd|trust|holdings|group|partners|capital)\s*/gi, '')
        .replace(/[^a-z0-9]/g, '')
        + '.com';

      const hunterUrl = `https://api.hunter.io/v2/domain-search?domain=${domain}&api_key=${hunterKey}&limit=1`;
      const hunterResponse = await fetch(hunterUrl);

      if (hunterResponse.ok) {
        const hunterData = await hunterResponse.json() as {
          data?: {
            emails?: Array<{
              value: string;
              type: string;
              confidence: number;
              first_name: string;
              last_name: string;
              position: string;
            }>;
          };
        };

        const emails = hunterData.data?.emails ?? [];
        if (emails.length > 0) {
          const topEmail = emails[0];
          contact = {
            ...contact,
            name:       `${topEmail.first_name} ${topEmail.last_name}`.trim() || (contact.name as string),
            role:       topEmail.position || (contact.role as string),
            email:      topEmail.value,
            confidence: Math.max((contact.confidence as number) ?? 0, topEmail.confidence / 100),
            source:     'proxycurl+hunter',
          };
        }
      }
    } catch (err) {
      console.warn(`[ingest/contacts] Hunter.io failed for ${entityId}:`, err);
    }
  }

  // If we still don't have a contact and Hunter is available, try email-finder directly
  if (!contact && hunterKey) {
    try {
      // Extract first/last name if ownerName looks like a person
      const nameParts = ownerName.split(/\s+/);
      if (nameParts.length >= 2) {
        const firstName = nameParts[0];
        const lastName  = nameParts[nameParts.length - 1];

        const domain = ownerName
          .toLowerCase()
          .replace(/\s*(llc|inc|corp|ltd|trust|holdings|group|partners|capital)\s*/gi, '')
          .replace(/[^a-z0-9]/g, '')
          + '.com';

        const finderUrl = `https://api.hunter.io/v2/email-finder?domain=${domain}&first_name=${firstName}&last_name=${lastName}&api_key=${hunterKey}`;
        const finderResponse = await fetch(finderUrl);

        if (finderResponse.ok) {
          const finderData = await finderResponse.json() as {
            data?: {
              email: string;
              score: number;
              first_name: string;
              last_name: string;
              position: string;
            };
          };

          if (finderData.data?.email) {
            contact = {
              name:       `${finderData.data.first_name} ${finderData.data.last_name}`.trim(),
              role:       finderData.data.position || 'Contact',
              email:      finderData.data.email,
              phone:      null,
              confidence: finderData.data.score / 100,
              source:     'hunter',
            };
          }
        }
      }
    } catch (err) {
      console.warn(`[ingest/contacts] Hunter.io email-finder failed for ${entityId}:`, err);
    }
  }

  // ── Persist to DB + Redis ──────────────────────────────────────────────
  if (contact) {
    await query(`
      INSERT INTO contacts (entity_id, name, role, email, phone, confidence, source, enriched_at, expires_at)
      VALUES ($1, $2, $3, $4, $5, $6, $7, NOW(), NOW() + INTERVAL '30 days')
      ON CONFLICT (entity_id) DO UPDATE SET
        name        = EXCLUDED.name,
        role        = EXCLUDED.role,
        email       = EXCLUDED.email,
        phone       = EXCLUDED.phone,
        confidence  = EXCLUDED.confidence,
        source      = EXCLUDED.source,
        enriched_at = NOW(),
        expires_at  = NOW() + INTERVAL '30 days'
    `, [
      entityId,
      contact.name,
      contact.role,
      contact.email,
      contact.phone,
      contact.confidence,
      contact.source,
    ]);

    // Cache in Redis for 30 days (2592000 seconds)
    await setCache(`contact:${entityId}`, contact, 2_592_000);
  }

  return contact;
}

// ─── Async contact enrichment worker (on cache miss) ─────────────────────
// Called by the investor_discovery / owner_profile tools when
// contact_status = 'pending'. Writes to DB + Redis so the next query
// for the same entity is instant.

export async function enqueueContactEnrichment(entityId: string, ownerName: string): Promise<void> {
  // In production: push to a job queue (Bull, BullMQ, etc.)
  // Here: async fire-and-forget with the real enrichContact function
  setImmediate(async () => {
    try {
      console.log(`[ingest/contacts] Async enrichment for entity ${entityId}`);
      await enrichContact(entityId, ownerName);
    } catch (err) {
      console.error(`[ingest/contacts] Enrichment failed for ${entityId}:`, err);
    }
  });
}

export function startBackgroundJobs(): void {
  console.log('[ingest] Background jobs scheduled:');
  console.log('  - Parcel refresh:     Sunday 2 AM');
  console.log('  - Entity resolution:  Daily  3 AM');
  console.log('  - Portfolio graph:    Daily  4 AM');
  console.log('  - Contact pre-warm:  Daily  5 AM');
}
