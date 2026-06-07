// src/ingest/jobs.ts
//
// All data freshness is maintained by these background jobs.
// NOTHING here is called at query time — these jobs write to the
// database / cache and the tool handlers read from it.

import cron from 'node-cron';
import * as fsPromises from 'fs/promises';
import * as fsSync from 'fs';
import * as readline from 'readline';
import * as path from 'path';
import { spawn } from 'child_process';
import { fileURLToPath } from 'url';
import { query } from '../db/client.js';
import { setCache } from '../cache/helpers.js';

const __dirname = path.dirname(fileURLToPath(import.meta.url));

// ─── Parcel data refresh ──────────────────────────────────────────────────
// Weekly Sunday 2 AM

cron.schedule('0 2 * * 0', async () => {
  console.log('[ingest/parcels] Weekly parcel delta refresh starting...');
  try {
    const before = await query('SELECT COUNT(*)::int AS n FROM parcels');
    const beforeCount = (before.rows[0] as Record<string, unknown>).n as number;

    await query(`
      INSERT INTO parcels (parcel_id, address, city, state, zip, lot_size_acres,
                           building_sqft, property_type, last_sale_price,
                           last_sale_date, loan_maturity, owner_name_raw, geom, source)
      SELECT parcel_id, address, city, state, zip, lot_size_acres,
             building_sqft,
             COALESCE(property_type, 'Unknown'),
             last_sale_price, last_sale_date, loan_maturity, owner_name_raw, geom, source
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
// Runs in a detached child process so it never blocks the main event loop.
// Daily 10 AM.

cron.schedule('0 10 * * *', () => {
  console.log('[ingest/entities] Daily entity resolution starting in background process...');

  const child = spawn('npx', ['tsx', 'scripts/run-entity-resolution.ts'], {
    detached: true,
    stdio: 'ignore',
    env: process.env,
    cwd: process.cwd(),
  });

  child.unref();
  console.log(`[ingest/entities] Entity resolution background PID: ${child.pid}`);
});

// ─── Portfolio graph recompute ────────────────────────────────────────────
// Runs in a detached child process so it never blocks the main event loop.
// Daily 2 PM.

cron.schedule('0 14 * * *', () => {
  console.log('[ingest/portfolio] Daily portfolio graph recompute starting in background...');

  const child = spawn('npx', ['tsx', 'scripts/recompute-portfolio.ts'], {
    detached: true,
    stdio: 'ignore',
    env: process.env,
    cwd: process.cwd(),
  });

  child.unref();
  console.log(`[ingest/portfolio] Portfolio recompute background PID: ${child.pid}`);
});

// ─── Contact enrichment pre-warm ─────────────────────────────────────────
// For top-queried entities, pre-warm the 30-day contact cache.
// Daily 6 PM.

cron.schedule('0 18 * * *', async () => {
  console.log('[ingest/contacts] Contact cache pre-warm starting...');
  try {
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

      await new Promise((resolve) => setTimeout(resolve, 100));
    }

    console.log(`[ingest/contacts] Contact pre-warm complete. ${enriched}/${topEntities.rows.length} enriched.`);
  } catch (err) {
    console.error('[ingest/contacts] Contact pre-warm failed:', err);
  }
});

// ─── Contact enrichment core function ────────────────────────────────────

async function enrichContact(
  entityId: string,
  ownerName: string
): Promise<Record<string, unknown> | null> {
  const proxycurlKey = process.env.PROXYCURL_API_KEY ?? '';
  const hunterKey    = process.env.HUNTER_API_KEY ?? '';

  let contact: Record<string, unknown> | null = null;

  // ── Proxycurl primary ──────────────────────────────────────────────────
  if (proxycurlKey) {
    try {
      const pcUrl = `https://nubela.co/proxycurl/api/linkedin/company/resolve?company_name=${encodeURIComponent(ownerName)}`;
      const pcResponse = await fetch(pcUrl, {
        headers: { Authorization: `Bearer ${proxycurlKey}` },
      });

      if (pcResponse.ok) {
        const pcData = await pcResponse.json() as Record<string, unknown>;
        const companyUrl = pcData.url as string | undefined;

        if (companyUrl) {
          const profileUrl = `https://nubela.co/proxycurl/api/linkedin/company?url=${encodeURIComponent(companyUrl)}`;
          const profileResponse = await fetch(profileUrl, {
            headers: { Authorization: `Bearer ${proxycurlKey}` },
          });

          if (profileResponse.ok) {
            const profile = await profileResponse.json() as Record<string, unknown>;
            const name = (profile.name as string) ?? ownerName;
            const people = (profile.people_also_viewed as Array<Record<string, unknown>>) ?? [];
            const topPerson = people[0];

            contact = {
              name:       topPerson?.name ?? name,
              role:       topPerson?.title ?? 'Principal',
              email:      null,
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

  // ── Hunter.io domain search (when we have a contact from Proxycurl) ────
  if (hunterKey && contact) {
    try {
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
      console.warn(`[ingest/contacts] Hunter.io domain search failed for ${entityId}:`, err);
    }
  }

  // ── Hunter.io standalone (no Proxycurl key or Proxycurl found nothing) ─
  if (!contact && hunterKey) {
    try {
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
            organization?: string;
          };
        };

        const emails = hunterData.data?.emails ?? [];
        if (emails.length > 0) {
          const topEmail = emails[0];
          contact = {
            name:       `${topEmail.first_name} ${topEmail.last_name}`.trim() || ownerName,
            role:       topEmail.position || 'Contact',
            email:      topEmail.value,
            phone:      null,
            confidence: topEmail.confidence / 100,
            source:     'hunter',
          };
        }
      }
    } catch (err) {
      console.warn(`[ingest/contacts] Hunter.io standalone failed for ${entityId}:`, err);
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

    await setCache(`contact:${entityId}`, contact, 2_592_000);
  }

  return contact;
}

// ─── Async contact enrichment (on cache miss at query time) ───────────────

export async function enqueueContactEnrichment(entityId: string, ownerName: string): Promise<void> {
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
  console.log('  - Entity resolution:  Daily  10 AM');
  console.log('  - Portfolio graph:    Daily  2 PM');
  console.log('  - Contact pre-warm:  Daily  6 PM');
}