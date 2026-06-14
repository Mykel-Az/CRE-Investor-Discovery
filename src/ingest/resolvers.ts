// src/ingest/resolvers.ts
//
// Four-layer pipeline — real implementations:
//   Layer 1 — Parcel lookup (PostGIS / OpenAddresses / ATTOM)
//   Layer 2 — Entity resolution (SOS bulk filings / GLEIF / fuzzball)
//   Layer 3 — Portfolio graph (PostgreSQL edge table)
//   Layer 4 — Contact enrichment (Proxycurl primary / Hunter.io fallback)

import type { z } from 'zod';
import type {
  InvestorDiscoveryOutput,
  OwnerProfileOutput,
  ParcelLookupOutput,
} from '../schemas/cre.js';
import { query } from '../db/client.js';
import { getCached, setCache } from '../cache/helpers.js';
import { enqueueContactEnrichment } from './jobs.js';

type DiscoveryResult = z.infer<typeof InvestorDiscoveryOutput>;
type OwnerResult     = z.infer<typeof OwnerProfileOutput>;
type ParcelResult    = z.infer<typeof ParcelLookupOutput>;

// ─── Helpers ──────────────────────────────────────────────────────────────

/** How many seconds since a timestamp — used for freshness_secs */
function secsSince(ts: Date | string | null): number {
  if (!ts) return 0;
  return Math.floor((Date.now() - new Date(ts).getTime()) / 1000);
}

/** 24 hours in seconds — stale threshold */
const STALE_THRESHOLD = 86_400;

// Contact enrichment fires external API calls (Proxycurl/Hunter). Doing this
// at query time — even fire-and-forget — floods the event loop and DB pool
// when a corridor returns 25 owners, so it is OFF by default. Contacts are
// populated by the daily pre-warm cron instead. Set INLINE_CONTACT_ENRICHMENT
// =true to re-enable on-demand enrichment.
const INLINE_CONTACT_ENRICHMENT = process.env.INLINE_CONTACT_ENRICHMENT === 'true';

function freshness(updatedAt: Date | string | null): 'fresh' | 'stale' {
  return secsSince(updatedAt) > STALE_THRESHOLD ? 'stale' : 'fresh';
}

const STATE_ABBREV: Record<string, string> = {
  'ALABAMA':'AL','ALASKA':'AK','ARIZONA':'AZ','ARKANSAS':'AR','CALIFORNIA':'CA',
  'COLORADO':'CO','CONNECTICUT':'CT','DELAWARE':'DE','FLORIDA':'FL','GEORGIA':'GA',
  'HAWAII':'HI','IDAHO':'ID','ILLINOIS':'IL','INDIANA':'IN','IOWA':'IA',
  'KANSAS':'KS','KENTUCKY':'KY','LOUISIANA':'LA','MAINE':'ME','MARYLAND':'MD',
  'MASSACHUSETTS':'MA','MICHIGAN':'MI','MINNESOTA':'MN','MISSISSIPPI':'MS','MISSOURI':'MO',
  'MONTANA':'MT','NEBRASKA':'NE','NEVADA':'NV','NEW HAMPSHIRE':'NH','NEW JERSEY':'NJ',
  'NEW MEXICO':'NM','NEW YORK':'NY','NORTH CAROLINA':'NC','NORTH DAKOTA':'ND','OHIO':'OH',
  'OKLAHOMA':'OK','OREGON':'OR','PENNSYLVANIA':'PA','RHODE ISLAND':'RI','SOUTH CAROLINA':'SC',
  'SOUTH DAKOTA':'SD','TENNESSEE':'TN','TEXAS':'TX','UTAH':'UT','VERMONT':'VT',
  'VIRGINIA':'VA','WASHINGTON':'WA','WEST VIRGINIA':'WV','WISCONSIN':'WI','WYOMING':'WY',
};

// ─── Layer 1 + 2 + 3 + 4: Full corridor discovery ─────────────────────────

export async function resolveCorridorOwners(params: {
  corridor: string;
  property_type: string;
  lot_size_min_acres: number;
  lot_size_max_acres?: number;
  max_results: number;
}): Promise<DiscoveryResult> {
  const _t0 = Date.now();
  const corridorParts = params.corridor.split(',').map((s) => s.trim());
  const roadName  = corridorParts[0] ?? params.corridor;
  const rawState  = corridorParts[1]?.toUpperCase().trim() ?? '';
  const roadState = STATE_ABBREV[rawState] ?? rawState;

  const searchRadius = parseInt(process.env.CORRIDOR_SEARCH_RADIUS_METERS ?? '400', 10);

  // Expand route-number variants so "Route 1" also matches "US 1", "US Highway 1", etc.
  // Only apply ILIKE for numbered routes — named streets (Broadway, Atlantic Ave) use FTS only
  // to avoid full sequential scans on the roads table.
  const routeNum = roadName.match(/\b(\d+[A-Z]?)\b/i)?.[1];
  const hasRouteNum = routeNum != null;
  const ilikePatterns = hasRouteNum ? [
    `%Route ${routeNum}%`,
    `%US ${routeNum}%`,
    `%US Route ${routeNum}%`,
    `%US Highway ${routeNum}%`,
    `%Highway ${routeNum}%`,
    `%US-${routeNum}%`,
    `%NJ ${routeNum}%`,
    `%NY ${routeNum}%`,
  ] : [];

  // ── Edges-first corridor query ──────────────────────────────────────────
  // Drive from the ~38K owned parcels (portfolio_edges), NOT the 16M parcels.
  // For each owned parcel we check nearby corridor roads via the roads
  // GIST + FTS indexes. The tool only ever surfaces parcels that have a
  // resolved owner, so starting from the owned set turns a 200s+ scan of
  // every parcel near 1,000+ road segments into a few seconds.
  const roadExists = hasRouteNum
    ? `(to_tsvector('english', r.road_name) @@ plainto_tsquery('english', $4) OR r.road_name ILIKE ANY($7::text[]))`
    : `to_tsvector('english', r.road_name) @@ plainto_tsquery('english', $4)`;

  // Fetch a candidate pool proportional to how many owners we need, not a flat
  // 500. A global ORDER BY would force every matching parcel to be found and
  // sorted (the slow path on dense corridors like Atlantic Avenue); instead we
  // let Postgres stop early at the cap and sort the fetched set in JS below.
  const fetchCap = Math.min(500, Math.max(100, (params.max_results ?? 10) * 15));

  const corridorQuery = `
    SELECT
      p.parcel_id, p.address, p.city, p.state, p.zip,
      p.lot_size_acres, p.building_sqft, p.property_type,
      p.last_sale_price, p.last_sale_date::text, p.loan_maturity::text,
      ST_Y(p.geom::geometry) AS latitude,
      ST_X(p.geom::geometry) AS longitude,
      pe.entity_id, pe.confidence AS resolution_confidence
    FROM portfolio_edges pe
    JOIN parcels p ON p.parcel_id = pe.parcel_id
    WHERE UPPER(p.property_type) = UPPER($1)
      AND (p.lot_size_acres IS NULL OR p.lot_size_acres >= $2)
      AND ($3::double precision IS NULL OR p.lot_size_acres <= $3)
      AND EXISTS (
        SELECT 1 FROM roads r
        WHERE ${roadExists}
          AND ($5 = '' OR UPPER(r.state) = $5)
          AND ST_DWithin(p.geom, r.geom, $6)
      )
    LIMIT ${fetchCap}
  `;

  const corridorParams = hasRouteNum
    ? [params.property_type, params.lot_size_min_acres, params.lot_size_max_acres ?? null, roadName, roadState, searchRadius, ilikePatterns]
    : [params.property_type, params.lot_size_min_acres, params.lot_size_max_acres ?? null, roadName, roadState, searchRadius];

  const corridorResult = await query(corridorQuery, corridorParams);
  const corridorRows = (corridorResult.rows as Record<string, unknown>[])
    .sort((a, b) => ((b.lot_size_acres as number) ?? 0) - ((a.lot_size_acres as number) ?? 0));

  if (corridorRows.length === 0) {
    // Return closest road names so the agent knows what corridor names exist
    // GROUP BY (not SELECT DISTINCT) so the similarity() sort key is allowed
    // under Postgres 18, which rejects ORDER BY exprs absent from a DISTINCT list.
    const closestRoadsResult = await query(
      `SELECT road_name, MAX(similarity(road_name, $2)) AS sim
       FROM roads
       WHERE ($1 = '' OR UPPER(state) = $1)
       GROUP BY road_name
       ORDER BY sim DESC
       LIMIT 5`,
      [roadState, roadName]
    ).catch(() => ({ rows: [] as Record<string, unknown>[] }));

    const closest_corridors = (closestRoadsResult.rows as Record<string, unknown>[])
      .map((r) => r.road_name as string)
      .filter(Boolean);

    return {
      status: 'empty',
      query_summary: {
        corridor:           params.corridor,
        property_type:      params.property_type,
        lot_size_min_acres: params.lot_size_min_acres,
        matched_parcels:    0,
        unique_owners:      0,
        closest_corridors:  closest_corridors.length > 0 ? closest_corridors : undefined,
      },
      owners:     [],
      fallback:   false,
      queried_at: new Date().toISOString(),
    };
  }

  // Group corridor parcels by owner, preserving lot-size-desc order for selection
  const corridorPropsByEntity = new Map<string, Record<string, unknown>[]>();
  const confidenceByEntity = new Map<string, number>();
  const entityOrder: string[] = [];
  for (const row of corridorRows) {
    const eid = row.entity_id as string;
    if (!eid) continue;
    if (!corridorPropsByEntity.has(eid)) {
      corridorPropsByEntity.set(eid, []);
      entityOrder.push(eid);
    }
    corridorPropsByEntity.get(eid)!.push(row);
    const conf = (row.resolution_confidence as number) ?? 0;
    confidenceByEntity.set(eid, Math.max(confidenceByEntity.get(eid) ?? 0, conf));
  }

  // Hard-cap the number of owners returned. The Context librarian agent has to
  // read and synthesize the entire tool result, so a 25+ owner payload (each
  // with nested corridor properties) drives 150s+ of model time. 15 owners is
  // ample for corridor discovery and keeps synthesis fast.
  const OWNER_RESPONSE_CAP = 15;
  const entityIds = entityOrder.slice(0, Math.min(params.max_results, OWNER_RESPONSE_CAP));

  // Entity metadata + full-portfolio summary + contacts — 3 batch queries
  const [entityMetaBatch, portfolioBatch, contactBatch] = await Promise.all([
    query(`
      SELECT entity_id, canonical_name, entity_type, jurisdiction, status, updated_at
      FROM entities WHERE entity_id = ANY($1)
    `, [entityIds]),

    query(`
      SELECT
        pe.entity_id,
        COUNT(*)::int AS property_count,
        SUM(p.building_sqft)::double precision AS total_sqft,
        ARRAY_AGG(DISTINCT p.state) AS states_present,
        ARRAY_AGG(DISTINCT p.property_type) AS property_types
      FROM portfolio_edges pe
      JOIN parcels p ON p.parcel_id = pe.parcel_id
      WHERE pe.entity_id = ANY($1)
      GROUP BY pe.entity_id
    `, [entityIds]),

    query(`
      SELECT entity_id, name, role, email, phone, confidence, source
      FROM contacts
      WHERE entity_id = ANY($1) AND expires_at > NOW()
    `, [entityIds]),
  ]);

  const entityMetaByEntity = new Map<string, Record<string, unknown>>(
    (entityMetaBatch.rows as Record<string, unknown>[]).map((r) => [r.entity_id as string, r])
  );

  const portfolioByEntity = new Map<string, Record<string, unknown>>(
    (portfolioBatch.rows as Record<string, unknown>[]).map((r) => [r.entity_id as string, r])
  );

  const contactByEntity = new Map<string, Record<string, unknown>>(
    (contactBatch.rows as Record<string, unknown>[]).map((r) => [r.entity_id as string, r])
  );

  // Selected owners in deterministic (lot-size-desc) order
  const selectedOwners: Record<string, unknown>[] = entityIds.map((eid) => {
    const meta = entityMetaByEntity.get(eid) ?? {};
    return {
      entity_id:             eid,
      canonical_name:        meta.canonical_name,
      entity_type:           meta.entity_type,
      updated_at:            meta.updated_at,
      resolution_confidence: confidenceByEntity.get(eid) ?? 0,
    };
  });

  // Build owner objects — Redis cache checks still run concurrently per owner
  const owners = await Promise.all(
    selectedOwners.map(async (owner: Record<string, unknown>) => {
      const entityId  = owner.entity_id as string;
      const updatedAt = owner.updated_at as string;

      const pf    = portfolioByEntity.get(entityId);
      const props = corridorPropsByEntity.get(entityId) ?? [];

      let contact = null;
      let contactStatus: 'available' | 'not_available' | 'pending' = 'not_available';

      const dbContact = contactByEntity.get(entityId);
      if (dbContact) {
        contact = {
          name:       (dbContact.name as string) ?? '',
          role:       (dbContact.role as string) ?? '',
          email:      (dbContact.email as string) ?? null,
          phone:      (dbContact.phone as string) ?? null,
          confidence: (dbContact.confidence as number) ?? 0,
          source:     (dbContact.source as string) ?? 'unknown',
        };
        contactStatus = 'available';
      } else if (INLINE_CONTACT_ENRICHMENT) {
        enqueueContactEnrichment(entityId, (owner.canonical_name as string) ?? '');
        contactStatus = 'pending';
      } else {
        contactStatus = 'pending';
      }

      const resolutionConfidence = (owner.resolution_confidence as number) ?? 0;

      return {
        owner_name:                   (owner.canonical_name as string) ?? 'Unknown',
        entity_id:                    entityId,
        entity_type:                  (owner.entity_type as 'LLC' | 'Trust' | 'Corporation' | 'Individual' | 'Unknown') ?? 'Unknown',
        entity_resolution_confidence: resolutionConfidence,
        resolution_required:          resolutionConfidence < 0.85,
        portfolio_summary: {
          property_count: (pf?.property_count as number) ?? 0,
          total_sqft:     (pf?.total_sqft as number) ?? null,
          states_present: (pf?.states_present as string[]) ?? [],
          property_types: (pf?.property_types as string[]) ?? [],
        },
        contact,
        contact_status: contactStatus,
        // Cap the corridor-property list per owner: the full count is in
        // portfolio_summary, and a long nested array per owner balloons the
        // payload the librarian agent must synthesize.
        properties_in_corridor: props.slice(0, 3).map((p: Record<string, unknown>) => ({
          address:         (p.address as string) ?? '',
          city:            (p.city as string) ?? '',
          state:           (p.state as string) ?? '',
          zip:             (p.zip as string) ?? '',
          lot_size_acres:  (p.lot_size_acres as number) ?? null,
          building_sqft:   (p.building_sqft as number) ?? null,
          property_type:   (p.property_type as string) ?? '',
          last_sale_price: (p.last_sale_price as number) ?? null,
          last_sale_date:  (p.last_sale_date as string) ?? null,
          loan_maturity:   (p.loan_maturity as string) ?? null,
          parcel_id:       (p.parcel_id as string) ?? '',
          latitude:        (p.latitude as number) ?? null,
          longitude:       (p.longitude as number) ?? null,
        })),
        data_freshness: freshness(updatedAt),
        freshness_secs: secsSince(updatedAt),
      };
    })
  );

  for (const o of owners) {
    query(
      'INSERT INTO query_log (tool_name, entity_id, corridor) VALUES ($1, $2, $3)',
      ['investor_discovery', o.entity_id, params.corridor]
    ).catch(() => {});
  }

  console.log(`[investor_discovery] ${params.corridor} ${params.property_type} → ${owners.length} owners in ${Date.now() - _t0}ms`);

  return {
    status:        owners.length > 0 ? 'ok' : 'empty',
    query_summary: {
      corridor:           params.corridor,
      property_type:      params.property_type,
      lot_size_min_acres: params.lot_size_min_acres,
      matched_parcels:    corridorRows.length,
      unique_owners:      owners.length,
    },
    owners,
    fallback:   false,
    queried_at: new Date().toISOString(),
  };
}

// ─── Layer 2 + 3 + 4: Owner profile by entity_id ──────────────────────────

export async function resolveOwnerProfile(params: {
  entity_id: string;
  include_all_properties: boolean;
}): Promise<OwnerResult> {
  // Entity lookup
  const entityResult = await query(
    `SELECT entity_id, canonical_name, entity_type, jurisdiction, status,
            incorporated_at::text, registered_agent_name, registered_agent_address,
            source, updated_at
     FROM entities WHERE entity_id = $1`,
    [params.entity_id]
  );

  if (entityResult.rows.length === 0) {
    return {
      entity_id:         params.entity_id,
      canonical_name:    'Unknown Entity',
      entity_type:       'Unknown',
      jurisdiction:      'Unknown',
      status:            'unknown',
      incorporated_at:   null,
      registered_agent:  null,
      beneficial_owners: [],
      contact:           null,
      contact_status:    'not_available',
      portfolio_summary: { property_count: 0, total_sqft: null, states_present: [], property_types: [] },
      properties:        [],
      source:            'not_found',
      data_freshness:    'stale',
      freshness_secs:    0,
      confidence:        0,
    };
  }

  const entity = entityResult.rows[0] as Record<string, unknown>;
  const updatedAt = entity.updated_at as string;

  // Beneficial owners
  const boResult = await query(
    `SELECT owner_name, role, ownership_pct FROM beneficial_owners WHERE entity_id = $1`,
    [params.entity_id]
  );

  // Portfolio summary
  const portfolioResult = await query(
    `SELECT
       COUNT(*)::int AS property_count,
       SUM(p.building_sqft)::double precision AS total_sqft,
       ARRAY_AGG(DISTINCT p.state) AS states_present,
       ARRAY_AGG(DISTINCT p.property_type) AS property_types
     FROM portfolio_edges pe
     JOIN parcels p ON p.parcel_id = pe.parcel_id
     WHERE pe.entity_id = $1`,
    [params.entity_id]
  );
  const pf = portfolioResult.rows[0] as Record<string, unknown> | undefined;

  // Properties (full list or empty depending on flag)
  let properties: Record<string, unknown>[] = [];
  if (params.include_all_properties) {
    const propsResult = await query(
      `SELECT
         p.parcel_id, p.address, p.city, p.state, p.zip,
         p.lot_size_acres, p.building_sqft, p.property_type,
         p.last_sale_price, p.last_sale_date::text, p.loan_maturity::text,
         ST_Y(p.geom::geometry) AS latitude,
         ST_X(p.geom::geometry) AS longitude
       FROM portfolio_edges pe
       JOIN parcels p ON p.parcel_id = pe.parcel_id
       WHERE pe.entity_id = $1
       ORDER BY p.state, p.city`,
      [params.entity_id]
    );
    properties = propsResult.rows as Record<string, unknown>[];
  }

  // Contact
  let contact = null;
  let contactStatus: 'available' | 'not_available' | 'pending' = 'not_available';

  const contactResult = await query(
    `SELECT name, role, email, phone, confidence, source
     FROM contacts WHERE entity_id = $1 AND expires_at > NOW()`,
    [params.entity_id]
  );

  if (contactResult.rows.length > 0) {
    const c = contactResult.rows[0] as Record<string, unknown>;
    contact = {
      name:       (c.name as string) ?? '',
      role:       (c.role as string) ?? '',
      email:      (c.email as string) ?? null,
      phone:      (c.phone as string) ?? null,
      confidence: (c.confidence as number) ?? 0,
      source:     (c.source as string) ?? 'unknown',
    };
    contactStatus = 'available';
  } else if (INLINE_CONTACT_ENRICHMENT) {
    enqueueContactEnrichment(params.entity_id, (entity.canonical_name as string) ?? '');
    contactStatus = 'pending';
  } else {
    contactStatus = 'pending';
  }

  // Log query
  query(
    'INSERT INTO query_log (tool_name, entity_id) VALUES ($1, $2)',
    ['owner_profile', params.entity_id]
  ).catch(() => { /* best-effort */ });

  return {
    entity_id:         (entity.entity_id as string),
    canonical_name:    (entity.canonical_name as string),
    entity_type:       (entity.entity_type as 'LLC' | 'Trust' | 'Corporation' | 'Individual' | 'Unknown') ?? 'Unknown',
    jurisdiction:      (entity.jurisdiction as string) ?? 'Unknown',
    status:            (entity.status as 'active' | 'dissolved' | 'suspended' | 'unknown') ?? 'unknown',
    incorporated_at:   (entity.incorporated_at as string) ?? null,
    registered_agent:  entity.registered_agent_name
      ? { name: entity.registered_agent_name as string, address: (entity.registered_agent_address as string) ?? '' }
      : null,
    beneficial_owners: boResult.rows.map((bo: Record<string, unknown>) => ({
      name:          (bo.owner_name as string) ?? '',
      role:          (bo.role as string) ?? '',
      ownership_pct: (bo.ownership_pct as number) ?? null,
    })),
    contact,
    contact_status: contactStatus,
    portfolio_summary: {
      property_count: (pf?.property_count as number) ?? 0,
      total_sqft:     (pf?.total_sqft as number) ?? null,
      states_present: (pf?.states_present as string[]) ?? [],
      property_types: (pf?.property_types as string[]) ?? [],
    },
    properties: properties.map((p) => ({
      address:         (p.address as string) ?? '',
      city:            (p.city as string) ?? '',
      state:           (p.state as string) ?? '',
      zip:             (p.zip as string) ?? '',
      lot_size_acres:  (p.lot_size_acres as number) ?? null,
      building_sqft:   (p.building_sqft as number) ?? null,
      property_type:   (p.property_type as string) ?? '',
      last_sale_price: (p.last_sale_price as number) ?? null,
      last_sale_date:  (p.last_sale_date as string) ?? null,
      loan_maturity:   (p.loan_maturity as string) ?? null,
      parcel_id:       (p.parcel_id as string) ?? '',
      latitude:        (p.latitude as number) ?? null,
      longitude:       (p.longitude as number) ?? null,
    })),
    source:         (entity.source as string) ?? 'unknown',
    data_freshness: freshness(updatedAt),
    freshness_secs: secsSince(updatedAt),
    confidence:     1.0,
  };
}

// ─── Layer 1: Single parcel by address ────────────────────────────────────

export async function resolveParcel(address: string): Promise<ParcelResult> {
  // Parse "Street, City ST". The street drives matching against p.address,
  // which has both an FTS GIN index and a trigram index. Matching a
  // concatenation of address+city+state+zip (the old query) defeats every
  // index and forces a sequential scan of all 16M parcels — the cause of the
  // 38s lookups.
  const parts  = address.split(',').map((s) => s.trim()).filter(Boolean);
  const street = parts[0] ?? address;
  const city   = (parts[1] ?? '').replace(/\s+[A-Za-z]{2}$/, '').trim(); // drop trailing state abbrev

  const SELECT = `
    SELECT
      p.parcel_id, p.address, p.city, p.state, p.zip,
      p.lot_size_acres, p.building_sqft, p.property_type,
      p.last_sale_price, p.last_sale_date::text, p.loan_maturity::text,
      ST_Y(p.geom::geometry) AS latitude,
      ST_X(p.geom::geometry) AS longitude,
      p.owner_name_raw, p.updated_at,
      pe.entity_id,
      e.canonical_name AS owner_name
    FROM parcels p
    LEFT JOIN portfolio_edges pe ON pe.parcel_id = p.parcel_id
    LEFT JOIN entities e ON e.entity_id = pe.entity_id`;

  // City is a soft boost (prefer the matching city), never a hard filter — so a
  // mis-parsed city can't turn a real match into a false "not found".
  const ORDER = `
    ORDER BY
      (CASE WHEN $2 <> '' AND p.city ILIKE '%' || $2 || '%' THEN 1 ELSE 0 END) DESC,
      similarity(p.address, $1) DESC,
      pe.confidence DESC NULLS LAST
    LIMIT 5`;

  // Primary: FTS on p.address (idx_parcels_address) — fast and precise.
  let result = await query(
    `${SELECT}
     WHERE to_tsvector('english', p.address) @@ plainto_tsquery('english', $1)
     ${ORDER}`,
    [street, city]
  );

  // Fallback: trigram fuzzy match (idx_parcels_address_trgm) when FTS misses
  // (abbreviations, ordinals, typos).
  if (result.rows.length === 0) {
    result = await query(
      `${SELECT}
       WHERE p.address % $1
       ${ORDER}`,
      [street, city]
    );
  }

  if (result.rows.length === 0) {
    return {
      status:         'not_found',
      parcel:         null,
      owner_name:     null,
      entity_id:      null,
      data_freshness: 'fresh',
      freshness_secs: 0,
    };
  }

  if (result.rows.length > 1) {
    // Only ambiguous if distinct parcels at different locations (not same building/lot splits)
    const uniqueAddresses = new Set(
      result.rows.map((r: Record<string, unknown>) => `${r.address as string}|${r.city as string}|${r.state as string}`)
    );
    if (uniqueAddresses.size > 1) {
      // Multiple distinct locations — prefer rows with a linked entity
      const withEntity = result.rows.filter((r: Record<string, unknown>) => r.entity_id);
      if (withEntity.length === 0) {
        // Take the best similarity match (first row, already ordered)
        // rather than returning ambiguous — gives the user something useful
      } else {
        // Use the first entity-linked parcel
        result.rows.splice(0, result.rows.length, withEntity[0]);
      }
    }
  }

  const row = result.rows[0] as Record<string, unknown>;
  const updatedAt = row.updated_at as string;

  return {
    status: 'found',
    parcel: {
      address:         (row.address as string) ?? '',
      city:            (row.city as string) ?? '',
      state:           (row.state as string) ?? '',
      zip:             (row.zip as string) ?? '',
      lot_size_acres:  (row.lot_size_acres as number) ?? null,
      building_sqft:   (row.building_sqft as number) ?? null,
      property_type:   (row.property_type as string) ?? '',
      last_sale_price: (row.last_sale_price as number) ?? null,
      last_sale_date:  (row.last_sale_date as string) ?? null,
      loan_maturity:   (row.loan_maturity as string) ?? null,
      parcel_id:       (row.parcel_id as string) ?? '',
      latitude:        (row.latitude as number) ?? null,
      longitude:       (row.longitude as number) ?? null,
    },
    owner_name:     (row.owner_name as string) ?? (row.owner_name_raw as string) ?? null,
    entity_id:      (row.entity_id as string) ?? null,
    data_freshness: freshness(updatedAt),
    freshness_secs: secsSince(updatedAt),
  };
}
