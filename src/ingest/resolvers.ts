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

function freshness(updatedAt: Date | string | null): 'fresh' | 'stale' {
  return secsSince(updatedAt) > STALE_THRESHOLD ? 'stale' : 'fresh';
}

// ─── Layer 1 + 2 + 3 + 4: Full corridor discovery ─────────────────────────

export async function resolveCorridorOwners(params: {
  corridor: string;
  property_type: string;
  lot_size_min_acres: number;
  lot_size_max_acres?: number;
  max_results: number;
}): Promise<DiscoveryResult> {
  // Step 1: Spatial query — find parcels within 400m of the named road corridor
  const corridorParts = params.corridor.split(',').map((s) => s.trim());
  const roadName  = corridorParts[0] ?? params.corridor;
  const roadState = corridorParts[1]?.toUpperCase() ?? '';

  const searchRadius = parseInt(process.env.CORRIDOR_SEARCH_RADIUS_METERS ?? '400', 10);

  const parcelQuery = `
    SELECT
      p.parcel_id, p.address, p.city, p.state, p.zip,
      p.lot_size_acres, p.building_sqft, p.property_type,
      p.last_sale_price, p.last_sale_date::text, p.loan_maturity::text,
      ST_Y(p.geom::geometry) AS latitude,
      ST_X(p.geom::geometry) AS longitude,
      p.owner_name_raw, p.updated_at
    FROM parcels p
    JOIN roads r ON ST_DWithin(p.geom, r.geom, $1)
    WHERE to_tsvector('english', r.road_name) @@ plainto_tsquery('english', $2)
      AND ($3 = '' OR UPPER(r.state) = $3)
      AND UPPER(p.property_type) = UPPER($4)
      AND (p.lot_size_acres IS NULL OR p.lot_size_acres >= $5)
      AND ($6::double precision IS NULL OR p.lot_size_acres <= $6)
    ORDER BY p.lot_size_acres DESC NULLS LAST
    LIMIT 500
  `;

  const parcelsResult = await query(parcelQuery, [
    searchRadius,
    roadName,
    roadState,
    params.property_type,
    params.lot_size_min_acres,
    params.lot_size_max_acres ?? null,
  ]);

  if (parcelsResult.rows.length === 0) {
    return {
      status: 'empty',
      query_summary: {
        corridor:           params.corridor,
        property_type:      params.property_type,
        lot_size_min_acres: params.lot_size_min_acres,
        matched_parcels:    0,
        unique_owners:      0,
      },
      owners:     [],
      fallback:   false,
      queried_at: new Date().toISOString(),
    };
  }

  // Step 2: Group parcels by owner via portfolio_edges → entities
  const parcelIds = parcelsResult.rows.map((r: Record<string, unknown>) => r.parcel_id as string);

  const ownerQuery = `
    SELECT DISTINCT ON (e.entity_id)
      e.entity_id, e.canonical_name, e.entity_type, e.jurisdiction,
      e.status, e.updated_at,
      pe.confidence AS resolution_confidence
    FROM portfolio_edges pe
    JOIN entities e ON e.entity_id = pe.entity_id
    WHERE pe.parcel_id = ANY($1)
    ORDER BY e.entity_id, pe.confidence DESC
    LIMIT $2
  `;

  const ownersResult = await query(ownerQuery, [parcelIds, params.max_results]);

  // Step 3: For each owner, build the full response object
  const owners = await Promise.all(
    ownersResult.rows.map(async (owner: Record<string, unknown>) => {
      const entityId     = owner.entity_id as string;
      const updatedAt    = owner.updated_at as string;

      // Portfolio summary
      const portfolioQuery = `
        SELECT
          COUNT(*)::int AS property_count,
          SUM(p.building_sqft)::double precision AS total_sqft,
          ARRAY_AGG(DISTINCT p.state) AS states_present,
          ARRAY_AGG(DISTINCT p.property_type) AS property_types
        FROM portfolio_edges pe
        JOIN parcels p ON p.parcel_id = pe.parcel_id
        WHERE pe.entity_id = $1
      `;
      const portfolioResult = await query(portfolioQuery, [entityId]);
      const pf = portfolioResult.rows[0] as Record<string, unknown> | undefined;

      // Properties in this corridor
      const corridorPropsQuery = `
        SELECT
          p.parcel_id, p.address, p.city, p.state, p.zip,
          p.lot_size_acres, p.building_sqft, p.property_type,
          p.last_sale_price, p.last_sale_date::text, p.loan_maturity::text,
          ST_Y(p.geom::geometry) AS latitude,
          ST_X(p.geom::geometry) AS longitude
        FROM portfolio_edges pe
        JOIN parcels p ON p.parcel_id = pe.parcel_id
        WHERE pe.entity_id = $1
          AND pe.parcel_id = ANY($2)
      `;
      const corridorProps = await query(corridorPropsQuery, [entityId, parcelIds]);

      // Contact (from DB, then Redis cache)
      let contact = null;
      let contactStatus: 'available' | 'not_available' | 'pending' = 'not_available';

      const contactResult = await query(
        `SELECT name, role, email, phone, confidence, source, expires_at
         FROM contacts WHERE entity_id = $1 AND expires_at > NOW()`,
        [entityId]
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
      } else {
        // Check Redis for pending enrichment, or enqueue
        const cachedContact = await getCached<Record<string, unknown>>(`contact:${entityId}`);
        if (cachedContact) {
          contact = cachedContact as unknown as typeof contact;
          contactStatus = 'available';
        } else {
          // Enqueue async enrichment — will be available on next query
          await enqueueContactEnrichment(entityId, (owner.canonical_name as string) ?? '');
          contactStatus = 'pending';
        }
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
        properties_in_corridor: corridorProps.rows.map((p: Record<string, unknown>) => ({
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

  // Log query for pre-warming analytics
  for (const o of owners) {
    query(
      'INSERT INTO query_log (tool_name, entity_id, corridor) VALUES ($1, $2, $3)',
      ['investor_discovery', o.entity_id, params.corridor]
    ).catch(() => { /* best-effort logging */ });
  }

  return {
    status:        owners.length > 0 ? 'ok' : 'empty',
    query_summary: {
      corridor:           params.corridor,
      property_type:      params.property_type,
      lot_size_min_acres: params.lot_size_min_acres,
      matched_parcels:    parcelsResult.rows.length,
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
  } else {
    const cachedContact = await getCached<Record<string, unknown>>(`contact:${params.entity_id}`);
    if (cachedContact) {
      contact = cachedContact as unknown as typeof contact;
      contactStatus = 'available';
    } else {
      await enqueueContactEnrichment(params.entity_id, (entity.canonical_name as string) ?? '');
      contactStatus = 'pending';
    }
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
  // Full-text search on address with fallback to ILIKE
  const result = await query(
    `SELECT
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
     LEFT JOIN entities e ON e.entity_id = pe.entity_id
     WHERE to_tsvector('english', p.address || ' ' || p.city || ' ' || p.state || ' ' || p.zip)
           @@ plainto_tsquery('english', $1)
        OR UPPER(p.address || ' ' || p.city || ' ' || p.state) LIKE UPPER($2)
     ORDER BY pe.confidence DESC NULLS LAST
     LIMIT 5`,
    [address, `%${address.replace(/\s+/g, '%')}%`]
  );

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
    // Check if multiple distinct parcels matched — ambiguous
    const uniqueParcels = new Set(result.rows.map((r: Record<string, unknown>) => r.parcel_id));
    if (uniqueParcels.size > 1) {
      return {
        status:         'ambiguous',
        parcel:         null,
        owner_name:     null,
        entity_id:      null,
        data_freshness: 'fresh',
        freshness_secs: 0,
      };
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
