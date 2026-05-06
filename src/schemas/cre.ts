// src/schemas/cre.ts
import { z } from 'zod';

// ─── Shared sub-schemas ────────────────────────────────────────────────────

export const ContactSchema = z.object({
  name:       z.string(),
  role:       z.string(),
  email:      z.string().nullable(),
  phone:      z.string().nullable(),
  confidence: z.number().min(0).max(1),
  source:     z.string(),
});

export const PropertySchema = z.object({
  address:         z.string(),
  city:            z.string(),
  state:           z.string(),
  zip:             z.string(),
  lot_size_acres:  z.number().nullable(),
  building_sqft:   z.number().nullable(),
  property_type:   z.string(),
  last_sale_price: z.number().nullable(),
  last_sale_date:  z.string().nullable(),
  loan_maturity:   z.string().nullable(),
  parcel_id:       z.string(),
  latitude:        z.number().nullable(),
  longitude:       z.number().nullable(),
});

export const PortfolioSummarySchema = z.object({
  property_count: z.number(),
  total_sqft:     z.number().nullable(),
  states_present: z.array(z.string()),
  property_types: z.array(z.string()),
});

// ─── Tool 1: investor_discovery ───────────────────────────────────────────

export const InvestorDiscoveryInput = z.object({
  corridor: z
    .string()
    .default('Highway 9, NJ')
    .describe('Road corridor and state, e.g. "Highway 9, NJ" or "Route 1, CA"'),
  property_type: z
    .string()
    .default('Retail')
    .describe('CRE property type: Retail, Office, Industrial, Multifamily, Mixed-Use, Land'),
  lot_size_min_acres: z
    .number()
    .optional()
    .default(0)
    .describe('Minimum lot size in acres (0 = no minimum)'),
  lot_size_max_acres: z
    .number()
    .optional()
    .describe('Maximum lot size in acres (omit for no maximum)'),
  max_results: z
    .number()
    .min(1)
    .max(50)
    .optional()
    .default(10)
    .describe('Max owner records to return (1-50)'),
});

export const InvestorDiscoveryOutput = z.object({
  status:        z.enum(['ok', 'empty', 'partial']),
  query_summary: z.object({
    corridor:           z.string(),
    property_type:      z.string(),
    lot_size_min_acres: z.number(),
    matched_parcels:    z.number(),
    unique_owners:      z.number(),
  }),
  owners: z.array(z.object({
    owner_name:                   z.string(),
    entity_id:                    z.string(),
    entity_type:                  z.enum(['LLC', 'Trust', 'Corporation', 'Individual', 'Unknown']),
    entity_resolution_confidence: z.number().min(0).max(1),
    resolution_required:          z.boolean(),
    portfolio_summary:            PortfolioSummarySchema,
    contact:                      ContactSchema.nullable(),
    contact_status:               z.enum(['available', 'not_available', 'pending']),
    properties_in_corridor:       z.array(PropertySchema),
    data_freshness:               z.enum(['fresh', 'stale']),
    freshness_secs:               z.number(),
  })),
  fallback:   z.boolean(),
  queried_at: z.string(),
});

// ─── Tool 2: owner_profile ────────────────────────────────────────────────

export const OwnerProfileInput = z.object({
  entity_id: z
    .string()
    .describe('entity_id from investor_discovery response'),
  include_all_properties: z
    .boolean()
    .optional()
    .default(false)
    .describe('If true, return full nationwide portfolio (may be large)'),
});

export const OwnerProfileOutput = z.object({
  entity_id:         z.string(),
  canonical_name:    z.string(),
  entity_type:       z.enum(['LLC', 'Trust', 'Corporation', 'Individual', 'Unknown']),
  jurisdiction:      z.string(),
  status:            z.enum(['active', 'dissolved', 'suspended', 'unknown']),
  incorporated_at:   z.string().nullable(),
  registered_agent:  z.object({ name: z.string(), address: z.string() }).nullable(),
  beneficial_owners: z.array(z.object({
    name:          z.string(),
    role:          z.string(),
    ownership_pct: z.number().nullable(),
  })),
  contact:           ContactSchema.nullable(),
  contact_status:    z.enum(['available', 'not_available', 'pending']),
  portfolio_summary: PortfolioSummarySchema,
  properties:        z.array(PropertySchema),
  source:            z.string(),
  data_freshness:    z.enum(['fresh', 'stale']),
  freshness_secs:    z.number(),
  confidence:        z.number().min(0).max(1),
});

// ─── Tool 3: parcel_lookup ────────────────────────────────────────────────

export const ParcelLookupInput = z.object({
  address: z
    .string()
    .default('1240 Highway 9, Woodbridge NJ')
    .describe('Full street address of the commercial property'),
});

export const ParcelLookupOutput = z.object({
  status:         z.enum(['found', 'not_found', 'ambiguous']),
  parcel:         PropertySchema.nullable(),
  owner_name:     z.string().nullable(),
  entity_id:      z.string().nullable(),
  data_freshness: z.enum(['fresh', 'stale']),
  freshness_secs: z.number(),
});

// ─── Input shapes for registerTool (ZodRawShapeCompat) ────────────────────
// registerTool expects the .shape of a ZodObject, not the ZodObject itself.

export const investorDiscoveryInputShape = InvestorDiscoveryInput.shape;
export const ownerProfileInputShape      = OwnerProfileInput.shape;
export const parcelLookupInputShape      = ParcelLookupInput.shape;
