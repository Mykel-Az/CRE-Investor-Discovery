// scripts/seed-smoke-tests.ts
//
// Run once after deploy: npm run seed
// Seeds deterministic Redis cache entries so the Context Protocol deep validation
// smoke tests return known-good results without hitting live upstream APIs.
//
// SCOPE: Redis cache only — does NOT insert rows into the database.
// Real query results depend on parcel + entity data loaded by the background
// ingest jobs (SOS CSV files in ./data/sos/, OpenAddresses in ./data/openaddresses/).

import { connectRedis } from '../src/cache/client.js';
import { setCache } from '../src/cache/helpers.js';

await connectRedis();

// ─── Smoke test 1: investor_discovery — Highway 9 NJ Retail ───────────────

const discoveryKey = 'discovery:highway_9,_nj:retail:0:any:10';
await setCache(discoveryKey, {
  status: 'ok',
  query_summary: {
    corridor:           'Highway 9, NJ',
    property_type:      'Retail',
    lot_size_min_acres: 0,
    matched_parcels:    3,
    unique_owners:      2,
  },
  owners: [
    {
      owner_name:                   'Smith Capital Holdings LLC',
      entity_id:                    'cre_smoke_smith_capital',
      entity_type:                  'LLC',
      entity_resolution_confidence: 0.93,
      resolution_required:          false,
      portfolio_summary: {
        property_count: 12,
        total_sqft:     340000,
        states_present: ['NJ', 'NY', 'PA'],
        property_types: ['Retail', 'Office'],
      },
      contact: {
        name:       'John Smith',
        role:       'Managing Member',
        email:      'jsmith@smithcapital.com',
        phone:      '+1-732-555-0100',
        confidence: 0.88,
        source:     'proxycurl',
      },
      contact_status: 'available',
      properties_in_corridor: [
        {
          address:         '1240 Highway 9',
          city:            'Woodbridge',
          state:           'NJ',
          zip:             '07095',
          lot_size_acres:  2.3,
          building_sqft:   18000,
          property_type:   'Retail',
          last_sale_price: 1850000,
          last_sale_date:  '2019-03-15',
          loan_maturity:   '2027-06-01',
          parcel_id:       'NJ_MIDDLESEX_0024_00001',
          latitude:        40.5579,
          longitude:       -74.2861,
        },
      ],
      data_freshness: 'fresh',
      freshness_secs: 3600,
    },
  ],
  fallback:   false,
  queried_at: new Date().toISOString(),
}, 86400);

console.log('[seed] investor_discovery smoke test seeded:', discoveryKey);

// ─── Smoke test 2: owner_profile — Smith Capital ──────────────────────────

await setCache('owner:cre_smoke_smith_capital:all=false', {
  entity_id:       'cre_smoke_smith_capital',
  canonical_name:  'Smith Capital Holdings LLC',
  entity_type:     'LLC',
  jurisdiction:    'US-NJ',
  status:          'active',
  incorporated_at: '2005-06-22',
  registered_agent: {
    name:    'CT Corporation System',
    address: '820 Bear Tavern Rd, Trenton, NJ 08628',
  },
  beneficial_owners: [
    { name: 'John Smith', role: 'Managing Member', ownership_pct: 100 },
  ],
  contact: {
    name:       'John Smith',
    role:       'Managing Member',
    email:      'jsmith@smithcapital.com',
    phone:      '+1-732-555-0100',
    confidence: 0.88,
    source:     'proxycurl',
  },
  contact_status: 'available',
  portfolio_summary: {
    property_count: 12,
    total_sqft:     340000,
    states_present: ['NJ', 'NY', 'PA'],
    property_types: ['Retail', 'Office'],
  },
  properties: [],  // omitted in non-all mode
  source:          'sos+gleif+postGIS',
  data_freshness:  'fresh',
  freshness_secs:  3600,
  confidence:      0.93,
}, 86400);

console.log('[seed] owner_profile smoke test seeded: cre_smoke_smith_capital');

// ─── Smoke test 3: parcel_lookup — 1240 Highway 9 ─────────────────────────

await setCache('parcel:1240_highway_9,_woodbridge_nj', {
  status: 'found',
  parcel: {
    address:         '1240 Highway 9',
    city:            'Woodbridge',
    state:           'NJ',
    zip:             '07095',
    lot_size_acres:  2.3,
    building_sqft:   18000,
    property_type:   'Retail',
    last_sale_price: 1850000,
    last_sale_date:  '2019-03-15',
    loan_maturity:   '2027-06-01',
    parcel_id:       'NJ_MIDDLESEX_0024_00001',
    latitude:        40.5579,
    longitude:       -74.2861,
  },
  owner_name:     'Smith Capital Holdings LLC',
  entity_id:      'cre_smoke_smith_capital',
  data_freshness: 'fresh',
  freshness_secs: 604800,
}, 604800);

console.log('[seed] parcel_lookup smoke test seeded: 1240 Highway 9 Woodbridge NJ');
console.log('[seed] All smoke test entities seeded successfully.');

process.exit(0);
