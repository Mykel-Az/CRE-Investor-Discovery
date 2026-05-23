// scripts/smoke-test.ts
import { resolveCorridorOwners } from '../src/ingest/resolvers.js';

console.log('Running investor_discovery smoke test...\n');

const result = await resolveCorridorOwners({
  corridor:           'Broadway, NY',
  property_type:      'Retail',
  lot_size_min_acres: 0,
  max_results:        5,
});

console.log('Status          :', result.status);
console.log('Matched parcels :', result.query_summary.matched_parcels);
console.log('Unique owners   :', result.query_summary.unique_owners);
console.log('');

for (const owner of result.owners) {
  console.log('Owner           :', owner.owner_name);
  console.log('Entity type     :', owner.entity_type);
  console.log('Resolution conf :', owner.entity_resolution_confidence);
  console.log('Properties here :', owner.properties_in_corridor.length);
  console.log('Portfolio total :', owner.portfolio_summary.property_count);
  console.log('Contact status  :', owner.contact_status);
  console.log('---');
}

process.exit(0);
