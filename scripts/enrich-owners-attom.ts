// scripts/enrich-owners-attom.ts
//
// Enriches parcels with owner names, property type, lot size, and sale data
// from the ATTOM property API. Run after load-parcels to populate owner_name_raw
// and other fields that Overture Maps does not provide.
//
// Usage:
//   node --env-file=.env --import tsx scripts/enrich-owners-attom.ts
//   node --env-file=.env --import tsx scripts/enrich-owners-attom.ts -- nj 500
//
// Arguments:
//   arg1 — comma-separated state codes to target (default: all states with NULL owners)
//   arg2 — max parcels to enrich per run (default: 500)
//
// Costs ~$0.02/record via ATTOM. Start small to validate before bulk enrichment.

import pg from 'pg';

const ATTOM_KEY = process.env.ATTOM_API_KEY;
const DB_URL    = process.env.DATABASE_URL ?? 'postgresql://cre:cre@localhost:5432/cre_investor';
const BASE_URL  = 'https://api.gateway.attomdata.com/propertyapi/v1.0.0';

if (!ATTOM_KEY) {
  console.error('[attom] ATTOM_API_KEY is not set in .env — aborting.');
  process.exit(1);
}

const pool = new pg.Pool({ connectionString: DB_URL, max: 3 });

const argState = process.argv[2]?.toLowerCase().split(',').map(s => s.trim()).filter(Boolean);
const argLimit = parseInt(process.argv[3] ?? '500', 10);

// ─── ATTOM response types ─────────────────────────────────────────────────────

interface AttomOwner {
  owner1?: { fullname?: string; lastname?: string; firstname?: string };
  corporateindicator?: string;
}

interface AttomProperty {
  summary?:    { proptype?: string; propSubType?: string };
  lot?:        { lotsize1?: number; lotsize2?: number };
  building?:   { size?: { bldgsize?: number; grosssize?: number } };
  sale?:       { saleRecDate?: string; salesearchdate?: string; saleamt?: number };
  owner?:      AttomOwner;
}

interface AttomResponse {
  property?: AttomProperty[];
  status?:   { code?: number; msg?: string };
}

// ─── Helpers ──────────────────────────────────────────────────────────────────

function mapPropertyType(proptype?: string, subtype?: string): string {
  const t = `${proptype ?? ''} ${subtype ?? ''}`.toUpperCase().trim();
  if (t.includes('RETAIL') || t.includes('STORE') || t.includes('SHOPPING')) return 'Retail';
  if (t.includes('OFFICE'))                                                    return 'Office';
  if (t.includes('INDUSTRIAL') || t.includes('WAREHOUSE') || t.includes('FLEX')) return 'Industrial';
  if (t.includes('MULTIFAMILY') || t.includes('APARTMENT'))                   return 'Multifamily';
  if (t.includes('MIXED'))                                                     return 'Mixed-Use';
  if (t.includes('LAND') || t.includes('VACANT') || t.includes('LOT'))        return 'Land';
  if (t.includes('HOTEL') || t.includes('MOTEL') || t.includes('HOSPITALITY')) return 'Hospitality';
  if (t.includes('COMMERCIAL'))                                                return 'Retail';
  return 'Unknown';
}

function extractOwnerName(owner?: AttomOwner): string | null {
  if (!owner) return null;
  const o = owner.owner1;
  if (!o) return null;
  if (o.fullname?.trim()) return o.fullname.trim();
  const parts = [o.firstname, o.lastname].filter(Boolean).join(' ').trim();
  return parts || null;
}

function lotSizeAcres(lot?: AttomProperty['lot']): number | null {
  if (!lot) return null;
  // ATTOM lotsize1 is typically in sqft; lotsize2 sometimes in acres
  if (lot.lotsize2 && lot.lotsize2 < 10_000) return lot.lotsize2; // already acres
  if (lot.lotsize1 && lot.lotsize1 > 0)      return lot.lotsize1 / 43_560;
  return null;
}

async function fetchAttomProperty(address: string, city: string, state: string, zip: string): Promise<AttomProperty | null> {
  const address2 = [city, state, zip].filter(Boolean).join(' ');
  const url = `${BASE_URL}/property/detail?address1=${encodeURIComponent(address)}&address2=${encodeURIComponent(address2)}`;

  const res = await fetch(url, {
    headers: { apikey: ATTOM_KEY!, Accept: 'application/json' },
  });

  if (res.status === 404 || res.status === 400) return null;
  if (!res.ok) throw new Error(`ATTOM HTTP ${res.status}`);

  const data = await res.json() as AttomResponse;
  return data.property?.[0] ?? null;
}

// ─── Main ─────────────────────────────────────────────────────────────────────

console.log('[attom] Owner enrichment starting...');
console.log(`[attom] Target states: ${argState?.join(', ').toUpperCase() ?? 'all'}`);
console.log(`[attom] Limit: ${argLimit} parcels\n`);

const stateFilter = argState?.length
  ? `AND UPPER(p.state) = ANY(ARRAY[${argState.map(s => `'${s.toUpperCase()}'`).join(',')}])`
  : '';

const client = await pool.connect();
let parcels: Array<{ parcel_id: string; address: string; city: string; state: string; zip: string }>;

try {
  const result = await client.query(
    `SELECT parcel_id, address, city, state, zip
     FROM parcels
     WHERE owner_name_raw IS NULL
       AND address IS NOT NULL
       ${stateFilter}
     ORDER BY ingested_at DESC
     LIMIT $1`,
    [argLimit]
  );
  parcels = result.rows as typeof parcels;
} finally {
  client.release();
}

console.log(`[attom] Found ${parcels.length} parcels to enrich.\n`);

let enriched = 0;
let notFound = 0;
let errors   = 0;

for (let i = 0; i < parcels.length; i++) {
  const p = parcels[i];

  try {
    const prop = await fetchAttomProperty(p.address, p.city, p.state, p.zip);

    if (!prop) {
      notFound++;
    } else {
      const ownerName   = extractOwnerName(prop.owner);
      const propType    = mapPropertyType(prop.summary?.proptype, prop.summary?.propSubType);
      const lotAcres    = lotSizeAcres(prop.lot);
      const bldgSqft    = prop.building?.size?.bldgsize ?? prop.building?.size?.grosssize ?? null;
      const saleDate    = prop.sale?.saleRecDate ?? prop.sale?.salesearchdate ?? null;
      const salePrice   = prop.sale?.saleamt ?? null;

      const updateClient = await pool.connect();
      try {
        await updateClient.query(
          `UPDATE parcels SET
             owner_name_raw  = COALESCE($2, owner_name_raw),
             property_type   = CASE WHEN $3 != 'Unknown' THEN $3 ELSE property_type END,
             lot_size_acres  = COALESCE($4, lot_size_acres),
             building_sqft   = COALESCE($5, building_sqft),
             last_sale_date  = COALESCE($6::date, last_sale_date),
             last_sale_price = COALESCE($7, last_sale_price),
             updated_at      = NOW()
           WHERE parcel_id = $1`,
          [p.parcel_id, ownerName, propType, lotAcres, bldgSqft, saleDate, salePrice]
        );
        if (ownerName) enriched++;
      } finally {
        updateClient.release();
      }
    }
  } catch (err) {
    errors++;
    if (errors <= 5) console.error(`[attom] Error on ${p.parcel_id}:`, (err as Error).message);
  }

  if ((i + 1) % 50 === 0) {
    process.stdout.write(`  ${i + 1}/${parcels.length} processed — ${enriched} enriched, ${notFound} not found, ${errors} errors\r`);
  }

  // ATTOM rate limit: ~3 req/sec on standard plans
  await new Promise(r => setTimeout(r, 350));
}

console.log(`\n[attom] Done.`);
console.log(`  Enriched with owner:  ${enriched}`);
console.log(`  Not found in ATTOM:   ${notFound}`);
console.log(`  Errors:               ${errors}`);
console.log('\n[attom] Next step: restart the server — the nightly entity resolution job');
console.log('         will match owner_name_raw values to entities at 3 AM,');
console.log('         or trigger it manually by restarting and waiting for the cron.');

await pool.end();
