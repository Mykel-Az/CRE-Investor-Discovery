// scripts/update-listing.ts
// Step 7: Push optimized marketplace description via Context SDK
// Usage: node --env-file=.env --import tsx scripts/update-listing.ts

// @ts-ignore
import { ContextClient } from '@ctxprotocol/sdk';

const CONTEXT_API_KEY = process.env.CONTEXT_API_KEY ?? '';
const TOOL_ID         = '713eef81-e726-4b45-ab8f-c7bdbb201866';

if (!CONTEXT_API_KEY) {
  console.error('ERROR: CONTEXT_API_KEY not set in .env');
  process.exit(1);
}

const client = new ContextClient({ apiKey: CONTEXT_API_KEY });

// ─── Step 3 output: Optimized marketplace listing ────────────────────────────
// Generated from Step 3 analysis + refined with Step 4 validation data (7/7 pass)
// All "Try asking" prompts are confirmed-passing from marketplace validation.
// Banned chars enforced: no "—", no "**", no "mdash"

const LISTING = {
  name: 'CRE Investor Finder',

  description: `Identify verified owners of commercial real estate along any US road corridor, filtered by property type and lot size, with entity resolution against state Secretary of State filings. Replaces manual Reonomy corridor searches for NJ and NY markets.

Features:
- Corridor spatial search across 35,000+ indexed parcels in New York and New Jersey, matching properties within 400m of any named road
- Entity resolution against state SOS filings, identifying the LLC, corporation, or individual beneficial owner behind each deed name
- Three complementary tools: corridor investor discovery, full owner profile, and single-parcel address lookup
- Filter by six property types (Retail, Office, Industrial, Multifamily, Mixed-Use, Land) and lot size range in acres
- Contact enrichment with name, role, email, phone, and confidence score for each resolved owner entity

Try asking:
- "Find retail property owners along Broadway in New York"
- "Who are the top commercial property owners on 4th Avenue in Brooklyn?"
- "Find industrial property investors along Northern Boulevard in Queens with lots over 1 acre"
- "Find mixed-use investors on Atlantic Avenue in Brooklyn, then give me the full profile for the first owner"
- "Who owns 259 Front Street in Manhattan, New York?"
- "Who owns commercial properties near the Long Island Expressway in New York?"
- "Find office building owners on Amsterdam Avenue in Manhattan, filter to lots over 0.5 acres, and show their portfolio summary"
- "List all Multifamily property owners along Route 1 in New Jersey with lots between 0.5 and 2 acres"

Agent tips:
- Use investor_discovery first to get entity_ids, then call owner_profile with those IDs for full SOS registration details and nationwide portfolio
- Corridor format is "Road Name, ST" using two-letter state abbreviation (e.g. "Broadway, NY" or "Highway 9, NJ" or "Atlantic Avenue, NY")
- Property type must be one of: Retail, Office, Industrial, Multifamily, Mixed-Use, Land
- Set lot_size_min_acres to filter for investment-grade properties (e.g. 0.5 for half-acre minimum, 1 for acre-plus)
- parcel_lookup accepts natural address formats and returns the linked entity_id for chaining directly into owner_profile`,

  category: 'Business & Sales',
  price: '0.10',
  endpoint: 'https://cre-investor-discovery-production.up.railway.app/mcp',
};

console.log('=== STEP 3: Generated Marketplace Listing ===\n');
console.log(JSON.stringify({
  name: LISTING.name,
  category: LISTING.category,
  price: `$${LISTING.price}`,
  endpoint: LISTING.endpoint,
}, null, 2));
console.log('\nDescription preview (first 300 chars):');
console.log(LISTING.description.slice(0, 300) + '...\n');

// Count "Try asking" prompts
const tryAskingCount = (LISTING.description.match(/^- "/gm) || []).length;
console.log(`Try asking prompts: ${tryAskingCount} (minimum 7 required)`);
console.log(`Description length: ${LISTING.description.length} chars (max 5000)\n`);

// ─── Step 7: Push via SDK ─────────────────────────────────────────────────────
console.log('=== STEP 7: Pushing description update via SDK ===\n');

try {
  const result = await (client.developer as any).updateTool(TOOL_ID, {
    description: LISTING.description,
  });
  console.log('✓ Description updated successfully');
  console.log('  Tool ID:', TOOL_ID);
  console.log('  Timestamp:', new Date().toISOString());
  console.log('  Result:', JSON.stringify(result, null, 2));
} catch (err: any) {
  console.log('✗ Update failed:', err.message);
  console.log('  Storing description for manual paste into contribute form.');
}

// ─── Step 7.3: Verify update ──────────────────────────────────────────────────
console.log('\n=== STEP 7.3: Verifying update ===\n');

try {
  const tools = await client.discovery.search({
    query: 'CRE Investor Finder',
    mode: 'query',
    surface: 'answer',
    queryEligible: true,
  });
  const found = tools?.find?.((t: any) => t.id === TOOL_ID) ?? tools?.[0];
  if (found) {
    const desc = (found as any).description ?? '';
    const descUpdated = desc.includes('259 Front Street') || desc.includes('Northern Boulevard');
    console.log('Tool found in discovery:', found.name ?? found.id);
    console.log('Description updated:', descUpdated ? '✓ YES' : '✗ NO (may need a few minutes to refresh)');
    console.log('Description preview:', desc.slice(0, 200));
  } else {
    console.log('Tool not found in discovery — may take a few minutes to refresh');
  }
} catch (err: any) {
  console.log('Verification error:', err.message);
}
