CRE Investor Discovery
Context Protocol · Tier S · Query Mode · $0.10/response · Unbundles: Reonomy ($4K–$12K/yr) + CoStar ($7K–$20K/yr)

What This Is
CRE brokers pay Reonomy $4K–$12K/year for one workflow: filter commercial properties by corridor and lot size, then pull verified owner contacts in one step. Since Altus Group acquired Reonomy in November 2021, renewal prices have nearly doubled and users are actively canceling. This tool delivers that exact workflow for $0.10/query. CoStar tenant intelligence, full comps, and listings are NOT replicated.

Exact Data Payload
Input: { corridor: "Highway 9, NJ", property_type: "Retail", lot_size_min_acres: 1 }

Response per owner:
{ "owner_name": "Smith Capital Holdings LLC", "entity_resolution_confidence": 0.93,
  "portfolio_summary": { "property_count": 12, "total_sqft": 340000 },
  "contact": { "name": "John Smith", "role": "Managing Member",
    "email": "jsmith@smithcapital.com", "confidence": 0.88 },
  "properties_in_corridor": [{ "address": "1240 Highway 9, Woodbridge NJ",
    "lot_size_acres": 2.3, "last_sale_price": 1850000, "loan_maturity": "2027-06-01" }] }

Failure states (all return structured JSON): no match → status: empty · no contact → contact_status: not_available · ambiguous entity → resolution_required: true · timeout → status: partial, fallback: cached

Architecture — Four-Layer Pre-Ingested Pipeline (no upstream calls at query time)
•Layer 1 — Parcels: OpenAddresses (free, 600M+ records) + State/County GIS bulk downloads (free, all 50 states) + USGS National Map (free) + ATTOM one-time bulk ingest. Pre-loaded into PostGIS. Weekly refresh.
•Layer 2 — Entity Resolution: 50-state Secretary of State bulk CSV filings + GLEIF REST API (free, no key) + RapidFuzz (token_set_ratio > 90). Canonical name → corporate registry ID. Nested LLC→Trust→Individual resolved via graph traversal. 85%+ precision target, validated on 50-parcel spot-check pre-launch.
•Layer 3 — Portfolio Graph: PostgreSQL edge table (owner_entity_id → parcel IDs nationwide). Daily recompute. No live computation at query time.
•Layer 4 — Contact Enrichment: Proxycurl primary (~$0.02/record) + Hunter.io fallback (~$0.01/email). Redis-cached 30 days per entity. Async worker populates on cache miss.

Latency · Caching · Unit Economics

Layer	Latency	Cache	Refresh
Parcel spatial lookup	200ms	PostGIS	Monthly
Property filter	300ms	PostgreSQL	Weekly
Entity resolution	250ms	PostgreSQL	Weekly
Portfolio graph	400ms	PostgreSQL	Daily
Contact enrichment	200ms	Redis	30 days
Query result	—	Redis	24 hours
p95 total (cached)	~1.3s	—	—
Worst case (miss)	< 8s	—	—

Cost Component	Per Query
Parcel data (free govt sources + ATTOM amortized)	$0.004
Contact enrichment — Proxycurl (70% cache hit)	$0.004
Compute + DB + Redis + Storage	$0.010
Total · Margin at $0.10	~$0.018 · 82%

Evidence of Demand
“What I liked about Reonomy was being able to filter certain corridors and search by lot size and immediately pull the owner’s contact info.” — named CRE broker, evaluating whether to reactivate subscription
•@jackberdan — May 1, 2024

“I pull a data list using Reonomy. I load that list directly into Mojo Dialer. I dial three owners at the same time.” — active production use
•@HenryEisenstein — March 1, 2026

“They nearly doubled my annual subscription upon renewal... I called to cancel and they denied a refund.” — post-acquisition price-doubling thread
•r/CommercialRealEstate — "Did Reonomy jack up its rates? We terminated our relationship..."

“I used to have Reonomy. It’s just too expensive.”
•@Karina_inCRE — Feb 20, 2026

Why This Gets Funded
This payload cannot be replicated with free tools. It requires parcel normalization across 3,000+ counties, LLC-to-owner entity resolution across 50-state corporate registries, cross-jurisdiction portfolio linking, and B2B contact enrichment — capabilities that exist only inside Reonomy and CoStar today. At $0.018/query and 82% margin, it is sustainably profitable at $0.10 pricing and delivers the exact workflow a named broker described word-for-word in a public post.