# CRE Investor Discovery MCP Server

Delivers the core Reonomy corridor-filter workflow at $0.10/query.

## Prerequisites

- [Node.js 22+](https://nodejs.org/)
- [Docker Desktop](https://www.docker.com/products/docker-desktop/) — provides PostgreSQL + PostGIS + Redis locally
- [Python 3.8+](https://www.python.org/downloads/) — required for parcel data download

## Quick Start (local)

```bash
# 1. Install dependencies
npm install

# 2. Configure environment
cp .env.example .env        # fill in your API keys; Docker credentials are pre-filled

# 3. Start PostgreSQL + Redis via Docker
docker compose up -d

# 4. Initialize the schema
npm run init-db

# 5. Download parcel data (Overture Maps, ~430 MB for NJ+NY, requires Python)
npm run download-parcels -- nj,ny

# 6. Load parcels into the database (~18M rows, takes 5-10 min)
npm run load-parcels -- nj,ny

# 7. Start the server
npm run dev                  # development with hot reload
npm run build && npm start   # production
```

**Available state codes:** `nj ny fl tx ca il pa ga co wa az ma nc oh mi`

> **On a hosting server:** Run steps 5 and 6 on the server directly — avoids transferring large files and sidesteps local disk constraints.

## Environment Variables

See `.env.example` for all required variables.

## Tools

| Tool | Description | Price |
|------|-------------|-------|
| `investor_discovery` | Corridor + lot-size filter → owner contacts | $0.10 |
| `owner_profile` | Full entity profile + nationwide portfolio | $0.05 |
| `parcel_lookup` | Single address → parcel data + owner link | $0.02 |

## Architecture

Four pre-ingested layers — nothing calls upstream at query time:

1. **Parcels** — PostGIS / OpenAddresses / County GIS / ATTOM (weekly refresh)
2. **Entity Resolution** — SOS bulk CSV files + GLEIF API (free) / RapidFuzz (daily refresh)
3. **Portfolio Graph** — PostgreSQL edge table, daily recompute
4. **Contact Enrichment** — Proxycurl + Hunter.io, Redis-cached 30 days

## Latency

| Layer | p95 (cached) | Worst case (cold) |
|-------|-------------|-------------------|
| Corridor discovery | ~1.3s | < 8s |
| Owner profile | ~250ms | < 3s |
| Parcel lookup | ~200ms | < 2s |

## Data Sources

All four layers are fully implemented in `src/ingest/`:

- **Layer 1:** PostGIS spatial query against ingested OpenAddresses + ATTOM data (`resolvers.ts`)
- **Layer 2:** SOS bulk CSV ingest (`jobs.ts` reads `./data/sos/{state}_entities.csv`) + GLEIF REST API (free, no key required) + RapidFuzz `token_set_ratio`
- **Layer 3:** PostgreSQL portfolio edge table — daily recompute via `jobs.ts`
- **Layer 4:** Proxycurl `GET /proxycurl/api/linkedin/company` → Hunter.io fallback, async on cache miss

## Deployment

1. Provision PostgreSQL + PostGIS for parcel/entity/portfolio data
2. Provision Redis for cache layer
3. Set all `.env` variables (see `.env.example`)
4. Run `npm run init-db` to apply the schema
5. Download state SOS bulk CSV files into `./data/sos/` — one file per state named `{state}_entities.csv`
   Required columns: `filing_number,name,entity_type,status,incorporated_date,registered_agent_name,registered_agent_address,filing_url`
   Most state SOS offices publish free bulk exports; start with your target markets (FL, NJ, TX, CA, NY)
6. Run `npm run seed` to pre-seed smoke test cache entries
7. Deploy behind HTTPS
8. Update `endpoint` in `marketplace-listing.json` to your production URL
9. Uncomment `createContextMiddleware()` in `src/index.ts`
10. Submit via Context Protocol contribution form

> **Note:** `npm run seed` only pre-populates the Redis cache with smoke test responses. Real query results depend on parcel and entity data loaded via the background ingest jobs.
