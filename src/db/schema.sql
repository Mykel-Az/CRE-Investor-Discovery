-- CRE Investor Discovery — PostGIS Database Schema
-- Run once against a PostgreSQL 14+ database with PostGIS extension enabled.
--
-- Prerequisites:
--   CREATE DATABASE cre_investor;
--   \c cre_investor
--   CREATE EXTENSION IF NOT EXISTS postgis;

-- ═══════════════════════════════════════════════════════════════════════════
-- Layer 1 — Parcels
-- Source: OpenAddresses bulk + State/County GIS + ATTOM one-time ingest
-- ═══════════════════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS parcels (
    parcel_id       TEXT PRIMARY KEY,
    address         TEXT NOT NULL,
    city            TEXT NOT NULL,
    state           TEXT NOT NULL,
    zip             TEXT NOT NULL,
    lot_size_acres  DOUBLE PRECISION,
    building_sqft   DOUBLE PRECISION,
    property_type   TEXT NOT NULL DEFAULT 'Unknown',
    last_sale_price DOUBLE PRECISION,
    last_sale_date  DATE,
    loan_maturity   DATE,
    owner_name_raw  TEXT,                           -- Raw name from deed/tax record
    geom            GEOGRAPHY(Point, 4326),         -- WGS84 lat/lng
    source          TEXT NOT NULL DEFAULT 'openaddresses',
    ingested_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_parcels_geom        ON parcels USING GIST (geom);
CREATE INDEX IF NOT EXISTS idx_parcels_state        ON parcels (state);
CREATE INDEX IF NOT EXISTS idx_parcels_property_type ON parcels (property_type);
CREATE INDEX IF NOT EXISTS idx_parcels_lot_size     ON parcels (lot_size_acres);
CREATE INDEX IF NOT EXISTS idx_parcels_owner_raw    ON parcels (owner_name_raw);
CREATE INDEX IF NOT EXISTS idx_parcels_address      ON parcels USING GIN (to_tsvector('english', address));

-- ═══════════════════════════════════════════════════════════════════════════
-- Parcels staging — landing zone for weekly ETL before upsert into parcels
-- Truncated and reloaded by external ingest scripts; no constraints needed
-- ═══════════════════════════════════════════════════════════════════════════

CREATE UNLOGGED TABLE IF NOT EXISTS parcels_staging (
    parcel_id       TEXT,
    address         TEXT,
    city            TEXT,
    state           TEXT,
    zip             TEXT,
    lot_size_acres  DOUBLE PRECISION,
    building_sqft   DOUBLE PRECISION,
    property_type   TEXT,
    last_sale_price DOUBLE PRECISION,
    last_sale_date  DATE,
    loan_maturity   DATE,
    owner_name_raw  TEXT,
    geom            GEOGRAPHY(Point, 4326),
    source          TEXT
);

-- ═══════════════════════════════════════════════════════════════════════════
-- Roads — corridor geometries for ST_DWithin spatial queries
-- Source: USGS National Map / OpenStreetMap road network extracts
-- ═══════════════════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS roads (
    road_id     SERIAL PRIMARY KEY,
    road_name   TEXT NOT NULL,
    state       TEXT NOT NULL,
    geom        GEOGRAPHY(LineString, 4326),
    source      TEXT NOT NULL DEFAULT 'osm',
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_roads_geom ON roads USING GIST (geom);
CREATE INDEX IF NOT EXISTS idx_roads_name ON roads USING GIN (to_tsvector('english', road_name));

-- ═══════════════════════════════════════════════════════════════════════════
-- Layer 2 — Entities (canonical names from SOS bulk filings + GLEIF)
-- ═══════════════════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS entities (
    entity_id        TEXT PRIMARY KEY,               -- e.g. 'sos_nj_0100101010' or 'gleif_5493001KJTIIGC8Y1R12'
    canonical_name   TEXT NOT NULL,
    entity_type      TEXT NOT NULL DEFAULT 'Unknown', -- LLC, Trust, Corporation, Individual, Unknown
    jurisdiction     TEXT,                            -- e.g. 'US-NJ'
    status           TEXT DEFAULT 'unknown',          -- active, dissolved, suspended, unknown
    incorporated_at  DATE,
    registered_agent_name    TEXT,
    registered_agent_address TEXT,
    lei              TEXT,                            -- GLEIF Legal Entity Identifier (if known)
    sos_filing_url   TEXT,                            -- State SOS filing page URL
    source           TEXT NOT NULL DEFAULT 'sos',    -- sos, gleif
    created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_entities_name   ON entities USING GIN (to_tsvector('english', canonical_name));
CREATE INDEX IF NOT EXISTS idx_entities_type   ON entities (entity_type);
CREATE INDEX IF NOT EXISTS idx_entities_status ON entities (status);

-- ═══════════════════════════════════════════════════════════════════════════
-- Entity Aliases — fuzzy-matched alternate names for entity resolution
-- ═══════════════════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS entity_aliases (
    alias_id   SERIAL PRIMARY KEY,
    entity_id  TEXT NOT NULL REFERENCES entities(entity_id) ON DELETE CASCADE,
    alias_name TEXT NOT NULL,
    match_score DOUBLE PRECISION,                    -- fuzzball token_set_ratio score
    source     TEXT NOT NULL DEFAULT 'rapidfuzz',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_aliases_entity ON entity_aliases (entity_id);
CREATE INDEX IF NOT EXISTS idx_aliases_name   ON entity_aliases USING GIN (to_tsvector('english', alias_name));

-- ═══════════════════════════════════════════════════════════════════════════
-- Beneficial Owners — LLC→Trust→Individual ownership chain
-- ═══════════════════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS beneficial_owners (
    id             SERIAL PRIMARY KEY,
    entity_id      TEXT NOT NULL REFERENCES entities(entity_id) ON DELETE CASCADE,
    owner_name     TEXT NOT NULL,
    role           TEXT NOT NULL DEFAULT 'Member',
    ownership_pct  DOUBLE PRECISION,
    -- Self-referencing for nested resolution (LLC owns LLC)
    parent_entity_id TEXT REFERENCES entities(entity_id) ON DELETE SET NULL,
    source         TEXT NOT NULL DEFAULT 'sos_filing',
    created_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_beneficial_entity ON beneficial_owners (entity_id);
CREATE INDEX IF NOT EXISTS idx_beneficial_parent ON beneficial_owners (parent_entity_id);

-- ═══════════════════════════════════════════════════════════════════════════
-- Layer 3 — Portfolio Edges (owner_entity_id → parcel_id)
-- Recomputed daily by background job
-- ═══════════════════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS portfolio_edges (
    entity_id  TEXT NOT NULL REFERENCES entities(entity_id) ON DELETE CASCADE,
    parcel_id  TEXT NOT NULL REFERENCES parcels(parcel_id) ON DELETE CASCADE,
    confidence DOUBLE PRECISION NOT NULL DEFAULT 1.0,
    method     TEXT NOT NULL DEFAULT 'exact',          -- exact, fuzzy, graph
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (entity_id, parcel_id)
);

CREATE INDEX IF NOT EXISTS idx_portfolio_entity ON portfolio_edges (entity_id);
CREATE INDEX IF NOT EXISTS idx_portfolio_parcel ON portfolio_edges (parcel_id);

-- ═══════════════════════════════════════════════════════════════════════════
-- Layer 4 — Contacts (Proxycurl primary / Hunter.io fallback)
-- ═══════════════════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS contacts (
    entity_id   TEXT PRIMARY KEY REFERENCES entities(entity_id) ON DELETE CASCADE,
    name        TEXT NOT NULL,
    role        TEXT,
    email       TEXT,
    phone       TEXT,
    confidence  DOUBLE PRECISION NOT NULL DEFAULT 0,
    source      TEXT NOT NULL DEFAULT 'proxycurl',     -- proxycurl, hunter
    enriched_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    expires_at  TIMESTAMPTZ NOT NULL DEFAULT (NOW() + INTERVAL '30 days')
);

-- ═══════════════════════════════════════════════════════════════════════════
-- Query Log — tracks popular entities for contact pre-warming
-- ═══════════════════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS query_log (
    id          SERIAL PRIMARY KEY,
    tool_name   TEXT NOT NULL,
    entity_id   TEXT,
    corridor    TEXT,
    queried_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_query_log_entity ON query_log (entity_id);
CREATE INDEX IF NOT EXISTS idx_query_log_time   ON query_log (queried_at);
