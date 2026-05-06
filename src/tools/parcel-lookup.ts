// src/tools/parcel-lookup.ts
import type { McpServer } from '@modelcontextprotocol/sdk/server/mcp.js';
import {
  ParcelLookupInput,
  ParcelLookupOutput,
  parcelLookupInputShape,
} from '../schemas/cre.js';
import { getCached, setCache } from '../cache/helpers.js';
import { resolveParcel } from '../ingest/resolvers.js';
import { structuredError } from '../errors/codes.js';

export function registerParcelLookup(server: McpServer): void {
  server.registerTool(
    'parcel_lookup',
    {
      title:       'CRE Parcel Lookup',
      description: 'Look up a single commercial property by street address. Returns parcel data, lot size, last sale, loan maturity, and the linked owner entity_id for use with owner_profile.',
      inputSchema:  parcelLookupInputShape,
      outputSchema: ParcelLookupOutput,
      _meta: {
        surface:       'both',
        queryEligible: true,
        latencyClass:  'instant',
        pricing:       { executeUsd: '0.02' },
        rateLimit: {
          maxRequestsPerMinute: 200,
          cooldownMs:           300,
          maxConcurrency:       30,
        },
        dataBroker: {
          deterministic: true,
          auditFields:   ['data_freshness', 'freshness_secs'],
        },
      },
    },
    async (args) => {
      let parsed;
      try {
        parsed = ParcelLookupInput.parse(args);
      } catch (err) {
        return structuredError('SCHEMA_VALIDATION_FAIL', `Invalid input: ${String(err)}`);
      }

      const normalised = parsed.address.toLowerCase().trim();
      const cacheKey   = `parcel:${normalised.replace(/\s+/g, '_')}`;

      const cached = await getCached<Record<string, unknown>>(cacheKey);
      if (cached) {
        return {
          content:           [{ type: 'text', text: JSON.stringify(cached) }],
          structuredContent: cached,
        };
      }

      let result;
      try {
        result = await resolveParcel(parsed.address);
      } catch (err) {
        return structuredError('UPSTREAM_UNAVAILABLE', `Parcel resolution error: ${String(err)}`);
      }

      if (result.status === 'ambiguous') {
        return structuredError(
          'ENTITY_AMBIGUOUS',
          `Address matches multiple parcels: ${parsed.address}`,
          { resolution_required: true, status: 'ambiguous' }
        );
      }

      // Weekly TTL — parcel data refreshes weekly per proposal
      await setCache(cacheKey, result, 604800);

      const payload = result as unknown as Record<string, unknown>;
      return {
        content:           [{ type: 'text', text: JSON.stringify(result) }],
        structuredContent: payload,
      };
    }
  );
}
