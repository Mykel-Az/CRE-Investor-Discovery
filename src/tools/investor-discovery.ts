// src/tools/investor-discovery.ts
import type { McpServer } from '@modelcontextprotocol/sdk/server/mcp.js';
import {
  InvestorDiscoveryInput,
  InvestorDiscoveryOutput,
  investorDiscoveryInputShape,
} from '../schemas/cre.js';
import { getCached, setCache } from '../cache/helpers.js';
import { resolveCorridorOwners } from '../ingest/resolvers.js';
import { structuredError } from '../errors/codes.js';

export function registerInvestorDiscovery(server: McpServer): void {
  server.registerTool(
    'investor_discovery',
    {
      title:       'CRE Investor Discovery',
      description: 'Filter commercial properties along US road corridor by property type and lot size, then return verified owner contacts in one call. Replaces the core Reonomy corridor-filter workflow at $0.10/query.',
      inputSchema:  investorDiscoveryInputShape,
      outputSchema: InvestorDiscoveryOutput,
      _meta: {
        surface:       'both',
        queryEligible: true,
        latencyClass:  'fast',
        pricing:       { executeUsd: '0.10' },
        rateLimit: {
          maxRequestsPerMinute: 60,
          cooldownMs:           1000,
          maxConcurrency:       10,
        },
        dataBroker: {
          deterministic: true,
          auditFields:   ['data_freshness', 'freshness_secs', 'entity_resolution_confidence'],
        },
      },
    },
    async (args) => {
      let parsed;
      try {
        parsed = InvestorDiscoveryInput.parse(args);
      } catch (err) {
        return structuredError('SCHEMA_VALIDATION_FAIL', `Invalid input: ${String(err)}`);
      }

      const cacheKey = [
        'discovery',
        parsed.corridor.toLowerCase().replace(/\s+/g, '_'),
        parsed.property_type.toLowerCase(),
        parsed.lot_size_min_acres ?? 0,
        parsed.lot_size_max_acres ?? 'any',
        parsed.max_results,
      ].join(':');

      const cached = await getCached<Record<string, unknown>>(cacheKey);
      if (cached) {
        return {
          content:           [{ type: 'text', text: JSON.stringify(cached) }],
          structuredContent: cached,
        };
      }

      let result;
      try {
        result = await resolveCorridorOwners({
          corridor:           parsed.corridor,
          property_type:      parsed.property_type,
          lot_size_min_acres: parsed.lot_size_min_acres ?? 0,
          lot_size_max_acres: parsed.lot_size_max_acres,
          max_results:        parsed.max_results ?? 10,
        });
      } catch (err) {
        return structuredError('UPSTREAM_UNAVAILABLE', `Pipeline error: ${String(err)}`, {
          fallback: true,
          status:   'partial',
        });
      }

      // Cache 24 hours (proposal spec)
      await setCache(cacheKey, result, 86400);

      const payload = result as unknown as Record<string, unknown>;
      return {
        content:           [{ type: 'text', text: JSON.stringify(result) }],
        structuredContent: payload,
      };
    }
  );
}
