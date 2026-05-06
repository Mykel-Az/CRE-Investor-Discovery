// src/tools/owner-profile.ts
import type { McpServer } from '@modelcontextprotocol/sdk/server/mcp.js';
import {
  OwnerProfileInput,
  OwnerProfileOutput,
  ownerProfileInputShape,
} from '../schemas/cre.js';
import { getCached, setCache } from '../cache/helpers.js';
import { resolveOwnerProfile } from '../ingest/resolvers.js';
import { structuredError } from '../errors/codes.js';

export function registerOwnerProfile(server: McpServer): void {
  server.registerTool(
    'owner_profile',
    {
      title:       'CRE Owner Profile',
      description: 'Retrieve the full profile for a CRE owner entity: registration status, beneficial owners, nationwide portfolio, and verified contact. Use entity_id from investor_discovery.',
      inputSchema:  ownerProfileInputShape,
      outputSchema: OwnerProfileOutput,
      _meta: {
        surface:       'both',
        queryEligible: true,
        latencyClass:  'instant',
        pricing:       { executeUsd: '0.05' },
        rateLimit: {
          maxRequestsPerMinute: 120,
          cooldownMs:           500,
          maxConcurrency:       20,
        },
        dataBroker: {
          deterministic: true,
          auditFields:   ['source', 'freshness_secs', 'confidence', 'data_freshness'],
        },
      },
    },
    async (args) => {
      let parsed;
      try {
        parsed = OwnerProfileInput.parse(args);
      } catch (err) {
        return structuredError('SCHEMA_VALIDATION_FAIL', `Invalid input: ${String(err)}`);
      }

      const cacheKey = `owner:${parsed.entity_id}:all=${parsed.include_all_properties}`;

      const cached = await getCached<Record<string, unknown>>(cacheKey);
      if (cached) {
        return {
          content:           [{ type: 'text', text: JSON.stringify(cached) }],
          structuredContent: cached,
        };
      }

      let result;
      try {
        result = await resolveOwnerProfile({
          entity_id:              parsed.entity_id,
          include_all_properties: parsed.include_all_properties ?? false,
        });
      } catch (err) {
        return structuredError('UPSTREAM_UNAVAILABLE', `Owner resolution error: ${String(err)}`);
      }

      if (!result || result.status === 'unknown') {
        return structuredError(
          'NO_PROPERTIES_FOUND',
          `No owner found for entity_id: ${parsed.entity_id}`
        );
      }

      await setCache(cacheKey, result, 86400);

      const payload = result as unknown as Record<string, unknown>;
      return {
        content:           [{ type: 'text', text: JSON.stringify(result) }],
        structuredContent: payload,
      };
    }
  );
}
