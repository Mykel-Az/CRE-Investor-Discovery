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

function formatProfile(r: any): string {
  if (!r) return 'Owner profile not found.';
  const lines: string[] = [
    `${r.canonical_name} (${r.entity_type}, ${r.entity_id})`,
    `Status: ${r.status} | Jurisdiction: ${r.jurisdiction}${r.incorporated_at ? ` | Incorporated: ${r.incorporated_at}` : ''}`,
  ];
  if (r.beneficial_owners?.length) {
    lines.push(`Beneficial owners: ${r.beneficial_owners.map((b: any) => `${b.name} (${b.role}${b.ownership_pct != null ? `, ${b.ownership_pct}%` : ''})`).join('; ')}`);
  }
  if (r.registered_agent) {
    lines.push(`Registered agent: ${r.registered_agent.name} — ${r.registered_agent.address}`);
  }
  const ps = r.portfolio_summary;
  if (ps) {
    lines.push(`Portfolio: ${ps.property_count} propert${ps.property_count === 1 ? 'y' : 'ies'}${ps.total_sqft ? ` | ${ps.total_sqft.toLocaleString()} sqft total` : ''} | States: ${(ps.states_present ?? []).join(', ')} | Types: ${(ps.property_types ?? []).join(', ')}`);
  }
  if (r.contact) {
    lines.push(`Contact: ${r.contact.name} (${r.contact.role})${r.contact.email ? ` | ${r.contact.email}` : ''}${r.contact.phone ? ` | ${r.contact.phone}` : ''}`);
  }
  const props = r.properties ?? [];
  if (props.length) {
    lines.push(`\nProperties (${props.length} shown):`);
    for (const p of props.slice(0, 5)) {
      const size = p.lot_size_acres ? ` | ${p.lot_size_acres.toFixed(2)} ac` : '';
      lines.push(`  • ${p.address}, ${p.city}, ${p.state} — ${p.property_type}${size}`);
    }
    if (props.length > 5) lines.push(`  … and ${props.length - 5} more`);
  }
  return lines.join('\n');
}

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
          content:           [{ type: 'text', text: formatProfile(cached as any) }],
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
        content:           [{ type: 'text', text: formatProfile(result as any) }],
        structuredContent: payload,
      };
    }
  );
}
