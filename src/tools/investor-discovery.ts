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

function formatDiscovery(r: any): string {
  if (!r || !r.owners) return 'No results found.';
  const { query_summary: q, owners } = r;
  if (!owners.length) {
    const base = `No ${q?.property_type ?? 'commercial'} property owners found along ${q?.corridor ?? 'the corridor'}.`;
    const suggestions = q?.closest_corridors?.length
      ? ` Closest available corridors in this state: ${q.closest_corridors.join(', ')}. Retry with one of these exact corridor names.`
      : '';
    return base + suggestions;
  }
  const lines: string[] = [
    `Found ${owners.length} ${q?.property_type ?? ''} property owner(s) along ${q?.corridor ?? 'the corridor'} (${q?.matched_parcels ?? 0} parcels matched).`,
  ];
  for (const o of owners.slice(0, 10)) {
    const props = o.properties_in_corridor ?? [];
    const portCount = o.portfolio_summary?.property_count ?? props.length;
    const contact = o.contact ? ` | Contact: ${o.contact.name}${o.contact.email ? ` <${o.contact.email}>` : ''}${o.contact.phone ? ` ${o.contact.phone}` : ''}` : '';
    lines.push(`• ${o.owner_name} (${o.entity_type}, ${o.entity_id}) — ${props.length} propert${props.length === 1 ? 'y' : 'ies'} in corridor, ${portCount} total in portfolio${contact}`);
    for (const p of props.slice(0, 3)) {
      const size = p.lot_size_acres ? ` | ${p.lot_size_acres.toFixed(2)} ac` : '';
      const sqft = p.building_sqft ? ` | ${p.building_sqft.toLocaleString()} sqft` : '';
      lines.push(`  - ${p.address}, ${p.city}, ${p.state} ${p.zip}${size}${sqft}`);
    }
  }
  return lines.join('\n');
}

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
        latencyClass:  'slow',
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
        'discovery:v2',
        parsed.corridor.toLowerCase().replace(/\s+/g, '_'),
        parsed.property_type.toLowerCase(),
        parsed.lot_size_min_acres ?? 0,
        parsed.lot_size_max_acres ?? 'any',
        parsed.max_results,
      ].join(':');

      const cached = await getCached<Record<string, unknown>>(cacheKey);
      if (cached) {
        return {
          content:           [{ type: 'text', text: formatDiscovery(cached as any) }],
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
        content:           [{ type: 'text', text: formatDiscovery(result as any) }],
        structuredContent: payload,
      };
    }
  );
}
