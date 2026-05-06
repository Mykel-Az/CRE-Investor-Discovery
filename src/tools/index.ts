// src/tools/index.ts
import type { McpServer } from '@modelcontextprotocol/sdk/server/mcp.js';
import { registerInvestorDiscovery } from './investor-discovery.js';
import { registerOwnerProfile }      from './owner-profile.js';
import { registerParcelLookup }      from './parcel-lookup.js';

export function registerAllTools(server: McpServer): void {
  registerInvestorDiscovery(server);
  registerOwnerProfile(server);
  registerParcelLookup(server);
}
