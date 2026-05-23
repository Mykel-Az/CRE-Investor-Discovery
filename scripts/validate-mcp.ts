// scripts/validate-mcp.ts
// Step 2: Direct endpoint validation using @modelcontextprotocol/sdk
import { Client } from '@modelcontextprotocol/sdk/client/index.js';
import { StreamableHTTPClientTransport } from '@modelcontextprotocol/sdk/client/streamableHttp.js';

const ENDPOINT = 'https://cre-investor-discovery-production.up.railway.app/mcp';

console.log('=== STEP 2: Direct Endpoint Validation ===\n');
console.log('Endpoint:', ENDPOINT);

// 2.1 Connection + Tool Discovery
const client = new Client({ name: 'validator', version: '1.0.0' });
const transport = new StreamableHTTPClientTransport(new URL(ENDPOINT));

try {
  await client.connect(transport);
  console.log('✓ Connected successfully\n');
} catch (err) {
  console.error('✗ Connection failed:', err);
  process.exit(1);
}

// List tools
const { tools } = await client.listTools();
console.log(`Discovered ${tools.length} tool(s):\n`);

const results: Array<{
  name: string; schemaOk: boolean; smokePass: boolean;
  queryReady: boolean; executeReady: boolean; notes: string[];
  responseTime?: number;
}> = [];

for (const tool of tools) {
  console.log(`── ${tool.name}`);
  console.log(`   Description: ${tool.description?.slice(0, 120)}...`);

  const notes: string[] = [];
  let schemaOk = true;

  // 2.2 Schema Quality Audit
  const input = tool.inputSchema as any;
  const output = (tool as any).outputSchema;

  if (!output) { notes.push('MISSING outputSchema (Data Broker Standard violation)'); schemaOk = false; }
  if (!input?.properties || Object.keys(input.properties).length === 0) {
    notes.push('No input properties defined'); schemaOk = false;
  }

  // Check for descriptions and examples on input properties
  if (input?.properties) {
    for (const [prop, def] of Object.entries(input.properties as Record<string, any>)) {
      if (!def.description) notes.push(`Input '${prop}' missing description`);
      if (!def.examples && !def.default && !def.enum) notes.push(`Input '${prop}' missing examples/default`);
    }
  }

  const meta = (tool as any)._meta;
  if (!meta) {
    notes.push('Missing _meta (surface, queryEligible, latencyClass, pricing, rateLimit)'); schemaOk = false;
  } else {
    if (!meta.surface)       notes.push('_meta.surface not set');
    if (!meta.latencyClass)  notes.push('_meta.latencyClass not set');
    if (!meta.pricing?.executeUsd) notes.push('_meta.pricing.executeUsd not set');
    if (!meta.rateLimit)     notes.push('_meta.rateLimit not set');
  }

  // 2.3 Smoke Test — generate sample inputs
  const sampleArgs: Record<string, any> = {};
  if (input?.properties) {
    for (const [prop, def] of Object.entries(input.properties as Record<string, any>)) {
      if (def.examples?.[0] !== undefined) sampleArgs[prop] = def.examples[0];
      else if (def.default !== undefined)  sampleArgs[prop] = def.default;
      else if (def.enum?.[0] !== undefined) sampleArgs[prop] = def.enum[0];
      else if (def.type === 'string')       sampleArgs[prop] = prop === 'corridor' ? 'Broadway, NY' : prop === 'property_type' ? 'Retail' : prop === 'entity_id' ? 'sos_ny_139857' : prop === 'address' ? '350 5th Avenue, New York, NY' : 'test';
      else if (def.type === 'number')       sampleArgs[prop] = 0;
      else if (def.type === 'boolean')      sampleArgs[prop] = false;
      else if (def.type === 'integer')      sampleArgs[prop] = 5;
    }
  }

  console.log(`   Sample args: ${JSON.stringify(sampleArgs)}`);

  let smokePass = false;
  let queryReady = false;
  let executeReady = false;
  let responseTime: number | undefined;

  try {
    const start = Date.now();
    const response = await client.callTool({ name: tool.name, arguments: sampleArgs });
    responseTime = Date.now() - start;
    smokePass = true;

    const content = response.content as any[];
    const hasContent = content?.length > 0;
    const hasText = content?.some((c: any) => c.type === 'text' && c.text?.length > 10);
    const hasStructured = !!(response as any).structuredContent;

    if (!hasContent) { notes.push('Empty content array'); smokePass = false; }
    if (!hasText)    notes.push('No meaningful text in response');
    if (!hasStructured) notes.push('Missing structuredContent');

    queryReady = hasContent && hasText && !!output;
    executeReady = queryReady && !!meta?.pricing?.executeUsd && !!meta?.surface;

    // Check response time
    if (responseTime > 60_000) notes.push(`Response too slow: ${responseTime}ms (>60s)`);

    // Sample output preview
    const textContent = content?.find((c: any) => c.type === 'text');
    if (textContent?.text) {
      const preview = textContent.text.slice(0, 200);
      console.log(`   Response (${responseTime}ms): ${preview}...`);
    }

    // Validate outputSchema if present
    if (output && hasStructured) {
      const structured = (response as any).structuredContent;
      for (const [key, def] of Object.entries((output.properties ?? {}) as Record<string, any>)) {
        if (output.required?.includes(key) && structured[key] === undefined) {
          notes.push(`structuredContent missing required field: '${key}'`);
        }
      }
    }

  } catch (err: any) {
    notes.push(`Smoke test error: ${err.message}`);
    responseTime = undefined;
  }

  results.push({ name: tool.name, schemaOk, smokePass, queryReady, executeReady, notes, responseTime });

  const status = smokePass ? '✓' : '✗';
  console.log(`   ${status} Smoke: ${smokePass ? 'PASS' : 'FAIL'} | Schema: ${schemaOk ? 'OK' : 'ISSUES'} | Query: ${queryReady ? 'READY' : 'NOT READY'} | Execute: ${executeReady ? 'READY' : 'NOT READY'}`);
  if (notes.length) console.log(`   Issues: ${notes.join('; ')}`);
  console.log();
}

// 2.4 Summary table
console.log('\n=== VALIDATION SUMMARY TABLE ===');
console.log('Tool Name'.padEnd(30) + 'Schema OK'.padEnd(12) + 'Smoke Test'.padEnd(12) + 'Query Ready'.padEnd(14) + 'Execute Ready'.padEnd(14) + 'Notes');
console.log('─'.repeat(100));
for (const r of results) {
  console.log(
    r.name.padEnd(30) +
    (r.schemaOk ? '✓' : '✗').padEnd(12) +
    (r.smokePass ? 'PASS' : 'FAIL').padEnd(12) +
    (r.queryReady ? 'YES' : 'NO').padEnd(14) +
    (r.executeReady ? 'YES' : 'NO').padEnd(14) +
    (r.notes[0] ?? 'OK')
  );
}

await client.close();
process.exit(0);
