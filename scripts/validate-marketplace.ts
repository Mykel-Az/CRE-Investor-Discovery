// scripts/validate-marketplace.ts
// Steps 4 + 5: Context Marketplace SDK validation
// Usage: node --env-file=.env --import tsx scripts/validate-marketplace.ts

// @ts-ignore
import { ContextClient } from '@ctxprotocol/sdk';

const CONTEXT_API_KEY = process.env.CONTEXT_API_KEY ?? '';
const TOOL_ID         = '713eef81-e726-4b45-ab8f-c7bdbb201866';

const client = new ContextClient({ apiKey: CONTEXT_API_KEY });

// ─── Step 4.1: Discover the tool ─────────────────────────────────────────────
console.log('=== STEP 4: Query Mode Marketplace Validation ===\n');
console.log('Tool ID:', TOOL_ID);

let discoveredTool: any = null;
try {
  const tools = await client.discovery.search({
    query: 'CRE Corridor Investor Discovery',
    mode:  'query',
    surface: 'answer',
    queryEligible: true,
  });
  discoveredTool = tools?.find?.((t: any) => t.id === TOOL_ID) ?? tools?.[0];
  if (discoveredTool) {
    console.log('✓ Tool discovered:', discoveredTool.name ?? discoveredTool.id);
  } else {
    console.log('✗ Tool not found in discovery results — may not be staked/active yet');
    console.log('  Proceeding with known tool ID...\n');
  }
} catch (err: any) {
  console.log('Discovery error:', err.message, '— proceeding with known tool ID');
}

// ─── Step 4.2: Prompt suite (from Step 3.4 "Try asking" questions) ───────────
const prompts = [
  // 1. Core happy-path
  'Find retail property owners along Broadway in New York',
  // 2. Discovery/listing
  'Who are the top commercial property owners on 4th Avenue in Brooklyn?',
  // 3. Advanced filtered
  'Find industrial property investors along Northern Boulevard in Queens with lots over 1 acre',
  // 4. Multi-step workflow
  'Find mixed-use investors on Atlantic Avenue in Brooklyn, then give me the full profile for the first owner',
  // 5. Parcel lookup
  'Look up the owner of 350 5th Avenue New York NY',
  // 6. Edge-case
  'Who owns commercial properties near the Long Island Expressway in New York?',
  // 7. Power-user
  'Find office building owners on Amsterdam Avenue in Manhattan, filter to lots over 0.5 acres, and show their portfolio summary',
];

// ─── Step 4.3: Run Query Mode tests ──────────────────────────────────────────
console.log(`\nRunning ${prompts.length} Query Mode prompts...\n`);

const DETERMINISTIC_FAIL_MARKERS = [
  'i am unable to', "i'm unable to", 'i cannot provide', "i'm not able to",
  'i do not have', 'no data available', 'could not fulfill',
  'slug not found', 'event not found', 'market not found', 'could not resolve',
  'not found', 'could not fulfill',
];

function checkDeterministicFail(text: string): string | null {
  const lower = text.toLowerCase();
  for (const marker of DETERMINISTIC_FAIL_MARKERS) {
    if (lower.includes(marker)) return marker;
  }
  return null;
}

const results: Array<{
  prompt: string;
  pass: boolean;
  autoFailMarker?: string;
  costUsd?: number;
  durationMs?: number;
  toolsUsed?: string[];
  retryCount?: number;
  error?: string;
  responsePreview?: string;
}> = [];

for (let i = 0; i < prompts.length; i++) {
  const prompt = prompts[i];
  console.log(`[${i + 1}/${prompts.length}] "${prompt.slice(0, 70)}..."`);

  try {
    // @ts-ignore — queryDepth is in the live API but not yet in the TS type
    const answer = await client.query.run({
      query:                prompt,
      tools:                [TOOL_ID],
      queryDepth:           'deep',
      responseShape:        'answer_with_evidence',
      includeDeveloperTrace: true,
    });

    // QueryBaseResult fields (correct SDK paths)
    const text       = (answer as any)?.response ?? '';
    const cost       = (answer as any)?.cost?.totalCostUsd;
    const duration   = (answer as any)?.durationMs;
    const toolsUsed  = (answer as any)?.toolsUsed ?? [];
    const trace      = (answer as any)?.developerTrace ?? {};
    const retryCount = trace?.retryCount ?? 0;

    // 4.3.1 Deterministic failure gate
    const autoFail = text ? checkDeterministicFail(text) : 'empty response';
    const zeroToolCalls = Array.isArray(toolsUsed) && toolsUsed.length === 0;

    const pass = !autoFail && !zeroToolCalls && text?.length > 50;

    results.push({
      prompt,
      pass,
      autoFailMarker:  autoFail ?? undefined,
      costUsd:         cost,
      durationMs:      duration,
      toolsUsed:       Array.isArray(toolsUsed) ? toolsUsed : [],
      retryCount,
      responsePreview: text?.slice(0, 200),
    });

    const status = pass ? '  ✓ PASS' : '  ✗ FAIL';
    console.log(`${status} | cost: $${cost ?? '?'} | ${duration ?? '?'}ms | tools: ${JSON.stringify(toolsUsed)}`);
    if (autoFail)      console.log(`    AUTO-FAIL: "${autoFail}"`);
    if (zeroToolCalls) console.log(`    AUTO-FAIL: zero tool calls`);
    if (retryCount > 3) console.log(`    WARNING: high retry count (${retryCount})`);
    if (text)          console.log(`    Response: "${text.slice(0, 150)}..."`);
    console.log();

  } catch (err: any) {
    results.push({ prompt, pass: false, error: err.message });
    console.log(`  ✗ ERROR: ${err.message}\n`);
  }
}

// ─── Step 5: Execute Mode ──────────────────────────────────────────────────
console.log('\n=== STEP 5: Execute Mode Marketplace Validation ===\n');

try {
  const execTools = await client.discovery.search({
    query:   'CRE Corridor Investor Discovery',
    mode:    'execute',
    surface: 'execute',
  });
  const execTool = execTools?.find?.((t: any) => t.id === TOOL_ID) ?? execTools?.[0];

  if (!execTool) {
    console.log('Execute-eligible tool not found — may need execute pricing enabled in contribute form\n');
  } else {
    console.log('Execute tool:', execTool.name, '| methods:', execTool.mcpTools?.length ?? 0);
    const session = await client.tools.startSession({ maxSpendUsd: '2.00' });

    for (const method of (execTool.mcpTools ?? [])) {
      const args: Record<string, any> = {};
      if (method.name === 'investor_discovery') {
        args.corridor = 'Broadway, NY'; args.property_type = 'Retail'; args.max_results = 3;
      } else if (method.name === 'owner_profile') {
        args.entity_id = 'sos_ny_139857'; args.include_all_properties = false;
      } else if (method.name === 'parcel_lookup') {
        args.address = '350 5th Avenue, New York, NY';
      }

      try {
        const result = await client.tools.execute({
          toolId:    TOOL_ID,
          toolName:  method.name,
          args,
          sessionId: session.session.sessionId,
        });
        const price = result?.session?.methodPrice;
        const pass  = !!result?.result && !result?.error;
        console.log(`  ${pass ? '✓' : '✗'} ${method.name} | price: $${price ?? '?'} | ${pass ? 'PASS' : 'FAIL'}`);
      } catch (err: any) {
        console.log(`  ✗ ${method.name} | ERROR: ${err.message}`);
      }
    }
    await client.tools.closeSession(session.session.sessionId);
  }
} catch (err: any) {
  console.log('Execute mode error:', err.message);
}

// ─── Final summary ────────────────────────────────────────────────────────────
const passed  = results.filter(r => r.pass).length;
const failed  = results.filter(r => !r.pass).length;
const autoFails = results.filter(r => r.autoFailMarker).length;
const avgCost = results.filter(r => r.costUsd).reduce((s, r) => s + (r.costUsd ?? 0), 0) / (results.filter(r => r.costUsd).length || 1);

console.log('\n=== QUERY MODE RESULTS ===');
console.log(`Pass: ${passed}/${prompts.length} | Fail: ${failed} | Auto-fails: ${autoFails}`);
console.log(`Avg cost: $${avgCost.toFixed(4)}`);
console.log('\nPrompt-by-prompt:');
for (const r of results) {
  console.log(`  ${r.pass ? '✓' : '✗'} ${r.prompt.slice(0, 65).padEnd(65)} ${r.autoFailMarker ? `[AUTO-FAIL: ${r.autoFailMarker}]` : ''}`);
}

process.exit(failed > 0 ? 1 : 0);
