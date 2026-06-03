// src/index.ts
import express from 'express';
import cors from 'cors';
import { McpServer } from '@modelcontextprotocol/sdk/server/mcp.js';
import { StreamableHTTPServerTransport } from '@modelcontextprotocol/sdk/server/streamableHttp.js';
import { connectRedis, redis } from './cache/client.js';
import { connectDatabase, pool } from './db/client.js';
import { registerAllTools } from './tools/index.js';
import { startBackgroundJobs } from './ingest/jobs.js';
import { query } from './db/client.js';
import { createContextMiddleware } from '@ctxprotocol/sdk';

const app = express();
app.use(cors({
  origin: '*',
  methods: ['GET', 'POST', 'OPTIONS'],
  allowedHeaders: ['Content-Type', 'Accept', 'Authorization'],
}));
app.options('*', cors());
app.use(express.json());

// ─── Health check ─────────────────────────────────────────────────────────
app.get('/health', (_req, res) => {
  res.json({
    status: 'ok',
    service: 'cre-investor-discovery-mcp',
    version: '1.0.0',
    ts: new Date().toISOString(),
  });
  query('SELECT 1').catch(() => {});
});

// ─── MCP endpoint ─────────────────────────────────────────────────────────
app.use('/mcp', createContextMiddleware());
app.post('/mcp', async (req, res) => {
  const server = new McpServer({
    name:    'cre-investor-discovery-mcp',
    version: '1.0.0',
  });

  registerAllTools(server);

  const transport = new StreamableHTTPServerTransport({
    sessionIdGenerator: undefined,
  });

  await server.connect(transport);
  await transport.handleRequest(req, res, req.body);
});

// ─── Self-ping (defined at module scope, not inside main) ─────────────────
function startSelfPing(): void {
  const PORT = parseInt(process.env.PORT ?? '3000', 10);
  const selfUrl = process.env.RAILWAY_PUBLIC_DOMAIN
    ? `https://${process.env.RAILWAY_PUBLIC_DOMAIN}/health`
    : `http://localhost:${PORT}/health`;

  setInterval(async () => {
    try {
      const res = await fetch(selfUrl, { signal: AbortSignal.timeout(10_000) });
      if (!res.ok) console.warn('[keepalive] Self-ping returned', res.status);
    } catch (err) {
      console.warn('[keepalive] Self-ping failed:', (err as Error).message);
    }
  }, 4 * 60 * 1000);

  console.log('[keepalive] Self-ping loop started →', selfUrl);
}

// ─── Bootstrap ────────────────────────────────────────────────────────────
async function main(): Promise<void> {
  await connectRedis();
  await connectDatabase();
  startBackgroundJobs();
  startSelfPing();

  const PORT = parseInt(process.env.PORT ?? '3000', 10);
  app.listen(PORT, () => {
    console.log(`[server] CRE Investor Discovery MCP running on :${PORT}`);
    console.log(`[server] MCP endpoint: POST http://localhost:${PORT}/mcp`);
    console.log(`[server] Health:       GET  http://localhost:${PORT}/health`);
  });
}

main().catch((err) => {
  console.error('[server] Fatal error:', err);
  process.exit(1);
});

async function shutdown(signal: string): Promise<void> {
  console.log(`[server] ${signal} received — shutting down gracefully`);
  try {
    await pool.end();
    await redis.quit();
  } catch (err) {
    console.error('[server] Error during shutdown:', err);
  }
  process.exit(0);
}

process.on('SIGTERM', () => { shutdown('SIGTERM').catch(() => process.exit(1)); });
process.on('SIGINT',  () => { shutdown('SIGINT').catch(() => process.exit(1)); });