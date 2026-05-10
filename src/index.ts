// src/index.ts
import express from 'express';
import cors from 'cors';
import { McpServer } from '@modelcontextprotocol/sdk/server/mcp.js';
import { StreamableHTTPServerTransport } from '@modelcontextprotocol/sdk/server/streamableHttp.js';
import { connectRedis, redis } from './cache/client.js';
import { connectDatabase, pool } from './db/client.js';
import { registerAllTools } from './tools/index.js';
import { startBackgroundJobs } from './ingest/jobs.js';

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
  res.json({ status: 'ok', service: 'cre-investor-discovery-mcp', version: '1.0.0' });
});

// ─── MCP endpoint ─────────────────────────────────────────────────────────
// createContextMiddleware() is applied here when @ctxprotocol/sdk is available.
// This line secures paid tool calls; free discovery (tools/list) passes through.
// Uncomment when deploying to the Context Protocol marketplace:
// import { createContextMiddleware } from '@ctxprotocol/sdk';
// app.use('/mcp', createContextMiddleware());

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

// ─── Bootstrap ────────────────────────────────────────────────────────────
async function main(): Promise<void> {
  await connectRedis();
  await connectDatabase();
  startBackgroundJobs();

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
