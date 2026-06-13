// src/cache/client.ts
import { createClient } from 'redis';

export const redis = createClient({
  url: process.env.REDIS_URL ?? 'redis://localhost:6379',
  // Fail fast when Redis is unreachable: commands reject immediately instead
  // of queueing and waiting out the reconnect backoff. The cache is
  // best-effort (helpers swallow errors and fall through to live data), so a
  // down Redis must never add latency to a query. Previously each get/set
  // could stall ~15-30s on the reconnect strategy, adding 35s+ per tool call.
  disableOfflineQueue: true,
  socket: {
    reconnectStrategy: (retries: number) => {
      if (retries > 20) return new Error('Redis reconnect limit reached');
      return Math.min(retries * 200, 3000);
    },
    connectTimeout: 3000,
  }
});

redis.on('error', (err) => {
  // Don't log repeated timeout noise — just log the first of each type
  console.error('[cache] Redis error:', err.message);
});

redis.on('reconnecting', () => {
  console.log('[cache] Redis reconnecting...');
});

redis.on('ready', () => {
  console.log('[cache] Redis ready');
});

let _pingInterval: ReturnType<typeof setInterval> | null = null;
let connected = false;

export async function connectRedis(): Promise<void> {
  try {
    await redis.connect();
    connected = true;
    console.log('[cache] Redis connected');

    // Ping every 3 minutes — Upstash closes connections idle for too long.
    // 3 minutes is safely under any Upstash idle threshold.
    if (_pingInterval) clearInterval(_pingInterval);
    _pingInterval = setInterval(async () => {
      try {
        await redis.ping();
      } catch (err) {
        // Swallow — the reconnectStrategy above handles reconnection
      }
    }, 3 * 60 * 1000);

  } catch (err) {
    console.error('[cache] Redis unavailable — running without cache:', err);
  }
}

export function isConnected(): boolean {
  return connected && redis.isReady;
}

export { };