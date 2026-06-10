// src/cache/client.ts
import { createClient } from 'redis';

export const redis = createClient({
  url: process.env.REDIS_URL ?? 'redis://localhost:6379',
  socket: {
    reconnectStrategy: (retries: number) => {
      if (retries > 10) return new Error('Redis reconnect limit reached');
      return Math.min(retries * 500, 5000);
    },
    connectTimeout: 10000,
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