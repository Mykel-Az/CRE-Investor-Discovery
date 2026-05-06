// src/cache/client.ts
import { createClient } from 'redis';

export const redis = createClient({
  url: process.env.REDIS_URL ?? 'redis://localhost:6379',
});

redis.on('error', (err) => {
  // Log but never crash — cache is best-effort
  console.error('[cache] Redis error:', err.message);
});

let connected = false;

export async function connectRedis(): Promise<void> {
  try {
    await redis.connect();
    connected = true;
    console.log('[cache] Redis connected');
  } catch (err) {
    console.error('[cache] Redis unavailable — running without cache:', err);
  }
}

export function isConnected(): boolean {
  return connected;
}
