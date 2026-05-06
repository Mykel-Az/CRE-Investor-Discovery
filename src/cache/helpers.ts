// src/cache/helpers.ts
import { redis } from './client.js';

export async function getCached<T>(key: string): Promise<T | null> {
  try {
    const val = await redis.get(key);
    return val ? (JSON.parse(val) as T) : null;
  } catch {
    return null; // Cache miss on error — caller falls through to live
  }
}

export async function setCache(
  key: string,
  value: unknown,
  ttlSecs = 3600
): Promise<void> {
  try {
    await redis.set(key, JSON.stringify(value), { EX: ttlSecs });
  } catch {
    // Swallow — cache is best-effort; live data still served
  }
}

export async function deleteCache(key: string): Promise<void> {
  try {
    await redis.del(key);
  } catch {
    // Swallow
  }
}
