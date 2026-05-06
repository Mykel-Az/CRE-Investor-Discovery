// src/db/client.ts
import pg from 'pg';

const pool = new pg.Pool({
  connectionString: process.env.DATABASE_URL ?? 'postgresql://localhost:5432/cre_investor',
  max: 20,
  idleTimeoutMillis: 30_000,
  connectionTimeoutMillis: 5_000,
});

pool.on('error', (err) => {
  // Log but never crash — DB errors are handled per-query
  console.error('[db] Unexpected pool error:', err.message);
});

/**
 * Execute a parameterised SQL query against the PostGIS database.
 * All resolver functions use this as the single entry point.
 */
export async function query<T extends pg.QueryResultRow = pg.QueryResultRow>(
  text: string,
  params?: unknown[]
): Promise<pg.QueryResult<T>> {
  return pool.query<T>(text, params);
}

/**
 * Check database connectivity. Called at startup.
 */
export async function connectDatabase(): Promise<void> {
  try {
    const client = await pool.connect();
    const result = await client.query('SELECT PostGIS_Version()');
    client.release();
    console.log(`[db] PostgreSQL connected — PostGIS ${result.rows[0]?.postgis_version ?? 'not found'}`);
  } catch (err) {
    console.error('[db] PostgreSQL unavailable — resolvers will fail:', err);
  }
}

export { pool };
