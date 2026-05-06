// scripts/init-db.ts
// Runs src/db/schema.sql against DATABASE_URL.
// Usage: npm run init-db

import pg from 'pg';
import * as fs from 'fs';
import * as path from 'path';
import { fileURLToPath } from 'url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const sql = fs.readFileSync(path.resolve(__dirname, '../src/db/schema.sql'), 'utf8');
const url = process.env.DATABASE_URL ?? 'postgresql://cre:cre@localhost:5432/cre_investor';

const client = new pg.Client({ connectionString: url });
await client.connect();
console.log('[init-db] Connected to', url.replace(/:\/\/.*@/, '://***@'));

try {
  await client.query('CREATE EXTENSION IF NOT EXISTS postgis;');
  console.log('[init-db] PostGIS extension enabled.');
  await client.query(sql);
  console.log('[init-db] Schema applied successfully.');
} finally {
  await client.end();
}
