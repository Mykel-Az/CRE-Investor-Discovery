// scripts/download-sos-data.ts
//
// Downloads free SOS bulk entity CSV files for states that publish open data.
// Transforms each state's raw columns into the standard format expected by jobs.ts.
//
// Usage:  npm run download-sos
// Output: ./data/sos/{state}_entities.csv
//
// Automated (free direct download): NY, IA
// Manual (see printed instructions):  FL, TX, CA, NJ, IL, GA, PA, CO, AZ, WA

import * as fs from 'fs';
import * as path from 'path';
import * as readline from 'readline';
import { Readable } from 'stream';

const OUT_DIR = path.resolve(process.env.SOS_DATA_DIR ?? './data/sos');

const OUTPUT_COLS = [
  'filing_number',
  'name',
  'entity_type',
  'status',
  'incorporated_date',
  'registered_agent_name',
  'registered_agent_address',
  'filing_url',
] as const;

type Col = typeof OUTPUT_COLS[number];
type OutputRow = Record<Col, string>;

interface StateConfig {
  label:   string;
  url:     string;
  outFile: string;
  transform: (row: Record<string, string>) => OutputRow | null;
}

// ─── Helpers ──────────────────────────────────────────────────────────────────

function normalizeEntityType(raw: string): string {
  const t = raw.toUpperCase();
  if (t.includes('LLC') || t.includes('LIMITED LIABILITY'))                        return 'LLC';
  if (t.includes('TRUST'))                                                          return 'Trust';
  if (t.includes('PARTNERSHIP') || t.includes(' LP') || t.includes(' LLP'))        return 'Corporation';
  if (t.includes('CORP') || t.includes('INC') || t.includes('COMPANY') || t.includes('BANK')) return 'Corporation';
  if (t.includes('INDIVIDUAL') || t.includes('SOLE PROP'))                         return 'Individual';
  return 'Unknown';
}

function formatDate(raw: string): string {
  if (!raw) return '';
  return raw.slice(0, 10); // keep only YYYY-MM-DD, drop ISO timestamp
}

function addr(...parts: (string | undefined)[]): string {
  return parts.filter(p => p && p.trim()).join(', ');
}

function parseCSVLine(line: string): string[] {
  const result: string[] = [];
  let current = '';
  let inQuotes = false;
  for (const ch of line) {
    if (ch === '"')             { inQuotes = !inQuotes; }
    else if (ch === ',' && !inQuotes) { result.push(current.trim()); current = ''; }
    else                        { current += ch; }
  }
  result.push(current.trim());
  return result;
}

function csvEscape(val: string): string {
  if (val.includes(',') || val.includes('"') || val.includes('\n')) {
    return `"${val.replace(/"/g, '""')}"`;
  }
  return val;
}

// ─── State configs ────────────────────────────────────────────────────────────

const STATES: StateConfig[] = [

  // ── New York ────────────────────────────────────────────────────────────────
  // Source: data.ny.gov "Active Corporations: Beginning 1800"
  // ~2.4M records. Updated monthly by NY Department of State.
  {
    label:   'New York',
    url:     'https://data.ny.gov/api/views/n9v6-gdp6/rows.csv?accessType=DOWNLOAD',
    outFile: path.join(OUT_DIR, 'ny_entities.csv'),
    transform(row) {
      if (!row['dos_id'] || !row['current_entity_name']) return null;
      return {
        filing_number:            row['dos_id'],
        name:                     row['current_entity_name'],
        entity_type:              normalizeEntityType(row['entity_type'] ?? ''),
        status:                   'active',
        incorporated_date:        formatDate(row['initial_dos_filing_date'] ?? ''),
        registered_agent_name:    row['registered_agent_name'] ?? '',
        registered_agent_address: addr(
          row['registered_agent_address_1'],
          row['registered_agent_city'],
          row['registered_agent_state'],
          row['registered_agent_zip'],
        ),
        filing_url: `https://apps.dos.ny.gov/publicInquiry/EntityDisplay?DOS_ID=${row['dos_id']}`,
      };
    },
  },

  // ── Iowa ────────────────────────────────────────────────────────────────────
  // Source: data.iowa.gov "Active Iowa Business Entities"
  // ~250K records. Maintained by Iowa SOS.
  {
    label:   'Iowa',
    url:     'https://data.iowa.gov/api/views/ez5t-3qay/rows.csv?accessType=DOWNLOAD',
    outFile: path.join(OUT_DIR, 'ia_entities.csv'),
    transform(row) {
      if (!row['corp_number'] || !row['legal_name']) return null;
      return {
        filing_number:            row['corp_number'],
        name:                     row['legal_name'],
        entity_type:              normalizeEntityType(row['corporation_type'] ?? ''),
        status:                   'active',
        incorporated_date:        formatDate(row['effective_date'] ?? ''),
        registered_agent_name:    row['registered_agent'] ?? '',
        registered_agent_address: addr(
          row['ra_address_1'],
          row['ra_city'],
          row['ra_state'],
          row['ra_zip'],
        ),
        filing_url: '',
      };
    },
  },
];

// ─── Download + transform ─────────────────────────────────────────────────────

async function downloadState(config: StateConfig): Promise<void> {
  console.log(`\n[sos] Downloading ${config.label}...`);
  console.log(`      ${config.url}`);

  const response = await fetch(config.url, { headers: { Accept: 'text/csv' } });
  if (!response.ok)   throw new Error(`HTTP ${response.status} ${response.statusText}`);
  if (!response.body) throw new Error('No response body');

  fs.mkdirSync(path.dirname(config.outFile), { recursive: true });

  const out = fs.createWriteStream(config.outFile);
  out.write(OUTPUT_COLS.join(',') + '\n');

  const nodeStream = Readable.fromWeb(
    response.body as Parameters<typeof Readable.fromWeb>[0]
  );
  const rl = readline.createInterface({ input: nodeStream, crlfDelay: Infinity });

  let srcHeaders: string[] = [];
  let isFirstLine = true;
  let written = 0;
  let skipped = 0;

  for await (const line of rl) {
    if (isFirstLine) {
      // Strip quotes from header names and lowercase for consistent access
      srcHeaders = parseCSVLine(line).map(h => h.replace(/^"|"$/g, '').toLowerCase().replace(/\s+/g, '_'));
      isFirstLine = false;
      continue;
    }
    if (!line.trim()) continue;

    const cols = parseCSVLine(line).map(v => v.replace(/^"|"$/g, ''));
    const row  = Object.fromEntries(srcHeaders.map((h, i) => [h, cols[i] ?? '']));

    const outputRow = config.transform(row);
    if (!outputRow) { skipped++; continue; }

    out.write(OUTPUT_COLS.map(k => csvEscape(outputRow[k])).join(',') + '\n');
    written++;

    if (written % 50_000 === 0) {
      process.stdout.write(`  ${written.toLocaleString()} rows written...\r`);
    }
  }

  await new Promise<void>((resolve, reject) => {
    out.on('finish', resolve);
    out.on('error', reject);
    out.end();
  });

  console.log(
    `[sos] ${config.label} complete — ` +
    `${written.toLocaleString()} entities written, ${skipped} skipped` +
    ` → ${config.outFile}`
  );
}

// ─── Manual state instructions ────────────────────────────────────────────────

const MANUAL_STATES = [
  {
    state: 'FL',
    url:   'https://dos.myflorida.com/sunbiz/download/',
    notes: 'Free. Provide email to receive download link. Extract corporations_*.txt from ZIP, rename columns to standard format.',
  },
  {
    state: 'TX',
    url:   'https://www.sos.state.tx.us/corp/sosdata.shtml',
    notes: 'Free bulk data file. Create an account, request the "Corporations" or "LLC" dataset.',
  },
  {
    state: 'NJ',
    url:   'https://www.njportal.com/DOR/BusinessNameSearch',
    notes: 'No bulk download. File an OPRA request at https://www.njoag.gov/opra/ for bulk entity data.',
  },
  {
    state: 'CA',
    url:   'https://bizfileonline.sos.ca.gov/',
    notes: 'Paid. "Master Unload" bulk CSV starts at $100. Contact SOS at (916) 657-5448.',
  },
  {
    state: 'IL',
    url:   'https://apps.ilsos.gov/businessentitysearch/',
    notes: 'No bulk download available. File a FOIA request with the IL SOS for bulk data.',
  },
  {
    state: 'GA',
    url:   'https://ecorp.sos.ga.gov/',
    notes: 'Bulk data available on request. Contact the GA SOS office directly.',
  },
  {
    state: 'PA',
    url:   'https://www.corporations.pa.gov/',
    notes: 'No bulk download. File a Right-to-Know request with PA DOS.',
  },
  {
    state: 'CO',
    url:   'https://data.colorado.gov/',
    notes: 'Check data.colorado.gov for open datasets. Search "business entities".',
  },
  {
    state: 'AZ',
    url:   'https://ecorp.azcc.gov/',
    notes: 'Bulk subscription available via AZ Corporation Commission. Contact (602) 542-3026.',
  },
  {
    state: 'WA',
    url:   'https://www.sos.wa.gov/corps/',
    notes: 'File a public records request at https://www.sos.wa.gov/about/public-records-requests.aspx',
  },
];

function printManualInstructions(): void {
  const divider = '─'.repeat(70);
  console.log(`\n${divider}`);
  console.log('MANUAL DOWNLOADS NEEDED — states without free open-data portals:');
  console.log(divider);
  console.log('Place downloaded files in ./data/sos/ named {state}_entities.csv');
  console.log('Required columns (header row must match exactly):');
  console.log('  filing_number,name,entity_type,status,incorporated_date,');
  console.log('  registered_agent_name,registered_agent_address,filing_url\n');
  for (const s of MANUAL_STATES) {
    console.log(`  ${s.state}  ${s.url}`);
    console.log(`     ${s.notes}\n`);
  }
  console.log(divider);
}

// ─── Main ─────────────────────────────────────────────────────────────────────

console.log('[sos] SOS bulk data downloader starting...');
console.log(`[sos] Output directory: ${OUT_DIR}\n`);

let failed = 0;
for (const state of STATES) {
  try {
    await downloadState(state);
  } catch (err) {
    console.error(`[sos] FAILED ${state.label}:`, (err as Error).message);
    failed++;
  }
}

printManualInstructions();

console.log(
  `[sos] Done. ${STATES.length - failed}/${STATES.length} automated downloads succeeded.`
);
if (failed > 0) process.exit(1);
