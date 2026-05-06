// scripts/download-parcels.ts
//
// Downloads US address data from Overture Maps using the overturemaps Python CLI.
// Outputs one GeoJSONSeq file per state into ./data/openaddresses/.
//
// Usage:
//   npm run download-parcels              # downloads all default states
//   npm run download-parcels -- nj,ny    # downloads only NJ and NY
//
// Requires Python 3.8+ with pip. The overturemaps package is installed automatically.
//
// What this data provides:
//   ✅ Address (number + street), city, state, zip, lat/lng geometry
//   ❌ owner_name_raw — spatial queries will work but owner resolution
//      requires ATTOM or county GIS data (supplement separately)

import { execSync, spawnSync } from 'child_process';
import * as fs from 'fs';
import * as path from 'path';

// Force UTF-8 on Windows — Python defaults to CP1252 which can't encode
// some Unicode characters present in Overture Maps address data.
process.env['PYTHONIOENCODING'] = 'utf-8';
process.env['PYTHONUTF8']       = '1';

const OUT_DIR = path.resolve(process.env.OPENADDRESSES_DATA_DIR ?? './data/openaddresses');

// Bounding boxes: west,south,east,north (WGS84)
const ALL_STATES: Record<string, { name: string; bbox: string; sizeMb: number }> = {
  nj: { name: 'New Jersey',    bbox: '-75.5593,38.9284,-73.8977,41.3574', sizeMb: 80   },
  ny: { name: 'New York',      bbox: '-79.7624,40.4961,-71.8562,45.0153', sizeMb: 350  },
  fl: { name: 'Florida',       bbox: '-87.6349,24.3963,-79.9743,31.0009', sizeMb: 450  },
  tx: { name: 'Texas',         bbox: '-106.6456,25.8371,-93.5083,36.5007', sizeMb: 600 },
  ca: { name: 'California',    bbox: '-124.4096,32.5341,-114.1308,42.0095', sizeMb: 700},
  il: { name: 'Illinois',      bbox: '-91.5131,36.9703,-87.0199,42.5083', sizeMb: 280  },
  pa: { name: 'Pennsylvania',  bbox: '-80.5198,39.7198,-74.6895,42.2699', sizeMb: 260  },
  ga: { name: 'Georgia',       bbox: '-85.6052,30.3558,-80.7514,35.0008', sizeMb: 230  },
  co: { name: 'Colorado',      bbox: '-109.0603,36.9924,-102.0424,41.0034', sizeMb: 160},
  wa: { name: 'Washington',    bbox: '-124.7633,45.5435,-116.9160,48.9923', sizeMb: 200},
  az: { name: 'Arizona',       bbox: '-114.8183,31.3322,-109.0452,37.0042', sizeMb: 170},
  ma: { name: 'Massachusetts', bbox: '-73.5081,41.2373,-69.9282,42.8867', sizeMb: 160  },
  nc: { name: 'North Carolina',bbox: '-84.3219,33.8422,-75.4600,36.5880', sizeMb: 240  },
  oh: { name: 'Ohio',          bbox: '-84.8203,38.4032,-80.5186,41.9773', sizeMb: 270  },
  mi: { name: 'Michigan',      bbox: '-90.4182,41.6961,-82.4135,48.3061', sizeMb: 250  },
};

// ─── Resolve which states to download ─────────────────────────────────────────

const argStates = process.argv[2]?.toLowerCase().split(',').map(s => s.trim()).filter(Boolean);
type StateEntry = { name: string; bbox: string; sizeMb: number };
const targets: Record<string, StateEntry> = argStates?.length
  ? Object.fromEntries(argStates.map(k => [k, ALL_STATES[k]]).filter(([, v]) => v))
  : ALL_STATES;

if (argStates?.length && Object.keys(targets).length === 0) {
  console.error(`Unknown state code(s): ${argStates.join(', ')}`);
  console.error(`Valid codes: ${Object.keys(ALL_STATES).join(', ')}`);
  process.exit(1);
}

const totalMb = Object.values(targets).reduce((s, t) => s + t.sizeMb, 0);
console.log(`[parcels] Downloading ${Object.keys(targets).length} state(s) — ~${totalMb.toLocaleString()} MB total`);
console.log(`[parcels] Output directory: ${OUT_DIR}\n`);

// ─── Python / overturemaps check ──────────────────────────────────────────────

function run(cmd: string, opts?: { silent?: boolean }): boolean {
  const result = spawnSync(cmd, { shell: true, stdio: opts?.silent ? 'pipe' : 'inherit' });
  return result.status === 0;
}

function checkPython(): string {
  for (const bin of ['python3', 'python']) {
    const r = spawnSync(bin, ['--version'], { shell: true, stdio: 'pipe' });
    if (r.status === 0) return bin;
  }
  throw new Error(
    'Python 3.8+ is required to download Overture Maps data.\n' +
    'Install it from https://www.python.org/downloads/ then re-run this script.'
  );
}

const python = checkPython();
console.log(`[parcels] Python found: ${python}`);

// Install / upgrade overturemaps CLI if needed
console.log('[parcels] Ensuring overturemaps CLI is installed...');
const installed = run(`${python} -m overturemaps --version`, { silent: true });
if (!installed) {
  console.log('[parcels] Installing overturemaps via pip...');
  execSync(`${python} -m pip install overturemaps --quiet`, { stdio: 'inherit' });
}
console.log('[parcels] overturemaps CLI ready.\n');

// ─── Download per state ───────────────────────────────────────────────────────

fs.mkdirSync(OUT_DIR, { recursive: true });

const MAX_RETRIES = 4;

function sleep(ms: number) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

async function downloadState(code: string, state: { name: string; bbox: string; sizeMb: number }): Promise<boolean> {
  const outFile = path.join(OUT_DIR, `${code}_addresses.geojsonseq`);

  // Skip only if file is at least 80% of the expected size (guard against partial downloads)
  const minExpectedBytes = state.sizeMb * 1_048_576 * 0.8;
  if (fs.existsSync(outFile) && fs.statSync(outFile).size >= minExpectedBytes) {
    console.log(`[parcels] ${state.name} — already downloaded, skipping. (delete to re-download)`);
    return true;
  }

  const cmd = [
    python, '-m', 'overturemaps', 'download',
    `--bbox=${state.bbox}`,
    '-f', 'geojsonseq',
    '--type=address',
    '-o', `"${outFile}"`,
  ].join(' ');

  for (let attempt = 1; attempt <= MAX_RETRIES; attempt++) {
    if (attempt > 1) {
      const delaySec = attempt * 15;
      console.log(`[parcels] Retrying ${state.name} (attempt ${attempt}/${MAX_RETRIES}) in ${delaySec}s...`);
      await sleep(delaySec * 1000);
    } else {
      console.log(`[parcels] Downloading ${state.name} (~${state.sizeMb} MB)...`);
    }

    // Remove any partial file from a previous failed attempt
    if (fs.existsSync(outFile)) fs.unlinkSync(outFile);

    const ok = run(cmd);
    if (ok && fs.existsSync(outFile) && fs.statSync(outFile).size > 1024) {
      const sizeMb = (fs.statSync(outFile).size / 1_048_576).toFixed(1);
      console.log(`[parcels] ${state.name} done — ${sizeMb} MB → ${outFile}`);
      return true;
    }

    console.error(`[parcels] Attempt ${attempt} failed for ${state.name} (network error)`);
    if (fs.existsSync(outFile)) fs.unlinkSync(outFile);
  }

  console.error(`[parcels] FAILED: ${state.name} (all ${MAX_RETRIES} attempts exhausted)`);
  return false;
}

let succeeded = 0;
let failed    = 0;

for (const [code, state] of Object.entries(targets)) {
  const ok = await downloadState(code, state);
  if (ok) succeeded++; else failed++;
}

console.log(`\n[parcels] Download complete: ${succeeded} succeeded, ${failed} failed.`);
console.log('[parcels] Next step: npm run load-parcels');

if (failed > 0) process.exit(1);
