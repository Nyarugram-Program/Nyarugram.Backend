// Explicit maintenance operation only. Never called by build or service startup.
import { readFile, writeFile, mkdir } from 'node:fs/promises';
import path from 'node:path';
import os from 'node:os';
import { fileURLToPath } from 'node:url';
import { parseOfficial, generate, sha256, jsonBytes, sourceUrl } from './m49-data.mjs';

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const directory = path.join(root, 'Resources/M49');
const args = process.argv.slice(2);
const options = {};
for (let i = 0; i < args.length; i += 2) {
  if (!['--html', '--version', '--cache-dir'].includes(args[i]) || !args[i + 1]) throw Error('Usage: node tools/update-m49.mjs --version YYYY-MM-DD [--html snapshot.html] [--cache-dir path]');
  options[args[i]] = args[i + 1];
}
const version = options['--version'];
if (!/^\d{4}-\d{2}-\d{2}$/.test(version ?? '')) throw Error('Specify a snapshot version with --version YYYY-MM-DD');
let html;
if (options['--html']) html = await readFile(options['--html']);
else {
  const response = await fetch(sourceUrl, { signal: AbortSignal.timeout(45_000) });
  if (!response.ok) throw Error(`Official M49 download failed: HTTP ${response.status}`);
  html = Buffer.from(await response.arrayBuffer());
  const cache = options['--cache-dir'] ?? (process.platform === 'win32' ? 'D:/BotgramBuild/downloads' : path.join(os.tmpdir(), 'botgram-downloads'));
  if (process.platform === 'win32' && !path.resolve(cache).toUpperCase().startsWith('D:\\')) throw Error('Downloaded snapshots must remain on D: on this computer');
  await mkdir(cache, { recursive: true });
  await writeFile(path.join(cache, `un-m49-${version}.html`), html);
}
const source = parseOfficial(html.toString('utf8'));
const policyBytes = await readFile(path.join(directory, 'policy.json'));
const data = generate(source, JSON.parse(policyBytes));
const sourceBytes = jsonBytes(source), dataBytes = jsonBytes(data);
const manifest = {
  snapshotVersion: version, sourceUrl, retrievedOn: version,
  note: 'Application-pinned retrieval snapshot, not a UN release number. Numeric codes/mappings from the official English M49 table; labels and explicit overrides from policy.json.',
  sourceHtmlSha256: sha256(html), sourceSha256: sha256(sourceBytes), policySha256: sha256(policyBytes),
  dataSha256: sha256(dataBytes), officialCountryCount: Object.keys(source.countries).length,
  countryCount: Object.keys(data.countries).length, selectableRegionCount: data.regions.length - 1
};
await writeFile(path.join(directory, 'source.json'), sourceBytes);
await writeFile(path.join(directory, 'botgram-m49.json'), dataBytes);
await writeFile(path.join(directory, 'botgram-m49.sha256'), manifest.dataSha256 + '\n');
await writeFile(path.join(directory, 'manifest.json'), jsonBytes(manifest));
console.log(`Updated M49 ${version}: ${manifest.selectableRegionCount} regions, ${manifest.countryCount} countries. Review and commit the snapshot, policy and pins together.`);
