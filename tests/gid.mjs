// Legacy GID REST suite after JSON API retirement.
// The M49 snapshot remains an offline, shipped asset; its REST listing and
// the /api/me/gid mutation are retired. Positive GID coverage (region mapping,
// change, stable UUID/TL id) lives in tests/protobuf-business.mjs.
import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import { createHash } from 'node:crypto';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { isolatedBackend, assertRetired, RouteGroups } from './retirement-lib.mjs';
import { register, profile, Operation, envelope, call, ACCOUNT, sfield, concat } from './protobuf-plane.mjs';

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const data = await readFile(path.join(root, 'Resources/M49/botgram-m49.json'));
const manifest = JSON.parse(await readFile(path.join(root, 'Resources/M49/manifest.json'), 'utf8'));
const snapshot = JSON.parse(data.toString('utf8'));
assert.equal(createHash('sha256').update(data).digest('hex').toLowerCase(), manifest.dataSha256.toLowerCase());
assert.equal(snapshot.regions.filter(region => region.code !== '001').length, 29);

const host = await isolatedBackend('gid-retired');
let checks = 2;
try {
  for (const route of ['/api/gid/regions', '/api/me/gid']) {
    for (const method of ['GET', 'POST', 'PUT', 'DELETE', 'PATCH', 'HEAD', 'OPTIONS']) {
      await assertRetired(host.base, route, { method, authorization: 'Bearer invalid-token' });
      checks++;
    }
  }
  const before = await host.state();
  for (const route of RouteGroups.security) await assertRetired(host.base, route, { method: 'POST' });
  assert.equal(await host.state(), before);
  checks++;

  // Protobuf registration still maps a country to the most specific M49 region.
  const account = await register(host.base, 'gid_proto_' + Date.now().toString(36),
    'protobuf-gid-password', 'Gid', 'CN');
  assert.match(profile(account.user).gid, /^030\d{8}$/);
  checks++;
  const changed = await call(host.base, ACCOUNT, envelope(Operation.changeGid,
    concat(sfield(1, '999'), sfield(2, '00000000'))), account.token);
  // An invalid region is a typed error, never a JSON fallback.
  assert.equal(changed.status, 400);
  checks++;
  console.log(`PASS: retired GID REST suite and offline M49 snapshot (${checks} assertions)`);
} catch (error) {
  if (host.logs().trim()) console.error('BACKEND LOGS:\n' + host.logs());
  throw error;
} finally {
  await host.stop();
}
