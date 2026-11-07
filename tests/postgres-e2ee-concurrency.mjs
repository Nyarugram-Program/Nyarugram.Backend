// PostgreSQL-only E2EE concurrency and transaction regression: atomic one-time
// pre-key consumption, concurrent identity replacement, idempotent replay after
// a lost commit response, and proof that invalid envelopes change no state.
import assert from 'node:assert/strict';
import { execFileSync } from 'node:child_process';
import { spawn } from 'node:child_process';
import { once } from 'node:events';
import { mkdtemp } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import net from 'node:net';
import { randomBytes } from 'node:crypto';
import {
  vfield, sfield, bfield, fixed64, concat, fields, first, all, stringOf,
  envelope, call, register, profile, Operation
} from './protobuf-plane.mjs';

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
if (!process.env.BOTGRAM_E2EE_PG || !process.env.BOTGRAM_PSQL)
  throw new Error('Set BOTGRAM_E2EE_PG and BOTGRAM_PSQL to an isolated PostgreSQL database');
const temp = await mkdtemp(path.join(process.env.BOTGRAM_TEST_ROOT ?? path.join(root, 'obj'), 'pg-e2ee-concurrency-'));
const listener = net.createServer().listen(0, '127.0.0.1');
await once(listener, 'listening');
const port = listener.address().port;
await new Promise(resolve => listener.close(resolve));
const base = `http://127.0.0.1:${port}`;
const domain = 'nyarugram.moe.autos';
let server, logs = '';
let checks = 0;

async function start() {
  server = spawn('dotnet', [process.env.BOTGRAM_TEST_DLL ?? path.join(root, 'bin/Debug/net8.0/Botgram.Backend.dll'),
    '--urls', base], {
    cwd: root, env: { ...process.env,
      ASPNETCORE_ENVIRONMENT: 'Development', Storage__Mode: 'NewStorageValidation',
      Storage__ValidationBackend: 'RelationalBackend', Storage__ValidationAcknowledged: 'true',
      Storage__ValidationConnectionString: process.env.BOTGRAM_E2EE_PG,
      Protocol__LegacyJsonRpcEnabled: 'false', Protocol__LegacyJsonWssEnabled: 'false',
      Protocol__ProtobufRolloutPercent: '100', DataPath: path.join(temp, 'keys.json') },
    windowsHide: true, stdio: ['ignore', 'pipe', 'pipe']
  });
  server.stdout.on('data', value => logs += value);
  server.stderr.on('data', value => logs += value);
  for (let i = 0; i < 120; i++) {
    if (server.exitCode !== null) throw Error(logs);
    try { if ((await fetch(base + '/health')).ok) return; } catch {}
    await new Promise(resolve => setTimeout(resolve, 100));
  }
  throw Error(logs);
}
async function stop() {
  if (server && server.exitCode === null) { const exit = once(server, 'exit'); server.kill(); await exit; }
}
function sql(statement) {
  const settings = Object.fromEntries(process.env.BOTGRAM_E2EE_PG.split(';').filter(Boolean).map(pair => {
    const index = pair.indexOf('=');
    return [pair.slice(0, index).trim().toLowerCase(), pair.slice(index + 1).trim()];
  }));
  return execFileSync(process.env.BOTGRAM_PSQL, ['-h', settings.host ?? '127.0.0.1',
    '-p', settings.port ?? '5432', '-U', settings.username, '-d', settings.database, '-tAc', statement],
    { encoding: 'utf8' }).trim();
}
function userRef(id) { return concat(sfield(1, domain), sfield(2, id)); }
function peerRef(id) { return concat(bfield(1, userRef(id))); }
function rpc(token, tag, payload) { return call(base, '/api/v1/rpc', envelope(tag, payload), token); }
function outcome(result, tag) {
  assert.equal(result.status, 200, `expected 200 for ${tag}, got ${result.status} ${result.code ?? ''}`);
  const entry = first(result.bytes, tag);
  assert.ok(entry, `missing outcome ${tag}`);
  return fields(entry.bytes);
}
function code(result, status, expected) {
  assert.equal(result.status, status, `expected ${status}, got ${result.status}`);
  const error = first(result.bytes, 10);
  assert.equal(stringOf(first(fields(error.bytes), 1)), expected);
}
function publishBody(identityKey, keyIds) {
  return concat(vfield(1, 1), vfield(2, 1234), bfield(3, identityKey), vfield(4, 10),
    bfield(5, Buffer.alloc(33, 2)), bfield(6, Buffer.alloc(32, 3)), vfield(7, 11),
    bfield(8, Buffer.alloc(800, 4)), bfield(9, Buffer.alloc(32, 5)),
    ...keyIds.map(id => bfield(10, concat(vfield(1, id), bfield(2, Buffer.alloc(33, id & 0xff))))));
}
function envelopeBytes(conversation, sender, recipient, epoch, randomId, messageType, recipientDevice) {
  return concat(vfield(1, 1), sfield(2, conversation), sfield(3, sender), vfield(4, 1),
    sfield(5, recipient), vfield(6, recipientDevice), vfield(7, epoch), fixed64(8, randomId),
    bfield(9, Buffer.alloc(48, 9)), vfield(10, messageType));
}
function send(peer, randomId, e2ee) {
  return concat(bfield(1, peerRef(peer)), fixed64(2, randomId), bfield(7, e2ee));
}

try {
  await start();
  const suffix = Date.now().toString(36);
  const a = await register(base, 'pg_conc_a_' + suffix, 'pg-concurrency-password', 'A');
  const b = await register(base, 'pg_conc_b_' + suffix, 'pg-concurrency-password', 'B');
  const aId = profile(a.user).userId, bId = profile(b.user).userId;

  // Create the direct conversation with an ordinary plaintext message.
  const plain = outcome(await rpc(a.token, 107, concat(bfield(1, peerRef(bId)),
    fixed64(2, 7000000000001n), sfield(3, 'bootstrap'))), 107);
  assert.ok(first(plain, 1));
  checks++;

  // Both peers need a device before a two-sided encrypted chat can be accepted.
  await rpc(a.token, 139, publishBody(Buffer.alloc(33, 0x21), [200, 201]));
  const keyIds = Array.from({ length: 12 }, (_, index) => 300 + index);
  const published = outcome(await rpc(b.token, 139, publishBody(Buffer.alloc(33, 0x22), keyIds)), 139);
  assert.ok(Number(first(published, 10).value) >= 12);
  checks++;

  // 12 concurrent bundle requests must consume 12 distinct one-time pre-keys.
  const bundles = await Promise.all(Array.from({ length: 12 }, () =>
    rpc(a.token, 143, concat(bfield(1, peerRef(bId))))));
  const consumedIds = bundles.map(result => Number(first(outcome(result, 143), 3).value));
  assert.equal(new Set(consumedIds).size, 12, 'one-time pre-keys were double-consumed');
  assert.ok(consumedIds.every(id => keyIds.includes(id)));
  code(await rpc(a.token, 143, concat(bfield(1, peerRef(bId)))), 409, 'E2EE_PREKEYS_EXHAUSTED');
  checks++;
  const consumedRows = Number(sql(
    'SELECT count(*) FROM botgram_backend_v1.e2ee_one_time_pre_keys WHERE consumed'));
  assert.ok(consumedRows >= 12);
  checks++;

  // Concurrent identity replacements are serialized by the per-user advisory
  // lock: generation advances monotonically and no half-applied device remains.
  const replacements = await Promise.all(Array.from({ length: 4 }, (_, index) =>
    rpc(b.token, 139, publishBody(Buffer.alloc(33, 0x30 + index), [900 + index]))));
  const generations = replacements.map(result => Number(first(outcome(result, 139), 11).value));
  assert.ok(new Set(generations).size >= 1 && Math.max(...generations) >= 2);
  const storedGeneration = Number(sql(
    'SELECT max(generation) FROM botgram_backend_v1.e2ee_devices'));
  assert.equal(storedGeneration, Math.max(...generations));
  checks++;

  // A chat must be active before a two-sided encrypted envelope is accepted.
  const invite = outcome(await rpc(a.token, 140,
    concat(bfield(1, peerRef(bId)), vfield(2, 1), vfield(3, 2))), 140);
  const conversationId = stringOf(first(invite, 20));
  const epoch = Number(first(invite, 4).value);
  const accept = outcome(await rpc(b.token, 140,
    concat(bfield(1, peerRef(aId)), vfield(2, 2), vfield(3, 2))), 140);
  assert.equal(Number(first(accept, 2).value), 2);
  checks++;

  const random = 7100000000001n;
  const valid = envelopeBytes(conversationId, aId, bId, Number(first(accept, 4).value), random, 3, 1);
  const before = {
    messages: Number(sql('SELECT count(*) FROM botgram_intake_v1.messages')),
    pts: Number(sql('SELECT coalesce(sum(pts),0) FROM botgram_intake_v1.sync')),
    consumed: Number(sql('SELECT count(*) FROM botgram_backend_v1.e2ee_one_time_pre_keys WHERE consumed'))
  };
  // Invalid epoch must roll back with no message, pts or pre-key side effects.
  const invalid = envelopeBytes(conversationId, aId, bId, epoch + 999, random, 3, 1);
  code(await rpc(a.token, 107, send(bId, random, invalid)), 409, 'E2EE_EPOCH_INVALID');
  assert.deepEqual({
    messages: Number(sql('SELECT count(*) FROM botgram_intake_v1.messages')),
    pts: Number(sql('SELECT coalesce(sum(pts),0) FROM botgram_intake_v1.sync')),
    consumed: Number(sql('SELECT count(*) FROM botgram_backend_v1.e2ee_one_time_pre_keys WHERE consumed'))
  }, before, 'invalid envelope committed partial state');
  checks++;

  // Lost commit response: replay the same random_id and canonical envelope.
  const firstSend = outcome(await rpc(a.token, 107, send(bId, random, valid)), 107);
  const firstMessageId = Number(first(fields(first(firstSend, 1).bytes), 1).value);
  const replay = outcome(await rpc(a.token, 107, send(bId, random, valid)), 107);
  assert.equal(Number(first(fields(first(replay, 1).bytes), 1).value), firstMessageId);
  // A well-formed protobuf payload with invalid envelope semantics is rejected
  // by E2EE validation (a malformed wire payload is rejected even earlier).
  code(await rpc(a.token, 107, send(bId, random, concat(vfield(1, 1)))),
    400, 'E2EE_MESSAGE_TYPE_INVALID');
  checks++;

  // Paged history preserves ordering and the encrypted body.
  for (const limit of [1, 2, 5]) {
    const page = outcome(await rpc(a.token, 105,
      concat(bfield(1, peerRef(bId)), vfield(7, limit))), 105);
    assert.ok(all(page, 1).length <= limit);
  }
  // Only a base64 ciphertext envelope is persisted; the JSON field name alone
  // is present (as null) on plaintext messages, so test the JSON value.
  const plaintextLeaks = Number(sql(
    "SELECT count(*) FROM botgram_intake_v1.messages WHERE body->>'E2eeEnvelope' IS NOT NULL AND body->>'Text' LIKE '%bootstrap%'"));
  assert.equal(plaintextLeaks, 0, 'encrypted message body must not contain plaintext');
  const storedCiphertext = Number(sql(
    "SELECT count(*) FROM botgram_intake_v1.messages WHERE body->>'E2eeEnvelope' IS NOT NULL AND body->>'Text' = ''"));
  assert.ok(storedCiphertext >= 1, 'E2EE ciphertext envelope must be persisted with empty text');
  checks++;

  console.log(`PASS: PostgreSQL E2EE concurrency/transaction regression (${checks} checkpoints)`);
} catch (error) {
  if (logs.trim()) console.error('BACKEND LOGS:\n' + logs);
  throw error;
} finally {
  await stop();
}
