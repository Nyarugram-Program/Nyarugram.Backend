// Node 22+, no third-party protobuf runtime. Raw protobuf wire helpers keep this
// regression independent of generated JS code. Uses only an isolated database.
import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { once } from 'node:events';
import { mkdtemp, readFile } from 'node:fs/promises';
import { execFileSync } from 'node:child_process';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import net from 'node:net';
import { register, profile } from './protobuf-plane.mjs';

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
// BOTGRAM_E2EE_POSTGRES=1 runs the exact same protocol regression against a
// real PostgreSQL-authoritative backend (NewStorageValidation/RelationalBackend).
const postgres = process.env.BOTGRAM_E2EE_POSTGRES === '1';
const fixtureRoot = process.env.BOTGRAM_TEST_ROOT ?? path.join(root, 'obj');
const temp = await mkdtemp(path.join(fixtureRoot, 'e2ee-'));
const database = path.join(temp, 'db.json');
const listener = net.createServer().listen(0, '127.0.0.1');
await once(listener, 'listening');
const port = listener.address().port;
await new Promise(resolve => listener.close(resolve));
const base = `http://127.0.0.1:${port}`;
const domain = 'nyarugram.moe.autos';
let logs = '';
let server;
let sequence = 0;

async function start() {
  const isolated = {
    ASPNETCORE_ENVIRONMENT: 'Development',
    Storage__Mode: 'NewStorageValidation',
    Storage__ValidationBackend: 'RelationalBackend',
    Storage__ValidationAcknowledged: 'true',
    Storage__ValidationConnectionString: process.env.BOTGRAM_E2EE_PG,
    Protocol__LegacyJsonRpcEnabled: 'false',
    Protocol__LegacyJsonWssEnabled: 'false',
    Protocol__ProtobufRolloutPercent: '100',
    DataPath: path.join(temp, 'keys.json')
  };
  if (postgres && !isolated.Storage__ValidationConnectionString)
    throw new Error('BOTGRAM_E2EE_POSTGRES=1 requires BOTGRAM_E2EE_PG');
  server = spawn('dotnet', [process.env.BOTGRAM_TEST_DLL ?? path.join(root, 'bin/Debug/net8.0/Botgram.Backend.dll'),
    '--urls', base], {
    cwd: root, env: { ...process.env, ...(postgres ? isolated : { DataPath: database }) },
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
async function rest(method, route, token, body, status = 200) {
  const response = await fetch(base + route, {
    method, headers: { 'content-type': 'application/json', ...(token ? { authorization: `Bearer ${token}` } : {}) },
    body: body === undefined ? undefined : JSON.stringify(body)
  });
  const data = await response.json();
  assert.equal(response.status, status, `${method} ${route}: ${JSON.stringify(data)}`);
  return data;
}

// ---------------------------------------------------------------- protobuf
function varint(value) {
  let v = BigInt(value);
  if (v < 0n) v = (1n << 64n) + v;
  const out = [];
  while (v >= 0x80n) { out.push(Number(v & 0x7fn) | 0x80); v >>= 7n; }
  out.push(Number(v));
  return out;
}
function tag(field, wire) { return varint((field << 3) | wire); }
function vfield(field, value) { return [...tag(field, 0), ...varint(value)]; }
function sfield(field, value) { const bytes = Buffer.from(value, 'utf8'); return [...tag(field, 2), ...varint(bytes.length), ...bytes]; }
function bfield(field, bytes) { const buffer = Buffer.from(bytes); return [...tag(field, 2), ...varint(buffer.length), ...buffer]; }
function fixed64(field, value) {
  const buffer = Buffer.alloc(8);
  buffer.writeBigInt64LE(BigInt(value));
  return [...tag(field, 1), ...buffer];
}
function concat(...parts) { return Uint8Array.from(parts.flat()); }
function readVarint(buffer, offset) {
  let value = 0n, shift = 0n, index = offset;
  while (true) {
    const byte = buffer[index++];
    value |= BigInt(byte & 0x7f) << shift;
    if ((byte & 0x80) === 0) break;
    shift += 7n;
    if (shift > 63n) throw Error('varint');
  }
  return [value, index];
}
function parse(buffer) {
  const fields = new Map();
  let offset = 0;
  while (offset < buffer.length) {
    let key;
    [key, offset] = readVarint(buffer, offset);
    const field = Number(key >> 3n), wire = Number(key & 7n);
    let value;
    if (wire === 0) { let raw; [raw, offset] = readVarint(buffer, offset); value = raw; }
    else if (wire === 1) { value = Buffer.from(buffer.subarray(offset, offset + 8)).readBigInt64LE(); offset += 8; }
    else if (wire === 2) {
      let length; [length, offset] = readVarint(buffer, offset);
      value = Buffer.from(buffer.subarray(offset, offset + Number(length)));
      offset += Number(length);
    } else if (wire === 5) { value = Buffer.from(buffer.subarray(offset, offset + 4)).readUInt32LE(); offset += 4; }
    else throw Error('wire ' + wire);
    if (!fields.has(field)) fields.set(field, []);
    fields.get(field).push(value);
  }
  return fields;
}
function one(fields, field) { return fields.get(field)?.[0]; }
function num(fields, field) { const value = one(fields, field); return value === undefined ? undefined : Number(value); }
function text(fields, field) { const value = one(fields, field); return value === undefined ? undefined : value.toString('utf8'); }
function bytes(fields, field) { const value = one(fields, field); return value === undefined ? undefined : Buffer.from(value); }
function userRef(id) { return concat(sfield(1, domain), sfield(2, id)); }
function peerRef(id) { return concat(bfield(1, userRef(id))); }
function request(operationField, payload) {
  return concat(vfield(1, 1), sfield(2, 'e2ee-' + (++sequence)), bfield(operationField, payload));
}
async function protoRpc(token, operationField, payload) {
  const response = await fetch(base + '/api/v1/rpc', {
    method: 'POST',
    headers: { 'content-type': 'application/x-protobuf', accept: 'application/x-protobuf',
      ...(token ? { authorization: `Bearer ${token}` } : {}) },
    body: request(operationField, payload)
  });
  const raw = Buffer.from(await response.arrayBuffer());
  const fields = parse(raw);
  const error = one(fields, 10);
  const code = error === undefined ? undefined : text(parse(error), 1);
  return { status: response.status, fields, code, raw };
}
function publishBody(identityKey, options = {}) {
  const oneTime = [];
  for (let i = 0; i < (options.preKeys ?? 5); i++) {
    oneTime.push(...bfield(10, concat(vfield(1, 100 + i), bfield(2, Buffer.alloc(33, i + 1)))));
  }
  return concat(
    vfield(1, 1), vfield(2, 1234), bfield(3, identityKey),
    vfield(4, 10), bfield(5, Buffer.alloc(33, 2)), bfield(6, Buffer.alloc(32, 3)),
    vfield(7, 11), bfield(8, Buffer.alloc(800, 4)), bfield(9, Buffer.alloc(32, 5)), oneTime
  );
}
function actionBody(peer, action) { return concat(bfield(1, peerRef(peer)), vfield(2, action), vfield(3, 2)); }
function statusList(response, operationField) {
  const payload = one(response.fields, operationField);
  assert.ok(payload, 'status payload');
  if (operationField === 140) return [parse(payload)];
  const root = parse(payload);
  return (root.get(1) ?? []).map(value => parse(value));
}
function statusFor(response, operationField, peerUuid) {
  const chats = statusList(response, operationField);
  for (const fields of chats) {
    const peer = parse(one(fields, 1));
    fields.peerId = text(parse(one(peer, 1)), 2);
  }
  return chats.find(value => value.peerId === peerUuid);
}
function statusState(fields) { return num(fields, 2); }
function envelope(conversation, sender, recipient, epoch, randomId, cipher = Buffer.alloc(12, 9), messageType = 3, recipientDevice = 1) {
  return concat(
    vfield(1, 1), sfield(2, conversation), sfield(3, sender), vfield(4, 1),
    sfield(5, recipient), vfield(6, recipientDevice), vfield(7, epoch), fixed64(8, randomId),
    bfield(9, cipher), vfield(10, messageType)
  );
}
function sendBody(peer, randomId, textValue, envelopeBytes) {
  const fields = [bfield(1, peerRef(peer)), fixed64(2, randomId)];
  if (textValue) fields.push(sfield(3, textValue));
  if (envelopeBytes) fields.push(bfield(7, envelopeBytes));
  return concat(...fields);
}
function messageBody(response) {
  const sent = one(response.fields, 107);
  if (!sent) return null;
  const root = parse(sent);
  const message = parse(one(root, 1));
  return message;
}
function messageEnvelope(message) {
  const body = one(message, 12);
  return body === undefined ? null : parse(body);
}
function rpcCode(response, expected, code) {
  assert.equal(response.status, expected, `expected ${expected}, got ${response.status} ${response.code || ''}`);
  if (code) assert.equal(response.code, code, response.code);
}

try {
  await start();
  // Authentication now uses the Protobuf auth plane; the legacy JSON REST
  // register/login endpoints are retired (see tests/json-retirement.mjs).
  // A fresh suffix makes the regression repeatable against a persistent
  // PostgreSQL database without creating accounts in any production store.
  const suffix = postgres ? Date.now().toString(36) : '';
  const aliceAccount = await register(base, 'e2eealice' + suffix, 'e2ee-test-123', 'Alice');
  const bobAccount = await register(base, 'e2eebob' + suffix, 'e2ee-test-123', 'Bob');
  const carolAccount = await register(base, 'e2eecarol' + suffix, 'e2ee-test-123', 'Carol');
  const alice = { token: aliceAccount.token }, bob = { token: bobAccount.token }, carol = { token: carolAccount.token };
  const aliceId = profile(aliceAccount.user).userId,
        bobId = profile(bobAccount.user).userId,
        carolId = profile(carolAccount.user).userId;

  // Both peers must publish public pre-key material before an invitation can be accepted.
  const alicePublish = await protoRpc(alice.token, 139, publishBody(Buffer.alloc(33, 0x11)));
  rpcCode(alicePublish, 200);
  assert.equal(num(parse(one(alicePublish.fields, 139)), 10), 5);
  // Old action semantics are rejected explicitly, never silently reinterpreted.
  rpcCode(await protoRpc(alice.token, 140, concat(bfield(1, peerRef(bobId)), vfield(2, 1), vfield(3, 1))),
    400, 'E2EE_PROTOCOL_VERSION_UNSUPPORTED');
  // A sender can enable before the recipient has ever created a device or PIN.
  const invite = await protoRpc(alice.token, 140, actionBody(bobId, 1));
  rpcCode(invite, 200);
  const pending = statusFor(invite, 140, bobId);
  assert.equal(statusState(pending), 1);
  assert.equal(num(pending, 6), 1);
  const conversation = text(pending, 20);
  const unilateralEpoch = num(pending, 4);
  assert.ok(conversation);
  const reverseStatus = await protoRpc(bob.token, 141, concat(bfield(1, peerRef(aliceId))));
  assert.equal(num(statusFor(reverseStatus, 141, aliceId), 6) ?? 0, 0);
  rpcCode(await protoRpc(alice.token, 107, sendBody(bobId, 8001, 'plaintext leak')), 409, 'E2EE_PLAINTEXT_FORBIDDEN');
  rpcCode(await protoRpc(bob.token, 107, sendBody(aliceId, 8002, 'ordinary reverse direction')), 200);
  const unilateralEnvelope = envelope(conversation, aliceId, bobId, unilateralEpoch, 8003, Buffer.alloc(48, 9), 4, 0);
  rpcCode(await protoRpc(alice.token, 107, sendBody(bobId, 8003, null, unilateralEnvelope)), 200);
  rpcCode(await protoRpc(alice.token, 107, sendBody(bobId, 8003, null, unilateralEnvelope)), 200);
  rpcCode(await protoRpc(alice.token, 107, sendBody(bobId, 8004, null,
    envelope(conversation, aliceId, bobId, unilateralEpoch, 8004))), 400, 'E2EE_ONE_WAY_ENVELOPE_REQUIRED');
  // Dismissing/declining on the disabled side never disables the other person's setting.
  const decline = await protoRpc(bob.token, 140, actionBody(aliceId, 3));
  rpcCode(decline, 200); assert.equal(statusState(statusFor(decline, 140, aliceId)), 1);
  await stop(); await start();
  const stillOneWay = await protoRpc(alice.token, 141, concat(bfield(1, peerRef(bobId))));
  assert.equal(statusState(statusFor(stillOneWay, 141, bobId)), 1);
  const carolInvite = await protoRpc(alice.token, 140, actionBody(carolId, 1));
  rpcCode(carolInvite, 200);
  assert.equal(statusState(statusFor(carolInvite, 140, carolId)), 1);
  const bobPublish = await protoRpc(bob.token, 139, publishBody(Buffer.alloc(33, 0x22)));
  rpcCode(bobPublish, 200);
  const accept = await protoRpc(bob.token, 140, actionBody(aliceId, 2));
  rpcCode(accept, 200);
  const active = statusFor(accept, 140, aliceId);
  assert.equal(statusState(active), 2);
  assert.ok(num(active, 21) > 0);
  const epoch = num(active, 4);

  // New one-sided ciphertext is invalid once both directions are enabled; old records remain unchanged.
  rpcCode(await protoRpc(alice.token, 107, sendBody(bobId, 8200, null,
    envelope(conversation, aliceId, bobId, epoch, 8200, Buffer.alloc(48, 9), 4, 0))), 409, 'E2EE_ONE_WAY_NOT_ALLOWED');
  // A plaintext message must be rejected once active, while a proper envelope succeeds.
  rpcCode(await protoRpc(alice.token, 107, sendBody(bobId, 1001, 'plaintext leak')), 409, 'E2EE_PLAINTEXT_FORBIDDEN');
  const random = 1002;
  const envelopeBytes = envelope(conversation, aliceId, bobId, epoch, random);
  const sent = await protoRpc(alice.token, 107, sendBody(bobId, random, null, envelopeBytes));
  rpcCode(sent, 200);
  const stored = messageBody(sent);
  const storedEnvelope = messageEnvelope(stored);
  assert.ok(storedEnvelope);
  assert.equal(num(storedEnvelope, 7), epoch);
  assert.equal(bytes(storedEnvelope, 9).length, 12);
  assert.equal(bytes(storedEnvelope, 9)[0], 9);

  // Idempotent replay returns the same message; a changed ciphertext conflicts.
  const replay = await protoRpc(alice.token, 107, sendBody(bobId, random, null, envelopeBytes));
  rpcCode(replay, 200);
  const conflictEnvelope = envelope(conversation, aliceId, bobId, epoch, random, Buffer.alloc(12, 7));
  rpcCode(await protoRpc(alice.token, 107, sendBody(bobId, random, null, conflictEnvelope)), 409, 'RANDOM_ID_DUPLICATE');
  rpcCode(await protoRpc(alice.token, 107,
    sendBody(bobId, 1003, null, envelope('other-conversation', aliceId, bobId, epoch, 1003))), 409, 'E2EE_ENVELOPE_CONTEXT_INVALID');

  // Pre-key consumption is one-time and survives a restart.
  const bundle = await protoRpc(alice.token, 143, concat(bfield(1, peerRef(bobId))));
  rpcCode(bundle, 200);
  const bundleFields = parse(one(bundle.fields, 143));
  assert.ok(num(bundleFields, 3) >= 100);
  await stop(); await start();
  const bundle2 = await protoRpc(alice.token, 143, concat(bfield(1, peerRef(bobId))));
  rpcCode(bundle2, 200);
  const bundleFields2 = parse(one(bundle2.fields, 143));
  assert.notEqual(num(bundleFields2, 3), num(bundleFields, 3));

  // Each side disables only itself; the remaining enabled direction stays protected.
  rpcCode(await protoRpc(alice.token, 140, actionBody(bobId, 5)), 200);
  const closing = await protoRpc(alice.token, 141, concat(bfield(1, peerRef(bobId))));
  rpcCode(closing, 200);
  assert.equal(statusState(statusFor(closing, 141, bobId)), 1);
  assert.equal(num(statusFor(closing, 141, bobId), 6) ?? 0, 0);
  rpcCode(await protoRpc(alice.token, 107, sendBody(bobId, 8100, 'ordinary after disabling only self')), 200);
  rpcCode(await protoRpc(bob.token, 107, sendBody(aliceId, 8101, 'plaintext leak')), 409, 'E2EE_PLAINTEXT_FORBIDDEN');
  const bobOnly = await protoRpc(bob.token, 141, concat(bfield(1, peerRef(aliceId))));
  const bobOnlyState = statusFor(bobOnly, 141, aliceId);
  assert.equal(num(bobOnlyState, 6), 1);
  rpcCode(await protoRpc(bob.token, 107, sendBody(aliceId, 8102, null,
    envelope(conversation, bobId, aliceId, num(bobOnlyState, 4), 8102, Buffer.alloc(48, 7), 4, 0))), 200);
  const closed = await protoRpc(bob.token, 140, actionBody(aliceId, 6));
  rpcCode(closed, 200);
  assert.equal(statusState(statusFor(closed, 140, aliceId)), 5);
  const plainAfterClose = await protoRpc(alice.token, 107, sendBody(bobId, 1004, 'after close'));
  rpcCode(plainAfterClose, 200);
  rpcCode(await protoRpc(alice.token, 107,
    sendBody(bobId, 1005, null, envelope(conversation, aliceId, bobId, epoch, 1005))), 409, 'E2EE_NOT_ACTIVE');

  // Re-invite after close creates a new epoch.
  const reInvite = await protoRpc(alice.token, 140, actionBody(bobId, 1));
  rpcCode(reInvite, 200);
  const rePending = statusFor(reInvite, 140, bobId);
  assert.equal(statusState(rePending), 1);
  const reAccept = await protoRpc(bob.token, 140, actionBody(aliceId, 2));
  rpcCode(reAccept, 200);
  const reActive = statusFor(reAccept, 140, aliceId);
  assert.equal(statusState(reActive), 2);
  const epoch2 = num(reActive, 4);
  assert.ok(epoch2 > epoch);

  // A new identity key suspends the conversation until both peers confirm.
  const replaced = await protoRpc(bob.token, 139, publishBody(Buffer.alloc(33, 0x33)));
  rpcCode(replaced, 200);
  const suspendedResponse = await protoRpc(alice.token, 141, concat(bfield(1, peerRef(bobId))));
  rpcCode(suspendedResponse, 200);
  const suspended = statusFor(suspendedResponse, 141, bobId);
  assert.equal(statusState(suspended), 4);
  assert.equal(num(suspended, 9), 1);
  rpcCode(await protoRpc(alice.token, 107,
    sendBody(bobId, 1006, null, envelope(conversation, aliceId, bobId, epoch2, 1006))), 409, 'E2EE_NOT_ACTIVE');
  const aliceConfirm = await protoRpc(alice.token, 140, actionBody(bobId, 7));
  rpcCode(aliceConfirm, 200);
  assert.equal(statusState(statusFor(aliceConfirm, 140, bobId)), 4);
  const bobConfirm = await protoRpc(bob.token, 140, actionBody(aliceId, 7));
  rpcCode(bobConfirm, 200);
  const reactivated = statusFor(bobConfirm, 140, aliceId);
  assert.equal(statusState(reactivated), 2);
  const epoch3 = num(reactivated, 4);
  const afterIdentity = envelope(conversation, aliceId, bobId, epoch3, 1007);
  rpcCode(await protoRpc(alice.token, 107, sendBody(bobId, 1007, null, afterIdentity)), 200);

  // History/difference exposes only the envelope, never plaintext, and service markers.
  const history = await protoRpc(alice.token, 105, concat(bfield(1, peerRef(bobId)), vfield(7, 50)));
  rpcCode(history, 200);
  const page = parse(one(history.fields, 105));
  const messages = page.get(1).map(value => parse(value));
  const encrypted = messages.filter(value => one(value, 12) !== undefined);
  assert.ok(encrypted.length >= 2);
  for (const message of encrypted) assert.equal(one(message, 10), undefined);
  const service = messages.filter(value => one(value, 11) !== undefined);
  const serviceText = service.map(value => {
    const action = one(parse(one(value, 11)), 2);
    return action === undefined ? undefined : text(parse(action), 1);
  });
  assert.ok(serviceText.includes('e2ee:activated'));
  assert.ok(serviceText.includes('e2ee:one_way_enabled'));
  const oldSealed = encrypted.map(value => parse(one(value, 12))).find(value => num(value, 10) === 4 && text(value, 3) === aliceId);
  // Enabling later never replaces the earlier envelope or adds a recipient key.
  assert.ok(oldSealed);
  assert.equal(num(oldSealed, 10), 4);
  assert.equal(num(oldSealed, 6) ?? 0, 0);
  assert.ok(bytes(oldSealed, 9).equals(Buffer.alloc(48, 9)));
  assert.ok(serviceText.includes('e2ee:identity_changed') || serviceText.includes('e2ee:reactivated'));

  const diffStart = await protoRpc(bob.token, 100, new Uint8Array());
  rpcCode(diffStart, 200);
  // The active status is readable after restart as well.
  const status = await protoRpc(alice.token, 141, concat(bfield(1, peerRef(bobId))));
  rpcCode(status, 200);
  assert.equal(statusState(statusFor(status, 141, bobId)), 2);
  await stop(); await start();
  const statusAfterRestart = await protoRpc(alice.token, 141, concat(bfield(1, peerRef(bobId))));
  rpcCode(statusAfterRestart, 200);
  assert.equal(statusState(statusFor(statusAfterRestart, 141, bobId)), 2);

  // Legacy database fields stay absent and no plaintext appears in the snapshot.
  if (postgres) {
    // Same invariants against the PostgreSQL authority: ciphertext envelopes
    // live in messages.body jsonb, device/session state in dedicated v9 tables,
    // and no decrypted text is ever persisted on the server.
    if (!process.env.BOTGRAM_PSQL) throw new Error('BOTGRAM_E2EE_POSTGRES=1 requires BOTGRAM_PSQL');
    const settings = Object.fromEntries(process.env.BOTGRAM_E2EE_PG.split(';').filter(Boolean).map(pair => {
      const index = pair.indexOf('=');
      return [pair.slice(0, index).trim().toLowerCase(), pair.slice(index + 1).trim()];
    }));
    const args = ['-h', settings.host ?? '127.0.0.1', '-p', settings.port ?? '5432',
      '-U', settings.username, '-d', settings.database, '-tAc'];
    const query = sql => Number(execFileSync(process.env.BOTGRAM_PSQL, [...args, sql], { encoding: 'utf8' }).trim());
    assert.ok(query("SELECT count(*) FROM botgram_intake_v1.messages WHERE body::text ILIKE '%E2eeEnvelope%'") > 0);
    assert.equal(query("SELECT count(*) FROM botgram_intake_v1.messages WHERE body::text LIKE '%plaintext leak%'"), 0);
    assert.ok(query("SELECT count(*) FROM botgram_backend_v1.e2ee_devices") >= 2);
    assert.ok(query("SELECT count(*) FROM botgram_backend_v1.e2ee_conversations") >= 1);
    assert.ok(query("SELECT count(*) FROM botgram_backend_v1.e2ee_one_time_pre_keys") >= 1);
  } else {
    const snapshot = await readFile(database, 'utf8');
    assert.ok(!snapshot.includes('plaintext leak'));
    assert.ok(snapshot.toLowerCase().includes('e2eeenvelope'));
  }

  console.log(`PASS: directional E2EE without recipient keys, normal reverse direction, persistent old placeholders, independent disabling, protocol version guard, Signal activation, identity and restart (${postgres ? 'PostgreSQL' : 'legacy file'} authority)`);
} catch (error) {
  if (logs.trim()) console.error('BACKEND LOGS:\n' + logs);
  throw error;
} finally {
  await stop();
}



