// End-to-end HTTP integration for the new client: Protobuf capability
// negotiation, registration, login, password info, authenticator/recovery,
// premium, GID, ordinary chat, idempotency, difference and logout.
// Set BOTGRAM_BUSINESS_POSTGRES=1 with BOTGRAM_E2EE_PG to run against a real
// PostgreSQL-authoritative backend instead of the isolated legacy file store.
import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { once } from 'node:events';
import { mkdtemp } from 'node:fs/promises';
import { createHmac } from 'node:crypto';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import net from 'node:net';
import {
  varint, vfield, sfield, bfield, fixed64, concat, fields, first, all, stringOf,
  requestId, envelope, call, expectError, outcome, Operation, AUTH, ACCOUNT,
  credentials, register, login, capabilities, logout, premium, me, totp, changeGid, profile
} from './protobuf-plane.mjs';

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const postgres = process.env.BOTGRAM_BUSINESS_POSTGRES === '1';
const temp = await mkdtemp(path.join(process.env.BOTGRAM_TEST_ROOT ?? path.join(root, 'obj'), 'pb-business-'));
const database = path.join(temp, 'db.json');
const listener = net.createServer().listen(0, '127.0.0.1');
await once(listener, 'listening');
const port = listener.address().port;
await new Promise(resolve => listener.close(resolve));
const base = `http://127.0.0.1:${port}`;
const domain = 'nyarugram.moe.autos';
let server, logs = '';
let checks = 0;

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
const ok = () => { checks++; };
// proto3 omits default-valued scalars on the wire.
const num = (list, field) => { const entry = first(list, field); return entry ? Number(entry.value) : 0; };

function userRef(id) { return concat(sfield(1, domain), sfield(2, id)); }
function peerRef(id) { return concat(bfield(1, userRef(id))); }
function business(token, operationField, payload) {
  return call(base, '/api/v1/rpc', envelope(operationField, payload), token);
}
function rpcError(result, status, code) {
  expectError(result, status, code);
  ok();
}
function sendBody(peer, randomId, text) {
  return concat(bfield(1, peerRef(peer)), fixed64(2, randomId), sfield(3, text));
}
function messageId(result) {
  const sent = outcome(result, 107);
  const root = fields(sent);
  const message = fields(first(root, 1).bytes);
  return num(message, 1);
}
function base32Decode(value) {
  const alphabet = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ234567';
  let bits = 0, buffer = 0;
  const output = [];
  for (const character of value.toUpperCase().replace(/=+$/, '')) {
    const index = alphabet.indexOf(character);
    if (index < 0) continue;
    buffer = (buffer << 5) | index; bits += 5;
    if (bits >= 8) { bits -= 8; output.push((buffer >> bits) & 0xff); }
  }
  return Buffer.from(output);
}
function totpCode(secret, step) {
  const counter = Buffer.alloc(8);
  counter.writeBigUInt64BE(BigInt(step));
  const digest = createHmac('sha1', base32Decode(secret)).update(counter).digest();
  const offset = digest[digest.length - 1] & 15;
  return ((digest.readUInt32BE(offset) & 0x7fffffff) % 1_000_000).toString().padStart(6, '0');
}

try {
  await start();
  const suffix = Date.now().toString(36);
  const userA = 'pb_business_a_' + suffix, userB = 'pb_business_b_' + suffix;
  const password = 'protobuf-business-password';

  // 1. Capability discovery is Protobuf-only and advertises the retirement.
  const caps = await capabilities(base);
  const cap = fields(outcome(caps, Operation.capabilities));
  assert.equal(num(cap, 1), 1);
  assert.equal(stringOf(first(cap, 3)), 'protobuf-v1');
  assert.equal(stringOf(first(cap, 6)), 'BOTGRAM_JSON_API_RETIRED');
  assert.ok(all(cap, 2).some(f => stringOf(f) === 'protobuf-v1'));
  assert.ok(all(cap, 5).some(f => stringOf(f) === 'send-random-id-idempotency-v1'));
  ok();

  // 2. Registration with an explicit M49 country and the World fallback.
  const a = await register(base, userA, password, 'Account A', 'CN');
  const b = await register(base, userB, password, 'Account B');
  const aProfile = profile(a.user), bProfile = profile(b.user);
  assert.match(aProfile.gid, /^030\d{8}$/);
  assert.match(bProfile.gid, /^001\d{8}$/);
  assert.equal(aProfile.username, userA);
  assert.equal(aProfile.self, true);
  assert.ok(aProfile.tlUserId > 0);
  ok();

  // Duplicate username and wrong password are explicit typed errors.
  rpcError(await call(base, AUTH, envelope(Operation.register, credentials(userA, password, 'Duplicate'))),
    409, 'username_taken');
  rpcError(await call(base, AUTH, envelope(Operation.login,
    concat(sfield(1, userA), sfield(2, 'wrong-password')))), 401, 'invalid_credentials');

  // 3. Login and account profile round-trip.
  const relogin = await login(base, userA, password);
  assert.equal(profile(relogin.user).userId, aProfile.userId);
  assert.equal(profile(relogin.user).tlUserId, aProfile.tlUserId);
  const accountMe = await me(base, relogin.token);
  assert.equal(profile(outcome(accountMe, Operation.me)).userId, aProfile.userId);
  ok();

  // 4. Password info returns a serialized PasswordInfo, not a JSON object.
  const passwordInfo = await call(base, AUTH, envelope(Operation.password,
    concat(sfield(1, userA), sfield(2, password))));
  const info = fields(outcome(passwordInfo, Operation.password));
  assert.ok(first(info, 1).bytes.length > 0);
  ok();

  // 5. Premium activation is confirmed by the server before the client unlocks.
  assert.equal(num(fields(outcome(await premium(base, relogin.token), Operation.premium)), 1), 0);
  const activated = await premium(base, relogin.token, true);
  assert.equal(num(fields(outcome(activated, Operation.premium)), 1), 1);
  assert.equal(profile(outcome(await me(base, relogin.token), Operation.me)).premium, true);
  ok();

  // 6. GID change keeps the stable TL numeric identity and UUID.
  // A randomized suffix keeps the test repeatable against a persistent
  // PostgreSQL database (GIDs are unique across the whole authority).
  const gidNumber = String(10000000 + Math.floor(Math.random() * 89999999));
  const changed = await changeGid(base, relogin.token, '030', gidNumber);
  const changedProfile = profile(outcome(changed, Operation.changeGid));
  assert.equal(changedProfile.gid, '030' + gidNumber);
  assert.equal(changedProfile.userId, aProfile.userId);
  assert.equal(changedProfile.tlUserId, aProfile.tlUserId);
  ok();

  // 7. Authenticator setup, confirmation, recovery codes and one-time recovery login.
  const setup = fields(outcome(await totp(base, relogin.token, 'setup'), Operation.totp));
  const secret = stringOf(first(setup, 3));
  assert.ok(secret.length >= 16);
  assert.ok(stringOf(first(setup, 4)).startsWith('otpauth://totp/'));
  const step = Math.floor(Date.now() / 1000 / 30);
  const confirmed = fields(outcome(await totp(base, relogin.token, 'confirm', totpCode(secret, step)), Operation.totp));
  const codes = all(confirmed, 6).map(stringOf);
  assert.equal(codes.length, 10);
  assert.equal(num(confirmed, 2), 10);
  const status = fields(outcome(await totp(base, relogin.token, 'status'), Operation.totp));
  assert.equal(num(status, 1), 1);
  const recovered = await call(base, AUTH, envelope(Operation.recover,
    concat(sfield(1, userA), sfield(2, password), sfield(3, codes[0]))));
  assert.equal(stringOf(first(fields(outcome(recovered, Operation.recover)), 1)).length, 64);
  // Reusing a consumed recovery code must fail.
  rpcError(await call(base, AUTH, envelope(Operation.recover,
    concat(sfield(1, userA), sfield(2, password), sfield(3, codes[0])))), 400, 'TOTP_CODE_INVALID');
  ok();

  // 8. Ordinary chat, idempotent replay, conflict, history paging and difference.
  const random = 9000000000001n;
  const firstSend = await business(a.token, 107, sendBody(bProfile.userId, random, 'hello over protobuf'));
  const firstId = messageId(firstSend);
  ok();
  const replay = await business(a.token, 107, sendBody(bProfile.userId, random, 'hello over protobuf'));
  assert.equal(messageId(replay), firstId);
  rpcError(await business(a.token, 107, sendBody(bProfile.userId, random, 'different body')), 409, 'RANDOM_ID_DUPLICATE');
  const second = await business(b.token, 107, sendBody(aProfile.userId, random + 1n, 'reply over protobuf'));
  assert.ok(messageId(second) > firstId);
  const history = fields(outcome(await business(a.token, 105,
    concat(bfield(1, peerRef(bProfile.userId)), vfield(7, 50))), 105));
  assert.ok(all(history, 1).length >= 2, 'paged history must contain both messages');
  const state = fields(outcome(await business(a.token, 100, new Uint8Array()), 100));
  const pts = num(state, 1);
  assert.ok(pts >= 1);
  const difference = fields(outcome(await business(b.token, 101, vfield(1, 1)), 101));
  assert.ok(difference.length > 0);
  ok();

  // 9. Logout invalidates the bearer session; no JSON fallback exists.
  assert.equal(num(fields(outcome(await logout(base, a.token), Operation.logout)), 1), 1);
  rpcError(await call(base, ACCOUNT, envelope(Operation.me), a.token), 401, 'session_expired');
  const oldJson = await fetch(base + '/api/auth/login', { method: 'POST',
    headers: { 'content-type': 'application/json' }, body: '{}' });
  assert.equal(oldJson.status, 410);
  ok();

  await stop();
  // Restart: sessions, GID and messages must survive.
  await start();
  const afterRestart = await login(base, userA, password, { totpCode: codes[1] });
  assert.equal(profile(afterRestart.user).tlUserId, aProfile.tlUserId);
  assert.equal(profile(afterRestart.user).gid, '030' + gidNumber);
  const persisted = fields(outcome(await business(afterRestart.token, 105,
    concat(bfield(1, peerRef(bProfile.userId)), vfield(7, 50))), 105));
  assert.ok(all(persisted, 1).length >= 2);
  ok();

  console.log(`PASS: Protobuf authentication plane + ordinary chat (${checks} checkpoints, ${postgres ? 'PostgreSQL' : 'legacy file'} authority)`);
} catch (error) {
  if (logs.trim()) console.error('BACKEND LOGS:\n' + logs);
  throw error;
} finally {
  await stop();
}
