// Retirement matrix for every legacy JSON HTTP route and the JSON WSS subprotocol.
// Runs against an isolated LegacyOnly instance. No production data is touched.
import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { once } from 'node:events';
import { mkdtemp, readFile, stat } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import net from 'node:net';
import { capabilities, register, logout, Operation, outcome, first, stringOf, envelope, call, AUTH, ACCOUNT } from './protobuf-plane.mjs';

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const fixtureRoot = process.env.BOTGRAM_TEST_ROOT ?? path.join(root, 'obj');
const temp = await mkdtemp(path.join(fixtureRoot, 'retirement-'));
const database = path.join(temp, 'db.json');
const listener = net.createServer().listen(0, '127.0.0.1');
await once(listener, 'listening');
const port = listener.address().port;
await new Promise(resolve => listener.close(resolve));
const base = `http://127.0.0.1:${port}`;
const CODE = 'BOTGRAM_JSON_API_RETIRED';
let server, logs = '';
let checks = 0;

async function start(extra = {}) {
  server = spawn('dotnet', [process.env.BOTGRAM_TEST_DLL ?? path.join(root, 'bin/Debug/net8.0/Botgram.Backend.dll'),
    '--urls', base], {
    cwd: root, env: { ...process.env, DataPath: database, ...extra }, windowsHide: true,
    stdio: ['ignore', 'pipe', 'pipe']
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
  server = null;
}
async function snapshotState() {
  try { return (await readFile(database)).toString('utf8'); } catch { return null; }
}

const retired = [
  '/api/auth/register', '/api/auth/login', '/api/auth/password', '/api/auth/logout',
  '/api/security/totp', '/api/security/totp/setup', '/api/security/totp/confirm',
  '/api/security/totp/disable', '/api/security/totp/recovery',
  '/api/telegram/me', '/api/telegram/rpc', '/api/gid/regions', '/api/me/gid',
  '/api/protocol/capabilities', '/api/me', '/api/me/premium', '/api/me/premium/activate',
  '/api/users', '/api/conversations', '/api/conversations/abc/messages',
  '/api/conversations/abc/read', '/api/events', '/api/rtc/config',
  '/api/calls', '/api/calls/abc', '/api/calls/abc/accept', '/api/calls/abc/signals'
];
const methods = ['GET', 'POST', 'PUT', 'DELETE', 'PATCH', 'HEAD', 'OPTIONS'];

async function assertRetired(route, options = {}) {
  const response = await fetch(base + route, {
    method: options.method ?? 'POST',
    headers: { ...(options.contentType === null ? {} : { 'content-type': options.contentType ?? 'application/json' }),
      ...(options.authorization ? { authorization: options.authorization } : {}) },
    body: options.method === 'GET' || options.method === 'HEAD' ? undefined : (options.body ?? '{}')
  });
  const text = await response.text();
  checks++;
  assert.equal(response.status, 410, `${options.method ?? 'POST'} ${route} status: ${text.slice(0, 120)}`);
  assert.match(response.headers.get('content-type') ?? '', /^text\/plain/, `${route} must not be JSON`);
  assert.equal(response.headers.get('x-botgram-json-api-retired'), CODE);
  assert.equal(response.headers.get('deprecation'), 'true');
  assert.ok(response.headers.get('sunset'));
  if (options.method !== 'HEAD') {
    assert.ok(text.startsWith(CODE), `${route} body must start with the retirement code`);
    assert.ok(!text.trimStart().startsWith('{'), `${route} body must not be JSON`);
  }
}

async function rawUpgrade(protocols, authorization) {
  const socket = net.connect(port, '127.0.0.1');
  await once(socket, 'connect');
  return new Promise((resolve, reject) => {
    let buffer = '';
    const timer = setTimeout(() => { socket.destroy(); reject(new Error('upgrade timeout')); }, 5000);
    socket.on('data', chunk => {
      buffer += chunk.toString('latin1');
      if (buffer.includes('\r\n\r\n')) {
        clearTimeout(timer); socket.destroy();
        const head = buffer.split('\r\n\r\n')[0];
        const status = Number((head.match(/^HTTP\/1\.1 (\d+)/) ?? [])[1] ?? 0);
        resolve({ status, head, body: buffer.split('\r\n\r\n')[1] ?? '' });
      }
    });
    socket.on('error', error => { clearTimeout(timer); reject(error); });
    const headers = [
      `GET /api/ws?after=0 HTTP/1.1`, `Host: 127.0.0.1:${port}`,
      'Connection: Upgrade', 'Upgrade: websocket',
      'Sec-WebSocket-Version: 13', 'Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==',
      ...(protocols ? [`Sec-WebSocket-Protocol: ${protocols}`] : []),
      ...(authorization ? [`Authorization: ${authorization}`] : []),
      '', ''
    ];
    socket.write(headers.join('\r\n'));
  });
}

try {
  await start();
  // Health probe uses the same non-JSON format and stays usable by probes.
  const health = await fetch(base + '/health');
  const healthText = await health.text();
  checks++;
  assert.equal(health.status, 200);
  assert.match(health.headers.get('content-type'), /^text\/plain/);
  assert.match(healthText, /status=ok/);
  assert.match(healthText, new RegExp(CODE));
  assert.ok(!healthText.trimStart().startsWith('{'));
  // The probe is a key/value format consumed by monitors: every key must be
  // unique, otherwise last-wins parsers silently drop a backend-specific field.
  const healthKeys = healthText.trim().split('\n').map(line => line.split('=', 1)[0]);
  assert.equal(new Set(healthKeys).size, healthKeys.length, `duplicate health keys: ${healthKeys.join(',')}`);
  assert.equal(healthKeys.filter(key => key === 'json_api').length, 1);
  assert.equal(healthKeys.filter(key => key === 'storage').length, 1);

  const before = await snapshotState();

  // Every retired route, every method, with and without credentials, with JSON
  // and Protobuf content types, trailing slashes, duplicate slashes and case.
  for (const route of retired) {
    for (const method of methods) {
      await assertRetired(route, { method });
      await assertRetired(route, { method, authorization: 'Bearer invalid-token', contentType: 'application/x-protobuf' });
    }
    await assertRetired(route + '/', { method: 'POST', authorization: 'Bearer invalid-token' });
    await assertRetired(route.replace('/api/', '/api//'), { method: 'POST', contentType: 'application/json; charset=utf-8' });
    await assertRetired(route.toUpperCase().replace('/API/', '/api/'), { method: 'POST', contentType: null });
  }

  // Protobuf endpoints must never sniff or accept a JSON body.
  for (const path of ['/api/v1/rpc', AUTH, ACCOUNT]) {
    const response = await fetch(base + path, { method: 'POST',
      headers: { 'content-type': 'application/json', accept: 'application/json' }, body: '{"username":"x"}' });
    checks++;
    assert.equal(response.status, 415, `${path} must reject a JSON body`);
    assert.match(response.headers.get('content-type') ?? '', /^text\/plain/);
    assert.ok((await response.text()).includes('BOTGRAM_MEDIA_TYPE_UNSUPPORTED'));
  }

  // Protobuf capability discovery is the non-JSON replacement.
  const caps = await capabilities(base);
  const capBytes = outcome(caps, Operation.capabilities);
  checks++;
  assert.equal(stringOf(first(capBytes, 6)), CODE);
  assert.equal(stringOf(first(capBytes, 3)), 'protobuf-v1');
  assert.ok(capBytes.length > 0);

  // JSON WSS negotiation is rejected before upgrade, with and without auth.
  for (const protocol of ['botgram.events.v1.json', 'botgram.event-hint.v1.protobuf, botgram.events.v1.json']) {
    for (const authorization of [null, 'Bearer invalid-token']) {
      const result = await rawUpgrade(protocol, authorization);
      checks++;
      assert.equal(result.status, 410, `JSON WSS ${protocol} must be 410`);
      assert.match(result.head, /x-botgram-json-api-retired/i);
    }
  }
  const bare = await rawUpgrade(null, null);
  checks++;
  assert.equal(bare.status, 410, 'WSS without the Protobuf subprotocol must be 410');

  // No retired request may write to the authoritative store.
  const after = await snapshotState();
  checks++;
  assert.equal(after, before, 'Rejected requests must not modify the database');

  // A valid Protobuf session can still use the Protobuf WSS channel.
  const account = await register(base, 'retirement_user', 'retirement-password-123', 'Retirement');
  const protobufWs = await rawUpgrade('botgram.event-hint.v1.protobuf', `Bearer ${account.token}`);
  checks++;
  assert.equal(protobufWs.status, 101, `Protobuf WSS must upgrade: ${protobufWs.head.slice(0, 160)}`);
  assert.match(protobufWs.head, /botgram\.event-hint\.v1\.protobuf/);

  // Logout through Protobuf works and the account cannot be re-used afterwards.
  const ended = await logout(base, account.token);
  checks++;
  assert.equal(ended.status, 200);
  const reused = await call(base, ACCOUNT, envelope(Operation.me), account.token);
  checks++;
  assert.equal(reused.status, 401);

  await stop();

  // Legacy switches, rollout settings and storage mode cannot re-enable JSON.
  for (const extra of [
    { Protocol__LegacyJsonRpcEnabled: 'true', Protocol__LegacyJsonWssEnabled: 'true', Protocol__ProtobufRolloutPercent: '100' },
    { Protocol__LegacyJsonRpcEnabled: 'true', Protocol__LegacyJsonWssEnabled: 'false', Protocol__ProtobufRolloutPercent: '0' },
    { Storage__Mode: 'LegacyOnly', Protocol__UsageLogEnabled: 'true' }
  ]) {
    await start(extra);
    await assertRetired('/api/auth/login', { method: 'POST', authorization: 'Bearer invalid-token' });
    await assertRetired('/api/telegram/rpc', { method: 'POST', contentType: 'application/x-protobuf' });
    await stop();
  }

  console.log(`PASS: JSON API retirement matrix (${checks} assertions, ${retired.length} routes x ${methods.length} methods)`);
} finally {
  await stop();
}
