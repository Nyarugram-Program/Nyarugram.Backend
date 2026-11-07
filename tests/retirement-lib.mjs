// Shared harness for legacy JSON suites that now verify retirement rather than
// exercising business behaviour. Business regressions live in
// protobuf-business.mjs, e2ee.mjs and the PostgreSQL variants.
import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { once } from 'node:events';
import { mkdtemp, readFile } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import net from 'node:net';

export const CODE = 'BOTGRAM_JSON_API_RETIRED';
const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');

export const RouteGroups = {
  auth: ['/api/auth/register', '/api/auth/login', '/api/auth/password', '/api/auth/logout', '/api/me', '/api/users'],
  conversations: ['/api/conversations', '/api/conversations/abc/messages', '/api/conversations/abc/read', '/api/events'],
  rpc: ['/api/telegram/rpc', '/api/telegram/me', '/api/protocol/capabilities'],
  security: ['/api/security/totp', '/api/security/totp/setup', '/api/security/totp/confirm',
    '/api/gid/regions', '/api/me/gid', '/api/me/premium', '/api/me/premium/activate'],
  calls: ['/api/rtc/config', '/api/calls', '/api/calls/abc', '/api/calls/abc/accept', '/api/calls/abc/signals']
};

export async function isolatedBackend(label) {
  const temp = await mkdtemp(path.join(process.env.BOTGRAM_TEST_ROOT ?? path.join(root, 'obj'), label + '-'));
  const database = path.join(temp, 'db.json');
  const listener = net.createServer().listen(0, '127.0.0.1');
  await once(listener, 'listening');
  const port = listener.address().port;
  await new Promise(resolve => listener.close(resolve));
  const base = `http://127.0.0.1:${port}`;
  let logs = '';
  const server = spawn('dotnet', [process.env.BOTGRAM_TEST_DLL ?? path.join(root, 'bin/Debug/net8.0/Botgram.Backend.dll'),
    '--urls', base], {
    cwd: root, env: { ...process.env, DataPath: database }, windowsHide: true,
    stdio: ['ignore', 'pipe', 'pipe']
  });
  server.stdout.on('data', value => logs += value);
  server.stderr.on('data', value => logs += value);
  for (let i = 0; i < 120; i++) {
    if (server.exitCode !== null) throw new Error(logs);
    try { if ((await fetch(base + '/health')).ok) break; } catch {}
    await new Promise(resolve => setTimeout(resolve, 100));
    if (i === 119) throw new Error('Startup timeout: ' + logs);
  }
  return {
    base, database, port, logs: () => logs,
    async state() { try { return (await readFile(database)).toString('utf8'); } catch { return null; } },
    async stop() { if (server.exitCode === null) { const exit = once(server, 'exit'); server.kill(); await exit; } }
  };
}

export async function assertRetired(base, route, options = {}) {
  const method = options.method ?? 'POST';
  const response = await fetch(base + route, {
    method,
    headers: {
      ...(options.contentType === null ? {} : { 'content-type': options.contentType ?? 'application/json' }),
      ...(options.authorization ? { authorization: options.authorization } : {})
    },
    body: method === 'GET' || method === 'HEAD' ? undefined : (options.body ?? '{}')
  });
  const text = await response.text();
  assert.equal(response.status, 410, `${method} ${route}: ${text.slice(0, 100)}`);
  assert.match(response.headers.get('content-type') ?? '', /^text\/plain/);
  assert.equal(response.headers.get('x-botgram-json-api-retired'), CODE);
  assert.equal(response.headers.get('deprecation'), 'true');
  if (method !== 'HEAD') {
    assert.ok(text.startsWith(CODE));
    assert.ok(!text.trimStart().startsWith('{'));
  }
  return text;
}
