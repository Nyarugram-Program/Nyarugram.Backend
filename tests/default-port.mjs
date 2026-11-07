import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { once } from 'node:events';
import { mkdtemp } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import net from 'node:net';

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const dll = process.env.BOTGRAM_TEST_DLL ?? path.join(root, 'bin/Debug/net8.0/Botgram.Backend.dll');
const temp = await mkdtemp(path.join(process.env.BOTGRAM_TEST_ROOT ?? path.join(root, 'obj'), 'default-port-'));
// Fail if another process owns the intended port; never stop an unrelated listener.
const probe = net.createServer().listen(8521, '127.0.0.1');
await once(probe, 'listening');
await new Promise(resolve => probe.close(resolve));
const env = { ...process.env, DataPath: path.join(temp, 'db.json') };
for (const key of Object.keys(env)) {
  if (/^(ASPNETCORE_|DOTNET_)?(URLS|HTTP_PORTS|HTTPS_PORTS)$/i.test(key)) delete env[key];
}
let logs = '';
// No --urls: exercise the published appsettings default, rather than a test override.
const server = spawn('dotnet', [dll], {
  cwd: process.env.BOTGRAM_TEST_DLL ? path.dirname(dll) : root, env, windowsHide: true, stdio: ['ignore', 'pipe', 'pipe']
});
server.stdout.on('data', x => logs += x); server.stderr.on('data', x => logs += x);
try {
  let ready = false;
  for (let i = 0; i < 100; i++) {
    if (server.exitCode !== null) throw Error(logs);
    try {
      const response = await fetch('http://127.0.0.1:8521/health');
      if (response.ok) {
        // Health is a non-JSON key/value document after JSON API retirement.
        assert.match(response.headers.get('content-type') ?? '', /^text\/plain/);
        assert.match(await response.text(), /status=ok/);
        ready = true; break;
      }
    } catch {}
    await new Promise(resolve => setTimeout(resolve, 100));
  }
  assert(ready, logs);
  const retired = await fetch('http://127.0.0.1:8521/api/me');
  assert.equal(retired.status, 410);
  assert.match(retired.headers.get('content-type') ?? '', /^text\/plain/);
  assert.ok((await retired.text()).startsWith('BOTGRAM_JSON_API_RETIRED'));
  console.log('PASS: published default port 8521, non-JSON health probe and retired JSON ingress; isolated database');
} finally {
  if (server.exitCode === null) { const exit = once(server, 'exit'); server.kill(); await exit; }
}
