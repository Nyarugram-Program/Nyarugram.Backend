import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { once } from 'node:events';
import { mkdtemp } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import net from 'node:net';

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const temp = await mkdtemp(path.join(process.env.BOTGRAM_TEST_ROOT ?? path.join(root, 'obj'), 'forwarded-headers-'));
const dll = process.env.BOTGRAM_TEST_DLL ?? path.join(root, 'bin/Debug/net8.0/Botgram.Backend.dll');

async function scenario(name, proxies, test) {
  const listener = net.createServer().listen(0, '127.0.0.1');
  await once(listener, 'listening');
  const base = `http://127.0.0.1:${listener.address().port}`;
  await new Promise(resolve => listener.close(resolve));
  const env = { ...process.env, DataPath: path.join(temp, `${name}.json`) };
  for (const key of Object.keys(env)) {
    if (/^ReverseProxy__/i.test(key) || /^ASPNETCORE_FORWARDEDHEADERS_ENABLED$/i.test(key)) delete env[key];
  }
  const args = proxies.map((ip, i) => `--ReverseProxy:KnownProxies:${i}=${ip}`);
  const server = spawn('dotnet', [dll, '--urls', base, ...args], {
    cwd: root, env, windowsHide: true, stdio: ['ignore', 'pipe', 'pipe']
  });
  let logs = '';
  server.stdout.on('data', x => logs += x); server.stderr.on('data', x => logs += x);
  try {
    let ready = false;
    for (let i = 0; i < 100; i++) {
      if (server.exitCode !== null) throw Error(logs);
      try { if ((await fetch(base + '/health')).ok) { ready = true; break; } } catch {}
      await new Promise(resolve => setTimeout(resolve, 100));
    }
    assert(ready, logs);
    async function request(forwardedFor, status) {
      const response = await fetch(base + '/api/auth/register', {
        method: 'POST', headers: {
          'content-type': 'application/json',
          'x-forwarded-for': forwardedFor,
          'x-forwarded-proto': 'https'
        }, body: '{}'
      });
      const text = await response.text();
      assert.equal(response.status, status, `${name}, X-Forwarded-For ${forwardedFor}: ${text}`);
      if (status === 400) assert.equal(JSON.parse(text).error, 'invalid_username');
    }
    await test(request);
    console.log(`PASS: forwarded headers ${name}`);
  } finally {
    if (server.exitCode === null) { const exit = once(server, 'exit'); server.kill(); await exit; }
  }
}

// Changing spoofed headers must not escape the actual connection IP's auth bucket.
for (const [name, proxies] of [['disabled', []], ['untrusted-loopback', ['192.0.2.10']]]) {
  await scenario(name, proxies, async request => {
    for (let i = 1; i <= 15; i++) await request(`198.51.100.${i}`, 400);
    await request('203.0.113.1', 429);
  });
}
await scenario('trusted-client-partitions-and-single-hop', ['127.0.0.1'], async request => {
  for (let i = 0; i < 15; i++) await request('198.51.100.1', 400);
  await request('198.51.100.1', 429);
  await request('198.51.100.2', 400);
  await request('203.0.113.1, 198.51.100.1', 429);
  // Even when a second hop is itself trusted, only the nearest forwarded entry is consumed.
  for (let i = 0; i < 15; i++) await request('127.0.0.1', 400);
  await request('203.0.113.2, 127.0.0.1', 429);
});
