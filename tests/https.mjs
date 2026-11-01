// Node 22+, standard library only. Run only against an explicitly isolated deployment.
import assert from 'node:assert/strict';
import https from 'node:https';
import tls from 'node:tls';
import { createHash, randomBytes } from 'node:crypto';

const origin = new URL(process.env.BOTGRAM_HTTPS_TEST_ORIGIN ?? 'https://nyarugram.moe.autos:8521');
assert(origin.protocol === 'https:' && !origin.username && !origin.password &&
  origin.pathname === '/' && !origin.search && !origin.hash, 'Expected a plain HTTPS origin');
const timeout = 15_000;
const maxPayload = 1024 * 1024;
const sessions = new Set();
const sockets = new Set();
const secrets = new Set();
const tlsOptions = { rejectUnauthorized: true, checkServerIdentity: tls.checkServerIdentity };

function request(method, route, token, body, expected = 200) {
  return new Promise((resolve, reject) => {
    const data = body === undefined ? undefined : JSON.stringify(body);
    const req = https.request(new URL(route, origin), {
      ...tlsOptions, method, signal: AbortSignal.timeout(timeout),
      headers: {
        accept: 'application/json',
        ...(data === undefined ? {} : { 'content-type': 'application/json', 'content-length': Buffer.byteLength(data) }),
        ...(token ? { authorization: `Bearer ${token}` } : {})
      }
    }, response => {
      let size = 0;
      const chunks = [];
      response.on('error', reject);
      response.on('data', chunk => {
        size += chunk.length;
        if (size > maxPayload) response.destroy(new Error('HTTPS response exceeds test limit'));
        else chunks.push(chunk);
      });
      response.on('end', () => {
        try {
          assert.equal(response.statusCode, expected, `${method} ${route}: unexpected HTTP status`);
          const text = Buffer.concat(chunks).toString('utf8');
          resolve(text ? JSON.parse(text) : null);
        } catch { reject(new Error(`${method} ${route}: unexpected status or JSON response (${response.statusCode})`)); }
      });
    });
    req.on('error', reject);
    req.end(data);
  });
}

// Node's browser-style WebSocket cannot set Authorization; probe RFC 6455 over verified TLS.
function openSocket(token, after, expected = 101) {
  return new Promise((resolve, reject) => {
    const hostname = origin.hostname.replace(/^\[|\]$/g, '');
    const socket = tls.connect({
      ...tlsOptions, host: hostname, port: Number(origin.port || 443), servername: hostname
    });
    sockets.add(socket);
    const key = randomBytes(16).toString('base64');
    const accept = createHash('sha1').update(key + '258EAFA5-E914-47DA-95CA-C5AB0DC85B11').digest('base64');
    let buffer = Buffer.alloc(0), upgraded = false, failure, waiter;
    let fragments = [], fragmentSize = 0, fragmented = false;
    const batches = [];
    const timer = setTimeout(() => fail(new Error('WSS handshake timeout')), timeout);
    function fail(error) {
      if (failure) return;
      failure = error;
      clearTimeout(timer);
      if (!upgraded) reject(error);
      if (waiter) { waiter.reject(error); waiter = undefined; }
      socket.destroy();
    }
    function deliver(payload) {
      const batch = JSON.parse(payload.toString('utf8'));
      assert(Array.isArray(batch.events) && Number.isSafeInteger(batch.cursor) && batch.cursor >= 0,
        'Invalid WSS event batch');
      if (waiter) { const pending = waiter; waiter = undefined; pending.resolve(batch); }
      else {
        assert(batches.length < 100, 'WSS batch queue exceeds test limit');
        batches.push(batch);
      }
    }
    const connection = {
      close() { socket.destroy(); },
      nextBatch(wait = timeout) {
        if (batches.length) return Promise.resolve(batches.shift());
        if (failure) return Promise.reject(failure);
        return new Promise((resolveBatch, rejectBatch) => {
          const deadline = setTimeout(() => fail(new Error('WSS message timeout')), wait);
          waiter = {
            resolve(value) { clearTimeout(deadline); resolveBatch(value); },
            reject(error) { clearTimeout(deadline); rejectBatch(error); }
          };
        });
      }
    };
    socket.on('error', fail);
    socket.on('close', () => {
      sockets.delete(socket);
      fail(new Error('WSS connection closed'));
    });
    socket.on('secureConnect', () => {
      if (!socket.authorized) return fail(new Error('WSS TLS is not trusted'));
      socket.write(`GET /api/ws?after=${after} HTTP/1.1\r\nHost: ${origin.host}\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: ${key}\r\nSec-WebSocket-Version: 13\r\n${token ? `Authorization: Bearer ${token}\r\n` : ''}\r\n`);
    });
    socket.on('data', chunk => {
      try {
        buffer = Buffer.concat([buffer, chunk]);
        if (!upgraded) {
          const end = buffer.indexOf('\r\n\r\n');
          assert(end >= 0 || buffer.length <= 16_384, 'WSS headers exceed test limit');
          if (end < 0) return;
          const lines = buffer.subarray(0, end).toString('latin1').split('\r\n');
          const status = Number(/^HTTP\/1\.1 (\d{3})\b/.exec(lines.shift())?.[1]);
          assert.equal(status, expected, 'Unexpected WSS handshake status');
          clearTimeout(timer);
          if (expected !== 101) { resolve(null); socket.destroy(); return; }
          const headers = new Map(lines.map(line => {
            const colon = line.indexOf(':');
            return [line.slice(0, colon).toLowerCase(), line.slice(colon + 1).trim()];
          }));
          assert.equal(headers.get('sec-websocket-accept'), accept, 'Invalid WSS accept key');
          assert.equal(headers.get('upgrade')?.toLowerCase(), 'websocket', 'Invalid WSS upgrade');
          assert(headers.get('connection')?.toLowerCase().split(/\s*,\s*/).includes('upgrade'), 'Invalid WSS connection header');
          upgraded = true;
          buffer = buffer.subarray(end + 4);
          resolve(connection);
        }
        while (buffer.length >= 2) {
          const final = (buffer[0] & 128) !== 0, opcode = buffer[0] & 15;
          assert.equal(buffer[0] & 112, 0, 'Unexpected WSS extension bits');
          assert.equal(buffer[1] & 128, 0, 'Server WSS frames must not be masked');
          let length = buffer[1] & 127, offset = 2;
          if (length === 126) {
            if (buffer.length < 4) return;
            length = buffer.readUInt16BE(2); offset = 4;
          } else if (length === 127) {
            if (buffer.length < 10) return;
            const wide = buffer.readBigUInt64BE(2);
            assert(wide <= BigInt(maxPayload), 'WSS frame exceeds test limit');
            length = Number(wide); offset = 10;
          }
          assert(length <= maxPayload, 'WSS frame exceeds test limit');
          if (buffer.length < offset + length) return;
          const payload = buffer.subarray(offset, offset + length);
          buffer = buffer.subarray(offset + length);
          if (opcode >= 8) {
            assert(final && length <= 125, 'Invalid WSS control frame');
            if (opcode === 8) return fail(new Error('WSS peer closed the connection'));
            assert(opcode === 9 || opcode === 10, 'Unknown WSS control frame');
            if (opcode === 9) {
              const mask = randomBytes(4), pong = Buffer.alloc(6 + length);
              pong[0] = 0x8a; pong[1] = 0x80 | length; mask.copy(pong, 2);
              for (let i = 0; i < length; i++) pong[6 + i] = payload[i] ^ mask[i % 4];
              socket.write(pong);
            }
            continue;
          }
          assert((opcode === 1 && !fragmented) || (opcode === 0 && fragmented), 'Expected WSS text or continuation frame');
          fragments.push(payload); fragmentSize += length;
          assert(fragmentSize <= maxPayload, 'WSS message exceeds test limit');
          fragmented = !final;
          if (final) {
            deliver(Buffer.concat(fragments));
            fragments = []; fragmentSize = 0;
          }
        }
      } catch { fail(new Error('Invalid WSS handshake, frame or event batch')); }
    });
  });
}

async function messageEvent(connection, message, after = 0) {
  const deadline = Date.now() + timeout;
  while (Date.now() < deadline) {
    const batch = await connection.nextBatch(Math.max(1, deadline - Date.now()));
    assert(batch.cursor >= after, 'WSS cursor regressed');
    assert(batch.events.every(event => Number.isSafeInteger(event.id) && event.id > after && event.id <= batch.cursor),
      'WSS replay included an event outside the requested cursor range');
    const event = batch.events.find(event => event.type === 'message.created' && event.payload.id === message.id);
    if (event) {
      assert.equal(event.payload.conversationId, message.conversationId);
      assert.equal(event.payload.senderId, message.senderId);
      assert.equal(event.payload.text, message.text);
      return batch.cursor;
    }
    after = batch.cursor;
  }
  throw new Error('Expected WSS message event was not received');
}

async function register(username, password) {
  const session = await request('POST', '/api/auth/register', null, { username, displayName: username, password });
  assert(typeof session.token === 'string' && session.token.length > 0, 'Registration did not return a token');
  secrets.add(session.token); sessions.add(session.token);
  return session;
}

try {
  const health = await request('GET', '/health');
  assert.equal(health.status, 'ok');
  assert.equal(health.protocol, 'botgram-v1');
  console.log('PASS: HTTPS health with trusted TLS and hostname verification');
  assert.equal(process.env.BOTGRAM_HTTPS_TEST_ISOLATED, '1',
    'Registration refused: set BOTGRAM_HTTPS_TEST_ISOLATED=1 only for an isolated test database');
  const suffix = randomBytes(8).toString('hex');
  const password = randomBytes(24).toString('base64url');
  secrets.add(password);
  const alice = await register(`https_a_${suffix}`, password);
  const bob = await register(`https_b_${suffix}`, password);
  const login = await request('POST', '/api/auth/login', null, { username: alice.user.username, password });
  assert(typeof login.token === 'string' && login.token.length > 0, 'Login did not return a token');
  secrets.add(login.token); sessions.add(login.token);
  assert.equal(login.user.id, alice.user.id);
  assert.equal((await request('GET', '/api/me', login.token)).id, alice.user.id);
  await request('GET', '/api/me', null, undefined, 401);
  console.log('PASS: isolated registration, login and Bearer authentication');

  await openSocket(null, 0, 401);
  console.log('PASS: unauthenticated WSS upgrade rejected with HTTP 401');
  const conversation = await request('POST', '/api/conversations', login.token,
    { kind: 'direct', memberIds: [bob.user.id] });
  const route = `/api/conversations/${conversation.id}/messages`;
  const initial = await request('GET', '/api/events?after=0', bob.token);
  const live = await openSocket(bob.token, initial.cursor);
  const first = await request('POST', route, login.token,
    { clientMessageId: `https-first-${suffix}`, text: 'HTTPS 隔离测试：你好 Bob' });
  assert.equal(first.senderId, alice.user.id);
  const cursor = await messageEvent(live, first, initial.cursor);
  live.close();
  const reply = await request('POST', route, bob.token,
    { clientMessageId: `https-reply-${suffix}`, text: 'HTTPS 隔离测试：你好 Alice' });
  assert.equal(reply.senderId, bob.user.id);
  const history = await request('GET', route, login.token);
  assert(history.some(message => message.id === first.id && message.text === first.text));
  assert(history.some(message => message.id === reply.id && message.text === reply.text));
  const replay = await request('GET', `/api/events?after=${cursor}`, bob.token);
  assert(replay.events.some(event => event.type === 'message.created' && event.payload.id === reply.id));
  const reconnected = await openSocket(bob.token, cursor);
  const replayCursor = await messageEvent(reconnected, reply, cursor);
  assert(replayCursor > cursor, 'WSS replay cursor did not advance');
  reconnected.close();
  console.log('PASS: two-user text messages, history, live Bearer WSS events and disconnected after-cursor replay');

  for (const token of [...sessions]) {
    await request('POST', '/api/auth/logout', token, undefined, 204);
    sessions.delete(token);
    await request('GET', '/api/me', token, undefined, 401);
    await openSocket(token, 0, 401);
  }
  console.log('PASS: logout invalidates every test token for HTTPS and WSS');
} catch (error) {
  let message = error instanceof Error ? error.message : 'Unknown test error';
  for (const secret of secrets) message = message.replaceAll(secret, '[redacted]');
  console.error(`FAIL: ${message}`);
  process.exitCode = 1;
} finally {
  for (const socket of sockets) socket.destroy();
  for (const token of sessions) {
    try { await request('POST', '/api/auth/logout', token, undefined, 204); }
    catch { console.error('FAIL: could not revoke a test session during cleanup'); process.exitCode = 1; }
  }
}
