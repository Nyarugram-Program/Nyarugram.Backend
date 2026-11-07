// Legacy TL-JSON RPC boundary suite after retirement. Every former boundary
// case must now be rejected at the single ingress with the same explicit
// non-JSON 410, without authentication, JSON sniffing or database writes.
// Positive typed-RPC boundaries live in tests/protobuf-business.mjs.
import assert from 'node:assert/strict';
import { isolatedBackend, assertRetired, CODE } from './retirement-lib.mjs';
import { register, Operation, envelope, call, AUTH, ACCOUNT } from './protobuf-plane.mjs';

const host = await isolatedBackend('rpc-boundaries-retired');
let checks = 0;
try {
  const bodies = [
    '{}',
    JSON.stringify({ $type: 'org.telegram.tgnet.TLRPC$TL_messages_getState' }),
    // Below the 128 KiB request-body boundary; an oversized body is rejected
    // by Kestrel at the transport layer before the retirement gate responds.
    JSON.stringify({ $type: 'org.telegram.tgnet.TLRPC$TL_messages_sendMessage', message: 'x'.repeat(60000) }),
    'not-json', '', '{"$type":null}', '[]'
  ];
  const variants = [
    '/api/telegram/rpc', '/api/telegram/rpc/', '/api//telegram/rpc',
    '/API/TELEGRAM/RPC', '/api/telegram/rpc?legacy=1'
  ];
  const before = await host.state();
  for (const route of variants) {
    for (const method of ['GET', 'POST', 'PUT', 'DELETE', 'PATCH', 'HEAD', 'OPTIONS']) {
      for (const body of bodies) {
        await assertRetired(host.base, route, { method, body, contentType: 'application/json' });
        checks++;
      }
    }
    for (const authorization of [null, 'Bearer invalid', 'Bearer ' + 'f'.repeat(64)]) {
      await assertRetired(host.base, route, { method: 'POST', authorization,
        contentType: 'application/x-protobuf', body: '{}' });
      checks++;
    }
  }
  assert.equal(await host.state(), before, 'rejected RPC requests must not write to the store');
  checks++;

  // A valid Protobuf session must not be able to re-enable the JSON RPC path.
  const account = await register(host.base, 'rpc_boundary_' + Date.now().toString(36),
    'rpc-boundaries-password', 'Boundary');
  await assertRetired(host.base, '/api/telegram/rpc', { method: 'POST',
    authorization: 'Bearer ' + account.token, contentType: 'application/json' });
  checks++;

  // Protobuf endpoints never sniff a JSON body.
  for (const path of ['/api/v1/rpc', AUTH, ACCOUNT]) {
    const response = await fetch(host.base + path, { method: 'POST',
      headers: { 'content-type': 'application/json', accept: 'application/json' }, body: '{}' });
    assert.equal(response.status, 415, path);
    assert.ok((await response.text()).includes('BOTGRAM_MEDIA_TYPE_UNSUPPORTED'));
    checks++;
  }
  console.log(`PASS: retired TL-JSON RPC boundary suite (${checks} assertions, no JSON sniffing or side effects)`);
} catch (error) {
  if (host.logs().trim()) console.error('BACKEND LOGS:\n' + host.logs());
  throw error;
} finally {
  await host.stop();
}
