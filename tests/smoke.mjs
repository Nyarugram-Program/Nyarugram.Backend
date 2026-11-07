// Legacy smoke suite after JSON API retirement.
// All former JSON REST smoke cases now verify that the retired ingress rejects
// them with the same explicit non-JSON 410 and has no business side effects.
// Positive smoke coverage moved to tests/protobuf-business.mjs and
// tests/protobuf-business.mjs with BOTGRAM_BUSINESS_POSTGRES=1.
import assert from 'node:assert/strict';
import { isolatedBackend, assertRetired, RouteGroups, CODE } from './retirement-lib.mjs';
import { register, Operation, outcome, first, stringOf, envelope, call, AUTH } from './protobuf-plane.mjs';

const host = await isolatedBackend('smoke-retired');
let checks = 0;
try {
  const health = await fetch(host.base + '/health');
  const healthText = await health.text();
  assert.equal(health.status, 200);
  assert.match(health.headers.get('content-type'), /^text\/plain/);
  assert.match(healthText, /status=ok/);
  assert.match(healthText, new RegExp(CODE));
  checks++;

  const before = await host.state();
  const routes = [...RouteGroups.auth, ...RouteGroups.conversations, ...RouteGroups.rpc,
    ...RouteGroups.security, ...RouteGroups.calls];
  for (const route of routes) {
    for (const method of ['GET', 'POST', 'PUT', 'DELETE', 'PATCH', 'HEAD', 'OPTIONS']) {
      await assertRetired(host.base, route, { method });
      checks++;
    }
    await assertRetired(host.base, route, { method: 'POST', authorization: 'Bearer invalid-token',
      contentType: 'application/x-protobuf' });
    checks++;
    await assertRetired(host.base, route + '/', { method: 'POST' });
    checks++;
  }
  // Rejected requests never touch the authoritative store.
  assert.equal(await host.state(), before);
  checks++;

  // The Protobuf replacement still performs a real registration.
  const account = await register(host.base, 'smoke_proto_' + Date.now().toString(36),
    'protobuf-smoke-password', 'Smoke');
  assert.equal(stringOf(first(outcome(account.result, Operation.register), 1)).length, 64);
  checks++;
  const badBody = await fetch(host.base + AUTH, { method: 'POST',
    headers: { 'content-type': 'application/json' }, body: '{"username":"x"}' });
  assert.equal(badBody.status, 415);
  checks++;

  console.log(`PASS: retired JSON smoke suite (${checks} assertions, ${routes.length} retired routes)`);
} catch (error) {
  if (host.logs().trim()) console.error('BACKEND LOGS:\n' + host.logs());
  throw error;
} finally {
  await host.stop();
}
