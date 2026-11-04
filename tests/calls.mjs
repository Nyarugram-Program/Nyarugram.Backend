// Node 22+, no dependencies. Build the backend before running this script.
// Covers the legacy TL-JSON channel used before protobuf rollout:
// messages.getDhConfig -> phone.requestCall/receivedCall/acceptCall/confirmCall
// -> updates.getDifference -> phone.sendSignalingData -> phone.discardCall.
import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { once } from 'node:events';
import { mkdtemp, readFile, writeFile } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import net from 'node:net';
import { randomBytes } from 'node:crypto';

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const fixtureRoot = process.env.BOTGRAM_TEST_ROOT ?? path.join(root, 'obj');
const temp = await mkdtemp(path.join(fixtureRoot, 'calls-'));
const database = path.join(temp, 'db.json');
const listener = net.createServer().listen(0, '127.0.0.1');
await once(listener, 'listening');
const port = listener.address().port;
await new Promise(resolve => listener.close(resolve));
const base = `http://127.0.0.1:${port}`;
const tl = (name, fields = {}) => ({ $type: 'org.telegram.tgnet.TLRPC$' + name, ...fields });
const phone = (name, fields = {}) => ({ $type: 'org.telegram.tgnet.tl.TL_phone$' + name, ...fields });
const bytes = length => ({ $bytes: randomBytes(length).toString('base64') });
const bytesOf = value => Buffer.from(value.$bytes, 'base64');
const protocol = () => phone('TL_phoneCallProtocol', {
  flags: 3, udp_p2p: true, udp_reflector: true, min_layer: 65, max_layer: 92, library_versions: ['10.0.0']
});
// The backend reads relay endpoints from the existing Rtc:IceServers section.
const env = {
  ...process.env, DataPath: database,
  Rtc__IceServers__0__Urls__0: 'turn:127.0.0.1:3478?transport=udp',
  Rtc__IceServers__0__Username: 'nyaru', Rtc__IceServers__0__Credential: 'turn-secret',
  Rtc__IceServers__1__Urls__0: 'stun:127.0.0.1:3479'
};
let server, running = false, logs = '';
async function start() {
  server = spawn('dotnet', [process.env.BOTGRAM_TEST_DLL ?? path.join(root, 'bin/Debug/net8.0/Botgram.Backend.dll'), '--urls', base], {
    cwd: root, env, windowsHide: true, stdio: ['ignore', 'pipe', 'pipe']
  });
  running = true;
  server.stdout.on('data', x => logs += x); server.stderr.on('data', x => logs += x);
  for (let i = 0; i < 100; i++) {
    if (server.exitCode !== null) throw Error(logs);
    try { if ((await fetch(base + '/health')).ok) return; } catch {}
    await new Promise(resolve => setTimeout(resolve, 100));
  }
  throw Error(logs);
}
async function stop() {
  if (!server || !running) return;
  running = false;
  const exit = once(server, 'exit');
  server.kill();
  await exit;
}
const fixtures = [];
async function req(method, route, token, body, status = 200) {
  const response = await fetch(base + route, { method,
    headers: { 'content-type': 'application/json', ...(token ? { authorization: `Bearer ${token}` } : {}) },
    body: body === undefined ? undefined : JSON.stringify(body) });
  const data = await response.json();
  assert.equal(response.status, status, `${method} ${route}: ${JSON.stringify(data)}`);
  return data;
}
async function rpc(token, type, fields = {}, status = 200) {
  const body = typeof type === 'string'
    ? { $type: type.includes('.') ? type : 'org.telegram.tgnet.TLRPC$' + type, ...fields }
    : { ...type, ...fields };
  const response = await req('POST', '/api/telegram/rpc', token, body, status);
  if (status === 200) fixtures.push({ requestType: body.$type, response });
  return response;
}
const updateCall = 'org.telegram.tgnet.tl.TL_update$TL_updatePhoneCall';
const updateSignal = 'org.telegram.tgnet.tl.TL_update$TL_updatePhoneCallSignalingData';
const callUpdates = diff => (diff.other_updates ?? []).filter(u => u.$type === updateCall);
const signalUpdates = diff => (diff.other_updates ?? []).filter(u => u.$type === updateSignal);
const state = token => rpc(token, 'TL_updates_getState');
const difference = (token, pts) => rpc(token, 'TL_updates_getDifference', { pts, date: 0, qts: 0 });

try {
  await start();
  const accounts = [];
  for (const username of ['alice', 'bob', 'carol'])
    accounts.push(await req('POST', '/api/auth/register', null, { username, displayName: username, password: 'correct-horse-123' }));
  const [alice, bob, carol] = accounts;
  const [aliceTl, bobTl] = [await req('GET', '/api/telegram/me', alice.token), await req('GET', '/api/telegram/me', bob.token)];
  const peerOf = user => ({ $type: 'org.telegram.tgnet.TLRPC$TL_inputUser', user_id: user.id, access_hash: 1 });
  const bobPeer = peerOf(bobTl);

  assert.equal((await rpc(null, 'org.telegram.tgnet.tl.TL_phone$requestCall', {}, 401)).error, 'unauthorized');

  // 1. DH config used by VoIPService before requestCall.
  const dh = await rpc(alice.token, 'TL_messages_getDhConfig', { random_length: 256, version: 0 });
  assert.equal(dh.$type, 'org.telegram.tgnet.TLRPC$TL_messages_dhConfig');
  assert.equal(dh.g, 3);
  assert.equal(dh.version, 1);
  assert.equal(bytesOf(dh.p).length, 256);
  assert.equal(bytesOf(dh.random).length, 256);

  // 2. Full profile advertises call capabilities for the profile page entry.
  const full = await rpc(alice.token, 'TL_users_getFullUser', { id: bobPeer });
  assert.equal(full.full_user.phone_calls_available, true);
  assert.equal(full.full_user.video_calls_available, true);
  assert(full.full_user.flags & 16 && full.full_user.flags & 8192);

  const alicePts = (await state(alice.token)).pts;
  const bobPts = (await state(bob.token)).pts;
  assert.equal(alicePts, 1);
  assert.equal(bobPts, 1);

  // 3. Caller opens the call; the invite carries g_a_hash only.
  const gAHash = bytes(256), randomId = 424242;
  const waiting = await rpc(alice.token, phone('requestCall'), {
    flags: 0, video: false, user_id: bobPeer, random_id: randomId, g_a_hash: gAHash, protocol: protocol()
  });
  assert.equal(waiting.$type, 'org.telegram.tgnet.tl.TL_phone$TL_phone_phoneCall');
  assert.equal(waiting.phone_call.$type, 'org.telegram.tgnet.tl.TL_phone$TL_phoneCallWaiting');
  assert(waiting.phone_call.id > 0);
  assert(waiting.phone_call.access_hash > 0);
  assert.equal(waiting.phone_call.admin_id, aliceTl.id);
  assert.equal(waiting.phone_call.participant_id, bobTl.id);
  assert.equal(waiting.phone_call.receive_date ?? 0, 0);
  assert.equal(waiting.users[0].id, bobTl.id);
  assertSafeUser(waiting);
  const callPeer = { $type: 'org.telegram.tgnet.tl.TL_inputPhoneCall', id: waiting.phone_call.id, access_hash: waiting.phone_call.access_hash };
  const callId = waiting.phone_call.id;

  // Retries with the same random_id reuse the existing call instead of ringing twice.
  const replay = await rpc(alice.token, phone('requestCall'), {
    flags: 0, video: false, user_id: bobPeer, random_id: randomId, g_a_hash: bytes(256), protocol: protocol()
  });
  assert.equal(replay.phone_call.id, callId);
  assert.equal(replay.phone_call.access_hash, waiting.phone_call.access_hash);

  await rpc(alice.token, phone('requestCall'), {
    flags: 0, user_id: peerOf(aliceTl), random_id: 1, g_a_hash: bytes(32), protocol: protocol()
  }, 400).then(x => assert.equal(x.error, 'CALL_PEER_INVALID'));
  await rpc(alice.token, phone('requestCall'), {
    flags: 0, user_id: tl('TL_inputUser', { user_id: 2147483647 }), random_id: 2, g_a_hash: bytes(32), protocol: protocol()
  }, 404).then(x => assert.equal(x.error, 'USER_ID_INVALID'));
  await rpc(carol.token, phone('requestCall'), {
    flags: 0, user_id: bobPeer, random_id: 3, g_a_hash: bytes(32), protocol: protocol()
  }, 409).then(x => assert.equal(x.error, 'CALL_BUSY'));
  await rpc(carol.token, phone('sendSignalingData'), { peer: callPeer, data: bytes(8) }, 400);
  await rpc(alice.token, phone('confirmCall'), {
    peer: callPeer, g_a: bytes(256), key_fingerprint: 123456789, protocol: protocol()
  }, 400).then(x => assert.equal(x.error, 'CALL_NOT_ACCEPTED'));
  const wrongHash = { ...callPeer, access_hash: callPeer.access_hash + 1 };
  await rpc(bob.token, phone('receivedCall'), { peer: wrongHash }, 400).then(x => assert.equal(x.error, 'CALL_NOT_FOUND'));

  // 4. Callee sees the invite through the normal difference stream.
  let diff = await difference(bob.token, bobPts);
  let updates = callUpdates(diff);
  assert.equal(updates.length, 1);
  assert.equal(updates[0].phone_call.$type, 'org.telegram.tgnet.tl.TL_phone$phoneCallRequested');
  assert.equal(updates[0].phone_call.admin_id, aliceTl.id);
  assert.equal(updates[0].phone_call.participant_id, bobTl.id);
  assert.deepEqual(bytesOf(updates[0].phone_call.g_a_hash), bytesOf(gAHash));
  let bobCursor = diff.state.pts;
  assert.equal(diff.new_messages.length, 0);

  // 5. Callee acknowledges the invite; the caller's waiting call gains receive_date.
  assert.equal((await rpc(bob.token, phone('receivedCall'), { peer: callPeer })).$type, 'org.telegram.tgnet.TLRPC$TL_boolTrue');
  diff = await difference(alice.token, alicePts);
  assert.equal(callUpdates(diff).at(-1).phone_call.$type, 'org.telegram.tgnet.tl.TL_phone$TL_phoneCallWaiting');
  assert(callUpdates(diff).at(-1).phone_call.receive_date > 0);
  let aliceCursor = diff.state?.pts ?? aliceCursor;

  // 6. Callee accepts with g_b; caller answers with g_a, key fingerprint and ICE relays.
  const gB = bytes(256);
  const accepted = await rpc(bob.token, phone('acceptCall'), { peer: callPeer, g_b: gB, protocol: protocol() });
  assert.equal(accepted.phone_call.$type, 'org.telegram.tgnet.tl.TL_phone$TL_phoneCallAccepted');
  assert.deepEqual(bytesOf(accepted.phone_call.g_b), bytesOf(gB));
  await rpc(bob.token, phone('acceptCall'), { peer: callPeer, g_b: bytes(256), protocol: protocol() }, 400)
    .then(x => assert.equal(x.error, 'CALL_ALREADY_ACCEPTED'));
  await rpc(alice.token, phone('acceptCall'), { peer: callPeer, g_b: bytes(256), protocol: protocol() }, 400)
    .then(x => assert.equal(x.error, 'CALL_NOT_FOUND'));
  diff = await difference(alice.token, aliceCursor);
  assert.equal(callUpdates(diff)[0].phone_call.$type, 'org.telegram.tgnet.tl.TL_phone$TL_phoneCallAccepted');
  aliceCursor = diff.state?.pts ?? aliceCursor;

  const gA = bytes(256), fingerprint = 9007199254740991;
  const active = await rpc(alice.token, phone('confirmCall'), { peer: callPeer, g_a: gA, key_fingerprint: fingerprint, protocol: protocol() });
  const phoneCall = active.phone_call;
  assert.equal(phoneCall.$type, 'org.telegram.tgnet.tl.TL_phone$TL_phoneCall');
  assert.deepEqual(bytesOf(phoneCall.g_a_or_b), bytesOf(gA));
  assert.equal(phoneCall.key_fingerprint, fingerprint);
  assert.equal(phoneCall.p2p_allowed, true);
  assert(phoneCall.flags & 32);
  assert(phoneCall.start_date > 0);
  assert.equal(phoneCall.connections.length, 2);
  const [turn, stun] = phoneCall.connections;
  assert.equal(turn.$type, 'org.telegram.tgnet.TLRPC$TL_phoneConnectionWebrtc');
  assert.deepEqual([turn.ip, turn.port, turn.turn, turn.stun, turn.username, turn.password],
    ['127.0.0.1', 3478, true, false, 'nyaru', 'turn-secret']);
  assert.equal(turn.flags, 1);
  assert.deepEqual([stun.ip, stun.port, stun.turn, stun.stun, stun.username, stun.password],
    ['127.0.0.1', 3479, false, true, '', '']);
  assert.equal(stun.flags, 2);
  assert.notEqual(turn.id, stun.id);
  diff = await difference(bob.token, bobCursor);
  assert.equal(callUpdates(diff).at(-1).phone_call.$type, 'org.telegram.tgnet.tl.TL_phone$TL_phoneCall');
  assert.equal(callUpdates(diff).at(-1).phone_call.connections.length, 2);
  bobCursor = diff.state?.pts ?? bobCursor;

  // 7. Encrypted channel signaling is relayed to the other participant only.
  const payload = bytes(64);
  assert.equal((await rpc(alice.token, phone('sendSignalingData'), { peer: callPeer, data: payload })).$type,
    'org.telegram.tgnet.TLRPC$TL_boolTrue');
  diff = await difference(bob.token, bobCursor);
  let signals = signalUpdates(diff);
  assert.equal(signals.length, 1);
  assert.equal(signals[0].phone_call_id, callId);
  assert.deepEqual(bytesOf(signals[0].data), bytesOf(payload));
  bobCursor = diff.state?.pts ?? bobCursor;
  diff = await difference(alice.token, aliceCursor);
  assert.equal(signalUpdates(diff).length, 0, 'a sender must not receive its own signaling data');
  aliceCursor = diff.state?.pts ?? aliceCursor;
  await rpc(carol.token, phone('sendSignalingData'), { peer: callPeer, data: payload }, 400)
    .then(x => assert.equal(x.error, 'CALL_NOT_FOUND'));

  // 8. Call config and debug helpers return the concrete types the app casts to.
  assert.equal((await rpc(alice.token, phone('getCallConfig'))).$type, 'org.telegram.tgnet.TLRPC$TL_dataJSON');
  assert.equal((await rpc(bob.token, phone('saveCallDebug'), { peer: callPeer, debug: { $bytes: bytes(16).$bytes } })).$type,
    'org.telegram.tgnet.TLRPC$TL_boolTrue');

  // 9. Hangup is broadcast to both sides as a discarded call inside a TL updates container.
  const hangup = await rpc(alice.token, phone('discardCall'), {
    peer: callPeer, duration: 0, reason: { $type: 'org.telegram.tgnet.TLRPC$TL_phoneCallDiscardReasonHangup' }, connection_id: 0
  });
  assert.equal(hangup.$type, 'org.telegram.tgnet.TLRPC$TL_updates');
  assert.equal(hangup.updates[0].$type, updateCall);
  assert.equal(hangup.updates[0].phone_call.$type, 'org.telegram.tgnet.tl.TL_phone$TL_phoneCallDiscarded');
  diff = await difference(bob.token, bobCursor);
  const discarded = callUpdates(diff).at(-1).phone_call;
  assert.equal(discarded.id, callId);
  assert.equal(discarded.reason.$type, 'org.telegram.tgnet.TLRPC$TL_phoneCallDiscardReasonHangup');
  assert.equal(typeof discarded.duration, 'number');
  bobCursor = diff.state?.pts ?? bobCursor;

  // 10. A finished call no longer blocks new ones.
  const second = await rpc(alice.token, phone('requestCall'), {
    flags: 0, user_id: bobPeer, random_id: randomId + 1, g_a_hash: bytes(256), protocol: protocol()
  });
  assert.notEqual(second.phone_call.id, callId);
  await rpc(alice.token, phone('discardCall'), {
    peer: { $type: 'org.telegram.tgnet.tl.TL_inputPhoneCall', id: second.phone_call.id, access_hash: second.phone_call.access_hash },
    duration: 0, reason: { $type: 'org.telegram.tgnet.TLRPC$TL_phoneCallDiscardReasonHangup' }, connection_id: 0
  });

  // 11. Call records and the update stream survive a restart.
  await stop();
  const snapshot = JSON.parse(await readFile(database, 'utf8'));
  const stored = snapshot.calls.find(c => c.telegramId === callId);
  assert(stored, 'call must be persisted with its Telegram id');
  assert.equal(stored.state, 'ended');
  await start();
  diff = await difference(bob.token, bobPts);
  assert(callUpdates(diff).some(u => u.phone_call.id === callId && u.phone_call.$type.endsWith('phoneCallRequested')));
  assert(callUpdates(diff).some(u => u.phone_call.id === callId && u.phone_call.$type.endsWith('TL_phoneCallDiscarded')));
  await stop();
  await writeFile(path.join(fixtureRoot, 'call-tl-fixtures.json'), JSON.stringify(fixtures));
  console.log(`PASS: TL-JSON 1:1 calls (DH config, request/receive/accept/confirm, difference updates, signaling relay, hangup, busy/idempotent/invalid, TURN+STUN relays, restart persistence); ${fixtures.length} TL fixtures`);
} finally {
  await stop();
}
function assertSafeUser(value) {
  if (value === null || typeof value !== 'object') return;
  for (const [key, item] of Object.entries(value)) {
    assert(!['salt', 'passwordHash', 'token', 'tokenHash'].includes(key), `private user field: ${key}`);
    assertSafeUser(item);
  }
}
