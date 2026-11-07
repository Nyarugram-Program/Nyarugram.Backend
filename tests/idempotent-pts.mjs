// Idempotent retry must return the original message's persistent per-user pts
// and pts_count, not the current sync cursor. Covers other messages, E2EE
// enable/close/reopen, identity replacement, restart, conflict and difference.
import assert from 'node:assert/strict';
import { execFileSync, spawn } from 'node:child_process';
import { once } from 'node:events';
import { mkdtemp } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import net from 'node:net';
import {
  vfield, sfield, bfield, fixed64, concat, fields, first, all, stringOf,
  envelope, call, register, profile
} from './protobuf-plane.mjs';

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const connectionString = process.env.BOTGRAM_E2EE_PG || process.env.BOTGRAM_PG_TEST_CONNECTION_STRING;
if (!connectionString) {
  console.error('ERROR: set BOTGRAM_E2EE_PG or BOTGRAM_PG_TEST_CONNECTION_STRING to an isolated PostgreSQL database.');
  process.exit(1);
}
if (!process.env.BOTGRAM_PSQL) {
  console.error('ERROR: set BOTGRAM_PSQL to the psql executable path.');
  process.exit(1);
}
const settings = Object.fromEntries(connectionString.split(';').filter(Boolean).map(pair => {
  const index = pair.indexOf('=');
  return [pair.slice(0, index).trim().toLowerCase(), pair.slice(index + 1).trim()];
}));
const database = settings.database ?? settings['initial catalog'] ?? '';
const pgHost = settings.host ?? settings.server ?? '127.0.0.1';
const pgPort = settings.port ?? '5432';
const pgUser = settings.username ?? settings['user id'] ?? settings.userid;
if (!/^botgram_test_[a-z0-9_]+$/i.test(database) || !['localhost', '127.0.0.1', '::1', '[::1]'].includes(pgHost.toLowerCase()))
  throw new Error('Refusing a non-isolated database; use a loopback botgram_test_* target');
function sql(statement) {
  return execFileSync(process.env.BOTGRAM_PSQL,
    ['-h', pgHost, '-p', pgPort, '-U', pgUser, '-d', database, '-tAc', statement],
    { encoding: 'utf8' }).trim();
}

const temp = await mkdtemp(path.join(process.env.BOTGRAM_TEST_ROOT ?? path.join(root, 'obj'), 'idempotent-pts-'));
const listener = net.createServer().listen(0, '127.0.0.1');
await once(listener, 'listening');
const port = listener.address().port;
await new Promise(resolve => listener.close(resolve));
const base = `http://127.0.0.1:${port}`;
const domain = 'nyarugram.moe.autos';
let server, logs = '';
let checks = 0;

async function start() {
  server = spawn('dotnet', [process.env.BOTGRAM_TEST_DLL ?? path.join(root, 'bin/Debug/net8.0/Botgram.Backend.dll'),
    '--urls', base], {
    cwd: root, env: { ...process.env,
      ASPNETCORE_ENVIRONMENT: 'Development', Storage__Mode: 'NewStorageValidation',
      Storage__ValidationBackend: 'RelationalBackend', Storage__ValidationAcknowledged: 'true',
      Storage__ValidationConnectionString: connectionString,
      Protocol__LegacyJsonRpcEnabled: 'false', Protocol__LegacyJsonWssEnabled: 'false',
      Protocol__ProtobufRolloutPercent: '100', DataPath: path.join(temp, 'keys.json') },
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

function userRef(id) { return concat(sfield(1, domain), sfield(2, id)); }
function peerRef(id) { return concat(bfield(1, userRef(id))); }
function rpc(token, tag, payload) { return call(base, '/api/v1/rpc', envelope(tag, payload), token); }
function outcome(result, tag) {
  assert.equal(result.status, 200, `expected 200 for ${tag}, got ${result.status} ${first(result.bytes, 10) ? stringOf(first(fields(first(result.bytes, 10).bytes), 1)) : ''}`);
  const entry = first(result.bytes, tag);
  assert.ok(entry, `missing outcome ${tag}`);
  return fields(entry.bytes);
}
function code(result, status, expected) {
  assert.equal(result.status, status, `expected ${status}, got ${result.status}`);
  assert.equal(stringOf(first(fields(first(result.bytes, 10).bytes), 1)), expected);
}
function publishBody(identity, ids) {
  return concat(vfield(1, 1), vfield(2, 1234), bfield(3, Buffer.alloc(33, identity)), vfield(4, 10),
    bfield(5, Buffer.alloc(33, 2)), bfield(6, Buffer.alloc(32, 3)), vfield(7, 11),
    bfield(8, Buffer.alloc(800, 4)), bfield(9, Buffer.alloc(32, 5)),
    ...ids.map(id => bfield(10, concat(vfield(1, id), bfield(2, Buffer.alloc(33, id & 0xff))))));
}
function envelopeBytes(conversation, sender, recipient, epoch, randomId, messageType = 3, recipientDevice = 1) {
  return concat(vfield(1, 1), sfield(2, conversation), sfield(3, sender), vfield(4, 1),
    sfield(5, recipient), vfield(6, recipientDevice), vfield(7, epoch), fixed64(8, randomId),
    bfield(9, Buffer.alloc(48, 9)), vfield(10, messageType));
}
function send(peer, randomId, text, e2ee) {
  const parts = [bfield(1, peerRef(peer)), fixed64(2, randomId)];
  if (text) parts.push(sfield(3, text));
  if (e2ee) parts.push(bfield(7, e2ee));
  return concat(...parts);
}
function sentInfo(result) {
  const sent = outcome(result, 107);
  const message = fields(first(sent, 1).bytes);
  const mapping = fields(first(sent, 2).bytes);
  const updates = fields(first(sent, 3).bytes);
  // The response also inserts the random-id mapping update at index 0, so find
  // the update that actually carries new_message instead of assuming it is first.
  const update = updates.filter(entry => entry.number === 1).map(entry => fields(entry.bytes))
    .find(candidate => first(candidate, 1));
  const newMessage = fields(first(update, 1).bytes);
  return {
    messageId: Number(first(message, 1).value),
    randomId: first(mapping, 1).bytes.readBigInt64LE(),
    pts: Number(first(newMessage, 2).value),
    ptsCount: Number(first(newMessage, 3).value)
  };
}
function chatFields(list) { return fields(first(list, 1).bytes); }
function confirmBody(peer, status) {
  return concat(bfield(1, peerRef(peer)), vfield(2, 7), vfield(3, 2),
    bfield(4, concat(bfield(1, first(status, 17).bytes), vfield(2, Number(first(status, 18).value)),
      vfield(3, Number(first(status, 4).value)), sfield(4, stringOf(first(status, 22))))));
}
function counters(userId) {
  const escaped = userId.replaceAll("'", "''");
  return {
    messages: Number(sql('SELECT count(*) FROM botgram_intake_v1.messages')),
    events: Number(sql('SELECT count(*) FROM botgram_intake_v1.events')),
    outbox: Number(sql('SELECT count(*) FROM botgram_intake_v1.outbox')),
    recipients: Number(sql('SELECT count(*) FROM botgram_intake_v1.event_recipients')),
    pts: Number(sql(`SELECT pts FROM botgram_intake_v1.sync WHERE user_id='${escaped}'`))
  };
}

try {
  await start();
  const suffix = Date.now().toString(36);
  const a = await register(base, 'pts_a_' + suffix, 'pts-password-12345', 'A');
  const b = await register(base, 'pts_b_' + suffix, 'pts-password-12345', 'B');
  const aId = profile(a.user).userId, bId = profile(b.user).userId;

  const firstRandom = -7100000000001n;
  const firstPayload = send(bId, firstRandom, 'first message');
  const firstInfo = sentInfo(await rpc(a.token, 107, firstPayload));
  assert.ok(firstInfo.messageId > 0);
  assert.equal(firstInfo.ptsCount, 1);
  assert.equal(firstInfo.randomId, firstRandom);
  checks++;
  console.log(`  [ok] first send id=${firstInfo.messageId} pts=${firstInfo.pts} pts_count=${firstInfo.ptsCount}`);

  async function retry(label, expected = firstInfo, token = a.token) {
    const before = counters(aId);
    const result = await rpc(token, 107, firstPayload);
    const info = sentInfo(result);
    assert.equal(info.messageId, expected.messageId, `${label}: message id changed`);
    assert.equal(info.pts, expected.pts, `${label}: pts changed (retry returned a cursor, not the original event pts)`);
    assert.equal(info.ptsCount, expected.ptsCount, `${label}: pts_count changed`);
    assert.equal(info.randomId, expected.randomId, `${label}: random id mapping changed`);
    assert.deepEqual(counters(aId), before, `${label}: retry mutated messages/events/outbox/recipients/sync`);
    checks++;
    console.log(`  [ok] retry ${label}: id=${info.messageId} pts=${info.pts} pts_count=${info.ptsCount}, counters unchanged`);
  }

  const secondPayload = send(bId, 7100000000002n, 'second message');
  const second = sentInfo(await rpc(a.token, 107, secondPayload));
  assert.ok(second.pts > firstInfo.pts);
  checks++;
  await retry('after another plaintext message');

  await rpc(a.token, 139, publishBody(0x21, [1, 2]));
  await rpc(b.token, 139, publishBody(0x22, [3, 4]));
  const invite = outcome(await rpc(a.token, 140, concat(bfield(1, peerRef(bId)), vfield(2, 1), vfield(3, 2))), 140);
  const conversation = stringOf(first(invite, 20));
  const accepted = outcome(await rpc(b.token, 140, concat(bfield(1, peerRef(aId)), vfield(2, 2), vfield(3, 2))), 140);
  assert.equal(Number(first(accepted, 2).value), 2);
  checks++;
  await retry('after enabling encryption');

  const epoch = Number(first(accepted, 4).value);
  const encryptedRandom = 7100000000003n;
  const encrypted = await rpc(a.token, 107,
    send(bId, encryptedRandom, null, envelopeBytes(conversation, aId, bId, epoch, encryptedRandom)));
  const encryptedInfo = sentInfo(encrypted);
  assert.ok(encryptedInfo.pts > second.pts);
  checks++;
  await retry('after an encrypted message');

  await rpc(a.token, 140, concat(bfield(1, peerRef(bId)), vfield(2, 5), vfield(3, 2)));
  await rpc(b.token, 140, concat(bfield(1, peerRef(aId)), vfield(2, 6), vfield(3, 2)));
  checks++;
  await retry('after closing encryption');

  await rpc(a.token, 140, concat(bfield(1, peerRef(bId)), vfield(2, 1), vfield(3, 2)));
  const reopened = outcome(await rpc(b.token, 140, concat(bfield(1, peerRef(aId)), vfield(2, 2), vfield(3, 2))), 140);
  assert.equal(Number(first(reopened, 2).value), 2);
  checks++;
  await retry('after reopening encryption');

  await rpc(b.token, 139, publishBody(0x44, [5]));
  const suspended = chatFields(outcome(await rpc(a.token, 141, concat(bfield(1, peerRef(bId)))), 141));
  assert.equal(Number(first(suspended, 2).value), 4);
  checks++;
  await retry('after identity replacement suspends the chat');

  const freshA = chatFields(outcome(await rpc(a.token, 141, concat(bfield(1, peerRef(bId)))), 141));
  const freshB = chatFields(outcome(await rpc(b.token, 141, concat(bfield(1, peerRef(aId)))), 141));
  outcome(await rpc(a.token, 140, confirmBody(bId, freshA)), 140);
  const reactivated = outcome(await rpc(b.token, 140, confirmBody(aId, freshB)), 140);
  assert.equal(Number(first(reactivated, 2).value), 2);
  checks++;
  await retry('after both identity confirmations');

  await stop(); await start();
  checks++;
  await retry('after restart');

  code(await rpc(a.token, 107, send(bId, firstRandom, 'tampered')), 409, 'RANDOM_ID_DUPLICATE');
  checks++;
  console.log('  [ok] same id with different content still conflicts');

  const difference = outcome(await rpc(a.token, 101, vfield(1, firstInfo.pts)), 101);
  const pageEntry = first(difference, 3) ?? first(difference, 4);
  assert.ok(pageEntry, `expected a difference page, got ${[...difference.keys()].join(',')}`);
  const page = fields(pageEntry.bytes);
  const statePts = Number(first(fields(first(page, 5).bytes), 1).value);
  const messageIds = all(page, 1).map(entry => Number(first(fields(entry.bytes), 1).value));
  assert.ok(statePts > firstInfo.pts, 'difference state did not advance');
  assert.ok(messageIds.includes(second.messageId), 'difference lost the intervening plaintext message');
  assert.ok(messageIds.includes(encryptedInfo.messageId), 'difference lost the intervening encrypted message');
  checks++;
  console.log(`  [ok] difference from original pts=${firstInfo.pts} returned ${messageIds.length} messages, state.pts=${statePts}`);

  // Remove the original event_recipients row and prove the retry fails explicitly
  // instead of silently falling back to the current sync cursor.
  sql(`DELETE FROM botgram_intake_v1.event_recipients WHERE user_id='${aId.replaceAll("'", "''")}' AND event_id=(SELECT id FROM botgram_intake_v1.events WHERE message_id=${second.messageId})`);
  code(await rpc(a.token, 107, secondPayload), 500, 'BOTGRAM_MESSAGE_EVENT_PTS_MISSING');
  checks++;
  console.log('  [ok] missing original event recipient fails explicitly, no cursor fallback');

  console.log(`PASS: idempotent retry returns the original persistent pts (${checks} checkpoints)`);
} catch (error) {
  if (logs.trim()) console.error('BACKEND LOGS:\n' + logs);
  throw error;
} finally {
  await stop();
}
