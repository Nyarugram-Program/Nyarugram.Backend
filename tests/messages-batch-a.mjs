// Batch A messages.* regression: 26 operations against a real PostgreSQL-authoritative
// backend (Protobuf-only transport, PostgreSQL-only storage). Uses an isolated database
// only; the production data directory and primary database are never opened.
// Requires: BOTGRAM_E2EE_PG (isolated connection string). Optional: BOTGRAM_TEST_DLL.
import assert from 'node:assert/strict';
import { spawn, spawnSync } from 'node:child_process';
import { once } from 'node:events';
import { mkdtemp } from 'node:fs/promises';
import path from 'node:path';
import net from 'node:net';
import { fileURLToPath } from 'node:url';
import {
  register, envelope, call, expectError, outcome, first, all, fields, stringOf,
  vfield, sfield, bfield, fixed64, concat, profile
} from './protobuf-plane.mjs';

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const temp = await mkdtemp(path.join(process.env.BOTGRAM_TEST_ROOT ?? path.join(root, 'obj'), 'messages-a-'));
const listener = net.createServer().listen(0, '127.0.0.1');
await once(listener, 'listening');
const port = listener.address().port;
await new Promise(resolve => listener.close(resolve));
const base = `http://127.0.0.1:${port}`;
const domain = 'nyarugram.moe.autos';
const connection = process.env.BOTGRAM_E2EE_PG;
if (!connection) throw new Error('BOTGRAM_E2EE_PG must point at an isolated PostgreSQL database');

let server, logs = '', checks = 0, sequence = 0;
const ok = () => { checks++; };

async function start() {
  server = spawn('dotnet', [process.env.BOTGRAM_TEST_DLL ?? path.join(root, 'bin/Debug/net8.0/Botgram.Backend.dll'),
    '--urls', base], {
    cwd: root,
    env: {
      ...process.env,
      ASPNETCORE_ENVIRONMENT: 'Development',
      Storage__Mode: 'NewStorageValidation',
      Storage__ValidationBackend: 'RelationalBackend',
      Storage__ValidationAcknowledged: 'true',
      Storage__ValidationConnectionString: connection,
      Protocol__LegacyJsonRpcEnabled: 'false',
      Protocol__LegacyJsonWssEnabled: 'false',
      Protocol__ProtobufRolloutPercent: '100',
      DataPath: path.join(temp, 'keys.json')
    },
    windowsHide: true, stdio: ['ignore', 'pipe', 'pipe']
  });
  server.stdout.on('data', value => logs += value);
  server.stderr.on('data', value => logs += value);
  for (let i = 0; i < 150; i++) {
    if (server.exitCode !== null) throw Error(logs);
    try { if ((await fetch(base + '/health')).ok) return; } catch {}
    await new Promise(resolve => setTimeout(resolve, 100));
  }
  throw Error('startup timeout: ' + logs);
}
async function stop() {
  if (server && server.exitCode === null) { const exit = once(server, 'exit'); server.kill(); await exit; }
}

// Permanent oneof tags from Protocol/botgram_rpc.proto.
const T = {
  getState: 100, getDialogs: 102, getPinnedDialogs: 103, getHistory: 105, getMessages: 106,
  sendMessage: 107, readHistory: 108, createGroup: 115,
  deleteMessages: 144, deleteHistory: 145, deleteSavedHistory: 146, deletePhoneCallHistory: 147,
  editMessage: 148, getMessageEditData: 149, forwardMessages: 150, setTyping: 151,
  saveDraft: 152, clearAllDrafts: 153, getAllDrafts: 154, updatePinnedMessage: 155,
  unpinAllMessages: 156, toggleDialogPin: 157, reorderPinnedDialogs: 158, markDialogUnread: 159,
  getDialogUnreadMarks: 160, readMessageContents: 161, getOnlines: 162, getCommonChats: 163,
  hidePeerSettingsBar: 164, reportSpam: 165, getMessagesViews: 166, getOutboxReadDate: 167,
  search: 168, getSearchCounters: 169, getDifference: 101
};

const rpc = (token, tag, payload = []) => call(base, '/api/v1/rpc', envelope(tag, payload), token);
const num = (list, field) => { const entry = first(list, field); return entry ? Number(entry.value) : 0; };
const has = (list, field) => Boolean(first(list, field));
const userRef = id => concat(sfield(1, domain), sfield(2, id));
const userPeer = id => concat(bfield(1, userRef(id)));
const selfPeer = () => vfield(3, 1);
const groupPeer = id => sfield(2, id);
const userSelector = id => concat(bfield(1, userRef(id)));
const selfSelector = () => vfield(2, 1);
const randomId = () => BigInt(Date.now()) * 1000n + BigInt(++sequence);

async function send(token, peer, text, random = randomId(), silent = false) {
  const payload = concat(bfield(1, peer), fixed64(2, random), sfield(3, text), vfield(6, silent ? 1 : 0));
  const result = await rpc(token, T.sendMessage, payload);
  const sent = fields(outcome(result, T.sendMessage));
  const message = fields(first(sent, 1).bytes);
  return { result, id: num(message, 1), text: stringOf(first(fields(first(message, 10).bytes), 1)), random };
}

function messageTexts(page) {
  return all(page, 1).map(entry => {
    const body = fields(entry.bytes);
    const text = first(body, 10);
    return text ? stringOf(first(fields(text.bytes), 1)) : null;
  });
}
function messageIds(page) {
  return all(page, 1).map(entry => num(fields(entry.bytes), 1));
}
function dialogPeers(page) {
  return all(page, 1).map(entry => {
    const peer = fields(first(entry.bytes, 1).bytes);
    const user = first(peer, 1);
    if (user) return { user: stringOf(first(fields(user.bytes), 2)) };
    const group = first(peer, 2);
    if (group) return { group: stringOf(group) };
    return { self: true };
  });
}
// Updates.updates is field 1; DifferencePage.updates is field 2.
function envelopeUpdateKinds(updates) {
  return all(updates, 1).map(entry => fields(entry.bytes)[0].number);
}
function differenceUpdateKinds(page) {
  return all(page, 2).map(entry => fields(entry.bytes)[0].number);
}
// Repeated numeric proto3 fields arrive packed: decode the length-delimited payload.
function packedInts(entry) {
  const bytes = Buffer.from(entry.bytes);
  const result = [];
  let index = 0;
  while (index < bytes.length) {
    let value = 0n, shift = 0n;
    for (;;) {
      const byte = bytes[index++];
      value |= BigInt(byte & 0x7f) << shift; shift += 7n;
      if (!(byte & 0x80)) break;
    }
    result.push(Number(value));
  }
  return result;
}
function viewsOf(result) {
  const list = fields(outcome(result, T.getMessagesViews));
  const entry = first(list, 1);
  return entry ? packedInts(entry) : [];
}
// Update.new_message = 1 -> NewMessage.message = 1 -> Message fields.
function forwardedMessage(updates) {
  const update = all(updates, 1).map(entry => fields(entry.bytes))
    .find(entry => entry[0].number === 1);
  const newMessage = fields(first(update, 1).bytes);
  return { message: fields(first(newMessage, 1).bytes), pts: num(newMessage, 2) };
}
// Direct database assertions for side-effect freedom. Uses the same isolated connection
// string the backend received; credentials stay in the process environment.
function dbQuery(sql) {
  const psql = process.env.BOTGRAM_PSQL;
  if (!psql) return null;
  const parts = {};
  for (const segment of connection.split(';')) {
    const index = segment.indexOf('=');
    if (index > 0) parts[segment.slice(0, index).trim().toLowerCase()] = segment.slice(index + 1).trim();
  }
  const result = spawnSync(psql, ['-h', parts.host, '-p', parts.port, '-U', parts.username,
    '-d', parts.database, '-qtA', '-c', sql],
    { env: { ...process.env, PGPASSWORD: parts.password ?? '' }, encoding: 'utf8' });
  if (result.status !== 0) throw new Error('psql failed: ' + result.stderr);
  return result.stdout.trim();
}
function dbCounts() {
  const value = dbQuery("SELECT (SELECT count(*) FROM botgram_intake_v1.messages)||'/'||" +
    "(SELECT count(*) FROM botgram_intake_v1.events)||'/'||(SELECT count(*) FROM botgram_intake_v1.outbox)");
  return value ?? 'unavailable';
}
function publishBody(identityKey) {
  const oneTime = [];
  for (let i = 0; i < 5; i++) {
    oneTime.push(...bfield(10, concat(vfield(1, 100 + i), bfield(2, Buffer.alloc(33, i + 1)))));
  }
  return concat(vfield(1, 1), vfield(2, 1234), bfield(3, identityKey),
    vfield(4, 10), bfield(5, Buffer.alloc(33, 2)), bfield(6, Buffer.alloc(32, 3)),
    vfield(7, 11), bfield(8, Buffer.alloc(800, 4)), bfield(9, Buffer.alloc(32, 5)), oneTime);
}
function actionBody(peer, action) {
  return concat(bfield(1, peer), vfield(2, action), vfield(3, 2));
}

async function pts(token) {
  return num(fields(outcome(await rpc(token, T.getState), T.getState)), 1);
}

try {
  await start();
  const suffix = Date.now().toString(36);
  const password = 'batch-a-password';
  const a = await register(base, 'batch_a_alice_' + suffix, password, 'Alice', 'CN');
  const b = await register(base, 'batch_a_bob_' + suffix, password, 'Bob');
  const c = await register(base, 'batch_a_carol_' + suffix, password, 'Carol');
  const alice = a.token, bob = b.token, carol = c.token;
  const aliceId = profile(a.user).userId, bobId = profile(b.user).userId, carolId = profile(c.user).userId;
  ok();

  // ---------------------------------------------------------------- setup
  const m1 = await send(alice, userPeer(bobId), 'hello alpha world');
  assert.equal(m1.text, 'hello alpha world');
  const groupResult = await rpc(alice, T.createGroup,
    concat(sfield(1, 'Batch A group'), bfield(2, userSelector(bobId))));
  const group = fields(outcome(groupResult, T.createGroup));
  const groupId = stringOf(first(fields(first(group, 1).bytes), 1));
  assert.match(groupId, /^[0-9a-f]{32}$/);
  ok();

  // ---------------------------------------------------------------- 148/149 edit
  const edited = await rpc(alice, T.editMessage,
    concat(bfield(1, userPeer(bobId)), vfield(2, m1.id), sfield(3, 'hello alpha edited')));
  const editedUpdates = fields(outcome(edited, T.editMessage));
  assert.ok(envelopeUpdateKinds(editedUpdates).includes(11), 'updateEditMessage present');
  ok();
  expectError(await rpc(bob, T.editMessage,
    concat(bfield(1, userPeer(aliceId)), vfield(2, m1.id), sfield(3, 'stolen'))),
    403, 'MESSAGE_AUTHOR_REQUIRED');
  ok();
  expectError(await rpc(alice, T.editMessage,
    concat(bfield(1, userPeer(bobId)), vfield(2, 2147483000), sfield(3, 'missing'))),
    400, 'MESSAGE_ID_INVALID');
  ok();
  // Same text again is not a silent no-op success.
  expectError(await rpc(alice, T.editMessage,
    concat(bfield(1, userPeer(bobId)), vfield(2, m1.id), sfield(3, 'hello alpha edited'))),
    400, 'MESSAGE_NOT_MODIFIED');
  ok();
  const editData = fields(outcome(await rpc(alice, T.getMessageEditData,
    concat(bfield(1, userPeer(bobId)), vfield(2, m1.id))), T.getMessageEditData));
  assert.equal(num(editData, 1), 0);
  ok();

  // ---------------------------------------------------------------- 168 search
  const found = fields(outcome(await rpc(bob, T.search,
    concat(bfield(1, userPeer(aliceId)), sfield(2, 'alpha'), vfield(3, 1), vfield(9, 50))), T.search));
  assert.equal(num(found, 4), 1);
  assert.deepEqual(messageTexts(found), ['hello alpha edited']);
  ok();
  const miss = fields(outcome(await rpc(bob, T.search,
    concat(bfield(1, userPeer(aliceId)), sfield(2, 'no-such-token'), vfield(3, 1))), T.search));
  assert.equal(num(miss, 4), 0);
  ok();
  // A non-member must not reach the conversation at all.
  expectError(await rpc(carol, T.search,
    concat(bfield(1, userPeer(aliceId)), sfield(2, 'alpha'), vfield(3, 1))), 404, 'PEER_ID_INVALID');
  ok();

  // ---------------------------------------------------------------- 169 counters
  const counters = fields(outcome(await rpc(bob, T.getSearchCounters,
    concat(bfield(1, userPeer(aliceId)), vfield(2, 1), vfield(2, 2))), T.getSearchCounters));
  const entries = all(counters, 1);
  assert.equal(entries.length, 2);
  assert.equal(num(fields(entries[0].bytes), 1), 1);
  assert.equal(num(fields(entries[0].bytes), 2), 1);
  assert.equal(num(fields(entries[1].bytes), 2), 0);
  ok();

  // ---------------------------------------------------------------- 155/156 pins + counters
  const pinned = await rpc(alice, T.updatePinnedMessage,
    concat(bfield(1, userPeer(bobId)), vfield(2, m1.id)));
  assert.ok(envelopeUpdateKinds(fields(outcome(pinned, T.updatePinnedMessage))).includes(12));
  ok();
  const pinnedCounters = fields(outcome(await rpc(bob, T.getSearchCounters,
    concat(bfield(1, userPeer(aliceId)), vfield(2, 2))), T.getSearchCounters));
  assert.equal(num(fields(all(pinnedCounters, 1)[0].bytes), 2), 1);
  ok();
  const pinnedSearch = fields(outcome(await rpc(bob, T.search,
    concat(bfield(1, userPeer(aliceId)), vfield(3, 2))), T.search));
  assert.deepEqual(messageIds(pinnedSearch), [m1.id]);
  ok();
  await rpc(alice, T.unpinAllMessages, bfield(1, userPeer(bobId)));
  const afterUnpin = fields(outcome(await rpc(bob, T.getSearchCounters,
    concat(bfield(1, userPeer(aliceId)), vfield(2, 2))), T.getSearchCounters));
  assert.equal(num(fields(all(afterUnpin, 1)[0].bytes), 2), 0);
  ok();

  // ---------------------------------------------------------------- 166 views (unique viewers)
  const firstView = viewsOf(await rpc(bob, T.getMessagesViews,
    concat(bfield(1, userPeer(aliceId)), vfield(2, m1.id), vfield(3, 1))));
  assert.deepEqual(firstView, [1]);
  const repeatView = viewsOf(await rpc(bob, T.getMessagesViews,
    concat(bfield(1, userPeer(aliceId)), vfield(2, m1.id), vfield(3, 1))));
  assert.deepEqual(repeatView, [1], 'same viewer never inflates the counter');
  await rpc(alice, T.getMessagesViews,
    concat(bfield(1, userPeer(bobId)), vfield(2, m1.id), vfield(3, 1)));
  const twoViews = viewsOf(await rpc(bob, T.getMessagesViews,
    concat(bfield(1, userPeer(aliceId)), vfield(2, m1.id), vfield(3, 0))));
  assert.deepEqual(twoViews, [2]);
  ok();

  // ---------------------------------------------------------------- 167 outbox read date
  expectError(await rpc(alice, T.getOutboxReadDate,
    concat(bfield(1, userPeer(bobId)), vfield(2, m1.id))), 403, 'USER_PRIVACY_RESTRICTED');
  ok();
  await rpc(bob, T.readHistory, concat(bfield(1, userPeer(aliceId)), vfield(2, m1.id)));
  const readDate = fields(outcome(await rpc(alice, T.getOutboxReadDate,
    concat(bfield(1, userPeer(bobId)), vfield(2, m1.id))), T.getOutboxReadDate));
  assert.ok(num(readDate, 1) > 0);
  ok();

  // ---------------------------------------------------------------- 161 read contents idempotency
  const contents = fields(outcome(await rpc(bob, T.readMessageContents, vfield(1, m1.id)),
    T.readMessageContents));
  assert.equal(num(contents, 2), 1);
  const contentsAgain = fields(outcome(await rpc(bob, T.readMessageContents, vfield(1, m1.id)),
    T.readMessageContents));
  assert.equal(num(contentsAgain, 2), 0);
  ok();

  // ---------------------------------------------------------------- 151 typing (pts + expiry)
  const bobBeforeTyping = await pts(bob);
  await rpc(alice, T.setTyping, concat(bfield(1, userPeer(bobId)), vfield(2, 1)));
  const typingDifference = fields(outcome(await rpc(bob, T.getDifference,
    vfield(1, bobBeforeTyping)), T.getDifference));
  const typingPage = fields(first(typingDifference, 3).bytes);
  assert.ok(differenceUpdateKinds(typingPage).includes(15), 'user typing reaches the peer');
  ok();
  await new Promise(resolve => setTimeout(resolve, 6500));
  const expiredDifference = fields(outcome(await rpc(bob, T.getDifference,
    vfield(1, bobBeforeTyping)), T.getDifference));
  const expiredPage = fields(first(expiredDifference, 3).bytes);
  assert.ok(!differenceUpdateKinds(expiredPage).includes(15), 'expired typing is never replayed');
  assert.ok(num(fields(first(expiredPage, 5).bytes), 1) > bobBeforeTyping);
  ok();

  // ---------------------------------------------------------------- 152/153/154 drafts
  await rpc(alice, T.saveDraft,
    concat(bfield(1, userPeer(bobId)), sfield(2, 'draft text'), vfield(4, m1.id)));
  const drafts = fields(outcome(await rpc(alice, T.getAllDrafts), T.getAllDrafts));
  const draftUpdates = all(drafts, 1).filter(entry => fields(entry.bytes)[0].number === 14);
  assert.equal(draftUpdates.length, 1);
  // Update.draft_changed is field 14; DraftChanged.draft is field 2; DraftState.text is field 1.
  const draftPayload = fields(first(draftUpdates[0].bytes, 14).bytes);
  assert.equal(stringOf(first(fields(first(draftPayload, 2).bytes), 1)), 'draft text');
  ok();
  assert.equal(all(fields(outcome(await rpc(bob, T.getAllDrafts), T.getAllDrafts)), 1).length, 0,
    'drafts stay account-private');
  ok();
  const dialogsWithDraft = fields(outcome(await rpc(alice, T.getDialogs, vfield(1, 100)), T.getDialogs));
  const aliceDialogs = all(dialogsWithDraft, 1).map(entry => fields(entry.bytes));
  const directDialog = aliceDialogs.find(dialog => {
    const user = first(fields(first(dialog, 1).bytes), 1);
    return user && stringOf(first(fields(user.bytes), 2)) === bobId;
  });
  assert.ok(directDialog && has(directDialog, 9), 'dialog carries the stored draft');
  ok();
  await rpc(alice, T.clearAllDrafts);
  assert.equal(all(fields(outcome(await rpc(alice, T.getAllDrafts), T.getAllDrafts)), 1).length, 0);
  ok();

  // ---------------------------------------------------------------- 150 forward
  const forwardRandom = randomId();
  const forwarded = await rpc(alice, T.forwardMessages, concat(
    bfield(1, userPeer(bobId)), bfield(2, groupPeer(groupId)),
    vfield(3, m1.id), fixed64(4, forwardRandom), vfield(5, 0), vfield(6, 0)));
  const forwardUpdates = fields(outcome(forwarded, T.forwardMessages));
  const kinds = envelopeUpdateKinds(forwardUpdates);
  assert.ok(kinds.includes(2), 'updateMessageID maps the forward random id');
  assert.ok(kinds.includes(1), 'updateNewMessage carries the copy');
  const copy = forwardedMessage(forwardUpdates);
  assert.equal(stringOf(first(fields(first(copy.message, 10).bytes), 1)), 'hello alpha edited');
  assert.ok(has(copy.message, 13), 'forward attribution is preserved');
  ok();
  // The same random_id retried returns the same message instead of a duplicate.
  const retried = await rpc(alice, T.forwardMessages, concat(
    bfield(1, userPeer(bobId)), bfield(2, groupPeer(groupId)),
    vfield(3, m1.id), fixed64(4, forwardRandom)));
  const retriedUpdates = fields(outcome(retried, T.forwardMessages));
  const retriedCopy = forwardedMessage(retriedUpdates);
  assert.equal(retriedCopy.message ? num(retriedCopy.message, 1) : 0, num(copy.message, 1));
  ok();

  // ---------------------------------------------------------------- 144 delete (self vs revoke)
  await rpc(bob, T.deleteMessages, concat(vfield(1, m1.id), vfield(2, 0)));
  const bobHistory = fields(outcome(await rpc(bob, T.getHistory,
    concat(bfield(1, userPeer(aliceId)), vfield(7, 50))), T.getHistory));
  assert.deepEqual(messageIds(bobHistory), [], 'hidden for the caller only');
  const aliceStillSees = fields(outcome(await rpc(alice, T.getHistory,
    concat(bfield(1, userPeer(bobId)), vfield(7, 50))), T.getHistory));
  assert.deepEqual(messageIds(aliceStillSees), [m1.id], 'peer view is untouched');
  ok();
  await rpc(alice, T.deleteMessages, concat(vfield(1, m1.id), vfield(2, 1)));
  const aliceHistory = fields(outcome(await rpc(alice, T.getHistory,
    concat(bfield(1, userPeer(bobId)), vfield(7, 50))), T.getHistory));
  assert.deepEqual(messageIds(aliceHistory), []);
  const leaked = fields(outcome(await rpc(alice, T.search,
    concat(bfield(1, userPeer(bobId)), sfield(2, 'alpha'), vfield(3, 1))), T.search));
  assert.equal(num(leaked, 4), 0, 'deleted content leaves search');
  ok();
  // Group revoke of another author's message is forbidden.
  const groupMessage = await send(alice, groupPeer(groupId), 'group alpha');
  expectError(await rpc(bob, T.deleteMessages, concat(vfield(1, groupMessage.id), vfield(2, 1))),
    403, 'CHAT_ADMIN_REQUIRED');
  ok();

  // ---------------------------------------------------------------- 145 delete history
  await rpc(alice, T.deleteHistory,
    concat(bfield(1, groupPeer(groupId)), vfield(2, 1), vfield(3, 0)));
  const clearedGroup = fields(outcome(await rpc(alice, T.getHistory,
    concat(bfield(1, groupPeer(groupId)), vfield(7, 50))), T.getHistory));
  assert.deepEqual(messageIds(clearedGroup), [], 'just_clear empties history');
  const stillListed = fields(outcome(await rpc(alice, T.getDialogs, vfield(1, 100)), T.getDialogs));
  assert.ok(dialogPeers(stillListed).some(peer => peer.group === groupId),
    'just_clear keeps the dialog listed');
  ok();
  await rpc(alice, T.deleteHistory,
    concat(bfield(1, groupPeer(groupId)), vfield(2, 0), vfield(3, 0)));
  const hiddenDialogs = fields(outcome(await rpc(alice, T.getDialogs, vfield(1, 100)), T.getDialogs));
  assert.ok(!dialogPeers(hiddenDialogs).some(peer => peer.group === groupId),
    'delete history hides the dialog until a newer message id arrives');
  ok();
  const revived = await send(bob, groupPeer(groupId), 'group beta');
  const revivedDialogs = fields(outcome(await rpc(alice, T.getDialogs, vfield(1, 100)), T.getDialogs));
  assert.ok(dialogPeers(revivedDialogs).some(peer => peer.group === groupId), 'new message revives it');
  ok();

  // ---------------------------------------------------------------- 146 saved history
  const saved = await send(alice, selfPeer(), 'saved note');
  await rpc(alice, T.deleteSavedHistory, concat(bfield(1, selfPeer()), vfield(2, 0)));
  const savedHistory = fields(outcome(await rpc(alice, T.getHistory,
    concat(bfield(1, selfPeer()), vfield(7, 50))), T.getHistory));
  assert.deepEqual(messageIds(savedHistory), []);
  const savedDialogs = fields(outcome(await rpc(alice, T.getDialogs, vfield(1, 100)), T.getDialogs));
  // Saved messages are addressed as the caller's own user peer in this model.
  assert.ok(dialogPeers(savedDialogs).some(peer => peer.user === aliceId), 'saved dialog stays visible');
  ok();
  assert.ok(saved.id > 0);

  // ---------------------------------------------------------------- 147 phone call history
  const callHistory = fields(outcome(await rpc(alice, T.deletePhoneCallHistory, vfield(1, 0)),
    T.deletePhoneCallHistory));
  assert.equal(num(callHistory, 3), 0, 'phone-call deletion terminates the client loop');
  ok();

  // ---------------------------------------------------------------- 157/158 dialog pin + pinned list
  await rpc(alice, T.toggleDialogPin, concat(bfield(1, userPeer(bobId)), vfield(2, 1)));
  await rpc(alice, T.reorderPinnedDialogs,
    concat(bfield(1, userPeer(bobId)), vfield(2, 1)));
  const pinnedDialogs = fields(outcome(await rpc(alice, T.getPinnedDialogs), T.getPinnedDialogs));
  const pinnedDialog = all(pinnedDialogs, 1).map(entry => fields(entry.bytes))
    .find(dialog => {
      const user = first(fields(first(dialog, 1).bytes), 1);
      return user && stringOf(first(fields(user.bytes), 2)) === bobId;
    });
  assert.ok(pinnedDialog, 'pinned dialog is returned by getPinnedDialogs');
  assert.ok(has(pinnedDialog, 7), 'pinned flag is set');
  ok();
  await rpc(alice, T.toggleDialogPin, concat(bfield(1, userPeer(bobId)), vfield(2, 0)));
  const unpinnedDialogs = fields(outcome(await rpc(alice, T.getPinnedDialogs), T.getPinnedDialogs));
  assert.equal(all(unpinnedDialogs, 1).length, 0);
  ok();

  // ---------------------------------------------------------------- 159/160 unread marks
  await rpc(alice, T.markDialogUnread, concat(bfield(1, groupPeer(groupId)), vfield(2, 1)));
  const marks = fields(outcome(await rpc(alice, T.getDialogUnreadMarks), T.getDialogUnreadMarks));
  assert.ok(all(marks, 1).some(entry => {
    const peer = fields(entry.bytes);
    const groupField = first(peer, 2);
    return groupField && stringOf(groupField) === groupId;
  }));
  ok();
  await rpc(alice, T.markDialogUnread, concat(bfield(1, groupPeer(groupId)), vfield(2, 0)));
  assert.equal(all(fields(outcome(await rpc(alice, T.getDialogUnreadMarks), T.getDialogUnreadMarks)), 1).length, 0);
  ok();

  // ---------------------------------------------------------------- 164/165 settings bar + spam
  assert.equal(num(fields(outcome(await rpc(alice, T.hidePeerSettingsBar,
    bfield(1, userPeer(bobId))), T.hidePeerSettingsBar)), 1), 1);
  assert.equal(num(fields(outcome(await rpc(alice, T.reportSpam,
    bfield(1, userPeer(bobId))), T.reportSpam)), 1), 1);
  ok();

  // ---------------------------------------------------------------- 162 onlines
  const onlines = fields(outcome(await rpc(alice, T.getOnlines, bfield(1, groupPeer(groupId))),
    T.getOnlines));
  assert.ok(num(onlines, 1) >= 0);
  expectError(await rpc(alice, T.getOnlines, bfield(1, userPeer(bobId))), 400, 'PEER_ID_INVALID');
  ok();

  // ---------------------------------------------------------------- 163 common chats
  const common = fields(outcome(await rpc(alice, T.getCommonChats,
    concat(bfield(1, userSelector(bobId)), vfield(3, 100))), T.getCommonChats));
  assert.ok(all(common, 1).some(entry => stringOf(first(fields(entry.bytes), 1)) === groupId));
  ok();
  expectError(await rpc(alice, T.getCommonChats,
    bfield(1, userSelector('00000000000000000000000000000000'))), 404, 'USER_ID_INVALID');
  ok();

  // ================================================================ review fixes
  // 1. Forwarding must pass the same E2EE admission as an ordinary send.
  const bcSource = await send(bob, userPeer(carolId), 'e2ee forward source');
  await rpc(bob, 139, publishBody(Buffer.alloc(33, 0x51)));
  await rpc(carol, 139, publishBody(Buffer.alloc(33, 0x52)));
  assert.equal((await rpc(bob, 140, actionBody(userPeer(carolId), 1))).status, 200);
  assert.equal((await rpc(carol, 140, actionBody(userPeer(bobId), 2))).status, 200);
  const bcHistoryBefore = messageIds(fields(outcome(await rpc(bob, T.getHistory,
    concat(bfield(1, userPeer(carolId)), vfield(7, 50))), T.getHistory)));
  const bobPtsBeforeForward = await pts(bob);
  const countsBeforeForward = dbCounts();
  if (countsBeforeForward === 'unavailable')
    console.log('NOT RUN: direct database counters (BOTGRAM_PSQL unset)');
  expectError(await rpc(bob, T.forwardMessages, concat(
    bfield(1, userPeer(carolId)), bfield(2, userPeer(carolId)),
    vfield(3, bcSource.id), fixed64(4, randomId()))), 409, 'E2EE_PLAINTEXT_FORBIDDEN');
  ok();
  assert.equal(await pts(bob), bobPtsBeforeForward, 'rejected forward leaves pts untouched');
  assert.deepEqual(messageIds(fields(outcome(await rpc(bob, T.getHistory,
    concat(bfield(1, userPeer(carolId)), vfield(7, 50))), T.getHistory))), bcHistoryBefore,
    'rejected forward stores no message');
  assert.equal(dbCounts(), countsBeforeForward, 'rejected forward leaves message/event/outbox rows untouched');
  const emptyDifference = fields(outcome(await rpc(bob, T.getDifference,
    vfield(1, bobPtsBeforeForward)), T.getDifference));
  assert.ok(has(emptyDifference, 1), 'rejected forward emits no replayable event');
  ok();

  // The reverse direction of a one-way (pending) chat still accepts plaintext, so a
  // forward there must succeed rather than being blocked by the admission check.
  const acSource = await send(alice, userPeer(carolId), 'one-way source');
  assert.equal((await rpc(carol, 140, actionBody(userPeer(aliceId), 1))).status, 200);
  const oneWayForward = await rpc(alice, T.forwardMessages, concat(
    bfield(1, userPeer(carolId)), bfield(2, userPeer(carolId)),
    vfield(3, acSource.id), fixed64(4, randomId())));
  const oneWayCopy = forwardedMessage(fields(outcome(oneWayForward, T.forwardMessages)));
  assert.equal(stringOf(first(fields(first(oneWayCopy.message, 10).bytes), 1)), 'one-way source');
  ok();

  // 2. View counts must be limited to the requested conversation and the caller's view.
  const viewDirect = await send(alice, userPeer(bobId), 'views direct');
  const viewGroup = await send(alice, groupPeer(groupId), 'views group');
  await rpc(bob, T.getMessagesViews,
    concat(bfield(1, userPeer(aliceId)), vfield(2, viewDirect.id), vfield(3, 1)));
  await rpc(bob, T.getMessagesViews,
    concat(bfield(1, groupPeer(groupId)), vfield(2, viewGroup.id), vfield(3, 1)));
  assert.deepEqual(viewsOf(await rpc(bob, T.getMessagesViews,
    concat(bfield(1, groupPeer(groupId)), vfield(2, viewGroup.id)))), [1], 'legitimate peer reports its count');
  assert.deepEqual(viewsOf(await rpc(bob, T.getMessagesViews,
    concat(bfield(1, userPeer(aliceId)), vfield(2, viewGroup.id)))), [0],
    'another conversation never reveals its view count');
  assert.deepEqual(viewsOf(await rpc(bob, T.getMessagesViews,
    concat(bfield(1, userPeer(aliceId)), vfield(2, viewDirect.id), vfield(2, viewGroup.id)))),
    [1, 0], 'positional results keep the requested order');
  // A member who has not counted yet must not be able to add a viewer through a wrong
  // peer: the earlier bob checks would pass even without the increment-side filter because
  // the unique-viewer rule already pinned the count at one.
  const trioResult = await rpc(alice, T.createGroup, concat(sfield(1, 'Views trio'),
    bfield(2, userSelector(bobId)), bfield(2, userSelector(carolId))));
  const trioId = stringOf(first(fields(first(fields(outcome(trioResult, T.createGroup)), 1).bytes), 1));
  const trioMessage = await send(alice, groupPeer(trioId), 'views trio target');
  await send(carol, selfPeer(), 'carol saved note');
  await rpc(carol, T.getMessagesViews,
    concat(bfield(1, selfPeer()), vfield(2, trioMessage.id), vfield(3, 1)));
  assert.deepEqual(viewsOf(await rpc(carol, T.getMessagesViews,
    concat(bfield(1, groupPeer(trioId)), vfield(2, trioMessage.id)))), [0],
    'a wrong peer adds no viewer and reports zero');
  const viewerRowsAfterWrongPeer = dbQuery(
    `SELECT count(*) FROM botgram_backend_v1.message_viewers WHERE message_id=${trioMessage.id} AND user_id='${carolId}'`);
  if (viewerRowsAfterWrongPeer !== null)
    assert.equal(viewerRowsAfterWrongPeer, '0', 'a wrong peer writes no viewer row');
  await rpc(carol, T.getMessagesViews,
    concat(bfield(1, groupPeer(trioId)), vfield(2, trioMessage.id), vfield(3, 1)));
  assert.deepEqual(viewsOf(await rpc(alice, T.getMessagesViews,
    concat(bfield(1, groupPeer(trioId)), vfield(2, trioMessage.id)))), [1],
    'the correct peer increments exactly once');
  const viewerRowsAfterCorrectPeer = dbQuery(
    `SELECT count(*) FROM botgram_backend_v1.message_viewers WHERE message_id=${trioMessage.id} AND user_id='${carolId}'`);
  if (viewerRowsAfterCorrectPeer !== null)
    assert.equal(viewerRowsAfterCorrectPeer, '1', 'the viewer row is written exactly once');
  assert.deepEqual(viewsOf(await rpc(carol, T.getMessagesViews,
    concat(bfield(1, selfPeer()), vfield(2, viewGroup.id)))), [0],
    'a non-member never learns a view count');
  ok();

  // 3. Message operations must honour the declared peer.
  expectError(await rpc(alice, T.editMessage, concat(
    bfield(1, groupPeer(groupId)), vfield(2, viewDirect.id), sfield(3, 'wrong peer'))),
    400, 'MESSAGE_ID_INVALID');
  ok();
  expectError(await rpc(alice, T.getMessageEditData, concat(
    bfield(1, groupPeer(groupId)), vfield(2, viewDirect.id))), 400, 'MESSAGE_ID_INVALID');
  ok();
  const countsBeforeMismatch = dbCounts();
  const ptsBeforeMismatch = await pts(alice);
  expectError(await rpc(alice, T.forwardMessages, concat(
    bfield(1, userPeer(bobId)), bfield(2, userPeer(bobId)),
    vfield(3, viewGroup.id), fixed64(4, randomId()))), 400, 'MESSAGE_ID_INVALID');
  ok();
  expectError(await rpc(alice, T.forwardMessages, concat(
    bfield(1, userPeer(bobId)), bfield(2, userPeer(bobId)),
    vfield(3, viewDirect.id), vfield(3, viewGroup.id), fixed64(4, randomId()), fixed64(4, randomId()))),
    400, 'MESSAGE_ID_INVALID');
  ok();
  assert.equal(dbCounts(), countsBeforeMismatch, 'mixed/batch peer violations roll back completely');
  assert.equal(await pts(alice), ptsBeforeMismatch, 'peer violations leave pts untouched');
  expectError(await rpc(alice, T.getOutboxReadDate, concat(
    bfield(1, userPeer(carolId)), vfield(2, viewDirect.id))), 400, 'MESSAGE_ID_INVALID');
  ok();

  // 4. pts_count must match the events a multi-conversation request really produced.
  await send(alice, userPeer(bobId), 'multi delete direct');
  const multiDirect = await send(alice, userPeer(bobId), 'multi delete direct 2');
  const multiGroup = await send(alice, groupPeer(groupId), 'multi delete group');
  const ptsBeforeMultiDelete = await pts(alice);
  const multiDelete = fields(outcome(await rpc(alice, T.deleteMessages, concat(
    vfield(1, multiDirect.id), vfield(1, multiGroup.id), vfield(2, 1))), T.deleteMessages));
  assert.equal(num(multiDelete, 2), 2, 'two conversations produce two events');
  assert.equal(num(multiDelete, 1), ptsBeforeMultiDelete + 2, 'pts reflects both events');
  ok();
  const repeatedDelete = fields(outcome(await rpc(alice, T.deleteMessages, concat(
    vfield(1, multiDirect.id), vfield(1, multiGroup.id), vfield(2, 1))), T.deleteMessages));
  assert.equal(num(repeatedDelete, 2), 0, 'a repeated delete produces no event');
  assert.equal(num(repeatedDelete, 1), ptsBeforeMultiDelete + 2, 'a repeated delete keeps pts');
  ok();
  const partialDirect = await send(alice, userPeer(bobId), 'partial direct');
  const partialGroup = await send(alice, groupPeer(groupId), 'partial group');
  await rpc(alice, T.deleteMessages, concat(vfield(1, partialDirect.id), vfield(2, 1)));
  const ptsBeforePartial = await pts(alice);
  const partialDelete = fields(outcome(await rpc(alice, T.deleteMessages, concat(
    vfield(1, partialDirect.id), vfield(1, partialGroup.id), vfield(2, 1))), T.deleteMessages));
  assert.equal(num(partialDelete, 2), 1, 'an already-deleted message contributes no event');
  assert.equal(num(partialDelete, 1), ptsBeforePartial + 1, 'pts counts only new events');
  ok();

  const contentDirect = await send(alice, userPeer(bobId), 'read contents direct');
  const contentGroup = await send(alice, groupPeer(groupId), 'read contents group');
  const bobPtsBeforeContents = await pts(bob);
  const contentRead = fields(outcome(await rpc(bob, T.readMessageContents, concat(
    vfield(1, contentDirect.id), vfield(1, contentGroup.id))), T.readMessageContents));
  assert.equal(num(contentRead, 2), 2, 'two conversations produce two content events');
  assert.equal(num(contentRead, 1), bobPtsBeforeContents + 2, 'content pts reflects both events');
  ok();
  const repeatedContents = fields(outcome(await rpc(bob, T.readMessageContents, concat(
    vfield(1, contentDirect.id), vfield(1, contentGroup.id))), T.readMessageContents));
  assert.equal(num(repeatedContents, 2), 0, 'a repeated content read produces no event');
  ok();
  const contentDifference = fields(outcome(await rpc(bob, T.getDifference,
    vfield(1, bobPtsBeforeContents)), T.getDifference));
  const contentPage = fields(first(contentDifference, 3).bytes);
  assert.equal(all(contentPage, 2).length, 2, 'difference replays both content events');
  assert.equal(num(fields(first(contentPage, 5).bytes), 1), bobPtsBeforeContents + 2,
    'difference state advances by exactly two');
  ok();

  // 5. A committed forward is identified by its persisted request identity, so source
  // edits, deletions and cleared history never turn a retry into a conflict. Each scenario
  // uses its own source message so one deletion cannot mask the next.
  const retryGroupResult = await rpc(alice, T.createGroup, concat(sfield(1, 'Retry source'),
    bfield(2, userSelector(bobId))));
  const retryGroupId = stringOf(first(fields(first(fields(outcome(retryGroupResult, T.createGroup)), 1).bytes), 1));
  const forwardText = message => stringOf(first(fields(first(message, 10).bytes), 1));
  const retryBody = (source, target, random) => concat(bfield(1, groupPeer(retryGroupId)),
    bfield(2, target), vfield(3, source.id), fixed64(4, random));
  const sourceHistoryIds = async token => messageIds(fields(outcome(await rpc(token, T.getHistory,
    concat(bfield(1, groupPeer(retryGroupId)), vfield(7, 50))), T.getHistory)));

  async function forwardSource(source, random, label) {
    const response = await rpc(alice, T.forwardMessages, retryBody(source, groupPeer(groupId), random));
    const copy = forwardedMessage(fields(outcome(response, T.forwardMessages)));
    const result = { id: num(copy.message, 1), pts: copy.pts, text: forwardText(copy.message) };
    assert.ok(result.id > 0 && result.text.length > 0, label);
    return result;
  }
  async function retrySame(source, random, expected, label) {
    const beforeCounts = dbCounts();
    const beforePts = await pts(alice);
    const response = await rpc(alice, T.forwardMessages, retryBody(source, groupPeer(groupId), random));
    const copy = forwardedMessage(fields(outcome(response, T.forwardMessages)));
    assert.equal(num(copy.message, 1), expected.id, label + ': same message id');
    assert.equal(copy.pts, expected.pts, label + ': original pts');
    assert.equal(forwardText(copy.message), expected.text, label + ': original content');
    assert.equal(dbCounts(), beforeCounts, label + ': no new message/event/outbox rows');
    assert.equal(await pts(alice), beforePts, label + ': no pts advance');
    ok();
  }

  // 5a. Source edited after the commit.
  const editSource = await send(alice, groupPeer(retryGroupId), 'retry edit v1');
  const editRandom = randomId();
  const editForward = await forwardSource(editSource, editRandom, 'source edit forward');
  assert.equal(editForward.text, 'retry edit v1');
  await rpc(alice, T.editMessage, concat(bfield(1, groupPeer(retryGroupId)),
    vfield(2, editSource.id), sfield(3, 'retry edit v2')));
  await retrySame(editSource, editRandom, editForward, 'retry after source edit');

  // A different request identity on the same random_id is a conflict, not a retry.
  const otherSource = await send(alice, groupPeer(retryGroupId), 'retry other source');
  const foreignSource = await send(alice, groupPeer(groupId), 'foreign source');
  expectError(await rpc(alice, T.forwardMessages, retryBody(otherSource, groupPeer(groupId), editRandom)),
    409, 'RANDOM_ID_DUPLICATE');
  ok();
  expectError(await rpc(alice, T.forwardMessages, retryBody(foreignSource, groupPeer(groupId), editRandom)),
    409, 'RANDOM_ID_DUPLICATE');
  ok();
  expectError(await rpc(alice, T.forwardMessages, retryBody(editSource, userPeer(carolId), editRandom)),
    409, 'RANDOM_ID_DUPLICATE');
  ok();
  // Options are part of the request identity; concat() does not flatten a nested
  // Uint8Array, so these payloads are written inline.
  expectError(await rpc(alice, T.forwardMessages, concat(
    bfield(1, groupPeer(retryGroupId)), bfield(2, groupPeer(groupId)),
    vfield(3, editSource.id), fixed64(4, editRandom), vfield(5, 1))),
    409, 'RANDOM_ID_DUPLICATE');
  ok();
  expectError(await rpc(alice, T.forwardMessages, concat(
    bfield(1, groupPeer(retryGroupId)), bfield(2, groupPeer(groupId)),
    vfield(3, editSource.id), fixed64(4, editRandom), vfield(6, 1))),
    409, 'RANDOM_ID_DUPLICATE');
  ok();

  // 5b. Caller hides the source.
  const hideSource = await send(alice, groupPeer(retryGroupId), 'retry hide source');
  const hideRandom = randomId();
  const hideForward = await forwardSource(hideSource, hideRandom, 'source hide forward');
  await rpc(alice, T.deleteMessages, concat(vfield(1, hideSource.id), vfield(2, 0)));
  const hideRow = dbQuery(`SELECT count(*) FROM botgram_backend_v1.deleted_messages ` +
    `WHERE user_id='${aliceId}' AND message_id=${hideSource.id}`);
  if (hideRow !== null) assert.equal(hideRow, '1', 'the caller hide row exists');
  assert.ok(!(await sourceHistoryIds(alice)).includes(hideSource.id),
    'the hidden source leaves the caller view');
  await retrySame(hideSource, hideRandom, hideForward, 'retry after the caller hides the source');

  // 5c. Both parties delete the source; it starts visible to the caller.
  const revokeSource = await send(alice, groupPeer(retryGroupId), 'retry revoke source');
  const revokeRandom = randomId();
  const revokeForward = await forwardSource(revokeSource, revokeRandom, 'source revoke forward');
  assert.ok((await sourceHistoryIds(alice)).includes(revokeSource.id), 'the source is visible before the revoke');
  assert.ok((await sourceHistoryIds(bob)).includes(revokeSource.id), 'the peer sees the source before the revoke');
  await rpc(alice, T.deleteMessages, concat(vfield(1, revokeSource.id), vfield(2, 1)));
  const revokeRow = dbQuery(`SELECT count(*) FROM botgram_backend_v1.message_deletions ` +
    `WHERE message_id=${revokeSource.id}`);
  if (revokeRow !== null) assert.equal(revokeRow, '1', 'the global deletion row exists');
  assert.ok(!(await sourceHistoryIds(bob)).includes(revokeSource.id),
    'the peer history drops the revoked source');
  const peerRead = fields(outcome(await rpc(bob, T.getMessages, vfield(1, revokeSource.id)), T.getMessages));
  assert.equal(all(peerRead, 1).length, 0, 'getMessages no longer returns the revoked source');
  await retrySame(revokeSource, revokeRandom, revokeForward, 'retry after both parties delete the source');

  // 5d. Hiding first and revoking second must still delete globally.
  const hideRevokeSource = await send(alice, groupPeer(retryGroupId), 'retry hide and revoke source');
  const hideRevokeRandom = randomId();
  const hideRevokeForward = await forwardSource(hideRevokeSource, hideRevokeRandom, 'hide+revoke forward');
  await rpc(alice, T.deleteMessages, concat(vfield(1, hideRevokeSource.id), vfield(2, 0)));
  await rpc(alice, T.deleteMessages, concat(vfield(1, hideRevokeSource.id), vfield(2, 1)));
  const hideRevokeRow = dbQuery(`SELECT count(*) FROM botgram_backend_v1.message_deletions ` +
    `WHERE message_id=${hideRevokeSource.id}`);
  if (hideRevokeRow !== null) assert.equal(hideRevokeRow, '1',
    'a revoke after a caller hide still deletes globally');
  assert.ok(!(await sourceHistoryIds(bob)).includes(hideRevokeSource.id),
    'the peer loses the revoked source');
  await retrySame(hideRevokeSource, hideRevokeRandom, hideRevokeForward, 'retry after hide then revoke');

  // 5e. History cleared in the source conversation.
  const clearSource = await send(alice, groupPeer(retryGroupId), 'retry clear source');
  const clearRandom = randomId();
  const clearForward = await forwardSource(clearSource, clearRandom, 'source clear forward');
  assert.ok((await sourceHistoryIds(alice)).includes(clearSource.id), 'the source is visible before the clear');
  await rpc(alice, T.deleteHistory, concat(bfield(1, groupPeer(retryGroupId)), vfield(2, 1)));
  const clearBoundary = dbQuery(`SELECT cleared_through FROM botgram_backend_v1.dialogs ` +
    `WHERE user_id='${aliceId}' AND conversation_id=` +
    `(SELECT conversation_id FROM botgram_intake_v1.messages WHERE id=${clearSource.id})`);
  if (clearBoundary !== null) assert.ok(Number(clearBoundary) >= clearSource.id,
    'the clear boundary covers the source');
  assert.ok(!(await sourceHistoryIds(alice)).includes(clearSource.id),
    'the cleared source leaves the caller view');
  await retrySame(clearSource, clearRandom, clearForward, 'retry after the source history is cleared');

  // 5f. Legacy records without the persisted identity are backfilled only on sufficient
  // evidence: text, entities, author, options and the stored creation time must all match.
  const stripIdentity = messageId => dbQuery(`UPDATE botgram_intake_v1.messages ` +
    `SET body = body - 'ForwardSourceConversationId' - 'ForwardSourceMessageId' - 'ForwardDropAuthor' ` +
    `WHERE id=${messageId} RETURNING id`);
  const bodyHas = (messageId, key) => dbQuery(
    `SELECT (body ? '${key}')::text FROM botgram_intake_v1.messages WHERE id=${messageId}`);

  const legacySource = await send(alice, groupPeer(retryGroupId), 'legacy source alpha');
  const legacyRandom = randomId();
  const legacyForward = await forwardSource(legacySource, legacyRandom, 'legacy forward');
  stripIdentity(legacyForward.id);
  const legacyStripped = bodyHas(legacyForward.id, 'ForwardSourceConversationId');
  if (legacyStripped !== null) assert.equal(legacyStripped, 'false', 'the record is reduced to its legacy shape');

  // Same author, identical text and entities, different creation time: adopting it would
  // rewrite the identity of a different source.
  const lookalike = await send(alice, groupPeer(retryGroupId), 'legacy source alpha');
  const beforeAmbiguous = dbCounts();
  const beforeAmbiguousPts = await pts(alice);
  expectError(await rpc(alice, T.forwardMessages, retryBody(lookalike, groupPeer(groupId), legacyRandom)),
    409, 'RANDOM_ID_DUPLICATE');
  const stillAbsent = bodyHas(legacyForward.id, 'ForwardSourceMessageId');
  if (stillAbsent !== null) assert.equal(stillAbsent, 'false',
    'a conflicting request leaves the identity absent');
  assert.equal(dbCounts(), beforeAmbiguous, 'a conflicting legacy request has no side effects');
  assert.equal(await pts(alice), beforeAmbiguousPts, 'a conflicting legacy request advances no pts');
  ok();

  // The true original request is still retryable and now carries a complete identity.
  const beforeLegacyBackfill = dbCounts();
  await retrySame(legacySource, legacyRandom, legacyForward, 'retry of the legacy original');
  assert.equal(dbCounts(), beforeLegacyBackfill, 'the legacy retry adds no rows');
  const backfilled = dbQuery(`SELECT body->>'ForwardSourceConversationId'||'/'||` +
    `(body->>'ForwardSourceMessageId')||'/'||(body->>'ForwardDropAuthor') ` +
    `FROM botgram_intake_v1.messages WHERE id=${legacyForward.id}`);
  if (backfilled !== null) assert.equal(backfilled, `${retryGroupId}/${legacySource.id}/false`,
    'the identity is backfilled from sufficient evidence');
  ok();

  // Once backfilled, the record no longer depends on the source at all.
  await rpc(alice, T.deleteMessages, concat(vfield(1, legacySource.id), vfield(2, 1)));
  await retrySame(legacySource, legacyRandom, legacyForward, 'retry after the backfilled source is deleted');

  // A legacy drop_author record has neither source author nor time: the evidence is
  // insufficient, so the retry is refused instead of adopting a lookalike.
  const dropSource = await send(alice, groupPeer(retryGroupId), 'legacy drop source');
  const dropRandom = randomId();
  const dropBody = () => concat(bfield(1, groupPeer(retryGroupId)), bfield(2, groupPeer(groupId)),
    vfield(3, dropSource.id), fixed64(4, dropRandom), vfield(6, 1));
  const dropCopy = forwardedMessage(fields(outcome(await rpc(alice, T.forwardMessages, dropBody()),
    T.forwardMessages)));
  stripIdentity(num(dropCopy.message, 1));
  expectError(await rpc(alice, T.forwardMessages, dropBody()), 409, 'RANDOM_ID_DUPLICATE');
  ok();

  // A legacy record missing the attribution time is equally ambiguous.
  const noTimeSource = await send(alice, groupPeer(retryGroupId), 'legacy no time source');
  const noTimeRandom = randomId();
  const noTimeForward = await forwardSource(noTimeSource, noTimeRandom, 'legacy no-time forward');
  dbQuery(`UPDATE botgram_intake_v1.messages ` +
    `SET body = body - 'ForwardSourceConversationId' - 'ForwardSourceMessageId' ` +
    `- 'ForwardDropAuthor' - 'ForwardFromAt' WHERE id=${noTimeForward.id}`);
  expectError(await rpc(alice, T.forwardMessages, retryBody(noTimeSource, groupPeer(groupId), noTimeRandom)),
    409, 'RANDOM_ID_DUPLICATE');
  ok();

  // A keep-author legacy record can never be the retry of a drop_author request.
  const keepSource = await send(alice, groupPeer(retryGroupId), 'legacy keep source');
  const keepRandom = randomId();
  const keepForward = await forwardSource(keepSource, keepRandom, 'legacy keep forward');
  stripIdentity(keepForward.id);
  const beforeOptionConflict = dbCounts();
  const beforeOptionPts = await pts(alice);
  expectError(await rpc(alice, T.forwardMessages, concat(
    bfield(1, groupPeer(retryGroupId)), bfield(2, groupPeer(groupId)),
    vfield(3, keepSource.id), fixed64(4, keepRandom), vfield(6, 1))), 409, 'RANDOM_ID_DUPLICATE');
  const keepStillAbsent = bodyHas(keepForward.id, 'ForwardDropAuthor');
  if (keepStillAbsent !== null) assert.equal(keepStillAbsent, 'false',
    'a drop_author request leaves the keep-author identity absent');
  assert.equal(dbCounts(), beforeOptionConflict, 'the option conflict has no side effects');
  assert.equal(await pts(alice), beforeOptionPts, 'the option conflict advances no pts');
  ok();
  // The real keep-author request still backfills, with drop_author recorded as false.
  await retrySame(keepSource, keepRandom, keepForward, 'retry after a drop_author conflict');
  const keepIdentity = dbQuery(`SELECT body->>'ForwardDropAuthor' ` +
    `FROM botgram_intake_v1.messages WHERE id=${keepForward.id}`);
  if (keepIdentity !== null) assert.equal(keepIdentity, 'false',
    'the backfilled identity keeps drop_author=false');
  ok();

  // Identical text and creation time describe two possible sources: neither may be adopted.
  const ambiguousFirst = await send(alice, groupPeer(retryGroupId), 'legacy ambiguous source');
  const ambiguousSecond = await send(alice, groupPeer(retryGroupId), 'legacy ambiguous source');
  dbQuery(`UPDATE botgram_intake_v1.messages SET body = jsonb_set(body, '{CreatedAt}', ` +
    `(SELECT body->'CreatedAt' FROM botgram_intake_v1.messages WHERE id=${ambiguousFirst.id})) ` +
    `WHERE id=${ambiguousSecond.id}`);
  const ambiguousRandom = randomId();
  const ambiguousForward = await forwardSource(ambiguousFirst, ambiguousRandom, 'legacy ambiguous forward');
  stripIdentity(ambiguousForward.id);
  expectError(await rpc(alice, T.forwardMessages,
    retryBody(ambiguousSecond, groupPeer(groupId), ambiguousRandom)), 409, 'RANDOM_ID_DUPLICATE');
  ok();
  expectError(await rpc(alice, T.forwardMessages,
    retryBody(ambiguousFirst, groupPeer(groupId), ambiguousRandom)), 409, 'RANDOM_ID_DUPLICATE');
  const ambiguousAbsent = bodyHas(ambiguousForward.id, 'ForwardSourceMessageId');
  if (ambiguousAbsent !== null) assert.equal(ambiguousAbsent, 'false',
    'an ambiguous legacy record is left untouched');
  ok();

  // Editing the true source must not narrow the candidate set so that a same-time lookalike
  // becomes the unique match: uniqueness rests on immutable author + creation time only.
  const editedFirst = await send(alice, groupPeer(retryGroupId), 'legacy edited source');
  const editedSecond = await send(alice, groupPeer(retryGroupId), 'legacy edited source');
  dbQuery(`UPDATE botgram_intake_v1.messages SET body = jsonb_set(body, '{CreatedAt}', ` +
    `(SELECT body->'CreatedAt' FROM botgram_intake_v1.messages WHERE id=${editedFirst.id})) ` +
    `WHERE id=${editedSecond.id}`);
  const editedRandom = randomId();
  const editedForward = await forwardSource(editedFirst, editedRandom, 'legacy edited forward');
  stripIdentity(editedForward.id);
  await rpc(alice, T.editMessage, concat(bfield(1, groupPeer(retryGroupId)),
    vfield(2, editedFirst.id), sfield(3, 'legacy edited source v2')));
  expectError(await rpc(alice, T.forwardMessages,
    retryBody(editedSecond, groupPeer(groupId), editedRandom)), 409, 'RANDOM_ID_DUPLICATE');
  ok();
  expectError(await rpc(alice, T.forwardMessages,
    retryBody(editedFirst, groupPeer(groupId), editedRandom)), 409, 'RANDOM_ID_DUPLICATE');
  const editedAbsent = bodyHas(editedForward.id, 'ForwardSourceMessageId');
  if (editedAbsent !== null) assert.equal(editedAbsent, 'false',
    'an edited ambiguous source leaves the record untouched');
  ok();

  // With unique immutable evidence, an edited source still backfills and the retry returns
  // the original forwarded content rather than the edited text.
  const stableSource = await send(alice, groupPeer(retryGroupId), 'legacy stable source');
  const stableRandom = randomId();
  const stableForward = await forwardSource(stableSource, stableRandom, 'legacy stable forward');
  stripIdentity(stableForward.id);
  await rpc(alice, T.editMessage, concat(bfield(1, groupPeer(retryGroupId)),
    vfield(2, stableSource.id), sfield(3, 'legacy stable source v2')));
  const stableRetry = await rpc(alice, T.forwardMessages,
    retryBody(stableSource, groupPeer(groupId), stableRandom));
  const stableCopy = forwardedMessage(fields(outcome(stableRetry, T.forwardMessages)));
  assert.equal(num(stableCopy.message, 1), stableForward.id, 'an edited but unique source still backfills');
  assert.equal(stableCopy.pts, stableForward.pts, 'the edited-source retry keeps its original pts');
  assert.equal(forwardText(stableCopy.message), stableForward.text,
    'the edited-source retry returns the original forwarded content');
  const stableIdentity = bodyHas(stableForward.id, 'ForwardSourceMessageId');
  if (stableIdentity !== null) assert.equal(stableIdentity, 'true',
    'the edited-source identity is backfilled');
  ok();

  // A batch may mix a backfillable legacy record with a new item.
  const batchLegacySource = await send(alice, groupPeer(retryGroupId), 'legacy batch source');
  const batchLegacyRandom = randomId();
  const batchLegacyForward = await forwardSource(batchLegacySource, batchLegacyRandom, 'legacy batch forward');
  stripIdentity(batchLegacyForward.id);
  const batchFreshSource = await send(alice, groupPeer(retryGroupId), 'legacy batch fresh');
  const batchFreshRandom = randomId();
  const mixedBatch = await rpc(alice, T.forwardMessages, concat(
    bfield(1, groupPeer(retryGroupId)), bfield(2, groupPeer(groupId)),
    vfield(3, batchLegacySource.id), vfield(3, batchFreshSource.id),
    fixed64(4, batchLegacyRandom), fixed64(4, batchFreshRandom)));
  const mixedIds = all(fields(outcome(mixedBatch, T.forwardMessages)), 1)
    .map(entry => fields(entry.bytes))
    .filter(update => update[0].number === 2)
    .map(update => num(fields(first(update, 2).bytes), 2));
  assert.equal(mixedIds.length, 2, 'both mixed batch items report a mapping');
  assert.equal(mixedIds[0], batchLegacyForward.id, 'the legacy item reuses its committed message');
  assert.ok(mixedIds[1] > 0 && mixedIds[1] !== batchLegacyForward.id, 'the new item commits');
  const mixedIdentity = bodyHas(batchLegacyForward.id, 'ForwardSourceConversationId');
  if (mixedIdentity !== null) assert.equal(mixedIdentity, 'true',
    'the batch backfills the legacy identity');
  ok();

  // Two concurrent identical requests commit once and both report the committed result.
  const concurrentSource = await send(alice, groupPeer(retryGroupId), 'concurrent source');
  const concurrentRandom = randomId();
  const concurrentBody = () => concat(bfield(1, groupPeer(retryGroupId)),
    bfield(2, groupPeer(groupId)), vfield(3, concurrentSource.id), fixed64(4, concurrentRandom));
  const [firstConcurrent, secondConcurrent] = await Promise.all([
    rpc(alice, T.forwardMessages, concurrentBody()),
    rpc(alice, T.forwardMessages, concurrentBody())
  ]);
  assert.equal(firstConcurrent.status, 200, 'first concurrent forward');
  assert.equal(secondConcurrent.status, 200, 'second concurrent forward');
  const concurrentIds = [firstConcurrent, secondConcurrent].map(response =>
    num(forwardedMessage(fields(outcome(response, T.forwardMessages))).message, 1));
  assert.equal(concurrentIds[1], concurrentIds[0], 'concurrent identical requests return one message');
  const concurrentRows = dbQuery(
    `SELECT count(*) FROM botgram_intake_v1.messages WHERE client_key='tl:${concurrentRandom}'`);
  if (concurrentRows !== null) assert.equal(concurrentRows, '1', 'the concurrent key stores one message row');
  ok();

  // A committed forward survives the target turning encrypted; a new request obeys it.
  const e2eeRetrySource = await send(alice, groupPeer(retryGroupId), 'e2ee retry source');
  const e2eeRetryRandom = randomId();
  const e2eeRetryBody = () => concat(bfield(1, groupPeer(retryGroupId)),
    bfield(2, userPeer(carolId)), vfield(3, e2eeRetrySource.id), fixed64(4, e2eeRetryRandom));
  const beforeE2ee = await rpc(alice, T.forwardMessages, e2eeRetryBody());
  const e2eeCopy = forwardedMessage(fields(outcome(beforeE2ee, T.forwardMessages)));
  // alice-carol is still the one-way chat carol initiated; alice publishing a device and
  // accepting makes it active, which forbids new plaintext sends in either direction.
  await rpc(alice, 139, publishBody(Buffer.alloc(33, 0x53)));
  assert.equal((await rpc(alice, 140, actionBody(userPeer(carolId), 2))).status, 200);
  const e2eeRetry = await rpc(alice, T.forwardMessages, e2eeRetryBody());
  const e2eeAgain = forwardedMessage(fields(outcome(e2eeRetry, T.forwardMessages)));
  assert.equal(num(e2eeAgain.message, 1), num(e2eeCopy.message, 1),
    'a committed retry survives the target turning encrypted');
  assert.equal(e2eeAgain.pts, e2eeCopy.pts, 'the committed retry keeps its original pts');
  expectError(await rpc(alice, T.forwardMessages, concat(bfield(1, groupPeer(retryGroupId)),
    bfield(2, userPeer(carolId)), vfield(3, e2eeRetrySource.id), fixed64(4, randomId()))),
    409, 'E2EE_PLAINTEXT_FORBIDDEN');
  ok();

  // Batch: a committed item plus a new item; then a batch whose second item is invalid.
  const batchSeededSource = await send(alice, groupPeer(retryGroupId), 'batch seeded source');
  const batchSeedRandom = randomId();
  const seed = await rpc(alice, T.forwardMessages, concat(bfield(1, groupPeer(retryGroupId)),
    bfield(2, groupPeer(groupId)), vfield(3, batchSeededSource.id), fixed64(4, batchSeedRandom)));
  const seededId = num(forwardedMessage(fields(outcome(seed, T.forwardMessages))).message, 1);
  const batchNewSource = await send(alice, groupPeer(retryGroupId), 'batch new source');
  const batchNewRandom = randomId();
  const batch = await rpc(alice, T.forwardMessages, concat(bfield(1, groupPeer(retryGroupId)),
    bfield(2, groupPeer(groupId)),
    vfield(3, batchSeededSource.id), vfield(3, batchNewSource.id),
    fixed64(4, batchSeedRandom), fixed64(4, batchNewRandom)));
  const batchMappingIds = all(fields(outcome(batch, T.forwardMessages)), 1)
    .map(entry => fields(entry.bytes))
    .filter(update => update[0].number === 2)
    // Update.message_id wraps MessageIdMapping: random_id = 1, message_id = 2.
    .map(update => num(fields(first(update, 2).bytes), 2));
  assert.equal(batchMappingIds.length, 2, 'both batch items report a mapping');
  assert.equal(batchMappingIds[0], seededId, 'the committed item returns its original message');
  assert.ok(batchMappingIds[1] > 0 && batchMappingIds[1] !== seededId, 'the new item commits');
  const batchNewRows = dbQuery(
    `SELECT count(*) FROM botgram_intake_v1.messages WHERE client_key='tl:${batchNewRandom}'`);
  if (batchNewRows !== null) assert.equal(batchNewRows, '1', 'the new batch item stores one message');
  ok();

  const beforeFailedBatch = dbCounts();
  const failedFirstRandom = randomId();
  expectError(await rpc(alice, T.forwardMessages, concat(bfield(1, groupPeer(retryGroupId)),
    bfield(2, groupPeer(groupId)),
    vfield(3, batchNewSource.id), vfield(3, 999999999),
    fixed64(4, failedFirstRandom), fixed64(4, randomId()))), 400, 'MESSAGE_ID_INVALID');
  assert.equal(dbCounts(), beforeFailedBatch, 'a failing batch rolls back its accepted items');
  const failedRows = dbQuery(
    `SELECT count(*) FROM botgram_intake_v1.messages WHERE client_key='tl:${failedFirstRandom}'`);
  if (failedRows !== null) assert.equal(failedRows, '0', 'the rolled back item leaves no message row');
  ok();

  // ---------------------------------------------------------------- restart persistence
  const bobPtsBeforeRestart = await pts(bob);
  await rpc(bob, T.saveDraft, concat(bfield(1, userPeer(aliceId)), sfield(2, 'survives restart')));
  await rpc(bob, T.toggleDialogPin, concat(bfield(1, userPeer(aliceId)), vfield(2, 1)));
  // A caller-scoped delete must still hide exactly this message after a restart, while
  // the peer keeps its own view.
  await rpc(bob, T.deleteMessages, concat(vfield(1, viewDirect.id), vfield(2, 0)));
  await stop();
  await start();
  // Committed forwards are still retryable after a restart with their original identity:
  // one written in the current format and one backfilled from a legacy record whose source
  // has since been deleted.
  await retrySame(editSource, editRandom, editForward, 'retry after restart');
  await retrySame(legacySource, legacyRandom, legacyForward, 'backfilled retry after restart');
  const draftsAfterRestart = fields(outcome(await rpc(bob, T.getAllDrafts), T.getAllDrafts));
  assert.equal(all(draftsAfterRestart, 1).filter(entry => fields(entry.bytes)[0].number === 14).length, 1);
  const pinnedAfterRestart = fields(outcome(await rpc(bob, T.getPinnedDialogs), T.getPinnedDialogs));
  assert.equal(all(pinnedAfterRestart, 1).length, 1);
  const historyAfterRestart = fields(outcome(await rpc(bob, T.getHistory,
    concat(bfield(1, userPeer(aliceId)), vfield(7, 50))), T.getHistory));
  assert.ok(!messageIds(historyAfterRestart).includes(viewDirect.id),
    'caller-scoped delete survives restart');
  const aliceHistoryAfterRestart = fields(outcome(await rpc(alice, T.getHistory,
    concat(bfield(1, userPeer(bobId)), vfield(7, 50))), T.getHistory));
  assert.ok(messageIds(aliceHistoryAfterRestart).includes(viewDirect.id),
    'the peer still sees the message it never deleted');
  assert.ok(await pts(bob) >= bobPtsBeforeRestart);
  ok();
  assert.ok(revived.id > 0);

  console.log(`PASS: batch A messages regression (${checks} checks, 26 operations, PostgreSQL-only)`);
} catch (error) {
  if (logs.trim()) console.error('BACKEND LOGS:\n' + logs.slice(-8000));
  throw error;
} finally {
  await stop();
}
