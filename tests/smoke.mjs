// Node 22+, no dependencies. Build the backend before running this script.
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
const temp = await mkdtemp(path.join(process.env.BOTGRAM_TEST_ROOT ?? path.join(root, 'obj'), 'smoke-'));
const database = path.join(temp, 'db.json');
const listener = net.createServer();
listener.listen(0, '127.0.0.1');
await once(listener, 'listening');
const port = listener.address().port;
await new Promise(resolve => listener.close(resolve));
const base = `http://127.0.0.1:${port}`;
const fixtures = [];
const rpcFixtures = [];
let server;
let logs = '';
async function start() {
  server = spawn('dotnet', [process.env.BOTGRAM_TEST_DLL ?? path.join(root, 'bin/Debug/net8.0/Botgram.Backend.dll'), '--urls', base], {
    cwd: root, env: { ...process.env, DataPath: database }, windowsHide: true, stdio: ['ignore', 'pipe', 'pipe']
  });
  server.stdout.on('data', x => { logs += x; });
  server.stderr.on('data', x => { logs += x; });
  for (let i = 0; i < 100; i++) {
    if (server.exitCode !== null) throw new Error(logs);
    try { if ((await fetch(`${base}/health`)).ok) return; } catch {}
    await new Promise(resolve => setTimeout(resolve, 100));
  }
  throw new Error(`Startup timeout: ${logs}`);
}
async function stop() {
  if (server && server.exitCode === null) { const ended = once(server, 'exit'); server.kill(); await ended; }
}
async function request(method, route, token, body, status = 200) {
  const response = await fetch(base + route, {
    method, headers: { 'content-type': 'application/json', ...(token ? { authorization: `Bearer ${token}` } : {}) },
    body: body === undefined ? undefined : JSON.stringify(body)
  });
  const text = await response.text();
  if (route === '/api/telegram/rpc') rpcFixtures.push({
    requestType: body.$type, request: body, status: response.status, expectedStatus: status,
    response: text ? JSON.parse(text) : null
  });
  assert.equal(response.status, status, `${method} ${route}: ${text}`);
  return text ? JSON.parse(text) : null;
}
// Minimal read-only WebSocket probe with Authorization, as used by Android OkHttp.
async function socketBatch(token, cursor) {
  const socket = net.connect(port, '127.0.0.1');
  await once(socket, 'connect');
  return new Promise((resolve, reject) => {
    let buffer = Buffer.alloc(0), upgraded = false;
    const timer = setTimeout(() => finish(new Error('WebSocket timeout')), 5000);
    function finish(error, value) { clearTimeout(timer); socket.destroy(); error ? reject(error) : resolve(value); }
    socket.on('error', error => finish(error));
    socket.on('data', chunk => {
      buffer = Buffer.concat([buffer, chunk]);
      if (!upgraded) {
        const end = buffer.indexOf('\r\n\r\n');
        if (end < 0) return;
        if (!buffer.subarray(0, end).toString().startsWith('HTTP/1.1 101')) return finish(new Error(buffer.toString()));
        upgraded = true; buffer = buffer.subarray(end + 4);
      }
      if (buffer.length < 2) return;
      let length = buffer[1] & 127, offset = 2;
      if (length === 126) { if (buffer.length < 4) return; length = buffer.readUInt16BE(2); offset = 4; }
      if (length === 127) { if (buffer.length < 10) return; length = Number(buffer.readBigUInt64BE(2)); offset = 10; }
      if (buffer.length >= offset + length) {
        try { finish(null, JSON.parse(buffer.subarray(offset, offset + length).toString())); }
        catch (error) { finish(error); }
      }
    });
    socket.write(`GET /api/ws?after=${cursor} HTTP/1.1\r\nHost: 127.0.0.1:${port}\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: ${randomBytes(16).toString('base64')}\r\nSec-WebSocket-Version: 13\r\nAuthorization: Bearer ${token}\r\n\r\n`);
  });
}

try {
  await start();
  await request('GET', '/api/me', null, undefined, 401);
  const register = username => request('POST', '/api/auth/register', null, { username, displayName: username, password: 'correct-horse-123' });
  const alice = await register('alice'), bob = await register('bob'), outsider = await register('outsider');
  await request('POST', '/api/auth/register', null, { username: 'alice', displayName: 'Alice', password: 'correct-horse-123' }, 409);
  await request('POST', '/api/auth/login', null, { username: 'alice', password: 'bad-password' }, 401);
  const login = await request('POST', '/api/auth/login', null, { username: 'ALICE', password: 'correct-horse-123' });
  assert.equal(login.user.id, alice.user.id);
  const tl = (type, fields = {}) => ({ $type: `org.telegram.tgnet.TLRPC$${type}`, ...fields });
  const rpc = async (token, type, fields, status = 200) => {
    const requestType = type.startsWith('org.') ? type : `org.telegram.tgnet.TLRPC$${type}`;
    const result = await request('POST', '/api/telegram/rpc', token, { $type: requestType, ...fields }, status);
    if (status === 200) fixtures.push({ requestType, response: result });
    return result;
  };
  const assertSafeUserDetails = value => {
    if (value === null || typeof value !== 'object') return;
    for (const [key, item] of Object.entries(value)) {
      assert(!['salt', 'passwordHash', 'token', 'tokenHash'].includes(key), `private user field: ${key}`);
      assertSafeUserDetails(item);
    }
  };
  const me = await request('GET', '/api/me', alice.token);
  assert.deepEqual(Object.keys(me).sort(), ['displayName', 'gid', 'id', 'username']);
  assert.deepEqual(me, alice.user);
  assertSafeUserDetails(me);
  const aliceTl = await request('GET', '/api/telegram/me', alice.token);
  const bobTl = await request('GET', '/api/telegram/me', bob.token);
  assert.equal(aliceTl.$type, 'org.telegram.tgnet.TLRPC$TL_user');
  assert.equal(typeof aliceTl.id, 'number');
  assert.notEqual(aliceTl.id, bobTl.id);
  const peer = tl('TL_inputPeerUser', { user_id: bobTl.id, access_hash: 1 });
  const firstHistory = await rpc(alice.token, 'TL_messages_getHistory', { peer, offset_id: 0, limit: 50 });
  assert.equal(firstHistory.messages.length, 0);
  assert(firstHistory.users.some(u => u.id === bobTl.id), 'first chat history must include the new peer');
  const peerSettings = await rpc(alice.token, 'TL_messages_getPeerSettings', { peer });
  assert.equal(peerSettings.$type, 'org.telegram.tgnet.TLRPC$TL_messages_peerSettings');
  assert(peerSettings.users.some(u => u.id === bobTl.id));
  const conv = await request('POST', '/api/conversations', alice.token, { kind: 'direct', memberIds: [bob.user.id] });
  const duplicate = await request('POST', '/api/conversations', bob.token, { kind: 'direct', memberIds: [alice.user.id] });
  assert.equal(duplicate.id, conv.id);
  const route = `/api/conversations/${conv.id}`;
  const userVector = await rpc(alice.token, 'TL_users_getUsers', { id: [tl('TL_inputUser', { user_id: bobTl.id })] });
  assert.equal(userVector.objects[0].id, bobTl.id);
  const unknownUser = tl('TL_inputUser', { user_id: 2147483647 });
  const duplicateUsers = await rpc(alice.token, 'TL_users_getUsers', {
    id: [tl('TL_inputUser', { user_id: bobTl.id }), unknownUser,
      tl('TL_inputUserSelf'), tl('TL_inputUser', { user_id: bobTl.id })]
  });
  assert.deepEqual(duplicateUsers.objects.map(u => u.id), [aliceTl.id, bobTl.id]);
  assert.equal(duplicateUsers.objects[0].self, true);
  assert.equal(duplicateUsers.objects[1].self, false);
  const unknownUsers = await rpc(alice.token, 'TL_users_getUsers', { id: [unknownUser] });
  assert.deepEqual(unknownUsers.objects, []);
  assertSafeUserDetails(duplicateUsers);
  assertSafeUserDetails(aliceTl);
  assertSafeUserDetails(bobTl);
  const selfPeer = tl('TL_inputPeerSelf');
  const saved = await rpc(alice.token, 'TL_messages_sendMessage', { peer: selfPeer, random_id: 987654, message: '收藏夹笔记' });
  assert.equal(saved.updates[1].message.peer_id.user_id, aliceTl.id);
  const savedHistory = await rpc(alice.token, 'TL_messages_getHistory', { peer: selfPeer, limit: 50 });
  assert.equal(savedHistory.messages[0].message, '收藏夹笔记');
  const found = await rpc(alice.token, 'TL_contacts_search', { q: 'bob', limit: 20 });
  assert.equal(found.users[0].id, bobTl.id);
  const resolved = await rpc(alice.token, 'TL_contacts_resolveUsername', { username: 'BOB' });
  assert.equal(resolved.peer.user_id, bobTl.id);
  assert.equal((await rpc(alice.token, 'TL_contacts_resolveUsername', { username: 'missing-user' }, 400)).error, 'USERNAME_NOT_OCCUPIED');
  assert.equal((await rpc(null, 'TL_updates_getState', {}, 401)).error, 'unauthorized');
  const addedContact = await rpc(alice.token, 'TL_contacts_addContact', {
    id: tl('TL_inputUser', { user_id: bobTl.id }), first_name: 'Bob', last_name: 'B', phone: '+00100000000',
    flags: 0, add_phone_privacy_exception: false
  });
  assert.equal(addedContact.$type, 'org.telegram.tgnet.TLRPC$TL_updates');
  assert.equal(addedContact.users.find(u => u.id === bobTl.id)?.contact, true, 'added contact must be returned with contact=true');
  const aliceContacts = await rpc(alice.token, 'TL_contacts_getContacts', { hash: 0 });
  assert(aliceContacts.users.some(u => u.id === bobTl.id && u.contact), 'added contact must be persisted');
  const alicePresenceBase = await rpc(alice.token, 'TL_updates_getState');
  const state = await rpc(bob.token, 'TL_updates_getState', { foreground: true });
  assert.equal(state.pts, 1, 'fresh accounts require a nonzero initial pts');
  const presenceDiff = await rpc(alice.token, 'TL_updates_getDifference', {
    pts: alicePresenceBase.pts, date: alicePresenceBase.date, qts: 0
  });
  const statusUpdate = presenceDiff.other_updates.find(u => u.$type.endsWith('$TL_updateUserStatus'));
  assert.equal(statusUpdate?.$type, 'org.telegram.tgnet.tl.TL_update$TL_updateUserStatus',
    'presence updates must use the Android class namespace');
  assert.equal(statusUpdate.user_id, bobTl.id);
  const sendFields = { peer, random_id: 12345678, message: '原 Telegram 页面消息',
    send_as: tl('TL_inputPeerUser', { user_id: aliceTl.id, access_hash: 1 }) };
  await rpc(alice.token, 'TL_messages_sendMessage', { ...sendFields, send_as: peer }, 403);
  const sent = await rpc(alice.token, 'TL_messages_sendMessage', sendFields);
  const duplicateTl = await rpc(alice.token, 'TL_messages_sendMessage', sendFields);
  assert.equal(sent.updates[0].id, duplicateTl.updates[0].id);
  assert.equal(sent.updates[1].$type, 'org.telegram.tgnet.tl.TL_update$TL_updateNewMessage');
  const diff = await rpc(bob.token, 'TL_updates_getDifference', { pts: state.pts, date: state.date, qts: 0 });
  assert.equal(diff.$type, 'org.telegram.tgnet.TLRPC$TL_updates_difference');
  assert(diff.new_messages.some(m => m.message === sendFields.message && m.out === false));
  assert.equal(diff.state.pts, state.pts + 1);
  const history = await rpc(alice.token, 'TL_messages_getHistory', { peer, offset_id: 0, add_offset: 0, limit: 50 });
  assert.equal(history.messages[0].message, sendFields.message);
  const aliceBeforeReply = await rpc(alice.token, 'TL_updates_getState');
  const reply = await rpc(bob.token, 'TL_messages_sendMessage', {
    peer: tl('TL_inputPeerUser', { user_id: aliceTl.id, access_hash: 1 }),
    send_as: tl('TL_inputPeerSelf'), random_id: 12345679, message: 'TL reply from Bob'
  });
  assert.equal(reply.updates[1].message.from_id.user_id, bobTl.id);
  const aliceDiff = await rpc(alice.token, 'TL_updates_getDifference', { pts: aliceBeforeReply.pts, date: aliceBeforeReply.date, qts: 0 });
  assert(aliceDiff.new_messages.some(m => m.message === 'TL reply from Bob' && m.out === false));
  const tlGroup = await rpc(alice.token, 'TL_messages_createChat', { title: '原界面群聊', users: [tl('TL_inputUser', { user_id: bobTl.id })] });
  const chat = tlGroup.updates.chats[0];
  assert.equal(tlGroup.$type, 'org.telegram.tgnet.TLRPC$TL_messages_invitedUsers');
  const groupPeer = tl('TL_inputPeerChat', { chat_id: chat.id });
  const styledFields = { peer: groupPeer, random_id: 23456789, message: 'hello', silent: true,
    entities: [tl('TL_messageEntityBold', { offset: 0, length: 5 })],
    reply_to: tl('TL_inputReplyToMessage', { reply_to_msg_id: tlGroup.updates.updates[0].message.id }) };
  const styled = await rpc(bob.token, 'TL_messages_sendMessage', styledFields);
  assert.equal(styled.updates[1].message.entities[0].length, 5);
  assert.equal(styled.updates[1].message.reply_to.reply_to_msg_id, styledFields.reply_to.reply_to_msg_id);
  assert.equal(styled.updates[1].message.silent, true);
  await rpc(bob.token, 'TL_messages_sendMessage', { ...styledFields, silent: false }, 409);
  await rpc(bob.token, 'TL_messages_sendMessage', { ...styledFields, random_id: 23456790,
    reply_to: tl('TL_inputReplyToMessage', { reply_to_msg_id: sent.updates[0].id }) }, 400);
  const groupDiff = await rpc(bob.token, 'TL_updates_getDifference', { pts: diff.state.pts, date: state.date, qts: 0 });
  assert(groupDiff.new_messages.some(m => m.$type.endsWith('$TL_messageService') && m.action.title === '原界面群聊'));
  await rpc(outsider.token, 'TL_messages_getHistory', { peer: tl('TL_inputPeerChat', { chat_id: chat.id }), limit: 50 }, 404);
  const full = await rpc(alice.token, 'TL_messages_getFullChat', { chat_id: chat.id });
  assert.equal(full.full_chat.participants.participants.length, 2);
  const fullUser = await rpc(alice.token, 'TL_users_getFullUser', { id: tl('TL_inputUser', { user_id: bobTl.id }) });
  assert.equal(fullUser.full_user.id, bobTl.id);
  assertSafeUserDetails(fullUser);
  assertSafeUserDetails(full);
  assertSafeUserDetails(peerSettings);
  await rpc(alice.token, 'TL_messages_getChats', { id: [chat.id] });
  const emptyDiff = await rpc(bob.token, 'TL_updates_getDifference', { pts: groupDiff.state.pts, date: state.date, qts: 0 });
  assert(emptyDiff.$type.endsWith('$TL_updates_differenceEmpty'));
  await rpc(alice.token, 'TL_messages_readHistory', { peer: groupPeer, max_id: 2147483647 });
  const dialogs = await rpc(alice.token, 'TL_messages_getDialogs', { limit: 100 });
  assert(dialogs.dialogs.some(d => d.peer.user_id === bobTl.id));
  const dialogPage = await rpc(alice.token, 'TL_messages_getDialogs', { limit: 1 });
  assert(dialogPage.$type.endsWith('$TL_messages_dialogsSlice'));
  const dialogPeers = value => value.dialogs.map(d => d.peer.user_id ?? -d.peer.chat_id);
  assert.deepEqual(dialogPeers(dialogs), [-chat.id, bobTl.id, aliceTl.id]);
  assert.equal(dialogPage.count, 3);
  assert.deepEqual(dialogPeers(dialogPage), [-chat.id]);
  const offsetPeerPage = await rpc(alice.token, 'TL_messages_getDialogs', {
    offset_peer: groupPeer, offset_id: saved.updates[0].id, limit: 1
  });
  assert.deepEqual(dialogPeers(offsetPeerPage), [bobTl.id]);
  assert.equal(offsetPeerPage.count, 3);
  const offsetPeerTail = await rpc(alice.token, 'TL_messages_getDialogs', { offset_peer: peer, limit: 100 });
  assert.deepEqual(dialogPeers(offsetPeerTail), [aliceTl.id]);
  const offsetIdPage = await rpc(alice.token, 'TL_messages_getDialogs', {
    offset_id: reply.updates[0].id, limit: 100
  });
  assert.deepEqual(dialogPeers(offsetIdPage), [aliceTl.id]);
  const unknownOffsetPage = await rpc(alice.token, 'TL_messages_getDialogs', {
    offset_peer: tl('TL_inputPeerUser', { user_id: unknownUser.user_id }),
    offset_id: reply.updates[0].id, limit: 100
  });
  assert.deepEqual(dialogPeers(unknownOffsetPage), [aliceTl.id]);
  const lastDialogPage = await rpc(alice.token, 'TL_messages_getDialogs', { offset_peer: selfPeer, limit: 100 });
  assert.deepEqual(lastDialogPage.dialogs, []);
  assert.deepEqual(lastDialogPage.messages, []);
  const folderDialogs = await rpc(alice.token, 'TL_messages_getDialogs', { folder_id: 1, limit: 100 });
  assert.deepEqual(folderDialogs.dialogs, []);
  assert.deepEqual(folderDialogs.messages, []);
  assert.equal(folderDialogs.count, 3);
  const peerDialogs = await rpc(alice.token, 'TL_messages_getPeerDialogs', { peers: [tl('TL_inputDialogPeer', { peer })] });
  assert.equal(peerDialogs.dialogs.length, 1);
  assert.deepEqual(dialogPeers(peerDialogs), [bobTl.id]);
  assert.deepEqual(peerDialogs.messages.map(m => m.id), [reply.updates[0].id]);
  const selectedDialogs = await rpc(alice.token, 'TL_messages_getPeerDialogs', {
    peers: [tl('TL_inputDialogPeer', { peer: selfPeer }), tl('TL_inputDialogPeer', { peer: groupPeer }),
      tl('TL_inputDialogPeer', { peer: groupPeer }),
      tl('TL_inputDialogPeer', { peer: tl('TL_inputPeerUser', { user_id: unknownUser.user_id }) })]
  });
  assert.deepEqual(dialogPeers(selectedDialogs), [-chat.id, aliceTl.id]);
  assert.deepEqual(selectedDialogs.messages.map(m => m.id), [styled.updates[0].id, saved.updates[0].id]);
  const hiddenDialogs = await rpc(outsider.token, 'TL_messages_getPeerDialogs', {
    peers: [tl('TL_inputDialogPeer', { peer: groupPeer })]
  });
  assert.deepEqual(hiddenDialogs.dialogs, []);
  assert.deepEqual(hiddenDialogs.messages, []);
  const pinnedDialogs = await rpc(alice.token, 'TL_messages_getPinnedDialogs', { folder_id: 0 });
  assert(pinnedDialogs.$type.endsWith('$TL_messages_peerDialogs'));
  assert.deepEqual(pinnedDialogs.dialogs, []);
  assert.deepEqual(pinnedDialogs.messages, []);
  await rpc(alice.token, 'org.telegram.tgnet.tl.TL_account$getNotifySettings', { peer: tl('TL_inputNotifyPeer', { peer }) });
  const reloaded = await rpc(alice.token, 'TL_messages_getMessages', { id: [tl('TL_inputMessageID', { id: sent.updates[0].id })] });
  assert.equal(reloaded.messages[0].id, sent.updates[0].id);
  const hiddenReload = await rpc(outsider.token, 'TL_messages_getMessages', { id: [tl('TL_inputMessageID', { id: sent.updates[0].id })] });
  assert.equal(hiddenReload.messages.length, 0);
  assert.equal((await rpc(alice.token, 'TL_messages_deleteMessages', { id: [1] }, 501)).error,
    'BOTGRAM_RPC_UNSUPPORTED:TL_messages_deleteMessages');
  await request('GET', `${route}/messages`, outsider.token, undefined, 404);
  await request('POST', `${route}/messages`, outsider.token, { clientMessageId: 'attack', text: 'no' }, 404);
  const sends = await Promise.all(Array.from({ length: 5 }, () => request('POST', `${route}/messages`, alice.token, { clientMessageId: 'once', text: '你好 Bob' })));
  assert.equal(new Set(sends.map(m => m.id)).size, 1);
  const message = sends[0];
  await request('POST', `${route}/messages`, alice.token, { clientMessageId: 'once', text: 'different' }, 409);
  assert.equal((await request('GET', '/api/conversations', bob.token)).find(c => c.conversation.id === conv.id).unreadCount, 2);
  await request('POST', `${route}/read`, bob.token, { messageId: message.id });
  assert.equal((await request('GET', '/api/conversations', bob.token)).find(c => c.conversation.id === conv.id).unreadCount, 0);
  const second = await request('POST', `${route}/messages`, bob.token, { clientMessageId: 'reply', text: '你好 Alice' });
  const bobConversations = await request('GET', '/api/conversations', bob.token);
  const directSummary = bobConversations.find(c => c.conversation.id === conv.id);
  for (const item of bobConversations) {
    assert.deepEqual(Object.keys(item).sort(), ['conversation', 'lastMessage', 'receipts', 'unreadCount', 'users']);
    assert(item.conversation.members.includes(bob.user.id));
    assert.deepEqual(item.users.map(u => u.id).sort(), [...item.conversation.members].sort());
    for (const user of item.users) {
      assert.deepEqual(Object.keys(user).sort(), ['displayName', 'gid', 'id', 'username']);
      assertSafeUserDetails(user);
    }
  }
  assert.deepEqual(directSummary.users.map(u => u.id).sort(), [alice.user.id, bob.user.id].sort());
  assert.deepEqual(directSummary.lastMessage, second);
  assert.equal(directSummary.unreadCount, 0);
  assert.deepEqual(directSummary.receipts, [{ conversationId: conv.id, userId: bob.user.id, messageId: message.id }]);
  const aliceSummary = (await request('GET', '/api/conversations', alice.token)).find(c => c.conversation.id === conv.id);
  assert.deepEqual(aliceSummary.lastMessage, second);
  assert.equal(aliceSummary.unreadCount, 2);
  assert.deepEqual(aliceSummary.receipts, directSummary.receipts);
  const outsiderConversations = await request('GET', '/api/conversations', outsider.token);
  assert.deepEqual(outsiderConversations, []);
  const page = await request('GET', `${route}/messages?before=${second.id}&limit=1`, alice.token);
  assert.deepEqual(page.map(m => m.id), [message.id]);
  const groups = await request('POST', '/api/conversations', alice.token, { kind: 'group', title: '测试群', memberIds: [bob.user.id, outsider.user.id] });
  await request('POST', `/api/conversations/${groups.id}/read`, alice.token, { messageId: message.id }, 400);
  const batch = await request('GET', '/api/events?after=0', bob.token);
  assert(batch.events.some(e => e.type === 'message.created'));
  const isolated = await request('GET', '/api/events?after=0', outsider.token);
  assert(!isolated.events.some(e => e.type === 'message.created'));
  const ws = await socketBatch(bob.token, 0);
  assert(ws.events.some(e => e.type === 'message.created'));
  const call = await request('POST', '/api/calls', alice.token, { conversationId: conv.id, calleeId: bob.user.id });
  assert((await request('GET', '/api/calls', bob.token)).some(c => c.id === call.id));
  assert.equal((await request('GET', '/api/calls', outsider.token)).length, 0);
  await request('POST', `/api/calls/${call.id}/accept`, alice.token, undefined, 403);
  await request('POST', `/api/calls/${call.id}/accept`, outsider.token, undefined, 404);
  await request('POST', `/api/calls/${call.id}/signals`, alice.token, { type: 'offer', data: 'sdp' }, 409);
  await request('POST', '/api/calls', bob.token, { conversationId: conv.id, calleeId: alice.user.id }, 409);
  await request('POST', `/api/calls/${call.id}/accept`, bob.token);
  await request('POST', `/api/calls/${call.id}/signals`, bob.token, { type: 'offer', data: 'sdp' }, 400);
  await request('POST', `/api/calls/${call.id}/signals`, alice.token, { type: 'offer', data: 'test-sdp' }, 202);
  const signal = await request('GET', `/api/events?after=${batch.cursor}`, bob.token);
  assert(signal.events.some(e => e.type === 'call.signal' && e.payload.data === 'test-sdp'));
  assert(!(await request('GET', `/api/events?after=${batch.cursor}`, alice.token)).events.some(e => e.type === 'call.signal'));
  await request('POST', `/api/calls/${call.id}/end`, alice.token);
  assert.equal((await request('GET', '/api/calls', bob.token)).length, 0);
  await request('POST', `/api/calls/${call.id}/signals`, alice.token, { type: 'ice', data: 'candidate' }, 409);
  await stop();
  await start();
  assert.deepEqual((await request('GET', `${route}/messages`, alice.token)).map(m => m.id).sort((a, b) => a - b),
    [sent.updates[0].id, reply.updates[0].id, message.id, second.id].sort((a, b) => a - b));
  assert.equal((await request('GET', '/api/telegram/me', alice.token)).id, aliceTl.id);
  assert.equal((await request('GET', '/api/events?after=0', bob.token)).cursor, signal.cursor + 1);
  const snapshot = JSON.parse(await readFile(database, 'utf8'));
  assert(!JSON.stringify(snapshot).includes(alice.token));
  assert(!JSON.stringify(snapshot).includes('correct-horse-123'));
  const ringing = await request('POST', '/api/calls', alice.token, { conversationId: conv.id, calleeId: bob.user.id });
  await stop();
  const expiredSnapshot = JSON.parse(await readFile(database, 'utf8'));
  expiredSnapshot.calls.find(c => c.id === ringing.id).updatedAt = new Date(Date.now() - 120_000).toISOString();
  for (const event of expiredSnapshot.events.filter(e => e.type === 'call.signal')) event.expiresAt = new Date(0).toISOString();
  await writeFile(database, JSON.stringify(expiredSnapshot));
  await start();
  let timeoutEvent;
  for (let i = 0; i < 70; i++) {
    const events = await request('GET', '/api/events?after=0', bob.token);
    assert(!events.events.some(e => e.type === 'call.signal'));
    timeoutEvent = events.events.find(e => e.type === 'call.updated' && e.payload.id === ringing.id && e.payload.state === 'ended');
    if (timeoutEvent) break;
    await new Promise(resolve => setTimeout(resolve, 100));
  }
  assert(timeoutEvent, 'background service must expire ringing calls without a write request');
  await request('POST', '/api/auth/logout', alice.token, undefined, 204);
  await request('GET', '/api/me', alice.token, undefined, 401);
  assert.equal((await rpc(alice.token, 'TL_updates_getState', {}, 401)).error, 'session_expired');
  await writeFile(path.join(fixtureRoot, 'tl-fixtures.json'), JSON.stringify(fixtures));
  console.log('PASS: auth, TL identity/dialogs/messages/groups/replies/entities/difference, isolation, concurrent idempotency, history, receipts, replay, WebSocket, REST call signals, restart persistence, expiry, logout');
} catch (error) {
  console.error(logs);
  throw error;
} finally {
  await stop();
  await writeFile(path.join(fixtureRoot, 'smoke-rpc-fixtures.json'), JSON.stringify(rpcFixtures, null, 2));
}
