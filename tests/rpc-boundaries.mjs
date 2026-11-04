// Node 22+, no dependencies. Uses only a new isolated database and loopback listener.
import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { once } from 'node:events';
import { mkdtemp, writeFile } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import net from 'node:net';

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const fixtureRoot = process.env.BOTGRAM_TEST_ROOT ?? path.join(root, 'obj');
const temp = await mkdtemp(path.join(process.env.BOTGRAM_TEST_ROOT ?? path.join(root, 'obj'), 'rpc-boundaries-'));
const listener = net.createServer();
listener.listen(0, '127.0.0.1');
await once(listener, 'listening');
const port = listener.address().port;
await new Promise(resolve => listener.close(resolve));
const base = `http://127.0.0.1:${port}`;
const tl = (type, fields = {}) => ({ $type: `org.telegram.tgnet.TLRPC$${type}`, ...fields });
const records = [], fixtures = [];
let logs = '';
const server = spawn('dotnet', [process.env.BOTGRAM_TEST_DLL ?? path.join(root, 'bin/Debug/net8.0/Botgram.Backend.dll'), '--urls', base], {
  cwd: root, env: { ...process.env, DataPath: path.join(temp, 'db.json') }, windowsHide: true, stdio: ['ignore', 'pipe', 'pipe']
});
server.stdout.on('data', x => { logs += x; });
server.stderr.on('data', x => { logs += x; });
async function request(route, token, body, status = 200) {
  const rawRequest = typeof body === 'string' ? body : JSON.stringify(body);
  const response = await fetch(base + route, { method: 'POST', headers: {
    'content-type': 'application/json', ...(token ? { authorization: `Bearer ${token}` } : {})
  }, body: rawRequest });
  const rawResponse = await response.text();
  const result = rawResponse ? JSON.parse(rawResponse) : null;
  if (route === '/api/telegram/rpc') {
    const input = JSON.parse(rawRequest);
    records.push({ requestType: input.$type, request: input, rawRequest, status: response.status, expectedStatus: status, response: result, rawResponse });
    // Preserve the server's integer literals; JSON.parse/stringify would round int64 extrema.
    if (response.status === 200) fixtures.push(`{"requestType":${JSON.stringify(input.$type)},"response":${rawResponse}}`);
  }
  assert.equal(response.status, status, `${route}: ${rawResponse}`);
  return { result, rawResponse };
}
async function rpc(token, type, fields = {}, status = 200) {
  return (await request('/api/telegram/rpc', token, tl(type, fields), status)).result;
}
async function error(token, type, fields, status, code) {
  assert.equal((await rpc(token, type, fields, status)).error, code);
}
try {
  let ready = false;
  for (let i = 0; i < 100; i++) {
    if (server.exitCode !== null) throw new Error(logs);
    try { if ((await fetch(`${base}/health`)).ok) { ready = true; break; } } catch {}
    await new Promise(resolve => setTimeout(resolve, 100));
  }
  assert(ready, `Startup timeout: ${logs}`);
  const register = async username => (await request('/api/auth/register', null, { username, displayName: username, password: 'boundary-test-123' })).result;
  const alice = await register('boundaryalice'), bob = await register('boundarybob'), outsider = await register('boundaryoutsider');
  const aliceId = (await (await fetch(`${base}/api/telegram/me`, { headers: { authorization: `Bearer ${alice.token}` } })).json()).id;
  const bobId = (await (await fetch(`${base}/api/telegram/me`, { headers: { authorization: `Bearer ${bob.token}` } })).json()).id;
  const peer = tl('TL_inputPeerUser', { user_id: bobId });
  const state = await rpc(bob.token, 'TL_updates_getState');
  assert.equal(state.pts, 1);
  for (const pts of [0, 1]) assert((await rpc(bob.token, 'TL_updates_getDifference', { pts })).$type.endsWith('$TL_updates_differenceEmpty'));
  const ids = [];
  for (let i = 0; i < 105; i++) {
    const sent = await rpc(alice.token, 'TL_messages_sendMessage', { peer, random_id: i + 1, message: `page-${i}` });
    ids.push(sent.updates[0].id);
  }
  const slice = await rpc(bob.token, 'TL_updates_getDifference', { pts: 1 });
  assert(slice.$type.endsWith('$TL_updates_differenceSlice'));
  assert.equal(slice.intermediate_state.pts, 101);
  assert.deepEqual(slice.new_messages.map(m => m.id), ids.slice(0, 100));
  assert.equal(slice.other_updates.length, 0);
  const zeroReplay = await rpc(bob.token, 'TL_updates_getDifference', { pts: 0 });
  assert.deepEqual(zeroReplay.new_messages, slice.new_messages);
  assert.equal(zeroReplay.intermediate_state.pts, 101);
  const tail = await rpc(bob.token, 'TL_updates_getDifference', { pts: slice.intermediate_state.pts });
  assert(tail.$type.endsWith('$TL_updates_difference'));
  assert.equal(tail.state.pts, 106);
  assert.deepEqual(tail.new_messages.map(m => m.id), ids.slice(100));
  assert.deepEqual((await rpc(bob.token, 'TL_updates_getDifference', { pts: 101 })).new_messages, tail.new_messages);
  assert((await rpc(bob.token, 'TL_updates_getDifference', { pts: 106 })).$type.endsWith('$TL_updates_differenceEmpty'));
  const tooLong = await rpc(bob.token, 'TL_updates_getDifference', { pts: 107 });
  assert(tooLong.$type.endsWith('$TL_updates_differenceTooLong'));
  assert.equal(tooLong.pts, 106);
  assert.equal(new Set([...slice.new_messages, ...tail.new_messages].map(m => m.id)).size, 105);

  for (const literal of ['-9223372036854775808', '9223372036854775807', '0', '-1']) {
    const fields = tl('TL_messages_sendMessage', { peer, message: `int64-${literal}` });
    const raw = JSON.stringify(fields).replace(/}$/, `,"random_id":${literal}}`);
    const first = await request('/api/telegram/rpc', alice.token, raw);
    assert.equal(first.rawResponse.match(/"random_id":(-?\d+)/)[1], literal);
    const duplicate = await request('/api/telegram/rpc', alice.token, raw);
    assert.equal(duplicate.result.updates[0].id, first.result.updates[0].id);
    assert.equal(duplicate.rawResponse.match(/"random_id":(-?\d+)/)[1], literal);
    const conflict = await request('/api/telegram/rpc', alice.token, raw.replace(`int64-${literal}`, 'changed'), 409);
    assert.equal(conflict.result.error, 'RANDOM_ID_DUPLICATE');
  }
  const names = ['Bold', 'Italic', 'Underline', 'Strike', 'Spoiler', 'Code', 'Pre', 'Url', 'TextUrl', 'Email', 'Phone', 'Mention', 'Hashtag', 'Cashtag', 'BotCommand'];
  const entities = names.map(name => tl(`TL_messageEntity${name}`, { offset: 1, length: 2,
    ...(name === 'Pre' ? { language: 'java' } : {}), ...(name === 'TextUrl' ? { url: 'https://example.invalid/' } : {}) }));
  const rich = await rpc(alice.token, 'TL_messages_sendMessage', { peer, random_id: 200, message: 'A😀Z', entities, silent: true,
    send_as: tl('TL_inputPeerSelf'), reply_to: tl('TL_inputReplyToMessage', { reply_to_msg_id: ids[0] }) });
  const message = rich.updates[1].message;
  assert.deepEqual(message.entities, entities);
  assert.equal(message.silent, true);
  assert.equal(message.reply_to.reply_to_msg_id, ids[0]);
  assert.equal(message.flags & (128 | 8 | 8192), 128 | 8 | 8192);
  const absent = (await rpc(alice.token, 'TL_messages_sendMessage', { peer, random_id: 201, message: 'plain', entities: null, reply_to: null, send_as: null, schedule_date: 0, reply_markup: null })).updates[1].message;
  assert.equal(absent.flags & (128 | 8 | 8192), 0);
  assert.equal(absent.entities, null);
  assert.equal(absent.reply_to, null);
  assert.equal(absent.silent, false);
  const empty = (await rpc(alice.token, 'TL_messages_sendMessage', { peer, random_id: 202, message: 'empty entities', entities: [], silent: false })).updates[1].message;
  assert.equal(empty.entities, null);
  for (const [offset, length] of [[2, 1], [1, 1], [-1, 1], [0, 0], [0, -1], [4, 1], [0, 5]]) {
    await error(alice.token, 'TL_messages_sendMessage', { peer, random_id: 300, message: 'A😀Z', entities: [tl('TL_messageEntityBold', { offset, length })] }, 400, 'ENTITY_BOUNDS_INVALID');
  }
  await error(alice.token, 'TL_messages_sendMessage', { peer, message: 'x', entities: Array(101).fill(tl('TL_messageEntityBold', { offset: 0, length: 1 })) }, 400, 'ENTITIES_TOO_LONG');
  for (const url of ['', 'x'.repeat(2049)]) await error(alice.token, 'TL_messages_sendMessage', { peer, message: 'x', entities: [tl('TL_messageEntityTextUrl', { offset: 0, length: 1, url })] }, 400, 'ENTITY_URL_INVALID');
  await error(alice.token, 'TL_messages_sendMessage', { peer, message: 'x', entities: [tl('TL_messageEntityCustomEmoji', { offset: 0, length: 1 })] }, 501, 'BOTGRAM_ENTITY_UNSUPPORTED');
  for (const optional of [{ schedule_date: 1 }, { reply_markup: tl('TL_replyKeyboardHide') }]) await error(alice.token, 'TL_messages_sendMessage', { peer, message: 'x', ...optional }, 501, 'BOTGRAM_MESSAGE_FEATURE_UNSUPPORTED');
  for (const optional of [{ top_msg_id: 1 }, { quote_text: 'quote' }, { reply_to_peer_id: tl('TL_inputPeerSelf') }]) await error(alice.token, 'TL_messages_sendMessage', { peer, message: 'x', reply_to: tl('TL_inputReplyToMessage', { reply_to_msg_id: ids[0], ...optional }) }, 501, 'BOTGRAM_REPLY_MODE_UNSUPPORTED');
  await error(alice.token, 'TL_messages_sendMessage', { peer, message: 'x', send_as: peer }, 403, 'SEND_AS_PEER_INVALID');
  const group = await rpc(alice.token, 'TL_messages_createChat', { title: 'boundary group', users: [tl('TL_inputUser', { user_id: bobId })] });
  const chatId = group.updates.chats[0].id;
  const groupPeer = tl('TL_inputPeerChat', { chat_id: chatId });
  for (const [type, fields] of [
    ['TL_messages_sendMessage', { peer: groupPeer, message: 'forbidden', random_id: 1 }],
    ['TL_messages_readHistory', { peer: groupPeer, max_id: ids[0] }],
    ['TL_messages_getFullChat', { chat_id: chatId }],
    ['TL_messages_getPeerSettings', { peer: groupPeer }]
  ]) await error(outsider.token, type, fields, 404, 'PEER_ID_INVALID');
  assert.equal((await rpc(outsider.token, 'TL_messages_getMessages', { id: [tl('TL_inputMessageID', { id: ids[0] })] })).messages.length, 0);
  const beforeRead = await rpc(bob.token, 'TL_updates_getState');
  const backPeer = tl('TL_inputPeerUser', { user_id: aliceId });
  assert.equal((await rpc(bob.token, 'TL_messages_readHistory', { peer: backPeer, max_id: ids[104] })).pts_count, 1);
  assert.equal((await rpc(bob.token, 'TL_messages_readHistory', { peer: backPeer, max_id: ids[0] })).pts_count, 0);
  const receipt = await rpc(bob.token, 'TL_updates_getDifference', { pts: beforeRead.pts });
  assert.equal(receipt.other_updates.length, 1);
  assert(receipt.other_updates[0].$type.endsWith('$TL_updateReadHistoryInbox'));
  assert.equal(receipt.other_updates[0].max_id, ids[104]);
  assert.equal(receipt.state.pts, beforeRead.pts + 1);
  console.log(`PASS: four difference variants, 105-event pagination/replay, exact int64 literals, 15 entities, UTF-16 rejection, optional fields, permissions; ${records.length} RPC calls, ${fixtures.length} successful TL fixtures`);
} catch (e) {
  console.error(logs);
  throw e;
} finally {
  if (server.exitCode === null) { const ended = once(server, 'exit'); server.kill(); await ended; }
  await writeFile(path.join(fixtureRoot, 'boundary-rpc-fixtures.json'), JSON.stringify(records, null, 2));
  await writeFile(path.join(fixtureRoot, 'boundary-tl-fixtures.json'), `[${fixtures.join(',')}]`);
}
