import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { once } from 'node:events';
import { mkdtemp, readFile, writeFile } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import net from 'node:net';

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const temp = await mkdtemp(path.join(process.env.BOTGRAM_TEST_ROOT ?? path.join(root, 'obj'), 'contacts-'));
const database = path.join(temp, 'db.json');
const socket = net.createServer().listen(0, '127.0.0.1');
await once(socket, 'listening');
const port = socket.address().port;
await new Promise(resolve => socket.close(resolve));
const base = `http://127.0.0.1:${port}`;
let server, logs = '';
async function start() {
  server = spawn('dotnet', [process.env.BOTGRAM_TEST_DLL ?? path.join(root, 'bin/Debug/net8.0/Botgram.Backend.dll'), '--urls', base], {
    cwd: root, env: { ...process.env, DataPath: database }, windowsHide: true, stdio: ['ignore', 'pipe', 'pipe']
  });
  server.stdout.on('data', x => logs += x); server.stderr.on('data', x => logs += x);
  for (let i = 0; i < 100; i++) {
    if (server.exitCode !== null) throw Error(logs);
    try { if ((await fetch(base + '/health')).ok) return; } catch {}
    await new Promise(resolve => setTimeout(resolve, 100));
  }
  throw Error(logs);
}
async function stop() {
  if (server && server.exitCode === null) { const exit = once(server, 'exit'); server.kill(); await exit; }
}
async function req(method, route, token, body, status = 200) {
  const response = await fetch(base + route, { method,
    headers: { 'content-type': 'application/json', ...(token ? { authorization: `Bearer ${token}` } : {}) },
    body: body === undefined ? undefined : JSON.stringify(body) });
  const data = await response.json();
  assert.equal(response.status, status, `${method} ${route}: ${JSON.stringify(data)}`);
  return data;
}
const type = name => 'org.telegram.tgnet.TLRPC$' + name;
const fixtures = [];
async function rpc(token, method, fields = {}, status = 200) {
  const response = await req('POST', '/api/telegram/rpc', token, { $type: type(method), ...fields }, status);
  if (status === 200) fixtures.push({ requestType: type(method), response });
  return response;
}
const contact = (phone, first_name = 'Private', last_name = 'Alias', client_id = 42) =>
  ({ $type: type('TL_inputPhoneContact'), phone, first_name, last_name, client_id });
const list = token => rpc(token, 'TL_contacts_getContacts', { hash: 0 });
const add = (token, contacts, status = 200) => rpc(token, 'TL_contacts_importContacts', { contacts }, status);
try {
  await start();
  const accounts = [];
  for (const username of ['alice', 'bob', 'carol']) {
    accounts.push(await req('POST', '/api/auth/register', null, { username, displayName: username, password: 'correct-horse-123' }));
  }
  const [alice, bob, carol] = accounts;
  assert.equal((await list(alice.token)).contacts.length, 0);
  await req('POST', '/api/telegram/rpc', null, { $type: type('TL_contacts_getContacts') }, 401);
  await req('POST', '/api/telegram/rpc', null, { $type: type('TL_contacts_importContacts'), contacts: [contact(bob.user.gid)] }, 401);
  const resolved = await rpc(alice.token, 'TL_contacts_resolvePhone', { phone: bob.user.gid });
  const bobId = resolved.users[0].id;
  assert.equal(resolved.users[0].contact, false);
  const added = await add(alice.token, [contact(bob.user.gid)]);
  assert.deepEqual(added.imported.map(x => [x.user_id, x.client_id]), [[bobId, 42]]);
  assert.equal(added.users[0].first_name, 'Private');
  assert.equal(added.users[0].last_name, 'Alias');
  assert.equal(added.users[0].contact, true);
  assert.equal(added.users[0].mutual_contact, false);
  assert(added.users[0].flags & 2048);
  assert.equal((await list(alice.token)).contacts[0].user_id, bobId);
  assert.equal((await list(bob.token)).contacts.length, 0);
  assert.equal((await list(carol.token)).contacts.length, 0);
  assert.equal((await rpc(carol.token, 'TL_contacts_resolvePhone', { phone: bob.user.gid })).users[0].first_name, 'bob');
  await add(alice.token, [contact(bob.user.gid.replace(/[^0-9]/g, ''), 'Updated', '', 43)]);
  assert.equal((await list(alice.token)).contacts.length, 1);
  assert.equal((await list(alice.token)).users[0].first_name, 'Updated');
  await add(bob.token, [contact(alice.user.gid, 'Alice private')]);
  assert.equal((await list(alice.token)).contacts[0].mutual, true);
  assert.equal((await list(alice.token)).users[0].mutual_contact, true);
  // Absent accounts and self do not create dangling/self contacts.
  const missing = '+030 99999999';
  assert.equal((await add(alice.token, [contact(missing)])).imported.length, 0);
  await rpc(alice.token, 'TL_contacts_resolvePhone', { phone: missing }, 400);
  assert.equal((await add(alice.token, [contact(alice.user.gid)])).imported.length, 0);
  for (const gid of ['+156 12345678', '+030 00000000', '+030 1234567', 'abc03012345678']) {
    await add(alice.token, [contact(gid)], 400);
  }
  await add(alice.token, [contact(bob.user.gid, '')], 400);
  await add(alice.token, [contact(bob.user.gid, 'x'.repeat(65))], 400);
  await add(alice.token, Array.from({ length: 101 }, () => contact(bob.user.gid)), 400);
  await add(alice.token, [null], 400);
  await add(alice.token, [42], 400);
  await add(alice.token, [{ ...contact(bob.user.gid), flags: 1, note: { text: 'unsupported' } }], 501);
  // A failed batch rolls back earlier additions and alias edits.
  await add(alice.token, [contact(carol.user.gid), contact(bob.user.gid, '')], 400);
  await add(alice.token, [contact(bob.user.gid, 'Must roll back'), contact('invalid')], 400);
  assert.equal((await list(alice.token)).contacts.length, 1);
  assert.equal((await list(alice.token)).users[0].first_name, 'Updated');
  await add(carol.token, [contact(bob.user.gid, 'Carol alias')]);
  const before = await rpc(carol.token, 'TL_updates_getState');
  const changed = await req('PUT', '/api/me/gid', bob.token, { regionCode: '030', number: '12345678' });
  const diff = await rpc(carol.token, 'TL_updates_getDifference', { pts: before.pts });
  assert(diff.other_updates.some(x => x.phone === changed.phone && x.user_id === bobId));
  assert(diff.users.some(x => x.id === bobId && x.first_name === 'Carol alias' && x.phone === changed.phone));
  const after = await list(alice.token);
  assert.equal(after.users[0].id, bobId);
  assert.equal(after.users[0].phone, changed.phone);
  await rpc(alice.token, 'TL_contacts_resolvePhone', { phone: bob.user.gid }, 400);
  // Sending and first history load use the original TL peer after adding.
  const peer = { $type: type('TL_inputPeerUser'), user_id: bobId, access_hash: 1 };
  await rpc(alice.token, 'TL_messages_sendMessage', { peer, message: 'contact hello', random_id: 73001 });
  assert.equal((await rpc(alice.token, 'TL_messages_getHistory', { peer, limit: 10 })).messages[0].message, 'contact hello');
  await stop(); await start();
  assert.deepEqual(await list(alice.token), after);
  assert.equal((await list(carol.token)).users[0].first_name, 'Carol alias');
  await stop();
  // Old snapshots lacking the new collection remain readable.
  const legacy = JSON.parse(await readFile(database, 'utf8'));
  delete legacy.contacts;
  await writeFile(database, JSON.stringify(legacy));
  await start();
  assert.equal((await list(alice.token)).contacts.length, 0);
  await writeFile(path.join(root, 'obj/contact-tl-fixtures.json'), JSON.stringify(fixtures));
  console.log(`PASS: GID import/resolve/list, aliases and owner isolation, mutual flags, duplicates, invalid/missing/self, atomic rollback, GID changes, messaging, restart and legacy snapshot; ${fixtures.length} TL fixtures`);
} finally { await stop(); }
