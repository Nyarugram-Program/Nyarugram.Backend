import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { once } from 'node:events';
import { mkdtemp, readFile, writeFile } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import net from 'node:net';

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const temp = await mkdtemp(path.join(process.env.BOTGRAM_TEST_ROOT ?? path.join(root, 'obj'), 'gid-'));
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
const rpc = (token, method, fields = {}) => req('POST', '/api/telegram/rpc', token, { $type: 'org.telegram.tgnet.TLRPC$' + method, ...fields });
try {
  await start();
  const accounts = [];
  for (const [username, country, region] of [['alice','CN','030'], ['bob','US','021'], ['carol',null,'001'], ['dave','ZZ','001'], ['erin','BR','005']]) {
    const account = await req('POST', '/api/auth/register', null, { username, displayName: username, password: 'correct-horse-123', locationCountry: country });
    assert.match(account.user.gid, new RegExp(`^\\+${region} [0-9]{8}$`));
    assert(!account.user.gid.endsWith('00000000'));
    accounts.push(account);
  }
  assert.equal(new Set(accounts.map(x => x.user.gid)).size, accounts.length);
  const [alice,bob,carol] = accounts;
  await req('PUT','/api/me/gid',null,{regionCode:'030',number:'12345678'},401);
  const regions = await req('GET','/api/gid/regions',alice.token);
  assert.equal(regions.length,29); assert(!regions.some(x => x.code === '001'));
  const conversation = await req('POST','/api/conversations',alice.token,{kind:'direct',memberIds:[bob.user.id]});
  const before = await rpc(bob.token,'TL_updates_getState');
  const updated = await req('PUT','/api/me/gid',alice.token,{regionCode:'030',number:'12345678'});
  assert.equal(updated.phone,'03012345678');
  const diff = await rpc(bob.token,'TL_updates_getDifference',{pts:before.pts,date:before.date,qts:0});
  assert(diff.other_updates.some(x => x.$type === 'org.telegram.tgnet.tl.TL_update$TL_updateUserPhone' && x.phone === updated.phone));
  assert(diff.users.some(x => x.phone === updated.phone));
  await writeFile(path.join(root,'obj/gid-tl-fixtures.json'), JSON.stringify([
    {requestType:'org.telegram.tgnet.TLRPC$TL_updates_getDifference',response:diff},
    {requestType:'org.telegram.tgnet.TLRPC$TL_users_getUsers',response:{ $type:'org.telegram.tgnet.Vector',objects:[updated] }}
  ]));
  await req('PUT','/api/me/gid',alice.token,{regionCode:'030',number:'12345678'}); // Own GID is allowed.
  for (const [regionCode,number] of [['001','12345678'],['156','12345678'],['030','00000000'],['030','1234567'],['030','123456789'],['03','012345678'],['030','１２３４５６７８']]) {
    await req('PUT','/api/me/gid',alice.token,{regionCode,number},400);
  }
  await req('PUT','/api/me/gid',bob.token,{regionCode:'030',number:'12345678'},409);
  const attempts = await Promise.all([bob,carol].map(x => fetch(base+'/api/me/gid', {method:'PUT',headers:{'content-type':'application/json',authorization:`Bearer ${x.token}`},body:JSON.stringify({regionCode:'150',number:'87654321'})})));
  assert.deepEqual(attempts.map(x => x.status).sort(),[200,409]);
  assert.equal((await req('GET','/api/me',alice.token)).gid,'+030 12345678');
  await stop(); await start();
  assert.equal((await req('GET','/api/me',alice.token)).gid,'+030 12345678');
  assert.equal((await req('GET','/api/conversations',bob.token)).find(x=>x.conversation.id===conversation.id).conversation.id,conversation.id);
  await stop();
  const legacy = JSON.parse(await readFile(database,'utf8'));
  legacy.users.forEach(x => delete x.gid);
  await writeFile(database,JSON.stringify(legacy));
  await start();
  const migrated = await Promise.all(accounts.map(x => req('GET','/api/me',x.token)));
  migrated.forEach((x,i)=>{assert.equal(x.id,accounts[i].user.id);assert.match(x.gid,/^\+001 (?!00000000)[0-9]{8}$/);});
  assert.equal(new Set(migrated.map(x=>x.gid)).size,accounts.length);
  await stop(); await start();
  for (const [i,x] of accounts.entries()) assert.equal((await req('GET','/api/me',x.token)).gid,migrated[i].gid);
  console.log('PASS: regional registration/fallback, 29 regions, GID validation, same-owner update, collisions and concurrent claim, TL updates, persistence, legacy migration with stable accounts/sessions/chats');
} finally { await stop(); }
