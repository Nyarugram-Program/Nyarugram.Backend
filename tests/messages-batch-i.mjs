// Batch I media regression: binary part upload/range download plus the four media RPCs
// against a real PostgreSQL-authoritative backend. Uses an isolated database only.
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
const temp = await mkdtemp(path.join(process.env.BOTGRAM_TEST_ROOT ?? path.join(root, 'obj'), 'messages-i-'));
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

const T = {
  getHistory: 105, sendMessage: 107, deleteMessages: 144, forwardMessages: 150,
  editMessage: 148, uploadMedia: 170, sendMedia: 171, sendMultiMedia: 172,
  getExtendedMedia: 173, getState: 100
};
const rpc = (token, tag, payload = []) => call(base, '/api/v1/rpc', envelope(tag, payload), token);
const num = (list, field) => { const entry = first(list, field); return entry ? Number(entry.value) : 0; };
const userPeer = id => concat(bfield(1, concat(sfield(1, domain), sfield(2, id))));
const selfPeer = () => vfield(3, 1);
const randomId = () => BigInt(Date.now()) * 1000n + BigInt(++sequence);

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
async function pts(token) {
  return num(fields(outcome(await rpc(token, T.getState), T.getState)), 1);
}
async function sendText(token, peer, text) {
  const response = await rpc(token, T.sendMessage, concat(bfield(1, peer), fixed64(2, randomId()), sfield(3, text)));
  return num(fields(first(fields(outcome(response, T.sendMessage)), 1).bytes), 1);
}

function mediaSource({ clientFileId, mediaId, kind = 1, mime = 'image/jpeg', name = '', size = 0,
  width = 0, height = 0, duration = 0, parts = 0 }) {
  const values = [];
  if (clientFileId !== undefined) values.push(vfield(1, clientFileId));
  if (mediaId !== undefined) values.push(vfield(2, mediaId));
  values.push(vfield(3, kind), sfield(4, mime), sfield(5, name), vfield(6, size),
    vfield(7, width), vfield(8, height), vfield(9, duration), vfield(10, parts));
  return concat(...values);
}
async function uploadPart(token, fileId, part, total, bytes) {
  const query = total > 0 ? `?total=${total}` : '';
  const response = await fetch(`${base}/api/v1/media/${fileId}/${part}${query}`, {
    method: 'PUT',
    headers: { 'content-type': 'application/octet-stream', authorization: `Bearer ${token}` },
    body: bytes
  });
  return response.status;
}
async function download(token, mediaId, offset, limit) {
  const response = await fetch(`${base}/api/v1/media/${mediaId}?offset=${offset}&limit=${limit}`, {
    headers: { authorization: `Bearer ${token}` }
  });
  return { status: response.status, bytes: Buffer.from(await response.arrayBuffer()),
    size: Number(response.headers.get('x-botgram-media-size') ?? 0) };
}
function mediaRef(bytes) {
  const media = first(bytes, 1);
  const ref = media ? fields(media.bytes) : fields(bytes);
  return {
    id: num(ref, 1), kind: num(ref, 2), mime: stringOf(first(ref, 3) ?? { bytes: Buffer.alloc(0) }),
    size: num(ref, 4), width: num(ref, 5), height: num(ref, 6), duration: num(ref, 7),
    name: stringOf(first(ref, 8) ?? { bytes: Buffer.alloc(0) }), parts: num(ref, 9)
  };
}
// Update.new_message -> NewMessage.message -> Message fields.
function newMessages(updates) {
  return all(updates, 1).map(entry => fields(entry.bytes))
    .filter(update => update[0].number === 1)
    .map(update => fields(first(fields(first(update, 1).bytes), 1).bytes));
}
function messageMedia(message) {
  const entry = first(message, 15);
  return entry ? mediaRef(entry.bytes) : null;
}
function messageText(message) {
  const text = first(message, 10);
  return text ? stringOf(first(fields(text.bytes), 1)) : '';
}
// MessageEntity with a bold kind, for the request-identity checks.
const bold = (offset, length) => concat(vfield(1, offset), vfield(2, length), bfield(10, Buffer.alloc(0)));
function albumItem(random, source, caption = '', options = {}) {
  const values = [fixed64(1, random), bfield(2, source), sfield(3, caption)];
  for (const entity of options.entities ?? []) values.push(bfield(4, entity));
  if (options.replyTo !== undefined) values.push(vfield(5, options.replyTo));
  if (options.silent) values.push(vfield(6, 1));
  return bfield(2, concat(...values));
}
async function sendMedia(token, peer, source, caption = '', random = randomId(), options = {}) {
  const values = [bfield(1, peer), fixed64(2, random), bfield(3, source), sfield(4, caption)];
  for (const entity of options.entities ?? []) values.push(bfield(5, entity));
  if (options.replyTo !== undefined) values.push(vfield(6, options.replyTo));
  if (options.silent) values.push(vfield(7, 1));
  return rpc(token, T.sendMedia, concat(...values));
}

const part = (fill, size = 100) => Buffer.alloc(size, fill);

try {
  await start();
  const suffix = Date.now().toString(36);
  const password = 'batch-i-password';
  const a = await register(base, 'batch_i_alice_' + suffix, password, 'Alice', 'CN');
  const b = await register(base, 'batch_i_bob_' + suffix, password, 'Bob');
  const c = await register(base, 'batch_i_carol_' + suffix, password, 'Carol');
  const alice = a.token, bob = b.token, carol = c.token;
  const aliceId = profile(a.user).userId, bobId = profile(b.user).userId, carolId = profile(c.user).userId;
  const direct = userPeer(bobId);
  await sendText(alice, direct, 'media chat');
  ok();

  // ---------------------------------------------------------------- part upload
  const fileId = 7_000_000_000_000_001;
  assert.equal(await uploadPart(alice, fileId, 0, 3, part(0x11)), 204);
  assert.equal(await uploadPart(alice, fileId, 1, 3, part(0x22)), 204);
  assert.equal(await uploadPart(alice, fileId, 2, 3, part(0x33, 40)), 204);
  ok();
  // Retrying the same bytes is idempotent; different bytes for the same part conflict.
  assert.equal(await uploadPart(alice, fileId, 0, 3, part(0x11)), 204);
  assert.equal(await uploadPart(alice, fileId, 0, 3, part(0x99)), 409);
  ok();
  // The declared part count is fixed by the first part.
  assert.equal(await uploadPart(alice, fileId, 0, 4, part(0x11)), 409);
  // A part beyond the declared range is rejected.
  assert.equal(await uploadPart(alice, fileId, 3, 3, part(0x44)), 400);
  ok();

  // ---------------------------------------------------------------- uploadMedia
  const uploaded = fields(outcome(await rpc(alice, T.uploadMedia, concat(bfield(1, direct),
    bfield(2, mediaSource({ clientFileId: fileId, kind: 1, mime: 'image/jpeg', name: 'photo.jpg',
      size: 240, width: 640, height: 480, parts: 3 })))), T.uploadMedia));
  const photo = mediaRef(uploaded);
  assert.ok(photo.id > 0);
  assert.equal(photo.kind, 1);
  assert.equal(photo.mime, 'image/jpeg');
  assert.equal(photo.size, 240);
  assert.equal(photo.parts, 3);
  assert.equal(photo.width, 640);
  ok();
  // A declared size that does not match the uploaded bytes is refused.
  const otherFile = 7_000_000_000_000_002;
  assert.equal(await uploadPart(alice, otherFile, 0, 1, part(0x55, 10)), 204);
  expectError(await rpc(alice, T.uploadMedia, concat(bfield(1, direct),
    bfield(2, mediaSource({ clientFileId: otherFile, size: 99, parts: 1 })))),
    409, 'MEDIA_SIZE_MISMATCH');
  ok();

  // ---------------------------------------------------------------- sendMedia
  const sent = await sendMedia(alice, direct, mediaSource({ mediaId: photo.id }), 'hello media');
  const sentUpdates = fields(outcome(sent, T.sendMedia));
  const sentMessage = newMessages(sentUpdates)[0];
  assert.ok(sentMessage, 'media message present');
  assert.ok(first(sentMessage, 15), 'media body present');
  const sentMedia = messageMedia(sentMessage);
  assert.equal(sentMedia.id, photo.id);
  assert.equal(messageText(sentMessage), 'hello media');
  ok();
  // Revoking a text media message with the same random id returns the same message.
  const retryRandom = randomId();
  const firstSend = await sendMedia(alice, direct, mediaSource({ mediaId: photo.id }), 'retry', retryRandom);
  const firstId = num(newMessages(fields(outcome(firstSend, T.sendMedia)))[0], 1);
  const retried = await sendMedia(alice, direct, mediaSource({ mediaId: photo.id }), 'retry', retryRandom);
  assert.equal(num(newMessages(fields(outcome(retried, T.sendMedia)))[0], 1), firstId);
  ok();

  // The retry identity is the whole request, not only the media: changed content or
  // options on the same random id are a conflict, never a silent replay.
  const identityFile = 7_000_000_000_000_045;
  assert.equal(await uploadPart(alice, identityFile, 0, 0, part(0x4a, 14)), 204);
  const identitySource = mediaSource({ clientFileId: identityFile, size: 14, parts: 1 });
  const identityRandom = randomId();
  const identityMessage = newMessages(fields(outcome(
    await sendMedia(alice, direct, identitySource, 'identity', identityRandom), T.sendMedia)))[0];
  const identityId = num(identityMessage, 1);
  expectError(await sendMedia(alice, direct, identitySource, 'identity changed', identityRandom),
    409, 'RANDOM_ID_DUPLICATE');
  ok();
  expectError(await sendMedia(alice, direct, identitySource, 'identity', identityRandom,
    { entities: [bold(0, 1)] }), 409, 'RANDOM_ID_DUPLICATE');
  ok();
  expectError(await sendMedia(alice, direct, identitySource, 'identity', identityRandom,
    { silent: true }), 409, 'RANDOM_ID_DUPLICATE');
  ok();
  expectError(await sendMedia(alice, direct, identitySource, 'identity', identityRandom,
    { replyTo: identityId }), 409, 'RANDOM_ID_DUPLICATE');
  ok();
  const identityReplay = await sendMedia(alice, direct, identitySource, 'identity', identityRandom);
  assert.equal(num(newMessages(fields(outcome(identityReplay, T.sendMedia)))[0], 1), identityId,
    'an identical retry replays the original message');
  ok();
  // Identical entities are part of an unchanged request; different ones are not.
  const entityRandom = randomId();
  const entityMessage = newMessages(fields(outcome(await sendMedia(alice, direct, identitySource,
    'identity', entityRandom, { entities: [bold(0, 4)] }), T.sendMedia)))[0];
  const entityReplay = await sendMedia(alice, direct, identitySource, 'identity', entityRandom,
    { entities: [bold(0, 4)] });
  assert.equal(num(newMessages(fields(outcome(entityReplay, T.sendMedia)))[0], 1), num(entityMessage, 1));
  ok();
  expectError(await sendMedia(alice, direct, identitySource, 'identity', entityRandom,
    { entities: [bold(0, 5)] }), 409, 'RANDOM_ID_DUPLICATE');
  ok();
  // The identity is checked before media authorization: with the only message deleted the
  // media is revoked, yet a changed request must still be a conflict instead of a 404.
  const revokedIdentityFile = 7_000_000_000_000_046;
  assert.equal(await uploadPart(alice, revokedIdentityFile, 0, 0, part(0x4b, 14)), 204);
  const revokedIdentitySource = mediaSource({ clientFileId: revokedIdentityFile, size: 14, parts: 1 });
  const revokedIdentityRandom = randomId();
  const revokedIdentity = newMessages(fields(outcome(await sendMedia(alice, direct, revokedIdentitySource,
    'revoke identity', revokedIdentityRandom), T.sendMedia)))[0];
  await rpc(alice, T.deleteMessages, concat(vfield(1, num(revokedIdentity, 1)), vfield(2, 1)));
  expectError(await sendMedia(alice, direct, revokedIdentitySource, 'revoke identity changed',
    revokedIdentityRandom), 409, 'RANDOM_ID_DUPLICATE');
  ok();

  // The request snapshot is immutable: editing the caption keeps the original request
  // retryable, while a request carrying the edited caption is a different request.
  const editableFile = 7_000_000_000_000_047;
  assert.equal(await uploadPart(alice, editableFile, 0, 0, part(0x4c, 12)), 204);
  const editableSource = mediaSource({ clientFileId: editableFile, size: 12, parts: 1 });
  const editableRandom = randomId();
  const editableMessage = newMessages(fields(outcome(await sendMedia(alice, direct, editableSource,
    'editable', editableRandom), T.sendMedia)))[0];
  const editableId = num(editableMessage, 1);
  assert.equal((await rpc(alice, T.editMessage, concat(bfield(1, direct), vfield(2, editableId),
    sfield(3, 'edited caption')))).status, 200);
  const editableRetry = await sendMedia(alice, direct, editableSource, 'editable', editableRandom);
  assert.equal(editableRetry.status, 200,
    'a retry after editing the caption returns the original message');
  const editableRetryMessage = newMessages(fields(outcome(editableRetry, T.sendMedia)))[0];
  assert.equal(num(editableRetryMessage, 1), editableId);
  assert.equal(messageText(editableRetryMessage), 'edited caption', 'the retry reflects the current body');
  ok();
  expectError(await sendMedia(alice, direct, editableSource, 'edited caption', editableRandom),
    409, 'RANDOM_ID_DUPLICATE');
  ok();

  // ---------------------------------------------------------------- download
  const firstRange = await download(bob, photo.id, 0, 100);
  assert.equal(firstRange.status, 200);
  assert.equal(firstRange.size, 240);
  assert.deepEqual(firstRange.bytes, part(0x11));
  const secondRange = await download(bob, photo.id, 100, 100);
  assert.deepEqual(secondRange.bytes, part(0x22));
  const lastRange = await download(bob, photo.id, 200, 100);
  assert.equal(lastRange.bytes.length, 40);
  assert.deepEqual(lastRange.bytes, part(0x33, 40));
  ok();
  // A non-member cannot read the bytes; the owner can.
  assert.equal((await download(carol, photo.id, 0, 100)).status, 404);
  assert.equal((await download(alice, photo.id, 0, 100)).status, 200);
  // Out-of-range reads are explicit errors.
  assert.equal((await download(bob, photo.id, 240, 10)).status, 400);
  ok();

  // ---------------------------------------------------------------- incomplete uploads
  const partialFile = 7_000_000_000_000_003;
  assert.equal(await uploadPart(alice, partialFile, 0, 2, part(0x66)), 204);
  expectError(await sendMedia(alice, direct, mediaSource({ clientFileId: partialFile, size: 100, parts: 2 })),
    409, 'MEDIA_UPLOAD_INCOMPLETE');
  ok();
  expectError(await sendMedia(alice, direct, mediaSource({ clientFileId: 7_000_000_000_000_099, parts: 1 })),
    400, 'MEDIA_UPLOAD_MISSING');
  ok();

  // A small file uploads without a total, and an unknown-size upload uses -1.
  const smallFile = 7_000_000_000_000_004;
  assert.equal(await uploadPart(alice, smallFile, 0, 0, part(0x44, 12)), 204);
  const smallMedia = mediaRef(fields(outcome(await rpc(alice, T.uploadMedia, concat(bfield(1, direct),
    bfield(2, mediaSource({ clientFileId: smallFile, kind: 1, mime: 'image/jpeg', size: 12, parts: 1 })))),
    T.uploadMedia)));
  assert.equal(smallMedia.parts, 1, 'a small file without a total completes');
  ok();
  const unknownFile = 7_000_000_000_000_005;
  assert.equal(await uploadPart(alice, unknownFile, 0, -1, part(0x45, 10)), 204);
  assert.equal(await uploadPart(alice, unknownFile, 1, -1, part(0x46, 10)), 204);
  const unknownMedia = mediaRef(fields(outcome(await rpc(alice, T.uploadMedia, concat(bfield(1, direct),
    bfield(2, mediaSource({ clientFileId: unknownFile, kind: 2, mime: 'application/pdf', size: 20, parts: 2 })))),
    T.uploadMedia)));
  assert.equal(unknownMedia.parts, 2, 'an unknown-size upload completes from its part count');
  ok();

  // A globally deleted message revokes its bytes and its reuse.
  const revokedFile = 7_000_000_000_000_041;
  assert.equal(await uploadPart(alice, revokedFile, 0, 0, part(0x47, 16)), 204);
  const revokedRandom = randomId();
  const revokedRequest = mediaSource({ clientFileId: revokedFile, kind: 2, mime: 'application/pdf', size: 16, parts: 1 });
  const revokedSend = await sendMedia(alice, direct, revokedRequest, 'revoked', revokedRandom);
  const revokedMessage = newMessages(fields(outcome(revokedSend, T.sendMedia)))[0];
  const revokedMedia = messageMedia(revokedMessage);
  assert.equal((await download(bob, revokedMedia.id, 0, 16)).status, 200, 'recipient can read before deletion');
  const deleteResponse = await rpc(alice, T.deleteMessages, concat(vfield(1, num(revokedMessage, 1)), vfield(2, 1)));
  assert.equal((await download(bob, revokedMedia.id, 0, 16)).status, 404,
    'a global deletion revokes the recipient download');
  assert.equal((await download(alice, revokedMedia.id, 0, 16)).status, 404,
    'a global deletion revokes the sender download');
  expectError(await sendMedia(alice, direct, mediaSource({ mediaId: revokedMedia.id })),
    404, 'MEDIA_NOT_FOUND');
  ok();
  // The same bytes referenced by the original upload id follow the same rule.
  expectError(await sendMedia(alice, direct,
    mediaSource({ clientFileId: revokedFile, kind: 2, mime: 'application/pdf', size: 16, parts: 1 })),
    404, 'MEDIA_NOT_FOUND');
  ok();
  // An idempotent retry of the committed send is answered before media authorization and
  // returns the original message and pts; downloads stay revoked.
  const revokedRetry = await sendMedia(alice, direct, revokedRequest, 'revoked', revokedRandom);
  const revokedRetryMessage = newMessages(fields(outcome(revokedRetry, T.sendMedia)))[0];
  assert.equal(num(revokedRetryMessage, 1), num(revokedMessage, 1),
    'a committed media retry returns the original message');
  assert.equal((await download(bob, revokedMedia.id, 0, 16)).status, 404,
    'the committed retry does not restore downloads');
  ok();

  // Forwarding keeps the attachment, and the copy alone keeps the bytes readable.
  const forwardFile = 7_000_000_000_000_042;
  assert.equal(await uploadPart(alice, forwardFile, 0, 0, part(0x48, 18)), 204);
  const forwardSource = await sendMedia(alice, direct,
    mediaSource({ clientFileId: forwardFile, kind: 2, mime: 'application/pdf', size: 18, parts: 1 }), 'forward me');
  const forwardSourceMessage = newMessages(fields(outcome(forwardSource, T.sendMedia)))[0];
  const forwardSourceMedia = messageMedia(forwardSourceMessage);
  const forwarded = await rpc(alice, T.forwardMessages, concat(bfield(1, direct), bfield(2, direct),
    vfield(3, num(forwardSourceMessage, 1)), fixed64(4, randomId())));
  const forwardedMessageBody = newMessages(fields(outcome(forwarded, T.forwardMessages)))[0];
  const forwardedMedia = messageMedia(forwardedMessageBody);
  assert.ok(forwardedMedia, 'the forwarded copy keeps its media');
  assert.equal(forwardedMedia.id, forwardSourceMedia.id, 'the forwarded copy reuses the same media');
  assert.equal(messageText(forwardedMessageBody), 'forward me', 'the caption survives the forward');
  ok();
  await rpc(alice, T.deleteMessages, concat(vfield(1, num(forwardSourceMessage, 1)), vfield(2, 1)));
  assert.equal((await download(bob, forwardSourceMedia.id, 0, 18)).status, 200,
    'the forwarded copy keeps the bytes readable');
  await rpc(alice, T.deleteMessages, concat(vfield(1, num(forwardedMessageBody, 1)), vfield(2, 1)));
  assert.equal((await download(bob, forwardSourceMedia.id, 0, 18)).status, 404,
    'deleting every reference revokes the bytes');
  ok();

  // A photo without declared dimensions gets them from the image header.
  const jpeg = Buffer.from('ffd8ffc00011080258032003012200021101031101ffd9', 'hex');
  const jpegFile = 7_000_000_000_000_043;
  assert.equal(await uploadPart(alice, jpegFile, 0, 0, jpeg), 204);
  const jpegMedia = mediaRef(fields(outcome(await rpc(alice, T.uploadMedia, concat(bfield(1, direct),
    bfield(2, mediaSource({ clientFileId: jpegFile, kind: 1, mime: 'image/jpeg', size: jpeg.length, parts: 1 })))),
    T.uploadMedia)));
  assert.equal(jpegMedia.width, 800, 'photo width parsed from the JPEG header');
  assert.equal(jpegMedia.height, 600, 'photo height parsed from the JPEG header');
  ok();
  // The same header split across two parts must still be read.
  const splitFile = 7_000_000_000_000_044;
  assert.equal(await uploadPart(alice, splitFile, 0, 2, jpeg.subarray(0, 10)), 204);
  assert.equal(await uploadPart(alice, splitFile, 1, 2, jpeg.subarray(10)), 204);
  const splitMedia = mediaRef(fields(outcome(await rpc(alice, T.uploadMedia, concat(bfield(1, direct),
    bfield(2, mediaSource({ clientFileId: splitFile, kind: 1, mime: 'image/jpeg', size: jpeg.length, parts: 2 })))),
    T.uploadMedia)));
  assert.equal(splitMedia.width, 800, 'photo width parsed across parts');
  assert.equal(splitMedia.height, 600, 'photo height parsed across parts');
  ok();

  // ---------------------------------------------------------------- albums
  const albumIds = [7_000_000_000_000_011, 7_000_000_000_000_012];
  for (const id of albumIds) assert.equal(await uploadPart(alice, id, 0, 1, part(0x77, 32)), 204);
  const albumR1 = randomId(), albumR2 = randomId();
  const albumBody = () => concat(bfield(1, direct),
    albumItem(albumR1, mediaSource({ clientFileId: albumIds[0], size: 32, parts: 1 }), 'first'),
    albumItem(albumR2, mediaSource({ clientFileId: albumIds[1], size: 32, parts: 1 }), 'second'));
  const album = await rpc(alice, T.sendMultiMedia, albumBody());
  const albumMessages = newMessages(fields(outcome(album, T.sendMultiMedia)));
  assert.equal(albumMessages.length, 2);
  assert.equal(messageText(albumMessages[0]), 'first');
  assert.equal(messageText(albumMessages[1]), 'second');
  const grouped = num(albumMessages[0], 16);
  assert.ok(grouped > 0);
  assert.equal(num(albumMessages[1], 16), grouped, 'both album items share the group id');
  assert.ok(messageMedia(albumMessages[0]) && messageMedia(albumMessages[1]));
  ok();
  // A retry of the whole album replays both messages under the original group.
  const albumReplay = await rpc(alice, T.sendMultiMedia, albumBody());
  const replayMessages = newMessages(fields(outcome(albumReplay, T.sendMultiMedia)));
  assert.equal(replayMessages.length, 2);
  assert.equal(num(replayMessages[0], 1), num(albumMessages[0], 1), 'the first item replays');
  assert.equal(num(replayMessages[1], 1), num(albumMessages[1], 1), 'the second item replays');
  assert.equal(num(replayMessages[0], 16), grouped, 'the replay keeps the original group');
  assert.equal(num(replayMessages[1], 16), grouped, 'both replay items keep the original group');
  ok();
  // Editing an album caption keeps the group replay intact.
  assert.equal((await rpc(alice, T.editMessage, concat(bfield(1, direct),
    vfield(2, num(albumMessages[0], 1)), sfield(3, 'first edited')))).status, 200);
  const editedReplay = newMessages(fields(outcome(await rpc(alice, T.sendMultiMedia, albumBody()),
    T.sendMultiMedia)));
  assert.equal(num(editedReplay[0], 1), num(albumMessages[0], 1));
  assert.equal(num(editedReplay[1], 1), num(albumMessages[1], 1));
  assert.equal(num(editedReplay[0], 16), grouped);
  ok();
  // A changed caption on an album item is a conflicting request, not a replay.
  expectError(await rpc(alice, T.sendMultiMedia, concat(bfield(1, direct),
    albumItem(albumR1, mediaSource({ clientFileId: albumIds[0], size: 32, parts: 1 }), 'first changed'),
    albumItem(albumR2, mediaSource({ clientFileId: albumIds[1], size: 32, parts: 1 }), 'second'))),
    409, 'RANDOM_ID_DUPLICATE');
  ok();
  // Duplicate random ids would make two items claim one commit; the album is rejected
  // before anything is written.
  const duplicateFile = 7_000_000_000_000_014;
  assert.equal(await uploadPart(alice, duplicateFile, 0, 1, part(0x79, 32)), 204);
  const duplicateRandom = randomId();
  const duplicateItem = () => albumItem(duplicateRandom,
    mediaSource({ clientFileId: duplicateFile, size: 32, parts: 1 }), 'duplicate');
  expectError(await rpc(alice, T.sendMultiMedia, concat(bfield(1, direct), duplicateItem(), duplicateItem())),
    400, 'BOTGRAM_RPC_INVALID_ARGUMENT');
  ok();
  const duplicateRow = dbQuery(`SELECT count(*) FROM botgram_intake_v1.messages ` +
    `WHERE client_key='tl:${duplicateRandom}'`);
  if (duplicateRow !== null) {
    assert.equal(duplicateRow, '0', 'a duplicate-id album writes no message');
    ok();
  }
  // Mixing a committed item with a new one would answer with two different groups, so the
  // album request is rejected as a conflict and the new item is not written.
  const mixedFile = 7_000_000_000_000_013;
  assert.equal(await uploadPart(alice, mixedFile, 0, 1, part(0x78, 32)), 204);
  const mixedRandom = randomId();
  const mixedBody = () => concat(bfield(1, direct),
    albumItem(albumR1, mediaSource({ clientFileId: albumIds[0], size: 32, parts: 1 }), 'first'),
    albumItem(mixedRandom, mediaSource({ clientFileId: mixedFile, size: 32, parts: 1 }), 'third'));
  expectError(await rpc(alice, T.sendMultiMedia, mixedBody()), 409, 'RANDOM_ID_DUPLICATE');
  ok();
  const mixedRow = dbQuery(`SELECT count(*) FROM botgram_intake_v1.messages ` +
    `WHERE client_key='tl:${mixedRandom}'`);
  if (mixedRow !== null) {
    assert.equal(mixedRow, '0', 'a rejected mixed album writes no message');
    ok();
  }
  // A committed plain send carries no group, so it cannot be combined into an album.
  expectError(await rpc(alice, T.sendMultiMedia, concat(bfield(1, direct),
    albumItem(retryRandom, mediaSource({ mediaId: photo.id }), 'retry'),
    albumItem(albumR2, mediaSource({ clientFileId: albumIds[1], size: 32, parts: 1 }), 'second'))),
    409, 'RANDOM_ID_DUPLICATE');
  ok();
  // A single-item album is rejected; the client sends plain sendMedia instead.
  const singleItem = bfield(2, concat(
    fixed64(1, randomId()),
    bfield(2, mediaSource({ mediaId: photo.id }))));
  expectError(await rpc(alice, T.sendMultiMedia, concat(bfield(1, direct), singleItem)),
    400, 'MEDIA_ALBUM_SIZE_INVALID');
  ok();

  // ---------------------------------------------------------------- E2EE admission
  const bc = userPeer(carolId);
  await sendText(bob, bc, 'e2ee media chat');
  await rpc(bob, 139, concat(vfield(1, 1), vfield(2, 1234), bfield(3, Buffer.alloc(33, 0x51)),
    vfield(4, 10), bfield(5, Buffer.alloc(33, 2)), bfield(6, Buffer.alloc(32, 3)),
    vfield(7, 11), bfield(8, Buffer.alloc(800, 4)), bfield(9, Buffer.alloc(32, 5))));
  await rpc(carol, 139, concat(vfield(1, 1), vfield(2, 1234), bfield(3, Buffer.alloc(33, 0x52)),
    vfield(4, 10), bfield(5, Buffer.alloc(33, 2)), bfield(6, Buffer.alloc(32, 3)),
    vfield(7, 11), bfield(8, Buffer.alloc(800, 4)), bfield(9, Buffer.alloc(32, 5))));
  const inviteBody = concat(bfield(1, bc), vfield(2, 1), vfield(3, 2));
  const acceptBody = concat(bfield(1, userPeer(bobId)), vfield(2, 2), vfield(3, 2));
  assert.equal((await rpc(bob, 140, inviteBody)).status, 200);
  assert.equal((await rpc(carol, 140, acceptBody)).status, 200);
  const mediaFile = 7_000_000_000_000_021;
  assert.equal(await uploadPart(bob, mediaFile, 0, 1, part(0x88, 20)), 204);
  expectError(await sendMedia(bob, bc, mediaSource({ clientFileId: mediaFile, size: 20, parts: 1 })),
    409, 'E2EE_PLAINTEXT_FORBIDDEN');
  ok();

  // ---------------------------------------------------------------- product boundary
  const extended = await rpc(alice, T.getExtendedMedia, concat(bfield(1, direct), vfield(2, photo.id)));
  expectError(extended, 501, 'BOTGRAM_EXTENDED_MEDIA_UNSUPPORTED');
  ok();

  // ---------------------------------------------------------------- abandoned upload sweep
  const abandoned = 7_000_000_000_000_031;
  assert.equal(await uploadPart(alice, abandoned, 0, 2, part(0x99, 8)), 204);
  const backdated = dbQuery(`UPDATE botgram_backend_v1.media_files SET created_at=clock_timestamp()-interval '2 days' ` +
    `WHERE owner_id='${aliceId}' AND client_file_id=${abandoned} RETURNING id`);
  if (backdated !== null) {
    let gone = false;
    for (let i = 0; i < 24 && !gone; i++) {
      await new Promise(resolve => setTimeout(resolve, 500));
      gone = dbQuery(`SELECT count(*) FROM botgram_backend_v1.media_files ` +
        `WHERE owner_id='${aliceId}' AND client_file_id=${abandoned}`) === '0';
    }
    assert.ok(gone, 'an abandoned upload is discarded by maintenance');
    ok();
  }

  console.log(`PASS: batch I media regression (${checks} checks, part upload/download, single and album messages)`);
} catch (error) {
  if (logs.trim()) console.error('BACKEND LOGS:\n' + logs.slice(-8000));
  throw error;
} finally {
  await stop();
}
