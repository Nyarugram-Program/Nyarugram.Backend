// Check response class names against this exact Android checkout, without compiling Android.
import { readFile } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import assert from 'node:assert/strict';
const backend = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const root = path.resolve(process.env.BOTGRAM_ANDROID_ROOT ?? path.join(backend, '../Botgram'));
const gateway = await readFile(path.join(backend, 'TelegramGateway.cs'), 'utf8');
const tlrpc = await readFile(path.join(root, 'TMessagesProj/src/main/java/org/telegram/tgnet/TLRPC.java'), 'utf8');
const updates = await readFile(path.join(root, 'TMessagesProj/src/main/java/org/telegram/tgnet/tl/TL_update.java'), 'utf8');
const types = new Set([...gateway.matchAll(/T\("([A-Za-z_]+)"/g)].map(m => m[1]));
for (const match of gateway.matchAll(/"(TL_messageEntity[A-Za-z]+)"/g)) types.add(match[1]);
for (const type of types) {
  if (type === 'Vector') continue;
  const source = ['TL_updateMessageID', 'TL_updateNewMessage', 'TL_updateReadHistoryInbox', 'TL_updateReadHistoryOutbox', 'TL_updateUserPhone'].includes(type) ? updates : tlrpc;
  assert(new RegExp(`class ${type}\\b`).test(source), `Missing response type ${type}`);
}
console.log(`PASS: ${types.size} gateway response types match the Android checkout`);
