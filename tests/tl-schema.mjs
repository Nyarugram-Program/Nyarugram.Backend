// Check response class names against this exact Android checkout, without compiling Android.
import { readFile } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import assert from 'node:assert/strict';
const backend = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const root = path.resolve(process.env.BOTGRAM_ANDROID_ROOT ?? path.join(backend, '../Botgram'));
const gateway = await readFile(path.join(backend, 'TelegramGateway.cs'), 'utf8');
const java = path.join(root, 'TMessagesProj/src/main/java/org/telegram/tgnet');
const tlrpc = await readFile(path.join(java, 'TLRPC.java'), 'utf8');
const updates = await readFile(path.join(java, 'tl/TL_update.java'), 'utf8');
const phone = await readFile(path.join(java, 'tl/TL_phone.java'), 'utf8');
// A quoted prefix followed by concatenation is not a complete response class.
const types = new Set([...gateway.matchAll(/T\("([A-Za-z_$]+)"\s*[,)]/g)].map(m => m[1]));
for (const match of gateway.matchAll(/"(TL_messageEntity[A-Za-z]+)"/g)) types.add(match[1]);
for (const type of types) {
  if (type === 'Vector') continue;
  const source = type.startsWith('TL_phone$') ? phone
    : type.startsWith('TL_update') && !type.startsWith('TL_updates') ? updates : tlrpc;
  const name = type.split('$').at(-1);
  assert(new RegExp(`class ${name}\\b`).test(source), `Missing response type ${type}`);
}
console.log(`PASS: ${types.size} gateway response types match the Android checkout`);
