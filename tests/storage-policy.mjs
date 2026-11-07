import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { access, mkdir, mkdtemp } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const dll = path.resolve(process.env.BOTGRAM_TEST_DLL ?? path.join(root, 'bin/Debug/net8.0/Botgram.Backend.dll'));
await access(dll);
const testRoot = process.env.BOTGRAM_TEST_ROOT ?? 'D:/BotgramBuild/tmp';
await mkdir(testRoot, { recursive: true });
const temp = await mkdtemp(path.join(testRoot, 'storage-policy-'));
let assertions = 0;
const cases = ['NewStoragePrimary', 'NewStorageShadow', 'NewStorageValidation', '1', 'legacyonly', 'Bogus', ''].map(mode => ({mode, extra: {}}));
cases.push(
  {mode: 'LegacyOnly', extra: {Storage__ValidationBackend: 'FourComponent'}},
  {mode: 'NewStorageValidation', extra: {Storage__ValidationBackend: 'FourComponent',
    Protocol__LegacyJsonRpcEnabled: 'false', Protocol__LegacyJsonWssEnabled: 'false'}},
  {mode: 'NewStorageValidation', extra: {Storage__ValidationBackend: 'FourComponent', Storage__FullStackAcknowledged: 'true',
    Protocol__LegacyJsonRpcEnabled: 'false', Protocol__LegacyJsonWssEnabled: 'false'}},
  {mode: 'LegacyOnly', extra: {Storage__ValidationBackend: 'RelationalThreeRpc'}},
  {mode: 'LegacyOnly', extra: {Storage__ValidationBackend: 'RelationalBackend'}},
  {mode: 'NewStorageValidation', extra: {Storage__ValidationBackend: 'RelationalBackend', ASPNETCORE_ENVIRONMENT: 'Development',
    Storage__ValidationAcknowledged: 'true', Storage__ValidationConnectionString: 'Host=127.0.0.1;Database=botgram_test_denied',
    Protocol__LegacyJsonRpcEnabled: 'true', Protocol__LegacyJsonWssEnabled: 'false'}},
  {mode: 'NewStorageValidation', extra: {Storage__ValidationBackend: 'RelationalBackend', ASPNETCORE_ENVIRONMENT: 'Development',
    Storage__ValidationAcknowledged: 'true', Storage__ValidationConnectionString: 'Host=127.0.0.1;Database=botgram_test_denied',
    Protocol__LegacyJsonRpcEnabled: 'false', Protocol__LegacyJsonWssEnabled: 'true'}},
  {mode: 'NewStorageValidation', extra: {Storage__ValidationBackend: 'Unknown'}},
  {mode: 'NewStorageValidation', extra: {Storage__ValidationBackend: 'RelationalThreeRpc', ASPNETCORE_ENVIRONMENT: 'Production', Storage__ValidationAcknowledged: 'true', Storage__ValidationConnectionString: 'Host=127.0.0.1;Database=botgram_test_denied'}},
  {mode: 'NewStorageValidation', extra: {ASPNETCORE_ENVIRONMENT: 'Development'}},
  {mode: 'NewStorageValidation', extra: {ASPNETCORE_ENVIRONMENT: 'Development', Storage__ValidationAcknowledged: 'true', Storage__ValidationConnectionString: 'Host=example.invalid;Database=botgram_test_denied'}},
  {mode: 'NewStorageValidation', extra: {ASPNETCORE_ENVIRONMENT: 'Development', Storage__ValidationAcknowledged: 'true', Storage__ValidationConnectionString: 'Host=127.0.0.1;Database=production'}},
  {mode: 'NewStorageValidation', extra: {ASPNETCORE_ENVIRONMENT: 'Production', Storage__ValidationAcknowledged: 'true', Storage__ValidationConnectionString: 'Host=127.0.0.1;Database=botgram_test_denied'}}
);
for (const [index, {mode, extra}] of cases.entries()) {
  const data = path.join(temp, String(index), 'db.json');
  const child = spawn('dotnet', [dll], { cwd: root, windowsHide: true,
    env: { ...process.env, Storage__Mode: mode, DataPath: data, ASPNETCORE_URLS: 'http://127.0.0.1:0', ...extra },
    stdio: ['ignore', 'pipe', 'pipe'] });
  let output = '';
  child.stdout.on('data', b => output += b);
  child.stderr.on('data', b => output += b);
  const code = await new Promise((resolve, reject) => {
    const timer = setTimeout(() => { child.kill(); reject(new Error('startup did not fail promptly')); }, 15000);
    child.once('error', error => { clearTimeout(timer); reject(error); });
    child.once('exit', code => { clearTimeout(timer); resolve(code); });
  });
  assert.notEqual(code, 0, `reject ${mode}`); assertions++;
  assert.match(output, /Storage:Mode|Storage:Validation|Storage:FullStack|FourComponent|RelationalThreeRpc is restricted|RelationalBackend|Relational backends|NewStoragePrimary|PrimaryAcknowledged|Privacy:KeyDirectory|cutover is not authorized|no verified implementation/); assertions++;
  await assert.rejects(access(path.dirname(data)), { code: 'ENOENT' }); assertions++;
}
console.log(`PASS: ${assertions} storage startup boundary assertions; no legacy data directories created; ${temp}`);
