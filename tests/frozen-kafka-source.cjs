'use strict';
const { test } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const crypto = require('node:crypto');
const path = require('node:path');
const read = file => fs.readFileSync(path.join(__dirname, '..', 'Storage.Tests', file), 'utf8').replace(/\r\n/g, '\n');
const source = read('FrozenKafkaDeliveryV1.cs');
const sha256 = value => crypto.createHash('sha256').update(value).digest('hex');
function section(name, input = source) {
  const start = `// BEGIN FROZEN ${name}\n`, end = `// END FROZEN ${name}`;
  assert.equal(input.split(start).length, 2);
  assert.equal(input.split(end).length, 2);
  return input.slice(input.indexOf(start) + start.length, input.indexOf(end));
}
test('frozen consumer and publisher differ from archived source only by namespace', () => {
  const old = section('KAFKA SOURCE').replace(
    'namespace Botgram.Backend.Storage.FrozenV1;', 'namespace Botgram.Backend.Storage;');
  assert.equal(sha256(old), '41a476b797ef03c3ed9835898656ed840ff820a2c55fad0cbebb60f298b12c22');
});
test('frozen event payload resolver matches the archived resolver', () => {
  assert.equal(sha256(section('PAYLOAD TYPES')), '3cb85955f41ab896fae19b1fbd67154b67612baaac52064f93960c556aa0a50c');
});
test('frozen outbox source query and claim match archived methods', () => {
  const outbox = read('FrozenOutboxV1.cs');
  assert.equal(sha256(section('OUTBOX SOURCE', outbox)), '49e3dda749f142f71079d1d46b1cb3aed013f89062daa145357749ef13a10fc0');
  assert.equal(sha256(section('OUTBOX CLAIM', outbox)), '196afe3bc79e005bbd27d046c1163524d9fb890b3b4eb698a05bb71a834b30d3');
});
