import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

// Offline source/registry consistency check, not a protobuf parser or runtime validator.
const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const args = process.argv.slice(2);
let androidRoot;
for (let i = 0; i < args.length; i++) {
  assert(['--android-root', '--AndroidRoot', '-AndroidRoot'].includes(args[i]), `Unknown argument: ${args[i]}`);
  assert(args[i + 1] && !args[i + 1].startsWith('-'), 'AndroidRoot requires a path');
  assert(!androidRoot, 'AndroidRoot specified twice');
  androidRoot = path.resolve(args[++i]);
}
const policy = JSON.parse(fs.readFileSync(path.join(root, 'Protocol/rpc-field-policy.json'), 'utf8'));
const protoBytes = fs.readFileSync(path.join(root, 'Protocol/botgram_rpc.proto'));
const proto = protoBytes.toString('utf8').replace(/\/\*[\s\S]*?\*\//g, '').replace(/\/\/[^\n]*/g, '');
assert.equal(policy.schemaVersion, 1);
assert.equal(policy.protocolMajor, 1);
assert.equal(policy.contract.serverDomain, 'nyarugram.moe.autos');
assert.equal(policy.contract.nonLocalDomainStatus, 501);
assert.equal(policy.contract.unknownOperationStatus, 501);
assert.equal(policy.contract.unknownMustUnderstandEntityStatus, 501);
assert.equal(policy.contract.missingRequiredOneofStatus, 400);
assert.match(proto, /syntax\s*=\s*"proto3"/);
assert.match(proto, /package\s+botgram\.rpc\.v1\s*;/);
assert(!/\b(?:Any|Struct|Value|ListValue)\b/.test(proto), 'No Any/Struct/JSON escape hatches allowed');
assert(!/\b(?:import|map)\b/.test(proto), 'v1 must remain a standalone explicitly typed schema');

const messages = new Map();
const oneofs = new Map();
const fieldPattern = /\b(?:(optional|repeated)\s+)?([\w.]+)\s+(\w+)\s*=\s*(\d+)\s*;/g;
for (const match of proto.matchAll(/\bmessage\s+(\w+)\s*\{/g)) {
  let end = match.index + match[0].length;
  const start = end;
  let depth = 1;
  while (depth && end < proto.length) {
    if (proto[end] === '{') depth++;
    if (proto[end] === '}') depth--;
    end++;
  }
  assert.equal(depth, 0, `Unclosed message ${match[1]}`);
  const body = proto.slice(start, end - 1);
  const fields = [...body.matchAll(fieldPattern)].map(m => ({ type: m[2], name: m[3], tag: Number(m[4]) }));
  assert(!messages.has(match[1]), `Duplicate message ${match[1]}`);
  assert.equal(new Set(fields.map(f => f.name)).size, fields.length, `Duplicate field in ${match[1]}`);
  assert.equal(new Set(fields.map(f => f.tag)).size, fields.length, `Duplicate tag in ${match[1]}`);
  messages.set(match[1], fields);
  for (const group of body.matchAll(/\boneof\s+(\w+)\s*\{([^{}]*)\}/g)) {
    oneofs.set(`${match[1]}.${group[1]}`, [...group[2].matchAll(fieldPattern)].map(m => ({ type: m[2], name: m[3], tag: Number(m[4]) })));
  }
}
const scalarTypes = new Set(['string', 'bool', 'bytes', 'int32', 'int64', 'uint32', 'uint64', 'sint32', 'sint64', 'fixed32', 'fixed64', 'sfixed32', 'sfixed64', 'float', 'double']);
const enumTypes = new Set([...proto.matchAll(/\benum\s+(\w+)\s*\{/g)].map(m => m[1]));
const binaryFields = new Set(['PasswordKdf.salt1', 'PasswordKdf.salt2', 'PasswordKdf.p',
  'SrpProof.a', 'SrpProof.m1', 'PasswordInfo.srp_b', 'PasswordInfo.new_secure_salt',
  'PasswordInfo.secure_random', 'PasswordUpdate.new_password_hash',
  'PhoneRequestCallRequest.g_a_hash', 'PhoneAcceptCallRequest.g_b', 'PhoneConfirmCallRequest.g_a',
  'PhoneSendSignalingDataRequest.data', 'PhoneCall.g_a_hash', 'PhoneCall.g_b', 'PhoneCall.g_a_or_b',
  'PhoneCallSignalingData.data', 'DhConfig.p', 'DhConfig.random',
  'AuthPasswordInfo.password_info',
  'E2eeEnvelope.ciphertext', 'E2eeOneTimePreKey.public_key',
  'E2eePublishDeviceRequest.identity_key', 'E2eePublishDeviceRequest.signed_pre_key',
  'E2eePublishDeviceRequest.signed_pre_key_signature', 'E2eePublishDeviceRequest.kyber_pre_key',
  'E2eePublishDeviceRequest.kyber_pre_key_signature',
  'E2eeDeviceInfo.identity_key', 'E2eeDeviceInfo.signed_pre_key',
  'E2eeDeviceInfo.signed_pre_key_signature', 'E2eeDeviceInfo.kyber_pre_key',
  'E2eeDeviceInfo.kyber_pre_key_signature',
  'E2eePeerBundle.pre_key', 'E2eePeerBundle.signed_pre_key',
  'E2eePeerBundle.signed_pre_key_signature', 'E2eePeerBundle.identity_key',
  'E2eePeerBundle.kyber_pre_key', 'E2eePeerBundle.kyber_pre_key_signature',
  'E2eeChatStatus.peer_identity_key', 'E2eeIdentityConfirmation.peer_identity_key']);
for (const [name, fields] of messages) {
  for (const field of fields) {
    assert(scalarTypes.has(field.type) || messages.has(field.type) || enumTypes.has(field.type), `Unknown type ${name}.${field.name}: ${field.type}`);
    if (field.type === 'bytes') assert(binaryFields.has(`${name}.${field.name}`), `Unregistered binary field ${name}.${field.name}`);
  }
}
function schemaField(target) {
  const [message, field, extra] = target.split('.');
  assert(!extra && messages.get(message)?.some(f => f.name === field), `Missing schema mapping ${target}`);
}
for (const name of policy.contract.requiredOneofs) assert(oneofs.has(name), `Missing required oneof ${name}`);
function checkFields(fields, context) {
  for (const [name, entry] of Object.entries(fields)) {
    const key = typeof entry === 'string' ? entry : entry.decision;
    const decision = policy.decisions[key];
    assert(decision && ['mapping', 'ignore', 'reject'].includes(decision.action), `Invalid decision ${context}.${name}`);
    assert(decision.reason && decision.when, `Unexplained decision ${context}.${name}`);
    if (decision.action === 'mapping') {
      assert(Array.isArray(entry.to) && entry.to.length, `Missing mapping ${context}.${name}`);
      entry.to.forEach(schemaField);
    }
    if (decision.action === 'reject') {
      assert.equal(decision.when, 'non-default');
      assert.equal(decision.httpStatus, 501);
    }
    if (decision.rejectOtherwise) {
      assert.equal(decision.action, 'ignore');
      assert.notEqual(decision.when, 'always');
      assert([403, 501].includes(decision.rejectOtherwise.httpStatus), `Invalid conditional rejection ${context}.${name}`);
      assert(decision.rejectOtherwise.code, `Missing conditional error ${context}.${name}`);
      assert.equal(entry.nested, 'InputPeer', `Conditional peer validation required ${context}.${name}`);
    }
    if (typeof entry === 'object' && entry.nested) assert(policy.nested[entry.nested], `Unknown nested policy ${entry.nested}`);
  }
}
checkFields(policy.inheritedFields, 'TLObject');
assert.equal(policy.requests.length, 51);
assert.equal(new Set(policy.requests.map(r => r.tl)).size, 51);
assert.equal(new Set(policy.requests.map(r => r.javaType)).size, 51);
assert.equal(policy.phoneOperations.length, 9);
assert.equal(new Set(policy.phoneOperations.map(r => r.tl)).size, 9);
assert.equal(new Set(policy.phoneOperations.map(r => r.javaType)).size, 9);
assert.equal(policy.transportOperations.length, 1);
const transport = policy.transportOperations[0];
assert.equal(transport.tag, 121);
assert.equal(transport.operation, 'resolve_identities');
assert.equal(transport.message, 'ResolveIdentitiesRequest');
assert.equal(transport.response, 'ResolvedIdentities');
assert(!('tl' in transport) && !('javaType' in transport), 'Transport bootstrap is not a TL class');
assert(transport.purpose && transport.validation && transport.identity, 'Transport policy must describe semantics');
for (const [key, message] of [['requestFields', transport.message], ['responseFields', transport.response]]) {
  transport[key].forEach(schemaField);
  assert.deepEqual([...transport[key]].sort(), messages.get(message).map(f => `${message}.${f.name}`).sort(), `Complete transport ${key}`);
}
assert.equal(policy.featureOperations.length, 8);
for (const feature of policy.featureOperations) {
  assert(feature.purpose && feature.validation, `Missing feature semantics: ${feature.operation}`);
  for (const [key, message] of [['requestFields', feature.message], ['responseFields', feature.response]]) {
    feature[key].forEach(schemaField);
    assert.deepEqual([...feature[key]].sort(), messages.get(message).map(f => `${message}.${f.name}`).sort(), `Complete feature ${feature.operation}.${key}`);
  }
}
assert.equal(policy.e2eeOperations.length, 5);
for (const operation of policy.e2eeOperations) {
  assert(operation.purpose && operation.validation, `Missing E2EE semantics: ${operation.operation}`);
  for (const [key, message] of [['requestFields', operation.message], ['responseFields', operation.response]]) {
    operation[key].forEach(schemaField);
    assert.deepEqual([...operation[key]].sort(), messages.get(message).map(f => `${message}.${f.name}`).sort(), `Complete E2EE ${operation.operation}.${key}`);
  }
}
assert.equal(policy.authOperations.length, 10);
for (const operation of policy.authOperations) {
  assert(operation.purpose && operation.validation, `Missing auth semantics: ${operation.operation}`);
  for (const [key, message] of [['requestFields', operation.message], ['responseFields', operation.response]]) {
    operation[key].forEach(schemaField);
    assert.deepEqual([...operation[key]].sort(), messages.get(message).map(f => `${message}.${f.name}`).sort(), `Complete auth ${operation.operation}.${key}`);
  }
}
const registeredOperations = [...policy.requests, ...policy.transportOperations, ...policy.featureOperations,
  ...policy.e2eeOperations, ...policy.phoneOperations];
const orderedOperations = [...registeredOperations].sort((a, b) => a.tag - b.tag);
assert.equal(new Set(registeredOperations.map(r => r.operation)).size, 74);
assert.equal(new Set(registeredOperations.map(r => r.tag)).size, 74);
for (const [i, request] of orderedOperations.entries()) {
  assert.equal(request.tag, 100 + i, `Tag gap before ${request.tl ?? request.operation}`);
}
const orderedAuth = [...policy.authOperations].sort((a, b) => a.tag - b.tag);
for (const [i, request] of orderedAuth.entries()) {
  assert.equal(request.tag, 200 + i, `Auth tag gap before ${request.operation}`);
}
for (const envelope of ['RpcRequest.operation', 'RpcResponse.outcome']) {
  const operations = oneofs.get(envelope).filter(f => f.tag >= 100);
  assert.equal(operations.length, 84, `${envelope} requires 21 original + bootstrap + 8 feature + 5 E2EE + 9 phone/DH + 26 message-lifecycle + 4 media operations + 10 auth operations`);
  for (const request of [...registeredOperations, ...policy.authOperations]) {
    const operation = operations.find(f => f.tag === request.tag);
    assert.equal(operation?.name, request.operation, `Tag drift: ${request.tl ?? request.operation}`);
    if (envelope.startsWith('RpcRequest')) assert.equal(operation.type, request.message);
    else if (request.response) assert.equal(operation.type, request.response);
  }
}
for (const request of [...policy.phoneOperations, ...policy.requests]) {
  checkFields(request.fields, request.tl);
  for (const target of Object.keys(request.wireDefaults ?? {})) schemaField(target);
}
for (const [name, group] of Object.entries(policy.nested)) {
  assert.equal(group.unknownSubclassStatus, 501, name);
  checkFields(group.fields, name);
  for (const [subclass, variant] of Object.entries(group.variants)) {
    checkFields(variant.fields, subclass);
    if (variant.select) schemaField(variant.select);
    if (variant.rejectStatus) assert.equal(variant.rejectStatus, 501, subclass);
    if (variant.select?.endsWith('.self')) assert.equal(variant.value, true, subclass);
  }
}
console.log(`PASS: v1 typed schema, 21 original + bootstrap + wallpaper + privacy + addContact + 9 phone/DH + 26 message-lifecycle + 4 media operations, mapping targets and policy decisions (${messages.size} messages)`);

if (androidRoot) {
  const clientProto = fs.readFileSync(path.join(androidRoot, 'Shared/botgram_rpc.proto'));
  assert(protoBytes.equals(clientProto), 'Client Shared/botgram_rpc.proto differs byte-for-byte from backend protocol');
  const inventory = JSON.parse(fs.readFileSync(path.join(androidRoot, 'Botgram-RPC-Inventory.json'), 'utf8'));
  function coverage(actual, registered, context) {
    assert(actual, `Missing inventory fields: ${context}`);
    const expected = [...new Set(actual.map(f => f.name))].sort();
    assert.deepEqual(Object.keys(registered).sort(), expected, `Field coverage mismatch: ${context}`);
  }
  for (const request of policy.requests) {
    assert.equal(inventory.suggestedProtoOneofTags[request.tl], request.tag, `Inventory tag mismatch ${request.tl}`);
    const source = inventory.rpcInventory.find(r => r.javaType === request.javaType);
    assert(source, `Missing RPC inventory ${request.javaType}`);
    assert.equal(inventory.backendAliases[source.name] ?? source.name, request.tl);
    coverage(source.requestFieldsIncludingInherited, { ...policy.inheritedFields, ...request.fields }, request.tl);
  }
  for (const [name, group] of Object.entries(policy.nested)) {
    const base = inventory.types[group.javaType];
    assert(base, `Missing nested inventory ${group.javaType}`);
    coverage(base.publicFieldsIncludingInherited, { ...policy.inheritedFields, ...group.fields }, name);
    const descendants = new Set();
    function collect(parent) {
      for (const [javaType, type] of Object.entries(inventory.types)) {
        if (type.baseClass === parent && !descendants.has(javaType)) {
          descendants.add(javaType);
          collect(javaType);
        }
      }
    }
    collect(group.javaType);
    assert.deepEqual(Object.keys(group.variants).sort(), [...descendants].map(k => k.split('$').at(-1)).sort(), `Subclass coverage mismatch: ${name}`);
    for (const javaType of descendants) {
      const variant = group.variants[javaType.split('$').at(-1)];
      coverage(inventory.types[javaType].publicFieldsIncludingInherited, { ...policy.inheritedFields, ...group.fields, ...variant.fields }, javaType);
    }
  }
  console.log('PASS: Android inventory complete inherited field/subclass coverage; 51 inventory tags; client/backend proto byte equality');
  console.log('NOT RUN: wallpaper Java field/subclass inventory coverage (requires the updated client feature inventory)');
} else {
  console.log('SKIP: Android inventory and client proto comparison (optional --android-root PATH; no Android build dependency)');
}
