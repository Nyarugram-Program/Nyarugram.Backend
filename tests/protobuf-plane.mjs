// Shared raw-protobuf helpers for the Protobuf-only HTTP planes. No third-party
// runtime; field numbers are the permanent allocations in botgram_rpc.proto.
import assert from 'node:assert/strict';

export function varint(value) {
  let v = BigInt(value);
  if (v < 0n) v = (1n << 64n) + v;
  const out = [];
  while (v >= 0x80n) { out.push(Number(v & 0x7fn) | 0x80); v >>= 7n; }
  out.push(Number(v));
  return out;
}
export function tag(field, wire) { return varint((field << 3) | wire); }
export function vfield(field, value) { return [...tag(field, 0), ...varint(value)]; }
export function sfield(field, value) {
  const bytes = Buffer.from(value ?? '', 'utf8');
  return [...tag(field, 2), ...varint(bytes.length), ...bytes];
}
export function bfield(field, bytes) {
  const buffer = Buffer.from(bytes ?? []);
  return [...tag(field, 2), ...varint(buffer.length), ...buffer];
}
export function fixed64(field, value) {
  const buffer = Buffer.alloc(8);
  buffer.writeBigInt64LE(BigInt(value));
  return [...tag(field, 1), ...buffer];
}
export function concat(...parts) { return Uint8Array.from(parts.flat()); }

export function fields(buffer) {
  const result = [];
  const bytes = Buffer.from(buffer);
  let index = 0;
  while (index < bytes.length) {
    let key = 0n, shift = 0n;
    for (;;) {
      const byte = bytes[index++];
      key |= BigInt(byte & 0x7f) << shift; shift += 7n;
      if (!(byte & 0x80)) break;
    }
    const number = Number(key >> 3n), wire = Number(key & 7n);
    if (wire === 0) {
      let value = 0n; shift = 0n;
      for (;;) {
        const byte = bytes[index++];
        value |= BigInt(byte & 0x7f) << shift; shift += 7n;
        if (!(byte & 0x80)) break;
      }
      result.push({ number, wire, value });
    } else if (wire === 2) {
      let length = 0n; shift = 0n;
      for (;;) {
        const byte = bytes[index++];
        length |= BigInt(byte & 0x7f) << shift; shift += 7n;
        if (!(byte & 0x80)) break;
      }
      result.push({ number, wire, bytes: bytes.subarray(index, index + Number(length)) });
      index += Number(length);
    } else if (wire === 1) {
      result.push({ number, wire, bytes: bytes.subarray(index, index + 8) });
      index += 8;
    } else if (wire === 5) {
      result.push({ number, wire, bytes: bytes.subarray(index, index + 4) });
      index += 4;
    } else throw new Error(`Unsupported wire type ${wire}`);
  }
  return result;
}
export function stringOf(entry) { return Buffer.from(entry.bytes).toString('utf8'); }
// Accept either raw bytes or an already parsed field list.
export function entries(buffer) { return Array.isArray(buffer) ? buffer : fields(buffer); }
export function first(buffer, number) { return entries(buffer).find(f => f.number === number); }
export function all(buffer, number) { return entries(buffer).filter(f => f.number === number); }

let sequence = 0;
export function requestId() { return 'pb-' + process.pid.toString(16) + '-' + (++sequence).toString(16); }

export function envelope(operationTag, payload = []) {
  return concat(vfield(1, 1), sfield(2, requestId()), bfield(operationTag, payload));
}

export async function call(base, path, body, token = null) {
  const response = await fetch(base + path, {
    method: 'POST',
    headers: {
      'content-type': 'application/x-protobuf',
      'accept': 'application/x-protobuf',
      ...(token ? { authorization: `Bearer ${token}` } : {})
    },
    body: Buffer.from(body)
  });
  const bytes = Buffer.from(await response.arrayBuffer());
  return { status: response.status, headers: response.headers, bytes,
    fields: bytes.length && (response.headers.get('content-type') ?? '').startsWith('application/x-protobuf')
      ? fields(bytes) : [] };
}

export function expectError(result, status, code) {
  assert.equal(result.status, status, `HTTP status for ${code}`);
  const error = first(result.bytes, 10);
  assert.ok(error, 'RpcError must be present');
  const codeField = first(error.bytes, 1);
  assert.equal(stringOf(codeField), code);
  const httpStatus = first(error.bytes, 5);
  assert.equal(Number(httpStatus.value), status);
}

export function outcome(result, tag) {
  assert.equal(result.status, 200, `HTTP 200 expected for outcome ${tag}`);
  const entry = first(result.bytes, tag);
  assert.ok(entry, `outcome field ${tag} must be present`);
  return entry.bytes;
}

export const Operation = {
  capabilities: 200, register: 201, login: 202, password: 203, recover: 204,
  logout: 205, me: 206, premium: 207, changeGid: 208, totp: 209
};
export const AUTH = '/api/v1/auth';
export const ACCOUNT = '/api/v1/account';

export function credentials(username, password, displayName, locationCountry) {
  return concat(sfield(1, username), sfield(2, password), sfield(3, displayName),
    locationCountry === undefined ? [] : sfield(4, locationCountry));
}

export async function register(base, username, password, displayName, locationCountry) {
  const result = await call(base, AUTH, envelope(Operation.register,
    credentials(username, password, displayName, locationCountry)));
  const session = outcome(result, Operation.register);
  return { token: stringOf(first(session, 1)), user: first(session, 3).bytes, result };
}

export async function login(base, username, password, options = {}) {
  const result = await call(base, AUTH, envelope(Operation.login, concat(
    sfield(1, username), sfield(2, password),
    options.totpCode === undefined ? [] : sfield(3, options.totpCode),
    options.passwordProof === undefined ? [] : sfield(4, options.passwordProof))));
  const session = outcome(result, Operation.login);
  return { token: stringOf(first(session, 1)), user: first(session, 3).bytes, result };
}

export async function capabilities(base) {
  return call(base, AUTH, envelope(Operation.capabilities));
}

export async function logout(base, token) {
  return call(base, ACCOUNT, envelope(Operation.logout), token);
}

export async function premium(base, token, activate = false) {
  return call(base, ACCOUNT, envelope(Operation.premium, vfield(1, activate ? 1 : 0)), token);
}

export async function me(base, token) {
  return call(base, ACCOUNT, envelope(Operation.me), token);
}

export async function totp(base, token, operation, code = '') {
  return call(base, ACCOUNT, envelope(Operation.totp,
    concat(sfield(1, operation), sfield(2, code))), token);
}

export async function changeGid(base, token, regionCode, number) {
  return call(base, ACCOUNT, envelope(Operation.changeGid,
    concat(sfield(1, regionCode), sfield(2, number))), token);
}

export function profile(userBytes) {
  const str = number => { const entry = first(userBytes, number); return entry ? stringOf(entry) : ''; };
  const num = number => { const entry = first(userBytes, number); return entry ? Number(entry.value) : 0; };
  const premiumField = first(userBytes, 8);
  return {
    userId: str(1), username: str(2), displayName: str(3), gid: str(4),
    tlUserId: num(5), firstName: str(6), lastName: str(7),
    premium: premiumField ? Number(premiumField.value) !== 0 : null,
    self: num(9) !== 0, contact: num(10) !== 0, mutualContact: num(11) !== 0,
    online: num(12) !== 0, lastSeenMs: num(13)
  };
}
