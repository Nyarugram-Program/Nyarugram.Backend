'use strict';
const fs = require('node:fs');
const crypto = require('node:crypto');
const digest = value => crypto.createHash('sha256').update(value).digest('hex');
const hashJson = value => digest(JSON.stringify(value));
const compare = (a, b) => a < b ? -1 : a > b ? 1 : 0;

function lex(source) {
  const tokens = [];
  for (let i = 0; i < source.length;) {
    const c = source[i];
    if (/\s/.test(c)) { i++; continue; }
    if (c === '/' && source[i + 1] === '/') {
      i += 2;
      while (i < source.length && source[i] !== '\n' && source[i] !== '\r') i++;
      continue;
    }
    if (c === '/' && source[i + 1] === '*') {
      const end = source.indexOf('*/', i + 2);
      if (end < 0) throw new Error('Unterminated block comment');
      i = end + 2;
      continue;
    }
    if (c === '"' || c === "'") {
      const start = i++;
      let closed = false;
      while (i < source.length) {
        if (source[i] === '\\') {
          if (i + 1 >= source.length) throw new Error('Unterminated string escape');
          i += 2;
        } else if (source[i++] === c) { closed = true; break; }
      }
      if (!closed) throw new Error('Unterminated string');
      tokens.push(source.slice(start, i));
      continue;
    }
    const word = source.slice(i).match(/^(?:[A-Za-z_][A-Za-z_0-9]*|0[xX][0-9A-Fa-f]+|(?:[0-9]+\.[0-9]*|\.[0-9]+|[0-9]+)(?:[eE][+-]?[0-9]+)?)/);
    if (word) { tokens.push(word[0]); i += word[0].length; continue; }
    tokens.push(c);
    i++;
  }
  return tokens;
}

function declarationSegments(tokens) {
  const segments = [];
  for (let i = 0; i < tokens.length;) {
    const start = i, kind = tokens[i++];
    let id = kind;
    if (['message', 'enum', 'service'].includes(kind)) {
      id += `:${tokens[i++]}`;
      if (tokens[i++] !== '{') throw new Error(`Missing declaration body for ${id}`);
      let depth = 1;
      while (i < tokens.length && depth) {
        if (tokens[i] === '{') depth++;
        if (tokens[i] === '}') depth--;
        i++;
      }
      if (depth) throw new Error(`Unterminated declaration ${id}`);
    } else {
      while (i < tokens.length && tokens[i] !== ';') i++;
      if (i === tokens.length) throw new Error(`Unterminated declaration ${id}`);
      if (kind === 'option') {
        const equals = tokens.indexOf('=', start + 1);
        if (equals < 0 || equals >= i) throw new Error('Malformed top-level option');
        id += `:${tokens.slice(start + 1, equals).join('')}`;
      }
      if (kind === 'import') id += `:${tokens.slice(start + 1, i).join('')}`;
      i++;
    }
    segments.push([id, hashJson(tokens.slice(start, i))]);
  }
  if (new Set(segments.map(item => item[0])).size !== segments.length) throw new Error('Duplicate top-level declaration');
  return segments;
}

function fingerprint(source) {
  const tokens = lex(source);
  const segments = declarationSegments(tokens);
  let i = 0;
  const messages = Object.create(null), enums = Object.create(null);
  const header = { syntax: null, package: null, imports: [], options: [] };
  const fail = message => { throw new Error(`${message} at token ${i}: ${tokens[i] ?? '<end>'}`); };
  const take = expected => {
    const value = tokens[i++];
    if (value === undefined || expected !== undefined && value !== expected) fail(`Expected ${expected ?? 'token'}`);
    return value;
  };
  const name = () => {
    const value = take();
    if (!/^[A-Za-z_][A-Za-z_0-9]*$/.test(value)) fail('Expected identifier');
    return value;
  };
  const statement = () => {
    const result = [];
    while (i < tokens.length && tokens[i] !== ';') {
      if (tokens[i] === '{' || tokens[i] === '}') fail('Unexpected block in statement');
      result.push(take());
    }
    take(';');
    return result;
  };
  const assignment = parts => {
    const at = parts.indexOf('=');
    if (at < 1 || at === parts.length - 1) fail('Malformed assignment');
    return [parts.slice(0, at).join(''), parts.slice(at + 1).join('')];
  };
  function field(parts, oneof) {
    const at = parts.indexOf('=');
    if (at < 2 || at + 1 >= parts.length) fail('Malformed field');
    let label = 'implicit';
    const left = parts.slice(0, at);
    if (['optional', 'repeated', 'required'].includes(left[0])) label = left.shift();
    if (left.length < 2) fail('Missing field type or name');
    const fieldName = left.pop();
    if (!/^[A-Za-z_][A-Za-z_0-9]*$/.test(fieldName)) fail('Invalid field name');
    const tag = Number(parts[at + 1]);
    if (!Number.isSafeInteger(tag) || tag <= 0 || tag > 536870911) fail('Invalid field tag');
    return { name: fieldName, tag, type: left.join(''), label, oneof, options: parts.slice(at + 2).join('') };
  }
  function parseEnum(prefix) {
    take('enum');
    const enumName = [...prefix, name()].join('.');
    if (Object.hasOwn(enums, enumName)) fail('Duplicate enum');
    const result = { values: [], options: [], reserved: [] };
    take('{');
    while (tokens[i] !== '}') {
      if (i >= tokens.length) fail('Unterminated enum');
      const parts = statement();
      if (!parts.length) continue;
      if (parts[0] === 'option') { result.options.push(parts.slice(1).join('')); continue; }
      if (parts[0] === 'reserved') { result.reserved.push(parts.slice(1).join('')); continue; }
      if (!/^[A-Za-z_][A-Za-z_0-9]*$/.test(parts[0]) || parts[1] !== '=') fail('Malformed enum value');
      const negative = parts[2] === '-';
      const number = Number(parts[negative ? 3 : 2]) * (negative ? -1 : 1);
      if (!Number.isSafeInteger(number)) fail('Invalid enum value');
      result.values.push({ name: parts[0], number, options: parts.slice(negative ? 4 : 3).join('') });
    }
    take('}');
    result.options.sort(); result.reserved.sort();
    enums[enumName] = result;
  }
  function parseMessage(prefix) {
    take('message');
    const components = [...prefix, name()];
    const messageName = components.join('.');
    if (Object.hasOwn(messages, messageName)) fail('Duplicate message');
    const result = { fields: [], oneofs: [], options: [], reserved: [], extensions: [] };
    take('{');
    while (tokens[i] !== '}') {
      if (i >= tokens.length) fail('Unterminated message');
      if (tokens[i] === 'message') { parseMessage(components); continue; }
      if (tokens[i] === 'enum') { parseEnum(components); continue; }
      if (tokens[i] === 'oneof') {
        take('oneof');
        const group = name();
        if (result.oneofs.some(item => item.name === group)) fail('Duplicate oneof');
        const item = { name: group, options: [] };
        result.oneofs.push(item);
        take('{');
        while (tokens[i] !== '}') {
          if (i >= tokens.length) fail('Unterminated oneof');
          const parts = statement();
          if (!parts.length) continue;
          if (parts[0] === 'option') item.options.push(parts.slice(1).join(''));
          else result.fields.push(field(parts, group));
        }
        take('}');
        item.options.sort();
        continue;
      }
      const parts = statement();
      if (!parts.length) continue;
      if (parts[0] === 'option') result.options.push(parts.slice(1).join(''));
      else if (parts[0] === 'reserved') result.reserved.push(parts.slice(1).join(''));
      else if (parts[0] === 'extensions') result.extensions.push(parts.slice(1).join(''));
      else result.fields.push(field(parts, null));
    }
    take('}');
    if (new Set(result.fields.map(item => item.name)).size !== result.fields.length ||
        new Set(result.fields.map(item => item.tag)).size !== result.fields.length) fail('Duplicate field name or tag');
    result.fields.sort((a, b) => a.tag - b.tag || compare(a.name, b.name));
    result.oneofs.sort((a, b) => compare(a.name, b.name));
    result.options.sort(); result.reserved.sort(); result.extensions.sort();
    messages[messageName] = result;
  }
  while (i < tokens.length) {
    if (tokens[i] === 'message') { parseMessage([]); continue; }
    if (tokens[i] === 'enum') { parseEnum([]); continue; }
    const parts = statement();
    if (!parts.length) continue;
    if (parts[0] === 'syntax') header.syntax = assignment(parts)[1];
    else if (parts[0] === 'package') header.package = parts.slice(1).join('');
    else if (parts[0] === 'import') header.imports.push(parts.slice(1).join(''));
    else if (parts[0] === 'option') header.options.push(parts.slice(1).join(''));
    else fail('Unsupported top-level declaration');
  }
  header.imports.sort(); header.options.sort();
  const sorted = object => Object.fromEntries(Object.entries(object).sort(([a], [b]) => compare(a, b)));
  const contract = { header, messages: sorted(messages), enums: sorted(enums) };
  const records = [];
  if (header.syntax !== null) records.push(`syntax\t${header.syntax}`);
  if (header.package !== null) records.push(`package\t${header.package}`);
  for (const option of header.options) {
    const equals = option.indexOf('=');
    if (equals < 1) fail('Malformed top-level option');
    records.push(`option\t${option.slice(0, equals)}\t${option.slice(equals + 1)}`);
  }
  for (const value of header.imports) records.push(`import\t${value}`);
  const messageRecords = Object.create(null);
  for (const [messageName, message] of Object.entries(contract.messages)) {
    const entries = [`message\t${messageName}`];
    for (const oneof of message.oneofs) {
      entries.push(`oneof\t${messageName}\t${oneof.name}`);
      for (const value of oneof.options) entries.push(`oneof-option\t${messageName}\t${oneof.name}\t${value}`);
    }
    for (const item of message.fields) {
      entries.push(`field\t${messageName}\t${item.tag}\t${item.name}\t${item.type}\t${item.label === 'implicit' ? 'singular' : item.label}\t${item.oneof ?? '-'}\t${item.options || '-'}`);
    }
    for (const value of message.options) entries.push(`message-option\t${messageName}\t${value}`);
    for (const value of message.reserved) entries.push(`message-reserved\t${messageName}\t${value}`);
    for (const value of message.extensions) entries.push(`extensions\t${messageName}\t${value}`);
    messageRecords[messageName] = entries.sort();
    records.push(...entries);
  }
  for (const [enumName, item] of Object.entries(contract.enums)) {
    records.push(`enum\t${enumName}`);
    for (const value of item.values) records.push(`enum-value\t${enumName}\t${value.name}\t${value.number}\t${value.options || '-'}`);
    for (const value of item.options) records.push(`enum-option\t${enumName}\t${value}`);
    for (const value of item.reserved) records.push(`enum-reserved\t${enumName}\t${value}`);
  }
  records.sort();
  const recordHash = items => digest(items.join('\n') + '\n');
  return {
    format: 'botgram-proto-fingerprint-v1',
    rawSha256: digest(source),
    tokenSha256: hashJson(tokens),
    tokenCount: tokens.length,
    declarationOrder: segments.map(item => item[0]),
    unorderedDeclarationsSha256: hashJson([...segments].sort(([a], [b]) => compare(a, b))),
    declarationSha256: Object.fromEntries(segments),
    contractSha256: hashJson(contract),
    comparisonSha256: recordHash(records),
    comparisonRecordCount: records.length,
    header,
    messageCount: Object.keys(messages).length,
    enumCount: Object.keys(enums).length,
    messageSha256: Object.fromEntries(Object.entries(contract.messages).map(([key, value]) => [key, hashJson(value)])),
    comparisonMessageSha256: Object.fromEntries(Object.entries(messageRecords).map(([key, value]) => [key, recordHash(value)])),
    enums: contract.enums,
    contract,
    comparisonRecords: records
  };
}

module.exports = { lex, declarationSegments, fingerprint };
if (require.main === module || process.argv[1] === '-') {
  if (!process.argv[2]) throw new Error('Usage: node proto-fingerprint.cjs path [--detail]');
  const result = fingerprint(fs.readFileSync(process.argv[2], 'utf8'));
  if (!process.argv.includes('--detail')) { delete result.contract; delete result.comparisonRecords; }
  console.log(JSON.stringify(result, null, 2));
}
