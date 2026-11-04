'use strict';
const { test } = require('node:test');
const assert = require('node:assert/strict');
const path = require('node:path');
const { fingerprint, lex } = require(path.join(__dirname, '..', 'tools', 'proto-fingerprint.cjs'));

const source = `syntax = "proto3";
package demo.v1;
option java_package = "demo//literal/*kept*/";
message Outer {
  optional string name = 1;
  oneof choice { int64 number = 2; string text = 3; }
  message Inner { repeated bool values = 1; }
  enum Mode { ZERO = 0; NEGATIVE = -1; }
}`;

test('comments and whitespace do not change token or contract hashes', () => {
  const altered = '/* start */\n' + source.replace('optional string name = 1;', 'optional  string /* comment */ name=1; // line');
  const original = fingerprint(source), compared = fingerprint(altered);
  assert.notEqual(original.rawSha256, compared.rawSha256);
  assert.equal(original.tokenSha256, compared.tokenSha256);
  assert.equal(original.contractSha256, compared.contractSha256);
  assert.equal(original.comparisonSha256, compared.comparisonSha256);
  assert.equal(original.header.options[0], 'java_package="demo//literal/*kept*/"');
  assert.deepEqual(original.contract.messages.Outer.fields.map(f => [f.name, f.label, f.oneof]), [
    ['name', 'optional', null], ['number', 'implicit', 'choice'], ['text', 'implicit', 'choice']
  ]);
  assert.equal(original.contract.enums['Outer.Mode'].values[1].number, -1);
  assert.equal(original.contract.messages['Outer.Inner'].fields[0].label, 'repeated');
  assert.ok(original.comparisonRecords.includes('field\tOuter\t1\tname\tstring\toptional\t-\t-'));
  assert.ok(original.comparisonRecords.includes('field\tOuter\t2\tnumber\tint64\tsingular\tchoice\t-'));
  assert.ok(original.comparisonRecords.includes('enum-value\tOuter.Mode\tNEGATIVE\t-1\t-'));
});

test('literal content, tag, type, presence, oneof and option changes are detected', () => {
  const original = fingerprint(source);
  for (const [before, after] of [
    ['demo//literal/*kept*/', 'demo//different/*kept*/'],
    ['name = 1', 'name = 4'],
    ['optional string name', 'optional bytes name'],
    ['optional string name', 'string name'],
    ['oneof choice', 'oneof different'],
    ['java_package', 'java_outer_classname']
  ]) {
    const changed = fingerprint(source.replace(before, after));
    assert.notEqual(changed.tokenSha256, original.tokenSha256);
    assert.notEqual(changed.contractSha256, original.contractSha256);
    assert.notEqual(changed.comparisonSha256, original.comparisonSha256);
  }
});

test('unterminated tokens, duplicate fields and unsupported blocks fail closed', () => {
  assert.throws(() => lex('/*'));
  assert.throws(() => lex('"unfinished'));
  assert.throws(() => fingerprint('message M { string a = 1; int64 b = 1; }'));
  assert.throws(() => fingerprint('service S { rpc R (X) returns (X); }'));
});

test('declaration hashes distinguish ordering from token changes', () => {
  const first = fingerprint('syntax="proto3"; message A { string x = 1; } message B { bool y = 2; }');
  const reordered = fingerprint('syntax="proto3"; message B { bool y = 2; } message A { string x = 1; }');
  assert.notEqual(first.tokenSha256, reordered.tokenSha256);
  assert.equal(first.unorderedDeclarationsSha256, reordered.unorderedDeclarationsSha256);
  assert.equal(first.comparisonSha256, reordered.comparisonSha256);
  const changed = fingerprint('syntax="proto3"; message A { bytes x = 1; } message B { bool y = 2; }');
  assert.notEqual(first.unorderedDeclarationsSha256, changed.unorderedDeclarationsSha256);
});
