// RelationalBackend outbox completion regression (review checklist, problem four).
//
// Run against a FRESH, isolated PostgreSQL database: set BOTGRAM_E2EE_PG (or
// BOTGRAM_PG_TEST_CONNECTION_STRING) to a loopback botgram_test_* database and
// BOTGRAM_PSQL to the psql executable. The script never connects to a production
// database, never prints connection credentials and never creates accounts
// outside the isolated database.
//
// Phase 1 starts an instance with the worker disabled and a tiny backlog cap,
// writes past the cap and proves storage_backlog_full with genuinely unfinished
// rows. Phase 2 restarts on the SAME database with the worker enabled and proves
// the full claim -> staging -> projection -> publication chain. Phase 3 keeps
// writing under a live worker, verifies idempotent replay and history/difference.
// Phase 4 restarts once more and verifies the worker resumes from interruption.
import assert from 'node:assert/strict';
import { execFileSync } from 'node:child_process';
import { spawn } from 'node:child_process';
import { once } from 'node:events';
import { mkdtemp } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import net from 'node:net';
import {
  vfield, sfield, bfield, fixed64, concat, fields, first, all,
  envelope, call, expectError, outcome, register, changeGid, profile
} from './protobuf-plane.mjs';

function parseConnection(value) {
  return Object.fromEntries(value.split(';').filter(Boolean).map(pair => {
    const index = pair.indexOf('=');
    return [pair.slice(0, index).trim().toLowerCase(), pair.slice(index + 1).trim()];
  }));
}

const connectionString = process.env.BOTGRAM_E2EE_PG || process.env.BOTGRAM_PG_TEST_CONNECTION_STRING;
if (!connectionString) {
  console.error('ERROR: set BOTGRAM_E2EE_PG or BOTGRAM_PG_TEST_CONNECTION_STRING to an isolated PostgreSQL database.');
  process.exit(1);
}
if (!process.env.BOTGRAM_PSQL) {
  console.error('ERROR: set BOTGRAM_PSQL to the psql executable path.');
  process.exit(1);
}
const settings = parseConnection(connectionString);
const database = settings.database ?? settings['initial catalog'] ?? '';
const pgHost = settings.host ?? settings.server ?? '127.0.0.1';
const pgPort = settings.port ?? '5432';
const pgUser = settings.username ?? settings['user id'] ?? settings.userid;
if (!/^botgram_test_[a-z0-9_]+$/i.test(database)) {
  console.error('ERROR: the isolation database must be named botgram_test_* (refusing a non-test database).');
  process.exit(1);
}
if (!['localhost', '127.0.0.1', '::1', '[::1]'].includes(pgHost.toLowerCase())) {
  console.error('ERROR: the isolation database host must be loopback (refusing a remote database).');
  process.exit(1);
}
if (!pgUser) {
  console.error('ERROR: the isolation connection string must include Username/User ID.');
  process.exit(1);
}
// Connection settings (including any password) are never logged. Authentication
// for psql is expected to come from PGPASSWORD / .pgpass / local trust.
function sql(statement) {
  return execFileSync(process.env.BOTGRAM_PSQL,
    ['-h', pgHost, '-p', pgPort, '-U', pgUser, '-d', database, '-tAc', statement],
    { encoding: 'utf8' }).trim();
}

// Mirrors PostgresMessageAcceptance.CapacityCountSql (internal, same assembly).
const pendingSql = `SELECT count(*) FROM botgram_intake_v1.outbox o
  WHERE o.state <> 'Published'
     OR (o.projection_version IS NULL AND NOT o.consumed)
     OR (o.projection_version IS NOT NULL AND NOT EXISTS (
        SELECT 1 FROM botgram_intake_v1.projection_tasks t
        JOIN botgram_intake_v1.projection_heads h
          ON h.conversation_id=t.conversation_id AND h.generation=t.generation
        WHERE t.outbox_id=o.id AND t.state='Complete'))`;
const pending = () => Number(sql(pendingSql));
const count = (statement) => Number(sql(statement));
const sleep = (ms) => new Promise(resolve => setTimeout(resolve, ms));

function diagnostics() {
  const q = (statement) => { try { return sql(statement); } catch (error) { return 'ERR:' + error.message; } };
  return [
    'pending=' + q(pendingSql),
    'outbox_total=' + q('SELECT count(*) FROM botgram_intake_v1.outbox'),
    "outbox_pending=" + q("SELECT count(*) FROM botgram_intake_v1.outbox WHERE state='Pending'"),
    "outbox_leased=" + q("SELECT count(*) FROM botgram_intake_v1.outbox WHERE state='Leased'"),
    "outbox_published=" + q("SELECT count(*) FROM botgram_intake_v1.outbox WHERE state='Published'"),
    "published_null_broker=" + q("SELECT count(*) FROM botgram_intake_v1.outbox WHERE state='Published' AND broker_topic IS NULL"),
    "consumed=" + q('SELECT count(*) FROM botgram_intake_v1.outbox WHERE consumed'),
    "tasks_complete=" + q("SELECT count(*) FROM botgram_intake_v1.projection_tasks WHERE state='Complete'"),
    "tasks_pending=" + q("SELECT count(*) FROM botgram_intake_v1.projection_tasks WHERE state='Pending'"),
    "tasks_leased=" + q("SELECT count(*) FROM botgram_intake_v1.projection_tasks WHERE state='Leased'"),
    "delivery_inbox=" + q('SELECT count(*) FROM botgram_intake_v1.delivery_inbox'),
    'events=' + q('SELECT count(*) FROM botgram_intake_v1.events'),
    'messages=' + q('SELECT count(*) FROM botgram_intake_v1.messages'),
    'recipients=' + q('SELECT count(*) FROM botgram_intake_v1.event_recipients')
  ].join(' ');
}

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const dll = process.env.BOTGRAM_TEST_DLL ?? path.join(root, 'bin/Debug/net8.0/Botgram.Backend.dll');
const temp = await mkdtemp(path.join(process.env.BOTGRAM_TEST_ROOT ?? path.join(root, 'obj'), 'relational-outbox-'));
const listener = net.createServer().listen(0, '127.0.0.1');
await once(listener, 'listening');
const port = listener.address().port;
await new Promise(resolve => listener.close(resolve));
const base = `http://127.0.0.1:${port}`;
const domain = 'nyarugram.moe.autos';
let server, logs = '';
let checks = 0;
const checkpoint = (label) => { checks++; console.log(`  [ok] ${label}`); };

async function start(extra = {}) {
  logs = '';
  server = spawn('dotnet', [dll, '--urls', base], {
    cwd: root,
    env: {
      ...process.env,
      ASPNETCORE_ENVIRONMENT: 'Development',
      Storage__Mode: 'NewStorageValidation',
      Storage__ValidationBackend: 'RelationalBackend',
      Storage__ValidationAcknowledged: 'true',
      Storage__ValidationConnectionString: connectionString,
      Protocol__LegacyJsonRpcEnabled: 'false',
      Protocol__LegacyJsonWssEnabled: 'false',
      Protocol__ProtobufRolloutPercent: '100',
      Storage__MaximumBacklog: '5',
      Storage__RelationalOutboxCompletionLeaseSeconds: '5',
      DataPath: path.join(temp, 'keys.json'),
      ...extra
    },
    windowsHide: true, stdio: ['ignore', 'pipe', 'pipe']
  });
  server.stdout.on('data', value => logs += value);
  server.stderr.on('data', value => logs += value);
  for (let i = 0; i < 180; i++) {
    if (server.exitCode !== null) throw new Error('backend exited during startup:\n' + logs);
    try { if ((await fetch(base + '/health')).ok) return; } catch {}
    await sleep(100);
  }
  throw new Error('backend did not become healthy:\n' + logs);
}
async function stop() {
  if (server && server.exitCode === null) { const exit = once(server, 'exit'); server.kill(); await exit; }
}
async function waitForDrain(target, timeoutMs) {
  const deadline = Date.now() + timeoutMs;
  let last = -1;
  while (Date.now() < deadline) {
    last = pending();
    if (last <= target) return last;
    await sleep(200);
  }
  throw new Error(`outbox did not drain to <=${target} within ${timeoutMs}ms (last=${last}): ${diagnostics()}`);
}

function userRef(id) { return concat(sfield(1, domain), sfield(2, id)); }
function peerRef(id) { return concat(bfield(1, userRef(id))); }
function rpc(token, tag, payload) { return call(base, '/api/v1/rpc', envelope(tag, payload), token); }
function sendBody(peer, randomId, text) {
  return concat(bfield(1, peerRef(peer)), fixed64(2, randomId), sfield(3, text));
}
function messageId(result) {
  const root = fields(outcome(result, 107));
  const message = fields(first(root, 1).bytes);
  return Number(first(message, 1).value);
}
let gidCounter = 0;
function gidNumber() {
  // Deterministically distinct 8-digit numbers so every gid change really emits an event.
  return String(10000000 + (++gidCounter) * 1000003 + Math.floor(Math.random() * 100000));
}

try {
  // ---- Phase 1: worker disabled, tiny cap -> durable backlog + 503. ----
  console.log('[phase 1] worker disabled: build an unfinished backlog');
  await start({ Storage__RelationalOutboxCompletionEnabled: 'false' });
  const baselineEventId = count('SELECT coalesce(max(event_id),0) FROM botgram_intake_v1.outbox');
  const scope = `event_id > ${baselineEventId}`;
  const oScope = `o.event_id > ${baselineEventId}`;
  const baseline = {
    tasks: count('SELECT count(*) FROM botgram_intake_v1.projection_tasks'),
    inbox: count('SELECT count(*) FROM botgram_intake_v1.delivery_inbox')
  };

  const suffix = Date.now().toString(36) + Math.random().toString(36).slice(2, 6);
  const a = await register(base, 'ro_a_' + suffix, 'relational-outbox-password', 'Account A');
  const b = await register(base, 'ro_b_' + suffix, 'relational-outbox-password', 'Account B');
  const aId = profile(a.user).userId, bId = profile(b.user).userId;
  assert.ok(aId && bId && aId !== bId, 'two distinct accounts must be registered');
  checkpoint('two accounts registered in the isolated database');

  // Bootstrap the direct conversation with one ordinary plaintext message.
  const bootstrapRandom = 8100000000001n;
  const bootstrap = await rpc(a.token, 107, sendBody(bId, bootstrapRandom, 'bootstrap'));
  assert.equal(bootstrap.status, 200, 'bootstrap message must succeed: ' + diagnostics());
  checkpoint('conversation established with a real message');

  // General events (gid changes) while the backlog is still below the cap.
  assert.equal((await changeGid(base, a.token, '030', gidNumber())).status, 200,
    'first general event must succeed: ' + diagnostics());
  assert.equal((await changeGid(base, a.token, '030', gidNumber())).status, 200,
    'second general event must succeed: ' + diagnostics());
  checkpoint('general events emitted (user.gid_changed)');

  // Keep writing messages until the durable backlog trips the capacity guard.
  let full = false;
  for (let i = 0; i < 12 && !full; i++) {
    const result = await rpc(a.token, 107, sendBody(bId, bootstrapRandom + 1n + BigInt(i), 'load-' + i));
    if (result.status === 503) { expectError(result, 503, 'storage_backlog_full'); full = true; break; }
    assert.equal(result.status, 200, `message ${i} should succeed before the cap: ${diagnostics()}`);
  }
  assert.ok(full, 'worker-disabled instance must eventually report storage_backlog_full (503)');
  checkpoint('storage_backlog_full (503) raised after writing past the cap');

  assert.ok(pending() >= 5, `unfinished outbox rows must be >= cap (5): ${diagnostics()}`);
  const added = count(`SELECT count(*) FROM botgram_intake_v1.outbox WHERE ${scope}`);
  assert.ok(added >= 5, `at least 5 outbox rows must have been written: ${diagnostics()}`);
  assert.equal(
    count(`SELECT count(*) FROM botgram_intake_v1.outbox WHERE ${scope} AND state='Pending'`),
    added, 'every newly written outbox row must still be Pending (worker disabled)');
  assert.equal(
    count(`SELECT count(*) FROM botgram_intake_v1.outbox WHERE ${scope} AND consumed`),
    0, 'no newly written row may be consumed before staging');
  assert.equal(count('SELECT count(*) FROM botgram_intake_v1.projection_tasks'), baseline.tasks,
    'no projection task may exist before staging');
  assert.equal(count('SELECT count(*) FROM botgram_intake_v1.delivery_inbox'), baseline.inbox,
    'delivery_inbox must not grow before staging');
  checkpoint('SQL confirms initial unfinished state (state/consumed/projection_tasks/inbox)');

  const sourceBefore = {
    events: count('SELECT count(*) FROM botgram_intake_v1.events'),
    messages: count('SELECT count(*) FROM botgram_intake_v1.messages'),
    recipients: count('SELECT count(*) FROM botgram_intake_v1.event_recipients')
  };

  // ---- Phase 2: same database, worker enabled -> full completion chain. ----
  console.log('[phase 2] worker enabled: drain the backlog through the real chain');
  await stop();
  await start({
    Storage__RelationalOutboxCompletionEnabled: 'true',
    Storage__RelationalOutboxCompletionIntervalMs: '100',
    Storage__RelationalOutboxCompletionBatchSize: '50',
    Storage__RelationalOutboxCompletionMaxBatchesPerCycle: '20'
  });
  assert.equal(await waitForDrain(0, 30000), 0, 'worker must drain all unfinished outbox rows');
  checkpoint('unfinished outbox rows fell to 0 within 30s');

  const generalTotal = count(`SELECT count(*) FROM botgram_intake_v1.outbox WHERE ${scope} AND projection_version IS NULL`);
  assert.ok(generalTotal >= 2, `general events must be present: ${diagnostics()}`);
  assert.equal(count(`SELECT count(*) FROM botgram_intake_v1.outbox
      WHERE ${scope} AND projection_version IS NULL AND consumed AND state='Published'
        AND broker_topic IS NULL AND broker_partition IS NULL AND broker_offset IS NULL`),
    generalTotal, 'every general event: consumed=true, Published, broker coordinates NULL');
  checkpoint('general events staged(consumed=true) + Published with NULL broker coords');

  const messageTotal = count(`SELECT count(*) FROM botgram_intake_v1.outbox WHERE ${scope} AND projection_version IS NOT NULL`);
  assert.ok(messageTotal >= 1, `message rows must be present: ${diagnostics()}`);
  assert.equal(count(`SELECT count(*) FROM botgram_intake_v1.outbox
      WHERE ${scope} AND projection_version IS NOT NULL AND state='Published'`),
    messageTotal, 'every message row is Published');
  assert.equal(count(`SELECT count(*) FROM botgram_intake_v1.outbox o
      WHERE ${oScope} AND o.projection_version IS NOT NULL AND EXISTS (
        SELECT 1 FROM botgram_intake_v1.projection_tasks t
        JOIN botgram_intake_v1.projection_heads h
          ON h.conversation_id=t.conversation_id AND h.generation=t.generation
        WHERE t.outbox_id=o.id AND t.state='Complete')`),
    messageTotal, 'every message has a Complete projection task on the active generation');
  checkpoint('messages Published with Complete projection on active generation');

  const stagedInbox = count(`SELECT count(*) FROM botgram_intake_v1.delivery_inbox i
      JOIN botgram_intake_v1.outbox o ON o.id=i.outbox_id WHERE ${oScope}`);
  assert.ok(stagedInbox >= added,
    `delivery_inbox must hold a real staged row per added outbox row (${stagedInbox}/${added}): ${diagnostics()}`);
  checkpoint('delivery_inbox proves real consumption (not a forged flag)');

  assert.ok(count('SELECT count(*) FROM botgram_intake_v1.events') >= sourceBefore.events,
    'events source rows must not be deleted');
  assert.ok(count('SELECT count(*) FROM botgram_intake_v1.messages') >= sourceBefore.messages,
    'messages source rows must not be deleted');
  assert.ok(count('SELECT count(*) FROM botgram_intake_v1.event_recipients') >= sourceBefore.recipients,
    'event_recipients source rows must not be deleted');
  checkpoint('events/messages/event_recipients source rows retained');

  // ---- Phase 3: sustained writes, idempotent replay, history/difference. ----
  console.log('[phase 3] sustained writes, idempotent replay, history/difference');
  const resumeRandom = 8300000000001n;
  let firstResumeId = null;
  let written = 0;
  for (let round = 0; round < 3; round++) {
    // Let the live worker drain between bursts so the tiny cap is never tripped
    // while still writing far more than the cap in total.
    await waitForDrain(1, 30000);
    for (let j = 0; j < 3; j++) {
      const result = await rpc(a.token, 107, sendBody(bId, resumeRandom + BigInt(written), 'after-recovery-' + written));
      assert.equal(result.status, 200,
        `post-recovery message ${written} must not be storage_backlog_full: ${diagnostics()}`);
      if (written === 0) firstResumeId = messageId(result);
      written++;
    }
    const general = await changeGid(base, a.token, '030', gidNumber());
    assert.equal(general.status, 200,
      `post-recovery general event ${round} must not be storage_backlog_full: ${diagnostics()}`);
  }
  assert.ok(written >= 9, 'more messages than the cap must succeed under a live worker');
  checkpoint('sustained writes (> cap) never hit storage_backlog_full');

  const beforeReplay = {
    messages: count('SELECT count(*) FROM botgram_intake_v1.messages'),
    pts: count('SELECT coalesce(sum(pts),0) FROM botgram_intake_v1.sync')
  };
  const replay = await rpc(a.token, 107, sendBody(bId, resumeRandom, 'after-recovery-0'));
  assert.equal(replay.status, 200, 'replay must succeed: ' + diagnostics());
  assert.equal(messageId(replay), firstResumeId, 'idempotent replay must return the same message id');
  assert.equal(count('SELECT count(*) FROM botgram_intake_v1.messages'), beforeReplay.messages,
    'replay must not insert another message');
  assert.equal(count('SELECT coalesce(sum(pts),0) FROM botgram_intake_v1.sync'), beforeReplay.pts,
    'replay must not advance pts');
  checkpoint('same random_id retry: same message id, no message/pts growth');

  const history = fields(outcome(await rpc(a.token, 105,
    concat(bfield(1, peerRef(bId)), vfield(7, 100))), 105));
  assert.ok(all(history, 1).length >= written,
    `history must contain all ${written} recovery messages: ${diagnostics()}`);
  const state = fields(outcome(await rpc(a.token, 100, new Uint8Array()), 100));
  assert.ok(Number(first(state, 1).value) >= 1, 'state must report a valid pts');
  const difference = fields(outcome(await rpc(a.token, 101, vfield(1, 1)), 101));
  assert.ok(difference.length > 0, 'difference must be non-empty');
  checkpoint('history / state / difference are complete and readable');

  // ---- Phase 4: restart and resume from interruption. ----
  console.log('[phase 4] restart and resume from interruption');
  await stop();
  await start({
    Storage__RelationalOutboxCompletionEnabled: 'true',
    Storage__RelationalOutboxCompletionIntervalMs: '100',
    Storage__RelationalOutboxCompletionBatchSize: '50',
    Storage__RelationalOutboxCompletionMaxBatchesPerCycle: '20'
  });
  assert.equal(await waitForDrain(0, 30000), 0,
    'after restart the worker must resume and leave 0 unfinished rows');
  const replay2 = await rpc(a.token, 107, sendBody(bId, resumeRandom, 'after-recovery-0'));
  assert.equal(replay2.status, 200, 'replay after restart must succeed');
  assert.equal(messageId(replay2), firstResumeId, 'idempotent replay stays idempotent after restart');
  checkpoint('restart resumes from interruption; replay still idempotent');

  console.log(`PASS: RelationalBackend outbox completion chain (${checks} checkpoints)`);
} catch (error) {
  console.error('FAIL: relational outbox completion regression');
  console.error('DIAGNOSTICS: ' + diagnostics());
  try { const health = await fetch(base + '/health'); console.error('HEALTH: ' + (await health.text()).slice(0, 800)); } catch {}
  if (logs.trim()) console.error('BACKEND LOGS:\n' + logs);
  throw error;
} finally {
  await stop();
}
