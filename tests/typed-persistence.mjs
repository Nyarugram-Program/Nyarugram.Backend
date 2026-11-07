// Node 22+, no packages. References an already-built backend DLL; never builds the backend.
import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { access, mkdir, mkdtemp, readFile, writeFile } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const dll = path.resolve(process.env.BOTGRAM_TEST_DLL ?? path.join(root, 'bin/Debug/net8.0/Botgram.Backend.dll'));
await access(dll);
const testRoot = path.resolve(process.env.BOTGRAM_TEST_ROOT ?? path.join(root, 'obj'));
await mkdir(testRoot, { recursive: true });
const temp = await mkdtemp(path.join(testRoot, 'typed-persistence-'));
const database = path.join(temp, 'db.json');
const createdAt = '2020-01-01T00:00:00+00:00';
const expiresAt = '2099-01-01T00:00:00+00:00';
const names = ['Bold', 'Italic', 'Underline', 'Strike', 'Spoiler', 'Code', 'Pre', 'Url', 'TextUrl', 'Email', 'Phone', 'Mention', 'Hashtag', 'Cashtag', 'BotCommand'];
const entities = names.map(name => ({ $type: `org.telegram.tgnet.TLRPC$TL_messageEntity${name}`, offset: 1, length: 2,
  ...(name === 'Pre' ? { language: 'java' } : {}), ...(name === 'TextUrl' ? { url: 'https://example.invalid/typed' } : {}) }));
const conversation = { id: 'conversation-typed', kind: 'direct', title: 'typed fixture', members: ['user-a', 'user-b'], createdAt };
const rich = { id: 1, conversationId: conversation.id, senderId: 'user-a', clientMessageId: 'legacy-rich', text: 'A😀Z', createdAt,
  serviceTitle: null, serviceMembers: null, telegramEntities: entities, replyToMessageId: 2, silent: true };
const plain = { ...rich, id: 2, clientMessageId: 'legacy-null', telegramEntities: null, replyToMessageId: null, silent: false };
const empty = { ...plain, id: 3, clientMessageId: 'legacy-empty', telegramEntities: [] };
const minimal = { id: 4, conversationId: conversation.id, senderId: 'user-b', clientMessageId: 'legacy-defaults', text: 'old', createdAt };
const receipt = { conversationId: conversation.id, userId: 'user-b', messageId: 4 };
const call = { id: 'call-typed', conversationId: conversation.id, callerId: 'user-a', calleeId: 'user-b', state: 'ended', updatedAt: createdAt };
const payloads = [
  ['conversation.created', conversation], ['message.created', rich], ['receipt.updated', receipt], ['call.updated', call],
  ['call.signal', { callId: call.id, senderId: 'user-a', type: 'offer', data: 'isolated-test-sdp' }],
  ['user.gid_changed', { userId: 'user-a', gid: '03012345678' }]
];
// Deliberately write the old JSON shape, not data serialized by the new converters.
const legacy = {
  users: ['a', 'b'].map((suffix, i) => ({ id: `user-${suffix}`, username: `typed${suffix}`, displayName: suffix,
    salt: 'fixture-only', passwordHash: 'fixture-only', gid: i ? '02187654321' : '03012345678' })),
  sessions: [{ tokenHash: 'fixture-only-not-a-token', userId: 'user-a', expiresAt }],
  conversations: [conversation], messages: [rich, plain, empty, minimal], receipts: [receipt],
  contacts: [{ ownerId: 'user-a', userId: 'user-b', firstName: 'Typed', lastName: 'Fixture' }], calls: [call],
  events: payloads.map(([type, payload], i) => ({ id: i + 1, recipients: ['user-a', 'user-b'], type, payload, createdAt,
    ...(i % 2 ? { expiresAt } : {}) })),
  nextMessageId: 5, nextEventId: 7,
  telegramIds: { 'user:user-a': 100000, 'user:user-b': 100001, 'chat:conversation-typed': 100002 }, nextTelegramId: 100003
};
await writeFile(database, JSON.stringify(legacy));
const expected = structuredClone(legacy);
// Defaults added by later models when a legacy JSON snapshot is loaded. Keep
// them explicit so the fixture keeps proving that no original field changes.
const messageDefaults = { e2eeEnvelope: null, serviceKind: null };
const callDefaults = { telegramId: 0, accessHash: 0, randomId: 0, video: false, protocol: null,
  gaHash: null, gb: null, ga: null, keyFingerprint: 0, date: 0, receiveDate: 0, startDate: 0,
  reason: 'Hangup', duration: 0, connections: null, p2pAllowed: false, requestProtocol: null, acceptProtocol: null };
const userDefaults = { premium: false, lastSeen: null, online: false, presenceNotifiedAt: null };
Object.assign(expected.messages[3], { serviceTitle: null, serviceMembers: null, telegramEntities: null, replyToMessageId: null, silent: false });
expected.messages = expected.messages.map(message => ({ ...messageDefaults, ...message }));
expected.calls = expected.calls.map(value => ({ ...callDefaults, ...value }));
expected.users = expected.users.map(value => ({ ...userDefaults, ...value }));
expected.sessions = expected.sessions.map(value => ({ foregroundAt: null, ...value }));
expected.e2eeDevices = [];
expected.e2eeConversations = [];
expected.nextCallId = 0;
for (const event of expected.events) {
  event.expiresAt ??= null;
  if (event.type === 'message.created') event.payload = { ...messageDefaults, ...event.payload };
  if (event.type === 'call.updated') event.payload = { ...callDefaults, ...event.payload };
}

const xml = value => value.replaceAll('&', '&amp;').replaceAll('<', '&lt;').replaceAll('>', '&gt;').replaceAll('"', '&quot;');
await writeFile(path.join(temp, 'TypedPersistence.csproj'), `<Project Sdk="Microsoft.NET.Sdk">
  <PropertyGroup>
    <OutputType>Exe</OutputType><TargetFramework>net8.0</TargetFramework>
    <ImplicitUsings>enable</ImplicitUsings><Nullable>enable</Nullable><TreatWarningsAsErrors>true</TreatWarningsAsErrors>
  </PropertyGroup>
  <ItemGroup>
    <FrameworkReference Include="Microsoft.AspNetCore.App" />
    <Reference Include="Botgram.Backend"><HintPath>${xml(dll)}</HintPath><Private>true</Private></Reference>
  </ItemGroup>
</Project>
`);
await writeFile(path.join(temp, 'Program.cs'), String.raw`
using System.Text.Json;
using Botgram.Backend;
using Microsoft.Extensions.Hosting;

var mode = args[0];
var databasePath = args[1];
var builder = Host.CreateApplicationBuilder();
builder.Configuration["DataPath"] = databasePath;
var store = new Store(builder.Configuration, builder.Environment);

void Check(bool condition, string message)
{
    if (!condition) throw new InvalidOperationException(message);
}
string Snapshot() => store.Read(db => JsonSerializer.Serialize(db, Store.Json));
void AssertTyped()
{
    store.Read(db =>
    {
        var kinds = new[] { MessageEntityKind.Bold, MessageEntityKind.Italic, MessageEntityKind.Underline,
            MessageEntityKind.Strike, MessageEntityKind.Spoiler, MessageEntityKind.Code, MessageEntityKind.Pre,
            MessageEntityKind.Url, MessageEntityKind.TextUrl, MessageEntityKind.Email, MessageEntityKind.Phone,
            MessageEntityKind.Mention, MessageEntityKind.Hashtag, MessageEntityKind.Cashtag, MessageEntityKind.BotCommand };
        var entities = db.Messages[0].TelegramEntities!;
        Check(entities.Length == 15, "All 15 legacy entity kinds must survive");
        Check(entities.Select(e => e.Kind).SequenceEqual(kinds), "Entity kinds must be strongly typed and ordered");
        Check(entities.All(e => e.Offset == 1 && e.Length == 2), "UTF-16 entity bounds changed");
        Check(entities[6].Language == "java" && entities[8].Url == "https://example.invalid/typed", "Entity metadata lost");
        Check(db.Messages[1].TelegramEntities is null, "Null entities changed");
        Check(db.Messages[2].TelegramEntities is { Length: 0 }, "Empty entities changed");
        Check(db.Messages[3].TelegramEntities is null && !db.Messages[3].Silent && db.Messages[3].ReplyToMessageId is null,
            "Missing legacy optional fields must retain defaults");
        Type[] types = [typeof(Conversation), typeof(Message), typeof(Receipt), typeof(Call), typeof(CallSignal), typeof(GidChanged)];
        Check(db.Events.Count == types.Length, "Six event types must survive");
        for (var i = 0; i < types.Length; i++)
            Check(db.Events[i].Payload.GetType() == types[i], "Event payload is not concrete: " + db.Events[i].Type);
        Check(((Message)db.Events[1].Payload).TelegramEntities!.SequenceEqual(entities), "Event message entity values differ");
        Check(((CallSignal)db.Events[4].Payload).Data == "isolated-test-sdp", "Signal payload lost");
        Check(((GidChanged)db.Events[5].Payload).Gid == "03012345678", "GID payload lost");
        return true;
    });
}
void Mutate(Database db)
{
    db.Conversations[0].Members[0] = "mutated-member";
    db.Messages[0].TelegramEntities![0] = new(MessageEntityKind.Italic, 0, 1);
    ((Conversation)db.Events[0].Payload).Members[0] = "mutated-event-member";
    ((Message)db.Events[1].Payload).TelegramEntities![0] = new(MessageEntityKind.Code, 0, 1);
    db.Users.Clear(); db.Sessions.Clear(); db.Conversations.Clear(); db.Messages.Clear(); db.Receipts.Clear();
    db.Contacts.Clear(); db.Calls.Clear(); db.Events.Clear(); db.TelegramIds.Clear();
    db.NextMessageId = 999; db.NextEventId = 999; db.NextTelegramId = 999;
}
AssertTyped();
var before = Snapshot();
var diskBefore = File.ReadAllBytes(databasePath);
if (mode == "rollback")
{
    var callbackFailed = false;
    try
    {
        store.Write<bool>(db => { Mutate(db); throw new InvalidOperationException("intentional-callback-failure"); });
    }
    catch (InvalidOperationException ex) when (ex.Message == "intentional-callback-failure") { callbackFailed = true; }
    Check(callbackFailed, "Callback failure was not propagated");
    Check(Snapshot() == before, "Callback failure did not restore all in-memory state");
    Check(File.ReadAllBytes(databasePath).SequenceEqual(diskBefore), "Callback failure changed persisted data");
    AssertTyped();

    // A directory at the temp-file path reliably rejects the atomic write, even under elevated users.
    Directory.CreateDirectory(databasePath + ".tmp");
    var persistenceFailed = false;
    try
    {
        try { store.Write(db => { Mutate(db); return true; }); }
        catch (Exception ex) when (ex is IOException or UnauthorizedAccessException) { persistenceFailed = true; }
    }
    finally { Directory.Delete(databasePath + ".tmp"); }
    Check(persistenceFailed, "Persistence failure was not propagated");
    Check(Snapshot() == before, "Persistence failure did not restore all in-memory state");
    Check(File.ReadAllBytes(databasePath).SequenceEqual(diskBefore), "Persistence failure changed persisted data");
    AssertTyped();
    if (OperatingSystem.IsWindows())
    {
        // Allow rollback reads, but deny replacement of the committed snapshot.
        var replaceFailed = false;
        using (var heldSnapshot = new FileStream(databasePath, FileMode.Open, FileAccess.Read, FileShare.Read))
        {
            try { store.Write(db => { Mutate(db); return true; }); }
            catch (Exception ex) when (ex is IOException or UnauthorizedAccessException) { replaceFailed = true; }
        }
        Check(replaceFailed, "Atomic replacement failure was not propagated");
        Check(Snapshot() == before, "Atomic replacement failure did not restore all in-memory state");
        Check(File.ReadAllBytes(databasePath).SequenceEqual(diskBefore), "Atomic replacement failure changed persisted data");
        AssertTyped();
    }
}
else Check(mode is "load" or "restart", "Unknown harness mode");
store.Write(db => true);
Check(Snapshot() == before, "Successful write changed logical state");
AssertTyped();
File.WriteAllText(args[2], Snapshot());
Console.WriteLine("PASS: " + mode);
`);

const buildRoot = 'D:\\BotgramBuild';
const env = { ...process.env,
  DOTNET_CLI_HOME: path.join(buildRoot, 'dotnet'), NUGET_PACKAGES: path.join(buildRoot, 'nuget'),
  NUGET_HTTP_CACHE_PATH: path.join(buildRoot, 'nuget-http'),
  TEMP: path.join(buildRoot, 'tmp'), TMP: path.join(buildRoot, 'tmp'), DOTNET_CLI_TELEMETRY_OPTOUT: '1',
  DOTNET_SKIP_FIRST_TIME_EXPERIENCE: '1', DOTNET_NOLOGO: '1'
};
async function run(args) {
  await new Promise((resolve, reject) => {
    const child = spawn('dotnet', args, { cwd: root, env, windowsHide: true, stdio: ['ignore', 'pipe', 'pipe'] });
    let output = '';
    child.stdout.on('data', chunk => { output += chunk; });
    child.stderr.on('data', chunk => { output += chunk; });
    child.once('error', reject);
    child.once('close', code => code === 0 ? resolve() : reject(new Error(`dotnet ${args[0]} failed (${code}):\n${output}`)));
  });
}
// A file Reference (not ProjectReference) prevents this build from touching the main project.
await run(['build', path.join(temp, 'TypedPersistence.csproj'), '--nologo', '--ignore-failed-sources', '-p:NuGetAudit=false']);
const harness = path.join(temp, 'bin/Debug/net8.0/TypedPersistence.dll');
for (const mode of ['load', 'rollback', 'restart']) {
  const resultPath = path.join(temp, `${mode}.json`);
  // Each invocation is a separate process, releasing Store's exclusive lease before restart.
  await run([harness, mode, database, resultPath]);
  assert.deepEqual(JSON.parse(await readFile(resultPath, 'utf8')), expected, `${mode}: legacy data changed`);
  assert.deepEqual(JSON.parse(await readFile(database, 'utf8')), expected, `${mode}: persisted data changed`);
}
console.log('PASS: legacy JSON with all 15 typed entities and six concrete event payloads; null/empty/default fields; stable identities/cursors; callback, temp-write and Windows atomic-replacement Store.Write rollback; successful write and process restart; Flush(true) failure injection NOT TESTED');
