using System.Security.Cryptography;
using System.Text.Json;
using Google.Protobuf;
using Botgram.Backend;
using Botgram.Backend.Storage;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using Microsoft.AspNetCore.DataProtection;
using Npgsql;
using P = Botgram.Protocol.V1;

internal static class SnapshotMigrationTests
{
    public static async Task RunAsync(string connectionString)
    {
        int assertions = 0;
        void Check(bool condition, string label)
        {
            if (!condition) throw new InvalidOperationException(label);
            assertions++;
        }
        var db = new Database();
        var alice = Auth.PrepareRegistration(new("migration_alice", "Alice", "isolated-password")) with { Gid = "03012345678", Premium = true };
        var bob = Auth.PrepareRegistration(new("migration_bob", "Bob", "isolated-password")) with { Gid = "02112345678" };
        var outsider = Auth.PrepareRegistration(new("migration_outside", "Outside", "isolated-password")) with { Gid = "00512345678" };
        db.Users.AddRange([alice, bob, outsider]);
        var auth = Auth.CreateSession(db, alice);
        Auth.CreateSession(db, bob);
        var outsideAuth = Auth.CreateSession(db, outsider);
        var conversation = ConversationMessageService.Create(db, "direct", "", new[] { alice.Id, bob.Id }.OrderDescending().ToArray());
        var first = ConversationMessageService.Send(db, conversation, alice.Id, new("legacy-key", "legacy text"), MessageWriteContract.Rest);
        ConversationMessageService.Send(db, conversation, bob.Id, new("legacy-reply", "reply", ReplyId: first.Id, Silent: true), MessageWriteContract.Telegram);
        ConversationMessageService.ReadThrough(db, conversation, bob.Id, first.Id);
        ConversationMessageService.CreateGroup(db, alice.Id, "Legacy group", [alice.Id, bob.Id]);
        var saved = ConversationMessageService.Create(db, "saved", "", [alice.Id]);
        ConversationMessageService.Send(db, saved, alice.Id, new("saved-key", "saved text"), MessageWriteContract.Rest);
        ContactService.Import(db, alice.Id, 1, [new(7, bob.Gid, "Private", "Name", false)]);
        var call = new Call(Guid.NewGuid().ToString("N"), conversation.Id, alice.Id, bob.Id, "ended", DateTimeOffset.UtcNow);
        db.Calls.Add(call);
        db.Emit(conversation.Members, "call.updated", call);
        db.Emit([bob.Id], "call.signal", new CallSignal(call.Id, alice.Id, "offer", "synthetic-offer"), DateTimeOffset.UtcNow.AddHours(1));
        foreach (var user in db.Users) IdentityService.UserId(db, user.Id);
        foreach (var group in db.Conversations.Where(c => c.Kind == "group")) IdentityService.ChatId(db, group);
        var privacy = AccountPrivacyService.State(db, alice.Id);
        privacy.AccountTtlDays = 180;
        privacy.DefaultHistoryTtlSeconds = 86400;
        privacy.TopPeersEnabled = false;
        privacy.Blocked[outsider.Id] = DateTimeOffset.UtcNow;
        privacy.Rules[(int)P.PrivacyKey.PhoneCall] = new P.PrivacyRuleSet { Key = P.PrivacyKey.PhoneCall,
            Rules = { new P.PrivacyRule { Kind = P.PrivacyRuleKind.DisallowAll } } }.ToByteArray();
        new AccountTotpService(new EphemeralDataProtectionProvider()).Begin(alice.Id, alice.Username, privacy.Totp);
        var root = Path.Combine(Environment.GetEnvironmentVariable("BOTGRAM_TEST_ROOT") ?? "D:/BotgramBuild/tmp", "migration-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(root);
        var original = Path.Combine(root, "original.private.json");
        await File.WriteAllTextAsync(original, JsonSerializer.Serialize(db, Store.Json));
        var originalHash = SHA256.HashData(await File.ReadAllBytesAsync(original));
        await using var source = NpgsqlDataSource.Create(connectionString);
        await PostgresBackendSchema.ApplyAsync(source);
        var migration = new PostgresSnapshotMigration(source);
        var invalid = PostgresSnapshotMigration.Normalize(db);
        invalid.Contacts.Add(new(Guid.NewGuid().ToString("N"), bob.Id, "invalid", ""));
        try { await migration.ImportAsync(invalid); throw new Exception("Invalid late FK import succeeded"); }
        catch (PostgresException exception) when (exception.SqlState == PostgresErrorCodes.ForeignKeyViolation) { assertions++; }
        await using (var check = source.CreateCommand("SELECT (SELECT count(*) FROM botgram_intake_v1.users)+(SELECT count(*) FROM botgram_intake_v1.messages)"))
            Check((long)(await check.ExecuteScalarAsync())! == 0, "failed import retained partial users/messages");
        Check(await migration.ImportAsync(db), "first import not performed");
        Check(!await migration.ImportAsync(db), "same source imported twice");
        var restored = await migration.ExportAsync();
        Check(PostgresSnapshotMigration.Fingerprint(restored) == PostgresSnapshotMigration.Fingerprint(db), "full business snapshot roundtrip");
        Check(originalHash.SequenceEqual(SHA256.HashData(await File.ReadAllBytesAsync(original))), "source snapshot was modified");
        var backend = new PostgresBackend(source);
        var session = await backend.RequireAsync(Auth.HashToken(auth.Token));
        Check(session.UserId == alice.Id, "legacy bearer session identity changed");
        Check((await backend.PremiumAsync(session)).Active, "Premium lost during import");
        var read = await backend.DispatchAsync(session, new P.RpcRequest { ReadHistory = new()
        {
            Peer = new() { User = new() { ServerDomain = "migration.test", UserId = bob.Id } }, MaxMessageId = 0
        } }, "migration.test", response => response.ReadHistory);
        Check(read.PtsCount == 0, "read-only peer resolution must support legacy member ordering");
        Check((await backend.HistoryAsync(session, conversation.Id, null, null)).Select(m => m.Id).SequenceEqual(db.Messages.Where(m => m.ConversationId == conversation.Id).Select(m => m.Id)), "history identities/order changed");
        Check((await backend.SendRestAsync(session, conversation.Id, new("legacy-key", "legacy text"), m => m)).Id == first.Id,
            "legacy message idempotency key lost");
        var outside = await backend.RequireAsync(Auth.HashToken(outsideAuth.Token));
        try { await backend.HistoryAsync(outside, conversation.Id, null, null); throw new Exception("Imported membership leaked"); }
        catch (ApiException exception) when (exception.Status == 404) { assertions++; }
        await using (var owner = await source.OpenConnectionAsync())
        {
            await using var acquire = new NpgsqlCommand("SELECT pg_advisory_lock(7459241304238)", owner);
            await acquire.ExecuteNonQueryAsync();
            try { await migration.ExportAsync(); throw new Exception("Offline export ignored active worker ownership"); }
            catch (InvalidOperationException) { assertions++; }
            await using var release = new NpgsqlCommand("SELECT pg_advisory_unlock(7459241304238)", owner);
            await release.ExecuteNonQueryAsync();
        }
        var sent = await backend.SendRestAsync(session, conversation.Id, new("after-import", "new text"), m => m);
        Check(sent.Id == db.NextMessageId, "next message identity not preserved");
        try { await migration.ImportAsync(db); throw new Exception("Rerun overwrote changed business state"); }
        catch (InvalidOperationException) { assertions++; }
        var exported = await migration.ExportAsync();
        Check(exported.TelegramIds.OrderBy(p => p.Key).SequenceEqual(db.TelegramIds.OrderBy(p => p.Key)), "TL identities changed after writes");
        var rollback = Path.Combine(root, "rollback.private.json");
        await File.WriteAllTextAsync(rollback, JsonSerializer.Serialize(exported, Store.Json));
        var host = Host.CreateApplicationBuilder();
        host.Configuration.AddInMemoryCollection(new Dictionary<string, string?> { ["DataPath"] = rollback, ["Storage:Mode"] = "LegacyOnly" });
        using (var legacy = new Store(host.Configuration, host.Environment))
        {
            Check(legacy.Read(state => PostgresSnapshotMigration.Fingerprint(state)) == PostgresSnapshotMigration.Fingerprint(exported),
                "real legacy Store rejected or changed rollback export");
            Check(legacy.Write(state => ConversationMessageService.Send(state, Auth.Conversation(state, conversation.Id, alice.Id), alice.Id,
                new("after-import", "new text"), MessageWriteContract.Rest)).Id == sent.Id, "legacy rollback lost new-write idempotency");
            Auth.VerifyPassword(legacy.Read(state => state.Users.Single(u => u.Id == alice.Id)), "isolated-password");
            assertions++;
        }
        var missingName = "botgram_test_missing_gid_" + Guid.NewGuid().ToString("N");
        await using (var create = source.CreateCommand("CREATE DATABASE " + missingName)) await create.ExecuteNonQueryAsync();
        try
        {
            var missingOptions = new NpgsqlConnectionStringBuilder(connectionString) { Database = missingName, Pooling = false };
            await using var missingSource = NpgsqlDataSource.Create(missingOptions.ConnectionString);
            await PostgresBackendSchema.ApplyAsync(missingSource);
            var old = new Database();
            old.Users.Add(alice with { Gid = "" });
            var missingMigration = new PostgresSnapshotMigration(missingSource);
            Check(await missingMigration.ImportAsync(old) && !await missingMigration.ImportAsync(old), "missing-GID source rerun");
            var assigned = await missingMigration.ExportAsync();
            Check(assigned.Users.Single().Id == alice.Id && assigned.Users.Single().Gid.StartsWith("001") &&
                assigned.Events.Single().Type == "user.gid_changed", "legacy GID assignment must be persistent and preserve UUID");
            Check(old.Users.Single().Gid == "" && old.Events.Count == 0, "missing-GID source was mutated");
        }
        finally
        {
            await using var drop = source.CreateCommand("DROP DATABASE " + missingName);
            await drop.ExecuteNonQueryAsync();
        }
        Console.WriteLine($"PASS: {assertions} snapshot migration assertions; full model, rollback legacy Store, atomic failure, rerun, ownership, UUID/TL/GID/session/message preservation");
    }
}
