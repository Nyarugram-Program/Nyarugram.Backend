using System.Net;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Text.RegularExpressions;
using Botgram.Backend;
using Botgram.Backend.Storage;
using Google.Protobuf;
using Npgsql;
using P = Botgram.Protocol.V1;

internal static class SchemaUpgradeTests
{
    private const string Password = "schema-upgrade-password";
    private const string SessionToken = "schema-upgrade-session-token";
    private const string ExpiredToken = "schema-upgrade-expired-token";
    private const string Salt = "00112233445566778899aabbccddeeff";
    private const string AliceId = "11111111111111111111111111111111";
    private const string BobId = "22222222222222222222222222222222";
    private const string ConversationId = "33333333333333333333333333333333";
    private const string CallId = "44444444444444444444444444444444";

    private static int assertions;

    public static async Task RunAsync(string connectionString)
    {
        var settings = new NpgsqlConnectionStringBuilder(connectionString) { Pooling = false };
        await using var source = NpgsqlDataSource.Create(settings.ConnectionString);
        await PostgresBackendSchema.ApplyAsync(source);
        await WriteMany(source,
            "DELETE FROM botgram_backend_v1.schema_versions WHERE version=8",
            "ALTER TABLE botgram_backend_v1.calls DROP COLUMN request_protocol, DROP COLUMN accept_protocol");
        await Seed(source);
        var before = await Digest(source);
        Check((await Versions(source)).SequenceEqual(Enumerable.Range(1, 7)), "fixture starts at schema v7");
        Check((await Scalar(source, "SELECT current_database()")) is string current && current == settings.Database,
            "fixture database identity");

        // Precheck reports the supported v7 -> v8 path without changing anything.
        var inspection = await PostgresSchemaUpgrade.InspectAsync(source);
        Check(inspection.MigrationRequired && inspection.VersionsBefore.SequenceEqual(Enumerable.Range(1, 7))
            && inspection.VersionsAfter.SequenceEqual(Enumerable.Range(1, 7)) && !inspection.Applied,
            "precheck reports a required, supported v7->v8 upgrade");
        Check((await Digest(source)) == before, "precheck does not change business state");

        await ExpectReject(() => PostgresSchemaUpgrade.InspectAsync(NpgsqlDataSource.Create(
                new NpgsqlConnectionStringBuilder(settings.ConnectionString) { Database = "botgram_production" }.ConnectionString)),
            "non-botgram_test database is rejected");
        await Write(source, "DELETE FROM botgram_backend_v1.schema_versions WHERE version=7");
        await ExpectReject(() => PostgresSchemaUpgrade.InspectAsync(source), "v1..v6 is not a supported start version");
        await Write(source, "INSERT INTO botgram_backend_v1.schema_versions(version) VALUES(7)");
        await Write(source, "INSERT INTO botgram_backend_v1.schema_versions(version) VALUES(9)");
        await ExpectReject(() => PostgresSchemaUpgrade.InspectAsync(source), "unknown schema versions are rejected");
        await Write(source, "DELETE FROM botgram_backend_v1.schema_versions WHERE version=9");

        // A live backend connection blocks the offline operation even before it writes.
        await using (var writer = await source.OpenConnectionAsync())
            await ExpectReject(() => PostgresSchemaUpgrade.ApplyAsync(source), "an active client blocks the upgrade");
        Check((await Versions(source)).SequenceEqual(Enumerable.Range(1, 7)) && (await Digest(source)) == before,
            "a rejected upgrade leaves the schema and data unchanged");

        // The four-component runtime lease is also honored when the competing session looks like
        // the maintenance tool.
        await using (var worker = await source.OpenConnectionAsync())
        {
            await SetApplicationName(worker);
            await using (var lockCommand = new NpgsqlCommand("SELECT pg_advisory_lock(7459241304238)", worker))
                await lockCommand.ExecuteNonQueryAsync();
            await ExpectReject(() => PostgresSchemaUpgrade.ApplyAsync(source), "a four-component worker lease blocks the upgrade");
            await using (var unlock = new NpgsqlCommand("SELECT pg_advisory_unlock(7459241304238)", worker))
                await unlock.ExecuteNonQueryAsync();
        }

        // Inject a real failure inside the migration UPDATE and prove the whole transaction rolls back.
        await WriteMany(source,
            """
            CREATE FUNCTION botgram_test_reject_schema_upgrade() RETURNS trigger LANGUAGE plpgsql AS $$
            BEGIN RAISE EXCEPTION 'isolated schema upgrade failure'; END $$;
            """,
            """
            CREATE TRIGGER botgram_test_reject_schema_upgrade BEFORE UPDATE ON botgram_backend_v1.calls
            FOR EACH ROW EXECUTE FUNCTION botgram_test_reject_schema_upgrade();
            """);
        await ExpectReject(() => PostgresSchemaUpgrade.ApplyAsync(source), "migration failure rolls back");
        Check((await Versions(source)).SequenceEqual(Enumerable.Range(1, 7)) && (await Digest(source)) == before
            && await ColumnCount(source, "request_protocol") == 0 && await ColumnCount(source, "accept_protocol") == 0,
            "a failed migration keeps v7, the original rows and no half-added columns");
        await WriteMany(source,
            "DROP TRIGGER botgram_test_reject_schema_upgrade ON botgram_backend_v1.calls",
            "DROP FUNCTION botgram_test_reject_schema_upgrade()");

        // Two maintenance processes race; exactly one applies, the other is rejected without data loss.
        var racing = new NpgsqlConnectionStringBuilder(settings.ConnectionString)
            { ApplicationName = PostgresSchemaUpgrade.ApplicationName };
        await using var first = NpgsqlDataSource.Create(racing.ConnectionString);
        await using var second = NpgsqlDataSource.Create(racing.ConnectionString);
        async Task<(bool Applied, string? Error)> TryApply(NpgsqlDataSource target)
        {
            try
            {
                var applied = await PostgresSchemaUpgrade.ApplyAsync(target);
                return (applied.Applied, null);
            }
            catch (InvalidOperationException error) { return (false, error.Message); }
            catch (Exception error) { return (false, error.GetType().Name + ": " + error.Message); }
        }
        var outcomes = await Task.WhenAll(TryApply(first), TryApply(second));
        Check(outcomes.Count(result => result.Applied) == 1,
            "concurrent upgrades have exactly one applying writer: "
            + string.Join(" | ", outcomes.Select(result => result.Error ?? (result.Applied ? "APPLIED" : "NO-OP"))));
        Check((await Versions(source)).SequenceEqual(Enumerable.Range(1, 8)), "the winner reaches v8");
        Check((await Digest(source)) == before, "the in-place upgrade preserves every seeded business row");
        var requestJson = (string)(await Scalar(source,
            "SELECT request_protocol::text FROM botgram_backend_v1.calls WHERE id=$1", CallId))!;
        var acceptJson = (string)(await Scalar(source,
            "SELECT accept_protocol::text FROM botgram_backend_v1.calls WHERE id=$1", CallId))!;
        var negotiatedJson = (string)(await Scalar(source,
            "SELECT protocol::text FROM botgram_backend_v1.calls WHERE id=$1", CallId))!;
        Check(requestJson == acceptJson && requestJson == negotiatedJson,
            "legacy call proposals are backfilled from the negotiated protocol");

        // Repeating on the target schema is a safe no-op.
        var repeat = await PostgresSchemaUpgrade.ApplyAsync(source);
        Check(!repeat.Applied && !repeat.MigrationRequired && repeat.VersionsBefore.SequenceEqual(Enumerable.Range(1, 8)),
            "repeated upgrade on v8 is a no-op");
        Check((await Digest(source)) == before, "a no-op does not touch business rows");

        // The current backend startup check, login, session, privacy, validator and call reads work.
        var backend = new PostgresBackend(source);
        await backend.VerifySchemaAsync();
        var session = await backend.RequireAsync(Auth.HashToken(SessionToken));
        Check(session.UserId == AliceId, "valid session survives the upgrade");
        await ExpectReject(() => backend.RequireAsync(Auth.HashToken(ExpiredToken)), "expired session stays invalid");
        var login = await backend.LoginAsync(new("seed_bob", Password));
        Check(login.User.Id == BobId && !string.IsNullOrEmpty(login.Token), "existing password still authenticates");
        Check((await backend.MeAsync(session)).Username == "seed_alice", "account view reads after upgrade");
        var privacy = await backend.DispatchAsync(session,
            new P.RpcRequest { AccountPrivacy = new() { GetRules = new() { Key = P.PrivacyKey.PhoneP2P } } },
            "isolated.test", response => response);
        Check(privacy.AccountPrivacy.Rules.Rules.Single().Kind == P.PrivacyRuleKind.DisallowAll,
            "privacy rules survive the upgrade");
        var stateNode = JsonNode.Parse((string)(await Scalar(source,
            "SELECT state::text FROM botgram_backend_v1.privacy_accounts WHERE user_id=$1", AliceId))!)!;
        var restored = JsonSerializer.Deserialize<AccountPrivacyState>(stateNode.ToJsonString(), Store.Json)!.Totp;
        Check(restored.Secret == "protected-totp-secret" && restored.RecoveryHashes.SequenceEqual(new[]
                { "sha256:recovery-a", "hmac:recovery-b" }),
            "validator and recovery material remain readable by the account model");
        var call = await backend.CallAsync(session, CallId);
        Check(call.Id == CallId && call.State == "ended"
            && call.TelegramId == 900001 && !string.IsNullOrEmpty(call.Protocol?["library_versions"]?.ToJsonString()),
            "native call state survives the upgrade");
        Check((await Scalar(source, "SELECT to_regclass('botgram_backend_v1.wallpaper_accounts')::text")) is string,
            "current startup schema check finds the wallpaper tables");

        Console.WriteLine($"PASS: {assertions} schema-upgrade assertions; the same isolated database was upgraded in place from v7 to v8");
    }

    // Prepares a named, disposable v7 database so the actual Storage.Admin CLI can be run
    // against one explicit target outside the automated assertions.
    public static async Task PrepareFixtureAsync(string adminConnectionString, string database)
    {
        var settings = new NpgsqlConnectionStringBuilder(adminConnectionString);
        if (!Loopback(settings.Host) ||
            !Regex.IsMatch(database, "\\Abotgram_test_[a-z0-9_]+\\z", RegexOptions.CultureInvariant))
            throw new InvalidOperationException("The fixture must be a loopback botgram_test_* database");
        await using var admin = NpgsqlDataSource.Create(settings.ConnectionString);
        await using (var drop = admin.CreateCommand($"DROP DATABASE IF EXISTS {database} WITH (FORCE)"))
            await drop.ExecuteNonQueryAsync();
        await using (var create = admin.CreateCommand($"CREATE DATABASE {database}"))
            await create.ExecuteNonQueryAsync();
        var fixture = new NpgsqlConnectionStringBuilder(settings.ConnectionString) { Database = database, Pooling = false };
        await using var source = NpgsqlDataSource.Create(fixture.ConnectionString);
        await PostgresBackendSchema.ApplyAsync(source);
        await WriteMany(source,
            "DELETE FROM botgram_backend_v1.schema_versions WHERE version=8",
            "ALTER TABLE botgram_backend_v1.calls DROP COLUMN request_protocol, DROP COLUMN accept_protocol");
        await Seed(source);
        Check((await Versions(source)).SequenceEqual(Enumerable.Range(1, 7)), "the CLI fixture starts at schema v7");
        Console.WriteLine($"PASS: prepared v7 fixture database {database}");
    }

    // Runs the same application reads on an already upgraded CLI target.
    public static async Task VerifyFixtureAsync(string connectionString)
    {
        var settings = new NpgsqlConnectionStringBuilder(connectionString) { Pooling = false };
        if (!Loopback(settings.Host) ||
            !Regex.IsMatch(settings.Database ?? "", "\\Abotgram_test_[a-z0-9_]+\\z", RegexOptions.CultureInvariant))
            throw new InvalidOperationException("The fixture must be a loopback botgram_test_* database");
        await using var source = NpgsqlDataSource.Create(settings.ConnectionString);
        var backend = new PostgresBackend(source);
        await backend.VerifySchemaAsync();
        Check((await Versions(source)).SequenceEqual(Enumerable.Range(1, 8)), "the CLI target is at schema v8");
        var requestJson = (string)(await Scalar(source,
            "SELECT request_protocol::text FROM botgram_backend_v1.calls WHERE id=$1", CallId))!;
        var acceptJson = (string)(await Scalar(source,
            "SELECT accept_protocol::text FROM botgram_backend_v1.calls WHERE id=$1", CallId))!;
        Check(requestJson == acceptJson, "the CLI target backfills both original call proposals");
        var session = await backend.RequireAsync(Auth.HashToken(SessionToken));
        Check(session.UserId == AliceId && (await backend.MeAsync(session)).Username == "seed_alice",
            "the CLI target keeps valid sessions and account reads");
        var login = await backend.LoginAsync(new("seed_bob", Password));
        Check(login.User.Id == BobId, "the CLI target keeps password authentication");
        var privacy = await backend.DispatchAsync(session,
            new P.RpcRequest { AccountPrivacy = new() { GetRules = new() { Key = P.PrivacyKey.PhoneP2P } } },
            "isolated.test", response => response);
        Check(privacy.AccountPrivacy.Rules.Rules.Single().Kind == P.PrivacyRuleKind.DisallowAll,
            "the CLI target keeps privacy rules");
        var stateNode = JsonNode.Parse((string)(await Scalar(source,
            "SELECT state::text FROM botgram_backend_v1.privacy_accounts WHERE user_id=$1", AliceId))!)!;
        var totp = JsonSerializer.Deserialize<AccountPrivacyState>(stateNode.ToJsonString(), Store.Json)!.Totp;
        Check(totp.Secret == "protected-totp-secret" && totp.RecoveryHashes.Count == 2,
            "the CLI target keeps validator and recovery material");
        var call = await backend.CallAsync(session, CallId);
        Check(call.Id == CallId && call.State == "ended" && call.TelegramId == 900001,
            "the CLI target keeps native call records");
        Console.WriteLine($"PASS: {assertions} CLI-target verification assertions; login, privacy, validator and call reads succeeded");
    }

    private static async Task Seed(NpgsqlDataSource source)
    {
        var passwordHash = Auth.HashPassword(Password, Salt);
        var protocol = JsonSerializer.Serialize(CallService.DefaultProtocol(), Store.Json);
        var rules = new P.PrivacyRuleSet
        {
            Rules = { new P.PrivacyRule { Kind = P.PrivacyRuleKind.DisallowAll } }
        }.ToByteArray();
        var privacy = JsonSerializer.Serialize(new AccountPrivacyState
        {
            Rules = { [(int)P.PrivacyKey.PhoneP2P] = rules },
            AccountTtlDays = 180,
            DefaultHistoryTtlSeconds = 86400,
            Password = new AccountPasswordState { Hint = "seed hint" },
            Totp = new AccountTotpState
            {
                Secret = "protected-totp-secret",
                RecoveryHashes = ["sha256:recovery-a", "hmac:recovery-b"]
            }
        }, Store.Json);
        await WriteMany(source,
            $"INSERT INTO botgram_intake_v1.users(id) VALUES ('{AliceId}'),('{BobId}')",
            $"""
            INSERT INTO botgram_intake_v1.sessions(token_hash,user_id,expires_at) VALUES
                ('{Auth.HashToken(SessionToken)}','{AliceId}',now()+interval '1 hour'),
                ('{Auth.HashToken(ExpiredToken)}','{AliceId}',now()-interval '1 hour')
            """,
            $"""
            INSERT INTO botgram_backend_v1.accounts(id,username,display_name,salt,password_hash,gid) VALUES
                ('{AliceId}','seed_alice','Seed Alice','{Salt}','{passwordHash}','00112345678'),
                ('{BobId}','seed_bob','Seed Bob','{Salt}','{passwordHash}','00187654321')
            """,
            $"INSERT INTO botgram_backend_v1.identities(key,value) VALUES ('user:{AliceId}',100000),('user:{BobId}',100001),('chat:{ConversationId}',100500)",
            $"INSERT INTO botgram_intake_v1.conversations(id,sequence) VALUES ('{ConversationId}',1)",
            $"INSERT INTO botgram_backend_v1.conversations(id,kind,title,created_at,direct_key) VALUES ('{ConversationId}','direct','',now(),'{AliceId}:{BobId}')",
            $"INSERT INTO botgram_intake_v1.members(conversation_id,user_id,position) VALUES ('{ConversationId}','{AliceId}',0),('{ConversationId}','{BobId}',1)",
            $"INSERT INTO botgram_backend_v1.contacts(owner_id,user_id,first_name,last_name) VALUES ('{AliceId}','{BobId}','Seed Bob','Contact')");
        long messageId;
        await using (var connection = await source.OpenConnectionAsync())
        await using (var command = new NpgsqlCommand("""
            INSERT INTO botgram_intake_v1.messages(conversation_id,conversation_sequence,sender_id,client_key,body)
            VALUES($1,1,$2,'seed-message', $3::jsonb) RETURNING id
            """, connection))
        {
            command.Parameters.AddWithValue(ConversationId);
            command.Parameters.AddWithValue(AliceId);
            command.Parameters.AddWithValue(JsonSerializer.Serialize(new
            {
                id = 1, conversationId = ConversationId, senderId = AliceId,
                clientMessageId = "seed-message", text = "migrated message",
                createdAt = "2026-11-05T00:00:00Z"
            }, Store.Json));
            messageId = (long)(await command.ExecuteScalarAsync())!;
        }
        await WriteMany(source,
            $"INSERT INTO botgram_intake_v1.events(id,message_id,type,created_at,schema_version) VALUES(1,{messageId},'message.created','2026-11-05T00:00:00Z',1)",
            $"INSERT INTO botgram_intake_v1.event_recipients(event_id,user_id,pts) VALUES (1,'{AliceId}',2),(1,'{BobId}',2)",
            $"INSERT INTO botgram_intake_v1.outbox(id,event_id,partition_key,schema_version,projection_version) VALUES('55555555555555555555555555555555',1,'{ConversationId}',1,1)",
            $"INSERT INTO botgram_intake_v1.sync(user_id,pts) VALUES ('{AliceId}',2),('{BobId}',2)",
            "UPDATE botgram_intake_v1.event_cursor SET value=1",
            $"INSERT INTO botgram_backend_v1.receipts(conversation_id,user_id,message_id) VALUES('{ConversationId}','{BobId}',{messageId})");
        await Write(source, $"INSERT INTO botgram_backend_v1.privacy_accounts(user_id,state) VALUES('{AliceId}', $1::jsonb)", privacy);
        await Write(source, $"""
            INSERT INTO botgram_backend_v1.calls(id,conversation_id,caller_id,callee_id,state,updated_at,
                telegram_id,access_hash,random_id,video,protocol,g_a_hash,g_b,g_a,key_fingerprint,date,receive_date,start_date,
                reason,duration,connections,p2p_allowed)
                VALUES('{CallId}','{ConversationId}','{AliceId}','{BobId}','ended','2026-11-05T00:00:00Z',
                900001,123456789,4242,false,'{protocol}'::jsonb,NULL,NULL,NULL,0,1762300000,0,1762300000,
                'Hangup',30,'[]'::jsonb,true)
            """);
    }

    private static async Task<int[]> Versions(NpgsqlDataSource source)
        => (int[])(await Scalar(source,
            "SELECT coalesce(array_agg(version ORDER BY version),'{}') FROM botgram_backend_v1.schema_versions"))!;

    private static async Task<int> ColumnCount(NpgsqlDataSource source, string column)
        => (int)(long)(await Scalar(source, """
            SELECT count(*) FROM information_schema.columns
            WHERE table_schema='botgram_backend_v1' AND table_name='calls' AND column_name=$1
            """, column))!;

    private static async Task<string> Digest(NpgsqlDataSource source)
    {
        var text = (string)((await Scalar(source, """
            SELECT concat_ws(E'\n',                (SELECT coalesce(jsonb_agg(to_jsonb(x) ORDER BY x.id),'[]')::text FROM (
                    SELECT id,username,display_name,salt,password_hash,gid,premium FROM botgram_backend_v1.accounts) x),
                (SELECT coalesce(jsonb_agg(to_jsonb(x) ORDER BY x.token_hash),'[]')::text FROM (
                    SELECT token_hash,user_id,expires_at FROM botgram_intake_v1.sessions) x),
                (SELECT coalesce(jsonb_agg(to_jsonb(x) ORDER BY x.key),'[]')::text FROM (
                    SELECT key,value FROM botgram_backend_v1.identities) x),
                (SELECT coalesce(jsonb_agg(to_jsonb(x) ORDER BY x.id),'[]')::text FROM (
                    SELECT id,kind,title,created_at,direct_key,ordinal FROM botgram_backend_v1.conversations) x),
                (SELECT coalesce(jsonb_agg(to_jsonb(x) ORDER BY x.conversation_id,x.user_id),'[]')::text FROM (
                    SELECT conversation_id,user_id,position FROM botgram_intake_v1.members) x),
                (SELECT coalesce(jsonb_agg(to_jsonb(x) ORDER BY x.id),'[]')::text FROM (
                    SELECT id,conversation_id,conversation_sequence,sender_id,client_key,body FROM botgram_intake_v1.messages) x),
                (SELECT coalesce(jsonb_agg(to_jsonb(x) ORDER BY x.id),'[]')::text FROM (
                    SELECT id,message_id,type,created_at,schema_version FROM botgram_intake_v1.events) x),
                (SELECT coalesce(jsonb_agg(to_jsonb(x) ORDER BY x.event_id,x.user_id),'[]')::text FROM (
                    SELECT event_id,user_id,pts FROM botgram_intake_v1.event_recipients) x),
                (SELECT coalesce(jsonb_agg(to_jsonb(x) ORDER BY x.id),'[]')::text FROM (
                    SELECT id,event_id,partition_key,schema_version,projection_version FROM botgram_intake_v1.outbox) x),
                (SELECT coalesce(jsonb_agg(to_jsonb(x) ORDER BY x.user_id),'[]')::text FROM (
                    SELECT user_id,pts FROM botgram_intake_v1.sync) x),
                (SELECT value FROM botgram_intake_v1.event_cursor)::text,
                (SELECT coalesce(jsonb_agg(to_jsonb(x) ORDER BY x.owner_id,x.user_id),'[]')::text FROM (
                    SELECT owner_id,user_id,first_name,last_name FROM botgram_backend_v1.contacts) x),
                (SELECT coalesce(jsonb_agg(to_jsonb(x) ORDER BY x.conversation_id,x.user_id),'[]')::text FROM (
                    SELECT conversation_id,user_id,message_id FROM botgram_backend_v1.receipts) x),
                (SELECT coalesce(jsonb_agg(to_jsonb(x) ORDER BY x.user_id),'[]')::text FROM (
                    SELECT user_id,state::text AS state FROM botgram_backend_v1.privacy_accounts) x),
                (SELECT coalesce(jsonb_agg(to_jsonb(x) ORDER BY x.id),'[]')::text FROM (
                    SELECT id,conversation_id,caller_id,callee_id,state,updated_at,telegram_id,access_hash,random_id,video,
                        protocol,g_a_hash,g_b,g_a,key_fingerprint,date,receive_date,start_date,reason,duration,connections,
                        p2p_allowed FROM botgram_backend_v1.calls) x))
            """))!);
        return Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(text)));
    }

    private static async Task<object?> Scalar(NpgsqlDataSource source, string sql, params object[] values)
    {
        await using var connection = await source.OpenConnectionAsync();
        await using var command = new NpgsqlCommand(sql, connection);
        foreach (var value in values) command.Parameters.AddWithValue(value);
        return await command.ExecuteScalarAsync();
    }

    private static async Task Write(NpgsqlDataSource source, string sql, params object[] values)
    {
        await using var connection = await source.OpenConnectionAsync();
        await using var command = new NpgsqlCommand(sql, connection);
        foreach (var value in values) command.Parameters.AddWithValue(value);
        await command.ExecuteNonQueryAsync();
    }

    private static async Task WriteMany(NpgsqlDataSource source, params string[] statements)
    {
        await using var connection = await source.OpenConnectionAsync();
        foreach (var sql in statements)
        {
            await using var command = new NpgsqlCommand(sql, connection);
            await command.ExecuteNonQueryAsync();
        }
    }

    private static async Task SetApplicationName(NpgsqlConnection connection)
    {
        await using var command = new NpgsqlCommand("SELECT set_config('application_name',$1,false)", connection);
        command.Parameters.AddWithValue(PostgresSchemaUpgrade.ApplicationName);
        await command.ExecuteNonQueryAsync();
    }

    private static async Task ExpectReject(Func<Task> action, string description)
    {
        try { await action(); }
        catch (InvalidOperationException) { Check(true, description); return; }
        catch (ApiException) { Check(true, description); return; }
        catch (PostgresException error) when (error.SqlState == "P0001") { Check(true, description); return; }
        throw new InvalidOperationException("Expected rejection: " + description);
    }

    private static bool Loopback(string? host) => host == "localhost" ||
        IPAddress.TryParse(host, out var address) && IPAddress.IsLoopback(address);

    private static void Check(bool condition, string description)
    {
        assertions++;
        if (!condition) throw new InvalidOperationException(description);
    }
}
