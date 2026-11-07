using System.Net;
using System.Text.Json;
using System.Text.RegularExpressions;
using Botgram.Backend;
using Botgram.Backend.Storage;
using Npgsql;

if (Environment.GetEnvironmentVariable("BOTGRAM_COMMIT_DIAGNOSTIC") != "1" &&
    Environment.GetEnvironmentVariable("BOTGRAM_CAPACITY_DIAGNOSTIC") != "1")
    await CacheTests.RunAsync();

var connectionString = Environment.GetEnvironmentVariable("BOTGRAM_PG_TEST_CONNECTION_STRING");
if (Environment.GetEnvironmentVariable("BOTGRAM_SCHEMA_UPGRADE_VERIFY") is { } verify)
{
    await SchemaUpgradeTests.VerifyFixtureAsync(verify);
    return;
}
if (string.IsNullOrWhiteSpace(connectionString))
{
    Console.WriteLine("SKIP: set BOTGRAM_PG_TEST_CONNECTION_STRING to a loopback PostgreSQL test database (botgram_test_*). Requires CREATE DATABASE permission.");
    return;
}
if (Environment.GetEnvironmentVariable("BOTGRAM_SCHEMA_UPGRADE_FIXTURE") is { } fixture)
{
    await SchemaUpgradeTests.PrepareFixtureAsync(connectionString, fixture);
    return;
}
var options = new NpgsqlConnectionStringBuilder(connectionString);
if (!(string.Equals(options.Host, "localhost", StringComparison.OrdinalIgnoreCase) ||
      (IPAddress.TryParse(options.Host, out var address) && IPAddress.IsLoopback(address))) ||
    !Regex.IsMatch(options.Database ?? "", "^botgram_test_[a-z0-9_]+$", RegexOptions.CultureInvariant))
    throw new InvalidOperationException("Only loopback and botgram_test_* databases are allowed.");
options.IncludeErrorDetail = false;
options.LogParameters = false;
await using var admin = NpgsqlDataSource.Create(options.ConnectionString);
var database = "botgram_test_intake_" + Guid.NewGuid().ToString("N");
await using (var create = admin.CreateCommand($"CREATE DATABASE {database}"))
    await create.ExecuteNonQueryAsync();
try
{
    options.Database = database;
    if (Environment.GetEnvironmentVariable("BOTGRAM_MIGRATION_TESTS") == "1")
        await SnapshotMigrationTests.RunAsync(options.ConnectionString);
    else if (Environment.GetEnvironmentVariable("BOTGRAM_SCHEMA_UPGRADE_TESTS") == "1")
    {
        try
        {
            await SchemaUpgradeTests.RunAsync(options.ConnectionString);
        }
        catch (Exception error)
        {
            await Console.Error.WriteLineAsync("FAIL: " + error);
            throw;
        }
    }
    else if (Environment.GetEnvironmentVariable("BOTGRAM_FULL_BACKEND_TESTS") == "1")
        await RelationalBackendTests.RunAsync(options.ConnectionString);
    else if (Environment.GetEnvironmentVariable("BOTGRAM_CAPACITY_DIAGNOSTIC") == "1")
        await CapacityDiagnostic.RunAsync(options.ConnectionString);
    else if (Environment.GetEnvironmentVariable("BOTGRAM_COMMIT_DIAGNOSTIC") == "1")
        await CommitDiagnostic.RunAsync(options.ConnectionString);
    else
    {
        await Run(options.ConnectionString);
        Console.WriteLine("PASS: PostgreSQL acceptance integration tests");
    }
}
finally
{
    await using var drop = admin.CreateCommand($"DROP DATABASE {database} WITH (FORCE)");
    await drop.ExecuteNonQueryAsync();
}

static async Task Run(string connectionString)
{
    MessageAcceptance Input(string key, string text = "hello", string user = "a", string session = "hash-a",
        MessageWriteContract contract = MessageWriteContract.Telegram) =>
        new(session, user, "chat", new(key, text), contract);
    var source = NpgsqlDataSource.Create(connectionString);
    try
    {
        var persistence = new PostgresMessageAcceptance(source);
        await persistence.InitializeAsync();
        await persistence.InitializeAsync();
        await using (var seed = source.CreateCommand("""
            INSERT INTO botgram_intake_v1.users(id) VALUES ('a'), ('b'), ('outsider');
            INSERT INTO botgram_intake_v1.sessions(token_hash, user_id, expires_at) VALUES
                ('hash-a', 'a', now() + interval '1 hour'), ('hash-a2', 'a', now() + interval '1 hour'),
                ('hash-b', 'b', now() + interval '1 hour'), ('hash-outsider', 'outsider', now() + interval '1 hour'),
                ('expired', 'a', now() - interval '1 hour');
            INSERT INTO botgram_intake_v1.conversations(id) VALUES ('chat'), ('other');
            INSERT INTO botgram_intake_v1.members(conversation_id, user_id) VALUES
                ('chat', 'a'), ('chat', 'b'), ('other', 'a'), ('other', 'b');
            """))
            await seed.ExecuteNonQueryAsync();

        async Task<AcceptedMessage> Accept(MessageAcceptance input) =>
            await persistence.AcceptAsync(input, result => result, CancellationToken.None);
        var parallel = await Task.WhenAll(Enumerable.Range(0, 16).Select(i =>
            Accept(Input("concurrent", session: i % 2 == 0 ? "hash-a" : "hash-a2"))));
        Check(parallel.Count(r => !r.Duplicate) == 1 && parallel.Select(r => r.Message.Id).Distinct().Count() == 1,
            "same-key concurrent acceptance");
        Check(parallel.All(r => r.Pts == 2), "duplicate pts");
        var stable = await Snapshot(source);
        await ExpectApi(() => Accept(Input("concurrent", "conflict")), 409, "RANDOM_ID_DUPLICATE");
        await ExpectApi(() => Accept(Input("concurrent") with { Command = new("concurrent", "hello", Silent: true) }),
            409, "RANDOM_ID_DUPLICATE");
        Check(stable == await Snapshot(source), "conflict changed state");

        try
        {
            await persistence.AcceptAsync<int>(Input("rollback"), _ => throw new ResponsePreparationFailed(), CancellationToken.None);
            throw new InvalidOperationException("Expected preparation failure");
        }
        catch (ResponsePreparationFailed) { }
        Check(stable == await Snapshot(source), "preparation exception did not atomically roll back");
        var afterRollback = await Accept(Input("rollback"));
        Check(!afterRollback.Duplicate && afterRollback.Pts == 3, "rollback key incorrectly persisted");

        // Only simulate losing the application response AFTER a successful commit, not an ambiguous commit failure.
        _ = await Accept(Input("lost-response"));
        stable = await Snapshot(source);
        var replay = await Accept(Input("lost-response"));
        Check(replay.Duplicate && replay.Pts == 4 && stable == await Snapshot(source), "lost response retry");
        await Accept(Input("later"));
        var replayed = await Accept(Input("lost-response"));
        Check(replayed.Duplicate && replayed.Pts == 4 && replayed.PtsCount == 1,
            "duplicate must return the original event pts, not the current cursor");

        var rest = await Accept(Input("rest", contract: MessageWriteContract.Rest));
        var restReplay = await Accept(Input("rest", contract: MessageWriteContract.Rest) with
        {
            Command = new("rest", "hello", [new(MessageEntityKind.Bold, 0, 2)], rest.Message.Id, true)
        });
        Check(restReplay.Duplicate, "REST deduplication must compare text only");
        await ExpectApi(() => Accept(Input("rest", "different", contract: MessageWriteContract.Rest)), 409, "idempotency_conflict");
        await ExpectApi(() => Accept(Input("", contract: MessageWriteContract.Rest)), 400, "invalid_client_message_id");
        await ExpectApi(() => Accept(Input(new string('x', 81), contract: MessageWriteContract.Rest)), 400, "invalid_client_message_id");
        await ExpectApi(() => Accept(Input("empty", " ")), 400, "MESSAGE_EMPTY");
        await ExpectApi(() => Accept(Input("entity") with { Command = new("entity", "hello", [new(MessageEntityKind.Bold, 5, 1)]) }),
            400, "ENTITY_BOUNDS_INVALID");
        await ExpectApi(() => Accept(Input("reply") with { Command = new("reply", "hello", ReplyId: long.MaxValue) }),
            400, "REPLY_MESSAGE_ID_INVALID");
        var typed = await Accept(Input("typed") with
        {
            Command = new("typed", "hello", [new(MessageEntityKind.TextUrl, 0, 5, Url: "https://example.test")], rest.Message.Id, true)
        });
        var typedReplay = await Accept(Input("typed") with
        {
            Command = new("typed", "hello", [new(MessageEntityKind.TextUrl, 0, 5, Url: "https://example.test")], rest.Message.Id, true)
        });
        Check(typedReplay.Duplicate && JsonSerializer.Serialize(typedReplay.Message) == JsonSerializer.Serialize(typed.Message),
            "typed message roundtrip");

        stable = await Snapshot(source);
        await ExpectApi(() => Accept(Input("forbidden", user: "outsider", session: "hash-outsider")), 404, "conversation_not_found");
        await ExpectApi(() => Accept(Input("expired", session: "expired")), 401, "session_expired");
        await ExpectApi(() => Accept(Input("wrong-user", user: "b")), 401, "session_expired");
        await ExpectApi(() => Accept(Input("missing-session", session: "missing")), 401, "session_expired");
        Check(stable == await Snapshot(source), "authorization failure changed state");

        await using (var revoke = source.CreateCommand("DELETE FROM botgram_intake_v1.sessions WHERE token_hash = 'hash-a2'"))
            await revoke.ExecuteNonQueryAsync();
        await ExpectApi(() => Accept(Input("revoked", session: "hash-a2")), 401, "session_expired");
        using (var cancelled = new CancellationTokenSource())
        {
            cancelled.Cancel();
            try
            {
                await persistence.AcceptAsync(Input("cancelled"), r => r, cancelled.Token);
                throw new InvalidOperationException("Expected cancellation");
            }
            catch (OperationCanceledException) { }
        }
        Check(stable == await Snapshot(source), "revocation or cancellation changed intake state");

        stable = await Snapshot(source);
        await using (var inject = source.CreateCommand("""
            CREATE FUNCTION botgram_intake_v1.reject_test_outbox() RETURNS trigger LANGUAGE plpgsql AS $$
            BEGIN RAISE EXCEPTION 'isolated outbox failure'; END $$;
            CREATE TRIGGER reject_test_outbox BEFORE INSERT ON botgram_intake_v1.outbox
            FOR EACH ROW EXECUTE FUNCTION botgram_intake_v1.reject_test_outbox();
            """))
            await inject.ExecuteNonQueryAsync();
        try
        {
            await Accept(Input("database-failure"));
            throw new InvalidOperationException("Expected database exception");
        }
        catch (PostgresException error) when (error.SqlState == "P0001") { }
        finally
        {
            await using var remove = source.CreateCommand("""
                DROP TRIGGER reject_test_outbox ON botgram_intake_v1.outbox;
                DROP FUNCTION botgram_intake_v1.reject_test_outbox();
                """);
            await remove.ExecuteNonQueryAsync();
        }
        Check(stable == await Snapshot(source), "database exception did not atomically roll back pts/event/message/outbox");
        Check(!(await Accept(Input("database-failure"))).Duplicate, "failed transaction retained idempotency key");

        // Distinct sessions/conversations with the same recipients exercise consistent sync lock ordering.
        await Task.WhenAll(Enumerable.Range(0, 12).Select(i => Accept(
            Input("cross-" + i, user: i % 2 == 0 ? "a" : "b", session: i % 2 == 0 ? "hash-a" : "hash-b") with
            { ConversationId = i % 2 == 0 ? "chat" : "other" })));
        await using (var invariant = source.CreateCommand("""
            SELECT (SELECT count(*) FROM botgram_intake_v1.messages) = (SELECT count(*) FROM botgram_intake_v1.events)
               AND (SELECT count(*) FROM botgram_intake_v1.events) = (SELECT count(*) FROM botgram_intake_v1.outbox)
               AND (SELECT count(*) * 2 FROM botgram_intake_v1.events) = (SELECT count(*) FROM botgram_intake_v1.event_recipients)
               AND NOT EXISTS (SELECT 1 FROM botgram_intake_v1.sync s WHERE s.pts <> 1 +
                   (SELECT count(*) FROM botgram_intake_v1.event_recipients r WHERE r.user_id = s.user_id))
               AND (SELECT value FROM botgram_intake_v1.event_cursor) = (SELECT max(id) FROM botgram_intake_v1.events)
               AND NOT EXISTS (SELECT 1 FROM botgram_intake_v1.event_recipients r JOIN botgram_intake_v1.event_recipients p
                   ON p.user_id = r.user_id AND p.event_id < r.event_id WHERE p.pts >= r.pts)
            """))
            Check((bool)(await invariant.ExecuteScalarAsync())!, "message/event/pts/outbox invariants");

        await using (var seedThree = source.CreateCommand("""
            INSERT INTO botgram_intake_v1.conversations(id) VALUES ('three');
            INSERT INTO botgram_intake_v1.members(conversation_id, user_id)
                VALUES ('three', 'a'), ('three', 'b'), ('three', 'outsider');
            """))
            await seedThree.ExecuteNonQueryAsync();
        async Task CheckThreeRecipientMapping(string key)
        {
            var previousPts = new Dictionary<string, int>();
            await using (var query = source.CreateCommand("""
                SELECT m.user_id, coalesce(s.pts, 1) FROM botgram_intake_v1.members m
                LEFT JOIN botgram_intake_v1.sync s ON s.user_id = m.user_id
                WHERE m.conversation_id = 'three'
                """))
            await using (var reader = await query.ExecuteReaderAsync())
                while (await reader.ReadAsync()) previousPts.Add(reader.GetString(0), reader.GetInt32(1));
            var accepted = await Accept(Input(key) with { ConversationId = "three" });
            var recipients = new Dictionary<string, int>();
            await using (var query = source.CreateCommand("""
                SELECT r.user_id, r.pts, s.pts FROM botgram_intake_v1.event_recipients r
                JOIN botgram_intake_v1.events e ON e.id = r.event_id
                JOIN botgram_intake_v1.sync s ON s.user_id = r.user_id WHERE e.message_id = $1
                """))
            {
                query.Parameters.AddWithValue(accepted.Message.Id);
                await using var reader = await query.ExecuteReaderAsync();
                while (await reader.ReadAsync())
                {
                    var user = reader.GetString(0);
                    var pts = reader.GetInt32(1);
                    Check(pts == previousPts[user] + 1 && pts == reader.GetInt32(2), "three-recipient user-to-pts mapping");
                    recipients.Add(user, pts);
                }
            }
            Check(!accepted.Duplicate && recipients.Count == 3 && recipients["a"] == accepted.Pts,
                "three-recipient acceptance response mapping");
        }
        await CheckThreeRecipientMapping("three-first");
        stable = await Snapshot(source);
        await using (var inject = source.CreateCommand("""
            CREATE FUNCTION botgram_intake_v1.reject_second_recipient() RETURNS trigger LANGUAGE plpgsql AS $$
            BEGIN
                IF NEW.user_id = 'b' AND EXISTS (
                    SELECT 1 FROM botgram_intake_v1.events e
                    JOIN botgram_intake_v1.messages m ON m.id = e.message_id
                    WHERE e.id = NEW.event_id AND m.conversation_id = 'three') THEN
                    RAISE EXCEPTION 'isolated second recipient failure';
                END IF;
                RETURN NEW;
            END $$;
            CREATE TRIGGER reject_second_recipient BEFORE INSERT ON botgram_intake_v1.event_recipients
            FOR EACH ROW EXECUTE FUNCTION botgram_intake_v1.reject_second_recipient();
            """))
            await inject.ExecuteNonQueryAsync();
        try
        {
            await Accept(Input("three-retry") with { ConversationId = "three" });
            throw new InvalidOperationException("Expected second-recipient insert failure");
        }
        catch (PostgresException error) when (error.SqlState == "P0001") { }
        finally
        {
            await using var remove = source.CreateCommand("""
                DROP TRIGGER reject_second_recipient ON botgram_intake_v1.event_recipients;
                DROP FUNCTION botgram_intake_v1.reject_second_recipient();
                """);
            await remove.ExecuteNonQueryAsync();
        }
        Check(stable == await Snapshot(source), "second-recipient failure did not atomically roll back all acceptance state");
        await CheckThreeRecipientMapping("three-retry");
    }
    finally { await source.DisposeAsync(); }

    await using var reopened = NpgsqlDataSource.Create(connectionString);
    var reopenedPersistence = new PostgresMessageAcceptance(reopened);
    var before = await Snapshot(reopened);
    var persisted = await reopenedPersistence.AcceptAsync(Input("lost-response"), r => r, CancellationToken.None);
    Check(persisted.Duplicate && before == await Snapshot(reopened), "durable deduplication after reopening datasource");
    await DeliveryTests.Run(connectionString);
    await BrokerProjectionTests.RunAsync(reopened, reopenedPersistence);
    await CrashRecoveryTests.RunAsync(connectionString);
    await CapacityTests.Run(connectionString);
}

static void Check(bool condition, string description)
{
    if (!condition) throw new InvalidOperationException(description);
}

static async Task ExpectApi(Func<Task<AcceptedMessage>> action, int status, string code)
{
    try { await action(); }
    catch (ApiException error) when (error.Status == status && error.Message == code) { return; }
    throw new InvalidOperationException($"Expected {status} {code}");
}

static async Task<string> Snapshot(NpgsqlDataSource source)
{
    await using var command = source.CreateCommand("""
        SELECT jsonb_build_array(
            (SELECT jsonb_agg(to_jsonb(m) ORDER BY id) FROM botgram_intake_v1.messages m),
            (SELECT jsonb_agg(to_jsonb(c) ORDER BY id) FROM botgram_intake_v1.conversations c),
            (SELECT jsonb_agg(to_jsonb(s) ORDER BY user_id) FROM botgram_intake_v1.sync s),
            (SELECT jsonb_agg(to_jsonb(e) ORDER BY id) FROM botgram_intake_v1.events e),
            (SELECT jsonb_agg(to_jsonb(r) ORDER BY event_id, user_id) FROM botgram_intake_v1.event_recipients r),
            (SELECT jsonb_agg(to_jsonb(o) ORDER BY event_id) FROM botgram_intake_v1.outbox o),
            (SELECT value FROM botgram_intake_v1.event_cursor))::text
        """);
    return (string)(await command.ExecuteScalarAsync())!;
}

sealed class ResponsePreparationFailed : Exception;
