using System.Collections.Concurrent;
using System.Diagnostics;
using System.Text.Json;
using Botgram.Backend;
using Botgram.Backend.Storage;
using Npgsql;

static class CapacityTests
{
    private const string LockSql = "SELECT pg_advisory_xact_lock(7459241304231)";
    private const string BacklogSql = """
        SELECT count(*) FROM botgram_intake_v1.outbox o
        WHERE o.state <> 'Published' OR NOT EXISTS (
            SELECT 1 FROM botgram_intake_v1.projection_tasks t
            JOIN botgram_intake_v1.projection_heads h ON h.conversation_id=t.conversation_id AND h.generation=t.generation
            WHERE t.outbox_id=o.id AND t.state='Complete')
        """;
    private static int assertions;
    private static readonly ConcurrentBag<object> samples = [];

    public static async Task Run(string connectionString)
    {
        await using var source = NpgsqlDataSource.Create(new NpgsqlConnectionStringBuilder(connectionString)
        { ApplicationName = "capacity-fault-writer", MaxPoolSize = 20, CommandTimeout = 10 }.ConnectionString);
        await using (var seed = source.CreateCommand("""
            INSERT INTO botgram_intake_v1.users SELECT 'fault-user-'||n FROM generate_series(0,31) n;
            INSERT INTO botgram_intake_v1.sessions
                SELECT 'fault-session-'||n,'fault-user-'||n,now()+interval '1 hour' FROM generate_series(0,15) n;
            INSERT INTO botgram_intake_v1.conversations SELECT 'fault-chat-'||n FROM generate_series(0,15) n;
            INSERT INTO botgram_intake_v1.members
                SELECT 'fault-chat-'||n, 'fault-user-'||u
                FROM generate_series(0,15) n CROSS JOIN generate_series(0,31) u WHERE u=n OR u=n+16;
            """))
            await seed.ExecuteNonQueryAsync();
        var acceptance = new PostgresMessageAcceptance(source);
        foreach (var concurrency in new[] { 1, 2, 8 })
        {
            var before = await Snapshot(source);
            var backlog = await Count(source);
            var slots = Math.Max(1, concurrency / 2);
            var bounded = new PostgresMessageAcceptance(source, checked((int)backlog + slots));
            var gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            var tasks = Enumerable.Range(0, concurrency).Select(async worker =>
            {
                await gate.Task;
                try
                {
                    using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(15));
                    var result = await Trace("boundary", concurrency, worker, () =>
                        bounded.AcceptAsync(Input(worker, $"boundary-{concurrency}"), r => r, timeout.Token));
                    Check(!result.Duplicate, "new boundary message");
                    return result;
                }
                catch (ApiException error) when (error.Status == 503 && error.Message == "storage_backlog_full") { return null; }
            }).ToArray();
            gate.SetResult();
            var results = await Task.WhenAll(tasks);
            Check(results.Count(r => r is not null) == slots && await Count(source) == backlog + slots,
                "global concurrent capacity cap across independent users/conversations");
            var accepted = results.First(r => r is not null)!;
            var identityBeforeRetry = await LastMessageIdentity(source);
            var duplicate = await Trace("full-retry", concurrency, 0, () => bounded.AcceptAsync(Input(
                int.Parse(accepted.Message.SenderId["fault-user-".Length..]), $"boundary-{concurrency}"), r => r, CancellationToken.None));
            Check(duplicate.Duplicate && duplicate.Message.Id == accepted.Message.Id, "full backlog permits permanent retry");
            Check(await LastMessageIdentity(source) == identityBeforeRetry, "duplicate does not allocate an identity");
            Check(before != await Snapshot(source), "successful boundary sends persisted");
            var sameKey = "same-random-" + concurrency;
            var identityBeforeConcurrent = await LastMessageIdentity(source);
            var retries = await Task.WhenAll(Enumerable.Range(0, concurrency).Select(worker =>
                Trace("same-random-id", concurrency, worker, () =>
                    acceptance.AcceptAsync(Input(15, sameKey), r => r, CancellationToken.None))));
            Check(retries.Count(r => !r.Duplicate) == 1 && retries.Select(r => r.Message.Id).Distinct().Count() == 1,
                "concurrent random_id retries allocate one message");
            Check(retries.Select(r => r.Pts).Distinct().Count() == 1, "concurrent retries do not advance pts");
            Check(await LastMessageIdentity(source) == identityBeforeConcurrent + 1,
                "concurrent same-random retries allocate exactly one identity");
            await Atomicity(source);
        }

        foreach (var timedOut in new[] { false, true })
        {
            var stable = await Snapshot(source);
            await using var blocker = await source.OpenConnectionAsync();
            await using var held = await blocker.BeginTransactionAsync();
            await using (var acquire = new NpgsqlCommand(LockSql, blocker, held)) await acquire.ExecuteScalarAsync();
            var options = new NpgsqlConnectionStringBuilder(connectionString)
            {
                ApplicationName = "capacity-cancel-writer", Pooling = false,
                CommandTimeout = timedOut ? 1 : 10, CancellationTimeout = 1000
            };
            await using var writer = NpgsqlDataSource.Create(options.ConnectionString);
            using var cancel = new CancellationTokenSource(TimeSpan.FromSeconds(10));
            var input = Input(0, timedOut ? "lock-timeout" : "lock-cancel");
            var pending = Trace(timedOut ? "lock-timeout" : "lock-cancel", 1, 0, () =>
                new PostgresMessageAcceptance(writer).AcceptAsync(input, r => r, cancel.Token));
            await WaitForAdvisory(source, "capacity-cancel-writer");
            if (!timedOut) cancel.Cancel();
            try { await pending; throw new InvalidOperationException("Expected waiting writer cancellation/timeout"); }
            catch (OperationCanceledException) when (!timedOut && cancel.IsCancellationRequested) { }
            catch (NpgsqlException error) when (timedOut && error.InnerException is TimeoutException) { }
            Check(await Snapshot(source) == stable, "waiting cancellation/timeout rolls back message/pts/cursor/outbox");
            await held.RollbackAsync();
            var recovered = await Trace("retry-after-wait-failure", 1, 0, () =>
                acceptance.AcceptAsync(input, r => r, CancellationToken.None));
            Check(!recovered.Duplicate, "cancelled/timeout key can be retried after lock release");
        }

        var beforeFailure = await Snapshot(source);
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        using var release = new ManualResetEventSlim();
        var holding = Task.Run(async () => await Trace("holder-preparation-failure", 2, 0, () =>
            acceptance.AcceptAsync<int>(Input(1, "held-rollback"), _ =>
        {
            entered.SetResult();
            if (!release.Wait(TimeSpan.FromSeconds(10))) throw new TimeoutException("Test response gate");
            throw new PreparedResponseFailure();
        }, CancellationToken.None)));
        await entered.Task.WaitAsync(TimeSpan.FromSeconds(10));
        Task<AcceptedMessage>? waiting = null;
        try
        {
            waiting = Trace("waiter-after-holder-rollback", 2, 1, () =>
                acceptance.AcceptAsync(Input(2, "after-held-rollback"), r => r, CancellationToken.None));
            await WaitForAdvisory(source, "capacity-fault-writer");
            Check(!waiting.IsCompleted, "independent send waits on holder's global capacity lock");
        }
        finally { release.Set(); }
        try { await holding; throw new InvalidOperationException("Expected prepared-response failure"); }
        catch (PreparedResponseFailure) { }
        Check(waiting is not null && !(await waiting).Duplicate, "rollback releases lock for waiting send");
        Check(!(await Trace("retry-after-preparation-failure", 1, 0, () =>
            acceptance.AcceptAsync(Input(1, "held-rollback"), r => r, CancellationToken.None))).Duplicate,
            "rollback leaves no dedup key");
        Check(await Snapshot(source) != beforeFailure, "successful post-rollback sends persist");

        // Even recovery replay, which may exceed an admission limit, must not race a capacity decision.
        await using (var completed = source.CreateCommand("""
            SELECT id FROM botgram_intake_v1.outbox ORDER BY event_id LIMIT 1
            """))
        {
            var id = (Guid)(await completed.ExecuteScalarAsync())!;
            await using (var publish = source.CreateCommand("UPDATE botgram_intake_v1.outbox SET state='Published' WHERE id=$1"))
            { publish.Parameters.AddWithValue(id); await publish.ExecuteNonQueryAsync(); }
            await using var blocker = await source.OpenConnectionAsync();
            await using var transaction = await blocker.BeginTransactionAsync();
            await using (var acquire = new NpgsqlCommand(LockSql, blocker, transaction)) await acquire.ExecuteScalarAsync();
            var replay = Trace("replay-lock", 1, 0, () => new PostgresDeliveryPersistence(source).ReplayAsync(id, CancellationToken.None));
            try
            {
                await WaitForAdvisory(source, "capacity-fault-writer");
                Check(!replay.IsCompleted, "replay participates in global capacity serialization");
            }
            finally { await transaction.RollbackAsync(); }
            Check(await replay, "replay completes after capacity holder releases");
        }
        await Atomicity(source);
        var fixture = new PostgresRelationalValidation(source);
        await fixture.InitializeAsync();
        var beforeSeed = await Snapshot(source);
        foreach (var lockSql in new[] { LockSql, "SELECT pts FROM botgram_intake_v1.sync FOR UPDATE" })
        {
            await using var blocker = await source.OpenConnectionAsync();
            await using var transaction = await blocker.BeginTransactionAsync();
            await using (var acquire = new NpgsqlCommand(lockSql, blocker, transaction)) await acquire.ExecuteScalarAsync();
            using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            try
            {
                await Trace("offline-seed-rejects-online-lock", 2, 0, async () =>
                {
                    await fixture.SeedAsync(new Database(), timeout.Token);
                    return true;
                });
                throw new InvalidOperationException("Expected busy target rejection");
            }
            catch (InvalidOperationException error) when (error.Message == "Relational fixture seeding requires an idle target") { }
            catch (PostgresException error) when (error.SqlState == "55P03") { }
            Check(await Snapshot(source) == beforeSeed, "offline seed cannot wait on or change an active capacity/sync target");
            await transaction.RollbackAsync();
        }
        if (Environment.GetEnvironmentVariable("BOTGRAM_CAPACITY_TEST_OUTPUT") is { Length: > 0 } output)
            await File.WriteAllTextAsync(output, JsonSerializer.Serialize(new
            {
                assertions, scope = "Correctness fault cases, not a steady-state benchmark. Individual fault samples have n=1; do not infer tail latency from them. SQL count includes attempted commands; lock-held ends before rollback disposal on failures.",
                samples
            }, new JsonSerializerOptions { WriteIndented = true }));
        Console.WriteLine($"PASS: {assertions} capacity assertions; c=1/2/8, boundary, duplicate, cancel, timeout, rollback, replay lock, offline seed exclusion");
    }

    private static async Task<T> Trace<T>(string scenario, int concurrency, int worker, Func<Task<T>> action)
    {
        using var activity = new Activity("capacity-fault").Start();
        activity.SetTag("diag.enabled", true);
        var startedAtUtc = DateTimeOffset.UtcNow;
        var started = Stopwatch.GetTimestamp();
        var outcome = "success";
        try { return await action(); }
        catch (Exception error)
        {
            outcome = error is ApiException api ? $"{api.Status}:{api.Message}" : error.GetType().Name;
            throw;
        }
        finally
        {
            var tags = activity.TagObjects.ToDictionary(t => t.Key, t => t.Value);
            samples.Add(new
            {
                scenario, concurrency, worker, outcome, startedAtUtc,
                elapsedMs = Stopwatch.GetElapsedTime(started).TotalMilliseconds,
                attemptedSqlCount = tags.ContainsKey("diag.accept_backend_pid")
                    ? tags.Keys.Count(k => k.StartsWith("diag.accept_sql_", StringComparison.Ordinal) &&
                        k.EndsWith("_ms", StringComparison.Ordinal)) : (int?)null, tags
            });
        }
    }

    private static MessageAcceptance Input(int worker, string key) => new($"fault-session-{worker}",
        $"fault-user-{worker}", $"fault-chat-{worker}", new($"tl:{key}", "capacity fault test"), MessageWriteContract.Telegram);

    private static async Task WaitForAdvisory(NpgsqlDataSource source, string application)
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        await using var query = source.CreateCommand("""
            SELECT count(*) FROM pg_stat_activity a JOIN pg_locks l ON l.pid=a.pid
            WHERE a.datname=current_database() AND a.application_name=$1 AND a.wait_event='advisory'
                AND l.locktype='advisory' AND NOT l.granted
                AND l.classid=(7459241304231::bigint>>32)::oid
                AND l.objid=(7459241304231::bigint & 4294967295)::oid AND l.objsubid=1
            """);
        query.Parameters.AddWithValue(application);
        while ((long)(await query.ExecuteScalarAsync(timeout.Token))! == 0) await Task.Delay(5, timeout.Token);
        Check(true, "observed actual capacity key in pg_locks/pg_stat_activity");
    }

    private static async Task<long> Count(NpgsqlDataSource source)
    {
        await using var query = source.CreateCommand(BacklogSql);
        return (long)(await query.ExecuteScalarAsync())!;
    }

    private static async Task<long> LastMessageIdentity(NpgsqlDataSource source)
    {
        await using var query = source.CreateCommand("SELECT last_value FROM botgram_intake_v1.messages_id_seq");
        return (long)(await query.ExecuteScalarAsync())!;
    }

    private static async Task<string> Snapshot(NpgsqlDataSource source)
    {
        await using var query = source.CreateCommand("""
            SELECT jsonb_build_array(
                (SELECT jsonb_agg(to_jsonb(m) ORDER BY id) FROM botgram_intake_v1.messages m),
                (SELECT jsonb_agg(to_jsonb(c) ORDER BY id) FROM botgram_intake_v1.conversations c),
                (SELECT jsonb_agg(to_jsonb(s) ORDER BY user_id) FROM botgram_intake_v1.sync s),
                (SELECT jsonb_agg(to_jsonb(e) ORDER BY id) FROM botgram_intake_v1.events e),
                (SELECT jsonb_agg(to_jsonb(r) ORDER BY event_id,user_id) FROM botgram_intake_v1.event_recipients r),
                (SELECT jsonb_agg(to_jsonb(o) ORDER BY event_id) FROM botgram_intake_v1.outbox o),
                (SELECT value FROM botgram_intake_v1.event_cursor))::text
            """);
        return (string)(await query.ExecuteScalarAsync())!;
    }

    private static async Task Atomicity(NpgsqlDataSource source)
    {
        await using var query = source.CreateCommand("""
            SELECT (SELECT count(*) FROM botgram_intake_v1.messages)=(SELECT count(*) FROM botgram_intake_v1.events)
                AND (SELECT count(*) FROM botgram_intake_v1.messages)=(SELECT count(*) FROM botgram_intake_v1.outbox)
                AND (SELECT value FROM botgram_intake_v1.event_cursor)=(SELECT count(*) FROM botgram_intake_v1.events)
                AND NOT EXISTS (SELECT 1 FROM botgram_intake_v1.sync s WHERE s.pts<>
                    1+(SELECT count(*) FROM botgram_intake_v1.event_recipients r WHERE r.user_id=s.user_id))
                AND NOT EXISTS (SELECT 1 FROM botgram_intake_v1.conversations c WHERE c.sequence<>
                    (SELECT count(*) FROM botgram_intake_v1.messages m WHERE m.conversation_id=c.id))
            """);
        Check((bool)(await query.ExecuteScalarAsync())!, "atomic message/event/outbox/sequence/pts/cursor");
    }

    private static void Check(bool value, string message)
    {
        if (!value) throw new InvalidOperationException(message);
        Interlocked.Increment(ref assertions);
    }

    private sealed class PreparedResponseFailure : Exception;
}
