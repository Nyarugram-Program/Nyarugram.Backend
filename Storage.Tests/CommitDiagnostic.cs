using System.Collections.Concurrent;
using System.Diagnostics;
using System.Text.Json;
using Botgram.Backend;
using Botgram.Backend.Storage;
using Npgsql;

static class CommitDiagnostic
{
    sealed record RequestSample(int Worker, int Index, double ElapsedMs, Dictionary<string, object?> Tags);
    internal sealed record WaitSample(double AtMs, int Pid, string State, string? WaitType, string? WaitEvent);
    internal sealed record WalSnapshot(decimal Records, decimal Bytes, decimal Writes, decimal Syncs,
        double WriteTimeMs, double SyncTimeMs, string Lsn, DateTime StatsReset);

    public static async Task RunAsync(string connectionString)
    {
        var observerOptions = new NpgsqlConnectionStringBuilder(connectionString)
        {
            Pooling = false, ApplicationName = "botgram-commit-observer"
        };
        await using var observer = new NpgsqlConnection(observerOptions.ConnectionString);
        await observer.OpenAsync();
        var settings = new Dictionary<string, string>();
        foreach (var name in new[] { "server_version", "fsync", "synchronous_commit", "track_wal_io_timing", "wal_sync_method" })
        {
            await using var command = new NpgsqlCommand($"SHOW {name}", observer);
            settings[name] = (string)(await command.ExecuteScalarAsync())!;
        }
        if (settings["fsync"] != "on" || settings["synchronous_commit"] != "on" || settings["track_wal_io_timing"] != "on")
            throw new InvalidOperationException("Diagnostic requires fsync, synchronous_commit and track_wal_io_timing all on; no durability settings are changed.");

        var groups = new List<object>();
        var actualSqlCounts = new HashSet<int>();
        foreach (var concurrency in new[] { 1, 2, 8 })
        {
            var prefix = "diag" + concurrency;
            var warmPids = new ConcurrentDictionary<int, byte>();
            await using (var setupSource = CreateSource(connectionString, 2))
                await new PostgresMessageAcceptance(setupSource).InitializeAsync();
            await using (var warmSource = CreateSource(connectionString, concurrency))
            {
                var acceptance = new PostgresMessageAcceptance(warmSource);
                await using (var seed = warmSource.CreateCommand("""
                    WITH users_added AS (
                        INSERT INTO botgram_intake_v1.users(id) VALUES ($1), ($2) RETURNING id
                    ), session_added AS (
                        INSERT INTO botgram_intake_v1.sessions(token_hash, user_id, expires_at)
                        SELECT $3, id, now() + interval '1 hour' FROM users_added WHERE id = $1
                    ), conversation_added AS (
                        INSERT INTO botgram_intake_v1.conversations(id) VALUES ($4) RETURNING id
                    )
                    INSERT INTO botgram_intake_v1.members(conversation_id, user_id)
                        SELECT c.id, u.id FROM conversation_added c CROSS JOIN users_added u
                    """))
                {
                    for (var worker = 0; worker < concurrency; worker++)
                    {
                        seed.Parameters.Clear();
                        foreach (var value in new[] { $"{prefix}-sender-{worker}", $"{prefix}-recipient-{worker}",
                            $"{prefix}-session-{worker}", $"{prefix}-chat-{worker}" })
                            seed.Parameters.Add(new NpgsqlParameter { Value = value });
                        await seed.ExecuteNonQueryAsync();
                    }
                }
                await Preopen(warmSource, concurrency, warmPids);
                await Task.WhenAll(Enumerable.Range(0, concurrency).Select(async worker =>
                {
                    for (var i = 0; i < 10; i++)
                        await Accept(acceptance, prefix, worker, i, "warm", warmPids);
                }));
            }
            await WaitForExit(observer, warmPids.Keys.ToArray());

            var pids = new ConcurrentDictionary<int, byte>();
            var samples = new ConcurrentBag<RequestSample>();
            var waits = new List<WaitSample>();
            var rtt = new List<double>();
            WalSnapshot before;
            var clock = new Stopwatch();
            double sampleWallMs;
            await using (var source = CreateSource(connectionString, concurrency))
            {
                // Preopen all physical writer connections without doing writes in the measured interval.
                await Preopen(source, concurrency, pids);
                before = await ReadStableWal(observer);
                using var stop = new CancellationTokenSource();
                clock.Start();
                var sampling = Observe(observer, pids.Keys.ToArray(), clock, waits, stop.Token);
                try
                {
                    var acceptance = new PostgresMessageAcceptance(source);
                    await Task.WhenAll(Enumerable.Range(0, concurrency).Select(async worker =>
                    {
                        for (var i = worker; i < 96; i += concurrency)
                            samples.Add(await Accept(acceptance, prefix, worker, i, "sample", pids));
                    }));
                    sampleWallMs = clock.Elapsed.TotalMilliseconds;
                }
                finally
                {
                    stop.Cancel();
                    await sampling;
                }
            }
            // Disposal closes physical pooled connections. Backend exit flushes their local WAL counters.
            await WaitForExit(observer, pids.Keys.ToArray());
            var after = await ReadStableWal(observer);
            if (before.StatsReset != after.StatsReset)
                throw new InvalidOperationException("WAL statistics were reset during a diagnostic group.");
            await using (var difference = new NpgsqlCommand("SELECT pg_wal_lsn_diff($1::pg_lsn, $2::pg_lsn)", observer))
            {
                difference.Parameters.AddWithValue(after.Lsn);
                difference.Parameters.AddWithValue(before.Lsn);
                var lsnBytes = (decimal)(await difference.ExecuteScalarAsync())!;
                for (var i = 0; i < 106; i++)
                {
                    var started = Stopwatch.GetTimestamp();
                    await using var ping = new NpgsqlCommand("SELECT 1", observer);
                    await ping.ExecuteScalarAsync();
                    if (i >= 10) rtt.Add(Stopwatch.GetElapsedTime(started).TotalMilliseconds);
                }
                var ordered = samples.OrderBy(s => s.Index).ToArray();
                actualSqlCounts.UnionWith(ordered.Select(s => Convert.ToInt32(s.Tags["diag.accept_sql_count"])));
                groups.Add(new
                {
                    concurrency, warmupPerWorker = 10, sampleCount = 96, independentSessions = true,
                    independentConversations = true, independentRecipients = true,
                    writerPids = pids.Keys.Order().ToArray(), warmWriterPids = warmPids.Keys.Order().ToArray(),
                    sampleWallMs, before, after,
                    walDelta = new
                    {
                        records = after.Records - before.Records, bytes = after.Bytes - before.Bytes,
                        writes = after.Writes - before.Writes, syncs = after.Syncs - before.Syncs,
                        writeTimeMs = after.WriteTimeMs - before.WriteTimeMs,
                        syncTimeMs = after.SyncTimeMs - before.SyncTimeMs, lsnBytes
                    },
                    elapsedMs = Summary(ordered.Select(s => s.ElapsedMs)),
                    tagMs = ordered.SelectMany(s => s.Tags.Keys).Where(k => k.EndsWith("_ms", StringComparison.Ordinal))
                        .Distinct().Order().ToDictionary(k => k, k => Summary(ordered.Where(s => s.Tags.ContainsKey(k))
                            .Select(s => Convert.ToDouble(s.Tags[k])))),
                    select1RttMs = Summary(rtt), select1RttSamplesMs = rtt,
                    waitCounts = waits.GroupBy(w => new { w.State, w.WaitType, w.WaitEvent })
                        .Select(g => new { g.Key.State, g.Key.WaitType, g.Key.WaitEvent, count = g.Count() }).ToArray(),
                    waitSamples = waits, requests = ordered
                });
            }
        }
        var directory = OperatingSystem.IsWindows() ? "D:/BotgramBuild/tmp" : "/mnt/d/BotgramBuild/tmp";
        Directory.CreateDirectory(directory);
        var path = Path.Combine(directory, $"commit-wal-diagnostic-{DateTime.UtcNow:yyyyMMdd-HHmmss}-{Guid.NewGuid():N}.json");
        await File.WriteAllTextAsync(path, JsonSerializer.Serialize(new
        {
            capturedAtUtc = DateTimeOffset.UtcNow, settings,
            expectedSqlCount = 16,
            actualSqlCounts = actualSqlCounts.Order().ToArray(),
            backendAssemblySha256 = Convert.ToHexString(System.Security.Cryptography.SHA256.HashData(
                await File.ReadAllBytesAsync(typeof(PostgresMessageAcceptance).Assembly.Location))),
            limitations = new[]
            {
                "Only a newly created botgram_test_intake_* database is used; PostgreSQL instance isolation must be provided by the caller.",
                "pg_stat_wal and WAL insert LSN are cluster-wide, not database/request-specific; other cluster activity and background WAL may contribute.",
                "Warmup and sample writer pools are disposed and all known backend PIDs must exit before statistics are read; stable reads also allow statistics publication to settle.",
                "Sample writer connections are preopened after warmup pool disposal; this is not a cold pool latency benchmark.",
                "Observer sampling requests a 1 ms delay between queries, not an exact 1 ms period; short waits may be missed, and instrumentation adds load.",
                "Wait samples are not exact per-request IO or COMMIT attribution; no per-request IO time is inferred by subtraction.",
                "COMMIT wall time includes server scheduling, durability/group commit, transport and client continuation; its residual is not pure network time.",
                "SELECT 1 RTT is a separate sequential observer-connection reference after each group, not concurrent request transport latency.",
                "Independent sessions/conversations/recipients still encounter the adapter's shared capacity advisory lock and event cursor. Stage timings are inclusive; do not sum parent and child stages."
            }, groups
        }, new JsonSerializerOptions { WriteIndented = true }));
        Console.WriteLine($"COMMIT/WAL diagnostic JSON: {path}");
    }

    static NpgsqlDataSource CreateSource(string connectionString, int concurrency) => NpgsqlDataSource.Create(
        new NpgsqlConnectionStringBuilder(connectionString)
        {
            ApplicationName = "botgram-commit-writer", MaxPoolSize = concurrency
        }.ConnectionString);

    internal static async Task Preopen(NpgsqlDataSource source, int count, ConcurrentDictionary<int, byte> pids)
    {
        var connections = new List<NpgsqlConnection>();
        try
        {
            for (var i = 0; i < count; i++)
            {
                var connection = await source.OpenConnectionAsync();
                connections.Add(connection);
                pids.TryAdd(connection.ProcessID, 0);
            }
        }
        finally { foreach (var connection in connections) await connection.DisposeAsync(); }
    }

    static async Task<RequestSample> Accept(PostgresMessageAcceptance acceptance, string prefix, int worker,
        int index, string phase, ConcurrentDictionary<int, byte> pids)
    {
        using var activity = new Activity("commit-diagnostic").Start();
        activity.SetTag("diag.enabled", true);
        var started = Stopwatch.GetTimestamp();
        var result = await acceptance.AcceptAsync(new MessageAcceptance($"{prefix}-session-{worker}",
            $"{prefix}-sender-{worker}", $"{prefix}-chat-{worker}", new($"{phase}-{index}", "commit diagnostic"),
            MessageWriteContract.Telegram), r => r, CancellationToken.None);
        var elapsed = Stopwatch.GetElapsedTime(started).TotalMilliseconds;
        if (result.Duplicate) throw new InvalidOperationException("Diagnostic sample unexpectedly duplicated.");
        var tags = activity.TagObjects.ToDictionary(t => t.Key, t => t.Value);
        if (activity.GetTagItem("diag.accept_backend_pid") is not int pid ||
            activity.GetTagItem("diag.accept_commit_ms") is not double)
            throw new InvalidOperationException("Missing acceptance diagnostic tags.");
        if (activity.GetTagItem("diag.accept_sql_count") is not int sqlCount || sqlCount != 16)
            throw new InvalidOperationException($"Expected acceptance SQL baseline 16, got {activity.GetTagItem("diag.accept_sql_count")}.");
        pids.TryAdd(pid, 0);
        return new(worker, index, elapsed, tags);
    }

    internal static async Task Observe(NpgsqlConnection observer, int[] pids, Stopwatch clock,
        List<WaitSample> samples, CancellationToken cancellationToken)
    {
        try
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                await using (var query = new NpgsqlCommand("""
                    SELECT pid, state, wait_event_type, wait_event FROM pg_stat_activity WHERE pid = ANY($1)
                    """, observer))
                {
                    query.Parameters.AddWithValue(pids);
                    await using var reader = await query.ExecuteReaderAsync(cancellationToken);
                    var at = clock.Elapsed.TotalMilliseconds;
                    while (await reader.ReadAsync(cancellationToken))
                        samples.Add(new(at, reader.GetInt32(0), reader.IsDBNull(1) ? "unknown" : reader.GetString(1),
                            reader.IsDBNull(2) ? null : reader.GetString(2), reader.IsDBNull(3) ? null : reader.GetString(3)));
                }
                await Task.Delay(1, cancellationToken);
            }
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested) { }
    }

    internal static async Task WaitForExit(NpgsqlConnection observer, int[] pids)
    {
        var timer = Stopwatch.StartNew();
        while (true)
        {
            await using var query = new NpgsqlCommand("SELECT count(*) FROM pg_stat_activity WHERE pid = ANY($1)", observer);
            query.Parameters.AddWithValue(pids);
            if ((long)(await query.ExecuteScalarAsync())! == 0) return;
            if (timer.Elapsed > TimeSpan.FromSeconds(15)) throw new TimeoutException("Writer backends did not exit; WAL statistics are not reliable.");
            await Task.Delay(20);
        }
    }

    internal static async Task<WalSnapshot> ReadStableWal(NpgsqlConnection observer)
    {
        WalSnapshot? previous = null;
        var stable = 0;
        for (var attempt = 0; attempt < 50; attempt++)
        {
            await using (var clear = new NpgsqlCommand("SELECT pg_stat_clear_snapshot()", observer))
                await clear.ExecuteNonQueryAsync();
            await using var query = new NpgsqlCommand("""
                SELECT wal_records::numeric, wal_bytes, wal_write::numeric, wal_sync::numeric,
                    wal_write_time, wal_sync_time, pg_current_wal_insert_lsn()::text, stats_reset
                FROM pg_stat_wal
                """, observer);
            await using (var reader = await query.ExecuteReaderAsync())
            {
                await reader.ReadAsync();
                var snapshot = new WalSnapshot(reader.GetDecimal(0), reader.GetDecimal(1), reader.GetDecimal(2),
                    reader.GetDecimal(3), reader.GetDouble(4), reader.GetDouble(5), reader.GetString(6), reader.GetDateTime(7));
                stable = snapshot == previous ? stable + 1 : 0;
                previous = snapshot;
                if (stable >= 3) return snapshot;
            }
            await Task.Delay(100);
        }
        throw new TimeoutException("Cluster WAL counters did not stabilize; run with an isolated PostgreSQL instance.");
    }

    internal static object Summary(IEnumerable<double> values)
    {
        var sorted = values.Order().ToArray();
        double Percentile(double p) => sorted[(int)Math.Ceiling(p * sorted.Length) - 1];
        return new { count = sorted.Length, min = sorted[0], mean = sorted.Average(),
            p50 = Percentile(.5), p95 = Percentile(.95), p99 = Percentile(.99), max = sorted[^1] };
    }
}
