using System.Collections.Concurrent;
using System.Diagnostics;
using System.Security.Cryptography;
using System.Text.Json;
using Botgram.Backend;
using Botgram.Backend.Storage;
using Npgsql;

static class CapacityDiagnostic
{
    private sealed record Sample(int Worker, int Index, double ElapsedMs, Dictionary<string, object?> Tags);

    public static async Task RunAsync(string connectionString)
    {
        var options = new NpgsqlConnectionStringBuilder(connectionString);
        if (!(options.Database?.StartsWith("botgram_test_intake_", StringComparison.Ordinal) ?? false))
            throw new InvalidOperationException("Capacity matrix requires the test runner's newly allocated database.");
        var timing = Environment.GetEnvironmentVariable("BOTGRAM_CAPACITY_TIMING") != "0";
        var output = Environment.GetEnvironmentVariable("BOTGRAM_CAPACITY_OUTPUT")
            ?? throw new InvalidOperationException("Set BOTGRAM_CAPACITY_OUTPUT to an isolated evidence file.");
        options.Pooling = false;
        options.ApplicationName = "botgram-capacity-observer";
        await using var observer = new NpgsqlConnection(options.ConnectionString);
        await observer.OpenAsync();
        var settings = new Dictionary<string, string>();
        foreach (var name in new[] { "server_version", "fsync", "synchronous_commit", "track_wal_io_timing", "wal_sync_method", "log_statement" })
        {
            await using var query = new NpgsqlCommand("SHOW " + name, observer);
            settings[name] = (string)(await query.ExecuteScalarAsync())!;
        }
        if (settings["fsync"] != "on" || settings["synchronous_commit"] != "on" ||
            settings["track_wal_io_timing"] != "on" || settings["log_statement"] != "none")
            throw new InvalidOperationException("Use durable settings, WAL timing and no full SQL logging.");
        options.Pooling = true;
        options.MinPoolSize = 0;
        options.MaxPoolSize = 20;
        options.ApplicationName = "botgram-capacity-writer";
        await using (var setup = NpgsqlDataSource.Create(options.ConnectionString))
            await new PostgresMessageAcceptance(setup).InitializeAsync();
        var groups = new List<object>();
        foreach (var grown in new[] { 0, 150 })
        foreach (var topology in new[] { "same-session", "same-user-same-chat", "same-user-cross-chat", "independent", "group" })
        foreach (var concurrency in new[] { 1, 2, 8 })
        {
            await using (var reset = new NpgsqlCommand("""
                TRUNCATE botgram_intake_v1.users, botgram_intake_v1.conversations RESTART IDENTITY CASCADE;
                UPDATE botgram_intake_v1.event_cursor SET value=0;
                """, observer))
                await reset.ExecuteNonQueryAsync();
            await Seed(observer, topology);
            var warmPids = new ConcurrentDictionary<int, byte>();
            await using (var warm = NpgsqlDataSource.Create(options.ConnectionString))
            {
                var acceptance = new PostgresMessageAcceptance(warm);
                for (var i = 0; i < grown + 10; i++)
                    await acceptance.AcceptAsync(Input(topology, i % 8, "warm-" + i), r => r, CancellationToken.None);
                await CommitDiagnostic.Preopen(warm, 1, warmPids);
            }
            await CommitDiagnostic.WaitForExit(observer, warmPids.Keys.ToArray());
            var pids = new ConcurrentDictionary<int, byte>();
            var samples = new ConcurrentBag<Sample>();
            var waits = new List<CommitDiagnostic.WaitSample>();
            var clock = new Stopwatch();
            var app = Process.GetCurrentProcess();
            TimeSpan appCpu;
            TimeSpan dbCpu = TimeSpan.Zero;
            long dbWorkingSet = 0;
            double wallMs;
            var walBefore = await CommitDiagnostic.ReadStableWal(observer);
            await using (var source = NpgsqlDataSource.Create(options.ConnectionString))
            {
                await CommitDiagnostic.Preopen(source, 20, pids);
                var backendBefore = pids.Keys.ToDictionary(pid => pid, pid =>
                {
                    using var process = Process.GetProcessById(pid);
                    return process.TotalProcessorTime;
                });
                app.Refresh();
                var cpuBefore = app.TotalProcessorTime;
                using var stop = new CancellationTokenSource();
                clock.Start();
                var sampling = timing ? CommitDiagnostic.Observe(observer, pids.Keys.ToArray(), clock, waits, stop.Token) : Task.CompletedTask;
                try
                {
                    var acceptance = new PostgresMessageAcceptance(source);
                    await Task.WhenAll(Enumerable.Range(0, concurrency).Select(async worker =>
                    {
                        for (var i = worker; i < 96; i += concurrency)
                        {
                            using var activity = timing ? new Activity("capacity-diagnostic").Start() : null;
                            activity?.SetTag("diag.enabled", true);
                            var started = Stopwatch.GetTimestamp();
                            using var deadline = new CancellationTokenSource(TimeSpan.FromSeconds(15));
                            var result = await acceptance.AcceptAsync(Input(topology, worker, "sample-" + i), r => r, deadline.Token);
                            var elapsed = Stopwatch.GetElapsedTime(started).TotalMilliseconds;
                            Check(!result.Duplicate, "unexpected matrix duplicate");
                            var tags = activity?.TagObjects.ToDictionary(t => t.Key, t => t.Value) ?? [];
                            if (timing)
                                Check(tags["diag.accept_sql_count"] is 16 &&
                                    tags["diag.accept_capacity_lock_key"] is "backlog-global-v1", "matrix SQL/key diagnostic");
                            samples.Add(new(worker, i, elapsed, tags));
                        }
                    }));
                    wallMs = clock.Elapsed.TotalMilliseconds;
                }
                finally { stop.Cancel(); await sampling; }
                app.Refresh();
                appCpu = app.TotalProcessorTime - cpuBefore;
                foreach (var pid in pids.Keys)
                {
                    using var process = Process.GetProcessById(pid);
                    dbCpu += process.TotalProcessorTime - backendBefore[pid];
                    dbWorkingSet += process.WorkingSet64;
                }
            }
            await CommitDiagnostic.WaitForExit(observer, pids.Keys.ToArray());
            var walAfter = await CommitDiagnostic.ReadStableWal(observer);
            Check(walBefore.StatsReset == walAfter.StatsReset, "WAL reset during group");
            await using (var invariant = new NpgsqlCommand("""
                SELECT (SELECT count(*) FROM botgram_intake_v1.messages)=$1
                    AND (SELECT count(*) FROM botgram_intake_v1.events)=$1
                    AND (SELECT count(*) FROM botgram_intake_v1.outbox)=$1
                    AND (SELECT value FROM botgram_intake_v1.event_cursor)=$1
                    AND NOT EXISTS (SELECT 1 FROM botgram_intake_v1.sync s WHERE s.pts<>
                        1+(SELECT count(*) FROM botgram_intake_v1.event_recipients r WHERE r.user_id=s.user_id))
                    AND NOT EXISTS (SELECT 1 FROM botgram_intake_v1.conversations c WHERE c.sequence<>
                        (SELECT count(*) FROM botgram_intake_v1.messages m WHERE m.conversation_id=c.id))
                    AND NOT EXISTS (SELECT 1 FROM (
                        SELECT pts, lag(pts) OVER (PARTITION BY user_id ORDER BY event_id) AS previous
                        FROM botgram_intake_v1.event_recipients) r WHERE previous>=pts)
                """, observer))
            {
                invariant.Parameters.AddWithValue((long)grown + 10 + 96);
                Check((bool)(await invariant.ExecuteScalarAsync())!, "matrix atomicity/pts/cursor/sequence");
            }
            var ordered = samples.OrderBy(s => s.Index).ToArray();
            var group = new
            {
                topology, concurrency, prefill = grown, warmup = 10, sampleCount = 96, minPoolSize = 0, maxPoolSize = 20,
                preopenedConnections = 20, timing, wallMs, throughputPerSecond = 96000 / wallMs,
                successes = ordered.Length, rejections = 0, unexpectedErrors = 0,
                elapsedMs = CommitDiagnostic.Summary(ordered.Select(s => s.ElapsedMs)),
                appCpuMs = appCpu.TotalMilliseconds, appWorkingSetBytes = app.WorkingSet64,
                dbWriterCpuMs = dbCpu.TotalMilliseconds, dbWriterWorkingSetBytes = dbWorkingSet,
                walBefore, walAfter,
                tagMs = ordered.SelectMany(s => s.Tags.Keys).Where(k => k.EndsWith("_ms", StringComparison.Ordinal))
                    .Distinct().Order().ToDictionary(k => k, k => CommitDiagnostic.Summary(ordered
                        .Where(s => s.Tags.ContainsKey(k)).Select(s => Convert.ToDouble(s.Tags[k])))),
                keyDistribution = ordered.Where(s => s.Tags.ContainsKey("diag.accept_capacity_lock_key"))
                    .GroupBy(s => s.Tags["diag.accept_capacity_lock_key"]?.ToString())
                    .Select(g => new { key = g.Key, requests = g.Count() }).ToArray(),
                waitCounts = waits.GroupBy(w => new { w.WaitType, w.WaitEvent })
                    .Select(g => new { g.Key.WaitType, g.Key.WaitEvent, count = g.Count() }).ToArray(),
                waitSamples = waits, requests = ordered
            };
            groups.Add(group);
            await File.WriteAllTextAsync(output, JsonSerializer.Serialize(new
            {
                capturedAtUtc = DateTimeOffset.UtcNow, settings, complete = groups.Count == 30,
                backendAssemblySha256 = Convert.ToHexString(SHA256.HashData(
                    await File.ReadAllBytesAsync(typeof(PostgresMessageAcceptance).Assembly.Location))),
                scope = "Direct acceptance adapter, not HTTP. Fresh identical dataset per group; fixed-size worker streams; two recipients except eight-recipient group. No detailed SQL log. Stages inclusive; lock-held measures client interval after acquisition through COMMIT API return. WAL cluster-wide; CPU covers this test process and writer backends, not WAL/background processes. No unexpected exception is swallowed; incomplete output is not PASS.",
                groups
            }, new JsonSerializerOptions { WriteIndented = true }));
            Console.WriteLine($"Capacity matrix {topology} grown={grown} c={concurrency}: {wallMs:F1}ms, {96000 / wallMs:F1}/s");
        }
        Console.WriteLine($"PASS: 30 capacity matrix groups / 2880 measured sends; raw evidence: {output}");
    }

    private static MessageAcceptance Input(string topology, int worker, string key)
    {
        var sender = topology is "independent" or "group" ? worker : 0;
        var chat = topology is "same-user-cross-chat" or "independent" ? worker : 0;
        return new($"session-{(topology == "same-session" ? 0 : worker)}", $"user-{sender}", $"chat-{chat}",
            new($"tl:{key}", "capacity diagnostic"), MessageWriteContract.Telegram);
    }

    private static async Task Seed(NpgsqlConnection connection, string topology)
    {
        await using (var seed = new NpgsqlCommand("""
            INSERT INTO botgram_intake_v1.users SELECT 'user-'||n FROM generate_series(0,15) n;
            INSERT INTO botgram_intake_v1.conversations SELECT 'chat-'||n FROM generate_series(0,7) n;
            """, connection))
            await seed.ExecuteNonQueryAsync();
        await using (var sessions = new NpgsqlCommand("""
            INSERT INTO botgram_intake_v1.sessions
                SELECT 'session-'||n, 'user-'||CASE WHEN $1 IN ('independent','group') THEN n ELSE 0 END,
                    now()+interval '1 hour' FROM generate_series(0,7) n
            """, connection))
        {
            sessions.Parameters.AddWithValue(topology);
            await sessions.ExecuteNonQueryAsync();
        }
        await using var command = new NpgsqlCommand("""
            INSERT INTO botgram_intake_v1.members
                SELECT 'chat-'||c, 'user-'||u FROM generate_series(0,7) c CROSS JOIN generate_series(0,15) u
                WHERE CASE WHEN $1='group' THEN u<8
                    WHEN $1='independent' THEN u=c OR u=c+8 ELSE u=0 OR u=c+8 END
            """, connection);
        command.Parameters.AddWithValue(topology);
        await command.ExecuteNonQueryAsync();
    }

    private static void Check(bool value, string message)
    { if (!value) throw new InvalidOperationException(message); }
}
