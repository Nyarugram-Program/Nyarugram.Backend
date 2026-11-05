using System.Diagnostics;
using System.Net;
using System.Text.Json;
using System.Text.Json.Nodes;
using Npgsql;
using P = Botgram.Protocol.V1;

internal static partial class HttpTests
{
    private static JsonNode NormalizeStorageResponse(P.RpcResponse response)
    {
        var node = JsonNode.Parse(Google.Protobuf.JsonFormatter.Default.Format(response))!;
        void Walk(JsonNode? current)
        {
            if (current is JsonObject obj)
            {
                if (obj["isSelf"]?.GetValue<bool>() == true)
                {
                    var until = obj["status"]?["onlineUntilMs"];
                    long now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
                    Check(until is not null && long.TryParse(until.ToString(), out var expiry)
                        && expiry > now && expiry <= now + 60_000, "storage response self online expiry");
                    obj["status"]!["onlineUntilMs"] = "clock";
                }
                foreach (var (key, value) in obj.ToArray())
                {
                    // Only wall clocks vary between replays; retain all business fields, including pts.
                    if (key is "dateMs" or "createdAtMs" or "updatedAtMs" or "joinedAtMs")
                    {
                        Check(value is not null && long.TryParse(value.ToString(), out var clock) && clock > 0,
                            "storage response valid clock " + key);
                        obj[key] = "clock";
                    }
                    else Walk(value);
                }
            }
            else if (current is JsonArray array) foreach (var item in array) Walk(item);
        }
        Walk(node);
        return node;
    }

    private static async Task<List<object>> StorageLatency(string connection, string seed)
    {
        var sendMatrix = Environment.GetEnvironmentVariable("BOTGRAM_SEND_MATRIX") == "1";
        var sendDiagnostic = Environment.GetEnvironmentVariable("BOTGRAM_SEND_DIAGNOSTIC") == "1";
        var warmup = sendMatrix || sendDiagnostic ? 10 : 5;
        var samples = sendMatrix ? 96 : sendDiagnostic ? 100 : 40;
        var results = new List<object>();
        foreach (var scenario in sendMatrix
            ? new (string Dataset, int Concurrency)[] { ("small", 1), ("small", 2), ("small", 8), ("grown", 1), ("grown", 2), ("grown", 8) }
            : new[] { (Dataset: "small", Concurrency: 1) })
        foreach (var operation in sendMatrix || sendDiagnostic ? new[] { "SendMessage" } : new[] { "GetState", "GetHistory", "SendMessage" })
        {
            var expected = new List<JsonNode>();
            var expectedChecks = new List<JsonNode>();
            foreach (var storage in sendMatrix ? new[] { "LegacyOnly", "PG-relational-pooled" }
                : sendDiagnostic ? new[] { "PG-relational-pooled" } : new[] { "LegacyOnly", "PG-snapshot-bridge-pooled", "PG-relational-pooled" })
            {
                var relational = storage == "PG-relational-pooled";
                var pg = storage != "LegacyOnly";
                var path = Path.Combine(Root, sendMatrix
                    ? $"latency-matrix-{scenario.Dataset}-{scenario.Concurrency}-{storage}.json"
                    : $"latency-{operation}-{storage}.json");
                var options = new NpgsqlConnectionStringBuilder(connection) { Pooling = true, MinPoolSize = 0, MaxPoolSize = 20 };
                await using var admin = NpgsqlDataSource.Create(connection);
                string? database = null;
                try
                {
                    if (relational)
                    {
                        database = "botgram_test_latency_" + Guid.NewGuid().ToString("N");
                        await using var create = admin.CreateCommand("CREATE DATABASE " + database);
                        await create.ExecuteNonQueryAsync();
                        options.Database = database;
                    }
                    else if (pg)
                    {
                        await using var source = NpgsqlDataSource.Create(options.ConnectionString);
                        await using var reset = source.CreateCommand("UPDATE botgram_validation_v1.state SET payload=$1");
                        reset.Parameters.AddWithValue(NpgsqlTypes.NpgsqlDbType.Jsonb, File.ReadAllText(seed));
                        Check(await reset.ExecuteNonQueryAsync() == 1, "latency restores identical seed");
                    }
                    else File.Copy(seed, path);
                    var times = new List<double>();
                    var rawSamples = new List<object>();
                    await using (var server = await Server.Start(path, validationConnection: pg ? options.ConnectionString : null,
                        validationSeed: pg ? seed : null, relational: relational))
                    {
                        if (sendMatrix)
                        {
                            var grownMessages = scenario.Dataset == "grown" ? 150 : 0;
                            var nextRequest = 0;
                            P.RpcRequest MatrixRequest()
                            {
                                var index = nextRequest++;
                                var request = Send(9_000_000L + index, "storage latency sample");
                                request.RequestId = $"send_matrix_{scenario.Dataset}_{scenario.Concurrency}_{storage}_{index:0000}";
                                return request;
                            }
                            void Validate(P.RpcRequest request, byte[] body, int status, string? contentType)
                            {
                                Check(status == 200 && contentType == Media, "send matrix HTTP protobuf success");
                                var decoded = P.RpcResponse.Parser.ParseFrom(body);
                                Check(decoded.ProtocolMajor == 1 && decoded.RequestId == request.RequestId, "send matrix envelope correlation");
                                Check(decoded.OutcomeCase == P.RpcResponse.OutcomeOneofCase.SendMessage, "send matrix successful SendMessage outcome");
                                var sent = decoded.SendMessage;
                                Check(sent.Message is not null && sent.Message.MessageId > 0 &&
                                    sent.Message.Text?.Text == request.SendMessage.Text && sent.Mapping is not null &&
                                    sent.Mapping.RandomId == request.SendMessage.RandomId &&
                                    sent.Mapping.MessageId == sent.Message.MessageId, "send matrix complete send response");
                            }
                            for (int i = 0; i < grownMessages + warmup; i++)
                            {
                                var request = MatrixRequest();
                                var response = await server.BenchmarkRequest(true, Google.Protobuf.MessageExtensions.ToByteArray(request), sampleResources: false);
                                Validate(request, response.Body, response.Status, response.ContentType);
                            }
                            for (int i = 0; i < samples; i += scenario.Concurrency)
                            {
                                var requests = Enumerable.Range(0, scenario.Concurrency).Select(_ => MatrixRequest()).ToArray();
                                var responses = await Task.WhenAll(requests.Select(request => server.BenchmarkRequest(true,
                                    Google.Protobuf.MessageExtensions.ToByteArray(request), sampleResources: false)));
                                for (int j = 0; j < responses.Length; j++)
                                {
                                    var response = responses[j];
                                    Validate(requests[j], response.Body, response.Status, response.ContentType);
                                    times.Add(response.Milliseconds);
                                    rawSamples.Add(new { requestId = requests[j].RequestId, elapsedMs = response.Milliseconds });
                                }
                            }
                            times.Sort();
                            var p50 = times[(int)Math.Ceiling(samples * .50) - 1];
                            var matrixP95 = times[(int)Math.Ceiling(samples * .95) - 1];
                            var matrixP99 = times[(int)Math.Ceiling(samples * .99) - 1];
                            results.Add(new { operation, storage, dataset = scenario.Dataset, grownMessages, warmup, samples,
                                concurrency = scenario.Concurrency, pooling = pg, minPoolSize = pg ? 0 : (int?)null,
                                maxPoolSize = pg ? 20 : (int?)null, p50Ms = p50, p95Ms = matrixP95, p99Ms = matrixP99,
                                sortedSamplesMs = times, rawSamples,
                                scope = "same alice-to-bob conversation only; different conversations not covered; fixed-size concurrent batches; loopback diagnostic, not production acceptance" });
                            Console.WriteLine($"Send matrix {storage} {scenario.Dataset}: concurrency={scenario.Concurrency} n={samples} p50={p50:F2} p95={matrixP95:F2} p99={matrixP99:F2}ms; same conversation only, different conversations not covered");
                        }
                        else
                        for (int i = 0; i < warmup + samples; i++)
                        {
                            var request = operation switch
                            {
                                "GetState" => Req(new P.GetStateRequest()),
                                "GetHistory" => Req(new P.GetHistoryRequest { Peer = Peer("bob"), Limit = 100 }),
                                _ => Send(8_000_000L + i, "storage latency sample")
                            };
                            if (sendDiagnostic) request.RequestId = $"send_diag_{i:000}";
                            var response = await server.BenchmarkRequest(true, Google.Protobuf.MessageExtensions.ToByteArray(request), sampleResources: false);
                            Check(response.Status == 200 && response.ContentType == Media, "storage latency HTTP protobuf success");
                            var decoded = P.RpcResponse.Parser.ParseFrom(response.Body);
                            Check(decoded.ProtocolMajor == 1 && decoded.RequestId == request.RequestId, "storage latency envelope correlation");
                            Check((int)decoded.OutcomeCase == (int)request.OperationCase, "storage latency correct operation");
                            if (!sendDiagnostic)
                            {
                                var normalized = NormalizeStorageResponse(decoded);
                                if (!pg) expected.Add(normalized);
                                else Equal(expected[i], normalized, $"{storage} {operation} response {i}");
                            }
                            if (i >= warmup)
                            {
                                times.Add(response.Milliseconds);
                                if (sendDiagnostic) rawSamples.Add(new { requestId = request.RequestId, elapsedMs = response.Milliseconds });
                            }
                        }
                    }
                    if (sendMatrix) continue;
                    // Restart the same store: all checks below are outside the timed sample set.
                    if (!sendDiagnostic)
                    await using (var server = await Server.Start(path, validationConnection: pg ? options.ConnectionString : null,
                        validationSeed: pg && !relational ? seed : null, relational: relational))
                    {
                        int checkIndex = 0;
                        async Task Compare(P.RpcRequest request, string actor = "alice", int status = 200, string? code = null)
                        {
                            var response = NormalizeStorageResponse(await server.Rpc(request, actor, status, code));
                            if (!pg) expectedChecks.Add(response);
                            else Equal(expectedChecks[checkIndex], response, $"{storage} {operation} restart/check {checkIndex}");
                            checkIndex++;
                        }
                        if (operation == "SendMessage")
                        {
                            var state = NormalizeStorageResponse(await server.Rpc(Req(new P.GetStateRequest())));
                            var history = NormalizeStorageResponse(await server.Rpc(Req(new P.GetHistoryRequest { Peer = Peer("bob"), Limit = 100 })));
                            await Compare(Send(8_000_000L, "storage latency sample"));
                            await Compare(Send(8_000_000L, "conflicting payload"), status: 409, code: "RANDOM_ID_DUPLICATE");
                            Equal(state, NormalizeStorageResponse(await server.Rpc(Req(new P.GetStateRequest()))), storage + " retry/conflict does not advance pts");
                            Equal(history, NormalizeStorageResponse(await server.Rpc(Req(new P.GetHistoryRequest { Peer = Peer("bob"), Limit = 100 }))), storage + " retry/conflict creates no duplicate");
                        }
                        await Compare(Req(new P.GetStateRequest()));
                        long offset = 0;
                        var seen = new HashSet<long>();
                        while (true)
                        {
                            var request = Req(new P.GetHistoryRequest { Peer = Peer("bob"), Limit = 10, OffsetMessageId = offset });
                            var response = await server.Rpc(request);
                            var normalized = NormalizeStorageResponse(response);
                            if (!pg) expectedChecks.Add(normalized);
                            else Equal(expectedChecks[checkIndex], normalized, $"{storage} {operation} history page {checkIndex}");
                            checkIndex++;
                            var messages = response.GetHistory.Messages;
                            if (messages.Count == 0) break;
                            Check(messages.All(m => seen.Add(m.MessageId)), storage + " pagination no duplicate IDs");
                            var next = messages.Min(m => m.MessageId);
                            Check(offset == 0 || next < offset, storage + " pagination advances");
                            offset = next;
                        }
                        var group = JsonNode.Parse(File.ReadAllText(seed))!["conversations"]!.AsArray()
                            .Single(c => Str(c!, "kind") == "group")!["id"]!.GetValue<string>();
                        await Compare(Req(new P.GetHistoryRequest { Peer = new P.PeerRef { GroupId = group } }), "outsider", 404, "PEER_ID_INVALID");
                        await Compare(new P.RpcRequest { ProtocolMajor = 1, RequestId = "unknown" }, status: 501, code: "BOTGRAM_RPC_UNSUPPORTED");
                        if (relational)
                            await server.Rpc(Req(new P.GetDialogsRequest()), status: 501, code: "BOTGRAM_RPC_UNSUPPORTED:GetDialogs");
                        Check(checkIndex == expectedChecks.Count, storage + " same number of complete response checkpoints");
                    }
                    times.Sort();
                    var median = times[(samples - 1) / 2];
                    var p95 = times[(int)Math.Ceiling(samples * .95) - 1];
                    var p99 = times[(int)Math.Ceiling(samples * .99) - 1];
                    if (sendDiagnostic)
                        results.Add(new { operation, storage, warmup, samples, concurrency = 1,
                            pooling = pg, minPoolSize = pg ? 0 : (int?)null, maxPoolSize = pg ? 20 : (int?)null,
                            medianMs = median, p95Ms = p95, p99Ms = p99, sortedSamplesMs = times, rawSamples });
                    else
                        results.Add(new { operation, storage, warmup, samples, concurrency = 1,
                            pooling = pg, minPoolSize = pg ? 0 : (int?)null, maxPoolSize = pg ? 20 : (int?)null,
                            medianMs = median, p95Ms = p95, p99Ms = p99, sortedSamplesMs = times });
                    Console.WriteLine($"Diagnostic latency {operation} {storage}: n={samples} median={median:F2} p95={p95:F2} p99={p99:F2}ms; loopback concurrency=1, not production acceptance");
                }
                finally
                {
                    if (database is not null)
                    {
                        NpgsqlConnection.ClearAllPools();
                        await using var drop = admin.CreateCommand("DROP DATABASE " + database + " WITH (FORCE)");
                        await drop.ExecuteNonQueryAsync();
                    }
                }
            }
        }
        return results;
    }

    private static async Task<int> StorageParity()
    {
        var adminString = Environment.GetEnvironmentVariable("BOTGRAM_PG_TEST_CONNECTION_STRING")
            ?? throw new InvalidOperationException("Storage parity requires isolated PostgreSQL");
        var options = new NpgsqlConnectionStringBuilder(adminString);
        if (!(options.Host == "localhost" || IPAddress.TryParse(options.Host, out var address) && IPAddress.IsLoopback(address)) ||
            !System.Text.RegularExpressions.Regex.IsMatch(options.Database ?? "", "\\Abotgram_test_[a-z0-9_]+\\z"))
            throw new InvalidOperationException("Parity requires loopback botgram_test_* admin database");
        var database = "botgram_test_parity_" + Guid.NewGuid().ToString("N");
        await using var admin = NpgsqlDataSource.Create(adminString);
        await using (var create = admin.CreateCommand("CREATE DATABASE " + database)) await create.ExecuteNonQueryAsync();
        options.Database = database;
        options.Pooling = true;
        options.MinPoolSize = 0;
        options.MaxPoolSize = 20;
        var connection = options.ConnectionString;
        var baseline = Path.Combine(Root, "storage-seed.json");
        var pgPath = Path.Combine(Root, "must-not-create-legacy.json");
        try
        {
            await using (var server = await Server.Start(baseline))
            {
                foreach (var name in new[] { "alice", "bob", "outsider" })
                {
                    var registration = await server.Json("POST", "/api/auth/register", new { username = name, displayName = name + " Test", password = "isolated-test-password" });
                    var token = registration["token"]!.GetValue<string>();
                    var me = await server.Json("GET", "/api/telegram/me", null, token);
                    Accounts.Add(name, new(registration["user"]!["id"]!.GetValue<string>(), token, Long(me, "id"), me["phone"]!.GetValue<string>()));
                }
            }
            var legacyPath = Path.Combine(Root, "storage-legacy.json");
            File.Copy(baseline, legacyPath);
            double legacyMs;
            await using (var legacy = await Server.Start(legacyPath))
            {
                var clock = Stopwatch.StartNew();
                await Scenario(legacy, true, capture: true);
                legacyMs = clock.Elapsed.TotalMilliseconds;
            }
            ValidationDatabases.Add(pgPath, connection);
            double pgMs;
            await using (var pg = await Server.Start(pgPath, validationConnection: connection, validationSeed: baseline))
            {
                var clock = Stopwatch.StartNew();
                await Scenario(pg, true);
                pgMs = clock.Elapsed.TotalMilliseconds;
            }
            Check(Operations.Count == 21, "storage parity covers all 21 RPC operations");
            var before = NormalizeSnapshot(Load(pgPath));
            await using (var pg = await Server.Start(pgPath, validationConnection: connection, validationSeed: baseline))
            {
                var retry = await pg.Rpc(Send(long.MinValue, "hello 😀", true));
                Check(retry.SendMessage.Message.MessageId == 1, "PG restart retry retains message ID");
                Equal(before, NormalizeSnapshot(Load(pgPath)), "PG restart retry retains full persistent state");
            }
            Check(!File.Exists(pgPath) && !File.Exists(pgPath + ".lock"), "PG mode never creates legacy persistence or lease");
            var latency = await StorageLatency(connection, legacyPath);
            var report = new { operations = Operations.Count, steps = Snapshots.Count, assertions, latency, legacyScenarioMs = legacyMs,
                pgSnapshotBridgeScenarioMs = pgMs,
                scope = "21 RPC full-state parity covers LegacyOnly versus PG snapshot bridge only; relational parity and latency cover GetState/GetHistory/SendMessage only",
                latencyScope = Environment.GetEnvironmentVariable("BOTGRAM_SEND_MATRIX") == "1"
                    ? "SendMessage only; LegacyOnly versus PG-relational-pooled Min=0 Max=20; independent identical seed per storage/dataset/concurrency; small or grown with 150 serial seed sends; 10 serial warmup + 96 total timed samples per case; concurrency 1/2/8 in fixed-size batches; HTTP/envelope/send outcome and message/mapping checks; raw requestId/elapsedMs and p50/p95/p99; same alice-to-bob conversation only, different conversations not covered; no matrix cross-storage response/restart comparison; loopback diagnostic, not production acceptance"
                    : Environment.GetEnvironmentVariable("BOTGRAM_SEND_DIAGNOSTIC") == "1"
                    ? "relational SendMessage only; 10 warmup + 100 timed samples, concurrency 1; HTTP/envelope checks only, no cross-storage response/restart comparison; pooled PG Min=0 Max=20; loopback diagnostic"
                    : "same seed per operation/storage, 5 warmup + 40 timed samples, concurrency 1; complete normalized protobuf response compared on every request; restart retry/conflict, history pagination, nonmember and unsupported rejection checks excluded from timing; pooled PG Min=0 Max=20; loopback diagnostic, not production acceptance",
                normalization = "complete protobuf JSON; only validated self online expiry and wall-clock dateMs/createdAtMs/updatedAtMs/joinedAtMs normalized; shared seed retains conversation IDs; pts and all other business values retained" };
            File.WriteAllText(Path.Combine(Root, "storage-parity.json"), JsonSerializer.Serialize(report, new JsonSerializerOptions { WriteIndented = true }));
            Console.WriteLine(Environment.GetEnvironmentVariable("BOTGRAM_SEND_MATRIX") == "1"
                ? $"PASS: snapshot-bridge parity plus SendMessage storage/dataset/concurrency matrix; same conversation only, different conversations not covered; {assertions} assertions"
                : Environment.GetEnvironmentVariable("BOTGRAM_SEND_DIAGNOSTIC") == "1"
                ? $"PASS: snapshot-bridge parity plus relational SendMessage diagnostic samples (not three-operation/restart parity); {assertions} assertions"
                : $"PASS: 21 RPC snapshot-bridge storage parity, {Snapshots.Count} complete-state checkpoints; three-operation Legacy/pooled-bridge/pooled-relational response parity and restart/pagination/rejection checks; legacy={legacyMs:F1}ms PG-bridge={pgMs:F1}ms (diagnostic only); {assertions} assertions");
            return 0;
        }
        finally
        {
            ValidationDatabases.Remove(pgPath);
            NpgsqlConnection.ClearAllPools();
            await using var drop = admin.CreateCommand("DROP DATABASE " + database + " WITH (FORCE)");
            await drop.ExecuteNonQueryAsync();
        }
    }
}
