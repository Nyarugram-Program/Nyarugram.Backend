using System.Diagnostics;
using System.Text.Json;
using Botgram.Backend;
using P = Botgram.Protocol.V1;

internal static partial class HttpTests
{
    private static async Task<int> MapperProfile()
    {
        var baseline = await TlsBaseline(100);
        var rows = new List<object>();
        for (int round = 0; round < 3; round++)
        foreach (var operation in new[] { "GetState", "GetHistory", "SendMessage" })
        foreach (var proto in round % 2 == 0 ? new[] { false, true } : new[] { true, false })
        {
            var db = JsonSerializer.Deserialize<Database>(File.ReadAllText(baseline), Store.Json)!;
            var request = operation switch
            {
                "GetState" => Req(new P.GetStateRequest()),
                "GetHistory" => Req(new P.GetHistoryRequest { Peer = Peer("bob"), Limit = 100 }),
                _ => Send(long.MinValue, "benchmark fixed message 😀")
            };
            var legacy = operation switch
            {
                "GetState" => T("TL_updates_getState"),
                "GetHistory" => T("TL_messages_getHistory", ("peer", PeerJ(Accounts["bob"])), ("limit", 100)),
                _ => T("TL_messages_sendMessage", ("peer", PeerJ(Accounts["bob"])), ("random_id", long.MinValue), ("message", "benchmark fixed message 😀"))
            };
            object Map() => proto ? ProtobufRpcMapper.Execute(db, Accounts["alice"].Id, request, Domain)
                : TelegramGateway.Execute(db, Accounts["alice"].Id, legacy);
            for (int i = 0; i < 200; i++) GC.KeepAlive(Map());
            const int count = 5000;
            var timings = new double[count];
            var allocations = GC.GetAllocatedBytesForCurrentThread();
            for (int i = 0; i < count; i++)
            {
                var start = Stopwatch.GetTimestamp();
                GC.KeepAlive(Map());
                timings[i] = Stopwatch.GetElapsedTime(start).TotalMilliseconds;
            }
            allocations = GC.GetAllocatedBytesForCurrentThread() - allocations;
            Array.Sort(timings);
            rows.Add(new { round, operation, protocol = proto ? "protobuf" : "json", count, warmup = 200,
                allocatedBytesPerCall = (double)allocations / count, p50Ms = timings[2499], p95Ms = timings[4749],
                p99Ms = timings[4949], maxMs = timings[^1] });
            Check(db.Messages.Count == 100, "mapper profile uses idempotent send; fixed database size");
        }
        File.WriteAllText(Path.Combine(Root, "mapper-profile.json"), JsonSerializer.Serialize(new
        {
            backendSha256 = Convert.ToHexString(System.Security.Cryptography.SHA256.HashData(File.ReadAllBytes(BackendDll))),
            scope = "Synchronous in-process mapper including shared services, excludes HTTP, TLS, request decode, response encode and Store persistence. Send is existing-key retry, not new send. Thread allocated bytes; no endpoint budget claim.",
            results = rows
        }, new JsonSerializerOptions { WriteIndented = true }));
        Console.WriteLine($"Mapper allocation profile complete; root={Root}; not an endpoint performance acceptance");
        return 0;
    }
}
