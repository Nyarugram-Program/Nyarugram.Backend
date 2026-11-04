using System.Diagnostics;
using System.Net.WebSockets;
using Botgram.Backend.Storage;
using Cassandra;
using Confluent.Kafka;
using Npgsql;
using StackExchange.Redis;
using P = Botgram.Protocol.V1;

internal static partial class HttpTests
{
    private static ClientWebSocket LocalSocket()
    {
        var socket = new ClientWebSocket();
        socket.Options.Proxy = null;
        if (tlsEnabled) socket.Options.RemoteCertificateValidationCallback = (_, cert, _, errors) => ValidateCertificate(cert, errors);
        return socket;
    }

    private static async Task<int> FourComponentTls()
    {
        try
        {
            BeginTls();
            CertificateValidationNegatives();
            return await RelationalBackend(fullStack: true);
        }
        finally { EndTls(); }
    }

    private static async Task FourComponentDrain(Server server)
    {
        var deadline = Stopwatch.StartNew();
        while (deadline.Elapsed < TimeSpan.FromSeconds(45))
        {
            var health = await server.Json("GET", "/health", null);
            var components = health["components"]!;
            if (Long(components, "pendingOutbox") == 0 && Long(components, "pendingProjection") == 0)
            {
                Check(health["fullStackReady"]!.GetValue<bool>(), "four-component drain readiness");
                return;
            }
            await Task.Delay(100);
        }
        throw new TimeoutException("Real four-component pipeline did not drain");
    }

    private static async Task FourComponentPipeline(Server server, NpgsqlDataSource source, FourComponentOptions stack, long messageId)
    {
        await FourComponentDrain(server);
        try { await FourComponentSetup.PrepareAsync(source, stack); throw new Exception("Online schema preparation was allowed"); }
        catch (InvalidOperationException exception) when (exception.Message == "Stop P7 before offline preparation")
        { Check(true, "offline preparation refuses the live worker lease before schema migration"); }
        await ClearCache(stack);
        var before = await server.Json("GET", "/health", null);
        var request = Req(new P.GetHistoryRequest { Peer = Peer("alice"), Limit = 100 });
        var first = await server.Rpc(request, "bob");
        var second = await server.Rpc(request, "bob");
        Check(first.GetHistory.Equals(second.GetHistory), "Redis read-through preserves projected history");
        var after = await server.Json("GET", "/health", null);
        Check(Long(after["reads"]!, "projectionReads") > Long(before["reads"]!, "projectionReads"), "history reads real Scylla rows");
        Check(Long(after["reads"]!, "cacheHits") > Long(before["reads"]!, "cacheHits"), "history hits real Redis after projection read");
        using (var legacy = LocalSocket())
        {
            legacy.Options.CollectHttpResponseDetails = true;
            legacy.Options.SetRequestHeader("Authorization", "Bearer " + Accounts["bob"].Token);
            legacy.Options.AddSubProtocol("botgram.events.v1.json");
            try { await legacy.ConnectAsync(server.WebSocketUri(0), CancellationToken.None); throw new Exception("Legacy WSS was accepted"); }
            catch (WebSocketException) { Check((int)legacy.HttpStatusCode == 410, "four-component rejects JSON WSS"); }
        }
        async Task<long> Count(string table)
        {
            await using var command = source.CreateCommand("SELECT count(*) FROM botgram_intake_v1." + table);
            return (long)(await command.ExecuteScalarAsync())!;
        }
        var inbox = await Count("delivery_inbox");
        var tasks = await Count("projection_tasks");
        var positions = await Count("delivery_positions");
        string conversation;
        await using (var command = source.CreateCommand("SELECT conversation_id FROM botgram_intake_v1.messages WHERE id=$1"))
        {
            command.Parameters.AddWithValue(messageId);
            conversation = (string)(await command.ExecuteScalarAsync())!;
        }
        var pg = new PostgresDeliveryPersistence(source);
        var original = (await pg.ReadAsync(conversation, 0, 1, 1, default)).Single();
        using (var publisher = new KafkaDeliveryPublisher(new ProducerConfig { BootstrapServers = stack.Kafka,
            MessageTimeoutMs = 5000 }, stack.Topic))
        {
            await publisher.PublishAsync(original, default);
            await publisher.PublishAsync(original, default);
        }
        var deadline = Stopwatch.StartNew();
        while (await Count("delivery_positions") < positions + 2 && deadline.Elapsed < TimeSpan.FromSeconds(20))
            await Task.Delay(100);
        Check(await Count("delivery_positions") == positions + 2, "Kafka late duplicate records committed after durable staging");
        Check(await Count("delivery_inbox") == inbox && await Count("projection_tasks") == tasks,
            "late Kafka duplicate preserves inbox and projection cardinality");
    }

    private static async Task FourComponentRebuild(NpgsqlDataSource source, FourComponentOptions stack, long messageId)
    {
        string conversation;
        await using (var command = source.CreateCommand("SELECT conversation_id FROM botgram_intake_v1.messages WHERE id=$1"))
        {
            command.Parameters.AddWithValue(messageId);
            conversation = (string)(await command.ExecuteScalarAsync())!;
        }
        using var cluster = Cluster.Builder().AddContactPoint(stack.Scylla).WithPort(stack.ScyllaPort).Build();
        using var session = await cluster.ConnectAsync();
        var projection = new ScyllaMessageProjection(session, new(stack.Keyspace, stack.Datacenter, 1));
        var sourceRows = new PostgresDeliveryPersistence(source);
        var pg = new PostgresDeliveryPersistence(source, new ScyllaSourceValidator(projection, sourceRows));
        var rebuild = await pg.BeginRebuildAsync(conversation, 1, default);
        var scope = new ConsumerScope(stack.Consumer, rebuild.Generation);
        var recovery = new ProjectionRecoveryCoordinator(pg, pg, scope);
        long after = 0;
        while (after < rebuild.SnapshotSequence) after = await recovery.StageBatchAsync(conversation, after, rebuild.SnapshotSequence);
        var worker = new ProjectionWorker(pg, projection, scope);
        while (await worker.RunBatchAsync() != 0) { }
        Check(await pg.ActivateAsync(rebuild, default), "real Scylla full readback validates offline rebuild activation");
        Check(await pg.GetActiveGenerationAsync(conversation) == rebuild.Generation, "rebuilt generation becomes active");
    }

    private static async Task ClearCache(FourComponentOptions stack)
    {
        using var connection = await ConnectionMultiplexer.ConnectAsync(stack.Redis);
        var server = connection.GetServer(connection.GetEndPoints().Single());
        await foreach (var key in server.KeysAsync(pattern: "botgram:v1:" + stack.CacheNamespace + ":message:*"))
            await connection.GetDatabase().KeyDeleteAsync(key);
    }

    private static async Task FourComponentCleanup(FourComponentOptions stack)
    {
        await ClearCache(stack);
        using var admin = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = stack.Kafka }).Build();
        await admin.DeleteTopicsAsync([stack.Topic]);
        for (var attempt = 0; ; attempt++)
        {
            try { await admin.DeleteGroupsAsync([stack.Consumer]); break; }
            catch (Confluent.Kafka.Admin.DeleteGroupsException exception) when (attempt < 60 &&
                exception.Results.All(r => r.Error.Code == ErrorCode.NonEmptyGroup)) { await Task.Delay(1000); }
        }
        using var cluster = Cluster.Builder().AddContactPoint(stack.Scylla).WithPort(stack.ScyllaPort).Build();
        using var session = await cluster.ConnectAsync();
        await session.ExecuteAsync(new SimpleStatement("DROP KEYSPACE IF EXISTS " + stack.Keyspace));
    }
}
