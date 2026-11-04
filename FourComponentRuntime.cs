using Cassandra;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Npgsql;
using StackExchange.Redis;

namespace Botgram.Backend.Storage;

public sealed class FourComponentRuntime(NpgsqlDataSource source, FourComponentOptions options,
    ILogger<FourComponentRuntime> logger) : BackgroundService
{
    internal const long RuntimeLock = 7459241304238;
    private NpgsqlConnection? ownership;
    private readonly SemaphoreSlim ownershipGate = new(1);
    private Cluster? cluster;
    private Cassandra.ISession? scylla;
    private ConnectionMultiplexer? redis;
    private KafkaDeliveryPublisher? publisher;
    private IAdminClient? admin;
    private PostgresDeliveryPersistence? persistence;
    private ScyllaMessageProjection? projection;
    private RedisMessageCache? cache;
    private long publisherTick, consumerTick, projectionTick;
    public ScyllaMessageProjection Projection => projection ?? throw new ApiException(503, "storage_starting");
    public RedisMessageCache Cache => cache ?? throw new ApiException(503, "storage_starting");

    public override async Task StartAsync(CancellationToken token)
    {
        await EnsureOwnershipAsync(token);
        await VerifyContractAsync(source, options, token);
        cluster = Cluster.Builder().AddContactPoint(options.Scylla).WithPort(options.ScyllaPort)
            .WithQueryOptions(new QueryOptions().SetConsistencyLevel(ConsistencyLevel.LocalQuorum))
            .WithSocketOptions(new SocketOptions().SetConnectTimeoutMillis(3000).SetReadTimeoutMillis(3000)).Build();
        scylla = await cluster.ConnectAsync().WaitAsync(token);
        projection = new(scylla, new(options.Keyspace, options.Datacenter, 1));
        // Schema is prepared offline. Startup only verifies the existing projection table.
        await scylla.ExecuteAsync(new SimpleStatement("SELECT generation FROM " + options.Keyspace +
            ".message_projection_v1 LIMIT 1")).WaitAsync(token);
        var redisOptions = ConfigurationOptions.Parse(options.Redis);
        redisOptions.AbortOnConnectFail = false;
        redisOptions.ConnectTimeout = 2000;
        redisOptions.AsyncTimeout = 1000;
        redisOptions.TieBreaker = "";
        redis = await ConnectionMultiplexer.ConnectAsync(redisOptions).WaitAsync(token);
        await redis.GetDatabase().PingAsync().WaitAsync(token);
        cache = new(new RedisMessageCacheTransport(redis.GetDatabase()), options.CacheNamespace, TimeSpan.FromMinutes(1));
        persistence = new(source, new ScyllaSourceValidator(projection, new PostgresDeliveryPersistence(source)));
        admin = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = options.Kafka,
            SocketTimeoutMs = 3000 }).Build();
        VerifyTopic(admin, options);
        using (var consumer = new ConsumerBuilder<string, byte[]>(ConsumerConfig(options)).Build())
        {
            var committed = consumer.Committed([new TopicPartition(options.Topic, 0)], TimeSpan.FromSeconds(5)).Single();
            if (committed.Offset < 0) throw new InvalidOperationException("P7 Kafka group requires explicit offline offset initialization");
        }
        publisher = new(new ProducerConfig { BootstrapServers = options.Kafka, MessageTimeoutMs = 5000 }, options.Topic);
        await base.StartAsync(token);
    }

    public async Task EnsureOwnershipAsync(CancellationToken token)
    {
        await ownershipGate.WaitAsync(token);
        try
        {
            if (ownership is not null && ownership.State != System.Data.ConnectionState.Open)
            {
                await ownership.DisposeAsync();
                ownership = null;
            }
            if (ownership is not null)
            {
                try
                {
                    await using var ping = new NpgsqlCommand("SELECT 1", ownership) { CommandTimeout = 2 };
                    await ping.ExecuteScalarAsync(token);
                    return;
                }
                catch (NpgsqlException) { await ownership.DisposeAsync(); ownership = null; }
            }
            var candidate = await source.OpenConnectionAsync(token);
            try
            {
                await using var acquire = new NpgsqlCommand("SELECT pg_try_advisory_lock($1)", candidate) { CommandTimeout = 2 };
                acquire.Parameters.AddWithValue(RuntimeLock);
                if (!(bool)(await acquire.ExecuteScalarAsync(token))!)
                    throw new ApiException(503, "storage_worker_owned_elsewhere");
                ownership = candidate;
            }
            catch { await candidate.DisposeAsync(); throw; }
        }
        finally { ownershipGate.Release(); }
    }

    internal static ConsumerConfig ConsumerConfig(FourComponentOptions options) => new()
    {
        BootstrapServers = options.Kafka, GroupId = options.Consumer,
        EnableAutoCommit = false, EnableAutoOffsetStore = false,
        AutoOffsetReset = AutoOffsetReset.Error, SocketTimeoutMs = 5000,
        SessionTimeoutMs = 6000, HeartbeatIntervalMs = 1000
    };

    internal static void VerifyTopic(IAdminClient admin, FourComponentOptions options)
    {
        var topic = admin.GetMetadata(options.Topic, TimeSpan.FromSeconds(5)).Topics.Single();
        if (topic.Error.IsError || topic.Partitions.Count != 1 || topic.Partitions.Any(p => p.Error.IsError || p.Replicas.Length != 1))
            throw new InvalidOperationException("P7 requires its prepared single-partition isolated Kafka topic");
    }

    internal static async Task VerifyContractAsync(NpgsqlDataSource source, FourComponentOptions options, CancellationToken token)
    {
        await using var command = source.CreateCommand("""
            SELECT event_schema,topic,consumer_name,keyspace,cache_namespace,initialized
            FROM botgram_backend_v1.runtime_contract WHERE singleton
            """);
        await using var reader = await command.ExecuteReaderAsync(token);
        if (!await reader.ReadAsync(token) || reader.GetInt32(0) != StorageContractVersions.WallpaperEventSchema ||
            reader.GetString(1) != options.Topic || reader.GetString(2) != options.Consumer ||
            reader.GetString(3) != options.Keyspace || reader.GetString(4) != options.CacheNamespace || !reader.GetBoolean(5))
            throw new InvalidOperationException("P7 offline runtime contract is missing or incompatible");
    }

    protected override Task ExecuteAsync(CancellationToken token) => Task.WhenAll(
        Publish(token), Project(token), Task.Factory.StartNew(() => Consume(token), token,
            TaskCreationOptions.LongRunning, TaskScheduler.Default));

    private async Task Publish(CancellationToken token)
    {
        var worker = new OutboxPublisherWorker(persistence!, publisher!);
        while (!token.IsCancellationRequested)
        {
            try
            {
                await EnsureOwnershipAsync(token);
                await VerifyContractAsync(source, options, token);
                await worker.RunBatchAsync(token);
                Interlocked.Exchange(ref publisherTick, DateTime.UtcNow.Ticks);
            }
            catch (OperationCanceledException) when (token.IsCancellationRequested) { break; }
            catch (Exception exception) { Log("publisher", exception); Interlocked.Exchange(ref publisherTick, 0); }
            await Task.Delay(100, token);
        }
    }

    private void Consume(CancellationToken token)
    {
        while (!token.IsCancellationRequested)
        {
            try
            {
                var generation = persistence!.GetActiveGenerationAsync("user:p7", token).GetAwaiter().GetResult();
                using var consumer = new KafkaInboxConsumer(ConsumerConfig(options), options.Topic,
                    new(options.Consumer, generation), persistence,
                    async (key, ct) => new(options.Consumer, await persistence.GetActiveGenerationAsync(key, ct)));
                while (!token.IsCancellationRequested)
                {
                    EnsureOwnershipAsync(token).GetAwaiter().GetResult();
                    VerifyContractAsync(source, options, token).GetAwaiter().GetResult();
                    consumer.ConsumeBatch(100, TimeSpan.FromMilliseconds(200), token);
                    Interlocked.Exchange(ref consumerTick, DateTime.UtcNow.Ticks);
                }
            }
            catch (OperationCanceledException) when (token.IsCancellationRequested) { break; }
            catch (Exception exception)
            {
                Log("consumer", exception);
                Interlocked.Exchange(ref consumerTick, 0);
                if (token.WaitHandle.WaitOne(TimeSpan.FromSeconds(1))) break;
            }
        }
    }

    private async Task Project(CancellationToken token)
    {
        while (!token.IsCancellationRequested)
        {
            try
            {
                await EnsureOwnershipAsync(token);
                await VerifyContractAsync(source, options, token);
                await using var command = source.CreateCommand("""
                    SELECT DISTINCT generation FROM botgram_intake_v1.projection_tasks
                    WHERE consumer_name=$1 AND state<>'Complete' LIMIT 100
                    """);
                command.Parameters.AddWithValue(options.Consumer);
                var generations = new List<Guid>();
                await using (var reader = await command.ExecuteReaderAsync(token))
                    while (await reader.ReadAsync(token)) generations.Add(reader.GetGuid(0));
                foreach (var generation in generations)
                    await new ProjectionWorker(persistence!, Projection, new(options.Consumer, generation)).RunBatchAsync(token);
                Interlocked.Exchange(ref projectionTick, DateTime.UtcNow.Ticks);
            }
            catch (OperationCanceledException) when (token.IsCancellationRequested) { break; }
            catch (Exception exception) { Log("projection", exception); Interlocked.Exchange(ref projectionTick, 0); }
            await Task.Delay(100, token);
        }
    }

    private void Log(string worker, Exception exception) => logger.LogWarning(
        "P7 {Worker} deferred, exception type={Type}", worker, exception.GetType().Name);

    public async Task<FourComponentHealth> HealthAsync(CancellationToken token)
    {
        long outbox = -1, projections = -1, blocked = -1;
        var postgres = false;
        var kafka = false;
        var scyllaReady = false;
        var redisReady = false;
        try
        {
            using var probe = CancellationTokenSource.CreateLinkedTokenSource(token);
            probe.CancelAfter(TimeSpan.FromSeconds(3));
            await EnsureOwnershipAsync(probe.Token);
            await using var command = source.CreateCommand("""
                SELECT (SELECT count(*) FROM botgram_intake_v1.outbox WHERE state<>'Published'),
                    (SELECT count(*) FROM botgram_intake_v1.projection_tasks WHERE state<>'Complete'),
                    (SELECT count(*) FROM botgram_intake_v1.outbox WHERE state='Blocked')
                """);
            command.CommandTimeout = 2;
            await using var reader = await command.ExecuteReaderAsync(token);
            await reader.ReadAsync(token);
            outbox = reader.GetInt64(0); projections = reader.GetInt64(1); blocked = reader.GetInt64(2);
            postgres = ownership?.State == System.Data.ConnectionState.Open;
        }
        catch (NpgsqlException) { }
        catch (ApiException) { }
        catch (OperationCanceledException) when (!token.IsCancellationRequested) { }
        try { await Task.Run(() => VerifyTopic(admin!, options), token); kafka = true; }
        catch (KafkaException) { }
        catch (InvalidOperationException) { }
        try { await scylla!.ExecuteAsync(new SimpleStatement("SELECT key FROM system.local")).WaitAsync(token); scyllaReady = true; }
        catch (Cassandra.DriverException) { }
        try { await redis!.GetDatabase().PingAsync().WaitAsync(TimeSpan.FromSeconds(2), token); redisReady = true; }
        catch (RedisException) { }
        catch (TimeoutException) { }
        var cutoff = DateTime.UtcNow.Subtract(TimeSpan.FromSeconds(15)).Ticks;
        var workers = Interlocked.Read(ref publisherTick) > cutoff && Interlocked.Read(ref consumerTick) > cutoff &&
            Interlocked.Read(ref projectionTick) > cutoff;
        return new(postgres && kafka && scyllaReady && redisReady && workers && blocked == 0,
            postgres, kafka, scyllaReady, redisReady, workers, outbox, projections, blocked);
    }

    public override void Dispose()
    {
        base.Dispose();
        publisher?.Dispose(); admin?.Dispose(); redis?.Dispose(); scylla?.Dispose(); cluster?.Dispose(); ownership?.Dispose();
    }
}

public sealed record FourComponentHealth(bool FullStackReady, bool Postgres, bool Kafka, bool Scylla,
    bool Redis, bool Workers, long PendingOutbox, long PendingProjection, long BlockedOutbox);

public static class FourComponentSetup
{
    public static async Task PrepareAsync(NpgsqlDataSource source, FourComponentOptions options, CancellationToken token = default)
    {
        await using var connection = await source.OpenConnectionAsync(token);
        await using var transaction = await connection.BeginTransactionAsync(token);
        await using (var guard = new NpgsqlCommand("SELECT pg_try_advisory_xact_lock($1)", connection, transaction))
        {
            guard.Parameters.AddWithValue(FourComponentRuntime.RuntimeLock);
            if (!(bool)(await guard.ExecuteScalarAsync(token))!) throw new InvalidOperationException("Stop P7 before offline preparation");
        }
        await PostgresBackendSchema.ApplyAsync(source, token);
        await using (var schema = new NpgsqlCommand("""
            CREATE TABLE IF NOT EXISTS botgram_backend_v1.runtime_contract (
                singleton boolean PRIMARY KEY DEFAULT true CHECK(singleton), event_schema integer NOT NULL,
                topic text NOT NULL, consumer_name text NOT NULL, keyspace text NOT NULL, cache_namespace text NOT NULL,
                initialized boolean NOT NULL DEFAULT false)
            """, connection, transaction))
            await schema.ExecuteNonQueryAsync(token);
        await using (var schema = new NpgsqlCommand("""
            INSERT INTO botgram_backend_v1.runtime_contract(singleton,event_schema,topic,consumer_name,keyspace,cache_namespace)
                VALUES(true,$1,$2,$3,$4,$5) ON CONFLICT DO NOTHING
            """, connection, transaction))
        {
            schema.Parameters.AddWithValue(StorageContractVersions.WallpaperEventSchema);
            schema.Parameters.AddWithValue(options.Topic); schema.Parameters.AddWithValue(options.Consumer);
            schema.Parameters.AddWithValue(options.Keyspace); schema.Parameters.AddWithValue(options.CacheNamespace);
            await schema.ExecuteNonQueryAsync(token);
        }
        bool initialized;
        await using (var check = new NpgsqlCommand("""
            SELECT event_schema,topic,consumer_name,keyspace,cache_namespace,initialized
            FROM botgram_backend_v1.runtime_contract WHERE singleton FOR UPDATE
            """, connection, transaction))
        await using (var reader = await check.ExecuteReaderAsync(token))
        {
            await reader.ReadAsync(token);
            if (reader.GetInt32(0) != StorageContractVersions.WallpaperEventSchema || reader.GetString(1) != options.Topic ||
                reader.GetString(2) != options.Consumer || reader.GetString(3) != options.Keyspace || reader.GetString(4) != options.CacheNamespace)
                throw new InvalidOperationException("Offline P7 namespaces or worker schema differ from the prepared contract");
            initialized = reader.GetBoolean(5);
        }
        using var admin = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = options.Kafka }).Build();
        try { await admin.CreateTopicsAsync([new TopicSpecification { Name = options.Topic, NumPartitions = 1, ReplicationFactor = 1 }]); }
        catch (CreateTopicsException exception) when (exception.Results.All(r => r.Error.Code == ErrorCode.TopicAlreadyExists)) { }
        FourComponentRuntime.VerifyTopic(admin, options);
        using (var consumer = new ConsumerBuilder<string, byte[]>(FourComponentRuntime.ConsumerConfig(options)).Build())
        {
            var partition = new TopicPartition(options.Topic, 0);
            if (!initialized)
            {
                var watermarks = consumer.QueryWatermarkOffsets(partition, TimeSpan.FromSeconds(5));
                if (watermarks.High.Value != 0) throw new InvalidOperationException("New P7 preparation requires an empty isolated Kafka topic");
                consumer.Assign(new TopicPartitionOffset(partition, 0));
                consumer.Commit([new TopicPartitionOffset(partition, 0)]);
            }
            else if (consumer.Committed([partition], TimeSpan.FromSeconds(5)).Single().Offset < 0)
                throw new InvalidOperationException("Previously initialized P7 offsets are missing; explicit recovery is required");
            consumer.Close();
        }
        using var cluster = Cluster.Builder().AddContactPoint(options.Scylla).WithPort(options.ScyllaPort).Build();
        using var session = await cluster.ConnectAsync().WaitAsync(token);
        await new ScyllaMessageProjection(session, new(options.Keyspace, options.Datacenter, 1)).InitializeAsync(token);
        await using (var complete = new NpgsqlCommand("UPDATE botgram_backend_v1.runtime_contract SET initialized=true WHERE singleton", connection, transaction))
            await complete.ExecuteNonQueryAsync(token);
        await transaction.CommitAsync(token);
    }
}
