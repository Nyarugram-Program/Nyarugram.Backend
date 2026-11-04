using System.Net;
using System.Text.Json;
using Botgram.Backend;
using Botgram.Backend.Storage;
using Cassandra;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Npgsql;
using FrozenKafka = Botgram.Backend.Storage.FrozenV1;

static class BrokerProjectionTests
{
    private static readonly CancellationToken Ct = CancellationToken.None;
    private static readonly DeliveryWorkerOptions Options = new()
    {
        BatchSize = 1000, RetryDelay = TimeSpan.FromMilliseconds(1),
        MaximumRetryDelay = TimeSpan.FromMilliseconds(10), MaximumPublishAttempts = 2
    };

    public static async Task RunAsync(NpgsqlDataSource source, PostgresMessageAcceptance acceptance)
    {
        var conversation = "broker_" + Guid.NewGuid().ToString("N");
        await using (var seed = source.CreateCommand("WITH seeded AS (INSERT INTO botgram_intake_v1.conversations(id) VALUES ($1) RETURNING id) " +
            "INSERT INTO botgram_intake_v1.members(conversation_id,user_id) SELECT id,u FROM seeded CROSS JOIN (VALUES ('a'),('b')) AS users(u)"))
        {
            seed.Parameters.AddWithValue(conversation);
            await seed.ExecuteNonQueryAsync();
        }
        for (var i = 0; i < 3; i++)
            await acceptance.AcceptAsync(new("hash-a", "a", conversation, new("broker-" + i, "projection " + i),
                MessageWriteContract.Telegram), x => x, Ct);
        var pg = new PostgresDeliveryPersistence(source);
        await pg.InitializeAsync();
        var deliveries = await pg.ReadAsync(conversation, 0, 3, 100, Ct);
        Check(deliveries.Count == 3, "fixture messages");
        var futureDelivery = FrozenCodecChecks(deliveries[0]);
        var publisher = new FakePublisher(conversation) { ThrowAfterWrite = true };
        var worker = new OutboxPublisherWorker(pg, publisher, Options);
        await worker.RunBatchAsync();
        Check(await State(source, deliveries[0].OutboxId) == "Pending", "ambiguous fake publish must not confirm");
        await Due(source, conversation);
        await worker.RunBatchAsync();
        Check(await State(source, deliveries[0].OutboxId) == "Blocked", "bounded publish failures retain blocked predecessor");
        Check(!publisher.Deliveries.Any(x => x.OutboxId == deliveries[1].OutboxId), "failed predecessor prevents successor publishing");
        Check(await pg.ReplayAsync(deliveries[0].OutboxId, Ct), "manual recovery of blocked event");
        publisher.ThrowAfterWrite = false;
        for (var i = 0; i < 3; i++) await worker.RunBatchAsync();
        Check(await State(source, deliveries[2].OutboxId) == "Published", "fake successful publish confirmed");
        Check(publisher.Deliveries.Where(x => x.OutboxId == deliveries[0].OutboxId)
            .All(x => DeliveryEventCodec.Encode(x).SequenceEqual(DeliveryEventCodec.Encode(deliveries[0]))), "retry retains original envelope");

        var scope = new ConsumerScope("broker-projection-tests", await pg.GetActiveGenerationAsync(conversation, Ct));
        var recovery = new ProjectionRecoveryCoordinator(pg, pg, scope, 1);
        long after = 0;
        while (after < 3) after = await recovery.StageBatchAsync(conversation, after, 3);
        Check(await recovery.StageBatchAsync(conversation, 0, 3) == 1, "recovery replay is idempotent");
        await using (var positions = source.CreateCommand("SELECT count(*) FROM botgram_intake_v1.delivery_positions WHERE consumer_name=$1 AND generation=$2"))
        {
            positions.Parameters.AddWithValue(scope.Name);
            positions.Parameters.AddWithValue(scope.Generation);
            Check((long)(await positions.ExecuteScalarAsync())! == 0, "PG recovery must not manufacture broker offsets");
        }
        var projection = new FakeProjection { ThrowAfterWrite = true };
        var projector = new ProjectionWorker(pg, projection, scope, Options);
        Check(await projector.RunBatchAsync() == 0, "ambiguous fake projection cannot complete PG tasks");
        Check((await pg.CheckpointAsync(conversation, scope.Generation, Ct)).ThroughSequence == 0, "failed projection checkpoint");
        await Due(source, conversation);
        projection.ThrowAfterWrite = false;
        Check(await projector.RunBatchAsync() == 3, "retry after fake durable write");
        Check(projection.Rows.Count == 3 && (await pg.CheckpointAsync(conversation, scope.Generation, Ct)).ThroughSequence == 3,
            "idempotent projection and contiguous checkpoint");
        Check(!await pg.ActivateAsync(await pg.BeginRebuildAsync(conversation, 1, Ct), Ct), "activation cannot trust fake writes/no validator");
        Console.WriteLine("PASS: real PostgreSQL worker orchestration with explicitly FAKE publisher/projection (not Kafka/Scylla durability)");
        await KafkaAsync(source, pg, deliveries, conversation, futureDelivery);
        await ScyllaAsync(source, pg, conversation);
    }

    private static DeliveryEvent FrozenCodecChecks(DeliveryEvent legacy)
    {
        Check(StorageContractVersions.EventSchema == 1 && StorageContractVersions.HistoryProjection == 1,
            "frozen codec requires the archived v1 constants");
        var decoded = FrozenKafka.DeliveryEventCodec.Decode(DeliveryEventCodec.Encode(legacy));
        Check(decoded.OutboxId == legacy.OutboxId && decoded.EventId == legacy.EventId &&
            decoded.Type == legacy.Type && decoded.PartitionKey == legacy.PartitionKey,
            "frozen codec accepts a v1 source event");
        Check(DeliveryEventCodec.Decode(FrozenKafka.DeliveryEventCodec.Encode(legacy)).OutboxId == legacy.OutboxId,
            "current codec accepts a frozen v1 event");
        var user = Guid.NewGuid().ToString("N");
        var future = new DeliveryEvent(Guid.NewGuid(), StorageContractVersions.WallpaperEventSchema, 1,
            "user:" + user, "wallpaper.changed", new WallpaperChanged(user, 1, null),
            [new RecipientPosition(user, null)], DateTimeOffset.UtcNow, null, null, null, null);
        var encoded = DeliveryEventCodec.Encode(future);
        try { FrozenKafka.DeliveryEventCodec.Validate(future); throw new InvalidOperationException("Expected v1 version rejection"); }
        catch (InvalidDataException) { }
        try { FrozenKafka.DeliveryEventCodec.Decode(encoded); throw new InvalidOperationException("Expected v1 type rejection"); }
        catch (JsonException exception) when (exception.Message == "Unknown stored event type: wallpaper.changed") { }
        return future;
    }

    private static async Task KafkaAsync(NpgsqlDataSource source, PostgresDeliveryPersistence pg,
        IReadOnlyList<DeliveryEvent> deliveries, string conversation, DeliveryEvent futureDelivery)
    {
        var bootstrap = Environment.GetEnvironmentVariable("BOTGRAM_KAFKA_TEST_BOOTSTRAP");
        if (string.IsNullOrWhiteSpace(bootstrap))
        { Console.WriteLine("SKIP: real Kafka (BOTGRAM_KAFKA_TEST_BOOTSTRAP not set)"); return; }
        foreach (var endpoint in bootstrap.Split(','))
        {
            if (!Uri.TryCreate("tcp://" + endpoint.Trim(), UriKind.Absolute, out var uri) || uri.Port <= 0 ||
                !Loopback(uri.Host) || uri.AbsolutePath != "/" || !string.IsNullOrEmpty(uri.UserInfo))
                throw new InvalidOperationException("Kafka tests only allow loopback host:port endpoints");
        }
        var topic = "botgram_test_" + Guid.NewGuid().ToString("N");
        var group = "botgram_test_" + Guid.NewGuid().ToString("N");
        using var admin = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = bootstrap }).Build();
        var created = false;
        try
        {
            await admin.CreateTopicsAsync([new TopicSpecification { Name = topic, NumPartitions = 1, ReplicationFactor = 1 }]);
            created = true;
            using var publisher = new KafkaDeliveryPublisher(new ProducerConfig
            { BootstrapServers = bootstrap, MessageTimeoutMs = 10000 }, topic);
            var scope = new ConsumerScope("broker-projection-tests", await pg.GetActiveGenerationAsync(conversation, Ct));
            var config = new ConsumerConfig
            {
                BootstrapServers = bootstrap, GroupId = group,
                AutoOffsetReset = AutoOffsetReset.Earliest
            };
            using (var initialize = new ConsumerBuilder<string, byte[]>(new ConsumerConfig(config)
            { EnableAutoCommit = false, EnableAutoOffsetStore = false }).Build())
            {
                initialize.Assign(new TopicPartitionOffset(topic, new Partition(0), new Offset(0)));
                initialize.Commit([new TopicPartitionOffset(topic, new Partition(0), new Offset(0))]);
                initialize.Close();
            }
            var recording = new RecordingInbox(pg);
            foreach (var delivery in deliveries) await publisher.PublishAsync(delivery, Ct);
            var lostAcknowledgement = new LostStageAcknowledgement(pg);
            await Task.Run(() =>
            {
                using var consumer = new KafkaInboxConsumer(config, topic, scope, lostAcknowledgement);
                using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(30));
                try
                {
                    while (true) consumer.ConsumeBatch(1, TimeSpan.FromMilliseconds(200), timeout.Token);
                }
                catch (IOException) when (lostAcknowledgement.Staged) { }
            });
            Check(lostAcknowledgement.Staged, "inbox committed before consumer crash boundary");
            await Task.Run(() =>
            {
                using var consumer = new KafkaInboxConsumer(config, topic, scope, recording,
                    async (id, ct) => new(scope.Name, await pg.GetActiveGenerationAsync(id, ct)));
                using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(30));
                while (recording.Count < deliveries.Count) consumer.ConsumeBatch(10, TimeSpan.FromMilliseconds(200), timeout.Token);
            });
            Check(recording.Count == deliveries.Count, "real Kafka stage before commit including replay of uncommitted offset");
            await using (var cardinality = source.CreateCommand("SELECT " +
                "(SELECT count(*) FROM botgram_intake_v1.delivery_inbox WHERE consumer_name=$1 AND generation=$2)=$3 AND " +
                "(SELECT count(*) FROM botgram_intake_v1.projection_tasks WHERE consumer_name=$1 AND generation=$2)=$3 AND " +
                "(SELECT count(*) FROM botgram_intake_v1.delivery_positions WHERE consumer_name=$1 AND generation=$2)=$3"))
            {
                cardinality.Parameters.AddWithValue(scope.Name);
                cardinality.Parameters.AddWithValue(scope.Generation);
                cardinality.Parameters.AddWithValue((long)deliveries.Count);
                Check((bool)(await cardinality.ExecuteScalarAsync())!, "consumer replay preserves inbox/task/position cardinality");
            }
            await Task.Run(() =>
            {
                using var consumer = new KafkaInboxConsumer(config, topic, scope, recording);
                using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(10));
                // Consume waits for assignment before concluding that the committed stream is empty.
                for (var i = 0; i < 5; i++) consumer.ConsumeBatch(10, TimeSpan.FromMilliseconds(500), timeout.Token);
            });
            Check(recording.Count == deliveries.Count, "real Kafka committed offsets survive consumer reopen");
            await FrozenKafkaAsync(admin, bootstrap, pg, scope, deliveries[0], futureDelivery);
            Console.WriteLine("PASS: real loopback Kafka publication, durable PG staging and offset commit/reopen");
        }
        finally
        {
            if (created) await admin.DeleteTopicsAsync([topic]);
        }
    }

    private static async Task FrozenKafkaAsync(IAdminClient admin, string bootstrap, PostgresDeliveryPersistence pg,
        ConsumerScope scope, DeliveryEvent legacy, DeliveryEvent future)
    {
        var topic = "botgram_test_" + Guid.NewGuid().ToString("N");
        var group = "botgram_test_" + Guid.NewGuid().ToString("N");
        var created = false;
        try
        {
            await admin.CreateTopicsAsync([new TopicSpecification { Name = topic, NumPartitions = 1, ReplicationFactor = 1 }]);
            created = true;
            var config = new ConsumerConfig { BootstrapServers = bootstrap, GroupId = group };
            using (var initialize = new ConsumerBuilder<string, byte[]>(new ConsumerConfig(config)
            { EnableAutoCommit = false, EnableAutoOffsetStore = false }).Build())
            {
                initialize.Assign(new TopicPartitionOffset(topic, new Partition(0), new Offset(0)));
                initialize.Commit([new TopicPartitionOffset(topic, new Partition(0), new Offset(0))]);
                initialize.Close();
            }
            using var oldPublisher = new FrozenKafka.KafkaDeliveryPublisher(new ProducerConfig
            { BootstrapServers = bootstrap, MessageTimeoutMs = 10000 }, topic);
            try
            {
                await oldPublisher.PublishAsync(future, Ct);
                throw new InvalidOperationException("Expected frozen publisher to reject v2");
            }
            catch (InvalidDataException) { }
            await oldPublisher.PublishAsync(legacy, Ct);
            using var publisher = new KafkaDeliveryPublisher(new ProducerConfig
            { BootstrapServers = bootstrap, MessageTimeoutMs = 10000 }, topic);
            await publisher.PublishAsync(future, Ct);
            var recording = new RecordingInbox(pg);
            await Task.Run(() =>
            {
                using var consumer = new FrozenKafka.KafkaInboxConsumer(config, topic, scope, recording);
                using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(30));
                while (recording.Count == 0) consumer.ConsumeBatch(1, TimeSpan.FromMilliseconds(200), timeout.Token);
                var rejected = false;
                try
                {
                    while (true) consumer.ConsumeBatch(1, TimeSpan.FromMilliseconds(200), timeout.Token);
                }
                catch (JsonException exception) when (exception.Message == "Unknown stored event type: wallpaper.changed")
                { rejected = true; }
                Check(rejected && recording.Count == 1, "frozen Kafka consumer stages v1 but rejects v2 before staging");
                try
                {
                    consumer.ConsumeBatch(1, TimeSpan.Zero, timeout.Token);
                    throw new InvalidOperationException("Expected failed consumer to require reopening");
                }
                catch (InvalidOperationException exception) when (exception.Message == "Reopen consumer after failed Stage or Commit") { }
            });
            using (var inspector = new ConsumerBuilder<string, byte[]>(new ConsumerConfig(config)
            { EnableAutoCommit = false, EnableAutoOffsetStore = false }).Build())
            {
                var position = inspector.Committed([new TopicPartition(topic, new Partition(0))], TimeSpan.FromSeconds(10)).Single();
                Check(position.Offset.Value == 1, "frozen consumer acknowledged the unsupported v2 record");
                inspector.Close();
            }
            Console.WriteLine("PASS: frozen v1 Kafka publisher/consumer accept v1 and reject wallpaper v2 without staging or offset commit");
        }
        finally
        {
            if (created) await admin.DeleteTopicsAsync([topic]);
        }
    }

    private static async Task ScyllaAsync(NpgsqlDataSource source, PostgresDeliveryPersistence pg, string conversation)
    {
        var contact = Environment.GetEnvironmentVariable("BOTGRAM_SCYLLA_TEST_CONTACT_POINT");
        if (string.IsNullOrWhiteSpace(contact))
        { Console.WriteLine("SKIP: real Scylla (BOTGRAM_SCYLLA_TEST_CONTACT_POINT not set)"); return; }
        if (!Loopback(contact)) throw new InvalidOperationException("Scylla tests only allow a loopback contact point");
        var keyspace = "botgram_test_" + Guid.NewGuid().ToString("N");
        using var cluster = Cluster.Builder().AddContactPoint(contact).Build();
        using var session = await cluster.ConnectAsync();
        var dc = (await session.ExecuteAsync(new SimpleStatement("SELECT data_center FROM system.local"))).First().GetValue<string>("data_center");
        var projection = new ScyllaMessageProjection(session, new(keyspace, dc, 1));
        try
        {
            await projection.InitializeAsync(Ct);
            var validated = new PostgresDeliveryPersistence(source, new ScyllaSourceValidator(projection, pg));
            var rebuild = await validated.BeginRebuildAsync(conversation, 1, Ct);
            var scope = new ConsumerScope("scylla-tests", rebuild.Generation);
            var recovery = new ProjectionRecoveryCoordinator(pg, pg, scope, 1);
            long after = 0;
            while (after < rebuild.SnapshotSequence)
                after = await recovery.StageBatchAsync(conversation, after, rebuild.SnapshotSequence);
            var firstDelivery = (await pg.ReadAsync(conversation, 0, 1, 1, Ct)).Single();
            await projection.UpsertAsync(ScyllaMessageProjection.FromDelivery(firstDelivery, rebuild.Generation), Ct);
            var worker = new ProjectionWorker(pg, projection, scope, Options);
            Check(await worker.RunBatchAsync() == rebuild.SnapshotSequence, "real Scylla projection completion");
            Check(await validated.ActivateAsync(rebuild, Ct), "real Scylla source readback validates activation");
            Check(await pg.GetActiveGenerationAsync(conversation, Ct) == rebuild.Generation, "activated head");
            var delivery = (await pg.ReadAsync(conversation, 0, 1, 1, Ct)).Single();
            var work = ScyllaMessageProjection.FromDelivery(delivery, rebuild.Generation);
            await projection.UpsertAsync(work, Ct);
            Check(ScyllaMessageProjection.Equivalent((await projection.ReadAsync(work.Generation, conversation, 1, work.Message.Id, Ct))!.Work, work),
                "real Scylla duplicate content");
            try
            {
                await projection.UpsertAsync(work with { Message = work.Message with { Text = "conflict" } }, Ct);
                throw new InvalidOperationException("Expected real Scylla content conflict");
            }
            catch (InvalidDataException) { }
            var validator = new ScyllaSourceValidator(projection, pg);
            var checkpoint = await pg.CheckpointAsync(conversation, rebuild.Generation, Ct);
            Check(await validator.CompareShadowAsync(rebuild, checkpoint with { ThroughSequence = 0 }, 3, Ct) == ShadowProjectionResult.Deferred,
                "shadow read defers projection lag instead of reporting mismatch");
            Check(await validator.CompareShadowAsync(rebuild, checkpoint with { Generation = Guid.NewGuid() }, 3, Ct) == ShadowProjectionResult.Deferred,
                "shadow read defers generation mismatch");
            Check(await validator.CompareShadowAsync(rebuild, checkpoint, 3, Ct) == ShadowProjectionResult.Match,
                "shadow read compares only at common committed watermark");
            var deliveries = await pg.ReadAsync(conversation, 0, rebuild.SnapshotSequence, 1000, Ct);
            async Task<ProjectionRebuild> CopyGenerationAsync()
            {
                var candidate = await validated.BeginRebuildAsync(conversation, 1, Ct);
                foreach (var item in deliveries)
                    await projection.UpsertAsync(ScyllaMessageProjection.FromDelivery(item, candidate.Generation), Ct);
                return candidate;
            }
            async Task InsertPhysicalAsync(ProjectionWork item, string physicalConversation, long bucket, long messageId)
            {
                var statement = new SimpleStatement($"INSERT INTO {keyspace}.message_projection_v1 " +
                    "(generation, conversation_id, bucket, message_id, sequence, message_version, format_version, outbox_id, event_id, payload) " +
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", item.Generation, physicalConversation, bucket, messageId,
                    item.ConversationSequence, item.MessageVersion, item.FormatVersion, item.OutboxId, item.EventId,
                    JsonSerializer.Serialize(item.Message, new JsonSerializerOptions(JsonSerializerDefaults.Web)));
                statement.SetConsistencyLevel(ConsistencyLevel.LocalQuorum);
                await session.ExecuteAsync(statement);
            }

            // Keep corrupt generations in place; a fresh rebuild must isolate rather than delete them.
            var extraBucket = await CopyGenerationAsync();
            await InsertPhysicalAsync(work with { Generation = extraBucket.Generation }, conversation, 99, work.Message.Id);
            Check(!await validator.ValidateAsync(extraBucket, rebuild.SnapshotSequence, Ct), "reject extra bucket outside expected source buckets");

            var wrongId = await CopyGenerationAsync();
            await InsertPhysicalAsync(work with { Generation = wrongId.Generation }, conversation, 0, long.MaxValue);
            Check(!await validator.ValidateAsync(wrongId, rebuild.SnapshotSequence, Ct), "reject physical message ID/payload mismatch");

            var wrongConversation = await CopyGenerationAsync();
            await InsertPhysicalAsync(work with
            {
                Generation = wrongConversation.Generation,
                Message = work.Message with { ConversationId = "wrong-payload-conversation" }
            }, conversation, 0, work.Message.Id);
            Check(!await validator.ValidateAsync(wrongConversation, rebuild.SnapshotSequence, Ct), "reject physical conversation/payload mismatch");

            var wrongSequence = await CopyGenerationAsync();
            await InsertPhysicalAsync(work with
            {
                Generation = wrongSequence.Generation,
                ConversationSequence = StorageContractVersions.MessagesPerBucket + 1L
            }, conversation, 0, work.Message.Id);
            Check(!await validator.ValidateAsync(wrongSequence, rebuild.SnapshotSequence, Ct), "reject physical bucket/sequence mismatch");

            var changing = await CopyGenerationAsync();
            var changingSource = new AuditMutationSource(pg, async () =>
                await InsertPhysicalAsync(work with
                {
                    Generation = changing.Generation,
                    Message = work.Message with { Text = "changed during source verification" }
                }, conversation, 0, work.Message.Id));
            Check(!await new ScyllaSourceValidator(projection, changingSource).ValidateAsync(changing, rebuild.SnapshotSequence, Ct),
                "second actual-row audit rejects observed concurrent target change");

            var missing = await validated.BeginRebuildAsync(conversation, 1, Ct);
            foreach (var item in deliveries)
                await projection.UpsertAsync(ScyllaMessageProjection.FromDelivery(item, Guid.NewGuid()), Ct);
            Check(!await validator.ValidateAsync(missing, rebuild.SnapshotSequence, Ct), "identical content in wrong generation cannot fill target");

            var emptyConversation = "scylla_empty_" + Guid.NewGuid().ToString("N");
            await using (var seed = source.CreateCommand("INSERT INTO botgram_intake_v1.conversations(id) VALUES ($1)"))
            {
                seed.Parameters.AddWithValue(emptyConversation);
                await seed.ExecuteNonQueryAsync();
            }
            var empty = await validated.BeginRebuildAsync(emptyConversation, 1, Ct);
            Check(await validator.ValidateAsync(empty, 0, Ct), "empty source with no actual rows validates despite other conversations");
            await projection.UpsertAsync(work with
            {
                Generation = empty.Generation,
                Message = work.Message with { ConversationId = emptyConversation }
            }, Ct);
            Check(!await validator.ValidateAsync(empty, 0, Ct), "empty source rejects polluted target generation");
            Check(!await validated.ActivateAsync(empty, Ct), "empty polluted generation cannot activate");

            var next = await validated.BeginRebuildAsync(conversation, 1, Ct);
            var stale = await validated.BeginRebuildAsync(conversation, 1, Ct);
            async Task CompleteGenerationAsync(ProjectionRebuild candidate)
            {
                var candidateScope = new ConsumerScope("scylla-tests", candidate.Generation);
                var candidateRecovery = new ProjectionRecoveryCoordinator(pg, pg, candidateScope, 1000);
                long position = 0;
                while (position < candidate.SnapshotSequence)
                    position = await candidateRecovery.StageBatchAsync(conversation, position, candidate.SnapshotSequence);
                Check(await new ProjectionWorker(pg, projection, candidateScope, Options).RunBatchAsync() == candidate.SnapshotSequence,
                    "complete fresh rebuild in real Scylla");
            }
            await CompleteGenerationAsync(next);
            await CompleteGenerationAsync(stale);
            Check(await validator.ValidateAsync(next, next.SnapshotSequence, Ct), "old generations and other conversations do not pollute target");
            Check(!await new ScyllaSourceValidator(projection, pg, 3).ValidateAsync(next, next.SnapshotSequence, Ct),
                "full-table audit fails closed when unrelated rows exceed budget");
            Check(await validated.ActivateAsync(next, Ct), "fresh rebuild switches active generation without deleting old projections");
            Check(await pg.GetActiveGenerationAsync(conversation, Ct) == next.Generation, "fresh rebuild becomes active");
            Check(ScyllaMessageProjection.Equivalent((await projection.ReadAsync(work.Generation, conversation, 1, work.Message.Id, Ct))!.Work, work),
                "old projection remains after switch");
            Check(await validator.ValidateAsync(stale, stale.SnapshotSequence, Ct), "stale candidate has valid actual rows");
            Check(!await validated.ActivateAsync(stale, Ct), "expired expected_active rejects otherwise complete rebuild");
            Check(await pg.GetActiveGenerationAsync(conversation, Ct) == next.Generation, "stale activation cannot change head");
            Console.WriteLine("PASS: real loopback Scylla bounded full-table audit, physical-key validation, empty/extra/wrong-generation rejection, retained generations and activation fencing");
        }
        finally { await session.ExecuteAsync(new SimpleStatement($"DROP KEYSPACE IF EXISTS {keyspace}")); }
    }

    private static bool Loopback(string host) => string.Equals(host, "localhost", StringComparison.OrdinalIgnoreCase) ||
        IPAddress.TryParse(host.Trim('[', ']'), out var address) && IPAddress.IsLoopback(address);

    private static async Task<string> State(NpgsqlDataSource source, Guid id)
    {
        await using var command = source.CreateCommand("SELECT state FROM botgram_intake_v1.outbox WHERE id=$1");
        command.Parameters.AddWithValue(id);
        return (string)(await command.ExecuteScalarAsync())!;
    }
    private static async Task Due(NpgsqlDataSource source, string conversation)
    {
        await using var command = source.CreateCommand("WITH due AS (UPDATE botgram_intake_v1.outbox SET retry_at=clock_timestamp()-interval '1 second' WHERE partition_key=$1 RETURNING id) " +
            "UPDATE botgram_intake_v1.projection_tasks SET retry_at=clock_timestamp()-interval '1 second' WHERE conversation_id=$1");
        command.Parameters.AddWithValue(conversation);
        await command.ExecuteNonQueryAsync();
    }
    private static void Check(bool value, string message)
    { if (!value) throw new InvalidOperationException(message); }

    // These doubles model ambiguous external success only; they do not attest Kafka/Scylla behavior.
    private sealed class FakePublisher(string conversation) : IDurableEventPublisher
    {
        public bool ThrowAfterWrite { get; set; }
        public List<DeliveryEvent> Deliveries { get; } = [];
        public Task<BrokerAcknowledgement> PublishAsync(DeliveryEvent delivery, CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            Deliveries.Add(delivery);
            if (delivery.PartitionKey == conversation && ThrowAfterWrite) throw new IOException("Fake lost broker acknowledgement");
            return Task.FromResult(new BrokerAcknowledgement(new("fake-topic", 0, Deliveries.Count)));
        }
    }
    private sealed class FakeProjection : IMessageProjection
    {
        public bool ThrowAfterWrite { get; set; }
        public Dictionary<Guid, ProjectionWork> Rows { get; } = [];
        public Task UpsertAsync(ProjectionWork work, CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (Rows.TryGetValue(work.OutboxId, out var prior) && !ScyllaMessageProjection.Equivalent(prior, work))
                throw new InvalidDataException("Fake projection conflict");
            Rows[work.OutboxId] = work;
            if (ThrowAfterWrite) throw new IOException("Fake lost projection acknowledgement");
            return Task.CompletedTask;
        }
    }
    private sealed class AuditMutationSource(IProjectionSource inner, Func<Task> mutate) : IProjectionSource
    {
        private bool mutated;
        public async Task<IReadOnlyList<DeliveryEvent>> ReadAsync(string conversationId, long afterSequence,
            long throughSequence, int limit, CancellationToken cancellationToken)
        {
            var page = await inner.ReadAsync(conversationId, afterSequence, throughSequence, limit, cancellationToken);
            if (!mutated)
            {
                mutated = true;
                await mutate();
            }
            return page;
        }
    }
    private sealed class LostStageAcknowledgement(IEventInboxPersistence inner) : IEventInboxPersistence
    {
        public bool Staged { get; private set; }
        public async Task<InboxStageResult> StageAsync(ConsumerScope consumer, DeliveryEvent delivery,
            BrokerPosition? position, CancellationToken cancellationToken)
        {
            await inner.StageAsync(consumer, delivery, position, cancellationToken);
            Staged = true;
            throw new IOException("Injected crash boundary after durable inbox commit before Kafka offset commit");
        }
    }
    private sealed class RecordingInbox(IEventInboxPersistence inner) : IEventInboxPersistence
    {
        public int Count { get; private set; }
        public async Task<InboxStageResult> StageAsync(ConsumerScope consumer, DeliveryEvent delivery,
            BrokerPosition? position, CancellationToken cancellationToken)
        {
            var result = await inner.StageAsync(consumer, delivery, position, cancellationToken);
            Count++;
            return result;
        }
    }
}
