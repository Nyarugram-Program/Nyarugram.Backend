using System.Text.Json;
using Botgram.Backend;
using Botgram.Backend.Storage;
using Npgsql;

static class DeliveryTests
{
    private static readonly CancellationToken Ct = CancellationToken.None;
    private static readonly TimeSpan Duration = TimeSpan.FromMinutes(1);

    public static async Task Run(string connectionString)
    {
        await using var source = NpgsqlDataSource.Create(connectionString);
        var acceptance = new PostgresMessageAcceptance(source);
        var verifier = new RecordingValidator();
        var pg = new PostgresDeliveryPersistence(source, verifier);
        await pg.InitializeAsync();
        await pg.InitializeAsync();
        async Task Sql(string sql)
        {
            await using var command = source.CreateCommand(sql);
            await command.ExecuteNonQueryAsync();
        }
        async Task<long> Count()
        {
            await using var command = source.CreateCommand("SELECT count(*) FROM botgram_intake_v1.messages");
            return (long)(await command.ExecuteScalarAsync())!;
        }
        MessageAcceptance Input(string key) => new("hash-a", "a", "chat", new(key, "delivery"), MessageWriteContract.Telegram);
        var first = (await pg.ClaimAsync(10, Duration, Ct)).First(x => x.Delivery.PartitionKey == "chat");
        Check(first.Delivery.Recipients.Select(x => x.UserId).SequenceEqual(new[] { "a", "b" }) &&
              first.Delivery.Recipients.All(x => x.Pts > 1), "original recipient pts");
        Check(!(await pg.ClaimAsync(10, Duration, Ct)).Any(x => x.Delivery.PartitionKey == "chat"), "leased predecessor blocks key");
        await Sql($"UPDATE botgram_intake_v1.outbox SET lease_until=clock_timestamp()-interval '1 second' WHERE id='{first.Delivery.OutboxId}'");
        Check(!await pg.RenewAsync(first.Delivery.OutboxId, first.LeaseToken, Duration, Ct), "expired renew");
        Check(!await pg.ConfirmPublishedAsync(first.Delivery.OutboxId, first.LeaseToken, new(new("test", 0, 1)), Ct), "expired confirmation");
        Check(!await pg.FailAsync(first.Delivery.OutboxId, first.LeaseToken, DateTimeOffset.UtcNow, false, Ct), "expired failure");
        var reclaimed = (await pg.ClaimAsync(10, Duration, Ct)).Single(x => x.Delivery.OutboxId == first.Delivery.OutboxId);
        Check(reclaimed.LeaseToken != first.LeaseToken && reclaimed.Attempt == first.Attempt + 1, "reclaim fencing");
        Check(Envelope(first.Delivery) == Envelope(reclaimed.Delivery), "reclaim envelope identity");
        Check(await pg.ConfirmPublishedAsync(reclaimed.Delivery.OutboxId, reclaimed.LeaseToken, new(new("test", 0, 9)), Ct), "persist broker ack");
        await using (var ack = source.CreateCommand($"SELECT broker_topic='test' AND broker_partition=0 AND broker_offset=9 FROM botgram_intake_v1.outbox WHERE id='{first.Delivery.OutboxId}'"))
            Check((bool)(await ack.ExecuteScalarAsync())!, "durable broker coordinates");
        Check(await pg.ReplayAsync(first.Delivery.OutboxId, Ct), "published replay");
        var replay = (await pg.ClaimAsync(10, Duration, Ct)).Single(x => x.Delivery.OutboxId == first.Delivery.OutboxId);
        Check(Envelope(replay.Delivery) == Envelope(first.Delivery), "manual replay identity");
        Check(await pg.FailAsync(replay.Delivery.OutboxId, replay.LeaseToken, DateTimeOffset.UtcNow, true, Ct), "block event");
        Check(!(await pg.ClaimAsync(10, Duration, Ct)).Any(x => x.Delivery.PartitionKey == "chat"), "blocked predecessor");
        Check(await pg.ReplayAsync(first.Delivery.OutboxId, Ct), "blocked replay");

        var generations = await Task.WhenAll(Enumerable.Range(0, 8).Select(_ => pg.GetActiveGenerationAsync("chat", Ct)));
        Check(generations.All(x => x != Guid.Empty && x == generations[0]), "concurrent initial generation allocation is nonempty and stable");
        var scope = new ConsumerScope("delivery-tests", generations[0]);
        var all = await pg.ReadAsync("chat", 0, long.MaxValue, 1000, Ct);
        Check(all.Select(x => x.ConversationSequence).SequenceEqual(Enumerable.Range(1, all.Count).Select(x => (long?)x)), "source ordered and continuous");
        var delivery = all[0];
        var unknown = Guid.NewGuid();
        await Reject(() => pg.StageAsync(scope with { Generation = Guid.Empty }, delivery, null, Ct));
        await Reject(() => pg.StageAsync(scope with { Generation = unknown }, delivery, null, Ct));
        var otherGeneration = await pg.GetActiveGenerationAsync("other", Ct);
        await Reject(() => pg.StageAsync(scope with { Generation = otherGeneration }, delivery, null, Ct));
        await using (var allocated = source.CreateCommand("""
            SELECT (SELECT count(*) FROM botgram_intake_v1.projection_generations WHERE conversation_id='chat')=1
                AND NOT EXISTS (SELECT 1 FROM botgram_intake_v1.projection_generations
                    WHERE generation='00000000-0000-0000-0000-000000000000')
                AND NOT EXISTS (SELECT 1 FROM botgram_intake_v1.delivery_inbox)
                AND NOT EXISTS (SELECT 1 FROM botgram_intake_v1.projection_tasks)
            """))
            Check((bool)(await allocated.ExecuteScalarAsync())!, "rejected stages must not allocate generations or create inbox/tasks");
        Check(await pg.StageAsync(scope, delivery, new("test", 0, 9), Ct) == InboxStageResult.Staged, "stage transaction");
        var reopened = new PostgresDeliveryPersistence(source);
        Check(await reopened.GetActiveGenerationAsync("chat", Ct) == scope.Generation, "active generation survives reopen");
        Check(await reopened.StageAsync(scope, delivery, new("test", 0, 11), Ct) == InboxStageResult.Duplicate, "durable duplicate after reopen/replay");
        await Reject(() => pg.StageAsync(scope, delivery with { Payload = ((Message)delivery.Payload) with { Text = "corrupt" } }, null, Ct));
        await Reject(() => pg.StageAsync(scope, delivery with { Recipients = [new("a", 999), new("b", 999)] }, null, Ct));
        await Reject(() => pg.StageAsync(scope, delivery with { EventId = delivery.EventId + 10000 }, null, Ct));
        await Reject(() => pg.StageAsync(scope, delivery with { SchemaVersion = 2 }, null, Ct));
        await Reject(() => pg.StageAsync(new("other-consumer", scope.Generation), delivery, null, Ct));
        await Reject(() => pg.StageAsync(scope, all[1], new("test", 0, 9), Ct));
        await pg.StageAsync(scope, all[1], null, Ct);
        var work = await pg.ClaimAsync(scope, 1000, Duration, Ct);
        var n1 = work.Single(x => x.Work.ConversationSequence == 1);
        var n2 = work.Single(x => x.Work.ConversationSequence == 2);
        Check(await pg.CompleteAsync(n2, Ct), "out of order completion");
        Check((await pg.CheckpointAsync("chat", scope.Generation, Ct)).ThroughSequence == 0, "checkpoint cannot jump gap");
        await Reject(() => pg.CompleteAsync(n1 with { Work = n1.Work with { Message = n1.Work.Message with { Text = "corrupt" } } }, Ct));
        await Sql($"UPDATE botgram_intake_v1.projection_tasks SET lease_until=clock_timestamp()-interval '1 second' WHERE outbox_id='{n1.Work.OutboxId}'");
        Check(!await pg.CompleteAsync(n1, Ct), "expired projection completion");
        Check(!await pg.RenewAsync(n1, Duration, Ct), "expired projection renew");
        Check(!await pg.RetryAsync(n1, DateTimeOffset.UtcNow, Ct), "expired projection retry");
        var fresh = (await pg.ClaimAsync(scope, 1000, Duration, Ct)).Single();
        Check(await pg.RetryAsync(fresh, DateTimeOffset.UtcNow.AddSeconds(-1), Ct), "projection retry");
        fresh = (await pg.ClaimAsync(scope, 1000, Duration, Ct)).Single();
        Check(await pg.CompleteAsync(fresh, Ct), "projection reclaim completion");
        Check((await pg.CheckpointAsync("chat", scope.Generation, Ct)).ThroughSequence == 2, "contiguous checkpoint advances through completed successor");

        var rebuild = await pg.BeginRebuildAsync("chat", 1, Ct);
        var competing = await pg.BeginRebuildAsync("chat", 1, Ct);
        async Task Fill(ProjectionRebuild target)
        {
            var consumer = new ConsumerScope("rebuild-tests", target.Generation);
            foreach (var item in await pg.ReadAsync("chat", 0, long.MaxValue, 1000, Ct))
                await pg.StageAsync(consumer, item, null, Ct);
            foreach (var item in await pg.ClaimAsync(consumer, 1000, Duration, Ct))
                Check(await pg.CompleteAsync(item, Ct), "rebuild completion");
        }
        await Fill(rebuild);
        await acceptance.AcceptAsync(Input("delivery-after-snapshot"), x => x, Ct);
        Check(!await pg.ActivateAsync(rebuild, Ct), "activation rejects new committed source gap");
        await Fill(rebuild);
        Check(!await reopened.ActivateAsync(rebuild, Ct), "activation requires actual external readback verifier");
        verifier.Result = false;
        Check(!await pg.ActivateAsync(rebuild, Ct), "validator rejection");
        verifier.Result = true;
        await Fill(competing);
        Check(await pg.ActivateAsync(rebuild, Ct), "validated activation");
        Check(verifier.LastSequence == rebuild.SnapshotSequence + 1, "validator receives CURRENT sequence under lock");
        Check(!await pg.ActivateAsync(competing, Ct), "competing active generation fencing");
        Check(await pg.GetActiveGenerationAsync("chat", Ct) == rebuild.Generation, "active generation follows activation");
        await Reject(() => pg.StageAsync(scope, delivery, null, Ct));
        await Reject(() => pg.StageAsync(new("rebuild-tests", competing.Generation), delivery, null, Ct));
        Check(await pg.StageAsync(new("rebuild-tests", rebuild.Generation), delivery, null, Ct) == InboxStageResult.Duplicate,
            "activated rebuild remains eligible for staging");
        Check((await pg.CheckpointAsync("chat", scope.Generation, Ct)).ThroughSequence == 2, "old generation retained");

        // PG-only tests use an explicitly fake validator to test orchestration, not Scylla durability.
        var locked = await pg.BeginRebuildAsync("chat", 1, Ct);
        await Fill(locked);
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        verifier.Gate = async () => { entered.SetResult(); await release.Task; };
        var activating = pg.ActivateAsync(locked, Ct);
        await entered.Task.WaitAsync(TimeSpan.FromSeconds(10));
        var send = acceptance.AcceptAsync(Input("delivery-during-activation"), x => x, Ct);
        await using (var monitor = source.CreateCommand("""
            SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND wait_event_type='Lock'
                AND query LIKE 'SELECT sequence FROM botgram_intake_v1.conversations%'
            """))
        {
            using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(10));
            while ((long)(await monitor.ExecuteScalarAsync(timeout.Token))! == 0)
                await Task.Delay(10, timeout.Token);
        }
        Check(!send.IsCompleted, "activation conversation lock must fence acceptance during external readback");
        release.SetResult();
        Check(await activating, "locked activation");
        await send;
        verifier.Gate = null;

        var count = await Count();
        var bounded = new PostgresMessageAcceptance(source, 1);
        try
        {
            await bounded.AcceptAsync(Input("over-capacity"), x => x, Ct);
            throw new InvalidOperationException("Expected capacity rejection");
        }
        catch (ApiException error) when (error.Status == 503 && error.Message == "storage_backlog_full") { }
        Check(await Count() == count, "capacity rejection added message");
        await Sql("UPDATE botgram_intake_v1.outbox SET state='Published',lease_token=NULL,lease_until=NULL");
        try
        {
            await bounded.AcceptAsync(Input("published-but-unprojected-capacity"), x => x, Ct);
            throw new InvalidOperationException("Expected unprojected capacity rejection");
        }
        catch (ApiException error) when (error.Status == 503 && error.Message == "storage_backlog_full") { }
        Check(await Count() == count, "published unprojected records must still consume capacity");
        Check((await bounded.AcceptAsync(Input("delivery-after-snapshot"), x => x, Ct)).Duplicate, "capacity must not reject duplicate recovery");
        async Task<long> Backlog()
        {
            await using var command = source.CreateCommand("""
                SELECT count(*) FROM botgram_intake_v1.outbox o
                WHERE o.state <> 'Published' OR NOT EXISTS (
                    SELECT 1 FROM botgram_intake_v1.projection_tasks t
                    JOIN botgram_intake_v1.projection_heads h ON h.conversation_id=t.conversation_id AND h.generation=t.generation
                    WHERE t.outbox_id=o.id AND t.state='Complete')
                """);
            return (long)(await command.ExecuteScalarAsync())!;
        }
        const int attempts = 12, available = 3;
        for (var i = 0; i < attempts; i++)
            await Sql($"""
                INSERT INTO botgram_intake_v1.sessions(token_hash,user_id,expires_at)
                    VALUES('capacity-session-{i}','a',now()+interval '1 hour');
                INSERT INTO botgram_intake_v1.conversations(id) VALUES('capacity-chat-{i}');
                INSERT INTO botgram_intake_v1.members(conversation_id,user_id) VALUES('capacity-chat-{i}','a');
                """);
        var initialBacklog = await Backlog();
        var capacity = checked((int)initialBacklog + available);
        var concurrentBounded = new PostgresMessageAcceptance(source, capacity);
        var start = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var concurrent = Enumerable.Range(0, attempts).Select(async i =>
        {
            await start.Task;
            try
            {
                var result = await concurrentBounded.AcceptAsync(new MessageAcceptance($"capacity-session-{i}", "a",
                    $"capacity-chat-{i}", new($"capacity-key-{i}", "capacity"), MessageWriteContract.Telegram), x => x, Ct);
                Check(!result.Duplicate, "capacity attempt must be a new message");
                return true;
            }
            catch (ApiException error) when (error.Status == 503 && error.Message == "storage_backlog_full") { return false; }
        }).ToArray();
        start.SetResult();
        var results = await Task.WhenAll(concurrent);
        Check(results.Count(x => x) == available && results.Count(x => !x) == attempts - available,
            "concurrent capacity decisions across sessions/conversations must reject excess writes");
        Check(await Backlog() == capacity && await Count() == count + available,
            "concurrent acceptance must not exceed backlog threshold or persist rejected messages");
        await using (var atomic = source.CreateCommand("""
            SELECT (SELECT count(*) FROM botgram_intake_v1.conversations WHERE id LIKE 'capacity-chat-%' AND sequence=1)=3
                AND NOT EXISTS (SELECT 1 FROM botgram_intake_v1.conversations WHERE id LIKE 'capacity-chat-%' AND sequence>1)
                AND (SELECT count(*) FROM botgram_intake_v1.events e JOIN botgram_intake_v1.messages m ON m.id=e.message_id
                    WHERE m.conversation_id LIKE 'capacity-chat-%')=3
                AND (SELECT count(*) FROM botgram_intake_v1.outbox WHERE partition_key LIKE 'capacity-chat-%')=3
            """))
            Check((bool)(await atomic.ExecuteScalarAsync())!, "capacity rejection preserves atomic sequence/event/outbox state");
        Console.WriteLine("PASS: PostgreSQL delivery, fencing, replay, checkpoint, activation and capacity tests (external validator is a test double)");
    }

    private static string Envelope(DeliveryEvent item) => JsonSerializer.Serialize(new { item.OutboxId, item.EventId,
        item.PartitionKey, Payload = (Message)item.Payload, item.Recipients, item.CreatedAt, item.ConversationSequence });
    private static void Check(bool value, string message)
    { if (!value) throw new InvalidOperationException(message); }
    private static async Task Reject(Func<Task> action)
    {
        try { await action(); }
        catch (Exception error) when (error is InvalidDataException or NotSupportedException) { return; }
        throw new InvalidOperationException("Expected content/schema conflict");
    }
    private sealed class RecordingValidator : IProjectionReadbackValidator
    {
        public bool Result { get; set; }
        public long LastSequence { get; private set; }
        public Func<Task>? Gate { get; set; }
        public async Task<bool> ValidateAsync(ProjectionRebuild rebuild, long throughSequence, CancellationToken cancellationToken)
        {
            LastSequence = throughSequence;
            if (Gate is not null) await Gate();
            return Result;
        }
    }
}
