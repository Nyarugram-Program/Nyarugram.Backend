using System.Text.Json;
using Npgsql;

namespace Botgram.Backend.Storage;

/// <summary>
/// Tuning for the PostgreSQL-only outbox completion worker. Every key lives in the
/// Storage section; out-of-range values are clamped to safe bounds.
/// </summary>
public sealed record RelationalOutboxOptions(
    bool Enabled = true,
    int IntervalMs = 250,
    int BatchSize = 100,
    int MaxBatchesPerCycle = 20,
    int StartDelayMs = 0,
    int LeaseSeconds = 30,
    string ConsumerName = "relational-outbox-v1")
{
    public static RelationalOutboxOptions Load(IConfiguration configuration) => new(
        configuration.GetValue<bool?>("Storage:RelationalOutboxCompletionEnabled") ?? true,
        Math.Clamp(configuration.GetValue<int?>("Storage:RelationalOutboxCompletionIntervalMs") ?? 250, 1, 60_000),
        Math.Clamp(configuration.GetValue<int?>("Storage:RelationalOutboxCompletionBatchSize") ?? 100, 1, 1000),
        Math.Clamp(configuration.GetValue<int?>("Storage:RelationalOutboxCompletionMaxBatchesPerCycle") ?? 20, 1, 200),
        Math.Clamp(configuration.GetValue<int?>("Storage:RelationalOutboxCompletionStartDelayMs") ?? 0, 0, 600_000),
        Math.Clamp(configuration.GetValue<int?>("Storage:RelationalOutboxCompletionLeaseSeconds") ?? 30, 5, 300));
}

public sealed record RelationalOutboxMetricsSnapshot(
    long Completed, long Failures, long Pending, string State, string? LastError, long Cycles);

/// <summary>
/// Thread-safe counters for /health. Longs use Interlocked; the string fields are
/// guarded by a small lock so a snapshot never mixes a stale state with a new error.
/// </summary>
public sealed class RelationalOutboxMetrics
{
    private long completed;
    private long failures;
    private long pending;
    private long cycles;
    private readonly object gate = new();
    private string state = "starting";
    private string? lastError;

    public long Completed => Interlocked.Read(ref completed);
    public long Failures => Interlocked.Read(ref failures);
    public long Pending => Interlocked.Read(ref pending);
    public long Cycles => Interlocked.Read(ref cycles);
    public string State { get { lock (gate) return state; } }
    public string? LastError { get { lock (gate) return lastError; } }

    public void RecordCompleted() => Interlocked.Increment(ref completed);

    public void RecordFailure(string? error)
    {
        Interlocked.Increment(ref failures);
        if (!string.IsNullOrEmpty(error)) lock (gate) lastError = error;
    }

    public void RecordCycle() => Interlocked.Increment(ref cycles);

    public void SetPending(long value) => Interlocked.Exchange(ref pending, value);

    public void SetState(string value) { lock (gate) state = value; }

    public void RecordError(string? error)
    {
        lock (gate) { state = "error"; lastError = error; }
    }

    public RelationalOutboxMetricsSnapshot Snapshot()
    {
        lock (gate)
            return new(Interlocked.Read(ref completed), Interlocked.Read(ref failures),
                Interlocked.Read(ref pending), state, lastError, Interlocked.Read(ref cycles));
    }
}

/// <summary>
/// RelationalBackend has no Kafka/Scylla/Redis workers, so outbox rows would stay
/// unfinished forever and eventually trigger storage_backlog_full. This service
/// closes the loop entirely inside PostgreSQL: claim -> durable inbox staging ->
/// verified projection completion -> publication. state='Published' is written
/// LAST, so PostgresMessageAcceptance.CapacityCountSql never mistakes an
/// intermediate state for completion. Broker coordinates intentionally stay NULL.
/// </summary>
public sealed class RelationalOutboxCompletionService : BackgroundService
{
    private readonly NpgsqlDataSource dataSource;
    private readonly RelationalOutboxOptions options;
    private readonly RelationalOutboxMetrics metrics;
    private readonly ILogger<RelationalOutboxCompletionService> logger;
    private readonly PostgresDeliveryPersistence persistence;

    public RelationalOutboxCompletionService(NpgsqlDataSource dataSource, RelationalOutboxOptions options,
        RelationalOutboxMetrics metrics, ILogger<RelationalOutboxCompletionService> logger)
    {
        this.dataSource = dataSource;
        this.options = options;
        this.metrics = metrics;
        this.logger = logger;
        persistence = new PostgresDeliveryPersistence(dataSource);
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        if (!options.Enabled)
        {
            metrics.SetState("disabled");
            // Keep the HostedService alive without ever claiming a row.
            try { await Task.Delay(Timeout.Infinite, stoppingToken); }
            catch (OperationCanceledException) { }
            return;
        }
        metrics.SetState("starting");
        try
        {
            if (options.StartDelayMs > 0) await Task.Delay(options.StartDelayMs, stoppingToken);
            using var timer = new PeriodicTimer(TimeSpan.FromMilliseconds(options.IntervalMs));
            while (await timer.WaitForNextTickAsync(stoppingToken))
                await RunCycleAsync(stoppingToken);
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested) { }
    }

    /// <summary>Manually run one cycle; returns the number of rows published this cycle.</summary>
    public Task<int> RunCycleOnceAsync(CancellationToken cancellationToken) => RunCycleAsync(cancellationToken);

    private async Task<int> RunCycleAsync(CancellationToken cancellationToken)
    {
        var published = 0;
        try
        {
            var leaseDuration = TimeSpan.FromSeconds(options.LeaseSeconds);
            for (var batch = 0; batch < options.MaxBatchesPerCycle; batch++)
            {
                cancellationToken.ThrowIfCancellationRequested();
                var leases = await persistence.ClaimAsync(options.BatchSize, leaseDuration, cancellationToken);
                if (leases.Count == 0) break;
                published += await ProcessBatchAsync(leases, leaseDuration, cancellationToken);
            }
            // The pending count scans the same predicate as admission, so refresh it
            // on activity or about once a second instead of four times per second.
            if (published > 0 || metrics.Cycles % 4 == 0)
                metrics.SetPending(await CountPendingAsync(cancellationToken));
            metrics.SetState("running");
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested) { throw; }
        catch (Exception exception)
        {
            logger.LogError(exception, "Relational outbox completion cycle failed");
            metrics.RecordError(exception.Message);
        }
        metrics.RecordCycle();
        return published;
    }

    private async Task<int> ProcessBatchAsync(IReadOnlyList<OutboxLease> leases,
        TimeSpan leaseDuration, CancellationToken cancellationToken)
    {
        var staged = new List<(ConsumerScope Scope, OutboxLease Lease)>();
        foreach (var lease in leases)
        {
            cancellationToken.ThrowIfCancellationRequested();
            try
            {
                var generation = await persistence.GetActiveGenerationAsync(lease.Delivery.PartitionKey, cancellationToken);
                var scope = new ConsumerScope(options.ConsumerName, generation);
                // Staged or Duplicate both mean the inbox row is durable. A replay of an
                // already-consumed event returns Duplicate and is still safe to publish.
                await persistence.StageAsync(scope, lease.Delivery, null, cancellationToken);
                staged.Add((scope, lease));
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested) { throw; }
            catch (Exception exception)
            {
                logger.LogWarning(exception, "Relational outbox staging failed for {OutboxId}", lease.Delivery.OutboxId);
                metrics.RecordFailure(exception.Message);
                await ReleaseOutboxAsync(lease);
            }
        }
        if (staged.Count == 0) return 0;

        var messageIds = staged.Where(entry => entry.Lease.Delivery.Payload is Message)
            .Select(entry => entry.Lease.Delivery.OutboxId).ToHashSet();
        var projected = new HashSet<Guid>();

        // A replayed/restarted row may already have a Complete projection task. ClaimAsync
        // only returns Pending/Leased tasks, so recognise Complete here instead of re-claiming.
        foreach (var entry in staged)
            if (entry.Lease.Delivery.Payload is Message && await AlreadyProjectedAsync(entry.Scope, entry.Lease, cancellationToken))
                projected.Add(entry.Lease.Delivery.OutboxId);

        var safety = options.MaxBatchesPerCycle * options.BatchSize;
        foreach (var scope in staged.Where(entry => entry.Lease.Delivery.Payload is Message)
                     .Select(entry => entry.Scope).Distinct())
        {
            var remaining = staged.Count(entry => entry.Scope == scope &&
                messageIds.Contains(entry.Lease.Delivery.OutboxId) && !projected.Contains(entry.Lease.Delivery.OutboxId));
            var processed = 0;
            while (remaining > 0 && processed < safety)
            {
                cancellationToken.ThrowIfCancellationRequested();
                var work = await persistence.ClaimAsync(scope, options.BatchSize, leaseDuration, cancellationToken);
                if (work.Count == 0) break;
                processed += work.Count;
                foreach (var projection in work)
                {
                    if (!messageIds.Contains(projection.Work.OutboxId))
                    {
                        // Not staged in this cycle: hand it back for a later cycle.
                        await ReleaseProjectionAsync(projection);
                        continue;
                    }
                    try
                    {
                        if (await persistence.CompleteAsync(projection, cancellationToken))
                        {
                            projected.Add(projection.Work.OutboxId);
                            remaining--;
                        }
                        else await ReleaseProjectionAsync(projection);
                    }
                    catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested) { throw; }
                    catch (Exception exception)
                    {
                        logger.LogWarning(exception, "Relational projection completion failed for {OutboxId}", projection.Work.OutboxId);
                        metrics.RecordFailure(exception.Message);
                        await ReleaseProjectionAsync(projection);
                    }
                }
            }
        }

        var published = 0;
        foreach (var (_, lease) in staged)
        {
            cancellationToken.ThrowIfCancellationRequested();
            // Non-message events only need durable staging (StageAsync set consumed=true).
            // Messages additionally require a verified Complete projection task.
            if (lease.Delivery.Payload is Message && !projected.Contains(lease.Delivery.OutboxId))
            {
                await ReleaseOutboxAsync(lease);
                continue;
            }
            try
            {
                if (await persistence.ConfirmRelationalPublishedAsync(lease.Delivery.OutboxId, lease.LeaseToken, cancellationToken))
                {
                    metrics.RecordCompleted();
                    published++;
                }
                else metrics.RecordFailure("Stale or expired lease during relational publication");
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested) { throw; }
            catch (Exception exception)
            {
                logger.LogWarning(exception, "Relational publication failed for {OutboxId}", lease.Delivery.OutboxId);
                metrics.RecordFailure(exception.Message);
                await ReleaseOutboxAsync(lease);
            }
        }
        return published;
    }

    private async Task<bool> AlreadyProjectedAsync(ConsumerScope scope, OutboxLease lease, CancellationToken cancellationToken)
    {
        var delivery = lease.Delivery;
        if (delivery.Payload is not Message message) return false;
        await using var connection = await dataSource.OpenConnectionAsync(cancellationToken);
        await using var command = new NpgsqlCommand("""
            SELECT 1 FROM botgram_intake_v1.projection_tasks t
            JOIN botgram_intake_v1.projection_heads h
                ON h.conversation_id=t.conversation_id AND h.generation=t.generation
            WHERE t.outbox_id=$1 AND t.consumer_name=$2 AND t.state='Complete'
                AND t.event_id=$3 AND t.conversation_id=$4 AND t.sequence=$5
                AND t.message=$6::jsonb AND t.message_version=$7 AND t.format=$8 AND t.generation=$9
            """, connection);
        foreach (var value in new object[]
        {
            delivery.OutboxId, scope.Name, delivery.EventId, delivery.PartitionKey,
            delivery.ConversationSequence!.Value, JsonSerializer.Serialize(message),
            delivery.MessageVersion!.Value, delivery.ProjectionFormatVersion!.Value, scope.Generation
        })
            command.Parameters.Add(new NpgsqlParameter { Value = value });
        return (await command.ExecuteScalarAsync(cancellationToken)) is not null;
    }

    private async Task<long> CountPendingAsync(CancellationToken cancellationToken)
    {
        await using var connection = await dataSource.OpenConnectionAsync(cancellationToken);
        await using var command = new NpgsqlCommand(PostgresMessageAcceptance.CapacityCountSql, connection);
        return (long)(await command.ExecuteScalarAsync(cancellationToken))!;
    }

    private async Task ReleaseOutboxAsync(OutboxLease lease)
    {
        try
        {
            // Best effort and uncoupled from shutdown: never let cancellation skip the release.
            await persistence.FailAsync(lease.Delivery.OutboxId, lease.LeaseToken,
                DateTimeOffset.UtcNow.AddSeconds(2), false, CancellationToken.None);
        }
        catch (Exception exception)
        { logger.LogDebug(exception, "Could not release outbox lease {OutboxId}", lease.Delivery.OutboxId); }
    }

    private async Task ReleaseProjectionAsync(ProjectionLease lease)
    {
        try
        {
            await persistence.RetryAsync(lease, DateTimeOffset.UtcNow.AddSeconds(2), CancellationToken.None);
        }
        catch (Exception exception)
        { logger.LogDebug(exception, "Could not release projection lease {OutboxId}", lease.Work.OutboxId); }
    }
}
