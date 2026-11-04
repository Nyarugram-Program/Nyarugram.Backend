namespace Botgram.Backend.Storage;

public sealed record DeliveryWorkerOptions
{
    public int BatchSize { get; init; } = 100;
    public TimeSpan LeaseDuration { get; init; } = TimeSpan.FromMinutes(1);
    public TimeSpan RetryDelay { get; init; } = TimeSpan.FromSeconds(1);
    public TimeSpan MaximumRetryDelay { get; init; } = TimeSpan.FromMinutes(1);
    public int MaximumPublishAttempts { get; init; } = 10;

    internal void Validate()
    {
        if (BatchSize is < 1 or > 1000 || LeaseDuration <= TimeSpan.Zero || LeaseDuration > TimeSpan.FromHours(1) ||
            RetryDelay <= TimeSpan.Zero || MaximumRetryDelay < RetryDelay || MaximumRetryDelay > TimeSpan.FromDays(1) ||
            MaximumPublishAttempts <= 0)
            throw new ArgumentOutOfRangeException(nameof(DeliveryWorkerOptions));
    }

    internal DateTimeOffset RetryAt(int attempt) => DateTimeOffset.UtcNow.AddMilliseconds(
        Math.Min(MaximumRetryDelay.TotalMilliseconds, RetryDelay.TotalMilliseconds * Math.Pow(2, Math.Clamp(attempt - 1, 0, 30))));
}

public sealed class OutboxPublisherWorker
{
    private readonly IOutboxPersistence outbox;
    private readonly IDurableEventPublisher publisher;
    private readonly DeliveryWorkerOptions options;

    public OutboxPublisherWorker(IOutboxPersistence outbox, IDurableEventPublisher publisher,
        DeliveryWorkerOptions? options = null)
    {
        this.outbox = outbox;
        this.publisher = publisher;
        this.options = options ?? new();
        this.options.Validate();
    }

    public async Task<int> RunBatchAsync(CancellationToken cancellationToken = default)
    {
        var leases = await outbox.ClaimAsync(options.BatchSize, options.LeaseDuration, cancellationToken);
        var confirmed = 0;
        foreach (var lease in leases)
        {
            cancellationToken.ThrowIfCancellationRequested();
            // Refresh claims queued behind slow external writes; stale leases never reach the publisher.
            if (!await outbox.RenewAsync(lease.Delivery.OutboxId, lease.LeaseToken, options.LeaseDuration, cancellationToken)) continue;
            try
            {
                var acknowledgement = await publisher.PublishAsync(lease.Delivery, cancellationToken);
                cancellationToken.ThrowIfCancellationRequested();
                if (await outbox.ConfirmPublishedAsync(lease.Delivery.OutboxId, lease.LeaseToken, acknowledgement, cancellationToken))
                    confirmed++;
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested) { throw; }
            catch (Exception)
            {
                // A lost acknowledgement/commit may have succeeded externally. Retain the original identity on retry.
                await outbox.FailAsync(lease.Delivery.OutboxId, lease.LeaseToken, options.RetryAt(lease.Attempt),
                    lease.Attempt >= options.MaximumPublishAttempts, cancellationToken);
            }
        }
        return confirmed;
    }
}

public sealed class ProjectionWorker
{
    private readonly IProjectionProgressPersistence progress;
    private readonly IMessageProjection projection;
    private readonly ConsumerScope scope;
    private readonly DeliveryWorkerOptions options;

    public ProjectionWorker(IProjectionProgressPersistence progress, IMessageProjection projection, ConsumerScope scope,
        DeliveryWorkerOptions? options = null)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(scope.Name);
        if (scope.Generation == Guid.Empty) throw new ArgumentException("Allocated generation required", nameof(scope));
        this.progress = progress;
        this.projection = projection;
        this.scope = scope;
        this.options = options ?? new();
        this.options.Validate();
    }

    public async Task<int> RunBatchAsync(CancellationToken cancellationToken = default)
    {
        var leases = await progress.ClaimAsync(scope, options.BatchSize, options.LeaseDuration, cancellationToken);
        var completed = 0;
        foreach (var lease in leases)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (!await progress.RenewAsync(lease, options.LeaseDuration, cancellationToken)) continue;
            try
            {
                await projection.UpsertAsync(lease.Work, cancellationToken);
                cancellationToken.ThrowIfCancellationRequested();
                if (await progress.CompleteAsync(lease, cancellationToken)) completed++;
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested) { throw; }
            catch (Exception)
            {
                await progress.RetryAsync(lease, options.RetryAt(lease.Attempt), cancellationToken);
            }
        }
        return completed;
    }
}

// Each call stages one bounded authoritative page. Keep this scope fixed throughout recovery/rebuild;
// an active-generation resolver belongs only on the live Kafka ingestion path.
public sealed class ProjectionRecoveryCoordinator
{
    private readonly IProjectionSource source;
    private readonly IEventInboxPersistence inbox;
    private readonly ConsumerScope scope;
    private readonly int batchSize;

    public ProjectionRecoveryCoordinator(IProjectionSource source, IEventInboxPersistence inbox, ConsumerScope scope,
        int batchSize = 100)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(scope.Name);
        if (scope.Generation == Guid.Empty) throw new ArgumentException("Allocated generation required", nameof(scope));
        if (batchSize is < 1 or > 1000) throw new ArgumentOutOfRangeException(nameof(batchSize));
        this.source = source;
        this.inbox = inbox;
        this.scope = scope;
        this.batchSize = batchSize;
    }

    public async Task<long> StageBatchAsync(string conversationId, long afterSequence, long throughSequence,
        CancellationToken cancellationToken = default)
    {
        if (afterSequence < 0 || throughSequence < afterSequence) throw new ArgumentOutOfRangeException(nameof(afterSequence));
        var page = await source.ReadAsync(conversationId, afterSequence, throughSequence, batchSize, cancellationToken);
        if (page.Count > batchSize) throw new InvalidDataException("Source exceeded recovery page limit");
        foreach (var delivery in page)
        {
            if (delivery.PartitionKey != conversationId || delivery.ConversationSequence != afterSequence + 1 ||
                delivery.ConversationSequence > throughSequence)
                throw new InvalidDataException("Recovery source sequence gap or conversation mismatch");
            await inbox.StageAsync(scope, delivery, null, cancellationToken);
            afterSequence = delivery.ConversationSequence.Value;
        }
        if (afterSequence < throughSequence && page.Count == 0)
            throw new InvalidDataException("Recovery source ended before requested boundary");
        return afterSequence;
    }
}
