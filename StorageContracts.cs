namespace Botgram.Backend.Storage;

// Frozen v1 contracts, not service registrations or claims of implemented adapters.
public static class StorageContractVersions
{
    public const int EventSchema = 1;
    public const int WallpaperEventSchema = 2;
    public const int HistoryProjection = 1;
    public const long InitialMessageVersion = 1;
    public const int MessagesPerBucket = 10_000;
    public static readonly TimeSpan HistoryCursorLifetime = TimeSpan.FromMinutes(15);
}

public sealed record MessageAcceptance(string SessionTokenHash, string UserId, string ConversationId,
    SendMessageCommand Command, MessageWriteContract Contract);
// Pts is the sender's CURRENT pts, including on a duplicate; it is not the original event's pts.
public sealed record AcceptedMessage(Message Message, bool Duplicate, int Pts);
public sealed record MessageDeduplicationKey(string ConversationId, string SenderId, string ClientKey);
public sealed record RecipientPosition(string UserId, int? Pts);

// Internal cursor state: the transport must authenticate an opaque encoding, never trust caller fields.
// Query is applied once; LastMessageId continues the resulting descending stream, not AddOffset again.
public sealed record HistoryCursor(int FormatVersion, string UserId, string ConversationId, HistoryQuery Query,
    Guid Generation, long SnapshotSequence, long ProjectedThroughSequence, long LastMessageId,
    DateTimeOffset ExpiresAt);
public sealed record HistoryPage(IReadOnlyList<Message> Messages, HistoryCursor? NextCursor);
public sealed record ProjectionCheckpoint(string ConversationId, Guid Generation, int FormatVersion,
    long ThroughSequence);
public sealed record ProjectionWork(Guid OutboxId, long EventId, Message Message, long ConversationSequence,
    long MessageVersion, int FormatVersion, Guid Generation);

// Immutable across retries/replays. EventId is a PG commit cursor, NOT a Kafka offset or a message ID.
public sealed record DeliveryEvent(Guid OutboxId, int SchemaVersion, long EventId, string PartitionKey,
    string Type, IEventPayload Payload, IReadOnlyList<RecipientPosition> Recipients,
    DateTimeOffset CreatedAt, DateTimeOffset? ExpiresAt,
    long? ConversationSequence, long? MessageVersion, int? ProjectionFormatVersion);
public sealed record BrokerPosition(string Topic, int Partition, long Offset);
public sealed record BrokerAcknowledgement(BrokerPosition Position);

public enum OutboxState { Pending, Leased, Published, Blocked }
public sealed record OutboxLease(DeliveryEvent Delivery, Guid LeaseToken, DateTimeOffset LeaseUntil, int Attempt);
public sealed record ConsumerScope(string Name, Guid Generation);
public enum InboxStageResult { Staged, Duplicate }
public sealed record ProjectionLease(ConsumerScope Consumer, ProjectionWork Work, Guid LeaseToken,
    DateTimeOffset LeaseUntil, int Attempt);
public sealed record ProjectionRebuild(string ConversationId, Guid Generation, Guid ExpectedActiveGeneration,
    int FormatVersion, long SnapshotSequence);

public interface IMessageAcceptancePersistence
{
    // Authorization, durable deduplication, pts/difference and outbox share one PG commit.
    // prepareResponse runs before commit; exceptions roll back. Ambiguous commit is NOT a rejection.
    Task<T> AcceptAsync<T>(MessageAcceptance input, Func<AcceptedMessage, T> prepareResponse,
        CancellationToken cancellationToken);
}

public interface IMessagePersistence : IMessageAcceptancePersistence
{
    // Reauthorize every page. Merge Scylla <= pinned watermark with PG above it, within the snapshot.
    Task<HistoryPage> HistoryAsync(string userId, string conversationId, HistoryQuery query,
        HistoryCursor? cursor, CancellationToken cancellationToken);
    Task<ReadResult> ReadThroughAsync(string sessionTokenHash, string userId, string conversationId,
        long maxMessageId, CancellationToken cancellationToken);
}

public interface ISyncPersistence
{
    // A consistent committed PG snapshot; durable stream entries do not expire with notifications.
    Task<int> PtsAsync(string userId, CancellationToken cancellationToken);
    Task<DifferenceResult> DifferenceAsync(string userId, int pts, CancellationToken cancellationToken);
    Task<EventPage> EventsAsync(string userId, long after, int limit, CancellationToken cancellationToken);
}

public interface IMessageProjection
{
    // Return only after quorum durability. Same version/content is a no-op; conflicting content fails.
    // Older versions never replace newer ones. Key includes generation, conversation, bucket, message ID.
    Task UpsertAsync(ProjectionWork work, CancellationToken cancellationToken);
}

public interface IDurableEventPublisher
{
    // Idempotent producer, acks=all; timeout is ambiguous. Ack is not a consumer/projection completion.
    Task<BrokerAcknowledgement> PublishAsync(DeliveryEvent delivery, CancellationToken cancellationToken);
}

public interface IOutboxPersistence
{
    // Claim only each partition key's oldest unpublished event; expired leases may be reclaimed.
    Task<IReadOnlyList<OutboxLease>> ClaimAsync(int limit, TimeSpan leaseDuration,
        CancellationToken cancellationToken);
    Task<bool> RenewAsync(Guid outboxId, Guid leaseToken, TimeSpan leaseDuration,
        CancellationToken cancellationToken);
    // False means stale/expired lease; all mutations compare the current token and PG clock.
    Task<bool> ConfirmPublishedAsync(Guid outboxId, Guid leaseToken, BrokerAcknowledgement acknowledgement,
        CancellationToken cancellationToken);
    // A blocked event is retained and prevents later events in its partition from passing it.
    Task<bool> FailAsync(Guid outboxId, Guid leaseToken, DateTimeOffset retryAt, bool blocked,
        CancellationToken cancellationToken);
    // Manual replay retains envelope/identity; only Published or Blocked may return to Pending.
    Task<bool> ReplayAsync(Guid outboxId, CancellationToken cancellationToken);
}

public interface IEventInboxPersistence
{
    // Atomically persist inbox deduplication AND recoverable projection work in PG before offset commit.
    // Key = (consumer name, generation, OutboxId); also enforce unique EventId and envelope equality.
    // Unsupported schema/type fails without staging or acknowledging. Duplicate still validates content.
    // Position is null for authoritative PG recovery/rebuild, which has no Kafka offset to commit.
    Task<InboxStageResult> StageAsync(ConsumerScope consumer, DeliveryEvent delivery, BrokerPosition? position,
        CancellationToken cancellationToken);
}

public interface IProjectionSource
{
    // Committed source envelopes in sequence order: afterSequence < sequence <= throughSequence.
    // Includes service messages, is bounded by limit, and never manufactures a new event identity.
    Task<IReadOnlyList<DeliveryEvent>> ReadAsync(string conversationId, long afterSequence,
        long throughSequence, int limit, CancellationToken cancellationToken);
}

public interface IProjectionProgressPersistence
{
    Task<IReadOnlyList<ProjectionLease>> ClaimAsync(ConsumerScope consumer, int limit, TimeSpan leaseDuration,
        CancellationToken cancellationToken);
    Task<bool> RenewAsync(ProjectionLease lease, TimeSpan leaseDuration, CancellationToken cancellationToken);
    // Called AFTER Scylla quorum success. Completion and contiguous checkpoint advance share a PG commit.
    // Completing N+1 cannot advance past a missing N. Stale/expired tokens return false.
    Task<bool> CompleteAsync(ProjectionLease lease, CancellationToken cancellationToken);
    Task<bool> RetryAsync(ProjectionLease lease, DateTimeOffset retryAt, CancellationToken cancellationToken);
    Task<ProjectionCheckpoint> CheckpointAsync(string conversationId, Guid generation,
        CancellationToken cancellationToken);
    // Allocate an isolated generation and pin authoritative source data at a committed snapshot.
    Task<ProjectionRebuild> BeginRebuildAsync(string conversationId, int formatVersion,
        CancellationToken cancellationToken);
    // Compare active generation and catch up through CURRENT committed sequence under conversation lock.
    // Activation also requires validated content and preserved source/cursor retention; otherwise false.
    Task<bool> ActivateAsync(ProjectionRebuild rebuild, CancellationToken cancellationToken);
}
