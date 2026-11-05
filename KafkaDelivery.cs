using System.Text.Json;
using Confluent.Kafka;

namespace Botgram.Backend.Storage;

public static class DeliveryEventCodec
{
    private static readonly JsonSerializerOptions Json = new(JsonSerializerDefaults.Web);
    private sealed record Envelope(Guid OutboxId, int SchemaVersion, long EventId, string PartitionKey,
        string Type, JsonElement Payload, RecipientPosition[] Recipients, DateTimeOffset CreatedAt,
        DateTimeOffset? ExpiresAt, long? ConversationSequence, long? MessageVersion, int? ProjectionFormatVersion);

    public static byte[] Encode(DeliveryEvent delivery)
    {
        Validate(delivery);
        return JsonSerializer.SerializeToUtf8Bytes(new Envelope(delivery.OutboxId, delivery.SchemaVersion,
            delivery.EventId, delivery.PartitionKey, delivery.Type,
            JsonSerializer.SerializeToElement(delivery.Payload, delivery.Payload.GetType(), Json),
            delivery.Recipients.ToArray(), delivery.CreatedAt, delivery.ExpiresAt,
            delivery.ConversationSequence, delivery.MessageVersion, delivery.ProjectionFormatVersion), Json);
    }

    public static DeliveryEvent Decode(ReadOnlySpan<byte> data)
    {
        var value = JsonSerializer.Deserialize<Envelope>(data, Json) ?? throw new JsonException("Missing delivery");
        var payload = (IEventPayload?)value.Payload.Deserialize(EventPayloadTypes.For(value.Type), Json)
            ?? throw new JsonException("Missing delivery payload");
        var delivery = new DeliveryEvent(value.OutboxId, value.SchemaVersion, value.EventId, value.PartitionKey,
            value.Type, payload, value.Recipients, value.CreatedAt, value.ExpiresAt,
            value.ConversationSequence, value.MessageVersion, value.ProjectionFormatVersion);
        Validate(delivery);
        return delivery;
    }

    public static void Validate(DeliveryEvent delivery)
    {
        var schema = delivery.Type switch
        {
            "wallpaper.changed" => StorageContractVersions.WallpaperEventSchema,
            "user.presence_changed" => StorageContractVersions.PresenceEventSchema,
            "call.updated" or "call.tl_signal" => StorageContractVersions.CallEventSchema,
            _ => StorageContractVersions.EventSchema
        };
        // Legacy v1 snapshots stored call events without pts; they stay readable for replay.
        var legacyCall = delivery.Type is "call.updated" or "call.tl_signal" &&
            delivery.SchemaVersion == StorageContractVersions.EventSchema;
        if (delivery.SchemaVersion != schema && !legacyCall ||
            delivery.OutboxId == Guid.Empty ||
            delivery.EventId <= 0 || delivery.Payload is null ||
            EventPayloadTypes.For(delivery.Type) != delivery.Payload.GetType())
            throw new InvalidDataException("Invalid delivery identity, schema or type");
        var partition = delivery.Payload switch
        {
            Message m => m.ConversationId,
            Conversation c => c.Id,
            Receipt r => r.ConversationId,
            Call c => c.ConversationId,
            GidChanged g => "user:" + g.UserId,
            PresenceChanged p => "user:" + p.UserId,
            WallpaperChanged w => "user:" + w.UserId,
            CallSignal => delivery.PartitionKey,
            _ => throw new InvalidDataException("Unknown delivery payload")
        };
        if (string.IsNullOrWhiteSpace(partition) || partition != delivery.PartitionKey)
            throw new InvalidDataException("Invalid delivery partition");
        var message = delivery.Type == "message.created";
        if (message ? delivery.ConversationSequence is not > 0 || delivery.MessageVersion is not > 0 ||
                delivery.ProjectionFormatVersion != StorageContractVersions.HistoryProjection :
            delivery.ConversationSequence is not null || delivery.MessageVersion is not null || delivery.ProjectionFormatVersion is not null)
            throw new InvalidDataException("Invalid projection metadata");
        var hasPts = delivery.Type is "message.created" or "receipt.updated" or "user.gid_changed" or "user.presence_changed"
            || delivery.SchemaVersion == StorageContractVersions.CallEventSchema;
        if (delivery.Type == "call.tl_signal" && delivery.SchemaVersion == StorageContractVersions.CallEventSchema &&
            delivery.ExpiresAt is null)
            throw new InvalidDataException("Call signaling requires an expiry");
        string? previous = null;
        if (delivery.Recipients is null) throw new InvalidDataException("Missing recipients");
        if (delivery.Payload is WallpaperChanged changed &&
            (changed.Revision <= 0 || !Guid.TryParseExact(changed.UserId, "N", out _) ||
             changed.ConversationId is not null && !Guid.TryParseExact(changed.ConversationId, "N", out _) ||
             delivery.Recipients.Count != 1 || delivery.Recipients[0]?.UserId != changed.UserId))
            throw new InvalidDataException("Invalid wallpaper revision recipients");
        foreach (var recipient in delivery.Recipients)
        {
            if (recipient is null || string.IsNullOrEmpty(recipient.UserId) ||
                previous is not null && StringComparer.Ordinal.Compare(previous, recipient.UserId) >= 0 ||
                (hasPts ? recipient.Pts is not > 1 : recipient.Pts is not null))
                throw new InvalidDataException("Invalid recipients");
            previous = recipient.UserId;
        }
    }
}

public sealed class KafkaDeliveryPublisher : IDurableEventPublisher, IDisposable
{
    private readonly IProducer<string, byte[]> producer;
    private readonly string topic;

    public KafkaDeliveryPublisher(ProducerConfig config, string topic)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(topic);
        this.topic = topic;
        var settings = new ProducerConfig(config) { EnableIdempotence = true, Acks = Acks.All };
        producer = new ProducerBuilder<string, byte[]>(settings).Build();
    }

    public async Task<BrokerAcknowledgement> PublishAsync(DeliveryEvent delivery, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var result = await producer.ProduceAsync(topic, new Message<string, byte[]>
        {
            Key = delivery.PartitionKey, Value = DeliveryEventCodec.Encode(delivery)
        }, cancellationToken);
        cancellationToken.ThrowIfCancellationRequested();
        if (result.Status != PersistenceStatus.Persisted) throw new IOException("Kafka delivery was not persisted");
        return new(new(result.Topic, result.Partition.Value, result.Offset.Value));
    }

    public void Dispose() => producer.Dispose();
}

// Synchronous on purpose: Consume, durable Stage and Commit all stay on the caller's single thread.
// A failed batch is terminal for this instance; reopening resumes from the last committed offset.
public sealed class KafkaInboxConsumer : IDisposable
{
    private readonly IConsumer<string, byte[]> consumer;
    private readonly IEventInboxPersistence inbox;
    private readonly ConsumerScope scope;
    private readonly Func<string, CancellationToken, Task<ConsumerScope>>? scopeResolver;
    private bool faulted;
    private int running;

    public KafkaInboxConsumer(ConsumerConfig config, string topic, ConsumerScope scope, IEventInboxPersistence inbox,
        Func<string, CancellationToken, Task<ConsumerScope>>? scopeResolver = null)
    {
        if (scope.Generation == Guid.Empty || string.IsNullOrWhiteSpace(scope.Name))
            throw new ArgumentException("Explicit consumer scope and generation required", nameof(scope));
        ArgumentException.ThrowIfNullOrWhiteSpace(topic);
        this.scope = scope;
        this.scopeResolver = scopeResolver;
        this.inbox = inbox;
        var settings = new ConsumerConfig(config)
        {
            EnableAutoCommit = false, EnableAutoOffsetStore = false,
            AutoOffsetReset = AutoOffsetReset.Error
        };
        consumer = new ConsumerBuilder<string, byte[]>(settings).Build();
        consumer.Subscribe(topic);
    }

    public int ConsumeBatch(int limit, TimeSpan pollTimeout, CancellationToken cancellationToken)
    {
        if (limit <= 0 || pollTimeout < TimeSpan.Zero) throw new ArgumentOutOfRangeException(nameof(limit));
        if (Interlocked.Exchange(ref running, 1) != 0) throw new InvalidOperationException("Consumer already running");
        try
        {
            if (faulted) throw new InvalidOperationException("Reopen consumer after failed Stage or Commit");
            var count = 0;
            while (count < limit)
            {
                cancellationToken.ThrowIfCancellationRequested();
                var record = consumer.Consume(pollTimeout);
                if (record is null) break;
                var delivery = DeliveryEventCodec.Decode(record.Message.Value);
                if (record.Message.Key != delivery.PartitionKey) throw new InvalidDataException("Kafka key mismatch");
                var resolved = scopeResolver is null ? scope :
                    scopeResolver(delivery.PartitionKey, cancellationToken).GetAwaiter().GetResult();
                if (resolved.Generation == Guid.Empty || resolved.Name != scope.Name)
                    throw new InvalidDataException("Resolver must retain consumer name and return an allocated generation");
                inbox.StageAsync(resolved, delivery, new(record.Topic, record.Partition.Value, record.Offset.Value),
                    cancellationToken).GetAwaiter().GetResult();
                cancellationToken.ThrowIfCancellationRequested();
                consumer.Commit([new TopicPartitionOffset(record.TopicPartition, new Offset(checked(record.Offset.Value + 1)))]);
                count++;
            }
            return count;
        }
        catch { faulted = true; throw; }
        finally { Volatile.Write(ref running, 0); }
    }

    public void Dispose() { consumer.Close(); consumer.Dispose(); }
}
