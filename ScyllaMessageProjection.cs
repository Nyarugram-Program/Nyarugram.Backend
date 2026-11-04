using System.Text.Json;
using System.Text.RegularExpressions;
using Cassandra;
using ISession = Cassandra.ISession;

namespace Botgram.Backend.Storage;

// Activate implementations must call this under their authoritative conversation lock,
// with the CURRENT committed sequence, not a value sampled before taking the lock.
public interface IProjectionReadbackValidator
{
    Task<bool> ValidateAsync(ProjectionRebuild rebuild, long throughSequence, CancellationToken cancellationToken);
}

public sealed record ScyllaProjectionOptions(string Keyspace, string LocalDatacenter, int ReplicationFactor = 3);
public sealed record ProjectedMessage(ProjectionWork Work);

public sealed class ScyllaMessageProjection : IMessageProjection
{
    private static readonly JsonSerializerOptions Json = new(JsonSerializerDefaults.Web);
    private readonly ISession session;
    private readonly ScyllaProjectionOptions options;
    private readonly string table;

    // Session/cluster lifetime belongs to the composition root; construction does not create schema.
    public ScyllaMessageProjection(ISession session, ScyllaProjectionOptions options)
    {
        if (!Regex.IsMatch(options.Keyspace, "^[a-z][a-z0-9_]{0,47}$", RegexOptions.CultureInvariant) ||
            !Regex.IsMatch(options.LocalDatacenter, "^[a-zA-Z0-9_-]+$", RegexOptions.CultureInvariant) ||
            options.ReplicationFactor <= 0)
            throw new ArgumentException("Invalid Scylla keyspace, datacenter or replication factor");
        this.session = session;
        this.options = options;
        table = options.Keyspace + ".message_projection_v1";
    }

    public async Task InitializeAsync(CancellationToken cancellationToken)
    {
        await ExecuteAsync(new SimpleStatement($"CREATE KEYSPACE IF NOT EXISTS {options.Keyspace} WITH replication = " +
            $"{{'class':'NetworkTopologyStrategy','{options.LocalDatacenter}':{options.ReplicationFactor}}} " +
            "AND tablets = {'enabled': false}"), cancellationToken);
        await ExecuteAsync(new SimpleStatement($"CREATE TABLE IF NOT EXISTS {table} (generation uuid, conversation_id text, " +
            "bucket bigint, message_id bigint, sequence bigint, message_version bigint, format_version int, " +
            "outbox_id uuid, event_id bigint, payload text, PRIMARY KEY ((generation, conversation_id, bucket), message_id)) " +
            "WITH CLUSTERING ORDER BY (message_id DESC)"), cancellationToken);
    }

    public async Task UpsertAsync(ProjectionWork work, CancellationToken cancellationToken)
    {
        Validate(work);
        var bucket = Bucket(work.ConversationSequence);
        var payload = JsonSerializer.Serialize(work.Message, Json);
        var insert = new SimpleStatement($"INSERT INTO {table} (generation, conversation_id, bucket, message_id, sequence, " +
            "message_version, format_version, outbox_id, event_id, payload) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?) IF NOT EXISTS",
            work.Generation, work.Message.ConversationId, bucket, work.Message.Id, work.ConversationSequence,
            work.MessageVersion, work.FormatVersion, work.OutboxId, work.EventId, payload);
        if (Applied(await ExecuteAsync(Lwt(insert), cancellationToken))) return;
        var existing = await ReadAsync(work.Generation, work.Message.ConversationId, work.ConversationSequence,
            work.Message.Id, cancellationToken) ?? throw new IOException("LWT row disappeared");
        if (existing.Work.ConversationSequence != work.ConversationSequence || existing.Work.FormatVersion != work.FormatVersion ||
            existing.Work.OutboxId != work.OutboxId || existing.Work.EventId != work.EventId)
            throw new InvalidDataException("Projection immutable identity conflict");
        if (existing.Work.MessageVersion > work.MessageVersion) return;
        if (existing.Work.MessageVersion == work.MessageVersion)
        {
            if (!Equivalent(existing.Work, work)) throw new InvalidDataException("Projection same-version content conflict");
            return;
        }
        var update = new SimpleStatement($"UPDATE {table} SET message_version = ?, payload = ? " +
            "WHERE generation = ? AND conversation_id = ? AND bucket = ? AND message_id = ? " +
            "IF message_version = ? AND payload = ?",
            work.MessageVersion, payload, work.Generation, work.Message.ConversationId, bucket, work.Message.Id,
            existing.Work.MessageVersion, JsonSerializer.Serialize(existing.Work.Message, Json));
        if (!Applied(await ExecuteAsync(Lwt(update), cancellationToken)))
            throw new IOException("Projection version CAS lost; retry durable work");
    }

    public async Task<ProjectedMessage?> ReadAsync(Guid generation, string conversationId, long sequence, long messageId,
        CancellationToken cancellationToken)
    {
        var rows = await ExecuteAsync(new SimpleStatement($"SELECT * FROM {table} WHERE generation = ? " +
            "AND conversation_id = ? AND bucket = ? AND message_id = ?", generation, conversationId, Bucket(sequence), messageId),
            cancellationToken);
        var row = rows.FirstOrDefault();
        return row is null ? null : new(FromRow(row));
    }

    // A bounded partition scan exposes actual stored keys/content, not a local mirror or write acknowledgement.
    public async Task<IReadOnlyList<ProjectionWork>> ReadBucketAsync(Guid generation, string conversationId, long bucket,
        int maximumRows, CancellationToken cancellationToken)
    {
        if (bucket < 0 || maximumRows <= 0 || maximumRows > StorageContractVersions.MessagesPerBucket)
            throw new ArgumentOutOfRangeException(nameof(maximumRows));
        var rows = await ExecuteAsync(new SimpleStatement($"SELECT * FROM {table} WHERE generation = ? " +
            "AND conversation_id = ? AND bucket = ? LIMIT ?", generation, conversationId, bucket, maximumRows + 1), cancellationToken);
        var result = rows.Select(FromRow).ToArray();
        if (result.Length > maximumRows) throw new InvalidDataException("Projection partition exceeds verification budget");
        return result;
    }

    // Offline, explicitly bounded full-table audit: unrelated partitions consume the same budget.
    // LIMIT bounds returned rows, not Scylla's scan cost; this is not an online history query.
    internal async Task<IReadOnlyDictionary<long, (ProjectionWork Work, string Payload)>> AuditAsync(
        Guid generation, string conversationId, int maximumRows, CancellationToken cancellationToken)
    {
        if (maximumRows <= 0 || maximumRows == int.MaxValue)
            throw new ArgumentOutOfRangeException(nameof(maximumRows));
        var rows = await ExecuteAsync(new SimpleStatement($"SELECT * FROM {table} LIMIT ?", maximumRows + 1), cancellationToken);
        var result = new Dictionary<long, (ProjectionWork Work, string Payload)>();
        var count = 0;
        foreach (var row in rows)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (++count > maximumRows) throw new InvalidDataException("Projection table exceeds audit budget");
            if (row.GetValue<Guid>("generation") != generation || row.GetValue<string>("conversation_id") != conversationId)
                continue;
            var work = FromRow(row);
            Validate(work);
            if (work.Message.ConversationId != conversationId || row.GetValue<long>("message_id") != work.Message.Id ||
                row.GetValue<long>("bucket") != Bucket(work.ConversationSequence) ||
                !result.TryAdd(work.Message.Id, (work, row.GetValue<string>("payload"))))
                throw new InvalidDataException("Projection physical key/payload mismatch or duplicate message");
        }
        return result;
    }

    public static bool Equivalent(ProjectionWork left, ProjectionWork right) =>
        left.OutboxId == right.OutboxId && left.EventId == right.EventId && left.Generation == right.Generation &&
        left.ConversationSequence == right.ConversationSequence && left.MessageVersion == right.MessageVersion &&
        left.FormatVersion == right.FormatVersion &&
        JsonSerializer.Serialize(left.Message, Json) == JsonSerializer.Serialize(right.Message, Json);

    public static ProjectionWork FromDelivery(DeliveryEvent delivery, Guid generation)
    {
        DeliveryEventCodec.Validate(delivery);
        if (delivery.Payload is not Message message) throw new InvalidDataException("Message source required");
        return new(delivery.OutboxId, delivery.EventId, message, delivery.ConversationSequence!.Value,
            delivery.MessageVersion!.Value, delivery.ProjectionFormatVersion!.Value, generation);
    }

    private static ProjectionWork FromRow(Row row) => new(row.GetValue<Guid>("outbox_id"), row.GetValue<long>("event_id"),
        JsonSerializer.Deserialize<Message>(row.GetValue<string>("payload"), Json) ?? throw new InvalidDataException("Missing projection payload"),
        row.GetValue<long>("sequence"), row.GetValue<long>("message_version"), row.GetValue<int>("format_version"), row.GetValue<Guid>("generation"));
    private static long Bucket(long sequence) => sequence > 0 ? (sequence - 1) / StorageContractVersions.MessagesPerBucket :
        throw new ArgumentOutOfRangeException(nameof(sequence));
    private static void Validate(ProjectionWork work)
    {
        if (work.Generation == Guid.Empty || work.OutboxId == Guid.Empty || work.EventId <= 0 || work.Message.Id <= 0 ||
            string.IsNullOrEmpty(work.Message.ConversationId) || work.ConversationSequence <= 0 || work.MessageVersion <= 0 ||
            work.FormatVersion != StorageContractVersions.HistoryProjection)
            throw new InvalidDataException("Invalid projection work");
    }
    private static SimpleStatement Lwt(SimpleStatement statement)
    {
        statement.SetSerialConsistencyLevel(ConsistencyLevel.LocalSerial);
        return statement;
    }
    private static bool Applied(RowSet rows) => rows.First().GetValue<bool>("[applied]");
    private async Task<RowSet> ExecuteAsync(SimpleStatement statement, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        statement.SetConsistencyLevel(ConsistencyLevel.LocalQuorum);
        statement.SetRetryPolicy(FallthroughRetryPolicy.Instance);
        // The driver has no CancellationToken overload. Never complete PG work after cancellation,
        // even though an in-flight Cassandra request may still finish at the server.
        var rows = await session.ExecuteAsync(statement).WaitAsync(cancellationToken);
        cancellationToken.ThrowIfCancellationRequested();
        return rows;
    }
}

public enum ShadowProjectionResult { Deferred, Match, Mismatch }

public sealed class ScyllaSourceValidator(ScyllaMessageProjection projection, IProjectionSource source,
    int maximumMessages = 100_000) : IProjectionReadbackValidator
{
    // Offline shadow comparison: lag or a different generation is not a semantic mismatch.
    // The caller must hold the authority's conversation lock while sampling and comparing.
    public async Task<ShadowProjectionResult> CompareShadowAsync(ProjectionRebuild active, ProjectionCheckpoint checkpoint,
        long committedSequence, CancellationToken cancellationToken)
    {
        if (checkpoint.ConversationId != active.ConversationId || checkpoint.Generation != active.Generation ||
            checkpoint.FormatVersion != active.FormatVersion || checkpoint.ThroughSequence != committedSequence)
            return ShadowProjectionResult.Deferred;
        return await ValidateAsync(active, committedSequence, cancellationToken)
            ? ShadowProjectionResult.Match : ShadowProjectionResult.Mismatch;
    }

    public async Task<bool> ValidateAsync(ProjectionRebuild rebuild, long throughSequence, CancellationToken cancellationToken)
    {
        if (maximumMessages <= 0 || maximumMessages == int.MaxValue || rebuild.Generation == Guid.Empty ||
            string.IsNullOrEmpty(rebuild.ConversationId) || rebuild.SnapshotSequence < 0 ||
            throughSequence < rebuild.SnapshotSequence || throughSequence > maximumMessages || throughSequence < 0 ||
            rebuild.FormatVersion != StorageContractVersions.HistoryProjection) return false;
        try
        {
            var before = await projection.AuditAsync(rebuild.Generation, rebuild.ConversationId, maximumMessages, cancellationToken);
            long after = 0;
            var expected = new Dictionary<long, ProjectionWork>();
            while (after < throughSequence)
            {
                var limit = (int)Math.Min(1000, throughSequence - after);
                var page = await source.ReadAsync(rebuild.ConversationId, after, throughSequence, limit, cancellationToken);
                if (page.Count == 0 || page.Count > limit) return false;
                foreach (var delivery in page)
                {
                    var work = ScyllaMessageProjection.FromDelivery(delivery, rebuild.Generation);
                    if (work.ConversationSequence != after + 1 || work.ConversationSequence > throughSequence ||
                        work.Message.ConversationId != rebuild.ConversationId || work.FormatVersion != rebuild.FormatVersion ||
                        !expected.TryAdd(work.Message.Id, work)) return false;
                    after = work.ConversationSequence;
                }
            }
            if (before.Count != expected.Count || before.Any(row => !expected.TryGetValue(row.Key, out var work) ||
                !ScyllaMessageProjection.Equivalent(row.Value.Work, work))) return false;
            // Detect observed target changes across source verification, not a distributed snapshot:
            // arbitrary external writes (including ABA or writes after this read) are not fenced by the PG lock.
            var final = await projection.AuditAsync(rebuild.Generation, rebuild.ConversationId, maximumMessages, cancellationToken);
            return final.Count == before.Count && final.All(row => before.TryGetValue(row.Key, out var prior) &&
                row.Value.Payload == prior.Payload && ScyllaMessageProjection.Equivalent(row.Value.Work, prior.Work));
        }
        catch (InvalidDataException) { return false; }
        catch (JsonException) { return false; }
    }
}
