using System.Data;
using System.Text.Json;
using Npgsql;

namespace Botgram.Backend.Storage;

// Explicitly initialized, never registered by the production host. No recovery data is deleted.
public sealed class PostgresDeliveryPersistence(NpgsqlDataSource dataSource,
    IProjectionReadbackValidator? verifyProjection = null) :
    IOutboxPersistence, IEventInboxPersistence, IProjectionSource, IProjectionProgressPersistence
{
    public async Task InitializeAsync(CancellationToken cancellationToken = default)
    {
        await using var connection = await dataSource.OpenConnectionAsync(cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);
        await Execute(connection, transaction, """
            SET LOCAL synchronous_commit = on;
            ALTER TABLE botgram_intake_v1.events
                ADD COLUMN IF NOT EXISTS payload jsonb,
                ADD COLUMN IF NOT EXISTS expires_at timestamptz;
            ALTER TABLE botgram_intake_v1.outbox
                ADD COLUMN IF NOT EXISTS consumed boolean NOT NULL DEFAULT false,
                ADD COLUMN IF NOT EXISTS state text NOT NULL DEFAULT 'Pending',
                ADD COLUMN IF NOT EXISTS lease_token uuid,
                ADD COLUMN IF NOT EXISTS lease_until timestamptz,
                ADD COLUMN IF NOT EXISTS attempt integer NOT NULL DEFAULT 0,
                ADD COLUMN IF NOT EXISTS retry_at timestamptz NOT NULL DEFAULT '-infinity',
                ADD COLUMN IF NOT EXISTS broker_topic text,
                ADD COLUMN IF NOT EXISTS broker_partition integer,
                ADD COLUMN IF NOT EXISTS broker_offset bigint;
            CREATE INDEX IF NOT EXISTS outbox_partition_order ON botgram_intake_v1.outbox(partition_key,event_id);
            CREATE TABLE IF NOT EXISTS botgram_intake_v1.event_delivery_generation (
                singleton boolean PRIMARY KEY DEFAULT true CHECK(singleton),
                generation uuid NOT NULL, consumer_name text);
            INSERT INTO botgram_intake_v1.event_delivery_generation(singleton,generation)
                VALUES(true,gen_random_uuid()) ON CONFLICT DO NOTHING;
            CREATE TABLE IF NOT EXISTS botgram_intake_v1.projection_heads (
                conversation_id text PRIMARY KEY REFERENCES botgram_intake_v1.conversations,
                generation uuid NOT NULL, format integer NOT NULL CHECK(format = 1));
            CREATE TABLE IF NOT EXISTS botgram_intake_v1.projection_generations (
                conversation_id text NOT NULL REFERENCES botgram_intake_v1.conversations,
                generation uuid NOT NULL, format integer NOT NULL CHECK(format = 1),
                consumer_name text, through_sequence bigint NOT NULL DEFAULT 0,
                expected_active uuid, snapshot_sequence bigint,
                PRIMARY KEY(conversation_id,generation));
            CREATE TABLE IF NOT EXISTS botgram_intake_v1.delivery_inbox (
                consumer_name text NOT NULL, generation uuid NOT NULL, outbox_id uuid NOT NULL,
                event_id bigint NOT NULL, envelope jsonb NOT NULL,
                PRIMARY KEY(consumer_name,generation,outbox_id), UNIQUE(consumer_name,generation,event_id));
            CREATE TABLE IF NOT EXISTS botgram_intake_v1.delivery_positions (
                consumer_name text NOT NULL, generation uuid NOT NULL, outbox_id uuid NOT NULL,
                topic text NOT NULL, partition integer NOT NULL CHECK(partition >= 0),
                broker_offset bigint NOT NULL CHECK(broker_offset >= 0),
                PRIMARY KEY(consumer_name,generation,topic,partition,broker_offset),
                FOREIGN KEY(consumer_name,generation,outbox_id)
                    REFERENCES botgram_intake_v1.delivery_inbox(consumer_name,generation,outbox_id));
            CREATE TABLE IF NOT EXISTS botgram_intake_v1.projection_tasks (
                consumer_name text NOT NULL, generation uuid NOT NULL, outbox_id uuid NOT NULL,
                event_id bigint NOT NULL, conversation_id text NOT NULL, sequence bigint NOT NULL,
                message jsonb NOT NULL, message_version bigint NOT NULL CHECK(message_version = 1),
                format integer NOT NULL CHECK(format = 1), state text NOT NULL DEFAULT 'Pending',
                lease_token uuid, lease_until timestamptz, attempt integer NOT NULL DEFAULT 0,
                retry_at timestamptz NOT NULL DEFAULT '-infinity',
                PRIMARY KEY(consumer_name,generation,outbox_id), UNIQUE(conversation_id,generation,sequence),
                FOREIGN KEY(consumer_name,generation,outbox_id)
                    REFERENCES botgram_intake_v1.delivery_inbox(consumer_name,generation,outbox_id),
                FOREIGN KEY(conversation_id,generation)
                    REFERENCES botgram_intake_v1.projection_generations(conversation_id,generation));
            """, cancellationToken);
        await transaction.CommitAsync(cancellationToken);
    }

    private const string SourceSql = """
        SELECT o.id,o.schema_version,e.id,o.partition_key,e.type,COALESCE(e.payload,m.body)::text,e.created_at,
            m.conversation_sequence,o.projection_version,
            (SELECT jsonb_agg(jsonb_build_object('UserId',r.user_id,'Pts',r.pts) ORDER BY r.user_id COLLATE "C")::text
             FROM botgram_intake_v1.event_recipients r WHERE r.event_id=e.id),e.expires_at
        FROM botgram_intake_v1.outbox o JOIN botgram_intake_v1.events e ON e.id=o.event_id
        LEFT JOIN botgram_intake_v1.messages m ON m.id=e.message_id
        """;

    private static async Task<List<DeliveryEvent>> Source(NpgsqlConnection connection, NpgsqlTransaction transaction,
        string predicate, CancellationToken ct, params object[] values)
    {
        await using var command = Command(connection, transaction, SourceSql + predicate, values);
        await using var reader = await command.ExecuteReaderAsync(ct);
        var result = new List<DeliveryEvent>();
        while (await reader.ReadAsync(ct))
            result.Add(new(reader.GetGuid(0), reader.GetInt32(1), reader.GetInt64(2), reader.GetString(3),
                reader.GetString(4), (IEventPayload)JsonSerializer.Deserialize(reader.GetString(5), EventPayloadTypes.For(reader.GetString(4)))!,
                reader.IsDBNull(9) ? [] : JsonSerializer.Deserialize<RecipientPosition[]>(reader.GetString(9))!
                    .OrderBy(r => r.UserId, StringComparer.Ordinal).ToArray(),
                reader.GetFieldValue<DateTimeOffset>(6), reader.IsDBNull(10) ? null : reader.GetFieldValue<DateTimeOffset>(10),
                reader.IsDBNull(7) ? null : reader.GetInt64(7), reader.IsDBNull(8) ? null : reader.GetInt64(8),
                reader.GetString(4) == "message.created" ? 1 : null));
        return result;
    }

    public async Task<IReadOnlyList<DeliveryEvent>> ReadAsync(string conversationId, long afterSequence,
        long throughSequence, int limit, CancellationToken cancellationToken)
    {
        Limit(limit);
        if (afterSequence < 0 || throughSequence < afterSequence) throw new ArgumentOutOfRangeException(nameof(afterSequence));
        await using var connection = await dataSource.OpenConnectionAsync(cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(IsolationLevel.RepeatableRead, cancellationToken);
        var result = await Source(connection, transaction,
            " WHERE m.conversation_id=$1 AND m.conversation_sequence>$2 AND m.conversation_sequence<=$3 ORDER BY m.conversation_sequence LIMIT $4",
            cancellationToken, conversationId, afterSequence, throughSequence, limit);
        await transaction.CommitAsync(cancellationToken);
        return result;
    }

    public async Task<IReadOnlyList<OutboxLease>> ClaimAsync(int limit, TimeSpan leaseDuration, CancellationToken cancellationToken)
    {
        Limit(limit); Duration(leaseDuration);
        await using var connection = await dataSource.OpenConnectionAsync(cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);
        await Durable(connection, transaction, cancellationToken);
        var ids = new List<Guid>();
        await using (var query = Command(connection, transaction, """
            SELECT o.id FROM botgram_intake_v1.outbox o
            WHERE ((o.state='Pending' AND o.retry_at<=clock_timestamp()) OR
                (o.state='Leased' AND o.lease_until<=clock_timestamp()))
              AND NOT EXISTS (SELECT 1 FROM botgram_intake_v1.outbox p
                WHERE p.partition_key=o.partition_key AND p.event_id<o.event_id AND p.state<>'Published')
            ORDER BY o.event_id LIMIT $1 FOR UPDATE OF o SKIP LOCKED
            """, limit))
        {
            await using var reader = await query.ExecuteReaderAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken)) ids.Add(reader.GetGuid(0));
        }
        var result = new List<OutboxLease>();
        foreach (var id in ids)
        {
            var token = Guid.NewGuid();
            await using var update = Command(connection, transaction, """
                UPDATE botgram_intake_v1.outbox SET state='Leased',lease_token=$2,
                    lease_until=clock_timestamp()+$3,attempt=attempt+1 WHERE id=$1 RETURNING lease_until,attempt
                """, id, token, leaseDuration);
            DateTimeOffset until; int attempt;
            await using (var reader = await update.ExecuteReaderAsync(cancellationToken))
            { await reader.ReadAsync(cancellationToken); until = reader.GetFieldValue<DateTimeOffset>(0); attempt = reader.GetInt32(1); }
            var delivery = (await Source(connection, transaction, " WHERE o.id=$1", cancellationToken, id)).Single();
            result.Add(new(delivery, token, until, attempt));
        }
        await transaction.CommitAsync(cancellationToken);
        return result;
    }

    public Task<bool> RenewAsync(Guid outboxId, Guid leaseToken, TimeSpan leaseDuration, CancellationToken cancellationToken)
    {
        Duration(leaseDuration);
        return OutboxMutation(outboxId, leaseToken, "lease_until=clock_timestamp()+$3", cancellationToken, leaseDuration);
    }

    public Task<bool> ConfirmPublishedAsync(Guid outboxId, Guid leaseToken, BrokerAcknowledgement acknowledgement,
        CancellationToken cancellationToken)
    {
        Position(acknowledgement.Position);
        return OutboxMutation(outboxId, leaseToken,
            "state='Published',broker_topic=$3,broker_partition=$4,broker_offset=$5,lease_token=NULL,lease_until=NULL",
            cancellationToken, acknowledgement.Position.Topic, acknowledgement.Position.Partition, acknowledgement.Position.Offset);
    }

    public Task<bool> FailAsync(Guid outboxId, Guid leaseToken, DateTimeOffset retryAt, bool blocked, CancellationToken cancellationToken) =>
        OutboxMutation(outboxId, leaseToken, "state=$3,retry_at=$4,lease_token=NULL,lease_until=NULL",
            cancellationToken, blocked ? "Blocked" : "Pending", retryAt);

    private async Task<bool> OutboxMutation(Guid id, Guid token, string set, CancellationToken ct, params object[] extra) =>
        await Mutate("UPDATE botgram_intake_v1.outbox SET " + set +
            " WHERE id=$1 AND lease_token=$2 AND state='Leased' AND lease_until>clock_timestamp()", ct, [id, token, .. extra]);

    public async Task<bool> ReplayAsync(Guid outboxId, CancellationToken cancellationToken)
    {
        await using var connection = await dataSource.OpenConnectionAsync(cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);
        await Durable(connection, transaction, cancellationToken);
        // Published + projected rows may re-enter the backlog. Serialize that increase with admission;
        // recovery is still allowed at capacity and retains the original event/outbox identity.
        await Scalar(connection, transaction, PostgresMessageAcceptance.CapacityLockSql, cancellationToken);
        var changed = await Execute(connection, transaction, """
            UPDATE botgram_intake_v1.outbox SET state='Pending',retry_at='-infinity',lease_token=NULL,lease_until=NULL
            WHERE id=$1 AND state IN ('Published','Blocked')
            """, cancellationToken, outboxId);
        await transaction.CommitAsync(cancellationToken);
        return changed == 1;
    }

    public async Task<InboxStageResult> StageAsync(ConsumerScope consumer, DeliveryEvent delivery, BrokerPosition? position,
        CancellationToken cancellationToken)
    {
        Scope(consumer); Validate(delivery); if (position is not null) Position(position);
        if (consumer.Generation == Guid.Empty) throw new InvalidDataException("An allocated nonempty generation is required");
        await using var connection = await dataSource.OpenConnectionAsync(cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);
        await Durable(connection, transaction, cancellationToken);
        object? owner;
        if (delivery.PartitionKey.StartsWith("user:", StringComparison.Ordinal))
        {
            owner = await Scalar(connection, transaction, """
                UPDATE botgram_intake_v1.event_delivery_generation SET consumer_name=COALESCE(consumer_name,$2)
                WHERE singleton AND generation=$1 RETURNING consumer_name
                """, cancellationToken, consumer.Generation, consumer.Name);
        }
        else
        {
            await LockConversation(connection, transaction, delivery.PartitionKey, cancellationToken);
            owner = await Scalar(connection, transaction, """
            UPDATE botgram_intake_v1.projection_generations g SET consumer_name=COALESCE(g.consumer_name,$3)
            FROM botgram_intake_v1.projection_heads h
            WHERE g.conversation_id=$1 AND g.generation=$2 AND h.conversation_id=g.conversation_id
                AND (g.generation=h.generation OR g.expected_active=h.generation)
            RETURNING g.consumer_name
            """, cancellationToken, delivery.PartitionKey, consumer.Generation, consumer.Name);
        }
        if (!Equals(owner, consumer.Name)) throw new InvalidDataException("Generation is unallocated, stale or belongs to another consumer");
        var original = (await Source(connection, transaction, " WHERE o.id=$1", cancellationToken, delivery.OutboxId)).SingleOrDefault();
        if (original is null || Envelope(original) != Envelope(delivery)) throw new InvalidDataException("Delivery differs from authoritative PG source");
        var envelope = Envelope(delivery);
        var inserted = await Execute(connection, transaction, """
            INSERT INTO botgram_intake_v1.delivery_inbox(consumer_name,generation,outbox_id,event_id,envelope)
            VALUES($1,$2,$3,$4,$5::jsonb) ON CONFLICT DO NOTHING
            """, cancellationToken, consumer.Name, consumer.Generation, delivery.OutboxId, delivery.EventId, envelope);
        var equal = await Scalar(connection, transaction, """
            SELECT 1 FROM botgram_intake_v1.delivery_inbox WHERE consumer_name=$1 AND generation=$2
                AND outbox_id=$3 AND event_id=$4 AND envelope=$5::jsonb
            """, cancellationToken, consumer.Name, consumer.Generation, delivery.OutboxId, delivery.EventId, envelope);
        if (equal is null) throw new InvalidDataException("Inbox identity/content conflict");
        if (position is not null)
        {
            await Execute(connection, transaction, """
                INSERT INTO botgram_intake_v1.delivery_positions VALUES($1,$2,$3,$4,$5,$6) ON CONFLICT DO NOTHING
                """, cancellationToken, consumer.Name, consumer.Generation, delivery.OutboxId, position.Topic, position.Partition, position.Offset);
            if (await Scalar(connection, transaction, """
                SELECT 1 FROM botgram_intake_v1.delivery_positions WHERE consumer_name=$1 AND generation=$2
                    AND topic=$3 AND partition=$4 AND broker_offset=$5 AND outbox_id=$6
                """, cancellationToken, consumer.Name, consumer.Generation, position.Topic, position.Partition, position.Offset, delivery.OutboxId) is null)
                throw new InvalidDataException("Broker position identity conflict");
        }
        if (delivery.Payload is Message)
            await Execute(connection, transaction, """
            INSERT INTO botgram_intake_v1.projection_tasks
                (consumer_name,generation,outbox_id,event_id,conversation_id,sequence,message,message_version,format)
            VALUES($1,$2,$3,$4,$5,$6,$7::jsonb,$8,$9) ON CONFLICT DO NOTHING
            """, cancellationToken, consumer.Name, consumer.Generation, delivery.OutboxId, delivery.EventId,
            delivery.PartitionKey, delivery.ConversationSequence!.Value, JsonSerializer.Serialize((Message)delivery.Payload),
            delivery.MessageVersion!.Value, delivery.ProjectionFormatVersion!.Value);
        else
            await Execute(connection, transaction, "UPDATE botgram_intake_v1.outbox SET consumed=true WHERE id=$1",
                cancellationToken, delivery.OutboxId);
        await transaction.CommitAsync(cancellationToken);
        return inserted == 1 ? InboxStageResult.Staged : InboxStageResult.Duplicate;
    }

    public async Task<IReadOnlyList<ProjectionLease>> ClaimAsync(ConsumerScope consumer, int limit, TimeSpan leaseDuration,
        CancellationToken cancellationToken)
    {
        Scope(consumer); Limit(limit); Duration(leaseDuration);
        await using var connection = await dataSource.OpenConnectionAsync(cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);
        await Durable(connection, transaction, cancellationToken);
        var result = new List<ProjectionLease>();
        await using (var command = Command(connection, transaction, """
            WITH selected AS (SELECT consumer_name,generation,outbox_id FROM botgram_intake_v1.projection_tasks
                WHERE consumer_name=$1 AND generation=$2 AND
                ((state='Pending' AND retry_at<=clock_timestamp()) OR (state='Leased' AND lease_until<=clock_timestamp()))
                ORDER BY event_id LIMIT $3 FOR UPDATE SKIP LOCKED)
            UPDATE botgram_intake_v1.projection_tasks t SET state='Leased', lease_token=gen_random_uuid(),
                lease_until=clock_timestamp()+$4,attempt=t.attempt+1 FROM selected s
            WHERE t.consumer_name=s.consumer_name AND t.generation=s.generation AND t.outbox_id=s.outbox_id
            RETURNING t.outbox_id,t.event_id,t.message::text,t.sequence,t.message_version,t.format,t.lease_token,t.lease_until,t.attempt
            """, consumer.Name, consumer.Generation, limit, leaseDuration))
        {
            await using var reader = await command.ExecuteReaderAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken))
                result.Add(new(consumer, new(reader.GetGuid(0), reader.GetInt64(1),
                    JsonSerializer.Deserialize<Message>(reader.GetString(2))!, reader.GetInt64(3), reader.GetInt64(4), reader.GetInt32(5),
                    consumer.Generation), reader.GetGuid(6), reader.GetFieldValue<DateTimeOffset>(7), reader.GetInt32(8)));
        }
        await transaction.CommitAsync(cancellationToken);
        return result;
    }

    public Task<bool> RenewAsync(ProjectionLease lease, TimeSpan leaseDuration, CancellationToken cancellationToken)
    {
        Duration(leaseDuration);
        return TaskMutation(lease, "lease_until=clock_timestamp()+$5", cancellationToken, leaseDuration);
    }

    public Task<bool> RetryAsync(ProjectionLease lease, DateTimeOffset retryAt, CancellationToken cancellationToken) =>
        TaskMutation(lease, "state='Pending',retry_at=$5,lease_token=NULL,lease_until=NULL", cancellationToken, retryAt);

    private Task<bool> TaskMutation(ProjectionLease lease, string set, CancellationToken ct, params object[] extra)
    {
        Scope(lease.Consumer);
        if (lease.Work.Generation != lease.Consumer.Generation) return Task.FromResult(false);
        return Mutate("UPDATE botgram_intake_v1.projection_tasks SET " + set +
            " WHERE consumer_name=$1 AND generation=$2 AND outbox_id=$3 AND lease_token=$4 AND state='Leased' AND lease_until>clock_timestamp()",
            ct, [lease.Consumer.Name, lease.Consumer.Generation, lease.Work.OutboxId, lease.LeaseToken, .. extra]);
    }

    // The worker invokes completion only AFTER actual external projection durability; PG cannot attest a cross-store write.
    public async Task<bool> CompleteAsync(ProjectionLease lease, CancellationToken cancellationToken)
    {
        Scope(lease.Consumer);
        if (lease.Work.Generation != lease.Consumer.Generation) return false;
        await using var connection = await dataSource.OpenConnectionAsync(cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);
        await Durable(connection, transaction, cancellationToken);
        await LockConversation(connection, transaction, lease.Work.Message.ConversationId, cancellationToken);
        await Scalar(connection, transaction, """
            SELECT through_sequence FROM botgram_intake_v1.projection_generations
            WHERE conversation_id=$1 AND generation=$2 FOR UPDATE
            """, cancellationToken, lease.Work.Message.ConversationId, lease.Work.Generation);
        var original = (await Source(connection, transaction, " WHERE o.id=$1", cancellationToken, lease.Work.OutboxId)).SingleOrDefault();
        if (original is null || original.EventId != lease.Work.EventId || original.ConversationSequence != lease.Work.ConversationSequence ||
            original.MessageVersion != lease.Work.MessageVersion || original.ProjectionFormatVersion != lease.Work.FormatVersion ||
            JsonSerializer.Serialize((Message)original.Payload) != JsonSerializer.Serialize(lease.Work.Message))
            throw new InvalidDataException("Projection completion differs from source");
        var changed = await Execute(connection, transaction, """
            UPDATE botgram_intake_v1.projection_tasks SET state='Complete',lease_token=NULL,lease_until=NULL
            WHERE consumer_name=$1 AND generation=$2 AND outbox_id=$3 AND lease_token=$4
                AND state='Leased' AND lease_until>clock_timestamp() AND event_id=$5 AND conversation_id=$6
                AND sequence=$7 AND message=$8::jsonb AND message_version=$9 AND format=$10
            """, cancellationToken, lease.Consumer.Name, lease.Work.Generation, lease.Work.OutboxId, lease.LeaseToken,
            lease.Work.EventId, lease.Work.Message.ConversationId, lease.Work.ConversationSequence,
            JsonSerializer.Serialize(lease.Work.Message), lease.Work.MessageVersion, lease.Work.FormatVersion);
        if (changed != 1) return false;
        await Execute(connection, transaction, """
            UPDATE botgram_intake_v1.projection_generations g SET through_sequence=COALESCE(
                (SELECT min(m.conversation_sequence)-1 FROM botgram_intake_v1.messages m
                 WHERE m.conversation_id=g.conversation_id AND m.conversation_sequence>g.through_sequence AND NOT EXISTS
                    (SELECT 1 FROM botgram_intake_v1.projection_tasks t WHERE t.conversation_id=g.conversation_id
                     AND t.generation=g.generation AND t.sequence=m.conversation_sequence AND t.state='Complete')),
                (SELECT sequence FROM botgram_intake_v1.conversations WHERE id=g.conversation_id))
            WHERE g.conversation_id=$1 AND g.generation=$2
            """, cancellationToken, lease.Work.Message.ConversationId, lease.Work.Generation);
        await transaction.CommitAsync(cancellationToken);
        return true;
    }

    public async Task<ProjectionCheckpoint> CheckpointAsync(string conversationId, Guid generation, CancellationToken cancellationToken)
    {
        await using var connection = await dataSource.OpenConnectionAsync(cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);
        await using var query = Command(connection, transaction, """
            SELECT format,through_sequence FROM botgram_intake_v1.projection_generations WHERE conversation_id=$1 AND generation=$2
            """, conversationId, generation);
        await using var reader = await query.ExecuteReaderAsync(cancellationToken);
        return await reader.ReadAsync(cancellationToken)
            ? new(conversationId, generation, reader.GetInt32(0), reader.GetInt64(1))
            : new(conversationId, generation, 1, 0);
    }

    public async Task<ProjectionRebuild> BeginRebuildAsync(string conversationId, int formatVersion, CancellationToken cancellationToken)
    {
        if (formatVersion != 1) throw new NotSupportedException("Unsupported projection format");
        await using var connection = await dataSource.OpenConnectionAsync(cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);
        await Durable(connection, transaction, cancellationToken);
        var sequence = await LockConversation(connection, transaction, conversationId, cancellationToken);
        var active = await EnsureHead(connection, transaction, conversationId, cancellationToken);
        var generation = Guid.NewGuid();
        await Execute(connection, transaction, """
            INSERT INTO botgram_intake_v1.projection_generations
                (conversation_id,generation,format,expected_active,snapshot_sequence) VALUES($1,$2,$3,$4,$5)
            """, cancellationToken, conversationId, generation, formatVersion, active, sequence);
        await transaction.CommitAsync(cancellationToken);
        return new(conversationId, generation, active, formatVersion, sequence);
    }

    public async Task<bool> ActivateAsync(ProjectionRebuild rebuild, CancellationToken cancellationToken)
    {
        await using var connection = await dataSource.OpenConnectionAsync(cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);
        await Durable(connection, transaction, cancellationToken);
        var current = await LockConversation(connection, transaction, rebuild.ConversationId, cancellationToken);
        // Compare every staged key and full payload to retained authoritative rows, not a max(sequence).
        var valid = await Scalar(connection, transaction, """
            SELECT 1 FROM botgram_intake_v1.projection_generations g
            JOIN botgram_intake_v1.projection_heads h USING(conversation_id)
            WHERE g.conversation_id=$1 AND g.generation=$2 AND g.expected_active=$3 AND h.generation=$3
                AND g.format=$4 AND g.snapshot_sequence=$5 AND g.through_sequence=$6
                AND (SELECT count(*) FROM botgram_intake_v1.messages m WHERE m.conversation_id=$1)=$6
                AND NOT EXISTS (SELECT 1 FROM botgram_intake_v1.messages m
                    JOIN botgram_intake_v1.events e ON e.message_id=m.id
                    JOIN botgram_intake_v1.outbox o ON o.event_id=e.id
                    WHERE m.conversation_id=$1 AND NOT EXISTS (
                        SELECT 1 FROM botgram_intake_v1.projection_tasks t
                        JOIN botgram_intake_v1.delivery_inbox i USING(consumer_name,generation,outbox_id)
                        WHERE t.conversation_id=$1 AND t.generation=$2 AND t.sequence=m.conversation_sequence
                            AND t.state='Complete' AND t.message=m.body AND t.event_id=e.id AND t.outbox_id=o.id
                            AND t.format=$4 AND t.message_version=o.projection_version))
                AND (SELECT count(*) FROM botgram_intake_v1.projection_tasks t WHERE t.conversation_id=$1 AND t.generation=$2)=$6
            """, cancellationToken, rebuild.ConversationId, rebuild.Generation, rebuild.ExpectedActiveGeneration,
            rebuild.FormatVersion, rebuild.SnapshotSequence, current);
        if (valid is null || verifyProjection is null) return false;
        // The injected verifier must quorum-read the actual target generation and compare the complete
        // key/content set to PG source through current. The conversation lock pins that source boundary.
        if (!await verifyProjection.ValidateAsync(rebuild, current, cancellationToken)) return false;
        await Execute(connection, transaction,
            "UPDATE botgram_intake_v1.projection_heads SET generation=$2,format=$3 WHERE conversation_id=$1",
            cancellationToken, rebuild.ConversationId, rebuild.Generation, rebuild.FormatVersion);
        await transaction.CommitAsync(cancellationToken);
        return true;
    }

    private static async Task<long> LockConversation(NpgsqlConnection connection, NpgsqlTransaction transaction, string id, CancellationToken ct) =>
        (long)(await Scalar(connection, transaction, "SELECT sequence FROM botgram_intake_v1.conversations WHERE id=$1 FOR UPDATE", ct, id)
            ?? throw new InvalidDataException("Unknown conversation"));

    public async Task<Guid> GetActiveGenerationAsync(string conversationId, CancellationToken cancellationToken = default)
    {
        await using var connection = await dataSource.OpenConnectionAsync(cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);
        await Durable(connection, transaction, cancellationToken);
        if (conversationId.StartsWith("user:", StringComparison.Ordinal))
        {
            var global = (Guid)(await Scalar(connection, transaction,
                "SELECT generation FROM botgram_intake_v1.event_delivery_generation WHERE singleton", cancellationToken))!;
            await transaction.CommitAsync(cancellationToken);
            return global;
        }
        await LockConversation(connection, transaction, conversationId, cancellationToken);
        var generation = await EnsureHead(connection, transaction, conversationId, cancellationToken);
        await transaction.CommitAsync(cancellationToken);
        return generation;
    }

    // Callers hold the conversation row lock before allocating or locking generation rows.
    private static async Task<Guid> EnsureHead(NpgsqlConnection connection, NpgsqlTransaction transaction, string id, CancellationToken ct)
    {
        if (await Scalar(connection, transaction,
            "SELECT generation FROM botgram_intake_v1.projection_heads WHERE conversation_id=$1", ct, id) is Guid active)
            return active;
        var generation = Guid.NewGuid();
        await Execute(connection, transaction, """
            INSERT INTO botgram_intake_v1.projection_generations(conversation_id,generation,format)
            VALUES($1,$2,1)
            """, ct, id, generation);
        await Execute(connection, transaction,
            "INSERT INTO botgram_intake_v1.projection_heads VALUES($1,$2,1)", ct, id, generation);
        return generation;
    }

    private static string Envelope(DeliveryEvent delivery) => JsonSerializer.Serialize(new
    {
        delivery.OutboxId, delivery.SchemaVersion, delivery.EventId, delivery.PartitionKey, delivery.Type,
        Payload = JsonSerializer.SerializeToElement(delivery.Payload, delivery.Payload.GetType()), delivery.Recipients, delivery.CreatedAt, delivery.ExpiresAt,
        delivery.ConversationSequence, delivery.MessageVersion, delivery.ProjectionFormatVersion
    });

    private static void Validate(DeliveryEvent delivery)
    {
        if (delivery.SchemaVersion != StorageContractVersions.EventSchema &&
            !(delivery.SchemaVersion == StorageContractVersions.WallpaperEventSchema && delivery.Type == "wallpaper.changed"))
            throw new NotSupportedException("Unsupported delivery schema/type");
        DeliveryEventCodec.Validate(delivery);
        if (delivery.Type != "message.created")
        {
            if (delivery.Recipients.Count == 0) throw new InvalidDataException("Missing delivery recipients");
            return;
        }
        var message = (Message)delivery.Payload;
        if (delivery.OutboxId == Guid.Empty || delivery.EventId <= 0 || message.Id <= 0 ||
            message.ConversationId != delivery.PartitionKey || string.IsNullOrWhiteSpace(message.SenderId) ||
            delivery.ConversationSequence is not > 0 || delivery.MessageVersion != 1 || delivery.ProjectionFormatVersion != 1 ||
            delivery.ExpiresAt is not null || delivery.Recipients.Count == 0 ||
            delivery.Recipients.Any(r => string.IsNullOrWhiteSpace(r.UserId) || r.Pts is not > 1) ||
            !delivery.Recipients.Select(r => r.UserId).SequenceEqual(delivery.Recipients.Select(r => r.UserId).Distinct(StringComparer.Ordinal).Order(StringComparer.Ordinal)))
            throw new InvalidDataException("Invalid v1 message envelope");
    }

    private static void Scope(ConsumerScope consumer)
    { if (string.IsNullOrWhiteSpace(consumer.Name) || consumer.Name.Length > 200) throw new ArgumentException("Invalid consumer name"); }
    private static void Position(BrokerPosition position)
    { if (string.IsNullOrWhiteSpace(position.Topic) || position.Partition < 0 || position.Offset < 0) throw new ArgumentException("Invalid broker position"); }
    private static void Limit(int limit)
    { if (limit is < 1 or > 1000) throw new ArgumentOutOfRangeException(nameof(limit)); }
    private static void Duration(TimeSpan duration)
    { if (duration <= TimeSpan.Zero || duration > TimeSpan.FromHours(1)) throw new ArgumentOutOfRangeException(nameof(duration)); }

    private async Task<bool> Mutate(string sql, CancellationToken ct, params object[] values)
    {
        await using var connection = await dataSource.OpenConnectionAsync(ct);
        await using var transaction = await connection.BeginTransactionAsync(ct);
        await Durable(connection, transaction, ct);
        var changed = await Execute(connection, transaction, sql, ct, values);
        await transaction.CommitAsync(ct);
        return changed == 1;
    }
    private static Task<int> Durable(NpgsqlConnection connection, NpgsqlTransaction transaction, CancellationToken ct) =>
        Execute(connection, transaction, "SET LOCAL synchronous_commit=on", ct);
    private static NpgsqlCommand Command(NpgsqlConnection connection, NpgsqlTransaction transaction, string sql, params object[] values)
    {
        var command = new NpgsqlCommand(sql, connection, transaction);
        foreach (var value in values) command.Parameters.Add(new NpgsqlParameter { Value = value });
        return command;
    }
    private static async Task<int> Execute(NpgsqlConnection connection, NpgsqlTransaction transaction, string sql, CancellationToken ct, params object[] values)
    {
        await using var command = Command(connection, transaction, sql, values);
        return await command.ExecuteNonQueryAsync(ct);
    }
    private static async Task<object?> Scalar(NpgsqlConnection connection, NpgsqlTransaction transaction, string sql, CancellationToken ct, params object[] values)
    {
        await using var command = Command(connection, transaction, sql, values);
        return await command.ExecuteScalarAsync(ct);
    }
}
