using System.Data;
using System.Text.Json;
using Npgsql;

namespace Botgram.Backend.Storage;

public sealed class PostgresMessageAcceptance(NpgsqlDataSource dataSource, int maximumBacklog = 100_000) : IMessageAcceptancePersistence
{
    internal const string CapacityLockSql = "SELECT pg_advisory_xact_lock(7459241304231)";
    internal const string CapacityCountSql = """
        SELECT count(*) FROM botgram_intake_v1.outbox o
        WHERE o.state <> 'Published'
            OR (o.projection_version IS NULL AND NOT o.consumed)
            OR (o.projection_version IS NOT NULL AND NOT EXISTS (
                SELECT 1 FROM botgram_intake_v1.projection_tasks t
                JOIN botgram_intake_v1.projection_heads h ON h.conversation_id=t.conversation_id AND h.generation=t.generation
                WHERE t.outbox_id=o.id AND t.state='Complete'))
        """;

    private static bool EnvelopeEqual(byte[]? left, byte[]? right)
    {
        if (left is null || right is null) return left is null && right is null;
        return left.AsSpan().SequenceEqual(right);
    }

    // Explicit opt-in only: constructing this adapter never changes the database.
    public async Task InitializeAsync(CancellationToken cancellationToken = default)
    {
        await using var connection = await dataSource.OpenConnectionAsync(cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);
        await using var command = new NpgsqlCommand("""
            CREATE SCHEMA IF NOT EXISTS botgram_intake_v1;
            CREATE TABLE IF NOT EXISTS botgram_intake_v1.users (
                id text PRIMARY KEY);
            CREATE TABLE IF NOT EXISTS botgram_intake_v1.sessions (
                token_hash text PRIMARY KEY, user_id text NOT NULL REFERENCES botgram_intake_v1.users,
                expires_at timestamptz NOT NULL);
            CREATE TABLE IF NOT EXISTS botgram_intake_v1.conversations (
                id text PRIMARY KEY, sequence bigint NOT NULL DEFAULT 0 CHECK (sequence >= 0));
            CREATE TABLE IF NOT EXISTS botgram_intake_v1.members (
                conversation_id text NOT NULL REFERENCES botgram_intake_v1.conversations,
                user_id text NOT NULL REFERENCES botgram_intake_v1.users,
                PRIMARY KEY (conversation_id, user_id));
            CREATE TABLE IF NOT EXISTS botgram_intake_v1.messages (
                id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                conversation_id text NOT NULL REFERENCES botgram_intake_v1.conversations,
                conversation_sequence bigint NOT NULL CHECK (conversation_sequence > 0),
                sender_id text NOT NULL REFERENCES botgram_intake_v1.users,
                client_key text NOT NULL, body jsonb NOT NULL,
                UNIQUE (conversation_id, conversation_sequence),
                UNIQUE (conversation_id, sender_id, client_key));
            CREATE TABLE IF NOT EXISTS botgram_intake_v1.sync (
                user_id text PRIMARY KEY REFERENCES botgram_intake_v1.users,
                pts integer NOT NULL DEFAULT 1 CHECK (pts >= 1));
            CREATE TABLE IF NOT EXISTS botgram_intake_v1.event_cursor (
                singleton boolean PRIMARY KEY DEFAULT true CHECK (singleton),
                value bigint NOT NULL DEFAULT 0 CHECK (value >= 0));
            INSERT INTO botgram_intake_v1.event_cursor(singleton) VALUES (true) ON CONFLICT DO NOTHING;
            CREATE TABLE IF NOT EXISTS botgram_intake_v1.events (
                id bigint PRIMARY KEY, message_id bigint NOT NULL UNIQUE REFERENCES botgram_intake_v1.messages,
                type text NOT NULL CHECK (type = 'message.created'),
                created_at timestamptz NOT NULL, schema_version integer NOT NULL CHECK (schema_version = 1));
            CREATE TABLE IF NOT EXISTS botgram_intake_v1.event_recipients (
                event_id bigint NOT NULL REFERENCES botgram_intake_v1.events,
                user_id text NOT NULL REFERENCES botgram_intake_v1.users,
                pts integer NOT NULL CHECK (pts > 1), PRIMARY KEY (event_id, user_id), UNIQUE (user_id, pts));
            CREATE TABLE IF NOT EXISTS botgram_intake_v1.outbox (
                id uuid PRIMARY KEY, event_id bigint NOT NULL UNIQUE REFERENCES botgram_intake_v1.events,
                partition_key text NOT NULL REFERENCES botgram_intake_v1.conversations,
                schema_version integer NOT NULL CHECK (schema_version = 1),
                projection_version bigint NOT NULL CHECK (projection_version = 1));
            """, connection, transaction);
        await command.ExecuteNonQueryAsync(cancellationToken);
        await transaction.CommitAsync(cancellationToken);
        await new PostgresDeliveryPersistence(dataSource).InitializeAsync(cancellationToken);
    }

    public async Task<T> AcceptAsync<T>(MessageAcceptance input, Func<AcceptedMessage, T> prepareResponse,
        CancellationToken cancellationToken)
    {
        var acquire = RpcDiagnosticStage.Start("accept_pool_acquire_ms");
        await using var connection = await dataSource.OpenConnectionAsync(cancellationToken);
        acquire.Dispose();
        RpcDiagnosticStage.Tag("accept_backend_pid", connection.ProcessID);
        var begin = RpcDiagnosticStage.Start("accept_begin_api_ms");
        await using var transaction = await connection.BeginTransactionAsync(IsolationLevel.ReadCommitted, cancellationToken);
        begin.Dispose();
        IDisposable? held = null;
        try
        {
            var response = await AcceptCore(input, connection, transaction, prepareResponse, stage => held = stage, cancellationToken);
            cancellationToken.ThrowIfCancellationRequested();
            // Ambiguous COMMIT propagates; neither the adapter nor its caller retries automatically.
            using (RpcDiagnosticStage.Start("accept_commit_ms")) await transaction.CommitAsync(cancellationToken);
            return response;
        }
        finally { held?.Dispose(); }
    }

    /// <summary>
    /// <paramref name="admitNewSend"/> runs only for a NEW send: authentication,
    /// membership, isolation and the durable idempotency lookup have already passed,
    /// but no E2EE/epoch admission has been applied yet. A committed retry therefore
    /// returns its original message even after the chat state changed, while a new
    /// message still faces the current encryption requirements.
    /// </summary>
    internal Task<AcceptedMessage> AcceptInTransactionAsync(MessageAcceptance input, NpgsqlConnection connection,
        NpgsqlTransaction transaction, Action<IDisposable> retainThroughCommit, CancellationToken token,
        Func<Task<byte[]?>>? admitNewSend = null) =>
        AcceptCore(input, connection, transaction, accepted => accepted, retainThroughCommit, token,
            admitNewSend: admitNewSend);

    internal Task<AcceptedMessage> AddGroupServiceAsync(Session session, Conversation conversation,
        NpgsqlConnection connection, NpgsqlTransaction transaction, Action<IDisposable> retainThroughCommit, CancellationToken token) =>
        AcceptCore(new(session.TokenHash, session.UserId, conversation.Id,
            new("service:" + conversation.Id, ""), MessageWriteContract.Telegram),
            connection, transaction, accepted => accepted, retainThroughCommit, token, conversation);

    /// <summary>
    /// Native centred E2EE service message. It shares the message/pts/event/outbox
    /// chain with ordinary messages and carries no plaintext.
    /// </summary>
    internal Task<AcceptedMessage> AddE2eeServiceInTransactionAsync(Session session, string conversationId,
        string clientKey, string code, NpgsqlConnection connection, NpgsqlTransaction transaction,
        Action<IDisposable> retainThroughCommit, CancellationToken token) =>
        AcceptCore(new(session.TokenHash, session.UserId, conversationId,
            new(clientKey, ""), MessageWriteContract.Telegram),
            connection, transaction, accepted => accepted, retainThroughCommit, token,
            e2eeServiceCode: code);

    private async Task<T> AcceptCore<T>(MessageAcceptance input, NpgsqlConnection connection, NpgsqlTransaction transaction,
        Func<AcceptedMessage, T> prepareResponse, Action<IDisposable> retainThroughCommit, CancellationToken cancellationToken,
        Conversation? service = null, string? e2eeServiceCode = null, Func<Task<byte[]?>>? admitNewSend = null)
    {
        if (transaction.Connection != connection || transaction.IsolationLevel != IsolationLevel.ReadCommitted)
            throw new InvalidOperationException("Message acceptance requires its caller's READ COMMITTED transaction");
        int sqlNumber = 0;
        async Task<object?> Scalar(string sql, params object[] values)
        {
            using var timing = RpcDiagnosticStage.Start($"accept_sql_{++sqlNumber:00}_ms");
            await using var query = new NpgsqlCommand(sql, connection, transaction);
            foreach (var value in values) query.Parameters.Add(new NpgsqlParameter { Value = value });
            return await query.ExecuteScalarAsync(cancellationToken);
        }
        async Task Execute(string sql, params object[] values)
        {
            using var timing = RpcDiagnosticStage.Start($"accept_sql_{++sqlNumber:00}_ms");
            await using var query = new NpgsqlCommand(sql, connection, transaction);
            foreach (var value in values) query.Parameters.Add(new NpgsqlParameter { Value = value });
            await query.ExecuteNonQueryAsync(cancellationToken);
        }

        var init = RpcDiagnosticStage.Start("accept_initialize_ms");
        await Execute("SET LOCAL synchronous_commit = on");
        init.Dispose();
        var permission = RpcDiagnosticStage.Start("accept_permission_idempotency_ms");
        // Lock before checking expiry: revocation/deletion cannot pass a successful acceptance.
        object? sessionUser;
        using (RpcDiagnosticStage.Start("accept_session_lock_ms"))
            sessionUser = await Scalar("""
                SELECT user_id FROM botgram_intake_v1.sessions WHERE token_hash = $1 FOR UPDATE
                """, input.SessionTokenHash);
        if (sessionUser is not string user || user != input.UserId ||
            await Scalar("""
                SELECT 1 FROM botgram_intake_v1.sessions
                WHERE token_hash = $1 AND expires_at > clock_timestamp()
                """, input.SessionTokenHash) is null)
            throw new ApiException(401, "session_expired");

        object? conversationSequence;
        using (RpcDiagnosticStage.Start("accept_conversation_lock_ms"))
            conversationSequence = await Scalar("SELECT sequence FROM botgram_intake_v1.conversations WHERE id = $1 FOR UPDATE",
                input.ConversationId);
        if (conversationSequence is null)
            throw new ApiException(404, "conversation_not_found");
        var recipients = new List<string>();
        await using (var query = new NpgsqlCommand("""
            SELECT user_id FROM botgram_intake_v1.members WHERE conversation_id = $1 FOR SHARE
            """, connection, transaction))
        {
            using var timing = RpcDiagnosticStage.Start($"accept_sql_{++sqlNumber:00}_ms");
            query.Parameters.AddWithValue(input.ConversationId);
            await using var reader = await query.ExecuteReaderAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken)) recipients.Add(reader.GetString(0));
        }
        if (!recipients.Contains(input.UserId, StringComparer.Ordinal))
            throw new ApiException(404, "conversation_not_found");
        recipients.Sort(StringComparer.Ordinal);

        var command = input.Command;
        if (service is not null && (service.Id != input.ConversationId || service.Kind != "group" ||
            !service.Members.Order(StringComparer.Ordinal).SequenceEqual(recipients)))
            throw new InvalidDataException("Group service does not match the authorized conversation");
        // Encrypted messages and service messages legitimately have empty text.
        // A media message may have an empty caption; text messages still require text.
        if (service is null && e2eeServiceCode is null && command.E2eeEnvelope is null && command.Media is null &&
            (string.IsNullOrWhiteSpace(command.Text) || command.Text.Length > 16000))
            throw new ApiException(400, input.Contract == MessageWriteContract.Telegram ? "MESSAGE_EMPTY" : "invalid_text");
        if (e2eeServiceCode is not null && command.E2eeEnvelope is not null)
            throw new ApiException(400, "E2EE_ENVELOPE_INVALID");
        if (input.Contract == MessageWriteContract.Rest && command.Key.Length is < 1 or > 80)
            throw new ApiException(400, "invalid_client_message_id");
        if (command.E2eeEnvelope is not null && command.Entities is { Length: > 0 })
            throw new ApiException(409, "E2EE_ENTITIES_FORBIDDEN");
        if (command.Entities is { } entities)
        {
            MessageEntityService.ValidateCount(entities.Length);
            foreach (var entity in entities) MessageEntityService.Validate(command.Text, entity);
        }
        if (command.ReplyId is { } replyId && await Scalar("""
            SELECT 1 FROM botgram_intake_v1.messages WHERE conversation_id = $1 AND id = $2
            """, input.ConversationId, replyId) is null)
            throw new ApiException(400, "REPLY_MESSAGE_ID_INVALID");

        var stored = await Scalar("""
            SELECT body::text FROM botgram_intake_v1.messages
            WHERE conversation_id = $1 AND sender_id = $2 AND client_key = $3
            """, input.ConversationId, input.UserId, command.Key);
        if (stored is string json)
        {
            var existing = JsonSerializer.Deserialize<Message>(json) ?? throw new InvalidDataException("Missing stored message");
            if (existing.Text != command.Text || !EnvelopeEqual(existing.E2eeEnvelope, command.E2eeEnvelope) ||
                (input.Contract == MessageWriteContract.Telegram &&
                (existing.ReplyToMessageId != command.ReplyId || existing.Silent != command.Silent ||
                 existing.Media?.MediaId != command.Media?.MediaId ||
                 !MessageEntityService.Equal(existing.TelegramEntities, command.Entities))))
                throw new ApiException(409, input.Contract == MessageWriteContract.Telegram ? "RANDOM_ID_DUPLICATE" : "idempotency_conflict");
            // The retry must report the pts this message originally received for the
            // current user, never the current sync cursor. A cursor fallback would make
            // the client jump over intervening updates it has not fetched yet.
            var originalPts = await Scalar("""
                SELECT r.pts FROM botgram_intake_v1.events e
                JOIN botgram_intake_v1.event_recipients r ON r.event_id = e.id
                WHERE e.message_id = $1 AND r.user_id = $2
                """, existing.Id, input.UserId);
            if (originalPts is not int pts)
            {
                Console.Error.WriteLine(
                    $"BOTGRAM_MESSAGE_EVENT_PTS_MISSING conversation={input.ConversationId} message_id={existing.Id} user={input.UserId}");
                throw new ApiException(500, "BOTGRAM_MESSAGE_EVENT_PTS_MISSING");
            }
            return prepareResponse(new(existing, true, pts));
        }

        // Durable idempotency is settled: this is a brand-new message. Only now does
        // the caller's E2EE/direction/epoch admission run, so a committed retry whose
        // response was lost is never rejected merely because the chat state moved on.
        if (admitNewSend is not null)
        {
            var canonical = await admitNewSend();
            if (canonical is not null) command = command with { E2eeEnvelope = canonical, Text = "" };
        }

        if (service is null && e2eeServiceCode is null)
        {
            var privacy = await PostgresPrivacyPolicy.Load(connection, transaction, recipients.ToArray(), input.UserId, cancellationToken);
            if (privacy.AccountPrivacy is not null)
            {
                var kind = (string?)await Scalar("SELECT kind FROM botgram_backend_v1.conversations WHERE id=$1", input.ConversationId);
                AccountPrivacyService.CheckMessage(privacy, new(input.ConversationId, kind ?? "", "", recipients.ToArray(), default), input.UserId);
            }
        }
        permission.Dispose();
        if (maximumBacklog < 1) throw new ArgumentOutOfRangeException(nameof(maximumBacklog));
        var capacity = RpcDiagnosticStage.Start("accept_capacity_ms");
        RpcDiagnosticStage.Tag("accept_capacity_lock_key", "backlog-global-v1");
        using (RpcDiagnosticStage.Start("accept_capacity_lock_wait_ms"))
            await Scalar(CapacityLockSql);
        retainThroughCommit(RpcDiagnosticStage.Start("accept_capacity_lock_held_ms"));
        long backlog;
        // Separate statement: ReadCommitted must take a fresh snapshot AFTER lock acquisition.
        using (RpcDiagnosticStage.Start("accept_capacity_count_ms"))
            backlog = (long)(await Scalar(CapacityCountSql))!;
        if (backlog >= maximumBacklog)
            throw new ApiException(503, "storage_backlog_full");
        capacity.Dispose();

        var write = RpcDiagnosticStage.Start("accept_message_write_ms");
        var sequence = (long)(await Scalar("""
            UPDATE botgram_intake_v1.conversations SET sequence = sequence + 1 WHERE id = $1 RETURNING sequence
            """, input.ConversationId))!;
        var message = new Message(0, input.ConversationId, input.UserId, command.Key,
            command.E2eeEnvelope is null ? command.Text : "",
            DateTimeOffset.UtcNow, service?.Title ?? e2eeServiceCode, service?.Members,
            TelegramEntities: command.Entities, ReplyToMessageId: command.ReplyId, Silent: command.Silent,
            E2eeEnvelope: command.E2eeEnvelope, ServiceKind: e2eeServiceCode is null ? null : "e2ee",
            ForwardFromUserId: command.ForwardFromUserId, ForwardFromAt: command.ForwardFromAt,
            ForwardSourceConversationId: command.ForwardSourceConversationId,
            ForwardSourceMessageId: command.ForwardSourceMessageId,
            ForwardDropAuthor: command.ForwardDropAuthor,
            Media: command.Media, GroupedId: command.GroupedId,
            // Persist the accepted request, not the mutable body: a later caption edit
            // must not turn an idempotent retry into a conflict.
            MediaRequest: command.Media is { } acceptedMedia
                ? new MediaRequestIdentity(command.Text,
                    command.Entities is { Length: > 0 } kept ? kept : null,
                    command.ReplyId, command.Silent, acceptedMedia.MediaId)
                : null);
        var messageId = (long)(await Scalar("""
            INSERT INTO botgram_intake_v1.messages(conversation_id, conversation_sequence, sender_id, client_key, body)
            VALUES ($1, $2, $3, $4, $5::jsonb) RETURNING id
            """, input.ConversationId, sequence, input.UserId, command.Key, JsonSerializer.Serialize(message)))!;
        message = message with { Id = messageId };
        await Execute("UPDATE botgram_intake_v1.messages SET body = $2::jsonb WHERE id = $1",
            messageId, JsonSerializer.Serialize(message));
        write.Dispose();

        var sync = RpcDiagnosticStage.Start("accept_sync_event_outbox_ms");
        var recipientPts = new List<(string UserId, int Pts)>();
        using (RpcDiagnosticStage.Start("accept_pts_ms"))
        await using (var query = new NpgsqlCommand("""
            INSERT INTO botgram_intake_v1.sync(user_id, pts)
            SELECT recipient, 2 FROM unnest($1::text[]) WITH ORDINALITY AS r(recipient, ordinal)
            ORDER BY ordinal
            ON CONFLICT (user_id) DO UPDATE SET pts = botgram_intake_v1.sync.pts + 1
            RETURNING user_id, pts
            """, connection, transaction))
        {
            using var timing = RpcDiagnosticStage.Start($"accept_sql_{++sqlNumber:00}_ms");
            query.Parameters.AddWithValue(recipients.ToArray());
            await using var reader = await query.ExecuteReaderAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken))
                recipientPts.Add((reader.GetString(0), reader.GetInt32(1)));
        }
        // This transactional singleton is acquired last and held through commit, not a PostgreSQL sequence.
        long eventId;
        using (RpcDiagnosticStage.Start("accept_event_cursor_ms"))
            eventId = (long)(await Scalar("""
                UPDATE botgram_intake_v1.event_cursor SET value = value + 1 WHERE singleton RETURNING value
                """))!;
        await Execute("""
            INSERT INTO botgram_intake_v1.events(id, message_id, type, created_at, schema_version)
            VALUES ($1, $2, 'message.created', $3, 1)
            """, eventId, messageId, message.CreatedAt);
        await Execute("""
            INSERT INTO botgram_intake_v1.event_recipients(event_id, user_id, pts)
            SELECT $1, user_id, pts FROM unnest($2::text[], $3::integer[]) AS r(user_id, pts)
            """, eventId, recipientPts.Select(r => r.UserId).ToArray(), recipientPts.Select(r => r.Pts).ToArray());
        await Execute("""
            INSERT INTO botgram_intake_v1.outbox(id, event_id, partition_key, schema_version, projection_version)
            VALUES ($1, $2, $3, 1, 1)
            """, Guid.NewGuid(), eventId, input.ConversationId);
        sync.Dispose();
        var prepare = RpcDiagnosticStage.Start("accept_response_prepare_ms");
        var response = prepareResponse(new(message, false, recipientPts.Single(p => p.UserId == input.UserId).Pts));
        prepare.Dispose();
        cancellationToken.ThrowIfCancellationRequested();
        RpcDiagnosticStage.Tag("accept_sql_count", sqlNumber);
        return response;
    }
}
