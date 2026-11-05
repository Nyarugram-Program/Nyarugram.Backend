using System.Data;
using System.Text.Json;
using Botgram.Backend.Storage;
using Npgsql;
using P = Botgram.Protocol.V1;

namespace Botgram.Backend;

// Explicit diagnostic fixture, not a production storage implementation or delivery host.
public sealed class PostgresRelationalValidation(NpgsqlDataSource source)
{
    public async Task InitializeAsync(CancellationToken cancellationToken = default)
    {
        await new PostgresMessageAcceptance(source).InitializeAsync(cancellationToken);
        await using var connection = await source.OpenConnectionAsync(cancellationToken);
        await using var command = new NpgsqlCommand("""
            CREATE SCHEMA IF NOT EXISTS botgram_relational_validation_v1;
            CREATE TABLE IF NOT EXISTS botgram_relational_validation_v1.users (
                id text PRIMARY KEY, username text NOT NULL, display_name text NOT NULL, gid text NOT NULL, ordinal integer NOT NULL);
            ALTER TABLE botgram_relational_validation_v1.users ADD COLUMN IF NOT EXISTS premium boolean NOT NULL DEFAULT false;
            CREATE TABLE IF NOT EXISTS botgram_relational_validation_v1.conversations (
                id text PRIMARY KEY, kind text NOT NULL, title text NOT NULL, created_at timestamptz NOT NULL, ordinal integer NOT NULL);
            CREATE TABLE IF NOT EXISTS botgram_relational_validation_v1.members (
                conversation_id text NOT NULL, user_id text NOT NULL, ordinal integer NOT NULL, PRIMARY KEY(conversation_id,user_id));
            CREATE TABLE IF NOT EXISTS botgram_relational_validation_v1.identities (key text PRIMARY KEY, value bigint NOT NULL);
            CREATE TABLE IF NOT EXISTS botgram_relational_validation_v1.contacts (
                owner_id text NOT NULL, user_id text NOT NULL, first_name text NOT NULL, last_name text NOT NULL, ordinal integer NOT NULL);
            CREATE TABLE IF NOT EXISTS botgram_relational_validation_v1.receipts (
                conversation_id text NOT NULL, user_id text NOT NULL, message_id bigint NOT NULL, ordinal integer NOT NULL);
            CREATE TABLE IF NOT EXISTS botgram_relational_validation_v1.seed (
                singleton boolean PRIMARY KEY CHECK(singleton), next_message_id bigint NOT NULL, next_event_id bigint NOT NULL,
                next_telegram_id bigint NOT NULL, source_events bigint NOT NULL, imported_message_events bigint NOT NULL,
                first_event_id bigint NOT NULL, last_event_id bigint NOT NULL);
            CREATE INDEX IF NOT EXISTS relational_history ON botgram_intake_v1.messages(conversation_id,id DESC);
            """, connection);
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    public async Task SeedAsync(Database database, CancellationToken cancellationToken = default)
    {
        await using var connection = await source.OpenConnectionAsync(cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);
        async Task Execute(string sql, params object[] values)
        {
            await using var command = Command(connection, transaction, sql, values);
            await command.ExecuteNonQueryAsync(cancellationToken);
        }
        await Execute("SET LOCAL synchronous_commit=on");
        // Keep capacity before sync, including offline imports. Neither acquisition may wait
        // for an online writer that already holds session/conversation/table locks.
        await using (var admission = Command(connection, transaction, "SELECT pg_try_advisory_xact_lock(7459241304231)"))
            if (!(bool)(await admission.ExecuteScalarAsync(cancellationToken))!)
                throw new InvalidOperationException("Relational fixture seeding requires an idle target");
        await Execute("""
            LOCK TABLE botgram_intake_v1.users, botgram_intake_v1.sessions, botgram_intake_v1.conversations,
              botgram_intake_v1.messages, botgram_intake_v1.events, botgram_intake_v1.sync,
              botgram_relational_validation_v1.seed IN ACCESS EXCLUSIVE MODE NOWAIT
            """);
        await using (var check = Command(connection, transaction, """
            SELECT EXISTS(SELECT 1 FROM botgram_intake_v1.users) OR EXISTS(SELECT 1 FROM botgram_intake_v1.conversations)
                OR EXISTS(SELECT 1 FROM botgram_intake_v1.messages) OR EXISTS(SELECT 1 FROM botgram_intake_v1.events)
                OR EXISTS(SELECT 1 FROM botgram_relational_validation_v1.seed)
                OR EXISTS(SELECT 1 FROM botgram_relational_validation_v1.users)
                OR EXISTS(SELECT 1 FROM botgram_relational_validation_v1.conversations)
                OR EXISTS(SELECT 1 FROM botgram_relational_validation_v1.members)
                OR EXISTS(SELECT 1 FROM botgram_relational_validation_v1.identities)
                OR EXISTS(SELECT 1 FROM botgram_relational_validation_v1.contacts)
                OR EXISTS(SELECT 1 FROM botgram_relational_validation_v1.receipts)
                OR EXISTS(SELECT 1 FROM botgram_intake_v1.members)
                OR EXISTS(SELECT 1 FROM botgram_intake_v1.sync)
                OR EXISTS(SELECT 1 FROM botgram_intake_v1.sessions)
                OR EXISTS(SELECT 1 FROM botgram_intake_v1.event_recipients)
                OR EXISTS(SELECT 1 FROM botgram_intake_v1.outbox)
                OR EXISTS(SELECT 1 FROM botgram_intake_v1.event_cursor WHERE value<>0)
            """))
            if ((bool)(await check.ExecuteScalarAsync(cancellationToken))!) throw new InvalidOperationException("Relational fixture requires an empty target; seeding is one-time only");
        for (var i = 0; i < database.Users.Count; i++)
        {
            var u = database.Users[i];
            await Execute("INSERT INTO botgram_intake_v1.users VALUES($1)", u.Id);
            await Execute("INSERT INTO botgram_relational_validation_v1.users(id,username,display_name,gid,ordinal,premium) VALUES($1,$2,$3,$4,$5,$6)",
                u.Id, u.Username, u.DisplayName, u.Gid, i, u.Premium);
            await Execute("INSERT INTO botgram_intake_v1.sync VALUES($1,$2)", u.Id, SyncService.Pts(database, u.Id));
        }
        foreach (var s in database.Sessions)
            await Execute("INSERT INTO botgram_intake_v1.sessions VALUES($1,$2,$3)", s.TokenHash, s.UserId, s.ExpiresAt);
        for (var i = 0; i < database.Conversations.Count; i++)
        {
            var c = database.Conversations[i];
            await Execute("INSERT INTO botgram_intake_v1.conversations VALUES($1,$2)", c.Id, (long)database.Messages.Count(m => m.ConversationId == c.Id));
            await Execute("INSERT INTO botgram_relational_validation_v1.conversations VALUES($1,$2,$3,$4,$5)", c.Id, c.Kind, c.Title, c.CreatedAt, i);
            for (var n = 0; n < c.Members.Length; n++)
            {
                await Execute("INSERT INTO botgram_intake_v1.members VALUES($1,$2)", c.Id, c.Members[n]);
                await Execute("INSERT INTO botgram_relational_validation_v1.members VALUES($1,$2,$3)", c.Id, c.Members[n], n);
            }
        }
        foreach (var pair in database.TelegramIds)
            await Execute("INSERT INTO botgram_relational_validation_v1.identities VALUES($1,$2)", pair.Key, pair.Value);
        for (var i = 0; i < database.Contacts.Count; i++)
        {
            var c = database.Contacts[i];
            await Execute("INSERT INTO botgram_relational_validation_v1.contacts VALUES($1,$2,$3,$4,$5)", c.OwnerId, c.UserId, c.FirstName, c.LastName, i);
        }
        for (var i = 0; i < database.Receipts.Count; i++)
        {
            var r = database.Receipts[i];
            await Execute("INSERT INTO botgram_relational_validation_v1.receipts VALUES($1,$2,$3,$4)", r.ConversationId, r.UserId, r.MessageId, i);
        }
        foreach (var group in database.Messages.GroupBy(m => m.ConversationId))
        {
            long sequence = 0;
            foreach (var m in group.OrderBy(m => m.Id))
                await Execute("""
                    INSERT INTO botgram_intake_v1.messages(id,conversation_id,conversation_sequence,sender_id,client_key,body)
                    OVERRIDING SYSTEM VALUE VALUES($1,$2,$3,$4,$5,$6::jsonb)
                    """, m.Id, m.ConversationId, ++sequence, m.SenderId, m.ClientMessageId, JsonSerializer.Serialize(m));
        }
        var recipientPts = database.Users.ToDictionary(u => u.Id, _ => 1);
        long imported = 0;
        foreach (var e in database.Events)
        {
            var counted = e.Type is "message.created" or "receipt.updated" or "user.gid_changed" or "user.presence_changed";
            if (counted) foreach (var uid in e.Recipients.Distinct()) recipientPts[uid]++;
            if (e.Type != "message.created" || e.Payload is not Message message ||
                !database.Messages.Any(m => m.Id == message.Id && JsonSerializer.Serialize(m) == JsonSerializer.Serialize(message))) continue;
            await Execute("INSERT INTO botgram_intake_v1.events VALUES($1,$2,'message.created',$3,1)", e.Id, message.Id, e.CreatedAt);
            foreach (var uid in e.Recipients.Distinct())
                await Execute("INSERT INTO botgram_intake_v1.event_recipients VALUES($1,$2,$3)", e.Id, uid, recipientPts[uid]);
            imported++;
        }
        if (database.NextMessageId <= database.Messages.Select(m => m.Id).DefaultIfEmpty(0).Max() ||
            database.NextEventId <= database.Events.Select(e => e.Id).DefaultIfEmpty(0).Max())
            throw new InvalidDataException("Invalid fixture identity cursors");
        await Execute("SELECT setval(pg_get_serial_sequence('botgram_intake_v1.messages','id'),$1,false)", database.NextMessageId);
        await Execute("UPDATE botgram_intake_v1.event_cursor SET value=$1 WHERE singleton", database.NextEventId - 1);
        await Execute("INSERT INTO botgram_relational_validation_v1.seed VALUES(true,$1,$2,$3,$4,$5,$6,$7)",
            database.NextMessageId, database.NextEventId, database.NextTelegramId, (long)database.Events.Count, imported,
            database.Events.Select(e => e.Id).DefaultIfEmpty(0).Min(), database.Events.Select(e => e.Id).DefaultIfEmpty(0).Max());
        await transaction.CommitAsync(cancellationToken);
    }

    public async Task<Session> RequireAsync(HttpContext context)
    {
        var header = context.Request.Headers.Authorization.ToString();
        if (!header.StartsWith("Bearer ", StringComparison.OrdinalIgnoreCase)) throw new ApiException(401, "unauthorized");
        var acquire = RpcDiagnosticStage.Start("auth_pool_acquire_ms");
        await using var connection = await source.OpenConnectionAsync(context.RequestAborted);
        acquire.Dispose();
        using var auth = RpcDiagnosticStage.Start("auth_query_ms");
        return await SessionAsync(connection, null, Auth.HashToken(header[7..]), context.RequestAborted);
    }

    private static async Task<Session> SessionAsync(NpgsqlConnection connection, NpgsqlTransaction? transaction, string hash, CancellationToken token)
    {
        await using var command = Command(connection, transaction,
            "SELECT user_id,expires_at FROM botgram_intake_v1.sessions WHERE token_hash=$1 AND expires_at>clock_timestamp()", hash);
        await using var reader = await command.ExecuteReaderAsync(token);
        if (!await reader.ReadAsync(token)) throw new ApiException(401, "session_expired");
        return new(hash, reader.GetString(0), reader.GetFieldValue<DateTimeOffset>(1));
    }

    public async Task<byte[]> DispatchAsync(Session session, P.RpcRequest request, string domain,
        Func<P.RpcResponse, byte[]> encode, CancellationToken token)
    {
        if (request.OperationCase is not (P.RpcRequest.OperationOneofCase.GetState or P.RpcRequest.OperationOneofCase.GetHistory or P.RpcRequest.OperationOneofCase.SendMessage))
            throw new ApiException(501, "BOTGRAM_RPC_UNSUPPORTED:" + request.OperationCase);
        var acquire = RpcDiagnosticStage.Start("view_pool_acquire_ms");
        await using var connection = await source.OpenConnectionAsync(token);
        acquire.Dispose();
        RpcDiagnosticStage.Tag("view_backend_pid", connection.ProcessID);
        var begin = RpcDiagnosticStage.Start("view_begin_api_ms");
        await using var transaction = await connection.BeginTransactionAsync(IsolationLevel.RepeatableRead, token);
        begin.Dispose();
        var auth = RpcDiagnosticStage.Start("view_auth_ms");
        var authenticated = await SessionAsync(connection, transaction, session.TokenHash, token);
        auth.Dispose();
        if (authenticated.UserId != session.UserId) throw new ApiException(401, "session_expired");
        if (request.OperationCase == P.RpcRequest.OperationOneofCase.GetState)
        {
            await using var command = Command(connection, transaction, "SELECT pts FROM botgram_intake_v1.sync WHERE user_id=$1", session.UserId);
            var pts = (int?)await command.ExecuteScalarAsync(token) ?? throw new InvalidDataException("Missing fixture sync state");
            var bytes = encode(new() { ProtocolMajor = 1, RequestId = request.RequestId,
                GetState = new() { Pts = pts, DateMs = DateTimeOffset.UtcNow.ToUnixTimeSeconds() * 1000 } });
            await transaction.CommitAsync(token);
            return bytes;
        }
        var load = RpcDiagnosticStage.Start("view_metadata_ms");
        var view = await LoadView(connection, transaction, session.UserId, request, token);
        load.Dispose();
        var peer = request.GetHistory?.Peer ?? request.SendMessage?.Peer;
        var conversation = ProtobufRpcMapper.ResolveRelationalPeer(view, session.UserId, peer, domain);
        if (request.OperationCase == P.RpcRequest.OperationOneofCase.GetHistory)
        {
            var q = ProtobufRpcMapper.RelationalHistoryQuery(request.GetHistory!);
            // The anchor is computed before min/max filtering, including non-monotonic dates.
            await using var query = Command(connection, transaction, PostgresBackend.HistorySql,
                conversation.Id, q.OffsetId, q.OffsetDate, q.AddOffset, q.MinId, q.MaxId, q.Limit);
            await using (var reader = await query.ExecuteReaderAsync(token))
                while (await reader.ReadAsync(token)) view.Messages.Add(JsonSerializer.Deserialize<Message>(reader.GetString(0))!);
            var bytes = encode(ProtobufRpcMapper.MapRelationalHistory(view, session.UserId, request, domain, conversation));
            await transaction.CommitAsync(token);
            return bytes;
        }
        var send = ProtobufRpcMapper.RelationalSendCommand(request.SendMessage!);
        using (RpcDiagnosticStage.Start("view_commit_ms")) await transaction.CommitAsync(token);
        // All fixture metadata is immutable; the acceptance transaction rechecks session and membership.
        return await new PostgresMessageAcceptance(source).AcceptAsync(
            new(session.TokenHash, session.UserId, conversation.Id, send, MessageWriteContract.Telegram),
            accepted => encode(ProtobufRpcMapper.MapRelationalSend(view, session.UserId, request, domain, accepted.Message, accepted.Pts)), token);
    }

    private static NpgsqlCommand Command(NpgsqlConnection connection, NpgsqlTransaction? transaction, string sql, params object[] values)
    {
        var command = new NpgsqlCommand(sql, connection, transaction);
        foreach (var value in values) command.Parameters.Add(new NpgsqlParameter { Value = value });
        return command;
    }

    private static async Task<Database> LoadView(NpgsqlConnection connection, NpgsqlTransaction transaction, string uid, P.RpcRequest request, CancellationToken token)
    {
        var view = new Database();
        int sqlNumber = 0;
        async Task Rows(string sql, Action<NpgsqlDataReader> read, params object[] values)
        {
            using var timing = RpcDiagnosticStage.Start($"view_sql_{++sqlNumber:00}_ms");
            await using var command = Command(connection, transaction, sql, values);
            await using var reader = await command.ExecuteReaderAsync(token);
            while (await reader.ReadAsync(token)) read(reader);
        }
        await Rows("""
            SELECT c.id,c.kind,c.title,c.created_at,array_agg(m.user_id ORDER BY m.ordinal)
            FROM botgram_relational_validation_v1.conversations c
            JOIN botgram_relational_validation_v1.members m ON m.conversation_id=c.id
            WHERE EXISTS(SELECT 1 FROM botgram_relational_validation_v1.members own WHERE own.conversation_id=c.id AND own.user_id=$1)
            GROUP BY c.id ORDER BY c.ordinal
            """, r => view.Conversations.Add(new(r.GetString(0), r.GetString(1), r.GetString(2), r.GetFieldValue<string[]>(4), r.GetFieldValue<DateTimeOffset>(3))), uid);
        var peer = request.GetHistory?.Peer ?? request.SendMessage?.Peer;
        var ids = view.Conversations.SelectMany(c => c.Members).Append(uid).Append(peer?.User?.UserId ?? "").Distinct().ToArray();
        await Rows("SELECT id,username,display_name,gid,premium FROM botgram_relational_validation_v1.users WHERE id=ANY($1) ORDER BY ordinal",
            r => view.Users.Add(new(r.GetString(0), r.GetString(1), r.GetString(2), "", "", r.GetString(3), r.GetBoolean(4))), (object)ids);
        var keys = ids.Select(id => "user:" + id).Concat(view.Conversations.Where(c => c.Kind == "group").Select(c => "chat:" + c.Id)).ToArray();
        await Rows("SELECT key,value FROM botgram_relational_validation_v1.identities WHERE key=ANY($1)",
            r => view.TelegramIds.Add(r.GetString(0), r.GetInt64(1)), (object)keys);
        foreach (var key in keys.Where(k => !k.EndsWith(':')))
            if (!view.TelegramIds.ContainsKey(key) && (key.StartsWith("chat:") || view.Users.Any(u => key == "user:" + u.Id)))
                throw new InvalidDataException("Missing fixture TL identity");
        await Rows("SELECT owner_id,user_id,first_name,last_name FROM botgram_relational_validation_v1.contacts WHERE (owner_id=$1 AND user_id=ANY($2)) OR (user_id=$1 AND owner_id=ANY($2)) ORDER BY ordinal",
            r => view.Contacts.Add(new(r.GetString(0), r.GetString(1), r.GetString(2), r.GetString(3))), uid, ids);
        await Rows("SELECT conversation_id,user_id,message_id FROM botgram_relational_validation_v1.receipts WHERE conversation_id=ANY($1) ORDER BY ordinal",
            r => view.Receipts.Add(new(r.GetString(0), r.GetString(1), r.GetInt64(2))), (object)view.Conversations.Select(c => c.Id).ToArray());
        return view;
    }
}
