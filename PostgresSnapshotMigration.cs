using System.Security.Cryptography;
using System.Text.Json;
using Npgsql;

namespace Botgram.Backend.Storage;

// Offline import/export for the isolated validation database. The input snapshot is never written.
public sealed class PostgresSnapshotMigration(NpgsqlDataSource source)
{
    private static DateTimeOffset Time(DateTimeOffset value) => new(value.UtcTicks - value.UtcTicks % 10, TimeSpan.Zero);
    private static IEventPayload Payload(IEventPayload value) => value switch
    {
        Message m => m with { CreatedAt = Time(m.CreatedAt) },
        Conversation c => c with { CreatedAt = Time(c.CreatedAt) },
        Call c => c with { UpdatedAt = Time(c.UpdatedAt) },
        _ => value
    };

    public static Database Normalize(Database input)
    {
        var db = JsonSerializer.Deserialize<Database>(JsonSerializer.Serialize(input, Store.Json), Store.Json)!;
        db.Sessions = db.Sessions.OrderBy(s => s.TokenHash, StringComparer.Ordinal)
            .Select(s => s with { ExpiresAt = Time(s.ExpiresAt) }).ToList();
        db.Conversations = db.Conversations.Select(c => c with { CreatedAt = Time(c.CreatedAt) }).ToList();
        db.Messages = db.Messages.OrderBy(m => m.Id).Select(m => (Message)Payload(m)).ToList();
        db.Calls = db.Calls.Select(c => (Call)Payload(c)).ToList();
        db.Receipts = db.Receipts.OrderBy(r => r.ConversationId, StringComparer.Ordinal).ThenBy(r => r.UserId, StringComparer.Ordinal).ToList();
        db.Events = db.Events.OrderBy(e => e.Id).Select(e => e with
        {
            CreatedAt = Time(e.CreatedAt), ExpiresAt = e.ExpiresAt is null ? null : Time(e.ExpiresAt.Value),
            Recipients = e.Recipients.Distinct(StringComparer.Ordinal).Order(StringComparer.Ordinal).ToArray(), Payload = Payload(e.Payload)
        }).ToList();
        db.TelegramIds = db.TelegramIds.OrderBy(p => p.Key, StringComparer.Ordinal).ToDictionary(p => p.Key, p => p.Value);
        if (db.AccountPrivacy is not null)
        {
            db.AccountPrivacy = db.AccountPrivacy.Where(p => !AccountPrivacyService.IsDefault(p.Value))
                .OrderBy(p => p.Key, StringComparer.Ordinal).ToDictionary(p => p.Key, p => p.Value);
            foreach (var state in db.AccountPrivacy.Values)
            {
                state.Rules = state.Rules.OrderBy(p => p.Key).ToDictionary(p => p.Key, p => p.Value);
                state.Blocked = state.Blocked.OrderBy(p => p.Key, StringComparer.Ordinal).ToDictionary(p => p.Key, p => Time(p.Value));
            }
            if (db.AccountPrivacy.Count == 0) db.AccountPrivacy = null;
        }
        return db;
    }

    public static string Fingerprint(Database db) => Convert.ToHexString(SHA256.HashData(
        JsonSerializer.SerializeToUtf8Bytes(Normalize(db), Store.Json))).ToLowerInvariant();

    private static void Validate(Database db)
    {
        if (db.Users.Any(u => !Guid.TryParseExact(u.Id, "N", out _)) ||
            db.Conversations.Any(c => !Guid.TryParseExact(c.Id, "N", out _)) ||
            db.NextMessageId <= db.Messages.Select(m => m.Id).DefaultIfEmpty(0).Max() ||
            db.NextEventId <= db.Events.Select(e => e.Id).DefaultIfEmpty(0).Max() ||
            db.NextTelegramId <= db.TelegramIds.Values.DefaultIfEmpty(0).Max() ||
            db.TelegramIds.Values.Any(id => id <= 0) || db.TelegramIds.Values.Distinct().Count() != db.TelegramIds.Count)
            throw new InvalidDataException("Invalid snapshot identities or cursors");
        var messages = db.Messages.ToDictionary(m => m.Id);
        foreach (var pair in db.AccountPrivacy ?? [])
        {
            if (!db.Users.Any(u => u.Id == pair.Key) || pair.Value.AccountTtlDays is < 1 or > 730
                || pair.Value.DefaultHistoryTtlSeconds is < 0 or > 31_536_000
                || pair.Value.Blocked.Keys.Any(id => !db.Users.Any(u => u.Id == id)))
                throw new InvalidDataException("Invalid snapshot privacy owner or retention settings");
            foreach (var rule in pair.Value.Rules)
            {
                var decoded = Botgram.Protocol.V1.PrivacyRuleSet.Parser.ParseFrom(rule.Value);
                if ((int)decoded.Key != rule.Key) throw new InvalidDataException("Invalid snapshot privacy rule key");
                AccountPrivacyService.ValidateRules(db, pair.Key, decoded, null);
            }
        }
        var events = db.Events.Where(e => e.Type == "message.created").ToArray();
        if (events.Length != messages.Count || events.Select(e => ((Message)e.Payload).Id).Distinct().Count() != messages.Count ||
            events.Any(e => e.Payload is not Message m || !messages.TryGetValue(m.Id, out var stored) ||
                JsonSerializer.Serialize(m) != JsonSerializer.Serialize(stored)))
            throw new InvalidDataException("Every snapshot message requires its original matching durable event");
        if (db.Events.Any(e => e.Type == "wallpaper.changed"))
            throw new InvalidDataException("Legacy snapshot cannot represent independent wallpaper state");
    }

    private async Task<NpgsqlTransaction> Begin(NpgsqlConnection connection, CancellationToken token)
    {
        var target = new NpgsqlConnectionStringBuilder(source.ConnectionString);
        if (!FourComponentOptions.Loopback(target.Host) || target.Database?.StartsWith("botgram_test_", StringComparison.Ordinal) != true)
            throw new InvalidOperationException("Snapshot migration requires a loopback botgram_test_* database");
        var transaction = await connection.BeginTransactionAsync(token);
        try
        {
            await using var guard = new NpgsqlCommand("""
                SELECT pg_try_advisory_xact_lock($1) AND pg_try_advisory_xact_lock(7459241304231)
                """, connection, transaction);
            guard.Parameters.AddWithValue(FourComponentRuntime.RuntimeLock);
            if (!(bool)(await guard.ExecuteScalarAsync(token))!) throw new InvalidOperationException("Stop workers before offline migration");
            await using var tables = new NpgsqlCommand("""
                SET LOCAL synchronous_commit=on;
                LOCK TABLE botgram_intake_v1.users,botgram_intake_v1.sessions,botgram_intake_v1.conversations,
                    botgram_intake_v1.messages,botgram_intake_v1.events,botgram_intake_v1.sync,
                    botgram_intake_v1.event_cursor,botgram_backend_v1.accounts,botgram_backend_v1.contacts,
                    botgram_backend_v1.calls,botgram_backend_v1.receipts,botgram_backend_v1.identities
                    IN ACCESS EXCLUSIVE MODE NOWAIT
                """, connection, transaction);
            await tables.ExecuteNonQueryAsync(token);
            return transaction;
        }
        catch { await transaction.DisposeAsync(); throw; }
    }

    public async Task<bool> ImportAsync(Database input, CancellationToken token = default)
    {
        var db = Normalize(input);
        var originalHash = Fingerprint(db);
        await using var connection = await source.OpenConnectionAsync(token);
        await using var transaction = await Begin(connection, token);
        async Task Execute(string sql, params object[] values)
        {
            await using var command = Command(connection, transaction, sql, values);
            await command.ExecuteNonQueryAsync(token);
        }
        await Execute("""
            CREATE TABLE IF NOT EXISTS botgram_backend_v1.migration_snapshot (
                singleton boolean PRIMARY KEY DEFAULT true CHECK(singleton), source_hash text NOT NULL, state_hash text NOT NULL)
            """);
        await using (var marker = Command(connection, transaction,
            "SELECT source_hash,state_hash FROM botgram_backend_v1.migration_snapshot WHERE singleton"))
        await using (var reader = await marker.ExecuteReaderAsync(token))
        {
            if (await reader.ReadAsync(token))
            {
                var sourceHash = reader.GetString(0);
                var stateHash = reader.GetString(1);
                await reader.DisposeAsync();
                if (sourceHash != originalHash || Fingerprint(await Export(connection, transaction, token)) != stateHash)
                    throw new InvalidOperationException("Import source differs or the imported business state has changed");
                await transaction.CommitAsync(token);
                return false;
            }
        }
        await using (var empty = Command(connection, transaction, """
            SELECT EXISTS(SELECT 1 FROM botgram_intake_v1.users) OR EXISTS(SELECT 1 FROM botgram_intake_v1.events)
                OR EXISTS(SELECT 1 FROM botgram_backend_v1.identities)
            """))
            if ((bool)(await empty.ExecuteScalarAsync(token))!) throw new InvalidOperationException("Import requires an empty isolated target");
        Gids.Migrate(db);
        db = Normalize(db);
        Validate(db);
        foreach (var user in db.Users)
        {
            await Execute("INSERT INTO botgram_intake_v1.users(id) VALUES($1)", user.Id);
            await Execute("""
                INSERT INTO botgram_backend_v1.accounts(id,username,display_name,salt,password_hash,gid,premium)
                VALUES($1,$2,$3,$4,$5,$6,$7)
                """, user.Id, user.Username, user.DisplayName, user.Salt, user.PasswordHash, user.Gid, user.Premium);
            await Execute("INSERT INTO botgram_intake_v1.sync(user_id,pts) VALUES($1,$2)", user.Id, SyncService.Pts(db, user.Id));
        }
        foreach (var session in db.Sessions)
            await Execute("INSERT INTO botgram_intake_v1.sessions(token_hash,user_id,expires_at) VALUES($1,$2,$3)",
                session.TokenHash, session.UserId, session.ExpiresAt);
        foreach (var pair in db.AccountPrivacy ?? [])
            await Execute("INSERT INTO botgram_backend_v1.privacy_accounts(user_id,state) VALUES($1,$2::jsonb)",
                pair.Key, JsonSerializer.Serialize(pair.Value, Store.Json));
        foreach (var conversation in db.Conversations)
        {
            await Execute("INSERT INTO botgram_intake_v1.conversations(id,sequence) VALUES($1,$2)",
                conversation.Id, (long)db.Messages.Count(m => m.ConversationId == conversation.Id));
            var directKey = conversation.Kind == "group" ? null : conversation.Kind + ":" + string.Join(",", conversation.Members.Order());
            await Execute("""
                INSERT INTO botgram_backend_v1.conversations(id,kind,title,created_at,direct_key) VALUES($1,$2,$3,$4,$5::text)
                """, conversation.Id, conversation.Kind, conversation.Title, conversation.CreatedAt, (object?)directKey ?? DBNull.Value);
            for (var i = 0; i < conversation.Members.Length; i++)
                await Execute("INSERT INTO botgram_intake_v1.members(conversation_id,user_id,position) VALUES($1,$2,$3)",
                    conversation.Id, conversation.Members[i], i);
        }
        foreach (var group in db.Messages.GroupBy(m => m.ConversationId))
        {
            long sequence = 0;
            foreach (var message in group.OrderBy(m => m.Id))
                await Execute("""
                    INSERT INTO botgram_intake_v1.messages(id,conversation_id,conversation_sequence,sender_id,client_key,body)
                    OVERRIDING SYSTEM VALUE VALUES($1,$2,$3,$4,$5,$6::jsonb)
                    """, message.Id, message.ConversationId, ++sequence, message.SenderId, message.ClientMessageId, JsonSerializer.Serialize(message));
        }
        foreach (var contact in db.Contacts)
            await Execute("INSERT INTO botgram_backend_v1.contacts(owner_id,user_id,first_name,last_name) VALUES($1,$2,$3,$4)",
                contact.OwnerId, contact.UserId, contact.FirstName, contact.LastName);
        foreach (var receipt in db.Receipts)
            await Execute("INSERT INTO botgram_backend_v1.receipts(conversation_id,user_id,message_id) VALUES($1,$2,$3)",
                receipt.ConversationId, receipt.UserId, receipt.MessageId);
        foreach (var call in db.Calls)
            await Execute("INSERT INTO botgram_backend_v1.calls(id,conversation_id,caller_id,callee_id,state,updated_at) VALUES($1,$2,$3,$4,$5,$6)",
                call.Id, call.ConversationId, call.CallerId, call.CalleeId, call.State, call.UpdatedAt);
        foreach (var identity in db.TelegramIds)
            await Execute("INSERT INTO botgram_backend_v1.identities(key,value) VALUES($1,$2)", identity.Key, identity.Value);
        var pts = db.Users.ToDictionary(u => u.Id, _ => 1);
        var sequences = db.Messages.GroupBy(m => m.ConversationId).SelectMany(group =>
            group.OrderBy(m => m.Id).Select((message, index) => (message.Id, Sequence: (long)index + 1))).ToDictionary(p => p.Id, p => p.Sequence);
        foreach (var item in db.Events)
        {
            var partition = item.Payload switch
            {
                Message m => m.ConversationId, Conversation c => c.Id, Receipt r => r.ConversationId,
                Call c => c.ConversationId, GidChanged g => "user:" + g.UserId,
                CallSignal s => db.Calls.Single(c => c.Id == s.CallId).ConversationId,
                _ => throw new InvalidDataException("Unsupported snapshot event")
            };
            var message = item.Payload as Message;
            var outboxId = Guid.NewGuid();
            var positions = item.Recipients.Select(recipient => new RecipientPosition(recipient,
                item.Type is "message.created" or "receipt.updated" or "user.gid_changed" ? ++pts[recipient] : null)).ToArray();
            DeliveryEventCodec.Validate(new(outboxId, 1, item.Id, partition, item.Type, item.Payload, positions,
                item.CreatedAt, item.ExpiresAt, message is null ? null : sequences[message.Id],
                message is null ? null : 1L, message is null ? null : 1));
            await Execute("""
                INSERT INTO botgram_intake_v1.events(id,message_id,type,created_at,schema_version,payload,expires_at)
                VALUES($1,$2::bigint,$3,$4,1,$5::jsonb,$6::timestamptz)
                """, item.Id, (object?)message?.Id ?? DBNull.Value, item.Type, item.CreatedAt,
                message is null ? JsonSerializer.Serialize(item.Payload, item.Payload.GetType()) : DBNull.Value,
                (object?)item.ExpiresAt ?? DBNull.Value);
            foreach (var recipient in positions)
            {
                await Execute("INSERT INTO botgram_intake_v1.event_recipients(event_id,user_id,pts) VALUES($1,$2,$3::integer)",
                    item.Id, recipient.UserId, (object?)recipient.Pts ?? DBNull.Value);
            }
            await Execute("""
                INSERT INTO botgram_intake_v1.outbox(id,event_id,partition_key,schema_version,projection_version)
                VALUES($1,$2,$3,1,$4::bigint)
                """, outboxId, item.Id, partition, message is null ? DBNull.Value : 1L);
        }
        await Execute("SELECT setval(pg_get_serial_sequence('botgram_intake_v1.messages','id'),$1,false)", db.NextMessageId);
        await Execute("SELECT setval('botgram_backend_v1.telegram_identity',$1,false)", db.NextTelegramId);
        await Execute("UPDATE botgram_intake_v1.event_cursor SET value=$1 WHERE singleton", db.NextEventId - 1);
        var hash = Fingerprint(db);
        if (Fingerprint(await Export(connection, transaction, token)) != hash) throw new InvalidDataException("Imported state does not match its canonical source");
        await Execute("INSERT INTO botgram_backend_v1.migration_snapshot(singleton,source_hash,state_hash) VALUES(true,$1,$2)", originalHash, hash);
        await transaction.CommitAsync(token);
        return true;
    }

    public async Task<Database> ExportAsync(CancellationToken token = default)
    {
        await using var connection = await source.OpenConnectionAsync(token);
        await using var transaction = await Begin(connection, token);
        var result = await Export(connection, transaction, token);
        await transaction.CommitAsync(token);
        return result;
    }

    private static NpgsqlCommand Command(NpgsqlConnection connection, NpgsqlTransaction transaction, string sql, params object[] values)
    {
        var command = new NpgsqlCommand(sql, connection, transaction);
        foreach (var value in values) command.Parameters.Add(new NpgsqlParameter { Value = value });
        return command;
    }

    private static async Task<Database> Export(NpgsqlConnection connection, NpgsqlTransaction transaction, CancellationToken token)
    {
        var db = new Database();
        async Task Rows(string sql, Action<NpgsqlDataReader> read)
        {
            await using var command = Command(connection, transaction, sql);
            await using var reader = await command.ExecuteReaderAsync(token);
            while (await reader.ReadAsync(token)) read(reader);
        }
        await Rows("SELECT EXISTS(SELECT 1 FROM botgram_backend_v1.wallpaper_accounts WHERE revision<>0 OR installed IS NOT NULL)",
            r => { if (r.GetBoolean(0)) throw new InvalidOperationException("Legacy export cannot represent wallpaper state; retain the relational database and pre-cutover backup"); });
        await Rows("SELECT id,username,display_name,salt,password_hash,gid,premium FROM botgram_backend_v1.accounts ORDER BY ordinal",
            r => db.Users.Add(new(r.GetString(0), r.GetString(1), r.GetString(2), r.GetString(3), r.GetString(4), r.GetString(5), r.GetBoolean(6))));
        await Rows("SELECT token_hash,user_id,expires_at FROM botgram_intake_v1.sessions ORDER BY token_hash",
            r => db.Sessions.Add(new(r.GetString(0), r.GetString(1), r.GetFieldValue<DateTimeOffset>(2))));
        await Rows("SELECT user_id,state::text FROM botgram_backend_v1.privacy_accounts ORDER BY user_id", r =>
        {
            db.AccountPrivacy ??= [];
            db.AccountPrivacy.Add(r.GetString(0), JsonSerializer.Deserialize<AccountPrivacyState>(r.GetString(1), Store.Json)
                ?? throw new InvalidDataException("Missing account privacy state"));
        });
        await Rows("""
            SELECT c.id,c.kind,c.title,ARRAY(SELECT user_id FROM botgram_intake_v1.members m WHERE m.conversation_id=c.id ORDER BY position),c.created_at
            FROM botgram_backend_v1.conversations c ORDER BY ordinal
            """, r => db.Conversations.Add(new(r.GetString(0), r.GetString(1), r.GetString(2), r.GetFieldValue<string[]>(3), r.GetFieldValue<DateTimeOffset>(4))));
        await Rows("SELECT body::text FROM botgram_intake_v1.messages ORDER BY id", r => db.Messages.Add(JsonSerializer.Deserialize<Message>(r.GetString(0))!));
        await Rows("SELECT conversation_id,user_id,message_id FROM botgram_backend_v1.receipts", r => db.Receipts.Add(new(r.GetString(0), r.GetString(1), r.GetInt64(2))));
        await Rows("SELECT owner_id,user_id,first_name,last_name FROM botgram_backend_v1.contacts ORDER BY ordinal", r => db.Contacts.Add(new(r.GetString(0), r.GetString(1), r.GetString(2), r.GetString(3))));
        await Rows("SELECT id,conversation_id,caller_id,callee_id,state,updated_at FROM botgram_backend_v1.calls ORDER BY ordinal",
            r => db.Calls.Add(new(r.GetString(0), r.GetString(1), r.GetString(2), r.GetString(3), r.GetString(4), r.GetFieldValue<DateTimeOffset>(5))));
        await Rows("SELECT key,value FROM botgram_backend_v1.identities ORDER BY key", r => db.TelegramIds.Add(r.GetString(0), r.GetInt64(1)));
        await Rows("""
            SELECT e.id,e.type,COALESCE(e.payload,m.body)::text,e.created_at,e.expires_at,
                ARRAY(SELECT user_id FROM botgram_intake_v1.event_recipients r WHERE r.event_id=e.id ORDER BY user_id COLLATE "C")
            FROM botgram_intake_v1.events e LEFT JOIN botgram_intake_v1.messages m ON m.id=e.message_id ORDER BY e.id
            """, r => db.Events.Add(new(r.GetInt64(0), r.GetFieldValue<string[]>(5), r.GetString(1),
                (IEventPayload)JsonSerializer.Deserialize(r.GetString(2), EventPayloadTypes.For(r.GetString(1)))!,
                r.GetFieldValue<DateTimeOffset>(3), r.IsDBNull(4) ? null : r.GetFieldValue<DateTimeOffset>(4))));
        await Rows("SELECT last_value+CASE WHEN is_called THEN 1 ELSE 0 END FROM botgram_intake_v1.messages_id_seq", r => db.NextMessageId = r.GetInt64(0));
        await Rows("SELECT value+1 FROM botgram_intake_v1.event_cursor WHERE singleton", r => db.NextEventId = r.GetInt64(0));
        await Rows("SELECT last_value+CASE WHEN is_called THEN 1 ELSE 0 END FROM botgram_backend_v1.telegram_identity", r => db.NextTelegramId = r.GetInt64(0));
        return Normalize(db);
    }
}
