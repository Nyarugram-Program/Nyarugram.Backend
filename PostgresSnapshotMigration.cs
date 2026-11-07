using System.Security.Cryptography;
using System.Text.Json;
using System.Text.Json.Nodes;
using Npgsql;

namespace Botgram.Backend.Storage;

// Offline import/export for the isolated validation database. The input snapshot is never written.
public sealed class PostgresSnapshotMigration(NpgsqlDataSource source)
{
    private static DateTimeOffset Time(DateTimeOffset value) => new(value.UtcTicks - value.UtcTicks % 10, TimeSpan.Zero);
    // PostgreSQL jsonb reorders object keys. Canonicalize JsonNode payloads so a
    // legacy snapshot and its relational roundtrip hash identically.
    private static void Sort(JsonNode? node)
    {
        if (node is JsonObject obj)
        {
            var entries = obj.Select(p => (p.Key, Value: p.Value?.DeepClone()))
                .OrderBy(p => p.Key, StringComparer.Ordinal).ToArray();
            obj.Clear();
            foreach (var (key, child) in entries) { Sort(child); obj.Add(key, child); }
        }
        else if (node is JsonArray array)
        {
            foreach (var child in array) Sort(child);
        }
    }

    private static JsonObject? Canonical(JsonObject? value)
    {
        if (value is null) return null;
        var copy = (JsonObject)value.DeepClone();
        Sort(copy);
        return copy;
    }

    private static IEventPayload Payload(IEventPayload value) => value switch
    {
        Message m => m with { CreatedAt = Time(m.CreatedAt) },
        Conversation c => c with { CreatedAt = Time(c.CreatedAt) },
        Call c => c with
        {
            UpdatedAt = Time(c.UpdatedAt), Protocol = Canonical(c.Protocol),
            Connections = c.Connections?.Select(v => (JsonObject)Canonical(v)!).ToArray(),
            RequestProtocol = Canonical(c.RequestProtocol), AcceptProtocol = Canonical(c.AcceptProtocol)
        },
        _ => value
    };

    public static Database Normalize(Database input)
    {
        var db = JsonSerializer.Deserialize<Database>(JsonSerializer.Serialize(input, Store.Json), Store.Json)!;
        db.Users = db.Users.Select(u => u with
        {
            LastSeen = u.LastSeen is { } seen ? Time(seen) : null,
            PresenceNotifiedAt = u.PresenceNotifiedAt is { } notified ? Time(notified) : null
        }).ToList();
        db.Sessions = db.Sessions.OrderBy(s => s.TokenHash, StringComparer.Ordinal)
            .Select(s => s with { ExpiresAt = Time(s.ExpiresAt), ForegroundAt = s.ForegroundAt is { } seen ? Time(seen) : null }).ToList();
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
        // E2EE directory and state are part of the canonical migration fingerprint.
        // Ordering is deterministic so import/export comparisons are stable.
        db.E2eeDevices = db.E2eeDevices.OrderBy(d => d.UserId, StringComparer.Ordinal).Select(d => d with
        {
            UpdatedAt = Time(d.UpdatedAt),
            OneTimePreKeys = d.OneTimePreKeys.OrderBy(k => k.KeyId).ToArray()
        }).ToList();
        db.E2eeConversations = db.E2eeConversations.OrderBy(c => c.ConversationId, StringComparer.Ordinal)
            .Select(c => c with
            {
                UpdatedAt = Time(c.UpdatedAt),
                IdentityConfirmedBy = c.IdentityConfirmedBy.Distinct(StringComparer.Ordinal).Order(StringComparer.Ordinal).ToArray()
            }).ToList();
        if (db.AccountPrivacy is not null)
        {
            db.AccountPrivacy = db.AccountPrivacy.Where(p => !AccountPrivacyService.IsDefault(p.Value))
                .OrderBy(p => p.Key, StringComparer.Ordinal).ToDictionary(p => p.Key, p => p.Value);
            foreach (var state in db.AccountPrivacy.Values)
            {
                state.SpecialContacts = state.SpecialContacts.Order(StringComparer.Ordinal).ToHashSet(StringComparer.Ordinal);
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
            if (pair.Value.SpecialContactsRevision < 0 || pair.Value.SpecialContacts.Count != 0 && pair.Value.SpecialContactsRevision == 0
                || pair.Value.SpecialContacts.Any(id => id == pair.Key || !db.Users.Any(u => u.Id == id)
                    || !db.Contacts.Any(c => c.OwnerId == pair.Key && c.UserId == id)
                    || AccountPrivacyService.IsBlocked(db, pair.Key, id) || AccountPrivacyService.IsBlocked(db, id, pair.Key)))
                throw new InvalidDataException("Invalid snapshot special contacts");
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
        foreach (var device in db.E2eeDevices)
        {
            if (!db.Users.Any(u => u.Id == device.UserId) || device.DeviceId != 1 || device.Generation < 1 ||
                device.RegistrationId is < 1 or > 16383 || device.SignedPreKeyId < 1 || device.KyberPreKeyId < 1 ||
                device.IdentityKey.Length is < 32 or > 4096 || device.SignedPreKey.Length is < 32 or > 4096 ||
                device.KyberPreKey.Length is < 32 or > 4096 ||
                device.OneTimePreKeys.Length > 100 ||
                device.OneTimePreKeys.Select(k => k.KeyId).Distinct().Count() != device.OneTimePreKeys.Length ||
                device.OneTimePreKeys.Any(k => k.KeyId < 1 || k.PublicKey.Length is < 32 or > 4096))
                throw new InvalidDataException("Invalid snapshot E2EE device directory");
            if (db.E2eeDevices.Select(d => d.UserId).Distinct().Count() != db.E2eeDevices.Count)
                throw new InvalidDataException("Duplicate snapshot E2EE device");
        }
        foreach (var chat in db.E2eeConversations)
        {
            if (!db.Conversations.Any(c => c.Id == chat.ConversationId) ||
                chat.State is not ("pending" or "active" or "closing" or "suspended" or "closed") ||
                chat.ProtocolVersion != 2 || chat.Epoch < 1 ||
                !db.Users.Any(u => u.Id == chat.InitiatorId) ||
                chat.CloseRequestedBy is { Length: > 0 } && !db.Users.Any(u => u.Id == chat.CloseRequestedBy) ||
                chat.IdentityConfirmedBy.Any(id => !db.Users.Any(u => u.Id == id)))
                throw new InvalidDataException("Invalid snapshot E2EE conversation state");
            if (db.E2eeConversations.Select(c => c.ConversationId).Distinct().Count() != db.E2eeConversations.Count)
                throw new InvalidDataException("Duplicate snapshot E2EE conversation");
        }
    }

    private async Task<NpgsqlTransaction> Begin(NpgsqlConnection connection, CancellationToken token)
    {
        var target = new NpgsqlConnectionStringBuilder(source.ConnectionString);
        var isolated = FourComponentOptions.Loopback(target.Host) &&
            target.Database?.StartsWith("botgram_test_", StringComparison.Ordinal) == true;
        if (!isolated && !PostgresSchemaUpgrade.PrimaryAuthorized(target.Database))
            throw new InvalidOperationException(
                "Snapshot migration requires a loopback botgram_test_* database or BOTGRAM_PRIMARY_MAINTENANCE=1 with a dedicated production database");
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
            // v9 tables may be absent on an un-upgraded database; lock them when
            // present so E2EE state is part of the same consistent snapshot.
            await using var e2eeTables = new NpgsqlCommand("""
                DO $lock$ BEGIN
                IF to_regclass('botgram_backend_v1.e2ee_devices') IS NOT NULL THEN
                    LOCK TABLE botgram_backend_v1.e2ee_devices,
                        botgram_backend_v1.e2ee_one_time_pre_keys,
                        botgram_backend_v1.e2ee_conversations IN ACCESS EXCLUSIVE MODE NOWAIT;
                END IF;
                END $lock$;
                """, connection, transaction);
            await e2eeTables.ExecuteNonQueryAsync(token);
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
        // Explicit schema compatibility boundary: E2EE tables are required.
        await using (var check = Command(connection, transaction,
            "SELECT ARRAY(SELECT version FROM botgram_backend_v1.schema_versions ORDER BY version)"))
        {
            var versions = (int[])(await check.ExecuteScalarAsync(token))!;
            if (!versions.SequenceEqual(Enumerable.Range(1, PostgresBackendSchema.CurrentVersion)))
                throw new InvalidOperationException(
                    "Snapshot import requires offline schema versions 1.." + PostgresBackendSchema.CurrentVersion
                    + "; run Storage.Admin upgrade before importing");
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
                OR EXISTS(SELECT 1 FROM botgram_backend_v1.e2ee_devices)
                OR EXISTS(SELECT 1 FROM botgram_backend_v1.e2ee_conversations)
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
            await Execute("""
                INSERT INTO botgram_backend_v1.presence_accounts(user_id,last_seen,online,notified_at) VALUES($1,$2,$3,$4)
                """, user.Id, (object?)user.LastSeen ?? DBNull.Value, user.Online, (object?)user.PresenceNotifiedAt ?? DBNull.Value);
        }
        foreach (var session in db.Sessions)
            await Execute("INSERT INTO botgram_intake_v1.sessions(token_hash,user_id,expires_at) VALUES($1,$2,$3)",
                session.TokenHash, session.UserId, session.ExpiresAt);
        foreach (var session in db.Sessions.Where(s => s.ForegroundAt is not null))
            await Execute("INSERT INTO botgram_backend_v1.presence_sessions(token_hash,foreground_at) VALUES($1,$2)",
                session.TokenHash, session.ForegroundAt!.Value);
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
        // E2EE public-key directory, one-time pre-keys (including consumed flags)
        // and per-chat state. UUIDs, GIDs, TL identities and message IDs are untouched.
        foreach (var device in db.E2eeDevices)
        {
            await Execute("""
                INSERT INTO botgram_backend_v1.e2ee_devices
                    (user_id,device_id,registration_id,identity_key,signed_pre_key_id,signed_pre_key,
                     signed_pre_key_signature,kyber_pre_key_id,kyber_pre_key,kyber_pre_key_signature,generation,updated_at)
                VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)
                """, device.UserId, (int)device.DeviceId, device.RegistrationId, device.IdentityKey,
                device.SignedPreKeyId, device.SignedPreKey, device.SignedPreKeySignature,
                device.KyberPreKeyId, device.KyberPreKey, device.KyberPreKeySignature,
                device.Generation, device.UpdatedAt);
            foreach (var key in device.OneTimePreKeys)
                await Execute("""
                    INSERT INTO botgram_backend_v1.e2ee_one_time_pre_keys(user_id,key_id,public_key,consumed)
                    VALUES($1,$2,$3,$4)
                    """, device.UserId, key.KeyId, key.PublicKey, key.Consumed);
        }
        foreach (var chat in db.E2eeConversations)
            await Execute("""
                INSERT INTO botgram_backend_v1.e2ee_conversations
                    (conversation_id,state,protocol_version,epoch,initiator_id,boundary_message_id,
                     close_boundary_message_id,close_requested_by,identity_confirmed_by,identity_generation,updated_at)
                VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
                """, chat.ConversationId, chat.State, (int)chat.ProtocolVersion, chat.Epoch, chat.InitiatorId,
                chat.BoundaryMessageId, chat.CloseBoundaryMessageId, (object?)chat.CloseRequestedBy ?? DBNull.Value,
                chat.IdentityConfirmedBy, chat.IdentityGeneration, chat.UpdatedAt);
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
            await Execute("""
                INSERT INTO botgram_backend_v1.calls(id,conversation_id,caller_id,callee_id,state,updated_at,
                    telegram_id,access_hash,random_id,video,protocol,g_a_hash,g_b,g_a,key_fingerprint,
                    date,receive_date,start_date,reason,duration,connections,p2p_allowed)
                VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11::jsonb,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21::jsonb,$22)
                """, call.Id, call.ConversationId, call.CallerId, call.CalleeId, call.State, call.UpdatedAt,
                call.TelegramId, call.AccessHash, call.RandomId, call.Video,
                call.Protocol?.ToJsonString() ?? (object)DBNull.Value,
                (object?)call.GAHash ?? DBNull.Value, (object?)call.GB ?? DBNull.Value, (object?)call.GA ?? DBNull.Value,
                call.KeyFingerprint, call.Date, call.ReceiveDate, call.StartDate, call.Reason, call.Duration,
                call.Connections is null ? (object)DBNull.Value : new JsonArray(call.Connections.Select(c => c.DeepClone()).ToArray()).ToJsonString(),
                call.P2pAllowed);
        // The legacy call counter can be ahead of the highest surviving call id.
        // Keep the sequence at both boundaries so replay and new calls never
        // reuse an id, and export can reconstruct the exact source counter.
        await Execute("""
            SELECT setval('botgram_backend_v1.call_identity',
                GREATEST(999999,(SELECT COALESCE(MAX(telegram_id),0) FROM botgram_backend_v1.calls),$1::bigint))
            """, Math.Max(0L, db.NextCallId - 1));
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
                PresenceChanged p => "user:" + p.UserId,
                CallSignal s => db.Calls.Single(c => c.Id == s.CallId).ConversationId,
                _ => throw new InvalidDataException("Unsupported snapshot event")
            };
            var message = item.Payload as Message;
            var outboxId = Guid.NewGuid();
            var positions = item.Recipients.Select(recipient => new RecipientPosition(recipient,
                item.Type is "message.created" or "receipt.updated" or "user.gid_changed" or "user.presence_changed" ? ++pts[recipient] : null)).ToArray();
            int schema = item.Type == "user.presence_changed" ? StorageContractVersions.PresenceEventSchema : StorageContractVersions.EventSchema;
            DeliveryEventCodec.Validate(new(outboxId, schema, item.Id, partition, item.Type, item.Payload, positions,
                item.CreatedAt, item.ExpiresAt, message is null ? null : sequences[message.Id],
                message is null ? null : 1L, message is null ? null : 1));
            await Execute("""
                INSERT INTO botgram_intake_v1.events(id,message_id,type,created_at,schema_version,payload,expires_at)
                VALUES($1,$2::bigint,$3,$4,$7,$5::jsonb,$6::timestamptz)
                """, item.Id, (object?)message?.Id ?? DBNull.Value, item.Type, item.CreatedAt,
                message is null ? JsonSerializer.Serialize(item.Payload, item.Payload.GetType()) : DBNull.Value,
                (object?)item.ExpiresAt ?? DBNull.Value, schema);
            foreach (var recipient in positions)
            {
                await Execute("INSERT INTO botgram_intake_v1.event_recipients(event_id,user_id,pts) VALUES($1,$2,$3::integer)",
                    item.Id, recipient.UserId, (object?)recipient.Pts ?? DBNull.Value);
            }
            await Execute("""
                INSERT INTO botgram_intake_v1.outbox(id,event_id,partition_key,schema_version,projection_version)
                VALUES($1,$2,$3,$5,$4::bigint)
                """, outboxId, item.Id, partition, message is null ? DBNull.Value : 1L, schema);
        }
        await Execute("SELECT setval(pg_get_serial_sequence('botgram_intake_v1.messages','id'),$1,false)", db.NextMessageId);
        await Execute("SELECT setval('botgram_backend_v1.telegram_identity',$1,false)", db.NextTelegramId);
        await Execute("UPDATE botgram_intake_v1.event_cursor SET value=$1 WHERE singleton", db.NextEventId - 1);
        var hash = Fingerprint(db);
        if (Fingerprint(await Export(connection, transaction, token)) != hash)
            throw new InvalidDataException("Imported state does not match its canonical source");
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
        await Rows("""
            SELECT a.id,a.username,a.display_name,a.salt,a.password_hash,a.gid,a.premium,p.last_seen,p.online,p.notified_at
            FROM botgram_backend_v1.accounts a LEFT JOIN botgram_backend_v1.presence_accounts p ON p.user_id=a.id ORDER BY a.ordinal
            """, r => db.Users.Add(new(r.GetString(0), r.GetString(1), r.GetString(2), r.GetString(3), r.GetString(4), r.GetString(5), r.GetBoolean(6),
                r.IsDBNull(7) ? null : r.GetFieldValue<DateTimeOffset>(7), !r.IsDBNull(8) && r.GetBoolean(8),
                r.IsDBNull(9) ? null : r.GetFieldValue<DateTimeOffset>(9))));
        await Rows("""
            SELECT s.token_hash,s.user_id,s.expires_at,h.foreground_at FROM botgram_intake_v1.sessions s
            LEFT JOIN botgram_backend_v1.presence_sessions h ON h.token_hash=s.token_hash ORDER BY s.token_hash
            """, r => db.Sessions.Add(new(r.GetString(0), r.GetString(1), r.GetFieldValue<DateTimeOffset>(2),
                r.IsDBNull(3) ? null : r.GetFieldValue<DateTimeOffset>(3))));
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
        var devices = new Dictionary<string, E2eeDevice>(StringComparer.Ordinal);
        await Rows($"""
            SELECT user_id,device_id,registration_id,identity_key,signed_pre_key_id,signed_pre_key,
                signed_pre_key_signature,kyber_pre_key_id,kyber_pre_key,kyber_pre_key_signature,generation,updated_at
            FROM botgram_backend_v1.e2ee_devices ORDER BY user_id
            """, r =>
        {
            var id = r.GetString(0);
            devices[id] = new(id, (uint)r.GetInt32(1), r.GetInt32(2), r.GetFieldValue<byte[]>(3),
                r.GetInt32(4), r.GetFieldValue<byte[]>(5), r.GetFieldValue<byte[]>(6),
                r.GetInt32(7), r.GetFieldValue<byte[]>(8), r.GetFieldValue<byte[]>(9),
                r.GetInt64(10), r.GetFieldValue<DateTimeOffset>(11), []);
        });
        await Rows("""
            SELECT user_id,key_id,public_key,consumed FROM botgram_backend_v1.e2ee_one_time_pre_keys
            ORDER BY user_id,key_id
            """, r =>
        {
            var id = r.GetString(0);
            if (!devices.TryGetValue(id, out var device)) throw new InvalidDataException("Orphan E2EE pre-key");
            devices[id] = device with { OneTimePreKeys = [.. device.OneTimePreKeys,
                new E2eeOneTimePreKey(r.GetInt32(1), r.GetFieldValue<byte[]>(2), r.GetBoolean(3))] };
        });
        db.E2eeDevices.AddRange(devices.Values);
        await Rows("""
            SELECT conversation_id,state,protocol_version,epoch,initiator_id,boundary_message_id,
                close_boundary_message_id,close_requested_by,identity_confirmed_by,identity_generation,updated_at
            FROM botgram_backend_v1.e2ee_conversations ORDER BY conversation_id
            """, r => db.E2eeConversations.Add(new(r.GetString(0), r.GetString(1), (uint)r.GetInt32(2),
            r.GetInt64(3), r.GetString(4), r.GetInt64(5), r.GetInt64(6), r.IsDBNull(7) ? null : r.GetString(7),
            r.GetFieldValue<string[]>(8), r.GetFieldValue<DateTimeOffset>(10), r.GetInt64(9))));
        await Rows(PostgresBackend.CallRows + " ORDER BY ordinal",
            r => db.Calls.Add(PostgresBackend.CallRow(r)));
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
        // Import pins the sequence at 999999 for a legacy snapshot that never
        // allocated a real call id; that sentinel reconstructs NextCallId=0
        // (lazy 1_000_000_000 initialization). Otherwise the sequence is authoritative.
        await Rows("""
            SELECT CASE WHEN last_value=999999 THEN 0
                ELSE last_value+CASE WHEN is_called THEN 1 ELSE 0 END END
            FROM botgram_backend_v1.call_identity
            """, r => db.NextCallId = r.GetInt64(0));
        return Normalize(db);
    }
}
