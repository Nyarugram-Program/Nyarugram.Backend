using System.Globalization;
using System.Security.Cryptography;
using System.Text;
using Google.Protobuf;
using P = Botgram.Protocol.V1;

namespace Botgram.Backend.Storage;

/// <summary>
/// PostgreSQL-authoritative E2EE directory and per-chat state machine.
///
/// Everything runs inside the caller's single ReadCommitted WriteAsync
/// transaction: pre-key consumption, device generation, epoch changes, service
/// messages, pts, events and outbox share one COMMIT. Per-user/per-conversation
/// advisory transaction locks serialize concurrent state transitions. The server
/// only handles public keys, protocol state and opaque ciphertext.
/// </summary>
public sealed partial class PostgresBackend
{
    private const uint DirectionalModeVersion = 2;
    private const uint EnvelopeVersion = 1;
    private const int MaxCiphertext = 65536;
    private const int MaxOneTimePreKeys = 100;
    private const string StatePending = "pending";
    private const string StateActive = "active";
    private const string StateClosing = "closing";
    private const string StateSuspended = "suspended";
    private const string StateClosed = "closed";
    private const long E2eeLockNamespace = 7459241304240;

    private sealed record E2eeDeviceRow(string UserId, uint DeviceId, int RegistrationId, byte[] IdentityKey,
        int SignedPreKeyId, byte[] SignedPreKey, byte[] SignedPreKeySignature,
        int KyberPreKeyId, byte[] KyberPreKey, byte[] KyberPreKeySignature,
        long Generation, DateTimeOffset UpdatedAt);

    private sealed record E2eeChatRow(string ConversationId, string State, uint ProtocolVersion, long Epoch,
        string InitiatorId, long BoundaryMessageId, long CloseBoundaryMessageId, string? CloseRequestedBy,
        string[] IdentityConfirmedBy, long IdentityGeneration, DateTimeOffset UpdatedAt);

    private const string DeviceColumns = """
        user_id,device_id,registration_id,identity_key,signed_pre_key_id,signed_pre_key,
        signed_pre_key_signature,kyber_pre_key_id,kyber_pre_key,kyber_pre_key_signature,
        generation,updated_at
        """;

    private static E2eeDeviceRow ReadDevice(Npgsql.NpgsqlDataReader r) => new(
        r.GetString(0), (uint)r.GetInt32(1), r.GetInt32(2), r.GetFieldValue<byte[]>(3),
        r.GetInt32(4), r.GetFieldValue<byte[]>(5), r.GetFieldValue<byte[]>(6),
        r.GetInt32(7), r.GetFieldValue<byte[]>(8), r.GetFieldValue<byte[]>(9),
        r.GetInt64(10), r.GetFieldValue<DateTimeOffset>(11));

    private static async Task<E2eeDeviceRow?> Device(Transaction work, string uid, bool forUpdate = false) =>
        (await work.Rows($"SELECT {DeviceColumns} FROM botgram_backend_v1.e2ee_devices WHERE user_id=$1"
            + (forUpdate ? " FOR UPDATE" : ""), ReadDevice, uid)).SingleOrDefault();

    private static async Task<E2eeChatRow?> Chat(Transaction work, string conversationId, bool forUpdate = false) =>
        (await work.Rows("""
            SELECT conversation_id,state,protocol_version,epoch,initiator_id,boundary_message_id,
                close_boundary_message_id,close_requested_by,identity_confirmed_by,identity_generation,updated_at
            FROM botgram_backend_v1.e2ee_conversations WHERE conversation_id=$1
            """ + (forUpdate ? " FOR UPDATE" : ""), r => new E2eeChatRow(r.GetString(0), r.GetString(1),
            (uint)r.GetInt32(2), r.GetInt64(3), r.GetString(4), r.GetInt64(5), r.GetInt64(6),
            r.IsDBNull(7) ? null : r.GetString(7), r.GetFieldValue<string[]>(8), r.GetInt64(9),
            r.GetFieldValue<DateTimeOffset>(10)), conversationId)).SingleOrDefault();

    private static Task<int> SaveChat(Transaction work, E2eeChatRow row) => work.Execute("""
        INSERT INTO botgram_backend_v1.e2ee_conversations
            (conversation_id,state,protocol_version,epoch,initiator_id,boundary_message_id,
             close_boundary_message_id,close_requested_by,identity_confirmed_by,identity_generation,updated_at)
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
        ON CONFLICT(conversation_id) DO UPDATE SET
            state=EXCLUDED.state, protocol_version=EXCLUDED.protocol_version, epoch=EXCLUDED.epoch,
            initiator_id=EXCLUDED.initiator_id, boundary_message_id=EXCLUDED.boundary_message_id,
            close_boundary_message_id=EXCLUDED.close_boundary_message_id,
            close_requested_by=EXCLUDED.close_requested_by,
            identity_confirmed_by=EXCLUDED.identity_confirmed_by,
            identity_generation=EXCLUDED.identity_generation, updated_at=EXCLUDED.updated_at
        """, row.ConversationId, row.State, (int)row.ProtocolVersion, row.Epoch, row.InitiatorId,
        row.BoundaryMessageId, row.CloseBoundaryMessageId, (object?)row.CloseRequestedBy ?? DBNull.Value,
        row.IdentityConfirmedBy, row.IdentityGeneration, row.UpdatedAt);

    // Deterministic lock order prevents deadlocks between user and chat keys.
    private static async Task Lock(Transaction work, params string[] keys)
    {
        foreach (var key in keys.Distinct(StringComparer.Ordinal).Order(StringComparer.Ordinal))
            // hashtextextended(text,bigint) returns bigint, matching the single
            // bigint advisory-lock signature (the two-argument form is int/int).
            await work.Scalar("SELECT pg_advisory_xact_lock(hashtextextended($2,$1))", E2eeLockNamespace, key);
    }

    private static async Task<long> LastMessageId(Transaction work, string conversationId)
    {
        var value = await work.Scalar(
            "SELECT coalesce(max(id),0) FROM botgram_intake_v1.messages WHERE conversation_id=$1", conversationId);
        return value is long id ? id : Convert.ToInt64(value, CultureInfo.InvariantCulture);
    }

    private static void RequireDirect(Conversation conversation)
    {
        if (conversation.Kind != "direct" || conversation.Members.Distinct().Count() != 2)
            throw new ApiException(409, "E2EE_TARGET_INVALID");
    }

    private static string Other(Conversation conversation, string uid) => conversation.Members.Single(m => m != uid);

    /// <summary>
    /// Canonical version of both devices' identities, ordered by user id so both
    /// participants observe the same value. Generation sums are never used as an
    /// identity binding: a hash covers the actual public keys and generations.
    /// </summary>
    private static string IdentityVersion(E2eeDeviceRow? self, E2eeDeviceRow? peer)
    {
        if (self is null || peer is null) return "";
        var (first, second) = string.CompareOrdinal(self.UserId, peer.UserId) <= 0 ? (self, peer) : (peer, self);
        var material = string.Join('\n',
            first.UserId, first.Generation.ToString(CultureInfo.InvariantCulture),
            Convert.ToBase64String(first.IdentityKey),
            second.UserId, second.Generation.ToString(CultureInfo.InvariantCulture),
            Convert.ToBase64String(second.IdentityKey));
        return Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(material))).ToLowerInvariant();
    }

    private static bool IsConfirmedBy(string[] entries, string uid) =>
        entries.Any(entry => entry == uid || entry.StartsWith(uid + "|", StringComparison.Ordinal));

    private static bool IsConfirmedVersion(string[] entries, string uid, string version) =>
        entries.Contains(uid + "|" + version, StringComparer.Ordinal);

    private static bool AllConfirmedVersion(string[] entries, string[] members, string version) =>
        members.All(member => IsConfirmedVersion(entries, member, version));

    private static void ValidateKey(ByteString key, string code)
    {
        if (key.Length is < 32 or > 4096) throw new ApiException(400, code);
    }

    private static void ValidateSignature(ByteString signature, string code)
    {
        if (signature.Length is < 16 or > 256) throw new ApiException(400, code);
    }

    private static P.E2eeState StateOf(string state) => state switch
    {
        StatePending => P.E2eeState.E2EePending,
        StateActive => P.E2eeState.E2EeActive,
        StateClosing => P.E2eeState.E2EeClosing,
        StateSuspended => P.E2eeState.E2EeSuspended,
        StateClosed => P.E2eeState.E2EeClosed,
        _ => P.E2eeState.E2EeDisabled
    };

    // ---------- RPC 1: publish device ----------

    private async Task<P.E2eeDeviceInfo> PublishDevice(Transaction work, Session session, P.E2eePublishDeviceRequest input)
    {
        var uid = session.UserId;
        if (input.DeviceId is < 1 or > 1) throw new ApiException(400, "E2EE_DEVICE_INVALID");
        if (input.RegistrationId is < 1 or > 16383) throw new ApiException(400, "E2EE_REGISTRATION_INVALID");
        ValidateKey(input.IdentityKey, "E2EE_IDENTITY_INVALID");
        ValidateKey(input.SignedPreKey, "E2EE_SIGNED_PREKEY_INVALID");
        ValidateSignature(input.SignedPreKeySignature, "E2EE_SIGNED_PREKEY_SIGNATURE_INVALID");
        ValidateKey(input.KyberPreKey, "E2EE_KYBER_PREKEY_INVALID");
        ValidateSignature(input.KyberPreKeySignature, "E2EE_KYBER_PREKEY_SIGNATURE_INVALID");
        if (input.SignedPreKeyId < 1 || input.KyberPreKeyId < 1) throw new ApiException(400, "E2EE_PREKEY_ID_INVALID");
        if (input.OneTimePreKeys.Count > MaxOneTimePreKeys) throw new ApiException(400, "E2EE_PREKEYS_TOO_MANY");
        var ids = new HashSet<int>();
        foreach (var key in input.OneTimePreKeys)
        {
            if (key.KeyId < 1 || !ids.Add(key.KeyId)) throw new ApiException(400, "E2EE_PREKEY_ID_INVALID");
            ValidateKey(key.PublicKey, "E2EE_PREKEY_INVALID");
        }

        await Lock(work, uid);
        var existing = await Device(work, uid, true);
        var identity = input.IdentityKey.ToByteArray();
        var replaced = existing is not null && !existing.IdentityKey.AsSpan().SequenceEqual(identity);
        var generation = existing is null ? 1 : replaced ? existing.Generation + 1 : existing.Generation;
        var now = DateTimeOffset.UtcNow;

        await work.Execute("""
            INSERT INTO botgram_backend_v1.e2ee_devices
                (user_id,device_id,registration_id,identity_key,signed_pre_key_id,signed_pre_key,
                 signed_pre_key_signature,kyber_pre_key_id,kyber_pre_key,kyber_pre_key_signature,generation,updated_at)
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)
            ON CONFLICT(user_id) DO UPDATE SET
                device_id=EXCLUDED.device_id, registration_id=EXCLUDED.registration_id,
                identity_key=EXCLUDED.identity_key, signed_pre_key_id=EXCLUDED.signed_pre_key_id,
                signed_pre_key=EXCLUDED.signed_pre_key, signed_pre_key_signature=EXCLUDED.signed_pre_key_signature,
                kyber_pre_key_id=EXCLUDED.kyber_pre_key_id, kyber_pre_key=EXCLUDED.kyber_pre_key,
                kyber_pre_key_signature=EXCLUDED.kyber_pre_key_signature,
                generation=EXCLUDED.generation, updated_at=EXCLUDED.updated_at
            """, uid, (int)input.DeviceId, input.RegistrationId, identity, input.SignedPreKeyId,
            input.SignedPreKey.ToByteArray(), input.SignedPreKeySignature.ToByteArray(), input.KyberPreKeyId,
            input.KyberPreKey.ToByteArray(), input.KyberPreKeySignature.ToByteArray(), generation, now);

        // An identity replacement invalidates every previously published pre-key.
        // Otherwise retain only still-unconsumed keys and merge the new batch.
        await work.Execute(replaced || existing is null
            ? "DELETE FROM botgram_backend_v1.e2ee_one_time_pre_keys WHERE user_id=$1"
            : "DELETE FROM botgram_backend_v1.e2ee_one_time_pre_keys WHERE user_id=$1 AND consumed", uid);
        foreach (var key in input.OneTimePreKeys)
            await work.Execute("""
                INSERT INTO botgram_backend_v1.e2ee_one_time_pre_keys(user_id,key_id,public_key,consumed)
                VALUES ($1,$2,$3,false)
                ON CONFLICT(user_id,key_id) DO UPDATE SET public_key=EXCLUDED.public_key, consumed=false
                """, uid, key.KeyId, key.PublicKey.ToByteArray());
        await work.Execute("""
            DELETE FROM botgram_backend_v1.e2ee_one_time_pre_keys WHERE user_id=$1 AND NOT consumed
              AND key_id NOT IN (SELECT key_id FROM botgram_backend_v1.e2ee_one_time_pre_keys
                                 WHERE user_id=$1 AND NOT consumed ORDER BY key_id LIMIT 100)
            """, uid);

        if (replaced) await SuspendForIdentityChange(work, session, uid);
        var remaining = (int)(long)(await work.Scalar("""
            SELECT count(*) FROM botgram_backend_v1.e2ee_one_time_pre_keys WHERE user_id=$1 AND NOT consumed
            """, uid))!;
        return new P.E2eeDeviceInfo
        {
            DeviceId = input.DeviceId, RegistrationId = input.RegistrationId,
            IdentityKey = ByteString.CopyFrom(identity),
            SignedPreKeyId = input.SignedPreKeyId, SignedPreKey = input.SignedPreKey,
            SignedPreKeySignature = input.SignedPreKeySignature,
            KyberPreKeyId = input.KyberPreKeyId, KyberPreKey = input.KyberPreKey,
            KyberPreKeySignature = input.KyberPreKeySignature,
            RemainingOneTimePreKeys = remaining, Generation = generation, IdentityReplaced = replaced
        };
    }

    // ---------- RPC 2: state machine ----------

    private async Task<P.E2eeChatStatus> E2eeAction(Transaction work, Session session, Conversation conversation,
        P.E2eeActionRequest input, string domain)
    {
        var uid = session.UserId;
        RequireDirect(conversation);
        if (input.Action == P.E2eeActionKind.E2EeActionUnspecified) throw new ApiException(400, "E2EE_ACTION_INVALID");
        if (input.ProtocolVersion != DirectionalModeVersion) throw new ApiException(400, "E2EE_PROTOCOL_VERSION_UNSUPPORTED");
        var peer = Other(conversation, uid);
        await Lock(work, conversation.Id, uid);
        var record = await Chat(work, conversation.Id, true);
        var boundary = await LastMessageId(work, conversation.Id);
        var now = DateTimeOffset.UtcNow;
        string? service = null;

        switch (input.Action)
        {
            case P.E2eeActionKind.E2EeInvite:
                if (record?.State == StatePending)
                {
                    if (record.InitiatorId == uid) return await MapStatus(work, conversation, uid, domain, record);
                    goto case P.E2eeActionKind.E2EeAccept;
                }
                if (record?.State is StateActive or StateClosing) return await MapStatus(work, conversation, uid, domain, record);
                if (record?.State == StateSuspended) throw new ApiException(409, "E2EE_IDENTITY_CONFIRMATION_REQUIRED");
                if (await Device(work, uid) is null) throw new ApiException(409, "E2EE_DEVICE_MISSING");
                record = new(conversation.Id, StatePending, DirectionalModeVersion, (record?.Epoch ?? 0) + 1,
                    uid, boundary, 0, null, [], 0, now);
                service = "e2ee:one_way_enabled";
                break;
            case P.E2eeActionKind.E2EeAccept:
            {
                if (record?.State is StateActive or StateClosing) return await MapStatus(work, conversation, uid, domain, record);
                if (record is null || record.State != StatePending) throw new ApiException(409, "E2EE_NOT_PENDING");
                if (record.InitiatorId == uid) return await MapStatus(work, conversation, uid, domain, record);
                if (await Device(work, uid) is null) throw new ApiException(409, "E2EE_DEVICE_MISSING");
                if (await Device(work, peer) is null) throw new ApiException(409, "E2EE_PEER_UNSUPPORTED");
                record = record with { State = StateActive, Epoch = record.Epoch + 1, BoundaryMessageId = boundary,
                    IdentityConfirmedBy = conversation.Members, UpdatedAt = now };
                service = "e2ee:activated";
                break;
            }
            case P.E2eeActionKind.E2EeDecline:
                return await MapStatus(work, conversation, uid, domain, record);
            case P.E2eeActionKind.E2EeCancel:
            case P.E2eeActionKind.E2EeCloseRequest:
            case P.E2eeActionKind.E2EeCloseConfirm:
            {
                if (record is null || record.State == StateClosed) return await MapStatus(work, conversation, uid, domain, record);
                if (record.State == StateSuspended) throw new ApiException(409, "E2EE_IDENTITY_CONFIRMATION_REQUIRED");
                if (record.State == StatePending)
                {
                    if (record.InitiatorId != uid) return await MapStatus(work, conversation, uid, domain, record);
                    record = record with { State = StateClosed, CloseBoundaryMessageId = boundary, UpdatedAt = now };
                }
                else
                {
                    record = record with { State = StatePending, InitiatorId = peer, Epoch = record.Epoch + 1,
                        BoundaryMessageId = boundary, CloseRequestedBy = null, IdentityConfirmedBy = [], UpdatedAt = now };
                }
                service = "e2ee:one_way_disabled";
                break;
            }
            case P.E2eeActionKind.E2EeConfirmIdentity:
            {
                if (record is null) return await MapStatus(work, conversation, uid, domain, null);
                // A stale repeat after activation is idempotent and changes nothing.
                if (record.State != StateSuspended) return await MapStatus(work, conversation, uid, domain, record);
                // The confirmation must carry the exact snapshot the user saw. An old
                // request without it is rejected instead of inheriting the current identity.
                var confirmation = input.IdentityConfirmation;
                if (confirmation is null || confirmation.PeerIdentityKey.Length is < 32 or > 4096 ||
                    string.IsNullOrEmpty(confirmation.IdentityVersion) || confirmation.Epoch <= 0)
                    throw new ApiException(400, "E2EE_IDENTITY_CONFIRMATION_REQUIRED");
                var self = await Device(work, uid);
                var peerDevice = await Device(work, peer);
                var currentVersion = IdentityVersion(self, peerDevice);
                var versionMatches = currentVersion.Length != 0 && confirmation.IdentityVersion.Length == currentVersion.Length &&
                    CryptographicOperations.FixedTimeEquals(
                        Encoding.UTF8.GetBytes(confirmation.IdentityVersion), Encoding.UTF8.GetBytes(currentVersion));
                if (!versionMatches || confirmation.Epoch != record.Epoch || peerDevice is null ||
                    confirmation.PeerGeneration != peerDevice.Generation ||
                    !confirmation.PeerIdentityKey.Span.SequenceEqual(peerDevice.IdentityKey))
                {
                    // Never mutate the record: the user confirmed an identity that is no
                    // longer current, so this request must not confirm the replacement.
                    throw new ApiException(409, "E2EE_IDENTITY_CHANGED");
                }
                // Drop confirmations bound to any older identity version. Only versioned
                // entries count while suspended, so a plain user id cannot inherit this one.
                var confirmed = record.IdentityConfirmedBy
                    .Where(entry => entry.EndsWith("|" + currentVersion, StringComparison.Ordinal)).ToList();
                if (!confirmed.Contains(uid + "|" + currentVersion, StringComparer.Ordinal))
                    confirmed.Add(uid + "|" + currentVersion);
                if (!confirmed.SequenceEqual(record.IdentityConfirmedBy, StringComparer.Ordinal))
                    record = record with { IdentityConfirmedBy = confirmed.ToArray(),
                        IdentityGeneration = record.IdentityGeneration + 1, UpdatedAt = now };
                if (AllConfirmedVersion(confirmed.ToArray(), conversation.Members, currentVersion))
                {
                    record = record with { State = StateActive, Epoch = record.Epoch + 1, BoundaryMessageId = boundary,
                        IdentityConfirmedBy = conversation.Members
                            .Select(member => member + "|" + currentVersion).ToArray(), UpdatedAt = now };
                    service = "e2ee:reactivated";
                }
                break;
            }
            default:
                throw new ApiException(400, "E2EE_ACTION_INVALID");
        }

        if (record is not null) await SaveChat(work, record);
        if (service is not null)
            await AddE2eeServiceMessage(work, session, conversation.Id, service, record?.Epoch ?? 0);
        return await MapStatus(work, conversation, uid, domain, record);
    }

    // ---------- RPC 3/4: single and batch status ----------

    private async Task<P.E2eeChatStatusList> E2eeStatusList(Transaction work, string uid, string domain,
        bool includePending, IReadOnlyCollection<string>? onlyConversations)
    {
        var result = new P.E2eeChatStatusList();
        var owned = await Owned(work, uid);
        foreach (var conversation in owned.Where(c => c.Kind == "direct" && c.Members.Distinct().Count() == 2 &&
            c.Members.Contains(uid) && (onlyConversations is null || onlyConversations.Contains(c.Id))))
        {
            var record = await Chat(work, conversation.Id);
            if (record is null) continue;
            if (!includePending && record.State == StatePending) continue;
            result.Chats.Add(await MapStatus(work, conversation, uid, domain, record));
        }
        return result;
    }

    private async Task<P.E2eeChatStatus> MapStatus(Transaction work, Conversation conversation, string uid,
        string domain, E2eeChatRow? record)
    {
        var peer = Other(conversation, uid);
        var peerDevice = await Device(work, peer);
        var selfDevice = await Device(work, uid);
        var tlUserId = (await UserIdentities(work, [peer]))[peer];
        // While suspended, a confirmation only counts for the current two-sided
        // version. An entry bound to an older version must not look confirmed.
        var currentVersion = IdentityVersion(selfDevice, peerDevice);
        var versioned = record?.State == StateSuspended;
        return new P.E2eeChatStatus
        {
            Peer = new() { User = new() { ServerDomain = domain, UserId = peer } },
            ConversationId = conversation.Id,
            State = record is null ? P.E2eeState.E2EeDisabled : StateOf(record.State),
            ProtocolVersion = DirectionalModeVersion,
            Epoch = record?.Epoch ?? 0,
            BoundaryMessageId = record?.BoundaryMessageId ?? 0,
            CloseBoundaryMessageId = record?.CloseBoundaryMessageId ?? 0,
            SelfInitiator = record is not null && record.InitiatorId == uid,
            AwaitingSelf = record switch
            {
                { State: StatePending } => record.InitiatorId != uid,
                { State: StateClosing } => record.CloseRequestedBy != uid,
                { State: StateSuspended } => currentVersion.Length == 0 ||
                    !IsConfirmedVersion(record.IdentityConfirmedBy, uid, currentVersion),
                _ => false
            },
            PeerSupported = peerDevice is not null,
            IdentityChanged = record?.State == StateSuspended,
            InitiatorUserId = record?.InitiatorId ?? "",
            ClosedByUserId = record?.CloseRequestedBy ?? "",
            SelfConfirmedIdentity = record is not null && (versioned
                ? IsConfirmedVersion(record.IdentityConfirmedBy, uid, currentVersion)
                : IsConfirmedBy(record.IdentityConfirmedBy, uid)),
            PeerConfirmedIdentity = record is not null && (versioned
                ? IsConfirmedVersion(record.IdentityConfirmedBy, peer, currentVersion)
                : IsConfirmedBy(record.IdentityConfirmedBy, peer)),
            SelfDeviceId = selfDevice?.DeviceId ?? 0,
            PeerDeviceId = peerDevice?.DeviceId ?? 0,
            PeerIdentityKey = peerDevice is null ? ByteString.Empty : ByteString.CopyFrom(peerDevice.IdentityKey),
            PeerGeneration = peerDevice?.Generation ?? 0,
            UpdatedAtMs = (record?.UpdatedAt ?? DateTimeOffset.UtcNow).ToUnixTimeMilliseconds(),
            TlUserId = tlUserId,
            IdentityVersion = IdentityVersion(selfDevice, peerDevice)
        };
    }

    // ---------- RPC 5: peer bundle + atomic pre-key consumption ----------

    private async Task<P.E2eePeerBundle> PeerBundle(Transaction work, Conversation conversation, string uid)
    {
        RequireDirect(conversation);
        var peer = Other(conversation, uid);
        await Lock(work, peer);
        var device = await Device(work, peer, true) ?? throw new ApiException(409, "E2EE_PEER_UNSUPPORTED");
        var consumed = await work.Rows("""
            UPDATE botgram_backend_v1.e2ee_one_time_pre_keys SET consumed=true
            WHERE user_id=$1 AND key_id=(
                SELECT key_id FROM botgram_backend_v1.e2ee_one_time_pre_keys
                WHERE user_id=$1 AND NOT consumed ORDER BY key_id LIMIT 1 FOR UPDATE SKIP LOCKED)
            RETURNING key_id,public_key
            """, r => (KeyId: r.GetInt32(0), PublicKey: r.GetFieldValue<byte[]>(1)), peer);
        if (consumed.Count != 1) throw new ApiException(409, "E2EE_PREKEYS_EXHAUSTED");
        return new P.E2eePeerBundle
        {
            DeviceId = device.DeviceId, RegistrationId = device.RegistrationId,
            PreKeyId = consumed[0].KeyId, PreKey = ByteString.CopyFrom(consumed[0].PublicKey),
            SignedPreKeyId = device.SignedPreKeyId, SignedPreKey = ByteString.CopyFrom(device.SignedPreKey),
            SignedPreKeySignature = ByteString.CopyFrom(device.SignedPreKeySignature),
            IdentityKey = ByteString.CopyFrom(device.IdentityKey),
            KyberPreKeyId = device.KyberPreKeyId, KyberPreKey = ByteString.CopyFrom(device.KyberPreKey),
            KyberPreKeySignature = ByteString.CopyFrom(device.KyberPreKeySignature),
            Generation = device.Generation
        };
    }

    // ---------- Send validation + service messages ----------

    /// <summary>
    /// Validates directionality and returns the canonical envelope bytes, or null
    /// for an allowed plaintext message. Called before message acceptance in the
    /// same transaction, so a failed request cannot consume keys or commit state.
    /// </summary>
    private async Task<byte[]?> ValidateSend(Transaction work, Session session, Conversation conversation,
        SendMessageCommand command, string domain)
    {
        var uid = session.UserId;
        await Lock(work, conversation.Id);
        var record = await Chat(work, conversation.Id, true);
        if (command.E2eeEnvelope is null)
        {
            if (record is null || record.State == StateClosed) return null;
            if (record.State is StateActive or StateClosing || (record.State == StatePending && record.InitiatorId == uid))
                throw new ApiException(409, "E2EE_PLAINTEXT_FORBIDDEN");
            if (record.State == StateSuspended) throw new ApiException(409, "E2EE_IDENTITY_CONFIRMATION_REQUIRED");
            return null; // Reverse-direction plaintext stays available in one-way mode.
        }
        RequireDirect(conversation);
        if (record is null || record.State is not (StateActive or StateClosing or StatePending))
            throw new ApiException(409, "E2EE_NOT_ACTIVE");
        if (command.Text.Length != 0 || command.Entities is { Length: > 0 })
            throw new ApiException(409, "E2EE_PLAINTEXT_FORBIDDEN");
        P.E2eeEnvelope envelope;
        try { envelope = P.E2eeEnvelope.Parser.ParseFrom(command.E2eeEnvelope); }
        catch (InvalidProtocolBufferException) { throw new ApiException(400, "E2EE_ENVELOPE_INVALID"); }
        if (envelope.ProtocolVersion != EnvelopeVersion) throw new ApiException(400, "E2EE_PROTOCOL_VERSION_UNSUPPORTED");
        if (envelope.MessageType is < 2 or > 4) throw new ApiException(400, "E2EE_MESSAGE_TYPE_INVALID");
        var peer = Other(conversation, uid);
        if (envelope.ConversationId != conversation.Id || envelope.SenderUserId != uid || envelope.RecipientUserId != peer)
            throw new ApiException(409, "E2EE_ENVELOPE_CONTEXT_INVALID");
        var self = await Device(work, uid) ?? throw new ApiException(409, "E2EE_DEVICE_MISSING");
        if (envelope.SenderDeviceId != self.DeviceId) throw new ApiException(409, "E2EE_ENVELOPE_DEVICE_INVALID");
        if (record.State == StatePending)
        {
            if (record.InitiatorId != uid) throw new ApiException(409, "E2EE_NOT_ENABLED_FOR_SENDER");
            if (envelope.MessageType != 4 || envelope.RecipientDeviceId != 0 || envelope.Ciphertext.Length < 28)
                throw new ApiException(400, "E2EE_ONE_WAY_ENVELOPE_REQUIRED");
        }
        else
        {
            if (envelope.MessageType == 4) throw new ApiException(409, "E2EE_ONE_WAY_NOT_ALLOWED");
            var peerDevice = await Device(work, peer) ?? throw new ApiException(409, "E2EE_PEER_UNSUPPORTED");
            if (envelope.RecipientDeviceId != peerDevice.DeviceId) throw new ApiException(409, "E2EE_ENVELOPE_DEVICE_INVALID");
        }
        if (envelope.KeyEpoch != record.Epoch) throw new ApiException(409, "E2EE_EPOCH_INVALID");
        // Compare against the canonical decimal form instead of parsing with a
        // restricted NumberStyles: sfixed64 is a full signed 64-bit value and
        // clients legitimately generate negative random ids. This rejects zero,
        // leading '+', leading zeros, whitespace and any overflow without ever
        // converting through a narrower or floating-point representation.
        if (envelope.RandomId == 0 ||
            !command.Key.Equals("tl:" + envelope.RandomId.ToString(CultureInfo.InvariantCulture), StringComparison.Ordinal))
            throw new ApiException(400, "E2EE_RANDOM_ID_INVALID");
        if (envelope.Ciphertext.Length is < 1 or > MaxCiphertext) throw new ApiException(400, "E2EE_CIPHERTEXT_INVALID");
        return envelope.ToByteArray();
    }

    private async Task SuspendForIdentityChange(Transaction work, Session session, string uid)
    {
        var owned = await Owned(work, uid);
        foreach (var conversation in owned.Where(c => c.Kind == "direct" && c.Members.Distinct().Count() == 2 &&
            c.Members.Contains(uid)))
        {
            var record = await Chat(work, conversation.Id, true);
            if (record is null || record.State is not (StateActive or StateClosing)) continue;
            // Confirmation bindings are versioned snapshots, so every old confirmation
            // must be discarded. identity_generation is now a monotonic revision of
            // suspensions, never a sum of device generations.
            await SaveChat(work, record with { State = StateSuspended, IdentityConfirmedBy = [],
                CloseRequestedBy = null, IdentityGeneration = record.IdentityGeneration + 1,
                UpdatedAt = DateTimeOffset.UtcNow });
            await AddE2eeServiceMessage(work, session, conversation.Id, "e2ee:identity_changed", record.Epoch);
        }
    }

    private async Task AddE2eeServiceMessage(Transaction work, Session session, string conversationId,
        string code, long epoch)
    {
        // Deterministic key: a repeated transition is rejected by the state machine
        // before reaching this point, and a replay cannot create a second message.
        var key = "e2ee:" + code + ":" + epoch.ToString(CultureInfo.InvariantCulture);
        await new PostgresMessageAcceptance(source, maximumBacklog).AddE2eeServiceInTransactionAsync(
            session, conversationId, key, code, work.Connection, work.NativeTransaction,
            work.RetainThroughCommit, work.Token);
    }
}
