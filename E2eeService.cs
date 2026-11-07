using System.Globalization;
using System.Security.Cryptography;
using System.Text;
using Google.Protobuf;
using P = Botgram.Protocol.V1;

namespace Botgram.Backend;

// Server-side pre-key directory and per-chat state machine. This file never sees
// plaintext, private keys, PIN material or unlock keys.
public static class E2eeService
{
    public const uint ProtocolVersion = 1;
    public const uint DirectionalModeVersion = 2;
    private const int MaxCiphertext = 65536;
    private const int MaxOneTimePreKeys = 100;
    private const string StatePending = "pending";
    private const string StateActive = "active";
    private const string StateClosing = "closing";
    private const string StateSuspended = "suspended";
    private const string StateClosed = "closed";

    public static E2eeConversation? Find(Database db, string conversationId) =>
        db.E2eeConversations.FirstOrDefault(c => c.ConversationId == conversationId);

    public static E2eeDevice? Device(Database db, string userId) =>
        db.E2eeDevices.FirstOrDefault(d => d.UserId == userId);

    public static P.E2eeDeviceInfo Publish(Database db, string uid, P.E2eePublishDeviceRequest input)
    {
        if (input.DeviceId is < 1 or > 1)
            throw new ApiException(400, "E2EE_DEVICE_INVALID");
        if (input.RegistrationId is < 1 or > 16383)
            throw new ApiException(400, "E2EE_REGISTRATION_INVALID");
        ValidateKey(input.IdentityKey, "E2EE_IDENTITY_INVALID");
        ValidateKey(input.SignedPreKey, "E2EE_SIGNED_PREKEY_INVALID");
        ValidateSignature(input.SignedPreKeySignature, "E2EE_SIGNED_PREKEY_SIGNATURE_INVALID");
        ValidateKey(input.KyberPreKey, "E2EE_KYBER_PREKEY_INVALID");
        ValidateSignature(input.KyberPreKeySignature, "E2EE_KYBER_PREKEY_SIGNATURE_INVALID");
        if (input.SignedPreKeyId < 1 || input.KyberPreKeyId < 1)
            throw new ApiException(400, "E2EE_PREKEY_ID_INVALID");
        if (input.OneTimePreKeys.Count > MaxOneTimePreKeys)
            throw new ApiException(400, "E2EE_PREKEYS_TOO_MANY");
        var ids = new HashSet<int>();
        foreach (var key in input.OneTimePreKeys)
        {
            if (key.KeyId < 1 || !ids.Add(key.KeyId))
                throw new ApiException(400, "E2EE_PREKEY_ID_INVALID");
            ValidateKey(key.PublicKey, "E2EE_PREKEY_INVALID");
        }

        var existing = Device(db, uid);
        var identity = input.IdentityKey.ToByteArray();
        var replaced = existing is not null && !existing.IdentityKey.AsSpan().SequenceEqual(identity);
        var generation = existing is null ? 1 : replaced ? existing.Generation + 1 : existing.Generation;

        var merged = new Dictionary<int, E2eeOneTimePreKey>();
        if (!replaced && existing is not null)
            foreach (var key in existing.OneTimePreKeys.Where(k => !k.Consumed))
                merged[key.KeyId] = key;
        foreach (var key in input.OneTimePreKeys)
            merged[key.KeyId] = new E2eeOneTimePreKey(key.KeyId, key.PublicKey.ToByteArray(), false);
        var oneTime = merged.Values.OrderBy(k => k.KeyId).Take(MaxOneTimePreKeys).ToArray();

        var device = new E2eeDevice(uid, input.DeviceId, input.RegistrationId, identity,
            input.SignedPreKeyId, input.SignedPreKey.ToByteArray(), input.SignedPreKeySignature.ToByteArray(),
            input.KyberPreKeyId, input.KyberPreKey.ToByteArray(), input.KyberPreKeySignature.ToByteArray(),
            generation, DateTimeOffset.UtcNow, oneTime);
        if (existing is not null) db.E2eeDevices.Remove(existing);
        db.E2eeDevices.Add(device);

        if (replaced) SuspendForIdentityChange(db, uid);
        return new P.E2eeDeviceInfo
        {
            DeviceId = device.DeviceId, RegistrationId = device.RegistrationId,
            IdentityKey = ByteString.CopyFrom(device.IdentityKey),
            SignedPreKeyId = device.SignedPreKeyId, SignedPreKey = ByteString.CopyFrom(device.SignedPreKey),
            SignedPreKeySignature = ByteString.CopyFrom(device.SignedPreKeySignature),
            KyberPreKeyId = device.KyberPreKeyId, KyberPreKey = ByteString.CopyFrom(device.KyberPreKey),
            KyberPreKeySignature = ByteString.CopyFrom(device.KyberPreKeySignature),
            RemainingOneTimePreKeys = oneTime.Length, Generation = device.Generation,
            IdentityReplaced = replaced
        };
    }

    public static P.E2eePeerBundle PeerBundle(Database db, Conversation conversation, string uid)
    {
        RequireDirect(conversation);
        var peer = Other(conversation, uid);
        var device = Device(db, peer) ?? throw new ApiException(409, "E2EE_PEER_UNSUPPORTED");
        var index = Array.FindIndex(device.OneTimePreKeys, k => !k.Consumed);
        if (index < 0) throw new ApiException(409, "E2EE_PREKEYS_EXHAUSTED");
        var selected = device.OneTimePreKeys[index];
        var updated = device.OneTimePreKeys.ToArray();
        updated[index] = selected with { Consumed = true };
        db.E2eeDevices[db.E2eeDevices.IndexOf(device)] = device with { OneTimePreKeys = updated };
        return new P.E2eePeerBundle
        {
            DeviceId = device.DeviceId, RegistrationId = device.RegistrationId,
            PreKeyId = selected.KeyId, PreKey = ByteString.CopyFrom(selected.PublicKey),
            SignedPreKeyId = device.SignedPreKeyId, SignedPreKey = ByteString.CopyFrom(device.SignedPreKey),
            SignedPreKeySignature = ByteString.CopyFrom(device.SignedPreKeySignature),
            IdentityKey = ByteString.CopyFrom(device.IdentityKey),
            KyberPreKeyId = device.KyberPreKeyId, KyberPreKey = ByteString.CopyFrom(device.KyberPreKey),
            KyberPreKeySignature = ByteString.CopyFrom(device.KyberPreKeySignature),
            Generation = device.Generation
        };
    }

    public static P.E2eeChatStatus Action(Database db, Conversation conversation, string uid,
        P.E2eeActionRequest input, string domain)
    {
        RequireDirect(conversation);
        if (input.Action == P.E2eeActionKind.E2EeActionUnspecified)
            throw new ApiException(400, "E2EE_ACTION_INVALID");
        if (input.ProtocolVersion != DirectionalModeVersion)
            throw new ApiException(400, "E2EE_PROTOCOL_VERSION_UNSUPPORTED");
        var peer = Other(conversation, uid);
        var record = Find(db, conversation.Id);
        switch (input.Action)
        {
            case P.E2eeActionKind.E2EeInvite:
            {
                if (record?.State == StatePending)
                {
                    if (record.InitiatorId == uid) return MapStatus(db, conversation, uid, domain, record);
                    goto case P.E2eeActionKind.E2EeAccept;
                }
                if (record?.State is StateActive or StateClosing) return MapStatus(db, conversation, uid, domain, record);
                if (record?.State == StateSuspended) throw new ApiException(409, "E2EE_IDENTITY_CONFIRMATION_REQUIRED");
                if (Device(db, uid) is null) throw new ApiException(409, "E2EE_DEVICE_MISSING");
                record = new E2eeConversation(conversation.Id, StatePending, DirectionalModeVersion, (record?.Epoch ?? 0) + 1,
                    uid, LastMessageId(db, conversation.Id), 0, null, [], DateTimeOffset.UtcNow);
                Replace(db, record);
                ConversationMessageService.AddE2eeServiceMessage(db, conversation, uid, "e2ee:one_way_enabled");
                return MapStatus(db, conversation, uid, domain, record);
            }
            case P.E2eeActionKind.E2EeAccept:
            {
                if (record?.State is StateActive or StateClosing) return MapStatus(db, conversation, uid, domain, record);
                if (record is null || record.State != StatePending) throw new ApiException(409, "E2EE_NOT_PENDING");
                if (record.InitiatorId == uid) return MapStatus(db, conversation, uid, domain, record);
                RequirePeerSupported(db, uid, peer);
                record = record with { State = StateActive, Epoch = record.Epoch + 1,
                    BoundaryMessageId = LastMessageId(db, conversation.Id), IdentityConfirmedBy = conversation.Members,
                    UpdatedAt = DateTimeOffset.UtcNow };
                Replace(db, record);
                ConversationMessageService.AddE2eeServiceMessage(db, conversation, uid, "e2ee:activated");
                return MapStatus(db, conversation, uid, domain, record);
            }
            case P.E2eeActionKind.E2EeDecline:
                return MapStatus(db, conversation, uid, domain, record);
            case P.E2eeActionKind.E2EeCancel:
            case P.E2eeActionKind.E2EeCloseRequest:
            case P.E2eeActionKind.E2EeCloseConfirm:
            {
                if (record is null || record.State == StateClosed) return MapStatus(db, conversation, uid, domain, record);
                if (record.State == StateSuspended) throw new ApiException(409, "E2EE_IDENTITY_CONFIRMATION_REQUIRED");
                if (record.State == StatePending)
                {
                    if (record.InitiatorId != uid) return MapStatus(db, conversation, uid, domain, record);
                    record = record with { State = StateClosed, CloseBoundaryMessageId = LastMessageId(db, conversation.Id),
                        UpdatedAt = DateTimeOffset.UtcNow };
                }
                else
                {
                    record = record with { State = StatePending, InitiatorId = peer, Epoch = record.Epoch + 1,
                        BoundaryMessageId = LastMessageId(db, conversation.Id), CloseRequestedBy = null,
                        IdentityConfirmedBy = [], UpdatedAt = DateTimeOffset.UtcNow };
                }
                Replace(db, record);
                ConversationMessageService.AddE2eeServiceMessage(db, conversation, uid, "e2ee:one_way_disabled");
                return MapStatus(db, conversation, uid, domain, record);
            }
            case P.E2eeActionKind.E2EeConfirmIdentity:
            {
                if (record is null) return Disabled(db, conversation, uid, domain);
                // A stale repeat after activation is idempotent and changes nothing.
                if (record.State != StateSuspended) return MapStatus(db, conversation, uid, domain, record);
                // The legacy file store follows the same binding contract as the
                // relational path: the request must carry the exact identity the
                // user saw, otherwise it is rejected without touching the record.
                var confirmation = input.IdentityConfirmation;
                if (confirmation is null || confirmation.PeerIdentityKey.Length is < 32 or > 4096 ||
                    string.IsNullOrEmpty(confirmation.IdentityVersion) || confirmation.Epoch <= 0)
                    throw new ApiException(400, "E2EE_IDENTITY_CONFIRMATION_REQUIRED");
                var self = Device(db, uid);
                var peerDevice = Device(db, peer);
                var currentVersion = IdentityVersion(self, peerDevice);
                var versionMatches = currentVersion.Length != 0 && confirmation.IdentityVersion.Length == currentVersion.Length &&
                    CryptographicOperations.FixedTimeEquals(
                        Encoding.UTF8.GetBytes(confirmation.IdentityVersion), Encoding.UTF8.GetBytes(currentVersion));
                if (!versionMatches || confirmation.Epoch != record.Epoch || peerDevice is null ||
                    confirmation.PeerGeneration != peerDevice.Generation ||
                    !confirmation.PeerIdentityKey.Span.SequenceEqual(peerDevice.IdentityKey))
                    throw new ApiException(409, "E2EE_IDENTITY_CHANGED");
                var confirmed = record.IdentityConfirmedBy
                    .Where(entry => entry.EndsWith("|" + currentVersion, StringComparison.Ordinal)).ToList();
                if (!confirmed.Contains(uid + "|" + currentVersion, StringComparer.Ordinal))
                    confirmed.Add(uid + "|" + currentVersion);
                if (!confirmed.SequenceEqual(record.IdentityConfirmedBy, StringComparer.Ordinal))
                    record = record with
                    {
                        IdentityConfirmedBy = confirmed.ToArray(),
                        IdentityGeneration = record.IdentityGeneration + 1,
                        UpdatedAt = DateTimeOffset.UtcNow
                    };
                if (AllConfirmedVersion(confirmed.ToArray(), conversation.Members, currentVersion))
                {
                    record = record with
                    {
                        State = StateActive, Epoch = record.Epoch + 1,
                        BoundaryMessageId = LastMessageId(db, conversation.Id),
                        IdentityConfirmedBy = conversation.Members
                            .Select(member => member + "|" + currentVersion).ToArray(),
                        UpdatedAt = DateTimeOffset.UtcNow
                    };
                    ConversationMessageService.AddE2eeServiceMessage(db, conversation, uid, "e2ee:reactivated");
                }
                Replace(db, record);
                return MapStatus(db, conversation, uid, domain, record);
            }
            default:
                throw new ApiException(400, "E2EE_ACTION_INVALID");
        }
    }

    public static byte[]? ValidateSend(Database db, Conversation conversation, string uid, SendMessageCommand command)
    {
        var record = Find(db, conversation.Id);
        if (command.E2eeEnvelope is null)
        {
            if (record is null || record.State == StateClosed) return null;
            if (record.State is StateActive or StateClosing || (record.State == StatePending && record.InitiatorId == uid))
                throw new ApiException(409, "E2EE_PLAINTEXT_FORBIDDEN");
            if (record.State == StateSuspended)
                throw new ApiException(409, "E2EE_IDENTITY_CONFIRMATION_REQUIRED");
            return null;
        }
        RequireDirect(conversation);
        if (record is null || record.State is not (StateActive or StateClosing or StatePending))
            throw new ApiException(409, "E2EE_NOT_ACTIVE");
        if (command.Text.Length != 0 || command.Entities is { Length: > 0 })
            throw new ApiException(409, "E2EE_PLAINTEXT_FORBIDDEN");
        P.E2eeEnvelope envelope;
        try
        {
            envelope = P.E2eeEnvelope.Parser.ParseFrom(command.E2eeEnvelope);
        }
        catch (InvalidProtocolBufferException)
        {
            throw new ApiException(400, "E2EE_ENVELOPE_INVALID");
        }
        if (envelope.ProtocolVersion != ProtocolVersion)
            throw new ApiException(400, "E2EE_PROTOCOL_VERSION_UNSUPPORTED");
        if (envelope.MessageType is < 2 or > 4)
            throw new ApiException(400, "E2EE_MESSAGE_TYPE_INVALID");
        if (envelope.ConversationId != conversation.Id || envelope.SenderUserId != uid ||
            envelope.RecipientUserId != Other(conversation, uid))
            throw new ApiException(409, "E2EE_ENVELOPE_CONTEXT_INVALID");
        var self = Device(db, uid) ?? throw new ApiException(409, "E2EE_DEVICE_MISSING");
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
            var peer = Device(db, Other(conversation, uid)) ?? throw new ApiException(409, "E2EE_PEER_UNSUPPORTED");
            if (envelope.RecipientDeviceId != peer.DeviceId) throw new ApiException(409, "E2EE_ENVELOPE_DEVICE_INVALID");
        }
        if (envelope.KeyEpoch != record.Epoch)
            throw new ApiException(409, "E2EE_EPOCH_INVALID");
        // Full signed 64-bit random ids are legal; compare the canonical decimal form
        // so '+', leading zeros, whitespace, overflow and zero cannot slip through.
        if (envelope.RandomId == 0 ||
            !command.Key.Equals("tl:" + envelope.RandomId.ToString(CultureInfo.InvariantCulture), StringComparison.Ordinal))
            throw new ApiException(400, "E2EE_RANDOM_ID_INVALID");
        if (envelope.Ciphertext.Length is < 1 or > MaxCiphertext)
            throw new ApiException(400, "E2EE_CIPHERTEXT_INVALID");
        return envelope.ToByteArray();
    }

    public static P.E2eeChatStatusList StatusList(Database db, string uid, string domain, bool includePending,
        IReadOnlyCollection<string>? onlyConversations)
    {
        var result = new P.E2eeChatStatusList();
        var conversations = db.Conversations.Where(c => c.Kind == "direct" && c.Members.Contains(uid) &&
            (onlyConversations is null || onlyConversations.Contains(c.Id)));
        foreach (var conversation in conversations)
        {
            var record = Find(db, conversation.Id);
            if (record is null) continue;
            if (!includePending && record.State == StatePending) continue;
            result.Chats.Add(MapStatus(db, conversation, uid, domain, record));
        }
        return result;
    }

    public static P.E2eeChatStatus MapStatus(Database db, Conversation conversation, string uid,
        string domain, E2eeConversation? record = null)
    {
        record ??= Find(db, conversation.Id);
        var peer = Other(conversation, uid);
        var peerDevice = Device(db, peer);
        var selfDevice = Device(db, uid);
        var currentVersion = IdentityVersion(selfDevice, peerDevice);
        var versioned = record?.State == StateSuspended;
        var result = new P.E2eeChatStatus
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
            TlUserId = IdentityService.UserId(db, peer),
            IdentityVersion = IdentityVersion(selfDevice, peerDevice)
        };
        return result;
    }

    /// <summary>
    /// Canonical version of both devices' identities, ordered by user id so both
    /// participants observe the same value. Never derives binding from a sum of
    /// generations; the hash covers the actual keys and generations.
    /// </summary>
    private static string IdentityVersion(E2eeDevice? self, E2eeDevice? peer)
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

    private static P.E2eeChatStatus Disabled(Database db, Conversation conversation, string uid, string domain) =>
        MapStatus(db, conversation, uid, domain, null);

    private static void SuspendForIdentityChange(Database db, string uid)
    {
        foreach (var conversation in db.Conversations.Where(c => c.Kind == "direct" && c.Members.Contains(uid)).ToArray())
        {
            var record = Find(db, conversation.Id);
            if (record is null || record.State is not (StateActive or StateClosing)) continue;
            Replace(db, record with
            {
                State = StateSuspended, IdentityConfirmedBy = [], CloseRequestedBy = null,
                // Versioned confirmations make every old binding stale; the counter is
                // now a monotonic suspension revision for diagnostics, not a key.
                IdentityGeneration = record.IdentityGeneration + 1,
                UpdatedAt = DateTimeOffset.UtcNow
            });
            ConversationMessageService.AddE2eeServiceMessage(db, conversation, uid, "e2ee:identity_changed");
        }
    }

    private static void Replace(Database db, E2eeConversation record)
    {
        var index = db.E2eeConversations.FindIndex(c => c.ConversationId == record.ConversationId);
        if (index < 0) db.E2eeConversations.Add(record);
        else db.E2eeConversations[index] = record;
    }

    private static void RequireDirect(Conversation conversation)
    {
        if (conversation.Kind != "direct" || conversation.Members.Distinct().Count() != 2)
            throw new ApiException(409, "E2EE_TARGET_INVALID");
    }

    private static string Other(Conversation conversation, string uid) =>
        conversation.Members.Single(m => m != uid);

    private static void RequirePeerSupported(Database db, string uid, string peer)
    {
        if (Device(db, uid) is null) throw new ApiException(409, "E2EE_DEVICE_MISSING");
        if (Device(db, peer) is null) throw new ApiException(409, "E2EE_PEER_UNSUPPORTED");
    }

    private static long LastMessageId(Database db, string conversationId) =>
        db.Messages.Where(m => m.ConversationId == conversationId).Select(m => m.Id).DefaultIfEmpty(0).Max();

    private static P.E2eeState StateOf(string state) => state switch
    {
        StatePending => P.E2eeState.E2EePending,
        StateActive => P.E2eeState.E2EeActive,
        StateClosing => P.E2eeState.E2EeClosing,
        StateSuspended => P.E2eeState.E2EeSuspended,
        StateClosed => P.E2eeState.E2EeClosed,
        _ => P.E2eeState.E2EeDisabled
    };

    private static void ValidateKey(ByteString key, string code)
    {
        if (key.Length is < 32 or > 4096) throw new ApiException(400, code);
    }

    private static void ValidateSignature(ByteString signature, string code)
    {
        if (signature.Length is < 16 or > 256) throw new ApiException(400, code);
    }
}

