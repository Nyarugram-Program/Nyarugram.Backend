namespace Botgram.Backend;

public sealed record User(string Id, string Username, string DisplayName, string Salt, string PasswordHash, string Gid = "", bool Premium = false,
    DateTimeOffset? LastSeen = null, bool Online = false, DateTimeOffset? PresenceNotifiedAt = null);
public sealed record Session(string TokenHash, string UserId, DateTimeOffset ExpiresAt, DateTimeOffset? ForegroundAt = null);
public sealed record Conversation(string Id, string Kind, string Title, string[] Members, DateTimeOffset CreatedAt) : IEventPayload;
public sealed record Message(long Id, string ConversationId, string SenderId, string ClientMessageId, string Text, DateTimeOffset CreatedAt,
    string? ServiceTitle = null, string[]? ServiceMembers = null, MessageEntity[]? TelegramEntities = null,
    long? ReplyToMessageId = null, bool Silent = false, byte[]? E2eeEnvelope = null,
    string? ServiceKind = null) : IEventPayload;
// Public pre-key material only. Private keys, PINs and plaintext never enter the database.
public sealed record E2eeOneTimePreKey(int KeyId, byte[] PublicKey, bool Consumed);
public sealed record E2eeDevice(string UserId, uint DeviceId, int RegistrationId, byte[] IdentityKey,
    int SignedPreKeyId, byte[] SignedPreKey, byte[] SignedPreKeySignature,
    int KyberPreKeyId, byte[] KyberPreKey, byte[] KyberPreKeySignature,
    long Generation, DateTimeOffset UpdatedAt, E2eeOneTimePreKey[] OneTimePreKeys);
// state: pending, active, closing, suspended, closed. Absence means disabled.
public sealed record E2eeConversation(string ConversationId, string State, uint ProtocolVersion, long Epoch,
    string InitiatorId, long BoundaryMessageId, long CloseBoundaryMessageId, string? CloseRequestedBy,
    string[] IdentityConfirmedBy, DateTimeOffset UpdatedAt,
    // Relational only: device generation observed when an identity change suspended
    // the chat. Legacy snapshots omit it and default to zero.
    long IdentityGeneration = 0);
public sealed record Receipt(string ConversationId, string UserId, long MessageId) : IEventPayload;
public sealed record Contact(string OwnerId, string UserId, string FirstName, string LastName);
public sealed record Call(string Id, string ConversationId, string CallerId, string CalleeId, string State, DateTimeOffset UpdatedAt,
    long TelegramId = 0, long AccessHash = 0, long RandomId = 0, bool Video = false,
    System.Text.Json.Nodes.JsonObject? Protocol = null,
    byte[]? GAHash = null, byte[]? GB = null, byte[]? GA = null, long KeyFingerprint = 0,
    int Date = 0, int ReceiveDate = 0, int StartDate = 0, string Reason = "Hangup", int Duration = 0,
    System.Text.Json.Nodes.JsonObject[]? Connections = null, bool P2pAllowed = false,
    System.Text.Json.Nodes.JsonObject? RequestProtocol = null,
    System.Text.Json.Nodes.JsonObject? AcceptProtocol = null) : IEventPayload;
[System.Text.Json.Serialization.JsonConverter(typeof(ChatEventJsonConverter))]
public sealed record ChatEvent(long Id, string[] Recipients, string Type, IEventPayload Payload, DateTimeOffset CreatedAt, DateTimeOffset? ExpiresAt = null);
public sealed class Database
{
    public List<User> Users { get; set; } = [];
    public List<Session> Sessions { get; set; } = [];
    public List<Conversation> Conversations { get; set; } = [];
    public List<Message> Messages { get; set; } = [];
    public List<Receipt> Receipts { get; set; } = [];
    public List<Contact> Contacts { get; set; } = [];
    [System.Text.Json.Serialization.JsonIgnore(Condition = System.Text.Json.Serialization.JsonIgnoreCondition.WhenWritingNull)]
    public Dictionary<string, AccountPrivacyState>? AccountPrivacy { get; set; }
    public List<Call> Calls { get; set; } = [];
    public List<E2eeDevice> E2eeDevices { get; set; } = [];
    public List<E2eeConversation> E2eeConversations { get; set; } = [];
    public List<ChatEvent> Events { get; set; } = [];
    public long NextMessageId { get; set; } = 1;
    public long NextEventId { get; set; } = 1;
    public long NextCallId { get; set; } = 0;
    // Stable Telegram-compatible numeric identities live on the server, never in UI adapters.
    public Dictionary<string, long> TelegramIds { get; set; } = [];
    public long NextTelegramId { get; set; } = 100_000;
    public long TelegramId(string key)
    {
        if (!TelegramIds.TryGetValue(key, out var id)) TelegramIds[key] = id = NextTelegramId++;
        return id;
    }
    public void Emit(string[] recipients, string type, IEventPayload payload, DateTimeOffset? expiresAt = null)
    {
        if (EventPayloadTypes.For(type) != payload.GetType()) throw new InvalidDataException("Event payload does not match event type");
        Events.Add(new(NextEventId++, recipients, type, payload, DateTimeOffset.UtcNow, expiresAt));
    }
}
public sealed record RegisterInput(string Username, string DisplayName, string Password, string? LocationCountry = null);
public sealed record ChangeGidInput(string RegionCode, string Number);
public sealed record LoginInput(string Username, string Password, string? PasswordProof = null, string? TotpCode = null);
public sealed record ConversationInput(string Kind, string? Title, string[] MemberIds);
public sealed record MessageInput(string ClientMessageId, string Text);
public sealed record ReadInput(long MessageId);
public sealed record CallInput(string ConversationId, string CalleeId);
public sealed record SignalInput(string Type, string Data);
public sealed class ApiException(int status, string code) : Exception(code)
{
    public int Status { get; } = status;
}
