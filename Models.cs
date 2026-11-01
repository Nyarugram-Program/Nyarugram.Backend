namespace Botgram.Backend;

public sealed record User(string Id, string Username, string DisplayName, string Salt, string PasswordHash, string Gid = "");
public sealed record Session(string TokenHash, string UserId, DateTimeOffset ExpiresAt);
public sealed record Conversation(string Id, string Kind, string Title, string[] Members, DateTimeOffset CreatedAt);
public sealed record Message(long Id, string ConversationId, string SenderId, string ClientMessageId, string Text, DateTimeOffset CreatedAt,
    string? ServiceTitle = null, string[]? ServiceMembers = null, System.Text.Json.JsonElement? TelegramEntities = null,
    long? ReplyToMessageId = null, bool Silent = false);
public sealed record Receipt(string ConversationId, string UserId, long MessageId);
public sealed record Contact(string OwnerId, string UserId, string FirstName, string LastName);
public sealed record Call(string Id, string ConversationId, string CallerId, string CalleeId, string State, DateTimeOffset UpdatedAt);
public sealed record ChatEvent(long Id, string[] Recipients, string Type, object Payload, DateTimeOffset CreatedAt, DateTimeOffset? ExpiresAt = null);
public sealed class Database
{
    public List<User> Users { get; set; } = [];
    public List<Session> Sessions { get; set; } = [];
    public List<Conversation> Conversations { get; set; } = [];
    public List<Message> Messages { get; set; } = [];
    public List<Receipt> Receipts { get; set; } = [];
    public List<Contact> Contacts { get; set; } = [];
    public List<Call> Calls { get; set; } = [];
    public List<ChatEvent> Events { get; set; } = [];
    public long NextMessageId { get; set; } = 1;
    public long NextEventId { get; set; } = 1;
    // Stable Telegram-compatible numeric identities live on the server, never in UI adapters.
    public Dictionary<string, long> TelegramIds { get; set; } = [];
    public long NextTelegramId { get; set; } = 100_000;
    public long TelegramId(string key)
    {
        if (!TelegramIds.TryGetValue(key, out var id)) TelegramIds[key] = id = NextTelegramId++;
        return id;
    }
    public void Emit(string[] recipients, string type, object payload, DateTimeOffset? expiresAt = null)
        => Events.Add(new(NextEventId++, recipients, type, payload, DateTimeOffset.UtcNow, expiresAt));
}
public sealed record RegisterInput(string Username, string DisplayName, string Password, string? LocationCountry = null);
public sealed record ChangeGidInput(string RegionCode, string Number);
public sealed record LoginInput(string Username, string Password);
public sealed record ConversationInput(string Kind, string? Title, string[] MemberIds);
public sealed record MessageInput(string ClientMessageId, string Text);
public sealed record ReadInput(long MessageId);
public sealed record CallInput(string ConversationId, string CalleeId);
public sealed record SignalInput(string Type, string Data);
public sealed class ApiException(int status, string code) : Exception(code)
{
    public int Status { get; } = status;
}
