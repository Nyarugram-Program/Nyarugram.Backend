using System.Text.Json;
using System.Text.Json.Serialization;

namespace Botgram.Backend;

[JsonConverter(typeof(EventPayloadJsonConverter))]
public interface IEventPayload { }
public sealed record GidChanged(string UserId, string Gid) : IEventPayload;
public sealed record CallSignal(string CallId, string SenderId, string Type, string Data) : IEventPayload;
// Batch A message-lifecycle payloads (schema v10). All are pts stream events.
public sealed record MessageDeleted(string ConversationId, long[] MessageIds) : IEventPayload;
public sealed record MessagePinned(string ConversationId, long[] MessageIds, bool Pinned) : IEventPayload;
public sealed record ContentsRead(string ConversationId, long[] MessageIds) : IEventPayload;
public sealed record StoredDraft(string Text, MessageEntity[]? Entities, long? ReplyTo, DateTimeOffset UpdatedAt);
public sealed record DraftChanged(string ConversationId, StoredDraft? Draft) : IEventPayload;
public sealed record UserTyping(string ConversationId, string UserId, int Action) : IEventPayload;

internal static class EventPayloadTypes
{
    public static Type For(string type) => type switch
    {
        "conversation.created" => typeof(Conversation),
        "message.created" => typeof(Message),
        "message.edited" => typeof(Message),
        "message.deleted" => typeof(MessageDeleted),
        "message.pinned" => typeof(MessagePinned),
        "content.read" => typeof(ContentsRead),
        "draft.changed" => typeof(DraftChanged),
        "user.typing" => typeof(UserTyping),
        "receipt.updated" => typeof(Receipt),
        "call.updated" => typeof(Call),
        "call.signal" => typeof(CallSignal),
        "call.tl_signal" => typeof(CallSignal),
        "user.gid_changed" => typeof(GidChanged),
        "user.presence_changed" => typeof(PresenceChanged),
        "wallpaper.changed" => typeof(WallpaperChanged),
        _ => throw new JsonException("Unknown stored event type: " + type)
    };
}

// The enclosing event type is the legacy snapshot discriminator; no new JSON field is added.
public sealed class ChatEventJsonConverter : JsonConverter<ChatEvent>
{
    public override ChatEvent Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
    {
        using var document = JsonDocument.ParseValue(ref reader);
        var root = document.RootElement;
        var type = root.GetProperty("type").GetString() ?? throw new JsonException("Missing event type");
        var payload = (IEventPayload?)root.GetProperty("payload").Deserialize(EventPayloadTypes.For(type), options)
            ?? throw new JsonException("Missing event payload");
        return new(root.GetProperty("id").GetInt64(), root.GetProperty("recipients").Deserialize<string[]>(options)
            ?? throw new JsonException("Missing event recipients"), type, payload,
            root.GetProperty("createdAt").GetDateTimeOffset(),
            root.TryGetProperty("expiresAt", out var expires) && expires.ValueKind != JsonValueKind.Null ? expires.GetDateTimeOffset() : null);
    }

    public override void Write(Utf8JsonWriter writer, ChatEvent value, JsonSerializerOptions options)
    {
        if (EventPayloadTypes.For(value.Type) != value.Payload.GetType()) throw new JsonException("Event payload does not match event type");
        writer.WriteStartObject();
        writer.WriteNumber("id", value.Id);
        writer.WritePropertyName("recipients");
        JsonSerializer.Serialize(writer, value.Recipients, options);
        writer.WriteString("type", value.Type);
        writer.WritePropertyName("payload");
        JsonSerializer.Serialize(writer, value.Payload, value.Payload.GetType(), options);
        writer.WriteString("createdAt", value.CreatedAt);
        writer.WritePropertyName("expiresAt");
        JsonSerializer.Serialize(writer, value.ExpiresAt, options);
        writer.WriteEndObject();
    }
}

// REST and WebSocket retain their original concrete payload shape as well.
public sealed class EventPayloadJsonConverter : JsonConverter<IEventPayload>
{
    public override IEventPayload Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
        => throw new JsonException("Event payload requires its enclosing event type");
    public override void Write(Utf8JsonWriter writer, IEventPayload value, JsonSerializerOptions options)
        => JsonSerializer.Serialize(writer, value, value.GetType(), options);
}
