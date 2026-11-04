using System.Text.Json;
using System.Text.Json.Serialization;

namespace Botgram.Backend;

public enum MessageEntityKind
{
    Bold, Italic, Underline, Strike, Spoiler, Code, Pre, Url, TextUrl, Email, Phone,
    Mention, Hashtag, Cashtag, BotCommand
}

[JsonConverter(typeof(MessageEntityJsonConverter))]
public readonly record struct MessageEntity(MessageEntityKind Kind, int Offset, int Length, string Language = "", string Url = "");

public static class MessageEntityService
{
    public static void ValidateCount(int count)
    {
        if (count > 100) throw new ApiException(400, "ENTITIES_TOO_LONG");
    }

    public static void ValidateKind(MessageEntityKind kind)
    {
        if (!Enum.IsDefined(kind)) throw new ApiException(501, "BOTGRAM_ENTITY_UNSUPPORTED");
    }

    public static void ValidateBounds(string text, int offset, int length)
    {
        if (offset < 0 || length <= 0 || length > text.Length || offset > text.Length - length)
            throw new ApiException(400, "ENTITY_BOUNDS_INVALID");
        int end = offset + length;
        if ((offset > 0 && char.IsLowSurrogate(text[offset]) && char.IsHighSurrogate(text[offset - 1])) ||
            (end < text.Length && char.IsLowSurrogate(text[end]) && char.IsHighSurrogate(text[end - 1])))
            throw new ApiException(400, "ENTITY_BOUNDS_INVALID");
    }

    public static MessageEntity Validate(string text, MessageEntity entity)
    {
        ValidateKind(entity.Kind);
        ValidateBounds(text, entity.Offset, entity.Length);
        if (entity.Kind == MessageEntityKind.TextUrl && entity.Url.Length is 0 or > 2048)
            throw new ApiException(400, "ENTITY_URL_INVALID");
        return entity with
        {
            Language = entity.Kind == MessageEntityKind.Pre ? entity.Language : "",
            Url = entity.Kind == MessageEntityKind.TextUrl ? entity.Url : ""
        };
    }

    public static bool Equal(MessageEntity[]? left, MessageEntity[]? right)
        => left is null ? right is null : right is not null && left.AsSpan().SequenceEqual(right);
}

public sealed class MessageEntityJsonConverter : JsonConverter<MessageEntity>
{
    private const string Prefix = "TL_messageEntity";

    public static MessageEntityKind ParseKind(string type)
    {
        string name = type.Split('$').Last();
        if (name.StartsWith(Prefix, StringComparison.Ordinal) &&
            Enum.TryParse<MessageEntityKind>(name[Prefix.Length..], out var kind) &&
            Enum.IsDefined(kind) && name == Prefix + kind)
            return kind;
        return (MessageEntityKind)(-1);
    }

    public override MessageEntity Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
    {
        using var document = JsonDocument.ParseValue(ref reader);
        var value = document.RootElement;
        var kind = ParseKind(value.GetProperty("$type").GetString() ?? "");
        if (!Enum.IsDefined(kind)) throw new JsonException("Unsupported stored message entity");
        return new(kind, value.GetProperty("offset").GetInt32(), value.GetProperty("length").GetInt32(),
            kind == MessageEntityKind.Pre && value.TryGetProperty("language", out var language) ? language.GetString() ?? "" : "",
            kind == MessageEntityKind.TextUrl && value.TryGetProperty("url", out var url) ? url.GetString() ?? "" : "");
    }

    public override void Write(Utf8JsonWriter writer, MessageEntity value, JsonSerializerOptions options)
    {
        if (!Enum.IsDefined(value.Kind)) throw new JsonException("Unsupported stored message entity");
        writer.WriteStartObject();
        writer.WriteString("$type", "org.telegram.tgnet.TLRPC$" + Prefix + value.Kind);
        writer.WriteNumber("offset", value.Offset);
        writer.WriteNumber("length", value.Length);
        if (value.Kind == MessageEntityKind.Pre) writer.WriteString("language", value.Language);
        if (value.Kind == MessageEntityKind.TextUrl) writer.WriteString("url", value.Url);
        writer.WriteEndObject();
    }
}
