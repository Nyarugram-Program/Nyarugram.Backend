using System.Globalization;
using System.Text;
using System.Text.Json;
using System.Text.RegularExpressions;
using StackExchange.Redis;

namespace Botgram.Backend.Storage;

// Transport seam for deterministic cache-only fault tests; never an authority for any business state.
public interface IMessageCacheTransport
{
    Task<string?> GetAsync(string key, CancellationToken cancellationToken);
    Task SetAsync(string key, string payload, TimeSpan ttl, CancellationToken cancellationToken);
}

public sealed class RedisMessageCacheTransport(IDatabase database) : IMessageCacheTransport
{
    public async Task<string?> GetAsync(string key, CancellationToken cancellationToken)
    {
        var value = await database.StringGetAsync(key).WaitAsync(cancellationToken);
        return value.IsNull ? null : value.ToString();
    }

    public async Task SetAsync(string key, string payload, TimeSpan ttl, CancellationToken cancellationToken)
    {
        if (!await database.StringSetAsync(key, payload, ttl).WaitAsync(cancellationToken))
            throw new IOException("Message cache write was not accepted");
    }
}

// Only immutable, committed PG Message payloads belong here. No sessions, deduplication, pts or null entries.
// The caller owns the multiplexer. This adapter is intentionally not registered in production.
public sealed class RedisMessageCache
{
    public static readonly TimeSpan MaximumTtl = TimeSpan.FromMinutes(5);
    public static readonly TimeSpan MaximumOperationTimeout = TimeSpan.FromSeconds(2);
    private readonly IMessageCacheTransport transport;
    private readonly string prefix;
    private readonly TimeSpan ttl;
    private readonly TimeSpan operationTimeout;

    public RedisMessageCache(IMessageCacheTransport transport, string environment, TimeSpan ttl,
        TimeSpan? operationTimeout = null)
    {
        ArgumentNullException.ThrowIfNull(transport);
        if (environment is null || !Regex.IsMatch(environment, "\\A[a-z0-9][a-z0-9_-]{0,63}\\z", RegexOptions.CultureInvariant))
            throw new ArgumentException("An explicit lowercase cache environment is required", nameof(environment));
        if (ttl <= TimeSpan.Zero || ttl > MaximumTtl || ttl.TotalMilliseconds < 1)
            throw new ArgumentOutOfRangeException(nameof(ttl));
        var timeout = operationTimeout ?? TimeSpan.FromMilliseconds(250);
        if (timeout < TimeSpan.FromMilliseconds(1) || timeout > MaximumOperationTimeout)
            throw new ArgumentOutOfRangeException(nameof(operationTimeout));
        this.transport = transport;
        prefix = $"botgram:v1:{environment}:message:";
        this.ttl = ttl;
        this.operationTimeout = timeout;
    }

    // authorize must query authoritative session/membership state on EVERY call, including cache hits.
    // readCommitted must read PG after successful commit, never an uncommitted/prepared response.
    // Neither callback's failures are caught, retried or converted into successful cache misses.
    public async Task<Message> ReadThroughAsync(Guid generation, string conversationId, long messageId,
        Func<CancellationToken, Task> authorize,
        Func<CancellationToken, Task<Message>> readCommitted,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(authorize);
        ArgumentNullException.ThrowIfNull(readCommitted);
        var key = Key(generation, conversationId, messageId);
        cancellationToken.ThrowIfCancellationRequested();
        await authorize(cancellationToken);
        cancellationToken.ThrowIfCancellationRequested();
        string? payload = null;
        try
        {
            payload = await transport.GetAsync(key, cancellationToken)
                .WaitAsync(operationTimeout, cancellationToken);
        }
        catch (Exception exception) when (IsCacheFailure(exception)) { }
        cancellationToken.ThrowIfCancellationRequested();
        if (payload is not null)
        {
            try
            {
                var entry = JsonSerializer.Deserialize<CacheEntry>(payload);
                if (entry is not null && entry.Format == 1 && entry.Key == key &&
                    ValidIdentity(entry.Message, conversationId, messageId))
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    return entry.Message;
                }
            }
            catch (Exception exception) when (exception is JsonException or NotSupportedException or
                InvalidOperationException or KeyNotFoundException or FormatException or OverflowException) { }
        }

        var message = await readCommitted(cancellationToken);
        cancellationToken.ThrowIfCancellationRequested();
        if (!ValidIdentity(message, conversationId, messageId))
            throw new InvalidDataException("Authoritative message is missing or does not match the requested identity");
        // Message is a record but contains arrays: never expose the authoritative instance's mutable arrays.
        var snapshot = message with
        {
            ServiceMembers = message.ServiceMembers?.ToArray(),
            TelegramEntities = message.TelegramEntities?.ToArray()
        };
        try
        {
            var serialized = JsonSerializer.Serialize(new CacheEntry(1, key, snapshot));
            await transport.SetAsync(key, serialized, ttl, cancellationToken)
                .WaitAsync(operationTimeout, cancellationToken);
        }
        catch (Exception exception) when (IsCacheFailure(exception) || exception is JsonException or NotSupportedException) { }
        cancellationToken.ThrowIfCancellationRequested();
        return snapshot;
    }

    private string Key(Guid generation, string conversationId, long messageId)
    {
        if (generation == Guid.Empty) throw new ArgumentException("A cache generation is required", nameof(generation));
        ArgumentException.ThrowIfNullOrEmpty(conversationId);
        if (messageId <= 0) throw new ArgumentOutOfRangeException(nameof(messageId));
        // Hex encoding preserves exact string identity and prevents delimiter collisions.
        return prefix + generation.ToString("N") + ":" + Convert.ToHexString(Encoding.UTF8.GetBytes(conversationId)) +
            ":" + messageId.ToString(CultureInfo.InvariantCulture);
    }

    private static bool ValidIdentity(Message? message, string conversationId, long messageId) =>
        message is not null && message.Id == messageId && message.ConversationId == conversationId &&
        message.SenderId is not null && message.ClientMessageId is not null && message.Text is not null;

    private static bool IsCacheFailure(Exception exception) =>
        exception is RedisException or TimeoutException or IOException or ObjectDisposedException;

    private sealed record CacheEntry(int Format, string Key, Message Message);
}
