using System.Collections.Concurrent;
using System.Text;

namespace Botgram.Backend;

/// <summary>
/// Single ingress gate for the retired JSON/HTTP API. It runs before authentication,
/// so unauthenticated, authenticated, malformed and replayed requests all receive the
/// same explicit non-JSON 410 response. The response body is a small key/value text
/// document (never JSON) so older clients can still surface the code reliably.
///
/// HTTP/WS encoding is intentionally independent from JSON file persistence and from
/// existing PostgreSQL JSONB columns: this gate only touches the wire protocol.
/// </summary>
public static class JsonApiRetirement
{
    public const string Code = "BOTGRAM_JSON_API_RETIRED";
    public const string RetiredHeader = "X-Botgram-Json-Api-Retired";
    public const string ProtobufSubprotocol = "botgram.event-hint.v1.protobuf";
    public const string JsonSubprotocol = "botgram.events.v1.json";
    public const string ProtobufMediaType = "application/x-protobuf";

    // Protobuf-only endpoints. Everything else under /api is retired.
    private static readonly HashSet<string> ProtobufEndpoints = new(StringComparer.OrdinalIgnoreCase)
    {
        "/api/v1/rpc", "/api/v1/auth", "/api/v1/account"
    };

    private const string Body =
        "BOTGRAM_JSON_API_RETIRED\n" +
        "status=410\n" +
        "reason=json-http-api-retired\n" +
        "replacement=application/x-protobuf\n" +
        "authentication=/api/v1/auth\n" +
        "account=/api/v1/account\n" +
        "rpc=/api/v1/rpc\n" +
        "capabilities=/api/v1/auth#auth_capabilities\n";

    public static string Normalize(string? raw)
    {
        if (string.IsNullOrEmpty(raw)) return "/";
        var parts = raw.Split('/', StringSplitOptions.RemoveEmptyEntries);
        return parts.Length == 0 ? "/" : "/" + string.Join('/', parts);
    }

    private static bool IsProtobufEndpoint(string path) => ProtobufEndpoints.Contains(path);

    public static bool IsRetiredPath(string path) =>
        path.Equals("/api", StringComparison.OrdinalIgnoreCase) ||
        path.StartsWith("/api/", StringComparison.OrdinalIgnoreCase) && !IsProtobufEndpoint(path);

    private static Task Write(HttpContext context, int status, string code, string body)
    {
        context.Response.StatusCode = status;
        context.Response.ContentType = "text/plain; charset=utf-8";
        context.Response.Headers[RetiredHeader] = code;
        context.Response.Headers["Deprecation"] = "true";
        context.Response.Headers["Sunset"] = "Thu, 01 Jan 2026 00:00:00 GMT";
        context.Response.Headers.CacheControl = "no-store";
        context.Response.Headers["X-Botgram-Deprecated"] = "true";
        context.Response.ContentLength = Encoding.UTF8.GetByteCount(body);
        return context.Response.WriteAsync(body, context.RequestAborted);
    }

    public static Task Reject(HttpContext context) => Write(context, 410, Code, Body);

    /// <summary>
    /// Rejects a Protobuf endpoint call that tries to smuggle a JSON body. Never
    /// sniffs the payload and never falls back to a JSON handler.
    /// </summary>
    public static Task RejectMediaType(HttpContext context) => Write(context, 415,
        "BOTGRAM_MEDIA_TYPE_UNSUPPORTED", "BOTGRAM_MEDIA_TYPE_UNSUPPORTED\nstatus=415\nrequired=application/x-protobuf\n");

    /// <summary>The health probe speaks the same non-JSON key/value text format.</summary>
    public static Task Health(HttpContext context, IEnumerable<KeyValuePair<string, string>> fields, int status = 200)
    {
        var text = new StringBuilder(status == 200
            ? "status=ok\nprotocol=botgram-v1\njson_api=BOTGRAM_JSON_API_RETIRED\n"
            : "status=degraded\nprotocol=botgram-v1\njson_api=BOTGRAM_JSON_API_RETIRED\n");
        foreach (var (key, value) in fields) text.Append(key).Append('=').Append(value).Append('\n');
        context.Response.StatusCode = status;
        context.Response.ContentType = "text/plain; charset=utf-8";
        context.Response.Headers.CacheControl = "no-store";
        var body = text.ToString();
        context.Response.ContentLength = Encoding.UTF8.GetByteCount(body);
        return context.Response.WriteAsync(body, context.RequestAborted);
    }

    public static async Task Guard(HttpContext context, Func<Task> next)
    {
        var path = Normalize(context.Request.Path.Value);
        var method = context.Request.Method;

        if (path.Equals("/health", StringComparison.OrdinalIgnoreCase))
        {
            await next();
            return;
        }

        if (path.Equals("/api/ws", StringComparison.OrdinalIgnoreCase))
        {
            // Reject before the WebSocket upgrade. This gate runs ahead of
            // UseWebSockets(), so read the raw upgrade headers directly rather
            // than IHttpWebSocketFeature (which is not populated yet).
            var upgrade = string.Equals(context.Request.Headers.Upgrade.ToString(), "websocket",
                StringComparison.OrdinalIgnoreCase);
            var offered = context.Request.Headers["Sec-WebSocket-Protocol"]
                .SelectMany(value => (value ?? "").Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries))
                .ToArray();
            var jsonOffered = offered.Any(p => string.Equals(p, JsonSubprotocol, StringComparison.OrdinalIgnoreCase));
            var protobufOffered = offered.Any(p => string.Equals(p, ProtobufSubprotocol, StringComparison.OrdinalIgnoreCase));
            if (!upgrade || jsonOffered || !protobufOffered)
            {
                await Reject(context);
                return;
            }
            await next();
            return;
        }

        if (IsProtobufEndpoint(path))
        {
            if (!string.Equals(method, "POST", StringComparison.OrdinalIgnoreCase))
            {
                await Reject(context);
                return;
            }
            var contentType = context.Request.ContentType?.Split(';')[0].Trim();
            if (!string.Equals(contentType, ProtobufMediaType, StringComparison.OrdinalIgnoreCase))
            {
                await RejectMediaType(context);
                return;
            }
            await next();
            return;
        }

        // Any other /api path is a retired JSON endpoint, regardless of method,
        // credentials, content type or legacy configuration switches.
        if (IsRetiredPath(path))
        {
            await Reject(context);
            return;
        }
        await next();
    }
}

/// <summary>
/// Authentication-plane limiter. Registration/login/recovery used a 15/min
/// "auth" policy on the retired REST paths; the Protobuf auth endpoint keeps the
/// same bound without depending on the old route registration.
/// </summary>
public static class AuthPlaneRateLimit
{
    private sealed class Window { public int Count; public DateTimeOffset Reset; }
    private static readonly ConcurrentDictionary<string, Window> Windows = new();

    public static void Check(string key, int limit = 15, int windowSeconds = 60)
    {
        var now = DateTimeOffset.UtcNow;
        if (Windows.Count > 10000)
            foreach (var (name, entry) in Windows)
                if (entry.Reset <= now) Windows.TryRemove(name, out _);
        var window = Windows.GetOrAdd(key, _ => new Window());
        lock (window)
        {
            if (window.Reset <= now) { window.Reset = now.AddSeconds(windowSeconds); window.Count = 0; }
            if (++window.Count > limit) throw new ApiException(429, "BOTGRAM_AUTH_RATE_LIMITED");
        }
    }
}
