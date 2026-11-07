using System.Net.WebSockets;
using Google.Protobuf;

namespace Botgram.Backend.Storage;

// Isolated PG-authority validation host. It does not claim Kafka/Scylla/Redis readiness.
public static class RelationalHttpRoutes
{
    private static Session Session(HttpContext context) => (Session)context.Items["session"]!;

    public static Task<Session> RequireAsync(HttpContext context)
    {
        var header = context.Request.Headers.Authorization.ToString();
        if (!header.StartsWith("Bearer ", StringComparison.OrdinalIgnoreCase)) throw new ApiException(401, "unauthorized");
        return context.RequestServices.GetRequiredService<PostgresBackend>().RequireAsync(Auth.HashToken(header[7..]), context.RequestAborted);
    }

    public static void Map(WebApplication app, IConfiguration configuration, ProtocolPolicy policy)
    {
        // /health and both Protobuf authentication planes are registered once in
        // Program.cs before this map. All remaining JSON REST routes below are
        // unreachable: JsonApiRetirement.Guard rejects every retired path first.
        app.MapPost("/api/auth/register", (RegisterInput input, PostgresBackend backend, HttpContext c) =>
            backend.RegisterAsync(input, c.RequestAborted)).RequireRateLimiting("auth");
        app.MapPost("/api/auth/login", (LoginInput input, PostgresBackend backend, HttpContext c) =>
            backend.LoginAsync(input, c.RequestAborted)).RequireRateLimiting("auth");
        app.MapPost("/api/auth/password", (LoginInput input, PostgresBackend backend, HttpContext c) =>
            backend.LoginPasswordAsync(input, c.RequestAborted)).RequireRateLimiting("auth");
        var api = app.MapGroup("/api").RequireRateLimiting("api");
        api.MapGet("/security/totp", (HttpContext c, PostgresBackend backend) =>
            backend.TotpAsync(Session(c), "status", null, c.RequestAborted));
        api.MapPost("/security/totp/{operation}", (string operation, TotpInput input, HttpContext c, PostgresBackend backend) =>
            backend.TotpAsync(Session(c), operation, input.Code, c.RequestAborted));
        api.MapPost("/v1/rpc", ProtobufRpcEndpoint.HandleBackend);
        api.MapPost("/telegram/rpc", () => Results.Json(new { error = "BOTGRAM_JSON_RPC_DISABLED" }, statusCode: 410));
        api.MapGet("/protocol/capabilities", () => new
        {
            negotiationVersion = 1, protocols = new[] { "protobuf-v1" }, selectedProtocol = "protobuf-v1",
            eventSubprotocols = new[] { "botgram.event-hint.v1.protobuf" },
            capabilities = new[] { "session-protocol-pinning", "send-random-id-idempotency-v1", "difference-pts-v1",
                "wss-wake-v1", "nofile-wallpaper-revision-v1" }
        });
        api.MapGet("/gid/regions", () => Gids.Regions.Where(r => r.Code != "001"));
        api.MapGet("/telegram/me", async (PostgresBackend backend, HttpContext c) =>
            TelegramGateway.User(await backend.CurrentProfileAsync(Session(c), c.RequestAborted)));
        api.MapPut("/me/gid", (ChangeGidInput input, PostgresBackend backend, HttpContext c) =>
            backend.ChangeGidAsync(Session(c), input, TelegramGateway.User, c.RequestAborted));
        api.MapPost("/auth/logout", async (PostgresBackend backend, HttpContext c) =>
        {
            await backend.LogoutAsync(Session(c), c.RequestAborted);
            return Results.NoContent();
        });
        api.MapGet("/me", (PostgresBackend backend, HttpContext c) => backend.MeAsync(Session(c), c.RequestAborted));
        api.MapGet("/me/premium", (PostgresBackend backend, HttpContext c) => backend.PremiumAsync(Session(c), token: c.RequestAborted));
        api.MapPost("/me/premium/activate", (PostgresBackend backend, HttpContext c) =>
            backend.PremiumAsync(Session(c), activate: true, token: c.RequestAborted));
        api.MapGet("/users", (string? username, PostgresBackend backend, HttpContext c) =>
            backend.SearchUsersAsync(Session(c), username, c.RequestAborted));
        api.MapPost("/conversations", (ConversationInput input, PostgresBackend backend, HttpContext c) =>
            backend.CreateConversationAsync(Session(c), input, c.RequestAborted));
        api.MapGet("/conversations", (PostgresBackend backend, HttpContext c) =>
            backend.ConversationListAsync(Session(c), c.RequestAborted));
        api.MapGet("/conversations/{id}/messages", (string id, long? before, int? limit, PostgresBackend backend, HttpContext c) =>
            backend.HistoryAsync(Session(c), id, before, limit, c.RequestAborted));
        api.MapPost("/conversations/{id}/messages", (string id, MessageInput input, PostgresBackend backend, HttpContext c) =>
            backend.SendRestAsync(Session(c), id, input, message => message, c.RequestAborted));
        api.MapPost("/conversations/{id}/read", (string id, ReadInput input, PostgresBackend backend, HttpContext c) =>
            backend.ReadExactAsync(Session(c), id, input.MessageId, c.RequestAborted));
        api.MapGet("/events", (long? after, int? limit, PostgresBackend backend, HttpContext c) =>
            backend.EventsAsync(Session(c), after ?? 0, limit ?? 100, c.RequestAborted));
        api.MapGet("/ws", (long? after, PostgresBackend backend, HttpContext c) => Socket(after, backend, c, policy));
        api.MapGet("/rtc/config", () => new
        {
            iceServers = configuration.GetSection("Rtc:IceServers").GetChildren().Select(server => new
            {
                urls = server.GetSection("Urls").Get<string[]>() ?? [],
                username = server["Username"], credential = server["Credential"]
            }).ToArray()
        });
        api.MapPost("/calls", (CallInput input, PostgresBackend backend, HttpContext c) =>
            backend.CreateCallAsync(Session(c), input, c.RequestAborted));
        api.MapGet("/calls", (PostgresBackend backend, HttpContext c) => backend.CallsAsync(Session(c), c.RequestAborted));
        api.MapGet("/calls/{id}", (string id, PostgresBackend backend, HttpContext c) => backend.CallAsync(Session(c), id, c.RequestAborted));
        api.MapPost("/calls/{id}/{action}", (string id, string action, PostgresBackend backend, HttpContext c) =>
            backend.ChangeCallAsync(Session(c), id, action, c.RequestAborted));
        api.MapPost("/calls/{id}/signals", async (string id, SignalInput input, PostgresBackend backend, HttpContext c) =>
        {
            await backend.SignalAsync(Session(c), id, input, c.RequestAborted);
            return Results.Accepted();
        });
    }

    private static async Task Socket(long? after, PostgresBackend backend, HttpContext c, ProtocolPolicy policy)
    {
        if (after < 0) throw new ApiException(400, "invalid_cursor");
        if (!c.WebSockets.IsWebSocketRequest) { c.Response.StatusCode = 400; return; }
        const string protocol = "botgram.event-hint.v1.protobuf";
        var offered = c.WebSockets.WebSocketRequestedProtocols;
        if (!offered.Contains(protocol))
        {
            var legacy = offered.Count == 0 || offered.Contains("botgram.events.v1.json");
            c.Items["protocol_usage_selected"] = legacy ? "json" : "unknown";
            c.Items[legacy ? "protocol_usage_disabled" : "protocol_usage_unsupported"] = true;
            throw new ApiException(legacy ? 410 : 400,
                legacy ? JsonApiRetirement.Code : "unsupported_event_subprotocol");
        }
        // The unified retirement gate runs before this handler; legacy switches
        // can neither re-enable JSON WSS nor break the Protobuf event channel.
        var session = Session(c);
        // Reauthorize before upgrading, then again on each bounded query.
        Botgram.Protocol.V1.EventHint? first = await backend.EventHintAsync(session, after ?? 0, c.RequestAborted);
        using var socket = await c.WebSockets.AcceptWebSocketAsync(protocol);
        var usage = c.RequestServices.GetRequiredService<ProtocolUsage>();
        c.Items["protocol_usage_selected"] = "protobuf";
        usage.AcceptedWebSocket(c, true);
        using var cancellation = CancellationTokenSource.CreateLinkedTokenSource(c.RequestAborted);
        var token = cancellation.Token;
        var receiveFailure = 0;
        async Task Receive()
        {
            try
            {
                var buffer = new byte[1024];
                while (!token.IsCancellationRequested)
                {
                    var message = await socket.ReceiveAsync(buffer.AsMemory(), token);
                    if (message.MessageType == WebSocketMessageType.Close) break;
                    if (message.Count != 0) { Volatile.Write(ref receiveFailure, 2); break; }
                }
            }
            catch (OperationCanceledException) when (token.IsCancellationRequested) { }
            catch (WebSocketException) { Volatile.Write(ref receiveFailure, 1); }
            finally { await cancellation.CancelAsync(); }
        }
        var receive = Receive();
        var outcome = "closed";
        try
        {
            long cursor = after ?? 0;
            long? revision = null;
            while (!token.IsCancellationRequested && socket.State == WebSocketState.Open)
            {
                var hint = first ?? await backend.EventHintAsync(session, cursor, token);
                first = null;
                if (hint.Cursor != cursor || revision != hint.WallpaperRevision)
                {
                    using var timeout = CancellationTokenSource.CreateLinkedTokenSource(token);
                    timeout.CancelAfter(TimeSpan.FromSeconds(10));
                    await socket.SendAsync(hint.ToByteArray(), WebSocketMessageType.Binary, true, timeout.Token);
                    cursor = hint.Cursor;
                    revision = hint.WallpaperRevision;
                }
                await Task.Delay(500, token);
            }
        }
        catch (ApiException exception) when (exception.Status == 401) { outcome = "session_expired"; }
        catch (OperationCanceledException) { outcome = token.IsCancellationRequested ? "cancelled" : "send_timeout"; }
        catch (WebSocketException) { outcome = "transport_error"; }
        catch (Npgsql.NpgsqlException exception)
        {
            outcome = "storage_error";
            c.RequestServices.GetRequiredService<ILogger<PostgresBackend>>()
                .LogError("Relational WebSocket storage failure, type={Type}, transient={Transient}", exception.GetType().Name, exception.IsTransient);
        }
        finally
        {
            var failure = Volatile.Read(ref receiveFailure);
            if (outcome is "closed" or "cancelled" && failure != 0)
                outcome = failure == 2 ? "client_payload_rejected" : "transport_error";
            usage.EndedWebSocket(true, outcome);
            if (socket.State is WebSocketState.Open or WebSocketState.CloseReceived)
            {
                using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(2));
                try { await socket.CloseOutputAsync(WebSocketCloseStatus.NormalClosure, outcome, timeout.Token); }
                catch (OperationCanceledException) { }
                catch (WebSocketException) { }
            }
            // Cancelling a pending ReceiveAsync aborts the socket. Send the close
            // frame first so revocation reaches the client with its reason.
            await cancellation.CancelAsync();
            await receive;
        }
    }
}

public sealed class RelationalMaintenanceService(PostgresBackend backend, ILogger<RelationalMaintenanceService> logger) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        using var timer = new PeriodicTimer(TimeSpan.FromSeconds(5));
        while (await timer.WaitForNextTickAsync(stoppingToken))
        {
            try { await backend.MaintainAsync(stoppingToken); }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested) { break; }
            catch (ApiException exception) when (exception.Status == 503)
            { logger.LogWarning("Relational maintenance delayed by durable backlog capacity"); }
            catch (Npgsql.NpgsqlException exception) when (exception.IsTransient)
            { logger.LogWarning("Relational maintenance transient storage failure, type={Type}", exception.GetType().Name); }
        }
    }
}
