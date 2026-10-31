using System.Net.WebSockets;
using System.Security.Cryptography;
using System.Text.Json;
using System.Text.RegularExpressions;
using System.Threading.RateLimiting;
using Botgram.Backend;
using Microsoft.AspNetCore.RateLimiting;

var builder = WebApplication.CreateBuilder(args);
builder.WebHost.ConfigureKestrel(o => o.Limits.MaxRequestBodySize = 128 * 1024);
builder.Services.AddSingleton<Store>();
builder.Services.AddHostedService<CallExpiryService>();
builder.Services.AddRateLimiter(options =>
{
    options.RejectionStatusCode = 429;
    options.AddPolicy("auth", context => RateLimitPartition.GetFixedWindowLimiter(
        context.Connection.RemoteIpAddress?.ToString() ?? "unknown", _ => new FixedWindowRateLimiterOptions
        { PermitLimit = 15, Window = TimeSpan.FromMinutes(1), QueueLimit = 0 }));
    options.AddPolicy("api", context => RateLimitPartition.GetFixedWindowLimiter(
        context.Items["uid"]?.ToString() ?? context.Connection.RemoteIpAddress?.ToString() ?? "unknown",
        _ => new FixedWindowRateLimiterOptions { PermitLimit = 300, Window = TimeSpan.FromMinutes(1), QueueLimit = 0 }));
});
var app = builder.Build();
// Fail at startup if the database is invalid or already in use.
_ = app.Services.GetRequiredService<Store>();
app.Use(async (context, next) =>
{
    try { await next(context); }
    catch (ApiException ex)
    {
        context.Response.StatusCode = ex.Status;
        await context.Response.WriteAsJsonAsync(new { error = ex.Message });
    }
    catch (BadHttpRequestException)
    {
        context.Response.StatusCode = 400;
        await context.Response.WriteAsJsonAsync(new { error = "invalid_request" });
    }
});
app.UseWebSockets(new WebSocketOptions { KeepAliveInterval = TimeSpan.FromSeconds(20) });
app.UseRouting();
app.Use(async (context, next) =>
{
    var path = context.Request.Path;
    if (path.StartsWithSegments("/api") && path != "/api/auth/register" && path != "/api/auth/login")
    {
        var session = Auth.Require(context, context.RequestServices.GetRequiredService<Store>());
        context.Items["uid"] = session.UserId;
        context.Items["session"] = session;
    }
    await next(context);
});
app.UseRateLimiter();
string Uid(HttpContext context) => (string)context.Items["uid"]!;
void Check(bool valid, string code) { if (!valid) throw new ApiException(400, code); }

app.MapGet("/health", () => Results.Ok(new { status = "ok", protocol = "botgram-v1" }));
app.MapPost("/api/auth/register", (RegisterInput input, Store store) =>
{
    Check(input.Username is not null && Regex.IsMatch(input.Username, "^[a-zA-Z0-9_]{3,32}$"), "invalid_username");
    Check(!string.IsNullOrWhiteSpace(input.DisplayName) && input.DisplayName.Length <= 80, "invalid_display_name");
    Check(input.Password is { Length: >= 10 and <= 256 }, "password_length_10_to_256");
    var username = input.Username!.ToLowerInvariant();
    var salt = Convert.ToHexString(RandomNumberGenerator.GetBytes(16));
    var user = new User(Guid.NewGuid().ToString("N"), username, input.DisplayName.Trim(), salt, Auth.HashPassword(input.Password!, salt));
    return store.Write(db =>
    {
        if (db.Users.Any(u => u.Username == username)) throw new ApiException(409, "username_taken");
        user = user with { Gid = Gids.Allocate(db, Gids.FromCountry(input.LocationCountry)) };
        db.Users.Add(user);
        return Auth.CreateSession(db, user);
    });
}).RequireRateLimiting("auth");
app.MapPost("/api/auth/login", (LoginInput input, Store store) =>
{
    Check(input.Username is { Length: <= 32 } && input.Password is { Length: <= 256 }, "invalid_credentials");
    var user = store.Read(db => db.Users.FirstOrDefault(u => u.Username == input.Username!.ToLowerInvariant()));
    // Perform the same password derivation even when the username is absent.
    var hash = Auth.HashPassword(input.Password!, user?.Salt ?? new string('0', 32));
    if (user is null || !CryptographicOperations.FixedTimeEquals(Convert.FromHexString(hash), Convert.FromHexString(user.PasswordHash)))
        throw new ApiException(401, "invalid_credentials");
    return store.Write(db => Auth.CreateSession(db, user));
}).RequireRateLimiting("auth");

var api = app.MapGroup("/api").RequireRateLimiting("api");
api.MapGet("/telegram/me", (HttpContext c, Store store) => store.Write(db => TelegramGateway.CurrentUser(db, Uid(c))));
api.MapGet("/gid/regions", () => Gids.Regions.Where(r => r.Code != "001"));
api.MapPut("/me/gid", (ChangeGidInput input, HttpContext c, Store store) => store.Write(db =>
{
    Gids.Change(db, Uid(c), input);
    return TelegramGateway.CurrentUser(db, Uid(c));
}));
api.MapPost("/telegram/rpc", (System.Text.Json.Nodes.JsonObject input, HttpContext c, Store store) =>
    store.Write(db => TelegramGateway.Execute(db, Uid(c), input)));
api.MapPost("/auth/logout", (HttpContext c, Store store) => store.Write(db =>
{
    db.Sessions.RemoveAll(s => s.TokenHash == ((Session)c.Items["session"]!).TokenHash);
    return Results.NoContent();
}));
api.MapGet("/me", (HttpContext c, Store store) => store.Read(db => Auth.PublicUser(db.Users.Single(u => u.Id == Uid(c)))));
api.MapGet("/users", (string? username, Store store) =>
{
    Check(username is { Length: >= 3 and <= 32 }, "exact_username_required");
    return store.Read(db => db.Users.Where(u => u.Username == username!.ToLowerInvariant()).Select(Auth.PublicUser).ToArray());
});
api.MapPost("/conversations", (ConversationInput input, HttpContext c, Store store) =>
{
    Check(input.Kind is "direct" or "group", "invalid_kind");
    Check(input.MemberIds is { Length: >= 1 and <= 99 } && input.MemberIds.All(id => !string.IsNullOrWhiteSpace(id)), "invalid_members");
    var members = input.MemberIds!.Append(Uid(c)).Distinct().Order().ToArray();
    Check(input.Kind == "direct" ? members.Length == 2 : members.Length >= 2, "invalid_members");
    Check(input.Kind != "group" || (!string.IsNullOrWhiteSpace(input.Title) && input.Title.Length <= 100), "invalid_title");
    return store.Write(db =>
    {
        Check(members.All(id => db.Users.Any(u => u.Id == id)), "unknown_member");
        if (input.Kind == "direct")
        {
            var existing = db.Conversations.FirstOrDefault(x => x.Kind == "direct" && x.Members.SequenceEqual(members));
            if (existing is not null) return existing;
        }
        var conversation = new Conversation(Guid.NewGuid().ToString("N"), input.Kind, input.Kind == "group" ? input.Title!.Trim() : "", members, DateTimeOffset.UtcNow);
        db.Conversations.Add(conversation);
        db.Emit(members, "conversation.created", conversation);
        return conversation;
    });
});
api.MapGet("/conversations", (HttpContext c, Store store) => store.Read(db =>
    db.Conversations.Where(x => x.Members.Contains(Uid(c))).Select(x => new
    {
        conversation = x,
        lastMessage = db.Messages.LastOrDefault(m => m.ConversationId == x.Id),
        unreadCount = db.Messages.Count(m => m.ConversationId == x.Id && m.SenderId != Uid(c) &&
            m.Id > (db.Receipts.FirstOrDefault(r => r.ConversationId == x.Id && r.UserId == Uid(c))?.MessageId ?? 0)),
        receipts = db.Receipts.Where(r => r.ConversationId == x.Id).ToArray(),
        users = db.Users.Where(u => x.Members.Contains(u.Id)).Select(Auth.PublicUser).ToArray()
    }).ToArray()));
api.MapGet("/conversations/{id}/messages", (string id, long? before, int? limit, HttpContext c, Store store) =>
{
    Check(before is null or > 0 && limit is null or >= 1 and <= 100, "invalid_pagination");
    return store.Read(db =>
    {
        Auth.Conversation(db, id, Uid(c));
        return db.Messages.Where(m => m.ConversationId == id && (before is null || m.Id < before))
            .OrderByDescending(m => m.Id).Take(limit ?? 50).Reverse().ToArray();
    });
});
api.MapPost("/conversations/{id}/messages", (string id, MessageInput input, HttpContext c, Store store) =>
{
    Check(!string.IsNullOrWhiteSpace(input.Text) && input.Text.Length <= 16000, "invalid_text");
    Check(input.ClientMessageId is { Length: >= 1 and <= 80 }, "invalid_client_message_id");
    return store.Write(db =>
    {
        var conversation = Auth.Conversation(db, id, Uid(c));
        var existing = db.Messages.FirstOrDefault(m => m.ConversationId == id && m.SenderId == Uid(c) && m.ClientMessageId == input.ClientMessageId);
        if (existing is not null)
        {
            if (existing.Text != input.Text) throw new ApiException(409, "idempotency_conflict");
            return existing;
        }
        var message = new Message(db.NextMessageId++, id, Uid(c), input.ClientMessageId, input.Text, DateTimeOffset.UtcNow);
        db.Messages.Add(message);
        db.Emit(conversation.Members, "message.created", message);
        return message;
    });
});
api.MapPost("/conversations/{id}/read", (string id, ReadInput input, HttpContext c, Store store) => store.Write(db =>
{
    var conversation = Auth.Conversation(db, id, Uid(c));
    Check(db.Messages.Any(m => m.Id == input.MessageId && m.ConversationId == id), "message_not_found");
    var previous = db.Receipts.FirstOrDefault(r => r.ConversationId == id && r.UserId == Uid(c));
    if (previous is not null && previous.MessageId >= input.MessageId) return previous;
    if (previous is not null) db.Receipts.Remove(previous);
    var receipt = new Receipt(id, Uid(c), input.MessageId);
    db.Receipts.Add(receipt);
    db.Emit(conversation.Members, "receipt.updated", receipt);
    return receipt;
}));

object Events(Store store, string uid, long after, int limit) => store.Read(db =>
{
    var events = db.Events.Where(e => e.Id > after && e.Recipients.Contains(uid) &&
        (e.ExpiresAt is null || e.ExpiresAt > DateTimeOffset.UtcNow)).Take(limit).ToArray();
    // Advance across invisible and expired events only after the visible page is exhausted.
    var cursor = events.Length == limit ? events[^1].Id : Math.Max(after, db.NextEventId - 1);
    return new { events = events.Select(e => new { e.Id, e.Type, e.Payload, e.CreatedAt }).ToArray(), cursor };
});
api.MapGet("/events", (long? after, int? limit, HttpContext c, Store store) =>
{
    Check(after is null or >= 0 && limit is null or >= 1 and <= 200, "invalid_pagination");
    return Events(store, Uid(c), after ?? 0, limit ?? 100);
});
api.MapGet("/ws", async (long? after, HttpContext c, Store store) =>
{
    Check(after is null or >= 0, "invalid_cursor");
    if (!c.WebSockets.IsWebSocketRequest) { c.Response.StatusCode = 400; return; }
    using var socket = await c.WebSockets.AcceptWebSocketAsync();
    using var cancellation = CancellationTokenSource.CreateLinkedTokenSource(c.RequestAborted);
    var token = cancellation.Token;
    var session = (Session)c.Items["session"]!;
    async Task Receive()
    {
        try
        {
            var buffer = new byte[1024];
            while (!token.IsCancellationRequested)
            {
                var received = await socket.ReceiveAsync(buffer.AsMemory(), token);
                if (received.MessageType == WebSocketMessageType.Close) break;
                // Client operations use HTTP. Do not accept arbitrary socket payloads.
                if (received.Count > 0) break;
            }
        }
        catch (OperationCanceledException) { }
        catch (WebSocketException) { }
        finally { await cancellation.CancelAsync(); }
    }
    var receive = Receive();
    try
    {
        long cursor = after ?? 0;
        while (!token.IsCancellationRequested && socket.State == WebSocketState.Open)
        {
            if (!store.Read(db => db.Sessions.Any(s => s.TokenHash == session.TokenHash && s.ExpiresAt > DateTimeOffset.UtcNow))) break;
            var batch = JsonSerializer.SerializeToElement(Events(store, Uid(c), cursor, 100), Store.Json);
            var next = batch.GetProperty("cursor").GetInt64();
            if (next != cursor || batch.GetProperty("events").GetArrayLength() > 0)
            {
                await socket.SendAsync(JsonSerializer.SerializeToUtf8Bytes(batch), WebSocketMessageType.Text, true, token);
                cursor = next;
            }
            await Task.Delay(500, token);
        }
    }
    catch (OperationCanceledException) { }
    catch (WebSocketException) { }
    finally
    {
        await cancellation.CancelAsync();
        await receive;
        if (socket.State is WebSocketState.Open or WebSocketState.CloseReceived)
        {
            using var closeTimeout = new CancellationTokenSource(TimeSpan.FromSeconds(2));
            try { await socket.CloseOutputAsync(WebSocketCloseStatus.NormalClosure, "closed", closeTimeout.Token); }
            catch (OperationCanceledException) { }
            catch (WebSocketException) { }
        }
    }
});

api.MapGet("/rtc/config", (IConfiguration config) => new
{
    iceServers = config.GetSection("Rtc:IceServers").GetChildren().Select(server => new
    {
        urls = server.GetSection("Urls").Get<string[]>() ?? [],
        username = server["Username"], credential = server["Credential"]
    }).ToArray()
});
api.MapPost("/calls", (CallInput input, HttpContext c, Store store) => store.Write(db =>
{
    var conversation = Auth.Conversation(db, input.ConversationId, Uid(c));
    Check(conversation.Kind == "direct" && input.CalleeId != Uid(c) && conversation.Members.Contains(input.CalleeId), "invalid_callee");
    if (db.Calls.Any(call => call.State is "ringing" or "active" &&
        (call.CallerId == Uid(c) || call.CalleeId == Uid(c) || call.CallerId == input.CalleeId || call.CalleeId == input.CalleeId)))
        throw new ApiException(409, "user_busy");
    var call = new Call(Guid.NewGuid().ToString("N"), conversation.Id, Uid(c), input.CalleeId, "ringing", DateTimeOffset.UtcNow);
    db.Calls.Add(call);
    db.Emit(conversation.Members, "call.updated", call);
    return call;
}));
api.MapGet("/calls", (HttpContext c, Store store) => store.Write(db => db.Calls
    .Where(call => (call.CallerId == Uid(c) || call.CalleeId == Uid(c)) && call.State is "ringing" or "active")
    .ToArray()));
api.MapGet("/calls/{id}", (string id, HttpContext c, Store store) => store.Write(db => Auth.Call(db, id, Uid(c))));
api.MapPost("/calls/{id}/{action}", (string id, string action, HttpContext c, Store store) => store.Write(db =>
{
    var call = Auth.Call(db, id, Uid(c));
    Check(action is "accept" or "reject" or "end", "invalid_action");
    if (action is "accept" or "reject")
    {
        if (call.CalleeId != Uid(c)) throw new ApiException(403, "callee_only");
        if (call.State != "ringing") throw new ApiException(409, "call_not_ringing");
    }
    if (call.State is "ended" or "rejected") return call;
    var updated = call with { State = action == "accept" ? "active" : action == "reject" ? "rejected" : "ended", UpdatedAt = DateTimeOffset.UtcNow };
    db.Calls[db.Calls.IndexOf(call)] = updated;
    db.Emit([call.CallerId, call.CalleeId], "call.updated", updated);
    return updated;
}));
api.MapPost("/calls/{id}/signals", (string id, SignalInput input, HttpContext c, Store store) => store.Write(db =>
{
    var call = Auth.Call(db, id, Uid(c));
    if (call.State != "active") throw new ApiException(409, "call_not_active");
    Check(input.Type is "offer" or "answer" or "ice", "invalid_signal_type");
    Check(!string.IsNullOrWhiteSpace(input.Data) && input.Data.Length <= 64000, "invalid_signal_data");
    Check(input.Type != "offer" || call.CallerId == Uid(c), "caller_offers_only");
    Check(input.Type != "answer" || call.CalleeId == Uid(c), "callee_answers_only");
    var recipient = call.CallerId == Uid(c) ? call.CalleeId : call.CallerId;
    db.Emit([recipient], "call.signal", new { callId = id, senderId = Uid(c), input.Type, input.Data }, DateTimeOffset.UtcNow.AddMinutes(5));
    return Results.Accepted();
}));
app.Run();
