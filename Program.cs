using Google.Protobuf;
using System.Net;
using System.Net.WebSockets;
using System.Security.Cryptography;
using System.Text.Json;
using System.Text.RegularExpressions;
using System.Threading.RateLimiting;
using Botgram.Backend;
using Microsoft.AspNetCore.HttpOverrides;
using Microsoft.AspNetCore.RateLimiting;
using Microsoft.AspNetCore.DataProtection;

var builder = WebApplication.CreateBuilder(args);
// Validate before resolving Store or creating any legacy data/lease files.
var storagePolicy = StoragePolicy.Load(builder.Configuration, builder.Environment);
CallService.Configure(builder.Configuration);
builder.Services.AddSingleton(storagePolicy);
builder.WebHost.ConfigureKestrel(o => o.Limits.MaxRequestBodySize = 128 * 1024);
var knownProxies = builder.Configuration.GetSection("ReverseProxy:KnownProxies").Get<string[]>() ?? [];
if (knownProxies.Length > 0)
{
    var proxyAddresses = knownProxies.Select(IPAddress.Parse).ToArray();
    builder.Services.Configure<ForwardedHeadersOptions>(options =>
    {
        options.ForwardedHeaders = ForwardedHeaders.XForwardedFor | ForwardedHeaders.XForwardedProto;
        options.ForwardLimit = 1;
        options.KnownNetworks.Clear();
        options.KnownProxies.Clear();
        foreach (var address in proxyAddresses) options.KnownProxies.Add(address);
    });
}
var relationalValidation = storagePolicy.Mode == StorageMode.NewStorageValidation &&
    builder.Configuration["Storage:ValidationBackend"] == "RelationalThreeRpc";
// NewStoragePrimary is the production cutover: the same relational/四组件
// implementation, with a dedicated connection string and no Development bypass.
var relationalBackend = (storagePolicy.Mode == StorageMode.NewStorageValidation || storagePolicy.IsPrimary) &&
    builder.Configuration["Storage:ValidationBackend"] is "RelationalBackend" or "FourComponent";
var fourComponent = builder.Configuration["Storage:ValidationBackend"] == "FourComponent";
var privacyKeys = builder.Configuration["Privacy:KeyDirectory"];
if (relationalBackend && string.IsNullOrWhiteSpace(privacyKeys) && string.IsNullOrWhiteSpace(builder.Configuration["DataPath"]))
    throw new InvalidOperationException("Privacy:KeyDirectory is required for durable authenticator keys in relational mode");
privacyKeys ??= Path.GetFullPath(builder.Configuration["DataPath"] ?? Path.Combine(builder.Environment.ContentRootPath, "data", "botgram.json")) + ".keys";
var dataProtection = builder.Services.AddDataProtection().SetApplicationName("Botgram.AccountSecurity.v1")
    .PersistKeysToFileSystem(new DirectoryInfo(privacyKeys));
if (OperatingSystem.IsWindows()) dataProtection.ProtectKeysWithDpapi();
builder.Services.AddSingleton<AccountTotpService>();
if (fourComponent)
{
    builder.Services.AddSingleton(Botgram.Backend.Storage.FourComponentOptions.Load(builder.Configuration));
    builder.Services.AddSingleton<Botgram.Backend.Storage.FourComponentRuntime>();
    builder.Services.AddSingleton<Botgram.Backend.Storage.FourComponentReads>();
    builder.Services.AddHostedService(services => services.GetRequiredService<Botgram.Backend.Storage.FourComponentRuntime>());
}
if (!relationalBackend) builder.Services.AddSingleton<Store>();
if (relationalValidation || relationalBackend)
{
    var connection = new Npgsql.NpgsqlConnectionStringBuilder(
        StoragePolicy.ResolveConnectionString(builder.Configuration, storagePolicy)) { Pooling = true };
    builder.Services.AddSingleton(_ => Npgsql.NpgsqlDataSource.Create(connection.ConnectionString));
    if (relationalValidation) builder.Services.AddSingleton<PostgresRelationalValidation>();
    else
    {
        builder.Services.AddSingleton<Botgram.Backend.Storage.PostgresBackend>();
        builder.Services.AddHostedService<Botgram.Backend.Storage.RelationalMaintenanceService>();
    }
}
builder.Services.AddSingleton<ProtocolUsage>();
if (!relationalValidation && !relationalBackend) builder.Services.AddHostedService<CallExpiryService>();
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
var serverDomain = builder.Configuration["Protocol:ServerDomain"];
if (string.IsNullOrWhiteSpace(serverDomain) || Uri.CheckHostName(serverDomain) != UriHostNameType.Dns ||
    serverDomain != serverDomain.ToLowerInvariant() || serverDomain.EndsWith('.'))
    throw new InvalidOperationException("Protocol:ServerDomain must be an explicit lowercase DNS authority without port");
var protocolPolicy = ProtocolPolicy.Load(builder.Configuration);
var protobufPercent = protocolPolicy.ProtobufRolloutPercent;
var legacyJsonEnabled = protocolPolicy.LegacyJsonRpcEnabled;
var auditRpc = builder.Configuration.GetValue<bool>("Protocol:AuditRpc");
var app = builder.Build();
if (knownProxies.Length > 0) app.UseForwardedHeaders();
// Retired JSON API gate: before authentication, routing, rate limiting and any
// storage access. Every retired path gets the same explicit non-JSON 410.
app.Use((context, next) => JsonApiRetirement.Guard(context, next));
app.Use((context, next) => app.Services.GetRequiredService<ProtocolUsage>().Observe(context, next));
// Fail at startup if the database is invalid or already in use.
if (relationalBackend)
    await app.Services.GetRequiredService<Botgram.Backend.Storage.PostgresBackend>().VerifySchemaAsync();
else if (relationalValidation)
{
    var relational = app.Services.GetRequiredService<PostgresRelationalValidation>();
    await relational.InitializeAsync();
    var seedPath = builder.Configuration["Storage:ValidationSeedPath"];
    if (seedPath is not null)
    {
        var seed = JsonSerializer.Deserialize<Database>(await File.ReadAllTextAsync(seedPath), Store.Json)
            ?? throw new InvalidDataException("Invalid relational validation seed");
        await relational.SeedAsync(seed);
    }
}
else _ = app.Services.GetRequiredService<Store>();
app.Use(async (context, next) =>
{
    var path = context.Request.Path.Value?.TrimEnd('/');
    bool protobuf = string.Equals(path, ProtobufRpcEndpoint.Path, StringComparison.OrdinalIgnoreCase);
    bool legacy = string.Equals(path, "/api/telegram/rpc", StringComparison.OrdinalIgnoreCase);
    if (!builder.Configuration.GetValue<bool>("Protocol:AuditRpc") || (!protobuf && !legacy))
    {
        await next(context);
        return;
    }
    var started = System.Diagnostics.Stopwatch.GetTimestamp();
    var diagnosticStages = builder.Configuration.GetValue<bool>("Protocol:DiagnosticStages");
    if (diagnosticStages) System.Diagnostics.Activity.Current?.SetTag("diag.enabled", true);
    var gcBefore = diagnosticStages ? new[] { GC.CollectionCount(0), GC.CollectionCount(1), GC.CollectionCount(2) } : null;
    var originalRequestBody = context.Request.Body;
    var originalResponseBody = context.Response.Body;
    if (diagnosticStages)
    {
        context.Request.Body = new RpcDiagnosticStream(originalRequestBody, "request_body_read_ms");
        context.Response.Body = new RpcDiagnosticStream(originalResponseBody, "response_body_write_ms");
    }
    bool completed = false;
    try { await next(context); completed = true; }
    finally
    {
        if (diagnosticStages)
        {
            context.Request.Body = originalRequestBody;
            context.Response.Body = originalResponseBody;
            if (context.Items["diag.json_result_ready"] is long ready)
                System.Diagnostics.Activity.Current?.SetTag("diag.json_encode_write_ms", System.Diagnostics.Stopwatch.GetElapsedTime(ready).TotalMilliseconds);
        }
        #region debug-point request-stages
        if (diagnosticStages)
            app.Logger.LogInformation("RPC_STAGES trace={Trace} request_id={RequestId} elapsed_ms={Elapsed} stages={Stages} process_gc0_delta={Gc0} process_gc1_delta={Gc1} process_gc2_delta={Gc2} pending={Pending}",
                context.TraceIdentifier, context.Items["rpc_request_id"] ?? "-", System.Diagnostics.Stopwatch.GetElapsedTime(started).TotalMilliseconds,
                string.Join(";", System.Diagnostics.Activity.Current?.TagObjects.Where(t => t.Key.StartsWith("diag.")).Select(t => t.Key + "=" + t.Value) ?? []),
                GC.CollectionCount(0) - gcBefore![0], GC.CollectionCount(1) - gcBefore[1], GC.CollectionCount(2) - gcBefore[2], System.Threading.ThreadPool.PendingWorkItemCount);
        #endregion
        app.Logger.LogInformation("RPC_AUDIT protocol={Protocol} operation={Operation} request_id={RequestId} peer_kind={PeerKind} max_id={MaxId} error={Error} status={Status} elapsed_ms={ElapsedMs} completed={Completed} aborted={Aborted}",
            protobuf ? "protobuf" : "json", context.Items["rpc_operation"] ?? "unparsed",
            context.Items["rpc_request_id"] ?? "-", context.Items["rpc_peer_kind"] ?? "-",
            context.Items["rpc_max_id"] ?? "-", context.Items["rpc_error"] ?? "none", context.Response.StatusCode, System.Diagnostics.Stopwatch.GetElapsedTime(started).TotalMilliseconds,
            completed, context.RequestAborted.IsCancellationRequested);
    }
});
// All framework-level failures use the same non-JSON error format. Protobuf
// endpoints encode their own RpcError before this handler is reached.
static Task PlainError(HttpContext context, int status, string code)
{
    context.Response.StatusCode = status;
    context.Response.ContentType = "text/plain; charset=utf-8";
    context.Response.Headers.CacheControl = "no-store";
    var body = code + "\nstatus=" + status + "\n";
    context.Response.ContentLength = System.Text.Encoding.UTF8.GetByteCount(body);
    return context.Response.WriteAsync(body, context.RequestAborted);
}
app.Use(async (context, next) =>
{
    try { await next(context); }
    catch (ApiException ex)
    {
        context.Items["rpc_error"] = ex.Message;
        await PlainError(context, ex.Status, ex.Message);
    }
    catch (BadHttpRequestException)
    {
        context.Items["rpc_error"] = "invalid_request";
        await PlainError(context, 400, "invalid_request");
    }
    catch (Npgsql.NpgsqlException) when (fourComponent)
    {
        await PlainError(context, 503, "storage_unavailable");
    }
});
if (relationalValidation) app.Use(async (context, next) =>
{
    var path = JsonApiRetirement.Normalize(context.Request.Path.Value);
    if (!string.Equals(path, "/health", StringComparison.OrdinalIgnoreCase) &&
        !string.Equals(path, ProtobufRpcEndpoint.Path, StringComparison.OrdinalIgnoreCase) &&
        !string.Equals(path, AuthProtobufEndpoint.AuthPath, StringComparison.OrdinalIgnoreCase) &&
        !string.Equals(path, AuthProtobufEndpoint.AccountPath, StringComparison.OrdinalIgnoreCase))
        throw new ApiException(501, "BOTGRAM_RELATIONAL_ROUTE_UNSUPPORTED");
    await next(context);
});
app.UseWebSockets(new WebSocketOptions { KeepAliveInterval = TimeSpan.FromSeconds(20) });
app.UseRouting();
app.Use(async (context, next) =>
{
    var path = context.Request.Path;
    if (fourComponent && path.StartsWithSegments("/api"))
        await context.RequestServices.GetRequiredService<Botgram.Backend.Storage.FourComponentRuntime>()
            .EnsureOwnershipAsync(context.RequestAborted);
    var normalizedPath = JsonApiRetirement.Normalize(path.Value);
    // /api/v1/auth is pre-authentication; /api/v1/account validates its own
    // bearer inside the Protobuf endpoint so it can return a typed RpcError.
    if (normalizedPath.StartsWith("/api/", StringComparison.OrdinalIgnoreCase) &&
        normalizedPath != "/api/auth/register" && normalizedPath != "/api/auth/login" &&
        normalizedPath != "/api/auth/password" && normalizedPath != AuthProtobufEndpoint.AuthPath &&
        normalizedPath != AuthProtobufEndpoint.AccountPath)
    {
        var session = relationalBackend
            ? await Botgram.Backend.Storage.RelationalHttpRoutes.RequireAsync(context)
            : relationalValidation
            ? await context.RequestServices.GetRequiredService<PostgresRelationalValidation>().RequireAsync(context)
            : Auth.Require(context, context.RequestServices.GetRequiredService<Store>());
        context.Items["uid"] = session.UserId;
        context.Items["session"] = session;
        if (string.Equals(path.Value?.TrimEnd('/'), "/api/telegram/rpc", StringComparison.OrdinalIgnoreCase) &&
            !legacyJsonEnabled)
        {
            context.Items["protocol_usage_disabled"] = true;
            throw new ApiException(410, "BOTGRAM_JSON_RPC_DISABLED");
        }
    }
    await next(context);
});
app.UseRateLimiter();
// Protobuf-only planes are shared by both storage backends.
app.MapGet("/health", async (HttpContext context) =>
{
    // JsonApiRetirement.Health already emits status/protocol/json_api; only add
    // backend-specific fields here so the probe never contains duplicate keys.
    var fields = new List<KeyValuePair<string, string>>
    {
        new("storage", relationalBackend ? "postgresql" : "legacy-file")
    };
    var status = 200;
    if (fourComponent)
    {
        var runtime = context.RequestServices.GetService<Botgram.Backend.Storage.FourComponentRuntime>();
        if (runtime is not null)
        {
            var health = await runtime.HealthAsync(context.RequestAborted);
            fields.Add(new("full_stack_ready", health.FullStackReady ? "true" : "false"));
            fields.Add(new("postgres", health.Postgres ? "up" : "down"));
            fields.Add(new("kafka", health.Kafka ? "up" : "down"));
            fields.Add(new("scylla", health.Scylla ? "up" : "down"));
            fields.Add(new("redis", health.Redis ? "up" : "down"));
            fields.Add(new("workers", health.Workers ? "up" : "down"));
            fields.Add(new("pending_outbox", health.PendingOutbox.ToString(System.Globalization.CultureInfo.InvariantCulture)));
            fields.Add(new("pending_projection", health.PendingProjection.ToString(System.Globalization.CultureInfo.InvariantCulture)));
            if (!health.FullStackReady) status = 503;
        }
    }
    await JsonApiRetirement.Health(context, fields, status);
});
app.MapPost(AuthProtobufEndpoint.AuthPath, (HttpContext context) => AuthProtobufEndpoint.Handle(context, true))
    .RequireRateLimiting("auth");
app.MapPost(AuthProtobufEndpoint.AccountPath, (HttpContext context) => AuthProtobufEndpoint.Handle(context, false))
    .RequireRateLimiting("api");
if (relationalBackend)
{
    Botgram.Backend.Storage.RelationalHttpRoutes.Map(app, builder.Configuration, protocolPolicy);
    app.Run();
    return;
}
string Uid(HttpContext context) => (string)context.Items["uid"]!;
void Check(bool valid, string code) { if (!valid) throw new ApiException(400, code); }

app.MapPost("/api/auth/register", (RegisterInput input, Store store) =>
{
    var user = Auth.PrepareRegistration(input);
    return store.Write(db =>
    {
        if (db.Users.Any(u => u.Username == user.Username)) throw new ApiException(409, "username_taken");
        user = user with { Gid = Gids.Allocate(db, Gids.FromCountry(input.LocationCountry)) };
        db.Users.Add(user);
        return Auth.CreateSession(db, user);
    });
}).RequireRateLimiting("auth");
app.MapPost("/api/auth/login", (LoginInput input, Store store, AccountTotpService totp) =>
{
    Auth.ValidateLogin(input);
    return store.Write(db =>
    {
        var user = db.Users.FirstOrDefault(u => u.Username == input.Username.ToLowerInvariant());
        Auth.VerifyPassword(user, input.Password);
        AccountPasswordService.VerifyLogin(user!, AccountPrivacyService.State(db, user!.Id).Password, input.PasswordProof);
        totp.Verify(user.Id, AccountPrivacyService.State(db, user.Id).Totp, input.TotpCode);
        return Auth.CreateSession(db, user);
    });
}).RequireRateLimiting("auth");
app.MapPost("/api/auth/password", (LoginInput input, Store store) =>
{
    Auth.ValidateLogin(input);
    return store.Read(db =>
    {
        var user = db.Users.FirstOrDefault(u => u.Username == input.Username.ToLowerInvariant());
        Auth.VerifyPassword(user, input.Password);
        var state = db.AccountPrivacy?.GetValueOrDefault(user!.Id)?.Password ?? new();
        return AccountPasswordService.LoginInfo(user!, state);
    });
}).RequireRateLimiting("auth");

var api = app.MapGroup("/api").RequireRateLimiting("api");
api.MapGet("/security/totp", (HttpContext c, Store store) => store.Read(db =>
    AccountTotpService.Status(db.AccountPrivacy?.GetValueOrDefault(Uid(c))?.Totp ?? new())));
api.MapPost("/security/totp/{operation}", (string operation, TotpInput input, HttpContext c, Store store, AccountTotpService totp) =>
    store.Write(db => totp.Execute(Uid(c), db.Users.Single(u => u.Id == Uid(c)).Username,
        AccountPrivacyService.State(db, Uid(c)).Totp, operation, input.Code)));
api.MapGet("/telegram/me", (HttpContext c, Store store) => store.Write(db => TelegramGateway.CurrentUser(db, Uid(c))));
api.MapGet("/gid/regions", () => Gids.Regions.Where(r => r.Code != "001"));
api.MapPut("/me/gid", (ChangeGidInput input, HttpContext c, Store store) => store.Write(db =>
{
    Gids.Change(db, Uid(c), input);
    return TelegramGateway.CurrentUser(db, Uid(c));
}));
api.MapGet("/protocol/capabilities", (HttpContext c) =>
{
    var session = (Session)c.Items["session"]!;
    // The same bearer session always receives the same cohort; never expose its hash.
    var cohort = System.Buffers.Binary.BinaryPrimitives.ReadUInt32BigEndian(
        SHA256.HashData(System.Text.Encoding.UTF8.GetBytes(session.TokenHash))) % 100;
    return Results.Ok(new
    {
        negotiationVersion = 1,
        protocols = legacyJsonEnabled ? new[] { "protobuf-v1", "tl-json-v1" } : new[] { "protobuf-v1" },
        selectedProtocol = cohort < protobufPercent ? "protobuf-v1" : "tl-json-v1",
        eventSubprotocols = protocolPolicy.LegacyJsonWssEnabled
            ? new[] { "botgram.event-hint.v1.protobuf", "botgram.events.v1.json" }
            : new[] { "botgram.event-hint.v1.protobuf" },
        capabilities = new[] { "session-protocol-pinning", "send-random-id-idempotency-v1", "difference-pts-v1", "wss-wake-v1" }
    });
});
if (relationalValidation) api.MapPost("/v1/rpc", ProtobufRpcEndpoint.HandleRelational);
else api.MapPost("/v1/rpc", ProtobufRpcEndpoint.Handle);
api.MapPost("/telegram/rpc", (System.Text.Json.Nodes.JsonObject input, HttpContext c, Store store) =>
{
    if (auditRpc) c.Items["rpc_operation"] = input["$type"]?.GetValue<string>() ?? "unparsed";
    var result = store.Write(db => TelegramGateway.Execute(db, Uid(c), input, (Session)c.Items["session"]!));
    if (System.Diagnostics.Activity.Current?.GetTagItem("diag.enabled") is true)
        c.Items["diag.json_result_ready"] = System.Diagnostics.Stopwatch.GetTimestamp();
    return result;
});
api.MapPost("/auth/logout", (HttpContext c, Store store) => store.Write(db =>
{
    db.Sessions.RemoveAll(s => s.TokenHash == ((Session)c.Items["session"]!).TokenHash);
    return Results.NoContent();
}));
api.MapGet("/me", (HttpContext c, Store store) => store.Read(db => Auth.PublicUser(db.Users.Single(u => u.Id == Uid(c)))));
api.MapGet("/me/premium", (HttpContext c, Store store) =>
    store.Read(db => PremiumService.Get(db, (Session)c.Items["session"]!)));
api.MapPost("/me/premium/activate", (HttpContext c, Store store) =>
    store.Write(db => PremiumService.Activate(db, (Session)c.Items["session"]!)));
api.MapGet("/users", (string? username, Store store) =>
{
    Check(username is { Length: >= 3 and <= 32 }, "exact_username_required");
    return store.Read(db => db.Users.Where(u => u.Username == username!.ToLowerInvariant()).Select(Auth.PublicUser).ToArray());
});
api.MapPost("/conversations", (ConversationInput input, HttpContext c, Store store) =>
    store.Write(db => ConversationMessageService.CreateRest(db, Uid(c), input)));
api.MapGet("/conversations", (HttpContext c, Store store) => store.Read(db =>
    ConversationQueryService.List(db, Uid(c))));
api.MapGet("/conversations/{id}/messages", (string id, long? before, int? limit, HttpContext c, Store store) =>
{
    Check(before is null or > 0 && limit is null or >= 1 and <= 100, "invalid_pagination");
    return store.Read(db =>
    {
        var conversation = Auth.Conversation(db, id, Uid(c));
        return ConversationMessageService.History(db, conversation, new(OffsetId: before ?? 0, Limit: limit ?? 50)).Reverse().ToArray();
    });
});
api.MapPost("/conversations/{id}/messages", (string id, MessageInput input, HttpContext c, Store store) =>
{
    Check(!string.IsNullOrWhiteSpace(input.Text) && input.Text.Length <= 16000, "invalid_text");
    Check(input.ClientMessageId is { Length: >= 1 and <= 80 }, "invalid_client_message_id");
    return store.Write(db =>
    {
        var conversation = Auth.Conversation(db, id, Uid(c));
        return ConversationMessageService.Send(db, conversation, Uid(c), new(input.ClientMessageId, input.Text), MessageWriteContract.Rest);
    });
});
api.MapPost("/conversations/{id}/read", (string id, ReadInput input, HttpContext c, Store store) => store.Write(db =>
{
    var conversation = Auth.Conversation(db, id, Uid(c));
    return ConversationMessageService.ReadExact(db, conversation, Uid(c), input.MessageId);
}));

EventPage Events(Store store, string uid, long after, int limit) =>
    store.Read(db => SyncService.Events(db, uid, after, limit));
api.MapGet("/events", (long? after, int? limit, HttpContext c, Store store) =>
{
    Check(after is null or >= 0 && limit is null or >= 1 and <= 200, "invalid_pagination");
    return Events(store, Uid(c), after ?? 0, limit ?? 100);
});
api.MapGet("/ws", async (long? after, HttpContext c, Store store) =>
{
    Check(after is null or >= 0, "invalid_cursor");
    if (!c.WebSockets.IsWebSocketRequest) { c.Response.StatusCode = 400; return; }
    const string protobufHints = "botgram.event-hint.v1.protobuf";
    const string jsonEvents = "botgram.events.v1.json";
    var offered = c.WebSockets.WebSocketRequestedProtocols;
    var selected = offered.Contains(protobufHints) ? protobufHints : offered.Contains(jsonEvents) ? jsonEvents : null;
    c.Items["protocol_usage_selected"] = selected == protobufHints ? "protobuf" : selected == jsonEvents || offered.Count == 0 ? "json" : "unknown";
    if (offered.Count > 0 && selected is null)
    {
        c.Items["protocol_usage_unsupported"] = true;
        c.Response.StatusCode = 400;
        await c.Response.WriteAsJsonAsync(new { error = "unsupported_event_subprotocol" });
        return;
    }
    if (selected != protobufHints && !protocolPolicy.LegacyJsonWssEnabled)
    {
        c.Items["protocol_usage_disabled"] = true;
        c.Response.StatusCode = 410;
        await c.Response.WriteAsJsonAsync(new { error = "BOTGRAM_JSON_WSS_DISABLED" });
        return;
    }
    using var socket = await c.WebSockets.AcceptWebSocketAsync(selected);
    c.RequestServices.GetRequiredService<ProtocolUsage>().AcceptedWebSocket(c, selected == protobufHints);
    using var cancellation = CancellationTokenSource.CreateLinkedTokenSource(c.RequestAborted);
    var token = cancellation.Token;
    var session = (Session)c.Items["session"]!;
    var receiveFailure = 0;
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
                if (received.Count > 0) { Volatile.Write(ref receiveFailure, 2); break; }
            }
        }
        catch (OperationCanceledException) { }
        catch (WebSocketException) { Volatile.Write(ref receiveFailure, 1); }
        finally { await cancellation.CancelAsync(); }
    }
    var receive = Receive();
    var endOutcome = "closed";
    try
    {
        long cursor = after ?? 0;
        while (!token.IsCancellationRequested && socket.State == WebSocketState.Open)
        {
            if (!store.Read(db => db.Sessions.Any(s => s.TokenHash == session.TokenHash && s.ExpiresAt > DateTimeOffset.UtcNow))) { endOutcome = "session_expired"; break; }
            var batch = Events(store, Uid(c), cursor, 100);
            var next = batch.Cursor;
            if (next != cursor || batch.Events.Length > 0)
            {
                byte[] payload = selected == protobufHints
                    ? new Botgram.Protocol.V1.EventHint { ProtocolMajor = 1, Cursor = next }.ToByteArray()
                    : JsonSerializer.SerializeToUtf8Bytes(batch, Store.Json);
                // Await each send: no unbounded per-connection queue; a stalled peer is disconnected.
                using var sendTimeout = CancellationTokenSource.CreateLinkedTokenSource(token);
                sendTimeout.CancelAfter(TimeSpan.FromSeconds(10));
                await socket.SendAsync(payload, selected == protobufHints ? WebSocketMessageType.Binary : WebSocketMessageType.Text, true, sendTimeout.Token);
                cursor = next;
            }
            await Task.Delay(500, token);
        }
    }
    catch (OperationCanceledException) { endOutcome = token.IsCancellationRequested ? "cancelled" : "send_timeout"; }
    catch (WebSocketException) { endOutcome = "transport_error"; }
    finally
    {
        await cancellation.CancelAsync();
        await receive;
        var receiveResult = Volatile.Read(ref receiveFailure);
        if (endOutcome is "closed" or "cancelled" && receiveResult != 0)
            endOutcome = receiveResult == 2 ? "client_payload_rejected" : "transport_error";
        c.RequestServices.GetRequiredService<ProtocolUsage>().EndedWebSocket(selected == protobufHints, endOutcome);
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
    CallService.ValidateCallee(conversation, Uid(c), input.CalleeId);
    if (!AccountPrivacyService.Allowed(db, input.CalleeId, Uid(c), Botgram.Protocol.V1.PrivacyKey.PhoneCall))
        throw new ApiException(403, "USER_PRIVACY_RESTRICTED");
    if (db.Calls.Any(call => CallService.InProgress(call.State) &&
        (call.CallerId == Uid(c) || call.CalleeId == Uid(c) || call.CallerId == input.CalleeId || call.CalleeId == input.CalleeId)))
        throw new ApiException(409, "user_busy");
    var call = new Call(Guid.NewGuid().ToString("N"), conversation.Id, Uid(c), input.CalleeId, "ringing", DateTimeOffset.UtcNow);
    db.Calls.Add(call);
    db.Emit(conversation.Members, "call.updated", call);
    return call;
}));
api.MapGet("/calls", (HttpContext c, Store store) => store.Write(db => db.Calls
    .Where(call => (call.CallerId == Uid(c) || call.CalleeId == Uid(c)) && CallService.InProgress(call.State))
    .ToArray()));
api.MapGet("/calls/{id}", (string id, HttpContext c, Store store) => store.Write(db => Auth.Call(db, id, Uid(c))));
api.MapPost("/calls/{id}/{action}", (string id, string action, HttpContext c, Store store) => store.Write(db =>
{
    var call = Auth.Call(db, id, Uid(c));
    var updated = CallService.Transition(call, Uid(c), action, DateTimeOffset.UtcNow);
    if (updated == call) return call;
    db.Calls[db.Calls.IndexOf(call)] = updated;
    db.Emit([call.CallerId, call.CalleeId], "call.updated", updated);
    return updated;
}));
api.MapPost("/calls/{id}/signals", (string id, SignalInput input, HttpContext c, Store store) => store.Write(db =>
{
    var call = Auth.Call(db, id, Uid(c));
    var recipient = CallService.SignalRecipient(call, Uid(c), input);
    db.Emit([recipient], "call.signal", new CallSignal(id, Uid(c), input.Type, input.Data), DateTimeOffset.UtcNow.AddMinutes(5));
    return Results.Accepted();
}));
app.Run();
