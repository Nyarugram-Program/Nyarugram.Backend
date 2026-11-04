using System.Diagnostics.Metrics;
using System.Net;
using System.Net.WebSockets;
using System.Text.Json;
using System.Text.Json.Nodes;
using Botgram.Backend;
using Google.Protobuf;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging.Abstractions;
using P = Botgram.Protocol.V1;

internal static partial class HttpTests
{
    private static async Task<int> Retirement()
    {
        var defaults = ProtocolPolicy.Load(new ConfigurationBuilder().Build());
        Check(defaults == new ProtocolPolicy(true, true, 100), "omitted protocol settings preserve compatible defaults");
        foreach (var rollout in new[] { -1, 101, 0, 50 })
        {
            var invalid = new ConfigurationBuilder().AddInMemoryCollection(new Dictionary<string, string?>
            {
                ["Protocol:LegacyJsonRpcEnabled"] = "false", ["Protocol:ProtobufRolloutPercent"] = rollout.ToString()
            }).Build();
            bool rejected = false;
            try { ProtocolPolicy.Load(invalid); } catch (InvalidOperationException) { rejected = true; }
            Check(rejected, "inconsistent or out-of-range rollout rejected");
        }
        await UsageCounters();
        BeginTls();
        var results = new List<object>();
        try
        {
            var path = await TlsBaseline(1);
            long lastPts = 0;
            long expectedId = 1;
            var modes = new[] { ("A", true, true, 100), ("B", false, true, 100), ("C", true, false, 0), ("D", false, false, 100), ("E", true, true, 100) };
            var identity = Accounts.ToDictionary(p => p.Key, p => (p.Value.Id, p.Value.TlId, p.Value.Gid));
            foreach (var (mode, rpc, wss, rollout) in modes)
            {
                string log;
                await using (var s = await Server.Start(path, legacyEnabled: rpc, rollout: rollout, legacyWssEnabled: wss, usageLog: true))
                {
                    log = s.LogPath;
                    var before = Load(path);
                    await s.Raw(Req(new P.GetStateRequest()).ToByteArray(), 401, "unauthorized", token: null);
                    await s.Json("POST", "/api/telegram/rpc", T("TL_updates_getState"), status: 401);
                    await UpgradeRejected(s, null, null, 401, "unauthorized");
                    await UpgradeRejected(s, null, "invalid-session", 401, "session_expired");
                    var caps = await s.Json("GET", "/api/protocol/capabilities", null, Accounts["alice"].Token);
                    Check(Arr(caps, "protocols").Any(p => p!.GetValue<string>() == "tl-json-v1") == rpc, "RPC advertisement agrees with admission " + mode);
                    Check(Arr(caps, "eventSubprotocols").Any(p => p!.GetValue<string>() == "botgram.events.v1.json") == wss, "WSS advertisement agrees with admission " + mode);
                    Check(Str(caps, "selectedProtocol") == (rollout == 0 ? "tl-json-v1" : "protobuf-v1"), "0/100 cohort selection " + mode);
                    await UpgradeRejected(s, "unknown.v1", Accounts["alice"].Token, 400, "unsupported_event_subprotocol");
                    if (!rpc)
                    {
                        var denied = await s.Json("POST", "/api/telegram/rpc", T("TL_messages_sendMessage", ("peer", PeerJ(Accounts["bob"])), ("random_id", 999L), ("message", "isolated denied write")), Accounts["alice"].Token, 410);
                        Check(Str(denied, "error") == "BOTGRAM_JSON_RPC_DISABLED", "pinned JSON write explicitly rejected without migration " + mode);
                    }
                    if (!wss)
                    {
                        await UpgradeRejected(s, null, Accounts["alice"].Token, 410, "BOTGRAM_JSON_WSS_DISABLED");
                        await UpgradeRejected(s, "botgram.events.v1.json", Accounts["alice"].Token, 410, "BOTGRAM_JSON_WSS_DISABLED");
                    }
                    Equal(before, Load(path), "capabilities and all rejected requests leave snapshot unchanged " + mode);
                    if (rpc)
                    {
                        await s.Json("POST", "/api/telegram/rpc", T("TL_updates_getState"), Accounts["alice"].Token);
                        await s.Json("POST", "/api/telegram/rpc", T("TL_messages_sendMessage", ("peer", PeerJ(Accounts["bob"])), ("random_id", long.MinValue), ("message", "benchmark fixed message 😀")), Accounts["alice"].Token);
                    }
                    var state = (await s.Rpc(Req(new P.GetStateRequest()))).GetState;
                    Check(state.Pts >= lastPts, "pts never regresses across configuration restarts " + mode);
                    var retry = (await s.Rpc(Send(long.MinValue, "benchmark fixed message 😀"))).SendMessage;
                    Check(retry.Mapping.MessageId == 1, "cross-mode retry preserves original ID " + mode);
                    var sent = (await s.Rpc(Send(20000 + expectedId, "isolated mode fixture"))).SendMessage;
                    expectedId++;
                    Check(sent.Mapping.MessageId == expectedId, "new message IDs remain contiguous " + mode);
                    lastPts = (await s.Rpc(Req(new P.GetStateRequest()))).GetState.Pts;
                    Check(lastPts == state.Pts + 1, "mode adds exactly one event/pts " + mode);
                    await EventConnection(s, "botgram.event-hint.v1.protobuf");
                    // Offering both must select PB even with JSON disabled and rollout=0.
                    await EventConnection(s, "both");
                    await EventConnection(s, "botgram.event-hint.v1.protobuf", rejectPayload: true);
                    if (wss) { await EventConnection(s, null); await EventConnection(s, "botgram.events.v1.json"); }
                    var login = await s.Json("POST", "/api/auth/login", new { username = "alice", password = "isolated-test-password" });
                    var token = Str(login, "token");
                    Check(Str(login["user"]!, "id") == Accounts["alice"].Id, "REST relogin retains UUID " + mode);
                    var newCaps = await s.Json("GET", "/api/protocol/capabilities", null, token);
                    Check(Str(newCaps, "selectedProtocol") == (rollout == 0 ? "tl-json-v1" : "protobuf-v1"), "new login can renegotiate safely " + mode);
                    await s.Json("POST", "/api/auth/logout", null, token, 204);
                    await s.Json("GET", "/api/protocol/capabilities", null, token, 401);
                    await UpgradeRejected(s, "botgram.event-hint.v1.protobuf", token, 401, "session_expired");
                    var registered = await s.Json("POST", "/api/auth/register", new { username = "mode_" + mode.ToLowerInvariant(), displayName = "Isolated Mode", password = "isolated-test-password" });
                    await s.Json("GET", "/api/me", null, Str(registered, "token"));
                    var gidRegions = await s.Json("GET", "/api/gid/regions", null, Accounts["alice"].Token);
                    Check(gidRegions.AsArray().Count == 29, "GID REST remains available " + mode);
                    await s.Json("PUT", "/api/me/gid", new { regionCode = "030", number = "12345678" }, Str(registered, "token"), mode == "A" ? 200 : 409);
                    foreach (var (name, original) in identity)
                    {
                        var me = await s.Json("GET", "/api/telegram/me", null, Accounts[name].Token);
                        Check(Long(me, "id") == original.TlId && Str(me, "phone") == original.Gid, "existing TL/GID identities retained " + name + mode);
                        Check(Arr(Load(path), "users").Any(u => Str(u!, "username") == name && Str(u!, "id") == original.Id), "existing UUID retained " + name + mode);
                    }
                    Check(Arr(Load(path), "messages").Count == expectedId, "no implicit retry/duplicate/denied write persisted " + mode);
                }
                var logs = File.ReadAllText(log);
                Check(logs.Contains("PROTOCOL_USAGE transport=rpc protocol=protobuf outcome=success status=200"), "PB usage observable " + mode);
                Check(logs.Contains("PROTOCOL_USAGE transport=wss protocol=protobuf outcome=success status=101"), "PB WSS admission observable " + mode);
                Check(logs.Contains("outcome=authentication_failed status=401"), "authentication failures separately observable " + mode);
                Check(logs.Contains("outcome=unsupported_client status=400"), "unsupported WSS client separately observable " + mode);
                Check(logs.Contains("PROTOCOL_WSS_END protocol=protobuf outcome=client_payload_rejected"), "post-upgrade WSS rejection separately observable " + mode);
                Check(!logs.Contains("Bearer ") && !logs.Contains("isolated-test-password") && !logs.Contains("isolated mode fixture"), "usage logs omit credentials and message bodies " + mode);
                if (!rpc) Check(logs.Contains("protocol=json outcome=disabled status=410"), "disabled RPC counted " + mode);
                if (!wss) Check(logs.Contains("transport=wss protocol=json outcome=disabled status=410"), "disabled WSS counted " + mode);
                results.Add(new { mode, legacyJsonRpc = rpc, legacyJsonWss = wss, rollout, lastPts, lastMessageId = expectedId, log });
            }
            var unchanged = Load(path);
            bool invalidStartup = false;
            try { await using var invalid = await Server.Start(path, legacyEnabled: false, rollout: 0); }
            catch (Exception ex) when (ex.Message.StartsWith("isolated backend exited with", StringComparison.Ordinal)) { invalidStartup = true; }
            Check(invalidStartup, "contradictory startup configuration rejected");
            Equal(unchanged, Load(path), "invalid configuration fails before database mutation");
            File.WriteAllText(Path.Combine(Root, "retirement.json"), JsonSerializer.Serialize(new { scope = "Same isolated database across A/B/C/D/E restarts, real pinned localhost TLS/WSS; no production switch, no Android device", assertions, results }, new JsonSerializerOptions { WriteIndented = true }));
            Console.WriteLine($"P7 local retirement PASS assertions={assertions}; modes=A/B/C/D/E; root={Root}; production retirement NOT APPROVED; P5/P6 unchanged");
            return 0;
        }
        finally { EndTls(); }
    }

    private static async Task UpgradeRejected(Server s, string? protocol, string? token, int status, string code)
    {
        using var request = new HttpRequestMessage(HttpMethod.Get, "/api/ws?after=0");
        request.Headers.Connection.Add("Upgrade"); request.Headers.Upgrade.Add(new("websocket"));
        request.Headers.Add("Sec-WebSocket-Version", "13"); request.Headers.Add("Sec-WebSocket-Key", "dGhlIHNhbXBsZSBub25jZQ==");
        if (protocol is not null) request.Headers.Add("Sec-WebSocket-Protocol", protocol);
        if (token is not null) request.Headers.Authorization = new("Bearer", token);
        using var response = await s.Client.SendAsync(request);
        Check((int)response.StatusCode == status, "WSS rejection HTTP status " + status);
        var body = JsonNode.Parse(await response.Content.ReadAsStringAsync())!;
        Check(Str(body, "error") == code, "WSS rejection code " + code);
    }

    private static async Task EventConnection(Server s, string? protocol, bool rejectPayload = false)
    {
        using var socket = new ClientWebSocket();
        socket.Options.RemoteCertificateValidationCallback = (_, cert, _, errors) => ValidateCertificate(cert, errors);
        socket.Options.SetRequestHeader("Authorization", "Bearer " + Accounts["bob"].Token);
        if (protocol == "both") { socket.Options.AddSubProtocol("botgram.events.v1.json"); socket.Options.AddSubProtocol("botgram.event-hint.v1.protobuf"); }
        else if (protocol is not null) socket.Options.AddSubProtocol(protocol);
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        await socket.ConnectAsync(s.WebSocketUri(0), timeout.Token);
        Check(socket.State == WebSocketState.Open, "retirement WSS actual upgrade");
        var buffer = new byte[8192];
        var received = await socket.ReceiveAsync(buffer.AsMemory(), timeout.Token);
        var pb = protocol is "both" or "botgram.event-hint.v1.protobuf";
        Check(protocol is null ? string.IsNullOrEmpty(socket.SubProtocol) : socket.SubProtocol == (pb ? "botgram.event-hint.v1.protobuf" : protocol), "WSS selection independent of HTTP cohort");
        Check(received.MessageType == (pb ? WebSocketMessageType.Binary : WebSocketMessageType.Text) && received.EndOfMessage, "actual event frame media");
        if (pb) Check(P.EventHint.Parser.ParseFrom(buffer, 0, received.Count).Cursor > 0, "PB hint decodes after restart");
        else Check(Long(JsonNode.Parse(buffer.AsSpan(0, received.Count))!, "cursor") > 0, "JSON cursor decodes after restart");
        if (rejectPayload)
        {
            await socket.SendAsync(new byte[] { 1 }.AsMemory(), WebSocketMessageType.Binary, true, timeout.Token);
            var closed = await socket.ReceiveAsync(buffer.AsMemory(), timeout.Token);
            Check(closed.MessageType == WebSocketMessageType.Close, "invalid upstream payload closes WSS without state mutation");
            return;
        }
        await socket.CloseOutputAsync(WebSocketCloseStatus.NormalClosure, "test complete", timeout.Token);
    }

    private static async Task UsageCounters()
    {
        var measurements = new List<(string Name, long Value, Dictionary<string, string> Tags)>();
        using var listener = new MeterListener();
        listener.InstrumentPublished = (instrument, source) => { if (instrument.Meter.Name == ProtocolUsage.MeterName) source.EnableMeasurementEvents(instrument); };
        listener.SetMeasurementEventCallback<long>((instrument, value, tags, _) => measurements.Add((instrument.Name, value, tags.ToArray().ToDictionary(t => t.Key, t => t.Value?.ToString() ?? ""))));
        listener.Start();
        using var usage = new ProtocolUsage(NullLogger<ProtocolUsage>.Instance, new ConfigurationBuilder().Build());
        foreach (var status in new[] { 200, 401, 410, 500 })
        {
            var context = new DefaultHttpContext(); context.Request.Path = "/api/telegram/rpc";
            await usage.Observe(context, c => { c.Response.StatusCode = status; if (status == 410) c.Items["protocol_usage_disabled"] = true; return Task.CompletedTask; });
        }
        var ws = new DefaultHttpContext(); ws.Request.Path = "/api/ws";
        await usage.Observe(ws, c => { usage.AcceptedWebSocket(c, true); return Task.CompletedTask; });
        usage.EndedWebSocket(true, "closed");
        var cancelled = new DefaultHttpContext(); cancelled.Request.Path = "/api/v1/rpc";
        cancelled.RequestAborted = new CancellationToken(canceled: true);
        await usage.Observe(cancelled, _ => Task.CompletedTask);
        Check(measurements.Where(m => m.Name == "botgram.protocol.attempts").Sum(m => m.Value) == 6, "metric counts each HTTP/admission exactly once");
        Check(measurements.Where(m => m.Name == "botgram.protocol.errors").Sum(m => m.Value) == 4, "metric numerator matches errors and cancelled transport");
        Check(measurements.Any(m => m.Name == "botgram.protocol.attempts" && m.Tags.GetValueOrDefault("outcome") == "cancelled" && m.Tags.GetValueOrDefault("http_status") == "0"), "handled cancellation is not reported as HTTP success");
        Check(measurements.Count(m => m.Name == "botgram.protocol.wss.ends") == 1, "WSS lifetime metric independent of handshake denominator");
        Check(measurements.All(m => m.Tags.Keys.All(k => k is "transport" or "protocol" or "outcome" or "http_status")), "metric labels are low-cardinality and contain no identities");
    }
}
