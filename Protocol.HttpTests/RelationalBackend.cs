using System.Net;
using System.Net.WebSockets;
using System.Text.Json.Nodes;
using Botgram.Backend.Storage;
using Google.Protobuf;
using Npgsql;
using P = Botgram.Protocol.V1;
using Microsoft.Extensions.Configuration;

internal static partial class HttpTests
{
    private static async Task<int> RelationalBackend(bool fullStack = false)
    {
        CallRelayConfiguration();
        var adminString = Environment.GetEnvironmentVariable("BOTGRAM_PG_TEST_CONNECTION_STRING")
            ?? throw new InvalidOperationException("Relational HTTP tests require isolated PostgreSQL");
        var options = new NpgsqlConnectionStringBuilder(adminString);
        if (!(options.Host == "localhost" || IPAddress.TryParse(options.Host, out var address) && IPAddress.IsLoopback(address)) ||
            !System.Text.RegularExpressions.Regex.IsMatch(options.Database ?? "", "\\Abotgram_test_[a-z0-9_]+\\z"))
            throw new InvalidOperationException("Relational HTTP tests require loopback botgram_test_* admin database");
        var database = "botgram_test_http_" + Guid.NewGuid().ToString("N");
        FourComponentOptions? stack = null;
        if (fullStack)
        {
            var path = Environment.GetEnvironmentVariable("BOTGRAM_P7_TEST_CONFIG")
                ?? throw new InvalidOperationException("Four-component HTTP tests require BOTGRAM_P7_TEST_CONFIG");
            var scope = "botgram_test_" + Guid.NewGuid().ToString("N");
            stack = FourComponentOptions.Load(new ConfigurationBuilder().AddJsonFile(Path.GetFullPath(path)).Build())
                with { Topic = scope, Consumer = scope, Keyspace = scope, CacheNamespace = scope };
        }
        await using var admin = NpgsqlDataSource.Create(adminString);
        await using (var create = admin.CreateCommand("CREATE DATABASE " + database)) await create.ExecuteNonQueryAsync();
        options.Database = database;
        var noStore = Path.Combine(Root, "relational-no-store", "forbidden.json");
        long messageId = 0, wallpaperRevision = 0;
        try
        {
            await using var source = NpgsqlDataSource.Create(options.ConnectionString);
            await PostgresBackendSchema.ApplyAsync(source);
            if (stack is not null) await FourComponentSetup.PrepareAsync(source, stack);
            async Task<object?> Scalar(string sql)
            {
                await using var command = source.CreateCommand(sql);
                return await command.ExecuteScalarAsync();
            }
            P.RpcRequest Correlate(P.RpcRequest request)
            {
                request.ProtocolMajor = 1; request.RequestId = Guid.NewGuid().ToString("N");
                return request;
            }
            async Task<P.EventHint> Hint(ClientWebSocket socket)
            {
                using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(10));
                var buffer = new byte[1024];
                var frame = await socket.ReceiveAsync(buffer.AsMemory(), timeout.Token);
                Check(frame.MessageType == WebSocketMessageType.Binary && frame.EndOfMessage, "PG WSS binary bounded hint");
                return P.EventHint.Parser.ParseFrom(buffer.AsSpan(0, frame.Count));
            }
            Task<Server> Start() => Server.Start(noStore, legacyEnabled: false, legacyWssEnabled: false,
                validationConnection: options.ConnectionString, fullRelational: true, fullStack: stack, relays: true);
            await using (var server = await Start())
            {
                foreach (var name in new[] { "alice", "bob", "outsider" })
                {
                    var auth = await server.Json("POST", "/api/auth/register",
                        new { username = name, displayName = name, password = "isolated-password" });
                    var token = Str(auth, "token");
                    var me = await server.Json("GET", "/api/telegram/me", null, token);
                    Accounts.Add(name, new(Str(auth["user"]!, "id"), token, Long(me, "id"), Str(me, "phone")));
                }
                var health = await server.Json("GET", "/health", null);
                Check(health["fullStackReady"]!.GetValue<bool>() == fullStack, "configured component readiness");
                var caps = await server.Json("GET", "/api/protocol/capabilities", null, Accounts["alice"].Token);
                Check(Str(caps, "selectedProtocol") == "protobuf-v1", "PG protocol negotiation");
                await server.Json("POST", "/api/telegram/rpc", T("TL_updates_getState"), Accounts["alice"].Token, 410);
                await server.Json("GET", "/api/me", null, status: 401);
                var sent = await server.Rpc(Send(987654321, "PG HTTP"));
                messageId = sent.SendMessage.Message.MessageId;
                Check((await server.Rpc(Send(987654321, "PG HTTP"))).SendMessage.Message.MessageId == messageId, "PG HTTP send retry");
                await server.Rpc(Req(new P.GetStateRequest()));
                await server.Rpc(Req(new P.GetDifferenceRequest { Pts = 1 }));
                await server.Rpc(Req(new P.GetHistoryRequest { Peer = Peer("bob"), Limit = 1 }));
                await server.Rpc(Req(new P.GetMessagesRequest { MessageIds = { messageId } }));
                await server.Rpc(Req(new P.ReadHistoryRequest { Peer = Peer("alice"), MaxMessageId = messageId }), "bob");
                await server.Rpc(Req(new P.GetDialogsRequest()));
                await server.Rpc(Req(new P.GetPinnedDialogsRequest()));
                await server.Rpc(Req(new P.GetPeerDialogsRequest { Peers = { Peer("bob") } }));
                await server.Rpc(Req(new P.GetUsersRequest { Users = { Selector("bob") } }));
                await server.Rpc(Req(new P.GetContactsRequest()));
                await server.Rpc(Req(new P.SearchContactsRequest { Query = "bob" }));
                await server.Rpc(Req(new P.ResolveUsernameRequest { Username = "bob" }));
                await server.Rpc(Req(new P.ResolveGidRequest { Gid = Accounts["bob"].Gid }));
                await server.Rpc(Req(new P.ImportContactsRequest { Contacts = {
                    new P.ImportContact { Gid = Accounts["bob"].Gid, FirstName = "Friend", ClientId = 1 } } }));
                await server.Rpc(Req(new P.GetPeerSettingsRequest { Peer = Peer("bob") }));
                await server.Rpc(Req(new P.GetFullUserRequest { User = Selector("bob") }));
                await server.Rpc(Req(new P.GetNotifySettingsRequest { Target = new() { AllUsers = new() } }));
                var group = (await server.Rpc(Req(new P.CreateGroupRequest { Title = "PG HTTP", Users = { Selector("bob") } }))).CreateGroup.Group;
                await server.Rpc(Req(new P.GetGroupsRequest { GroupIds = { group.GroupId } }));
                await server.Rpc(Req(new P.GetFullGroupRequest { GroupId = group.GroupId }));
                await server.Rpc(Correlate(new() { ResolveIdentities = new() { TlUserIds = { Accounts["bob"].TlId }, TlChatIds = { group.TlChatId } } }));
                await server.Rpc(Req(new P.GetFullGroupRequest { GroupId = group.GroupId }), "outsider", 404, "PEER_ID_INVALID");
                await server.Json("POST", "/api/me/premium/activate", new { }, Accounts["alice"].Token);
                Check((await server.Json("GET", "/api/me/premium", null, Accounts["alice"].Token))["active"]!.GetValue<bool>(),
                    "PG HTTP Premium activation");
                var conversation = await server.Json("POST", "/api/conversations",
                    new { kind = "direct", memberIds = new[] { Accounts["bob"].Id } }, Accounts["alice"].Token);
                var conversationId = Str(conversation, "id");
                var rest = await server.Json("POST", $"/api/conversations/{conversationId}/messages",
                    new { clientMessageId = "http-rest-retry", text = "REST PG authority" }, Accounts["alice"].Token);
                Check(Long(await server.Json("POST", $"/api/conversations/{conversationId}/messages",
                    new { clientMessageId = "http-rest-retry", text = "REST PG authority" }, Accounts["alice"].Token), "id") == Long(rest, "id"),
                    "PG REST retry shares permanent authority");
                Check((await server.Json("GET", $"/api/conversations/{conversationId}/messages", null,
                    Accounts["bob"].Token)).AsArray().Count == 2, "REST history includes PB and REST messages");
                await server.Json("POST", $"/api/conversations/{conversationId}/read", new { messageId = Long(rest, "id") }, Accounts["bob"].Token);
                await server.Json("GET", "/api/conversations", null, Accounts["alice"].Token);
                await server.Json("GET", "/api/users?username=bob", null, Accounts["alice"].Token);
                var call = await server.Json("POST", "/api/calls",
                    new { conversationId, calleeId = Accounts["bob"].Id }, Accounts["alice"].Token);
                var callId = Str(call, "id");
                await server.Json("POST", $"/api/calls/{callId}/accept", new { }, Accounts["alice"].Token, 403);
                Check(Str(await server.Json("POST", $"/api/calls/{callId}/accept", new { }, Accounts["bob"].Token), "state") == "active",
                    "PG HTTP callee accepts");
                using (var signal = new HttpRequestMessage(HttpMethod.Post, $"/api/calls/{callId}/signals"))
                {
                    signal.Headers.Authorization = new("Bearer", Accounts["alice"].Token);
                    signal.Content = new StringContent("{\"type\":\"offer\",\"data\":\"synthetic-offer\"}",
                        System.Text.Encoding.UTF8, "application/json");
                    using var response = await server.Client.SendAsync(signal);
                    Check(response.StatusCode == HttpStatusCode.Accepted, "PG HTTP call signaling");
                }
                await server.Json("GET", "/api/calls", null, Accounts["bob"].Token);
                await server.Json("GET", $"/api/calls/{callId}", null, Accounts["bob"].Token);
                await server.Json("GET", $"/api/calls/{callId}", null, Accounts["outsider"].Token, 404);
                await server.Json("POST", $"/api/calls/{callId}/end", new { }, Accounts["alice"].Token);
                var dh = await server.Rpc(Req(new P.GetDhConfigRequest { Version = 0, RandomLength = 32 }));
                Check(dh.GetDhConfig.G == 3 && dh.GetDhConfig.P.Length == 256 && dh.GetDhConfig.Random.Length == 32 &&
                    !dh.GetDhConfig.NotModified, "PG getDhConfig returns prime and fresh random");
                Check((await server.Rpc(Req(new P.GetDhConfigRequest { Version = 1, RandomLength = 8 }))).GetDhConfig.NotModified,
                    "PG getDhConfig not modified");
                var callConfig = (await server.Rpc(Req(new P.PhoneGetCallConfigRequest()))).GetCallConfig.Data;
                Check(callConfig.Contains("\"botgram_calls\"") && callConfig.Contains("\"turn\"") &&
                    callConfig.Contains("\"stun\""), "PG getCallConfig reports TURN/STUN capabilities");
                Check((await server.Rpc(Req(new P.PhoneSaveCallDebugRequest { Debug = "{}" }))).SaveCallDebug.Value,
                    "PG saveCallDebug");
                var phoneProtocol = new P.PhoneCallProtocol { UdpP2P = true, UdpReflector = true, MinLayer = 65, MaxLayer = 92 };
                phoneProtocol.LibraryVersions.Add("10.0.0");
                byte[] gA = new byte[256]; Array.Fill(gA, (byte)2);
                byte[] gB = new byte[256]; Array.Fill(gB, (byte)3);
                var requested = await server.Rpc(Req(new P.PhoneRequestCallRequest
                {
                    User = Selector("bob"), RandomId = 777000111, Video = false,
                    GAHash = ByteString.CopyFrom(System.Security.Cryptography.SHA256.HashData(gA)), Protocol = phoneProtocol
                }));
                var phoneCall = requested.RequestCall.PhoneCall;
                Check(phoneCall.CallId > 0 && phoneCall.AccessHash != 0 && phoneCall.State == P.PhoneCallState.Waiting &&
                    phoneCall.P2PAllowed && requested.RequestCall.Users.Count == 1, "PG requestCall allocates identity");
                var replayRequest = new P.PhoneRequestCallRequest
                {
                    User = Selector("bob"), RandomId = 777000111, Video = false,
                    GAHash = ByteString.CopyFrom(System.Security.Cryptography.SHA256.HashData(gA)), Protocol = phoneProtocol
                };
                Check((await server.Rpc(Req(replayRequest))).RequestCall.PhoneCall.CallId == phoneCall.CallId,
                    "PG requestCall idempotent retry");
                await server.Rpc(Req(new P.PhoneRequestCallRequest
                {
                    User = Selector("bob"), RandomId = 777000111, Video = false,
                    GAHash = ByteString.CopyFrom(new byte[32]), Protocol = phoneProtocol
                }), "alice", 409, "CALL_IDEMPOTENCY_CONFLICT");
                var reference = new P.PhoneCallRef { CallId = phoneCall.CallId, AccessHash = phoneCall.AccessHash };
                Check((await server.Rpc(Req(new P.PhoneReceivedCallRequest { Peer = reference }), "bob")).ReceivedCall.Value,
                    "PG receivedCall");
                var accepted = await server.Rpc(Req(new P.PhoneAcceptCallRequest
                {
                    Peer = reference, GB = ByteString.CopyFrom(gB), Protocol = phoneProtocol
                }), "bob");
                Check(accepted.AcceptCall.PhoneCall.State == P.PhoneCallState.Accepted, "PG acceptCall");
                await server.Rpc(Req(new P.PhoneAcceptCallRequest
                {
                    Peer = reference, GB = ByteString.CopyFrom(gB), Protocol = phoneProtocol
                }), "bob");
                byte[] otherB = new byte[256]; Array.Fill(otherB, (byte)4);
                await server.Rpc(Req(new P.PhoneAcceptCallRequest
                {
                    Peer = reference, GB = ByteString.CopyFrom(otherB), Protocol = phoneProtocol
                }), "bob", 409, "CALL_ALREADY_ACCEPTED");
                byte[] otherA = new byte[256]; Array.Fill(otherA, (byte)5);
                await server.Rpc(Req(new P.PhoneConfirmCallRequest
                {
                    Peer = reference, GA = ByteString.CopyFrom(otherA), KeyFingerprint = 0x1122334455, Protocol = phoneProtocol
                }), "alice", 400, "CALL_HASH_MISMATCH");
                var confirmed = await server.Rpc(Req(new P.PhoneConfirmCallRequest
                {
                    Peer = reference, GA = ByteString.CopyFrom(gA), KeyFingerprint = 0x1122334455, Protocol = phoneProtocol
                }), "alice");
                Check(confirmed.ConfirmCall.PhoneCall.State == P.PhoneCallState.Active &&
                    confirmed.ConfirmCall.PhoneCall.KeyFingerprint == 0x1122334455, "PG confirmCall validates g_a hash");
                byte[] signalData = { 1, 2, 3, 4 };
                Check((await server.Rpc(Req(new P.PhoneSendSignalingDataRequest
                {
                    Peer = reference, Data = ByteString.CopyFrom(signalData)
                }))).SendSignalingData.Value, "PG sendSignalingData");
                var bobState = await server.Rpc(Req(new P.GetStateRequest()), "bob");
                var difference = await server.Rpc(Req(new P.GetDifferenceRequest { Pts = Math.Max(1, bobState.GetState.Pts - 8) }), "bob");
                var page = difference.GetDifference.Complete ?? difference.GetDifference.Slice;
                Check(page is not null && (page.Updates.Any(u => u.PhoneCallSignaling is { CallId: > 0 }) ||
                    page.Updates.Any(u => u.PhoneCall is { PhoneCall.State: P.PhoneCallState.Active })),
                    "PG difference carries phone updates");
                var discarded = await server.Rpc(Req(new P.PhoneDiscardCallRequest
                {
                    Peer = reference, Duration = 3, Reason = P.PhoneCallDiscardReason.Hangup
                }), "bob");
                Check(discarded.DiscardCall.Updates_.Any(u => u.PhoneCall.PhoneCall.State == P.PhoneCallState.Discarded),
                    "PG discardCall converges to terminal state");
                await server.Rpc(Req(new P.AccountPrivacyRequest
                {
                    SetRules = new() { Key = P.PrivacyKey.PhoneP2P, Rules = { new P.PrivacyRule { Kind = P.PrivacyRuleKind.DisallowAll } } }
                }), "bob");
                var relayed = await server.Rpc(Req(new P.PhoneRequestCallRequest
                {
                    User = Selector("bob"), RandomId = 777000222, Video = false,
                    GAHash = ByteString.CopyFrom(System.Security.Cryptography.SHA256.HashData(gA)), Protocol = phoneProtocol
                }));
                var relayedCall = relayed.RequestCall.PhoneCall;
                Check(!relayedCall.P2PAllowed && !relayedCall.Protocol.UdpP2P,
                    "PG callee PhoneP2P disallow forces the call off peer-to-peer");
                var relayedRef = new P.PhoneCallRef { CallId = relayedCall.CallId, AccessHash = relayedCall.AccessHash };
                await server.Rpc(Req(new P.PhoneReceivedCallRequest { Peer = relayedRef }), "bob");
                await server.Rpc(Req(new P.PhoneAcceptCallRequest
                {
                    Peer = relayedRef, GB = ByteString.CopyFrom(gB), Protocol = phoneProtocol
                }), "bob");
                var relayedConfirmed = await server.Rpc(Req(new P.PhoneConfirmCallRequest
                {
                    Peer = relayedRef, GA = ByteString.CopyFrom(gA), KeyFingerprint = 0x1122334456, Protocol = phoneProtocol
                }));
                Check(relayedConfirmed.ConfirmCall.PhoneCall.Connections.Any(c => c.Turn) &&
                    relayedConfirmed.ConfirmCall.PhoneCall.Connections.Any(c => c.Stun),
                    "PG relayed call exposes TURN and STUN endpoints to the client");
                await server.Rpc(Req(new P.PhoneDiscardCallRequest
                {
                    Peer = relayedRef, Duration = 1, Reason = P.PhoneCallDiscardReason.Hangup
                }), "bob");
                // Both proposals are kept; accept intersects the layer range and library versions,
                // and confirm must echo the caller's original proposal.
                var callerProposal = new P.PhoneCallProtocol { UdpP2P = true, UdpReflector = true, MinLayer = 60, MaxLayer = 95 };
                callerProposal.LibraryVersions.Add("11.0.0");
                callerProposal.LibraryVersions.Add("10.0.0");
                var calleeProposal = new P.PhoneCallProtocol { UdpP2P = true, UdpReflector = true, MinLayer = 75, MaxLayer = 100 };
                calleeProposal.LibraryVersions.Add("9.0.0");
                calleeProposal.LibraryVersions.Add("10.0.0");
                var negotiatedRequest = await server.Rpc(Req(new P.PhoneRequestCallRequest
                {
                    User = Selector("bob"), RandomId = 777000333, Video = false,
                    GAHash = ByteString.CopyFrom(System.Security.Cryptography.SHA256.HashData(gA)), Protocol = callerProposal
                }));
                var negotiatedRef = new P.PhoneCallRef
                {
                    CallId = negotiatedRequest.RequestCall.PhoneCall.CallId,
                    AccessHash = negotiatedRequest.RequestCall.PhoneCall.AccessHash
                };
                var negotiatedAccepted = await server.Rpc(Req(new P.PhoneAcceptCallRequest
                {
                    Peer = negotiatedRef, GB = ByteString.CopyFrom(gB), Protocol = calleeProposal
                }), "bob");
                var negotiatedProtocol = negotiatedAccepted.AcceptCall.PhoneCall.Protocol;
                Check(negotiatedProtocol.MinLayer == 75 && negotiatedProtocol.MaxLayer == 95 &&
                    negotiatedProtocol.LibraryVersions.SequenceEqual(new[] { "10.0.0" }) &&
                    !negotiatedProtocol.UdpP2P,
                    "PG accept intersects layers, library versions and forced relay privacy");
                var negotiatedPts = (await server.Rpc(Req(new P.GetStateRequest()), "bob")).GetState.Pts;
                var requestRetryAfterAccept = await server.Rpc(Req(new P.PhoneRequestCallRequest
                {
                    User = Selector("bob"), RandomId = 777000333, Video = false,
                    GAHash = ByteString.CopyFrom(System.Security.Cryptography.SHA256.HashData(gA)), Protocol = callerProposal
                }));
                Check(requestRetryAfterAccept.RequestCall.PhoneCall.CallId == negotiatedRef.CallId &&
                    requestRetryAfterAccept.RequestCall.PhoneCall.State == P.PhoneCallState.Accepted,
                    "PG requestCall retry after accept returns the same call and state");
                var acceptRetryAfterAccept = await server.Rpc(Req(new P.PhoneAcceptCallRequest
                {
                    Peer = negotiatedRef, GB = ByteString.CopyFrom(gB), Protocol = calleeProposal
                }), "bob");
                Check(acceptRetryAfterAccept.AcceptCall.PhoneCall.CallId == negotiatedRef.CallId &&
                    acceptRetryAfterAccept.AcceptCall.PhoneCall.State == P.PhoneCallState.Accepted &&
                    acceptRetryAfterAccept.AcceptCall.PhoneCall.Protocol.MinLayer == 75,
                    "PG acceptCall retry after accept returns the same call and negotiated protocol");
                Check((await server.Rpc(Req(new P.GetStateRequest()), "bob")).GetState.Pts == negotiatedPts,
                    "PG request and accept retries do not advance pts");
                await server.Rpc(Req(new P.PhoneConfirmCallRequest
                {
                    Peer = negotiatedRef, GA = ByteString.CopyFrom(gA), KeyFingerprint = 0x1122334457,
                    Protocol = calleeProposal
                }), "alice", 409, "CALL_PROTOCOL_MISMATCH");
                var negotiatedConfirmed = await server.Rpc(Req(new P.PhoneConfirmCallRequest
                {
                    Peer = negotiatedRef, GA = ByteString.CopyFrom(gA), KeyFingerprint = 0x1122334457,
                    Protocol = callerProposal
                }));
                Check(negotiatedConfirmed.ConfirmCall.PhoneCall.Protocol.MinLayer == 75 &&
                    negotiatedConfirmed.ConfirmCall.PhoneCall.Protocol.MaxLayer == 95 &&
                    negotiatedConfirmed.ConfirmCall.PhoneCall.Protocol.LibraryVersions.SequenceEqual(new[] { "10.0.0" }),
                    "PG confirm keeps the negotiated protocol");
                var activePts = (await server.Rpc(Req(new P.GetStateRequest()), "bob")).GetState.Pts;
                var confirmRetry = await server.Rpc(Req(new P.PhoneConfirmCallRequest
                {
                    Peer = negotiatedRef, GA = ByteString.CopyFrom(gA), KeyFingerprint = 0x1122334457,
                    Protocol = callerProposal
                }));
                Check(confirmRetry.ConfirmCall.PhoneCall.State == P.PhoneCallState.Active &&
                    confirmRetry.ConfirmCall.PhoneCall.CallId == negotiatedRef.CallId,
                    "PG confirmCall retry after activation returns the same active call");
                var acceptRetryAfterConfirm = await server.Rpc(Req(new P.PhoneAcceptCallRequest
                {
                    Peer = negotiatedRef, GB = ByteString.CopyFrom(gB), Protocol = calleeProposal
                }), "bob");
                Check(acceptRetryAfterConfirm.AcceptCall.PhoneCall.State == P.PhoneCallState.Active,
                    "PG acceptCall retry after activation converges on the active call");
                var requestRetryAfterConfirm = await server.Rpc(Req(new P.PhoneRequestCallRequest
                {
                    User = Selector("bob"), RandomId = 777000333, Video = false,
                    GAHash = ByteString.CopyFrom(System.Security.Cryptography.SHA256.HashData(gA)), Protocol = callerProposal
                }));
                Check(requestRetryAfterConfirm.RequestCall.PhoneCall.State == P.PhoneCallState.Active,
                    "PG requestCall retry after activation converges on the active call");
                Check((await server.Rpc(Req(new P.GetStateRequest()), "bob")).GetState.Pts == activePts,
                    "PG confirm and accept retries do not advance pts");
                await server.Rpc(Req(new P.PhoneDiscardCallRequest
                {
                    Peer = negotiatedRef, Duration = 1, Reason = P.PhoneCallDiscardReason.Hangup
                }), "bob");
                var terminalPts = (await server.Rpc(Req(new P.GetStateRequest()), "bob")).GetState.Pts;
                var terminalRequestRetry = await server.Rpc(Req(new P.PhoneRequestCallRequest
                {
                    User = Selector("bob"), RandomId = 777000333, Video = false,
                    GAHash = ByteString.CopyFrom(System.Security.Cryptography.SHA256.HashData(gA)), Protocol = callerProposal
                }));
                Check(terminalRequestRetry.RequestCall.PhoneCall.State == P.PhoneCallState.Discarded &&
                    terminalRequestRetry.RequestCall.PhoneCall.CallId == negotiatedRef.CallId,
                    "PG requestCall retry after hangup returns the stable terminal state");
                await server.Rpc(Req(new P.PhoneAcceptCallRequest
                {
                    Peer = negotiatedRef, GB = ByteString.CopyFrom(gB), Protocol = calleeProposal
                }), "bob", 400, "CALL_ALREADY_ACCEPTED");
                Check((await server.Rpc(Req(new P.GetStateRequest()), "bob")).GetState.Pts == terminalPts,
                    "PG terminal retries do not revive the call or advance pts");

                var emptyVersions = new P.PhoneCallProtocol { UdpP2P = true, UdpReflector = true, MinLayer = 65, MaxLayer = 92 };
                await server.Rpc(Req(new P.PhoneRequestCallRequest
                {
                    User = Selector("bob"), RandomId = 777000334, Video = false,
                    GAHash = ByteString.CopyFrom(System.Security.Cryptography.SHA256.HashData(gA)), Protocol = emptyVersions
                }), "alice", 400, "CALL_INVALID_PROTOCOL_LIBRARY");
                var zeroLayer = new P.PhoneCallProtocol { UdpP2P = true, UdpReflector = true, MinLayer = 0, MaxLayer = 92 };
                zeroLayer.LibraryVersions.Add("10.0.0");
                await server.Rpc(Req(new P.PhoneRequestCallRequest
                {
                    User = Selector("bob"), RandomId = 777000335, Video = false,
                    GAHash = ByteString.CopyFrom(System.Security.Cryptography.SHA256.HashData(gA)), Protocol = zeroLayer
                }), "alice", 400, "CALL_INVALID_PROTOCOL_LAYERS");
                var hugeLayer = new P.PhoneCallProtocol { UdpP2P = true, UdpReflector = true, MinLayer = 65, MaxLayer = 1001 };
                hugeLayer.LibraryVersions.Add("10.0.0");
                await server.Rpc(Req(new P.PhoneRequestCallRequest
                {
                    User = Selector("bob"), RandomId = 777000336, Video = false,
                    GAHash = ByteString.CopyFrom(System.Security.Cryptography.SHA256.HashData(gA)), Protocol = hugeLayer
                }), "alice", 400, "CALL_INVALID_PROTOCOL_LAYERS");

                var incompatibleProtocol = new P.PhoneCallProtocol
                {
                    UdpP2P = true, UdpReflector = true, MinLayer = 90, MaxLayer = 95,
                    LibraryVersions = { "10.0.0" }
                };
                var incompatibleRequest = await server.Rpc(Req(new P.PhoneRequestCallRequest
                {
                    User = Selector("bob"), RandomId = 777000337, Video = false,
                    GAHash = ByteString.CopyFrom(System.Security.Cryptography.SHA256.HashData(gA)),
                    Protocol = incompatibleProtocol
                }));
                var incompatibleRef = new P.PhoneCallRef
                {
                    CallId = incompatibleRequest.RequestCall.PhoneCall.CallId,
                    AccessHash = incompatibleRequest.RequestCall.PhoneCall.AccessHash
                };
                await server.Rpc(Req(new P.PhoneAcceptCallRequest
                {
                    Peer = incompatibleRef, GB = ByteString.CopyFrom(gB),
                    Protocol = new P.PhoneCallProtocol
                    {
                        UdpP2P = true, UdpReflector = true, MinLayer = 60, MaxLayer = 80,
                        LibraryVersions = { "10.0.0" }
                    }
                }), "bob", 409, "CALL_PROTOCOL_INCOMPATIBLE");
                await server.Rpc(Req(new P.PhoneDiscardCallRequest
                {
                    Peer = incompatibleRef, Duration = 0, Reason = P.PhoneCallDiscardReason.Hangup
                }), "alice");

                // Profile capability fields must follow the same privacy decisions as call entry.
                await server.Rpc(Req(new P.AccountPrivacyRequest
                {
                    SetRules = new() { Key = P.PrivacyKey.PhoneCall,
                        Rules = { new P.PrivacyRule { Kind = P.PrivacyRuleKind.AllowAll } } }
                }), "bob");
                await server.Rpc(Req(new P.AccountPrivacyRequest
                {
                    SetRules = new() { Key = P.PrivacyKey.PhoneP2P,
                        Rules = { new P.PrivacyRule { Kind = P.PrivacyRuleKind.AllowAll } } }
                }), "bob");
                var capability = await server.Rpc(Req(new P.GetFullUserRequest { User = Selector("bob") }), "alice");
                Check(capability.GetFullUser.PhoneCallsAvailable && capability.GetFullUser.VideoCallsAvailable &&
                    !capability.GetFullUser.PhoneCallsPrivate, "profile call capability follows allow-all privacy");
                await server.Rpc(Req(new P.AccountPrivacyRequest
                {
                    SetRules = new() { Key = P.PrivacyKey.PhoneCall,
                        Rules = { new P.PrivacyRule { Kind = P.PrivacyRuleKind.DisallowAll } } }
                }), "bob");
                capability = await server.Rpc(Req(new P.GetFullUserRequest { User = Selector("bob") }), "alice");
                Check(!capability.GetFullUser.PhoneCallsAvailable && !capability.GetFullUser.VideoCallsAvailable,
                    "PhoneCall privacy disables phone and video capability");
                await server.Rpc(Req(new P.AccountPrivacyRequest
                {
                    SetRules = new() { Key = P.PrivacyKey.PhoneP2P,
                        Rules = { new P.PrivacyRule { Kind = P.PrivacyRuleKind.DisallowAll } } }
                }), "bob");
                capability = await server.Rpc(Req(new P.GetFullUserRequest { User = Selector("bob") }), "alice");
                Check(capability.GetFullUser.PhoneCallsPrivate, "PhoneP2P privacy is reported separately");
                await server.Rpc(Req(new P.AccountPrivacyRequest
                {
                    SetRules = new() { Key = P.PrivacyKey.PhoneCall,
                        Rules = { new P.PrivacyRule { Kind = P.PrivacyRuleKind.AllowAll } } }
                }), "bob");

                await server.Json("GET", "/api/events?after=0", null, Accounts["bob"].Token);
                for (int i = 0; i < 105; i++)
                    await server.Rpc(Send(899_000 + i, "WSS page " + i));
                var firstPage = await server.Json("GET", "/api/events?after=0&limit=100", null, Accounts["alice"].Token);
                var firstCursor = Long(firstPage, "cursor");
                var nextPage = await server.Json("GET", $"/api/events?after={firstCursor}&limit=100", null, Accounts["alice"].Token);
                var nextCursor = Long(nextPage, "cursor");
                Check(Arr(firstPage, "events").Count == 100 && Arr(nextPage, "events").Count > 0 &&
                    Arr(nextPage, "events").Count < 100 && nextCursor > firstCursor,
                    "PG event fixture spans a full page and partial page");
                Check(!Arr(firstPage, "events").Select(e => Long(e!, "id"))
                    .Intersect(Arr(nextPage, "events").Select(e => Long(e!, "id"))).Any(),
                    "PG event pages do not overlap");
                using var socket = LocalSocket();
                socket.Options.SetRequestHeader("Authorization", "Bearer " + Accounts["alice"].Token);
                socket.Options.AddSubProtocol("botgram.event-hint.v1.protobuf");
                using var deadline = new CancellationTokenSource(TimeSpan.FromSeconds(10));
                await socket.ConnectAsync(server.WebSocketUri(0), deadline.Token);
                var first = await Hint(socket);
                Check(first.HasWallpaperRevision && first.WallpaperRevision == 0 && first.Cursor == firstCursor,
                    "PG WSS first page matches event cursor and has explicit revision");
                var next = await Hint(socket);
                Check(next.Cursor == nextCursor && next.HasWallpaperRevision && next.WallpaperRevision == 0,
                    "PG WSS does not skip the second event page");
                using (var resumed = LocalSocket())
                {
                    resumed.Options.SetRequestHeader("Authorization", "Bearer " + Accounts["alice"].Token);
                    resumed.Options.AddSubProtocol("botgram.event-hint.v1.protobuf");
                    using var reconnect = new CancellationTokenSource(TimeSpan.FromSeconds(10));
                    await resumed.ConnectAsync(server.WebSocketUri(firstCursor), reconnect.Token);
                    Check((await Hint(resumed)).Cursor == nextCursor, "PG WSS reconnect resumes from saved page cursor");
                    resumed.Abort();
                }
                P.Wallpaper black = new() { Id = 0, NoFile = new() { Settings = new() { BackgroundColor = 0 } } };
                await server.Rpc(Correlate(new() { SaveWallpaper = new() { Wallpaper = black } }));
                await server.Rpc(Correlate(new() { InstallWallpaper = new() { Wallpaper = black } }));
                var changed = await server.Rpc(Correlate(new() { SetChatWallpaper = new() { Peer = Peer("bob"), Wallpaper = black } }));
                wallpaperRevision = changed.SetChatWallpaper.Revision;
                P.EventHint hint;
                do { hint = await Hint(socket); } while (hint.WallpaperRevision < wallpaperRevision);
                Check(hint.Cursor > first.Cursor, "wallpaper wake has independent event cursor");
                var list = (await server.Rpc(Correlate(new() { GetWallpapers = new() }))).GetWallpapers;
                Check(list.Revision == wallpaperRevision && list.Installed.Wallpaper.Equals(black), "queryable wallpaper recovery");
                await server.Json("POST", "/api/auth/logout", new { }, Accounts["alice"].Token, 204);
                using var revoked = new CancellationTokenSource(TimeSpan.FromSeconds(10));
                var receive = new byte[1024];
                WebSocketReceiveResult close;
                do { close = await socket.ReceiveAsync(new ArraySegment<byte>(receive), revoked.Token); }
                while (close.MessageType != WebSocketMessageType.Close);
                Check(close.CloseStatusDescription == "session_expired", "PG WSS session revocation");
                await server.Raw(Req(new P.GetStateRequest()).ToByteArray(), 401, "session_expired", Accounts["alice"].Token);
                Check(!Directory.Exists(Path.GetDirectoryName(noStore)), "PG HTTP resolved or initialized legacy Store");
                Check(await Scalar("SELECT to_regclass('botgram_validation_v1.state')::text") is DBNull, "PG HTTP created snapshot bridge");
                if (stack is not null) await FourComponentPipeline(server, source, stack, messageId);
            }
            if (stack is not null) await FourComponentRebuild(source, stack, messageId);
            await using (var server = await Start())
            {
                var login = await server.Json("POST", "/api/auth/login", new { username = "alice", password = "isolated-password" });
                Accounts["alice"] = Accounts["alice"] with { Token = Str(login, "token") };
                Check((await server.Rpc(Send(987654321, "PG HTTP"))).SendMessage.Message.MessageId == messageId, "PG restart permanent dedupe");
                Check((await server.Rpc(Correlate(new() { GetWallpapers = new() }))).GetWallpapers.Revision == wallpaperRevision,
                    "PG restart/dropped hints revision recovery");
                Check((await server.Rpc(Req(new P.GetFullUserRequest { User = Selector("bob") }))).GetFullUser.Wallpaper.Wallpaper.NoFile.Settings.HasBackgroundColor,
                    "PG restart full wallpaper recovery");
                Check(!Directory.Exists(Path.GetDirectoryName(noStore)), "PG restart touched Store");
                if (stack is not null)
                {
                    await server.Rpc(Req(new P.GetHistoryRequest { Peer = Peer("bob"), Limit = 100 }));
                    await FourComponentDrain(server);
                    Check((await server.Json("GET", "/health", null))["fullStackReady"]!.GetValue<bool>(),
                        "four-component host recovers after restart and rebuild");
                }
            }
            Console.WriteLine(fullStack ? $"PASS: {assertions} real four-component HTTP/WSS assertions, delivery, cache, duplicate and rebuild/restart"
                : $"PASS: {assertions} isolated PG HTTP/WSS assertions; NOT Kafka/Scylla/Redis or complete P7 acceptance");
            return 0;
        }
        finally
        {
            try { if (stack is not null) await FourComponentCleanup(stack); }
            finally
            {
                await using var drop = admin.CreateCommand("DROP DATABASE " + database);
                await drop.ExecuteNonQueryAsync();
            }
        }
    }
}
