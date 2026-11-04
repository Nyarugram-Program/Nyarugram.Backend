using System.Diagnostics;
using System.Net;
using System.Net.Http.Headers;
using System.Net.Sockets;
using System.Net.WebSockets;
using System.Text.Json;
using System.Text.Json.Nodes;
using Google.Protobuf;
using Npgsql;
using P = Botgram.Protocol.V1;

internal static partial class HttpTests
{
    private const string Media = "application/x-protobuf";
    private const string Domain = "isolated.test";
    private static readonly string Root = Path.Combine(Environment.GetEnvironmentVariable("BOTGRAM_TEST_ROOT") ?? "D:/BotgramBuild/tmp", "p6-http-runs-" + Guid.NewGuid().ToString("N"));
    private static readonly Dictionary<string, Account> Accounts = [];
    private static readonly Dictionary<string, JsonNode> Snapshots = [];
    private static readonly Dictionary<string, JsonNode> Semantics = [];
    private static readonly HashSet<P.RpcRequest.OperationOneofCase> Operations = [];
    private static readonly Dictionary<string, long> GroupIds = [];
    private static int assertions;
    private static string BackendDll => Environment.GetEnvironmentVariable("BOTGRAM_TEST_DLL") ?? typeof(Botgram.Backend.Store).Assembly.Location;
    private sealed record Account(string Id, string Token, long TlId, string Gid);

    public static async Task<int> Main(string[] args)
    {
        Directory.CreateDirectory(Root);
        try
        {
            if (args.SequenceEqual(new[] { "--privacy-security" })) { await PrivacySecurity(); return 0; }
            if (args.SequenceEqual(new[] { "--storage-parity" })) return await StorageParity();
            if (args.SequenceEqual(new[] { "--relational-backend" })) return await RelationalBackend();
            if (args.SequenceEqual(new[] { "--four-component" })) return await RelationalBackend(fullStack: true);
            if (args.SequenceEqual(new[] { "--four-component-tls" })) return await FourComponentTls();
            if (args.SequenceEqual(new[] { "--benchmark" })) return await Benchmark();
            if (args.SequenceEqual(new[] { "--joint-tls" })) return await JointTls();
            if (args.SequenceEqual(new[] { "--joint-proxy-tls" })) return await JointTls(proxy: true);
            if (args.SequenceEqual(new[] { "--benchmark-tls" })) return await BenchmarkTls();
            if (args.SequenceEqual(new[] { "--certificate-security" })) return await CertificateSecurity();
            if (args.SequenceEqual(new[] { "--mapper-profile" })) return await MapperProfile();
            if (args.SequenceEqual(new[] { "--retirement" })) return await Retirement();
            if (args.Length != 0) throw new ArgumentException("Expected no arguments, --benchmark, --joint-tls, --joint-proxy-tls, --benchmark-tls, --certificate-security, --mapper-profile or --retirement");
            var baseline = Path.Combine(Root, "baseline.json");
            await using (var server = await Server.Start(baseline))
            {
                foreach (var name in new[] { "alice", "bob", "outsider" })
                {
                    var registration = await server.Json("POST", "/api/auth/register", new { username = name, displayName = name + " Test", password = "isolated-test-password" });
                    var token = registration["token"]!.GetValue<string>();
                    var me = await server.Json("GET", "/api/telegram/me", null, token);
                    Accounts.Add(name, new(registration["user"]!["id"]!.GetValue<string>(), token, Long(me, "id"), me["phone"]!.GetValue<string>()));
                }
                Check(Accounts.Values.Select(a => a.TlId).Distinct().Count() == 3, "baseline stable TL IDs");
            }
            foreach (var proto in new[] { false, true })
            {
                var path = Path.Combine(Root, proto ? "proto.json" : "json.json");
                File.Copy(baseline, path);
                await using var server = await Server.Start(path);
                await Scenario(server, proto);
            }
            Check(Operations.Count == 21, "all 21 operations executed");
            var protoPath = Path.Combine(Root, "proto.json");
            await Bootstrap(protoPath);
            await ProtocolEvidence(protoPath);
            await using (var server = await Server.Start(protoPath)) await NegativeAndPagination(server);
            await using (var server = await Server.Start(protoPath))
            {
                var retry = await server.Rpc(Send(long.MinValue, "hello 😀", true));
                Check(retry.SendMessage.Message.MessageId == 1, "restart retry retains original message ID");
                Check(Load(protoPath)["messages"]!.AsArray().Count(m => m!["clientMessageId"]!.GetValue<string>() == "tl:" + long.MinValue) == 1, "restart retry no duplicate");
            }
            await CapabilityNegotiation(baseline);
            await ReadHistoryBoundaries(baseline);
            await WebSocketRegression(baseline);
            await PremiumRegression();
            Console.WriteLine($"PASS: {assertions} assertions; 21 TL operations + transport-only identity bootstrap; isolated snapshots at {Root}");
            Console.WriteLine("Legacy getMessages deliberately uses object {id:id}; this does not validate real Android's integer-list request. Cancellation proves retry single execution, not that an aborted request never commits.");
            return 0;
        }
        catch (Exception ex)
        {
            Console.Error.WriteLine($"FAIL: {ex.GetType().Name}: {ex.Message}\n{ex.StackTrace}\nIsolated test directory: {Root}");
            return 1;
        }
    }

    private sealed record BenchmarkSample(string Protocol, string Operation, int RunOrder, int Iteration, bool Warmup,
        double HttpMilliseconds, int RequestBodyBytes, int ResponseBodyBytes, int Status,
        double BackendCpuMilliseconds, long WorkingSetBytes, long PrivateMemoryBytes);

    private static async Task<int> Benchmark()
    {
        const int warmup = 5, samples = 40;
        const string text = "benchmark fixed message 😀";
        var baseline = Path.Combine(Root, "benchmark-baseline.json");
        await using (var server = await Server.Start(baseline))
        {
            foreach (var name in new[] { "alice", "bob" })
            {
                var registration = await server.Json("POST", "/api/auth/register", new { username = name, displayName = name + " Test", password = "isolated-test-password" });
                var token = registration["token"]!.GetValue<string>();
                var me = await server.Json("GET", "/api/telegram/me", null, token);
                Accounts.Add(name, new(registration["user"]!["id"]!.GetValue<string>(), token, Long(me, "id"), me["phone"]!.GetValue<string>()));
            }
            Check(Accounts["alice"].TlId != Accounts["bob"].TlId, "benchmark distinct stable TL identities");
            for (int i = 0; i < 100; i++)
                await server.Rpc(Send(long.MinValue + i, text));
        }
        Check(Arr(Load(baseline), "messages").Count == 100, "benchmark baseline has 100 messages");
        var raw = new List<BenchmarkSample>();
        var summaries = new List<object>();
        var operations = new[] { "GetState", "GetHistory", "SendMessage" };
        int order = 0;
        for (int operationIndex = 0; operationIndex < operations.Length; operationIndex++)
        {
            var operation = operations[operationIndex];
            // Alternate which transport runs first; each operation gets its own unchanged baseline copy.
            foreach (var proto in operationIndex % 2 == 0 ? new[] { false, true } : new[] { true, false })
            {
                var protocol = proto ? "protobuf" : "json";
                var path = Path.Combine(Root, $"benchmark-{protocol}-{operation}.json");
                File.Copy(baseline, path);
                order++;
                await using (var server = await Server.Start(path))
                {
                    for (int i = 0; i < warmup + samples; i++)
                    {
                        long key = long.MaxValue - i;
                        var request = operation switch
                        {
                            "GetState" => Req(new P.GetStateRequest()),
                            "GetHistory" => Req(new P.GetHistoryRequest { Peer = Peer("bob"), Limit = 100 }),
                            _ => Send(key, text)
                        };
                        var legacy = operation switch
                        {
                            "GetState" => T("TL_updates_getState"),
                            "GetHistory" => T("TL_messages_getHistory", ("peer", PeerJ(Accounts["bob"])), ("limit", 100)),
                            _ => T("TL_messages_sendMessage", ("peer", PeerJ(Accounts["bob"])), ("random_id", key), ("message", text))
                        };
                        var bytes = proto ? request.ToByteArray() : System.Text.Encoding.UTF8.GetBytes(legacy.ToJsonString());
                        var result = await server.BenchmarkRequest(proto, bytes);
                        Check(result.Status == 200, "benchmark " + protocol + " " + operation + " HTTP " + result.Status);
                        Check(result.ContentType == (proto ? Media : "application/json"), "benchmark response media");
                        if (proto)
                        {
                            var parsed = P.RpcResponse.Parser.ParseFrom(result.Body);
                            Check(parsed.ProtocolMajor == 1 && parsed.RequestId == request.RequestId, "benchmark protobuf envelope correlation");
                            Check((int)parsed.OutcomeCase == (int)request.OperationCase, "benchmark protobuf success oneof");
                            if (operation == "GetState") Check(parsed.GetState.Pts == 101 && parsed.GetState.DateMs > 0, "benchmark protobuf state");
                            else if (operation == "GetHistory")
                            {
                                Check(parsed.GetHistory.Messages.Count == 100 && parsed.GetHistory.Messages.Select(m => m.MessageId).Order().SequenceEqual(Enumerable.Range(1, 100).Select(n => (long)n)), "benchmark protobuf history IDs");
                                Check(parsed.GetHistory.Messages.All(m => m.Text?.Text == text && m.Sender.UserId == Accounts["alice"].Id && m.Peer.User.UserId == Accounts["bob"].Id), "benchmark protobuf history content");
                            }
                            else
                            {
                                var sent = parsed.SendMessage;
                                Check(sent.Mapping.RandomId == key && sent.Mapping.MessageId == 101 + i && sent.Message.MessageId == 101 + i && sent.Message.Text?.Text == text, "benchmark protobuf send mapping and content");
                                Check(sent.Updates.Updates_.Count == 2 && sent.Updates.Updates_[0].MessageId?.RandomId == key && sent.Updates.Updates_[1].NewMessage?.Message.MessageId == 101 + i, "benchmark protobuf send updates");
                            }
                        }
                        else
                        {
                            var parsed = JsonNode.Parse(result.Body)!;
                            var expectedType = operation switch { "GetState" => "TL_updates_state", "GetHistory" => "TL_messages_messages", _ => "TL_updates" };
                            Check(Str(parsed, "$type") == "org.telegram.tgnet.TLRPC$" + expectedType, "benchmark JSON success type");
                            if (operation == "GetState") Check(Long(parsed, "pts") == 101 && Long(parsed, "date") > 0, "benchmark JSON state");
                            else if (operation == "GetHistory")
                            {
                                var messages = Arr(parsed, "messages");
                                Check(messages.Count == 100 && messages.Select(m => Long(m!, "id")).Order().SequenceEqual(Enumerable.Range(1, 100).Select(n => (long)n)), "benchmark JSON history IDs");
                                Check(messages.All(m => Str(m!, "message") == text && Long(m!["from_id"]!, "user_id") == Accounts["alice"].TlId && Long(m["peer_id"]!, "user_id") == Accounts["bob"].TlId), "benchmark JSON history content");
                            }
                            else
                            {
                                var updates = Arr(parsed, "updates");
                                Check(updates.Count == 2 && Str(updates[0]!, "$type").EndsWith("$TL_updateMessageID", StringComparison.Ordinal) && Long(updates[0]!, "random_id") == key && Long(updates[0]!, "id") == 101 + i, "benchmark JSON send mapping");
                                Check(Str(updates[1]!, "$type").EndsWith("$TL_updateNewMessage", StringComparison.Ordinal) && Long(updates[1]!["message"]!, "id") == 101 + i && Str(updates[1]!["message"]!, "message") == text, "benchmark JSON send content");
                            }
                        }
                        raw.Add(new(protocol, operation, order, i, i < warmup, result.Milliseconds, bytes.Length, result.Body.Length,
                            result.Status, result.CpuMilliseconds, result.WorkingSetBytes, result.PrivateMemoryBytes));
                    }
                }
                var persisted = Load(path);
                var storedMessages = Arr(persisted, "messages");
                Check(storedMessages.Count == (operation == "SendMessage" ? 145 : 100), "benchmark single execution persisted count");
                if (operation == "SendMessage")
                    for (int i = 0; i < warmup + samples; i++)
                        Check(storedMessages.Count(m => Str(m!, "clientMessageId") == "tl:" + (long.MaxValue - i)) == 1, "benchmark each int64 key committed once");
                var measured = raw.Where(s => s.RunOrder == order && !s.Warmup).ToArray();
                var latency = measured.Select(s => s.HttpMilliseconds).Order().ToArray();
                summaries.Add(new
                {
                    protocol, operation, runOrder = order, warmupRequests = warmup, measuredRequests = samples,
                    meanHttpMilliseconds = latency.Average(), medianHttpMilliseconds = (latency[19] + latency[20]) / 2,
                    p95HttpMilliseconds = latency[(int)Math.Ceiling(samples * 0.95) - 1], minHttpMilliseconds = latency[0], maxHttpMilliseconds = latency[^1],
                    meanRequestBodyBytes = measured.Average(s => s.RequestBodyBytes), meanResponseBodyBytes = measured.Average(s => s.ResponseBodyBytes),
                    backendCpuMilliseconds = measured.Sum(s => s.BackendCpuMilliseconds),
                    meanWorkingSetBytes = measured.Average(s => s.WorkingSetBytes), maxWorkingSetBytes = measured.Max(s => s.WorkingSetBytes),
                    meanPrivateMemoryBytes = measured.Average(s => s.PrivateMemoryBytes), maxPrivateMemoryBytes = measured.Max(s => s.PrivateMemoryBytes),
                    statuses = measured.GroupBy(s => s.Status).ToDictionary(g => g.Key.ToString(), g => g.Count())
                });
            }
        }
        var options = new JsonSerializerOptions { WriteIndented = true };
        File.WriteAllText(Path.Combine(Root, "benchmark-samples.json"), JsonSerializer.Serialize(raw, options));
        File.WriteAllText(Path.Combine(Root, "benchmark.json"), JsonSerializer.Serialize(new
        {
            schemaVersion = 1, recordedAtUtc = DateTimeOffset.UtcNow, runtime = System.Runtime.InteropServices.RuntimeInformation.FrameworkDescription,
            os = System.Runtime.InteropServices.RuntimeInformation.OSDescription, processorCount = Environment.ProcessorCount,
            backendSha256 = Convert.ToHexString(System.Security.Cryptography.SHA256.HashData(File.ReadAllBytes(BackendDll))),
            baselineMessages = 100, warmupRequestsPerOperation = warmup, measuredRequestsPerOperation = samples,
            requestsPerMeasuredInstancePerUser = warmup + samples, baselineAliceRequests = 102,
            timing = "Stopwatch around SendAsync(ResponseHeadersRead) through full ReadAsByteArrayAsync; excludes encoding, validation and process sampling. Bytes are HTTP body bytes only, not headers or TCP framing.",
            cpu = "Backend process TotalProcessorTime delta bracketing each HTTP request; excludes validation and memory sampling, includes concurrent backend activity; OS accounting granularity may yield zero deltas.",
            memory = "Backend WorkingSet64 and PrivateMemorySize64 sampled after each complete response; not allocation counts or continuous peaks.",
            isolation = "Fresh stopped-baseline database copy and random loopback port per operation/transport, sequential requests, 45 unique identical int64 send keys per transport, no retries or limiter changes.",
            orderCaveat = "JSON first for GetState/SendMessage, protobuf first for GetHistory. Alternation reduces but does not eliminate JIT, filesystem cache, thermal and run-order bias; this is a local baseline, not a performance budget or pass claim.",
            rawSamples = "benchmark-samples.json", results = summaries
        }, options));
        Console.WriteLine($"Benchmark recorded without performance thresholds: {Path.Combine(Root, "benchmark.json")}; raw samples: {Path.Combine(Root, "benchmark-samples.json")}");
        return 0;
    }

    private static async Task Scenario(Server s, bool proto, bool capture = false)
    {
        var a = Accounts["alice"]; var b = Accounts["bob"];
        async Task<P.RpcResponse?> Step(string key, P.RpcRequest request, JsonObject legacy,
            Func<P.RpcResponse, JsonNode> fromProto, Func<JsonNode, JsonNode> fromJson, string actor = "alice")
        {
            P.RpcResponse? result = null;
            JsonNode semantic;
            if (proto)
            {
                result = await s.Rpc(request, actor);
                Operations.Add(request.OperationCase);
                semantic = fromProto(result);
            }
            else semantic = fromJson(await s.Json("POST", "/api/telegram/rpc", legacy, Accounts[actor].Token));
            var snapshot = NormalizeSnapshot(Load(s.DataPath));
            if (proto && !capture)
            {
                Equal(Semantics[key], semantic, key + " response semantics");
                Equal(Snapshots[key], snapshot, key + " complete persisted state");
            }
            else { Semantics.Add(key, semantic); Snapshots.Add(key, snapshot); }
            return result;
        }
        await Step("state", Req(new P.GetStateRequest()), T("TL_updates_getState"), r => State(r.GetState), StateJ);
        var sendJ = T("TL_messages_sendMessage", ("peer", PeerJ(b)), ("random_id", long.MinValue), ("message", "hello 😀"), ("silent", true),
            ("entities", new[] { T("TL_messageEntityBold", ("offset", 6), ("length", 2)) }));
        await Step("send", Send(long.MinValue, "hello 😀", true), sendJ, r => Sent(r.SendMessage), SentJ);
        await Step("retry-min", Send(long.MinValue, "hello 😀", true), sendJ, r => Sent(r.SendMessage), SentJ);
        var reply = Send(long.MaxValue, "reply"); reply.SendMessage.ReplyToMessageId = 1;
        var replyJ = T("TL_messages_sendMessage", ("peer", PeerJ(b)), ("random_id", long.MaxValue), ("message", "reply"),
            ("reply_to", T("TL_inputReplyToMessage", ("reply_to_msg_id", 1))));
        await Step("reply", reply, replyJ, r => Sent(r.SendMessage), SentJ);
        await Step("retry-max", reply, replyJ, r => Sent(r.SendMessage), SentJ);
        await Step("read", Req(new P.ReadHistoryRequest { Peer = Peer("alice"), MaxMessageId = 2 }),
            T("TL_messages_readHistory", ("peer", PeerJ(a)), ("max_id", 2)), r => Node(new { r.ReadHistory.Pts, r.ReadHistory.PtsCount }),
            j => Node(new { Pts = Long(j, "pts"), PtsCount = Long(j, "pts_count") }), "bob");
        await Step("read-noop", Req(new P.ReadHistoryRequest { Peer = Peer("alice"), MaxMessageId = 1 }),
            T("TL_messages_readHistory", ("peer", PeerJ(a)), ("max_id", 1)), r => Node(new { r.ReadHistory.Pts, r.ReadHistory.PtsCount }),
            j => Node(new { Pts = Long(j, "pts"), PtsCount = Long(j, "pts_count") }), "bob");
        await Step("import", Req(new P.ImportContactsRequest { Contacts = { new P.ImportContact { ClientId = long.MaxValue, Gid = b.Gid, FirstName = "Local", LastName = "Bob" } } }),
            T("TL_contacts_importContacts", ("contacts", new[] { T("TL_inputPhoneContact", ("client_id", long.MaxValue), ("phone", b.Gid), ("first_name", "Local"), ("last_name", "Bob")) })),
            r => Node(new { imported = r.ImportContacts.Imported.Select(i => new { id = Tl(i.User), clientId = i.ClientId }), users = r.ImportContacts.Users.Select(User), retry = r.ImportContacts.RetryContacts.ToArray(), popular = r.ImportContacts.PopularInvites.Count }),
            j => Node(new { imported = Arr(j, "imported").Select(i => new { id = Long(i!, "user_id"), clientId = Long(i!, "client_id") }), users = Arr(j, "users").Select(i => UserJ(i!)), retry = Arr(j, "retry_contacts").Select(i => i!.GetValue<long>()).ToArray(), popular = Arr(j, "popular_invites").Count }));
        await Step("create-group", Req(new P.CreateGroupRequest { Title = "isolated group", Users = { Selector("bob") } }),
            T("TL_messages_createChat", ("title", "isolated group"), ("users", new[] { PeerJ(b) })),
            r => Node(new { group = Group(r.CreateGroup.Group), updates = Updates(r.CreateGroup.Updates), missing = r.CreateGroup.MissingInvitees.Count }),
            j => Node(new { group = GroupJ(Arr(j["updates"]!, "chats")[0]!), updates = UpdatesJ(j["updates"]!), missing = 0 }));
        var group = Load(s.DataPath)["conversations"]!.AsArray().Single(c => c!["kind"]!.GetValue<string>() == "group")!["id"]!.GetValue<string>();
        var chatId = Load(s.DataPath)["telegramIds"]!["chat:" + group]!.GetValue<long>();
        await Step("dialogs", Req(new P.GetDialogsRequest()), T("TL_messages_getDialogs"), r => Dialogs(r.GetDialogs), j => DialogsJ(j, true));
        await Step("pinned", Req(new P.GetPinnedDialogsRequest()), T("TL_messages_getPinnedDialogs"), r => Peers(r.GetPinnedDialogs), j => DialogsJ(j, false));
        await Step("peer-dialogs", Req(new P.GetPeerDialogsRequest { Peers = { Peer("bob"), new P.PeerRef { GroupId = group } } }),
            T("TL_messages_getPeerDialogs", ("peers", new[] { new { peer = PeerJ(b) }, new { peer = T("TL_inputPeerChat", ("chat_id", chatId)) } })),
            r => Peers(r.GetPeerDialogs), j => DialogsJ(j, false));
        await Step("history", Req(new P.GetHistoryRequest { Peer = Peer("bob") }), T("TL_messages_getHistory", ("peer", PeerJ(b))), r => Page(r.GetHistory), PageJ);
        // The frozen JSON gateway reads object.id, not the actual Android integer-list shape.
        await Step("messages", Req(new P.GetMessagesRequest { MessageIds = { 1, 2, 3 } }), T("TL_messages_getMessages", ("id", new[] { new { id = 1 }, new { id = 2 }, new { id = 3 } })), r => Page(r.GetMessages), PageJ);
        await Step("search", Req(new P.SearchContactsRequest { Query = "bob" }), T("TL_contacts_search", ("q", "bob")),
            r => Node(new { results = r.SearchContacts.Results.Select(PeerId), users = r.SearchContacts.Users.Select(User), groups = r.SearchContacts.Groups.Select(Group), mine = r.SearchContacts.MyResults.Count }),
            j => Node(new { results = Arr(j, "results").Select(i => PeerIdJ(i!)), users = Arr(j, "users").Select(i => UserJ(i!)), groups = Array.Empty<object>(), mine = 0 }));
        await Step("username", Req(new P.ResolveUsernameRequest { Username = "bob" }), T("TL_contacts_resolveUsername", ("username", "bob")), r => Resolved(r.ResolveUsername), ResolvedJ);
        await Step("users", Req(new P.GetUsersRequest { Users = { new P.UserSelector { Self = true }, Selector("bob") } }),
            T("TL_users_getUsers", ("id", new[] { T("TL_inputUserSelf"), PeerJ(b) })), r => Node(r.GetUsers.Users.Select(User)), j => Node(Arr(j, "objects").Select(i => UserJ(i!))));
        await Step("contacts", Req(new P.GetContactsRequest()), T("TL_contacts_getContacts"),
            r => Node(new { count = r.GetContacts.SavedCount, contacts = r.GetContacts.Contacts.Select(c => new { id = Tl(c.User), mutual = c.Mutual }), users = r.GetContacts.Users.Select(User) }),
            j => Node(new { count = Long(j, "saved_count"), contacts = Arr(j, "contacts").Select(c => new { id = Long(c!, "user_id"), mutual = Bool(c!, "mutual") }), users = Arr(j, "users").Select(i => UserJ(i!)) }));
        await Step("gid", Req(new P.ResolveGidRequest { Gid = b.Gid }), T("TL_contacts_resolvePhone", ("phone", b.Gid)), r => Resolved(r.ResolveGid), ResolvedJ);
        await Step("peer-settings", Req(new P.GetPeerSettingsRequest { Peer = Peer("bob") }), T("TL_messages_getPeerSettings", ("peer", PeerJ(b))),
            r => Node(new { settings = r.GetPeerSettings.Settings.ToString(), users = r.GetPeerSettings.Users.Select(User), groups = r.GetPeerSettings.Groups.Select(Group) }),
            j => Node(new { settings = "{ }", users = Arr(j, "users").Select(i => UserJ(i!)), groups = Arr(j, "chats").Select(i => GroupJ(i!)) }));
        await Step("full-user", Req(new P.GetFullUserRequest { User = Selector("bob") }), T("TL_users_getFullUser", ("id", PeerJ(b))),
            r => Node(new { id = Tl(r.GetFullUser.User), about = r.GetFullUser.About, common = r.GetFullUser.CommonGroupsCount, available = r.GetFullUser.PhoneCallsAvailable, priv = r.GetFullUser.PhoneCallsPrivate, users = r.GetFullUser.Users.Select(User) }),
            j => Node(new { id = Long(j["full_user"]!, "id"), about = Str(j["full_user"]!, "about"), common = Long(j["full_user"]!, "common_chats_count"), available = Bool(j["full_user"]!, "phone_calls_available"), priv = Bool(j["full_user"]!, "phone_calls_private"), users = Arr(j, "users").Select(i => UserJ(i!)) }));
        await Step("full-group", Req(new P.GetFullGroupRequest { GroupId = group }), T("TL_messages_getFullChat", ("chat_id", chatId)),
            r => Node(new { group = Group(r.GetFullGroup.Group), about = r.GetFullGroup.About, version = r.GetFullGroup.ParticipantsVersion, members = r.GetFullGroup.Participants.Select(p => new { id = Tl(p.User), inviter = Tl(p.Inviter) }), users = r.GetFullGroup.Users.Select(User), groups = r.GetFullGroup.Groups.Select(Group) }),
            j => Node(new { group = GroupJ(Arr(j, "chats")[0]!), about = Str(j["full_chat"]!, "about"), version = Long(j["full_chat"]!["participants"]!, "version"), members = Arr(j["full_chat"]!["participants"]!, "participants").Select(p => new { id = Long(p!, "user_id"), inviter = Long(p!, "inviter_id") }), users = Arr(j, "users").Select(i => UserJ(i!)), groups = Arr(j, "chats").Select(i => GroupJ(i!)) }));
        await Step("groups", Req(new P.GetGroupsRequest { GroupIds = { group } }), T("TL_messages_getChats", ("id", new[] { chatId })), r => Node(r.GetGroups.Groups.Select(Group)), j => Node(Arr(j, "chats").Select(i => GroupJ(i!))));
        await Step("notify", Req(new P.GetNotifySettingsRequest { Target = new P.NotifyTarget { AllBroadcasts = new() } }), T("TL_account_getNotifySettings"),
            r => Node(new { preview = r.GetNotifySettings.HasShowPreviews, silent = r.GetNotifySettings.HasSilent, mute = r.GetNotifySettings.HasMuteUntilMs }), j => Node(new { preview = j["show_previews"] is not null, silent = j["silent"] is not null, mute = j["mute_until"] is not null }));
        await Step("difference", Req(new P.GetDifferenceRequest { Pts = 1 }), T("TL_updates_getDifference", ("pts", 1)), r => Difference(r.GetDifference), DifferenceJ);
        Check(Load(s.DataPath)["messages"]!.AsArray().Count == 3, "scenario message count with retries");
    }

    private static async Task CapabilityNegotiation(string sourcePath)
    {
        foreach (var (rollout, legacyEnabled) in new[] { (100, true), (0, true), (50, true), (100, false) })
        {
            var path = Path.Combine(Root, $"capabilities-{rollout}-{legacyEnabled}.json");
            File.Copy(sourcePath, path);
            await using var s = await Server.Start(path, legacyEnabled: legacyEnabled, rollout: rollout);
            var unauthorized = await s.Json("GET", "/api/protocol/capabilities", null, status: 401);
            Check(Str(unauthorized, "error") == "unauthorized", "capabilities requires bearer");
            var expired = await s.Json("GET", "/api/protocol/capabilities", null, "not-a-session", 401);
            Check(Str(expired, "error") == "session_expired", "capabilities rejects invalid session");
            var login = await s.Json("POST", "/api/auth/login", new { username = "alice", password = "isolated-test-password" });
            var secondToken = Str(login, "token");
            Check(secondToken != Accounts["alice"].Token, "capabilities uses distinct sessions for the same user");
            var before = Load(path);
            foreach (var token in Accounts.Values.Select(a => a.Token).Append(secondToken))
            {
                var tokenHash = Convert.ToHexString(System.Security.Cryptography.SHA256.HashData(System.Text.Encoding.UTF8.GetBytes(token)));
                var session = Arr(before, "sessions").Single(entry => Str(entry!, "tokenHash") == tokenHash)!;
                var cohort = System.Buffers.Binary.BinaryPrimitives.ReadUInt32BigEndian(
                    System.Security.Cryptography.SHA256.HashData(System.Text.Encoding.UTF8.GetBytes(Str(session, "tokenHash")))) % 100;
                var expected = cohort < rollout ? "protobuf-v1" : "tl-json-v1";
                var first = await s.Json("GET", "/api/protocol/capabilities", null, token);
                Check(Long(first, "negotiationVersion") == 1, "capabilities negotiation version");
                Check(Arr(first, "protocols").Select(p => p!.GetValue<string>()).SequenceEqual(
                    legacyEnabled ? new[] { "protobuf-v1", "tl-json-v1" } : new[] { "protobuf-v1" }), "capabilities advertises only enabled protocols");
                Check(Str(first, "selectedProtocol") == expected, $"capabilities session hash selection at {rollout} percent");
                Check(Arr(first, "capabilities").Select(c => c!.GetValue<string>()).SequenceEqual(new[]
                {
                    "session-protocol-pinning", "send-random-id-idempotency-v1", "difference-pts-v1", "wss-wake-v1"
                }), "capabilities supported features");
                Equal(first, await s.Json("GET", "/api/protocol/capabilities", null, token), "capabilities stable for the same bearer");
                Check(Arr(first, "eventSubprotocols").Select(p => p!.GetValue<string>()).SequenceEqual(new[]
                {
                    "botgram.event-hint.v1.protobuf", "botgram.events.v1.json"
                }), "capabilities advertises event subprotocols in preference order");
                Check(first.AsObject().Count == 5, "capabilities exposes no session fields");
            }
            Equal(before, Load(path), "capability negotiation does not mutate persisted state");
        }
    }

    private static async Task WebSocketRegression(string sourcePath)
    {
        const string jsonProtocol = "botgram.events.v1.json", hintProtocol = "botgram.event-hint.v1.protobuf";
        int beforeAssertions = assertions;
        var path = Path.Combine(Root, "websocket.json");
        File.Copy(sourcePath, path);
        await using var s = await Server.Start(path);

        async Task<ClientWebSocket> Connect(long after, string? token, params string[] protocols)
        {
            var socket = new ClientWebSocket();
            socket.Options.CollectHttpResponseDetails = true;
            if (tlsEnabled) socket.Options.RemoteCertificateValidationCallback = (_, cert, _, errors) => ValidateCertificate(cert, errors);
            if (token is not null) socket.Options.SetRequestHeader("Authorization", "Bearer " + token);
            foreach (var protocol in protocols) socket.Options.AddSubProtocol(protocol);
            using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(10));
            try
            {
                await socket.ConnectAsync(s.WebSocketUri(after), timeout.Token);
                Check(socket.State == WebSocketState.Open && socket.HttpStatusCode == HttpStatusCode.SwitchingProtocols, "WebSocket real loopback upgrade");
                if (tlsEnabled) Console.WriteLine($"WSS upgrade status=101 selected_subprotocol={socket.SubProtocol ?? "none"} after={after}");
                return socket;
            }
            catch { socket.Dispose(); throw; }
        }
        async Task Reject(long after, string? token, HttpStatusCode status, params string[] protocols)
        {
            using var socket = new ClientWebSocket();
            socket.Options.CollectHttpResponseDetails = true;
            if (tlsEnabled) socket.Options.RemoteCertificateValidationCallback = (_, cert, _, errors) => ValidateCertificate(cert, errors);
            if (token is not null) socket.Options.SetRequestHeader("Authorization", "Bearer " + token);
            foreach (var protocol in protocols) socket.Options.AddSubProtocol(protocol);
            using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(10));
            try
            {
                await socket.ConnectAsync(s.WebSocketUri(after), timeout.Token);
                throw new Exception("WebSocket unexpectedly accepted rejected handshake");
            }
            catch (WebSocketException)
            {
                Check(socket.HttpStatusCode == status && socket.State != WebSocketState.Open, "WebSocket handshake rejects with HTTP " + (int)status);
            }
        }
        async Task<(WebSocketMessageType Type, byte[] Body)> Receive(ClientWebSocket socket)
        {
            using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(8));
            using var body = new MemoryStream();
            var buffer = new byte[4096];
            WebSocketReceiveResult frame;
            do
            {
                frame = await socket.ReceiveAsync(new ArraySegment<byte>(buffer), timeout.Token);
                body.Write(buffer, 0, frame.Count);
                Check(body.Length <= 1024 * 1024, "WebSocket regression frame bounded");
            } while (!frame.EndOfMessage);
            if (tlsEnabled) Console.WriteLine($"WSS frame type={frame.MessageType} size={body.Length}");
            return (frame.MessageType, body.ToArray());
        }
        async Task<JsonNode> JsonPage(ClientWebSocket socket)
        {
            var frame = await Receive(socket);
            Check(frame.Type == WebSocketMessageType.Text, "JSON events use text frames");
            var page = JsonNode.Parse(frame.Body)!;
            Check(page.AsObject().Count == 2 && page["events"] is JsonArray && page["cursor"] is JsonValue, "JSON retains EventPage shape");
            Check(Arr(page, "events").Count <= 100, "JSON event page limited to 100");
            if (tlsEnabled) Console.WriteLine($"WSS JSON cursor={Long(page, "cursor")} events={Arr(page, "events").Count}");
            return page;
        }
        async Task<long> Hint(ClientWebSocket socket)
        {
            var frame = await Receive(socket);
            Check(frame.Type == WebSocketMessageType.Binary, "protobuf hints use binary frames");
            var hint = P.EventHint.Parser.ParseFrom(frame.Body);
            Check(hint.ProtocolMajor == 1 && hint.Cursor >= 0, "protobuf EventHint major and int64 cursor");
            Check(frame.Body.SequenceEqual(new P.EventHint { ProtocolMajor = 1, Cursor = hint.Cursor }.ToByteArray()), "protobuf hint contains only major and cursor, no event payload or unknown fields");
            if (tlsEnabled) Console.WriteLine($"WSS hint cursor={hint.Cursor}");
            return hint.Cursor;
        }
        async Task Closed(ClientWebSocket socket, string reason)
        {
            try
            {
                var frame = await Receive(socket);
                Check(frame.Type == WebSocketMessageType.Close, reason);
            }
            catch (WebSocketException ex) when (ex.WebSocketErrorCode == WebSocketError.ConnectionClosedPrematurely)
            {
                Check(socket.State == WebSocketState.Aborted, reason + " (transport disconnected without close frame)");
            }
        }

        await Reject(0, null, HttpStatusCode.Unauthorized, hintProtocol);
        await Reject(0, "not-a-session", HttpStatusCode.Unauthorized, jsonProtocol);
        await Reject(0, Accounts["bob"].Token, HttpStatusCode.BadRequest, "botgram.unknown.v9");
        await Reject(-1, Accounts["bob"].Token, HttpStatusCode.BadRequest, hintProtocol);
        var startPts = (await s.Rpc(Req(new P.GetStateRequest()), "bob")).GetState.Pts;
        var expectedMessages = new List<P.Message>();
        for (int i = 0; i < 105; i++) expectedMessages.Add((await s.Rpc(Send(70000 + i, "ws message " + i))).SendMessage.Message);
        const string privateText = "outsider private WebSocket sentinel";
        var privateRequest = Send(71000, privateText);
        privateRequest.SendMessage.Peer = Peer("outsider");
        await s.Rpc(privateRequest, "outsider");
        var first = await s.Json("GET", "/api/events?after=0&limit=100", null, Accounts["bob"].Token);
        var firstCursor = Long(first, "cursor");
        var second = await s.Json("GET", $"/api/events?after={firstCursor}&limit=100", null, Accounts["bob"].Token);
        var endCursor = Long(second, "cursor");
        Check(Arr(first, "events").Count == 100 && Arr(second, "events").Count > 0 && Arr(second, "events").Count < 100 && endCursor > firstCursor, "WebSocket fixture exceeds 100 visible events with a partial final page");
        Check(!Arr(first, "events").Select(e => Long(e!, "id")).Intersect(Arr(second, "events").Select(e => Long(e!, "id"))).Any(), "event pages have disjoint IDs");

        using (var legacy = await Connect(0, Accounts["bob"].Token))
        using (var json = await Connect(0, Accounts["bob"].Token, jsonProtocol))
        using (var proto = await Connect(0, Accounts["bob"].Token, jsonProtocol, hintProtocol))
        {
            Check(string.IsNullOrEmpty(legacy.SubProtocol) && json.SubProtocol == jsonProtocol && proto.SubProtocol == hintProtocol, "legacy fallback, explicit JSON and protobuf priority negotiation");
            foreach (var expected in new[] { first, second })
            {
                Equal(expected, await JsonPage(legacy), "legacy WebSocket matches original EventPage");
                Equal(expected, await JsonPage(json), "negotiated JSON WebSocket matches original EventPage");
                Check(await Hint(proto) == Long(expected, "cursor"), "JSON and protobuf pagination cursors agree");
                Check(!expected.ToJsonString().Contains(privateText, StringComparison.Ordinal), "other user's event payload absent from JSON pages");
            }
            legacy.Abort(); json.Abort(); proto.Abort();
        }
        using (var json = await Connect(firstCursor, Accounts["bob"].Token, jsonProtocol))
        using (var proto = await Connect(firstCursor, Accounts["bob"].Token, hintProtocol))
        {
            Equal(second, await JsonPage(json), "JSON reconnect resumes exclusively after saved cursor");
            Check(await Hint(proto) == endCursor, "protobuf reconnect resumes after saved cursor");
            // Intentionally discard this wake-up without advancing the saved TL pts.
            json.Abort(); proto.Abort();
        }
        using (var json = await Connect(0, Accounts["alice"].Token, jsonProtocol))
        using (var proto = await Connect(0, Accounts["alice"].Token, hintProtocol))
        {
            var page = await JsonPage(json);
            Check(await Hint(proto) == Long(page, "cursor"), "sender JSON and hint cursor parity");
            page = await JsonPage(json);
            Check(await Hint(proto) == Long(page, "cursor"), "sender final page cursor parity");
            Check(!page.ToJsonString().Contains(privateText, StringComparison.Ordinal), "nonrecipient cannot read outsider-only payload");
            json.Abort(); proto.Abort();
        }
        // These messages arrive while disconnected, so neither wake-up can be consumed.
        for (int i = 0; i < 2; i++) expectedMessages.Add((await s.Rpc(Send(72000 + i, "ws offline " + i))).SendMessage.Message);
        var afterOffline = await s.Json("GET", $"/api/events?after={endCursor}&limit=100", null, Accounts["bob"].Token);
        var latestCursor = Long(afterOffline, "cursor");
        using (var json = await Connect(endCursor, Accounts["bob"].Token, jsonProtocol))
        using (var proto = await Connect(endCursor, Accounts["bob"].Token, hintProtocol))
        {
            Equal(afterOffline, await JsonPage(json), "reconnect replays events produced while offline");
            Check(await Hint(proto) == latestCursor && latestCursor > endCursor, "reconnect hint advances for missed events");
            json.Abort(); proto.Abort();
        }
        var recovered = new List<P.Message>();
        var pts = startPts;
        int differencePages = 0;
        while (true)
        {
            var difference = (await s.Rpc(Req(new P.GetDifferenceRequest { Pts = pts }), "bob")).GetDifference;
            differencePages++;
            if (difference.Slice is not null)
            {
                Check(difference.Slice.Messages.Count == 100 && difference.Slice.State.Pts == pts + 100, "missed hints recover first 100 messages with original difference pts");
                recovered.AddRange(difference.Slice.Messages);
                pts = difference.Slice.State.Pts;
            }
            else
            {
                Check(difference.Complete is not null, "missed hints finish with complete difference");
                recovered.AddRange(difference.Complete!.Messages);
                pts = difference.Complete.State.Pts;
                break;
            }
            Check(differencePages < 3, "difference catch-up has bounded pages");
        }
        Check(differencePages == 2 && pts == startPts + expectedMessages.Count, "discarded and lost hints preserve complete two-page pts catch-up");
        Check(recovered.Select(m => m.MessageId).SequenceEqual(expectedMessages.Select(m => m.MessageId)) && recovered.Select(m => m.Text.Text).SequenceEqual(expectedMessages.Select(m => m.Text.Text)), "difference recovers every message ID and body in order without duplicates");
        Check(recovered.All(m => m.Sender.UserId == Accounts["alice"].Id) && recovered.All(m => m.Text.Text != privateText), "difference catch-up excludes other users' private events");
        Check((await s.Rpc(Req(new P.GetStateRequest()), "bob")).GetState.Pts == pts && (await s.Rpc(Req(new P.GetDifferenceRequest { Pts = pts }), "bob")).GetDifference.Empty is not null, "caught-up pts matches GetState and empty GetDifference");

        if (tlsEnabled) Console.WriteLine("Negative input bounds: <=2 connections, <=10 frames/second, <=10 seconds/case; actual one connection and one 2-byte frame/case; observe resources 5 seconds; stop on failed health, process exit or timeout. Server only sends EventHint, it does not parse it.");
        foreach (var protocol in new[] { jsonProtocol, hintProtocol })
        foreach (var type in new[] { WebSocketMessageType.Text, WebSocketMessageType.Binary })
        {
            using var socket = await Connect(latestCursor, Accounts["bob"].Token, protocol);
            using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            await socket.SendAsync(new ArraySegment<byte>(new byte[] { 0x7b, 0x7d }), type, true, timeout.Token);
            await Closed(socket, "nonempty client " + type + " payload disconnects " + protocol);
            if (tlsEnabled) await Task.Delay(110);
        }
        var login = await s.Json("POST", "/api/auth/login", new { username = "bob", password = "isolated-test-password" });
        var logoutToken = Str(login, "token");
        using (var json = await Connect(latestCursor, logoutToken, jsonProtocol))
        using (var proto = await Connect(latestCursor, logoutToken, hintProtocol))
        {
            await s.Json("POST", "/api/auth/logout", new { }, logoutToken, 204);
            await Closed(json, "logout disconnects JSON session socket");
            await Closed(proto, "logout disconnects protobuf session socket");
        }
        await Reject(latestCursor, logoutToken, HttpStatusCode.Unauthorized, hintProtocol);
        await s.Rpc(Req(new P.GetStateRequest()), "bob");
        if (tlsEnabled)
        {
            Console.WriteLine($"difference pages={differencePages} recoveredMessages={recovered.Count} startPts={startPts} finalPts={pts}");
            await s.ObserveHealth();
        }
        Console.WriteLine($"WebSocket PASS: {assertions - beforeAssertions} assertions; isolated data={path}; transport={(proxyTlsEnabled ? "Caddy WSS to HTTP Kestrel; exact leaf pin; CA chain/Android NOT TESTED" : tlsEnabled ? "Kestrel direct WSS; reverse proxy NOT TESTED; Android NOT TESTED" : "WS; TLS not exercised")}");
    }

    private static async Task ReadHistoryBoundaries(string sourcePath)
    {
        var path = Path.Combine(Root, "read-history-boundaries.json");
        File.Copy(sourcePath, path);
        var rejected = new List<(string RequestId, long MaxId)>();
        string log;
        await using (var s = await Server.Start(path, audit: true))
        {
        log = s.LogPath;
        var sent = await s.Rpc(Send(884422, "read history boundary"));
        var pts = (await s.Rpc(Req(new P.GetStateRequest()), "bob")).GetState.Pts;
        var before = Load(path);
        foreach (var maxId in new[] { -1L, (long)int.MaxValue + 1 })
        {
            var request = Req(new P.ReadHistoryRequest { Peer = Peer("alice"), MaxMessageId = maxId });
            await s.Rpc(request, "bob", 400, "BOTGRAM_RPC_INVALID_ARGUMENT");
            rejected.Add((request.RequestId, maxId));
            Equal(before, Load(path), "invalid read max ID does not mutate state");
        }
        var zero = (await s.Rpc(Req(new P.ReadHistoryRequest { Peer = Peer("alice"), MaxMessageId = 0 }), "bob")).ReadHistory;
        Check(zero.Pts == pts && zero.PtsCount == 0, "read max ID zero succeeds without advancing pts");
        Equal(before, Load(path), "read max ID zero is a persisted no-op");
        var maximum = (await s.Rpc(Req(new P.ReadHistoryRequest { Peer = Peer("alice"), MaxMessageId = int.MaxValue }), "bob")).ReadHistory;
        Check(maximum.Pts == pts + 1 && maximum.PtsCount == 1, "read int32 maximum succeeds and advances pts");
        var receipt = Arr(Load(path), "receipts").Single(r => Str(r!, "userId") == Accounts["bob"].Id)!;
        Check(Long(receipt, "messageId") == sent.SendMessage.Message.MessageId, "read int32 maximum advances to the actual last message");
        var after = Load(path);
        var repeated = (await s.Rpc(Req(new P.ReadHistoryRequest { Peer = Peer("alice"), MaxMessageId = int.MaxValue }), "bob")).ReadHistory;
        Check(repeated.Pts == maximum.Pts && repeated.PtsCount == 0, "repeated read int32 maximum is a no-op");
        Equal(after, Load(path), "repeated read int32 maximum preserves persisted state");
        }
        var evidence = File.ReadAllText(log);
        if (tlsEnabled)
        {
            foreach (var line in evidence.Split('\n').Where(line => line.Contains("operation=ReadHistory ", StringComparison.Ordinal) && line.Contains("status=400 ", StringComparison.Ordinal))) Console.WriteLine(line.Trim());
            Console.WriteLine("Historical real-device ReadHistory 400 attribution: INCONCLUSIVE; isolated boundary audit is not device evidence.");
        }
        foreach (var (requestId, maxId) in rejected)
            Check(evidence.Split('\n').Any(line => line.Contains($"operation=ReadHistory request_id={requestId} peer_kind=User max_id={maxId} error=BOTGRAM_RPC_INVALID_ARGUMENT status=400 ", StringComparison.Ordinal)),
                "read rejection audit correlates request, peer kind, max ID, error and response status");
        Check(!Accounts.Values.Any(a => evidence.Contains(a.Token, StringComparison.Ordinal) || evidence.Contains(a.Id, StringComparison.Ordinal))
            && !evidence.Contains("read history boundary", StringComparison.Ordinal), "read audit excludes bearer, user UUID and message body");
    }

    private static async Task ProtocolEvidence(string sourcePath)
    {
        var path = Path.Combine(Root, "protobuf-only.json");
        File.Copy(sourcePath, path);
        string log;
        await using (var s = await Server.Start(path, legacyEnabled: false, audit: true))
        {
            log = s.LogPath;
            var before = Load(path);
            foreach (var route in new[] { "/api/telegram/rpc", "/API/telegram/RPC/" })
            {
                var rejection = await s.Json("POST", route, T("TL_messages_sendMessage"), Accounts["alice"].Token, 410);
                Check(Str(rejection, "error") == "BOTGRAM_JSON_RPC_DISABLED", "legacy write explicitly disabled");
            }
            Equal(before, Load(path), "disabled JSON cannot mutate state");
            await s.Json("GET", "/api/telegram/me", null, Accounts["alice"].Token);
            await s.Json("GET", "/api/gid/regions", null, Accounts["alice"].Token);
            await s.Raw(Req(new P.GetStateRequest()).ToByteArray(), 401, "unauthorized", null);
            await s.Rpc(Req(new P.GetStateRequest()));
            var sent = await s.Rpc(Send(772211, "P5 protobuf-only evidence"));
            Check(sent.SendMessage.Message.Text.Text == "P5 protobuf-only evidence", "protobuf-only write succeeds");
            var history = await s.Rpc(Req(new P.GetHistoryRequest { Peer = Peer("bob") }));
            Check(history.GetHistory.Messages.Any(m => m.MessageId == sent.SendMessage.Message.MessageId), "protobuf-only read observes write");
            Check(Load(path)["messages"]!.AsArray().Count(m => Str(m!, "clientMessageId") == "tl:772211") == 1, "protobuf write executed once");
        }
        var evidence = File.ReadAllText(log);
        foreach (var (protocol, operation, status) in new[] { ("json", "unparsed", 410), ("protobuf", "SendMessage", 200), ("protobuf", "GetHistory", 200), ("protobuf", "unparsed", 401) })
            Check(evidence.Split('\n').Any(line => line.Contains($"protocol={protocol} operation={operation} ", StringComparison.Ordinal)
                && line.Contains($" status={status} ", StringComparison.Ordinal)), $"audit evidence {protocol}/{operation}/{status}");
        Check(!evidence.Contains(Accounts["alice"].Token, StringComparison.Ordinal) && !evidence.Contains("P5 protobuf-only evidence", StringComparison.Ordinal), "audit excludes bearer and message body");
    }

    private static async Task Bootstrap(string sourcePath)
    {
        var path = Path.Combine(Root, "bootstrap.json");
        File.Copy(sourcePath, path);
        var before = Load(path);
        var group = before["conversations"]!.AsArray().Single(c => Str(c!, "kind") == "group")!;
        var groupId = Str(group, "id");
        var chatId = before["telegramIds"]!["chat:" + groupId]!.GetValue<long>();
        var input = new P.ResolveIdentitiesRequest { TlUserIds = { Accounts["bob"].TlId, Accounts["alice"].TlId }, TlChatIds = { chatId } };
        P.ResolvedIdentities? first = null;
        for (int restart = 0; restart < 2; restart++)
        {
            await using var s = await Server.Start(path);
            var empty = Req(new P.ResolveIdentitiesRequest());
            await s.Raw(empty.ToByteArray(), 401, "unauthorized", null);
            await s.Raw(empty.ToByteArray(), 401, "session_expired", "invalid-bootstrap-session");
            var bootstrap = (await s.Rpc(empty)).ResolveIdentities;
            Check(bootstrap.ServerDomain == Domain && bootstrap.Groups.Count == 0 && bootstrap.Users.Count == 1, "empty bootstrap domain and self only");
            Check(bootstrap.Users[0].IsSelf && bootstrap.Users[0].Identity.UserId == Accounts["alice"].Id && bootstrap.Users[0].TlUserId == Accounts["alice"].TlId, "bootstrap authenticated self identity");
            var found = (await s.Rpc(Req(input))).ResolveIdentities;
            Check(found.ServerDomain == Domain && found.Users.Count == 2 && found.Groups.Count == 1, "bootstrap exact summaries and self deduplication");
            var bob = found.Users.Single(u => u.Identity.UserId == Accounts["bob"].Id);
            Check(bob.Identity.ServerDomain == Domain && bob.TlUserId == Accounts["bob"].TlId && bob.Username == "bob" && bob.DisplayName == "bob Test" && bob.FirstName == "Local" && bob.LastName == "Bob" && bob.Gid == Accounts["bob"].Gid && bob.IsContact && !bob.IsSelf && !bob.IsMutualContact, "bootstrap real contact summary");
            Check(found.Groups[0].GroupId == groupId && found.Groups[0].TlChatId == chatId && found.Groups[0].Title == "isolated group" && found.Groups[0].ParticipantCount == 2 && found.Groups[0].Version == 1 && found.Groups[0].CreatedAtMs == DateTimeOffset.Parse(Str(group, "createdAt")).ToUnixTimeMilliseconds(), "bootstrap real group summary");
            if (first is not null) Check(first.Equals(found), "bootstrap stable across restart");
            first = found;
            await s.Rpc(Req(input), "outsider", 404, "PEER_ID_INVALID");
            foreach (var ids in new[] { new long[] { 0 }, new long[] { -1 }, new long[] { Accounts["bob"].TlId, Accounts["bob"].TlId } })
            {
                await s.Rpc(Req(new P.ResolveIdentitiesRequest { TlUserIds = { ids } }), status: 400, code: "BOTGRAM_RPC_INVALID_ARGUMENT");
                await s.Rpc(Req(new P.ResolveIdentitiesRequest { TlChatIds = { ids } }), status: 400, code: "BOTGRAM_RPC_INVALID_ARGUMENT");
            }
            await s.Rpc(Req(new P.ResolveIdentitiesRequest { TlUserIds = { long.MaxValue } }), status: 404, code: "USER_ID_INVALID");
            await s.Rpc(Req(new P.ResolveIdentitiesRequest { TlChatIds = { long.MaxValue } }), status: 404, code: "PEER_ID_INVALID");
            await s.Rpc(Req(new P.ResolveIdentitiesRequest { TlUserIds = { chatId } }), status: 404, code: "USER_ID_INVALID");
            await s.Rpc(Req(new P.ResolveIdentitiesRequest { TlChatIds = { Accounts["bob"].TlId } }), status: 404, code: "PEER_ID_INVALID");
            var over = new P.ResolveIdentitiesRequest();
            over.TlUserIds.Add(Enumerable.Range(1, 51).Select(i => (long)i));
            over.TlChatIds.Add(Enumerable.Range(52, 50).Select(i => (long)i));
            await s.Rpc(Req(over), status: 400, code: "BOTGRAM_RPC_INVALID_ARGUMENT");
            Equal(before, Load(path), "bootstrap preserves entire database including TelegramIds and next counter");
        }
        var missingPath = Path.Combine(Root, "bootstrap-missing.json");
        var missing = before.DeepClone();
        missing["telegramIds"]!.AsObject().Remove("user:" + Accounts["alice"].Id);
        missing["telegramIds"]!.AsObject().Remove("user:" + Accounts["outsider"].Id);
        File.WriteAllText(missingPath, missing.ToJsonString());
        await using (var s = await Server.Start(missingPath))
        {
            await s.Rpc(Req(new P.ResolveIdentitiesRequest()), status: 404, code: "SELF_IDENTITY_NOT_FOUND");
            await s.Rpc(Req(new P.ResolveIdentitiesRequest { TlUserIds = { Accounts["outsider"].TlId } }), "bob", 404, "USER_ID_INVALID");
            var self = (await s.Rpc(Req(new P.ResolveIdentitiesRequest()), "bob")).ResolveIdentities;
            Check(self.Users.Single().Identity.UserId == Accounts["bob"].Id, "self summary follows authenticated actor");
            Equal(missing, Load(missingPath), "missing identity is never allocated by bootstrap");
        }
    }

    private static async Task NegativeAndPagination(Server s)
    {
        async Task Bad(P.RpcRequest request, int status, string code, string actor = "alice") => await s.Rpc(request, actor, status, code);
        var state = Req(new P.GetStateRequest());
        await s.Raw(state.ToByteArray(), 401, "unauthorized", null);
        await s.Raw(state.ToByteArray(), 401, "session_expired", "not-a-session");
        await s.Raw([], 400, "BOTGRAM_REQUEST_ID_INVALID");
        await s.Raw([0xff], 400, "BOTGRAM_PROTOBUF_MALFORMED");
        await s.Raw([0x12, 0x05, 0x61], 400, "BOTGRAM_PROTOBUF_MALFORMED");
        // Unknown nested groups exercise the protobuf recursion limit without relying on business schema recursion.
        await s.Raw(Enumerable.Repeat((byte)0x2b, 40).Concat(Enumerable.Repeat((byte)0x2c, 40)).ToArray(), 400, "BOTGRAM_PROTOBUF_MALFORMED");
        await s.Raw(state.ToByteArray(), 415, "BOTGRAM_MEDIA_TYPE_UNSUPPORTED", media: "application/json");
        await s.Raw(state.ToByteArray(), 415, "BOTGRAM_CONTENT_ENCODING_UNSUPPORTED", encoding: "gzip");
        await s.Raw(new byte[128 * 1024 + 1], 413, "BOTGRAM_REQUEST_TOO_LARGE");
        await s.ChunkedOversize();
        var invalid = state.Clone(); invalid.ProtocolMajor = 2;
        await Bad(invalid, 400, "BOTGRAM_PROTOCOL_MAJOR_UNSUPPORTED");
        foreach (var id in new[] { "", new string('a', 65), "bad.id", "非ASCII" })
        {
            invalid = state.Clone(); invalid.RequestId = id;
            await Bad(invalid, 400, "BOTGRAM_REQUEST_ID_INVALID");
        }
        invalid = state.Clone(); invalid.RequestId = new string('a', 64); await s.Rpc(invalid);
        await Bad(new P.RpcRequest { ProtocolMajor = 1, RequestId = "unknown" }, 501, "BOTGRAM_RPC_UNSUPPORTED");
        // An unknown operation field must not be accepted as a successful no-op.
        await s.Raw(new P.RpcRequest { ProtocolMajor = 1, RequestId = "future" }.ToByteArray().Concat(new byte[] { 0xca, 0x0c, 0x00 }).ToArray(), 501, "BOTGRAM_RPC_UNSUPPORTED");
        invalid = Send(300, "text"); invalid.SendMessage.Peer.User.ServerDomain = "foreign.test";
        await Bad(invalid, 501, "BOTGRAM_REMOTE_DOMAIN_UNSUPPORTED");
        invalid = Send(301, "text"); invalid.SendMessage.Entities.Add(new P.MessageEntity { LengthUtf16 = 1 });
        await Bad(invalid, 501, "BOTGRAM_ENTITY_UNSUPPORTED");
        invalid = Send(302, "😀"); invalid.SendMessage.Entities.Add(new P.MessageEntity { OffsetUtf16 = 1, LengthUtf16 = 1, Bold = new() });
        await Bad(invalid, 400, "ENTITY_BOUNDS_INVALID");
        await Bad(Req(new P.GetDifferenceRequest { Pts = -1 }), 400, "BOTGRAM_RPC_INVALID_ARGUMENT");
        await Bad(Req(new P.GetMessagesRequest { MessageIds = { (long)int.MaxValue + 1 } }), 400, "BOTGRAM_RPC_INVALID_ARGUMENT");
        await Bad(Req(new P.GetDialogsRequest { Limit = 0 }), 400, "BOTGRAM_RPC_INVALID_ARGUMENT");
        await Bad(Req(new P.GetHistoryRequest { Peer = Peer("bob"), Limit = 0 }), 400, "BOTGRAM_RPC_INVALID_ARGUMENT");
        await s.Rpc(Req(new P.GetDialogsRequest()));
        await s.Rpc(Req(new P.GetHistoryRequest { Peer = Peer("bob") }));
        await Bad(Req(new P.GetMessagesRequest { MessageIds = { 1, 1 } }), 400, "BOTGRAM_RPC_INVALID_ARGUMENT");
        await Bad(Req(new P.GetUsersRequest { Users = { Selector("bob"), Selector("bob") } }), 400, "BOTGRAM_RPC_INVALID_ARGUMENT");
        await Bad(Req(new P.GetPeerDialogsRequest { Peers = { Peer("bob"), Peer("bob") } }), 400, "BOTGRAM_RPC_INVALID_ARGUMENT");
        await Bad(Req(new P.CreateGroupRequest { Title = "duplicate", Users = { Selector("bob"), Selector("bob") } }), 400, "BOTGRAM_RPC_INVALID_ARGUMENT");
        var contact = new P.ImportContact { ClientId = 9, Gid = Accounts["bob"].Gid, FirstName = "B" };
        await Bad(Req(new P.ImportContactsRequest { Contacts = { contact, contact.Clone() } }), 400, "BOTGRAM_RPC_INVALID_ARGUMENT");
        var second = contact.Clone(); second.ClientId = 10;
        await Bad(Req(new P.ImportContactsRequest { Contacts = { contact, second } }), 400, "BOTGRAM_RPC_INVALID_ARGUMENT");
        var group = Load(s.DataPath)["conversations"]!.AsArray().Single(c => Str(c!, "kind") == "group")!["id"]!.GetValue<string>();
        await Bad(Req(new P.GetGroupsRequest { GroupIds = { group, group } }), 400, "BOTGRAM_RPC_INVALID_ARGUMENT");
        var before = NormalizeSnapshot(Load(s.DataPath));
        await Bad(Req(new P.GetFullGroupRequest { GroupId = group }), 404, "PEER_ID_INVALID", "outsider");
        await Bad(Req(new P.GetHistoryRequest { Peer = new P.PeerRef { GroupId = group } }), 404, "PEER_ID_INVALID", "outsider");
        await Bad(Req(new P.GetGroupsRequest { GroupIds = { group } }), 404, "PEER_ID_INVALID", "outsider");
        Equal(before, NormalizeSnapshot(Load(s.DataPath)), "nonmember errors do not mutate state");
        var outsiders = await s.Rpc(Req(new P.GetDialogsRequest()), "outsider");
        Check(outsiders.GetDialogs.Groups.Count == 0 && outsiders.GetDialogs.Messages.Count == 0, "nonmember no group/message leak");
        await Bad(Send(long.MinValue, "conflicting payload"), 409, "RANDOM_ID_DUPLICATE");
        var changedSilent = Send(long.MinValue, "hello 😀", true); changedSilent.SendMessage.Silent = false;
        await Bad(changedSilent, 409, "RANDOM_ID_DUPLICATE");
        var max = Send(long.MaxValue, "reply"); max.SendMessage.ReplyToMessageId = 1;
        await s.Rpc(max);
        var diskBefore = NormalizeSnapshot(Load(s.DataPath));
        Directory.CreateDirectory(s.DataPath + ".tmp");
        try
        {
            string expectedStorageError;
            try
            {
                using var probe = new FileStream(s.DataPath + ".tmp", FileMode.Create, FileAccess.Write, FileShare.None);
                throw new InvalidOperationException("Directory fixture unexpectedly allowed file creation");
            }
            catch (UnauthorizedAccessException) { expectedStorageError = "BOTGRAM_STORAGE_FAILURE"; }
            catch (IOException) { expectedStorageError = "BOTGRAM_STORAGE_OR_TRANSPORT_FAILURE"; }
            await Bad(Send(8001, "disk retry"), 500, expectedStorageError);
        }
        finally { Directory.Delete(s.DataPath + ".tmp"); }
        Equal(diskBefore, NormalizeSnapshot(Load(s.DataPath)), "disk failure preserves persisted snapshot");
        await s.Rpc(Send(8001, "disk retry"));
        await s.Rpc(Send(8001, "disk retry"));
        Check(Load(s.DataPath)["messages"]!.AsArray().Count(m => Str(m!, "clientMessageId") == "tl:8001") == 1, "disk rollback retry executes once");
        await s.CancelUpload(Send(9001, "cancel retry"));
        var cancelled = await s.Rpc(Send(9001, "cancel retry"));
        var cancelledAgain = await s.Rpc(Send(9001, "cancel retry"));
        Check(cancelled.SendMessage.Mapping.MessageId == cancelledAgain.SendMessage.Mapping.MessageId, "cancel retry stable mapping");
        Check(Load(s.DataPath)["messages"]!.AsArray().Count(m => Str(m!, "clientMessageId") == "tl:9001") == 1, "cancel retry executes once");
        var concurrentStart = (await s.Rpc(Req(new P.GetStateRequest()))).GetState.Pts;
        var concurrent = await Task.WhenAll(Enumerable.Range(0, 8).Select(i => s.Rpc(Send(9500 + i, "concurrent fixture"))));
        Check(concurrent.Select(r => r.SendMessage.Mapping.MessageId).Distinct().Count() == 8, "concurrent same-conversation sends have unique IDs");
        foreach (var result in concurrent)
            Check(result.SendMessage.Message.Equals(result.SendMessage.Updates.Updates_[1].NewMessage.Message), "send response and update preserve complete protobuf message equality");
        var concurrentIds = concurrent.Select(r => r.SendMessage.Mapping.MessageId).Order().ToArray();
        Check(concurrentIds.Zip(concurrentIds.Skip(1), (a, b) => b == a + 1).All(x => x), "concurrent committed message IDs are contiguous");
        Check((await s.Rpc(Req(new P.GetStateRequest()))).GetState.Pts == concurrentStart + 8, "concurrent sends advance pts once each");
        var start = (await s.Rpc(Req(new P.GetStateRequest()))).GetState.Pts;
        for (int i = 0; i < 105; i++) await s.Rpc(Send(10000 + i, "page " + i));
        var first = (await s.Rpc(Req(new P.GetDifferenceRequest { Pts = start }))).GetDifference;
        Check(first.Slice is not null && first.Slice.Messages.Count == 100 && first.Slice.State.Pts == start + 100, "difference slice 100 cursor");
        var next = (await s.Rpc(Req(new P.GetDifferenceRequest { Pts = first.Slice!.State.Pts }))).GetDifference;
        Check(next.Complete is not null && next.Complete.Messages.Count == 5 && next.Complete.State.Pts == start + 105, "difference complete remainder");
        Check(first.Slice.Messages.Select(m => m.MessageId).Intersect(next.Complete!.Messages.Select(m => m.MessageId)).Count() == 0, "difference no duplicate messages");
        Check((await s.Rpc(Req(new P.GetDifferenceRequest { Pts = next.Complete.State.Pts }))).GetDifference.Empty is not null, "difference empty at end");
        var last = next.Complete.Messages[^1].MessageId;
        var read = await s.Rpc(Req(new P.ReadHistoryRequest { Peer = Peer("alice"), MaxMessageId = last }), "bob");
        Check(read.ReadHistory.PtsCount == 1, "read receipt advances");
        var receipt = (await s.Rpc(Req(new P.GetDifferenceRequest { Pts = start + 105 }))).GetDifference.Complete;
        Check(receipt is not null && receipt.Updates.Any(u => u.ReadOutbox?.MaxMessageId == last), "outbox receipt difference");
        var gidPts = (await s.Rpc(Req(new P.GetStateRequest()))).GetState.Pts;
        await s.Json("PUT", "/api/me/gid", new { regionCode = "030", number = "87654321" }, Accounts["bob"].Token);
        var gid = (await s.Rpc(Req(new P.GetDifferenceRequest { Pts = gidPts }))).GetDifference.Complete;
        Check(gid is not null && gid.Updates.Any(u => u.GidChanged?.User.UserId == Accounts["bob"].Id && u.GidChanged.Gid == "03087654321"), "GID change difference identity");
        await s.AssertRateLimit(state);
        await s.Json("POST", "/api/auth/logout", new { }, Accounts["outsider"].Token, 204);
        await s.Raw(state.ToByteArray(), 401, "session_expired", Accounts["outsider"].Token);
    }

    private static P.RpcRequest Req(IMessage operation)
    {
        var r = new P.RpcRequest { ProtocolMajor = 1, RequestId = "http_test-1" };
        var field = P.RpcRequest.Descriptor.Fields.InFieldNumberOrder().Single(f => f.FieldType == Google.Protobuf.Reflection.FieldType.Message && f.MessageType == operation.Descriptor);
        field.Accessor.SetValue(r, operation); return r;
    }
    private static P.UserRef Reference(string name) => new() { ServerDomain = Domain, UserId = Accounts[name].Id };
    private static P.PeerRef Peer(string name) => new() { User = Reference(name) };
    private static P.UserSelector Selector(string name) => new() { User = Reference(name) };
    private static P.RpcRequest Send(long random, string text, bool formatted = false)
    {
        var input = new P.SendMessageRequest { Peer = Peer("bob"), RandomId = random, Text = text, Silent = formatted };
        if (formatted) input.Entities.Add(new P.MessageEntity { OffsetUtf16 = 6, LengthUtf16 = 2, Bold = new() });
        return Req(input);
    }
    private static JsonObject T(string type, params (string Name, object? Value)[] fields)
    {
        var obj = new JsonObject { ["$type"] = "org.telegram.tgnet.TLRPC$" + type };
        foreach (var (name, value) in fields) obj[name] = JsonSerializer.SerializeToNode(value);
        return obj;
    }
    private static JsonObject PeerJ(Account account) => T("TL_inputPeerUser", ("user_id", account.TlId));
    private static JsonNode Node(object value) => JsonSerializer.SerializeToNode(value)!;
    private static long Long(JsonNode j, string field) => j[field]?.GetValue<long>() ?? 0;
    private static string Str(JsonNode j, string field) => j[field]?.GetValue<string>() ?? "";
    private static bool Bool(JsonNode j, string field) => j[field]?.GetValue<bool>() ?? false;
    private static JsonArray Arr(JsonNode j, string field) => j[field]?.AsArray() ?? [];
    private static long Tl(P.UserRef user)
    {
        Check(user.ServerDomain == Domain, "local identity domain");
        return Accounts.Values.Single(a => a.Id == user.UserId).TlId;
    }
    private static long PeerId(P.PeerRef peer) => peer.PeerCase switch
    {
        P.PeerRef.PeerOneofCase.User => Tl(peer.User),
        P.PeerRef.PeerOneofCase.GroupId => -GroupIds[peer.GroupId],
        _ => throw new Exception("unexpected response peer " + peer.PeerCase)
    };
    private static long PeerIdJ(JsonNode peer) => Long(peer, "user_id") != 0 ? Long(peer, "user_id") : -Long(peer, "chat_id");
    private static object User(P.UserSummary u) => new { id = Tl(u.Identity), tl = u.TlUserId, username = u.Username, first = u.FirstName, last = u.LastName, gid = u.Gid, self = u.IsSelf, contact = u.IsContact, mutual = u.IsMutualContact };
    private static object UserJ(JsonNode u) => new { id = Long(u, "id"), tl = Long(u, "id"), username = Str(u, "username"), first = Str(u, "first_name"), last = Str(u, "last_name"), gid = Str(u, "phone"), self = Bool(u, "self"), contact = Bool(u, "contact"), mutual = Bool(u, "mutual_contact") };
    private static object Group(P.GroupSummary g)
    {
        Check(Guid.TryParseExact(g.GroupId, "N", out _), "group UUID reference");
        Check(g.CreatedAtMs > 0 && Math.Abs(DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() - g.CreatedAtMs) < 3_600_000, "group date milliseconds");
        if (GroupIds.TryGetValue(g.GroupId, out var existing)) Check(existing == g.TlChatId, "stable group UUID to TL identity");
        else GroupIds.Add(g.GroupId, g.TlChatId);
        return new { id = g.TlChatId, title = g.Title, count = g.ParticipantCount, version = g.Version };
    }
    private static object GroupJ(JsonNode g) => new { id = Long(g, "id"), title = Str(g, "title"), count = Long(g, "participants_count"), version = Long(g, "version") };
    private static JsonNode State(P.SyncState state) { Check(state.DateMs > 0 && state.DateMs % 1000 == 0, "state milliseconds"); return Node(new { pts = state.Pts, qts = state.Qts, seq = state.Seq, unread = state.UnreadCount }); }
    private static JsonNode StateJ(JsonNode j) => Node(new { pts = Long(j, "pts"), qts = Long(j, "qts"), seq = Long(j, "seq"), unread = Long(j, "unread_count") });
    private static object Message(P.Message m)
    {
        Check(m.CreatedAtMs > 0, "message timestamp");
        return new { id = m.MessageId, peer = PeerId(m.Peer), sender = Tl(m.Sender), outgoing = m.Outgoing, unread = m.Unread,
            text = m.Text?.Text ?? "", silent = m.Text?.Silent ?? false, reply = m.Text?.Reply?.MessageId ?? 0,
            entities = m.Text?.Entities.Select(e => new { kind = e.KindCase.ToString(), offset = (long)e.OffsetUtf16, length = (long)e.LengthUtf16, language = e.Pre?.Language ?? "", url = e.TextUrl?.Url ?? "" }).ToArray() ?? [],
            title = m.Service?.GroupCreated.Title ?? "", members = m.Service?.GroupCreated.Members.Select(Tl).ToArray() ?? [] };
    }
    private static object MessageJ(JsonNode m) => new { id = Long(m, "id"), peer = PeerIdJ(m["peer_id"]!), sender = PeerIdJ(m["from_id"]!), outgoing = Bool(m, "out"), unread = Bool(m, "unread"),
        text = Str(m, "message"), silent = Bool(m, "silent"), reply = m["reply_to"] is { } reply ? Long(reply, "reply_to_msg_id") : 0,
        entities = Arr(m, "entities").Select(e => new { kind = Str(e!, "$type").Split("TL_messageEntity").Last(), offset = Long(e!, "offset"), length = Long(e!, "length"), language = Str(e!, "language"), url = Str(e!, "url") }).ToArray(),
        title = m["action"] is { } action ? Str(action, "title") : "", members = m["action"] is { } action2 ? Arr(action2, "users").Select(i => i!.GetValue<long>()).ToArray() : [] };
    private static object Update(P.Update u) => u.KindCase switch
    {
        P.Update.KindOneofCase.MessageId => new { kind = "id", id = u.MessageId.MessageId, random = u.MessageId.RandomId },
        P.Update.KindOneofCase.NewMessage => new { kind = "new", message = Message(u.NewMessage.Message), pts = u.NewMessage.Pts, count = u.NewMessage.PtsCount },
        P.Update.KindOneofCase.ReadInbox => Receipt(u.ReadInbox, "in"),
        P.Update.KindOneofCase.ReadOutbox => Receipt(u.ReadOutbox, "out"),
        _ => throw new Exception("unexpected semantic update " + u.KindCase)
    };
    private static object Receipt(P.ReadReceipt r, string kind) => new { kind, peer = PeerId(r.Peer), max = r.MaxMessageId, pts = r.Pts, count = r.PtsCount };
    private static object UpdateJ(JsonNode u)
    {
        var type = Str(u, "$type").Split('$').Last();
        return type switch
        {
            "TL_updateMessageID" => new { kind = "id", id = Long(u, "id"), random = Long(u, "random_id") },
            "TL_updateNewMessage" => new { kind = "new", message = MessageJ(u["message"]!), pts = Long(u, "pts"), count = Long(u, "pts_count") },
            "TL_updateReadHistoryInbox" or "TL_updateReadHistoryOutbox" => new { kind = type.EndsWith("Inbox") ? "in" : "out", peer = PeerIdJ(u["peer"]!), max = Long(u, "max_id"), pts = Long(u, "pts"), count = Long(u, "pts_count") },
            _ => throw new Exception("unexpected JSON update " + type)
        };
    }
    private static object Updates(P.Updates u) => new { updates = u.Updates_.Select(Update), users = u.Users.Select(User), groups = u.Groups.Select(Group), seq = u.Seq };
    private static object UpdatesJ(JsonNode u) => new { updates = Arr(u, "updates").Select(i => UpdateJ(i!)), users = Arr(u, "users").Select(i => UserJ(i!)), groups = Arr(u, "chats").Select(i => GroupJ(i!)), seq = Long(u, "seq") };
    private static JsonNode Sent(P.MessageSent sent)
    {
        Check(sent.Mapping.MessageId == sent.Message.MessageId && sent.Updates.Updates_.Count == 2, "send mapping and updates");
        Equal(Node(Message(sent.Message)), Node(Message(sent.Updates.Updates_[1].NewMessage.Message)), "send standalone/update message");
        return Node(Updates(sent.Updates));
    }
    private static JsonNode SentJ(JsonNode j) => Node(UpdatesJ(j));
    private static JsonNode Page(P.MessagePage p) => Node(new { messages = p.Messages.Select(Message), users = p.Users.Select(User), groups = p.Groups.Select(Group) });
    private static JsonNode PageJ(JsonNode p) => Node(new { messages = Arr(p, "messages").Select(i => MessageJ(i!)), users = Arr(p, "users").Select(i => UserJ(i!)), groups = Arr(p, "chats").Select(i => GroupJ(i!)) });
    private static object Dialog(P.Dialog d) => new { peer = PeerId(d.Peer), top = d.TopMessageId, inbox = d.ReadInboxMaxId, outbox = d.ReadOutboxMaxId, unread = d.UnreadCount };
    private static object DialogJ(JsonNode d) => new { peer = PeerIdJ(d["peer"]!), top = Long(d, "top_message"), inbox = Long(d, "read_inbox_max_id"), outbox = Long(d, "read_outbox_max_id"), unread = Long(d, "unread_count") };
    private static JsonNode Dialogs(P.DialogPage p) => Node(new { dialogs = p.Dialogs.Select(Dialog), messages = p.Messages.Select(Message), users = p.Users.Select(User), groups = p.Groups.Select(Group), count = p.TotalCount, slice = p.IsSlice });
    private static JsonNode Peers(P.PeerDialogs p) => Node(new { dialogs = p.Dialogs.Select(Dialog), messages = p.Messages.Select(Message), users = p.Users.Select(User), groups = p.Groups.Select(Group), state = State(p.State) });
    private static JsonNode DialogsJ(JsonNode p, bool all) => all
        ? Node(new { dialogs = Arr(p, "dialogs").Select(i => DialogJ(i!)), messages = Arr(p, "messages").Select(i => MessageJ(i!)), users = Arr(p, "users").Select(i => UserJ(i!)), groups = Arr(p, "chats").Select(i => GroupJ(i!)), count = Long(p, "count"), slice = Str(p, "$type").EndsWith("Slice") })
        : Node(new { dialogs = Arr(p, "dialogs").Select(i => DialogJ(i!)), messages = Arr(p, "messages").Select(i => MessageJ(i!)), users = Arr(p, "users").Select(i => UserJ(i!)), groups = Arr(p, "chats").Select(i => GroupJ(i!)), state = StateJ(p["state"]!) });
    private static JsonNode Resolved(P.ResolvedPeer p) => Node(new { peer = PeerId(p.Peer), users = p.Users.Select(User), groups = p.Groups.Select(Group) });
    private static JsonNode ResolvedJ(JsonNode p) => Node(new { peer = PeerIdJ(p["peer"]!), users = Arr(p, "users").Select(i => UserJ(i!)), groups = Arr(p, "chats").Select(i => GroupJ(i!)) });
    private static JsonNode Difference(P.Difference d)
    {
        var page = d.Complete ?? d.Slice ?? throw new Exception("scenario expected nonempty difference");
        return Node(new { kind = d.Complete is not null ? "complete" : "slice", messages = page.Messages.Select(Message), updates = page.Updates.Select(Update), users = page.Users.Select(User), groups = page.Groups.Select(Group), state = State(page.State) });
    }
    private static JsonNode DifferenceJ(JsonNode d) => Node(new { kind = Str(d, "$type").EndsWith("Slice") ? "slice" : "complete", messages = Arr(d, "new_messages").Select(i => MessageJ(i!)), updates = Arr(d, "other_updates").Select(i => UpdateJ(i!)), users = Arr(d, "users").Select(i => UserJ(i!)), groups = Arr(d, "chats").Select(i => GroupJ(i!)), state = StateJ(d["state"] ?? d["intermediate_state"]!) });

    private static readonly Dictionary<string, string> ValidationDatabases = [];
    private static JsonNode Load(string path)
    {
        if (!ValidationDatabases.TryGetValue(path, out var connectionString)) return JsonNode.Parse(File.ReadAllText(path))!;
        using var source = NpgsqlDataSource.Create(connectionString);
        using var command = source.CreateCommand("SELECT payload::text FROM botgram_validation_v1.state WHERE singleton");
        return JsonNode.Parse((string)command.ExecuteScalar()!)!;
    }
    private static JsonNode NormalizeSnapshot(JsonNode source)
    {
        var ids = Arr(source, "conversations").Select((c, index) => (Original: Str(c!, "id"), Canonical: "000000000000000000000000" + (index + 1).ToString("D8"))).ToDictionary(x => x.Original, x => x.Canonical);
        // Every conversation UUID is replaced everywhere, including dictionary keys, service keys and event payloads.
        // Timestamps are mapped by structural field path, not dropped. Baseline session expiry is compared exactly.
        // Validate real timestamps first; entity/message/event fields and all non-clock business values remain intact.
        JsonNode? Walk(JsonNode? node, string path, string field)
        {
            if (node is JsonObject obj)
            {
                var result = new JsonObject();
                foreach (var (key, value) in obj) result[Replace(key)] = Walk(value, path + "/" + Replace(key), key);
                return result;
            }
            if (node is JsonArray array) return new JsonArray(array.Select((n, i) => Walk(n, path + "/" + i, field)).ToArray());
            if (node is JsonValue valueNode && valueNode.TryGetValue<string>(out var text))
            {
                if (field is "createdAt" or "updatedAt" || field == "expiresAt" && !path.StartsWith("/sessions/", StringComparison.Ordinal))
                {
                    Check(DateTimeOffset.TryParse(text, out var date) && Math.Abs((DateTimeOffset.UtcNow - date).TotalHours) < 1, "valid isolated clock " + path);
                    return JsonValue.Create("clock:" + path);
                }
                return JsonValue.Create(Replace(text));
            }
            return node?.DeepClone();
        }
        string Replace(string value)
        {
            foreach (var (original, canonical) in ids) value = value.Replace(original, canonical, StringComparison.Ordinal);
            return value;
        }
        return Walk(source, "", "")!;
    }
    private static void Check(bool condition, string message)
    {
        assertions++;
        if (!condition) throw new Exception(message);
    }
    private static void Equal(JsonNode expected, JsonNode actual, string context)
    {
        assertions++;
        if (!JsonNode.DeepEquals(expected, actual))
        {
            // Do not print snapshots: even isolated snapshots contain password/session digests.
            var path = DifferencePath(expected, actual, "");
            throw new Exception(context + " mismatch at " + path);
        }
    }
    private static string DifferencePath(JsonNode? a, JsonNode? b, string path)
    {
        if (a is JsonObject ao && b is JsonObject bo)
        {
            foreach (var key in ao.Select(k => k.Key).Union(bo.Select(k => k.Key)))
                if (!JsonNode.DeepEquals(ao[key], bo[key])) return DifferencePath(ao[key], bo[key], path + "/" + key);
        }
        if (a is JsonArray aa && b is JsonArray ba)
        {
            if (aa.Count != ba.Count) return path + "/count";
            for (int i = 0; i < aa.Count; i++) if (!JsonNode.DeepEquals(aa[i], ba[i])) return DifferencePath(aa[i], ba[i], path + "/" + i);
        }
        return path;
    }

    private sealed class Server(Process process, HttpClient client, string dataPath) : IAsyncDisposable
    {
        private Process? proxyProcess;
        public string DataPath { get; } = dataPath;
        public Uri WebSocketUri(long after) => new UriBuilder(client.BaseAddress!) { Scheme = tlsEnabled ? "wss" : "ws", Path = "/api/ws", Query = "after=" + after.ToString(System.Globalization.CultureInfo.InvariantCulture) }.Uri;
        public HttpClient Client => client;
        public string LogPath => Path.Combine(Root, $"backend-{process.Id}.stdout.log");
        public static async Task<Server> Start(string dataPath, bool legacyEnabled = true, bool audit = false, int rollout = 100,
            bool legacyWssEnabled = true, bool usageLog = false, string? validationConnection = null, string? validationSeed = null,
            bool relational = false, bool fullRelational = false,
            Botgram.Backend.Storage.FourComponentOptions? fullStack = null)
        {
            var listener = new TcpListener(IPAddress.Loopback, 0); listener.Start();
            int port = ((IPEndPoint)listener.LocalEndpoint).Port; listener.Stop();
            var start = new ProcessStartInfo("dotnet") { UseShellExecute = false, CreateNoWindow = true, RedirectStandardOutput = true, RedirectStandardError = true, WorkingDirectory = Path.GetDirectoryName(BackendDll)! };
            start.ArgumentList.Add(BackendDll); start.ArgumentList.Add("--urls"); start.ArgumentList.Add(tlsEnabled && !proxyTlsEnabled ? $"https://127.0.0.1:{port}" : $"http://127.0.0.1:{port}");
            if (tlsEnabled && !proxyTlsEnabled)
            {
                start.Environment["Kestrel__Certificates__Default__Path"] = certificatePath;
                start.Environment["Kestrel__Certificates__Default__Password"] = certificatePassword;
            }
            start.Environment["DataPath"] = dataPath;
            start.Environment["Protocol__ServerDomain"] = Domain;
            start.Environment["Protocol__LegacyJsonRpcEnabled"] = legacyEnabled.ToString();
            start.Environment["Protocol__LegacyJsonWssEnabled"] = legacyWssEnabled.ToString();
            start.Environment["Protocol__UsageLogEnabled"] = usageLog.ToString();
            start.Environment["Protocol__ProtobufRolloutPercent"] = rollout.ToString(System.Globalization.CultureInfo.InvariantCulture);
            start.Environment["Protocol__AuditRpc"] = (audit || Environment.GetEnvironmentVariable("BOTGRAM_DIAGNOSTIC_STAGES") == "1").ToString();
            start.Environment["Protocol__DiagnosticStages"] = (Environment.GetEnvironmentVariable("BOTGRAM_DIAGNOSTIC_STAGES") == "1").ToString();
            start.Environment["Storage__Mode"] = validationConnection is null ? "LegacyOnly" : "NewStorageValidation";
            start.Environment["Storage__ValidationBackend"] = fullStack is not null ? "FourComponent" : fullRelational ? "RelationalBackend" : relational ? "RelationalThreeRpc" : "SnapshotBridge";
            if (fullStack is not null)
            {
                start.Environment["Storage__FullStackAcknowledged"] = "true";
                foreach (var pair in new Dictionary<string, string>
                {
                    ["Kafka"] = fullStack.Kafka, ["Topic"] = fullStack.Topic, ["Consumer"] = fullStack.Consumer,
                    ["Scylla"] = fullStack.Scylla, ["ScyllaPort"] = fullStack.ScyllaPort.ToString(),
                    ["Keyspace"] = fullStack.Keyspace, ["Datacenter"] = fullStack.Datacenter,
                    ["Redis"] = fullStack.Redis, ["CacheNamespace"] = fullStack.CacheNamespace
                }) start.Environment["Storage__FullStack__" + pair.Key] = pair.Value;
            }
            start.Environment["ASPNETCORE_ENVIRONMENT"] = validationConnection is null ? "Production" : "Development";
            if (validationConnection is not null)
            {
                start.Environment["Storage__ValidationAcknowledged"] = "true";
                start.Environment["Storage__ValidationConnectionString"] = validationConnection;
                if (validationSeed is not null) start.Environment["Storage__ValidationSeedPath"] = validationSeed;
            }
            var process = Process.Start(start) ?? throw new Exception("backend process did not start");
            var logPrefix = Path.Combine(Root, $"backend-{process.Id}");
            var logLock = new object();
            process.OutputDataReceived += (_, e) => { if (e.Data is not null) lock (logLock) File.AppendAllText(logPrefix + ".stdout.log", e.Data + Environment.NewLine); };
            process.ErrorDataReceived += (_, e) => { if (e.Data is not null) lock (logLock) File.AppendAllText(logPrefix + ".stderr.log", e.Data + Environment.NewLine); };
            Console.WriteLine($"Backend started: pid={process.Id}; logs={logPrefix}; data={dataPath}");
            process.BeginOutputReadLine(); process.BeginErrorReadLine();
            var handler = new HttpClientHandler { UseProxy = false };
            if (tlsEnabled) handler.ServerCertificateCustomValidationCallback = (_, cert, _, errors) => ValidateCertificate(cert, errors);
            var server = new Server(process, new HttpClient(handler) { BaseAddress = new Uri(tlsEnabled ? $"https://localhost:{port}" : $"http://127.0.0.1:{port}"), Timeout = TimeSpan.FromSeconds(20) }, dataPath);
            try
            {
                if (proxyTlsEnabled)
                {
                    var proxyListener = new TcpListener(IPAddress.Loopback, 0); proxyListener.Start();
                    int proxyPort = ((IPEndPoint)proxyListener.LocalEndpoint).Port; proxyListener.Stop();
                    var configPath = Path.Combine(Root, $"caddy-{process.Id}.json");
                    var config = new
                    {
                        admin = new { disabled = true, config = new { persist = false } },
                        storage = new { module = "file_system", root = Path.Combine(Root, $"caddy-storage-{process.Id}") },
                        apps = new
                        {
                            tls = new { certificates = new { load_files = new[] { new { certificate = certificatePemPath, key = privateKeyPemPath } } } },
                            http = new { servers = new { isolated = new
                            {
                                listen = new[] { $"127.0.0.1:{proxyPort}" },
                                automatic_https = new { disable = true },
                                tls_connection_policies = new[] { new { } },
                                routes = new[] { new { handle = new[] { new { handler = "reverse_proxy", upstreams = new[] { new { dial = $"127.0.0.1:{port}" } } } } } }
                            } } }
                        }
                    };
                    File.WriteAllText(configPath, JsonSerializer.Serialize(config));
                    var proxyStart = new ProcessStartInfo(caddyPath) { UseShellExecute = false, CreateNoWindow = true, RedirectStandardOutput = true, RedirectStandardError = true, WorkingDirectory = Root };
                    proxyStart.ArgumentList.Add("run"); proxyStart.ArgumentList.Add("--config"); proxyStart.ArgumentList.Add(configPath);
                    foreach (var variable in new[] { "APPDATA", "LOCALAPPDATA", "XDG_DATA_HOME", "XDG_CONFIG_HOME" }) proxyStart.Environment[variable] = Root;
                    server.proxyProcess = Process.Start(proxyStart) ?? throw new Exception("Caddy process did not start");
                    var proxyLog = Path.Combine(Root, $"caddy-{server.proxyProcess.Id}");
                    server.proxyProcess.OutputDataReceived += (_, e) => { if (e.Data is not null) lock (logLock) File.AppendAllText(proxyLog + ".stdout.log", e.Data + Environment.NewLine); };
                    server.proxyProcess.ErrorDataReceived += (_, e) => { if (e.Data is not null) lock (logLock) File.AppendAllText(proxyLog + ".stderr.log", e.Data + Environment.NewLine); };
                    server.proxyProcess.BeginOutputReadLine(); server.proxyProcess.BeginErrorReadLine();
                    server.Client.BaseAddress = new Uri($"https://localhost:{proxyPort}");
                    Console.WriteLine($"Caddy started: pid={server.proxyProcess.Id}; listen=127.0.0.1:{proxyPort}; upstream=http://127.0.0.1:{port}; logs={proxyLog}; config={configPath}");
                }
                using var startupTimeout = new CancellationTokenSource(TimeSpan.FromSeconds(30));
                for (int i = 0; i < 100; i++)
                {
                    if (process.HasExited) throw new Exception("isolated backend exited with " + process.ExitCode);
                    if (server.proxyProcess is { HasExited: true } exitedProxy) throw new Exception("isolated Caddy exited with " + exitedProxy.ExitCode);
                    try { using var health = await server.Client.GetAsync("/health", startupTimeout.Token); if (health.IsSuccessStatusCode) return server; }
                    catch (HttpRequestException) { }
                    await Task.Delay(50);
                }
                throw new Exception("isolated backend health timeout");
            }
            catch { await server.DisposeAsync(); throw; }
        }
        public async Task<(byte[] Body, int Status, string? ContentType, double Milliseconds, double CpuMilliseconds, long WorkingSetBytes, long PrivateMemoryBytes)> BenchmarkRequest(bool proto, byte[] bytes, bool sampleResources = true)
        {
            using var request = new HttpRequestMessage(HttpMethod.Post, proto ? "/api/v1/rpc" : "/api/telegram/rpc");
            request.Headers.Authorization = new("Bearer", Accounts["alice"].Token);
            request.Content = new ByteArrayContent(bytes);
            request.Content.Headers.ContentType = new(proto ? Media : "application/json");
            if (sampleResources) process.Refresh();
            var cpuBefore = sampleResources ? process.TotalProcessorTime : TimeSpan.Zero;
            var start = Stopwatch.GetTimestamp();
            using var response = await client.SendAsync(request, HttpCompletionOption.ResponseHeadersRead);
            var body = await response.Content.ReadAsByteArrayAsync();
            var elapsed = Stopwatch.GetElapsedTime(start).TotalMilliseconds;
            if (sampleResources) process.Refresh();
            var cpu = sampleResources ? (process.TotalProcessorTime - cpuBefore).TotalMilliseconds : 0;
            return (body, (int)response.StatusCode, response.Content.Headers.ContentType?.MediaType, elapsed, cpu,
                sampleResources ? process.WorkingSet64 : 0, sampleResources ? process.PrivateMemorySize64 : 0);
        }

        public async Task<JsonNode> Json(string method, string path, object? body, string? token = null, int status = 200)
        {
            using var request = new HttpRequestMessage(new HttpMethod(method), path);
            if (token is not null) request.Headers.Authorization = new("Bearer", token);
            if (body is not null) request.Content = new StringContent(JsonSerializer.Serialize(body), System.Text.Encoding.UTF8, "application/json");
            using var response = await client.SendAsync(request);
            Check((int)response.StatusCode == status, method + " " + path + " HTTP expected " + status + " got " + (int)response.StatusCode);
            if (status == 204) return new JsonObject();
            return JsonNode.Parse(await response.Content.ReadAsStringAsync())!;
        }
        public async Task<P.RpcResponse> Rpc(P.RpcRequest request, string actor = "alice", int status = 200, string? code = null)
        {
            var response = await Raw(request.ToByteArray(), status, code, Accounts[actor].Token);
            if (status == 200)
            {
                Check(response.ProtocolMajor == 1 && response.RequestId == request.RequestId, "response envelope correlation");
                Check((int)response.OutcomeCase == (int)request.OperationCase, "response operation corresponds to request");
            }
            return response;
        }
        public async Task<P.RpcResponse> Raw(byte[] bytes, int status, string? code, string? token = "DEFAULT", string media = Media, bool chunked = false, string? encoding = null)
        {
            using var request = new HttpRequestMessage(HttpMethod.Post, "/api/v1/rpc");
            if (token == "DEFAULT") token = Accounts["alice"].Token;
            if (token is not null) request.Headers.Authorization = new("Bearer", token);
            request.Content = chunked ? new UnknownLengthContent(bytes) : new ByteArrayContent(bytes);
            request.Content.Headers.ContentType = new MediaTypeHeaderValue(media);
            if (encoding is not null) request.Content.Headers.ContentEncoding.Add(encoding);
            using var response = await client.SendAsync(request);
            Check((int)response.StatusCode == status, "protobuf HTTP expected " + status + " got " + (int)response.StatusCode);
            var result = await response.Content.ReadAsByteArrayAsync();
            var type = response.Content.Headers.ContentType?.MediaType;
            if (status is 401 or 429 && type == "application/json")
            {
                var json = JsonNode.Parse(result)!;
                if (code is not null) Check(Str(json, "error") == code, "middleware JSON error code");
                return new();
            }
            Check(type == Media, "protobuf response content type");
            var parsed = P.RpcResponse.Parser.ParseFrom(result);
            if (status != 200)
            {
                Check(parsed.Error is not null && parsed.Error.HttpStatus == status, "protobuf error HTTP status");
                Check(parsed.Error!.Code == code, "protobuf error code expected " + code + " got " + parsed.Error.Code);
            }
            return parsed;
        }
        public async Task AssertRateLimit(P.RpcRequest state)
        {
            for (int i = 0; i < 301; i++)
            {
                using var request = new HttpRequestMessage(HttpMethod.Post, "/api/v1/rpc");
                request.Headers.Authorization = new("Bearer", Accounts["alice"].Token);
                request.Content = new ByteArrayContent(state.ToByteArray());
                request.Content.Headers.ContentType = new(Media);
                using var response = await client.SendAsync(request);
                if ((int)response.StatusCode == 429)
                {
                    var bytes = await response.Content.ReadAsByteArrayAsync();
                    if (response.Content.Headers.ContentType?.MediaType == Media)
                    {
                        var error = P.RpcResponse.Parser.ParseFrom(bytes).Error;
                        Check(error is not null && error.HttpStatus == 429, "rate limit protobuf status");
                    }
                    else if (bytes.Length > 0)
                    {
                        Check(response.Content.Headers.ContentType?.MediaType == "application/json", "rate limit JSON media");
                        _ = JsonNode.Parse(bytes);
                    }
                    Check(true, "HTTP 429 reached within API permit budget"); return;
                }
                Check(response.StatusCode == HttpStatusCode.OK, "rate limit preceding responses successful");
            }
            throw new Exception("API limiter did not reject >300 requests");
        }
        public async Task ChunkedOversize()
        {
            using var tcp = new TcpClient();
            await tcp.ConnectAsync(client.BaseAddress!.Host, client.BaseAddress.Port);
            using var stream = tcp.GetStream();
            using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(10));
            var header = System.Text.Encoding.ASCII.GetBytes($"POST /api/v1/rpc HTTP/1.1\r\nHost: {client.BaseAddress.Authority}\r\nAuthorization: Bearer {Accounts["alice"].Token}\r\nContent-Type: {Media}\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n20001\r\n");
            var wire = header.Concat(new byte[128 * 1024 + 1]).Concat("\r\n0\r\n\r\n"u8.ToArray()).ToArray();
            try { await stream.WriteAsync(wire, timeout.Token); }
            catch (IOException) { /* The server may reject while the upload is in flight; still require its response. */ }
            async Task<string> Line()
            {
                var bytes = new List<byte>(); var one = new byte[1];
                while (await stream.ReadAsync(one, timeout.Token) != 0)
                {
                    if (one[0] == 10) return System.Text.Encoding.ASCII.GetString(bytes.ToArray()).TrimEnd('\r');
                    bytes.Add(one[0]);
                }
                throw new Exception("chunked response ended before headers/body");
            }
            var status = await Line();
            Check(status.Contains(" 413 ", StringComparison.Ordinal), "chunked upload HTTP 413");
            var headers = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
            string line;
            while ((line = await Line()).Length != 0)
            {
                int colon = line.IndexOf(':'); headers[line[..colon]] = line[(colon + 1)..].Trim();
            }
            Check(headers["Content-Type"].Split(';')[0] == Media, "chunked error protobuf media");
            using var body = new MemoryStream();
            if (headers.TryGetValue("Transfer-Encoding", out var transfer) && transfer == "chunked")
            {
                while (true)
                {
                    int length = Convert.ToInt32((await Line()).Split(';')[0], 16);
                    if (length == 0) break;
                    var chunk = new byte[length]; await stream.ReadExactlyAsync(chunk, timeout.Token); body.Write(chunk);
                    Check((await Line()).Length == 0, "chunk terminator");
                }
            }
            else
            {
                var bytes = new byte[int.Parse(headers["Content-Length"], System.Globalization.CultureInfo.InvariantCulture)];
                await stream.ReadExactlyAsync(bytes, timeout.Token); body.Write(bytes);
            }
            var error = P.RpcResponse.Parser.ParseFrom(body.ToArray()).Error;
            Check(error is not null && error.HttpStatus == 413 && error.Code == "BOTGRAM_REQUEST_INVALID", "chunked size protobuf status/code");
        }
        public async Task CancelUpload(P.RpcRequest request)
        {
            using var cancellation = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            using var message = new HttpRequestMessage(HttpMethod.Post, "/api/v1/rpc");
            message.Headers.Authorization = new("Bearer", Accounts["alice"].Token);
            message.Content = new PausedUpload(request.ToByteArray(), entered, cancellation.Token);
            message.Content.Headers.ContentType = new MediaTypeHeaderValue(Media);
            var task = client.SendAsync(message, cancellation.Token);
            await entered.Task.WaitAsync(TimeSpan.FromSeconds(5));
            cancellation.Cancel();
            try { using var ignored = await task; }
            catch (OperationCanceledException) { }
            catch (HttpRequestException) when (cancellation.IsCancellationRequested) { }
        }
        [System.Runtime.InteropServices.DllImport("kernel32.dll", SetLastError = true)]
        [return: System.Runtime.InteropServices.MarshalAs(System.Runtime.InteropServices.UnmanagedType.Bool)]
        private static extern bool QueryProcessCycleTime(IntPtr processHandle, out ulong cycleTime);
        public (double CpuMilliseconds, long WorkingSetBytes, long PrivateMemoryBytes, ulong CpuCycles) Resources()
        {
            process.Refresh();
            ulong cycles = 0;
            if (OperatingSystem.IsWindows() && !QueryProcessCycleTime(process.Handle, out cycles))
                throw new System.ComponentModel.Win32Exception(System.Runtime.InteropServices.Marshal.GetLastWin32Error());
            return (process.TotalProcessorTime.TotalMilliseconds, process.WorkingSet64, process.PrivateMemorySize64, cycles);
        }
        public bool Exited => process.HasExited;
        public long LifetimePeakPagedMemoryBytes { get { process.Refresh(); return process.PeakPagedMemorySize64; } }
        // Counters are OS-observed handles/threads of the isolated backend process; sampled, never a leak proof.
        public (int Threads, int Handles) Counters()
        {
            process.Refresh();
            return (process.Threads.Count, process.HandleCount);
        }
        // Default platform trust must reject the temporary self-signed leaf without an exact pin.
        public async Task<bool> UntrustedTlsRejected()
        {
            using var untrusted = new HttpClient { BaseAddress = client.BaseAddress, Timeout = TimeSpan.FromSeconds(10) };
            try
            {
                using var response = await untrusted.GetAsync("/health");
                return false;
            }
            catch (HttpRequestException) { return true; }
        }
        public async Task<bool> MismatchedTlsHostRejected()
        {
            bool nameMismatchObserved = false;
            bool certificateObserved = false;
            using var handler = new HttpClientHandler();
            handler.ServerCertificateCustomValidationCallback = (_, cert, _, errors) =>
            {
                certificateObserved = true;
                nameMismatchObserved |= (errors & System.Net.Security.SslPolicyErrors.RemoteCertificateNameMismatch) != 0;
                return ValidateCertificate(cert, errors);
            };
            using var invalidHost = new HttpClient(handler)
            {
                BaseAddress = new UriBuilder(client.BaseAddress!) { Host = "127.0.0.1" }.Uri,
                Timeout = TimeSpan.FromSeconds(10)
            };
            try { using var response = await invalidHost.GetAsync("/health"); return false; }
            catch (HttpRequestException ex)
            {
                // Caddy can reject IP/no-SNI before sending a leaf; do not count connection errors as TLS rejection.
                var handshakeRejected = proxyTlsEnabled && !certificateObserved && ex.InnerException is System.Security.Authentication.AuthenticationException;
                Console.WriteLine($"TLS invalid-host rejection: platformNameMismatch={nameMismatchObserved}; proxyHandshakeRejected={handshakeRejected}");
                return nameMismatchObserved || handshakeRejected;
            }
        }
        public Task SampleMemory(CancellationToken token, List<(double ElapsedMs, long WorkingSetBytes, long PrivateMemoryBytes)> samples) => Task.Run(async () =>
        {
            using var observed = Process.GetProcessById(process.Id);
            var started = Stopwatch.GetTimestamp();
            while (!token.IsCancellationRequested)
            {
                observed.Refresh();
                samples.Add((Stopwatch.GetElapsedTime(started).TotalMilliseconds, observed.WorkingSet64, observed.PrivateMemorySize64));
                try { await Task.Delay(2, token); }
                catch (OperationCanceledException) when (token.IsCancellationRequested) { break; }
            }
        });
        public async Task ObserveHealth()
        {
            var before = Resources();
            for (int i = 0; i < 5; i++)
            {
                await Task.Delay(1000);
                using var response = await client.GetAsync("/health");
                Check(response.IsSuccessStatusCode && !process.HasExited, "post-negative health stop condition");
                var sample = Resources();
                Console.WriteLine($"resource observation second={i + 1} cpuTotalMs={sample.CpuMilliseconds} workingSetBytes={sample.WorkingSetBytes} privateMemoryBytes={sample.PrivateMemoryBytes} health=200");
            }
            Console.WriteLine($"resource observation cpuDeltaMs={Resources().CpuMilliseconds - before.CpuMilliseconds}; samples=5; interval=1s; not continuous peaks");
        }
        public async ValueTask DisposeAsync()
        {
            client.Dispose();
            if (proxyProcess is not null)
            {
                bool proxyStoppedByTest = !proxyProcess.HasExited;
                if (proxyStoppedByTest) { proxyProcess.Kill(entireProcessTree: true); await proxyProcess.WaitForExitAsync(); }
                proxyProcess.WaitForExit();
                Console.WriteLine($"Caddy stopped: pid={proxyProcess.Id}; exit={proxyProcess.ExitCode}; stoppedByTest={proxyStoppedByTest}");
                proxyProcess.Dispose();
            }
            bool stoppedByTest = !process.HasExited;
            if (stoppedByTest) { process.Kill(entireProcessTree: true); await process.WaitForExitAsync(); }
            process.WaitForExit();
            Console.WriteLine($"Backend stopped: pid={process.Id}; exit={process.ExitCode}; stoppedByTest={stoppedByTest}");
            process.Dispose();
        }
    }
    private sealed class UnknownLengthContent(byte[] bytes) : HttpContent
    {
        protected override bool TryComputeLength(out long length) { length = 0; return false; }
        protected override async Task SerializeToStreamAsync(Stream stream, TransportContext? context) => await stream.WriteAsync(bytes);
    }
    private sealed class PausedUpload(byte[] bytes, TaskCompletionSource entered, CancellationToken token) : HttpContent
    {
        protected override bool TryComputeLength(out long length) { length = 0; return false; }
        protected override async Task SerializeToStreamAsync(Stream stream, TransportContext? context)
        {
            await stream.WriteAsync(bytes.AsMemory(0, Math.Max(1, bytes.Length / 2)), token);
            await stream.FlushAsync(token);
            entered.TrySetResult();
            await Task.Delay(Timeout.Infinite, token);
        }
    }
}
