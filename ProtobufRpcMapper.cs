using System.Globalization;
using System.Security.Cryptography;
using System.Text.Json.Nodes;
using Google.Protobuf;
using P = Botgram.Protocol.V1;

namespace Botgram.Backend;

public static class ProtobufRpcMapper
{
    public static P.RpcResponse Execute(Database db, string uid, P.RpcRequest request, string serverDomain, Session? session = null)
        => new Adapter(db, uid, serverDomain, authenticatedSession: session).Execute(request);

    public static void ValidateCount(int count) => Adapter.Count(count);
    public static void ValidateUnique<T>(IEnumerable<T> values) => Adapter.Unique(values);
    public static long ValidateMessageId(long value, bool positive = false) => Adapter.MessageId(value, positive);
    public static int ValidateLimit(bool present, int value, int fallback) => Adapter.Limit(present, value, fallback);
    public static P.PhoneCallProtocol MapProtocol(JsonObject? protocol) => Adapter.Protocol(protocol);
    public static JsonObject? MapProtocolJson(P.PhoneCallProtocol? protocol) => Adapter.ProtocolJson(protocol);
    public static P.PhoneCall MapPhoneCall(Call call, long adminTlUserId, long participantTlUserId, bool callerView)
        => Adapter.PhoneCall(call, adminTlUserId, participantTlUserId, callerView);

    public static P.Message MapMessage(Message message, P.PeerRef peer, string uid, string domain, long read)
    {
        using var diagnostic = RpcDiagnosticStage.Start("protobuf_message_map_ms");
        P.UserRef Reference(string id) => new() { ServerDomain = domain, UserId = id };
        var result = new P.Message
        {
            MessageId = ValidateMessageId(message.Id, true), Peer = peer, Sender = Reference(message.SenderId),
            CreatedAtMs = message.CreatedAt.ToUnixTimeMilliseconds(), Outgoing = message.SenderId == uid,
            Unread = message.ServiceTitle is null && message.SenderId != uid && message.Id > read
        };
        if (message.ServiceTitle is not null)
        {
            var action = new P.GroupCreatedAction { Title = message.ServiceTitle };
            action.Members.Add((message.ServiceMembers ?? []).Select(Reference));
            result.Service = new() { GroupCreated = action };
        }
        else
        {
            result.Text = new() { Text = message.Text, Silent = message.Silent };
            result.Text.Entities.Add((message.TelegramEntities ?? []).Select(Adapter.Entity));
            if (message.ReplyToMessageId is { } reply)
                result.Text.Reply = new() { MessageId = ValidateMessageId(reply, true) };
        }
        return result;
    }

    public static string UserReferenceId(P.UserRef? reference, string domain)
    {
        if (reference is null) throw new ApiException(400, "BOTGRAM_RPC_INVALID_ARGUMENT");
        if (!string.Equals(reference.ServerDomain, domain, StringComparison.Ordinal))
            throw new ApiException(501, "BOTGRAM_REMOTE_DOMAIN_UNSUPPORTED");
        if (!Guid.TryParseExact(reference.UserId, "N", out _))
            throw new ApiException(400, "BOTGRAM_RPC_INVALID_ARGUMENT");
        return reference.UserId;
    }

    public static string SelectedUserId(P.UserSelector? selector, string uid, string domain) =>
        selector?.TargetCase switch
        {
            P.UserSelector.TargetOneofCase.User => UserReferenceId(selector.User, domain),
            P.UserSelector.TargetOneofCase.Self when selector.Self => uid,
            _ => throw new ApiException(400, "BOTGRAM_RPC_INVALID_ARGUMENT")
        };

    public static P.UserSummary MapUser(UserProfile user, string domain) => new()
    {
        Identity = new() { ServerDomain = domain, UserId = user.Id }, TlUserId = user.TelegramId,
        Username = user.Username, DisplayName = user.DisplayName, FirstName = user.FirstName, LastName = user.LastName,
        Gid = user.Gid, IsSelf = user.IsSelf, IsContact = user.IsContact, IsMutualContact = user.IsMutual,
        NyarugramPremium = user.Premium, Status = MapStatus(user)
    };

    public static P.UserStatus MapStatus(UserProfile user)
    {
        var now = DateTimeOffset.UtcNow;
        if (user.LastSeen is not { } seen) return new();
        return user.Online && now - seen <= PresenceService.OnlineWindow
            ? new() { OnlineUntilMs = (seen + PresenceService.OnlineWindow).ToUnixTimeMilliseconds() }
            : new() { LastSeenMs = seen.ToUnixTimeMilliseconds() };
    }

    public static P.GroupSummary MapGroup(Conversation conversation, long telegramId) => new()
    {
        GroupId = conversation.Id, TlChatId = telegramId, Title = conversation.Title,
        ParticipantCount = conversation.Members.Length, CreatedAtMs = conversation.CreatedAt.ToUnixTimeMilliseconds(), Version = 1
    };

    public static Conversation ResolveRelationalPeer(Database view, string uid, P.PeerRef? peer, string domain)
    {
        if (peer?.PeerCase == P.PeerRef.PeerOneofCase.GroupId && Guid.TryParseExact(peer.GroupId, "N", out _) &&
            !view.Conversations.Any(c => c.Kind == "group" && c.Id == peer.GroupId && c.Members.Contains(uid)))
            throw new ApiException(404, "PEER_ID_INVALID");
        var id = new Adapter(view, uid, domain).PeerId(peer);
        return view.Conversations.FirstOrDefault(c => c.Members.Contains(uid) && IdentityService.DialogId(view, c, uid) == id)
            ?? throw new ApiException(501, "BOTGRAM_RELATIONAL_CONVERSATION_NOT_PRESEEDED");
    }

    public static HistoryQuery RelationalHistoryQuery(P.GetHistoryRequest input)
    {
        if (input.OffsetDateMs < 0 || input.OffsetDateMs % 1000 != 0) throw new ApiException(400, "BOTGRAM_RPC_INVALID_ARGUMENT");
        return new(Adapter.MessageId(input.OffsetMessageId), input.OffsetDateMs / 1000, input.AddOffset,
            Adapter.MessageId(input.MinMessageId), Adapter.MessageId(input.MaxMessageId), Adapter.Limit(input.HasLimit, input.Limit, 50));
    }

    public static SendMessageCommand RelationalSendCommand(P.SendMessageRequest input)
    {
        MessageEntityService.ValidateCount(input.Entities.Count);
        return new("tl:" + input.RandomId.ToString(CultureInfo.InvariantCulture), input.Text,
            input.Entities.Count == 0 ? null : input.Entities.Select(e => Adapter.Entity(e, input.Text)).ToArray(),
            input.HasReplyToMessageId ? Adapter.MessageId(input.ReplyToMessageId, true) : null, input.Silent);
    }

    public static P.RpcResponse MapRelationalHistory(Database view, string uid, P.RpcRequest request, string domain, Conversation conversation)
        => new() { ProtocolMajor = 1, RequestId = request.RequestId,
            GetHistory = new Adapter(view, uid, domain).Messages(view.Messages, UserQueryService.Participants(view, conversation, uid)) };

    public static P.RpcResponse MapRelationalSend(Database view, string uid, P.RpcRequest request, string domain, Message message, int pts)
    {
        var adapter = new Adapter(view, uid, domain, pts);
        var mapping = new P.MessageIdMapping { RandomId = request.SendMessage.RandomId, MessageId = Adapter.MessageId(message.Id, true) };
        var mapped = adapter.Message(message);
        return new() { ProtocolMajor = 1, RequestId = request.RequestId, SendMessage = new()
        {
            Message = mapped, Mapping = mapping,
            Updates = adapter.Updates([new P.Update { MessageId = mapping }, adapter.NewMessage(message, mapped)])
        } };
    }

    private sealed class Adapter(Database db, string uid, string serverDomain, int? ptsOverride = null, Session? authenticatedSession = null)
    {
        private static long Now => DateTimeOffset.UtcNow.ToUnixTimeSeconds() * 1000;
        private static ApiException Invalid() => new(400, "BOTGRAM_RPC_INVALID_ARGUMENT");
        public static void Count(int count)
        {
            if (count > 100) throw Invalid();
        }
        public static void Unique<T>(IEnumerable<T> values)
        {
            var seen = new HashSet<T>();
            foreach (var value in values)
                if (!seen.Add(value)) throw Invalid();
        }
        public static long MessageId(long value, bool positive = false)
        {
            if (value < (positive ? 1 : 0) || value > int.MaxValue) throw Invalid();
            return value;
        }
        public static int Limit(bool present, int value, int fallback)
        {
            if (!present) return fallback;
            if (value is < 1 or > 100) throw Invalid();
            return value;
        }
        private static void Folder(int value)
        {
            if (value < 0) throw Invalid();
        }
        private User FindUser(P.UserRef? reference)
        {
            var id = UserReferenceId(reference, serverDomain);
            return db.Users.FirstOrDefault(u => u.Id == id)
                ?? throw new ApiException(404, "USER_ID_INVALID");
        }
        private User SelectUser(P.UserSelector? selector)
        {
            if (selector is null) throw Invalid();
            return selector.TargetCase switch
            {
                P.UserSelector.TargetOneofCase.User => FindUser(selector.User),
                P.UserSelector.TargetOneofCase.Self when selector.Self => db.Users.Single(u => u.Id == uid),
                _ => throw Invalid()
            };
        }
        private Conversation FindGroup(string id)
        {
            if (!Guid.TryParseExact(id, "N", out _)) throw Invalid();
            return ConversationQueryService.Owned(db, uid).FirstOrDefault(c => c.Id == id && c.Kind == "group")
                ?? throw new ApiException(404, "PEER_ID_INVALID");
        }
        public long PeerId(P.PeerRef? peer)
        {
            if (peer is null) throw Invalid();
            return peer.PeerCase switch
            {
                P.PeerRef.PeerOneofCase.User => IdentityService.UserId(db, FindUser(peer.User).Id),
                P.PeerRef.PeerOneofCase.GroupId => -IdentityService.ChatId(db, FindGroup(peer.GroupId)),
                P.PeerRef.PeerOneofCase.Self when peer.Self => IdentityService.UserId(db, uid),
                _ => throw Invalid()
            };
        }
        private P.UserRef Reference(string id) => new() { ServerDomain = serverDomain, UserId = id };
        private P.PeerRef Peer(long id) => id > 0
            ? new() { User = Reference(IdentityService.FindUser(db, id).Id) }
            : new() { GroupId = ConversationQueryService.Owned(db, uid)
                .Single(c => c.Kind == "group" && IdentityService.ChatId(db, c) == -id).Id };
        private P.UserSummary User(UserProfile user) => MapUser(user, serverDomain);
        private P.GroupSummary Group(Conversation conversation) => MapGroup(conversation, IdentityService.ChatId(db, conversation));
        private IEnumerable<P.UserSummary> Related(bool contacts) => UserQueryService.Related(db, uid, contacts).Select(User);
        private IEnumerable<P.GroupSummary> Groups() => ConversationQueryService.Owned(db, uid).Where(c => c.Kind == "group").Select(Group);
        private P.SyncState State(int? pts = null) => new() { Pts = pts ?? SyncService.Pts(db, uid), DateMs = Now };

        public static MessageEntity Entity(P.MessageEntity entity, string text)
        {
            var kind = entity.KindCase switch
            {
                P.MessageEntity.KindOneofCase.Bold => MessageEntityKind.Bold,
                P.MessageEntity.KindOneofCase.Italic => MessageEntityKind.Italic,
                P.MessageEntity.KindOneofCase.Underline => MessageEntityKind.Underline,
                P.MessageEntity.KindOneofCase.Strike => MessageEntityKind.Strike,
                P.MessageEntity.KindOneofCase.Spoiler => MessageEntityKind.Spoiler,
                P.MessageEntity.KindOneofCase.Code => MessageEntityKind.Code,
                P.MessageEntity.KindOneofCase.Pre => MessageEntityKind.Pre,
                P.MessageEntity.KindOneofCase.Url => MessageEntityKind.Url,
                P.MessageEntity.KindOneofCase.TextUrl => MessageEntityKind.TextUrl,
                P.MessageEntity.KindOneofCase.Email => MessageEntityKind.Email,
                P.MessageEntity.KindOneofCase.Phone => MessageEntityKind.Phone,
                P.MessageEntity.KindOneofCase.Mention => MessageEntityKind.Mention,
                P.MessageEntity.KindOneofCase.Hashtag => MessageEntityKind.Hashtag,
                P.MessageEntity.KindOneofCase.Cashtag => MessageEntityKind.Cashtag,
                P.MessageEntity.KindOneofCase.BotCommand => MessageEntityKind.BotCommand,
                _ => throw new ApiException(501, "BOTGRAM_ENTITY_UNSUPPORTED")
            };
            if (entity.OffsetUtf16 > int.MaxValue || entity.LengthUtf16 > int.MaxValue) throw Invalid();
            return MessageEntityService.Validate(text, new(kind, (int)entity.OffsetUtf16, (int)entity.LengthUtf16,
                entity.Pre?.Language ?? "", entity.TextUrl?.Url ?? ""));
        }
        public static P.MessageEntity Entity(MessageEntity entity)
        {
            MessageEntityService.ValidateKind(entity.Kind);
            var result = new P.MessageEntity { OffsetUtf16 = checked((uint)entity.Offset), LengthUtf16 = checked((uint)entity.Length) };
            switch (entity.Kind)
            {
                case MessageEntityKind.Bold: result.Bold = new(); break;
                case MessageEntityKind.Italic: result.Italic = new(); break;
                case MessageEntityKind.Underline: result.Underline = new(); break;
                case MessageEntityKind.Strike: result.Strike = new(); break;
                case MessageEntityKind.Spoiler: result.Spoiler = new(); break;
                case MessageEntityKind.Code: result.Code = new(); break;
                case MessageEntityKind.Pre: result.Pre = new() { Language = entity.Language }; break;
                case MessageEntityKind.Url: result.Url = new(); break;
                case MessageEntityKind.TextUrl: result.TextUrl = new() { Url = entity.Url }; break;
                case MessageEntityKind.Email: result.Email = new(); break;
                case MessageEntityKind.Phone: result.Phone = new(); break;
                case MessageEntityKind.Mention: result.Mention = new(); break;
                case MessageEntityKind.Hashtag: result.Hashtag = new(); break;
                case MessageEntityKind.Cashtag: result.Cashtag = new(); break;
                case MessageEntityKind.BotCommand: result.BotCommand = new(); break;
            }
            return result;
        }
        public P.Message Message(Message message)
        {
            var view = ConversationQueryService.DescribeMessage(db, message, uid);
            var read = db.Receipts.FirstOrDefault(r => r.ConversationId == message.ConversationId && r.UserId == uid)?.MessageId ?? 0;
            return MapMessage(message, Peer(view.PeerId), uid, serverDomain, read);
        }
        private P.Dialog Dialog(DialogView view) => new()
        {
            Peer = Peer(view.PeerId), TopMessageId = MessageId(view.LastMessage?.Id ?? 0),
            ReadInboxMaxId = MessageId(view.Inbox), ReadOutboxMaxId = MessageId(view.Outbox),
            UnreadCount = view.UnreadCount, NotifySettings = new()
        };
        public P.Update NewMessage(Message message, P.Message? mapped = null) => new()
        {
            NewMessage = new() { Message = mapped ?? Message(message), Pts = ptsOverride ?? SyncService.Pts(db, uid), PtsCount = 1 }
        };
        public P.Updates Updates(IEnumerable<P.Update> updates, IEnumerable<Conversation>? conversations = null)
        {
            var result = new P.Updates { DateMs = Now };
            result.Updates_.Add(updates);
            result.Users.Add(Related(false));
            result.Groups.Add((conversations ?? ConversationQueryService.Owned(db, uid)).Where(c => c.Kind == "group").Select(Group));
            return result;
        }
        private P.Difference Difference(int pts)
        {
            var difference = SyncService.Difference(db, uid, pts);
            if (difference.Kind == DifferenceKind.TooLong) return new() { TooLong = new() { Pts = difference.Pts } };
            if (difference.Kind == DifferenceKind.Empty) return new() { Empty = new() { DateMs = Now } };
            var page = new P.DifferencePage { State = State(difference.Pts) };
            for (int i = 0; i < difference.Events.Length; i++)
            {
                if (difference.Hidden?[i] == true) continue;
                var e = difference.Events[i];
                switch (e.Payload)
                {
                    case Message message:
                        page.Messages.Add(Message(message));
                        break;
                    case GidChanged changed:
                        var user = UserQueryService.Find(db, IdentityService.UserId(db, changed.UserId), uid);
                        page.Updates.Add(new P.Update { GidChanged = new() { User = Reference(user.Id), Gid = user.Gid } });
                        break;
                    case Call call when call.TelegramId != 0:
                        page.Updates.Add(PhoneUpdate(call));
                        break;
                    case CallSignal signal when signal.Type == "tl":
                        var tlCall = db.Calls.FirstOrDefault(item => item.Id == signal.CallId && item.TelegramId != 0);
                        if (tlCall is not null)
                            page.Updates.Add(new P.Update
                            {
                                PhoneCallSignaling = new()
                                {
                                    CallId = tlCall.TelegramId,
                                    Data = ByteString.CopyFrom(Convert.FromBase64String(signal.Data))
                                }
                            });
                        break;
                    case PresenceChanged presence:
                        var presenceUser = UserQueryService.Find(db, IdentityService.UserId(db, presence.UserId), uid);
                        page.Updates.Add(new P.Update { PresenceChanged = new() { User = Reference(presenceUser.Id), Status = MapStatus(presenceUser) } });
                        break;
                    case Receipt receipt:
                        var conversation = Auth.Conversation(db, receipt.ConversationId, uid);
                        var read = new P.ReadReceipt
                        {
                            Peer = Peer(IdentityService.DialogId(db, conversation, uid)), MaxMessageId = MessageId(receipt.MessageId),
                            Pts = difference.PreviousCount + i + 2, PtsCount = 1
                        };
                        page.Updates.Add(receipt.UserId == uid ? new P.Update { ReadInbox = read } : new P.Update { ReadOutbox = read });
                        break;
                }
            }
            page.Users.Add(UserQueryService.Related(db, uid, true)
                .Concat(difference.Events.Select(e => e.Payload).OfType<PresenceChanged>()
                    .Select(p => UserQueryService.Find(db, IdentityService.UserId(db, p.UserId), uid)))
                .DistinctBy(u => u.Id).Select(User));
            page.Groups.Add(Groups());
            return difference.Kind == DifferenceKind.Slice ? new() { Slice = page } : new() { Complete = page };
        }
        private P.PeerDialogs PeerDialogs(DialogPage page)
        {
            var result = new P.PeerDialogs { State = State() };
            result.Dialogs.Add(page.Dialogs.Select(Dialog));
            result.Messages.Add(page.Dialogs.Where(d => d.LastMessage is not null).Select(d => Message(d.LastMessage!)));
            result.Users.Add(Related(true));
            result.Groups.Add(Groups());
            return result;
        }
        public P.MessagePage Messages(IEnumerable<Message> messages, IEnumerable<UserProfile>? users = null)
        {
            var result = new P.MessagePage();
            result.Messages.Add(messages.Select(Message));
            result.Users.Add(users is null ? Related(true) : users.Select(User));
            result.Groups.Add(Groups());
            return result;
        }
        private P.ResolvedIdentities ResolveIdentities(P.ResolveIdentitiesRequest input)
        {
            Count(input.TlUserIds.Count + input.TlChatIds.Count);
            Unique(input.TlUserIds);
            Unique(input.TlChatIds);
            if (input.TlUserIds.Concat(input.TlChatIds).Any(id => id <= 0)) throw Invalid();
            var self = db.Users.Single(u => u.Id == uid);
            if (!db.TelegramIds.TryGetValue("user:" + uid, out var selfId) || selfId <= 0)
                throw new ApiException(404, "SELF_IDENTITY_NOT_FOUND");
            var result = new P.ResolvedIdentities { ServerDomain = serverDomain };
            result.Users.Add(User(UserQueryService.Profile(db, self, uid)));
            foreach (var id in input.TlUserIds)
            {
                var key = db.TelegramIds.FirstOrDefault(pair => pair.Value == id && pair.Key.StartsWith("user:", StringComparison.Ordinal)).Key;
                var user = key is null ? null : db.Users.FirstOrDefault(u => "user:" + u.Id == key);
                if (user is null) throw new ApiException(404, "USER_ID_INVALID");
                if (user.Id != uid) result.Users.Add(User(UserQueryService.Profile(db, user, uid)));
            }
            foreach (var id in input.TlChatIds)
            {
                var key = db.TelegramIds.FirstOrDefault(pair => pair.Value == id && pair.Key.StartsWith("chat:", StringComparison.Ordinal)).Key;
                var group = key is null ? null : ConversationQueryService.Owned(db, uid)
                    .FirstOrDefault(c => c.Kind == "group" && "chat:" + c.Id == key);
                if (group is null) throw new ApiException(404, "PEER_ID_INVALID");
                result.Groups.Add(Group(group));
            }
            return result;
        }

        private P.ResolvedPeer Resolved(UserProfile user)
        {
            var result = new P.ResolvedPeer { Peer = new() { User = Reference(user.Id) } };
            result.Users.Add(User(user));
            return result;
        }

        // ---------- phone.* (1:1 calls) ----------
        private static int Seconds(long milliseconds)
        {
            if (milliseconds < 0 || milliseconds % 1000 != 0 || milliseconds / 1000 > int.MaxValue) throw Invalid();
            return (int)(milliseconds / 1000);
        }
        private static long Milliseconds(int seconds) => checked((long)seconds * 1000);
        // Stored protocol JSON keeps plain ints, so numeric reads must accept every JSON integer shape.
        private static long Number(JsonNode? node, long min = long.MinValue, long max = long.MaxValue)
        {
            if (node is JsonValue value)
            {
                if (value.TryGetValue<long>(out var wide) && wide >= min && wide <= max) return wide;
                if (value.TryGetValue<int>(out var number) && number >= min && number <= max) return number;
                if (value.TryGetValue<double>(out var real) && real == Math.Floor(real) && real >= min && real <= max) return (long)real;
            }
            throw Invalid();
        }
        internal static P.PhoneCallProtocol Protocol(JsonObject? source)
        {
            var result = new P.PhoneCallProtocol();
            if (source is null) return result;
            result.UdpP2P = source["udp_p2p"]?.GetValue<bool>() ?? false;
            result.UdpReflector = source["udp_reflector"]?.GetValue<bool>() ?? false;
            result.MinLayer = (int)Number(source["min_layer"], 0, int.MaxValue);
            result.MaxLayer = (int)Number(source["max_layer"], 0, int.MaxValue);
            if (source["library_versions"] is JsonArray versions)
                result.LibraryVersions.Add(versions.Select(version => version?.GetValue<string>() ?? ""));
            return result;
        }
        internal static JsonObject? ProtocolJson(P.PhoneCallProtocol? protocol)
        {
            if (protocol is null) return null;
            return new JsonObject
            {
                ["$type"] = "org.telegram.tgnet.tl.TL_phone$TL_phoneCallProtocol",
                ["flags"] = (protocol.UdpP2P ? 1 : 0) | (protocol.UdpReflector ? 2 : 0),
                ["udp_p2p"] = protocol.UdpP2P,
                ["udp_reflector"] = protocol.UdpReflector,
                ["min_layer"] = protocol.MinLayer,
                ["max_layer"] = protocol.MaxLayer,
                ["library_versions"] = new JsonArray(protocol.LibraryVersions.Select(version => (JsonNode?)JsonValue.Create(version)).ToArray())
            };
        }
        private static P.PhoneCallDiscardReason DiscardReason(string reason) => reason switch
        {
            "Missed" => P.PhoneCallDiscardReason.Missed,
            "Busy" => P.PhoneCallDiscardReason.Busy,
            "Disconnect" => P.PhoneCallDiscardReason.Disconnect,
            _ => P.PhoneCallDiscardReason.Hangup
        };
        private P.PhoneCall PhoneCall(Call call) => PhoneCall(call,
            IdentityService.UserId(db, call.CallerId), IdentityService.UserId(db, call.CalleeId), call.CallerId == uid);
        internal static P.PhoneCall PhoneCall(Call call, long adminTlUserId, long participantTlUserId, bool callerView)
        {
            var result = new P.PhoneCall
            {
                CallId = call.TelegramId, AccessHash = call.AccessHash,
                State = call.State switch
                {
                    "ringing" => callerView ? P.PhoneCallState.Waiting : P.PhoneCallState.Requested,
                    "accepted" => P.PhoneCallState.Accepted,
                    "active" => P.PhoneCallState.Active,
                    _ => P.PhoneCallState.Discarded
                },
                AdminTlUserId = adminTlUserId,
                ParticipantTlUserId = participantTlUserId,
                DateMs = Milliseconds(call.Date), Video = call.Video, P2PAllowed = call.P2pAllowed,
                Protocol = Protocol(call.Protocol ?? CallService.DefaultProtocol()),
                ReceiveDateMs = Milliseconds(call.ReceiveDate), StartDateMs = Milliseconds(call.StartDate),
                GAHash = ByteString.CopyFrom(call.GAHash ?? []), GB = ByteString.CopyFrom(call.GB ?? []),
                GAOrB = ByteString.CopyFrom(call.GA ?? []), KeyFingerprint = call.KeyFingerprint,
                DiscardReason = DiscardReason(call.Reason), Duration = call.Duration
            };
            foreach (var connection in call.Connections ?? [])
            {
                long id = Number(connection["id"]);
                long port = Number(connection["port"], 0, int.MaxValue);
                result.Connections.Add(new P.PhoneConnection
                {
                    Id = id, Ip = connection["ip"]?.GetValue<string>() ?? "", Ipv6 = connection["ipv6"]?.GetValue<string>() ?? "",
                    Port = (int)port, Turn = connection["turn"]?.GetValue<bool>() ?? false,
                    Stun = connection["stun"]?.GetValue<bool>() ?? false,
                    Username = connection["username"]?.GetValue<string>() ?? "", Password = connection["password"]?.GetValue<string>() ?? ""
                });
            }
            return result;
        }
        private P.PhoneCallResult PhoneResult(Call call)
        {
            var result = new P.PhoneCallResult { PhoneCall = PhoneCall(call) };
            string other = call.CallerId == uid ? call.CalleeId : call.CallerId;
            result.Users.Add(User(UserQueryService.Find(db, IdentityService.UserId(db, other), uid)));
            return result;
        }
        private P.Update PhoneUpdate(Call call) => new() { PhoneCall = new() { PhoneCall = PhoneCall(call) } };
        private Call RequireCall(P.PhoneCallRef? peer)
        {
            if (peer is null) throw new ApiException(400, "CALL_NOT_FOUND");
            return CallService.Require(db, peer.CallId, peer.AccessHash, uid);
        }
        private P.PhoneCallResult RequestCall(P.PhoneRequestCallRequest input)
        {
            var callee = SelectUser(input.User);
            if (callee.Id == uid) throw new ApiException(400, "CALL_PEER_INVALID");
            if (!AccountPrivacyService.Allowed(db, callee.Id, uid, P.PrivacyKey.PhoneCall))
                throw new ApiException(403, "USER_PRIVACY_RESTRICTED");
            byte[] gaHash = input.GAHash.ToByteArray();
            var protocol = ProtocolJson(input.Protocol);
            var replay = db.Calls.FirstOrDefault(call => call.TelegramId != 0 && call.CallerId == uid && call.RandomId == input.RandomId);
            if (replay is not null)
            {
                if (replay.CalleeId != callee.Id || replay.Video != input.Video ||
                    !CryptographicOperations.FixedTimeEquals(replay.GAHash ?? [], gaHash) ||
                    !JsonNode.DeepEquals(replay.RequestProtocol ?? replay.Protocol, protocol))
                    throw new ApiException(409, "CALL_IDEMPOTENCY_CONFLICT");
                return PhoneResult(replay);
            }
            CallService.ValidateGaHash(gaHash);
            CallService.ValidateProtocol(protocol);
            bool p2p = CallService.EffectiveP2p(protocol!,
                AccountPrivacyService.Allowed(db, uid, callee.Id, P.PrivacyKey.PhoneP2P),
                AccountPrivacyService.Allowed(db, callee.Id, uid, P.PrivacyKey.PhoneP2P));
            CallService.RequireRelayWhenP2pDisabled(p2p);
            long peerId = IdentityService.UserId(db, callee.Id);
            var conversation = ConversationMessageService.FindByPeer(db, peerId, uid, create: true);
            CallService.ValidateCallee(conversation, uid, callee.Id);
            if (db.Calls.Any(call => CallService.InProgress(call.State) &&
                (call.CallerId == uid || call.CalleeId == uid || call.CallerId == callee.Id || call.CalleeId == callee.Id)))
                throw new ApiException(409, "CALL_BUSY");
            if (db.NextCallId == 0) db.NextCallId = 1_000_000_000;
            var created = new Call(Guid.NewGuid().ToString("N"), conversation.Id, uid, callee.Id, "ringing", DateTimeOffset.UtcNow,
                TelegramId: db.NextCallId++, AccessHash: CallService.NewAccessHash(), RandomId: input.RandomId,
                Video: input.Video, Protocol: CallService.NormalizeProtocol(protocol!, p2p),
                GAHash: gaHash, Date: Seconds(Now), P2pAllowed: p2p, RequestProtocol: protocol);
            CallService.Save(db, created);
            db.Emit([created.CallerId, created.CalleeId], "call.updated", created);
            return PhoneResult(created);
        }

        public P.RpcResponse Execute(P.RpcRequest request)
        {
            if (request.HasForeground)
                PresenceService.Report(db, authenticatedSession ?? throw new ApiException(401, "session_expired"), request.Foreground);
            var response = new P.RpcResponse { ProtocolMajor = 1, RequestId = request.RequestId };
            switch (request.OperationCase)
            {
                case P.RpcRequest.OperationOneofCase.AccountPrivacy:
                    response.AccountPrivacy = AccountPrivacyService.Execute(db,
                        authenticatedSession ?? throw new ApiException(401, "session_expired"), request.AccountPrivacy, serverDomain);
                    break;
                case P.RpcRequest.OperationOneofCase.ResolveIdentities:
                    response.ResolveIdentities = ResolveIdentities(request.ResolveIdentities);
                    break;
                case P.RpcRequest.OperationOneofCase.GetState:
                    response.GetState = State();
                    break;
                case P.RpcRequest.OperationOneofCase.GetDifference:
                    if (request.GetDifference.Pts < 0) throw Invalid();
                    response.GetDifference = Difference(request.GetDifference.Pts);
                    break;
                case P.RpcRequest.OperationOneofCase.GetDialogs:
                {
                    var input = request.GetDialogs;
                    Folder(input.FolderId);
                    var query = new DialogQuery(DialogSelection.All, input.FolderId, MessageId(input.OffsetMessageId),
                        input.OffsetPeer is null ? 0 : PeerId(input.OffsetPeer), Limit(input.HasLimit, input.Limit, 100));
                    var page = ConversationQueryService.Dialogs(db, uid, query);
                    var peers = PeerDialogs(page);
                    response.GetDialogs = new() { TotalCount = page.TotalCount, IsSlice = page.IsSlice };
                    response.GetDialogs.Dialogs.Add(peers.Dialogs);
                    response.GetDialogs.Messages.Add(peers.Messages);
                    response.GetDialogs.Users.Add(peers.Users);
                    response.GetDialogs.Groups.Add(peers.Groups);
                    break;
                }
                case P.RpcRequest.OperationOneofCase.GetPinnedDialogs:
                    response.GetPinnedDialogs = PeerDialogs(ConversationQueryService.Dialogs(db, uid, new(DialogSelection.Pinned)));
                    break;
                case P.RpcRequest.OperationOneofCase.GetPeerDialogs:
                {
                    var input = request.GetPeerDialogs;
                    Count(input.Peers.Count);
                    Folder(input.FolderId);
                    var peers = input.Peers.Select(PeerId).ToArray();
                    Unique(peers);
                    response.GetPeerDialogs = PeerDialogs(ConversationQueryService.Dialogs(db, uid,
                        new(DialogSelection.Peers, input.FolderId, Peers: peers)));
                    break;
                }
                case P.RpcRequest.OperationOneofCase.GetHistory:
                {
                    var input = request.GetHistory;
                    if (input.OffsetDateMs < 0 || input.OffsetDateMs % 1000 != 0) throw Invalid();
                    var query = new HistoryQuery(MessageId(input.OffsetMessageId), input.OffsetDateMs / 1000,
                        input.AddOffset, MessageId(input.MinMessageId), MessageId(input.MaxMessageId), Limit(input.HasLimit, input.Limit, 50));
                    var conversation = ConversationMessageService.FindByPeer(db, PeerId(input.Peer), uid, true);
                    response.GetHistory = Messages(ConversationMessageService.History(db, conversation, query),
                        UserQueryService.Participants(db, conversation, uid));
                    break;
                }
                case P.RpcRequest.OperationOneofCase.GetMessages:
                {
                    Count(request.GetMessages.MessageIds.Count);
                    var ids = request.GetMessages.MessageIds.Select(id => MessageId(id, true)).ToArray();
                    Unique(ids);
                    response.GetMessages = Messages(ConversationQueryService.Messages(db, uid, ids));
                    break;
                }
                case P.RpcRequest.OperationOneofCase.SendMessage:
                {
                    var input = request.SendMessage;
                    MessageEntityService.ValidateCount(input.Entities.Count);
                    var entities = input.Entities.Count == 0 ? null : input.Entities.Select(e => Entity(e, input.Text)).ToArray();
                    long? reply = input.HasReplyToMessageId ? MessageId(input.ReplyToMessageId, true) : null;
                    var conversation = ConversationMessageService.FindByPeer(db, PeerId(input.Peer), uid, true);
                    var message = ConversationMessageService.Send(db, conversation, uid,
                        new("tl:" + input.RandomId.ToString(CultureInfo.InvariantCulture), input.Text, entities, reply, input.Silent), MessageWriteContract.Telegram);
                    var mapping = new P.MessageIdMapping { RandomId = input.RandomId, MessageId = MessageId(message.Id, true) };
                    // Both fields describe the same immutable result during this request's encoding.
                    var mapped = Message(message);
                    response.SendMessage = new()
                    {
                        Message = mapped, Mapping = mapping,
                        Updates = Updates([new P.Update { MessageId = mapping }, NewMessage(message, mapped)])
                    };
                    break;
                }
                case P.RpcRequest.OperationOneofCase.ReadHistory:
                {
                    var input = request.ReadHistory;
                    var maxId = MessageId(input.MaxMessageId);
                    var conversation = ConversationMessageService.FindByPeer(db, PeerId(input.Peer), uid);
                    var result = ConversationMessageService.ReadThrough(db, conversation, uid, maxId);
                    response.ReadHistory = new() { Pts = SyncService.Pts(db, uid), PtsCount = result.Changed ? 1 : 0 };
                    break;
                }
                case P.RpcRequest.OperationOneofCase.SearchContacts:
                {
                    var found = ContactService.Search(db, request.SearchContacts.Query, uid);
                    response.SearchContacts = new();
                    response.SearchContacts.Users.Add(found.Select(User));
                    response.SearchContacts.Results.Add(found.Select(u => new P.PeerRef { User = Reference(u.Id) }));
                    break;
                }
                case P.RpcRequest.OperationOneofCase.ResolveUsername:
                    response.ResolveUsername = Resolved(ContactService.ResolveUsername(db, request.ResolveUsername.Username, uid));
                    break;
                case P.RpcRequest.OperationOneofCase.GetUsers:
                {
                    Count(request.GetUsers.Users.Count);
                    var ids = request.GetUsers.Users.Select(SelectUser).Select(u => u.Id).ToArray();
                    Unique(ids);
                    response.GetUsers = new();
                    response.GetUsers.Users.Add(UserQueryService.ByIds(db, ids.Select(id => IdentityService.UserId(db, id)).ToArray(), uid).Select(User));
                    break;
                }
                case P.RpcRequest.OperationOneofCase.GetSpecialContacts:
                    response.GetSpecialContacts = AccountPrivacyService.SpecialContacts(db, uid, null, serverDomain);
                    break;
                case P.RpcRequest.OperationOneofCase.SetSpecialContact:
                    response.SetSpecialContact = AccountPrivacyService.SpecialContacts(db, uid, request.SetSpecialContact, serverDomain);
                    break;
                case P.RpcRequest.OperationOneofCase.GetContacts:
                {
                    var contacts = ContactService.List(db, uid);
                    response.GetContacts = new() { SavedCount = contacts.Length };
                    response.GetContacts.Contacts.Add(contacts.Select(c => new P.Contact { User = Reference(c.User.Id), Mutual = c.Mutual }));
                    response.GetContacts.Users.Add(contacts.Select(c => User(c.User)));
                    break;
                }
                case P.RpcRequest.OperationOneofCase.ResolveGid:
                    response.ResolveGid = Resolved(ContactService.ResolveGid(db, request.ResolveGid.Gid, uid));
                    break;
                case P.RpcRequest.OperationOneofCase.AddContact:
                {
                    var input = request.AddContact;
                    var target = SelectUser(input.User);
                    var added = ContactService.Add(db, uid, IdentityService.UserId(db, target.Id), input.FirstName, input.LastName, false);
                    response.AddContact = new() { DateMs = Now };
                    response.AddContact.Users.Add(User(added));
                    break;
                }
                case P.RpcRequest.OperationOneofCase.RequestCall:
                    response.RequestCall = RequestCall(request.RequestCall);
                    break;
                case P.RpcRequest.OperationOneofCase.ReceivedCall:
                {
                    var call = RequireCall(request.ReceivedCall.Peer);
                    if (call.CalleeId != uid) throw new ApiException(400, "CALL_NOT_FOUND");
                    if (call.State == "ringing" && call.ReceiveDate == 0)
                    {
                        var updated = call with { ReceiveDate = Seconds(Now) };
                        CallService.Save(db, updated);
                        db.Emit([call.CallerId], "call.updated", updated);
                    }
                    response.ReceivedCall = new P.BoolValue { Value = true };
                    break;
                }
                case P.RpcRequest.OperationOneofCase.AcceptCall:
                {
                    var input = request.AcceptCall;
                    var call = RequireCall(input.Peer);
                    if (call.CalleeId != uid) throw new ApiException(400, "CALL_NOT_FOUND");
                    byte[] gb = input.GB.ToByteArray();
                    var protocol = ProtocolJson(input.Protocol);
                    CallService.ValidatePublicKey(gb, "CALL_INVALID_GB");
                    CallService.ValidateProtocol(protocol);
                    // Only the first accept wins; other devices may replay the same key material and proposal.
                    if (call.State is "accepted" or "active")
                    {
                        if (CryptographicOperations.FixedTimeEquals(call.GB ?? [], gb) &&
                            JsonNode.DeepEquals(call.AcceptProtocol ?? call.Protocol, protocol))
                        {
                            response.AcceptCall = PhoneResult(call);
                            break;
                        }
                        throw new ApiException(409, "CALL_ALREADY_ACCEPTED");
                    }
                    if (call.State != "ringing") throw new ApiException(400, "CALL_ALREADY_ACCEPTED");
                    // The accept can only further restrict P2P; privacy resolved at request time stays in force.
                    bool p2p = call.P2pAllowed && CallService.RequestsP2p(protocol!);
                    CallService.RequireRelayWhenP2pDisabled(p2p);
                    var negotiated = CallService.NegotiateProtocol(
                        call.RequestProtocol ?? call.Protocol ?? CallService.DefaultProtocol(), protocol!, p2p);
                    var updated = call with
                    {
                        State = "accepted", GB = gb, UpdatedAt = DateTimeOffset.UtcNow,
                        Protocol = negotiated, AcceptProtocol = protocol, P2pAllowed = p2p
                    };
                    CallService.Save(db, updated);
                    db.Emit([call.CallerId], "call.updated", updated);
                    response.AcceptCall = PhoneResult(updated);
                    break;
                }
                case P.RpcRequest.OperationOneofCase.ConfirmCall:
                {
                    var input = request.ConfirmCall;
                    var call = RequireCall(input.Peer);
                    if (call.CallerId != uid) throw new ApiException(400, "CALL_NOT_FOUND");
                    byte[] ga = input.GA.ToByteArray();
                    var protocol = ProtocolJson(input.Protocol);
                    CallService.ValidateGa(ga, call.GAHash);
                    // Confirm must echo the caller's original proposal; the negotiated result stays final.
                    CallService.RequireSameCapabilities(
                        call.RequestProtocol ?? call.Protocol ?? CallService.DefaultProtocol(), protocol!);
                    if (input.KeyFingerprint == 0) throw new ApiException(400, "CALL_INVALID_KEY_FINGERPRINT");
                    if (call.State == "active")
                    {
                        if (CryptographicOperations.FixedTimeEquals(call.GA ?? [], ga) && call.KeyFingerprint == input.KeyFingerprint)
                        {
                            response.ConfirmCall = PhoneResult(call);
                            break;
                        }
                        throw new ApiException(409, "CALL_ALREADY_ACTIVE");
                    }
                    if (call.State != "accepted") throw new ApiException(400, "CALL_NOT_ACCEPTED");
                    var updated = call with
                    {
                        State = "active", GA = ga, KeyFingerprint = input.KeyFingerprint,
                        StartDate = Seconds(Now), UpdatedAt = DateTimeOffset.UtcNow, Connections = CallService.PhoneConnections()
                    };
                    CallService.Save(db, updated);
                    db.Emit([call.CalleeId], "call.updated", updated);
                    response.ConfirmCall = PhoneResult(updated);
                    break;
                }
                case P.RpcRequest.OperationOneofCase.DiscardCall:
                {
                    var input = request.DiscardCall;
                    var call = RequireCall(input.Peer);
                    // A retry keeps the original terminal outcome instead of rewriting it.
                    if (call.State is not ("ended" or "rejected"))
                    {
                        int duration = input.Duration;
                        if (duration < 0) throw new ApiException(400, "CALL_INVALID_DURATION");
                        if (duration == 0 && call.StartDate != 0) duration = Math.Max(0, Seconds(Now) - call.StartDate);
                        string reason = input.Reason switch
                        {
                            P.PhoneCallDiscardReason.Missed => "Missed",
                            P.PhoneCallDiscardReason.Busy => "Busy",
                            P.PhoneCallDiscardReason.Disconnect => "Disconnect",
                            _ => "Hangup"
                        };
                        call = call with { State = "ended", Reason = reason, Duration = duration, UpdatedAt = DateTimeOffset.UtcNow };
                        CallService.Save(db, call);
                        db.Emit([call.CallerId, call.CalleeId], "call.updated", call);
                    }
                    response.DiscardCall = Updates([PhoneUpdate(call)]);
                    break;
                }
                case P.RpcRequest.OperationOneofCase.SendSignalingData:
                {
                    var input = request.SendSignalingData;
                    var call = RequireCall(input.Peer);
                    if (call.State != "active") throw new ApiException(400, "CALL_NOT_ACTIVE");
                    byte[] data = input.Data.ToByteArray();
                    if (data.Length is 0 or > CallService.MaximumSignalingBytes)
                        throw new ApiException(400, "CALL_SIGNALING_TOO_LARGE");
                    string other = call.CallerId == uid ? call.CalleeId : call.CallerId;
                    db.Emit([other], "call.tl_signal", new CallSignal(call.Id, uid, "tl", Convert.ToBase64String(data)),
                        DateTimeOffset.UtcNow.AddMinutes(5));
                    response.SendSignalingData = new P.BoolValue { Value = true };
                    break;
                }
                case P.RpcRequest.OperationOneofCase.GetCallConfig:
                    response.GetCallConfig = new P.DataJson { Data = CallService.CallConfigJson() };
                    break;
                case P.RpcRequest.OperationOneofCase.SaveCallDebug:
                    response.SaveCallDebug = new P.BoolValue { Value = true };
                    break;
                case P.RpcRequest.OperationOneofCase.GetDhConfig:
                {
                    var input = request.GetDhConfig;
                    var config = CallService.GetDhConfig(input.Version, input.RandomLength);
                    response.GetDhConfig = new P.DhConfig
                    {
                        Version = config.Version, G = config.G, NotModified = config.NotModified,
                        P = ByteString.CopyFrom(config.P), Random = ByteString.CopyFrom(config.Random)
                    };
                    break;
                }
                case P.RpcRequest.OperationOneofCase.ImportContacts:
                {
                    var input = request.ImportContacts.Contacts;
                    Count(input.Count);
                    Unique(input.Select(c => c.ClientId));
                    var commands = input.Select(c => new ImportContactCommand(c.ClientId, ContactService.NormalizeGid(c.Gid), c.FirstName, c.LastName, false)).ToArray();
                    Unique(commands.Select(c => c.Gid));
                    var imported = ContactService.Import(db, uid, input.Count, commands);
                    response.ImportContacts = new();
                    response.ImportContacts.Imported.Add(imported.Imported.Select(c => new P.ImportedContact { User = Reference(c.UserId), ClientId = c.ClientId }));
                    response.ImportContacts.Users.Add(imported.Users.Select(User));
                    break;
                }
                case P.RpcRequest.OperationOneofCase.CreateGroup:
                {
                    var input = request.CreateGroup;
                    Count(input.Users.Count);
                    var ids = input.Users.Select(SelectUser).Select(u => u.Id).ToArray();
                    Unique(ids);
                    var (conversation, service) = ConversationMessageService.CreateGroup(db, uid, input.Title, ids);
                    response.CreateGroup = new() { Group = Group(conversation), Updates = Updates([NewMessage(service)], [conversation]) };
                    break;
                }
                case P.RpcRequest.OperationOneofCase.GetPeerSettings:
                {
                    var details = ConversationQueryService.PeerSettings(db, uid, PeerId(request.GetPeerSettings.Peer));
                    response.GetPeerSettings = new() { Settings = new() };
                    response.GetPeerSettings.Users.Add(details.Users.Select(User));
                    response.GetPeerSettings.Groups.Add(details.Chats.Select(Group));
                    break;
                }
                case P.RpcRequest.OperationOneofCase.GetFullUser:
                {
                    var selected = SelectUser(request.GetFullUser.User);
                    var user = UserQueryService.Find(db, IdentityService.UserId(db, selected.Id), uid);
                    response.GetFullUser = new()
                    {
                        User = Reference(user.Id), Settings = new(), NotifySettings = new(), About = "", CommonGroupsCount = 0,
                        PhoneCallsAvailable = AccountPrivacyService.Allowed(db, user.Id, uid, P.PrivacyKey.PhoneCall),
                        VideoCallsAvailable = AccountPrivacyService.Allowed(db, user.Id, uid, P.PrivacyKey.PhoneCall),
                        PhoneCallsPrivate = !AccountPrivacyService.Allowed(db, user.Id, uid, P.PrivacyKey.PhoneP2P)
                    };
                    response.GetFullUser.Users.Add(User(user));
                    break;
                }
                case P.RpcRequest.OperationOneofCase.GetFullGroup:
                {
                    var conversation = FindGroup(request.GetFullGroup.GroupId);
                    response.GetFullGroup = new() { Group = Group(conversation), ParticipantsVersion = 1, NotifySettings = new() };
                    response.GetFullGroup.Participants.Add(conversation.Members.Select(id => new P.Participant
                    {
                        User = Reference(id), Inviter = Reference(conversation.Members[0]), JoinedAtMs = conversation.CreatedAt.ToUnixTimeMilliseconds()
                    }));
                    response.GetFullGroup.Users.Add(Related(true));
                    response.GetFullGroup.Groups.Add(Group(conversation));
                    break;
                }
                case P.RpcRequest.OperationOneofCase.GetGroups:
                {
                    var input = request.GetGroups.GroupIds;
                    Count(input.Count);
                    Unique(input);
                    var groups = input.Select(FindGroup).ToArray();
                    response.GetGroups = new();
                    response.GetGroups.Groups.Add(ConversationQueryService.Chats(db, uid, groups.Select(c => IdentityService.ChatId(db, c)).ToArray()).Select(Group));
                    break;
                }
                case P.RpcRequest.OperationOneofCase.GetNotifySettings:
                {
                    var target = request.GetNotifySettings.Target;
                    if (target is null) throw Invalid();
                    switch (target.TargetCase)
                    {
                        case P.NotifyTarget.TargetOneofCase.Peer:
                            ConversationQueryService.PeerSettings(db, uid, PeerId(target.Peer));
                            break;
                        case P.NotifyTarget.TargetOneofCase.AllUsers:
                        case P.NotifyTarget.TargetOneofCase.AllGroups:
                        case P.NotifyTarget.TargetOneofCase.AllBroadcasts:
                            break;
                        case P.NotifyTarget.TargetOneofCase.None:
                            throw Invalid();
                        default:
                            throw new ApiException(501, "BOTGRAM_NOTIFY_TARGET_UNSUPPORTED");
                    }
                    response.GetNotifySettings = new();
                    break;
                }
                default:
                    throw new ApiException(501, "BOTGRAM_RPC_UNSUPPORTED:" + request.OperationCase);
            }
            return response;
        }
    }
}
