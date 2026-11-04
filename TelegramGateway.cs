using System.Text.Json;
using System.Text.Json.Nodes;

namespace Botgram.Backend;

/// <summary>
/// Server-side compatibility for the Android app's TL models. This is TL object RPC over
/// authenticated HTTPS, not an implementation of Telegram's MTProto wire encryption.
/// UI and message controllers receive their original concrete TL response types.
/// </summary>
public static class TelegramGateway
{
    private static int Now => (int)DateTimeOffset.UtcNow.ToUnixTimeSeconds();
    private static JsonObject T(string name, params (string Key, object? Value)[] fields)
    {
        string className = name == "Vector" ? "org.telegram.tgnet.Vector" :
            name.StartsWith("TL_phone$", StringComparison.Ordinal) ? "org.telegram.tgnet.tl." + name :
            name is "TL_updateMessageID" or "TL_updateNewMessage" or "TL_updateReadHistoryInbox" or "TL_updateReadHistoryOutbox" or "TL_updateUserPhone"
                or "TL_updateUserStatus" or "TL_updatePhoneCall" or "TL_updatePhoneCallSignalingData"
                ? "org.telegram.tgnet.tl.TL_update$" + name : "org.telegram.tgnet.TLRPC$" + name;
        var result = new JsonObject { ["$type"] = className };
        foreach (var (key, value) in fields) result[key] = JsonSerializer.SerializeToNode(value, Store.Json);
        return result;
    }
    private static JsonObject Bytes(byte[]? data) => new() { ["$bytes"] = Convert.ToBase64String(data ?? []) };
    private static long N(JsonNode? node, string name, long fallback = 0) => node?[name]?.GetValue<long>() ?? fallback;
    private static string S(JsonNode? node, string name) => node?[name]?.GetValue<string>() ?? "";
    private static int I(long number) => checked((int)number);
    private static long UserId(Database db, string id) => IdentityService.UserId(db, id);
    private static long ChatId(Database db, Conversation c) => IdentityService.ChatId(db, c);
    private static long PeerId(JsonNode? peer, Database db, string uid)
    {
        if (S(peer, "$type").EndsWith("Self", StringComparison.Ordinal)) return UserId(db, uid);
        if (N(peer, "user_id") != 0) return N(peer, "user_id");
        return -N(peer, "chat_id");
    }
    private static JsonObject Peer(long id) => id > 0 ? T("TL_peerUser", ("user_id", id)) : T("TL_peerChat", ("chat_id", -id));
    private static long DialogId(Database db, Conversation c, string uid) => IdentityService.DialogId(db, c, uid);
    private static Conversation FindConversation(Database db, long peer, string uid, bool create = false)
        => ConversationMessageService.FindByPeer(db, peer, uid, create);
    // Presence is derived from the last authenticated request seen for that account.
    private static JsonObject UserStatus(UserProfile user)
    {
        if (user.IsSelf) return T("TL_userStatusOnline", ("expires", Now + 60));
        if (user.LastSeen is not { } seen) return T("TL_userStatusEmpty");
        if (DateTimeOffset.UtcNow - seen <= PresenceService.OnlineWindow) return T("TL_userStatusOnline", ("expires", Now + 60));
        return T("TL_userStatusOffline", ("was_online", (int)seen.ToUnixTimeSeconds()));
    }
    public static JsonObject User(UserProfile user) => T("TL_user",
        ("id", user.TelegramId), ("access_hash", 1L), ("first_name", user.FirstName), ("last_name", user.LastName),
        ("username", user.Username), ("phone", user.Gid), ("self", user.IsSelf),
        ("contact", user.IsContact), ("mutual_contact", user.IsMutual),
        ("premium", user.Premium),
        ("flags", 1 | 2 | 4 | 8 | 16 | 32 | 64 | (user.IsSelf ? 1024 : 0) | (user.IsContact ? 2048 : 0) | (user.IsMutual ? 4096 : 0)
            | (user.Premium ? 1 << 28 : 0)),
        ("photo", T("TL_userProfilePhotoEmpty")), ("status", UserStatus(user)));

    private static JsonObject Chat(Database db, Conversation c) => T("TL_chat", ("id", ChatId(db, c)), ("title", c.Title),
        ("photo", T("TL_chatPhotoEmpty")), ("participants_count", c.Members.Length), ("date", (int)c.CreatedAt.ToUnixTimeSeconds()), ("version", 1));
    private static JsonObject Message(Database db, Message m, string uid)
    {
        var view = ConversationQueryService.DescribeMessage(db, m, uid);
        if (m.ServiceTitle is not null)
            return T("TL_messageService", ("id", I(m.Id)), ("date", (int)m.CreatedAt.ToUnixTimeSeconds()),
                ("out", view.Out), ("flags", 256 | (view.Out ? 2 : 0)),
                ("from_id", Peer(view.SenderId)), ("peer_id", Peer(view.PeerId)),
                ("action", T("TL_messageActionChatCreate", ("title", m.ServiceTitle), ("users", view.ServiceMembers))));
        return T("TL_message", ("id", I(m.Id)), ("date", (int)m.CreatedAt.ToUnixTimeSeconds()),
            ("message", m.Text), ("out", view.Out), ("unread", view.Unread),
            ("flags", 256 | 512 | (view.Out ? 2 : 0) | (m.TelegramEntities is not null ? 128 : 0) | (m.ReplyToMessageId is not null ? 8 : 0) | (m.Silent ? 8192 : 0)),
            ("silent", m.Silent), ("entities", m.TelegramEntities),
            ("reply_to", m.ReplyToMessageId is null ? null : T("TL_messageReplyHeader", ("flags", 16), ("reply_to_msg_id", I(m.ReplyToMessageId.Value)))),
            ("from_id", Peer(view.SenderId)), ("peer_id", Peer(view.PeerId)),
            ("dialog_id", view.PeerId), ("media", T("TL_messageMediaEmpty")));
    }
    private static int Pts(Database db, string uid) => SyncService.Pts(db, uid);
    private static JsonObject State(Database db, string uid, int? pts = null) => T("TL_updates_state", ("pts", pts ?? Pts(db, uid)), ("qts", 0), ("date", Now), ("seq", 0), ("unread_count", 0));
    private static JsonObject Dialog(DialogView view) => T("TL_dialog", ("id", view.PeerId), ("peer", Peer(view.PeerId)),
        ("top_message", I(view.LastMessage?.Id ?? 0)),
        ("read_inbox_max_id", I(view.Inbox)), ("read_outbox_max_id", I(view.Outbox)),
        ("unread_count", view.UnreadCount), ("notify_settings", T("TL_peerNotifySettings")));
    private static JsonObject Updates(Database db, string uid, IEnumerable<JsonObject> updates, IEnumerable<Conversation>? conversations = null)
        => T("TL_updates", ("updates", updates.ToArray()), ("date", Now), ("seq", 0),
            ("users", UserQueryService.Related(db, uid, false).Select(User).ToArray()),
            ("chats", (conversations ?? ConversationQueryService.Owned(db, uid)).Where(c => c.Kind == "group").Select(c => Chat(db, c)).ToArray()));
    public static JsonObject CurrentUser(Database db, string uid) => User(UserQueryService.Current(db, uid));

    private static MessageEntity[]? Entities(JsonObject request, string text)
    {
        if (request["entities"] is not JsonArray input || input.Count == 0) return null;
        MessageEntityService.ValidateCount(input.Count);
        var result = new List<MessageEntity>();
        foreach (var entity in input)
        {
            var kind = MessageEntityJsonConverter.ParseKind(S(entity, "$type"));
            MessageEntityService.ValidateKind(kind);
            var value = new MessageEntity(kind, I(N(entity, "offset")), I(N(entity, "length")));
            MessageEntityService.ValidateBounds(text, value.Offset, value.Length);
            if (kind == MessageEntityKind.Pre) value = value with { Language = S(entity, "language") };
            if (kind == MessageEntityKind.TextUrl) value = value with { Url = S(entity, "url") };
            result.Add(MessageEntityService.Validate(text, value));
        }
        return result.ToArray();
    }

    // ---------- phone.* (1:1 calls) ----------
    // Telegram's 2048-bit DH prime; clients derive the voice-call auth key from p/g and the exchanged g_a/g_b.
    private static readonly byte[] DhPrime = Convert.FromHexString(
        "c71caeb9c6b1c9048e6c522f70f13f73980d40238e3e21c14934d037563d930f" +
        "48198a0aa7c14058229493d22530f4dbfa336f6e0ac925139543aed44cce7c3720" +
        "fd51f69458705ac68cd4fe6b6b13abdc9746512969328454f18faf8c595f64247" +
        "7fe96bb2a941d5bcd1d4ac8cc49880708fa9b378e3c4f3a9060bee67cf9a4a4a" +
        "695811051907e162753b56b0f6b410dba74d8a84b2a14b3144e0ef1284754fd17" +
        "ed950d5965b4b9dd46582db1178d169c6bc465b0d6ff9ca3928fef5b9ae4e418" +
        "fc15e83ebea0f87fa9ff5eed70050ded2849f47bf959d956850ce929851f0d8115" +
        "f635b105ee2e4e15d04b2454bf6f4fadf034b10403119cd8e3b92fcc5b");
    private static byte[] B(JsonNode? node) => S(node, "$bytes") is { Length: > 0 } text ? Convert.FromBase64String(text) : [];
    private static JsonObject DefaultProtocol() => T("TL_phone$TL_phoneCallProtocol",
        ("flags", 3), ("udp_p2p", true), ("udp_reflector", true), ("min_layer", 65), ("max_layer", 92),
        ("library_versions", new[] { "10.0.0" }));
    private static JsonObject? Protocol(JsonNode? node) => node is JsonObject value ? (JsonObject)value.DeepClone() : null;
    private static Call? FindTelegramCall(Database db, long id) =>
        id == 0 ? null : db.Calls.FirstOrDefault(call => call.TelegramId == id);
    private static Call RequireCall(Database db, JsonNode? peer, string uid)
    {
        var call = FindTelegramCall(db, N(peer, "id")) ?? throw new ApiException(400, "CALL_NOT_FOUND");
        if (call.AccessHash != N(peer, "access_hash") || (call.CallerId != uid && call.CalleeId != uid))
            throw new ApiException(400, "CALL_NOT_FOUND");
        return call;
    }
    private static void SaveCall(Database db, Call call)
    {
        int index = db.Calls.FindIndex(existing => existing.Id == call.Id);
        if (index < 0) db.Calls.Add(call); else db.Calls[index] = call;
    }
    private static JsonObject[] PhoneConnections()
    {
        var result = new List<JsonObject>();
        long id = 1;
        foreach (var relay in CallService.Relays)
            result.Add(T("TL_phoneConnectionWebrtc", ("flags", relay.Turn ? 1 : 2), ("id", id++),
                ("ip", relay.Host), ("ipv6", ""), ("port", relay.Port), ("turn", relay.Turn), ("stun", !relay.Turn),
                ("username", relay.Turn ? relay.Username : ""), ("password", relay.Turn ? relay.Password : "")));
        return result.ToArray();
    }
    private static JsonObject CallJson(Database db, Call call, string uid)
    {
        long admin = UserId(db, call.CallerId), participant = UserId(db, call.CalleeId);
        var protocol = call.Protocol ?? DefaultProtocol();
        int videoFlag = call.Video ? 64 : 0;
        if (call.State == "ringing" && call.CallerId == uid)
            return T("TL_phone$TL_phoneCallWaiting", ("flags", videoFlag | (call.ReceiveDate != 0 ? 1 : 0)),
                ("video", call.Video), ("id", call.TelegramId), ("access_hash", call.AccessHash), ("date", call.Date),
                ("admin_id", admin), ("participant_id", participant), ("protocol", protocol), ("receive_date", call.ReceiveDate));
        if (call.State == "ringing")
            return T("TL_phone$phoneCallRequested", ("flags", videoFlag), ("video", call.Video),
                ("id", call.TelegramId), ("access_hash", call.AccessHash), ("date", call.Date),
                ("admin_id", admin), ("participant_id", participant), ("g_a_hash", Bytes(call.GAHash)), ("protocol", protocol));
        if (call.State == "accepted")
            return T("TL_phone$TL_phoneCallAccepted", ("flags", videoFlag), ("video", call.Video),
                ("id", call.TelegramId), ("access_hash", call.AccessHash), ("date", call.Date),
                ("admin_id", admin), ("participant_id", participant), ("g_b", Bytes(call.GB)), ("protocol", protocol));
        if (call.State == "active")
            return T("TL_phone$TL_phoneCall", ("flags", videoFlag | (CallService.P2p ? 32 : 0)), ("video", call.Video),
                ("p2p_allowed", CallService.P2p), ("id", call.TelegramId), ("access_hash", call.AccessHash), ("date", call.Date),
                ("admin_id", admin), ("participant_id", participant), ("g_a_or_b", Bytes(call.GA)),
                ("key_fingerprint", call.KeyFingerprint), ("protocol", protocol),
                ("connections", (call.Connections ?? []).ToArray()), ("start_date", call.StartDate));
        return T("TL_phone$TL_phoneCallDiscarded", ("flags", videoFlag | 1 | 2), ("video", call.Video),
            ("id", call.TelegramId), ("reason", T("TL_phoneCallDiscardReason" + call.Reason)),
            ("duration", call.Duration));
    }
    private static JsonObject PhoneResult(Database db, Call call, string uid)
    {
        string other = call.CallerId == uid ? call.CalleeId : call.CallerId;
        return T("TL_phone$TL_phone_phoneCall", ("phone_call", CallJson(db, call, uid)),
            ("users", new[] { User(UserQueryService.Find(db, UserId(db, other), uid)) }));
    }
    private static string ReasonName(string type) =>
        type.EndsWith("Missed", StringComparison.Ordinal) ? "Missed" :
        type.EndsWith("Busy", StringComparison.Ordinal) ? "Busy" :
        type.EndsWith("Disconnect", StringComparison.Ordinal) ? "Disconnect" : "Hangup";
    private static JsonObject RequestCall(Database db, string uid, JsonObject request)
    {
        long peerId = PeerId(request["user_id"], db, uid);
        var callee = IdentityService.FindUser(db, peerId);
        if (callee.Id == uid) throw new ApiException(400, "CALL_PEER_INVALID");
        if (!AccountPrivacyService.Allowed(db, callee.Id, uid, Botgram.Protocol.V1.PrivacyKey.PhoneCall))
            throw new ApiException(403, "USER_PRIVACY_RESTRICTED");
        int randomId = I(N(request, "random_id"));
        var replay = db.Calls.FirstOrDefault(call => call.TelegramId != 0 && call.CallerId == uid && call.RandomId == randomId);
        if (replay is not null && CallService.InProgress(replay.State)) return PhoneResult(db, replay, uid);
        var conversation = ConversationMessageService.FindByPeer(db, peerId, uid, create: true);
        CallService.ValidateCallee(conversation, uid, callee.Id);
        if (db.Calls.Any(call => CallService.InProgress(call.State) &&
            (call.CallerId == uid || call.CalleeId == uid || call.CallerId == callee.Id || call.CalleeId == callee.Id)))
            throw new ApiException(409, "CALL_BUSY");
        if (db.NextCallId == 0) db.NextCallId = 1_000_000_000;
        // Kept within the IEEE-754 safe integer range so the TL-JSON field round-trips
        // unchanged through the app and generic JSON tooling.
        long accessHash = BitConverter.ToInt64(System.Security.Cryptography.RandomNumberGenerator.GetBytes(8)) & 0x1FFFFFFFFFFFFF;
        if (accessHash == 0) accessHash = 1;
        var created = new Call(Guid.NewGuid().ToString("N"), conversation.Id, uid, callee.Id, "ringing", DateTimeOffset.UtcNow,
            TelegramId: db.NextCallId++, AccessHash: accessHash, RandomId: randomId,
            Video: request["video"]?.GetValue<bool>() ?? false, Protocol: Protocol(request["protocol"]) ?? DefaultProtocol(),
            GAHash: B(request["g_a_hash"]), Date: Now);
        SaveCall(db, created);
        db.Emit([created.CallerId, created.CalleeId], "call.updated", created);
        return PhoneResult(db, created, uid);
    }
    private static JsonObject ExecutePhone(Database db, string uid, string method, JsonObject request)
    {
        switch (method)
        {
            case "requestCall": return RequestCall(db, uid, request);
            case "receivedCall":
            {
                var call = RequireCall(db, request["peer"], uid);
                if (call.CalleeId != uid) throw new ApiException(400, "CALL_NOT_FOUND");
                if (call.State == "ringing" && call.ReceiveDate == 0)
                {
                    var updated = call with { ReceiveDate = Now };
                    SaveCall(db, updated);
                    db.Emit([call.CallerId], "call.updated", updated);
                }
                return T("TL_boolTrue");
            }
            case "acceptCall":
            {
                var call = RequireCall(db, request["peer"], uid);
                if (call.CalleeId != uid) throw new ApiException(400, "CALL_NOT_FOUND");
                if (call.State != "ringing") throw new ApiException(400, "CALL_ALREADY_ACCEPTED");
                var updated = call with
                {
                    State = "accepted", GB = B(request["g_b"]), UpdatedAt = DateTimeOffset.UtcNow,
                    Protocol = Protocol(request["protocol"]) ?? call.Protocol
                };
                SaveCall(db, updated);
                db.Emit([call.CallerId], "call.updated", updated);
                return PhoneResult(db, updated, uid);
            }
            case "confirmCall":
            {
                var call = RequireCall(db, request["peer"], uid);
                if (call.CallerId != uid) throw new ApiException(400, "CALL_NOT_FOUND");
                if (call.State != "accepted") throw new ApiException(400, "CALL_NOT_ACCEPTED");
                var updated = call with
                {
                    State = "active", GA = B(request["g_a"]), KeyFingerprint = N(request, "key_fingerprint"),
                    StartDate = Now, UpdatedAt = DateTimeOffset.UtcNow, Connections = PhoneConnections(),
                    Protocol = Protocol(request["protocol"]) ?? call.Protocol
                };
                SaveCall(db, updated);
                db.Emit([call.CalleeId], "call.updated", updated);
                return PhoneResult(db, updated, uid);
            }
            case "discardCall":
            {
                var call = RequireCall(db, request["peer"], uid);
                string reason = ReasonName(S(request["reason"], "$type"));
                int duration = I(N(request, "duration"));
                if (duration <= 0 && call.StartDate != 0) duration = Math.Max(0, Now - call.StartDate);
                var updated = call with { State = "ended", Reason = reason, Duration = duration, UpdatedAt = DateTimeOffset.UtcNow };
                SaveCall(db, updated);
                db.Emit([call.CallerId, call.CalleeId], "call.updated", updated);
                return Updates(db, uid, new[] { T("TL_updatePhoneCall", ("phone_call", CallJson(db, updated, uid))) });
            }
            case "sendSignalingData":
            {
                var call = RequireCall(db, request["peer"], uid);
                if (call.State != "active") throw new ApiException(400, "CALL_NOT_ACTIVE");
                string other = call.CallerId == uid ? call.CalleeId : call.CallerId;
                db.Emit([other], "call.tl_signal", new CallSignal(call.Id, uid, "tl", Convert.ToBase64String(B(request["data"]))));
                return T("TL_boolTrue");
            }
            case "getCallConfig": return T("TL_dataJSON", ("data", "{}"));
            case "saveCallDebug": return T("TL_boolTrue");
            default: throw new ApiException(501, "BOTGRAM_RPC_UNSUPPORTED:" + method);
        }
    }

    public static JsonObject Execute(Database db, string uid, JsonObject request)
    {
        using var diagnostic = RpcDiagnosticStage.Start("json_mapper_ms");
        string method = S(request, "$type").Split('$').Last();
        // Every authenticated RPC carries presence; the outer write transaction persists it.
        PresenceService.Touch(db, uid);
        var owned = ConversationQueryService.Owned(db, uid);
        var users = UserQueryService.Related(db, uid, true).Select(User).ToArray();
        var chats = owned.Where(c => c.Kind == "group").Select(c => Chat(db, c)).ToArray();
        switch (method)
        {
            case "TL_updates_getState": return State(db, uid);
            case "TL_updates_getDifference":
            {
                var difference = SyncService.Difference(db, uid, I(N(request, "pts")));
                int after = difference.PreviousCount;
                var page = difference.Events;
                if (difference.Kind == DifferenceKind.TooLong) return T("TL_updates_differenceTooLong", ("pts", difference.Pts));
                if (difference.Kind == DifferenceKind.Empty) return T("TL_updates_differenceEmpty", ("date", Now), ("seq", 0));
                var messages = new List<JsonObject>(); var updates = new List<JsonObject>();
                foreach (var e in page)
                {
                    if (e.Payload is Message m)
                    {
                        messages.Add(Message(db, m, uid));
                    }
                    else if (e.Payload is GidChanged changed)
                    {
                        var changedUser = UserQueryService.Find(db, UserId(db, changed.UserId), uid);
                        updates.Add(T("TL_updateUserPhone", ("user_id", changedUser.TelegramId), ("phone", changedUser.Gid)));
                    }
                    else if (e.Payload is PresenceChanged presence)
                    {
                        var changedUser = UserQueryService.Find(db, UserId(db, presence.UserId), uid);
                        updates.Add(T("TL_updateUserStatus", ("user_id", changedUser.TelegramId), ("status", UserStatus(changedUser))));
                    }
                    else if (e.Payload is Receipt r)
                    {
                        var c = Auth.Conversation(db, r.ConversationId, uid);
                        updates.Add(T(r.UserId == uid ? "TL_updateReadHistoryInbox" : "TL_updateReadHistoryOutbox",
                            ("peer", Peer(DialogId(db, c, uid))), ("max_id", I(r.MessageId)), ("pts", after + messages.Count + updates.Count + 2), ("pts_count", 1)));
                    }
                    else if (e.Payload is Call call && call.TelegramId != 0 && (call.CallerId == uid || call.CalleeId == uid))
                    {
                        updates.Add(T("TL_updatePhoneCall", ("phone_call", CallJson(db, call, uid))));
                    }
                    else if (e.Payload is CallSignal signal && signal.SenderId != uid)
                    {
                        var tlCall = db.Calls.FirstOrDefault(item => item.Id == signal.CallId && item.TelegramId != 0);
                        if (tlCall is not null)
                            updates.Add(T("TL_updatePhoneCallSignalingData",
                                ("phone_call_id", tlCall.TelegramId), ("data", Bytes(Convert.FromBase64String(signal.Data)))));
                    }
                }
                bool more = difference.Kind == DifferenceKind.Slice;
                return T(more ? "TL_updates_differenceSlice" : "TL_updates_difference", ("new_messages", messages),
                    ("other_updates", updates), ("users", users), ("chats", chats),
                    (more ? "intermediate_state" : "state", State(db, uid, after + page.Length + 1)));
            }
            case "TL_messages_getDialogs":
            case "TL_messages_getPinnedDialogs":
            case "TL_messages_getPeerDialogs":
            {
                var selection = method == "TL_messages_getPinnedDialogs" ? DialogSelection.Pinned :
                    method == "TL_messages_getPeerDialogs" ? DialogSelection.Peers : DialogSelection.All;
                long folder = selection == DialogSelection.Pinned ? 0 : N(request, "folder_id");
                var peers = selection == DialogSelection.Peers
                    ? (request["peers"] as JsonArray ?? []).Select(p => PeerId(p?["peer"], db, uid)).ToArray() : null;
                long offset = selection == DialogSelection.All ? N(request, "offset_id") : 0;
                long offsetPeer = selection == DialogSelection.All ? PeerId(request["offset_peer"], db, uid) : 0;
                int limit = selection == DialogSelection.All ? I(N(request, "limit", 100)) : 100;
                var page = ConversationQueryService.Dialogs(db, uid, new DialogQuery(selection, folder == 0 ? 0 : 1, offset, offsetPeer, limit, peers));
                string responseType = method != "TL_messages_getDialogs" ? "TL_messages_peerDialogs" : page.IsSlice ? "TL_messages_dialogsSlice" : "TL_messages_dialogs";
                return T(responseType, ("count", page.TotalCount),
                    ("dialogs", page.Dialogs.Select(Dialog).ToArray()), ("users", users), ("chats", chats), ("state", State(db, uid)),
                    ("messages", page.Dialogs.Select(d => d.LastMessage).Where(m => m is not null).Select(m => Message(db, m!, uid)).ToArray()));
            }
            case "TL_messages_getHistory":
            {
                var c = FindConversation(db, PeerId(request["peer"], db, uid), uid, true);
                var values = ConversationMessageService.History(db, c, new(N(request, "offset_id"), N(request, "offset_date"),
                    I(N(request, "add_offset")), N(request, "min_id"), N(request, "max_id"), I(N(request, "limit", 50))));
                var participants = UserQueryService.Participants(db, c, uid).Select(User).ToArray();
                return T("TL_messages_messages", ("messages", values.Select(m => Message(db, m, uid)).ToArray()), ("users", participants), ("chats", chats));
            }
            case "TL_messages_getMessages":
            {
                var ids = (request["id"] as JsonArray ?? []).Select(p => N(p, "id")).ToArray();
                return T("TL_messages_messages", ("users", users), ("chats", chats),
                    ("messages", ConversationQueryService.Messages(db, uid, ids).Select(m => Message(db, m, uid)).ToArray()));
            }
            case "TL_messages_sendMessage":
            {
                if (N(request, "schedule_date") != 0 || request["reply_markup"] is not null)
                    throw new ApiException(501, "BOTGRAM_MESSAGE_FEATURE_UNSUPPORTED");
                // The Android sender includes send_as even for an ordinary message
                // from the current user. Alternate/impersonated senders remain forbidden.
                if (request["send_as"] is JsonNode sender && PeerId(sender, db, uid) != UserId(db, uid))
                    throw new ApiException(403, "SEND_AS_PEER_INVALID");
                var c = FindConversation(db, PeerId(request["peer"], db, uid), uid, true);
                string text = S(request, "message"), key = "tl:" + N(request, "random_id");
                if (string.IsNullOrWhiteSpace(text) || text.Length > 16000) throw new ApiException(400, "MESSAGE_EMPTY");
                MessageEntity[]? entities = Entities(request, text);
                long? replyId = null;
                if (request["reply_to"] is JsonObject reply)
                {
                    if (!S(reply, "$type").EndsWith("$TL_inputReplyToMessage", StringComparison.Ordinal) || N(reply, "top_msg_id") != 0 ||
                        S(reply, "quote_text").Length != 0 || (reply["reply_to_peer_id"] is not null && PeerId(reply["reply_to_peer_id"], db, uid) != DialogId(db, c, uid)))
                        throw new ApiException(501, "BOTGRAM_REPLY_MODE_UNSUPPORTED");
                    replyId = N(reply, "reply_to_msg_id");
                }
                bool silent = request["silent"]?.GetValue<bool>() ?? false;
                var message = ConversationMessageService.Send(db, c, uid, new(key, text, entities, replyId, silent), MessageWriteContract.Telegram);
                return Updates(db, uid, [T("TL_updateMessageID", ("id", I(message.Id)), ("random_id", N(request, "random_id"))),
                    T("TL_updateNewMessage", ("message", Message(db, message, uid)), ("pts", Pts(db, uid)), ("pts_count", 1))]);
            }
            case "TL_messages_readHistory":
            {
                var c = FindConversation(db, PeerId(request["peer"], db, uid), uid);
                var result = ConversationMessageService.ReadThrough(db, c, uid, N(request, "max_id"));
                return T("TL_messages_affectedMessages", ("pts", Pts(db, uid)), ("pts_count", result.Changed ? 1 : 0));
            }
            case "TL_contacts_search":
            {
                var found = ContactService.Search(db, S(request, "q"), uid);
                return T("TL_contacts_found", ("users", found.Select(User).ToArray()),
                    ("results", found.Select(u => Peer(u.TelegramId)).ToArray()));
            }
            case "TL_contacts_resolveUsername":
            {
                var user = ContactService.ResolveUsername(db, S(request, "username"), uid);
                return T("TL_contacts_resolvedPeer", ("peer", Peer(user.TelegramId)), ("users", new[] { User(user) }));
            }
            case "TL_users_getUsers":
            {
                var ids = (request["id"] as JsonArray ?? []).Select(p => PeerId(p, db, uid)).ToArray();
                return T("Vector", ("objects", UserQueryService.ByIds(db, ids, uid).Select(User).ToArray()));
            }
            case "TL_contacts_getContacts":
            {
                var contacts = ContactService.List(db, uid);
                return T("TL_contacts_contacts", ("saved_count", contacts.Length),
                    ("contacts", contacts.Select(c => T("TL_contact", ("user_id", c.User.TelegramId),
                        ("mutual", c.Mutual))).ToArray()),
                    ("users", contacts.Select(c => User(c.User)).ToArray()));
            }
            case "TL_contacts_resolvePhone":
            {
                var user = ContactService.ResolveGid(db, S(request, "phone"), uid);
                return T("TL_contacts_resolvedPeer", ("peer", Peer(user.TelegramId)), ("users", new[] { User(user) }));
            }
            case "TL_contacts_importContacts":
            {
                if (request["contacts"] is not JsonArray input || input.Count is 0 or > 100)
                    throw new ApiException(400, "CONTACTS_TOO_MUCH");
                var commands = input.Select(item =>
                {
                    if (item is not JsonObject || S(item, "$type") != "org.telegram.tgnet.TLRPC$TL_inputPhoneContact")
                        throw new ApiException(400, "CONTACT_INVALID");
                    return new ImportContactCommand(N(item, "client_id"), S(item, "phone"), S(item, "first_name"),
                        S(item, "last_name"), item["note"] is not null || N(item, "flags") != 0);
                });
                var result = ContactService.Import(db, uid, input.Count, commands);
                return T("TL_contacts_importedContacts", ("imported", result.Imported.Select(c =>
                        T("TL_importedContact", ("user_id", UserId(db, c.UserId)), ("client_id", c.ClientId))).ToArray()),
                    ("retry_contacts", Array.Empty<long>()), ("popular_invites", Array.Empty<object>()),
                    ("users", result.Users.Select(User).ToArray()));
            }
            case "TL_contacts_addContact":
            {
                ContactService.Add(db, uid, PeerId(request["id"], db, uid), S(request, "first_name"), S(request, "last_name"),
                    request["note"] is JsonObject || (N(request, "flags") & 2) != 0);
                // The client only persists the new contact from response users, so include contacts here.
                return T("TL_updates", ("updates", Array.Empty<object>()), ("date", Now), ("seq", 0),
                    ("users", UserQueryService.Related(db, uid, true).Select(User).ToArray()),
                    ("chats", Array.Empty<object>()));
            }
            case "TL_messages_createChat":
            {
                string title = S(request, "title");
                var ids = (request["users"] as JsonArray ?? []).Select(p => UserQueryService.Find(db, PeerId(p, db, uid), uid).Id).Append(uid).Distinct().Order().ToArray();
                var (c, service) = ConversationMessageService.CreateGroup(db, uid, title, ids);
                // The real createChat response in this checkout wraps Updates in invitedUsers.
                return T("TL_messages_invitedUsers", ("updates", Updates(db, uid,
                    [T("TL_updateNewMessage", ("message", Message(db, service, uid)), ("pts", Pts(db, uid)), ("pts_count", 1))], [c])));
            }
            case "TL_messages_getPeerSettings":
            {
                var details = ConversationQueryService.PeerSettings(db, uid, PeerId(request["peer"], db, uid));
                return T("TL_messages_peerSettings", ("settings", T("TL_peerSettings")),
                    ("users", details.Users.Select(User).ToArray()),
                    ("chats", details.Chats.Select(c => Chat(db, c)).ToArray()));
            }
            case "TL_users_getFullUser":
            {
                var user = UserQueryService.Find(db, PeerId(request["id"], db, uid), uid);
                return T("TL_users_userFull", ("users", new[] { User(user) }),
                    ("full_user", T("TL_userFull", ("id", user.TelegramId), ("flags", 16 | 8192), ("settings", T("TL_peerSettings")),
                        ("notify_settings", T("TL_peerNotifySettings")), ("about", ""), ("common_chats_count", 0),
                        ("phone_calls_available", true), ("video_calls_available", true), ("phone_calls_private", false))));
            }
            case "TL_messages_getFullChat":
            {
                var c = ConversationMessageService.FindByPeer(db, -N(request, "chat_id"), uid);
                return T("TL_messages_chatFull", ("users", users), ("chats", new[] { Chat(db, c) }),
                    ("full_chat", T("TL_chatFull", ("id", ChatId(db, c)), ("about", ""),
                        ("participants", T("TL_chatParticipants", ("chat_id", ChatId(db, c)), ("version", 1),
                            ("participants", c.Members.Select(id => T("TL_chatParticipant", ("user_id", UserId(db, id)), ("inviter_id", UserId(db, c.Members[0])), ("date", (int)c.CreatedAt.ToUnixTimeSeconds()))).ToArray()))),
                        ("chat_photo", T("TL_photoEmpty", ("id", 0L))), ("notify_settings", T("TL_peerNotifySettings")))));
            }
            case "TL_messages_getChats":
            {
                var ids = (request["id"] as JsonArray ?? []).Select(p => p!.GetValue<long>()).ToArray();
                return T("TL_messages_chats", ("chats", ConversationQueryService.Chats(db, uid, ids).Select(c => Chat(db, c)).ToArray()));
            }
            case "TL_account_getNotifySettings":
            case "getNotifySettings": return T("TL_peerNotifySettings");
            case "TL_messages_getDhConfig":
                return T("TL_messages_dhConfig", ("g", 3), ("p", Bytes(DhPrime)), ("version", 1),
                    ("random", Bytes(System.Security.Cryptography.RandomNumberGenerator.GetBytes(256))));
            case "requestCall":
            case "receivedCall":
            case "acceptCall":
            case "confirmCall":
            case "discardCall":
            case "sendSignalingData":
            case "getCallConfig":
            case "saveCallDebug":
                return ExecutePhone(db, uid, method, request);
            // No sticker catalogue exists on this server. Returning valid empty results keeps the
            // client's sticker bootstrap from retrying unsupported methods and exhausting the rate limit.
            case "TL_messages_getStickerSet":
                return T("TL_messages_stickerSet",
                    ("set", T("TL_stickerSet", ("id", 0L), ("access_hash", 0L), ("title", ""), ("short_name", ""), ("count", 0), ("hash", 0))),
                    ("packs", Array.Empty<object>()), ("keywords", Array.Empty<object>()), ("documents", Array.Empty<object>()));
            case "TL_messages_getFeaturedStickers":
            case "TL_messages_getFeaturedEmojiStickers":
                return T("TL_messages_featuredStickersNotModified", ("count", 0));
            case "TL_messages_getArchivedStickers":
                return T("TL_messages_archivedStickers", ("count", 0), ("sets", Array.Empty<object>()));
            case "TL_messages_getEmojiStickers":
            case "TL_messages_getAllStickers":
            case "TL_messages_getMaskStickers":
                return T("TL_messages_allStickers", ("hash2", 0L), ("sets", Array.Empty<object>()));
            default: throw new ApiException(501, "BOTGRAM_RPC_UNSUPPORTED:" + method);
        }
    }
}
