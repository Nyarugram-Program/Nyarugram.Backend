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
            name is "TL_updateMessageID" or "TL_updateNewMessage" or "TL_updateReadHistoryInbox" or "TL_updateReadHistoryOutbox" or "TL_updateUserPhone"
                ? "org.telegram.tgnet.tl.TL_update$" + name : "org.telegram.tgnet.TLRPC$" + name;
        var result = new JsonObject { ["$type"] = className };
        foreach (var (key, value) in fields) result[key] = JsonSerializer.SerializeToNode(value, Store.Json);
        return result;
    }
    private static long N(JsonNode? node, string name, long fallback = 0) => node?[name]?.GetValue<long>() ?? fallback;
    private static string S(JsonNode? node, string name) => node?[name]?.GetValue<string>() ?? "";
    private static int I(long number) => checked((int)number);
    private static long UserId(Database db, string id) => db.TelegramId("user:" + id);
    private static long ChatId(Database db, Conversation c) => db.TelegramId("chat:" + c.Id);
    private static User FindUser(Database db, long id) => db.Users.FirstOrDefault(u => UserId(db, u.Id) == id)
        ?? throw new ApiException(404, "USER_ID_INVALID");
    private static long PeerId(JsonNode? peer, Database db, string uid)
    {
        if (S(peer, "$type").EndsWith("Self", StringComparison.Ordinal)) return UserId(db, uid);
        if (N(peer, "user_id") != 0) return N(peer, "user_id");
        return -N(peer, "chat_id");
    }
    private static JsonObject Peer(long id) => id > 0 ? T("TL_peerUser", ("user_id", id)) : T("TL_peerChat", ("chat_id", -id));
    private static long DialogId(Database db, Conversation c, string uid) => c.Kind == "group" ? -ChatId(db, c) :
        c.Kind == "saved" ? UserId(db, uid) : UserId(db, c.Members.First(m => m != uid));
    private static Conversation FindConversation(Database db, long peer, string uid, bool create = false)
    {
        var result = db.Conversations.FirstOrDefault(c => c.Members.Contains(uid) && DialogId(db, c, uid) == peer);
        if (result is not null) return result;
        if (!create || peer <= 0) throw new ApiException(404, "PEER_ID_INVALID");
        var other = FindUser(db, peer);
        var members = new[] { uid, other.Id }.Distinct().Order().ToArray();
        result = new(Guid.NewGuid().ToString("N"), other.Id == uid ? "saved" : "direct", "", members, DateTimeOffset.UtcNow);
        db.Conversations.Add(result); db.Emit(members, "conversation.created", result);
        return result;
    }
    private static JsonObject User(Database db, User user, string uid) => T("TL_user",
        ("id", UserId(db, user.Id)), ("access_hash", 1L), ("first_name", user.DisplayName), ("last_name", ""),
        ("username", user.Username), ("phone", user.Gid), ("self", user.Id == uid),
        ("flags", 1 | 2 | 8 | 16 | 32 | 64 | (user.Id == uid ? 1024 : 0)),
        ("photo", T("TL_userProfilePhotoEmpty")), ("status", T("TL_userStatusEmpty")));
    private static JsonObject Chat(Database db, Conversation c) => T("TL_chat", ("id", ChatId(db, c)), ("title", c.Title),
        ("photo", T("TL_chatPhotoEmpty")), ("participants_count", c.Members.Length), ("date", (int)c.CreatedAt.ToUnixTimeSeconds()), ("version", 1));
    private static JsonObject Message(Database db, Message m, string uid)
    {
        var conversation = Auth.Conversation(db, m.ConversationId, uid);
        var read = db.Receipts.FirstOrDefault(r => r.ConversationId == conversation.Id && r.UserId == uid)?.MessageId ?? 0;
        if (m.ServiceTitle is not null)
            return T("TL_messageService", ("id", I(m.Id)), ("date", (int)m.CreatedAt.ToUnixTimeSeconds()),
                ("out", m.SenderId == uid), ("flags", 256 | (m.SenderId == uid ? 2 : 0)),
                ("from_id", Peer(UserId(db, m.SenderId))), ("peer_id", Peer(DialogId(db, conversation, uid))),
                ("action", T("TL_messageActionChatCreate", ("title", m.ServiceTitle), ("users", (m.ServiceMembers ?? []).Select(id => UserId(db, id)).ToArray()))));
        return T("TL_message", ("id", I(m.Id)), ("date", (int)m.CreatedAt.ToUnixTimeSeconds()),
            ("message", m.Text), ("out", m.SenderId == uid), ("unread", m.SenderId != uid && m.Id > read),
            ("flags", 256 | 512 | (m.SenderId == uid ? 2 : 0) | (m.TelegramEntities is not null ? 128 : 0) | (m.ReplyToMessageId is not null ? 8 : 0) | (m.Silent ? 8192 : 0)),
            ("silent", m.Silent), ("entities", m.TelegramEntities),
            ("reply_to", m.ReplyToMessageId is null ? null : T("TL_messageReplyHeader", ("flags", 16), ("reply_to_msg_id", I(m.ReplyToMessageId.Value)))),
            ("from_id", Peer(UserId(db, m.SenderId))), ("peer_id", Peer(DialogId(db, conversation, uid))),
            ("dialog_id", DialogId(db, conversation, uid)), ("media", T("TL_messageMediaEmpty")));
    }
    private static IEnumerable<ChatEvent> Stream(Database db, string uid) => db.Events.Where(e => e.Recipients.Contains(uid) && e.Type is "message.created" or "receipt.updated" or "user.gid_changed");
    // Telegram treats pts=0 as uninitialized. Start at 1 so the first received message is not skipped.
    private static int Pts(Database db, string uid) => 1 + Stream(db, uid).Count();
    private static JsonObject State(Database db, string uid, int? pts = null) => T("TL_updates_state", ("pts", pts ?? Pts(db, uid)), ("qts", 0), ("date", Now), ("seq", 0), ("unread_count", 0));
    private static JsonObject Dialog(Database db, Conversation c, string uid)
    {
        long inbox = db.Receipts.FirstOrDefault(r => r.ConversationId == c.Id && r.UserId == uid)?.MessageId ?? 0;
        long outbox = db.Receipts.Where(r => r.ConversationId == c.Id && r.UserId != uid).Select(r => r.MessageId).DefaultIfEmpty(0).Max();
        return T("TL_dialog", ("id", DialogId(db, c, uid)), ("peer", Peer(DialogId(db, c, uid))),
            ("top_message", I(db.Messages.LastOrDefault(m => m.ConversationId == c.Id)?.Id ?? 0)),
            ("read_inbox_max_id", I(inbox)), ("read_outbox_max_id", I(outbox)),
            ("unread_count", db.Messages.Count(m => m.ConversationId == c.Id && m.SenderId != uid && m.Id > inbox)),
            ("notify_settings", T("TL_peerNotifySettings")));
    }
    private static JsonObject Updates(Database db, string uid, IEnumerable<JsonObject> updates, IEnumerable<Conversation>? conversations = null)
        => T("TL_updates", ("updates", updates.ToArray()), ("date", Now), ("seq", 0),
            ("users", db.Users.Where(u => u.Id == uid || db.Conversations.Any(c => c.Members.Contains(uid) && c.Members.Contains(u.Id))).Select(u => User(db, u, uid)).ToArray()),
            ("chats", (conversations ?? db.Conversations.Where(c => c.Members.Contains(uid))).Where(c => c.Kind == "group").Select(c => Chat(db, c)).ToArray()));
    public static JsonObject CurrentUser(Database db, string uid) => User(db, db.Users.Single(u => u.Id == uid), uid);

    private static JsonElement? Entities(JsonObject request, string text)
    {
        if (request["entities"] is not JsonArray input || input.Count == 0) return null;
        if (input.Count > 100) throw new ApiException(400, "ENTITIES_TOO_LONG");
        var result = new List<JsonObject>();
        foreach (var entity in input)
        {
            string name = S(entity, "$type").Split('$').Last();
            if (name is not ("TL_messageEntityBold" or "TL_messageEntityItalic" or "TL_messageEntityUnderline" or
                "TL_messageEntityStrike" or "TL_messageEntitySpoiler" or "TL_messageEntityCode" or "TL_messageEntityPre" or
                "TL_messageEntityUrl" or "TL_messageEntityTextUrl" or "TL_messageEntityEmail" or "TL_messageEntityPhone" or
                "TL_messageEntityMention" or "TL_messageEntityHashtag" or "TL_messageEntityCashtag" or "TL_messageEntityBotCommand"))
                throw new ApiException(501, "BOTGRAM_ENTITY_UNSUPPORTED");
            int offset = I(N(entity, "offset")), length = I(N(entity, "length"));
            if (offset < 0 || length <= 0 || length > text.Length || offset > text.Length - length)
                throw new ApiException(400, "ENTITY_BOUNDS_INVALID");
            int end = offset + length;
            if ((offset > 0 && char.IsLowSurrogate(text[offset]) && char.IsHighSurrogate(text[offset - 1])) ||
                (end < text.Length && char.IsLowSurrogate(text[end]) && char.IsHighSurrogate(text[end - 1])))
                throw new ApiException(400, "ENTITY_BOUNDS_INVALID");
            var normalized = T(name, ("offset", offset), ("length", length));
            if (name == "TL_messageEntityPre") normalized["language"] = S(entity, "language");
            if (name == "TL_messageEntityTextUrl")
            {
                string url = S(entity, "url");
                if (url.Length is 0 or > 2048) throw new ApiException(400, "ENTITY_URL_INVALID");
                normalized["url"] = url;
            }
            result.Add(normalized);
        }
        return JsonSerializer.SerializeToElement(result, Store.Json);
    }

    public static JsonObject Execute(Database db, string uid, JsonObject request)
    {
        string method = S(request, "$type").Split('$').Last();
        var owned = db.Conversations.Where(c => c.Members.Contains(uid)).ToArray();
        var users = db.Users.Where(u => u.Id == uid || owned.Any(c => c.Members.Contains(u.Id))).Select(u => User(db, u, uid)).ToArray();
        var chats = owned.Where(c => c.Kind == "group").Select(c => Chat(db, c)).ToArray();
        switch (method)
        {
            case "TL_updates_getState": return State(db, uid);
            case "TL_updates_getDifference":
            {
                int after = Math.Max(0, I(N(request, "pts")) - 1);
                var stream = Stream(db, uid).ToArray();
                if (after > stream.Length) return T("TL_updates_differenceTooLong", ("pts", Pts(db, uid)));
                var page = stream.Skip(after).Take(100).ToArray();
                if (page.Length == 0) return T("TL_updates_differenceEmpty", ("date", Now), ("seq", 0));
                var messages = new List<JsonObject>(); var updates = new List<JsonObject>();
                foreach (var e in page)
                {
                    var value = JsonSerializer.SerializeToElement(e.Payload, Store.Json);
                    if (e.Type == "message.created")
                    {
                        var m = value.Deserialize<Message>(Store.Json)!; messages.Add(Message(db, m, uid));
                    }
                    else if (e.Type == "user.gid_changed")
                    {
                        var changedId = value.GetProperty("userId").GetString()!;
                        updates.Add(T("TL_updateUserPhone", ("user_id", UserId(db, changedId)), ("phone", db.Users.Single(u => u.Id == changedId).Gid)));
                    }
                    else
                    {
                        var r = value.Deserialize<Receipt>(Store.Json)!;
                        var c = Auth.Conversation(db, r.ConversationId, uid);
                        updates.Add(T(r.UserId == uid ? "TL_updateReadHistoryInbox" : "TL_updateReadHistoryOutbox",
                            ("peer", Peer(DialogId(db, c, uid))), ("max_id", I(r.MessageId)), ("pts", after + messages.Count + updates.Count + 2), ("pts_count", 1)));
                    }
                }
                bool more = after + page.Length < stream.Length;
                return T(more ? "TL_updates_differenceSlice" : "TL_updates_difference", ("new_messages", messages),
                    ("other_updates", updates), ("users", users), ("chats", chats),
                    (more ? "intermediate_state" : "state", State(db, uid, after + page.Length + 1)));
            }
            case "TL_messages_getDialogs":
            case "TL_messages_getPinnedDialogs":
            case "TL_messages_getPeerDialogs":
            {
                var selected = owned.AsEnumerable();
                if (method == "TL_messages_getPinnedDialogs" || N(request, "folder_id") != 0) selected = [];
                if (method == "TL_messages_getPeerDialogs")
                {
                    var peers = (request["peers"] as JsonArray ?? []).Select(p => PeerId(p?["peer"], db, uid)).ToHashSet();
                    selected = selected.Where(c => peers.Contains(DialogId(db, c, uid)));
                }
                selected = selected.OrderByDescending(c => db.Messages.LastOrDefault(m => m.ConversationId == c.Id)?.Id ?? 0).ThenBy(c => c.Id);
                if (method == "TL_messages_getDialogs")
                {
                    long offset = N(request, "offset_id");
                    long offsetPeer = PeerId(request["offset_peer"], db, uid);
                    if (offsetPeer != 0 && selected.Any(c => DialogId(db, c, uid) == offsetPeer))
                        selected = selected.SkipWhile(c => DialogId(db, c, uid) != offsetPeer).Skip(1);
                    else if (offset != 0) selected = selected.Where(c => (db.Messages.LastOrDefault(m => m.ConversationId == c.Id)?.Id ?? 0) < offset);
                    selected = selected.Take(Math.Clamp(I(N(request, "limit", 100)), 1, 100));
                }
                var array = selected.ToArray();
                string responseType = method != "TL_messages_getDialogs" ? "TL_messages_peerDialogs" : array.Length < owned.Length ? "TL_messages_dialogsSlice" : "TL_messages_dialogs";
                return T(responseType, ("count", owned.Length),
                    ("dialogs", array.Select(c => Dialog(db, c, uid)).ToArray()), ("users", users), ("chats", chats), ("state", State(db, uid)),
                    ("messages", array.Select(c => db.Messages.LastOrDefault(m => m.ConversationId == c.Id)).Where(m => m is not null).Select(m => Message(db, m!, uid)).ToArray()));
            }
            case "TL_messages_getHistory":
            {
                var c = FindConversation(db, PeerId(request["peer"], db, uid), uid, true);
                long before = N(request, "offset_id");
                // Around-message/forward pagination requires a separate contract; fail rather than return a misleading page.
                var history = db.Messages.Where(m => m.ConversationId == c.Id).OrderByDescending(m => m.Id).ToArray();
                int anchor = before != 0 ? Array.FindIndex(history, m => m.Id < before) :
                    N(request, "offset_date") != 0 ? Array.FindIndex(history, m => m.CreatedAt.ToUnixTimeSeconds() < N(request, "offset_date")) : 0;
                if (anchor < 0) anchor = history.Length;
                int start = Math.Clamp(anchor + I(N(request, "add_offset")), 0, history.Length);
                var values = history.Skip(start).Where(m => (N(request, "min_id") == 0 || m.Id > N(request, "min_id")) &&
                    (N(request, "max_id") == 0 || m.Id < N(request, "max_id"))).Take(Math.Clamp(I(N(request, "limit", 50)), 1, 100));
                return T("TL_messages_messages", ("messages", values.Select(m => Message(db, m, uid)).ToArray()), ("users", users), ("chats", chats));
            }
            case "TL_messages_getMessages":
            {
                var ids = (request["id"] as JsonArray ?? []).Select(p => N(p, "id")).ToHashSet();
                var conversationIds = owned.Select(c => c.Id).ToHashSet();
                return T("TL_messages_messages", ("users", users), ("chats", chats),
                    ("messages", db.Messages.Where(m => ids.Contains(m.Id) && conversationIds.Contains(m.ConversationId)).Select(m => Message(db, m, uid)).ToArray()));
            }
            case "TL_messages_sendMessage":
            {
                if (N(request, "schedule_date") != 0 || request["send_as"] is not null || request["reply_markup"] is not null)
                    throw new ApiException(501, "BOTGRAM_MESSAGE_FEATURE_UNSUPPORTED");
                var c = FindConversation(db, PeerId(request["peer"], db, uid), uid, true);
                string text = S(request, "message"), key = "tl:" + N(request, "random_id");
                if (string.IsNullOrWhiteSpace(text) || text.Length > 16000) throw new ApiException(400, "MESSAGE_EMPTY");
                JsonElement? entities = Entities(request, text);
                long? replyId = null;
                if (request["reply_to"] is JsonObject reply)
                {
                    if (!S(reply, "$type").EndsWith("$TL_inputReplyToMessage", StringComparison.Ordinal) || N(reply, "top_msg_id") != 0 ||
                        S(reply, "quote_text").Length != 0 || (reply["reply_to_peer_id"] is not null && PeerId(reply["reply_to_peer_id"], db, uid) != DialogId(db, c, uid)))
                        throw new ApiException(501, "BOTGRAM_REPLY_MODE_UNSUPPORTED");
                    replyId = N(reply, "reply_to_msg_id");
                    if (!db.Messages.Any(m => m.ConversationId == c.Id && m.Id == replyId)) throw new ApiException(400, "REPLY_MESSAGE_ID_INVALID");
                }
                bool silent = request["silent"]?.GetValue<bool>() ?? false;
                var message = db.Messages.FirstOrDefault(m => m.ConversationId == c.Id && m.SenderId == uid && m.ClientMessageId == key);
                if (message is not null && (message.Text != text || message.ReplyToMessageId != replyId || message.Silent != silent ||
                    message.TelegramEntities?.GetRawText() != entities?.GetRawText())) throw new ApiException(409, "RANDOM_ID_DUPLICATE");
                if (message is null)
                {
                    message = new(db.NextMessageId++, c.Id, uid, key, text, DateTimeOffset.UtcNow,
                        TelegramEntities: entities, ReplyToMessageId: replyId, Silent: silent);
                    db.Messages.Add(message); db.Emit(c.Members, "message.created", message);
                }
                return Updates(db, uid, [T("TL_updateMessageID", ("id", I(message.Id)), ("random_id", N(request, "random_id"))),
                    T("TL_updateNewMessage", ("message", Message(db, message, uid)), ("pts", Pts(db, uid)), ("pts_count", 1))]);
            }
            case "TL_messages_readHistory":
            {
                var c = FindConversation(db, PeerId(request["peer"], db, uid), uid);
                long max = db.Messages.Where(m => m.ConversationId == c.Id && m.Id <= N(request, "max_id")).Select(m => m.Id).DefaultIfEmpty(0).Max();
                if (max == 0) return T("TL_messages_affectedMessages", ("pts", Pts(db, uid)), ("pts_count", 0));
                var previous = db.Receipts.FirstOrDefault(r => r.ConversationId == c.Id && r.UserId == uid);
                bool changed = previous is null || previous.MessageId < max;
                if (changed)
                {
                    if (previous is not null) db.Receipts.Remove(previous);
                    var receipt = new Receipt(c.Id, uid, max); db.Receipts.Add(receipt); db.Emit(c.Members, "receipt.updated", receipt);
                }
                return T("TL_messages_affectedMessages", ("pts", Pts(db, uid)), ("pts_count", changed ? 1 : 0));
            }
            case "TL_contacts_search":
            {
                string query = S(request, "q").TrimStart('@');
                var found = db.Users.Where(u => u.Username.Equals(query, StringComparison.OrdinalIgnoreCase)).ToArray();
                return T("TL_contacts_found", ("users", found.Select(u => User(db, u, uid)).ToArray()),
                    ("results", found.Select(u => Peer(UserId(db, u.Id))).ToArray()));
            }
            case "TL_contacts_resolveUsername":
            {
                var user = db.Users.FirstOrDefault(u => u.Username.Equals(S(request, "username"), StringComparison.OrdinalIgnoreCase))
                    ?? throw new ApiException(400, "USERNAME_NOT_OCCUPIED");
                return T("TL_contacts_resolvedPeer", ("peer", Peer(UserId(db, user.Id))), ("users", new[] { User(db, user, uid) }));
            }
            case "TL_users_getUsers":
            {
                var ids = (request["id"] as JsonArray ?? []).Select(p => PeerId(p, db, uid)).ToHashSet();
                return T("Vector", ("objects", db.Users.Where(u => ids.Contains(UserId(db, u.Id))).Select(u => User(db, u, uid)).ToArray()));
            }
            case "TL_contacts_getContacts":
                return T("TL_contacts_contacts", ("saved_count", 0), ("contacts", Array.Empty<object>()), ("users", Array.Empty<object>()));
            case "TL_messages_createChat":
            {
                string title = S(request, "title");
                var ids = (request["users"] as JsonArray ?? []).Select(p => FindUser(db, PeerId(p, db, uid)).Id).Append(uid).Distinct().Order().ToArray();
                if (ids.Length is < 2 or > 100 || string.IsNullOrWhiteSpace(title) || title.Length > 100) throw new ApiException(400, "CHAT_TITLE_EMPTY");
                var c = new Conversation(Guid.NewGuid().ToString("N"), "group", title, ids, DateTimeOffset.UtcNow);
                db.Conversations.Add(c); db.Emit(ids, "conversation.created", c);
                var service = new Message(db.NextMessageId++, c.Id, uid, "service:" + c.Id, "", DateTimeOffset.UtcNow, title, ids);
                db.Messages.Add(service); db.Emit(ids, "message.created", service);
                // The real createChat response in this checkout wraps Updates in invitedUsers.
                return T("TL_messages_invitedUsers", ("updates", Updates(db, uid,
                    [T("TL_updateNewMessage", ("message", Message(db, service, uid)), ("pts", Pts(db, uid)), ("pts_count", 1))], [c])));
            }
            case "TL_users_getFullUser":
            {
                var user = FindUser(db, PeerId(request["id"], db, uid));
                return T("TL_users_userFull", ("users", new[] { User(db, user, uid) }),
                    ("full_user", T("TL_userFull", ("id", UserId(db, user.Id)), ("settings", T("TL_peerSettings")),
                        ("notify_settings", T("TL_peerNotifySettings")), ("about", ""), ("common_chats_count", 0),
                        ("phone_calls_available", false), ("phone_calls_private", false))));
            }
            case "TL_messages_getFullChat":
            {
                var c = FindConversation(db, -N(request, "chat_id"), uid);
                return T("TL_messages_chatFull", ("users", users), ("chats", new[] { Chat(db, c) }),
                    ("full_chat", T("TL_chatFull", ("id", ChatId(db, c)), ("about", ""),
                        ("participants", T("TL_chatParticipants", ("chat_id", ChatId(db, c)), ("version", 1),
                            ("participants", c.Members.Select(id => T("TL_chatParticipant", ("user_id", UserId(db, id)), ("inviter_id", UserId(db, c.Members[0])), ("date", (int)c.CreatedAt.ToUnixTimeSeconds()))).ToArray()))),
                        ("chat_photo", T("TL_photoEmpty", ("id", 0L))), ("notify_settings", T("TL_peerNotifySettings")))));
            }
            case "TL_messages_getChats":
            {
                var ids = (request["id"] as JsonArray ?? []).Select(p => p!.GetValue<long>()).ToHashSet();
                return T("TL_messages_chats", ("chats", owned.Where(c => c.Kind == "group" && ids.Contains(ChatId(db, c))).Select(c => Chat(db, c)).ToArray()));
            }
            case "TL_account_getNotifySettings":
            case "getNotifySettings": return T("TL_peerNotifySettings");
            default: throw new ApiException(501, "BOTGRAM_RPC_UNSUPPORTED:" + method);
        }
    }
}
