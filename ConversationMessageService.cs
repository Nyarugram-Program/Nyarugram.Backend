using Google.Protobuf;
using P = Botgram.Protocol.V1;

namespace Botgram.Backend;

public sealed record SendMessageCommand(string Key, string Text, MessageEntity[]? Entities = null,
    long? ReplyId = null, bool Silent = false, byte[]? E2eeEnvelope = null,
    // Forward attribution carried through the same durable acceptance path (v10).
    string? ForwardFromUserId = null, DateTimeOffset? ForwardFromAt = null,
    // Durable forward request identity (schema v10 body extension); null for normal sends.
    string? ForwardSourceConversationId = null, long? ForwardSourceMessageId = null,
    bool? ForwardDropAuthor = null,
    // Media body and album grouping (batch I); Text holds the caption for media messages.
    MessageMedia? Media = null, long? GroupedId = null);
public sealed record HistoryQuery(long OffsetId = 0, long OffsetDate = 0, int AddOffset = 0,
    long MinId = 0, long MaxId = 0, int Limit = 50);
public sealed record ReadResult(Receipt? Receipt, bool Changed);
public enum MessageWriteContract { Telegram, Rest }
public sealed record ConversationDefinition(string Kind, string Title, string[] Members);

public static class ConversationMessageService
{
    public static Message Send(Database db, Conversation conversation, string uid, SendMessageCommand command,
        MessageWriteContract contract)
    {
        using var diagnostic = RpcDiagnosticStage.Start("send_service_ms");
        byte[]? canonical = null;
        if (command.E2eeEnvelope is { } rawEnvelope)
        {
            try
            {
                canonical = P.E2eeEnvelope.Parser.ParseFrom(rawEnvelope).ToByteArray();
            }
            catch (InvalidProtocolBufferException)
            {
                throw new ApiException(400, "E2EE_ENVELOPE_INVALID");
            }
        }
        // Idempotent retries must return the original message even after the chat state changed.
        var existing = db.Messages.FirstOrDefault(m => m.ConversationId == conversation.Id && m.SenderId == uid && m.ClientMessageId == command.Key);
        if (existing is not null)
        {
            bool conflict = existing.Text != (canonical is null ? command.Text : "") ||
                !EnvelopeEqual(existing.E2eeEnvelope, canonical) ||
                (contract == MessageWriteContract.Telegram &&
                (existing.ReplyToMessageId != command.ReplyId || existing.Silent != command.Silent ||
                 !MessageEntityService.Equal(existing.TelegramEntities, command.Entities)));
            if (conflict) throw new ApiException(409, contract == MessageWriteContract.Telegram ? "RANDOM_ID_DUPLICATE" : "idempotency_conflict");
            return existing;
        }
        var e2ee = E2eeService.ValidateSend(db, conversation, uid, command);
        if (e2ee is null && (string.IsNullOrWhiteSpace(command.Text) || command.Text.Length > 16000))
            throw new ApiException(400, contract == MessageWriteContract.Telegram ? "MESSAGE_EMPTY" : "invalid_text");
        if (contract == MessageWriteContract.Rest && command.Key.Length is < 1 or > 80)
            throw new ApiException(400, "invalid_client_message_id");
        if (command.Entities is { } entities)
        {
            if (e2ee is not null) throw new ApiException(409, "E2EE_ENTITIES_FORBIDDEN");
            MessageEntityService.ValidateCount(entities.Length);
            foreach (var entity in entities) MessageEntityService.Validate(command.Text, entity);
        }
        if (command.ReplyId is not null && !db.Messages.Any(m => m.ConversationId == conversation.Id && m.Id == command.ReplyId))
            throw new ApiException(400, "REPLY_MESSAGE_ID_INVALID");
        AccountPrivacyService.CheckMessage(db, conversation, uid);
        return AddMessage(db, conversation, uid, command.Key, e2ee is null ? command.Text : "",
            command.Entities, command.ReplyId, command.Silent, e2ee);
    }

    public static Message[] History(Database db, Conversation conversation, HistoryQuery query)
    {
        using var diagnostic = RpcDiagnosticStage.Start("history_service_ms");
        var history = db.Messages.Where(m => m.ConversationId == conversation.Id).OrderByDescending(m => m.Id).ToArray();
        int anchor = query.OffsetId != 0 ? Array.FindIndex(history, m => m.Id < query.OffsetId) :
            query.OffsetDate != 0 ? Array.FindIndex(history, m => m.CreatedAt.ToUnixTimeSeconds() < query.OffsetDate) : 0;
        if (anchor < 0) anchor = history.Length;
        int start = Math.Clamp(anchor + query.AddOffset, 0, history.Length);
        return history.Skip(start).Where(m => (query.MinId == 0 || m.Id > query.MinId) &&
            (query.MaxId == 0 || m.Id < query.MaxId)).Take(Math.Clamp(query.Limit, 1, 100)).ToArray();
    }

    public static ReadResult ReadThrough(Database db, Conversation conversation, string uid, long maxId)
    {
        long max = db.Messages.Where(m => m.ConversationId == conversation.Id && m.Id <= maxId).Select(m => m.Id).DefaultIfEmpty(0).Max();
        if (max == 0) return new(null, false);
        var previous = db.Receipts.FirstOrDefault(r => r.ConversationId == conversation.Id && r.UserId == uid);
        bool changed = previous is null || previous.MessageId < max;
        return new(changed ? AdvanceRead(db, conversation, uid, max) : previous, changed);
    }

    public static Conversation CreateRest(Database db, string uid, ConversationInput input)
    {
        var definition = ValidateRestCreation(uid, input);
        var members = definition.Members;
        if (!members.All(id => db.Users.Any(u => u.Id == id))) throw new ApiException(400, "unknown_member");
        if (input.Kind == "group" && members.Any(id => !AccountPrivacyService.Allowed(db, id, uid, Botgram.Protocol.V1.PrivacyKey.ChatInvite)))
            throw new ApiException(403, "USER_PRIVACY_RESTRICTED");
        if (input.Kind == "direct")
        {
            var existing = db.Conversations.FirstOrDefault(c => c.Kind == "direct" && c.Members.SequenceEqual(members));
            if (existing is not null) return existing;
        }
        return Create(db, definition.Kind, definition.Title, members);
    }

    public static ConversationDefinition ValidateRestCreation(string uid, ConversationInput input)
    {
        if (input.Kind is not ("direct" or "group")) throw new ApiException(400, "invalid_kind");
        if (input.MemberIds is not { Length: >= 1 and <= 99 } || input.MemberIds.Any(string.IsNullOrWhiteSpace))
            throw new ApiException(400, "invalid_members");
        var members = input.MemberIds.Append(uid).Distinct().Order().ToArray();
        if (input.Kind == "direct" ? members.Length != 2 : members.Length < 2)
            throw new ApiException(400, "invalid_members");
        if (input.Kind == "group" && (string.IsNullOrWhiteSpace(input.Title) || input.Title.Length > 100))
            throw new ApiException(400, "invalid_title");
        return new(input.Kind, input.Kind == "group" ? input.Title!.Trim() : "", members);
    }

    public static Receipt ReadExact(Database db, Conversation conversation, string uid, long messageId)
    {
        if (!db.Messages.Any(m => m.Id == messageId && m.ConversationId == conversation.Id))
            throw new ApiException(400, "message_not_found");
        return AdvanceRead(db, conversation, uid, messageId);
    }

    public static Conversation Create(Database db, string kind, string title, string[] members)
    {
        var conversation = new Conversation(Guid.NewGuid().ToString("N"), kind, title, members, DateTimeOffset.UtcNow);
        db.Conversations.Add(conversation);
        db.Emit(members, "conversation.created", conversation);
        return conversation;
    }

    public static (Conversation Conversation, Message ServiceMessage) CreateGroup(Database db, string uid, string title, string[] members)
    {
        var ids = members.Append(uid).Distinct().Order().ToArray();
        if (ids.Length is < 2 or > 100 || string.IsNullOrWhiteSpace(title) || title.Length > 100)
            throw new ApiException(400, "CHAT_TITLE_EMPTY");
        if (ids.Any(id => !AccountPrivacyService.Allowed(db, id, uid, Botgram.Protocol.V1.PrivacyKey.ChatInvite)))
            throw new ApiException(403, "USER_PRIVACY_RESTRICTED");
        var conversation = Create(db, "group", title, ids);
        var service = new Message(db.NextMessageId++, conversation.Id, uid, "service:" + conversation.Id, "",
            DateTimeOffset.UtcNow, title, ids);
        db.Messages.Add(service);
        db.Emit(ids, "message.created", service);
        return (conversation, service);
    }

    public static Conversation FindByPeer(Database db, long peer, string uid, bool create = false)
    {
        var result = db.Conversations.FirstOrDefault(c => c.Members.Contains(uid) && IdentityService.DialogId(db, c, uid) == peer);
        if (result is not null) return result;
        if (!create || peer <= 0) throw new ApiException(404, "PEER_ID_INVALID");
        var other = IdentityService.FindUser(db, peer);
        var members = new[] { uid, other.Id }.Distinct().Order().ToArray();
        return Create(db, other.Id == uid ? "saved" : "direct", "", members);
    }

    public static Message AddMessage(Database db, Conversation conversation, string uid, string key, string text,
        MessageEntity[]? entities = null, long? replyId = null, bool silent = false, byte[]? e2eeEnvelope = null,
        string? serviceTitle = null, string[]? serviceMembers = null, string? serviceKind = null)
    {
        var message = new Message(db.NextMessageId++, conversation.Id, uid, key, text, DateTimeOffset.UtcNow,
            serviceTitle, serviceMembers, entities, replyId, silent, e2eeEnvelope, serviceKind);
        db.Messages.Add(message);
        db.Emit(conversation.Members, "message.created", message);
        return message;
    }

    public static Message AddE2eeServiceMessage(Database db, Conversation conversation, string uid, string code)
    {
        var message = new Message(db.NextMessageId++, conversation.Id, uid, "e2ee:" + db.NextMessageId, "",
            DateTimeOffset.UtcNow, code, conversation.Members, null, null, false, null, "e2ee");
        db.Messages.Add(message);
        db.Emit(conversation.Members, "message.created", message);
        return message;
    }

    private static bool EnvelopeEqual(byte[]? left, byte[]? right)
    {
        if (left is null || right is null) return left is null && right is null;
        return left.AsSpan().SequenceEqual(right);
    }

    public static Receipt AdvanceRead(Database db, Conversation conversation, string uid, long messageId)
    {
        var previous = db.Receipts.FirstOrDefault(r => r.ConversationId == conversation.Id && r.UserId == uid);
        if (previous is not null && previous.MessageId >= messageId) return previous;
        if (previous is not null) db.Receipts.Remove(previous);
        var receipt = new Receipt(conversation.Id, uid, messageId);
        db.Receipts.Add(receipt);
        db.Emit(conversation.Members, "receipt.updated", receipt);
        return receipt;
    }
}
