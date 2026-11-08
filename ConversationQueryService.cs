namespace Botgram.Backend;

public enum DialogSelection { All, Pinned, Peers }
public sealed record DialogQuery(DialogSelection Selection, int FolderId = 0, long OffsetId = 0,
    long OffsetPeer = 0, int Limit = 100, long[]? Peers = null);
// DraftView is the server-side draft snapshot returned with a dialog.
public sealed record DraftView(string Text, MessageEntity[]? Entities, long? ReplyTo, DateTimeOffset UpdatedAt);
public sealed record DialogView(Conversation Conversation, long PeerId, Message? LastMessage,
    long Inbox, long Outbox, int UnreadCount, Receipt[] Receipts,
    // Batch A per-account dialog state; defaults keep legacy in-memory results unchanged.
    bool Pinned = false, bool UnreadMark = false, bool SettingsBarHidden = false,
    long HiddenThrough = 0, long PinOrder = 0, DraftView? Draft = null);
public sealed record DialogPage(DialogView[] Dialogs, int TotalCount, bool IsSlice);
public sealed record MessageView(Message Message, long SenderId, long PeerId, bool Out, bool Unread, long[] ServiceMembers);
public sealed record PeerDetails(UserProfile[] Users, Conversation[] Chats);
public sealed record ConversationListItem(Conversation Conversation, Message? LastMessage, int UnreadCount,
    Receipt[] Receipts, PublicUserResult[] Users);

public static class ConversationQueryService
{
    public static Conversation[] Owned(Database db, string uid) => db.Conversations.Where(c => c.Members.Contains(uid)).ToArray();
    public static Message? LastMessage(Database db, Conversation conversation) => db.Messages.LastOrDefault(m => m.ConversationId == conversation.Id);
    private static DialogView Describe(Database db, Conversation conversation, string uid, long peerId)
    {
        var receipts = db.Receipts.Where(r => r.ConversationId == conversation.Id).ToArray();
        long inbox = receipts.FirstOrDefault(r => r.UserId == uid)?.MessageId ?? 0;
        return new(conversation, peerId, LastMessage(db, conversation), inbox,
            receipts.Where(r => r.UserId != uid).Select(r => r.MessageId).DefaultIfEmpty(0).Max(),
            db.Messages.Count(m => m.ConversationId == conversation.Id && m.SenderId != uid && m.Id > inbox), receipts);
    }
    public static DialogPage Dialogs(Database db, string uid, DialogQuery query)
    {
        var owned = Owned(db, uid);
        IEnumerable<Conversation> selected = owned;
        if (query.Selection == DialogSelection.Pinned || query.FolderId != 0) selected = [];
        if (query.Selection == DialogSelection.Peers)
            selected = selected.Where(c => (query.Peers ?? []).Contains(IdentityService.DialogId(db, c, uid)));
        selected = selected.OrderByDescending(c => LastMessage(db, c)?.Id ?? 0).ThenBy(c => c.Id);
        if (query.Selection == DialogSelection.All)
        {
            if (query.OffsetPeer != 0 && selected.Any(c => IdentityService.DialogId(db, c, uid) == query.OffsetPeer))
                selected = selected.SkipWhile(c => IdentityService.DialogId(db, c, uid) != query.OffsetPeer).Skip(1);
            else if (query.OffsetId != 0) selected = selected.Where(c => (LastMessage(db, c)?.Id ?? 0) < query.OffsetId);
            selected = selected.Take(Math.Clamp(query.Limit, 1, 100));
        }
        var result = selected.Select(c => Describe(db, c, uid, IdentityService.DialogId(db, c, uid))).ToArray();
        return new(result, owned.Length, result.Length < owned.Length);
    }
    public static ConversationListItem[] List(Database db, string uid) => Owned(db, uid).Select(c =>
    {
        var view = Describe(db, c, uid, 0);
        return new ConversationListItem(c, view.LastMessage, view.UnreadCount, view.Receipts,
            db.Users.Where(u => c.Members.Contains(u.Id)).Select(Auth.PublicUser).ToArray());
    }).ToArray();
    public static Message[] Messages(Database db, string uid, long[] ids)
    {
        var owned = Owned(db, uid).Select(c => c.Id).ToHashSet();
        return db.Messages.Where(m => ids.Contains(m.Id) && owned.Contains(m.ConversationId)).ToArray();
    }
    public static MessageView DescribeMessage(Database db, Message message, string uid)
    {
        var conversation = Auth.Conversation(db, message.ConversationId, uid);
        long read = db.Receipts.FirstOrDefault(r => r.ConversationId == conversation.Id && r.UserId == uid)?.MessageId ?? 0;
        return new(message, IdentityService.UserId(db, message.SenderId), IdentityService.DialogId(db, conversation, uid),
            message.SenderId == uid, message.SenderId != uid && message.Id > read,
            (message.ServiceMembers ?? []).Select(id => IdentityService.UserId(db, id)).ToArray());
    }
    public static Conversation[] Chats(Database db, string uid, long[] ids) => Owned(db, uid)
        .Where(c => c.Kind == "group" && ids.Contains(IdentityService.ChatId(db, c))).ToArray();
    public static PeerDetails PeerSettings(Database db, string uid, long peer)
    {
        if (peer > 0) return new([UserQueryService.Find(db, peer, uid)], []);
        var conversation = ConversationMessageService.FindByPeer(db, peer, uid);
        return new(UserQueryService.Participants(db, conversation, uid), [conversation]);
    }
}
