namespace Botgram.Backend.Storage;

public sealed partial class PostgresBackend
{
    private static async Task<Conversation> ConversationById(Transaction work, string uid, string id) =>
        (await Owned(work, uid)).SingleOrDefault(c => c.Id == id) ?? throw new ApiException(404, "conversation_not_found");

    public Task<PublicUserResult[]> SearchUsersAsync(Session session, string? username, CancellationToken token = default)
    {
        if (username is not { Length: >= 3 and <= 32 }) throw new ApiException(400, "exact_username_required");
        return WriteAsync(session, async work =>
            (await work.Rows("""
                SELECT id,username,display_name,salt,password_hash,gid,premium FROM botgram_backend_v1.accounts WHERE username=$1
                """, UserRow, username.ToLowerInvariant())).Select(Auth.PublicUser).ToArray(), token);
    }

    public Task<ConversationListItem[]> ConversationListAsync(Session session, CancellationToken token = default) =>
        WriteAsync(session, async work =>
        {
            var uid = session.UserId;
            var conversations = await Owned(work, uid);
            var ids = conversations.Select(c => c.Id).ToArray();
            var receipts = await work.Rows("""
                SELECT conversation_id,user_id,message_id FROM botgram_backend_v1.receipts WHERE conversation_id=ANY($1)
                """, r => new Receipt(r.GetString(0), r.GetString(1), r.GetInt64(2)), (object)ids);
            var summaries = await work.Rows("""
                SELECT c.id,last.body::text,(SELECT count(*)::integer FROM botgram_intake_v1.messages m
                    WHERE m.conversation_id=c.id AND m.sender_id<>$2 AND m.id>COALESCE(r.message_id,0))
                FROM botgram_backend_v1.conversations c
                LEFT JOIN botgram_backend_v1.receipts r ON r.conversation_id=c.id AND r.user_id=$2
                LEFT JOIN LATERAL (SELECT body FROM botgram_intake_v1.messages m
                    WHERE m.conversation_id=c.id ORDER BY m.id DESC LIMIT 1) last ON true
                WHERE c.id=ANY($1)
                """, r => (Id: r.GetString(0), Last: r.IsDBNull(1) ? null : StoredMessage(r.GetString(1)),
                    Unread: r.GetInt32(2)), ids, uid);
            var users = await work.Rows("""
                SELECT id,username,display_name,salt,password_hash,gid,premium FROM botgram_backend_v1.accounts
                WHERE id=ANY($1) ORDER BY ordinal
                """, UserRow, (object)conversations.SelectMany(c => c.Members).Distinct().ToArray());
            return conversations.Select(c =>
            {
                var summary = summaries.Single(s => s.Id == c.Id);
                return new ConversationListItem(c, summary.Last, summary.Unread,
                    receipts.Where(r => r.ConversationId == c.Id).ToArray(),
                    users.Where(u => c.Members.Contains(u.Id)).Select(Auth.PublicUser).ToArray());
            }).ToArray();
        }, token);

    public Task<Message[]> HistoryAsync(Session session, string id, long? before, int? limit, CancellationToken token = default)
    {
        if (before is not (null or > 0) || limit is not (null or >= 1 and <= 100))
            throw new ApiException(400, "invalid_pagination");
        return WriteAsync(session, async work =>
        {
            await ConversationById(work, session.UserId, id);
            var messages = await HistoryMessages(work, session,
                id, before ?? 0L, 0L, 0, 0L, 0L, limit ?? 50);
            return messages.AsEnumerable().Reverse().ToArray();
        }, token);
    }

    public Task<T> SendRestAsync<T>(Session session, string id, MessageInput input, Func<Message, T> prepareResponse,
        CancellationToken token = default)
    {
        if (string.IsNullOrWhiteSpace(input.Text) || input.Text.Length > 16000) throw new ApiException(400, "invalid_text");
        if (input.ClientMessageId is not { Length: >= 1 and <= 80 }) throw new ApiException(400, "invalid_client_message_id");
        return WriteAsync(session, async work =>
        {
            await ConversationById(work, session.UserId, id);
            var accepted = await new PostgresMessageAcceptance(source, maximumBacklog).AcceptInTransactionAsync(
                new(session.TokenHash, session.UserId, id, new(input.ClientMessageId, input.Text), MessageWriteContract.Rest),
                work.Connection, work.NativeTransaction, work.RetainThroughCommit, work.Token);
            return prepareResponse(accepted.Message);
        }, token);
    }

    public Task<Receipt> ReadExactAsync(Session session, string id, long messageId, CancellationToken token = default) =>
        WriteAsync(session, async work =>
        {
            var conversation = await ConversationById(work, session.UserId, id);
            if (await work.Scalar("SELECT id FROM botgram_intake_v1.messages WHERE conversation_id=$1 AND id=$2", id, messageId) is null)
                throw new ApiException(400, "message_not_found");
            return (await ReadThrough(work, session.UserId, conversation, messageId)).Receipt!;
        }, token);
}
