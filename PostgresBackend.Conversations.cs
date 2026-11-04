namespace Botgram.Backend.Storage;

public sealed partial class PostgresBackend
{
    private const string ConversationRows = """
        SELECT c.id,c.kind,c.title,
            ARRAY(SELECT m.user_id FROM botgram_intake_v1.members m WHERE m.conversation_id=c.id ORDER BY m.position,m.user_id COLLATE "C"),
            c.created_at
        FROM botgram_backend_v1.conversations c
        """;

    private static async Task<Conversation[]> Owned(Transaction work, string uid) =>
        (await work.Rows(ConversationRows + """
             WHERE EXISTS(SELECT 1 FROM botgram_intake_v1.members own WHERE own.conversation_id=c.id AND own.user_id=$1)
             ORDER BY c.ordinal
            """, r => new Conversation(r.GetString(0), r.GetString(1), r.GetString(2),
                r.GetFieldValue<string[]>(3), r.GetFieldValue<DateTimeOffset>(4)), uid)).ToArray();

    private async Task<Conversation> CreateConversation(Transaction work, ConversationDefinition definition)
    {
        var members = await work.Rows("""
            SELECT id FROM botgram_backend_v1.accounts WHERE id=ANY($1) ORDER BY id COLLATE "C" FOR SHARE
            """, r => r.GetString(0), (object)definition.Members);
        if (members.Count != definition.Members.Length) throw new ApiException(400, "unknown_member");
        string? directKey = definition.Kind == "group" ? null : definition.Kind + ":" + string.Join(",", definition.Members);
        if (directKey is not null)
        {
            // Separate two-int advisory namespace; hash collisions only serialize unrelated pairs.
            await work.Scalar("SELECT pg_advisory_xact_lock(2147000001,hashtext($1))", directKey);
            var existing = await work.Scalar("SELECT id FROM botgram_backend_v1.conversations WHERE direct_key=$1", directKey);
            if (existing is string id)
                return (await Owned(work, definition.Members[0])).Single(c => c.Id == id);
        }
        var created = DateTimeOffset.UtcNow;
        created = created.AddTicks(-(created.Ticks % 10));
        var conversation = new Conversation(Guid.NewGuid().ToString("N"), definition.Kind, definition.Title, definition.Members, created);
        await work.Execute("INSERT INTO botgram_intake_v1.conversations(id) VALUES($1)", conversation.Id);
        await work.Execute("""
            INSERT INTO botgram_backend_v1.conversations(id,kind,title,created_at,direct_key) VALUES($1,$2,$3,$4,$5::text)
            """, conversation.Id, conversation.Kind, conversation.Title, conversation.CreatedAt, (object?)directKey ?? DBNull.Value);
        await work.Execute("""
            INSERT INTO botgram_intake_v1.members(conversation_id,user_id,position)
            SELECT $1,id,(ordinal-1)::integer FROM unnest($2::text[]) WITH ORDINALITY AS m(id,ordinal)
            """, conversation.Id, conversation.Members);
        await Emit(work, "conversation.created", conversation, conversation.Id, conversation.Members);
        return conversation;
    }

    public Task<Conversation> CreateConversationAsync(Session session, ConversationInput input, CancellationToken token = default)
    {
        var definition = ConversationMessageService.ValidateRestCreation(session.UserId, input);
        return WriteAsync(session, async work =>
        {
            await CheckInvites(work, session.UserId, definition.Kind, definition.Members);
            return await CreateConversation(work, definition);
        }, token);
    }

    private static async Task CheckInvites(Transaction work, string uid, string kind, string[] members)
    {
        if (kind != "group") return;
        var privacy = await PostgresPrivacyPolicy.Load(work.Connection, work.NativeTransaction, members, uid, work.Token);
        if (members.Any(id => !AccountPrivacyService.Allowed(privacy, id, uid, Botgram.Protocol.V1.PrivacyKey.ChatInvite)))
            throw new ApiException(403, "USER_PRIVACY_RESTRICTED");
    }

    public Task<Conversation[]> ConversationsAsync(Session session, CancellationToken token = default) =>
        WriteAsync(session, work => Owned(work, session.UserId), token);

    private static async Task<Conversation> FindGroup(Transaction work, string uid, string id)
    {
        if (!Guid.TryParseExact(id, "N", out _)) throw new ApiException(400, "BOTGRAM_RPC_INVALID_ARGUMENT");
        return (await Owned(work, uid)).SingleOrDefault(c => c.Kind == "group" && c.Id == id)
            ?? throw new ApiException(404, "PEER_ID_INVALID");
    }

    private static Task<Dictionary<string, long>> GroupIdentities(Transaction work, IEnumerable<Conversation> groups) =>
        Identities(work, IdentityKind.Group, groups.Where(c => c.Kind == "group").Select(c => c.Id).ToArray());
}
