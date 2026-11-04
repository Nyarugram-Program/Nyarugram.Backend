using P = Botgram.Protocol.V1;

namespace Botgram.Backend.Storage;

public sealed partial class PostgresBackend
{
    private static async Task<long> PeerIdentity(Transaction work, string uid, P.PeerRef? peer, string domain)
    {
        if (peer?.PeerCase == P.PeerRef.PeerOneofCase.GroupId)
        {
            var group = await FindGroup(work, uid, peer.GroupId);
            return -(await GroupIdentities(work, [group]))[group.Id];
        }
        var id = peer?.PeerCase switch
        {
            P.PeerRef.PeerOneofCase.User => ProtobufRpcMapper.UserReferenceId(peer.User, domain),
            P.PeerRef.PeerOneofCase.Self when peer.Self => uid,
            _ => throw new ApiException(400, "BOTGRAM_RPC_INVALID_ARGUMENT")
        };
        if (await work.Scalar("SELECT id FROM botgram_backend_v1.accounts WHERE id=$1", id) is null)
            throw new ApiException(404, "USER_ID_INVALID");
        return (await Identities(work, IdentityKind.User, [id]))[id];
    }

    private static async Task<DialogPage> Dialogs(Transaction work, string uid, DialogQuery query)
    {
        var owned = await Owned(work, uid);
        var groupIds = await GroupIdentities(work, owned);
        var userIds = await Identities(work, IdentityKind.User, owned.Where(c => c.Kind != "group")
            .Select(c => c.Kind == "saved" ? uid : c.Members.Single(id => id != uid)).Distinct().ToArray());
        var rows = await work.Rows("""
            SELECT c.id,last.body::text,COALESCE(own.message_id,0),
                COALESCE((SELECT max(r.message_id) FROM botgram_backend_v1.receipts r
                    WHERE r.conversation_id=c.id AND r.user_id<>$1),0),
                (SELECT count(*)::integer FROM botgram_intake_v1.messages m
                    WHERE m.conversation_id=c.id AND m.sender_id<>$1 AND m.id>COALESCE(own.message_id,0))
            FROM botgram_backend_v1.conversations c
            JOIN botgram_intake_v1.members member ON member.conversation_id=c.id AND member.user_id=$1
            LEFT JOIN botgram_backend_v1.receipts own ON own.conversation_id=c.id AND own.user_id=$1
            LEFT JOIN LATERAL (SELECT body FROM botgram_intake_v1.messages m
                WHERE m.conversation_id=c.id ORDER BY m.id DESC LIMIT 1) last ON true
            """, r => (Id: r.GetString(0), Last: r.IsDBNull(1) ? null : StoredMessage(r.GetString(1)),
                Inbox: r.GetInt64(2), Outbox: r.GetInt64(3), Unread: r.GetInt32(4)), uid);
        var byId = rows.ToDictionary(r => r.Id);
        IEnumerable<DialogView> selected = owned.Select(c =>
        {
            var row = byId[c.Id];
            var peer = c.Kind == "group" ? -groupIds[c.Id] :
                userIds[c.Kind == "saved" ? uid : c.Members.Single(id => id != uid)];
            return new DialogView(c, peer, row.Last, row.Inbox, row.Outbox, row.Unread, []);
        });
        if (query.Selection == DialogSelection.Pinned || query.FolderId != 0) selected = [];
        if (query.Selection == DialogSelection.Peers) selected = selected.Where(d => (query.Peers ?? []).Contains(d.PeerId));
        selected = selected.OrderByDescending(d => d.LastMessage?.Id ?? 0).ThenBy(d => d.Conversation.Id);
        if (query.Selection == DialogSelection.All)
        {
            if (query.OffsetPeer != 0 && selected.Any(d => d.PeerId == query.OffsetPeer))
                selected = selected.SkipWhile(d => d.PeerId != query.OffsetPeer).Skip(1);
            else if (query.OffsetId != 0) selected = selected.Where(d => (d.LastMessage?.Id ?? 0) < query.OffsetId);
            selected = selected.Take(query.Limit);
        }
        var result = selected.ToArray();
        return new(result, owned.Length, result.Length < owned.Length);
    }

    private static async Task<P.PeerDialogs> MappedDialogs(Transaction work, string uid, string domain, DialogPage page, int pts)
    {
        var result = new P.PeerDialogs { State = new()
        {
            Pts = pts,
            DateMs = DateTimeOffset.UtcNow.ToUnixTimeSeconds() * 1000
        } };
        result.Groups.Add(await MappedGroups(work, uid));
        result.Dialogs.Add(page.Dialogs.Select(d => new P.Dialog
        {
            Peer = ConversationPeer(d.Conversation, uid, domain),
            TopMessageId = ProtobufRpcMapper.ValidateMessageId(d.LastMessage?.Id ?? 0),
            ReadInboxMaxId = ProtobufRpcMapper.ValidateMessageId(d.Inbox),
            ReadOutboxMaxId = ProtobufRpcMapper.ValidateMessageId(d.Outbox),
            UnreadCount = d.UnreadCount, NotifySettings = new()
        }));
        result.Messages.Add(await MappedMessages(work, uid, domain,
            page.Dialogs.Where(d => d.LastMessage is not null).Select(d => d.LastMessage!).ToArray()));
        result.Users.Add((await Related(work, uid, true)).Select(u => ProtobufRpcMapper.MapUser(u, domain)));
        return result;
    }
}
