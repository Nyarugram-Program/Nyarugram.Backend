using System.Text.Json;
using Google.Protobuf;
using P = Botgram.Protocol.V1;

namespace Botgram.Backend.Storage;

public sealed partial class PostgresBackend
{
    // One authoritative visibility rule for every read path: the caller's cleared
    // boundary, per-user hidden rows and conversation-wide revocations. Deleted
    // content must never reappear through history, search, dialogs or difference replay.
    private const string VisibleMessagesSql = """
        visible AS (
            SELECT m.id,m.body,m.conversation_sequence
            FROM botgram_intake_v1.messages m
            LEFT JOIN botgram_backend_v1.dialogs d
                ON d.conversation_id=m.conversation_id AND d.user_id=$8
            WHERE m.conversation_id=$1
              AND m.id>COALESCE(d.cleared_through,0)
              AND NOT EXISTS(SELECT 1 FROM botgram_backend_v1.message_deletions del WHERE del.message_id=m.id)
              AND NOT EXISTS(SELECT 1 FROM botgram_backend_v1.deleted_messages h
                  WHERE h.message_id=m.id AND h.user_id=$8))
        """;
    private const string HistoryWindowSql = "WITH " + VisibleMessagesSql + """
        ,anchor AS (
            SELECT CASE WHEN $2::bigint<>0 THEN (SELECT count(*) FROM visible WHERE id >= $2)
            WHEN $3::bigint<>0 THEN (SELECT count(*) FROM visible WHERE id >
                COALESCE((SELECT max(id) FROM visible WHERE floor(extract(epoch FROM (body->>'CreatedAt')::timestamptz)) < $3),0))
            ELSE 0 END AS position),
        windowed AS (SELECT id,body,conversation_sequence FROM visible ORDER BY id DESC
            OFFSET (SELECT greatest(0, CASE WHEN position+$4::bigint>2147483647
                THEN position+$4::bigint-4294967296 ELSE position+$4::bigint END) FROM anchor))
        """;
    private const string HistoryFilterSql = " WHERE ($5::bigint=0 OR id>$5) AND ($6::bigint=0 OR id<$6) ORDER BY id DESC LIMIT $7";
    internal const string HistorySql = HistoryWindowSql + " SELECT body::text FROM windowed" + HistoryFilterSql;
    private const string HistoryMetadataSql = HistoryWindowSql + " SELECT id,conversation_sequence FROM windowed" + HistoryFilterSql;

    private static Message StoredMessage(string json) =>
        JsonSerializer.Deserialize<Message>(json) ?? throw new InvalidDataException("Missing stored message");

    private async Task<Message> ReadMessage(Transaction work, Session session, string conversation, long id, long sequence)
    {
        async Task<Message> Authority(CancellationToken token)
        {
            var value = await work.Scalar("SELECT body::text FROM botgram_intake_v1.messages WHERE conversation_id=$1 AND id=$2", conversation, id);
            return value is string json ? StoredMessage(json) : throw new ApiException(404, "message_not_found");
        }
        if (reads is null) return await Authority(work.Token);
        return await reads.ReadAsync(work.Connection, work.NativeTransaction, conversation, id, sequence,
            async token => { await work.Authorize(session); await ConversationById(work, session.UserId, conversation); },
            Authority, work.Token);
    }

    private async Task<Message[]> HistoryMessages(Transaction work, Session session, string conversation, params object[] arguments)
    {
        // $8 is always the caller: hidden rows are filtered inside the shared visible CTE.
        if (reads is null)
            return (await work.Rows(HistorySql, r => StoredMessage(r.GetString(0)), [conversation, .. arguments, session.UserId])).ToArray();
        var metadata = await work.Rows(HistoryMetadataSql, r => (Id: r.GetInt64(0), Sequence: r.GetInt64(1)),
            [conversation, .. arguments, session.UserId]);
        var messages = new List<Message>();
        foreach (var item in metadata) messages.Add(await ReadMessage(work, session, conversation, item.Id, item.Sequence));
        return messages.ToArray();
    }

    // Ids from ANY(batch) that are still visible to the caller; used to keep deleted
    // content out of difference replay and read responses.
    private static async Task<HashSet<long>> VisibleMessageIds(Transaction work, string uid, long[] ids)
    {
        if (ids.Length == 0) return [];
        var visible = await work.Rows("""
            SELECT m.id FROM botgram_intake_v1.messages m
            LEFT JOIN botgram_backend_v1.dialogs d
                ON d.conversation_id=m.conversation_id AND d.user_id=$1
            WHERE m.id=ANY($2) AND m.id>COALESCE(d.cleared_through,0)
              AND NOT EXISTS(SELECT 1 FROM botgram_backend_v1.message_deletions del WHERE del.message_id=m.id)
              AND NOT EXISTS(SELECT 1 FROM botgram_backend_v1.deleted_messages h
                  WHERE h.message_id=m.id AND h.user_id=$1)
            """, r => r.GetInt64(0), uid, (object)ids);
        return visible.ToHashSet();
    }

    private static async Task<P.Updates> MessageUpdates(Transaction work, string uid, string domain,
        AcceptedMessage accepted, Conversation[]? groups = null)
    {
        var result = new P.Updates { DateMs = DateTimeOffset.UtcNow.ToUnixTimeSeconds() * 1000 };
        if (groups is null) result.Groups.Add(await MappedGroups(work, uid));
        else
        {
            var mappings = await GroupIdentities(work, groups);
            result.Groups.Add(groups.Select(c => ProtobufRpcMapper.MapGroup(c, mappings[c.Id])));
        }
        var message = (await MappedMessages(work, uid, domain, [accepted.Message])).Single();
        result.Updates_.Add(new P.Update { NewMessage = new() {
            Message = message, Pts = accepted.Pts, PtsCount = accepted.PtsCount } });
        result.Users.Add((await Related(work, uid, false)).Select(u => ProtobufRpcMapper.MapUser(u, domain)));
        return result;
    }

    private static P.PeerRef ConversationPeer(Conversation conversation, string uid, string domain) =>
        conversation.Kind == "group" ? new() { GroupId = conversation.Id } :
        new() { User = new() { ServerDomain = domain,
            UserId = conversation.Kind == "saved" ? uid : conversation.Members.Single(id => id != uid) } };

    private async Task<Conversation> ResolveConversation(Transaction work, string uid, P.PeerRef? peer,
        string domain, bool create = false)
    {
        if (peer?.PeerCase == P.PeerRef.PeerOneofCase.GroupId) return await FindGroup(work, uid, peer.GroupId);
        var id = peer?.PeerCase switch
        {
            P.PeerRef.PeerOneofCase.User => ProtobufRpcMapper.UserReferenceId(peer.User, domain),
            P.PeerRef.PeerOneofCase.Self when peer.Self => uid,
            _ => throw new ApiException(400, "BOTGRAM_RPC_INVALID_ARGUMENT")
        };
        if (await work.Scalar("SELECT id FROM botgram_backend_v1.accounts WHERE id=$1", id) is null)
            throw new ApiException(404, "USER_ID_INVALID");
        var kind = id == uid ? "saved" : "direct";
        var members = new[] { uid, id }.Distinct().Order().ToArray();
        var found = (await Owned(work, uid)).FirstOrDefault(c => c.Kind == kind && c.Members.Order().SequenceEqual(members));
        if (found is not null) return found;
        if (!create) throw new ApiException(404, "PEER_ID_INVALID");
        return await CreateConversation(work, new(kind, "", members));
    }

    private static async Task<P.GroupSummary[]> MappedGroups(Transaction work, string uid)
    {
        var groups = (await Owned(work, uid)).Where(c => c.Kind == "group").ToArray();
        var mappings = await GroupIdentities(work, groups);
        return groups.Select(c => ProtobufRpcMapper.MapGroup(c, mappings[c.Id])).ToArray();
    }

    private static async Task<P.Message[]> MappedMessages(Transaction work, string uid, string domain, Message[] messages)
    {
        var owned = (await Owned(work, uid)).ToDictionary(c => c.Id);
        var ids = messages.Select(m => m.ConversationId).Distinct().ToArray();
        if (ids.Any(id => !owned.ContainsKey(id))) throw new ApiException(404, "conversation_not_found");
        var receipts = (await work.Rows("""
            SELECT conversation_id,message_id FROM botgram_backend_v1.receipts
            WHERE user_id=$1 AND conversation_id=ANY($2)
            """, r => (Conversation: r.GetString(0), Read: r.GetInt64(1)), uid, ids)).ToDictionary(r => r.Conversation, r => r.Read);
        await GroupIdentities(work, ids.Select(id => owned[id]));
        await Identities(work, IdentityKind.User, messages.SelectMany(m =>
            (m.ServiceMembers ?? []).Append(m.SenderId).Concat(owned[m.ConversationId].Members)).Distinct().ToArray());
        return messages.Select(m => ProtobufRpcMapper.MapMessage(m, ConversationPeer(owned[m.ConversationId], uid, domain),
            uid, domain, receipts.GetValueOrDefault(m.ConversationId))).ToArray();
    }

    private static async Task<P.MessagePage> MessagePage(Transaction work, string uid, string domain,
        Message[] messages, Conversation? participants = null)
    {
        var result = new P.MessagePage();
        result.Groups.Add(await MappedGroups(work, uid));
        result.Messages.Add(await MappedMessages(work, uid, domain, messages));
        var users = participants is null ? await Related(work, uid, true) :
            await Profiles(work, uid, "a.id=ANY($2)", (object)participants.Members);
        result.Users.Add(users.Select(u => ProtobufRpcMapper.MapUser(u, domain)));
        return result;
    }

    private static async Task<P.Difference> MappedDifference(Transaction work, string uid, string domain, int pts)
    {
        var difference = await Difference(work, uid, pts);
        if (difference.Kind == DifferenceKind.TooLong) return new() { TooLong = new() { Pts = difference.Pts } };
        var now = DateTimeOffset.UtcNow.ToUnixTimeSeconds() * 1000;
        if (difference.Kind == DifferenceKind.Empty) return new() { Empty = new() { DateMs = now } };
        var page = new P.DifferencePage { State = new() { Pts = difference.Pts, DateMs = now } };
        page.Groups.Add(await MappedGroups(work, uid));
        // A replayed message.created must not resurrect content deleted afterwards.
        var payloadMessages = difference.Events.Select(e => e.Payload).OfType<Message>().ToArray();
        var visibleMessages = await VisibleMessageIds(work, uid,
            payloadMessages.Select(m => m.Id).Distinct().ToArray());
        page.Messages.Add(await MappedMessages(work, uid, domain,
            payloadMessages.Where(m => visibleMessages.Contains(m.Id)).ToArray()));
        var owned = (await Owned(work, uid)).ToDictionary(c => c.Id);
        for (var i = 0; i < difference.Events.Length; i++)
        {
            if (difference.Events[i].ExpiresAt is { } expires && expires <= DateTimeOffset.UtcNow) continue;
            switch (difference.Events[i].Payload)
            {
                case GidChanged changed:
                    var user = (await Profiles(work, uid, "a.id=$2", changed.UserId)).Single();
                    page.Updates.Add(new P.Update { GidChanged = new()
                    {
                        User = new() { ServerDomain = domain, UserId = user.Id }, Gid = user.Gid
                    } });
                    break;
                case PresenceChanged presence:
                    var changedPresence = (await Profiles(work, uid, "a.id=$2", presence.UserId)).Single();
                    page.Updates.Add(new P.Update { PresenceChanged = new()
                    {
                        User = new() { ServerDomain = domain, UserId = presence.UserId },
                        Status = ProtobufRpcMapper.MapStatus(changedPresence)
                    } });
                    break;
                case Receipt receipt:
                    if (!owned.TryGetValue(receipt.ConversationId, out var conversation))
                        throw new ApiException(404, "conversation_not_found");
                    var read = new P.ReadReceipt { Peer = ConversationPeer(conversation, uid, domain),
                        MaxMessageId = ProtobufRpcMapper.ValidateMessageId(receipt.MessageId),
                        Pts = difference.PreviousCount + i + 2, PtsCount = 1 };
                    page.Updates.Add(receipt.UserId == uid ? new P.Update { ReadInbox = read } : new P.Update { ReadOutbox = read });
                    break;
                case Call call:
                {
                    // Replay converges every call event onto the persisted state; stale or foreign calls stay invisible.
                    var current = (await work.Rows(CallRows + " WHERE id=$1", CallRow, call.Id)).SingleOrDefault();
                    if (current is null || (current.CallerId != uid && current.CalleeId != uid)) break;
                    var ids = await UserIdentities(work, [current.CallerId, current.CalleeId]);
                    page.Updates.Add(new P.Update
                    {
                        PhoneCall = new()
                        {
                            PhoneCall = ProtobufRpcMapper.MapPhoneCall(current, ids[current.CallerId],
                                ids[current.CalleeId], current.CallerId == uid)
                        }
                    });
                    break;
                }
                case CallSignal signal when signal.Type == "tl":
                {
                    // Expired or obsolete signaling must not resurrect a finished call.
                    var current = (await work.Rows(CallRows + " WHERE id=$1", CallRow, signal.CallId)).SingleOrDefault();
                    if (current is null || current.State != "active" ||
                        (current.CallerId != uid && current.CalleeId != uid)) break;
                    page.Updates.Add(new P.Update
                    {
                        PhoneCallSignaling = new()
                        {
                            CallId = current.TelegramId,
                            Data = ByteString.CopyFrom(Convert.FromBase64String(signal.Data))
                        }
                    });
                    break;
                }
                // --- Batch A message lifecycle. All carry the recipient's own pts. ---
                case Message edited when difference.Events[i].Type == "message.edited":
                {
                    if (!visibleMessages.Contains(edited.Id)) break;
                    page.Updates.Add(new P.Update
                    {
                        MessageEdited = new()
                        {
                            Message = (await MappedMessages(work, uid, domain, [edited])).Single(),
                            Pts = difference.PreviousCount + i + 2, PtsCount = 1
                        }
                    });
                    break;
                }
                case MessageDeleted deleted:
                {
                    var update = new P.MessagesDeleted { Pts = difference.PreviousCount + i + 2, PtsCount = 1 };
                    update.MessageIds.Add(deleted.MessageIds);
                    page.Updates.Add(new P.Update { MessagesDeleted = update });
                    break;
                }
                case MessagePinned pinned when owned.TryGetValue(pinned.ConversationId, out var pinnedConversation):
                {
                    var update = new P.MessagesPinned
                    {
                        Peer = ConversationPeer(pinnedConversation, uid, domain),
                        Pinned = pinned.Pinned,
                        Pts = difference.PreviousCount + i + 2, PtsCount = 1
                    };
                    update.MessageIds.Add(pinned.MessageIds);
                    page.Updates.Add(new P.Update { MessagesPinned = update });
                    break;
                }
                case ContentsRead contents:
                {
                    var update = new P.ContentsRead { Pts = difference.PreviousCount + i + 2, PtsCount = 1 };
                    update.MessageIds.Add(contents.MessageIds);
                    page.Updates.Add(new P.Update { ContentsRead = update });
                    break;
                }
                case DraftChanged draft when owned.TryGetValue(draft.ConversationId, out var draftConversation):
                {
                    var update = new P.DraftChanged { Peer = ConversationPeer(draftConversation, uid, domain) };
                    if (draft.Draft is { } stored)
                    {
                        var state = new P.DraftState
                        {
                            Text = stored.Text,
                            UpdatedAtMs = stored.UpdatedAt.ToUnixTimeMilliseconds()
                        };
                        // proto3 optional scalars use presence accessors, not nullable types.
                        if (stored.ReplyTo is { } reply) state.ReplyToMessageId = reply;
                        state.Entities.Add((stored.Entities ?? []).Select(ProtobufRpcMapper.MapEntity));
                        update.Draft = state;
                    }
                    page.Updates.Add(new P.Update { DraftChanged = update });
                    break;
                }
                case UserTyping typing when owned.TryGetValue(typing.ConversationId, out var typingConversation):
                    page.Updates.Add(new P.Update
                    {
                        UserTyping = new()
                        {
                            Peer = ConversationPeer(typingConversation, uid, domain),
                            User = new() { ServerDomain = domain, UserId = typing.UserId },
                            Action = (P.TypingAction)typing.Action
                        }
                    });
                    break;
            }
        }
        var subjects = difference.Events.Select(e => e.Payload).OfType<PresenceChanged>().Select(p => p.UserId)
            .Concat(difference.Events.Select(e => e.Payload).OfType<Call>()
                .SelectMany(c => new[] { c.CallerId, c.CalleeId }))
            .Distinct().ToArray();
        var presenceUsers = await Profiles(work, uid, "a.id=ANY($2)", (object)subjects);
        page.Users.Add((await Related(work, uid, true)).Concat(presenceUsers).DistinctBy(u => u.Id)
            .Select(u => ProtobufRpcMapper.MapUser(u, domain)));
        return difference.Kind == DifferenceKind.Slice ? new() { Slice = page } : new() { Complete = page };
    }

    private async Task<ReadResult> ReadThrough(Transaction work, string uid, Conversation conversation, long maxId)
    {
        await work.Scalar("SELECT sequence FROM botgram_intake_v1.conversations WHERE id=$1 FOR UPDATE", conversation.Id);
        // Only visible messages can advance the cursor: clearing history must not leave
        // the receipt parked on a hidden message.
        var max = (long)(await work.Scalar("""
            SELECT COALESCE(max(m.id),0) FROM botgram_intake_v1.messages m
            LEFT JOIN botgram_backend_v1.dialogs d
                ON d.conversation_id=m.conversation_id AND d.user_id=$2
            WHERE m.conversation_id=$1 AND m.id<=$3 AND m.id>COALESCE(d.cleared_through,0)
              AND NOT EXISTS(SELECT 1 FROM botgram_backend_v1.message_deletions del WHERE del.message_id=m.id)
              AND NOT EXISTS(SELECT 1 FROM botgram_backend_v1.deleted_messages h
                  WHERE h.message_id=m.id AND h.user_id=$2)
            """, conversation.Id, uid, maxId))!;
        if (max == 0) return new(null, false);
        var previous = await work.Scalar("""
            SELECT message_id FROM botgram_backend_v1.receipts WHERE conversation_id=$1 AND user_id=$2
            """, conversation.Id, uid);
        if (previous is long read && read >= max) return new(new(conversation.Id, uid, read), false);
        var receipt = new Receipt(conversation.Id, uid, max);
        await Emit(work, "receipt.updated", receipt, conversation.Id, conversation.Members);
        // read_at backs messages.getOutboxReadDate for the author's private seen row.
        await work.Execute("""
            INSERT INTO botgram_backend_v1.receipts(conversation_id,user_id,message_id,read_at)
            VALUES($1,$2,$3,clock_timestamp())
            ON CONFLICT(conversation_id,user_id) DO UPDATE
                SET message_id=excluded.message_id,read_at=excluded.read_at
            """, conversation.Id, uid, max);
        return new(receipt, true);
    }
}
