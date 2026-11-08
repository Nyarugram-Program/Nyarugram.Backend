using System.Text.Json;
using P = Botgram.Protocol.V1;

namespace Botgram.Backend.Storage;

/// <summary>
/// Batch A messages.* operations. Every handler runs inside the caller's READ COMMITTED
/// transaction (PostgresBackend.DispatchAsync), so business state, per-user pts, events and
/// the outbox row share one commit. Handlers never commit or open their own connection.
/// </summary>
public sealed partial class PostgresBackend
{
    private static async Task<int> CurrentPts(Transaction work, string uid) =>
        (int)(await work.Scalar("SELECT pts FROM botgram_intake_v1.sync WHERE user_id=$1", uid))!;

    // Resolves message rows the caller may currently see. Missing ids are either unknown
    // or hidden by an earlier delete; strict callers turn that into MESSAGE_ID_INVALID,
    // while delete/read paths treat them as already applied so retries stay idempotent.
    // includeCallerHidden ignores the caller's private hide and cleared boundary: a
    // bilateral revoke must not depend on what the caller still displays locally.
    private static async Task<List<(long Id, string ConversationId, string SenderId, Message Body, long Sequence)>>
        LookupMessages(Transaction work, string uid, long[] ids, bool strict, bool includeCallerHidden = false)
    {
        if (ids.Length == 0) return [];
        var rows = await work.Rows("""
            SELECT m.id,m.conversation_id,m.sender_id,m.body::text,m.conversation_sequence
            FROM botgram_intake_v1.messages m
            JOIN botgram_intake_v1.members own ON own.conversation_id=m.conversation_id AND own.user_id=$1
            LEFT JOIN botgram_backend_v1.dialogs d ON d.conversation_id=m.conversation_id AND d.user_id=$1
            WHERE m.id=ANY($2)
              AND ($3::boolean OR (m.id>COALESCE(d.cleared_through,0)
                   AND NOT EXISTS(SELECT 1 FROM botgram_backend_v1.deleted_messages h
                       WHERE h.message_id=m.id AND h.user_id=$1)))
              AND NOT EXISTS(SELECT 1 FROM botgram_backend_v1.message_deletions del WHERE del.message_id=m.id)
            ORDER BY m.id
            """, r => (Id: r.GetInt64(0), ConversationId: r.GetString(1), SenderId: r.GetString(2),
                Body: StoredMessage(r.GetString(3)), Sequence: r.GetInt64(4)), uid, (object)ids, includeCallerHidden);
        if (strict && rows.Count != ids.Distinct().Count())
            throw new ApiException(400, "MESSAGE_ID_INVALID");
        return rows;
    }

    private static async Task<P.Updates> UpdatesEnvelope(Transaction work, string uid, string domain,
        params P.Update[] updates)
    {
        var result = new P.Updates { DateMs = DateTimeOffset.UtcNow.ToUnixTimeSeconds() * 1000 };
        result.Groups.Add(await MappedGroups(work, uid));
        result.Updates_.Add(updates);
        result.Users.Add((await Related(work, uid, false)).Select(u => ProtobufRpcMapper.MapUser(u, domain)));
        return result;
    }

    private static async Task<long> NextPinOrder(Transaction work, string uid) =>
        (long)(await work.Scalar("""
            SELECT COALESCE(max(pin_order)+1,0) FROM botgram_backend_v1.dialogs WHERE user_id=$1 AND pinned
            """, uid))!;

    private static P.DraftState? MapDraft(StoredDraft? draft, DateTimeOffset? updated)
    {
        if (draft is null) return null;
        var state = new P.DraftState
        {
            Text = draft.Text,
            UpdatedAtMs = (updated ?? draft.UpdatedAt).ToUnixTimeMilliseconds()
        };
        // proto3 optional scalars expose Has*/Clear* rather than nullable types.
        if (draft.ReplyTo is { } reply) state.ReplyToMessageId = reply;
        state.Entities.Add((draft.Entities ?? []).Select(ProtobufRpcMapper.MapEntity));
        return state;
    }

    // ---------------------------------------------------------------- deletes
    private async Task<P.AffectedMessages> DeleteMessages(Transaction work, string uid, P.DeleteMessagesRequest input)
    {
        ProtobufRpcMapper.ValidateCount(input.MessageIds.Count);
        var ids = input.MessageIds.Select(id => ProtobufRpcMapper.ValidateMessageId(id, true)).ToArray();
        ProtobufRpcMapper.ValidateUnique(ids);
        // A bilateral revoke ignores the caller's private hide and cleared boundary: the
        // message still exists for everyone else, so the global delete must apply.
        var found = await LookupMessages(work, uid, ids, false, includeCallerHidden: input.Revoke);
        var owned = (await Owned(work, uid)).ToDictionary(c => c.Id);
        var selfOnly = new List<long>();
        var global = new Dictionary<string, List<long>>();
        foreach (var message in found)
        {
            if (!input.Revoke)
            {
                selfOnly.Add(message.Id);
                continue;
            }
            // Revoke in a direct chat is bilateral; a group message belongs to its author,
            // and this product has no channel/admin roles that could delete another member's.
            if (owned[message.ConversationId].Kind == "group" && message.SenderId != uid)
                throw new ApiException(403, "CHAT_ADMIN_REQUIRED");
            if (!global.TryGetValue(message.ConversationId, out var list))
                global[message.ConversationId] = list = [];
            list.Add(message.Id);
        }
        foreach (var id in selfOnly)
            await work.Execute("""
                INSERT INTO botgram_backend_v1.deleted_messages(user_id,message_id) VALUES($1,$2)
                ON CONFLICT DO NOTHING
                """, uid, id);
        var events = 0;
        foreach (var (conversationId, messageIds) in global)
        {
            var inserted = await work.Rows("""
                INSERT INTO botgram_backend_v1.message_deletions(message_id,conversation_id)
                SELECT m.id,m.conversation_id FROM botgram_intake_v1.messages m WHERE m.id=ANY($1)
                ON CONFLICT(message_id) DO NOTHING RETURNING message_id
                """, r => r.GetInt64(0), (object)messageIds.ToArray());
            if (inserted.Count == 0) continue;
            await Emit(work, "message.deleted", new MessageDeleted(conversationId, inserted.ToArray()),
                conversationId, owned[conversationId].Members);
            // Each emitted event advances every recipient's pts by exactly one.
            events++;
        }
        // A caller-scoped hide consumes no pts, so the difference cursor stays untouched.
        // pts_count reports the events this transaction actually produced for the caller;
        // a multi-conversation batch therefore returns the real count, not a fixed 1.
        return new() { Pts = await CurrentPts(work, uid), PtsCount = events };
    }

    private async Task<P.AffectedHistory> DeleteHistory(Transaction work, string uid, P.DeleteHistoryRequest input,
        string domain)
    {
        var conversation = await ResolveConversation(work, uid, input.Peer, domain);
        var maxId = ProtobufRpcMapper.ValidateMessageId(input.MaxMessageId);
        var boundary = maxId != 0 ? maxId : (long)(await work.Scalar("""
            SELECT COALESCE(max(id),0) FROM botgram_intake_v1.messages WHERE conversation_id=$1
            """, conversation.Id))!;
        var revoke = input.Revoke && conversation.Kind != "group";
        if (input.Revoke && conversation.Kind == "group")
            throw new ApiException(403, "CHAT_ADMIN_REQUIRED");
        // Saved messages keep their dialog visible; other chats hide until a newer id arrives.
        var hide = !input.JustClear && conversation.Kind != "saved" ? boundary : 0L;
        string[] targets = revoke ? conversation.Members : [uid];
        foreach (var target in targets)
            await work.Execute("""
                INSERT INTO botgram_backend_v1.dialogs(user_id,conversation_id,cleared_through,hidden_through)
                VALUES($1,$2,$3,$4)
                ON CONFLICT(user_id,conversation_id) DO UPDATE
                    SET cleared_through=greatest(dialogs.cleared_through,excluded.cleared_through),
                        hidden_through=greatest(dialogs.hidden_through,excluded.hidden_through)
                """, target, conversation.Id, boundary, hide);
        var changed = false;
        if (revoke && boundary > 0)
        {
            var cleared = await work.Rows("""
                SELECT id FROM botgram_intake_v1.messages WHERE conversation_id=$1 AND id<=$2 ORDER BY id
                """, r => r.GetInt64(0), conversation.Id, boundary);
            if (cleared.Count > 0)
            {
                await Emit(work, "message.deleted", new MessageDeleted(conversation.Id, cleared.ToArray()),
                    conversation.Id, conversation.Members);
                changed = true;
            }
        }
        // The Android client re-issues deleteHistory while offset>0; this server always
        // finishes in one call, so the terminal value must be zero.
        return new() { Pts = await CurrentPts(work, uid), PtsCount = changed ? 1 : 0, Offset = 0 };
    }

    private async Task<P.AffectedHistory> DeleteSavedHistory(Transaction work, string uid,
        P.DeleteSavedHistoryRequest input, string domain)
    {
        var conversation = await ResolveConversation(work, uid, input.Peer, domain);
        if (conversation.Kind != "saved") throw new ApiException(404, "PEER_ID_INVALID");
        var maxId = ProtobufRpcMapper.ValidateMessageId(input.MaxMessageId);
        var boundary = maxId != 0 ? maxId : (long)(await work.Scalar("""
            SELECT COALESCE(max(id),0) FROM botgram_intake_v1.messages WHERE conversation_id=$1
            """, conversation.Id))!;
        await work.Execute("""
            INSERT INTO botgram_backend_v1.dialogs(user_id,conversation_id,cleared_through) VALUES($1,$2,$3)
            ON CONFLICT(user_id,conversation_id) DO UPDATE
                SET cleared_through=greatest(dialogs.cleared_through,excluded.cleared_through)
            """, uid, conversation.Id, boundary);
        return new() { Pts = await CurrentPts(work, uid), PtsCount = 0, Offset = 0 };
    }

    private async Task<P.AffectedFoundMessages> DeletePhoneCallHistory(Transaction work, string uid,
        P.DeletePhoneCallHistoryRequest input)
    {
        var cleared = DateTimeOffset.UtcNow;
        await work.Execute("""
            INSERT INTO botgram_backend_v1.call_deletions(user_id,cleared_through) VALUES($1,$2)
            ON CONFLICT(user_id) DO UPDATE SET cleared_through=excluded.cleared_through
            """, uid, cleared);
        if (input.Revoke)
        {
            var peers = await work.Rows("""
                SELECT DISTINCT CASE WHEN caller_id=$1 THEN callee_id ELSE caller_id END FROM botgram_backend_v1.calls
                WHERE caller_id=$1 OR callee_id=$1
                """, r => r.GetString(0), uid);
            foreach (var peer in peers)
                await work.Execute("""
                    INSERT INTO botgram_backend_v1.call_deletions(user_id,cleared_through) VALUES($1,$2)
                    ON CONFLICT(user_id) DO UPDATE SET cleared_through=excluded.cleared_through
                    """, peer, cleared);
        }
        // The Android call-log page builds a local updateDeleteMessages from this list;
        // this server keeps no call-log messages, so it returns an empty page and offset 0.
        return new() { Pts = await CurrentPts(work, uid), PtsCount = 0, Offset = 0 };
    }

    // ---------------------------------------------------------------- edit
    private async Task<P.Updates> EditMessage(Transaction work, string uid, P.EditMessageRequest input,
        string domain)
    {
        // The declared peer must be the conversation that owns the message; membership is
        // already enforced by ResolveConversation.
        var conversation = await ResolveConversation(work, uid, input.Peer, domain);
        var messageId = ProtobufRpcMapper.ValidateMessageId(input.MessageId, true);
        var found = (await LookupMessages(work, uid, [messageId], true)).Single();
        if (found.ConversationId != conversation.Id) throw new ApiException(400, "MESSAGE_ID_INVALID");
        if (found.SenderId != uid) throw new ApiException(403, "MESSAGE_AUTHOR_REQUIRED");
        var body = found.Body;
        // Only ordinary text bodies are editable; service, encrypted and forwarded-only
        // payloads keep their original semantics instead of being rewritten.
        if (body.ServiceTitle is not null || body.E2eeEnvelope is not null)
            throw new ApiException(400, "MESSAGE_ID_INVALID");
        if (DateTimeOffset.UtcNow - body.CreatedAt > TimeSpan.FromHours(48))
            throw new ApiException(400, "MESSAGE_EDIT_TIME_EXPIRED");
        var text = input.Text ?? "";
        if (string.IsNullOrWhiteSpace(text) || text.Length > 16000)
            throw new ApiException(400, "MESSAGE_EMPTY");
        MessageEntityService.ValidateCount(input.Entities.Count);
        var entities = input.Entities.Select(entity => ProtobufRpcMapper.ParseEntity(entity, text)).ToArray();
        if (text == body.Text && MessageEntityService.Equal(entities, body.TelegramEntities))
            throw new ApiException(400, "MESSAGE_NOT_MODIFIED");
        // A media message written before the request snapshot existed: keep its pre-edit
        // content as the snapshot so the original send stays retryable after this edit.
        if (body.Media is { } media && body.MediaRequest is null)
            body = body with
            {
                MediaRequest = new MediaRequestIdentity(body.Text, body.TelegramEntities,
                    body.ReplyToMessageId, body.Silent, media.MediaId)
            };
        var updated = body with { Text = text, TelegramEntities = entities };
        await work.Execute("UPDATE botgram_intake_v1.messages SET body=$2::jsonb WHERE id=$1",
            updated.Id, JsonSerializer.Serialize(updated));
        await Emit(work, "message.edited", updated, conversation.Id, conversation.Members);
        var mapped = (await MappedMessages(work, uid, domain, [updated])).Single();
        return await UpdatesEnvelope(work, uid, domain, new P.Update
        {
            MessageEdited = new() { Message = mapped, Pts = await CurrentPts(work, uid), PtsCount = 1 }
        });
    }

    private async Task<P.MessageEditData> GetMessageEditData(Transaction work, string uid,
        P.GetMessageEditDataRequest input, string domain)
    {
        var conversation = await ResolveConversation(work, uid, input.Peer, domain);
        var messageId = ProtobufRpcMapper.ValidateMessageId(input.MessageId, true);
        var found = (await LookupMessages(work, uid, [messageId], true)).Single();
        if (found.ConversationId != conversation.Id) throw new ApiException(400, "MESSAGE_ID_INVALID");
        if (found.SenderId != uid) throw new ApiException(403, "MESSAGE_AUTHOR_REQUIRED");
        if (found.Body.ServiceTitle is not null || found.Body.E2eeEnvelope is not null)
            throw new ApiException(400, "MESSAGE_ID_INVALID");
        // Media captions do not exist in this product, so caption editing is never available.
        return new() { Caption = false };
    }

    // ---------------------------------------------------------------- forward
    // A committed forward is recognised by the identity persisted in its message body, so a
    // retry is answered before the source row is read at all. Source edits, deletions and
    // cleared history therefore cannot turn a committed retry into a conflict. The lookup is
    // per sender and client key across conversations: the same random_id in another target
    // is a conflicting request, not a second message.
    private static async Task<(string ConversationId, Message Message)[]> CommittedClientKey(Transaction work,
        string senderId, string clientKey)
    {
        return (await work.Rows("""
            SELECT conversation_id,body::text FROM botgram_intake_v1.messages
            WHERE sender_id=$1 AND client_key=$2 ORDER BY id
            """, r => (ConversationId: r.GetString(0), Message: StoredMessage(r.GetString(1))),
            senderId, clientKey)).ToArray();
    }

    // Original per-user pts of a committed message; a retry must never report the current
    // cursor, which would make the client skip intervening updates.
    private static async Task<int> CommittedMessagePts(Transaction work, long messageId, string uid)
    {
        var pts = await work.Scalar("""
            SELECT r.pts FROM botgram_intake_v1.events e
            JOIN botgram_intake_v1.event_recipients r ON r.event_id=e.id
            WHERE e.message_id=$1 AND r.user_id=$2
            """, messageId, uid);
        if (pts is not int value)
        {
            Console.Error.WriteLine($"BOTGRAM_MESSAGE_EVENT_PTS_MISSING message_id={messageId} user={uid}");
            throw new ApiException(500, "BOTGRAM_MESSAGE_EVENT_PTS_MISSING");
        }
        return value;
    }

    // True when the stored message is the commit of exactly this request. A row without the
    // persisted identity predates it: it is accepted only while its stored content still
    // matches the current source, and the identity is then backfilled so later retries no
    // longer depend on the source at all.
    private static async Task RequireSameForwardRequest(Transaction work, Message stored, string sourceConversation,
        long sourceMessageId, P.ForwardMessagesRequest input)
    {
        if (stored.ForwardSourceConversationId is not null)
        {
            var sameRequest = stored.ForwardSourceConversationId == sourceConversation
                && stored.ForwardSourceMessageId == sourceMessageId
                && stored.Silent == input.Silent
                && (stored.ForwardDropAuthor ?? false) == input.DropAuthor;
            if (!sameRequest) throw new ApiException(409, "RANDOM_ID_DUPLICATE");
            return;
        }
        // Records written before the identity was persisted carry only the attribution.
        // A record with attribution was produced by a keep-author forward, so a drop_author
        // request can never be its retry. Uniqueness must rest on immutable evidence only:
        // body text and entities are editable, so filtering by them would drop an edited
        // source from the candidate set and let a lookalike be adopted in its place.
        // Ticks are compared in code because PostgreSQL timestamps truncate to microseconds.
        if (stored.ForwardFromUserId is null || stored.ForwardFromAt is null || input.DropAuthor)
            throw new ApiException(409, "RANDOM_ID_DUPLICATE");
        var candidates = await work.Rows("""
            SELECT body::text FROM botgram_intake_v1.messages WHERE sender_id=$1
            """, r => StoredMessage(r.GetString(0)), stored.ForwardFromUserId);
        var matching = candidates.Where(message => message.CreatedAt == stored.ForwardFromAt.Value).ToArray();
        if (matching.Length != 1 || matching[0].Id != sourceMessageId
            || matching[0].ConversationId != sourceConversation || stored.Silent != input.Silent)
            throw new ApiException(409, "RANDOM_ID_DUPLICATE");
        var upgraded = stored with
        {
            ForwardSourceConversationId = sourceConversation,
            ForwardSourceMessageId = sourceMessageId,
            // A legacy record with attribution was always a keep-author forward.
            ForwardDropAuthor = false
        };
        await work.Execute("UPDATE botgram_intake_v1.messages SET body=$2::jsonb WHERE id=$1",
            upgraded.Id, JsonSerializer.Serialize(upgraded));
    }

    private async Task<P.Updates> ForwardMessages(Transaction work, Session session, P.ForwardMessagesRequest input,
        string domain)
    {
        ProtobufRpcMapper.ValidateCount(input.MessageIds.Count);
        var ids = input.MessageIds.Select(id => ProtobufRpcMapper.ValidateMessageId(id, true)).ToArray();
        var randomIds = input.RandomIds.ToArray();
        if (ids.Length == 0 || ids.Length != randomIds.Length)
            throw new ApiException(400, "BOTGRAM_RPC_INVALID_ARGUMENT");
        ProtobufRpcMapper.ValidateUnique(ids);
        ProtobufRpcMapper.ValidateUnique(randomIds);
        // Membership/existence of both ends resolves before any source row is read.
        var from = await ResolveConversation(work, session.UserId, input.FromPeer, domain);
        var target = await ResolveConversation(work, session.UserId, input.ToPeer, domain, create: true);
        var updates = new List<P.Update>();
        // Forwarded album items keep their grouping under fresh ids.
        var groups = new Dictionary<long, long>();
        long? ForwardGroup(long? source)
        {
            if (source is not { } value) return null;
            if (!groups.TryGetValue(value, out var mapped))
                groups[value] = mapped = Random.Shared.NextInt64(1, long.MaxValue);
            return mapped;
        }
        for (var i = 0; i < ids.Length; i++)
        {
            var key = "tl:" + randomIds[i];
            var committedRows = await CommittedClientKey(work, session.UserId, key);
            var committed = committedRows
                .Where(row => row.ConversationId == target.Id)
                .Select(row => row.Message).FirstOrDefault();
            if (committed is not null)
            {
                await RequireSameForwardRequest(work, committed, from.Id, ids[i], input);
                var originalPts = await CommittedMessagePts(work, committed.Id, session.UserId);
                var existing = (await MappedMessages(work, session.UserId, domain, [committed])).Single();
                updates.Add(new P.Update
                {
                    MessageId = new()
                    {
                        RandomId = randomIds[i],
                        MessageId = ProtobufRpcMapper.ValidateMessageId(committed.Id, true)
                    }
                });
                updates.Add(new P.Update
                {
                    NewMessage = new() { Message = existing, Pts = originalPts, PtsCount = 1 }
                });
                continue;
            }
            // The key is already committed somewhere else: reusing a random_id for another
            // target conversation is a conflicting forward, never a second message.
            if (committedRows.Length > 0) throw new ApiException(409, "RANDOM_ID_DUPLICATE");
            // New request: the source must still be visible and belong to from_peer.
            var origin = (await LookupMessages(work, session.UserId, [ids[i]], true)).Single();
            if (origin.ConversationId != from.Id) throw new ApiException(400, "MESSAGE_ID_INVALID");
            var body = origin.Body;
            // Copying an encrypted or service body would either leak plaintext semantics or
            // fabricate a message the original action never produced.
            if (body.ServiceTitle is not null || body.E2eeEnvelope is not null)
                throw new ApiException(400, "BOTGRAM_FORWARD_UNSUPPORTED");
            var command = new SendMessageCommand(key, body.Text, body.TelegramEntities, null, input.Silent,
                ForwardFromUserId: input.DropAuthor ? null : body.SenderId,
                ForwardFromAt: input.DropAuthor ? null : body.CreatedAt,
                ForwardSourceConversationId: from.Id,
                ForwardSourceMessageId: origin.Id,
                ForwardDropAuthor: input.DropAuthor,
                // A forwarded attachment keeps its media and album grouping.
                Media: body.Media, GroupedId: ForwardGroup(body.GroupedId));
            var accepted = await new PostgresMessageAcceptance(source, maximumBacklog).AcceptInTransactionAsync(
                new(session.TokenHash, session.UserId, target.Id, command, MessageWriteContract.Telegram),
                work.Connection, work.NativeTransaction, work.RetainThroughCommit, work.Token,
                // A forward is a new plaintext send and must pass the same E2EE direction,
                // state and identity admission as an ordinary message. The acceptance path
                // invokes this only for a brand-new send, so a retry of a committed forward
                // still returns its original message and pts without re-evaluating a chat
                // state that legitimately moved on.
                () => ValidateSend(work, session, target, command, domain));
            // A concurrent identical request may commit between the lookup above and the
            // acceptance lock; the accepted message is then the already committed original.
            if (accepted.Duplicate)
                await RequireSameForwardRequest(work, accepted.Message, from.Id, ids[i], input);
            if (body.Media is { } forwardedMedia)
                await LinkMediaMessage(work, forwardedMedia.MediaId, accepted.Message.Id, target.Id);
            var mapped = (await MappedMessages(work, session.UserId, domain, [accepted.Message])).Single();
            updates.Add(new P.Update
            {
                MessageId = new()
                {
                    RandomId = randomIds[i],
                    MessageId = ProtobufRpcMapper.ValidateMessageId(accepted.Message.Id, true)
                }
            });
            updates.Add(new P.Update
            {
                NewMessage = new() { Message = mapped, Pts = accepted.Pts, PtsCount = accepted.PtsCount }
            });
        }
        return await UpdatesEnvelope(work, session.UserId, domain, updates.ToArray());
    }

    // ---------------------------------------------------------------- typing
    private async Task<P.BoolValue> SetTyping(Transaction work, string uid, P.SetTypingRequest input, string domain)
    {
        var conversation = await ResolveConversation(work, uid, input.Peer, domain);
        if (input.Action is P.TypingAction.Unspecified)
            throw new ApiException(400, "BOTGRAM_RPC_INVALID_ARGUMENT");
        // Transient: the six-second expiry keeps it out of every later difference replay.
        await Emit(work, "user.typing", new UserTyping(conversation.Id, uid, (int)input.Action),
            conversation.Id, conversation.Members, DateTimeOffset.UtcNow.AddSeconds(6));
        return new() { Value = true };
    }

    // ---------------------------------------------------------------- drafts
    private async Task<P.BoolValue> SaveDraft(Transaction work, string uid, P.SaveDraftRequest input, string domain)
    {
        var conversation = await ResolveConversation(work, uid, input.Peer, domain);
        var text = input.Text ?? "";
        long? reply = input.HasReplyToMessageId ? input.ReplyToMessageId : null;
        if (reply is { } replyId)
        {
            ProtobufRpcMapper.ValidateMessageId(replyId, true);
            if (await work.Scalar("""
                SELECT 1 FROM botgram_intake_v1.messages WHERE conversation_id=$1 AND id=$2
                """, conversation.Id, replyId) is null)
                throw new ApiException(400, "REPLY_MESSAGE_ID_INVALID");
        }
        if (string.IsNullOrWhiteSpace(text))
        {
            await work.Execute("""
                UPDATE botgram_backend_v1.dialogs SET draft=NULL,draft_updated_at=NULL
                WHERE user_id=$1 AND conversation_id=$2 AND draft IS NOT NULL
                """, uid, conversation.Id);
            await Emit(work, "draft.changed", new DraftChanged(conversation.Id, null),
                conversation.Id, [uid]);
            return new() { Value = true };
        }
        MessageEntityService.ValidateCount(input.Entities.Count);
        var entities = input.Entities.Select(entity => ProtobufRpcMapper.ParseEntity(entity, text)).ToArray();
        var stored = new StoredDraft(text, entities.Length == 0 ? null : entities, reply, DateTimeOffset.UtcNow);
        await work.Execute("""
            INSERT INTO botgram_backend_v1.dialogs(user_id,conversation_id,draft,draft_updated_at)
            VALUES($1,$2,$3::jsonb,$4)
            ON CONFLICT(user_id,conversation_id) DO UPDATE
                SET draft=excluded.draft,draft_updated_at=excluded.draft_updated_at
            """, uid, conversation.Id, JsonSerializer.Serialize(stored), stored.UpdatedAt);
        // Drafts are account-private; only the owning session receives the update.
        await Emit(work, "draft.changed", new DraftChanged(conversation.Id, stored), conversation.Id, [uid]);
        return new() { Value = true };
    }

    private async Task<P.BoolValue> ClearAllDrafts(Transaction work, string uid)
    {
        var cleared = await work.Rows("""
            UPDATE botgram_backend_v1.dialogs SET draft=NULL,draft_updated_at=NULL
            WHERE user_id=$1 AND draft IS NOT NULL RETURNING conversation_id
            """, r => r.GetString(0), uid);
        foreach (var conversationId in cleared)
            await Emit(work, "draft.changed", new DraftChanged(conversationId, null), conversationId, [uid]);
        return new() { Value = true };
    }

    private async Task<P.Updates> GetAllDrafts(Transaction work, string uid, string domain)
    {
        var rows = await work.Rows("""
            SELECT conversation_id,draft::text,draft_updated_at FROM botgram_backend_v1.dialogs
            WHERE user_id=$1 AND draft IS NOT NULL ORDER BY draft_updated_at
            """, r => (Conversation: r.GetString(0),
                Draft: JsonSerializer.Deserialize<StoredDraft>(r.GetString(1)),
                At: r.IsDBNull(2) ? (DateTimeOffset?)null : r.GetFieldValue<DateTimeOffset>(2)), uid);
        var owned = (await Owned(work, uid)).ToDictionary(c => c.Id);
        var updates = new List<P.Update>();
        foreach (var row in rows.Where(r => owned.ContainsKey(r.Conversation)))
            updates.Add(new P.Update
            {
                DraftChanged = new()
                {
                    Peer = ConversationPeer(owned[row.Conversation], uid, domain),
                    Draft = MapDraft(row.Draft, row.At)
                }
            });
        return await UpdatesEnvelope(work, uid, domain, updates.ToArray());
    }

    // ---------------------------------------------------------------- pins
    private async Task<P.Updates> UpdatePinnedMessage(Transaction work, string uid,
        P.UpdatePinnedMessageRequest input, string domain)
    {
        var conversation = await ResolveConversation(work, uid, input.Peer, domain);
        var messageId = ProtobufRpcMapper.ValidateMessageId(input.MessageId, true);
        var message = (await LookupMessages(work, uid, [messageId], true)).Single();
        if (message.ConversationId != conversation.Id) throw new ApiException(400, "MESSAGE_ID_INVALID");
        var changed = input.Unpin
            ? await work.Execute("""
                DELETE FROM botgram_backend_v1.pinned_messages WHERE conversation_id=$1 AND message_id=$2
                """, conversation.Id, message.Id) > 0
            : await work.Scalar("""
                INSERT INTO botgram_backend_v1.pinned_messages(conversation_id,message_id,pinned_by)
                VALUES($1,$2,$3) ON CONFLICT(conversation_id,message_id) DO NOTHING RETURNING 1
                """, conversation.Id, message.Id, uid) is not null;
        var update = new P.MessagesPinned
        {
            Peer = ConversationPeer(conversation, uid, domain),
            Pinned = !input.Unpin
        };
        update.MessageIds.Add(message.Id);
        if (changed)
        {
            await Emit(work, "message.pinned", new MessagePinned(conversation.Id, [message.Id], !input.Unpin),
                conversation.Id, conversation.Members);
            update.Pts = await CurrentPts(work, uid);
            update.PtsCount = 1;
        }
        return await UpdatesEnvelope(work, uid, domain, new P.Update { MessagesPinned = update });
    }

    private async Task<P.AffectedHistory> UnpinAllMessages(Transaction work, string uid,
        P.UnpinAllMessagesRequest input, string domain)
    {
        var conversation = await ResolveConversation(work, uid, input.Peer, domain);
        var removed = await work.Rows("""
            DELETE FROM botgram_backend_v1.pinned_messages WHERE conversation_id=$1 RETURNING message_id
            """, r => r.GetInt64(0), conversation.Id);
        var changed = removed.Count > 0;
        if (changed)
            await Emit(work, "message.pinned", new MessagePinned(conversation.Id, removed.ToArray(), false),
                conversation.Id, conversation.Members);
        return new() { Pts = await CurrentPts(work, uid), PtsCount = changed ? 1 : 0, Offset = 0 };
    }

    // ---------------------------------------------------------------- dialog state
    private async Task<P.BoolValue> ToggleDialogPin(Transaction work, string uid, P.ToggleDialogPinRequest input,
        string domain)
    {
        var conversation = await ResolveConversation(work, uid, input.Peer, domain);
        if (input.Pinned)
            await work.Execute("""
                INSERT INTO botgram_backend_v1.dialogs(user_id,conversation_id,pinned,pin_order)
                VALUES($1,$2,true,$3)
                ON CONFLICT(user_id,conversation_id) DO UPDATE
                    SET pinned=true,
                        pin_order=CASE WHEN dialogs.pinned THEN dialogs.pin_order ELSE excluded.pin_order END
                """, uid, conversation.Id, await NextPinOrder(work, uid));
        else
            await work.Execute("""
                INSERT INTO botgram_backend_v1.dialogs(user_id,conversation_id,pinned,pin_order)
                VALUES($1,$2,false,0)
                ON CONFLICT(user_id,conversation_id) DO UPDATE SET pinned=false,pin_order=0
                """, uid, conversation.Id);
        return new() { Value = true };
    }

    private async Task<P.BoolValue> ReorderPinnedDialogs(Transaction work, string uid,
        P.ReorderPinnedDialogsRequest input, string domain)
    {
        ProtobufRpcMapper.ValidateCount(input.Order.Count);
        var conversations = new List<string>();
        foreach (var peer in input.Order)
            conversations.Add((await ResolveConversation(work, uid, peer, domain)).Id);
        ProtobufRpcMapper.ValidateUnique(conversations);
        for (var i = 0; i < conversations.Count; i++)
            await work.Execute("""
                INSERT INTO botgram_backend_v1.dialogs(user_id,conversation_id,pinned,pin_order)
                VALUES($1,$2,true,$3)
                ON CONFLICT(user_id,conversation_id) DO UPDATE SET pinned=true,pin_order=excluded.pin_order
                """, uid, conversations[i], (long)i);
        return new() { Value = true };
    }

    private async Task<P.BoolValue> MarkDialogUnread(Transaction work, string uid, P.MarkDialogUnreadRequest input,
        string domain)
    {
        var conversation = await ResolveConversation(work, uid, input.Peer, domain);
        await work.Execute("""
            INSERT INTO botgram_backend_v1.dialogs(user_id,conversation_id,unread_mark) VALUES($1,$2,$3)
            ON CONFLICT(user_id,conversation_id) DO UPDATE SET unread_mark=excluded.unread_mark
            """, uid, conversation.Id, input.Unread);
        return new() { Value = true };
    }

    private async Task<P.PeerList> GetDialogUnreadMarks(Transaction work, string uid, string domain)
    {
        var ids = await work.Rows("""
            SELECT d.conversation_id FROM botgram_backend_v1.dialogs d
            JOIN botgram_intake_v1.members m ON m.conversation_id=d.conversation_id AND m.user_id=$1
            WHERE d.user_id=$1 AND d.unread_mark ORDER BY d.conversation_id COLLATE "C"
            """, r => r.GetString(0), uid);
        var owned = (await Owned(work, uid)).ToDictionary(c => c.Id);
        var result = new P.PeerList();
        foreach (var id in ids)
            if (owned.TryGetValue(id, out var conversation))
                result.Peers.Add(ConversationPeer(conversation, uid, domain));
        return result;
    }

    private async Task<P.BoolValue> HidePeerSettingsBar(Transaction work, string uid,
        P.HidePeerSettingsBarRequest input, string domain)
    {
        var conversation = await ResolveConversation(work, uid, input.Peer, domain);
        await work.Execute("""
            INSERT INTO botgram_backend_v1.dialogs(user_id,conversation_id,settings_bar_hidden) VALUES($1,$2,true)
            ON CONFLICT(user_id,conversation_id) DO UPDATE SET settings_bar_hidden=true
            """, uid, conversation.Id);
        return new() { Value = true };
    }

    // ---------------------------------------------------------------- contents / views
    private async Task<P.AffectedMessages> ReadMessageContents(Transaction work, string uid,
        P.ReadMessageContentsRequest input)
    {
        ProtobufRpcMapper.ValidateCount(input.MessageIds.Count);
        var ids = input.MessageIds.Select(id => ProtobufRpcMapper.ValidateMessageId(id, true)).ToArray();
        ProtobufRpcMapper.ValidateUnique(ids);
        // Already hidden/removed ids count as read so a retried request stays idempotent.
        var found = await LookupMessages(work, uid, ids, false);
        var events = 0;
        foreach (var group in found.GroupBy(m => m.ConversationId))
        {
            var inserted = new List<long>();
            foreach (var message in group)
                if (await work.Scalar("""
                    INSERT INTO botgram_backend_v1.content_reads(user_id,conversation_id,message_id) VALUES($1,$2,$3)
                    ON CONFLICT(user_id,message_id) DO NOTHING RETURNING 1
                    """, uid, group.Key, message.Id) is not null)
                    inserted.Add(message.Id);
            if (inserted.Count == 0) continue;
            await Emit(work, "content.read", new ContentsRead(group.Key, inserted.ToArray()), group.Key, [uid]);
            // One event per affected conversation, each advancing the caller's pts by one.
            events++;
        }
        return new() { Pts = await CurrentPts(work, uid), PtsCount = events };
    }

    private async Task<P.MessageViewsList> GetMessagesViews(Transaction work, string uid,
        P.GetMessagesViewsRequest input, string domain)
    {
        var conversation = await ResolveConversation(work, uid, input.Peer, domain);
        ProtobufRpcMapper.ValidateCount(input.MessageIds.Count);
        var ids = input.MessageIds.Select(id => ProtobufRpcMapper.ValidateMessageId(id, true)).ToArray();
        ProtobufRpcMapper.ValidateUnique(ids);
        // readable = visible to the caller at all; visible = additionally inside the
        // requested conversation. Increment and reporting both use the scoped set, so a
        // wrong peer can neither add a viewer nor reveal another conversation's count.
        var messages = await LookupMessages(work, uid, ids, false);
        var visible = messages.Where(message => message.ConversationId == conversation.Id)
            .Select(message => message.Id).ToHashSet();
        if (input.Increment)
            foreach (var id in visible)
            {
                // One viewer contributes once; repeated opens never inflate the counter.
                if (await work.Scalar("""
                    INSERT INTO botgram_backend_v1.message_viewers(message_id,user_id) VALUES($1,$2)
                    ON CONFLICT DO NOTHING RETURNING 1
                    """, id, uid) is null) continue;
                await work.Execute("""
                    INSERT INTO botgram_backend_v1.message_views(message_id,view_count) VALUES($1,1)
                    ON CONFLICT(message_id) DO UPDATE
                        SET view_count=botgram_backend_v1.message_views.view_count+1
                    """, id);
            }
        var counts = visible.Count == 0 ? new Dictionary<long, long>() : (await work.Rows("""
            SELECT message_id,view_count FROM botgram_backend_v1.message_views WHERE message_id=ANY($1)
            """, r => (Id: r.GetInt64(0), Count: r.GetInt64(1)), (object)visible.ToArray()))
            .ToDictionary(x => x.Id, x => x.Count);
        var result = new P.MessageViewsList();
        foreach (var id in ids)
            result.Views.Add((int)Math.Min(int.MaxValue, counts.GetValueOrDefault(id)));
        return result;
    }

    private async Task<P.OutboxReadDate> GetOutboxReadDate(Transaction work, string uid,
        P.GetOutboxReadDateRequest input, string domain)
    {
        var conversation = await ResolveConversation(work, uid, input.Peer, domain);
        if (conversation.Kind != "direct") throw new ApiException(400, "PEER_ID_INVALID");
        var messageId = ProtobufRpcMapper.ValidateMessageId(input.MessageId, true);
        var message = (await LookupMessages(work, uid, [messageId], true)).Single();
        // The message must belong to the declared direct conversation.
        if (message.ConversationId != conversation.Id) throw new ApiException(400, "MESSAGE_ID_INVALID");
        if (message.SenderId != uid) throw new ApiException(403, "MESSAGE_AUTHOR_REQUIRED");
        var peer = conversation.Members.Single(id => id != uid);
        // Read the epoch inside SQL: a scalar timestamptz comes back as DateTime, which
        // is not a DateTimeOffset and would silently fail the presence check below.
        var readAtMs = await work.Scalar("""
            SELECT (extract(epoch FROM read_at)*1000)::bigint FROM botgram_backend_v1.receipts
            WHERE conversation_id=$1 AND user_id=$2 AND message_id>=$3 AND read_at IS NOT NULL
            """, conversation.Id, peer, message.Id);
        // Unread or privacy-restricted: the original page shows its own localized message.
        if (readAtMs is not long readAt) throw new ApiException(403, "USER_PRIVACY_RESTRICTED");
        return new() { DateMs = readAt };
    }

    // ---------------------------------------------------------------- read-only lookups
    private async Task<P.ChatOnlines> GetOnlines(Transaction work, string uid, P.GetOnlinesRequest input,
        string domain)
    {
        if (input.Peer?.PeerCase != P.PeerRef.PeerOneofCase.GroupId)
            throw new ApiException(400, "PEER_ID_INVALID");
        var group = await FindGroup(work, uid, input.Peer.GroupId);
        var onlines = (int)(await work.Scalar("""
            SELECT count(*)::integer FROM botgram_intake_v1.members m
            JOIN botgram_backend_v1.presence_accounts p ON p.user_id=m.user_id
            WHERE m.conversation_id=$1 AND p.online
              AND p.last_seen>clock_timestamp()-interval '30 seconds'
            """, group.Id))!;
        return new() { Onlines = onlines };
    }

    private async Task<P.GroupList> GetCommonChats(Transaction work, string uid, P.GetCommonChatsRequest input,
        string domain)
    {
        var target = ProtobufRpcMapper.SelectedUserId(input.User, uid, domain);
        if (await work.Scalar("SELECT 1 FROM botgram_backend_v1.accounts WHERE id=$1", target) is null)
            throw new ApiException(404, "USER_ID_INVALID");
        var limit = ProtobufRpcMapper.ValidateLimit(input.Limit != 0, input.Limit, 100);
        var below = ProtobufRpcMapper.ValidateMessageId(input.MaxChatId);
        var shared = await work.Rows("""
            SELECT c.id,c.kind,c.title,
                ARRAY(SELECT m.user_id FROM botgram_intake_v1.members m
                    WHERE m.conversation_id=c.id ORDER BY m.position,m.user_id COLLATE "C"),
                c.created_at
            FROM botgram_backend_v1.conversations c
            JOIN botgram_intake_v1.members own ON own.conversation_id=c.id AND own.user_id=$1
            JOIN botgram_intake_v1.members other ON other.conversation_id=c.id AND other.user_id=$2
            WHERE c.kind='group' ORDER BY c.ordinal
            """, r => new Conversation(r.GetString(0), r.GetString(1), r.GetString(2),
                r.GetFieldValue<string[]>(3), r.GetFieldValue<DateTimeOffset>(4)), uid, target);
        var mappings = await GroupIdentities(work, shared);
        var result = new P.GroupList();
        result.Groups.Add(shared.Select(c => (Id: mappings[c.Id], Group: c))
            .Where(x => below == 0 || x.Id < below)
            .OrderByDescending(x => x.Id).Take(limit)
            .Select(x => ProtobufRpcMapper.MapGroup(x.Group, x.Id)));
        return result;
    }

    private async Task<P.BoolValue> ReportSpam(Transaction work, string uid, P.ReportSpamRequest input, string domain)
    {
        var conversation = await ResolveConversation(work, uid, input.Peer, domain);
        await work.Execute("""
            INSERT INTO botgram_backend_v1.peer_reports(reporter_id,conversation_id,kind) VALUES($1,$2,'spam')
            ON CONFLICT DO NOTHING
            """, uid, conversation.Id);
        return new() { Value = true };
    }

    // ---------------------------------------------------------------- search
    // Shared WHERE for conversation search and its counters. Parameter order is fixed:
    // $1 uid, $2 conversation, $3 escaped query, $4 sender, $5 min epoch, $6 max epoch,
    // $7 min id, $8 max id, $9 pinned only.
    private const string SearchFilterSql = """
        FROM botgram_intake_v1.messages m
        LEFT JOIN botgram_backend_v1.dialogs d ON d.conversation_id=m.conversation_id AND d.user_id=$1
        WHERE m.conversation_id=$2 AND m.id>COALESCE(d.cleared_through,0)
          AND NOT EXISTS(SELECT 1 FROM botgram_backend_v1.message_deletions del WHERE del.message_id=m.id)
          AND NOT EXISTS(SELECT 1 FROM botgram_backend_v1.deleted_messages h
              WHERE h.message_id=m.id AND h.user_id=$1)
          AND (m.body->>'ServiceTitle') IS NULL AND (m.body->>'E2eeEnvelope') IS NULL
          AND ($3::text='' OR (m.body->>'Text') ILIKE '%'||$3||'%' ESCAPE '\')
          AND ($4::text='' OR m.sender_id=$4)
          AND ($5::bigint=0 OR floor(extract(epoch FROM (m.body->>'CreatedAt')::timestamptz))>=$5)
          AND ($6::bigint=0 OR floor(extract(epoch FROM (m.body->>'CreatedAt')::timestamptz))<=$6)
          AND ($7::bigint=0 OR m.id>=$7)
          AND ($8::bigint=0 OR m.id<=$8)
          AND (NOT $9::boolean OR EXISTS(SELECT 1 FROM botgram_backend_v1.pinned_messages p
              WHERE p.message_id=m.id AND p.conversation_id=$2))
        """;

    private static string EscapeLike(string value) =>
        value.Replace("\\", "\\\\").Replace("%", "\\%").Replace("_", "\\_");

    private static (string Query, long MinDate, long MaxDate) SearchBounds(P.SearchRequest input)
    {
        var query = EscapeLike((input.Query ?? "").Trim());
        if (query.Length > 256) throw new ApiException(400, "BOTGRAM_RPC_INVALID_ARGUMENT");
        var minDate = input.MinDateMs <= 0 ? 0 : input.MinDateMs / 1000;
        var maxDate = input.MaxDateMs <= 0 ? 0 : input.MaxDateMs / 1000;
        return (query, minDate, maxDate);
    }

    private async Task<P.SearchPage> Search(Transaction work, string uid, P.SearchRequest input, string domain)
    {
        var conversation = await ResolveConversation(work, uid, input.Peer, domain);
        if (input.Filter is not (P.SearchFilter.Unspecified or P.SearchFilter.Empty or P.SearchFilter.Pinned))
            throw new ApiException(501, "BOTGRAM_SEARCH_FILTER_UNSUPPORTED");
        if (input.AddOffset < 0) throw new ApiException(501, "BOTGRAM_RPC_UNSUPPORTED:add_offset");
        var (query, minDate, maxDate) = SearchBounds(input);
        var sender = input.FromUser is null
            ? "" : ProtobufRpcMapper.SelectedUserId(input.FromUser, uid, domain);
        var limit = ProtobufRpcMapper.ValidateLimit(input.Limit != 0, input.Limit, 50);
        var pinnedOnly = input.Filter == P.SearchFilter.Pinned;
        var offsetId = ProtobufRpcMapper.ValidateMessageId(input.OffsetMessageId);
        var minId = ProtobufRpcMapper.ValidateMessageId(input.MinMessageId);
        var maxId = ProtobufRpcMapper.ValidateMessageId(input.MaxMessageId);
        object[] Filter() => [uid, conversation.Id, query, sender, minDate, maxDate, minId, maxId, pinnedOnly];
        var total = (int)(await work.Scalar("SELECT count(*)::integer " + SearchFilterSql, Filter()))!;
        var messages = await work.Rows("SELECT m.body::text " + SearchFilterSql + """
              AND ($10::bigint=0 OR m.id<$10)
            ORDER BY m.id DESC LIMIT $11 OFFSET $12
            """, r => StoredMessage(r.GetString(0)),
            [.. Filter(), offsetId, limit, (long)Math.Max(0, input.AddOffset)]);
        var result = new P.SearchPage { Count = total };
        result.Messages.Add(await MappedMessages(work, uid, domain, messages.ToArray()));
        result.Groups.Add(await MappedGroups(work, uid));
        result.Users.Add((await Related(work, uid, true)).Select(u => ProtobufRpcMapper.MapUser(u, domain)));
        return result;
    }

    private async Task<P.SearchCounters> GetSearchCounters(Transaction work, string uid,
        P.GetSearchCountersRequest input, string domain)
    {
        var conversation = await ResolveConversation(work, uid, input.Peer, domain);
        ProtobufRpcMapper.ValidateCount(input.Filters.Count);
        var result = new P.SearchCounters();
        foreach (var filter in input.Filters)
        {
            if (filter is not (P.SearchFilter.Unspecified or P.SearchFilter.Empty or P.SearchFilter.Pinned))
                throw new ApiException(501, "BOTGRAM_SEARCH_FILTER_UNSUPPORTED");
            var count = (int)(await work.Scalar("SELECT count(*)::integer " + SearchFilterSql,
                uid, conversation.Id, "", "", 0L, 0L, 0L, 0L,
                filter == P.SearchFilter.Pinned))!;
            result.Counters.Add(new P.SearchCounter { Filter = filter, Count = count });
        }
        return result;
    }
}
