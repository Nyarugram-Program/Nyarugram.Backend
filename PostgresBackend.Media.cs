using P = Botgram.Protocol.V1;

namespace Botgram.Backend.Storage;

// Bounded limits for the binary media transport. One HTTP request carries one part.
public static class MediaPolicy
{
    public const int MaxPartBytes = 1024 * 1024;
    public const int MaxParts = 16384;
    public const long MaxFileBytes = 2L * 1024 * 1024 * 1024;
    public const long MaxPendingBytesPerAccount = 4L * 1024 * 1024 * 1024;
    public const int MaxAlbumItems = 10;
    public const int MaxMimeLength = 128;
    public const int MaxFileNameLength = 255;
}

public sealed record MediaDownload(byte[] Bytes, long TotalSize);

/// <summary>
/// Batch I media: part upload, range download and the four media RPC handlers. Bytes never
/// enter the RPC envelope; the binary endpoints call these methods with the same session.
/// </summary>
public sealed partial class PostgresBackend
{
    // ---------------------------------------------------------------- binary transport
    public Task UploadPartAsync(Session session, long clientFileId, int part, int totalParts,
        byte[] bytes, CancellationToken token = default)
    {
        if (clientFileId == 0) throw new ApiException(400, "MEDIA_FILE_ID_INVALID");
        // The Android client sends -1 when the total is unknown until the stream ends.
        var declared = totalParts < 0 ? 0 : totalParts;
        if (part < 0 || part >= MediaPolicy.MaxParts || declared > MediaPolicy.MaxParts
            || (declared > 0 && part >= declared))
            throw new ApiException(400, "MEDIA_PART_INVALID");
        if (bytes.Length is < 1 or > MediaPolicy.MaxPartBytes)
            throw new ApiException(413, "MEDIA_PART_TOO_LARGE");
        return WriteAsync<object?>(session, async work =>
        {
            // Serialize writers of one upload so the row, part count and quota stay coherent.
            await work.Scalar("SELECT pg_advisory_xact_lock(2147000003,hashtext($1))",
                session.UserId + ":" + clientFileId);
            var rows = await work.Rows("""
                SELECT id,total_parts,completed FROM botgram_backend_v1.media_files
                WHERE owner_id=$1 AND client_file_id=$2
                """, r => (Id: r.GetInt64(0), Total: r.GetInt32(1), Completed: r.GetBoolean(2)),
                session.UserId, clientFileId);
            long mediaId;
            if (rows.Count == 0)
            {
                mediaId = (long)(await work.Scalar("""
                    INSERT INTO botgram_backend_v1.media_files(owner_id,client_file_id,total_parts)
                    VALUES($1,$2,$3) RETURNING id
                    """, session.UserId, clientFileId, declared))!;
            }
            else
            {
                if (rows[0].Completed) throw new ApiException(409, "MEDIA_ALREADY_COMPLETED");
                if (rows[0].Total > 0 && declared > 0 && rows[0].Total != declared)
                    throw new ApiException(409, "MEDIA_UPLOAD_CONFLICT");
                mediaId = rows[0].Id;
                if (rows[0].Total == 0 && declared > 0)
                    await work.Execute("UPDATE botgram_backend_v1.media_files SET total_parts=$2 WHERE id=$1",
                        mediaId, declared);
            }
            if (await work.Scalar("""
                INSERT INTO botgram_backend_v1.media_parts(media_id,part,bytes) VALUES($1,$2,$3)
                ON CONFLICT(media_id,part) DO NOTHING RETURNING 1
                """, mediaId, part, bytes) is null)
            {
                // Retrying the same part is fine; the same part with other bytes is a conflict.
                if (await work.Scalar("""
                    SELECT 1 FROM botgram_backend_v1.media_parts WHERE media_id=$1 AND part=$2 AND bytes=$3
                    """, mediaId, part, bytes) is null)
                    throw new ApiException(409, "MEDIA_PART_CONFLICT");
                return null;
            }
            var pending = (long)(await work.Scalar("""
                SELECT COALESCE(sum(octet_length(p.bytes)),0) FROM botgram_backend_v1.media_parts p
                JOIN botgram_backend_v1.media_files f ON f.id=p.media_id
                WHERE f.owner_id=$1 AND NOT f.completed
                """, session.UserId))!;
            if (pending > MediaPolicy.MaxPendingBytesPerAccount)
                throw new ApiException(507, "MEDIA_QUOTA_EXCEEDED");
            return null;
        }, token);
    }

    public Task<MediaDownload> DownloadAsync(Session session, long mediaId, long offset, int limit,
        CancellationToken token = default)
    {
        if (mediaId <= 0) throw new ApiException(400, "MEDIA_INVALID");
        if (offset < 0 || limit is < 1 or > MediaPolicy.MaxPartBytes)
            throw new ApiException(400, "MEDIA_RANGE_INVALID");
        return WriteAsync(session, async work =>
        {
            // Bytes stay available only while a message reference is still visible to the
            // caller: a global deletion or the caller's own hide revokes the download.
            var total = await work.Scalar("""
                SELECT f.byte_size FROM botgram_backend_v1.media_files f
                WHERE f.id=$1 AND f.completed AND (EXISTS (
                    SELECT 1 FROM botgram_backend_v1.media_messages mm
                    JOIN botgram_intake_v1.members mem
                      ON mem.conversation_id=mm.conversation_id AND mem.user_id=$2
                    LEFT JOIN botgram_backend_v1.dialogs dlg
                      ON dlg.conversation_id=mm.conversation_id AND dlg.user_id=$2
                    WHERE mm.media_id=f.id
                      AND mm.message_id>COALESCE(dlg.cleared_through,0)
                      AND NOT EXISTS(SELECT 1 FROM botgram_backend_v1.message_deletions del
                                     WHERE del.message_id=mm.message_id)
                      AND NOT EXISTS(SELECT 1 FROM botgram_backend_v1.deleted_messages h
                                     WHERE h.message_id=mm.message_id AND h.user_id=$2))
                  -- An owner may read a completed upload that has never been published;
                  -- once any message references it, only a visible reference grants access.
                  OR (f.owner_id=$2 AND NOT EXISTS(
                      SELECT 1 FROM botgram_backend_v1.media_messages any_ref WHERE any_ref.media_id=f.id)))
                """, mediaId, session.UserId);
            if (total is not long byteSize) throw new ApiException(404, "MEDIA_NOT_FOUND");
            if (offset >= byteSize) throw new ApiException(400, "MEDIA_RANGE_INVALID");
            var end = Math.Min(byteSize, offset + limit);
            var parts = await work.Rows("""
                WITH sized AS (
                    SELECT part, bytes, octet_length(bytes) AS size,
                        sum(octet_length(bytes)) OVER (ORDER BY part) - octet_length(bytes) AS start
                    FROM botgram_backend_v1.media_parts WHERE media_id=$1)
                SELECT bytes, size, start FROM sized
                WHERE start < $2 AND start + size > $3
                ORDER BY part
                """, r => (Bytes: (byte[])r.GetValue(0), Size: r.GetInt64(1), Start: r.GetInt64(2)),
                mediaId, end, offset);
            var buffer = new byte[end - offset];
            foreach (var part in parts)
            {
                var from = Math.Max(offset, part.Start);
                var to = Math.Min(end, part.Start + part.Size);
                if (to <= from) continue;
                Buffer.BlockCopy(part.Bytes, (int)(from - part.Start), buffer, (int)(from - offset),
                    (int)(to - from));
            }
            return new MediaDownload(buffer, byteSize);
        }, token);
    }

    // ---------------------------------------------------------------- resolution
    // Resolves a client-supplied source to a completed media row. A freshly uploaded
    // client_file_id is completed here (part count and size are verified); an existing
    // media_id is reusable only when the caller owns it or shares a conversation with it.
    // A media stays readable/reusable only through a message reference the caller can still
    // see. An owner without any reference may still use a completed upload that was never
    // published; once references exist (even tombstoned ones) only a visible one counts.
    private static async Task<bool> MediaVisibleTo(Transaction work, string uid, long mediaId) =>
        await work.Scalar("""
            SELECT 1 FROM botgram_backend_v1.media_messages mm
            JOIN botgram_intake_v1.members mem
              ON mem.conversation_id=mm.conversation_id AND mem.user_id=$2
            LEFT JOIN botgram_backend_v1.dialogs dlg
              ON dlg.conversation_id=mm.conversation_id AND dlg.user_id=$2
            WHERE mm.media_id=$1
              AND mm.message_id>COALESCE(dlg.cleared_through,0)
              AND NOT EXISTS(SELECT 1 FROM botgram_backend_v1.message_deletions del
                             WHERE del.message_id=mm.message_id)
              AND NOT EXISTS(SELECT 1 FROM botgram_backend_v1.deleted_messages h
                             WHERE h.message_id=mm.message_id AND h.user_id=$2)
            LIMIT 1
            """, mediaId, uid) is not null;

    private static async Task<bool> MediaHasReferences(Transaction work, long mediaId) =>
        await work.Scalar("""
            SELECT 1 FROM botgram_backend_v1.media_messages WHERE media_id=$1 LIMIT 1
            """, mediaId) is not null;

    private static async Task<MessageMedia> ResolveMedia(Transaction work, string uid, P.MediaSource? input)
    {
        if (input is null) throw new ApiException(400, "MEDIA_INVALID");
        switch (input.OriginCase)
        {
            case P.MediaSource.OriginOneofCase.MediaId:
            {
                if (input.MediaId <= 0) throw new ApiException(400, "MEDIA_INVALID");
                // Reusing an existing media also requires a reference the caller can see.
                var row = await work.Rows("""
                    SELECT f.id,f.kind,f.mime,f.byte_size,f.width,f.height,f.duration,f.file_name,f.total_parts
                    FROM botgram_backend_v1.media_files f
                    WHERE f.id=$1 AND f.completed AND (EXISTS (
                        SELECT 1 FROM botgram_backend_v1.media_messages mm
                        JOIN botgram_intake_v1.members mem
                          ON mem.conversation_id=mm.conversation_id AND mem.user_id=$2
                        LEFT JOIN botgram_backend_v1.dialogs dlg
                          ON dlg.conversation_id=mm.conversation_id AND dlg.user_id=$2
                        WHERE mm.media_id=f.id
                          AND mm.message_id>COALESCE(dlg.cleared_through,0)
                          AND NOT EXISTS(SELECT 1 FROM botgram_backend_v1.message_deletions del
                                         WHERE del.message_id=mm.message_id)
                          AND NOT EXISTS(SELECT 1 FROM botgram_backend_v1.deleted_messages h
                                         WHERE h.message_id=mm.message_id AND h.user_id=$2))
                      OR (f.owner_id=$2 AND NOT EXISTS(
                          SELECT 1 FROM botgram_backend_v1.media_messages any_ref WHERE any_ref.media_id=f.id)))
                    """, MediaRow, input.MediaId, uid);
                if (row.Count == 0) throw new ApiException(404, "MEDIA_NOT_FOUND");
                return row[0];
            }
            case P.MediaSource.OriginOneofCase.ClientFileId:
            {
                ValidateMetadata(input);
                var rows = await work.Rows("""
                    SELECT f.id,f.completed,f.total_parts,
                        (SELECT count(*)::integer FROM botgram_backend_v1.media_parts p WHERE p.media_id=f.id),
                        (SELECT COALESCE(sum(octet_length(p.bytes)),0)::bigint
                           FROM botgram_backend_v1.media_parts p WHERE p.media_id=f.id)
                    FROM botgram_backend_v1.media_files f
                    WHERE f.owner_id=$1 AND f.client_file_id=$2
                    """, r => (Id: r.GetInt64(0), Completed: r.GetBoolean(1), Total: r.GetInt32(2),
                        Parts: r.GetInt32(3), Bytes: r.GetInt64(4)), uid, input.ClientFileId);
                if (rows.Count == 0) throw new ApiException(400, "MEDIA_UPLOAD_MISSING");
                var upload = rows[0];
                if (!upload.Completed)
                {
                    // Parts must be contiguous from zero. The total is either the count sent
                    // with the first part or the count the client now declares; both may be
                    // unknown (0) for a stream whose size was only known at the end.
                    var highest = (int)(await work.Scalar("""
                        SELECT COALESCE(max(part)+1,0) FROM botgram_backend_v1.media_parts WHERE media_id=$1
                        """, upload.Id))!;
                    if (upload.Parts < 1 || upload.Parts != highest)
                        throw new ApiException(409, "MEDIA_UPLOAD_INCOMPLETE");
                    if (input.TotalParts > 0 && upload.Total > 0 && input.TotalParts != upload.Total)
                        throw new ApiException(409, "MEDIA_UPLOAD_CONFLICT");
                    var expected = input.TotalParts > 0 ? input.TotalParts : upload.Total;
                    if (expected > 0 && expected != upload.Parts)
                        throw new ApiException(409, "MEDIA_UPLOAD_INCOMPLETE");
                    if (upload.Bytes is < 1 or > MediaPolicy.MaxFileBytes)
                        throw new ApiException(413, "MEDIA_FILE_TOO_LARGE");
                    if (input.ByteSize > 0 && input.ByteSize != upload.Bytes)
                        throw new ApiException(409, "MEDIA_SIZE_MISMATCH");
                    // The TL upload carries no dimensions for a photo; read them from the
                    // image header so received bubbles keep their aspect ratio.
                    var width = input.Width;
                    var height = input.Height;
                    if (input.Kind == P.MediaKind.Photo && (width <= 0 || height <= 0))
                    {
                        // The image header may straddle part boundaries, so read a bounded
                        // prefix across consecutive parts instead of only part zero.
                        var head = await MediaHead(work, upload.Id, 64 * 1024);
                        var size = MediaImage.Dimensions(head, input.Mime);
                        if (size.Width > 0 && size.Height > 0) { width = size.Width; height = size.Height; }
                    }
                    await work.Execute("""
                        UPDATE botgram_backend_v1.media_files
                        SET kind=$2,mime=$3,file_name=$4,byte_size=$5,width=$6,height=$7,duration=$8,
                            total_parts=$9,completed=true
                        WHERE id=$1
                        """, upload.Id, input.Kind == P.MediaKind.Photo ? "photo" : "document",
                        input.Mime, input.FileName, upload.Bytes, width, height,
                        input.Duration, upload.Parts);
                }
                // A completed upload that was already published follows the same rule as an
                // existing media: after every reference is deleted, neither form may reuse
                // it. A never-published upload stays usable by its owner.
                if (!upload.Completed && await MediaHasReferences(work, upload.Id))
                    throw new ApiException(409, "MEDIA_UPLOAD_CONFLICT");
                if (upload.Completed && await MediaHasReferences(work, upload.Id)
                    && !await MediaVisibleTo(work, uid, upload.Id))
                    throw new ApiException(404, "MEDIA_NOT_FOUND");
                // A retry of the same request reuses the completed row and its stored metadata.
                return (await work.Rows("""
                    SELECT f.id,f.kind,f.mime,f.byte_size,f.width,f.height,f.duration,f.file_name,f.total_parts
                    FROM botgram_backend_v1.media_files f WHERE f.id=$1
                    """, MediaRow, upload.Id)).Single();
            }
            default:
                throw new ApiException(400, "MEDIA_INVALID");
        }
    }

    private static MessageMedia MediaRow(Npgsql.NpgsqlDataReader r) => new(r.GetInt64(0), r.GetString(1),
        r.GetString(2), r.GetInt64(3), r.GetInt32(4), r.GetInt32(5), r.GetInt32(6), r.GetString(7), r.GetInt32(8));

    private static void ValidateMetadata(P.MediaSource input)
    {
        if (input.Kind is not (P.MediaKind.Photo or P.MediaKind.Document))
            throw new ApiException(400, "MEDIA_KIND_INVALID");
        if (input.Mime.Length is < 1 or > MediaPolicy.MaxMimeLength)
            throw new ApiException(400, "MEDIA_MIME_INVALID");
        if (input.FileName.Length > MediaPolicy.MaxFileNameLength)
            throw new ApiException(400, "MEDIA_NAME_INVALID");
        if (input.Width < 0 || input.Height < 0 || input.Duration < 0)
            throw new ApiException(400, "MEDIA_METADATA_INVALID");
    }

    internal static P.MediaRef ToRef(MessageMedia media) => new()
    {
        MediaId = media.MediaId,
        Kind = media.Kind == "photo" ? P.MediaKind.Photo : P.MediaKind.Document,
        Mime = media.Mime, ByteSize = media.ByteSize, Width = media.Width, Height = media.Height,
        Duration = media.Duration, FileName = media.FileName, TotalParts = media.TotalParts
    };

    private static async Task ShareMedia(Transaction work, long mediaId, string conversationId) =>
        await work.Execute("""
            INSERT INTO botgram_backend_v1.media_shares(media_id,conversation_id) VALUES($1,$2)
            ON CONFLICT DO NOTHING
            """, mediaId, conversationId);

    // Downloads and reuse resolve through this reference, so deleting the message revokes
    // the bytes without deleting the stored parts.
    private static async Task LinkMediaMessage(Transaction work, long mediaId, long messageId,
        string conversationId) =>
        await work.Execute("""
            INSERT INTO botgram_backend_v1.media_messages(media_id,message_id,conversation_id)
            VALUES($1,$2,$3) ON CONFLICT DO NOTHING
            """, mediaId, messageId, conversationId);

    // Bounded prefix of the stored media, assembled across consecutive parts.
    private static async Task<byte[]> MediaHead(Transaction work, long mediaId, int maxBytes)
    {
        using var buffer = new MemoryStream();
        for (var part = 0; buffer.Length < maxBytes; part++)
        {
            var bytes = await work.Scalar("""
                SELECT bytes FROM botgram_backend_v1.media_parts WHERE media_id=$1 AND part=$2
                """, mediaId, part) as byte[];
            if (bytes is null) break;
            var take = Math.Min(bytes.Length, maxBytes - (int)buffer.Length);
            if (take > 0) buffer.Write(bytes, 0, take);
        }
        return buffer.ToArray();
    }

    // Media id a request refers to, resolved without authorization checks so a committed
    // retry can still be recognised after the media was revoked for downloads.
    private static async Task<long?> RequestedMediaId(Transaction work, string uid, P.MediaSource? source)
    {
        if (source is null) return null;
        if (source.OriginCase == P.MediaSource.OriginOneofCase.MediaId) return source.MediaId;
        if (source.OriginCase == P.MediaSource.OriginOneofCase.ClientFileId)
            return await work.Scalar("""
                SELECT id FROM botgram_backend_v1.media_files WHERE owner_id=$1 AND client_file_id=$2
                """, uid, source.ClientFileId) as long?;
        return null;
    }

    // Committed reply for a media send retry; the stored message still carries the album
    // grouping a replay must reproduce.
    private sealed record CommittedMedia(P.Update Mapping, P.Update NewMessage, Message Stored);

    // A committed media send is answered before the current media authorization, so an
    // idempotent retry keeps working after the message was deleted; downloads stay revoked
    // because they are authorized separately. The retry must match the whole original
    // request — media, caption, entities, reply target and send options — because a
    // different request on the same random id is a conflict, never a silent replay.
    private static async Task<CommittedMedia?> CommittedMediaSend(Transaction work,
        string uid, string domain, string conversationId, long randomId, P.MediaSource? source,
        string caption, MessageEntity[]? entities, long? replyId, bool silent)
    {
        var rows = await CommittedClientKey(work, uid, "tl:" + randomId);
        var existing = rows.Where(row => row.ConversationId == conversationId)
            .Select(row => row.Message).FirstOrDefault();
        if (existing is null) return null;
        var requested = await RequestedMediaId(work, uid, source);
        // New messages carry the accepted request as an immutable snapshot, so a caption
        // edit cannot invalidate a retry. Records written before the snapshot existed fall
        // back to their current body, which is the best evidence they still have.
        var sameRequest = existing.MediaRequest is { } identity
            ? requested == identity.MediaId
              && identity.Caption == caption
              && identity.ReplyToMessageId == replyId
              && identity.Silent == silent
              && MessageEntityService.Equal(identity.Entities, entities)
            : existing.Media is { } media
              && requested == media.MediaId
              && existing.Text == caption
              && existing.ReplyToMessageId == replyId
              && existing.Silent == silent
              && MessageEntityService.Equal(existing.TelegramEntities, entities);
        if (!sameRequest) throw new ApiException(409, "RANDOM_ID_DUPLICATE");
        var originalPts = await CommittedMessagePts(work, existing.Id, uid);
        var mapped = (await MappedMessages(work, uid, domain, [existing])).Single();
        return new CommittedMedia(new P.Update
        {
            MessageId = new()
            {
                RandomId = randomId,
                MessageId = ProtobufRpcMapper.ValidateMessageId(existing.Id, true)
            }
        }, new P.Update
        {
            NewMessage = new() { Message = mapped, Pts = originalPts, PtsCount = 1 }
        }, existing);
    }

    // ---------------------------------------------------------------- RPC handlers
    private async Task<P.UploadMediaResult> UploadMedia(Transaction work, string uid, P.UploadMediaRequest input,
        string domain)
    {
        await ResolveConversation(work, uid, input.Peer, domain);
        var media = await ResolveMedia(work, uid, input.Media);
        return new() { Media = ToRef(media) };
    }

    private async Task<P.Updates> SendMedia(Transaction work, Session session, P.SendMediaRequest input, string domain)
    {
        var target = await ResolveConversation(work, session.UserId, input.Peer, domain, create: true);
        var caption = input.Caption ?? "";
        if (caption.Length > 16000) throw new ApiException(400, "MESSAGE_EMPTY");
        MessageEntityService.ValidateCount(input.Entities.Count);
        var entities = input.Entities.Select(entity => ProtobufRpcMapper.ParseEntity(entity, caption)).ToArray();
        var reply = input.HasReplyToMessageId ? input.ReplyToMessageId : (long?)null;
        var sentEntities = entities.Length == 0 ? null : entities;
        if (reply is { } replyId) ProtobufRpcMapper.ValidateMessageId(replyId, true);
        // A committed retry is answered before media authorization: deleting the message
        // revokes downloads but must not make the same request fail. The whole request is
        // part of the identity, so changed content or options are a conflict.
        if (await CommittedMediaSend(work, session.UserId, domain, target.Id, input.RandomId, input.Media,
                caption, sentEntities, reply, input.Silent) is { } committed)
            return await UpdatesEnvelope(work, session.UserId, domain, committed.Mapping, committed.NewMessage);
        var media = await ResolveMedia(work, session.UserId, input.Media);
        var command = new SendMessageCommand("tl:" + input.RandomId, caption, sentEntities, reply,
            input.Silent, Media: media);
        var accepted = await AcceptMedia(work, session, target, command, domain);
        await ShareMedia(work, media.MediaId, target.Id);
        await LinkMediaMessage(work, media.MediaId, accepted.Message.Id, target.Id);
        return await MediaUpdates(work, session.UserId, domain, accepted, input.RandomId);
    }

    // One prepared album item: its request identity for a retry and its values for a send.
    private sealed record AlbumItem(long RandomId, P.MediaSource? Media, string Caption,
        MessageEntity[]? Entities, long? Reply, bool Silent);

    private async Task<P.Updates> SendMultiMedia(Transaction work, Session session, P.SendMultiMediaRequest input,
        string domain)
    {
        ProtobufRpcMapper.ValidateCount(input.Items.Count);
        if (input.Items.Count is < 2 or > MediaPolicy.MaxAlbumItems)
            throw new ApiException(400, "MEDIA_ALBUM_SIZE_INVALID");
        // Duplicate ids would make two items claim one commit; reject before any lookup or
        // write so the request has no side effects.
        ProtobufRpcMapper.ValidateUnique(input.Items.Select(item => item.RandomId));
        var target = await ResolveConversation(work, session.UserId, input.Peer, domain, create: true);
        // The album is one request: prepare every item before accepting any of them.
        var items = new List<AlbumItem>();
        foreach (var item in input.Items)
        {
            var caption = item.Caption ?? "";
            if (caption.Length > 16000) throw new ApiException(400, "MESSAGE_EMPTY");
            MessageEntityService.ValidateCount(item.Entities.Count);
            var entities = item.Entities.Select(entity => ProtobufRpcMapper.ParseEntity(entity, caption)).ToArray();
            var reply = item.HasReplyToMessageId ? item.ReplyToMessageId : (long?)null;
            if (reply is { } replyId) ProtobufRpcMapper.ValidateMessageId(replyId, true);
            items.Add(new AlbumItem(item.RandomId, item.Media, caption,
                entities.Length == 0 ? null : entities, reply, item.Silent));
        }
        var committed = new List<CommittedMedia?>(items.Count);
        foreach (var item in items)
            committed.Add(await CommittedMediaSend(work, session.UserId, domain, target.Id, item.RandomId,
                item.Media, item.Caption, item.Entities, item.Reply, item.Silent));
        var replay = committed.Count(entry => entry is not null);
        if (replay > 0)
        {
            // A retry reproduces the committed album: every item is committed and all of
            // them share the original group. A request that mixes committed and new items,
            // or combines items committed elsewhere, would answer with two different groups,
            // so it is a conflict instead of a silently split album. Items committed by a
            // plain send carry no group and cannot be reinterpreted as album items.
            var matches = committed.Where(entry => entry is not null).Select(entry => entry!).ToArray();
            var groups = matches.Select(entry => entry.Stored.GroupedId).Distinct().ToArray();
            if (matches.Length != items.Count || groups.Length != 1 || groups[0] is null)
                throw new ApiException(409, "RANDOM_ID_DUPLICATE");
            var retried = new List<P.Update>();
            foreach (var entry in matches)
            {
                retried.Add(entry.Mapping);
                retried.Add(entry.NewMessage);
            }
            return await UpdatesEnvelope(work, session.UserId, domain, retried.ToArray());
        }
        // One album id per request; every item shares it so the original page groups them.
        var groupedId = Random.Shared.NextInt64(1, long.MaxValue);
        var updates = new List<P.Update>();
        foreach (var item in items)
        {
            var media = await ResolveMedia(work, session.UserId, item.Media);
            var command = new SendMessageCommand("tl:" + item.RandomId, item.Caption, item.Entities,
                item.Reply, item.Silent, Media: media, GroupedId: groupedId);
            var accepted = await AcceptMedia(work, session, target, command, domain);
            await ShareMedia(work, media.MediaId, target.Id);
            await LinkMediaMessage(work, media.MediaId, accepted.Message.Id, target.Id);
            var mapped = (await MappedMessages(work, session.UserId, domain, [accepted.Message])).Single();
            updates.Add(new P.Update
            {
                MessageId = new()
                {
                    RandomId = item.RandomId,
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

    // Paid/extended media is a product boundary: this self-hosted backend has no paid media,
    // so the request fails explicitly instead of fabricating a preview.
    private static P.Updates GetExtendedMedia(Transaction work, string uid, P.GetExtendedMediaRequest input,
        string domain) =>
        throw new ApiException(501, "BOTGRAM_EXTENDED_MEDIA_UNSUPPORTED");

    private async Task<AcceptedMessage> AcceptMedia(Transaction work, Session session, Conversation target,
        SendMessageCommand command, string domain) =>
        await new PostgresMessageAcceptance(source, maximumBacklog).AcceptInTransactionAsync(
            new(session.TokenHash, session.UserId, target.Id, command, MessageWriteContract.Telegram),
            work.Connection, work.NativeTransaction, work.RetainThroughCommit, work.Token,
            // Media is plaintext content: a chat that forbids plaintext must reject it too.
            () => ValidateSend(work, session, target, command, domain));

    private async Task<P.Updates> MediaUpdates(Transaction work, string uid, string domain,
        AcceptedMessage accepted, long randomId)
    {
        var mapped = (await MappedMessages(work, uid, domain, [accepted.Message])).Single();
        return await UpdatesEnvelope(work, uid, domain,
            new P.Update
            {
                MessageId = new()
                {
                    RandomId = randomId,
                    MessageId = ProtobufRpcMapper.ValidateMessageId(accepted.Message.Id, true)
                }
            },
            new P.Update { NewMessage = new() { Message = mapped, Pts = accepted.Pts, PtsCount = accepted.PtsCount } });
    }
}
