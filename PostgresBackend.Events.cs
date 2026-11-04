using System.Text.Json;
using Npgsql;

namespace Botgram.Backend.Storage;

public sealed partial class PostgresBackend
{
    private async Task<ChatEvent> Emit(Transaction work, string type, IEventPayload payload, string partition,
        string[] recipients, DateTimeOffset? expires = null)
    {
        if (type == "message.created" || EventPayloadTypes.For(type) != payload.GetType())
            throw new InvalidDataException("General event path cannot manufacture message acceptance");
        if (maximumBacklog < 1) throw new ArgumentOutOfRangeException(nameof(maximumBacklog));
        await work.Scalar(PostgresMessageAcceptance.CapacityLockSql);
        if ((long)(await work.Scalar(PostgresMessageAcceptance.CapacityCountSql))! >= maximumBacklog)
            throw new ApiException(503, "storage_backlog_full");
        recipients = recipients.Distinct(StringComparer.Ordinal).Order(StringComparer.Ordinal).ToArray();
        if (recipients.Length == 0) throw new InvalidDataException("Missing event recipients");
        var stream = type is "receipt.updated" or "user.gid_changed";
        int?[] positions;
        if (stream)
        {
            var updated = await work.Rows("""
                INSERT INTO botgram_intake_v1.sync(user_id,pts)
                SELECT recipient,2 FROM unnest($1::text[]) recipient ORDER BY recipient COLLATE "C"
                ON CONFLICT(user_id) DO UPDATE SET pts=botgram_intake_v1.sync.pts+1 RETURNING user_id,pts
                """, r => (User: r.GetString(0), Pts: r.GetInt32(1)), (object)recipients);
            var byUser = updated.ToDictionary(r => r.User, r => r.Pts, StringComparer.Ordinal);
            positions = recipients.Select(r => (int?)byUser[r]).ToArray();
        }
        else positions = new int?[recipients.Length];
        var eventId = (long)(await work.Scalar("""
            UPDATE botgram_intake_v1.event_cursor SET value=value+1 WHERE singleton RETURNING value
            """))!;
        var created = DateTimeOffset.UtcNow;
        var schema = type == "wallpaper.changed" ? StorageContractVersions.WallpaperEventSchema : StorageContractVersions.EventSchema;
        var delivery = new DeliveryEvent(Guid.NewGuid(), schema, eventId, partition, type, payload,
            recipients.Select((r, i) => new RecipientPosition(r, positions[i])).ToArray(),
            created, expires, null, null, null);
        DeliveryEventCodec.Validate(delivery);
        await work.Execute("""
            INSERT INTO botgram_intake_v1.events(id,type,created_at,schema_version,payload,expires_at)
            VALUES($1,$2,$3,$6,$4::jsonb,$5::timestamptz)
            """, eventId, type, created, JsonSerializer.Serialize(payload, payload.GetType()), (object?)expires ?? DBNull.Value, schema);
        await work.Execute("""
            INSERT INTO botgram_intake_v1.event_recipients(event_id,user_id,pts)
            SELECT $1,user_id,pts FROM unnest($2::text[],$3::integer[]) r(user_id,pts)
            """, eventId, recipients, positions);
        await work.Execute("""
            INSERT INTO botgram_intake_v1.outbox(id,event_id,partition_key,schema_version,projection_version)
            VALUES($1,$2,$3,$4,NULL)
            """, delivery.OutboxId, eventId, partition, schema);
        return new(eventId, recipients, type, payload, created, expires);
    }

    public Task<T> ChangeGidAsync<T>(Session session, ChangeGidInput input, Func<UserProfile, T> prepareResponse,
        CancellationToken token = default)
    {
        var candidate = Gids.ValidateChange(input);
        return Change();
        async Task<T> Change()
        {
            try
            {
                return await WriteAsync(session, async work =>
                {
                    // Serialize GID swaps before taking account rows; reciprocal UNIQUE waits cannot deadlock.
                    await work.Scalar("SELECT pg_advisory_xact_lock(2147000002,0)");
                    var previous = (string)(await work.Scalar("""
                        SELECT gid FROM botgram_backend_v1.accounts WHERE id=$1 FOR UPDATE
                        """, session.UserId))!;
                    if (candidate != previous)
                    {
                        await work.Execute("UPDATE botgram_backend_v1.accounts SET gid=$2 WHERE id=$1", session.UserId, candidate);
                        var recipients = await work.Rows("""
                            SELECT other.user_id FROM botgram_intake_v1.members own
                            JOIN botgram_intake_v1.members other ON other.conversation_id=own.conversation_id WHERE own.user_id=$1
                            UNION SELECT owner_id FROM botgram_backend_v1.contacts WHERE user_id=$1
                            UNION SELECT $1
                            """, r => r.GetString(0), session.UserId);
                        await Emit(work, "user.gid_changed", new GidChanged(session.UserId, candidate),
                            "user:" + session.UserId, recipients.ToArray());
                    }
                    return prepareResponse((await Profiles(work, session.UserId, "a.id=$2", session.UserId)).Single());
                }, token);
            }
            catch (PostgresException exception) when (exception.SqlState == PostgresErrorCodes.UniqueViolation &&
                exception.ConstraintName == "backend_accounts_gid_key")
            {
                throw new ApiException(409, "gid_taken");
            }
        }
    }

    private const string EventRows = """
        SELECT e.id,e.type,COALESCE(e.payload,m.body)::text,e.created_at,e.expires_at,
            ARRAY(SELECT recipient.user_id FROM botgram_intake_v1.event_recipients recipient
                WHERE recipient.event_id=e.id ORDER BY recipient.user_id COLLATE "C"),r.pts
        FROM botgram_intake_v1.events e
        JOIN botgram_intake_v1.event_recipients r ON r.event_id=e.id
        LEFT JOIN botgram_intake_v1.messages m ON m.id=e.message_id
        """;

    private const string EventPageFilter = """
         WHERE r.user_id=$1 AND e.id>$2 AND e.id<=$3
            AND (e.expires_at IS NULL OR e.expires_at>clock_timestamp()) ORDER BY e.id LIMIT $4
        """;

    private static long EventPageCursor(long after, long upper, int limit, int count, long lastId) =>
        count == limit ? lastId : Math.Max(after, upper);

    private static ChatEvent EventRow(NpgsqlDataReader reader) => new(reader.GetInt64(0),
        reader.GetFieldValue<string[]>(5), reader.GetString(1),
        (IEventPayload)JsonSerializer.Deserialize(reader.GetString(2), EventPayloadTypes.For(reader.GetString(1)))!,
        reader.GetFieldValue<DateTimeOffset>(3), reader.IsDBNull(4) ? null : reader.GetFieldValue<DateTimeOffset>(4));

    public Task<EventPage> EventsAsync(Session session, long after, int limit, CancellationToken token = default)
    {
        if (after < 0 || limit is < 1 or > 200) throw new ApiException(400, "invalid_pagination");
        return WriteAsync(session, async work =>
        {
            var upper = (long)(await work.Scalar("SELECT value FROM botgram_intake_v1.event_cursor WHERE singleton"))!;
            var rows = await work.Rows(EventRows + EventPageFilter, EventRow, session.UserId, after, upper, limit);
            return new EventPage(rows.Select(e => new EventView(e.Id, e.Type, e.Payload, e.CreatedAt)).ToArray(),
                EventPageCursor(after, upper, limit, rows.Count, rows.Count == 0 ? 0 : rows[^1].Id));
        }, token);
    }

    public Task<DifferenceResult> DifferenceAsync(Session session, int pts, CancellationToken token = default) =>
        WriteAsync(session, work => Difference(work, session.UserId, pts), token);

    private static async Task<DifferenceResult> Difference(Transaction work, string uid, int pts)
    {
        if (pts < 0) throw new ApiException(400, "BOTGRAM_RPC_INVALID_ARGUMENT");
        var after = Math.Max(0, pts - 1);
        var upper = (int)(await work.Scalar("SELECT pts FROM botgram_intake_v1.sync WHERE user_id=$1", uid))!;
        if (after > upper - 1) return new(DifferenceKind.TooLong, [], after, upper);
        var page = await work.Rows(EventRows + """
             WHERE r.user_id=$1 AND r.pts>$2 AND r.pts<=$3 ORDER BY r.pts LIMIT 100
            """, r => (Event: EventRow(r), Pts: r.GetInt32(6)), uid, Math.Max(1, pts), upper);
        if (page.Count == 0) return new(DifferenceKind.Empty, [], after, upper);
        if (!page.Select(e => e.Pts).SequenceEqual(Enumerable.Range(after + 2, page.Count)))
            throw new InvalidDataException("Noncontiguous authoritative pts stream");
        return new(page[^1].Pts < upper ? DifferenceKind.Slice : DifferenceKind.Complete,
            page.Select(e => e.Event).ToArray(), after, page[^1].Pts);
    }
}
