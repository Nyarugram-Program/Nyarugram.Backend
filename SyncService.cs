namespace Botgram.Backend;

public enum DifferenceKind { Empty, Complete, Slice, TooLong }
public sealed record DifferenceResult(DifferenceKind Kind, ChatEvent[] Events, int PreviousCount, int Pts);
public sealed record EventView(long Id, string Type, IEventPayload Payload, DateTimeOffset CreatedAt);
public sealed record EventPage(EventView[] Events, long Cursor);

public static class SyncService
{
    public static EventPage Events(Database db, string uid, long after, int limit)
    {
        var events = db.Events.Where(e => e.Id > after && e.Recipients.Contains(uid) &&
            (e.ExpiresAt is null || e.ExpiresAt > DateTimeOffset.UtcNow)).Take(limit).ToArray();
        // Invisible/expired events advance the cursor only after the visible page is exhausted.
        long cursor = events.Length == limit ? events[^1].Id : Math.Max(after, db.NextEventId - 1);
        return new(events.Select(e => new EventView(e.Id, e.Type, e.Payload, e.CreatedAt)).ToArray(), cursor);
    }

    public static IEnumerable<ChatEvent> Stream(Database db, string uid) => db.Events.Where(e => e.Recipients.Contains(uid)
        && e.Type is "message.created" or "receipt.updated" or "user.gid_changed" or "user.presence_changed" or "call.updated" or "call.tl_signal");
    public static int Pts(Database db, string uid)
    {
        using var diagnostic = RpcDiagnosticStage.Start("pts_service_ms");
        return 1 + Stream(db, uid).Count();
    }
    public static DifferenceResult Difference(Database db, string uid, int pts)
    {
        int after = Math.Max(0, pts - 1);
        var stream = Stream(db, uid).ToArray();
        if (after > stream.Length) return new(DifferenceKind.TooLong, [], after, stream.Length + 1);
        var page = stream.Skip(after).Take(100).ToArray();
        if (page.Length == 0) return new(DifferenceKind.Empty, [], after, stream.Length + 1);
        return new(after + page.Length < stream.Length ? DifferenceKind.Slice : DifferenceKind.Complete,
            page, after, after + page.Length + 1);
    }
}
