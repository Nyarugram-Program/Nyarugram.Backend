namespace Botgram.Backend;

// Presence is derived from authenticated RPC activity. A user is online while their last
// request is within OnlineWindow; state transitions are pushed to people who can see them.
public static class PresenceService
{
    public static readonly TimeSpan OnlineWindow = TimeSpan.FromSeconds(60);

    public static bool IsOnline(User user, DateTimeOffset now) => user.LastSeen is { } seen && now - seen <= OnlineWindow;

    public static void Touch(Database db, string uid)
    {
        int index = db.Users.FindIndex(u => u.Id == uid);
        if (index < 0) return;
        var user = db.Users[index];
        var now = DateTimeOffset.UtcNow;
        bool wasOnline = IsOnline(user, now);
        db.Users[index] = user with { LastSeen = now, Online = true };
        if (!wasOnline) Emit(db, user.Id);
    }

    public static void Expire(Database db)
    {
        var now = DateTimeOffset.UtcNow;
        foreach (var user in db.Users.Where(u => u.Online && !IsOnline(u, now)).ToArray())
        {
            int index = db.Users.IndexOf(user);
            db.Users[index] = user with { Online = false };
            Emit(db, user.Id);
        }
    }

    private static void Emit(Database db, string userId)
    {
        var recipients = db.Users.Where(u => u.Id != userId
            && (db.Conversations.Any(c => c.Members.Contains(userId) && c.Members.Contains(u.Id))
                || db.Contacts.Any(c => (c.OwnerId == u.Id && c.UserId == userId) || (c.OwnerId == userId && c.UserId == u.Id))))
            .Select(u => u.Id).ToArray();
        if (recipients.Length > 0) db.Emit(recipients, "user.presence_changed", new PresenceChanged(userId));
    }
}

public sealed record PresenceChanged(string UserId) : IEventPayload;
