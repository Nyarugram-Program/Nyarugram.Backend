namespace Botgram.Backend;

public static class PresenceService
{
    public static readonly TimeSpan OnlineWindow = TimeSpan.FromSeconds(60);
    public static readonly TimeSpan RefreshWindow = TimeSpan.FromSeconds(30);

    public static bool IsOnline(User user, DateTimeOffset now) => user.Online
        && user.LastSeen is { } seen && now - seen <= OnlineWindow;

    // Background difference polling must not extend a session's online window.
    public static void Report(Database db, Session session, bool foreground)
    {
        int index = db.Sessions.FindIndex(s => s.TokenHash == session.TokenHash && s.UserId == session.UserId);
        if (index < 0) throw new ApiException(401, "session_expired");
        var now = DateTimeOffset.UtcNow;
        db.Sessions[index] = db.Sessions[index] with { ForegroundAt = foreground ? now : null };
        int userIndex = db.Users.FindIndex(u => u.Id == session.UserId);
        var user = db.Users[userIndex];
        bool online = db.Sessions.Any(s => s.UserId == session.UserId && s.ExpiresAt > now
            && s.ForegroundAt is { } seen && now - seen <= OnlineWindow);
        bool notify = online != user.Online || online
            && (user.PresenceNotifiedAt is not { } notified || now - notified >= RefreshWindow);
        db.Users[userIndex] = user with
        {
            LastSeen = foreground ? now : user.LastSeen,
            Online = online,
            PresenceNotifiedAt = notify ? now : user.PresenceNotifiedAt
        };
        if (notify) Emit(db, session.UserId);
    }

    public static void Expire(Database db)
    {
        var now = DateTimeOffset.UtcNow;
        foreach (var user in db.Users.Where(u => u.Online).ToArray())
        {
            bool online = db.Sessions.Any(s => s.UserId == user.Id && s.ExpiresAt > now
                && s.ForegroundAt is { } seen && now - seen <= OnlineWindow);
            // Existing snapshots have no per-session reports. Let their old window expire.
            if (online || user.PresenceNotifiedAt is null && IsOnline(user, now)) continue;
            int index = db.Users.IndexOf(user);
            db.Users[index] = user with { Online = false, PresenceNotifiedAt = now };
            Emit(db, user.Id);
        }
    }

    private static void Emit(Database db, string userId)
    {
        var recipients = db.Users.Where(u => u.Id != userId
            && AccountPrivacyService.Allowed(db, userId, u.Id, Botgram.Protocol.V1.PrivacyKey.StatusTimestamp)
            && (db.Conversations.Any(c => c.Members.Contains(userId) && c.Members.Contains(u.Id))
                || db.Contacts.Any(c => (c.OwnerId == u.Id && c.UserId == userId) || (c.OwnerId == userId && c.UserId == u.Id))))
            .Select(u => u.Id).ToArray();
        if (recipients.Length > 0) db.Emit(recipients, "user.presence_changed", new PresenceChanged(userId));
    }
}

public sealed record PresenceChanged(string UserId) : IEventPayload;
