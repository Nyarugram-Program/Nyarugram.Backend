using P = Botgram.Protocol.V1;

namespace Botgram.Backend.Storage;

public sealed partial class PostgresBackend
{
    private async Task MaintainPresence(Transaction work, Session session, bool? foreground)
    {
        // Serialize session aggregation and transitions across backend instances.
        await work.Scalar("SELECT pg_advisory_xact_lock(2147000003,0)");
        var now = DateTimeOffset.UtcNow;
        var threshold = now - PresenceService.OnlineWindow;
        var expired = await work.Rows("""
            UPDATE botgram_backend_v1.presence_accounts p SET online=false,notified_at=$1
            WHERE p.online AND NOT EXISTS (
                SELECT 1 FROM botgram_backend_v1.presence_sessions h
                JOIN botgram_intake_v1.sessions s ON s.token_hash=h.token_hash
                WHERE s.user_id=p.user_id AND s.expires_at>$1 AND h.foreground_at>=$2)
            RETURNING p.user_id
            """, r => r.GetString(0), now, threshold);
        foreach (var uid in expired) await EmitPresence(work, uid);
        if (foreground is null) return;

        await work.Execute("""
            INSERT INTO botgram_backend_v1.presence_accounts(user_id) VALUES($1) ON CONFLICT DO NOTHING
            """, session.UserId);
        if (foreground.Value)
            await work.Execute("""
                INSERT INTO botgram_backend_v1.presence_sessions(token_hash,foreground_at) VALUES($1,$2)
                ON CONFLICT(token_hash) DO UPDATE SET foreground_at=excluded.foreground_at
                """, session.TokenHash, now);
        else
            await work.Execute("DELETE FROM botgram_backend_v1.presence_sessions WHERE token_hash=$1", session.TokenHash);
        bool online = (bool)(await work.Scalar("""
            SELECT EXISTS(SELECT 1 FROM botgram_backend_v1.presence_sessions h
                JOIN botgram_intake_v1.sessions s ON s.token_hash=h.token_hash
                WHERE s.user_id=$1 AND s.expires_at>$2 AND h.foreground_at>=$3)
            """, session.UserId, now, threshold))!;
        var previous = (await work.Rows("""
            SELECT online,notified_at FROM botgram_backend_v1.presence_accounts WHERE user_id=$1
            """, r => (Online: r.GetBoolean(0), Notified: r.IsDBNull(1) ? (DateTimeOffset?)null : r.GetFieldValue<DateTimeOffset>(1)),
            session.UserId)).Single();
        bool notify = online != previous.Online || online
            && (previous.Notified is not { } notified || now - notified >= PresenceService.RefreshWindow);
        await work.Execute("""
            UPDATE botgram_backend_v1.presence_accounts SET online=$2,
                last_seen=CASE WHEN $3 THEN $4 ELSE last_seen END,
                notified_at=CASE WHEN $5 THEN $4 ELSE notified_at END WHERE user_id=$1
            """, session.UserId, online, foreground.Value, now, notify);
        if (notify) await EmitPresence(work, session.UserId);
    }

    private async Task EmitPresence(Transaction work, string uid)
    {
        var related = await work.Rows("""
            SELECT other.user_id FROM botgram_intake_v1.members own
            JOIN botgram_intake_v1.members other ON other.conversation_id=own.conversation_id WHERE own.user_id=$1
            UNION SELECT owner_id FROM botgram_backend_v1.contacts WHERE user_id=$1
            UNION SELECT user_id FROM botgram_backend_v1.contacts WHERE owner_id=$1
            """, r => r.GetString(0), uid);
        var recipients = new List<string>();
        foreach (var actor in related.Where(id => id != uid))
        {
            var privacy = await PostgresPrivacyPolicy.Load(work.Connection, work.NativeTransaction, [uid], actor, work.Token);
            if (AccountPrivacyService.Allowed(privacy, uid, actor, P.PrivacyKey.StatusTimestamp)) recipients.Add(actor);
        }
        if (recipients.Count != 0)
            await Emit(work, "user.presence_changed", new PresenceChanged(uid), "user:" + uid, recipients.ToArray());
    }
}
