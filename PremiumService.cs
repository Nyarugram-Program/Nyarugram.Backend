namespace Botgram.Backend;

public sealed record PremiumStatus(bool Active);

public static class PremiumService
{
    public static PremiumStatus Get(Database db, Session session)
    {
        Auth.Recheck(db, session);
        return new(db.Users.Single(u => u.Id == session.UserId).Premium);
    }

    public static PremiumStatus Activate(Database db, Session session)
    {
        Auth.Recheck(db, session);
        var index = db.Users.FindIndex(u => u.Id == session.UserId);
        db.Users[index] = db.Users[index] with { Premium = true };
        return new(true);
    }
}
