namespace Botgram.Backend;

public sealed record UserProfile(string Id, string Username, string DisplayName, string Gid, long TelegramId,
    string FirstName, string LastName, bool IsSelf, bool IsContact, bool IsMutual, bool Premium = false,
    DateTimeOffset? LastSeen = null);

public static class UserQueryService
{
    public static UserProfile Profile(Database db, User user, string uid)
    {
        var contact = db.Contacts.FirstOrDefault(c => c.OwnerId == uid && c.UserId == user.Id);
        bool mutual = contact is not null && db.Contacts.Any(c => c.OwnerId == user.Id && c.UserId == uid);
        var profile = Profile(user, uid, IdentityService.UserId(db, user.Id), contact, mutual);
        return AccountPrivacyService.Allowed(db, user.Id, uid, Botgram.Protocol.V1.PrivacyKey.PhoneNumber)
            ? profile : profile with { Gid = "" };
    }

    public static UserProfile Profile(User user, string uid, long telegramId, Contact? contact, bool mutual)
        => new(user.Id, user.Username, user.DisplayName, user.Gid, telegramId,
            contact?.FirstName ?? user.DisplayName, contact?.LastName ?? "", user.Id == uid, contact is not null, mutual, user.Premium,
            user.LastSeen);

    public static UserProfile Current(Database db, string uid) => Profile(db, db.Users.Single(u => u.Id == uid), uid);

    public static UserProfile Find(Database db, long id, string uid) => Profile(db, IdentityService.FindUser(db, id), uid);

    public static UserProfile[] ByIds(Database db, long[] ids, string uid) => db.Users
        .Where(u => ids.Contains(IdentityService.UserId(db, u.Id))).Select(u => Profile(db, u, uid)).ToArray();

    public static UserProfile[] Related(Database db, string uid, bool includeContacts) => db.Users
        .Where(u => u.Id == uid || db.Conversations.Any(c => c.Members.Contains(uid) && c.Members.Contains(u.Id)) ||
            includeContacts && db.Contacts.Any(c => c.OwnerId == uid && c.UserId == u.Id))
        .Select(u => Profile(db, u, uid)).ToArray();

    public static UserProfile[] Participants(Database db, Conversation conversation, string uid) => db.Users
        .Where(u => conversation.Members.Contains(u.Id)).Select(u => Profile(db, u, uid)).ToArray();
}
