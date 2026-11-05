namespace Botgram.Backend;

public sealed record ImportContactCommand(long ClientId, string Gid, string FirstName, string LastName, bool HasNotes);
public sealed record ImportedContact(long ClientId, string UserId);
public sealed record ImportContactsResult(ImportedContact[] Imported, UserProfile[] Users);
public sealed record ContactView(UserProfile User, bool Mutual);

public static class ContactService
{
    public static UserProfile[] Search(Database db, string query, string uid) => db.Users
        .Where(u => u.Username.Equals(query.TrimStart('@'), StringComparison.OrdinalIgnoreCase))
        .Select(u => UserQueryService.Profile(db, u, uid)).ToArray();

    public static UserProfile ResolveUsername(Database db, string username, string uid)
    {
        var user = db.Users.FirstOrDefault(u => u.Username.Equals(username, StringComparison.OrdinalIgnoreCase))
            ?? throw new ApiException(400, "USERNAME_NOT_OCCUPIED");
        return UserQueryService.Profile(db, user, uid);
    }

    public static ContactView[] List(Database db, string uid) => db.Contacts.Where(c => c.OwnerId == uid)
        .Select(c => new ContactView(UserQueryService.Profile(db, db.Users.Single(u => u.Id == c.UserId), uid),
            db.Contacts.Any(other => other.OwnerId == c.UserId && other.UserId == uid))).ToArray();

    public static string NormalizeGid(string input)
    {
        string gid = input.Trim().Replace(" ", "");
        if (gid.StartsWith('+')) gid = gid[1..];
        if (!Gids.Valid(gid)) throw new ApiException(400, "PHONE_NUMBER_INVALID");
        return gid;
    }

    public static UserProfile ResolveGid(Database db, string input, string uid)
    {
        string gid = NormalizeGid(input);
        var user = db.Users.FirstOrDefault(u => u.Gid == gid) ?? throw new ApiException(400, "PHONE_NOT_OCCUPIED");
        if (!AccountPrivacyService.Allowed(db, user.Id, uid, Botgram.Protocol.V1.PrivacyKey.AddedByPhone))
            throw new ApiException(400, "PHONE_NOT_OCCUPIED");
        return UserQueryService.Profile(db, user, uid);
    }

    public static ImportContactsResult Import(Database db, string uid, int count, IEnumerable<ImportContactCommand> commands)
    {
        if (count is 0 or > 100) throw new ApiException(400, "CONTACTS_TOO_MUCH");
        var imported = new List<ImportedContact>();
        var found = new HashSet<string>();
        foreach (var command in commands)
        {
            var validated = ValidateImport(command);
            var user = db.Users.FirstOrDefault(u => u.Gid == validated.Gid);
            if (user is null || user.Id == uid) continue;
            if (!AccountPrivacyService.Allowed(db, user.Id, uid, Botgram.Protocol.V1.PrivacyKey.AddedByPhone)) continue;
            db.Contacts.RemoveAll(c => c.OwnerId == uid && c.UserId == user.Id);
            db.Contacts.Add(new(uid, user.Id, validated.FirstName, validated.LastName));
            imported.Add(new(command.ClientId, user.Id));
            found.Add(user.Id);
        }
        return new(imported.ToArray(), db.Users.Where(u => found.Contains(u.Id))
            .Select(u => UserQueryService.Profile(db, u, uid)).ToArray());
    }

    public static ImportContactCommand ValidateImport(ImportContactCommand command)
    {
        string gid = NormalizeGid(command.Gid);
        string first = command.FirstName.Trim(), last = command.LastName.Trim();
        if (first.Length == 0 || first.Length > 64 || last.Length > 64)
            throw new ApiException(400, "FIRSTNAME_INVALID");
        if (command.HasNotes) throw new ApiException(501, "BOTGRAM_CONTACT_NOTES_UNSUPPORTED");
        return command with { Gid = gid, FirstName = first, LastName = last };
    }

    public static (string First, string Last) Names(string displayName, string firstName, string lastName, bool hasNote)
    {
        if (hasNote) throw new ApiException(501, "BOTGRAM_CONTACT_NOTES_UNSUPPORTED");
        string first = firstName.Trim(), last = lastName.Trim();
        if (first.Length == 0) first = displayName;
        if (first.Length == 0 || first.Length > 64 || last.Length > 64)
            throw new ApiException(400, "FIRSTNAME_INVALID");
        return (first, last);
    }

    public static UserProfile Add(Database db, string uid, long telegramId, string firstName, string lastName, bool hasNote)
    {
        var user = IdentityService.FindUser(db, telegramId);
        if (user.Id == uid) throw new ApiException(400, "CONTACT_ADD_SELF");
        var (first, last) = Names(user.DisplayName, firstName, lastName, hasNote);
        db.Contacts.RemoveAll(c => c.OwnerId == uid && c.UserId == user.Id);
        db.Contacts.Add(new(uid, user.Id, first, last));
        return UserQueryService.Profile(db, user, uid);
    }
}
