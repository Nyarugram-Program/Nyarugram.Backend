namespace Botgram.Backend.Storage;

public sealed partial class PostgresBackend
{
    private enum IdentityKind { User, Group }
    private static Task<Dictionary<string, long>> UserIdentities(Transaction work, string[] ids) =>
        Identities(work, IdentityKind.User, ids);

    private static async Task<Dictionary<string, long>> Identities(Transaction work, IdentityKind kind, string[] ids)
    {
        var (table, prefix) = kind switch
        {
            IdentityKind.User => ("accounts", "user:"),
            IdentityKind.Group => ("conversations", "chat:"),
            _ => throw new ArgumentOutOfRangeException(nameof(kind))
        };
        await work.Execute($"""
            INSERT INTO botgram_backend_v1.identities(key,value)
            SELECT $2||a.id,nextval('botgram_backend_v1.telegram_identity')
            FROM botgram_backend_v1.{table} a WHERE a.id=ANY($1)
              AND NOT EXISTS(SELECT 1 FROM botgram_backend_v1.identities i WHERE i.key=$2||a.id)
            ORDER BY a.id COLLATE "C" ON CONFLICT(key) DO NOTHING
            """, ids, prefix);
        var mappings = await work.Rows($"""
            SELECT a.id,i.value FROM botgram_backend_v1.{table} a
            JOIN botgram_backend_v1.identities i ON i.key=$2||a.id WHERE a.id=ANY($1)
            """, r => (Id: r.GetString(0), Value: r.GetInt64(1)), ids, prefix);
        return mappings.ToDictionary(m => m.Id, m => m.Value, StringComparer.Ordinal);
    }

    // Predicates are internal SQL constants, never client-supplied strings.
    private static Task<UserProfile[]> Profiles(Transaction work, string uid, string predicate, params object[] arguments) =>
        ProfilesOrdered(work, uid, predicate, "a.ordinal", arguments);

    private static async Task<UserProfile[]> ProfilesOrdered(Transaction work, string uid, string predicate, string order, object[] arguments)
    {
        var values = new object[] { uid }.Concat(arguments).ToArray();
        var rows = await work.Rows($"""
            SELECT a.id,a.username,a.display_name,a.salt,a.password_hash,a.gid,a.premium,
                c.first_name,c.last_name,
                EXISTS(SELECT 1 FROM botgram_backend_v1.contacts reverse WHERE reverse.owner_id=a.id AND reverse.user_id=$1), p.last_seen, p.online
            FROM botgram_backend_v1.accounts a
            LEFT JOIN botgram_backend_v1.contacts c ON c.owner_id=$1 AND c.user_id=a.id
            LEFT JOIN botgram_backend_v1.presence_accounts p ON p.user_id=a.id
            WHERE {predicate} ORDER BY {order}
            """, r => (User: UserRow(r) with { LastSeen = r.IsDBNull(10) ? null : r.GetFieldValue<DateTimeOffset>(10), Online = !r.IsDBNull(11) && r.GetBoolean(11) },
                Contact: r.IsDBNull(7) ? null : new Contact(uid, r.GetString(0), r.GetString(7), r.GetString(8)),
                Mutual: !r.IsDBNull(7) && r.GetBoolean(9)), values);
        var ids = await UserIdentities(work, rows.Select(r => r.User.Id).ToArray());
        var privacy = await PostgresPrivacyPolicy.Load(work.Connection, work.NativeTransaction, rows.Select(r => r.User.Id).ToArray(), uid, work.Token);
        return rows.Select(r =>
        {
            var profile = UserQueryService.Profile(r.User, uid, ids[r.User.Id], r.Contact, r.Mutual);
            if (!AccountPrivacyService.Allowed(privacy, r.User.Id, uid, Botgram.Protocol.V1.PrivacyKey.PhoneNumber))
                profile = profile with { Gid = "" };
            return AccountPrivacyService.Allowed(privacy, r.User.Id, uid, Botgram.Protocol.V1.PrivacyKey.StatusTimestamp)
                ? profile : profile with { LastSeen = null, Online = false };
        }).ToArray();
    }

    private static async Task<UserProfile[]> SelectedProfiles(Transaction work, string uid, string[] ids)
    {
        var existing = await work.Rows("SELECT id FROM botgram_backend_v1.accounts WHERE id=ANY($1)",
            r => r.GetString(0), (object)ids);
        if (existing.Count != ids.Length) throw new ApiException(404, "USER_ID_INVALID");
        // Legacy ByIds enumerates all users and assigns missing mappings, including for an empty selection.
        var all = await work.Rows("SELECT id FROM botgram_backend_v1.accounts", r => r.GetString(0));
        await UserIdentities(work, all.ToArray());
        return await Profiles(work, uid, "a.id=ANY($2)", (object)ids);
    }

    private static async Task<UserProfile> ResolveProfile(Transaction work, string uid, string predicate, string value, string error)
    {
        var profiles = await Profiles(work, uid, predicate, value);
        var profile = profiles.SingleOrDefault() ?? throw new ApiException(400, error);
        if (predicate == "a.gid=$2")
        {
            var privacy = await PostgresPrivacyPolicy.Load(work.Connection, work.NativeTransaction, [profile.Id], uid, work.Token);
            if (!AccountPrivacyService.Allowed(privacy, profile.Id, uid, Botgram.Protocol.V1.PrivacyKey.AddedByPhone)) throw new ApiException(400, error);
        }
        return profile;
    }

    private static async Task<ContactView[]> Contacts(Transaction work, string uid)
    {
        var users = await ProfilesOrdered(work, uid, "c.owner_id IS NOT NULL", "c.ordinal", []);
        return users.Select(user => new ContactView(user, user.IsMutual)).ToArray();
    }

    private static async Task<ImportContactsResult> ImportContacts(Transaction work, string uid, ImportContactCommand[] commands)
    {
        if (commands.Length is 0 or > 100) throw new ApiException(400, "CONTACTS_TOO_MUCH");
        var validated = commands.Select(ContactService.ValidateImport).ToArray();
        var users = await work.Rows("""
            SELECT id,gid FROM botgram_backend_v1.accounts WHERE gid=ANY($1) AND id<>$2
            ORDER BY id COLLATE "C" FOR SHARE
            """, r => (Id: r.GetString(0), Gid: r.GetString(1)), validated.Select(c => c.Gid).ToArray(), uid);
        var byGid = users.ToDictionary(u => u.Gid, u => u.Id, StringComparer.Ordinal);
        var privacy = await PostgresPrivacyPolicy.Load(work.Connection, work.NativeTransaction, users.Select(u => u.Id).ToArray(), uid, work.Token);
        foreach (var user in users.Where(u => !AccountPrivacyService.Allowed(privacy, u.Id, uid, Botgram.Protocol.V1.PrivacyKey.AddedByPhone)))
            byGid.Remove(user.Gid);
        await UserIdentities(work, users.Select(u => u.Id).ToArray());
        var known = validated.Where(c => byGid.ContainsKey(c.Gid)).ToArray();
        foreach (var command in known.OrderBy(c => byGid[c.Gid], StringComparer.Ordinal))
            await work.Execute("""
                INSERT INTO botgram_backend_v1.contacts(owner_id,user_id,first_name,last_name) VALUES($1,$2,$3,$4)
                ON CONFLICT(owner_id,user_id) DO UPDATE SET first_name=excluded.first_name,last_name=excluded.last_name
                """, uid, byGid[command.Gid], command.FirstName, command.LastName);
        foreach (var command in known)
            await work.Execute("""
                UPDATE botgram_backend_v1.contacts SET ordinal=DEFAULT WHERE owner_id=$1 AND user_id=$2
                """, uid, byGid[command.Gid]);
        var profiles = await Profiles(work, uid, "a.id=ANY($2)", (object)byGid.Values.ToArray());
        return new(known.Select(c => new ImportedContact(c.ClientId, byGid[c.Gid])).ToArray(), profiles);
    }

    private static Task<UserProfile[]> Related(Transaction work, string uid, bool contacts) =>
        Profiles(work, uid, """
            a.id=$1 OR EXISTS(SELECT 1 FROM botgram_intake_v1.members own
                JOIN botgram_intake_v1.members other ON other.conversation_id=own.conversation_id
                WHERE own.user_id=$1 AND other.user_id=a.id)
            """ + (contacts ? " OR c.owner_id IS NOT NULL" : ""));
}
