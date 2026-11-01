using System.Security.Cryptography;
using System.Text.Json;
using System.Text.RegularExpressions;

namespace Botgram.Backend;

public static class Gids
{
    public sealed record Region(string Code, string Name, string NameZh, string Abbreviation);
    private sealed record Geography(Region[] Regions, Dictionary<string, string> Countries);
    private static readonly Geography Data = JsonSerializer.Deserialize<Geography>(
        typeof(Gids).Assembly.GetManifestResourceStream("Botgram.M49")!, Store.Json)!;
    public static IReadOnlyList<Region> Regions => Data.Regions;
    public static string FromCountry(string? country) => country is not null
        && Data.Countries.TryGetValue(country.Trim().ToUpperInvariant(), out var region) ? region : "001";
    public static string Format(string gid) => gid.Length == 11 ? $"+{gid[..3]} {gid[3..]}" : "";
    public static bool Valid(string gid) => Regex.IsMatch(gid, "^[0-9]{11}$")
        && Data.Regions.Any(r => r.Code == gid[..3]) && gid[3..] != "00000000";

    // Caller holds Store's write lock: allocation and persistence are one transaction.
    public static string Allocate(Database db, string region)
    {
        for (var attempt = 0; attempt < 1024; attempt++)
        {
            var candidate = region + RandomNumberGenerator.GetInt32(1, 100_000_000).ToString("D8");
            if (db.Users.All(u => u.Gid != candidate)) return candidate;
        }
        throw new ApiException(503, "gid_allocation_busy");
    }

    public static void Migrate(Database db)
    {
        for (var i = 0; i < db.Users.Count; i++)
        {
            if (string.IsNullOrEmpty(db.Users[i].Gid))
            {
                db.Users[i] = db.Users[i] with { Gid = Allocate(db, "001") };
                var user = db.Users[i];
                var recipients = db.Conversations.Where(c => c.Members.Contains(user.Id)).SelectMany(c => c.Members).Append(user.Id).Distinct().ToArray();
                db.Emit(recipients, "user.gid_changed", new { userId = user.Id, gid = user.Gid });
            }
            else if (!Valid(db.Users[i].Gid)) throw new InvalidDataException("Invalid stored GID");
        }
        if (db.Users.Select(u => u.Gid).Distinct().Count() != db.Users.Count)
            throw new InvalidDataException("Duplicate stored GID");
    }

    public static User Change(Database db, string uid, ChangeGidInput input)
    {
        var candidate = (input.RegionCode ?? "") + (input.Number ?? "");
        if (input.RegionCode is not { Length: 3 } || input.Number is not { Length: 8 }
            || !Valid(candidate) || input.RegionCode == "001") throw new ApiException(400, "invalid_gid");
        if (db.Users.Any(u => u.Id != uid && u.Gid == candidate)) throw new ApiException(409, "gid_taken");
        var index = db.Users.FindIndex(u => u.Id == uid);
        var updated = db.Users[index] with { Gid = candidate };
        if (updated.Gid == db.Users[index].Gid) return updated;
        db.Users[index] = updated;
        var recipients = db.Conversations.Where(c => c.Members.Contains(uid)).SelectMany(c => c.Members)
            .Concat(db.Contacts.Where(c => c.UserId == uid).Select(c => c.OwnerId)).Append(uid).Distinct().ToArray();
        db.Emit(recipients, "user.gid_changed", new { userId = uid, gid = candidate });
        return updated;
    }
}
