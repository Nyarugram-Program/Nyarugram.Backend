using System.Security.Cryptography;
using System.Text;

namespace Botgram.Backend;

public static class Auth
{
    public static string HashToken(string token) => Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(token)));
    public static string HashPassword(string password, string salt) => Convert.ToHexString(
        Rfc2898DeriveBytes.Pbkdf2(password, Convert.FromHexString(salt), 210_000, HashAlgorithmName.SHA256, 32));
    public static object PublicUser(User u) => new { u.Id, u.Username, u.DisplayName, gid = Gids.Format(u.Gid) };
    public static object CreateSession(Database db, User user)
    {
        var token = Convert.ToHexString(RandomNumberGenerator.GetBytes(32));
        var expiresAt = DateTimeOffset.UtcNow.AddDays(30);
        db.Sessions.Add(new(HashToken(token), user.Id, expiresAt));
        return new { token, expiresAt, user = PublicUser(user) };
    }
    public static Session Require(HttpContext context, Store store)
    {
        var header = context.Request.Headers.Authorization.ToString();
        if (!header.StartsWith("Bearer ", StringComparison.OrdinalIgnoreCase)) throw new ApiException(401, "unauthorized");
        var hash = HashToken(header[7..]);
        return store.Read(db => db.Sessions.FirstOrDefault(s => s.TokenHash == hash && s.ExpiresAt > DateTimeOffset.UtcNow))
            ?? throw new ApiException(401, "session_expired");
    }
    public static Conversation Conversation(Database db, string id, string userId)
        => db.Conversations.FirstOrDefault(c => c.Id == id && c.Members.Contains(userId))
            ?? throw new ApiException(404, "conversation_not_found");
    public static Call Call(Database db, string id, string userId)
        => db.Calls.FirstOrDefault(c => c.Id == id && (c.CallerId == userId || c.CalleeId == userId))
            ?? throw new ApiException(404, "call_not_found");
}
