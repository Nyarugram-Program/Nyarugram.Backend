using System.Security.Cryptography;
using System.Text;
using System.Text.RegularExpressions;

namespace Botgram.Backend;

public sealed record PublicUserResult(string Id, string Username, string DisplayName, string Gid);
public sealed record SessionResult(string Token, DateTimeOffset ExpiresAt, PublicUserResult User);

public static class Auth
{
    public static string HashToken(string token) => Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(token)));
    public static string HashPassword(string password, string salt) => Convert.ToHexString(
        Rfc2898DeriveBytes.Pbkdf2(password, Convert.FromHexString(salt), 210_000, HashAlgorithmName.SHA256, 32));
    public static PublicUserResult PublicUser(User u) => new(u.Id, u.Username, u.DisplayName, Gids.Format(u.Gid));
    public static PublicUserResult PublicUser(UserProfile u) => new(u.Id, u.Username, u.DisplayName, Gids.Format(u.Gid));
    public static User PrepareRegistration(RegisterInput input)
    {
        if (input.Username is null || !Regex.IsMatch(input.Username, "^[a-zA-Z0-9_]{3,32}$"))
            throw new ApiException(400, "invalid_username");
        if (string.IsNullOrWhiteSpace(input.DisplayName) || input.DisplayName.Length > 80)
            throw new ApiException(400, "invalid_display_name");
        if (input.Password is not { Length: >= 10 and <= 256 })
            throw new ApiException(400, "password_length_10_to_256");
        var salt = Convert.ToHexString(RandomNumberGenerator.GetBytes(16));
        return new(Guid.NewGuid().ToString("N"), input.Username.ToLowerInvariant(), input.DisplayName.Trim(),
            salt, HashPassword(input.Password, salt));
    }

    public static void ValidateLogin(LoginInput input)
    {
        if (input.Username is not { Length: <= 32 } || input.Password is not { Length: <= 256 })
            throw new ApiException(400, "invalid_credentials");
    }

    public static void VerifyPassword(User? user, string password)
    {
        var hash = HashPassword(password, user?.Salt ?? new string('0', 32));
        if (user is null || !CryptographicOperations.FixedTimeEquals(
                Convert.FromHexString(hash), Convert.FromHexString(user.PasswordHash)))
            throw new ApiException(401, "invalid_credentials");
    }

    public static (Session Stored, SessionResult Result) NewSession(User user)
    {
        var token = Convert.ToHexString(RandomNumberGenerator.GetBytes(32));
        var expiresAt = DateTimeOffset.UtcNow.AddDays(30);
        return (new(HashToken(token), user.Id, expiresAt), new(token, expiresAt, PublicUser(user)));
    }

    public static SessionResult CreateSession(Database db, User user)
    {
        var session = NewSession(user);
        db.Sessions.Add(session.Stored);
        return session.Result;
    }

    public static void Recheck(Database db, Session session)
    {
        if (!db.Sessions.Any(s => s.TokenHash == session.TokenHash && s.UserId == session.UserId && s.ExpiresAt > DateTimeOffset.UtcNow))
            throw new ApiException(401, "session_expired");
    }
    public static Session Require(HttpContext context, Store store)
    {
        using var diagnostic = RpcDiagnosticStage.Start("auth_ms");
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
