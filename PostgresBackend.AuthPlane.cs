namespace Botgram.Backend.Storage;

public sealed partial class PostgresBackend
{
    /// <summary>
    /// Protobuf authentication-plane variant of LoginPasswordAsync: returns the
    /// serialized PasswordInfo instead of an anonymous JSON DTO. The SRP state,
    /// verifier and challenge creation are identical.
    /// </summary>
    public Task<byte[]> LoginPasswordBytesAsync(LoginInput input, CancellationToken token = default)
    {
        Auth.ValidateLogin(input);
        return WriteAsync(null, async work =>
        {
            var users = await work.Rows("""
                SELECT id,username,display_name,salt,password_hash,gid,premium FROM botgram_backend_v1.accounts
                WHERE username=$1 FOR SHARE
                """, UserRow, input.Username.ToLowerInvariant());
            var user = users.SingleOrDefault();
            Auth.VerifyPassword(user, input.Password);
            return AccountPasswordService.LoginInfoBytes(user!, (await LoginPrivacy(work, user!.Id)).Password);
        }, token);
    }
}
