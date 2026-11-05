using System.Data;
using Npgsql;

namespace Botgram.Backend.Storage;

public sealed partial class PostgresBackend(NpgsqlDataSource source, int maximumBacklog = 100_000,
    FourComponentReads? reads = null, AccountTotpService? totp = null)
{
    public async Task VerifySchemaAsync(CancellationToken token = default)
    {
        await using var connection = await source.OpenConnectionAsync(token);
        await using var command = new NpgsqlCommand("""
            SELECT ARRAY(SELECT version FROM botgram_backend_v1.schema_versions ORDER BY version),
                to_regclass('botgram_backend_v1.wallpaper_accounts')::text,
                to_regclass('botgram_backend_v1.privacy_accounts')::text
            """, connection);
        await using var reader = await command.ExecuteReaderAsync(token);
        if (!await reader.ReadAsync(token) || !reader.GetFieldValue<int[]>(0).SequenceEqual(new[] { 1, 2, 3, 4, 5, 6, 7, 8 }) || reader.IsDBNull(1) || reader.IsDBNull(2))
            throw new InvalidDataException("Relational backend requires the explicit offline schema migration through version 8");
    }

    public Task<Botgram.Protocol.V1.EventHint> EventHintAsync(Session session, long after, CancellationToken token = default)
    {
        if (after < 0) throw new ApiException(400, "invalid_cursor");
        return WriteAsync(session, async work =>
        {
            // Read the event cursor first. A concurrent wallpaper commit may cause an extra wake, never a skipped revision.
            var upper = (long)(await work.Scalar("SELECT value FROM botgram_intake_v1.event_cursor WHERE singleton"))!;
            const int limit = 100;
            var ids = await work.Rows("""
                SELECT e.id FROM botgram_intake_v1.events e
                JOIN botgram_intake_v1.event_recipients r ON r.event_id=e.id
                """ + EventPageFilter, r => r.GetInt64(0), session.UserId, after, upper, limit);
            var cursor = EventPageCursor(after, upper, limit, ids.Count, ids.Count == 0 ? 0 : ids[^1]);
            var wallpaper = await WallpaperAccount(work, session.UserId);
            return new Botgram.Protocol.V1.EventHint { ProtocolMajor = 1, Cursor = cursor,
                WallpaperRevision = wallpaper.Revision };
        }, token);
    }

    private sealed class Transaction(NpgsqlConnection connection, NpgsqlTransaction transaction, CancellationToken token) : IDisposable
    {
        private readonly Stack<IDisposable> retained = new();
        public NpgsqlConnection Connection => connection;
        public NpgsqlTransaction NativeTransaction => transaction;
        public CancellationToken Token => token;
        public void RetainThroughCommit(IDisposable stage) => retained.Push(stage);
        public void Dispose()
        {
            while (retained.TryPop(out var stage)) stage.Dispose();
        }

        public NpgsqlCommand Command(string sql, params object[] values)
        {
            var command = new NpgsqlCommand(sql, connection, transaction);
            foreach (var value in values) command.Parameters.Add(new NpgsqlParameter { Value = value });
            return command;
        }

        public async Task<object?> Scalar(string sql, params object[] values)
        {
            await using var command = Command(sql, values);
            return await command.ExecuteScalarAsync(token);
        }

        public async Task<int> Execute(string sql, params object[] values)
        {
            await using var command = Command(sql, values);
            return await command.ExecuteNonQueryAsync(token);
        }

        public async Task<List<T>> Rows<T>(string sql, Func<NpgsqlDataReader, T> read, params object[] values)
        {
            await using var command = Command(sql, values);
            await using var reader = await command.ExecuteReaderAsync(token);
            var rows = new List<T>();
            while (await reader.ReadAsync(token)) rows.Add(read(reader));
            return rows;
        }

        public async Task Authorize(Session session)
        {
            var owner = await Scalar("""
                SELECT user_id FROM botgram_intake_v1.sessions WHERE token_hash=$1 FOR UPDATE
                """, session.TokenHash);
            if (owner is not string uid || uid != session.UserId ||
                await Scalar("""
                    SELECT 1 FROM botgram_intake_v1.sessions WHERE token_hash=$1 AND expires_at>clock_timestamp()
                    """, session.TokenHash) is null)
                throw new ApiException(401, "session_expired");
        }
    }

    private async Task<T> WriteAsync<T>(Session? session, Func<Transaction, Task<T>> action, CancellationToken token)
    {
        await using var connection = await source.OpenConnectionAsync(token);
        await using var transaction = await connection.BeginTransactionAsync(IsolationLevel.ReadCommitted, token);
        using var work = new Transaction(connection, transaction, token);
        await work.Execute("SET LOCAL synchronous_commit=on");
        if (session is not null) await work.Authorize(session);
        var result = await action(work);
        token.ThrowIfCancellationRequested();
        await transaction.CommitAsync(token);
        return result;
    }

    private static User UserRow(NpgsqlDataReader reader) => new(reader.GetString(0), reader.GetString(1),
        reader.GetString(2), reader.GetString(3), reader.GetString(4), reader.GetString(5), reader.GetBoolean(6));

    private static async Task<SessionResult> AddSession(Transaction work, User user)
    {
        var session = Auth.NewSession(user);
        await work.Execute("""
            INSERT INTO botgram_intake_v1.sessions(token_hash,user_id,expires_at) VALUES($1,$2,$3)
            """, session.Stored.TokenHash, user.Id, session.Stored.ExpiresAt);
        return session.Result;
    }

    public async Task<SessionResult> RegisterAsync(RegisterInput input, CancellationToken token = default)
    {
        var user = Auth.PrepareRegistration(input);
        try
        {
            return await WriteAsync(null, async work =>
            {
                await work.Execute("INSERT INTO botgram_intake_v1.users(id) VALUES($1)", user.Id);
                for (var attempt = 0; attempt < 1024; attempt++)
                {
                    var candidate = user with { Gid = Gids.Candidate(Gids.FromCountry(input.LocationCountry)) };
                    var inserted = await work.Scalar("""
                        INSERT INTO botgram_backend_v1.accounts(id,username,display_name,salt,password_hash,gid)
                        VALUES($1,$2,$3,$4,$5,$6) ON CONFLICT(gid) DO NOTHING RETURNING id
                        """, candidate.Id, candidate.Username, candidate.DisplayName, candidate.Salt, candidate.PasswordHash, candidate.Gid);
                    if (inserted is null) continue;
                    await work.Execute("INSERT INTO botgram_intake_v1.sync(user_id) VALUES($1)", candidate.Id);
                    return await AddSession(work, candidate);
                }
                throw new ApiException(503, "gid_allocation_busy");
            }, token);
        }
        catch (PostgresException exception) when (exception.SqlState == PostgresErrorCodes.UniqueViolation &&
            exception.ConstraintName == "backend_accounts_username_key")
        {
            throw new ApiException(409, "username_taken");
        }
    }

    public Task<SessionResult> LoginAsync(LoginInput input, CancellationToken token = default)
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
            var state = await LoginPrivacy(work, user!.Id);
            AccountPasswordService.VerifyLogin(user, state.Password, input.PasswordProof);
            if (state.Totp.Secret.Length != 0)
            {
                if (totp is null) throw new ApiException(503, "TOTP_SERVICE_UNAVAILABLE");
                totp.Verify(user.Id, state.Totp, input.TotpCode);
                await SaveLoginPrivacy(work, user.Id, state);
            }
            return await AddSession(work, user!);
        }, token);
    }

    private static async Task<AccountPrivacyState> LoginPrivacy(Transaction work, string uid)
    {
        await work.Execute("INSERT INTO botgram_backend_v1.privacy_accounts(user_id) VALUES($1) ON CONFLICT DO NOTHING", uid);
        var json = (string)(await work.Scalar("SELECT state::text FROM botgram_backend_v1.privacy_accounts WHERE user_id=$1 FOR UPDATE", uid))!;
        return System.Text.Json.JsonSerializer.Deserialize<AccountPrivacyState>(json, Store.Json)
            ?? throw new InvalidDataException("Missing privacy state");
    }

    private static Task<int> SaveLoginPrivacy(Transaction work, string uid, AccountPrivacyState state) =>
        work.Execute("UPDATE botgram_backend_v1.privacy_accounts SET state=$2::jsonb WHERE user_id=$1", uid,
            System.Text.Json.JsonSerializer.Serialize(state, Store.Json));

    public Task<object> TotpAsync(Session session, string operation, string? code, CancellationToken token = default) =>
        WriteAsync<object>(session, async work =>
        {
            if (totp is null) throw new ApiException(503, "TOTP_SERVICE_UNAVAILABLE");
            var state = await LoginPrivacy(work, session.UserId);
            var username = (string)(await work.Scalar("SELECT username FROM botgram_backend_v1.accounts WHERE id=$1", session.UserId))!;
            var result = totp.Execute(session.UserId, username, state.Totp, operation, code);
            await SaveLoginPrivacy(work, session.UserId, state);
            return result;
        }, token);

    public Task<object> LoginPasswordAsync(LoginInput input, CancellationToken token = default)
    {
        Auth.ValidateLogin(input);
        return WriteAsync<object>(null, async work =>
        {
            var users = await work.Rows("""
                SELECT id,username,display_name,salt,password_hash,gid,premium FROM botgram_backend_v1.accounts
                WHERE username=$1 FOR SHARE
                """, UserRow, input.Username.ToLowerInvariant());
            var user = users.SingleOrDefault();
            Auth.VerifyPassword(user, input.Password);
            return AccountPasswordService.LoginInfo(user!, (await LoginPrivacy(work, user!.Id)).Password);
        }, token);
    }

    public async Task<Session> RequireAsync(string tokenHash, CancellationToken token = default)
    {
        await using var connection = await source.OpenConnectionAsync(token);
        await using var query = new NpgsqlCommand("""
            SELECT user_id,expires_at FROM botgram_intake_v1.sessions
            WHERE token_hash=$1 AND expires_at>clock_timestamp()
            """, connection);
        query.Parameters.AddWithValue(tokenHash);
        await using var reader = await query.ExecuteReaderAsync(token);
        if (!await reader.ReadAsync(token)) throw new ApiException(401, "session_expired");
        return new(tokenHash, reader.GetString(0), reader.GetFieldValue<DateTimeOffset>(1));
    }

    public Task<bool> LogoutAsync(Session session, CancellationToken token = default) =>
        WriteAsync(session, async work =>
            await work.Execute("DELETE FROM botgram_intake_v1.sessions WHERE token_hash=$1", session.TokenHash) == 1, token);

    public Task<PublicUserResult> MeAsync(Session session, CancellationToken token = default) =>
        WriteAsync(session, async work =>
        {
            var users = await work.Rows("""
                SELECT id,username,display_name,salt,password_hash,gid,premium FROM botgram_backend_v1.accounts WHERE id=$1
                """, UserRow, session.UserId);
            return Auth.PublicUser(users.Single());
        }, token);

    public Task<UserProfile> CurrentProfileAsync(Session session, CancellationToken token = default) =>
        WriteAsync(session, async work => (await Profiles(work, session.UserId, "a.id=$2", session.UserId)).Single(), token);

    public Task<PremiumStatus> PremiumAsync(Session session, bool activate = false, CancellationToken token = default) =>
        WriteAsync(session, async work => new PremiumStatus((bool)(await work.Scalar(activate
            ? "UPDATE botgram_backend_v1.accounts SET premium=true WHERE id=$1 RETURNING premium"
            : "SELECT premium FROM botgram_backend_v1.accounts WHERE id=$1", session.UserId))!), token);
}
