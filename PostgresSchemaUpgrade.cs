using System.Text.RegularExpressions;
using Npgsql;

namespace Botgram.Backend.Storage;

public sealed record SchemaUpgradeReport(
    string Host, string Database, string User, string ServerVersion,
    int[] VersionsBefore, int[] VersionsAfter, bool MigrationRequired, bool Applied, long OtherClientBackends);

// Explicit offline maintenance for one already-provisioned isolated database. It never starts the
// service, never falls back to the legacy JSON store and never prints the connection string or
// credentials. Only a loopback botgram_test_* target is accepted.
public static class PostgresSchemaUpgrade
{
    public const int TargetVersion = PostgresBackendSchema.CurrentVersion;
    public const string ApplicationName = "botgram-schema-upgrade";
    private static readonly int[] RequiredVersions = Enumerable.Range(1, TargetVersion).ToArray();

    private static void ValidateTarget(NpgsqlDataSource source)
    {
        var settings = new NpgsqlConnectionStringBuilder(source.ConnectionString);
        if (!FourComponentOptions.Loopback(settings.Host) || settings.Database is not { } database ||
            !Regex.IsMatch(database, "\\Abotgram_test_[a-z0-9_]+\\z", RegexOptions.CultureInvariant))
            throw new InvalidOperationException("Schema maintenance requires a loopback botgram_test_* database");
    }

    public static async Task<SchemaUpgradeReport> InspectAsync(NpgsqlDataSource source, CancellationToken token = default)
    {
        ValidateTarget(source);
        await using var connection = await source.OpenConnectionAsync(token);
        await SetApplicationName(connection, token);
        var identity = await Identity(connection, null, token);
        var versions = await Versions(connection, null, token);
        if (versions.Length == 0)
            throw new InvalidOperationException("The target database has no botgram_backend_v1.schema_versions table");
        var supported = Supported(versions) || versions.SequenceEqual(RequiredVersions);
        if (!supported)
            throw new InvalidOperationException("Unsupported schema version set: " + string.Join(',', versions)
                + "; supported start versions are 1..7 and the target is " + TargetVersion);
        return identity with
        {
            VersionsBefore = versions, VersionsAfter = versions,
            MigrationRequired = !versions.SequenceEqual(RequiredVersions), Applied = false
        };
    }

    public static async Task<SchemaUpgradeReport> ApplyAsync(NpgsqlDataSource source, CancellationToken token = default)
    {
        ValidateTarget(source);
        await using var connection = await source.OpenConnectionAsync(token);
        await SetApplicationName(connection, token);
        await using var transaction = await connection.BeginTransactionAsync(token);
        // The maintenance gate is acquired first so two upgrade processes never interleave.
        if (!(bool)(await Scalar(connection, transaction,
            "SELECT pg_try_advisory_xact_lock(7459241304233)", token))!)
            throw new InvalidOperationException("Another maintenance process is already upgrading the database");
        // Then verify the runtime lease, capacity key and schema key are free.
        if (!(bool)(await Scalar(connection, transaction, """
            SELECT pg_try_advisory_xact_lock(7459241304238)
               AND pg_try_advisory_xact_lock(7459241304231)
               AND pg_try_advisory_xact_lock(7459241304232)
            """, token))!)
            throw new InvalidOperationException("A backend, four-component worker or maintenance process holds the database lease");
        long others = (long)(await Scalar(connection, transaction, """
            SELECT count(*) FROM pg_stat_activity
            WHERE datname=current_database() AND pid<>pg_backend_pid()
                AND backend_type='client backend' AND application_name <> $1
            """, token, ApplicationName))!;
        if (others != 0)
            throw new InvalidOperationException("Stop the backend and all readers/writers before the offline schema upgrade");
        var identity = await Identity(connection, transaction, token);
        var before = await Versions(connection, transaction, token);
        if (before.Length == 0)
            throw new InvalidOperationException("The target database has no botgram_backend_v1.schema_versions table");
        if (before.SequenceEqual(RequiredVersions))
        {
            await transaction.CommitAsync(token);
            await new PostgresBackend(source).VerifySchemaAsync(token);
            return identity with
            {
                VersionsBefore = before, VersionsAfter = before, MigrationRequired = false, Applied = false
            };
        }
        if (!Supported(before))
            throw new InvalidOperationException("Unsupported schema version set: " + string.Join(',', before)
                + "; supported start versions are 1..7 and the target is " + TargetVersion);
        await PostgresBackendSchema.ApplyInTransactionAsync(connection, transaction, token);
        var after = await Versions(connection, transaction, token);
        if (!after.SequenceEqual(RequiredVersions))
            throw new InvalidDataException("Schema upgrade did not reach the exact target version set");
        await transaction.CommitAsync(token);
        await new PostgresBackend(source).VerifySchemaAsync(token);
        return identity with
        {
            VersionsBefore = before, VersionsAfter = after, MigrationRequired = true, Applied = true
        };
    }

    private static bool Supported(int[] versions) => versions.SequenceEqual(Enumerable.Range(1, 7).ToArray());

    private static async Task SetApplicationName(NpgsqlConnection connection, CancellationToken token)
    {
        await using var command = new NpgsqlCommand("SELECT set_config('application_name',$1,false)", connection);
        command.Parameters.AddWithValue(ApplicationName);
        await command.ExecuteNonQueryAsync(token);
    }

    private static async Task<SchemaUpgradeReport> Identity(NpgsqlConnection connection,
        NpgsqlTransaction? transaction, CancellationToken token)
    {
        await using var command = new NpgsqlCommand("""
            SELECT current_database(), current_user, version(),
                (SELECT count(*) FROM pg_stat_activity
                 WHERE datname=current_database() AND pid<>pg_backend_pid()
                     AND backend_type='client backend' AND application_name <> $1)
            """, connection, transaction);
        command.Parameters.AddWithValue(ApplicationName);
        await using var reader = await command.ExecuteReaderAsync(token);
        if (!await reader.ReadAsync(token)) throw new InvalidDataException("The target database did not answer the identity query");
        return new SchemaUpgradeReport(connection.Host ?? "", reader.GetString(0), reader.GetString(1), reader.GetString(2),
            [], [], false, false, reader.GetInt64(3));
    }

    private static async Task<int[]> Versions(NpgsqlConnection connection, NpgsqlTransaction? transaction,
        CancellationToken token)
    {
        await using var guard = new NpgsqlCommand("SELECT to_regclass('botgram_backend_v1.schema_versions')::text",
            connection, transaction);
        if (await guard.ExecuteScalarAsync(token) is not string) return [];
        await using var command = new NpgsqlCommand(
            "SELECT coalesce(array_agg(version ORDER BY version),'{}') FROM botgram_backend_v1.schema_versions",
            connection, transaction);
        return (int[])(await command.ExecuteScalarAsync(token))!;
    }

    private static async Task<object?> Scalar(NpgsqlConnection connection, NpgsqlTransaction? transaction,
        string sql, CancellationToken token, params object[] values)
    {
        await using var command = new NpgsqlCommand(sql, connection, transaction);
        foreach (var value in values) command.Parameters.AddWithValue(value);
        return await command.ExecuteScalarAsync(token);
    }
}
