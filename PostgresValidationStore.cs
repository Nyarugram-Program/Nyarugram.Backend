using System.Net;
using System.Text.Json;
using System.Text.RegularExpressions;
using Npgsql;
using NpgsqlTypes;

namespace Botgram.Backend;

// Isolated full-snapshot compatibility bridge, not the relational intake/projection architecture.
internal sealed class PostgresValidationStore : IDisposable
{
    private readonly NpgsqlDataSource source;

    internal static string Validate(IConfiguration configuration)
    {
        if (!configuration.GetValue<bool>("Storage:ValidationAcknowledged"))
            throw new InvalidOperationException("Storage:Mode validation requires Development and explicit acknowledgement");
        var options = new NpgsqlConnectionStringBuilder(configuration["Storage:ValidationConnectionString"] ?? "");
        if (!(string.Equals(options.Host, "localhost", StringComparison.OrdinalIgnoreCase) || IPAddress.TryParse(options.Host, out var address) && IPAddress.IsLoopback(address)) ||
            !Regex.IsMatch(options.Database ?? "", "\\Abotgram_test_[a-z0-9_]+\\z"))
            throw new InvalidOperationException("Storage:Mode validation requires an isolated loopback botgram_test_* database");
        options.IncludeErrorDetail = false;
        options.LogParameters = false;
        return options.ConnectionString;
    }

    internal PostgresValidationStore(IConfiguration configuration)
    {
        source = NpgsqlDataSource.Create(Validate(configuration));
        using var connection = source.OpenConnection();
        using var transaction = connection.BeginTransaction();
        using (var schema = new NpgsqlCommand("SET LOCAL synchronous_commit=on; CREATE SCHEMA IF NOT EXISTS botgram_validation_v1; " +
            "CREATE TABLE IF NOT EXISTS botgram_validation_v1.state(singleton boolean PRIMARY KEY CHECK(singleton), payload jsonb NOT NULL)", connection, transaction))
            schema.ExecuteNonQuery();
        using var exists = new NpgsqlCommand("SELECT count(*) FROM botgram_validation_v1.state", connection, transaction);
        if ((long)exists.ExecuteScalar()! == 0)
        {
            var seed = configuration["Storage:ValidationSeedPath"];
            var database = seed is null ? new Database() : JsonSerializer.Deserialize<Database>(File.ReadAllText(seed), Store.Json)
                ?? throw new InvalidDataException("Invalid validation seed");
            Gids.Migrate(database);
            using var insert = new NpgsqlCommand("INSERT INTO botgram_validation_v1.state VALUES(true,$1) ON CONFLICT DO NOTHING", connection, transaction);
            insert.Parameters.AddWithValue(NpgsqlDbType.Jsonb, JsonSerializer.Serialize(database, Store.Json));
            insert.ExecuteNonQuery();
        }
        transaction.Commit();
    }

    internal T Read<T>(Func<Database, T> action)
    {
        using var command = source.CreateCommand("SELECT payload::text FROM botgram_validation_v1.state WHERE singleton");
        var database = JsonSerializer.Deserialize<Database>((string)command.ExecuteScalar()!, Store.Json)
            ?? throw new InvalidDataException("Missing validation state");
        return action(database);
    }

    internal T Write<T>(Func<Database, T> action)
    {
        using var connection = source.OpenConnection();
        using var transaction = connection.BeginTransaction();
        using var select = new NpgsqlCommand("SET LOCAL synchronous_commit=on; SELECT payload::text FROM botgram_validation_v1.state WHERE singleton FOR UPDATE", connection, transaction);
        var database = JsonSerializer.Deserialize<Database>((string)select.ExecuteScalar()!, Store.Json)
            ?? throw new InvalidDataException("Missing validation state");
        Store.Maintain(database);
        var result = action(database);
        using var update = new NpgsqlCommand("UPDATE botgram_validation_v1.state SET payload=$1 WHERE singleton", connection, transaction);
        update.Parameters.AddWithValue(NpgsqlDbType.Jsonb, JsonSerializer.Serialize(database, Store.Json));
        if (update.ExecuteNonQuery() != 1) throw new InvalidDataException("Missing validation state");
        // An ambiguous commit is never retried here; callers must recover with their original idempotency key.
        transaction.Commit();
        return result;
    }

    public void Dispose() => source.Dispose();
}
