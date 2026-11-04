using System.Text.Json;
using Npgsql;

namespace Botgram.Backend.Storage.FrozenV1;

internal sealed class PostgresOutboxV1(NpgsqlDataSource dataSource)
{
// BEGIN FROZEN OUTBOX SOURCE
    private const string SourceSql = """
        SELECT o.id,o.schema_version,e.id,o.partition_key,e.type,m.body::text,e.created_at,
            m.conversation_sequence,o.projection_version,
            (SELECT jsonb_agg(jsonb_build_object('UserId',r.user_id,'Pts',r.pts) ORDER BY r.user_id COLLATE "C")::text
             FROM botgram_intake_v1.event_recipients r WHERE r.event_id=e.id)
        FROM botgram_intake_v1.outbox o JOIN botgram_intake_v1.events e ON e.id=o.event_id
        JOIN botgram_intake_v1.messages m ON m.id=e.message_id
        """;

    private static async Task<List<DeliveryEvent>> Source(NpgsqlConnection connection, NpgsqlTransaction transaction,
        string predicate, CancellationToken ct, params object[] values)
    {
        await using var command = Command(connection, transaction, SourceSql + predicate, values);
        await using var reader = await command.ExecuteReaderAsync(ct);
        var result = new List<DeliveryEvent>();
        while (await reader.ReadAsync(ct))
            result.Add(new(reader.GetGuid(0), reader.GetInt32(1), reader.GetInt64(2), reader.GetString(3),
                reader.GetString(4), JsonSerializer.Deserialize<Message>(reader.GetString(5))!,
                reader.IsDBNull(9) ? [] : JsonSerializer.Deserialize<RecipientPosition[]>(reader.GetString(9))!
                    .OrderBy(r => r.UserId, StringComparer.Ordinal).ToArray(),
                reader.GetFieldValue<DateTimeOffset>(6), null, reader.GetInt64(7), reader.GetInt64(8), 1));
        return result;
    }

// END FROZEN OUTBOX SOURCE

// BEGIN FROZEN OUTBOX CLAIM
    public async Task<IReadOnlyList<OutboxLease>> ClaimAsync(int limit, TimeSpan leaseDuration, CancellationToken cancellationToken)
    {
        Limit(limit); Duration(leaseDuration);
        await using var connection = await dataSource.OpenConnectionAsync(cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);
        await Durable(connection, transaction, cancellationToken);
        var ids = new List<Guid>();
        await using (var query = Command(connection, transaction, """
            SELECT o.id FROM botgram_intake_v1.outbox o
            WHERE ((o.state='Pending' AND o.retry_at<=clock_timestamp()) OR
                (o.state='Leased' AND o.lease_until<=clock_timestamp()))
              AND NOT EXISTS (SELECT 1 FROM botgram_intake_v1.outbox p
                WHERE p.partition_key=o.partition_key AND p.event_id<o.event_id AND p.state<>'Published')
            ORDER BY o.event_id LIMIT $1 FOR UPDATE OF o SKIP LOCKED
            """, limit))
        {
            await using var reader = await query.ExecuteReaderAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken)) ids.Add(reader.GetGuid(0));
        }
        var result = new List<OutboxLease>();
        foreach (var id in ids)
        {
            var token = Guid.NewGuid();
            await using var update = Command(connection, transaction, """
                UPDATE botgram_intake_v1.outbox SET state='Leased',lease_token=$2,
                    lease_until=clock_timestamp()+$3,attempt=attempt+1 WHERE id=$1 RETURNING lease_until,attempt
                """, id, token, leaseDuration);
            DateTimeOffset until; int attempt;
            await using (var reader = await update.ExecuteReaderAsync(cancellationToken))
            { await reader.ReadAsync(cancellationToken); until = reader.GetFieldValue<DateTimeOffset>(0); attempt = reader.GetInt32(1); }
            var delivery = (await Source(connection, transaction, " WHERE o.id=$1", cancellationToken, id)).Single();
            result.Add(new(delivery, token, until, attempt));
        }
        await transaction.CommitAsync(cancellationToken);
        return result;
    }

// END FROZEN OUTBOX CLAIM

    private static void Limit(int limit)
    { if (limit is < 1 or > 1000) throw new ArgumentOutOfRangeException(nameof(limit)); }
    private static void Duration(TimeSpan duration)
    { if (duration <= TimeSpan.Zero || duration > TimeSpan.FromHours(1)) throw new ArgumentOutOfRangeException(nameof(duration)); }
    private static Task<int> Durable(NpgsqlConnection connection, NpgsqlTransaction transaction, CancellationToken ct) =>
        Execute(connection, transaction, "SET LOCAL synchronous_commit=on", ct);
    private static NpgsqlCommand Command(NpgsqlConnection connection, NpgsqlTransaction transaction, string sql, params object[] values)
    {
        var command = new NpgsqlCommand(sql, connection, transaction);
        foreach (var value in values) command.Parameters.Add(new NpgsqlParameter { Value = value });
        return command;
    }
    private static async Task<int> Execute(NpgsqlConnection connection, NpgsqlTransaction transaction, string sql, CancellationToken ct, params object[] values)
    {
        await using var command = Command(connection, transaction, sql, values);
        return await command.ExecuteNonQueryAsync(ct);
    }
}
