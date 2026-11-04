using Npgsql;

namespace Botgram.Backend.Storage;

public sealed class FourComponentReads(FourComponentRuntime runtime)
{
    private long cacheHits, projectionReads, authorityReads;
    public object Metrics => new { cacheHits = Interlocked.Read(ref cacheHits),
        projectionReads = Interlocked.Read(ref projectionReads), authorityReads = Interlocked.Read(ref authorityReads) };

    public async Task<Message> ReadAsync(NpgsqlConnection connection, NpgsqlTransaction transaction,
        string conversation, long id, long sequence, Func<CancellationToken, Task> authorize,
        Func<CancellationToken, Task<Message>> authority, CancellationToken token)
    {
        await using var command = new NpgsqlCommand("""
            SELECT h.generation,g.through_sequence FROM botgram_intake_v1.projection_heads h
            JOIN botgram_intake_v1.projection_generations g USING(conversation_id,generation)
            WHERE h.conversation_id=$1
            """, connection, transaction);
        command.Parameters.AddWithValue(conversation);
        Guid generation = Guid.Empty;
        long through = 0;
        await using (var reader = await command.ExecuteReaderAsync(token))
            if (await reader.ReadAsync(token)) { generation = reader.GetGuid(0); through = reader.GetInt64(1); }
        async Task<Message> Durable(CancellationToken ct)
        {
            if (sequence <= through)
            {
                ProjectedMessage? projected;
                try { projected = await runtime.Projection.ReadAsync(generation, conversation, sequence, id, ct); }
                catch (Cassandra.DriverException) { throw new ApiException(503, "projection_unavailable"); }
                if (projected is null || projected.Work.ConversationSequence != sequence ||
                    projected.Work.Message.ConversationId != conversation || projected.Work.Message.Id != id)
                    throw new ApiException(503, "projection_unavailable");
                Interlocked.Increment(ref projectionReads);
                return projected.Work.Message;
            }
            Interlocked.Increment(ref authorityReads);
            return await authority(ct);
        }
        if (generation == Guid.Empty) return await Durable(token);
        var loaded = false;
        var result = await runtime.Cache.ReadThroughAsync(generation, conversation, id, authorize,
            async ct => { loaded = true; return await Durable(ct); }, token);
        if (!loaded) Interlocked.Increment(ref cacheHits);
        return result;
    }
}
