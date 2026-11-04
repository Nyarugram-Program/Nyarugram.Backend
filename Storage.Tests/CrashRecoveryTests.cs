using System.Net;
using System.Net.Sockets;
using System.Buffers.Binary;
using System.Text;
using Botgram.Backend;
using Botgram.Backend.Storage;
using Npgsql;

static class CrashRecoveryTests
{
    public static async Task RunAsync(string connectionString)
    {
        var options = new NpgsqlConnectionStringBuilder(connectionString);
        var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        var port = ((IPEndPoint)listener.LocalEndpoint).Port;
        var relay = RelayAsync(listener, options.Port, timeout.Token);
        options.Port = port;
        options.SslMode = SslMode.Disable;
        options.Pooling = false;
        options.Timeout = 10;
        options.CommandTimeout = 10;
        options.CancellationTimeout = 1000;
        await using var proxy = NpgsqlDataSource.Create(options.ConnectionString);
        var input = new MessageAcceptance("hash-a", "a", "chat", new("commit-uncertain", "commit relay"), MessageWriteContract.Telegram);
        var failed = false;
        try
        {
            await new PostgresMessageAcceptance(proxy).AcceptAsync(input, x => x, timeout.Token);
        }
        catch (NpgsqlException) { failed = true; }
        finally { listener.Stop(); }
        if (!failed || !await relay) throw new InvalidOperationException("COMMIT relay must observe committed ReadyForQuery and lose acknowledgement");
        await using var direct = NpgsqlDataSource.Create(connectionString);
        var recovered = await new PostgresMessageAcceptance(direct).AcceptAsync(input, x => x, timeout.Token);
        if (!recovered.Duplicate) throw new InvalidOperationException("Uncertain COMMIT retry was not deduplicated");
        await using var invariant = direct.CreateCommand("SELECT (SELECT count(*) FROM botgram_intake_v1.messages WHERE client_key='commit-uncertain')=1 AND " +
            "(SELECT count(*) FROM botgram_intake_v1.events WHERE message_id=$1)=1 AND " +
            "(SELECT count(*) FROM botgram_intake_v1.outbox WHERE event_id=(SELECT id FROM botgram_intake_v1.events WHERE message_id=$1))=1 AND " +
            "NOT EXISTS(SELECT 1 FROM botgram_intake_v1.sync s WHERE s.pts<>1+(SELECT count(*) FROM botgram_intake_v1.event_recipients r WHERE r.user_id=s.user_id))");
        invariant.Parameters.AddWithValue(recovered.Message.Id);
        if (!(bool)(await invariant.ExecuteScalarAsync(timeout.Token))!) throw new InvalidOperationException("Uncertain COMMIT changed message/event/outbox/pts cardinality");
        Console.WriteLine("PASS: real PostgreSQL COMMIT reply dropped at protocol proxy after committed ReadyForQuery; retry retains one message/event/outbox and pts");
    }

    private static async Task<bool> RelayAsync(TcpListener listener, int targetPort, CancellationToken ct)
    {
        using var client = await listener.AcceptTcpClientAsync(ct);
        using var backend = new TcpClient();
        await backend.ConnectAsync(IPAddress.Loopback, targetPort, ct);
        var front = client.GetStream();
        var back = backend.GetStream();
        var lengthBytes = new byte[4];
        await front.ReadExactlyAsync(lengthBytes, ct);
        var length = BinaryPrimitives.ReadInt32BigEndian(lengthBytes);
        if (length is < 8 or > 65536) throw new InvalidDataException("Invalid PostgreSQL startup frame");
        var startup = new byte[length - 4];
        await front.ReadExactlyAsync(startup, ct);
        await back.WriteAsync(lengthBytes, ct);
        await back.WriteAsync(startup, ct);
        var commit = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var upstream = Task.Run(async () =>
        {
            try
            {
                while (true)
                {
                    var frame = await Frame(front, ct);
                    // Npgsql emits transaction control as a simple Query frame.
                    if (frame[0] == (byte)'Q' && Encoding.UTF8.GetString(frame, 5, frame.Length - 5).TrimEnd('\0').Trim().Equals("COMMIT", StringComparison.OrdinalIgnoreCase))
                        commit.TrySetResult();
                    await back.WriteAsync(frame, ct);
                }
            }
            catch (Exception error) when (error is IOException or OperationCanceledException or ObjectDisposedException) { }
        }, ct);
        try
        {
            while (true)
            {
                var frame = await Frame(back, ct);
                if (commit.Task.IsCompleted)
                {
                    if (frame[0] == (byte)'Z')
                    {
                        var committed = frame.Length == 6 && frame[5] == (byte)'I';
                        client.Close();
                        backend.Close();
                        await upstream;
                        return committed;
                    }
                    if (frame[0] == (byte)'E') throw new InvalidOperationException("Backend rejected COMMIT");
                    continue;
                }
                await front.WriteAsync(frame, ct);
            }
        }
        finally { client.Close(); backend.Close(); }
    }

    private static async Task<byte[]> Frame(NetworkStream stream, CancellationToken ct)
    {
        var header = new byte[5];
        await stream.ReadExactlyAsync(header, ct);
        var length = BinaryPrimitives.ReadInt32BigEndian(header.AsSpan(1));
        if (length is < 4 or > 16 * 1024 * 1024) throw new InvalidDataException("Invalid PostgreSQL frame");
        var frame = new byte[length + 1];
        header.CopyTo(frame, 0);
        await stream.ReadExactlyAsync(frame.AsMemory(5), ct);
        return frame;
    }
}
