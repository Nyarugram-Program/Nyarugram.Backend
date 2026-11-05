using Npgsql;
using Botgram.Backend;
using Botgram.Backend.Storage;
using Botgram.Backend.Storage.FrozenV1;
using Google.Protobuf;
using P = Botgram.Protocol.V1;
using CurrentDeliveryEventCodec = Botgram.Backend.Storage.DeliveryEventCodec;

internal static class FrozenOutboxTests
{
    private sealed record OutboxRow(Guid Id, string State, int Attempt, bool NoLease, bool NoBrokerPosition,
        int SchemaVersion, string Type, bool NoMessageId, bool OldSourceMatches);

    public static async Task RunAsync(string testConnectionString)
    {
        var options = new NpgsqlConnectionStringBuilder(testConnectionString) { Pooling = false };
        if (options.Host is not ("127.0.0.1" or "localhost" or "::1") ||
            options.Database is not string database || !database.StartsWith("botgram_test_", StringComparison.Ordinal))
            throw new InvalidOperationException("Frozen outbox test requires a loopback botgram_test_ database.");

        var name = $"botgram_test_frozen_v1_{Guid.NewGuid():N}";
        await using var admin = NpgsqlDataSource.Create(options.ConnectionString);
        await using (var create = admin.CreateCommand($"CREATE DATABASE {name}"))
            await create.ExecuteNonQueryAsync();

        try
        {
            options.Database = name;
            await using var source = NpgsqlDataSource.Create(options.ConnectionString);
            await PostgresBackendSchema.ApplyAsync(source);
            var backend = new PostgresBackend(source);
            var registration = await backend.RegisterAsync(new("frozen_outbox", "Frozen Outbox", "isolated-password"));
            var session = await backend.RequireAsync(Auth.HashToken(registration.Token));
            await using (var seed = source.CreateCommand("""
                WITH c AS (INSERT INTO botgram_intake_v1.conversations(id) VALUES ('frozen_message') RETURNING id)
                INSERT INTO botgram_intake_v1.members(conversation_id,user_id) SELECT id,$1 FROM c
                """))
            {
                seed.Parameters.AddWithValue(session.UserId);
                await seed.ExecuteNonQueryAsync();
            }
            await new PostgresMessageAcceptance(source).AcceptAsync(
                new(session.TokenHash, session.UserId, "frozen_message", new("frozen", "legacy message"),
                    MessageWriteContract.Telegram), accepted => accepted, CancellationToken.None);
            var oldOutbox = new PostgresOutboxV1(source);
            var legacy = await oldOutbox.ClaimAsync(1, TimeSpan.FromSeconds(15), CancellationToken.None);
            Check(legacy.Count == 1 && legacy[0].Delivery.Type == "message.created",
                "frozen outbox first claims a real v1 message");
            var currentOutbox = new PostgresDeliveryPersistence(source);
            Check(await currentOutbox.FailAsync(legacy[0].Delivery.OutboxId, legacy[0].LeaseToken,
                DateTimeOffset.UtcNow.AddMinutes(-1), false, CancellationToken.None), "restore v1 row to pending");

            await backend.DispatchAsync(session, new P.RpcRequest
            {
                ProtocolMajor = 1,
                SaveWallpaper = new P.SaveWallpaperRequest
                {
                    Wallpaper = new P.Wallpaper
                    {
                        NoFile = new P.NoFileWallpaper
                        {
                            Settings = new P.WallpaperSettings { BackgroundColor = 0x112233 }
                        }
                    }
                }
            }, "frozen-outbox-test", result => result.ToByteArray());

            var observer = await backend.RegisterAsync(new("presence_observer", "Observer", "isolated-password"));
            var observerSession = await backend.RequireAsync(Auth.HashToken(observer.Token));
            await backend.DispatchAsync(observerSession, new P.RpcRequest
            {
                AddContact = new() { User = new() { User = new() { ServerDomain = "frozen-outbox-test", UserId = session.UserId } }, FirstName = "Contact" }
            }, "frozen-outbox-test", result => result.ToByteArray());
            await backend.DispatchAsync(session, new P.RpcRequest { Foreground = true, GetState = new() },
                "frozen-outbox-test", result => result.ToByteArray());

            var before = await ReadOutbox(source);
            Check(before.Count == 3 && before.All(row => row.State == "Pending" && row.NoLease && row.NoBrokerPosition) &&
                before[0].Type == "message.created" && before[0].SchemaVersion == 1 && before[0].OldSourceMatches &&
                before[1].Type == "wallpaper.changed" && before[1].SchemaVersion == 2 && before[1].NoMessageId &&
                !before[1].OldSourceMatches && before[2].Type == "user.presence_changed" && before[2].SchemaVersion == 3,
                "v1 precedes non-message v2/v3 events, invisible to the frozen inner join");

            for (var attempt = 0; attempt < 2; attempt++)
            {
                var rejected = false;
                try
                {
                    await oldOutbox.ClaimAsync(100, TimeSpan.FromSeconds(15), CancellationToken.None);
                }
                catch (InvalidOperationException exception) when (
                    exception.StackTrace?.Contains("System.Linq", StringComparison.Ordinal) == true)
                {
                    rejected = true;
                }
                Check(rejected && (await ReadOutbox(source)).SequenceEqual(before),
                    "frozen outbox rejects and rolls back its complete lease transaction");
            }

            var current = await currentOutbox.ClaimAsync(3, TimeSpan.FromSeconds(15), CancellationToken.None);
            Check(current.Count == 2 && current.All(item => item.Delivery.SchemaVersion != 3),
                "presence waits for the earlier wallpaper in the same user partition");
            var wallpaper = current.Single(item => item.Delivery.OutboxId == before[1].Id);
            Check(await currentOutbox.ConfirmPublishedAsync(wallpaper.Delivery.OutboxId, wallpaper.LeaseToken,
                new(new("presence-test", 0, 1)), CancellationToken.None), "acknowledge wallpaper predecessor");
            var presence = (await currentOutbox.ClaimAsync(3, TimeSpan.FromSeconds(15), CancellationToken.None)).Single();
            var decoded = CurrentDeliveryEventCodec.Decode(CurrentDeliveryEventCodec.Encode(presence.Delivery));
            Check(decoded.SchemaVersion == 3 && decoded.Payload is PresenceChanged && decoded.OutboxId == before[2].Id,
                "current outbox and codec deliver presence after its predecessor");
            Console.WriteLine("PASS: frozen old outbox rolls back mixed v1/v2/v3; current outbox delivers all three in partition order");
        }
        finally
        {
            await using var drop = admin.CreateCommand($"DROP DATABASE {name}");
            await drop.ExecuteNonQueryAsync();
        }
    }

    private static async Task<List<OutboxRow>> ReadOutbox(NpgsqlDataSource source)
    {
        await using var command = source.CreateCommand("""
            SELECT o.id,o.state,o.attempt,o.lease_token IS NULL,o.broker_topic IS NULL,
                e.schema_version,e.type,e.message_id IS NULL,
                EXISTS(SELECT 1 FROM botgram_intake_v1.messages m WHERE m.id=e.message_id)
            FROM botgram_intake_v1.outbox o JOIN botgram_intake_v1.events e ON e.id=o.event_id ORDER BY e.id
            """);
        await using var reader = await command.ExecuteReaderAsync();
        var result = new List<OutboxRow>();
        while (await reader.ReadAsync())
            result.Add(new(reader.GetGuid(0), reader.GetString(1), reader.GetInt32(2),
                reader.GetBoolean(3), reader.GetBoolean(4), reader.GetInt32(5), reader.GetString(6),
                reader.GetBoolean(7), reader.GetBoolean(8)));
        return result;
    }

    private static void Check(bool condition, string description)
    {
        if (!condition) throw new InvalidOperationException($"Frozen outbox assertion failed: {description}");
    }
}
