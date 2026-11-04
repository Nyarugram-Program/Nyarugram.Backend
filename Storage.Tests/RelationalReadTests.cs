using System.Text.Json;
using Botgram.Backend;
using Botgram.Backend.Storage;
using Google.Protobuf;
using Npgsql;
using P = Botgram.Protocol.V1;

static class RelationalReadTests
{
    public static async Task<int> RunAsync(NpgsqlDataSource source)
    {
        var assertions = 0;
        void Check(bool value, string label)
        {
            if (!value) throw new InvalidOperationException(label);
            assertions++;
        }
        async Task<object?> Scalar(string sql, params object[] values)
        {
            await using var command = source.CreateCommand(sql);
            foreach (var value in values) command.Parameters.Add(new NpgsqlParameter { Value = value });
            return await command.ExecuteScalarAsync();
        }
        async Task<T[]> Rows<T>(string sql, Func<NpgsqlDataReader, T> read)
        {
            await using var command = source.CreateCommand(sql);
            await using var reader = await command.ExecuteReaderAsync();
            var rows = new List<T>();
            while (await reader.ReadAsync()) rows.Add(read(reader));
            return rows.ToArray();
        }
        async Task Reject(Func<Task> action, int status, string code)
        {
            try { await action(); }
            catch (ApiException exception) when (exception.Status == status && exception.Message == code)
            { assertions++; return; }
            throw new InvalidOperationException($"Expected {status}:{code}");
        }
        var backend = new PostgresBackend(source);
        var registration = await backend.RegisterAsync(new("read_owner", "Reader", "isolated-password"));
        var other = await backend.RegisterAsync(new("read_other", "Other", "isolated-password"));
        var outside = await backend.RegisterAsync(new("read_outside", "Outside", "isolated-password"));
        var session = await backend.RequireAsync(Auth.HashToken(registration.Token));
        var otherSession = await backend.RequireAsync(Auth.HashToken(other.Token));
        var outsideSession = await backend.RequireAsync(Auth.HashToken(outside.Token));
        var group = await backend.CreateConversationAsync(session, new("group", "Read tests", [other.User.Id]));
        var acceptance = new PostgresMessageAcceptance(source);
        var messages = new List<Message>();
        for (var i = 0; i < 105; i++)
            messages.Add((await acceptance.AcceptAsync(new(otherSession.TokenHash, otherSession.UserId, group.Id,
                new("read-test-" + i, "message " + i,
                    i == 1 ? [new(MessageEntityKind.Bold, 0, 7)] : null, i == 1 ? messages[0].Id : null, i == 1),
                MessageWriteContract.Telegram), r => r, default)).Message);
        const string domain = "read.test";
        async Task<P.RpcResponse> Rpc(P.RpcRequest request, Session? actor = null) =>
            P.RpcResponse.Parser.ParseFrom(await backend.DispatchAsync(actor ?? session, request, domain, r => r.ToByteArray()));
        P.PeerRef peer = new() { GroupId = group.Id };
        await Rpc(new() { GetHistory = new() { Peer = peer, Limit = 1 } });
        async Task<Database> Fixture()
        {
            var db = new Database();
            db.Users.AddRange(await Rows("""
                SELECT id,username,display_name,salt,password_hash,gid,premium FROM botgram_backend_v1.accounts ORDER BY ordinal
                """, r => new User(r.GetString(0), r.GetString(1), r.GetString(2), r.GetString(3), r.GetString(4), r.GetString(5), r.GetBoolean(6))));
            db.Conversations.AddRange(await backend.ConversationsAsync(session));
            db.Messages.AddRange(await Rows("SELECT body::text FROM botgram_intake_v1.messages ORDER BY id",
                r => JsonSerializer.Deserialize<Message>(r.GetString(0))!));
            db.Receipts.AddRange(await Rows("SELECT conversation_id,user_id,message_id FROM botgram_backend_v1.receipts",
                r => new Receipt(r.GetString(0), r.GetString(1), r.GetInt64(2))));
            db.Events.AddRange(await Rows("""
                SELECT e.id,e.type,COALESCE(e.payload,m.body)::text,e.created_at,e.expires_at,
                    ARRAY(SELECT r.user_id FROM botgram_intake_v1.event_recipients r WHERE r.event_id=e.id ORDER BY r.user_id)
                FROM botgram_intake_v1.events e LEFT JOIN botgram_intake_v1.messages m ON m.id=e.message_id ORDER BY e.id
                """, r => new ChatEvent(r.GetInt64(0), r.GetFieldValue<string[]>(5), r.GetString(1),
                    (IEventPayload)JsonSerializer.Deserialize(r.GetString(2), r.GetString(1) switch
                    {
                        "message.created" => typeof(Message), "conversation.created" => typeof(Conversation),
                        "receipt.updated" => typeof(Receipt), "user.gid_changed" => typeof(GidChanged),
                        _ => throw new InvalidDataException("Unexpected fixture event")
                    })!,
                    r.GetFieldValue<DateTimeOffset>(3), r.IsDBNull(4) ? null : r.GetFieldValue<DateTimeOffset>(4))));
            foreach (var pair in await Rows("SELECT key,value FROM botgram_backend_v1.identities",
                r => KeyValuePair.Create(r.GetString(0), r.GetInt64(1)))) db.TelegramIds.Add(pair.Key, pair.Value);
            db.NextTelegramId = db.TelegramIds.Values.DefaultIfEmpty(99999).Max() + 1;
            db.NextEventId = db.Events[^1].Id + 1;
            return db;
        }
        static void NormalizeTime(P.RpcResponse response)
        {
            if (response.GetDifference?.Empty is { } empty) empty.DateMs = 0;
            if ((response.GetDifference?.Complete ?? response.GetDifference?.Slice)?.State is { } state) state.DateMs = 0;
            if (response.GetPinnedDialogs?.State is { } pinned) pinned.DateMs = 0;
            if (response.GetPeerDialogs?.State is { } peers) peers.DateMs = 0;
        }
        async Task<P.RpcResponse> Parity(P.RpcRequest request)
        {
            var fixture = await Fixture();
            var expected = ProtobufRpcMapper.Execute(fixture, session.UserId, request, domain);
            var actual = await Rpc(request);
            NormalizeTime(expected);
            NormalizeTime(actual);
            Check(expected.ToByteArray().SequenceEqual(actual.ToByteArray()), "Read parity: " + request.OperationCase);
            return actual;
        }
        await Parity(new() { GetHistory = new() { Peer = peer } });
        await Parity(new() { GetHistory = new() { Peer = peer, Limit = 1 } });
        await Parity(new() { GetHistory = new() { Peer = peer, OffsetMessageId = messages[50].Id, AddOffset = -3, Limit = 8 } });
        await Parity(new() { GetHistory = new() { Peer = peer, MinMessageId = messages[10].Id, MaxMessageId = messages[90].Id, Limit = 100 } });
        await Parity(new() { GetHistory = new() { Peer = peer, OffsetDateMs = messages[0].CreatedAt.ToUnixTimeSeconds() * 1000, AddOffset = -4 } });
        await Parity(new() { GetMessages = new() { MessageIds = { messages[1].Id, int.MaxValue, messages[0].Id } } });
        var slice = await Parity(new() { GetDifference = new() { Pts = 1 } });
        Check(slice.GetDifference.Slice.Messages.Count == 100 && slice.GetDifference.Slice.State.Pts == 101, "difference page boundary");
        var tail = await Parity(new() { GetDifference = new() { Pts = 101 } });
        Check(tail.GetDifference.Complete.Messages.Count == 5 && tail.GetDifference.Complete.State.Pts == 106, "difference tail");
        await Parity(new() { GetDifference = new() { Pts = 0 } });
        await Parity(new() { GetDifference = new() { Pts = 106 } });
        await Parity(new() { GetDifference = new() { Pts = 107 } });
        await Reject(() => Rpc(new() { GetDifference = new() { Pts = -1 } }), 400, "BOTGRAM_RPC_INVALID_ARGUMENT");
        await Reject(() => Rpc(new() { GetMessages = new() { MessageIds = { messages[0].Id, messages[0].Id } } }), 400, "BOTGRAM_RPC_INVALID_ARGUMENT");
        await Reject(() => Rpc(new() { GetHistory = new() { Peer = peer, Limit = 0 } }), 400, "BOTGRAM_RPC_INVALID_ARGUMENT");
        await Reject(() => Rpc(new() { GetHistory = new() { Peer = peer, OffsetDateMs = 1 } }), 400, "BOTGRAM_RPC_INVALID_ARGUMENT");
        await Reject(() => Rpc(new() { GetHistory = new() { Peer = peer } }, outsideSession), 404, "PEER_ID_INVALID");
        Check((await Rpc(new() { GetMessages = new() { MessageIds = { messages[0].Id } } }, outsideSession))
            .GetMessages.Messages.Count == 0, "unauthorized message ID exposed");
        async Task<long> EventCount() => (long)(await Scalar("SELECT count(*) FROM botgram_intake_v1.events"))!;
        var before = await EventCount();
        var request = new P.RpcRequest { ReadHistory = new() { Peer = peer, MaxMessageId = messages[50].Id } };
        try
        {
            await backend.DispatchAsync<int>(session, request, domain, _ => throw new PreparationFailure());
            throw new InvalidOperationException("Expected receipt response failure");
        }
        catch (PreparationFailure) { assertions++; }
        Check(await EventCount() == before &&
            (long)(await Scalar("SELECT count(*) FROM botgram_backend_v1.receipts WHERE user_id=$1", session.UserId))! == 0,
            "receipt response failure left state");
        await Reject(() => new PostgresBackend(source, 1).DispatchAsync(session, request, domain, r => r.ToByteArray()),
            503, "storage_backlog_full");
        Check(await EventCount() == before, "receipt capacity failure left event");
        var read = await Parity(request);
        Check(read.ReadHistory.Pts == 107 && read.ReadHistory.PtsCount == 1, "read pts advancement");
        await Parity(request);
        await Parity(new() { ReadHistory = new() { Peer = peer, MaxMessageId = messages[10].Id } });
        Check(await EventCount() == before + 1, "read retry/regression emitted event");
        await Parity(new() { GetHistory = new() { Peer = peer, Limit = 100 } });
        var receiptDifference = await Parity(new() { GetDifference = new() { Pts = 106 } });
        Check(receiptDifference.GetDifference.Complete.Updates.Single().ReadInbox.MaxMessageId == messages[50].Id,
            "receipt inbox difference");
        var otherDifference = await Rpc(new() { GetDifference = new() { Pts = 106 } }, otherSession);
        Check(otherDifference.GetDifference.Complete.Updates.Single().ReadOutbox.Pts == 107, "receipt outbox difference");
        var actors = await Task.WhenAll(Enumerable.Range(0, 8).Select(async _ =>
            await backend.RequireAsync(Auth.HashToken((await backend.LoginAsync(new("read_owner", "isolated-password"))).Token))));
        using (var deadline = new CancellationTokenSource(TimeSpan.FromSeconds(15)))
        {
            var readAll = new P.RpcRequest { ReadHistory = new() { Peer = peer, MaxMessageId = int.MaxValue } };
            var results = await Task.WhenAll(actors.Select(actor =>
                backend.DispatchAsync(actor, readAll, domain, r => r.ReadHistory.PtsCount, deadline.Token)));
            Check(results.Sum() == 1 && await EventCount() == before + 2, "concurrent receipts not idempotent");
        }
        var savedRequest = new P.RpcRequest { GetHistory = new() { Peer = new() { Self = true } } };
        before = await EventCount();
        try
        {
            await backend.DispatchAsync<int>(session, savedRequest, domain, _ => throw new PreparationFailure());
            throw new InvalidOperationException("Expected history preparation failure");
        }
        catch (PreparationFailure) { assertions++; }
        Check(!(await backend.ConversationsAsync(session)).Any(c => c.Kind == "saved") && await EventCount() == before,
            "history implicit creation escaped rollback");
        using (var deadline = new CancellationTokenSource(TimeSpan.FromSeconds(15)))
            await Task.WhenAll(actors.Select(actor => backend.DispatchAsync(actor, savedRequest, domain, r => r.ToByteArray(), deadline.Token)));
        Check((await backend.ConversationsAsync(session)).Count(c => c.Kind == "saved") == 1 && await EventCount() == before + 1,
            "history implicit creation race");
        Check((await Rpc(new() { GetState = new() })).GetState.Pts == 108, "nonstream history creation advanced pts");
        await Parity(new() { GetDialogs = new() });
        await Parity(new() { GetDialogs = new() { Limit = 1 } });
        await Parity(new() { GetDialogs = new() { OffsetPeer = peer } });
        await Parity(new() { GetDialogs = new() { OffsetMessageId = messages[^1].Id } });
        await Parity(new() { GetDialogs = new() { FolderId = 1 } });
        await Parity(new() { GetPinnedDialogs = new() });
        await Parity(new() { GetPeerDialogs = new() { Peers = { new P.PeerRef { Self = true }, peer } } });
        await Parity(new() { GetPeerDialogs = new() { FolderId = 1, Peers = { peer } } });
        await Reject(() => Rpc(new() { GetDialogs = new() { FolderId = -1 } }), 400, "BOTGRAM_RPC_INVALID_ARGUMENT");
        await Reject(() => Rpc(new() { GetPeerDialogs = new() { Peers = { peer, peer } } }), 400, "BOTGRAM_RPC_INVALID_ARGUMENT");
        await Reject(() => Rpc(new() { GetPeerDialogs = new() { Peers = { peer } } }, outsideSession), 404, "PEER_ID_INVALID");
        await backend.LogoutAsync(session);
        await Reject(() => Rpc(new() { GetHistory = new() { Peer = peer } }), 401, "session_expired");
        return assertions;
    }

    private sealed class PreparationFailure : Exception;
}
