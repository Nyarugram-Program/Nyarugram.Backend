using System.Diagnostics;
using System.Text.Json;
using Botgram.Backend;
using Botgram.Backend.Storage;
using Npgsql;
using Google.Protobuf;
using P = Botgram.Protocol.V1;

static class RelationalBackendTests
{
    public static async Task RunAsync(string connectionString)
    {
        await using var source = NpgsqlDataSource.Create(connectionString);
        await PostgresBackendSchema.ApplyAsync(source);
        await PostgresBackendSchema.ApplyAsync(source);
        var backend = new PostgresBackend(source);
        var assertions = 0;
        void Check(bool value, string message)
        {
            if (!value) throw new InvalidOperationException(message);
            assertions++;
        }
        async Task<object?> Scalar(string sql, params object[] values)
        {
            await using var command = source.CreateCommand(sql);
            foreach (var value in values) command.Parameters.Add(new NpgsqlParameter { Value = value });
            return await command.ExecuteScalarAsync();
        }
        async Task ExpectApi(Func<Task> action, int status, string code)
        {
            try { await action(); }
            catch (ApiException exception) when (exception.Status == status && exception.Message == code)
            { assertions++; return; }
            throw new InvalidOperationException($"Expected {status}:{code}");
        }

        var alice = await backend.RegisterAsync(new("alice", " Alice ", "isolated-password-a", "CN"));
        var bob = await backend.RegisterAsync(new("bob", "Bob", "isolated-password-b", "unknown"));
        Check(alice.User.Gid.StartsWith("+030 ", StringComparison.Ordinal), "regional registration");
        Check(bob.User.Gid.StartsWith("+001 ", StringComparison.Ordinal), "fallback registration");
        Check(alice.User.DisplayName == "Alice", "shared registration normalization");
        Check(Guid.TryParseExact(alice.User.Id, "N", out _) && alice.User.Id != bob.User.Id, "UUID identity");
        var session = await backend.RequireAsync(Auth.HashToken(alice.Token));
        Check(session.UserId == alice.User.Id, "same authority authentication");
        Check((await backend.MeAsync(session)) == alice.User, "public user roundtrip");
        Check((long)(await Scalar("SELECT count(*) FROM botgram_backend_v1.identities"))! == 0,
            "registration must not preallocate lazy TL identities");
        var publicJson = JsonSerializer.Serialize(alice, Store.Json);
        Check(!publicJson.Contains("password", StringComparison.OrdinalIgnoreCase) &&
            !publicJson.Contains("salt", StringComparison.OrdinalIgnoreCase), "public result leaked credentials");
        Check((long)(await Scalar("SELECT count(*) FROM botgram_intake_v1.sessions WHERE token_hash=$1", alice.Token))! == 0,
            "plaintext token persisted");
        Check(await Scalar("SELECT to_regclass('botgram_validation_v1.snapshots')::text") is DBNull,
            "snapshot bridge must not be initialized");
        await ExpectApi(() => backend.RegisterAsync(new("ALICE", "Duplicate", "isolated-password-c")), 409, "username_taken");
        Check((long)(await Scalar("SELECT count(*) FROM botgram_intake_v1.users"))! == 2 &&
            (long)(await Scalar("SELECT count(*) FROM botgram_backend_v1.accounts"))! == 2 &&
            (long)(await Scalar("SELECT count(*) FROM botgram_intake_v1.sessions"))! == 2, "duplicate registration left partial rows");
        await ExpectApi(() => backend.LoginAsync(new("alice", "wrong-password")), 401, "invalid_credentials");
        await ExpectApi(() => backend.LoginAsync(new("missing", "wrong-password")), 401, "invalid_credentials");
        await ExpectApi(() => backend.RegisterAsync(new("bad", "Bad", "short")), 400, "password_length_10_to_256");

        var contenders = await Task.WhenAll(Enumerable.Range(0, 8).Select(async _ =>
        {
            try { await backend.RegisterAsync(new("contended", "Contended", "isolated-password")); return true; }
            catch (ApiException exception) when (exception.Status == 409 && exception.Message == "username_taken")
            { return false; }
        }));
        Check(contenders.Count(x => x) == 1, "concurrent username uniqueness");
        Check((long)(await Scalar("SELECT count(*) FROM botgram_intake_v1.users"))! == 3 &&
            (long)(await Scalar("SELECT count(*) FROM botgram_backend_v1.accounts"))! == 3, "concurrent registration rollback");

        var logins = await Task.WhenAll(Enumerable.Range(0, 8).Select(_ => backend.LoginAsync(new("ALICE", "isolated-password-a"))));
        var sessions = await Task.WhenAll(logins.Select(l => backend.RequireAsync(Auth.HashToken(l.Token))));
        var profiles = await Task.WhenAll(sessions.Select(s => backend.CurrentProfileAsync(s)));
        Check(profiles.Select(p => p.TelegramId).Distinct().Count() == 1, "concurrent lazy identity changed");
        Check(profiles.All(p => p.IsSelf && p.FirstName == "Alice" && !p.IsContact), "shared profile mapping");
        Check((long)(await Scalar("SELECT count(*) FROM botgram_backend_v1.identities"))! == 1, "duplicate identity rows");
        Check((int)(await Scalar("SELECT pts FROM botgram_intake_v1.sync WHERE user_id=$1", alice.User.Id))! == 1,
            "profile read advanced pts");
        await using (var reopened = NpgsqlDataSource.Create(connectionString))
        {
            var reopenedBackend = new PostgresBackend(reopened);
            var restored = await reopenedBackend.CurrentProfileAsync(await reopenedBackend.RequireAsync(session.TokenHash));
            Check(restored.TelegramId == profiles[0].TelegramId && restored.Gid == profiles[0].Gid,
                "reopen identity/GID preservation");
        }

        const long gate = 7459241304235;
        await using var blocker = await source.OpenConnectionAsync();
        await using var block = await blocker.BeginTransactionAsync();
        await using (var hold = new NpgsqlCommand($"SELECT pg_advisory_xact_lock({gate})", blocker, block))
            await hold.ExecuteNonQueryAsync();
        await Scalar($"""
            CREATE FUNCTION botgram_backend_v1.test_identity_wait() RETURNS trigger LANGUAGE plpgsql AS $$
            BEGIN PERFORM pg_advisory_xact_lock({gate}); RETURN NEW; END $$;
            CREATE TRIGGER test_identity_wait BEFORE INSERT ON botgram_backend_v1.identities
                FOR EACH ROW EXECUTE FUNCTION botgram_backend_v1.test_identity_wait();
            """);
        var bobSession = await backend.RequireAsync(Auth.HashToken(bob.Token));
        using var deadline = new CancellationTokenSource(TimeSpan.FromSeconds(15));
        var inFlight = backend.CurrentProfileAsync(bobSession, deadline.Token);
        async Task WaitFor(Func<Task<bool>> condition)
        {
            var elapsed = Stopwatch.StartNew();
            while (!await condition())
            {
                if (elapsed.Elapsed > TimeSpan.FromSeconds(5)) throw new TimeoutException("Expected database lock wait");
                await Task.Delay(10, deadline.Token);
            }
        }
        await WaitFor(async () => (bool)(await Scalar($"""
            SELECT EXISTS(SELECT 1 FROM pg_locks WHERE locktype='advisory' AND NOT granted
                AND classid=({gate}::bigint>>32)::oid AND objid=({gate}::bigint&4294967295)::oid AND objsubid=1)
            """))!);
        var logout = backend.LogoutAsync(bobSession, deadline.Token);
        await WaitFor(async () => (bool)(await Scalar("""
            SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE datname=current_database()
                AND wait_event_type='Lock' AND query LIKE '%sessions WHERE token_hash=$1 FOR UPDATE%')
            """))!);
        Check(!inFlight.IsCompleted && !logout.IsCompleted, "revocation did not serialize with authenticated operation");
        await block.CommitAsync();
        Check((await inFlight).Id == bob.User.Id && await logout, "in-flight operation and revocation ordering");
        await Scalar("""
            DROP TRIGGER test_identity_wait ON botgram_backend_v1.identities;
            DROP FUNCTION botgram_backend_v1.test_identity_wait();
            """);
        await ExpectApi(() => backend.MeAsync(bobSession), 401, "session_expired");
        await ExpectApi(() => backend.RequireAsync(bobSession.TokenHash), 401, "session_expired");

        var chat = Guid.NewGuid().ToString("N");
        await Scalar("INSERT INTO botgram_intake_v1.conversations(id) VALUES($1)", chat);
        await Scalar("INSERT INTO botgram_intake_v1.members(conversation_id,user_id) VALUES($1,$2),($1,$3)",
            chat, alice.User.Id, bob.User.Id);
        var acceptance = new PostgresMessageAcceptance(source);
        await ExpectApi(() => acceptance.AcceptAsync(new(bobSession.TokenHash, bob.User.Id, chat,
            new("revoked", "must reject"), MessageWriteContract.Telegram), r => r, CancellationToken.None),
            401, "session_expired");
        Check((long)(await Scalar("SELECT count(*) FROM botgram_intake_v1.messages"))! == 0, "revoked send persisted");
        var accepted = await acceptance.AcceptAsync(new(session.TokenHash, session.UserId, chat,
            new("accepted", "same authority"), MessageWriteContract.Telegram), r => r, CancellationToken.None);
        Check(accepted.Pts == 2 && accepted.Message.SenderId == alice.User.Id, "authenticated relational acceptance");
        await Scalar("UPDATE botgram_intake_v1.sessions SET expires_at=clock_timestamp()-interval '1 second' WHERE token_hash=$1",
            sessions[0].TokenHash);
        await ExpectApi(() => backend.CurrentProfileAsync(sessions[0]), 401, "session_expired");
        const string domain = "migration.test";
        async Task<byte[]> Rpc(Session actor, P.RpcRequest request) =>
            await backend.DispatchAsync(actor, request, domain, response => response.ToByteArray());
        var emptyUsers = new P.RpcRequest { ProtocolMajor = 1, RequestId = "empty-users", GetUsers = new() };
        Check(P.RpcResponse.Parser.ParseFrom(await Rpc(session, emptyUsers)).GetUsers.Users.Count == 0,
            "empty user selection");
        Check((long)(await Scalar("SELECT count(*) FROM botgram_backend_v1.identities"))! == 3,
            "GetUsers must retain legacy lazy allocation side effects");
        var legacy = new Database();
        await using (var users = source.CreateCommand("""
            SELECT id,username,display_name,salt,password_hash,gid FROM botgram_backend_v1.accounts ORDER BY ordinal
            """))
        await using (var reader = await users.ExecuteReaderAsync())
            while (await reader.ReadAsync())
                legacy.Users.Add(new(reader.GetString(0), reader.GetString(1), reader.GetString(2),
                    reader.GetString(3), reader.GetString(4), reader.GetString(5)));
        await using (var ids = source.CreateCommand("SELECT key,value FROM botgram_backend_v1.identities"))
        await using (var reader = await ids.ExecuteReaderAsync())
            while (await reader.ReadAsync()) legacy.TelegramIds.Add(reader.GetString(0), reader.GetInt64(1));
        async Task<P.RpcResponse> Parity(P.RpcRequest request, Session? actor = null)
        {
            actor ??= session;
            request.ProtocolMajor = 1;
            request.RequestId = "user-parity";
            var expectedResponse = ProtobufRpcMapper.Execute(legacy, actor.UserId, request, domain);
            // This fixture has no wallpaper records; the new PG extension must explicitly report cleared.
            if (expectedResponse.GetFullUser is { } fullUser) fullUser.Wallpaper = new() { Cleared = new() };
            if (expectedResponse.GetFullGroup is { } fullGroup) fullGroup.Wallpaper = new() { Cleared = new() };
            var expected = expectedResponse.ToByteArray();
            var actual = await Rpc(actor, request);
            Check(actual.SequenceEqual(expected), $"User RPC parity: {request.OperationCase}");
            return P.RpcResponse.Parser.ParseFrom(actual);
        }
        P.UserSelector Select(string id) => new() { User = new() { UserId = id, ServerDomain = domain } };
        await Parity(new() { GetUsers = new() { Users = { Select(bob.User.Id), new P.UserSelector { Self = true } } } });
        await Parity(new() { SearchContacts = new() { Query = "@BOB" } });
        await Parity(new() { ResolveUsername = new() { Username = "BOB" } });
        await Parity(new() { ResolveGid = new() { Gid = bob.User.Gid } });
        await Parity(new() { GetContacts = new() });
        await Parity(new() { GetFullUser = new() { User = Select(bob.User.Id) } });
        await Parity(new() { GetFullUser = new() { User = new() { Self = true } } });
        var unknownGid = Gids.Allocate(legacy, "030");
        P.RpcRequest Import(string gid, string name, long clientId = 1) => new()
        {
            ProtocolMajor = 1, RequestId = "import",
            ImportContacts = new() { Contacts = { new P.ImportContact { Gid = gid, FirstName = name, ClientId = clientId } } }
        };
        var import = Import(bob.User.Gid, " Private Bob ");
        import.ImportContacts.Contacts.Add(new P.ImportContact { Gid = alice.User.Gid, FirstName = "Self", ClientId = 2 });
        import.ImportContacts.Contacts.Add(new P.ImportContact { Gid = unknownGid, FirstName = "Unknown", ClientId = 3 });
        var imported = await Parity(import);
        Check(imported.ImportContacts.Imported.Count == 1 &&
            imported.ImportContacts.Users.Single().FirstName == "Private Bob", "private contact names and skipped entries");
        await Parity(new() { GetContacts = new() });
        await Parity(new() { ResolveUsername = new() { Username = "bob" } });
        var bobLogin = await backend.LoginAsync(new("bob", "isolated-password-b"));
        bobSession = await backend.RequireAsync(Auth.HashToken(bobLogin.Token));
        await Parity(Import(alice.User.Gid, "Private Alice"), bobSession);
        var mutual = await Parity(new() { GetContacts = new() });
        Check(mutual.GetContacts.Contacts.Single().Mutual && mutual.GetContacts.Users.Single().IsMutualContact,
            "mutual contact relation");
        Check((await backend.CurrentProfileAsync(bobSession)).FirstName == "Bob", "private contact name leaked to subject");
        var third = legacy.Users.Single(u => u.Username == "contended");
        await Parity(Import(third.Gid, "Third"));
        await Parity(Import(bob.User.Gid, "Renamed Bob"));
        var ordered = await Parity(new() { GetContacts = new() });
        Check(ordered.GetContacts.Users.Select(u => u.Identity.UserId).SequenceEqual(new[] { third.Id, bob.User.Id }),
            "reimport moves contact to end");
        try
        {
            await backend.DispatchAsync<int>(session, Import(bob.User.Gid, "must rollback"), domain,
                _ => throw new PreparationFailure());
            throw new InvalidOperationException("Expected response preparation failure");
        }
        catch (PreparationFailure) { assertions++; }
        await Parity(new() { GetContacts = new() });
        var invalidImport = Import(third.Gid, "must not persist");
        invalidImport.ImportContacts.Contacts.Add(new P.ImportContact
            { Gid = bob.User.Gid, FirstName = " ", ClientId = 2 });
        await ExpectApi(() => Rpc(session, invalidImport), 400, "FIRSTNAME_INVALID");
        await Parity(new() { GetContacts = new() });
        var duplicate = Import(bob.User.Gid, "Duplicate");
        duplicate.ImportContacts.Contacts.Add(duplicate.ImportContacts.Contacts[0].Clone());
        await ExpectApi(() => Rpc(session, duplicate), 400, "BOTGRAM_RPC_INVALID_ARGUMENT");
        await ExpectApi(() => Rpc(session, new() { ResolveUsername = new() { Username = "@bob" } }),
            400, "USERNAME_NOT_OCCUPIED");
        await ExpectApi(() => Rpc(session, new() { ResolveGid = new() { Gid = "invalid" } }), 400, "PHONE_NUMBER_INVALID");
        await ExpectApi(() => Rpc(session, new() { GetFullUser = new() { User = Select(Guid.NewGuid().ToString("N")) } }),
            404, "USER_ID_INVALID");
        var remote = Select(bob.User.Id);
        remote.User.ServerDomain = "remote.test";
        await ExpectApi(() => Rpc(session, new() { GetFullUser = new() { User = remote } }),
            501, "BOTGRAM_REMOTE_DOMAIN_UNSUPPORTED");
        var actors = sessions.Skip(1).Append(session).ToArray();
        using (var concurrentDeadline = new CancellationTokenSource(TimeSpan.FromSeconds(15)))
        {
            var updates = await Task.WhenAll(actors.Select((actor, index) => backend.DispatchAsync(actor,
                Import(bob.User.Gid, "concurrent-" + index), domain, response => response.ToByteArray(), concurrentDeadline.Token)));
            Check(updates.All(bytes => P.RpcResponse.Parser.ParseFrom(bytes).ImportContacts.Users.Count == 1),
                "concurrent contact imports");
        }
        Check((long)(await Scalar("SELECT count(*) FROM botgram_backend_v1.contacts WHERE owner_id=$1", alice.User.Id))! == 2,
            "concurrent contact uniqueness");
        Check((int)(await Scalar("SELECT pts FROM botgram_intake_v1.sync WHERE user_id=$1", alice.User.Id))! == 2 &&
            (long)(await Scalar("SELECT count(*) FROM botgram_intake_v1.events"))! == 1,
            "contact writes unexpectedly advanced stream");
        var candidates = Enumerable.Range(12345678, 20).Select(n => "030" + n)
            .Where(gid => legacy.Users.All(u => u.Gid != gid)).Take(3).ToArray();
        ChangeGidInput ChangeTo(string gid) => new(gid[..3], gid[3..]);
        var changed = await backend.ChangeGidAsync(session, ChangeTo(candidates[0]), p => p);
        Check(changed.Gid == candidates[0] && changed.TelegramId == profiles[0].TelegramId, "GID change retained identity");
        var difference = await backend.DifferenceAsync(session, 1);
        Check(difference.Kind == DifferenceKind.Complete && difference.Pts == 3 &&
            difference.Events.Select(e => e.Type).SequenceEqual(new[] { "message.created", "user.gid_changed" }),
            "durable mixed-event pts difference");
        Check((await backend.DifferenceAsync(bobSession, 2)).Events.Single().Type == "user.gid_changed",
            "GID recipients include conversation peers");
        Check((await backend.DifferenceAsync(session, 0)).Pts == 3 &&
            (await backend.DifferenceAsync(session, 3)).Kind == DifferenceKind.Empty &&
            (await backend.DifferenceAsync(session, 4)).Kind == DifferenceKind.TooLong, "difference boundaries");
        var eventPage = await backend.EventsAsync(session, 0, 1);
        Check(eventPage.Events.Length == 1 && eventPage.Cursor == eventPage.Events[0].Id, "event page cursor");
        Check((await backend.EventsAsync(session, eventPage.Cursor, 200)).Events.Single().Type == "user.gid_changed",
            "event paging recovery");
        await backend.ChangeGidAsync(session, ChangeTo(candidates[0]), p => p);
        Check((await backend.DifferenceAsync(session, 3)).Kind == DifferenceKind.Empty, "unchanged GID emitted an event");
        await ExpectApi(() => backend.ChangeGidAsync(bobSession, ChangeTo(candidates[0]), p => p), 409, "gid_taken");
        try
        {
            await backend.ChangeGidAsync<int>(session, ChangeTo(candidates[1]), _ => throw new PreparationFailure());
            throw new InvalidOperationException("Expected GID response failure");
        }
        catch (PreparationFailure) { assertions++; }
        Check((await backend.CurrentProfileAsync(session)).Gid == candidates[0] &&
            (await backend.DifferenceAsync(session, 3)).Kind == DifferenceKind.Empty, "GID response failure was not atomic");
        var bounded = new PostgresBackend(source, 2);
        await ExpectApi(() => bounded.ChangeGidAsync(session, ChangeTo(candidates[1]), p => p), 503, "storage_backlog_full");
        Check((await backend.CurrentProfileAsync(session)).Gid == candidates[0], "capacity rejection left changed GID");
        var delivery = new PostgresDeliveryPersistence(source);
        var leases = await delivery.ClaimAsync(100, TimeSpan.FromMinutes(1), default);
        var gidLease = leases.Single(l => l.Delivery.Type == "user.gid_changed");
        Check(gidLease.Delivery.Payload is GidChanged gid && gid.Gid == candidates[0] &&
            gidLease.Delivery.ConversationSequence is null && gidLease.Delivery.Recipients.All(r => r.Pts == 3),
            "general event source envelope");
        var scope = new ConsumerScope("backend-events", await delivery.GetActiveGenerationAsync("user:" + alice.User.Id));
        Check(await delivery.StageAsync(scope, gidLease.Delivery, new("isolated", 0, 10), default) == InboxStageResult.Staged,
            "durable general event inbox");
        Check((long)(await Scalar("SELECT count(*) FROM botgram_intake_v1.projection_tasks WHERE outbox_id=$1",
            gidLease.Delivery.OutboxId))! == 0 &&
            (long)(await Scalar("SELECT count(*) FROM botgram_intake_v1.delivery_positions WHERE outbox_id=$1",
            gidLease.Delivery.OutboxId))! == 1, "nonmessage inbox must persist position without fake projection");
        await ExpectApi(() => bounded.ChangeGidAsync(session, ChangeTo(candidates[1]), p => p), 503, "storage_backlog_full");
        Check(await delivery.ConfirmPublishedAsync(gidLease.Delivery.OutboxId, gidLease.LeaseToken,
            new(new("isolated", 0, 10)), default), "general publication confirmation");
        Check((await bounded.ChangeGidAsync(session, ChangeTo(candidates[1]), p => p)).Gid == candidates[1],
            "capacity did not release after durable publish and inbox");
        Check(await delivery.ReplayAsync(gidLease.Delivery.OutboxId, default), "general replay");
        await ExpectApi(() => bounded.ChangeGidAsync(session, ChangeTo(candidates[2]), p => p), 503, "storage_backlog_full");
        var replay = (await delivery.ClaimAsync(100, TimeSpan.FromMinutes(1), default))
            .Single(l => l.Delivery.OutboxId == gidLease.Delivery.OutboxId);
        Check(await delivery.StageAsync(scope, replay.Delivery, new("isolated", 0, 11), default) == InboxStageResult.Duplicate,
            "general replay inbox deduplication");
        Check(await delivery.ConfirmPublishedAsync(replay.Delivery.OutboxId, replay.LeaseToken,
            new(new("isolated", 0, 11)), default), "general replay confirmation");
        Check((await backend.DifferenceAsync(session, 4)).Kind == DifferenceKind.Empty &&
            (long)(await Scalar("SELECT count(*) FROM botgram_intake_v1.events"))! == 3,
            "broker replay changed business event or pts");
        var direct = await Task.WhenAll(actors.Select(actor =>
            backend.CreateConversationAsync(actor, new("direct", null, [bob.User.Id]))));
        Check(direct.Select(c => c.Id).Distinct().Count() == 1, "concurrent direct creation was not unique");
        Check((await backend.CreateConversationAsync(bobSession, new("direct", null, [alice.User.Id]))).Id == direct[0].Id,
            "reverse direct creation did not reuse pair");
        Check((await backend.ConversationsAsync(session)).Single().Id == direct[0].Id,
            "relational owned conversation read");
        Check((long)(await Scalar("SELECT count(*) FROM botgram_intake_v1.events WHERE type='conversation.created'"))! == 1 &&
            (await backend.DifferenceAsync(session, 4)).Kind == DifferenceKind.Empty, "conversation creation advanced pts or duplicated event");
        var conversationLease = (await delivery.ClaimAsync(100, TimeSpan.FromMinutes(1), default))
            .Single(l => l.Delivery.Type == "conversation.created");
        Check(conversationLease.Delivery.Recipients.All(r => r.Pts is null) &&
            conversationLease.Delivery.Payload is Conversation c && c.Members.SequenceEqual(direct[0].Members),
            "nonstream event recipients and member order");
        var conversationScope = new ConsumerScope("backend-events", await delivery.GetActiveGenerationAsync(direct[0].Id));
        Check(await delivery.StageAsync(conversationScope, conversationLease.Delivery, new("isolated", 1, 0), default)
            == InboxStageResult.Staged, "conversation event inbox");
        Check((await delivery.CheckpointAsync(direct[0].Id, conversationScope.Generation, default)).ThroughSequence == 0,
            "nonmessage inbox fabricated a message watermark");
        Check(await delivery.ConfirmPublishedAsync(conversationLease.Delivery.OutboxId, conversationLease.LeaseToken,
            new(new("isolated", 1, 0)), default), "conversation event publication");
        var beforeReject = (long)(await Scalar("SELECT count(*) FROM botgram_backend_v1.conversations"))!;
        await ExpectApi(() => new PostgresBackend(source, 1).CreateConversationAsync(session, new("group", "Rejected", [bob.User.Id])),
            503, "storage_backlog_full");
        Check((long)(await Scalar("SELECT count(*) FROM botgram_backend_v1.conversations"))! == beforeReject,
            "conversation capacity rejection left metadata");
        var group = await backend.CreateConversationAsync(session, new("group", " First group ", [bob.User.Id]));
        var group2 = await backend.CreateConversationAsync(session, new("group", "Second group", [bob.User.Id, third.Id]));
        Check(group.Title == "First group" && group.Members.Length == 2, "shared REST group creation rules");
        await Rpc(session, new() { GetGroups = new() });
        legacy.Conversations.AddRange(await backend.ConversationsAsync(session));
        legacy.Users[legacy.Users.FindIndex(u => u.Id == alice.User.Id)] =
            legacy.Users.Single(u => u.Id == alice.User.Id) with { Gid = candidates[1] };
        legacy.Contacts.Clear();
        await using (var contacts = source.CreateCommand("""
            SELECT owner_id,user_id,first_name,last_name FROM botgram_backend_v1.contacts ORDER BY ordinal
            """))
        await using (var reader = await contacts.ExecuteReaderAsync())
            while (await reader.ReadAsync())
                legacy.Contacts.Add(new(reader.GetString(0), reader.GetString(1), reader.GetString(2), reader.GetString(3)));
        legacy.TelegramIds.Clear();
        await using (var ids = source.CreateCommand("SELECT key,value FROM botgram_backend_v1.identities"))
        await using (var reader = await ids.ExecuteReaderAsync())
            while (await reader.ReadAsync()) legacy.TelegramIds.Add(reader.GetString(0), reader.GetInt64(1));
        await Parity(new() { GetGroups = new() { GroupIds = { group2.Id, group.Id } } });
        await Parity(new() { GetFullGroup = new() { GroupId = group.Id } });
        await Parity(new() { GetPeerSettings = new() { Peer = new() { GroupId = group.Id } } });
        await Parity(new() { GetPeerSettings = new() { Peer = new() { User = Select(bob.User.Id).User } } });
        await Parity(new() { GetPeerSettings = new() { Peer = new() { Self = true } } });
        await Parity(new() { GetNotifySettings = new() { Target = new() { Peer = new() { GroupId = group.Id } } } });
        await Parity(new() { GetNotifySettings = new() { Target = new() { AllUsers = new() } } });
        await Parity(new()
        {
            ResolveIdentities = new()
            {
                TlUserIds = { legacy.TelegramIds["user:" + bob.User.Id], legacy.TelegramIds["user:" + alice.User.Id] },
                TlChatIds = { legacy.TelegramIds["chat:" + group2.Id], legacy.TelegramIds["chat:" + group.Id] }
            }
        });
        var thirdLogin = await backend.LoginAsync(new("contended", "isolated-password"));
        var thirdSession = await backend.RequireAsync(Auth.HashToken(thirdLogin.Token));
        await ExpectApi(() => Rpc(thirdSession, new() { GetFullGroup = new() { GroupId = group.Id } }), 404, "PEER_ID_INVALID");
        await ExpectApi(() => Rpc(thirdSession, new()
            { ResolveIdentities = new() { TlChatIds = { legacy.TelegramIds["chat:" + group.Id] } } }),
            404, "PEER_ID_INVALID");
        var unmapped = await backend.RegisterAsync(new("unmapped", "Unmapped", "isolated-password"));
        var unmappedSession = await backend.RequireAsync(Auth.HashToken(unmapped.Token));
        await ExpectApi(() => Rpc(unmappedSession, new() { ResolveIdentities = new() }), 404, "SELF_IDENTITY_NOT_FOUND");
        Check(await Scalar("SELECT value FROM botgram_backend_v1.identities WHERE key=$1", "user:" + unmapped.User.Id) is null,
            "identity bootstrap allocated an absent mapping");
        Check(!(await backend.PremiumAsync(session)).Active && !(await backend.PremiumAsync(bobSession)).Active,
            "Premium must default to disabled");
        var eventsBeforePremium = (long)(await Scalar("SELECT count(*) FROM botgram_intake_v1.events"))!;
        var premium = await Task.WhenAll(actors.Select(actor => backend.PremiumAsync(actor, activate: true)));
        Check(premium.All(p => p.Active) && (await backend.PremiumAsync(session)).Active &&
            !(await backend.PremiumAsync(bobSession)).Active, "Premium activation idempotency and account isolation");
        await using (var reopened = NpgsqlDataSource.Create(connectionString))
            Check((await new PostgresBackend(reopened).PremiumAsync(session)).Active, "Premium durability after reopen");
        Check((long)(await Scalar("SELECT count(*) FROM botgram_intake_v1.events"))! == eventsBeforePremium,
            "Premium activation manufactured existing event types");
        var premiumUser = P.RpcResponse.Parser.ParseFrom(await Rpc(session,
            new() { GetFullUser = new() { User = new() { Self = true } } })).GetFullUser.Users.Single();
        Check(premiumUser.HasNyarugramPremium && premiumUser.NyarugramPremium, "relational Premium protobuf projection");
        var inactiveUser = P.RpcResponse.Parser.ParseFrom(await Rpc(bobSession,
            new() { GetFullUser = new() { User = new() { Self = true } } })).GetFullUser.Users.Single();
        Check(inactiveUser.HasNyarugramPremium && !inactiveUser.NyarugramPremium, "relational Premium false presence");
        await backend.ChangeGidAsync(bobSession, ChangeTo(candidates[2]), p => p);
        using (var swapDeadline = new CancellationTokenSource(TimeSpan.FromSeconds(10)))
        {
            var swaps = await Task.WhenAll(new[] { (Actor: session, Target: candidates[2]), (Actor: bobSession, Target: candidates[1]) }
                .Select(async swap =>
                {
                    try { await backend.ChangeGidAsync(swap.Actor, ChangeTo(swap.Target), p => p, swapDeadline.Token); return false; }
                    catch (ApiException exception) when (exception.Status == 409 && exception.Message == "gid_taken") { return true; }
                }));
            Check(swaps.All(rejected => rejected), "reciprocal GID conflicts must reject without deadlock");
        }
        assertions += await RelationalReadTests.RunAsync(source);
        assertions += await RelationalWriteTests.RunAsync(source);
        assertions += await RelationalRestTests.RunAsync(source);
        assertions += await RelationalWallpaperTests.RunAsync(source, connectionString);
        assertions += await RelationalPrivacyTests.RunAsync(source);
        Console.WriteLine($"PASS: {assertions} relational backend assertions; real PG, not full RPC or four-component acceptance");
    }

    private sealed class PreparationFailure : Exception;
}
