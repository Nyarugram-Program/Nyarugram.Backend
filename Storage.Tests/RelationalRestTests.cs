using Botgram.Backend;
using Botgram.Backend.Storage;
using Npgsql;

static class RelationalRestTests
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
        async Task Reject(Func<Task> action, int status, string code)
        {
            try { await action(); }
            catch (ApiException exception) when (exception.Status == status && exception.Message == code)
            { assertions++; return; }
            throw new InvalidOperationException($"Expected {status}:{code}");
        }
        var backend = new PostgresBackend(source);
        async Task<Session> Register(string name) =>
            await backend.RequireAsync(Auth.HashToken((await backend.RegisterAsync(new(name, name, "isolated-password"))).Token));
        var alice = await Register("rest_alice");
        var bob = await Register("rest_bob");
        var outside = await Register("rest_outside");
        var identities = (long)(await Scalar("SELECT count(*) FROM botgram_backend_v1.identities"))!;
        Check((await backend.SearchUsersAsync(alice, "REST_BOB")).Single().Id == bob.UserId, "REST exact username");
        await Reject(() => backend.SearchUsersAsync(alice, "re"), 400, "exact_username_required");
        var direct = await backend.CreateConversationAsync(alice, new("direct", null, [bob.UserId]));
        var otherDirect = await backend.CreateConversationAsync(bob, new("direct", null, [outside.UserId]));
        var message = await backend.SendRestAsync(alice, direct.Id, new("rest-id", "text"), m => m);
        var retry = await backend.SendRestAsync(alice, direct.Id, new("rest-id", "text"), m => m);
        Check(message.Id == retry.Id, "REST idempotency");
        await Reject(() => backend.SendRestAsync(alice, direct.Id, new("rest-id", "conflict"), m => m), 409, "idempotency_conflict");
        await Reject(() => backend.SendRestAsync(outside, direct.Id, new("bad", "text"), m => m), 404, "conversation_not_found");
        await Reject(() => backend.SendRestAsync(alice, direct.Id, new("", "text"), m => m), 400, "invalid_client_message_id");
        await Reject(() => backend.HistoryAsync(alice, direct.Id, 0, null), 400, "invalid_pagination");
        var second = await backend.SendRestAsync(bob, direct.Id, new("rest-other", "second"), m => m);
        Check((await backend.HistoryAsync(alice, direct.Id, null, null)).Select(m => m.Id).SequenceEqual(new[] { message.Id, second.Id }),
            "REST chronological history");
        Check((await backend.HistoryAsync(alice, direct.Id, second.Id, 1)).Single().Id == message.Id, "REST before/limit");
        var list = await backend.ConversationListAsync(alice);
        Check(list.Single().UnreadCount == 1 && list[0].LastMessage?.Id == second.Id &&
            list[0].Users.Select(u => u.Id).SequenceEqual(new[] { alice.UserId, bob.UserId }), "REST conversation summary");
        await Reject(() => backend.ReadExactAsync(alice, direct.Id, long.MaxValue), 400, "message_not_found");
        Check((await backend.ReadExactAsync(alice, direct.Id, second.Id)).MessageId == second.Id &&
            (await backend.ReadExactAsync(alice, direct.Id, message.Id)).MessageId == second.Id, "REST read cursor regressed");
        Check((await backend.ConversationListAsync(alice)).Single().UnreadCount == 0 &&
            (long)(await Scalar("SELECT count(*) FROM botgram_backend_v1.identities"))! == identities,
            "REST reads allocated unnecessary TL identities or ignored receipt");
        var pts = (await backend.DifferenceAsync(alice, 1)).Pts;
        var actors = await Task.WhenAll(Enumerable.Range(0, 8).Select(async _ =>
            await backend.RequireAsync(Auth.HashToken((await backend.LoginAsync(new("rest_alice", "isolated-password"))).Token))));
        using (var deadline = new CancellationTokenSource(TimeSpan.FromSeconds(15)))
        {
            var attempts = await Task.WhenAll(actors.Select(async actor =>
            {
                try { return await backend.CreateCallAsync(actor, new(direct.Id, bob.UserId), deadline.Token); }
                catch (ApiException exception) when (exception.Status == 409 && exception.Message == "user_busy") { return null; }
            }));
            Check(attempts.Count(c => c is not null) == 1, "parallel calls violated account busy invariant");
        }
        var call = (await backend.CallsAsync(alice)).Single();
        await Reject(() => backend.CreateCallAsync(outside, new(otherDirect.Id, bob.UserId)), 409, "user_busy");
        await Reject(() => backend.CallAsync(outside, call.Id), 404, "call_not_found");
        await Reject(() => backend.ChangeCallAsync(alice, call.Id, "accept"), 403, "callee_only");
        await Reject(() => backend.SignalAsync(alice, call.Id, new("offer", "sdp")), 409, "call_not_active");
        var bounded = new PostgresBackend(source, 1);
        await Reject(() => bounded.ChangeCallAsync(bob, call.Id, "accept"), 503, "storage_backlog_full");
        Check((await backend.CallAsync(bob, call.Id)).State == "ringing", "call capacity rollback");
        Check((await backend.ChangeCallAsync(bob, call.Id, "accept")).State == "active", "callee accept");
        await Reject(() => backend.ChangeCallAsync(bob, call.Id, "accept"), 409, "call_not_ringing");
        await Reject(() => backend.SignalAsync(bob, call.Id, new("offer", "sdp")), 400, "caller_offers_only");
        await Reject(() => backend.SignalAsync(alice, call.Id, new("answer", "sdp")), 400, "callee_answers_only");
        await Reject(() => backend.SignalAsync(alice, call.Id, new("invalid", "sdp")), 400, "invalid_signal_type");
        await Reject(() => backend.SignalAsync(alice, call.Id, new("ice", "")), 400, "invalid_signal_data");
        var cursor = (await backend.EventsAsync(alice, 0, 200)).Cursor;
        await backend.SignalAsync(alice, call.Id, new("offer", "synthetic-sdp"));
        Check((await backend.EventsAsync(alice, cursor, 200)).Events.All(e => e.Type != "call.signal"),
            "signal sent back to sender");
        var signal = (await backend.EventsAsync(bob, cursor, 200)).Events.Single(e => e.Type == "call.signal");
        Check(signal.Payload is CallSignal s && s.CallId == call.Id && s.Data == "synthetic-sdp", "signal recipient payload");
        await Scalar("UPDATE botgram_intake_v1.events SET expires_at=clock_timestamp()-interval '1 second' WHERE id=$1", signal.Id);
        var expiredPage = await backend.EventsAsync(bob, cursor, 200);
        Check(expiredPage.Events.Length == 0 && expiredPage.Cursor >= signal.Id, "expired signal blocked event cursor");
        Check((await backend.DifferenceAsync(alice, 1)).Pts == pts, "calls/signals advanced message pts");
        Check((await backend.ChangeCallAsync(alice, call.Id, "end")).State == "ended", "caller end");
        var eventCount = (long)(await Scalar("SELECT count(*) FROM botgram_intake_v1.events"))!;
        await backend.ChangeCallAsync(bob, call.Id, "end");
        Check((long)(await Scalar("SELECT count(*) FROM botgram_intake_v1.events"))! == eventCount, "terminal call repeated event");
        var expiring = await backend.CreateCallAsync(alice, new(direct.Id, bob.UserId));
        await Scalar("UPDATE botgram_backend_v1.calls SET updated_at=clock_timestamp()-interval '61 seconds' WHERE id=$1", expiring.Id);
        await backend.MaintainAsync();
        Check((await backend.CallAsync(alice, expiring.Id)).State == "ended" && (await backend.CallsAsync(alice)).Length == 0,
            "ringing call expiry");
        var activeExpiry = await backend.CreateCallAsync(alice, new(direct.Id, bob.UserId));
        await backend.ChangeCallAsync(bob, activeExpiry.Id, "accept");
        await Scalar("UPDATE botgram_backend_v1.calls SET updated_at=clock_timestamp()-interval '121 minutes' WHERE id=$1", activeExpiry.Id);
        Check((await backend.CallAsync(bob, activeExpiry.Id)).State == "ended", "active call expiry before request");
        await Scalar("UPDATE botgram_intake_v1.sessions SET expires_at=clock_timestamp()-interval '1 second' WHERE token_hash=$1", actors[0].TokenHash);
        await using (var blocker = await source.OpenConnectionAsync())
        await using (var transaction = await blocker.BeginTransactionAsync())
        {
            await using var command = new NpgsqlCommand("SELECT user_id FROM botgram_intake_v1.sessions WHERE token_hash=$1 FOR UPDATE", blocker, transaction);
            command.Parameters.AddWithValue(actors[0].TokenHash);
            await command.ExecuteScalarAsync();
            using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            await backend.MaintainAsync(timeout.Token);
            Check(await Scalar("SELECT user_id FROM botgram_intake_v1.sessions WHERE token_hash=$1", actors[0].TokenHash) is string,
                "maintenance did not skip locked expired session");
            await transaction.RollbackAsync();
        }
        await backend.MaintainAsync();
        Check(await Scalar("SELECT user_id FROM botgram_intake_v1.sessions WHERE token_hash=$1", actors[0].TokenHash) is null,
            "expired session cleanup");
        await backend.LogoutAsync(alice);
        await Reject(() => backend.CreateCallAsync(alice, new(direct.Id, bob.UserId)), 401, "session_expired");
        return assertions;
    }
}
