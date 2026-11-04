using System.Diagnostics;
using Botgram.Backend;
using Botgram.Backend.Storage;
using Google.Protobuf;
using Npgsql;
using P = Botgram.Protocol.V1;

static class RelationalWriteTests
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
        var owner = await backend.RegisterAsync(new("write_owner", "Writer", "isolated-password"));
        var other = await backend.RegisterAsync(new("write_other", "Other", "isolated-password"));
        var outsider = await backend.RegisterAsync(new("write_outside", "Outside", "isolated-password"));
        var session = await backend.RequireAsync(Auth.HashToken(owner.Token));
        var otherSession = await backend.RequireAsync(Auth.HashToken(other.Token));
        var outsideSession = await backend.RequireAsync(Auth.HashToken(outsider.Token));
        const string domain = "write.test";
        async Task<P.RpcResponse> Rpc(P.RpcRequest request, Session? actor = null) =>
            P.RpcResponse.Parser.ParseFrom(await backend.DispatchAsync(actor ?? session, request, domain, r => r.ToByteArray()));
        P.PeerRef UserPeer(string id) => new() { User = new() { UserId = id, ServerDomain = domain } };
        var create = new P.RpcRequest { CreateGroup = new() { Title = " Group title ",
            Users = { new P.UserSelector { User = UserPeer(other.User.Id).User } } } };
        async Task<string> Counts() => (string)(await Scalar("""
            SELECT concat((SELECT count(*) FROM botgram_backend_v1.conversations),':',
                (SELECT count(*) FROM botgram_intake_v1.messages),':',
                (SELECT count(*) FROM botgram_intake_v1.events),':',
                (SELECT count(*) FROM botgram_intake_v1.outbox),':',
                (SELECT sum(pts) FROM botgram_intake_v1.sync),':',
                (SELECT value FROM botgram_intake_v1.event_cursor))
            """))!;
        async Task Rollback(P.RpcRequest request)
        {
            var before = await Counts();
            try
            {
                await backend.DispatchAsync<int>(session, request, domain, _ => throw new PreparationFailure());
                throw new InvalidOperationException("Expected write preparation failure");
            }
            catch (PreparationFailure) { assertions++; }
            Check(await Counts() == before, "write response failure leaked state: " + request.OperationCase);
        }
        await Rollback(create);
        var backlog = (long)(await Scalar("""
            SELECT count(*) FROM botgram_intake_v1.outbox o WHERE o.state<>'Published'
                OR (o.projection_version IS NULL AND NOT o.consumed)
                OR (o.projection_version IS NOT NULL AND NOT EXISTS(
                    SELECT 1 FROM botgram_intake_v1.projection_tasks t
                    JOIN botgram_intake_v1.projection_heads h ON h.conversation_id=t.conversation_id AND h.generation=t.generation
                    WHERE t.outbox_id=o.id AND t.state='Complete'))
            """))!;
        var counts = await Counts();
        await Reject(() => new PostgresBackend(source, checked((int)backlog + 1))
            .DispatchAsync(session, create, domain, r => r.ToByteArray()), 503, "storage_backlog_full");
        Check(await Counts() == counts, "second group event capacity failure did not roll back first event");
        var created = (await Rpc(create)).CreateGroup;
        var group = created.Group.GroupId;
        Check(created.Group.Title == " Group title " && created.Group.ParticipantCount == 2 &&
            created.Updates.Groups.Single().Equals(created.Group), "group response semantics");
        var service = created.Updates.Updates_.Single().NewMessage;
        Check(service.Pts == 2 && service.PtsCount == 1 && service.Message.Service.GroupCreated.Title == " Group title " &&
            service.Message.Service.GroupCreated.Members.Select(m => m.UserId).SequenceEqual(new[] { owner.User.Id, other.User.Id }.Order()) &&
            !service.Message.Unread, "group service contents/pts");
        Check((long)(await Scalar("SELECT sequence FROM botgram_intake_v1.conversations WHERE id=$1", group))! == 1 &&
            (long)(await Scalar("SELECT count(*) FROM botgram_intake_v1.outbox WHERE partition_key=$1", group))! == 2,
            "group did not commit conversation and service outbox atomically");
        P.RpcRequest Send(long randomId, string text, P.PeerRef? peer = null) => new()
        {
            SendMessage = new() { Peer = peer ?? new() { GroupId = group }, RandomId = randomId, Text = text }
        };
        var first = Send(long.MinValue, "hello bold");
        first.SendMessage.Entities.Add(new P.MessageEntity { Bold = new(), OffsetUtf16 = 6, LengthUtf16 = 4 });
        first.SendMessage.ReplyToMessageId = service.Message.MessageId;
        first.SendMessage.Silent = true;
        await Rollback(first);
        var actors = await Task.WhenAll(Enumerable.Range(0, 8).Select(async _ =>
            await backend.RequireAsync(Auth.HashToken((await backend.LoginAsync(new("write_owner", "isolated-password"))).Token))));
        using (var deadline = new CancellationTokenSource(TimeSpan.FromSeconds(15)))
        {
            var results = await Task.WhenAll(actors.Select(actor =>
                backend.DispatchAsync(actor, first, domain, r => r.SendMessage, deadline.Token)));
            Check(results.Select(r => r.Message.MessageId).Distinct().Count() == 1 &&
                results.All(r => r.Updates.Updates_[1].NewMessage.Pts == 3), "concurrent random_id allocated duplicates");
            var result = results[0];
            Check(result.Mapping.RandomId == long.MinValue && result.Mapping.MessageId == result.Message.MessageId &&
                result.Message.Text.Reply.MessageId == service.Message.MessageId && result.Message.Text.Silent &&
                result.Message.Text.Entities.Single().Bold is not null &&
                result.Updates.Updates_[0].MessageId.Equals(result.Mapping) &&
                result.Updates.Updates_[1].NewMessage.Message.Equals(result.Message), "send transport mapping lost options");
        }
        Check((long)(await Scalar("SELECT sequence FROM botgram_intake_v1.conversations WHERE id=$1", group))! == 2 &&
            (long)(await Scalar("SELECT count(*) FROM botgram_intake_v1.messages WHERE conversation_id=$1", group))! == 2,
            "concurrent retry advanced sequence");
        counts = await Counts();
        await Rpc(first);
        Check(await Counts() == counts, "lost successful response retry changed authoritative state");
        await Reject(() => Rpc(Send(long.MinValue, "conflict")), 409, "RANDOM_ID_DUPLICATE");
        await Reject(() => Rpc(first, outsideSession), 404, "PEER_ID_INVALID");
        await Reject(() => Rpc(Send(2, "")), 400, "MESSAGE_EMPTY");
        var invalidReply = Send(2, "wrong reply");
        invalidReply.SendMessage.ReplyToMessageId = int.MaxValue;
        await Reject(() => Rpc(invalidReply), 400, "REPLY_MESSAGE_ID_INVALID");
        Check(await Counts() == counts, "send rejection changed authoritative state");
        var implicitSend = Send(123, "new direct", UserPeer(outsider.User.Id));
        await Rollback(implicitSend);
        Check(!(await backend.ConversationsAsync(session)).Any(c => c.Kind == "direct"), "implicit direct creation escaped rollback");
        var directSend = (await Rpc(implicitSend)).SendMessage;
        Check(directSend.Message.Peer.User.UserId == outsider.User.Id &&
            (await backend.ConversationsAsync(session)).Count(c => c.Kind == "direct") == 1,
            "implicit direct acceptance missing");
        var crossReply = Send(9, "cross conversation");
        crossReply.SendMessage.ReplyToMessageId = directSend.Message.MessageId;
        await Reject(() => Rpc(crossReply), 400, "REPLY_MESSAGE_ID_INVALID");
        await using (var blocker = await source.OpenConnectionAsync())
        await using (var transaction = await blocker.BeginTransactionAsync())
        {
            await using (var command = new NpgsqlCommand("SELECT pg_advisory_xact_lock(7459241304231)", blocker, transaction))
                await command.ExecuteNonQueryAsync();
            counts = await Counts();
            using var cancel = new CancellationTokenSource(TimeSpan.FromSeconds(10));
            var waiting = backend.DispatchAsync(session, Send(999, "cancelled"), domain, r => r.ToByteArray(), cancel.Token);
            var watch = Stopwatch.StartNew();
            while (!(bool)(await Scalar("""
                SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE datname=current_database()
                    AND wait_event_type='Lock' AND query='SELECT pg_advisory_xact_lock(7459241304231)')
                """))!)
            {
                if (watch.Elapsed > TimeSpan.FromSeconds(5)) throw new TimeoutException("Acceptance did not reach capacity lock");
                await Task.Delay(10);
            }
            cancel.Cancel();
            try { await waiting; throw new InvalidOperationException("Expected cancellation"); }
            catch (OperationCanceledException) { assertions++; }
            Check(await Counts() == counts, "cancelled RPC acceptance changed state");
            await transaction.RollbackAsync();
        }
        Check((await Rpc(Send(999, "cancelled"))).SendMessage.Message.Text.Text == "cancelled",
            "send did not recover after cancelled lock wait");
        var otherDifference = (await Rpc(new() { GetDifference = new() { Pts = 1 } }, otherSession)).GetDifference.Complete;
        Check(otherDifference.Messages[0].Service.GroupCreated.Title == created.Group.Title &&
            otherDifference.Messages[0].Unread == false &&
            otherDifference.Messages.Skip(1).All(m => m.Unread), "group service/text difference semantics");
        await backend.LogoutAsync(session);
        await Reject(() => Rpc(Send(777, "revoked")), 401, "session_expired");
        return assertions;
    }

    private sealed class PreparationFailure : Exception;
}
