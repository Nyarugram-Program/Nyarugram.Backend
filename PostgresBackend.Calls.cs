namespace Botgram.Backend.Storage;

public sealed partial class PostgresBackend
{
    private const string CallRows = """
        SELECT id,conversation_id,caller_id,callee_id,state,updated_at FROM botgram_backend_v1.calls
        """;
    private static Call CallRow(Npgsql.NpgsqlDataReader r) =>
        new(r.GetString(0), r.GetString(1), r.GetString(2), r.GetString(3), r.GetString(4), r.GetFieldValue<DateTimeOffset>(5));

    private async Task ExpireCalls(Transaction work)
    {
        // All call mutations use this namespace before taking capacity/recipient/event locks.
        await work.Scalar("SELECT pg_advisory_xact_lock(2147000003,0)");
        var expired = await work.Rows(CallRows + """
             WHERE (state='ringing' AND updated_at<clock_timestamp()-interval '60 seconds')
                OR (state='active' AND updated_at<clock_timestamp()-interval '2 hours') ORDER BY ordinal
            """, CallRow);
        foreach (var call in expired)
            await SaveCall(work, call with { State = "ended", UpdatedAt = DateTimeOffset.UtcNow });
    }

    private async Task SaveCall(Transaction work, Call call)
    {
        await Emit(work, "call.updated", call, call.ConversationId, [call.CallerId, call.CalleeId]);
        await work.Execute("""
            UPDATE botgram_backend_v1.calls SET state=$2,updated_at=$3 WHERE id=$1
            """, call.Id, call.State, call.UpdatedAt);
    }

    private static async Task<Call> FindCall(Transaction work, string uid, string id)
    {
        var call = (await work.Rows(CallRows + " WHERE id=$1 AND (caller_id=$2 OR callee_id=$2)", CallRow, id, uid))
            .SingleOrDefault() ?? throw new ApiException(404, "call_not_found");
        await ConversationById(work, uid, call.ConversationId);
        return call;
    }

    public Task<Call> CreateCallAsync(Session session, CallInput input, CancellationToken token = default) =>
        WriteAsync(session, async work =>
        {
            await ExpireCalls(work);
            var conversation = await ConversationById(work, session.UserId, input.ConversationId);
            CallService.ValidateCallee(conversation, session.UserId, input.CalleeId);
            var privacy = await PostgresPrivacyPolicy.Load(work.Connection, work.NativeTransaction, [input.CalleeId], session.UserId, work.Token);
            if (!AccountPrivacyService.Allowed(privacy, input.CalleeId, session.UserId, Botgram.Protocol.V1.PrivacyKey.PhoneCall))
                throw new ApiException(403, "USER_PRIVACY_RESTRICTED");
            if (await work.Scalar("""
                SELECT 1 FROM botgram_backend_v1.calls WHERE state IN ('ringing','active')
                    AND (caller_id=ANY($1) OR callee_id=ANY($1)) LIMIT 1
                """, (object)new[] { session.UserId, input.CalleeId }) is not null)
                throw new ApiException(409, "user_busy");
            var call = new Call(Guid.NewGuid().ToString("N"), conversation.Id, session.UserId, input.CalleeId,
                "ringing", DateTimeOffset.UtcNow);
            await Emit(work, "call.updated", call, conversation.Id, conversation.Members);
            await work.Execute("""
                INSERT INTO botgram_backend_v1.calls(id,conversation_id,caller_id,callee_id,state,updated_at)
                VALUES($1,$2,$3,$4,$5,$6)
                """, call.Id, call.ConversationId, call.CallerId, call.CalleeId, call.State, call.UpdatedAt);
            return call;
        }, token);

    public Task<Call[]> CallsAsync(Session session, CancellationToken token = default) =>
        WriteAsync(session, async work =>
        {
            await ExpireCalls(work);
            var calls = await work.Rows(CallRows + """
                 WHERE (caller_id=$1 OR callee_id=$1) AND state IN ('ringing','active') ORDER BY ordinal
                """, CallRow, session.UserId);
            foreach (var call in calls) await ConversationById(work, session.UserId, call.ConversationId);
            return calls.ToArray();
        }, token);

    public Task<Call> CallAsync(Session session, string id, CancellationToken token = default) =>
        WriteAsync(session, async work =>
        {
            await ExpireCalls(work);
            return await FindCall(work, session.UserId, id);
        }, token);

    public Task<Call> ChangeCallAsync(Session session, string id, string action, CancellationToken token = default) =>
        WriteAsync(session, async work =>
        {
            await ExpireCalls(work);
            var previous = await FindCall(work, session.UserId, id);
            var updated = CallService.Transition(previous, session.UserId, action, DateTimeOffset.UtcNow);
            if (updated != previous) await SaveCall(work, updated);
            return updated;
        }, token);

    public Task SignalAsync(Session session, string id, SignalInput input, CancellationToken token = default) =>
        WriteAsync(session, async work =>
        {
            await ExpireCalls(work);
            var call = await FindCall(work, session.UserId, id);
            var recipient = CallService.SignalRecipient(call, session.UserId, input);
            await Emit(work, "call.signal", new CallSignal(id, session.UserId, input.Type, input.Data),
                call.ConversationId, [recipient], DateTimeOffset.UtcNow.AddMinutes(5));
            return true;
        }, token);

    public Task MaintainAsync(CancellationToken token = default) =>
        WriteAsync(null, async work =>
        {
            await ExpireCalls(work);
            // Streams/Outbox retain their immutable source. Notification reads filter expiry instead.
            await work.Execute("""
                DELETE FROM botgram_intake_v1.sessions WHERE token_hash IN (
                    SELECT token_hash FROM botgram_intake_v1.sessions WHERE expires_at<=clock_timestamp()
                    FOR UPDATE SKIP LOCKED)
                """);
            return true;
        }, token);
}
