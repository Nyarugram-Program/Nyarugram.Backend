using System.Security.Cryptography;
using System.Text.Json.Nodes;
using Google.Protobuf;
using P = Botgram.Protocol.V1;

namespace Botgram.Backend.Storage;

public sealed partial class PostgresBackend
{
    internal const string CallRows = """
        SELECT id,conversation_id,caller_id,callee_id,state,updated_at,
            telegram_id,access_hash,random_id,video,protocol::text,request_protocol::text,accept_protocol::text,
            g_a_hash,g_b,g_a,key_fingerprint,
            date,receive_date,start_date,reason,duration,connections::text,p2p_allowed
        FROM botgram_backend_v1.calls
        """;

    internal static Call CallRow(Npgsql.NpgsqlDataReader r)
    {
        static JsonObject? Protocol(string? text) => text is null ? null : (JsonObject)JsonNode.Parse(text)!;
        static JsonObject[]? Connections(string? text) => text is null ? null :
            JsonNode.Parse(text)!.AsArray().Select(node => (JsonObject)node!.DeepClone()).ToArray();
        static byte[]? Bytes(Npgsql.NpgsqlDataReader reader, int index) =>
            reader.IsDBNull(index) ? null : reader.GetFieldValue<byte[]>(index);
        return new(r.GetString(0), r.GetString(1), r.GetString(2), r.GetString(3), r.GetString(4),
            r.GetFieldValue<DateTimeOffset>(5), r.GetInt64(6), r.GetInt64(7), r.GetInt64(8), r.GetBoolean(9),
            Protocol(r.IsDBNull(10) ? null : r.GetString(10)),
            Bytes(r, 13), Bytes(r, 14), Bytes(r, 15), r.GetInt64(16),
            r.GetInt32(17), r.GetInt32(18), r.GetInt32(19), r.GetString(20), r.GetInt32(21),
            Connections(r.IsDBNull(22) ? null : r.GetString(22)), r.GetBoolean(23),
            RequestProtocol: Protocol(r.IsDBNull(11) ? null : r.GetString(11)),
            AcceptProtocol: Protocol(r.IsDBNull(12) ? null : r.GetString(12)));
    }

    private static object DbJson(JsonNode? node) => node is null ? DBNull.Value : node.ToJsonString();

    private static object DbConnections(JsonObject[]? connections) =>
        connections is null ? DBNull.Value : new JsonArray(connections.Select(c => c.DeepClone()).ToArray()).ToJsonString();

    private const string CallInsert = """
        INSERT INTO botgram_backend_v1.calls(id,conversation_id,caller_id,callee_id,state,updated_at,
            telegram_id,access_hash,random_id,video,protocol,request_protocol,accept_protocol,
            g_a_hash,g_b,g_a,key_fingerprint,
            date,receive_date,start_date,reason,duration,connections,p2p_allowed)
        VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11::jsonb,$12::jsonb,$13::jsonb,
            $14,$15,$16,$17,$18,$19,$20,$21,$22,$23::jsonb,$24)
        """;

    private const string CallUpdate = """
        UPDATE botgram_backend_v1.calls SET state=$2,updated_at=$3,telegram_id=$4,access_hash=$5,random_id=$6,
            video=$7,protocol=$8::jsonb,request_protocol=$9::jsonb,accept_protocol=$10::jsonb,
            g_a_hash=$11,g_b=$12,g_a=$13,key_fingerprint=$14,
            date=$15,receive_date=$16,start_date=$17,reason=$18,duration=$19,connections=$20::jsonb,p2p_allowed=$21
        WHERE id=$1
        """;

    private static object[] CallValues(Call call) =>
    [
        call.Id, call.ConversationId, call.CallerId, call.CalleeId, call.State, call.UpdatedAt,
        call.TelegramId, call.AccessHash, call.RandomId, call.Video, DbJson(call.Protocol),
        DbJson(call.RequestProtocol), DbJson(call.AcceptProtocol),
        (object?)call.GAHash ?? DBNull.Value, (object?)call.GB ?? DBNull.Value, (object?)call.GA ?? DBNull.Value,
        call.KeyFingerprint, call.Date, call.ReceiveDate, call.StartDate, call.Reason, call.Duration,
        DbConnections(call.Connections), call.P2pAllowed
    ];

    private async Task InsertCall(Transaction work, Call call, string[] recipients)
    {
        await Emit(work, "call.updated", call, call.ConversationId, recipients);
        await work.Execute(CallInsert, CallValues(call));
    }

    private async Task SaveCall(Transaction work, Call call, string[]? recipients = null)
    {
        await Emit(work, "call.updated", call, call.ConversationId, recipients ?? [call.CallerId, call.CalleeId]);
        var values = CallValues(call);
        await work.Execute(CallUpdate, [values[0], .. values.Skip(4)]);
    }

    private async Task ExpireCalls(Transaction work)
    {
        // All call mutations use this namespace before taking capacity/recipient/event locks.
        await work.Scalar("SELECT pg_advisory_xact_lock(2147000003,0)");
        var expired = await work.Rows(CallRows + """
             WHERE (state='ringing' AND updated_at<clock_timestamp()-interval '60 seconds')
                OR (state='accepted' AND updated_at<clock_timestamp()-interval '60 seconds')
                OR (state='active' AND updated_at<clock_timestamp()-interval '2 hours') ORDER BY ordinal
            """, CallRow);
        foreach (var call in expired)
            await SaveCall(work, call with { State = "ended", UpdatedAt = DateTimeOffset.UtcNow });
    }

    private static async Task<Call> FindCall(Transaction work, string uid, string id)
    {
        var call = (await work.Rows(CallRows + " WHERE id=$1 AND (caller_id=$2 OR callee_id=$2)", CallRow, id, uid))
            .SingleOrDefault() ?? throw new ApiException(404, "call_not_found");
        await ConversationById(work, uid, call.ConversationId);
        return call;
    }

    private static async Task<Call> RequireCall(Transaction work, string uid, P.PhoneCallRef? reference)
    {
        if (reference is null) throw new ApiException(400, "CALL_NOT_FOUND");
        var call = (await work.Rows(CallRows + " WHERE telegram_id=$1", CallRow, reference.CallId)).SingleOrDefault();
        if (call is null || call.AccessHash != reference.AccessHash || (call.CallerId != uid && call.CalleeId != uid))
            throw new ApiException(400, "CALL_NOT_FOUND");
        return call;
    }

    private static int Seconds(DateTimeOffset now) => (int)now.ToUnixTimeSeconds();

    private static async Task<P.PhoneCallResult> PhoneResult(Transaction work, string uid, string domain, Call call)
    {
        var ids = await UserIdentities(work, [call.CallerId, call.CalleeId]);
        string other = call.CallerId == uid ? call.CalleeId : call.CallerId;
        var profile = (await Profiles(work, uid, "a.id=$2", other)).Single();
        var result = new P.PhoneCallResult
        {
            PhoneCall = ProtobufRpcMapper.MapPhoneCall(call, ids[call.CallerId], ids[call.CalleeId], call.CallerId == uid)
        };
        result.Users.Add(ProtobufRpcMapper.MapUser(profile, domain));
        return result;
    }

    private static P.Updates PhoneUpdates(Call call, long callerTlId, long calleeTlId, bool callerView, string domain)
    {
        var updates = new P.Updates { DateMs = DateTimeOffset.UtcNow.ToUnixTimeSeconds() * 1000 };
        updates.Updates_.Add(new P.Update
        {
            PhoneCall = new() { PhoneCall = ProtobufRpcMapper.MapPhoneCall(call, callerTlId, calleeTlId, callerView) }
        });
        return updates;
    }

    public Task<Call> CreateCallAsync(Session session, CallInput input, CancellationToken token = default) =>
        WriteAsync(session, async work =>
        {
            await ExpireCalls(work);
            var conversation = await ConversationById(work, session.UserId, input.ConversationId);
            CallService.ValidateCallee(conversation, session.UserId, input.CalleeId);
            var privacy = await PostgresPrivacyPolicy.Load(work.Connection, work.NativeTransaction, [input.CalleeId], session.UserId, work.Token);
            if (!AccountPrivacyService.Allowed(privacy, input.CalleeId, session.UserId, P.PrivacyKey.PhoneCall))
                throw new ApiException(403, "USER_PRIVACY_RESTRICTED");
            if (await work.Scalar("""
                SELECT 1 FROM botgram_backend_v1.calls WHERE state IN ('ringing','accepted','active')
                    AND (caller_id=ANY($1) OR callee_id=ANY($1)) LIMIT 1
                """, (object)new[] { session.UserId, input.CalleeId }) is not null)
                throw new ApiException(409, "user_busy");
            var now = DateTimeOffset.UtcNow;
            var call = new Call(Guid.NewGuid().ToString("N"), conversation.Id, session.UserId, input.CalleeId,
                "ringing", now, TelegramId: (long)(await work.Scalar(
                    "SELECT nextval('botgram_backend_v1.call_identity')"))!, AccessHash: CallService.NewAccessHash(),
                Date: Seconds(now));
            await InsertCall(work, call, conversation.Members);
            return call;
        }, token);

    public Task<Call[]> CallsAsync(Session session, CancellationToken token = default) =>
        WriteAsync(session, async work =>
        {
            await ExpireCalls(work);
            var calls = await work.Rows(CallRows + """
                 WHERE (caller_id=$1 OR callee_id=$1) AND state IN ('ringing','accepted','active') ORDER BY ordinal
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

    // ---- Telegram phone.* operations over the protobuf transport ----

    private async Task<P.PhoneCallResult> PhoneRequestCall(Transaction work, Session session,
        P.PhoneRequestCallRequest input, string domain)
    {
        var uid = session.UserId;
        var callee = ProtobufRpcMapper.SelectedUserId(input.User, uid, domain);
        if (callee == uid) throw new ApiException(400, "CALL_PEER_INVALID");
        if (await work.Scalar("SELECT 1 FROM botgram_backend_v1.accounts WHERE id=$1", callee) is null)
            throw new ApiException(404, "USER_ID_INVALID");
        byte[] gaHash = input.GAHash.ToByteArray();
        var protocol = ProtobufRpcMapper.MapProtocolJson(input.Protocol);
        var replay = (await work.Rows(CallRows + """
             WHERE caller_id=$1 AND random_id=$2 AND random_id<>0 ORDER BY ordinal LIMIT 1
            """, CallRow, uid, input.RandomId)).SingleOrDefault();
        if (replay is not null)
        {
            if (replay.CalleeId != callee || replay.Video != input.Video ||
                !CryptographicOperations.FixedTimeEquals(replay.GAHash ?? [], gaHash) ||
                !JsonNode.DeepEquals(replay.RequestProtocol ?? replay.Protocol, protocol))
                throw new ApiException(409, "CALL_IDEMPOTENCY_CONFLICT");
            return await PhoneResult(work, uid, domain, replay);
        }
        CallService.ValidateGaHash(gaHash);
        CallService.ValidateProtocol(protocol);
        var calleeRules = await PostgresPrivacyPolicy.Load(work.Connection, work.NativeTransaction, [callee], uid, work.Token);
        if (!AccountPrivacyService.Allowed(calleeRules, callee, uid, P.PrivacyKey.PhoneCall))
            throw new ApiException(403, "USER_PRIVACY_RESTRICTED");
        var callerRules = await PostgresPrivacyPolicy.Load(work.Connection, work.NativeTransaction, [uid], callee, work.Token);
        bool p2p = CallService.EffectiveP2p(protocol!,
            AccountPrivacyService.Allowed(callerRules, uid, callee, P.PrivacyKey.PhoneP2P),
            AccountPrivacyService.Allowed(calleeRules, callee, uid, P.PrivacyKey.PhoneP2P));
        CallService.RequireRelayWhenP2pDisabled(p2p);
        var conversation = await ResolveConversation(work, uid,
            new P.PeerRef { User = new() { ServerDomain = domain, UserId = callee } }, domain, true);
        CallService.ValidateCallee(conversation, uid, callee);
        if (await work.Scalar("""
            SELECT 1 FROM botgram_backend_v1.calls WHERE state IN ('ringing','accepted','active')
                AND (caller_id=ANY($1) OR callee_id=ANY($1)) LIMIT 1
            """, (object)new[] { uid, callee }) is not null)
            throw new ApiException(409, "CALL_BUSY");
        var now = DateTimeOffset.UtcNow;
        var created = new Call(Guid.NewGuid().ToString("N"), conversation.Id, uid, callee, "ringing", now,
            TelegramId: (long)(await work.Scalar("SELECT nextval('botgram_backend_v1.call_identity')"))!,
            AccessHash: CallService.NewAccessHash(), RandomId: input.RandomId, Video: input.Video,
            Protocol: CallService.NormalizeProtocol(protocol!, p2p), GAHash: gaHash, Date: Seconds(now), P2pAllowed: p2p,
            RequestProtocol: protocol);
        await InsertCall(work, created, [created.CallerId, created.CalleeId]);
        return await PhoneResult(work, uid, domain, created);
    }

    private async Task<P.BoolValue> PhoneReceivedCall(Transaction work, Session session, P.PhoneCallRef? reference)
    {
        var call = await RequireCall(work, session.UserId, reference);
        if (call.CalleeId != session.UserId) throw new ApiException(400, "CALL_NOT_FOUND");
        if (call.State == "ringing" && call.ReceiveDate == 0)
        {
            var updated = call with { ReceiveDate = Seconds(DateTimeOffset.UtcNow) };
            await SaveCall(work, updated, [call.CallerId]);
        }
        return new P.BoolValue { Value = true };
    }

    private async Task<P.PhoneCallResult> PhoneAcceptCall(Transaction work, Session session,
        P.PhoneAcceptCallRequest input, string domain)
    {
        var call = await RequireCall(work, session.UserId, input.Peer);
        if (call.CalleeId != session.UserId) throw new ApiException(400, "CALL_NOT_FOUND");
        byte[] gb = input.GB.ToByteArray();
        var protocol = ProtobufRpcMapper.MapProtocolJson(input.Protocol);
        CallService.ValidatePublicKey(gb, "CALL_INVALID_GB");
        CallService.ValidateProtocol(protocol);
        // Only the first accept wins; other devices may replay the same key material and proposal.
        if (call.State is "accepted" or "active")
        {
            if (CryptographicOperations.FixedTimeEquals(call.GB ?? [], gb) &&
                JsonNode.DeepEquals(call.AcceptProtocol ?? call.Protocol, protocol))
                return await PhoneResult(work, session.UserId, domain, call);
            throw new ApiException(409, "CALL_ALREADY_ACCEPTED");
        }
        if (call.State != "ringing") throw new ApiException(400, "CALL_ALREADY_ACCEPTED");
        bool p2p = call.P2pAllowed && CallService.RequestsP2p(protocol!);
        CallService.RequireRelayWhenP2pDisabled(p2p);
        var negotiated = CallService.NegotiateProtocol(
            call.RequestProtocol ?? call.Protocol ?? CallService.DefaultProtocol(), protocol!, p2p);
        var updated = call with
        {
            State = "accepted", GB = gb, UpdatedAt = DateTimeOffset.UtcNow,
            Protocol = negotiated, AcceptProtocol = protocol, P2pAllowed = p2p
        };
        await SaveCall(work, updated, [call.CallerId]);
        return await PhoneResult(work, session.UserId, domain, updated);
    }

    private async Task<P.PhoneCallResult> PhoneConfirmCall(Transaction work, Session session,
        P.PhoneConfirmCallRequest input, string domain)
    {
        var call = await RequireCall(work, session.UserId, input.Peer);
        if (call.CallerId != session.UserId) throw new ApiException(400, "CALL_NOT_FOUND");
        byte[] ga = input.GA.ToByteArray();
        var protocol = ProtobufRpcMapper.MapProtocolJson(input.Protocol);
        CallService.ValidateGa(ga, call.GAHash);
        // Confirm must echo the caller's original proposal; the negotiated result is immutable.
        CallService.RequireSameCapabilities(
            call.RequestProtocol ?? call.Protocol ?? CallService.DefaultProtocol(), protocol!);
        if (input.KeyFingerprint == 0) throw new ApiException(400, "CALL_INVALID_KEY_FINGERPRINT");
        if (call.State == "active")
        {
            if (CryptographicOperations.FixedTimeEquals(call.GA ?? [], ga) && call.KeyFingerprint == input.KeyFingerprint)
                return await PhoneResult(work, session.UserId, domain, call);
            throw new ApiException(409, "CALL_ALREADY_ACTIVE");
        }
        if (call.State != "accepted") throw new ApiException(400, "CALL_NOT_ACCEPTED");
        var updated = call with
        {
            State = "active", GA = ga, KeyFingerprint = input.KeyFingerprint, StartDate = Seconds(DateTimeOffset.UtcNow),
            UpdatedAt = DateTimeOffset.UtcNow, Connections = CallService.PhoneConnections()
        };
        await SaveCall(work, updated, [call.CalleeId]);
        return await PhoneResult(work, session.UserId, domain, updated);
    }

    private async Task<P.Updates> PhoneDiscardCall(Transaction work, Session session, P.PhoneDiscardCallRequest input,
        string domain)
    {
        var call = await RequireCall(work, session.UserId, input.Peer);
        // A retry keeps the original terminal outcome instead of rewriting it.
        if (call.State is not ("ended" or "rejected"))
        {
            int duration = input.Duration;
            if (duration < 0) throw new ApiException(400, "CALL_INVALID_DURATION");
            if (duration == 0 && call.StartDate != 0) duration = Math.Max(0, Seconds(DateTimeOffset.UtcNow) - call.StartDate);
            string reason = input.Reason switch
            {
                P.PhoneCallDiscardReason.Missed => "Missed",
                P.PhoneCallDiscardReason.Busy => "Busy",
                P.PhoneCallDiscardReason.Disconnect => "Disconnect",
                _ => "Hangup"
            };
            call = call with { State = "ended", Reason = reason, Duration = duration, UpdatedAt = DateTimeOffset.UtcNow };
            await SaveCall(work, call);
        }
        var ids = await UserIdentities(work, [call.CallerId, call.CalleeId]);
        return PhoneUpdates(call, ids[call.CallerId], ids[call.CalleeId], call.CallerId == session.UserId, domain);
    }

    private async Task<P.BoolValue> PhoneSignaling(Transaction work, Session session,
        P.PhoneSendSignalingDataRequest input)
    {
        var call = await RequireCall(work, session.UserId, input.Peer);
        if (call.State != "active") throw new ApiException(400, "CALL_NOT_ACTIVE");
        byte[] data = input.Data.ToByteArray();
        if (data.Length is 0 or > CallService.MaximumSignalingBytes)
            throw new ApiException(400, "CALL_SIGNALING_TOO_LARGE");
        string other = call.CallerId == session.UserId ? call.CalleeId : call.CallerId;
        await Emit(work, "call.tl_signal", new CallSignal(call.Id, session.UserId, "tl", Convert.ToBase64String(data)),
            call.ConversationId, [other], DateTimeOffset.UtcNow.AddMinutes(5));
        return new P.BoolValue { Value = true };
    }

    private static P.DataJson PhoneCallConfig() => new() { Data = CallService.CallConfigJson() };

    private static P.BoolValue PhoneSavedDebug(P.PhoneSaveCallDebugRequest input)
    {
        if (input.Debug.Length > CallService.MaximumCallDebugBytes) throw new ApiException(400, "CALL_DEBUG_TOO_LARGE");
        return new P.BoolValue { Value = true };
    }

    private static P.DhConfig PhoneDhConfig(P.GetDhConfigRequest input)
    {
        var config = CallService.GetDhConfig(input.Version, input.RandomLength);
        return new P.DhConfig
        {
            Version = config.Version, G = config.G, P = ByteString.CopyFrom(config.P),
            Random = ByteString.CopyFrom(config.Random), NotModified = config.NotModified
        };
    }
}
