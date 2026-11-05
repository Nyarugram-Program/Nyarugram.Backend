using System.Security.Cryptography;
using System.Text.Json.Nodes;
using Google.Protobuf;
using P = Botgram.Protocol.V1;

internal static partial class HttpTests
{
    // LegacyOnly still serves the normal protobuf HTTP endpoint. This scenario keeps JSON RPC and
    // JSON WSS disabled so every assertion below exercises ProtobufRpcMapper directly.
    private static async Task NativeCallsProtobufOnly()
    {
        const string password = "isolated-calls-password";
        var path = Path.Combine(Root, "calls-protobuf-only.json");
        var stunPath = Path.Combine(Root, "calls-protobuf-stun-only.json");
        var server = await Server.Start(path, legacyEnabled: false, legacyWssEnabled: false, relays: true);

        async Task<(string Token, string Id)> Register(Server current, string name)
        {
            var response = await current.Json("POST", "/api/auth/register",
                new { username = name, displayName = name, password });
            return (Str(response, "token"), Str(response["user"]!, "id"));
        }
        async Task<string> Login(Server current, string name)
        {
            var response = await current.Json("POST", "/api/auth/login", new { username = name, password });
            return Str(response, "token");
        }

        string alice, bob, bobSecond;
        string bobId, carolId;
        try
        {
            (alice, _) = await Register(server, "calls_alice");
            (bob, bobId) = await Register(server, "calls_bob");
            (_, carolId) = await Register(server, "calls_carol");
            bobSecond = await Login(server, "calls_bob");
        }
        catch
        {
            await server.DisposeAsync();
            throw;
        }

        async Task<P.RpcResponse> Rpc(string token, P.RpcRequest request, int status = 200, string? code = null)
        {
            request.ProtocolMajor = 1;
            request.RequestId = Guid.NewGuid().ToString("N");
            return await server.Raw(request.ToByteArray(), status, code, token);
        }
        Task<P.RpcResponse> State(string token) => Rpc(token, new P.RpcRequest { GetState = new() });
        Task<P.RpcResponse> Difference(string token, int pts)
            => Rpc(token, new P.RpcRequest { GetDifference = new() { Pts = pts } });
        int Pts(P.RpcResponse state) => state.GetState.Pts;
        P.PhoneCallRef Peer(P.PhoneCall call) => new() { CallId = call.CallId, AccessHash = call.AccessHash };
        P.UserSelector Select(string id) => new() { User = new P.UserRef { ServerDomain = Domain, UserId = id } };
        P.PhoneCallProtocol Protocol(int min, int max, bool p2p, bool reflector, params string[] versions)
        {
            var protocol = new P.PhoneCallProtocol { UdpP2P = p2p, UdpReflector = reflector, MinLayer = min, MaxLayer = max };
            protocol.LibraryVersions.Add(versions);
            return protocol;
        }
        ByteString Key(byte fill)
        {
            var value = new byte[256];
            Array.Fill(value, fill);
            return ByteString.CopyFrom(value);
        }
        ByteString Hash(ByteString key) => ByteString.CopyFrom(SHA256.HashData(key.ToByteArray()));
        void CheckProtocol(P.PhoneCallProtocol actual, int min, int max, bool p2p, bool reflector,
            params string[] versions) => Check(actual.MinLayer == min && actual.MaxLayer == max && actual.UdpP2P == p2p
                && actual.UdpReflector == reflector && actual.LibraryVersions.SequenceEqual(versions),
                $"negotiated protocol expected {min}-{max} p2p={p2p} reflector={reflector} versions={string.Join(',', versions)}");

        async Task<P.PhoneCall> Request(string token, string callee, long randomId, ByteString gaHash,
            P.PhoneCallProtocol protocol, bool video = false)
            => (await Rpc(token, new P.RpcRequest
            {
                RequestCall = new P.PhoneRequestCallRequest
                {
                    User = Select(callee), RandomId = randomId, Video = video, GAHash = gaHash, Protocol = protocol
                }
            })).RequestCall.PhoneCall;
        async Task<P.PhoneCall> Accept(string token, P.PhoneCallRef peer, ByteString gb, P.PhoneCallProtocol protocol,
            int status = 200, string? code = null)
        {
            var response = await Rpc(token, new P.RpcRequest
            {
                AcceptCall = new P.PhoneAcceptCallRequest { Peer = peer, GB = gb, Protocol = protocol }
            }, status, code);
            return status == 200 ? response.AcceptCall.PhoneCall : new P.PhoneCall();
        }
        async Task<P.PhoneCallResult> Confirm(string token, P.PhoneCallRef peer, ByteString ga, long fingerprint,
            P.PhoneCallProtocol protocol, int status = 200, string? code = null)
        {
            var response = await Rpc(token, new P.RpcRequest
            {
                ConfirmCall = new P.PhoneConfirmCallRequest
                {
                    Peer = peer, GA = ga, KeyFingerprint = fingerprint, Protocol = protocol
                }
            }, status, code);
            return status == 200 ? response.ConfirmCall : new P.PhoneCallResult();
        }
        Task<P.RpcResponse> Discard(string token, P.PhoneCallRef peer) => Rpc(token, new P.RpcRequest
        {
            DiscardCall = new P.PhoneDiscardCallRequest { Peer = peer, Duration = 0, Reason = P.PhoneCallDiscardReason.Hangup }
        });
        async Task Conflict(string callee, long randomId, ByteString gaHash, P.PhoneCallProtocol protocol, bool video,
            string token = "")
            => await Rpc(token.Length == 0 ? alice : token, new P.RpcRequest
            {
                RequestCall = new P.PhoneRequestCallRequest
                {
                    User = Select(callee), RandomId = randomId, Video = video, GAHash = gaHash, Protocol = protocol
                }
            }, 409, "CALL_IDEMPOTENCY_CONFLICT");

        try
        {
            // ---- Both sides propose different, partially overlapping capability sets. ----
            var callerProposal = Protocol(60, 95, true, true, "11.0.0", "10.0.0", "10.0.0");
            var calleeProposal = Protocol(75, 100, true, true, "9.0.0", "10.0.0", "10.0.0");
            var ga = Key(2);
            var gb = Key(3);
            var bobStart = Pts(await State(bob));
            var call = await Request(alice, bobId, 101, Hash(ga), callerProposal);
            Check(call.State == P.PhoneCallState.Waiting && call.CallId > 0 && call.AccessHash != 0,
                "protobuf requestCall allocates a ringing call");
            Check(call.P2PAllowed && call.Protocol.UdpP2P && call.Protocol.UdpReflector,
                "requestCall stores the caller proposal and effective P2P");
            CheckProtocol(call.Protocol, 60, 95, true, true, "11.0.0", "10.0.0", "10.0.0");
            var callerPeer = Peer(call);
            Check(Pts(await State(bob)) == bobStart + 1, "requestCall emits one event to the callee");
            Check((await Rpc(bob, new P.RpcRequest { ReceivedCall = new P.PhoneReceivedCallRequest { Peer = callerPeer } })).ReceivedCall.Value,
                "receivedCall acknowledges the invitation");
            var accepted = await Accept(bob, callerPeer, gb, calleeProposal);
            Check(accepted.State == P.PhoneCallState.Accepted && accepted.CallId == call.CallId
                && accepted.AccessHash == call.AccessHash, "protobuf acceptCall keeps the same call identity");
            CheckProtocol(accepted.Protocol, 75, 95, true, true, "10.0.0");
            var acceptedPts = Pts(await State(bob));
            Check(acceptedPts == bobStart + 1, "acceptCall does not emit an event to the callee");

            // Stable original proposals keep request and accept retries idempotent.
            var replayRequest = await Request(alice, bobId, 101, Hash(ga), callerProposal);
            Check(replayRequest.CallId == call.CallId && replayRequest.State == P.PhoneCallState.Accepted,
                "requestCall retry after accept returns the same call and current state");
            var replayAccept = await Accept(bob, callerPeer, gb, calleeProposal);
            Check(replayAccept.CallId == call.CallId && replayAccept.State == P.PhoneCallState.Accepted,
                "acceptCall retry after accept returns the same call");
            CheckProtocol(replayAccept.Protocol, 75, 95, true, true, "10.0.0");
            Check(Pts(await State(bob)) == acceptedPts, "request and accept retries do not advance pts");
            await Accept(bob, callerPeer, Key(4), calleeProposal, 409, "CALL_ALREADY_ACCEPTED");
            await Accept(bob, callerPeer, gb, Protocol(75, 95, true, true, "9.0.0"), 409, "CALL_ALREADY_ACCEPTED");

            // Confirm must echo the caller's original proposal and must not replace the negotiated result.
            await Rpc(alice, new P.RpcRequest
            {
                ConfirmCall = new P.PhoneConfirmCallRequest
                {
                    Peer = callerPeer, GA = ga, KeyFingerprint = 123456789,
                    Protocol = Protocol(60, 95, true, true, "11.0.0", "10.0.0")
                }
            }, 409, "CALL_PROTOCOL_MISMATCH");
            Check((await Request(alice, bobId, 101, Hash(ga), callerProposal)).State == P.PhoneCallState.Accepted,
                "a rejected confirm leaves the call accepted");
            var confirmed = await Confirm(alice, callerPeer, ga, 123456789, callerProposal);
            Check(confirmed.PhoneCall.State == P.PhoneCallState.Active && confirmed.PhoneCall.CallId == call.CallId
                && confirmed.PhoneCall.KeyFingerprint == 123456789 && confirmed.PhoneCall.Connections.Count == 2,
                "confirmCall activates the call and publishes both relays");
            CheckProtocol(confirmed.PhoneCall.Protocol, 75, 95, true, true, "10.0.0");
            var activePts = Pts(await State(bob));
            Check(activePts == acceptedPts + 1, "confirmCall emits one event to the callee");
            var replayConfirm = await Confirm(alice, callerPeer, ga, 123456789, callerProposal);
            Check(replayConfirm.PhoneCall.State == P.PhoneCallState.Active && replayConfirm.PhoneCall.CallId == call.CallId,
                "confirmCall retry after activation returns the same active call");
            Check(Pts(await State(bob)) == activePts, "confirm retry does not advance pts");
            var afterActiveAccept = await Accept(bob, callerPeer, gb, calleeProposal);
            Check(afterActiveAccept.State == P.PhoneCallState.Active && afterActiveAccept.CallId == call.CallId,
                "acceptCall retry after activation converges on the active call");
            var afterActiveRequest = await Request(alice, bobId, 101, Hash(ga), callerProposal);
            Check(afterActiveRequest.State == P.PhoneCallState.Active && afterActiveRequest.CallId == call.CallId,
                "requestCall retry after activation converges on the active call");
            await Confirm(alice, callerPeer, Key(5), 123456789, callerProposal, 400, "CALL_HASH_MISMATCH");
            await Confirm(alice, callerPeer, ga, 999, callerProposal, 409, "CALL_ALREADY_ACTIVE");
            var bobDifference = (await Difference(bob, bobStart)).GetDifference.Complete;
            Check(bobDifference is not null && bobDifference.Updates.Any(update =>
                    update.PhoneCall.PhoneCall.CallId == call.CallId && update.PhoneCall.PhoneCall.State == P.PhoneCallState.Active
                    && update.PhoneCall.PhoneCall.Protocol.LibraryVersions.SequenceEqual(new[] { "10.0.0" })),
                "difference replay exposes the negotiated protocol, not the caller version");
            await Discard(alice, callerPeer);
            var endedPts = Pts(await State(bob));
            Check((await Request(alice, bobId, 101, Hash(ga), callerProposal)).State == P.PhoneCallState.Discarded,
                "request retry after hangup returns the stable terminal state");
            await Accept(bob, callerPeer, gb, calleeProposal, 400, "CALL_ALREADY_ACCEPTED");
            await Confirm(alice, callerPeer, ga, 123456789, callerProposal, 400, "CALL_NOT_ACCEPTED");
            Check(Pts(await State(bob)) == endedPts, "terminal retries do not revive the call or advance pts");

            // ---- Incompatible proposals must not create partial state. ----
            var incompatible = await Request(alice, bobId, 102, Hash(ga), Protocol(90, 95, true, true, "10.0.0"));
            var incompatiblePeer = Peer(incompatible);
            var incompatiblePts = Pts(await State(bob));
            await Accept(bob, incompatiblePeer, gb, Protocol(60, 80, true, true, "10.0.0"), 409, "CALL_PROTOCOL_INCOMPATIBLE");
            await Accept(bob, incompatiblePeer, gb, Protocol(60, 95, true, true, "9.0.0"), 409, "CALL_PROTOCOL_INCOMPATIBLE");
            await Accept(bob, incompatiblePeer, gb, Protocol(60, 80, true, true), 400, "CALL_INVALID_PROTOCOL_LIBRARY");
            await Accept(bob, incompatiblePeer, gb, Protocol(0, 80, true, true, "10.0.0"), 400, "CALL_INVALID_PROTOCOL_LAYERS");
            Check((await Request(alice, bobId, 102, Hash(ga), Protocol(90, 95, true, true, "10.0.0"))).State
                == P.PhoneCallState.Waiting, "failed accepts leave the call ringing");
            Check(Pts(await State(bob)) == incompatiblePts, "failed accepts roll back events and pts");
            var compatibleAccept = await Accept(bob, incompatiblePeer, Key(7), Protocol(80, 95, true, true, "10.0.0"));
            CheckProtocol(compatibleAccept.Protocol, 90, 95, true, true, "10.0.0");
            Check(compatibleAccept.State == P.PhoneCallState.Accepted,
                "a compatible proposal can still accept the same ringing call");
            await Discard(alice, incompatiblePeer);

            // ---- Replaying the request with different parameters is an explicit conflict. ----
            var replaySource = await Request(alice, bobId, 103, Hash(ga), Protocol(65, 92, true, true, "10.0.0"));
            var replayPeer = Peer(replaySource);
            var conflictPts = Pts(await State(bob));
            await Conflict(carolId, 103, Hash(ga), Protocol(65, 92, true, true, "10.0.0"), false);
            await Conflict(bobId, 103, Hash(ga), Protocol(65, 92, true, true, "10.0.0"), true);
            await Conflict(bobId, 103, Key(8), Protocol(65, 92, true, true, "10.0.0"), false);
            await Conflict(bobId, 103, Hash(ga), Protocol(65, 92, true, true, "9.0.0"), false);
            Check(Pts(await State(bob)) == conflictPts, "conflicting request replays do not allocate calls or events");
            await Discard(alice, replayPeer);

            // ---- Privacy resolves at request time; a callee may still disable P2P when accepting. ----
            await Rpc(bob, new P.RpcRequest
            {
                AccountPrivacy = new P.AccountPrivacyRequest
                {
                    SetRules = new() { Key = P.PrivacyKey.PhoneP2P, Rules = { new P.PrivacyRule { Kind = P.PrivacyRuleKind.DisallowAll } } }
                }
            });
            var relayed = await Request(alice, bobId, 104, Hash(ga), Protocol(65, 92, true, true, "10.0.0"));
            Check(!relayed.P2PAllowed && !relayed.Protocol.UdpP2P,
                "callee PhoneP2P privacy forces the call off peer-to-peer at request time");
            var relayedAccepted = await Accept(bob, Peer(relayed), Key(9), Protocol(75, 100, false, true, "10.0.0"));
            Check(!relayedAccepted.P2PAllowed && !relayedAccepted.Protocol.UdpP2P,
                "an accept that disables P2P keeps the effective value false");
            var relayedConfirmed = await Confirm(alice, Peer(relayed), ga, 42, Protocol(65, 92, true, true, "10.0.0"));
            Check(relayedConfirmed.PhoneCall.Connections.Any(connection => connection.Turn)
                && relayedConfirmed.PhoneCall.Connections.Any(connection => connection.Stun),
                "a relayed call still publishes TURN and STUN endpoints");
            await Discard(alice, Peer(relayed));
            await Rpc(bob, new P.RpcRequest
            {
                AccountPrivacy = new P.AccountPrivacyRequest
                {
                    SetRules = new() { Key = P.PrivacyKey.PhoneP2P, Rules = { new P.PrivacyRule { Kind = P.PrivacyRuleKind.AllowAll } } }
                }
            });
            var mixed = await Request(alice, bobId, 105, Hash(ga), Protocol(65, 92, true, false, "10.0.0"));
            Check(mixed.P2PAllowed, "caller keeps P2P when privacy allows it");
            var mixedAccepted = await Accept(bob, Peer(mixed), Key(11), Protocol(75, 100, false, true, "10.0.0"));
            Check(!mixedAccepted.Protocol.UdpP2P && !mixedAccepted.Protocol.UdpReflector,
                "accept intersects P2P and reflector abilities from both sides");
            await Discard(alice, Peer(mixed));

            // ---- Multi-device accept competition converges on one call. ----
            var race = await Request(alice, bobId, 106, Hash(ga), Protocol(65, 92, true, true, "10.0.0"));
            var racePeer = Peer(race);
            async Task<bool> Race(ByteString key, string token)
            {
                try
                {
                    await Rpc(token, new P.RpcRequest
                    {
                        AcceptCall = new P.PhoneAcceptCallRequest
                        {
                            Peer = racePeer, GB = key, Protocol = Protocol(65, 92, true, true, "10.0.0")
                        }
                    });
                    return true;
                }
                catch (Exception error) when (error.Message.Contains("expected 200 got 409", StringComparison.Ordinal))
                {
                    return false;
                }
            }
            var outcomes = await Task.WhenAll(Race(Key(12), bob), Race(Key(13), bobSecond));
            Check(outcomes.Count(success => success) == 1,
                "concurrent multi-device accepts produce exactly one winner");
            Check((await Request(alice, bobId, 106, Hash(ga), Protocol(65, 92, true, true, "10.0.0"))).State
                == P.PhoneCallState.Accepted, "the winning device accepted the same call");
            var winningKey = outcomes[0] ? Key(12) : Key(13);
            var losingKey = outcomes[0] ? Key(13) : Key(12);
            await Accept(bob, racePeer, winningKey, Protocol(65, 92, true, true, "10.0.0"));
            await Accept(bobSecond, racePeer, losingKey, Protocol(65, 92, true, true, "10.0.0"), 409, "CALL_ALREADY_ACCEPTED");
            await Discard(alice, racePeer);

            // ---- Persist both proposals and the negotiated result across a restart. ----
            var restartCall = await Request(alice, bobId, 107, Hash(ga),
                Protocol(60, 95, true, true, "11.0.0", "10.0.0"));
            var restartPeer = Peer(restartCall);
            var restartAccepted = await Accept(bob, restartPeer, Key(14),
                Protocol(75, 100, true, true, "9.0.0", "10.0.0"));
            CheckProtocol(restartAccepted.Protocol, 75, 95, true, true, "10.0.0");
            var restartConfirmed = await Confirm(alice, restartPeer, ga, 9007199254740991,
                Protocol(60, 95, true, true, "11.0.0", "10.0.0"));
            Check(restartConfirmed.PhoneCall.State == P.PhoneCallState.Active, "restart fixture is active");
            var restartPts = Pts(await State(bob));
            await server.DisposeAsync();
            var stored = JsonNode.Parse(await File.ReadAllTextAsync(path))!["calls"]!.AsArray()
                .Single(item => item!["telegramId"]!.GetValue<long>() == restartCall.CallId)!;
            Check(stored["requestProtocol"]!["library_versions"]!.AsArray().Select(item => item!.GetValue<string>())
                    .SequenceEqual(new[] { "11.0.0", "10.0.0" })
                && stored["acceptProtocol"]!["library_versions"]!.AsArray().Select(item => item!.GetValue<string>())
                    .SequenceEqual(new[] { "9.0.0", "10.0.0" })
                && stored["protocol"]!["library_versions"]!.AsArray().Select(item => item!.GetValue<string>())
                    .SequenceEqual(new[] { "10.0.0" }),
                "the snapshot keeps both original proposals and the negotiated result");
            server = await Server.Start(path, legacyEnabled: false, legacyWssEnabled: false, relays: true);
            var restartedRequest = await Request(alice, bobId, 107, Hash(ga),
                Protocol(60, 95, true, true, "11.0.0", "10.0.0"));
            Check(restartedRequest.State == P.PhoneCallState.Active && restartedRequest.CallId == restartCall.CallId,
                "requestCall retry after restart returns the active call");
            var restartedAccept = await Accept(bob, restartPeer, Key(14),
                Protocol(75, 100, true, true, "9.0.0", "10.0.0"));
            Check(restartedAccept.State == P.PhoneCallState.Active && restartedAccept.CallId == restartCall.CallId,
                "acceptCall retry after restart returns the active call");
            var restartedConfirm = await Confirm(alice, restartPeer, ga, 9007199254740991,
                Protocol(60, 95, true, true, "11.0.0", "10.0.0"));
            Check(restartedConfirm.PhoneCall.State == P.PhoneCallState.Active, "confirmCall retry after restart returns active");
            Check(Pts(await State(bob)) == restartPts, "restart retries do not advance pts");
            CheckProtocol(restartedConfirm.PhoneCall.Protocol, 75, 95, true, true, "10.0.0");
            await Discard(alice, restartPeer);

            // ---- STUN alone cannot carry an accept that disables P2P. ----
            await server.DisposeAsync();
            server = await Server.Start(stunPath, legacyEnabled: false, legacyWssEnabled: false,
                relays: true, stunOnly: true);
            (alice, _) = await Register(server, "stun_alice");
            (bob, bobId) = await Register(server, "stun_bob");
            var stunConfig = (await Rpc(alice, new P.RpcRequest { GetCallConfig = new P.PhoneGetCallConfigRequest() })).GetCallConfig.Data;
            Check(!stunConfig.Contains("\"turn\"", StringComparison.Ordinal) && stunConfig.Contains("\"stun\"", StringComparison.Ordinal),
                "the STUN-only fixture publishes no TURN relay");
            var stunCall = await Request(alice, bobId, 201, Hash(ga), Protocol(65, 92, true, true, "10.0.0"));
            Check(stunCall.P2PAllowed, "a P2P request is allowed without TURN");
            var stunPts = Pts(await State(bob));
            await Accept(bob, Peer(stunCall), Key(16), Protocol(65, 92, false, true, "10.0.0"), 503, "CALL_RELAY_UNAVAILABLE");
            Check((await Request(alice, bobId, 201, Hash(ga), Protocol(65, 92, true, true, "10.0.0"))).State
                == P.PhoneCallState.Waiting, "a missing TURN leaves the call ringing");
            Check(Pts(await State(bob)) == stunPts, "a missing TURN does not emit events or advance pts");
            await Discard(alice, Peer(stunCall));
        }
        finally
        {
            await server.DisposeAsync();
        }
        Console.WriteLine($"PASS: {assertions} assertions; LegacyOnly protobuf-only call negotiation, idempotency, conflicts, privacy, relays and restart");
    }
}
