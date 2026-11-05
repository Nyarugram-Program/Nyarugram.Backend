using System.Security.Cryptography;
using System.Text.Json.Nodes;

namespace Botgram.Backend;

public sealed record CallRelay(string Host, int Port, bool Turn, string Username, string Password,
    string Transport = "udp", string Ipv6 = "");

// Immutable voice-call DH configuration. The version identifies this exact p/g pair,
// so a client that already cached it only needs fresh random padding.
public sealed record DhConfigResult(int Version, int G, byte[] P, byte[] Random, bool NotModified);

public static class CallService
{
    public const int DhConfigVersion = 1;
    public const int DhConfigG = 3;
    public const int MaximumDhRandomLength = 256;
    public const int MaximumSignalingBytes = 4096;
    public const int MaximumCallDebugBytes = 16384;

    // Telegram's 2048-bit DH prime; clients derive the voice-call auth key from p/g and the exchanged g_a/g_b.
    public static readonly byte[] DhPrime = Convert.FromHexString(
        "c71caeb9c6b1c9048e6c522f70f13f73980d40238e3e21c14934d037563d930f" +
        "48198a0aa7c14058229493d22530f4dbfa336f6e0ac925139543aed44cce7c3720" +
        "fd51f69458705ac68cd4fe6b6b13abdc9746512969328454f18faf8c595f64247" +
        "7fe96bb2a941d5bcd1d4ac8cc49880708fa9b378e3c4f3a9060bee67cf9a4a4a" +
        "695811051907e162753b56b0f6b410dba74d8a84b2a14b3144e0ef1284754fd17" +
        "ed950d5965b4b9dd46582db1178d169c6bc465b0d6ff9ca3928fef5b9ae4e418" +
        "fc15e83ebea0f87fa9ff5eed70050ded2849f47bf959d956850ce929851f0d8115" +
        "f635b105ee2e4e15d04b2454bf6f4fadf034b10403119cd8e3b92fcc5b");

    public static bool InProgress(string state) => state is "ringing" or "accepted" or "active";

    // Shared by both wire transports so the JSON and protobuf paths keep the same call rules.
    public static JsonObject DefaultProtocol() => new()
    {
        ["$type"] = "org.telegram.tgnet.tl.TL_phone$TL_phoneCallProtocol",
        ["flags"] = 3,
        ["udp_p2p"] = true,
        ["udp_reflector"] = true,
        ["min_layer"] = 65,
        ["max_layer"] = 92,
        ["library_versions"] = new JsonArray("10.0.0")
    };

    public static JsonObject? CloneProtocol(JsonNode? node) => node is JsonObject value ? (JsonObject)value.DeepClone() : null;

    public static Call Require(Database db, long id, long accessHash, string uid)
    {
        var call = id == 0 ? null : db.Calls.FirstOrDefault(item => item.TelegramId == id);
        if (call is null || call.AccessHash != accessHash || (call.CallerId != uid && call.CalleeId != uid))
            throw new ApiException(400, "CALL_NOT_FOUND");
        return call;
    }

    public static void Save(Database db, Call call)
    {
        int index = db.Calls.FindIndex(existing => existing.Id == call.Id);
        if (index < 0) db.Calls.Add(call); else db.Calls[index] = call;
    }

    // Kept within the IEEE-754 safe integer range so the value round-trips unchanged
    // through the app and generic JSON tooling.
    public static long NewAccessHash()
    {
        long hash = BitConverter.ToInt64(RandomNumberGenerator.GetBytes(8)) & 0x1FFFFFFFFFFFFF;
        return hash == 0 ? 1 : hash;
    }

    public static JsonObject[] PhoneConnections()
    {
        var result = new List<JsonObject>();
        long id = 1;
        foreach (var relay in Relays)
        {
            // phoneConnectionWebrtc carries IPv4/domain in ip and IPv6 in ipv6. The app's
            // native RTC endpoint does not map its tcp flag, so only TURN/UDP and STUN/UDP
            // are advertised here; unsupported transports are recorded in RejectedRelays.
            bool ipv6 = relay.Ipv6.Length > 0;
            result.Add(new JsonObject
            {
                ["$type"] = "org.telegram.tgnet.TLRPC$TL_phoneConnectionWebrtc",
                ["flags"] = relay.Turn ? 1 : 2,
                ["id"] = id++,
                ["ip"] = ipv6 ? "" : relay.Host,
                ["ipv6"] = ipv6 ? relay.Ipv6 : "",
                ["port"] = relay.Port,
                ["turn"] = relay.Turn,
                ["stun"] = !relay.Turn,
                ["username"] = relay.Turn ? relay.Username : "",
                ["password"] = relay.Turn ? relay.Password : ""
            });
        }
        return result.ToArray();
    }

    public static bool P2p { get; private set; } = true;
    // Only relays usable by the native RTC engine (TURN/UDP and STUN/UDP) are published.
    public static CallRelay[] Relays { get; private set; } = [];
    // Configured URLs that were understood but cannot be used by the current engine.
    public static (string Url, string Reason)[] RejectedRelays { get; private set; } = [];

    public sealed record RelayConfig(string Url, string Host, int Port, string Scheme, string Transport);

    // Relay endpoints come from the existing Rtc:IceServers section; no new configuration is required.
    public static void Configure(IConfiguration config)
    {
        P2p = config.GetValue("Rtc:P2p", true);
        var relays = new List<CallRelay>();
        var rejected = new List<(string, string)>();
        foreach (var server in config.GetSection("Rtc:IceServers").GetChildren())
        {
            string username = server["Username"] ?? "", password = server["Credential"] ?? "";
            foreach (var url in server.GetSection("Urls").Get<string[]>() ?? [])
            {
                if (string.IsNullOrWhiteSpace(url)) continue;
                var parsed = ParseRelay(url.Trim(), username, password);
                if (parsed is null) continue;
                if (parsed.Value.Reason is { } reason)
                {
                    rejected.Add((parsed.Value.Url.Url, reason));
                    continue;
                }
                if (parsed.Value.Relay is not { } relay) continue;
                if (!relays.Contains(relay)) relays.Add(relay);
            }
        }
        Relays = relays.ToArray();
        RejectedRelays = rejected.ToArray();
        foreach (var (url, reason) in RejectedRelays)
            System.Diagnostics.Debug.WriteLine($"Botgram call relay ignored: {url} ({reason})");
    }

    private static (RelayConfig Url, CallRelay? Relay, string? Reason)? ParseRelay(string url,
        string username, string password)
    {
        int colon = url.IndexOf(':');
        if (colon <= 0) return null;
        string scheme = url[..colon].ToLowerInvariant();
        if (scheme is not ("turn" or "turns" or "stun" or "stuns")) return null;
        string rest = url[(colon + 1)..];
        int query = rest.IndexOf('?');
        string queryText = query >= 0 ? rest[(query + 1)..] : "";
        if (query >= 0) rest = rest[..query];
        if (rest.EndsWith('/')) rest = rest[..^1];
        string transport = scheme is "turns" or "stuns" ? "tls" :
            QueryValue(queryText, "transport")?.ToLowerInvariant() ?? "udp";
        if (transport is not ("udp" or "tcp" or "tls"))
            throw new InvalidOperationException($"Invalid TURN/STUN transport in Rtc:IceServers URL '{url}'");
        string host;
        int port;
        if (rest.StartsWith('['))
        {
            int close = rest.IndexOf(']');
            if (close <= 1) throw new InvalidOperationException($"Invalid IPv6 host in Rtc:IceServers URL '{url}'");
            host = rest[1..close];
            string tail = rest[(close + 1)..];
            if (tail.Length > 0 && tail[0] != ':')
                throw new InvalidOperationException($"Invalid IPv6 host in Rtc:IceServers URL '{url}'");
            port = tail.Length > 1 ? ParsePort(tail[1..], url) : DefaultPort(scheme);
        }
        else
        {
            int portColon = rest.LastIndexOf(':');
            if (portColon > 0 && rest.IndexOf(':') != portColon)
                throw new InvalidOperationException($"IPv6 hosts must use brackets in Rtc:IceServers URL '{url}'");
            host = portColon > 0 ? rest[..portColon] : rest;
            port = portColon > 0 ? ParsePort(rest[(portColon + 1)..], url) : DefaultPort(scheme);
        }
        if (host.Length == 0) throw new InvalidOperationException($"Missing host in Rtc:IceServers URL '{url}'");
        var config = new RelayConfig(url, host, port, scheme, transport);
        if (transport != "udp")
            return (config, null, $"{scheme} transport '{transport}' is not supported by the native RTC engine");
        if (scheme is "stuns" or "turns")
            return (config, null, $"{scheme} is not supported by the native RTC engine");
        bool turn = scheme == "turn";
        if (turn && (string.IsNullOrWhiteSpace(username) || string.IsNullOrWhiteSpace(password)))
            throw new InvalidOperationException($"TURN relay in Rtc:IceServers URL '{url}' is missing Username/Credential");
        bool ipv6 = host.Contains(':');
        return (config, new CallRelay(host, port, turn, username, password, transport, ipv6 ? host : ""), null);
    }

    private static int DefaultPort(string scheme) => scheme is "turns" or "stuns" ? 5349 : 3478;

    private static int ParsePort(string text, string url)
    {
        if (!int.TryParse(text, out int port) || port is < 1 or > 65535)
            throw new InvalidOperationException($"Invalid port in Rtc:IceServers URL '{url}'");
        return port;
    }

    private static string? QueryValue(string query, string name)
    {
        foreach (var item in query.Split('&', StringSplitOptions.RemoveEmptyEntries))
        {
            int equals = item.IndexOf('=');
            if (equals > 0 && item[..equals].Equals(name, StringComparison.OrdinalIgnoreCase))
                return item[(equals + 1)..];
        }
        return null;
    }

    public static string CallConfigJson() => new JsonObject
    {
        ["botgram_calls"] = new JsonObject
        {
            ["p2p"] = P2p,
            ["relays"] = new JsonArray(Relays.Select(relay => (JsonNode)new JsonObject
            {
                ["scheme"] = relay.Turn ? "turn" : "stun",
                ["host"] = relay.Host,
                ["port"] = relay.Port,
                ["transport"] = relay.Transport
            }).ToArray()),
            ["unsupported"] = new JsonArray(RejectedRelays.Select(item => (JsonNode)new JsonObject
            {
                ["url"] = item.Url, ["reason"] = item.Reason
            }).ToArray())
        }
    }.ToJsonString();

    public static bool Expired(Call call, DateTimeOffset now) =>
        InProgress(call.State) &&
        now - call.UpdatedAt > (call.State == "active" ? TimeSpan.FromHours(2) : TimeSpan.FromSeconds(60));

    public static void ValidateCallee(Conversation conversation, string uid, string callee)
    {
        if (conversation.Kind != "direct" || callee == uid || !conversation.Members.Contains(callee))
            throw new ApiException(400, "invalid_callee");
    }

    public static Call Transition(Call call, string uid, string action, DateTimeOffset now)
    {
        if (action is not ("accept" or "reject" or "end")) throw new ApiException(400, "invalid_action");
        if (action is "accept" or "reject")
        {
            if (call.CalleeId != uid) throw new ApiException(403, "callee_only");
            if (call.State != "ringing") throw new ApiException(409, "call_not_ringing");
        }
        return call.State is "ended" or "rejected" ? call :
            call with { State = action == "accept" ? "active" : action == "reject" ? "rejected" : "ended", UpdatedAt = now };
    }

    public static string SignalRecipient(Call call, string uid, SignalInput input)
    {
        if (call.State != "active") throw new ApiException(409, "call_not_active");
        if (input.Type is not ("offer" or "answer" or "ice")) throw new ApiException(400, "invalid_signal_type");
        if (string.IsNullOrWhiteSpace(input.Data) || input.Data.Length > 64000) throw new ApiException(400, "invalid_signal_data");
        if (input.Type == "offer" && call.CallerId != uid) throw new ApiException(400, "caller_offers_only");
        if (input.Type == "answer" && call.CalleeId != uid) throw new ApiException(400, "callee_answers_only");
        return call.CallerId == uid ? call.CalleeId : call.CallerId;
    }

    public static DhConfigResult GetDhConfig(int version, int randomLength)
    {
        if (randomLength is < 0 or > MaximumDhRandomLength)
            throw new ApiException(400, "DH_CONFIG_INVALID_RANDOM_LENGTH");
        byte[] random = RandomNumberGenerator.GetBytes(randomLength);
        bool notModified = version == DhConfigVersion;
        return notModified
            ? new DhConfigResult(DhConfigVersion, 0, [], random, true)
            : new DhConfigResult(DhConfigVersion, DhConfigG, (byte[])DhPrime.Clone(), random, false);
    }

    // g_a_hash is the 32-byte SHA-256 commitment the caller makes before it learns g_b.
    public static void ValidateGaHash(byte[]? hash)
    {
        if (hash is not { Length: 32 }) throw new ApiException(400, "CALL_INVALID_GA_HASH");
    }

    // Both Diffie-Hellman public values are exactly 256 bytes (2048-bit, no padding changes).
    public static void ValidatePublicKey(byte[]? value, string code)
    {
        if (value is not { Length: 256 }) throw new ApiException(400, code);
    }

    public static void ValidateGa(byte[]? ga, byte[]? gaHash)
    {
        ValidatePublicKey(ga, "CALL_INVALID_GA");
        if (gaHash is not { Length: 32 } || !CryptographicOperations.FixedTimeEquals(SHA256.HashData(ga!), gaHash))
            throw new ApiException(400, "CALL_HASH_MISMATCH");
    }

    // Version 1 is not a valid layer, and anything above 1000 cannot belong to a real client.
    public const int MinimumLayer = 1;
    public const int MaximumLayer = 1000;

    public static void ValidateProtocol(JsonObject? protocol)
    {
        if (protocol is null) throw new ApiException(400, "CALL_PROTOCOL_REQUIRED");
        int min = Layer(protocol, "min_layer");
        int max = Layer(protocol, "max_layer");
        if (min < MinimumLayer || max < min || max > MaximumLayer)
            throw new ApiException(400, "CALL_INVALID_PROTOCOL_LAYERS");
        if (protocol["library_versions"] is not JsonArray versions || versions.Count is 0 or > 32)
            throw new ApiException(400, "CALL_INVALID_PROTOCOL_LIBRARY");
        foreach (var version in versions)
            if (version is not JsonValue value || !value.TryGetValue<string>(out var text) ||
                string.IsNullOrWhiteSpace(text) || text.Length > 64)
                throw new ApiException(400, "CALL_INVALID_PROTOCOL_LIBRARY");
    }

    public static int Layer(JsonObject protocol, string field)
    {
        if (protocol[field] is not JsonValue value) throw new ApiException(400, "CALL_INVALID_PROTOCOL_LAYERS");
        if (value.TryGetValue<int>(out var number)) return number;
        if (value.TryGetValue<long>(out var wide) && wide is >= int.MinValue and <= int.MaxValue) return (int)wide;
        throw new ApiException(400, "CALL_INVALID_PROTOCOL_LAYERS");
    }

    public static string[] LibraryVersions(JsonObject protocol)
    {
        if (protocol["library_versions"] is not JsonArray versions)
            throw new ApiException(400, "CALL_INVALID_PROTOCOL_LIBRARY");
        return versions.Select(item => item?.GetValue<string>() ?? "").ToArray();
    }

    public static bool RequestsP2p(JsonObject protocol) =>
        protocol["udp_p2p"] is JsonValue value && value.TryGetValue<bool>(out var enabled) && enabled;

    private static bool RequestsReflector(JsonObject protocol) =>
        protocol["udp_reflector"] is JsonValue value && value.TryGetValue<bool>(out var enabled) && enabled;

    // The caller and callee proposals are intersected once, at accept time. All later
    // updates (including confirm and difference replay) reuse this negotiated value.
    public static JsonObject NegotiateProtocol(JsonObject caller, JsonObject callee, bool p2pAllowed)
    {
        ValidateProtocol(caller);
        ValidateProtocol(callee);
        int min = Math.Max(Layer(caller, "min_layer"), Layer(callee, "min_layer"));
        int max = Math.Min(Layer(caller, "max_layer"), Layer(callee, "max_layer"));
        if (max < min) throw new ApiException(409, "CALL_PROTOCOL_INCOMPATIBLE");
        var calleeVersions = new HashSet<string>(LibraryVersions(callee), StringComparer.Ordinal);
        var versions = LibraryVersions(caller)
            .Where(calleeVersions.Contains)
            .Distinct(StringComparer.Ordinal)
            .ToArray();
        if (versions.Length == 0) throw new ApiException(409, "CALL_PROTOCOL_INCOMPATIBLE");
        bool reflector = RequestsReflector(caller) && RequestsReflector(callee);
        var negotiated = (JsonObject)caller.DeepClone();
        negotiated["flags"] = (p2pAllowed ? 1 : 0) | (reflector ? 2 : 0);
        negotiated["udp_p2p"] = p2pAllowed;
        negotiated["udp_reflector"] = reflector;
        negotiated["min_layer"] = min;
        negotiated["max_layer"] = max;
        negotiated["library_versions"] = new JsonArray(versions.Select(version => (JsonNode?)JsonValue.Create(version)).ToArray());
        return negotiated;
    }

    // Key material is derived from the layer range and library version. Confirm must echo
    // the caller's original proposal so a late or retried update cannot change the result.
    public static void RequireSameCapabilities(JsonObject original, JsonObject confirm)
    {
        ValidateProtocol(confirm);
        if (Layer(original, "min_layer") != Layer(confirm, "min_layer") ||
            Layer(original, "max_layer") != Layer(confirm, "max_layer") ||
            !LibraryVersions(original).SequenceEqual(LibraryVersions(confirm), StringComparer.Ordinal))
            throw new ApiException(409, "CALL_PROTOCOL_MISMATCH");
    }

    // The client proposes P2P; the effective value also depends on the server switch and
    // both participants' privacy rules, and the stored protocol must agree with it.
    public static bool EffectiveP2p(JsonObject protocol, bool callerAllowsCallee, bool calleeAllowsCaller)
    {
        bool requested = protocol["udp_p2p"] is JsonValue value && value.TryGetValue<bool>(out var enabled) && enabled;
        return P2p && requested && callerAllowsCallee && calleeAllowsCaller;
    }

    public static JsonObject NormalizeProtocol(JsonObject protocol, bool p2pAllowed)
    {
        var normalized = (JsonObject)protocol.DeepClone();
        bool reflector = RequestsReflector(protocol);
        normalized["flags"] = (p2pAllowed ? 1 : 0) | (reflector ? 2 : 0);
        normalized["udp_p2p"] = p2pAllowed;
        normalized["udp_reflector"] = reflector;
        return normalized;
    }

    // STUN only discovers addresses; disabling P2P requires at least one real TURN relay.
    public static void RequireRelayWhenP2pDisabled(bool p2pAllowed)
    {
        if (!p2pAllowed && !Relays.Any(relay => relay.Turn))
            throw new ApiException(503, "CALL_RELAY_UNAVAILABLE");
    }
}
