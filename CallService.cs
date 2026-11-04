namespace Botgram.Backend;

public sealed record CallRelay(string Host, int Port, bool Turn, string Username, string Password);

public static class CallService
{
    public static bool InProgress(string state) => state is "ringing" or "accepted" or "active";

    public static bool P2p { get; private set; } = true;
    public static CallRelay[] Relays { get; private set; } = [];

    // Relay endpoints come from the existing Rtc:IceServers section; no new configuration is required.
    public static void Configure(IConfiguration config)
    {
        P2p = config.GetValue("Rtc:P2p", true);
        var relays = new List<CallRelay>();
        foreach (var server in config.GetSection("Rtc:IceServers").GetChildren())
        {
            string username = server["Username"] ?? "", password = server["Credential"] ?? "";
            foreach (var url in server.GetSection("Urls").Get<string[]>() ?? [])
            {
                int colon = url.IndexOf(':');
                if (colon <= 0) continue;
                string scheme = url[..colon].ToLowerInvariant();
                if (scheme is not ("turn" or "stun")) continue;
                string rest = url[(colon + 1)..];
                int query = rest.IndexOf('?');
                if (query >= 0) rest = rest[..query];
                int portColon = rest.LastIndexOf(':');
                string host = portColon > 0 ? rest[..portColon] : rest;
                int port = portColon > 0 && int.TryParse(rest[(portColon + 1)..], out var parsed) ? parsed : 3478;
                if (host.Length == 0) continue;
                relays.Add(new CallRelay(host, port, scheme == "turn", username, password));
            }
        }
        Relays = relays.ToArray();
    }

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
}
