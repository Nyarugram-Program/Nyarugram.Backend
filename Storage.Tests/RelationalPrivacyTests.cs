using Botgram.Backend;
using Botgram.Backend.Storage;
using Google.Protobuf;
using Microsoft.AspNetCore.DataProtection;
using Npgsql;
using P = Botgram.Protocol.V1;

internal static class RelationalPrivacyTests
{
    public static async Task<int> RunAsync(NpgsqlDataSource source)
    {
        int assertions = 0;
        void Check(bool ok, string label) { if (!ok) throw new Exception(label); assertions++; }
        async Task Reject(Func<Task> action, string error)
        {
            try { await action(); throw new Exception("Expected " + error); }
            catch (ApiException exception) when (exception.Message == error) { assertions++; }
        }
        var security = new AccountTotpService(new EphemeralDataProtectionProvider());
        var backend = new PostgresBackend(source, totp: security);
        var alice = await backend.RegisterAsync(new("privacy_pg_alice", "Alice", "isolated-password"));
        var bob = await backend.RegisterAsync(new("privacy_pg_bob", "Bob", "isolated-password"));
        var own = await backend.RequireAsync(Auth.HashToken(alice.Token));
        var other = await backend.RequireAsync(Auth.HashToken(bob.Token));
        async Task<P.AccountPrivacyResponse> Rpc(Session session, P.AccountPrivacyRequest input)
        {
            return await backend.DispatchAsync(session, new P.RpcRequest { ProtocolMajor = 1, RequestId = "pgprivacy", AccountPrivacy = input }, "pg.test", r => r.AccountPrivacy);
        }
        await Rpc(own, new() { SetRules = new() { Key = P.PrivacyKey.PhoneNumber, Rules = { new P.PrivacyRule { Kind = P.PrivacyRuleKind.DisallowAll } } } });
        Check((await Rpc(own, new() { GetRules = new() { Key = P.PrivacyKey.PhoneNumber } })).Rules.Rules.Single().Kind == P.PrivacyRuleKind.DisallowAll, "PG rules persist");
        var profile = await backend.DispatchAsync(other, new P.RpcRequest { ProtocolMajor = 1, RequestId = "profile", ResolveUsername = new() { Username = "privacy_pg_alice" } }, "pg.test", r => r.ResolveUsername);
        Check(profile.Users.Single().Gid == "", "PG Gram ID hidden from other account");
        await Rpc(own, new() { SetRules = new() { Key = P.PrivacyKey.ChatInvite, Rules = { new P.PrivacyRule { Kind = P.PrivacyRuleKind.DisallowAll } } } });
        await Reject(() => backend.CreateConversationAsync(other, new("group", "Denied invite", [alice.User.Id])), "USER_PRIVACY_RESTRICTED");
        await Rpc(own, new() { SetRules = new() { Key = P.PrivacyKey.PhoneCall, Rules = { new P.PrivacyRule { Kind = P.PrivacyRuleKind.DisallowAll } } } });
        var chat = await backend.CreateConversationAsync(other, new("direct", null, [alice.User.Id]));
        await Reject(() => backend.CreateCallAsync(other, new(chat.Id, alice.User.Id)), "USER_PRIVACY_RESTRICTED");
        await Rpc(own, new() { BlockUser = new() { UserId = bob.User.Id, ServerDomain = "pg.test" } });
        await Reject(() => backend.SendRestAsync(other, chat.Id, new("denied", "blocked"), m => m), "USER_IS_BLOCKED");
        await Rpc(own, new() { UnblockUser = new() { UserId = bob.User.Id, ServerDomain = "pg.test" } });
        await backend.SendRestAsync(other, chat.Id, new("allowed", "unblocked"), m => m);
        assertions++;
        var setup = (TotpEnrollment)await backend.TotpAsync(own, "setup", null);
        byte[] secret = Decode(setup.Secret);
        var recovery = (TotpRecovery)await backend.TotpAsync(own, "confirm", AccountTotpService.Code(secret, DateTimeOffset.UtcNow.ToUnixTimeSeconds() / 30));
        Check(recovery.RecoveryCodes.Length == 10 && recovery.RecoveryCodes.Distinct().Count() == 10
            && recovery.RecoveryCodes.All(code => code.Length == 8 && code.All(char.IsAsciiDigit)), "PG generates ten eight-digit numeric recovery codes");
        await Reject(() => backend.LoginAsync(new("privacy_pg_alice", "isolated-password")), "TOTP_REQUIRED");
        var attempts = await Task.WhenAll(Enumerable.Range(0, 2).Select(async _ =>
        {
            try { await backend.LoginAsync(new("privacy_pg_alice", "isolated-password", TotpCode: recovery.RecoveryCodes[0])); return true; }
            catch (ApiException exception) when (exception.Message == "TOTP_CODE_INVALID") { return false; }
        }));
        Check(attempts.Count(ok => ok) == 1, "PG concurrent recovery code single use");
        var reopened = new PostgresBackend(source, totp: security);
        Check(((TotpStatus)await reopened.TotpAsync(own, "status", null)).RecoveryCodesRemaining == 9, "PG TOTP state durable across backend instances");
        await reopened.TotpAsync(own, "disable", recovery.RecoveryCodes[1]);
        await reopened.LoginAsync(new("privacy_pg_alice", "isolated-password"));
        assertions++;
        return assertions;
    }

    private static byte[] Decode(string value)
    {
        const string alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZ234567";
        var bytes = new List<byte>(); int bits = 0, buffer = 0;
        foreach (char c in value)
        {
            buffer = (buffer << 5) | alphabet.IndexOf(c); bits += 5;
            if (bits >= 8) { bits -= 8; bytes.Add((byte)(buffer >> bits)); }
        }
        return bytes.ToArray();
    }
}
