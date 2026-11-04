using System.Text;
using System.Text.Json.Nodes;
using Botgram.Backend;
using Google.Protobuf;
using Microsoft.AspNetCore.DataProtection;
using System.Security.Cryptography;
using P = Botgram.Protocol.V1;

internal static partial class HttpTests
{
    private static async Task PrivacySecurity()
    {
        var provider = new EphemeralDataProtectionProvider();
        var security = new AccountTotpService(provider);
        var recoveryState = new AccountTotpState();
        var setup = security.Begin("recovery-format", "format", recoveryState);
        var generated = security.Confirm("recovery-format", recoveryState, TotpCode(setup.Secret)).RecoveryCodes;
        Check(generated.Length == 10 && generated.Distinct().Count() == 10
            && generated.All(code => code.Length == 8 && code.All(char.IsAsciiDigit)), "ten unique eight-digit recovery codes");
        for (int i = 0; i < generated.Length; i++)
        {
            // Each verifier represents a fresh process sharing the durable keyring and state.
            new AccountTotpService(provider).Verify("recovery-format", recoveryState, generated[i]);
            Check(recoveryState.RecoveryHashes.Count == 9 - i, "each recovery code destroys one stored digest");
        }
        try { security.Verify("recovery-format", recoveryState, generated[0]); throw new Exception("Used recovery code accepted"); }
        catch (ApiException error) when (error.Message == "TOTP_CODE_INVALID") { assertions++; }
        foreach (var legacy in new[] { "00123456", "00112233445566778899AABBCCDDEEFF" })
        {
            recoveryState.RecoveryHashes.Add(Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(legacy))));
            security.Verify("recovery-format", recoveryState, legacy.ToLowerInvariant());
            Check(recoveryState.RecoveryHashes.Count == 0, "old stored codes and leading zeros remain usable once");
        }
        // RFC 6238 SHA-1 vectors, reduced to the six digits accepted by authenticators.
        var vectorSecret = Encoding.ASCII.GetBytes("12345678901234567890");
        var vectors = new (long Time, string Code)[] { (59, "287082"), (1111111109, "081804"),
            (1111111111, "050471"), (1234567890, "005924"), (2000000000, "279037"), (20000000000, "353130") };
        foreach (var vector in vectors) Check(AccountTotpService.Code(vectorSecret, vector.Time / 30) == vector.Code, "RFC6238 SHA1 vector");
        string path = Path.Combine(Root, "privacy-security.json");
        string aliceToken, aliceId, bobToken, bobId, secret;
        string[] recovery;
        await using (var server = await Server.Start(path, legacyEnabled: false, legacyWssEnabled: false))
        {
            async Task<JsonNode> Register(string username) => await server.Json("POST", "/api/auth/register",
                new { username, displayName = username, password = "isolated-security-password" });
            var alice = await Register("security_alice"); var bob = await Register("security_bob");
            aliceToken = alice["token"]!.GetValue<string>(); aliceId = alice["user"]!["id"]!.GetValue<string>();
            bobToken = bob["token"]!.GetValue<string>(); bobId = bob["user"]!["id"]!.GetValue<string>();
            async Task<P.AccountPrivacyResponse> Rpc(P.AccountPrivacyRequest input, string? token = null, int status = 200, string? error = null)
            {
                var response = await server.Raw(new P.RpcRequest { ProtocolMajor = 1, RequestId = "security", AccountPrivacy = input }.ToByteArray(), status, error, token ?? aliceToken);
                return response.AccountPrivacy;
            }
            foreach (P.PrivacyKey key in Enum.GetValues<P.PrivacyKey>().Where(k => k != P.PrivacyKey.Unspecified))
            {
                var initial = await Rpc(new() { GetRules = new() { Key = key } });
                Check(initial.Rules.Rules.Single().Kind == P.PrivacyRuleKind.AllowAll, "privacy default");
                var rule = new P.PrivacyRuleSet { Key = key, Rules = { new P.PrivacyRule { Kind = P.PrivacyRuleKind.AllowUsers,
                    Users = { new P.UserRef { ServerDomain = Domain, UserId = bobId } } }, new P.PrivacyRule { Kind = P.PrivacyRuleKind.DisallowAll } } };
                Check((await Rpc(new() { SetRules = rule })).Rules.Equals(rule), "privacy rules roundtrip");
            }
            await Rpc(new() { SetGlobal = new() { HideReadMarks = true, NewNoncontactPeersRequirePremium = true } });
            Check((await Rpc(new() { GetGlobal = new() })).Global.HideReadMarks, "global settings roundtrip");
            await Rpc(new() { SetAccountTtl = new() { Days = 180 } });
            Check((await Rpc(new() { GetAccountTtl = new() })).AccountTtl.Days == 180, "account ttl persistence");
            await Rpc(new() { SetDefaultHistoryTtl = new() { Seconds = 86400 } });
            Check((await Rpc(new() { GetDefaultHistoryTtl = new() })).HistoryTtl.Seconds == 86400, "default history ttl persistence");
            await Rpc(new() { BlockUser = new() { ServerDomain = Domain, UserId = bobId } });
            await Rpc(new() { BlockUser = new() { ServerDomain = Domain, UserId = bobId } });
            Check((await Rpc(new() { GetBlocked = new() { Limit = 100 } })).Blocked.Count == 1, "block idempotency");
            Check((await Rpc(new() { GetBlocked = new() { Limit = 100 } }, bobToken)).Blocked.Count == 0, "blacklist owner isolation");
            await Rpc(new() { UnblockUser = new() { ServerDomain = Domain, UserId = bobId } });
            await Rpc(new() { SetAccountTtl = new() { Days = 0 } }, status: 400, error: "TTL_DAYS_INVALID");
            var status = await server.Json("GET", "/api/security/totp", null, aliceToken);
            Check(!status["enabled"]!.GetValue<bool>(), "TOTP off by default");
            await server.Json("POST", "/api/security/totp/setup", new { code = "" }, status: 401);
            var enrollment = await server.Json("POST", "/api/security/totp/setup", new { code = "" }, aliceToken);
            secret = enrollment["secret"]!.GetValue<string>();
            Check(secret.Length == 32 && enrollment["uri"]!.GetValue<string>().Contains("otpauth://totp/"), "TOTP provisioning URI");
            Check(!(await server.Json("GET", "/api/security/totp", null, bobToken))["enabled"]!.GetValue<bool>(), "TOTP owner isolation");
            var enrollmentCode = TotpCode(secret);
            var confirmed = await server.Json("POST", "/api/security/totp/confirm", new { code = enrollmentCode }, aliceToken);
            recovery = confirmed["recoveryCodes"]!.AsArray().Select(c => c!.GetValue<string>()).ToArray();
            Check(recovery.Length == 10 && recovery.Distinct().Count() == 10
                && recovery.All(code => code.Length == 8 && code.All(char.IsAsciiDigit)), "ten eight-digit recovery codes from actual HTTP endpoint");
            await server.Json("POST", "/api/security/totp/disable", new { code = "" }, aliceToken, 400);
            Check((await server.Json("GET", "/api/security/totp", null, aliceToken))["enabled"]!.GetValue<bool>(), "empty management code preserves bearer and protection");
            await server.Json("POST", "/api/auth/login", new { username = "security_alice", password = "isolated-security-password" }, status: 401);
            var invalid = await server.Json("POST", "/api/auth/login", new { username = "security_alice", password = "isolated-security-password", totpCode = enrollmentCode }, status: 400);
            Check(invalid["error"]!.GetValue<string>() == "TOTP_CODE_INVALID", "enrollment code cannot be replayed for login");
            await server.Json("POST", "/api/auth/login", new { username = "security_alice", password = "wrong-password", totpCode = recovery[0] }, status: 401);
            var login = await server.Json("POST", "/api/auth/login", new { username = "security_alice", password = "isolated-security-password", totpCode = recovery[0] });
            Check(login["token"] is not null, "TOTP recovery login");
            await server.Json("POST", "/api/auth/login", new { username = "security_alice", password = "isolated-security-password", totpCode = recovery[0] }, status: 400);
            Check((await server.Json("GET", "/api/security/totp", null, aliceToken))["recoveryCodesRemaining"]!.GetValue<int>() == 9, "recovery consumed exactly once");
            // Verify no secret or usable recovery code is persisted in plaintext.
            string snapshot = await File.ReadAllTextAsync(path);
            Check(!snapshot.Contains(secret) && recovery.All(code => !snapshot.Contains(code)), "TOTP secrets protected at rest");
        }
        await using (var server = await Server.Start(path, legacyEnabled: false, legacyWssEnabled: false))
        {
            Check((await server.Json("GET", "/api/security/totp", null, aliceToken))["enabled"]!.GetValue<bool>(), "TOTP persists after restart");
            await server.Json("POST", "/api/auth/login", new { username = "security_alice", password = "isolated-security-password" }, status: 401);
            await server.Json("POST", "/api/auth/login", new { username = "security_alice", password = "isolated-security-password", totpCode = recovery[0] }, status: 400);
            var step = DateTimeOffset.UtcNow.ToUnixTimeSeconds() / 30;
            while (DateTimeOffset.UtcNow.ToUnixTimeSeconds() / 30 == step) await Task.Delay(250);
            var verifiedLogin = await server.Json("POST", "/api/auth/login", new { username = "security_alice", password = "isolated-security-password", totpCode = TotpCode(secret) });
            Check(verifiedLogin["token"] is not null, "fresh authenticator code works after restart with durable keyring");
            var replaced = await server.Json("POST", "/api/security/totp/recovery", new { code = recovery[1] }, aliceToken);
            await server.Json("POST", "/api/security/totp/disable", new { code = recovery[2] }, aliceToken, 400);
            await server.Json("POST", "/api/security/totp/disable", new { code = replaced["recoveryCodes"]![0]!.GetValue<string>() }, aliceToken);
            await server.Json("POST", "/api/auth/login", new { username = "security_alice", password = "isolated-security-password" });
            Check(!(await server.Json("GET", "/api/security/totp", null, aliceToken))["enabled"]!.GetValue<bool>(), "TOTP disabled only after proof");
            var response = await server.Raw(new P.RpcRequest { ProtocolMajor = 1, RequestId = "restart", AccountPrivacy = new() { GetRules = new() { Key = P.PrivacyKey.PhoneCall } } }.ToByteArray(), 200, null, aliceToken);
            Check(response.AccountPrivacy.Rules.Rules.Last().Kind == P.PrivacyRuleKind.DisallowAll, "privacy rules survive restart");
            await server.Json("POST", "/api/security/totp/setup", new { code = "" }, bobToken);
            for (int i = 0; i < 5; i++)
                await server.Json("POST", "/api/security/totp/confirm", new { code = "wrong" }, bobToken, 400);
            var limited = await server.Json("POST", "/api/security/totp/confirm", new { code = "wrong" }, bobToken, 429);
            Check(limited["error"]!.GetValue<string>() == "TOTP_TOO_MANY_ATTEMPTS", "TOTP attempts limited per account across failed transactions");
        }
        Console.WriteLine($"PASS privacy/security HTTP and RFC6238 regressions, {assertions} assertions. Isolated data at {Root}");
    }

    private static string TotpCode(string secret)
    {
        const string alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZ234567";
        int buffer = 0, bits = 0;
        var bytes = new List<byte>();
        foreach (char c in secret)
        {
            buffer = (buffer << 5) | alphabet.IndexOf(c); bits += 5;
            if (bits >= 8) { bits -= 8; bytes.Add((byte)(buffer >> bits)); }
        }
        return AccountTotpService.Code(bytes.ToArray(), DateTimeOffset.UtcNow.ToUnixTimeSeconds() / 30);
    }
}
