using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.Security.Cryptography;
using System.Text;
using Microsoft.AspNetCore.DataProtection;

namespace Botgram.Backend;

public sealed class AccountTotpState
{
    public string Secret { get; set; } = "";
    public string PendingSecret { get; set; } = "";
    public DateTimeOffset PendingExpiresAt { get; set; }
    public long LastUsedStep { get; set; } = -1;
    public List<string> RecoveryHashes { get; set; } = [];
}

public sealed record TotpInput(string Code);
public sealed record TotpStatus(bool Enabled, int RecoveryCodesRemaining);
public sealed record TotpEnrollment(string Secret, string Uri, DateTimeOffset ExpiresAt);
public sealed record TotpRecovery(string[] RecoveryCodes);

public sealed class AccountTotpService(IDataProtectionProvider protection)
{
    private const string Alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZ234567";
    private sealed class Attempts { public int Count; public DateTimeOffset Reset; }
    private readonly ConcurrentDictionary<string, Attempts> attempts = new();
    private IDataProtector Protector(string uid) => protection.CreateProtector("Botgram.Totp.v1", uid);

    public static TotpStatus Status(AccountTotpState state) => new(state.Secret.Length != 0, state.RecoveryHashes.Count);

    public TotpEnrollment Begin(string uid, string username, AccountTotpState state)
    {
        if (state.Secret.Length != 0) throw new ApiException(409, "TOTP_ALREADY_ENABLED");
        var secret = RandomNumberGenerator.GetBytes(20);
        state.PendingSecret = Protector(uid).Protect(Convert.ToBase64String(secret));
        state.PendingExpiresAt = DateTimeOffset.UtcNow.AddMinutes(10);
        var text = Base32(secret);
        return new(text, "otpauth://totp/" + Uri.EscapeDataString("Nyarugram:" + username)
            + "?secret=" + text + "&issuer=Nyarugram&algorithm=SHA1&digits=6&period=30", state.PendingExpiresAt);
    }

    public TotpRecovery Confirm(string uid, AccountTotpState state, string code)
    {
        if (state.Secret.Length != 0) throw new ApiException(409, "TOTP_ALREADY_ENABLED");
        if (state.PendingSecret.Length == 0 || state.PendingExpiresAt <= DateTimeOffset.UtcNow)
            throw new ApiException(400, "TOTP_ENROLLMENT_EXPIRED");
        Limit(uid);
        var step = Match(uid, state.PendingSecret, code, -1);
        state.Secret = state.PendingSecret;
        state.PendingSecret = "";
        state.PendingExpiresAt = default;
        state.LastUsedStep = step;
        return Recovery(uid, state);
    }

    public void Verify(string uid, AccountTotpState state, string? code)
    {
        if (state.Secret.Length == 0) return;
        if (string.IsNullOrEmpty(code)) throw new ApiException(401, "TOTP_REQUIRED");
        Limit(uid);
        var normalized = code.Replace("-", "").Replace(" ", "").ToUpperInvariant();
        if (normalized.Length == 8 || normalized.Length == 32)
        {
            var digest = SHA256.HashData(Encoding.UTF8.GetBytes(normalized));
            var keyedDigest = state.RecoveryHashes.Any(hash => hash.StartsWith("v2:", StringComparison.Ordinal))
                ? RecoveryDigest(uid, state, normalized) : [];
            var index = state.RecoveryHashes.FindIndex(hash => hash.StartsWith("v2:", StringComparison.Ordinal)
                ? CryptographicOperations.FixedTimeEquals(Convert.FromHexString(hash[3..]), keyedDigest)
                : CryptographicOperations.FixedTimeEquals(Convert.FromHexString(hash), digest));
            if (index < 0) throw new ApiException(400, "TOTP_CODE_INVALID");
            state.RecoveryHashes.RemoveAt(index);
            return;
        }
        state.LastUsedStep = Match(uid, state.Secret, normalized, state.LastUsedStep);
    }

    public object Execute(string uid, string username, AccountTotpState state, string operation, string? code)
    {
        if (operation == "status") return Status(state);
        if (operation == "setup") return Begin(uid, username, state);
        if (operation == "confirm") return Confirm(uid, state, code ?? "");
        if (state.Secret.Length == 0) throw new ApiException(400, "TOTP_NOT_ENABLED");
        if (string.IsNullOrWhiteSpace(code)) throw new ApiException(400, "TOTP_CODE_INVALID");
        Verify(uid, state, code);
        if (operation == "recovery") return Recovery(uid, state);
        if (operation != "disable") throw new ApiException(400, "TOTP_OPERATION_INVALID");
        state.Secret = ""; state.PendingSecret = ""; state.RecoveryHashes.Clear(); state.LastUsedStep = -1;
        return Status(state);
    }

    private void Limit(string uid)
    {
        var now = DateTimeOffset.UtcNow;
        if (attempts.Count > 10000)
            foreach (var entry in attempts.Where(p => p.Value.Reset <= now)) attempts.TryRemove(entry.Key, out _);
        var item = attempts.GetOrAdd(uid, _ => new());
        lock (item)
        {
            if (item.Reset <= now) { item.Reset = now.AddMinutes(1); item.Count = 0; }
            if (++item.Count > 5) throw new ApiException(429, "TOTP_TOO_MANY_ATTEMPTS");
        }
    }

    private long Match(string uid, string encryptedSecret, string code, long previous)
    {
        if (code.Length != 6 || code.Any(c => c < '0' || c > '9')) throw new ApiException(400, "TOTP_CODE_INVALID");
        var secret = Convert.FromBase64String(Protector(uid).Unprotect(encryptedSecret));
        try
        {
            var current = DateTimeOffset.UtcNow.ToUnixTimeSeconds() / 30;
            for (var step = current - 1; step <= current + 1; step++)
                if (step > previous && CryptographicOperations.FixedTimeEquals(Encoding.ASCII.GetBytes(Code(secret, step)), Encoding.ASCII.GetBytes(code))) return step;
            throw new ApiException(400, "TOTP_CODE_INVALID");
        }
        finally { CryptographicOperations.ZeroMemory(secret); }
    }

    public static string Code(byte[] secret, long step)
    {
        Span<byte> counter = stackalloc byte[8];
        BinaryPrimitives.WriteInt64BigEndian(counter, step);
        var digest = HMACSHA1.HashData(secret, counter);
        int offset = digest[^1] & 15;
        return ((BinaryPrimitives.ReadUInt32BigEndian(digest.AsSpan(offset, 4)) & 0x7fffffff) % 1_000_000).ToString("D6", System.Globalization.CultureInfo.InvariantCulture);
    }

    private byte[] RecoveryDigest(string uid, AccountTotpState state, string code)
    {
        var key = Convert.FromBase64String(Protector(uid).Unprotect(state.Secret));
        try { return HMACSHA256.HashData(key, Encoding.UTF8.GetBytes(code)); }
        finally { CryptographicOperations.ZeroMemory(key); }
    }

    private TotpRecovery Recovery(string uid, AccountTotpState state)
    {
        var unique = new HashSet<string>(StringComparer.Ordinal);
        while (unique.Count < 10)
            unique.Add(RandomNumberGenerator.GetInt32(100_000_000).ToString("D8", System.Globalization.CultureInfo.InvariantCulture));
        var codes = unique.ToArray();
        state.RecoveryHashes = codes.Select(code => "v2:" + Convert.ToHexString(RecoveryDigest(uid, state, code))).ToList();
        return new(codes);
    }

    private static string Base32(byte[] bytes)
    {
        var output = new StringBuilder();
        int buffer = 0, bits = 0;
        foreach (var value in bytes)
        {
            buffer = (buffer << 8) | value; bits += 8;
            while (bits >= 5) { bits -= 5; output.Append(Alphabet[(buffer >> bits) & 31]); }
        }
        if (bits > 0) output.Append(Alphabet[(buffer << (5 - bits)) & 31]);
        return output.ToString();
    }
}
