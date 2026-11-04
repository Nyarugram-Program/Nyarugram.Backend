using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.Numerics;
using System.Security.Cryptography;
using Google.Protobuf;
using P = Botgram.Protocol.V1;

namespace Botgram.Backend;

public sealed class AccountPasswordState
{
    public byte[] Algorithm { get; set; } = [];
    public byte[] Verifier { get; set; } = [];
    public string Hint { get; set; } = "";
}

public static class AccountPasswordService
{
    public static object LoginInfo(User user, AccountPasswordState state) => new
    {
        passwordInfo = Convert.ToBase64String(Info(user.Id, "login:" + user.PasswordHash, state).ToByteArray())
    };

    public static void VerifyLogin(User user, AccountPasswordState state, string? encodedProof)
    {
        if (state.Verifier.Length == 0) return;
        if (string.IsNullOrEmpty(encodedProof)) throw new ApiException(401, "SESSION_PASSWORD_NEEDED");
        if (encodedProof.Length > 2048) throw new ApiException(400, "PASSWORD_HASH_INVALID");
        P.SrpProof proof;
        try { proof = P.SrpProof.Parser.ParseFrom(Convert.FromBase64String(encodedProof)); }
        catch (Exception error) when (error is FormatException or InvalidProtocolBufferException)
        { throw new ApiException(400, "PASSWORD_HASH_INVALID"); }
        Verify(user.Id, "login:" + user.PasswordHash, state, proof);
    }

    // Same audited 2048-bit safe prime used by the original Android SRP helper.
    public static readonly byte[] Prime = Convert.FromHexString(
        "c71caeb9c6b1c9048e6c522f70f13f73980d40238e3e21c14934d037563d930f48198a0aa7c14058229493d22530f4dbfa336f6e0ac925139543aed44cce7c3720fd51f69458705ac68cd4fe6b6b13abdc9746512969328454f18faf8c595f642477fe96bb2a941d5bcd1d4ac8cc49880708fa9b378e3c4f3a9060bee67cf9a4a4a695811051907e162753b56b0f6b410dba74d8a84b2a14b3144e0ef1284754fd17ed950d5965b4b9dd46582db1178d169c6bc465b0d6ff9ca3928fef5b9ae4e418fc15e83ebea0f87fa9ff5eed70050ded2849f47bf959d956850ce929851f0d8115f635b105ee2e4e15d04b2454bf6f4fadf034b10403119cd8e3b92fcc5b");
    private static readonly BigInteger Modulus = Number(Prime);
    private static readonly byte[] Generator = Pad(new BigInteger(3));
    private static readonly BigInteger Multiplier = Number(Hash(Prime, Generator));
    private sealed record Challenge(string User, string Scope, byte[] Verifier, byte[] Secret,
        byte[] Public, DateTimeOffset ExpiresAt);
    private static readonly ConcurrentDictionary<long, Challenge> challenges = new();

    private static BigInteger Number(ReadOnlySpan<byte> value) => new(value, isUnsigned: true, isBigEndian: true);

    private static byte[] Pad(BigInteger value)
    {
        var bytes = value.ToByteArray(isUnsigned: true, isBigEndian: true);
        if (bytes.Length > 256) throw new ApiException(400, "PASSWORD_HASH_INVALID");
        var result = new byte[256];
        bytes.CopyTo(result.AsSpan(256 - bytes.Length));
        return result;
    }

    private static byte[] Hash(params byte[][] values)
    {
        using var hash = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
        foreach (var bytes in values) hash.AppendData(bytes);
        return hash.GetHashAndReset();
    }

    public static P.PasswordInfo Info(string uid, string scope, AccountPasswordState state)
    {
        var now = DateTimeOffset.UtcNow;
        foreach (var pair in challenges.Where(p => p.Value.ExpiresAt <= now)) challenges.TryRemove(pair.Key, out _);
        foreach (var pair in challenges.Where(p => p.Value.User == uid && p.Value.Scope == scope)
            .OrderByDescending(p => p.Value.ExpiresAt).Skip(8)) challenges.TryRemove(pair.Key, out _);
        if (challenges.Count >= 10000) throw new ApiException(503, "PASSWORD_CHALLENGE_CAPACITY");
        var result = new P.PasswordInfo
        {
            HasPassword = state.Verifier.Length != 0, Hint = state.Hint,
            NewAlgo = new() { G = 3, P = ByteString.CopyFrom(Prime), Salt1 = ByteString.CopyFrom(RandomNumberGenerator.GetBytes(16)),
                Salt2 = ByteString.CopyFrom(RandomNumberGenerator.GetBytes(16)) },
            NewSecureSalt = ByteString.CopyFrom(RandomNumberGenerator.GetBytes(32)),
            SecureRandom = ByteString.CopyFrom(RandomNumberGenerator.GetBytes(32))
        };
        if (!result.HasPassword) return result;
        result.CurrentAlgo = P.PasswordKdf.Parser.ParseFrom(state.Algorithm);
        var secret = RandomNumberGenerator.GetBytes(256);
        var b = Pad((Multiplier * Number(state.Verifier) + BigInteger.ModPow(3, Number(secret), Modulus)) % Modulus);
        var challenge = new Challenge(uid, scope, state.Verifier.ToArray(), secret, b, now.AddMinutes(5));
        long id;
        do { id = BinaryPrimitives.ReadInt64BigEndian(RandomNumberGenerator.GetBytes(8)) & long.MaxValue; }
        while (id == 0 || !challenges.TryAdd(id, challenge));
        result.SrpId = id;
        result.SrpB = ByteString.CopyFrom(b);
        return result;
    }

    public static void Verify(string uid, string scope, AccountPasswordState state, P.SrpProof proof)
    {
        if (state.Verifier.Length == 0)
        {
            if (!proof.Empty || proof.SrpId != 0 || proof.A.Length != 0 || proof.M1.Length != 0)
                throw new ApiException(400, "PASSWORD_HASH_INVALID");
            return;
        }
        if (proof.Empty || proof.A.Length != 256 || proof.M1.Length != 32)
            throw new ApiException(400, "PASSWORD_HASH_INVALID");
        if (!challenges.TryRemove(proof.SrpId, out var challenge) || challenge.User != uid || challenge.Scope != scope
            || challenge.ExpiresAt <= DateTimeOffset.UtcNow || !CryptographicOperations.FixedTimeEquals(challenge.Verifier, state.Verifier))
            throw new ApiException(400, "SRP_ID_INVALID");
        var a = Number(proof.A.Span);
        if (a <= 1 || a >= Modulus - 1) throw new ApiException(400, "PASSWORD_HASH_INVALID");
        var algo = P.PasswordKdf.Parser.ParseFrom(state.Algorithm);
        var u = Number(Hash(proof.A.ToByteArray(), challenge.Public));
        if (u == 0) throw new ApiException(400, "PASSWORD_HASH_INVALID");
        var shared = BigInteger.ModPow((a * BigInteger.ModPow(Number(state.Verifier), u, Modulus)) % Modulus,
            Number(challenge.Secret), Modulus);
        var primeHash = Hash(Prime);
        var generatorHash = Hash(Generator);
        for (var i = 0; i < primeHash.Length; i++) primeHash[i] ^= generatorHash[i];
        var expected = Hash(primeHash, Hash(algo.Salt1.ToByteArray()), Hash(algo.Salt2.ToByteArray()),
            proof.A.ToByteArray(), challenge.Public, Hash(Pad(shared)));
        if (!CryptographicOperations.FixedTimeEquals(expected, proof.M1.Span))
            throw new ApiException(400, "PASSWORD_HASH_INVALID");
    }

    public static void Update(string uid, string scope, AccountPasswordState state, P.PasswordUpdate input)
    {
        if (input.Proof is null) throw new ApiException(400, "PASSWORD_HASH_INVALID");
        Verify(uid, scope, state, input.Proof);
        if (input.ChangeSecureSettings) throw new ApiException(501, "SECURE_VALUES_UNSUPPORTED");
        if (input.ChangeEmail && input.Email.Length != 0) throw new ApiException(503, "EMAIL_RECOVERY_NOT_CONFIGURED");
        if (!input.ChangePassword) return;
        if (input.Hint.Length > 128) throw new ApiException(400, "PASSWORD_HINT_TOO_LONG");
        if (input.NewPasswordHash.Length == 0)
        {
            state.Algorithm = [];
            state.Verifier = [];
            state.Hint = "";
            return;
        }
        var algo = input.NewAlgo;
        if (algo is null || algo.G != 3 || !algo.P.Span.SequenceEqual(Prime)
            || algo.Salt1.Length is < 32 or > 128 || algo.Salt2.Length is < 8 or > 128
            || input.NewPasswordHash.Length != 256)
            throw new ApiException(400, "PASSWORD_HASH_INVALID");
        var verifier = Number(input.NewPasswordHash.Span);
        if (verifier <= 1 || verifier >= Modulus - 1) throw new ApiException(400, "PASSWORD_HASH_INVALID");
        state.Algorithm = algo.ToByteArray();
        state.Verifier = input.NewPasswordHash.ToByteArray();
        state.Hint = input.Hint;
    }
}
