using System.Net;
using System.Text.Json.Nodes;
using Botgram.Backend;
using Botgram.Backend.Storage;
using StackExchange.Redis;

internal static class CacheTests
{
    // Call before the PostgreSQL-only runner's optional-environment early return.
    public static async Task RunAsync()
    {
        await FakeAsync();
        Console.WriteLine("PASS: message cache fake transport tests (not a real Redis fault exercise)");
        var connection = Environment.GetEnvironmentVariable("BOTGRAM_REDIS_TEST_CONNECTION_STRING");
        if (string.IsNullOrWhiteSpace(connection))
        {
            Console.WriteLine("SKIP: real Redis tests require BOTGRAM_REDIS_TEST_CONNECTION_STRING (loopback only)");
            return;
        }
        await RealAsync(connection);
        Console.WriteLine("PASS: real loopback Redis roundtrip, namespace key deletion/repopulation and real client connection loss (disposed original transport); authority fallback and injected PG read failure propagation (not Redis server shutdown or a real PG outage)");
    }

    private static Message Sample(long id = 7, string conversation = "chat") =>
        new(id, conversation, "a", "client", "hello", DateTimeOffset.UnixEpoch,
            ServiceMembers: ["a", "b"], TelegramEntities: [new(MessageEntityKind.Bold, 0, 5)], Silent: true);

    private static async Task FakeAsync()
    {
        var fake = new FakeTransport();
        var cache = new RedisMessageCache(fake, "test", TimeSpan.FromMinutes(1), TimeSpan.FromMilliseconds(20));
        var generation = Guid.NewGuid();
        var source = Sample();
        int authorizations = 0, reads = 0;
        bool allowed = true, authorityFails = false;
        Task Authorize(CancellationToken token)
        {
            token.ThrowIfCancellationRequested();
            authorizations++;
            if (!allowed) throw new ApiException(403, "revoked");
            return Task.CompletedTask;
        }
        Task<Message> Source(CancellationToken token)
        {
            token.ThrowIfCancellationRequested();
            reads++;
            if (authorityFails) throw new AuthorityFailed();
            return Task.FromResult(source);
        }
        Task<Message> Read(CancellationToken token = default) =>
            cache.ReadThroughAsync(generation, "chat", 7, Authorize, Source, token);

        var first = await Read();
        Check(reads == 1 && authorizations == 1 && first.Id == source.Id && first.Text == source.Text, "initial authority read");
        Check(fake.LastTtl == TimeSpan.FromMinutes(1) && fake.LastKey!.StartsWith("botgram:v1:test:message:", StringComparison.Ordinal),
            "TTL or environment prefix");
        first.ServiceMembers![0] = "mutated";
        first.TelegramEntities![0] = new(MessageEntityKind.Italic, 0, 5);
        Check(source.ServiceMembers![0] == "a" && source.TelegramEntities![0].Kind == MessageEntityKind.Bold,
            "cache exposed authoritative arrays");
        var hit = await Read();
        Check(reads == 1 && authorizations == 2 && hit.ServiceMembers![0] == "a" &&
            hit.TelegramEntities![0].Kind == MessageEntityKind.Bold && hit.Silent, "immutable message roundtrip/cache hit");
        hit.ServiceMembers![0] = "second mutation";
        Check((await Read()).ServiceMembers![0] == "a", "cache hit shared mutable arrays");

        allowed = false;
        int gets = fake.Gets;
        await ExpectAsync<ApiException>(() => Read());
        Check(fake.Gets == gets && reads == 1, "revocation did not stop before cached read");
        allowed = true;
        fake.Clear();
        await Read();
        Check(reads == 2, "fake cache clearing did not rebuild");
        fake.Disconnected = true;
        await Read();
        Check(reads == 3, "fake disconnection did not fall back");
        authorityFails = true;
        await ExpectAsync<AuthorityFailed>(() => Read());
        Check(reads == 4, "authority failure was hidden");
        fake.Disconnected = false;
        fake.Clear();
        await ExpectAsync<AuthorityFailed>(() => Read());
        Check(fake.Values.Count == 0, "failed authority populated cache");
        authorityFails = false;
        fake.WriteFails = true;
        Check((await Read()).Id == 7, "cache write failure changed successful authority result");
        fake.WriteFails = false;
        await Read();

        foreach (var corruption in new[] { "json", "key", "generation", "id", "conversation", "null", "entity" })
        {
            var node = JsonNode.Parse(fake.Values[fake.LastKey!])!;
            switch (corruption)
            {
                case "key": node["Key"] = "other"; break;
                case "generation": node["Key"] = fake.LastKey!.Replace(generation.ToString("N"), Guid.NewGuid().ToString("N")); break;
                case "id": node["Message"]!["Id"] = 8; break;
                case "conversation": node["Message"]!["ConversationId"] = "other"; break;
                case "null": node["Message"] = null; break;
                case "entity": node["Message"]!["TelegramEntities"]![0] = new JsonObject(); break;
            }
            fake.Values[fake.LastKey!] = corruption == "json" ? "{broken" : node.ToJsonString();
            int before = reads;
            Check((await Read()).Id == 7 && reads == before + 1, "corrupt cache did not fall back: " + corruption);
        }

        var originalKey = fake.LastKey!;
        int beforeGeneration = reads;
        await cache.ReadThroughAsync(Guid.NewGuid(), "chat", 7, Authorize, Source);
        Check(reads == beforeGeneration + 1 && fake.LastKey != originalKey, "generation namespace collision");
        var otherEnvironment = new RedisMessageCache(fake, "other", TimeSpan.FromSeconds(1));
        await otherEnvironment.ReadThroughAsync(generation, "chat", 7, Authorize, Source);
        Check(fake.LastKey!.StartsWith("botgram:v1:other:", StringComparison.Ordinal), "environment namespace collision");

        fake.Clear();
        fake.HangRead = true;
        int beforeTimeout = reads;
        await Read();
        Check(reads == beforeTimeout + 1, "cache timeout did not fall back");
        fake.HangRead = false;
        fake.Clear();
        fake.HangWrite = true;
        Check((await Read()).Id == 7, "cache write timeout failed committed read");
        fake.HangWrite = false;

        using var canceled = new CancellationTokenSource();
        canceled.Cancel();
        int beforeCancel = authorizations;
        await ExpectAsync<OperationCanceledException>(() => Read(canceled.Token));
        Check(authorizations == beforeCancel, "pre-cancellation ignored");
        using var duringGet = new CancellationTokenSource();
        fake.HangRead = true;
        fake.OnGet = duringGet.Cancel;
        int beforeCanceledRead = reads;
        await ExpectAsync<OperationCanceledException>(() => Read(duringGet.Token));
        Check(reads == beforeCanceledRead, "canceled cache get fell back to authority");
        fake.HangRead = false;
        fake.OnGet = null;
        fake.Clear();
        await ExpectAsync<OperationCanceledException>(() => cache.ReadThroughAsync(generation, "chat", 7,
            _ => Task.FromException(new OperationCanceledException()), Source));
        await ExpectAsync<OperationCanceledException>(() => cache.ReadThroughAsync(generation, "chat", 7,
            Authorize, _ => Task.FromException<Message>(new OperationCanceledException())));
        using var duringSet = new CancellationTokenSource();
        fake.OnSet = duringSet.Cancel;
        await ExpectAsync<OperationCanceledException>(() => Read(duringSet.Token));
        fake.OnSet = null;

        fake.Clear();
        await ExpectAsync<IOException>(() => cache.ReadThroughAsync(generation, "chat", 7, Authorize,
            _ => Task.FromException<Message>(new IOException("authoritative PG unavailable"))));
        await ExpectAsync<TimeoutException>(() => cache.ReadThroughAsync(generation, "chat", 7, Authorize,
            _ => Task.FromException<Message>(new TimeoutException("authoritative PG timeout"))));
        await ExpectAsync<InvalidDataException>(() => cache.ReadThroughAsync(generation, "chat", 7, Authorize,
            _ => Task.FromResult<Message>(null!)));
        await ExpectAsync<InvalidDataException>(() => cache.ReadThroughAsync(generation, "chat", 7, Authorize,
            _ => Task.FromResult(Sample(8))));
        Check(fake.Values.Count == 0, "invalid authority response was cached");
        foreach (var ttl in new[] { TimeSpan.Zero, TimeSpan.FromTicks(1), TimeSpan.FromSeconds(-1), RedisMessageCache.MaximumTtl + TimeSpan.FromMilliseconds(1) })
            Expect<ArgumentOutOfRangeException>(() => new RedisMessageCache(fake, "test", ttl));
        Expect<ArgumentOutOfRangeException>(() => new RedisMessageCache(fake, "test", TimeSpan.FromSeconds(1), TimeSpan.Zero));
        Expect<ArgumentOutOfRangeException>(() => new RedisMessageCache(fake, "test", TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(3)));
        foreach (var environment in new[] { "", "Test", "test:other", "test\n" })
            Expect<ArgumentException>(() => new RedisMessageCache(fake, environment, TimeSpan.FromSeconds(1)));
        await ExpectAsync<ArgumentException>(() => cache.ReadThroughAsync(Guid.Empty, "chat", 7, Authorize, Source));
        await ExpectAsync<ArgumentOutOfRangeException>(() => cache.ReadThroughAsync(generation, "chat", 0, Authorize, Source));
    }

    private static async Task RealAsync(string connection)
    {
        var options = ConfigurationOptions.Parse(connection);
        if (options.EndPoints.Count == 0 || !string.IsNullOrEmpty(options.ServiceName) ||
            options.EndPoints.Any(endpoint => endpoint switch
            {
                IPEndPoint ip => !IPAddress.IsLoopback(ip.Address),
                DnsEndPoint dns => !string.Equals(dns.Host, "localhost", StringComparison.OrdinalIgnoreCase) &&
                    !(IPAddress.TryParse(dns.Host, out var address) && IPAddress.IsLoopback(address)),
                _ => true
            }))
            throw new InvalidOperationException("Redis integration tests require explicit loopback endpoints, without service discovery");
        options.AllowAdmin = false;
        options.AbortOnConnectFail = true;
        options.ConnectRetry = 0;
        options.ConnectTimeout = 1500;
        options.AsyncTimeout = 1000;
        options.TieBreaker = "";
        using var connectionHandle = await ConnectionMultiplexer.ConnectAsync(options);
        var database = connectionHandle.GetDatabase();
        var tracked = new TrackedRedisTransport(new RedisMessageCacheTransport(database));
        var environment = "test_" + Guid.NewGuid().ToString("N");
        var cache = new RedisMessageCache(tracked, environment, TimeSpan.FromSeconds(30), TimeSpan.FromSeconds(2));
        var generation = Guid.NewGuid();
        int reads = 0, authorizations = 0;
        bool allowed = true;
        Task Authorize(CancellationToken token)
        {
            token.ThrowIfCancellationRequested();
            authorizations++;
            if (!allowed) throw new ApiException(403, "revoked");
            return Task.CompletedTask;
        }
        Task<Message> Source(CancellationToken token)
        {
            token.ThrowIfCancellationRequested();
            reads++;
            return Task.FromResult(Sample());
        }
        Task<Message> Read() => cache.ReadThroughAsync(generation, "chat", 7, Authorize, Source);
        try
        {
            await Read();
            var key = tracked.Keys.Single();
            Check(key.StartsWith($"botgram:v1:{environment}:", StringComparison.Ordinal), "real Redis namespace");
            Check(await database.KeyExistsAsync(key), "real Redis was not populated");
            var ttl = await database.KeyTimeToLiveAsync(key);
            Check(ttl.HasValue && ttl.Value > TimeSpan.Zero && ttl.Value <= TimeSpan.FromSeconds(30), "real Redis TTL");
            var hit = await Read();
            Check(hit.Id == 7 && reads == 1 && authorizations == 2, "real Redis roundtrip hit");
            allowed = false;
            await ExpectAsync<ApiException>(Read);
            Check(reads == 1, "real Redis cached payload bypassed revocation");
            allowed = true;
            await database.KeyDeleteAsync(key);
            await Read();
            Check(reads == 2, "real Redis namespace deletion did not repopulate from authority");
            await database.StringSetAsync(key, "{bad", TimeSpan.FromSeconds(30));
            await Read();
            Check(reads == 3, "real Redis corrupt payload did not repopulate");
            await database.KeyDeleteAsync(key);
            await ExpectAsync<AuthorityFailed>(() => cache.ReadThroughAsync(generation, "chat", 7, Authorize,
                _ => Task.FromException<Message>(new AuthorityFailed())));
            Check(!await database.KeyExistsAsync(key), "real Redis authority failure became a cached success");
        }
        finally
        {
            // No FLUSHDB, scan, or deletion of any key outside this invocation's random namespace.
            foreach (var key in tracked.Keys)
            {
                Check(key.StartsWith($"botgram:v1:{environment}:", StringComparison.Ordinal), "cleanup namespace escaped");
                await database.KeyDeleteAsync(key);
            }
        }

        // Cleanup above must finish while connected; keep the cache bound to the original transport.
        connectionHandle.Dispose();
        var disposedKey = tracked.Keys.Single();
        await ExpectAsync<ObjectDisposedException>(() => tracked.GetAsync(disposedKey, CancellationToken.None));
        int readsBeforeLoss = reads, authorizationsBeforeLoss = authorizations;
        var fallback = await Read();
        Check(fallback.Id == 7 && fallback.Text == "hello" && reads == readsBeforeLoss + 1 &&
            authorizations == authorizationsBeforeLoss + 1, "real Redis client connection loss did not fall back to authority");
        foreach (var failure in new Exception[]
        {
            new IOException("injected authoritative PG read failure"),
            new TimeoutException("injected authoritative PG read timeout")
        })
        {
            int failedReads = 0;
            var observed = false;
            try
            {
                await cache.ReadThroughAsync(generation, "chat", 7, Authorize, token =>
                {
                    token.ThrowIfCancellationRequested();
                    failedReads++;
                    return Task.FromException<Message>(failure);
                });
            }
            catch (Exception exception) when (ReferenceEquals(exception, failure))
            {
                observed = true;
            }
            Check(observed && failedReads == 1, "disposed Redis connection hid authoritative PG failure or returned success");
        }
        Check(authorizations == authorizationsBeforeLoss + 3, "client connection loss bypassed authorization");
    }

    private static void Check(bool condition, string name)
    {
        if (!condition) throw new InvalidOperationException("Cache test failed: " + name);
    }

    private static async Task ExpectAsync<T>(Func<Task> action) where T : Exception
    {
        try { await action(); }
        catch (T) { return; }
        throw new InvalidOperationException("Expected " + typeof(T).Name);
    }

    private static void Expect<T>(Action action) where T : Exception
    {
        try { action(); }
        catch (T) { return; }
        throw new InvalidOperationException("Expected " + typeof(T).Name);
    }

    private sealed class AuthorityFailed : Exception { }

    private sealed class FakeTransport : IMessageCacheTransport
    {
        public Dictionary<string, string> Values { get; } = [];
        public bool Disconnected { get; set; }
        public bool WriteFails { get; set; }
        public bool HangRead { get; set; }
        public bool HangWrite { get; set; }
        public Action? OnGet { get; set; }
        public Action? OnSet { get; set; }
        public int Gets { get; private set; }
        public string? LastKey { get; private set; }
        public TimeSpan LastTtl { get; private set; }
        public void Clear() => Values.Clear();

        public Task<string?> GetAsync(string key, CancellationToken cancellationToken)
        {
            Gets++;
            LastKey = key;
            OnGet?.Invoke();
            cancellationToken.ThrowIfCancellationRequested();
            if (Disconnected) throw new IOException("fake disconnected");
            if (HangRead) return new TaskCompletionSource<string?>(TaskCreationOptions.RunContinuationsAsynchronously).Task;
            return Task.FromResult(Values.GetValueOrDefault(key));
        }

        public Task SetAsync(string key, string payload, TimeSpan ttl, CancellationToken cancellationToken)
        {
            OnSet?.Invoke();
            cancellationToken.ThrowIfCancellationRequested();
            if (Disconnected || WriteFails) throw new IOException("fake cache write failure");
            if (HangWrite) return new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously).Task;
            LastKey = key;
            LastTtl = ttl;
            Values[key] = payload;
            return Task.CompletedTask;
        }
    }

    private sealed class TrackedRedisTransport(IMessageCacheTransport inner) : IMessageCacheTransport
    {
        public HashSet<string> Keys { get; } = [];
        public Task<string?> GetAsync(string key, CancellationToken cancellationToken) => inner.GetAsync(key, cancellationToken);
        public Task SetAsync(string key, string payload, TimeSpan ttl, CancellationToken cancellationToken)
        {
            Keys.Add(key);
            return inner.SetAsync(key, payload, ttl, cancellationToken);
        }
    }
}
