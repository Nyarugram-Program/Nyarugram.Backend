using System.Net.Security;
using System.Net.WebSockets;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using System.Text.Json;
using System.Text.Json.Nodes;
using Google.Protobuf;
using P = Botgram.Protocol.V1;

internal static partial class HttpTests
{
    private static bool tlsEnabled, proxyTlsEnabled;
    private static string certificatePath = "", certificatePassword = "", certificateHash = "";
    private static string certificatePemPath = "", privateKeyPemPath = "", caddyPath = "";
    private static bool ValidateCertificate(X509Certificate? certificate, SslPolicyErrors errors)
    {
        if (certificate is null || (errors & ~SslPolicyErrors.RemoteCertificateChainErrors) != 0) return false;
        using var leaf = new X509Certificate2(certificate);
        return Convert.ToHexString(SHA256.HashData(leaf.RawData)) == certificateHash
            && DateTime.UtcNow >= leaf.NotBefore.ToUniversalTime() && DateTime.UtcNow <= leaf.NotAfter.ToUniversalTime();
    }

    private static void BeginTls()
    {
        tlsEnabled = true;
        using var key = RSA.Create(2048);
        var request = new CertificateRequest("CN=localhost", key, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
        var san = new SubjectAlternativeNameBuilder(); san.AddDnsName("localhost");
        request.CertificateExtensions.Add(san.Build());
        request.CertificateExtensions.Add(new X509BasicConstraintsExtension(false, false, 0, true));
        request.CertificateExtensions.Add(new X509EnhancedKeyUsageExtension(new OidCollection { new Oid("1.3.6.1.5.5.7.3.1") }, true));
        using var cert = request.CreateSelfSigned(DateTimeOffset.UtcNow.AddMinutes(-5), DateTimeOffset.UtcNow.AddHours(2));
        certificateHash = Convert.ToHexString(SHA256.HashData(cert.RawData));
        certificatePassword = Convert.ToHexString(RandomNumberGenerator.GetBytes(32));
        certificatePath = Path.Combine(Root, "temporary-localhost.pfx");
        File.WriteAllBytes(certificatePath, cert.Export(X509ContentType.Pfx, certificatePassword));
        if (proxyTlsEnabled)
        {
            certificatePemPath = Path.Combine(Root, "temporary-localhost.pem");
            privateKeyPemPath = Path.Combine(Root, "temporary-localhost-key.pem");
            File.WriteAllText(certificatePemPath, cert.ExportCertificatePem());
            File.WriteAllText(privateKeyPemPath, key.ExportPkcs8PrivateKeyPem());
        }
        Console.WriteLine($"TLS leaf SHA256={certificateHash}; hostname localhost checked by platform; exact leaf pin; CA chain validation NOT TESTED; no trust-store changes; reverse proxy={(proxyTlsEnabled ? "Caddy under test" : "NOT TESTED")}; Android NOT TESTED; root={Root}");
    }
    private static void EndTls()
    {
        foreach (var path in new[] { certificatePath, certificatePemPath, privateKeyPemPath })
            if (File.Exists(path)) File.Delete(path);
        certificatePassword = "";
        Console.WriteLine("Cleanup: temporary PFX/PEM/private key removed; owned backend/Caddy child processes disposed; test snapshots and logs retained (synthetic credentials only).");
    }
    private static async Task<string> TlsBaseline(int messages)
    {
        var path = Path.Combine(Root, "tls-baseline.json");
        await using var s = await Server.Start(path);
        foreach (var name in new[] { "alice", "bob", "outsider" })
        {
            var registration = await s.Json("POST", "/api/auth/register", new { username = name, displayName = name + " Test", password = "isolated-test-password" });
            var token = Str(registration, "token");
            var me = await s.Json("GET", "/api/telegram/me", null, token);
            Accounts.Add(name, new(Str(registration["user"]!, "id"), token, Long(me, "id"), Str(me, "phone")));
        }
        for (int i = 0; i < messages; i++) await s.Rpc(Send(long.MinValue + i, "benchmark fixed message 😀"));
        return path;
    }
    private static void CertificateValidationNegatives()
    {
        using var current = new X509Certificate2(certificatePath, certificatePassword);
        Check(ValidateCertificate(current, SslPolicyErrors.None), "pinned leaf accepted without chain trust");
        Check(!ValidateCertificate(current, SslPolicyErrors.RemoteCertificateNameMismatch), "hostname mismatch rejected");
        Check(!ValidateCertificate(current, SslPolicyErrors.RemoteCertificateNotAvailable), "missing certificate rejected");
        using var key = RSA.Create(2048);
        var request = new CertificateRequest("CN=localhost", key, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
        request.CertificateExtensions.Add(new SubjectAlternativeNameBuilder().Build());
        using var other = request.CreateSelfSigned(DateTimeOffset.UtcNow.AddMinutes(-5), DateTimeOffset.UtcNow.AddHours(2));
        Check(!ValidateCertificate(other, SslPolicyErrors.None), "different leaf rejected by exact pin");
        using var expired = request.CreateSelfSigned(DateTimeOffset.UtcNow.AddDays(-10), DateTimeOffset.UtcNow.AddDays(-1));
        var originalPin = certificateHash;
        try
        {
            // Pin the expired leaf so these assertions exercise time validation, not a pin mismatch.
            certificateHash = Convert.ToHexString(SHA256.HashData(expired.RawData));
            Check(!ValidateCertificate(expired, SslPolicyErrors.None), "expired pinned leaf rejected");
            Check(!ValidateCertificate(expired, SslPolicyErrors.RemoteCertificateChainErrors), "expired pinned leaf rejected even when only chain errors are tolerated");
        }
        finally { certificateHash = originalPin; }
        Console.WriteLine("Certificate validation negatives PASS: pin, hostname, missing, expired; platform CA chain still not configured for the test leaf");
    }
    private static async Task ResourceRelease(string sourcePath)
    {
        const string jsonProtocol = "botgram.events.v1.json";
        var path = Path.Combine(Root, "resource-release.json");
        File.Copy(sourcePath, path);
        await using var s = await Server.Start(path);
        var before = s.Counters();
        for (int i = 0; i < 10; i++)
        {
            using (var socket = new ClientWebSocket())
            {
                socket.Options.CollectHttpResponseDetails = true;
                if (tlsEnabled) socket.Options.RemoteCertificateValidationCallback = (_, cert, _, errors) => ValidateCertificate(cert, errors);
                socket.Options.SetRequestHeader("Authorization", "Bearer " + Accounts["bob"].Token);
                socket.Options.AddSubProtocol(jsonProtocol);
                using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(10));
                await socket.ConnectAsync(s.WebSocketUri(0), timeout.Token);
                Check(socket.State == WebSocketState.Open, "resource-release WSS upgrade");
                // Close without reading a frame: the server must release the connection promptly.
                await socket.CloseOutputAsync(WebSocketCloseStatus.NormalClosure, "resource-release", timeout.Token);
            }
            await s.Rpc(Req(new P.GetStateRequest()), "bob");
        }
        var after = s.Counters();
        var samples = new List<(int Threads, int Handles)>();
        for (int i = 0; i < 5; i++)
        {
            await Task.Delay(1000);
            using var health = await s.Client.GetAsync("/health");
            Check(health.IsSuccessStatusCode && !s.Exited, "post-churn health while observing resource release");
            samples.Add(s.Counters());
        }
        var settled = samples[^1];
        Check(settled.Threads <= before.Threads + 16, $"thread count bounded after churn ({before.Threads}->{settled.Threads})");
        Check(settled.Handles <= before.Handles + 32, $"handle count bounded after churn ({before.Handles}->{settled.Handles})");
        Console.WriteLine($"Resource release: wssConnections=10 rpcRequests=10 threads {before.Threads}->{string.Join('/', samples.Select(x => x.Threads))} handles {before.Handles}->{string.Join('/', samples.Select(x => x.Handles))}; sampled, not a long-run leak proof");
    }
    private static async Task<int> JointTls(bool proxy = false)
    {
        proxyTlsEnabled = proxy;
        if (proxy)
        {
            caddyPath = Environment.GetEnvironmentVariable("BOTGRAM_TEST_CADDY") ?? "";
            if (!Path.IsPathFullyQualified(caddyPath) || !File.Exists(caddyPath))
                throw new ArgumentException("BOTGRAM_TEST_CADDY must be an existing absolute Caddy executable path");
        }
        try
        {
            BeginTls();
            CertificateValidationNegatives();
            var baseline = await TlsBaseline(0);
            await using (var s = await Server.Start(baseline))
            {
                Check(await s.UntrustedTlsRejected(), "self-signed leaf is rejected by default platform trust");
                Check(await s.MismatchedTlsHostRejected(), "real TLS rejects invalid host via platform name check or proxy handshake refusal");
            }
            await WebSocketRegression(baseline);
            await ReadHistoryBoundaries(baseline);
            await ResourceRelease(baseline);
            Console.WriteLine($"Joint TLS PASS assertions={assertions}; root={Root}; reverse proxy={(proxyTlsEnabled ? "Caddy TLS to HTTP Kestrel" : "NOT TESTED")}; CA chain validation NOT TESTED (exact leaf pin only); Android NOT TESTED; historical 400 INCONCLUSIVE");
            return 0;
        }
        finally { EndTls(); }
    }
    private static async Task<int> BenchmarkTls()
    {
        BeginTls();
        try
        {
            var baseline = await TlsBaseline(100);
            var raw = new List<object>();
            var summaries = new List<object>();
            for (int round = 0; round < 3; round++)
            foreach (var concurrency in new[] { 1, 2 })
            foreach (var operation in new[] { "GetState", "GetHistory", "SendMessage" })
            foreach (var proto in round % 2 == 0 ? new[] { false, true } : new[] { true, false })
            {
                var protocol = proto ? "protobuf" : "json";
                var path = Path.Combine(Root, $"tls-bench-r{round}-c{concurrency}-{operation}-{protocol}.json");
                File.Copy(baseline, path);
                await using var s = await Server.Start(path);
                var latencies = new List<double>();
                var requestSizes = new List<int>(); var responseSizes = new List<int>();
                double cpu = 0; long maxWorkingSet = 0, maxPrivate = 0;
                ulong cpuCycles = 0;
                var memorySamples = new List<(double ElapsedMs, long WorkingSetBytes, long PrivateMemoryBytes)>();
                using var sampling = new CancellationTokenSource();
                var sampler = s.SampleMemory(sampling.Token, memorySamples);
                try
                {
                // Stay below the unchanged 300/min limiter; identical counts and baseline per protocol.
                foreach (var phase in new[] { (Warmup: true, Count: 20), (Warmup: false, Count: 200) })
                for (int offset = 0; offset < phase.Count; offset += concurrency)
                {
                    var before = s.Resources();
                    var tasks = Enumerable.Range(offset, Math.Min(concurrency, phase.Count - offset)).Select(async i =>
                    {
                        var key = long.MaxValue - (phase.Warmup ? i : i + 20);
                        var request = operation switch
                        {
                            "GetState" => Req(new P.GetStateRequest()),
                            "GetHistory" => Req(new P.GetHistoryRequest { Peer = Peer("bob"), Limit = 100 }),
                            _ => Send(key, "benchmark fixed message 😀")
                        };
                        var legacy = operation switch
                        {
                            "GetState" => T("TL_updates_getState"),
                            "GetHistory" => T("TL_messages_getHistory", ("peer", PeerJ(Accounts["bob"])), ("limit", 100)),
                            _ => T("TL_messages_sendMessage", ("peer", PeerJ(Accounts["bob"])), ("random_id", key), ("message", "benchmark fixed message 😀"))
                        };
                        var bytes = proto ? request.ToByteArray() : System.Text.Encoding.UTF8.GetBytes(legacy.ToJsonString());
                        var result = await s.BenchmarkRequest(proto, bytes, sampleResources: false);
                        if (result.Status != 200 || result.ContentType != (proto ? Media : "application/json")) throw new Exception("TLS benchmark status/media failure");
                        if (proto)
                        {
                            var response = P.RpcResponse.Parser.ParseFrom(result.Body);
                            if (response.RequestId != request.RequestId || (int)response.OutcomeCase != (int)request.OperationCase) throw new Exception("TLS benchmark protobuf correlation");
                            if (operation == "GetHistory" && response.GetHistory.Messages.Count != 100) throw new Exception("TLS benchmark history count");
                            if (operation == "SendMessage" && response.SendMessage.Mapping.RandomId != key) throw new Exception("TLS benchmark send key");
                        }
                        else
                        {
                            var response = JsonNode.Parse(result.Body)!;
                            var expected = operation switch { "GetState" => "TL_updates_state", "GetHistory" => "TL_messages_messages", _ => "TL_updates" };
                            if (Str(response, "$type") != "org.telegram.tgnet.TLRPC$" + expected) throw new Exception("TLS benchmark JSON type");
                            if (operation == "GetHistory" && Arr(response, "messages").Count != 100) throw new Exception("TLS benchmark JSON history count");
                            if (operation == "SendMessage" && !Arr(response, "updates").Any(u => u?["random_id"]?.GetValue<long>() == key)) throw new Exception("TLS benchmark JSON send key");
                        }
                        return (i, result.Milliseconds, RequestBytes: bytes.Length, ResponseBytes: result.Body.Length, result.Status);
                    }).ToArray();
                    var results = await Task.WhenAll(tasks);
                    var after = s.Resources();
                    var batchCpu = after.CpuMilliseconds - before.CpuMilliseconds;
                    var batchCycles = after.CpuCycles - before.CpuCycles;
                    raw.Add(new { round, concurrency, operation, protocol, phase.Warmup, batchOffset = offset, batchCpuMilliseconds = batchCpu, batchCpuCycles = batchCycles, after.WorkingSetBytes, after.PrivateMemoryBytes, requests = results.Select(r => new { iteration = r.i, httpMilliseconds = r.Milliseconds, requestBodyBytes = r.RequestBytes, responseBodyBytes = r.ResponseBytes, status = r.Status }) });
                    if (!phase.Warmup)
                    {
                        cpu += batchCpu; maxWorkingSet = Math.Max(maxWorkingSet, after.WorkingSetBytes); maxPrivate = Math.Max(maxPrivate, after.PrivateMemoryBytes);
                        cpuCycles += batchCycles;
                        latencies.AddRange(results.Select(r => r.Milliseconds)); requestSizes.AddRange(results.Select(r => r.RequestBytes)); responseSizes.AddRange(results.Select(r => r.ResponseBytes));
                    }
                }
                }
                finally { sampling.Cancel(); await sampler; }
                maxWorkingSet = Math.Max(maxWorkingSet, memorySamples.Max(m => m.WorkingSetBytes));
                maxPrivate = Math.Max(maxPrivate, memorySamples.Max(m => m.PrivateMemoryBytes));
                var maxSampleGapMs = memorySamples.Zip(memorySamples.Skip(1), (a, b) => b.ElapsedMs - a.ElapsedMs).DefaultIfEmpty(0).Max();
                raw.Add(new { round, concurrency, operation, protocol, memorySamples = memorySamples.Select(m => new { m.ElapsedMs, m.WorkingSetBytes, m.PrivateMemoryBytes }) });
                Check(Arr(Load(path), "messages").Count == (operation == "SendMessage" ? 320 : 100), "TLS benchmark persisted message count");
                latencies.Sort();
                summaries.Add(new { round, concurrency, operation, protocol, warmup = 20, measured = 200, memorySampleCount = memorySamples.Count, maxSampleGapMs, lifetimePeakPagedMemoryBytes = s.LifetimePeakPagedMemoryBytes, cpuMsPerRequest = cpu / 200, cpuCycles, cpuCyclesPerRequest = (double)cpuCycles / 200, meanMs = latencies.Average(), medianMs = (latencies[99] + latencies[100]) / 2, p95Ms = latencies[189], p99Ms = latencies[197], minMs = latencies[0], maxMs = latencies[^1], meanRequestBodyBytes = requestSizes.Average(), meanResponseBodyBytes = responseSizes.Average(), cpuMilliseconds = cpu, maxWorkingSetBytes = maxWorkingSet, maxPrivateMemoryBytes = maxPrivate });
                Console.WriteLine($"TLS benchmark round={round} concurrency={concurrency} operation={operation} protocol={protocol} measured=200 warmup=20 meanMs={latencies.Average():F3} p95Ms={latencies[189]:F3} cpuMs={cpu:F3}");
            }
            var options = new JsonSerializerOptions { WriteIndented = true };
            File.WriteAllText(Path.Combine(Root, "benchmark-tls-samples.json"), JsonSerializer.Serialize(raw, options));
            File.WriteAllText(Path.Combine(Root, "benchmark-tls.json"), JsonSerializer.Serialize(new
            {
                recordedAtUtc = DateTimeOffset.UtcNow, runtime = System.Runtime.InteropServices.RuntimeInformation.FrameworkDescription,
                processorCount = Environment.ProcessorCount, backendSha256 = Convert.ToHexString(SHA256.HashData(File.ReadAllBytes(BackendDll))),
                rounds = 3, concurrencyLevels = new[] { 1, 2 }, measuredRequests = 7200, warmupRequests = 720,
                isolation = "Fresh independent stopped baseline copy, random loopback HTTPS port each cell; 220 requests per measured instance/user (<300/min); baseline 100 sends; second round reverses protocol order; no production endpoint or limiter modifications.",
                timing = "Stopwatch SendAsync(ResponseHeadersRead) through full body read, excludes encoding/validation; body bytes only, not TLS wire bytes; reused connections, first request may include handshake.",
                cpu = "Process TotalProcessorTime and Windows QueryProcessCycleTime once per nonoverlapping batch; warmups excluded. Cycle counts supplement coarse CPU time and are not converted to milliseconds or substituted for the original CPU budget.",
                memory = "Dedicated observer samples WorkingSet64/PrivateMemorySize64 throughout warmup and measurement, target interval 2ms; timestamps/gaps retained. Sampled high water is a lower bound. OS lifetime PeakPagedMemorySize64 includes startup and is supplemental; not an exact private-byte peak in the measurement window.",
                caveats = "Local Kestrel direct TLS, no thresholds or performance pass claim; reverse proxy NOT TESTED; Android NOT TESTED; JIT/cache/order/thermal bias remains.",
                results = summaries
            }, options));
            Console.WriteLine($"TLS benchmark complete; no performance thresholds; root={Root}");
            return 0;
        }
        finally { EndTls(); }
    }
}

