using System.Net;
using System.Net.Security;
using System.Net.Sockets;
using System.Security.Authentication;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using System.Text.Json;

internal static partial class HttpTests
{
    // This is an isolated test-client policy, never installed as platform or Android trust.
    private static async Task<int> CertificateSecurity()
    {
        var now = DateTimeOffset.UtcNow;
        var keys = new List<RSA>();
        var certificates = new List<X509Certificate2>();
        var results = new List<object>();
        var temporaryFiles = new List<string>();
        var openssl = Environment.GetEnvironmentVariable("BOTGRAM_TEST_OPENSSL") ?? "C:/Program Files/Git/usr/bin/openssl.exe";
        if (!File.Exists(openssl)) throw new FileNotFoundException("Set BOTGRAM_TEST_OPENSSL to an existing OpenSSL executable");
        X509Certificate2 Issue(string name, X509Certificate2? issuer, bool ca,
            DateTimeOffset from, DateTimeOffset to)
        {
            var key = RSA.Create(2048); keys.Add(key);
            var request = new CertificateRequest("CN=" + name, key, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
            request.CertificateExtensions.Add(new X509BasicConstraintsExtension(ca, ca, ca ? 1 : 0, true));
            request.CertificateExtensions.Add(new X509KeyUsageExtension(ca
                ? X509KeyUsageFlags.KeyCertSign | X509KeyUsageFlags.CrlSign
                : X509KeyUsageFlags.DigitalSignature | X509KeyUsageFlags.KeyEncipherment, true));
            request.CertificateExtensions.Add(new X509SubjectKeyIdentifierExtension(request.PublicKey, false));
            if (!ca)
            {
                var san = new SubjectAlternativeNameBuilder(); san.AddDnsName("localhost");
                request.CertificateExtensions.Add(san.Build());
                request.CertificateExtensions.Add(new X509EnhancedKeyUsageExtension(new OidCollection { new("1.3.6.1.5.5.7.3.1") }, true));
            }
            X509Certificate2 cert;
            if (issuer is null) cert = request.CreateSelfSigned(from, to);
            else
            {
                using var publicCertificate = request.Create(issuer, from, to, RandomNumberGenerator.GetBytes(16));
                cert = publicCertificate.CopyWithPrivateKey(key);
            }
            certificates.Add(cert);
            return cert;
        }
        try
        {
            var root = Issue("Botgram ephemeral test root", null, true, now.AddDays(-30), now.AddDays(30));
            var intermediate = Issue("Botgram ephemeral intermediate", root, true, now.AddDays(-20), now.AddDays(20));
            var valid = Issue("localhost", intermediate, false, now.AddDays(-1), now.AddDays(1));
            var expired = Issue("localhost expired", intermediate, false, now.AddDays(-10), now.AddDays(-1));
            var future = Issue("localhost future", intermediate, false, now.AddDays(1), now.AddDays(2));
            var unknownRoot = Issue("Botgram untrusted test root", null, true, now.AddDays(-30), now.AddDays(30));
            var unknownIntermediate = Issue("Botgram untrusted intermediate", unknownRoot, true, now.AddDays(-20), now.AddDays(20));
            var unknownLeaf = Issue("localhost untrusted", unknownIntermediate, false, now.AddDays(-1), now.AddDays(1));
            // A fresh issuer without AIA prevents earlier successful handshakes populating an intermediate cache.
            var missingIntermediate = Issue("Botgram missing intermediate", root, true, now.AddDays(-20), now.AddDays(20));
            var incomplete = Issue("localhost missing chain", missingIntermediate, false, now.AddDays(-1), now.AddDays(1));
            await Handshake("valid-chain-correct-host", valid, intermediate, "localhost", true);
            await Handshake("wrong-hostname", valid, intermediate, "wrong.invalid", false);
            await Handshake("expired-leaf", expired, intermediate, "localhost", false);
            await Handshake("not-yet-valid-leaf", future, intermediate, "localhost", false);
            await Handshake("untrusted-root", unknownLeaf, unknownIntermediate, "localhost", false);
            await Handshake("missing-intermediate", incomplete, null, "localhost", false);
            await Handshake("wrong-intermediate", incomplete, unknownIntermediate, "localhost", false);

            async Task Handshake(string name, X509Certificate2 leaf, X509Certificate2? sentIntermediate, string host, bool expected)
            {
                using var deadline = new CancellationTokenSource(TimeSpan.FromSeconds(10));
                using var listener = new TcpListener(IPAddress.Loopback, 0);
                listener.Start(1);
                int port = ((IPEndPoint)listener.LocalEndpoint).Port;
                listener.Stop();
                bool callbackSeen = false, nameValid = false, chainValid = false, accepted = false;
                string[] chainStatuses = [];
                var leafPath = Path.Combine(Root, name + "-leaf.pem");
                var keyPath = Path.Combine(Root, name + "-key.pem");
                var chainPath = Path.Combine(Root, name + "-chain.pem");
                temporaryFiles.AddRange(new[] { leafPath, keyPath, chainPath });
                File.WriteAllText(leafPath, leaf.ExportCertificatePem());
                using (var privateKey = leaf.GetRSAPrivateKey()!) File.WriteAllText(keyPath, privateKey.ExportPkcs8PrivateKeyPem());
                var start = new System.Diagnostics.ProcessStartInfo(openssl)
                { UseShellExecute = false, CreateNoWindow = true, RedirectStandardOutput = true, RedirectStandardError = true, WorkingDirectory = Root };
                foreach (var argument in new[] { "s_server", "-accept", "127.0.0.1:" + port, "-cert", leafPath, "-key", keyPath, "-tls1_2", "-quiet", "-www" }) start.ArgumentList.Add(argument);
                if (sentIntermediate is not null)
                {
                    File.WriteAllText(chainPath, sentIntermediate.ExportCertificatePem() +
                        (name == "untrusted-root" ? Environment.NewLine + unknownRoot.ExportCertificatePem() : ""));
                    start.ArgumentList.Add("-cert_chain"); start.ArgumentList.Add(chainPath);
                }
                using var server = System.Diagnostics.Process.Start(start) ?? throw new Exception("OpenSSL did not start");
                var stdout = server.StandardOutput.ReadToEndAsync();
                var stderr = server.StandardError.ReadToEndAsync();
                try
                {
                    using var client = new TcpClient();
                    for (int attempt = 0; ; attempt++)
                    {
                        if (server.HasExited) throw new Exception("Isolated OpenSSL exited before handshake: " + server.ExitCode);
                        try { await client.ConnectAsync(IPAddress.Loopback, port, deadline.Token); break; }
                        catch (SocketException) when (attempt < 50) { await Task.Delay(50, deadline.Token); }
                    }
                    using var stream = new SslStream(client.GetStream(), false, (_, remote, remoteChain, errors) =>
                    {
                        callbackSeen = true;
                        nameValid = remote is not null && (errors & (SslPolicyErrors.RemoteCertificateNameMismatch | SslPolicyErrors.RemoteCertificateNotAvailable)) == 0;
                        if (remote is null) return false;
                        using var custom = new X509Chain();
                        custom.ChainPolicy.TrustMode = X509ChainTrustMode.CustomRootTrust;
                        custom.ChainPolicy.CustomTrustStore.Add(root);
                        custom.ChainPolicy.RevocationMode = X509RevocationMode.NoCheck;
                        custom.ChainPolicy.DisableCertificateDownloads = true;
                        custom.ChainPolicy.VerificationFlags = X509VerificationFlags.NoFlag;
                        custom.ChainPolicy.VerificationTime = now.UtcDateTime;
                        custom.ChainPolicy.ApplicationPolicy.Add(new Oid("1.3.6.1.5.5.7.3.1"));
                        if (remoteChain is not null)
                            foreach (var element in remoteChain.ChainElements.Cast<X509ChainElement>().Skip(1))
                                custom.ChainPolicy.ExtraStore.Add(element.Certificate);
                        using var remoteLeaf = new X509Certificate2(remote);
                        chainValid = custom.Build(remoteLeaf);
                        chainStatuses = custom.ChainStatus.Select(s => s.Status.ToString()).ToArray();
                        return nameValid && chainValid;
                    });
                    await stream.AuthenticateAsClientAsync(new SslClientAuthenticationOptions
                    {
                        TargetHost = host, EnabledSslProtocols = SslProtocols.Tls12,
                        CertificateRevocationCheckMode = X509RevocationMode.NoCheck
                    }, deadline.Token);
                    await stream.WriteAsync(System.Text.Encoding.ASCII.GetBytes("GET / HTTP/1.0\r\n\r\n"), deadline.Token);
                    var payload = new byte[1];
                    accepted = await stream.ReadAsync(payload, deadline.Token) == 1;
                }
                catch (Exception ex) when (ex is AuthenticationException or IOException) { }
                finally
                {
                    if (!server.HasExited) { server.Kill(entireProcessTree: true); await server.WaitForExitAsync(); }
                    await stdout; await stderr;
                    foreach (var path in new[] { leafPath, keyPath, chainPath }) if (File.Exists(path)) File.Delete(path);
                }
                Check(callbackSeen, "TLS certificate callback reached: " + name);
                Check(accepted == expected, "isolated CA TLS expected acceptance: " + name);
                if (name == "wrong-hostname") Check(!nameValid && chainValid, "wrong hostname rejected independently of valid chain");
                if (name is "expired-leaf" or "not-yet-valid-leaf") Check(chainStatuses.Contains("NotTimeValid"), "time failure observed in chain: " + name);
                if (name == "untrusted-root") Check(chainStatuses.Contains("UntrustedRoot"), "complete chain terminates at untrusted root");
                if (name is "missing-intermediate" or "wrong-intermediate") Check(chainStatuses.Contains("PartialChain"), "incomplete chain observed: " + name);
                results.Add(new { name, expected, accepted, callbackSeen, nameValid, chainValid, chainStatuses });
                Console.WriteLine($"Isolated CA TLS {name}: accepted={accepted} nameValid={nameValid} chainValid={chainValid} status={string.Join(',', chainStatuses)}");
            }
            var report = Path.Combine(Root, "certificate-security.json");
            File.WriteAllText(report, JsonSerializer.Serialize(new
            {
                recordedAtUtc = DateTimeOffset.UtcNow,
                scope = "Real TCP/TLS12 OpenSSL server to .NET SslStream client with per-client CustomRootTrust; no global trust changes, no AIA downloads; revocation not tested; not public CA or Android validation",
                opensslSha256 = Convert.ToHexString(SHA256.HashData(File.ReadAllBytes(openssl))),
                privateKeysConfinedToTestDirectoryAndRemoved = temporaryFiles.All(p => !File.Exists(p)), temporaryListenersStopped = true, results
            }, new JsonSerializerOptions { WriteIndented = true }));
            Console.WriteLine($"Certificate security PASS: seven real handshakes; report={report}; public CA/Android/revocation NOT TESTED; D-drive PEM keys removed");
            return 0;
        }
        finally
        {
            foreach (var path in temporaryFiles) if (File.Exists(path)) File.Delete(path);
            foreach (var certificate in certificates) certificate.Dispose();
            foreach (var key in keys) key.Dispose();
        }
    }
}
