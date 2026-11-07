using System.Net;
using System.Text.RegularExpressions;
using StackExchange.Redis;

namespace Botgram.Backend.Storage;

public sealed record FourComponentOptions(string Kafka, string Topic, string Consumer,
    string Scylla, int ScyllaPort, string Keyspace, string Datacenter, string Redis, string CacheNamespace)
{
    internal static bool Loopback(string? host) => host == "localhost" ||
        IPAddress.TryParse(host, out var address) && IPAddress.IsLoopback(address);

    /// <summary>
    /// Validation mode keeps its strict loopback + botgram_test_* isolation.
    /// Production mode accepts explicitly configured private endpoints but
    /// rejects test namespaces, so a validation stack can never be mistaken
    /// for the production stack.
    /// </summary>
    public static FourComponentOptions Load(IConfiguration configuration, bool production = false)
    {
        string Required(string key) => configuration["Storage:FullStack:" + key] is { Length: > 0 } value
            ? value : throw new InvalidOperationException("Missing Storage:FullStack:" + key);
        var options = new FourComponentOptions(Required("Kafka"), Required("Topic"), Required("Consumer"),
            Required("Scylla"), configuration.GetValue("Storage:FullStack:ScyllaPort", 9042),
            Required("Keyspace"), Required("Datacenter"), Required("Redis"), Required("CacheNamespace"));
        bool Namespace(string value, int max) => Regex.IsMatch(value, "\\A[a-z][a-z0-9_]{1," + (max - 1) + "}\\z") &&
            (production ? !value.StartsWith("botgram_test_", StringComparison.Ordinal)
                        : value.StartsWith("botgram_test_", StringComparison.Ordinal));
        if (options.Kafka.Split(',').Any(endpoint =>
                !Uri.TryCreate("tcp://" + endpoint, UriKind.Absolute, out var uri) || uri.Port <= 0 ||
                uri.AbsolutePath != "/" || uri.UserInfo.Length != 0 || !production && !Loopback(uri.Host)) ||
            (!production && !Loopback(options.Scylla)) || options.ScyllaPort is < 1 or > 65535 ||
            !Namespace(options.Topic, 64) || !Namespace(options.Consumer, 64) ||
            !Namespace(options.Keyspace, 48) || !Namespace(options.CacheNamespace, 64))
            throw new InvalidOperationException(production
                ? "FourComponent production requires explicit endpoints and non-test namespaces"
                : "FourComponent requires explicit loopback test services and botgram_test_* namespaces");
        var redis = ConfigurationOptions.Parse(options.Redis);
        if (redis.EndPoints.Count == 0 || (!production &&
            (redis.ServiceName is not null || redis.EndPoints.Count != 1 || redis.EndPoints.Any(endpoint => endpoint switch
            {
                IPEndPoint ip => !IPAddress.IsLoopback(ip.Address),
                DnsEndPoint dns => !Loopback(dns.Host),
                _ => true
            }))))
            throw new InvalidOperationException(production
                ? "FourComponent production requires at least one explicit Redis endpoint"
                : "FourComponent requires one explicit loopback Redis endpoint");
        return options;
    }
}
