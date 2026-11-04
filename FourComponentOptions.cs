using System.Net;
using System.Text.RegularExpressions;
using StackExchange.Redis;

namespace Botgram.Backend.Storage;

public sealed record FourComponentOptions(string Kafka, string Topic, string Consumer,
    string Scylla, int ScyllaPort, string Keyspace, string Datacenter, string Redis, string CacheNamespace)
{
    internal static bool Loopback(string? host) => host == "localhost" ||
        IPAddress.TryParse(host, out var address) && IPAddress.IsLoopback(address);

    public static FourComponentOptions Load(IConfiguration configuration)
    {
        string Required(string key) => configuration["Storage:FullStack:" + key] is { Length: > 0 } value
            ? value : throw new InvalidOperationException("Missing Storage:FullStack:" + key);
        var options = new FourComponentOptions(Required("Kafka"), Required("Topic"), Required("Consumer"),
            Required("Scylla"), configuration.GetValue("Storage:FullStack:ScyllaPort", 9042),
            Required("Keyspace"), Required("Datacenter"), Required("Redis"), Required("CacheNamespace"));
        if (options.Kafka.Split(',').Any(endpoint =>
                !Uri.TryCreate("tcp://" + endpoint, UriKind.Absolute, out var uri) || !Loopback(uri.Host) ||
                uri.Port <= 0 || uri.AbsolutePath != "/" || uri.UserInfo.Length != 0) ||
            !Loopback(options.Scylla) || options.ScyllaPort is < 1 or > 65535 ||
            !Regex.IsMatch(options.Topic, "\\Abotgram_test_[a-z0-9_]+\\z") ||
            !Regex.IsMatch(options.Consumer, "\\Abotgram_test_[a-z0-9_]+\\z") ||
            !Regex.IsMatch(options.Keyspace, "\\Abotgram_test_[a-z0-9_]+\\z") ||
            !Regex.IsMatch(options.CacheNamespace, "\\Abotgram_test_[a-z0-9_]+\\z") ||
            options.Keyspace.Length > 48 || options.CacheNamespace.Length > 64 ||
            options.Topic.Length > 64 || options.Consumer.Length > 64)
            throw new InvalidOperationException("FourComponent requires explicit loopback test services and botgram_test_* namespaces");
        var redis = ConfigurationOptions.Parse(options.Redis);
        if (redis.EndPoints.Count != 1 || redis.ServiceName is not null || redis.EndPoints.Any(endpoint => endpoint switch
            {
                IPEndPoint ip => !IPAddress.IsLoopback(ip.Address),
                DnsEndPoint dns => !Loopback(dns.Host),
                _ => true
            }))
            throw new InvalidOperationException("FourComponent requires one explicit loopback Redis endpoint");
        return options;
    }
}
