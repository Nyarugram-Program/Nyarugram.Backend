using System.Net;
using System.Text.RegularExpressions;
using Npgsql;

namespace Botgram.Backend;

public enum StorageMode { LegacyOnly, NewStorageShadow, NewStorageValidation, NewStoragePrimary }

public sealed record StoragePolicy(StorageMode Mode)
{
    public static StoragePolicy Load(IConfiguration configuration, IHostEnvironment environment)
    {
        var configured = configuration["Storage:Mode"] ?? nameof(StorageMode.LegacyOnly);
        if (!Enum.GetNames<StorageMode>().Contains(configured, StringComparer.Ordinal))
            throw new InvalidOperationException("Storage:Mode must name an explicit supported storage mode");
        var mode = Enum.Parse<StorageMode>(configured);
        var backend = configuration["Storage:ValidationBackend"] ?? "SnapshotBridge";
        if (backend is not ("SnapshotBridge" or "RelationalThreeRpc" or "RelationalBackend" or "FourComponent"))
            throw new InvalidOperationException("Storage:ValidationBackend must be SnapshotBridge, RelationalThreeRpc, RelationalBackend or FourComponent");
        if (backend is "RelationalThreeRpc" or "RelationalBackend" or "FourComponent" && mode != StorageMode.NewStorageValidation)
            throw new InvalidOperationException($"{backend} is restricted to NewStorageValidation");
        if (backend is "RelationalBackend" or "FourComponent" &&
            (configuration.GetValue("Protocol:LegacyJsonRpcEnabled", true) ||
             configuration.GetValue("Protocol:LegacyJsonWssEnabled", true) ||
             configuration.GetValue("Protocol:ProtobufRolloutPercent", 100) != 100))
            throw new InvalidOperationException("RelationalBackend validation currently requires explicitly configured protobuf-only transport");
        if (backend == "FourComponent")
        {
            if (!configuration.GetValue<bool>("Storage:FullStackAcknowledged"))
                throw new InvalidOperationException("FourComponent requires Storage:FullStackAcknowledged=true");
            _ = Storage.FourComponentOptions.Load(configuration);
        }
        if (mode == StorageMode.NewStoragePrimary)
            throw new InvalidOperationException("Production storage cutover is not authorized or implemented");
        if (mode == StorageMode.NewStorageShadow)
            throw new InvalidOperationException("The selected storage mode has no verified implementation; refusing legacy fallback");
        if (mode == StorageMode.NewStorageValidation)
        {
            if (!environment.IsDevelopment() ||
                !string.Equals(configuration["Storage:ValidationAcknowledged"], "true", StringComparison.OrdinalIgnoreCase))
                throw new InvalidOperationException("Storage:Validation requires Development and Storage:ValidationAcknowledged=true");
            NpgsqlConnectionStringBuilder connection;
            try
            {
                var value = configuration["Storage:ValidationConnectionString"];
                if (string.IsNullOrWhiteSpace(value)) throw new ArgumentException();
                connection = new(value);
            }
            catch (ArgumentException)
            {
                throw new InvalidOperationException("Storage:ValidationConnectionString must be an explicit isolated PostgreSQL connection");
            }
            var host = connection.Host;
            if (!(string.Equals(host, "localhost", StringComparison.OrdinalIgnoreCase) ||
                    (IPAddress.TryParse(host, out var address) && IPAddress.IsLoopback(address))) ||
                connection.Database is not { } database || !Regex.IsMatch(database, "\\Abotgram_test_[a-z0-9_]+\\z"))
                throw new InvalidOperationException("Storage:ValidationConnectionString requires a loopback host and botgram_test_* database");
        }
        return new(mode);
    }
}
