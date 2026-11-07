using System.Net;
using System.Text.RegularExpressions;
using Npgsql;

namespace Botgram.Backend;

public enum StorageMode { LegacyOnly, NewStorageShadow, NewStorageValidation, NewStoragePrimary }

/// <summary>
/// Loaded before any Store/lease file is created. NewStorageValidation keeps its
/// original isolated-loopback test restrictions. NewStoragePrimary is the real
/// production cutover: an explicitly acknowledged, dedicated connection string,
/// protobuf-only transport and a durable key ring. It never pretends to be a
/// Development environment and never renames a test database as production.
/// </summary>
public sealed record StoragePolicy(StorageMode Mode, bool IsPrimary = false)
{
    public const string PrimaryConnectionKey = "Storage:PrimaryConnectionString";

    public static string ResolveConnectionString(IConfiguration configuration, StoragePolicy policy) =>
        policy.IsPrimary
            ? configuration[PrimaryConnectionKey] ?? throw new InvalidOperationException("Missing " + PrimaryConnectionKey)
            : PostgresValidationStore.Validate(configuration);

    public static StoragePolicy Load(IConfiguration configuration, IHostEnvironment environment)
    {
        var configured = configuration["Storage:Mode"] ?? nameof(StorageMode.LegacyOnly);
        if (!Enum.GetNames<StorageMode>().Contains(configured, StringComparer.Ordinal))
            throw new InvalidOperationException("Storage:Mode must name an explicit supported storage mode");
        var mode = Enum.Parse<StorageMode>(configured);
        var backend = configuration["Storage:ValidationBackend"] ?? "SnapshotBridge";
        if (backend is not ("SnapshotBridge" or "RelationalThreeRpc" or "RelationalBackend" or "FourComponent"))
            throw new InvalidOperationException("Storage:ValidationBackend must be SnapshotBridge, RelationalThreeRpc, RelationalBackend or FourComponent");
        var primary = mode == StorageMode.NewStoragePrimary;
        if (backend is "RelationalThreeRpc" or "RelationalBackend" or "FourComponent" &&
            mode is not (StorageMode.NewStorageValidation or StorageMode.NewStoragePrimary))
            throw new InvalidOperationException($"{backend} is restricted to NewStorageValidation or NewStoragePrimary");
        if (primary && backend is not ("RelationalBackend" or "FourComponent"))
            throw new InvalidOperationException("NewStoragePrimary requires ValidationBackend=RelationalBackend or FourComponent");
        if (backend is "RelationalBackend" or "FourComponent" &&
            (configuration.GetValue("Protocol:LegacyJsonRpcEnabled", true) ||
             configuration.GetValue("Protocol:LegacyJsonWssEnabled", true) ||
             configuration.GetValue("Protocol:ProtobufRolloutPercent", 100) != 100))
            throw new InvalidOperationException("Relational backends require explicitly configured protobuf-only transport");
        if (backend == "FourComponent")
        {
            if (!configuration.GetValue<bool>("Storage:FullStackAcknowledged"))
                throw new InvalidOperationException("FourComponent requires Storage:FullStackAcknowledged=true");
            _ = Storage.FourComponentOptions.Load(configuration, primary);
        }
        if (primary)
        {
            if (!configuration.GetValue<bool>("Storage:PrimaryAcknowledged"))
                throw new InvalidOperationException("Storage:Mode=NewStoragePrimary requires Storage:PrimaryAcknowledged=true");
            if (string.IsNullOrWhiteSpace(configuration["Privacy:KeyDirectory"]))
                throw new InvalidOperationException("NewStoragePrimary requires an explicit Privacy:KeyDirectory");
            var value = configuration[PrimaryConnectionKey];
            if (string.IsNullOrWhiteSpace(value))
                throw new InvalidOperationException("NewStoragePrimary requires " + PrimaryConnectionKey);
            NpgsqlConnectionStringBuilder connection;
            try { connection = new(value); }
            catch (ArgumentException)
            { throw new InvalidOperationException(PrimaryConnectionKey + " must be an explicit Npgsql connection string"); }
            if (string.IsNullOrWhiteSpace(connection.Host) || string.IsNullOrWhiteSpace(connection.Database) ||
                connection.Database.StartsWith("botgram_test_", StringComparison.OrdinalIgnoreCase))
                throw new InvalidOperationException(
                    PrimaryConnectionKey + " must name a dedicated non-test database; test namespaces are not production targets");
            return new(mode, true);
        }
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
