using Botgram.Backend;
using Botgram.Backend.Storage;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using Npgsql;
using System.Text.Json;

var commands = new[] { "prepare", "init", "import", "export", "inspect", "upgrade" };
if (args.Length is < 2 or > 3 || !commands.Contains(args[0], StringComparer.Ordinal))
    throw new ArgumentException("Expected prepare|init|inspect|upgrade <private-config.json>, or import|export <private-config.json> <snapshot.json>");
if (args[0] is "prepare" or "init" or "inspect" or "upgrade")
{
    if (args.Length != 2)
        throw new ArgumentException("Expected prepare|init|inspect|upgrade <private-config.json>, or import|export <private-config.json> <snapshot.json>");
}
else if (args.Length != 3)
    throw new ArgumentException("Expected prepare|inspect|upgrade <private-config.json>, or import|export <private-config.json> <snapshot.json>");
var builder = Host.CreateApplicationBuilder(new HostApplicationBuilderSettings { EnvironmentName = Environments.Development });
builder.Configuration.AddJsonFile(Path.GetFullPath(args[1]), optional: false);
var primary = string.Equals(builder.Configuration["Storage:Mode"], nameof(StorageMode.NewStoragePrimary), StringComparison.Ordinal);
if (primary)
{
    if (!string.Equals(builder.Configuration["Storage:PrimaryAcknowledged"], "true", StringComparison.OrdinalIgnoreCase))
        throw new InvalidOperationException("Production offline maintenance requires Storage:PrimaryAcknowledged=true");
    // A production database is never disguised as a Development validation target.
    builder.Environment.EnvironmentName = Environments.Production;
    Environment.SetEnvironmentVariable("BOTGRAM_PRIMARY_MAINTENANCE", "1");
}
var connectionString = (primary
    ? builder.Configuration[StoragePolicy.PrimaryConnectionKey]
    : builder.Configuration["Storage:ValidationConnectionString"])
    ?? throw new InvalidOperationException("An explicit PostgreSQL connection string is required");
var backendName = builder.Configuration["Storage:ValidationBackend"];
if (args[0] is "inspect" or "upgrade")
{
    // Offline schema maintenance only audits the declared isolated target. It deliberately does not
    // load the runtime transport switches or start the service.
    if (!primary && !string.Equals(builder.Configuration["Storage:Mode"], nameof(StorageMode.NewStorageValidation), StringComparison.Ordinal))
        throw new InvalidOperationException("Schema maintenance requires Storage:Mode=NewStorageValidation or NewStoragePrimary");
    if (!primary && !string.Equals(builder.Configuration["Storage:ValidationAcknowledged"], "true", StringComparison.OrdinalIgnoreCase))
        throw new InvalidOperationException("Schema maintenance requires Storage:ValidationAcknowledged=true");
    if (backendName is not ("RelationalBackend" or "FourComponent"))
        throw new InvalidOperationException("Schema maintenance requires Storage:ValidationBackend=RelationalBackend or FourComponent");
    var settings = new NpgsqlConnectionStringBuilder(connectionString);
    settings.Pooling = false;
    settings.ApplicationName = PostgresSchemaUpgrade.ApplicationName;
    await using var source = NpgsqlDataSource.Create(settings.ConnectionString);
    var report = args[0] == "inspect"
        ? await PostgresSchemaUpgrade.InspectAsync(source)
        : await PostgresSchemaUpgrade.ApplyAsync(source);
    Console.WriteLine($"Target: host={report.Host}; database={report.Database}; user={report.User}; server={report.ServerVersion}");
    Console.WriteLine($"Other client backends: {report.OtherClientBackends}");
    Console.WriteLine($"Schema versions: {string.Join(',', report.VersionsBefore)} -> {string.Join(',', report.VersionsAfter)}");
    if (args[0] == "inspect")
        Console.WriteLine(report.MigrationRequired
            ? $"PASS: precheck only, migration to v{PostgresSchemaUpgrade.TargetVersion} is required and supported"
            : "PASS: precheck only, the target already has the current schema");
    else
        Console.WriteLine(report.Applied
            ? $"PASS: schema upgraded in place and the backend startup check accepted v{PostgresSchemaUpgrade.TargetVersion}"
            : "PASS: no migration required; the backend startup check accepted the current schema");
    return;
}
if (args[0] == "init")
{
    // New-database initialization without Kafka/Scylla/Redis. Re-running is a
    // no-op: every version block is guarded by schema_versions.
    if (backendName is not ("RelationalBackend" or "FourComponent"))
        throw new InvalidOperationException("init requires ValidationBackend=RelationalBackend or FourComponent");
    await using (var source = NpgsqlDataSource.Create(connectionString))
        await PostgresBackendSchema.ApplyAsync(source);
    Console.WriteLine($"PASS: PostgreSQL schema v{PostgresBackendSchema.CurrentVersion} applied (new database or idempotent re-run)");
    return;
}
if (!primary && (!string.Equals(builder.Configuration["Storage:Mode"], nameof(StorageMode.NewStorageValidation), StringComparison.Ordinal) ||
    !string.Equals(builder.Configuration["Storage:ValidationAcknowledged"], "true", StringComparison.OrdinalIgnoreCase)))
    throw new InvalidOperationException("Isolated prepare/import/export require NewStorageValidation and ValidationAcknowledged=true");
// Snapshot import/export are pure PostgreSQL operations. A production cutover
// that intentionally uses PostgreSQL as the sole authority (RelationalBackend)
// must be able to migrate without provisioning Kafka/Scylla/Redis. Only the
// four-component prepare step keeps the full-stack requirement.
if (backendName is not ("RelationalBackend" or "FourComponent"))
    throw new InvalidOperationException("Storage.Admin prepare/import/export require ValidationBackend=RelationalBackend or FourComponent");
if (args[0] == "prepare" && backendName != "FourComponent")
    throw new InvalidOperationException("Storage.Admin prepare requires ValidationBackend=FourComponent; use init for a PostgreSQL-only target");
await using var stack = NpgsqlDataSource.Create(connectionString);
switch (args[0])
{
    case "prepare":
        await FourComponentSetup.PrepareAsync(stack, FourComponentOptions.Load(builder.Configuration, primary));
        Console.WriteLine(primary
            ? "PASS: production PostgreSQL schema, worker contract, Kafka topic/offset and Scylla projection prepared"
            : "PASS: isolated PostgreSQL schema, worker contract, Kafka topic/offset and Scylla projection prepared");
        break;
    case "import":
        var file = new FileInfo(args[2]);
        if (file.Length > 256 * 1024 * 1024) throw new InvalidOperationException("Snapshot exceeds the explicit 256 MiB offline import budget");
        var input = JsonSerializer.Deserialize<Database>(await File.ReadAllTextAsync(file.FullName), Store.Json)
            ?? throw new InvalidDataException("Invalid snapshot");
        var imported = await new PostgresSnapshotMigration(stack).ImportAsync(input);
        Console.WriteLine(imported ? "PASS: snapshot imported atomically and canonical state verified" : "PASS: same snapshot and imported state verified, no duplicate import");
        break;
    case "export":
        var snapshot = await new PostgresSnapshotMigration(stack).ExportAsync();
        var destination = Path.GetFullPath(args[2]);
        var temporary = destination + ".tmp-" + Guid.NewGuid().ToString("N");
        try
        {
            await using (var output = new FileStream(temporary, FileMode.CreateNew, FileAccess.Write, FileShare.None))
            {
                await JsonSerializer.SerializeAsync(output, snapshot, Store.Json);
                output.Flush(flushToDisk: true);
            }
            File.Move(temporary, destination);
        }
        finally { File.Delete(temporary); }
        Console.WriteLine("PASS: offline legacy-compatible snapshot exported to a new file");
        break;
}
