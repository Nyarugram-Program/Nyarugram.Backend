using Botgram.Backend;
using Botgram.Backend.Storage;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using Npgsql;
using System.Text.Json;

var commands = new[] { "prepare", "import", "export", "inspect", "upgrade" };
if (args.Length is < 2 or > 3 || !commands.Contains(args[0], StringComparer.Ordinal))
    throw new ArgumentException("Expected prepare|inspect|upgrade <private-config.json>, or import|export <private-config.json> <snapshot.json>");
if (args[0] is "prepare" or "inspect" or "upgrade")
{
    if (args.Length != 2)
        throw new ArgumentException("Expected prepare|inspect|upgrade <private-config.json>, or import|export <private-config.json> <snapshot.json>");
}
else if (args.Length != 3)
    throw new ArgumentException("Expected prepare|inspect|upgrade <private-config.json>, or import|export <private-config.json> <snapshot.json>");
var builder = Host.CreateApplicationBuilder(new HostApplicationBuilderSettings { EnvironmentName = Environments.Development });
builder.Configuration.AddJsonFile(Path.GetFullPath(args[1]), optional: false);
if (args[0] is "inspect" or "upgrade")
{
    // Offline schema maintenance only audits the declared isolated target. It deliberately does not
    // load the runtime transport switches or start the service.
    if (!string.Equals(builder.Configuration["Storage:Mode"], nameof(StorageMode.NewStorageValidation), StringComparison.Ordinal))
        throw new InvalidOperationException("Schema maintenance requires Storage:Mode=NewStorageValidation");
    if (!string.Equals(builder.Configuration["Storage:ValidationAcknowledged"], "true", StringComparison.OrdinalIgnoreCase))
        throw new InvalidOperationException("Schema maintenance requires Storage:ValidationAcknowledged=true");
    if (builder.Configuration["Storage:ValidationBackend"] is not ("RelationalBackend" or "FourComponent"))
        throw new InvalidOperationException("Schema maintenance requires Storage:ValidationBackend=RelationalBackend or FourComponent");
    var settings = new NpgsqlConnectionStringBuilder(builder.Configuration["Storage:ValidationConnectionString"]
        ?? throw new InvalidOperationException("Storage:ValidationConnectionString is required"));
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
var policy = StoragePolicy.Load(builder.Configuration, builder.Environment);
if (policy.Mode != StorageMode.NewStorageValidation || builder.Configuration["Storage:ValidationBackend"] != "FourComponent")
    throw new InvalidOperationException("Storage.Admin prepare/import/export require the explicit isolated FourComponent configuration");
await using var stack = NpgsqlDataSource.Create(builder.Configuration["Storage:ValidationConnectionString"]!);
switch (args[0])
{
    case "prepare":
        await FourComponentSetup.PrepareAsync(stack, FourComponentOptions.Load(builder.Configuration));
        Console.WriteLine("PASS: isolated PostgreSQL schema, worker contract, Kafka topic/offset and Scylla projection prepared");
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
