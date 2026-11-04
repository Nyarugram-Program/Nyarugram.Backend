using Botgram.Backend;
using Botgram.Backend.Storage;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using Npgsql;
using System.Text.Json;

if (args.Length is < 2 or > 3 || args[0] is not ("prepare" or "import" or "export") ||
    (args[0] == "prepare") != (args.Length == 2))
    throw new ArgumentException("Expected prepare <private-config.json>, or import/export <private-config.json> <snapshot.json>");
var builder = Host.CreateApplicationBuilder(new HostApplicationBuilderSettings { EnvironmentName = Environments.Development });
builder.Configuration.AddJsonFile(Path.GetFullPath(args[1]), optional: false);
var policy = StoragePolicy.Load(builder.Configuration, builder.Environment);
if (policy.Mode != StorageMode.NewStorageValidation || builder.Configuration["Storage:ValidationBackend"] != "FourComponent")
    throw new InvalidOperationException("Storage.Admin requires the explicit isolated FourComponent configuration");
await using var source = NpgsqlDataSource.Create(builder.Configuration["Storage:ValidationConnectionString"]!);
switch (args[0])
{
    case "prepare":
        await FourComponentSetup.PrepareAsync(source, FourComponentOptions.Load(builder.Configuration));
        Console.WriteLine("PASS: isolated PostgreSQL schema, worker contract, Kafka topic/offset and Scylla projection prepared");
        break;
    case "import":
        var file = new FileInfo(args[2]);
        if (file.Length > 256 * 1024 * 1024) throw new InvalidOperationException("Snapshot exceeds the explicit 256 MiB offline import budget");
        var input = JsonSerializer.Deserialize<Database>(await File.ReadAllTextAsync(file.FullName), Store.Json)
            ?? throw new InvalidDataException("Invalid snapshot");
        var imported = await new PostgresSnapshotMigration(source).ImportAsync(input);
        Console.WriteLine(imported ? "PASS: snapshot imported atomically and canonical state verified" : "PASS: same snapshot and imported state verified, no duplicate import");
        break;
    case "export":
        var snapshot = await new PostgresSnapshotMigration(source).ExportAsync();
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
