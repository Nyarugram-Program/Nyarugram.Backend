using System.Text.Json;

namespace Botgram.Backend;

// Single-process store. Writes are serialized and atomically replace the snapshot.
public sealed class Store : IDisposable
{
    public static readonly JsonSerializerOptions Json = new(JsonSerializerDefaults.Web);
    private readonly object gate = new();
    private readonly string path = "";
    private Database db = new();
    private readonly FileStream? lease;
    private readonly PostgresValidationStore? validation;
    #region debug-point store-stages
    private readonly bool diagnostic;
    #endregion

    public Store(IConfiguration config, IHostEnvironment environment)
    {
        #region debug-point store-stages
        diagnostic = config.GetValue<bool>("Protocol:DiagnosticStages");
        #endregion
        var policy = StoragePolicy.Load(config, environment);
        if (policy.Mode == StorageMode.NewStorageValidation)
        {
            validation = new PostgresValidationStore(config);
            return;
        }
        path = Path.GetFullPath(config["DataPath"] ?? Path.Combine(environment.ContentRootPath, "data", "botgram.json"));
        Directory.CreateDirectory(Path.GetDirectoryName(path)!);
        // Refuse a second process using the same database.
        lease = new FileStream(path + ".lock", FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.None);
        db = File.Exists(path) ? JsonSerializer.Deserialize<Database>(File.ReadAllText(path), Json)
            ?? throw new InvalidDataException("Invalid database") : new();
        // Existing accounts keep their UUID, sessions and chats; GIDs are assigned once.
        if (db.Users.Any(u => string.IsNullOrEmpty(u.Gid))) Write(database => { Gids.Migrate(database); return true; });
        else Gids.Migrate(db);
    }

    public T Read<T>(Func<Database, T> action) { if (validation is not null) return validation.Read(action); lock (gate) return action(db); }

    public void Dispose() { validation?.Dispose(); lease?.Dispose(); }

    internal static void Maintain(Database database)
    {
        var now = DateTimeOffset.UtcNow;
        database.Sessions.RemoveAll(s => s.ExpiresAt <= now);
        database.Events.RemoveAll(e => e.ExpiresAt <= now);
        PresenceService.Expire(database);
        foreach (var call in database.Calls.Where(c => CallService.InProgress(c.State)).ToArray())
        {
            if (!CallService.Expired(call, now)) continue;
            var ended = call with { State = "ended", UpdatedAt = now };
            database.Calls[database.Calls.IndexOf(call)] = ended;
            database.Emit([call.CallerId, call.CalleeId], "call.updated", ended);
        }
    }

    public T Write<T>(Func<Database, T> action)
    {
        if (validation is not null) return validation.Write(action);
        #region debug-point store-stages
        var stage = diagnostic ? System.Diagnostics.Stopwatch.GetTimestamp() : 0;
        #endregion
        lock (gate)
        {
            #region debug-point store-stages
            if (diagnostic) System.Diagnostics.Activity.Current?.SetTag("diag.lock_ms", System.Diagnostics.Stopwatch.GetElapsedTime(stage).TotalMilliseconds);
            if (diagnostic) stage = System.Diagnostics.Stopwatch.GetTimestamp();
            #endregion
            try
            {
                Maintain(db);
                #region debug-point store-stages
                if (diagnostic) System.Diagnostics.Activity.Current?.SetTag("diag.maintenance_ms", System.Diagnostics.Stopwatch.GetElapsedTime(stage).TotalMilliseconds);
                if (diagnostic) stage = System.Diagnostics.Stopwatch.GetTimestamp();
                #endregion
                var result = action(db);
                #region debug-point store-stages
                if (diagnostic) System.Diagnostics.Activity.Current?.SetTag("diag.action_ms", System.Diagnostics.Stopwatch.GetElapsedTime(stage).TotalMilliseconds);
                if (diagnostic) stage = System.Diagnostics.Stopwatch.GetTimestamp();
                #endregion
                using (var file = new FileStream(path + ".tmp", FileMode.Create, FileAccess.Write, FileShare.None))
                {
                    JsonSerializer.Serialize(file, db, Json);
                    #region debug-point store-stages
                    if (diagnostic) System.Diagnostics.Activity.Current?.SetTag("diag.persist_encode_ms", System.Diagnostics.Stopwatch.GetElapsedTime(stage).TotalMilliseconds);
                    if (diagnostic) stage = System.Diagnostics.Stopwatch.GetTimestamp();
                    #endregion
                    file.Flush(true);
                    #region debug-point store-stages
                    if (diagnostic) System.Diagnostics.Activity.Current?.SetTag("diag.flush_ms", System.Diagnostics.Stopwatch.GetElapsedTime(stage).TotalMilliseconds);
                    if (diagnostic) stage = System.Diagnostics.Stopwatch.GetTimestamp();
                    #endregion
                }
                File.Move(path + ".tmp", path, true);
                #region debug-point store-stages
                if (diagnostic) System.Diagnostics.Activity.Current?.SetTag("diag.replace_ms", System.Diagnostics.Stopwatch.GetElapsedTime(stage).TotalMilliseconds);
                #endregion
                GC.KeepAlive(lease);
                return result;
            }
            catch (Exception failure)
            {
                // The on-disk snapshot is still the pre-transaction state: reading it back is
                // equivalent to the former in-memory copy, without serializing the whole database
                // on every request.
                try
                {
                    db = File.Exists(path) ? JsonSerializer.Deserialize<Database>(File.ReadAllText(path), Json)
                        ?? throw new InvalidDataException("Invalid database") : new Database();
                }
                catch (Exception restore)
                {
                    throw new AggregateException("Failed write could not be rolled back", failure, restore);
                }
                throw;
            }
        }
    }
}
