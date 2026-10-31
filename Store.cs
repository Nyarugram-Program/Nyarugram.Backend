using System.Text.Json;

namespace Botgram.Backend;

// Single-process store. Writes are serialized and atomically replace the snapshot.
public sealed class Store
{
    public static readonly JsonSerializerOptions Json = new(JsonSerializerDefaults.Web);
    private readonly object gate = new();
    private readonly string path;
    private Database db;
    private readonly FileStream lease;

    public Store(IConfiguration config, IHostEnvironment environment)
    {
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

    public T Read<T>(Func<Database, T> action) { lock (gate) return action(db); }

    public T Write<T>(Func<Database, T> action)
    {
        lock (gate)
        {
            var previous = JsonSerializer.Serialize(db, Json);
            try
            {
                var now = DateTimeOffset.UtcNow;
                db.Sessions.RemoveAll(s => s.ExpiresAt <= now);
                db.Events.RemoveAll(e => e.ExpiresAt <= now);
                foreach (var call in db.Calls.Where(c => c.State is "ringing" or "active").ToArray())
                {
                    var timeout = call.State == "ringing" ? TimeSpan.FromSeconds(60) : TimeSpan.FromHours(2);
                    if (now - call.UpdatedAt <= timeout) continue;
                    var ended = call with { State = "ended", UpdatedAt = now };
                    db.Calls[db.Calls.IndexOf(call)] = ended;
                    db.Emit([call.CallerId, call.CalleeId], "call.updated", ended);
                }
                var result = action(db);
                using (var file = new FileStream(path + ".tmp", FileMode.Create, FileAccess.Write, FileShare.None))
                {
                    JsonSerializer.Serialize(file, db, Json);
                    file.Flush(true);
                }
                File.Move(path + ".tmp", path, true);
                GC.KeepAlive(lease);
                return result;
            }
            catch { db = JsonSerializer.Deserialize<Database>(previous, Json)!; throw; }
        }
    }
}
