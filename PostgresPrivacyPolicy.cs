using System.Text.Json;
using Npgsql;
using P = Botgram.Protocol.V1;

namespace Botgram.Backend.Storage;

internal static class PostgresPrivacyPolicy
{
    internal static async Task<Database> Load(NpgsqlConnection connection, NpgsqlTransaction transaction,
        string[] owners, string actor, CancellationToken token)
    {
        async Task<List<T>> Rows<T>(string sql, Func<NpgsqlDataReader, T> read, params object[] values)
        {
            await using var query = new NpgsqlCommand(sql, connection, transaction);
            foreach (var value in values) query.Parameters.AddWithValue(value);
            await using var reader = await query.ExecuteReaderAsync(token);
            var results = new List<T>();
            while (await reader.ReadAsync(token)) results.Add(read(reader));
            return results;
        }
        // The generic intake test/ingestion schema can exist without the app backend schema.
        if (!(await Rows("SELECT to_regclass('botgram_backend_v1.privacy_accounts') IS NOT NULL", r => r.GetBoolean(0))).Single())
            return new();
        var states = await Rows("""
            SELECT user_id,state::text FROM botgram_backend_v1.privacy_accounts
            WHERE user_id=ANY($1) ORDER BY user_id COLLATE "C"
            """, r => (Owner: r.GetString(0), State: JsonSerializer.Deserialize<AccountPrivacyState>(r.GetString(1), Store.Json)
                ?? throw new InvalidDataException("Missing privacy state")), (object)owners);
        var db = new Database { AccountPrivacy = states.ToDictionary(s => s.Owner, s => s.State, StringComparer.Ordinal) };
        if (states.Count == 0) return db;
        db.Users = await Rows("SELECT id,premium FROM botgram_backend_v1.accounts WHERE id=$1",
            r => new User(r.GetString(0), "", "", "", "", Premium: r.GetBoolean(1)), actor);
        db.Contacts = await Rows("SELECT owner_id,user_id FROM botgram_backend_v1.contacts WHERE owner_id=ANY($1) AND user_id=$2",
            r => new Contact(r.GetString(0), r.GetString(1), "", ""), owners, actor);
        var groups = states.SelectMany(s => s.State.Rules.Values).Select(P.PrivacyRuleSet.Parser.ParseFrom)
            .SelectMany(r => r.Rules).SelectMany(r => r.GroupIds).Distinct().ToArray();
        db.Conversations = await Rows("""
            SELECT c.id FROM botgram_backend_v1.conversations c JOIN botgram_intake_v1.members m ON m.conversation_id=c.id
            WHERE c.id=ANY($1) AND c.kind='group' AND m.user_id=$2
            """, r => new Conversation(r.GetString(0), "group", "", [actor], default), groups, actor);
        return db;
    }
}
