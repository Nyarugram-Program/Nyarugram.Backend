using System.Text.Json;
using P = Botgram.Protocol.V1;

namespace Botgram.Backend.Storage;

public sealed partial class PostgresBackend
{
    private async Task<P.AccountPrivacyResponse> AccountPrivacy(Transaction work, Session session,
        P.AccountPrivacyRequest input, string domain)
    {
        await work.Execute("INSERT INTO botgram_backend_v1.privacy_accounts(user_id) VALUES($1) ON CONFLICT DO NOTHING", session.UserId);
        var json = (string)(await work.Scalar("SELECT state::text FROM botgram_backend_v1.privacy_accounts WHERE user_id=$1 FOR UPDATE", session.UserId))!;
        var state = JsonSerializer.Deserialize<AccountPrivacyState>(json, Store.Json)
            ?? throw new InvalidDataException("Missing privacy state");
        var rules = state.Rules.Values.Select(P.PrivacyRuleSet.Parser.ParseFrom).ToList();
        if (input.SetRules is not null) rules.Add(input.SetRules);
        var ids = rules.SelectMany(r => r.Rules).SelectMany(r => r.Users).Select(u => u.UserId)
            .Concat(state.Blocked.Keys).Append(session.UserId).ToHashSet(StringComparer.Ordinal);
        if (input.BlockUser is not null) ids.Add(ProtobufRpcMapper.UserReferenceId(input.BlockUser, domain));
        if (input.UnblockUser is not null) ids.Add(ProtobufRpcMapper.UserReferenceId(input.UnblockUser, domain));
        if (input.DeleteContacts is not null)
            foreach (var user in input.DeleteContacts.Users) ids.Add(ProtobufRpcMapper.UserReferenceId(user, domain));
        var groups = rules.SelectMany(r => r.Rules).SelectMany(r => r.GroupIds).Distinct(StringComparer.Ordinal).ToArray();
        var profiles = await Profiles(work, session.UserId, "a.id=ANY($2)", (object)ids.ToArray());
        var view = new Database
        {
            AccountPrivacy = new() { [session.UserId] = state },
            Users = profiles.Select(p => new User(p.Id, p.Username, p.DisplayName, "", "", p.Gid, p.Premium)).ToList(),
            Contacts = await work.Rows("""
                SELECT owner_id,user_id,first_name,last_name FROM botgram_backend_v1.contacts
                WHERE (owner_id=$1 AND user_id=ANY($2)) OR (user_id=$1 AND owner_id=ANY($2))
                """, r => new Contact(r.GetString(0), r.GetString(1), r.GetString(2), r.GetString(3)), session.UserId, ids.ToArray()),
            Conversations = await work.Rows("""
                SELECT c.id,c.kind,c.title,c.created_at,
                    ARRAY(SELECT user_id FROM botgram_intake_v1.members WHERE conversation_id=c.id ORDER BY user_id)
                FROM botgram_backend_v1.conversations c WHERE c.id=ANY($1)
                """, r => new Conversation(r.GetString(0), r.GetString(1), r.GetString(2), r.GetFieldValue<string[]>(4),
                    r.GetFieldValue<DateTimeOffset>(3)), (object)groups),
            Sessions = await work.Rows("SELECT token_hash,user_id,expires_at FROM botgram_intake_v1.sessions WHERE user_id=$1",
                r => new Session(r.GetString(0), r.GetString(1), r.GetFieldValue<DateTimeOffset>(2)), session.UserId)
        };
        foreach (var profile in profiles) view.TelegramIds["user:" + profile.Id] = profile.TelegramId;
        foreach (var mapping in await GroupIdentities(work, view.Conversations)) view.TelegramIds["chat:" + mapping.Key] = mapping.Value;
        var previousSessions = view.Sessions.Select(s => s.TokenHash).ToHashSet(StringComparer.Ordinal);
        var previousContacts = view.Contacts.Where(c => c.OwnerId == session.UserId).Select(c => c.UserId).ToHashSet(StringComparer.Ordinal);
        var result = AccountPrivacyService.Execute(view, session, input, domain);
        if (input.OperationCase == P.AccountPrivacyRequest.OperationOneofCase.ResetSavedContacts)
            await work.Execute("DELETE FROM botgram_backend_v1.contacts WHERE owner_id=$1", session.UserId);
        else
        {
            previousContacts.ExceptWith(view.Contacts.Where(c => c.OwnerId == session.UserId).Select(c => c.UserId));
            if (previousContacts.Count != 0)
                await work.Execute("DELETE FROM botgram_backend_v1.contacts WHERE owner_id=$1 AND user_id=ANY($2)", session.UserId, previousContacts.ToArray());
        }
        await work.Execute("UPDATE botgram_backend_v1.privacy_accounts SET state=$2::jsonb WHERE user_id=$1",
            session.UserId, JsonSerializer.Serialize(state, Store.Json));
        previousSessions.ExceptWith(view.Sessions.Select(s => s.TokenHash));
        if (previousSessions.Count != 0)
            await work.Execute("DELETE FROM botgram_intake_v1.sessions WHERE user_id=$1 AND token_hash=ANY($2)",
                session.UserId, previousSessions.ToArray());
        return result;
    }
}
