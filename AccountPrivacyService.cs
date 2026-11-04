using System.Buffers.Binary;
using Google.Protobuf;
using P = Botgram.Protocol.V1;

namespace Botgram.Backend;

public sealed class AccountPrivacyState
{
    public Dictionary<int, byte[]> Rules { get; set; } = [];
    public byte[] Global { get; set; } = [];
    public Dictionary<string, DateTimeOffset> Blocked { get; set; } = [];
    public int AccountTtlDays { get; set; } = 365;
    public int DefaultHistoryTtlSeconds { get; set; }
    public AccountPasswordState Password { get; set; } = new();
    public AccountTotpState Totp { get; set; } = new();
    public bool TopPeersEnabled { get; set; } = true;
}

public static class AccountPrivacyService
{
    public static bool IsDefault(AccountPrivacyState state) => state.Rules.Count == 0 && state.Global.Length == 0
        && state.Blocked.Count == 0 && state.AccountTtlDays == 365 && state.DefaultHistoryTtlSeconds == 0
        && state.Password.Verifier.Length == 0 && state.Totp.Secret.Length == 0 && state.Totp.PendingSecret.Length == 0
        && state.TopPeersEnabled;

    public static AccountPrivacyState State(Database db, string uid)
    {
        db.AccountPrivacy ??= [];
        if (!db.AccountPrivacy.TryGetValue(uid, out var state)) db.AccountPrivacy[uid] = state = new();
        return state;
    }

    public static bool IsBlocked(Database db, string owner, string user)
        => db.AccountPrivacy?.GetValueOrDefault(owner)?.Blocked.ContainsKey(user) == true;

    public static bool Allowed(Database db, string owner, string actor, P.PrivacyKey key)
    {
        if (owner == actor) return true;
        if (IsBlocked(db, owner, actor)) return false;
        var bytes = db.AccountPrivacy?.GetValueOrDefault(owner)?.Rules.GetValueOrDefault((int)key);
        if (bytes is null) return true;
        bool contact = db.Contacts.Any(c => c.OwnerId == owner && c.UserId == actor);
        foreach (var rule in P.PrivacyRuleSet.Parser.ParseFrom(bytes).Rules)
        {
            switch (rule.Kind)
            {
                case P.PrivacyRuleKind.AllowAll: return true;
                case P.PrivacyRuleKind.DisallowAll: return false;
                case P.PrivacyRuleKind.AllowContacts when contact: return true;
                case P.PrivacyRuleKind.DisallowContacts when contact: return false;
                case P.PrivacyRuleKind.AllowUsers when rule.Users.Any(u => u.UserId == actor): return true;
                case P.PrivacyRuleKind.DisallowUsers when rule.Users.Any(u => u.UserId == actor): return false;
                case P.PrivacyRuleKind.AllowChatParticipants when SharedGroup(rule): return true;
                case P.PrivacyRuleKind.DisallowChatParticipants when SharedGroup(rule): return false;
                case P.PrivacyRuleKind.AllowPremium when db.Users.Any(u => u.Id == actor && u.Premium): return true;
            }
        }
        return false;

        bool SharedGroup(P.PrivacyRule rule) => db.Conversations.Any(c => c.Kind == "group"
            && rule.GroupIds.Contains(c.Id) && c.Members.Contains(actor));
    }

    public static void CheckMessage(Database db, Conversation conversation, string sender)
    {
        if (conversation.Kind != "direct") return;
        foreach (var recipient in conversation.Members.Where(id => id != sender))
        {
            if (IsBlocked(db, recipient, sender) || IsBlocked(db, sender, recipient))
                throw new ApiException(403, "USER_IS_BLOCKED");
            var bytes = db.AccountPrivacy?.GetValueOrDefault(recipient)?.Global;
            if (bytes is null || bytes.Length == 0) continue;
            var global = P.GlobalPrivacySettings.Parser.ParseFrom(bytes);
            if (global.NewNoncontactPeersRequirePremium && !db.Users.Any(u => u.Id == sender && u.Premium)
                && !db.Contacts.Any(c => c.OwnerId == recipient && c.UserId == sender))
                throw new ApiException(403, "USER_PRIVACY_RESTRICTED");
        }
    }

    public static P.AccountPrivacyResponse Execute(Database db, Session session, P.AccountPrivacyRequest request, string domain)
    {
        string uid = session.UserId;
        var state = State(db, uid);
        var result = new P.AccountPrivacyResponse();
        var related = new HashSet<string>();
        P.UserRef Reference(string id) => new() { ServerDomain = domain, UserId = id };
        string User(P.UserRef user)
        {
            var id = ProtobufRpcMapper.UserReferenceId(user, domain);
            if (!db.Users.Any(u => u.Id == id)) throw new ApiException(404, "USER_ID_INVALID");
            related.Add(id);
            return id;
        }
        switch (request.OperationCase)
        {
            case P.AccountPrivacyRequest.OperationOneofCase.DeleteContacts:
                if (request.DeleteContacts.Users.Count > 10000) throw new ApiException(400, "CONTACTS_TOO_MUCH");
                var deletedIds = request.DeleteContacts.Users.Select(User).ToHashSet();
                db.Contacts.RemoveAll(c => c.OwnerId == uid && deletedIds.Contains(c.UserId));
                result.ContactsDeleted = new();
                break;
            case P.AccountPrivacyRequest.OperationOneofCase.ResetSavedContacts:
                // Imported contacts are the only server-side address book representation.
                db.Contacts.RemoveAll(c => c.OwnerId == uid);
                result.Applied = true;
                break;
            case P.AccountPrivacyRequest.OperationOneofCase.ToggleTopPeers:
                state.TopPeersEnabled = request.ToggleTopPeers;
                result.Applied = true;
                break;
            case P.AccountPrivacyRequest.OperationOneofCase.GetWebAuthorizations:
                // This backend has no website-login issuer, so there are no website grants.
                result.WebAuthorizations = new();
                break;
            case P.AccountPrivacyRequest.OperationOneofCase.ResetWebAuthorization:
                throw new ApiException(400, "HASH_INVALID");
            case P.AccountPrivacyRequest.OperationOneofCase.ResetWebAuthorizations:
            case P.AccountPrivacyRequest.OperationOneofCase.ClearPaymentInfo:
                // Neither payment credentials nor shipping addresses are accepted or stored.
                result.Applied = true;
                break;
            case P.AccountPrivacyRequest.OperationOneofCase.GetPasskeys:
                result.Passkeys = new();
                break;
            case P.AccountPrivacyRequest.OperationOneofCase.GetPassword:
                result.Password = AccountPasswordService.Info(uid, session.TokenHash, state.Password);
                break;
            case P.AccountPrivacyRequest.OperationOneofCase.GetPasswordSettings:
                AccountPasswordService.Verify(uid, session.TokenHash, state.Password, request.GetPasswordSettings);
                result.PasswordSettings = new();
                break;
            case P.AccountPrivacyRequest.OperationOneofCase.UpdatePasswordSettings:
                AccountPasswordService.Update(uid, session.TokenHash, state.Password, request.UpdatePasswordSettings);
                result.Applied = true;
                break;
            case P.AccountPrivacyRequest.OperationOneofCase.GetRules:
                Key(request.GetRules.Key);
                result.Rules = state.Rules.TryGetValue((int)request.GetRules.Key, out var rules)
                    ? P.PrivacyRuleSet.Parser.ParseFrom(rules)
                    : new() { Key = request.GetRules.Key, Rules = { new P.PrivacyRule { Kind = P.PrivacyRuleKind.AllowAll } } };
                break;
            case P.AccountPrivacyRequest.OperationOneofCase.SetRules:
                ValidateRules(db, uid, request.SetRules, domain);
                state.Rules[(int)request.SetRules.Key] = request.SetRules.ToByteArray();
                result.Rules = request.SetRules.Clone();
                break;
            case P.AccountPrivacyRequest.OperationOneofCase.GetGlobal:
                result.Global = state.Global.Length == 0 ? new() : P.GlobalPrivacySettings.Parser.ParseFrom(state.Global);
                break;
            case P.AccountPrivacyRequest.OperationOneofCase.SetGlobal:
                if (request.SetGlobal.NoncontactPeersPaidStars != 0)
                    throw new ApiException(501, "PAID_MESSAGES_UNSUPPORTED");
                state.Global = request.SetGlobal.ToByteArray();
                result.Global = request.SetGlobal.Clone();
                break;
            case P.AccountPrivacyRequest.OperationOneofCase.GetAccountTtl:
                result.AccountTtl = new() { Days = state.AccountTtlDays };
                break;
            case P.AccountPrivacyRequest.OperationOneofCase.SetAccountTtl:
                if (request.SetAccountTtl.Days is < 1 or > 730) throw new ApiException(400, "TTL_DAYS_INVALID");
                state.AccountTtlDays = request.SetAccountTtl.Days;
                result.Applied = true;
                break;
            case P.AccountPrivacyRequest.OperationOneofCase.GetDefaultHistoryTtl:
                result.HistoryTtl = new() { Seconds = state.DefaultHistoryTtlSeconds };
                break;
            case P.AccountPrivacyRequest.OperationOneofCase.SetDefaultHistoryTtl:
                if (request.SetDefaultHistoryTtl.Seconds is < 0 or > 31_536_000 || request.SetDefaultHistoryTtl.Seconds % 60 != 0)
                    throw new ApiException(400, "TTL_PERIOD_INVALID");
                state.DefaultHistoryTtlSeconds = request.SetDefaultHistoryTtl.Seconds;
                result.Applied = true;
                break;
            case P.AccountPrivacyRequest.OperationOneofCase.GetBlocked:
                var page = request.GetBlocked;
                if (page.Offset < 0 || page.Limit is < 1 or > 100) throw new ApiException(400, "LIMIT_INVALID");
                result.Blocked = new() { Count = state.Blocked.Count };
                foreach (var pair in state.Blocked.OrderByDescending(p => p.Value).ThenBy(p => p.Key, StringComparer.Ordinal)
                    .Skip(page.Offset).Take(page.Limit))
                {
                    related.Add(pair.Key);
                    result.Blocked.Blocked.Add(new P.BlockedUser { User = Reference(pair.Key), BlockedAtMs = pair.Value.ToUnixTimeMilliseconds() });
                }
                break;
            case P.AccountPrivacyRequest.OperationOneofCase.BlockUser:
                var blockedId = User(request.BlockUser);
                if (blockedId == uid) throw new ApiException(400, "USER_ID_INVALID");
                state.Blocked.TryAdd(blockedId, DateTimeOffset.UtcNow);
                result.Applied = true;
                break;
            case P.AccountPrivacyRequest.OperationOneofCase.UnblockUser:
                state.Blocked.Remove(User(request.UnblockUser));
                result.Applied = true;
                break;
            case P.AccountPrivacyRequest.OperationOneofCase.GetAuthorizations:
                result.Authorizations = new() { AuthorizationTtlDays = 30 };
                foreach (var item in db.Sessions.Where(s => s.UserId == uid && s.ExpiresAt > DateTimeOffset.UtcNow))
                    result.Authorizations.Sessions.Add(new P.AuthorizationInfo
                    {
                        Hash = SessionId(item), Current = item.TokenHash == session.TokenHash,
                        AppName = "Nyarugram"
                    });
                break;
            case P.AccountPrivacyRequest.OperationOneofCase.ResetAuthorization:
                var matches = db.Sessions.Where(s => s.UserId == uid && SessionId(s) == request.ResetAuthorization.Hash).ToArray();
                if (matches.Length != 1 || matches[0].TokenHash == session.TokenHash)
                    throw new ApiException(400, "HASH_INVALID");
                db.Sessions.Remove(matches[0]);
                result.Applied = true;
                break;
            case P.AccountPrivacyRequest.OperationOneofCase.ResetOtherAuthorizations:
                db.Sessions.RemoveAll(s => s.UserId == uid && s.TokenHash != session.TokenHash);
                result.Applied = true;
                break;
            default:
                throw new ApiException(400, "ACCOUNT_PRIVACY_OPERATION_INVALID");
        }
        if (result.Rules is not null)
        {
            foreach (var reference in result.Rules.Rules.SelectMany(r => r.Users)) related.Add(reference.UserId);
            var groups = result.Rules.Rules.SelectMany(r => r.GroupIds).ToHashSet();
            foreach (var group in db.Conversations.Where(c => groups.Contains(c.Id) && c.Members.Contains(uid)))
                result.Groups.Add(new P.GroupSummary { GroupId = group.Id, TlChatId = IdentityService.ChatId(db, group),
                    Title = group.Title, ParticipantCount = group.Members.Length, CreatedAtMs = group.CreatedAt.ToUnixTimeMilliseconds(), Version = 1 });
        }
        foreach (var user in db.Users.Where(u => related.Contains(u.Id)))
            result.Users.Add(ProtobufRpcMapper.MapUser(UserQueryService.Profile(db, user, uid), domain));
        return result;
    }

    private static long SessionId(Session session)
    {
        long value = BinaryPrimitives.ReadInt64BigEndian(Convert.FromHexString(session.TokenHash)) & long.MaxValue;
        return value == 0 ? 1 : value;
    }

    private static void Key(P.PrivacyKey key)
    {
        if (!Enum.IsDefined(key) || key == P.PrivacyKey.Unspecified) throw new ApiException(400, "PRIVACY_KEY_INVALID");
    }

    internal static void ValidateRules(Database db, string uid, P.PrivacyRuleSet input, string? domain)
    {
        Key(input.Key);
        if (input.Rules.Count is < 1 or > 128) throw new ApiException(400, "PRIVACY_RULES_INVALID");
        foreach (var rule in input.Rules)
        {
            if (!Enum.IsDefined(rule.Kind) || rule.Kind == P.PrivacyRuleKind.Unspecified
                || rule.Users.Count > 1000 || rule.GroupIds.Count > 100)
                throw new ApiException(400, "PRIVACY_RULES_INVALID");
            bool users = rule.Kind is P.PrivacyRuleKind.AllowUsers or P.PrivacyRuleKind.DisallowUsers;
            bool groups = rule.Kind is P.PrivacyRuleKind.AllowChatParticipants or P.PrivacyRuleKind.DisallowChatParticipants;
            if ((!users && rule.Users.Count != 0) || (!groups && rule.GroupIds.Count != 0))
                throw new ApiException(400, "PRIVACY_RULES_INVALID");
            ProtobufRpcMapper.ValidateUnique(rule.Users.Select(u => u.UserId));
            foreach (var reference in rule.Users)
            {
                string id = domain is null ? reference.UserId : ProtobufRpcMapper.UserReferenceId(reference, domain);
                if (!db.Users.Any(u => u.Id == id)) throw new ApiException(400, "USER_ID_INVALID");
            }
            ProtobufRpcMapper.ValidateUnique(rule.GroupIds);
            if (rule.GroupIds.Any(id => !db.Conversations.Any(c => c.Id == id && c.Kind == "group" && c.Members.Contains(uid))))
                throw new ApiException(400, "CHAT_ID_INVALID");
        }
    }
}
