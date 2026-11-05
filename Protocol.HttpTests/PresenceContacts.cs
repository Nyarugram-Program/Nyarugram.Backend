using System.Net;
using System.Text.Json.Nodes;
using Botgram.Backend;
using Botgram.Backend.Storage;
using Google.Protobuf;
using Npgsql;
using P = Botgram.Protocol.V1;

internal static partial class HttpTests
{
    private static async Task PresenceContacts(bool postgres = false, bool protobufOnly = false)
    {
        NpgsqlDataSource? admin = null, source = null;
        string? connection = null, database = null;
        var path = Path.Combine(Root, "presence.json");
        Task<Server> Start() => Server.Start(path, legacyEnabled: !postgres && !protobufOnly, legacyWssEnabled: !postgres && !protobufOnly,
            validationConnection: connection, fullRelational: postgres);
        P.UserRef Reference(string id) => new() { ServerDomain = Domain, UserId = id };
        try
        {
            if (postgres)
            {
                var settings = new NpgsqlConnectionStringBuilder(Environment.GetEnvironmentVariable("BOTGRAM_PG_TEST_CONNECTION_STRING")
                    ?? throw new InvalidOperationException("Presence PostgreSQL regression requires an isolated test connection"));
                if (!(settings.Host == "localhost" || IPAddress.TryParse(settings.Host, out var ip) && IPAddress.IsLoopback(ip))
                    || !System.Text.RegularExpressions.Regex.IsMatch(settings.Database ?? "", "^botgram_test_[a-z0-9_]+$"))
                    throw new InvalidOperationException("Only loopback botgram_test_* databases are allowed");
                admin = NpgsqlDataSource.Create(settings.ConnectionString);
                database = "botgram_test_presence_" + Guid.NewGuid().ToString("N");
                await using var create = admin.CreateCommand("CREATE DATABASE " + database);
                await create.ExecuteNonQueryAsync();
                settings.Database = database;
                connection = settings.ConnectionString;
                source = NpgsqlDataSource.Create(connection);
                await PostgresBackendSchema.ApplyAsync(source);
            }
            string aliceToken, bobToken, aliceId, bobId;
            long bobTlId;
            await using (var server = await Start())
            {
                async Task<(string Token, string Id)> Register(string name)
                {
                    var response = await server.Json("POST", "/api/auth/register",
                        new { username = name, displayName = name, password = "isolated-presence-password" });
                    return (Str(response, "token"), Str(response["user"]!, "id"));
                }
                (aliceToken, aliceId) = await Register("presence_alice");
                (bobToken, bobId) = await Register("presence_bob");
                var (carolToken, carolId) = await Register("presence_carol");
                bobTlId = Long(await server.Json("GET", "/api/telegram/me", null, bobToken), "id");
                async Task<P.RpcResponse> Rpc(string token, P.RpcRequest request, int status = 200, string? code = null)
                {
                    request.ProtocolMajor = 1;
                    request.RequestId = Guid.NewGuid().ToString("N");
                    return await server.Raw(request.ToByteArray(), status, code, token);
                }
                Task<P.RpcResponse> State(string token, bool? foreground = null)
                {
                    var request = new P.RpcRequest { GetState = new() };
                    if (foreground is { } active) request.Foreground = active;
                    return Rpc(token, request);
                }
                async Task<P.UserSummary> Observe() => (await Rpc(aliceToken, new()
                    { GetUsers = new() { Users = { new P.UserSelector { User = Reference(bobId) } } } })).GetUsers.Users.Single();
                Task<P.RpcResponse> Add(string token, string id, string first = "Bob", int status = 200, string? code = null) => Rpc(token, new()
                    { AddContact = new() { User = new() { User = Reference(id) }, FirstName = first, LastName = "Alias" } }, status, code);
                Task<P.RpcResponse> Privacy(P.AccountPrivacyRequest input) => Rpc(bobToken, new() { AccountPrivacy = input });
                Task<P.RpcResponse> Rules(params P.PrivacyRuleKind[] kinds) => Privacy(new()
                {
                    SetRules = new() { Key = P.PrivacyKey.StatusTimestamp,
                        Rules = { kinds.Select(kind => new P.PrivacyRule { Kind = kind }) } }
                });
                async Task<P.DifferencePage> Difference(string token, int pts)
                {
                    var response = await Rpc(token, new() { GetDifference = new() { Pts = pts } });
                    return response.GetDifference.Complete ?? response.GetDifference.Slice
                        ?? throw new Exception("Expected a presence difference page");
                }
                await State(bobToken);
                Check((await Observe()).Status.KindCase == P.UserStatus.KindOneofCase.None,
                    "omitted foreground does not manufacture activity");
                var added = await Add(aliceToken, bobId);
                Check(added.AddContact.Users.Single().IsContact && added.AddContact.Users.Single().FirstName == "Bob",
                    "protobuf addContact returns target with contact flag and alias");
                await Add(aliceToken, bobId);
                Check((await Rpc(aliceToken, new() { GetContacts = new() })).GetContacts.Contacts.Count == 1,
                    "protobuf addContact retry is deduplicated");
                await Add(aliceToken, aliceId, status: 400, code: "CONTACT_ADD_SELF");
                await Add(aliceToken, Guid.NewGuid().ToString("N"), status: 404, code: "USER_ID_INVALID");
                await Add(aliceToken, bobId, new string('x', 65), 400, "FIRSTNAME_INVALID");
                Check((await Rpc(aliceToken, new() { GetContacts = new() })).GetContacts.Users.Single().FirstName == "Bob",
                    "invalid contact mutations roll back");

                Task<P.RpcResponse> Special(string token, string id, bool enabled, int status = 200, string? code = null) => Rpc(token,
                    new() { SetSpecialContact = new() { User = new() { User = Reference(id) }, Enabled = enabled } }, status, code);
                async Task<P.SpecialContacts> Snapshot(string token) => (await Rpc(token, new() { GetSpecialContacts = new() })).GetSpecialContacts;
                var specialPts = (await State(aliceToken)).GetState.Pts;
                Check((await Snapshot(aliceToken)).Revision == 0, "special default revision is zero");
                await Special(aliceToken, carolId, true, 400, "CONTACT_ID_INVALID");
                await Special(aliceToken, aliceId, true, 400, "CONTACT_ID_INVALID");
                await Special(aliceToken, Guid.NewGuid().ToString("N"), false, 404, "USER_ID_INVALID");
                var enabled = (await Special(aliceToken, bobId, true)).SetSpecialContact;
                Check(enabled.Revision == 1 && enabled.Users.Single().UserId == bobId, "special enable returns full UUID snapshot");
                await Task.WhenAll(Enumerable.Range(0, 8).Select(_ => Special(aliceToken, bobId, true)));
                Check((await Snapshot(aliceToken)).Revision == 1, "concurrent special retries increment once");
                Check((await Snapshot(bobToken)).Users.Count == 0, "special state is account private");
                await Add(aliceToken, bobId, "Alias changed");
                Check((await Snapshot(aliceToken)).Revision == 1, "contact upsert preserves special preference");
                Check((await State(aliceToken)).GetState.Pts == specialPts, "special mutations never change pts");
                await Special(aliceToken, bobId, false);
                Check((await Special(aliceToken, bobId, false)).SetSpecialContact.Revision == 2, "special disable retry is idempotent");
                await Special(aliceToken, bobId, true);
                await Rpc(aliceToken, new() { AccountPrivacy = new() { DeleteContacts = new() { Users = { Reference(bobId) } } } });
                Check((await Snapshot(aliceToken)).Revision == 4 && (await Snapshot(aliceToken)).Users.Count == 0, "delete clears special once");
                await Add(aliceToken, bobId);
                Check((await Snapshot(aliceToken)).Users.Count == 0, "readding contact does not restore special");
                await Special(aliceToken, bobId, true);
                await Privacy(new() { BlockUser = Reference(aliceId) });
                Check((await Snapshot(aliceToken)).Revision == 6 && (await Snapshot(aliceToken)).Users.Count == 0, "reverse block clears special atomically");
                await Special(aliceToken, bobId, true, 403, "USER_IS_BLOCKED");
                await Privacy(new() { UnblockUser = Reference(aliceId) });
                Check((await Snapshot(aliceToken)).Users.Count == 0, "unblock does not restore special");
                await Special(aliceToken, bobId, true);
                await Rpc(aliceToken, new() { AccountPrivacy = new() { ResetSavedContacts = new() } });
                Check((await Snapshot(aliceToken)).Revision == 8, "reset contacts clears special revision");
                await Add(aliceToken, bobId);
                await Special(aliceToken, bobId, true);
                await server.Json("PUT", "/api/me/gid", new { regionCode = "030", number = "12345678" }, bobToken);
                Check((await Snapshot(aliceToken)).Users.Single().UserId == bobId && (await Snapshot(aliceToken)).Revision == 9,
                    "GID mutation preserves special UUID and revision");

                int before = (await State(aliceToken)).GetState.Pts;
                await State(bobToken, true);
                var online = await Observe();
                Check(online.Status.KindCase == P.UserStatus.KindOneofCase.OnlineUntilMs,
                    "protobuf foreground reports online");
                var diff = await Difference(aliceToken, before);
                Check(diff.Updates.Single().PresenceChanged.User.UserId == bobId
                    && diff.Updates.Single().PresenceChanged.Status.OnlineUntilMs > 0 && diff.State.Pts == before + 1,
                    "protobuf presence difference retains identity, status and pts");
                await State(bobToken, false);
                var offline = (await Observe()).Status;
                Check(offline.KindCase == P.UserStatus.KindOneofCase.LastSeenMs && offline.LastSeenMs > 0,
                    "background transitions to last-seen status");
                for (var i = 0; i < 3; i++) await State(bobToken, false);
                Check((await Observe()).Status.LastSeenMs == offline.LastSeenMs,
                    "background polling never refreshes last seen");

                var login = await server.Json("POST", "/api/auth/login",
                    new { username = "presence_bob", password = "isolated-presence-password" });
                var secondToken = Str(login, "token");
                await State(bobToken, true);
                await State(secondToken, true);
                await State(bobToken, false);
                Check((await Observe()).Status.KindCase == P.UserStatus.KindOneofCase.OnlineUntilMs,
                    "one background device preserves another foreground session");
                await State(secondToken, false);
                Check((await Observe()).Status.KindCase == P.UserStatus.KindOneofCase.LastSeenMs,
                    "all sessions background makes account offline");

                await Rules(P.PrivacyRuleKind.DisallowAll);
                before = (await State(aliceToken)).GetState.Pts;
                await State(bobToken, true);
                Check((await Observe()).Status.KindCase == P.UserStatus.KindOneofCase.None,
                    "StatusTimestamp DisallowAll redacts protobuf status");
                Check((await State(aliceToken)).GetState.Pts == before, "hidden presence is never broadcast");
                // Replay a previously visible event after a privacy change.
                diff = await Difference(aliceToken, 1);
                Check(diff.Updates.Where(u => u.PresenceChanged is not null)
                    .All(u => u.PresenceChanged.Status.KindCase == P.UserStatus.KindOneofCase.None),
                    "queued presence events use current privacy rules");
                if (!postgres && !protobufOnly)
                {
                    var legacy = await server.Json("POST", "/api/telegram/rpc",
                        T("TL_users_getUsers", ("id", new[] { T("TL_inputUser", ("user_id", bobTlId)) })), aliceToken);
                    Check(Str(legacy["objects"]![0]!["status"]!, "$type").EndsWith("$TL_userStatusEmpty"),
                        "legacy JSON also redacts hidden status");
                }
                await Rules(P.PrivacyRuleKind.AllowContacts);
                Check((await Observe()).Status.KindCase == P.UserStatus.KindOneofCase.None,
                    "contact-only visibility uses owner's contacts");
                await Add(bobToken, aliceId, "Alice");
                Check((await Observe()).Status.KindCase == P.UserStatus.KindOneofCase.OnlineUntilMs,
                    "owner's contact can see online status");
                await Rules(P.PrivacyRuleKind.AllowAll);
                await Privacy(new() { BlockUser = Reference(aliceId) });
                before = (await State(aliceToken)).GetState.Pts;
                await State(bobToken, false);
                await State(bobToken, true);
                Check((await Observe()).Status.KindCase == P.UserStatus.KindOneofCase.None,
                    "blocked user cannot observe status");
                Check((await State(aliceToken)).GetState.Pts == before, "blocked user receives no presence transitions");
                await Privacy(new() { UnblockUser = Reference(aliceId) });

                // Carol only appears in Bob's contacts. Include Bob in difference.users
                // so the Android mapper can resolve the update without a prior search.
                await Add(bobToken, carolId, "Carol");
                before = (await State(carolToken)).GetState.Pts;
                await State(bobToken, false);
                diff = await Difference(carolToken, before);
                Check(diff.Users.Any(u => u.Identity.UserId == bobId)
                    && diff.Updates.Single().PresenceChanged.User.UserId == bobId,
                    "reverse-contact presence includes subject identity for Android");
                if (!postgres && !protobufOnly)
                {
                    var legacy = await server.Json("POST", "/api/telegram/rpc",
                        T("TL_users_getUsers", ("id", new[] { T("TL_inputUser", ("user_id", bobTlId)) })), aliceToken);
                    Check(Long(legacy["objects"]![0]!["status"]!, "expires") > 0,
                        "legacy offline status uses actual Android expires field");
                }
                await State(bobToken, true);
                await Special(aliceToken, bobId, true);
            }

            // Advance persisted heartbeat times while the isolated backend is stopped.
            var old = DateTimeOffset.UtcNow.AddSeconds(-61);
            if (source is not null)
            {
                foreach (var sql in new[] {
                    "UPDATE botgram_backend_v1.presence_sessions SET foreground_at=$1",
                    "UPDATE botgram_backend_v1.presence_accounts SET last_seen=$1,notified_at=$1 WHERE online" })
                {
                    await using var age = source.CreateCommand(sql);
                    age.Parameters.AddWithValue(old);
                    await age.ExecuteNonQueryAsync();
                }
            }
            else
            {
                var data = Load(path);
                foreach (var session in data["sessions"]!.AsArray())
                    if (session!["foregroundAt"] is not null) session["foregroundAt"] = old;
                foreach (var user in data["users"]!.AsArray().Where(u => u!["online"]!.GetValue<bool>()))
                {
                    user!["lastSeen"] = old;
                    user["presenceNotifiedAt"] = old;
                }
                await File.WriteAllTextAsync(path, data.ToJsonString());
            }
            await using (var server = await Start())
            {
                var request = new P.RpcRequest { ProtocolMajor = 1, RequestId = "restart", GetContacts = new() };
                var response = await server.Raw(request.ToByteArray(), 200, null, aliceToken);
                Check(response.GetContacts.Contacts.Count == 1 && response.GetContacts.Users.Single().Identity.UserId == bobId,
                    "added contacts survive restart");
                var special = await server.Raw(new P.RpcRequest { ProtocolMajor = 1, RequestId = "special-restart", GetSpecialContacts = new() }.ToByteArray(), 200, null, aliceToken);
                Check(special.GetSpecialContacts.Revision == 11 && special.GetSpecialContacts.Users.Single().UserId == bobId,
                    "special UUID and revision survive restart");
                Check(response.GetContacts.Users.Single().Status.KindCase == P.UserStatus.KindOneofCase.LastSeenMs,
                    "stale foreground expires after restart and background requests");
            }
            Console.WriteLine($"PASS: presence/contact { (postgres ? "PostgreSQL" : "LegacyOnly") } regression; {assertions} assertions");
        }
        finally
        {
            if (source is not null) await source.DisposeAsync();
            if (admin is not null)
            {
                await using var drop = admin.CreateCommand("DROP DATABASE IF EXISTS " + database + " WITH (FORCE)");
                await drop.ExecuteNonQueryAsync();
                await admin.DisposeAsync();
            }
        }
    }
}
