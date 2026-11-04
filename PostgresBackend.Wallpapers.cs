using System.Text.Json;
using P = Botgram.Protocol.V1;

namespace Botgram.Backend.Storage;

public sealed partial class PostgresBackend
{
    private const string WallpaperGate = "SELECT pg_advisory_xact_lock(2147000004,0)";

    private static async Task<(long Revision, WallpaperDefinition? Installed)> WallpaperAccount(Transaction work, string uid)
    {
        var rows = await work.Rows("""
            SELECT revision,installed::text FROM botgram_backend_v1.wallpaper_accounts WHERE user_id=$1
            """, r => (Revision: r.GetInt64(0), Installed: r.IsDBNull(1) ? null : WallpaperService.Read(r.GetString(1))), uid);
        return rows.Count == 0 ? (0L, null) : rows.Single();
    }

    public Task<long> WallpaperRevisionAsync(Session session, CancellationToken token = default) =>
        WriteAsync(session, async work => (await WallpaperAccount(work, session.UserId)).Revision, token);

    private async Task<long> WallpaperChanged(Transaction work, string uid, string? conversation)
    {
        var revision = (long)(await work.Scalar("""
            INSERT INTO botgram_backend_v1.wallpaper_accounts(user_id,revision) VALUES($1,1)
            ON CONFLICT(user_id) DO UPDATE SET revision=botgram_backend_v1.wallpaper_accounts.revision+1
            RETURNING revision
            """, uid))!;
        await Emit(work, "wallpaper.changed", new WallpaperChanged(uid, revision, conversation), "user:" + uid, [uid]);
        return revision;
    }

    private static async Task<P.WallpaperState> ChatWallpaper(Transaction work, string uid, string? conversation)
    {
        if (conversation is null) return WallpaperService.State(null);
        var rows = await work.Rows("""
            SELECT definition::text,overridden FROM (
                SELECT definition,true AS overridden FROM botgram_backend_v1.wallpaper_overrides
                    WHERE conversation_id=$1 AND user_id=$2
                UNION ALL
                SELECT definition,false AS overridden FROM botgram_backend_v1.wallpaper_shared WHERE conversation_id=$1
            ) states ORDER BY overridden DESC LIMIT 1
            """, r => WallpaperService.State(r.IsDBNull(0) ? null : WallpaperService.Read(r.GetString(0)), r.GetBoolean(1)),
            conversation, uid);
        return rows.SingleOrDefault() ?? WallpaperService.State(null);
    }

    private static async Task<Conversation?> UserWallpaperConversation(Transaction work, string uid, string other)
    {
        var members = new[] { uid, other }.Distinct().Order().ToArray();
        return (await Owned(work, uid)).SingleOrDefault(c =>
            c.Kind == (uid == other ? "saved" : "direct") && c.Members.SequenceEqual(members));
    }

    private static async Task<P.WallpaperList> Wallpapers(Transaction work, string uid, P.GetWallpapersRequest input)
    {
        await work.Scalar(WallpaperGate);
        var account = await WallpaperAccount(work, uid);
        var result = new P.WallpaperList { Revision = account.Revision, Hash = account.Revision,
            Installed = WallpaperService.State(account.Installed),
            NotModified = input.HasHash && input.Hash == account.Revision };
        if (!result.NotModified)
            result.Wallpapers.Add(await work.Rows("""
                SELECT definition::text FROM botgram_backend_v1.wallpaper_saved WHERE user_id=$1 ORDER BY ordinal
                """, r => WallpaperService.Map(WallpaperService.Read(r.GetString(0))), uid));
        return result;
    }

    private async Task<P.WallpaperMutationResult> SaveWallpaper(Transaction work, string uid, P.SaveWallpaperRequest input)
    {
        var wallpaper = WallpaperService.Parse(input.Wallpaper);
        var key = WallpaperService.Key(wallpaper);
        await work.Scalar(WallpaperGate);
        bool changed;
        if (input.Unsave)
            changed = await work.Execute("DELETE FROM botgram_backend_v1.wallpaper_saved WHERE user_id=$1 AND key=$2", uid, key) != 0;
        else
        {
            var existing = await work.Scalar("SELECT 1 FROM botgram_backend_v1.wallpaper_saved WHERE user_id=$1 AND key=$2", uid, key);
            if (existing is null && (long)(await work.Scalar(
                "SELECT count(*) FROM botgram_backend_v1.wallpaper_saved WHERE user_id=$1", uid))! >= 100)
                throw new ApiException(409, "WALLPAPER_SAVED_LIMIT");
            changed = await work.Execute("""
                INSERT INTO botgram_backend_v1.wallpaper_saved(user_id,key,definition) VALUES($1,$2,$3::jsonb)
                ON CONFLICT(user_id,key) DO NOTHING
                """, uid, key, JsonSerializer.Serialize(wallpaper)) != 0;
        }
        if (changed) await WallpaperChanged(work, uid, null);
        var account = await WallpaperAccount(work, uid);
        return new() { Applied = true, Revision = account.Revision, Installed = WallpaperService.State(account.Installed) };
    }

    private async Task<P.WallpaperMutationResult> InstallWallpaper(Transaction work, string uid, P.InstallWallpaperRequest input)
    {
        if (input.SelectionCase is not (P.InstallWallpaperRequest.SelectionOneofCase.Wallpaper or P.InstallWallpaperRequest.SelectionOneofCase.Clear))
            throw new ApiException(400, "WALLPAPER_SELECTION_INVALID");
        var wallpaper = input.SelectionCase == P.InstallWallpaperRequest.SelectionOneofCase.Wallpaper ?
            WallpaperService.Parse(input.Wallpaper) : null;
        await work.Scalar(WallpaperGate);
        var account = await WallpaperAccount(work, uid);
        if (account.Installed != wallpaper)
        {
            await work.Execute("""
                INSERT INTO botgram_backend_v1.wallpaper_accounts(user_id,installed) VALUES($1,$2::jsonb)
                ON CONFLICT(user_id) DO UPDATE SET installed=excluded.installed
                """, uid, wallpaper is null ? DBNull.Value : JsonSerializer.Serialize(wallpaper));
            await WallpaperChanged(work, uid, null);
            account = await WallpaperAccount(work, uid);
        }
        return new() { Applied = true, Revision = account.Revision, Installed = WallpaperService.State(account.Installed) };
    }

    private async Task<P.ChatWallpaperResult> SetWallpaper(Transaction work, string uid, string domain, P.SetChatWallpaperRequest input)
    {
        WallpaperService.ValidateSet(input);
        // Take this gate before any implicit direct creation (which can acquire capacity).
        await work.Scalar(WallpaperGate);
        Conversation? conversation;
        P.PeerRef peer;
        if (input.Peer.PeerCase == P.PeerRef.PeerOneofCase.GroupId)
        {
            conversation = await FindGroup(work, uid, input.Peer.GroupId);
            peer = new() { GroupId = conversation.Id };
        }
        else
        {
            var other = input.Peer.PeerCase switch
            {
                P.PeerRef.PeerOneofCase.User => ProtobufRpcMapper.UserReferenceId(input.Peer.User, domain),
                P.PeerRef.PeerOneofCase.Self when input.Peer.Self => uid,
                _ => throw new ApiException(400, "BOTGRAM_RPC_INVALID_ARGUMENT")
            };
            if (await work.Scalar("SELECT id FROM botgram_backend_v1.accounts WHERE id=$1", other) is null)
                throw new ApiException(404, "USER_ID_INVALID");
            peer = new() { User = new() { ServerDomain = domain, UserId = other } };
            conversation = await UserWallpaperConversation(work, uid, other);
            if (conversation is null && !input.Revert)
                conversation = await ResolveConversation(work, uid, peer, domain, true);
        }
        if (input.ForBoth && (conversation?.Kind != "direct" || conversation.Members.Length != 2))
            throw new ApiException(400, "WALLPAPER_FOR_BOTH_UNSUPPORTED");
        var wallpaper = input.SelectionCase == P.SetChatWallpaperRequest.SelectionOneofCase.Wallpaper ?
            WallpaperService.Parse(input.Wallpaper) : null;
        if (conversation is not null)
        {
            var changed = false;
            if (input.Revert)
                changed = await work.Execute("""
                    DELETE FROM botgram_backend_v1.wallpaper_overrides WHERE conversation_id=$1 AND user_id=$2
                    """, conversation.Id, uid) != 0;
            else
            {
                var json = wallpaper is null ? (object)DBNull.Value : JsonSerializer.Serialize(wallpaper);
                if (input.ForBoth)
                {
                    changed = await work.Execute("""
                        INSERT INTO botgram_backend_v1.wallpaper_shared(conversation_id,definition) VALUES($1,$2::jsonb)
                        ON CONFLICT(conversation_id) DO UPDATE SET definition=excluded.definition
                        WHERE botgram_backend_v1.wallpaper_shared.definition IS DISTINCT FROM excluded.definition
                        """, conversation.Id, json) != 0;
                    changed |= await work.Execute("DELETE FROM botgram_backend_v1.wallpaper_overrides WHERE conversation_id=$1",
                        conversation.Id) != 0;
                }
                else
                    changed = await work.Execute("""
                        INSERT INTO botgram_backend_v1.wallpaper_overrides(conversation_id,user_id,definition) VALUES($1,$2,$3::jsonb)
                        ON CONFLICT(conversation_id,user_id) DO UPDATE SET definition=excluded.definition
                        WHERE botgram_backend_v1.wallpaper_overrides.definition IS DISTINCT FROM excluded.definition
                        """, conversation.Id, uid, json) != 0;
            }
            if (changed)
                foreach (var recipient in (input.ForBoth ? conversation.Members : new[] { uid }).Order(StringComparer.Ordinal))
                    await WallpaperChanged(work, recipient, conversation.Id);
        }
        var state = await ChatWallpaper(work, uid, conversation?.Id);
        var account = await WallpaperAccount(work, uid);
        var updates = new P.Updates { DateMs = DateTimeOffset.UtcNow.ToUnixTimeSeconds() * 1000 };
        updates.Updates_.Add(new P.Update { PeerWallpaperChanged = new() { Peer = peer, State = state } });
        if (conversation?.Kind == "group")
        {
            var mapping = await GroupIdentities(work, [conversation]);
            updates.Groups.Add(ProtobufRpcMapper.MapGroup(conversation, mapping[conversation.Id]));
        }
        var users = conversation?.Members ?? [uid, peer.User.UserId];
        updates.Users.Add((await Profiles(work, uid, "a.id=ANY($2)", (object)users.Distinct().ToArray()))
            .Select(u => ProtobufRpcMapper.MapUser(u, domain)));
        return new() { State = state, Revision = account.Revision, Updates = updates };
    }
}
