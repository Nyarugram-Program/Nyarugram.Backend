using Botgram.Backend;
using Botgram.Backend.Storage;
using Google.Protobuf;
using Npgsql;
using P = Botgram.Protocol.V1;

static class RelationalWallpaperTests
{
    public static async Task<int> RunAsync(NpgsqlDataSource source, string connectionString)
    {
        var assertions = 0;
        void Check(bool value, string label)
        {
            if (!value) throw new InvalidOperationException(label);
            assertions++;
        }
        async Task<object?> Scalar(string sql, params object[] values)
        {
            await using var command = source.CreateCommand(sql);
            foreach (var value in values) command.Parameters.Add(new NpgsqlParameter { Value = value });
            return await command.ExecuteScalarAsync();
        }
        async Task Reject(Func<Task> action, int status, string code)
        {
            try { await action(); }
            catch (ApiException exception) when (exception.Status == status && exception.Message == code)
            { assertions++; return; }
            throw new InvalidOperationException($"Expected {status}:{code}");
        }
        var backend = new PostgresBackend(source);
        async Task<Session> Register(string name) =>
            await backend.RequireAsync(Auth.HashToken((await backend.RegisterAsync(new(name, name, "isolated-password"))).Token));
        var alice = await Register("wallpaper_alice");
        var bob = await Register("wallpaper_bob");
        var outsider = await Register("wallpaper_outside");
        const string domain = "wallpaper.test";
        P.PeerRef Peer(string id) => new() { User = new() { UserId = id, ServerDomain = domain } };
        async Task<P.RpcResponse> Rpc(Session actor, P.RpcRequest request) =>
            await backend.DispatchAsync(actor, request, domain, r => P.RpcResponse.Parser.ParseFrom(r.ToByteArray()));
        Task<P.RpcResponse> Get(Session actor) => Rpc(actor, new() { GetWallpapers = new() });
        async Task<P.WallpaperState> Full(Session actor, string other) =>
            (await Rpc(actor, new() { GetFullUser = new() { User = new() { User = Peer(other).User } } })).GetFullUser.Wallpaper;
        void CheckResponse(P.ChatWallpaperResult result, P.PeerRef peer, string label)
        {
            Check(result.State is not null && result.Updates is not null && result.Updates.Updates_.Count == 1 &&
                result.Updates.Updates_[0].KindCase == P.Update.KindOneofCase.PeerWallpaperChanged &&
                result.Updates.Updates_[0].PeerWallpaperChanged.Peer.Equals(peer) &&
                result.Updates.Updates_[0].PeerWallpaperChanged.State.Equals(result.State),
                label + " did not return the matching peer update");
        }
        P.Wallpaper black = new() { Id = 0, NoFile = new() { Settings = new() { BackgroundColor = 0 } } };
        P.Wallpaper gradient = new() { Id = long.MinValue, NoFile = new() { Dark = true, Settings = new()
        {
            BackgroundColor = 0, SecondBackgroundColor = 0xffffff, ThirdBackgroundColor = 0xabcdef,
            FourthBackgroundColor = 0, Rotation = 0, Intensity = -100, Motion = true
        } } };
        Check(WallpaperService.Map(WallpaperService.Parse(gradient)).Equals(gradient), "wallpaper settings presence roundtrip");
        var initial = (await Get(alice)).GetWallpapers;
        Check(initial.Revision == 0 && initial.Wallpapers.Count == 0 &&
            initial.Installed.ValueCase == P.WallpaperState.ValueOneofCase.Cleared, "wallpaper initial state");
        var save = new P.RpcRequest { SaveWallpaper = new() { Wallpaper = black } };
        var saved = (await Rpc(alice, save)).SaveWallpaper;
        Check(saved.Applied && saved.Revision == 1, "wallpaper save durable revision");
        await Rpc(alice, save);
        Check(await backend.WallpaperRevisionAsync(alice) == 1, "wallpaper save retry advanced revision");
        var zeroGradient = gradient.Clone(); zeroGradient.Id = 0;
        await Rpc(alice, new() { SaveWallpaper = new() { Wallpaper = zeroGradient } });
        Check((await Get(alice)).GetWallpapers.Wallpapers.Count == 2, "same nofile ID lost distinct settings");
        Check((await Get(bob)).GetWallpapers.Revision == 0 && (await Get(bob)).GetWallpapers.Wallpapers.Count == 0,
            "saved wallpapers leaked across accounts");
        var installed = (await Rpc(alice, new() { InstallWallpaper = new() { Wallpaper = gradient } })).InstallWallpaper;
        Check(installed.Installed.Wallpaper.Equals(gradient) && installed.Revision == 3, "global install persistence");
        var conditional = (await Rpc(alice, new() { GetWallpapers = new() { Hash = installed.Revision } })).GetWallpapers;
        Check(conditional.NotModified && conditional.Revision == 3 && conditional.Installed.Wallpaper.Equals(gradient),
            "hash hit omitted recovery revision or installed state");
        await Rpc(alice, new() { SaveWallpaper = new() { Wallpaper = black, Unsave = true } });
        Check((await Get(alice)).GetWallpapers.Wallpapers.Single().Equals(zeroGradient) &&
            (await Get(alice)).GetWallpapers.Installed.Wallpaper.Equals(gradient), "unsave changed installed state");
        async Task Rollback(P.RpcRequest request)
        {
            var before = (await Get(alice)).ToByteArray();
            var events = (long)(await Scalar("SELECT count(*) FROM botgram_intake_v1.events"))!;
            try
            {
                await backend.DispatchAsync<int>(alice, request, domain, _ => throw new PreparationFailure());
                throw new InvalidOperationException("Expected wallpaper response preparation failure");
            }
            catch (PreparationFailure) { assertions++; }
            Check(before.SequenceEqual((await Get(alice)).ToByteArray()) &&
                events == (long)(await Scalar("SELECT count(*) FROM botgram_intake_v1.events"))!, "wallpaper rollback leaked state");
        }
        await Rollback(new() { InstallWallpaper = new() { Clear = new() } });
        var invalid = black.Clone(); invalid.NoFile.Settings.ClearBackgroundColor();
        await Reject(() => Rpc(alice, new() { SaveWallpaper = new() { Wallpaper = invalid } }), 400, "WALLPAPER_BACKGROUND_REQUIRED");
        invalid.NoFile.Settings.BackgroundColor = 0x1000000;
        await Reject(() => Rpc(alice, new() { SaveWallpaper = new() { Wallpaper = invalid } }), 400, "WALLPAPER_SETTINGS_INVALID");
        invalid = black.Clone(); invalid.NoFile.Settings.SecondBackgroundColor = 0;
        await Reject(() => Rpc(alice, new() { SaveWallpaper = new() { Wallpaper = invalid } }), 400, "WALLPAPER_SETTINGS_INVALID");
        invalid = black.Clone(); invalid.NoFile.Settings.Emoticon = "theme";
        await Reject(() => Rpc(alice, new() { SaveWallpaper = new() { Wallpaper = invalid } }), 501, "BOTGRAM_WALLPAPER_EMOTICON_UNSUPPORTED");
        await Reject(() => Rpc(alice, new() { SaveWallpaper = new() { Wallpaper = new() } }), 501, "BOTGRAM_WALLPAPER_KIND_UNSUPPORTED");
        await Reject(() => Rpc(alice, new() { InstallWallpaper = new() }), 400, "WALLPAPER_SELECTION_INVALID");
        var set = new P.RpcRequest { SetChatWallpaper = new() { Peer = Peer(bob.UserId), Wallpaper = black } };
        await Rollback(set);
        Check((await backend.ConversationsAsync(alice)).Length == 0, "wallpaper implicit conversation escaped rollback");
        var personal = (await Rpc(alice, set)).SetChatWallpaper;
        CheckResponse(personal, Peer(bob.UserId), "personal wallpaper");
        Check(personal.State.Wallpaper.Equals(black) && personal.State.Overridden, "personal wallpaper state");
        Check((await Full(bob, alice.UserId)).ValueCase == P.WallpaperState.ValueOneofCase.Cleared,
            "personal setting leaked into other view");
        var shared = set.Clone(); shared.SetChatWallpaper.ForBoth = true; shared.SetChatWallpaper.Wallpaper = gradient;
        var sharedResponse = (await Rpc(alice, shared)).SetChatWallpaper;
        CheckResponse(sharedResponse, Peer(bob.UserId), "shared wallpaper");
        Check(sharedResponse.State.Wallpaper.Equals(gradient) && !sharedResponse.State.Overridden, "shared response state");
        Check((await Full(bob, alice.UserId)).Wallpaper.Equals(gradient) && !(await Full(alice, bob.UserId)).Overridden,
            "shared wallpaper or override removal");
        var clear = new P.RpcRequest { SetChatWallpaper = new() { Peer = Peer(alice.UserId), Clear = new() } };
        var clearResponse = (await Rpc(bob, clear)).SetChatWallpaper;
        CheckResponse(clearResponse, Peer(alice.UserId), "personal clear");
        Check(clearResponse.State.ValueCase == P.WallpaperState.ValueOneofCase.Cleared && clearResponse.State.Overridden,
            "personal clear response state");
        var cleared = await Full(bob, alice.UserId);
        Check(cleared.ValueCase == P.WallpaperState.ValueOneofCase.Cleared && cleared.Overridden &&
            (await Full(alice, bob.UserId)).Wallpaper.Equals(gradient), "personal clear failed to suppress shared state");
        // Simulate dropped hints/reconnect: only the durable revision and full query are used.
        var recoveredRevision = (await Get(bob)).GetWallpapers.Revision;
        await using (var reopened = NpgsqlDataSource.Create(connectionString))
        {
            var restored = new PostgresBackend(reopened);
            var restoredState = await restored.DispatchAsync(bob,
                new P.RpcRequest { GetFullUser = new() { User = new() { User = Peer(alice.UserId).User } } },
                domain, r => r.GetFullUser.Wallpaper);
            Check(await restored.WallpaperRevisionAsync(bob) == recoveredRevision && restoredState.Equals(cleared),
                "reopen lost durable revision or clear override");
        }
        var revertResponse = (await Rpc(bob, new() { SetChatWallpaper = new() { Peer = Peer(alice.UserId), Revert = true } })).SetChatWallpaper;
        CheckResponse(revertResponse, Peer(alice.UserId), "revert");
        Check(revertResponse.State.Wallpaper.Equals(gradient) && !revertResponse.State.Overridden, "revert response state");
        Check((await Full(bob, alice.UserId)).Wallpaper.Equals(gradient) && !(await Full(bob, alice.UserId)).Overridden,
            "revert failed to reveal shared state");
        var beforeRetry = await backend.WallpaperRevisionAsync(bob);
        var retryResponse = (await Rpc(bob, new() { SetChatWallpaper = new() { Peer = Peer(alice.UserId), Revert = true } })).SetChatWallpaper;
        CheckResponse(retryResponse, Peer(alice.UserId), "repeated revert");
        Check(await backend.WallpaperRevisionAsync(bob) == beforeRetry, "repeated revert emitted revision");
        var selfResponse = (await Rpc(alice, new() { SetChatWallpaper = new() { Peer = new() { Self = true }, Revert = true } })).SetChatWallpaper;
        CheckResponse(selfResponse, Peer(alice.UserId), "self peer normalization");
        var group = await backend.CreateConversationAsync(alice, new("group", "wallpaper group", [bob.UserId]));
        var groupSet = new P.RpcRequest { SetChatWallpaper = new() { Peer = new() { GroupId = group.Id }, Wallpaper = black } };
        var groupResponse = (await Rpc(alice, groupSet)).SetChatWallpaper;
        CheckResponse(groupResponse, new P.PeerRef { GroupId = group.Id }, "group wallpaper");
        Check(groupResponse.State.Wallpaper.Equals(black) && groupResponse.State.Overridden &&
            groupResponse.Updates.Groups.Single().GroupId == group.Id, "group response state or identity");
        Check((await Rpc(alice, new() { GetFullGroup = new() { GroupId = group.Id } })).GetFullGroup.Wallpaper.Overridden,
            "group full missing personal wallpaper");
        groupSet.SetChatWallpaper.ForBoth = true;
        await Reject(() => Rpc(alice, groupSet), 400, "WALLPAPER_FOR_BOTH_UNSUPPORTED");
        await Reject(() => Rpc(outsider, groupSet), 404, "PEER_ID_INVALID");
        await Reject(() => Rpc(alice, new() { SetChatWallpaper = new() { Peer = Peer(bob.UserId), ServiceMessageId = 1 } }),
            501, "BOTGRAM_WALLPAPER_SERVICE_MESSAGE_UNSUPPORTED");
        var revisionBeforeReject = await backend.WallpaperRevisionAsync(alice);
        await Reject(() => new PostgresBackend(source, 1).DispatchAsync(alice,
            new P.RpcRequest { InstallWallpaper = new() { Clear = new() } }, domain, r => r.ToByteArray()), 503, "storage_backlog_full");
        Check(await backend.WallpaperRevisionAsync(alice) == revisionBeforeReject &&
            (await Get(alice)).GetWallpapers.Installed.Wallpaper.Equals(gradient), "capacity rollback lost installed wallpaper");
        Check((await backend.DifferenceAsync(alice, 1)).Kind == DifferenceKind.Empty &&
            (await backend.DifferenceAsync(bob, 1)).Kind == DifferenceKind.Empty &&
            (long)(await Scalar("""
                SELECT count(*) FROM botgram_intake_v1.messages m JOIN botgram_intake_v1.members own
                ON own.conversation_id=m.conversation_id WHERE own.user_id=$1
                """, alice.UserId))! == 0, "wallpaper changes polluted message sequence/pts");
        var delivery = new PostgresDeliveryPersistence(source);
        var leases = await delivery.ClaimAsync(1000, TimeSpan.FromMinutes(1), default);
        var lease = leases.First(l => l.Delivery.Type == "wallpaper.changed" &&
            ((WallpaperChanged)l.Delivery.Payload).UserId == alice.UserId);
        Check(lease.Delivery.SchemaVersion == 2 && lease.Delivery.Recipients.Single().Pts is null &&
            lease.Delivery.ProjectionFormatVersion is null, "wallpaper envelope version/projection metadata");
        Check(DeliveryEventCodec.Decode(DeliveryEventCodec.Encode(lease.Delivery)).Payload.Equals(lease.Delivery.Payload),
            "wallpaper v2 codec roundtrip");
        try { DeliveryEventCodec.Validate(lease.Delivery with { SchemaVersion = 1 }); throw new Exception("Expected v1 rejection"); }
        catch (InvalidDataException) { assertions++; }
        var generation = await delivery.GetActiveGenerationAsync("user:" + alice.UserId);
        // Existing general-event tests own this generation under this same consumer name.
        var scope = new ConsumerScope("backend-events", generation);
        Check(await delivery.StageAsync(scope, lease.Delivery, new("wallpapers", 0, 0), default) == InboxStageResult.Staged,
            "wallpaper durable inbox");
        Check(await delivery.StageAsync(scope, lease.Delivery, new("wallpapers", 0, 1), default) == InboxStageResult.Duplicate &&
            (long)(await Scalar("SELECT count(*) FROM botgram_intake_v1.projection_tasks WHERE outbox_id=$1", lease.Delivery.OutboxId))! == 0,
            "wallpaper duplicate generated fake projection");
        Check(await delivery.ConfirmPublishedAsync(lease.Delivery.OutboxId, lease.LeaseToken, new(new("wallpapers", 0, 1)), default),
            "wallpaper publication confirmation");
        await FrozenOutboxTests.RunAsync(connectionString);
        return assertions;
    }

    private sealed class PreparationFailure : Exception;
}
