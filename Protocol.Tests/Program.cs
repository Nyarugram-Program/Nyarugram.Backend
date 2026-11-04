using Botgram.Protocol.V1;
using Google.Protobuf;
using OldEnvelope = Botgram.Protocol.Compat.Old.Envelope;
using NewEnvelope = Botgram.Protocol.Compat.New.Envelope;
using FrozenRpc = Botgram.Protocol.Compat.PreWallpaper;
using WireMessage = Botgram.Protocol.V1.Message;

internal static class Program
{
    private const string SendHex = "0801120167da06100a02180111ffffffffffffffff1a0141";
    private static readonly string[] Operations = ["get_state", "get_difference", "get_dialogs", "get_pinned_dialogs", "get_peer_dialogs", "get_history", "get_messages", "send_message", "read_history", "search_contacts", "resolve_username", "get_users", "get_contacts", "resolve_gid", "import_contacts", "create_group", "get_peer_settings", "get_full_user", "get_full_group", "get_groups", "get_notify_settings"];
    private static readonly string[] Entities = ["bold", "italic", "pre", "text_url", "underline", "strike", "spoiler", "code", "url", "email", "phone", "mention", "hashtag", "cashtag", "bot_command"];
    private static void Check(bool value, string label)
    {
        if (!value) throw new InvalidOperationException(label);
    }
    private static UserRef Local() => new() { ServerDomain = "test.invalid", UserId = "local-user" };
    private static PeerRef Peer() => new() { User = Local() };
    private static WireMessage Text()
    {
        var body = new TextMessage { Text = "A😀Z", Silent = true, Reply = new ReplyRef { MessageId = 42 } };
        for (var i = 0; i < Entities.Length; i++)
        {
            var entity = new MessageEntity { OffsetUtf16 = 1, LengthUtf16 = 2 };
            var field = MessageEntity.Descriptor.FindFieldByName(Entities[i]);
            Check(field.FieldNumber == 10 + i, "entity frozen tag");
            IMessage kind = i switch
            {
                2 => new PreEntity { Language = "java" },
                3 => new TextUrlEntity { Url = "https://example.invalid/😀" },
                _ => new Empty()
            };
            field.Accessor.SetValue(entity, kind);
            body.Entities.Add(entity);
        }
        return new WireMessage { MessageId = 43, Peer = Peer(), Sender = Local(), CreatedAtMs = 1700000000123, Outgoing = true, Unread = true, Text = body };
    }
    private static WireMessage Service() => new()
    {
        MessageId = 44, Peer = new PeerRef { GroupId = "group" }, Sender = Local(),
        Service = new ServiceMessage { GroupCreated = new GroupCreatedAction { Title = "组😀", Members = { Local() } } }
    };
    private static Dictionary<string, IMessage> Fixtures()
    {
        var all = new Dictionary<string, IMessage>();
        for (var i = 0; i < Operations.Length; i++)
        {
            var request = new RpcRequest { ProtocolMajor = 1, RequestId = "tags" };
            var response = new RpcResponse { ProtocolMajor = 1, RequestId = "tags" };
            var a = RpcRequest.Descriptor.FindFieldByName(Operations[i]);
            var b = RpcResponse.Descriptor.FindFieldByName(Operations[i]);
            Check(a.FieldNumber == 100 + i && b.FieldNumber == 100 + i, "operation frozen tag " + Operations[i]);
            a.Accessor.SetValue(request, a.MessageType.Parser.ParseFrom(Array.Empty<byte>()));
            b.Accessor.SetValue(response, b.MessageType.Parser.ParseFrom(Array.Empty<byte>()));
            all.Add("request-tag-" + i, request);
            all.Add("response-tag-" + i, response);
        }
        all.Add("request-golden", new RpcRequest { ProtocolMajor = 1, RequestId = "g", SendMessage = new SendMessageRequest { Peer = new PeerRef { Self = true }, RandomId = -1, Text = "A" } });
        all.Add("request-rich", new RpcRequest { ProtocolMajor = 1, RequestId = "rich", SendMessage = new SendMessageRequest { Peer = Peer(), RandomId = long.MinValue, Text = Text().Text.Text, Entities = { Text().Text.Entities }, ReplyToMessageId = 0, Silent = true } });
        var updates = new Updates
        {
            DateMs = 1700000000123, Seq = 9,
            Updates_ =
            {
                new Update { NewMessage = new NewMessage { Message = Text(), Pts = 2, PtsCount = 1 } },
                new Update { MessageId = new MessageIdMapping { RandomId = long.MaxValue, MessageId = 43 } },
                new Update { ReadInbox = new ReadReceipt { Peer = Peer(), MaxMessageId = 43, Pts = 3, PtsCount = 1 } },
                new Update { ReadOutbox = new ReadReceipt { Peer = Peer(), MaxMessageId = 43, Pts = 4, PtsCount = 1 } },
                new Update { GidChanged = new GidChanged { User = Local(), Gid = "03012345678" } }
            }
        };
        for (var i = 0; i < updates.Updates_.Count; i++) Check((int)updates.Updates_[i].KindCase == i + 1, "update frozen tag");
        all.Add("response-rich", new RpcResponse { SendMessage = new MessageSent { Message = Text(), Mapping = new MessageIdMapping { RandomId = -1, MessageId = 43 }, Updates = updates } });
        for (var i = 1; i <= 4; i++)
        {
            var page = new DifferencePage { Messages = { Text(), Service() }, Updates = { updates.Updates_ }, State = new SyncState { Pts = 4 } };
            var difference = i switch
            {
                1 => new Difference { Empty = new DifferenceEmpty() },
                2 => new Difference { TooLong = new DifferenceTooLong() },
                3 => new Difference { Complete = page },
                4 => new Difference { Slice = page },
                _ => throw new InvalidOperationException()
            };
            Check((int)difference.KindCase == i, "difference frozen tag");
            all.Add("response-difference-" + i, new RpcResponse { GetDifference = difference });
        }
        all.Add("request-optional-absent", new RpcRequest { GetDialogs = new GetDialogsRequest() });
        all.Add("request-optional-zero", new RpcRequest { GetDialogs = new GetDialogsRequest { Limit = 0 } });
        all.Add("request-history-absent", new RpcRequest { GetHistory = new GetHistoryRequest() });
        all.Add("request-history-zero", new RpcRequest { GetHistory = new GetHistoryRequest { Limit = 0 } });
        all.Add("request-reply-absent", new RpcRequest { SendMessage = new SendMessageRequest() });
        all.Add("request-reply-zero", new RpcRequest { SendMessage = new SendMessageRequest { ReplyToMessageId = 0 } });
        all.Add("response-notify-absent", new RpcResponse { GetNotifySettings = new NotifySettings() });
        all.Add("response-notify-zero", new RpcResponse { GetNotifySettings = new NotifySettings { ShowPreviews = false, Silent = false, MuteUntilMs = 0 } });
        var premiumAbsent = new UserSummary { Identity = Local(), TlUserId = 100000 };
        var premiumFalse = premiumAbsent.Clone(); premiumFalse.NyarugramPremium = false;
        var premiumTrue = premiumAbsent.Clone(); premiumTrue.NyarugramPremium = true;
        all.Add("response-premium-absent", new RpcResponse { GetUsers = new UserList { Users = { premiumAbsent } } });
        all.Add("response-premium-false", new RpcResponse { GetUsers = new UserList { Users = { premiumFalse } } });
        all.Add("response-premium-true", new RpcResponse { GetUsers = new UserList { Users = { premiumTrue } } });
        all.Add("compat-premium-true", Botgram.Protocol.Compat.Old.UserSummary.Parser.ParseFrom(premiumTrue.ToByteArray()));
        all.Add("compat-premium-false", Botgram.Protocol.Compat.Old.UserSummary.Parser.ParseFrom(premiumFalse.ToByteArray()));
        all.Add("response-error-absent", new RpcResponse { Error = new RpcError { Code = "WAIT", HttpStatus = 429 } });
        all.Add("response-error-zero", new RpcResponse { Error = new RpcError { Code = "WAIT", HttpStatus = 429, RetryAfterMs = 0 } });
        all.Add("response-fixed64", new RpcResponse { ImportContacts = new ImportedContacts
        {
            Imported = { new ImportedContact { ClientId = long.MinValue, User = Local() } },
            PopularInvites = { new PopularInvite { ClientId = long.MaxValue, Importers = 1 } },
            RetryContacts = { long.MinValue, long.MaxValue, -7 }
        } });
        all.Add("request-fixed64", new RpcRequest { ImportContacts = new ImportContactsRequest { Contacts = { new ImportContact { ClientId = long.MinValue }, new ImportContact { ClientId = long.MaxValue }, new ImportContact { ClientId = -7 } } } });
        all.Add("request-signed64", new RpcRequest { GetHistory = new GetHistoryRequest { Peer = Peer(), OffsetMessageId = long.MinValue, OffsetDateMs = long.MaxValue, MinMessageId = -1, MaxMessageId = long.MaxValue, AddOffset = int.MinValue } });
        all.Add("response-signed64", new RpcResponse { GetState = new SyncState { DateMs = long.MinValue, Pts = int.MinValue, Qts = int.MaxValue } });
        all.Add("request-identities", new RpcRequest { GetUsers = new GetUsersRequest { Users = { new UserSelector { User = new UserRef() }, new UserSelector { User = Local() }, new UserSelector { User = new UserRef { ServerDomain = "remote.invalid", UserId = "foreign-user" } } } } });
        all.Add("compat-new", new NewEnvelope { Id = "compat", Status = NewEnvelope.Types.Status.Future, Extra = "extra😀" });
        all.Add("compat-old", new OldEnvelope { Id = "old", Status = OldEnvelope.Types.Status.Ready });
        foreach (var (name, tag) in new[] { ("get_wallpapers", 122), ("save_wallpaper", 123), ("install_wallpaper", 124), ("set_chat_wallpaper", 125) })
        {
            var request = new RpcRequest { ProtocolMajor = 1 };
            var response = new RpcResponse { ProtocolMajor = 1 };
            var a = RpcRequest.Descriptor.FindFieldByName(name);
            var b = RpcResponse.Descriptor.FindFieldByName(name);
            Check(a.FieldNumber == tag && b.FieldNumber == tag, "wallpaper tag " + name);
            a.Accessor.SetValue(request, a.MessageType.Parser.ParseFrom(Array.Empty<byte>()));
            b.Accessor.SetValue(response, b.MessageType.Parser.ParseFrom(Array.Empty<byte>()));
            all.Add("request-wallpaper-tag-" + tag, request);
            all.Add("response-wallpaper-tag-" + tag, response);
            all.Add("compat-frozen-request-" + tag, FrozenRpc.RpcRequest.Parser.ParseFrom(request.ToByteArray()));
            all.Add("compat-frozen-response-" + tag, FrozenRpc.RpcResponse.Parser.ParseFrom(response.ToByteArray()));
        }
        var wallpaper = new Wallpaper { Id = long.MinValue, NoFile = new() { Dark = true, Settings = new()
        {
            BackgroundColor = 0, SecondBackgroundColor = 0xffffff, ThirdBackgroundColor = 0, FourthBackgroundColor = 0xabcdef,
            Rotation = 0, Intensity = -100, Blur = true, Motion = true, Emoticon = ""
        } } };
        all.Add("request-wallpaper-rich", new RpcRequest { SetChatWallpaper = new() { Peer = Peer(), Wallpaper = wallpaper, ForBoth = true } });
        all.Add("request-wallpaper-revert", new RpcRequest { SetChatWallpaper = new() { Peer = Peer(), Revert = true } });
        all.Add("request-wallpaper-clear", new RpcRequest { SetChatWallpaper = new() { Peer = Peer(), Clear = new() } });
        all.Add("response-wallpaper-absent", new RpcResponse { GetFullUser = new() { User = Local() } });
        all.Add("response-wallpaper-cleared", new RpcResponse { GetFullUser = new() { User = Local(), Wallpaper = new() { Cleared = new(), Overridden = true } } });
        all.Add("response-wallpaper-rich", new RpcResponse { SetChatWallpaper = new() { Revision = long.MaxValue,
            State = new() { Wallpaper = wallpaper }, Updates = new() { Updates_ = {
                new Update { PeerWallpaperChanged = new() { Peer = Peer(), State = new() { Wallpaper = wallpaper } } }
            } } } });
        all.Add("hint-wallpaper-absent", new EventHint { ProtocolMajor = 1, Cursor = 5 });
        all.Add("hint-wallpaper-zero", new EventHint { ProtocolMajor = 1, Cursor = 5, WallpaperRevision = 0 });
        all.Add("compat-frozen-premium-absent", new FrozenRpc.UserSummary
        {
            Identity = new() { ServerDomain = "test.invalid", UserId = "local-user" }, TlUserId = 100000
        });
        all.Add("compat-frozen-premium-false", FrozenRpc.UserSummary.Parser.ParseFrom(premiumFalse.ToByteArray()));
        all.Add("compat-frozen-premium-true", FrozenRpc.UserSummary.Parser.ParseFrom(premiumTrue.ToByteArray()));
        all.Add("compat-frozen-full-old", new FrozenRpc.RpcResponse
        {
            GetFullUser = new() { User = new() { ServerDomain = "test.invalid", UserId = "local-user" } }
        });
        all.Add("compat-frozen-full-clear", FrozenRpc.RpcResponse.Parser.ParseFrom(all["response-wallpaper-cleared"].ToByteArray()));
        all.Add("compat-frozen-group-present", FrozenRpc.RpcResponse.Parser.ParseFrom(new RpcResponse
        {
            GetFullGroup = new() { Wallpaper = new() { Wallpaper = wallpaper, Overridden = true } }
        }.ToByteArray()));
        all.Add("compat-frozen-hint-old", new FrozenRpc.EventHint { ProtocolMajor = 1, Cursor = 5 });
        all.Add("compat-frozen-hint-zero", FrozenRpc.EventHint.Parser.ParseFrom(all["hint-wallpaper-zero"].ToByteArray()));
        var wallpaperUpdate = ((RpcResponse)all["response-wallpaper-rich"]).SetChatWallpaper.Updates.Updates_.Single();
        all.Add("compat-frozen-update", FrozenRpc.Update.Parser.ParseFrom(wallpaperUpdate.ToByteArray()));
        return all;
    }
    private static void WireChecks(Dictionary<string, IMessage> samples)
    {
        Check(samples["request-golden"].ToByteArray().SequenceEqual(Convert.FromHexString(SendHex)), "manual golden");
        Check("A😀Z".Length == 4 && "A😀Z".Substring(1, 2) == "😀", "UTF16 span");
        var unknown = Convert.FromHexString("ca0c03010203");
        var request = RpcRequest.Parser.ParseFrom(unknown);
        var response = RpcResponse.Parser.ParseFrom(unknown);
        Check(request.OperationCase == RpcRequest.OperationOneofCase.None && request.ToByteArray().SequenceEqual(unknown), "unknown operation retained, NOT_SET");
        Check(response.OutcomeCase == RpcResponse.OutcomeOneofCase.None && response.ToByteArray().SequenceEqual(unknown), "unknown outcome retained, NOT_SET");
        Check(RpcRequest.Parser.ParseFrom(Convert.FromHexString("a20600da0600")).OperationCase == RpcRequest.OperationOneofCase.SendMessage, "request oneof last wins");
        Check(RpcResponse.Parser.ParseFrom(Convert.FromHexString("a20600da0600")).OutcomeCase == RpcResponse.OutcomeOneofCase.SendMessage, "response oneof last wins");
        foreach (var bad in new[] { "da0604", "da06031a0241", "80" })
        {
            foreach (var parser in new MessageParser[] { RpcRequest.Parser, RpcResponse.Parser })
            {
                var rejected = false;
                try { parser.ParseFrom(Convert.FromHexString(bad)); }
                catch (InvalidProtocolBufferException) { rejected = true; }
                Check(rejected, "malformed/truncated rejected " + bad);
            }
        }
        var old = OldEnvelope.Parser.ParseFrom(samples["compat-new"].ToByteArray());
        Check((int)old.Status == 7, "unknown enum retained");
        var back = NewEnvelope.Parser.ParseFrom(old.ToByteArray());
        Check(back.Extra == "extra😀" && (int)back.Status == 7, "new-old-new fields and enum retained");
        Check(NewEnvelope.Parser.ParseFrom(samples["compat-old"].ToByteArray()).Extra == "", "old-new default");
        Check(!((RpcRequest)samples["request-optional-absent"]).GetDialogs.HasLimit && ((RpcRequest)samples["request-optional-zero"]).GetDialogs.HasLimit, "optional limit presence");
        Check(!((RpcRequest)samples["request-history-absent"]).GetHistory.HasLimit && ((RpcRequest)samples["request-history-zero"]).GetHistory.HasLimit, "optional history presence");
        Check(!((RpcRequest)samples["request-reply-absent"]).SendMessage.HasReplyToMessageId && ((RpcRequest)samples["request-reply-zero"]).SendMessage.HasReplyToMessageId, "optional reply presence");
        Check(!((RpcResponse)samples["response-notify-absent"]).GetNotifySettings.HasSilent && ((RpcResponse)samples["response-notify-zero"]).GetNotifySettings.HasSilent, "optional false presence");
        var notifyAbsent = ((RpcResponse)samples["response-notify-absent"]).GetNotifySettings;
        var notifyZero = ((RpcResponse)samples["response-notify-zero"]).GetNotifySettings;
        Check(!notifyAbsent.HasShowPreviews && !notifyAbsent.HasMuteUntilMs && notifyZero.HasShowPreviews && notifyZero.HasMuteUntilMs, "all notify optional presence");
        Check(!((RpcResponse)samples["response-error-absent"]).Error.HasRetryAfterMs && ((RpcResponse)samples["response-error-zero"]).Error.HasRetryAfterMs, "optional retry presence");
        var premiumAbsent = ((RpcResponse)samples["response-premium-absent"]).GetUsers.Users.Single();
        var premiumFalse = ((RpcResponse)samples["response-premium-false"]).GetUsers.Users.Single();
        var premiumTrue = ((RpcResponse)samples["response-premium-true"]).GetUsers.Users.Single();
        Check(!premiumAbsent.HasNyarugramPremium && !premiumAbsent.NyarugramPremium &&
            premiumFalse.HasNyarugramPremium && !premiumFalse.NyarugramPremium &&
            premiumTrue.HasNyarugramPremium && premiumTrue.NyarugramPremium, "Premium absent/false/true presence");
        foreach (var active in new[] { false, true })
        {
            var oldUser = samples["compat-premium-" + active.ToString().ToLowerInvariant()];
            var restored = UserSummary.Parser.ParseFrom(oldUser.ToByteArray());
            Check(restored.Identity.Equals(Local()) && restored.TlUserId == 100000 &&
                restored.HasNyarugramPremium && restored.NyarugramPremium == active, "old user schema preserves optional Premium");
        }
        var richWallpaper = ((RpcRequest)samples["request-wallpaper-rich"]).SetChatWallpaper.Wallpaper;
        var colors = richWallpaper.NoFile.Settings;
        Check(richWallpaper.Id == long.MinValue && colors.HasBackgroundColor && colors.BackgroundColor == 0 &&
            colors.HasThirdBackgroundColor && colors.ThirdBackgroundColor == 0 && colors.HasRotation && colors.Rotation == 0 &&
            colors.HasEmoticon && colors.Emoticon == "", "wallpaper signed ID and zero/empty presence");
        Check(((RpcResponse)samples["response-wallpaper-absent"]).GetFullUser.Wallpaper is null &&
            ((RpcResponse)samples["response-wallpaper-cleared"]).GetFullUser.Wallpaper.ValueCase == WallpaperState.ValueOneofCase.Cleared,
            "unknown full state is not explicit clear");
        Check(!((EventHint)samples["hint-wallpaper-absent"]).HasWallpaperRevision &&
            ((EventHint)samples["hint-wallpaper-zero"]).HasWallpaperRevision, "wallpaper revision zero presence");
        Check(((RpcResponse)samples["response-wallpaper-rich"]).SetChatWallpaper.Updates.Updates_.Single().KindCase ==
            Update.KindOneofCase.PeerWallpaperChanged, "wallpaper response update kind");
        foreach (var tag in Enumerable.Range(122, 4))
        {
            var oldRequest = (FrozenRpc.RpcRequest)samples["compat-frozen-request-" + tag];
            var oldResponse = (FrozenRpc.RpcResponse)samples["compat-frozen-response-" + tag];
            Check(oldRequest.OperationCase == FrozenRpc.RpcRequest.OperationOneofCase.None &&
                (int)RpcRequest.Parser.ParseFrom(oldRequest.ToByteArray()).OperationCase == tag,
                "frozen request unknown operation retained " + tag);
            Check(oldResponse.OutcomeCase == FrozenRpc.RpcResponse.OutcomeOneofCase.None &&
                (int)RpcResponse.Parser.ParseFrom(oldResponse.ToByteArray()).OutcomeCase == tag,
                "frozen response unknown outcome retained " + tag);
        }
        var oldPremium = UserSummary.Parser.ParseFrom(samples["compat-frozen-premium-absent"].ToByteArray());
        Check(!oldPremium.HasNyarugramPremium && !oldPremium.NyarugramPremium, "frozen Premium absence");
        foreach (var active in new[] { false, true })
        {
            var restored = UserSummary.Parser.ParseFrom(samples["compat-frozen-premium-" + active.ToString().ToLowerInvariant()].ToByteArray());
            Check(restored.HasNyarugramPremium && restored.NyarugramPremium == active, "frozen Premium presence retention");
        }
        Check(RpcResponse.Parser.ParseFrom(samples["compat-frozen-full-old"].ToByteArray()).GetFullUser.Wallpaper is null,
            "frozen full response leaves wallpaper unknown");
        var oldFull = (FrozenRpc.RpcResponse)samples["compat-frozen-full-clear"];
        Check(oldFull.OutcomeCase == FrozenRpc.RpcResponse.OutcomeOneofCase.GetFullUser &&
            RpcResponse.Parser.ParseFrom(oldFull.ToByteArray()).GetFullUser.Wallpaper.ValueCase == WallpaperState.ValueOneofCase.Cleared,
            "frozen full response retains explicit clear");
        var oldGroup = (FrozenRpc.RpcResponse)samples["compat-frozen-group-present"];
        var restoredGroup = RpcResponse.Parser.ParseFrom(oldGroup.ToByteArray()).GetFullGroup.Wallpaper;
        Check(oldGroup.OutcomeCase == FrozenRpc.RpcResponse.OutcomeOneofCase.GetFullGroup &&
            restoredGroup.ValueCase == WallpaperState.ValueOneofCase.Wallpaper && restoredGroup.Overridden,
            "frozen group response retains wallpaper");
        Check(!EventHint.Parser.ParseFrom(samples["compat-frozen-hint-old"].ToByteArray()).HasWallpaperRevision &&
            EventHint.Parser.ParseFrom(samples["compat-frozen-hint-zero"].ToByteArray()).HasWallpaperRevision &&
            EventHint.Parser.ParseFrom(samples["compat-frozen-hint-zero"].ToByteArray()).WallpaperRevision == 0,
            "frozen hint distinguishes missing revision and zero");
        var oldUpdate = (FrozenRpc.Update)samples["compat-frozen-update"];
        Check(oldUpdate.KindCase == FrozenRpc.Update.KindOneofCase.None &&
            Update.Parser.ParseFrom(oldUpdate.ToByteArray()).KindCase == Update.KindOneofCase.PeerWallpaperChanged,
            "frozen client cannot interpret new update kind");
    }
    public static void Main(string[] args)
    {
        if (args.Length != 2) throw new ArgumentException("java-fixtures csharp-fixtures");
        var samples = Fixtures();
        WireChecks(samples);
        Directory.CreateDirectory(args[1]);
        Check(Directory.GetFiles(args[0], "*.bin").Length == samples.Count + 3, "Java fixture coverage");
        var throughJavaOld = NewEnvelope.Parser.ParseFrom(File.ReadAllBytes(Path.Combine(args[0], "compat-through-old.bin")));
        Check(throughJavaOld.Extra == "extra😀" && (int)throughJavaOld.Status == 7, "Java old-schema → C# new-schema retention");
        foreach (var (name, expected) in samples)
        {
            var data = File.ReadAllBytes(Path.Combine(args[0], name + ".bin"));
            var parsed = expected.Descriptor.Parser.ParseFrom(data);
            Check(expected.Equals(parsed), "Java → C# " + name);
            File.WriteAllBytes(Path.Combine(args[1], name + ".bin"), expected.ToByteArray());
        }
        foreach (var name in new[] { "request-unknown", "response-unknown" })
        {
            var data = File.ReadAllBytes(Path.Combine(args[0], name + ".bin"));
            IMessage parsed = name.StartsWith("request", StringComparison.Ordinal) ? RpcRequest.Parser.ParseFrom(data) : RpcResponse.Parser.ParseFrom(data);
            Check(parsed.ToByteArray().SequenceEqual(Convert.FromHexString("0801ca0c03010203")), "cross-runtime unknown retained");
            File.WriteAllBytes(Path.Combine(args[1], name + ".bin"), parsed.ToByteArray());
        }
        var throughOld = OldEnvelope.Parser.ParseFrom(File.ReadAllBytes(Path.Combine(args[0], "compat-new.bin")));
        File.WriteAllBytes(Path.Combine(args[1], "compat-through-old.bin"), throughOld.ToByteArray());
        Console.WriteLine($"C# protobuf OK: {samples.Count} fixtures, Java → C# verified; C# → Java fixtures written, binary only");
    }
}
