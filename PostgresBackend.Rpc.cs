using P = Botgram.Protocol.V1;

namespace Botgram.Backend.Storage;

public sealed partial class PostgresBackend
{
    public Task<T> DispatchAsync<T>(Session session, P.RpcRequest request, string domain,
        Func<P.RpcResponse, T> prepareResponse, CancellationToken token = default) =>
        WriteAsync(session, async work =>
            prepareResponse(await ExecuteRpc(work, session, request, domain)), token);

    private async Task<P.RpcResponse> ExecuteRpc(Transaction work, Session session, P.RpcRequest request, string domain)
    {
        var uid = session.UserId;
        var response = new P.RpcResponse { ProtocolMajor = 1, RequestId = request.RequestId };
        P.UserRef Reference(string id) => new() { ServerDomain = domain, UserId = id };
        P.UserSummary Map(UserProfile user) => ProtobufRpcMapper.MapUser(user, domain);
        P.ResolvedPeer Resolved(UserProfile user)
        {
            var result = new P.ResolvedPeer { Peer = new() { User = Reference(user.Id) } };
            result.Users.Add(Map(user));
            return result;
        }
        switch (request.OperationCase)
        {
            case P.RpcRequest.OperationOneofCase.AccountPrivacy:
                response.AccountPrivacy = await AccountPrivacy(work, session, request.AccountPrivacy, domain);
                break;
            case P.RpcRequest.OperationOneofCase.GetWallpapers:
                response.GetWallpapers = await Wallpapers(work, uid, request.GetWallpapers);
                break;
            case P.RpcRequest.OperationOneofCase.SaveWallpaper:
                response.SaveWallpaper = await SaveWallpaper(work, uid, request.SaveWallpaper);
                break;
            case P.RpcRequest.OperationOneofCase.InstallWallpaper:
                response.InstallWallpaper = await InstallWallpaper(work, uid, request.InstallWallpaper);
                break;
            case P.RpcRequest.OperationOneofCase.SetChatWallpaper:
                response.SetChatWallpaper = await SetWallpaper(work, uid, domain, request.SetChatWallpaper);
                break;
            case P.RpcRequest.OperationOneofCase.SendMessage:
            {
                var input = request.SendMessage;
                var command = ProtobufRpcMapper.RelationalSendCommand(input);
                var conversation = await ResolveConversation(work, uid, input.Peer, domain, true);
                var accepted = await new PostgresMessageAcceptance(source, maximumBacklog).AcceptInTransactionAsync(
                    new(session.TokenHash, uid, conversation.Id, command, MessageWriteContract.Telegram),
                    work.Connection, work.NativeTransaction, work.RetainThroughCommit, work.Token);
                var mapping = new P.MessageIdMapping { RandomId = input.RandomId,
                    MessageId = ProtobufRpcMapper.ValidateMessageId(accepted.Message.Id, true) };
                var updates = await MessageUpdates(work, uid, domain, accepted);
                updates.Updates_.Insert(0, new P.Update { MessageId = mapping });
                response.SendMessage = new() { Message = updates.Updates_[1].NewMessage.Message,
                    Mapping = mapping, Updates = updates };
                break;
            }
            case P.RpcRequest.OperationOneofCase.CreateGroup:
            {
                var input = request.CreateGroup;
                ProtobufRpcMapper.ValidateCount(input.Users.Count);
                var ids = input.Users.Select(s => ProtobufRpcMapper.SelectedUserId(s, uid, domain)).ToArray();
                ProtobufRpcMapper.ValidateUnique(ids);
                var found = (long)(await work.Scalar("SELECT count(*) FROM botgram_backend_v1.accounts WHERE id=ANY($1)",
                    (object)ids))!;
                if (found != ids.Length) throw new ApiException(404, "USER_ID_INVALID");
                var members = ids.Append(uid).Distinct().Order().ToArray();
                if (members.Length is < 2 or > 100 || string.IsNullOrWhiteSpace(input.Title) || input.Title.Length > 100)
                    throw new ApiException(400, "CHAT_TITLE_EMPTY");
                await CheckInvites(work, uid, "group", members);
                var conversation = await CreateConversation(work, new("group", input.Title, members));
                var accepted = await new PostgresMessageAcceptance(source, maximumBacklog).AddGroupServiceAsync(
                    session, conversation, work.Connection, work.NativeTransaction, work.RetainThroughCommit, work.Token);
                var mapped = ProtobufRpcMapper.MapGroup(conversation, (await GroupIdentities(work, [conversation]))[conversation.Id]);
                response.CreateGroup = new() { Group = mapped,
                    Updates = await MessageUpdates(work, uid, domain, accepted, [conversation]) };
                break;
            }
            case P.RpcRequest.OperationOneofCase.GetDialogs:
            {
                var input = request.GetDialogs;
                if (input.FolderId < 0) throw new ApiException(400, "BOTGRAM_RPC_INVALID_ARGUMENT");
                var offset = ProtobufRpcMapper.ValidateMessageId(input.OffsetMessageId);
                var limit = ProtobufRpcMapper.ValidateLimit(input.HasLimit, input.Limit, 100);
                var pts = (int)(await work.Scalar("SELECT pts FROM botgram_intake_v1.sync WHERE user_id=$1", uid))!;
                await GroupIdentities(work, await Owned(work, uid));
                var offsetPeer = input.OffsetPeer is null ? 0 : await PeerIdentity(work, uid, input.OffsetPeer, domain);
                var page = await Dialogs(work, uid, new(DialogSelection.All, input.FolderId, offset, offsetPeer, limit));
                var mapped = await MappedDialogs(work, uid, domain, page, pts);
                response.GetDialogs = new() { TotalCount = page.TotalCount, IsSlice = page.IsSlice };
                response.GetDialogs.Dialogs.Add(mapped.Dialogs);
                response.GetDialogs.Messages.Add(mapped.Messages);
                response.GetDialogs.Users.Add(mapped.Users);
                response.GetDialogs.Groups.Add(mapped.Groups);
                break;
            }
            case P.RpcRequest.OperationOneofCase.GetPinnedDialogs:
            {
                var pts = (int)(await work.Scalar("SELECT pts FROM botgram_intake_v1.sync WHERE user_id=$1", uid))!;
                response.GetPinnedDialogs = await MappedDialogs(work, uid, domain,
                    await Dialogs(work, uid, new(DialogSelection.Pinned)), pts);
                break;
            }
            case P.RpcRequest.OperationOneofCase.GetPeerDialogs:
            {
                var input = request.GetPeerDialogs;
                ProtobufRpcMapper.ValidateCount(input.Peers.Count);
                if (input.FolderId < 0) throw new ApiException(400, "BOTGRAM_RPC_INVALID_ARGUMENT");
                // Read the cursor before dialog rows; commits racing the page remain replayable.
                var pts = (int)(await work.Scalar("SELECT pts FROM botgram_intake_v1.sync WHERE user_id=$1", uid))!;
                // Acquire all group mappings before user mappings, independent of request order.
                await GroupIdentities(work, await Owned(work, uid));
                var peers = new List<long>();
                foreach (var peer in input.Peers) peers.Add(await PeerIdentity(work, uid, peer, domain));
                ProtobufRpcMapper.ValidateUnique(peers);
                response.GetPeerDialogs = await MappedDialogs(work, uid, domain,
                    await Dialogs(work, uid, new(DialogSelection.Peers, input.FolderId, Peers: peers.ToArray())), pts);
                break;
            }
            case P.RpcRequest.OperationOneofCase.GetDifference:
                response.GetDifference = await MappedDifference(work, uid, domain, request.GetDifference.Pts);
                break;
            case P.RpcRequest.OperationOneofCase.GetHistory:
            {
                var input = request.GetHistory;
                var query = ProtobufRpcMapper.RelationalHistoryQuery(input);
                var conversation = await ResolveConversation(work, uid, input.Peer, domain, true);
                var messages = await HistoryMessages(work, session, conversation.Id,
                    query.OffsetId, query.OffsetDate, query.AddOffset, query.MinId, query.MaxId, query.Limit);
                response.GetHistory = await MessagePage(work, uid, domain, messages, conversation);
                break;
            }
            case P.RpcRequest.OperationOneofCase.GetMessages:
            {
                var input = request.GetMessages.MessageIds;
                ProtobufRpcMapper.ValidateCount(input.Count);
                var ids = input.Select(id => ProtobufRpcMapper.ValidateMessageId(id, true)).ToArray();
                ProtobufRpcMapper.ValidateUnique(ids);
                var messages = new List<Message>();
                if (reads is null) messages = await work.Rows("""
                    SELECT m.body::text FROM botgram_intake_v1.messages m
                    JOIN botgram_intake_v1.members own ON own.conversation_id=m.conversation_id AND own.user_id=$1
                    WHERE m.id=ANY($2) ORDER BY m.id
                    """, r => StoredMessage(r.GetString(0)), uid, ids);
                else
                {
                    var metadata = await work.Rows("""
                        SELECT m.id,m.conversation_id,m.conversation_sequence FROM botgram_intake_v1.messages m
                        JOIN botgram_intake_v1.members own ON own.conversation_id=m.conversation_id AND own.user_id=$1
                        WHERE m.id=ANY($2) ORDER BY m.id
                        """, r => (Id: r.GetInt64(0), Conversation: r.GetString(1), Sequence: r.GetInt64(2)), uid, ids);
                    foreach (var item in metadata)
                        messages.Add(await ReadMessage(work, session, item.Conversation, item.Id, item.Sequence));
                }
                response.GetMessages = await MessagePage(work, uid, domain, messages.ToArray());
                break;
            }
            case P.RpcRequest.OperationOneofCase.ReadHistory:
            {
                var input = request.ReadHistory;
                var max = ProtobufRpcMapper.ValidateMessageId(input.MaxMessageId);
                var conversation = await ResolveConversation(work, uid, input.Peer, domain);
                var read = await ReadThrough(work, uid, conversation, max);
                response.ReadHistory = new()
                {
                    Pts = (int)(await work.Scalar("SELECT pts FROM botgram_intake_v1.sync WHERE user_id=$1", uid))!,
                    PtsCount = read.Changed ? 1 : 0
                };
                break;
            }
            case P.RpcRequest.OperationOneofCase.GetState:
                response.GetState = new()
                {
                    Pts = (int)(await work.Scalar("SELECT pts FROM botgram_intake_v1.sync WHERE user_id=$1", uid))!,
                    DateMs = DateTimeOffset.UtcNow.ToUnixTimeSeconds() * 1000
                };
                break;
            case P.RpcRequest.OperationOneofCase.GetUsers:
            {
                ProtobufRpcMapper.ValidateCount(request.GetUsers.Users.Count);
                var ids = request.GetUsers.Users.Select(s => ProtobufRpcMapper.SelectedUserId(s, uid, domain)).ToArray();
                ProtobufRpcMapper.ValidateUnique(ids);
                response.GetUsers = new();
                response.GetUsers.Users.Add((await SelectedProfiles(work, uid, ids)).Select(Map));
                break;
            }
            case P.RpcRequest.OperationOneofCase.SearchContacts:
            {
                var users = await Profiles(work, uid, "lower(a.username)=lower($2)", request.SearchContacts.Query.TrimStart('@'));
                response.SearchContacts = new();
                response.SearchContacts.Users.Add(users.Select(Map));
                response.SearchContacts.Results.Add(users.Select(u => new P.PeerRef { User = Reference(u.Id) }));
                break;
            }
            case P.RpcRequest.OperationOneofCase.ResolveUsername:
                response.ResolveUsername = Resolved(await ResolveProfile(work, uid, "lower(a.username)=lower($2)",
                    request.ResolveUsername.Username, "USERNAME_NOT_OCCUPIED"));
                break;
            case P.RpcRequest.OperationOneofCase.ResolveGid:
                response.ResolveGid = Resolved(await ResolveProfile(work, uid, "a.gid=$2",
                    ContactService.NormalizeGid(request.ResolveGid.Gid), "PHONE_NOT_OCCUPIED"));
                break;
            case P.RpcRequest.OperationOneofCase.GetContacts:
            {
                var contacts = await Contacts(work, uid);
                response.GetContacts = new() { SavedCount = contacts.Length };
                response.GetContacts.Contacts.Add(contacts.Select(c => new P.Contact { User = Reference(c.User.Id), Mutual = c.Mutual }));
                response.GetContacts.Users.Add(contacts.Select(c => Map(c.User)));
                break;
            }
            case P.RpcRequest.OperationOneofCase.GetFullUser:
            {
                var id = ProtobufRpcMapper.SelectedUserId(request.GetFullUser.User, uid, domain);
                var users = await Profiles(work, uid, "a.id=$2", id);
                if (users.Length == 0) throw new ApiException(404, "USER_ID_INVALID");
                response.GetFullUser = new() { User = Reference(id), Settings = new(), NotifySettings = new() };
                response.GetFullUser.Wallpaper = await ChatWallpaper(work, uid, (await UserWallpaperConversation(work, uid, id))?.Id);
                response.GetFullUser.Users.Add(Map(users[0]));
                break;
            }
            case P.RpcRequest.OperationOneofCase.ImportContacts:
            {
                var input = request.ImportContacts.Contacts;
                ProtobufRpcMapper.ValidateCount(input.Count);
                ProtobufRpcMapper.ValidateUnique(input.Select(c => c.ClientId));
                var commands = input.Select(c => new ImportContactCommand(c.ClientId, ContactService.NormalizeGid(c.Gid),
                    c.FirstName, c.LastName, false)).ToArray();
                ProtobufRpcMapper.ValidateUnique(commands.Select(c => c.Gid));
                var imported = await ImportContacts(work, uid, commands);
                response.ImportContacts = new();
                response.ImportContacts.Imported.Add(imported.Imported.Select(c =>
                    new P.ImportedContact { User = Reference(c.UserId), ClientId = c.ClientId }));
                response.ImportContacts.Users.Add(imported.Users.Select(Map));
                break;
            }
            case P.RpcRequest.OperationOneofCase.GetPeerSettings:
                response.GetPeerSettings = await PeerSettings(work, uid, request.GetPeerSettings.Peer, domain);
                break;
            case P.RpcRequest.OperationOneofCase.GetNotifySettings:
            {
                var target = request.GetNotifySettings.Target;
                if (target is null) throw new ApiException(400, "BOTGRAM_RPC_INVALID_ARGUMENT");
                switch (target.TargetCase)
                {
                    case P.NotifyTarget.TargetOneofCase.Peer:
                        await PeerSettings(work, uid, target.Peer, domain);
                        break;
                    case P.NotifyTarget.TargetOneofCase.AllUsers:
                    case P.NotifyTarget.TargetOneofCase.AllGroups:
                    case P.NotifyTarget.TargetOneofCase.AllBroadcasts: break;
                    default: throw new ApiException(400, "BOTGRAM_RPC_INVALID_ARGUMENT");
                }
                response.GetNotifySettings = new();
                break;
            }
            case P.RpcRequest.OperationOneofCase.GetGroups:
            {
                var ids = request.GetGroups.GroupIds;
                ProtobufRpcMapper.ValidateCount(ids.Count);
                ProtobufRpcMapper.ValidateUnique(ids);
                foreach (var id in ids) await FindGroup(work, uid, id);
                var groups = (await Owned(work, uid)).Where(c => c.Kind == "group").ToArray();
                var mappings = await GroupIdentities(work, groups);
                response.GetGroups = new();
                response.GetGroups.Groups.Add(groups.Where(g => ids.Contains(g.Id))
                    .Select(g => ProtobufRpcMapper.MapGroup(g, mappings[g.Id])));
                break;
            }
            case P.RpcRequest.OperationOneofCase.GetFullGroup:
            {
                var group = await FindGroup(work, uid, request.GetFullGroup.GroupId);
                var mappings = await GroupIdentities(work, [group]);
                var mapped = ProtobufRpcMapper.MapGroup(group, mappings[group.Id]);
                response.GetFullGroup = new() { Group = mapped, ParticipantsVersion = 1, NotifySettings = new() };
                response.GetFullGroup.Wallpaper = await ChatWallpaper(work, uid, group.Id);
                response.GetFullGroup.Participants.Add(group.Members.Select(id => new P.Participant
                {
                    User = Reference(id), Inviter = Reference(group.Members[0]),
                    JoinedAtMs = group.CreatedAt.ToUnixTimeMilliseconds()
                }));
                response.GetFullGroup.Users.Add((await Related(work, uid, true)).Select(Map));
                response.GetFullGroup.Groups.Add(mapped);
                break;
            }
            case P.RpcRequest.OperationOneofCase.ResolveIdentities:
            {
                var input = request.ResolveIdentities;
                ProtobufRpcMapper.ValidateCount(input.TlUserIds.Count + input.TlChatIds.Count);
                ProtobufRpcMapper.ValidateUnique(input.TlUserIds);
                ProtobufRpcMapper.ValidateUnique(input.TlChatIds);
                if (input.TlUserIds.Concat(input.TlChatIds).Any(id => id <= 0))
                    throw new ApiException(400, "BOTGRAM_RPC_INVALID_ARGUMENT");
                if (await work.Scalar("SELECT value FROM botgram_backend_v1.identities WHERE key=$1", "user:" + uid) is not long)
                    throw new ApiException(404, "SELF_IDENTITY_NOT_FOUND");
                var users = await work.Rows("""
                    SELECT i.value,a.id FROM botgram_backend_v1.identities i
                    JOIN botgram_backend_v1.accounts a ON i.key='user:'||a.id WHERE i.value=ANY($1)
                    """, r => (Value: r.GetInt64(0), Id: r.GetString(1)), (object)input.TlUserIds.ToArray());
                if (users.Count != input.TlUserIds.Count) throw new ApiException(404, "USER_ID_INVALID");
                var groups = await work.Rows("""
                    SELECT i.value,c.id FROM botgram_backend_v1.identities i
                    JOIN botgram_backend_v1.conversations c ON i.key='chat:'||c.id
                    JOIN botgram_intake_v1.members m ON m.conversation_id=c.id AND m.user_id=$2
                    WHERE c.kind='group' AND i.value=ANY($1)
                    """, r => (Value: r.GetInt64(0), Id: r.GetString(1)), input.TlChatIds.ToArray(), uid);
                if (groups.Count != input.TlChatIds.Count) throw new ApiException(404, "PEER_ID_INVALID");
                var profiles = (await Profiles(work, uid, "a.id=ANY($2)",
                    (object)users.Select(u => u.Id).Append(uid).Distinct().ToArray())).ToDictionary(u => u.Id);
                response.ResolveIdentities = new() { ServerDomain = domain };
                response.ResolveIdentities.Users.Add(Map(profiles[uid]));
                foreach (var id in input.TlUserIds)
                {
                    var user = users.Single(u => u.Value == id);
                    if (user.Id != uid) response.ResolveIdentities.Users.Add(Map(profiles[user.Id]));
                }
                foreach (var id in input.TlChatIds)
                    response.ResolveIdentities.Groups.Add(ProtobufRpcMapper.MapGroup(
                        await FindGroup(work, uid, groups.Single(g => g.Value == id).Id), id));
                break;
            }
            default:
                throw new ApiException(501, "BOTGRAM_RPC_UNSUPPORTED:" + request.OperationCase);
        }
        return response;
    }

    private static async Task<P.PeerSettingsResult> PeerSettings(Transaction work, string uid, P.PeerRef? peer, string domain)
    {
        if (peer is null) throw new ApiException(400, "BOTGRAM_RPC_INVALID_ARGUMENT");
        var result = new P.PeerSettingsResult { Settings = new() };
        if (peer.PeerCase == P.PeerRef.PeerOneofCase.GroupId)
        {
            var group = await FindGroup(work, uid, peer.GroupId);
            var ids = await GroupIdentities(work, [group]);
            result.Groups.Add(ProtobufRpcMapper.MapGroup(group, ids[group.Id]));
            result.Users.Add((await Profiles(work, uid, "a.id=ANY($2)", (object)group.Members))
                .Select(user => ProtobufRpcMapper.MapUser(user, domain)));
        }
        else
        {
            var id = peer.PeerCase switch
            {
                P.PeerRef.PeerOneofCase.User => ProtobufRpcMapper.UserReferenceId(peer.User, domain),
                P.PeerRef.PeerOneofCase.Self when peer.Self => uid,
                _ => throw new ApiException(400, "BOTGRAM_RPC_INVALID_ARGUMENT")
            };
            var users = await Profiles(work, uid, "a.id=$2", id);
            if (users.Length == 0) throw new ApiException(404, "USER_ID_INVALID");
            result.Users.Add(ProtobufRpcMapper.MapUser(users[0], domain));
        }
        return result;
    }
}
