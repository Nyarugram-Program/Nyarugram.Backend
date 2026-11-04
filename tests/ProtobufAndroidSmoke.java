import com.google.protobuf.CodedOutputStream;
import com.sun.net.httpserver.HttpServer;
import java.io.ByteArrayOutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.telegram.botgram.BotgramApi;
import org.telegram.botgram.BotgramProtobufIdentity;
import org.telegram.botgram.BotgramProtobufRequestMapper;
import org.telegram.botgram.BotgramProtobufResponseMapper;
import org.telegram.botgram.protocol.v1.BotgramRpc;
import org.telegram.tgnet.SerializedData;
import org.telegram.tgnet.TLObject;
import org.telegram.tgnet.TLRPC;
import org.telegram.tgnet.tl.TL_account;
import org.telegram.tgnet.tl.TL_update;

/** Host-only generated protobuf -> production mapper -> original Android TL wire checks. */
public final class ProtobufAndroidSmoke {
    private static final String UUID = "11111111111111111111111111111111";
    private static final String GROUP = "22222222222222222222222222222222";
    private static final long USER_ID = 9007199254740993L;
    private static final long CHAT_ID = 9007199254740995L;
    private static final long RANDOM_ID = Long.MIN_VALUE + 123;
    private static final long NOW = 1700000000123L;
    private static final BotgramRpc.Empty EMPTY = BotgramRpc.Empty.getDefaultInstance();
    private static final BotgramRpc.UserRef USER = BotgramRpc.UserRef.newBuilder().setServerDomain("host.invalid").setUserId(UUID).build();
    private static final BotgramRpc.UserSummary SUMMARY = BotgramRpc.UserSummary.newBuilder().setIdentity(USER).setTlUserId(USER_ID)
            .setFirstName("Host").setLastName("Test").setUsername("host").setGid("03012345678").setIsSelf(true).setIsContact(true).build();
    private static final BotgramRpc.GroupSummary GROUP_SUMMARY = BotgramRpc.GroupSummary.newBuilder().setGroupId(GROUP).setTlChatId(CHAT_ID)
            .setTitle("Group").setParticipantCount(1).setCreatedAtMs(NOW).setVersion(1).build();
    private static final BotgramRpc.PeerRef SELF = BotgramRpc.PeerRef.newBuilder().setSelf(true).build();
    private static final BotgramRpc.NotifySettings NOTIFY = BotgramRpc.NotifySettings.newBuilder().setShowPreviews(false).setSilent(true).setMuteUntilMs(NOW).build();
    private static final BotgramRpc.SyncState STATE = BotgramRpc.SyncState.newBuilder().setPts(7).setQts(2).setDateMs(NOW).setSeq(3).build();
    private static final Set<BotgramRpc.RpcResponse.OutcomeCase> outcomes = new HashSet<>();
    private static int checks;
    private final BotgramProtobufResponseMapper responses;
    private final BotgramProtobufRequestMapper requests;

    private ProtobufAndroidSmoke(BotgramProtobufIdentity identity) {
        responses = new BotgramProtobufResponseMapper(identity);
        requests = new BotgramProtobufRequestMapper(identity);
    }

    private static void check(boolean condition, String label) {
        if (!condition) throw new AssertionError(label);
        checks++;
    }

    private static BotgramRpc.RpcResponse.Builder envelope() {
        return BotgramRpc.RpcResponse.newBuilder().setProtocolMajor(1).setRequestId("host-test");
    }

    private TLObject roundTrip(BotgramRpc.RpcResponse.Builder builder, TLObject request, Class<?> expected) throws Exception {
        BotgramRpc.RpcResponse wire = BotgramRpc.RpcResponse.parseFrom(builder.build().toByteArray());
        outcomes.add(wire.getOutcomeCase());
        TLObject mapped = responses.map(wire);
        boolean constructorMatches = mapped.getClass() == expected
                || expected == TLRPC.TL_users_userFull.class && mapped.getClass() == BotgramProtobufResponseMapper.FullUser.class
                || expected == TLRPC.TL_messages_chatFull.class && mapped.getClass() == BotgramProtobufResponseMapper.FullGroup.class;
        check(constructorMatches, wire.getOutcomeCase() + " constructor");
        SerializedData output = new SerializedData();
        mapped.serializeToStream(output);
        byte[] bytes = output.toByteArray();
        SerializedData input = new SerializedData(bytes);
        TLObject decoded = request.deserializeResponse(input, input.readInt32(true), true);
        check(decoded != null && decoded.getClass() == expected && input.remaining() == 0, wire.getOutcomeCase() + " TL parse");
        SerializedData repeated = new SerializedData();
        decoded.serializeToStream(repeated);
        check(Arrays.equals(bytes, repeated.toByteArray()), wire.getOutcomeCase() + " TL byte stability");
        return decoded;
    }

    private interface Attempt { void run() throws Exception; }
    private static void rejects(String label, int status, Attempt action) throws Exception {
        try { action.run(); } catch (BotgramApi.ApiError error) {
            check(error.status == status, label + " status: " + error.status);
            return;
        }
        throw new AssertionError(label + " was accepted");
    }

    private void badResponse(String label, BotgramRpc.RpcResponse.Builder response) throws Exception {
        rejects(label, 400, () -> responses.map(BotgramRpc.RpcResponse.parseFrom(response.build().toByteArray())));
    }

    private static ArrayList<BotgramRpc.MessageEntity> entities() {
        ArrayList<BotgramRpc.MessageEntity> result = new ArrayList<>();
        result.add(BotgramRpc.MessageEntity.newBuilder().setBold(EMPTY).build());
        result.add(BotgramRpc.MessageEntity.newBuilder().setItalic(EMPTY).build());
        result.add(BotgramRpc.MessageEntity.newBuilder().setPre(BotgramRpc.PreEntity.newBuilder().setLanguage("java")).build());
        result.add(BotgramRpc.MessageEntity.newBuilder().setTextUrl(BotgramRpc.TextUrlEntity.newBuilder().setUrl("https://example.invalid")).build());
        result.add(BotgramRpc.MessageEntity.newBuilder().setUnderline(EMPTY).build());
        result.add(BotgramRpc.MessageEntity.newBuilder().setStrike(EMPTY).build());
        result.add(BotgramRpc.MessageEntity.newBuilder().setSpoiler(EMPTY).build());
        result.add(BotgramRpc.MessageEntity.newBuilder().setCode(EMPTY).build());
        result.add(BotgramRpc.MessageEntity.newBuilder().setUrl(EMPTY).build());
        result.add(BotgramRpc.MessageEntity.newBuilder().setEmail(EMPTY).build());
        result.add(BotgramRpc.MessageEntity.newBuilder().setPhone(EMPTY).build());
        result.add(BotgramRpc.MessageEntity.newBuilder().setMention(EMPTY).build());
        result.add(BotgramRpc.MessageEntity.newBuilder().setHashtag(EMPTY).build());
        result.add(BotgramRpc.MessageEntity.newBuilder().setCashtag(EMPTY).build());
        result.add(BotgramRpc.MessageEntity.newBuilder().setBotCommand(EMPTY).build());
        for (int i = 0; i < result.size(); i++) result.set(i, result.get(i).toBuilder().setOffsetUtf16(0).setLengthUtf16(2).build());
        return result;
    }

    private static BotgramRpc.Message.Builder message() {
        return BotgramRpc.Message.newBuilder().setMessageId(17).setPeer(SELF).setSender(USER).setCreatedAtMs(NOW).setOutgoing(true)
                .setText(BotgramRpc.TextMessage.newBuilder().setText("\ud83d\ude00 host text").setSilent(true).addAllEntities(entities())
                        .setReply(BotgramRpc.ReplyRef.newBuilder().setMessageId(16)));
    }

    private static BotgramRpc.MessagePage.Builder page(BotgramRpc.Message.Builder message) {
        return BotgramRpc.MessagePage.newBuilder().addUsers(SUMMARY).addGroups(GROUP_SUMMARY).addMessages(message);
    }

    private void run() throws Exception {
        BotgramRpc.Message text = message().build();
        BotgramRpc.Message service = message().clearText().setPeer(BotgramRpc.PeerRef.newBuilder().setGroupId(GROUP))
                .setService(BotgramRpc.ServiceMessage.newBuilder().setGroupCreated(BotgramRpc.GroupCreatedAction.newBuilder().setTitle("Group").addMembers(USER))).build();
        BotgramRpc.MessageIdMapping mapping = BotgramRpc.MessageIdMapping.newBuilder().setRandomId(RANDOM_ID).setMessageId(17).build();
        BotgramRpc.ReadReceipt read = BotgramRpc.ReadReceipt.newBuilder().setPeer(SELF).setMaxMessageId(17).setPts(7).setPtsCount(1).build();
        BotgramRpc.Updates updates = BotgramRpc.Updates.newBuilder().addUsers(SUMMARY).addGroups(GROUP_SUMMARY).setDateMs(NOW).setSeq(3)
                .addUpdates(BotgramRpc.Update.newBuilder().setNewMessage(BotgramRpc.NewMessage.newBuilder().setMessage(text).setPts(7).setPtsCount(1)))
                .addUpdates(BotgramRpc.Update.newBuilder().setMessageId(mapping))
                .addUpdates(BotgramRpc.Update.newBuilder().setReadInbox(read))
                .addUpdates(BotgramRpc.Update.newBuilder().setReadOutbox(read))
                .addUpdates(BotgramRpc.Update.newBuilder().setGidChanged(BotgramRpc.GidChanged.newBuilder().setUser(USER).setGid("03087654321"))).build();
        BotgramRpc.Dialog dialog = BotgramRpc.Dialog.newBuilder().setPeer(SELF).setTopMessageId(17).setReadInboxMaxId(16).setReadOutboxMaxId(16).setNotifySettings(NOTIFY).build();
        BotgramRpc.DialogPage dialogs = BotgramRpc.DialogPage.newBuilder().addDialogs(dialog).addMessages(text).addUsers(SUMMARY).addGroups(GROUP_SUMMARY).setTotalCount(1).build();
        BotgramRpc.PeerDialogs peers = BotgramRpc.PeerDialogs.newBuilder().addDialogs(dialog).addMessages(text).addUsers(SUMMARY).addGroups(GROUP_SUMMARY).setState(STATE).build();
        BotgramRpc.ResolvedPeer resolved = BotgramRpc.ResolvedPeer.newBuilder().setPeer(SELF).addUsers(SUMMARY).build();
        TLRPC.TL_updates_state state = (TLRPC.TL_updates_state) roundTrip(envelope().setGetState(STATE), new TLRPC.TL_updates_getState(), TLRPC.TL_updates_state.class);
        check(state.date == NOW / 1000 && state.pts == 7, "milliseconds converted without truncating long");
        BotgramRpc.DifferencePage diff = BotgramRpc.DifferencePage.newBuilder().addMessages(text).addMessages(service).addAllUpdates(updates.getUpdatesList()).addUsers(SUMMARY).addGroups(GROUP_SUMMARY).setState(STATE).build();
        roundTrip(envelope().setGetDifference(BotgramRpc.Difference.newBuilder().setEmpty(BotgramRpc.DifferenceEmpty.newBuilder().setDateMs(NOW).setSeq(3))), new TLRPC.TL_updates_getDifference(), TLRPC.TL_updates_differenceEmpty.class);
        roundTrip(envelope().setGetDifference(BotgramRpc.Difference.newBuilder().setTooLong(BotgramRpc.DifferenceTooLong.newBuilder().setPts(7))), new TLRPC.TL_updates_getDifference(), TLRPC.TL_updates_differenceTooLong.class);
        for (boolean slice : new boolean[]{false, true}) {
            BotgramRpc.Difference.Builder value = BotgramRpc.Difference.newBuilder();
            if (slice) value.setSlice(diff); else value.setComplete(diff);
            TLRPC.updates_Difference decoded = (TLRPC.updates_Difference) roundTrip(envelope().setGetDifference(value), new TLRPC.TL_updates_getDifference(), slice ? TLRPC.TL_updates_differenceSlice.class : TLRPC.TL_updates_difference.class);
            check(decoded.new_messages.size() == 2 && decoded.new_messages.get(1) instanceof TLRPC.TL_messageService, "text/service messages");
            checkUpdates(decoded.other_updates);
        }
        roundTrip(envelope().setGetDialogs(dialogs), new TLRPC.TL_messages_getDialogs(), TLRPC.TL_messages_dialogs.class);
        roundTrip(envelope().setGetDialogs(dialogs.toBuilder().setIsSlice(true)), new TLRPC.TL_messages_getDialogs(), TLRPC.TL_messages_dialogsSlice.class);
        roundTrip(envelope().setGetPinnedDialogs(peers), new TLRPC.TL_messages_getPinnedDialogs(), TLRPC.TL_messages_peerDialogs.class);
        roundTrip(envelope().setGetPeerDialogs(peers), new TLRPC.TL_messages_getPeerDialogs(), TLRPC.TL_messages_peerDialogs.class);
        TLRPC.TL_messages_peerDialogs emptyPeer = (TLRPC.TL_messages_peerDialogs) roundTrip(envelope().setGetPeerDialogs(BotgramRpc.PeerDialogs.newBuilder().setState(STATE)), new TLRPC.TL_messages_getPeerDialogs(), TLRPC.TL_messages_peerDialogs.class);
        check(emptyPeer.dialogs.isEmpty(), "first-open missing dialog is a successful empty result");
        TLRPC.messages_Messages emptyHistory = (TLRPC.messages_Messages) roundTrip(envelope().setGetHistory(BotgramRpc.MessagePage.getDefaultInstance()), new TLRPC.TL_messages_getHistory(), TLRPC.TL_messages_messages.class);
        check(emptyHistory.messages.isEmpty(), "first-open history can complete with no messages");
        TLRPC.messages_Messages history = (TLRPC.messages_Messages) roundTrip(envelope().setGetHistory(page(message())), new TLRPC.TL_messages_getHistory(), TLRPC.TL_messages_messages.class);
        TLRPC.Message decodedMessage = history.messages.get(0);
        check(decodedMessage.peer_id.user_id == USER_ID && decodedMessage.from_id.user_id == USER_ID && decodedMessage.reply_to.reply_to_msg_id == 16 && decodedMessage.silent && decodedMessage.out, "self send/reply fields");
        check(decodedMessage.entities.size() == 15, "15 response entities");
        Set<Class<?>> entityTypes = new HashSet<>();
        for (TLRPC.MessageEntity entity : decodedMessage.entities) { entityTypes.add(entity.getClass()); check(entity.offset == 0 && entity.length == 2, "UTF16 entity bounds"); }
        check(entityTypes.size() == 15 && "java".equals(decodedMessage.entities.get(2).language) && "https://example.invalid".equals(decodedMessage.entities.get(3).url), "entity constructors/payloads");
        roundTrip(envelope().setGetMessages(page(message())), new TLRPC.TL_messages_getMessages(), TLRPC.TL_messages_messages.class);
        TLRPC.TL_updates sent = (TLRPC.TL_updates) roundTrip(envelope().setSendMessage(BotgramRpc.MessageSent.newBuilder().setMessage(text).setMapping(mapping).setUpdates(updates)), new TLRPC.TL_messages_sendMessage(), TLRPC.TL_updates.class);
        checkUpdates(sent.updates);
        roundTrip(envelope().setReadHistory(BotgramRpc.AffectedMessages.newBuilder().setPts(7).setPtsCount(1)), new TLRPC.TL_messages_readHistory(), TLRPC.TL_messages_affectedMessages.class);
        roundTrip(envelope().setSearchContacts(BotgramRpc.FoundPeers.newBuilder().addUsers(SUMMARY).addGroups(GROUP_SUMMARY).addResults(SELF).addMyResults(BotgramRpc.PeerRef.newBuilder().setGroupId(GROUP))), new TLRPC.TL_contacts_search(), TLRPC.TL_contacts_found.class);
        roundTrip(envelope().setResolveUsername(resolved), new TLRPC.TL_contacts_resolveUsername(), TLRPC.TL_contacts_resolvedPeer.class);
        roundTrip(envelope().setResolveGid(resolved), new TLRPC.TL_contacts_resolvePhone(), TLRPC.TL_contacts_resolvedPeer.class);
        roundTrip(envelope().setGetUsers(BotgramRpc.UserList.newBuilder().addUsers(SUMMARY)), new TLRPC.TL_users_getUsers(), org.telegram.tgnet.Vector.class);
        roundTrip(envelope().setGetContacts(BotgramRpc.ContactList.newBuilder().addUsers(SUMMARY).setSavedCount(1).addContacts(BotgramRpc.Contact.newBuilder().setUser(USER).setMutual(true))), new TLRPC.TL_contacts_getContacts(), TLRPC.TL_contacts_contacts.class);
        TLRPC.TL_contacts_importedContacts imported = (TLRPC.TL_contacts_importedContacts) roundTrip(envelope().setImportContacts(BotgramRpc.ImportedContacts.newBuilder().addUsers(SUMMARY)
                .addImported(BotgramRpc.ImportedContact.newBuilder().setUser(USER).setClientId(RANDOM_ID)).addRetryContacts(Long.MAX_VALUE)
                .addPopularInvites(BotgramRpc.PopularInvite.newBuilder().setClientId(Long.MIN_VALUE).setImporters(2))), new TLRPC.TL_contacts_importContacts(), TLRPC.TL_contacts_importedContacts.class);
        check(imported.imported.get(0).client_id == RANDOM_ID && imported.retry_contacts.get(0) == Long.MAX_VALUE && imported.popular_invites.get(0).client_id == Long.MIN_VALUE, "sfixed64 contact IDs");
        roundTrip(envelope().setCreateGroup(BotgramRpc.GroupCreated.newBuilder().setGroup(GROUP_SUMMARY).setUpdates(updates).addMissingInvitees(BotgramRpc.MissingInvitee.newBuilder().setUser(USER).setPremiumRequiredForPm(true))), new TLRPC.TL_messages_createChat(), TLRPC.TL_messages_invitedUsers.class);
        roundTrip(envelope().setGetPeerSettings(BotgramRpc.PeerSettingsResult.newBuilder().setSettings(BotgramRpc.PeerSettings.getDefaultInstance()).addUsers(SUMMARY)), new TLRPC.TL_messages_getPeerSettings(), TLRPC.TL_messages_peerSettings.class);
        roundTrip(envelope().setGetFullUser(BotgramRpc.UserFullResult.newBuilder().setUser(USER).setSettings(BotgramRpc.PeerSettings.getDefaultInstance()).setNotifySettings(NOTIFY).setAbout("about").addUsers(SUMMARY)), new TLRPC.TL_users_getFullUser(), TLRPC.TL_users_userFull.class);
        roundTrip(envelope().setGetFullGroup(BotgramRpc.GroupFullResult.newBuilder().setGroup(GROUP_SUMMARY).setNotifySettings(NOTIFY).addUsers(SUMMARY).setAbout("about").setParticipantsVersion(1)
                .addParticipants(BotgramRpc.Participant.newBuilder().setUser(USER).setInviter(USER).setJoinedAtMs(NOW))), new TLRPC.TL_messages_getFullChat(), TLRPC.TL_messages_chatFull.class);
        roundTrip(envelope().setGetGroups(BotgramRpc.GroupList.newBuilder().addGroups(GROUP_SUMMARY)), new TLRPC.TL_messages_getChats(), TLRPC.TL_messages_chats.class);
        roundTrip(envelope().setGetNotifySettings(NOTIFY), new TL_account.getNotifySettings(), TLRPC.TL_peerNotifySettings.class);
        check(outcomes.size() == 21, "all 21 application response outcomes");
        requestChecks(decodedMessage);
        negativeChecks();
    }

    private static void checkUpdates(ArrayList<TLRPC.Update> values) {
        Class<?>[] types = {TL_update.TL_updateNewMessage.class, TL_update.TL_updateMessageID.class, TL_update.TL_updateReadHistoryInbox.class, TL_update.TL_updateReadHistoryOutbox.class, TL_update.TL_updateUserPhone.class};
        check(values.size() == types.length, "five updates");
        for (int i = 0; i < types.length; i++) check(values.get(i).getClass() == types[i], "update constructor " + i);
        check(((TL_update.TL_updateMessageID) values.get(1)).random_id == RANDOM_ID, "sfixed64 random_id");
        check(((TL_update.TL_updateUserPhone) values.get(4)).user_id == USER_ID, "int64 user identity");
    }

    private void requestChecks(TLRPC.Message decoded) throws Exception {
        TLRPC.TL_messages_getDialogs dialogs = new TLRPC.TL_messages_getDialogs();
        dialogs.limit = 100;
        dialogs.offset_peer = new TLRPC.TL_inputPeerEmpty();
        dialogs.exclude_pinned = true;
        BotgramRpc.RpcRequest dialogsWire = BotgramRpc.RpcRequest.parseFrom(requests.map(dialogs, "dialogs").toByteArray());
        check(dialogsWire.getGetDialogs().getLimit() == 100 && !dialogsWire.getGetDialogs().hasOffsetPeer(), "native initial dialogs request accepts exclude_pinned");
        dialogs.exclude_pinned = false;
        check(requests.map(dialogs, "dialogs").equals(dialogsWire), "no pinned dialogs: exclude_pinned preserves results");
        dialogs.exclude_pinned = true;
        dialogs.offset_id = 17;
        TLRPC.TL_inputPeerUser offset = new TLRPC.TL_inputPeerUser();
        offset.user_id = USER_ID;
        dialogs.offset_peer = offset;
        BotgramRpc.GetDialogsRequest next = requests.map(dialogs, "next-dialogs").getGetDialogs();
        check(next.getOffsetMessageId() == 17 && next.getOffsetPeer().getUser().equals(USER), "exclude_pinned preserves pagination offset and peer");
        TLRPC.TL_messages_sendMessage send = new TLRPC.TL_messages_sendMessage();
        send.peer = new TLRPC.TL_inputPeerSelf();
        send.send_as = new TLRPC.TL_inputPeerSelf();
        send.message = decoded.message;
        send.random_id = RANDOM_ID;
        send.silent = true;
        send.clear_draft = true;
        send.entities.addAll(decoded.entities);
        TLRPC.TL_inputReplyToMessage reply = new TLRPC.TL_inputReplyToMessage();
        reply.reply_to_msg_id = 16;
        TLRPC.TL_inputPeerUser explicitSelf = new TLRPC.TL_inputPeerUser();
        explicitSelf.user_id = USER_ID;
        reply.reply_to_peer_id = explicitSelf;
        send.reply_to = reply;
        BotgramRpc.RpcRequest wire = BotgramRpc.RpcRequest.parseFrom(requests.map(send, "send").toByteArray());
        check(wire.getSendMessage().getPeer().getSelf() && wire.getSendMessage().getReplyToMessageId() == 16 && wire.getSendMessage().getRandomId() == RANDOM_ID, "request self/reply/sfixed64");
        check(wire.getSendMessage().getEntitiesList().equals(entities()), "15 request entity round-trips");
        send.clear_draft = false;
        check(requests.map(send, "send").equals(wire), "local clear_draft true/false preserve identical send semantics");
        send.clear_draft = true;
        send.peer = explicitSelf;
        reply.reply_to_peer_id = new TLRPC.TL_inputPeerSelf();
        check(requests.map(send, "explicit-self").getSendMessage().getPeer().getUser().equals(USER), "explicit self identity");
        send.no_webpage = true;
        rejects("unsupported send option", 501, () -> requests.map(send, "unsupported"));
        rejects("unsupported request", 501, () -> requests.map(new TLRPC.TL_help_getConfig(), "unsupported"));
        send.no_webpage = false;
        send.entities.clear();
        TLRPC.TL_messageEntityUnknown unknown = new TLRPC.TL_messageEntityUnknown();
        unknown.offset = 0; unknown.length = 1;
        send.entities.add(unknown);
        rejects("unsupported request entity", 501, () -> requests.map(send, "entity"));
        send.entities.clear();
        TLRPC.TL_messageEntityBold split = new TLRPC.TL_messageEntityBold();
        split.offset = 1; split.length = 1;
        send.entities.add(split);
        rejects("request surrogate split", 400, () -> requests.map(send, "surrogate"));
        TLRPC.TL_contacts_importContacts contacts = new TLRPC.TL_contacts_importContacts();
        TLRPC.TL_inputPhoneContact contact = new TLRPC.TL_inputPhoneContact();
        contact.client_id = Long.MAX_VALUE; contact.phone = "03012345678";
        contacts.contacts.add(contact);
        check(BotgramRpc.RpcRequest.parseFrom(requests.map(contacts, "contacts").toByteArray()).getImportContacts().getContacts(0).getClientId() == Long.MAX_VALUE, "request sfixed64 client_id");
    }

    private void negativeChecks() throws Exception {
        badResponse("missing outcome", envelope());
        badResponse("missing difference", envelope().setGetDifference(BotgramRpc.Difference.getDefaultInstance()));
        badResponse("missing peer", envelope().setGetHistory(page(message().setPeer(BotgramRpc.PeerRef.getDefaultInstance()))));
        badResponse("missing body", envelope().setGetHistory(page(message().clearText())));
        badResponse("missing service action", envelope().setGetHistory(page(message().clearText().setService(BotgramRpc.ServiceMessage.getDefaultInstance()))));
        badResponse("missing update kind", envelope().setCreateGroup(BotgramRpc.GroupCreated.newBuilder().setGroup(GROUP_SUMMARY).setUpdates(BotgramRpc.Updates.newBuilder().addUpdates(BotgramRpc.Update.getDefaultInstance()))));
        badResponse("message int32 overflow", envelope().setGetHistory(page(message().setMessageId(2147483648L))));
        badResponse("negative message id", envelope().setGetHistory(page(message().setMessageId(-1))));
        badResponse("reply int32 overflow", envelope().setGetHistory(page(message().setText(BotgramRpc.TextMessage.newBuilder().setText("x").setReply(BotgramRpc.ReplyRef.newBuilder().setMessageId(2147483648L))))));
        badResponse("time positive overflow", envelope().setGetState(STATE.toBuilder().setDateMs(2147483648000L)));
        badResponse("time negative overflow", envelope().setGetState(STATE.toBuilder().setDateMs(-2147483648001L)));
        badResponse("notify time overflow", envelope().setGetNotifySettings(NOTIFY.toBuilder().setMuteUntilMs(Long.MAX_VALUE)));
        badResponse("entity unsigned int32 overflow", envelope().setGetHistory(page(message().setText(BotgramRpc.TextMessage.newBuilder().setText("abc").addEntities(BotgramRpc.MessageEntity.newBuilder().setBold(EMPTY).setOffsetUtf16(-1).setLengthUtf16(1))))));
        badResponse("missing entity kind", envelope().setGetHistory(page(message().setText(BotgramRpc.TextMessage.newBuilder().setText("abc").addEntities(BotgramRpc.MessageEntity.newBuilder().setLengthUtf16(1))))));
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        CodedOutputStream unknown = CodedOutputStream.newInstance(bytes);
        unknown.writeUInt32(2, 1); unknown.writeByteArray(99, new byte[0]); unknown.flush();
        BotgramRpc.MessageEntity unknownEntity = BotgramRpc.MessageEntity.parseFrom(bytes.toByteArray());
        badResponse("unknown entity wire tag", envelope().setGetHistory(page(message().setText(BotgramRpc.TextMessage.newBuilder().setText("abc").addEntities(unknownEntity)))));
        TLRPC.TL_updates_state negativeTime = (TLRPC.TL_updates_state) roundTrip(envelope().setGetState(STATE.toBuilder().setDateMs(-1)), new TLRPC.TL_updates_getState(), TLRPC.TL_updates_state.class);
        check(negativeTime.date == -1, "negative milliseconds floor to seconds");
        for (int boundary : new int[]{Integer.MIN_VALUE, Integer.MAX_VALUE}) {
            TLRPC.TL_updates_state edge = (TLRPC.TL_updates_state) roundTrip(envelope().setGetState(STATE.toBuilder().setDateMs((long) boundary * 1000)), new TLRPC.TL_updates_getState(), TLRPC.TL_updates_state.class);
            check(edge.date == boundary, "timestamp int32 boundary");
        }
        rejects("RPC error", 409, () -> responses.map(envelope().setError(BotgramRpc.RpcError.newBuilder().setHttpStatus(409).setCode("conflict")).build()));
    }

    private static void rpcChecks() throws Exception {
        AtomicInteger calls = new AtomicInteger();
        AtomicInteger jsonCalls = new AtomicInteger();
        AtomicReference<Throwable> handlerError = new AtomicReference<>();
        AtomicInteger status = new AtomicInteger(200);
        AtomicReference<String> media = new AtomicReference<>("application/x-protobuf");
        AtomicReference<byte[]> body = new AtomicReference<>();
        BotgramRpc.RpcRequest request = BotgramRpc.RpcRequest.newBuilder().setProtocolMajor(1)
                .setRequestId("host-test").setGetState(BotgramRpc.GetStateRequest.getDefaultInstance()).build();
        byte[] valid = envelope().setGetState(STATE).build().toByteArray();
        body.set(valid);
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", exchange -> {
            calls.incrementAndGet();
            try {
                String contentType = exchange.getRequestHeaders().getFirst("Content-Type");
                String accept = exchange.getRequestHeaders().getFirst("Accept");
                if ((contentType != null && contentType.contains("json")) || (accept != null && accept.contains("json"))) jsonCalls.incrementAndGet();
                if (!"/api/v1/rpc".equals(exchange.getRequestURI().getPath()) || !"POST".equals(exchange.getRequestMethod())
                        || !"application/x-protobuf".equals(contentType) || !"application/x-protobuf".equals(accept)
                        || !request.equals(BotgramRpc.RpcRequest.parseFrom(exchange.getRequestBody()))) {
                    throw new AssertionError("real API protobuf request/path/headers");
                }
                byte[] bytes = body.get();
                exchange.getResponseHeaders().set("Content-Type", media.get());
                exchange.sendResponseHeaders(status.get(), bytes.length);
                exchange.getResponseBody().write(bytes);
            } catch (Throwable error) { handlerError.compareAndSet(null, error); }
            finally { exchange.close(); }
        });
        server.start();
        try {
            BotgramApi api = new BotgramApi("http://127.0.0.1:" + server.getAddress().getPort(), null);
            check(api.rpc(request).getGetState().equals(STATE), "real API valid protobuf response");
            String[] labels = {"HTTP 401 non-protobuf", "HTTP 200 wrong media", "request_id mismatch", "major mismatch",
                    "outcome mismatch", "missing outcome", "HTTP/RpcError status mismatch", "HTTP 200 RpcError", "malformed protobuf"};
            byte[][] bodies = {
                    "unauthorized".getBytes(StandardCharsets.UTF_8), valid,
                    envelope().setRequestId("another-request").setGetState(STATE).build().toByteArray(),
                    envelope().setProtocolMajor(2).setGetState(STATE).build().toByteArray(),
                    envelope().setGetContacts(BotgramRpc.ContactList.getDefaultInstance()).build().toByteArray(),
                    envelope().build().toByteArray(),
                    envelope().setError(BotgramRpc.RpcError.newBuilder().setHttpStatus(409).setCode("conflict")).build().toByteArray(),
                    envelope().setError(BotgramRpc.RpcError.newBuilder().setHttpStatus(409).setCode("conflict")).build().toByteArray(),
                    new byte[]{(byte) 0x80}
            };
            for (int i = 0; i < labels.length; i++) {
                status.set(i == 0 ? 401 : i == 6 ? 403 : 200);
                media.set(i < 2 ? "application/json" : "application/x-protobuf");
                body.set(bodies[i]);
                int before = calls.get();
                if (i == labels.length - 1) {
                    try {
                        api.rpc(request);
                        throw new AssertionError("malformed protobuf was accepted");
                    } catch (com.google.protobuf.InvalidProtocolBufferException expected) {
                        check(true, "real API malformed protobuf rejected");
                    }
                } else {
                    rejects("real API " + labels[i], i == 0 ? 401 : 502, () -> api.rpc(request));
                }
                check(calls.get() == before + 1 && jsonCalls.get() == 0, labels[i] + " no retry or JSON fallback");
                check(handlerError.get() == null, labels[i] + " protobuf request contract: " + handlerError.get());
            }
            check(calls.get() == labels.length + 1 && jsonCalls.get() == 0, "all real API calls use protobuf exactly once");
        } finally { server.stop(0); }
    }

    private static void identityChecks(BotgramApi api, BotgramProtobufIdentity first, AtomicInteger resolverCalls) throws Exception {
        int before = resolverCalls.get();
        BotgramProtobufIdentity inactive = new BotgramProtobufIdentity(api, UUID, () -> false);
        rejects("inactive identity initialize", 401, inactive::initialize);
        rejects("inactive identity learn", 401, () -> inactive.learn(Arrays.asList(SUMMARY), Arrays.asList(GROUP_SUMMARY)));
        check(resolverCalls.get() == before, "inactive identity makes no resolver request");
        AtomicBoolean active = new AtomicBoolean(true);
        BotgramProtobufIdentity second = new BotgramProtobufIdentity(api, UUID, active::get);
        second.initialize();
        check(resolverCalls.get() == before + 1 && second.selfId() == USER_ID, "independent identity bootstraps its own cache");
        BotgramRpc.UserRef firstUser = USER.toBuilder().setUserId("33333333333333333333333333333333").build();
        BotgramRpc.UserRef secondUser = USER.toBuilder().setUserId("44444444444444444444444444444444").build();
        BotgramRpc.UserSummary firstSummary = SUMMARY.toBuilder().setIdentity(firstUser).setTlUserId(USER_ID + 10).setIsSelf(false).build();
        BotgramRpc.UserSummary secondSummary = SUMMARY.toBuilder().setIdentity(secondUser).setTlUserId(USER_ID + 20).setIsSelf(false).build();
        BotgramRpc.GroupSummary firstGroup = GROUP_SUMMARY.toBuilder().setGroupId("55555555555555555555555555555555").setTlChatId(CHAT_ID + 10).build();
        BotgramRpc.GroupSummary secondGroup = GROUP_SUMMARY.toBuilder().setGroupId("66666666666666666666666666666666").setTlChatId(CHAT_ID + 20).build();
        first.learn(Arrays.asList(firstSummary), Arrays.asList(firstGroup));
        check(first.userId(firstUser) == USER_ID + 10 && first.groupId(firstGroup.getGroupId()) == CHAT_ID + 10, "first identity learns user/group");
        rejects("second does not share first user cache", 502, () -> second.userId(firstUser));
        rejects("second does not share first group cache", 502, () -> second.groupId(firstGroup.getGroupId()));
        active.set(false);
        rejects("initialized inactive identity initialize", 401, second::initialize);
        rejects("initialized inactive identity learn", 401, () -> second.learn(Arrays.asList(secondSummary), Arrays.asList(secondGroup)));
        active.set(true);
        rejects("inactive learn did not cache user", 502, () -> second.userId(secondUser));
        rejects("inactive learn did not cache group", 502, () -> second.groupId(secondGroup.getGroupId()));
        second.learn(Arrays.asList(secondSummary), Arrays.asList(secondGroup));
        check(second.userId(secondUser) == USER_ID + 20 && second.groupId(secondGroup.getGroupId()) == CHAT_ID + 20, "second identity learns user/group");
        rejects("first does not share second user cache", 502, () -> first.userId(secondUser));
        rejects("first does not share second group cache", 502, () -> first.groupId(secondGroup.getGroupId()));
        check(first.selfId() == USER_ID && second.selfId() == USER_ID && resolverCalls.get() == before + 1, "isolated caches preserve self and avoid extra resolver calls");
    }

    private static void blackholeChecks() throws Exception {
        AtomicInteger calls = new AtomicInteger();
        AtomicInteger jsonCalls = new AtomicInteger();
        AtomicInteger mode = new AtomicInteger();
        AtomicReference<Throwable> handlerError = new AtomicReference<>();
        AtomicReference<java.util.concurrent.CountDownLatch> release = new AtomicReference<>();
        java.util.concurrent.ExecutorService workers = java.util.concurrent.Executors.newCachedThreadPool();
        BotgramRpc.RpcRequest request = BotgramRpc.RpcRequest.newBuilder().setProtocolMajor(1)
                .setRequestId("host-test").setGetState(BotgramRpc.GetStateRequest.getDefaultInstance()).build();
        byte[] valid = envelope().setGetState(STATE).build().toByteArray();
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.setExecutor(workers);
        server.createContext("/", exchange -> {
            calls.incrementAndGet();
            java.util.concurrent.CountDownLatch gate = release.get();
            try {
                if (!"/api/v1/rpc".equals(exchange.getRequestURI().getPath())) jsonCalls.incrementAndGet();
                if (!request.equals(BotgramRpc.RpcRequest.parseFrom(exchange.getRequestBody())))
                    throw new AssertionError("blackhole request contract");
                int current = mode.get();
                exchange.getResponseHeaders().set("Content-Type", "application/x-protobuf");
                if (current == 2) {
                    exchange.sendResponseHeaders(200, valid.length);
                    exchange.getResponseBody().write(valid, 0, 1);
                    exchange.getResponseBody().flush();
                }
                if (current != 0) {
                    if (!gate.await(35, java.util.concurrent.TimeUnit.SECONDS))
                        throw new AssertionError("blackhole test did not release handler");
                    return;
                }
                exchange.sendResponseHeaders(200, valid.length);
                exchange.getResponseBody().write(valid);
            } catch (Throwable error) { handlerError.compareAndSet(null, error); }
            finally { exchange.close(); }
        });
        server.start();
        try {
            BotgramApi api = new BotgramApi("http://127.0.0.1:" + server.getAddress().getPort(), null);
            for (int phase = 1; phase <= 2; phase++) {
                mode.set(phase);
                java.util.concurrent.CountDownLatch gate = new java.util.concurrent.CountDownLatch(1);
                release.set(gate);
                int before = calls.get();
                long started = System.nanoTime();
                try {
                    api.rpc(request);
                    throw new AssertionError("blackhole must time out");
                } catch (java.net.SocketTimeoutException expected) {
                    long elapsed = java.util.concurrent.TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started);
                    check(elapsed >= 18000 && elapsed < 35000, "production 20s read timeout is bounded");
                    System.out.println("BLACKHOLE phase=" + phase + " elapsed_ms=" + elapsed + " timeout=SocketTimeoutException");
                } finally { gate.countDown(); }
                check(calls.get() == before + 1 && jsonCalls.get() == 0, "blackhole no retry or JSON fallback");
                mode.set(0);
                check(api.rpc(request).getGetState().equals(STATE), "same API recovers after blackhole");
                check(calls.get() == before + 2 && handlerError.get() == null, "recovery one request without handler failure");
            }
        } finally {
            if (release.get() != null) release.get().countDown();
            server.stop(0);
            workers.shutdownNow();
        }
    }

    private static void negotiationChecks() throws Exception {
        AtomicInteger calls = new AtomicInteger();
        AtomicInteger retired = new AtomicInteger();
        AtomicInteger legacyCalls = new AtomicInteger();
        AtomicInteger unexpectedPbCalls = new AtomicInteger();
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/api/protocol/capabilities", exchange -> {
            calls.incrementAndGet();
            String bearer = exchange.getRequestHeaders().getFirst("Authorization");
            String selected = retired.get() == 0 && "Bearer json-session".equals(bearer) ? "tl-json-v1" : "protobuf-v1";
            String protocols = retired.get() == 0 ? "[\"protobuf-v1\",\"tl-json-v1\"]" : "[\"protobuf-v1\"]";
            byte[] bytes = ("{\"negotiationVersion\":1,\"protocols\":" + protocols + ","
                    + "\"selectedProtocol\":\"" + selected + "\",\"capabilities\":[\"send-random-id-idempotency-v1\"]}").getBytes(StandardCharsets.UTF_8);
            if ("Bearer invalid-session".equals(bearer)) bytes = "{}".getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(200, bytes.length);
            exchange.getResponseBody().write(bytes);
            exchange.close();
        });
        server.createContext("/api/telegram/rpc", exchange -> {
            legacyCalls.incrementAndGet();
            byte[] bytes = (retired.get() == 0 ? "{}" : "{\"error\":\"BOTGRAM_JSON_RPC_DISABLED\"}").getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(retired.get() == 0 ? 200 : 410, bytes.length);
            exchange.getResponseBody().write(bytes); exchange.close();
        });
        server.createContext("/api/v1/rpc", exchange -> {
            unexpectedPbCalls.incrementAndGet(); exchange.sendResponseHeaders(500, -1); exchange.close();
        });
        server.start();
        try {
            String url = "http://127.0.0.1:" + server.getAddress().getPort();
            BotgramApi proto = new BotgramApi(url, "proto-session");
            BotgramApi json = new BotgramApi(url, "json-session");
            check("protobuf-v1".equals(proto.negotiate()), "protobuf session negotiated");
            check("tl-json-v1".equals(json.negotiate()), "JSON session negotiated independently");
            check("protobuf-v1".equals(proto.negotiate()) && calls.get() == 2, "session protocol pinned without refetch");
            rejects("invalid negotiation fails closed", 502, () -> new BotgramApi(url, "invalid-session").negotiate());
            check("protobuf-v1".equals(proto.negotiate()) && calls.get() == 3, "other session failure does not invalidate pin");
            retired.set(1);
            int beforeRetirement = calls.get();
            check("tl-json-v1".equals(json.negotiate()) && calls.get() == beforeRetirement, "existing JSON API remains pinned after retirement advertisement");
            rejects("pinned JSON request explicitly rejected", 410, () -> json.object("POST", "/api/telegram/rpc", new org.json.JSONObject()));
            check(legacyCalls.get() == 1 && unexpectedPbCalls.get() == 0 && calls.get() == beforeRetirement, "410 does not retry or implicitly renegotiate/switch a write");
            check("tl-json-v1".equals(json.negotiate()), "410 preserves old session pin");
            check("protobuf-v1".equals(new BotgramApi(url, "json-session").negotiate()), "fresh API negotiates PB after JSON is disabled");
            retired.set(0);
            json.object("POST", "/api/telegram/rpc", new org.json.JSONObject());
            check(legacyCalls.get() == 2 && unexpectedPbCalls.get() == 0, "restoring compatibility allows same JSON API without identity migration");
        } finally { server.stop(0); }
    }

    private void privacyChecks() throws Exception {
        TLRPC.InputPrivacyKey[] keys = { new TLRPC.TL_inputPrivacyKeyStatusTimestamp(), new TLRPC.TL_inputPrivacyKeyChatInvite(),
            new TLRPC.TL_inputPrivacyKeyPhoneCall(), new TLRPC.TL_inputPrivacyKeyPhoneP2P(), new TLRPC.TL_inputPrivacyKeyProfilePhoto(),
            new TLRPC.TL_inputPrivacyKeyForwards(), new TLRPC.TL_inputPrivacyKeyPhoneNumber(), new TLRPC.TL_inputPrivacyKeyAddedByPhone(),
            new TLRPC.TL_inputPrivacyKeyVoiceMessages(), new TLRPC.TL_inputPrivacyKeyAbout(), new TLRPC.TL_inputPrivacyKeyBirthday(),
            new TLRPC.TL_inputPrivacyKeyStarGiftsAutoSave(), new TLRPC.TL_inputPrivacyKeySavedMusic(), new TLRPC.TL_inputPrivacyKeyNoPaidMessages() };
        for (int i = 0; i < keys.length; i++) {
            TL_account.setPrivacy request = new TL_account.setPrivacy(); request.key = keys[i];
            TLRPC.TL_inputPrivacyValueAllowUsers allowed = new TLRPC.TL_inputPrivacyValueAllowUsers();
            TLRPC.TL_inputUser user = new TLRPC.TL_inputUser(); user.user_id = USER_ID; allowed.users.add(user);
            request.rules.add(allowed); request.rules.add(new TLRPC.TL_inputPrivacyValueDisallowAll());
            BotgramRpc.PrivacyRuleSet rules = requests.map(request, "privacy").getAccountPrivacy().getSetRules();
            check(rules.getKeyValue() == i + 1 && rules.getRules(0).getUsers(0).getUserId().equals(UUID), "explicit privacy request key and user");
            TL_account.privacyRules decoded = (TL_account.privacyRules)roundTrip(envelope().setAccountPrivacy(
                BotgramRpc.AccountPrivacyResponse.newBuilder().setRules(rules).addUsers(SUMMARY)), request, TL_account.privacyRules.class);
            check(((TLRPC.TL_privacyValueAllowUsers)decoded.rules.get(0)).users.get(0) == USER_ID, "privacy user keeps 64-bit identity");
        }
        roundTrip(envelope().setAccountPrivacy(BotgramRpc.AccountPrivacyResponse.newBuilder().setAccountTtl(BotgramRpc.AccountTtl.newBuilder().setDays(180))), new TL_account.getAccountTTL(), TLRPC.TL_accountDaysTTL.class);
        roundTrip(envelope().setAccountPrivacy(BotgramRpc.AccountPrivacyResponse.newBuilder().setHistoryTtl(BotgramRpc.HistoryTtl.newBuilder().setSeconds(86400))), new TLRPC.TL_messages_getDefaultHistoryTTL(), TLRPC.TL_defaultHistoryTTL.class);
        roundTrip(envelope().setAccountPrivacy(BotgramRpc.AccountPrivacyResponse.newBuilder().setBlocked(BotgramRpc.BlockedUsers.newBuilder().setCount(1)
            .addBlocked(BotgramRpc.BlockedUser.newBuilder().setUser(USER).setBlockedAtMs(NOW))).addUsers(SUMMARY)), new TLRPC.TL_contacts_getBlocked(), TLRPC.TL_contacts_blockedSlice.class);
        roundTrip(envelope().setAccountPrivacy(BotgramRpc.AccountPrivacyResponse.newBuilder().setWebAuthorizations(EMPTY)), new TL_account.getWebAuthorizations(), TL_account.webAuthorizations.class);
        roundTrip(envelope().setAccountPrivacy(BotgramRpc.AccountPrivacyResponse.newBuilder().setPasskeys(EMPTY)), new TL_account.getPasskeys(), TL_account.Passkeys.class);
        roundTrip(envelope().setAccountPrivacy(BotgramRpc.AccountPrivacyResponse.newBuilder().setContactsDeleted(EMPTY).addUsers(SUMMARY)), new TLRPC.TL_contacts_deleteContacts(), TLRPC.TL_updates.class);
    }

    public static void main(String[] args) throws Exception {
        negotiationChecks();
        rpcChecks();
        if (Arrays.asList(args).contains("--blackhole")) blackholeChecks();
        AtomicInteger resolverCalls = new AtomicInteger();
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/api/v1/rpc", exchange -> {
            resolverCalls.incrementAndGet();
            try {
                BotgramRpc.RpcRequest request = BotgramRpc.RpcRequest.parseFrom(exchange.getRequestBody());
                if (!request.hasResolveIdentities()) throw new AssertionError("resolver operation");
                byte[] bytes = envelope().setRequestId(request.getRequestId()).setResolveIdentities(BotgramRpc.ResolvedIdentities.newBuilder()
                        .setServerDomain("host.invalid").addUsers(SUMMARY).addGroups(GROUP_SUMMARY)).build().toByteArray();
                exchange.getResponseHeaders().set("Content-Type", "application/x-protobuf");
                exchange.sendResponseHeaders(200, bytes.length);
                exchange.getResponseBody().write(bytes);
            } finally { exchange.close(); }
        });
        server.start();
        try {
            BotgramApi api = new BotgramApi("http://127.0.0.1:" + server.getAddress().getPort(), null);
            BotgramProtobufIdentity identity = new BotgramProtobufIdentity(api, UUID, () -> true);
            identity.initialize();
            check(identity.selfId() == USER_ID, "real API resolver bootstrap");
            ProtobufAndroidSmoke smoke = new ProtobufAndroidSmoke(identity);
            smoke.run();
            smoke.privacyChecks();
            identityChecks(api, identity, resolverCalls);
            System.out.println("PASS: 21 original response outcomes plus privacy/security, 15 entities, 4 differences, 5 updates; " + checks + " assertions against original Android TL classes (host only)");
        } finally { server.stop(0); }
    }
}
