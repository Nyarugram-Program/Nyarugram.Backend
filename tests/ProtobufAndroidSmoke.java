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
import org.telegram.tgnet.tl.TL_phone;
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
                .addUpdates(BotgramRpc.Update.newBuilder().setMessageId(mapping).build())
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
        callChecks();
        check(outcomes.size() == 29, "all 29 application response outcomes");
        presenceContactChecks();
        requestChecks(decodedMessage);
        negativeChecks();
    }

    private void presenceContactChecks() throws Exception {
        TLRPC.TL_contacts_addContact request = new TLRPC.TL_contacts_addContact();
        TLRPC.TL_inputUser target = new TLRPC.TL_inputUser();
        target.user_id = USER_ID;
        request.id = target;
        request.first_name = "Contact";
        request.last_name = "Alias";
        BotgramRpc.RpcRequest encoded = requests.map(request, "add-contact");
        check(encoded.hasAddContact() && encoded.getAddContact().getUser().getUser().equals(USER)
                && encoded.getAddContact().getFirstName().equals("Contact"), "addContact typed request mapping");
        // The original editor always supplies a textWithEntities object; empty content means no note.
        TLRPC.TL_textWithEntities emptyNote = new TLRPC.TL_textWithEntities();
        request.note = emptyNote;
        encoded = BotgramRpc.RpcRequest.parseFrom(requests.map(request, "add-contact-empty-note").toByteArray());
        check(encoded.hasAddContact() && encoded.getAddContact().getFirstName().equals("Contact"), "empty note object equals no note");
        request.flags = 2;
        encoded = BotgramRpc.RpcRequest.parseFrom(requests.map(request, "add-contact-empty-note-flag").toByteArray());
        check(encoded.hasAddContact(), "empty note with note flag equals no note");
        request.flags = 0;
        emptyNote.text = "Private note";
        rejects("contact note content rejected before transport", 501, () -> requests.map(request, "note"));
        emptyNote.text = "";
        TLRPC.TL_messageEntityBold bold = new TLRPC.TL_messageEntityBold();
        bold.offset = 0;
        bold.length = 1;
        emptyNote.entities.add(bold);
        rejects("contact note entities rejected before transport", 501, () -> requests.map(request, "note-entities"));
        emptyNote.entities.clear();
        request.note = null;
        request.flags = 2;
        rejects("note flag without a note object is invalid", 400, () -> requests.map(request, "note-flag-invalid"));
        request.flags = 4;
        rejects("unknown addContact flags rejected", 501, () -> requests.map(request, "unknown-flags"));
        request.flags = 0;
        request.add_phone_privacy_exception = true;
        rejects("contact phone privacy exception rejected", 501, () -> requests.map(request, "phone-exception"));
        request.add_phone_privacy_exception = false;
        TLRPC.TL_contacts_importContacts importRequest = new TLRPC.TL_contacts_importContacts();
        TLRPC.TL_inputPhoneContact importContact = new TLRPC.TL_inputPhoneContact();
        importContact.phone = "+03012345678";
        importContact.first_name = "Contact";
        importContact.client_id = 7;
        importContact.note = new TLRPC.TL_textWithEntities();
        importRequest.contacts.add(importContact);
        encoded = BotgramRpc.RpcRequest.parseFrom(requests.map(importRequest, "import-empty-note").toByteArray());
        check(encoded.getImportContacts().getContacts(0).getGid().equals("+03012345678"), "empty import note equals no note");
        importContact.flags = 1;
        encoded = BotgramRpc.RpcRequest.parseFrom(requests.map(importRequest, "import-empty-note-flag").toByteArray());
        check(encoded.getImportContacts().getContactsCount() == 1, "empty import note with flag equals no note");
        importContact.note.text = "Private note";
        rejects("import note content rejected before transport", 501, () -> requests.map(importRequest, "import-note"));
        importContact.note.text = "";
        importContact.note = null;
        rejects("import note flag without object is invalid", 400, () -> requests.map(importRequest, "import-note-invalid"));
        importContact.flags = 0;
        for (BotgramRpc.UserStatus status : new BotgramRpc.UserStatus[] {
                BotgramRpc.UserStatus.newBuilder().setOnlineUntilMs(NOW).build(),
                BotgramRpc.UserStatus.newBuilder().setLastSeenMs(NOW - 60000).build(),
                BotgramRpc.UserStatus.getDefaultInstance() }) {
            BotgramRpc.UserSummary summary = SUMMARY.toBuilder().setStatus(status).build();
            BotgramRpc.Updates values = BotgramRpc.Updates.newBuilder().setDateMs(NOW).addUsers(summary)
                    .addUpdates(BotgramRpc.Update.newBuilder().setPresenceChanged(BotgramRpc.PresenceChanged.newBuilder()
                            .setUser(USER).setStatus(status))).build();
            TLRPC.TL_updates decoded = (TLRPC.TL_updates) roundTrip(envelope().setAddContact(values), request, TLRPC.TL_updates.class);
            TLRPC.UserStatus userStatus = decoded.users.get(0).status;
            TL_update.TL_updateUserStatus update = (TL_update.TL_updateUserStatus) decoded.updates.get(0);
            Class<?> expected = status.hasOnlineUntilMs() ? TLRPC.TL_userStatusOnline.class
                    : status.hasLastSeenMs() ? TLRPC.TL_userStatusOffline.class : TLRPC.TL_userStatusEmpty.class;
            check(userStatus.getClass() == expected && update.status.getClass() == expected && update.user_id == USER_ID,
                    "presence concrete user/update TL classes");
            check(userStatus.expires == update.status.expires, "presence user/update timestamp equality");
            if (status.hasLastSeenMs()) check(userStatus.expires == (NOW - 60000) / 1000, "offline last-seen timestamp survives TL serialization");
        }
        badResponse("presence without status", envelope().setAddContact(BotgramRpc.Updates.newBuilder().addUsers(SUMMARY)
                .addUpdates(BotgramRpc.Update.newBuilder().setPresenceChanged(BotgramRpc.PresenceChanged.newBuilder().setUser(USER)))));
    }

    private void callChecks() throws Exception {
        BotgramRpc.PhoneCallProtocol protocol = BotgramRpc.PhoneCallProtocol.newBuilder()
                .setUdpP2P(true).setUdpReflector(true).setMinLayer(65).setMaxLayer(92)
                .addLibraryVersions("10.0.0").build();
        BotgramRpc.PhoneCall base = BotgramRpc.PhoneCall.newBuilder().setCallId(555L).setAccessHash(77L)
                .setAdminTlUserId(USER_ID).setParticipantTlUserId(USER_ID + 1).setDateMs(NOW).setVideo(true)
                .setProtocol(protocol).build();

        TL_phone.requestCall request = new TL_phone.requestCall();
        TL_phone.TL_phone_phoneCall waiting = (TL_phone.TL_phone_phoneCall) roundTrip(
                envelope().setRequestCall(BotgramRpc.PhoneCallResult.newBuilder()
                        .setPhoneCall(base.toBuilder().setState(BotgramRpc.PhoneCallState.PHONE_CALL_STATE_WAITING)
                                .setReceiveDateMs(NOW)).addUsers(SUMMARY)),
                request, TL_phone.TL_phone_phoneCall.class);
        check(waiting.phone_call.getClass() == TL_phone.TL_phoneCallWaiting.class, "waiting call constructor");
        check(waiting.phone_call.id == 555L && waiting.phone_call.access_hash == 77L
                && waiting.phone_call.admin_id == USER_ID && waiting.phone_call.participant_id == USER_ID + 1
                && waiting.phone_call.receive_date == NOW / 1000 && waiting.phone_call.video
                && waiting.phone_call.protocol.udp_p2p && waiting.phone_call.protocol.min_layer == 65
                && waiting.phone_call.protocol.library_versions.equals(java.util.Collections.singletonList("10.0.0")),
                "waiting call fields");
        check(waiting.users.size() == 1 && waiting.users.get(0).id == USER_ID, "call result users");

        TL_phone.TL_phone_phoneCall requested = (TL_phone.TL_phone_phoneCall) roundTrip(
                envelope().setRequestCall(BotgramRpc.PhoneCallResult.newBuilder()
                        .setPhoneCall(base.toBuilder().setState(BotgramRpc.PhoneCallState.PHONE_CALL_STATE_REQUESTED)
                                .setGAHash(com.google.protobuf.ByteString.copyFrom(new byte[] {1, 2, 3})))),
                request, TL_phone.TL_phone_phoneCall.class);
        check(requested.phone_call.getClass() == TL_phone.phoneCallRequested.class
                && java.util.Arrays.equals(requested.phone_call.g_a_hash, new byte[] {1, 2, 3}), "requested call g_a_hash");

        TL_phone.TL_phone_phoneCall accepted = (TL_phone.TL_phone_phoneCall) roundTrip(
                envelope().setAcceptCall(BotgramRpc.PhoneCallResult.newBuilder()
                        .setPhoneCall(base.toBuilder().setState(BotgramRpc.PhoneCallState.PHONE_CALL_STATE_ACCEPTED)
                                .setGB(com.google.protobuf.ByteString.copyFrom(new byte[] {4, 5})))),
                new TL_phone.acceptCall(), TL_phone.TL_phone_phoneCall.class);
        check(accepted.phone_call.getClass() == TL_phone.TL_phoneCallAccepted.class
                && java.util.Arrays.equals(accepted.phone_call.g_b, new byte[] {4, 5}), "accepted call g_b");

        BotgramRpc.PhoneConnection connection = BotgramRpc.PhoneConnection.newBuilder()
                .setId(9L).setIp("127.0.0.1").setIpv6("::1").setPort(8443)
                .setTurn(true).setStun(false).setUsername("user").setPassword("secret").build();
        TL_phone.TL_phone_phoneCall active = (TL_phone.TL_phone_phoneCall) roundTrip(
                envelope().setConfirmCall(BotgramRpc.PhoneCallResult.newBuilder()
                        .setPhoneCall(base.toBuilder().setState(BotgramRpc.PhoneCallState.PHONE_CALL_STATE_ACTIVE)
                                .setStartDateMs(NOW).setP2PAllowed(true).addConnections(connection)
                                .setGAOrB(com.google.protobuf.ByteString.copyFrom(new byte[] {6, 7}))
                                .setKeyFingerprint(123L))),
                new TL_phone.confirmCall(), TL_phone.TL_phone_phoneCall.class);
        check(active.phone_call.getClass() == TL_phone.TL_phoneCall.class, "active call constructor");
        check(active.phone_call.p2p_allowed && active.phone_call.start_date == NOW / 1000
                && active.phone_call.key_fingerprint == 123L && active.phone_call.connections.size() == 1
                && java.util.Arrays.equals(active.phone_call.g_a_or_b, new byte[] {6, 7}), "active call fields");
        TLRPC.PhoneConnection wireConnection = active.phone_call.connections.get(0);
        check(wireConnection.getClass() == TLRPC.TL_phoneConnectionWebrtc.class && wireConnection.turn
                && !wireConnection.stun && wireConnection.id == 9L && wireConnection.port == 8443
                && "127.0.0.1".equals(wireConnection.ip) && "::1".equals(wireConnection.ipv6)
                && "user".equals(wireConnection.username), "call connection fields");

        BotgramRpc.PhoneCall discarded = base.toBuilder()
                .setState(BotgramRpc.PhoneCallState.PHONE_CALL_STATE_DISCARDED)
                .setDiscardReason(BotgramRpc.PhoneCallDiscardReason.PHONE_CALL_DISCARD_REASON_BUSY)
                .setDuration(12).build();
        BotgramRpc.Updates discardedUpdates = BotgramRpc.Updates.newBuilder().setDateMs(NOW)
                .addUpdates(BotgramRpc.Update.newBuilder().setPhoneCall(
                        BotgramRpc.PhoneCallChanged.newBuilder().setPhoneCall(discarded))).build();
        TLRPC.TL_updates discardResult = (TLRPC.TL_updates) roundTrip(
                envelope().setDiscardCall(discardedUpdates), new TL_phone.discardCall(), TLRPC.TL_updates.class);
        TL_update.TL_updatePhoneCall callUpdate = (TL_update.TL_updatePhoneCall) discardResult.updates.get(0);
        check(callUpdate.phone_call.getClass() == TL_phone.TL_phoneCallDiscarded.class
                && callUpdate.phone_call.reason.getClass() == TLRPC.TL_phoneCallDiscardReasonBusy.class
                && callUpdate.phone_call.duration == 12 && callUpdate.phone_call.video, "discarded call update");

        BotgramRpc.Difference difference = BotgramRpc.Difference.newBuilder()
                .setComplete(BotgramRpc.DifferencePage.newBuilder().setState(STATE)
                        .addUpdates(BotgramRpc.Update.newBuilder().setPhoneCallSignaling(
                                BotgramRpc.PhoneCallSignalingData.newBuilder().setCallId(555L)
                                        .setData(com.google.protobuf.ByteString.copyFrom(new byte[] {9, 8})))))
                .build();
        TLRPC.TL_updates_difference differenceResult = (TLRPC.TL_updates_difference) roundTrip(
                envelope().setGetDifference(difference), new TLRPC.TL_updates_getDifference(),
                TLRPC.TL_updates_difference.class);
        TL_update.TL_updatePhoneCallSignalingData signaling =
                (TL_update.TL_updatePhoneCallSignalingData) differenceResult.other_updates.get(0);
        check(signaling.phone_call_id == 555L && java.util.Arrays.equals(signaling.data, new byte[] {9, 8}),
                "call signaling update");

        TLRPC.Bool receivedResult = (TLRPC.Bool) roundTrip(envelope().setReceivedCall(
                BotgramRpc.BoolValue.newBuilder().setValue(true)), new TL_phone.receivedCall(), TLRPC.TL_boolTrue.class);
        check(receivedResult instanceof TLRPC.TL_boolTrue, "receivedCall bool");
        roundTrip(envelope().setSendSignalingData(BotgramRpc.BoolValue.newBuilder().setValue(true)),
                new TL_phone.sendSignalingData(), TLRPC.TL_boolTrue.class);
        roundTrip(envelope().setSaveCallDebug(BotgramRpc.BoolValue.newBuilder().setValue(true)),
                new TL_phone.saveCallDebug(), TLRPC.TL_boolTrue.class);
        TLRPC.TL_dataJSON configResult = (TLRPC.TL_dataJSON) roundTrip(envelope().setGetCallConfig(
                BotgramRpc.DataJson.newBuilder().setData("{}")), new TL_phone.getCallConfig(), TLRPC.TL_dataJSON.class);
        check("{}".equals(configResult.data), "call config data");

        TLRPC.TL_inputPhoneCall peer = new TLRPC.TL_inputPhoneCall();
        peer.id = 555L;
        peer.access_hash = 77L;
        TL_phone.requestCall outgoing = new TL_phone.requestCall();
        outgoing.video = true;
        TLRPC.TL_inputUser callee = new TLRPC.TL_inputUser();
        callee.user_id = USER_ID;
        outgoing.user_id = callee;
        outgoing.random_id = 4242;
        outgoing.g_a_hash = new byte[] {1, 2};
        TL_phone.TL_phoneCallProtocol outgoingProtocol = new TL_phone.TL_phoneCallProtocol();
        outgoingProtocol.udp_p2p = true;
        outgoingProtocol.udp_reflector = true;
        outgoingProtocol.min_layer = 65;
        outgoingProtocol.max_layer = 92;
        outgoingProtocol.library_versions.add("10.0.0");
        outgoing.protocol = outgoingProtocol;
        BotgramRpc.PhoneRequestCallRequest requestWire = requests.map(outgoing, "request-call").getRequestCall();
        check(requestWire.getVideo() && requestWire.getRandomId() == 4242
                && java.util.Arrays.equals(requestWire.getGAHash().toByteArray(), new byte[] {1, 2})
                && requestWire.getProtocol().getUdpP2P() && requestWire.getProtocol().getMinLayer() == 65
                && requestWire.getProtocol().getLibraryVersionsList().equals(java.util.Collections.singletonList("10.0.0")),
                "requestCall mapping");

        TL_phone.discardCall hangup = new TL_phone.discardCall();
        hangup.peer = peer;
        hangup.duration = 12;
        hangup.reason = new TLRPC.TL_phoneCallDiscardReasonBusy();
        BotgramRpc.PhoneDiscardCallRequest discardWire = requests.map(hangup, "discard-call").getDiscardCall();
        check(discardWire.getPeer().getCallId() == 555L && discardWire.getPeer().getAccessHash() == 77L
                && discardWire.getDuration() == 12
                && discardWire.getReason() == BotgramRpc.PhoneCallDiscardReason.PHONE_CALL_DISCARD_REASON_BUSY,
                "discardCall mapping");

        TL_phone.sendSignalingData outgoingSignal = new TL_phone.sendSignalingData();
        outgoingSignal.peer = peer;
        outgoingSignal.data = new byte[] {7};
        BotgramRpc.PhoneSendSignalingDataRequest signalWire =
                requests.map(outgoingSignal, "signal").getSendSignalingData();
        check(signalWire.getPeer().getCallId() == 555L
                && java.util.Arrays.equals(signalWire.getData().toByteArray(), new byte[] {7}), "sendSignalingData mapping");

        TL_phone.saveCallDebug outgoingDebug = new TL_phone.saveCallDebug();
        outgoingDebug.peer = peer;
        outgoingDebug.debug = new TLRPC.TL_dataJSON();
        outgoingDebug.debug.data = "{\"log\":true}";
        check(requests.map(outgoingDebug, "call-debug").getSaveCallDebug().getDebug().equals("{\"log\":true}"),
                "saveCallDebug mapping");
        check(requests.map(new TL_phone.getCallConfig(), "call-config").hasGetCallConfig(), "getCallConfig mapping");
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
        // The client now negotiates over the Protobuf auth plane. Every legacy
        // JSON ingress must fail with the same explicit 410 code.
        AtomicInteger authCalls = new AtomicInteger();
        AtomicInteger retiredCalls = new AtomicInteger();
        AtomicInteger unexpectedRpcCalls = new AtomicInteger();
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/api/v1/auth", exchange -> {
            authCalls.incrementAndGet();
            byte[] requestBytes = exchange.getRequestBody().readAllBytes();
            BotgramRpc.RpcRequest request = BotgramRpc.RpcRequest.parseFrom(requestBytes);
            String bearer = exchange.getRequestHeaders().getFirst("Authorization");
            boolean invalid = "Bearer invalid-session".equals(bearer);
            BotgramRpc.AuthCapabilities caps = BotgramRpc.AuthCapabilities.newBuilder()
                    .setNegotiationVersion(1)
                    .addProtocols("protobuf-v1")
                    .setSelectedProtocol(invalid ? "tl-json-v1" : "protobuf-v1")
                    .addCapabilities("send-random-id-idempotency-v1")
                    .addEventSubprotocols("botgram.event-hint.v1.protobuf")
                    .setJsonApiStatus("BOTGRAM_JSON_API_RETIRED")
                    .build();
            byte[] bytes = BotgramRpc.RpcResponse.newBuilder().setProtocolMajor(1)
                    .setRequestId(request.getRequestId()).setAuthCapabilities(caps).build().toByteArray();
            exchange.getResponseHeaders().set("Content-Type", "application/x-protobuf");
            exchange.sendResponseHeaders(200, bytes.length);
            exchange.getResponseBody().write(bytes);
            exchange.close();
        });
        server.createContext("/api/protocol/capabilities", exchange -> {
            retiredCalls.incrementAndGet();
            byte[] bytes = "BOTGRAM_JSON_API_RETIRED\nstatus=410\n".getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().set("Content-Type", "text/plain; charset=utf-8");
            exchange.getResponseHeaders().set("X-Botgram-Json-Api-Retired", "BOTGRAM_JSON_API_RETIRED");
            exchange.sendResponseHeaders(410, bytes.length);
            exchange.getResponseBody().write(bytes);
            exchange.close();
        });
        server.createContext("/api/telegram/rpc", exchange -> {
            retiredCalls.incrementAndGet();
            byte[] bytes = "BOTGRAM_JSON_API_RETIRED\nstatus=410\n".getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().set("Content-Type", "text/plain; charset=utf-8");
            exchange.getResponseHeaders().set("X-Botgram-Json-Api-Retired", "BOTGRAM_JSON_API_RETIRED");
            exchange.sendResponseHeaders(410, bytes.length);
            exchange.getResponseBody().write(bytes);
            exchange.close();
        });
        server.createContext("/api/v1/rpc", exchange -> {
            unexpectedRpcCalls.incrementAndGet();
            exchange.sendResponseHeaders(500, -1);
            exchange.close();
        });
        server.start();
        try {
            String url = "http://127.0.0.1:" + server.getAddress().getPort();
            BotgramApi proto = new BotgramApi(url, "proto-session");
            check("protobuf-v1".equals(proto.negotiate()), "protobuf session negotiated over /api/v1/auth");
            check("protobuf-v1".equals(proto.negotiate()) && authCalls.get() == 1, "session protocol pinned without refetch");
            rejects("non-protobuf capability advertisement fails closed", 502,
                    () -> new BotgramApi(url, "invalid-session").negotiate());
            check("protobuf-v1".equals(proto.negotiate()) && authCalls.get() == 2,
                    "another session's failure does not invalidate this pin");
            rejects("retired capability discovery is rejected", 410,
                    () -> new BotgramApi(url, "proto-session").object("GET", "/api/protocol/capabilities", null));
            rejects("retired JSON TL-RPC is rejected without JSON fallback", 410,
                    () -> new BotgramApi(url, "proto-session").object("POST", "/api/telegram/rpc", new org.json.JSONObject()));
            check(retiredCalls.get() == 2 && unexpectedRpcCalls.get() == 0,
                    "410 does not retry on the Protobuf business endpoint");
            check("protobuf-v1".equals(proto.negotiate()) && authCalls.get() == 2,
                    "410 preserves the pinned Protobuf protocol");
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

    private void expectOperation(TLObject request, BotgramRpc.RpcRequest.OperationCase expected) throws Exception {
        check(requests.map(request, "batch-a").getOperationCase() == expected, expected + " request mapping");
    }

    /** Batch A: every messages.* request maps to its own operation and every response
     *  round-trips through the original Android TL classes with stable bytes. */
    private void batchAChecks() throws Exception {
        TLRPC.TL_inputPeerSelf selfPeer = new TLRPC.TL_inputPeerSelf();
        TLRPC.TL_inputDialogPeer selfDialog = new TLRPC.TL_inputDialogPeer();
        selfDialog.peer = new TLRPC.TL_inputPeerSelf();

        TLRPC.TL_messages_deleteMessages deleteMessages = new TLRPC.TL_messages_deleteMessages();
        deleteMessages.revoke = true; deleteMessages.id.add(17); deleteMessages.id.add(18);
        expectOperation(deleteMessages, BotgramRpc.RpcRequest.OperationCase.DELETE_MESSAGES);
        BotgramRpc.DeleteMessagesRequest mappedDelete = requests.map(deleteMessages, "batch-a").getDeleteMessages();
        check(mappedDelete.getRevoke() && mappedDelete.getMessageIdsCount() == 2
                && mappedDelete.getMessageIds(1) == 18, "delete messages ids and revoke");

        TLRPC.TL_messages_deleteHistory deleteHistory = new TLRPC.TL_messages_deleteHistory();
        deleteHistory.peer = selfPeer; deleteHistory.just_clear = true; deleteHistory.revoke = true; deleteHistory.max_id = 17;
        expectOperation(deleteHistory, BotgramRpc.RpcRequest.OperationCase.DELETE_HISTORY);
        TLRPC.TL_messages_deleteSavedHistory deleteSaved = new TLRPC.TL_messages_deleteSavedHistory();
        deleteSaved.peer = selfPeer; deleteSaved.max_id = 17;
        expectOperation(deleteSaved, BotgramRpc.RpcRequest.OperationCase.DELETE_SAVED_HISTORY);
        expectOperation(new TLRPC.TL_messages_deletePhoneCallHistory(), BotgramRpc.RpcRequest.OperationCase.DELETE_PHONE_CALL_HISTORY);
        TLRPC.TL_messages_editMessage edit = new TLRPC.TL_messages_editMessage();
        edit.peer = selfPeer; edit.id = 17; edit.message = "edited";
        expectOperation(edit, BotgramRpc.RpcRequest.OperationCase.EDIT_MESSAGE);
        TLRPC.TL_messages_getMessageEditData editData = new TLRPC.TL_messages_getMessageEditData();
        editData.peer = selfPeer; editData.id = 17;
        expectOperation(editData, BotgramRpc.RpcRequest.OperationCase.GET_MESSAGE_EDIT_DATA);
        TLRPC.TL_messages_forwardMessages forward = new TLRPC.TL_messages_forwardMessages();
        forward.from_peer = selfPeer; forward.to_peer = selfPeer; forward.id.add(17); forward.random_id.add(RANDOM_ID);
        expectOperation(forward, BotgramRpc.RpcRequest.OperationCase.FORWARD_MESSAGES);
        TLRPC.TL_messages_setTyping typing = new TLRPC.TL_messages_setTyping();
        typing.peer = selfPeer; typing.action = new TLRPC.TL_sendMessageRecordAudioAction();
        expectOperation(typing, BotgramRpc.RpcRequest.OperationCase.SET_TYPING);
        check(requests.map(typing, "batch-a").getSetTyping().getAction() == BotgramRpc.TypingAction.RECORD_AUDIO,
                "typing action carries the concrete kind");
        TLRPC.TL_messages_saveDraft draft = new TLRPC.TL_messages_saveDraft();
        draft.peer = selfPeer; draft.message = "draft text";
        expectOperation(draft, BotgramRpc.RpcRequest.OperationCase.SAVE_DRAFT);
        expectOperation(new TLRPC.TL_messages_clearAllDrafts(), BotgramRpc.RpcRequest.OperationCase.CLEAR_ALL_DRAFTS);
        expectOperation(new TLRPC.TL_messages_getAllDrafts(), BotgramRpc.RpcRequest.OperationCase.GET_ALL_DRAFTS);
        TLRPC.TL_messages_updatePinnedMessage pin = new TLRPC.TL_messages_updatePinnedMessage();
        pin.peer = selfPeer; pin.id = 17;
        expectOperation(pin, BotgramRpc.RpcRequest.OperationCase.UPDATE_PINNED_MESSAGE);
        TLRPC.TL_messages_unpinAllMessages unpin = new TLRPC.TL_messages_unpinAllMessages();
        unpin.peer = selfPeer;
        expectOperation(unpin, BotgramRpc.RpcRequest.OperationCase.UNPIN_ALL_MESSAGES);
        TLRPC.TL_messages_toggleDialogPin togglePin = new TLRPC.TL_messages_toggleDialogPin();
        togglePin.peer = selfDialog; togglePin.pinned = true;
        expectOperation(togglePin, BotgramRpc.RpcRequest.OperationCase.TOGGLE_DIALOG_PIN);
        TLRPC.TL_messages_reorderPinnedDialogs reorder = new TLRPC.TL_messages_reorderPinnedDialogs();
        reorder.order.add(selfDialog);
        expectOperation(reorder, BotgramRpc.RpcRequest.OperationCase.REORDER_PINNED_DIALOGS);
        TLRPC.TL_messages_markDialogUnread markUnread = new TLRPC.TL_messages_markDialogUnread();
        markUnread.peer = selfDialog; markUnread.unread = true;
        expectOperation(markUnread, BotgramRpc.RpcRequest.OperationCase.MARK_DIALOG_UNREAD);
        expectOperation(new TLRPC.TL_messages_getDialogUnreadMarks(), BotgramRpc.RpcRequest.OperationCase.GET_DIALOG_UNREAD_MARKS);
        TLRPC.TL_messages_readMessageContents contents = new TLRPC.TL_messages_readMessageContents();
        contents.id.add(17);
        expectOperation(contents, BotgramRpc.RpcRequest.OperationCase.READ_MESSAGE_CONTENTS);
        TLRPC.TL_messages_getOnlines onlines = new TLRPC.TL_messages_getOnlines();
        onlines.peer = selfPeer;
        expectOperation(onlines, BotgramRpc.RpcRequest.OperationCase.GET_ONLINES);
        TLRPC.TL_messages_getCommonChats common = new TLRPC.TL_messages_getCommonChats();
        common.user_id = new TLRPC.TL_inputUserSelf(); common.max_id = 17; common.limit = 10;
        expectOperation(common, BotgramRpc.RpcRequest.OperationCase.GET_COMMON_CHATS);
        TLRPC.TL_messages_hidePeerSettingsBar hideBar = new TLRPC.TL_messages_hidePeerSettingsBar();
        hideBar.peer = selfPeer;
        expectOperation(hideBar, BotgramRpc.RpcRequest.OperationCase.HIDE_PEER_SETTINGS_BAR);
        TLRPC.TL_messages_reportSpam report = new TLRPC.TL_messages_reportSpam();
        report.peer = selfPeer;
        expectOperation(report, BotgramRpc.RpcRequest.OperationCase.REPORT_SPAM);
        TLRPC.TL_messages_getMessagesViews views = new TLRPC.TL_messages_getMessagesViews();
        views.peer = selfPeer; views.id.add(17); views.increment = true;
        expectOperation(views, BotgramRpc.RpcRequest.OperationCase.GET_MESSAGES_VIEWS);
        TLRPC.TL_messages_getOutboxReadDate readDate = new TLRPC.TL_messages_getOutboxReadDate();
        readDate.peer = selfPeer; readDate.msg_id = 17;
        expectOperation(readDate, BotgramRpc.RpcRequest.OperationCase.GET_OUTBOX_READ_DATE);
        TLRPC.TL_messages_search search = new TLRPC.TL_messages_search();
        search.peer = selfPeer; search.q = "alpha"; search.filter = new TLRPC.TL_inputMessagesFilterEmpty();
        search.limit = 21; search.min_date = 100; search.offset_id = 17;
        expectOperation(search, BotgramRpc.RpcRequest.OperationCase.SEARCH);
        BotgramRpc.SearchRequest mappedSearch = requests.map(search, "batch-a").getSearch();
        check(mappedSearch.getFilter() == BotgramRpc.SearchFilter.EMPTY && mappedSearch.getMinDateMs() == 100000L
                && mappedSearch.getLimit() == 21, "search query, filter and date conversion");
        TLRPC.TL_messages_search pinnedSearch = new TLRPC.TL_messages_search();
        pinnedSearch.peer = selfPeer; pinnedSearch.filter = new TLRPC.TL_inputMessagesFilterPinned();
        check(requests.map(pinnedSearch, "batch-a").getSearch().getFilter() == BotgramRpc.SearchFilter.PINNED,
                "pinned search filter");
        TLRPC.TL_messages_getSearchCounters counters = new TLRPC.TL_messages_getSearchCounters();
        counters.peer = selfPeer; counters.filters.add(new TLRPC.TL_inputMessagesFilterEmpty());
        counters.filters.add(new TLRPC.TL_inputMessagesFilterPinned());
        expectOperation(counters, BotgramRpc.RpcRequest.OperationCase.GET_SEARCH_COUNTERS);
        check(requests.map(counters, "batch-a").getGetSearchCounters().getFiltersCount() == 2,
                "search counters keep the requested filter order");

        BotgramRpc.Message text = message().build();
        BotgramRpc.MessageIdMapping mapping = BotgramRpc.MessageIdMapping.newBuilder().setRandomId(RANDOM_ID).setMessageId(17).build();
        BotgramRpc.AffectedMessages affected = BotgramRpc.AffectedMessages.newBuilder().setPts(9).setPtsCount(1).build();
        roundTrip(envelope().setDeleteMessages(affected), new TLRPC.TL_messages_deleteMessages(), TLRPC.TL_messages_affectedMessages.class);
        BotgramRpc.AffectedHistory history = BotgramRpc.AffectedHistory.newBuilder().setPts(9).setPtsCount(1).build();
        roundTrip(envelope().setDeleteHistory(history), new TLRPC.TL_messages_deleteHistory(), TLRPC.TL_messages_affectedHistory.class);
        roundTrip(envelope().setDeleteSavedHistory(history), new TLRPC.TL_messages_deleteSavedHistory(), TLRPC.TL_messages_affectedHistory.class);
        roundTrip(envelope().setUnpinAllMessages(history), new TLRPC.TL_messages_unpinAllMessages(), TLRPC.TL_messages_affectedHistory.class);
        TLRPC.TL_messages_affectedHistory zeroOffset = (TLRPC.TL_messages_affectedHistory) roundTrip(
                envelope().setDeleteHistory(history), new TLRPC.TL_messages_deleteHistory(), TLRPC.TL_messages_affectedHistory.class);
        check(zeroOffset.offset == 0 && zeroOffset.pts == 9, "history deletion terminates the client retry loop");
        BotgramRpc.AffectedFoundMessages found = BotgramRpc.AffectedFoundMessages.newBuilder()
                .setPts(9).setPtsCount(0).addMessages(17).build();
        roundTrip(envelope().setDeletePhoneCallHistory(found), new TLRPC.TL_messages_deletePhoneCallHistory(), TLRPC.TL_messages_affectedFoundMessages.class);
        roundTrip(envelope().setGetMessageEditData(BotgramRpc.MessageEditData.newBuilder().setCaption(true)),
                new TLRPC.TL_messages_getMessageEditData(), TLRPC.TL_messages_messageEditData.class);
        for (int i = 0; i < 8; i++) {
            BotgramRpc.BoolValue applied = BotgramRpc.BoolValue.newBuilder().setValue(true).build();
            TLObject request = i == 0 ? new TLRPC.TL_messages_setTyping() : i == 1 ? new TLRPC.TL_messages_saveDraft()
                    : i == 2 ? new TLRPC.TL_messages_clearAllDrafts() : i == 3 ? new TLRPC.TL_messages_toggleDialogPin()
                    : i == 4 ? new TLRPC.TL_messages_reorderPinnedDialogs() : i == 5 ? new TLRPC.TL_messages_markDialogUnread()
                    : i == 6 ? new TLRPC.TL_messages_hidePeerSettingsBar() : new TLRPC.TL_messages_reportSpam();
            BotgramRpc.RpcResponse.Builder builder = i == 0 ? envelope().setSetTyping(applied)
                    : i == 1 ? envelope().setSaveDraft(applied) : i == 2 ? envelope().setClearAllDrafts(applied)
                    : i == 3 ? envelope().setToggleDialogPin(applied) : i == 4 ? envelope().setReorderPinnedDialogs(applied)
                    : i == 5 ? envelope().setMarkDialogUnread(applied) : i == 6 ? envelope().setHidePeerSettingsBar(applied)
                    : envelope().setReportSpam(applied);
            roundTrip(builder, request, TLRPC.TL_boolTrue.class);
        }
        roundTrip(envelope().setGetOnlines(BotgramRpc.ChatOnlines.newBuilder().setOnlines(3)),
                new TLRPC.TL_messages_getOnlines(), TLRPC.TL_chatOnlines.class);
        roundTrip(envelope().setGetCommonChats(BotgramRpc.GroupList.newBuilder().addGroups(GROUP_SUMMARY)),
                new TLRPC.TL_messages_getCommonChats(), TLRPC.TL_messages_chats.class);
        roundTrip(envelope().setGetMessagesViews(BotgramRpc.MessageViewsList.newBuilder().addViews(5).addViews(0)),
                new TLRPC.TL_messages_getMessagesViews(), TLRPC.TL_messages_messageViews.class);
        roundTrip(envelope().setGetOutboxReadDate(BotgramRpc.OutboxReadDate.newBuilder().setDateMs(NOW)),
                new TLRPC.TL_messages_getOutboxReadDate(), TLRPC.TL_outboxReadDate.class);
        roundTrip(envelope().setGetDialogUnreadMarks(BotgramRpc.PeerList.newBuilder().addPeers(SELF)),
                new TLRPC.TL_messages_getDialogUnreadMarks(), org.telegram.tgnet.Vector.class);
        TLRPC.messages_Messages searched = (TLRPC.messages_Messages) roundTrip(envelope().setSearch(
                BotgramRpc.SearchPage.newBuilder().setCount(1).addMessages(text).addUsers(SUMMARY).addGroups(GROUP_SUMMARY)),
                new TLRPC.TL_messages_search(), TLRPC.TL_messages_messagesSlice.class);
        check(searched.count == 1 && searched.messages.size() == 1, "search page carries count and messages");
        roundTrip(envelope().setGetSearchCounters(BotgramRpc.SearchCounters.newBuilder()
                        .addCounters(BotgramRpc.SearchCounter.newBuilder().setFilter(BotgramRpc.SearchFilter.EMPTY).setCount(1))
                        .addCounters(BotgramRpc.SearchCounter.newBuilder().setFilter(BotgramRpc.SearchFilter.PINNED))),
                new TLRPC.TL_messages_getSearchCounters(), org.telegram.tgnet.Vector.class);

        BotgramRpc.Updates edits = BotgramRpc.Updates.newBuilder().addUsers(SUMMARY).addGroups(GROUP_SUMMARY).setDateMs(NOW)
                .addUpdates(BotgramRpc.Update.newBuilder().setMessageEdited(BotgramRpc.MessageEdited.newBuilder()
                        .setMessage(text).setPts(9).setPtsCount(1)).build()).build();
        TLRPC.TL_updates editEnvelope = (TLRPC.TL_updates) roundTrip(envelope().setEditMessage(edits),
                new TLRPC.TL_messages_editMessage(), TLRPC.TL_updates.class);
        check(editEnvelope.updates.get(0) instanceof org.telegram.tgnet.tl.TL_update.TL_updateEditMessage,
                "edit update kind");
        BotgramRpc.Updates forwarded = BotgramRpc.Updates.newBuilder().addUsers(SUMMARY).addGroups(GROUP_SUMMARY).setDateMs(NOW)
                .addUpdates(BotgramRpc.Update.newBuilder().setMessageId(mapping))
                .addUpdates(BotgramRpc.Update.newBuilder().setNewMessage(BotgramRpc.NewMessage.newBuilder()
                        .setMessage(text).setPts(9).setPtsCount(1)).build()).build();
        TLRPC.TL_updates forwardEnvelope = (TLRPC.TL_updates) roundTrip(envelope().setForwardMessages(forwarded),
                new TLRPC.TL_messages_forwardMessages(), TLRPC.TL_updates.class);
        check(forwardEnvelope.updates.size() == 2
                && forwardEnvelope.updates.get(0) instanceof org.telegram.tgnet.tl.TL_update.TL_updateMessageID,
                "forward response keeps the random id mapping");

        BotgramRpc.DifferencePage.Builder differencePage = BotgramRpc.DifferencePage.newBuilder().setState(STATE).addMessages(text)
                .addUpdates(BotgramRpc.Update.newBuilder().setMessagesDeleted(BotgramRpc.MessagesDeleted.newBuilder()
                        .addMessageIds(17).setPts(8).setPtsCount(1)).build())
                .addUpdates(BotgramRpc.Update.newBuilder().setMessagesPinned(BotgramRpc.MessagesPinned.newBuilder()
                        .setPeer(SELF).addMessageIds(17).setPinned(true).setPts(9).setPtsCount(1)).build())
                .addUpdates(BotgramRpc.Update.newBuilder().setContentsRead(BotgramRpc.ContentsRead.newBuilder()
                        .addMessageIds(17).setPts(10).setPtsCount(1)).build())
                .addUpdates(BotgramRpc.Update.newBuilder().setDraftChanged(BotgramRpc.DraftChanged.newBuilder()
                        .setPeer(SELF).setDraft(BotgramRpc.DraftState.newBuilder().setText("draft").setUpdatedAtMs(NOW))).build())
                .addUpdates(BotgramRpc.Update.newBuilder().setUserTyping(BotgramRpc.UserTyping.newBuilder()
                        .setPeer(SELF).setUser(USER).setAction(BotgramRpc.TypingAction.TYPING)).build());
        TLRPC.TL_updates_difference difference = (TLRPC.TL_updates_difference) roundTrip(
                envelope().setGetDifference(BotgramRpc.Difference.newBuilder().setComplete(differencePage.build())),
                new TLRPC.TL_updates_getDifference(), TLRPC.TL_updates_difference.class);
        check(difference.other_updates.get(0) instanceof org.telegram.tgnet.tl.TL_update.TL_updateDeleteMessages
                && difference.other_updates.get(1) instanceof org.telegram.tgnet.tl.TL_update.TL_updatePinnedMessages
                && difference.other_updates.get(2) instanceof org.telegram.tgnet.tl.TL_update.TL_updateReadMessagesContents
                && difference.other_updates.get(3) instanceof org.telegram.tgnet.tl.TL_update.TL_updateDraftMessage
                && difference.other_updates.get(4) instanceof org.telegram.tgnet.tl.TL_update.TL_updateUserTyping,
                "difference replays all batch A update kinds");

        BotgramRpc.Dialog stateDialog = BotgramRpc.Dialog.newBuilder().setPeer(SELF).setTopMessageId(17)
                .setReadInboxMaxId(16).setReadOutboxMaxId(16).setNotifySettings(NOTIFY).setPinned(true).setUnreadMark(true)
                .setDraft(BotgramRpc.DraftState.newBuilder().setText("draft").setUpdatedAtMs(NOW).setReplyToMessageId(16)).build();
        TLRPC.TL_messages_peerDialogs pinnedDialogs = (TLRPC.TL_messages_peerDialogs) roundTrip(
                envelope().setGetPinnedDialogs(BotgramRpc.PeerDialogs.newBuilder().addDialogs(stateDialog)
                        .addMessages(text).addUsers(SUMMARY).addGroups(GROUP_SUMMARY).setState(STATE)),
                new TLRPC.TL_messages_getPinnedDialogs(), TLRPC.TL_messages_peerDialogs.class);
        TLRPC.TL_dialog state = (TLRPC.TL_dialog) pinnedDialogs.dialogs.get(0);
        check(state.pinned && state.unread_mark && state.draft != null && "draft".equals(state.draft.message)
                && state.draft.reply_to != null, "dialog carries pin, unread mark and draft");

        BotgramRpc.Message forwardedMessage = message().setForwardFrom(USER).setForwardFromDateMs(NOW - 1000).build();
        TLRPC.messages_Messages forwardedHistory = (TLRPC.messages_Messages) roundTrip(
                envelope().setGetHistory(page(forwardedMessage.toBuilder())),
                new TLRPC.TL_messages_getHistory(), TLRPC.TL_messages_messages.class);
        TLRPC.Message withHeader = forwardedHistory.messages.get(0);
        check(withHeader.fwd_from != null && withHeader.fwd_from.from_id != null
                && withHeader.fwd_from.date == (int) ((NOW - 1000) / 1000), "forward attribution round-trip");
    }

    /** Batch I: media requests map to typed sources and media messages round-trip as TL. */
    private void batchIChecks() throws Exception {
        TLRPC.TL_inputMediaUploadedPhoto uploaded = new TLRPC.TL_inputMediaUploadedPhoto();
        uploaded.file = new TLRPC.TL_inputFile();
        uploaded.file.id = 4242;
        uploaded.file.parts = 3;
        uploaded.file.name = "photo.jpg";
        TLRPC.TL_messages_sendMedia send = new TLRPC.TL_messages_sendMedia();
        send.peer = new TLRPC.TL_inputPeerSelf();
        send.media = uploaded;
        send.message = "caption";
        send.random_id = RANDOM_ID;
        BotgramRpc.SendMediaRequest mapped = requests.map(send, "batch-i").getSendMedia();
        check(mapped.getMedia().getClientFileId() == 4242
                && mapped.getMedia().getKind() == BotgramRpc.MediaKind.PHOTO, "uploaded photo request");
        check("caption".equals(mapped.getCaption()) && mapped.getRandomId() == RANDOM_ID,
                "media caption and random id");

        TLRPC.TL_inputMediaPhoto existing = new TLRPC.TL_inputMediaPhoto();
        existing.id = new TLRPC.TL_inputPhoto();
        existing.id.id = 77;
        existing.id.access_hash = 1;
        send.media = existing;
        check(requests.map(send, "batch-i").getSendMedia().getMedia().getMediaId() == 77,
                "existing media request");

        BotgramRpc.MediaRef photo = BotgramRpc.MediaRef.newBuilder().setMediaId(901)
                .setKind(BotgramRpc.MediaKind.PHOTO).setMime("image/jpeg").setByteSize(240)
                .setWidth(640).setHeight(480).setTotalParts(3).build();
        BotgramRpc.Message photoMessage = message()
                .setMedia(BotgramRpc.MediaMessage.newBuilder().setMedia(photo)).build();
        BotgramRpc.Updates photoUpdates = BotgramRpc.Updates.newBuilder().addUsers(SUMMARY)
                .addGroups(GROUP_SUMMARY).setDateMs(NOW)
                .addUpdates(BotgramRpc.Update.newBuilder().setNewMessage(
                        BotgramRpc.NewMessage.newBuilder().setMessage(photoMessage).setPts(9).setPtsCount(1)))
                .build();
        TLRPC.TL_updates photoEnvelope = (TLRPC.TL_updates) roundTrip(
                envelope().setSendMedia(photoUpdates), new TLRPC.TL_messages_sendMedia(), TLRPC.TL_updates.class);
        TLRPC.Message photoResult = ((org.telegram.tgnet.tl.TL_update.TL_updateNewMessage)
                photoEnvelope.updates.get(0)).message;
        check(photoResult.media instanceof TLRPC.TL_messageMediaPhoto
                && photoResult.media.photo != null && photoResult.media.photo.id == 901, "photo media round-trip");
        // TL_photoSize carries type/w/h/size on the wire; the location is rebuilt from the
        // photo id by the deserializer, which is also what the download interceptor reads.
        check(photoResult.media.photo.sizes.size() == 1
                && "y".equals(photoResult.media.photo.sizes.get(0).type)
                && photoResult.media.photo.sizes.get(0).w == 640
                && photoResult.media.photo.sizes.get(0).h == 480
                && photoResult.media.photo.sizes.get(0).size == 240
                && photoResult.media.photo.sizes.get(0).location != null,
                "photo size round-trip");

        BotgramRpc.MediaRef video = BotgramRpc.MediaRef.newBuilder().setMediaId(902)
                .setKind(BotgramRpc.MediaKind.DOCUMENT).setMime("video/mp4").setByteSize(2048)
                .setWidth(1280).setHeight(720).setDuration(12).setFileName("clip.mp4").setTotalParts(4).build();
        BotgramRpc.Message videoMessage = message()
                .setMedia(BotgramRpc.MediaMessage.newBuilder().setMedia(video)).setGroupedId(555).build();
        BotgramRpc.Updates videoUpdates = BotgramRpc.Updates.newBuilder().addUsers(SUMMARY)
                .addGroups(GROUP_SUMMARY).setDateMs(NOW)
                .addUpdates(BotgramRpc.Update.newBuilder().setNewMessage(
                        BotgramRpc.NewMessage.newBuilder().setMessage(videoMessage).setPts(9).setPtsCount(1)))
                .build();
        TLRPC.TL_updates videoEnvelope = (TLRPC.TL_updates) roundTrip(
                envelope().setSendMultiMedia(videoUpdates), new TLRPC.TL_messages_sendMultiMedia(), TLRPC.TL_updates.class);
        TLRPC.Message videoResult = ((org.telegram.tgnet.tl.TL_update.TL_updateNewMessage)
                videoEnvelope.updates.get(0)).message;
        check(videoResult.media instanceof TLRPC.TL_messageMediaDocument
                && videoResult.media.document != null && videoResult.media.document.id == 902
                && "video/mp4".equals(videoResult.media.document.mime_type), "document media round-trip");
        check(videoResult.grouped_id == 555, "album id round-trip");
        boolean hasVideo = false, hasName = false;
        for (TLRPC.DocumentAttribute attribute : videoResult.media.document.attributes) {
            if (attribute instanceof TLRPC.TL_documentAttributeVideo) hasVideo = true;
            if (attribute instanceof TLRPC.TL_documentAttributeFilename) hasName = true;
        }
        check(hasVideo && hasName, "document attributes round-trip");

        roundTrip(envelope().setUploadMedia(BotgramRpc.UploadMediaResult.newBuilder().setMedia(photo)),
                new TLRPC.TL_messages_uploadMedia(), TLRPC.TL_messageMediaPhoto.class);
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
            smoke.batchAChecks();
            smoke.batchIChecks();
            identityChecks(api, identity, resolverCalls);
            System.out.println("PASS: 29 application response outcomes plus privacy/security, 15 entities, 4 differences, 5 updates; " + checks + " assertions against original Android TL classes (host only)");
        } finally { server.stop(0); }
    }
}
