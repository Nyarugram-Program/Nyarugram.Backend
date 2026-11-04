namespace Botgram.Backend;

public static class IdentityService
{
    public static long UserId(Database db, string id) => db.TelegramId("user:" + id);
    public static long ChatId(Database db, Conversation conversation) => db.TelegramId("chat:" + conversation.Id);
    public static User FindUser(Database db, long id) => db.Users.FirstOrDefault(user => UserId(db, user.Id) == id)
        ?? throw new ApiException(404, "USER_ID_INVALID");
    public static long DialogId(Database db, Conversation conversation, string uid) => conversation.Kind == "group" ? -ChatId(db, conversation) :
        conversation.Kind == "saved" ? UserId(db, uid) : UserId(db, conversation.Members.First(member => member != uid));
}
