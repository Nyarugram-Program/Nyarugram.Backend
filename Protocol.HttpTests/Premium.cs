using System.Text.Json.Nodes;
using Google.Protobuf;
using P = Botgram.Protocol.V1;

internal static partial class HttpTests
{
    private static async Task PremiumRegression()
    {
        var path = Path.Combine(Root, "premium.json");
        string aliceToken, bobToken, aliceId, bobId;
        await using (var server = await Server.Start(path, legacyEnabled: false, legacyWssEnabled: false))
        {
            var alice = await server.Json("POST", "/api/auth/register",
                new { username = "premium_a", displayName = "Premium A", password = "isolated-premium-password" });
            var bob = await server.Json("POST", "/api/auth/register",
                new { username = "premium_b", displayName = "Premium B", password = "isolated-premium-password" });
            aliceToken = alice["token"]!.GetValue<string>();
            bobToken = bob["token"]!.GetValue<string>();
            aliceId = alice["user"]!["id"]!.GetValue<string>();
            bobId = bob["user"]!["id"]!.GetValue<string>();
            await server.Json("GET", "/api/me/premium", null, status: 401);
            Check(!(await server.Json("GET", "/api/me/premium", null, aliceToken))["active"]!.GetValue<bool>(),
                "Premium defaults to false");
            var before = await server.Json("GET", "/api/telegram/me", null, aliceToken);
            Check(!before["premium"]!.GetValue<bool>() && (before["flags"]!.GetValue<int>() & (1 << 28)) == 0,
                "default TL premium flag");
            Check((await server.Json("POST", "/api/me/premium/activate", new { userId = bobId }, aliceToken))["active"]!.GetValue<bool>(),
                "authenticated free activation");
            for (var i = 0; i < 3; i++)
                Check((await server.Json("POST", "/api/me/premium/activate", new { }, aliceToken))["active"]!.GetValue<bool>(),
                    "Premium repeated activation");
            Check(!(await server.Json("GET", "/api/me/premium", null, bobToken))["active"]!.GetValue<bool>(),
                "activation must not affect another account");
            var tl = await server.Json("GET", "/api/telegram/me", null, aliceToken);
            Check(tl["premium"]!.GetValue<bool>() && (tl["flags"]!.GetValue<int>() & (1 << 28)) != 0,
                "activated TL premium flag");
            var request = new P.RpcRequest
                { ProtocolMajor = 1, RequestId = "premium", GetFullUser = new() { User = new() { Self = true } } };
            var response = await server.Raw(request.ToByteArray(), 200, null, aliceToken);
            Check(response.GetFullUser.Users.Single().HasNyarugramPremium && response.GetFullUser.Users.Single().NyarugramPremium,
                "activated protobuf Premium presence and value");
            response = await server.Raw(request.ToByteArray(), 200, null, bobToken);
            Check(response.GetFullUser.Users.Single().HasNyarugramPremium && !response.GetFullUser.Users.Single().NyarugramPremium,
                "inactive protobuf Premium is explicitly false");
            var originalId = (await server.Json("GET", "/api/me", null, aliceToken))["id"]!.GetValue<string>();
            Check(originalId == aliceId, "activation changed UUID");
            await server.Json("POST", "/api/auth/logout", new { }, aliceToken, 204);
            await server.Json("POST", "/api/me/premium/activate", new { }, aliceToken, 401);
            var login = await server.Json("POST", "/api/auth/login",
                new { username = "premium_a", password = "isolated-premium-password" });
            aliceToken = login["token"]!.GetValue<string>();
        }
        var data = JsonNode.Parse(await File.ReadAllTextAsync(path))!;
        data["users"]!.AsArray().Single(u => u!["id"]!.GetValue<string>() == bobId)!.AsObject().Remove("premium");
        await File.WriteAllTextAsync(path, data.ToJsonString());
        await using (var server = await Server.Start(path, legacyEnabled: false, legacyWssEnabled: false))
        {
            Check((await server.Json("GET", "/api/me/premium", null, aliceToken))["active"]!.GetValue<bool>(),
                "Premium persisted after restart and login");
            Check(!(await server.Json("GET", "/api/me/premium", null, bobToken))["active"]!.GetValue<bool>(),
                "legacy missing Premium field must remain false");
            Check((await server.Json("GET", "/api/protocol/capabilities", null, aliceToken))["selectedProtocol"]!.GetValue<string>() == "protobuf-v1",
                "Premium REST remains available with JSON RPC disabled");
        }
    }
}
