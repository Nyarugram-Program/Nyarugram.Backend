using Google.Protobuf;
using P = Botgram.Protocol.V1;

namespace Botgram.Backend;

/// <summary>
/// Typed execution of the Protobuf authentication plane over either storage
/// backend. Business rules are reused verbatim from the legacy services; only the
/// wire representation changes. No endpoint here accepts or returns JSON.
/// </summary>
public static class AuthPlane
{
    public static bool IsPreAuthentication(P.RpcRequest.OperationOneofCase operation) => operation is
        P.RpcRequest.OperationOneofCase.AuthCapabilities or
        P.RpcRequest.OperationOneofCase.AuthRegister or
        P.RpcRequest.OperationOneofCase.AuthLogin or
        P.RpcRequest.OperationOneofCase.AuthPassword or
        P.RpcRequest.OperationOneofCase.AuthRecover;

    public static bool IsAccount(P.RpcRequest.OperationOneofCase operation) => operation is
        P.RpcRequest.OperationOneofCase.AuthLogout or
        P.RpcRequest.OperationOneofCase.AccountMe or
        P.RpcRequest.OperationOneofCase.Premium or
        P.RpcRequest.OperationOneofCase.ChangeGid or
        P.RpcRequest.OperationOneofCase.Totp;

    public static bool IsAuthenticationPlane(P.RpcRequest.OperationOneofCase operation) =>
        IsPreAuthentication(operation) || IsAccount(operation);

    private static P.AccountProfile Profile(UserProfile user) => new()
    {
        UserId = user.Id, Username = user.Username, DisplayName = user.DisplayName, Gid = user.Gid,
        TlUserId = user.TelegramId, FirstName = user.FirstName, LastName = user.LastName,
        NyarugramPremium = user.Premium, IsSelf = user.IsSelf, IsContact = user.IsContact,
        IsMutualContact = user.IsMutual, Online = user.Online,
        LastSeenMs = user.LastSeen?.ToUnixTimeMilliseconds() ?? 0
    };

    private static P.AuthSession Session(SessionResult result, UserProfile user) => new()
    {
        Token = result.Token, ExpiresAtMs = result.ExpiresAt.ToUnixTimeMilliseconds(), User = Profile(user)
    };

    private static Session Stored(SessionResult result) =>
        new(Auth.HashToken(result.Token), result.User.Id, result.ExpiresAt);

    public static P.RpcResponse CapabilitiesResponse(string requestId) => new()
    {
        ProtocolMajor = 1, RequestId = requestId, AuthCapabilities = Capabilities()
    };

    private static P.AuthCapabilities Capabilities() => new()
    {
        NegotiationVersion = 1,
        Protocols = { "protobuf-v1" },
        SelectedProtocol = "protobuf-v1",
        EventSubprotocols = { JsonApiRetirement.ProtobufSubprotocol },
        Capabilities = { "session-protocol-pinning", "send-random-id-idempotency-v1",
            "difference-pts-v1", "wss-wake-v1", "json-api-retired-v1" },
        JsonApiStatus = JsonApiRetirement.Code
    };

    private static P.TotpResult Totp(object? result) => result switch
    {
        TotpStatus status => new() { Enabled = status.Enabled, RecoveryCodesRemaining = status.RecoveryCodesRemaining },
        TotpEnrollment enrollment => new()
        {
            Enabled = false, Secret = enrollment.Secret, Uri = enrollment.Uri,
            ExpiresAtMs = enrollment.ExpiresAt.ToUnixTimeMilliseconds()
        },
        TotpRecovery recovery => new()
        {
            Enabled = true, RecoveryCodesRemaining = recovery.RecoveryCodes.Length,
            RecoveryCodes = { recovery.RecoveryCodes }
        },
        null => new() { Enabled = false },
        _ => throw new ApiException(500, "BOTGRAM_AUTH_PLANE_RESULT_INVALID")
    };

    private static LoginInput Login(P.AuthLoginRequest input) => new(input.Username, input.Password,
        input.HasPasswordProof ? input.PasswordProof : null, input.HasTotpCode ? input.TotpCode : null);

    public static P.RpcResponse ExecuteLegacy(Database db, Session? session, P.RpcRequest request, AccountTotpService totpService)
    {
        if (session is not null) Auth.Recheck(db, session);
        var response = new P.RpcResponse { ProtocolMajor = 1, RequestId = request.RequestId };
        switch (request.OperationCase)
        {
            case P.RpcRequest.OperationOneofCase.AuthCapabilities:
                response.AuthCapabilities = Capabilities();
                break;
            case P.RpcRequest.OperationOneofCase.AuthRegister:
            {
                var input = request.AuthRegister;
                var register = new RegisterInput(input.Username, input.DisplayName, input.Password,
                    input.HasLocationCountry ? input.LocationCountry : null);
                var user = Auth.PrepareRegistration(register);
                if (db.Users.Any(u => u.Username == user.Username)) throw new ApiException(409, "username_taken");
                user = user with { Gid = Gids.Allocate(db, Gids.FromCountry(register.LocationCountry)) };
                db.Users.Add(user);
                response.AuthRegister = Session(Auth.CreateSession(db, user), UserQueryService.Current(db, user.Id));
                break;
            }
            case P.RpcRequest.OperationOneofCase.AuthLogin:
            case P.RpcRequest.OperationOneofCase.AuthRecover:
            {
                var login = request.OperationCase == P.RpcRequest.OperationOneofCase.AuthRecover
                    ? new LoginInput(request.AuthRecover.Username, request.AuthRecover.Password,
                        TotpCode: request.AuthRecover.RecoveryCode)
                    : Login(request.AuthLogin);
                if (request.OperationCase == P.RpcRequest.OperationOneofCase.AuthRecover &&
                    string.IsNullOrWhiteSpace(request.AuthRecover.RecoveryCode))
                    throw new ApiException(400, "TOTP_CODE_INVALID");
                Auth.ValidateLogin(login);
                var user = db.Users.FirstOrDefault(u => u.Username == login.Username.ToLowerInvariant());
                Auth.VerifyPassword(user, login.Password);
                AccountPasswordService.VerifyLogin(user!, AccountPrivacyService.State(db, user!.Id).Password, login.PasswordProof);
                totpService.Verify(user.Id, AccountPrivacyService.State(db, user.Id).Totp, login.TotpCode);
                var result = Auth.CreateSession(db, user);
                var value = Session(result, UserQueryService.Current(db, user.Id));
                if (request.OperationCase == P.RpcRequest.OperationOneofCase.AuthRecover) response.AuthRecover = value;
                else response.AuthLogin = value;
                break;
            }
            case P.RpcRequest.OperationOneofCase.AuthPassword:
            {
                var input = request.AuthPassword;
                var login = new LoginInput(input.Username, input.Password);
                Auth.ValidateLogin(login);
                var user = db.Users.FirstOrDefault(u => u.Username == login.Username.ToLowerInvariant());
                Auth.VerifyPassword(user, login.Password);
                var state = db.AccountPrivacy?.GetValueOrDefault(user!.Id)?.Password ?? new();
                response.AuthPassword = new()
                {
                    PasswordInfo = ByteString.CopyFrom(AccountPasswordService.LoginInfoBytes(user!, state))
                };
                break;
            }
            case P.RpcRequest.OperationOneofCase.AuthLogout:
                db.Sessions.RemoveAll(s => s.TokenHash == session!.TokenHash);
                response.AuthLogout = new() { Value = true };
                break;
            case P.RpcRequest.OperationOneofCase.AccountMe:
                response.AccountMe = Profile(UserQueryService.Current(db, session!.UserId));
                break;
            case P.RpcRequest.OperationOneofCase.Premium:
            {
                var state = request.Premium.Activate
                    ? PremiumService.Activate(db, session!)
                    : PremiumService.Get(db, session!);
                response.Premium = new P.PremiumStatus { Active = state.Active };
                break;
            }
            case P.RpcRequest.OperationOneofCase.ChangeGid:
                Gids.Change(db, session!.UserId, new(request.ChangeGid.RegionCode, request.ChangeGid.Number));
                response.ChangeGid = Profile(UserQueryService.Current(db, session.UserId));
                break;
            case P.RpcRequest.OperationOneofCase.Totp:
                response.Totp = Totp(totpService.Execute(session!.UserId,
                    db.Users.Single(u => u.Id == session.UserId).Username,
                    AccountPrivacyService.State(db, session.UserId).Totp,
                    request.Totp.Operation, request.Totp.Code));
                break;
            default:
                throw new ApiException(400, "BOTGRAM_AUTH_PLANE_OPERATION_INVALID");
        }
        return response;
    }

    public static async Task<P.RpcResponse> ExecuteRelationalAsync(
        Botgram.Backend.Storage.PostgresBackend backend, Session? session, P.RpcRequest request, CancellationToken token)
    {
        var response = new P.RpcResponse { ProtocolMajor = 1, RequestId = request.RequestId };
        switch (request.OperationCase)
        {
            case P.RpcRequest.OperationOneofCase.AuthCapabilities:
                response.AuthCapabilities = Capabilities();
                break;
            case P.RpcRequest.OperationOneofCase.AuthRegister:
            {
                var input = request.AuthRegister;
                var result = await backend.RegisterAsync(new RegisterInput(input.Username, input.DisplayName,
                    input.Password, input.HasLocationCountry ? input.LocationCountry : null), token);
                response.AuthRegister = Session(result, await backend.CurrentProfileAsync(Stored(result), token));
                break;
            }
            case P.RpcRequest.OperationOneofCase.AuthLogin:
            {
                var result = await backend.LoginAsync(Login(request.AuthLogin), token);
                response.AuthLogin = Session(result, await backend.CurrentProfileAsync(Stored(result), token));
                break;
            }
            case P.RpcRequest.OperationOneofCase.AuthRecover:
            {
                var input = request.AuthRecover;
                if (string.IsNullOrWhiteSpace(input.RecoveryCode)) throw new ApiException(400, "TOTP_CODE_INVALID");
                var result = await backend.LoginAsync(new LoginInput(input.Username, input.Password,
                    TotpCode: input.RecoveryCode), token);
                response.AuthRecover = Session(result, await backend.CurrentProfileAsync(Stored(result), token));
                break;
            }
            case P.RpcRequest.OperationOneofCase.AuthPassword:
            {
                var input = request.AuthPassword;
                response.AuthPassword = new()
                {
                    PasswordInfo = ByteString.CopyFrom(await backend.LoginPasswordBytesAsync(
                        new LoginInput(input.Username, input.Password), token))
                };
                break;
            }
            case P.RpcRequest.OperationOneofCase.AuthLogout:
                response.AuthLogout = new() { Value = await backend.LogoutAsync(session!, token) };
                break;
            case P.RpcRequest.OperationOneofCase.AccountMe:
                response.AccountMe = Profile(await backend.CurrentProfileAsync(session!, token));
                break;
            case P.RpcRequest.OperationOneofCase.Premium:
            {
                var state = await backend.PremiumAsync(session!, request.Premium.Activate, token: token);
                response.Premium = new P.PremiumStatus { Active = state.Active };
                break;
            }
            case P.RpcRequest.OperationOneofCase.ChangeGid:
                await backend.ChangeGidAsync(session!, new(request.ChangeGid.RegionCode, request.ChangeGid.Number),
                    TelegramGateway.User, token);
                response.ChangeGid = Profile(await backend.CurrentProfileAsync(session!, token));
                break;
            case P.RpcRequest.OperationOneofCase.Totp:
                response.Totp = Totp(await backend.TotpAsync(session!, request.Totp.Operation, request.Totp.Code, token));
                break;
            default:
                throw new ApiException(400, "BOTGRAM_AUTH_PLANE_OPERATION_INVALID");
        }
        return response;
    }
}
