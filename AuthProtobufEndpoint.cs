using Google.Protobuf;
using P = Botgram.Protocol.V1;

namespace Botgram.Backend;

/// <summary>
/// Protobuf authentication plane. /api/v1/auth is unauthenticated and replaces
/// JSON register/login/password/recovery/capability negotiation; /api/v1/account
/// carries the same RpcRequest/RpcResponse envelope for session-bound management
/// (logout, me, premium, GID, authenticator). Both endpoints are Protobuf-only;
/// no JSON body is ever accepted, sniffed or produced.
/// </summary>
public static class AuthProtobufEndpoint
{
    public const string AuthPath = "/api/v1/auth";
    public const string AccountPath = "/api/v1/account";
    private const int MaxRequest = 128 * 1024;
    private const int MaxResponse = 8 * 1024 * 1024;

    public static Task Handle(HttpContext context, bool preAuthentication)
        => HandleCore(context, preAuthentication);

    private static async Task HandleCore(HttpContext context, bool preAuthentication)
    {
        string requestId = "";
        try
        {
            if (!string.Equals(context.Request.ContentType?.Split(';')[0].Trim(),
                    JsonApiRetirement.ProtobufMediaType, StringComparison.OrdinalIgnoreCase))
                throw new ApiException(415, "BOTGRAM_MEDIA_TYPE_UNSUPPORTED");
            if (context.Request.Headers.ContentEncoding.Count != 0)
                throw new ApiException(415, "BOTGRAM_CONTENT_ENCODING_UNSUPPORTED");
            if (context.Request.ContentLength > MaxRequest) throw new ApiException(413, "BOTGRAM_REQUEST_TOO_LARGE");

            using var body = new MemoryStream();
            var buffer = new byte[8192];
            int count;
            while ((count = await context.Request.Body.ReadAsync(buffer, context.RequestAborted)) != 0)
            {
                if (body.Length + count > MaxRequest) throw new ApiException(413, "BOTGRAM_REQUEST_TOO_LARGE");
                body.Write(buffer, 0, count);
            }
            body.Position = 0;
            using var input = CodedInputStream.CreateWithLimits(body, MaxRequest, 32);
            var request = P.RpcRequest.Parser.ParseFrom(input);
            if (request.RequestId.Length is < 1 or > 64 ||
                request.RequestId.Any(c => !char.IsAsciiLetterOrDigit(c) && c is not '-' and not '_'))
                throw new ApiException(400, "BOTGRAM_REQUEST_ID_INVALID");
            requestId = request.RequestId;
            if (request.ProtocolMajor != 1) throw new ApiException(400, "BOTGRAM_PROTOCOL_MAJOR_UNSUPPORTED");

            var operation = request.OperationCase;
            if (!AuthPlane.IsAuthenticationPlane(operation))
                throw new ApiException(400, "BOTGRAM_AUTH_PLANE_OPERATION_INVALID");
            if (preAuthentication != AuthPlane.IsPreAuthentication(operation))
                throw new ApiException(401, preAuthentication ? "BOTGRAM_AUTH_PLANE_ENDPOINT_INVALID" : "unauthorized");

            Session? session = null;
            if (!preAuthentication)
            {
                var store = context.RequestServices.GetService<Store>();
                var backend = context.RequestServices.GetService<Botgram.Backend.Storage.PostgresBackend>();
                var header = context.Request.Headers.Authorization.ToString();
                if (!header.StartsWith("Bearer ", StringComparison.OrdinalIgnoreCase))
                    throw new ApiException(401, "unauthorized");
                var hash = Auth.HashToken(header[7..]);
                session = backend is not null
                    ? await Botgram.Backend.Storage.RelationalHttpRoutes.RequireAsync(context)
                    : Auth.Require(context, store!);
                if (session.TokenHash != hash) throw new ApiException(401, "unauthorized");
                context.Items["session"] = session;
                context.Items["uid"] = session.UserId;
            }

            if (preAuthentication)
                AuthPlaneRateLimit.Check(context.Connection.RemoteIpAddress?.ToString() ?? "unknown");

            byte[] Encode(P.RpcResponse response)
            {
                var bytes = response.ToByteArray();
                if (bytes.Length > MaxResponse) throw new ApiException(413, "BOTGRAM_RESPONSE_TOO_LARGE");
                return bytes;
            }

            var backendService = context.RequestServices.GetService<Botgram.Backend.Storage.PostgresBackend>();
            var legacyStore = context.RequestServices.GetService<Store>();
            byte[] output;
            if (request.OperationCase == P.RpcRequest.OperationOneofCase.AuthCapabilities)
            {
                // Discovery is read-only and must never touch the authoritative store.
                output = Encode(AuthPlane.CapabilitiesResponse(requestId));
            }
            else if (backendService is not null)
            {
                var response = await AuthPlane.ExecuteRelationalAsync(backendService, session, request, context.RequestAborted);
                output = Encode(response);
            }
            else if (legacyStore is not null)
            {
                var totp = context.RequestServices.GetRequiredService<AccountTotpService>();
                output = legacyStore.Write(db => Encode(AuthPlane.ExecuteLegacy(db, session, request, totp)));
            }
            else throw new InvalidOperationException("No authentication backend is registered");

            context.Response.ContentType = JsonApiRetirement.ProtobufMediaType;
            context.Response.ContentLength = output.Length;
            await context.Response.Body.WriteAsync(output, context.RequestAborted);
        }
        catch (ApiException ex) { await Error(context, requestId, ex.Status, ex.Message); }
        catch (Npgsql.NpgsqlException) when (!context.Response.HasStarted)
        { await Error(context, requestId, 500, "BOTGRAM_STORAGE_FAILURE"); }
        catch (InvalidProtocolBufferException) { await Error(context, requestId, 400, "BOTGRAM_PROTOBUF_MALFORMED"); }
        catch (BadHttpRequestException ex) { await Error(context, requestId, ex.StatusCode == 413 ? 413 : 400, "BOTGRAM_REQUEST_INVALID"); }
        catch (OperationCanceledException) when (context.RequestAborted.IsCancellationRequested) { }
        catch (IOException) when (!context.Response.HasStarted)
        { await Error(context, requestId, 500, "BOTGRAM_STORAGE_OR_TRANSPORT_FAILURE"); }
    }

    private static async Task Error(HttpContext context, string requestId, int status, string code)
    {
        if (context.Response.HasStarted || context.RequestAborted.IsCancellationRequested) return;
        context.Response.StatusCode = status;
        context.Response.ContentType = JsonApiRetirement.ProtobufMediaType;
        var response = new P.RpcResponse
        {
            ProtocolMajor = 1, RequestId = requestId,
            Error = new() { HttpStatus = (uint)status, Code = code, Message = code, Retryable = status >= 500 && status != 501 }
        };
        var bytes = response.ToByteArray();
        context.Response.ContentLength = bytes.Length;
        await context.Response.Body.WriteAsync(bytes, context.RequestAborted);
    }
}
