using Google.Protobuf;
using P = Botgram.Protocol.V1;

namespace Botgram.Backend;

public static class ProtobufRpcEndpoint
{
    public const string Path = "/api/v1/rpc";
    public const string MediaType = "application/x-protobuf";
    private const int MaxRequest = 128 * 1024;
    private const int MaxResponse = 8 * 1024 * 1024;

    public static Task Handle(HttpContext context, Store store, IConfiguration configuration)
        => HandleCore(context, store, configuration, null);

    public static Task HandleRelational(HttpContext context, PostgresRelationalValidation relational, IConfiguration configuration)
        => HandleCore(context, null, configuration, relational);

    public static Task HandleBackend(HttpContext context, Storage.PostgresBackend backend, IConfiguration configuration)
        => HandleCore(context, null, configuration, null, backend);

    private static async Task HandleCore(HttpContext context, Store? store, IConfiguration configuration,
        PostgresRelationalValidation? relational, Storage.PostgresBackend? backend = null)
    {
        string requestId = "";
        var diagnostic = configuration.GetValue<bool>("Protocol:DiagnosticStages");
        var decodeStage = diagnostic ? System.Diagnostics.Stopwatch.GetTimestamp() : 0;
        try
        {
            if (!string.Equals(context.Request.ContentType?.Split(';')[0].Trim(), MediaType, StringComparison.OrdinalIgnoreCase))
                throw new ApiException(415, "BOTGRAM_MEDIA_TYPE_UNSUPPORTED");
            if (context.Request.Headers.ContentEncoding.Count != 0)
                throw new ApiException(415, "BOTGRAM_CONTENT_ENCODING_UNSUPPORTED");
            if (context.Request.ContentLength > MaxRequest) throw new ApiException(413, "BOTGRAM_REQUEST_TOO_LARGE");
            using var body = context.Request.ContentLength is > 0 and <= MaxRequest
                ? new MemoryStream((int)context.Request.ContentLength.Value)
                : new MemoryStream();
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
            if (diagnostic) System.Diagnostics.Activity.Current?.SetTag("diag.request_read_decode_ms", System.Diagnostics.Stopwatch.GetElapsedTime(decodeStage).TotalMilliseconds);
            if (request.RequestId.Length is < 1 or > 64 || request.RequestId.Any(c => !char.IsAsciiLetterOrDigit(c) && c is not '-' and not '_'))
                throw new ApiException(400, "BOTGRAM_REQUEST_ID_INVALID");
            requestId = request.RequestId;
            context.Items["rpc_request_id"] = requestId;
            if (request.ReadHistory is { } read)
            {
                context.Items["rpc_peer_kind"] = read.Peer?.PeerCase.ToString() ?? "missing";
                context.Items["rpc_max_id"] = read.MaxMessageId;
            }
            if (request.ProtocolMajor != 1) throw new ApiException(400, "BOTGRAM_PROTOCOL_MAJOR_UNSUPPORTED");
            if (request.OperationCase == P.RpcRequest.OperationOneofCase.None)
                throw new ApiException(501, "BOTGRAM_RPC_UNSUPPORTED");
            context.Items["rpc_operation"] = request.OperationCase.ToString();
            var session = (Session)context.Items["session"]!;
            var domain = configuration["Protocol:ServerDomain"]
                ?? throw new InvalidOperationException("Protocol:ServerDomain is required");
            // Serialize within the transaction: invalid/unrepresentable results must not commit writes.
            byte[] Encode(P.RpcResponse response)
            {
                var stage = diagnostic ? System.Diagnostics.Stopwatch.GetTimestamp() : 0;
                var result = response.ToByteArray();
                if (result.Length > MaxResponse) throw new ApiException(413, "BOTGRAM_RESPONSE_TOO_LARGE");
                if (stage != 0) System.Diagnostics.Activity.Current?.SetTag("diag.response_encode_ms", System.Diagnostics.Stopwatch.GetElapsedTime(stage).TotalMilliseconds);
                context.RequestAborted.ThrowIfCancellationRequested();
                return result;
            }
            var bytes = backend is not null
                ? await backend.DispatchAsync(session, request, domain, Encode, context.RequestAborted)
                : relational is not null
                ? await relational.DispatchAsync(session, request, domain, Encode, context.RequestAborted)
                : store!.Write(db =>
            {
                context.RequestAborted.ThrowIfCancellationRequested();
                Auth.Recheck(db, session);
                #region debug-point protobuf-stages
                var stage = diagnostic ? System.Diagnostics.Stopwatch.GetTimestamp() : 0;
                #endregion
                var response = ProtobufRpcMapper.Execute(db, session.UserId, request, domain, session);
                #region debug-point protobuf-stages
                if (stage != 0) System.Diagnostics.Activity.Current?.SetTag("diag.protobuf_mapper_ms", System.Diagnostics.Stopwatch.GetElapsedTime(stage).TotalMilliseconds);
                #endregion
                return Encode(response);
            });
            context.Response.ContentType = MediaType;
            await context.Response.Body.WriteAsync(bytes, context.RequestAborted);
        }
        catch (ApiException ex) { await Error(context, requestId, ex.Status, ex.Message); }
        catch (Npgsql.NpgsqlException) when (!context.Response.HasStarted)
        { await Error(context, requestId, 500, "BOTGRAM_STORAGE_FAILURE"); }
        catch (InvalidProtocolBufferException) { await Error(context, requestId, 400, "BOTGRAM_PROTOBUF_MALFORMED"); }
        catch (BadHttpRequestException ex) { await Error(context, requestId, ex.StatusCode == 413 ? 413 : 400, "BOTGRAM_REQUEST_INVALID"); }
        catch (OperationCanceledException) when (context.RequestAborted.IsCancellationRequested) { }
        catch (IOException) when (!context.Response.HasStarted)
        { await Error(context, requestId, 500, "BOTGRAM_STORAGE_OR_TRANSPORT_FAILURE"); }
        catch (UnauthorizedAccessException) when (!context.Response.HasStarted)
        { await Error(context, requestId, 500, "BOTGRAM_STORAGE_FAILURE"); }
    }

    private static async Task Error(HttpContext context, string requestId, int status, string code)
    {
        context.Items["rpc_error"] = code;
        if (context.Response.HasStarted || context.RequestAborted.IsCancellationRequested) return;
        context.Response.StatusCode = status;
        context.Response.ContentType = MediaType;
        var response = new P.RpcResponse
        {
            ProtocolMajor = 1, RequestId = requestId,
            Error = new() { HttpStatus = (uint)status, Code = code, Message = code, Retryable = status >= 500 && status != 501 }
        };
        await context.Response.Body.WriteAsync(response.ToByteArray(), context.RequestAborted);
    }
}
