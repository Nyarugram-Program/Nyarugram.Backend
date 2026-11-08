using Microsoft.AspNetCore.Http.Features;

namespace Botgram.Backend.Storage;

/// <summary>
/// Binary media transport. Both endpoints require the same Bearer session as /api/v1/rpc
/// but carry raw bytes instead of a Protobuf envelope: one part per upload request and a
/// bounded byte range per download request.
/// </summary>
public static class MediaHttpEndpoint
{
    public static async Task Upload(HttpContext context, PostgresBackend backend, long clientFileId, int part,
        CancellationToken token)
    {
        // The service-wide Kestrel limit stays at 128 KiB for RPC; raise it for this request only.
        var feature = context.Features.Get<IHttpMaxRequestBodySizeFeature>();
        if (feature is { IsReadOnly: false }) feature.MaxRequestBodySize = MediaPolicy.MaxPartBytes;
        // saveFilePart has no total and saveBigFilePart may send -1 for an unknown size;
        // both mean "unknown" and are completed later from the declared part count.
        var totalParts = 0;
        var totalQuery = context.Request.Query["total"];
        if (totalQuery.Count > 0 && !string.IsNullOrEmpty(totalQuery[0]))
        {
            if (!int.TryParse(totalQuery[0], out totalParts) || totalParts > MediaPolicy.MaxParts)
                throw new ApiException(400, "MEDIA_PART_INVALID");
            if (totalParts < 0) totalParts = 0;
        }
        if (context.Request.ContentLength is > MediaPolicy.MaxPartBytes)
            throw new ApiException(413, "MEDIA_PART_TOO_LARGE");
        using var buffer = new MemoryStream(context.Request.ContentLength is > 0 and <= MediaPolicy.MaxPartBytes
            ? (int)context.Request.ContentLength.Value : 0);
        var chunk = new byte[8192];
        int read;
        while ((read = await context.Request.Body.ReadAsync(chunk, token)) > 0)
        {
            if (buffer.Length + read > MediaPolicy.MaxPartBytes)
                throw new ApiException(413, "MEDIA_PART_TOO_LARGE");
            buffer.Write(chunk, 0, read);
        }
        var session = (Session)context.Items["session"]!;
        await backend.UploadPartAsync(session, clientFileId, part, totalParts, buffer.ToArray(), token);
        context.Response.StatusCode = StatusCodes.Status204NoContent;
    }

    public static async Task Download(HttpContext context, PostgresBackend backend, long mediaId,
        CancellationToken token)
    {
        var offset = long.TryParse(context.Request.Query["offset"], out var requestedOffset) ? requestedOffset : 0;
        var limit = int.TryParse(context.Request.Query["limit"], out var requestedLimit)
            ? requestedLimit : MediaPolicy.MaxPartBytes;
        var session = (Session)context.Items["session"]!;
        var result = await backend.DownloadAsync(session, mediaId, offset, limit, token);
        context.Response.ContentType = "application/octet-stream";
        context.Response.ContentLength = result.Bytes.Length;
        context.Response.Headers["X-Botgram-Media-Size"] = result.TotalSize.ToString();
        await context.Response.Body.WriteAsync(result.Bytes, token);
    }
}
