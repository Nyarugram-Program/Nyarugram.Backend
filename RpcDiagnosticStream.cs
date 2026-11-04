namespace Botgram.Backend;

// Diagnostic only. The owner restores and disposes the original HTTP streams.
internal sealed class RpcDiagnosticStream(Stream inner, string stage) : Stream
{
    public override bool CanRead => inner.CanRead;
    public override bool CanSeek => inner.CanSeek;
    public override bool CanWrite => inner.CanWrite;
    public override long Length => inner.Length;
    public override long Position { get => inner.Position; set => inner.Position = value; }
    public override void Flush() => inner.Flush();
    public override Task FlushAsync(CancellationToken token) => inner.FlushAsync(token);
    public override long Seek(long offset, SeekOrigin origin) => inner.Seek(offset, origin);
    public override void SetLength(long value) => inner.SetLength(value);
    public override int Read(byte[] buffer, int offset, int count)
    {
        using var timing = RpcDiagnosticStage.Start(stage);
        return inner.Read(buffer, offset, count);
    }
    public override async Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken token)
    {
        using var timing = RpcDiagnosticStage.Start(stage);
        return await inner.ReadAsync(buffer, offset, count, token);
    }
    public override async ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken token = default)
    {
        using var timing = RpcDiagnosticStage.Start(stage);
        return await inner.ReadAsync(buffer, token);
    }
    public override void Write(byte[] buffer, int offset, int count)
    {
        using var timing = RpcDiagnosticStage.Start(stage);
        inner.Write(buffer, offset, count);
    }
    public override async Task WriteAsync(byte[] buffer, int offset, int count, CancellationToken token)
    {
        using var timing = RpcDiagnosticStage.Start(stage);
        await inner.WriteAsync(buffer, offset, count, token);
    }
    public override async ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken token = default)
    {
        using var timing = RpcDiagnosticStage.Start(stage);
        await inner.WriteAsync(buffer, token);
    }
}
