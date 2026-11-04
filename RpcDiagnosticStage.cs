using System.Diagnostics;

namespace Botgram.Backend;

// Inclusive, wall-clock stages. Nested stages must not be added to their parents.
internal readonly struct RpcDiagnosticStage : IDisposable
{
    private readonly Activity? activity;
    private readonly string name;
    private readonly long started;
    private RpcDiagnosticStage(Activity? activity, string name)
    {
        this.activity = activity;
        this.name = activity is null ? string.Empty : "diag." + name;
        started = activity is null ? 0 : Stopwatch.GetTimestamp();
    }
    public static RpcDiagnosticStage Start(string name)
        => new(Activity.Current?.GetTagItem("diag.enabled") is true ? Activity.Current : null, name);
    public static void Tag(string name, object value)
    {
        if (Activity.Current?.GetTagItem("diag.enabled") is true) Activity.Current.SetTag("diag." + name, value);
    }
    public void Dispose()
    {
        if (activity is null) return;
        var previous = activity.GetTagItem(name) is double value ? value : 0;
        activity.SetTag(name, previous + Stopwatch.GetElapsedTime(started).TotalMilliseconds);
    }
}
