using System.Diagnostics;
using System.Diagnostics.Metrics;

namespace Botgram.Backend;

// Only fixed protocol/outcome labels. No identity, credentials, URLs or payloads.
public sealed class ProtocolUsage : IDisposable
{
    public const string MeterName = "Botgram.ProtocolUsage";
    private readonly Meter meter = new(MeterName, "1.0");
    private readonly Counter<long> attempts;
    private readonly Counter<long> errors;
    private readonly Counter<long> socketEnds;
    private readonly ILogger logger;
    private readonly bool logEnabled;

    public ProtocolUsage(ILogger<ProtocolUsage> logger, IConfiguration configuration)
    {
        this.logger = logger;
        logEnabled = configuration.GetValue("Protocol:UsageLogEnabled", false);
        attempts = meter.CreateCounter<long>("botgram.protocol.attempts");
        errors = meter.CreateCounter<long>("botgram.protocol.errors");
        socketEnds = meter.CreateCounter<long>("botgram.protocol.wss.ends");
    }

    public async Task Observe(HttpContext context, RequestDelegate next)
    {
        var path = context.Request.Path.Value?.TrimEnd('/');
        var transport = path?.ToLowerInvariant() switch
        {
            "/api/telegram/rpc" or "/api/v1/rpc" => "rpc",
            "/api/ws" => "wss",
            "/api/protocol/capabilities" => "negotiation",
            _ => null
        };
        if (transport is null) { await next(context); return; }
        var protocol = path!.Equals("/api/telegram/rpc", StringComparison.OrdinalIgnoreCase) ? "json"
            : transport == "rpc" ? "protobuf" : transport == "negotiation" ? "capabilities" : "unselected";
        bool completed = false;
        try { await next(context); completed = true; }
        finally
        {
            // Successful WSS admission is counted at 101, never delayed until socket closure.
            if (!context.Items.ContainsKey("protocol_usage_recorded"))
            {
                if (context.Items["protocol_usage_selected"] is string selected) protocol = selected;
                var aborted = context.RequestAborted.IsCancellationRequested;
                var status = (!completed || aborted) && !context.Response.HasStarted ? 0 : context.Response.StatusCode;
                var outcome = aborted ? "cancelled" : status == 401 ? "authentication_failed"
                    : context.Items["protocol_usage_disabled"] is true ? "disabled"
                    : context.Items["protocol_usage_unsupported"] is true ? "unsupported_client"
                    : !completed ? "failed"
                    : status >= 500 ? "failed" : status >= 400 ? "rejected" : "success";
                Record(transport, protocol, outcome, status);
            }
        }
    }

    public void AcceptedWebSocket(HttpContext context, bool protobuf)
    {
        context.Items["protocol_usage_recorded"] = true;
        Record("wss", protobuf ? "protobuf" : "json", "success", 101);
    }

    public void EndedWebSocket(bool protobuf, string outcome)
    {
        var protocol = protobuf ? "protobuf" : "json";
        socketEnds.Add(1, new TagList { { "protocol", protocol }, { "outcome", outcome } });
        if (logEnabled) logger.LogInformation("PROTOCOL_WSS_END protocol={Protocol} outcome={Outcome}", protocol, outcome);
    }

    private void Record(string transport, string protocol, string outcome, int status)
    {
        var tags = new TagList { { "transport", transport }, { "protocol", protocol }, { "outcome", outcome }, { "http_status", status } };
        attempts.Add(1, tags);
        if (outcome != "success") errors.Add(1, tags);
        if (logEnabled) logger.LogInformation("PROTOCOL_USAGE transport={Transport} protocol={Protocol} outcome={Outcome} status={Status}", transport, protocol, outcome, status);
    }

    public void Dispose() => meter.Dispose();
}
