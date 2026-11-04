namespace Botgram.Backend;

// One immutable startup snapshot drives both advertisements and admission.
public sealed record ProtocolPolicy(bool LegacyJsonRpcEnabled, bool LegacyJsonWssEnabled, int ProtobufRolloutPercent)
{
    public static ProtocolPolicy Load(IConfiguration configuration)
    {
        var policy = new ProtocolPolicy(configuration.GetValue("Protocol:LegacyJsonRpcEnabled", true),
            configuration.GetValue("Protocol:LegacyJsonWssEnabled", true),
            configuration.GetValue("Protocol:ProtobufRolloutPercent", 100));
        if (policy.ProtobufRolloutPercent is < 0 or > 100)
            throw new InvalidOperationException("Protocol:ProtobufRolloutPercent must be between 0 and 100");
        if (!policy.LegacyJsonRpcEnabled && policy.ProtobufRolloutPercent != 100)
            throw new InvalidOperationException("Disabling JSON RPC requires a 100 percent protobuf rollout");
        return policy;
    }
}
