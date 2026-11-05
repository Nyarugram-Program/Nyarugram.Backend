using System.Collections;
using Google.Protobuf;
using Google.Protobuf.Reflection;
using P = Botgram.Protocol.V1;

internal static class ProtobufParity
{
    public static bool Equal(P.RpcResponse expected, P.RpcResponse actual, long started)
    {
        long finished = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        void Visit(IMessage message)
        {
            if (message is P.UserSummary { IsSelf: true } user)
            {
                if (user.Status?.KindCase != P.UserStatus.KindOneofCase.OnlineUntilMs
                    || user.Status.OnlineUntilMs < started + 60_000 || user.Status.OnlineUntilMs > finished + 60_000)
                    throw new InvalidOperationException("Self online expiry must follow the request clock");
                user.Status.OnlineUntilMs = 60_000;
            }
            foreach (var field in message.Descriptor.Fields.InFieldNumberOrder().Where(f => f.FieldType == FieldType.Message))
            {
                var value = field.Accessor.GetValue(message);
                if (field.IsRepeated)
                {
                    foreach (IMessage child in (IEnumerable)value) Visit(child);
                }
                else if (value is IMessage child) Visit(child);
            }
        }
        // Retain byte equality for every field except the separately validated self clock.
        expected = expected.Clone();
        actual = actual.Clone();
        Visit(expected);
        Visit(actual);
        return expected.ToByteArray().SequenceEqual(actual.ToByteArray());
    }
}
