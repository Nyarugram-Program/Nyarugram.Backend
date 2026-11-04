import java.nio.file.Files;
import java.nio.file.Path;
import org.json.JSONArray;
import org.json.JSONObject;
import org.telegram.botgram.BotgramTransport;
import org.telegram.tgnet.SerializedData;
import org.telegram.tgnet.TLObject;

/** Host-side contract check against the actual compiled Android TL classes. */
public final class TLCodecSmoke {
    public static void main(String[] args) throws Exception {
        JSONArray fixtures = new JSONArray(Files.readString(Path.of(args[0])));
        JSONArray samples = new JSONArray();
        for (int i = 0; i < fixtures.length(); i++) {
            JSONObject fixture = fixtures.getJSONObject(i);
            String requestType = fixture.getString("requestType");
            try {
                TLObject request = (TLObject)Class.forName(requestType).getDeclaredConstructor().newInstance();
                TLObject response = (TLObject)BotgramTransport.decode(fixture.getJSONObject("response"), TLObject.class, 0);
                SerializedData output = new SerializedData();
                response.serializeToStream(output);
                byte[] bytes = output.toByteArray();
                SerializedData input = new SerializedData(bytes);
                TLObject decoded = request.deserializeResponse(input, input.readInt32(true), true);
                if (decoded == null || decoded.getClass() != response.getClass() || input.remaining() != 0)
                    throw new AssertionError("TL response did not round-trip cleanly");
                samples.put(new JSONObject().put("requestType", requestType)
                    .put("responseType", decoded.getClass().getName())
                    .put("base64", java.util.Base64.getEncoder().encodeToString(bytes)));
            } catch (Throwable error) {
                throw new AssertionError("Fixture " + i + " for " + requestType, error);
            }
        }
        Files.writeString(Path.of(args[0] + ".tl-bytes.json"), samples.toString(2));
        System.out.println("PASS: " + fixtures.length() + " responses decoded, serialized and parsed by original Android TL classes");
    }
}
