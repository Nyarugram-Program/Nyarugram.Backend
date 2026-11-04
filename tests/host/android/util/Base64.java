package android.util;

/**
 * Host-JVM stand-in for the Android framework helper used by BotgramTransport.
 * The android.jar shipped with the SDK only contains stubs that throw at runtime.
 */
public final class Base64 {
    public static final int DEFAULT = 0;
    public static final int NO_PADDING = 1;
    public static final int NO_WRAP = 2;
    public static final int URL_SAFE = 8;

    private Base64() {}

    public static byte[] decode(String text, int flags) {
        return java.util.Base64.getMimeDecoder().decode(text);
    }

    public static String encodeToString(byte[] data, int flags) {
        return java.util.Base64.getEncoder().encodeToString(data);
    }
}
