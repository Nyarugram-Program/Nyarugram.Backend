package android.text;

/** Minimal environment shim for the original TL parser, not shipped in the APK. */
public final class TextUtils {
    public static boolean isEmpty(CharSequence value) { return value == null || value.length() == 0; }
    public static boolean equals(CharSequence a, CharSequence b) {
        return a == b || (a != null && b != null && a.toString().contentEquals(b));
    }
}
