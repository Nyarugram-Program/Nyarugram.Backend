package org.telegram.messenger;

/** Host test environment only: exclude Android's local attach-path trailer from wire data. */
public final class ApplicationLoader {
    public static boolean isAndroidTestEnvironment() { return true; }
}
