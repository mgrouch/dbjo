package org.github.dbjo.rdb;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

public final class IndexKeys {
    private IndexKeys() {}

    public static final byte SEP = 0x00;

    /** Join value bytes and pk bytes as: valueBytes + SEP + pkBytes */
    public static byte[] unique(byte[] valueBytes, byte[] pkBytes) {
        byte[] out = new byte[valueBytes.length + 1 + pkBytes.length];
        System.arraycopy(valueBytes, 0, out, 0, valueBytes.length);
        out[valueBytes.length] = SEP;
        System.arraycopy(pkBytes, 0, out, valueBytes.length + 1, pkBytes.length);
        return out;
    }

    /** Convenience: value is a UTF-8 string */
    public static byte[] uniqueUtf8(String value, byte[] pkBytes) {
        return unique(value.getBytes(StandardCharsets.UTF_8), pkBytes);
    }

    /** Convenience: both are UTF-8 strings */
    public static byte[] uniqueUtf8(String value, String pk) {
        return uniqueUtf8(value, pk.getBytes(StandardCharsets.UTF_8));
    }

    /**
     * Prefix for scanning all keys for a given value in a (value+SEP+pk) index:
     * returns valueBytes + SEP.
     */
    public static byte[] valuePrefix(byte[] valueBytes) {
        byte[] out = Arrays.copyOf(valueBytes, valueBytes.length + 1);
        out[valueBytes.length] = SEP;
        return out;
    }

    /** UTF-8 convenience for valuePrefix. */
    public static byte[] valuePrefixUtf8(String value) {
        return valuePrefix(value.getBytes(StandardCharsets.UTF_8));
    }

    /**
     * End-exclusive key for prefix scans. This returns the smallest byte array that is
     * strictly greater than any key with the given prefix (lexicographic byte order).
     *
     * If the prefix is all 0xFF bytes, there is no finite end key; returns null.
     */
    public static byte[] prefixEndExclusive(byte[] prefix) {
        byte[] end = Arrays.copyOf(prefix, prefix.length);
        for (int i = end.length - 1; i >= 0; i--) {
            int b = end[i] & 0xFF;
            if (b != 0xFF) {
                end[i] = (byte) (b + 1);
                return Arrays.copyOf(end, i + 1);
            }
        }
        return null; // no possible end key
    }
}
