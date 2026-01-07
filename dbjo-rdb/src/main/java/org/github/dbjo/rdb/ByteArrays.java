package org.github.dbjo.rdb;

import java.util.Arrays;

public final class ByteArrays {
    private ByteArrays() {}

    /** Unsigned lexicographic compare (RocksDB ordering). */
    public static int compare(byte[] a, byte[] b) {
        if (a == b) return 0;
        if (a == null) return -1;
        if (b == null) return 1;
        int n = Math.min(a.length, b.length);
        for (int i = 0; i < n; i++) {
            int ai = a[i] & 0xFF;
            int bi = b[i] & 0xFF;
            if (ai != bi) return Integer.compare(ai, bi);
        }
        return Integer.compare(a.length, b.length);
    }

    public static int indexOf(byte[] a, byte v) {
        if (a == null) return -1;
        for (int i = 0; i < a.length; i++) if (a[i] == v) return i;
        return -1;
    }

    public static byte[] concat(byte[] a, byte[] b) {
        if (a == null || a.length == 0) return (b == null) ? new byte[0] : Arrays.copyOf(b, b.length);
        if (b == null || b.length == 0) return Arrays.copyOf(a, a.length);
        byte[] out = new byte[a.length + b.length];
        System.arraycopy(a, 0, out, 0, a.length);
        System.arraycopy(b, 0, out, a.length, b.length);
        return out;
    }

    public static byte[] concat(byte[] a, byte sep) {
        if (a == null) a = new byte[0];
        byte[] out = new byte[a.length + 1];
        System.arraycopy(a, 0, out, 0, a.length);
        out[a.length] = sep;
        return out;
    }

    public static byte[] concat(byte[] a, byte sep, byte[] b) {
        if (a == null) a = new byte[0];
        if (b == null) b = new byte[0];
        byte[] out = new byte[a.length + 1 + b.length];
        System.arraycopy(a, 0, out, 0, a.length);
        out[a.length] = sep;
        System.arraycopy(b, 0, out, a.length + 1, b.length);
        return out;
    }

    /**
     * Returns the smallest byte[] that is strictly greater than all keys with the given prefix.
     * If the prefix is all 0xFF bytes, there is no upper bound -> returns null.
     */
    public static byte[] prefixEndExclusive(byte[] prefix) {
        if (prefix == null) return null;
        byte[] end = Arrays.copyOf(prefix, prefix.length);
        for (int i = end.length - 1; i >= 0; i--) {
            int v = end[i] & 0xFF;
            if (v != 0xFF) {
                end[i] = (byte) (v + 1);
                return Arrays.copyOf(end, i + 1);
            }
        }
        return null; // no upper bound
    }
}
