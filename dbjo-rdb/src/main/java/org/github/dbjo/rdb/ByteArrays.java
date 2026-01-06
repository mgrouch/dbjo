package org.github.dbjo.rdb;

import java.util.Arrays;

final class ByteArrays {
    private ByteArrays() {}

    static int compare(byte[] a, byte[] b) {
        int n = Math.min(a.length, b.length);
        for (int i = 0; i < n; i++) {
            int ai = a[i] & 0xFF;
            int bi = b[i] & 0xFF;
            if (ai != bi) return Integer.compare(ai, bi);
        }
        return Integer.compare(a.length, b.length);
    }

    static byte[] concat(byte[] a, byte b) {
        byte[] out = Arrays.copyOf(a, a.length + 1);
        out[a.length] = b;
        return out;
    }

    static byte[] concat(byte[] a, byte[] b) {
        byte[] out = Arrays.copyOf(a, a.length + b.length);
        System.arraycopy(b, 0, out, a.length, b.length);
        return out;
    }

    /** Returns the smallest byte[] that is strictly greater than all keys with the given prefix. Null if none. */
    static byte[] prefixEndExclusive(byte[] prefix) {
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

    static int indexOf(byte[] a, byte sep) {
        for (int i = 0; i < a.length; i++) if (a[i] == sep) return i;
        return -1;
    }

    public static byte[] indexKey(byte[] valueBytes, byte sep, byte[] pkBytes) {
        byte[] out = new byte[valueBytes.length + 1 + pkBytes.length];
        System.arraycopy(valueBytes, 0, out, 0, valueBytes.length);
        out[valueBytes.length] = sep;
        System.arraycopy(pkBytes, 0, out, valueBytes.length + 1, pkBytes.length);
        return out;
    }
}
