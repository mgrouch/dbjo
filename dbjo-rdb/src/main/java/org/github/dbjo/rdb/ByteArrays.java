package org.github.dbjo.rdb;

import java.util.Arrays;

public final class ByteArrays {
    private ByteArrays() {}

    public static byte[] concat(byte[] a, byte sep, byte[] b) {
        byte[] out = Arrays.copyOf(a, a.length + 1 + b.length);
        out[a.length] = sep;
        System.arraycopy(b, 0, out, a.length + 1, b.length);
        return out;
    }
}
