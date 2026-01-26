package org.github.dbjo.rdb;

import java.util.Arrays;

public final class IndexKeys {
    private IndexKeys() {}

    private static final byte ESC = 0x00;
    private static final byte ESC_00 = 0x01;
    public static final byte[] SEP = new byte[]{0x00, 0x00};

    /** indexKey = ESC(valueBytes) + 0x00 0x00 + pkBytes */
    public static byte[] unique(byte[] valueBytes, byte[] pkBytes) {
        byte[] ev = escape(valueBytes);
        byte[] out = new byte[ev.length + SEP.length + pkBytes.length];
        System.arraycopy(ev, 0, out, 0, ev.length);
        out[ev.length] = 0x00;
        out[ev.length + 1] = 0x00;
        System.arraycopy(pkBytes, 0, out, ev.length + 2, pkBytes.length);
        return out;
    }

    /** Prefix to seek/iterate for a single value (Eq): ESC(value) + SEP */
    public static byte[] prefix(byte[] valueBytes) {
        return ByteArrays.concat(escape(valueBytes), SEP);
    }

    /** Find separator position (index into key where SEP starts). */
    public static int sepPos(byte[] idxKey) {
        for (int i = 0; i < idxKey.length - 1; i++) {
            if (idxKey[i] != ESC) continue;
            byte b2 = idxKey[i + 1];
            if (b2 == 0x00) return i;      // SEP found
            if (b2 == ESC_00) { i++; continue; } // escaped 0x00, skip pair
            // malformed encoding
            return -1;
        }
        return -1;
    }

    /** Slice PK bytes from idxKey; returns null if malformed. */
    public static byte[] pkFromIndexKey(byte[] idxKey) {
        int p = sepPos(idxKey);
        if (p < 0) return null;
        int pkStart = p + 2;
        if (pkStart > idxKey.length) return null;
        return Arrays.copyOfRange(idxKey, pkStart, idxKey.length);
    }

    /** Slice escaped value part (for comparisons); returns null if malformed. */
    public static byte[] escapedValuePart(byte[] idxKey) {
        int p = sepPos(idxKey);
        if (p < 0) return null;
        return Arrays.copyOfRange(idxKey, 0, p);
    }

    private static byte[] escape(byte[] in) {
        if (in == null || in.length == 0) return new byte[0];
        int zeros = 0;
        for (byte b : in) if (b == 0x00) zeros++;
        if (zeros == 0) return Arrays.copyOf(in, in.length);

        byte[] out = new byte[in.length + zeros];
        int j = 0;
        for (byte b : in) {
            if (b == 0x00) {
                out[j++] = 0x00;
                out[j++] = ESC_00;
            } else {
                out[j++] = b;
            }
        }
        return out;
    }
}
