package org.github.dbjo.rdb;

import java.util.Arrays;

/**
 * Binary-safe, order-preserving index key encoding.
 *
 * <p>Format:
 * <pre>
 *   indexKey = ESC(valueBytes) || 0x00 0x00 || pkBytes
 * </pre>
 *
 * <p>ESC is an order-preserving escape that removes any occurrence of 0x00 0x00 inside
 * the escaped value:
 * <ul>
 *   <li>b != 0x00 => [b]</li>
 *   <li>b == 0x00 => [0x00, 0x01]</li>
 * </ul>
 *
 * <p>This preserves RocksDB's unsigned lexicographic ordering by {@code valueBytes} first,
 * then {@code pkBytes}.
 */
public final class IndexKeys {
    private IndexKeys() {}

    /** Separator between (escaped) value bytes and PK bytes. */
    public static final byte[] SEP = new byte[] { 0x00, 0x00 };

    private static final byte ESC = 0x00;
    private static final byte ESC_00 = 0x01;

    /** indexKey = ESC(valueBytes) || SEP || pkBytes */
    public static byte[] unique(byte[] valueBytes, byte[] pkBytes) {
        byte[] ev = escapeValue(valueBytes);
        if (pkBytes == null) pkBytes = new byte[0];

        byte[] out = new byte[ev.length + SEP.length + pkBytes.length];
        System.arraycopy(ev, 0, out, 0, ev.length);
        out[ev.length] = 0x00;
        out[ev.length + 1] = 0x00;
        System.arraycopy(pkBytes, 0, out, ev.length + 2, pkBytes.length);
        return out;
    }

    /** Prefix for equality seek/scan: ESC(valueBytes) || SEP */
    public static byte[] prefix(byte[] valueBytes) {
        return ByteArrays.concat(escapeValue(valueBytes), SEP);
    }

    /** Escape only (no separator). */
    public static byte[] escapeValue(byte[] valueBytes) {
        if (valueBytes == null || valueBytes.length == 0) return new byte[0];

        int zeros = 0;
        for (byte b : valueBytes) if (b == 0x00) zeros++;
        if (zeros == 0) return Arrays.copyOf(valueBytes, valueBytes.length);

        byte[] out = new byte[valueBytes.length + zeros];
        int j = 0;
        for (byte b : valueBytes) {
            if (b == 0x00) {
                out[j++] = 0x00;
                out[j++] = ESC_00;
            } else {
                out[j++] = b;
            }
        }
        return out;
    }

    /**
     * Locate the separator position (start index of SEP) in a full index key.
     * Returns -1 if malformed.
     */
    public static int sepPos(byte[] idxKey) {
        if (idxKey == null) return -1;
        for (int i = 0; i < idxKey.length - 1; i++) {
            if (idxKey[i] != ESC) continue;

            byte b2 = idxKey[i + 1];
            if (b2 == 0x00) return i;            // SEP found (0x00 0x00)
            if (b2 == ESC_00) { i++; continue; } // escaped 0x00 (0x00 0x01), skip pair

            // malformed encoding
            return -1;
        }
        return -1;
    }

    /** Extract PK bytes from a full index key. Returns null if malformed. */
    public static byte[] pkFromIndexKey(byte[] idxKey) {
        int p = sepPos(idxKey);
        if (p < 0) return null;
        int pkStart = p + 2;
        if (pkStart > idxKey.length) return null;
        return Arrays.copyOfRange(idxKey, pkStart, idxKey.length);
    }

    /** Extract escaped value part from a full index key. Returns null if malformed. */
    public static byte[] escapedValuePart(byte[] idxKey) {
        int p = sepPos(idxKey);
        if (p < 0) return null;
        return Arrays.copyOfRange(idxKey, 0, p);
    }
}
