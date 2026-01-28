package org.github.dbjo.rdb;

import java.util.Arrays;

/**
 * Index key encoding used by both DAO maintenance and JDBC index scans.
 *
 * We encode the *index value bytes* using a mem-comparable escaping that:
 *  - preserves lexicographic order of the original bytes
 *  - ensures the terminator sequence 0x00 0x00 occurs only once (at the end)
 *
 * Encoding:
 *  - each 0x00 byte becomes 0x00 0xFF
 *  - all other bytes unchanged
 *  - append terminator 0x00 0x00
 *
 * Unique index entry key:
 *    key = enc(indexValueBytes) || pkBytes
 */
public final class IndexKeys {
    private IndexKeys() {}

    private static final byte Z = 0x00;
    private static final byte ESC = (byte) 0xFF;

    /** Build the unique key for an index entry. */
    public static byte[] unique(byte[] indexValueBytes, byte[] pkBytes) {
        if (indexValueBytes == null) indexValueBytes = new byte[0];
        if (pkBytes == null) pkBytes = new byte[0];

        byte[] prefix = prefixEq(indexValueBytes);
        byte[] out = Arrays.copyOf(prefix, prefix.length + pkBytes.length);
        System.arraycopy(pkBytes, 0, out, prefix.length, pkBytes.length);
        return out;
    }

    /** Prefix used to scan all PKs for the exact index value (EQ/IN). */
    public static byte[] prefixEq(byte[] indexValueBytes) {
        if (indexValueBytes == null) indexValueBytes = new byte[0];

        // worst-case expansion: every byte is 0 -> doubles + terminator(2)
        byte[] tmp = new byte[indexValueBytes.length * 2 + 2];
        int p = 0;
        for (byte b : indexValueBytes) {
            if (b == Z) {
                tmp[p++] = Z;
                tmp[p++] = ESC;
            } else {
                tmp[p++] = b;
            }
        }
        tmp[p++] = Z;
        tmp[p++] = Z; // terminator

        return Arrays.copyOf(tmp, p);
    }

    /** Extract pk bytes from a unique index key. */
    public static byte[] extractPk(byte[] uniqueKey) {
        int end = encodedValueEnd(uniqueKey);
        if (end < 0) return new byte[0];
        int pkOff = end + 2;
        if (pkOff >= uniqueKey.length) return new byte[0];
        return Arrays.copyOfRange(uniqueKey, pkOff, uniqueKey.length);
    }

    /** Extract the encoded value prefix (including terminator). */
    public static byte[] extractEncodedValuePrefix(byte[] uniqueKey) {
        int end = encodedValueEnd(uniqueKey);
        if (end < 0) return new byte[0];
        return Arrays.copyOfRange(uniqueKey, 0, end + 2);
    }

    /** True if key begins with prefix bytes. */
    public static boolean startsWith(byte[] key, byte[] prefix) {
        if (key == null || prefix == null) return false;
        if (prefix.length > key.length) return false;
        for (int i = 0; i < prefix.length; i++) {
            if (key[i] != prefix[i]) return false;
        }
        return true;
    }

    /**
     * Find the terminator 0x00 0x00 that ends the encoded value.
     * Returns index of the first 0x00 in the terminator, or -1 if not found.
     */
    private static int encodedValueEnd(byte[] key) {
        if (key == null || key.length < 2) return -1;
        for (int i = 0; i < key.length - 1; i++) {
            if (key[i] == Z && key[i + 1] == Z) {
                return i;
            }
        }
        return -1;
    }
}
