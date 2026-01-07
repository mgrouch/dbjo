package org.github.dbjo.rdb;

public final class IndexKeys {
    private IndexKeys() {}
    private static final byte SEP = 0x00;

    /** indexKey = valueKeyBytes + 0x00 + primaryKeyBytes */
    public static byte[] unique(byte[] valueKeyBytes, byte[] pkBytes) {
        return ByteArrays.concat(valueKeyBytes, SEP, pkBytes);
    }
}
