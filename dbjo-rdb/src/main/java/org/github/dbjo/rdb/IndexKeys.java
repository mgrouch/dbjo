package org.github.dbjo.rdb;

public final class IndexKeys {
    private IndexKeys() {}
    public static final byte SEP = 0;

    /** indexKey = valueBytes + 0x00 + pkBytes */
    public static byte[] unique(byte[] valueBytes, byte[] pkBytes) {
        return ByteArrays.concat(valueBytes, SEP, pkBytes);
    }
}
