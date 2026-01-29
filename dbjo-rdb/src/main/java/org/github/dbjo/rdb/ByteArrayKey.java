package org.github.dbjo.rdb;

import java.util.Arrays;

/** Hash/equals wrapper for byte[] contents. */
public final class ByteArrayKey {
    private final byte[] bytes;
    private final int hash;

    public ByteArrayKey(byte[] bytes) {
        this.bytes = (bytes == null) ? new byte[0] : Arrays.copyOf(bytes, bytes.length);
        this.hash = Arrays.hashCode(this.bytes);
    }

    public byte[] bytes() { return Arrays.copyOf(bytes, bytes.length); }

    @Override public boolean equals(Object o) {
        return (o instanceof ByteArrayKey other) && Arrays.equals(this.bytes, other.bytes);
    }

    @Override public int hashCode() { return hash; }
}
