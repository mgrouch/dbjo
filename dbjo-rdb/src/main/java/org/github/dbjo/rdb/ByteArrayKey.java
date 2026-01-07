package org.github.dbjo.rdb;

import java.util.Arrays;
import java.util.Objects;

/** Value-based wrapper for byte[] for HashSet/HashMap keys. */
public final class ByteArrayKey {
    private final byte[] bytes;
    private final int hash;

    public ByteArrayKey(byte[] bytes) {
        this.bytes = Objects.requireNonNull(bytes, "bytes");
        this.hash = Arrays.hashCode(bytes);
    }

    public byte[] bytes() { return bytes; }

    @Override public boolean equals(Object o) {
        return (o instanceof ByteArrayKey other) && Arrays.equals(bytes, other.bytes);
    }

    @Override public int hashCode() { return hash; }
}
