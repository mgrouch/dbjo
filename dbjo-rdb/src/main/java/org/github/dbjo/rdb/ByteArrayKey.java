package org.github.dbjo.rdb;

import java.util.Arrays;

public record ByteArrayKey(byte[] bytes) {
    public ByteArrayKey {
        if (bytes == null) throw new NullPointerException("bytes");
    }
    @Override public boolean equals(Object o) {
        return (o instanceof ByteArrayKey k) && Arrays.equals(bytes, k.bytes);
    }
    @Override public int hashCode() { return Arrays.hashCode(bytes); }
}
