package org.github.dbjo.rdb;

import java.nio.charset.StandardCharsets;
import java.util.Objects;

@FunctionalInterface
public interface IndexKeyCodec<V> {
    byte[] encode(V v);

    static IndexKeyCodec<String> stringUtf8() {
        return s -> (s == null) ? null : s.getBytes(StandardCharsets.UTF_8);
    }

    static IndexKeyCodec<byte[]> rawBytes() {
        return b -> b; // already encoded
    }

    static <V> IndexKeyCodec<V> requireNonNull(IndexKeyCodec<V> c) {
        return v -> Objects.requireNonNull(c.encode(v), "encoded index key is null");
    }
}
