package org.github.dbjo.rdb;

import java.nio.charset.StandardCharsets;
import java.util.Objects;

@FunctionalInterface
public interface IndexKeyCodec<V> {
    byte[] encode(V v);

    static IndexKeyCodec<String> stringUtf8() {
        return s -> (s == null) ? null : s.getBytes(StandardCharsets.UTF_8);
    }

    /** Order-preserving signed 32-bit integer encoding. */
    static IndexKeyCodec<Integer> int32() {
        return i -> {
            if (i == null) return null;
            int x = i ^ 0x8000_0000;
            return new byte[]{
                    (byte) (x >>> 24),
                    (byte) (x >>> 16),
                    (byte) (x >>> 8),
                    (byte) (x)
            };
        };
    }

    /** Order-preserving signed 64-bit integer encoding. */
    static IndexKeyCodec<Long> int64() {
        return l -> {
            if (l == null) return null;
            long x = l ^ 0x8000_0000_0000_0000L;
            return new byte[]{
                    (byte) (x >>> 56),
                    (byte) (x >>> 48),
                    (byte) (x >>> 40),
                    (byte) (x >>> 32),
                    (byte) (x >>> 24),
                    (byte) (x >>> 16),
                    (byte) (x >>> 8),
                    (byte) (x)
            };
        };
    }

    static IndexKeyCodec<byte[]> rawBytes() {
        return b -> b;
    }

    static <V> IndexKeyCodec<V> requireNonNull(IndexKeyCodec<V> c) {
        return v -> Objects.requireNonNull(c.encode(v), "encoded index key is null");
    }
}
