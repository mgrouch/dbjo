package org.github.dbjo.rdb;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

@FunctionalInterface
public interface IndexKeyCodec<V> {
    byte[] encode(V v);

    static IndexKeyCodec<String> utf8() {
        return s -> s.getBytes(StandardCharsets.UTF_8);
    }

    /** Order-preserving long (big-endian). */
    static IndexKeyCodec<Long> int64be() {
        return v -> ByteBuffer.allocate(Long.BYTES).putLong(v).array();
    }

    static IndexKeyCodec<byte[]> bytes() {
        return Objects::requireNonNull;
    }
}
