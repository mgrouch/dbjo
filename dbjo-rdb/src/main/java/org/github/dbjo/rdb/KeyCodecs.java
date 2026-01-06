package org.github.dbjo.rdb;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public final class KeyCodecs {
    private KeyCodecs() {}

    public static KeyCodec<String> stringUtf8() {
        return new KeyCodec<>() {
            @Override public byte[] encodeKey(String key) { return key.getBytes(StandardCharsets.UTF_8); }
            @Override public String decodeKey(byte[] bytes) { return new String(bytes, StandardCharsets.UTF_8); }
        };
    }

    // Big-endian preserves numeric ordering lexicographically for signed longs if you offset.
    public static KeyCodec<Long> orderedLong() {
        return new KeyCodec<>() {
            @Override public byte[] encodeKey(Long key) {
                long v = key ^ 0x8000_0000_0000_0000L; // flip sign bit to preserve order
                return ByteBuffer.allocate(Long.BYTES).putLong(v).array();
            }
            @Override public Long decodeKey(byte[] bytes) {
                long v = ByteBuffer.wrap(bytes).getLong();
                return v ^ 0x8000_0000_0000_0000L;
            }
        };
    }
}
